require "logger"
require "oj"

require_relative "kafka_batch/version"
require_relative "kafka_batch/errors"
require_relative "kafka_batch/configuration"
require_relative "kafka_batch/instrumentation"
require_relative "kafka_batch/stores/base"
require_relative "kafka_batch/stores/mysql_store"
require_relative "kafka_batch/stores/redis_store"
require_relative "kafka_batch/producer"
require_relative "kafka_batch/cancellation_cache"
require_relative "kafka_batch/liveness"
require_relative "kafka_batch/lag"
require_relative "kafka_batch/topics"
require_relative "kafka_batch/fairness/scheduler"
require_relative "kafka_batch/fairness/dispatcher"
require_relative "kafka_batch/worker"
require_relative "kafka_batch/batch"
require_relative "kafka_batch/reconciler"
require_relative "kafka_batch/consumers/job_consumer"
require_relative "kafka_batch/consumers/retry_consumer"
require_relative "kafka_batch/consumers/event_consumer"
require_relative "kafka_batch/consumers/callback_consumer"
require_relative "kafka_batch/web"

module KafkaBatch
  class << self
    # ── Configuration ─────────────────────────────────────────────────────

    def configuration
      @configuration ||= Configuration.new
    end
    alias config configuration

    def configure
      yield configuration
    end

    # Identifier for THIS process/pod, used to record which consumer ran a
    # batch's callbacks. Prefers the K8s pod name (ENV["HOSTNAME"]) and falls
    # back to the OS hostname; suffixed with the PID to disambiguate workers.
    def node_id
      @node_id ||= begin
        require "socket"
        host = ENV["HOSTNAME"]
        host = Socket.gethostname if host.nil? || host.empty?
        "#{host}##{Process.pid}"
      end
    end

    # ── Store ──────────────────────────────────────────────────────────────

    # Returns the configured store singleton.
    # Thread-safe via double-checked locking.
    # @return [Stores::MysqlStore, Stores::RedisStore]
    def store
      return @store if @store
      store_mutex.synchronize do
        @store ||= begin
          config.validate!
          case config.store
          when :mysql
            Stores::MysqlStore.new
          when :redis
            Stores::RedisStore.new
          else
            raise ConfigurationError, "Unknown store: #{config.store}"
          end
        end
      end
    end

    # ── Fairness scheduler (multi-tenant WFQ) ───────────────────────────────

    # Optional Redis-backed virtual-time WFQ scheduler for STRICT weighted shares.
    # NOT used by the default fairness path (the Dispatcher needs no Redis) — it's
    # a standalone engine to build a custom dispatcher/worker around.
    # @return [Fairness::Scheduler]
    def fairness_scheduler
      @fairness_scheduler ||= Fairness::Scheduler.new
    end

    # ── Worker registry ────────────────────────────────────────────────────

    # Called automatically when a class includes KafkaBatch::Worker.
    def register_worker(klass)
      workers_mutex.synchronize do
        @workers ||= []
        @workers << klass unless @workers.include?(klass)
      end
    end

    # All registered worker classes.
    # @return [Array<Class>]
    def workers
      workers_mutex.synchronize { Array(@workers) }
    end

    # True if any registered worker opts into the multi-tenant fair lane. Used to
    # decide whether to wire the dispatcher/ready consumer and create those topics.
    # @return [Boolean]
    def fairness?
      workers.any?(&:fairness?)
    end

    # ── Karafka routing helper ─────────────────────────────────────────────
    #
    # Call this INSIDE your karafka.rb routes.draw block, passing `self` (the
    # routing builder). Make sure your worker classes are loaded first so they
    # are registered (reference them, or eager-load):
    #
    #   class KarafkaApp < Karafka::App
    #     routes.draw do
    #       MyWorker  # ensure workers are loaded/registered
    #       KafkaBatch.draw_routes(self)
    #       # ... your own routes
    #     end
    #   end
    #
    # It creates TWO consumer groups so the control plane (events/callbacks/
    # retry) is isolated from job execution and isn't blocked behind long jobs:
    #   "<consumer_group>-control" – events + callbacks + retry
    #   "<consumer_group>-jobs"    – one topic per registered worker
    #
    # With config.concurrency > 1 (recommended), control messages are then
    # worked in parallel with jobs, so progress/callbacks propagate promptly.
    def draw_routes(builder)
      cfg     = config
      workers = KafkaBatch.workers

      # Fairness is per-worker: fair workers share the ingest→ready lane; plain
      # workers consume their own topic. Both run side by side.
      fair_workers = workers.select(&:fairness?)
      plain_topics = workers.reject(&:fairness?).map(&:kafka_topic).uniq
      any_fair     = fair_workers.any?

      # Topics for the shared "-jobs" group: plain worker topics + (if any worker
      # is fair) the ready topic the dispatcher feeds.
      job_topics = plain_topics.dup
      job_topics << cfg.fairness_ready_topic if any_fair

      # Karafka's routing DSL methods (consumer_group/topic/consumer) are private
      # and only resolve with implicit self, so define routes inside the builder
      # via instance_eval. Locals remain available via closure.
      builder.instance_eval do
        consumer_group "#{cfg.consumer_group}-control" do
          topic(cfg.events_topic)    { consumer KafkaBatch::Consumers::EventConsumer }
          topic(cfg.callbacks_topic) { consumer KafkaBatch::Consumers::CallbackConsumer }
          # One retry topic per delay tier so a slow tier (e.g. large/20m) never
          # head-of-line-blocks a fast one (e.g. short/30s).
          cfg.retry_topics.each do |retry_topic|
            topic(retry_topic) { consumer KafkaBatch::Consumers::RetryConsumer }
          end
        end

        if any_fair
          # Fair workers: jobs land on the ingest topic; the Dispatcher forwards
          # them (throttled) onto the ready topic, drained by the JobConsumer swarm
          # in the -jobs group below. No Redis or extra process on the path.
          consumer_group "#{cfg.consumer_group}-dispatch" do
            topic(cfg.fairness_ingest_topic) { consumer KafkaBatch::Fairness::Dispatcher }
          end
        end

        unless job_topics.empty?
          consumer_group "#{cfg.consumer_group}-jobs" do
            # Dedup: several workers may share a topic (e.g. the config.jobs_topic
            # default). JobConsumer dispatches per-message via embedded worker_class.
            # Includes the ready topic when any worker opts into fairness.
            job_topics.uniq.each do |job_topic|
              topic(job_topic) { consumer KafkaBatch::Consumers::JobConsumer }
            end
          end
        end
      end
    end

    # ── Topic validation ───────────────────────────────────────────────────

    # Verify that all KafkaBatch topics exist in the Kafka cluster.
    # Called at boot when config.validate_topics_on_boot = true.
    # Raises ConfigurationError with a list of missing topics.
    def validate_topics!
      # Derive the real topic set from the same source as `rake create_topics`:
      # per-worker job topics (or fairness ingest/ready), plus the control plane
      # (events, callbacks, retry tiers, dead_letter). Note: config.jobs_topic is
      # NOT validated — nothing produces to or consumes from it.
      required = KafkaBatch::Topics.specs.map { |s| s[:name] }.compact.uniq

      # Attempt to list topics via WaterDrop's internal Rdkafka handle
      existing = begin
        producer   = KafkaBatch::Producer.instance
        rd_handle  = producer.respond_to?(:client) ? producer.client : nil
        if rd_handle.respond_to?(:metadata)
          rd_handle.metadata(true, nil, 5000).topics.map(&:topic)
        else
          nil  # can't introspect – skip
        end
      rescue => e
        logger.warn("[KafkaBatch] validate_topics!: could not fetch topic list: #{e.message}")
        nil
      end

      return if existing.nil?  # skip if we couldn't fetch

      missing = required - existing
      unless missing.empty?
        raise ConfigurationError,
          "The following Kafka topics do not exist: #{missing.join(', ')}. " \
          "Create them or set config.validate_topics_on_boot = false to suppress this check."
      end

      logger.info("[KafkaBatch] All #{required.size} required topics verified.")

      validate_fairness_partitions!(strict: true)
    end

    # Number of partitions on the fairness ingest topic, or nil if it can't be
    # determined (Karafka::Admin unavailable / cluster unreachable / topic missing).
    def fairness_ingest_partition_count
      return nil unless defined?(Karafka) && defined?(Karafka::Admin)

      topic = Karafka::Admin.cluster_info.topics
                            .find { |t| t[:topic_name] == config.fairness_ingest_topic }
      topic && topic[:partition_count]
    rescue => e
      logger.warn("[KafkaBatch] could not read partition count for '#{config.fairness_ingest_topic}': #{e.message}")
      nil
    end

    # Warn (or raise, when strict) if the fairness ingest topic has too few
    # partitions. Tenants are spread across partitions by key hash, so too few
    # means tenants collide onto one partition and fairness degrades — a single
    # partition gives no fairness at all. No-op unless a worker opts into fairness.
    # @param strict [Boolean] raise ConfigurationError instead of warning
    def validate_fairness_partitions!(strict: config.validate_topics_on_boot)
      return unless fairness?

      count = fairness_ingest_partition_count
      return if count.nil?  # couldn't determine — don't false-alarm

      min = [config.fairness_min_ingest_partitions.to_i, 2].max
      return if count >= min

      msg = "[KafkaBatch] a worker opts into fairness but ingest topic '#{config.fairness_ingest_topic}' has " \
            "#{count} partition(s) (recommended >= #{min}). Tenants are hashed to partitions, so too " \
            "few means tenants share a partition (1 = no fairness at all). Recreate the topic with more " \
            "partitions (≈ your max concurrent tenant count)."

      raise ConfigurationError, msg if strict

      logger.warn(msg)
    end

    # ── Logging ────────────────────────────────────────────────────────────

    def logger
      config.logger
    end

    # ── Reset (for tests) ─────────────────────────────────────────────────

    def reset!
      @configuration      = nil
      @store              = nil
      @workers            = []
      @store_mutex        = nil
      @workers_mutex      = nil
      @fairness_scheduler = nil
      @node_id            = nil
      Producer.reset!
      CancellationCache.reset!
      Liveness.reset!
    end

    private

    def store_mutex
      @store_mutex ||= Mutex.new
    end

    def workers_mutex
      @workers_mutex ||= Mutex.new
    end
  end
end

# Load Rails integration if Rails is available
require_relative "kafka_batch/railtie" if defined?(Rails::Railtie)

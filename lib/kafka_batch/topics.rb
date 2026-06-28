module KafkaBatch
  # Declarative provisioning for the Kafka topics the gem uses — the Kafka
  # equivalent of a database migration.
  #
  # Kafka has no schema/migration system, so topic creation is normally a manual
  # ops step (or relies on auto-create, which is discouraged in production because
  # it can't control partition counts). This module derives the full topic set
  # from the current configuration and creates any that are missing, idempotently.
  #
  #   KafkaBatch::Topics.create_all!                 # sensible per-topic defaults
  #   KafkaBatch::Topics.create_all!(partitions: 12) # force every topic to 12
  #
  # Usually invoked via `rake kafka_batch:create_topics` (see the Railtie).
  module Topics
    module_function

    # Per-topic default partition counts (used when the caller doesn't force a
    # single count). These are starting points — size them for your throughput.
    DEFAULT_PARTITIONS = {
      jobs:        6,
      events:      3,
      callbacks:   1,
      retry:       3,   # per tier
      dead_letter: 1,
      ingest:      12,  # fairness: ≈ max concurrent tenants
      ready:       6    # fairness: swarm parallelism
    }.freeze

    # The full set of topics implied by the current config.
    #
    # @param partitions [Integer, nil] force this count for every topic; when nil
    #   each topic uses its DEFAULT_PARTITIONS entry.
    # @param replication_factor [Integer]
    # @return [Array<Hash>] specs: { name:, partitions:, replication_factor: }
    def specs(partitions: nil, replication_factor: 1)
      cfg   = KafkaBatch.config
      specs = []
      add   = lambda do |name, category|
        return if name.nil? || name.to_s.empty?

        specs << {
          name:               name,
          partitions:         (partitions || DEFAULT_PARTITIONS.fetch(category)).to_i,
          replication_factor: replication_factor.to_i
        }
      end

      if cfg.fairness_enabled
        # Jobs funnel through ingest -> dispatcher -> ready; per-worker topics
        # are not used in this mode.
        add.call(cfg.fairness_ingest_topic, :ingest)
        add.call(cfg.fairness_ready_topic, :ready)
      else
        # Jobs are produced to each registered worker's own topic (see
        # Batch#produce_job), so those are the real job topics — not jobs_topic.
        job_topics(cfg).each { |t| add.call(t, :jobs) }
      end

      add.call(cfg.events_topic, :events)
      add.call(cfg.callbacks_topic, :callbacks)
      cfg.retry_topics.each { |t| add.call(t, :retry) }
      add.call(cfg.dead_letter_topic, :dead_letter)

      specs.uniq { |s| s[:name] }
    end

    # Create every configured topic that doesn't already exist. Existing topics
    # are left untouched (Kafka can only grow partitions, never shrink, so we
    # never silently mutate them — log and skip instead).
    #
    # @return [Hash] { created: [names], skipped: [names], failed: [{name:, error:}] }
    def create_all!(partitions: nil, replication_factor: 1, logger: KafkaBatch.logger)
      unless defined?(Karafka) && defined?(Karafka::Admin)
        raise KafkaBatch::ConfigurationError,
              "Karafka::Admin is required to create topics (load Karafka first)"
      end

      existing = existing_topic_names
      result   = { created: [], skipped: [], failed: [] }

      specs(partitions: partitions, replication_factor: replication_factor).each do |spec|
        if existing.include?(spec[:name])
          logger&.info("[KafkaBatch::Topics] exists  #{spec[:name]} (skipped)")
          result[:skipped] << spec[:name]
          next
        end

        begin
          Karafka::Admin.create_topic(spec[:name], spec[:partitions], spec[:replication_factor])
          logger&.info(
            "[KafkaBatch::Topics] created #{spec[:name]} " \
            "(partitions=#{spec[:partitions]} rf=#{spec[:replication_factor]})"
          )
          result[:created] << spec[:name]
        rescue StandardError => e
          # A racing creator (or eventual-consistency on the topic list) shows up
          # as "already exists" — treat that as skipped, not failed.
          if e.message.to_s.match?(/exist/i)
            logger&.info("[KafkaBatch::Topics] exists  #{spec[:name]} (skipped, raced)")
            result[:skipped] << spec[:name]
          else
            logger&.error("[KafkaBatch::Topics] FAILED  #{spec[:name]}: #{e.class}: #{e.message}")
            result[:failed] << { name: spec[:name], error: e.message }
          end
        end
      end

      result
    end

    # Job topics for non-fairness mode: every registered worker's own topic.
    # Falls back to config.jobs_topic when no workers are loaded (e.g. the rake
    # task ran without eager-loading the app). Workers without a topic are
    # skipped rather than raising.
    # @return [Array<String>]
    def job_topics(cfg = KafkaBatch.config)
      topics = KafkaBatch.workers.filter_map do |w|
        w.kafka_topic
      rescue StandardError
        nil
      end.uniq

      topics.empty? ? [cfg.jobs_topic].compact : topics
    end

    # Names of topics that already exist on the cluster.
    # @return [Array<String>]
    def existing_topic_names
      Karafka::Admin.cluster_info.topics.map { |t| t[:topic_name] }
    rescue StandardError => e
      KafkaBatch.logger&.warn("[KafkaBatch::Topics] could not list existing topics: #{e.message}")
      []
    end
  end
end

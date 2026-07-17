# frozen_string_literal: true

require "redis"
require "connection_pool"
require_relative "redis_client"

module KafkaBatch
  # Opt-in Redis-backed throughput/error-rate history for the Web UI's
  # Performance page. Subscribes to the existing job.processed / job.retried /
  # job.failed / workset.reclaimed instrumentation events and writes
  # best-effort HINCRBY counters into per-bucket Redis hashes — never raises
  # into the hot path (same circuit-breaker pattern as KafkaBatch::Liveness).
  #
  # Disabled by default. Enable with:
  #   config.performance_metrics_enabled = true
  #
  # Data model: one Redis HASH per (bucket, status), e.g.
  #   kafka_batch:perf:min:<bucket_start_epoch>:processed
  #   kafka_batch:perf:min:<bucket_start_epoch>:failed
  #   kafka_batch:perf:min:<bucket_start_epoch>:retried
  #   kafka_batch:perf:min:<bucket_start_epoch>:reclaimed
  # Hash field = job_type (worker_class), plus "_all" (system total across all
  # job types) and "_other" (overflow once performance_metrics_max_job_types
  # distinct job types have been seen — a safety valve, not the common case).
  # Bucket width defaults to 60s (config.performance_metrics_bucket_seconds).
  # Every UI range (5m/1h/3h/24h) reads from these same buckets — see
  # KafkaBatch::PerformanceMetrics::Reader, which downsamples for wide ranges.
  #
  # Non-Rails: call KafkaBatch::PerformanceMetrics.install! once after configure.
  module PerformanceMetrics
    KEY_PREFIX       = "kafka_batch:perf:min:"
    STATUSES         = %i[processed failed retried reclaimed].freeze
    ALL_FIELD        = "_all"
    OTHER_FIELD      = "_other"
    CIRCUIT_COOLDOWN = 30
    MAX_FIELD_BYTES  = 200
    EVENT_PATTERN    = /\A(job\.(?:processed|retried|failed)|workset\.reclaimed)\.kafka_batch\z/.freeze

    class << self
      def install!(force: false)
        return unless defined?(ActiveSupport::Notifications)
        return unless enabled?

        @mutex ||= Mutex.new
        @mutex.synchronize do
          return if @installed && !force

          @subscription = ActiveSupport::Notifications.subscribe(EVENT_PATTERN) do |*args|
            handle(ActiveSupport::Notifications::Event.new(*args))
          end
          @installed = true
          KafkaBatch.logger.info(
            "[KafkaBatch][PerformanceMetrics] installed retention=#{retention}s " \
            "bucket=#{bucket_seconds}s max_job_types=#{max_job_types}"
          )
        end
      end

      def reset!
        @mutex&.synchronize do
          if defined?(ActiveSupport::Notifications) && @subscription
            ActiveSupport::Notifications.unsubscribe(@subscription)
          end
          @subscription = nil
          @installed    = false
        end
        @pool&.shutdown(&:close) rescue nil
        @pool                = nil
        @circuit_open_until  = nil
        @known_job_types     = nil
        @known_mutex         = nil
      end

      def installed?
        !!@installed
      end

      def enabled?
        KafkaBatch.config.performance_metrics_enabled
      end

      # True when the feature is enabled AND Redis is currently reachable.
      def available?
        enabled? && !redis_with { |r| r.ping }.nil?
      end

      # ── Event handling (best-effort; never raises) ──────────────────────

      def handle(event)
        return unless enabled?
        return unless sampled?

        case event.name.sub(/\.kafka_batch\z/, "")
        when "job.processed"
          record(:processed, job_type: event.payload[:worker_class])
        when "job.retried"
          record(:retried, job_type: event.payload[:worker_class])
        when "job.failed"
          record(:failed, job_type: event.payload[:worker_class])
        when "workset.reclaimed"
          n = event.payload[:reclaimed].to_i
          record(:reclaimed, job_type: nil, count: n) if n.positive?
        end
      rescue StandardError => e
        KafkaBatch.logger.debug("[KafkaBatch][PerformanceMetrics] handle failed: #{e.message}")
      end

      # Public write entry point — best-effort, never raises.
      def record(status, job_type: nil, count: 1, at: Time.now)
        return unless enabled?

        status = status.to_sym
        return unless STATUSES.include?(status)

        field = field_for(job_type)
        key   = bucket_key(status, at)
        redis_with do |r|
          r.pipelined do |pipe|
            pipe.hincrby(key, ALL_FIELD, count)
            pipe.hincrby(key, field, count) unless field == ALL_FIELD
            pipe.expire(key, retention)
          end
        end
        nil
      end

      # ── Bucketing helpers (shared with Reader) ──────────────────────────

      def bucket_seconds
        s = KafkaBatch.config.performance_metrics_bucket_seconds.to_i
        s.positive? ? s : 60
      end

      def retention
        r = KafkaBatch.config.performance_metrics_retention.to_i
        r.positive? ? r : 24 * 3600
      end

      def max_job_types
        m = KafkaBatch.config.performance_metrics_max_job_types.to_i
        m.positive? ? m : 50
      end

      def sample_rate
        r = KafkaBatch.config.performance_metrics_sample_rate.to_f
        (r.positive? && r <= 1.0) ? r : 1.0
      end

      # Start-of-bucket epoch second containing `at`.
      def bucket_start(at = Time.now)
        secs = bucket_seconds
        (at.to_i / secs) * secs
      end

      def bucket_key(status, at = Time.now)
        "#{KEY_PREFIX}#{bucket_start(at)}:#{status}"
      end

      def reset_known_job_types!
        known_mutex.synchronize { @known_job_types = {} }
      end

      # Shared pooled/circuit-broken Redis access — also used by Reader for
      # queries, so the dashboard degrades the same way writes do.
      def redis_with
        return nil unless redis_circuit_closed?
        redis_pool.with { |r| yield r }
      rescue StandardError => e
        redis_trip_circuit!
        KafkaBatch.logger.debug("[KafkaBatch][PerformanceMetrics] Redis unavailable: #{e.message}")
        nil
      end

      private

      def sampled?
        rate = sample_rate
        rate >= 1.0 || rand < rate
      end

      # Redis hash field for a job_type — the raw name once seen (up to
      # performance_metrics_max_job_types distinct names per process), "_all"
      # when there is no job_type (e.g. workset.reclaimed sweeps), or "_other"
      # once the cap is reached (overflow safety valve).
      def field_for(job_type)
        jt = job_type.to_s.strip
        return ALL_FIELD if jt.empty?

        jt = jt.byteslice(0, MAX_FIELD_BYTES) if jt.bytesize > MAX_FIELD_BYTES
        max = max_job_types

        known_mutex.synchronize do
          @known_job_types ||= {}
          return jt if @known_job_types.key?(jt)
          return OTHER_FIELD if @known_job_types.size >= max

          @known_job_types[jt] = true
          jt
        end
      end

      def known_mutex
        @known_mutex ||= Mutex.new
      end

      def redis_pool
        @pool ||= ConnectionPool.new(size: 3, timeout: 1) do
          KafkaBatch::RedisClient.new(KafkaBatch.config, timeout: 1, reconnect_attempts: 0) ||
            raise(ConfigurationError, "Redis is not configured")
        end
      end

      def redis_circuit_closed?
        return false unless KafkaBatch.config.redis_configured?
        @circuit_open_until.nil? || Time.now >= @circuit_open_until
      end

      def redis_trip_circuit!
        @circuit_open_until = Time.now + CIRCUIT_COOLDOWN
        @pool = nil
      end
    end
  end
end

require_relative "performance_metrics/reader"

module KafkaBatch
  class Configuration
    # ── Store ────────────────────────────────────────────────────────────────
    # :mysql  – uses ActiveRecord (requires kafka_batch migrations)
    # :redis  – uses Redis (no migrations needed)
    attr_accessor :store

    # ── Kafka connection ─────────────────────────────────────────────────────
    attr_accessor :brokers          # Array<String>  e.g. ["localhost:9092"]

    # ── Topic names ──────────────────────────────────────────────────────────
    attr_accessor :jobs_topic       # String  default: "kafka_batch.jobs"
    attr_accessor :events_topic     # String  default: "kafka_batch.events"
    attr_accessor :callbacks_topic  # String  default: "kafka_batch.callbacks"
    attr_accessor :dead_letter_topic # String  default: "kafka_batch.dead_letter"

    # ── Retry topic ──────────────────────────────────────────────────────────
    # Failed jobs are forwarded here with a retry_after timestamp instead of
    # sleeping inside the job consumer (which would block the Kafka partition).
    # The RetryConsumer waits via Karafka pause() then re-enqueues to the
    # original topic.
    attr_accessor :retry_topic       # String  default: "kafka_batch.jobs.retry"

    # ── Consumer ─────────────────────────────────────────────────────────────
    attr_accessor :consumer_group   # String

    # ── Cancellation ─────────────────────────────────────────────────────────
    # When true, JobConsumer skips execution of jobs whose batch was cancelled.
    # The set of cancelled batch ids is cached per process and refreshed at most
    # once per cancellation_cache_ttl seconds (NOT read from the store on every
    # job), so cancellation takes effect within that window – some already-queued
    # jobs may still run before the next refresh, which is an accepted trade-off.
    attr_accessor :skip_cancelled_jobs   # Boolean – default true
    attr_accessor :cancellation_cache_ttl  # Integer – seconds; default 120

    # ── Liveness (running jobs / consumers dashboard) ─────────────────────────
    # Visibility into currently-running jobs and live consumer processes.
    #   :redis – (default) full per-job tracking in Redis (config.redis_url),
    #            short TTL, best-effort. Most detailed; needs Redis.
    #   :store – consumer heartbeat + sampled "current job" in the configured
    #            store (e.g. MySQL). Bounded, low-impact (writes scale with
    #            consumers, NOT job throughput); reliable via last_seen + sweep.
    #   :off   – disabled.
    attr_accessor :liveness_backend            # Symbol – default :redis
    attr_accessor :track_running_jobs          # Boolean – default true (gates :redis writes)
    attr_accessor :liveness_ttl                # Integer – seconds; default 30 (staleness window)
    attr_accessor :liveness_heartbeat_interval # Integer – seconds; default 5 (:store write throttle)

    # ── Retry behaviour ──────────────────────────────────────────────────────
    # Retries use exponential (geometric) backoff: delays grow from retry_backoff
    # (first retry) up to retry_max_backoff (the LAST retry, default 24h).
    attr_accessor :max_retries        # Integer – default per worker (worker can override)
    attr_accessor :retry_backoff      # Integer – seconds; first-retry delay (base)
    attr_accessor :retry_max_backoff  # Integer – seconds; last-retry delay cap (default 24h)

    # ── Completion-event emission retries ────────────────────────────────────
    # After a job succeeds, the consumer produces a completion event. If that
    # produce fails (transient Kafka issue) it is retried inline before giving
    # up and leaving the offset uncommitted for redelivery. These tune that
    # inline retry. NOTE: the backoff sleeps on the Karafka worker thread, so
    # keep the product (retries * backoff) modest.
    attr_accessor :event_emit_retries  # Integer – attempts; default 3
    attr_accessor :event_emit_backoff  # Integer – seconds; linear: attempt * backoff

    # ── Redis (only when store: :redis) ─────────────────────────────────────
    attr_accessor :redis_url        # String  e.g. "redis://localhost:6379/0"
    attr_accessor :redis_pool_size  # Integer

    # ── TTL for batch metadata in Redis ─────────────────────────────────────
    attr_accessor :batch_ttl        # Integer – seconds; default 7 days

    # ── Reconciliation ───────────────────────────────────────────────────────
    # A periodic sweep that re-checks "running" batches that look stuck.
    attr_accessor :reconciliation_interval  # Integer – seconds; default 300

    # Max time a single reconciler sweep is expected to take. Used purely as
    # the distributed-lock TTL so a crashed reconciler eventually releases the
    # lock. Kept independent of the staleness threshold above.
    attr_accessor :reconciler_lock_ttl      # Integer – seconds; default 600

    # ── Passthrough rdkafka config ───────────────────────────────────────────
    # Merged on top of defaults for the producer.
    attr_accessor :producer_config  # Hash<String, Object>

    # Merged on top of defaults for every consumer.
    attr_accessor :consumer_config  # Hash<String, Object>

    # ── Topic validation ─────────────────────────────────────────────────────
    # When true, KafkaBatch verifies that all configured topics exist in Kafka
    # during Rails boot (requires a working broker connection at startup).
    # Disabled by default to avoid blocking startup in test/CI environments.
    attr_accessor :validate_topics_on_boot  # Boolean  default: false

    # ── Logging ──────────────────────────────────────────────────────────────
    attr_accessor :logger

    def initialize
      @store                    = :mysql
      @skip_cancelled_jobs      = true
      @cancellation_cache_ttl   = 120
      @liveness_backend            = :redis
      @track_running_jobs          = true
      @liveness_ttl                = 30
      @liveness_heartbeat_interval = 5
      @brokers                  = ["localhost:9092"]
      @jobs_topic               = "kafka_batch.jobs"
      @events_topic             = "kafka_batch.events"
      @callbacks_topic          = "kafka_batch.callbacks"
      @dead_letter_topic        = "kafka_batch.dead_letter"
      @retry_topic              = "kafka_batch.jobs.retry"
      @consumer_group           = "kafka-batch"
      @max_retries              = 3
      @retry_backoff            = 5
      @retry_max_backoff        = 24 * 3600  # 24 hours
      @event_emit_retries       = 3
      @event_emit_backoff       = 2
      @redis_url                = "redis://localhost:6379/0"
      @redis_pool_size          = 5
      @batch_ttl                = 7 * 24 * 3600  # 7 days
      @reconciliation_interval  = 300
      @reconciler_lock_ttl      = 600
      @producer_config          = {}
      @consumer_config          = {}
      @validate_topics_on_boot  = false
      @logger                   = Logger.new($stdout).tap { |l| l.progname = "KafkaBatch" }
    end

    def validate!
      raise ConfigurationError, "store must be :mysql or :redis" unless %i[mysql redis].include?(@store)
      raise ConfigurationError, "brokers must not be empty"       if Array(@brokers).empty?

      unless %i[redis store off].include?(@liveness_backend)
        raise ConfigurationError, "liveness_backend must be :redis, :store, or :off"
      end

      if @store == :redis
        raise ConfigurationError, "redis_url must be set for :redis store" if @redis_url.nil? || @redis_url.empty?
      end
    end
  end
end

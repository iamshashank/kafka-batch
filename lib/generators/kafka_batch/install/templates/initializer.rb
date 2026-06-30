# frozen_string_literal: true
<%
  redis_store = @store == "redis"
  mysql_store = @store == "mysql"
%>
# Load the UI entry point in every process (web server, Sidekiq, Karafka worker).
#
# kafka_batch/ui provides: Configuration, the Redis/MySQL store, Lag, Liveness,
# ConsumptionControl, CancellationCache, and the Rack dashboard — everything the
# dashboard needs, with NO dependency on Karafka consumers or the producer.
#
# The Karafka server process gets the full backend (consumers, Batch, Producer,
# Reconciler, Workers DSL) via `require "kafka_batch"` at the top of karafka.rb.
# That file internally does `require_relative "kafka_batch/ui"` first, so there
# is no double-load — Ruby's $LOADED_FEATURES deduplication is transparent.
require "kafka_batch/ui"

KafkaBatch.configure do |config|
  # ── State store ─────────────────────────────────────────────────────────────
  # :mysql  – persistent, queryable, survives Redis restarts
  #           Requires: rails g kafka_batch:install --store mysql && rails db:migrate
  # :redis  – lower latency, no schema migration needed
  config.store = :<%= @store %>

  # ── Kafka brokers ────────────────────────────────────────────────────────────
  config.brokers = ENV.fetch("KAFKA_BROKERS", "localhost:9092").split(",")
<% if redis_store %>

  # ── Redis connection (store: :redis) ─────────────────────────────────────────
  # Used by BOTH the :redis store above and the :redis liveness backend below.
  config.redis_url       = ENV.fetch("REDIS_URL", "redis://localhost:6379/0")
  config.redis_pool_size = 5
  # Batch metadata TTL in Redis. After this, old batch records expire automatically.
  config.batch_ttl       = 7 * 24 * 3600   # 7 days; set nil to never expire

  # Maximum batch IDs kept in the ALL_INDEX ZSET (used by the web UI batch list).
  # Oldest entries are evicted automatically when the cap is reached so the ZSET
  # never grows unbounded.
  config.all_index_max_size = 200_000
<% end %>
<% if mysql_store %>

  # ── Optional Redis (store: :mysql) ───────────────────────────────────────────
  # The MySQL store does NOT require Redis. However, setting redis_url enables the
  # optional :redis liveness backend (full per-job tracking) and the Redis-backed
  # Fairness Scheduler. If you don't have Redis, keep these commented — MySQL
  # provides everything needed including liveness (:store backend below).
  # config.redis_url       = ENV.fetch("REDIS_URL", "redis://localhost:6379/0")
  # config.redis_pool_size = 5
<% end %>

  # ── Topic names ───────────────────────────────────────────────────────────────
  # Change these (or add a KAFKA_PREFIX env var) if you want a namespace prefix,
  # e.g. KAFKA_PREFIX=myapp → "myapp.kafka_batch.jobs".
  prefix = ENV["KAFKA_PREFIX"].to_s.strip
  prefix = prefix.empty? ? "" : "#{prefix}."

  config.jobs_topic        = "#{prefix}kafka_batch.jobs"
  config.events_topic      = "#{prefix}kafka_batch.events"
  config.callbacks_topic   = "#{prefix}kafka_batch.callbacks"
  config.dead_letter_topic = "#{prefix}kafka_batch.dead_letter"
  # Retry-topic prefix; the actual per-tier topics are <prefix>.short/.medium/.large.
  config.retry_topic       = "#{prefix}kafka_batch.jobs.retry"

  # ── Consumer group ────────────────────────────────────────────────────────────
  config.consumer_group = "#{prefix}kafka-batch"

  # ── Cancellation ──────────────────────────────────────────────────────────────
  # When true, JobConsumer skips jobs whose batch was cancelled. Cancelled batch
  # ids are cached per process and refreshed at most once per
  # cancellation_cache_ttl seconds (no per-job store read), so cancellation is
  # eventually-consistent within that window.
  config.skip_cancelled_jobs    = true
  config.cancellation_cache_ttl = 120  # seconds

  # ── Live activity (running jobs / consumers dashboard) ────────────────────────
<% if mysql_store %>
  # Backend for the /live page:
  #   :store – (recommended for MySQL) writes consumer heartbeats + a sampled
  #            "current job" to kafka_batch_consumer_heartbeats (from the
  #            migration). Low-impact: scales with consumer count, NOT job
  #            throughput. Requires no extra infrastructure.
  #   :redis – full per-job tracking in Redis (requires config.redis_url above).
  #            More detailed; use if you have Redis and want richer /live data.
  #   :off   – disabled entirely.
  config.liveness_backend             = :store
  config.track_running_jobs           = true  # sampled current-job writes to DB
  config.liveness_ttl                 = 30    # seconds; heartbeats older than this = stale
  config.liveness_heartbeat_interval  = 5     # seconds between DB heartbeat writes

  # Karafka consumers reload pause/resume state from the kafka_batch_consumption_pauses
  # table at most this often. The /lag Web UI always reads fresh state.
<% else %>
  # Backend for the /live page:
  #   :redis – (default) full per-job tracking in Redis (config.redis_url), TTL'd.
  #   :store – consumer heartbeat + sampled current job in the configured store.
  #   :off   – disabled entirely.
  config.liveness_backend             = :redis
  config.track_running_jobs           = true  # gates the per-job :redis writes
  config.liveness_ttl                 = 30    # seconds (staleness window)
  config.liveness_heartbeat_interval  = 5     # seconds (:store write throttle)

  # Karafka consumers reload pause/resume state from Redis at most this often.
  # The /lag Web UI always reads fresh state.
<% end %>
  config.consumption_control_refresh_interval = 60  # seconds

  # ── Retry behaviour ───────────────────────────────────────────────────────────
  # Tiered retries: each delay tier has its own Kafka topic
  # (<retry_topic>.short/.medium/.large), so a slow tier never head-of-line-
  # blocks a fast one. By default the Nth retry walks the progression
  # (1st→short, 2nd→medium, 3rd+→large). A Worker can pin all of its retries
  # to one tier with `retry_tier :medium`.
  config.max_retries            = 3    # attempts before dead letter (override per Worker)
  config.retry_jitter           = 0.1  # +/- 10% to avoid retry storms
  config.retry_tiers            = { short: 30, medium: 7 * 60, large: 20 * 60 }  # seconds
  config.retry_tier_progression = %i[short medium large]

  # Maximum single-pause duration (seconds) in RetryConsumer. When a retry is
  # further in the future than this, the consumer pauses for this long then
  # re-checks, keeping the partition from being suspended for extreme durations.
  config.retry_max_pause_seconds = 30

  # After this many retries a still-failing job counts toward its batch's
  # on_complete (counted as failed) so the batch needn't wait for the full retry
  # budget — the job keeps retrying up to max_retries in the background. Default
  # 3 == max_retries default → no early completion. Lower it (e.g. 1) if you
  # want the batch to finish as soon as a job has exhausted its "fast" retries.
  # on_success is unaffected (fires only when every job truly succeeds).
  config.complete_after_retries = 3

  # ── Completion-event emission retries ─────────────────────────────────────────
  # Inline retries when producing the post-job completion event fails. The backoff
  # sleeps on the Karafka worker thread, so keep retries * backoff modest.
  config.event_emit_retries = 3  # attempts
  config.event_emit_backoff = 1  # seconds; linear: attempt * backoff

  # ── Failure metadata retention ────────────────────────────────────────────────
  # Failure records are only a dashboard convenience – the real job data is
  # durable in Kafka (retry + dead-letter topics).
<% if mysql_store %>
  # For MySQL, failures_ttl is used by the reconciler purge sweep to delete rows
  # older than this threshold (no automatic TTL unlike Redis). Run
  # `rake kafka_batch:reconcile` on a schedule to keep the table bounded.
  config.failures_ttl           = 7 * 24 * 3600   # 7 days; purged by reconciler
<% else %>
  # For Redis, failures_ttl is a hard TTL on the key; records expire automatically.
  config.failures_ttl           = 24 * 3600        # 1 day; auto-expires in Redis
<% end %>
  config.max_failures_per_batch = 1000  # 0 = unlimited; caps rows/keys tracked per batch

  # ── Producer safety guard ─────────────────────────────────────────────────────
  # Raise a clear ProducerError when an encoded payload exceeds this size, rather
  # than getting an opaque rdkafka failure at the broker. Matches Kafka's typical
  # broker default of 1 MiB. Set to 0 or nil to disable the guard entirely.
  config.max_message_bytes = 1_048_576  # 1 MiB

  # ── Multi-tenant fairness (Kafka-only; NO Redis required) ────────────────────
  # Fairness is a PER-WORKER opt-in — there is no global switch. A worker that
  # declares `fairness true` shares capacity dynamically across tenants: 1 active
  # tenant uses 100%, N split ~1/N (work-conserving). Its jobs land on the ingest
  # topic (keyed by tenant); the Dispatcher forwards them onto the ready topic
  # (throttled by depth watermarks); the normal JobConsumer swarm drains it.
  #
  #   class CampaignSendWorker
  #     include KafkaBatch::Worker
  #     fairness true
  #   end
  #
  # Tag jobs via: Batch.create(tenant_id: "acme") / batch.push(Worker, payload)

  # Fairness accounting mode (only applies if you use the Redis-backed Scheduler):
  #   :time_fairness      – (recommended) vtime advances at *completion* by
  #                         actual_seconds / weight. Correct for 20-60s jobs.
  #   :job_count_fairness – vtime advances at *dispatch* by 1/weight. Simpler;
  #                         only fair when all tenants' jobs have similar runtimes.
  config.fairness_mode = :time_fairness

  # How long (seconds) the dispatcher caches the full tenant-weight map from Redis.
  # Weight changes written via the /weights UI propagate to all dispatchers within
  # this window.
  config.fairness_weight_cache_ttl = 60  # seconds

  config.fairness_ingest_topic   = "#{prefix}kafka_batch.ingest"  # per-tenant intake
  config.fairness_ready_topic    = "#{prefix}kafka_batch.ready"   # throttled execution queue
  config.fairness_ready_lag_high = 5000   # dispatcher pauses forwarding above this depth
  config.fairness_ready_lag_low  = 1000   # ...resumes below this depth
  # Tenants hash to ingest partitions, so the topic needs enough partitions
  # (≈ max concurrent tenants) or tenants collide and fairness degrades. This
  # boot check warns (or raises under validate_topics_on_boot) if it has fewer:
  config.fairness_min_ingest_partitions = 2

  # Redis-backed Scheduler (strict weighted shares) — optional; only needed if
  # you build the advanced WFQ dispatcher. Leave commented unless you use it.
  # config.fairness_global_concurrency      = 50
  # config.fairness_max_inflight_per_tenant = 3
  # config.fairness_ready_window            = 500
  # config.fairness_default_weight          = 1.0

  # ── Priority queues (non-fair, 4-topic 2-group design) ───────────────────────
  # Workers opt in by setting kafka_topic to one of these four topic names.
  #   fast-group: p1 yields briefly when p0 has lag  (weighted priority)
  #   slow-group: p1 pauses entirely while p0 has lag (strict priority)
  config.fast_p0_topic = "#{prefix}kafka_batch.jobs.fast_p0"
  config.fast_p1_topic = "#{prefix}kafka_batch.jobs.fast_p1"
  config.slow_p0_topic = "#{prefix}kafka_batch.jobs.slow_p0"
  config.slow_p1_topic = "#{prefix}kafka_batch.jobs.slow_p1"
  config.priority_lag_check_interval = 2  # seconds between p0 lag checks per p1 consumer

  # ── Reconciliation ────────────────────────────────────────────────────────────
  # A periodic sweep that re-checks "running" batches that look stuck and
<% if mysql_store %>
  # (for MySQL) purges failure rows older than failures_ttl. Runs automatically
  # inside the EventConsumer — no cron needed.
  config.reconciliation_interval = 300  # seconds between sweeps
  # MySQL advisory lock TTL: GET_LOCK('kafka_batch_reconciler', 0) auto-releases
  # after this many seconds if the holding process crashes without calling
  # RELEASE_LOCK. Should exceed the maximum expected sweep duration.
<% else %>
  # purges stale Redis keys. Runs automatically inside the EventConsumer.
  config.reconciliation_interval = 300  # seconds between sweeps
  # Redis distributed-lock TTL for a single reconciler sweep (max expected runtime).
<% end %>
  config.reconciler_lock_ttl   = 600   # seconds
  # Cap how many batches one sweep processes. Without a cap, a large incident
  # backlog can hold the lock for minutes and produce a callback burst downstream.
  # The next tick handles the remainder.
  config.max_reconcile_per_run = 100

  # ── Advanced: raw rdkafka / WaterDrop config overrides ───────────────────────
  # config.producer_config = { "compression.type" => "snappy" }
  # config.consumer_config = { "fetch.min.bytes"  => "1024"   }

  # ── Topic validation ──────────────────────────────────────────────────────────
  # When true, KafkaBatch verifies all configured topics exist in Kafka during
  # Rails boot (requires a working broker connection at startup). Disabled by
  # default to avoid blocking startup in test/CI environments.
  config.validate_topics_on_boot = false

  # ── Logging ───────────────────────────────────────────────────────────────────
  # Defaults to Rails.logger when running inside Rails.
  # config.logger = Logger.new($stdout)
end

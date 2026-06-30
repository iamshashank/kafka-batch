class CreateKafkaBatchTables < ActiveRecord::Migration[6.0]
  # Single v1 schema migration — creates every table the gem needs in one pass.
  # Run this instead of individual incremental migrations.
  #
  # Tables:
  #   kafka_batch_records              – one row per batch (the core ledger)
  #   kafka_batch_consumer_offsets     – offset deduplication (offset_inbox mode)
  #   kafka_batch_failures             – per-job failure tracking for the dashboard
  #   kafka_batch_consumer_heartbeats  – live consumer view (liveness_backend: :store)
  #   kafka_batch_consumption_pauses   – pause/resume state (store: :mysql fallback)
  #   kafka_batch_tenant_weights       – per-tenant WFQ weight overrides (store: :mysql)
  def change

    # ── kafka_batch_records ──────────────────────────────────────────────────
    # Core batch ledger. UUID primary key matches the ID generated in Ruby.
    # All job-count columns are incremented atomically by SQL expressions;
    # never replaced wholesale to avoid lost-update races.
    create_table :kafka_batch_records, id: false, force: :cascade do |t|
      t.string  :id,                    limit: 36,   null: false, primary_key: true

      # Job accounting
      t.integer :total_jobs,            null: false
      t.integer :completed_count,       null: false, default: 0
      t.integer :failed_count,          null: false, default: 0

      # Lifecycle status:
      #   pending   – created but jobs not yet produced (transient)
      #   running   – jobs are being processed
      #   success   – all jobs succeeded          → on_success callback fired
      #   complete  – all finished, ≥1 failed     → on_complete callback fired
      #   cancelled – manually cancelled
      t.string  :status,                limit: 20,   null: false, default: "running"

      # Callback worker class names (nil = no callback)
      t.string  :on_success,            limit: 255
      t.string  :on_complete,           limit: 255

      # Arbitrary caller-supplied JSON metadata
      t.text    :meta

      # Human-readable label for the dashboard (Batch.create(description: "…"))
      t.string  :description,           limit: 1000

      # Multi-tenant identifier (Batch.create(tenant_id: "acme"))
      t.string  :tenant_id,             limit: 255

      # Timestamps
      t.datetime :created_at,           null: false
      t.datetime :finished_at

      # Streaming / open batch support: jobs can be pushed incrementally until
      # locked_at is set. Completion callbacks only fire after locking.
      t.datetime :locked_at

      # At-most-once callback claim guard (CallbackConsumer UPDATE WHERE IS NULL).
      # Also used by the reconciler to detect lost callbacks.
      t.datetime :callback_dispatched_at
      t.string   :callback_dispatched_by, limit: 255
    end

    # MySQL requires an explicit ALTER for a custom string primary key
    execute "ALTER TABLE kafka_batch_records ADD PRIMARY KEY (id);"

    # Reconciler: stale_batches → WHERE status = 'running' AND created_at < ?
    add_index :kafka_batch_records, :status,     name: "idx_kb_records_status"
    add_index :kafka_batch_records, :created_at, name: "idx_kb_records_created_at"
    add_index :kafka_batch_records, %i[status created_at],
              name: "idx_kb_records_status_created_at"

    # Reconciler: done_batches_without_callback →
    #   WHERE status IN ('success','complete')
    #     AND callback_dispatched_at IS NULL
    #     AND finished_at < ?
    # Leading status prunes to terminal batches; finished_at drives the range;
    # callback_dispatched_at is a covering column for the IS NULL predicate.
    add_index :kafka_batch_records, %i[status callback_dispatched_at finished_at],
              name: "idx_kb_records_done_no_callback"

    # Dashboard tenant filter
    add_index :kafka_batch_records, :tenant_id, name: "idx_kb_records_tenant_id"


    # ── kafka_batch_consumer_offsets ─────────────────────────────────────────
    # One row per (source_topic, source_partition). The EventConsumer advances
    # last_offset monotonically to deduplicate redelivered completion events
    # without a per-job row. Only used in counting_mode: :offset_inbox.
    create_table :kafka_batch_consumer_offsets do |t|
      t.string  :source_topic,     limit: 255, null: false
      t.integer :source_partition,             null: false
      t.bigint  :last_offset,                  null: false, default: 0
      t.datetime :updated_at
    end

    add_index :kafka_batch_consumer_offsets, %i[source_topic source_partition],
              unique: true, name: "uq_kb_consumer_offsets"


    # ── kafka_batch_failures ─────────────────────────────────────────────────
    # One row per failing job (upserted on each failure event). Surfaces
    # problems in the dashboard immediately (status "retrying") rather than
    # only after the retry budget is exhausted ("failed").
    create_table :kafka_batch_failures do |t|
      t.string   :batch_id,      limit: 36,  null: false
      t.string   :job_id,        limit: 36,  null: false
      t.string   :worker_class,  limit: 255
      t.string   :error_class,   limit: 255
      t.text     :error_message
      t.integer  :attempt,                   null: false, default: 0   # 0-based
      t.string   :status,        limit: 20,  null: false, default: "failed"  # "retrying"|"failed"
      t.datetime :next_retry_at                                         # nil once exhausted
      t.datetime :failed_at,                 null: false
    end

    # Idempotent upsert: redelivered exhaustion for the same job is a no-op
    add_index :kafka_batch_failures, %i[batch_id job_id],
              unique: true, name: "uq_kb_failures"

    # list_failures: WHERE batch_id = ? ORDER BY failed_at DESC
    # Composite covers both the equality filter and the sort in one index.
    add_index :kafka_batch_failures, %i[batch_id failed_at],
              name: "idx_kb_failures_batch_failed_at"

    # list_all_failures: ORDER BY failed_at DESC (cross-batch recency view)
    add_index :kafka_batch_failures, :failed_at,
              name: "idx_kb_failures_failed_at"


    # ── kafka_batch_consumer_heartbeats ──────────────────────────────────────
    # One row per live consumer pod/thread (upserted on a throttled heartbeat).
    # Holds a sampled "current job" so the dashboard shows what each consumer
    # is working on without a per-job insert/delete. Staleness handled by
    # last_seen + periodic sweep. Only used when liveness_backend: :store.
    create_table :kafka_batch_consumer_heartbeats, id: false do |t|
      t.string   :consumer_id,        limit: 128, null: false
      t.string   :hostname,           limit: 255
      t.integer  :pid
      t.string   :topic,              limit: 255
      t.string   :current_job_id,     limit: 36
      t.string   :current_worker,     limit: 255
      t.string   :current_batch_id,   limit: 36
      t.string   :current_topic,      limit: 255
      t.integer  :current_partition
      t.integer  :jobs_done,          null: false, default: 0
      t.datetime :last_seen,          null: false
    end

    add_index :kafka_batch_consumer_heartbeats, :consumer_id,
              unique: true, name: "uq_kb_consumer_heartbeats"
    add_index :kafka_batch_consumer_heartbeats, :last_seen,
              name: "idx_kb_consumer_heartbeats_last_seen"


    # ── kafka_batch_consumption_pauses ───────────────────────────────────────
    # Pause/resume state for the /lag dashboard when store: :mysql and Redis
    # is unavailable. partition_id = -1 pauses the whole topic; any other
    # value pauses a single partition.
    create_table :kafka_batch_consumption_pauses do |t|
      t.string   :consumer_group, limit: 255, null: false
      t.string   :topic_name,     limit: 255, null: false
      t.integer  :partition_id,              null: false
      t.datetime :created_at,               null: false
    end

    add_index :kafka_batch_consumption_pauses,
              %i[consumer_group topic_name partition_id],
              unique: true, name: "uq_kb_consumption_pauses"


    # ── kafka_batch_tenant_weights ───────────────────────────────────────────
    # Per-tenant WFQ weight overrides for the Fairness::Scheduler.
    # Only used when store: :mysql. When store: :redis, weights live in the
    # kafka_batch:fair:weight Redis hash instead.
    # The Scheduler caches this table in-process for fairness_weight_cache_ttl
    # seconds (default 60s) to avoid a MySQL round-trip per job completion.
    create_table :kafka_batch_tenant_weights do |t|
      t.string  :tenant_id, limit: 255, null: false
      # DECIMAL(10,4): supports weights like 1.0000, 1.5000, 0.2500.
      # Values <= 0 are rejected by the application layer before insertion.
      t.decimal :weight, precision: 10, scale: 4, null: false, default: "1.0"
      t.datetime :updated_at, null: false
    end

    add_index :kafka_batch_tenant_weights, :tenant_id,
              unique: true, name: "uq_kb_tenant_weights_tenant_id"
  end
end

class CreateKafkaBatchScheduledJobs < ActiveRecord::Migration[6.0]
  # Delayed-job index for perform_in / perform_at when config.schedule_store = :mysql.
  # DETACHED from config.store — the batch ledger can be Redis while this large,
  # long-horizon index lives on cheap disk here.
  #
  # Holds only a COMPACT POINTER to the payload (which is in the scheduled_topic
  # on Kafka): partition_id + kafka_offset. job_id is the primary key so cancel /
  # ack are single indexed deletes; run_at drives the due scan; lease_until gives
  # crash-safe at-least-once claiming (SELECT ... FOR UPDATE SKIP LOCKED).
  def change
    create_table :kafka_batch_scheduled_jobs, id: false do |t|
      t.string   :job_id,       limit: 36,  null: false
      t.datetime :run_at,       precision: 6, null: false
      t.integer  :partition_id,             null: false
      t.bigint   :kafka_offset,             null: false
      t.string   :batch_id,     limit: 36                 # nil for standalone jobs
      t.datetime :lease_until,  precision: 6              # nil = claimable; set = leased until
      t.datetime :created_at,   precision: 6, null: false
    end

    # Primary key: cancel(job_id) / ack are O(1) point deletes.
    add_index :kafka_batch_scheduled_jobs, :job_id, unique: true, name: "uq_kb_scheduled_job_id"

    # Due scan: WHERE run_at <= now AND (lease_until IS NULL OR lease_until <= now)
    #           ORDER BY run_at LIMIT n. Composite covers the filter + ordering.
    add_index :kafka_batch_scheduled_jobs, %i[run_at lease_until],
              name: "idx_kb_scheduled_due"

    # reclaim / dashboard-by-batch lookups.
    add_index :kafka_batch_scheduled_jobs, :batch_id, name: "idx_kb_scheduled_batch_id"
  end
end

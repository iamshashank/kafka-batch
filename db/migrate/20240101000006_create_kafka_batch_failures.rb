class CreateKafkaBatchFailures < ActiveRecord::Migration[6.0]
  # Always-on failure tracking: one row per job that has failed at least once, so
  # the dashboard surfaces problems immediately (status "retrying") instead of
  # only after all retries are exhausted hours later (status "failed").
  # Upserted per (batch_id, job_id); bounded by the number of failing jobs.
  def change
    create_table :kafka_batch_failures do |t|
      t.string   :batch_id,      limit: 36,  null: false
      t.string   :job_id,        limit: 36,  null: false
      t.string   :worker_class,  limit: 255
      t.string   :error_class,   limit: 255
      t.text     :error_message
      t.integer  :attempt,       null: false, default: 0  # 0-based attempt that failed
      t.string   :status,        limit: 20,  null: false, default: "failed"  # "retrying" | "failed"
      t.datetime :next_retry_at  # when the next retry is due (nil once exhausted)
      t.datetime :failed_at,     null: false
    end

    # Idempotent recording: a redelivered exhaustion for the same job is a no-op.
    add_index :kafka_batch_failures, %i[batch_id job_id], unique: true,
              name: "uq_kafka_batch_failures"
    add_index :kafka_batch_failures, :batch_id, name: "idx_kafka_batch_failures_batch_id"
    # Supports the cross-batch "all failures" view ordered by recency.
    add_index :kafka_batch_failures, :failed_at, name: "idx_kafka_batch_failures_failed_at"
  end
end

class CreateKafkaBatchJobCompletions < ActiveRecord::Migration[6.0]
  def change
    # This table serves as the idempotency guard for job completion events.
    # A unique constraint on (batch_id, job_id) ensures that even if the
    # EventConsumer delivers a message more than once, the counter is only
    # incremented once per job.
    create_table :kafka_batch_job_completions do |t|
      t.string  :batch_id, limit: 36,  null: false
      t.string  :job_id,   limit: 36,  null: false
      t.string  :status,   limit: 10,  null: false  # "success" | "failed"

      t.datetime :created_at, null: false
    end

    # The unique constraint that makes insert-based dedup work.
    # INSERT raises ActiveRecord::RecordNotUnique on duplicate → we skip.
    add_index :kafka_batch_job_completions, %i[batch_id job_id],
              unique: true,
              name:   "uq_kafka_batch_job_completions"

    # Speed up lookups when the reconciler joins against this table
    add_index :kafka_batch_job_completions, :batch_id,
              name: "idx_kafka_batch_job_completions_batch_id"

    # Foreign key (optional – comment out if you don't want FK constraints)
    # add_foreign_key :kafka_batch_job_completions, :kafka_batch_records,
    #                 column: :batch_id, primary_key: :id, on_delete: :cascade
  end
end

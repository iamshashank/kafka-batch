class CreateKafkaBatchRecords < ActiveRecord::Migration[6.0]
  def change
    create_table :kafka_batch_records, id: false, force: :cascade do |t|
      # UUID primary key – matches the batch ID generated in Ruby
      t.string  :id,              limit: 36,  null: false, primary_key: true

      # Job counts
      t.integer :total_jobs,      null: false
      t.integer :completed_count, null: false, default: 0
      t.integer :failed_count,    null: false, default: 0

      # Lifecycle:
      #   pending   – created but jobs not yet produced (transient, should not persist)
      #   running   – jobs are being processed
      #   success   – all jobs completed successfully → on_success callback fired
      #   complete  – all jobs finished, ≥1 failed    → on_complete callback fired
      #   cancelled – manually cancelled
      t.string  :status,          limit: 20, null: false, default: "running"

      # Callback worker class names (nullable – no callback if nil)
      t.string  :on_success,      limit: 255
      t.string  :on_complete,     limit: 255

      # Arbitrary JSON metadata provided by the caller
      t.text    :meta

      t.datetime :created_at,  null: false
      t.datetime :finished_at
    end

    execute "ALTER TABLE kafka_batch_records ADD PRIMARY KEY (id);"

    add_index :kafka_batch_records, :status
    add_index :kafka_batch_records, :created_at
    # Composite index used by the reconciler query
    add_index :kafka_batch_records, %i[status created_at],
              name: "idx_kafka_batch_records_on_status_and_created_at"
  end
end

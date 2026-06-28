class AddDescriptionToKafkaBatchRecords < ActiveRecord::Migration[6.0]
  # Optional human-readable description set at Batch.create(description: "...")
  # and surfaced in the Web UI so operators can tell batches apart at a glance.
  def change
    add_column :kafka_batch_records, :description, :string, limit: 1000, null: true, default: nil
  end
end

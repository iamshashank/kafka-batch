class AddCallbackDispatchedByToKafkaBatchRecords < ActiveRecord::Migration[6.0]
  # Records which consumer pod/process actually ran a batch's callbacks, for
  # operational tracking. Set atomically when the callback dispatch is claimed.
  def change
    add_column :kafka_batch_records, :callback_dispatched_by, :string, limit: 255, null: true, default: nil
  end
end

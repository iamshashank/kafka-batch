class AddTenantIdToKafkaBatchRecords < ActiveRecord::Migration[6.0]
  # Optional tenant identifier set at Batch.create(tenant_id: "acme").
  # Used by the multi-tenant fairness scheduler as the default tenant for all
  # jobs pushed into the batch, and surfaced in the Web UI with a colour marker
  # so operators can quickly spot which tenant a batch belongs to.
  def change
    add_column :kafka_batch_records, :tenant_id, :string, limit: 255, null: true, default: nil
    add_index  :kafka_batch_records, :tenant_id
  end
end

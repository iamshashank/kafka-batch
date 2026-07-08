class CreateKafkaBatchAuditLogs < ActiveRecord::Migration[6.0]
  def change
    create_table :kafka_batch_audit_logs do |t|
      t.string   :action,     limit: 64,  null: false
      t.string   :path,       limit: 255, null: false
      t.string   :method,     limit: 8,   null: false, default: "POST"
      t.string   :actor,      limit: 255
      t.string   :node_id,    limit: 255
      t.string   :status,     limit: 16,  null: false, default: "ok"
      t.text     :metadata
      t.datetime :created_at,              null: false
    end

    add_index :kafka_batch_audit_logs, :created_at, name: "idx_kb_audit_created_at"
    add_index :kafka_batch_audit_logs, :action,     name: "idx_kb_audit_action"
  end
end

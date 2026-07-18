class CreateKafkaBatchRecurringSchedules < ActiveRecord::Migration[6.0]
  # Recurring ("whenever"-style) cron schedules + fire idempotency ledger.
  # Shared with the Go control plane (pkg/cron). Copy via:
  #   rails g kafka_batch:install --recurring
  # then rails db:migrate. Tables live on schedule_store_database_connection
  # (same DB as the delayed-job MySQL index when that is enabled).
  #
  # kafka_batch_recurring_schedules — cron definitions
  # kafka_batch_recurring_fires     — (schedule_id, fire_at) PRIMARY KEY is the
  #   exactly-once guarantee: INSERT IGNORE makes a second emit of the same
  #   instant a no-op (leader flap / Go+Ruby concurrent tickers).
  def change
    create_table :kafka_batch_recurring_schedules do |t|
      t.string   :name,           limit: 191, null: false
      t.string   :cron_expr,      limit: 120, null: false
      t.string   :timezone,       limit: 64,  null: false, default: "UTC"
      t.string   :job_type,       limit: 120, null: false
      t.json     :args_json
      t.string   :tenant_id,      limit: 120
      t.boolean  :enabled,                    null: false, default: true
      t.string   :misfire_policy, limit: 16,  null: false, default: "fire_once"
      t.datetime :next_run_at,                null: false
      t.datetime :last_fire_at
      t.datetime :created_at,                 null: false
      t.datetime :updated_at,                 null: false
    end

    add_index :kafka_batch_recurring_schedules, :name, unique: true, name: "uq_name"
    # Due scan: WHERE enabled = 1 AND next_run_at <= ? ORDER BY next_run_at
    add_index :kafka_batch_recurring_schedules, %i[enabled next_run_at], name: "idx_due"

    create_table :kafka_batch_recurring_fires, id: false do |t|
      t.bigint   :schedule_id,              null: false
      t.datetime :fire_at,                  null: false
      t.string   :status,        limit: 16, null: false, default: "pending"
      t.string   :job_id,        limit: 191
      t.datetime :created_at,               null: false
      t.datetime :dispatched_at
    end

    # Composite PK matches Go EnsureSchema / 0002_recurring_schedules.sql.
    reversible do |dir|
      dir.up do
        execute "ALTER TABLE kafka_batch_recurring_fires ADD PRIMARY KEY (schedule_id, fire_at)"
      end
      dir.down do
        execute "ALTER TABLE kafka_batch_recurring_fires DROP PRIMARY KEY"
      end
    end

    add_index :kafka_batch_recurring_fires, %i[status created_at], name: "idx_pending"
  end
end

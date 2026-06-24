require "active_record"

module KafkaBatchSpec
  module ActiveRecordSupport
    module_function

    def establish!
      return if @established

      ActiveRecord::Base.establish_connection(
        adapter:  "sqlite3",
        database: ":memory:"
      )
      load_schema!
      @established = true
    end

    # SQLite-compatible equivalent of the gem's MySQL migrations. The shipped
    # migrations use MySQL-only DDL (ALTER TABLE ... ADD PRIMARY KEY), so for
    # the test database we declare the same logical schema portably.
    def load_schema!
      ActiveRecord::Schema.verbose = false
      ActiveRecord::Schema.define do
        create_table :kafka_batch_records, id: :string, force: true do |t|
          t.integer  :total_jobs,             null: false
          t.integer  :completed_count,        null: false, default: 0
          t.integer  :failed_count,           null: false, default: 0
          t.string   :status,                 null: false, default: "running"
          t.string   :on_success
          t.string   :on_complete
          t.text     :meta
          t.datetime :created_at,             null: false
          t.datetime :finished_at
          t.datetime :callback_dispatched_at
        end
        add_index :kafka_batch_records, :status
        add_index :kafka_batch_records, %i[status created_at]
        add_index :kafka_batch_records, %i[status callback_dispatched_at finished_at]

        create_table :kafka_batch_job_completions, force: true do |t|
          t.string   :batch_id, null: false
          t.string   :job_id,   null: false
          t.string   :status,   null: false
          t.datetime :created_at, null: false
        end
        add_index :kafka_batch_job_completions, %i[batch_id job_id], unique: true
        add_index :kafka_batch_job_completions, :batch_id
      end
    end

    def truncate!
      establish!
      conn = ActiveRecord::Base.connection
      conn.execute("DELETE FROM kafka_batch_records")
      conn.execute("DELETE FROM kafka_batch_job_completions")
    end
  end
end

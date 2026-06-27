class CreateKafkaBatchConsumerHeartbeats < ActiveRecord::Migration[6.0]
  # Only used when config.liveness_backend = :store.
  #
  # One row per live consumer process, upserted on a throttled heartbeat (writes
  # scale with the number of consumers, NOT job throughput). Holds a SAMPLED
  # "current job" so the dashboard can show what each consumer is working on
  # without a per-job insert/delete. Staleness is handled by last_seen + a sweep.
  def change
    create_table :kafka_batch_consumer_heartbeats, id: false do |t|
      t.string   :consumer_id,      limit: 128, null: false
      t.string   :hostname,         limit: 255
      t.integer  :pid
      t.string   :topic,            limit: 255
      t.string   :current_job_id,   limit: 36
      t.string   :current_worker,   limit: 255
      t.string   :current_batch_id, limit: 36
      t.string   :current_topic,    limit: 255
      t.integer  :current_partition
      t.integer  :jobs_done,        null: false, default: 0
      t.datetime :last_seen,        null: false
    end

    add_index :kafka_batch_consumer_heartbeats, :consumer_id, unique: true,
              name: "uq_kafka_batch_consumer_heartbeats"
    add_index :kafka_batch_consumer_heartbeats, :last_seen,
              name: "idx_kafka_batch_consumer_heartbeats_last_seen"
  end
end

require "redis"

module KafkaBatchSpec
  module RedisHelper
    TEST_URL = ENV.fetch("KAFKA_BATCH_TEST_REDIS_URL", "redis://localhost:6379/15")

    module_function

    # Whether a Redis server is reachable for the store specs.
    def available?
      return @available unless @available.nil?
      @available = begin
        Redis.new(url: TEST_URL).ping == "PONG"
      rescue StandardError
        false
      end
    end

    def flush!
      Redis.new(url: TEST_URL).flushdb
    end

    # Simulate a batch stuck in "running" with all jobs counted but not finalized
    # (e.g. a lost completion-event path). Used by reconciler specs.
    def simulate_stuck_running!(batch_id, completed_count:, created_at: Time.now - 3600)
      r = Redis.new(url: TEST_URL)
      key = "kafka_batch:b:#{batch_id}"
      r.hset(key, "completed_count", completed_count.to_s)
      r.hset(key, "created_at", created_at.iso8601)
      r.zadd("kafka_batch:index:running", created_at.to_f, batch_id)
    end

    # Simulate a finished batch whose callback was never dispatched.
    def simulate_lost_callback!(batch_id, finished_at: Time.now - 3600)
      r = Redis.new(url: TEST_URL)
      key = "kafka_batch:b:#{batch_id}"
      r.hset(key, "status", "success")
      r.hset(key, "completed_count", "1")
      r.hset(key, "finished_at", finished_at.iso8601)
      r.zrem("kafka_batch:index:running", batch_id)
      r.zadd("kafka_batch:index:done", finished_at.to_f, batch_id)
    end
  end
end

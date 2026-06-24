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
  end
end

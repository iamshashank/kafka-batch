# frozen_string_literal: true

require "connection_pool"
require "oj"
require "securerandom"
require "time"
require_relative "../redis_client"

module KafkaBatch
  module Ai
    # One global chat history for the admin dashboard (shared across pods).
    # Stored as a Redis LIST of JSON messages; trimmed to ai_chat_history_max_lines.
    module ChatHistory
      KEY = "kafka_batch:ai:chat:history"

      class << self
        def list(limit: nil)
          max = limit || max_lines
          max = max_lines if max <= 0
          rows = redis_with { |r| r.lrange(KEY, 0, max - 1) } || []
          rows.map { |raw| Oj.load(raw) rescue nil }.compact
        end

        def append!(role:, content:, citations: nil, meta: nil)
          entry = {
            "id" => SecureRandom.hex(8),
            "role" => role.to_s,
            "content" => content.to_s,
            "at" => Time.now.utc.iso8601
          }
          entry["citations"] = citations if citations
          entry["meta"] = meta if meta

          redis_with do |r|
            r.lpush(KEY, Oj.dump(entry))
            r.ltrim(KEY, 0, max_lines - 1)
          end
          entry
        end

        def clear!
          redis_with { |r| r.del(KEY) }
          true
        end

        def size
          redis_with { |r| r.llen(KEY) } || 0
        end

        def max_lines
          n = KafkaBatch.config.ai_chat_history_max_lines.to_i
          n.positive? ? n : 500
        end

        def reset_pool!
          @pool&.shutdown(&:close) rescue nil
          @pool = nil
        end

        private

        def redis_with
          return nil unless KafkaBatch.config.redis_configured?

          pool.with { |r| yield r }
        end

        def pool
          @pool ||= ConnectionPool.new(size: 1, timeout: 3) do
            client = RedisClient.new(KafkaBatch.config)
            raise "Redis not configured" unless client

            client
          end
        end
      end
    end
  end
end

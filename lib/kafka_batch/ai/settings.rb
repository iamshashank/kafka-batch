# frozen_string_literal: true

require "connection_pool"
require "oj"
require_relative "../redis_client"
require_relative "crypto"
require_relative "knowledge_index"

module KafkaBatch
  module Ai
    # OpenRouter settings in Redis (encrypted API key). Masked on read.
    module Settings
      KEY = "kafka_batch:ai:settings"

      DEFAULT_MODELS = [
        "openai/gpt-4o-mini",
        "openai/gpt-4o",
        "anthropic/claude-sonnet-4",
        "google/gemini-2.5-flash",
        "meta-llama/llama-3.3-70b-instruct"
      ].freeze

      class << self
        def show
          raw = redis_with { |r| r.hgetall(KEY) } || {}
          key_set = !raw["api_key_ciphertext"].to_s.empty?
          {
            "configured" => key_set && Crypto.configured?,
            "api_key_set" => key_set,
            "api_key_masked" => key_set ? mask_preview(raw["api_key_preview"]) : nil,
            "model" => raw["model"].to_s.empty? ? default_model : raw["model"],
            "base_url" => raw["base_url"].to_s.empty? ? default_base_url : raw["base_url"],
            "encryption_configured" => Crypto.configured?,
            "suggested_models" => DEFAULT_MODELS,
            "chat_history_max_lines" => KafkaBatch.config.ai_chat_history_max_lines.to_i,
            "knowledge_ready" => knowledge_ready?
          }
        end

        def update!(api_key: nil, model: nil, base_url: nil, clear_api_key: false)
          fields = {}
          if clear_api_key
            fields["api_key_ciphertext"] = ""
            fields["api_key_preview"] = ""
          elsif api_key && !api_key.to_s.strip.empty?
            plain = api_key.to_s.strip
            fields["api_key_ciphertext"] = Crypto.encrypt(plain)
            fields["api_key_preview"] = plain[-4..] || plain
          end
          fields["model"] = model.to_s.strip unless model.nil?
          fields["base_url"] = base_url.to_s.strip unless base_url.nil?
          fields["updated_at"] = Time.now.utc.iso8601

          redis_with do |r|
            r.hset(KEY, fields) unless fields.empty?
          end
          show
        end

        def api_key
          raw = redis_with { |r| r.hget(KEY, "api_key_ciphertext") }
          Crypto.decrypt(raw)
        end

        def model
          m = redis_with { |r| r.hget(KEY, "model") }.to_s.strip
          m.empty? ? default_model : m
        end

        def base_url
          u = redis_with { |r| r.hget(KEY, "base_url") }.to_s.strip
          u.empty? ? default_base_url : u
        end

        def reset_pool!
          @pool&.shutdown(&:close) rescue nil
          @pool = nil
        end

        private

        def default_model
          m = KafkaBatch.config.ai_openrouter_default_model.to_s.strip
          m.empty? ? DEFAULT_MODELS.first : m
        end

        def default_base_url
          "https://openrouter.ai/api/v1"
        end

        def mask_preview(last4)
          return "***" if last4.nil? || last4.empty?

          "••••#{last4}"
        end

        def knowledge_ready?
          meta = KnowledgeIndex.meta
          !meta["corpus_version"].to_s.empty?
        rescue StandardError
          false
        end

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

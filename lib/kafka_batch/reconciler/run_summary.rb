# frozen_string_literal: true

require "oj"
require "json"

module KafkaBatch
  module Reconciler
    # Persists the latest reconciler sweep (and lock skips) in Redis for the UI.
    module RunSummary
      KEY_LAST = "kafka_batch:reconciler:last"
      KEY_SKIP = "kafka_batch:reconciler:last_skip"
      MAX_DETAILS = 25

      module_function

      # @param summary [Hash]
      def save_last!(summary)
        h = summary.transform_keys(&:to_s)
        h["details"] = Oj.dump(Array(h["details"]), mode: :compat)
        redis { |r| r.hset(KEY_LAST, h) }
      rescue StandardError => e
        KafkaBatch.logger.warn("[KafkaBatch][Reconciler::RunSummary] save_last failed: #{e.message}")
      end

      def save_skip!
        now = Time.now.utc.iso8601(3)
        redis { |r| r.hset(KEY_SKIP, "at" => now, "reason" => "lock_held") }
      rescue StandardError => e
        KafkaBatch.logger.warn("[KafkaBatch][Reconciler::RunSummary] save_skip failed: #{e.message}")
      end

      # @return [Hash, nil] symbolized keys; :details is an Array<Hash>
      def load_last
        raw = redis { |r| r.hgetall(KEY_LAST) }
        return nil if raw.nil? || raw.empty?

        parse_hash(raw)
      rescue StandardError => e
        KafkaBatch.logger.warn("[KafkaBatch][Reconciler::RunSummary] load_last failed: #{e.message}")
        nil
      end

      # @return [Hash, nil]
      def load_skip
        raw = redis { |r| r.hgetall(KEY_SKIP) }
        return nil if raw.nil? || raw.empty?

        { at: raw["at"], reason: raw["reason"] }
      rescue StandardError => e
        KafkaBatch.logger.warn("[KafkaBatch][Reconciler::RunSummary] load_skip failed: #{e.message}")
        nil
      end

      def parse_hash(raw)
        h = raw.transform_keys(&:to_sym)
        details = h[:details]
        h[:details] =
          if details.nil? || details.to_s.empty?
            []
          else
            Array(Oj.load(details, mode: :compat)).map { |d| d.transform_keys(&:to_sym) }
          end
        h
      end
      private_class_method :parse_hash

      def redis(&block)
        KafkaBatch.store.with_redis(&block)
      end
      private_class_method :redis
    end
  end
end

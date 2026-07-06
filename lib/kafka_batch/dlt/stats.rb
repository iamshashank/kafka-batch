# frozen_string_literal: true

require "oj"

module KafkaBatch
  module Dlt
    # Cached DLT topic totals + recent type breakdown for the dashboard.
    module Stats
      KEY = "kafka_batch:dlt:stats"
      TTL = 60

      module_function

      # @param refresh [Boolean] bypass Redis cache
      # @return [Hash, nil] nil when Kafka is unavailable
      def fetch(refresh: false)
        unless refresh
          cached = read_cache
          return cached if cached
        end

        stats = compute
        return nil unless stats

        write_cache(stats)
        stats
      rescue StandardError => e
        KafkaBatch.logger.warn("[KafkaBatch][Dlt::Stats] fetch failed: #{e.message}")
        nil
      end

      def compute
        reader = Reader.new
        wm     = reader.watermarks
        sample = reader.sample_messages
        reader.close

        by_type = Hash.new(0)
        sample.each { |m| by_type[m[:dlt_type].to_s.empty? ? "unknown" : m[:dlt_type]] += 1 }

        {
          topic:         wm[:topic],
          partitions:    wm[:partitions],
          total:         wm[:total],
          by_type:       by_type.sort_by { |_, c| -c }.to_h,
          sample_size:   sample.size,
          sample_limited: wm[:total] > sample.size,
          cached_at:     Time.now.utc.iso8601(3)
        }
      end

      def read_cache
        raw = redis { |r| r.get(KEY) }
        return nil if raw.nil? || raw.empty?

        Oj.load(raw, symbol_keys: true)
      rescue StandardError
        nil
      end

      def write_cache(stats)
        redis { |r| r.set(KEY, Oj.dump(stats, mode: :compat), ex: TTL) }
      rescue StandardError
        nil
      end

      def redis(&block)
        KafkaBatch.store.with_redis(&block)
      end
    end
  end
end

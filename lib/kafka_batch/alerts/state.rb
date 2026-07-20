# frozen_string_literal: true

require "connection_pool"
require "oj"
require "time"
require_relative "../redis_client"

module KafkaBatch
  module Alerts
    # Open incidents, breach counters, last evaluation, lag baselines.
    module State
      OPEN_KEY = "kafka_batch:alerts:open"
      BREACH_KEY = "kafka_batch:alerts:breach"
      HEALTHY_KEY = "kafka_batch:alerts:healthy"
      LAST_KEY = "kafka_batch:alerts:last"
      BASELINE_KEY = "kafka_batch:alerts:baseline"
      DLT_COUNTER_PREFIX = "kafka_batch:alerts:dlt:min:"
      CRON_STALE_KEY = "kafka_batch:alerts:cron_stale"
      LOCK_KEY = "kafka_batch:alerts:lock"

      class << self
        NOTIFY_DEDUPE_PREFIX = "kafka_batch:alerts:notify_dedupe:"

        def try_lock!(ttl:)
          won = redis_with { |r| r.set(LOCK_KEY, "1", nx: true, ex: [ttl.to_i, 2].max) }
          won == true || won == "OK"
        end

        def open_alerts
          raw = redis_with { |r| r.hgetall(OPEN_KEY) } || {}
          raw.map do |_fp, json|
            Oj.load(json)
          rescue StandardError
            nil
          end.compact
        end

        def get_open(fingerprint)
          raw = redis_with { |r| r.hget(OPEN_KEY, fingerprint) }
          return nil if raw.nil? || raw.empty?

          Oj.load(raw)
        rescue StandardError
          nil
        end

        def set_open!(fingerprint, incident)
          redis_with { |r| r.hset(OPEN_KEY, fingerprint, Oj.dump(incident, mode: :compat)) }
        end

        # Atomically open an incident. Returns true only for the first writer
        # (Ruby or Go) so Slack/webhook fire once per fingerprint.
        def claim_open!(fingerprint, incident)
          json = Oj.dump(incident, mode: :compat)
          won = redis_with { |r| r.hsetnx(OPEN_KEY, fingerprint, json) }
          won == true || won == 1
        end

        # Updates summary on an already-open incident without re-notifying.
        def touch_open!(fingerprint, summary:)
          open = get_open(fingerprint)
          return false if open.nil?

          open["summary"] = summary
          set_open!(fingerprint, open)
          true
        end

        # Returns true if an open incident was removed (so resolve may notify once).
        def clear_open!(fingerprint)
          n = redis_with { |r| r.hdel(OPEN_KEY, fingerprint) }
          n.to_i.positive?
        end

        # Cross-runtime notify gate: only the first claimer for event+fingerprint
        # may deliver (prevents Ruby+Go or double-tick duplicates).
        def claim_notify!(fingerprint, event, ttl:)
          key = "#{NOTIFY_DEDUPE_PREFIX}#{event}:#{fingerprint}"
          ex = [ttl.to_i, 60].max
          won = redis_with { |r| r.set(key, "1", nx: true, ex: ex) }
          won == true || won == "OK"
        end

        def breach_count(fingerprint)
          redis_with { |r| r.hget(BREACH_KEY, fingerprint) }.to_i
        end

        def incr_breach!(fingerprint)
          redis_with do |r|
            r.hincrby(BREACH_KEY, fingerprint, 1)
            r.hset(HEALTHY_KEY, fingerprint, "0")
          end
        end

        def reset_breach!(fingerprint)
          redis_with { |r| r.hset(BREACH_KEY, fingerprint, "0") }
        end

        def healthy_count(fingerprint)
          redis_with { |r| r.hget(HEALTHY_KEY, fingerprint) }.to_i
        end

        def incr_healthy!(fingerprint)
          redis_with do |r|
            r.hincrby(HEALTHY_KEY, fingerprint, 1)
            r.hset(BREACH_KEY, fingerprint, "0")
          end
        end

        def reset_healthy!(fingerprint)
          redis_with { |r| r.hset(HEALTHY_KEY, fingerprint, "0") }
        end

        def save_last!(summary)
          redis_with { |r| r.set(LAST_KEY, Oj.dump(summary, mode: :compat)) }
        end

        def load_last
          raw = redis_with { |r| r.get(LAST_KEY) }
          return nil if raw.nil? || raw.empty?

          Oj.load(raw)
        rescue StandardError
          nil
        end

        def load_baseline
          raw = redis_with { |r| r.get(BASELINE_KEY) }
          return {} if raw.nil? || raw.empty?

          Oj.load(raw)
        rescue StandardError
          {}
        end

        def save_baseline!(hash)
          redis_with { |r| r.set(BASELINE_KEY, Oj.dump(hash, mode: :compat), ex: 86_400) }
        end

        def incr_dlt!(at: Time.now)
          bucket = (at.to_i / 60) * 60
          key = "#{DLT_COUNTER_PREFIX}#{bucket}"
          redis_with do |r|
            r.incr(key)
            r.expire(key, 3600)
          end
        end

        def dlt_count_last_minute(at: Time.now)
          bucket = (at.to_i / 60) * 60
          redis_with { |r| r.get("#{DLT_COUNTER_PREFIX}#{bucket}") }.to_i
        end

        def mark_cron_stale!(schedule:, job_type:, stale_seconds:)
          redis_with do |r|
            r.hset(
              CRON_STALE_KEY,
              schedule.to_s,
              Oj.dump(
                {
                  "schedule" => schedule.to_s,
                  "job_type" => job_type.to_s,
                  "stale_seconds" => stale_seconds.to_i,
                  "at" => Time.now.utc.iso8601
                },
                mode: :compat
              )
            )
            r.expire(CRON_STALE_KEY, 3600)
          end
        end

        def cron_stale_entries
          raw = redis_with { |r| r.hgetall(CRON_STALE_KEY) } || {}
          raw.map do |_k, json|
            Oj.load(json)
          rescue StandardError
            nil
          end.compact
        end

        def clear_cron_stale!(schedule)
          redis_with { |r| r.hdel(CRON_STALE_KEY, schedule.to_s) }
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

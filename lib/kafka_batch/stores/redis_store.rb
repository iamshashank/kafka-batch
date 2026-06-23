require "redis"
require "connection_pool"
require_relative "base"

module KafkaBatch
  module Stores
    class RedisStore < Base
      # Redis key layout:
      #   kafka_batch:b:{id}            – Hash of batch fields
      #   kafka_batch:b:{id}:done_jobs  – Set of "job_id:status" strings (dedup)
      #
      # All keys expire after KafkaBatch.config.batch_ttl seconds.

      KEY_PREFIX = "kafka_batch:b"
      BATCH_DONE_LUA = <<~LUA.freeze
        -- KEYS[1] = batch hash key
        -- KEYS[2] = done_jobs set key
        -- ARGV[1] = job_id:status  (dedup member)
        -- ARGV[2] = field to increment ("completed_count" or "failed_count")
        -- ARGV[3] = ttl in seconds

        -- Dedup check: SADD returns 1 if added, 0 if already present
        local added = redis.call('SADD', KEYS[2], ARGV[1])
        if added == 0 then
          return {0, 'duplicate'}
        end

        redis.call('EXPIRE', KEYS[2], tonumber(ARGV[3]))

        -- Increment counter
        redis.call('HINCRBY', KEYS[1], ARGV[2], 1)

        local total     = tonumber(redis.call('HGET', KEYS[1], 'total_jobs'))       or 0
        local completed = tonumber(redis.call('HGET', KEYS[1], 'completed_count'))  or 0
        local failed    = tonumber(redis.call('HGET', KEYS[1], 'failed_count'))     or 0
        local status    = redis.call('HGET', KEYS[1], 'status')

        -- If already finalised (concurrent write race), treat as duplicate
        if status == 'success' or status == 'complete' or status == 'cancelled' then
          return {0, 'duplicate'}
        end

        if (completed + failed) >= total then
          local outcome = (failed > 0) and 'complete' or 'success'
          redis.call('HSET',   KEYS[1], 'status', outcome)
          redis.call('HSET',   KEYS[1], 'finished_at', tostring(ARGV[4] or ''))
          redis.call('EXPIRE', KEYS[1], tonumber(ARGV[3]))
          return {1, outcome}
        end

        return {2, 'continue'}
      LUA

      def initialize
        cfg = KafkaBatch.config
        @pool = ConnectionPool.new(size: cfg.redis_pool_size, timeout: 5) do
          Redis.new(url: cfg.redis_url)
        end
        @ttl = cfg.batch_ttl
      end

      # ── Public interface ──────────────────────────────────────────────────

      def create_batch(id:, total_jobs:, on_success: nil, on_complete: nil, meta: {})
        key = batch_key(id)
        with_redis do |r|
          # NX: only set if not exists (idempotent)
          r.hsetnx(key, "id",              id)
          # If the key already existed, hsetnx on "id" returns false – we're done.
          existing = r.hget(key, "total_jobs")
          return if existing # already created

          r.hset(key,
            "id",              id,
            "total_jobs",      total_jobs.to_s,
            "completed_count", "0",
            "failed_count",    "0",
            "status",          "running",
            "on_success",      on_success.to_s,
            "on_complete",     on_complete.to_s,
            "meta",            serialize(meta),
            "created_at",      Time.now.iso8601
          )
          r.expire(key, @ttl)
        end
      end

      def find_batch(id)
        with_redis do |r|
          h = r.hgetall(batch_key(id))
          return nil if h.nil? || h.empty?
          hash_to_batch(h)
        end
      end

      def record_job_completion(batch_id:, job_id:, status:)
        field     = status == "success" ? "completed_count" : "failed_count"
        member    = "#{job_id}:#{status}"
        now       = Time.now.iso8601
        bkey      = batch_key(batch_id)
        done_key  = "#{bkey}:done_jobs"

        result = with_redis do |r|
          r.eval(BATCH_DONE_LUA,
            keys: [bkey, done_key],
            argv: [member, field, @ttl.to_s, now]
          )
        end

        code, payload = result

        case code
        when 0
          payload == "duplicate" ? { status: :duplicate } : { status: :not_found }
        when 1
          batch = find_batch(batch_id)
          { status: :done, outcome: payload, batch: batch }
        when 2
          { status: :continue }
        end
      end

      def update_batch_status(id, status)
        with_redis do |r|
          r.hset(batch_key(id), "status", status)
        end
      end

      def stale_batches(older_than:)
        # Redis doesn't support scanning by field value efficiently.
        # In practice, the reconciler is driven by a background rake task that
        # stores batch IDs in a separate sorted set (by created_at score).
        # This implementation requires that the host app uses the reconciler pattern.
        # See KafkaBatch::Reconciler.
        KafkaBatch.logger.warn(
          "[KafkaBatch] RedisStore#stale_batches is a no-op. " \
          "Use the kafka_batch:reconcile Rake task with an external batch ID list."
        )
        []
      end

      private

      def batch_key(id)
        "#{KEY_PREFIX}:#{id}"
      end

      def with_redis(&block)
        @pool.with(&block)
      rescue Redis::BaseError => e
        raise StoreError, "Redis error: #{e.message}"
      end

      def hash_to_batch(h)
        {
          id:              h["id"],
          total_jobs:      h["total_jobs"].to_i,
          completed_count: h["completed_count"].to_i,
          failed_count:    h["failed_count"].to_i,
          status:          h["status"],
          on_success:      presence(h["on_success"]),
          on_complete:     presence(h["on_complete"]),
          meta:            deserialize(h["meta"]),
          created_at:      h["created_at"],
          finished_at:     h["finished_at"]
        }
      end

      def serialize(obj)
        return "" if obj.nil? || (obj.respond_to?(:empty?) && obj.empty?)
        Oj.dump(obj, mode: :compat)
      end

      def deserialize(str)
        return {} if str.nil? || str.empty?
        Oj.load(str)
      rescue Oj::ParseError
        {}
      end

      def presence(str)
        (str.nil? || str.empty?) ? nil : str
      end
    end
  end
end

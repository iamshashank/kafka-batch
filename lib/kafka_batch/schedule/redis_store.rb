require "redis"
require "connection_pool"
require_relative "base"

module KafkaBatch
  module Schedule
    # Redis ZSET backend for the delayed-job index.
    #
    #   kafka_batch:sched:pending   – ZSET, score = run-at epoch,       member = job_id:partition:offset
    #   kafka_batch:sched:inflight  – ZSET, score = lease-expiry epoch,  member = job_id:partition:offset
    #
    # claim_due and reclaim are each a single atomic Lua call, so the poller can
    # run in every process with no double-dispatch and no leader election.
    class RedisStore < Base
      PENDING  = "kafka_batch:sched:pending".freeze
      INFLIGHT = "kafka_batch:sched:inflight".freeze
      READ_MISS = "kafka_batch:sched:read_miss".freeze

      # Atomically claim due members: take up to ARGV[2] members with score <= now
      # from PENDING, remove them, and add them to INFLIGHT scored at the lease
      # expiry. Returns the claimed members. Ordered by run-at (score) — the poller
      # re-groups by partition/offset for efficient reads.
      #   KEYS[1]=PENDING KEYS[2]=INFLIGHT
      #   ARGV[1]=now ARGV[2]=limit ARGV[3]=lease_until
      CLAIM_DUE_LUA = <<~LUA.freeze
        local due = redis.call('ZRANGEBYSCORE', KEYS[1], '-inf', ARGV[1], 'LIMIT', 0, tonumber(ARGV[2]))
        if #due == 0 then return {} end
        for i = 1, #due do
          redis.call('ZREM', KEYS[1], due[i])
          redis.call('ZADD', KEYS[2], tonumber(ARGV[3]), due[i])
        end
        return due
      LUA

      # Return lease-expired in-flight members (score <= now) to PENDING so they
      # are re-dispatched. Runs due-now (score = now). Returns the count reclaimed.
      #   KEYS[1]=INFLIGHT KEYS[2]=PENDING  ARGV[1]=now ARGV[2]=limit
      RECLAIM_LUA = <<~LUA.freeze
        local expired = redis.call('ZRANGEBYSCORE', KEYS[1], '-inf', ARGV[1], 'LIMIT', 0, tonumber(ARGV[2]))
        for i = 1, #expired do
          redis.call('ZREM', KEYS[1], expired[i])
          redis.call('ZADD', KEYS[2], tonumber(ARGV[1]), expired[i])
        end
        return #expired
      LUA

      def initialize(pool: nil)
        cfg   = KafkaBatch.config
        @pool = pool || ConnectionPool.new(size: cfg.redis_pool_size, timeout: 5) do
          KafkaBatch::RedisClient.new(cfg) || raise(ConfigurationError, "Redis is not configured")
        end
        # Reclaim in bounded chunks so one sweep can't monopolise Redis.
        @reclaim_limit = [cfg.schedule_batch_size.to_i * 5, 500].max
      end

      def schedule(job_id:, run_at:, partition:, offset:, batch_id: nil)
        member = Member.build(job_id, partition, offset)
        with { |r| r.zadd(PENDING, epoch(run_at), member) }
        member
      end

      # Bulk schedule: one ZADD with all (score, member) pairs — a single atomic
      # command regardless of count.
      def schedule_many(entries)
        return [] if entries.empty?

        pairs = entries.map do |e|
          [epoch(e[:run_at]), Member.build(e[:job_id], e[:partition], e[:offset])]
        end
        with { |r| r.zadd(PENDING, pairs) }
        pairs.map(&:last)
      end

      def claim_due(now:, lease_seconds:, limit:)
        lease_until = epoch(now) + lease_seconds.to_i
        with do |r|
          r.eval(CLAIM_DUE_LUA,
            keys: [PENDING, INFLIGHT],
            argv: [epoch(now).to_s, limit.to_i.to_s, lease_until.to_s])
        end
      end

      def ack(members)
        members = Array(members)
        return 0 if members.empty?

        with { |r| r.zrem(INFLIGHT, members) }
      end

      def reclaim(now:)
        with do |r|
          r.eval(RECLAIM_LUA,
            keys: [INFLIGHT, PENDING],
            argv: [epoch(now).to_s, @reclaim_limit.to_s])
        end.to_i
      end

      # The Redis backend can't remove a member from job_id alone (the member also
      # embeds partition/offset, unknown here). Per-job cancellation is honoured by
      # the SchedulePoller, which checks the batch-level CancellationCache before
      # re-producing. See Schedule::Base#cancel.
      def cancel(_job_id)
        false
      end

      def list(limit: 100, offset: 0)
        rows = with do |r|
          r.zrange(PENDING, offset.to_i, offset.to_i + limit.to_i - 1, withscores: true)
        end
        rows.map do |member, score|
          parsed = Member.parse(member) || {}
          parsed.merge(run_at: Time.at(score.to_f), batch_id: nil)
        end
      end

      def size
        with { |r| r.zcard(PENDING) }.to_i
      end

      # Track repeated failed reads for a leased scheduled pointer (poison offset).
      def record_read_miss(member)
        with { |r| r.hincrby(READ_MISS, member, 1) }.to_i
      end

      def read_misses(member)
        with { |r| r.hget(READ_MISS, member).to_i }
      end

      def clear_read_miss(member)
        with { |r| r.hdel(READ_MISS, member) }
      end

      # Search by job_id. The member embeds partition:offset, so we can't ZSCORE
      # directly — ZSCAN with a "job_id:*" match finds it (job_ids are unique, so
      # at most one hit). Checks pending first, then the leased set.
      def find(job_id)
        pattern = "#{job_id}:*"
        [[PENDING, :pending], [INFLIGHT, :leased]].each do |key, state|
          hit = scan_match(key, pattern)
          next unless hit

          member, score = hit
          parsed = Member.parse(member) or next
          return parsed.merge(run_at: Time.at(score.to_f), batch_id: nil, state: state)
        end
        nil
      end

      private

      # Return the first [member, score] in +key+ matching +pattern+, or nil.
      def scan_match(key, pattern)
        cursor = "0"
        loop do
          cursor, pairs = with { |r| r.zscan(key, cursor, match: pattern, count: 200) }
          pairs.each { |member, score| return [member, score] }
          break if cursor == "0"
        end
        nil
      end

      def epoch(t)
        t.respond_to?(:to_f) ? t.to_f : Time.parse(t.to_s).to_f
      end

      def with(&block)
        @pool.with(&block)
      rescue Redis::BaseError => e
        raise StoreError, "Redis error: #{e.message}"
      end
    end
  end
end

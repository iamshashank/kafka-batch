# frozen_string_literal: true

require "connection_pool"

module KafkaBatch
  module Fairness
    # Resolves tenant_id → fairness ingest partition.
    #
    # Resolution order:
    #   1. config.fairness_tenant_partitions[tenant_id] — static map wins
    #   2. Redis checkout (when config.fairness_dynamic_tenant_partitions)
    #   3. nil → caller may fall back to murmur2 key-hash
    #
    # Dynamic mode keeps a per-lane Redis HASH (tenant → partition) and a SET of
    # free partition numbers. On boot / warm!, the free set is seeded from the
    # ingest topic's partition count minus already-assigned tenants.
    class TenantPartitions
      CHECKOUT_LUA = <<~LUA.freeze
        local tenant = ARGV[1]
        local count  = tonumber(ARGV[2])
        if not tenant or not count or count < 1 then return -2 end

        local existing = redis.call('HGET', KEYS[1], tenant)
        if existing then
          local p = tonumber(existing)
          if p and p >= 0 and p < count then return p end
          redis.call('HDEL', KEYS[1], tenant)
        end

        local p = redis.call('SPOP', KEYS[2])
        if not p then return -1 end

        p = tonumber(p)
        if not p or p < 0 or p >= count then
          redis.call('SADD', KEYS[2], p)
          return -2
        end

        redis.call('HSET', KEYS[1], tenant, p)
        return p
      LUA

      class << self
        def resolve(tenant_id, type = :time)
          return nil if tenant_id.nil?

          tid  = tenant_id.to_s
          lane = type.to_sym

          hit = read_cache(lane, tid)
          return hit unless hit.nil?

          configured = configured_partition(tid, lane)
          if configured
            write_cache(lane, tid, configured)
            return configured
          end

          return nil unless dynamic?

          partition = checkout(lane, tid)
          if partition
            write_cache(lane, tid, partition)
            return partition
          end

          nil
        end

        # Seed / reconcile the free-partition pool for a lane from the live topic
        # partition count. Safe to call on every boot and before checkout.
        def warm!(type = :time)
          return unless dynamic?

          lane  = type.to_sym
          count = KafkaBatch.fairness_ingest_partition_count(lane)
          return unless count&.positive?

          with_redis do |r|
            map_key  = map_key(lane)
            free_key = free_key(lane)

            raw = r.hgetall(map_key)
            valid = {}
            raw.each do |tenant, part|
              p = part.to_i
              if p >= 0 && p < count
                valid[tenant] = p
              else
                r.hdel(map_key, tenant)
              end
            end

            taken = valid.values.uniq
            all   = (0...count).to_a
            free  = all - taken

            stored_count = r.get(meta_key(lane))&.to_i
            if stored_count != count
              r.del(free_key)
              r.sadd(free_key, free) if free.any?
              r.set(meta_key(lane), count)
            else
              current = r.smembers(free_key).map(&:to_i)
              missing = free - current
              r.sadd(free_key, missing) if missing.any?
              current.each { |p| r.srem(free_key, p) if p < 0 || p >= count }
            end
          end
        rescue StandardError => e
          KafkaBatch.logger.warn(
            "[KafkaBatch::Fairness::TenantPartitions] warm!(#{lane}) failed: #{e.message}"
          )
        end

        def reset!
          @pool  = nil
          @cache = {}
        end

        def all_assigned(type = :time)
          lane = type.to_sym
          with_redis { |r| r.hgetall(map_key(lane)) } || {}
        rescue StandardError
          {}
        end

        private

        def dynamic?
          KafkaBatch.config.fairness_dynamic_tenant_partitions
        end

        def cache_ttl
          KafkaBatch.config.fairness_tenant_partition_cache_ttl.to_i
        end

        def configured_partition(tenant_id, type)
          map = KafkaBatch.config.fairness_tenant_partitions
          return nil if map.nil? || map.empty?

          configured = map[tenant_id]
          return nil if configured.nil?

          n = configured.to_i
          count = KafkaBatch.fairness_ingest_partition_count(type)
          if count && n >= count
            KafkaBatch.logger.warn(
              "[KafkaBatch] fairness_tenant_partitions[#{tenant_id}]=#{n} is out of range " \
              "(topic has #{count} partitions). Ignoring."
            )
            return nil
          end

          n
        end

        def checkout(lane, tenant_id)
          warm!(lane)

          count = KafkaBatch.fairness_ingest_partition_count(lane)
          return nil unless count&.positive?

          result =
            with_redis do |r|
              r.eval(CHECKOUT_LUA, keys: [map_key(lane), free_key(lane)], argv: [tenant_id, count])
            end

          case result.to_i
          when -1
            KafkaBatch.logger.warn(
              "[KafkaBatch::Fairness::TenantPartitions] no free ingest partitions left on " \
              "#{lane} lane (#{count} partitions, all assigned). " \
              "Add partitions to #{KafkaBatch.config.fairness_ingest_topic(lane)} or disable " \
              "fairness_dynamic_tenant_partitions."
            )
            nil
          when -2
            nil
          else
            result.to_i
          end
        rescue StandardError => e
          KafkaBatch.logger.warn(
            "[KafkaBatch::Fairness::TenantPartitions] checkout(#{tenant_id}, #{lane}) failed: #{e.message}"
          )
          nil
        end

        def read_cache(lane, tenant_id)
          ttl = cache_ttl
          return nil if ttl <= 0

          entry = (@cache ||= {})[[lane, tenant_id]]
          return nil unless entry
          return entry[:partition] if monotonic_now - entry[:at] < ttl

          @cache.delete([lane, tenant_id])
          nil
        end

        def write_cache(lane, tenant_id, partition)
          ttl = cache_ttl
          return if ttl <= 0

          (@cache ||= {})[[lane, tenant_id]] = { partition: partition, at: monotonic_now }
        end

        def monotonic_now
          Process.clock_gettime(Process::CLOCK_MONOTONIC)
        end

        def map_key(lane)
          "kafka_batch:tenant_partitions:#{lane}"
        end

        def free_key(lane)
          "#{map_key(lane)}:free"
        end

        def meta_key(lane)
          "#{map_key(lane)}:partition_count"
        end

        def with_redis
          return nil unless KafkaBatch.config.redis_configured?

          pool.with { |r| yield r }
        rescue StandardError => e
          KafkaBatch.logger.warn("[KafkaBatch::Fairness::TenantPartitions] Redis error: #{e.message}")
          nil
        end

        def pool
          cfg = KafkaBatch.config
          @pool ||= ConnectionPool.new(size: cfg.redis_pool_size, timeout: 5) do
            KafkaBatch::RedisClient.new(cfg) ||
              raise(ConfigurationError, "Redis is not configured")
          end
        end
      end
    end
  end
end

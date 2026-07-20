# frozen_string_literal: true

require "connection_pool"
require "oj"
require_relative "../../redis_client"
require_relative "catalog"

module KafkaBatch
  module Ai
    module LiveData
      # Validates tool name + args, then runs a single allowlisted O(1) Redis read.
      # Never issues writes (SET/DEL/HSET/EVAL/…) or O(N) scans (KEYS/SCAN/LRANGE/…).
      class Executor
        Error = Class.new(StandardError)
        Denied = Class.new(Error)
        UnknownTool = Class.new(Error)
        InvalidArgs = Class.new(Error)

        ALLOWED_REDIS = %i[
          get exists ttl pttl
          hget hmget hexists hlen
          llen scard zcard zscore
          sismember
        ].freeze

        MAX_JSON_BYTES = 8_192

        def initialize(redis_pool: nil)
          @redis_pool = redis_pool
        end

        # @return [Hash] ok, tool, label, data|error
        def call(name, arguments = {})
          tool = name.to_s
          raise UnknownTool, "unknown tool: #{tool}" unless Catalog.names.include?(tool)

          args = stringify_keys(arguments)
          data =
            case tool
            when "get_batch" then get_batch(args)
            when "get_batch_index" then get_batch_index(args)
            when "get_counts" then get_counts
            when "get_fairness_snapshot" then get_fairness_snapshot(args)
            when "get_tenant_weight" then get_tenant_weight(args)
            when "get_tenant_ready_depth" then get_tenant_ready_depth(args)
            when "get_schedule_depth" then get_schedule_depth
            when "get_workset_job" then get_workset_job(args)
            when "get_live_consumer" then get_live_consumer(args)
            when "get_reconciler_last" then get_reconciler_last
            when "consumption_paused" then consumption_paused(args)
            else
              raise UnknownTool, "unknown tool: #{tool}"
            end

          {
            "ok" => true,
            "tool" => tool,
            "label" => Catalog.label_for(tool),
            "data" => data
          }
        rescue Denied, UnknownTool, InvalidArgs => e
          {
            "ok" => false,
            "tool" => tool,
            "label" => Catalog.label_for(tool),
            "error" => e.message
          }
        rescue StandardError => e
          KafkaBatch.logger.warn("[KafkaBatch][Ai::LiveData] #{tool}: #{e.class}: #{e.message}")
          {
            "ok" => false,
            "tool" => tool,
            "label" => Catalog.label_for(tool),
            "error" => e.message
          }
        end

        def reset_pool!
          @owned_pool&.shutdown(&:close) rescue nil
          @owned_pool = nil
          @redis_pool = nil
        end

        private

        def get_batch(args)
          id = require_id!(args, "batch_id")
          values = redis_read(:hmget, "kafka_batch:b:#{id}", *Catalog::BATCH_FIELDS)
          return { "found" => false, "batch_id" => id } if values.nil? || values.all?(&:nil?)

          fields = Catalog::BATCH_FIELDS.zip(values).to_h
          return { "found" => false, "batch_id" => id } if fields["id"].to_s.empty? && fields["status"].to_s.empty?

          { "found" => true, "batch" => fields }
        end

        def get_batch_index(args)
          id = require_id!(args, "batch_id")
          {
            "batch_id" => id,
            "running" => redis_read(:zscore, "kafka_batch:index:running", id),
            "done" => redis_read(:zscore, "kafka_batch:index:done", id),
            "cancelled" => redis_read(:zscore, "kafka_batch:index:cancelled", id),
            "all" => redis_read(:zscore, "kafka_batch:index:all", id)
          }
        end

        def get_counts
          values = redis_read(:hmget, "kafka_batch:counts", *Catalog::COUNT_FIELDS) || []
          counts = Catalog::COUNT_FIELDS.zip(values).to_h.transform_values { |v| v.to_i }
          { "counts" => counts, "total" => counts.values.sum }
        end

        def get_fairness_snapshot(args)
          lane = require_lane!(args)
          ns = "kafka_batch:fair_#{lane}"
          {
            "lane" => lane,
            "ring_size" => redis_read(:zcard, "#{ns}:ring").to_i,
            "leases_inflight" => redis_read(:zcard, "#{ns}:leases").to_i,
            "weight_entries" => redis_read(:hlen, "#{ns}:weight").to_i,
            "vtime_entries" => redis_read(:hlen, "#{ns}:vtime").to_i,
            "forwarding_staged" => redis_read(:hlen, "#{ns}:forwarding").to_i
          }
        end

        def get_tenant_weight(args)
          lane = require_lane!(args)
          tenant = require_id!(args, "tenant_id")
          raw = redis_read(:hget, "kafka_batch:fair_#{lane}:weight", tenant)
          {
            "lane" => lane,
            "tenant_id" => tenant,
            "weight" => raw.nil? ? nil : raw.to_f,
            "default_weight" => KafkaBatch.config.fairness_default_weight.to_f
          }
        end

        def get_tenant_ready_depth(args)
          lane = require_lane!(args)
          tenant = require_id!(args, "tenant_id")
          {
            "lane" => lane,
            "tenant_id" => tenant,
            "ready_llen" => redis_read(:llen, "kafka_batch:fair_#{lane}:ready:#{tenant}").to_i
          }
        end

        def get_schedule_depth
          {
            "pending" => redis_read(:zcard, "kafka_batch:sched:pending").to_i,
            "inflight" => redis_read(:zcard, "kafka_batch:sched:inflight").to_i
          }
        end

        def get_workset_job(args)
          job_id = require_id!(args, "job_id")
          key = "kafka_batch:work:job:#{job_id}"
          return { "found" => false, "job_id" => job_id } unless redis_truthy?(redis_read(:exists, key))

          raw = redis_read(:get, key)
          ttl = redis_read(:ttl, key)
          meta = redact_workset(raw)
          { "found" => true, "job_id" => job_id, "ttl_seconds" => ttl, "claim" => meta }
        end

        def get_live_consumer(args)
          consumer_id = require_id!(args, "consumer_id")
          key = "kafka_batch:live:consumer:#{consumer_id}"
          raw = redis_read(:get, key)
          ttl = redis_read(:ttl, key)
          return { "found" => false, "consumer_id" => consumer_id } if raw.nil? || raw.empty?

          {
            "found" => true,
            "consumer_id" => consumer_id,
            "ttl_seconds" => ttl,
            "heartbeat" => truncate_parsed(raw)
          }
        end

        def get_reconciler_last
          raw = redis_read(:get, "kafka_batch:reconciler:last")
          return { "found" => false } if raw.nil? || raw.empty?

          { "found" => true, "summary" => truncate_parsed(raw) }
        end

        def consumption_paused(args)
          group = require_id!(args, "group")
          topic = require_id!(args, "topic")
          member = "#{group}\x1f#{topic}"
          topic_paused = redis_truthy?(redis_read(:sismember, "kafka_batch:consumption:topics", member))

          out = {
            "group" => group,
            "topic" => topic,
            "topic_paused" => topic_paused
          }
          if args.key?("partition") && !args["partition"].nil?
            part = Integer(args["partition"])
            raise InvalidArgs, "partition must be >= 0" if part.negative?

            pmember = "#{group}\x1f#{topic}\x1f#{part}"
            out["partition"] = part
            out["partition_paused"] =
              redis_truthy?(redis_read(:sismember, "kafka_batch:consumption:partitions", pmember))
          end
          out
        end

        def redis_truthy?(value)
          case value
          when true then true
          when false, nil then false
          else value.to_i.positive?
          end
        end

        def require_id!(args, key)
          val = args[key].to_s
          raise InvalidArgs, "#{key} is required" if val.empty?
          raise InvalidArgs, "#{key} has invalid characters" unless Catalog::ID_PATTERN.match?(val)

          val
        end

        def require_lane!(args)
          lane = args["lane"].to_s
          raise InvalidArgs, "lane must be time or throughput" unless Catalog::LANES.include?(lane)

          lane
        end

        def stringify_keys(obj)
          case obj
          when Hash
            obj.each_with_object({}) { |(k, v), h| h[k.to_s] = stringify_keys(v) }
          when Array
            obj.map { |v| stringify_keys(v) }
          else
            obj
          end
        end

        def redact_workset(raw)
          parsed = truncate_parsed(raw)
          return parsed unless parsed.is_a?(Hash)

          parsed = parsed.dup
          %w[payload job message body].each do |k|
            next unless parsed.key?(k)

            parsed[k] = "[redacted]"
          end
          parsed
        end

        def truncate_parsed(raw)
          str = raw.to_s
          if str.bytesize > MAX_JSON_BYTES
            return { "_truncated" => true, "_bytes" => str.bytesize, "preview" => str.byteslice(0, 512) }
          end

          Oj.load(str)
        rescue Oj::ParseError, EncodingError
          str.byteslice(0, 512)
        end

        def redis_read(cmd, *argv)
          raise Denied, "redis command not allowlisted: #{cmd}" unless ALLOWED_REDIS.include?(cmd.to_sym)
          raise Denied, "Redis is not configured" unless KafkaBatch.config.redis_configured?

          pool.with do |r|
            unless r.respond_to?(cmd)
              raise Denied, "redis client cannot #{cmd}"
            end

            r.public_send(cmd, *argv)
          end
        end

        def pool
          @redis_pool || owned_pool
        end

        def owned_pool
          @owned_pool ||= ConnectionPool.new(size: 1, timeout: 3) do
            client = RedisClient.new(KafkaBatch.config)
            raise Denied, "Redis is not configured" unless client

            client
          end
        end
      end
    end
  end
end

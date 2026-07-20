# frozen_string_literal: true

require_relative "live_data/catalog"
require_relative "live_data/executor"

module KafkaBatch
  module Ai
    # Allowlisted O(1) read-only Redis lookups for the dashboard assistant.
    module LiveData
      BATCH_ID_IN_TEXT = /
        \bbatch(?:\s+id)?[:\s#]+([a-zA-Z0-9_.:-]{1,128})\b
        | \b([0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12})\b
      /ix.freeze

      class << self
        def enabled?
          return false unless KafkaBatch.config.ai_knowledge_enabled
          return false unless KafkaBatch.config.ai_live_data_enabled

          KafkaBatch.config.redis_configured?
        end

        # OpenRouter tool schemas — opt-in only (many providers 400 on tools).
        def model_tools_enabled?
          enabled? && KafkaBatch.config.ai_live_data_model_tools
        end

        def suggested_prompts(context: nil)
          return [] unless KafkaBatch.config.ai_knowledge_enabled

          Catalog.suggested_prompts(context: context)
        end

        def open_router_tools
          Catalog.open_router_tools
        end

        def executor
          @executor ||= Executor.new
        end

        def reset!
          @executor&.reset_pool!
          @executor = nil
        end

        # Prefetch obvious lookups from UI context + message text (no LLM needed).
        # @return [Array<Hash>] tool results
        def prefetch(message:, context: nil)
          return [] unless enabled?

          ctx = Catalog.normalize_context(context)
          text = message.to_s
          calls = []

          if (bid = ctx["batch_id"] || detect_batch_id(text))
            calls << ["get_batch", { "batch_id" => bid }]
            calls << ["get_batch_index", { "batch_id" => bid }]
          end
          if (lane = ctx["lane"] || detect_lane(text))
            calls << ["get_fairness_snapshot", { "lane" => lane }] if fairness_intent?(text) || ctx["lane"]
          end
          if (jid = ctx["job_id"])
            calls << ["get_workset_job", { "job_id" => jid }]
          end
          if (tid = ctx["tenant_id"])
            lane = ctx["lane"] || detect_lane(text) || "time"
            calls << ["get_tenant_weight", { "lane" => lane, "tenant_id" => tid }]
            calls << ["get_tenant_ready_depth", { "lane" => lane, "tenant_id" => tid }]
          end
          calls << ["get_counts", {}] if counts_intent?(text)
          calls << ["get_schedule_depth", {}] if schedule_intent?(text)
          calls << ["get_reconciler_last", {}] if reconciler_intent?(text)

          # Chip copy that asks for fairness without an explicit lane
          if fairness_intent?(text) && calls.none? { |n, _| n == "get_fairness_snapshot" }
            calls << ["get_fairness_snapshot", { "lane" => detect_lane(text) || "time" }]
          end

          max = KafkaBatch.config.ai_live_data_max_calls.to_i
          max = 3 if max <= 0
          uniq = []
          seen = {}
          calls.each do |name, args|
            key = [name, args]
            next if seen[key]

            seen[key] = true
            uniq << [name, args]
          end
          uniq.first(max).map { |name, args| executor.call(name, args) }
        end

        def detect_batch_id(message)
          m = message.to_s.match(BATCH_ID_IN_TEXT)
          return nil unless m

          id = m[1] || m[2]
          Catalog::ID_PATTERN.match?(id.to_s) ? id.to_s : nil
        end

        def detect_lane(message)
          t = message.to_s.downcase
          return "throughput" if t.include?("throughput")
          return "time" if t.match?(/\btime\s+lane\b/) || t.include?("fair_time") ||
            t.match?(/\bfairness\b.*\btime\b/) || t.match?(/\btime\b.*\bfairness\b/)

          nil
        end

        def counts_intent?(message)
          t = message.to_s.downcase
          t.match?(/\b(status\s+counts?|batch\s+counts?|how many batches|counts in redis)\b/)
        end

        def schedule_intent?(message)
          t = message.to_s.downcase
          t.match?(/\b(schedule|delayed[- ]job|pending and inflight|sched:pending)\b/)
        end

        def fairness_intent?(message)
          t = message.to_s.downcase
          t.match?(/\b(fairness|fair lane|ready window|leases?|vtime|wfq)\b/)
        end

        def reconciler_intent?(message)
          t = message.to_s.downcase
          t.include?("reconciler")
        end
      end
    end
  end
end

# frozen_string_literal: true

module KafkaBatch
  module Ai
    module LiveData
      # Named, read-only Redis lookups the assistant may request.
      # Each tool owns a fixed key template and an O(1) Redis command — never
      # KEYS/SCAN/LRANGE/ZRANGE/SMEMBERS/HGETALL or any write.
      module Catalog
        ID_PATTERN = /\A[a-zA-Z0-9_.:-]{1,128}\z/.freeze
        LANES = %w[time throughput].freeze

        BATCH_FIELDS = %w[
          id status total_jobs completed_count failed_count touched_count
          description tenant_id created_at finished_at locked_at
          complete_callback_dispatched_at success_callback_dispatched_at
          callback_dispatched_at
        ].freeze

        COUNT_FIELDS = %w[running success complete cancelled pending].freeze

        TOOLS = [
          {
            name: "get_batch",
            label: "batch status",
            description: "Read ledger fields for one batch by id (status, job counts, timestamps).",
            parameters: {
              type: "object",
              properties: {
                batch_id: { type: "string", description: "Batch id" }
              },
              required: ["batch_id"],
              additionalProperties: false
            }
          },
          {
            name: "get_batch_index",
            label: "batch index",
            description: "O(1) membership scores for a batch in running/done/cancelled/all indexes.",
            parameters: {
              type: "object",
              properties: {
                batch_id: { type: "string", description: "Batch id" }
              },
              required: ["batch_id"],
              additionalProperties: false
            }
          },
          {
            name: "get_counts",
            label: "status counts",
            description: "Dashboard batch status counters (running/success/complete/cancelled/pending).",
            parameters: {
              type: "object",
              properties: {},
              additionalProperties: false
            }
          },
          {
            name: "get_fairness_snapshot",
            label: "fairness pressure",
            description: "O(1) sizes for a fairness lane: ring, leases, weights, vtime, forwarding.",
            parameters: {
              type: "object",
              properties: {
                lane: {
                  type: "string",
                  enum: LANES,
                  description: "Fairness lane: time or throughput"
                }
              },
              required: ["lane"],
              additionalProperties: false
            }
          },
          {
            name: "get_tenant_weight",
            label: "tenant weight",
            description: "Read one tenant weight override from a fairness lane.",
            parameters: {
              type: "object",
              properties: {
                lane: { type: "string", enum: LANES },
                tenant_id: { type: "string" }
              },
              required: %w[lane tenant_id],
              additionalProperties: false
            }
          },
          {
            name: "get_tenant_ready_depth",
            label: "tenant ready depth",
            description: "LLEN of one tenant's bounded ready list on a fairness lane.",
            parameters: {
              type: "object",
              properties: {
                lane: { type: "string", enum: LANES },
                tenant_id: { type: "string" }
              },
              required: %w[lane tenant_id],
              additionalProperties: false
            }
          },
          {
            name: "get_schedule_depth",
            label: "schedule depth",
            description: "ZCARD of delayed-job pending and inflight schedule indexes.",
            parameters: {
              type: "object",
              properties: {},
              additionalProperties: false
            }
          },
          {
            name: "get_workset_job",
            label: "workset claim",
            description: "SuperFetch workset claim metadata for one job_id (payload redacted).",
            parameters: {
              type: "object",
              properties: {
                job_id: { type: "string" }
              },
              required: ["job_id"],
              additionalProperties: false
            }
          },
          {
            name: "get_live_consumer",
            label: "live consumer",
            description: "Liveness heartbeat JSON for one consumer id, if present.",
            parameters: {
              type: "object",
              properties: {
                consumer_id: { type: "string" }
              },
              required: ["consumer_id"],
              additionalProperties: false
            }
          },
          {
            name: "get_reconciler_last",
            label: "reconciler summary",
            description: "Last reconciler sweep summary (read-only GET).",
            parameters: {
              type: "object",
              properties: {},
              additionalProperties: false
            }
          },
          {
            name: "consumption_paused",
            label: "consumption pause",
            description: "SISMEMBER check whether a consumer-group topic (and optional partition) is paused.",
            parameters: {
              type: "object",
              properties: {
                group: { type: "string", description: "Consumer group id" },
                topic: { type: "string" },
                partition: {
                  type: "integer",
                  description: "Optional partition number; omit for topic-level pause"
                }
              },
              required: %w[group topic],
              additionalProperties: false
            }
          }
        ].freeze

        class << self
          def names
            @names ||= TOOLS.map { |t| t[:name] }.freeze
          end

          def label_for(name)
            tool = TOOLS.find { |t| t[:name] == name.to_s }
            tool ? tool[:label] : name.to_s
          end

          def open_router_tools
            TOOLS.map do |t|
              {
                "type" => "function",
                "function" => {
                  "name" => t[:name],
                  "description" => t[:description],
                  "parameters" => t[:parameters]
                }
              }
            end
          end

          def suggested_prompts(context: nil)
            prompts = [
              {
                "id" => "docs_superfetch",
                "label" => "What is SuperFetch?",
                "message" => "What is SuperFetch and how do claim window vs concurrency differ?"
              },
              {
                "id" => "counts",
                "label" => "Batch status counts",
                "message" => "What are the current batch status counts in Redis?"
              },
              {
                "id" => "fairness_time",
                "label" => "Fairness pressure (time)",
                "message" => "How much fairness pressure is on the time lane right now?"
              },
              {
                "id" => "schedule",
                "label" => "Delayed job depth",
                "message" => "How deep is the delayed-job schedule (pending and inflight)?"
              },
              {
                "id" => "partitions",
                "label" => "Live partitions",
                "message" => "How many partitions does fair_time ingest have on the live broker?"
              }
            ]

            ctx = normalize_context(context)
            if (bid = ctx["batch_id"])
              prompts.unshift(
                "id" => "batch_context",
                "label" => "Inspect this batch",
                "message" => "What is the status of batch #{bid}?",
                "context" => { "batch_id" => bid }
              )
            end
            if (lane = ctx["lane"])
              prompts.unshift(
                "id" => "fairness_context",
                "label" => "Fairness on this lane",
                "message" => "How much fairness pressure is on the #{lane} lane right now?",
                "context" => { "lane" => lane }
              )
            end
            prompts
          end

          def normalize_context(context)
            return {} if context.nil?

            h =
              case context
              when Hash then context
              else {}
              end
            out = {}
            bid = h["batch_id"] || h[:batch_id]
            out["batch_id"] = bid.to_s if bid && ID_PATTERN.match?(bid.to_s)
            lane = (h["lane"] || h[:lane]).to_s
            out["lane"] = lane if LANES.include?(lane)
            tid = h["tenant_id"] || h[:tenant_id]
            out["tenant_id"] = tid.to_s if tid && ID_PATTERN.match?(tid.to_s)
            jid = h["job_id"] || h[:job_id]
            out["job_id"] = jid.to_s if jid && ID_PATTERN.match?(jid.to_s)
            out
          end
        end
      end
    end
  end
end

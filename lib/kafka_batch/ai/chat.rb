# frozen_string_literal: true

require "oj"
require_relative "settings"
require_relative "chat_history"
require_relative "retriever"
require_relative "open_router"
require_relative "knowledge_index"
require_relative "live_data"

module KafkaBatch
  module Ai
    # RAG chat over the packaged knowledge corpus + live config snapshot.
    # Optional allowlisted O(1) Redis reads via LiveData (never writes).
    module Chat
      SYSTEM_PROMPT = <<~TXT.freeze
        You are the kafka-batch admin dashboard assistant.
        Answer using the provided knowledge context about kafka-batch (Ruby + Go).
        The Live configuration snapshot (source: config) is AUTHORITATIVE for THIS cluster.
        For partition counts: use live_broker_partitions / broker_partitions from that
        snapshot only. create_default_partitions / configured_partitions and any docs
        mentioning DEFAULT_PARTITIONS (e.g. 768) are create_topics defaults — not live
        cluster size. If live_broker_partitions is n/a or topic_inventory_available is
        false, say broker metadata is unavailable; do not invent a count from docs.
        For handlers, job_types, topics, runtimes, fairness lanes, and priority queues:
        use the AUTHORITATIVE LIVE ROUTING section (from kafka_batch_handlers.yml and
        config/kafka_batch priority YAML). Prefer those over generic docs examples.
        When LIVE REDIS LOOKUPS are provided, treat them as AUTHORITATIVE for that
        specific key. Do not invent Redis metrics.
        If the context is insufficient for other questions, say you do not know from the
        docs — do not invent Redis keys or live metrics.
        Prefer concise, operator-focused answers. Mention relevant config knobs when useful.
        When citing, refer to section titles from the context.
      TXT

      class << self
        # @param message [String]
        # @param context [Hash, nil] optional UI context (batch_id, lane, …)
        # @return [Hash] ok, reply, citations, live_lookups, history_size
        def ask(message, context: nil)
          message = message.to_s.strip
          raise ArgumentError, "message is blank" if message.empty?

          api_key = Settings.api_key
          raise ArgumentError, "OpenRouter API key is not configured (AI Settings)" if api_key.nil? || api_key.empty?

          unless KnowledgeIndex.meta["corpus_version"]
            KnowledgeIndex.sync!
          end

          contexts = Retriever.search(message)
          citations = contexts.map do |c|
            { "id" => c["id"], "title" => c["title"], "source" => c["source"], "section" => c["section"] }
          end

          context_block =
            if contexts.empty?
              "(No matching knowledge chunks found. Answer only if the question is trivial; otherwise say you need docs.)"
            else
              contexts.map.with_index(1) do |c, i|
                "### Context #{i}: #{c['title']} (#{c['source']})\n#{c['text']}"
              end.join("\n\n")
            end

          live_lookups = []
          if LiveData.enabled?
            live_lookups.concat(LiveData.prefetch(message: message, context: context))
          end

          live_block =
            if live_lookups.empty?
              nil
            else
              "LIVE REDIS LOOKUPS (authoritative, read-only):\n#{Oj.dump(live_lookups)}"
            end

          recent = ChatHistory.list(limit: 12).reverse
          history_msgs = recent.map { |m| { "role" => m["role"], "content" => m["content"].to_s[0, 2000] } }

          messages = [
            { "role" => "system", "content" => SYSTEM_PROMPT },
            { "role" => "system", "content" => "Knowledge context:\n\n#{context_block}" }
          ]
          messages << { "role" => "system", "content" => live_block } if live_block
          messages.concat(history_msgs)
          messages << { "role" => "user", "content" => message }

          client = OpenRouter.new(
            api_key: api_key,
            model: Settings.model,
            base_url: Settings.base_url
          )

          # Default: no OpenRouter tools (prefetch only). Opt-in model tools often
          # cause provider HTTP 400 on OpenRouter.
          tools = LiveData.model_tools_enabled? ? LiveData.open_router_tools : nil
          reply = run_completion(client, messages, tools, live_lookups)

          ChatHistory.append!(role: "user", content: message)
          ChatHistory.append!(
            role: "assistant",
            content: reply,
            citations: citations,
            meta: live_lookups.empty? ? nil : { "live_lookups" => summarize_lookups(live_lookups) }
          )

          {
            "ok" => true,
            "reply" => reply,
            "citations" => citations,
            "live_lookups" => summarize_lookups(live_lookups),
            "live_data_enabled" => LiveData.enabled?,
            "model" => Settings.model,
            "history_size" => ChatHistory.size
          }
        end

        private

        def run_completion(client, messages, tools, live_lookups)
          return complete_once(client, messages, nil) if tools.nil? || tools.empty?

          begin
            run_with_tools(client, messages, tools, live_lookups)
          rescue OpenRouter::Error => e
            # Providers often reject tool schemas with opaque "Provider returned error".
            if e.message.match?(/HTTP 400/)
              KafkaBatch.logger.warn(
                "[KafkaBatch][Ai::Chat] OpenRouter rejected tools (#{e.message}); retrying without tools"
              )
              complete_once(client, messages, nil)
            else
              raise
            end
          end
        end

        def complete_once(client, messages, tools)
          result = client.chat(messages: messages, tools: tools)
          text = result["content"].to_s.strip
          return text unless text.empty?

          "I could not form an answer from the available context."
        end

        def run_with_tools(client, messages, tools, live_lookups)
          max = KafkaBatch.config.ai_live_data_max_calls.to_i
          max = 3 if max <= 0
          budget = [max - live_lookups.size, 0].max

          loop do
            result = client.chat(messages: messages, tools: tools)
            tool_calls = result["tool_calls"]
            content = result["content"]

            if tool_calls.nil? || tool_calls.empty? || budget <= 0
              text = content.to_s.strip
              return text unless text.empty?
              return "I looked up live data but could not form an answer. Try asking about a specific batch id or fairness lane."
            end

            messages << {
              "role" => "assistant",
              "content" => content,
              "tool_calls" => tool_calls
            }

            tool_calls.each do |tc|
              break if budget <= 0

              fn = tc["function"] || {}
              name = fn["name"].to_s
              raw_args = fn["arguments"]
              args =
                case raw_args
                when Hash then raw_args
                when String then (Oj.load(raw_args) rescue {})
                else {}
                end
              args = {} unless args.is_a?(Hash)

              lookup = LiveData.executor.call(name, args)
              live_lookups << lookup
              budget -= 1

              messages << {
                "role" => "tool",
                "tool_call_id" => tc["id"].to_s,
                "name" => name,
                "content" => Oj.dump(lookup)
              }
            end

            tools = nil if budget <= 0
          end
        end

        def summarize_lookups(lookups)
          lookups.map do |l|
            {
              "tool" => l["tool"],
              "label" => l["label"] || LiveData::Catalog.label_for(l["tool"]),
              "ok" => l["ok"] != false
            }
          end
        end
      end
    end
  end
end

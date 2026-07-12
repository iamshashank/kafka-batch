# frozen_string_literal: true

require "oj"

module KafkaBatch
  module Callbacks
    # Dispatches batch callbacks when a batch finalizes.
    #
    # Job callbacks (Sidekiq-style): enqueue a normal job to a user topic — Go or
    # Ruby runtime depending on handler manifest / worker registration.
    #
    # Legacy callbacks: produce to callbacks_topic for CallbackConsumer (Ruby class).
    module Dispatcher
      class << self
        # @return [Symbol] :none, :job_only, :legacy_only, or :mixed
        def dispatch!(batch:, outcome:, finished_at: nil)
          summary = batch_summary(batch, outcome, finished_at)
          legacy_needed = false
          job_produced  = false

          if outcome == "success" && present?(batch[:on_success])
            case Callback.parse(batch[:on_success])
            when Callback::Job
              produce_job_callback!(batch[:on_success], summary, kind: :on_success)
              job_produced = true
            when Callback::Legacy
              legacy_needed = true
            end
          end

          if present?(batch[:on_complete])
            case Callback.parse(batch[:on_complete])
            when Callback::Job
              produce_job_callback!(batch[:on_complete], summary, kind: :on_complete)
              job_produced = true
            when Callback::Legacy
              legacy_needed = true
            end
          end

          if legacy_needed
            produce_legacy!(batch, outcome, summary)
            return :mixed if job_produced

            return :legacy_only
          end

          if job_produced
            KafkaBatch.store.claim_callback(batch[:id], KafkaBatch.node_id)
            return :job_only
          end

          :none
        end

        def any_legacy?(batch)
          [batch[:on_success], batch[:on_complete]].compact.any? do |raw|
            spec = Callback.parse(raw)
            spec.is_a?(Callback::Legacy)
          end
        end

        private

        def batch_summary(batch, outcome, finished_at)
          summary = {
            "batch_id"        => batch[:id],
            "outcome"         => outcome,
            "total_jobs"      => batch[:total_jobs],
            "completed_count" => batch[:completed_count],
            "failed_count"    => batch[:failed_count],
            "callback_args"   => batch[:callback_args] || {},
            "finished_at"     => finished_at || batch[:finished_at] || Time.now.utc.iso8601,
            "description"     => batch[:description],
            "tenant_id"       => batch[:tenant_id]
          }
          summary["reconciled"] = batch[:reconciled] if batch.key?(:reconciled)
          summary.compact
        end

        def produce_job_callback!(raw_spec, summary, kind:)
          spec = Callback.parse(raw_spec)
          raise ArgumentError, "expected job callback" unless spec.is_a?(Callback::Job)

          definition = KafkaBatch::Batch.resolve_definition!(spec.job_type)
          batch_id     = summary["batch_id"]
          job_id       = "#{batch_id}:#{kind}"

          payload = summary.merge("callback_kind" => kind.to_s)
          message = KafkaBatch::Batch.build_message_for(
            definition: definition,
            payload:    payload,
            job_id:     job_id,
            batch_id:   nil,
            attempt:    0
          )

          route = route_for(spec, definition, job_id: job_id, batch_id: batch_id)
          KafkaBatch::Producer.produce_sync(
            topic:     route[:topic],
            payload:   message,
            key:       route[:key],
            partition: route[:partition]
          )

          KafkaBatch::Instrumentation.callback_invoked(
            batch_id:        batch_id,
            callback_class:  spec.job_type,
            callback_method: kind.to_s
          )
        end

        def route_for(spec, definition, job_id:, batch_id:)
          if spec.topic && !spec.topic.empty?
            topic = topic_includes_prefix?(spec.topic) ? spec.topic : KafkaBatch.config.resolve_topic(spec.topic)
            { topic: topic, key: job_id, partition: nil }
          elsif definition.fairness?
            raise ConfigurationError,
                  "callback job_type=#{spec.job_type.inspect} uses fairness — set an explicit topic"
          else
            KafkaBatch::Batch.route_for_definition(definition, job_id: job_id, batch_id: batch_id)
          end
        end

        def topic_includes_prefix?(topic)
          prefix = KafkaBatch.config.topic_prefix.to_s.strip
          return true if prefix.empty?

          topic.start_with?("#{prefix}.")
        end

        def produce_legacy!(batch, outcome, summary)
          payload = summary.merge(
            "on_success"  => batch[:on_success],
            "on_complete" => batch[:on_complete]
          )
          KafkaBatch::Producer.produce_sync(
            topic:   KafkaBatch.config.callbacks_topic,
            payload: payload,
            key:     batch[:id]
          )
        end

        def present?(value)
          !value.nil? && !value.to_s.strip.empty?
        end
      end
    end
  end
end

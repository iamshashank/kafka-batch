require "karafka"
require "oj"
require "time"

module KafkaBatch
  module Consumers
    # Karafka consumer that processes individual batch jobs.
    #
    # One instance of this consumer is routed to every worker topic registered
    # via KafkaBatch::Worker.  The routing is wired up by KafkaBatch.draw_routes.
    #
    # Message payload schema (JSON):
    #   {
    #     "job_id"        => "uuid",
    #     "batch_id"      => "uuid | null",
    #     "worker_class"  => "FullyQualifiedWorkerClassName",
    #     "payload"       => { ... },
    #     "attempt"       => 0,
    #     "max_retries"   => 3,
    #     "retry_backoff" => 5,
    #     "enqueued_at"   => "2024-01-01T00:00:00Z"
    #   }
    class JobConsumer < Karafka::BaseConsumer
      # Karafka calls this method once per polled batch.
      def consume
        messages.each do |message|
          process_message(message)
        end
      end

      private

      def process_message(message)
        data = decode(message.raw_payload)

        job_id       = data["job_id"]
        batch_id     = data["batch_id"]
        worker_class = resolve_worker(data["worker_class"])
        payload      = data["payload"] || {}
        attempt      = data["attempt"].to_i
        max_retries  = data.fetch("max_retries",  KafkaBatch.config.max_retries).to_i
        backoff      = data.fetch("retry_backoff", KafkaBatch.config.retry_backoff).to_i

        KafkaBatch.logger.debug(
          "[KafkaBatch][JobConsumer] #{worker_class}#perform job_id=#{job_id} " \
          "batch_id=#{batch_id} attempt=#{attempt}"
        )

        worker_class.new.perform(payload)

        # ── Success ────────────────────────────────────────────────────────
        emit_event(batch_id: batch_id, job_id: job_id, status: "success", worker_class: worker_class)
        mark_as_consumed!(message)

      rescue StandardError => e
        handle_failure(
          message:      message,
          data:         data,
          error:        e,
          job_id:       job_id,
          batch_id:     batch_id,
          worker_class: worker_class,
          attempt:      attempt,
          max_retries:  max_retries,
          backoff:      backoff
        )
      end

      def handle_failure(message:, data:, error:, job_id:, batch_id:,
                         worker_class:, attempt:, max_retries:, backoff:)
        KafkaBatch.logger.error(
          "[KafkaBatch][JobConsumer] job_id=#{job_id} attempt=#{attempt} " \
          "error=#{error.class}: #{error.message}"
        )

        if attempt < max_retries
          next_attempt = attempt + 1
          sleep_for    = backoff * next_attempt  # linear backoff

          KafkaBatch.logger.info(
            "[KafkaBatch][JobConsumer] Retrying job_id=#{job_id} " \
            "attempt=#{next_attempt} after #{sleep_for}s"
          )

          sleep(sleep_for)

          KafkaBatch::Batch.reenqueue(
            topic:        message.topic,
            message:      data,
            next_attempt: next_attempt
          )
        else
          # Exhausted – record failure and optionally send to DLT
          KafkaBatch.logger.error(
            "[KafkaBatch][JobConsumer] job_id=#{job_id} exhausted #{max_retries} retries – failing"
          )

          emit_event(batch_id: batch_id, job_id: job_id, status: "failed", worker_class: worker_class)

          publish_to_dlt(data: data, error: error, topic: message.topic)
        end

        # Acknowledge regardless of outcome – we've handled the message
        # (either re-enqueued it or moved it to the DLT).
        mark_as_consumed!(message)
      end

      def emit_event(batch_id:, job_id:, status:, worker_class:)
        return unless batch_id  # standalone job – no batch to update

        KafkaBatch::Producer.produce_sync(
          topic:   KafkaBatch.config.events_topic,
          payload: {
            "batch_id"     => batch_id,
            "job_id"       => job_id,
            "status"       => status,
            "worker_class" => worker_class.to_s,
            "occurred_at"  => Time.now.iso8601
          },
          key: batch_id
        )
      end

      def publish_to_dlt(data:, error:, topic:)
        KafkaBatch::Producer.produce_sync(
          topic:   KafkaBatch.config.dead_letter_topic,
          payload: data.merge(
            "dlt_source_topic" => topic,
            "dlt_error_class"  => error.class.name,
            "dlt_error_message"=> error.message,
            "dlt_at"           => Time.now.iso8601
          ),
          key: data["job_id"]
        )
      rescue KafkaBatch::ProducerError => e
        # DLT failure must not mask the original error
        KafkaBatch.logger.error("[KafkaBatch][JobConsumer] DLT publish failed: #{e.message}")
      end

      def resolve_worker(class_name)
        klass = Object.const_get(class_name)
        raise ArgumentError, "#{class_name} does not include KafkaBatch::Worker" \
          unless klass.include?(KafkaBatch::Worker)
        klass
      rescue NameError
        raise ArgumentError, "Unknown worker class: #{class_name}"
      end

      def decode(raw)
        Oj.load(raw)
      rescue Oj::ParseError => e
        raise ArgumentError, "Invalid JSON payload: #{e.message}"
      end
    end
  end
end

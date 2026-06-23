require "karafka"
require "oj"
require "time"

module KafkaBatch
  module Consumers
    # Karafka consumer that fires on_success / on_complete callbacks.
    #
    # Reads from KafkaBatch.config.callbacks_topic.
    #
    # Message payload schema (produced by EventConsumer):
    #   {
    #     "batch_id"        => "uuid",
    #     "outcome"         => "success" | "complete",
    #     "total_jobs"      => 100,
    #     "completed_count" => 98,
    #     "failed_count"    => 2,
    #     "on_success"      => "MySuccessWorker",    # may be null
    #     "on_complete"     => "MyCompleteWorker",   # may be null
    #     "meta"            => { ... },
    #     "finished_at"     => "ISO8601"
    #   }
    #
    # Callback worker interface:
    #   Any plain Ruby class that responds to #on_success(batch_summary) or
    #   #on_complete(batch_summary).  The batch_summary is the full decoded hash.
    #
    #   Example:
    #
    #     class MySuccessWorker
    #       def on_success(batch)
    #         NotifySlack.call("Batch #{batch['batch_id']} finished!")
    #       end
    #     end
    #
    #     class MyCompleteWorker
    #       def on_complete(batch)
    #         CleanupTempFiles.call(batch['meta']['temp_dir'])
    #       end
    #     end
    class CallbackConsumer < Karafka::BaseConsumer
      def consume
        messages.each { |msg| process_callback(msg) }
      end

      private

      def process_callback(message)
        data    = decode(message.raw_payload)
        outcome = data["outcome"]

        KafkaBatch.logger.info(
          "[KafkaBatch][CallbackConsumer] batch_id=#{data['batch_id']} outcome=#{outcome}"
        )

        # on_success fires only when every job succeeded
        if outcome == "success" && data["on_success"].present_str?
          invoke_callback(data["on_success"], :on_success, data)
        end

        # on_complete fires for every terminal state (success or partial failure)
        if data["on_complete"].present_str?
          invoke_callback(data["on_complete"], :on_complete, data)
        end

        mark_as_consumed!(message)
      end

      def invoke_callback(class_name, method_name, batch_summary)
        klass = Object.const_get(class_name)
        instance = klass.new

        unless instance.respond_to?(method_name)
          KafkaBatch.logger.error(
            "[KafkaBatch][CallbackConsumer] #{class_name} does not respond to ##{method_name}"
          )
          return
        end

        KafkaBatch.logger.debug(
          "[KafkaBatch][CallbackConsumer] Calling #{class_name}##{method_name}"
        )
        instance.public_send(method_name, batch_summary)

      rescue NameError => e
        KafkaBatch.logger.error(
          "[KafkaBatch][CallbackConsumer] Cannot resolve callback class '#{class_name}': #{e.message}"
        )
      rescue StandardError => e
        # Log but don't re-raise – a callback failure should not block the consumer.
        # The message is still committed.  If you need retries on callbacks, wire up
        # the callback class itself as a KafkaBatch::Worker.
        KafkaBatch.logger.error(
          "[KafkaBatch][CallbackConsumer] #{class_name}##{method_name} raised " \
          "#{e.class}: #{e.message}\n#{e.backtrace&.first(5)&.join("\n")}"
        )
      end

      def decode(raw)
        Oj.load(raw)
      rescue Oj::ParseError => e
        raise ArgumentError, "Invalid JSON in callback message: #{e.message}"
      end
    end
  end
end

# Minimal helper to avoid ActiveSupport dependency for blank? checks
class String
  def present_str?
    !nil? && !empty?
  end
end

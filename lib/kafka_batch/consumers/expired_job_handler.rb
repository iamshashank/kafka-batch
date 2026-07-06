# frozen_string_literal: true

module KafkaBatch
  module Consumers
    # Karafka-consumer mixin: expire jobs and commit the source offset.
    module ExpiredJobHandler
      private

      def expired_job?(data)
        KafkaBatch::JobExpiry.expired?(data)
      end

      def handle_expired_job(message:, data:, log_tag: self.class.name.split("::").last)
        topic, partition, offset = KafkaBatch::JobExpiry.source_coords(data, message: message)
        KafkaBatch::JobExpiry.drop!(
          data: data, topic: topic, partition: partition, offset: offset, log_tag: log_tag
        )
        mark_as_consumed!(message)
      end
    end
  end
end

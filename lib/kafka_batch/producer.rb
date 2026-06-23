require "waterdrop"
require "oj"

module KafkaBatch
  # Thread-safe, singleton WaterDrop producer.
  # Wraps WaterDrop::Producer with a synchronous produce helper and
  # auto-initialisation from KafkaBatch.config.
  module Producer
    class << self
      # @return [WaterDrop::Producer]
      def instance
        @instance || @mutex.synchronize { @instance ||= build }
      end

      # Produce a single message synchronously.
      # Blocks until the broker acknowledges delivery (acks: all).
      #
      # @param topic   [String]
      # @param payload [Hash, String]
      # @param key     [String, nil]   optional partition key
      # @param headers [Hash]          optional Kafka headers
      def produce_sync(topic:, payload:, key: nil, headers: {})
        instance.produce_sync(
          topic:   topic,
          payload: encode(payload),
          key:     key&.to_s,
          headers: headers
        )
      rescue WaterDrop::Errors::ProducerNotStartedError,
             WaterDrop::Errors::MessageInvalidError,
             Rdkafka::RdkafkaError => e
        raise KafkaBatch::ProducerError, "Kafka produce failed: #{e.message}"
      end

      # Close and reset the producer (e.g. in tests or after fork).
      def reset!
        @mutex.synchronize do
          @instance&.close
          @instance = nil
        end
      end

      private

      def build
        cfg = KafkaBatch.config

        WaterDrop::Producer.new do |config|
          config.deliver = true
          config.kafka   = {
            "bootstrap.servers":         cfg.brokers.join(","),
            "request.required.acks":     "all",      # strongest durability guarantee
            "message.send.max.retries":  3,
            "retry.backoff.ms":          200,
            "socket.timeout.ms":         30_000,
            "message.timeout.ms":        30_000
          }.merge(cfg.producer_config)

          config.logger = cfg.logger
        end
      end

      def encode(payload)
        payload.is_a?(String) ? payload : Oj.dump(payload, mode: :compat)
      end
    end

    @mutex = Mutex.new
  end
end

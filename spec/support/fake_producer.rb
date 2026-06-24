# Captures messages that would have been produced to Kafka so specs can assert
# on them without a live broker.
module FakeProducer
  Produced = Struct.new(:topic, :payload, :key, :headers, keyword_init: true)

  class << self
    def reset!
      @messages = []
      @raise_on = nil
    end

    def record(topic:, payload:, key: nil, headers: {})
      raise KafkaBatch::ProducerError, "boom (#{topic})" if @raise_on && @raise_on.call(topic)

      messages << Produced.new(topic: topic, payload: payload, key: key, headers: headers)
      true
    end

    def messages
      @messages ||= []
    end

    def for_topic(topic)
      messages.select { |m| m.topic == topic }
    end

    # Make produce_sync raise a ProducerError for topics matching the block.
    def raise_for(&predicate)
      @raise_on = predicate
    end
  end
end

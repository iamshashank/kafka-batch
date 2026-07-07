# frozen_string_literal: true

module KafkaBatch
  module Consumers
    # Parameterized job consumer for a topic inside a priority group.
    # Rank 0 runs unconditionally; lower ranks yield when higher topics have lag.
    class PriorityJobConsumer < JobConsumer
      include PriorityGate

      class << self
        # @return [Hash] :rank, :mode, :higher_topics, :consumer_group, :topic,
        #   :weighted_interleave
        attr_accessor :priority_spec

        def build(spec)
          klass = Class.new(self)
          klass.priority_spec = spec.freeze
          klass
        end
      end

      def consume
        spec = self.class.priority_spec
        rank = spec[:rank].to_i

        # Per-message yield checks so weighted interleave is per job, not per
        # poll batch (Karafka may deliver many messages per consume call).
        messages.each do |message|
          if rank.positive? && should_yield_to_higher?(spec)
            yield_for_priority(spec)
            return
          end
          process_message(message)
        end
      end

      private

      def should_yield_to_higher?(spec)
        higher = spec[:higher_topics]
        return false if higher.nil? || higher.empty?

        group = spec[:consumer_group]
        return false unless higher_topics_have_lag?(higher, group)

        case spec[:mode]
        when :strict
          true
        when :weighted
          every = spec[:weighted_interleave].to_i
          every = 4 if every < 1
          @priority_weighted_tick = (@priority_weighted_tick || 0) + 1
          (@priority_weighted_tick % every) != 0
        else
          true
        end
      end

      def yield_for_priority(spec)
        pause_ms = (KafkaBatch.config.priority_lag_check_interval * 1_000).to_i
        higher   = spec[:higher_topics]

        KafkaBatch.logger.debug(
          "[KafkaBatch][PriorityGate] #{self.class.name} rank #{spec[:rank]} " \
          "higher topics have lag (#{higher.join(', ')}) – pausing #{pause_ms}ms " \
          "(mode=#{spec[:mode]})"
        )
        KafkaBatch::Instrumentation.consumer_priority_yielded(
          consumer_class: self.class.name,
          p0_topic:       higher.first,
          consumer_group: spec[:consumer_group],
          pause_ms:       pause_ms,
          mode:           spec[:mode].to_s,
          rank:           spec[:rank],
          higher_topics:  higher
        )
        # Karafka::BaseConsumer#pause(offset, timeout_ms) — NOT pause(timeout_ms).
        seek = messages.empty? ? :consecutive : messages.first.offset
        pause(seek, pause_ms)
      end
    end
  end
end

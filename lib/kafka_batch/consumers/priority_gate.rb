module KafkaBatch
  module Consumers
    # Mixin for lower-ranked priority consumers.  Lag checks call the Karafka
    # Admin API at most once per +priority_lag_check_interval+ seconds per
    # consumer instance.  On error the check FAILS OPEN.
    module PriorityGate
      # Returns true when any of the given higher-ranked topics have lag in the
      # consumer group.  Result is cached for priority_lag_check_interval
      # seconds (monotonic clock, per instance) unless +force+ is true.
      #
      # @param higher_topics [Array<String>]
      # @param consumer_group [String]
      # @param force [Boolean] bypass cache (used after a strict-mode pause)
      # @return [Boolean]
      def higher_topics_have_lag?(higher_topics, consumer_group, force: false)
        topics = Array(higher_topics).map(&:to_s).reject(&:empty?)
        return false if topics.empty?

        now      = Process.clock_gettime(Process::CLOCK_MONOTONIC)
        interval = KafkaBatch.config.priority_lag_check_interval.to_f

        unless force
          if @priority_last_check && (now - @priority_last_check) < interval
            return @priority_last_result
          end
        end

        @priority_higher_topics    = topics
        @priority_consumer_group   = consumer_group

        @priority_last_check  = now
        @priority_last_result =
          begin
            data = KafkaBatch::Lag.read_group(consumer_group, topics)
            topics.any? do |topic|
              partitions = (data[consumer_group] || {})[topic] || {}
              partitions.values.any? { |info| info[:lag].to_i.positive? }
            end
          rescue StandardError => e
            KafkaBatch.logger.debug(
              "[KafkaBatch][PriorityGate] lag check for #{topics.join(', ')} failed – " \
              "failing open: #{e.message}"
            )
            false
          end
      end
    end
  end
end

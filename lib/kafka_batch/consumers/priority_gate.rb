module KafkaBatch
  module Consumers
    # Mixin for lower-ranked priority consumers.  Lag checks call the Karafka
    # Admin API at most once per +priority_lag_check_interval+ seconds per
    # consumer instance.  On error the last successful result is retained so a
    # transient Admin failure does not silently disable prioritization.
    module PriorityGate
      # Returns true when any of the given higher-ranked topics have lag in the
      # consumer group.  Result is cached for priority_lag_check_interval
      # seconds (monotonic clock, per instance) unless +force+ is true.
      #
      # Topics paused at topic level via /lag are excluded — a paused p0 must not
      # block p1 (the operator intentionally stopped draining the higher rank).
      #
      # @param higher_topics [Array<String>]
      # @param consumer_group [String]
      # @param force [Boolean] bypass cache (used after a strict-mode pause)
      # @return [Boolean]
      def higher_topics_have_lag?(higher_topics, consumer_group, force: false)
        topics = active_higher_topics(higher_topics, consumer_group)
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
            KafkaBatch.logger.warn(
              "[KafkaBatch][PriorityGate] lag check for #{topics.join(', ')} failed – " \
              "using last result: #{e.message}"
            )
            @priority_last_result.nil? ? false : @priority_last_result
          end
      end

      private

      def active_higher_topics(higher_topics, consumer_group)
        Array(higher_topics).map(&:to_s).reject(&:empty?).reject do |topic|
          KafkaBatch::ConsumptionControl.topic_level_paused?(
            group: consumer_group, topic: topic
          )
        end
      end
    end
  end
end

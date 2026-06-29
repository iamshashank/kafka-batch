module KafkaBatch
  module Consumers
    # Processes fast-tier normal (p1) jobs with a weighted priority gate.
    #
    # When fast-p0 has pending lag this consumer pauses for
    # +priority_lag_check_interval+ seconds so FastP0Consumer gets CPU time.
    # Because fast jobs are short-running the pause is small and p1 latency
    # stays bounded.  Under zero p0 load this consumer runs at full throughput.
    class FastP1Consumer < JobConsumer
      include PriorityGate

      def consume
        if p0_has_lag?(KafkaBatch.config.fast_p0_topic, KafkaBatch.fast_consumer_group)
          yield_to_p0
          return
        end
        super
      end
    end
  end
end

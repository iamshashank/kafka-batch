module KafkaBatch
  module Consumers
    # Processes slow-tier normal (p1) jobs with a strict priority gate.
    #
    # This consumer pauses for +priority_lag_check_interval+ seconds whenever
    # slow-p0 has any pending lag, ensuring no NEW p1 jobs are started while
    # p0 work exists.  In-flight p1 jobs are not preempted — strict priority
    # applies to job selection, not execution.  Under zero p0 load this
    # consumer runs at full throughput.
    class SlowP1Consumer < JobConsumer
      include PriorityGate

      def consume
        if p0_has_lag?(KafkaBatch.config.slow_p0_topic, KafkaBatch.slow_consumer_group)
          yield_to_p0
          return
        end
        super
      end
    end
  end
end

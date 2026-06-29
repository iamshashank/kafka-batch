module KafkaBatch
  module Consumers
    # Processes slow-tier critical (p0) jobs.
    #
    # No priority gate — this consumer always runs unconditionally.
    # It shares the kafka-batch-jobs-slow consumer group with SlowP1Consumer.
    class SlowP0Consumer < JobConsumer
    end
  end
end

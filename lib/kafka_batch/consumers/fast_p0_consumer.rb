module KafkaBatch
  module Consumers
    # Processes fast-tier critical (p0) jobs.
    #
    # No priority gate — this consumer always runs unconditionally.
    # It shares the kafka-batch-jobs-fast consumer group with FastP1Consumer;
    # scale up this group's concurrency to handle p0 throughput peaks.
    class FastP0Consumer < JobConsumer
    end
  end
end

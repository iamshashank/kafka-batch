module KafkaBatchSpec
  module WorkerRuns
    class << self
      def reset!
        @runs = []
      end

      def record(name, payload)
        runs << { name: name, payload: payload }
      end

      def runs
        @runs ||= []
      end
    end
  end
end

class SuccessfulWorker
  include KafkaBatch::Worker
  kafka_topic "test.success"

  def perform(payload)
    KafkaBatchSpec::WorkerRuns.record(:success, payload)
  end
end

class FailingWorker
  include KafkaBatch::Worker
  kafka_topic "test.fail"
  max_retries 2
  retry_backoff 7

  def perform(_payload)
    raise "always fails"
  end
end

# A plain class that is NOT a KafkaBatch::Worker (for negative validation).
class NotAWorker
end

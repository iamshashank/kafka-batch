module KafkaBatch
  class Error < StandardError; end

  # Raised when the gem is misconfigured
  class ConfigurationError < Error; end

  # Raised when a batch cannot be found in the store
  class BatchNotFoundError < Error; end

  # Raised when Kafka message production fails
  class ProducerError < Error; end

  # Raised on store read/write failures
  class StoreError < Error; end

  # Raised when a job exhausts all retry attempts
  class JobExhaustedError < Error
    attr_reader :job_id, :batch_id, :worker_class, :payload, :cause

    def initialize(msg = nil, job_id:, batch_id:, worker_class:, payload:, cause: nil)
      @job_id       = job_id
      @batch_id     = batch_id
      @worker_class = worker_class
      @payload      = payload
      @cause        = cause
      super(msg || "Job #{job_id} exhausted retries (#{worker_class})")
    end
  end
end

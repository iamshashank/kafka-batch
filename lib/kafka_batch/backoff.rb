module KafkaBatch
  # Fixed retry schedule (Kafka-friendly – short, bounded delays):
  #   first retry  -> first_delay   (default 10s)
  #   later retries -> interval      (default 180s / 3 min)
  # plus optional +/- jitter to avoid synchronized retry storms.
  #
  # Short delays keep the RetryConsumer's pause() head-of-line wait negligible
  # (<= interval), so no scheduler/re-queue machinery is needed.
  module Backoff
    module_function

    # @param next_attempt [Integer] 1-based index of the upcoming retry
    # @param first_delay  [Numeric] seconds before the 1st retry
    # @param interval     [Numeric] seconds before every subsequent retry
    # @param jitter       [Float]   fraction (0..1) of +/- randomization
    # @return [Float] delay in seconds
    def delay(next_attempt:, first_delay:, interval:, jitter: 0.0)
      base = (next_attempt.to_i <= 1 ? first_delay : interval).to_f
      j    = jitter.to_f
      return base if j <= 0

      base * (1 + ((rand * 2) - 1) * j)  # base * (1 ± jitter)
    end
  end
end

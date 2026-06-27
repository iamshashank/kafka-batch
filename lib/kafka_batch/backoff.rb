module KafkaBatch
  # Exponential (geometric) retry backoff.
  #
  # Delays grow geometrically from +base+ (the first retry) up to +cap+ (the
  # LAST retry), so the final retry lands at the cap (default 24h). The ratio is
  # derived from the worker's max_retries so the schedule always ends at the cap
  # regardless of how many retries are configured.
  #
  #   base=5s, max_retries=3, cap=24h  =>  ~5s, ~11m, 24h
  module Backoff
    module_function

    # @param next_attempt [Integer] 1-based index of the upcoming retry (1..max_retries)
    # @param max_retries  [Integer] total retries configured for the worker
    # @param base         [Numeric] first-retry delay in seconds
    # @param cap          [Numeric] last-retry delay in seconds (the maximum)
    # @return [Float] delay in seconds before this retry
    def delay(next_attempt:, max_retries:, base:, cap:)
      base = base.to_f
      cap  = cap.to_f
      base = 1.0 if base <= 0
      return cap if cap <= base          # misconfig / degenerate – just use the cap
      return cap if max_retries.to_i <= 1

      ratio = (cap / base)**(1.0 / (max_retries - 1))
      delay = base * (ratio**(next_attempt - 1))
      [delay, cap].min
    end
  end
end

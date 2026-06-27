require "set"

module KafkaBatch
  # Process-local cache of cancelled batch ids.
  #
  # Instead of reading the store on every job to check for cancellation, the
  # JobConsumer asks this cache, which refreshes the full set of cancelled batch
  # ids at most once per KafkaBatch.config.cancellation_cache_ttl seconds. This
  # turns "one store read per job" into "one store read per process per window".
  #
  # Consequence: cancellation is eventually-consistent – jobs already queued may
  # still run until the next refresh. That is an accepted trade-off for throughput.
  module CancellationCache
    @mutex      = Mutex.new
    @ids        = nil   # Set of cancelled batch ids, or nil before first load
    @fetched_at = nil   # monotonic seconds of last successful refresh

    class << self
      # @return [Boolean] whether the batch is known-cancelled as of the last refresh
      def cancelled?(batch_id)
        return false if batch_id.nil?
        ids = current_ids
        ids.include?(batch_id)
      end

      # Drop the cache (tests / after fork).
      def reset!
        @mutex.synchronize do
          @ids        = nil
          @fetched_at = nil
        end
      end

      private

      def current_ids
        # Fast path: fresh enough, no lock needed.
        cached = @ids
        return cached if cached && fresh?(@fetched_at)

        @mutex.synchronize do
          # Re-check under the lock in case another thread just refreshed.
          return @ids if @ids && fresh?(@fetched_at)

          @ids        = fetch_ids
          @fetched_at = now
          @ids
        end
      end

      def fetch_ids
        Set.new(KafkaBatch.store.cancelled_batch_ids)
      rescue StandardError => e
        KafkaBatch.logger.warn(
          "[KafkaBatch][CancellationCache] refresh failed: #{e.message} – keeping previous set"
        )
        @ids || Set.new
      end

      def fresh?(stamp)
        stamp && (now - stamp) < KafkaBatch.config.cancellation_cache_ttl
      end

      def now
        Process.clock_gettime(Process::CLOCK_MONOTONIC)
      end
    end
  end
end

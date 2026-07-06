module KafkaBatch
  module Reconciler
    # Tracks per-run outcomes for the dashboard summary.
    class Collector
      MAX_DETAILS = RunSummary::MAX_DETAILS

      attr_reader :triggered_by

      def initialize(triggered_by:)
        @triggered_by = triggered_by.to_s
        @found_stale  = 0
        @found_lost   = 0
        @stale        = []
        @lost         = []
        @details      = []
        @counts       = {
          recovered_stale: 0,
          refired_lost:    0,
          skipped_stale:   0,
          produce_failed:  0
        }
      end

      def identify(stale_all_size, stale, lost_all_size, lost)
        @found_stale = stale_all_size
        @found_lost  = lost_all_size
        @stale       = stale
        @lost        = lost
        @capped_stale = stale_all_size > stale.size
        @capped_lost  = lost_all_size > lost.size
      end

      def record_stale(batch_id, outcome, batch: nil)
        case outcome
        when :recovered_running, :recovered_empty
          @counts[:recovered_stale] += 1
        when :skipped_open, :skipped_in_progress
          @counts[:skipped_stale] += 1
        when :produce_failed
          @counts[:produce_failed] += 1
        end
        add_detail(batch_id, outcome, batch)
      end

      def record_lost(batch_id, outcome, batch: nil)
        case outcome
        when :refired_lost
          @counts[:refired_lost] += 1
        when :produce_failed
          @counts[:produce_failed] += 1
        end
        add_detail(batch_id, outcome, batch)
      end

      def finish(duration)
        {
          ran_at:           Time.now.utc.iso8601(3),
          triggered_by:     @triggered_by,
          duration:         duration.round(3),
          found_stale:      @found_stale,
          processed_stale:  @stale.size,
          found_lost:       @found_lost,
          processed_lost:   @lost.size,
          capped_stale:     @capped_stale ? "1" : "0",
          capped_lost:      @capped_lost ? "1" : "0",
          recovered_stale:  @counts[:recovered_stale],
          refired_lost:     @counts[:refired_lost],
          skipped_stale:    @counts[:skipped_stale],
          produce_failed:   @counts[:produce_failed],
          details:          @details
        }
      end

      private

      def add_detail(batch_id, action, batch)
        return if @details.size >= MAX_DETAILS

        row = {
          batch_id: batch_id.to_s,
          action:   action.to_s
        }
        if batch
          row[:outcome]      = (batch[:status] || batch["outcome"]).to_s
          row[:total_jobs]   = batch[:total_jobs]
          row[:failed_count] = batch[:failed_count]
        end
        @details << row
      end
    end
  end
end

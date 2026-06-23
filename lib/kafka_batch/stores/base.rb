module KafkaBatch
  module Stores
    # Abstract interface every store must implement.
    # All methods that mutate state must be safe to call concurrently from
    # multiple processes / threads.
    class Base
      # Create a new batch record.
      #
      # @param id         [String]  UUID for the batch
      # @param total_jobs [Integer] number of jobs that will be produced
      # @param on_success [String, nil] worker class name called when ALL jobs succeed
      # @param on_complete [String, nil] worker class name called when ALL jobs finish (any status)
      # @param meta       [Hash]   arbitrary user data
      # @return [void]
      def create_batch(id:, total_jobs:, on_success: nil, on_complete: nil, meta: {})
        raise NotImplementedError, "#{self.class}#create_batch"
      end

      # Fetch a batch by id.
      # @return [Hash, nil] with keys :id, :total_jobs, :completed_count,
      #                      :failed_count, :status, :on_success, :on_complete,
      #                      :meta, :created_at, :finished_at
      def find_batch(id)
        raise NotImplementedError, "#{self.class}#find_batch"
      end

      # Atomically record that a single job finished.
      # Must be idempotent – duplicate calls for the same job_id are no-ops.
      #
      # @param batch_id [String]
      # @param job_id   [String] unique per-job UUID (used for dedup)
      # @param status   [String] "success" | "failed"
      # @return [Hash]
      #   { status: :done,      outcome: "success"|"complete", batch: <batch_hash> }  – batch just finished
      #   { status: :continue                                                       }  – still more jobs outstanding
      #   { status: :duplicate                                                      }  – already recorded, skip
      #   { status: :not_found                                                      }  – batch unknown
      def record_job_completion(batch_id:, job_id:, status:)
        raise NotImplementedError, "#{self.class}#record_job_completion"
      end

      # Update the batch's top-level status field.
      # @param id     [String]
      # @param status [String] e.g. "cancelled", "reconciled"
      # @return [void]
      def update_batch_status(id, status)
        raise NotImplementedError, "#{self.class}#update_batch_status"
      end

      # Return batches in "running" state that were created before +older_than+.
      # Used by the reconciler to detect stuck batches.
      # @param older_than [Time]
      # @return [Array<Hash>]
      def stale_batches(older_than:)
        raise NotImplementedError, "#{self.class}#stale_batches"
      end
    end
  end
end

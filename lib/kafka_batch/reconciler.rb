module KafkaBatch
  # Periodic sweep to detect and recover stuck batches.
  #
  # A batch is "stuck" when all jobs have been processed but a completion
  # event was lost (e.g. producer crash before the event was flushed).
  # The reconciler re-checks counter state and re-triggers callbacks.
  #
  # Run via Rake task:
  #   bundle exec rake kafka_batch:reconcile
  #
  # Or schedule it with a cron / Sidekiq-Scheduler / Karafka scheduled job.
  module Reconciler
    # @param older_than [Integer] seconds – only inspect batches older than this
    def self.run(older_than: KafkaBatch.config.reconciliation_interval)
      threshold = Time.now - older_than
      batches   = KafkaBatch.store.stale_batches(older_than: threshold)

      KafkaBatch.logger.info(
        "[KafkaBatch][Reconciler] Found #{batches.size} stale batch(es)"
      )

      batches.each { |b| reconcile_batch(b) }
    end

    def self.reconcile_batch(batch)
      id      = batch[:id]
      total   = batch[:total_jobs].to_i
      done    = batch[:completed_count].to_i + batch[:failed_count].to_i

      KafkaBatch.logger.info(
        "[KafkaBatch][Reconciler] batch_id=#{id} total=#{total} done=#{done}"
      )

      return unless done >= total && total.positive?

      KafkaBatch.logger.warn(
        "[KafkaBatch][Reconciler] batch_id=#{id} appears complete but status=#{batch[:status]} – re-triggering"
      )

      outcome = batch[:failed_count].to_i.positive? ? "complete" : "success"
      KafkaBatch.store.update_batch_status(id, outcome)

      KafkaBatch::Producer.produce_sync(
        topic:   KafkaBatch.config.callbacks_topic,
        payload: batch.merge("outcome" => outcome, "reconciled" => true),
        key:     id
      )
    end
  end
end

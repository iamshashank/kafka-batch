require_relative "reconciler/run_summary"
require_relative "reconciler/collector"

module KafkaBatch
  # Periodic sweep that detects and recovers two categories of stuck batches:
  #
  #  1. Stuck-running: status="running" but all jobs are done
  #     Cause: EventConsumer never incremented the counter to completion
  #     (e.g. event messages were lost before the consumer started).
  #
  #  2. Lost-callback: status="success"|"complete" but callback was never
  #     dispatched (callback_dispatched_at IS NULL).
  #     Cause: EventConsumer crashed after updating the store record but before
  #     successfully producing the message to the callbacks topic.
  #
  # Run via Rake:
  #   bundle exec rake kafka_batch:reconcile
  #
  # Or schedule with cron / Whenever / a Karafka scheduled consumer.
  module Reconciler
    # @param older_than [Integer] seconds – only inspect batches older than this
    # @param triggered_by [Symbol] :rake | :consumer
    # @return [Symbol] :completed | :lock_skipped
    def self.run(older_than: KafkaBatch.config.reconciliation_interval, triggered_by: :rake)
      start_time = Time.now
      collector  = Collector.new(triggered_by: triggered_by)

      lock_ok = KafkaBatch.store.with_reconciler_lock(ttl: KafkaBatch.config.reconciler_lock_ttl) do
        threshold = Time.now - older_than
        max       = [KafkaBatch.config.max_reconcile_per_run.to_i, 1].max

        stale_all = KafkaBatch.store.stale_batches(older_than: threshold)
        if stale_all.size > max
          KafkaBatch.logger.warn(
            "[KafkaBatch][Reconciler] #{stale_all.size} stuck-running batches found; " \
            "processing first #{max} this run (config.max_reconcile_per_run=#{max})"
          )
        end
        stale = stale_all.first(max)
        KafkaBatch.logger.info(
          "[KafkaBatch][Reconciler] Found #{stale_all.size} stuck-running batch(es), processing #{stale.size}"
        )

        lost_all = KafkaBatch.store.done_batches_without_callback(older_than: threshold)
        if lost_all.size > max
          KafkaBatch.logger.warn(
            "[KafkaBatch][Reconciler] #{lost_all.size} lost-callback batches found; " \
            "processing first #{max} this run (config.max_reconcile_per_run=#{max})"
          )
        end
        lost = lost_all.first(max)
        KafkaBatch.logger.info(
          "[KafkaBatch][Reconciler] Found #{lost_all.size} lost-callback batch(es), processing #{lost.size}"
        )

        collector.identify(stale_all.size, stale, lost_all.size, lost)

        stale.each do |batch|
          outcome = reconcile_running(batch)
          collector.record_stale(batch[:id], outcome, batch: batch)
        end
        lost.each do |batch|
          outcome = refire_callback(batch)
          collector.record_lost(batch[:id], outcome, batch: batch)
        end

        KafkaBatch.store.reconcile_batch_counts! if KafkaBatch.store.respond_to?(:reconcile_batch_counts!)
        KafkaBatch.store.purge_stale_failures! if KafkaBatch.store.respond_to?(:purge_stale_failures!)

        true
      end

      unless lock_ok
        RunSummary.save_skip!
        return :lock_skipped
      end

      duration = Time.now - start_time
      summary  = collector.finish(duration)
      RunSummary.save_last!(summary)

      KafkaBatch::Instrumentation.reconciler_ran(
        stale_count:  summary[:recovered_stale],
        lost_count:   summary[:refired_lost],
        duration:     duration,
        triggered_by: triggered_by
      )
      KafkaBatch.logger.info("[KafkaBatch][Reconciler] Done in #{duration.round(2)}s")
      :completed
    end

    # @return [Symbol]
    def self.reconcile_running(batch)
      id    = batch[:id]
      total = batch[:total_jobs].to_i
      done  = batch[:completed_count].to_i + batch[:failed_count].to_i

      KafkaBatch.logger.info(
        "[KafkaBatch][Reconciler] stuck-running batch_id=#{id} " \
        "total=#{total} done=#{done}"
      )

      fresh = KafkaBatch.store.find_batch(id)
      unless fresh
        KafkaBatch.logger.info("[KafkaBatch][Reconciler] batch_id=#{id} no longer exists – skipping")
        return :skipped_gone
      end
      unless fresh[:status] == "running"
        KafkaBatch.logger.info(
          "[KafkaBatch][Reconciler] batch_id=#{id} is #{fresh[:status]} – skipping"
        )
        return :skipped_not_running
      end
      batch = fresh

      if batch[:locked_at].nil?
        KafkaBatch.logger.info(
          "[KafkaBatch][Reconciler] batch_id=#{id} is still open (unlocked) – skipping"
        )
        return :skipped_open
      end

      if total == 0
        KafkaBatch.logger.info(
          "[KafkaBatch][Reconciler] batch_id=#{id} is a sealed empty batch – completing as success"
        )
        unless KafkaBatch.store.mark_finished(id, "success")
          return :skipped_not_running
        end
        return :produce_failed unless produce_callback(batch.merge("outcome" => "success"))

        return :recovered_empty
      end

      unless done >= total
        KafkaBatch.logger.warn(
          "[KafkaBatch][Reconciler] batch_id=#{id} genuinely still running – skipping"
        )
        return :skipped_in_progress
      end

      outcome = batch[:failed_count].to_i.positive? ? "complete" : "success"
      unless KafkaBatch.store.mark_finished(id, outcome)
        return :skipped_not_running
      end

      KafkaBatch.logger.warn(
        "[KafkaBatch][Reconciler] batch_id=#{id} transitioned to #{outcome} – producing callback"
      )

      return :produce_failed unless produce_callback(batch.merge("outcome" => outcome))

      :recovered_running
    end

    # @return [Symbol]
    def self.refire_callback(batch)
      id = batch[:id]
      fresh = KafkaBatch.store.find_batch(id)
      unless fresh && %w[success complete].include?(fresh[:status])
        KafkaBatch.logger.info(
          "[KafkaBatch][Reconciler] batch_id=#{id} no longer needs callback – skipping"
        )
        return :skipped_not_done
      end

      KafkaBatch.logger.warn(
        "[KafkaBatch][Reconciler] lost-callback batch_id=#{id} " \
        "status=#{fresh[:status]} – re-producing callback message"
      )

      ok = produce_callback(fresh.merge(
        "outcome"    => fresh[:status],
        "reconciled" => true
      ))
      ok ? :refired_lost : :produce_failed
    end

    # @return [Boolean]
    def self.produce_callback(batch)
      KafkaBatch::Producer.produce_sync(
        topic:   KafkaBatch.config.callbacks_topic,
        payload: {
          "batch_id"        => batch[:id],
          "outcome"         => batch["outcome"] || batch[:status],
          "total_jobs"      => batch[:total_jobs],
          "completed_count" => batch[:completed_count],
          "failed_count"    => batch[:failed_count],
          "on_success"      => batch[:on_success],
          "on_complete"     => batch[:on_complete],
          "meta"            => batch[:meta],
          "finished_at"     => batch[:finished_at],
          "reconciled"      => batch["reconciled"] || false
        },
        key: batch[:id]
      )
      true
    rescue KafkaBatch::ProducerError => e
      KafkaBatch.logger.error(
        "[KafkaBatch][Reconciler] Failed to produce callback for " \
        "batch_id=#{batch[:id]}: #{e.message}"
      )
      false
    end
  end
end

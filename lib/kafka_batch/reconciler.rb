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
    #
    # #10 fix: the distributed lock is held ONLY while identifying which batches
    # need work and sweeping heartbeats (fast store reads + one DELETE).  All
    # produce_sync calls (slow Kafka broker round-trips) happen OUTSIDE the lock
    # so we never hold the distributed lock across network I/O.
    # The store's own claim_callback guard (HSETNX / conditional UPDATE) prevents
    # double-dispatch even if two processes reach the produce phase concurrently.
    def self.run(older_than: KafkaBatch.config.reconciliation_interval, triggered_by: :rake)
      start_time = Time.now
      stale      = nil
      lost       = nil

      KafkaBatch.store.with_reconciler_lock(ttl: KafkaBatch.config.reconciler_lock_ttl) do
        threshold = Time.now - older_than
        # Fix #20: cap per-run work so an incident spike (thousands of batches
        # going stale at once) doesn't hold the distributed lock for minutes or
        # produce a callback burst. Remaining batches are picked up next tick.
        max = [KafkaBatch.config.max_reconcile_per_run.to_i, 1].max

        # ── 1. Identify stuck-running batches ─────────────────────────────────
        stale_all = KafkaBatch.store.stale_batches(older_than: threshold)
        if stale_all.size > max
          KafkaBatch.logger.warn(
            "[KafkaBatch][Reconciler] #{stale_all.size} stuck-running batches found; " \
            "processing first #{max} this run (config.max_reconcile_per_run=#{max})"
          )
        end
        stale = stale_all.first(max)
        KafkaBatch.logger.info("[KafkaBatch][Reconciler] Found #{stale_all.size} stuck-running batch(es), processing #{stale.size}")

        # ── 2. Identify done batches with lost callbacks ───────────────────────
        lost_all = KafkaBatch.store.done_batches_without_callback(older_than: threshold)
        if lost_all.size > max
          KafkaBatch.logger.warn(
            "[KafkaBatch][Reconciler] #{lost_all.size} lost-callback batches found; " \
            "processing first #{max} this run (config.max_reconcile_per_run=#{max})"
          )
        end
        lost = lost_all.first(max)
        KafkaBatch.logger.info("[KafkaBatch][Reconciler] Found #{lost_all.size} lost-callback batch(es), processing #{lost.size}")

        # ── 3. Sweep stale consumer heartbeats (:store liveness backend) ─────
        if KafkaBatch.config.liveness_backend == :store
          begin
            KafkaBatch.store.sweep_stale_heartbeats(Time.now - KafkaBatch.config.liveness_ttl)
          rescue => e
            KafkaBatch.logger.warn("[KafkaBatch][Reconciler] heartbeat sweep failed: #{e.message}")
          end
        end
      end

      # Lock was not acquired (another process holds it) – nothing to do.
      return unless stale

      # ── Produce callbacks OUTSIDE the lock ─────────────────────────────────
      # mark_finished + produce_callback run here; both are idempotent and safe
      # to call from multiple processes thanks to the claim_callback HSETNX guard.
      stale.each { |b| reconcile_running(b) }
      lost.each  { |b| refire_callback(b)   }

      duration = Time.now - start_time
      KafkaBatch::Instrumentation.reconciler_ran(
        stale_count:  stale.size,
        lost_count:   lost.size,
        duration:     duration,
        triggered_by: triggered_by
      )
      KafkaBatch.logger.info("[KafkaBatch][Reconciler] Done in #{duration.round(2)}s")
    end

    # Re-evaluates a batch that's been stuck in "running" too long.
    # If counter arithmetic shows it's actually done, transitions it and fires
    # the callback.  Otherwise logs and moves on.
    def self.reconcile_running(batch)
      id    = batch[:id]
      total = batch[:total_jobs].to_i
      done  = batch[:completed_count].to_i + batch[:failed_count].to_i

      KafkaBatch.logger.info(
        "[KafkaBatch][Reconciler] stuck-running batch_id=#{id} " \
        "total=#{total} done=#{done}"
      )

      # Open (never-locked) batches may still receive more jobs – they are not
      # stuck, just not finalized yet. Leave them alone.
      if batch[:locked_at].nil?
        KafkaBatch.logger.info(
          "[KafkaBatch][Reconciler] batch_id=#{id} is still open (unlocked) – skipping"
        )
        return
      end

      # Bug #12 fix: a sealed empty batch (total_jobs == 0, locked_at present)
      # is degenerate-complete. The guard `done >= total && total.positive?` was
      # always false for these, leaving them stuck in "running" forever.
      if total == 0
        KafkaBatch.logger.info(
          "[KafkaBatch][Reconciler] batch_id=#{id} is a sealed empty batch – completing as success"
        )
        KafkaBatch.store.mark_finished(id, "success")
        produce_callback(batch.merge("outcome" => "success"))
        return
      end

      unless done >= total
        KafkaBatch.logger.warn(
          "[KafkaBatch][Reconciler] batch_id=#{id} genuinely still running – skipping"
        )
        return
      end

      outcome = batch[:failed_count].to_i.positive? ? "complete" : "success"
      # mark_finished stamps finished_at and registers the batch for
      # lost-callback recovery, so even if the callback we produce below is
      # also lost, a later sweep can still re-fire it.
      KafkaBatch.store.mark_finished(id, outcome)

      KafkaBatch.logger.warn(
        "[KafkaBatch][Reconciler] batch_id=#{id} transitioned to #{outcome} – producing callback"
      )

      produce_callback(batch.merge("outcome" => outcome))
    end

    # Re-produces the callback message for a done batch whose callback was never
    # dispatched.  The CallbackConsumer's atomic claim_callback guard ensures the
    # callback itself fires at most once even if this runs multiple times.
    def self.refire_callback(batch)
      KafkaBatch.logger.warn(
        "[KafkaBatch][Reconciler] lost-callback batch_id=#{batch[:id]} " \
        "status=#{batch[:status]} – re-producing callback message"
      )

      produce_callback(batch.merge(
        "outcome"    => batch[:status],
        "reconciled" => true
      ))
    end

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
    rescue KafkaBatch::ProducerError => e
      KafkaBatch.logger.error(
        "[KafkaBatch][Reconciler] Failed to produce callback for " \
        "batch_id=#{batch[:id]}: #{e.message}"
      )
    end
  end
end

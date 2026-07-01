require "oj"

module KafkaBatch
  module Fairness
    # Per-process background thread that turns config.fairness_mode into real,
    # active fairness. It repeatedly pulls the fairest next job from the Redis
    # WFQ Scheduler (#checkout) and forwards it to the Kafka ready topic, where
    # the JobConsumer swarm executes it.
    #
    # This is the engine half of the default fairness path:
    #
    #   ingest topic → Dispatcher (Scheduler#enqueue → bounded Redis window)
    #                → Forwarder  (Scheduler#checkout → ready topic)   ← this file
    #                → ready topic → JobConsumer → perform → Scheduler#complete
    #
    # Fairness modes (Scheduler decides ordering; the Forwarder just drains it):
    #   :job_count_fairness – #checkout advances a tenant's vtime by 1/weight, so
    #                         forwards are shared by weighted job count.
    #   :time_fairness      – #checkout only reserves an in-flight slot; vtime is
    #                         advanced by duration/weight when the JobConsumer
    #                         calls #complete after perform. Forwards are shared by
    #                         weighted wall-clock time.
    #
    # Concurrency control (both modes) comes from the Scheduler:
    #   * fairness_global_concurrency      – total forwarded-but-not-completed jobs
    #                                        (bounds ready-topic depth; keeps
    #                                        fairness dynamic).
    #   * fairness_max_inflight_per_tenant – per-tenant slice of that window
    #                                        (enforces interleaving in time mode).
    #
    # Safe to run in MANY processes at once: #checkout is a single atomic Redis
    # Lua call, so concurrent forwarders share one WFQ ring and never double-pick
    # a job. The Dispatcher lazily starts one forwarder per process that is
    # assigned ingest partitions.
    class Forwarder
      DEFAULT_IDLE_SLEEP = 0.05  # seconds
      DEFAULT_BURST      = 50    # max forwards per loop iteration before yielding

      @mutex   = Mutex.new
      @running = false
      @thread  = nil

      class << self
        attr_reader :thread

        # Start the singleton forwarder thread for this process (idempotent).
        def ensure_running!
          @mutex.synchronize do
            return if @running && @thread&.alive?
            @running = true
            @thread  = Thread.new { new.run }
            @thread.name = "kafka-batch-fairness-forwarder" if @thread.respond_to?(:name=)
          end
        end

        # Signal the forwarder loop to stop and (optionally) join it.
        def stop!(timeout: 5)
          t = nil
          @mutex.synchronize do
            @running = false
            t = @thread
            @thread = nil
          end
          t&.join(timeout)
          nil
        end

        def running?
          @running && @thread&.alive?
        end

        # Test/reset seam: forget any thread reference without joining.
        def reset!
          @mutex.synchronize do
            @running = false
            @thread  = nil
          end
        end
      end

      def running?
        self.class.running?
      end

      # Main loop: forward a burst of fairly-selected jobs, then sleep briefly if
      # nothing was ready (or the global in-flight window is full).
      def run
        KafkaBatch.logger.info(
          "[KafkaBatch][Fairness::Forwarder] started (mode=#{KafkaBatch.config.fairness_mode})"
        )
        idle  = idle_sleep
        burst = DEFAULT_BURST

        while running?
          begin
            forwarded = 0
            forwarded += 1 while forwarded < burst && running? && forward_once
            sleep(idle) if forwarded.zero?
          rescue StandardError => e
            KafkaBatch.logger.error(
              "[KafkaBatch][Fairness::Forwarder] loop error: #{e.class}: #{e.message}"
            )
            sleep(idle)
          end
        end

        KafkaBatch.logger.info("[KafkaBatch][Fairness::Forwarder] stopped")
      end

      # Check out one fairly-selected job and forward it to the ready topic.
      # @return [Boolean] true if a job was forwarded; false when nothing is
      #   ready or the global in-flight window is full.
      def forward_once
        sched = KafkaBatch.scheduler
        return false unless sched

        job = sched.checkout
        return false unless job

        payload = mark_slot(job[:payload], job[:tenant_id])
        KafkaBatch::Producer.produce_sync(
          topic:   KafkaBatch.config.fairness_ready_topic,
          payload: payload,
          key:     job_key(payload)
        )
        true
      end

      private

      def idle_sleep
        v = KafkaBatch.config.fairness_forwarder_idle_sleep.to_f
        v.positive? ? v : DEFAULT_IDLE_SLEEP
      end

      # Stamp the fair-slot marker + tenant_id into the raw job JSON so the
      # JobConsumer knows this ready message holds one Scheduler in-flight slot
      # and must release it (Scheduler#complete) exactly once when done.
      def mark_slot(raw, tenant_id)
        data = Oj.load(raw)
        data["_fair_slot"] = true
        data["tenant_id"] ||= tenant_id
        Oj.dump(data, mode: :compat)
      rescue StandardError
        raw
      end

      # Spread across ready-topic partitions by job_id.
      def job_key(raw)
        Oj.load(raw)["job_id"]
      rescue StandardError
        nil
      end
    end
  end
end

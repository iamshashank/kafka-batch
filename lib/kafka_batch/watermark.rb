# frozen_string_literal: true

require "set"

module KafkaBatch
  # Redis-free alternative to SuperFetch for Karafka job consumers.
  #
  # Where SuperFetch claims Redis ownership and marks the Kafka offset *before*
  # #perform, watermark runs jobs concurrently out of order and commits only the
  # contiguous completed-offset prefix per partition (the "watermark"). On crash
  # or rebalance, everything after the last committed watermark is redelivered
  # and re-run — so handlers MUST be idempotent and per-topic job runtimes should
  # be similar (a slow job holds the watermark; every faster job that finished
  # behind it re-runs on crash). See KafkaBatch::Configuration#execution_mode and
  # the README "Execution mode" section. Go parity: kafka-batch-go
  # pkg/daemon/watermark.go.
  #
  # Two limits (same knobs as SuperFetch):
  #   super_fetch_claim_window — max dispatched-but-not-yet-committed per process
  #   super_fetch_concurrency  — max concurrent #perform
  #
  # Threading contract: mark_as_consumed is called ONLY from the listener thread
  # (dispatch / flush), never from a #perform pool thread — the same thread that
  # SuperFetch marks on. Pool threads only record completions into an in-memory
  # per-partition tracker.
  module Watermark
    class Executor
      PartState = Struct.new(:expected, :inited, :done) # done: {offset => message}

      def initialize
        @mutex        = Mutex.new
        @parts        = {}   # [topic, partition] => PartState
        @accepting    = true
        @window       = nil  # SizedQueue
        @perform_sem  = nil  # SizedQueue
        @in_flight    = 0
      end

      # Listener-thread entry for a poll batch: flush prior completions, dispatch
      # each message to the pool, then flush again.
      def dispatch(consumer, messages)
        flush(consumer)
        messages.each { |message| dispatch_one(consumer, message) }
        flush(consumer)
      end

      # Dispatch one message: block on the window (backpressure), register it in
      # the partition tracker, and run #perform on a pool thread. Does NOT mark.
      def dispatch_one(consumer, message)
        return unless accepting?

        acquire_window!
        register(message)
        @mutex.synchronize { @in_flight += 1 }
        Thread.new do
          Thread.current.name = "kafka-batch-watermark" if Thread.current.respond_to?(:name=)
          perform(consumer, message)
        end
      end

      # Advance every partition's contiguous prefix and mark those offsets for
      # commit. Must run on the listener (consumer) thread. Called at the start and
      # end of each dispatch so completions commit as soon as the prefix reaches
      # them, without marking from pool threads.
      def flush(consumer)
        ready = []
        @mutex.synchronize do
          @parts.each do |_key, st|
            next unless st.inited

            while (msg = st.done[st.expected])
              st.done.delete(st.expected)
              ready << msg
              st.expected += 1
            end
          end
        end
        return if ready.empty?

        ready.each do |msg|
          begin
            consumer.mark_as_consumed(msg)
          rescue StandardError => e
            KafkaBatch.logger.warn("[KafkaBatch][Watermark] mark_as_consumed failed offset=#{msg.offset}: #{e.message}")
          ensure
            release_window!
          end
        end
      end

      # Stop accepting new dispatches and wait for in-flight #perform to finish.
      # Any completion not yet flushed re-runs on restart (documented at-least-once).
      # @return [Integer] remaining in-flight (0 = drained cleanly)
      def drain(timeout: 30)
        deadline = monotonic_now + timeout.to_f
        @mutex.synchronize { @accepting = false }
        loop do
          done = @mutex.synchronize { @in_flight.zero? }
          break if done
          break if monotonic_now >= deadline

          sleep 0.05
        end
        remaining = @mutex.synchronize { @in_flight }
        if remaining.positive?
          KafkaBatch.logger.warn(
            "[KafkaBatch][Watermark] drain timed out with #{remaining} in-flight job(s) — they re-run on restart"
          )
        end
        remaining
      end

      def accepting?
        @mutex.synchronize { @accepting }
      end

      # Jobs dispatched but not yet finalized (committed or failed). For drain and tests.
      def in_flight_count
        @mutex.synchronize { @in_flight }
      end

      # Record a successful terminal outcome (called from the pipeline via
      # JobConsumer#commit_offset! on the pool thread). The offset becomes
      # committable once it is the head of the contiguous prefix; its window slot
      # is released when flush marks it. Idempotent per #perform via a thread-local
      # guard so a raise after commit_offset! cannot double-finalize.
      def note_done(message)
        return if Thread.current[:kafka_batch_wm_finalized]

        Thread.current[:kafka_batch_wm_finalized] = true
        key = part_key(message)
        @mutex.synchronize do
          st = @parts[key]
          st.done[message.offset] = message if st && (!st.inited || message.offset >= st.expected)
        end
      end

      def reset!
        drain(timeout: 5)
        @mutex.synchronize do
          @parts.clear
          @accepting   = true
          @window      = nil
          @perform_sem = nil
          @in_flight   = 0
        end
      end

      private

      # Infra failure (Process/produce error surfaced as a raise). The offset is
      # not committed and blocks its partition's watermark until redelivery on the
      # next rebalance/restart. Release its own window slot so the single failed
      # message does not permanently pin a slot; completed-but-blocked messages
      # behind it keep theirs, so the process backpressures instead of growing the
      # pending map without bound.
      def note_failed(message)
        release_window!
      end

      def perform(consumer, message)
        acquire_perform_slot!
        Thread.current[:kafka_batch_wm]           = self
        Thread.current[:kafka_batch_wm_finalized] = false
        begin
          consumer.send(:process_message, message)
          # process_message funnels every terminal outcome through
          # JobConsumer#commit_offset! → note_done. A normal return with no
          # note_done (should not happen) is treated as done to avoid a stuck slot.
          note_done(message) unless Thread.current[:kafka_batch_wm_finalized]
        rescue StandardError => e
          KafkaBatch.logger.error(
            "[KafkaBatch][Watermark] perform error offset=#{message.offset} topic=#{message.topic}: " \
            "#{e.class}: #{e.message} — not committing (redelivers on restart)"
          )
          note_failed(message) unless Thread.current[:kafka_batch_wm_finalized]
        ensure
          Thread.current[:kafka_batch_wm]           = nil
          Thread.current[:kafka_batch_wm_finalized] = nil
          @mutex.synchronize { @in_flight -= 1 if @in_flight.positive? }
          release_perform_slot!
        end
      end

      # Seed / reset the partition tracker. A record below the current watermark
      # (redelivery after rebalance) resets expected so the prefix re-forms.
      def register(message)
        key = part_key(message)
        @mutex.synchronize do
          st = (@parts[key] ||= PartState.new(0, false, {}))
          if !st.inited || message.offset < st.expected
            st.expected = message.offset
            st.inited   = true
            st.done.delete_if { |off, _| off < message.offset }
          end
        end
      end

      def part_key(message)
        [message.topic, message.partition]
      end

      def concurrency
        n = KafkaBatch.config.super_fetch_concurrency.to_i
        n.positive? ? n : 1
      end

      def window_size
        n = KafkaBatch.config.super_fetch_claim_window.to_i
        return n if n >= concurrency

        concurrency * 2
      end

      def acquire_window!
        window_queue.pop
      end

      def release_window!
        window_queue << true
      end

      def acquire_perform_slot!
        perform_sem_queue.pop
      end

      def release_perform_slot!
        perform_sem_queue << true
      end

      def window_queue
        @mutex.synchronize do
          return @window if @window

          n = window_size
          @window = SizedQueue.new(n)
          n.times { @window << true }
          @window
        end
      end

      def perform_sem_queue
        @mutex.synchronize do
          return @perform_sem if @perform_sem

          n = concurrency
          @perform_sem = SizedQueue.new(n)
          n.times { @perform_sem << true }
          @perform_sem
        end
      end

      def monotonic_now
        Process.clock_gettime(Process::CLOCK_MONOTONIC)
      end
    end

    class << self
      def executor
        @mutex ||= Mutex.new
        @mutex.synchronize { @executor ||= Executor.new }
      end

      def reset!
        @mutex ||= Mutex.new
        @mutex.synchronize do
          @executor&.reset!
          @executor = nil
        end
      end

      def drain(timeout: nil)
        timeout = KafkaBatch.config.super_fetch_drain_timeout if timeout.nil?
        executor.drain(timeout: timeout)
      end
    end
  end
end

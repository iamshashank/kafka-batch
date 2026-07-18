# frozen_string_literal: true

module KafkaBatch
  module PerformanceMetrics
    # Cluster-wide Redis RTT sampler for the Performance page. Every process
    # with performance_metrics_enabled runs a ticker, but only the NX lock
    # winner issues a timed PING and writes into the shared :rtt bucket —
    # typically ~4 probes/min regardless of pod count.
    module RttProbe
      LOCK_KEY = "kafka_batch:perf:rtt:probe_lock"

      class << self
        def start!
          return unless PerformanceMetrics.enabled?

          @mutex ||= Mutex.new
          @mutex.synchronize do
            return if @thread&.alive?

            @stop = false
            interval = probe_interval
            @thread = Thread.new do
              Thread.current.name = "kafka-batch-redis-rtt-probe" if Thread.current.respond_to?(:name=)
              until @stop
                begin
                  tick!
                rescue StandardError => e
                  KafkaBatch.logger.debug("[KafkaBatch][PerformanceMetrics::RttProbe] tick failed: #{e.message}")
                end
                sleep(interval)
                break if @stop
              end
            end
            KafkaBatch.logger.info(
              "[KafkaBatch][PerformanceMetrics::RttProbe] started interval=#{interval}s " \
              "timeout=#{probe_timeout}s"
            )
          end
        end

        def stop!
          @mutex ||= Mutex.new
          @mutex.synchronize do
            @stop = true
            thr = @thread
            @thread = nil
            thr&.join(1)
          end
        end

        def running?
          !!@thread&.alive?
        end

        # One probe attempt — used by the loop and by specs.
        def tick!
          return unless PerformanceMetrics.enabled?
          return unless try_lock!

          probe!
        end

        private

        def probe_interval
          s = KafkaBatch.config.redis_rtt_probe_interval.to_f
          s.positive? ? s : 15.0
        end

        def probe_timeout
          t = KafkaBatch.config.redis_rtt_probe_timeout.to_f
          t.positive? ? t : 0.2
        end

        def lock_ttl
          [(probe_interval * 2).ceil, 2].max
        end

        def try_lock!
          won = PerformanceMetrics.redis_with do |r|
            r.set(LOCK_KEY, "1", nx: true, ex: lock_ttl)
          end
          won == true || won == "OK"
        end

        def probe!
          timeout = probe_timeout
          client = KafkaBatch::RedisClient.new(
            KafkaBatch.config,
            timeout: timeout,
            connect_timeout: timeout,
            reconnect_attempts: 0
          )
          raise ConfigurationError, "Redis is not configured" if client.nil?

          t0 = Process.clock_gettime(Process::CLOCK_MONOTONIC)
          client.ping
          elapsed_us = ((Process.clock_gettime(Process::CLOCK_MONOTONIC) - t0) * 1_000_000).round
          client.close rescue nil

          if elapsed_us > (timeout * 1_000_000).round
            PerformanceMetrics.record_rtt_error
          else
            PerformanceMetrics.record_rtt(elapsed_us)
          end
        rescue StandardError
          client&.close rescue nil
          PerformanceMetrics.record_rtt_error
        end
      end
    end
  end
end

# frozen_string_literal: true

require_relative "alerts/payload"
require_relative "alerts/settings"
require_relative "alerts/state"
require_relative "alerts/rules"
require_relative "alerts/availability"
require_relative "alerts/sampler"
require_relative "alerts/notifiers/multi"
require_relative "alerts/evaluator"

module KafkaBatch
  # Health alert evaluator + Redis-backed settings (dashboard /alerts).
  module Alerts
    class << self
      def enabled?
        effective_config["enabled"] == true
      end

      def effective_config
        Settings.effective
      rescue StandardError
        {
          "enabled" => KafkaBatch.config.alerts_enabled,
          "interval" => KafkaBatch.config.alerts_interval
        }
      end

      def start!
        return unless should_run_evaluator?

        @mutex ||= Mutex.new
        @mutex.synchronize do
          return if @thread&.alive?

          @stop = false
          @thread = Thread.new do
            Thread.current.name = "kafka-batch-alerts" if Thread.current.respond_to?(:name=)
            until @stop
              begin
                evaluate_once! if enabled?
              rescue StandardError => e
                KafkaBatch.logger.warn("[KafkaBatch][Alerts] tick failed: #{e.message}")
              end
              sleep(interval_seconds)
              break if @stop
            end
          end
          KafkaBatch.logger.info("[KafkaBatch][Alerts] evaluator started interval=#{interval_seconds}s")
        end
      end

      def stop!
        @mutex ||= Mutex.new
        @mutex.synchronize do
          @stop = true
          thr = @thread
          @thread = nil
          thr&.join(2)
        end
      end

      def running?
        !!@thread&.alive?
      end

      def evaluate_once!
        Evaluator.evaluate_once!(config: effective_config)
      end

      def status
        {
          "enabled" => enabled?,
          "running" => running?,
          "open" => State.open_alerts,
          "last_evaluation" => State.load_last,
          "settings_version" => Settings.version
        }
      end

      def test_channel!(channel)
        cfg = effective_config
        payload = Payload.test(channel: channel)
        Notifiers::Multi.new(cfg).deliver(payload, only: channel)
      end

      def install_subscriptions!
        return if @subscribed

        @subscribed = true
        if defined?(ActiveSupport::Notifications)
          ActiveSupport::Notifications.subscribe(/dlt\.published\.kafka_batch\z/) do |*_args|
            State.incr_dlt! rescue nil
          end
          ActiveSupport::Notifications.subscribe(/cron\.stale\.kafka_batch\z/) do |*args|
            event = ActiveSupport::Notifications::Event.new(*args)
            p = event.payload || {}
            State.mark_cron_stale!(
              schedule: p[:schedule] || p["schedule"],
              job_type: p[:job_type] || p["job_type"],
              stale_seconds: p[:stale_seconds] || p["stale_seconds"]
            ) rescue nil
          end
        end
      end

      def reset!
        stop!
        Settings.reset_pool!
        State.reset_pool!
        @subscribed = false
      end

      # True when this process should host the evaluator thread (control plane).
      # Execution-only Karafka pods and UI/API pods do not run it.
      # Escape hatch: config.alerts_run_on_ui (or KAFKA_BATCH_ALERTS_RUN_ON_UI).
      def control_plane_process?
        return true if KafkaBatch.config.alerts_run_on_ui
        return false unless defined?(Karafka::App)

        groups = karafka_consumer_group_names
        if groups.empty?
          # No drawn groups yet / filter unknown — fall back to KB_ROLE.
          return kb_role_control?
        end

        groups.any? { |g| control_consumer_group?(g) }
      end

      private

      def should_run_evaluator?
        return false unless KafkaBatch.config.redis_configured?
        return false unless defined?(Karafka::App) || KafkaBatch.config.alerts_run_on_ui

        control_plane_process?
      end

      def kb_role_control?
        roles = ENV.fetch("KB_ROLE", "all").split(",").map { |r| r.strip.downcase }
        return true if roles.empty? || (roles & %w[all control scheduler]).any?

        # Explicit execution-only roles
        return false if (roles & %w[jobs execution worker exec]).any?

        true
      end

      def karafka_consumer_group_names
        return [] unless defined?(Karafka::App)

        if Karafka::App.respond_to?(:consumer_groups)
          Array(Karafka::App.consumer_groups).map { |g| g.respond_to?(:name) ? g.name.to_s : g.to_s }
        else
          []
        end
      rescue StandardError
        []
      end

      def control_consumer_group?(name)
        n = name.to_s
        n.end_with?("-control") || n.include?("-dispatch-")
      end

      def interval_seconds
        n = effective_config["interval"].to_i
        n.positive? ? n : 60
      end
    end
  end
end

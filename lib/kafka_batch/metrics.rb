# frozen_string_literal: true

module KafkaBatch
  # Opt-in bridge from ActiveSupport::Notifications → StatsD / Datadog / custom proc.
  #
  # Enable in the initializer:
  #   config.metrics_enabled  = true
  #   config.metrics_adapter  = :statsd   # :datadog (same wire API), :proc
  #   config.metrics_client   = Statsd.new("localhost", 8125)
  #
  # Prometheus (no hard dependency — use :proc):
  #   config.metrics_adapter = :proc
  #   config.metrics_proc = ->(name, payload, duration_ms) {
  #     MY_PROMETHEUS.counter(name.tr(".", "_"), labels: payload.slice(:worker_class)).inc
  #   }
  #
  # Non-Rails: KafkaBatch::Metrics.install! after configure.
  module Metrics
    NAMESPACE_PATTERN = /\.kafka_batch\z/.freeze

    class << self
      def install!(force: false)
        return unless defined?(ActiveSupport::Notifications)

        cfg = KafkaBatch.config
        return unless cfg.metrics_enabled

        adapter = cfg.metrics_adapter
        return if adapter.nil? || adapter == :null || adapter == false

        @mutex ||= Mutex.new
        @mutex.synchronize do
          return if @installed && !force

          subscriber = build_adapter(cfg)
          @subscription = ActiveSupport::Notifications.subscribe(NAMESPACE_PATTERN) do |*args|
            subscriber.call(ActiveSupport::Notifications::Event.new(*args))
          end
          @installed = true
          KafkaBatch.logger.info(
            "[KafkaBatch][Metrics] installed adapter=#{adapter} prefix=#{cfg.metrics_prefix}"
          )
        end
      end

      def reset!
        @mutex&.synchronize do
          if defined?(ActiveSupport::Notifications) && @subscription
            ActiveSupport::Notifications.unsubscribe(@subscription)
          end
          @subscription = nil
          @installed    = false
        end
      end

      private

      def build_adapter(cfg)
        case cfg.metrics_adapter
        when :proc
          ProcAdapter.new(cfg.metrics_proc || cfg.metrics_client)
        when :statsd, :datadog
          StatsdAdapter.new(cfg.metrics_client, prefix: cfg.metrics_prefix)
        else
          raise ConfigurationError,
                "metrics_adapter must be :statsd, :datadog, or :proc (got #{cfg.metrics_adapter.inspect})"
        end
      end
    end

    # ── Adapters ───────────────────────────────────────────────────────────

    class StatsdAdapter
      def initialize(client, prefix: "kafka_batch")
        @client = client
        @prefix = prefix.to_s
        unless @client.respond_to?(:increment)
          raise ConfigurationError, "metrics_client must respond to #increment (StatsD/Datadog client)"
        end
      end

      def call(event)
        metric = "#{@prefix}.#{event.name.sub(NAMESPACE_PATTERN, '').tr('.', '_')}"
        tags   = tags_for(event.payload)

        @client.increment("#{metric}.count", tags: tags)
        @client.timing("#{metric}.duration", event.duration, tags: tags) if @client.respond_to?(:timing)
      rescue StandardError => e
        KafkaBatch.logger.debug("[KafkaBatch][Metrics] emit failed: #{e.message}")
      end

      private

      def tags_for(payload)
        payload.each_with_object([]) do |(k, v), arr|
          next if v.nil?
          next if k == :payload || k == :error

          val = v.is_a?(String) || v.is_a?(Numeric) ? v : v.to_s
          next if val.empty? || val.bytesize > 128

          arr << "#{k}:#{val}"
        end
      end
    end

    class ProcAdapter
      def initialize(handler)
        unless handler.respond_to?(:call)
          raise ConfigurationError, "metrics_proc / metrics_client must respond to #call for :proc adapter"
        end

        @handler = handler
      end

      def call(event)
        @handler.call(event.name, event.payload, event.duration)
      rescue StandardError => e
        KafkaBatch.logger.debug("[KafkaBatch][Metrics] proc emit failed: #{e.message}")
      end
    end
  end
end

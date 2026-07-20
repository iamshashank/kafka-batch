# frozen_string_literal: true

module KafkaBatch
  module Alerts
    module Notifiers
      class Metrics
        def deliver(payload)
          return false unless defined?(KafkaBatch::Metrics) && KafkaBatch.config.metrics_enabled

          name =
            case payload.event
            when "fired" then "alert.fired"
            when "resolved" then "alert.resolved"
            when "test" then "alert.test"
            else "alert.event"
            end
          KafkaBatch::Instrumentation.instrument(
            name,
            {
              rule_id: payload.rule_id,
              severity: payload.severity,
              fingerprint: payload.fingerprint,
              event: payload.event
            }
          )
          true
        rescue StandardError => e
          KafkaBatch.logger.debug("[KafkaBatch][Alerts::Metrics] #{e.message}")
          false
        end
      end
    end
  end
end

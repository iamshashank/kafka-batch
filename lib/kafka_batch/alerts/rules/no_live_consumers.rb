# frozen_string_literal: true

require_relative "base"

module KafkaBatch
  module Alerts
    module Rules
      class NoLiveConsumers < Base
        self.id = "no_live_consumers"
        self.title = "No live consumers with lag"
        self.description =
          "There is Kafka topic lag (pending work) but zero live consumer heartbeats."
        self.detail =
          "Compares topic_pending / lag sample against Liveness live consumer count. Fires when " \
          "pending > 0 and live_consumers == 0. Requires liveness backend :redis."
        self.remediation =
          "Check /live and Karafka/Go worker deployments; confirm heartbeats and Redis connectivity. " \
          "Scale execution pods or fix crash-looping consumers."
        self.default_severity = "critical"
        self.requires = [:liveness]
        self.link = "/live"
        self.settings = []

        def evaluate(sample)
          pending = sample["pending_total"].to_i
          consumers = sample["live_consumers"].to_i
          return [] if pending <= 0
          return [] if consumers.positive?

          [
            finding(
              fingerprint: id,
              summary: "topic_pending=#{pending} but live consumers=0",
              sample: { "pending_total" => pending, "live_consumers" => consumers }
            )
          ]
        end
      end
    end
  end
end

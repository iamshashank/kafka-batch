# frozen_string_literal: true

require_relative "base"

module KafkaBatch
  module Alerts
    module Rules
      class ScheduleDepthHigh < Base
        self.id = "schedule_depth_high"
        self.title = "Delayed-job schedule depth high"
        self.description =
          "Redis schedule pending ZCARD is above threshold (poller stuck, undersized, or overloaded)."
        self.detail =
          "Samples ZCARD of the delayed-job pending index (sched:pending). Fires when depth ≥ " \
          "schedule_pending_max. Often means schedule_poller is off, not running on enough pods, " \
          "or enqueue rate outpaces poller throughput."
        self.remediation =
          "Enable schedule_poller only on a few scheduler pods (KB_ROLE=scheduler); check /scheduled; " \
          "verify Redis/MySQL schedule store health and poller logs."
        self.default_severity = "warning"
        self.requires = []
        self.link = "/scheduled"
        self.settings = [
          {
            "key" => "schedule_pending_max",
            "label" => "Schedule pending max",
            "default" => 10_000,
            "meaning" => "Maximum acceptable ZCARD of the delayed-job pending index."
          }
        ]

        def evaluate(sample)
          pending = sample["schedule_pending"].to_i
          max = @config["schedule_pending_max"].to_i
          return [] if pending < max

          [
            finding(
              fingerprint: id,
              summary: "sched:pending=#{pending} (threshold #{max}), inflight=#{sample['schedule_inflight'].to_i}.",
              sample: {
                "pending" => pending,
                "inflight" => sample["schedule_inflight"].to_i
              }
            )
          ]
        end
      end
    end
  end
end

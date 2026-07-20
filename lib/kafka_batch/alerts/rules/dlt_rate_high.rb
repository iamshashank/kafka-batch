# frozen_string_literal: true

require_relative "base"

module KafkaBatch
  module Alerts
    module Rules
      class DltRateHigh < Base
        self.id = "dlt_rate_high"
        self.title = "Dead-letter rate high"
        self.description =
          "Dead-letter publishes in the last minute exceed the configured rate threshold."
        self.detail =
          "Counts dlt.published instrumentation events into a per-minute Redis counter sampled each " \
          "tick. Fires when that count ≥ dlt_per_minute. Indicates poison jobs, missing handlers, or " \
          "retries exhausted."
        self.remediation =
          "Inspect /dead_letter payloads and handler_manifest coverage; fix failing job_types; " \
          "raise max_retries only if failures are transient."
        self.default_severity = "warning"
        self.requires = []
        self.link = "/dead_letter"
        self.settings = [
          {
            "key" => "dlt_per_minute",
            "label" => "DLT / minute",
            "default" => 50,
            "meaning" => "Maximum dead-letter publishes allowed in a rolling one-minute window."
          }
        ]

        def evaluate(sample)
          count = sample["dlt_per_minute"].to_i
          max = @config["dlt_per_minute"].to_i
          return [] if count < max

          [
            finding(
              fingerprint: id,
              summary: "DLT publishes last minute=#{count} (threshold #{max}).",
              sample: { "dlt_per_minute" => count }
            )
          ]
        end
      end
    end
  end
end

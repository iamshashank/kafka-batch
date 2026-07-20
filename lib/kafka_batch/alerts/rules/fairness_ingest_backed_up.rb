# frozen_string_literal: true

require_relative "base"

module KafkaBatch
  module Alerts
    module Rules
      class FairnessIngestBackedUp < Base
        self.id = "fairness_ingest_backed_up"
        self.title = "Fairness ingest backed up"
        self.description =
          "Fair ingest lag is high while ready lag stays near zero (forwarder / checkout stuck)."
        self.detail =
          "Per fairness lane (time / throughput): fires when ingest_lag ≥ fairness_ingest_lag and " \
          "ready_lag ≤ fairness_ready_max_when_stuck. That pattern usually means jobs pile up on " \
          "ingest while the forwarder is not checking them out to ready."
        self.remediation =
          "Open /fairness/{lane}; confirm {CG}-dispatch-* and forwarder are running; check Redis " \
          "leases, weights, and global concurrency. Look for forwarder errors in control logs."
        self.default_severity = "warning"
        self.requires = []
        self.link = "/fairness/time"
        self.settings = [
          {
            "key" => "fairness_ingest_lag",
            "label" => "Fair ingest lag",
            "default" => 5000,
            "meaning" => "Minimum ingest-topic lag that counts as backed up for a lane."
          },
          {
            "key" => "fairness_ready_max_when_stuck",
            "label" => "Fair ready max when stuck",
            "default" => 10,
            "meaning" => "Ready lag must stay at or below this while ingest is high (stuck forwarder signal)."
          }
        ]

        def evaluate(sample)
          ingest_max = @config["fairness_ingest_lag"].to_i
          ready_max = @config["fairness_ready_max_when_stuck"].to_i
          findings = []

          Array(sample["fairness"]).each do |lane|
            ingest = lane["ingest_lag"].to_i
            ready = lane["ready_lag"].to_i
            next if ingest < ingest_max
            next if ready > ready_max

            findings << finding(
              fingerprint: "#{id}:#{lane['lane']}",
              summary: "Fair #{lane['lane']} ingest_lag=#{ingest} ready_lag=#{ready} (forwarder may be stuck).",
              link: "/fairness/#{lane['lane']}",
              sample: lane
            )
          end
          findings
        end
      end
    end
  end
end

# frozen_string_literal: true

require_relative "base"

module KafkaBatch
  module Alerts
    module Rules
      class ReconcilerStale < Base
        self.id = "reconciler_stale"
        self.title = "Reconciler stale or failing"
        self.description =
          "Reconciler has not run recently, never wrote a summary, or last run looked unhealthy."
        self.detail =
          "Reads the Redis reconciler last-run summary (same as /reconciler). Fires if missing, " \
          "if ran_at age > reconciler_max_age seconds, or if produce_failed > 0 / found_stale ≥ 10."
        self.remediation =
          "Ensure a control plane runs EventConsumer reconciler ticks (or rake kafka_batch:reconcile). " \
          "Inspect produce failures and stuck running batches on /reconciler."
        self.default_severity = "warning"
        self.requires = []
        self.link = "/reconciler"
        self.settings = [
          {
            "key" => "reconciler_max_age",
            "label" => "Reconciler max age (s)",
            "default" => 900,
            "meaning" => "Maximum acceptable seconds since the last successful reconciler summary."
          }
        ]

        def evaluate(sample)
          last = sample["reconciler"]
          max_age = @config["reconciler_max_age"].to_i
          findings = []

          if last.nil? || last.empty?
            findings << finding(
              fingerprint: "#{id}:missing",
              summary: "No reconciler run summary in Redis (never ran or Redis unavailable)."
            )
            return findings
          end

          ran_at = Time.parse(last["ran_at"].to_s) rescue nil
          age = ran_at ? (Time.now - ran_at).to_i : nil
          if age && age > max_age
            findings << finding(
              fingerprint: "#{id}:age",
              summary: "Last reconciler run #{age}s ago (max #{max_age}s).",
              sample: last
            )
          end

          produce_failed = last["produce_failed"].to_i
          found_stale = last["found_stale"].to_i
          if produce_failed.positive? || found_stale >= 10
            findings << finding(
              fingerprint: "#{id}:failures",
              summary: "Reconciler produce_failed=#{produce_failed} found_stale=#{found_stale}.",
              sample: last
            )
          end
          findings
        end
      end
    end
  end
end

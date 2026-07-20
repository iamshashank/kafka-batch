# frozen_string_literal: true

require_relative "base"

module KafkaBatch
  module Alerts
    module Rules
      class RedisRttHigh < Base
        self.id = "redis_rtt_high"
        self.title = "Redis RTT elevated"
        self.description =
          "Cluster Redis probe latency (avg/max) or error rate exceeds thresholds."
        self.detail =
          "Uses PerformanceMetrics Redis RTT probes (same series as /performance). Breaches when " \
          "latest avg ≥ rtt_avg_ms, max ≥ rtt_max_ms, or probe error_rate ≥ rtt_error_rate. " \
          "Requires performance_metrics_enabled."
        self.remediation =
          "Open /performance; check Redis CPU, network, and connection pool saturation. " \
          "Reduce hot KEYS/SCAN usage; scale Redis or lower fairness/schedule churn."
        self.default_severity = "warning"
        self.requires = [:performance_metrics]
        self.link = "/performance"
        self.settings = [
          {
            "key" => "rtt_avg_ms",
            "label" => "RTT avg ms",
            "default" => 50.0,
            "meaning" => "Fire when latest average Redis RTT reaches this many milliseconds."
          },
          {
            "key" => "rtt_max_ms",
            "label" => "RTT max ms",
            "default" => 200.0,
            "meaning" => "Fire when latest max Redis RTT reaches this many milliseconds."
          },
          {
            "key" => "rtt_error_rate",
            "label" => "RTT error rate",
            "default" => 0.25,
            "meaning" => "Fraction of failed probes in the sample window (0.0–1.0) that triggers the rule."
          }
        ]

        def evaluate(sample)
          rtt = sample["rtt"]
          return [] unless rtt.is_a?(Hash)

          avg = rtt["latest_avg_ms"] || rtt["avg_ms"]
          max = rtt["latest_max_ms"] || rtt["max_ms"]
          errors = rtt["errors"].to_i
          probes = rtt["probes"].to_i
          error_rate = probes.positive? ? errors.to_f / probes : 0.0

          breached =
            avg.to_f >= @config["rtt_avg_ms"].to_f ||
            max.to_f >= @config["rtt_max_ms"].to_f ||
            error_rate >= @config["rtt_error_rate"].to_f

          return [] unless breached

          [
            finding(
              fingerprint: id,
              summary: "Redis RTT avg=#{avg}ms max=#{max}ms errors=#{errors}/#{probes}",
              sample: rtt
            )
          ]
        end
      end
    end
  end
end

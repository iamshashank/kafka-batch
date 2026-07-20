# frozen_string_literal: true

require_relative "base"

module KafkaBatch
  module Alerts
    module Rules
      class LagStuckGrowing < Base
        self.id = "lag_stuck_growing"
        self.title = "Lag growing without consumption"
        self.description =
          "Kafka lag is above threshold and committed offsets are stuck while the backlog still grows."
        self.detail =
          "Compares consecutive evaluator ticks per consumer-group/topic. Fires when lag ≥ lag_threshold, " \
          "the group’s committed sum did not advance, and either lag grew by ≥ lag_growth_min or the " \
          "topic end offset advanced. Paused partitions (ConsumptionControl) are skipped."
        self.remediation =
          "Check /lag for that group/topic; verify JobConsumer pods are up and not paused; look for " \
          "handler errors, SuperFetch reclaim storms, or a stuck partition. Resume if intentionally paused."
        self.default_severity = "critical"
        self.requires = []
        self.link = "/lag"
        self.settings = [
          {
            "key" => "lag_threshold",
            "label" => "Lag threshold",
            "default" => 1000,
            "meaning" => "Minimum Kafka lag (messages) before this rule considers the topic."
          },
          {
            "key" => "lag_growth_min",
            "label" => "Lag growth min",
            "default" => 100,
            "meaning" => "Minimum lag increase between ticks (or end-offset growth) to treat as worsening."
          }
        ]

        def evaluate(sample)
          threshold = @config["lag_threshold"].to_i
          growth_min = @config["lag_growth_min"].to_i
          baseline = sample["lag_baseline"] || {}
          paused = sample["paused_keys"] || []
          findings = []

          Array(sample["lag_topics"]).each do |row|
            group = row["group"].to_s
            topic = row["topic"].to_s
            lag = row["lag"].to_i
            next if lag < threshold
            next if paused.include?("#{group}\x1f#{topic}")

            key = "#{group}|#{topic}"
            prev = baseline[key] || {}
            prev_committed = prev["committed"]
            prev_lag = prev["lag"].to_i
            committed = row["committed_sum"]
            end_sum = row["end_sum"]

            committed_stuck =
              !prev_committed.nil? &&
              !committed.nil? &&
              prev_committed.to_i == committed.to_i
            lag_grew = lag - prev_lag >= growth_min
            end_grew =
              !prev["end_sum"].nil? &&
              !end_sum.nil? &&
              end_sum.to_i > prev["end_sum"].to_i

            next unless committed_stuck && (lag_grew || end_grew)

            findings << finding(
              fingerprint: "#{id}:#{group}:#{topic}",
              summary: "#{topic} (#{group}) lag=#{lag} committed stuck; backlog still growing.",
              sample: row
            )
          end
          findings
        end
      end
    end
  end
end

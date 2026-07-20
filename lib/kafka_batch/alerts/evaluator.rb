# frozen_string_literal: true

require "time"
require_relative "settings"
require_relative "state"
require_relative "sampler"
require_relative "rules"
require_relative "availability"
require_relative "payload"
require_relative "notifiers/multi"

module KafkaBatch
  module Alerts
    module Evaluator
      class << self
        def evaluate_once!(config: nil)
          cfg = config || Settings.effective
          return { "ok" => false, "reason" => "disabled" } unless cfg["enabled"]
          return { "ok" => false, "reason" => "redis" } unless KafkaBatch.config.redis_configured?

          ttl = [cfg["interval"].to_i, 30].max
          return { "ok" => false, "reason" => "lock" } unless State.try_lock!(ttl: ttl)

          sample = Sampler.collect(cfg)
          findings = run_rules(cfg, sample)
          transitions = apply_hysteresis(cfg, findings)
          notify_transitions(cfg, transitions)
          Sampler.persist_baseline!(sample)

          summary = {
            "ran_at" => Time.now.utc.iso8601,
            "findings" => findings.size,
            "open" => State.open_alerts.size,
            "fired" => transitions.count { |t| t[:event] == "fired" },
            "resolved" => transitions.count { |t| t[:event] == "resolved" },
            "settings_version" => Settings.version
          }
          State.save_last!(summary)
          summary.merge("ok" => true)
        rescue StandardError => e
          KafkaBatch.logger.warn("[KafkaBatch][Alerts::Evaluator] #{e.class}: #{e.message}")
          { "ok" => false, "reason" => e.message }
        end

        private

        def run_rules(cfg, sample)
          findings = []
          Rules.catalog.each do |klass|
            rule = klass.new(cfg)
            next unless rule.enabled?

            ok, = Availability.requirements_met?(klass.requires)
            next unless ok

            findings.concat(Array(rule.evaluate(sample)))
          rescue StandardError => e
            KafkaBatch.logger.warn("[KafkaBatch][Alerts] rule #{klass.id}: #{e.message}")
          end
          findings
        end

        # Notify at most once per open and once per resolve for a fingerprint.
        # No periodic "reminder" re-fires while the incident stays open.
        def apply_hysteresis(cfg, findings)
          for_ticks = [cfg["for_ticks"].to_i, 1].max
          resolve_ticks = [cfg["resolve_ticks"].to_i, 1].max
          now = Time.now.utc
          active = findings.each_with_object({}) { |f, h| h[f.fingerprint] = f }
          transitions = []

          active.each do |fp, finding|
            State.incr_breach!(fp)
            State.reset_healthy!(fp)
            count = State.breach_count(fp)
            open = State.get_open(fp)

            if open
              # Still breached — keep incident, refresh summary, do not re-notify.
              State.touch_open!(fp, summary: finding.summary)
              next
            end

            next if count < for_ticks

            incident = {
              "fingerprint" => fp,
              "rule_id" => finding.rule_id,
              "title" => finding.title,
              "summary" => finding.summary,
              "severity" => finding.severity,
              "link" => finding.link,
              "sample" => finding.sample,
              "fired_at" => now.iso8601,
              "last_notify_at" => now.iso8601
            }
            # HSETNX: only the first control plane (Ruby or Go) opens + notifies.
            next unless State.claim_open!(fp, incident)

            transitions << { event: "fired", finding: finding, fired_at: incident["fired_at"] }
          end

          State.open_alerts.each do |incident|
            fp = incident["fingerprint"].to_s
            next if active.key?(fp)

            State.incr_healthy!(fp)
            State.reset_breach!(fp)
            next if State.healthy_count(fp) < resolve_ticks
            next unless State.clear_open!(fp)

            State.reset_healthy!(fp)
            finding = Rules::Finding.new(
              rule_id: incident["rule_id"],
              fingerprint: fp,
              title: incident["title"],
              summary: incident["summary"],
              severity: incident["severity"],
              link: incident["link"],
              sample: incident["sample"] || {}
            )
            transitions << {
              event: "resolved",
              finding: finding,
              fired_at: incident["fired_at"]
            }
          end

          transitions
        end

        def notify_transitions(cfg, transitions)
          multi = Notifiers::Multi.new(cfg)
          # Short TTL: block concurrent duplicate delivers, but allow a later
          # re-open of the same fingerprint after resolve to notify again.
          dedupe_ttl = [[cfg["interval"].to_i * 3, 120].max, 3600].min
          transitions.each do |t|
            finding = t[:finding]
            fired_at = t[:fired_at].to_s
            dedupe_event = fired_at.empty? ? t[:event].to_s : "#{t[:event]}:#{fired_at}"
            next unless State.claim_notify!(finding.fingerprint, dedupe_event, ttl: dedupe_ttl)

            payload = Payload.new(
              event: t[:event],
              rule_id: finding.rule_id,
              title: finding.title,
              summary: finding.summary,
              severity: finding.severity,
              fingerprint: finding.fingerprint,
              link: finding.link,
              sample: finding.sample,
              fired_at: t[:fired_at] || (t[:event] == "fired" ? Time.now.utc.iso8601 : nil),
              resolved_at: t[:event] == "resolved" ? Time.now.utc.iso8601 : nil
            )
            multi.deliver(payload)
          end
        end
      end
    end
  end
end

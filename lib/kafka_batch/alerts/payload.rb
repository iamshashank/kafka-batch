# frozen_string_literal: true

require "oj"
require "securerandom"
require "time"

module KafkaBatch
  module Alerts
    # Normalized notification payload for all channels.
    class Payload
      ATTRS = %i[
        event rule_id title summary severity fingerprint
        link sample fired_at resolved_at
      ].freeze

      attr_reader(*ATTRS)

      def initialize(**opts)
        @event = opts[:event].to_s
        @rule_id = opts[:rule_id].to_s
        @title = opts[:title].to_s
        @summary = opts[:summary].to_s
        @severity = (opts[:severity] || "warning").to_s
        @fingerprint = opts[:fingerprint].to_s
        @link = opts[:link]
        @sample = opts[:sample] || {}
        @fired_at = opts[:fired_at]
        @resolved_at = opts[:resolved_at]
      end

      def to_h
        {
          "event" => event,
          "rule_id" => rule_id,
          "title" => title,
          "summary" => summary,
          "severity" => severity,
          "fingerprint" => fingerprint,
          "link" => link,
          "sample" => sample,
          "fired_at" => fired_at,
          "resolved_at" => resolved_at,
          "source" => "kafka-batch"
        }.compact
      end

      def to_json
        Oj.dump(to_h, mode: :compat)
      end

      def self.test(channel:)
        new(
          event: "test",
          rule_id: "test",
          title: "kafka-batch alert test (#{channel})",
          summary: "This is a test notification from the Alerts settings page.",
          severity: "warning",
          fingerprint: "test:#{channel}:#{SecureRandom.hex(4)}",
          fired_at: Time.now.utc.iso8601
        )
      end
    end
  end
end

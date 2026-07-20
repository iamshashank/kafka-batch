# frozen_string_literal: true

require_relative "slack"
require_relative "webhook"
require_relative "email"
require_relative "metrics"
require_relative "../availability"

module KafkaBatch
  module Alerts
    module Notifiers
      class Multi
        def initialize(config)
          @config = config
        end

        def deliver(payload, only: nil)
          channels = Availability.channels(@config)
          targets = channels.select { |c| c["ready_to_send"] }
          targets = targets.select { |c| c["id"] == only.to_s } if only
          return false if targets.empty?

          ok = false
          targets.each do |ch|
            notifier = build(ch["id"])
            next unless notifier

            ok = true if notifier.deliver(payload)
          end
          ok
        end

        private

        def build(id)
          case id
          when "slack"
            Slack.new(webhook_url: @config["slack_webhook_url"])
          when "webhook"
            Webhook.new(urls: @config["webhook_urls"])
          when "email"
            Email.new(
              to: @config["email_to"],
              from: @config["email_from"],
              smtp_address: @config["email_smtp_address"],
              smtp_port: @config["email_smtp_port"],
              smtp_user: @config["email_smtp_user"],
              smtp_password: @config["email_smtp_password"]
            )
          when "metrics"
            Metrics.new
          end
        end
      end
    end
  end
end

# frozen_string_literal: true

require "net/http"
require "uri"
require "oj"

module KafkaBatch
  module Alerts
    module Notifiers
      class Slack
        def initialize(webhook_url:)
          @webhook_url = webhook_url.to_s
        end

        def deliver(payload)
          return false if @webhook_url.empty?

          body = {
            "text" => "[#{payload.severity}] #{payload.title}",
            "blocks" => [
              {
                "type" => "header",
                "text" => { "type" => "plain_text", "text" => "#{payload.event.upcase}: #{payload.title}"[0, 150] }
              },
              {
                "type" => "section",
                "text" => { "type" => "mrkdwn", "text" => payload.summary.to_s[0, 2900] }
              },
              {
                "type" => "context",
                "elements" => [
                  { "type" => "mrkdwn", "text" => "rule=`#{payload.rule_id}` · `#{payload.fingerprint}`" }
                ]
              }
            ]
          }
          if payload.link
            body["blocks"] << {
              "type" => "actions",
              "elements" => [
                {
                  "type" => "button",
                  "text" => { "type" => "plain_text", "text" => "Open dashboard" },
                  "url" => absolute_link(payload.link)
                }
              ]
            }
          end
          post_json(@webhook_url, body)
        end

        private

        def absolute_link(path)
          return path if path.to_s.start_with?("http")

          path.to_s.start_with?("/") ? path.to_s : "/#{path}"
        end

        def post_json(url, body)
          uri = URI.parse(url)
          http = Net::HTTP.new(uri.host, uri.port)
          http.use_ssl = uri.scheme == "https"
          http.open_timeout = 5
          http.read_timeout = 10
          req = Net::HTTP::Post.new(uri.request_uri)
          req["Content-Type"] = "application/json"
          req.body = Oj.dump(body, mode: :compat)
          res = http.request(req)
          res.is_a?(Net::HTTPSuccess)
        rescue StandardError => e
          KafkaBatch.logger.warn("[KafkaBatch][Alerts::Slack] #{e.class}: #{e.message}")
          false
        end
      end
    end
  end
end

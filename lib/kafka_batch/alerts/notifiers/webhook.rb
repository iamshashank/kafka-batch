# frozen_string_literal: true

require "net/http"
require "uri"
require "oj"

module KafkaBatch
  module Alerts
    module Notifiers
      class Webhook
        def initialize(urls:)
          @urls = Array(urls).map(&:to_s).reject(&:empty?)
        end

        def deliver(payload)
          return false if @urls.empty?

          ok = true
          @urls.each do |url|
            ok = false unless post_json(url, payload.to_h)
          end
          ok
        end

        private

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
          KafkaBatch.logger.warn("[KafkaBatch][Alerts::Webhook] #{e.class}: #{e.message}")
          false
        end
      end
    end
  end
end

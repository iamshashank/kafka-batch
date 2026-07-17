# frozen_string_literal: true

require "json"
require "net/http"
require "uri"
require "oj"

module KafkaBatch
  module Ai
    # Thin OpenRouter Chat Completions client (OpenAI-compatible).
    class OpenRouter
      Error = Class.new(StandardError)

      def initialize(api_key:, model:, base_url:)
        @api_key = api_key.to_s
        @model = model.to_s
        @base_url = base_url.to_s.sub(%r{/+\z}, "")
      end

      # @param messages [Array<Hash>] role/content pairs
      # @return [String] assistant text
      def chat(messages:, temperature: 0.2, max_tokens: 1200)
        raise Error, "OpenRouter API key is not set" if @api_key.empty?
        raise Error, "model is blank" if @model.empty?

        uri = URI.parse("#{@base_url}/chat/completions")
        body = {
          "model" => @model,
          "messages" => messages,
          "temperature" => temperature,
          "max_tokens" => max_tokens
        }

        http = Net::HTTP.new(uri.host, uri.port)
        http.use_ssl = (uri.scheme == "https")
        http.open_timeout = 10
        http.read_timeout = 60

        req = Net::HTTP::Post.new(uri.request_uri)
        req["Authorization"] = "Bearer #{@api_key}"
        req["Content-Type"] = "application/json"
        req["HTTP-Referer"] = "https://github.com/y-shashank/kafka-batch"
        req["X-Title"] = "kafka-batch"
        req.body = Oj.dump(body)

        res = http.request(req)
        payload = Oj.load(res.body.to_s) rescue {}
        unless res.is_a?(Net::HTTPSuccess)
          msg = payload.dig("error", "message") || payload["error"] || res.body.to_s[0, 300]
          raise Error, "OpenRouter HTTP #{res.code}: #{msg}"
        end

        content = payload.dig("choices", 0, "message", "content")
        raise Error, "OpenRouter returned empty content" if content.nil? || content.to_s.strip.empty?

        content.to_s
      end
    end
  end
end

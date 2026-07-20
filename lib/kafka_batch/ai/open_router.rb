# frozen_string_literal: true

require "json"
require "net/http"
require "openssl"
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

      # @param messages [Array<Hash>] role/content pairs (and optional tool messages)
      # @param tools [Array<Hash>, nil] OpenAI-style tool schemas
      # @return [Hash] "content" => String|nil, "tool_calls" => Array|nil
      def chat(messages:, temperature: 0.2, max_tokens: 1200, tools: nil)
        raise Error, "OpenRouter API key is not set" if @api_key.empty?
        raise Error, "model is blank" if @model.empty?

        uri = URI.parse("#{@base_url}/chat/completions")
        body = {
          "model" => @model,
          "messages" => messages,
          "temperature" => temperature,
          "max_tokens" => max_tokens
        }
        if tools && !tools.empty?
          body["tools"] = tools
          body["tool_choice"] = "auto"
        end

        payload = request!(uri, body)
        message = payload.dig("choices", 0, "message") || {}
        content = message["content"]
        tool_calls = message["tool_calls"]
        tool_calls = nil if tool_calls.nil? || tool_calls.empty?

        if (content.nil? || content.to_s.strip.empty?) && tool_calls.nil?
          raise Error, "OpenRouter returned empty content"
        end

        {
          "content" => content.nil? ? nil : content.to_s,
          "tool_calls" => tool_calls
        }
      end

      private

      def request!(uri, body)
        http = Net::HTTP.new(uri.host, uri.port)
        http.use_ssl = (uri.scheme == "https")
        http.open_timeout = 10
        http.read_timeout = 60
        configure_ssl!(http) if http.use_ssl?

        req = Net::HTTP::Post.new(uri.request_uri)
        req["Authorization"] = "Bearer #{@api_key}"
        req["Content-Type"] = "application/json"
        req["HTTP-Referer"] = "https://github.com/y-shashank/kafka-batch"
        req["X-Title"] = "kafka-batch"
        req.body = Oj.dump(body)

        res = http.request(req)
        payload = Oj.load(res.body.to_s) rescue {}
        unless res.is_a?(Net::HTTPSuccess)
          raise Error, "OpenRouter HTTP #{res.code}: #{format_error(payload, res)}"
        end
        payload
      rescue OpenSSL::SSL::SSLError => e
        raise Error, ssl_error_message(e)
      end

      def format_error(payload, res)
        err = payload.is_a?(Hash) ? payload["error"] : nil
        parts = []
        if err.is_a?(Hash)
          parts << err["message"].to_s if err["message"]
          meta = err["metadata"]
          if meta.is_a?(Hash)
            raw = meta["raw"].to_s
            if !raw.empty?
              nested = (Oj.load(raw) rescue nil)
              nested_msg = nested.is_a?(Hash) ? nested.dig("error", "message") : nil
              parts << nested_msg if nested_msg && !parts.include?(nested_msg)
              parts << "provider=#{meta['provider_name']}" if meta["provider_name"]
            end
          end
          parts << "type=#{err['type']}" if err["type"]
        elsif err
          parts << err.to_s
        end
        parts << res.body.to_s[0, 300] if parts.empty?
        parts.reject { |p| p.nil? || p.to_s.empty? }.join(" — ")
      end

      # Provide an explicit CA store so ruby/openssl does not apply
      # V_FLAG_CRL_CHECK_ALL (OpenSSL 3.6 + macOS fails with "unable to get
      # certificate CRL" when CRLs are not available locally). Peer verify stays on.
      def configure_ssl!(http)
        store = OpenSSL::X509::Store.new
        store.set_default_paths
        http.cert_store = store
        http.verify_mode = OpenSSL::SSL::VERIFY_PEER
      end

      def ssl_error_message(error)
        msg = error.message.to_s
        hint =
          if msg.include?("CRL")
            " Local OpenSSL/CRL quirk — kafka-batch already disables CRL checks on this client; " \
              "if it persists, add `gem \"openssl\", \">= 3.3.1\"` to the app Gemfile " \
              "or upgrade Ruby (3.3.11+ / 3.4.10+)."
          else
            ""
          end
        "OpenRouter SSL error: #{msg}.#{hint}"
      end
    end
  end
end

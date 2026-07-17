# frozen_string_literal: true

require "base64"
require "digest"
require "openssl"
require "securerandom"
require "oj"

module KafkaBatch
  module Ai
    # AES-256-GCM helpers for OpenRouter API keys stored in Redis.
    # Key material is derived from config.ai_encryption_salt (required to store secrets).
    module Crypto
      VERSION = 1
      ALGO = "aes-256-gcm".freeze

      class << self
        def configured?
          salt = KafkaBatch.config.ai_encryption_salt.to_s
          !salt.strip.empty?
        end

        def encrypt(plaintext)
          raise ConfigurationError, "config.ai_encryption_salt is required to store AI secrets" unless configured?

          key = derive_key
          iv  = SecureRandom.random_bytes(12)
          cipher = OpenSSL::Cipher.new(ALGO)
          cipher.encrypt
          cipher.key = key
          cipher.iv  = iv
          ciphertext = cipher.update(plaintext.to_s) + cipher.final
          tag = cipher.auth_tag
          payload = {
            "v" => VERSION,
            "iv" => Base64.strict_encode64(iv),
            "tag" => Base64.strict_encode64(tag),
            "ct" => Base64.strict_encode64(ciphertext)
          }
          Base64.strict_encode64(Oj.dump(payload))
        end

        def decrypt(blob)
          raise ConfigurationError, "config.ai_encryption_salt is required to read AI secrets" unless configured?
          return nil if blob.nil? || blob.to_s.empty?

          payload = Oj.load(Base64.strict_decode64(blob.to_s))
          iv  = Base64.strict_decode64(payload.fetch("iv"))
          tag = Base64.strict_decode64(payload.fetch("tag"))
          ct  = Base64.strict_decode64(payload.fetch("ct"))
          key = derive_key
          cipher = OpenSSL::Cipher.new(ALGO)
          cipher.decrypt
          cipher.key = key
          cipher.iv  = iv
          cipher.auth_tag = tag
          cipher.update(ct) + cipher.final
        rescue StandardError => e
          KafkaBatch.logger.warn("[KafkaBatch][Ai::Crypto] decrypt failed: #{e.class}: #{e.message}")
          nil
        end

        private

        def derive_key
          Digest::SHA256.digest(KafkaBatch.config.ai_encryption_salt.to_s)
        end
      end
    end
  end
end

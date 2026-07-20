# frozen_string_literal: true

module KafkaBatch
  module Alerts
    module Notifiers
      class Email
        def initialize(to:, from:, smtp_address:, smtp_port:, smtp_user:, smtp_password:)
          @to = to.to_s
          @from = from.to_s.empty? ? "kafka-batch-alerts@localhost" : from.to_s
          @smtp_address = smtp_address.to_s
          @smtp_port = smtp_port.to_i
          @smtp_user = smtp_user.to_s
          @smtp_password = smtp_password.to_s
        end

        def deliver(payload)
          return false if @to.empty? || @smtp_address.empty?
          return false unless load_smtp!

          recipients = @to.split(/[,\s]+/).map(&:strip).reject(&:empty?)
          return false if recipients.empty?

          subject = "[kafka-batch][#{payload.severity}] #{payload.title}"
          body = <<~MSG
            From: #{@from}
            To: #{recipients.join(', ')}
            Subject: #{subject}
            MIME-Version: 1.0
            Content-Type: text/plain; charset=UTF-8

            #{payload.summary}

            rule: #{payload.rule_id}
            fingerprint: #{payload.fingerprint}
            event: #{payload.event}
            link: #{payload.link}
          MSG

          smtp = Net::SMTP.new(@smtp_address, @smtp_port)
          smtp.enable_starttls_auto if smtp.respond_to?(:enable_starttls_auto)
          if @smtp_user.empty?
            smtp.start(@smtp_address) { |s| s.send_message(body, @from, recipients) }
          else
            smtp.start(@smtp_address, @smtp_user, @smtp_password, :login) do |s|
              s.send_message(body, @from, recipients)
            end
          end
          true
        rescue StandardError => e
          KafkaBatch.logger.warn("[KafkaBatch][Alerts::Email] #{e.class}: #{e.message}")
          false
        end

        private

        def load_smtp!
          return true if defined?(Net::SMTP)

          require "net/smtp"
          true
        rescue LoadError
          KafkaBatch.logger.warn(
            "[KafkaBatch][Alerts::Email] net/smtp is not available — " \
            "add `gem \"net-smtp\"` to the host Gemfile for Ruby 3.4+"
          )
          false
        end
      end
    end
  end
end

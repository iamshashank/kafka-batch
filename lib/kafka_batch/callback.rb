# frozen_string_literal: true

require "oj"

module KafkaBatch
  # Batch callback spec — either a job (Sidekiq-style, routed to a user topic) or a
  # legacy Ruby class name (invoked by CallbackConsumer).
  module Callback
    Job = Struct.new(:job_type, :topic, keyword_init: true) do
      def job?
        true
      end

      def legacy?
        false
      end
    end

    Legacy = Struct.new(:class_name, keyword_init: true) do
      def job?
        false
      end

      def legacy?
        true
      end
    end

    class << self
      # Job callback — +job_type+ matches handler manifest / kbatch.Register.
      # Optional +topic+ overrides manifest routing (required for custom queues).
      def job(job_type, topic: nil)
        jt = job_type.to_s.strip
        raise ArgumentError, "job_type required" if jt.empty?

        Job.new(job_type: jt, topic: topic&.to_s&.strip)
      end

      # Job callback from a Ruby Worker (job_type + kafka_topic).
      def worker(worker_class)
        KafkaBatch::Batch.ensure_worker!(worker_class)
        Job.new(job_type: worker_class.job_type, topic: worker_class.kafka_topic)
      end

      # Serialize for Redis / wire storage.
      def dump(spec)
        case spec
        when Job
          Oj.dump({"job_type" => spec.job_type, "topic" => spec.topic}.compact)
        when Legacy
          spec.class_name.to_s
        when String
          spec.to_s
        else
          raise ArgumentError, "unsupported callback spec: #{spec.class}"
        end
      end

      # Parse a stored callback value.
      def parse(value)
        return nil if value.nil?

        s = value.to_s.strip
        return nil if s.empty?

        if s.start_with?("{")
          h = Oj.load(s)
          h = h.transform_keys(&:to_s) if h.is_a?(Hash)
          jt = h["job_type"].to_s.strip
          raise ArgumentError, "callback job_type required" if jt.empty?

          Job.new(job_type: jt, topic: h["topic"]&.to_s&.strip)
        else
          Legacy.new(class_name: s)
        end
      rescue Oj::ParseError
        Legacy.new(class_name: s)
      end

      def job?(value)
        spec = value.is_a?(Job) ? value : parse(value)
        spec.is_a?(Job)
      end

      def legacy?(value)
        spec = value.is_a?(Legacy) ? value : parse(value)
        spec.is_a?(Legacy)
      end

      def normalize(spec)
        case spec
        when Job, Legacy then spec
        when String
          parse(spec) || Legacy.new(class_name: spec)
        else
          raise ArgumentError, "unsupported callback spec: #{spec.class}"
        end
      end
    end
  end
end

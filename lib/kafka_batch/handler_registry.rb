# frozen_string_literal: true

module KafkaBatch
# Maps stable job_type identifiers to execution handlers (:ruby in-process via Karafka).
  class HandlerRegistry
    class UnknownHandler < Error; end

    Handler = Struct.new(:job_type, :runtime, :worker_class, :executor, :definition, keyword_init: true) do
      def worker_class_name
        definition&.worker_class_name || worker_class&.name.to_s
      end
    end

    @mutex           = Mutex.new
    @by_job_type     = {}
    @by_worker_class = {}

    class << self
      def register_ruby(worker_class)
        unless worker_class.is_a?(Class) && worker_class.include?(KafkaBatch::Worker)
          raise ArgumentError, "#{worker_class} must include KafkaBatch::Worker"
        end

        runtime = worker_class.executor
        if runtime == :go
          raise ArgumentError,
                "executor :go is removed — declare runtime: go in the handler manifest and run kbatch worker"
        end
        unless runtime == :ruby
          raise ArgumentError, "unsupported executor #{runtime.inspect} for #{worker_class}"
        end

        register_definition(HandlerDefinition.from_worker(worker_class))
      end

      def register_definition(definition, executor: nil)
        job_type = definition.job_type
        worker_class = definition.worker_class
        runtime = definition.runtime

        exec =
          case runtime
          when :ruby
            if worker_class
              executor || Executors::Ruby.new(worker_class)
            elsif definition.declared_worker_class_name.to_s.empty?
              raise ArgumentError, "ruby handler missing worker_class for #{job_type}"
            else
              # Manifest loaded before Zeitwerk resolved the class — bind later.
              nil
            end
          when :go
            nil
          else
            raise ArgumentError, "unsupported runtime #{runtime.inspect} for #{job_type}"
          end

        handler = Handler.new(
          job_type:      job_type,
          runtime:       runtime,
          worker_class:  worker_class,
          executor:      exec,
          definition:    definition
        )

        @mutex.synchronize do
          existing = @by_job_type[job_type]
          if existing
            unless compatible_reregistration?(existing, handler)
              raise ArgumentError,
                    "job_type #{job_type.inspect} already registered to #{existing.worker_class || existing.definition&.worker_class_name}"
            end
          end

          bound = merge_handler(existing, handler)
          @by_job_type[job_type] = bound
          if bound.worker_class&.name && !bound.worker_class.name.to_s.empty?
            @by_worker_class[bound.worker_class.name] = bound
          elsif bound.definition&.declared_worker_class_name && !bound.definition.declared_worker_class_name.empty?
            @by_worker_class[bound.definition.declared_worker_class_name] = bound
          end
          bound
        end
      end
      # @return [Handler]
      # @raise [UnknownHandler]
      def resolve!(data)
        job_type    = data["job_type"]
        worker_name = data["worker_class"]

        if job_type && !job_type.to_s.empty?
          handler = @mutex.synchronize { @by_job_type[job_type.to_s] }
          return ensure_ruby_bound!(handler) if handler
        end

        if worker_name && !worker_name.to_s.empty?
          handler = resolve_by_worker_class!(worker_name.to_s)
          if job_type && !job_type.to_s.empty? && handler.job_type != job_type.to_s
            raise UnknownHandler,
                  "job_type #{job_type.inspect} does not match #{handler.job_type.inspect} " \
                  "for #{worker_name}"
          end
          return ensure_ruby_bound!(handler)
        end

        raise UnknownHandler, "Unknown job_type: #{job_type}" if job_type && !job_type.to_s.empty?

        raise UnknownHandler, "Missing job_type and worker_class"
      end

      def definition!(job_type)
        handler = @mutex.synchronize { @by_job_type[job_type.to_s] }
        raise UnknownHandler, "Unknown job_type: #{job_type}" unless handler

        ensure_ruby_bound!(handler).definition
      end

      def lookup_by_job_type(job_type)
        @mutex.synchronize { @by_job_type[job_type.to_s] }
      end

      # Resolve handler runtime from a job payload (for fair forwarder routing).
      # Defaults to :ruby when unknown (Ruby-only stacks).
      def runtime_for_payload(data)
        data = data.transform_keys(&:to_s) if data.respond_to?(:transform_keys)
        job_type = data["job_type"]
        if job_type && !job_type.to_s.empty?
          handler = lookup_by_job_type(job_type)
          return handler.runtime if handler
        end

        worker_name = data["worker_class"]
        if worker_name && !worker_name.to_s.empty?
          handler = @mutex.synchronize { @by_worker_class[worker_name.to_s] }
          return handler.runtime if handler
        end

        :ruby
      end

      private :lookup_by_job_type

      def resolve_by_worker_class!(worker_name)
        handler = @mutex.synchronize { @by_worker_class[worker_name] }
        return handler if handler

        klass = Object.const_get(worker_name)
        raise UnknownHandler, "#{worker_name} does not include KafkaBatch::Worker" \
          unless klass.include?(KafkaBatch::Worker)

        register_ruby(klass)
      rescue NameError
        raise UnknownHandler, "Unknown worker class: #{worker_name}"
      end
      private :resolve_by_worker_class!

      def ensure_ruby_bound!(handler)
        return handler unless handler
        return handler unless handler.runtime == :ruby
        return handler if handler.worker_class && handler.executor

        name = handler.definition&.declared_worker_class_name || handler.definition&.worker_class_name
        raise UnknownHandler, "ruby handler #{handler.job_type.inspect} has no worker_class" \
          if name.nil? || name.to_s.empty? || name.to_s.start_with?("go:")

        klass =
          if name.respond_to?(:safe_constantize)
            name.safe_constantize
          else
            Object.const_get(name)
          end
        raise UnknownHandler, "Unknown worker class: #{name}" unless klass
        raise UnknownHandler, "#{name} does not include KafkaBatch::Worker" \
          unless klass.include?(KafkaBatch::Worker)

        register_ruby(klass)
      rescue NameError
        raise UnknownHandler, "Unknown worker class: #{name}"
      end
      private :ensure_ruby_bound!

      def compatible_reregistration?(existing, incoming)
        return true if existing.worker_class == incoming.worker_class
        return true if existing.worker_class.nil? && incoming.worker_class
        return true if incoming.worker_class.nil? && existing.worker_class

        existing_name = existing.definition&.worker_class_name
        incoming_name = incoming.definition&.worker_class_name
        existing_name && incoming_name && existing_name == incoming_name
      end
      private :compatible_reregistration?

      def merge_handler(existing, incoming)
        return incoming unless existing
        return incoming if incoming.worker_class
        return existing if existing.worker_class

        incoming
      end
      private :merge_handler

      def registered?(job_type)
        @mutex.synchronize { @by_job_type.key?(job_type.to_s) }
      end

      def reset!
        @mutex.synchronize do
          @by_job_type     = {}
          @by_worker_class = {}
        end
      end
    end
  end
end

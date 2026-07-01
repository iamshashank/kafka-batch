require "rails/generators"

module KafkaBatch
  module Generators
    class InstallGenerator < Rails::Generators::Base
      source_root File.expand_path("templates", __dir__)
      namespace "kafka_batch:install"

      desc "Creates a KafkaBatch initializer, copies the topic-creation shell " \
           "script, and (when --store mysql) copies migrations."

      class_option :store, type: :string, default: "mysql",
                           desc: "State store to use: mysql or redis"

      def validate_store_option
        unless %w[mysql redis].include?(options[:store])
          raise ArgumentError, "--store must be 'mysql' or 'redis', got '#{options[:store]}'"
        end
        @store = options[:store]
      end

      def create_initializer
        template "initializer.rb", "config/initializers/kafka_batch.rb"
      end

      def copy_shell_script
        copy_file "create_kafka_topics.sh", "bin/create_kafka_topics.sh"
        # Make it executable right away so the developer can run it immediately.
        in_root { chmod "bin/create_kafka_topics.sh", 0o755 rescue nil }
      end

      def copy_migrations
        if @store == "mysql"
          rake "kafka_batch:install_migrations"
        end
      end

      def show_next_steps
        say "\n"
        say "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━", :green
        say "  KafkaBatch installed  (store: #{@store})", :green
        say "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━\n", :green

        say "\n1. Add KafkaBatch routes to your karafka.rb:\n"
        say <<~ROUTES
          class KarafkaApp < Karafka::App
            routes.draw do
              KafkaBatch.draw_routes(self)
              # ... your other routes
            end
          end
        ROUTES

        if @store == "mysql"
          say "\n2. Run the migrations:\n"
          say "     rails db:migrate\n"
          say "\n3. Create Kafka topics (choose one):\n"
        else
          say "\n2. Create Kafka topics (choose one):\n"
        end

        say "   # Rake task (requires a running Kafka broker + Karafka loaded):"
        say "     bundle exec rake kafka_batch:create_topics\n"
        say ""
        say "   # Shell script (works without Rails — for CI, Docker init, etc.):"
        say "     KAFKA_BROKERS=localhost:9092 ./bin/create_kafka_topics.sh\n"
        say ""
        say "   # With a topic prefix (must match KAFKA_PREFIX in your env):"
        say "     KAFKA_BROKERS=kafka:9092 KAFKA_PREFIX=myapp ./bin/create_kafka_topics.sh\n"
        say ""
        say "   # Force a specific partition count and replication factor:"
        say "     PARTITIONS=12 REPLICATION_FACTOR=3 ./bin/create_kafka_topics.sh\n"

        say "\n4. Mount the web dashboard in config/routes.rb:\n"
        say <<~ROUTES
          # Protect this behind authentication (e.g. authenticate :admin).
          mount KafkaBatch::Web => "/kafka_batch"
        ROUTES

        say "\n5. Review config/initializers/kafka_batch.rb and tune:\n"
        say "   - retry_tiers / max_retries\n"
        say "   - fairness_mode  (:time_fairness or :job_count_fairness)\n"
        say "   - max_message_bytes  (1 MiB default; match your broker limit)\n"
        say "   - reconciliation_interval / max_reconcile_per_run\n"
        say "   - liveness_backend  (:redis / :off)\n"
        if @store == "redis"
          say "   - redis_url / redis_pool_size / batch_ttl\n"
          say "   - all_index_max_size  (batch list page history cap)\n"
        end
        say "\n"
      end
    end
  end
end

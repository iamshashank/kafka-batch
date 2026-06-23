require "rails/railtie"

module KafkaBatch
  class Railtie < Rails::Railtie
    railtie_name :kafka_batch

    # Make KafkaBatch.config.logger default to Rails.logger
    initializer "kafka_batch.logger" do
      KafkaBatch.config.logger = Rails.logger if KafkaBatch.config.logger.is_a?(Logger)
    end

    # Validate configuration once the app is fully loaded
    initializer "kafka_batch.validate_config", after: :load_config_initializers do
      KafkaBatch.config.validate!
    rescue KafkaBatch::ConfigurationError => e
      raise e
    end

    # Gracefully close the WaterDrop producer on server shutdown
    config.after_initialize do
      at_exit { KafkaBatch::Producer.reset! }
    end

    # ── Rake tasks ───────────────────────────────────────────────────────────
    rake_tasks do
      namespace :kafka_batch do
        desc "Run the stuck-batch reconciler once"
        task reconcile: :environment do
          KafkaBatch::Reconciler.run
        end

        desc "Generate KafkaBatch migrations (MySQL store only)"
        task :install_migrations do
          source = File.expand_path("../../db/migrate", __dir__)
          dest   = Rails.root.join("db", "migrate")
          Dir["#{source}/*.rb"].sort.each do |file|
            base    = File.basename(file)
            target  = dest.join(base)
            if File.exist?(target)
              puts "  [skip] #{base} already exists"
            else
              FileUtils.cp(file, target)
              puts "  [copy] #{base}"
            end
          end
        end

        desc "Print all registered KafkaBatch workers"
        task workers: :environment do
          KafkaBatch.workers.each do |w|
            puts "  #{w.name} → topic: #{w.kafka_topic}  retries: #{w.max_retries}"
          end
        end
      end
    end

    # ── Generators ───────────────────────────────────────────────────────────
    generators do
      require_relative "../../lib/generators/kafka_batch/install/install_generator"
    end
  end
end

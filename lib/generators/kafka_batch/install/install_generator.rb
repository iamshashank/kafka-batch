require "rails/generators"

module KafkaBatch
  module Generators
    class InstallGenerator < Rails::Generators::Base
      source_root File.expand_path("templates", __dir__)
      namespace "kafka_batch:install"

      desc "Creates a KafkaBatch initializer and copies migrations (MySQL store)."

      class_option :store, type: :string, default: "mysql",
                           desc: "State store to use: mysql or redis"

      def create_initializer
        template "initializer.rb", "config/initializers/kafka_batch.rb"
      end

      def copy_migrations
        if options[:store] == "mysql"
          rake "kafka_batch:install_migrations"
        end
      end

      def show_karafka_instructions
        say "\n"
        say "Add KafkaBatch routes to your karafka.rb:", :green
        say <<~INSTRUCTIONS
          class KarafkaApp < Karafka::App
            routes.draw do
              KafkaBatch.draw_routes(self)
              # ... your other routes
            end
          end
        INSTRUCTIONS
      end
    end
  end
end

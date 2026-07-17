# frozen_string_literal: true

require "active_record"

module KafkaBatch
  # Bind ActiveRecord models to a host-app database connection.
  #
  # Supports:
  #   nil              — inherit ActiveRecord::Base's default connection
  #   Class            — copy connection from an AR model (e.g. connects_to :audit)
  #   Symbol / String  — Rails database.yml name (e.g. :kafka_batch_audit)
  #   Hash             — passed to establish_connection (url/adapter/host/…)
  #
  # +klass+ must be a *named* ActiveRecord::Base subclass — AR raises
  # "Anonymous class is not allowed." on establish_connection otherwise.
  module DatabaseConnection
    module_function

    def bind(klass, connection:)
      case connection
      when nil
        klass
      when Class
        unless connection < ActiveRecord::Base
          raise ConfigurationError,
                "database connection class must inherit ActiveRecord::Base (got #{connection})"
        end

        copy_from_class(connection, klass)
      when Symbol, String
        establish_named!(klass, connection.to_s)
      when Hash
        klass.establish_connection(connection)
      else
        raise ConfigurationError,
              "unsupported database connection type #{connection.class} " \
              "(expected nil, Class, Symbol, String, or Hash)"
      end

      klass
    end

    def copy_from_class(source_class, target_class)
      if source_class.respond_to?(:connection_db_config) && source_class.connection_db_config
        target_class.establish_connection(source_class.connection_db_config.configuration_hash)
      else
        target_class.establish_connection(source_class.connection_pool.db_config.configuration_hash)
      end
    rescue StandardError => e
      raise ConfigurationError,
            "could not copy database connection from #{source_class}: #{e.message}"
    end

    def establish_named!(klass, name)
      if defined?(Rails) && ActiveRecord::Base.respond_to?(:configurations)
        cfg = ActiveRecord::Base.configurations
        spec =
          if cfg.respond_to?(:find_db_config)
            cfg.find_db_config(name)
          elsif cfg.respond_to?(:configs_for)
            cfg.configs_for(env_name: Rails.env, name: name).first
          end

        if spec
          hash = spec.respond_to?(:configuration_hash) ? spec.configuration_hash : spec.config
          klass.establish_connection(hash)
          return klass
        end
      end

      raise ConfigurationError,
            "Unknown database connection #{name.inspect}. " \
            "Define it in database.yml or pass an ActiveRecord::Base subclass via " \
            "config.*_database_connection."
    end
  end
end

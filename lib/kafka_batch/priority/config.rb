# frozen_string_literal: true

require "yaml"

module KafkaBatch
  module Priority
    # One Sidekiq.yml-style priority group loaded from a YAML file.
    #
    #   consumer_group_suffix: jobs-fast
    #   mode: weighted          # weighted | strict
    #   weighted_interleave: 4  # optional — lower ranks proceed 1-in-N while higher lag
    #   topics:                 # highest priority first
    #     - kafka_batch.jobs.p0
    #     - kafka_batch.jobs.p1
    class Config
      MODES = %i[weighted strict].freeze

      attr_reader :path, :consumer_group_suffix, :consumer_group, :mode,
                  :topics, :weighted_interleave

      def initialize(path:, consumer_group_suffix:, consumer_group:, mode:,
                     topics:, weighted_interleave:)
        @path                   = path
        @consumer_group_suffix  = consumer_group_suffix
        @consumer_group         = consumer_group
        @mode                   = mode
        @topics                 = topics
        @weighted_interleave    = weighted_interleave
        freeze
      end

      # @param path [String]
      # @param cfg [KafkaBatch::Configuration]
      # @return [KafkaBatch::Priority::Config]
      def self.load(path, cfg: KafkaBatch.config)
        path = File.expand_path(path)
        raise ConfigurationError, "priority config not found: #{path}" unless File.file?(path)

        raw = YAML.safe_load(File.read(path), permitted_classes: [], aliases: true) || {}
        unless raw.is_a?(Hash)
          raise ConfigurationError, "priority config #{path} must be a YAML mapping"
        end

        suffix = raw["consumer_group_suffix"].to_s.strip
        if suffix.empty?
          raise ConfigurationError,
                "priority config #{path} requires consumer_group_suffix"
        end

        mode = raw.fetch("mode", "weighted").to_s.strip.downcase.to_sym
        unless MODES.include?(mode)
          raise ConfigurationError,
                "priority config #{path} mode must be weighted or strict (got #{raw['mode'].inspect})"
        end

        topic_list = Array(raw["topics"]).map { |t| t.to_s.strip }.reject(&:empty?)
        if topic_list.empty?
          raise ConfigurationError, "priority config #{path} requires a non-empty topics list"
        end

        if topic_list.uniq.length != topic_list.length
          dupes = topic_list.group_by(&:itself).select { |_, v| v.size > 1 }.keys
          raise ConfigurationError,
                "priority config #{path} lists duplicate topics: #{dupes.join(', ')}"
        end

        resolved_topics = topic_list.map { |t| cfg.resolve_topic(t) }

        jobs_topic = cfg.jobs_topic.to_s
        if resolved_topics.include?(jobs_topic)
          raise ConfigurationError,
                "priority config #{path} must not include the default jobs topic " \
                "(#{jobs_topic}) — that topic is always flat JobConsumer only"
        end

        interleave = (raw["weighted_interleave"] || cfg.priority_weighted_interleave).to_i
        interleave = 4 if interleave < 1

        new(
          path:                  path,
          consumer_group_suffix: suffix,
          consumer_group:        "#{cfg.consumer_group}-#{suffix}",
          mode:                  mode,
          topics:                resolved_topics,
          weighted_interleave:   interleave
        )
      end

      def rank_for(topic)
        topics.index(topic)
      end

      def higher_topics_for(topic)
        idx = rank_for(topic)
        return [] if idx.nil? || idx.zero?

        topics[0...idx]
      end
    end
  end
end

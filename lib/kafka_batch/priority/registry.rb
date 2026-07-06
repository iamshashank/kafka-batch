# frozen_string_literal: true

module KafkaBatch
  module Priority
    # Loads priority YAML files and validates that no topic is assigned to more
    # than one consumer group (prevents double consumption).
    class Registry
      attr_reader :configs

      def initialize(configs)
        @configs = configs
        validate!
      end

      # @param paths [Array<String>]
      # @param cfg [KafkaBatch::Configuration]
      def self.load(paths, cfg: KafkaBatch.config)
        paths = Array(paths).map { |p| p.to_s.strip }.reject(&:empty?).map { |p| File.expand_path(p) }.uniq
        return new([]) if paths.empty?

        configs = paths.map { |path| Config.load(path, cfg: cfg) }
        new(configs)
      end

      def empty?
        configs.empty?
      end

      # All priority topics across every group.
      def all_topics
        @all_topics ||= configs.flat_map(&:topics).uniq
      end

      # Topics that must NOT appear on the flat -jobs consumer group.
      def reserved_topics
        all_topics
      end

      def consumer_groups
        configs.map(&:consumer_group)
      end

      def config_for_topic(topic)
        configs.find { |c| c.topics.include?(topic) }
      end

      def validate_plain_topics!(plain_topics)
        overlap = Array(plain_topics) & reserved_topics
        return if overlap.empty?

        details = overlap.map do |topic|
          group = config_for_topic(topic)&.consumer_group
          "#{topic} (priority group #{group})"
        end
        raise ConfigurationError,
              "topic(s) cannot be on both a priority consumer group and flat -jobs: " \
              "#{details.join(', ')}"
      end

      private

      def validate!
        topic_to_group = {}
        group_to_path  = {}

        configs.each do |cfg|
          if group_to_path.key?(cfg.consumer_group)
            other = group_to_path[cfg.consumer_group]
            raise ConfigurationError,
                  "duplicate consumer group #{cfg.consumer_group} in #{cfg.path} and #{other}"
          end
          group_to_path[cfg.consumer_group] = cfg.path

          cfg.topics.each do |topic|
            if topic_to_group.key?(topic)
              raise ConfigurationError,
                    "topic #{topic} is assigned to multiple consumer groups " \
                    "(#{topic_to_group[topic]} and #{cfg.consumer_group}) — " \
                    "each topic may belong to exactly one group to prevent double consumption"
            end
            topic_to_group[topic] = cfg.consumer_group
          end
        end
      end
    end
  end
end

# Minimal Karafka routes builder stand-in for draw_routes specs.
module KafkaBatchSpec
  class RouteCapture
    attr_reader :groups, :consumers, :max_messages

    def initialize
      @groups       = {}
      @consumers    = {}
      @max_messages = {}
      @current_group = nil
      @current_topic = nil
    end

    def consumer_group(name, &block)
      @current_group = name
      @groups[name]  = []
      instance_eval(&block) if block
    ensure
      @current_group = nil
    end

    def topic(name, &block)
      raise "topic outside consumer_group" unless @current_group

      @current_topic = name
      @groups[@current_group] << name
      instance_eval(&block) if block
    ensure
      @current_topic = nil
    end

    def consumer(klass)
      @consumers[[@current_group, @current_topic]] = klass
    end

    def max_messages(count)
      @max_messages[[@current_group, @current_topic]] = count
    end
  end
end

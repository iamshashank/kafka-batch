RSpec.describe KafkaBatch::Worker do
  describe "class configuration" do
    it "exposes the configured topic" do
      expect(SuccessfulWorker.kafka_topic).to eq("test.success")
    end

    it "supports per-worker retry overrides" do
      expect(FailingWorker.max_retries).to eq(2)
      expect(FailingWorker.retry_backoff).to eq(7)
    end

    it "falls back to the global defaults when not overridden" do
      KafkaBatch.config.max_retries   = 9
      KafkaBatch.config.retry_backoff = 11
      expect(SuccessfulWorker.max_retries).to eq(9)
      expect(SuccessfulWorker.retry_backoff).to eq(11)
    end

    it "raises if the topic was never set" do
      klass = Class.new { include KafkaBatch::Worker }
      expect { klass.kafka_topic }.to raise_error(KafkaBatch::ConfigurationError)
    end
  end

  it "registers including classes in the global registry" do
    klass = Class.new { include KafkaBatch::Worker }
    expect(KafkaBatch.workers).to include(klass)
  end

  it "raises NotImplementedError when #perform is not overridden" do
    klass = Class.new { include KafkaBatch::Worker }
    expect { klass.new.perform({}) }.to raise_error(NotImplementedError)
  end
end

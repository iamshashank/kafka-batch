RSpec.describe KafkaBatch::Worker do
  describe "class configuration" do
    it "exposes the configured topic" do
      expect(SuccessfulWorker.kafka_topic).to eq("test.success")
    end

    it "supports a per-worker max_retries override" do
      expect(FailingWorker.max_retries).to eq(2)
    end

    it "falls back to the global max_retries default when not overridden" do
      KafkaBatch.config.max_retries = 9
      expect(SuccessfulWorker.max_retries).to eq(9)
    end

    it "defaults retry_tier to nil (uses the progression)" do
      expect(SuccessfulWorker.retry_tier).to be_nil
    end

    it "supports a per-worker retry_tier override" do
      expect(TierPinnedWorker.retry_tier).to eq(:large)
    end

    it "falls back to config.jobs_topic when no topic is set" do
      klass = Class.new { include KafkaBatch::Worker }
      expect(klass.kafka_topic).to eq(KafkaBatch.config.jobs_topic)
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

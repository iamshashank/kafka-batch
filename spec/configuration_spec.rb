RSpec.describe KafkaBatch::Configuration do
  subject(:config) { described_class.new }

  describe "defaults" do
    it "defaults to the mysql store" do
      expect(config.store).to eq(:mysql)
    end

    it "ships sane topic + retry defaults" do
      expect(config.jobs_topic).to eq("kafka_batch.jobs")
      expect(config.retry_topic).to eq("kafka_batch.jobs.retry")
      expect(config.max_retries).to eq(3)
      expect(config.retry_first_delay).to eq(10)
      expect(config.retry_delay).to eq(180)
      expect(config.retry_jitter).to eq(0.1)
    end

    it "decouples the reconciler lock TTL from the staleness threshold" do
      expect(config.reconciliation_interval).to eq(300)
      expect(config.reconciler_lock_ttl).to eq(600)
    end

    it "exposes configurable event-emission retry knobs" do
      expect(config.event_emit_retries).to eq(3)
      expect(config.event_emit_backoff).to eq(2)
    end
  end

  describe "#validate!" do
    it "passes with valid mysql config" do
      config.store   = :mysql
      config.brokers = ["localhost:9092"]
      expect { config.validate! }.not_to raise_error
    end

    it "rejects an unknown store" do
      config.store = :cassandra
      expect { config.validate! }.to raise_error(KafkaBatch::ConfigurationError, /mysql or :redis/)
    end

    it "rejects empty brokers" do
      config.brokers = []
      expect { config.validate! }.to raise_error(KafkaBatch::ConfigurationError, /brokers/)
    end

    it "requires a redis_url for the redis store" do
      config.store     = :redis
      config.redis_url = ""
      expect { config.validate! }.to raise_error(KafkaBatch::ConfigurationError, /redis_url/)
    end
  end
end

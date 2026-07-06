# frozen_string_literal: true

RSpec.describe KafkaBatch::Fairness::TenantPartitions do
  before do
    skip "Redis unavailable" unless KafkaBatchSpec::RedisHelper.available?

    KafkaBatch.config.redis_url = KafkaBatchSpec::RedisHelper::TEST_URL
    KafkaBatch.config.fairness_dynamic_tenant_partitions = true
    KafkaBatch.config.fairness_tenant_partition_cache_ttl = 30
    KafkaBatch.config.fairness_tenant_partitions = {}
    described_class.reset!
    KafkaBatchSpec::RedisHelper.flush!

    allow(KafkaBatch).to receive(:fairness_ingest_partition_count).and_return(4)
  end

  after { described_class.reset! }

  describe ".resolve" do
    it "checks out a dedicated partition for each new tenant" do
      p1 = described_class.resolve("acme", :time)
      p2 = described_class.resolve("globex", :time)
      expect(p1).to be_between(0, 3)
      expect(p2).to be_between(0, 3)
      expect(p1).not_to eq(p2)
    end

    it "returns the same partition on repeat lookup (idempotent)" do
      first = described_class.resolve("acme", :time)
      second = described_class.resolve("acme", :time)
      expect(second).to eq(first)
    end

    it "keeps time and throughput lanes independent" do
      time_p = described_class.resolve("acme", :time)
      tp_p   = described_class.resolve("acme", :throughput)
      expect(time_p).to be_between(0, 3)
      expect(tp_p).to be_between(0, 3)
      expect(described_class.all_assigned(:time)["acme"]).to eq(time_p.to_s)
      expect(described_class.all_assigned(:throughput)["acme"]).to eq(tp_p.to_s)
    end

    it "prefers config.fairness_tenant_partitions over dynamic checkout" do
      KafkaBatch.config.fairness_tenant_partitions = { "acme" => 3 }
      expect(described_class.resolve("acme", :time)).to eq(3)
      expect(described_class.all_assigned(:time)).not_to have_key("acme")
    end

    it "returns nil when dynamic mode is off and tenant is not pinned" do
      KafkaBatch.config.fairness_dynamic_tenant_partitions = false
      expect(described_class.resolve("acme", :time)).to be_nil
    end

    it "falls back to dynamic checkout when configured partition is out of range" do
      KafkaBatch.config.fairness_tenant_partitions = { "acme" => 99 }
      p = described_class.resolve("acme", :time)
      expect(p).to be_between(0, 3)
      expect(described_class.all_assigned(:time)["acme"]).to eq(p.to_s)
    end

    it "warns and returns nil when all partitions are assigned" do
      assigned = []
      %w[t1 t2 t3 t4].each { |t| assigned << described_class.resolve(t, :time) }
      expect(assigned.uniq.size).to eq(4)

      expect(KafkaBatch.logger).to receive(:warn).with(/no free ingest partitions/)
      expect(described_class.resolve("overflow", :time)).to be_nil
    end
  end

  describe ".warm!" do
    it "reconciles the free pool when the topic grows" do
      described_class.resolve("acme", :time)
      allow(KafkaBatch).to receive(:fairness_ingest_partition_count).and_return(6)

      described_class.warm!(:time)

      extra = described_class.resolve("newco", :time)
      expect(extra).to be_between(0, 5)
      expect(described_class.all_assigned(:time).size).to eq(2)
    end
  end

  describe "in-process cache" do
    it "avoids repeated Redis reads within the TTL" do
      cached = described_class.resolve("acme", :time)

      r = Redis.new(url: KafkaBatchSpec::RedisHelper::TEST_URL)
      r.hdel("kafka_batch:tenant_partitions:time", "acme")

      expect(described_class.resolve("acme", :time)).to eq(cached)
    end
  end

  describe "Batch.route_for integration" do
    it "produces to the checked-out partition with no key" do
      partition = described_class.resolve("acme", :time)
      route = KafkaBatch::Batch.route_for(FairWorker, job_id: "j1", tenant_id: "acme")
      expect(route[:topic]).to eq(KafkaBatch.config.fair_time_ingest_topic)
      expect(route[:partition]).to eq(partition)
      expect(route[:key]).to be_nil
    end
  end
end

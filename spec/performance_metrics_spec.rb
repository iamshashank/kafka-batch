RSpec.describe KafkaBatch::PerformanceMetrics do
  describe "when disabled (default)" do
    it "is disabled by default and record/available? no-op without raising" do
      expect(KafkaBatch.config.performance_metrics_enabled).to eq(false)
      expect(described_class.enabled?).to eq(false)
      expect(described_class.available?).to eq(false)
      expect { described_class.record(:processed, job_type: "W") }.not_to raise_error
    end

    it "install! does not subscribe when disabled" do
      described_class.install!
      expect(described_class.installed?).to eq(false)
    end
  end

  describe "with Redis available and enabled" do
    before do
      skip "Redis unavailable" unless KafkaBatchSpec::RedisHelper.available?
      KafkaBatch.config.redis_url                       = KafkaBatchSpec::RedisHelper::TEST_URL
      KafkaBatch.config.performance_metrics_enabled      = true
      KafkaBatch.config.performance_metrics_retention    = 3600
      KafkaBatch.config.performance_metrics_bucket_seconds = 60
      KafkaBatch.config.performance_metrics_max_job_types  = 50
      KafkaBatch.config.performance_metrics_sample_rate  = 1.0
      described_class.reset!
      KafkaBatchSpec::RedisHelper.flush!
    end

    it "reports enabled and available" do
      expect(described_class.enabled?).to eq(true)
      expect(described_class.available?).to eq(true)
    end

    it "increments the system total and job_type field on record(:processed)" do
      described_class.record(:processed, job_type: "FooWorker")
      described_class.record(:processed, job_type: "FooWorker")

      key = described_class.bucket_key(:processed)
      redis = Redis.new(url: KafkaBatchSpec::RedisHelper::TEST_URL)
      expect(redis.hget(key, "_all")).to eq("2")
      expect(redis.hget(key, "FooWorker")).to eq("2")
      expect(redis.ttl(key)).to be > 0
      expect(redis.ttl(key)).to be <= 3600
    end

    it "only increments the system total when job_type is nil (e.g. reclaim sweeps)" do
      described_class.record(:reclaimed, job_type: nil, count: 3)

      key = described_class.bucket_key(:reclaimed)
      redis = Redis.new(url: KafkaBatchSpec::RedisHelper::TEST_URL)
      expect(redis.hget(key, "_all")).to eq("3")
      expect(redis.hkeys(key)).to eq(["_all"])
    end

    it "folds job types past performance_metrics_max_job_types into _other" do
      KafkaBatch.config.performance_metrics_max_job_types = 1
      described_class.reset_known_job_types!

      described_class.record(:processed, job_type: "First")
      described_class.record(:processed, job_type: "Second")

      key = described_class.bucket_key(:processed)
      redis = Redis.new(url: KafkaBatchSpec::RedisHelper::TEST_URL)
      expect(redis.hget(key, "First")).to eq("1")
      expect(redis.hget(key, "Second")).to be_nil
      expect(redis.hget(key, "_other")).to eq("1")
    end

    it "ignores unknown statuses" do
      expect { described_class.record(:bogus, job_type: "W") }.not_to raise_error
      key = described_class.bucket_key(:processed)
      redis = Redis.new(url: KafkaBatchSpec::RedisHelper::TEST_URL)
      expect(redis.exists?(key)).to eq(false)
    end

    it "subscribes to job.processed / job.retried / job.failed / workset.reclaimed via install!" do
      described_class.install!
      expect(described_class.installed?).to eq(true)

      KafkaBatch::Instrumentation.job_processed(job_id: "j1", batch_id: "b1", worker_class: "MyWorker", duration: 0.1)
      KafkaBatch::Instrumentation.job_retried(job_id: "j2", batch_id: "b1", worker_class: "MyWorker", attempt: 1, next_attempt: 2)
      KafkaBatch::Instrumentation.job_failed(job_id: "j3", batch_id: "b1", worker_class: "MyWorker", attempt: 3, error: StandardError.new("boom"))
      KafkaBatch::Instrumentation.workset_reclaimed(checked: 5, reclaimed: 2, failed: 0, skipped: 3, duration: 0.02)

      redis = Redis.new(url: KafkaBatchSpec::RedisHelper::TEST_URL)
      expect(redis.hget(described_class.bucket_key(:processed), "_all")).to eq("1")
      expect(redis.hget(described_class.bucket_key(:retried), "_all")).to eq("1")
      expect(redis.hget(described_class.bucket_key(:failed), "_all")).to eq("1")
      expect(redis.hget(described_class.bucket_key(:reclaimed), "_all")).to eq("2")
    end

    it "does not raise or write when Redis is unreachable" do
      KafkaBatch.config.redis_url = "redis://127.0.0.1:6390/0" # nothing listening
      described_class.reset!
      expect(described_class.available?).to eq(false)
      expect { described_class.record(:processed, job_type: "W") }.not_to raise_error
    end
  end

  describe KafkaBatch::PerformanceMetrics::Reader do
    before do
      skip "Redis unavailable" unless KafkaBatchSpec::RedisHelper.available?
      KafkaBatch.config.redis_url                          = KafkaBatchSpec::RedisHelper::TEST_URL
      KafkaBatch.config.performance_metrics_enabled        = true
      KafkaBatch.config.performance_metrics_retention      = 3600
      KafkaBatch.config.performance_metrics_bucket_seconds = 60
      KafkaBatch::PerformanceMetrics.reset!
      KafkaBatchSpec::RedisHelper.flush!
    end

    it "aggregates totals and per-job-type rows for the requested range" do
      3.times { KafkaBatch::PerformanceMetrics.record(:processed, job_type: "FooWorker") }
      KafkaBatch::PerformanceMetrics.record(:failed, job_type: "FooWorker")
      KafkaBatch::PerformanceMetrics.record(:reclaimed, job_type: nil, count: 2)

      data = described_class.new.fetch(range: "5m")

      expect(data[:range]).to eq("5m")
      expect(data[:totals][:processed]).to eq(3)
      expect(data[:totals][:failed]).to eq(1)
      expect(data[:totals][:reclaimed]).to eq(2)
      expect(data[:points]).to be_an(Array)
      expect(data[:points].last[:processed]).to eq(3)

      row = data[:job_types].find { |r| r[:job_type] == "FooWorker" }
      expect(row).not_to be_nil
      expect(row[:processed]).to eq(3)
      expect(row[:failed]).to eq(1)
      expect(row[:sparkline]).to be_an(Array)
    end

    it "falls back to the default range for unknown range values" do
      data = described_class.new.fetch(range: "9d")
      expect(data[:range]).to eq(described_class::DEFAULT_RANGE)
    end

    it "restricts job_types to the requested list when given" do
      KafkaBatch::PerformanceMetrics.record(:processed, job_type: "Alpha")
      KafkaBatch::PerformanceMetrics.record(:processed, job_type: "Beta")

      data = described_class.new.fetch(range: "5m", job_types: ["Beta"])
      expect(data[:job_types].map { |r| r[:job_type] }).to eq(["Beta"])
    end

    it "downsamples the 24h range to far fewer than 1440 raw buckets" do
      data = described_class.new.fetch(range: "24h")
      expect(data[:points].size).to be < 1440
      expect(data[:bucket_seconds]).to be > 60
    end
  end
end

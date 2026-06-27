RSpec.describe KafkaBatch::Liveness do
  describe "with Redis available" do
    before do
      skip "Redis unavailable" unless KafkaBatchSpec::RedisHelper.available?
      KafkaBatch.config.redis_url          = KafkaBatchSpec::RedisHelper::TEST_URL
      KafkaBatch.config.track_running_jobs = true
      KafkaBatch.config.liveness_ttl       = 30
      described_class.reset!
      KafkaBatchSpec::RedisHelper.flush!
    end

    it "reports available" do
      expect(described_class.available?).to be(true)
    end

    it "records a running job and clears it on finish" do
      described_class.job_started(job_id: "j1", batch_id: "b1", worker_class: "W", topic: "t", partition: 0)
      running = described_class.running_jobs
      expect(running.map { |j| j["job_id"] }).to include("j1")
      expect(running.first["consumer_id"]).to eq(described_class.consumer_id)

      described_class.job_finished("j1")
      expect(described_class.running_jobs.map { |j| j["job_id"] }).not_to include("j1")
    end

    it "registers a consumer heartbeat" do
      described_class.heartbeat(topic: "test.success")
      consumers = described_class.consumers
      expect(consumers.map { |c| c["consumer_id"] }).to include(described_class.consumer_id)
      expect(consumers.first["pid"]).to eq(Process.pid)
    end

    it "no-ops when track_running_jobs is false" do
      KafkaBatch.config.track_running_jobs = false
      described_class.job_started(job_id: "j9", batch_id: "b1", worker_class: "W")
      KafkaBatch.config.track_running_jobs = true
      expect(described_class.running_jobs.map { |j| j["job_id"] }).not_to include("j9")
    end
  end

  describe "with the :store backend (heartbeat sampling, no Redis)" do
    before do
      KafkaBatch.config.liveness_backend = :store
      described_class.reset!
    end

    it "is available without Redis" do
      expect(described_class.available?).to be(true)
    end

    it "records a consumer heartbeat with the sampled current job" do
      described_class.job_started(job_id: "j1", batch_id: "b1", worker_class: "W", topic: "t", partition: 0)

      consumers = described_class.consumers
      expect(consumers.size).to eq(1)
      expect(consumers.first["consumer_id"]).to eq(described_class.consumer_id)

      running = described_class.running_jobs
      expect(running.map { |j| j["job_id"] }).to include("j1")
      expect(running.first["worker_class"]).to eq("W")
      expect(running.first["consumer_id"]).to eq(described_class.consumer_id)
    end
  end

  describe "when Redis is not reachable" do
    before do
      KafkaBatch.config.redis_url          = "redis://127.0.0.1:6390/0" # nothing listening
      KafkaBatch.config.track_running_jobs = true
      described_class.reset!
    end

    it "reports unavailable and never raises on writes" do
      expect(described_class.available?).to be(false)
      expect { described_class.job_started(job_id: "j1", batch_id: "b1", worker_class: "W") }.not_to raise_error
      expect { described_class.heartbeat(topic: "t") }.not_to raise_error
      expect(described_class.running_jobs).to eq([])
    end
  end
end

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
      KafkaBatch.config.liveness_backend            = :store
      KafkaBatch.config.liveness_heartbeat_interval = 5
      KafkaBatch.config.liveness_ttl                = 30
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

    it "throttles store writes to at most one per liveness_heartbeat_interval" do
      # Stub the clock BEFORE job_started so @last_heartbeat_at is set under
      # our controlled time (job_started also calls store_heartbeat internally).
      t = 0.0
      allow(Process).to receive(:clock_gettime).with(Process::CLOCK_MONOTONIC) { t }

      described_class.job_started(job_id: "j1", batch_id: "b1", worker_class: "W")
      # job_started called store_heartbeat at t=0 → @last_heartbeat_at = 0.0

      store = KafkaBatch.store
      # Advance past the interval so our first explicit call writes, then
      # confirm subsequent calls within the NEW interval are throttled.
      t = 6.0  # past the 5s interval
      expect(store).to receive(:record_heartbeat).once.and_call_original

      described_class.send(:store_heartbeat, topic: "t")  # writes at t=6
      t = 7.0  # within new interval → throttled
      described_class.send(:store_heartbeat, topic: "t")
      t = 8.0  # still throttled
      described_class.send(:store_heartbeat, topic: "t")
    end

    it "allows another write after the heartbeat interval elapses" do
      t = 0.0
      allow(Process).to receive(:clock_gettime).with(Process::CLOCK_MONOTONIC) { t }

      store = KafkaBatch.store
      # Two writes expected: one at t=0 and one at t=6 (past 5s interval).
      expect(store).to receive(:record_heartbeat).twice.and_call_original

      described_class.send(:store_heartbeat, topic: "t")  # write at t=0
      t = 6.0  # past the 5s interval
      described_class.send(:store_heartbeat, topic: "t")  # write again
    end

    it "running_jobs only returns heartbeats with a current_job_id set" do
      # Consumer 1: has a current job
      described_class.job_started(job_id: "j1", batch_id: "b1", worker_class: "W1",
                                  topic: "t", partition: 0)
      # Flush the heartbeat for consumer 1 immediately
      t = 0.0
      allow(Process).to receive(:clock_gettime).with(Process::CLOCK_MONOTONIC) { t }
      described_class.send(:store_heartbeat, topic: "t")

      # Consumer 2: idle (no current job). Simulate by writing a heartbeat
      # directly to the store without a job_id.
      consumer2_id = "idle-consumer-#{SecureRandom.hex(3)}"
      KafkaBatch.store.record_heartbeat(
        consumer2_id,
        hostname: "host2", pid: 9999, topic: "t", jobs_done: 0,
        current_job_id: nil, current_worker: nil,
        current_batch_id: nil, current_topic: nil, current_partition: nil
      )

      running = described_class.running_jobs
      consumers = described_class.consumers

      # Only the job-bearing consumer appears in running_jobs
      expect(running.map { |j| j["job_id"] }).to include("j1")
      expect(running.map { |j| j["consumer_id"] }).not_to include(consumer2_id)

      # Both appear in consumers (alive = has recent heartbeat)
      all_cids = consumers.map { |c| c["consumer_id"] }
      expect(all_cids).to include(described_class.consumer_id, consumer2_id)
    end

    it "jobs_done counter increments each time a job finishes" do
      # Stub clock first so @last_heartbeat_at is set under our controlled time.
      t = 0.0
      allow(Process).to receive(:clock_gettime).with(Process::CLOCK_MONOTONIC) { t }

      described_class.job_started(job_id: "j1", batch_id: "b1", worker_class: "W")
      # job_started triggered store_heartbeat at t=0 → @last_heartbeat_at = 0
      described_class.job_started(job_id: "j2", batch_id: "b1", worker_class: "W")
      # within interval, throttled
      described_class.job_finished("j1")
      described_class.job_finished("j2")

      # Advance past interval so the explicit heartbeat actually writes.
      t = 6.0
      described_class.send(:store_heartbeat, topic: "t")

      hb = KafkaBatch.store.list_heartbeats(Time.now - 60)
      expect(hb.first[:jobs_done]).to eq(2)
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

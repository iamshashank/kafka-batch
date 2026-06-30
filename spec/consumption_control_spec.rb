RSpec.describe KafkaBatch::ConsumptionControl do
  describe "redis backend" do
    before do
      skip "Redis unavailable" unless KafkaBatchSpec::RedisHelper.available?
      KafkaBatch.config.store     = :mysql
      KafkaBatch.config.redis_url = KafkaBatchSpec::RedisHelper::TEST_URL
      described_class.reset!
      KafkaBatchSpec::RedisHelper.flush!
    end

    it "pauses and resumes a whole topic" do
      described_class.pause_topic(group: "g", topic: "demo")
      expect(described_class.paused?(group: "g", topic: "demo", partition: 0)).to eq(true)
      expect(described_class.paused?(group: "g", topic: "demo", partition: 3)).to eq(true)

      described_class.resume_topic(group: "g", topic: "demo")
      expect(described_class.paused?(group: "g", topic: "demo", partition: 0)).to eq(false)
    end

    it "pauses and resumes a single partition" do
      described_class.pause_partition(group: "g", topic: "demo", partition: 2)
      expect(described_class.paused?(group: "g", topic: "demo", partition: 2)).to eq(true)
      expect(described_class.paused?(group: "g", topic: "demo", partition: 1)).to eq(false)

      described_class.resume_partition(group: "g", topic: "demo", partition: 2)
      expect(described_class.paused?(group: "g", topic: "demo", partition: 2)).to eq(false)
    end

    it "prefers redis over mysql when both are available" do
      expect(described_class.backend).to eq(:redis)
    end
  end

  describe "mysql backend" do
    before do
      KafkaBatch.config.store     = :mysql
      KafkaBatch.config.redis_url = ""
      described_class.reset!
    end

    it "pauses and resumes a whole topic" do
      expect(described_class.backend).to eq(:mysql)

      described_class.pause_topic(group: "g", topic: "demo")
      expect(described_class.paused?(group: "g", topic: "demo", partition: 0)).to eq(true)

      described_class.resume_topic(group: "g", topic: "demo")
      expect(described_class.paused?(group: "g", topic: "demo", partition: 0)).to eq(false)
    end

    it "pauses and resumes a single partition" do
      described_class.pause_partition(group: "g", topic: "demo", partition: 2)
      expect(described_class.paused?(group: "g", topic: "demo", partition: 2)).to eq(true)
      expect(described_class.paused?(group: "g", topic: "demo", partition: 1)).to eq(false)

      described_class.resume_partition(group: "g", topic: "demo", partition: 2)
      expect(described_class.paused?(group: "g", topic: "demo", partition: 2)).to eq(false)
    end
  end

  # ── Backend probe cache (30s TTL, separate backend_mutex) ────────────────
  describe "backend probe cache" do
    before do
      skip "Redis unavailable" unless KafkaBatchSpec::RedisHelper.available?
      KafkaBatch.config.redis_url = KafkaBatchSpec::RedisHelper::TEST_URL
      described_class.reset!
    end

    it "caches the backend probe and calls detect_backend only once within 30s" do
      call_count = 0
      allow(described_class).to receive(:detect_backend).and_wrap_original do |orig|
        call_count += 1
        orig.call
      end

      t = 0.0
      allow(Process).to receive(:clock_gettime).with(Process::CLOCK_MONOTONIC) { t }

      described_class.backend   # first call at t=0 → detect_backend called
      t = 10.0
      described_class.backend   # within 30s → cached
      t = 20.0
      described_class.backend   # still within 30s → cached

      expect(call_count).to eq(1)
    end

    it "re-probes after the 30s cache window expires" do
      call_count = 0
      allow(described_class).to receive(:detect_backend).and_wrap_original do |orig|
        call_count += 1
        orig.call
      end

      t = 0.0
      allow(Process).to receive(:clock_gettime).with(Process::CLOCK_MONOTONIC) { t }

      described_class.backend   # probe at t=0
      t = 31.0
      described_class.backend   # TTL expired → re-probe

      expect(call_count).to eq(2)
    end

    it "uses a dedicated backend_mutex distinct from cache_mutex to prevent deadlock" do
      # If backend_mutex == cache_mutex, calling backend from inside cached_snapshot
      # (which holds cache_mutex) would deadlock on MRI due to non-reentrant Mutex.
      # We verify they are different objects.
      b_mutex = described_class.send(:backend_mutex)
      c_mutex = described_class.send(:cache_mutex)
      expect(b_mutex).not_to equal(c_mutex)
    end

    it "does not deadlock when cached_snapshot calls load_snapshot which calls backend" do
      # Exercise the call chain that was previously a deadlock risk:
      # paused? → cached_snapshot (holds cache_mutex) → load_snapshot → backend (needs backend_mutex)
      described_class.pause_topic(group: "g", topic: "t")
      expect { described_class.paused?(group: "g", topic: "t", partition: 0) }.not_to raise_error
    end
  end

  describe "consumer snapshot cache" do
    before do
      skip "Redis unavailable" unless KafkaBatchSpec::RedisHelper.available?
      KafkaBatch.config.redis_url = KafkaBatchSpec::RedisHelper::TEST_URL
      KafkaBatch.config.consumption_control_refresh_interval = 60
      described_class.reset!
      KafkaBatchSpec::RedisHelper.flush!
    end

    it "reuses the cached snapshot until the refresh interval elapses" do
      described_class.pause_topic(group: "g", topic: "demo")
      expect(described_class.paused?(group: "g", topic: "demo", partition: 0)).to eq(true)

      # Simulate another process resuming without invalidating this process's cache.
      described_class.send(:redis_resume_topic, "g", "demo")
      expect(described_class.paused?(group: "g", topic: "demo", partition: 0)).to eq(true)

      snap = described_class.snapshot(refresh: true)
      expect(snap[:topics]).not_to include(described_class.topic_key("g", "demo"))
    end

    it "reloads after consumption_control_refresh_interval seconds" do
      t = 0.0
      allow(Process).to receive(:clock_gettime).with(Process::CLOCK_MONOTONIC) { t }

      described_class.pause_topic(group: "g", topic: "demo")
      expect(described_class.paused?(group: "g", topic: "demo", partition: 0)).to eq(true)

      described_class.send(:redis_resume_topic, "g", "demo")
      expect(described_class.paused?(group: "g", topic: "demo", partition: 0)).to eq(true)

      t = 61.0
      expect(described_class.paused?(group: "g", topic: "demo", partition: 0)).to eq(false)
    end
  end
end

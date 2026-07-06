RSpec.describe KafkaBatch::Reconciler do
  let(:store) { KafkaBatch.store }

  before do
    skip "Redis unavailable" unless KafkaBatchSpec::RedisHelper.available?
    KafkaBatch.reset!
    KafkaBatch.configure do |c|
      c.store     = :redis
      c.redis_url = KafkaBatchSpec::RedisHelper::TEST_URL
      c.batch_ttl = 3600
    end
    KafkaBatchSpec::RedisHelper.flush!

    allow(KafkaBatch.store).to receive(:with_reconciler_lock).and_yield
  end

  describe "stuck-running recovery" do
    it "transitions a fully-finished but still-running batch and fires its callback" do
      id = SecureRandom.uuid
      store.create_batch(id: id, total_jobs: 1, on_complete: "RecordingCallback")
      KafkaBatchSpec::RedisHelper.simulate_stuck_running!(id, completed_count: 1)

      described_class.run

      expect(store.find_batch(id)[:status]).to eq("success")
      cb = FakeProducer.for_topic(KafkaBatch.config.callbacks_topic)
      expect(cb.size).to eq(1)
      expect(cb.first.payload["batch_id"]).to eq(id)
    end

    it "stamps finished_at so a re-lost callback stays recoverable" do
      id = SecureRandom.uuid
      store.create_batch(id: id, total_jobs: 1, on_complete: "RecordingCallback")
      KafkaBatchSpec::RedisHelper.simulate_stuck_running!(id, completed_count: 1)

      described_class.run

      batch = store.find_batch(id)
      expect(batch[:finished_at]).not_to be_nil
      expect(batch[:callback_dispatched_at]).to be_nil

      recoverable = store.done_batches_without_callback(older_than: Time.now + 60)
      expect(recoverable.map { |b| b[:id] }).to include(id)
    end

    it "leaves a genuinely-incomplete batch alone" do
      id = SecureRandom.uuid
      store.create_batch(id: id, total_jobs: 5)
      KafkaBatchSpec::RedisHelper.simulate_stuck_running!(id, completed_count: 1)

      described_class.run

      expect(store.find_batch(id)[:status]).to eq("running")
      expect(FakeProducer.for_topic(KafkaBatch.config.callbacks_topic)).to be_empty
    end
  end

  describe "lost-callback recovery" do
    it "re-produces a callback for a finished batch whose callback never dispatched" do
      id = SecureRandom.uuid
      store.create_batch(id: id, total_jobs: 1, on_complete: "RecordingCallback")
      KafkaBatchSpec::RedisHelper.simulate_lost_callback!(id)

      described_class.run

      cb = FakeProducer.for_topic(KafkaBatch.config.callbacks_topic)
      expect(cb.size).to eq(1)
      expect(cb.first.payload["reconciled"]).to be(true)
    end
  end

  # ── Per-run cap (max_reconcile_per_run) ──────────────────────────────────
  describe "max_reconcile_per_run cap" do
    it "processes at most max_reconcile_per_run stuck-running batches per sweep" do
      KafkaBatch.config.max_reconcile_per_run = 3
      cap = KafkaBatch.config.max_reconcile_per_run

      ids = Array.new(cap + 2) do
        id = SecureRandom.uuid
        store.create_batch(id: id, total_jobs: 1, on_complete: "RecordingCallback")
        KafkaBatchSpec::RedisHelper.simulate_stuck_running!(id, completed_count: 1)
        id
      end

      described_class.run

      callbacks_produced = FakeProducer.for_topic(KafkaBatch.config.callbacks_topic).size
      expect(callbacks_produced).to eq(cap)

      still_running = ids.count { |id| store.find_batch(id)[:status] == "running" }
      expect(still_running).to eq(2)
    end

    it "processes at most max_reconcile_per_run lost-callback batches per sweep" do
      KafkaBatch.config.max_reconcile_per_run = 2
      cap = KafkaBatch.config.max_reconcile_per_run

      Array.new(cap + 2) do
        id = SecureRandom.uuid
        store.create_batch(id: id, total_jobs: 1, on_complete: "RecordingCallback")
        KafkaBatchSpec::RedisHelper.simulate_lost_callback!(id)
      end

      described_class.run

      expect(FakeProducer.for_topic(KafkaBatch.config.callbacks_topic).size).to eq(cap)
    end

    it "emits a warning log when capping the stuck-running sweep" do
      KafkaBatch.config.max_reconcile_per_run = 1

      2.times do
        id = SecureRandom.uuid
        store.create_batch(id: id, total_jobs: 1, on_complete: "RecordingCallback")
        KafkaBatchSpec::RedisHelper.simulate_stuck_running!(id, completed_count: 1)
      end

      allow(KafkaBatch.logger).to receive(:warn)
      expect(KafkaBatch.logger).to receive(:warn).with(/processing first 1/i)
      described_class.run
    end

    it "processes all batches when count is below the cap" do
      KafkaBatch.config.max_reconcile_per_run = 100

      2.times do
        id = SecureRandom.uuid
        store.create_batch(id: id, total_jobs: 1, on_complete: "RecordingCallback")
        KafkaBatchSpec::RedisHelper.simulate_stuck_running!(id, completed_count: 1)
      end

      described_class.run

      expect(FakeProducer.for_topic(KafkaBatch.config.callbacks_topic).size).to eq(2)
    end

    it "persists a run summary for the dashboard" do
      id = SecureRandom.uuid
      store.create_batch(id: id, total_jobs: 1, on_complete: "RecordingCallback")
      KafkaBatchSpec::RedisHelper.simulate_stuck_running!(id, completed_count: 1)

      described_class.run(triggered_by: :rake)

      last = KafkaBatch::Reconciler::RunSummary.load_last
      expect(last).not_to be_nil
      expect(last[:triggered_by]).to eq("rake")
      expect(last[:recovered_stale].to_i).to eq(1)
    end

    it "records lock skips when the reconciler lock is held" do
      Redis.new(url: KafkaBatchSpec::RedisHelper::TEST_URL).set(
        "kafka_batch:b:reconciler_lock", "other", nx: true, ex: 300
      )
      allow(KafkaBatch.store).to receive(:with_reconciler_lock).and_call_original

      expect(described_class.run).to eq(:lock_skipped)

      skip = KafkaBatch::Reconciler::RunSummary.load_skip
      expect(skip[:reason]).to eq("lock_held")
    end
  end

  # ── with_reconciler_lock (Redis-backed for all store modes) ──────────────
  describe "with_reconciler_lock" do
    subject(:redis_store) { KafkaBatch::Stores::RedisStore.new }

    before do
      allow(KafkaBatch.store).to receive(:with_reconciler_lock).and_call_original
    end

    it "yields the block and returns its value" do
      result = redis_store.with_reconciler_lock { 42 }
      expect(result).to eq(42)
    end

    it "releases the lock after the block even if the block raises" do
      expect do
        redis_store.with_reconciler_lock { raise "boom" }
      end.not_to raise_error

      executed = false
      redis_store.with_reconciler_lock { executed = true }
      expect(executed).to be(true)
    end

    it "does not yield when another holder already owns the lock" do
      Redis.new(url: KafkaBatchSpec::RedisHelper::TEST_URL).set(
        "kafka_batch:b:reconciler_lock", "other", nx: true, ex: 300
      )

      calls = []
      redis_store.with_reconciler_lock { calls << :ran }
      expect(calls).to be_empty
    end
  end
end

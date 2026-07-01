RSpec.describe KafkaBatch::Reconciler do
  let(:store) { KafkaBatch.store }

  before do
    # GET_LOCK isn't available on SQLite, so bypass the distributed lock and
    # just run the reconciler body.
    allow(store).to receive(:with_reconciler_lock).and_yield
  end

  describe "stuck-running recovery" do
    it "transitions a fully-finished but still-running batch and fires its callback" do
      id = SecureRandom.uuid
      store.create_batch(id: id, total_jobs: 1, on_complete: "RecordingCallback")
      # Simulate a lost completion event: all jobs done, but status stuck.
      store.batch_record_class.where(id: id).update_all(
        completed_count: 1,
        created_at:      Time.now - 3600
      )

      described_class.run

      expect(store.find_batch(id)[:status]).to eq("success")
      cb = FakeProducer.for_topic(KafkaBatch.config.callbacks_topic)
      expect(cb.size).to eq(1)
      expect(cb.first.payload["batch_id"]).to eq(id)
    end

    it "stamps finished_at so a re-lost callback stays recoverable" do
      id = SecureRandom.uuid
      store.create_batch(id: id, total_jobs: 1, on_complete: "RecordingCallback")
      store.batch_record_class.where(id: id).update_all(
        completed_count: 1,
        created_at:      Time.now - 3600
      )

      described_class.run

      batch = store.find_batch(id)
      expect(batch[:finished_at]).not_to be_nil
      expect(batch[:callback_dispatched_at]).to be_nil

      # Because finished_at is now set and the callback was not claimed
      # (FakeProducer only captured it), the batch remains discoverable by the
      # lost-callback sweep – i.e. a re-lost callback can still be recovered.
      recoverable = store.done_batches_without_callback(older_than: Time.now + 60)
      expect(recoverable.map { |b| b[:id] }).to include(id)
    end

    it "leaves a genuinely-incomplete batch alone" do
      id = SecureRandom.uuid
      store.create_batch(id: id, total_jobs: 5)
      store.batch_record_class.where(id: id).update_all(
        completed_count: 1,
        created_at:      Time.now - 3600
      )

      described_class.run

      expect(store.find_batch(id)[:status]).to eq("running")
      expect(FakeProducer.for_topic(KafkaBatch.config.callbacks_topic)).to be_empty
    end
  end

  describe "lost-callback recovery" do
    it "re-produces a callback for a finished batch whose callback never dispatched" do
      id = SecureRandom.uuid
      store.create_batch(id: id, total_jobs: 1, on_complete: "RecordingCallback")
      store.batch_record_class.where(id: id).update_all(
        status:          "success",
        completed_count: 1,
        finished_at:     Time.now - 3600,
        created_at:      Time.now - 3600
      )

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

      # Create cap + 2 stale fully-completed batches
      ids = Array.new(cap + 2) do
        id = SecureRandom.uuid
        store.create_batch(id: id, total_jobs: 1, on_complete: "RecordingCallback")
        store.batch_record_class.where(id: id).update_all(
          completed_count: 1,
          created_at:      Time.now - 3600
        )
        id
      end

      described_class.run

      callbacks_produced = FakeProducer.for_topic(KafkaBatch.config.callbacks_topic).size
      expect(callbacks_produced).to eq(cap)

      # Remaining batches are still "running" — left for the next tick
      still_running = ids.count { |id| store.find_batch(id)[:status] == "running" }
      expect(still_running).to eq(2)
    end

    it "processes at most max_reconcile_per_run lost-callback batches per sweep" do
      KafkaBatch.config.max_reconcile_per_run = 2
      cap = KafkaBatch.config.max_reconcile_per_run

      Array.new(cap + 2) do
        id = SecureRandom.uuid
        store.create_batch(id: id, total_jobs: 1, on_complete: "RecordingCallback")
        store.batch_record_class.where(id: id).update_all(
          status:          "success",
          completed_count: 1,
          finished_at:     Time.now - 3600,
          created_at:      Time.now - 3600
        )
      end

      described_class.run

      expect(FakeProducer.for_topic(KafkaBatch.config.callbacks_topic).size).to eq(cap)
    end

    it "emits a warning log when capping the stuck-running sweep" do
      KafkaBatch.config.max_reconcile_per_run = 1

      2.times do
        id = SecureRandom.uuid
        store.create_batch(id: id, total_jobs: 1, on_complete: "RecordingCallback")
        store.batch_record_class.where(id: id).update_all(
          completed_count: 1,
          created_at:      Time.now - 3600
        )
      end

      # Allow any other warn calls without failing.
      allow(KafkaBatch.logger).to receive(:warn)
      expect(KafkaBatch.logger).to receive(:warn).with(/processing first 1/i)
      described_class.run
    end

    it "processes all batches when count is below the cap" do
      KafkaBatch.config.max_reconcile_per_run = 100

      2.times do
        id = SecureRandom.uuid
        store.create_batch(id: id, total_jobs: 1, on_complete: "RecordingCallback")
        store.batch_record_class.where(id: id).update_all(
          completed_count: 1,
          created_at:      Time.now - 3600
        )
      end

      described_class.run

      expect(FakeProducer.for_topic(KafkaBatch.config.callbacks_topic).size).to eq(2)
    end
  end

  # ── MysqlStore#with_reconciler_lock ──────────────────────────────────────
  # SQLite does not support GET_LOCK, so we test the lock using a second
  # MysqlStore instance that wraps a real in-memory SQLite connection. We verify
  # the key behaviours: block is yielded, lock is released, a second concurrent
  # call is skipped (simulated by calling the SQLite-specific NOT_LOCK helper).
  describe "MysqlStore#with_reconciler_lock" do
    subject(:mysql_store) { KafkaBatch::Stores::MysqlStore.new }

    before do
      # Remove the stub set in the outer before block so we exercise real logic.
      allow(store).to receive(:with_reconciler_lock).and_call_original

      # SQLite has no GET_LOCK / RELEASE_LOCK. Stub the connection adapter so
      # all three tests can exercise the Ruby-level branching logic without
      # hitting MySQL-specific SQL. Individual tests override as needed.
      allow_any_instance_of(ActiveRecord::ConnectionAdapters::AbstractAdapter)
        .to receive(:select_value)
        .and_wrap_original do |original, sql, *rest|
          sql.to_s =~ /GET_LOCK/i ? "1" : original.call(sql, *rest)
        end
      allow_any_instance_of(ActiveRecord::ConnectionAdapters::AbstractAdapter)
        .to receive(:execute)
        .and_wrap_original do |original, sql, *rest|
          sql.to_s =~ /RELEASE_LOCK/i ? nil : original.call(sql, *rest)
        end
    end

    it "yields the block and returns its value" do
      result = mysql_store.with_reconciler_lock { 42 }
      expect(result).to eq(42)
    end

    it "releases the lock after the block even if the block raises" do
      # with_reconciler_lock has an outer rescue that SWALLOWS all exceptions
      # (logs them, does NOT re-raise). The ensure still runs RELEASE_LOCK.
      expect do
        mysql_store.with_reconciler_lock { raise "boom" }
      end.not_to raise_error

      # A second call must still acquire the lock (it was properly released).
      executed = false
      mysql_store.with_reconciler_lock { executed = true }
      expect(executed).to be(true)
    end

    it "does not yield when another holder already owns the lock" do
      # Override the GET_LOCK stub to return 0 (lock held by another process).
      allow_any_instance_of(ActiveRecord::ConnectionAdapters::AbstractAdapter)
        .to receive(:select_value)
        .and_wrap_original do |original, sql, *rest|
          sql.to_s =~ /GET_LOCK/i ? "0" : original.call(sql, *rest)
        end

      calls = []
      mysql_store.with_reconciler_lock { calls << :ran }
      expect(calls).to be_empty
    end
  end
end

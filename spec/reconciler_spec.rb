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
end

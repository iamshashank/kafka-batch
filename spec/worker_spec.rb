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

    it "defaults fairness to false" do
      expect(SuccessfulWorker.fairness?).to eq(false)
    end

    it "supports a per-worker fairness opt-in" do
      expect(FairWorker.fairness?).to eq(true)
    end

    it "supports a per-worker retry_tier override" do
      expect(TierPinnedWorker.retry_tier).to eq(:large)
    end

    it "falls back to config.jobs_topic when no topic is set" do
      klass = Class.new { include KafkaBatch::Worker }
      expect(klass.kafka_topic).to eq(KafkaBatch.config.jobs_topic)
    end

    describe "complete_after_retries" do
      it "falls back to the global config default when not overridden" do
        KafkaBatch.config.complete_after_retries = 7
        klass = Class.new { include KafkaBatch::Worker }
        expect(klass.complete_after_retries).to eq(7)
      end

      it "can be pinned per worker, independently of max_retries" do
        klass = Class.new do
          include KafkaBatch::Worker
          complete_after_retries 1
        end
        expect(klass.complete_after_retries).to eq(1)
      end
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

  # ── Instance helpers (kafka_batch_id / batch) ─────────────────────────────
  describe "#batch instance helper" do
    it "returns nil when kafka_batch_id is nil" do
      worker = SuccessfulWorker.new
      worker.kafka_batch_id = nil
      expect(worker.batch).to be_nil
    end

    it "returns nil when kafka_batch_id is an empty string" do
      worker = SuccessfulWorker.new
      worker.kafka_batch_id = ""
      expect(worker.batch).to be_nil
    end

    it "returns a Batch with the correct id when kafka_batch_id is set" do
      id = SecureRandom.uuid
      KafkaBatch.store.create_batch(id: id, total_jobs: 1)

      worker = SuccessfulWorker.new
      worker.kafka_batch_id = id
      b = worker.batch
      expect(b).to be_a(KafkaBatch::Batch)
      expect(b.id).to eq(id)
    end

    it "memoizes the Batch on repeated calls (no extra store round-trips)" do
      id = SecureRandom.uuid
      KafkaBatch.store.create_batch(id: id, total_jobs: 1)

      worker = SuccessfulWorker.new
      worker.kafka_batch_id = id
      expect(worker.batch).to equal(worker.batch)  # same object identity
    end
  end
end

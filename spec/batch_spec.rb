RSpec.describe KafkaBatch::Batch do
  describe ".create" do
    it "writes the store record with the exact job count before producing" do
      id = described_class.create(on_complete: "RecordingCallback") do |b|
        b.push(SuccessfulWorker, { "user_id" => 1 })
        b.push(SuccessfulWorker, { "user_id" => 2 })
      end

      batch = KafkaBatch.store.find_batch(id)
      expect(batch[:total_jobs]).to eq(2)
      expect(batch[:status]).to eq("running")
      expect(batch[:on_complete]).to eq("RecordingCallback")
    end

    it "produces one job message per push, tagged with the batch id" do
      id = described_class.create do |b|
        b.push(SuccessfulWorker, { "user_id" => 1 })
        b.push(SuccessfulWorker, { "user_id" => 2 })
      end

      produced = FakeProducer.for_topic("test.success")
      expect(produced.size).to eq(2)
      expect(produced.map { |m| m.payload["batch_id"] }.uniq).to eq([id])
      expect(produced.first.payload["worker_class"]).to eq("SuccessfulWorker")
      expect(produced.first.payload["attempt"]).to eq(0)
      expect(produced.first.payload["max_retries"]).to eq(SuccessfulWorker.max_retries)
    end

    it "skips empty batches without writing a record" do
      id = described_class.create { |_b| }
      expect(KafkaBatch.store.find_batch(id)).to be_nil
      expect(FakeProducer.messages).to be_empty
    end

    it "rolls back the store record if producing fails midway" do
      FakeProducer.raise_for { |topic| topic == "test.success" }

      expect {
        described_class.create do |b|
          b.push(SuccessfulWorker, { "user_id" => 1 })
        end
      }.to raise_error(KafkaBatch::ProducerError)

      # The rolled-back batch must not linger in "running".
      expect(KafkaBatch.store.batch_record_class.count).to eq(0)
    end
  end

  describe ".push validation" do
    it "rejects classes that don't include KafkaBatch::Worker" do
      batch = described_class.new
      expect { batch.push(NotAWorker) }.to raise_error(ArgumentError, /must include/)
    end
  end

  describe ".enqueue" do
    it "produces a single standalone job with a nil batch_id" do
      job_id = described_class.enqueue(SuccessfulWorker, { "user_id" => 7 })

      produced = FakeProducer.for_topic("test.success")
      expect(produced.size).to eq(1)
      expect(produced.first.payload["batch_id"]).to be_nil
      expect(produced.first.payload["job_id"]).to eq(job_id)
    end
  end

  describe ".reenqueue" do
    it "re-produces the message with the next attempt number" do
      described_class.reenqueue(
        topic:        "test.success",
        message:      { "job_id" => "j1", "attempt" => 1, "payload" => {} },
        next_attempt: 2
      )

      msg = FakeProducer.for_topic("test.success").first
      expect(msg.payload["attempt"]).to eq(2)
      expect(msg.key).to eq("j1")
    end
  end

  describe ".cancel" do
    it "sets the batch status to cancelled" do
      id = described_class.create { |b| b.push(SuccessfulWorker, {}) }
      described_class.cancel(id)
      expect(KafkaBatch.store.find_batch(id)[:status]).to eq("cancelled")
    end
  end
end

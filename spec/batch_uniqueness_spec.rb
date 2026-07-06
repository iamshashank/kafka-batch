# frozen_string_literal: true

RSpec.describe "KafkaBatch::Batch job uniqueness" do
  before do
    skip "Redis not available" unless KafkaBatchSpec::RedisHelper.available?
    KafkaBatchSpec::RedisHelper.flush!
    FakeProducer.reset!
  end

  describe ".enqueue" do
    it "returns a job_id for the first enqueue and nil for a duplicate" do
      first = KafkaBatch::Batch.enqueue(UniqWorker, { "id" => 42 })
      dup   = KafkaBatch::Batch.enqueue(UniqWorker, { "id" => 42 })

      expect(first).to be_a(String)
      expect(dup).to be_nil
      expect(FakeProducer.messages.size).to eq(1)
    end

    it "allows the same payload after the lock is released" do
      jid = KafkaBatch::Batch.enqueue(UniqWorker, { "id" => 7 })
      KafkaBatch::Uniqueness.release(UniqWorker, { "id" => 7 }, job_id: jid)

      second = KafkaBatch::Batch.enqueue(UniqWorker, { "id" => 7 })
      expect(second).to be_a(String)
      expect(FakeProducer.messages.size).to eq(2)
    end

    it "raises DuplicateJobError when config.uniq_on_duplicate is :raise" do
      KafkaBatch.config.uniq_on_duplicate = :raise
      KafkaBatch::Batch.enqueue(UniqWorker, { "id" => 1 })

      expect {
        KafkaBatch::Batch.enqueue(UniqWorker, { "id" => 1 })
      }.to raise_error(KafkaBatch::DuplicateJobError)
    ensure
      KafkaBatch.config.uniq_on_duplicate = :skip
    end
  end

  describe "#push" do
    it "skips duplicate jobs without growing total_jobs" do
      batch = KafkaBatch::Batch.create
      first = batch.push(UniqWorker, { "n" => 1 })
      dup   = batch.push(UniqWorker, { "n" => 1 })

      expect(first).to be_a(String)
      expect(dup).to be_nil
      expect(KafkaBatch.store.find_batch(batch.id)[:total_jobs]).to eq(1)
      expect(FakeProducer.messages.size).to eq(1)
    end
  end

  describe "#push_many" do
    it "returns nil slots for duplicates and enqueues only unique payloads" do
      batch = KafkaBatch::Batch.create
      ids   = batch.push_many(UniqWorker, [{ "n" => 1 }, { "n" => 1 }, { "n" => 2 }])

      expect(ids[0]).to be_a(String)
      expect(ids[1]).to be_nil
      expect(ids[2]).to be_a(String)
      expect(KafkaBatch.store.find_batch(batch.id)[:total_jobs]).to eq(2)
      expect(FakeProducer.messages.size).to eq(2)
    end
  end
end

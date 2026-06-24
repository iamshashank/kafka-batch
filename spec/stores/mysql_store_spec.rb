RSpec.describe KafkaBatch::Stores::MysqlStore do
  subject(:store) { described_class.new }

  def new_batch(id: SecureRandom.uuid, total: 2, **opts)
    store.create_batch(id: id, total_jobs: total, **opts)
    id
  end

  describe "#create_batch / #find_batch" do
    it "persists and round-trips batch fields including meta" do
      id = new_batch(total: 3, on_success: "S", on_complete: "C", meta: { "k" => "v" })
      batch = store.find_batch(id)

      expect(batch[:total_jobs]).to eq(3)
      expect(batch[:status]).to eq("running")
      expect(batch[:on_success]).to eq("S")
      expect(batch[:meta]).to eq("k" => "v")
    end

    it "is idempotent on duplicate id" do
      id = new_batch
      expect { store.create_batch(id: id, total_jobs: 2) }.not_to raise_error
      expect(store.batch_record_class.count).to eq(1)
    end
  end

  describe "#record_job_completion" do
    it "returns :continue while jobs remain" do
      id = new_batch(total: 2)
      result = store.record_job_completion(batch_id: id, job_id: "j1", status: "success")
      expect(result[:status]).to eq(:continue)
    end

    it "dedups repeated completions for the same job" do
      id = new_batch(total: 2)
      store.record_job_completion(batch_id: id, job_id: "j1", status: "success")
      dup = store.record_job_completion(batch_id: id, job_id: "j1", status: "success")
      expect(dup[:status]).to eq(:duplicate)
    end

    it "marks the batch :done with outcome success when all jobs succeed" do
      id = new_batch(total: 2)
      store.record_job_completion(batch_id: id, job_id: "j1", status: "success")
      result = store.record_job_completion(batch_id: id, job_id: "j2", status: "success")

      expect(result[:status]).to eq(:done)
      expect(result[:outcome]).to eq("success")
      expect(store.find_batch(id)[:status]).to eq("success")
    end

    it "marks the batch :done with outcome complete when any job fails" do
      id = new_batch(total: 2)
      store.record_job_completion(batch_id: id, job_id: "j1", status: "success")
      result = store.record_job_completion(batch_id: id, job_id: "j2", status: "failed")

      expect(result[:status]).to eq(:done)
      expect(result[:outcome]).to eq("complete")
    end

    it "returns :not_found for an unknown batch" do
      result = store.record_job_completion(batch_id: "nope", job_id: "j1", status: "success")
      expect(result[:status]).to eq(:not_found)
    end
  end

  describe "#claim_callback / #callback_dispatched?" do
    it "lets exactly one caller win the claim" do
      id = new_batch
      expect(store.callback_dispatched?(id)).to be(false)
      expect(store.claim_callback(id)).to be(true)
      expect(store.claim_callback(id)).to be(false)
      expect(store.callback_dispatched?(id)).to be(true)
    end
  end

  describe "reconciler queries" do
    it "#stale_batches returns running batches older than the threshold" do
      id = new_batch
      stale = store.stale_batches(older_than: Time.now + 60)
      expect(stale.map { |b| b[:id] }).to include(id)
    end

    it "#done_batches_without_callback finds finished, unclaimed batches" do
      id = new_batch(total: 1)
      store.record_job_completion(batch_id: id, job_id: "j1", status: "success")

      lost = store.done_batches_without_callback(older_than: Time.now + 60)
      expect(lost.map { |b| b[:id] }).to include(id)

      store.claim_callback(id)
      after = store.done_batches_without_callback(older_than: Time.now + 60)
      expect(after.map { |b| b[:id] }).not_to include(id)
    end
  end

  describe "#delete_batch" do
    it "removes the batch and its job completions" do
      id = new_batch(total: 2)
      store.record_job_completion(batch_id: id, job_id: "j1", status: "success")
      store.delete_batch(id)

      expect(store.find_batch(id)).to be_nil
      expect(store.job_completion_class.where(batch_id: id).count).to eq(0)
    end
  end
end

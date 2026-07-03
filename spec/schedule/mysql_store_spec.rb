RSpec.describe KafkaBatch::Schedule::MysqlStore do
  let(:store) { described_class.new }

  before(:each) { KafkaBatchSpec::ActiveRecordSupport.truncate! }

  def schedule(job_id:, run_at:, partition: 0, offset: 1, batch_id: nil)
    store.schedule(job_id: job_id, run_at: run_at, partition: partition, offset: offset, batch_id: batch_id)
  end

  describe "#schedule / #claim_due" do
    it "claims only due rows, soonest first, as member strings" do
      now = Time.now
      schedule(job_id: "past",   run_at: now - 10, partition: 1, offset: 5)
      schedule(job_id: "future", run_at: now + 3600, partition: 2, offset: 9)

      claimed = store.claim_due(now: now, lease_seconds: 60, limit: 10)

      expect(claimed).to eq(["past:1:5"])
      expect(store.size).to eq(2) # rows remain until ack; only the claim was leased
    end

    it "does not re-claim a leased row until its lease expires (crash recovery)" do
      now = Time.now
      schedule(job_id: "crashed", run_at: now - 1)

      first = store.claim_due(now: now, lease_seconds: 30, limit: 10)
      expect(first).to eq(["crashed:0:1"])

      # still leased → not handed out again
      expect(store.claim_due(now: now + 10, lease_seconds: 30, limit: 10)).to be_empty
      # lease expired → reclaimable
      expect(store.claim_due(now: now + 60, lease_seconds: 30, limit: 10)).to eq(["crashed:0:1"])
    end
  end

  describe "#schedule_many (bulk)" do
    it "inserts all rows in one call" do
      now = Time.now
      store.schedule_many([
        { job_id: "a", run_at: now - 1, partition: 0, offset: 1, batch_id: nil },
        { job_id: "b", run_at: now - 1, partition: 1, offset: 2, batch_id: "bx" },
        { job_id: "c", run_at: now + 3600, partition: 2, offset: 3, batch_id: nil }
      ])

      expect(store.size).to eq(3)
      expect(store.claim_due(now: now, lease_seconds: 60, limit: 10).sort).to eq(["a:0:1", "b:1:2"])
    end
  end

  describe "#ack" do
    it "deletes rows by job_id parsed from the member" do
      now = Time.now
      schedule(job_id: "done", run_at: now - 1, partition: 3, offset: 8)
      claimed = store.claim_due(now: now, lease_seconds: 60, limit: 10)

      store.ack(claimed)

      expect(store.size).to eq(0)
    end
  end

  describe "#reclaim" do
    it "clears expired leases so the rows are claimable again" do
      now = Time.now
      schedule(job_id: "j", run_at: now - 1)
      store.claim_due(now: now, lease_seconds: 30, limit: 10)

      expect(store.reclaim(now: now + 60)).to eq(1)
      expect(store.claim_due(now: now + 61, lease_seconds: 30, limit: 10)).to eq(["j:0:1"])
    end
  end

  describe "#cancel (native per-job)" do
    it "deletes a pending row and decrements the batch total_jobs" do
      allow(KafkaBatch).to receive(:store).and_return(instance_double("ledger", add_jobs: :ok))
      schedule(job_id: "c1", run_at: Time.now + 500, batch_id: "b9")

      expect(store.cancel("c1")).to be(true)
      expect(store.size).to eq(0)
      expect(KafkaBatch.store).to have_received(:add_jobs).with("b9", -1)
    end

    it "returns false for an already-leased (claimed) row" do
      now = Time.now
      schedule(job_id: "c2", run_at: now - 1)
      store.claim_due(now: now, lease_seconds: 60, limit: 10)

      expect(store.cancel("c2")).to be(false)
      expect(store.size).to eq(1)
    end
  end

  describe "#find (search by job_id)" do
    it "returns the row with decoded coordinates and pending state" do
      schedule(job_id: "needle", run_at: Time.now + 100, partition: 4, offset: 88, batch_id: "b")
      expect(store.find("needle")).to include(job_id: "needle", partition: 4, offset: 88, batch_id: "b", state: :pending)
    end

    it "reports leased state after a claim, and nil for unknown" do
      now = Time.now
      schedule(job_id: "leased", run_at: now - 1)
      store.claim_due(now: now, lease_seconds: 60, limit: 10)

      expect(store.find("leased")).to include(state: :leased)
      expect(store.find("nope")).to be_nil
    end
  end

  describe "#list" do
    it "returns pending rows with decoded coordinates" do
      at = Time.now + 300
      schedule(job_id: "abc", run_at: at, partition: 4, offset: 42, batch_id: "bb")

      row = store.list.first
      expect(row).to include(job_id: "abc", partition: 4, offset: 42, batch_id: "bb")
    end
  end
end

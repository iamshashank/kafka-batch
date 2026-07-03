RSpec.describe KafkaBatch::Schedule::RedisStore do
  let(:store) { described_class.new }

  before(:each) do
    skip "Redis unavailable at #{KafkaBatchSpec::RedisHelper::TEST_URL}" unless KafkaBatchSpec::RedisHelper.available?
    KafkaBatch.config.redis_url          = KafkaBatchSpec::RedisHelper::TEST_URL
    KafkaBatch.config.schedule_batch_size = 100
    KafkaBatchSpec::RedisHelper.flush!
  end

  def schedule(job_id:, run_at:, partition: 0, offset: 1)
    store.schedule(job_id: job_id, run_at: run_at, partition: partition, offset: offset)
  end

  describe "#schedule / #claim_due" do
    it "returns only due entries, soonest first, and leaves future ones pending" do
      now = Time.now
      schedule(job_id: "past",   run_at: now - 10, partition: 1, offset: 5)
      schedule(job_id: "future", run_at: now + 3600, partition: 2, offset: 9)

      claimed = store.claim_due(now: now, lease_seconds: 60, limit: 10)

      expect(claimed).to eq(["past:1:5"])
      expect(store.size).to eq(1) # future one still pending
    end

    it "honours the limit" do
      now = Time.now
      3.times { |i| schedule(job_id: "j#{i}", run_at: now - 1, partition: 0, offset: i) }

      expect(store.claim_due(now: now, lease_seconds: 60, limit: 2).size).to eq(2)
    end

    it "does not hand the same entry to two concurrent claims (leased out)" do
      now = Time.now
      schedule(job_id: "only", run_at: now - 1)

      first  = store.claim_due(now: now, lease_seconds: 60, limit: 10)
      second = store.claim_due(now: now, lease_seconds: 60, limit: 10)

      expect(first).to eq(["only:0:1"])
      expect(second).to be_empty # already leased, not due for reclaim yet
    end
  end

  describe "#schedule_many (bulk)" do
    it "adds all pointers in one call and they claim by due time" do
      now = Time.now
      store.schedule_many([
        { job_id: "a", run_at: now - 1, partition: 0, offset: 1, batch_id: nil },
        { job_id: "b", run_at: now - 1, partition: 0, offset: 2, batch_id: nil },
        { job_id: "c", run_at: now + 3600, partition: 0, offset: 3, batch_id: nil }
      ])

      expect(store.size).to eq(3)
      expect(store.claim_due(now: now, lease_seconds: 60, limit: 10).sort).to eq(["a:0:1", "b:0:2"])
    end
  end

  describe "#ack" do
    it "permanently removes leased entries so they never re-dispatch" do
      now     = Time.now
      schedule(job_id: "done", run_at: now - 1)
      claimed = store.claim_due(now: now, lease_seconds: 60, limit: 10)

      store.ack(claimed)

      # even after the lease would expire, nothing comes back
      expect(store.reclaim(now: now + 120)).to eq(0)
      expect(store.claim_due(now: now + 200, lease_seconds: 60, limit: 10)).to be_empty
    end
  end

  describe "#reclaim (crash recovery)" do
    it "returns a claimed-but-unacked entry to pending once its lease expires" do
      now     = Time.now
      schedule(job_id: "crashed", run_at: now - 1)

      claimed = store.claim_due(now: now, lease_seconds: 30, limit: 10)
      expect(claimed).to eq(["crashed:0:1"])
      # simulate crash: never ack.

      # Before the lease expires it must NOT be re-dispatched...
      expect(store.reclaim(now: now + 10)).to eq(0)
      expect(store.claim_due(now: now + 10, lease_seconds: 30, limit: 10)).to be_empty

      # ...after the lease expires, reclaim returns it and it can be claimed again.
      expect(store.reclaim(now: now + 60)).to eq(1)
      expect(store.claim_due(now: now + 61, lease_seconds: 30, limit: 10)).to eq(["crashed:0:1"])
    end
  end

  describe "#cancel" do
    it "is a no-op in the Redis backend (cancellation is via CancellationCache)" do
      schedule(job_id: "x", run_at: Time.now + 100)
      expect(store.cancel("x")).to be(false)
    end
  end

  describe "#find (search by job_id)" do
    it "finds a pending job and reports its coordinates and state" do
      schedule(job_id: "needle", run_at: Time.now + 100, partition: 4, offset: 88)

      hit = store.find("needle")
      expect(hit).to include(job_id: "needle", partition: 4, offset: 88, state: :pending)
    end

    it "finds a leased (in-flight) job" do
      now = Time.now
      schedule(job_id: "leased", run_at: now - 1)
      store.claim_due(now: now, lease_seconds: 60, limit: 10)

      expect(store.find("leased")).to include(job_id: "leased", state: :leased)
    end

    it "returns nil for an unknown job_id" do
      expect(store.find("nope")).to be_nil
    end
  end

  describe "#list / #size" do
    it "lists pending entries with parsed coordinates and run_at" do
      at = Time.now + 500
      schedule(job_id: "abc", run_at: at, partition: 3, offset: 42)

      row = store.list.first
      expect(row[:job_id]).to eq("abc")
      expect(row[:partition]).to eq(3)
      expect(row[:offset]).to eq(42)
      expect(row[:run_at].to_i).to be_within(1).of(at.to_i)
    end
  end
end

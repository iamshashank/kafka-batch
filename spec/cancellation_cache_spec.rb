RSpec.describe KafkaBatch::CancellationCache do
  let(:store) { double("store") }

  before { allow(KafkaBatch).to receive(:store).and_return(store) }

  it "reports cancelled ids as cancelled and others as not" do
    allow(store).to receive(:cancelled_batch_ids).and_return(%w[b1 b2])
    expect(described_class.cancelled?("b1")).to be(true)
    expect(described_class.cancelled?("zzz")).to be(false)
  end

  it "returns false for a nil batch_id without touching the store" do
    expect(described_class.cancelled?(nil)).to be(false)
    expect(store).not_to have_received(:cancelled_batch_ids) if store.respond_to?(:cancelled_batch_ids)
  end

  it "refreshes from the store only once within the TTL window" do
    KafkaBatch.config.cancellation_cache_ttl = 120
    allow(store).to receive(:cancelled_batch_ids).and_return(%w[b1])

    5.times { described_class.cancelled?("b1") }

    expect(store).to have_received(:cancelled_batch_ids).once
  end

  it "refreshes again after reset!" do
    allow(store).to receive(:cancelled_batch_ids).and_return([])
    described_class.cancelled?("x")
    described_class.reset!
    described_class.cancelled?("x")
    expect(store).to have_received(:cancelled_batch_ids).twice
  end

  it "re-reads when the cache is stale (ttl = 0)" do
    KafkaBatch.config.cancellation_cache_ttl = 0
    allow(store).to receive(:cancelled_batch_ids).and_return([])
    3.times { described_class.cancelled?("x") }
    expect(store).to have_received(:cancelled_batch_ids).at_least(3).times
  end

  it "keeps the previous set when a refresh raises" do
    KafkaBatch.config.cancellation_cache_ttl = 0
    call = 0
    allow(store).to receive(:cancelled_batch_ids) do
      call += 1
      call == 1 ? %w[b1] : raise(KafkaBatch::StoreError, "down")
    end

    expect(described_class.cancelled?("b1")).to be(true) # first load
    expect(described_class.cancelled?("b1")).to be(true) # refresh fails → keep previous
  end

  # ── CancellationCache#add optimistic insertion ────────────────────────────
  describe "#add optimistic insertion" do
    it "makes a newly cancelled batch visible immediately without waiting for TTL" do
      KafkaBatch.config.cancellation_cache_ttl = 300  # long TTL → no auto-refresh
      allow(store).to receive(:cancelled_batch_ids).and_return([])  # store doesn't know yet

      # Warm the cache with an empty set
      expect(described_class.cancelled?("fresh")).to be(false)

      # Optimistically add the id (e.g. just after writing to the store)
      described_class.add("fresh")

      # Should be visible immediately without a store round-trip
      expect(described_class.cancelled?("fresh")).to be(true)
      # The store was only called once (the initial warm-up), not again
      expect(store).to have_received(:cancelled_batch_ids).once
    end

    it "preserves existing cached ids when adding a new one" do
      allow(store).to receive(:cancelled_batch_ids).and_return(%w[b1 b2])
      described_class.cancelled?("b1")  # warm cache

      described_class.add("b3")

      expect(described_class.cancelled?("b1")).to be(true)
      expect(described_class.cancelled?("b2")).to be(true)
      expect(described_class.cancelled?("b3")).to be(true)
    end

    it "is a no-op for nil batch_id" do
      expect { described_class.add(nil) }.not_to raise_error
    end

    it "does not modify the previously-stored set object (immutable snapshot)" do
      allow(store).to receive(:cancelled_batch_ids).and_return(%w[b1])
      described_class.cancelled?("b1")  # prime cache

      old_snap = described_class.instance_variable_get(:@snapshot)
      old_ids  = old_snap[:ids].dup

      described_class.add("b_new")

      # The snapshot was replaced, not mutated
      new_snap = described_class.instance_variable_get(:@snapshot)
      expect(new_snap[:ids]).not_to equal(old_snap[:ids])
      # The OLD ids set is unchanged
      expect(old_ids).not_to include("b_new")
    end
  end
end

# frozen_string_literal: true

require "spec_helper"

RSpec.describe KafkaBatch::Reconciler::RunSummary do
  before do
    skip "Redis unavailable" unless KafkaBatchSpec::RedisHelper.available?
    KafkaBatch.reset!
    KafkaBatch.configure do |c|
      c.store     = :redis
      c.redis_url = KafkaBatchSpec::RedisHelper::TEST_URL
    end
    KafkaBatchSpec::RedisHelper.flush!
  end

  it "round-trips a last-run summary" do
    described_class.save_last!(
      ran_at: "2026-01-01T00:00:00Z",
      triggered_by: "consumer",
      duration: 1.23,
      found_stale: 2,
      processed_stale: 2,
      found_lost: 1,
      processed_lost: 1,
      capped_stale: "0",
      capped_lost: "0",
      recovered_stale: 1,
      refired_lost: 1,
      skipped_stale: 1,
      produce_failed: 0,
      details: [{ batch_id: "abc", action: "recovered_running" }]
    )

    loaded = described_class.load_last
    expect(loaded[:triggered_by]).to eq("consumer")
    expect(loaded[:recovered_stale]).to eq("1")
    expect(loaded[:details].size).to eq(1)
    expect(loaded[:details].first[:batch_id]).to eq("abc")
  end

  it "records lock skips" do
    described_class.save_skip!
    skip = described_class.load_skip
    expect(skip[:reason]).to eq("lock_held")
    expect(skip[:at]).not_to be_nil
  end
end

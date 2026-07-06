# frozen_string_literal: true

require "spec_helper"

RSpec.describe KafkaBatch::Reconciler::Collector do
  it "summarizes identification and outcomes" do
    c = described_class.new(triggered_by: :rake)
    c.identify(5, [{ id: "a" }, { id: "b" }], 1, [{ id: "c" }])
    c.record_stale("a", :recovered_running, batch: { status: "success", total_jobs: 1, failed_count: 0 })
    c.record_stale("b", :skipped_in_progress, batch: { status: "running", total_jobs: 5, failed_count: 1 })
    c.record_lost("c", :refired_lost, batch: { status: "complete", total_jobs: 2, failed_count: 1 })

    s = c.finish(0.5)
    expect(s[:found_stale]).to eq(5)
    expect(s[:processed_stale]).to eq(2)
    expect(s[:capped_stale]).to eq("1")
    expect(s[:recovered_stale]).to eq(1)
    expect(s[:skipped_stale]).to eq(1)
    expect(s[:refired_lost]).to eq(1)
    expect(s[:details].size).to eq(3)
  end
end

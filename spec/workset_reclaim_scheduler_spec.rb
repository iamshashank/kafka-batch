# frozen_string_literal: true

require "spec_helper"

RSpec.describe KafkaBatch::Workset::ReclaimScheduler do
  before do
    skip "Redis not available" unless KafkaBatchSpec::RedisHelper.available?
  end

  let(:store) { KafkaBatch::Workset::Store.new }
  let(:scheduler) { described_class.new(store: store) }

  def claim!(job_id:, consumer_id:)
    store.claim(
      job_id: job_id, payload: %({"job_id":"#{job_id}","worker_class":"W"}),
      topic: "jobs", partition: 0, offset: 1, consumer_id: consumer_id,
      lease_ttl: 60, steal_grace: -1
    )
  end

  def kill_consumer!(consumer_id)
    Redis.new(url: KafkaBatchSpec::RedisHelper::TEST_URL)
         .del("#{KafkaBatch::Workset::LIVE_CONSUMER_PREFIX}#{consumer_id}")
  end

  def age_claim!(job_id, age_sec)
    entry = store.get_entry(job_id)
    entry.claimed_at_unix = Time.now.to_i - age_sec
    raw = store.send(:dump_entry, entry)
    Redis.new(url: KafkaBatchSpec::RedisHelper::TEST_URL)
         .set("#{KafkaBatch::Workset::JOB_KEY_PREFIX}#{job_id}", raw)
    Redis.new(url: KafkaBatchSpec::RedisHelper::TEST_URL)
         .zadd(KafkaBatch::Workset::INDEX_KEY, entry.claimed_at_unix, job_id)
  end

  it "emits workset.reclaimed with sweep counts even when nothing is reclaimed" do
    expect(KafkaBatch::Instrumentation).to receive(:workset_reclaimed).with(
      checked: 0, reclaimed: 0, failed: 0, skipped: 0, duration: kind_of(Numeric)
    )

    scheduler.tick(limit: 10, grace: 40)
  end

  it "emits workset.reclaimed reflecting a successful orphan reclaim" do
    claim!(job_id: "rs-1", consumer_id: "gone")
    kill_consumer!("gone")
    age_claim!("rs-1", 60)

    expect(KafkaBatch::Instrumentation).to receive(:workset_reclaimed).with(
      checked: 1, reclaimed: 1, failed: 0, skipped: 0, duration: kind_of(Numeric)
    )

    scheduler.tick(limit: 10, grace: 40)
  end
end

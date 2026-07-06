# frozen_string_literal: true

require "spec_helper"

RSpec.describe KafkaBatch::Dlt::Stats do
  before do
    skip "Redis unavailable" unless KafkaBatchSpec::RedisHelper.available?
    KafkaBatch.reset!
    KafkaBatch.configure do |c|
      c.store     = :redis
      c.redis_url = KafkaBatchSpec::RedisHelper::TEST_URL
    end
    KafkaBatchSpec::RedisHelper.flush!
  end

  it "caches computed stats in Redis" do
    reader = instance_double(KafkaBatch::Dlt::Reader)
    allow(KafkaBatch::Dlt::Reader).to receive(:new).and_return(reader)
    allow(reader).to receive(:watermarks).and_return(topic: "kafka_batch.dead_letter", partitions: 3, total: 10, watermarks: {})
    allow(reader).to receive(:sample_messages).and_return([
      { dlt_type: "job" },
      { dlt_type: "job" },
      { dlt_type: "expired" }
    ])
    allow(reader).to receive(:close)

    stats = described_class.fetch(refresh: true)
    expect(stats[:total]).to eq(10)
    expect(stats[:by_type]).to eq("job" => 2, "expired" => 1)

    cached = described_class.fetch(refresh: false)
    expect(cached[:total]).to eq(10)
    expect(KafkaBatch::Dlt::Reader).to have_received(:new).once
  end
end

# frozen_string_literal: true

require "oj"

RSpec.describe KafkaBatch::Ai::LiveData::Executor do
  before do
    skip "Redis unavailable" unless KafkaBatchSpec::RedisHelper.available?
    KafkaBatch.config.redis_url = KafkaBatchSpec::RedisHelper::TEST_URL
    KafkaBatch.config.ai_live_data_enabled = true
    KafkaBatch::Ai::LiveData.reset!
    KafkaBatchSpec::RedisHelper.flush!
  end

  after { KafkaBatch::Ai::LiveData.reset! }

  def exec
    KafkaBatch::Ai::LiveData.executor
  end

  it "reads batch fields with HMGET and never invents missing batches" do
    r = Redis.new(url: KafkaBatchSpec::RedisHelper::TEST_URL)
    r.hset("kafka_batch:b:b1", "id", "b1", "status", "running", "total_jobs", "3", "completed_count", "1")

    result = exec.call("get_batch", "batch_id" => "b1")
    expect(result["ok"]).to eq(true)
    expect(result["data"]["found"]).to eq(true)
    expect(result["data"]["batch"]["status"]).to eq("running")
    expect(result["data"]["batch"]["total_jobs"]).to eq("3")

    missing = exec.call("get_batch", "batch_id" => "nope")
    expect(missing["data"]["found"]).to eq(false)
  end

  it "rejects invalid ids and unknown tools" do
    bad = exec.call("get_batch", "batch_id" => "bad id*")
    expect(bad["ok"]).to eq(false)
    expect(bad["error"]).to match(/invalid/)

    unk = exec.call("flushdb", {})
    expect(unk["ok"]).to eq(false)
    expect(unk["error"]).to match(/unknown tool/)
  end

  it "returns O(1) fairness snapshot sizes" do
    r = Redis.new(url: KafkaBatchSpec::RedisHelper::TEST_URL)
    r.zadd("kafka_batch:fair_time:ring", 1.0, "acme")
    r.zadd("kafka_batch:fair_time:leases", Time.now.to_f + 60, "slot-1")
    r.hset("kafka_batch:fair_time:weight", "acme", "2.0")

    result = exec.call("get_fairness_snapshot", "lane" => "time")
    expect(result["ok"]).to eq(true)
    expect(result["data"]["ring_size"]).to eq(1)
    expect(result["data"]["leases_inflight"]).to eq(1)
    expect(result["data"]["weight_entries"]).to eq(1)
  end

  it "reads schedule depth via ZCARD" do
    r = Redis.new(url: KafkaBatchSpec::RedisHelper::TEST_URL)
    r.zadd("kafka_batch:sched:pending", Time.now.to_f, "j1:0:1")
    r.zadd("kafka_batch:sched:inflight", Time.now.to_f + 30, "j2:0:2")

    result = exec.call("get_schedule_depth", {})
    expect(result["data"]).to eq("pending" => 1, "inflight" => 1)
  end

  it "redacts workset payload fields" do
    r = Redis.new(url: KafkaBatchSpec::RedisHelper::TEST_URL)
    r.set("kafka_batch:work:job:j1", Oj.dump("job_id" => "j1", "payload" => { "secret" => 1 }, "fence" => 9))

    result = exec.call("get_workset_job", "job_id" => "j1")
    expect(result["data"]["found"]).to eq(true)
    expect(result["data"]["claim"]["payload"]).to eq("[redacted]")
    expect(result["data"]["claim"]["fence"]).to eq(9)
  end

  it "only allowlists read commands on the executor" do
    expect(described_class::ALLOWED_REDIS).to include(:get, :hmget, :zcard, :llen, :sismember)
    expect(described_class::ALLOWED_REDIS).not_to include(:set, :del, :hset, :eval, :keys, :scan, :lrange)
  end
end

RSpec.describe KafkaBatch::Ai::LiveData do
  before do
    skip "Redis unavailable" unless KafkaBatchSpec::RedisHelper.available?
    KafkaBatch.config.redis_url = KafkaBatchSpec::RedisHelper::TEST_URL
    KafkaBatch.config.ai_knowledge_enabled = true
    KafkaBatch.config.ai_live_data_enabled = true
    KafkaBatch.config.ai_live_data_max_calls = 3
    described_class.reset!
    KafkaBatchSpec::RedisHelper.flush!
  end

  after { described_class.reset! }

  it "prefetches batch tools from message text" do
    r = Redis.new(url: KafkaBatchSpec::RedisHelper::TEST_URL)
    r.hset("kafka_batch:b:abc-123", "id", "abc-123", "status", "complete")

    lookups = described_class.prefetch(message: "status of batch abc-123")
    tools = lookups.map { |l| l["tool"] }
    expect(tools).to include("get_batch", "get_batch_index")
    expect(lookups.find { |l| l["tool"] == "get_batch" }.dig("data", "batch", "status")).to eq("complete")
  end

  it "is disabled when ai_live_data_enabled is false" do
    KafkaBatch.config.ai_live_data_enabled = false
    expect(described_class.enabled?).to eq(false)
    expect(described_class.prefetch(message: "batch x")).to eq([])
  end

  it "suggests page-aware prompts" do
    prompts = described_class.suggested_prompts(context: { "batch_id" => "b9" })
    expect(prompts.first["id"]).to eq("batch_context")
    expect(prompts.first["message"]).to include("b9")
  end

  it "prefetches counts and schedule from natural-language intents" do
    r = Redis.new(url: KafkaBatchSpec::RedisHelper::TEST_URL)
    r.hset("kafka_batch:counts", "running", "2")
    r.zadd("kafka_batch:sched:pending", Time.now.to_f, "j:0:1")

    counts = described_class.prefetch(message: "What are the current batch status counts in Redis?")
    expect(counts.map { |l| l["tool"] }).to include("get_counts")

    sched = described_class.prefetch(message: "How deep is the delayed-job schedule?")
    expect(sched.map { |l| l["tool"] }).to include("get_schedule_depth")
  end

  it "does not enable OpenRouter model tools by default" do
    expect(described_class.model_tools_enabled?).to eq(false)
  end
end

# frozen_string_literal: true

require "oj"

RSpec.describe KafkaBatch::Ai::Crypto do
  before { KafkaBatch.config.ai_encryption_salt = "test-salt-#{Process.pid}" }

  after { KafkaBatch.config.ai_encryption_salt = "" }

  it "round-trips plaintext" do
    blob = described_class.encrypt("sk-or-secret")
    expect(blob).not_to include("sk-or-secret")
    expect(described_class.decrypt(blob)).to eq("sk-or-secret")
  end

  it "requires a salt to encrypt" do
    KafkaBatch.config.ai_encryption_salt = ""
    expect { described_class.encrypt("x") }.to raise_error(KafkaBatch::ConfigurationError, /ai_encryption_salt/)
  end
end

RSpec.describe KafkaBatch::Ai::ChatHistory do
  before do
    skip "Redis unavailable" unless KafkaBatchSpec::RedisHelper.available?
    KafkaBatch.config.redis_url = KafkaBatchSpec::RedisHelper::TEST_URL
    KafkaBatch.config.ai_chat_history_max_lines = 5
    described_class.reset_pool!
    KafkaBatchSpec::RedisHelper.flush!
  end

  after do
    described_class.reset_pool!
    KafkaBatch.config.ai_chat_history_max_lines = 500
  end

  it "appends, lists newest-first, and trims to max_lines" do
    6.times { |i| described_class.append!(role: "user", content: "m#{i}") }
    expect(described_class.size).to eq(5)
    listed = described_class.list
    expect(listed.map { |m| m["content"] }).to eq(%w[m5 m4 m3 m2 m1])
  end

  it "clears history" do
    described_class.append!(role: "assistant", content: "hi")
    described_class.clear!
    expect(described_class.size).to eq(0)
  end
end

RSpec.describe KafkaBatch::Ai::Settings do
  before do
    skip "Redis unavailable" unless KafkaBatchSpec::RedisHelper.available?
    KafkaBatch.config.redis_url = KafkaBatchSpec::RedisHelper::TEST_URL
    KafkaBatch.config.ai_encryption_salt = "settings-salt"
    described_class.reset_pool!
    KafkaBatchSpec::RedisHelper.flush!
  end

  after do
    described_class.reset_pool!
    KafkaBatch.config.ai_encryption_salt = ""
  end

  it "stores an encrypted key and returns a masked preview" do
    shown = described_class.update!(api_key: "sk-or-abcdefgh", model: "openai/gpt-4o-mini")
    expect(shown["api_key_set"]).to eq(true)
    expect(shown["api_key_masked"]).to eq("••••efgh")
    expect(shown["model"]).to eq("openai/gpt-4o-mini")
    expect(described_class.api_key).to eq("sk-or-abcdefgh")
  end

  it "clears the api key" do
    described_class.update!(api_key: "sk-or-abcdefgh")
    shown = described_class.update!(clear_api_key: true)
    expect(shown["api_key_set"]).to eq(false)
    expect(described_class.api_key).to be_nil
  end
end

RSpec.describe KafkaBatch::Ai::Retriever do
  before do
    skip "Redis unavailable" unless KafkaBatchSpec::RedisHelper.available?
    KafkaBatch.config.redis_url = KafkaBatchSpec::RedisHelper::TEST_URL
    KafkaBatch.config.ai_knowledge_enabled = true
    KafkaBatch::Ai::KnowledgeIndex.reset_pool!
    KafkaBatchSpec::RedisHelper.flush!
    KafkaBatch::Ai::KnowledgeIndex.sync!
  end

  after { KafkaBatch::Ai::KnowledgeIndex.reset_pool! }

  it "returns scored knowledge chunks for a docs query" do
    hits = described_class.search("super_fetch_concurrency fairness")
    expect(hits).not_to be_empty
    expect(hits.first).to include("id", "title", "text")
  end

  it "puts the live config chunk first so broker partitions beat DEFAULT_PARTITIONS docs" do
    hits = described_class.search("how many partitions fair time ready")
    expect(hits.first["id"]).to eq(KafkaBatch::Ai::KnowledgeIndex::LIVE_CONFIG_CHUNK_ID)
    expect(hits.first["text"]).to include("live_broker_partitions=")
  end
end

RSpec.describe KafkaBatch::Ai::Chat do
  before do
    skip "Redis unavailable" unless KafkaBatchSpec::RedisHelper.available?
    KafkaBatch.config.redis_url = KafkaBatchSpec::RedisHelper::TEST_URL
    KafkaBatch.config.ai_knowledge_enabled = true
    KafkaBatch.config.ai_encryption_salt = "chat-salt"
    KafkaBatch::Ai::Settings.reset_pool!
    KafkaBatch::Ai::ChatHistory.reset_pool!
    KafkaBatch::Ai::KnowledgeIndex.reset_pool!
    KafkaBatchSpec::RedisHelper.flush!
    KafkaBatch::Ai::KnowledgeIndex.sync!
    KafkaBatch::Ai::Settings.update!(api_key: "sk-or-test", model: "openai/gpt-4o-mini")
  end

  after do
    KafkaBatch::Ai::Settings.reset_pool!
    KafkaBatch::Ai::ChatHistory.reset_pool!
    KafkaBatch::Ai::KnowledgeIndex.reset_pool!
    KafkaBatch.config.ai_encryption_salt = ""
  end

  it "retrieves context, calls OpenRouter, and appends global history" do
    KafkaBatch.config.ai_live_data_enabled = false
    fake = instance_double(KafkaBatch::Ai::OpenRouter)
    expect(KafkaBatch::Ai::OpenRouter).to receive(:new).and_return(fake)
    expect(fake).to receive(:chat) do |messages:, tools: nil|
      expect(messages.any? { |m| m["role"] == "system" && m["content"].include?("Knowledge context") }).to eq(true)
      expect(messages.last).to eq("role" => "user", "content" => "What is SuperFetch?")
      expect(tools).to be_nil
      { "content" => "SuperFetch leases work from Redis.", "tool_calls" => nil }
    end

    result = described_class.ask("What is SuperFetch?")
    expect(result["ok"]).to eq(true)
    expect(result["reply"]).to include("SuperFetch")
    expect(result["history_size"]).to eq(2)
    chron = KafkaBatch::Ai::ChatHistory.list.reverse
    expect(chron.map { |m| m["role"] }).to eq(%w[user assistant])
  end

  it "prefetches live batch data without sending OpenRouter tools by default" do
    KafkaBatch.config.ai_live_data_enabled = true
    KafkaBatch.config.ai_live_data_model_tools = false
    r = Redis.new(url: KafkaBatchSpec::RedisHelper::TEST_URL)
    r.hset("kafka_batch:b:live1", "id", "live1", "status", "running", "total_jobs", "2")

    fake = instance_double(KafkaBatch::Ai::OpenRouter)
    expect(KafkaBatch::Ai::OpenRouter).to receive(:new).and_return(fake)
    expect(fake).to receive(:chat) do |messages:, tools: nil|
      expect(tools).to be_nil
      live = messages.find { |m| m["role"] == "system" && m["content"].to_s.include?("LIVE REDIS LOOKUPS (authoritative") }
      expect(live).not_to be_nil
      expect(live["content"]).to include("running")
      { "content" => "Batch live1 is running with 2 jobs.", "tool_calls" => nil }
    end

    result = described_class.ask("What is the status of batch live1?")
    expect(result["reply"]).to include("running")
    expect(result["live_lookups"].map { |l| l["tool"] }).to include("get_batch")
  end

  it "retries without tools when OpenRouter returns HTTP 400 for tool schemas" do
    KafkaBatch.config.ai_live_data_enabled = true
    KafkaBatch.config.ai_live_data_model_tools = true

    fake = instance_double(KafkaBatch::Ai::OpenRouter)
    expect(KafkaBatch::Ai::OpenRouter).to receive(:new).and_return(fake)
    expect(fake).to receive(:chat).with(hash_including(tools: kind_of(Array))).and_raise(
      KafkaBatch::Ai::OpenRouter::Error, "OpenRouter HTTP 400: Provider returned error"
    )
    expect(fake).to receive(:chat).with(hash_including(tools: nil)).and_return(
      "content" => "Answer without tools.", "tool_calls" => nil
    )

    result = described_class.ask("What is SuperFetch?")
    expect(result["reply"]).to eq("Answer without tools.")
  end

  it "rejects blank messages and missing API keys" do
    expect { described_class.ask("  ") }.to raise_error(ArgumentError, /blank/)
    KafkaBatch::Ai::Settings.update!(clear_api_key: true)
    expect { described_class.ask("hi") }.to raise_error(ArgumentError, /API key/)
  end
end

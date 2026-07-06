# frozen_string_literal: true

require "redis"

RSpec.describe KafkaBatch::Uniqueness do
  before do
    skip "Redis not available" unless KafkaBatchSpec::RedisHelper.available?
    KafkaBatch::Uniqueness.reset!
    KafkaBatchSpec::RedisHelper.flush!
  end

  describe ".digest" do
    it "returns 8 raw bytes (64-bit XXHash64, not hex)" do
      d = described_class.digest(UniqWorker, { "id" => 1 })
      expect(d).to be_a(String)
      expect(d.encoding).to eq(Encoding::ASCII_8BIT)
      expect(d.bytesize).to eq(8)
      expect(d).to eq(described_class.digest(UniqWorker, { "id" => 1 }))
    end

    it "is stable across key order in the payload" do
      a = described_class.digest(UniqWorker, { "a" => 1, "b" => 2 })
      b = described_class.digest(UniqWorker, { "b" => 2, "a" => 1 })
      expect(a).to eq(b)
    end

    it "differs by worker class" do
      payload = { "id" => 1 }
      a = described_class.digest(UniqWorker, payload)
      b = described_class.digest(SuccessfulWorker, payload)
      expect(a).not_to eq(b)
    end
  end

  describe "claim / release" do
    it "allows one in-flight job and rejects a duplicate" do
      expect(described_class.claim(UniqWorker, { "id" => 1 }, job_id: "job-a")).to eq(true)
      expect(described_class.claim(UniqWorker, { "id" => 1 }, job_id: "job-b")).to eq(false)
      expect(described_class.claim(UniqWorker, { "id" => 2 }, job_id: "job-c")).to eq(true)
    end

    it "no-ops for workers without uniq true" do
      expect(described_class.claim(SuccessfulWorker, { "id" => 1 }, job_id: "j1")).to eq(true)
      expect(described_class.claim(SuccessfulWorker, { "id" => 1 }, job_id: "j2")).to eq(true)
    end

    it "releases only the owning job_id" do
      described_class.claim(UniqWorker, { "id" => 1 }, job_id: "owner")
      described_class.release(UniqWorker, { "id" => 1 }, job_id: "other")
      expect(described_class.claim(UniqWorker, { "id" => 1 }, job_id: "next")).to eq(false)

      described_class.release(UniqWorker, { "id" => 1 }, job_id: "owner")
      expect(described_class.claim(UniqWorker, { "id" => 1 }, job_id: "next")).to eq(true)
    end

    it "stores the lock under a binary-suffixed Redis key" do
      expect(described_class.claim(UniqWorker, { "id" => 9 }, job_id: "jid-9")).to eq(true)
      digest = described_class.digest(UniqWorker, { "id" => 9 })
      key    = "#{described_class::KEY_PREFIX}#{digest}"

      r = Redis.new(url: KafkaBatchSpec::RedisHelper::TEST_URL)
      expect(r.get(key)).to eq("jid-9")
      expect(key.bytesize).to eq(described_class::KEY_PREFIX.bytesize + 8)
    end
  end
end

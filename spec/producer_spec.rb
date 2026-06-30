RSpec.describe KafkaBatch::Producer do
  describe ".encode" do
    it "JSON-encodes a hash payload" do
      json = described_class.send(:encode, { "a" => 1 })
      expect(Oj.load(json)).to eq("a" => 1)
    end

    it "passes a String payload through untouched" do
      expect(described_class.send(:encode, "raw")).to eq("raw")
    end
  end

  describe ".produce_sync error wrapping" do
    it "wraps underlying WaterDrop errors in ProducerError" do
      allow(described_class).to receive(:produce_sync).and_call_original
      fake = double("producer")
      allow(fake).to receive(:produce_sync).and_raise(
        WaterDrop::Errors::ProducerClosedError, "closed"
      )
      allow(described_class).to receive(:instance).and_return(fake)

      expect {
        described_class.produce_sync(topic: "t", payload: { x: 1 }, key: "k")
      }.to raise_error(KafkaBatch::ProducerError, /Kafka produce failed/)
    end
  end

  # ── max_message_bytes size guard ─────────────────────────────────────────
  describe ".encode max_message_bytes guard" do
    after { KafkaBatch.config.max_message_bytes = 1_048_576 }  # restore default

    it "raises ProducerError when encoded payload exceeds max_message_bytes" do
      KafkaBatch.config.max_message_bytes = 20
      expect {
        described_class.send(:encode, { "data" => "a" * 100 })
      }.to raise_error(KafkaBatch::ProducerError, /Payload too large/)
    end

    it "includes byte size and limit in the error message" do
      KafkaBatch.config.max_message_bytes = 5
      err = begin
        described_class.send(:encode, "toolong")
      rescue KafkaBatch::ProducerError => e
        e
      end
      expect(err.message).to match(/\d+ bytes exceeds/)
      expect(err.message).to include("5")
    end

    it "does NOT raise when the payload is within the limit" do
      KafkaBatch.config.max_message_bytes = 1_048_576
      expect { described_class.send(:encode, { "x" => 1 }) }.not_to raise_error
    end

    it "skips the size guard when max_message_bytes is 0 (disabled)" do
      KafkaBatch.config.max_message_bytes = 0
      expect { described_class.send(:encode, "x" * 10_000) }.not_to raise_error
    end

    it "skips the size guard when max_message_bytes is nil" do
      KafkaBatch.config.max_message_bytes = nil
      expect { described_class.send(:encode, "x" * 10_000) }.not_to raise_error
    end
  end

  describe ".build kafka config normalization" do
    after { @producer&.close rescue nil }

    it "normalizes keys to symbols and lets user overrides win" do
      KafkaBatch.config.producer_config = {
        "compression.type"   => "snappy",   # string key from the user
        :"bootstrap.servers" => "override:9092"
      }

      @producer = described_class.send(:build)
      kafka     = @producer.config.kafka

      expect(kafka.keys).to all(be_a(Symbol))
      expect(kafka[:"bootstrap.servers"]).to eq("override:9092")
      expect(kafka[:"compression.type"]).to eq("snappy")
      expect(kafka[:"request.required.acks"]).to eq("all")
    end
  end
end

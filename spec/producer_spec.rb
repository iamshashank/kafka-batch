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

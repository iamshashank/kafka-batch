# frozen_string_literal: true

RSpec.describe KafkaBatch::Callback do
  describe ".job / .parse / .dump" do
    it "round-trips a job callback with topic" do
      spec = described_class.job("segment.export.on_success", topic: "segment.exports.callbacks")
      raw  = described_class.dump(spec)
      parsed = described_class.parse(raw)

      expect(parsed).to be_a(described_class::Job)
      expect(parsed.job_type).to eq("segment.export.on_success")
      expect(parsed.topic).to eq("segment.exports.callbacks")
    end

    it "treats plain strings as legacy Ruby classes" do
      parsed = described_class.parse("ImportCallbacks")
      expect(parsed).to be_a(described_class::Legacy)
      expect(parsed.class_name).to eq("ImportCallbacks")
    end

    it "builds a worker callback from a Worker class" do
      worker = Class.new do
        include KafkaBatch::Worker
        kafka_topic "callbacks.ruby"
        def self.name = "TestCallbackWorker"
        def perform(_payload); end
      end

      spec = described_class.worker(worker)
      expect(spec.job_type).to eq("test_callback")
      expect(spec.topic).to eq("callbacks.ruby")
    end
  end
end

# frozen_string_literal: true

require "spec_helper"

RSpec.describe KafkaBatch::Consumers::PriorityJobConsumer do
  let(:consumer) { described_class.new }
  let(:spec) do
    {
      rank:                1,
      mode:                mode,
      higher_topics:       %w[kafka_batch.jobs.p0],
      consumer_group:      "kafka-batch-jobs-fast",
      topic:               "kafka_batch.jobs.p1",
      weighted_interleave: 4
    }
  end
  let(:klass) { described_class.build(spec) }

  before do
    consumer.extend(KafkaBatch::Consumers::PriorityGate)
    allow(consumer).to receive(:pause)
    allow(KafkaBatch::Instrumentation).to receive(:consumer_priority_yielded)
  end

  describe "weighted mode" do
    let(:mode) { :weighted }

    it "interleaves lower-rank work while higher topics have lag" do
      inst = klass.new
      allow(inst).to receive(:higher_topics_have_lag?).and_return(true)
      allow(inst).to receive(:pause)
      allow(inst).to receive(:super) # won't be called

      yields = 0
      allows = 0
      8.times do
        if inst.send(:should_yield_to_higher?, spec)
          yields += 1
        else
          allows += 1
        end
      end
      expect(allows).to eq(2)  # 1-in-4 interleave
      expect(yields).to eq(6)
    end
  end

  describe "strict mode" do
    let(:mode) { :strict }

    it "always yields when higher topics have lag" do
      inst = klass.new
      allow(inst).to receive(:higher_topics_have_lag?).and_return(true)
      expect(inst.send(:should_yield_to_higher?, spec)).to be(true)
    end
  end

  describe "#consume" do
    let(:mode) { :strict }

    it "rank 0 does not check higher-topic lag" do
      rank0 = described_class.build(spec.merge(rank: 0, higher_topics: [], mode: :strict))
      inst  = build_consumer(rank0)
      allow(inst).to receive(:messages).and_return([])
      expect(inst).not_to receive(:higher_topics_have_lag?)
      inst.consume
    end

    it "rank 1 pauses at the batch offset and skips processing when higher topics have lag" do
      inst = build_consumer(klass)
      msg  = instance_double("Karafka::Messages::Message", offset: 42)
      allow(inst).to receive(:messages).and_return([msg])
      allow(inst).to receive(:higher_topics_have_lag?).and_return(true)
      expect(inst).to receive(:pause).with(42, 2_000)
      expect(inst).not_to receive(:process_message)
      inst.consume
    end

    it "rank 1 processes when higher topics have no lag" do
      inst = build_consumer(klass)
      msg  = instance_double("Karafka::Messages::Message")
      allow(inst).to receive(:messages).and_return([msg])
      allow(inst).to receive(:higher_topics_have_lag?).and_return(false)
      expect(inst).not_to receive(:pause)
      expect(inst).to receive(:process_message).with(msg)
      inst.consume
    end
    it "rank 1 processes when a higher topic is topic-paused via /lag" do
      inst = build_consumer(klass)
      msg  = instance_double("Karafka::Messages::Message")
      allow(inst).to receive(:messages).and_return([msg])
      allow(KafkaBatch::ConsumptionControl).to receive(:topic_level_paused?)
        .with(group: spec[:consumer_group], topic: "kafka_batch.jobs.p0").and_return(true)
      expect(inst).not_to receive(:pause)
      expect(inst).to receive(:process_message).with(msg)
      inst.consume
    end
  end
end

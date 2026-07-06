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
end

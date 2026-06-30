RSpec.describe KafkaBatch::Consumers::ConsumptionGate do
  let(:consumer_class) do
    Class.new(Karafka::BaseConsumer) do
      prepend KafkaBatch::Consumers::ConsumptionGate

      attr_accessor :ran

      def consume
        self.ran = true
      end
    end
  end

  let(:consumer) { consumer_class.allocate }
  let(:group)    { double(id: "kafka-batch-jobs") }
  let(:topic)    { double(name: "demo.jobs", consumer_group: group) }

  before do
    allow(consumer).to receive(:topic).and_return(topic)
    allow(consumer).to receive(:partition).and_return(0)
    allow(consumer).to receive(:messages).and_return([double(offset: 12)])
    allow(consumer).to receive(:pause)
    allow(consumer).to receive(:resume)
    allow(KafkaBatch::ConsumptionControl).to receive(:available?).and_return(true)
  end

  it "pauses consumption when the partition is marked paused" do
    allow(KafkaBatch::ConsumptionControl).to receive(:paused?)
      .with(group: "kafka-batch-jobs", topic: "demo.jobs", partition: 0).and_return(true)

    consumer.consume

    expect(consumer).to have_received(:pause).with(12, nil, true)
    expect(consumer.ran).to be_nil
  end

  it "runs consume when not paused" do
    allow(KafkaBatch::ConsumptionControl).to receive(:paused?).and_return(false)

    consumer.consume

    expect(consumer.ran).to eq(true)
    expect(consumer).not_to have_received(:pause)
  end

  it "passes through to consume (does NOT pause) when ConsumptionControl is unavailable" do
    allow(KafkaBatch::ConsumptionControl).to receive(:available?).and_return(false)
    # paused? must never be called when the control plane is down
    expect(KafkaBatch::ConsumptionControl).not_to receive(:paused?)

    consumer.consume

    expect(consumer.ran).to eq(true)
    expect(consumer).not_to have_received(:pause)
  end

  it "calls pause only once when invoked repeatedly while already paused (no double-pause)" do
    allow(KafkaBatch::ConsumptionControl).to receive(:paused?).and_return(true)

    consumer.consume
    consumer.consume  # second call while still paused

    expect(consumer).to have_received(:pause).once
  end

  it "resumes and runs consume when transitioning from paused to unpaused" do
    # First call: paused
    allow(KafkaBatch::ConsumptionControl).to receive(:paused?).and_return(true)
    consumer.consume
    expect(consumer.ran).to be_nil

    # Second call: unpaused
    allow(KafkaBatch::ConsumptionControl).to receive(:paused?).and_return(false)
    consumer.consume

    expect(consumer).to have_received(:resume).once
    expect(consumer.ran).to eq(true)
  end
end

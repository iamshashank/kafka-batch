# frozen_string_literal: true

require "spec_helper"

RSpec.describe KafkaBatch::Watermark do
  # Fake Karafka consumer: process_message emulates JobConsumer#commit_offset!
  # routing to the watermark executor (Thread.current[:kafka_batch_wm].note_done),
  # optionally blocking on a per-offset gate or raising to simulate infra failure.
  class FakeWmConsumer
    def initialize(gates: {}, fail_offsets: [])
      @gates        = gates
      @fail_offsets = fail_offsets
      @marked       = []
      @marked_mu    = Mutex.new
    end

    def marked
      @marked_mu.synchronize { @marked.dup }
    end

    def mark_as_consumed(message)
      @marked_mu.synchronize { @marked << message.offset }
    end

    def process_message(message)
      if (gate = @gates[message.offset])
        gate.pop # block until the test releases this offset
      end
      raise "boom #{message.offset}" if @fail_offsets.include?(message.offset)

      # What JobConsumer#commit_offset! does under watermark mode:
      Thread.current[:kafka_batch_wm].note_done(message)
    end
  end

  def msgs(topic, partition, offsets)
    offsets.map { |o| FakeMessage.new(payload: { "job_id" => "j#{o}" }, topic: topic, offset: o, partition: partition) }
  end

  def wait_until(timeout: 3)
    deadline = Process.clock_gettime(Process::CLOCK_MONOTONIC) + timeout
    until yield
      raise "condition not met within #{timeout}s" if Process.clock_gettime(Process::CLOCK_MONOTONIC) > deadline

      sleep 0.005
    end
  end

  let(:executor) { KafkaBatch::Watermark::Executor.new }

  before do
    KafkaBatch.configure do |c|
      c.super_fetch_concurrency  = 16
      c.super_fetch_claim_window = 64
    end
  end

  it "commits nothing past an incomplete head, then the whole prefix once the head lands" do
    head_gate = Queue.new
    consumer  = FakeWmConsumer.new(gates: { 100 => head_gate })

    executor.dispatch(consumer, msgs("t", 0, [100, 101, 102, 103, 104]))
    # 101..104 finish; 100 (the head) is gated.
    wait_until { executor.in_flight_count == 1 }
    executor.flush(consumer)
    expect(consumer.marked).to be_empty

    head_gate << true # release the head
    wait_until { executor.in_flight_count.zero? }
    executor.flush(consumer)
    expect(consumer.marked).to eq([100, 101, 102, 103, 104])
  end

  it "does not commit past an infra-failed offset (blocks its partition's prefix)" do
    consumer = FakeWmConsumer.new(fail_offsets: [101])

    executor.dispatch(consumer, msgs("t", 0, [100, 101, 102]))
    wait_until { executor.in_flight_count.zero? }
    executor.flush(consumer)

    # 100 commits; 101 failed → 102 (completed) stays uncommitted behind the gap.
    expect(consumer.marked).to eq([100])
  end

  it "advances partitions independently — a stalled head on one does not block another" do
    p0_head = Queue.new
    consumer = FakeWmConsumer.new(gates: { 10 => p0_head })

    batch = msgs("t", 0, [10, 11]) + msgs("t", 1, [20, 21])
    executor.dispatch(consumer, batch)
    wait_until { executor.in_flight_count == 1 } # only partition-0 head pending
    executor.flush(consumer)

    expect(consumer.marked.sort).to eq([20, 21]) # partition 1 committed
    expect(consumer.marked).not_to include(10, 11)

    p0_head << true
    wait_until { executor.in_flight_count.zero? }
    executor.flush(consumer)
    expect(consumer.marked.sort).to eq([10, 11, 20, 21])
  end

  it "re-forms the prefix from a redelivered (lower) offset after a rebalance" do
    consumer = FakeWmConsumer.new
    executor.dispatch(consumer, msgs("t", 0, [50, 51]))
    wait_until { executor.in_flight_count.zero? }
    executor.flush(consumer)
    expect(consumer.marked).to eq([50, 51])

    # Redelivery: offset 50 comes back (rebalance). Tracker resets; prefix re-forms.
    executor.dispatch(consumer, msgs("t", 0, [50, 51]))
    wait_until { executor.in_flight_count.zero? }
    executor.flush(consumer)
    expect(consumer.marked).to eq([50, 51, 50, 51])
  end
end

RSpec.describe "execution_mode configuration" do
  it "defaults to :superfetch" do
    expect(KafkaBatch.config.execution_mode).to eq(:superfetch)
    expect(KafkaBatch.config.watermark_mode?).to be(false)
  end

  it "routes KafkaBatch.job_executor by mode" do
    expect(KafkaBatch.job_executor).to be_a(KafkaBatch::SuperFetch::Executor)
    KafkaBatch.config.execution_mode = :watermark
    expect(KafkaBatch.config.watermark_mode?).to be(true)
    expect(KafkaBatch.job_executor).to be_a(KafkaBatch::Watermark::Executor)
  end

  it "rejects an unknown mode at validate!" do
    KafkaBatch.config.execution_mode = :bogus
    expect { KafkaBatch.config.validate! }.to raise_error(KafkaBatch::ConfigurationError, /execution_mode/)
  end
end

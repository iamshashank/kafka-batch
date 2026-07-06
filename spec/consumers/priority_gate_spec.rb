# frozen_string_literal: true

require "spec_helper"

RSpec.describe KafkaBatch::Consumers::PriorityGate do
  let(:consumer) do
    Class.new do
      include KafkaBatch::Consumers::PriorityGate
    end.new
  end

  let(:group)  { "kafka-batch-jobs-fast" }
  let(:topics) { %w[kafka_batch.jobs.p0 kafka_batch.jobs.p1] }

  it "returns true when any higher topic has lag" do
    allow(KafkaBatch::Lag).to receive(:read_group).with(group, topics).and_return(
      group => {
        "kafka_batch.jobs.p0" => { 0 => { lag: 0 } },
        "kafka_batch.jobs.p1" => { 0 => { lag: 3 } }
      }
    )

    expect(consumer.higher_topics_have_lag?(topics, group)).to be(true)
  end

  it "returns false when all higher topics have zero lag" do
    allow(KafkaBatch::Lag).to receive(:read_group).and_return(
      group => {
        "kafka_batch.jobs.p0" => { 0 => { lag: 0 } }
      }
    )

    expect(consumer.higher_topics_have_lag?(["kafka_batch.jobs.p0"], group)).to be(false)
  end

  it "caches lag results within priority_lag_check_interval" do
    allow(KafkaBatch::Lag).to receive(:read_group).and_return(
      group => { "kafka_batch.jobs.p0" => { 0 => { lag: 5 } } }
    )

    expect(consumer.higher_topics_have_lag?(["kafka_batch.jobs.p0"], group)).to be(true)
    expect(consumer.higher_topics_have_lag?(["kafka_batch.jobs.p0"], group)).to be(true)
    expect(KafkaBatch::Lag).to have_received(:read_group).once
  end

  it "fails open when the lag API errors" do
    allow(KafkaBatch::Lag).to receive(:read_group).and_raise(StandardError, "broker down")

    expect(consumer.higher_topics_have_lag?(topics, group)).to be(false)
  end
end

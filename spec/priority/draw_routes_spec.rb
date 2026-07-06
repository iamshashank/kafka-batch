# frozen_string_literal: true

require "spec_helper"
require_relative "../support/priority_workers"
require_relative "../support/route_capture"

RSpec.describe "KafkaBatch priority draw_routes" do
  let(:fast_yml) { File.expand_path("../fixtures/priority/fast.yml", __dir__) }
  let(:cg)       { KafkaBatch.config.consumer_group }

  before do
    KafkaBatch.config.priority_config_paths = [fast_yml]
    [PriorityP0Worker, PriorityP1Worker, SuccessfulWorker].each do |w|
      KafkaBatch.register_worker(w)
    end
  end

  it "wires priority topics into their own consumer group, not flat -jobs" do
    capture = KafkaBatchSpec::RouteCapture.new
    KafkaBatch.draw_routes(capture)

    expect(capture.groups["#{cg}-jobs-fast"]).to eq(%w[kafka_batch.jobs.p0 kafka_batch.jobs.p1])
    expect(capture.groups["#{cg}-jobs"]).to eq([SuccessfulWorker.kafka_topic])
    expect(capture.groups["#{cg}-jobs"]).not_to include("kafka_batch.jobs.p0", "kafka_batch.jobs.p1")
  end

  it "assigns PriorityJobConsumer subclasses with increasing rank" do
    capture = KafkaBatchSpec::RouteCapture.new
    KafkaBatch.draw_routes(capture)

    p0_klass = capture.consumers[["#{cg}-jobs-fast", "kafka_batch.jobs.p0"]]
    p1_klass = capture.consumers[["#{cg}-jobs-fast", "kafka_batch.jobs.p1"]]

    expect(p0_klass).to be < KafkaBatch::Consumers::PriorityJobConsumer
    expect(p1_klass).to be < KafkaBatch::Consumers::PriorityJobConsumer
    expect(p0_klass.priority_spec[:rank]).to eq(0)
    expect(p1_klass.priority_spec[:rank]).to eq(1)
    expect(p1_klass.priority_spec[:mode]).to eq(:weighted)
    expect(p1_klass.priority_spec[:higher_topics]).to eq(["kafka_batch.jobs.p0"])
  end

  it "includes priority groups in consumer_groups" do
    groups = KafkaBatch.consumer_groups
    expect(groups).to include("#{cg}-jobs-fast")
    expect(groups).to include("#{cg}-jobs")
  end

  it "loads priority registry from config paths" do
    registry = KafkaBatch.priority_registry
    expect(registry.configs.size).to eq(1)
    expect(registry.all_topics).to eq(%w[kafka_batch.jobs.p0 kafka_batch.jobs.p1])
  end
end

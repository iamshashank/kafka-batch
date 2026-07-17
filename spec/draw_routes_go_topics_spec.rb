# frozen_string_literal: true

require "spec_helper"
require_relative "support/route_capture"

RSpec.describe "KafkaBatch.draw_routes Go topic isolation" do
  let(:cg) { KafkaBatch.config.consumer_group }
  let(:handlers_yml) { File.expand_path("fixtures/handlers/go_ruby.yml", __dir__) }
  let(:fast_yml) { File.expand_path("fixtures/priority/fast.yml", __dir__) }
  let(:go_fast_yml) { File.expand_path("fixtures/priority/go_fast.yml", __dir__) }

  before do
    KafkaBatch.reset!
    KafkaBatch.configure do |c|
      c.daemon_mode = false
      c.handler_manifest_path = handlers_yml
      c.priority_config_paths = [fast_yml, go_fast_yml]
      c.fair_time_ready_go_topic = ""
      c.fair_time_ready_ruby_topic = ""
      c.fair_throughput_ready_go_topic = ""
      c.fair_throughput_ready_ruby_topic = ""
    end
    KafkaBatch::HandlerManifest.load!(handlers_yml)
    KafkaBatch.register_worker(SuccessfulWorker)
  end

  it "does not subscribe Ruby -jobs to Go plain topics from the manifest" do
    capture = KafkaBatchSpec::RouteCapture.new
    KafkaBatch.draw_routes(capture)

    expect(capture.groups["#{cg}-jobs"]).to include(SuccessfulWorker.kafka_topic)
    expect(capture.groups["#{cg}-jobs"]).not_to include("segment.exports")
  end

  it "does not draw Ruby routes for Go-only priority groups" do
    capture = KafkaBatchSpec::RouteCapture.new
    KafkaBatch.draw_routes(capture)

    expect(capture.groups).not_to have_key("#{cg}-jobs-go-fast")
    expect(capture.groups["#{cg}-jobs-fast"]).to eq(%w[kafka_batch.jobs.p1])
  end

  it "still exposes Go priority topics under go-worker groups for /lag" do
    groups = KafkaBatch::Lag.gem_groups_with_topics
    expect(groups["#{cg}-go-worker-jobs"]).to include("segment.exports")
    expect(groups["#{cg}-go-worker-jobs-go-fast"]).to eq(
      %w[kafka_batch.jobs.go_fast_p0 kafka_batch.jobs.go_fast_p1]
    )
    expect(groups["#{cg}-go-worker-jobs-fast"]).to eq(%w[kafka_batch.jobs.p0])
  end
end

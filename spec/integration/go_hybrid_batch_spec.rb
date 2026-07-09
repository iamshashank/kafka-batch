# frozen_string_literal: true

require_relative "../support/ruby_worker_integration_helper"
require_relative "../support/ruby_daemon_workers"

RSpec.describe "Go daemon hybrid batch (integration)", :integration do
  include KafkaBatchSpec::RubyWorkerIntegrationHelper
  include KafkaBatchSpec::GoWorkerLifecycle

  before(:each) do
    integration_preflight!
    KafkaBatchSpec::WorkerRuns.reset!
    @go_marker = nil
    @ruby_marker = nil
    start_ruby_worker_stack!(tmpdir_prefix: "kbatch-hybrid")
  end

  after(:each) do
    ENV.delete("KBATCH_GO_HYBRID_MARKER")
    ENV.delete("KBATCH_RUBY_HYBRID_MARKER")
    stop_ruby_worker_stack!
  end

  def write_manifest!
    @manifest_path = File.join(@tmpdir, "handlers.yml")
    @go_topic = "kb.hybrid.go.#{suffix}"
    @ruby_topic = "kb.hybrid.ruby.#{suffix}"
    @go_marker = File.join(@tmpdir, "go_marker")
    @ruby_marker = File.join(@tmpdir, "ruby_marker")
    ENV["KBATCH_GO_HYBRID_MARKER"] = @go_marker
    ENV["KBATCH_RUBY_HYBRID_MARKER"] = @ruby_marker

    File.write(@manifest_path, {
      "handlers" => {
        "integration.go_hybrid_partner" => {
          "runtime" => "go",
          "topic" => @go_topic,
          "apply_topic_prefix" => false,
          "max_retries" => 2
        },
        "integration.ruby_hybrid" => {
          "runtime" => "ruby",
          "worker_class" => "IntegrationRubyHybridWorker",
          "topic" => @ruby_topic,
          "apply_topic_prefix" => false,
          "max_retries" => 2
        }
      }
    }.to_yaml)
  end

  def extra_daemon_config
    {}
  end

  def integration_topics
    super + [@go_topic, @ruby_topic]
  end

  def after_stack_started!
    start_go_worker!
  end

  it "completes a batch with one Go job and one Ruby job on separate runtime topics" do
    go_id = ruby_id = nil
    batch = KafkaBatch::Batch.create(description: "hybrid #{suffix}") do |b|
      go_id   = b.push_job("integration.go_hybrid_partner", { "role" => "go" })
      ruby_id = b.push_job("integration.ruby_hybrid", { "role" => "ruby" })
    end

    wait_for_batch!(batch.id)

    expect(File.read(@go_marker)).to eq(go_id)
    expect(File.read(@ruby_marker)).to eq(ruby_id)

    names = KafkaBatchSpec::WorkerRuns.runs.map { |r| r[:name] }
    expect(names).to include(:integration_ruby_hybrid)

    reloaded = KafkaBatch.store.find_batch(batch.id)
    expect(reloaded[:status]).to eq("success")
    expect(reloaded[:completed_count]).to eq(2)
  end
end

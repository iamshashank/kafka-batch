# frozen_string_literal: true

require "spec_helper"

RSpec.describe KafkaBatch::Metrics do
  before { described_class.reset! }

  after do
    described_class.reset!
    KafkaBatch.config.metrics_enabled = false
  end

  it "forwards AS::Notification events to a statsd client" do
    client = double("statsd", increment: true, timing: true)
    KafkaBatch.configure do |c|
      c.metrics_enabled = true
      c.metrics_adapter = :statsd
      c.metrics_client  = client
    end

    described_class.install!(force: true)

    ActiveSupport::Notifications.instrument("job.processed.kafka_batch", job_id: "j1", batch_id: "b1", worker_class: "W") { sleep 0.001 }

    expect(client).to have_received(:increment).with("kafka_batch.job_processed.count", tags: array_including("job_id:j1"))
    expect(client).to have_received(:timing).with("kafka_batch.job_processed.duration", kind_of(Numeric), tags: anything)
  end

  it "statsd adapter maps notification events to client calls" do
    client = double("statsd", increment: true, timing: true)
    adapter = KafkaBatch::Metrics::StatsdAdapter.new(client, prefix: "kafka_batch")
    event = ActiveSupport::Notifications::Event.new(
      "job.processed.kafka_batch", Process.clock_gettime(Process::CLOCK_MONOTONIC),
      Process.clock_gettime(Process::CLOCK_MONOTONIC) + 0.01, "abc",
      { job_id: "j1", batch_id: "b1", worker_class: "W" }
    )
    adapter.call(event)
    expect(client).to have_received(:increment).with("kafka_batch.job_processed.count", tags: array_including("job_id:j1"))
  end

  it "forwards events to a custom proc adapter" do
    events = []
    KafkaBatch.configure do |c|
      c.metrics_enabled = true
      c.metrics_adapter = :proc
      c.metrics_proc    = ->(name, payload, duration_ms) { events << [name, payload, duration_ms] }
    end

    described_class.install!
    KafkaBatch::Instrumentation.dlt_published(dlt_type: "job", source_topic: "t")

    expect(events.size).to eq(1)
    expect(events.first[0]).to eq("dlt.published.kafka_batch")
  end
end

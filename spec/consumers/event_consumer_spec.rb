RSpec.describe KafkaBatch::Consumers::EventConsumer do
  let(:consumer) { build_consumer(described_class) }

  def event(id:, job_id:, status:)
    FakeMessage.new(
      topic:   KafkaBatch.config.events_topic,
      payload: { "batch_id" => id, "job_id" => job_id, "status" => status, "occurred_at" => Time.now.iso8601 }
    )
  end

  it "increments the batch and produces a callback when the batch finishes" do
    id = SecureRandom.uuid
    KafkaBatch.store.create_batch(id: id, total_jobs: 2, on_complete: "RecordingCallback")

    consumer.send(:process_event, event(id: id, job_id: "j1", status: "success"))
    expect(FakeProducer.for_topic(KafkaBatch.config.callbacks_topic)).to be_empty

    consumer.send(:process_event, event(id: id, job_id: "j2", status: "success"))
    cb = FakeProducer.for_topic(KafkaBatch.config.callbacks_topic)
    expect(cb.size).to eq(1)
    expect(cb.first.payload["outcome"]).to eq("success")
    expect(cb.first.payload["on_complete"]).to eq("RecordingCallback")
  end

  it "produces a complete (not success) callback when a job failed" do
    id = SecureRandom.uuid
    KafkaBatch.store.create_batch(id: id, total_jobs: 2)

    consumer.send(:process_event, event(id: id, job_id: "j1", status: "success"))
    consumer.send(:process_event, event(id: id, job_id: "j2", status: "failed"))

    expect(FakeProducer.for_topic(KafkaBatch.config.callbacks_topic).first.payload["outcome"]).to eq("complete")
  end

  it "is idempotent on duplicate events" do
    id = SecureRandom.uuid
    KafkaBatch.store.create_batch(id: id, total_jobs: 2)

    2.times { consumer.send(:process_event, event(id: id, job_id: "j1", status: "success")) }
    expect(KafkaBatch.store.find_batch(id)[:completed_count]).to eq(1)
  end

  it "routes malformed JSON to the DLT" do
    msg = FakeMessage.new(topic: KafkaBatch.config.events_topic, payload: "{bad")
    consumer.send(:process_event, msg)
    expect(FakeProducer.for_topic(KafkaBatch.config.dead_letter_topic).first.payload["dlt_type"]).to eq("malformed_event")
  end

  it "skips events with missing fields without producing anything" do
    msg = FakeMessage.new(topic: KafkaBatch.config.events_topic, payload: { "batch_id" => "x" })
    consumer.send(:process_event, msg)
    expect(FakeProducer.messages).to be_empty
  end
end

RSpec.describe KafkaBatch::Consumers::EventConsumer do
  let(:consumer) { build_consumer(described_class) }

  # Completion events carry the job message's source coordinates and are
  # deduplicated by a monotonic per-partition cursor.
  def event(id:, status:, src_offset:, src_partition: 0, src_topic: "wt")
    FakeMessage.new(
      topic:   KafkaBatch.config.events_topic,
      payload: {
        "batch_id"      => id,
        "job_id"        => "j#{src_offset}",
        "status"        => status,
        "occurred_at"   => Time.now.iso8601,
        "src_topic"     => src_topic,
        "src_partition" => src_partition,
        "src_offset"    => src_offset
      }
    )
  end

  it "increments the batch and produces a callback when the batch finishes" do
    id = SecureRandom.uuid
    KafkaBatch.store.create_batch(id: id, total_jobs: 2, on_complete: "RecordingCallback")

    consumer.send(:process_event, event(id: id, status: "success", src_offset: 10))
    expect(FakeProducer.for_topic(KafkaBatch.config.callbacks_topic)).to be_empty

    consumer.send(:process_event, event(id: id, status: "success", src_offset: 11))
    cb = FakeProducer.for_topic(KafkaBatch.config.callbacks_topic)
    expect(cb.size).to eq(1)
    expect(cb.first.payload["outcome"]).to eq("success")
    expect(cb.first.payload["on_complete"]).to eq("RecordingCallback")
  end

  it "applies a whole poll in one batch, counting every event exactly once" do
    id = SecureRandom.uuid
    KafkaBatch.store.create_batch(id: id, total_jobs: 3, on_complete: "RecordingCallback")

    msgs = [
      event(id: id, status: "success", src_offset: 10),
      event(id: id, status: "success", src_offset: 11),
      event(id: id, status: "success", src_offset: 10),  # duplicate offset – must not double-count
      event(id: id, status: "failed",  src_offset: 12)
    ]
    allow(consumer).to receive(:messages).and_return(msgs)

    consumer.consume

    b = KafkaBatch.store.find_batch(id)
    expect(b[:completed_count]).to eq(2)  # offset 10 counted once
    expect(b[:failed_count]).to eq(1)

    cb = FakeProducer.for_topic(KafkaBatch.config.callbacks_topic)
    expect(cb.size).to eq(1)
    expect(cb.first.payload["outcome"]).to eq("complete")
  end

  it "commits the poll exactly once (marks only the last message consumed)" do
    id = SecureRandom.uuid
    KafkaBatch.store.create_batch(id: id, total_jobs: 2)
    msgs = [event(id: id, status: "success", src_offset: 1), event(id: id, status: "success", src_offset: 2)]
    allow(consumer).to receive(:messages).and_return(msgs)

    consumer.consume
    expect(consumer).to have_received(:mark_as_consumed!).with(msgs.last).once
  end

  it "produces a complete (not success) callback when a job failed" do
    id = SecureRandom.uuid
    KafkaBatch.store.create_batch(id: id, total_jobs: 2)

    consumer.send(:process_event, event(id: id, status: "success", src_offset: 10))
    consumer.send(:process_event, event(id: id, status: "failed", src_offset: 11))

    expect(FakeProducer.for_topic(KafkaBatch.config.callbacks_topic).first.payload["outcome"]).to eq("complete")
  end

  it "deduplicates a re-produced/redelivered event by source offset" do
    id = SecureRandom.uuid
    KafkaBatch.store.create_batch(id: id, total_jobs: 2)

    2.times { consumer.send(:process_event, event(id: id, status: "success", src_offset: 10)) }
    expect(KafkaBatch.store.find_batch(id)[:completed_count]).to eq(1)
  end

  it "routes malformed JSON to the DLT" do
    msg = FakeMessage.new(topic: KafkaBatch.config.events_topic, payload: "{bad")
    consumer.send(:process_event, msg)
    expect(FakeProducer.for_topic(KafkaBatch.config.dead_letter_topic).first.payload["dlt_type"]).to eq("malformed_event")
  end

  it "skips events with missing batch_id/status without producing anything" do
    msg = FakeMessage.new(topic: KafkaBatch.config.events_topic, payload: { "batch_id" => "x" })
    consumer.send(:process_event, msg)
    expect(FakeProducer.messages).to be_empty
  end

  it "skips events missing source coordinates" do
    id = SecureRandom.uuid
    KafkaBatch.store.create_batch(id: id, total_jobs: 1)
    msg = FakeMessage.new(topic: KafkaBatch.config.events_topic,
                          payload: { "batch_id" => id, "status" => "success" })

    consumer.send(:process_event, msg)
    expect(KafkaBatch.store.find_batch(id)[:completed_count]).to eq(0)
  end
end

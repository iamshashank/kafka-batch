RSpec.describe KafkaBatch::Consumers::RetryConsumer do
  let(:consumer) { build_consumer(described_class) }

  def retry_message(retry_after:, retry_to: "test.success", offset: 5)
    FakeMessage.new(
      topic:   KafkaBatch.config.retry_topic,
      offset:  offset,
      payload: {
        "job_id"      => "j1",
        "attempt"     => 1,
        "payload"     => { "x" => 1 },
        "retry_after" => retry_after,
        "retry_to"    => retry_to
      }
    )
  end

  it "re-enqueues a due message to its original topic, stripping retry metadata" do
    consumer.send(:process_retry, retry_message(retry_after: (Time.now - 10).iso8601))

    msg = FakeProducer.for_topic("test.success").first
    expect(msg).not_to be_nil
    expect(msg.payload).not_to have_key("retry_after")
    expect(msg.payload).not_to have_key("retry_to")
    expect(msg.payload["attempt"]).to eq(1)
    expect(msg.key).to eq("j1")
  end

  it "pauses the partition (no produce) when the message is not yet due" do
    msg = retry_message(retry_after: (Time.now + 120).iso8601, offset: 9)
    consumer.send(:process_retry, msg)

    expect(consumer).to have_received(:pause).with(9, kind_of(Integer))
    expect(FakeProducer.for_topic("test.success")).to be_empty
  end

  it "stops the batch at the first not-yet-due message and never skips it (regression)" do
    not_due = retry_message(retry_after: (Time.now + 120).iso8601, offset: 9)
    later   = retry_message(retry_after: (Time.now - 10).iso8601,  offset: 10)
    allow(consumer).to receive(:messages).and_return([not_due, later])

    consumer.consume

    # Paused on the not-due head; must NOT advance past it by handling `later`.
    expect(consumer).to have_received(:pause).with(9, kind_of(Integer))
    expect(FakeProducer.for_topic("test.success")).to be_empty
    expect(consumer).not_to have_received(:mark_as_consumed!)
  end

  it "re-enqueues immediately when retry_after is missing (treats as due now)" do
    msg = retry_message(retry_after: nil, retry_to: "test.success")
    consumer.send(:process_retry, msg)

    expect(FakeProducer.for_topic("test.success").size).to eq(1)
    expect(consumer).not_to have_received(:pause)
  end

  it "fails the batch job AND DLTs an unroutable message (missing retry_to) so the batch can finish" do
    msg = FakeMessage.new(
      topic:   KafkaBatch.config.retry_topic,
      payload: { "job_id" => "j1", "batch_id" => "b1", "worker_class" => "W" }
    )
    consumer.send(:process_retry, msg)

    # Emits a failed completion event so the batch still drains (no silent drop).
    evt = FakeProducer.for_topic(KafkaBatch.config.events_topic).first
    expect(evt).not_to be_nil
    expect(evt.payload["status"]).to eq("failed")
    expect(evt.payload["batch_id"]).to eq("b1")

    dlt = FakeProducer.for_topic(KafkaBatch.config.dead_letter_topic)
    expect(dlt.first.payload["dlt_type"]).to eq("retry_routing")
  end

  it "does not emit an event for an unroutable standalone job (no batch_id)" do
    msg = FakeMessage.new(topic: KafkaBatch.config.retry_topic, payload: { "job_id" => "j1" })
    consumer.send(:process_retry, msg)

    expect(FakeProducer.for_topic(KafkaBatch.config.events_topic)).to be_empty
    expect(FakeProducer.for_topic(KafkaBatch.config.dead_letter_topic).first.payload["dlt_type"]).to eq("retry_routing")
  end
end

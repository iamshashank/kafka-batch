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

  it "routes a malformed retry message (missing fields) to the DLT" do
    msg = FakeMessage.new(topic: KafkaBatch.config.retry_topic, payload: { "job_id" => "j1" })
    consumer.send(:process_retry, msg)

    dlt = FakeProducer.for_topic(KafkaBatch.config.dead_letter_topic)
    expect(dlt.first.payload["dlt_type"]).to eq("retry_routing")
  end
end

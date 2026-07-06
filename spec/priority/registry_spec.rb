# frozen_string_literal: true

require "spec_helper"

RSpec.describe KafkaBatch::Priority::Registry do
  let(:cfg) { KafkaBatch.config }
  let(:fast) { File.expand_path("../fixtures/priority/fast.yml", __dir__) }
  let(:slow) { File.expand_path("../fixtures/priority/slow.yml", __dir__) }

  it "loads multiple configs" do
    registry = described_class.load([fast, slow], cfg: cfg)
    expect(registry.configs.size).to eq(2)
    expect(registry.all_topics.size).to eq(5)
  end

  it "raises when the same topic appears in two groups" do
    overlap = File.join(Dir.mktmpdir, "overlap.yml")
    File.write(overlap, <<~YAML)
      consumer_group_suffix: other
      mode: strict
      topics:
        - kafka_batch.jobs.p0
    YAML
    expect { described_class.load([fast, overlap], cfg: cfg) }
      .to raise_error(KafkaBatch::ConfigurationError, /multiple consumer groups/)
  ensure
    File.delete(overlap) if File.file?(overlap)
  end

  it "raises when a topic would also be on flat -jobs" do
    registry = described_class.load([fast], cfg: cfg)
    expect { registry.validate_plain_topics!(["kafka_batch.jobs.p1", "test.success"]) }
      .to raise_error(KafkaBatch::ConfigurationError, /flat -jobs/)
  end

  it "returns empty registry for no paths" do
    registry = described_class.load([], cfg: cfg)
    expect(registry).to be_empty
    expect(registry.all_topics).to eq([])
  end
end

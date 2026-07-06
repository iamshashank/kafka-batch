# frozen_string_literal: true

require "spec_helper"

RSpec.describe KafkaBatch::Priority::Config do
  let(:cfg) { KafkaBatch.config }
  let(:fixture) { File.expand_path("../fixtures/priority/fast.yml", __dir__) }

  it "loads a valid YAML file" do
    config = described_class.load(fixture, cfg: cfg)
    expect(config.consumer_group_suffix).to eq("jobs-fast")
    expect(config.consumer_group).to eq("#{cfg.consumer_group}-jobs-fast")
    expect(config.mode).to eq(:weighted)
    expect(config.topics).to eq(%w[kafka_batch.jobs.p0 kafka_batch.jobs.p1])
  end

  it "applies topic_prefix to topic names" do
    cfg.topic_prefix = "myapp"
    config = described_class.load(fixture, cfg: cfg)
    expect(config.topics).to eq(%w[myapp.kafka_batch.jobs.p0 myapp.kafka_batch.jobs.p1])
  end

  it "rejects the default jobs topic in a priority group" do
    path = File.join(Dir.mktmpdir, "bad.yml")
    File.write(path, <<~YAML)
      consumer_group_suffix: bad
      mode: strict
      topics:
        - kafka_batch.jobs
    YAML
    expect { described_class.load(path, cfg: cfg) }
      .to raise_error(KafkaBatch::ConfigurationError, /default jobs topic/)
  ensure
    File.delete(path) if File.file?(path)
  end

  it "rejects duplicate topics within one file" do
    path = File.join(Dir.mktmpdir, "dup.yml")
    File.write(path, <<~YAML)
      consumer_group_suffix: dup
      mode: strict
      topics:
        - kafka_batch.jobs.a
        - kafka_batch.jobs.a
    YAML
    expect { described_class.load(path, cfg: cfg) }
      .to raise_error(KafkaBatch::ConfigurationError, /duplicate topics/)
  ensure
    File.delete(path) if File.file?(path)
  end

  it "computes rank and cumulative higher topics" do
    config = described_class.load(fixture, cfg: cfg)
    expect(config.rank_for("kafka_batch.jobs.p1")).to eq(1)
    expect(config.higher_topics_for("kafka_batch.jobs.p1")).to eq(["kafka_batch.jobs.p0"])
    expect(config.higher_topics_for("kafka_batch.jobs.p0")).to eq([])
  end
end

RSpec.describe KafkaBatch::Configuration do
  describe "#resolved_priority_config_paths" do
    it "merges config paths with ENV variables" do
      cfg = KafkaBatch.config
      cfg.priority_config_paths = ["/app/a.yml"]
      allow(ENV).to receive(:[]).and_call_original
      allow(ENV).to receive(:[]).with("KAFKA_BATCH_PRIORITY_CONFIG").and_return("/app/b.yml")
      allow(ENV).to receive(:[]).with("KAFKA_BATCH_PRIORITY_CONFIGS").and_return("/app/c.yml,/app/d.yml")

      paths = cfg.resolved_priority_config_paths
      expect(paths).to include(
        File.expand_path("/app/a.yml"),
        File.expand_path("/app/b.yml"),
        File.expand_path("/app/c.yml"),
        File.expand_path("/app/d.yml")
      )
    end
  end
end

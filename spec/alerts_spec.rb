# frozen_string_literal: true

require "oj"
require "net/http"

RSpec.describe KafkaBatch::Alerts::Settings do
  before do
    skip "Redis unavailable" unless KafkaBatchSpec::RedisHelper.available?
    KafkaBatch.config.redis_url = KafkaBatchSpec::RedisHelper::TEST_URL
    KafkaBatch.config.ai_encryption_salt = "alerts-salt"
    described_class.reset_pool!
    KafkaBatchSpec::RedisHelper.flush!
  end

  after do
    described_class.reset_pool!
    KafkaBatch.config.ai_encryption_salt = ""
  end

  it "merges library defaults and encrypts secrets" do
    shown = described_class.update!(
      enabled: true,
      lag_threshold: 42,
      channel_slack: true,
      slack_webhook_url: "https://hooks.slack.com/services/T/B/xxxSECRET"
    )
    expect(shown["enabled"]).to eq(true)
    expect(shown["lag_threshold"]).to eq(42)
    expect(shown["secrets"]["slack_webhook_url"]["set"]).to eq(true)
    expect(shown["secrets"]["slack_webhook_url"]["masked"]).to include("CRET")

    eff = described_class.effective
    expect(eff["slack_webhook_url"]).to include("hooks.slack.com")
    expect(described_class.version.to_i).to be >= 1
  end

  it "keeps existing secret when blank on update" do
    described_class.update!(slack_webhook_url: "https://hooks.example/a")
    described_class.update!(enabled: true, slack_webhook_url: "")
    expect(described_class.effective["slack_webhook_url"]).to eq("https://hooks.example/a")
  end
end

RSpec.describe KafkaBatch::Alerts::Rules::LagStuckGrowing do
  it "fires when committed stuck and lag grows" do
    cfg = {
      "lag_threshold" => 100,
      "lag_growth_min" => 10,
      "rules" => { "lag_stuck_growing" => { "enabled" => true, "severity" => "critical" } }
    }
    sample = {
      "lag_baseline" => {
        "g|t" => { "committed" => 10, "end_sum" => 200, "lag" => 190 }
      },
      "paused_keys" => [],
      "lag_topics" => [
        {
          "group" => "g",
          "topic" => "t",
          "lag" => 250,
          "committed_sum" => 10,
          "end_sum" => 260
        }
      ]
    }
    findings = described_class.new(cfg).evaluate(sample)
    expect(findings.size).to eq(1)
    expect(findings.first.fingerprint).to include("lag_stuck_growing")
  end

  it "skips paused topics" do
    cfg = { "lag_threshold" => 1, "lag_growth_min" => 1, "rules" => {} }
    sample = {
      "lag_baseline" => { "g|t" => { "committed" => 1, "end_sum" => 2, "lag" => 1 } },
      "paused_keys" => ["g\x1ft"],
      "lag_topics" => [
        { "group" => "g", "topic" => "t", "lag" => 100, "committed_sum" => 1, "end_sum" => 101 }
      ]
    }
    expect(described_class.new(cfg).evaluate(sample)).to be_empty
  end
end

RSpec.describe KafkaBatch::Alerts::State do
  before do
    skip "Redis unavailable" unless KafkaBatchSpec::RedisHelper.available?
    KafkaBatch.config.redis_url = KafkaBatchSpec::RedisHelper::TEST_URL
    described_class.reset_pool!
    KafkaBatchSpec::RedisHelper.flush!
  end

  after { described_class.reset_pool! }

  it "tracks breach hysteresis counters" do
    expect(described_class.breach_count("fp1")).to eq(0)
    described_class.incr_breach!("fp1")
    described_class.incr_breach!("fp1")
    expect(described_class.breach_count("fp1")).to eq(2)
    described_class.reset_breach!("fp1")
    expect(described_class.breach_count("fp1")).to eq(0)
  end

  it "opens and clears incidents" do
    described_class.set_open!("fp", "fingerprint" => "fp", "rule_id" => "x", "title" => "t")
    expect(described_class.open_alerts.size).to eq(1)
    expect(described_class.clear_open!("fp")).to eq(true)
    expect(described_class.open_alerts).to be_empty
    expect(described_class.clear_open!("fp")).to eq(false)
  end

  it "claims open and notify at most once (no duplicate Slack)" do
    incident = { "fingerprint" => "dup", "rule_id" => "x", "title" => "t", "fired_at" => "t1" }
    expect(described_class.claim_open!("dup", incident)).to eq(true)
    expect(described_class.claim_open!("dup", incident)).to eq(false)

    expect(described_class.claim_notify!("dup", "fired:t1", ttl: 120)).to eq(true)
    expect(described_class.claim_notify!("dup", "fired:t1", ttl: 120)).to eq(false)
  end
end

RSpec.describe KafkaBatch::Alerts::Evaluator do
  before do
    skip "Redis unavailable" unless KafkaBatchSpec::RedisHelper.available?
    KafkaBatch.config.redis_url = KafkaBatchSpec::RedisHelper::TEST_URL
    KafkaBatch::Alerts::Settings.reset_pool!
    KafkaBatch::Alerts::State.reset_pool!
    KafkaBatchSpec::RedisHelper.flush!
  end

  after do
    KafkaBatch::Alerts::Settings.reset_pool!
    KafkaBatch::Alerts::State.reset_pool!
  end

  it "does not re-notify while an incident stays open" do
    delivers = 0
    allow_any_instance_of(KafkaBatch::Alerts::Notifiers::Multi).to receive(:deliver) { delivers += 1 }

    finding = KafkaBatch::Alerts::Rules::Finding.new(
      rule_id: "lag_stuck_growing",
      fingerprint: "lag_stuck_growing:g:t",
      title: "Lag",
      summary: "stuck",
      severity: "critical",
      link: "/lag",
      sample: {}
    )
    cfg = {
      "enabled" => true,
      "for_ticks" => 1,
      "resolve_ticks" => 2,
      "interval" => 60,
      "cooldown_seconds" => 1,
      "rules" => {}
    }

    allow(KafkaBatch::Alerts::Sampler).to receive(:collect).and_return({})
    allow(KafkaBatch::Alerts::Sampler).to receive(:persist_baseline!)
    allow(KafkaBatch::Alerts::Evaluator).to receive(:run_rules).and_return([finding])

    # Force private path via evaluate_once with stubbed rules — call apply via evaluate
    # by stubbing run_rules through send after opening lock.
    3.times do
      KafkaBatch::Alerts::Evaluator.evaluate_once!(config: cfg)
    end

    # First tick opens + notifies once; subsequent ticks while still open must not notify.
    expect(delivers).to eq(1)
    expect(KafkaBatch::Alerts::State.get_open(finding.fingerprint)).not_to be_nil
  end
end

RSpec.describe KafkaBatch::Alerts::Notifiers::Slack do
  it "posts JSON to the webhook URL" do
    response = instance_double(Net::HTTPResponse, is_a?: true)
    allow(response).to receive(:is_a?).with(Net::HTTPSuccess).and_return(true)
    http = instance_double(Net::HTTP)
    allow(Net::HTTP).to receive(:new).and_return(http)
    allow(http).to receive(:use_ssl=)
    allow(http).to receive(:open_timeout=)
    allow(http).to receive(:read_timeout=)
    expect(http).to receive(:request) do |req|
      body = Oj.load(req.body)
      expect(body["blocks"]).to be_a(Array)
      response
    end

    payload = KafkaBatch::Alerts::Payload.test(channel: "slack")
    expect(described_class.new(webhook_url: "https://hooks.example/slack").deliver(payload)).to eq(true)
  end
end

RSpec.describe KafkaBatch::Alerts::Availability do
  it "marks slack unavailable without encryption salt" do
    KafkaBatch.config.ai_encryption_salt = ""
    channels = described_class.channels(
      "channel_slack" => true,
      "slack_webhook_url" => nil,
      "channel_webhook" => false,
      "webhook_urls" => [],
      "channel_email" => false,
      "email_to" => "",
      "email_smtp_address" => "",
      "channel_metrics" => false
    )
    slack = channels.find { |c| c["id"] == "slack" }
    expect(slack["available"]).to eq(false)
    expect(slack["unavailable_reason"]).to match(/encryption_salt/)
  end
end

RSpec.describe "Alerts web API", type: :request do
  # Exercised via KafkaBatch::Web in web_spec style when Redis is up —
  # covered lightly here through Settings + Availability only.
  it "exposes rule metadata for all v1 rules" do
    meta = KafkaBatch::Alerts::Rules.metadata
    ids = meta.map { |r| r["id"] }
    expect(ids).to include(
      "lag_stuck_growing", "redis_rtt_high", "no_live_consumers", "reconciler_stale",
      "fairness_ingest_backed_up", "dlt_rate_high", "schedule_depth_high", "cron_stale"
    )
    lag = meta.find { |r| r["id"] == "lag_stuck_growing" }
    expect(lag["detail"]).to be_a(String)
    expect(lag["remediation"]).to be_a(String)
    expect(lag["settings"].map { |s| s["key"] }).to include("lag_threshold", "lag_growth_min")
  end
end

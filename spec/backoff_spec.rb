RSpec.describe KafkaBatch::Backoff do
  it "uses first_delay for the first retry and interval thereafter (no jitter)" do
    expect(described_class.delay(next_attempt: 1, first_delay: 10, interval: 180, jitter: 0)).to eq(10.0)
    expect(described_class.delay(next_attempt: 2, first_delay: 10, interval: 180, jitter: 0)).to eq(180.0)
    expect(described_class.delay(next_attempt: 5, first_delay: 10, interval: 180, jitter: 0)).to eq(180.0)
  end

  it "applies +/- jitter within bounds" do
    50.times do
      d = described_class.delay(next_attempt: 2, first_delay: 10, interval: 180, jitter: 0.1)
      expect(d).to be_between(180 * 0.9, 180 * 1.1)
    end
  end

  it "treats zero/negative jitter as exact" do
    expect(described_class.delay(next_attempt: 2, first_delay: 10, interval: 180, jitter: 0)).to eq(180.0)
    expect(described_class.delay(next_attempt: 1, first_delay: 7, interval: 180, jitter: 0)).to eq(7.0)
  end
end

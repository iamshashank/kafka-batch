RSpec.describe KafkaBatch::Backoff do
  it "starts at base and ends at the cap (last retry lands at 24h)" do
    base = 5
    cap  = 24 * 3600
    max  = 4

    first = described_class.delay(next_attempt: 1,   max_retries: max, base: base, cap: cap)
    last  = described_class.delay(next_attempt: max, max_retries: max, base: base, cap: cap)

    expect(first).to be_within(0.001).of(5)
    expect(last).to be_within(0.001).of(cap)
  end

  it "grows monotonically and stays within [base, cap]" do
    delays = (1..5).map do |n|
      described_class.delay(next_attempt: n, max_retries: 5, base: 10, cap: 24 * 3600)
    end

    expect(delays).to eq(delays.sort)            # increasing
    expect(delays.first).to be_within(0.001).of(10)
    expect(delays.last).to be_within(0.001).of(24 * 3600)
    expect(delays).to all(be <= 24 * 3600)
  end

  it "never exceeds the cap, even past the last attempt" do
    expect(described_class.delay(next_attempt: 99, max_retries: 3, base: 5, cap: 24 * 3600)).to be <= 24 * 3600
  end

  it "uses the cap for a single-retry worker" do
    expect(described_class.delay(next_attempt: 1, max_retries: 1, base: 5, cap: 24 * 3600)).to eq((24 * 3600).to_f)
  end
end

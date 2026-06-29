RSpec.describe KafkaBatch::Partition do
  # Vectors from Apache Kafka UtilsTest.testMurmur2
  it "matches Kafka's murmur2 for known keys" do
    expect(signed_murmur2("foobar")).to eq(-790_332_482)
    expect(signed_murmur2("abc")).to eq(479_470_107)
    expect(described_class.for_key("foobar", 3)).to eq(described_class.to_positive(-790_332_482) % 3)
  end

  def signed_murmur2(str)
    h = described_class.murmur2(str.b)
    h >= 0x80000000 ? h - 0x100000000 : h
  end

  it "returns partition 0 when there is only one partition" do
    expect(described_class.for_key("any-tenant", 1)).to eq(0)
  end

  it "is stable for the same tenant_id" do
    a = described_class.for_key("acme", 128)
    b = described_class.for_key("acme", 128)
    expect(a).to eq(b)
  end

  it "raises when partition_count is not positive" do
    expect { described_class.for_key("x", 0) }.to raise_error(ArgumentError)
  end
end

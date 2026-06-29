module KafkaBatch
  # Kafka's default partitioner: murmur2(key bytes) → positive hash mod partition count.
  # Matches librdkafka / the Java producer so ingest keys land on the same
  # partition the broker would assign.
  module Partition
    module_function

    # @param key [String]
    # @param partition_count [Integer]
    # @return [Integer] partition in 0..partition_count-1
    def for_key(key, partition_count)
      raise ArgumentError, "partition_count must be positive" unless partition_count.to_i.positive?

      to_positive(murmur2(key.to_s.b)) % partition_count.to_i
    end

    # Apache Kafka org.apache.kafka.common.utils.Utils.murmur2
    # @param data [String] binary string
    # @return [Integer] 32-bit hash (unsigned)
    def murmur2(data)
      length  = data.bytesize
      seed    = 0x9747b28c
      m       = 0x5bd1e995
      r       = 24
      h       = u32(seed ^ length)
      length4 = length >> 2

      length4.times do |i|
        i4 = i << 2
        k  = u32((data.getbyte(i4) & 0xff) |
                 ((data.getbyte(i4 + 1) & 0xff) << 8) |
                 ((data.getbyte(i4 + 2) & 0xff) << 16) |
                 ((data.getbyte(i4 + 3) & 0xff) << 24))
        k  = u32(k * m)
        k  = u32(k ^ (k >> r))
        k  = u32(k * m)
        h  = u32(h * m)
        h  = u32(h ^ k)
      end

      index = length4 << 2
      rem   = length - index
      if rem >= 3
        h = u32(h ^ ((data.getbyte(index + 2) & 0xff) << 16))
      end
      if rem >= 2
        h = u32(h ^ ((data.getbyte(index + 1) & 0xff) << 8))
      end
      if rem >= 1
        h = u32(h ^ (data.getbyte(index) & 0xff))
        h = u32(h * m)
      end

      h = u32(h ^ (h >> 13))
      h = u32(h * m)
      u32(h ^ (h >> 15))
    end

    # Kafka Utils.toPositive
    def to_positive(number)
      u32(number) & 0x7fffffff
    end

    def u32(n)
      n & 0xffffffff
    end
  end
end

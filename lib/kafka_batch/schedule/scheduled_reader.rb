require "rdkafka"

module KafkaBatch
  module Schedule
    # Reads job payloads back from the durable `scheduled_topic` by (partition,
    # offset). The schedule index only stores compact pointers; this is how the
    # poller recovers the actual message to re-produce when a job comes due.
    #
    # Efficiency: callers pass a whole claimed batch grouped by partition and
    # sorted by offset. For each partition we assign + seek ONCE to the lowest
    # needed offset and poll FORWARD; librdkafka serves a contiguous fetch buffer,
    # so consecutive needed offsets cost no extra round-trips — scattered point
    # reads become near-sequential consumption.
    #
    # A long-lived, assign-based (no subscription/rebalance) consumer is reused
    # across calls; only the assignment changes per drain.
    class ScheduledReader
      # How long to wait for each poll before giving up on a partition (ms).
      POLL_TIMEOUT_MS = 2_000
      # Safety cap: max messages scanned forward on a partition per read call,
      # beyond the span of needed offsets, before we stop (guards against a bad
      # pointer sending us on an unbounded scan).
      SCAN_SLACK = 1_000

      def initialize(topic: nil, consumer: nil)
        @topic    = topic || KafkaBatch.config.scheduled_topic
        @consumer = consumer  # injectable for tests
      end

      # @param by_partition [Hash{Integer => Array<Integer>}] needed offsets per
      #   partition (each array will be sorted ascending here).
      # @return [Hash] {
      #   found: { "partition:offset" => payload_string },
      #   lost:  [ "partition:offset", ... ]   # offset below low watermark (retention-deleted)
      # }
      def read(by_partition)
        found = {}
        lost  = []
        return { found: found, lost: lost } if by_partition.nil? || by_partition.empty?

        by_partition.each do |partition, offsets|
          wanted = offsets.uniq.sort
          next if wanted.empty?

          read_partition(partition.to_i, wanted, found, lost)
        end

        { found: found, lost: lost }
      end

      def close
        @consumer&.close
      rescue StandardError
        # best-effort
      ensure
        @consumer = nil
      end

      private

      def read_partition(partition, wanted, found, lost)
        low, high = consumer.query_watermark_offsets(@topic, partition, POLL_TIMEOUT_MS)

        # Offsets below the low watermark have been removed by log retention — they
        # are unrecoverable. Surface them so the poller can drop (not retry forever).
        need = []
        wanted.each do |off|
          if off < low
            lost << Member.build_key(partition, off)
          elsif off >= high
            # Not yet readable (shouldn't happen: we produced before scheduling).
            # Leave it un-found so the poller retries via the lease/reclaim path.
          else
            need << off
          end
        end
        return if need.empty?

        assign_at(partition, need.first)
        remaining  = need.dup
        max_offset = need.last
        scan_until = max_offset + SCAN_SLACK

        while !remaining.empty?
          msg = consumer.poll(POLL_TIMEOUT_MS)
          break if msg.nil?                       # no more data within timeout
          next  if msg.partition != partition
          break if msg.offset > scan_until        # overshot; stop scanning

          if remaining.include?(msg.offset)
            found[Member.build_key(partition, msg.offset)] = msg.payload
            remaining.delete(msg.offset)
          end
        end
      rescue Rdkafka::RdkafkaError => e
        # Transient read error (e.g. offset moved out of range between the
        # watermark query and the fetch). Leave the unfound offsets un-acked so
        # the lease/reclaim path retries them; never crash the poller.
        KafkaBatch.logger.warn(
          "[KafkaBatch][ScheduledReader] read error on #{@topic}/#{partition}: #{e.message}"
        )
      ensure
        unassign
      end

      def assign_at(partition, offset)
        tpl = Rdkafka::Consumer::TopicPartitionList.new
        tpl.add_topic_and_partitions_with_offsets(@topic, partition => offset)
        consumer.assign(tpl)
      end

      def unassign
        consumer.assign(Rdkafka::Consumer::TopicPartitionList.new)
      rescue StandardError
        # best-effort
      end

      def consumer
        @consumer ||= build_consumer
      end

      def build_consumer
        cfg = KafkaBatch.config
        base = {
          "bootstrap.servers"  => Array(cfg.brokers).join(","),
          # Assign-based reads need a group.id to construct the consumer, but we
          # never subscribe or commit — no rebalance, no offset storage.
          "group.id"           => "#{cfg.consumer_group}-schedule-reader",
          "enable.auto.commit" => false,
          # We pre-filter offsets against the watermarks, so an assigned offset is
          # always in range; "error" surfaces the rare race (retention advanced
          # after the watermark query) as an RdkafkaError we rescue and retry.
          "auto.offset.reset"  => "error"
        }
        overrides = (cfg.consumer_config || {}).each_with_object({}) do |(k, v), h|
          h[k.to_s] = v
        end
        Rdkafka::Config.new(base.merge(overrides)).consumer
      end
    end
  end
end

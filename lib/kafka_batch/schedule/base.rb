module KafkaBatch
  # Delayed-job index (the Sidekiq perform_in / perform_at equivalent).
  #
  # This is NOT the batch ledger — it is a small, separate index that answers one
  # question efficiently: "which jobs are due to run now?" The job PAYLOAD is not
  # stored here; it lives in Kafka (config.scheduled_topic). Each entry is a
  # COMPACT POINTER to that message:
  #
  #   member = "<job_id>:<partition>:<offset>"   scored by run-at (epoch seconds)
  #
  # so the index size is independent of payload size. Two backends implement the
  # same interface, chosen by config.schedule_store (detached from config.store):
  #
  #   Schedule::RedisStore  – ZSET, RAM-resident, lowest latency.
  #   Schedule::MysqlStore  – table, disk-resident, cheap at scale, native cancel.
  #
  # At-least-once delivery: claim_due moves due entries into a LEASED state; the
  # SchedulePoller re-produces them and only then calls #ack. If the process
  # crashes between claim and ack, the lease expires and #reclaim (run by any
  # poller in any process) returns the entry to the pending set — so nothing is
  # lost. Duplicates are rare and safe (the JobConsumer dedups completions by
  # job_id and the producer is idempotent).
  module Schedule
    # Encode/parse the compact pointer. partition and offset are always the last
    # two colon-delimited fields (both integers); the job_id occupies everything
    # before them, so we split from the RIGHT — a job_id that itself contains ':'
    # is still parsed correctly.
    module Member
      module_function

      def build(job_id, partition, offset)
        "#{job_id}:#{partition}:#{offset}"
      end

      # Location-only key "partition:offset" — how the reader keys payloads it
      # fetched (it can't know job_id from an offset). The poller maps a member's
      # location to its payload via this key.
      def build_key(partition, offset)
        "#{partition}:#{offset}"
      end

      def key_of(member)
        p = parse(member)
        p && build_key(p[:partition], p[:offset])
      end

      # @return [Hash, nil] { job_id:, partition:(Integer), offset:(Integer) }
      def parse(member)
        rest, _, offset = member.to_s.rpartition(":")
        job_id, _, part = rest.rpartition(":")
        return nil if job_id.empty? || part.empty? || offset.empty?

        { job_id: job_id, partition: part.to_i, offset: offset.to_i }
      end

      def job_id_of(member)
        parse(member)&.fetch(:job_id)
      end
    end

    class Base
      # Persist a delayed job pointer.
      # @param run_at [Time, Float] absolute time (or epoch seconds) the job is due
      def schedule(job_id:, run_at:, partition:, offset:, batch_id: nil)
        raise NotImplementedError, "#{self.class}#schedule"
      end

      # Persist many delayed job pointers in one shot (bulk perform_in). Backends
      # override with a single atomic write; the default falls back to N #schedule
      # calls.
      # @param entries [Array<Hash>] each { job_id:, run_at:, partition:, offset:, batch_id: }
      # @return [Array<String>] the member strings, in order
      def schedule_many(entries)
        entries.map { |e| schedule(**e) }
      end

      # Atomically claim up to +limit+ entries whose run-at <= now, moving them to
      # a leased state (lease expires at now + lease_seconds). Safe to call from
      # many processes concurrently — no entry is handed to two pollers.
      # @return [Array<String>] claimed member strings ("job_id:partition:offset")
      def claim_due(now:, lease_seconds:, limit:)
        raise NotImplementedError, "#{self.class}#claim_due"
      end

      # Permanently remove leased entries once their job has been re-produced.
      # @param members [Array<String>]
      def ack(members)
        raise NotImplementedError, "#{self.class}#ack"
      end

      # Return leased entries whose lease has expired (crashed poller) to the
      # pending set so another poller re-dispatches them.
      # @return [Integer] number of entries reclaimed
      def reclaim(now:)
        raise NotImplementedError, "#{self.class}#reclaim"
      end

      # Cancel a still-pending scheduled job by job_id.
      # @return [Boolean] true if it was pending and got removed. The Redis
      #   backend cannot remove by job_id alone (see class docs) and returns
      #   false; cancellation there is honoured by the poller via CancellationCache.
      def cancel(job_id)
        raise NotImplementedError, "#{self.class}#cancel"
      end

      # List pending entries, soonest-due first, for the dashboard.
      # @return [Array<Hash>] { job_id:, partition:, offset:, run_at:, batch_id: }
      def list(limit: 100, offset: 0)
        raise NotImplementedError, "#{self.class}#list"
      end

      # Look up a single scheduled job by id (dashboard search).
      # @return [Hash, nil] { job_id:, partition:, offset:, run_at:, batch_id:, state: :pending|:leased }
      def find(job_id)
        raise NotImplementedError, "#{self.class}#find"
      end

      # Count of pending (not-yet-due + due-but-unclaimed) entries.
      # @return [Integer]
      def size
        raise NotImplementedError, "#{self.class}#size"
      end
    end
  end
end

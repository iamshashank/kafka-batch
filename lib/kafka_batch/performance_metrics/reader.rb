# frozen_string_literal: true

module KafkaBatch
  module PerformanceMetrics
    # Reads the per-bucket Redis hashes written by PerformanceMetrics.record
    # and assembles the payload for GET /api/performance. All ranges read from
    # the same buckets (config.performance_metrics_bucket_seconds, default
    # 60s); wide ranges (24h) are downsampled server-side to ~300 points so
    # the browser never has to chart 1000+ raw buckets.
    class Reader
      RANGES = {
        "5m"  => 5 * 60,
        "1h"  => 60 * 60,
        "3h"  => 3 * 60 * 60,
        "24h" => 24 * 60 * 60
      }.freeze
      DEFAULT_RANGE = "1h"
      DOWNSAMPLE_TARGET_POINTS = 300
      DEFAULT_TOP_N = 10
      COUNT_STATUSES = %i[processed failed retried].freeze

      # @param range [String] one of RANGES.keys; unknown values fall back to "1h"
      # @param job_types [Array<String>, nil] when present, restrict the
      #   per-job-type panel to exactly these types instead of auto top-N.
      # @param top_n [Integer] how many job types to include when job_types is nil
      def fetch(range: DEFAULT_RANGE, job_types: nil, top_n: DEFAULT_TOP_N)
        range = RANGES.key?(range.to_s) ? range.to_s : DEFAULT_RANGE
        bucket_secs = PerformanceMetrics.bucket_seconds
        window      = RANGES[range]
        n_buckets   = [(window / bucket_secs.to_f).ceil, 1].max
        latest      = PerformanceMetrics.bucket_start(Time.now)
        starts      = Array.new(n_buckets) { |i| latest - (n_buckets - 1 - i) * bucket_secs }

        raw = load_buckets(starts)
        group_size = downsample_group_size(n_buckets)
        points, job_type_totals, job_type_series = build_points(starts, raw, group_size)

        totals = Hash.new(0)
        points.each { |p| (COUNT_STATUSES + [:reclaimed]).each { |k| totals[k] += p[k] } }

        {
          range: range,
          bucket_seconds: bucket_secs * group_size,
          points: points,
          job_types: build_job_type_rows(job_type_totals, job_type_series, job_types, top_n),
          totals: totals
        }
      end

      private

      # @return [Hash{[Symbol, Integer] => Hash{String=>String}}]
      def load_buckets(starts)
        futures = {}
        result = PerformanceMetrics.redis_with do |r|
          r.pipelined do |pipe|
            PerformanceMetrics::STATUSES.each do |status|
              starts.each do |b|
                futures[[status, b]] = pipe.hgetall(PerformanceMetrics.bucket_key(status, Time.at(b)))
              end
            end
          end
        end
        return {} if result.nil?

        futures.transform_values(&:value)
      rescue StandardError => e
        KafkaBatch.logger.debug("[KafkaBatch][PerformanceMetrics::Reader] load_buckets failed: #{e.message}")
        {}
      end

      def downsample_group_size(n_buckets)
        return 1 if n_buckets <= DOWNSAMPLE_TARGET_POINTS

        (n_buckets / DOWNSAMPLE_TARGET_POINTS.to_f).ceil
      end

      # @return [Array(Array<Hash>, Hash{String=>Hash}, Array<Hash{String=>Integer}>)]
      def build_points(starts, raw, group_size)
        job_type_totals = {}
        job_type_series = []
        points = []

        starts.each_slice(group_size) do |group|
          bucket = { t: group.first, processed: 0, failed: 0, retried: 0, reclaimed: 0 }
          jt_processed = Hash.new(0)

          group.each do |b|
            PerformanceMetrics::STATUSES.each do |status|
              fields = raw[[status, b]]
              next if fields.nil? || fields.empty?

              fields.each do |field, count_str|
                count = count_str.to_i
                next unless count.positive?

                if field == PerformanceMetrics::ALL_FIELD
                  bucket[status] += count
                elsif field != PerformanceMetrics::OTHER_FIELD && COUNT_STATUSES.include?(status)
                  totals = (job_type_totals[field] ||= { processed: 0, failed: 0, retried: 0 })
                  totals[status] += count
                  jt_processed[field] += count if status == :processed
                end
              end
            end
          end

          points << bucket
          job_type_series << jt_processed
        end

        [points, job_type_totals, job_type_series]
      end

      def build_job_type_rows(job_type_totals, job_type_series, requested_types, top_n)
        types =
          if requested_types && !requested_types.empty?
            requested_types
          else
            job_type_totals.keys.sort_by { |jt| -job_type_totals[jt][:processed] }.first(top_n.to_i.positive? ? top_n.to_i : DEFAULT_TOP_N)
          end

        types.map do |jt|
          totals = job_type_totals[jt] || { processed: 0, failed: 0, retried: 0 }
          {
            job_type: jt,
            processed: totals[:processed].to_i,
            failed: totals[:failed].to_i,
            retried: totals[:retried].to_i,
            sparkline: job_type_series.map { |h| h[jt] || 0 }
          }
        end
      end
    end
  end
end

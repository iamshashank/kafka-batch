RSpec.describe KafkaBatch::Web do
  def get(path, query: "")
    KafkaBatch::Web.call(
      "REQUEST_METHOD" => "GET", "PATH_INFO" => path,
      "SCRIPT_NAME" => "/kafka_batch", "QUERY_STRING" => query
    )
  end

  def post(path)
    KafkaBatch::Web.call(
      "REQUEST_METHOD" => "POST", "PATH_INFO" => path,
      "SCRIPT_NAME" => "/kafka_batch", "QUERY_STRING" => ""
    )
  end

  def seed(total: 3, **opts)
    id = SecureRandom.uuid
    KafkaBatch.store.create_batch(id: id, total_jobs: total, **opts)
    id
  end

  describe "GET /" do
    it "marks dashboard responses no-store so a refresh always shows current data" do
      _s, headers, = get("/")
      expect(headers["cache-control"]).to eq("no-store")
    end

    it "reflects live store counts on each request" do
      seed(total: 5)
      first = get("/").last.join
      seed(total: 3)
      second = get("/").last.join
      # the Total metric increases between requests (no caching, fresh read)
      expect(first).not_to eq(second)
    end

    it "renders the batch list with metrics" do
      id = seed(on_complete: "RecordingCallback")
      status, headers, body = get("/")
      html = body.join

      expect(status).to eq(200)
      expect(headers["content-type"]).to match(%r{text/html})
      expect(html).to include("KafkaBatch")
      expect(html).to include("metric-value")          # summary cards
      expect(html).to include(id[0, 8])                # batch row
      expect(html).to include("Pending")
    end

    it "filters by status" do
      running = seed
      cancelled = seed
      KafkaBatch::Batch.cancel(cancelled)

      html = get("/", query: "status=cancelled").last.join
      expect(html).to include(cancelled[0, 8])
      expect(html).not_to include(running[0, 8])
    end
  end

  describe "GET /failures (all batches)" do
    it "lists failures across batches and the dashboard links to it" do
      a = seed(total: 1)
      b = seed(total: 1)
      KafkaBatch.store.record_failure(batch_id: a, job_id: "ja", worker_class: "W", error_class: "Boom", error_message: "msg-a", status: "failed")
      KafkaBatch.store.record_failure(batch_id: b, job_id: "jb", worker_class: "W", error_class: "Nope", error_message: "msg-b", status: "retrying")

      # dashboard has a link to the global failures page
      expect(get("/").last.join).to include("/kafka_batch/failures")

      html = get("/failures").last.join
      expect(html).to include("Failures across all batches")
      expect(html).to include("Boom")
      expect(html).to include("Nope")
      expect(html).to include(a[0, 8])
      expect(html).to include(b[0, 8])
    end

    it "filters by status" do
      a = seed(total: 1)
      KafkaBatch.store.record_failure(batch_id: a, job_id: "ja", worker_class: "W", error_class: "PermanentBoom", error_message: "m", status: "failed")
      KafkaBatch.store.record_failure(batch_id: a, job_id: "jr", worker_class: "W", error_class: "FlakyTimeout", error_message: "m", status: "retrying")

      html = get("/failures", query: "status=failed").last.join
      expect(html).to include("PermanentBoom")
      expect(html).not_to include("FlakyTimeout")
    end
  end

  describe "GET /live" do
    it "shows running jobs and consumers when Redis is available" do
      skip "Redis unavailable" unless KafkaBatchSpec::RedisHelper.available?
      KafkaBatch.config.redis_url          = KafkaBatchSpec::RedisHelper::TEST_URL
      KafkaBatch.config.track_running_jobs = true
      KafkaBatch::Liveness.reset!
      KafkaBatchSpec::RedisHelper.flush!

      KafkaBatch::Liveness.heartbeat(topic: "test.success")
      KafkaBatch::Liveness.job_started(job_id: "live-1", batch_id: "b1", worker_class: "ProcessUserWorker", topic: "test.success", partition: 0)

      html = get("/live").last.join
      expect(html).to include("Active consumers")
      expect(html).to include("Running jobs")
      expect(html).to include("ProcessUserWorker")
      expect(html).to include("live-1"[0, 8])
      # dashboard links to it
      expect(get("/").last.join).to include("/kafka_batch/live")
    end

    it "shows an unavailable message when Redis liveness is down" do
      allow(KafkaBatch::Liveness).to receive(:available?).and_return(false)
      html = get("/live").last.join
      expect(html).to include("requires Redis")
    end
  end

  describe "GET /batches/:id" do
    it "renders the batch detail" do
      id = seed(total: 5)
      status, _h, body = get("/batches/#{id}")
      expect(status).to eq(200)
      expect(body.join).to include(id[0, 8])
    end

    it "404s for an unknown batch" do
      status, = get("/batches/does-not-exist")
      expect(status).to eq(404)
    end

    it "shows recorded failures for the batch" do
      id = seed(total: 2)
      KafkaBatch.store.record_failure(
        batch_id: id, job_id: "jx", worker_class: "ProcessUserWorker",
        error_class: "RuntimeError", error_message: "kaboom"
      )

      html = get("/batches/#{id}").last.join
      expect(html).to include("Job failures")
      expect(html).to include("RuntimeError")
      expect(html).to include("kaboom")
    end
  end

  describe "POST /batches/:id/cancel" do
    it "cancels the batch and redirects" do
      id = seed
      status, headers, = post("/batches/#{id}/cancel")

      expect(status).to eq(302)
      expect(headers["location"]).to eq("/kafka_batch/")
      expect(KafkaBatch.store.find_batch(id)[:status]).to eq("cancelled")
    end
  end

  describe "POST /batches/:id/delete" do
    it "deletes the batch and redirects" do
      id = seed
      status, _h, = post("/batches/#{id}/delete")

      expect(status).to eq(302)
      expect(KafkaBatch.store.find_batch(id)).to be_nil
    end
  end

  it "404s unknown routes" do
    status, = get("/nope")
    expect(status).to eq(404)
  end

  describe "time formatting" do
    let(:web) { described_class.new }

    it "renders times as UTC in 24-hour format with an explicit suffix" do
      expect(web.send(:fmt_time, Time.utc(2026, 6, 27, 20, 19, 44))).to eq("2026-06-27 20:19:44 UTC")
      expect(web.send(:fmt_time, Time.new(2026, 6, 27, 22, 19, 44, "+02:00"))).to eq("2026-06-27 20:19:44 UTC")
      expect(web.send(:fmt_time, "2026-06-27T20:19:44Z")).to eq("2026-06-27 20:19:44 UTC")
      expect(web.send(:fmt_time, nil)).to eq("—")
      expect(web.send(:fmt_time, "")).to eq("—")
    end

    it "renders UTC timestamps on the batch detail page" do
      id = seed(total: 1)
      html = get("/batches/#{id}").last.join
      expect(html).to match(/\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2} UTC/)
    end

    it "formats next-retry ETA as a human duration" do
      expect(web.send(:fmt_eta, Time.now + 90)).to match(/\Ain 1m \d+s\z/)
      expect(web.send(:fmt_eta, Time.now + 3 * 3600 + 120)).to match(/\Ain 3h \d+m\z/)
      expect(web.send(:fmt_eta, Time.now - 5)).to eq("due now")
      expect(web.send(:fmt_eta, nil)).to eq("—")
    end
  end

  it "shows the next-retry ETA for a retrying failure" do
    id = seed(total: 1)
    KafkaBatch.store.record_failure(
      batch_id: id, job_id: "jr", worker_class: "W", error_class: "Flaky",
      error_message: "x", attempt: 0, status: "retrying", next_retry_at: Time.now + 7200
    )
    html = get("/batches/#{id}").last.join
    expect(html).to include("Next retry")
    expect(html).to match(/in \d+h/)
  end
end

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
end

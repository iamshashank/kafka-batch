require "erb"
require "cgi"
require "time"

module KafkaBatch
  # A minimal, dependency-free Rack application for inspecting batches –
  # think "Sidekiq Web" but tiny. Mount it in your routes:
  #
  #   # config/routes.rb
  #   mount KafkaBatch::Web => "/kafka_batch"
  #
  # It works with whichever store is configured (MySQL or Redis). Because it
  # exposes destructive actions (cancel / delete), mount it behind your own
  # authentication (e.g. `authenticate :admin do ... end` or HTTP basic auth).
  class Web
    PER_PAGE = 25

    STATUS_COLORS = {
      "running"   => "#3b82f6",
      "success"   => "#10b981",
      "complete"  => "#f59e0b",
      "cancelled" => "#6b7280",
      "pending"   => "#8b5cf6"
    }.freeze

    def self.call(env)
      new.call(env)
    end

    def call(env)
      @script_name = env["SCRIPT_NAME"].to_s
      method       = env["REQUEST_METHOD"]
      path         = env["PATH_INFO"].to_s
      path         = "/" if path.empty?
      params       = parse_query(env["QUERY_STRING"])

      if method == "GET" && path == "/"
        html(render_index(params))
      elsif method == "GET" && path == "/failures"
        html(render_failures(params))
      elsif method == "GET" && path == "/live"
        html(render_live)
      elsif method == "GET" && (m = path.match(%r{\A/batches/([^/]+)\z}))
        batch = KafkaBatch.store.find_batch(m[1])
        batch ? html(render_show(batch)) : not_found
      elsif method == "POST" && (m = path.match(%r{\A/batches/([^/]+)/cancel\z}))
        KafkaBatch::Batch.cancel(m[1])
        redirect_to_index
      elsif method == "POST" && (m = path.match(%r{\A/batches/([^/]+)/delete\z}))
        KafkaBatch.store.delete_batch(m[1])
        redirect_to_index
      else
        not_found
      end
    rescue StandardError => e
      KafkaBatch.logger.error("[KafkaBatch::Web] #{e.class}: #{e.message}")
      [500, html_headers,
       [layout("Error", "<div class='card'><h2>500</h2><pre>#{h(e.message)}</pre></div>")]]
    end

    private

    # ── Responses ──────────────────────────────────────────────────────────

    # Dashboard data is always live; never let a browser/proxy cache it (also
    # prevents Rails' Rack::ETag from issuing 304s that mask counter updates).
    def html_headers
      { "content-type" => "text/html; charset=utf-8", "cache-control" => "no-store" }
    end

    # Wrap an HTML body string in the layout; pass through ready-made responses.
    def html(body_or_response)
      return body_or_response if body_or_response.is_a?(Array)
      [200, html_headers, [layout("Batches", body_or_response)]]
    end

    def not_found
      [404, html_headers, [layout("Not found", "<div class='card'><h2>404</h2><p>Not found.</p></div>")]]
    end

    def redirect_to_index
      [302, { "location" => index_path, "cache-control" => "no-store", "content-type" => "text/html" }, []]
    end

    # ── Pages ──────────────────────────────────────────────────────────────

    def render_index(params)
      status   = non_empty(params["status"])
      page     = [params["page"].to_i, 1].max
      offset   = (page - 1) * PER_PAGE
      counts   = safe_counts
      # Fetch one extra row to detect whether a next page exists.
      batches  = KafkaBatch.store.list_batches(status: status, limit: PER_PAGE + 1, offset: offset)
      has_next = batches.size > PER_PAGE
      batches  = batches.first(PER_PAGE)

      summary = summary_cards(counts)
      filters = status_filters(status, counts)
      rows    = batches.map { |b| batch_row(b) }.join
      rows    = "<tr><td colspan='8' class='empty'>No batches found.</td></tr>" if batches.empty?
      pager   = pagination(page, has_next, status)

      <<~HTML
        #{summary}
        <div class="toolbar"><a class="btn" href="#{failures_path}">⚠ View all failures</a> <a class="btn" href="#{live_path}">▶ Live activity</a></div>
        #{filters}
        <div class="card">
          <table>
            <thead>
              <tr>
                <th>Batch</th><th>Status</th><th>Total</th><th>Done</th>
                <th>Failed</th><th>Pending</th><th>Progress</th><th>Actions</th>
              </tr>
            </thead>
            <tbody>#{rows}</tbody>
          </table>
        </div>
        #{pager}
      HTML
    end

    def render_failures(params)
      status   = non_empty(params["status"])
      page     = [params["page"].to_i, 1].max
      offset   = (page - 1) * PER_PAGE
      failures = KafkaBatch.store.list_all_failures(limit: PER_PAGE + 1, offset: offset, status: status)
      has_next = failures.size > PER_PAGE
      failures = failures.first(PER_PAGE)

      filter_links = [["All", nil], ["Retrying", "retrying"], ["Failed", "failed"]].map do |label, s|
        cls  = (status == s || (s.nil? && status.nil?)) ? "chip active" : "chip"
        href = s ? "#{failures_path}?status=#{s}" : failures_path
        "<a class='#{cls}' href='#{href}'>#{label}</a>"
      end.join

      rows = failures.map do |f|
        color = f[:status] == "retrying" ? "#f59e0b" : "#ef4444"
        <<~ROW.gsub(/\n\s*/, "")
          <tr>
            <td><a class="mono" href="#{show_path(f[:batch_id])}">#{h(short_id(f[:batch_id]))}</a></td>
            <td class="mono">#{h(short_id(f[:job_id]))}</td>
            <td>#{h(f[:worker_class])}</td>
            <td><span class="badge" style="background:#{color}">#{h(f[:status])}</span></td>
            <td>#{f[:attempt].to_i + 1}</td>
            <td>#{next_retry_cell(f)}</td>
            <td class="danger">#{h(f[:error_class])}</td>
            <td>#{h(f[:error_message])}</td>
            <td>#{fmt_time(f[:failed_at])}</td>
          </tr>
        ROW
      end.join
      rows = "<tr><td colspan='9' class='empty'>No failures recorded.</td></tr>" if failures.empty?

      qs        = status ? "&status=#{status}" : ""
      prev_link = page > 1 ? "<a class='btn' href='#{failures_path}?page=#{page - 1}#{qs}'>← Prev</a>" : ""
      next_link = has_next ? "<a class='btn' href='#{failures_path}?page=#{page + 1}#{qs}'>Next →</a>" : ""
      pager     = (prev_link.empty? && next_link.empty?) ? "" : "<div class='pager'>#{prev_link}<span class='page'>Page #{page}</span>#{next_link}</div>"

      <<~HTML
        <p><a class="back" href="#{index_path}">← All batches</a></p>
        <div class="chips">#{filter_links}</div>
        <div class="card">
          <h2>Failures across all batches</h2>
          <table>
            <thead><tr><th>Batch</th><th>Job</th><th>Worker</th><th>Status</th><th>Attempt</th><th>Next retry</th><th>Error</th><th>Message</th><th>Failed at</th></tr></thead>
            <tbody>#{rows}</tbody>
          </table>
        </div>
        #{pager}
      HTML
    end

    def render_live
      unless KafkaBatch::Liveness.available?
        msg = if KafkaBatch::Liveness.backend == :off
          "Live activity is disabled (<code>config.liveness_backend = :off</code>)."
        else
          "This feature requires Redis (<code>config.redis_url</code>) and it is not currently reachable, so running-job and consumer info is unavailable."
        end
        return <<~HTML
          <p><a class="back" href="#{index_path}">← All batches</a></p>
          <div class="card">
            <h2>Live activity</h2>
            <p class="muted">#{msg}</p>
          </div>
        HTML
      end

      consumers = KafkaBatch::Liveness.consumers
      jobs      = KafkaBatch::Liveness.running_jobs

      consumer_rows = consumers.map do |c|
        "<tr><td class='mono'>#{h(c['consumer_id'])}</td><td>#{h(c['hostname'])}</td>" \
        "<td>#{h(c['pid'])}</td><td>#{h(c['topic'])}</td><td>#{fmt_time(c['last_seen'])}</td></tr>"
      end.join
      consumer_rows = "<tr><td colspan='5' class='empty'>No active consumers seen.</td></tr>" if consumers.empty?

      job_rows = jobs.map do |j|
        batch = j["batch_id"] ? "<a class='mono' href='#{show_path(j['batch_id'])}'>#{h(short_id(j['batch_id']))}</a>" : "<span class='muted'>—</span>"
        "<tr><td class='mono'>#{h(short_id(j['job_id']))}</td><td>#{batch}</td>" \
        "<td>#{h(j['worker_class'])}</td><td class='mono'>#{h(j['consumer_id'])}</td>" \
        "<td>#{h(j['topic'])}/#{h(j['partition'])}</td><td>#{fmt_time(j['started_at'])}</td></tr>"
      end.join
      job_rows = "<tr><td colspan='6' class='empty'>No jobs currently running.</td></tr>" if jobs.empty?

      <<~HTML
        <p><a class="back" href="#{index_path}">← All batches</a></p>
        <div class="metrics">
          <div class="metric"><div class="metric-value">#{consumers.size}</div><div class="metric-label">Consumers</div></div>
          <div class="metric"><div class="metric-value">#{jobs.size}</div><div class="metric-label">Running jobs</div></div>
        </div>
        <div class="card">
          <h3>Active consumers</h3>
          <table>
            <thead><tr><th>Consumer</th><th>Host</th><th>PID</th><th>Topic</th><th>Last seen</th></tr></thead>
            <tbody>#{consumer_rows}</tbody>
          </table>
        </div>
        <div class="card">
          <h3>Running jobs</h3>
          <p class="muted">Backend: <code>#{h(KafkaBatch::Liveness.backend)}</code>. Approximate snapshot#{KafkaBatch::Liveness.backend == :store ? ' (sampled per consumer at heartbeat)' : ''} — short-lived jobs may not always appear. Auto-refreshing every 5s.</p>
          <table>
            <thead><tr><th>Job</th><th>Batch</th><th>Worker</th><th>Consumer</th><th>Topic/Part</th><th>Started</th></tr></thead>
            <tbody>#{job_rows}</tbody>
          </table>
        </div>
        <script>setTimeout(function(){ location.reload(); }, 5000);</script>
      HTML
    end

    def render_show(b)
      pend = pending(b)
      meta = b[:meta].nil? || b[:meta].empty? ? "—" : "<pre>#{h(b[:meta].inspect)}</pre>"

      rows = {
        "ID"             => h(b[:id]),
        "Status"         => status_badge(b[:status]),
        "Total jobs"     => b[:total_jobs],
        "Completed"      => b[:completed_count],
        "Failed"         => b[:failed_count],
        "Pending"        => pend,
        "on_success"     => h(b[:on_success] || "—"),
        "on_complete"    => h(b[:on_complete] || "—"),
        "Created at"     => fmt_time(b[:created_at]),
        "Finished at"    => fmt_time(b[:finished_at]),
        "Callback fired" => (b[:callback_dispatched_at].to_s.empty? ? "no" : fmt_time(b[:callback_dispatched_at])),
        "Meta"           => meta
      }.map { |k, v| "<tr><th>#{k}</th><td>#{v}</td></tr>" }.join

      <<~HTML
        <p><a class="back" href="#{index_path}">← All batches</a></p>
        <div class="card">
          <h2>Batch #{h(short_id(b[:id]))}</h2>
          #{progress_bar(b)}
          <table class="detail"><tbody>#{rows}</tbody></table>
          <div class="actions">#{actions_for(b)}</div>
        </div>
        #{failures_section(b)}
      HTML
    end

    def failures_section(b)
      failures = KafkaBatch.store.list_failures(b[:id], limit: 100)
      return "" if failures.empty?

      rows = failures.map do |f|
        color = f[:status] == "retrying" ? "#f59e0b" : "#ef4444"
        <<~ROW.gsub(/\n\s*/, "")
          <tr>
            <td class="mono">#{h(short_id(f[:job_id]))}</td>
            <td>#{h(f[:worker_class])}</td>
            <td><span class="badge" style="background:#{color}">#{h(f[:status])}</span></td>
            <td>#{f[:attempt].to_i + 1}</td>
            <td>#{next_retry_cell(f)}</td>
            <td class="danger">#{h(f[:error_class])}</td>
            <td>#{h(f[:error_message])}</td>
            <td>#{fmt_time(f[:failed_at])}</td>
          </tr>
        ROW
      end.join

      more = failures.size >= 100 ? "<p class='muted'>Showing the first 100 failing jobs.</p>" : ""

      <<~HTML
        <div class="card">
          <h3>Job failures (#{failures.size})</h3>
          <p class="muted">Recorded on the first failed attempt — <span class="badge" style="background:#f59e0b">retrying</span> while retries remain, <span class="badge" style="background:#ef4444">failed</span> once exhausted.</p>
          <table>
            <thead><tr><th>Job</th><th>Worker</th><th>Status</th><th>Attempt</th><th>Next retry</th><th>Error</th><th>Message</th><th>Failed at</th></tr></thead>
            <tbody>#{rows}</tbody>
          </table>
          #{more}
        </div>
      HTML
    rescue StandardError => e
      KafkaBatch.logger.warn("[KafkaBatch::Web] list_failures failed: #{e.message}")
      ""
    end

    # ── Partials ───────────────────────────────────────────────────────────

    def summary_cards(counts)
      total = counts.values.sum
      cards = [["Total", total, "#111827"]]
      %w[running success complete cancelled].each do |s|
        cards << [s.capitalize, counts[s].to_i, STATUS_COLORS[s]]
      end
      inner = cards.map do |label, value, color|
        "<div class='metric'><div class='metric-value' style='color:#{color}'>#{value}</div>" \
        "<div class='metric-label'>#{label}</div></div>"
      end.join
      "<div class='metrics'>#{inner}</div>"
    end

    def status_filters(active, counts)
      links = [["All", nil]] + %w[running success complete cancelled].map { |s| [s.capitalize, s] }
      items = links.map do |label, s|
        cls  = (active == s || (s.nil? && active.nil?)) ? "chip active" : "chip"
        href = s ? "#{index_path}?status=#{s}" : index_path
        n    = s ? " (#{counts[s].to_i})" : ""
        "<a class='#{cls}' href='#{href}'>#{label}#{n}</a>"
      end.join
      "<div class='chips'>#{items}</div>"
    end

    def batch_row(b)
      pend = pending(b)
      <<~HTML
        <tr>
          <td><a href="#{show_path(b[:id])}" class="mono">#{h(short_id(b[:id]))}</a></td>
          <td>#{status_badge(b[:status])}</td>
          <td>#{b[:total_jobs]}</td>
          <td>#{b[:completed_count]}</td>
          <td class="#{b[:failed_count].to_i.positive? ? 'danger' : ''}">#{b[:failed_count]}</td>
          <td>#{pend}</td>
          <td style="min-width:120px">#{progress_bar(b)}</td>
          <td class="actions">#{actions_for(b)}</td>
        </tr>
      HTML
    end

    def actions_for(b)
      buttons = []
      if b[:status] == "running"
        buttons << form_button(cancel_path(b[:id]), "Cancel", "warn",
                               "Cancel this batch? Remaining jobs will not run.")
      end
      buttons << form_button(delete_path(b[:id]), "Delete", "danger-btn",
                             "Delete this batch record permanently?")
      buttons.join(" ")
    end

    def form_button(action, label, css, confirm)
      "<form method='post' action='#{action}' onsubmit=\"return confirm('#{h(confirm)}')\" style='display:inline'>" \
      "<button type='submit' class='btn #{css}'>#{label}</button></form>"
    end

    def progress_bar(b)
      total = b[:total_jobs].to_i
      done  = b[:completed_count].to_i
      fail  = b[:failed_count].to_i
      return "<span class='muted'>—</span>" if total.zero?

      dpct = (done * 100.0 / total).round(1)
      fpct = (fail * 100.0 / total).round(1)
      <<~HTML.gsub(/\n\s*/, "")
        <div class="bar" title="#{done}/#{total} done, #{fail} failed">
          <div class="bar-done" style="width:#{dpct}%"></div>
          <div class="bar-fail" style="width:#{fpct}%"></div>
        </div>
      HTML
    end

    def status_badge(status)
      color = STATUS_COLORS[status] || "#6b7280"
      "<span class='badge' style='background:#{color}'>#{h(status)}</span>"
    end

    def pagination(page, has_next, status)
      qs = status ? "&status=#{status}" : ""
      prev_link = page > 1 ? "<a class='btn' href='#{index_path}?page=#{page - 1}#{qs}'>← Prev</a>" : ""
      next_link = has_next ? "<a class='btn' href='#{index_path}?page=#{page + 1}#{qs}'>Next →</a>" : ""
      return "" if prev_link.empty? && next_link.empty?
      "<div class='pager'>#{prev_link}<span class='page'>Page #{page}</span>#{next_link}</div>"
    end

    # ── Helpers ────────────────────────────────────────────────────────────

    def pending(b)
      [b[:total_jobs].to_i - b[:completed_count].to_i - b[:failed_count].to_i, 0].max
    end

    def safe_counts
      KafkaBatch.store.batch_counts || {}
    rescue StandardError => e
      KafkaBatch.logger.warn("[KafkaBatch::Web] batch_counts failed: #{e.message}")
      {}
    end

    def short_id(id)
      id.to_s[0, 8]
    end

    def index_path
      @script_name.empty? ? "/" : "#{@script_name}/"
    end

    def failures_path
      "#{@script_name}/failures"
    end

    def live_path
      "#{@script_name}/live"
    end

    def show_path(id)
      "#{@script_name}/batches/#{CGI.escape(id.to_s)}"
    end

    def cancel_path(id)
      "#{show_path(id)}/cancel"
    end

    def delete_path(id)
      "#{show_path(id)}/delete"
    end

    def non_empty(v)
      v.nil? || v.empty? ? nil : v
    end

    def parse_query(qs)
      CGI.parse(qs.to_s).transform_values(&:first)
    end

    def h(text)
      CGI.escapeHTML(text.to_s)
    end

    # Human "time until" a future timestamp, e.g. "in 23h 59m" / "in 5m 3s".
    def fmt_eta(value)
      return "—" if value.nil? || (value.respond_to?(:empty?) && value.empty?)
      t = value.respond_to?(:to_time) ? value.to_time : Time.parse(value.to_s)
      secs = (t - Time.now).round
      return "due now" if secs <= 0

      d = secs / 86_400; secs %= 86_400
      hh = secs / 3_600; secs %= 3_600
      mm = secs / 60;    ss = secs % 60
      parts = []
      parts << "#{d}d"  if d.positive?
      parts << "#{hh}h" if hh.positive?
      parts << "#{mm}m" if mm.positive?
      parts << "#{ss}s" if ss.positive?
      "in #{parts.first(2).join(' ')}"
    rescue StandardError
      "—"
    end

    # Table cell for a failure's next retry (ETA + absolute UTC), or "—".
    def next_retry_cell(failure)
      return "—" unless failure[:status] == "retrying" && failure[:next_retry_at]
      "#{fmt_eta(failure[:next_retry_at])}<br><span class='muted'>#{fmt_time(failure[:next_retry_at])}</span>"
    end

    # Render any timestamp (Time, ActiveRecord time, or ISO8601 string) as
    # UTC in 24-hour format with an explicit suffix: "2026-06-27 20:19:44 UTC".
    def fmt_time(value)
      return "—" if value.nil? || (value.respond_to?(:empty?) && value.empty?)

      t =
        if value.respond_to?(:to_time)
          value.to_time
        else
          Time.parse(value.to_s)
        end
      t.utc.strftime("%Y-%m-%d %H:%M:%S UTC")
    rescue StandardError
      h(value.to_s)
    end

    def layout(title, body)
      <<~HTML
        <!DOCTYPE html>
        <html lang="en">
        <head>
          <meta charset="utf-8">
          <meta name="viewport" content="width=device-width, initial-scale=1">
          <title>KafkaBatch — #{h(title)}</title>
          <style>#{CSS}</style>
        </head>
        <body>
          <header><a href="#{index_path}" class="logo">KafkaBatch</a><span class="tag">batches</span></header>
          <main>#{body}</main>
        </body>
        </html>
      HTML
    end

    CSS = <<~CSS
      * { box-sizing: border-box; }
      body { margin: 0; font-family: -apple-system, BlinkMacSystemFont, "Segoe UI", Roboto, Helvetica, Arial, sans-serif;
             background: #f3f4f6; color: #111827; }
      header { background: #111827; color: #fff; padding: 14px 24px; display: flex; align-items: baseline; gap: 10px; }
      header .logo { color: #fff; text-decoration: none; font-weight: 700; font-size: 18px; }
      header .tag { color: #9ca3af; font-size: 13px; }
      main { max-width: 1100px; margin: 24px auto; padding: 0 16px; }
      .card { background: #fff; border: 1px solid #e5e7eb; border-radius: 10px; padding: 16px; margin-bottom: 16px; }
      table { width: 100%; border-collapse: collapse; font-size: 14px; }
      th, td { text-align: left; padding: 10px 8px; border-bottom: 1px solid #f0f0f0; }
      thead th { color: #6b7280; font-size: 12px; text-transform: uppercase; letter-spacing: .04em; }
      td.empty { text-align: center; color: #9ca3af; padding: 28px; }
      .mono, .detail th { font-family: ui-monospace, SFMono-Regular, Menlo, monospace; }
      .metrics { display: flex; gap: 12px; flex-wrap: wrap; margin-bottom: 16px; }
      .metric { background: #fff; border: 1px solid #e5e7eb; border-radius: 10px; padding: 14px 18px; min-width: 110px; }
      .metric-value { font-size: 26px; font-weight: 700; }
      .metric-label { color: #6b7280; font-size: 12px; text-transform: uppercase; letter-spacing: .04em; }
      .toolbar { margin-bottom: 12px; }
      .chips { margin-bottom: 12px; display: flex; gap: 8px; flex-wrap: wrap; }
      .chip { text-decoration: none; color: #374151; background: #fff; border: 1px solid #e5e7eb;
              padding: 5px 12px; border-radius: 999px; font-size: 13px; }
      .chip.active { background: #111827; color: #fff; border-color: #111827; }
      .badge { color: #fff; padding: 3px 9px; border-radius: 999px; font-size: 12px; text-transform: capitalize; }
      .bar { background: #eef0f3; border-radius: 999px; height: 8px; overflow: hidden; display: flex; }
      .bar-done { background: #10b981; height: 100%; }
      .bar-fail { background: #ef4444; height: 100%; }
      .btn { display: inline-block; text-decoration: none; border: 1px solid #d1d5db; background: #fff;
             color: #374151; padding: 5px 12px; border-radius: 7px; font-size: 13px; cursor: pointer; }
      .btn.warn { border-color: #f59e0b; color: #b45309; }
      .btn.danger-btn { border-color: #ef4444; color: #b91c1c; }
      .actions { white-space: nowrap; }
      td.danger { color: #b91c1c; font-weight: 600; }
      .pager { display: flex; gap: 12px; align-items: center; justify-content: center; margin: 8px 0 24px; }
      .page { color: #6b7280; font-size: 13px; }
      .back { color: #2563eb; text-decoration: none; }
      .muted { color: #9ca3af; }
      .detail th { width: 180px; color: #6b7280; vertical-align: top; }
      pre { white-space: pre-wrap; word-break: break-word; margin: 0; font-size: 12px; }
      h2 { margin-top: 0; }
    CSS
  end
end

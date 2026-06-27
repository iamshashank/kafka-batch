require "erb"
require "cgi"

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
      [500, { "content-type" => "text/html; charset=utf-8" },
       [layout("Error", "<div class='card'><h2>500</h2><pre>#{h(e.message)}</pre></div>")]]
    end

    private

    # ── Responses ──────────────────────────────────────────────────────────

    # Wrap an HTML body string in the layout; pass through ready-made responses.
    def html(body_or_response)
      return body_or_response if body_or_response.is_a?(Array)
      [200, { "content-type" => "text/html; charset=utf-8" }, [layout("Batches", body_or_response)]]
    end

    def not_found
      [404, { "content-type" => "text/html; charset=utf-8" },
       [layout("Not found", "<div class='card'><h2>404</h2><p>Not found.</p></div>")]]
    end

    def redirect_to_index
      [302, { "location" => index_path, "content-type" => "text/html" }, []]
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
        "Created at"     => h(b[:created_at].to_s),
        "Finished at"    => h(b[:finished_at].to_s.empty? ? "—" : b[:finished_at].to_s),
        "Callback fired" => h(b[:callback_dispatched_at].to_s.empty? ? "no" : b[:callback_dispatched_at].to_s),
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
      HTML
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

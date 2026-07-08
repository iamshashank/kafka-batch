package fairness

import (
	"context"
	"encoding/json"
)

// Dispatcher consumes fair ingest messages and enqueues into the WFQ scheduler.
type Dispatcher struct {
	Lane      Lane
	Scheduler *Scheduler
	Window    int
}

// Outcome describes one ingest message.
type Outcome struct {
	CommitOffset bool
	Enqueued     bool
	Backpressure bool
	TenantID     string
}

func (d *Dispatcher) Process(ctx context.Context, raw []byte) (Outcome, error) {
	out := Outcome{CommitOffset: true}
	var m map[string]interface{}
	if err := json.Unmarshal(raw, &m); err != nil {
		return out, nil
	}
	tenantID, _ := m["tenant_id"].(string)
	if tenantID == "" {
		tenantID = "default"
	}
	out.TenantID = tenantID

	if d.Scheduler == nil {
		d.Scheduler = &Scheduler{Lane: d.Lane, Window: d.Window}
	}
	ok, err := d.Scheduler.Enqueue(ctx, tenantID, raw)
	if err != nil {
		return out, err
	}
	if !ok {
		out.Backpressure = true
		out.CommitOffset = false
		return out, nil
	}
	out.Enqueued = true
	return out, nil
}

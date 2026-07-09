package instrument

import (
	"log"
	"sync"
)

// Handler receives lifecycle events from the Go control plane and worker.
// Event names mirror Ruby ActiveSupport::Notifications (without .kafka_batch suffix):
//   job.processed, job.retried, job.failed, job.cancelled, job.expired,
//   batch.completed, callback.invoked, callback.failed, dlt.published,
//   scheduled.dispatched, consumer.priority_yielded
var Handler func(event string, payload map[string]interface{}, durationMs float64)

var mu sync.RWMutex

// SetHandler installs a process-wide instrumentation callback (tests, metrics bridge).
func SetHandler(fn func(string, map[string]interface{}, float64)) {
	mu.Lock()
	defer mu.Unlock()
	Handler = fn
}

// Emit publishes one instrumentation event. Always logs at debug; optional Handler receives a copy.
func Emit(event string, payload map[string]interface{}, durationMs float64) {
	if payload == nil {
		payload = map[string]interface{}{}
	}
	log.Printf("[kbatch-instrument] event=%s duration_ms=%.2f payload=%v", event, durationMs, payload)
	mu.RLock()
	fn := Handler
	mu.RUnlock()
	if fn != nil {
		fn(event, payload, durationMs)
	}
}

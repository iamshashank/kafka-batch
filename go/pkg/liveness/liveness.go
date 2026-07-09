package liveness

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"sync"
	"time"

	"github.com/google/uuid"
	"github.com/redis/go-redis/v9"
)

const consumerPrefix = "kafka_batch:live:consumer:"

// Reporter writes Redis consumer heartbeats for the Ruby /live dashboard.
type Reporter struct {
	Client     *redis.Client
	TTL        time.Duration
	ConsumerID string

	mu sync.Mutex
}

func NewReporter(client *redis.Client, ttl time.Duration) *Reporter {
	if ttl <= 0 {
		ttl = 30 * time.Second
	}
	host, _ := os.Hostname()
	return &Reporter{
		Client:     client,
		TTL:        ttl,
		ConsumerID: fmt.Sprintf("%s:%d:%s", host, os.Getpid(), uuid.NewString()[:6]),
	}
}

func (r *Reporter) Heartbeat(ctx context.Context, topic string) {
	if r == nil || r.Client == nil {
		return
	}
	payload, _ := json.Marshal(map[string]interface{}{
		"consumer_id": r.ConsumerID,
		"hostname":    hostname(),
		"pid":         os.Getpid(),
		"topic":       topic,
		"last_seen":   time.Now().UTC().Format(time.RFC3339),
		"runtime":     "go",
	})
	_ = r.Client.Set(ctx, consumerPrefix+r.ConsumerID, payload, r.TTL).Err()
}

func hostname() string {
	h, _ := os.Hostname()
	return h
}

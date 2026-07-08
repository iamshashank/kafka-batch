package fairness

import (
	"context"
	"fmt"

	"github.com/redis/go-redis/v9"
)

// Scheduler is the Redis WFQ scheduler for one fairness lane (Phase 3c slice).
type Scheduler struct {
	Lane   Lane
	Client *redis.Client
	Window int
}

// Enqueue appends a job payload to a tenant's bounded ready window.
// Returns false when the window is full (caller should backpressure).
func (s *Scheduler) Enqueue(ctx context.Context, tenantID string, payload []byte) (bool, error) {
	if err := ValidateLane(s.Lane); err != nil {
		return false, err
	}
	window := s.Window
	if window <= 0 {
		window = 100
	}
	res, err := s.Client.Eval(ctx, EnqueueLua,
		[]string{ringKey(s.Lane), vtimeKey(s.Lane)},
		tenantID, string(payload), window, readyPrefix(s.Lane),
	).Int()
	if err != nil {
		return false, err
	}
	return res == 1, nil
}

// ReadyDepth returns the number of jobs buffered for a tenant.
func (s *Scheduler) ReadyDepth(ctx context.Context, tenantID string) (int64, error) {
	return s.Client.LLen(ctx, ReadyKey(s.Lane, tenantID)).Result()
}

// RingSize returns tenants currently in the WFQ ring.
func (s *Scheduler) RingSize(ctx context.Context) (int64, error) {
	n, err := s.Client.ZCard(ctx, ringKey(s.Lane)).Result()
	if err != nil {
		return 0, fmt.Errorf("ring size: %w", err)
	}
	return n, nil
}

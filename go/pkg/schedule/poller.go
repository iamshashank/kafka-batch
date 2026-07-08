package schedule

import (
	"context"
	"encoding/json"
	"log"
	"time"

	"github.com/y-shashank/kafka-batch/go/pkg/config"
)

// Producer dispatches due jobs back to execution topics.
type Producer interface {
	Produce(ctx context.Context, topic, key string, payload []byte) error
}

// Router resolves a scheduled payload to a destination topic/key.
type Router interface {
	Route(payload map[string]interface{}) (topic, key string, err error)
}

// BatchCancelled reports whether a batch should be dropped.
type BatchCancelled func(ctx context.Context, batchID string) (bool, error)

// Poller drains the delayed-job index (Ruby SchedulePoller).
type Poller struct {
	Cfg       config.Daemon
	Store     *RedisStore
	Reader    *Reader
	Producer  Producer
	Router    Router
	Cancelled BatchCancelled
	Now       func() time.Time

	lastReclaim time.Time
}

const maxReadMisses = 10

func (p *Poller) Tick(ctx context.Context) (int, error) {
	now := p.now()
	if p.Cfg.ScheduleReclaimEvery > 0 && now.Sub(p.lastReclaim) >= p.Cfg.ScheduleReclaimEvery {
		if _, err := p.Store.Reclaim(ctx, now); err != nil {
			return 0, err
		}
		p.lastReclaim = now
	}

	members, err := p.Store.ClaimDue(ctx, now, p.Cfg.ScheduleLeaseSeconds, p.Cfg.ScheduleBatchSize)
	if err != nil || len(members) == 0 {
		return 0, err
	}

	byPartition := map[int32][]int64{}
	parsed := make([]struct {
		member string
		Member
	}, 0, len(members))
	for _, m := range members {
		pm, ok := ParseMember(m)
		if !ok {
			continue
		}
		parsed = append(parsed, struct {
			member string
			Member
		}{m, pm})
		byPartition[pm.Partition] = append(byPartition[pm.Partition], pm.Offset)
	}

	read, err := p.Reader.Read(ctx, byPartition)
	if err != nil {
		return 0, err
	}

	acked := 0
	done := make([]string, 0, len(parsed))
	for _, item := range parsed {
		loc := BuildKey(item.Partition, item.Offset)
		raw, ok := read.Found[loc]
		if !ok {
			misses, _ := p.Store.RecordReadMiss(ctx, item.member)
			if misses >= maxReadMisses {
				_ = p.Store.ClearReadMiss(ctx, item.member)
				done = append(done, item.member)
			}
			continue
		}
		_ = p.Store.ClearReadMiss(ctx, item.member)

		if p.produceDue(ctx, raw, item.JobID) {
			acked++
			done = append(done, item.member)
		}
	}
	if len(done) > 0 {
		_ = p.Store.Ack(ctx, done)
	}
	return acked, nil
}

func (p *Poller) produceDue(ctx context.Context, raw []byte, jobID string) bool {
	var data map[string]interface{}
	if err := json.Unmarshal(raw, &data); err != nil {
		log.Printf("[kbatch-schedule] invalid payload job_id=%s: %v", jobID, err)
		return true
	}
	if batchID, _ := data["batch_id"].(string); batchID != "" && p.Cancelled != nil {
		cancelled, err := p.Cancelled(ctx, batchID)
		if err == nil && cancelled {
			return true
		}
	}
	topic, key, err := p.Router.Route(data)
	if err != nil {
		log.Printf("[kbatch-schedule] route job_id=%s: %v", jobID, err)
		return true
	}
	if key == "" {
		if k, ok := data["job_id"].(string); ok {
			key = k
		}
	}
	if err := p.Producer.Produce(ctx, topic, key, raw); err != nil {
		log.Printf("[kbatch-schedule] produce job_id=%s: %v", jobID, err)
		return false
	}
	return true
}

func (p *Poller) now() time.Time {
	if p.Now != nil {
		return p.Now()
	}
	return time.Now()
}

// Run blocks until ctx is cancelled.
func (p *Poller) Run(ctx context.Context) {
	interval := p.Cfg.SchedulePollInterval
	if interval <= 0 {
		interval = 5 * time.Second
	}
	wait := interval
	for {
		select {
		case <-ctx.Done():
			return
		default:
		}
		n, err := p.Tick(ctx)
		if err != nil {
			log.Printf("[kbatch-schedule] tick error: %v", err)
			time.Sleep(wait)
			wait = min(wait*2, 30*time.Second)
			continue
		}
		if n == 0 {
			time.Sleep(wait)
			wait = min(wait*2, 30*time.Second)
		} else {
			wait = interval
		}
	}
}

func min(a, b time.Duration) time.Duration {
	if a < b {
		return a
	}
	return b
}

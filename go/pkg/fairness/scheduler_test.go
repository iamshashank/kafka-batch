package fairness

import (
	"context"
	"encoding/json"
	"testing"

	"github.com/alicebob/miniredis/v2"
	"github.com/redis/go-redis/v9"
)

func TestSchedulerEnqueueAndWindow(t *testing.T) {
	mr := miniredis.RunT(t)
	rdb := redis.NewClient(&redis.Options{Addr: mr.Addr()})
	s := &Scheduler{Lane: LaneTime, Client: rdb, Window: 2}
	ctx := context.Background()

	ok, err := s.Enqueue(ctx, "t1", []byte(`{"job_id":"j1"}`))
	if err != nil || !ok {
		t.Fatalf("enqueue1 ok=%v err=%v", ok, err)
	}
	ok, err = s.Enqueue(ctx, "t1", []byte(`{"job_id":"j2"}`))
	if err != nil || !ok {
		t.Fatalf("enqueue2 ok=%v err=%v", ok, err)
	}
	ok, err = s.Enqueue(ctx, "t1", []byte(`{"job_id":"j3"}`))
	if err != nil || ok {
		t.Fatalf("enqueue3 should be full ok=%v err=%v", ok, err)
	}
	depth, err := s.ReadyDepth(ctx, "t1")
	if err != nil || depth != 2 {
		t.Fatalf("depth=%d err=%v", depth, err)
	}
	n, err := s.RingSize(ctx)
	if err != nil || n != 1 {
		t.Fatalf("ring=%d err=%v", n, err)
	}
}

func TestDispatcherEnqueueFromJobMessage(t *testing.T) {
	mr := miniredis.RunT(t)
	rdb := redis.NewClient(&redis.Options{Addr: mr.Addr()})
	sched := &Scheduler{Lane: LaneTime, Client: rdb, Window: 10}
	d := &Dispatcher{Lane: LaneTime, Scheduler: sched, Window: 10}

	raw, _ := json.Marshal(map[string]interface{}{
		"job_id": "j1", "tenant_id": "tenant-a", "payload": map[string]interface{}{"x": 1},
	})
	out, err := d.Process(context.Background(), raw)
	if err != nil {
		t.Fatal(err)
	}
	if !out.Enqueued || out.TenantID != "tenant-a" {
		t.Fatalf("out %+v", out)
	}
	depth, _ := sched.ReadyDepth(context.Background(), "tenant-a")
	if depth != 1 {
		t.Fatalf("depth %d", depth)
	}
}

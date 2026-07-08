package schedule

import (
	"context"
	"fmt"
	"strconv"
	"time"

	"github.com/twmb/franz-go/pkg/kgo"
)

// Reader fetches payloads from the scheduled topic by partition/offset.
type Reader struct {
	topic  string
	client *kgo.Client
}

func NewReader(brokers []string, topic string) (*Reader, error) {
	cl, err := kgo.NewClient(
		kgo.SeedBrokers(brokers...),
		kgo.ConsumerGroup("kbatch-schedule-reader"),
		kgo.ConsumeTopics(topic),
	)
	if err != nil {
		return nil, err
	}
	return &Reader{topic: topic, client: cl}, nil
}

type ReadResult struct {
	Found map[string][]byte
	Lost  []string
}

// Read loads payloads for partition→offsets map.
func (r *Reader) Read(ctx context.Context, byPartition map[int32][]int64) (ReadResult, error) {
	out := ReadResult{Found: map[string][]byte{}}
	if len(byPartition) == 0 {
		return out, nil
	}

	for partition, offsets := range byPartition {
		if len(offsets) == 0 {
			continue
		}
		want := make(map[int64]struct{}, len(offsets))
		minOff := offsets[0]
		for _, off := range offsets {
			want[off] = struct{}{}
			if off < minOff {
				minOff = off
			}
		}

		assign := map[string]map[int32]kgo.Offset{
			r.topic: {partition: kgo.NewOffset().At(minOff)},
		}
		r.client.AddConsumePartitions(assign)

		deadline := time.Now().Add(5 * time.Second)
		for time.Now().Before(deadline) && len(want) > 0 {
			fetches := r.client.PollFetches(ctx)
			if errs := fetches.Errors(); len(errs) > 0 {
				return out, fmt.Errorf("poll scheduled topic: %v", errs[0].Err)
			}
			fetches.EachRecord(func(rec *kgo.Record) {
				if rec.Topic != r.topic || rec.Partition != partition {
					return
				}
				if _, ok := want[rec.Offset]; ok {
					key := BuildKey(partition, rec.Offset)
					out.Found[key] = append([]byte(nil), rec.Value...)
					delete(want, rec.Offset)
				}
			})
		}
	}

	return out, nil
}

func (r *Reader) Close() {
	if r.client != nil {
		r.client.Close()
	}
}

// ParseOffsets converts string map keys from tests.
func ParseOffsets(m map[string][]int64) map[int32][]int64 {
	out := map[int32][]int64{}
	for k, offs := range m {
		p, _ := strconv.Atoi(k)
		out[int32(p)] = offs
	}
	return out
}

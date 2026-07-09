package daemon

import (
	"context"
	"encoding/json"
	"fmt"

	"github.com/twmb/franz-go/pkg/kgo"

	"github.com/y-shashank/kafka-batch/go/pkg/config"
	"github.com/y-shashank/kafka-batch/go/pkg/control/job"
	"github.com/y-shashank/kafka-batch/go/pkg/kafkaclient"
	"github.com/y-shashank/kafka-batch/go/pkg/protocol"
)

// BuildJobHandler returns the shared plain/fair-ready job consumer callback.
func BuildJobHandler(cfg config.Daemon, prod *kafkaclient.Client, jobProc *job.Processor) func(*kgo.Record) error {
	return func(rec *kgo.Record) error {
		src := protocol.SourceCoords{Topic: rec.Topic, Partition: rec.Partition, Offset: rec.Offset}
		out, err := jobProc.Process(context.Background(), rec.Value, src)
		if err != nil {
			return err
		}
		if out.Event != nil {
			raw, _ := json.Marshal(out.Event)
			key := fmt.Sprintf("%s/%d", out.Event.SrcTopic, out.Event.SrcPartition)
			if err := prod.Produce(context.Background(), cfg.EventsTopic, key, raw); err != nil {
				return err
			}
		}
		if out.RetryPayload != nil {
			if err := prod.Produce(context.Background(), out.RetryTopic, out.RetryKey, out.RetryPayload); err != nil {
				return err
			}
		}
		if out.DLTPayload != nil {
			if err := prod.Produce(context.Background(), cfg.DeadLetterTopic, out.DLTKey, out.DLTPayload); err != nil {
				return err
			}
		}
		if !out.CommitOffset {
			return fmt.Errorf("job not committed")
		}
		return nil
	}
}

package job

import (
	"encoding/json"

	"github.com/y-shashank/kafka-batch/go/pkg/instrument"
	"github.com/y-shashank/kafka-batch/go/pkg/protocol"
)

func emitJobProcessed(job protocol.JobMessage, durationMs float64) {
	instrument.Emit("job.processed", instrument.JobPayload(job.JobID, deref(job.BatchID), job.WorkerClass, nil), durationMs)
}

func emitJobCancelled(job protocol.JobMessage) {
	instrument.Emit("job.cancelled", instrument.JobPayload(job.JobID, deref(job.BatchID), job.WorkerClass, nil), 0)
}

func emitJobExpired(job protocol.JobMessage, validTill string) {
	instrument.Emit("job.expired", instrument.JobPayload(job.JobID, deref(job.BatchID), job.WorkerClass, map[string]interface{}{
		"valid_till": validTill,
	}), 0)
}

func emitJobRetried(job protocol.JobMessage, nextAttempt int, retryTopic string) {
	instrument.Emit("job.retried", instrument.JobPayload(job.JobID, deref(job.BatchID), job.WorkerClass, map[string]interface{}{
		"attempt":      job.Attempt,
		"next_attempt": nextAttempt,
		"retry_topic":  retryTopic,
	}), 0)
}

func emitJobFailed(job protocol.JobMessage, attempt int, errClass, errMsg string) {
	instrument.Emit("job.failed", instrument.JobPayload(job.JobID, deref(job.BatchID), job.WorkerClass, map[string]interface{}{
		"attempt":       attempt,
		"error_class":   errClass,
		"error_message": errMsg,
	}), 0)
}

func emitDLTPublished(jobID, batchID, dltType, sourceTopic string) {
	instrument.Emit("dlt.published", map[string]interface{}{
		"job_id":       jobID,
		"batch_id":     batchID,
		"dlt_type":     dltType,
		"source_topic": sourceTopic,
	}, 0)
}

func dltMeta(raw []byte) (jobID, batchID, dltType string) {
	var m map[string]interface{}
	_ = json.Unmarshal(raw, &m)
	if s, ok := m["job_id"].(string); ok {
		jobID = s
	}
	if s, ok := m["batch_id"].(string); ok {
		batchID = s
	}
	if s, ok := m["dlt_type"].(string); ok {
		dltType = s
	}
	return jobID, batchID, dltType
}

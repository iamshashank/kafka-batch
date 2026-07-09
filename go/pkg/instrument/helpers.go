package instrument

import "time"

func Since(start time.Time) float64 {
	if start.IsZero() {
		return 0
	}
	return float64(time.Since(start).Milliseconds())
}

func JobPayload(jobID, batchID, workerClass string, extra map[string]interface{}) map[string]interface{} {
	out := map[string]interface{}{
		"job_id":        jobID,
		"batch_id":      batchID,
		"worker_class":  workerClass,
	}
	for k, v := range extra {
		out[k] = v
	}
	return out
}

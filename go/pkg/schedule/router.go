package schedule

import (
	"fmt"

	"github.com/y-shashank/kafka-batch/go/pkg/config"
)

// ManifestRouter routes scheduled jobs using handler manifest topics.
type ManifestRouter struct {
	Manifest config.Manifest
	Default  string
}

func (r ManifestRouter) Route(payload map[string]interface{}) (topic, key string, err error) {
	jobType, _ := payload["job_type"].(string)
	if jobType != "" {
		if h, ok := r.Manifest.Handlers[jobType]; ok && h.Topic != "" {
			return h.Topic, str(payload["job_id"]), nil
		}
	}
	worker, _ := payload["worker_class"].(string)
	if worker != "" {
		for jt, h := range r.Manifest.Handlers {
			if h.Runtime == "go" && ("go:"+jt) == worker && h.Topic != "" {
				return h.Topic, str(payload["job_id"]), nil
			}
		}
	}
	if r.Default != "" {
		return r.Default, str(payload["job_id"]), nil
	}
	return "", "", fmt.Errorf("no route for job_type=%q worker_class=%q", jobType, worker)
}

func str(v interface{}) string {
	s, _ := v.(string)
	return s
}

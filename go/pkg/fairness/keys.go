package fairness

import "fmt"

func prefix(lane Lane) string {
	return "kafka_batch:fair_" + lane.String()
}

func ringKey(lane Lane) string   { return prefix(lane) + ":ring" }
func vtimeKey(lane Lane) string  { return prefix(lane) + ":vtime" }
func readyPrefix(lane Lane) string { return prefix(lane) + ":ready:" }

// ReadyKey returns the per-tenant ready list key.
func ReadyKey(lane Lane, tenantID string) string {
	return readyPrefix(lane) + tenantID
}

// KeysFor returns Redis keys used by enqueue.
func KeysFor(lane Lane, tenantID string) []string {
	return []string{ringKey(lane), vtimeKey(lane), ReadyKey(lane, tenantID)}
}

func ValidateLane(lane Lane) error {
	switch lane {
	case LaneTime, LaneThroughput:
		return nil
	default:
		return fmt.Errorf("unknown fairness lane %q", lane)
	}
}

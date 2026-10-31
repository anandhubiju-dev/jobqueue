package metrics

import (
	"expvar"
	"testing"
)

func TestMetricNamesRegistered(t *testing.T) {
	for _, name := range []string{
		"jobs_started", "jobs_completed", "jobs_failed", "jobs_retried",
		"jobs_taken_over", "job_attempt_count", "job_attempt_ms_total",
	} {
		if expvar.Get(name) == nil {
			t.Errorf("metric %q is not registered", name)
		}
	}
}

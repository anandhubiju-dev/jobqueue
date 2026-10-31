package metrics

import (
	"expvar"
	"time"
)

var (
	JobsStarted   = expvar.NewInt("jobs_started")
	JobsCompleted = expvar.NewInt("jobs_completed")
	JobsFailed    = expvar.NewInt("jobs_failed")
	JobsRetried   = expvar.NewInt("jobs_retried")
	JobsTakenOver = expvar.NewInt("jobs_taken_over")

	durationCount  = expvar.NewInt("job_attempt_count")
	durationMillis = expvar.NewInt("job_attempt_ms_total")
)

// ObserveDuration records how long one attempt took.
// Average = job_attempt_ms_total / job_attempt_count
func ObserveDuration(d time.Duration) {
	durationCount.Add(1)
	durationMillis.Add(d.Milliseconds())
}

// RegisterGauge publishes a value computed fresh on every read.
func RegisterGauge(name string, f func() int64) {
	expvar.Publish(name, expvar.Func(func() any { return f() }))
}

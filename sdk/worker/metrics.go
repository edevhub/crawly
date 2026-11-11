package worker

import "time"

// Metrics receives what a worker observes about itself, for the caller to count. The
// worker calls it from its own goroutines, so an implementation must be safe for
// concurrent use and must not block.
//
// The interface keeps the SDK free of any metrics library. The sub-package prom
// implements it with Prometheus. Pool and Handler values come from the worker's own
// registrations and are few; Code is whatever the handlers report, so an
// implementation that stores it as a label should bound its distinct values.
type Metrics interface {
	// SessionOpened and SessionClosed bracket one session with the Frontier, from its
	// Welcome to the end of the stream. A worker reconnects with a new session.
	SessionOpened(pool string)
	SessionClosed(pool string)
	// TaskStarted is called when a handler starts. Handler is empty when the task names
	// a handler this worker has not registered.
	TaskStarted(pool, handler string)
	// TaskFinished is called when the handler returns, once per TaskStarted.
	TaskFinished(TaskDone)
	// LeaseLost is called when the worker learns that it no longer holds a task: the
	// Frontier revoked the lease (reason "fenced", "expired", "run_cancelled" or "max_duration"), the
	// lease ran out without an extension ("no_extension"), or the Frontier discarded
	// the report ("report_discarded").
	LeaseLost(pool, reason string)
}

// TaskDone is what a finished handler leaves behind.
type TaskDone struct {
	Pool    string
	Handler string
	// Kind is how the task ended: "success", "retry", "failed", "released", or "lost"
	// when the lease was lost and nothing was reported.
	Kind string
	// Code is the outcome code, empty for a success without one.
	Code string
	// Duration is how long the handler ran.
	Duration time.Duration
	// Fetch is what the handler reported about its fetch, nil if nothing.
	Fetch *FetchInfo
}

// WithMetrics makes the worker report to m. Default: no metrics. It does not start
// a listener or touch any global registry: where the numbers go is up to m.
func WithMetrics(m Metrics) Option {
	return func(w *Worker) {
		if m != nil {
			w.metrics = m
		}
	}
}

type noMetrics struct{}

func (noMetrics) SessionOpened(string)    {}
func (noMetrics) SessionClosed(string)    {}
func (noMetrics) TaskStarted(_, _ string) {}
func (noMetrics) TaskFinished(TaskDone)   {}
func (noMetrics) LeaseLost(_, _ string)   {}

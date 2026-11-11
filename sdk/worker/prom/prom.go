// Package prom reports a worker's metrics to Prometheus. It is the one place the worker
// SDK depends on the Prometheus client, so a spider that does not use it does not link it.
//
//	reg := prometheus.NewRegistry()
//	m, _ := prom.New(reg)
//	w, _ := worker.New("frontier:7070", worker.WithInsecure(), worker.WithMetrics(m))
//	// serve reg with promhttp.HandlerFor(reg, promhttp.HandlerOpts{}) wherever you like
//
// The package registers on the registry it is given and on nothing else, and opens no
// listener.
package prom

import (
	"sync"

	"github.com/prometheus/client_golang/prometheus"

	"github.com/edevhub/crawly/sdk/worker"
)

// maxCodes bounds the distinct outcome codes kept as label values. Codes are chosen by
// the handlers; the ones past the bound are counted as "other".
const maxCodes = 100

// Metrics implements worker.Metrics on Prometheus collectors.
type Metrics struct {
	sessions  *prometheus.GaugeVec
	opened    *prometheus.CounterVec
	running   *prometheus.GaugeVec
	finished  *prometheus.CounterVec
	duration  *prometheus.HistogramVec
	fetch     *prometheus.HistogramVec
	leaseLost *prometheus.CounterVec

	mu    sync.Mutex
	codes map[string]struct{}
}

var _ worker.Metrics = (*Metrics)(nil)

// New creates the metrics and registers them on reg. It fails if reg already has
// metrics of the same names, e.g. from a second worker: give each worker its own
// registry, or wrap the registry with prometheus.WrapRegistererWith.
func New(reg prometheus.Registerer) (*Metrics, error) {
	const ns = "crawly_worker"
	m := &Metrics{codes: map[string]struct{}{},
		sessions: prometheus.NewGaugeVec(prometheus.GaugeOpts{Namespace: ns, Name: "sessions",
			Help: "Sessions open with the Frontier, by pool."}, []string{"pool"}),
		opened: prometheus.NewCounterVec(prometheus.CounterOpts{Namespace: ns, Name: "sessions_opened_total",
			Help: "Sessions opened with the Frontier, by pool. More than one per pool means the worker reconnected."}, []string{"pool"}),
		running: prometheus.NewGaugeVec(prometheus.GaugeOpts{Namespace: ns, Name: "tasks_running",
			Help: "Handlers running, by pool and handler."}, []string{"pool", "handler"}),
		finished: prometheus.NewCounterVec(prometheus.CounterOpts{Namespace: ns, Name: "tasks_finished_total",
			Help: "Handlers that returned, by pool, handler, how the task ended (success, retry, failed, released, lost) and its code."},
			[]string{"pool", "handler", "kind", "code"}),
		duration: prometheus.NewHistogramVec(prometheus.HistogramOpts{Namespace: ns, Name: "task_duration_seconds",
			Help:    "How long handlers ran, by pool and handler.",
			Buckets: []float64{0.05, 0.1, 0.25, 0.5, 1, 2.5, 5, 10, 30, 60, 300}}, []string{"pool", "handler"}),
		fetch: prometheus.NewHistogramVec(prometheus.HistogramOpts{Namespace: ns, Name: "fetch_duration_seconds",
			Help:    "Fetch duration handlers reported, by pool and handler.",
			Buckets: []float64{0.05, 0.1, 0.25, 0.5, 1, 2.5, 5, 10, 30, 60}}, []string{"pool", "handler"}),
		leaseLost: prometheus.NewCounterVec(prometheus.CounterOpts{Namespace: ns, Name: "leases_lost_total",
			Help: "Tasks the worker lost while holding them, by reason: fenced, expired, run_cancelled, max_duration, no_extension, report_discarded."},
			[]string{"pool", "reason"}),
	}
	for _, c := range []prometheus.Collector{m.sessions, m.opened, m.running, m.finished, m.duration, m.fetch, m.leaseLost} {
		if err := reg.Register(c); err != nil {
			return nil, err
		}
	}
	return m, nil
}

// handler names a handler for a label: a task for one the worker lacks has none.
func handler(h string) string {
	if h == "" {
		return "unregistered"
	}
	return h
}

func (m *Metrics) code(c string) string {
	m.mu.Lock()
	defer m.mu.Unlock()
	if _, ok := m.codes[c]; ok {
		return c
	}
	if len(m.codes) >= maxCodes {
		return "other"
	}
	m.codes[c] = struct{}{}
	return c
}

// PoolStarted implements worker.Metrics.
func (m *Metrics) PoolStarted(pool string) {
	m.sessions.WithLabelValues(pool)
	m.opened.WithLabelValues(pool)
}

// SessionOpened implements worker.Metrics.
func (m *Metrics) SessionOpened(pool string) {
	m.sessions.WithLabelValues(pool).Inc()
	m.opened.WithLabelValues(pool).Inc()
}

// SessionClosed implements worker.Metrics.
func (m *Metrics) SessionClosed(pool string) { m.sessions.WithLabelValues(pool).Dec() }

// TaskStarted implements worker.Metrics.
func (m *Metrics) TaskStarted(pool, h string) { m.running.WithLabelValues(pool, handler(h)).Inc() }

// TaskFinished implements worker.Metrics.
func (m *Metrics) TaskFinished(d worker.TaskDone) {
	h := handler(d.Handler)
	m.running.WithLabelValues(d.Pool, h).Dec()
	m.finished.WithLabelValues(d.Pool, h, d.Kind, m.code(d.Code)).Inc()
	m.duration.WithLabelValues(d.Pool, h).Observe(d.Duration.Seconds())
	if d.Fetch != nil && d.Fetch.Latency > 0 {
		m.fetch.WithLabelValues(d.Pool, h).Observe(d.Fetch.Latency.Seconds())
	}
}

// LeaseLost implements worker.Metrics.
func (m *Metrics) LeaseLost(pool, reason string) { m.leaseLost.WithLabelValues(pool, reason).Inc() }

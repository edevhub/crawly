package frontier

import (
	"context"
	"strings"
	"sync"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"google.golang.org/grpc"
	"google.golang.org/grpc/status"

	taskv1 "github.com/edevhub/crawly/internal/gen/frontier/task/v1"
	"github.com/edevhub/crawly/internal/store"
)

// The Frontier's metrics (README, Metrics). Every metric is registered on the registry
// the caller gives the Frontier; the Frontier opens no listener of its own.
//
// Labels keep the series bounded:
//
//   - policy is the most specific global policy that matches the queue (policy.Set.Class),
//     never the queue key, because queue keys are hosts and there is no bound on them;
//   - pool and code come from the workers, so each is capped at a fixed number of distinct
//     values per process, and values past the cap are folded into "other". The codes the
//     Frontier itself uses are reserved and never folded. A worker id labels lease expiries
//     only, and only while the worker is recent: an id that has not been seen for
//     workerWindow is forgotten, with its series, so a rolling fleet does not use the cap up;
//   - nothing is labelled by run, task or URL.
const (
	namespace = "crawly_frontier"

	// maxPools, maxWorkers and maxCodes cap the distinct values of a label that workers
	// choose. Generous for a real deployment, finite for a faulty one.
	maxPools   = 100
	maxWorkers = 200
	maxCodes   = 100

	// otherLabel stands for the values past a cap.
	otherLabel = "other"

	// workerWindow is how long a worker id keeps its label after its last expiry.
	workerWindow = time.Hour

	// dueLimit is how many due queues the store lists per pool for the oldest-due gauge.
	dueLimit = 1000
	// statsTimeout bounds one refresh of the store's state.
	statsTimeout = 5 * time.Second
	// defaultStatsInterval is how often the store's state is refreshed, whether or not
	// anyone scrapes: a scrape never waits for the store.
	defaultStatsInterval = 15 * time.Second
)

// codeUndelivered is the release code of an assignment that never reached its worker.
const codeUndelivered = "undelivered"

// metricCodes are the codes the Frontier itself gives outcomes and releases. They keep
// their label whatever the workers report.
var metricCodes = []string{CodeNoHandler, CodeShutdown, codeUndelivered, "lease_expired", "revoked"}

// releaseCode is the label of a released task's code: a closed set, so that an alert on
// no_handler does not depend on what handlers report.
func releaseCode(code string) string {
	switch code {
	case CodeNoHandler, CodeShutdown, codeUndelivered:
		return code
	}
	return otherLabel
}

// capped admits at most max distinct values and folds the rest into "other".
type capped struct {
	mu   sync.Mutex
	max  int
	seen map[string]struct{}
}

// newCapped makes a set that admits limit values besides the reserved ones.
func newCapped(limit int, reserved ...string) *capped {
	c := &capped{max: limit + len(reserved), seen: map[string]struct{}{}}
	for _, r := range reserved {
		c.seen[r] = struct{}{}
	}
	return c
}

func (c *capped) label(v string) string {
	c.mu.Lock()
	defer c.mu.Unlock()
	if _, ok := c.seen[v]; ok {
		return v
	}
	if len(c.seen) >= c.max {
		return otherLabel
	}
	c.seen[v] = struct{}{}
	return v
}

// recent admits at most max values that were seen within ttl, and folds the rest into
// "other". A value that is not seen again for ttl is forgotten, and evict is called with
// it so that its series can go.
type recent struct {
	mu    sync.Mutex
	max   int
	ttl   time.Duration
	now   func() time.Time
	seen  map[string]time.Time
	evict func(string)
}

func newRecent(limit int, ttl time.Duration, evict func(string)) *recent {
	return &recent{max: limit, ttl: ttl, now: time.Now, seen: map[string]time.Time{}, evict: evict}
}

func (r *recent) label(v string) string {
	r.mu.Lock()
	now := r.now()
	if _, ok := r.seen[v]; ok {
		r.seen[v] = now
		r.mu.Unlock()
		return v
	}
	var gone []string
	if len(r.seen) >= r.max {
		gone = r.forgetLocked(now)
	}
	out := otherLabel
	if len(r.seen) < r.max {
		r.seen[v], out = now, v
	}
	r.mu.Unlock()
	r.evictAll(gone)
	return out
}

// sweep forgets the values that have not been seen for ttl.
func (r *recent) sweep() {
	r.mu.Lock()
	gone := r.forgetLocked(r.now())
	r.mu.Unlock()
	r.evictAll(gone)
}

func (r *recent) forgetLocked(now time.Time) []string {
	var gone []string
	for v, at := range r.seen {
		if now.Sub(at) >= r.ttl {
			delete(r.seen, v)
			gone = append(gone, v)
		}
	}
	return gone
}

func (r *recent) evictAll(gone []string) {
	for _, v := range gone {
		r.evict(v)
	}
}

// metrics holds the event metrics of a replica. The gauges are read at scrape time by
// stateCollector.
type metrics struct {
	class func(queue string) string // the policy group of a queue

	pools, codes *capped
	workers      *recent

	dispatched   *prometheus.CounterVec
	dispatchWait *prometheus.HistogramVec
	outcomes     *prometheus.CounterVec
	attempts     *prometheus.CounterVec
	released     *prometheus.CounterVec
	expiries     *prometheus.CounterVec
	blocks       *prometheus.CounterVec
	fetch        *prometheus.HistogramVec
	added        *prometheus.CounterVec
	opened       *prometheus.CounterVec
	storeTime    *prometheus.HistogramVec
	storeErrors  *prometheus.CounterVec
	handled      *prometheus.CounterVec
	handling     *prometheus.HistogramVec
	unserved     *prometheus.CounterVec
	strained     *prometheus.CounterVec
	runsEnded    *prometheus.CounterVec
}

func newMetrics(reg prometheus.Registerer, class func(string) string) (*metrics, error) {
	counter := func(name, help string, labels ...string) *prometheus.CounterVec {
		return prometheus.NewCounterVec(prometheus.CounterOpts{Namespace: namespace, Name: name, Help: help}, labels)
	}
	histogram := func(name, help string, buckets []float64, labels ...string) *prometheus.HistogramVec {
		return prometheus.NewHistogramVec(prometheus.HistogramOpts{Namespace: namespace, Name: name, Help: help, Buckets: buckets}, labels)
	}
	m := &metrics{class: class, pools: newCapped(maxPools), codes: newCapped(maxCodes, metricCodes...),
		dispatched: counter("dispatched_tasks_total", "Tasks handed to workers, by pool and the policy of the queue.", "pool", "policy"),
		dispatchWait: histogram("dispatch_wait_seconds",
			"How long a task was eligible before it was dispatched, by the policy of its queue: what politeness, blocks and busy workers cost.",
			[]float64{0.01, 0.1, 0.5, 1, 2.5, 5, 10, 30, 60, 300, 900, 3600, 14400}, "policy"),
		outcomes: counter("task_outcomes_total",
			"Outcomes workers reported, by the kind and code the worker gave, and the result the Frontier decided on (succeeded, retry, failed, released).",
			"kind", "code", "result"),
		attempts: counter("attempts_total", "Attempts that ended with an outcome or a lease expiry. A release is not an attempt.", "pool"),
		released: counter("released_tasks_total",
			"Tasks handed back to the Frontier, by pool and code (no_handler, shutdown, or undelivered for an assignment that never reached its worker).",
			"pool", "code"),
		expiries: counter("lease_expiries_total",
			"Leases the reaper found expired, by the worker that held them and the pool. A worker that has had no expiry for an hour is forgotten: its series go and a new expiry starts it again; when many workers expire at once the rest are counted as other.",
			"worker", "pool"),
		blocks: counter("queue_blocks_total", "Queue blocks that outcomes set, by the policy of the queue.", "policy"),
		fetch: histogram("fetch_duration_seconds", "Fetch duration workers reported, by the policy of the queue.",
			[]float64{0.05, 0.1, 0.25, 0.5, 1, 2.5, 5, 10, 30, 60}, "policy"),
		added: counter("tasks_added_total",
			"Tasks submitted, by source (seed, discovered) and result (added, duplicate, over_budget, run_closed, invalid).", "source", "status"),
		opened: counter("sessions_opened_total", "Worker sessions opened on this replica, by pool.", "pool"),
		storeTime: histogram("store_call_duration_seconds", "Duration of store calls, by method.",
			[]float64{0.0005, 0.001, 0.0025, 0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1, 2.5, 5}, "method"),
		storeErrors: counter("store_errors_total",
			"Store calls that failed, by method and kind (unavailable, timeout, other). Answers such as not found and a stale lease are not failures. A timeout includes a caller's own deadline.",
			"method", "kind"),
		unserved: counter("unserved_failed_tasks_total",
			"Tasks this replica's sweep failed because no session ran their handler for the grace period (code no_handler, no attempt recorded), by pool.", "pool"),
		strained: counter("strained_attempts_total",
			"Attempts that ended in a retry the outcome marked as strain on the site (the queue is then slowed), by the policy of the queue.", "policy"),
		runsEnded: counter("runs_ended_total",
			"Runs that reached a final state, by state (completed, cancelled, expired, aborted) and, for aborted, the rule that ended it (code, max_failed, consecutive_failures). Every replica reads the whole run event log and counts every event, so take the max over replicas, not the sum.",
			"state", "rule"),
		handled: counter("grpc_server_handled_total", "gRPC calls this replica finished, by service, method and status code.",
			"grpc_service", "grpc_method", "grpc_code"),
		handling: histogram("grpc_server_handling_seconds", "How long unary gRPC calls took, by service and method. Streams are not timed: they last as long as their session.",
			[]float64{0.001, 0.005, 0.025, 0.1, 0.5, 1, 5, 30}, "grpc_service", "grpc_method"),
	}
	m.workers = newRecent(maxWorkers, workerWindow, func(w string) { m.expiries.DeletePartialMatch(prometheus.Labels{"worker": w}) })
	for _, c := range []prometheus.Collector{m.dispatched, m.dispatchWait, m.outcomes, m.attempts, m.released, m.expiries, m.blocks,
		m.fetch, m.added, m.opened, m.storeTime, m.storeErrors, m.handled, m.handling, m.unserved, m.strained, m.runsEnded} {
		if err := reg.Register(c); err != nil {
			return nil, err
		}
	}
	// Series that start at zero, so that the first event is a rise a rate can see.
	for _, method := range storeMethods {
		for _, kind := range storeErrorKinds {
			m.storeErrors.WithLabelValues(method, kind)
		}
	}
	for _, state := range []string{"completed", "cancelled", "expired"} {
		m.runsEnded.WithLabelValues(state, "")
	}
	for _, rule := range abortRules {
		m.runsEnded.WithLabelValues("aborted", rule)
	}
	return m, nil
}

// abortRules names the rules of a run's limits that end it, as the label of runs_ended_total.
var abortRules = map[store.AbortRule]string{
	store.AbortOnCode: "code", store.AbortMaxFailed: "max_failed", store.AbortConsecutiveFailures: "consecutive_failures",
}

// runEnded records the event that ended a run; other events are not counted.
func (m *metrics) runEnded(ev *store.RunEvent) {
	switch ev.Type {
	case store.EventCompleted:
		m.runsEnded.WithLabelValues("completed", "").Inc()
	case store.EventCancelled:
		m.runsEnded.WithLabelValues("cancelled", "").Inc()
	case store.EventExpired:
		m.runsEnded.WithLabelValues("expired", "").Inc()
	case store.EventAborted:
		rule := otherLabel
		if a := ev.Run.Abort; a != nil {
			if r, ok := abortRules[a.Rule]; ok {
				rule = r
			}
		}
		m.runsEnded.WithLabelValues("aborted", rule).Inc()
	case store.EventCreated, store.EventSealed, store.EventPaused, store.EventResumed:
	}
}

// unservedFailed records the tasks a sweep failed.
func (m *metrics) unservedFailed(failed []store.FailedTasks) {
	for _, f := range failed {
		m.unserved.WithLabelValues(m.pools.label(string(f.Pool))).Add(float64(f.Tasks))
	}
}

// initPool creates the series of a pool that has none yet.
func (m *metrics) initPool(pool store.PoolName) {
	p := m.pools.label(string(pool))
	m.attempts.WithLabelValues(p)
	m.opened.WithLabelValues(p)
	m.unserved.WithLabelValues(p)
	for _, code := range []string{CodeNoHandler, CodeShutdown, codeUndelivered} {
		m.released.WithLabelValues(p, code)
	}
}

// outcomeKinds names the kinds a worker can report; anything else is "unknown", so that
// a faulty worker cannot make up label values.
var outcomeKinds = map[taskv1.Outcome_Kind]string{
	taskv1.Outcome_KIND_SUCCESS: "success", taskv1.Outcome_KIND_RETRY: "retry",
	taskv1.Outcome_KIND_FAILED: "failed", taskv1.Outcome_KIND_RELEASED: "released",
}

var results = map[store.TaskResult]string{
	store.ResultSucceeded: "succeeded", store.ResultFailed: "failed", store.ResultRetry: "retry", store.ResultReleased: "released",
}

var addStatuses = map[store.AddStatus]string{
	store.AddAdded: "added", store.AddDuplicate: "duplicate", store.AddOverBudget: "over_budget",
	store.AddRunClosed: "run_closed", store.AddInvalid: "invalid",
}

func name(m map[taskv1.Outcome_Kind]string, k taskv1.Outcome_Kind) string {
	if v, ok := m[k]; ok {
		return v
	}
	return "unknown"
}

// dispatch records the assignments of one Dispatch call.
func (m *metrics) dispatch(pool store.PoolName, as []store.Assignment) {
	p := m.pools.label(string(pool))
	for i := range as {
		class := m.class(string(as[i].Info.Queue))
		m.dispatched.WithLabelValues(p, class).Inc()
		m.dispatchWait.WithLabelValues(class).Observe(as[i].Waited.Seconds())
	}
}

// finished records an outcome the store accepted. kind is the worker's.
func (m *metrics) finished(info store.LeaseInfo, kind taskv1.Outcome_Kind, f store.Finish) {
	pool, class := m.pools.label(string(info.Pool)), m.class(string(info.Queue))
	result := results[f.Result]
	m.outcomes.WithLabelValues(name(outcomeKinds, kind), m.codes.label(f.Record.Code), result).Inc()
	if f.Result == store.ResultReleased {
		m.released.WithLabelValues(pool, releaseCode(f.Record.Code)).Inc()
	} else {
		m.attempts.WithLabelValues(pool).Inc()
	}
	if f.Strain {
		m.strained.WithLabelValues(class).Inc()
	}
	if f.Block > 0 {
		m.blocks.WithLabelValues(class).Inc()
	}
	if f.Record.Latency > 0 {
		m.fetch.WithLabelValues(class).Observe(f.Record.Latency.Seconds())
	}
}

// expired records a lease the reaper ended.
func (m *metrics) expired(info store.LeaseInfo, f store.Finish) {
	pool := m.pools.label(string(info.Pool))
	worker := info.Worker.ID
	if worker == "" {
		worker = "unknown"
	}
	m.expiries.WithLabelValues(m.workers.label(worker), pool).Inc()
	m.finished(info, taskv1.Outcome_KIND_RETRY, f)
}

// undelivered records an assignment returned because it never reached its worker.
func (m *metrics) undelivered(pool store.PoolName) {
	p := m.pools.label(string(pool))
	m.released.WithLabelValues(p, codeUndelivered).Inc()
	m.outcomes.WithLabelValues("released", codeUndelivered, "released").Inc()
}

// tasksAdded records the results of adding tasks.
func (m *metrics) tasksAdded(source string, res []store.AddResult) {
	for _, r := range res {
		m.added.WithLabelValues(source, addStatuses[r.Status]).Inc()
	}
}

// grpcHandled records a finished call; unary calls are timed as well.
func (m *metrics) grpcHandled(fullMethod string, err error, took time.Duration, unary bool) {
	service, method := splitMethod(fullMethod)
	m.handled.WithLabelValues(service, method, status.Code(err).String()).Inc()
	if unary {
		m.handling.WithLabelValues(service, method).Observe(took.Seconds())
	}
}

// splitMethod splits "/pkg.Service/Method".
func splitMethod(full string) (service, method string) {
	full = strings.TrimPrefix(full, "/")
	service, method, _ = strings.Cut(full, "/")
	return service, method
}

// MetricsOptions are the gRPC server options that count the calls the replica handles.
// They add to ServerOptions.
func (s *Server) MetricsOptions() []grpc.ServerOption {
	return []grpc.ServerOption{
		grpc.ChainUnaryInterceptor(func(ctx context.Context, req any, info *grpc.UnaryServerInfo, h grpc.UnaryHandler) (any, error) {
			start := time.Now()
			resp, err := h(ctx, req)
			s.m.grpcHandled(info.FullMethod, err, time.Since(start), true)
			return resp, err
		}),
		grpc.ChainStreamInterceptor(func(srv any, ss grpc.ServerStream, info *grpc.StreamServerInfo, h grpc.StreamHandler) error {
			err := h(srv, ss)
			s.m.grpcHandled(info.FullMethod, err, 0, false)
			return err
		}),
	}
}

// ---- gauges

// stateCollector reports what the replica holds in memory (per replica) and what the
// store holds (global: every replica reports the same). The store's state is refreshed
// in the background, and a scrape only reads the last good copy: it never waits for the
// store, and a store that is slow or down does not make the gauges disappear. The
// refresh's own health is exported, so that stale figures can be told from fresh ones.
type stateCollector struct {
	s *Server

	sessions, credits, assignments *prometheus.Desc
	tasks, oldestDue, info         *prometheus.Desc
	statsUp, statsOK               *prometheus.Desc

	mu     sync.Mutex
	tried  bool      // a refresh has finished
	up     bool      // the last refresh succeeded
	lastOK time.Time // wall clock of the last good refresh; zero: none yet
	at     time.Time // the store clock at the last good refresh
	stats  []store.PoolStats
}

func newStateCollector(s *Server) *stateCollector {
	desc := func(name, help string, labels ...string) *prometheus.Desc {
		return prometheus.NewDesc(namespace+"_"+name, help, labels, nil)
	}
	return &stateCollector{s: s,
		sessions:    desc("sessions", "Worker sessions open on this replica, by pool. Per replica: sum over replicas.", "pool"),
		credits:     desc("credits", "Execution slots workers granted on this replica and have not used, by pool. Per replica: sum over replicas.", "pool"),
		assignments: desc("assignments", "Assignments held by sessions of this replica and not completed, by pool. Per replica: sum over replicas.", "pool"),
		tasks: desc("pool_tasks",
			"Tasks of a pool across all runs, by state: pending (ready to dispatch), delayed (waiting for a time, a slot or a resume), in_flight. Global: every replica reports the same, use max. As of the last good refresh.",
			"pool", "state"),
		oldestDue: desc("oldest_due_task_age_seconds",
			"Age of the longest waiting ready work in a pool, by the policy of its queue: how long nothing but a worker has been missing. Absent when no work is due. Global: use max. As of the last good refresh.",
			"pool", "policy"),
		info:    desc("replica_info", "Always 1; names the replica and the policy set it runs.", "replica", "policy_set"),
		statsUp: desc("pool_stats_up", "1 when the last refresh of the store's state (pool_tasks, oldest_due_task_age_seconds) succeeded, 0 when it failed. Absent before the first refresh."),
		statsOK: desc("pool_stats_last_success_timestamp_seconds", "When the store's state was last read successfully, in Unix seconds; 0 before the first success."),
	}
}

func (c *stateCollector) Describe(ch chan<- *prometheus.Desc) {
	for _, d := range []*prometheus.Desc{c.sessions, c.credits, c.assignments, c.tasks, c.oldestDue, c.info, c.statsUp, c.statsOK} {
		ch <- d
	}
}

// loop refreshes the store's state at once and then at every interval, and forgets the
// worker ids that were not seen for a while.
func (c *stateCollector) loop(ctx context.Context, every time.Duration) {
	t := time.NewTicker(every)
	defer t.Stop()
	for {
		c.refresh(ctx)
		c.s.m.workers.sweep()
		select {
		case <-ctx.Done():
			return
		case <-t.C:
		}
	}
}

// refresh reads the store's state, keeping the last good copy when it cannot.
func (c *stateCollector) refresh(parent context.Context) {
	ctx, cancel := context.WithTimeout(parent, statsTimeout)
	defer cancel()
	at, err := c.s.store.Now(ctx)
	var stats []store.PoolStats
	if err == nil {
		stats, err = c.s.store.PoolStats(ctx, dueLimit)
	}
	if err != nil && parent.Err() != nil {
		return // the replica is stopping
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	c.tried, c.up = true, err == nil
	if err != nil {
		c.s.log.Warn("metrics: reading the pool statistics", "err", err)
		return
	}
	c.lastOK, c.at, c.stats = time.Now(), at, stats
}

// pool is what a metric says about one pool label.
type poolFigures struct {
	sessions, credits, assignments int
	pending, delayed, inFlight     uint64
	due                            map[string]time.Time // oldest ready time by policy
}

func (c *stateCollector) Collect(ch chan<- prometheus.Metric) {
	s := c.s
	ch <- prometheus.MustNewConstMetric(c.info, prometheus.GaugeValue, 1, s.cfg.ReplicaID, s.pol.Hash())

	c.mu.Lock()
	stats, tried, up, lastOK, at := c.stats, c.tried, c.up, c.lastOK, c.at
	c.mu.Unlock()
	if tried {
		ch <- prometheus.MustNewConstMetric(c.statsUp, prometheus.GaugeValue, b2f(up))
		ch <- prometheus.MustNewConstMetric(c.statsOK, prometheus.GaugeValue, float64(lastOK.Unix()))
	}
	now := at.Add(time.Since(lastOK))

	// Everything is summed by the label first, and emitted once: pools folded into
	// "other" must not produce the same series twice, which would fail the whole scrape.
	by := map[string]*poolFigures{}
	figures := func(pool store.PoolName) *poolFigures {
		label := s.m.pools.label(string(pool))
		if by[label] == nil {
			by[label] = &poolFigures{due: map[string]time.Time{}}
		}
		return by[label]
	}
	s.mu.Lock()
	pools := make([]*pool, 0, len(s.pools))
	for _, p := range s.pools {
		pools = append(pools, p)
	}
	s.mu.Unlock()
	for _, p := range pools {
		f := figures(p.name)
		p.mu.Lock()
		for sess := range p.sessions {
			f.sessions++
			f.credits += sess.credits
			f.assignments += len(sess.leases)
		}
		p.mu.Unlock()
	}
	for i := range stats {
		ps := &stats[i]
		f := figures(ps.Pool)
		f.pending, f.delayed, f.inFlight = f.pending+ps.Pending, f.delayed+ps.Delayed, f.inFlight+ps.InFlight
		for _, d := range ps.Due {
			class := s.m.class(string(d.Queue))
			if at, seen := f.due[class]; !seen || d.Since.Before(at) {
				f.due[class] = d.Since
			}
		}
	}
	for pool, f := range by {
		ch <- prometheus.MustNewConstMetric(c.sessions, prometheus.GaugeValue, float64(f.sessions), pool)
		ch <- prometheus.MustNewConstMetric(c.credits, prometheus.GaugeValue, float64(f.credits), pool)
		ch <- prometheus.MustNewConstMetric(c.assignments, prometheus.GaugeValue, float64(f.assignments), pool)
		if lastOK.IsZero() {
			continue // nothing has been read from the store yet
		}
		// A pool the replica knows that has no work reports zero, so that it can be told
		// from a pool nobody reports.
		for state, n := range map[string]uint64{"pending": f.pending, "delayed": f.delayed, "in_flight": f.inFlight} {
			ch <- prometheus.MustNewConstMetric(c.tasks, prometheus.GaugeValue, float64(n), pool, state)
		}
		for class, since := range f.due {
			ch <- prometheus.MustNewConstMetric(c.oldestDue, prometheus.GaugeValue, max(now.Sub(since), 0).Seconds(), pool, class)
		}
	}
}

func b2f(b bool) float64 {
	if b {
		return 1
	}
	return 0
}

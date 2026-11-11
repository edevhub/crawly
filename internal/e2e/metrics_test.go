package e2e

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	dto "github.com/prometheus/client_model/go"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/status"

	workerv1 "github.com/edevhub/crawly/internal/gen/frontier/worker/v1"
	"github.com/edevhub/crawly/sdk"
	"github.com/edevhub/crawly/sdk/run"
	"github.com/edevhub/crawly/sdk/session"
	"github.com/edevhub/crawly/sdk/task"
	"github.com/edevhub/crawly/sdk/worker"
	"github.com/edevhub/crawly/sdk/worker/prom"
)

// gathered is one reading of a registry.
type gathered []*dto.MetricFamily

func gather(t *testing.T, reg prometheus.Gatherer) gathered {
	t.Helper()
	mfs, err := reg.Gather()
	if err != nil {
		t.Fatal(err)
	}
	return mfs
}

// sum adds up the series of a metric whose labels include the given pairs (name, value,
// name, value, ...). A histogram counts as its number of observations. ok is false when
// no series matches.
func (g gathered) sum(name string, labels ...string) (total float64, ok bool) {
	for _, mf := range g {
		if mf.GetName() != name {
			continue
		}
	series:
		for _, m := range mf.GetMetric() {
			have := map[string]string{}
			for _, l := range m.GetLabel() {
				have[l.GetName()] = l.GetValue()
			}
			for i := 0; i+1 < len(labels); i += 2 {
				if have[labels[i]] != labels[i+1] {
					continue series
				}
			}
			ok = true
			switch {
			case m.GetHistogram() != nil:
				total += float64(m.GetHistogram().GetSampleCount())
			case m.GetCounter() != nil:
				total += m.GetCounter().GetValue()
			default:
				total += m.GetGauge().GetValue()
			}
		}
	}
	return total, ok
}

// want fails the test unless the metric's matching series satisfy check.
func (g gathered) want(t *testing.T, check func(float64) bool, name string, labels ...string) {
	t.Helper()
	got, ok := g.sum(name, labels...)
	if !ok || !check(got) {
		t.Errorf("%s%v = %v (present: %v)", name, labels, got, ok)
	}
}

func equals(n float64) func(float64) bool  { return func(v float64) bool { return v == n } }
func atLeast(n float64) func(float64) bool { return func(v float64) bool { return v >= n } }

func eventually(t *testing.T, what string, cond func() bool) {
	t.Helper()
	for deadline := time.Now().Add(15 * time.Second); time.Now().Before(deadline); time.Sleep(20 * time.Millisecond) {
		if cond() {
			return
		}
	}
	t.Fatalf("timed out waiting for %s", what)
}

// TestMetrics runs a crawl with a retry that blocks the queue, a duplicate, and a task
// no worker can run, and reads both sides: the Frontier's metrics and the worker's.
func TestMetrics(t *testing.T) { backends(t, testMetrics) }

func testMetrics(t *testing.T, start startFunc) { //nolint:thelper // a test body, not a helper: failures must point into it
	ctx := t.Context()
	f := start(t)
	rc := runClient(t, f.Addr)

	workerReg := prometheus.NewRegistry()
	wm, err := prom.New(workerReg)
	if err != nil {
		t.Fatal(err)
	}
	w, err := worker.New(f.Addr, worker.WithInsecure(), worker.WithID("w1"), worker.WithMetrics(wm), worker.WithShutdownGrace(time.Second))
	if err != nil {
		t.Fatal(err)
	}
	var flaky bool
	register(t, w, "page", worker.HandlerFunc(func(_ context.Context, tk *worker.Task) (*worker.Result, error) {
		switch tk.URL {
		case "https://a.example/ok":
			return &worker.Result{Fetch: &worker.FetchInfo{HTTPStatus: 200, Latency: 20 * time.Millisecond},
				Tasks: []worker.NewTask{{URL: "https://a.example/child"}, {URL: "https://a.example/child"}}}, nil
		case "https://a.example/flaky":
			if !flaky { // one task at a time reaches this handler: the queue is blocked meanwhile
				flaky = true
				return nil, worker.Retry("http_503", errors.New("try later"), worker.BlockQueue(50*time.Millisecond))
			}
		}
		return nil, nil
	}))
	go func() { _ = w.Run(ctx) }()

	if _, err := rc.CreateRun(ctx, &run.CreateRunRequest{RunId: "m"}); err != nil {
		t.Fatal(err)
	}
	seed(t, ctx, f.Addr, "m",
		&task.Spec{Request: &task.Request{Url: "https://a.example/ok"}, Handler: "page"},
		&task.Spec{Request: &task.Request{Url: "https://a.example/flaky"}, Handler: "page"},
		&task.Spec{Request: &task.Request{Url: "https://a.example/ghost"}, Handler: "nobody"})
	if _, err := rc.SealRun(ctx, &run.SealRunRequest{RunId: "m"}); err != nil {
		t.Fatal(err)
	}
	st := waitRun(t, ctx, rc, "m").GetStats()
	if st.GetSucceeded() != 3 || st.GetFailed() != 1 {
		t.Fatalf("stats: %v", st)
	}

	// The Frontier's side. The policy of the test Frontier matches every queue.
	g := gather(t, f.Metrics)
	g.want(t, atLeast(5), "crawly_frontier_dispatched_tasks_total", "pool", "default", "policy", "fast")
	g.want(t, atLeast(5), "crawly_frontier_dispatch_wait_seconds", "policy", "fast")
	g.want(t, equals(1), "crawly_frontier_task_outcomes_total", "kind", "retry", "code", "http_503", "result", "retry")
	g.want(t, atLeast(3), "crawly_frontier_task_outcomes_total", "kind", "success", "result", "succeeded")
	g.want(t, atLeast(1), "crawly_frontier_released_tasks_total", "pool", "default", "code", "no_handler")
	g.want(t, atLeast(4), "crawly_frontier_attempts_total", "pool", "default")
	g.want(t, equals(1), "crawly_frontier_queue_blocks_total", "policy", "fast")
	g.want(t, equals(1), "crawly_frontier_fetch_duration_seconds", "policy", "fast")
	g.want(t, equals(3), "crawly_frontier_tasks_added_total", "source", "seed", "status", "added")
	g.want(t, equals(1), "crawly_frontier_tasks_added_total", "source", "discovered", "status", "added")
	g.want(t, equals(1), "crawly_frontier_tasks_added_total", "source", "discovered", "status", "duplicate")
	g.want(t, atLeast(1), "crawly_frontier_sessions_opened_total", "pool", "default")
	g.want(t, atLeast(1), "crawly_frontier_store_call_duration_seconds", "method", "Dispatch")
	g.want(t, atLeast(1), "crawly_frontier_store_call_duration_seconds", "method", "Finish")
	g.want(t, equals(1), "crawly_frontier_grpc_server_handled_total", "grpc_service", "crawly.frontier.run.v1.RunService", "grpc_method", "CreateRun", "grpc_code", "OK")
	g.want(t, equals(1), "crawly_frontier_grpc_server_handling_seconds", "grpc_method", "CreateRun")
	g.want(t, equals(1), "crawly_frontier_sessions", "pool", "default")
	// The store's state is refreshed in the background: wait for a refresh after the run.
	eventually(t, "the store's gauges to show the finished run", func() bool {
		g := gather(t, f.Metrics)
		pending, ok1 := g.sum("crawly_frontier_pool_tasks", "pool", "default", "state", "pending")
		flight, ok2 := g.sum("crawly_frontier_pool_tasks", "pool", "default", "state", "in_flight")
		return ok1 && ok2 && pending == 0 && flight == 0
	})
	gather(t, f.Metrics).want(t, equals(1), "crawly_frontier_pool_stats_up")
	g.want(t, equals(1), "crawly_frontier_replica_info")
	if n, _ := g.sum("crawly_frontier_store_errors_total"); n != 0 {
		t.Errorf("no store call failed, and %v were counted as failures", n)
	}

	// The worker's side. A handler is reported finished just after its report is sent.
	eventually(t, "the worker to report its handlers finished", func() bool {
		n, _ := gather(t, workerReg).sum("crawly_worker_tasks_running")
		return n == 0
	})
	wg := gather(t, workerReg)
	wg.want(t, equals(1), "crawly_worker_sessions", "pool", "default")
	wg.want(t, atLeast(1), "crawly_worker_sessions_opened_total", "pool", "default")
	wg.want(t, atLeast(3), "crawly_worker_tasks_finished_total", "pool", "default", "handler", "page", "kind", "success")
	wg.want(t, equals(1), "crawly_worker_tasks_finished_total", "handler", "page", "kind", "retry", "code", "http_503")
	wg.want(t, atLeast(1), "crawly_worker_tasks_finished_total", "handler", "unregistered", "kind", "released", "code", "no_handler")
	wg.want(t, equals(1), "crawly_worker_fetch_duration_seconds", "handler", "page")
	wg.want(t, atLeast(4), "crawly_worker_task_duration_seconds", "handler", "page")
	wg.want(t, equals(0), "crawly_worker_tasks_running")
}

// TestMetricsSilentWorker: a worker that takes an assignment and never answers. The
// Frontier lists its session while it holds the assignment, and counts the lease it
// reaps against that worker.
func TestMetricsSilentWorker(t *testing.T) { backends(t, testMetricsSilentWorker) }

func testMetricsSilentWorker(t *testing.T, start startFunc) { //nolint:thelper // a test body, not a helper: failures must point into it
	ctx := t.Context()
	f := start(t)
	rc := runClient(t, f.Addr)
	if _, err := rc.CreateRun(ctx, &run.CreateRunRequest{RunId: "silent"}); err != nil {
		t.Fatal(err)
	}
	seed(t, ctx, f.Addr, "silent", &task.Spec{Request: &task.Request{Url: "https://a.example/"}, Handler: "page"})

	conn, err := grpc.NewClient(f.Addr, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = conn.Close() })
	// The replica may not have started its loops yet: it answers Unavailable until it has.
	var stream grpc.BidiStreamingClient[workerv1.WorkRequest, workerv1.WorkResponse]
	for deadline := time.Now().Add(10 * time.Second); ; time.Sleep(20 * time.Millisecond) {
		var err error
		stream, err = workerv1.NewWorkerServiceClient(conn).Work(ctx)
		if err != nil {
			t.Fatal(err)
		}
		for _, m := range []*workerv1.WorkRequest{
			{Msg: &workerv1.WorkRequest_Hello{Hello: &workerv1.Hello{WorkerId: "ghost", BuildVersion: "b1", Pool: "default"}}},
			{Msg: &workerv1.WorkRequest_Credit{Credit: &workerv1.Credit{Slots: 3}}},
		} {
			if err := stream.Send(m); err != nil {
				break // the stream ended: Recv says why
			}
		}
		m, err := stream.Recv()
		if err == nil && m.GetWelcome() != nil {
			break
		}
		if status.Code(err) != codes.Unavailable || time.Now().After(deadline) {
			t.Fatalf("opening a session: %v %v", m, err)
		}
	}
	for {
		m, err := stream.Recv()
		if err != nil {
			t.Fatal(err)
		}
		if m.GetAssignment() != nil {
			break
		}
	}

	sc := clientOf(t, f.Addr).Sessions
	list, err := sc.ListSessions(ctx, &session.ListSessionsRequest{})
	if err != nil || len(list.GetSessions()) != 1 {
		t.Fatalf("ListSessions: %v %v", list, err)
	}
	s := list.GetSessions()[0]
	if s.GetWorkerId() != "ghost" || s.GetBuildVersion() != "b1" || s.GetPool() != "default" || s.GetCredits() != 2 ||
		s.GetUnfinishedAssignments() != 1 || s.GetReplicaId() == "" || s.GetReplicaId() != list.GetReplicaId() ||
		s.GetSessionId() == "" || time.Since(s.GetConnectedTime().AsTime()) > time.Minute {
		t.Fatalf("session: %v", s)
	}
	if other, err := sc.ListSessions(ctx, &session.ListSessionsRequest{Pool: "browser"}); err != nil || len(other.GetSessions()) != 0 {
		t.Fatalf("ListSessions of another pool: %v %v", other, err)
	}
	g := gather(t, f.Metrics)
	g.want(t, equals(1), "crawly_frontier_sessions", "pool", "default")
	g.want(t, equals(2), "crawly_frontier_credits", "pool", "default")
	g.want(t, equals(1), "crawly_frontier_assignments", "pool", "default")

	// The lease is two seconds: the reaper ends it and names the worker.
	eventually(t, "the lease of the silent worker to be counted", func() bool {
		n, _ := gather(t, f.Metrics).sum("crawly_frontier_lease_expiries_total", "worker", "ghost", "pool", "default")
		return n >= 1
	})
	gather(t, f.Metrics).want(t, atLeast(1), "crawly_frontier_task_outcomes_total", "kind", "retry", "code", "lease_expired")
}

// TestMetricsDueWork: work that is ready and has no worker shows as a growing age, and
// as a pool with no sessions.
func TestMetricsDueWork(t *testing.T) { backends(t, testMetricsDueWork) }

func testMetricsDueWork(t *testing.T, start startFunc) { //nolint:thelper // a test body, not a helper: failures must point into it
	ctx := t.Context()
	f := start(t)
	rc := runClient(t, f.Addr)
	if _, err := rc.CreateRun(ctx, &run.CreateRunRequest{RunId: "due"}); err != nil {
		t.Fatal(err)
	}
	seed(t, ctx, f.Addr, "due",
		&task.Spec{Request: &task.Request{Url: "https://a.example/1"}, Handler: "page"},
		&task.Spec{Request: &task.Request{Url: "https://b.example/1"}, Handler: "page", Pool: "browser"})
	time.Sleep(300 * time.Millisecond)
	eventually(t, "the store's gauges to show the work", func() bool {
		n, _ := gather(t, f.Metrics).sum("crawly_frontier_pool_tasks", "pool", "browser", "state", "pending")
		return n == 1
	})

	g := gather(t, f.Metrics)
	g.want(t, func(v float64) bool { return v >= 0.3 && v < 30 }, "crawly_frontier_oldest_due_task_age_seconds", "pool", "default", "policy", "fast")
	g.want(t, func(v float64) bool { return v >= 0.3 && v < 30 }, "crawly_frontier_oldest_due_task_age_seconds", "pool", "browser", "policy", "fast")
	g.want(t, equals(1), "crawly_frontier_pool_tasks", "pool", "default", "state", "pending")
	g.want(t, equals(1), "crawly_frontier_pool_tasks", "pool", "browser", "state", "pending")
	g.want(t, equals(0), "crawly_frontier_sessions", "pool", "default")
	g.want(t, equals(0), "crawly_frontier_sessions", "pool", "browser")
	g.want(t, equals(1), "crawly_frontier_pool_stats_up")
}

// clientOf returns the SDK client that runClient made for the address.
func clientOf(t *testing.T, addr string) *sdk.Client {
	t.Helper()
	c, ok := clients[addr]
	if !ok {
		t.Fatalf("no client for %s", addr)
	}
	return c
}

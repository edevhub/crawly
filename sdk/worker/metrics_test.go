package worker_test

import (
	"context"
	"sync"
	"testing"
	"time"

	"github.com/edevhub/crawly/sdk/run"
	"github.com/edevhub/crawly/sdk/worker"
)

// recorder is a worker.Metrics that keeps what it is told.
type recorder struct {
	mu      sync.Mutex
	opened  int
	closed  int
	started []string
	done    []worker.TaskDone
	lost    []string
}

func (r *recorder) SessionOpened(string) { r.mu.Lock(); r.opened++; r.mu.Unlock() }
func (r *recorder) SessionClosed(string) { r.mu.Lock(); r.closed++; r.mu.Unlock() }
func (r *recorder) TaskStarted(pool, handler string) {
	r.mu.Lock()
	r.started = append(r.started, pool+"/"+handler)
	r.mu.Unlock()
}
func (r *recorder) TaskFinished(d worker.TaskDone) {
	r.mu.Lock()
	r.done = append(r.done, d)
	r.mu.Unlock()
}
func (r *recorder) LeaseLost(pool, reason string) {
	r.mu.Lock()
	r.lost = append(r.lost, pool+"/"+reason)
	r.mu.Unlock()
}

// TestMetricsHooks: the worker reports its sessions, its handlers, and a lease it lost.
func TestMetricsHooks(t *testing.T) {
	e := newEnv(t)
	rec := &recorder{}
	started := make(chan struct{})
	w := newWorker(t, e.addr, worker.WithMetrics(rec))
	register(t, w, "page", worker.HandlerFunc(func(ctx context.Context, tk *worker.Task) (*worker.Result, error) {
		if tk.URL == "https://a.example/hang" {
			close(started)
			<-ctx.Done()
			return nil, ctx.Err()
		}
		return &worker.Result{Code: "http_200", Fetch: &worker.FetchInfo{Latency: time.Second}}, nil
	}))
	stop := e.start(w)

	e.crawl("fine", nil, spec("https://a.example/ok", "page"))
	e.open("hung", nil, spec("https://a.example/hang", "page"))
	<-started
	if _, err := e.c.Runs.CancelRun(context.Background(), &run.CancelRunRequest{RunId: "hung"}); err != nil {
		t.Fatal(err)
	}
	for deadline := time.Now().Add(10 * time.Second); ; time.Sleep(10 * time.Millisecond) {
		rec.mu.Lock()
		n := len(rec.done)
		rec.mu.Unlock()
		if n == 2 {
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("%d handlers were reported as finished, want 2", n)
		}
	}
	if err := stop(); err != nil {
		t.Fatal(err)
	}

	rec.mu.Lock()
	defer rec.mu.Unlock()
	if rec.opened != 1 || rec.closed != 1 {
		t.Errorf("sessions opened %d, closed %d", rec.opened, rec.closed)
	}
	if len(rec.started) != 2 || rec.started[0] != "default/page" {
		t.Errorf("started: %v", rec.started)
	}
	if len(rec.done) != 2 {
		t.Fatalf("finished: %+v", rec.done)
	}
	ok, hung := rec.done[0], rec.done[1]
	if ok.Pool != "default" || ok.Handler != "page" || ok.Kind != "success" || ok.Code != "http_200" || ok.Fetch == nil || ok.Fetch.Latency != time.Second {
		t.Errorf("the finished handler: %+v", ok)
	}
	if hung.Kind != "lost" || hung.Duration <= 0 {
		t.Errorf("the revoked handler: %+v", hung)
	}
	if len(rec.lost) != 1 || rec.lost[0] != "default/run_cancelled" {
		t.Errorf("leases lost: %v", rec.lost)
	}
}

// TestWithMetricsNilKeepsDefault: a nil Metrics is no metrics, not a crash.
func TestWithMetricsNilKeepsDefault(t *testing.T) {
	e := newEnv(t)
	w := newWorker(t, e.addr, worker.WithMetrics(nil))
	register(t, w, "page", noop)
	e.start(w)
	if st := e.crawl("nil", nil, spec("https://a.example/", "page")); st.GetSucceeded() != 1 {
		t.Fatalf("stats: %v", st)
	}
}

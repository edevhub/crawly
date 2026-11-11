package crawlytest_test

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/edevhub/crawly/sdk"
	"github.com/edevhub/crawly/sdk/crawlytest"
	"github.com/edevhub/crawly/sdk/local"
	"github.com/edevhub/crawly/sdk/run"
	"github.com/edevhub/crawly/sdk/task"
	"github.com/edevhub/crawly/sdk/worker"
)

// site is a listing of three products, one of which is gone and one of which fails once.
func site(t *testing.T) *httptest.Server {
	t.Helper()
	var flaky atomic.Int64
	mux := http.NewServeMux()
	mux.HandleFunc("/{$}", func(w http.ResponseWriter, _ *http.Request) { _, _ = fmt.Fprint(w, "/p/1 /p/2 /p/3 /p/1") })
	mux.HandleFunc("/p/1", func(w http.ResponseWriter, _ *http.Request) { _, _ = fmt.Fprint(w, "product 1") })
	mux.HandleFunc("/p/2", func(w http.ResponseWriter, _ *http.Request) {
		if flaky.Add(1) == 1 {
			w.WriteHeader(http.StatusServiceUnavailable)
			return
		}
		_, _ = fmt.Fprint(w, "product 2")
	})
	srv := httptest.NewServer(mux)
	t.Cleanup(srv.Close)
	return srv
}

// TestCrawl is a spider test as a user of the SDK writes it: recorded pages, the real
// handlers, and assertions on the run and on what was emitted.
func TestCrawl(t *testing.T) {
	srv := site(t)
	f := crawlytest.Start(t)
	w, err := f.Worker()
	if err != nil {
		t.Fatal(err)
	}
	var items atomic.Int64
	listing := worker.ParserFunc(func(_ context.Context, _ *worker.Task, r *worker.Response) (*worker.Result, error) {
		res := &worker.Result{}
		for path := range strings.FieldsSeq(string(r.Body)) {
			res.Tasks = append(res.Tasks, worker.NewTask{URL: srv.URL + path, Handler: "product"})
		}
		return res, nil
	})
	product := worker.ParserFunc(func(context.Context, *worker.Task, *worker.Response) (*worker.Result, error) {
		items.Add(1)
		return &worker.Result{Items: 1}, nil
	})
	if err := w.Register("listing", worker.HTTP(listing)); err != nil {
		t.Fatal(err)
	}
	if err := w.Register("product", worker.HTTP(product)); err != nil {
		t.Fatal(err)
	}

	var events atomic.Int64
	final, err := f.Crawl(t.Context(), w, []*task.Spec{{Request: &task.Request{Url: srv.URL + "/"}, Handler: "listing"}},
		local.WithWait(sdk.OnEvent(func(*run.Event) { events.Add(1) })))
	if err != nil {
		t.Fatal(err)
	}
	st := final.GetStats()
	// The listing and two products succeed, one of them on its second attempt; the
	// third product is a 404; the repeated link is a duplicate.
	if final.GetState() != run.StateCompleted || st.GetSucceeded() != 3 || st.GetFailed() != 1 || st.GetDuplicates() != 1 ||
		st.GetByCode()["http_404"] != 1 || st.GetByCode()["http_503"] != 1 || items.Load() != 2 {
		t.Fatalf("state %v, stats %v, %d items", final.GetState(), st, items.Load())
	}
	if events.Load() == 0 {
		t.Error("the wait options were not applied")
	}
}

// page is a seed for the handler "page".
func page(url string) []*task.Spec {
	return []*task.Spec{{Request: &task.Request{Url: url}, Handler: "page"}}
}

// newWorker is a worker of f whose handler "page" counts its calls.
func newWorker(t *testing.T, f *local.Frontier, calls *atomic.Int64, opts ...worker.RegisterOption) *worker.Worker {
	t.Helper()
	w, err := f.Worker()
	if err != nil {
		t.Fatal(err)
	}
	err = w.Register("page", worker.HandlerFunc(func(context.Context, *worker.Task) (*worker.Result, error) {
		calls.Add(1)
		return nil, nil
	}), opts...)
	if err != nil {
		t.Fatal(err)
	}
	return w
}

// TestCrawlErrors: what cannot be crawled is an error found before a run exists, and
// a crawl that fails later leaves no work behind for the next one.
func TestCrawlErrors(t *testing.T) {
	f := crawlytest.Start(t)
	var calls atomic.Int64
	bare, err := f.Worker()
	if err != nil {
		t.Fatal(err)
	}
	if _, err := f.Crawl(t.Context(), bare, page("https://a.example/")); err == nil {
		t.Error("a worker without handlers must be an error")
	}
	if _, err := f.Crawl(t.Context(), newWorker(t, f, &calls, worker.InPool("other")), page("https://a.example/")); err == nil {
		t.Error("a seed for a pool the worker does not serve must be an error")
	}
	// A seed that is not added fails the crawl after the run was created.
	seeds := append(page("https://a.example/first"), page("not a url")...)
	final, err := f.Crawl(t.Context(), newWorker(t, f, &calls), seeds)
	if err == nil || final.GetState() != run.StateCancelled {
		t.Errorf("a seed that is not added: run %v, err %v; want the cancelled run and an error", final.GetState(), err)
	}
	// A worker serves one crawl: the second fails when the worker is started.
	used := newWorker(t, f, &calls)
	if _, err := f.Crawl(t.Context(), used, page("https://a.example/once")); err != nil {
		t.Fatal(err)
	}
	if final, err := f.Crawl(t.Context(), used, page("https://a.example/twice")); err == nil || final.GetState() != run.StateCancelled {
		t.Errorf("a used worker: run %v, err %v", final.GetState(), err)
	}

	// None of the failed crawls left a task for this one: its handler runs once.
	calls.Store(0)
	final, err = f.Crawl(t.Context(), newWorker(t, f, &calls), page("https://a.example/clean"))
	if err != nil || final.GetStats().GetSucceeded() != 1 || calls.Load() != 1 {
		t.Fatalf("after failed crawls: %v, %d handler calls, %v", final.GetStats(), calls.Load(), err)
	}

	if _, err := local.Start(local.WithPolicies([]byte("{"))); err == nil {
		t.Error("policies that do not parse must be an error")
	}
}

// TestCrawlRequest: the caller's request is not changed, so it can be used again.
func TestCrawlRequest(t *testing.T) {
	f := crawlytest.Start(t)
	var calls atomic.Int64
	req := &run.CreateRunRequest{Limits: &run.Limits{MaxTasks: 10}}
	for i := range 2 {
		final, err := f.Crawl(t.Context(), newWorker(t, f, &calls), page("https://a.example/"), local.WithRun(req))
		if err != nil || final.GetStats().GetSucceeded() != 1 || final.GetLimits().GetMaxTasks() != 10 {
			t.Fatalf("crawl %d: %v %v", i, final, err)
		}
	}
	if req.GetRunId() != "" {
		t.Errorf("the request was changed: run id %q", req.GetRunId())
	}
	// No seeds: the run completes at once.
	if final, err := f.Crawl(t.Context(), newWorker(t, f, &calls), nil); err != nil || final.GetState() != run.StateCompleted {
		t.Fatalf("no seeds: %v %v", final, err)
	}
}

// TestCrawlCut: a crawl cut by its context returns the run as it was, cancelled, and
// one cut by Close returns ErrClosed.
func TestCrawlCut(t *testing.T) {
	block := make(chan struct{})
	defer close(block)
	slow := func(f *local.Frontier) *worker.Worker {
		w, err := f.Worker(worker.WithShutdownGrace(10 * time.Millisecond))
		if err != nil {
			t.Fatal(err)
		}
		if err := w.Register("page", worker.HandlerFunc(func(ctx context.Context, _ *worker.Task) (*worker.Result, error) {
			select {
			case <-block:
			case <-ctx.Done():
			}
			return nil, ctx.Err()
		})); err != nil {
			t.Fatal(err)
		}
		return w
	}

	f := crawlytest.Start(t)
	ctx, cancel := context.WithTimeout(t.Context(), 300*time.Millisecond)
	defer cancel()
	final, err := f.Crawl(ctx, slow(f), page("https://a.example/"))
	if !errors.Is(err, context.DeadlineExceeded) || final.GetState() != run.StateCancelled {
		t.Fatalf("deadline: run %v, err %v", final.GetState(), err)
	}

	closing, err := local.Start()
	if err != nil {
		t.Fatal(err)
	}
	time.AfterFunc(300*time.Millisecond, closing.Close)
	started := time.Now()
	if _, err := closing.Crawl(t.Context(), slow(closing), page("https://a.example/")); !errors.Is(err, local.ErrClosed) {
		t.Fatalf("closed: %v", err)
	}
	if took := time.Since(started); took > 10*time.Second {
		t.Fatalf("the crawl outlived its frontier by %v", took)
	}
	closing.Close() // twice is safe
}

// TestLabels: a label follows the subtree of the task that has it, unless a task sets
// another or clears it, and the run's statistics are kept per label.
func TestLabels(t *testing.T) {
	f := crawlytest.Start(t)
	w, err := f.Worker()
	if err != nil {
		t.Fatal(err)
	}
	const base = "https://a.example"
	var seen sync.Map // URL -> label the handler saw
	err = w.Register("node", worker.HandlerFunc(func(_ context.Context, tk *worker.Task) (*worker.Result, error) {
		seen.Store(tk.URL, tk.Label)
		switch tk.URL {
		case base + "/n":
			return &worker.Result{Tasks: []worker.NewTask{
				{URL: base + "/n/1"},                   // inherits
				{URL: base + "/n/2", NoLabel: true},    // cleared
				{URL: base + "/n/3", Label: "shop"},    // replaced
				{URL: base + "/n/4", Label: "no good"}, // invalid: not an allowed label
			}}, nil
		case base + "/n/1":
			return &worker.Result{Tasks: []worker.NewTask{{URL: base + "/n/1/a"}}}, nil
		case base + "/n/1/a":
			return nil, worker.Fail("boom", errors.New("the page is broken"))
		default:
			return nil, nil
		}
	}))
	if err != nil {
		t.Fatal(err)
	}
	news := "news"
	final, err := f.Crawl(t.Context(), w, []*task.Spec{{Request: &task.Request{Url: base + "/n"}, Handler: "node", Label: &news}})
	if err != nil {
		t.Fatal(err)
	}
	want := map[string]string{base + "/n": "news", base + "/n/1": "news", base + "/n/1/a": "news", base + "/n/2": "", base + "/n/3": "shop"}
	for url, label := range want {
		if got, ok := seen.Load(url); !ok || got != label {
			t.Errorf("%s: the handler saw label %v (%v), want %q", url, got, ok, label)
		}
	}
	if _, ok := seen.Load(base + "/n/4"); ok {
		t.Error("a task with an invalid label ran")
	}
	by := final.GetStats().GetByLabel()
	if len(by) != 2 || by["news"].GetSucceeded() != 2 || by["news"].GetFailed() != 1 || by["news"].GetFailedByCode()["boom"] != 1 ||
		by["shop"].GetSucceeded() != 1 || by["shop"].GetFailed() != 0 {
		t.Fatalf("by label: %v", by)
	}
	if st := final.GetStats(); st.GetSucceeded() != 4 || st.GetFailed() != 1 {
		t.Fatalf("stats: %v", st)
	}
}

// Package e2e runs a Frontier, a worker and a test site in one process.
package e2e

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"net/http/httptest"
	"regexp"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	healthpb "google.golang.org/grpc/health/grpc_health_v1"

	"github.com/edevhub/crawly/internal/testfrontier"
	"github.com/edevhub/crawly/sdk"
	"github.com/edevhub/crawly/sdk/run"
	"github.com/edevhub/crawly/sdk/task"
	"github.com/edevhub/crawly/sdk/worker"
)

var clients = map[string]*sdk.Client{}

// startFunc starts a Frontier for a test.
type startFunc func(testing.TB) *testfrontier.Frontier

// backends runs a test on every store. The PostgreSQL one is skipped when no
// database is configured.
func backends(t *testing.T, test func(*testing.T, startFunc)) {
	t.Helper()
	t.Run("memory", func(t *testing.T) { test(t, testfrontier.Start) })
	t.Run("postgres", func(t *testing.T) { test(t, testfrontier.StartPostgres) })
}

func runClient(t *testing.T, addr string) run.Client {
	t.Helper()
	c, err := sdk.NewClient(addr, sdk.WithInsecure())
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if err := c.Close(); err != nil {
			t.Errorf("closing the client: %v", err)
		}
	})
	clients[addr] = c
	return c.Runs
}

func register(t *testing.T, w *worker.Worker, name string, h worker.Handler, opts ...worker.RegisterOption) {
	t.Helper()
	if err := w.Register(name, h, opts...); err != nil {
		t.Fatal(err)
	}
}

func seed(t *testing.T, ctx context.Context, addr, runID string, specs ...*task.Spec) {
	t.Helper()
	res, err := clients[addr].Seed(ctx, runID, specs...)
	if err != nil {
		t.Fatal(err)
	}
	for _, r := range res {
		if r.GetStatus() != task.StatusAdded {
			t.Fatalf("seed: %v", r.GetStatus())
		}
	}
}

func waitRun(t *testing.T, ctx context.Context, rc run.Client, runID string) *run.Run {
	t.Helper()
	deadline := time.Now().Add(20 * time.Second)
	for time.Now().Before(deadline) {
		res, err := rc.GetRun(ctx, &run.GetRunRequest{RunId: runID})
		if err != nil {
			t.Fatal(err)
		}
		if res.GetRun().GetState() == run.StateCompleted {
			return res.GetRun()
		}
		time.Sleep(20 * time.Millisecond)
	}
	res, _ := rc.GetRun(ctx, &run.GetRunRequest{RunId: runID})
	t.Fatalf("run did not complete: %v", res.GetRun())
	return nil
}

var link = regexp.MustCompile(`href="([^"]+)"`)

// TestCrawl follows links over a small site: discovery, dedup, a permanent failure, a
// retried failure and a panic, through to run completion.
func TestCrawl(t *testing.T) { backends(t, testCrawl) }

func testCrawl(t *testing.T, start startFunc) { //nolint:thelper // a test body, not a helper: failures must point into it
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	var flaky atomic.Int32
	mux := http.NewServeMux()
	mux.HandleFunc("/", func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/" {
			http.NotFound(w, r)
			return
		}
		_, _ = fmt.Fprint(w, `<a href="/p/1"><a href="/p/2"><a href="/p/3"><a href="/missing"><a href="/flaky"><a href="/boom">`)
	})
	mux.HandleFunc("/p/", func(w http.ResponseWriter, _ *http.Request) {
		_, _ = fmt.Fprint(w, `<a href="/p/1"><a href="/p/2"><a href="/">`) // only duplicates
	})
	mux.HandleFunc("/flaky", func(w http.ResponseWriter, _ *http.Request) {
		if flaky.Add(1) == 1 {
			http.Error(w, "try again", http.StatusServiceUnavailable)
			return
		}
		_, _ = fmt.Fprint(w, "ok")
	})
	mux.HandleFunc("/boom", func(w http.ResponseWriter, _ *http.Request) { _, _ = fmt.Fprint(w, "boom") })
	site := httptest.NewServer(mux)
	defer site.Close()

	addr := start(t).Addr
	rc := runClient(t, addr)

	var mu sync.Mutex
	seen := map[string]int{}
	spider := worker.ParserFunc(func(_ context.Context, t *worker.Task, resp *worker.Response) (*worker.Result, error) {
		mu.Lock()
		seen[t.URL]++
		mu.Unlock()
		if string(resp.Body) == "boom" {
			panic("spider bug")
		}
		res := &worker.Result{Items: 1}
		for _, m := range link.FindAllStringSubmatch(string(resp.Body), -1) {
			res.Tasks = append(res.Tasks, worker.NewTask{URL: site.URL + m[1]})
		}
		return res, nil
	})
	w, err := worker.New(addr, worker.WithInsecure(), worker.WithID("w1"), worker.WithBuildVersion("test"), worker.WithMaxConcurrentTasks(4),
		worker.WithShutdownGrace(2*time.Second))
	if err != nil {
		t.Fatal(err)
	}
	if err := w.Register("page", worker.HTTP(spider)); err != nil {
		t.Fatal(err)
	}
	done := make(chan error, 1)
	go func() { done <- w.Run(ctx) }()

	if _, err := rc.CreateRun(ctx, &run.CreateRunRequest{RunId: "crawl"}); err != nil {
		t.Fatal(err)
	}
	seed(t, ctx, addr, "crawl", &task.Spec{Request: &task.Request{Url: site.URL + "/"}, Handler: "page"})
	if _, err := rc.SealRun(ctx, &run.SealRunRequest{RunId: "crawl"}); err != nil {
		t.Fatal(err)
	}
	st := waitRun(t, ctx, rc, "crawl").GetStats()
	// 7 tasks: /, /p/1..3, /missing, /flaky, /boom. /missing fails (404); /boom panics
	// on every attempt and fails after max_attempts; /flaky succeeds on its retry.
	if st.GetSucceeded() != 5 || st.GetFailed() != 2 || st.GetDiscovered() != 6 {
		t.Fatalf("stats: %v", st)
	}
	if st.GetByCode()["http_404"] != 1 || st.GetByCode()["http_503"] != 1 || st.GetByCode()["panic"] != 3 {
		t.Fatalf("codes: %v", st.GetByCode())
	}
	if st.GetDuplicates() == 0 {
		t.Fatalf("links repeated across pages must be dropped as duplicates: %v", st)
	}
	mu.Lock()
	if seen[site.URL+"/p/1"] != 1 {
		t.Fatalf("a page was parsed %d times", seen[site.URL+"/p/1"])
	}
	mu.Unlock()

	tasks, err := rc.ListTasks(ctx, &run.ListTasksRequest{RunId: "crawl", Code: "http_404"})
	if err != nil || len(tasks.GetTasks()) != 1 || tasks.GetTasks()[0].GetAttempts()[0].GetWorkerId() != "w1" {
		t.Fatalf("ListTasks by code: %v %v", tasks, err)
	}

	cancel()
	select {
	case err := <-done:
		if err != nil {
			t.Fatalf("worker.Run: %v", err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("worker did not shut down")
	}
}

// TestHandlerStrategies covers a plain Handler, pools, a missing handler, a classified
// error with a queue block, and a lease that outlives its first duration.
func TestHandlerStrategies(t *testing.T) { backends(t, testHandlerStrategies) }

func testHandlerStrategies(t *testing.T, start startFunc) { //nolint:thelper // a test body, not a helper: failures must point into it
	ctx := t.Context()
	addr := start(t).Addr
	rc := runClient(t, addr)

	var slowRuns, blocked atomic.Int32
	w, err := worker.New(addr, worker.WithInsecure(), worker.WithMaxConcurrentTasks(2), worker.WithShutdownGrace(time.Second))
	if err != nil {
		t.Fatal(err)
	}
	// Runs longer than the 2 s lease: the SDK must keep the lease alive.
	register(t, w, "slow", worker.HandlerFunc(func(ctx context.Context, _ *worker.Task) (*worker.Result, error) {
		slowRuns.Add(1)
		select {
		case <-time.After(3 * time.Second):
			return &worker.Result{Code: "slow_ok"}, nil
		case <-ctx.Done():
			return nil, ctx.Err()
		}
	}))
	register(t, w, "render", worker.HandlerFunc(func(_ context.Context, _ *worker.Task) (*worker.Result, error) {
		return &worker.Result{Code: "rendered", Items: 2}, nil
	}), worker.InPool("browser"))
	register(t, w, "limited", worker.HandlerFunc(func(_ context.Context, _ *worker.Task) (*worker.Result, error) {
		if blocked.Add(1) == 1 {
			return nil, worker.Retry("captcha", nil, worker.After(100*time.Millisecond), worker.BlockQueue(300*time.Millisecond))
		}
		return nil, worker.Fail("gone", nil)
	}))
	workerDone := make(chan error, 1)
	go func() { workerDone <- w.Run(ctx) }()
	t.Cleanup(func() {
		if err := <-workerDone; err != nil {
			t.Errorf("worker: %v", err)
		}
	})

	_, err = rc.CreateRun(ctx, &run.CreateRunRequest{RunId: "mixed", Limits: &run.Limits{MaxDepth: 3}})
	if err != nil {
		t.Fatal(err)
	}
	// Idempotent on the same request, a conflict on a different one.
	if _, err := rc.CreateRun(ctx, &run.CreateRunRequest{RunId: "mixed", Limits: &run.Limits{MaxDepth: 3}}); err != nil {
		t.Fatal(err)
	}
	if _, err := rc.CreateRun(ctx, &run.CreateRunRequest{RunId: "mixed"}); err == nil {
		t.Fatal("a different spec for the same run must be rejected")
	}
	seed(t, ctx, addr, "mixed",
		&task.Spec{Request: &task.Request{Url: "https://a.example/slow"}, Handler: "slow"},
		&task.Spec{Request: &task.Request{Url: "https://b.example/page"}, Handler: "render", Pool: "browser"},
		&task.Spec{Request: &task.Request{Url: "https://c.example/x"}, Handler: "limited"},
		&task.Spec{Request: &task.Request{Url: "https://d.example/y"}, Handler: "unknown"},
	)
	if _, err := rc.SealRun(ctx, &run.SealRunRequest{RunId: "mixed"}); err != nil {
		t.Fatal(err)
	}
	st := waitRun(t, ctx, rc, "mixed").GetStats()
	codes := st.GetByCode()
	if st.GetSucceeded() != 2 || st.GetFailed() != 2 {
		t.Fatalf("stats: %v", st)
	}
	if codes["slow_ok"] != 1 || codes["rendered"] != 1 || codes["captcha"] != 1 || codes["gone"] != 1 || codes["no_handler"] != 1 {
		t.Fatalf("codes: %v", codes)
	}
	if codes["lease_expired"] != 0 || slowRuns.Load() != 1 {
		t.Fatalf("the long handler lost its lease: codes %v, runs %d", codes, slowRuns.Load())
	}
	if st.GetQueueBlocks() != 1 {
		t.Fatalf("queue blocks: %d", st.GetQueueBlocks())
	}
}

// TestCancel cancels a run with work in flight: the running handlers are revoked and
// every task of the run ends as cancelled, the ones that never ran included.
func TestCancel(t *testing.T) { backends(t, testCancel) }

func testCancel(t *testing.T, start startFunc) { //nolint:thelper // a test body, not a helper: failures must point into it
	ctx := t.Context()
	addr := start(t).Addr
	rc := runClient(t, addr)

	started, revoked := make(chan struct{}, 8), make(chan struct{}, 8)
	w, err := worker.New(addr, worker.WithInsecure(), worker.WithMaxConcurrentTasks(2), worker.WithShutdownGrace(time.Second))
	if err != nil {
		t.Fatal(err)
	}
	register(t, w, "hang", worker.HandlerFunc(func(ctx context.Context, _ *worker.Task) (*worker.Result, error) {
		started <- struct{}{}
		<-ctx.Done()
		revoked <- struct{}{}
		return nil, ctx.Err()
	}))
	workerDone := make(chan error, 1)
	go func() { workerDone <- w.Run(ctx) }()
	t.Cleanup(func() {
		if err := <-workerDone; err != nil {
			t.Errorf("worker: %v", err)
		}
	})

	if _, err := rc.CreateRun(ctx, &run.CreateRunRequest{RunId: "doomed"}); err != nil {
		t.Fatal(err)
	}
	seed(t, ctx, addr, "doomed",
		&task.Spec{Request: &task.Request{Url: "https://a.example/1"}, Handler: "hang"},
		&task.Spec{Request: &task.Request{Url: "https://b.example/2"}, Handler: "hang"},
		&task.Spec{Request: &task.Request{Url: "https://c.example/3"}, Handler: "hang"},
	)
	wait := func(ch <-chan struct{}, what string) {
		t.Helper()
		for range 2 {
			select {
			case <-ch:
			case <-time.After(10 * time.Second):
				t.Fatalf("two handlers were not %s", what)
			}
		}
	}
	wait(started, "started")
	res, err := rc.CancelRun(ctx, &run.CancelRunRequest{RunId: "doomed"})
	if err != nil || res.GetRun().GetState() != run.StateCancelled {
		t.Fatalf("CancelRun: %v %v", res, err)
	}
	wait(revoked, "revoked")

	var last *run.Stats
	for deadline := time.Now().Add(10 * time.Second); time.Now().Before(deadline); time.Sleep(20 * time.Millisecond) {
		got, err := rc.GetRun(ctx, &run.GetRunRequest{RunId: "doomed"})
		if err != nil {
			t.Fatal(err)
		}
		if last = got.GetRun().GetStats(); last.GetCancelled() == 3 && last.GetPending() == 0 && last.GetInFlight() == 0 {
			return
		}
	}
	t.Fatalf("the cancelled run did not drain: %v", last)
}

// TestBalance: work that trickles in is shared between idle workers, also when one of
// them has fewer free slots than the other.
func TestBalance(t *testing.T) {
	ctx := t.Context()
	addr := testfrontier.Start(t).Addr
	rc := runClient(t, addr)

	var counts [2]atomic.Int32
	hold := make(chan struct{})
	for i := range counts {
		w, err := worker.New(addr, worker.WithInsecure(), worker.WithMaxConcurrentTasks(4), worker.WithShutdownGrace(time.Second))
		if err != nil {
			t.Fatal(err)
		}
		register(t, w, "count", worker.HandlerFunc(func(context.Context, *worker.Task) (*worker.Result, error) {
			counts[i].Add(1)
			return &worker.Result{}, nil
		}))
		// A long task: whoever takes it has one free slot fewer than the other worker.
		register(t, w, "hold", worker.HandlerFunc(func(ctx context.Context, _ *worker.Task) (*worker.Result, error) {
			select {
			case <-hold:
			case <-ctx.Done():
			}
			return &worker.Result{}, nil
		}))
		go func() { _ = w.Run(ctx) }()
	}

	if _, err := rc.CreateRun(ctx, &run.CreateRunRequest{RunId: "balance"}); err != nil {
		t.Fatal(err)
	}
	seed(t, ctx, addr, "balance", &task.Spec{Request: &task.Request{Url: "https://a.example/hold"}, Handler: "hold"})
	const n = 120
	for i := range n {
		seed(t, ctx, addr, "balance", &task.Spec{Request: &task.Request{Url: fmt.Sprintf("https://a.example/%d", i)}, Handler: "count"})
		time.Sleep(3 * time.Millisecond)
	}
	close(hold)
	if _, err := rc.SealRun(ctx, &run.SealRunRequest{RunId: "balance"}); err != nil {
		t.Fatal(err)
	}
	waitRun(t, ctx, rc, "balance")
	a, b := counts[0].Load(), counts[1].Load()
	if a+b != n || min(a, b) < n*35/100 {
		t.Fatalf("the workers ran %d and %d of %d tasks", a, b, n)
	}
}

// TestHealth: the standard gRPC health service says whether the replica takes work.
func TestHealth(t *testing.T) {
	f := testfrontier.Start(t)
	conn, err := grpc.NewClient(f.Addr, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = conn.Close() }()
	check := func() healthpb.HealthCheckResponse_ServingStatus {
		res, err := healthpb.NewHealthClient(conn).Check(t.Context(), &healthpb.HealthCheckRequest{})
		if err != nil {
			t.Fatal(err)
		}
		return res.GetStatus()
	}
	deadline := time.Now().Add(5 * time.Second)
	for check() != healthpb.HealthCheckResponse_SERVING {
		if time.Now().After(deadline) {
			t.Fatalf("a running replica reports %v", check())
		}
		time.Sleep(10 * time.Millisecond)
	}
	f.Drain()
	if got := check(); got != healthpb.HealthCheckResponse_NOT_SERVING {
		t.Fatalf("a draining replica reports %v", got)
	}
}

// TestAbortOnBlock: a site that blocks the crawler ends a sealed run as aborted, with the
// rule and the task that tripped it, instead of leaving it to wait for the block to end.
// A second run that meets the block shows it in its stats, and a watcher and Wait both see
// the abort as the end of the run.
func TestAbortOnBlock(t *testing.T) { backends(t, testAbortOnBlock) }

func testAbortOnBlock(t *testing.T, start startFunc) { //nolint:thelper // a test body, not a helper: failures must point into it
	ctx := t.Context()
	addr := start(t).Addr
	rc := runClient(t, addr)

	w, err := worker.New(addr, worker.WithInsecure(), worker.WithMaxConcurrentTasks(1), worker.WithShutdownGrace(time.Second))
	if err != nil {
		t.Fatal(err)
	}
	var ran atomic.Int32
	register(t, w, "page", worker.HandlerFunc(func(context.Context, *worker.Task) (*worker.Result, error) {
		ran.Add(1)
		return nil, worker.Fail("blocked", errors.New("the site refuses the crawler"), worker.BlockQueue(time.Hour))
	}))
	workerDone := make(chan error, 1)
	go func() { workerDone <- w.Run(ctx) }()
	t.Cleanup(func() {
		if err := <-workerDone; err != nil {
			t.Errorf("worker: %v", err)
		}
	})

	// The watcher comes first: it learns of the run when it is created.
	watch, err := rc.WatchRuns(ctx, &run.WatchRunsRequest{RunId: "crawl"})
	if err != nil {
		t.Fatal(err)
	}
	events := make(chan *run.Event, 32)
	go func() {
		defer close(events)
		for {
			res, err := watch.Recv()
			if err != nil {
				return
			}
			events <- res.GetEvent()
		}
	}()

	if _, err := rc.CreateRun(ctx, &run.CreateRunRequest{RunId: "crawl", Limits: &run.Limits{AbortOnCodes: []string{"blocked"}}}); err != nil {
		t.Fatal(err)
	}
	specs := make([]*task.Spec, 0, 6)
	for i := range 6 {
		specs = append(specs, &task.Spec{Request: &task.Request{Url: fmt.Sprintf("https://blocked.example/%d", i)}, Handler: "page"})
	}
	seed(t, ctx, addr, "crawl", specs...) // one batch: the first block must not end the run before the seeds are in
	if _, err := rc.SealRun(ctx, &run.SealRunRequest{RunId: "crawl"}); err != nil {
		t.Fatal(err)
	}

	waitCtx, stop := context.WithTimeout(ctx, 20*time.Second)
	defer stop()
	final, err := clients[addr].Wait(waitCtx, "crawl")
	if err != nil {
		t.Fatal(err)
	}
	if final.GetState() != run.StateAborted {
		t.Fatalf("the run ended %v, want aborted: %v", final.GetState(), final)
	}
	why := final.GetAbortReason()
	if why.GetRule() != run.AbortOnCode || why.GetCode() != "blocked" || why.GetTaskId() == "" {
		t.Fatalf("abort reason: %v", why)
	}
	for deadline := time.Now().Add(10 * time.Second); ; time.Sleep(20 * time.Millisecond) {
		got, err := rc.GetRun(ctx, &run.GetRunRequest{RunId: "crawl"})
		if err != nil {
			t.Fatal(err)
		}
		st := got.GetRun().GetStats()
		if st.GetFailed() == 1 && st.GetCancelled() == 5 && st.GetPending() == 0 && st.GetInFlight() == 0 {
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("the aborted run did not drain: %v", st)
		}
	}
	if n := ran.Load(); n != 1 {
		t.Fatalf("%d tasks ran; the first block should have ended the run", n)
	}
	var last *run.Event
	for ev := range events {
		last = ev
		if ev.GetType() == run.EventAborted {
			break
		}
	}
	if last.GetType() != run.EventAborted || last.GetRun().GetAbortReason().GetTaskId() != why.GetTaskId() {
		t.Fatalf("the watch did not report the abort: %v", last)
	}

	// The site is still blocked: a second run on it waits, and its stats say so.
	if _, err := rc.CreateRun(ctx, &run.CreateRunRequest{RunId: "second"}); err != nil {
		t.Fatal(err)
	}
	seed(t, ctx, addr, "second",
		&task.Spec{Request: &task.Request{Url: "https://blocked.example/a"}, Handler: "page"},
		&task.Spec{Request: &task.Request{Url: "https://blocked.example/b"}, Handler: "page"})
	if _, err := rc.SealRun(ctx, &run.SealRunRequest{RunId: "second"}); err != nil {
		t.Fatal(err)
	}
	time.Sleep(300 * time.Millisecond) // long enough for a dispatch that should not happen
	got, err := rc.GetRun(ctx, &run.GetRunRequest{RunId: "second"})
	if err != nil {
		t.Fatal(err)
	}
	bq := got.GetRun().GetStats().GetBlockedQueues()
	if got.GetRun().GetState() != run.StateSealed || len(bq) != 1 || bq[0].GetQueueKey() != "blocked.example" ||
		bq[0].GetReason() != "blocked" || bq[0].GetWaiting() != 2 || bq[0].GetUnblockTime().AsTime().Before(time.Now().Add(30*time.Minute)) {
		t.Fatalf("the second run: %v", got.GetRun())
	}
	if n := ran.Load(); n != 1 {
		t.Fatalf("a task of the second run ran while the queue was blocked (%d)", n)
	}
	if _, err := rc.CancelRun(ctx, &run.CancelRunRequest{RunId: "second"}); err != nil {
		t.Fatal(err)
	}
}

// TestAbortRevokesLeases: a run ended by a rule revokes the handlers still running its
// tasks, as a cancel does.
func TestAbortRevokesLeases(t *testing.T) { backends(t, testAbortRevokesLeases) }

func testAbortRevokesLeases(t *testing.T, start startFunc) { //nolint:thelper // a test body, not a helper: failures must point into it
	ctx := t.Context()
	addr := start(t).Addr
	rc := runClient(t, addr)

	started, revoked := make(chan struct{}, 1), make(chan struct{}, 1)
	w, err := worker.New(addr, worker.WithInsecure(), worker.WithMaxConcurrentTasks(2), worker.WithShutdownGrace(time.Second))
	if err != nil {
		t.Fatal(err)
	}
	register(t, w, "hang", worker.HandlerFunc(func(ctx context.Context, _ *worker.Task) (*worker.Result, error) {
		started <- struct{}{}
		<-ctx.Done()
		revoked <- struct{}{}
		return nil, ctx.Err()
	}))
	register(t, w, "fail", worker.HandlerFunc(func(context.Context, *worker.Task) (*worker.Result, error) {
		<-started // fail while the other task is running
		return nil, worker.Fail("gone", errors.New("gone"))
	}))
	workerDone := make(chan error, 1)
	go func() { workerDone <- w.Run(ctx) }()
	t.Cleanup(func() {
		if err := <-workerDone; err != nil {
			t.Errorf("worker: %v", err)
		}
	})

	if _, err := rc.CreateRun(ctx, &run.CreateRunRequest{RunId: "r", Limits: &run.Limits{MaxFailed: 1}}); err != nil {
		t.Fatal(err)
	}
	seed(t, ctx, addr, "r",
		&task.Spec{Request: &task.Request{Url: "https://a.example/hang"}, Handler: "hang"},
		&task.Spec{Request: &task.Request{Url: "https://b.example/fail"}, Handler: "fail"})
	select {
	case <-revoked:
	case <-time.After(10 * time.Second):
		t.Fatal("the running handler was not revoked when the run was aborted")
	}
	got, err := rc.GetRun(ctx, &run.GetRunRequest{RunId: "r"})
	if err != nil || got.GetRun().GetState() != run.StateAborted || got.GetRun().GetAbortReason().GetRule() != run.AbortMaxFailed {
		t.Fatalf("%v %v", got, err)
	}
}

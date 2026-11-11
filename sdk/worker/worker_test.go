package worker_test

import (
	"context"
	"math"
	"net"
	"net/http"
	"slices"
	"strconv"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"github.com/edevhub/crawly/internal/testfrontier"
	"github.com/edevhub/crawly/sdk"
	"github.com/edevhub/crawly/sdk/run"
	"github.com/edevhub/crawly/sdk/task"
	"github.com/edevhub/crawly/sdk/worker"
)

var noop = worker.HandlerFunc(func(context.Context, *worker.Task) (*worker.Result, error) { return nil, nil })

func TestNewValidates(t *testing.T) {
	if _, err := worker.New(""); err == nil {
		t.Error("an empty target must be rejected")
	}
	if _, err := worker.New("localhost:1", worker.WithInsecure(), worker.WithMaxConcurrentTasks(0)); err == nil {
		t.Error("zero concurrent tasks must be rejected")
	}
	if _, err := worker.New("localhost:1"); err == nil {
		t.Error("transport security must be chosen explicitly")
	}
	for _, rate := range []float64{-1, math.NaN(), math.Inf(1)} {
		if _, err := worker.New("localhost:1", worker.WithInsecure(), worker.WithMaxTasksPerSecond(rate)); err == nil {
			t.Errorf("a task rate of %v must be rejected", rate)
		}
	}
	if _, err := worker.New("localhost:1", worker.WithInsecure(), worker.WithMaxTasksPerSecond(0.5)); err != nil {
		t.Errorf("a task rate below one a second must be accepted: %v", err)
	}
	if _, err := worker.New("localhost:1", worker.WithInsecure()); err != nil {
		t.Errorf("defaults must be valid: %v", err)
	}
}

func TestRegisterValidates(t *testing.T) {
	w := newWorker(t, "localhost:1")
	if err := w.Register("", noop); err == nil {
		t.Error("a handler needs a name")
	}
	if err := w.Register("page", nil); err == nil {
		t.Error("a handler needs an implementation")
	}
	for _, pool := range []string{"Browser", "has space", "a-b", "this_pool_name_is_longer_than_32_chars"} {
		if err := w.Register("p", noop, worker.InPool(pool)); err == nil {
			t.Errorf("pool %q must be rejected", pool)
		}
	}
	if err := w.Register("page", noop); err != nil {
		t.Fatal(err)
	}
	if err := w.Register("page", noop); err == nil {
		t.Error("a duplicate name must be rejected")
	}
	if err := w.Register("render", noop, worker.InPool("browser_2")); err != nil {
		t.Errorf("a valid pool was rejected: %v", err)
	}
}

func TestRunValidates(t *testing.T) {
	w := newWorker(t, "localhost:1")
	if err := w.Run(context.Background()); err == nil {
		t.Fatal("Run without handlers must fail")
	}

	w = newWorker(t, "localhost:1", worker.WithShutdownGrace(10*time.Millisecond))
	register(t, w, "page", noop)
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- w.Run(ctx) }()
	// Run has started once it refuses a registration.
	deadline := time.Now().Add(5 * time.Second)
	for i := 0; w.Register("late"+strconv.Itoa(i), noop) == nil; i++ {
		if time.Now().After(deadline) {
			t.Fatal("Register after Run must fail")
		}
		time.Sleep(time.Millisecond)
	}
	if err := w.Run(ctx); err == nil {
		t.Error("a second Run must fail")
	}
	// Nothing listens on the target: Run keeps retrying and still stops on cancel.
	cancel()
	select {
	case err := <-done:
		if err != nil {
			t.Fatalf("Run: %v", err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("Run did not stop without a Frontier")
	}
}

// env is a Frontier, a client and a run for one test.
type env struct {
	t    *testing.T
	f    *testfrontier.Frontier
	addr string
	c    *sdk.Client
}

func newEnv(t *testing.T) *env {
	t.Helper()
	f := testfrontier.Start(t)
	e := dial(t, f.Addr)
	e.f = f
	return e
}

func dial(t *testing.T, addr string) *env {
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
	return &env{t: t, addr: addr, c: c}
}

// start runs a worker until the test ends.
func (e *env) start(w *worker.Worker) (stop func() error) {
	e.t.Helper()
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- w.Run(ctx) }()
	var once sync.Once
	var err error
	stop = func() error {
		once.Do(func() {
			cancel()
			select {
			case err = <-done:
			case <-time.After(10 * time.Second):
				e.t.Error("worker did not shut down")
			}
		})
		return err
	}
	e.t.Cleanup(func() {
		if err := stop(); err != nil {
			e.t.Errorf("worker: %v", err)
		}
	})
	return stop
}

// crawl creates a run from seeds, seals it and waits for it to complete.
func (e *env) crawl(id string, limits *run.Limits, seeds ...*task.Spec) *run.Stats {
	e.t.Helper()
	e.open(id, limits, seeds...)
	return e.wait(id)
}

func (e *env) open(id string, limits *run.Limits, seeds ...*task.Spec) {
	e.t.Helper()
	ctx := context.Background()
	if _, err := e.c.Runs.CreateRun(ctx, &run.CreateRunRequest{RunId: id, Limits: limits}); err != nil {
		e.t.Fatal(err)
	}
	res, err := e.c.Seed(ctx, id, seeds...)
	if err != nil {
		e.t.Fatal(err)
	}
	for _, r := range res {
		if r.GetStatus() != task.StatusAdded {
			e.t.Fatalf("seed: %v", r.GetStatus())
		}
	}
	if _, err := e.c.Runs.SealRun(ctx, &run.SealRunRequest{RunId: id}); err != nil {
		e.t.Fatal(err)
	}
}

func (e *env) wait(id string) *run.Stats {
	e.t.Helper()
	var last *run.Run
	for deadline := time.Now().Add(20 * time.Second); time.Now().Before(deadline); time.Sleep(20 * time.Millisecond) {
		res, err := e.c.Runs.GetRun(context.Background(), &run.GetRunRequest{RunId: id})
		if status.Code(err) == codes.Unavailable {
			continue // the Frontier was restarted and the connection has not noticed yet
		}
		if err != nil {
			e.t.Fatal(err)
		}
		if last = res.GetRun(); last.GetState() == run.StateCompleted {
			return last.GetStats()
		}
	}
	e.t.Fatalf("run %s did not complete: %v", id, last)
	return nil
}

func newWorker(t *testing.T, target string, opts ...worker.Option) *worker.Worker {
	t.Helper()
	w, err := worker.New(target, append([]worker.Option{worker.WithInsecure()}, opts...)...)
	if err != nil {
		t.Fatal(err)
	}
	return w
}

func register(t *testing.T, w *worker.Worker, name string, h worker.Handler, opts ...worker.RegisterOption) {
	t.Helper()
	if err := w.Register(name, h, opts...); err != nil {
		t.Fatal(err)
	}
}

func spec(url, handler string) *task.Spec {
	return &task.Spec{Request: &task.Request{Url: url}, Handler: handler}
}

// TestTaskFields checks what a handler is given, and that discovered tasks carry
// their parent, depth, context and routing.
func TestTaskFields(t *testing.T) {
	e := newEnv(t)
	var mu sync.Mutex
	got := map[string]worker.Task{}
	record := func(tk *worker.Task) {
		mu.Lock()
		got[tk.Handler] = *tk
		mu.Unlock()
	}
	w, err := worker.New(e.addr, worker.WithInsecure(), worker.WithID("w-fields"))
	if err != nil {
		t.Fatal(err)
	}
	register(t, w, "list", worker.HandlerFunc(func(_ context.Context, tk *worker.Task) (*worker.Result, error) {
		record(tk)
		return &worker.Result{Items: 1, Tasks: []worker.NewTask{
			{URL: "https://a.example/item/1", Handler: "item", Context: map[string][]byte{"category": []byte("books")}},
			{URL: "https://a.example/render/1", Handler: "render"},
			{URL: "https://a.example/list?page=2"}, // no handler: the one that found it
		}}, nil
	}))
	register(t, w, "item", worker.HandlerFunc(func(_ context.Context, tk *worker.Task) (*worker.Result, error) {
		record(tk)
		return &worker.Result{Items: 5}, nil
	}))
	register(t, w, "render", worker.HandlerFunc(func(_ context.Context, tk *worker.Task) (*worker.Result, error) {
		record(tk)
		return nil, nil // a nil result is a plain success
	}), worker.InPool("browser"))
	e.start(w)

	// max_depth 1 stops the second page from discovering a third generation.
	st := e.crawl("fields", &run.Limits{MaxDepth: 1}, spec("https://a.example/list", "list"))
	if st.GetSucceeded() != 4 || st.GetFailed() != 0 || st.GetDiscovered() != 3 {
		t.Fatalf("stats: %v", st)
	}
	mu.Lock()
	defer mu.Unlock()
	item, render := got["item"], got["render"]
	if item.URL != "https://a.example/item/1" || item.Method != http.MethodGet || item.RunID != "fields" ||
		item.QueueKey != "a.example" || item.Depth != 1 || item.Attempt != 1 || item.ID == "" || item.ParentID == "" {
		t.Fatalf("item task: %+v", item)
	}
	if string(item.Context["category"]) != "books" {
		t.Fatalf("context was not carried: %v", item.Context)
	}
	if item.FetchTimeout <= 0 {
		t.Fatalf("the policy's fetch timeout must reach the handler: %v", item.FetchTimeout)
	}
	if render.URL != "https://a.example/render/1" {
		t.Fatalf("a task for a handler in another pool was not routed there: %+v", render)
	}
}

// TestHandlerTimeout: WithTimeout bounds a handler, and a handler that runs out of
// time is a retryable failure.
func TestHandlerTimeout(t *testing.T) {
	e := newEnv(t)
	var calls atomic.Int32
	w := newWorker(t, e.addr)
	register(t, w, "stuck", worker.HandlerFunc(func(ctx context.Context, _ *worker.Task) (*worker.Result, error) {
		calls.Add(1)
		<-ctx.Done()
		return nil, ctx.Err()
	}), worker.WithTimeout(50*time.Millisecond))
	e.start(w)

	st := e.crawl("timeout", nil, spec("https://a.example/", "stuck"))
	if st.GetFailed() != 1 || st.GetByCode()["timeout"] != 3 || calls.Load() != 3 {
		t.Fatalf("stats: %v, calls %d", st, calls.Load())
	}
}

// TestUnclassifiedError: a plain error is retried under the code "error".
func TestUnclassifiedError(t *testing.T) {
	e := newEnv(t)
	var calls atomic.Int32
	w := newWorker(t, e.addr)
	register(t, w, "flaky", worker.HandlerFunc(func(context.Context, *worker.Task) (*worker.Result, error) {
		if calls.Add(1) == 1 {
			return nil, context.Canceled // not the worker's context: an ordinary error
		}
		return &worker.Result{Code: "ok"}, nil
	}))
	e.start(w)

	st := e.crawl("plain", nil, spec("https://a.example/", "flaky"))
	if st.GetSucceeded() != 1 || st.GetByCode()["error"] != 1 || st.GetByCode()["ok"] != 1 {
		t.Fatalf("stats: %v", st)
	}
}

// TestConcurrencyLimit: a worker never runs more handlers than it was configured for.
func TestConcurrencyLimit(t *testing.T) {
	e := newEnv(t)
	var running, peak atomic.Int32
	w := newWorker(t, e.addr, worker.WithMaxConcurrentTasks(3))
	register(t, w, "page", worker.HandlerFunc(func(context.Context, *worker.Task) (*worker.Result, error) {
		n := running.Add(1)
		for {
			p := peak.Load()
			if n <= p || peak.CompareAndSwap(p, n) {
				break
			}
		}
		time.Sleep(30 * time.Millisecond)
		running.Add(-1)
		return nil, nil
	}))
	e.start(w)

	seeds := make([]*task.Spec, 0, 24)
	for _, host := range []string{"a", "b", "c", "d", "e", "f"} {
		for _, path := range []string{"1", "2", "3", "4"} {
			seeds = append(seeds, spec("https://"+host+".example/"+path, "page"))
		}
	}
	st := e.crawl("limit", nil, seeds...)
	if st.GetSucceeded() != uint64(len(seeds)) {
		t.Fatalf("stats: %v", st)
	}
	if p := peak.Load(); p > 3 {
		t.Fatalf("peak concurrency %d, want at most 3", p)
	}
}

// TestMaxTasksPerSecond: the worker starts tasks no faster than its rate, over all its
// pools together, however many slots are free.
func TestMaxTasksPerSecond(t *testing.T) {
	e := newEnv(t)
	const rate, perPool = 20, 8
	var mu sync.Mutex
	var starts []time.Time
	handler := worker.HandlerFunc(func(context.Context, *worker.Task) (*worker.Result, error) {
		mu.Lock()
		starts = append(starts, time.Now())
		mu.Unlock()
		return nil, nil
	})
	w := newWorker(t, e.addr, worker.WithMaxConcurrentTasks(8), worker.WithMaxTasksPerSecond(rate))
	register(t, w, "page", handler)
	register(t, w, "render", handler, worker.InPool("browser"))
	e.start(w)

	seeds := make([]*task.Spec, 0, 2*perPool)
	for i := range perPool {
		host := "https://h" + strconv.Itoa(i) + ".example/"
		render := spec(host+"render", "render")
		render.Pool = "browser"
		seeds = append(seeds, spec(host+"page", "page"), render)
	}
	st := e.crawl("paced", nil, seeds...)
	if st.GetSucceeded() != uint64(len(seeds)) {
		t.Fatalf("stats: %v", st)
	}
	mu.Lock()
	defer mu.Unlock()
	slices.SortFunc(starts, time.Time.Compare)
	// 16 tasks at 20 a second are 15 intervals of 50 ms. Unpaced, they take a few
	// milliseconds.
	span, want := starts[len(starts)-1].Sub(starts[0]), time.Duration(len(seeds)-1)*time.Second/rate
	if span < want*9/10 {
		t.Fatalf("%d tasks started within %v; at %d a second they take at least %v", len(seeds), span, rate, want)
	}
	// No window of half a second holds more than half a second's worth, give or take
	// the timing of the handlers.
	for i := range starts {
		n := 0
		for _, at := range starts[i:] {
			if at.Sub(starts[i]) < 500*time.Millisecond {
				n++
			}
		}
		if n > rate/2+2 {
			t.Fatalf("%d tasks started within half a second at %d a second", n, rate)
		}
	}
}

// TestGracefulShutdown: a handler that is running when the worker stops is given the
// grace period to finish, and its result is reported.
func TestGracefulShutdown(t *testing.T) {
	e := newEnv(t)
	started := make(chan struct{})
	w := newWorker(t, e.addr, worker.WithShutdownGrace(5*time.Second))
	register(t, w, "slow", worker.HandlerFunc(func(context.Context, *worker.Task) (*worker.Result, error) {
		close(started)
		time.Sleep(300 * time.Millisecond)
		return &worker.Result{Code: "finished"}, nil
	}))
	stop := e.start(w)

	e.open("drain", nil, spec("https://a.example/", "slow"))
	<-started
	if err := stop(); err != nil {
		t.Fatalf("Run: %v", err)
	}
	// No worker is left: the run completes only if the result was reported.
	if st := e.wait("drain"); st.GetByCode()["finished"] != 1 || st.GetSucceeded() != 1 {
		t.Fatalf("stats: %v", st)
	}
}

// TestStartsBeforeFrontier: a worker keeps trying until the Frontier is there.
func TestStartsBeforeFrontier(t *testing.T) {
	lis, err := (&net.ListenConfig{}).Listen(t.Context(), "tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	addr := lis.Addr().String()
	if err := lis.Close(); err != nil {
		t.Fatal(err)
	}

	w := newWorker(t, addr)
	register(t, w, "page", noop)
	e := &env{t: t, addr: addr}
	e.start(w)
	time.Sleep(300 * time.Millisecond)

	e = dial(t, testfrontier.StartAt(t, addr).Addr)
	if st := e.crawl("late", nil, spec("https://a.example/", "page")); st.GetSucceeded() != 1 {
		t.Fatalf("stats: %v", st)
	}
}

// TestReconnect: the Frontier is replaced while a handler runs. The handler is not
// disturbed, its lease is kept alive across the reconnect, and its result and
// discovered tasks are reported to the new replica.
func TestReconnect(t *testing.T) {
	e := newEnv(t)
	started, release := make(chan struct{}), make(chan struct{})
	var runs atomic.Int32
	w := newWorker(t, e.addr, worker.WithMaxConcurrentTasks(2))
	register(t, w, "slow", worker.HandlerFunc(func(ctx context.Context, _ *worker.Task) (*worker.Result, error) {
		if runs.Add(1) == 1 {
			close(started)
		}
		select {
		case <-release:
			return &worker.Result{Code: "survived", Tasks: []worker.NewTask{{URL: "https://a.example/found", Handler: "page"}}}, nil
		case <-ctx.Done():
			return nil, ctx.Err()
		}
	}))
	register(t, w, "page", noop)
	e.start(w)

	e.open("reconnect", nil, spec("https://a.example/", "slow"))
	<-started
	e.f.Stop()
	e.f.Restart()
	// Longer than the 2 s lease: only heartbeats on the new session keep it.
	time.Sleep(3 * time.Second)
	close(release)

	st := e.wait("reconnect")
	if st.GetSucceeded() != 2 || st.GetByCode()["survived"] != 1 || st.GetDiscovered() != 1 {
		t.Fatalf("stats: %v", st)
	}
	if st.GetByCode()["lease_expired"] != 0 || runs.Load() != 1 {
		t.Fatalf("the task was run %d times, codes %v", runs.Load(), st.GetByCode())
	}
}

// TestResultSurvivesOutage: a handler finishes while no Frontier is reachable. Its
// result is kept and delivered when one is back.
func TestResultSurvivesOutage(t *testing.T) {
	e := newEnv(t)
	started, release, finished := make(chan struct{}), make(chan struct{}), make(chan struct{})
	w := newWorker(t, e.addr)
	register(t, w, "page", worker.HandlerFunc(func(context.Context, *worker.Task) (*worker.Result, error) {
		close(started)
		<-release
		defer close(finished)
		return &worker.Result{Code: "kept"}, nil
	}))
	e.start(w)

	e.open("outage", nil, spec("https://a.example/", "page"))
	<-started
	e.f.Stop()
	close(release)
	<-finished
	e.f.Restart()

	if st := e.wait("outage"); st.GetByCode()["kept"] != 1 || st.GetSucceeded() != 1 {
		t.Fatalf("stats: %v", st)
	}
}

// TestLeaseRunsOut: with no Frontier to extend its lease, a handler is cancelled when
// the lease runs out, because another worker may be given the task.
func TestLeaseRunsOut(t *testing.T) {
	e := newEnv(t)
	started, cancelled := make(chan struct{}), make(chan struct{})
	var first, last sync.Once
	w := newWorker(t, e.addr, worker.WithShutdownGrace(time.Second))
	register(t, w, "page", worker.HandlerFunc(func(ctx context.Context, _ *worker.Task) (*worker.Result, error) {
		first.Do(func() { close(started) })
		<-ctx.Done()
		last.Do(func() { close(cancelled) })
		return nil, ctx.Err()
	}))
	e.start(w)

	e.open("lost", nil, spec("https://a.example/", "page"))
	<-started
	e.f.Stop()
	select {
	case <-cancelled:
	case <-time.After(10 * time.Second): // the lease is 2 s
		t.Fatal("the handler kept running after its lease ran out")
	}
}

// TestCancelRunRevokes: cancelling a run cancels its running handlers.
func TestCancelRunRevokes(t *testing.T) {
	e := newEnv(t)
	started, cancelled := make(chan struct{}), make(chan struct{})
	w := newWorker(t, e.addr)
	register(t, w, "page", worker.HandlerFunc(func(ctx context.Context, _ *worker.Task) (*worker.Result, error) {
		close(started)
		<-ctx.Done()
		close(cancelled)
		return nil, ctx.Err()
	}))
	e.start(w)

	e.open("cancelled", nil, spec("https://a.example/", "page"))
	<-started
	if _, err := e.c.Runs.CancelRun(context.Background(), &run.CancelRunRequest{RunId: "cancelled"}); err != nil {
		t.Fatal(err)
	}
	select {
	case <-cancelled:
	case <-time.After(900 * time.Millisecond): // well before the first heartbeat at 1 s
		t.Fatal("the handler was not cancelled by the revocation")
	}
}

// TestFrontierShutdown: a Frontier with connected workers shuts down promptly, and the
// workers carry on with the next replica.
func TestFrontierShutdown(t *testing.T) {
	e := newEnv(t)
	w := newWorker(t, e.addr)
	register(t, w, "page", noop)
	e.start(w)
	if st := e.crawl("before", nil, spec("https://a.example/1", "page")); st.GetSucceeded() != 1 {
		t.Fatalf("stats: %v", st)
	}

	began := time.Now()
	if !e.f.Shutdown(10 * time.Second) {
		t.Fatal("the shutdown had to be forced: a connected worker did not let go")
	}
	if d := time.Since(began); d > 5*time.Second {
		t.Fatalf("the shutdown took %v", d)
	}

	e.f.Restart()
	if st := e.crawl("after", nil, spec("https://a.example/2", "page")); st.GetSucceeded() != 1 {
		t.Fatalf("stats: %v", st)
	}
}

// TestRollingDeploy: a worker without a task's handler hands the task back, and a
// worker of the same pool that has the handler runs it. No task fails and no attempt
// is spent on the way.
func TestRollingDeploy(t *testing.T) {
	e := newEnv(t)
	e.f.Stop()
	e.f.NoHandlerGrace = time.Minute
	e.f.Restart()
	old := newWorker(t, e.addr, worker.WithID("old"))
	register(t, old, "listing", noop)
	e.start(old)
	current := newWorker(t, e.addr, worker.WithID("current"))
	var products atomic.Int64
	register(t, current, "listing", noop)
	register(t, current, "product", worker.HandlerFunc(func(context.Context, *worker.Task) (*worker.Result, error) {
		products.Add(1)
		return nil, nil
	}))
	e.start(current)

	const n = 40
	seeds := make([]*task.Spec, n)
	for i := range seeds {
		seeds[i] = spec("https://a.example/product/"+strconv.Itoa(i), "product")
	}
	st := e.crawl("rolling", nil, seeds...)
	if st.GetSucceeded() != n || st.GetFailed() != 0 || st.GetAttempts() != n || products.Load() != n {
		t.Fatalf("stats: %v, %d products handled", st, products.Load())
	}
}

// TestNoHandler: a task whose handler no worker has fails once it waited long enough.
func TestNoHandler(t *testing.T) {
	e := newEnv(t)
	w := newWorker(t, e.addr)
	register(t, w, "page", noop)
	e.start(w)
	st := e.crawl("unknown", nil, spec("https://a.example/", "nobody"))
	if st.GetFailed() != 1 || st.GetByCode()["no_handler"] != 1 || st.GetAttempts() != 1 {
		t.Fatalf("stats: %v", st)
	}
}

// TestShutdownHandsBack: a handler still running when the shutdown grace passes is
// cancelled and its task handed back: the next worker runs it as its first attempt.
func TestShutdownHandsBack(t *testing.T) {
	e := newEnv(t)
	started := make(chan struct{})
	first := newWorker(t, e.addr, worker.WithShutdownGrace(50*time.Millisecond))
	register(t, first, "slow", worker.HandlerFunc(func(ctx context.Context, _ *worker.Task) (*worker.Result, error) {
		close(started)
		<-ctx.Done()
		return nil, ctx.Err()
	}))
	stop := e.start(first)
	e.open("cut", nil, spec("https://a.example/", "slow"))
	<-started
	if err := stop(); err != nil {
		t.Fatal(err)
	}

	var attempt atomic.Uint32
	second := newWorker(t, e.addr)
	register(t, second, "slow", worker.HandlerFunc(func(_ context.Context, tk *worker.Task) (*worker.Result, error) {
		attempt.Store(tk.Attempt)
		return nil, nil
	}))
	e.start(second)
	st := e.wait("cut")
	if st.GetSucceeded() != 1 || st.GetAttempts() != 1 || attempt.Load() != 1 || len(st.GetByCode()) != 0 {
		t.Fatalf("the cut task must be run again as attempt 1: %v, attempt %d", st, attempt.Load())
	}
}

// Package local runs a Frontier inside the calling process, on the in-memory store. It
// is the Frontier the crawly-frontier command runs, with the same scheduling and the
// same politeness, without a second process and without a database:
//
//   - a dry run: crawl a site once with a spider and look at what it emits;
//   - tests of a spider, end to end (see package crawlytest).
//
// A dry run:
//
//	f, _ := local.Start()
//	defer f.Close()
//	w, _ := f.Worker()
//	w.Register("page", worker.HTTP(parser))
//	seeds := []*task.Spec{{Request: &task.Request{Url: "https://example.com/"}, Handler: "page"}}
//	final, err := f.Crawl(ctx, w, seeds, local.WithRun(&run.CreateRunRequest{Limits: &run.Limits{MaxTasks: 50}}))
//
// The state lives in memory and is gone when the process ends. For anything that must
// survive a restart, run crawly-frontier with PostgreSQL.
package local

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"net"
	"slices"
	"sync"
	"sync/atomic"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/protobuf/proto"

	"github.com/edevhub/crawly/internal/frontier"
	"github.com/edevhub/crawly/internal/policy"
	"github.com/edevhub/crawly/internal/store/memstore"
	"github.com/edevhub/crawly/sdk"
	"github.com/edevhub/crawly/sdk/run"
	"github.com/edevhub/crawly/sdk/task"
	"github.com/edevhub/crawly/sdk/worker"
)

// Option configures a local Frontier.
type Option func(*config)

type config struct {
	policies []byte
	log      *slog.Logger
}

// WithPolicies sets the policies, in the format of the Frontier's policy file:
// {"policies": [...]} in proto JSON. Default: the Frontier's built-in defaults.
func WithPolicies(file []byte) Option { return func(c *config) { c.policies = file } }

// WithLogger sets the logger of the Frontier and of the workers it creates. Default:
// they log nothing.
func WithLogger(l *slog.Logger) Option { return func(c *config) { c.log = l } }

// ErrClosed is returned by a Crawl whose Frontier was closed under it.
var ErrClosed = errors.New("local: the frontier was closed")

// Frontier is a Frontier running in this process.
type Frontier struct {
	addr    string
	log     *slog.Logger
	grpc    *grpc.Server
	closed  chan struct{} // closed by Close
	cancel  context.CancelFunc
	stopped sync.WaitGroup
	once    sync.Once
	runs    atomic.Uint64 // numbers the runs Crawl names itself
}

// Start runs a Frontier on a free local port until Close.
func Start(opts ...Option) (*Frontier, error) {
	cfg := config{log: slog.New(slog.DiscardHandler)}
	for _, o := range opts {
		o(&cfg)
	}
	pol, err := policy.New(nil)
	if cfg.policies != nil {
		pol, err = policy.Parse(cfg.policies)
	}
	if err != nil {
		return nil, fmt.Errorf("local: policies: %w", err)
	}
	srv, err := frontier.New(frontier.Config{Store: memstore.New(pol), Policies: pol, Logger: cfg.log,
		ReaperInterval: 100 * time.Millisecond, MaintainInterval: 100 * time.Millisecond, PollInterval: 100 * time.Millisecond})
	if err != nil {
		return nil, err
	}
	lis, err := (&net.ListenConfig{}).Listen(context.Background(), "tcp", "127.0.0.1:0")
	if err != nil {
		return nil, err
	}
	ctx, cancel := context.WithCancel(context.Background())
	f := &Frontier{addr: lis.Addr().String(), log: cfg.log, grpc: grpc.NewServer(frontier.ServerOptions()...), closed: make(chan struct{}), cancel: cancel}
	srv.Register(f.grpc)
	f.stopped.Go(func() {
		if err := srv.Run(ctx); err != nil {
			cfg.log.Error("local frontier: background loops", "err", err)
		}
	})
	f.stopped.Go(func() {
		if err := f.grpc.Serve(lis); err != nil && !errors.Is(err, grpc.ErrServerStopped) {
			cfg.log.Error("local frontier: serving", "err", err)
		}
	})
	return f, nil
}

// Addr is the address the Frontier listens on, on the loopback interface, without TLS.
func (f *Frontier) Addr() string { return f.addr }

// Close stops the Frontier and drops its state. A Crawl that is running returns
// ErrClosed. It is safe to call twice.
func (f *Frontier) Close() {
	f.once.Do(func() {
		close(f.closed)
		f.cancel()
		f.grpc.Stop()
		f.stopped.Wait()
	})
}

// Client connects a scheduler client to the Frontier.
func (f *Frontier) Client() (*sdk.Client, error) { return sdk.NewClient(f.addr, sdk.WithInsecure()) }

// Worker creates a worker for the Frontier, with the Frontier's logger. Register
// handlers on it and pass it to Crawl, or run it yourself.
func (f *Frontier) Worker(opts ...worker.Option) (*worker.Worker, error) {
	return worker.New(f.addr, append([]worker.Option{worker.WithInsecure(), worker.WithLogger(f.log)}, opts...)...)
}

// CrawlOption configures one Crawl.
type CrawlOption func(*crawl)

type crawl struct {
	req  *run.CreateRunRequest
	wait []sdk.WaitOption
}

// WithRun gives the run its id, limits and policy. The request is not changed; an
// empty run id gets one. Default: a run without limits, under the global policies.
func WithRun(req *run.CreateRunRequest) CrawlOption { return func(c *crawl) { c.req = req } }

// WithWait passes options to the wait for the run's end, such as sdk.OnEvent and
// sdk.StatsEvery to report progress.
func WithWait(opts ...sdk.WaitOption) CrawlOption {
	return func(c *crawl) { c.wait = append(c.wait, opts...) }
}

// cleanupTimeout bounds what Crawl does after its context ended.
const cleanupTimeout = 5 * time.Second

// Crawl runs one crawl to its end: it creates the run, adds the seeds, seals the run,
// runs the worker until the run has ended, and returns the run with its statistics.
// The handlers are the worker's, called as they are against any Frontier.
//
// When Crawl fails after it created the run, it cancels the run, so that nothing of
// it is left for a later Crawl, and returns the run as it was last seen together with
// the error: a dry run cut by its deadline still has its statistics.
//
// A seed the Frontier does not add is an error, and so is a seed for a pool the
// worker does not serve, which no one would ever run. A handler can still add tasks
// nobody serves, to another pool or for a long-blocked site, and the run then does not
// end by itself: give ctx a deadline, or the run a deadline_time.
//
// The worker is stopped when Crawl returns, so a worker serves one Crawl. Workers are
// given work by pool, whatever the run: Crawls that run at the same time on one
// Frontier in the same pool run each other's tasks.
func (f *Frontier) Crawl(ctx context.Context, w *worker.Worker, seeds []*task.Spec, opts ...CrawlOption) (*run.Run, error) {
	var cfg crawl
	for _, o := range opts {
		o(&cfg)
	}
	req := &run.CreateRunRequest{}
	if cfg.req != nil {
		req = proto.CloneOf(cfg.req)
	}
	if req.GetRunId() == "" {
		req.RunId = fmt.Sprintf("local/%d-%d", time.Now().UnixNano(), f.runs.Add(1))
	}
	// What can be known to fail is found before anything is created.
	pools := w.Pools()
	if len(pools) == 0 {
		return nil, errors.New("local: the worker has no handlers registered")
	}
	for i, s := range seeds {
		pool := s.GetPool()
		if pool == "" {
			pool = worker.DefaultPool
		}
		if !slices.Contains(pools, pool) {
			return nil, fmt.Errorf("local: seed %d (%s) is for pool %q, which the worker does not serve (%v)",
				i, s.GetRequest().GetUrl(), pool, pools)
		}
	}
	// The crawl ends with the caller's context or with the Frontier.
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()
	go func() {
		select {
		case <-f.closed:
			cancel()
		case <-ctx.Done():
		}
	}()

	c, err := f.Client()
	if err != nil {
		return nil, err
	}
	defer func() { _ = c.Close() }() // the crawl is over; a close error changes nothing
	if _, err := c.Runs.CreateRun(ctx, req); err != nil {
		return nil, f.closedOr(err)
	}
	final, err := f.crawl(ctx, c, w, req.GetRunId(), seeds, cfg.wait)
	if err == nil {
		return final, nil
	}
	// Leave nothing behind: a run with tasks would be served by the next worker.
	cleanup, done := context.WithTimeout(context.WithoutCancel(ctx), cleanupTimeout)
	defer done()
	if res, cerr := c.Runs.CancelRun(cleanup, &run.CancelRunRequest{RunId: req.GetRunId()}); cerr == nil {
		final = res.GetRun()
	}
	return final, f.closedOr(err)
}

// closedOr is ErrClosed when the Frontier was closed, which is then why err happened.
func (f *Frontier) closedOr(err error) error {
	select {
	case <-f.closed:
		return ErrClosed
	default:
		return err
	}
}

// crawl seeds and seals the run and runs the worker until the run has ended.
func (f *Frontier) crawl(ctx context.Context, c *sdk.Client, w *worker.Worker, id string, seeds []*task.Spec, wait []sdk.WaitOption) (*run.Run, error) {
	res, err := c.Seed(ctx, id, seeds...)
	if err != nil {
		return nil, err
	}
	for i, r := range res {
		if r.GetStatus() != task.StatusAdded {
			return nil, fmt.Errorf("local: seed %d (%s) was not added: %s", i, seeds[i].GetRequest().GetUrl(), r.GetStatus())
		}
	}
	if _, err := c.Runs.SealRun(ctx, &run.SealRunRequest{RunId: id}); err != nil {
		return nil, err
	}
	wctx, stop := context.WithCancel(ctx)
	defer stop()
	done := make(chan error, 1)
	go func() {
		done <- w.Run(wctx)
		stop() // a worker that could not run ends the wait below
	}()
	final, err := c.Wait(wctx, id, wait...)
	stop()
	if werr := <-done; werr != nil {
		return nil, fmt.Errorf("local: worker: %w", werr)
	}
	return final, err
}

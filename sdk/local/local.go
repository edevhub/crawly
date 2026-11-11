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
//	final, err := f.Crawl(ctx, w, &run.CreateRunRequest{Limits: &run.Limits{MaxTasks: 50}},
//		&task.Spec{Request: &task.Request{Url: "https://example.com/"}, Handler: "page"})
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
	"sync"
	"time"

	"google.golang.org/grpc"

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

// WithLogger sets the Frontier's logger. Default: it logs nothing.
func WithLogger(l *slog.Logger) Option { return func(c *config) { c.log = l } }

// Frontier is a Frontier running in this process.
type Frontier struct {
	// Addr is the address it listens on, on the loopback interface, without TLS.
	Addr string

	grpc    *grpc.Server
	cancel  context.CancelFunc
	stopped sync.WaitGroup
	once    sync.Once
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
		ReaperInterval: 100 * time.Millisecond, MaintainInterval: 100 * time.Millisecond})
	if err != nil {
		return nil, err
	}
	lis, err := (&net.ListenConfig{}).Listen(context.Background(), "tcp", "127.0.0.1:0")
	if err != nil {
		return nil, err
	}
	ctx, cancel := context.WithCancel(context.Background())
	f := &Frontier{Addr: lis.Addr().String(), grpc: grpc.NewServer(frontier.ServerOptions()...), cancel: cancel}
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

// Close stops the Frontier and drops its state. It is safe to call twice.
func (f *Frontier) Close() {
	f.once.Do(func() {
		f.grpc.Stop()
		f.cancel()
		f.stopped.Wait()
	})
}

// Client connects a scheduler client to the Frontier.
func (f *Frontier) Client() (*sdk.Client, error) { return sdk.NewClient(f.Addr, sdk.WithInsecure()) }

// Worker creates a worker for the Frontier. Register handlers on it and pass it to
// Crawl, or run it yourself.
func (f *Frontier) Worker(opts ...worker.Option) (*worker.Worker, error) {
	return worker.New(f.Addr, append([]worker.Option{worker.WithInsecure()}, opts...)...)
}

// Crawl runs one crawl to its end: it creates the run, adds the seeds, seals the run,
// runs the worker until the run has ended, and returns the run with its statistics.
// The handlers are the worker's, called as they are against any Frontier.
//
// req may be nil. It gives the run its limits and policy; an empty run id gets one. A
// seed the Frontier does not add is an error. The worker is stopped when Crawl
// returns, so a worker serves one Crawl.
func (f *Frontier) Crawl(ctx context.Context, w *worker.Worker, req *run.CreateRunRequest, seeds ...*task.Spec) (*run.Run, error) {
	c, err := f.Client()
	if err != nil {
		return nil, err
	}
	defer func() { _ = c.Close() }() // the crawl is over; a close error changes nothing
	if req == nil {
		req = &run.CreateRunRequest{}
	}
	if req.GetRunId() == "" {
		req.RunId = fmt.Sprintf("local/%d", time.Now().UnixNano())
	}
	if _, err := c.Runs.CreateRun(ctx, req); err != nil {
		return nil, err
	}
	res, err := c.Seed(ctx, req.GetRunId(), seeds...)
	if err != nil {
		return nil, err
	}
	for i, r := range res {
		if r.GetStatus() != task.StatusAdded {
			return nil, fmt.Errorf("local: seed %d (%s) was not added: %s", i, seeds[i].GetRequest().GetUrl(), r.GetStatus())
		}
	}
	if _, err := c.Runs.SealRun(ctx, &run.SealRunRequest{RunId: req.GetRunId()}); err != nil {
		return nil, err
	}

	wctx, stop := context.WithCancel(ctx)
	defer stop()
	done := make(chan error, 1)
	go func() {
		done <- w.Run(wctx)
		stop() // a worker that could not run ends the wait below
	}()
	final, err := c.Wait(wctx, req.GetRunId())
	stop()
	if werr := <-done; werr != nil {
		return nil, fmt.Errorf("local: worker: %w", werr)
	}
	if err != nil {
		return nil, err
	}
	return final, nil
}

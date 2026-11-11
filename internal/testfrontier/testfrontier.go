// Package testfrontier runs a Frontier inside a test, on the in-memory store or on
// PostgreSQL.
package testfrontier

import (
	"context"
	"errors"
	"net"
	"slices"
	"sync"
	"testing"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"google.golang.org/grpc"

	"github.com/edevhub/crawly/internal/frontier"
	"github.com/edevhub/crawly/internal/policy"
	"github.com/edevhub/crawly/internal/store"
	"github.com/edevhub/crawly/internal/store/memstore"
	"github.com/edevhub/crawly/internal/store/pgstore"
	"github.com/edevhub/crawly/internal/store/pgstore/pgtest"
)

// Policies are fast limits and short leases, so tests finish quickly.
const Policies = `{"policies": [{"name": "fast", "limits": {"ratePerSecond": 500, "burst": 50, "maxConcurrency": 8},
  "behaviour": {"maxAttempts": 3, "backoff": {"initial": "0.05s", "max": "0.1s"}, "leaseDuration": "2s"}}]}`

// Frontier is a Frontier replica running inside a test. Its store outlives the
// replica, so Stop followed by Restart behaves like a replica being replaced.
type Frontier struct {
	// Addr is the address the replica listens on.
	Addr string
	// Metrics is the registry the replica's metrics are in. Restart replaces it.
	Metrics *prometheus.Registry
	// ServerOptions are added to the gRPC server of the next Restart.
	ServerOptions []grpc.ServerOption
	// NoHandlerGrace is how long the next Restart lets a task wait for a session that
	// has its handler. Zero: five seconds, longer than a worker takes to reconnect.
	NoHandlerGrace time.Duration

	tb    testing.TB
	pol   *policy.Set
	store store.Store

	mu      sync.Mutex
	server  *frontier.Server
	grpc    *grpc.Server
	cancel  context.CancelFunc
	stopped sync.WaitGroup
}

// Start runs a Frontier on a free local port until the test ends.
func Start(tb testing.TB) *Frontier {
	tb.Helper()
	return StartAt(tb, "127.0.0.1:0")
}

// StartAt is Start on a given address.
func StartAt(tb testing.TB, addr string) *Frontier {
	tb.Helper()
	pol := policiesFrom(tb, Policies)
	return start(tb, addr, pol, memstore.New(pol))
}

// StartPostgres is Start with the PostgreSQL store, in a schema of its own. The test
// is skipped when no PostgreSQL is configured (see pgtest).
func StartPostgres(tb testing.TB) *Frontier {
	tb.Helper()
	return StartPostgresWith(tb, Policies)
}

// StartWith is Start with the policies in the given JSON, instead of Policies.
func StartWith(tb testing.TB, policies string) *Frontier {
	tb.Helper()
	pol := policiesFrom(tb, policies)
	return start(tb, "127.0.0.1:0", pol, memstore.New(pol))
}

// StartPostgresWith is StartPostgres with the policies in the given JSON.
func StartPostgresWith(tb testing.TB, policies string) *Frontier {
	tb.Helper()
	dsn := pgtest.Schema(tb)
	pol := policiesFrom(tb, policies)
	st, err := pgstore.New(context.Background(), dsn, pol)
	if err != nil {
		tb.Fatal(err)
	}
	f := start(tb, "127.0.0.1:0", pol, st)
	tb.Cleanup(st.Close) // runs after the replica has stopped
	return f
}

func policiesFrom(tb testing.TB, src string) *policy.Set {
	tb.Helper()
	pol, err := policy.Parse([]byte(src))
	if err != nil {
		tb.Fatal(err)
	}
	return pol
}

func start(tb testing.TB, addr string, pol *policy.Set, st store.Store) *Frontier {
	tb.Helper()
	f := &Frontier{Addr: addr, tb: tb, pol: pol, store: st}
	f.Restart()
	tb.Cleanup(f.Stop)
	return f
}

// Restart starts a new replica over the same store and on the same address. The
// previous one must have been stopped.
func (f *Frontier) Restart() {
	f.tb.Helper()
	grace := f.NoHandlerGrace
	if grace <= 0 {
		grace = 5 * time.Second
	}
	reg := prometheus.NewRegistry()
	srv, err := frontier.New(frontier.Config{Store: f.store, Policies: f.pol, Metrics: reg, NoHandlerGrace: grace, NoHandlerDelay: 50 * time.Millisecond,
		ReaperInterval: 50 * time.Millisecond, PoolStatsInterval: 50 * time.Millisecond, MaintainInterval: 50 * time.Millisecond, PollInterval: 100 * time.Millisecond})
	if err != nil {
		f.tb.Fatal(err)
	}
	lis, err := (&net.ListenConfig{}).Listen(context.Background(), "tcp", f.Addr)
	if err != nil {
		f.tb.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	g := grpc.NewServer(slices.Concat(frontier.ServerOptions(), srv.MetricsOptions(), f.ServerOptions)...)
	srv.Register(g)
	f.mu.Lock()
	f.Addr, f.Metrics, f.server, f.grpc, f.cancel = lis.Addr().String(), reg, srv, g, cancel
	f.mu.Unlock()
	f.stopped.Go(func() {
		if err := srv.Run(ctx); err != nil {
			f.tb.Errorf("frontier loops: %v", err)
		}
	})
	f.stopped.Go(func() {
		if err := g.Serve(lis); err != nil && !errors.Is(err, grpc.ErrServerStopped) {
			f.tb.Errorf("serving: %v", err)
		}
	})
}

// Stop kills the replica without warning, as a crash would. It is safe to call twice.
func (f *Frontier) Stop() {
	f.mu.Lock()
	g, cancel := f.grpc, f.cancel
	f.grpc, f.cancel = nil, nil
	f.mu.Unlock()
	if g == nil {
		return
	}
	g.Stop()
	cancel()
	f.stopped.Wait()
}

// Drain makes the replica stop taking work, as the first step of a shutdown does.
func (f *Frontier) Drain() {
	f.mu.Lock()
	srv := f.server
	f.mu.Unlock()
	srv.Drain()
}

// Shutdown stops the replica the way a deployment does: it drains the sessions, then
// stops serving. It reports whether that finished before the timeout.
func (f *Frontier) Shutdown(timeout time.Duration) bool {
	f.mu.Lock()
	srv, g, cancel := f.server, f.grpc, f.cancel
	f.grpc, f.cancel = nil, nil
	f.mu.Unlock()
	if g == nil {
		return true
	}
	ctx, done := context.WithTimeout(context.Background(), timeout)
	defer done()
	srv.Shutdown(ctx, g)
	graceful := ctx.Err() == nil
	cancel()
	f.stopped.Wait()
	return graceful
}

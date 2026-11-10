// Package testfrontier runs a Frontier on the in-memory store inside a test.
package testfrontier

import (
	"context"
	"errors"
	"net"
	"sync"
	"testing"
	"time"

	"google.golang.org/grpc"

	"github.com/edevhub/crawly/internal/frontier"
	"github.com/edevhub/crawly/internal/policy"
	"github.com/edevhub/crawly/internal/store"
	"github.com/edevhub/crawly/internal/store/memstore"
)

// Policies are fast limits and short leases, so tests finish quickly.
const Policies = `{"policies": [{"name": "fast", "limits": {"ratePerSecond": 500, "burst": 50, "maxConcurrency": 8},
  "behaviour": {"maxAttempts": 3, "backoff": {"initial": "0.05s", "max": "0.1s"}, "leaseDuration": "2s"}}]}`

// Frontier is a Frontier replica running inside a test. Its store outlives the
// replica, so Stop followed by Restart behaves like a replica being replaced.
type Frontier struct {
	// Addr is the address the replica listens on.
	Addr string

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
	pol, err := policy.Parse([]byte(Policies))
	if err != nil {
		tb.Fatal(err)
	}
	f := &Frontier{Addr: addr, tb: tb, pol: pol, store: memstore.New(pol)}
	f.Restart()
	tb.Cleanup(f.Stop)
	return f
}

// Restart starts a new replica over the same store and on the same address. The
// previous one must have been stopped.
func (f *Frontier) Restart() {
	f.tb.Helper()
	srv, err := frontier.New(frontier.Config{Store: f.store, Policies: f.pol,
		ReaperInterval: 50 * time.Millisecond, MaintainInterval: 50 * time.Millisecond, PollInterval: 100 * time.Millisecond})
	if err != nil {
		f.tb.Fatal(err)
	}
	lis, err := (&net.ListenConfig{}).Listen(context.Background(), "tcp", f.Addr)
	if err != nil {
		f.tb.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	g := grpc.NewServer(frontier.ServerOptions()...)
	srv.Register(g)
	f.mu.Lock()
	f.Addr, f.server, f.grpc, f.cancel = lis.Addr().String(), srv, g, cancel
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

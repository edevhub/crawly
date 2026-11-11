package frontier

import (
	"context"
	"strings"
	"testing"
	"time"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	workerv1 "github.com/edevhub/crawly/internal/gen/frontier/worker/v1"
	"github.com/edevhub/crawly/internal/policy"
	"github.com/edevhub/crawly/internal/store"
	"github.com/edevhub/crawly/internal/store/memstore"
	"github.com/edevhub/crawly/internal/wire"
)

func TestSessionHandlers(t *testing.T) {
	got, err := sessionHandlers([]string{"b", "a", "b"})
	if err != nil || strings.Join(got, ",") != "a,b" {
		t.Fatalf("%v %v", got, err)
	}
	if got, err := sessionHandlers(nil); err != nil || got != nil {
		t.Fatalf("no handlers: %v %v", got, err)
	}
	tooMany := make([]string, wire.MaxSessionHandlers+1)
	for i := range tooMany {
		tooMany[i] = strings.Repeat("h", 1+i%5) + string(rune('a'+i%26)) + strings.Repeat("x", i/26)
	}
	for name, in := range map[string][]string{
		"empty":    {"a", ""},
		"nul":      {"a\x00b"},
		"long":     {strings.Repeat("h", wire.MaxHandlerBytes+1)},
		"too many": tooMany,
	} {
		if _, err := sessionHandlers(in); status.Code(err) != codes.InvalidArgument {
			t.Errorf("%s: %v", name, err)
		}
	}
}

// TestDispatchRoundServesEverySession: a session the store has nothing for must not
// end the round while another session, with other handlers, has a free credit.
func TestDispatchRoundServesEverySession(t *testing.T) {
	pol, err := policy.New(nil)
	if err != nil {
		t.Fatal(err)
	}
	st := memstore.New(pol)
	s, err := New(Config{Store: st, Policies: pol})
	if err != nil {
		t.Fatal(err)
	}
	ctx := context.Background()
	if _, err := st.CreateRun(ctx, store.RunSpec{ID: "r", Hash: []byte("r")}); err != nil {
		t.Fatal(err)
	}
	if _, err := st.AddTasks(ctx, store.AddRequest{Run: "r", Tasks: []store.NewTask{
		{Fingerprint: store.Fingerprint{1}, Queue: "q1", Pool: "default", Handler: "b"},
		{Fingerprint: store.Fingerprint{2}, Queue: "q2", Pool: "default", Handler: "b"}}}); err != nil {
		t.Fatal(err)
	}

	p := &pool{name: "default", sessions: map[*session]struct{}{}, wake: make(chan struct{}, 1)}
	newSession := func(id string, served uint64, handlers ...string) *session {
		sctx, cancel := context.WithCancel(ctx)
		t.Cleanup(cancel)
		ss := &session{id: store.SessionID(id), ctx: sctx, cancel: cancel, out: make(chan *workerv1.WorkResponse, 8), credits: 4, served: served,
			handlers: handlers, sig: strings.Join(handlers, "\x00"), leases: map[store.LeaseID]store.RunID{}, undelivered: map[store.LeaseID]store.LeaseRef{}}
		p.sessions[ss] = struct{}{}
		return ss
	}
	// The session of "a" has waited longest, so it is asked first.
	a, b := newSession("a", 1, "a"), newSession("b", 9, "b")
	if next := s.dispatchRound(ctx, p); !next.IsZero() {
		t.Fatalf("next ready %v: nothing is waiting for a time", next)
	}
	if len(a.out) != 0 || len(b.out) != 2 {
		t.Fatalf("the session of a was sent %d tasks and the session of b %d, want 0 and 2", len(a.out), len(b.out))
	}
}

// TestSweepUnservedFailsOnlyUnservedHandlers: a replica reports the handlers its
// sessions have, and a task whose handler nobody reported for the grace period fails.
func TestSweepUnservedFailsOnlyUnservedHandlers(t *testing.T) {
	pol, err := policy.New(nil)
	if err != nil {
		t.Fatal(err)
	}
	st := memstore.New(pol)
	const grace = 50 * time.Millisecond
	s, err := New(Config{Store: st, Policies: pol, NoHandlerGrace: grace, MaintainInterval: grace / 4})
	if err != nil {
		t.Fatal(err)
	}
	ctx := context.Background()
	if _, err := st.CreateRun(ctx, store.RunSpec{ID: "r", Hash: []byte("r")}); err != nil {
		t.Fatal(err)
	}
	if _, err := st.AddTasks(ctx, store.AddRequest{Run: "r", Tasks: []store.NewTask{
		{Fingerprint: store.Fingerprint{1}, Queue: "q1", Pool: "default", Handler: "page"},
		{Fingerprint: store.Fingerprint{2}, Queue: "q2", Pool: "default", Handler: "ghost"}}}); err != nil {
		t.Fatal(err)
	}
	p := &pool{name: "default", sessions: map[*session]struct{}{}, wake: make(chan struct{}, 1)}
	p.sessions[&session{handlers: []string{"page"}}] = struct{}{}
	s.pools["default"] = p

	time.Sleep(3 * grace)
	s.sweepUnserved(ctx)
	r, err := st.GetRun(ctx, "r")
	if err != nil {
		t.Fatal(err)
	}
	if r.Stats.Failed != 1 || r.Stats.ByCode[CodeNoHandler] != 1 || r.Stats.Pending != 1 {
		t.Fatalf("stats: %+v, want the task of ghost failed and the one of page pending", r.Stats)
	}
}

// TestSweepUnservedSparesLegacySessions: a session that names no handlers (a worker
// of an older SDK) is offered every task of its pool, so no handler of the pool is
// unserved while it is connected, and for the grace period after it leaves.
func TestSweepUnservedSparesLegacySessions(t *testing.T) {
	pol, err := policy.New(nil)
	if err != nil {
		t.Fatal(err)
	}
	st := memstore.New(pol)
	const grace = 100 * time.Millisecond
	s, err := New(Config{Store: st, Policies: pol, NoHandlerGrace: grace, MaintainInterval: grace / 4})
	if err != nil {
		t.Fatal(err)
	}
	ctx := context.Background()
	if _, err := st.CreateRun(ctx, store.RunSpec{ID: "r", Hash: []byte("r")}); err != nil {
		t.Fatal(err)
	}
	if _, err := st.AddTasks(ctx, store.AddRequest{Run: "r", Tasks: []store.NewTask{
		{Fingerprint: store.Fingerprint{1}, Queue: "q1", Pool: "default", Handler: "page"}}}); err != nil {
		t.Fatal(err)
	}
	legacy := &session{}
	p := &pool{name: "default", sessions: map[*session]struct{}{legacy: {}}, wake: make(chan struct{}, 1)}
	s.pools["default"] = p
	stats := func() store.RunStats {
		r, err := st.GetRun(ctx, "r")
		if err != nil {
			t.Fatal(err)
		}
		return r.Stats
	}

	// The task is older than the grace, and the session has been there all along.
	for range 12 {
		time.Sleep(grace / 4)
		s.sweepUnserved(ctx)
		if got := stats(); got.Failed != 0 || got.Pending != 1 {
			t.Fatalf("a task that a session without handlers can run was failed: %+v", got)
		}
	}
	// The session leaves. Nothing fails within the grace period of its last report.
	delete(p.sessions, legacy)
	s.sweepUnserved(ctx)
	if got := stats(); got.Failed != 0 {
		t.Fatalf("failed within the grace period of the last report: %+v", got)
	}
	time.Sleep(3 * grace)
	s.sweepUnserved(ctx)
	if got := stats(); got.Failed != 1 || got.ByCode[CodeNoHandler] != 1 {
		t.Fatalf("a task nobody could run was not failed: %+v", got)
	}
}

// countingStore counts the reports of handlers.
type countingStore struct {
	store.Store
	reports int
	last    []store.PoolHandlers
}

func (c *countingStore) ServeHandlers(ctx context.Context, sets []store.PoolHandlers) error {
	c.reports++
	c.last = sets
	return c.Store.ServeHandlers(ctx, sets)
}

// TestReportHandlersOnChange: the store hears of the handlers when they change and as
// a heartbeat, not on every tick.
func TestReportHandlersOnChange(t *testing.T) {
	pol, err := policy.New(nil)
	if err != nil {
		t.Fatal(err)
	}
	cs := &countingStore{Store: memstore.New(pol)}
	const grace = 400 * time.Millisecond // a heartbeat every 40ms
	s, err := New(Config{Store: cs, Policies: pol, NoHandlerGrace: grace, MaintainInterval: 10 * time.Millisecond})
	if err != nil {
		t.Fatal(err)
	}
	ctx := context.Background()
	p := &pool{name: "default", sessions: map[*session]struct{}{{handlers: []string{"a"}}: {}}, wake: make(chan struct{}, 1)}
	s.pools["default"] = p
	report := func() {
		t.Helper()
		if err := s.reportHandlers(ctx); err != nil {
			t.Fatal(err)
		}
	}
	for range 5 {
		report()
	}
	if cs.reports != 1 {
		t.Fatalf("%d reports for the same handlers, want 1", cs.reports)
	}
	p.sessions[&session{}] = struct{}{} // a session that names no handlers: the handler ""
	report()
	if cs.reports != 2 || len(cs.last) != 1 || strings.Join(cs.last[0].Handlers, ",") != ",a" {
		t.Fatalf("%d reports, last %+v: want the change reported, with the wildcard", cs.reports, cs.last)
	}
	time.Sleep(50 * time.Millisecond)
	report()
	if cs.reports != 3 {
		t.Fatalf("%d reports: want a heartbeat after a while", cs.reports)
	}
}

// TestStoredHandler: a handler name no worker could have registered does not fail the
// task: it is stored as unknown, as before names were checked.
func TestStoredHandler(t *testing.T) {
	for in, want := range map[string]string{
		"page": "page",
		"":     "",
		strings.Repeat("h", wire.MaxHandlerBytes):   strings.Repeat("h", wire.MaxHandlerBytes),
		strings.Repeat("h", wire.MaxHandlerBytes+1): "",
		"a\x00b": "",
	} {
		if got := storedHandler(in); got != want {
			t.Errorf("%q: %q, want %q", in[:min(len(in), 8)], got[:min(len(got), 8)], want)
		}
	}
}

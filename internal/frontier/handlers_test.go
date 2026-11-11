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
	s, err := New(Config{Store: st, Policies: pol, NoHandlerGrace: grace})
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

package frontier

import (
	"context"
	"fmt"
	"testing"
	"time"

	sessionv1 "github.com/edevhub/crawly/internal/gen/frontier/session/v1"
	"github.com/edevhub/crawly/internal/store"
)

func TestListSessionsPages(t *testing.T) {
	srv, _ := testServer(t, nil)
	ctx := t.Context()
	base := time.Now()
	ended, end := context.WithCancel(ctx) // sessions that have ended are skipped by the dispatch loop
	end()
	for _, pool := range []store.PoolName{"default", "browser"} {
		p := srv.pool(ctx, pool)
		p.mu.Lock()
		for i := range 3 {
			p.sessions[&session{id: store.SessionID(fmt.Sprintf("r/%s%d", pool, i)), pool: pool, ctx: ended, since: base.Add(time.Duration(i) * time.Second),
				worker: store.WorkerInfo{ID: "w"}, handlers: []string{"parse", "fetch"}, leases: map[store.LeaseID]store.RunID{"l": "r"}, credits: 2}] = struct{}{}
		}
		p.mu.Unlock()
	}
	svc := sessionService{s: srv}

	var got []string
	token := ""
	for pages := 0; ; pages++ {
		if pages > 10 {
			t.Fatal("paging does not end")
		}
		res, err := svc.ListSessions(ctx, &sessionv1.ListSessionsRequest{PageSize: 4, PageToken: token})
		if err != nil {
			t.Fatal(err)
		}
		if res.GetReplicaId() == "" || len(res.GetSessions()) > 4 {
			t.Fatalf("page: %v", res)
		}
		for _, s := range res.GetSessions() {
			got = append(got, s.GetSessionId())
			if s.GetCredits() != 2 || s.GetUnfinishedAssignments() != 1 || fmt.Sprint(s.GetHandlers()) != "[parse fetch]" {
				t.Errorf("session: %v", s)
			}
		}
		if token = res.GetNextPageToken(); token == "" {
			break
		}
	}
	want := []string{"r/browser0", "r/browser1", "r/browser2", "r/default0", "r/default1", "r/default2"}
	if fmt.Sprint(got) != fmt.Sprint(want) {
		t.Errorf("sessions across pages: %v, want %v", got, want)
	}
	res, err := svc.ListSessions(ctx, &sessionv1.ListSessionsRequest{Pool: "default"})
	if err != nil || len(res.GetSessions()) != 3 || res.GetNextPageToken() != "" {
		t.Errorf("one pool: %v %v", res, err)
	}
}

package memstore_test

import (
	"errors"
	"testing"
	"time"

	policyv1 "github.com/edevhub/crawly/internal/gen/frontier/policy/v1"
	"github.com/edevhub/crawly/internal/store"
	"github.com/edevhub/crawly/internal/store/memstore"
	"github.com/edevhub/crawly/internal/store/storetest"
)

func TestConformance(t *testing.T) {
	storetest.Run(t, func(pol store.Policies, now func() time.Time) store.Store {
		return memstore.New(pol, memstore.WithClock(now))
	})
}

type fixed struct{}

func (fixed) Hash() string { return "test" }

func (fixed) For(store.RunID, *policyv1.RunPolicy, store.QueueKey) store.Effective {
	return store.Effective{Weight: 1, Lease: store.LeaseSettings{Duration: time.Minute, MaxDuration: time.Hour}}
}

// Ended runs are deleted after their retention, with their tasks and idle queues.
func TestRetention(t *testing.T) {
	ctx := t.Context()
	clock := storetest.NewClock()
	s := memstore.New(fixed{}, memstore.WithClock(clock.Now), memstore.WithRetention(time.Hour))
	create := func(id string) {
		t.Helper()
		if _, err := s.CreateRun(ctx, store.RunSpec{ID: store.RunID(id), Hash: []byte(id)}); err != nil {
			t.Fatal(err)
		}
		var fp store.Fingerprint
		copy(fp[:], id)
		if _, err := s.AddTasks(ctx, store.AddRequest{Run: store.RunID(id), Tasks: []store.NewTask{{Queue: "q-" + store.QueueKey(id), Fingerprint: fp}}}); err != nil {
			t.Fatal(err)
		}
	}
	create("old")
	create("live")
	if _, err := s.CancelRun(ctx, "old"); err != nil {
		t.Fatal(err)
	}
	tasks, _, err := s.ListTasks(ctx, store.TaskFilter{Run: "old"}, store.Page{})
	if err != nil || len(tasks) != 1 {
		t.Fatalf("tasks of the run: %d %v", len(tasks), err)
	}

	clock.Advance(30 * time.Minute)
	if err := s.Maintain(ctx); err != nil {
		t.Fatal(err)
	}
	if _, err := s.GetRun(ctx, "old"); err != nil {
		t.Fatalf("the run inside its retention: %v", err)
	}

	clock.Advance(time.Hour)
	if err := s.Maintain(ctx); err != nil {
		t.Fatal(err)
	}
	if _, err := s.GetRun(ctx, "old"); !errors.Is(err, store.ErrNotFound) {
		t.Fatalf("the run after its retention: %v", err)
	}
	if _, err := s.GetTask(ctx, "old", tasks[0].ID, nil); !errors.Is(err, store.ErrNotFound) {
		t.Fatalf("a task of the deleted run: %v", err)
	}
	if _, err := s.GetQueue(ctx, "q-old"); !errors.Is(err, store.ErrNotFound) {
		t.Fatalf("the queue only the deleted run used: %v", err)
	}
	// The open run is as old, and stays.
	if run, err := s.GetRun(ctx, "live"); err != nil || run.Stats.Pending != 1 {
		t.Fatalf("the open run: %+v %v", run, err)
	}
	if _, err := s.GetQueue(ctx, "q-live"); err != nil {
		t.Fatal(err)
	}
	// The ID is free again.
	create("old")
}

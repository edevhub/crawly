package frontier

import (
	"testing"
	"time"

	taskv1 "github.com/edevhub/crawly/internal/gen/frontier/task/v1"
	"github.com/edevhub/crawly/internal/policy"
	"github.com/edevhub/crawly/internal/store"
	"github.com/edevhub/crawly/internal/store/memstore"
)

// TestNoHandlerGrace: a session that names no handlers is offered every task and hands
// back the ones it cannot run. The grace of such a task counts the time no worker could
// run it, and a worker that runs it, whatever the result, ends it.
func TestNoHandlerGrace(t *testing.T) {
	pol, err := policy.New(nil)
	if err != nil {
		t.Fatal(err)
	}
	const grace = 50 * time.Millisecond
	s, err := New(Config{Store: memstore.New(pol), Policies: pol, NoHandlerGrace: grace, MaintainInterval: grace / 4, NoHandlerDelay: time.Second})
	if err != nil {
		t.Fatal(err)
	}
	info := store.LeaseInfo{Task: "t1", Ref: store.LeaseRef{Attempt: 1}}
	lacks := &taskv1.Outcome{Kind: taskv1.Outcome_KIND_RELEASED, Code: CodeNoHandler}
	// report is what handleComplete does with an outcome the store accepted.
	report := func(o *taskv1.Outcome) store.Finish {
		f := s.decide(info, o, nil)
		s.served(info, store.WorkerInfo{ID: "w"}, o, f)
		return f
	}

	if f := report(lacks); f.Result != store.ResultReleased || f.RetryIn != time.Second {
		t.Fatalf("the first hand-back: %+v", f)
	}
	// A worker with the handler runs the task and asks for a retry.
	if f := report(&taskv1.Outcome{Kind: taskv1.Outcome_KIND_RETRY, Code: "http_503"}); f.Result != store.ResultRetry {
		t.Fatalf("the retry: %+v", f)
	}
	time.Sleep(2 * grace)
	if f := report(lacks); f.Result != store.ResultReleased {
		t.Fatalf("a task that was run since its first hand-back must start a new grace: %+v", f)
	}
	// Nobody runs it for the grace: it fails, with an attempt recorded as a failure.
	time.Sleep(2 * grace)
	f := report(lacks)
	if f.Result != store.ResultFailed || f.Record.Code != CodeNoHandler || f.Record.Kind != int32(taskv1.Outcome_KIND_FAILED) || f.RetryIn != 0 {
		t.Fatalf("after the grace: %+v", f)
	}
	if _, kept := s.unserved.Load(info.Task); kept {
		t.Error("the record of a failed task was kept")
	}

	// A decision the store did not take leaves no record behind.
	other := store.LeaseInfo{Task: "t2"}
	s.decide(other, lacks, nil)
	if _, kept := s.unserved.Load(other.Task); kept {
		t.Error("decide must not record: the store may refuse the outcome")
	}
}

// TestReleaseCodes: a task goes back only for the reasons the SDK has. Any other code
// is a permanent failure, as an unknown kind is, and a hand-back never blocks a queue.
func TestReleaseCodes(t *testing.T) {
	pol, err := policy.New(nil)
	if err != nil {
		t.Fatal(err)
	}
	s, err := New(Config{Store: memstore.New(pol), Policies: pol})
	if err != nil {
		t.Fatal(err)
	}
	info := store.LeaseInfo{Task: "t", Ref: store.LeaseRef{Attempt: 1}}
	if f := s.decide(info, &taskv1.Outcome{Kind: taskv1.Outcome_KIND_RELEASED, Code: CodeShutdown}, nil); f.Result != store.ResultReleased || f.RetryIn != 0 {
		t.Errorf("shutdown: %+v", f)
	}
	for _, code := range []string{"", "whatever"} {
		if f := s.decide(info, &taskv1.Outcome{Kind: taskv1.Outcome_KIND_RELEASED, Code: code}, nil); f.Result != store.ResultFailed {
			t.Errorf("code %q: %+v, want a failure", code, f)
		}
	}
}

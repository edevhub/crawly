package frontier

import (
	"testing"
	"time"

	"google.golang.org/protobuf/types/known/durationpb"

	policyv1 "github.com/edevhub/crawly/internal/gen/frontier/policy/v1"
	taskv1 "github.com/edevhub/crawly/internal/gen/frontier/task/v1"
	"github.com/edevhub/crawly/internal/policy"
	"github.com/edevhub/crawly/internal/store"
	"github.com/edevhub/crawly/internal/store/memstore"
)

// TestQueueBlockCeiling: one outcome blocks a queue for as long as the queue's policy
// allows, 24 hours unless it says otherwise, and a run cannot raise that for itself.
func TestQueueBlockCeiling(t *testing.T) {
	pol, err := policy.Parse([]byte(`{"policies": [
		{"name": "default"},
		{"name": "cooldown", "match": {"queueKey": "shop.example"}, "behaviour": {"maxQueueBlock": "259200s"}}]}`))
	if err != nil {
		t.Fatal(err)
	}
	s, err := New(Config{Store: memstore.New(pol), Policies: pol})
	if err != nil {
		t.Fatal(err)
	}
	asked := &taskv1.Outcome{Kind: taskv1.Outcome_KIND_FAILED, Code: "blocked", BlockQueueFor: durationpb.New(100 * time.Hour)}
	// CreateRun rejects such a policy; one that got through must still change nothing.
	greedy := &policyv1.RunPolicy{Behaviour: &policyv1.Behaviour{MaxQueueBlock: durationpb.New(1000 * time.Hour)},
		Overrides: []*policyv1.RunQueueOverride{{QueueKey: "*", Behaviour: &policyv1.Behaviour{MaxQueueBlock: durationpb.New(2000 * time.Hour)}}}}
	if err := policy.ValidateRun(greedy); err == nil {
		t.Error("a run that sets the ceiling must be rejected")
	}
	if err := policy.ValidateRun(&policyv1.RunPolicy{Overrides: greedy.GetOverrides()}); err == nil {
		t.Error("a run override that sets the ceiling must be rejected")
	}
	if r := pol.Resolve(greedy, "shop.example"); r.Behaviour.GetMaxQueueBlock().AsDuration() != 72*time.Hour ||
		r.Provenance["behaviour.max_queue_block"] != "cooldown" {
		t.Errorf("resolved for a run: %v from %q", r.Behaviour.GetMaxQueueBlock().AsDuration(), r.Provenance["behaviour.max_queue_block"])
	}
	for _, bad := range []string{"0s", "-5s"} {
		if _, err := policy.Parse([]byte(`{"policies": [{"name": "p", "behaviour": {"maxQueueBlock": "` + bad + `"}}]}`)); err == nil {
			t.Errorf("a ceiling of %s must be rejected", bad)
		}
	}
	for queue, want := range map[store.QueueKey]time.Duration{"shop.example": 72 * time.Hour, "other.example": 24 * time.Hour} {
		if got := s.decide(store.LeaseInfo{Queue: queue}, asked, nil).Block; got != want {
			t.Errorf("%s: blocked for %v, want %v", queue, got, want)
		}
		if got := s.decide(store.LeaseInfo{Queue: queue, RunPolicy: greedy}, asked, nil).Block; got != want {
			t.Errorf("%s with a run policy of its own: blocked for %v, want %v", queue, got, want)
		}
	}
	short := &taskv1.Outcome{Kind: taskv1.Outcome_KIND_RETRY, BlockQueueFor: durationpb.New(time.Minute)}
	if got := s.decide(store.LeaseInfo{Queue: "shop.example", Ref: store.LeaseRef{Attempt: 1}}, short, nil).Block; got != time.Minute {
		t.Errorf("a block under the ceiling: %v", got)
	}
}

// TestStrainRetries: a failure that is a sign of strain is judged against
// max_strain_attempts, not max_attempts, while adaptive pacing is on; with it off the
// signal means nothing, and a task that was handed back is never a sign of strain.
func TestStrainRetries(t *testing.T) {
	pol, err := policy.Parse([]byte(`{"policies": [
		{"name": "default", "behaviour": {"maxAttempts": 3, "maxStrainAttempts": 6}},
		{"name": "fixed", "match": {"queueKey": "fixed.example"}, "limits": {"adaptive": {"enabled": false}}},
		{"name": "short", "match": {"queueKey": "short.example"}, "behaviour": {"maxStrainAttempts": 2}}]}`))
	if err != nil {
		t.Fatal(err)
	}
	s, err := New(Config{Store: memstore.New(pol), Policies: pol})
	if err != nil {
		t.Fatal(err)
	}
	strained := &taskv1.Outcome{Kind: taskv1.Outcome_KIND_RETRY, Code: "http_503", Strain: true}
	plain := &taskv1.Outcome{Kind: taskv1.Outcome_KIND_RETRY, Code: "parse"}
	cases := []struct {
		name    string
		queue   store.QueueKey
		o       *taskv1.Outcome
		attempt uint32
		result  store.TaskResult
		strain  bool
	}{
		{"strain: past max_attempts", "a.example", strained, 5, store.ResultRetry, true},
		{"strain: the ceiling ends it", "a.example", strained, 6, store.ResultFailed, true},
		{"no strain: max_attempts", "a.example", plain, 3, store.ResultFailed, false},
		{"adaptive off: the signal is ignored", "fixed.example", strained, 3, store.ResultFailed, false},
		{"the ceiling is never below max_attempts", "short.example", strained, 2, store.ResultRetry, true},
		{"handed back", "a.example", &taskv1.Outcome{Kind: taskv1.Outcome_KIND_RELEASED, Code: "shutdown", Strain: true}, 1, store.ResultReleased, false},
	}
	for _, c := range cases {
		f := s.decide(store.LeaseInfo{Queue: c.queue, Ref: store.LeaseRef{Attempt: c.attempt}}, c.o, nil)
		if f.Result != c.result || f.Strain != c.strain {
			t.Errorf("%s: result %v strain %v, want %v %v", c.name, f.Result, f.Strain, c.result, c.strain)
		}
	}
}

package frontier

import (
	"fmt"
	"testing"
	"time"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/types/known/durationpb"

	policyv1 "github.com/edevhub/crawly/internal/gen/frontier/policy/v1"
	runv1 "github.com/edevhub/crawly/internal/gen/frontier/run/v1"
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

// TestStrainRetries: a task has two budgets. Failures that are signs of strain count
// against max_strain_attempts, the others against max_attempts, whatever came before;
// with adaptive pacing off the signal means nothing, and a task that was handed back is
// never a sign of strain.
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
		name     string
		queue    store.QueueKey
		o        *taskv1.Outcome
		attempt  uint32
		strained uint32 // earlier attempts that ended with strain
		result   store.TaskResult
		strain   bool
		extended bool
	}{
		{"strain, first attempt", "a.example", strained, 1, 0, store.ResultRetry, true, false},
		{"strain: past max_attempts is the extended budget", "a.example", strained, 5, 4, store.ResultRetry, true, true},
		{"strain: the budget ends it", "a.example", strained, 6, 5, store.ResultFailed, true, false},
		{"no strain: max_attempts", "a.example", plain, 3, 0, store.ResultFailed, false, false},
		{"a 500 after four strain attempts is the first of its own", "a.example", plain, 5, 4, store.ResultRetry, false, false},
		{"and the third of its own ends the task", "a.example", plain, 7, 4, store.ResultFailed, false, false},
		{"adaptive off: the signal is ignored", "fixed.example", strained, 3, 0, store.ResultFailed, false, false},
		{"the budget is never below max_attempts", "short.example", strained, 2, 0, store.ResultRetry, true, false},
		{"handed back", "a.example", &taskv1.Outcome{Kind: taskv1.Outcome_KIND_RELEASED, Code: "shutdown", Strain: true}, 1, 0, store.ResultReleased, false, false},
	}
	for _, c := range cases {
		f := s.decide(store.LeaseInfo{Queue: c.queue, Ref: store.LeaseRef{Attempt: c.attempt}, StrainAttempts: c.strained}, c.o, nil)
		if f.Result != c.result || f.Strain != c.strain || f.Extended != c.extended {
			t.Errorf("%s: result %v strain %v extended %v, want %v %v %v", c.name, f.Result, f.Strain, f.Extended, c.result, c.strain, c.extended)
		}
	}
}

// TestValidateAbortRules: abort_on_codes that cannot work as written are rejected at
// CreateRun.
func TestValidateAbortRules(t *testing.T) {
	many := make([]string, maxAbortCodes+1)
	for i := range many {
		many[i] = "code" + string(rune('a'+i%26)) + string(rune('a'+i/26))
	}
	for name, tc := range map[string]struct {
		limits *runv1.RunLimits
		ok     bool
	}{
		"none":                {nil, true},
		"rules without codes": {&runv1.RunLimits{MaxFailed: 5, MaxConsecutiveFailures: 3}, true},
		"codes":               {&runv1.RunLimits{AbortOnCodes: []string{"blocked", "captcha", "no_handler", "lease_expired"}}, true},
		"empty code":          {&runv1.RunLimits{AbortOnCodes: []string{"blocked", ""}}, false},
		"duplicate":           {&runv1.RunLimits{AbortOnCodes: []string{"blocked", "captcha", "blocked"}}, false},
		"revoked":             {&runv1.RunLimits{AbortOnCodes: []string{"revoked"}}, false},
		"shutdown":            {&runv1.RunLimits{AbortOnCodes: []string{"shutdown"}}, false},
		"at the bound":        {&runv1.RunLimits{AbortOnCodes: many[:maxAbortCodes]}, true},
		"over the bound":      {&runv1.RunLimits{AbortOnCodes: many}, false},
	} {
		if err := validateAbortRules(tc.limits); (err == nil) != tc.ok {
			t.Errorf("%s: %v", name, err)
		}
	}
}

// TestAnsweredOutcomes: an outcome with an answer from the site and no sign of strain
// is a step back to the policy's rate; one without an answer, or that blocks the queue,
// is not.
func TestAnsweredOutcomes(t *testing.T) {
	pol, err := policy.Parse([]byte(`{"policies": [{"name": "default"}]}`))
	if err != nil {
		t.Fatal(err)
	}
	s, err := New(Config{Store: memstore.New(pol), Policies: pol})
	if err != nil {
		t.Fatal(err)
	}
	status := func(code uint32) *taskv1.FetchInfo { return &taskv1.FetchInfo{HttpStatus: code} }
	cases := []struct {
		name  string
		o     *taskv1.Outcome
		fetch *taskv1.FetchInfo
		want  bool
	}{
		{"success", &taskv1.Outcome{Kind: taskv1.Outcome_KIND_SUCCESS}, status(200), true},
		{"a 404", &taskv1.Outcome{Kind: taskv1.Outcome_KIND_FAILED, Code: "http_404"}, status(404), true},
		{"a 500 that is no strain", &taskv1.Outcome{Kind: taskv1.Outcome_KIND_RETRY, Code: "http_500"}, status(500), true},
		{"a retry without an answer", &taskv1.Outcome{Kind: taskv1.Outcome_KIND_RETRY, Code: "panic"}, nil, false},
		{"strain", &taskv1.Outcome{Kind: taskv1.Outcome_KIND_RETRY, Code: "http_503", Strain: true}, status(503), false},
		{"a block", &taskv1.Outcome{Kind: taskv1.Outcome_KIND_RETRY, Code: "http_429", BlockQueueFor: durationpb.New(time.Minute)}, status(429), false},
	}
	for _, c := range cases {
		if got := s.decide(store.LeaseInfo{Queue: "a.example", Ref: store.LeaseRef{Attempt: 1}}, c.o, c.fetch).Answered; got != c.want {
			t.Errorf("%s: answered = %v, want %v", c.name, got, c.want)
		}
	}
}

// TestUnsupportedIsAFailedPrecondition: a deployment that cannot take abort rules yet is
// reported as a condition the caller can fix, not as an internal error.
func TestUnsupportedIsAFailedPrecondition(t *testing.T) {
	err := grpcErr(fmt.Errorf("%w: a replica is old", store.ErrUnsupported))
	if status.Code(err) != codes.FailedPrecondition {
		t.Fatalf("%v", err)
	}
}

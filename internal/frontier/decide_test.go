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

// TestUnsupportedIsAFailedPrecondition: a deployment that cannot take abort rules yet is
// reported as a condition the caller can fix, not as an internal error.
func TestUnsupportedIsAFailedPrecondition(t *testing.T) {
	err := grpcErr(fmt.Errorf("%w: a replica is old", store.ErrUnsupported))
	if status.Code(err) != codes.FailedPrecondition {
		t.Fatalf("%v", err)
	}
}

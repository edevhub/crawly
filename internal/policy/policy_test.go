package policy

import (
	"testing"
	"time"

	"google.golang.org/protobuf/proto"

	policyv1 "github.com/edevhub/crawly/internal/gen/frontier/policy/v1"
)

const cfg = `{"policies": [
  {"name": "all", "limits": {"ratePerSecond": 2, "maxConcurrency": 4}},
  {"name": "example", "match": {"queueKey": "*.example.com"}, "limits": {"ratePerSecond": 0.5},
   "behaviour": {"maxAttempts": 5, "leaseDuration": "30s"}}
]}`

func TestResolveSpecificityAndProvenance(t *testing.T) {
	s, err := Parse([]byte(cfg))
	if err != nil {
		t.Fatal(err)
	}
	r := s.Resolve(nil, "www.example.com")
	if r.Limits.GetRatePerSecond() != 0.5 || r.Limits.GetMaxConcurrency() != 4 {
		t.Fatalf("limits: %v", r.Limits)
	}
	if r.Provenance["limits.rate_per_second"] != "example" || r.Provenance["limits.max_concurrency"] != "all" {
		t.Fatalf("provenance: %v", r.Provenance)
	}
	if r.Behaviour.GetMaxAttempts() != 5 || r.Behaviour.GetLeaseDuration().AsDuration() != 30*time.Second {
		t.Fatalf("behaviour: %v", r.Behaviour)
	}
	if r.Provenance["behaviour.fetch_timeout"] != builtin {
		t.Fatalf("provenance: %v", r.Provenance)
	}
	if other := s.Resolve(nil, "other.org"); other.Limits.GetRatePerSecond() != 2 || other.Own {
		t.Fatalf("other: %v", other.Limits)
	}
}

func TestRunOverrideSetsOwn(t *testing.T) {
	s, _ := Parse([]byte(cfg))
	rp := &policyv1.RunPolicy{
		Weight: proto.Uint32(3),
		Overrides: []*policyv1.RunQueueOverride{
			{QueueKey: "www.example.com", Limits: &policyv1.QueueLimits{RatePerSecond: proto.Float64(10)}},
		},
	}
	e := s.For("r", rp, "www.example.com")
	if !e.Own || e.Limits.Interval != 100*time.Millisecond || e.Queue.Interval != 2*time.Second || e.Weight != 3 {
		t.Fatalf("effective: %+v", e)
	}
	if e := s.For("r", rp, "api.example.com"); e.Own {
		t.Fatalf("override must not apply: %+v", e)
	}
}

func TestMatch(t *testing.T) {
	for _, c := range []struct {
		p, k string
		want bool
	}{
		{"", "a.com", true}, {"a.com", "a.com", true}, {"a.com", "b.com", false},
		{"*.a.com", "x.a.com", true}, {"*.a.com", "a.com", false}, {"a*c", "abc", true}, {"a*c", "ab", false},
	} {
		if got := Match(c.p, c.k); got != c.want {
			t.Errorf("Match(%q, %q) = %v", c.p, c.k, got)
		}
	}
}

func TestHashChangesWithContent(t *testing.T) {
	a, _ := Parse([]byte(cfg))
	b, _ := Parse([]byte(`{"policies": []}`))
	if a.Hash() == b.Hash() || a.Hash() == "" {
		t.Fatal("hash must identify the set")
	}
	if _, err := Parse([]byte(`{"policies": [{"name": "a"}, {"name": "a"}]}`)); err == nil {
		t.Fatal("duplicate names must be rejected")
	}
}

// TestRateTooSmall: a rate whose interval does not fit in a time.Duration is refused,
// instead of overflowing into "no limit".
func TestRateTooSmall(t *testing.T) {
	if _, err := Parse([]byte(`{"policies": [{"name": "slow", "limits": {"ratePerSecond": 1e-11}}]}`)); err == nil {
		t.Fatal("a rate of 1e-11 per second must be rejected")
	}
	set, err := Parse([]byte(`{"policies": [{"name": "slow", "limits": {"ratePerSecond": 0.00001}}]}`))
	if err != nil {
		t.Fatalf("one request in about a day must be accepted: %v", err)
	}
	if got := ToLimits(set.Resolve(nil, "any.example").Limits).Interval; got <= 0 {
		t.Fatalf("interval %v", got)
	}
}

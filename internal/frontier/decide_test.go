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
	greedy := &policyv1.RunPolicy{Behaviour: &policyv1.Behaviour{MaxQueueBlock: durationpb.New(1000 * time.Hour)}}
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

package frontier

import (
	"testing"

	"google.golang.org/protobuf/proto"

	taskv1 "github.com/edevhub/crawly/internal/gen/frontier/task/v1"
	"github.com/edevhub/crawly/internal/policy"
	"github.com/edevhub/crawly/internal/store"
	"github.com/edevhub/crawly/internal/store/memstore"
)

// TestTaskLabels: a task without a label of its own takes the label of the task that
// discovered it, an empty label clears it, and a label that breaks the rules is invalid.
func TestTaskLabels(t *testing.T) {
	pol, err := policy.New(nil)
	if err != nil {
		t.Fatal(err)
	}
	s, err := New(Config{Store: memstore.New(pol), Policies: pol})
	if err != nil {
		t.Fatal(err)
	}
	for _, c := range []struct {
		name    string
		label   *string
		inherit string
		want    string
		invalid bool
	}{
		{name: "a seed without a label", want: ""},
		{name: "a seed with an empty label", label: new(""), want: ""},
		{name: "a seed with a label", label: new("news"), want: "news"},
		{name: "discovered, unset", inherit: "news", want: "news"},
		{name: "discovered, replaced", label: new("shop"), inherit: "news", want: "shop"},
		{name: "discovered, cleared", label: new(""), inherit: "news", want: ""},
		{name: "a space", label: new("a b"), invalid: true},
		{name: "too long", label: new("0123456789012345678901234567890123456789012345678901234567890123456789"), invalid: true},
		{name: "a bad label on a task that would inherit", label: new("a,b"), inherit: "news", invalid: true},
	} {
		t.Run(c.name, func(t *testing.T) {
			ts := &taskv1.TaskSpec{Request: &taskv1.Request{Url: "https://a.example/"}, Label: c.label}
			nt := s.newTasks(store.RunSpec{KeyRule: store.KeyHost}, []*taskv1.TaskSpec{ts}, c.inherit)[0]
			if nt.Invalid != c.invalid {
				t.Fatalf("invalid = %v", nt.Invalid)
			}
			if c.invalid {
				return
			}
			stored := &taskv1.TaskSpec{}
			if err := proto.Unmarshal(nt.Spec, stored); err != nil {
				t.Fatal(err)
			}
			if nt.Label != c.want || stored.GetLabel() != c.want {
				t.Errorf("label %q, stored %q, want %q", nt.Label, stored.GetLabel(), c.want)
			}
		})
	}
	// The label is not part of the identity of a task.
	a := s.newTasks(store.RunSpec{KeyRule: store.KeyHost}, []*taskv1.TaskSpec{
		{Request: &taskv1.Request{Url: "https://a.example/"}, Label: new("x")},
		{Request: &taskv1.Request{Url: "https://a.example/"}, Label: new("y")}}, "")
	if a[0].Fingerprint != a[1].Fingerprint {
		t.Error("the label changed the fingerprint")
	}
}

// The label a worker sees and the one the statistics count are the same: the stored
// column. A spec that carries a label the store does not know (written by a Frontier
// that predates labels) must not show one to the handler.
func TestAssignmentLabelFollowsStore(t *testing.T) {
	pol, err := policy.New(nil)
	if err != nil {
		t.Fatal(err)
	}
	s, err := New(Config{Store: memstore.New(pol), Policies: pol})
	if err != nil {
		t.Fatal(err)
	}
	raw, err := proto.Marshal(&taskv1.TaskSpec{Request: &taskv1.Request{Url: "https://a.example/"}, Label: new("news")})
	if err != nil {
		t.Fatal(err)
	}
	for _, label := range []string{"", "shop"} {
		a := s.assignment(store.Assignment{Spec: raw, Info: store.LeaseInfo{Label: label}})
		if got := a.GetTask().GetLabel(); got != label {
			t.Errorf("column %q: the worker saw %q", label, got)
		}
	}
}

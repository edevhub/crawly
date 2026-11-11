package pgstore

import (
	"bytes"
	"strconv"
	"testing"
	"time"

	"github.com/edevhub/crawly/internal/store"
)

func TestIDsRoundTrip(t *testing.T) {
	task := taskKey{bucket: 201, day: dayOf(time.Date(2026, 11, 11, 15, 0, 0, 0, time.UTC)), id: (&idGen{}).next()}
	got, ok := parseTask(task.ID())
	if !ok || got != task {
		t.Fatalf("task ID: got %+v %v, want %+v", got, ok, task)
	}
	if want := time.Date(2026, 11, 11, 0, 0, 0, 0, time.UTC); !task.date().Equal(want) {
		t.Fatalf("date of the task: %v, want %v", task.date(), want)
	}
	lease := leaseKey{task: task, attempt: 7, nonce: -42}
	back, ok := parseLease(lease.ID())
	if !ok || back != lease {
		t.Fatalf("lease ID: got %+v %v, want %+v", back, ok, lease)
	}
	for _, bad := range []string{"", "nope.1.x", "AAAA", string(task.ID())} {
		if _, ok := parseLease(store.LeaseID(bad)); ok {
			t.Errorf("%q was accepted as a lease ID", bad)
		}
	}
	if _, ok := parseTask(store.TaskID(lease.ID())); ok {
		t.Error("a lease ID was accepted as a task ID")
	}
}

func TestIDsIncrease(t *testing.T) {
	g := &idGen{}
	prev := g.next()
	for range 10_000 { // more than fit into one millisecond's sequence
		next := g.next()
		if bytes.Compare(next[:], prev[:]) <= 0 {
			t.Fatalf("%x does not follow %x", next, prev)
		}
		if next[6]>>4 != 7 || next[8]>>6 != 2 {
			t.Fatalf("%x is not a version 7 UUID", next)
		}
		prev = next
	}
}

func TestBucketIsStable(t *testing.T) {
	// The bucket is part of every task ID, so the hash must never change.
	for key, want := range map[store.QueueKey]uint8{"example.com": 137, "shop.example": 155, "": 153} {
		if got := bucketOf(key); got != want {
			t.Errorf("bucket of %q: %d, want %d", key, got, want)
		}
	}
	seen := map[uint8]bool{}
	for i := range 2000 {
		seen[bucketOf(store.QueueKey(strconv.Itoa(i)+".example"))] = true
	}
	if len(seen) < 200 {
		t.Errorf("2000 keys fell into %d of %d buckets", len(seen), buckets)
	}
}

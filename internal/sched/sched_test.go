package sched

import (
	"testing"
	"time"
)

var t0 = time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)

func TestAdmittedBurstAndRate(t *testing.T) {
	l := Limits{Interval: time.Second, Burst: 3}
	if got := Admitted(l, time.Time{}, t0, 0); got != 3 {
		t.Fatalf("fresh queue admits the burst: got %d", got)
	}
	tat := Advance(l, time.Time{}, t0, 3)
	if got := Admitted(l, tat, t0, 0); got != 0 {
		t.Fatalf("burst used: got %d", got)
	}
	if got := Admitted(l, tat, t0.Add(time.Second), 0); got != 1 {
		t.Fatalf("one interval later admits one: got %d", got)
	}
	if e := EligibleAt(l, tat); !e.Equal(t0.Add(time.Second)) {
		t.Fatalf("eligible at %v", e)
	}
}

func TestAdmittedConcurrency(t *testing.T) {
	l := Limits{MaxConcurrency: 2}
	if got := Admitted(l, time.Time{}, t0, 1); got != 1 {
		t.Fatalf("got %d", got)
	}
	if got := Admitted(l, time.Time{}, t0, 2); got != 0 {
		t.Fatalf("got %d", got)
	}
	if got := Admitted(Limits{}, time.Time{}, t0, 100); got != Unlimited {
		t.Fatalf("got %d", got)
	}
}

func TestAdvanceCappedWaitsOneInterval(t *testing.T) {
	l := Limits{Interval: time.Second, Burst: 1}
	tat := time.Time{}
	now := t0
	// An hour of override traffic at ten times the queue's rate.
	for range 36000 {
		tat = AdvanceCapped(l, tat, now, 1)
		now = now.Add(100 * time.Millisecond)
	}
	if wait := EligibleAt(l, tat).Sub(now); wait > time.Second {
		t.Fatalf("other runs wait %v after the override stops, want <= 1s", wait)
	}
}

func TestFairnessNoBankedCredit(t *testing.T) {
	var vclock, a, b float64
	// A runs alone for a long time.
	for range 10000 {
		a, vclock = Charge(Start(a, vclock), vclock, 1, 1)
	}
	// B becomes eligible: both compete, B must get about half, not everything.
	gotA, gotB := 0, 0
	for range 1000 {
		if Start(b, vclock) < Start(a, vclock) {
			b, vclock = Charge(Start(b, vclock), vclock, 1, 1)
			gotB++
		} else {
			a, vclock = Charge(Start(a, vclock), vclock, 1, 1)
			gotA++
		}
	}
	if gotB < 450 || gotB > 550 {
		t.Fatalf("B got %d of 1000, A got %d", gotB, gotA)
	}
}

func TestFairnessWeights(t *testing.T) {
	var vclock, a, b float64
	gotA, gotB := 0, 0
	for range 900 {
		if Start(b, vclock) < Start(a, vclock) {
			b, vclock = Charge(Start(b, vclock), vclock, 1, 1)
			gotB++
		} else {
			a, vclock = Charge(Start(a, vclock), vclock, 2, 1)
			gotA++
		}
	}
	if gotA < 580 || gotA > 620 {
		t.Fatalf("weight 2 got %d of 900, weight 1 got %d", gotA, gotB)
	}
}

func TestRefusalPeriod(t *testing.T) {
	if got := RefusalPeriod(Limits{Interval: time.Second}); got != 100*time.Millisecond {
		t.Fatalf("got %v", got)
	}
	if got := RefusalPeriod(Limits{Interval: 200 * time.Millisecond}); got != 50*time.Millisecond {
		t.Fatalf("got %v", got)
	}
}

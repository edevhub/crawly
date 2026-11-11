package sched

import (
	"testing"
	"time"
)

var aimd = Adaptive{Enabled: true, MaxInterval: 8 * time.Second, Decrease: 0.5, Recovery: 0.25, MaxOutage: 10 * time.Minute}

func TestApply(t *testing.T) {
	base := Limits{Interval: time.Second, Burst: 3, MaxConcurrency: 4}
	cases := []struct {
		name string
		a    Adaptive
		l    Limits
		s    Strain
		want Limits
	}{
		{"not slowed", aimd, base, Strain{}, base},
		{"slowed to half: half the concurrency and burst", aimd, base, Strain{Interval: 2 * time.Second},
			Limits{Interval: 2 * time.Second, Burst: 1, MaxConcurrency: 2}},
		{"near the ceiling: one at a time", aimd, base, Strain{Interval: 8 * time.Second},
			Limits{Interval: 8 * time.Second, Burst: 1, MaxConcurrency: 1}},
		{"proportional, rounded up", aimd, Limits{Interval: time.Second, Burst: 8, MaxConcurrency: 20}, Strain{Interval: 4 * time.Second},
			Limits{Interval: 4 * time.Second, Burst: 2, MaxConcurrency: 5}},
		{"no concurrency limit stays none", aimd, Limits{Interval: time.Second, Burst: 1}, Strain{Interval: 4 * time.Second},
			Limits{Interval: 4 * time.Second, Burst: 1}},
		{"limits slower than the queue are left alone", aimd, Limits{Interval: 10 * time.Second, Burst: 1}, Strain{Interval: 4 * time.Second},
			Limits{Interval: 10 * time.Second, Burst: 1}},
		{"off", Adaptive{}, base, Strain{Interval: 4 * time.Second}, base},
	}
	for _, c := range cases {
		if got := c.a.Apply(c.l, c.s); got != c.want {
			t.Errorf("%s: %+v, want %+v", c.name, got, c.want)
		}
	}
}

func TestStrainedHalvesTheRateUpToTheCeiling(t *testing.T) {
	s := Strain{}
	for i, want := range []time.Duration{2 * time.Second, 4 * time.Second, 8 * time.Second, 8 * time.Second} {
		now := t0.Add(time.Duration(i+1) * time.Minute)
		s = aimd.Strained(time.Second, s, now, now)
		if s.Interval != want {
			t.Fatalf("interval %v, want %v", s.Interval, want)
		}
	}
	if s.Steps != 3 {
		t.Fatalf("steps %d: the step at the ceiling is not one", s.Steps)
	}
	// A queue whose own interval is above the ceiling is never slowed.
	if got := aimd.Strained(time.Minute, Strain{}, t0, t0); got.Interval != 0 || got.Steps != 0 || !got.Last.Equal(t0) {
		t.Fatalf("slowed a queue that is slower than the ceiling: %+v", got)
	}
	// No rate limit, nothing to slow.
	if got := aimd.Strained(0, Strain{}, t0, t0); got != (Strain{}) {
		t.Fatalf("slowed a queue without a rate limit: %+v", got)
	}
}

func TestStrainedOncePerEpoch(t *testing.T) {
	s := aimd.Strained(time.Second, Strain{}, t0.Add(time.Second), t0)
	if s.Steps != 1 {
		t.Fatalf("first: %+v", s)
	}
	// An attempt dispatched before the step says nothing about the pace the step set.
	again := aimd.Strained(time.Second, s, t0.Add(2*time.Second), t0.Add(500*time.Millisecond))
	if again.Interval != s.Interval || again.Steps != 1 || !again.Last.Equal(t0.Add(2*time.Second)) {
		t.Fatalf("a second step for the same epoch: %+v", again)
	}
	// One dispatched after the step is a new step.
	next := aimd.Strained(time.Second, s, t0.Add(5*time.Second), t0.Add(3*time.Second))
	if next.Steps != 2 || next.Interval != 4*time.Second {
		t.Fatalf("next epoch: %+v", next)
	}
}

func TestAnsweredRaisesTheRateLinearly(t *testing.T) {
	s := Strain{Interval: 8 * time.Second}
	// 1/8 + 1/4 = 3/8, then 5/8, 7/8, 9/8 requests a second: back at the policy's rate.
	for i, want := range []time.Duration{2666666666, 1600000000, 1142857142, 0} {
		now := t0.Add(time.Duration(i+1) * time.Minute)
		s = aimd.Answered(time.Second, s, now, now)
		if d := s.Interval - want; d < -time.Millisecond || d > time.Millisecond {
			t.Fatalf("step %d: interval %v, want %v", i, s.Interval, want)
		}
	}
	if got := aimd.Answered(time.Second, Strain{}, t0, t0); got != (Strain{}) {
		t.Fatalf("a queue at its rate changed: %+v", got)
	}
	slowed := Strain{Interval: 4 * time.Second, At: t0.Add(time.Minute)}
	if got := aimd.Answered(time.Second, slowed, t0.Add(2*time.Minute), t0); got != slowed {
		t.Fatalf("a success from before the last step recovered: %+v", got)
	}
}

func TestCeilingAndOutage(t *testing.T) {
	s := aimd.Strained(time.Second, Strain{}, t0, t0)
	s = aimd.Strained(time.Second, s, t0.Add(time.Minute), t0.Add(time.Minute))
	if !s.CeilingAt.IsZero() || aimd.Outage(s, t0.Add(time.Hour)) {
		t.Fatalf("not at the ceiling yet: %+v", s)
	}
	ceiling := t0.Add(2 * time.Minute)
	s = aimd.Strained(time.Second, s, ceiling, ceiling)
	if s.Interval != 8*time.Second || !s.CeilingAt.Equal(ceiling) {
		t.Fatalf("at the ceiling: %+v", s)
	}
	// Failures at the ceiling do not move the time it was reached.
	s = aimd.Strained(time.Second, s, ceiling.Add(time.Minute), ceiling.Add(time.Minute))
	if !s.CeilingAt.Equal(ceiling) {
		t.Fatalf("ceiling moved: %+v", s)
	}
	if aimd.Outage(s, ceiling.Add(9*time.Minute)) || !aimd.Outage(s, ceiling.Add(10*time.Minute)) {
		t.Fatal("an outage is MaxOutage at the ceiling")
	}
	// A recovery step leaves the ceiling.
	if got := aimd.Answered(time.Second, s, ceiling.Add(time.Hour), ceiling.Add(time.Hour)); !got.CeilingAt.IsZero() || aimd.Outage(got, ceiling.Add(2*time.Hour)) {
		t.Fatalf("recovered, yet in an outage: %+v", got)
	}
}

func TestDecayed(t *testing.T) {
	s := Strain{Interval: 4 * time.Second, At: t0, Last: t0.Add(5 * time.Minute)}
	if got := aimd.Decayed(s, t0.Add(14*time.Minute)); got.Interval != s.Interval {
		t.Fatalf("a failure nine minutes ago, yet forgotten: %+v", got)
	}
	if got := aimd.Decayed(s, t0.Add(15*time.Minute)); got.Interval != 0 || !got.CeilingAt.IsZero() {
		t.Fatalf("idle for MaxOutage, yet remembered: %+v", got)
	}
	if got := aimd.Decayed(Strain{}, t0.Add(time.Hour)); got != (Strain{}) {
		t.Fatalf("nothing to forget: %+v", got)
	}
}

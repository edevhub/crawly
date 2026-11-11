package sched

import (
	"math"
	"time"
)

// Adaptive is the AIMD pacing of a queue (ADR 0001, adaptive pacing). The zero value is
// off.
type Adaptive struct {
	Enabled     bool
	MaxInterval time.Duration // the slowest interval the queue is slowed to
	Decrease    float64       // factor the rate is multiplied by on a strain step, in (0, 1)
	Recovery    float64       // share of the policy's rate added back on a recovery step, in (0, 1]
	// MaxOutage is how long the queue may sit at its ceiling, and how long it may sit
	// idle, before the slowdown stops being extended to tasks (Outage) or is forgotten
	// (Decayed).
	MaxOutage time.Duration
}

// Strain is the adaptive state of one queue. The zero value is a queue at its policy's
// rate. It is kept with the queue's GCRA timestamp and changes in the same transaction
// as the outcome that moves it.
type Strain struct {
	Interval  time.Duration // the interval the queue is slowed to; 0: not slowed
	At        time.Time     // the last step in either direction: the start of the epoch
	CeilingAt time.Time     // since when the interval is at its ceiling; zero: it is not
	Steps     uint64        // strain steps taken
	Last      time.Time     // the last outcome that signalled strain, stepped or not
}

// Slowed reports whether the queue runs slower than the limits it was given.
func (a Adaptive) Slowed(l Limits, s Strain) bool {
	return a.Enabled && s.Interval > l.Interval
}

// Apply returns the limits in force for a queue in this state: the interval is the
// longer of the limits' and the queue's current one. Concurrency and burst come down in
// the same proportion as the rate (never below 1), so that a cut in rate is a cut of the
// same size for a queue that is bound by latency, and one request at a time is reached
// only near the ceiling. Limits that are slower already are left as they are. No
// concurrency limit stays none: the rate bounds it.
func (a Adaptive) Apply(l Limits, s Strain) Limits {
	if !a.Slowed(l, s) {
		return l
	}
	ratio := float64(l.Interval) / float64(s.Interval)
	out := Limits{Interval: s.Interval, Burst: max(1, int(math.Floor(float64(l.burst())*ratio)))}
	if l.MaxConcurrency > 0 {
		out.MaxConcurrency = max(1, int(math.Ceil(float64(l.MaxConcurrency)*ratio)))
	}
	return out
}

// Strained is the state after an attempt ended with a sign of strain. base is the
// queue's own interval. dispatched is when the attempt was dispatched: an attempt from
// before the last step says nothing about the pace that step set, so it takes no step.
// This makes the requests that fail together one step, whatever their number.
func (a Adaptive) Strained(base time.Duration, s Strain, now, dispatched time.Time) Strain {
	if !a.Enabled || base <= 0 {
		return s
	}
	s.Last = now
	if dispatched.Before(s.At) {
		return s
	}
	cur := s.Interval
	if cur <= 0 {
		cur = base
	}
	ceiling := max(a.MaxInterval, base)
	next := ceiling
	if f := float64(cur) / a.Decrease; f < float64(ceiling) {
		next = time.Duration(f)
	}
	if next <= cur {
		return s // at the ceiling already
	}
	s.Interval, s.At, s.Steps = next, now, s.Steps+1
	if next >= ceiling {
		s.CeilingAt = now
	}
	return s
}

// Answered is the state after an attempt ended with an answer from the site that is no
// sign of strain, with the same rule for dispatched. The rate grows by Recovery of the
// base rate per step, so the way back is linear where the way down is geometric.
func (a Adaptive) Answered(base time.Duration, s Strain, now, dispatched time.Time) Strain {
	if !a.Enabled || s.Interval <= 0 || dispatched.Before(s.At) {
		return s
	}
	cur, b := float64(s.Interval), float64(base)
	next := time.Duration(cur * b / (b + a.Recovery*cur)) // in rates: 1/next = 1/cur + Recovery/base
	s.At, s.CeilingAt = now, time.Time{}
	if next <= base {
		next = 0
	}
	s.Interval = next
	return s
}

// Outage reports whether the queue has been at its ceiling for MaxOutage: the site is
// down, not struggling. A task retried only because of the extended strain budget is
// then failed instead.
func (a Adaptive) Outage(s Strain, now time.Time) bool {
	return a.Enabled && s.Interval > 0 && !s.CeilingAt.IsZero() && now.Sub(s.CeilingAt) >= a.MaxOutage
}

// Decayed forgets a slowdown that nothing has touched for MaxOutage: neither a step
// nor an outcome that signalled strain. The site has had time to recover, and a queue
// that sits idle must not start its next run at the pace of the last outage.
func (a Adaptive) Decayed(s Strain, now time.Time) Strain {
	if s.Interval <= 0 || a.MaxOutage <= 0 {
		return s
	}
	last := s.At
	if s.Last.After(last) {
		last = s.Last
	}
	if now.Sub(last) >= a.MaxOutage {
		s.Interval, s.At, s.CeilingAt = 0, now, time.Time{}
	}
	return s
}

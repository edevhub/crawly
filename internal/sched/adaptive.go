package sched

import "time"

// Adaptive is the AIMD pacing of a queue (ADR 0001, adaptive pacing). The zero value is
// off.
type Adaptive struct {
	Enabled     bool
	MaxInterval time.Duration // the slowest interval the queue is slowed to
	Decrease    float64       // factor the rate is multiplied by on a strain step, in (0, 1)
	Recovery    float64       // share of the policy's rate added back on a recovery step, in (0, 1]
}

// Strain is the adaptive state of one queue. The zero value is a queue at its policy's
// rate. It is kept with the queue's GCRA timestamp and changes in the same transaction
// as the outcome that moves it.
type Strain struct {
	Interval time.Duration // the interval the queue is slowed to; 0: not slowed
	At       time.Time     // the last step in either direction: the start of the epoch
	Steps    uint64        // strain steps taken
	Last     time.Time     // the last outcome that signalled strain, stepped or not
}

// Apply returns the limits in force for a queue in this state: the interval is the
// longer of the limits' and the queue's current one, and while it is the queue's, one
// request at a time and no burst. Limits that are slower already are left as they are.
func (a Adaptive) Apply(l Limits, s Strain) Limits {
	if !a.Enabled || s.Interval <= l.Interval {
		return l
	}
	return Limits{Interval: s.Interval, Burst: 1, MaxConcurrency: 1}
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
	return s
}

// Succeeded is the state after an attempt ended without a sign of strain, with the same
// rule for dispatched. The rate grows by Recovery of the base rate per step, so the way
// back is linear where the way down is geometric.
func (a Adaptive) Succeeded(base time.Duration, s Strain, now, dispatched time.Time) Strain {
	if !a.Enabled || s.Interval <= 0 || dispatched.Before(s.At) {
		return s
	}
	cur, b := float64(s.Interval), float64(base)
	next := time.Duration(cur * b / (b + a.Recovery*cur)) // in rates: 1/next = 1/cur + Recovery/base
	s.At = now
	if next <= base {
		next = 0
	}
	s.Interval = next
	return s
}

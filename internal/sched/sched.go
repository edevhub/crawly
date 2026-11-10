// Package sched holds the scheduling arithmetic of ADR 0001: GCRA politeness and
// start-time fair queueing. It is pure: no clock, no state, no I/O. Every store
// implementation calls it, so the rules exist once.
package sched

import (
	"math"
	"time"
)

// Limits are politeness limits in GCRA form.
type Limits struct {
	Interval       time.Duration // T; 0 = unlimited rate
	Burst          int           // b >= 1
	MaxConcurrency int           // 0 = unlimited
}

// Unlimited is returned by Admitted when nothing bounds the count.
const Unlimited = math.MaxInt32

func (l Limits) burst() int {
	if l.Burst < 1 {
		return 1
	}
	return l.Burst
}

// tolerance is τ = (b − 1)·T.
func (l Limits) tolerance() time.Duration {
	return time.Duration(l.burst()-1) * l.Interval
}

// Admitted is how many tasks the limits admit at now, given the GCRA timestamp tat and
// the number already in flight.
func Admitted(l Limits, tat, now time.Time, inFlight int) int {
	n := Unlimited
	if l.Interval > 0 {
		base := tat
		if now.After(base) {
			base = now
		}
		room := now.Add(l.tolerance()).Sub(base)
		if room < 0 {
			n = 0
		} else {
			n = int(room/l.Interval) + 1
			if n > l.burst() {
				n = l.burst()
			}
		}
	}
	if l.MaxConcurrency > 0 {
		n = min(n, l.MaxConcurrency-inFlight)
	}
	if n < 0 {
		n = 0
	}
	return n
}

// Advance is the GCRA timestamp after dispatching n tasks at now.
func Advance(l Limits, tat, now time.Time, n int) time.Time {
	if l.Interval <= 0 || n <= 0 {
		return tat
	}
	base := tat
	if now.After(base) {
		base = now
	}
	return base.Add(time.Duration(n) * l.Interval)
}

// AdvanceCapped is Advance for traffic that was admitted under other limits (a run
// override). The cap means that once such traffic stops, runs under these limits wait
// at most one interval.
func AdvanceCapped(l Limits, tat, now time.Time, n int) time.Time {
	next := Advance(l, tat, now, n)
	if limit := now.Add(time.Duration(l.burst()) * l.Interval); next.After(limit) {
		return limit
	}
	return next
}

// EligibleAt is the earliest time the rate limit admits one more task. It says nothing
// about concurrency, which is released by an event, not by time.
func EligibleAt(l Limits, tat time.Time) time.Time {
	if l.Interval <= 0 {
		return time.Time{}
	}
	return tat.Add(-l.tolerance())
}

// Start is a backlog's start tag when it is picked: its own tag, clamped to the queue's
// clock so that a backlog that could not be picked for a while cannot bank credit.
func Start(vtime, vclock float64) float64 {
	if vclock > vtime {
		return vclock
	}
	return vtime
}

// Charge returns the backlog's tag and the queue's clock after dispatching n of its
// tasks from the given start tag. A batch of n counts as n tasks served one after the
// other: the clock moves to the start tag of the last of them. Moving it only to the
// batch's start would let one large batch leave the clock behind, and a backlog that
// becomes eligible afterwards would then take the queue over.
func Charge(start, vclock, weight float64, n int) (vtime, clock float64) {
	if weight <= 0 {
		weight = 1
	}
	if n < 1 {
		return start, vclock
	}
	clock = vclock
	if last := start + float64(n-1)/weight; last > clock {
		clock = last
	}
	return start + float64(n)/weight, clock
}

// RefusalPeriod is how long a pool with the lowest start tag keeps first refusal on a
// queue before it forfeits its turn (ADR 0001, fairness across pools).
func RefusalPeriod(queue Limits) time.Duration {
	const ceiling = 100 * time.Millisecond
	if queue.Interval > 0 {
		return min(queue.Interval/4, ceiling)
	}
	return ceiling
}

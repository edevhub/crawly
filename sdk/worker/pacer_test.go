package worker

import (
	"testing"
	"time"
)

func TestPacer(t *testing.T) {
	now := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)
	p := &pacer{every: 100 * time.Millisecond}

	if n, _ := p.take(0, now); n != 0 {
		t.Fatalf("granted %d when none was wanted", n)
	}
	// One at a time, however many are wanted, and the next after one interval.
	n, wait := p.take(5, now)
	if n != 1 || wait != 100*time.Millisecond {
		t.Fatalf("first grant: %d, then wait %v", n, wait)
	}
	if n, wait := p.take(4, now.Add(40*time.Millisecond)); n != 0 || wait != 60*time.Millisecond {
		t.Fatalf("inside the interval: %d, wait %v", n, wait)
	}
	if n, _ := p.take(4, now.Add(100*time.Millisecond)); n != 1 {
		t.Fatalf("after the interval: %d", n)
	}
	// Time that passed unused is not saved up: after a long pause still one at a time.
	later := now.Add(time.Hour)
	if n, _ := p.take(4, later); n != 1 {
		t.Fatalf("after a pause: %d", n)
	}
	if n, wait := p.take(3, later); n != 0 || wait != 100*time.Millisecond {
		t.Fatalf("right after it: %d, wait %v", n, wait)
	}

	// No pacer: everything wanted, at once.
	var none *pacer
	if n, wait := none.take(7, now); n != 7 || wait != 0 {
		t.Fatalf("without a limit: %d, wait %v", n, wait)
	}
}

package safeconv_test

import (
	"math"
	"testing"

	"github.com/edevhub/crawly/internal/safeconv"
)

func TestSaturates(t *testing.T) {
	if got := safeconv.Int32(math.MaxInt64); got != math.MaxInt32 {
		t.Errorf("Int32 high: %d", got)
	}
	if got := safeconv.Int32(math.MinInt64); got != math.MinInt32 {
		t.Errorf("Int32 low: %d", got)
	}
	if got := safeconv.Uint32(math.MaxInt64); got != math.MaxUint32 {
		t.Errorf("Uint32 high: %d", got)
	}
	if got := safeconv.Uint32(-1); got != 0 {
		t.Errorf("Uint32 low: %d", got)
	}
	if got := safeconv.Uint64(-1); got != 0 {
		t.Errorf("Uint64 low: %d", got)
	}
	if got := safeconv.Int(math.MaxUint64); got != math.MaxInt {
		t.Errorf("Int high: %d", got)
	}
}

func TestKeepsValuesInRange(t *testing.T) {
	if safeconv.Int32(-7) != -7 || safeconv.Uint32(7) != 7 || safeconv.Uint64(7) != 7 || safeconv.Int(7) != 7 {
		t.Error("an in-range value must not change")
	}
}

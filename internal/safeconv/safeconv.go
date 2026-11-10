// Package safeconv converts between integer types of different width, saturating
// instead of wrapping when the value does not fit.
package safeconv

import "math"

// Int32 converts n to int32, clamping it to the int32 range.
func Int32(n int) int32 {
	if n < math.MinInt32 {
		return math.MinInt32
	}
	if n > math.MaxInt32 {
		return math.MaxInt32
	}
	return int32(n)
}

// Uint32 converts n to uint32, clamping it to the uint32 range.
func Uint32(n int) uint32 {
	if n < 0 {
		return 0
	}
	if uint64(n) > math.MaxUint32 {
		return math.MaxUint32
	}
	return uint32(n)
}

// Uint64 converts n to uint64, mapping negative values to zero.
func Uint64(n int) uint64 {
	if n < 0 {
		return 0
	}
	return uint64(n)
}

// Int converts n to int, clamping it to the int range.
func Int(n uint64) int {
	if n > math.MaxInt {
		return math.MaxInt
	}
	return int(n)
}

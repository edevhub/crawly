package pgstore

// What the tests of the external package need to take the locks the store takes.
const LabelLockClass = lockLabels

// LockKey is the second key of a run's advisory locks.
func LockKey(run int64) int32 { return lockKey(run) }

package pgstore

import "context"

// UpTo applies the migrations up to a version, for tests that need an older schema.
func (m *Migrator) UpTo(ctx context.Context, version int64) error {
	_, err := m.p.UpTo(ctx, version)
	return err
}

// DownTo reverts the migrations above a version, for tests of the rollback.
func (m *Migrator) DownTo(ctx context.Context, version int64) error {
	_, err := m.p.DownTo(ctx, version)
	return err
}

// What the tests of the external package need to take the locks the store takes.
const LabelLockClass = lockLabels

// LockKey is the second key of a run's advisory locks.
func LockKey(run int64) int32 { return lockKey(run) }

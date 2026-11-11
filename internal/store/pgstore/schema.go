package pgstore

import (
	"context"
	"fmt"
	"strings"
	"time"

	"github.com/jackc/pgx/v5"
)

// Advisory lock classes (the first key of a two-key advisory lock).
const (
	lockRun      = 1 // (1, run_no): shared by AddTasks, exclusive for lifecycle and completion
	lockEvents   = 2 // (2, 0): run_events append, so cursors follow commit order
	lockPolicies = 3 // (3, 0): policy set activation
	lockSchema   = 4 // (4, 0): schema creation and partition changes
)

// month is the first day of t's month, in UTC.
func month(t time.Time) time.Time {
	t = t.UTC()
	return time.Date(t.Year(), t.Month(), 1, 0, 0, 0, 0, time.UTC)
}

func partition(table string, m time.Time) string {
	return fmt.Sprintf("%s_y%04dm%02d", table, m.Year(), int(m.Month()))
}

// ensureMonth creates the tasks and attempts partitions that hold runs created in t's
// month. All tasks of a run live in the partition of the run's creation date.
func (s *Store) ensureMonth(ctx context.Context, t time.Time) error {
	m := month(t)
	s.mu.Lock()
	known := s.months[m]
	s.mu.Unlock()
	if known {
		return nil
	}
	tx, err := s.db.Begin(ctx)
	if err != nil {
		return err
	}
	defer rollback(ctx, tx)
	if _, err := tx.Exec(ctx, "SELECT pg_advisory_xact_lock($1::int, 0)", lockSchema); err != nil {
		return err
	}
	from, to := m.Format(time.DateOnly), m.AddDate(0, 1, 0).Format(time.DateOnly)
	for _, stmt := range []string{
		fmt.Sprintf(`CREATE TABLE IF NOT EXISTS %s PARTITION OF tasks FOR VALUES FROM ('%s') TO ('%s')
			WITH (toast_tuple_target = 256, autovacuum_vacuum_scale_factor = 0.01)`,
			pgx.Identifier{partition("tasks", m)}.Sanitize(), from, to),
		fmt.Sprintf(`CREATE TABLE IF NOT EXISTS %s PARTITION OF attempts FOR VALUES FROM ('%s') TO ('%s')`,
			pgx.Identifier{partition("attempts", m)}.Sanitize(), from, to),
	} {
		if _, err := tx.Exec(ctx, stmt); err != nil {
			return fmt.Errorf("pgstore: creating a partition: %w", err)
		}
	}
	if err := tx.Commit(ctx); err != nil {
		return err
	}
	s.mu.Lock()
	s.months[m] = true
	s.mu.Unlock()
	return nil
}

// dropPartition drops one partition under the schema lock. The drop needs an
// exclusive lock on the parent table for a moment; the pool's lock timeout keeps it
// from queueing behind a long transaction with everything else behind it.
func (s *Store) dropPartition(ctx context.Context, name string) error {
	tx, err := s.db.Begin(ctx)
	if err != nil {
		return err
	}
	defer rollback(ctx, tx)
	if _, err := tx.Exec(ctx, "SELECT pg_advisory_xact_lock($1::int, 0)", lockSchema); err != nil {
		return err
	}
	if _, err := tx.Exec(ctx, "DROP TABLE IF EXISTS "+pgx.Identifier{name}.Sanitize()); err != nil {
		return err
	}
	return tx.Commit(ctx)
}

// dropOldPartitions drops the tasks and attempts partitions whose every run was
// created before the cut-off. A month is kept while one of its runs is open or
// sealed, or still has tasks to drain: their tasks live in the partition.
func (s *Store) dropOldPartitions(ctx context.Context, cutoff time.Time) error {
	rows, err := s.db.Query(ctx, `SELECT c.relname FROM pg_inherits i
		JOIN pg_class c ON c.oid = i.inhrelid JOIN pg_class p ON p.oid = i.inhparent
		WHERE p.oid IN (to_regclass('tasks'), to_regclass('attempts')) ORDER BY c.relname`)
	if err != nil {
		return err
	}
	names, err := pgx.CollectRows(rows, pgx.RowTo[string])
	if err != nil {
		return err
	}
	for _, name := range names {
		rest, ok := strings.CutPrefix(name, "tasks_y")
		if !ok {
			if rest, ok = strings.CutPrefix(name, "attempts_y"); !ok {
				continue
			}
		}
		m, err := time.Parse("2006m01", rest)
		if err != nil || !m.AddDate(0, 1, 0).Before(cutoff) {
			continue
		}
		var busy bool
		if err := s.db.QueryRow(ctx, `SELECT
			EXISTS (SELECT 1 FROM runs WHERE created_on >= $1 AND created_on < $2 AND state IN ('open', 'sealed'))
			OR EXISTS (SELECT 1 FROM backlogs WHERE run_created_on >= $1 AND run_created_on < $2 AND (pending > 0 OR in_flight > 0))`,
			m, m.AddDate(0, 1, 0)).Scan(&busy); err != nil {
			return err
		}
		if busy {
			continue
		}
		if err := s.dropPartition(ctx, name); err != nil {
			if pgCode(err) == codeLockTimeout {
				continue // the table is busy: the next Maintain tries again
			}
			return fmt.Errorf("pgstore: dropping partition %s: %w", name, err)
		}
		s.mu.Lock()
		delete(s.months, m)
		s.mu.Unlock()
	}
	return nil
}

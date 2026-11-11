package pgstore

import (
	"context"
	"fmt"
	"strings"
	"time"

	"github.com/jackc/pgx/v5"
)

// schemaVersion is the version this code expects. The schema is created once, by the
// first replica to connect.
const schemaVersion = 1

// schema is ADR 0002's schema.
const schema = `
CREATE TYPE run_state  AS ENUM ('open', 'sealed', 'completed', 'cancelled', 'expired');
CREATE TYPE task_state AS ENUM ('pending', 'in_flight', 'succeeded', 'failed', 'expired', 'cancelled');

CREATE TABLE runs (
  run_no      bigint      GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
  run_id      text        NOT NULL UNIQUE,
  spec_hash   bytea       NOT NULL,
  created_on  date        NOT NULL,
  labels      jsonb       NOT NULL DEFAULT '{}',
  policy      bytea       NOT NULL,
  key_rule    smallint    NOT NULL,
  deadline    timestamptz,
  max_tasks   bigint,
  max_depth   int,
  state       run_state   NOT NULL DEFAULT 'open',
  paused      boolean     NOT NULL DEFAULT false,
  resuming    boolean     NOT NULL DEFAULT false,
  created_at  timestamptz NOT NULL,
  sealed_at   timestamptz,
  finished_at timestamptz
);
CREATE INDEX runs_active   ON runs (deadline) WHERE state IN ('open', 'sealed');
CREATE INDEX runs_draining ON runs (run_no)   WHERE state IN ('cancelled', 'expired');
CREATE INDEX runs_resuming ON runs (run_no)   WHERE resuming;
CREATE INDEX runs_labels   ON runs USING gin (labels);

CREATE TABLE run_events (
  id     bigint      GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
  run_no bigint      NOT NULL REFERENCES runs ON DELETE CASCADE,
  type   smallint    NOT NULL,
  at     timestamptz NOT NULL
);
CREATE INDEX run_events_run ON run_events (run_no, id);

CREATE TABLE policy_sets (
  hash         text        PRIMARY KEY,
  first_seen   timestamptz NOT NULL,
  activated_at timestamptz NOT NULL
);

CREATE TABLE replicas (
  replica_id  text        PRIMARY KEY,
  policy_hash text        NOT NULL REFERENCES policy_sets,
  started_at  timestamptz NOT NULL,
  seen_at     timestamptz NOT NULL
);

CREATE TABLE pools (
  pool_id smallint GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
  name    text     NOT NULL UNIQUE CHECK (name ~ '^[a-z0-9_]{1,32}$')
);

CREATE TABLE queues (
  key           text             PRIMARY KEY,
  bucket        smallint         NOT NULL,
  tat           timestamptz      NOT NULL DEFAULT '-infinity',
  in_flight     int              NOT NULL DEFAULT 0,
  vclock        double precision NOT NULL DEFAULT 0,
  blocked_until timestamptz,
  block_reason  text,
  last_used_at  timestamptz      NOT NULL
) WITH (autovacuum_vacuum_scale_factor = 0.01);

CREATE TABLE backlogs (
  queue_key      text             NOT NULL,
  run_no         bigint           NOT NULL,
  pool_id        smallint         NOT NULL,
  bucket         smallint         NOT NULL,
  run_created_on date             NOT NULL,
  pending        bigint           NOT NULL DEFAULT 0,
  in_flight      int              NOT NULL DEFAULT 0,
  vtime          double precision NOT NULL DEFAULT 0,
  own_tat        timestamptz      NOT NULL DEFAULT '-infinity',
  ready_at       timestamptz,
  tasks_total    bigint           NOT NULL DEFAULT 0,
  succeeded      bigint           NOT NULL DEFAULT 0,
  failed         bigint           NOT NULL DEFAULT 0,
  expired        bigint           NOT NULL DEFAULT 0,
  cancelled      bigint           NOT NULL DEFAULT 0,
  attempts       bigint           NOT NULL DEFAULT 0,
  discovered     bigint           NOT NULL DEFAULT 0,
  duplicates     bigint           NOT NULL DEFAULT 0,
  over_budget    bigint           NOT NULL DEFAULT 0,
  queue_blocks   bigint           NOT NULL DEFAULT 0,
  PRIMARY KEY (queue_key, run_no, pool_id)
) WITH (autovacuum_vacuum_scale_factor = 0.01);
CREATE INDEX backlogs_ready  ON backlogs (pool_id, ready_at) WHERE ready_at IS NOT NULL;
CREATE INDEX backlogs_queue  ON backlogs (queue_key, run_no, pool_id) WHERE pending > 0;
CREATE INDEX backlogs_active ON backlogs (run_no) WHERE pending > 0 OR in_flight > 0;
CREATE INDEX backlogs_run    ON backlogs (run_no, queue_key);

CREATE TABLE backlog_codes (
  queue_key text   NOT NULL,
  run_no    bigint NOT NULL,
  code      text   NOT NULL,
  count     bigint NOT NULL DEFAULT 0,
  PRIMARY KEY (queue_key, run_no, code)
);
CREATE INDEX backlog_codes_run ON backlog_codes (run_no);

CREATE TABLE tasks (
  run_created_on   date        NOT NULL,
  task_id          uuid        NOT NULL,
  run_no           bigint      NOT NULL,
  queue_key        text        NOT NULL,
  pool_id          smallint    NOT NULL,
  fingerprint      bytea       NOT NULL CHECK (length(fingerprint) = 16),
  state            task_state  NOT NULL DEFAULT 'pending',
  priority         int         NOT NULL DEFAULT 0,
  not_before       timestamptz NOT NULL DEFAULT '-infinity',
  expires_at       timestamptz,
  depth            int         NOT NULL,
  parent_task_id   uuid,
  parent_bucket    smallint,
  attempt          int         NOT NULL DEFAULT 0,
  lease_nonce      bigint,
  worker_id        text,
  worker_version   text,
  dispatched_at    timestamptz,
  lease_expires_at timestamptz,
  lease_deadline   timestamptz,
  spec             bytea       NOT NULL,
  created_at       timestamptz NOT NULL,
  finished_at      timestamptz,
  PRIMARY KEY (run_created_on, task_id),
  UNIQUE (run_created_on, run_no, fingerprint)
) PARTITION BY RANGE (run_created_on);
ALTER TABLE tasks ALTER COLUMN spec SET STORAGE EXTERNAL;

CREATE INDEX tasks_claim  ON tasks (run_no, queue_key, pool_id, priority DESC, task_id)
  INCLUDE (not_before, expires_at) WHERE state = 'pending';
CREATE INDEX tasks_next   ON tasks (run_no, queue_key, pool_id, not_before) WHERE state = 'pending';
CREATE INDEX tasks_leases ON tasks (lease_expires_at) WHERE state = 'in_flight';
CREATE INDEX tasks_ttl    ON tasks (expires_at) WHERE state = 'pending' AND expires_at IS NOT NULL;
CREATE INDEX tasks_run    ON tasks (run_no, task_id);

CREATE TABLE attempts (
  run_created_on date        NOT NULL,
  task_id        uuid        NOT NULL,
  attempt        int         NOT NULL,
  run_no         bigint      NOT NULL,
  worker_id      text        NOT NULL,
  worker_version text        NOT NULL,
  policy_hash    text        NOT NULL,
  dispatched_at  timestamptz NOT NULL,
  finished_at    timestamptz NOT NULL,
  result         smallint    NOT NULL,
  kind           smallint    NOT NULL DEFAULT 0,
  code           text,
  message        text,
  http_status    int,
  final_url      text,
  latency_ms     int,
  bytes          bigint,
  response_ref   text,
  content_hash   text,
  items_emitted  int,
  PRIMARY KEY (run_created_on, task_id, attempt)
) PARTITION BY RANGE (run_created_on);
CREATE INDEX attempts_run_code ON attempts (run_no, code, finished_at);

CREATE TABLE schema_version (version int NOT NULL);
`

// Advisory lock classes (the first key of a two-key advisory lock).
const (
	lockRun      = 1 // (1, run_no): shared by AddTasks, exclusive for lifecycle and completion
	lockEvents   = 2 // (2, 0): run_events append, so cursors follow commit order
	lockPolicies = 3 // (3, 0): policy set activation
	lockSchema   = 4 // (4, 0): schema creation and partition changes
)

// migrate creates the schema if this database does not have it yet.
func (s *Store) migrate(ctx context.Context) error {
	tx, err := s.db.Begin(ctx)
	if err != nil {
		return err
	}
	defer rollback(ctx, tx)
	if _, err := tx.Exec(ctx, "SET LOCAL statement_timeout = 0; SET LOCAL lock_timeout = 0"); err != nil {
		return err
	}
	if _, err := tx.Exec(ctx, "SELECT pg_advisory_xact_lock($1::int, 0)", lockSchema); err != nil {
		return err
	}
	var exists bool
	if err := tx.QueryRow(ctx, "SELECT to_regclass('schema_version') IS NOT NULL").Scan(&exists); err != nil {
		return err
	}
	if exists {
		var have int
		if err := tx.QueryRow(ctx, "SELECT coalesce(max(version), 0) FROM schema_version").Scan(&have); err != nil {
			return err
		}
		if have != schemaVersion {
			return fmt.Errorf("pgstore: database has schema version %d, this build needs %d", have, schemaVersion)
		}
		return tx.Commit(ctx)
	}
	if _, err := tx.Exec(ctx, schema); err != nil {
		return fmt.Errorf("pgstore: creating the schema: %w", err)
	}
	if _, err := tx.Exec(ctx, "INSERT INTO schema_version (version) VALUES ($1)", schemaVersion); err != nil {
		return err
	}
	return tx.Commit(ctx)
}

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
		if _, err := s.db.Exec(ctx, "DROP TABLE IF EXISTS "+pgx.Identifier{name}.Sanitize()); err != nil {
			return fmt.Errorf("pgstore: dropping partition %s: %w", name, err)
		}
		s.mu.Lock()
		delete(s.months, m)
		s.mu.Unlock()
	}
	return nil
}

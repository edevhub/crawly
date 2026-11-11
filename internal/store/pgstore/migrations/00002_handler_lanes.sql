-- Dispatch by handler (ADR 0003). A task names its handler, and the readiness of a
-- backlog is split by handler into lanes, so that a session that can run some handlers
-- finds work without passing over what it cannot run.
--
-- This migration only adds (ADR 0002, "Migrations"): the build before it keeps working
-- on the migrated schema, so a rolling update can follow the migration. It reads and
-- writes backlogs.ready_at and the old tasks_claim and tasks_next, so they stay, and
-- two triggers keep backlogs.ready_at and the lanes in step:
--   - a write to lanes sets backlogs.ready_at to the earliest readiness of the
--     backlog's lanes (early is allowed, late is not);
--   - a write to backlogs.ready_at by the build before makes the backlog's lanes
--     ready no later than that, and the lane of tasks of unknown handler exists.
-- A later migration drops the column, the index and the triggers once no build before
-- this one runs. The indexes on tasks for the new column are built per partition, with
-- no lock that stops writes, by the migrator after this file (migrate.go).
--
-- Tasks that exist now have no known handler: '' stands for "unknown", and every
-- session may try such a task.

-- +goose Up
SET LOCAL lock_timeout = '5s';

-- One row per handler that has tasks in a backlog. ready_at is what backlogs.ready_at
-- was, for the tasks of that handler.
CREATE TABLE lanes (
  queue_key text        NOT NULL,
  run_no    bigint      NOT NULL,
  pool_id   smallint    NOT NULL,
  handler   text        NOT NULL,
  ready_at  timestamptz,
  PRIMARY KEY (queue_key, run_no, pool_id, handler)
) WITH (autovacuum_vacuum_scale_factor = 0.01);
-- queue_key and run_no close the index so that a scan of one handler's lanes can resume
-- from a position.
CREATE INDEX lanes_ready      ON lanes (pool_id, handler, ready_at, queue_key, run_no) WHERE ready_at IS NOT NULL;
CREATE INDEX lanes_ready_pool ON lanes (pool_id, ready_at) WHERE ready_at IS NOT NULL;
CREATE INDEX lanes_run        ON lanes (run_no, queue_key, pool_id, handler);

-- +goose StatementBegin
CREATE FUNCTION crawly_backlog_to_lanes() RETURNS trigger LANGUAGE plpgsql AS $$
BEGIN
  IF NEW.ready_at IS NULL THEN
    UPDATE lanes SET ready_at = NULL
      WHERE queue_key = NEW.queue_key AND run_no = NEW.run_no AND pool_id = NEW.pool_id AND ready_at IS NOT NULL;
  ELSIF NEW.ready_at < 'infinity' THEN
    UPDATE lanes SET ready_at = least(ready_at, NEW.ready_at)
      WHERE queue_key = NEW.queue_key AND run_no = NEW.run_no AND pool_id = NEW.pool_id;
    INSERT INTO lanes (queue_key, run_no, pool_id, handler, ready_at)
      VALUES (NEW.queue_key, NEW.run_no, NEW.pool_id, '', NEW.ready_at)
      ON CONFLICT (queue_key, run_no, pool_id, handler) DO UPDATE SET ready_at = least(lanes.ready_at, EXCLUDED.ready_at);
  END IF;
  RETURN NULL;
END $$;
-- +goose StatementEnd

-- +goose StatementBegin
CREATE FUNCTION crawly_lanes_to_backlog() RETURNS trigger LANGUAGE plpgsql AS $$
DECLARE k record;
BEGIN
  IF TG_OP = 'DELETE' THEN k := OLD; ELSE k := NEW; END IF;
  UPDATE backlogs b SET ready_at = m.r
    FROM (SELECT min(l.ready_at) AS r FROM lanes l
            WHERE l.queue_key = k.queue_key AND l.run_no = k.run_no AND l.pool_id = k.pool_id) m
    WHERE b.queue_key = k.queue_key AND b.run_no = k.run_no AND b.pool_id = k.pool_id
      AND b.ready_at IS DISTINCT FROM m.r;
  RETURN NULL;
END $$;
-- +goose StatementEnd

-- The first trigger is created first: it takes a lock that waits for the writers of
-- backlogs, so no write by the build before slips in between the copy below and the
-- trigger. Only a write that is not the other trigger's (depth 0) is copied.
CREATE TRIGGER backlog_ready_insert AFTER INSERT ON backlogs
  FOR EACH ROW WHEN (pg_trigger_depth() = 0 AND NEW.ready_at IS NOT NULL) EXECUTE FUNCTION crawly_backlog_to_lanes();
CREATE TRIGGER backlog_ready_update AFTER UPDATE OF ready_at ON backlogs
  FOR EACH ROW WHEN (pg_trigger_depth() = 0 AND NEW.ready_at IS DISTINCT FROM OLD.ready_at)
  EXECUTE FUNCTION crawly_backlog_to_lanes();

INSERT INTO lanes (queue_key, run_no, pool_id, handler, ready_at)
  SELECT queue_key, run_no, pool_id, '', ready_at FROM backlogs WHERE pending > 0;

CREATE TRIGGER lanes_ready_sync AFTER INSERT OR DELETE OR UPDATE OF ready_at ON lanes
  FOR EACH ROW EXECUTE FUNCTION crawly_lanes_to_backlog();

-- The handlers the replicas have sessions for, as the replicas last reported them. The
-- handler '' is a session that names no handlers: it serves every handler of the pool.
CREATE TABLE pool_handlers (
  pool    text        NOT NULL,
  handler text        NOT NULL,
  seen_at timestamptz NOT NULL,
  PRIMARY KEY (pool, handler)
);

-- One row: when the sweep of tasks without a handler last ran (so that one replica
-- at a time does it), where it stopped, and when it last reported.
CREATE TABLE unserved_sweep (
  id          int         PRIMARY KEY CHECK (id = 1),
  swept_at    timestamptz NOT NULL DEFAULT 'epoch',
  reported_at timestamptz NOT NULL DEFAULT 'epoch',
  cur_pool    smallint    NOT NULL DEFAULT 0,
  cur_handler text        NOT NULL DEFAULT '',
  cur_ready   timestamptz NOT NULL DEFAULT 'epoch',
  cur_queue   text        NOT NULL DEFAULT '',
  cur_run     bigint      NOT NULL DEFAULT 0
);
INSERT INTO unserved_sweep (id) VALUES (1);

-- The statements that lock tasks come last, so that its lock is held for the shortest
-- time: a lock taken by a transaction lasts until it commits.
ALTER TABLE tasks ADD COLUMN handler text NOT NULL DEFAULT '';

-- ON ONLY: the parent index is valid once every partition has its index attached, and a
-- partition created from now on gets its own. The existing ones are built concurrently.
CREATE INDEX tasks_claim_h ON ONLY tasks (run_no, queue_key, pool_id, handler, priority DESC, task_id)
  INCLUDE (not_before, expires_at) WHERE state = 'pending';
CREATE INDEX tasks_next_h ON ONLY tasks (run_no, queue_key, pool_id, handler, not_before) WHERE state = 'pending';

-- +goose Down
-- Rolling back: stop the replicas of this build, run this, start the build before. The
-- build before works on the schema as soon as the replicas of this one are gone, and
-- backlogs.ready_at is current. Tasks keep no memory of their handler.
SET LOCAL lock_timeout = '5s';
DROP TABLE IF EXISTS unserved_sweep;
DROP TABLE IF EXISTS pool_handlers;
DROP TRIGGER IF EXISTS lanes_ready_sync ON lanes;
DROP TRIGGER IF EXISTS backlog_ready_update ON backlogs;
DROP TRIGGER IF EXISTS backlog_ready_insert ON backlogs;
DROP FUNCTION IF EXISTS crawly_lanes_to_backlog();
DROP FUNCTION IF EXISTS crawly_backlog_to_lanes();
DROP TABLE IF EXISTS lanes;
DROP INDEX IF EXISTS tasks_claim_h;
DROP INDEX IF EXISTS tasks_next_h;
ALTER TABLE tasks DROP COLUMN handler;

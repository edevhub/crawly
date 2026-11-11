-- Dispatch by handler (ADR 0003). A task names its handler, and the readiness index of
-- a backlog is split by handler into lanes, so that a session that can run some
-- handlers finds work without passing over what it cannot run.
--
-- Tasks that exist now have no known handler: '' stands for "unknown", and every
-- session may try such a task.

-- +goose Up
ALTER TABLE tasks ADD COLUMN handler text NOT NULL DEFAULT '';

DROP INDEX tasks_claim;
DROP INDEX tasks_next;
CREATE INDEX tasks_claim ON tasks (run_no, queue_key, pool_id, handler, priority DESC, task_id)
  INCLUDE (not_before, expires_at) WHERE state = 'pending';
CREATE INDEX tasks_next  ON tasks (run_no, queue_key, pool_id, handler, not_before) WHERE state = 'pending';

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
CREATE INDEX lanes_ready      ON lanes (pool_id, handler, ready_at) WHERE ready_at IS NOT NULL;
CREATE INDEX lanes_ready_pool ON lanes (pool_id, ready_at) WHERE ready_at IS NOT NULL;
CREATE INDEX lanes_run        ON lanes (run_no, queue_key, pool_id, handler);

INSERT INTO lanes (queue_key, run_no, pool_id, handler, ready_at)
  SELECT queue_key, run_no, pool_id, '', ready_at FROM backlogs WHERE pending > 0;

DROP INDEX backlogs_ready;
ALTER TABLE backlogs DROP COLUMN ready_at;

-- The handlers the replicas have sessions for, as the replicas last reported them.
CREATE TABLE pool_handlers (
  pool    text        NOT NULL,
  handler text        NOT NULL,
  seen_at timestamptz NOT NULL,
  PRIMARY KEY (pool, handler)
);

-- +goose Down
DROP TABLE IF EXISTS pool_handlers;
ALTER TABLE backlogs ADD COLUMN ready_at timestamptz;
UPDATE backlogs b SET ready_at = (SELECT min(l.ready_at) FROM lanes l
  WHERE l.queue_key = b.queue_key AND l.run_no = b.run_no AND l.pool_id = b.pool_id);
CREATE INDEX backlogs_ready ON backlogs (pool_id, ready_at) WHERE ready_at IS NOT NULL;
DROP TABLE IF EXISTS lanes;
DROP INDEX tasks_claim;
DROP INDEX tasks_next;
CREATE INDEX tasks_claim ON tasks (run_no, queue_key, pool_id, priority DESC, task_id)
  INCLUDE (not_before, expires_at) WHERE state = 'pending';
CREATE INDEX tasks_next  ON tasks (run_no, queue_key, pool_id, not_before) WHERE state = 'pending';
ALTER TABLE tasks DROP COLUMN handler;

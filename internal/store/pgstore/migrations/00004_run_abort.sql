-- Abort rules: a run that a rule of its limits ends has the state 'aborted', and keeps the
-- rule, the code and the task that tripped it. The counters are those of the rules.
--
-- A new enum value cannot be used in the transaction that adds it, and the index of
-- draining runs names it, so this migration runs outside a transaction. Every statement
-- can be run again. The migrator sets a lock timeout, so a statement that needs a lock
-- the live Frontier holds fails and is run again, instead of queueing every query behind
-- it. The new index is built without blocking writes, and the old one stays until it is
-- ready.
--
-- Replicas of earlier builds treat an 'aborted' run as live. The column
-- replicas.abort_rules is how the Frontier knows whether any such replica is running: it
-- accepts abort rules at CreateRun only when none is. Rolling back to an earlier build
-- needs the Down step first, which turns aborted runs into cancelled ones.

-- +goose NO TRANSACTION
-- +goose Up
ALTER TYPE run_state ADD VALUE IF NOT EXISTS 'aborted';

ALTER TABLE runs
  ADD COLUMN IF NOT EXISTS abort_codes text[],
  ADD COLUMN IF NOT EXISTS max_failed bigint,
  ADD COLUMN IF NOT EXISTS max_consecutive_failures int,
  ADD COLUMN IF NOT EXISTS failed_count bigint NOT NULL DEFAULT 0,
  ADD COLUMN IF NOT EXISTS failed_streak bigint NOT NULL DEFAULT 0,
  ADD COLUMN IF NOT EXISTS abort_rule smallint,
  ADD COLUMN IF NOT EXISTS abort_code text,
  ADD COLUMN IF NOT EXISTS abort_task text;

-- The counters are updated on every final failure of a run with rules: leave room on the
-- page for the update to stay in it.
ALTER TABLE runs SET (fillfactor = 70);

ALTER TABLE replicas ADD COLUMN IF NOT EXISTS abort_rules boolean NOT NULL DEFAULT false;

DROP INDEX CONCURRENTLY IF EXISTS runs_draining_v2; -- what an interrupted build left
CREATE INDEX CONCURRENTLY runs_draining_v2 ON runs (run_no) WHERE state IN ('cancelled', 'expired', 'aborted');
DROP INDEX CONCURRENTLY IF EXISTS runs_draining;
ALTER INDEX IF EXISTS runs_draining_v2 RENAME TO runs_draining;

-- +goose Down
-- Run this before the Frontier goes back to a build without abort rules: that build
-- takes an 'aborted' run for a live one and would crawl it to its end. The enum value
-- stays, as PostgreSQL cannot remove one.
UPDATE runs SET state = 'cancelled' WHERE state = 'aborted';
ALTER TABLE runs
  DROP COLUMN IF EXISTS abort_codes,
  DROP COLUMN IF EXISTS max_failed,
  DROP COLUMN IF EXISTS max_consecutive_failures,
  DROP COLUMN IF EXISTS failed_count,
  DROP COLUMN IF EXISTS failed_streak,
  DROP COLUMN IF EXISTS abort_rule,
  DROP COLUMN IF EXISTS abort_code,
  DROP COLUMN IF EXISTS abort_task;
ALTER TABLE replicas DROP COLUMN IF EXISTS abort_rules;
DROP INDEX CONCURRENTLY IF EXISTS runs_draining_v2; -- what an interrupted build left
CREATE INDEX CONCURRENTLY runs_draining_v2 ON runs (run_no) WHERE state IN ('cancelled', 'expired');
DROP INDEX CONCURRENTLY IF EXISTS runs_draining;
ALTER INDEX IF EXISTS runs_draining_v2 RENAME TO runs_draining;

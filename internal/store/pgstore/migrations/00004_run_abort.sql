-- Abort rules: a run that a rule of its limits ends has the state 'aborted', and keeps the
-- rule, the code and the task that tripped it. The counters are those of the rules.
--
-- A new enum value cannot be used in the transaction that adds it, and the index of
-- draining runs names it, so this migration runs outside a transaction. Every statement
-- can be run again.

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

DROP INDEX IF EXISTS runs_draining;
CREATE INDEX runs_draining ON runs (run_no) WHERE state IN ('cancelled', 'expired', 'aborted');

-- +goose Down
-- The enum value stays: PostgreSQL cannot remove one. Runs in that state keep it.
DROP INDEX IF EXISTS runs_draining;
CREATE INDEX runs_draining ON runs (run_no) WHERE state IN ('cancelled', 'expired');
ALTER TABLE runs
  DROP COLUMN IF EXISTS abort_codes,
  DROP COLUMN IF EXISTS max_failed,
  DROP COLUMN IF EXISTS max_consecutive_failures,
  DROP COLUMN IF EXISTS failed_count,
  DROP COLUMN IF EXISTS failed_streak,
  DROP COLUMN IF EXISTS abort_rule,
  DROP COLUMN IF EXISTS abort_code,
  DROP COLUMN IF EXISTS abort_task;

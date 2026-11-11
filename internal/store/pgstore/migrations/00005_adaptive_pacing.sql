-- +goose Up
-- Adaptive pacing of a queue (ADR 0001): the interval the queue is slowed to and the
-- bookkeeping of its steps. They live in the queue row, with the GCRA timestamp.
ALTER TABLE queues
  ADD COLUMN slow_interval_ns bigint      NOT NULL DEFAULT 0,
  ADD COLUMN strain_at        timestamptz,
  ADD COLUMN strain_steps     bigint      NOT NULL DEFAULT 0,
  ADD COLUMN last_strain_at   timestamptz;

-- +goose Down
ALTER TABLE queues
  DROP COLUMN slow_interval_ns,
  DROP COLUMN strain_at,
  DROP COLUMN strain_steps,
  DROP COLUMN last_strain_at;

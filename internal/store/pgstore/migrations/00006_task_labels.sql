-- Task labels: a second dimension of the run statistics. run_labels is the registry of
-- the labels a run has accepted, which bounds their number. backlog_labels holds the
-- counters, kept per queue like the other counters of a backlog and summed on read.

-- +goose Up
-- The ALTER needs a lock on tasks and every partition. If a long query holds one, give up
-- instead of queueing the whole Frontier behind it: run the migration again later.
SET LOCAL lock_timeout = '5s';

ALTER TABLE runs ADD COLUMN invalid bigint NOT NULL DEFAULT 0;
ALTER TABLE tasks ADD COLUMN label text NOT NULL DEFAULT '';

CREATE TABLE run_labels (
  run_no bigint NOT NULL REFERENCES runs ON DELETE CASCADE,
  label  text   NOT NULL,
  PRIMARY KEY (run_no, label)
);

CREATE TABLE backlog_labels (
  queue_key    text   NOT NULL,
  run_no       bigint NOT NULL,
  label        text   NOT NULL,
  succeeded    bigint NOT NULL DEFAULT 0,
  failed       bigint NOT NULL DEFAULT 0,
  expired      bigint NOT NULL DEFAULT 0,
  cancelled    bigint NOT NULL DEFAULT 0,
  failed_codes jsonb  NOT NULL DEFAULT '{}',
  PRIMARY KEY (queue_key, run_no, label)
);
CREATE INDEX backlog_labels_run ON backlog_labels (run_no);

-- +goose Down
DROP TABLE IF EXISTS backlog_labels;
DROP TABLE IF EXISTS run_labels;
ALTER TABLE tasks DROP COLUMN IF EXISTS label;
ALTER TABLE runs DROP COLUMN IF EXISTS invalid;

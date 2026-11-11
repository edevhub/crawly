-- Task labels: a second dimension of the run statistics. run_labels is the registry of
-- the labels a run has accepted, which bounds their number. backlog_labels holds the
-- counters, kept per queue like the other counters of a backlog and summed on read.

-- +goose Up
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

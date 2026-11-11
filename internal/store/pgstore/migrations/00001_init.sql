-- The schema of ADR 0002. The monthly partitions of tasks and attempts are not part of
-- a migration: the store creates and drops them as time passes.

-- +goose Up
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

-- +goose Down
DROP TABLE IF EXISTS attempts CASCADE;
DROP TABLE IF EXISTS tasks CASCADE;
DROP TABLE IF EXISTS backlog_codes CASCADE;
DROP TABLE IF EXISTS backlogs CASCADE;
DROP TABLE IF EXISTS queues CASCADE;
DROP TABLE IF EXISTS pools CASCADE;
DROP TABLE IF EXISTS replicas CASCADE;
DROP TABLE IF EXISTS policy_sets CASCADE;
DROP TABLE IF EXISTS run_events CASCADE;
DROP TABLE IF EXISTS runs CASCADE;
DROP TYPE IF EXISTS task_state;
DROP TYPE IF EXISTS run_state;

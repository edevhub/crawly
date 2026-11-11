# ADR 0002: PostgreSQL store

- **Status:** Implemented in [`internal/store/pgstore`](../../internal/store/pgstore)
- **Date:** 2026-11-09
- **Related:** [ADR 0001: Frontier storage architecture](0001-frontier-storage-architecture.md),
  [`api/crawly/frontier/`](../../api/crawly/frontier/) (proto packages)

## Context

ADR 0001 defines the `Store` interface, the scheduling model and the conformance suite. This
ADR is the first implementation: PostgreSQL 18 on a single primary, serving several Frontier
replicas. It is the simplest engine to operate and the starting point of the scaling path
([Scaling within PostgreSQL](#scaling-within-postgresql)); its sizing below describes this
implementation, not a limit of the architecture. It incorporates the peer review of the first draft (lock order, per-queue
fairness, GCRA, partition pruning, event ordering, TOAST, compact keys) and fixes the
review's own gaps (idle backlogs banking credit, lease IDs without a nonce, overrides under
one GCRA timestamp, row order inside a table, spinning on expired tasks), and round 2 of the
review: pools instead of capability classes, the fairness clamp at pick time, the candidate
query, locking every backlog of a queue in dispatch, UTC dates, inspect indexes, and fenced
bucket ownership in the scaling path. Round 3: a retry after a contended queue, queue rows
protected from cleanup, run state read after the locks, the lease lookup, first refusal
across pools, and finished backlogs kept off the hot path.

## Decision

Implement `Store` on one PostgreSQL primary. Each `Store` method is one short transaction at
`READ COMMITTED`. The scheduling arithmetic runs in Go inside the transaction, so a
transaction is a few round trips: statements that do not depend on each other's results
are sent as one pipeline. `Dispatch` is the exception: one transaction per queue it
dispatches from (ADR 0001). No transaction stays open while a worker fetches.

The schema follows ADR 0001's locality rule: everything a hot transaction writes is keyed by
one queue. Hot state lives on two tables per queue: `queues` (shared politeness and the
fair-queueing clock) and `backlogs` (one row per run, queue and pool: ledger, fairness
tag, own politeness state, stats counters), with `lanes` (one row per handler that has tasks
in a backlog: readiness). There is no run-level counter table.

## Schema

### Overview

```
runs ── run_events                     pools
  │                                      │
  └──────── backlogs (queue × run × pool) ── queues (per key)
                 │        ├── backlog_codes
                 │        ├── lanes (× handler)
                 │        └── backlog_labels       run_labels (per run)
              tasks (partitioned by run_created_on) ── attempts (partitioned)
```

| Table | Role | Written by |
|---|---|---|
| `runs` | Immutable run spec, lifecycle state | Lifecycle only |
| `run_events` | Ordered lifecycle log for `WatchRuns` | Lifecycle only |
| `replicas`, `policy_sets` | Replica registry, policy set hashes seen | `Start` |
| `pools` | Pool names to small integer keys | First task of a pool |
| `queues` | Shared politeness state and fairness clock per queue key | Dispatch, finish, block |
| `backlogs` | Ledger, fairness, own politeness, stats per (queue, run, pool) | Dispatch, finish, add |
| `lanes` | Readiness per (queue, run, pool, handler) | Dispatch, finish, add, sweeps |
| `pool_handlers` | Handlers the replicas have sessions for, and when each was last reported | `ServeHandlers` |
| `unserved_sweep` | One row: when the sweep of tasks without a handler last ran, where it stopped, when it last reported | `SweepUnserved` |
| `backlog_codes` | Outcome code counts per backlog (`RunStats.by_code`) | Finish |
| `backlog_labels` | Final task counts per queue, run and task label (`RunStats.by_label`) | Finish, drain, TTL expiry |
| `run_labels` | The labels a run has accepted, which bounds their number | `AddTasks` that brings a new label |
| `tasks` | Every task; holds the current lease | ~3 updates per task |
| `attempts` | One row per finished attempt | Insert only |

### Enums and identifiers

```sql
CREATE TYPE run_state  AS ENUM ('open', 'sealed', 'completed', 'cancelled', 'expired', 'aborted');
CREATE TYPE task_state AS ENUM ('pending', 'in_flight', 'succeeded', 'failed', 'expired', 'cancelled');
```

`DELAYED` in the API is `pending` with `not_before > now()`, derived on read.

Opaque IDs, encoded by the store and never parsed by clients:

- `task_id` = `bucket (1 byte) ‖ run_created_on (days, 2 bytes) ‖ uuidv7 (16 bytes)`,
  base64url. The bucket and date route a lookup to one partition and, later, one shard.
- `lease_id` = `task_id ‖ attempt (4 bytes) ‖ nonce (8 random bytes)`. The nonce makes a
  lease ID unique per dispatch even if a dispatch is lost and the attempt number repeats.

`bucket` = `hash(queue_key) mod 256` with a fixed hash (xxh64 of the key bytes), computed in
Go. A task's ID carries the bucket of its own queue, so a task row keeps the bucket of its
parent next to the parent's UUID: a child on another queue can still name its parent.

### Runs and events

```sql
CREATE TABLE runs (
  run_no      bigint      GENERATED ALWAYS AS IDENTITY PRIMARY KEY,  -- internal key
  run_id      text        NOT NULL UNIQUE,                          -- API key
  spec_hash   bytea       NOT NULL,                                 -- CreateRun idempotency
  created_on  date        NOT NULL,                                 -- partition key of its tasks
  labels      jsonb       NOT NULL DEFAULT '{}',
  policy      bytea       NOT NULL,                                 -- RunPolicy, serialized proto
  key_rule    smallint    NOT NULL,
  deadline    timestamptz,
  max_tasks   bigint,
  max_depth   int,
  abort_codes text[],                                               -- abort rules, see Abort
  max_failed  bigint,
  max_consecutive_failures int,
  failed_count  bigint    NOT NULL DEFAULT 0,                       -- counters of the abort rules,
  failed_streak bigint    NOT NULL DEFAULT 0,                       -- kept only for runs that have such rules; fillfactor 70
  abort_rule  smallint,                                             -- why the run was aborted
  abort_code  text,
  abort_task  text,
  state       run_state   NOT NULL DEFAULT 'open',
  paused      boolean     NOT NULL DEFAULT false,
  resuming    boolean     NOT NULL DEFAULT false,                   -- resume sweep not finished
  created_at  timestamptz NOT NULL DEFAULT now(),
  sealed_at   timestamptz,
  finished_at timestamptz
);
CREATE INDEX runs_active   ON runs (deadline) WHERE state IN ('open', 'sealed');
CREATE INDEX runs_draining ON runs (run_no)   WHERE state IN ('cancelled', 'expired', 'aborted');
CREATE INDEX runs_resuming ON runs (run_no)   WHERE resuming;
CREATE INDEX runs_labels   ON runs USING gin (labels);

CREATE TABLE run_events (
  id     bigint      GENERATED ALWAYS AS IDENTITY PRIMARY KEY,  -- the EventCursor
  run_no bigint      NOT NULL REFERENCES runs ON DELETE CASCADE,
  type   smallint    NOT NULL,
  at     timestamptz NOT NULL DEFAULT now()
);
CREATE INDEX run_events_run ON run_events (run_no, id);
```

Everything in `runs` except `state`, `paused`, `sealed_at` and `finished_at` is immutable, so
replicas cache the spec (including the resolved `Effective` per queue) forever.

### Replicas and policy sets

```sql
CREATE TABLE policy_sets (
  hash         text        PRIMARY KEY,
  first_seen   timestamptz NOT NULL DEFAULT now(),
  activated_at timestamptz NOT NULL DEFAULT now()   -- the latest one is the current set
);

CREATE TABLE replicas (
  replica_id  text        PRIMARY KEY,
  policy_hash text        NOT NULL REFERENCES policy_sets,
  started_at  timestamptz NOT NULL,
  seen_at     timestamptz NOT NULL
);
```

Global policies are not stored: they are configuration (ADR 0001). The store keeps only the
hashes, to detect a change of set (including a rollback) and to report drift.

### Pools

```sql
CREATE TABLE pools (
  pool_id smallint GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
  name    text     NOT NULL UNIQUE          -- TaskSpec.pool, Hello.pool
          CHECK (name ~ '^[a-z0-9_]{1,32}$')
);
```

Pools exist only to give backlogs and indexes a small key. A session subscribes by name; a
pool row is created by the first task in it, and a session for a pool with no row yet simply
finds no work. Work in a pool is announced on the pool's channel, which is named after the
pool, not its id. The name rule keeps `frontier_work_<name>` a valid channel identifier
(under 63 bytes, no quoting). Creating a pool row is announced on `frontier_pools`: a
replica then listens on the new pool's channel and wakes that pool once, because the
pool's first notification may have been sent before it listened.

### Queues and backlogs

```sql
CREATE TABLE queues (
  key           text             PRIMARY KEY,
  bucket        smallint         NOT NULL,
  tat           timestamptz      NOT NULL DEFAULT '-infinity',  -- GCRA, shared limits
  in_flight     int              NOT NULL DEFAULT 0,
  vclock        double precision NOT NULL DEFAULT 0,            -- fair-queueing clock
  blocked_until timestamptz,
  block_reason  text,
  slow_interval_ns bigint        NOT NULL DEFAULT 0,            -- adaptive pacing: 0 = at the policy's rate
  strain_at        timestamptz,                                 -- start of the current epoch
  strain_steps     bigint        NOT NULL DEFAULT 0,
  last_strain_at   timestamptz,
  ceiling_at       timestamptz,                                 -- since when the interval is at its ceiling
  last_used_at  timestamptz      NOT NULL DEFAULT now()         -- for cleanup
);

CREATE TABLE backlogs (
  queue_key      text             NOT NULL,
  run_no         bigint           NOT NULL,
  pool_id       smallint         NOT NULL,
  bucket         smallint         NOT NULL,
  run_created_on date             NOT NULL,                      -- partition key for claims
  -- ledger
  pending        bigint           NOT NULL DEFAULT 0,            -- not dispatched, incl. delayed
  in_flight      int              NOT NULL DEFAULT 0,
  -- scheduling
  vtime          double precision NOT NULL DEFAULT 0,
  own_tat        timestamptz      NOT NULL DEFAULT '-infinity',  -- GCRA under a run override
  ready_at       timestamptz,                                    -- the earliest of the backlog's lanes; see Lanes
  -- stats (RunStats is the sum over the run's backlogs)
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
);
-- A queue's backlogs with work, for dispatch and finish.
CREATE INDEX backlogs_queue  ON backlogs (queue_key, run_no, pool_id) WHERE pending > 0;
-- Completion check and lifecycle fan-out.
CREATE INDEX backlogs_active ON backlogs (run_no) WHERE pending > 0 OR in_flight > 0;
-- Stats and ListQueues by run.
CREATE INDEX backlogs_run    ON backlogs (run_no, queue_key);
-- For the build before lanes, which reads readiness here; dropped with ready_at in a later
-- migration (see Migrations). This build does not read it.
CREATE INDEX backlogs_ready  ON backlogs (pool_id, ready_at) WHERE ready_at IS NOT NULL;

-- One row per handler that has pending tasks in a backlog: what the backlog's readiness
-- was before handlers existed, kept per handler.
CREATE TABLE lanes (
  queue_key text        NOT NULL,
  run_no    bigint      NOT NULL,
  pool_id   smallint    NOT NULL,
  handler   text        NOT NULL,                  -- '': handler unknown, any session may try
  ready_at  timestamptz,                           -- NULL / 'infinity': see below
  PRIMARY KEY (queue_key, run_no, pool_id, handler)
);
-- Readiness index: earliest-ready lanes per pool and handler. The last two columns let a
-- scan of one handler's lanes resume from a position.
CREATE INDEX lanes_ready      ON lanes (pool_id, handler, ready_at, queue_key, run_no) WHERE ready_at IS NOT NULL;
CREATE INDEX lanes_ready_pool ON lanes (pool_id, ready_at) WHERE ready_at IS NOT NULL;
CREATE INDEX lanes_run        ON lanes (run_no, queue_key, pool_id, handler);

CREATE TABLE backlog_codes (
  queue_key text   NOT NULL,
  run_no    bigint NOT NULL,
  code      text   NOT NULL,
  count     bigint NOT NULL DEFAULT 0,
  PRIMARY KEY (queue_key, run_no, code)
);
CREATE INDEX backlog_codes_run ON backlog_codes (run_no);

-- Task labels (migration 00005). tasks has a label column, empty for none.
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
  failed_codes jsonb  NOT NULL DEFAULT '{}',   -- code of the last attempt of each failed task
  PRIMARY KEY (queue_key, run_no, label)
);
CREATE INDEX backlog_labels_run ON backlog_labels (run_no);
```

The label counters are per queue, like `backlog_codes`, so a `Finish` updates a row of the
queue it already holds and no counter is shared by all queues of a run. `run_labels` is
written only when a label is new to a run. `AddTasks` reads it; when the batch brings a label
the run does not have and the run is not full, it first takes advisory lock `(5, run_no)`,
reads the registry again, and rejects the tasks whose label would exceed the limit as
`INVALID`. Batches with known labels, and batches to a full run (labels are never removed,
so a full run stays full), never take the lock. `runs.invalid` counts the tasks rejected
as invalid; it is written after the run's shared advisory lock, only when a batch had
rejections. The migration sets `lock_timeout` for its `ALTER TABLE`, which needs a lock on
`tasks` and all its partitions; if it times out, run it again. A task that loses the insert race to a duplicate does not
register its label.

Backlog rows are kept after their run ends, for stats, so a queue accumulates one row per
past run. The hot path never touches those: dispatch and finish read and lock only the
queue's backlogs with `pending > 0` (`backlogs_queue`), in primary key order, plus the
finished task's own backlog. Stats live on the backlog row the hot path already updates, so
counting costs no extra row.

**Lanes.** A task names its handler (`TaskSpec.handler`), and a session names the handlers
it can run (ADR 0003). Politeness, fairness, the ledger and the statistics stay per
backlog: they are about the queue's load and the run's share, not about which worker runs a
task. What a session needs is to find work it can run without reading past work it cannot,
so readiness is kept per lane: one row for each handler that has pending tasks in the
backlog. A backlog with no pending tasks has no lanes. The lanes of a backlog are written
only by a transaction that holds the backlog's row, so they need no lock order of their own.

`lanes.ready_at` has three kinds of value:

- a time: a task of the lane may be admitted then (early is allowed, late is not);
- `'infinity'`: the lane has pending tasks but waits for an event, a concurrency slot freed
  by a `Finish`, which lowers it;
- `NULL`: not dispatchable: the run is paused or ended. A lane whose tasks are all gone is
  deleted, not set to `NULL`: a lane is kept only while its backlog has pending tasks, and
  is deleted when a dispatch finds nothing claimable and nothing due in it.

Every lane of a backlog waits for the same admission (block, rate, concurrency, another
pool's first refusal). What differs is when the lane's own tasks are due, which only a
dispatch for a session that can run the handler learns, so only it writes that time. A
dispatch that cannot run a lane lowers or raises it only for the reasons that hold for all
handlers.

Tasks that exist when the lanes are introduced have no known handler: they get handler `''`,
and every session may try them. So do tasks that a build without handlers adds while the two
builds run side by side, and tasks whose handler name is longer than a worker could register
or holds a NUL byte, which `AddTasks` accepts as before and stores as unknown.

`backlogs.ready_at` stays, for the build before lanes: it claims by it. Two triggers keep
both views in step, so that the two builds can run at once. A write to `lanes` sets the
backlog's `ready_at` to the earliest `ready_at` among its lanes (`NULL` when it has none or
all are paused). A write to `backlogs.ready_at` that does not come from that trigger, so
from the build before, makes the backlog's lanes ready no later than the new value, creates
the lane of unknown handler if the backlog has none, and sets them all to `NULL` when the
value is `NULL`. Both only ever make readiness earlier. This build never writes
`backlogs.ready_at` itself, and reads only `lanes`.

`GetRun` sums the run's backlogs (`backlogs_run`), its `backlog_codes` and its
`backlog_labels`, so its cost grows
with the number of queues the run has work on (ADR 0001, Scaling model and limits).

Which limits a backlog is admitted against (shared or own) is not stored: it comes from
`Policies.For(run, queue)`, which is immutable for a given policy set.

### Tasks

```sql
CREATE TABLE tasks (
  run_created_on   date        NOT NULL,
  task_id          uuid        NOT NULL,             -- the uuidv7 part of the API task_id
  run_no           bigint      NOT NULL,
  queue_key        text        NOT NULL,
  pool_id         smallint    NOT NULL,
  fingerprint      bytea       NOT NULL CHECK (length(fingerprint) = 16),
  state            task_state  NOT NULL DEFAULT 'pending',
  handler          text        NOT NULL DEFAULT '',  -- TaskSpec.handler; '': unknown
  priority         int         NOT NULL DEFAULT 0,
  not_before       timestamptz NOT NULL DEFAULT '-infinity',
  expires_at       timestamptz,
  depth            int         NOT NULL,
  parent_task_id   uuid,
  parent_bucket    smallint,                         -- the bucket in the parent's task_id
  attempt          int         NOT NULL DEFAULT 0,
  strain_attempts  int         NOT NULL DEFAULT 0,    -- attempts that ended in a retry with strain
  lease_nonce      bigint,
  worker_id        text,
  worker_version   text,
  dispatched_at    timestamptz,
  lease_expires_at timestamptz,
  lease_deadline   timestamptz,
  spec             bytea       NOT NULL,             -- Request, handler, context
  created_at       timestamptz NOT NULL DEFAULT now(),
  finished_at      timestamptz,
  PRIMARY KEY (run_created_on, task_id),
  UNIQUE (run_created_on, run_no, fingerprint)       -- dedup (fingerprint includes the queue key)
) PARTITION BY RANGE (run_created_on);

ALTER TABLE tasks ALTER COLUMN spec SET STORAGE EXTERNAL;
-- On each partition: WITH (toast_tuple_target = 256)

-- This build claims by handler:
CREATE INDEX tasks_claim_h ON tasks (run_no, queue_key, pool_id, handler, priority DESC, task_id)
  INCLUDE (not_before, expires_at) WHERE state = 'pending';
CREATE INDEX tasks_next_h  ON tasks (run_no, queue_key, pool_id, handler, not_before)
  WHERE state = 'pending';
-- The build before claims by these, and the migration keeps them. They are dropped in the
-- later migration that drops backlogs.ready_at.
CREATE INDEX tasks_claim   ON tasks (run_no, queue_key, pool_id, priority DESC, task_id)
  INCLUDE (not_before, expires_at) WHERE state = 'pending';
CREATE INDEX tasks_next    ON tasks (run_no, queue_key, pool_id, not_before) WHERE state = 'pending';
CREATE INDEX tasks_leases ON tasks (lease_expires_at) WHERE state = 'in_flight';
CREATE INDEX tasks_ttl    ON tasks (expires_at) WHERE state = 'pending' AND expires_at IS NOT NULL;
-- Inspect API: ListTasks by run, including finished tasks.
CREATE INDEX tasks_run    ON tasks (run_no, task_id);
```

Partitions are monthly, created ahead of time by `Maintain`. All tasks of a run share the
run's `created_on`, so the dedup constraint is valid on the partitioned table and every
run-scoped query names one partition.

`tasks_claim_h` and `tasks_next_h` are keyed by handler after the pool, so the pending tasks of
one handler in one backlog are one index range, in priority order (or in `not_before`
order). A claim for several handlers reads one range per handler.

`tasks_claim_h` includes `not_before` and `expires_at`. A claim locks rows, so it still
fetches the heap tuple of a task that waits for a retry backoff before it passes over it.

`spec` is stored out of line from a few hundred bytes up, so state transitions copy a small
heap tuple and only dispatch reads the spec (one TOAST fetch).

No fillfactor tuning: every task transition changes `state` (in partial index predicates)
or `lease_expires_at` (indexed), and every backlog update changes `pending` or
`in_flight` (in index predicates) and every lane update changes `ready_at`, so none of these updates is HOT. Autovacuum settings below
account for that.

### Attempts

```sql
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
  result         smallint    NOT NULL,  -- succeeded, failed, retry, lease_expired, revoked
  kind           smallint    NOT NULL DEFAULT 0,  -- the worker's outcome kind, as reported
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

-- Inspect API: ListTasks by run and outcome code.
CREATE INDEX attempts_run_code ON attempts (run_no, code, finished_at);
```

Insert only. Stats by code come from `backlog_codes`, not from scanning attempts.

## Lock order

One total order, used by every operation. Within a table, rows are locked in primary key
order.

```
1. tasks       one row, by primary key decoded from a lease ID   (Finish, ExtendLease, discover parent)
1b. advisory (5, run_no)  exclusive: AddTasks that bring a label the run does not have
2. pools       new rows, by name                                 (AddTasks)
   queues      by key: NO KEY UPDATE (Finish, BlockQueue, Maintain), KEY SHARE (AddTasks,
               after inserting new rows in key order), UPDATE (queue cleanup, SKIP LOCKED)
3. tasks       new rows, by (run_no, fingerprint)                (AddTasks inserts)
4. backlogs    by (queue_key, run_no, pool_id)
5. backlog_codes by (queue_key, run_no, code)
   backlog_labels by (queue_key, run_no, label)
6. advisory (1, run_no)   shared: AddTasks; exclusive: completion, SealRun, lifecycle
7. runs        by run_no
8. advisory (2, 0)        run_events append

Outside the order (SKIP LOCKED: never waits, so never part of a cycle):
   queues      in Dispatch
   tasks       claimed rows in Dispatch and Maintain
```

Why it is deadlock-free:

- Every operation takes locks in this order, skipping levels it does not need.
- Step 1 is the only "first" lock that is not in key order, and each operation takes at most
  one such row. Two operations holding one task each then take queues in key order.
- Dispatch never waits for its queue or a task row (`SKIP LOCKED`), so a finish holding a
  task and waiting for its queue cannot be waited on by the dispatch holding that queue.
- `AddTasks` takes `FOR KEY SHARE` on its queue rows, in key order. That lock conflicts only
  with a row delete, so it never waits for a dispatch or a finish, which take
  `FOR NO KEY UPDATE`. It stops queue cleanup from deleting a queue between the insert and
  the backlog upsert.
- Inside `backlogs`, a dispatch locks the backlogs of its queue (one key range) and an
  add locks the backlogs of its batch, both in primary key order, so they cannot cross.
- `lanes` rows are written by transactions that hold their backlog (dispatch, finish, add,
  drain, expiry, the unserved-handler sweep), after it and in primary key order. The resume
  sweep and the readiness reset at start lock the backlogs of a batch first, in key order,
  and then update their lanes, so they take the two in the same order. The trigger that
  writes `backlogs.ready_at` after a lane write finds the backlog already locked.

## Operations

`$d` is always the run's `created_on`, so every query on `tasks` and `attempts` prunes to one
partition. `E` is the resolved `Effective` for (run, queue), from the replica's cache. The
arithmetic is `sched`'s, run in Go inside the transaction.

### Dispatch

**Candidates** (no locks, served by `lanes_ready`). A session that names handlers probes the
index once per handler, so it never reads past a lane it cannot run; the probes are merged:

```sql
SELECT queue_key, min(ready_at) AS ready_at
FROM (SELECT l.queue_key, l.ready_at
      FROM unnest($handlers) AS h(handler)           -- the session's handlers, and ''
      CROSS JOIN LATERAL (SELECT queue_key, ready_at FROM lanes
                          WHERE pool_id = $pool AND handler = h.handler AND ready_at <= now()
                          ORDER BY ready_at LIMIT 256) l
      ORDER BY l.ready_at LIMIT 256) c
GROUP BY queue_key
ORDER BY ready_at
LIMIT 64;
```

A session that names no handler reads `lanes_ready_pool` instead, with no handler condition.
The inner limit bounds lanes, the outer one queues, so a queue with many runs cannot crowd
the others out of the candidate list, and queues come back in `ready_at` order.

The replica tries candidate queues in `ready_at` order, one transaction per queue, until the
session's slots are used or the candidates run out:

```sql
-- 1. Queue, or pass over it if another operation holds it (see Waiting).
SELECT tat, in_flight, vclock, blocked_until FROM queues
WHERE key = $k FOR NO KEY UPDATE SKIP LOCKED;

-- 2. All its backlogs with pending tasks, every pool, locked in key order, and their
--    lanes (read under those locks).
SELECT * FROM backlogs
WHERE queue_key = $k AND pending > 0
ORDER BY run_no, pool_id
FOR UPDATE;
SELECT run_no, pool_id, handler, ready_at FROM lanes WHERE queue_key = $k;

-- 3. Run state, read after the locks are held.
SELECT run_no, state, paused FROM runs WHERE run_no = ANY($run_nos);
```

Backlogs of other pools are locked too (same key range, same order): fairness is decided
across pools, and step 6 can write them the exact politeness time; otherwise their loops
would pay a refused check for every dispatch on the queue.

Run state is a separate statement on purpose: it is read as late as possible, after any
wait for the backlog locks. A join in statement 2 would keep the `runs` row it read before
that wait. Pause and cancel do not lock backlogs (see Lifecycle), so a dispatch that read
the state just before they committed still hands out its tasks; ADR 0001 accepts that.

4. **Admit and pick, in Go** (`sched`). Backlogs of paused or ended runs are set to
   `ready_at = NULL` on every lane (this heals an add that raced a pause). Before it does that, the
   dispatch locks those `runs` rows `FOR SHARE` and reads them again: a resume either
   committed before, and the backlog stays, or waits for this transaction, and its sweep
   then finds the backlog. Without the lock a resume sweep could pass a backlog just
   before a dispatch that still saw the pause takes it out. For each remaining backlog,
   compute admission (ADR 0001: block, shared or own GCRA, concurrency, as slowed by adaptive pacing) and the number
   admitted. A backlog of the session's pool is a candidate only if it has a ready lane for
   a handler the session can run; a backlog of another pool, if it has a ready lane for a
   handler of unknown name `''`, or one that `pool_handlers` says a session of that pool had
   within the last five minutes (a session that names no handlers counts as having all):
   that pool's own sessions judge the rest, and work none of them can run neither holds the
   queue nor wakes the pool. The reports of a replica that predates `pool_handlers` are
   missing, so while such a replica runs, its pool's lanes of named handlers have no first
   refusal on queues it shares. Take the admitted backlog with the lowest start tag
   `greatest(vtime, vclock)` across every pool:

   - **in the session's pool:** dispatch from it, `n` = min(slots, admitted);
   - **in another pool, inside its first-refusal period** (ADR 0001; counted from
     `greatest(ready_at, the limiter's eligible time)`): set this pool's backlogs on the
     queue to `ready_at` = the end of the period (every lane of them), `NOTIFY` the other pool's channel, commit
     and go to the next candidate queue;
   - **in another pool, period over:** charge it one task
     (`vtime = greatest(vtime, vclock) + 1 / weight`, `vclock = greatest(vclock, start)`)
     and pick again.

   If concurrency, not time, is what refuses a backlog, the `ready_at` of its lanes becomes
   `'infinity'`.

5. **Claim:**

   ```sql
   WITH picked AS MATERIALIZED (
     SELECT p.task_id
     FROM unnest($handlers) AS h(handler)      -- the backlog's lanes the session can run
     CROSS JOIN LATERAL (
       SELECT task_id, priority FROM tasks
       WHERE run_created_on = $d AND run_no = $r AND queue_key = $k AND pool_id = $c
         AND handler = h.handler AND state = 'pending' AND not_before <= now()
         AND (expires_at IS NULL OR expires_at > now())
       ORDER BY priority DESC, task_id
       LIMIT $n
       FOR UPDATE SKIP LOCKED) p
     ORDER BY p.priority DESC, p.task_id
     LIMIT $n)
   UPDATE tasks t SET state = 'in_flight', attempt = t.attempt + 1, lease_nonce = $nonce,
          worker_id = $w, worker_version = $v, dispatched_at = now(),
          lease_expires_at = now() + $lease, lease_deadline = now() + $max_lease
   FROM picked WHERE t.run_created_on = $d AND t.task_id = picked.task_id
   RETURNING t.task_id, t.attempt, t.depth, t.parent_task_id, t.parent_bucket, t.spec;
   ```

   Each lane is read through `tasks_claim` on its own, and the best `n` of them are kept, so
   the order is the one a single backlog would give (priority across the handlers the session
   can run) and no task is read except the ones taken, and the ones of the other lanes that
   were not. The picked rows are materialised on purpose. Written as `WHERE (…) IN (SELECT … LIMIT $n
   FOR UPDATE SKIP LOCKED)`, the planner may run the sub-select again for every outer row,
   and the statement then claims more than `n` tasks. The same form is used wherever
   `Maintain` changes a bounded batch of tasks.

   If it returns fewer than `n` rows, nothing more of the session's lanes is due. Recompute
   the `ready_at` of each of those lanes from `tasks_next` (the earliest `not_before` among
   its non-expired pending tasks; the lane is deleted if there is none) and, if slots
   remain, try the next admitted backlog on the queue. Other lanes of the backlog keep their
   value. This stops a lane of expired tasks from being selected again before `Maintain`
   expires them.

6. **Write back**, all rows already locked:

   ```sql
   UPDATE queues SET
     tat       = least(greatest(tat, now()) + $n * $T_queue, now() + $b_queue * $T_queue),
     in_flight = in_flight + $n,
     vclock    = greatest(vclock, $start + ($n - 1) / $weight),  -- $start = greatest(vtime, vclock) before
     last_used_at = now()
   WHERE key = $k;

   UPDATE backlogs SET
     pending = pending - $n, in_flight = in_flight + $n, attempts = attempts + $n,
     vtime   = greatest(vtime, $vclock_before) + $n / $weight,
     own_tat = CASE WHEN $own THEN greatest(own_tat, now()) + $n * $T_own ELSE own_tat END
   WHERE queue_key = $k AND run_no = $r AND pool_id = $c;
   ```

   and `lanes.ready_at`, computed in Go, for the lanes of every locked backlog that waits
   (the lanes of a backlog that has no pending task are deleted). A lane the session could
   not run is given only the admission time and the other pool's first refusal, which hold
   for every handler: `greatest(ready_at, <their next admission time>)`. That value stays early (never late) because it combines a
   stored early value with the exact politeness time.

7. Commit, then send the assignments.

**Waiting.** The pool's dispatch loop (one per pool on each replica, ADR 0003) waits for the
first of:

- a notification on `frontier_work_<pool name>`. A notification for a pool whose sessions
  have free credits runs the candidate query at once, not on the next loop tick (Temporal's
  sync match); notifications are coalesced to one run per loop iteration;
- `NextReadyAt`, the earliest future `ready_at` below `'infinity'` among the lanes the
  session can run (one index probe on `lanes_ready` per handler). A loop with sessions that
  have different handlers asks the store for each set in turn and waits for the earliest;
- **20 ms, if the round passed over a locked queue row or claimed fewer tasks than admitted
  because rows were locked.** The holder (a finish, or a dispatch for another pool) may
  commit without changing readiness and so without a `NOTIFY`, and the queue's `ready_at`
  is already in the past, so nothing else would wake the loop for it;
- one second, the fallback poll, which also covers a lost `LISTEN` connection.

### Add tasks

For seeds (`Parent` nil) and discovered tasks (`Parent` set). Fingerprints, queue keys,
pools and TTLs come from the core.

1. **Parent** (discovered only):
   `SELECT depth, attempt, lease_nonce, state, lease_expires_at FROM tasks WHERE run_created_on = $d
   AND task_id = $p FOR SHARE`. Reject the batch if the lease is not live. The share lock
   makes the parent's `Finish` wait for this commit, so discoveries are durable first.
   Depth = parent depth + 1; tasks over `max_depth` are `OVER_BUDGET`.
2. **Budget:** compare the run's task count with `max_tasks`. The count is the sum of
   `backlogs.tasks_total`, cached per replica for one second (ADR 0001: soft budget).
3. **Pools and queues**, sorted:
   `INSERT INTO pools (name) … ON CONFLICT DO NOTHING`,
   `INSERT INTO queues (key, bucket) … ON CONFLICT DO NOTHING`, then
   `SELECT key FROM queues WHERE key = ANY($keys) ORDER BY key FOR KEY SHARE`. If a key is
   missing from the result (cleanup deleted it in between), repeat the insert and the lock
   for it. The lock keeps the queue row alive until this batch's backlogs are committed.
4. **Tasks**, sorted by fingerprint:
   `INSERT INTO tasks … ON CONFLICT (run_created_on, run_no, fingerprint) DO NOTHING RETURNING task_id, fingerprint`.
   Rows not returned are `DUPLICATE`.
5. **Backlogs**, one upsert per (queue, run, pool) in primary key order:
   - `pending += added`, `tasks_total += added`, `discovered += added` (discovered only),
     `duplicates += dup`, `over_budget += rejected`.

   **Lanes**, after the backlogs, one upsert per (queue, run, pool, handler) in primary key
   order: `ready_at = least(ready_at, greatest(now(), min_not_before))`. This is done for a
   paused run too: dispatch takes the lane out again the first time it meets it.
6. **Run state:** `SELECT pg_advisory_xact_lock_shared(1, $run_no)`, then read `runs.state`.
   For seeds, anything but `open` rolls back the batch as `RUN_CLOSED`; for discoveries,
   anything but `open` or `sealed` does. Because the lock comes last, `SealRun` and
   completion (which take it exclusively) either commit before this read, and the batch is
   rejected, or wait for this batch to commit, and see its tasks.
   No fairness clamp here: dispatch clamps at pick time.
7. Commit. `NOTIFY frontier_work_<pool name>` for pools that got a ready backlog.

### Finish

Used for worker completions (`Source = Worker`) and the reaper (`Source = Reaper`).

1. **Task by lease:** `SELECT … FROM tasks WHERE run_created_on = $d AND task_id = $t FOR UPDATE`.
   Valid when `state = 'in_flight'`, `attempt` and `lease_nonce` match, and
   `lease_expires_at > now()` for a worker or `≤ now()` for the reaper. Otherwise return
   `STALE` (or `UNKNOWN` if the row does not exist) and change nothing.
2. **Queue:** `SELECT … FROM queues WHERE key = $k FOR NO KEY UPDATE`.
3. **Backlogs of the queue with pending tasks, and the task's own**, in key order,
   `FOR UPDATE` (a freed slot can make any of them ready):
   `WHERE queue_key = $k AND (pending > 0 OR (run_no = $r AND pool_id = $p))`. Backlogs of
   finished runs are not touched. Then read the run's state in a separate statement, as in
   dispatch.
4. **Task transition:**
   - `Succeeded` / `Failed` → final state, `finished_at = now()`;
   - `Retry` → `pending`, `not_before = now() + $retry_in`, lease columns cleared;
   - `Released` (the assignment never reached a worker) → `pending`, `not_before = now()`,
     `attempt -= 1`, lease columns cleared. Steps 5 and 8 are skipped: no attempt row and
     no outcome code. The queue's politeness state is left as dispatch advanced it, so a
     release never lets a queue exceed its limits;
   - if the run is `cancelled`, `expired` or `aborted`, the task becomes `cancelled` /
     `expired` / `cancelled` instead, the attempt is recorded as `revoked` and the call reports the lease as
     `STALE`: the result the worker sent is not applied.
5. **Attempt row** insert.
6. **Queue:** `in_flight -= 1`; if `Block > 0`:
   `blocked_until = greatest(blocked_until, now() + $block)`, `block_reason = $code`.
   The adaptive state moves with the outcome (ADR 0001, Adaptive pacing), in the same
   row update: it is decayed and, when adaptive pacing is off, cleared; a `Strain` outcome
   takes a strain step (`slow_interval_ns`, `strain_at`, `ceiling_at`, `strain_steps`,
   `tat = greatest(tat, now() + interval)`, and the same for the `own_tat` of the locked
   backlogs of runs with limits of their own); an `Answered` outcome takes a recovery step.
   `last_strain_at` is set for every `Strain` outcome. Both compare the attempt's
   `dispatched_at` with `strain_at`, which the task row already has. An `Extended` retry
   becomes a failure while the queue has been at its ceiling for the policy's
   `max_outage`. A retry with `Strain` adds one to `tasks.strain_attempts`, which the
   lease context carries to the core. When the interval got shorter, the `ready_at` of the
   locked backlogs that wait for a time is
   lowered to their new admission time: a value computed for the longer interval would be
   late.
7. **Backlog** of the task: `in_flight -= 1`, result counter `+= 1`, `queue_blocks += 1` if
   blocked; for a retry `pending += 1` and
   `ready_at = least(ready_at, now() + $retry_in)` on the task's lane (it is added if the
   backlog had deleted it). **The lanes of the other locked backlogs on the queue:** lower
   `ready_at` to their next admission time if the freed slot changes it (this is what brings
   a lane back from `'infinity'`), or raise it to the end of a new block.
8. **`backlog_codes`:** `count += 1` for the outcome code. **`backlog_labels`:** for a task
   with a label that reached a final state, `succeeded`, `failed` (with its code in
   `failed_codes`), `expired` or `cancelled` `+= 1`. The sweep that fails tasks without a
   handler, the drain of a cancelled, expired or aborted run and TTL expiry count the
   same way.
9. **Abort rules** (below), then **completion** unless the run was aborted, if the backlog now has `pending = 0 AND in_flight = 0` (see below).
10. Commit; `NOTIFY` if a backlog became ready.

### Lease lookup

`SELECT run_no, queue_key, pool_id, attempt, depth, state FROM tasks WHERE run_created_on = $d
AND task_id = $t`, with the key decoded from the lease ID. One primary key read, no locks.
The core calls it only when a `Complete` arrives for a lease this replica did not dispatch;
`Finish` does the fencing.

### Abort

Part of Finish, after step 8, for an attempt that was applied (not `Released`, not a task of a
run that was over). The rules are those of `RunLimits` (ADR 0001); `store.RunLimits.Trip`
evaluates them for both stores. A run with no rules is not touched.

1. Needed only when the attempt can change a counter or trip the code rule: a final failure
   of a run with `max_failed` or `max_consecutive_failures` (not one of the reaper's), a
   success of a run with `max_consecutive_failures`, or an attempt with a listed code.
2. If the completion check of step 9 is due (the backlog is empty),
   `pg_advisory_xact_lock(1, $run_no)` first: `complete` takes it, and the order of every
   lifecycle change is the lock, then the row. Otherwise this lock is not taken, so a
   failure storm does not queue ahead of `AddTasks`, which takes it in shared mode.
3. A final failure: `UPDATE runs SET failed_count = failed_count + 1, failed_streak =
   failed_streak + 1 WHERE run_no = $r AND state IN ('open', 'sealed') RETURNING ...`. A
   success: `SET failed_streak = 0 WHERE ... AND failed_streak <> 0`, which locks the row
   only when there is a streak. No row means the run is over: another outcome ended it first.
4. If `Trip` says a rule trips: `UPDATE runs SET state = 'aborted', finished_at,
   resuming = false, abort_rule, abort_code, abort_task WHERE run_no = $r AND state IN
   ('open', 'sealed')`, and `ABORTED` is appended to `run_events` only if a row changed.
   `end` does this for cancel and deadline too. The completion check of step 9 is skipped,
   and `complete` itself updates only a run that is still `sealed`.

The row lock, held from the counter update to the commit, orders the outcomes of a run that
change a counter, whichever queues they come from, which is what "in a row" means. Two
outcomes that trip a rule at once give one abort: the second finds the run over. No path
takes the advisory lock while it holds the run's row, so the order cannot be reversed. The
cost of a failure storm is one row update per final failure on one row, and the table is
created with `fillfactor = 70` so that these updates stay on their page. Waiting for that
row counts against the pool's `lock_timeout`: a storm slower than that fails Finish, which
is retried; `TestFailureStormOnOneRun` runs 64 queues against it. The drain handles an aborted run like a cancelled
one. Adding the `aborted` value to `run_state` and to the draining index is a migration that
runs outside a transaction, because PostgreSQL cannot use a new enum value in the
transaction that adds it. The migrator sets a `lock_timeout` of three seconds, so a
statement that needs a lock the live Frontier holds fails and is run again instead of
queueing every query behind it, and the index is built `CONCURRENTLY` next to the old one.

**Rollout and rollback.** A build without the rules reads `aborted` as no known state, which
is not final: it would dispatch the run, extend its leases and never drain it. The
Frontier gates on this. `replicas.abort_rules` is set by a replica that knows the rules
(the column defaults to false for any other), and `CreateRun` with rules, for a run that
does not exist yet, is refused with `store.ErrUnsupported` while a replica without it has
reported within the last minute. The order is: migrate, upgrade every replica, then let
schedulers use the rules. A replica that keeps its ID across a downgrade keeps its flag, so
give replicas fresh IDs, or clear it, when going back. To go back to a build without the
rules, run the Down step of the migration first: it turns `aborted` runs into `cancelled`
ones. The enum value stays. Within this build an unknown run state name reads as
`cancelled` and is logged, so a later state is final here too.

### Extend lease

Lock the task by lease, check validity as in Finish, then
`lease_expires_at = least(now() + $by, lease_deadline)`. One row, no other locks. A lease
of a run that was cancelled, has expired or was aborted is not extended: it is reported as `STALE`.

### Block queue

The operator call follows the lock order of Finish: create the queue row if it is missing,
lock it `FOR NO KEY UPDATE`, lock its backlogs with pending tasks, set or clear
`blocked_until`. Clearing it also resets the adaptive state (`slow_interval_ns = 0`,
`strain_at = now()`, `tat = least(tat, now())`). A block raises the `ready_at` of those backlogs' lanes to its end. Lifting
or shortening a block lowers every `ready_at` that lies in the future to `now()`: a value
that an earlier block raised would otherwise be late.

### Expired leases (reaper)

```sql
SELECT run_created_on, task_id, attempt, lease_nonce, run_no, queue_key
FROM tasks WHERE state = 'in_flight' AND lease_expires_at <= now()
ORDER BY lease_expires_at LIMIT $limit;
```

No locks: the core finishes each lease with `Source = Reaper`, and fencing makes a race with
a late worker completion safe (one of them gets `STALE`). Partitions with no in-flight tasks
have empty `tasks_leases` indexes, so the scan touches only active partitions.

### Pool statistics

`PoolStats` serves the metrics and is read by each replica on an interval, so it reads
backlogs and never tasks. Its cost grows with the backlogs that have work, times the
number of replicas, and it runs under a timeout. The totals per pool are one grouped read over the backlogs that have work
(`backlogs_active`), joined to their runs, because the readiness of a backlog of a paused
or ended run is only lowered by the next dispatch round and must not count. The queues
whose work is ready come from the readiness index, one bounded read per pool:

```sql
SELECT b.queue_key, b.ready_at FROM backlogs b JOIN runs r USING (run_no)
WHERE b.pool_id = $pool AND b.ready_at <= now() AND b.pending > 0
  AND r.state IN ('open', 'sealed') AND NOT r.paused
ORDER BY b.ready_at LIMIT $due_limit;
```

A backlog is ready when `ready_at` has come. A `NULL` or `'infinity'` readiness, and one in
the future, count the backlog's tasks as delayed.

The dispatch query returns each claimed task's creation time and `not_before`, from which
the assignment's wait is computed, and the worker the lease was dispatched to is the
`worker_id` the claim wrote.

### Run completion

Runs inside Finish (step 9), inside `Maintain` after expiring or cancelling the last tasks of
a backlog, and in `SealRun`:

```sql
SELECT pg_advisory_xact_lock(1, $run_no);
SELECT state FROM runs WHERE run_no = $run_no;
SELECT NOT EXISTS (SELECT 1 FROM backlogs
                   WHERE run_no = $run_no AND (pending > 0 OR in_flight > 0));  -- backlogs_active
-- if state = 'sealed' and nothing is active:
UPDATE runs SET state = 'completed', finished_at = now() WHERE run_no = $run_no;
SELECT pg_advisory_xact_lock(2, 0);
INSERT INTO run_events (run_no, type) VALUES ($run_no, 'completed');
NOTIFY frontier_runs;
```

Correctness under `READ COMMITTED`: two transactions T1 and T2 empty the run's last two
backlogs. T1 takes the lock first, sees T2's backlog still active (T2 is uncommitted) and
commits without completing. T2 waits for the lock until T1 commits; its next statement takes
a new snapshot, sees both backlogs empty and completes the run. The lock is taken only when
a backlog empties, so it is off the hot path.

The same lock, taken shared by `AddTasks` (step 6), closes the seed/seal race and makes it
impossible to add a task to a run that has just completed.

### Lifecycle

All follow the lock order: advisory `(1, run_no)` exclusive → `runs` → advisory `(2, 0)` →
`run_events` → `NOTIFY frontier_runs`. None of them touches `backlogs`: each is a constant
number of rows, whatever the size of the run, and the effect on backlogs is eventual.

- **CreateRun:** `INSERT … ON CONFLICT (run_id) DO NOTHING`; if the existing row has a
  different `spec_hash`, `ErrConflict`. `created_on = (now() AT TIME ZONE 'UTC')::date`: the date is part of task IDs and
  partition routing, so it must not depend on a session's time zone. The database time zone
  is pinned to UTC as well.
- **SealRun:** `state = 'sealed'`, then the completion check in the same transaction.
- **PauseRun:** set `paused`, clear `resuming`. Backlogs are not touched: a dispatch that
  meets a backlog of a paused run sets the `ready_at` of its lanes to `NULL` (dispatch
  step 4), which takes them out of the readiness index the first time they come up.
- **ResumeRun:** clear `paused`, set `resuming`. The replica that served the call then runs
  the resume sweep, and `Maintain` finishes it if that replica dies: in batches, each its
  own transaction in primary key order,
  `UPDATE lanes SET ready_at = now() WHERE run_no = $r`, with a `NOTIFY` per pool. Each
  batch re-reads the run and stops if it was paused again. The last batch clears `resuming`
  if no lane of the run has `NULL`. The sweep resets every lane and not only the ones with
  `NULL`: nothing keeps the readiness of a paused run up to date, so a lane may still wait
  (`'infinity'`) for a slot that was freed during the pause, or for a block that was lifted.
  Dispatch works out the readiness again.
- **CancelRun:** `state = 'cancelled'`, `finished_at = now()`. Returns at once; dispatch
  skips the run's backlogs as for a paused run, and `Maintain` drains them.
- **Abort:** by `Finish` (see Abort), not by a call.
- **Deadline:** `Maintain` finds runs past `deadline` (`runs_active`) and ends them like
  cancel with `state = 'expired'`.

Replicas learn about `CANCELLED`, `EXPIRED` and `ABORTED` from `run_events` and push `LeaseRevoked` for
the leases they dispatched to their sessions.

### Maintain

Each job processes a bounded batch per call and is safe on all replicas at once (row locks
with `SKIP LOCKED`, or `pg_try_advisory_xact_lock` per job):

| Job | Work |
|---|---|
| Task TTL | Per backlog with expired pending tasks (`tasks_ttl`): lock queue, backlog, expire tasks `SKIP LOCKED`, adjust `pending` / `expired`, recompute the lanes' `ready_at`, completion check |
| Resume sweep | For runs with `resuming` (`runs_resuming`): the batches described under ResumeRun |
| Cancel, abort and deadline drain | Per backlog of a `cancelled` / `aborted` / `expired` run (`runs_draining`, `backlogs_active`): pending tasks to `cancelled` / `expired`; in-flight tasks to the same state with a `revoked` attempt; release `queues.in_flight`; adjust the ledger |
| Partitions | Create next month's `tasks` and `attempts` partitions; drop partitions past retention, except in months where a run is still `open` / `sealed` or has tasks to drain |
| Queues | Delete `queues` rows with no backlog rows and `last_used_at` older than a day: lock the candidates `FOR UPDATE SKIP LOCKED` (an add holding `KEY SHARE` makes the row skipped), re-check that no backlog exists, then delete |
| Replicas | Delete replicas not seen for an hour |
| Unserved handlers | Every minute at most, see Handlers below: lanes whose handler no replica has reported for the grace period are dropped if they are stale, and their old tasks are failed with code `no_handler` |
| Ledger audit | Compare a sample of backlogs' `pending` / `in_flight` with counts from `tasks`, and count backlogs without a `queues` row; report both (they should be zero). One statement, so it sees one snapshot and can run under load |

### Start

Upsert the replica row. Then, in one transaction under `pg_advisory_xact_lock(3, 0)`: read
the hash with the latest `activated_at`; if it is not this replica's, upsert this hash with
`activated_at = now()`. That covers a new set and a rollback to an earlier one, and only the
first replica of a rollout does it.

If the set changed, `UPDATE lanes SET ready_at = now() WHERE ready_at IS NOT NULL`
(in primary key order, in batches, each its own transaction; `'infinity'` is reset too,
lanes of paused and ended runs keep `NULL` on purpose) and `NOTIFY` every pool channel.

### Handlers

A replica reports the handlers its sessions can run (`ServeHandlers`) when they change and
otherwise as a heartbeat, every `min(grace / 10, 1 minute)` (`-no-handler-grace` is at least
four `Maintain` intervals): `INSERT INTO pool_handlers (pool, handler, seen_at) … ON CONFLICT
DO UPDATE SET seen_at`, in key order. A session that names no handlers is the handler `''`:
it can run any task of its pool, so while its row is newer than the grace period no handler
of the pool is unserved.

`SweepUnserved` is one bounded round. The replica asks every `min(grace / 4, 1 minute)`,
and the store lets one round through per that time, whichever replica asks:

```sql
UPDATE unserved_sweep SET swept_at = now() WHERE swept_at <= now() - $every RETURNING <cursor>;
```

A replica that gets no row does nothing. The round finds the handlers that have lanes with
a skip scan over `lanes_ready`, which reads one entry per (pool, handler) and not one per
lane, and keeps the pairs with no `pool_handlers` row newer than the grace period, for the
handler or for `''`. Then it walks the lanes of those pairs from the cursor, in the order of
`lanes_ready` (`ready_at, queue_key, run_no`, a keyset page of at most 50), and looks at at
most `maintainBacklogs` lanes, with a transaction of its own for each: the queue (`SKIP
LOCKED`), then the backlogs as dispatch does. It checks whether the lane has a pending task
through `tasks_claim_h`; if not, the lane is stale and is deleted. If it has, and the grace
period has passed since the newest `replicas.started_at` (a restart of every replica must not
fail tasks whose workers have not reconnected yet), it fails the pending tasks of the lane
created at least a grace period ago, and keeps going on a lane while a transaction fails a
full page of them (`maintainTasks`, `SKIP LOCKED`): `state = 'failed'`, one `attempts` row
with code `no_handler` so that `GetTask` and `ListTasks` explain the failure,
`backlogs.failed` and `backlog_codes` up by the count. The attempt was never dispatched, so
`backlogs.attempts` does not change. A run that this empties completes. The round stores
where it stopped in `unserved_sweep` and the next one, on any replica, goes on from there, so
every lane is reached however many wait, and a round costs the same whatever their number.

What waits is counted after the round, per (pool, handler), as the queues and runs that have
lanes (up to 10,000), and returned with a flag that says whether this call is the one to log
it: `unserved_sweep.reported_at` lets one call through per `min(grace, 10 minutes)`, so the
replicas do not each log the same lines. The core logs it at `WARN`, and, while the
restart guard holds back failures, a line that says so. This is what an operator sees of
tasks that wait for a handler: `RunStats` counts them as pending.

The registry is the only state about sessions that the store keeps. It says that some
replica had a session with the handler lately, not which one, and not that the session has
a free credit.

## Migrations

The schema is a set of versioned SQL migrations, embedded in the Frontier binary and
applied with [goose](https://github.com/pressly/goose) by a command of its own:

```sh
crawly-frontier migrate -postgres "$URL"          # applies what is pending
crawly-frontier migrate -postgres "$URL" status   # lists the migrations
```

- **A step of the rollout:** the store never changes the schema. On start it reads the
  highest applied version and refuses to run when it is lower than the version of its
  build. A higher version is accepted with a warning, so the replicas of the previous
  build keep running during a rolling update. In this sense, a migration must stay
  compatible with the build before it.
- **Version table:** goose records the applied migrations in a table, `crawly_migrations`
  by default. The name is configurable (`-postgres-migrations-table`, or
  `CRAWLY_POSTGRES_MIGRATIONS_TABLE`), optionally as `schema.table`, so the table does not
  collide with the migrations of another application in the same database. The Frontier
  takes the same setting to find the version.
- **Concurrent runs:** the command holds a session advisory lock while it works, so
  several runs at once are safe: one applies the migrations and the others find nothing
  to do. The lock needs a direct connection, not one through a transaction pooler.
- **Expand, then contract:** a migration only adds what the build before does not know, and
  keeps what it reads and writes working. What the new build replaces is dropped by a later
  migration, once no replica of the build before runs. Migration 2 (handler lanes) is an
  expand step: it adds `tasks.handler`, `lanes`, `pool_handlers`, `unserved_sweep` and two
  indexes on `tasks`, and keeps `backlogs.ready_at`, `backlogs_ready`, `tasks_claim` and
  `tasks_next`, which the build before claims by. The two triggers of Lanes keep the old
  and the new readiness in step while both builds run. The contract migration drops the
  column, its index, the triggers, and the old two indexes, and the store refuses a
  database below the version that added what it needs.
- **No lock that stops the tasks:** migration 2 sets `lock_timeout` to 5 s for its own
  transaction, so that it fails, and can be run again, instead of queueing behind a long
  query with every operation queued behind it. Its indexes on `tasks` are declared on the
  parent only (`CREATE INDEX … ON ONLY`), and the command builds them on each existing
  partition after the migration with `CREATE INDEX CONCURRENTLY`, which waits for running
  transactions but blocks no write, and attaches them. It checks them at every run and
  finishes an interrupted build; a partition made later gets them from the parent. The
  store works without them, slower, until they exist.
- **Rolling back:** the migrations are forward only in normal use: no command rolls one back.
  Because migration 2 only adds, the build before runs on the migrated schema, so going back
  is stopping the new replicas. Removing its objects is a decision of its own: stop every
  replica of the new build, then run goose `down` for version 2 by hand. It drops the
  objects and leaves `backlogs.ready_at` current; tasks keep no memory of their handler.
- **Partitions:** the monthly partitions of `tasks` and `attempts` are not migrations.
  The store creates and drops them as time passes.

## Retention

- `tasks` and `attempts`: monthly partitions by `run_created_on`, dropped (or archived)
  after the retention period. No bulk `DELETE`. A month is kept while one of its runs is
  not ended or still has pending or in-flight tasks, because their tasks live in that
  partition and a lease of a dropped task could never be finished.
- `runs`, `run_events`, `run_labels`, `backlogs`, `lanes`, `backlog_codes`, `backlog_labels`: deleted with the run after a longer
  retention, kept for run comparisons. The delete locks the run before its backlogs, which
  is the reverse of the lock order. It is allowed because it only takes ended runs whose
  backlogs have no pending or in-flight tasks, so nothing else locks those backlogs. The
  replica then sends `deleted` on `frontier_runs`, and every replica drops its run cache,
  because a run ID is free again after the delete.
- Raw responses live in object storage (`response_ref`) under their own retention.

## Operational settings

- **Durability:** `synchronous_commit = on` for every transaction. Fencing would survive a
  lost dispatch (the lease nonce), but a lost dispatch also loses the queue's politeness
  update, which allows a burst after a crash. Group commit keeps the fsync cost low under
  concurrent sessions.
- **Autovacuum:** per table on `tasks` partitions, `backlogs`, `lanes` and `queues`:
  `autovacuum_vacuum_scale_factor = 0.01`, `autovacuum_vacuum_cost_limit` raised. All
  updates on these tables are non-HOT by design. `queues` has one row per host;
  `backlogs` grows with retained runs, but only rows with pending tasks are in its hot
  indexes.
- **Pooling:** PgBouncer in transaction mode for operations. Each replica keeps **one direct
  connection** for `LISTEN frontier_work_<pool name>, frontier_runs` (LISTEN does not work through
  a transaction pooler). Notifications carry no payload.
- **Timeouts:** `statement_timeout` 2s and `lock_timeout` 1s on operation transactions;
  `idle_in_transaction_session_timeout` 5s. The store sets them on its connections unless
  the connection URL sets them.
- **Internal retries:** the store retries deadlock (`40P01`), serialization (`40001`) and
  lock timeout (`55P03`) errors up to three times with jitter before returning
  `ErrUnavailable`. Under the lock order, deadlocks are a bug: each one is logged and
  counted.

## Capacity

First-principles estimate per vCPU for this schema, with a warm cache and synchronous
commit (from the review, to be replaced by the benchmark):

| Figure | Estimate |
|---|---|
| Dispatch, backend CPU | 0.5–0.8 ms |
| Finish, backend CPU | 0.5–0.8 ms |
| Add, batch of 50 links with 5 new | 1.5–2 ms |
| Sustainable task rate per vCPU (half a core, vacuum keeping up) | 200–300 tasks/s |
| Ceiling per vCPU (core saturated) | 400–600 tasks/s |
| Active tasks resident before dedup probes hit disk, per 2 GiB | 3–4M |

**Reference deployment.** To make the estimate concrete, and as the benchmark profile: a
small instance (2 vCPU, 8 GiB, SSD) under a workload of about 100k tasks a day, peaking at
≈ 150 dispatches/s, over a few thousand queues and tens of open runs. That load is under 25%
of one core at peak and leaves the second core for autovacuum, WAL and the candidate
queries. It is an example, not a design target: a larger instance scales the per-vCPU
figures above until one of the limits below binds, and past that the scaling path applies.

Handlers add to this. A backlog writes one lane row for each handler it has that waits, where
it wrote one backlog row before, and a lane update is non-HOT across four indexes
(`lanes_ready_pool` serves only sessions that name no handlers); a session that names
handlers reads one index range per handler, which is why a session names at most 64. Until
the contract migration, `tasks` also carries the two partial indexes of the build before,
so a pending task is in four partial indexes. Dispatch does one candidate query per distinct
set of handlers among a pool's sessions that found nothing in the round.

What binds first on a single primary, in order: CPU (seven indexes on `tasks`,
non-HOT updates), fsync rate (two to three commits per task; group commit hides it with ten
or more sessions), memory for the dedup index of open runs.

**Benchmark** before implementation is accepted: a pgbench custom script with one add batch,
ten dispatches and ten finishes per loop, sixteen clients, on the reference instance and
on one larger instance, to confirm the per-vCPU figures. Record the measured ceilings here.

**Signals and thresholds** that trigger the next implementation (ADR 0001): dispatch p99
above 50 ms, `queues` / `backlogs` lock waits above 5% of dispatch time, or primary CPU above
60% at the expected peak. Also watch `n_dead_tup` and autovacuum lag on `tasks` and
`backlogs`, the dedup index's cache hit ratio, `pg_stat_database.deadlocks` (must stay flat)
and the ledger audit metric.

**Health signals** (alert on the last two):

- share of dispatches served on a notification without waiting for a loop tick (Temporal's
  sync-match rate);
- backlog age per pool: the oldest `ready_at` in the past among `lanes_ready_pool`;
- ready-to-dispatched latency per pool (Temporal's schedule-to-start): dispatch time minus
  the backlog's `ready_at` at claim, as a histogram.

## Scaling within PostgreSQL

Not built now; the schema is ready for it.

1. **More replicas on one primary.** Replicas compete through `SKIP LOCKED` on `queues`. Fine
   up to about three.
2. **Bucket ownership with work stealing.** A `bucket_leases` table
   (`bucket`, `owner`, `range_id bigint`, `expires_at`) assigns each replica a set of the 256
   buckets, renewed over its direct connection. The candidate query first filters on owned
   buckets and falls back to all buckets when the session still has free slots. Workers
   still connect to any replica.

   **Fencing:** `range_id` is bumped on every change of owner. Every write transaction of an
   owner starts with `SELECT range_id FROM bucket_leases WHERE bucket = $b FOR SHARE` and
   compares it with the id it acquired; a mismatch aborts with `ErrOwnershipLost`, and the
   replica drops its in-memory state for the bucket. Taking over a bucket locks its row
   `FOR UPDATE`, which waits for the old owner's in-flight writes, then bumps the id. Stolen
   work (a bucket the replica does not own) is dispatched without in-memory state, through
   the ordinary transactions above. The `bucket_leases` row lock comes before `queues` in
   the lock order.
3. **Sharded primaries.** Each shard holds `queues`, `backlogs`, `lanes`, `backlog_codes`, `tasks`
   and `attempts` for a range of buckets; a small control database holds `runs`,
   `run_events`, `replicas`, `policy_sets` and `pools`. Every hot transaction stays on one
   shard because everything it writes is keyed by one queue (dedup included, since the
   fingerprint includes the queue key). Completion and the seed/seal race move the advisory
   lock to the control database as described in ADR 0001. Resharding moves whole buckets
   with a short per-bucket pause. Citus can do the routing (`queue_key` as the distribution
   column, `runs` as a reference table) if writing it is not wanted.

## Tests specific to this implementation

On top of `storetest` (ADR 0001):

- `EXPLAIN` of the claim, completion, `ListTasks` and Maintain queries prunes to one
  partition.
- Concurrent dispatch, finish, discover, pause and cancel on one run across two queues and
  two buckets for a minute: `pg_stat_database.deadlocks` unchanged.
- Kill the primary connection between claim and commit: the task is pending again and no
  lease with the lost nonce is accepted.
- Ledger audit reports zero difference after a randomized workload.
- `EXPLAIN` of the candidate, claim, sweep and sweep-page queries reads indexes only: one
  `lanes_ready` or `tasks_claim_h` range per handler, no sort of the page, no scan of the
  table (`TestQueryPlans`).
- The migration that introduces lanes gives every waiting backlog one lane for handler `''`
  with the readiness it had, and the build before keeps working on the migrated schema: its
  statements find the work that this build adds, and its writes reach the lanes
  (`TestMigrationKeepsPreviousBuildWorking`). Rolling it back leaves that schema.
- A pause or cancel committing while a dispatch waits for a backlog lock on the queue: the
  dispatch claims nothing from that run.
- Pause, resume and cancel on a run with 100,000 backlogs each commit in constant time; the
  resume sweep survives the death of the replica that started it.
- `EXPLAIN` of dispatch and finish on a queue with a thousand finished backlogs reads only
  the ones with pending tasks.
- Queue cleanup racing `AddTasks` on the same key: the orphaned-backlog metric stays zero.

## Consequences

- No run-level row is written on the hot path; the hottest rows are one queue's `queues` and
  `backlogs` rows, written at that queue's dispatch rate.
- The readiness index is small and churny by design. It depends on invariant "early, never
  late", enforced by every operation above and checked by `storetest`'s property test (`RoutingProperty`: random adds, dispatches by sessions with random handlers, finishes and time under a concurrency limit; nothing is dispatched to a session without its handler and every task runs) and by tests that fail when a lane is not woken or a later task delays a ready one.
- `GetRun` and `WatchRuns` stats cost a scan of the run's backlogs instead of one row, which
  grows with the queues a run has work on (ADR 0001, Scaling model and limits).
- Run IDs and fingerprints are compact in indexes (`run_no`, 16 bytes); the API keeps the
  text `run_id`.
- The schema already carries `bucket` and bucket-routable IDs, so the scaling steps need no
  migration of existing rows.

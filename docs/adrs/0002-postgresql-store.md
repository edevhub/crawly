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
tag, own politeness state, stats counters, readiness). There is no run-level counter table.

## Schema

### Overview

```
runs ── run_events                     pools
  │                                      │
  └──────── backlogs (queue × run × pool) ── queues (per key)
                 │        └── backlog_codes
              tasks (partitioned by run_created_on) ── attempts (partitioned)
```

| Table | Role | Written by |
|---|---|---|
| `runs` | Immutable run spec, lifecycle state | Lifecycle only |
| `run_events` | Ordered lifecycle log for `WatchRuns` | Lifecycle only |
| `replicas`, `policy_sets` | Replica registry, policy set hashes seen | `Start` |
| `pools` | Pool names to small integer keys | First task of a pool |
| `queues` | Shared politeness state and fairness clock per queue key | Dispatch, finish, block |
| `backlogs` | Ledger, fairness, own politeness, stats, readiness per (queue, run, pool) | Dispatch, finish, add |
| `backlog_codes` | Outcome code counts per backlog (`RunStats.by_code`) | Finish |
| `tasks` | Every task; holds the current lease | ~3 updates per task |
| `attempts` | One row per finished attempt | Insert only |

### Enums and identifiers

```sql
CREATE TYPE run_state  AS ENUM ('open', 'sealed', 'completed', 'cancelled', 'expired');
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
  state       run_state   NOT NULL DEFAULT 'open',
  paused      boolean     NOT NULL DEFAULT false,
  resuming    boolean     NOT NULL DEFAULT false,                   -- resume sweep not finished
  created_at  timestamptz NOT NULL DEFAULT now(),
  sealed_at   timestamptz,
  finished_at timestamptz
);
CREATE INDEX runs_active   ON runs (deadline) WHERE state IN ('open', 'sealed');
CREATE INDEX runs_draining ON runs (run_no)   WHERE state IN ('cancelled', 'expired');
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
  ready_at       timestamptz,                                    -- NULL / 'infinity': see below
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
-- Readiness index: earliest-ready backlogs per pool.
CREATE INDEX backlogs_ready  ON backlogs (pool_id, ready_at) WHERE ready_at IS NOT NULL;
-- A queue's backlogs with work, for dispatch and finish.
CREATE INDEX backlogs_queue  ON backlogs (queue_key, run_no, pool_id) WHERE pending > 0;
-- Completion check and lifecycle fan-out.
CREATE INDEX backlogs_active ON backlogs (run_no) WHERE pending > 0 OR in_flight > 0;
-- Stats and ListQueues by run.
CREATE INDEX backlogs_run    ON backlogs (run_no, queue_key);

CREATE TABLE backlog_codes (
  queue_key text   NOT NULL,
  run_no    bigint NOT NULL,
  code      text   NOT NULL,
  count     bigint NOT NULL DEFAULT 0,
  PRIMARY KEY (queue_key, run_no, code)
);
CREATE INDEX backlog_codes_run ON backlog_codes (run_no);
```

Backlog rows are kept after their run ends, for stats, so a queue accumulates one row per
past run. The hot path never touches those: dispatch and finish read and lock only the
queue's backlogs with `pending > 0` (`backlogs_queue`), in primary key order, plus the
finished task's own backlog. Stats live on the backlog row the hot path already updates, so
counting costs no extra row.

`ready_at` has three kinds of value:

- a time: the backlog may have a task admitted then (early is allowed, late is not);
- `'infinity'`: it has pending tasks but waits for an event, a concurrency slot freed by a
  `Finish`, which lowers it;
- `NULL`: not dispatchable: no pending tasks, or the run is paused or ended.

`GetRun` sums the run's backlogs (`backlogs_run`) and its `backlog_codes`, so its cost grows
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
  priority         int         NOT NULL DEFAULT 0,
  not_before       timestamptz NOT NULL DEFAULT '-infinity',
  expires_at       timestamptz,
  depth            int         NOT NULL,
  parent_task_id   uuid,
  parent_bucket    smallint,                         -- the bucket in the parent's task_id
  attempt          int         NOT NULL DEFAULT 0,
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

CREATE INDEX tasks_claim  ON tasks (run_no, queue_key, pool_id, priority DESC, task_id)
  INCLUDE (not_before, expires_at) WHERE state = 'pending';
CREATE INDEX tasks_next   ON tasks (run_no, queue_key, pool_id, not_before)
  WHERE state = 'pending';
CREATE INDEX tasks_leases ON tasks (lease_expires_at) WHERE state = 'in_flight';
CREATE INDEX tasks_ttl    ON tasks (expires_at) WHERE state = 'pending' AND expires_at IS NOT NULL;
-- Inspect API: ListTasks by run, including finished tasks.
CREATE INDEX tasks_run    ON tasks (run_no, task_id);
```

Partitions are monthly, created ahead of time by `Maintain`. All tasks of a run share the
run's `created_on`, so the dedup constraint is valid on the partitioned table and every
run-scoped query names one partition.

`tasks_claim` includes `not_before` and `expires_at`. A claim locks rows, so it still
fetches the heap tuple of a task that waits for a retry backoff before it passes over it.

`spec` is stored out of line from a few hundred bytes up, so state transitions copy a small
heap tuple and only dispatch reads the spec (one TOAST fetch).

No fillfactor tuning: every task transition changes `state` (in partial index predicates)
or `lease_expires_at` (indexed), and every backlog update changes `ready_at` or the
`pending`/`in_flight` predicate, so none of these updates is HOT. Autovacuum settings below
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
2. pools       new rows, by name                                 (AddTasks)
   queues      by key: NO KEY UPDATE (Finish, BlockQueue, Maintain), KEY SHARE (AddTasks,
               after inserting new rows in key order), UPDATE (queue cleanup, SKIP LOCKED)
3. tasks       new rows, by (run_no, fingerprint)                (AddTasks inserts)
4. backlogs    by (queue_key, run_no, pool_id)
5. backlog_codes by (queue_key, run_no, code)
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

## Operations

`$d` is always the run's `created_on`, so every query on `tasks` and `attempts` prunes to one
partition. `E` is the resolved `Effective` for (run, queue), from the replica's cache. The
arithmetic is `sched`'s, run in Go inside the transaction.

### Dispatch

**Candidates** (no locks, served by `backlogs_ready`):

```sql
SELECT queue_key, min(ready_at) AS ready_at
FROM (SELECT queue_key, ready_at FROM backlogs
      WHERE pool_id = $pool AND ready_at <= now()
      ORDER BY ready_at LIMIT 256) c
GROUP BY queue_key
ORDER BY ready_at
LIMIT 64;
```

The inner limit bounds backlogs, the outer one queues, so a queue with many runs cannot
crowd the others out of the candidate list, and queues come back in `ready_at` order.

The replica tries candidate queues in `ready_at` order, one transaction per queue, until the
session's slots are used or the candidates run out:

```sql
-- 1. Queue, or pass over it if another operation holds it (see Waiting).
SELECT tat, in_flight, vclock, blocked_until FROM queues
WHERE key = $k FOR NO KEY UPDATE SKIP LOCKED;

-- 2. All its backlogs with pending tasks, every pool, locked in key order.
SELECT * FROM backlogs
WHERE queue_key = $k AND pending > 0
ORDER BY run_no, pool_id
FOR UPDATE;

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
   `ready_at = NULL` (this heals an add that raced a pause). Before it does that, the
   dispatch locks those `runs` rows `FOR SHARE` and reads them again: a resume either
   committed before, and the backlog stays, or waits for this transaction, and its sweep
   then finds the backlog. Without the lock a resume sweep could pass a backlog just
   before a dispatch that still saw the pause takes it out. For each remaining backlog,
   compute admission (ADR 0001: block, shared or own GCRA, concurrency) and the number
   admitted. Take the admitted backlog with the lowest start tag `greatest(vtime, vclock)`
   across every pool:

   - **in the session's pool:** dispatch from it, `n` = min(slots, admitted);
   - **in another pool, inside its first-refusal period** (ADR 0001; counted from
     `greatest(ready_at, the limiter's eligible time)`): set this pool's backlogs on the
     queue to `ready_at` = the end of the period, `NOTIFY` the other pool's channel, commit
     and go to the next candidate queue;
   - **in another pool, period over:** charge it one task
     (`vtime = greatest(vtime, vclock) + 1 / weight`, `vclock = greatest(vclock, start)`)
     and pick again.

   If concurrency, not time, is what refuses a backlog, its `ready_at` becomes `'infinity'`.

5. **Claim:**

   ```sql
   WITH picked AS MATERIALIZED (
     SELECT task_id FROM tasks
     WHERE run_created_on = $d AND run_no = $r AND queue_key = $k AND pool_id = $c
       AND state = 'pending' AND not_before <= now()
       AND (expires_at IS NULL OR expires_at > now())
     ORDER BY priority DESC, task_id
     LIMIT $n
     FOR UPDATE SKIP LOCKED)
   UPDATE tasks t SET state = 'in_flight', attempt = t.attempt + 1, lease_nonce = $nonce,
          worker_id = $w, worker_version = $v, dispatched_at = now(),
          lease_expires_at = now() + $lease, lease_deadline = now() + $max_lease
   FROM picked WHERE t.run_created_on = $d AND t.task_id = picked.task_id
   RETURNING t.task_id, t.attempt, t.depth, t.parent_task_id, t.parent_bucket, t.spec;
   ```

   The picked rows are materialised on purpose. Written as `WHERE (…) IN (SELECT … LIMIT $n
   FOR UPDATE SKIP LOCKED)`, the planner may run the sub-select again for every outer row,
   and the statement then claims more than `n` tasks. The same form is used wherever
   `Maintain` changes a bounded batch of tasks.

   If it returns fewer than `n` rows, recompute this backlog's `ready_at` from `tasks_next`
   (the earliest `not_before` among non-expired pending tasks, or `NULL`) and, if slots
   remain, try the next admitted backlog on the queue. This stops a backlog of expired tasks
   from being selected again before `Maintain` expires them.

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
     own_tat = CASE WHEN $own THEN greatest(own_tat, now()) + $n * $T_own ELSE own_tat END,
     ready_at = $ready                                  -- computed in Go
   WHERE queue_key = $k AND run_no = $r AND pool_id = $c;
   ```

   and, for the other locked backlogs on the queue, `ready_at = greatest(ready_at,
   <their next admission time>)`. That value stays early (never late) because it combines a
   stored early value with the exact politeness time.

7. Commit, then send the assignments.

**Waiting.** The pool's dispatch loop (one per pool on each replica, ADR 0003) waits for the
first of:

- a notification on `frontier_work_<pool name>`. A notification for a pool whose sessions
  have free credits runs the candidate query at once, not on the next loop tick (Temporal's
  sync match); notifications are coalesced to one run per loop iteration;
- `NextReadyAt`, the earliest future `ready_at` in the pool below `'infinity'` (one index
  probe on `backlogs_ready`);
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
     `duplicates += dup`, `over_budget += rejected`;
   - `ready_at = least(ready_at, greatest(now(), min_not_before))`. This is done for a
     paused run too: dispatch takes the backlog out again the first time it meets it.
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
   - if the run is `cancelled` or `expired`, the task becomes `cancelled` / `expired`
     instead, the attempt is recorded as `revoked` and the call reports the lease as
     `STALE`: the result the worker sent is not applied.
5. **Attempt row** insert.
6. **Queue:** `in_flight -= 1`; if `Block > 0`:
   `blocked_until = greatest(blocked_until, now() + $block)`, `block_reason = $code`.
7. **Backlog** of the task: `in_flight -= 1`, result counter `+= 1`, `queue_blocks += 1` if
   blocked; for a retry `pending += 1` and
   `ready_at = least(ready_at, now() + $retry_in)`. **Other locked backlogs on the queue:**
   lower `ready_at` to their next admission time if the freed slot changes it (this is what
   brings a backlog back from `'infinity'`), or raise it to the end of a new block.
8. **`backlog_codes`:** `count += 1` for the outcome code.
9. **Completion** if the backlog now has `pending = 0 AND in_flight = 0` (see below).
10. Commit; `NOTIFY` if a backlog became ready.

### Lease lookup

`SELECT run_no, queue_key, pool_id, attempt, depth, state FROM tasks WHERE run_created_on = $d
AND task_id = $t`, with the key decoded from the lease ID. One primary key read, no locks.
The core calls it only when a `Complete` arrives for a lease this replica did not dispatch;
`Finish` does the fencing.

### Extend lease

Lock the task by lease, check validity as in Finish, then
`lease_expires_at = least(now() + $by, lease_deadline)`. One row, no other locks. A lease
of a run that was cancelled or has expired is not extended: it is reported as `STALE`.

### Block queue

The operator call follows the lock order of Finish: create the queue row if it is missing,
lock it `FOR NO KEY UPDATE`, lock its backlogs with pending tasks, set or clear
`blocked_until`. A block raises the `ready_at` of those backlogs to its end. Lifting or
shortening a block lowers every `ready_at` that lies in the future to `now()`: a value
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

`PoolStats` serves the metrics and runs on every scrape, so it reads backlogs and never
tasks. The totals per pool are one grouped read over the backlogs that have work
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
  meets a backlog of a paused run sets its `ready_at = NULL` (dispatch step 4), which takes
  it out of the readiness index the first time it comes up.
- **ResumeRun:** clear `paused`, set `resuming`. The replica that served the call then runs
  the resume sweep, and `Maintain` finishes it if that replica dies: in batches, each its
  own transaction in primary key order,
  `UPDATE backlogs SET ready_at = now() WHERE run_no = $r AND pending > 0`,
  with a `NOTIFY` per pool. Each batch re-reads the run and stops if it was paused again.
  The last batch clears `resuming`. The sweep resets every backlog with pending tasks and
  not only the ones with `NULL`: nothing keeps the readiness of a paused run up to date,
  so a backlog may still wait (`'infinity'`) for a slot that was freed during the pause,
  or for a block that was lifted. Dispatch works out the readiness again.
- **CancelRun:** `state = 'cancelled'`, `finished_at = now()`. Returns at once; dispatch
  skips the run's backlogs as for a paused run, and `Maintain` drains them.
- **Deadline:** `Maintain` finds runs past `deadline` (`runs_active`) and ends them like
  cancel with `state = 'expired'`.

Replicas learn about `CANCELLED` and `EXPIRED` from `run_events` and push `LeaseRevoked` for
the leases they dispatched to their sessions.

### Maintain

Each job processes a bounded batch per call and is safe on all replicas at once (row locks
with `SKIP LOCKED`, or `pg_try_advisory_xact_lock` per job):

| Job | Work |
|---|---|
| Task TTL | Per backlog with expired pending tasks (`tasks_ttl`): lock queue, backlog, expire tasks `SKIP LOCKED`, adjust `pending` / `expired`, recompute `ready_at`, completion check |
| Resume sweep | For runs with `resuming` (`runs_resuming`): the batches described under ResumeRun |
| Cancel and deadline drain | Per backlog of a `cancelled` / `expired` run (`runs_draining`, `backlogs_active`): pending tasks to `cancelled` / `expired`; in-flight tasks to the same state with a `revoked` attempt; release `queues.in_flight`; adjust the ledger |
| Partitions | Create next month's `tasks` and `attempts` partitions; drop partitions past retention, except in months where a run is still `open` / `sealed` or has tasks to drain |
| Queues | Delete `queues` rows with no backlog rows and `last_used_at` older than a day: lock the candidates `FOR UPDATE SKIP LOCKED` (an add holding `KEY SHARE` makes the row skipped), re-check that no backlog exists, then delete |
| Replicas | Delete replicas not seen for an hour |
| Ledger audit | Compare a sample of backlogs' `pending` / `in_flight` with counts from `tasks`, and count backlogs without a `queues` row; report both (they should be zero). One statement, so it sees one snapshot and can run under load |

### Start

Upsert the replica row. Then, in one transaction under `pg_advisory_xact_lock(3, 0)`: read
the hash with the latest `activated_at`; if it is not this replica's, upsert this hash with
`activated_at = now()`. That covers a new set and a rollback to an earlier one, and only the
first replica of a rollout does it.

If the set changed, `UPDATE backlogs SET ready_at = now() WHERE pending > 0 AND ready_at IS NOT NULL`
(in primary key order, in batches, each its own transaction; `'infinity'` is reset too,
backlogs of paused and ended runs keep `NULL` on purpose) and `NOTIFY` every pool channel.

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
- **Forward only:** the command applies migrations and reports their state. There is no
  command to roll a migration back.
- **Partitions:** the monthly partitions of `tasks` and `attempts` are not migrations.
  The store creates and drops them as time passes.

## Retention

- `tasks` and `attempts`: monthly partitions by `run_created_on`, dropped (or archived)
  after the retention period. No bulk `DELETE`. A month is kept while one of its runs is
  not ended or still has pending or in-flight tasks, because their tasks live in that
  partition and a lease of a dropped task could never be finished.
- `runs`, `run_events`, `backlogs`, `backlog_codes`: deleted with the run after a longer
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
- **Autovacuum:** per table on `tasks` partitions, `backlogs` and `queues`:
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
- backlog age per pool: the oldest `ready_at` in the past among `backlogs_ready`;
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
3. **Sharded primaries.** Each shard holds `queues`, `backlogs`, `backlog_codes`, `tasks`
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
  late", enforced by every operation above and checked by `storetest`'s property test.
- `GetRun` and `WatchRuns` stats cost a scan of the run's backlogs instead of one row, which
  grows with the queues a run has work on (ADR 0001, Scaling model and limits).
- Run IDs and fingerprints are compact in indexes (`run_no`, 16 bytes); the API keeps the
  text `run_id`.
- The schema already carries `bucket` and bucket-routable IDs, so the scaling steps need no
  migration of existing rows.

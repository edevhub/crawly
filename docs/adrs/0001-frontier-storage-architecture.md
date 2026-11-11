# ADR 0001: Frontier storage architecture

- **Status:** Proposed
- **Date:** 2026-11-09
- **Related:** [`api/crawly/frontier/`](../../api/crawly/frontier/) (proto packages),
  [ADR 0002: PostgreSQL store](0002-postgresql-store.md),
  [ADR 0003: Worker protocol and SDK](0003-worker-protocol-and-sdk.md)
- **Naming:** the proto follows buf's standard lint rules and AIP naming. Enum values are written here without their proto
  prefix (`RETRY` is `Outcome.KIND_RETRY`, `SEALED` is `Run.STATE_SEALED`), and the model's
  timestamps by their short names (`expires_at` is `Lease.expire_time`, `not_before` is
  `TaskSpec.not_before_time`, `deadline` is `RunLimits.deadline_time`).
- **Revision:** replaces the first draft of this ADR ("Frontier storage on PostgreSQL") after
  peer review. The PostgreSQL schema and transactions moved to ADR 0002. Round 2 of the
  review and its Temporal comparison replaced capability classes with named pools and
  fixed fairness, dispatch atomicity and message ordering. Round 3 added the lease lookup,
  one dispatch loop per pool, fairness across pools and the retry after a contended queue.

## Context

The Frontier holds all crawling state and hands work to a fleet of stateless workers over a
bidirectional gRPC stream. Schedulers create runs, seed them and seal them. Workers lease
tasks, report outcomes and add discovered tasks to the same run.

The state has to support:

- **Runs** with a lifecycle (`OPEN → SEALED → COMPLETED | CANCELLED | EXPIRED`), budgets, a
  deadline, an immutable policy override, stats and an event stream.
- **Tasks** deduplicated per run, with lineage (depth, parent), retries and fenced leases.
- **Queues** identified by an opaque, global key. Politeness state is shared by every run
  with work on the queue.
- **Dispatch**: for a free worker slot, pick the next task that respects politeness, is fair
  between the runs on its queue and belongs to the worker's pool.

The first draft put all of this in PostgreSQL tables and let the Frontier's code talk to
them directly. Review found three correctness bugs in it (deadlocks from an inconsistent
lock order, fairness at the wrong scope, run-level hot rows) and a structural problem: the
scheduling rules, the transaction design and the scaling options were one piece. This
revision separates them:

- **This ADR:** what the Frontier needs from storage, as a Go interface with precise
  semantics, and the scheduling model every implementation must follow.
- **ADR 0002:** how PostgreSQL implements that interface.

## Usage context

This section describes the system the Frontier serves, so the design can be evaluated
against the workloads it must carry. It names no particular client: use cases build on
these shapes, and the design must not depend on any one of them.

### The framework

Crawly is a distributed crawling framework written in Go. Existing frameworks (Scrapy,
Colly, Geziyor) are built for one-off crawls on a single machine: the queue, politeness and
dedup live in the crawler process. Crawly separates scheduling from fetching so that any
number of stateless workers can share one crawl, while politeness towards each site holds
across the whole fleet.

### Components

| Component | Responsibility | State |
|---|---|---|
| **Scheduler** | Owns the rules of its use case: when to crawl, which URLs to seed, run-level policy overrides. Creates, seeds and seals runs | Its own |
| **Frontier** | Holds the crawl state: runs, tasks, leases, dedup, politeness, run stats. Dispatches work to workers. Knows nothing about the use case | **The `Store` defined here** |
| **Worker** (spider) | Fetches and parses what it is given. Knows the source's markup and link structure, classifies outcomes (success, retry, permanent failure, queue block), adds discovered URLs to the same run. Never decides *when* to fetch | None (stateless) |
| **Item pipeline** | Receives extracted items from workers and passes them downstream | Outside the Frontier |
| **Raw storage** | Object storage for raw responses (`response_ref`), for lineage and replay | Object storage |

```
  ┌────────────┐ CreateRun / AddTasks / SealRun   ┌──────────────────────────┐
  │ Scheduler  │─────────────────────────────────▶│     Frontier replicas    │
  └────────────┘     WatchRuns (events, stats)    │  core: sessions, policy, │
               ◀──────────────────────────────────│  outcomes, loops         │
                                                  │  ──────── Store ──────── │
                                                  │  PostgreSQL (ADR 0002)   │
                                                  └────────────┬─────────────┘
                                            Work (bidi stream) │
                                  Assignment ▼   ▲ Complete / Discover
  ┌───────────────┐    items      ┌──────────────────────────┐
  │ Item pipeline │◀──────────────│   Workers (N, stateless) │──▶ target sites
  └───────────────┘               └────────────┬─────────────┘
                                               └──▶ raw responses (object storage)
```

Only the Frontier touches its store. Workers and schedulers talk to it only through the gRPC
API.

### Workload shapes

The design must carry these shapes, alone and mixed:

1. **Growing runs.** The scheduler seeds a few entry points and workers discover the rest
   by following links. The run grows while it runs, so its size is not known in advance. It
   ends when discovery dries up or a limit stops it.
2. **Fixed runs.** The scheduler seeds every URL and workers discover nothing. The size is
   known when the run is sealed. Such runs often carry a deadline.
3. **Single-task runs.** One URL fetched on demand, often under a policy override.

Runs of any shape can target the same sites at the same time. Politeness limits are per
site, not per run, so concurrent runs on one host share one politeness state. A run can
deliberately override the limits.

### Lifecycle of a run

```
1. A scheduler creates a run with labels and limits, streams its seeds and seals it.
2. Workers lease tasks. The Frontier dispatches at each site's limit (e.g. 1 RPS,
   concurrency 2), shared with every other run on that site.
3. A site answers 429 with Retry-After: 120. The worker reports RETRY with
   block_queue_for=120s. All runs pause dispatch on that site for 2 minutes.
4. Workers add discovered links to the run. Dedup drops the ones the run already has.
5. The last task finishes. Nothing is pending or in flight on a sealed run: the run
   completes, and WatchRuns emits COMPLETED with final stats.
6. Whatever consumes the results reads the run's stats, outcome codes and items and
   decides what they mean.
```

### Why runs

Runs are the unit of observability and lineage, not just of scheduling:

- **Tracing:** every task, attempt and item carries the run ID, so any extracted value can
  be traced to the run, attempt, worker version and raw response that produced it.
- **Alerting:** error rate, outcome codes and yield (items per page) per run are the health
  signal for a spider. A drop in yield with 200 responses usually means the site layout
  changed.
- **Comparison:** runs are compared outside the Frontier (dashboards, anomaly detection)
  using `ListRuns`, `GetRun` and run labels: new URLs, failures, duration, throughput.
- **Ownership:** the scheduler decides what goes into each run, and the consumer of the
  results decides what a missing or failing URL means. Spiders and the Frontier don't make
  such decisions.

### What the Frontier is not

- Not a store of domain data: it does not keep extracted items or decide what they mean.
- Not a long-term URL database: dedup is per run, and task history is kept for a retention
  period.
- Not a content store: raw responses go to object storage and items to the pipeline.
- Not a global URL database: there is no "every URL ever seen" set. State is scoped to
  runs, whatever their size, and what to revisit is the scheduler's decision.
- Not a policy management service: policies are configuration, changed by deploying.
- Not an authentication layer: who may call which service is settled in deployment (mTLS,
  a gRPC interceptor). The protocol and the store carry no identities.

### Scale

The architecture targets no particular size. It is built to grow by adding partitions,
Frontier replicas and workers, and [Scaling model and limits](#scaling-model-and-limits)
states what scales out, what does not, and which costs grow with size. Sizing figures
belong to an implementation and its deployment (ADR 0002 has them for the first one).

## Decision

1. **One `Store` interface is the Frontier's only stateful dependency.** Each method is one
   business transaction (dispatch, finish an attempt, add tasks, seal a run…) and is atomic
   (`Dispatch` per queue it dispatches from).
   The Frontier core never composes several store calls into one logical change, so it
   never needs a transaction handle, a lock or a retry loop. How a method is made atomic
   (one PostgreSQL transaction, a FoundationDB transaction, a Redis script plus a
   PostgreSQL transaction with compensation) is the implementation's business.
2. **The scheduling model is defined here, independent of storage** (see
   [Scheduling model](#scheduling-model)). Its arithmetic (GCRA, fair-queueing clocks,
   readiness) lives in one pure Go package, `sched`, which every implementation calls.
   Implementations decide where the state lives and how it is updated atomically; they don't
   reinvent the rules.
3. **One conformance suite** (`storetest`) runs against every implementation. An
   implementation is correct when it passes it (see [Conformance suite](#conformance-suite)).
4. **The first implementation is PostgreSQL on a single primary** (ADR 0002). It is the
   simplest engine that passes the suite, not the size the design is meant for. Sharded
   PostgreSQL, PostgreSQL + Redis and FoundationDB are known to fit the interface
   ([Implementations](#implementations)) and are not built ahead of need.
5. **Policies are static.** Global policies are loaded from configuration when a replica
   starts. A run's policy override is given at `CreateRun` and never changes. There is no
   reload and no policy-writing RPC (see [Policies](#policies)).
6. **Fairness is per queue.** `RunPolicy.weight` is a run's share of each queue it has work
   on, relative to the other runs on that queue. There is no fleet-wide share of worker
   slots between runs (see [Consequences](#consequences)).
7. **No hot path writes run-wide or fleet-wide state** (see
   [Locality rule](#locality-rule)). This is what keeps a single database contention-free
   and what makes every implementation shardable by queue.
8. **Workers subscribe to named pools**, as Temporal workers poll a task queue. A task names
   its pool; a worker session serves exactly one pool. There is no capability matching
   (ADR 0003).

## Responsibilities

| Frontier core | Store |
|---|---|
| gRPC services, worker sessions (one pool each), credits, `Drain` | Durable state of runs, tasks, attempts, queues |
| Handling a session's inbound messages in stream order (ADR 0003) | Atomicity of every operation (per queue for `Dispatch`) |
| Canonicalising requests, fingerprints, queue keys (`KeyRule`), pool names | Dedup on insert |
| Loading, validating, resolving and explaining policies | Applying resolved limits during dispatch (politeness, fairness) |
| Turning an `Outcome` into a transition: retry or fail (`max_attempts`), backoff and jitter, `retry_after`, `block_queue_for`. The run and queue of a lease come from the replica's cache of its own dispatches, or from `Work.Lease` | Lease fencing, lease expiry, extension caps |
| One dispatch loop per pool on each replica: it holds the free credits of the pool's sessions, asks for work at once on a notification, otherwise at `NextReadyAt`, with a fallback poll every second, and hands each assignment to a session | Selecting and claiming tasks; readiness index |
| Reaper loop: expired leases → retry or fail | Finding expired leases |
| `WatchRuns`: events plus periodic stats snapshots | Ordered, gap-free run event log; aggregated stats |
| Pushing `LeaseRevoked` for runs that were cancelled or expired | Run limits (`max_tasks`, `max_depth`, `deadline`), run termination |
| A per-replica budget of store calls: when exhausted, fail the call with `UNAVAILABLE` instead of queueing, so a slow store does not become a retry storm | The clock (`Now`) and background maintenance |
| Metrics, logging, tracing | |

The store never interprets an `Outcome`: it receives a decided transition. The core never
reads a timestamp from its own clock to schedule anything: it passes durations, and the
store applies them with its own clock.

## Store interface

The interface is split by consumer, so each part of the core depends only on what it uses.
Types are abbreviated; IDs are opaque strings produced by the store.

```go
package store

// Store is everything the Frontier keeps between requests.
type Store interface {
	Lifecycle
	Work
	Inspect
	Background
}

// Lifecycle serves RunService. Every method is atomic and idempotent where noted.
type Lifecycle interface {
	// CreateRun is idempotent on spec.ID: the same spec returns the existing run, a
	// different spec (compared by spec.Hash) returns ErrConflict.
	CreateRun(ctx context.Context, spec RunSpec) (Run, error)
	// SealRun stops seeding. An empty sealed run completes in the same call.
	SealRun(ctx context.Context, id RunID) (Run, error)
	PauseRun(ctx context.Context, id RunID) (Run, error)
	ResumeRun(ctx context.Context, id RunID) (Run, error)
	// CancelRun reports CANCELLED immediately; the backlog drains in Maintain.
	CancelRun(ctx context.Context, id RunID) (Run, error)
	GetRun(ctx context.Context, id RunID) (Run, error)
	ListRuns(ctx context.Context, f RunFilter, page Page) ([]Run, PageToken, error)
	// RunEvents returns lifecycle events after the cursor, in commit order, without gaps.
	RunEvents(ctx context.Context, after EventCursor, limit int) ([]RunEvent, error)
}

// Work serves WorkerService, AddTasks and the reaper.
type Work interface {
	// AddTasks adds seeds (req.Parent == nil; run must be OPEN) or discovered tasks
	// (req.Parent set; the parent lease must be live). One result per task, in order.
	AddTasks(ctx context.Context, req AddRequest) ([]AddResult, error)
	// Dispatch claims up to req.Slots tasks of req.Pool, following the scheduling model.
	// It never blocks waiting for work. It is atomic per queue, not per call: it can
	// return assignments together with an error, and the caller must deliver every
	// assignment it returns, because those claims are committed.
	Dispatch(ctx context.Context, req DispatchRequest) (DispatchResult, error)
	// Lease returns what the core needs to decide a transition for a lease it did not
	// dispatch itself (a worker reconnected to another replica): run, queue, pool,
	// attempt, depth. A plain read: it does not fence, Finish does.
	Lease(ctx context.Context, lease LeaseRef) (LeaseInfo, error)
	// ExtendLease moves the expiry to now+by, capped by the lease deadline.
	ExtendLease(ctx context.Context, lease LeaseRef, by time.Duration) (Lease, error)
	// Finish ends an attempt with a transition decided by the core. Fenced: a lease that
	// is not the task's current one returns Stale or Unknown and changes nothing.
	Finish(ctx context.Context, f Finish) (FinishStatus, error)
	// ExpiredLeases lists leases past their expiry, oldest first, for the reaper.
	ExpiredLeases(ctx context.Context, limit int) ([]ExpiredLease, error)
}

// Inspect serves GetTask, ListTasks and QueueService.
type Inspect interface {
	GetTask(ctx context.Context, ref TaskRef) (TaskStatus, error)
	ListTasks(ctx context.Context, f TaskFilter, page Page) ([]TaskStatus, PageToken, error)
	GetQueue(ctx context.Context, key QueueKey) (Queue, error)
	ListQueues(ctx context.Context, f QueueFilter, page Page) ([]Queue, PageToken, error)
	// BlockQueue sets (or with a zero time, clears) an operator block for all runs.
	BlockQueue(ctx context.Context, key QueueKey, until time.Time, reason string) (Queue, error)
	// PoolStats sums the work of every pool across all runs, for monitoring: tasks
	// pending, delayed and in flight, and the queues whose work is ready, longest ready
	// first, up to a limit. It never reads tasks.
	PoolStats(ctx context.Context, dueLimit int) ([]PoolStats, error)
}

// Background serves replica startup, the dispatch loop and maintenance.
type Background interface {
	// Start registers the replica. A policy set that differs from the one registered
	// last (a new set or a rollback) resets readiness, so no backlog stays parked by a
	// limit that no longer applies.
	Start(ctx context.Context, r Replica) error
	// Notifications delivers coalesced wake-ups: work became ready in a pool, or a
	// run event was appended. A missed notification only delays work until the next
	// DispatchResult.NextReadyAt or poll.
	Notifications(ctx context.Context) (<-chan Notification, error)
	// Maintain runs one bounded round of background work: task TTL expiry, run
	// deadlines, cancel drains, retention. Safe to call concurrently from all replicas.
	Maintain(ctx context.Context) (MaintainReport, error)
	// Now is the store clock, the only clock used for scheduling.
	Now(ctx context.Context) (time.Time, error)
}
```

The store is constructed with the resolved policy view, which it consults during dispatch:

```go
// Policies is pure and immutable for the lifetime of the process.
type Policies interface {
	// Hash identifies the policy set (canonical encoding of all global policies).
	Hash() string
	// For resolves the limits for a run's tasks on a queue. Results are cacheable forever.
	For(run RunID, rp *policyv1.RunPolicy, queue QueueKey) Effective
}

type Effective struct {
	Limits Limits        // what this run's tasks are admitted against
	Own    bool          // Limits come from a run override: admitted on the run's own allowance
	Queue  Limits        // the queue's shared limits, consumed by every dispatch
	Weight float64       // fair share on this queue
	Lease  LeaseSettings // duration and maximum duration
}

type Limits struct {
	Interval       time.Duration // T = max(1/rate_per_second, min_delay); 0 = unlimited
	Burst          int           // b >= 1
	MaxConcurrency int           // 0 = unlimited
}
```

The main request and result types:

```go
type RunSpec struct {
	ID      RunID
	Labels  map[string]string
	Policy  *policyv1.RunPolicy // immutable after creation
	Limits  RunLimits             // deadline, max_tasks, max_depth
	KeyRule KeyRule
	Paused  bool
	Hash    []byte                // canonical hash of the fields above
}

type AddRequest struct {
	Run    RunID
	Parent *LeaseRef // nil: seeds; set: discovered, depth = parent depth + 1
	Tasks  []NewTask
}

type NewTask struct {
	Fingerprint Fingerprint   // 16 bytes: hash(queue key ‖ canonical request or dedup_key)
	Queue       QueueKey
	Pool        PoolName      // TaskSpec.pool, default "default"
	Priority    int32
	NotBefore   time.Time     // zero: immediately
	TTL         time.Duration // resolved Behaviour.task_ttl; 0: none
	Spec        []byte        // serialized Request, handler and context
}

type DispatchRequest struct {
	Session SessionID  // chosen by the pool's dispatch loop before the call
	Worker  WorkerInfo // id and version, recorded on the attempt
	Pool    PoolName   // the session's pool
	Slots   int        // the session's free credits
}

type DispatchResult struct {
	Assignments []Assignment
	// Earliest time more work is expected in the pool. Zero: wait for a notification.
	// If the call passed over a queue or task that another operation held, this is a
	// few milliseconds from now: nobody will send a notification for that work.
	NextReadyAt time.Time
}

type LeaseRef struct {
	ID      LeaseID // unique per dispatch, never reused
	Attempt uint32
}

type Finish struct {
	Lease   LeaseRef
	Source  FinishSource  // Worker: lease must be live. Reaper: lease must have expired.
	Result  TaskResult    // Succeeded, Failed, Retry or Released
	RetryIn time.Duration // Retry only: not before now+RetryIn
	Block   time.Duration // > 0: block the task's queue for all runs, extending any block
	Record  AttemptRecord // outcome code and message, fetch info, worker, policy set hash
}
```

### Errors

`ErrNotFound`, `ErrConflict` (idempotency mismatch), `ErrRunClosed` (operation not allowed
in the run's state), `ErrUnavailable` (transient: the core may retry the whole call).
Implementations retry their own internal conflicts (deadlocks, serialization failures,
contention); the core sees them only as latency. Per-task outcomes of `AddTasks` and
fencing results of `Finish` are results, not errors. `Dispatch` is the one method that can
return a partial result with an error (it commits per queue); every returned assignment is
committed and must be sent.

## Scheduling model

The rules every implementation follows. `sched` implements the arithmetic.

### Terms

- **Queue**: a politeness domain, identified by its key. Holds the shared politeness state:
  a GCRA timestamp `tat`, the number of tasks in flight, a block (`blocked_until`, reason)
  and a fair-queueing clock `vclock`.
- **Pool**: a named set of workers that execute the same kind of task, e.g. `default` or
  `browser`. A task names its pool; a worker session serves one pool. A worker that lacks
  the task's handler hands it back for another worker of the pool (ADR 0003).
- **Backlog**: the tasks of one run on one queue in one pool. Holds `pending`
  (not dispatched, including delayed), `in_flight`, a fair-queueing tag `vtime`, an own
  GCRA timestamp `own_tat` (used only under a run override), counters for stats, and
  `ready_at`.
- **Bucket**: `hash(queue key) mod 256`. Every piece of hot state belongs to exactly one
  queue and therefore one bucket. Implementations may use buckets to spread or shard state;
  the model does not depend on them.

**For readers who know Temporal:** a pool is a task queue, a credit is a slot (an
outstanding poll), a lease is an activity task token plus attempt, `lease_duration` is the
heartbeat timeout, `max_lease_duration` is the start-to-close timeout, `task_ttl` is close
to the schedule-to-close timeout (measured from when the task is added, across retries, but
enforced only while the task is pending), and `Behaviour` is the retry policy, with the
spider's `FAILED` playing the part of a non-retryable error. A Crawly queue (a host) has no
Temporal counterpart: Temporal rate-limits per task queue, which is a worker pool, not per
external resource.

### Politeness (GCRA)

For limits with interval `T`, burst `b` and tolerance `τ = (b − 1)·T`, at time `now`:

- **Admitted** count: `n_max = floor((now + τ − max(tat, now)) / T) + 1`, at least 0 and at
  most `b`. Unlimited (`T = 0`) admits any number.
- **On dispatch of `n`**: `tat = max(tat, now) + n·T`.
- **Next eligible time**: `tat − τ`.

A backlog's tasks are admitted when all of these hold:

1. the queue is not blocked: `now ≥ blocked_until`. Blocks apply to every run, overridden
   or not, because a block is the site's state, not a policy;
2. without an override: the queue's `tat` admits them under the queue's limits, and
   `queue.in_flight < max_concurrency`;
3. with an override (`Effective.Own`): the backlog's `own_tat` admits them under the
   override's limits, and `backlog.in_flight < max_concurrency` of the override. Other runs'
   traffic is ignored: an override **replaces** the queue's limits for that run;
4. the claimed tasks have `not_before ≤ now` and have not expired.

Every dispatch **counts towards the queue's shared state**, override or not:
`queue.in_flight += n` and
`queue.tat = min(max(queue.tat, now) + n·T_queue, now + b_queue·T_queue)`.
The cap means traffic under a looser override can saturate the queue for other runs while
it lasts, but once it stops they wait at most one interval `T_queue` for their next slot.
Without the cap, an hour of override traffic at ten times the queue's rate would block the
other runs for nine hours after it stopped.

Politeness is guaranteed at dispatch time, by the store clock. With no prefetch and one
stream message per assignment, the time between dispatch and fetch varies by milliseconds;
policies' intervals are seconds. `Assignment` carries no start time.

### Fairness (start-time fair queueing per queue)

The clamp happens when a backlog is picked, not when it becomes active. Among the admitted
backlogs on a queue, dispatch picks the one with the lowest **start tag**
`start = max(vtime, queue.vclock)` (ties: lower run, then pool). After dispatching `n` of its
tasks:

- `backlog.vtime = start + n / weight`
- `queue.vclock = max(queue.vclock, start + (n − 1) / weight)`

A batch of `n` counts as `n` tasks served one after the other, so the clock moves to the
start tag of the last one. Moving it only to the batch's start would let one large batch
leave the clock behind, and a backlog that becomes eligible afterwards would take the queue
over.

A backlog that could not be picked for a while, for whatever reason (no tasks, paused, a far
`not_before`, an exhausted override allowance), re-enters at the queue's clock. It cannot
bank credit and take the queue over when it becomes eligible again. A new backlog starts at
`vtime = 0`, which the clamp turns into the clock.

With two runs of equal weight on one queue, each gets half of the queue's admitted
dispatches, however many other queues either run has.

**Across pools.** The pick looks at the admitted backlogs of every pool on the queue, not
only the pool being dispatched for. When the lowest start tag belongs to a backlog of
another pool, that pool has **first refusal**: the dispatch takes nothing from the queue,
wakes the other pool, and makes its own backlogs on the queue ready again when the refusal
period ends. The period is 100 ms (a quarter of the queue's interval if that is shorter),
counted from when the favoured backlog became admissible. If the other pool has not taken
its turn by then (it has no free worker), it **forfeits** the turn: the backlog is charged
one task (`vtime = start + 1 / weight`, `vclock = max(vclock, start)`) and the pick is
repeated. So runs in different pools share a queue by weight while both pools have free
workers, and a pool without free workers costs the others at most the refusal period per
forfeited turn, never the queue.

Fair shares and own allowances belong to **backlogs**, which are per (queue, run, pool). A
run with tasks in two pools on one queue holds two shares and, under an override, two
allowances there.

### Selection order

1. **Across queues:** the store keeps a readiness index of backlogs by `ready_at`, the
   earliest time the backlog could have a task admitted (task `not_before`, politeness,
   block, another pool's first refusal). A backlog that waits for an event, not a time (a
   concurrency slot), is kept out of the index until that event makes it ready. Dispatch
   takes queues in `ready_at` order. `ready_at` **may be early, never
   late**: an early value costs one refused check, a late one leaves work idle. Every
   change that can make work eligible sooner (new tasks, a freed concurrency slot, a
   retry, an unblock, a resume, a new policy set) lowers it in the same operation.
2. **Within a queue:** fairness, as above.
3. **Within a backlog:** higher `priority` first, then insertion order.

Tasks are claimed only from the session's pool. A queue's politeness state and fairness
clock are shared across pools.

**Fairness comes before priority.** Temporal does the opposite: strict priority tiers are
dispatched before fairness applies. For a crawler that would let a high-priority run take a
shared host from every other run, which is the problem per-queue fairness exists to
prevent. `TaskSpec.priority` orders tasks inside one run's backlog only.

### Leases

- A dispatch creates a lease with `expires_at = now + lease_duration` and a deadline
  `dispatched_at + max_lease_duration`. `attempt` increments on every dispatch of the task.
- **Lease IDs are unique per dispatch and never reused**, including after a store failover
  that loses a committed dispatch. A lease is valid only if its ID and attempt match the
  task's current lease, the task is in flight and `now < expires_at`. Anything else is
  `STALE` (the task exists) or `UNKNOWN`.
- `ExtendLease` sets `expires_at = min(now + by, deadline)`. At the deadline the core sends
  `LeaseRevoked{MAX_DURATION}`.
- An expired lease is finished by the reaper as an attempt with code `lease_expired`. It
  counts towards `max_attempts`, so a task that crashes every worker eventually fails.
- Delivery is at least once: a task may be fetched twice when a lease expires while a slow
  worker is still working. Fencing keeps only one result.

### Runs and termination

- A run's spec (labels, policy, limits, key rule) is immutable. Its mutable state is the
  lifecycle state and the `paused` flag.
- Seeds are accepted only while the run is `OPEN`. Discovered tasks are accepted while the
  parent lease is live, in `OPEN` or `SEALED` runs.
- **Discovered tasks are durable before the parent finishes.** The core handles one
  session's inbound messages in stream order: a `DiscoverBatch` is committed before any
  later message of the session (such as the parent's `Complete`) is handled. The store
  rejects discoveries whose parent lease is no longer live, and holds a lock on the parent
  while it adds them, so a `Complete` arriving through another session (after a reconnect)
  waits for them. After a reconnect the SDK resends unacknowledged batches before
  completing their parent (ADR 0003).
- **A run completes exactly when it is sealed and none of its backlogs has pending or
  in-flight tasks.** The check is atomic with the change that empties the last backlog (or
  with `SealRun`). No task is ever added to a completed, cancelled or expired run: a seed
  racing `SealRun` is either accepted before the seal or rejected with `RUN_CLOSED`.
- **Pause, resume and cancel change the run's state in one small step and take effect
  eventually.** They never touch the run's backlogs in the same operation, so their cost
  does not depend on the size of the run.
  - `PauseRun`: a dispatch that starts after it returns takes nothing from the run. A
    dispatch already under way may still hand out its tasks, and leases in flight continue.
  - `ResumeRun`: the run's backlogs are made ready again in background batches, so its
    queues come back over a short time, not at once.
  - `CancelRun` and a passed `deadline` end the run immediately (`CANCELLED`, `EXPIRED`).
    Pending tasks and leases are drained in the background: pending tasks are cancelled or
    expired, leases end as revoked attempts, and their completions return `STALE`. A
    dispatch under way when the run ends may hand out tasks; they are drained the same way.
- `max_tasks` is soft: a batch is rejected (`OVER_BUDGET`) once the run's task count has
  reached the limit, and the count may be stale by at most one second, so concurrent
  batches can overshoot by what they add in that second. `max_depth` is exact.
- Pending tasks past their TTL expire and are never dispatched.

### Dedup

A task is a duplicate when its run already has a task with the same fingerprint. The core
computes the fingerprint over the queue key and the task's dedup key, so **dedup is per run
and queue**.

The dedup key is `TaskSpec.dedup_key` when the client sets it. Otherwise it is the
**default key** of the request. For a GET that is the normalised URL: scheme + host + path
+ query parameters, with the parameters sorted alphanumerically ascending. Scheme and host
are lowercased; path and query keep their case. The host keeps an explicit non-default
port, and an empty path is `/`. The fragment is not part of it. A method other than GET
is put in front of the URL, in upper case, and the SHA-256 of a body that is not empty
behind it. A GET without a body is therefore its URL alone, and an empty method is a GET.
Headers are never part of the key. The Frontier stores the method in upper case, so the
request that is sent is the one the key counts.

Path and query are not lowercased because the two possible mistakes are not equal. A key
that is too loose merges two pages, and one of them is never crawled, with no signal but
one more duplicate among many. A key that is too strict costs one extra fetch, which shows
in the stats. Case carries meaning on many sites (short ids, encoded tokens, wiki titles,
object keys), and only scheme and host are case-insensitive by the URL standard.

Method and body are part of the key for the same reason. Search and GraphQL APIs page
through one URL with a cursor in the body; a key of the URL alone keeps the first page and
drops the others as duplicates. A body that differs only in formatting costs one extra
fetch.

The default is one rule for every site, computed in the Frontier so that seeds from a
scheduler and links from a spider hash the same way. It is deliberately simple, and it is
wrong for some sites: it merges requests that differ only in headers, and it keeps apart
URLs that differ only in case on a case-insensitive site, or only in tracking or session
parameters. Anything site-specific is the spider's job: it sets `dedup_key` whenever
special rules apply.
 With the `HOST` and `REGISTERED_DOMAIN` key rules the queue
key follows from the URL, so this is the same as per run. It differs only when a client
sets explicit queue keys and puts the same request on two queues, which then fetches it
twice, as asked. Scoping dedup to the queue keeps the dedup check inside one queue's
bucket, so no implementation needs a cross-partition write to add a task.

### Events and stats

- Run events (`CREATED`, `SEALED`, `PAUSED`, `RESUMED`, `COMPLETED`, `CANCELLED`, `EXPIRED`)
  have a cursor that is ordered by commit: a client that resumes from cursor `c` sees every
  event after `c`, exactly once per read. `STATS` events are not stored; `WatchRuns`
  produces them from `GetRun` reads at `stats_interval`.
- Stats are exact counters kept on backlogs and summed on read. A read is not a snapshot
  across backlogs: during dispatch, `pending + in_flight` can be off by the tasks moving
  between them.
- `RunStats.delayed` is the pending tasks of backlogs whose `ready_at` is in the future, and
  `RunStats.pending` the rest. It is approximate: `ready_at` covers politeness and blocks as
  well as `not_before`, and is per backlog, not per task.

### Monitoring

The Frontier exports metrics, and an operator can list the connected sessions. Neither is
part of scheduling.

- **What is counted where.** Events (dispatches, outcomes, lease expiries, queue blocks,
  hand-backs, tasks added, store calls, gRPC calls) are counted by the replica that handled
  them: a metric is per replica, and a total is the sum over replicas. What a replica holds
  in memory (sessions, credits, unfinished assignments) is per replica as well. What the
  store holds (tasks per pool and state, the age of the longest ready work) is global: every
  replica reads it through `PoolStats` at scrape time and reports the same figure. A
  figure that cannot be read is left out, not reported as zero.
- **Bounded labels.** Queue keys are unbounded, so no metric carries one. A queue is
  labelled by the most specific global policy that matches it, or by `builtin` when none
  does, so the number of values is the number of policies plus one. A queue that needs a
  series of its own gets a policy of its own. Values that workers choose (pool, worker id,
  outcome code) are capped per process, and the values past the cap are counted as one
  `other`. Run, task and URL are never labels.
- **What the store contributes.** A dispatched assignment carries how long its task was
  eligible before the dispatch, by the store clock, and a lease names the worker it was
  dispatched to, so the reaper can say whose lease expired. `PoolStats` reports readiness
  as the store keeps it: a backlog is ready when nothing but a worker is missing, and its
  ready time may be earlier than the true one, never later (see Politeness), so the age of
  ready work may be too long and never too short. It lists a bounded number of queues per
  pool, so a policy whose ready work is younger than all of them is not shown.
- **Sessions.** A session is a stream held by one replica, and is kept nowhere else.
  `ListSessions` answers for the replica that serves the call and names it. Keeping
  sessions in the store would add a write for every session and a way to clean up after a
  replica that died, for a view the metrics already give in aggregate.

### Locality rule

Every write on the hot path (dispatch, finish, extend, add, reaper) goes to state owned by
**one queue**: the queue itself, the backlogs on it, their tasks and attempts. Run-wide state
(the run's lifecycle, the event log) is written only on lifecycle transitions, which happen
tens of times a day. So:

- the write rate of any piece of state is bounded by one queue's dispatch rate, which
  politeness keeps low (typically ≤ a few per second);
- every hot operation touches one bucket, so an implementation can shard by bucket without
  distributed transactions on the hot path;
- run completion is the one place where a hot operation looks at the whole run, and it only
  does so when it has just emptied a backlog.

## Policies

- **Global policies** are loaded from configuration at replica startup: name, queue key glob,
  precedence, `QueueLimits`, `Behaviour`. Invalid configuration stops the replica from
  starting.
- **Run policy** (`RunPolicy`: weight, limits, behaviour, per-queue overrides) is set at
  `CreateRun` and is part of the run's immutable spec.
- **Resolution**, done in the core: matching global policies by glob specificity, then
  precedence; then the run's policy; then the most specific matching `RunQueueOverride`.
  Most specific wins, field by field. A run's limits (from `RunPolicy.limits` or an
  override) set `Effective.Own`.
- **`ExplainPolicy`** returns the effective values and which policy or override set each
  field. `ListPolicies` and `GetPolicy` are read-only views of the loaded configuration.
- **Changing a policy is a deploy.** During a rolling restart, replicas with the old and the
  new set dispatch side by side; each dispatch is admitted by its replica's limits against
  the shared state, so for the length of the rollout a queue is held to whichever of the two
  sets it is checked against at the moment. Every attempt records the hash of the policy set
  that dispatched it, and each replica exports its hash as a metric, so drift is visible.
- On `Start`, a replica registers its policy hash. If it differs from the hash registered
  last (a new set, or a rollback to an earlier one), the replica resets `ready_at` of active
  backlogs to now, so no backlog stays parked by another set's stricter limit (readiness
  must never be late).

## Implementations

What each candidate does for the parts that make storage hard. All of them keep the gRPC API
and the `Store` interface unchanged.

| Concern | PostgreSQL, single primary (ADR 0002) | Sharded PostgreSQL | PostgreSQL + Redis | FoundationDB |
|---|---|---|---|---|
| Atomic dispatch | One transaction per queue; queue row locked with `SKIP LOCKED` | Same, on the queue's shard | Redis script admits and advances politeness and fairness; PostgreSQL claims the task; refund script if the claim comes back short | One transaction |
| Readiness index | Partial index on backlogs by `ready_at` | Per shard; replicas own buckets and steal from others when idle | Sorted set per pool | Ordered keys `(pool, ready_at, queue)` |
| Dedup | Unique index on `(run, fingerprint)` | Same, single shard because the fingerprint includes the queue key | PostgreSQL unique index | Key `(run, fingerprint)` |
| Run completion | Per-run lock taken only when a backlog empties | Empty the backlog on the shard, then the run lock in a control database and a read of the run's backlogs on every shard it touched; the sweeper re-checks sealed runs after a crash in between | As single PostgreSQL | Serializable read of the run's backlog range |
| Seed vs seal race | Shared/exclusive per-run lock | The shared lock is taken in the control database and held while the shard commits | As single PostgreSQL | Conflict on the run key |
| Event order | Events serialized by a global lock (rare) | In the control database | PostgreSQL | Versionstamped keys |
| Notifications | `LISTEN`/`NOTIFY` | Per shard | Redis pub/sub | Watches |
| Lost-write risk | None with synchronous commit | None | Redis state lost on failover is rebuilt from PostgreSQL; a lost refund only makes a queue more polite | None |

**Work stealing instead of per-replica sessions.** With bucket ownership, each replica first
dispatches from the buckets it owns and only touches other buckets when its sessions still
have free credits. Workers keep connecting to any replica through one address; the protocol
needs no replica discovery.

**Ownership is fenced.** Bucket ownership carries a `range_id` that increases on every
change of owner. Every write a bucket owner makes checks it in the same transaction, and a
mismatch aborts with `ErrOwnershipLost`: the replica drops any in-memory state for that
bucket. A hand-over drains the old owner's in-flight writes before the new owner bumps the
id. This is Temporal's shard `range_id` and `ShardOwnershipLostError`, and it matters as
soon as an owner keeps readiness or politeness state in memory.

**When to move.** A single primary has a ceiling (ADR 0002 estimates and benchmarks it).
The trigger to build the next implementation is a measured one: dispatch p99, lock waits,
or CPU on the primary above the thresholds in ADR 0002 at the deployment's peak.
Sharded PostgreSQL is the default next step because it reuses ADR 0002's schema and
transactions per shard.

## Conformance suite

`storetest` is a Go test suite that takes a constructor for a `Store` and runs against every
implementation. Tests run with concurrent clients and an injectable clock where the store
supports it. Each one names the rule it proves:

| Test | Rule |
|---|---|
| Two runs with different limits on one queue: the queue's rate stays within its limits, `tat` never goes backwards | Shared politeness |
| A run with a looser override exceeds the queue limit for itself; other runs back off while it runs and resume within one interval after it stops | Override replaces, still counts, capped |
| A block stops every run on the queue, including overridden ones | Blocks |
| Run A on 500 queues and run B on one shared queue, equal weights: B gets 50% ± 10% of the shared queue over a minute | Per-queue fairness |
| A backlog idle, paused, or held back by a far `not_before` while another advances by 10,000 dispatches gets ≤ its fair share when it becomes eligible | No banked credit |
| Two runs of equal weight in different pools on one queue, both pools with free slots: each gets 50% ± 10%. With no free slot in one pool, the other keeps the queue's rate less the refusal period per forfeited turn | Fairness across pools |
| A session in pool A never receives pool B tasks; a task in a pool with no session waits and is dispatched on the first session for that pool | Pools |
| A dispatch that passes over a contended queue returns a near `NextReadyAt`; with notifications disabled, every ready task is still dispatched within the retry interval | No lost wake-up |
| Adding tasks races queue cleanup for a minute: no backlog is ever left without its queue state | No orphaned backlog |
| `Lease` on a lease dispatched through another `Store` instance returns its run, queue and attempt | Lease lookup |
| A `Dispatch` that fails after committing some queues returns those assignments | Dispatch atomicity |
| A report with a stale attempt or a reused lease ID is `STALE`; a dispatch lost in a failover never yields two valid leases | Fencing |
| A sealed run whose last two tasks finish concurrently on different queues completes every time, over 1,000 iterations, without `Maintain` | Termination |
| Seeds racing `SealRun`: each is either counted in the run or rejected; none is pending in a completed run | No orphaned tasks |
| Discoveries from an expired lease are rejected; accepted discoveries survive a parent retry | Lineage |
| `RunEvents` resumed from any cursor under concurrent transitions never misses or repeats an event | Event order |
| A backlog whose tasks have all expired stops being selected without waiting for `Maintain` | No spin on expired tasks |
| No `ready_at` is ever later than the true eligible time (property test over random operations) | Readiness never late |
| Pause, resume and cancel racing dispatch and finish: a dispatch that starts after pause or cancel returns takes nothing from the run; a task handed out during the race is drained or completes normally; after resume every backlog with pending tasks becomes ready | Lifecycle (eventual) |
| A policy set on `Start` that differs from the last one (new, or rolled back) makes every active backlog ready | Policy rollout |
| Concurrent dispatch, finish and discover across queues of one run for a minute: no internal conflict is surfaced as an error | Atomicity |

Rules that belong to the core, not the store (message order on a session, the dispatch loop,
outcome decisions), are tested against the in-memory store (ADR 0003).

Implementations with bucket ownership add: a stale owner's write after a `range_id` bump is
rejected and leaves no partial state. Implementations add their own tests on top (ADR 0002:
partition pruning, deadlock counter).

## Scaling model and limits

The architecture sets no size target. This section says how it grows and where it stops, so
a deployment of any size can be judged against it.

### What scales out

- **Queues, tasks and total dispatch rate.** Every hot write belongs to one queue
  ([Locality rule](#locality-rule)), so the state partitions by bucket with no distributed
  transaction on the hot path. Total throughput is the sum over queues; adding partitions
  adds throughput.
- **Frontier replicas.** They hold no state between requests. Any replica serves any worker
  or scheduler; bucket ownership with work stealing spreads dispatch between them.
- **Workers and pools.** Stateless, added and removed freely; a pool is a name.
- **Runs.** Independent of each other; many runs cost no shared hot state.
- **Storage engines.** The `Store` interface and `storetest` let an engine be replaced
  without touching the core or the API.

### What does not scale out, by design

- **One queue's throughput.** A queue is a politeness domain: its rate is capped by its
  limits, and its state lives in one partition and is updated serially. That is the point
  of a queue.
- **The number of partitions.** State is spread over 256 buckets, so at most 256 shards.
  Changing the bucket count means rehashing queue keys and reissuing task IDs.
- **Control state.** Runs and their event log live in one place, and events are ordered by
  one lock. Its load is the rate of lifecycle transitions (create, seal, complete), not of
  tasks.

### Costs that grow with size

These are the known limits of the current design. Each names what it is proportional to.

| Cost | Grows with | Effect at large size | Way out |
|---|---|---|---|
| `ResumeRun` and the cancel drain work through the run's backlogs in background batches | Queues one run has work on | A resumed run spread over millions of hosts takes a while to be fully ready again | Accepted: the state change itself is one step |
| `GetRun` stats and the `max_tasks` count sum the run's backlogs | Queues one run has work on | Stats reads become scans | Accepted. Rollups per run and bucket if it ever hurts |
| Run completion on a sharded store reads the run's backlogs on every shard it touched | Shards one run has work on | A completion check fans out | Per-shard "run active here" markers |
| Dispatch and finish lock every backlog with pending tasks on the queue | Runs with work on one queue | Thousands of concurrent runs on one host slow that host's dispatch | Pick from an index of the lowest start tags, lock only the winner |
| A changed policy set resets the readiness of every active backlog | Active backlogs | A deploy that changes policies causes a write burst | Reset lazily, on the next check of each backlog |
| The core caches resolved policies per run and queue | Open runs × their queues | Replica memory | Bounded cache; resolution is pure and can be redone |
| Dedup keeps one index entry per task of every open run | Tasks in open runs | Memory per partition | More partitions; shorter runs |

None of these changes the model or the API. They are properties of how the first
implementation carries out an operation, and each fix stays inside the store or the core.

### What the model deliberately does not do

- No memory of URLs across runs: revisit policy belongs to the scheduler, which decides
  what goes into the next run.
- No limit across queues: nothing caps a run's or the fleet's total rate except worker
  slots and the run's budgets.

## Alternatives considered

- **Work-conserving pick within the session's pool only.** Simpler, but two runs in
  different pools on one site then split it by whichever dispatcher wins each token, not
  by weight. Replaced by first refusal.
- **One dispatch loop per session.** Every idle session wakes at the same `NextReadyAt` and
  all but one find nothing: the candidate query runs once per session per dispatch.
  Replaced by one loop per pool on each replica.
- **PostgreSQL tables used directly by the Frontier code** (the first draft). Simple, but
  the business rules, transactions and scaling were one piece, and review found deadlocks
  and hot rows in it. Replaced by the `Store` interface.
- **A separate `Limiter` interface** next to the store. Admission and claim then sit in two
  systems, so every dispatch needs acquire, claim and release-on-failure across them, and a
  failure in between leaks or double-counts capacity. Politeness is now part of the store's
  dispatch; an implementation that keeps politeness in Redis (the hybrid above) does that
  two-step internally, where it can be made safe.
- **Fine-grained repositories** (`Tasks`, `Queues`, `Runs`) with a transaction handle passed
  around. Leaks transaction boundaries and lock order into the core and ties it to stores
  with multi-statement transactions.
- **Run-first fairness** (a share of all worker slots per run). Lets a run on one host take
  that host from every other run (the review's finding), and needs a run-wide clock written
  on every dispatch. Rejected; see Consequences for what is lost.
- **Temporal as the dispatcher.** Its worker contract and storage design map closely onto
  the Frontier, and the worker vocabulary, pools, slot tuning and `range_id` fencing are
  borrowed (ADR 0003). Its scheduling does not fit: it rate-limits per task queue, so
  politeness per host would need a task queue and a worker per host, which does not scale
  to thousands of hosts. It is also not cheaper per task: each activity dispatch and
  completion writes the workflow's history and mutable state.
- **Capability classes** (handler plus label-subset matching of `Hello.labels` against
  `requires`). A class created after a session connected was invisible to it until it
  reconnected. Named pools are simpler and have no such gap.
- **A global rate limit per pool** (Temporal's `TaskQueueActivitiesPerSecond`). Not needed:
  workers limit themselves with their credits. If it is ever wanted, enforce it per bucket
  so the locality rule holds. Out of scope.
- **Dynamic policies** (reload, `PutPolicy`, `UpdateRunPolicy`). Adds versioning, replica
  agreement and invalidation of stored readiness for a need not yet seen. Removed.
- **URLFrontier's reference server** (RocksDB, sharded). Politeness is scoped per crawl,
  limiter and lease state is in memory, and there are no runs.
- **Cassandra / ScyllaDB, DynamoDB.** Poor fit for queue access patterns (tombstones, no
  ordered pop across partitions, hot partitions).

## Consequences

- The Frontier core is storage-agnostic and testable with an in-memory store that passes
  `storetest`.
- Each store method carries a whole business transaction, so the interface is wide (about
  20 methods). That is the price of keeping atomicity inside the store.
- The scheduling rules exist once (`sched`), but every implementation must call them at the
  right point of its transaction. `storetest` is what keeps them aligned.
- **No fleet-wide fairness between runs.** Runs compete per queue, not for worker slots. If a
  pool has fewer slots than its queues could use (e.g. headless browsers), a run on many
  hosts can occupy most slots while a deadline-bound run on a few hosts waits. Where slots,
  not politeness, are the limit, add slots to that pool or give the deadline-bound runs
  their own pool.
- Fair shares are per backlog: a run spread over two pools on one queue gets two shares.
- Fairness across pools costs up to one refusal period (≤ 100 ms) per turn that a pool
  without free workers forfeits.
- A looser run override can saturate a queue for other runs while it runs. That is the
  intended meaning of an override.
- Policy changes need a deploy and are mixed during the rollout.
- Metrics are per policy, not per queue: one misbehaving site among many that share a
  policy is found with `ListQueues`, not with a series of its own.
- The list of sessions is per replica. Seeing every worker means asking every replica.
- Dedup is per run and queue (identical to per run with host-derived keys).
- The default dedup key ignores headers, and keeps the case of path and query. Spiders
  for sites where headers matter, or where case does not, must set `dedup_key`.
- A change of the default key applies from the replica that has it. A run that is open
  across such an upgrade can fetch once more a request it had added under the old key, and
  replicas of both versions disagree on the key while the rollout lasts.
- `max_tasks` can be overshot by up to one second of concurrent additions.
- Opaque `task_id` and `lease_id` encodings belong to the implementation and must not be
  parsed by clients.

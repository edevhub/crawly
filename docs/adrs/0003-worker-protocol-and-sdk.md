# ADR 0003: Worker protocol and SDK

- **Status:** Proposed
- **Date:** 2026-11-09
- **Related:** [ADR 0001: Frontier storage architecture](0001-frontier-storage-architecture.md),
  [`api/crawly/frontier/worker/v1/worker.proto`](../../api/crawly/frontier/worker/v1/worker.proto) (`WorkerService`)
- **Naming:** enum values are written here without their proto prefix (`RETRY` is
  `Outcome.KIND_RETRY`, `STALE` is `CompleteAck.STATUS_STALE`). The stream's envelope
  messages are `WorkRequest` (worker to Frontier) and `WorkResponse` (Frontier to worker).

## Context

Workers (spiders) are stateless executors. They hold a `Work` stream to a Frontier replica,
receive assignments, extend leases, add discovered tasks and report outcomes. The first
draft matched workers to tasks by capability classes: a handler plus a subset match of
`Hello.labels` against `TaskSpec.requires`. Review found that a class created after a
session connected stayed invisible to that session until it reconnected, and that the order
of a session's `DiscoverBatch` and `Complete` was not specified.

Temporal's worker contract is close to this one: workers poll a named task queue, reserve
slots before polling, heartbeat long activities and are fenced by a task token and attempt.
Its scheduling does not fit a crawler (ADR 0001, Alternatives), but its worker model does.
This ADR borrows it.

## Decision

### Pools and sessions

- A **pool** is a named set of workers that execute the same kind of task, e.g. `default`
  (plain HTTP) or `browser` (headless). It is Temporal's task queue.
- A task names its pool in `TaskSpec.pool` (empty: `default`). There is no capability or
  label matching. Pool names match `[a-z0-9_]{1,32}`; anything else is `INVALID`.
- A **session** serves exactly one pool. `Hello` carries `pool`, `worker_id` and
  `build_version`. A worker process that serves several pools opens one session per pool.
- A session for a pool that has no tasks yet is valid: it receives work as soon as the first
  task of that pool is added.
- A worker that receives a task whose handler it does not have **hands it back**: outcome
  `RELEASED`, code `no_handler`. A handler name is a contract between builds, and in a
  rolling deploy the workers of the old build share a pool with the build that added a
  handler. The Frontier returns the task to its backlog without counting an attempt, holds
  it for a short delay and gives it out again, to whichever session is next. It fails the
  task with code `no_handler` only when no worker could run it for a configured time (one
  hour by default). The time starts with the first worker that lacks the handler and ends
  with the first worker that runs the task, whatever the result, so a long deploy does not
  fail a task that is being served.
  - Dispatch does not select by handler. The store claims work by pool and queue, and a
    handler filter under that would leave a backlog whose head no session can run. The
    price of the simpler rule is that every round of a handed-back task takes a politeness
    slot of its queue; the delay bounds that.
  - The time is kept per replica, in memory, one entry per task that is going round. A
    restart starts it again. Replicas that restart more often than the configured time
    therefore never fail such a task: it keeps going round until a worker has its handler
    or its run ends. The Frontier logs the first hand-back of a task and its failure; the
    run's stats show the code only once the task failed.

### Credits

- One credit is one execution slot, as a Temporal slot is one outstanding poll. The Frontier
  never holds more unfinished assignments on a session than the credits granted.
- Credits are additive. A worker grants a credit when a slot frees up; it never grants credit
  it cannot serve. Flow control (how many credits, how fast) lives entirely in the worker,
  through the SDK's tuner (below).
- **One dispatch loop per pool on each replica** holds the free credits of that pool's
  sessions. It asks the store for work for the session that has waited longest since it
  was last given work, for as many tasks as that session has credits, and sends it the
  assignments. Work is therefore shared between the workers of a pool in turns, whatever
  their free credits are: a worker that holds a long task still gets its turn, and adding
  a worker spreads the load at once. A loop per session would wake every idle session at
  the same moment for one task.

### Message order

- **The Frontier handles one session's inbound messages in stream order.** Each session has
  one inbound loop; a `DiscoverBatch` is committed to the store before the next message of
  the session is read. So a spider can send `DiscoverBatch` and then `Complete` for the same
  task without waiting for `DiscoverAck`, and no discovered link is lost even if that
  `Complete` finishes the run.
- Acks (`DiscoverAck`, `CompleteAck`, `LeaseExtended`) are sent in the order the messages
  were handled. `Assignment`s are interleaved freely.
- At these rates (tens of messages per second per session) serial handling costs nothing;
  dispatch runs in the pool's loop, not the session's.
- `DiscoverAck.status` tells a rejected batch (`STALE`, `UNKNOWN`: the parent lease is not
  live) from an accepted one.
- When the store is unavailable for a message (after the store's own retries, or the
  replica's call budget is exhausted), the Frontier closes the stream with `UNAVAILABLE`.
  The SDK reconnects and resends what was not acknowledged, in order. Leases are unaffected.
- Messages are bounded in bytes, so that no task a spider builds can exceed what a gRPC
  message carries. A task is limited to 1 MiB as encoded (request, headers, body and
  context together); the Frontier answers a larger one with `INVALID`. The SDK starts a
  new `DiscoverBatch` by count and by size, and a handler that returns a task over the
  limit fails its own task for good with code `task_too_large`, with none of its tasks
  added. The same bounds apply to seeds.
- A report the Frontier refuses with `RESOURCE_EXHAUSTED` is final for that report. The
  bounds above keep the SDK's messages under gRPC's default limit, so this happens only
  against a Frontier with a lower one. Resent as it is, the report would end every new
  session; left to its lease, the handler would build the same report on every attempt.
  The SDK therefore replaces it with `FAILED`, code `report_too_large`, without discovered
  tasks. The batches the Frontier took before the refusal stay added.
- The bounds are enforced by each side from its own version on. A worker of an older SDK
  still sends batches bounded by count only, and does not look at the result of each
  discovered task, so a task over 1 MiB that a new Frontier answers with `INVALID` is
  dropped without a sign on that worker. Workers are upgraded with the Frontier, or
  before it.

### Leases and heartbeats

- `Lease.expire_time` is the heartbeat deadline (Temporal's heartbeat timeout); the lease
  deadline from `max_lease_duration` is the start-to-close timeout.
- The SDK extends every running task's lease on a timer at half of `Lease.duration`, on
  its own monotonic clock, while the handler runs. It never compares `expire_time` with the
  worker's wall clock. A handler never calls extend itself.
- On `LeaseRevoked`, on an extension that fails, or when the lease deadline passes, the SDK
  cancels the handler's context, as Temporal cancels an activity context. Results of a
  cancelled handler are not reported. The SDK tracks the deadline itself: each
  acknowledged extension moves it to the time the extension was sent plus `Lease.duration`.
  A worker that cannot reach any Frontier therefore stops a handler when its lease runs
  out, without being told.
- An assignment is committed in the store before it is written to the stream. When a
  session ends with assignments still unwritten, the Frontier **releases** them (`Finish`
  with result `Released`): the tasks are dispatchable again at once and the attempt is not
  counted. An assignment that was written but lost on the way is not detected: its lease
  expires and the reaper retries it, which counts as an attempt.
- Leases are not bound to a session. After a reconnect (to any replica) the SDK keeps
  extending its leases, **resends unacknowledged `DiscoverBatch`es first**, and only then
  sends the `Complete`s that follow them. Resent batches are absorbed by dedup.

### Outcomes

Unchanged: `SUCCESS`, `RETRY` (with optional `retry_after`), `FAILED`, any of them with
`block_queue_for`. The spider classifies; the Frontier applies `max_attempts` and backoff,
as Temporal's retry policy does, with `FAILED` in the place of a non-retryable error type.

`RELEASED` is the fourth outcome, and the SDK's own: the worker did not run the task to an
end and hands it back. The Frontier undoes the dispatch as it does for an assignment that
never reached a worker: the task is pending again, no attempt is recorded and the attempt
number is used again. The SDK sends it in two cases:

- the worker lacks the task's handler (code `no_handler`, see Pools);
- the worker shut down and its grace passed while the handler ran (code `shutdown`). The
  handler's context is cancelled, the task is handed back and the session stays open until
  that is acknowledged. A deploy during a slow page therefore never uses up `max_attempts`
  of a task that was not at fault.

A handler cannot return it: a spider that could hand back any task at will could keep a
task alive for ever. For the same reason the Frontier accepts `RELEASED` with these two
codes only, and treats any other code as `FAILED`.

An outcome the handler classified itself stands, also when it arrives at the moment of the
cut: only a handler that returns its cancellation is handed back.

A Frontier says in `Welcome.releases` that it understands `RELEASED`. To a Frontier that
does not, the SDK sends what it sent before: `FAILED` for a missing handler, and nothing
for a cut handler, whose lease then expires. Workers and Frontier can therefore be
upgraded in any order.

Codes the SDK reports itself: `no_handler` and `shutdown` (`RELEASED`), `panic` (a handler
panicked: `RETRY`) and `redirect` (`SUCCESS`: the target of the redirect was added as a
task).

### Redirects and dedup keys

Both are the spider's side of the contract; the Frontier cannot see either.

- **Redirects.** Politeness is enforced per queue at dispatch and dedup when a task is
  added, so a fetch that follows a redirect makes a request the Frontier neither paced nor
  deduplicated. On the same host that request comes on top of the configured rate; on
  another host it runs outside that host's limits altogether. By default the SDK's fetcher
  follows no redirect: it adds the target as a discovered task (the Frontier derives its
  queue and drops it when the page is known already) and completes the original with
  `SUCCESS`, code `redirect`. The costs of this default:
  - The target is one level deeper than its task, so it counts against `max_depth`.
  - A page that redirects to its own URL, as a cookie gate does, would be dropped as a
    duplicate of its own task. The fetcher reports it as `FAILED` with code
    `redirect_loop` instead, so that it shows in the stats.

  A spider can make the fetcher follow redirects on the same host, or all of them, for the
  sites that need it, and accepts the extra requests with it.

  A 307 or 308 repeats the request, so its task keeps method and body. The headers of a
  task are the spider's, and credentials sit in headers of its own naming as often as in
  `Authorization` or `Cookie`. The task for the target therefore keeps them only on the same
  host and without a step down from https to http; otherwise only the headers that
  describe the content go along. The spider can adjust the task before it is added, which
  is also where it sets its own dedup key.
- **Dedup keys.** The Frontier's default dedup key is a simple normalisation of the request
  (ADR 0001, Dedup). A spider sets `TaskSpec.dedup_key` whenever its site needs special
  rules: case-insensitive paths, tracking or session parameters, requests told apart by
  a header.

### Spider strategies and registration

Writing a spider is plugging one implementation into the worker. The SDK owns everything
else on this page.

- **`Handler`** is the strategy interface: `Handle(ctx, *Task) (*Result, error)`. The spider
  is given a task and does everything itself, including the fetch. `HandlerFunc` adapts a
  function.
- **`Parser`** is the simpler strategy: `Parse(ctx, *Task, *Response) (*Result, error)`. The
  adapter `HTTP(parser, options...)` turns it into a `Handler`: it fetches, classifies the
  response (2xx parsed, 429 retried with a queue block, 408 and 5xx retried, other statuses
  failed) and applies the redirect rule above. The classification is a default: a spider
  hands any status to a parser of its own, whose return value is then the outcome. That is
  how a 404 becomes a result ("the product is gone") and a 403 a block of the queue.
- **Registration:** `Worker.Register(name, handler, options...)`, where `name` is the task's
  `handler` field. Options are functional: `InPool(name)` (the worker opens one session per
  pool its handlers serve) and `WithTimeout(d)`. Functional options were chosen over an
  options struct so that new options do not change the call for existing spiders.
- **Outcomes are Go errors.** A nil error is `SUCCESS`; `Retry(code, err, After(d),
  BlockQueue(d))` and `Fail(code, err)` classify failures; any other error is `RETRY` with
  code `error`; a context deadline is `RETRY` with code `timeout`.
- **Discovered tasks** are returned in `Result.Tasks`. A task that names no handler gets the
  handler that discovered it, and one that names no pool gets the pool its handler is
  registered in. Discovered tasks are reported only for a task that succeeded.

### Package layout

The Go module exports only what is under `sdk/`. The Frontier core, the stores, the
scheduling arithmetic and the generated protobuf code are under `internal/` and cannot be
imported from outside the module.

| Package | For | Main names |
|---|---|---|
| `sdk/worker` | Spiders | `worker.New`, `Handler`, `Parser`, `HTTP`, `Retry`, `Fail` |
| `sdk` | Schedulers and operators | `sdk.NewClient`, `Client.Seed` |
| `sdk/run` | Runs | `run.Client`, `run.Run`, `run.Limits`, `run.Stats`, requests, states, events |
| `sdk/task` | Tasks | `task.Spec`, `task.Request`, `task.AddResult` |
| `sdk/policy` | Policies | `policy.Client`, `policy.Run`, `policy.Limits`, `policy.Behaviour` |
| `sdk/queue` | Queues | `queue.Client`, `queue.Queue` |

One package per API package, so a name reads without a prefix that repeats its package
(`run.Limits`, not `sdk.RunLimits`). What a client must name is exported as **type
aliases** of the internal types, not as copies: there is one definition of each, and
exporting something later is one alias line.

### Worker SDK options

Named after Temporal's worker options so their behaviour is familiar:

| Option | Meaning | Temporal |
|---|---|---|
| `MaxConcurrentTasks` | Fixed number of credits | `MaxConcurrentActivityExecutionSize` |
| `MaxTasksPerSecond` | Throttles credit grants on the worker: one credit per interval, over all its pools | `MaxWorkerActivitiesPerSecond` |
| `ResourceBasedTuner{TargetCPU, TargetMemory, MinSlots, MaxSlots}` | Grants credits from measured CPU and memory, between a floor and a ceiling | Resource-based slot supplier |
| `CompositeTuner` (later) | Different tuners per handler | `WorkerTuner` composite |

These limit the worker, not the target sites: politeness stays in the Frontier.

A credit is one task the Frontier may send, so the rate limit needs nothing from the
Frontier: the worker grants a credit for a free slot only when the interval since its last
grant has passed, and one at a time, so unused time is not saved up into a burst. Credits
do not carry over to a new session; after a reconnect the free slots are granted again at
the same pace.

### Drain and shutdown

`Goodbye` stops new assignments to the session and lets running tasks finish. A credit
that crosses a `Goodbye` grants nothing.

`Drain` asks the worker to reconnect elsewhere. A replica that shuts down refuses new
sessions with `UNAVAILABLE`, sends `Drain` to every session and ends its watch streams,
then stops serving: gracefully up to a deadline, by force after it. Running tasks are not
affected, because leases are not bound to a session: the worker reports them to the
replica it reconnects to.

A session whose outbound queue stays full for five seconds is closed by the Frontier. Its
worker is gone or stuck, and one such session must not stall the pool's dispatch loop.
Both sides send gRPC keepalive pings, so a peer that vanished without closing the
connection is noticed.

### Transport security

The SDK has no default for transport security. A worker and a client must be given
credentials, or the explicit insecure option (`worker.WithInsecure`, `sdk.WithInsecure`),
and fail at construction otherwise. A plaintext connection is then always a visible
choice in the caller's code.

## Temporal mapping

| Temporal | Crawly |
|---|---|
| Task queue | Pool |
| Slot / outstanding poll | Credit |
| Activity task token + attempt | `lease_id` + `attempt` (the ID is unique per dispatch) |
| Heartbeat timeout | `lease_duration` |
| Start-to-close timeout | `max_lease_duration` |
| Schedule-to-close timeout | `task_ttl`, measured from add, across retries, enforced while pending |
| Retry policy, non-retryable errors | `Behaviour` (`max_attempts`, backoff), `FAILED` |
| Activity heartbeat, context cancellation | SDK lease timer, cancellation on `LeaseRevoked` |
| Unregistered activity type | Handed back (`RELEASED`, code `no_handler`); `FAILED` when no worker has the handler for long enough |
| Fairness keys and weights | Per-queue fair queueing by run weight (ADR 0001) |
| Priority tiers above fairness | Priority **below** fairness, inside a run's backlog (ADR 0001) |
| `TaskQueueActivitiesPerSecond` | Not built (ADR 0001, Alternatives) |
| (none) | Queue: politeness per host, shared by all runs |

## Alternatives considered

- **Capability classes with label matching.** More flexible routing, but sessions had to be
  re-matched whenever a new class appeared, and every distinct `requires` set split the
  readiness index. Named pools cover the common split (plain HTTP vs headless browser).
- **Spider waits for `DiscoverAck` before `Complete`.** Works, but puts a protocol obligation
  on every spider that is easy to get wrong. Ordered handling in the Frontier is simpler.
- **Running workers as Temporal activities.** Gives the SDK for free, but politeness per host
  cannot be expressed in Temporal's per-task-queue rate limits (ADR 0001).

## Consequences

- Routing is by name only. A new kind of worker is a new pool name in the scheduler's or
  spider's task specs, and sessions for it.
- A misrouted task (wrong pool, missing handler) fails visibly with `no_handler` instead of
  waiting forever for a worker that matches, after the time a rolling deploy is given.
- Spiders never think about leases, heartbeats or ordering; the SDK does.
- The Frontier keeps one inbound goroutine per session and one dispatch loop per pool.

## Tests

Core rules, tested against the in-memory store:

- A `DiscoverBatch` immediately followed by the parent's `Complete` on one session: every
  accepted link is in the run, even when that `Complete` finishes the run.
- A `Complete` for a lease dispatched by another replica is decided with the right
  `max_attempts` and backoff (through `Work.Lease`).
- Fifty idle sessions in one pool and one task becoming ready: one store dispatch call, not
  fifty.
- A store that returns `ErrUnavailable` closes the stream; after the reconnect the SDK's
  resends leave the run in the same state as an undisturbed session.

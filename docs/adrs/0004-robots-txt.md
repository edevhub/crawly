# ADR 0004: robots.txt: gate, parser and policy

- **Status:** Draft, for review. Revision 4, after two rounds of reliability review and
  one design review. The section "Points to confirm" lists the choices that are still
  open; each is written into the text below as proposed.
- **Date:** 2026-11-11
- **Related:** [ADR 0001: Frontier storage architecture](0001-frontier-storage-architecture.md),
  [ADR 0002: PostgreSQL store](0002-postgresql-store.md),
  [ADR 0003: Worker protocol and SDK](0003-worker-protocol-and-sdk.md),
  [RFC 9309: Robots Exclusion Protocol](https://www.rfc-editor.org/rfc/rfc9309)
- **Naming:** as in ADR 0001. An **origin** is a scheme, a host and a port
  (`https://shop.example:443`), the scope of one robots.txt file. A **page task** is any
  task that is not a robots task.

## Context

The Frontier decides which request goes out and when, and the workers make the requests.
Nothing reads robots.txt today: a page a site disallows is crawled, and a `Crawl-delay`
is ignored. A spider can check robots.txt itself, but then every spider implements it,
every worker process fetches the file again, and no worker can change the pace of a queue
that all runs share.

The rules of a site are a property of the site, as its politeness limits are. They belong
where the limits are kept and enforced: in the Frontier.

Four constraints shape the design:

- **The Frontier makes no requests to crawled sites.** It has no egress to them, no proxy
  settings and no `User-Agent`. The file has to be fetched by the workers that fetch the
  pages.
- **A queue is not an origin.** A queue key is whatever the run's key rule makes it: a
  host, a registered domain or an explicit key. One queue can cover several origins, and
  one origin can be reached through several queues.
- **The scheduler may know better.** Some crawls are of the operator's own sites, or run
  under an agreement with the site, or get the rules from another system. The scheduler
  needs a way to say so, and the operator of the Frontier needs the last word.
- **A gate must not be able to stop a run for good.** Whatever happens to the request for
  robots.txt, a task that waits for it has to end up dispatched or ended. It may wait
  only for what makes every task wait: a paused run or a blocked queue.

## Decision

### Overview

- The Frontier keeps what was fetched from `/robots.txt` of every origin it crawls, and
  judges every page task against it.
- A run's page tasks for an origin are **held** until the rules of the origin are known.
  That is the gate.
- When rules are needed and the Frontier does not have them, it adds a **robots task**
  to a run that needs them: paced by the queue like any request, with a reserved handler
  that every SDK worker has built in. Its result is recorded on the origin.
- Held tasks are judged in batches when the rules arrive. Allowed tasks join their
  backlog and disallowed ones end `SKIPPED`.
- The design is complete here and is built in two steps, with a benchmark of the claim
  path between them (see "Delivery").
- A background step, not the arrival of new tasks, keeps the gate moving. A held task
  waits for what makes any task wait (a paused run, a blocked queue), and ends after the
  policy's `hold_grace` when robots.txt cannot be fetched.
- A run can bring the rules itself or opt out. The queue's policy can forbid that, and
  an operator can pin the rules of an origin.

```
AddTasks ──► fresh rules for the origin? ──yes──► judge ──► added | ROBOTS_DISALLOWED
                    │ no
                    ▼
        hold the task; arm a robots task for the origin if none is under way
                    │
   worker: GET <origin>/robots.txt  ──►  Complete{RobotsResult}
                    │
        Frontier: record the result on the origin
                    │
   background: judge held tasks in batches ──► backlog | SKIPPED
               move the robots task when it is not dispatched in time,
               end held tasks when the fetch keeps failing
                    │
   dispatch: compare the rules' version, judge again ──► assign | SKIPPED | hold
```

### The record of an origin

The store keeps one record per origin, shared by every run and every queue.

| Field | Meaning |
|---|---|
| `result` | What the last fetch saw: `FILE` (2xx), `NONE` (4xx: there is no file), `FAILED` (no usable answer) |
| `body` | `FILE` only: the content, cut at the parse limit |
| `status`, `final_url`, `hops` | What the fetch saw, for the operator |
| `fetch_time` | When the robots task that brought the result was dispatched, on the store's clock |
| `rules_digest` | A digest of the rules in force: the groups of the file or the pin, with their `Allow`, `Disallow` and `Crawl-delay` lines in a normal form. `NONE` has the digest of an empty file |
| `rules_version` | Increases when `rules_digest` changes, when rules become known or unknown, and with a hold an operator asked for |
| `task`, `arm_time` | The robots task of the origin that has not ended, if there is one, with its run, queue and pool, and when it was armed |
| `queues` | The queues the origin's tasks were added to, the last few |
| `last_file` | `FAILED` only: the last `FILE` or `NONE` result before it, with its `fetch_time` |
| `pin`, `pin_expire_time` | Rules set by an operator, and until when |

- The record holds facts only. What a result means (how long it is fresh, what a 403
  means, which group applies) is decided when a task is judged, with the policy of the
  task's queue. Two queues that reach one origin can therefore read one record
  differently, and neither changes it for the other.
- A result replaces the stored one only if its `fetch_time` is newer. Results are
  ordered by when their task was dispatched, never by when they arrived. The time is the
  store's: a worker's clock decides neither which result wins nor how long it is fresh
  (ADR 0003). The worker's own time of the fetch is kept for display.
- The version follows the rules and not the bytes. Comments, `Sitemap` lines, lines
  that are not understood and the order of lines inside a group are not part of the
  digest, so a file that carries a timestamp or rotating sitemap lines keeps its
  version from fetch to fetch. A `FILE` that is followed by `FAILED` keeps it too: the
  rules in force are those of `last_file`. The digest covers every group and not only
  the one a queue's product token matches, since the token belongs to the queue. A
  change in another crawler's group therefore counts as a change, and that is accepted.
- A pin is never replaced by a fetch. It ends at `pin_expire_time` or when an operator
  removes it with `UnpinRobots`. While it lasts nothing is fetched. When it ends, the
  last fetched result is in force again and is read as stale, so it is fetched anew.
- Each replica parses `body` and keeps the parsed form in memory. A fix in the parser
  applies to every stored file without a new fetch.
- A record is removed when its `fetch_time` is older than the longest lifetime in the
  policy set, it has no pin and no robots task, and no task is held on it.
- Only `http` and `https` URLs have an origin. A task with another scheme is not gated.
- The origin of a task is stored with the task. Only held tasks are indexed by it: the
  index covers the held state alone, so it costs nothing on the tasks that are pending
  or in flight. ADR 0002 names index maintenance on tasks as the first bottleneck, and
  an index over every task of an origin would add to it.

Derived from the record and the queue's policy, the rules of an origin on a queue are:

| Rules | When | A page task is |
|---|---|---|
| **pinned** | a pin that has not ended | judged by the pin |
| **fresh** | `FILE` or `NONE`, and `fetch_time + lifetime` has not passed | judged |
| **stale** | `FILE` or `NONE`, and `fetch_time + lifetime` has passed | judged by the old rules; a robots task is armed |
| **kept** | `FAILED` with a `last_file` | judged by `last_file` (RFC 9309 allows a cached file while the file is unreachable); a robots task is armed at each `retry` |
| **unknown** | no record, `FAILED` without `last_file`, or an operator asked for a hold | held |

Rules that were known once do not become unknown because a refresh is late. A refresh
waits behind the same things as any task (a blocked queue, a paused run, other runs'
turns), and a backlog that was admitted under a file is not taken back for that. RFC
9309 says a cached file should not be used for longer than 24 hours; here it is used
until the refresh that was armed at that point has run, and that is a limitation.

### Judging

A page task is judged at three points. All three ask the same question of the same
matcher.

1. **When it is added**, if the rules are pinned, fresh, stale or kept. An allowed task joins
   its backlog. A disallowed one is refused with `AddResult.STATUS_ROBOTS_DISALLOWED`
   and the code that says why (see "Codes"). It never becomes a task, takes nothing from
   the run's budget and leaves no dedup record, so the same URL can be added once the
   rules change, as long as the task is still held or pending: a skipped task gives
   its dedup record back (see "Dedup, budget and counts").
2. **In the background, when the rules change.** The held tasks of the origin are
   judged in bounded batches: the store hands the core a batch, the core judges, and the
   store moves each task to its backlog or ends it `SKIPPED`, in one transaction per
   batch. This step needs no worker and no credit.

   The pending tasks of the origin are judged the same way when `rules_version`
   changes, which is when the rules changed and not when the file was fetched again.
   Two sets of rules are not ordered as stricter or looser, so every change counts.
   The store finds them without an index of their own: it walks the backlogs of the
   queues the record names, in bounded batches, and takes the tasks of the origin. On
   a queue that covers several origins the walk reads the other origins' tasks too.
   When the origin was reached through more queues than the record keeps, the tasks on
   the others are left to point 3. When an operator asks for a hold, the pending tasks
   are moved to held by the same walk. A backlog that became disallowed therefore ends
   without being dispatched.
3. **When it is dispatched**, as the last check. Every assignment comes out of the claim
   with the `rules_version` of its origin's record. The core compares it with its own copy and
   reloads the copy when they differ, so the check costs no store access otherwise. A
   task that is disallowed ends `SKIPPED`. A task whose rules became unknown goes back
   to held. In both cases the politeness slot the claim took is **not** given back: a
   refund that lands after another claim would let two requests out inside one interval,
   and a lost slot only makes the queue slower. Point 2 keeps this case rare. Until
   the second step of the delivery, point 3 is the only place where a change of rules
   reaches a pending task, one lost slot per task.

The decision at add is made against the record the store holds, not against the
replica's copy. The core sends, per task, its judgement and the `rules_version` it
judged by. The store takes the judgements whose version is the record's and returns the
other tasks, and the core judges those again with the record it gets back. A task can
therefore not be held on rules that arrived a moment before, and one origin that changed
does not send a whole batch round again.

### The gate

- A task that is added while the rules are unknown is **held**: a task state of its
  own. A held task is not in a backlog, takes no politeness slot, and is counted in
  `RunStats.held`. In the same transaction the store arms a robots task for the origin
  (see below).
- A run that brought rules for the origin, or does not obey robots.txt there, has
  nothing held: its tasks are judged against its own rules, or not at all.
- The gate opens through point 2 above. It is idempotent: the background step releases
  every held task whose origin has rules, whenever it finds one. A crash between writing
  the record and judging the held tasks therefore loses nothing.

**Liveness.** One background step, run by every replica like TTL expiry (ADR 0001,
`Maintain`), looks at the origins that have held tasks, in bounded rounds. A **holder**
is a run, queue and pool that has tasks held on the origin.

1. The origin has rules: judge and release the held tasks.
2. The origin has no rules and no robots task, and the last `FAILED` result is older than
   `retry`: arm a robots task with a holder whose run is not paused.
3. The origin's robots task was not dispatched within `arm_wait` of `arm_time`, or its
   run is paused: drop it and arm one with the next holder. Holders are taken in turn.
   A holder whose pool has a worker with `crawly.robots`, whose queue is not blocked
   and whose run is not paused comes before one that lacks any of the three. When
   there is no other holder, the task stays where it is. The fetch of an origin
   therefore does not depend on one run's weight or limits, on one pool's workers, or
   on one queue's block, as long as another holder is free of them.
4. The fetch keeps failing: when every result since a task was held is `FAILED` and the
   task has been held for `hold_grace`, it ends `SKIPPED` with `robots_unreachable`.
5. Nobody can fetch: when the origin has had no rules for `hold_grace` and no worker of
   a task's pool has reported the `crawly.robots` handler in that time, the task ends
   `SKIPPED` with `robots_no_handler`. This is judged per held task and its pool, for
   every pool that holds tasks on the origin, whichever pool the robots task is in.
   "No worker has the handler" is read from the store's registry of handlers per pool,
   which every replica keeps up to date for its sessions and which fails page tasks
   whose handler nobody has in the same way (ADR 0003). It is never read from one
   replica's sessions. A pool without the handler gets the rules from another pool's
   fetch when there is one (step 3), and its tasks are released by step 1.

What is left makes a held task wait and not end: a paused run and a blocked queue. Those
make the page tasks of the run and the queue wait too, and a pause or a block is a
request to wait. The task's TTL and the run's deadline apply to a held task as to any
other. A held task therefore ends, or its run is paused, or its queue is blocked, and a
sealed run with held tasks completes under the same conditions as one without. Nothing
in this depends on new tasks being added.

The transaction that ends the last held task of a sealed run completes the run. The
rule for completion reads: a run completes when it is sealed and has no pending,
delayed, held or in-flight page tasks.

### The robots task

**What it is.**

- A task of one run, on the queue and in the pool of the page task that needed the
  rules. It is fetched through the same workers and the same egress as that run's pages,
  and it waits for the queue's pace and for a queue block like any other request.
- It is at the head of its run's backlog on the queue: it is dispatched before any page
  task of the run there.
- It has a fingerprint of its own, outside the run's dedup: it leaves no dedup record
  and is no duplicate of anything. A page task for `/robots.txt` that a spider adds is
  an ordinary page task with its own dedup, and the matcher always allows it.
- It is never held and never judged.
- It takes nothing from `RunLimits.max_tasks`, has depth 0 and no parent, and is counted
  in `RunStats.robots` only: not in the run's task counters, not in `by_code`, and not
  towards `max_failed` and `max_consecutive_failures`. `ListTasks` shows it.
- At most one robots task per origin exists that has not ended. The record names it
  (`task`), and no run arms another while it is there: a second run that needs the same
  rules waits for the first run's result. The record is cleared in the transaction that
  ends the task, whatever ends it, the end of its run included. A task that is not
  dispatched within `arm_wait` is dropped and armed again with the next holder (see
  "Liveness"), so there is still one task per origin, and it moves.
- A robots task does not keep a run alive. A run completes when it has no page tasks
  left, and a robots task it still has is dropped with the run's other tasks. A result
  that arrives for a dropped task is discarded, as for any revoked lease.

**The handler.**

- Its name is `crawly.robots`. The worker SDK registers it in every worker, and a spider
  cannot register a handler under the `crawly.` prefix.
- Dispatch selects by handler (ADR 0003). The robots task is given only to a session
  that names `crawly.robots` in `Hello.handlers`. A session that names no handlers, as
  an older SDK does, is offered every other task of its pool but never this one.
- It requests `<origin>/robots.txt` and follows up to five redirects, to other hosts as
  well, as RFC 9309 asks. These hops go through the worker's HTTP client with the
  restrictions that hold for page fetches, and their hosts are reported. They are the
  one place where a worker follows a redirect to another host inside one task, and they
  are not paced by the other host's queue.
- It reads at most the parse limit of the decoded body and then stops reading, within
  the task's fetch timeout.
- It always reports `SUCCESS` with a `RobotsResult`, whatever the site answered, a
  failure before an answer included: the final status, the final URL, the hops, the
  body, `Retry-After` if the site sent one, the worker's time of the fetch (for display),
  and whether the fetch failed before an answer. The handler does not interpret any of it.
- It sends the queue's product token as its `User-Agent`, see "What the worker is told".
- A worker that is not built on the SDK implements the same contract.

**How it can end, and what follows.**

| End | Record | What follows |
|---|---|---|
| `RobotsResult` with 2xx | `FILE` | Held tasks are judged |
| `RobotsResult` with 4xx other than 429 | `NONE` | Held tasks are judged: everything is allowed, or see `forbidden` |
| `RobotsResult` with more than five redirects | `NONE` | The same |
| `RobotsResult` with 5xx, 429, timeout or connection failure | `FAILED` | A `Retry-After` blocks the queue, capped like any block. The next task is armed after `retry` |
| The attempt is lost (worker crash, lease expiry, panic) | none | The task is retried like any task, up to `max_attempts`. After the last attempt: `FAILED` |
| The task expires by TTL, or its run ends or is paused | none | The record is cleared, and the background step arms a task in a run that still holds tasks |
| It is not dispatched within `arm_wait`: no worker of the pool has the handler, the queue is blocked, or the run's turn does not come | none | The task is dropped and armed with the next holder. Held tasks whose pool has no handler end with `robots_no_handler` after `hold_grace` if no other pool brought the rules |
| A replica of an older build takes the result | none | The older build drops `RobotsResult` and ends the task. The background step arms a new one |

Treating 429 as `FAILED` is stricter than RFC 9309, which counts every 4xx as "the file
is unavailable". A 429 says the client is too fast, and reading it as "everything is
allowed" would answer a request to slow down with more requests.

### Codes

Every refusal and skip that robots.txt causes has a code:

| Code | Meaning |
|---|---|
| `robots_disallowed` | A rule of the file disallows the URL |
| `robots_forbidden` | robots.txt answered 401 or 403, and the queue's policy reads that as a refusal |
| `robots_crawl_delay` | The origin asks for a delay above the policy's ceiling |
| `robots_unreachable` | Every fetch of robots.txt failed for `hold_grace` |
| `robots_no_handler` | No worker of the pool had the `crawly.robots` handler for `hold_grace` |
| `robots_token_mismatch` | A worker was about to send a `User-Agent` that does not name the queue's product token. It is the code of a failed attempt and not of a skip |

- A refusal at add carries the code in `AddResult.code`, and a skipped task carries it
  as the code of its state.
- Both are counted per code in `RunStats.robots.by_code`, in the transaction that
  refuses or skips. The count does not depend on which of the three points judged the
  task, so it does not depend on whether another run fetched the rules first.

### Dedup, budget and counts

- **Dedup.** A task that ends `SKIPPED` with one of the codes above gives its dedup
  record back in the same transaction, as a refusal at add never takes one. The URL
  can be added again in the same run. Without this, an origin whose robots.txt is
  unreachable for a little longer than `hold_grace` would lose every page found in
  that time for the rest of the run, although the site recovered. The Frontier does
  not add the task again by itself: `ListTasks` filters by state and code, and the
  scheduler adds what it wants to try again.
- **Budget.** A held task is a task and counts for `RunLimits.max_tasks` from the
  moment it is added. A skipped task keeps counting: the budget is a count of tasks
  the run created, as it is for failed tasks. A refusal at add creates no task and
  counts for nothing.
- **Counts are events.** `RunStats.robots.by_code` counts refusals and skips, not
  distinct URLs. A disallowed link that is found on a thousand pages is refused a
  thousand times and counted a thousand times, because a refusal leaves nothing behind
  to recognise the next one by. `RunStats.robots` keeps refusals and skips apart, and
  `skipped` counts tasks. `abort_on_codes` asks whether a code occurred, so it does not
  depend on the difference.

### Ending a run on robots.txt

The Frontier does not end a run because of robots.txt by itself. A run that must end
when one of the codes above occurs lists the code in `RunLimits.abort_on_codes`.

- `abort_on_codes` is evaluated on the counts above as well as on recorded attempts, in
  the same transaction, on all three paths. A run that lists `robots_forbidden` ends
  `ABORTED` whether its first task was refused at add, skipped in the background or
  skipped at dispatch.
- A refused or skipped task is no failure: it does not count for `max_failed` and
  `max_consecutive_failures`. A page the site does not want fetched is not an error of
  the crawl.

### Crawl-delay

`Crawl-delay` is not part of RFC 9309, but sites use it and crawlers are expected to
honour it.

- The delay is that of the group that matches the queue's product token.
- **A delay is never undercut.** An origin whose delay cannot be applied is treated as
  disallowing everything, with the code `robots_crawl_delay`. It loses its own tasks
  and nothing else.
- **A queue of one host.** When the queue's key is the host of the origin, the delay is
  a floor under the queue's interval, up to `max_crawl_delay`. A delay above the
  ceiling cannot be applied.
- **A queue of several origins.** When the key is anything else, such as a registered
  domain or an explicit key, the floor would slow every other origin of the queue. One
  subdomain with a delay of a minute would slow its siblings sixty-fold on a queue
  that allows one request per second. The ceiling for these queues is therefore a
  setting of its own, `max_shared_crawl_delay`, and its default is zero: a delay that
  is longer than the queue's own interval cannot be applied, and a delay that the
  queue's interval satisfies already needs no floor. An operator who wants delayed
  subdomains crawled keys the queue by host or raises the ceiling, and accepts what it
  does to the queue. Whether a queue is of one host is read from the task: the core
  compares the queue's key with the host of the task's origin.
- **Where it lives.** The queue's limiter row keeps the delays of the origins whose tasks
  were dispatched on the queue: origin, delay and the time the origin's rules stop being
  fresh. A queue of one host has one entry per scheme and port. A shared queue with a
  raised ceiling keeps the longest few; when more origins have delays than the row
  keeps, a delay that was dropped applies again with the next dispatch of that origin,
  and until then the floor can be below it. That is a limitation of raising the
  ceiling. The floor of the queue is the longest delay that has not run out. An
  origin's entry ends with the freshness of its rules, so an origin that is no longer
  crawled stops slowing the queue after one lifetime. The row is written in the
  dispatch transaction, which holds it already, and never when tasks are added.
- **Where it applies.** In the store's limiter step. The resolved limits stay static and
  cacheable (ADR 0001); the floor is state of the queue, like a block. While the floor
  is above the policy's interval, the queue's burst and concurrency are 1: a site that
  asks for a pause between requests does not get them in parallel.
- **With adaptive pacing.** The interval in force is the longest of three: the interval
  the limits resolve to, the floor, and the interval adaptive pacing has slowed the
  queue to (ADR 0001, "Adaptive pacing"). Adaptive pacing keeps working on the resolved
  interval and does not know the floor. Its `max_interval` caps the strain interval
  only, and `max_crawl_delay` caps the floor only. Recovery steps the strain interval
  back towards the resolved interval; while it is below the floor, the floor decides.
  Strain under a floor still raises the strain interval from the resolved interval, so
  a queue with a long floor needs several steps of strain before strain slows it
  further. Clearing the adaptive state does not clear the floor, and the other way
  round.
- It holds for every run on the queue, a run with limits of its own included. Run limits
  replace the queue's limits for that run, but the delay is the site's demand.
- A value that is not a finite, non-negative number of seconds is ignored.
- `LimiterState` shows the floor in force and the origin it came from.

### What a run can say

`CreateRun` takes a `Robots` setting. It is immutable, like the run's policy.

```proto
message Robots {
  enum Mode {
    MODE_UNSPECIFIED = 0;  // OBEY
    MODE_OBEY = 1;         // fetch, hold, judge
    MODE_IGNORE = 2;       // no fetch, no hold, no judgement
  }
  // For origins without an entry in `files`.
  Mode mode = 1;
  // Origin to the content of its robots.txt, as the scheduler has it. For these origins
  // the run is judged by this content and nothing is fetched or held for it. An empty
  // content allows everything.
  map<string, string> files = 2;
}
```

- The rules a run brings are robots.txt text. There is one parser and one matcher, and
  a scheduler that fetched the file itself passes it on as it is.
- A key of `files` is an origin: scheme and host in lower case, an internationalised
  host in its ASCII form, the port left out when it is the scheme's default. `CreateRun`
  rejects a key that is not an `http` or `https` origin, an entry above the parse limit
  and a setting above the size limit of a run, so a mistyped origin does not fall back
  to fetched rules without anyone noticing.
- A run's rules are for that run only. They are never written to the origin's record, so
  one scheduler cannot change what another run is allowed to fetch.
- `Crawl-delay` in a run's file is ignored. A run that wants a slower pace on a queue
  sets its own limits there, as it does today.
- `MODE_IGNORE` with no files is the opt-out.
- A run without a stored setting is read as `MODE_IGNORE`. That is every run created
  before the upgrade, and every run created through a replica of an older build while
  the upgrade lasts: the older build does not write the setting, whatever the scheduler
  sent. Such a run finishes as it started. A run created through a replica of this
  build always has a stored setting, and "unspecified" is stored as `MODE_OBEY`.
- A pin overrides both: while an operator has pinned an origin, every run is judged by
  the pin there, a run with `MODE_IGNORE` or its own file included.
- The test harness (`sdk/crawlytest`) runs with `robots: {obey: false}` in its default
  policy, so a test's fixtures see no request for `/robots.txt` that the test did not
  ask for. A test of the gate turns it on and serves the file.

### What the policy says

Global policies get a `robots` block. It belongs to the queue, like `max_queue_block`:
it is read from the global policies only, and a run that sets it is rejected.

| Field | Default | Meaning |
|---|---|---|
| `obey` | `true` | `false`: nothing is fetched, held or judged on this queue, for any run |
| `enforce` | `false` | `true`: robots.txt is obeyed on this queue whatever the run says. A run's `MODE_IGNORE` and its `files` do not apply here |
| `product_token` | none | The token matched against `User-agent` lines. Without it only the `*` groups apply. The workers are told the token and send it (see "What the worker is told") |
| `lifetime` | 24h | How long a result is fresh. RFC 9309 says a cached file should not be used for longer than 24 hours |
| `forbidden` | `ALLOW` | What a 401 or 403 on robots.txt means: `ALLOW` (RFC 9309) or `DISALLOW` |
| `max_crawl_delay` | 1m | The ceiling for `Crawl-delay` on a queue whose key is the origin's host |
| `max_shared_crawl_delay` | 0 | The ceiling on any other queue. Zero: a delay above the queue's own interval is not applied, and the origin is disallowed |
| `retry` | 5m | The interval between robots tasks for an origin whose fetch failed |
| `arm_wait` | 1m | How long a robots task may wait to be dispatched before it is armed with another holder |
| `hold_grace` | 1h | How long a task is held while every fetch fails, or while no worker has the handler |

- The product token is one per queue. Two crawlers with different identities that share
  a Frontier are judged by the same group on a queue they share. That is a limitation.
- When `enforce` overrides a run's setting, `PolicyService` shows it in the resolved
  policy of the run and queue, and the first task the run adds to the queue is logged.
  The run is not rejected: at `CreateRun` the Frontier does not know which queues a run
  will reach.

### What the worker is told

The Frontier judges the tasks it dispatches. Two things happen in the worker and are
out of its sight: the `User-Agent` that is sent, and requests a task makes beyond its
own URL. The assignment of a task on an obeying queue therefore carries a `robots`
field with the queue's product token and the fact that the task was judged.

- **The token.** The SDK's HTTP adapter sends the token as the `User-Agent` when the
  handler set none. When the handler set one that does not name the token, the adapter
  makes no request and the attempt fails for good with `robots_token_mismatch`: a
  request under another identity would obey the wrong group without anyone noticing.
  The `crawly.robots` handler follows the same rule, so the file is fetched under the
  identity it is matched with. The failure is counted like any failed attempt, and a
  run can list the code in `abort_on_codes`. A queue without a `product_token` puts no
  demand on the `User-Agent`.
- **Redirects inside a task.** On a judged task the adapter follows no redirect inside
  the task, whatever `WithSameHostRedirects` and `WithCrossHostRedirects` say: the
  target becomes a discovered task, as it does by default, and is judged when it is
  added. A site that needs a redirect followed in place, such as one that sets a cookie
  and returns to the same URL, is crawled by a run that brings the rules or opts out,
  or on a queue that does not obey.
- **A handler's own requests.** A handler that makes requests of its own, beyond the
  URL of its task, makes them unjudged. The Frontier cannot see them. The contract of
  a handler on an obeying queue is to add such URLs as tasks; the token is available
  to it in the task. That is a gap, and it is stated here and in the SDK.
- **Trust.** The workers of any pool that crawls an origin can be given its robots
  task, and what they report becomes the rules of that origin for every run, the runs
  of other pools included. A worker is trusted today with the outcomes of its own
  tasks only, so this is a new crossing, while the Frontier has no roles yet. What
  does not depend on a worker's report is a pin and a run's own files. An operator who
  does not trust every pool with this pins the origins that matter or turns `obey` off
  on the queues the pool serves.

### Parser and matcher

The parser is a package of its own, `internal/robots`, without I/O: bytes in, a compiled
form out. The matcher answers one question: may this product token fetch this URL, and
with what delay.

Parsing follows RFC 9309:

- The input is UTF-8. A byte order mark is skipped. Lines end with CR, LF or CRLF. `#`
  starts a comment. A line is `key: value`, keys are case-insensitive, and a line that
  is not understood is ignored and does not end a group. Rules before the first
  `User-agent` line are ignored.
- The first 500 KiB of the decoded body are parsed. That is the limit the RFC requires
  a parser to accept, and it is the limit on `RobotsResult.body`. When the body was cut,
  the last line is dropped if it is incomplete: a rule cut short would match more than
  the site wrote.
- A group is one or more `User-agent` lines followed by rules. The rules are `Allow`,
  `Disallow` and `Crawl-delay`. `Sitemap` lines are kept with the origin's record and
  shown to the operator; the Frontier does not act on them.
- `Crawl-delay` is a number of seconds, fractions allowed.

Matching:

- **Group.** The product token is compared without case. All groups that name the token
  are merged. If none does, the `*` groups are merged. If there is none either,
  everything is allowed. `Crawl-delay` is taken from the merged group that was chosen,
  never from `*` when a group names the token.
- **Path.** The rule is compared with the path and query of the URL, case-sensitively.
  Both are normalised the same way before the comparison, as the RFC requires: octets
  outside ASCII are percent-encoded, a percent-escape of an unreserved character is
  decoded, a percent-escape of a reserved character stays an escape, and escapes are
  upper case. The delimiters of the path and the query (`/`, `?`, `=`, `&`) stay as
  they are written. `*` matches any sequence of
  characters and `$` anchors the end.
- **Precedence.** The rule with the longest pattern wins. Between an `Allow` and a
  `Disallow` of the same length, `Allow` wins. The RFC says this of equivalent rules;
  applying it to different patterns of the same length is a choice made here. A
  `Disallow` without a value is no rule.
- `/robots.txt` is always allowed.
- **Cost.** The compiled form indexes rules by their literal prefix and matches a
  pattern without backtracking. One URL costs at most the length of its path times the
  number of rules whose prefix matches, and that product is bounded by the parse limit.
- A replica keeps a bounded number of compiled files. A file that is not in memory is
  compiled outside the dispatch loop: the assignment that needs it waits for it, and the
  loop goes on with the other queues.

The parser is tested with the examples of RFC 9309, with a corpus of real files, and by
fuzzing: it must not fail, hang or allocate beyond a bound on any input of 500 KiB.

### Tests of the gate

The gate is behaviour of the store, so its rules are conformance tests in `storetest`
and run against every store:

- A held task is released when a result arrives, and when the result arrived between
  the judgement and the add.
- A crash between recording a result and judging the held tasks loses nothing: the next
  background round releases them.
- A robots task that is not dispatched within `arm_wait` moves to the next holder: from
  a pool without the handler to one with it, from a blocked queue to a free one, from
  a paused run, and from a run whose turn does not come.
- Held tasks end with `robots_unreachable` and `robots_no_handler` after `hold_grace`,
  per pool, and the transaction that ends the last one completes a sealed run.
- A skipped task gives its dedup record back, and the URL can be added again.
- At most one robots task per origin exists, under concurrent adds and concurrent
  background rounds.
- Results are ordered by dispatch time: a late result of an older task does not
  replace a newer one.
- A file that changes in bytes and not in rules keeps `rules_version`.
- Two replicas on one store: both run the background step at once; one records a
  result and the other judges at dispatch with its older copy; one holds while the
  other arms.

The handler, the token rule and the redirect rule are tested in the SDK against a
fixture site, and one end-to-end test runs a crawl through the gate.

### Operating it

**What the operator sees.**

- Metrics, labelled as the other Frontier metrics are (by policy, never by queue key or
  origin): robots fetches by result; held tasks and the age of the oldest; the time
  tasks spent held; refusals and skips by code; records by result; compiled files in
  memory, hits and misses; matcher time.
- Log lines: the first hold of a run on an origin; a result recorded, with status and
  hops; a robots task that ended without a result; held tasks ended after `hold_grace`,
  with the code and the count.
- `RunStats.held` is a counter of its own, so `delayed` keeps its meaning. `RunStats`
  lists the origins a run is held on, bounded like its blocked queues.
- `QueueService.ListRobots`: the records, filtered by result, by queue and by "has held
  tasks", with the held count and the runs holding.
- `QueueService.GetRobots(origin, queue_key)`: the record, how the queue's policy reads
  it (fresh, stale, kept, unknown), the group that matches the queue's product token,
  the delay, the sitemaps.
- `QueueService.CheckRobots(url, queue_key)`: what the matcher answers and which rule
  decided, for an operator who wants to know why a page is not crawled.

**What the operator can do.**

- `RefreshRobots(origin)`: the rules become stale now, so a robots task is armed and the
  old rules hold until its result is there. With `hold: true` they become unknown now:
  the pending tasks of the origin are moved to held in the background, and tasks are
  held until the new result is there (second step of the delivery).
- `PinRobots(origin, content, expire_time)`: the operator sets the rules of an origin
  for every run, whatever the run's setting. Nothing is fetched while the pin lasts. Pinning
  `Disallow: /` stops the crawl of an origin at once, and the background step ends its
  pending tasks. `UnpinRobots` removes the pin. Both are in the second step of the
  delivery.
- `BlockQueue` stays what it is: a stop for a queue and a time. A pin is for an origin
  and is about what may be fetched.

### API

- `AddResult.STATUS_ROBOTS_DISALLOWED` and `AddResult.code`.
- `TaskStatus.STATE_HELD` and `STATE_SKIPPED`.
- `RunStats.held`, `RunStats.skipped` (all skipped tasks) and `RunStats.robots`: files
  fetched, and refusals and skips by code. `skipped` is the total; `robots.by_code`
  breaks it down and adds the refusals, which never were tasks.
- `Run.robots` (`Robots`), set at `CreateRun`.
- `Policy.robots`, the block above.
- `Complete.robots` (`RobotsResult`), sent by the `crawly.robots` handler only. It is
  not kept with the attempt; the record keeps the body.
- `Assignment.robots`: the queue's product token, and that the task was judged.
- `QueueService`: `GetRobots`, `ListRobots`, `CheckRobots`, `RefreshRobots`,
  `PinRobots`, `UnpinRobots`.

### Rolling deploy and compatibility

**Frontier replicas of two builds on one store.** ADR 0002 requires a migration to stay
compatible with the build before it. For this design that means:

- `HELD` is a task state of its own. The claim of an older build selects pending tasks
  only, so an older replica never dispatches a held task.
- An older replica adds tasks without holding or judging them, and dispatches tasks
  without the last check. For the length of the rollout, tasks that go through an older
  replica are not judged. That is a limitation, and it holds for `enforce` too.
- An older replica that is given a robots task treats it as an ordinary task and drops
  the `RobotsResult`. The background step arms a new one. The cost is one more fetch per
  origin touched that way.
- Runs that exist at the upgrade, and runs created through an older replica during it,
  have no stored setting and are read as `MODE_IGNORE`.

The order that keeps older builds out of it is two steps: roll out the build with
`robots: {obey: false}` in the policies, then roll out the policies with `obey: true`.
The second step is a policy rollout, during which replicas run with two policy sets, as
with every policy change (ADR 0001). A replica with the old set does not hold or judge
the tasks that go through it, so the unjudged window is as long as that rollout and
covers those tasks. It does not touch what the other replicas held: a replica releases
the held tasks of a queue that does not obey only when every registered replica runs
its policy set, which the store knows from the set each replica registered with.

**Going back.** Rolling the policies back to `obey: false` is safe: once every replica
runs that set, the background step releases the held tasks and stops arming robots
tasks. Rolling the build back comes after that. An older build does not release held tasks, so
a rollback of the build while tasks are held leaves them held until the newer build
runs again.

**Workers.**

- A worker without the `crawly.robots` handler is never given the robots task. If no
  worker of a pool has it, the pool's page tasks for origins without rules end with
  `robots_no_handler` after `hold_grace`. Workers are upgraded before `obey` is turned
  on.
- A worker with the handler and a Frontier without this design: the handler is never
  called.

### Delivery

The design above is one design. It is built in two steps, because it adds two task
states, a table, a column on tasks and a background step to a store whose claim path
has no benchmark yet.

1. **First step.** The parser and matcher; the record; the hold, the robots task and
   its handler; the liveness step with its conformance tests; judging at add, on
   release and at dispatch; the codes, the dedup and budget rules; what the worker is
   told; `GetRobots`, `ListRobots`, `CheckRobots` and `RefreshRobots` without `hold`.
   In this step no `Crawl-delay` floor exists: an origin whose delay is longer than
   its queue's interval is disallowed with `robots_crawl_delay`, which is the rule for
   a delay that cannot be applied. A change of rules reaches pending tasks at dispatch.
2. **Second step**, after a benchmark of the claim path on the PostgreSQL store, with
   and without the first step and its origin column. The `Crawl-delay` floor in the
   limiter row; judging pending tasks in the background when the rules change;
   `RefreshRobots` with `hold`; `PinRobots` and `UnpinRobots`.

Nothing in the first step is changed by the second: the second adds to the record, the
limiter row and the background step. `max_crawl_delay` is accepted in the policy from
the first step and applies from the second.

## Scaling and limits

- **Records.** One row per origin, with up to 500 KiB of body for a `FILE` result or a
  pin and none otherwise. The store holds as many as there are origins crawled within one
  lifetime, the in-memory store in memory. A record is written when a robots task ends
  and when an operator acts, and read by replicas whose copy has an older version.
- **Held tasks.** State per task. Judging them costs one transaction per batch, and the
  batches of one origin follow each other; no transaction grows with the number of held
  tasks.
- **Judging.** One match against the compiled rules of one origin. No store access when
  the replica's copy has the `rules_version` the claim returned.
- **Fetches.** One robots task per origin at a time, whatever the number of runs, and
  one per lifetime while the origin is crawled. A task that waits is moved once per
  `arm_wait`, which costs two row writes per origin and move.
- **Judging pending tasks** happens when the rules change, not with every fetch. It
  walks the backlogs of the queues the record names, without an index of its own.
- **The background step** is bounded per round, like the other steps of `Maintain`, and
  several replicas can run it at once.
- Limitation: one record per origin. A site that answers robots.txt differently by
  client or by address is judged by the answer one run's workers got, for every run.
- Limitation: one product token per queue.
- Limitation: on a queue of several origins, an origin with a delay longer than the
  queue's interval is not crawled unless the operator raises `max_shared_crawl_delay`,
  and then it slows every origin of the queue, up to that ceiling.
- Limitation: the workers of one pool set the rules of an origin for every run.
- Limitation: requests a handler makes beyond its task's URL are not judged.
- Limitation: a task skipped because robots.txt was unreachable is not added again by
  the Frontier.
- Limitation: on a judged task, redirects are not followed in place.
- Limitation: a task claimed and then skipped or held again costs the queue one
  interval.
- Limitation: stale rules stay in force until the refresh has run, however long the
  refresh waits behind a block, a pause or other runs.
- Limitation: a held task waits as long as its run is paused or its queue is blocked.
- Limitation: the redirect hops of a robots.txt fetch are not paced by the queues of
  the hosts they reach.

## Alternatives considered

1. **The Frontier fetches robots.txt itself.** One fetch per origin and no robots task.
   Rejected: the Frontier would need egress to every crawled site and the proxies and
   identity of the workers.
2. **The worker checks before each request.** No change to the Frontier. Rejected: each
   worker process fetches and caches the file, a disallowed task is found only after it
   took a slot and reached a worker, the pace of the shared queue cannot be changed, and
   every worker that is not built on the SDK has to implement the parser.
3. **A robots task per run and origin, without a shared record.** The simplest gate.
   Rejected: many short runs on one site each fetch the file, two runs crawl one origin
   under different rules, and `Crawl-delay` would have no place to live between runs.
4. **A record per origin and pool, or per origin and product token.** It would follow a
   site that answers by client. Rejected: one more dimension on every lookup and more
   fetches, for a case that is rare; the limitation is stated instead.
5. **A system run and system workers for robots tasks.** Rejected: it needs a fleet
   with its own egress, which is alternative 1 with one more component, and a run that
   belongs to no scheduler.
6. **Judging only at dispatch**, with the gate opened in one store call and the slot of
   a skipped task given back. This was the first draft. Rejected: a refund that lands
   after another replica's claim breaks the interval, and a backlog that became
   disallowed would be ended one dispatch at a time, in the pool's dispatch loop, and
   only while workers have free slots.
7. **Interpreting the result when it is recorded** (storing "disallow all" for a 403).
   Rejected: `forbidden`, `lifetime` and the product token belong to a queue, and an
   origin can be reached through several queues.
8. **Structured rules in the run** instead of robots.txt text. Rejected: a second format
   with the same meaning, and a translation step for every scheduler that already has
   the file.
9. **A third-party parser.** Rejected: the matcher's answer, `Crawl-delay` and the
   limits above are part of the Frontier's contract with its operators, and the format
   is small enough to own.

## Consequences

- A run obeys robots.txt without any code in the spider.
- The first tasks of a run on a new origin wait for one more request to that origin.
- Every SDK worker can fetch robots.txt, whatever its spiders do.
- Tasks have two more states, `HELD` and `SKIPPED`, and schedulers that count tasks have
  two more counters.
- The store gets a table of origins, an origin on every task with an index over the
  held ones, and one more background step.
- A worker is told the product token of the queue and fails an attempt that would be
  sent under another identity.
- `abort_on_codes` is evaluated on refusals and skips as well as on attempts.
- An operator can see, refresh, check and pin the rules of an origin.

## Points to confirm

1. **Shared record.** The result is kept per origin and shared by runs, one robots task
   per origin is under way at a time, and the task belongs to a run that needs the
   rules. The first proposal was one robots task per run and origin with no shared
   record (alternative 3).
2. **Default.** `obey` is `true`: after the upgrade new runs obey robots.txt unless
   they or the policy say otherwise, and runs that exist at the upgrade do not.
3. **401 and 403.** The default follows RFC 9309 (allow). `forbidden: DISALLOW` is the
   stricter choice.
4. **429.** Read as a failed fetch and not as "no file", which is stricter than the RFC.
5. **`Crawl-delay` that cannot be applied.** The origin is treated as disallowed, and
   the run ends only if it lists `robots_crawl_delay`. The ceiling is one minute on a
   queue of one host and zero on a queue of several origins, so a subdomain cannot
   slow its siblings unless the operator allows it.
6. **`enforce`.** A queue's policy overrides a run's opt-out without rejecting the run.
7. **Product token.** One per queue, from the policy. A run cannot set its own.
8. **Counters.** The robots task is outside the run's task counters, budget and failure
   rules, in `RunStats.robots`.
9. **A run's own files.** robots.txt text keyed by origin, for that run only, with
   `Crawl-delay` ignored.
10. **The pin.** An operator can set the rules of an origin for every run, a run that
    opted out included.
11. **Late refresh.** Rules that were known stay in force until the refresh has run.
    The stricter choice, holding the backlog once the rules are a given time past their
    lifetime, takes back work that was admitted and was left out.
12. **`hold_grace`.** It ends held tasks only when every fetch fails or no worker has
    the handler. A paused run or a blocked queue makes them wait.
13. **A skip gives the dedup record back.** The other choice, a later add that revives
    the skipped task, keeps one task per URL and was left out because a refusal at add
    leaves nothing to revive.
14. **Token mismatch fails the attempt.** The softer choice is to send the request and
    report the difference in a metric and in `ListSessions`.
15. **No redirects in place on a judged task.** The other choice is to state the gap
    and leave `WithSameHostRedirects` as it is.
16. **Trust.** Any pool that crawls an origin can set its rules for every run, until
    roles exist.
17. **Budget.** Held and skipped tasks count for `max_tasks`.
18. **Delivery in two steps**, with the benchmark between them.

# ADR 0004: robots.txt: gate, parser and policy

- **Status:** Draft, for review. The section "Points to confirm" lists the choices that
  are still open; each is written into the text below as proposed.
- **Date:** 2026-11-11
- **Related:** [ADR 0001: Frontier storage architecture](0001-frontier-storage-architecture.md),
  [ADR 0003: Worker protocol and SDK](0003-worker-protocol-and-sdk.md),
  [RFC 9309: Robots Exclusion Protocol](https://www.rfc-editor.org/rfc/rfc9309)
- **Naming:** as in ADR 0001. An **origin** is a scheme, a host and a port
  (`https://shop.example:443`), the scope of one robots.txt file.

## Context

The Frontier decides which request goes out and when, and the workers make the requests.
Nothing reads robots.txt today: a page a site disallows is crawled, and a `Crawl-delay`
is ignored. A spider can check robots.txt itself, but then every spider implements it,
every worker process fetches the file again, and no worker can change the pace of a queue
that all runs share.

The rules of a site are a property of the site, as its politeness limits are. They belong
where the limits are kept and enforced: in the Frontier.

Three constraints shape the design:

- **The Frontier makes no requests to crawled sites.** It has no egress to them, no proxy
  settings and no `User-Agent`. A site may answer robots.txt differently by client and by
  address, so the file has to be fetched by the client that fetches the pages.
- **A queue is not an origin.** A queue key is whatever the run's key rule makes it: a
  host, a registered domain or an explicit key. One queue can cover several origins
  (`http` and `https`, subdomains), and each origin has its own file.
- **The scheduler may know better.** Some crawls are of the operator's own sites, or run
  under an agreement with the site, or get the rules from another system. The scheduler
  needs a way to say so, and the operator of the Frontier needs the last word.

## Decision

### Overview

- The Frontier keeps the robots.txt of every origin it crawls and judges every task
  against it.
- A run's tasks for an origin are **held** until the rules of the origin are known. That
  is the gate.
- When a run needs rules that the Frontier does not have, the Frontier adds a
  **robots task** to that run: an ordinary task, paced by the queue like any request,
  with a reserved handler that every SDK worker has built in. Its result seeds the rules
  and opens the gate.
- A run can bring the rules itself or opt out. The queue's policy can forbid that.

```
AddTasks ──► origin has fresh rules? ──yes──► judge ──► added | ROBOTS_DISALLOWED
                    │ no
                    ▼
        hold the task (run, origin)
        add the robots task of (run, origin), if the run has none
                    │
   worker: GET <origin>/robots.txt  ──►  Complete{robots result}
                    │
        Frontier: record on the origin, open the gate of every run waiting on it
                    │
        dispatch: judge again against the rules in force ──► assign | skip
```

### Rules of an origin

The Frontier keeps one record per origin, shared by every run:

| Field | Meaning |
|---|---|
| `state` | `KNOWN` (a file was read), `UNAVAILABLE` (there is no file: everything is allowed), `UNREACHABLE` (the file could not be read: nothing is allowed) |
| `body` | The file as fetched, cut at the parse limit |
| `status`, `fetch_time`, `final_url` | What the fetch saw, for the operator |
| `expire_time` | When the rules have to be read again |
| `retry_time` | `UNREACHABLE` only: when the next robots task may be added |

- The record keeps the file, and each replica parses it and keeps the parsed form in
  memory. A fix in the parser then applies to every stored file without a new fetch. The
  price is that two replicas of different builds can judge one URL differently for the
  length of a rolling deploy.
- Rules are **fresh** until `expire_time`. The lifetime is a policy setting, 24 hours by
  default, which is the longest RFC 9309 allows a cached file to be used.
- A record is removed when it has expired and no run holds tasks on it. The number of
  records is bounded by the origins crawled within one lifetime.
- Only `http` and `https` URLs have an origin. A task with another scheme is not gated.

### The gate

A task is in one of three positions towards the rules of its origin:

1. **The rules are fresh.** The task is judged when it is added. An allowed task joins
   its backlog. A disallowed one is refused with `AddResult.STATUS_ROBOTS_DISALLOWED`:
   it never becomes a task, takes nothing from the run's budget and leaves no dedup
   record, so the same URL can be added once the rules change.
2. **The rules are not known or not fresh, and the run has none of its own.** The task is
   added and **held**. A held task is outside its backlog, as a delayed task is: it is
   not eligible, it takes no politeness slot, and `RunStats` counts it as `delayed`. In
   the same transaction the store adds the robots task of the run and origin, unless the
   run already has one that has not ended.
3. **The run brought rules for the origin, or does not obey robots.txt there.** The task
   is judged against the run's rules, or not at all, and nothing is held.

When a robots task ends, the Frontier records the result on the origin and **opens the
gate**: the held tasks of every run waiting on that origin join their backlogs in one
store call. Held tasks are not read or judged one by one at that point.

Every task is **judged again when it is dispatched**, in the Frontier core, before the
assignment is sent. This is the check that counts; the one at add time is an early
answer. A task that is disallowed at dispatch ends in the state `SKIPPED` with the code
`robots_disallowed`. No request was made, so the politeness slot the claim took is given
back. The check at dispatch also covers rules that changed while a task waited.

Stale rules do not close the gate. When a task is added or dispatched for an origin
whose rules have expired, the Frontier adds a robots task to that run and keeps judging
by the old rules until the new ones arrive. A long run therefore refreshes the rules it
depends on, and an origin nobody crawls is not refreshed.

Two runs that start on one origin at the same moment each get a robots task. Both are
paced by the queue, the first result opens the gate for both, and the second result
replaces the first. The number of redundant fetches is bounded by the number of runs
that started inside one fetch.

### The robots task

- It is a task of the run that needed the rules, on the queue and in the pool of the
  task that caused it. It is therefore fetched through the same workers, the same egress
  and the same `User-Agent` as that run's pages, and it waits for the queue's pace and
  for a queue block like any other request.
- Its handler is the reserved name `crawly.robots`. The worker SDK registers it in every
  worker; a spider cannot register a handler under the `crawly.` prefix. Dispatch
  selects by handler (ADR 0003), so a worker that does not have it is never given the
  task.
- It takes nothing from `RunLimits.max_tasks`, has depth 0 and no parent. It is counted
  in `RunStats.robots` and not in the run's task counters, so the number of pages of a
  run does not depend on how many robots.txt files were read for it. `ListTasks` shows it.
- The built-in handler requests `<origin>/robots.txt` and follows up to five redirects,
  to other hosts as well, as RFC 9309 asks. These hops are the one place where a worker
  follows a redirect to another host inside one task; they are not paced by the other
  host's queue.
- The handler reports what it saw and does not interpret it. `Complete` carries a
  `RobotsResult`: the final status, the final URL, the body cut at the parse limit, and
  whether the fetch failed before an answer. The Frontier decides what the result means.
- A worker that is not built on the SDK implements the same contract: handler name,
  request, redirect limit, `RobotsResult`.

What a result means:

| Result | Origin becomes | Effect |
|---|---|---|
| 2xx | `KNOWN` | The body is parsed. An empty or unparseable body allows everything |
| 4xx | `UNAVAILABLE` | Everything is allowed |
| 401, 403, when the policy says `forbidden: DISALLOW` | `KNOWN`, disallow all | Tasks are skipped with `robots_forbidden` |
| More than five redirects | `UNAVAILABLE` | Everything is allowed |
| 5xx, 429, timeout, connection failure | `UNREACHABLE` | See below |

An origin that is `UNREACHABLE`:

- If rules were known before, they stay in force and the gate stays open. RFC 9309 allows
  a cached file to be used while the file cannot be read.
- Otherwise nothing is allowed. Held tasks stay held, and the next robots task is added
  at `retry_time` to a run that still holds tasks there. The retry interval is a policy
  setting.
- Tasks do not wait for ever. A task held for longer than the policy's
  `unreachable_grace` ends `SKIPPED` with the code `robots_unreachable`. The same happens
  when no worker of the pool has the `crawly.robots` handler for that long.

### Crawl-delay

`Crawl-delay` is not part of RFC 9309, but sites use it and crawlers are expected to
honour it.

- The delay of the group that matches the crawler is a floor under the interval of the
  queue the origin belongs to: the queue's interval is the longest of the policy's
  interval and the delays of its origins with `KNOWN` rules. It is part of the queue's
  shared limiter state, so it holds for every run and every replica.
- It also holds for a run with limits of its own. Run limits replace the queue's limits
  for that run (see `policy.proto`), but the delay is the site's demand, and a run cannot
  go under it.
- The policy has a ceiling, `max_crawl_delay`. A delay above it is not applied: the
  origin is treated as disallowing everything, and its tasks are skipped with the code
  `robots_crawl_delay`. Crawling faster than asked would break the site's rule, and
  crawling at a pace that never ends the run helps nobody.
- `LimiterState` shows the delay in force and the origin it came from.

### Ending a run on robots.txt

The Frontier does not end a run because of robots.txt by itself. Each case above gives
the affected tasks a code: `robots_disallowed`, `robots_forbidden`, `robots_unreachable`,
`robots_crawl_delay`. A run that must end when one of them occurs lists the code in
`RunLimits.abort_on_codes`, and then ends `ABORTED` with that code as the reason. A
skipped task counts for `abort_on_codes`. It does not count as a failure for
`max_failed` and `max_consecutive_failures`: a page the site does not want fetched is
not an error of the crawl.

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
  // the run is judged by this content and the Frontier fetches nothing. An empty
  // content allows everything.
  map<string, string> files = 2;
}
```

- The rules a run brings are robots.txt text. There is one parser and one matcher, and
  the scheduler does not translate a file into another format. A scheduler that fetched
  the file itself passes it on as it is.
- A run's rules are for that run only. They are never written to the origin's record, so
  one scheduler cannot change what another run is allowed to fetch.
- `Crawl-delay` in a run's file is honoured for that run's tasks only, as a floor under
  the run's interval on the queue.
- `MODE_IGNORE` with no files is the opt-out.

### What the policy says

Global policies get a `robots` block. It belongs to the queue, like `max_queue_block`:
it is read from the global policies only, and a run that sets it is rejected.

| Field | Default | Meaning |
|---|---|---|
| `product_token` | none | The token matched against `User-agent` lines. Without it only the `*` groups apply. It must be the token the workers send in their `User-Agent`. A run may set its own, since several crawlers can share one Frontier |
| `enforce` | `false` | `true`: robots.txt is obeyed on this queue whatever the run says. A run's `MODE_IGNORE` and its `files` do not apply here |
| `lifetime` | 24h | How long rules are fresh. At most 24h |
| `forbidden` | `ALLOW` | What a 401 or 403 on robots.txt means: `ALLOW` (RFC 9309) or `DISALLOW` |
| `max_crawl_delay` | none | The ceiling for `Crawl-delay`. Without it every delay is applied |
| `unreachable_retry` | 5m | The interval between robots tasks for an origin that cannot be read |
| `unreachable_grace` | 1h | How long a task is held for an origin that cannot be read |

When `enforce` overrides a run's setting, `PolicyService` shows it in the resolved policy
of the run and queue, and the first task the run adds to the queue is logged. The run is
not rejected: at `CreateRun` the Frontier does not know which queues a run will reach.

### Parser and matcher

The parser is a package of its own, `internal/robots`, without I/O: bytes in, rules out.
The matcher answers one question: may this product token fetch this URL, and with what
delay.

Parsing follows RFC 9309:

- The input is UTF-8. A byte order mark is skipped. Lines end with CR, LF or CRLF. `#`
  starts a comment. A line is `key: value`, keys are case-insensitive, and a line that
  is not understood is ignored and does not end a group.
- The first 500 KiB are parsed and the rest is ignored. That is the limit the RFC
  requires a parser to accept, and it is the limit on `RobotsResult.body`.
- A group is one or more `User-agent` lines followed by rules. The rules are `Allow`,
  `Disallow` and `Crawl-delay`. `Sitemap` lines are kept with the origin's record and
  shown to the operator; the Frontier does not act on them.
- `Crawl-delay` is a number of seconds, fractions allowed. A value that is not a number,
  or is negative, is ignored.

Matching:

- **Group.** The product token is compared without case. All groups that name the token
  are merged. If none does, the `*` groups are merged. If there is none either,
  everything is allowed. `Crawl-delay` is taken from the merged group that was chosen,
  never from `*` when a group names the token.
- **Path.** The rule is compared with the path and query of the URL, both normalised the
  same way: percent-encoded unreserved characters are decoded, other escapes are upper
  case. `*` matches any sequence of characters and `$` anchors the end.
- **Precedence.** The rule with the longest pattern wins. Between an `Allow` and a
  `Disallow` of the same length, `Allow` wins. A `Disallow` without a value is no rule.
- `/robots.txt` is always allowed.
- Matching one URL is linear in the number of rules, and one pattern is matched without
  backtracking, so a file made to be slow costs what its size allows and no more.

The parser is tested with the examples of RFC 9309, with a corpus of real files, and by
fuzzing: it must not fail, hang or allocate beyond a bound on any input of 500 KiB.

### API

- `AddResult.STATUS_ROBOTS_DISALLOWED`.
- `TaskStatus.STATE_SKIPPED`, `RunStats.skipped`, and `RunStats.robots`: files fetched,
  tasks held now, tasks refused at add, tasks skipped at dispatch.
- `Run.robots` (`Robots`), set at `CreateRun`.
- `Policy.robots`, the block above.
- `Complete.robots` (`RobotsResult`), sent by the `crawly.robots` handler only.
- `QueueService.GetRobots(origin)`: the record, the group that matches the queue's
  product token, the delay in force and the sitemaps. `QueueService.RefreshRobots(origin)`
  marks the rules expired, so the next run that touches the origin reads them again.
- `Check(origin, url)` on the same service answers what the matcher answers, for an
  operator who wants to know why a page is not crawled.

### Compatibility

- A Frontier with this design and a worker without the `crawly.robots` handler: the
  robots task is not given to that worker. If no worker of the pool has the handler, the
  run's tasks for new origins are held and then skipped with `robots_unreachable`. An
  operator who upgrades the Frontier first sets `MODE_IGNORE` on runs, or upgrades the
  workers first.
- A worker with the handler and a Frontier without this design: the handler is never
  called.
- Existing runs keep going. Their tasks for origins without a record are judged at
  dispatch like any other, so the first of them is held again until the rules arrive.

## Scaling and limits

- The origin record is one row per origin, written when a robots task ends and read at
  add and at dispatch through each replica's in-memory copy. The copy is bounded and
  refilled from the store; a record's version tells a replica that its copy is old.
- The gate is state per run and origin. Opening it is one store call whose cost is the
  number of held tasks.
- Judging a task costs one match against the rules of one origin and no store access
  when the copy is in memory.
- The robots.txt of an origin is fetched once per lifetime per run that needs it while
  no fresh rules exist. It is not fetched once per worker or once per task.
- Limitation: a replica that has an old copy of the rules can dispatch a task that the
  new rules disallow, for as long as it takes the replica to see the new version. The
  window is one dispatch.
- Limitation: rules are per origin, and a queue can cover several origins. A
  `Crawl-delay` of one origin slows the whole queue.

## Alternatives considered

1. **The Frontier fetches robots.txt itself.** One fetch per origin and no robots task.
   Rejected: the Frontier would need egress to every crawled site and the proxies and
   identity of the workers, and the site would see robots.txt fetched by another client
   than the one that crawls it.
2. **The worker checks before each request.** No change to the Frontier. Rejected: each
   worker process fetches and caches the file, a disallowed task is found only after it
   took a slot and reached a worker, the pace of the shared queue cannot be changed, and
   every worker that is not built on the SDK has to implement the parser.
3. **A robots task per run and origin, without a shared record.** The simplest gate.
   Rejected: many short runs on one site each fetch the file, two runs can crawl one
   origin under different rules, and `Crawl-delay` would have no place to live between
   runs.
4. **A system run and system workers for robots tasks.** The file is fetched once,
   whoever asks. Rejected: it needs a fleet with its own egress, which is alternative 1
   with one more component, and a run that belongs to no scheduler.
5. **Judging held tasks when the gate opens.** A disallowed task would never take a
   slot. Rejected: the store would have to hand task specifications to the core in
   batches, and the check at dispatch is needed anyway for rules that change. Giving the
   slot back at dispatch has the same effect.
6. **Structured rules in the run** (lists of allowed and disallowed patterns) instead of
   robots.txt text. Rejected: a second format with the same meaning, and a translation
   step for every scheduler that already has the file.
7. **A third-party parser.** Rejected: the matcher's answer, `Crawl-delay` and the
   limits above are part of the Frontier's contract with its operators, and the format
   is small enough to own.

## Consequences

- A run obeys robots.txt without any code in the spider.
- The first tasks of a run on a new origin wait for one more request to that origin.
- Every SDK worker can fetch robots.txt, whatever its spiders do.
- A task can end without having been attempted (`SKIPPED`), and schedulers that count
  tasks have one more state to count.
- The operator sees and controls robots.txt per origin: the record, a refresh, a check.

## Points to confirm

1. **Shared record.** The rules are kept per origin and shared by runs, and the robots
   task still belongs to the run that needed them. The first proposal was one robots
   task per run and origin with no shared record (alternative 3).
2. **401 and 403.** The default follows RFC 9309 (allow). `forbidden: DISALLOW` is the
   stricter choice some crawlers make.
3. **`Crawl-delay` above the ceiling.** The origin is treated as disallowed and the run
   ends only if it lists `robots_crawl_delay` in `abort_on_codes`. The other choice is
   to end every such run.
4. **`enforce`.** A queue's policy overrides a run's opt-out without rejecting the run.
   The other choice is to refuse that run's tasks for the queue as `INVALID`.
5. **Counters.** The robots task is outside the run's task counters and budget, in
   `RunStats.robots`.
6. **A run's own files.** They are robots.txt text keyed by origin, and apply to that
   run only.

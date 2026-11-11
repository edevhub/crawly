# Crawly

A distributed crawling framework in Go. Scheduling is separate from fetching: a **Frontier**
holds the crawl state and politeness per site, and any number of stateless **workers**
(spiders) share one crawl through it.

- Design: [ADR 0001](docs/adrs/0001-frontier-storage-architecture.md) (architecture and
  scheduling model), [ADR 0002](docs/adrs/0002-postgresql-store.md) (PostgreSQL store),
  [ADR 0003](docs/adrs/0003-worker-protocol-and-sdk.md) (worker protocol and SDK).
- API: [`api/crawly/frontier`](api/crawly/frontier) (protobuf, five versioned packages).
- Go API: [`sdk/worker`](sdk/worker) for spiders, [`sdk`](sdk) for the scheduler client,
  and `sdk/run`, `sdk/task`, `sdk/policy`, `sdk/queue` for the API types. Everything else is
  under `internal/`.

## Status

Early. What exists today:

| Part | State |
|---|---|
| SDK (`sdk/worker`, `sdk`): workers and scheduler client | Usable: handler and parser strategies, pools, heartbeats, reconnects, graceful shutdown |
| Frontier core (`internal/frontier`) | Usable: all four gRPC services, dispatch loop per pool, reaper, lease revocation |
| In-memory store (`internal/store/memstore`) | Passes the conformance suite. State is lost on restart; one replica only |
| PostgreSQL store (`internal/store/pgstore`, ADR 0002) | Passes the conformance suite. State survives restarts; several replicas share one database. Not benchmarked yet |
| Conformance suite (`internal/store/storetest`) | Covers politeness, overrides, blocks, fairness, pools, fencing, termination, lifecycle |

Not implemented yet: robots.txt handling, Frontier and worker metrics, a view of the
connected workers, roles for API callers (TLS and client certificates are there), an
operator CLI, bucket ownership, the per-replica store call budget, and the SDK's
resource-based tuner.

## Try it

Start a Frontier. Without a database it keeps the state in memory and loses it on restart:

```sh
go run github.com/edevhub/crawly/cmd/crawly-frontier@latest -listen :7070
```

With PostgreSQL 18 the state survives restarts and several replicas can share the database.
Apply the schema migrations first, and again before every new version starts:

```sh
export CRAWLY_POSTGRES='postgres://crawly:secret@localhost:5432/crawly'
go run github.com/edevhub/crawly/cmd/crawly-frontier@latest migrate
go run github.com/edevhub/crawly/cmd/crawly-frontier@latest -listen :7070
```

`migrate status` lists the migrations and whether they are applied. The Frontier does not
change the schema on its own and refuses to start on a database that is behind its build.
The migrations are recorded in the table `crawly_migrations`; to use another name, pass
`-postgres-migrations-table` (or `CRAWLY_POSTGRES_MIGRATIONS_TABLE`) to both commands.

`-postgres` takes the same URL. When that URL goes through a transaction pooler such as
PgBouncer, give the Frontier a direct one for `LISTEN` with `-postgres-listen` (or
`CRAWLY_POSTGRES_LISTEN`).

Run the example spider and scheduler against it:

```sh
go run github.com/edevhub/crawly/examples/quickstart@latest -url https://example.com/ -depth 2
```

## Install

```sh
go get github.com/edevhub/crawly
```

The module needs Go 1.26 or newer. With an older toolchain, `go` downloads a newer one or
fails, depending on `GOTOOLCHAIN`.

| Import | For |
|---|---|
| `github.com/edevhub/crawly/sdk/worker` | Writing spiders |
| `github.com/edevhub/crawly/sdk` | The client for schedulers and operators |
| `github.com/edevhub/crawly/sdk/run`, `sdk/task`, `sdk/policy`, `sdk/queue`, `sdk/session` | The API types, one package per API area |
| `github.com/edevhub/crawly/sdk/worker/prom` | Prometheus metrics of a worker (see [Metrics](#metrics)); import it only if you want them |

## Writing a spider

A spider implements one interface and registers it with a worker. The SDK handles the
session, credits, lease heartbeats, ordering and reconnects.

**Parser strategy**: the SDK fetches over HTTP, the spider parses.

```go
package main

import (
	"context"
	"log"
	"os/signal"
	"syscall"

	"github.com/edevhub/crawly/sdk/worker"
)

type productSpider struct{}

func (productSpider) Parse(ctx context.Context, t *worker.Task, resp *worker.Response) (*worker.Result, error) {
	res := &worker.Result{Items: 1}
	for _, link := range extractLinks(resp.URL, resp.Body) {
		res.Tasks = append(res.Tasks, worker.NewTask{URL: link}) // same handler, same run
	}
	return res, nil
}

func main() {
	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	w, err := worker.New("localhost:7070", worker.WithInsecure(), worker.WithMaxConcurrentTasks(16))
	if err != nil {
		log.Fatal(err)
	}
	if err := w.Register("product", worker.HTTP(productSpider{}, worker.WithUserAgent("my-bot/1.0"))); err != nil {
		log.Fatal(err)
	}
	if err := w.Run(ctx); err != nil { // returns after a graceful shutdown
		log.Fatal(err)
	}
}
```

`worker.ParserFunc` does the same for a plain function.

**Handler strategy**: the spider does everything itself (headless browser, API client).

```go
err := w.Register("render", worker.HandlerFunc(func(ctx context.Context, t *worker.Task) (*worker.Result, error) {
	html, err := browser.Render(ctx, t.URL)
	if err != nil {
		return nil, worker.Retry("render_failed", err)
	}
	return &worker.Result{Items: countItems(html), Code: "rendered"}, nil
}), worker.InPool("browser"), worker.WithTimeout(2*time.Minute))
```

One worker can register any number of handlers. A task is routed by its handler name.

### Options

| Where | Option | Meaning |
|---|---|---|
| `worker.New` | `WithMaxConcurrentTasks(n)` | How many handlers run at once, per pool the worker serves. Default 8 |
| | `WithMaxTasksPerSecond(n)` | How fast the worker takes work: at most `n` task starts a second, over all its pools. Protects the worker, not the sites |
| | `WithID(id)`, `WithBuildVersion(v)` | Recorded on every attempt, for lineage |
| | `WithShutdownGrace(d)` | How long running handlers get to finish on shutdown |
| | `WithInsecure()` or `WithDialOptions(...)` | Transport security has no default: plaintext must be asked for, or pass gRPC credentials |
| | `WithLogger(l)` | `slog` logger |
| | `WithMetrics(m)` | Report to a `worker.Metrics`; `prom.New(registry)` is the Prometheus one (see [Metrics](#metrics)). Default: none |
| `Register` | `InPool(name)` | The pool that executes the handler. The worker opens one session per pool |
| | `WithTimeout(d)` | Upper bound for one call of the handler |
| `worker.HTTP` | `WithUserAgent(ua)` | User-Agent for requests that carry none |
| | `WithHTTPClient(c)` | Your own client: proxies, cookies, TLS |
| | `WithMaxBodyBytes(n)` | Largest body accepted; a larger one fails with `body_too_large`. Default 10 MiB |
| | `WithContentTypes(types...)` | Parse only these media types; another one fails with `unsupported_content_type`. Default: all |
| | `WithStatus(parser, statuses...)` | Hand these statuses to a parser instead of the default classification: a 404 that is a result, a 403 that is a block |
| | `WithRedirect(func)` | Adjust the task added for a redirect before it is added, e.g. set its `DedupKey`. The function sees the redirect response |
| | `WithSameHostRedirects()` | Follow redirects on the task's host inside the task. Each hop is a request on top of the site's rate limit, and it is not deduplicated |
| | `WithCrossHostRedirects()` | Follow every redirect, also to other hosts, which are then fetched outside their own limits |

### Outcomes

Outcomes are plain Go errors:

| Return | Reported as |
|---|---|
| `nil` error | Success |
| `worker.Retry(code, err)` | Transient failure; retried with backoff up to the policy's `max_attempts` |
| `worker.Retry(code, err, worker.After(d))` | The same, and not before `d` |
| `worker.Retry(code, err, worker.BlockQueue(d))` | The same, and the whole site is paused for `d`, for every run |
| `worker.Fail(code, err)` | Permanent failure; never retried |
| any other error | Transient failure, code `error` (`timeout` for a deadline) |
| a panic | Transient failure, code `panic` |
| the worker shuts down before the handler returns | The task is handed back to the Frontier: another worker runs it, and no attempt is counted |

The codes end up in the run's stats (`Stats.ByCode`), so they are what you alert on.

The HTTP adapter classifies responses itself: 2xx is parsed, 429 retries and blocks the
site for the `Retry-After` delay, 408 and 5xx retry, other statuses fail, and a redirect is
not followed: its target is added as a new task, so that the Frontier paces and deduplicates
it like any other page. A parser can still return
`worker.Retry` or `worker.Fail` for a 2xx page, for example a captcha served with status 200.

A status means what the spider says it means. `WithStatus` hands a status to a parser of
its own, whose return value is the outcome of the task:

```go
gone := worker.ParserFunc(func(ctx context.Context, t *worker.Task, r *worker.Response) (*worker.Result, error) {
	return &worker.Result{Code: "gone"}, markGone(ctx, t.URL) // a 404 here is a result
})
h := worker.HTTP(parser,
	worker.WithStatus(gone, http.StatusNotFound, http.StatusGone),
	worker.WithStatus(worker.Blocked(72*time.Hour), http.StatusForbidden))
```

`worker.Blocked(d)` fails the task with code `blocked` and asks the Frontier to leave the
site alone for `d`, for every run. A parser that recognises a challenge page served with
status 200 can return the same: `return worker.Blocked(d).Parse(ctx, t, r)`.

A 307 or 308 repeats the request, so the task added for it keeps the method and the body.
It keeps the task's headers only on the same host: to another host, or from https to http,
only `Accept`, `Accept-Language`, `Content-Language`, `Content-Type` and `User-Agent` go
along, so that a site cannot send a spider's credentials to a host of its choosing. The
body goes along in every case, as it does in browsers; `WithRedirect` is the place to
change the task when the body carries a secret. A redirect from https to http is never
followed inside a task, also with `WithSameHostRedirects`: its target becomes a task.

### Delivery

A task is executed **at least once**. When a worker dies or loses its lease after the
handler did its work and before the Frontier recorded the result, the task is given out
again and the handler runs a second time. Storing items is your code's business
(`Result.Items` is only a count for the statistics), so write them idempotently: `Task.ID`
is the same for every attempt of a task and is the key to use.

### Discovered tasks

```go
res.Tasks = append(res.Tasks, worker.NewTask{
	URL:      "https://shop.example/item/42?ref=list",
	Handler:  "item",                                 // empty: the handler that found it
	DedupKey: "item/42",                              // empty: the normalised URL
	Context:  map[string][]byte{"category": []byte("books")}, // handed to the task as t.Context
})
```

Discovered tasks are deduplicated per run by a normalised URL (scheme + host + path +
sorted query; scheme and host lowercased, path and query as they are). A method other
than GET and a body that is not empty are part of the key as well, so the pages of a
search API behind one URL are separate tasks. Set `DedupKey` when a site needs special
rules: case-insensitive paths, tracking parameters, requests told apart by a header.

A task is limited to 1 MiB (`worker.MaxTaskBytes`): URL, headers, body and `Context`
together. A handler that returns a larger one fails its own task with code
`task_too_large`. Any number of tasks can be returned from one handler: the SDK sends them
in batches bounded by count and by size. Upgrade the workers with the Frontier or before
it: a worker of an older SDK does not notice a task the Frontier refuses for its size.

### Testing a spider, and dry runs

`sdk/crawlytest` runs a Frontier inside a test, on the in-memory store. The spider is
tested end to end, with the retries, dedup and limits it meets in production, against
pages you recorded, and without a Frontier process or a database:

```go
func TestSpider(t *testing.T) {
	site := httptest.NewServer(recordedPages) // your fixtures
	defer site.Close()

	f := crawlytest.Start(t)
	w, _ := f.Worker()
	w.Register("listing", worker.HTTP(listing))
	w.Register("product", worker.HTTP(product))

	final, err := f.Crawl(t.Context(), w,
		[]*task.Spec{{Request: &task.Request{Url: site.URL + "/"}, Handler: "listing"}})
	// assert on final.GetStats() and on what the parsers emitted
}
```

`sdk/local` is the same Frontier outside a test, for a dry run of a spider against a real
site: `local.Start()`, then `Crawl` with a `max_tasks` limit (`local.WithRun`) and a
deadline on its context. It applies the Frontier's default policies unless
`local.WithPolicies` gives it your policy file, so a dry run is as polite as the real
crawl. A crawl that is cut short returns the run as it stood, with its statistics, and
cancels it. Its state is in memory and gone when the process ends.

## Scheduling a run

```go
package main

import (
	"context"
	"log"
	"time"

	"github.com/edevhub/crawly/sdk"
	"github.com/edevhub/crawly/sdk/run"
	"github.com/edevhub/crawly/sdk/task"
)

func main() {
	ctx := context.Background()
	c, err := sdk.NewClient("localhost:7070", sdk.WithInsecure())
	if err != nil {
		log.Fatal(err)
	}
	defer c.Close()

	const id = "refresh/2026-11-10"
	if _, err := c.Runs.CreateRun(ctx, &run.CreateRunRequest{
		RunId:  id,
		Labels: map[string]string{"source": "example"},
		Limits: &run.Limits{MaxDepth: 3, MaxTasks: 10_000},
	}); err != nil {
		log.Fatal(err)
	}

	results, err := c.Seed(ctx, id,
		&task.Spec{Request: &task.Request{Url: "https://example.com/"}, Handler: "product"},
		&task.Spec{Request: &task.Request{Url: "https://example.com/sale"}, Handler: "product"},
	)
	if err != nil {
		log.Fatal(err)
	}
	for i, r := range results { // one result per task, in order
		if r.GetStatus() != task.StatusAdded {
			log.Printf("seed %d: %v", i, r.GetStatus())
		}
	}

	// Sealed: the run completes once nothing is pending or in flight.
	if _, err := c.Runs.SealRun(ctx, &run.SealRunRequest{RunId: id}); err != nil {
		log.Fatal(err)
	}

	// Blocks until the run has ended, and reconnects when a Frontier restarts.
	final, err := c.Wait(ctx, id, sdk.StatsEvery(5*time.Second), sdk.OnEvent(func(ev *run.Event) {
		st := ev.GetRun().GetStats()
		log.Printf("%v: %d ok, %d failed, %d pending, codes %v", ev.GetType(),
			st.GetSucceeded(), st.GetFailed(), st.GetPending(), st.GetByCode())
	}))
	if err != nil {
		log.Fatal(err)
	}
	log.Printf("run %s: %v", id, final.GetState())
}
```

`CreateRun` is idempotent for the same request, so a scheduler can retry it. `Seed` sends
tasks in batches; when it fails part of the way it returns the results of the batches
that were committed together with the error, and seeding again is safe. A seed over
1 MiB (`sdk.MaxTaskBytes`) is not sent and its result is `INVALID`. `Wait` returns the
run once it is completed, cancelled or expired; it reconnects with backoff when the stream
breaks, which happens on every restart of a Frontier, and fails with `NotFound` when the
run does not exist. Underneath it is `WatchRuns`, a raw stream that starts with the run's
current state when it is given a `RunId`. The other
run calls are `PauseRun`, `ResumeRun`, `CancelRun`, `GetRun`, `ListRuns`, `ListTasks` and
`GetTask`. `c.Queues`, `c.Policies` and `c.Sessions` are the operator APIs. `c.Sessions.ListSessions`
lists the workers that are connected: for each session its id, worker id, build version, pool,
connection time, unused credits, unfinished assignments and the replica that holds it. A
session lives in the memory of the replica that serves its stream, so the call answers for
the replica it reaches, and names it (`replica_id`). With several replicas, ask each one,
e.g. through a headless service, or read `crawly_frontier_sessions` for the count per pool.

A run can set limits for itself, for every site or for the sites a glob matches:

```go
rate := 0.5
c.Runs.CreateRun(ctx, &run.CreateRunRequest{
	RunId: id,
	Policy: &policy.Run{Overrides: []*policy.RunOverride{
		{QueueKey: "*.example.com", Limits: &policy.Limits{RatePerSecond: &rate}},
	}},
})
```

## Policies

Politeness limits are static configuration, given to the Frontier at start
(`-policies policies.json`). Without a file every site gets the built-in default (1 request
per second, 2 concurrent).

```json
{"policies": [
  {"name": "default", "limits": {"ratePerSecond": 1, "maxConcurrency": 2}},
  {"name": "example", "match": {"queueKey": "*.example.com"},
   "limits": {"ratePerSecond": 5, "burst": 5},
   "behaviour": {"maxAttempts": 5, "leaseDuration": "60s"}}
]}
```

These are the global policies. A run's own policy is given at `CreateRun` (see above).

A spider that finds the crawler blocked asks the Frontier to leave the site alone
(`worker.Blocked(d)`, or `worker.BlockQueue(d)` on any outcome). One outcome blocks a site
for 24 hours at most, unless the site's policy allows more with
`"behaviour": {"maxQueueBlock": "259200s"}` (72 hours). The ceiling is read from the global policies only,
since a block stops every run on the site, and a run that sets it is rejected. A site's own
`Retry-After` header is followed for 24 hours at most, whatever the ceiling.

## Running the Frontier

**Transport security.** Without flags the Frontier serves plaintext, which suits a trial
on one machine only: anyone who can reach the port can use the whole API.

```sh
crawly-frontier -tls-cert server.pem -tls-key server-key.pem -tls-client-ca clients-ca.pem
```

- `-tls-cert` and `-tls-key` turn TLS on. The files are read again for every new
  connection, so a renewed certificate needs no restart.
- `-tls-client-ca` makes every caller present a certificate signed by that CA (mutual
  TLS). Every holder of such a certificate may call every service: the Frontier has no
  roles yet, so keep worker and operator networks apart where that matters.

Workers and schedulers pass gRPC credentials instead of `WithInsecure()`:

```go
creds := credentials.NewTLS(&tls.Config{RootCAs: roots, Certificates: []tls.Certificate{clientCert}})
w, err := worker.New("frontier:7070", worker.WithDialOptions(grpc.WithTransportCredentials(creds)))
c, err := sdk.NewClient("frontier:7070", grpc.WithTransportCredentials(creds))
```

**Health.** The Frontier serves the standard gRPC health service (`grpc.health.v1`). It
answers `SERVING` while the replica takes work, and `NOT_SERVING` before its background
loops run, while it cannot reach the store, and from the moment it drains for a shutdown.
Use it for readiness, e.g. with `grpc_health_probe` or a Kubernetes gRPC probe.

**Retention.** By default nothing is deleted. `-retain-runs 720h` deletes a run, with its
statistics, that long after it ended. With PostgreSQL the tasks and attempts live in
monthly partitions, and `-retain-tasks 2160h` drops a partition once every run in it is
older than that and has no work left; the in-memory store deletes a run's tasks with the
run.

**Deploys.** A worker that is given a task for a handler it does not have hands the task
back, and the Frontier gives it to another worker of the pool. A rolling deploy that adds
or renames a handler therefore loses nothing while both builds run. A task whose handler no
worker has fails with code `no_handler` after `-no-handler-grace` (one hour by default). A
worker that stops lets its handlers finish for `WithShutdownGrace`, then cancels them and
hands their tasks back, so a deploy never uses up a task's attempts.

**Metrics.** `-metrics-addr :9090` serves Prometheus metrics at `/metrics`, in plain HTTP
and without authentication, so bind it to a private interface. Without the flag nothing
listens. See [Metrics](#metrics).

**Logs.** A lease that expires is logged at `WARN` with its run, task and queue: the worker
that held it has died or is stuck.

## Metrics

Both sides export Prometheus metrics, and neither opens a listener or touches the default
registry on its own.

**Frontier.** `crawly-frontier -metrics-addr :9090` serves `/metrics`, with the Go runtime
and process collectors. When you embed the Frontier, pass a `prometheus.Registerer` as
`frontier.Config.Metrics` and expose the registry yourself.

**Worker SDK.** `worker.WithMetrics(m)` takes a small interface, `worker.Metrics`, so the
SDK itself does not import a metrics library. The package `sdk/worker/prom` implements it
with Prometheus. A spider that does not import `prom` does not link `client_golang` (the
module's `go.mod` still lists it, and `sdk/local` and `sdk/crawlytest` link it because they run the Frontier in your process):

```go
reg := prometheus.NewRegistry()
m, err := prom.New(reg)
w, err := worker.New("frontier:7070", worker.WithInsecure(), worker.WithMetrics(m))
// serve it yourself: http.Handle("/metrics", promhttp.HandlerFor(reg, promhttp.HandlerOpts{}))
```

### Label rules

Queue keys are hosts, so there is no bound on them, and no metric carries one. Instead:

- **`policy`** is the name of the most specific global policy that matches the queue, or
  `builtin` when none does. A queue that has a policy of its own, or whose group has one
  (`*.example.com`), gets its own series; the others fall into the policy that matches
  them, or into `builtin`. The number of values is the number of policies plus one. To
  watch a site on its own, give it a policy. To find one site that is misbehaving among
  many, use the `ListQueues` call (`only_blocked`).
- **`pool`, `worker` and `code`** are chosen by workers. Each is capped per process:
  100 pools, 200 worker ids, 100 codes. A value past the cap is counted as `other`.
  Worker ids label lease expiries only.
- Nothing is labelled by run, task or URL.

### Scope with several replicas

Counters, histograms, `sessions`, `credits` and `assignments` are **per replica**: it counts
what it did and holds. Sum them over replicas. The gauges that read the store
(`pool_tasks`, `oldest_due_task_age_seconds`) are **global**: every replica reports the
same figure, so use `max`, not `sum`. They are read at scrape time, at most once every five
seconds, from the readiness index and the backlogs that have work, never from the tasks.
When the store cannot be read they are left out.

### Frontier metrics

All names start with `crawly_frontier_`.

| Name | Type | Labels | Scope | Serves the alert |
|---|---|---|---|---|
| `oldest_due_task_age_seconds` | gauge | `pool`, `policy` | global | Due work that nobody works. Age of the longest waiting ready work; absent when none is ready |
| `sessions` | gauge | `pool` | replica | The same alert: `oldest_due_task_age_seconds > 300 and on(pool) sum(sessions) == 0`. 0 for a pool the replica knows and has no session in |
| `pool_tasks` | gauge | `pool`, `state` (`pending`, `delayed`, `in_flight`) | global | Backlog size. `pending` is ready work, `delayed` waits for a time, a slot or a resume |
| `credits`, `assignments` | gauge | `pool` | replica | Free slots workers granted; assignments held and not completed |
| `lease_expiries_total` | counter | `worker`, `pool` | replica | A worker whose lease was reaped |
| `queue_blocks_total` | counter | `policy` | replica | A site blocked the crawler |
| `dispatch_wait_seconds` | histogram | `policy` | replica | Time spent keeping the pace: how long a task was eligible before it was dispatched |
| `fetch_duration_seconds` | histogram | `policy` | replica | A slow site, from the fetch time workers report |
| `released_tasks_total` | counter | `pool`, `code` (`no_handler`, `shutdown`, `undelivered`) | replica | Tasks going round between workers |
| `dispatched_tasks_total` | counter | `pool`, `policy` | replica | Dispatches |
| `task_outcomes_total` | counter | `pool`, `kind`, `code`, `result` | replica | Outcomes by the kind and code the worker gave, and what the Frontier decided (`succeeded`, `retry`, `failed`, `released`) |
| `attempts_total` | counter | `pool` | replica | Attempts that ended; a release is not one |
| `tasks_added_total` | counter | `source` (`seed`, `discovered`), `status` | replica | Added, `duplicate`, `over_budget`, `run_closed`, `invalid` |
| `sessions_opened_total` | counter | `pool` | replica | Reconnects: a rate far above the number of workers |
| `store_call_duration_seconds` | histogram | `method` | replica | A slow store |
| `store_errors_total` | counter | `method`, `kind` (`unavailable`, `other`) | replica | A failing store. Answers such as not found or a stale lease are not failures |
| `grpc_handled_total` | counter | `method`, `code` | replica | Calls finished. A `Work` call ends when its session does |
| `replica_info` | gauge | `replica`, `policy_set` | replica | Always 1; ties a series to the replica `ListSessions` names |

`oldest_due_task_age_seconds` measures how long a queue has been ready: not paused, not
blocked, not held back by politeness or by its concurrency limit. A queue held back by
its pace is not due. The store keeps the time a backlog became ready and may keep it
earlier than it was, never later, so the age may be too long, never too short. At most the
1000 longest ready backlogs of a pool are looked at, so a policy whose due work is younger
than all of those is not shown while a large part of the pool is already late.

### Worker metrics

All names start with `crawly_worker_`; the registry of the process says which worker.

| Name | Type | Labels | Meaning |
|---|---|---|---|
| `sessions` | gauge | `pool` | Sessions open with the Frontier: 0 while it reconnects |
| `sessions_opened_total` | counter | `pool` | More than one per pool means the worker reconnected |
| `tasks_running` | gauge | `pool`, `handler` | Handlers running now |
| `tasks_finished_total` | counter | `pool`, `handler`, `kind`, `code` | How tasks ended: `success`, `retry`, `failed`, `released`, or `lost` when the lease was lost and nothing was reported. `handler` is `unregistered` for a task whose handler the worker lacks |
| `task_duration_seconds` | histogram | `pool`, `handler` | How long handlers ran |
| `fetch_duration_seconds` | histogram | `pool`, `handler` | Fetch duration the handler reported (`worker.HTTP` reports it) |
| `leases_lost_total` | counter | `pool`, `reason` | `fenced`, `expired`, `run_cancelled`, `max_duration`, `no_extension`, `report_discarded` |

## Development

```sh
make generate   # regenerate Go code from the protos (needs buf)
make check      # buf lint, golangci-lint (strict, see .golangci.yml), tests with the race detector
```

`make check` starts a throwaway PostgreSQL 18 in Docker for the tests of the PostgreSQL
store (`make postgres-stop` removes it). Plain `go test ./...` skips those tests unless
`CRAWLY_TEST_POSTGRES` names a database they may use.

Releases are semantic version tags. The `Release` workflow computes the next version, runs
the full test suite and only then tags and publishes; a `v*` tag pushed by hand goes
through the same gate.

## License

[MIT](LICENSE).

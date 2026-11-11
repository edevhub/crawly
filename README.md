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

Not implemented yet: bucket ownership, the per-replica store call budget, Frontier metrics,
and the SDK's resource-based tuner.

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

| Import | For |
|---|---|
| `github.com/edevhub/crawly/sdk/worker` | Writing spiders |
| `github.com/edevhub/crawly/sdk` | The client for schedulers and operators |
| `github.com/edevhub/crawly/sdk/run`, `sdk/task`, `sdk/policy`, `sdk/queue` | The API types, one package per API area |

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
| `worker.New` | `WithMaxConcurrentTasks(n)` | How many handlers run at once |
| | `WithMaxTasksPerSecond(n)` | How fast the worker takes work: at most `n` task starts a second, over all its pools. Protects the worker, not the sites |
| | `WithID(id)`, `WithBuildVersion(v)` | Recorded on every attempt, for lineage |
| | `WithShutdownGrace(d)` | How long running handlers get to finish on shutdown |
| | `WithInsecure()` or `WithDialOptions(...)` | Transport security has no default: plaintext must be asked for, or pass gRPC credentials |
| | `WithLogger(l)` | `slog` logger |
| `Register` | `InPool(name)` | The pool that executes the handler. The worker opens one session per pool |
| | `WithTimeout(d)` | Upper bound for one call of the handler |
| `worker.HTTP` | `WithUserAgent(ua)` | User-Agent for requests that carry none |
| | `WithHTTPClient(c)` | Your own client: proxies, cookies, TLS |
| | `WithMaxBodyBytes(n)` | Largest body accepted; a larger one fails with `body_too_large`. Default 10 MiB |
| | `WithCrossHostRedirects()` | Follow redirects to other hosts instead of adding them as tasks |

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

The codes end up in the run's stats (`Stats.ByCode`), so they are what you alert on.

The HTTP adapter classifies responses itself: 2xx is parsed, 429 retries and blocks the
site for the `Retry-After` delay, 408 and 5xx retry, other statuses fail, and a redirect to
another host is added as a new task instead of followed. A parser can still return
`worker.Retry` or `worker.Fail` for a 2xx page, for example a captcha served with status 200.

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
sorted query; scheme and host lowercased, path and query as they are). Set `DedupKey` when
a site needs special rules: case-insensitive paths, tracking parameters, POST bodies.

## Scheduling a run

```go
package main

import (
	"context"
	"log"
	"time"

	"google.golang.org/protobuf/types/known/durationpb"

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

	watch, err := c.Runs.WatchRuns(ctx, &run.WatchRunsRequest{RunId: id, StatsInterval: durationpb.New(5 * time.Second)})
	if err != nil {
		log.Fatal(err)
	}
	for {
		res, err := watch.Recv()
		if err != nil {
			log.Fatal(err)
		}
		st := res.GetEvent().GetRun().GetStats()
		log.Printf("%v: %d ok, %d failed, %d pending, codes %v", res.GetEvent().GetType(),
			st.GetSucceeded(), st.GetFailed(), st.GetPending(), st.GetByCode())
		if res.GetEvent().GetType() == run.EventCompleted {
			return
		}
	}
}
```

`CreateRun` is idempotent for the same request, so a scheduler can retry it. `Seed` sends
tasks in batches; when it fails part of the way it returns the results of the batches
that were committed together with the error, and seeding again is safe. `WatchRuns` with
a `RunId` starts with the run's current state, so a watcher that subscribes after the
run has ended is told at once. The other
run calls are `PauseRun`, `ResumeRun`, `CancelRun`, `GetRun`, `ListRuns`, `ListTasks` and
`GetTask`. `c.Queues` and `c.Policies` are the operator APIs.

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

// Package worker is the Crawly worker SDK. A spider implements one small interface and
// registers it with a Worker; the SDK does the rest: the session with the Frontier,
// credits, lease heartbeats, ordering of discovered tasks and completions, reconnects
// and graceful shutdown.
//
// Two strategies are offered:
//
//   - Handler: the spider is given a task and does everything itself, including the
//     fetch. Use it for headless browsers, APIs or anything that is not a plain GET.
//   - Parser: the SDK fetches the page over HTTP and the spider only parses it. Wrap
//     it with HTTP to get a Handler.
//
// A minimal spider:
//
//	w, _ := worker.New("localhost:7070", worker.WithInsecure())
//	w.Register("page", worker.HTTP(worker.ParserFunc(
//		func(ctx context.Context, t *worker.Task, r *worker.Response) (*worker.Result, error) {
//			return &worker.Result{Items: 1}, nil
//		})))
//	w.Run(ctx)
//
// # Delivery
//
// A task is executed at least once. When a worker dies, or loses its lease, after the
// handler did its work and before the Frontier recorded the result, the task is given
// out again and the handler runs a second time. A handler that writes items
// somewhere must therefore write them idempotently. Task.ID is the key for that: it is
// the same for every attempt of a task, and Task.Attempt tells the attempts apart.
package worker

import (
	"context"
	"errors"
	"net/http"
	"time"
)

// Handler is the strategy a worker runs for a task. It is the only thing a spider has
// to implement. Handle is called concurrently for different tasks.
//
// Return a nil error for success. Return an error built with Retry or Fail to classify
// a failure; any other error is reported as a retryable failure with code "error". The
// context is cancelled when the lease is lost or when the worker shuts down and its
// grace has passed. A handler that lost its lease is not reported. A handler cut by a
// shutdown hands its task back: another worker gets it and no attempt is counted.
type Handler interface {
	Handle(ctx context.Context, task *Task) (*Result, error)
}

// HandlerFunc adapts a function to a Handler.
type HandlerFunc func(ctx context.Context, task *Task) (*Result, error)

// Handle calls f.
func (f HandlerFunc) Handle(ctx context.Context, task *Task) (*Result, error) { return f(ctx, task) }

// Task is one unit of work handed to a handler.
type Task struct {
	ID       string // the same for every attempt: the idempotency key for what the handler writes
	RunID    string
	QueueKey string // the politeness domain, usually the host
	Handler  string // the name the task was routed by
	ParentID string // the task that discovered this one; empty for seeds
	Depth    uint32 // 0 for seeds, parent depth + 1 for discovered tasks
	Attempt  uint32 // 1 for the first dispatch

	URL    string
	Method string // "GET" when the task named none
	Header http.Header
	Body   []byte

	// Context is opaque spider data carried from the task that added this one.
	Context map[string][]byte
	// FetchTimeout is the fetch timeout resolved by the Frontier's policies.
	FetchTimeout time.Duration
}

// Result is what a handler reports for a task it processed.
type Result struct {
	// Tasks are discovered tasks, added to the same run before the task completes.
	Tasks []NewTask
	// Items is how many items the spider emitted downstream. It is a count for the
	// run's statistics; storing the items is the handler's own business.
	Items int
	// Code is an optional machine-readable code for stats, e.g. "http_200".
	Code string
	// BlockQueueFor asks the Frontier to stop dispatching the task's whole queue, for
	// every run, for this long.
	BlockQueueFor time.Duration
	// Fetch describes the fetch, for lineage. The HTTP adapter fills it.
	Fetch *FetchInfo
}

// FetchInfo describes one fetch.
type FetchInfo struct {
	HTTPStatus  int
	FinalURL    string
	Latency     time.Duration
	Bytes       int64
	ResponseRef string // where the raw response was stored
	ContentHash string
}

// NewTask is a discovered task.
type NewTask struct {
	URL    string
	Method string
	Header http.Header
	Body   []byte
	// Handler that should process it. Empty: the handler that discovered it.
	Handler string
	// Pool that should execute it. Empty: the pool its handler is registered in on
	// this worker, else the pool of the task that discovered it.
	Pool string
	// QueueKey overrides the queue derived from the URL.
	QueueKey string
	// DedupKey overrides the default URL normalisation. Set it when the site needs
	// special rules: case-sensitive paths, tracking parameters, POST bodies.
	DedupKey  string
	Priority  int32
	NotBefore time.Time
	Context   map[string][]byte
}

// Kind classifies a failure.
type Kind int

const (
	// KindRetry is a transient failure. The Frontier retries it with backoff until
	// the policy's max_attempts.
	KindRetry Kind = iota + 1
	// KindFail is a permanent failure. It is never retried.
	KindFail
	// kindRelease hands the task back to the Frontier: it was not run to an end, and
	// no attempt is counted.
	kindRelease
)

// Codes of a task the worker hands back.
const (
	codeNoHandler = "no_handler"
	codeShutdown  = "shutdown"
)

// Error is a classified failure. Build one with Retry or Fail.
type Error struct {
	Kind          Kind
	Code          string        // machine-readable, for stats and alerting
	RetryAfter    time.Duration // KindRetry: not before this delay; zero uses policy backoff
	BlockQueueFor time.Duration // stop the whole queue, for every run, for this long
	Err           error
}

func (e *Error) Error() string {
	if e.Err != nil {
		return e.Code + ": " + e.Err.Error()
	}
	return e.Code
}

func (e *Error) Unwrap() error { return e.Err }

// ErrorOption adjusts a classified failure.
type ErrorOption func(*Error)

// After asks the Frontier not to retry the task before the delay.
func After(d time.Duration) ErrorOption { return func(e *Error) { e.RetryAfter = d } }

// BlockQueue asks the Frontier to stop dispatching the task's whole queue for the
// duration, e.g. from a Retry-After header on a 429.
func BlockQueue(d time.Duration) ErrorOption { return func(e *Error) { e.BlockQueueFor = d } }

// Retry reports a transient failure.
func Retry(code string, err error, opts ...ErrorOption) error {
	return newError(KindRetry, code, err, opts)
}

// Fail reports a permanent failure.
func Fail(code string, err error, opts ...ErrorOption) error {
	return newError(KindFail, code, err, opts)
}

func newError(kind Kind, code string, err error, opts []ErrorOption) error {
	e := &Error{Kind: kind, Code: code, Err: err}
	for _, o := range opts {
		o(e)
	}
	return e
}

// classify turns a handler error into a classified failure.
func classify(err error) *Error {
	if e, ok := errors.AsType[*Error](err); ok {
		return e
	}
	if errors.Is(err, context.DeadlineExceeded) {
		return &Error{Kind: KindRetry, Code: "timeout", Err: err}
	}
	return &Error{Kind: KindRetry, Code: "error", Err: err}
}

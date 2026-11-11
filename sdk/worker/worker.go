package worker

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"math"
	"net/http"
	"os"
	"runtime/debug"
	"slices"
	"sync"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/keepalive"
	"google.golang.org/protobuf/types/known/durationpb"
	"google.golang.org/protobuf/types/known/timestamppb"

	taskv1 "github.com/edevhub/crawly/internal/gen/frontier/task/v1"
	workerv1 "github.com/edevhub/crawly/internal/gen/frontier/worker/v1"
	"github.com/edevhub/crawly/internal/poolname"
	"github.com/edevhub/crawly/internal/safeconv"
)

// DefaultPool is the pool a handler serves unless registered with InPool.
const DefaultPool = poolname.Default

// Option configures a Worker.
type Option func(*Worker)

// WithID sets the worker's stable id, recorded on every attempt. Default: host name and pid.
func WithID(id string) Option { return func(w *Worker) { w.id = id } }

// WithBuildVersion sets the spider build version, recorded on every attempt.
func WithBuildVersion(v string) Option { return func(w *Worker) { w.version = v } }

// WithMaxConcurrentTasks sets how many tasks run at once in each pool the worker
// serves: a worker with handlers in three pools runs up to three times as many.
// Default 8.
func WithMaxConcurrentTasks(n int) Option { return func(w *Worker) { w.maxTasks = n } }

// WithMaxTasksPerSecond limits how fast the worker takes work: it starts at most n
// tasks per second, evenly spaced, across all its pools. It protects the worker and
// what it depends on; politeness towards the crawled sites stays in the Frontier.
// Zero, the default, sets no limit.
func WithMaxTasksPerSecond(n float64) Option { return func(w *Worker) { w.perSecond = n } }

// WithDialOptions adds gRPC dial options. Transport security is the caller's choice
// and has no default: pass grpc.WithTransportCredentials here, or use WithInsecure.
func WithDialOptions(opts ...grpc.DialOption) Option {
	return func(w *Worker) { w.dial = append(w.dial, opts...) }
}

// WithInsecure connects without transport security: no encryption and no
// authentication of the Frontier. Use it on a trusted network only.
func WithInsecure() Option {
	return WithDialOptions(grpc.WithTransportCredentials(insecure.NewCredentials()))
}

// WithLogger sets the logger. Default: slog.Default().
func WithLogger(l *slog.Logger) Option { return func(w *Worker) { w.log = l } }

// WithShutdownGrace sets how long Run waits for running handlers after its context is
// cancelled, before cancelling them. Default 30 seconds.
func WithShutdownGrace(d time.Duration) Option { return func(w *Worker) { w.grace = d } }

// RegisterOption configures one registered handler.
type RegisterOption func(*registration)

// InPool makes the handler serve the named pool instead of "default". The worker opens
// one session per pool its handlers serve.
func InPool(pool string) RegisterOption { return func(r *registration) { r.pool = pool } }

// WithTimeout bounds one execution of the handler. When it passes, the handler's
// context is cancelled and the task is reported as a retryable "timeout".
func WithTimeout(d time.Duration) RegisterOption { return func(r *registration) { r.timeout = d } }

type registration struct {
	name    string
	handler Handler
	pool    string
	timeout time.Duration
}

// Worker runs registered handlers against a Frontier.
type Worker struct {
	target    string
	id        string
	version   string
	maxTasks  int
	perSecond float64
	pace      *pacer // nil: no limit on how fast work is taken
	grace     time.Duration
	dial      []grpc.DialOption
	log       *slog.Logger

	mu       sync.Mutex
	handlers map[string]*registration // by handler name
	started  bool
}

// New creates a worker for the Frontier at target (a gRPC target such as "host:7070").
// It needs transport security options: WithInsecure, or WithDialOptions with
// credentials.
func New(target string, opts ...Option) (*Worker, error) {
	if target == "" {
		return nil, errors.New("worker: target is required")
	}
	host, _ := os.Hostname()
	w := &Worker{target: target, id: fmt.Sprintf("%s-%d", host, os.Getpid()), maxTasks: 8,
		grace: 30 * time.Second, log: slog.Default(), handlers: map[string]*registration{}}
	for _, o := range opts {
		o(w)
	}
	if w.maxTasks < 1 {
		return nil, errors.New("worker: MaxConcurrentTasks must be at least 1")
	}
	if w.perSecond < 0 || math.IsNaN(w.perSecond) || math.IsInf(w.perSecond, 0) {
		return nil, errors.New("worker: MaxTasksPerSecond must be a positive number, or zero for no limit")
	}
	if w.perSecond > 0 {
		w.pace = &pacer{every: time.Duration(float64(time.Second) / w.perSecond)}
	}
	if len(w.dial) == 0 {
		return nil, errors.New("worker: transport security is required: use WithDialOptions with credentials, or WithInsecure for plaintext")
	}
	return w, nil
}

// pacer spaces out the credits a worker grants: one every interval. A credit is one
// task the Frontier may send, so this bounds how fast the worker starts tasks. All
// pools of a worker share one pacer.
type pacer struct {
	every time.Duration

	mu   sync.Mutex
	next time.Time // when the next credit may be granted
}

// take reports how many of the want credits may be granted at now (none or one),
// and how long to wait before asking again when that is fewer than wanted.
func (p *pacer) take(want int, now time.Time) (granted int, wait time.Duration) {
	if p == nil {
		return want, 0
	}
	p.mu.Lock()
	defer p.mu.Unlock()
	if want > 0 && !now.Before(p.next) {
		granted, p.next = 1, now.Add(p.every)
	}
	return granted, p.next.Sub(now)
}

// Register adds a handler under the name tasks are routed by (the task's handler
// field). It must be called before Run.
func (w *Worker) Register(name string, h Handler, opts ...RegisterOption) error {
	if name == "" || h == nil {
		return errors.New("worker: a handler needs a name and an implementation")
	}
	r := &registration{name: name, handler: h, pool: DefaultPool}
	for _, o := range opts {
		o(r)
	}
	if !poolname.Valid(r.pool) {
		return fmt.Errorf("worker: pool %q does not match %s", r.pool, poolname.Pattern)
	}
	w.mu.Lock()
	defer w.mu.Unlock()
	if w.started {
		return errors.New("worker: Register called after Run")
	}
	if _, dup := w.handlers[name]; dup {
		return fmt.Errorf("worker: handler %q is already registered", name)
	}
	w.handlers[name] = r
	return nil
}

// Run connects to the Frontier and processes tasks until ctx is cancelled. It then
// stops taking work, lets running handlers finish (up to the shutdown grace), reports
// them and returns. Handlers still running when the grace passes are cancelled and
// their tasks handed back to the Frontier, which gives them to another worker without
// counting an attempt. Run waits a few more seconds for the cancelled handlers to
// return; a handler that ignores its context beyond that is abandoned, and logged, and
// its task waits for its lease to expire.
func (w *Worker) Run(ctx context.Context) error {
	w.mu.Lock()
	if w.started {
		w.mu.Unlock()
		return errors.New("worker: Run called twice")
	}
	w.started = true
	pools := map[string]bool{}
	for _, r := range w.handlers {
		pools[r.pool] = true
	}
	w.mu.Unlock()
	if len(pools) == 0 {
		return errors.New("worker: no handlers registered")
	}
	// Keepalive pings notice a Frontier that vanished without closing the connection.
	// The caller's options come last, so they can override it.
	dial := append([]grpc.DialOption{grpc.WithKeepaliveParams(keepalive.ClientParameters{
		Time: 30 * time.Second, Timeout: 10 * time.Second})}, w.dial...)
	conn, err := grpc.NewClient(w.target, dial...)
	if err != nil {
		return err
	}
	defer func() { _ = conn.Close() }() // nothing left to do with a close error at shutdown
	client := workerv1.NewWorkerServiceClient(conn)
	var wg sync.WaitGroup
	for pool := range pools {
		r := &runner{w: w, pool: pool, client: client, running: map[string]*active{}, idle: make(chan struct{}, 1)}
		wg.Go(func() { r.run(ctx) })
	}
	wg.Wait()
	return nil
}

// ---- one pool's session and tasks

// runner serves one pool: it keeps a session open, runs handlers and reports them.
// Tasks and their reports outlive a session: leases are not bound to it.
type runner struct {
	w      *Worker
	pool   string
	client workerv1.WorkerServiceClient

	sendMu sync.Mutex // serialises sends and the switch of stream
	stream grpc.BidiStreamingClient[workerv1.WorkRequest, workerv1.WorkResponse]
	gen    int         // session generation, to resend only what the current stream has not seen
	paced  *time.Timer // set while a credit waits for the pacer; guarded by sendMu

	mu       sync.Mutex
	running  map[string]*active // by lease id
	credits  int                // granted on the current stream and not used by an assignment yet
	unacked  []*report          // in the order the handlers finished
	draining bool
	cut      bool          // the shutdown grace passed: handlers were cancelled
	releases bool          // the Frontier of the current session takes a task back
	idle     chan struct{} // signalled when nothing runs and nothing is unacknowledged
	handlers sync.WaitGroup
}

type active struct {
	leaseID string
	attempt uint32
	cancel  context.CancelFunc

	// Guarded by runner.mu.
	revoked  bool
	deadline time.Time // when the lease expires, on this worker's clock
	beatSent time.Time // when the oldest unanswered extension was sent
	leaseFor time.Duration
}

// abandonAfter is how long Run waits for cancelled handlers to return.
const abandonAfter = 5 * time.Second

// report is everything owed to the Frontier for one finished task, in order: its
// discovered batches, then its completion.
type report struct {
	leaseID  string
	batches  []*workerv1.DiscoverBatch
	complete *workerv1.Complete
	sentGen  int
}

func (r *runner) run(ctx context.Context) {
	// Handlers and sessions outlive ctx by the shutdown grace. The session outlives
	// the handlers: what was cut is handed back before it closes.
	base, stop := context.WithCancel(context.WithoutCancel(ctx))
	defer stop()
	hctx, cut := context.WithCancel(base)
	go func() {
		<-ctx.Done()
		r.drain()
		select {
		case <-r.idle:
		case <-time.After(r.w.grace):
			r.w.log.Warn("shutdown grace passed with tasks still running: handing them back", "pool", r.pool)
			r.mu.Lock()
			r.cut = true
			r.mu.Unlock()
			cut()
			select {
			case <-r.idle:
			case <-time.After(abandonAfter):
			}
		}
		stop()
	}()
	// Run must not return while handlers still use what the caller owns.
	defer r.waitHandlers()
	wait := 200 * time.Millisecond
	for base.Err() == nil {
		started := time.Now()
		err := r.session(base, hctx)
		if base.Err() != nil {
			return
		}
		if time.Since(started) > 10*time.Second {
			wait = 200 * time.Millisecond
		}
		r.w.log.Warn("session ended, reconnecting", "pool", r.pool, "err", err, "in", wait)
		select {
		case <-base.Done():
			return
		case <-time.After(wait):
		}
		wait = min(wait*2, 10*time.Second)
	}
}

// waitHandlers waits for the handler goroutines, which have been cancelled by now.
func (r *runner) waitHandlers() {
	done := make(chan struct{})
	go func() {
		r.handlers.Wait()
		close(done)
	}()
	select {
	case <-done:
	case <-time.After(abandonAfter):
		r.w.log.Error("handlers ignored their cancelled context and were abandoned", "pool", r.pool)
	}
}

// drain stops taking work: the Frontier sends no more assignments after Goodbye. The
// flag is set under sendMu, so no credit can follow the Goodbye on the stream.
func (r *runner) drain() {
	r.sendMu.Lock()
	r.mu.Lock()
	r.draining = true
	r.mu.Unlock()
	if r.stream != nil {
		_ = r.stream.Send(&workerv1.WorkRequest{Msg: &workerv1.WorkRequest_Goodbye{Goodbye: &workerv1.Goodbye{}}})
	}
	r.sendMu.Unlock()
	r.checkIdle()
}

func (r *runner) checkIdle() {
	r.mu.Lock()
	idle := r.draining && len(r.running) == 0 && len(r.unacked) == 0
	r.mu.Unlock()
	if idle {
		select {
		case r.idle <- struct{}{}:
		default:
		}
	}
}

// session runs one stream until it breaks or the Frontier asks to reconnect. Handlers
// run under hctx, which ends before ctx at a shutdown.
func (r *runner) session(ctx, hctx context.Context) error {
	sctx, cancel := context.WithCancel(ctx)
	defer cancel()
	stream, err := r.client.Work(sctx)
	if err != nil {
		return err
	}
	if err := stream.Send(&workerv1.WorkRequest{Msg: &workerv1.WorkRequest_Hello{Hello: &workerv1.Hello{
		WorkerId: r.w.id, BuildVersion: r.w.version, Pool: r.pool}}}); err != nil {
		return err
	}
	// Switch to the new stream, then resend what the Frontier has not acknowledged, in
	// order: a task's discovered batches before its completion (ADR 0003).
	r.sendMu.Lock()
	r.stream = stream
	r.gen++
	r.mu.Lock()
	pending := append([]*report(nil), r.unacked...)
	r.credits = 0 // credits do not carry over to a new session
	r.mu.Unlock()
	for _, rep := range pending {
		r.sendReportLocked(rep)
	}
	r.grantLocked()
	r.sendMu.Unlock()
	defer func() {
		r.sendMu.Lock()
		if r.stream == stream {
			r.stream = nil
		}
		r.sendMu.Unlock()
	}()

	for {
		m, err := stream.Recv()
		if err != nil {
			return err
		}
		switch msg := m.GetMsg().(type) {
		case *workerv1.WorkResponse_Welcome:
			r.mu.Lock()
			r.releases = msg.Welcome.GetReleases()
			r.mu.Unlock()
			r.w.log.Info("session opened", "pool", r.pool, "session", msg.Welcome.GetSessionId())
		case *workerv1.WorkResponse_Assignment:
			r.start(hctx, msg.Assignment)
		case *workerv1.WorkResponse_Revoked:
			r.revoke(msg.Revoked.GetLeaseId(), msg.Revoked.GetReason().String())
		case *workerv1.WorkResponse_Extended:
			r.extended(msg.Extended.GetLeaseId())
		case *workerv1.WorkResponse_DiscoverAck:
			if st := msg.DiscoverAck.GetStatus(); st != workerv1.DiscoverAck_STATUS_ACCEPTED {
				r.w.log.Warn("discovered tasks were discarded: the lease was lost", "batch", msg.DiscoverAck.GetBatchId(), "status", st.String())
			}
			r.ackBatch(msg.DiscoverAck.GetBatchId())
		case *workerv1.WorkResponse_CompleteAck:
			if st := msg.CompleteAck.GetStatus(); st != workerv1.CompleteAck_STATUS_ACCEPTED {
				r.w.log.Warn("a result was discarded: the lease was lost", "lease", msg.CompleteAck.GetLeaseId(), "status", st.String())
			}
			r.ackComplete(msg.CompleteAck.GetLeaseId())
		case *workerv1.WorkResponse_Drain:
			return errors.New("the frontier asked to reconnect")
		}
	}
}

// grantLocked grants the Frontier a credit for every free slot that has none yet, as
// fast as the pacer allows, and comes back for the rest. The caller holds sendMu, so
// the count of free slots and the grant are one step: a session that reconnects in
// between cannot grant a slot a second time.
func (r *runner) grantLocked() {
	if r.stream == nil {
		return // the next session grants
	}
	r.mu.Lock()
	owed := r.w.maxTasks - len(r.running) - r.credits
	draining := r.draining
	r.mu.Unlock()
	if owed <= 0 || draining {
		return // no credit follows a Goodbye
	}
	n, wait := r.w.pace.take(owed, time.Now())
	if n > 0 {
		_ = r.stream.Send(&workerv1.WorkRequest{Msg: &workerv1.WorkRequest_Credit{Credit: &workerv1.Credit{Slots: safeconv.Uint32(n)}}})
		r.mu.Lock()
		r.credits += n
		r.mu.Unlock()
	}
	if n < owed && r.paced == nil {
		r.paced = time.AfterFunc(wait, func() {
			r.sendMu.Lock()
			r.paced = nil
			r.grantLocked()
			r.sendMu.Unlock()
		})
	}
}

// send writes one message to the current stream, if there is one.
func (r *runner) send(m *workerv1.WorkRequest) {
	r.sendMu.Lock()
	defer r.sendMu.Unlock()
	if r.stream != nil {
		_ = r.stream.Send(m) // a failed send ends the stream; the session loop sees it and reconnects
	}
}

// sendReportLocked writes a report to the current stream once. The caller holds sendMu.
func (r *runner) sendReportLocked(rep *report) {
	if r.stream == nil || rep.sentGen == r.gen {
		return
	}
	rep.sentGen = r.gen
	r.mu.Lock()
	batches := append([]*workerv1.DiscoverBatch(nil), rep.batches...)
	r.mu.Unlock()
	for _, b := range batches {
		if r.stream.Send(&workerv1.WorkRequest{Msg: &workerv1.WorkRequest_Discover{Discover: b}}) != nil {
			return
		}
	}
	_ = r.stream.Send(&workerv1.WorkRequest{Msg: &workerv1.WorkRequest_Complete{Complete: rep.complete}})
}

func (r *runner) ackBatch(id string) {
	r.mu.Lock()
	defer r.mu.Unlock()
	for _, rep := range r.unacked {
		if i := slices.IndexFunc(rep.batches, func(b *workerv1.DiscoverBatch) bool { return b.GetBatchId() == id }); i >= 0 {
			rep.batches = slices.Delete(rep.batches, i, i+1)
			return
		}
	}
}

func (r *runner) ackComplete(leaseID string) {
	r.mu.Lock()
	if i := slices.IndexFunc(r.unacked, func(rep *report) bool { return rep.leaseID == leaseID }); i >= 0 {
		r.unacked = slices.Delete(r.unacked, i, i+1)
	}
	r.mu.Unlock()
	r.checkIdle()
}

func (r *runner) revoke(leaseID, reason string) {
	r.mu.Lock()
	a := r.running[leaseID]
	if a != nil {
		a.revoked = true
	}
	r.mu.Unlock()
	if a != nil {
		r.w.log.Warn("lease revoked, cancelling the handler", "lease", leaseID, "reason", reason)
		a.cancel()
	}
}

// start runs the handler for an assignment in its own goroutine, with a heartbeat.
func (r *runner) start(ctx context.Context, a *workerv1.Assignment) {
	lease, pt := a.GetLease(), a.GetTask()
	task := &Task{ID: pt.GetTaskId(), RunID: pt.GetRunId(), QueueKey: pt.GetQueueKey(), Handler: pt.GetHandler(),
		ParentID: pt.GetParentTaskId(), Depth: pt.GetDepth(), Attempt: lease.GetAttempt(),
		URL: pt.GetRequest().GetUrl(), Method: pt.GetRequest().GetMethod(), Body: pt.GetRequest().GetBody(),
		Header: http.Header{}, Context: pt.GetContext(), FetchTimeout: pt.GetFetchTimeout().AsDuration()}
	if task.Method == "" {
		task.Method = http.MethodGet
	}
	for _, h := range pt.GetRequest().GetHeaders() {
		task.Header.Add(h.GetName(), h.GetValue())
	}
	r.w.mu.Lock()
	reg := r.w.handlers[task.Handler]
	r.w.mu.Unlock()

	hctx, cancel := context.WithCancel(ctx)
	act := &active{leaseID: lease.GetLeaseId(), attempt: lease.GetAttempt(), cancel: cancel,
		deadline: time.Now().Add(lease.GetDuration().AsDuration()), leaseFor: lease.GetDuration().AsDuration()}
	r.mu.Lock()
	r.running[act.leaseID] = act
	r.credits = max(r.credits-1, 0) // the assignment used one
	r.mu.Unlock()

	r.handlers.Go(func() {
		defer cancel()
		var res *Result
		var err error
		release := "" // the code of a hand-back, if the task goes back to the Frontier
		switch {
		case reg == nil || reg.pool != r.pool:
			// Another worker of the pool may have the handler, as during a rolling
			// deploy: the task goes back, at no cost to its attempts.
			release, err = codeNoHandler, fmt.Errorf("no handler %q registered for pool %q", task.Handler, r.pool)
		default:
			stopBeat := r.heartbeat(hctx, act, lease.GetDuration().AsDuration())
			res, err = r.call(hctx, reg, task)
			stopBeat()
		}
		// The slot is freed and its credit returned under sendMu, as one step: a
		// session that reconnects in between would count the slot as free and grant
		// it a second time.
		r.sendMu.Lock()
		r.mu.Lock()
		delete(r.running, act.leaseID)
		if r.cut && ctx.Err() != nil && cutShort(err) {
			// The shutdown cut the handler short: the task was not at fault.
			release = codeShutdown
		}
		reported := !act.revoked
		if release != "" && !r.releases {
			// A Frontier that does not know the hand-back would fail the task for
			// good. It gets what it got before: a failure for a missing handler,
			// and nothing for a cut, so that the lease expires and the task is retried.
			if release == codeShutdown {
				reported = false
			} else {
				err = Fail(release, err)
			}
			release = ""
		}
		var rep *report
		if reported {
			rep = buildReport(act, task, res, err, release, r.poolFor)
			r.unacked = append(r.unacked, rep)
		}
		r.mu.Unlock()
		// A lost lease is not reported: only its slot is returned.
		if rep != nil {
			r.sendReportLocked(rep)
		}
		r.grantLocked()
		r.sendMu.Unlock()
		r.checkIdle()
	})
}

// call runs the handler, turning a panic into a retryable failure.
func (r *runner) call(ctx context.Context, reg *registration, task *Task) (res *Result, err error) {
	if reg.timeout > 0 {
		var cancel context.CancelFunc
		ctx, cancel = context.WithTimeout(ctx, reg.timeout)
		defer cancel()
	}
	defer func() {
		if p := recover(); p != nil {
			r.w.log.Error("handler panicked", "handler", reg.name, "task", task.ID, "panic", p, "stack", string(debug.Stack()))
			res, err = nil, Retry("panic", fmt.Errorf("%v", p))
		}
	}()
	return reg.handler.Handle(ctx, task)
}

// extended records that the Frontier extended a lease: it now runs for its duration
// from when the extension was sent, which is the earliest the Frontier can have
// counted from.
func (r *runner) extended(leaseID string) {
	r.mu.Lock()
	defer r.mu.Unlock()
	if a := r.running[leaseID]; a != nil && !a.beatSent.IsZero() {
		a.deadline, a.beatSent = a.beatSent.Add(a.leaseFor), time.Time{}
	}
}

// heartbeat extends the lease at half of its duration while the handler runs, on the
// worker's own monotonic clock. When the lease runs out without an extension, e.g.
// because the Frontier cannot be reached, the handler is cancelled: another worker may
// already have the task.
func (r *runner) heartbeat(ctx context.Context, a *active, d time.Duration) (stop func()) {
	if d <= 0 {
		return func() {}
	}
	done := make(chan struct{})
	go func() {
		t := time.NewTicker(d / 2)
		defer t.Stop()
		for {
			select {
			case <-done:
				return
			case <-ctx.Done():
				return
			case now := <-t.C:
				r.mu.Lock()
				expired := now.After(a.deadline)
				if a.beatSent.IsZero() {
					a.beatSent = now
				}
				r.mu.Unlock()
				if expired {
					r.revoke(a.leaseID, "the lease ran out without an extension")
					return
				}
				r.send(&workerv1.WorkRequest{Msg: &workerv1.WorkRequest_Extend{Extend: &workerv1.ExtendLease{
					LeaseId: a.leaseID, Attempt: a.attempt, ExtendBy: durationpb.New(d)}}})
			}
		}
	}()
	return func() { close(done) }
}

// batchSize is how many discovered tasks go into one DiscoverBatch.
const batchSize = 200

// cutShort reports whether a handler's error says only that it was cancelled. An
// outcome the handler classified itself, such as a 404, stands even when it arrives
// at the moment of a shutdown.
func cutShort(err error) bool {
	if err == nil {
		return false
	}
	_, classified := errors.AsType[*Error](err)
	return !classified || errors.Is(err, context.Canceled)
}

// buildReport is what the worker owes the Frontier for a finished task. A release
// code hands the task back instead of reporting err as its outcome.
func buildReport(a *active, task *Task, res *Result, err error, release string, poolFor func(handler string) string) *report {
	if res == nil {
		res = &Result{}
	}
	out := &taskv1.Outcome{Kind: taskv1.Outcome_KIND_SUCCESS, Code: res.Code}
	if res.BlockQueueFor > 0 {
		out.BlockQueueFor = durationpb.New(res.BlockQueueFor)
	}
	if err != nil {
		e := classify(err)
		out.Kind, out.Code, out.Message = taskv1.Outcome_KIND_RETRY, e.Code, truncate(e.Error(), 500)
		if e.Kind == KindFail {
			out.Kind = taskv1.Outcome_KIND_FAILED
		}
		if release != "" {
			out.Kind, out.Code = taskv1.Outcome_KIND_RELEASED, release
		}
		if e.RetryAfter > 0 {
			out.RetryAfter = durationpb.New(e.RetryAfter)
		}
		if e.BlockQueueFor > 0 {
			out.BlockQueueFor = durationpb.New(e.BlockQueueFor)
		}
	}
	rep := &report{leaseID: a.leaseID, complete: &workerv1.Complete{LeaseId: a.leaseID, Attempt: a.attempt, Outcome: out,
		Fetch: &taskv1.FetchInfo{ItemsEmitted: safeconv.Uint32(res.Items)}}}
	if f := res.Fetch; f != nil {
		rep.complete.Fetch.HttpStatus, rep.complete.Fetch.FinalUrl = safeconv.Uint32(f.HTTPStatus), f.FinalURL
		rep.complete.Fetch.Latency, rep.complete.Fetch.Bytes = durationpb.New(f.Latency), uint64(max(f.Bytes, 0))
		rep.complete.Fetch.ResponseRef, rep.complete.Fetch.ContentHash = f.ResponseRef, f.ContentHash
	}
	if err != nil {
		return rep // discovered tasks are reported only for a task that succeeded
	}
	for i := 0; i < len(res.Tasks); i += batchSize {
		b := &workerv1.DiscoverBatch{LeaseId: a.leaseID, Attempt: a.attempt, BatchId: fmt.Sprintf("%s/%d", a.leaseID, i/batchSize)}
		chunk := res.Tasks[i:min(i+batchSize, len(res.Tasks))]
		for j := range chunk {
			b.Tasks = append(b.Tasks, taskSpec(task, chunk[j], poolFor))
		}
		rep.batches = append(rep.batches, b)
	}
	return rep
}

// poolFor is the pool of a discovered task that names none: the pool its handler is
// registered in on this worker, else the pool of the task that discovered it.
func (r *runner) poolFor(handler string) string {
	r.w.mu.Lock()
	defer r.w.mu.Unlock()
	if reg := r.w.handlers[handler]; reg != nil {
		return reg.pool
	}
	return r.pool
}

func taskSpec(parent *Task, nt NewTask, poolFor func(handler string) string) *taskv1.TaskSpec {
	ts := &taskv1.TaskSpec{Request: &taskv1.Request{Url: nt.URL, Method: nt.Method, Body: nt.Body},
		QueueKey: nt.QueueKey, DedupKey: nt.DedupKey, Handler: nt.Handler, Pool: nt.Pool, Priority: nt.Priority, Context: nt.Context}
	if ts.Handler == "" {
		ts.Handler = parent.Handler
	}
	if ts.Pool == "" {
		ts.Pool = poolFor(ts.Handler)
	}
	for k, vs := range nt.Header {
		for _, v := range vs {
			ts.Request.Headers = append(ts.Request.Headers, &taskv1.Header{Name: k, Value: v})
		}
	}
	if !nt.NotBefore.IsZero() {
		ts.NotBeforeTime = timestamppb.New(nt.NotBefore)
	}
	return ts
}

func truncate(s string, n int) string {
	if len(s) <= n {
		return s
	}
	return s[:n]
}

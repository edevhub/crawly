// Package frontier is the Frontier core (ADR 0001): the gRPC services, worker sessions,
// dispatch loops, outcome decisions and background loops. It keeps all state in a
// store.Store and can run as any number of replicas over the same store.
package frontier

import (
	"context"
	"crypto/sha256"
	"errors"
	"fmt"
	"log/slog"
	"maps"
	"math"
	"math/rand/v2"
	"slices"
	"strings"
	"sync"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/health"
	healthpb "google.golang.org/grpc/health/grpc_health_v1"
	"google.golang.org/grpc/keepalive"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/proto"

	policyv1 "github.com/edevhub/crawly/internal/gen/frontier/policy/v1"
	queuev1 "github.com/edevhub/crawly/internal/gen/frontier/queue/v1"
	runv1 "github.com/edevhub/crawly/internal/gen/frontier/run/v1"
	taskv1 "github.com/edevhub/crawly/internal/gen/frontier/task/v1"
	workerv1 "github.com/edevhub/crawly/internal/gen/frontier/worker/v1"
	"github.com/edevhub/crawly/internal/policy"
	"github.com/edevhub/crawly/internal/store"
	"github.com/edevhub/crawly/internal/wire"
)

// Config configures a Frontier replica.
type Config struct {
	Store    store.Store
	Policies *policy.Set
	// ReplicaID identifies this replica in the store. Empty: a random one.
	ReplicaID string
	Logger    *slog.Logger
	// Intervals of the background loops. Zero values use the defaults (one second).
	ReaperInterval   time.Duration
	MaintainInterval time.Duration
	PollInterval     time.Duration
	// NoHandlerGrace is how long a task is handed from worker to worker while none of
	// them has its handler, before it fails with code "no_handler". Zero: one hour.
	NoHandlerGrace time.Duration
	// NoHandlerDelay is how long such a task waits before it is given out again. Each
	// round takes a politeness slot of its queue, so the delay bounds what a pool
	// without the handler costs the queue's other tasks. Zero: five seconds.
	NoHandlerDelay time.Duration
}

// Server is one Frontier replica.
type Server struct {
	store store.Store
	pol   *policy.Set
	log   *slog.Logger
	cfg   Config

	mu        sync.Mutex
	pools     map[store.PoolName]*pool
	runSpecs  sync.Map // store.RunID -> store.RunSpec; a run's spec never changes
	leases    sync.Map // store.LeaseID -> store.LeaseInfo, for leases this replica dispatched
	events    signal   // broadcast: a run event was appended
	sessions  uint64
	loops     context.Context // set by Run: the lifetime of the dispatch loops
	draining  chan struct{}   // closed by Drain
	health    *health.Server
	unserved  sync.Map  // store.TaskID -> time.Time: since when no worker that got the task had its handler
	forgotten time.Time // guarded by mu: when forgetUnserved last looked at unserved
}

// New builds a replica. Call Register to expose it and Run to start its loops.
func New(cfg Config) (*Server, error) {
	if cfg.Store == nil || cfg.Policies == nil {
		return nil, errors.New("frontier: Store and Policies are required")
	}
	if cfg.Logger == nil {
		cfg.Logger = slog.Default()
	}
	if cfg.ReplicaID == "" {
		cfg.ReplicaID = fmt.Sprintf("replica-%08x", rand.Uint32()) //nolint:gosec // G404: a replica ID needs uniqueness, not unpredictability
	}
	for _, d := range []*time.Duration{&cfg.ReaperInterval, &cfg.MaintainInterval, &cfg.PollInterval} {
		if *d <= 0 {
			*d = time.Second
		}
	}
	if cfg.NoHandlerGrace <= 0 {
		cfg.NoHandlerGrace = time.Hour
	}
	if cfg.NoHandlerDelay <= 0 {
		cfg.NoHandlerDelay = 5 * time.Second
	}
	s := &Server{store: cfg.Store, pol: cfg.Policies, log: cfg.Logger, cfg: cfg,
		pools: map[store.PoolName]*pool{}, draining: make(chan struct{}), health: health.NewServer()}
	s.health.SetServingStatus("", healthpb.HealthCheckResponse_NOT_SERVING) // until Run
	return s, nil
}

// serving sets what the health service answers: whether this replica can take work
// now. It is not serving before its loops run, while the store cannot be reached, and
// from the moment it drains.
func (s *Server) serving(ok bool) {
	state := healthpb.HealthCheckResponse_NOT_SERVING
	select {
	case <-s.draining:
	default:
		if ok {
			state = healthpb.HealthCheckResponse_SERVING
		}
	}
	s.health.SetServingStatus("", state)
}

// Register exposes the four services and the standard gRPC health service on a gRPC
// server.
func (s *Server) Register(g grpc.ServiceRegistrar) {
	healthpb.RegisterHealthServer(g, s.health)
	workerv1.RegisterWorkerServiceServer(g, workerService{s: s})
	runv1.RegisterRunServiceServer(g, runService{s: s})
	policyv1.RegisterPolicyServiceServer(g, policyService{s: s})
	queuev1.RegisterQueueServiceServer(g, queueService{s: s})
}

// Run registers the replica with the store and runs its background loops until ctx is
// done: notifications, the reaper, maintenance and lease revocation.
func (s *Server) Run(ctx context.Context) error {
	if err := s.store.Start(ctx, store.Replica{ID: s.cfg.ReplicaID, PolicyHash: s.pol.Hash()}); err != nil {
		return err
	}
	notes, err := s.store.Notifications(ctx)
	if err != nil {
		return err
	}
	s.mu.Lock()
	s.loops = ctx
	s.mu.Unlock()
	s.serving(true)
	defer s.serving(false)
	var wg sync.WaitGroup
	for _, loop := range []func(context.Context){
		func(ctx context.Context) { s.notifications(ctx, notes) },
		s.reaper, s.maintain, s.revoker,
	} {
		wg.Go(func() { loop(ctx) })
	}
	wg.Wait()
	return nil
}

func (s *Server) notifications(ctx context.Context, notes <-chan store.Notification) {
	for {
		select {
		case <-ctx.Done():
			return
		case n := <-notes:
			if n.Runs {
				s.events.Broadcast()
			}
			if n.Pool != "" {
				s.mu.Lock()
				p := s.pools[n.Pool]
				s.mu.Unlock()
				if p != nil {
					p.wakeUp()
				}
			}
		}
	}
}

// reaper finishes expired leases as attempts with code lease_expired.
func (s *Server) reaper(ctx context.Context) {
	tick := time.NewTicker(s.cfg.ReaperInterval)
	defer tick.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-tick.C:
		}
		expired, err := s.store.ExpiredLeases(ctx, 200)
		if err != nil {
			s.log.Warn("reaper: listing expired leases", "err", err)
			continue
		}
		for i := range expired {
			info := &expired[i]
			f := s.decide(*info, &taskv1.Outcome{Kind: taskv1.Outcome_KIND_RETRY, Code: "lease_expired"}, nil)
			f.Source = store.SourceReaper
			done, err := s.store.Finish(ctx, f)
			switch {
			case err != nil:
				s.log.Warn("reaper: finishing lease", "lease", info.Ref.ID, "err", err)
			case done == store.FinishAccepted:
				s.unserved.Delete(info.Task) // a worker took the task and ran its handler
				// The worker that held the task is gone or stuck: worth a line.
				s.log.Warn("lease expired", "run", info.Run, "task", info.Task, "queue", info.Queue,
					"pool", info.Pool, "attempt", info.Ref.Attempt, "retry", f.Result == store.ResultRetry)
			}
			s.leases.Delete(info.Ref.ID)
		}
	}
}

func (s *Server) maintain(ctx context.Context) {
	tick := time.NewTicker(s.cfg.MaintainInterval)
	defer tick.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-tick.C:
			err := s.store.Maintain(ctx)
			s.forgetUnserved()
			if err != nil {
				s.log.Warn("maintain", "err", err)
			}
			// Maintain runs every tick, so it doubles as the probe of the store.
			s.serving(!errors.Is(err, store.ErrUnavailable))
		}
	}
}

// revoker pushes LeaseRevoked to sessions holding leases of runs that were cancelled
// or expired.
func (s *Server) revoker(ctx context.Context) {
	cursor, err := s.store.LastEventCursor(ctx)
	if err != nil {
		s.log.Warn("revoker: reading the event cursor", "err", err)
	}
	for {
		wait := s.events.Wait()
		evs, err := s.store.RunEvents(ctx, cursor, 500)
		if err != nil {
			s.log.Warn("revoker: reading events", "err", err)
		}
		for i := range evs {
			ev := &evs[i]
			cursor = ev.Cursor
			if ev.Type == store.EventCancelled || ev.Type == store.EventExpired {
				s.revokeRun(ev.Run.Spec.ID)
			}
		}
		if len(evs) > 0 {
			continue
		}
		select {
		case <-ctx.Done():
			return
		case <-wait:
		case <-time.After(s.cfg.PollInterval):
		}
	}
}

func (s *Server) revokeRun(run store.RunID) {
	type revocation struct {
		sess *session
		id   store.LeaseID
	}
	s.mu.Lock()
	pools := slices.Collect(maps.Values(s.pools))
	s.mu.Unlock()
	var revoked []revocation
	for _, p := range pools {
		p.mu.Lock()
		for sess := range p.sessions {
			for id, r := range sess.leases {
				if r == run {
					delete(sess.leases, id)
					revoked = append(revoked, revocation{sess: sess, id: id})
				}
			}
		}
		p.mu.Unlock()
	}
	// Sent without the pool lock: a slow session must not block the others.
	for _, r := range revoked {
		r.sess.send(&workerv1.WorkResponse{Msg: &workerv1.WorkResponse_Revoked{Revoked: &workerv1.LeaseRevoked{
			LeaseId: string(r.id), Reason: workerv1.LeaseRevoked_REASON_RUN_CANCELLED}}})
	}
	// The run's leases are gone from the store, whichever session holds them now.
	s.leases.Range(func(id, v any) bool {
		if info, ok := v.(store.LeaseInfo); ok && info.Run == run {
			s.leases.Delete(id)
		}
		return true
	})
}

// accepting returns the lifetime of the dispatch loops, or an error when the replica
// cannot take sessions: before Run, and after Drain.
func (s *Server) accepting() (context.Context, error) {
	s.mu.Lock()
	loops := s.loops
	s.mu.Unlock()
	select {
	case <-s.draining:
		return nil, status.Error(codes.Unavailable, "the frontier is shutting down")
	default:
	}
	if loops == nil {
		return nil, status.Error(codes.Unavailable, "the frontier is starting")
	}
	return loops, nil
}

// Drain prepares the replica for shutdown: it refuses new sessions, asks every worker
// to reconnect elsewhere and ends the watch streams. Leases are not bound to sessions,
// so the workers' running tasks carry on and report to another replica.
func (s *Server) Drain() {
	s.mu.Lock()
	select {
	case <-s.draining:
		s.mu.Unlock()
		return
	default:
		close(s.draining)
	}
	s.health.SetServingStatus("", healthpb.HealthCheckResponse_NOT_SERVING)
	pools := slices.Collect(maps.Values(s.pools))
	s.mu.Unlock()
	var sessions []*session
	for _, p := range pools {
		p.mu.Lock()
		for sess := range p.sessions {
			sess.credits, sess.goodbye = 0, true // no more work for this session
			sessions = append(sessions, sess)
		}
		p.mu.Unlock()
	}
	for _, sess := range sessions {
		sess.send(&workerv1.WorkResponse{Msg: &workerv1.WorkResponse_Drain{Drain: &workerv1.Drain{}}})
	}
}

// Shutdown drains the replica and stops the gRPC server: gracefully until ctx is done,
// then by force. Cancel Run's context after it returns.
func (s *Server) Shutdown(ctx context.Context, g *grpc.Server) {
	s.Drain()
	done := make(chan struct{})
	go func() {
		g.GracefulStop()
		close(done)
	}()
	select {
	case <-done:
	case <-ctx.Done():
		s.log.Warn("graceful stop timed out, closing the remaining streams")
		g.Stop()
		<-done
	}
}

// ServerOptions are the gRPC server options a Frontier should run with: keepalive
// pings, so that a worker that vanished without closing its connection is noticed.
func ServerOptions() []grpc.ServerOption {
	return []grpc.ServerOption{
		grpc.KeepaliveParams(keepalive.ServerParameters{Time: 30 * time.Second, Timeout: 10 * time.Second}),
		grpc.KeepaliveEnforcementPolicy(keepalive.EnforcementPolicy{MinTime: 10 * time.Second, PermitWithoutStream: true}),
	}
}

// CodeNoHandler is the code of a task no worker has a handler for.
const CodeNoHandler = "no_handler"

// CodeShutdown is the code of a task a worker handed back because it shut down.
const CodeShutdown = "shutdown"

// forgetUnserved drops the tasks that were handed back long ago and not since: they
// ended with their run, or their record was left behind by a store error. It looks at
// every record, so it runs a few times per grace period, not on every tick.
func (s *Server) forgetUnserved() {
	s.mu.Lock()
	due := time.Since(s.forgotten) >= s.cfg.NoHandlerGrace/4
	if due {
		s.forgotten = time.Now()
	}
	s.mu.Unlock()
	if !due {
		return
	}
	s.unserved.Range(func(k, v any) bool {
		if first, ok := v.(time.Time); ok && time.Since(first) > 2*s.cfg.NoHandlerGrace {
			s.unserved.Delete(k)
		}
		return true
	})
}

// served keeps the record of the tasks no worker has a handler for, once the store
// took the outcome. The record of a task starts with the first worker that lacked its
// handler and ends with the first that ran it, whatever the result: the grace counts
// the time nobody could run the task, not the time since a deploy began.
func (s *Server) served(info store.LeaseInfo, w store.WorkerInfo, o *taskv1.Outcome, f store.Finish) {
	if f.Result != store.ResultReleased || o.GetCode() != CodeNoHandler {
		s.unserved.Delete(info.Task)
		if o.GetKind() == taskv1.Outcome_KIND_RELEASED && o.GetCode() == CodeNoHandler {
			s.log.Warn("no worker had the task's handler for the grace period: the task failed",
				"task", info.Task, "run", info.Run, "pool", info.Pool, "grace", s.cfg.NoHandlerGrace)
		}
		return
	}
	if _, known := s.unserved.LoadOrStore(info.Task, time.Now()); !known {
		// Once per task: the sign of a rolling deploy, or of a handler nobody has.
		s.log.Info("a worker lacks the task's handler: the task is handed to the next worker",
			"task", info.Task, "run", info.Run, "pool", info.Pool, "worker", w.ID, "build", w.Version)
	}
}

// decide turns a worker's outcome into a transition (ADR 0001, Responsibilities): the
// spider classifies, the Frontier applies max_attempts and backoff.
func (s *Server) decide(info store.LeaseInfo, o *taskv1.Outcome, fetch *taskv1.FetchInfo) store.Finish {
	res := s.pol.Resolve(info.RunPolicy, string(info.Queue))
	beh := res.Behaviour
	// A sign of strain slows the queue (the store does it, with the outcome). The task
	// has a budget of its own for such failures: they say little about it while the
	// queue is slowed down, and max_attempts is judged against the other attempts.
	strain := o.GetStrain() && o.GetKind() != taskv1.Outcome_KIND_RELEASED && res.Queue.GetAdaptive().GetEnabled()
	// One outcome blocks the queue for as long as the queue's policy allows. A block
	// stops every run on the queue, so a site's Retry-After, or a spider's mistake, must
	// not stop it for good; a site that is still refusing when the block ends blocks the
	// queue again. The ceiling is the queue's: Resolve keeps a run's policy out of it.
	f := store.Finish{Lease: info.Ref, Source: store.SourceWorker, Strain: strain, Block: min(o.GetBlockQueueFor().AsDuration(), beh.GetMaxQueueBlock().AsDuration()),
		Record: store.AttemptRecord{Kind: int32(o.GetKind()), Code: o.GetCode(), Message: o.GetMessage(), PolicySet: s.pol.Hash(),
			HTTPStatus: fetch.GetHttpStatus(), FinalURL: fetch.GetFinalUrl(), Latency: fetch.GetLatency().AsDuration(),
			Bytes: fetch.GetBytes(), ResponseRef: fetch.GetResponseRef(), ContentHash: fetch.GetContentHash(), ItemsEmitted: fetch.GetItemsEmitted()}}
	switch o.GetKind() {
	case taskv1.Outcome_KIND_SUCCESS:
		f.Result = store.ResultSucceeded
		f.Answered = !strain && f.Block == 0
	case taskv1.Outcome_KIND_RELEASED:
		// The task was not run: nothing of the outcome but the hand-back applies.
		f.Block = 0
		switch o.GetCode() {
		case CodeShutdown:
			f.Result = store.ResultReleased
		case CodeNoHandler:
			// A worker of an older build shares a pool with the build that knows the
			// handler, as in a rolling deploy. The task goes round until a worker has
			// its handler, and fails only when none turned up for long enough.
			f.Result, f.RetryIn = store.ResultReleased, s.cfg.NoHandlerDelay
			if since, ok := s.unserved.Load(info.Task); ok {
				if first, ok := since.(time.Time); ok && time.Since(first) >= s.cfg.NoHandlerGrace {
					f.Result, f.RetryIn, f.Record.Kind = store.ResultFailed, 0, int32(taskv1.Outcome_KIND_FAILED)
				}
			}
		default:
			// Only the SDK's own reasons hand a task back. Anything else could keep
			// a task going round for ever, at no cost to its attempts.
			f.Result, f.Record.Kind = store.ResultFailed, int32(taskv1.Outcome_KIND_FAILED)
		}
	case taskv1.Outcome_KIND_RETRY:
		limit, attempt := beh.GetMaxAttempts(), info.Ref.Attempt
		if strain {
			// Two budgets: the strain attempts (this one included) against
			// max_strain_attempts, the others against max_attempts. What max_attempts
			// alone would not allow is granted by the strain budget, and the store ends
			// it when the queue has been at its ceiling for too long.
			if limit > 0 && info.StrainAttempts+1 >= max(limit, beh.GetMaxStrainAttempts()) {
				f.Result = store.ResultFailed
				break
			}
			f.Extended = limit > 0 && attempt >= limit
		} else {
			f.Answered = f.Block == 0 && fetch.GetHttpStatus() > 0
			if limit > 0 && attempt-min(info.StrainAttempts, attempt) >= limit {
				f.Result = store.ResultFailed
				break
			}
		}
		f.Result = store.ResultRetry
		if o.GetRetryAfter() != nil {
			f.RetryIn = o.GetRetryAfter().AsDuration()
		} else {
			f.RetryIn = backoff(beh.GetBackoff(), info.Ref.Attempt)
		}
	default:
		f.Result = store.ResultFailed
		f.Answered = f.Block == 0 && fetch.GetHttpStatus() > 0
	}
	return f
}

// backoff is initial × multiplier^(attempt−1), capped, with ± jitter.
func backoff(b *policyv1.Backoff, attempt uint32) time.Duration {
	d := float64(b.GetInitial().AsDuration())
	if m := b.GetMultiplier(); m > 1 && attempt > 1 {
		d *= math.Pow(m, float64(attempt-1))
	}
	if ceiling := float64(b.GetMax().AsDuration()); ceiling > 0 && d > ceiling {
		d = ceiling
	}
	if j := b.GetJitter(); j > 0 {
		d *= 1 + j*(2*rand.Float64()-1) //nolint:gosec // G404: jitter needs no cryptographic randomness
	}
	return time.Duration(d)
}

// runSpec returns a run's immutable spec, cached after the first read.
func (s *Server) runSpec(ctx context.Context, id store.RunID) (store.RunSpec, error) {
	if v, ok := s.runSpecs.Load(id); ok {
		if spec, ok := v.(store.RunSpec); ok {
			return spec, nil
		}
	}
	r, err := s.store.GetRun(ctx, id)
	if err != nil {
		return store.RunSpec{}, err
	}
	s.runSpecs.Store(id, r.Spec)
	return r.Spec, nil
}

// newTasks turns submitted specs into store tasks: pool, queue key, fingerprint, TTL.
// A spec that cannot be accepted becomes an Invalid entry, so results stay in order.
func (s *Server) newTasks(spec store.RunSpec, specs []*taskv1.TaskSpec) []store.NewTask {
	out := make([]store.NewTask, len(specs))
	for i, ts := range specs {
		pool, queue, fp, err := identify(spec.KeyRule, ts)
		if err != nil || proto.Size(ts) > wire.MaxTaskBytes {
			out[i].Invalid = true
			continue
		}
		stored := proto.CloneOf(ts)
		stored.QueueKey, stored.Pool = queue, string(pool)
		if stored.GetRequest().GetMethod() != "" {
			// Methods are case-sensitive on the wire. One spelling is stored and sent,
			// the one the dedup key counts, so that "post" and "POST" are one request.
			stored.Request.Method = strings.ToUpper(stored.GetRequest().GetMethod())
		}
		raw, err := proto.Marshal(stored)
		if err != nil {
			out[i].Invalid = true
			continue
		}
		out[i] = store.NewTask{Fingerprint: fp, Queue: store.QueueKey(queue), Pool: pool, Priority: ts.GetPriority(), Spec: raw,
			TTL: s.pol.Resolve(spec.Policy, queue).Behaviour.GetTaskTtl().AsDuration()}
		if ts.GetNotBeforeTime() != nil {
			out[i].NotBefore = ts.GetNotBeforeTime().AsTime()
		}
	}
	return out
}

func specHash(req *runv1.CreateRunRequest) []byte {
	b, _ := proto.MarshalOptions{Deterministic: true}.Marshal(req)
	sum := sha256.Sum256(b)
	return sum[:]
}

// grpcErr maps store errors to gRPC status codes.
func grpcErr(err error) error {
	switch {
	case err == nil:
		return nil
	case errors.Is(err, store.ErrNotFound):
		return status.Error(codes.NotFound, err.Error())
	case errors.Is(err, store.ErrConflict):
		return status.Error(codes.AlreadyExists, err.Error())
	case errors.Is(err, store.ErrRunClosed):
		return status.Error(codes.FailedPrecondition, err.Error())
	case errors.Is(err, store.ErrUnavailable):
		return status.Error(codes.Unavailable, err.Error())
	case errors.Is(err, context.Canceled):
		return status.Error(codes.Canceled, err.Error())
	}
	if _, ok := status.FromError(err); ok {
		return err
	}
	return status.Error(codes.Internal, err.Error())
}

// signal is a broadcast: Wait returns a channel closed by the next Broadcast.
type signal struct {
	mu sync.Mutex
	ch chan struct{}
}

func (s *signal) Wait() <-chan struct{} {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.ch == nil {
		s.ch = make(chan struct{})
	}
	return s.ch
}

func (s *signal) Broadcast() {
	s.mu.Lock()
	if s.ch != nil {
		close(s.ch)
		s.ch = nil
	}
	s.mu.Unlock()
}

package frontier

import (
	"context"
	"errors"
	"fmt"
	"io"
	"maps"
	"slices"
	"strings"
	"sync"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/durationpb"
	"google.golang.org/protobuf/types/known/timestamppb"

	taskv1 "github.com/edevhub/crawly/internal/gen/frontier/task/v1"
	workerv1 "github.com/edevhub/crawly/internal/gen/frontier/worker/v1"
	"github.com/edevhub/crawly/internal/poolname"
	"github.com/edevhub/crawly/internal/store"
	"github.com/edevhub/crawly/internal/wire"
)

// maxCredits bounds what one session can hold, against a misbehaving worker.
const maxCredits = 10_000

type workerService struct {
	workerv1.UnimplementedWorkerServiceServer
	s *Server
}

// pool is the set of sessions serving one pool on this replica, with one dispatch loop
// for all of them (ADR 0003).
type pool struct {
	name     store.PoolName
	mu       sync.Mutex
	sessions map[*session]struct{}
	served   uint64 // guarded by mu: counts the dispatches that handed out work
	wake     chan struct{}
}

func (p *pool) wakeUp() {
	select {
	case p.wake <- struct{}{}:
	default:
	}
}

type session struct {
	id       store.SessionID
	worker   store.WorkerInfo
	handlers []string // what the worker declared in Hello; none: it is offered every task
	sig      string   // the handlers as one string: sessions with the same one are given the same work
	ctx      context.Context
	cancel   context.CancelFunc
	out      chan *workerv1.WorkResponse

	credits int                           // guarded by pool.mu
	served  uint64                        // guarded by pool.mu: pool.served when it last got work; 0: never
	goodbye bool                          // guarded by pool.mu: the worker takes no more work
	leases  map[store.LeaseID]store.RunID // guarded by pool.mu

	mu          sync.Mutex
	undelivered map[store.LeaseID]store.LeaseRef // dispatched, not yet written to the stream
}

// sendTimeout is how long a message may wait for room in a session's queue. A session
// that cannot take a message for this long is closed: its worker is gone or stuck, and
// it must not hold up the pool's dispatch loop.
const sendTimeout = 5 * time.Second

// send queues a message for the session's stream. It gives up when the session ends,
// and ends the session when its queue stays full.
func (ss *session) send(m *workerv1.WorkResponse) {
	select {
	case ss.out <- m:
		return
	case <-ss.ctx.Done():
		return
	default:
	}
	t := time.NewTimer(sendTimeout)
	defer t.Stop()
	select {
	case ss.out <- m:
	case <-ss.ctx.Done():
	case <-t.C:
		ss.cancel()
	}
}

// takeUndelivered returns the leases that were dispatched to the session and never
// written to its stream, and forgets them.
func (ss *session) takeUndelivered() []store.LeaseRef {
	ss.mu.Lock()
	defer ss.mu.Unlock()
	refs := slices.Collect(maps.Values(ss.undelivered))
	clear(ss.undelivered)
	return refs
}

func (ss *session) delivered(id store.LeaseID) {
	ss.mu.Lock()
	delete(ss.undelivered, id)
	ss.mu.Unlock()
}

// pool returns the pool's sessions and dispatch loop, creating them on first use.
func (s *Server) pool(ctx context.Context, name store.PoolName) *pool {
	s.mu.Lock()
	defer s.mu.Unlock()
	p, ok := s.pools[name]
	if !ok {
		p = &pool{name: name, sessions: map[*session]struct{}{}, wake: make(chan struct{}, 1)}
		s.pools[name] = p
		go s.dispatchLoop(ctx, p)
	}
	return p
}

// release returns assignments that never reached a worker to their backlogs, so that a
// dropped session costs the tasks neither an attempt nor a lease duration of waiting.
func (s *Server) release(ctx context.Context, sess *session) {
	refs := sess.takeUndelivered()
	if len(refs) == 0 {
		return
	}
	// The session's context is gone; the release must still reach the store.
	ctx, cancel := context.WithTimeout(context.WithoutCancel(ctx), sendTimeout)
	defer cancel()
	for _, ref := range refs {
		s.leases.Delete(ref.ID)
		st, err := s.store.Finish(ctx, store.Finish{Lease: ref, Source: store.SourceWorker, Result: store.ResultReleased})
		if err != nil || st != store.FinishAccepted {
			// The lease expires on its own and the reaper retries the task.
			s.log.Warn("releasing an undelivered assignment", "lease", ref.ID, "status", st, "err", err)
		}
	}
	s.log.Info("released undelivered assignments", "session", sess.id, "count", len(refs))
}

// received is one inbound message of a session, or the error that ended its stream.
type received struct {
	msg *workerv1.WorkRequest
	err error
}

// Work is one worker session. Inbound messages are handled in stream order, so a
// DiscoverBatch is committed before a later Complete of the same session is handled.
func (w workerService) Work(stream grpc.BidiStreamingServer[workerv1.WorkRequest, workerv1.WorkResponse]) error {
	s := w.s
	loops, err := s.accepting()
	if err != nil {
		return err
	}
	first, err := stream.Recv()
	if err != nil {
		return err
	}
	hello := first.GetHello()
	if hello == nil {
		return status.Error(codes.InvalidArgument, "the first message of a session must be Hello")
	}
	if !poolname.Valid(hello.GetPool()) {
		return status.Errorf(codes.InvalidArgument, "pool %q does not match %s", hello.GetPool(), poolname.Pattern)
	}
	handlers, err := sessionHandlers(hello.GetHandlers())
	if err != nil {
		return err
	}
	ctx, cancel := context.WithCancel(stream.Context())
	defer cancel()
	s.mu.Lock()
	s.sessions++
	id := store.SessionID(fmt.Sprintf("%s/%d", s.cfg.ReplicaID, s.sessions))
	s.mu.Unlock()
	sess := &session{id: id, ctx: ctx, cancel: cancel, out: make(chan *workerv1.WorkResponse, 256),
		worker: store.WorkerInfo{ID: hello.GetWorkerId(), Version: hello.GetBuildVersion()}, handlers: handlers, sig: strings.Join(handlers, "\x00"),
		leases: map[store.LeaseID]store.RunID{}, undelivered: map[store.LeaseID]store.LeaseRef{}}
	p := s.pool(loops, store.PoolName(hello.GetPool()))
	p.mu.Lock()
	p.sessions[sess] = struct{}{}
	p.mu.Unlock()

	sendErr := make(chan error, 1)
	sent := make(chan struct{})
	go func() {
		defer close(sent)
		for {
			select {
			case <-ctx.Done():
				return
			case m := <-sess.out:
				if err := stream.Send(m); err != nil {
					sendErr <- err
					cancel()
					return
				}
				if a := m.GetAssignment(); a != nil {
					sess.delivered(store.LeaseID(a.GetLease().GetLeaseId()))
				}
			}
		}
	}()
	defer func() {
		p.mu.Lock()
		delete(p.sessions, sess)
		p.mu.Unlock()
		cancel()
		// Wait for the sender, so that what it wrote is not released. A sender stuck
		// on a dead connection is not waited for: it ends when this handler returns.
		select {
		case <-sent:
		case <-time.After(time.Second):
		}
		s.release(ctx, sess)
	}()

	// Recv is read in its own goroutine: the session must be able to end while the
	// worker is silent, e.g. when its queue overflows or the replica drains.
	in := make(chan received)
	go func() {
		for {
			m, err := stream.Recv()
			select {
			case in <- received{msg: m, err: err}:
			case <-ctx.Done():
				return
			}
			if err != nil {
				return
			}
		}
	}()

	now, err := s.store.Now(ctx)
	if err != nil {
		return grpcErr(err)
	}
	sess.send(&workerv1.WorkResponse{Msg: &workerv1.WorkResponse_Welcome{Welcome: &workerv1.Welcome{
		SessionId: string(id), ServerTime: timestamppb.New(now), Releases: true}}})
	s.log.Info("session opened", "session", id, "pool", p.name, "worker", sess.worker.ID, "build", sess.worker.Version, "handlers", handlers)
	defer s.log.Info("session closed", "session", id)

	ended := func() error {
		select {
		case e := <-sendErr:
			return e
		default:
		}
		if stream.Context().Err() == nil {
			// The Frontier ended the session, not the worker.
			return status.Error(codes.Unavailable, "the session was closed by the frontier")
		}
		return nil
	}
	for {
		var m *workerv1.WorkRequest
		select {
		case <-ctx.Done():
			return ended()
		case r := <-in:
			if r.err != nil {
				if errors.Is(r.err, io.EOF) || ctx.Err() != nil {
					return ended()
				}
				return r.err
			}
			m = r.msg
		}
		switch msg := m.GetMsg().(type) {
		case *workerv1.WorkRequest_Credit:
			p.mu.Lock()
			if !sess.goodbye { // a credit that crossed a Goodbye grants nothing
				sess.credits = min(sess.credits+int(msg.Credit.GetSlots()), maxCredits)
			}
			p.mu.Unlock()
			p.wakeUp()
		case *workerv1.WorkRequest_Goodbye:
			p.mu.Lock()
			sess.credits, sess.goodbye = 0, true
			p.mu.Unlock()
		case *workerv1.WorkRequest_Extend:
			err = s.handleExtend(ctx, p, sess, msg.Extend)
		case *workerv1.WorkRequest_Discover:
			err = s.handleDiscover(ctx, sess, msg.Discover)
		case *workerv1.WorkRequest_Complete:
			err = s.handleComplete(ctx, p, sess, msg.Complete)
		case *workerv1.WorkRequest_Hello:
			return status.Error(codes.InvalidArgument, "Hello sent twice")
		}
		if err != nil {
			// The store could not take the message: close the stream. The SDK
			// reconnects and resends what was not acknowledged, in order.
			return status.Error(codes.Unavailable, err.Error())
		}
	}
}

// sessionHandlers validates the handlers a worker declared and returns them sorted and
// without repeats. None is allowed: the session is then offered every task of its pool.
func sessionHandlers(declared []string) ([]string, error) {
	if len(declared) > wire.MaxSessionHandlers {
		return nil, status.Errorf(codes.InvalidArgument, "a session can name at most %d handlers", wire.MaxSessionHandlers)
	}
	for _, h := range declared {
		if h == "" || !wire.ValidHandler(h) {
			return nil, status.Errorf(codes.InvalidArgument, "handler name %.32q is empty, longer than %d bytes or holds a NUL byte", h, wire.MaxHandlerBytes)
		}
	}
	return slices.Compact(slices.Sorted(slices.Values(declared))), nil
}

// leaseInfo returns the context of a lease: from this replica's own dispatches, or
// from the store when another replica dispatched it.
func (s *Server) leaseInfo(ctx context.Context, ref store.LeaseRef) (store.LeaseInfo, bool, error) {
	if v, ok := s.leases.Load(ref.ID); ok {
		if info, ok := v.(store.LeaseInfo); ok && info.Ref == ref {
			return info, true, nil
		}
	}
	info, err := s.store.Lease(ctx, ref)
	if errors.Is(err, store.ErrNotFound) {
		return store.LeaseInfo{}, false, nil
	}
	return info, err == nil, err
}

func (s *Server) handleExtend(ctx context.Context, p *pool, sess *session, m *workerv1.ExtendLease) error {
	id := store.LeaseID(m.GetLeaseId())
	ref := store.LeaseRef{ID: id, Attempt: m.GetAttempt()}
	lease, st, err := s.store.ExtendLease(ctx, ref, m.GetExtendBy().AsDuration())
	if err != nil {
		return err
	}
	if st != store.FinishAccepted {
		p.mu.Lock()
		delete(sess.leases, id)
		p.mu.Unlock()
		s.leases.Delete(id)
		reason := workerv1.LeaseRevoked_REASON_FENCED
		if st == store.FinishUnknown {
			// The task is gone: finished, cancelled with its run, or expired.
			reason = workerv1.LeaseRevoked_REASON_EXPIRED
		}
		sess.send(&workerv1.WorkResponse{Msg: &workerv1.WorkResponse_Revoked{Revoked: &workerv1.LeaseRevoked{
			LeaseId: m.GetLeaseId(), Reason: reason}}})
		return nil
	}
	// A lease heartbeated here belongs to this session now, wherever it was
	// dispatched: a revocation of its run must reach this stream.
	if v, ok := s.leases.Load(id); ok {
		if info, ok := v.(store.LeaseInfo); ok {
			p.mu.Lock()
			sess.leases[id] = info.Run
			p.mu.Unlock()
		}
	}
	sess.send(&workerv1.WorkResponse{Msg: &workerv1.WorkResponse_Extended{Extended: &workerv1.LeaseExtended{
		LeaseId: m.GetLeaseId(), ExpireTime: timestamppb.New(lease.ExpiresAt)}}})
	return nil
}

func (s *Server) handleDiscover(ctx context.Context, sess *session, m *workerv1.DiscoverBatch) error {
	ack := &workerv1.DiscoverAck{BatchId: m.GetBatchId()}
	reply := func(st workerv1.DiscoverAck_Status) error {
		ack.Status = st
		sess.send(&workerv1.WorkResponse{Msg: &workerv1.WorkResponse_DiscoverAck{DiscoverAck: ack}})
		return nil
	}
	ref := store.LeaseRef{ID: store.LeaseID(m.GetLeaseId()), Attempt: m.GetAttempt()}
	info, ok, err := s.leaseInfo(ctx, ref)
	if err != nil {
		return err
	}
	if !ok {
		return reply(workerv1.DiscoverAck_STATUS_STALE)
	}
	spec, err := s.runSpec(ctx, info.Run)
	if err != nil {
		return err
	}
	res, err := s.store.AddTasks(ctx, store.AddRequest{Run: info.Run, Parent: &ref, Tasks: s.newTasks(spec, m.GetTasks(), info.Label),
		MaxLabels: s.cfg.MaxLabelsPerRun})
	switch {
	case errors.Is(err, store.ErrLeaseStale):
		return reply(workerv1.DiscoverAck_STATUS_STALE)
	case errors.Is(err, store.ErrLeaseUnknown), errors.Is(err, store.ErrNotFound):
		return reply(workerv1.DiscoverAck_STATUS_UNKNOWN)
	case err != nil:
		return err
	}
	ack.Results = addResults(res)
	return reply(workerv1.DiscoverAck_STATUS_ACCEPTED)
}

func (s *Server) handleComplete(ctx context.Context, p *pool, sess *session, m *workerv1.Complete) error {
	ref := store.LeaseRef{ID: store.LeaseID(m.GetLeaseId()), Attempt: m.GetAttempt()}
	info, ok, err := s.leaseInfo(ctx, ref)
	if err != nil {
		return err
	}
	st := store.FinishStale
	if ok {
		f := s.decide(info, m.GetOutcome(), m.GetFetch())
		if st, err = s.store.Finish(ctx, f); err != nil {
			return err
		}
		if st == store.FinishAccepted {
			s.served(info, sess.worker, m.GetOutcome(), f)
		}
	}
	s.leases.Delete(ref.ID)
	p.mu.Lock()
	delete(sess.leases, ref.ID)
	p.mu.Unlock()
	ack := &workerv1.CompleteAck{LeaseId: m.GetLeaseId(), Status: workerv1.CompleteAck_STATUS_ACCEPTED}
	switch st {
	case store.FinishAccepted:
		// The ack already says accepted.
	case store.FinishStale:
		ack.Status = workerv1.CompleteAck_STATUS_STALE
	case store.FinishUnknown:
		ack.Status = workerv1.CompleteAck_STATUS_UNKNOWN
	}
	sess.send(&workerv1.WorkResponse{Msg: &workerv1.WorkResponse_CompleteAck{CompleteAck: ack}})
	return nil
}

// dispatchLoop is the one dispatch loop of a pool on this replica. It asks the store
// for work when a notification or a credit arrives, at the store's NextReadyAt, and at
// the fallback poll interval.
func (s *Server) dispatchLoop(ctx context.Context, p *pool) {
	timer := time.NewTimer(s.cfg.PollInterval)
	defer timer.Stop()
	for {
		wait := s.cfg.PollInterval
		if next := s.dispatchRound(ctx, p); !next.IsZero() {
			now, err := s.store.Now(ctx)
			if err == nil {
				if d := next.Sub(now); d < wait {
					wait = max(d, time.Millisecond)
				}
			}
		}
		timer.Reset(wait)
		select {
		case <-ctx.Done():
			return
		case <-p.wake:
		case <-timer.C:
		}
	}
}

// dispatchRound hands out work until the store has none ready for any session that has
// a free credit. It returns when the store expects more work, if it said.
func (s *Server) dispatchRound(ctx context.Context, p *pool) time.Time {
	var next time.Time
	idle := map[string]bool{} // handler sets the store had nothing for in this round
	for ctx.Err() == nil {
		p.mu.Lock()
		// The session that has waited longest for work goes first, so that work is
		// shared between the workers of a pool whatever their free credits are.
		var sess *session
		for c := range p.sessions {
			if c.credits > 0 && c.ctx.Err() == nil && !idle[c.sig] && (sess == nil || c.served < sess.served) {
				sess = c
			}
		}
		var slots int
		if sess != nil {
			slots = sess.credits
		}
		p.mu.Unlock()
		if sess == nil {
			return next // a Credit wakes the loop
		}
		res, err := s.store.Dispatch(ctx, store.DispatchRequest{Session: sess.id, Worker: sess.worker, Pool: p.name, Handlers: sess.handlers, Slots: slots})
		if len(res.Assignments) > 0 {
			p.mu.Lock()
			p.served++
			sess.served = p.served
			p.mu.Unlock()
		}
		// Assignments returned are committed and must be delivered, error or not.
		for i := range res.Assignments {
			a := &res.Assignments[i]
			s.leases.Store(a.Lease.Ref.ID, a.Info)
			p.mu.Lock()
			sess.credits = max(sess.credits-1, 0)
			sess.leases[a.Lease.Ref.ID] = a.Info.Run
			p.mu.Unlock()
			sess.mu.Lock()
			sess.undelivered[a.Lease.Ref.ID] = a.Lease.Ref
			sess.mu.Unlock()
			sess.send(&workerv1.WorkResponse{Msg: &workerv1.WorkResponse_Assignment{Assignment: s.assignment(*a)}})
		}
		if sess.ctx.Err() != nil {
			// The session ended while its work was being claimed, possibly after its
			// own clean-up ran: release what it could not take.
			s.release(ctx, sess)
		}
		if err != nil {
			s.log.Warn("dispatch", "pool", p.name, "err", err)
			return time.Time{}
		}
		if len(res.Assignments) == 0 {
			// Sessions with other handlers may still be served.
			idle[sess.sig] = true
			if at := res.NextReadyAt; !at.IsZero() && (next.IsZero() || at.Before(next)) {
				next = at
			}
		}
	}
	return time.Time{}
}

func (s *Server) assignment(a store.Assignment) *workerv1.Assignment {
	ts := &taskv1.TaskSpec{}
	_ = proto.Unmarshal(a.Spec, ts)
	beh := s.pol.Resolve(a.Info.RunPolicy, string(a.Info.Queue)).Behaviour
	t := taskFromSpec(a.Info.Task, a.Info.Run, a.Info.Fingerprint, a.Info.Depth, a.Info.Parent, ts)
	t.FetchTimeout = beh.GetFetchTimeout()
	t.Label = a.Info.Label // the stored column, which is what the statistics count
	return &workerv1.Assignment{
		Lease: &workerv1.Lease{LeaseId: string(a.Lease.Ref.ID), Attempt: a.Lease.Ref.Attempt,
			ExpireTime: timestamppb.New(a.Lease.ExpiresAt), Duration: durationpb.New(a.Lease.Duration)},
		Task: t,
	}
}

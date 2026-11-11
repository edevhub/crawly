// Package memstore is an in-memory store.Store. It follows the scheduling model of
// ADR 0001 and passes storetest. It keeps everything in one process under one lock, so
// it suits tests, development and single-replica deployments that can lose their state
// on restart.
package memstore

import (
	"bytes"
	"cmp"
	"context"
	"crypto/rand"
	"encoding/hex"
	"fmt"
	"maps"
	"slices"
	"strconv"
	"strings"
	"sync"
	"time"

	taskv1 "github.com/edevhub/crawly/internal/gen/frontier/task/v1"
	"github.com/edevhub/crawly/internal/safeconv"
	"github.com/edevhub/crawly/internal/sched"
	"github.com/edevhub/crawly/internal/store"
)

// Store implements store.Store in memory.
type Store struct {
	mu     sync.Mutex
	now    func() time.Time
	pol    store.Policies
	runs   map[store.RunID]*run
	order  []*run
	queues map[store.QueueKey]*queue
	tasks  map[store.TaskID]*task
	ttl    map[*task]struct{} // pending tasks with an expiry
	leased map[*task]struct{} // tasks in flight
	events []store.RunEvent
	subs   map[chan store.Notification]struct{}
	seq    uint64
	served uint64
	keep   time.Duration // how long ended runs are kept; 0: for ever
	// Handlers that have a session (ServeHandlers), when each was last reported, and
	// when this store started (SweepUnserved).
	handlers map[store.HandlerRef]time.Time
	started  time.Time
}

// Option configures a Store.
type Option func(*Store)

// WithClock replaces the store clock, for tests.
func WithClock(now func() time.Time) Option { return func(s *Store) { s.now = now } }

// WithRetention makes Maintain delete ended runs, with their tasks, once they have
// been ended for this long. The default keeps them for ever.
func WithRetention(runs time.Duration) Option { return func(s *Store) { s.keep = runs } }

// New creates an empty store that resolves limits through pol.
func New(pol store.Policies, opts ...Option) *Store {
	s := &Store{
		now:    time.Now,
		pol:    pol,
		runs:   map[store.RunID]*run{},
		queues: map[store.QueueKey]*queue{},
		tasks:  map[store.TaskID]*task{},
		ttl:    map[*task]struct{}{},
		leased: map[*task]struct{}{},
		subs:   map[chan store.Notification]struct{}{},

		handlers: map[store.HandlerRef]time.Time{},
	}
	for _, o := range opts {
		o(s)
	}
	s.started = s.now()
	return s
}

type run struct {
	spec                      store.RunSpec
	state                     store.RunState
	paused                    bool
	created, sealed, finished time.Time
	total                     uint64 // tasks ever added
	active                    int    // pending + in flight
	fps                       map[store.Fingerprint]*task
	tasks                     []*task
	backlogs                  []*backlog
	stats                     store.RunStats // counters only; ledger figures are computed
}

func (r *run) dispatchable() bool {
	return (r.state == store.RunOpen || r.state == store.RunSealed) && !r.paused
}

type queue struct {
	key          store.QueueKey
	tat          time.Time
	inFlight     int
	vclock       float64
	blockedUntil time.Time
	blockReason  string
	backlogs     []*backlog
	offer        *offer
	served       uint64
}

// offer is another pool's first refusal on the queue's next dispatch.
type offer struct {
	b     *backlog
	since time.Time
}

type backlog struct {
	q        *queue
	r        *run
	pool     store.PoolName
	pending  []*task // by priority desc, then insertion order
	inFlight int
	vtime    float64
	ownTat   time.Time
}

type task struct {
	id        store.TaskID
	r         *run
	b         *backlog
	fp        store.Fingerprint
	depth     uint32
	parent    store.TaskID
	spec      []byte
	handler   string
	created   time.Time
	priority  int32
	seq       uint64
	notBefore time.Time
	expiresAt time.Time
	state     store.TaskState // Pending, InFlight or a final state; Delayed is derived
	attempt   uint32
	nonce     string
	worker    store.WorkerInfo
	leaseDur  time.Duration
	dispatch  time.Time
	expires   time.Time
	deadline  time.Time
	attempts  []store.Attempt
}

func (t *task) leaseID() store.LeaseID {
	return store.LeaseID(fmt.Sprintf("%s.%d.%s", t.id, t.attempt, t.nonce))
}

// ---- lifecycle

// CreateRun creates the run, or returns the existing one when the spec is identical.
func (s *Store) CreateRun(_ context.Context, spec store.RunSpec) (store.Run, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if r, ok := s.runs[spec.ID]; ok {
		if !bytes.Equal(r.spec.Hash, spec.Hash) {
			return store.Run{}, store.ErrConflict
		}
		return s.view(r), nil
	}
	r := &run{spec: spec, state: store.RunOpen, paused: spec.Paused, created: s.now(),
		fps: map[store.Fingerprint]*task{}, stats: store.RunStats{ByCode: map[string]uint64{}}}
	s.runs[spec.ID] = r
	s.order = append(s.order, r)
	s.event(r, store.EventCreated)
	return s.view(r), nil
}

// SealRun stops seeding; a run with no work left completes at once.
func (s *Store) SealRun(_ context.Context, id store.RunID) (store.Run, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	r, ok := s.runs[id]
	if !ok {
		return store.Run{}, store.ErrNotFound
	}
	if r.state == store.RunOpen {
		r.state, r.sealed = store.RunSealed, s.now()
		s.event(r, store.EventSealed)
		s.complete(r)
	}
	return s.view(r), nil
}

// PauseRun stops dispatching the run's tasks until it is resumed.
func (s *Store) PauseRun(_ context.Context, id store.RunID) (store.Run, error) {
	return s.setPaused(id, true)
}

// ResumeRun makes a paused run dispatchable again.
func (s *Store) ResumeRun(_ context.Context, id store.RunID) (store.Run, error) {
	return s.setPaused(id, false)
}

func (s *Store) setPaused(id store.RunID, paused bool) (store.Run, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	r, ok := s.runs[id]
	if !ok {
		return store.Run{}, store.ErrNotFound
	}
	if r.state.Ended() {
		return store.Run{}, store.ErrRunClosed
	}
	if r.paused != paused {
		r.paused = paused
		if paused {
			s.event(r, store.EventPaused)
		} else {
			s.event(r, store.EventResumed)
			for _, b := range r.backlogs {
				s.notify(b.pool)
			}
		}
	}
	return s.view(r), nil
}

// CancelRun ends the run as cancelled and cancels its pending and in-flight tasks.
func (s *Store) CancelRun(_ context.Context, id store.RunID) (store.Run, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	r, ok := s.runs[id]
	if !ok {
		return store.Run{}, store.ErrNotFound
	}
	if !r.state.Ended() {
		s.end(r, store.RunCancelled)
	}
	return s.view(r), nil
}

// end finishes a run early and drains its tasks.
func (s *Store) end(r *run, state store.RunState) {
	now := s.now()
	r.state, r.finished = state, now
	final, counter := store.TaskCancelled, &r.stats.Cancelled
	if state == store.RunExpired {
		final, counter = store.TaskExpired, &r.stats.Expired
	}
	for _, t := range r.tasks {
		switch t.state {
		case store.TaskPending:
			delete(s.ttl, t)
		case store.TaskInFlight:
			delete(s.leased, t)
			t.b.inFlight--
			t.b.q.inFlight--
			t.attempts = append(t.attempts, store.Attempt{Attempt: t.attempt, Worker: t.worker,
				DispatchedAt: t.dispatch, FinishedAt: now, Record: store.AttemptRecord{Code: "revoked"}})
		default:
			continue
		}
		t.state = final
		*counter++
	}
	for _, b := range r.backlogs {
		b.pending = nil
		s.wakeQueue(b.q)
	}
	r.active = 0
	if state == store.RunExpired {
		s.event(r, store.EventExpired)
	} else {
		s.event(r, store.EventCancelled)
	}
}

// complete finishes a sealed run that has nothing pending or in flight.
func (s *Store) complete(r *run) {
	if r.state == store.RunSealed && r.active == 0 {
		r.state, r.finished = store.RunCompleted, s.now()
		s.event(r, store.EventCompleted)
	}
}

// GetRun returns the run, or store.ErrNotFound.
func (s *Store) GetRun(_ context.Context, id store.RunID) (store.Run, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	r, ok := s.runs[id]
	if !ok {
		return store.Run{}, store.ErrNotFound
	}
	return s.view(r), nil
}

// ListRuns returns the runs that match the filter, one page at a time.
func (s *Store) ListRuns(_ context.Context, f store.RunFilter, page store.Page) ([]store.Run, string, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	var match []*run
	for _, r := range s.order {
		if len(f.States) > 0 && !slices.Contains(f.States, r.state) {
			continue
		}
		ok := true
		for k, v := range f.Labels {
			if r.spec.Labels[k] != v {
				ok = false
			}
		}
		if ok {
			match = append(match, r)
		}
	}
	lo, hi, next := window(len(match), page)
	out := make([]store.Run, 0, hi-lo)
	for _, r := range match[lo:hi] {
		out = append(out, s.view(r))
	}
	return out, next, nil
}

// RunEvents returns up to limit events after the cursor, in commit order.
func (s *Store) RunEvents(_ context.Context, after store.EventCursor, limit int) ([]store.RunEvent, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	from := safeconv.Int(uint64(after))
	if from >= len(s.events) {
		return nil, nil
	}
	evs := s.events[from:]
	if limit > 0 && len(evs) > limit {
		evs = evs[:limit]
	}
	return append([]store.RunEvent(nil), evs...), nil
}

// LastEventCursor returns the cursor of the newest event, or zero when there is none.
func (s *Store) LastEventCursor(context.Context) (store.EventCursor, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	return store.EventCursor(len(s.events)), nil
}

func (s *Store) event(r *run, typ store.RunEventType) {
	s.events = append(s.events, store.RunEvent{Cursor: store.EventCursor(len(s.events) + 1), Type: typ, Run: s.view(r), At: s.now()})
	s.send(store.Notification{Runs: true})
}

func (s *Store) view(r *run) store.Run {
	now := s.now()
	st := r.stats
	st.ByCode = make(map[string]uint64, len(r.stats.ByCode))
	maps.Copy(st.ByCode, r.stats.ByCode)
	for _, b := range r.backlogs {
		st.InFlight += safeconv.Uint64(b.inFlight)
		for _, t := range b.pending {
			if t.notBefore.After(now) {
				st.Delayed++
			} else {
				st.Pending++
			}
		}
	}
	return store.Run{Spec: r.spec, State: r.state, Paused: r.paused, CreatedAt: r.created,
		SealedAt: r.sealed, FinishedAt: r.finished, Stats: st}
}

// ---- tasks

// AddTasks adds seeds or discovered tasks to a run and reports the result of each, in order.
func (s *Store) AddTasks(_ context.Context, req store.AddRequest) ([]store.AddResult, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	r, ok := s.runs[req.Run]
	if !ok {
		return nil, store.ErrNotFound
	}
	now := s.now()
	var depth uint32
	var parent store.TaskID
	if req.Parent != nil {
		p, status := s.byLease(*req.Parent)
		switch {
		case status == store.FinishUnknown:
			return nil, store.ErrLeaseUnknown
		case status != store.FinishAccepted || !now.Before(p.expires) || p.r != r:
			return nil, store.ErrLeaseStale
		}
		depth, parent = p.depth+1, p.id
	}
	out := make([]store.AddResult, len(req.Tasks))
	open := r.state == store.RunOpen || (req.Parent != nil && r.state == store.RunSealed)
	woken := map[store.PoolName]bool{}
	for i := range req.Tasks {
		nt := &req.Tasks[i]
		switch {
		case nt.Invalid:
			out[i].Status = store.AddInvalid
		case !open:
			out[i].Status = store.AddRunClosed
		case r.spec.Limits.MaxDepth > 0 && depth > r.spec.Limits.MaxDepth,
			r.spec.Limits.MaxTasks > 0 && r.total >= r.spec.Limits.MaxTasks:
			out[i].Status = store.AddOverBudget
			r.stats.OverBudget++
		case r.fps[nt.Fingerprint] != nil:
			out[i] = store.AddResult{Status: store.AddDuplicate, TaskID: r.fps[nt.Fingerprint].id}
			r.stats.Duplicates++
		default:
			pool := nt.Pool
			if pool == "" {
				pool = store.DefaultPool
			}
			s.seq++
			t := &task{id: store.TaskID("t" + strconv.FormatUint(s.seq, 36)), r: r, fp: nt.Fingerprint,
				depth: depth, parent: parent, spec: nt.Spec, handler: nt.Handler, created: now, priority: nt.Priority, seq: s.seq,
				notBefore: nt.NotBefore, state: store.TaskPending}
			if nt.TTL > 0 {
				t.expiresAt = now.Add(nt.TTL)
				s.ttl[t] = struct{}{}
			}
			t.b = s.backlog(r, nt.Queue, pool)
			t.b.insert(t)
			s.tasks[t.id], r.fps[t.fp] = t, t
			r.tasks = append(r.tasks, t)
			r.total++
			r.active++
			if req.Parent != nil {
				r.stats.Discovered++
			}
			woken[pool] = true
			out[i] = store.AddResult{Status: store.AddAdded, TaskID: t.id}
		}
	}
	for pool := range woken {
		s.notify(pool)
	}
	return out, nil
}

func (s *Store) backlog(r *run, key store.QueueKey, pool store.PoolName) *backlog {
	q := s.queue(key)
	if i := slices.IndexFunc(q.backlogs, func(b *backlog) bool { return b.r == r && b.pool == pool }); i >= 0 {
		return q.backlogs[i]
	}
	b := &backlog{q: q, r: r, pool: pool}
	q.backlogs = append(q.backlogs, b)
	r.backlogs = append(r.backlogs, b)
	return b
}

func (s *Store) queue(key store.QueueKey) *queue {
	q, ok := s.queues[key]
	if !ok {
		q = &queue{key: key}
		s.queues[key] = q
	}
	return q
}

func (b *backlog) insert(t *task) {
	// Highest priority first, then in order of arrival.
	i, _ := slices.BinarySearchFunc(b.pending, t, func(p, t *task) int {
		return cmp.Or(cmp.Compare(t.priority, p.priority), cmp.Compare(p.seq, t.seq))
	})
	b.pending = slices.Insert(b.pending, i, t)
}

// handlerSet says which tasks a session can run: those of its handlers, and those
// with no handler. A nil set runs everything.
type handlerSet map[string]struct{}

func newHandlerSet(handlers []string) handlerSet {
	if len(handlers) == 0 {
		return nil
	}
	h := make(handlerSet, len(handlers))
	for _, n := range handlers {
		h[n] = struct{}{}
	}
	return h
}

func (h handlerSet) runs(t *task) bool {
	if h == nil || t.handler == "" {
		return true
	}
	_, ok := h[t.handler]
	return ok
}

// eligible counts the tasks that the set can run and that can be claimed now, and
// reports the earliest time a delayed one becomes claimable.
func (b *backlog) eligible(now time.Time, h handlerSet) (n int, next time.Time) {
	for _, t := range b.pending {
		if !h.runs(t) {
			continue
		}
		if t.notBefore.After(now) {
			if next.IsZero() || t.notBefore.Before(next) {
				next = t.notBefore
			}
			continue
		}
		n++
	}
	return n, next
}

// Dispatch leases up to req.Slots due tasks of the pool, fairly across runs and within the queue limits.
func (s *Store) Dispatch(_ context.Context, req store.DispatchRequest) (store.DispatchResult, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	now := s.now()
	s.expireDue(now)
	var res store.DispatchResult
	note := func(t time.Time) {
		if !t.IsZero() && (res.NextReadyAt.IsZero() || t.Before(res.NextReadyAt)) {
			res.NextReadyAt = t
		}
	}
	h := newHandlerSet(req.Handlers)
	var qs []*queue
	for _, q := range s.queues {
		for _, b := range q.backlogs {
			if b.pool == req.Pool && b.r.dispatchable() && slices.ContainsFunc(b.pending, h.runs) {
				qs = append(qs, q)
				break
			}
		}
	}
	// Longest-waiting queue first, so no queue is starved by its position.
	slices.SortFunc(qs, func(a, b *queue) int {
		return cmp.Or(cmp.Compare(a.served, b.served), cmp.Compare(a.key, b.key))
	})
	slots := req.Slots
	for _, q := range qs {
		if slots <= 0 {
			note(now) // more may be ready: the caller asks again when it has slots
			break
		}
		slots -= s.dispatchQueue(q, req, h, slots, now, &res, note)
	}
	return res, nil
}

// dispatchQueue claims up to slots tasks of req.Pool from one queue and returns how many.
func (s *Store) dispatchQueue(q *queue, req store.DispatchRequest, h handlerSet, slots int, now time.Time,
	res *store.DispatchResult, note func(time.Time)) int {
	taken := 0
	for taken < slots {
		if now.Before(q.blockedUntil) {
			note(q.blockedUntil)
			break
		}
		var best *backlog
		var bestEff store.Effective
		var bestStart float64
		var bestAdmit int
		mine := false
		for _, b := range q.backlogs {
			if !b.r.dispatchable() {
				continue
			}
			// Another pool's tasks are judged by their pool's sessions, not this one's.
			bh := h
			if b.pool != req.Pool {
				bh = nil
			}
			elig, next := b.eligible(now, bh)
			if elig == 0 {
				if b.pool == req.Pool {
					note(next)
				}
				continue
			}
			e := s.pol.For(b.r.spec.ID, b.r.spec.Policy, q.key)
			admit, at := sched.Admitted(e.Queue, q.tat, now, q.inFlight), sched.EligibleAt(e.Queue, q.tat)
			if e.Own {
				admit, at = sched.Admitted(e.Limits, b.ownTat, now, b.inFlight), sched.EligibleAt(e.Limits, b.ownTat)
			}
			if admit == 0 {
				// Refused by rate: ready at a known time. Refused by concurrency: a
				// Finish wakes the pool.
				if b.pool == req.Pool && at.After(now) {
					note(at)
				}
				continue
			}
			admit = min(admit, elig)
			if b.pool == req.Pool {
				mine = true
			}
			start := sched.Start(b.vtime, q.vclock)
			if best == nil || start < bestStart || (start == bestStart && before(b, best)) {
				best, bestEff, bestStart, bestAdmit = b, e, start, admit
			}
		}
		if best == nil || !mine {
			break
		}
		if best.pool != req.Pool {
			// Another pool holds the lowest start tag: it has first refusal.
			if q.offer == nil || q.offer.b != best {
				q.offer = &offer{b: best, since: now}
				s.notify(best.pool)
			}
			if end := q.offer.since.Add(sched.RefusalPeriod(bestEff.Queue)); now.Before(end) {
				note(end)
				break
			}
			// It did not take its turn: it forfeits, charged one task.
			best.vtime, q.vclock = sched.Charge(bestStart, q.vclock, bestEff.Weight, 1)
			continue
		}
		n := min(slots-taken, bestAdmit)
		claimed := best.claim(n, now, h)
		n = len(claimed)
		if bestEff.Own {
			best.ownTat = sched.Advance(bestEff.Limits, best.ownTat, now, n)
			q.tat = sched.AdvanceCapped(bestEff.Queue, q.tat, now, n)
		} else {
			q.tat = sched.Advance(bestEff.Queue, q.tat, now, n)
		}
		best.vtime, q.vclock = sched.Charge(bestStart, q.vclock, bestEff.Weight, n)
		best.inFlight += n
		q.inFlight += n
		q.offer = nil
		s.served++
		q.served = s.served
		best.r.stats.Attempts += uint64(n)
		for _, t := range claimed {
			delete(s.ttl, t)
			s.leased[t] = struct{}{}
			t.state, t.worker, t.dispatch = store.TaskInFlight, req.Worker, now
			t.attempt++
			t.nonce = nonce()
			t.leaseDur = bestEff.Lease.Duration
			t.expires = now.Add(bestEff.Lease.Duration)
			t.deadline = now.Add(bestEff.Lease.MaxDuration)
			if t.deadline.Before(t.expires) {
				t.deadline = t.expires
			}
			res.Assignments = append(res.Assignments, store.Assignment{
				Lease: store.Lease{Ref: store.LeaseRef{ID: t.leaseID(), Attempt: t.attempt}, ExpiresAt: t.expires, Duration: t.leaseDur},
				Info:  s.info(t),
				Spec:  t.spec,
			})
		}
		taken += n
	}
	return taken
}

// before breaks ties between backlogs: older run first, then pool name.
func before(a, b *backlog) bool {
	if a.r != b.r {
		return a.r.created.Before(b.r.created) || (a.r.created.Equal(b.r.created) && a.r.spec.ID < b.r.spec.ID)
	}
	return a.pool < b.pool
}

// claim removes up to n claimable tasks the set can run, highest priority first.
func (b *backlog) claim(n int, now time.Time, h handlerSet) []*task {
	var out []*task
	keep := b.pending[:0]
	for _, t := range b.pending {
		if len(out) < n && !t.notBefore.After(now) && h.runs(t) {
			out = append(out, t)
			continue
		}
		keep = append(keep, t)
	}
	for i := len(keep); i < len(b.pending); i++ {
		b.pending[i] = nil
	}
	b.pending = keep
	return out
}

func (s *Store) info(t *task) store.LeaseInfo {
	return store.LeaseInfo{Ref: store.LeaseRef{ID: t.leaseID(), Attempt: t.attempt}, Task: t.id, Run: t.r.spec.ID,
		RunPolicy: t.r.spec.Policy, Queue: t.b.q.key, Pool: t.b.pool, Fingerprint: t.fp, Depth: t.depth, Parent: t.parent}
}

// byLease finds the task of a lease. Accepted means the lease is the task's current
// one; liveness is the caller's check.
func (s *Store) byLease(ref store.LeaseRef) (*task, store.FinishStatus) {
	parts := strings.Split(string(ref.ID), ".")
	if len(parts) != 3 {
		return nil, store.FinishUnknown
	}
	t, ok := s.tasks[store.TaskID(parts[0])]
	if !ok {
		return nil, store.FinishUnknown
	}
	if t.state != store.TaskInFlight || t.attempt != ref.Attempt || t.leaseID() != ref.ID {
		return t, store.FinishStale
	}
	return t, store.FinishAccepted
}

// Lease returns the context of the task's current lease, or store.ErrNotFound if the lease is stale or unknown. It changes nothing.
func (s *Store) Lease(_ context.Context, ref store.LeaseRef) (store.LeaseInfo, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	t, status := s.byLease(ref)
	if status != store.FinishAccepted {
		return store.LeaseInfo{}, store.ErrNotFound
	}
	return s.info(t), nil
}

// ExtendLease moves the lease expiry to now plus by, or the lease duration if by is not positive, capped by the lease deadline.
func (s *Store) ExtendLease(_ context.Context, ref store.LeaseRef, by time.Duration) (store.Lease, store.FinishStatus, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	now := s.now()
	t, status := s.byLease(ref)
	if status != store.FinishAccepted {
		return store.Lease{}, status, nil
	}
	if !now.Before(t.expires) {
		return store.Lease{}, store.FinishStale, nil
	}
	if by <= 0 {
		by = t.leaseDur
	}
	t.expires = now.Add(by)
	if t.expires.After(t.deadline) {
		t.expires = t.deadline
	}
	return store.Lease{Ref: ref, ExpiresAt: t.expires, Duration: t.expires.Sub(now)}, store.FinishAccepted, nil
}

// Finish applies the transition the core decided for an attempt, unless the lease is stale.
func (s *Store) Finish(_ context.Context, f store.Finish) (store.FinishStatus, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	now := s.now()
	t, status := s.byLease(f.Lease)
	if status != store.FinishAccepted {
		return status, nil
	}
	if live := now.Before(t.expires); live != (f.Source == store.SourceWorker) {
		return store.FinishStale, nil
	}
	b, q, r := t.b, t.b.q, t.r
	delete(s.leased, t)
	b.inFlight--
	q.inFlight--
	if f.Result == store.ResultReleased {
		// The assignment never reached a worker: the attempt did not happen.
		t.attempt--
		r.stats.Attempts--
		t.state, t.notBefore = store.TaskPending, now.Add(f.RetryIn)
		if !t.expiresAt.IsZero() {
			s.ttl[t] = struct{}{}
		}
		b.insert(t)
		s.wakeQueue(q)
		return store.FinishAccepted, nil
	}
	t.attempts = append(t.attempts, store.Attempt{Attempt: t.attempt, Worker: t.worker,
		DispatchedAt: t.dispatch, FinishedAt: now, Record: f.Record})
	if f.Record.Code != "" {
		r.stats.ByCode[f.Record.Code]++
	}
	if f.Block > 0 {
		if until := now.Add(f.Block); until.After(q.blockedUntil) {
			q.blockedUntil, q.blockReason = until, f.Record.Code
		}
		r.stats.QueueBlocks++
	}
	switch f.Result {
	case store.ResultRetry:
		t.state, t.notBefore = store.TaskPending, now.Add(f.RetryIn)
		if !t.expiresAt.IsZero() {
			s.ttl[t] = struct{}{}
		}
		b.insert(t)
	case store.ResultFailed:
		t.state = store.TaskFailed
		r.stats.Failed++
		r.active--
	default:
		t.state = store.TaskSucceeded
		r.stats.Succeeded++
		r.active--
	}
	s.complete(r)
	s.wakeQueue(q)
	return store.FinishAccepted, nil
}

// ExpiredLeases returns up to limit leases past their expiry, oldest first.
func (s *Store) ExpiredLeases(_ context.Context, limit int) ([]store.LeaseInfo, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	now := s.now()
	var out []*task
	for t := range s.leased {
		if !now.Before(t.expires) {
			out = append(out, t)
		}
	}
	slices.SortFunc(out, func(a, b *task) int { return a.expires.Compare(b.expires) })
	if limit > 0 && len(out) > limit {
		out = out[:limit]
	}
	infos := make([]store.LeaseInfo, len(out))
	for i, t := range out {
		infos[i] = s.info(t)
	}
	return infos, nil
}

// expireDue drops pending tasks past their TTL.
func (s *Store) expireDue(now time.Time) {
	for t := range s.ttl {
		if now.Before(t.expiresAt) {
			continue
		}
		delete(s.ttl, t)
		if i := slices.Index(t.b.pending, t); i >= 0 {
			t.b.pending = slices.Delete(t.b.pending, i, i+1)
		}
		t.state = store.TaskExpired
		t.r.stats.Expired++
		t.r.active--
		s.complete(t.r)
	}
}

// ---- inspection

func (s *Store) status(t *task, now time.Time) store.TaskStatus {
	st := store.TaskStatus{ID: t.id, Run: t.r.spec.ID, Queue: t.b.q.key, Pool: t.b.pool, Fingerprint: t.fp,
		Depth: t.depth, Parent: t.parent, Spec: t.spec, State: t.state, Attempts: append([]store.Attempt(nil), t.attempts...)}
	if t.state == store.TaskPending && t.notBefore.After(now) {
		st.State, st.NextEligibleAt = store.TaskDelayed, t.notBefore
	}
	return st
}

// GetTask returns a task by ID, or by fingerprint when fp is set.
func (s *Store) GetTask(_ context.Context, id store.RunID, tid store.TaskID, fp *store.Fingerprint) (store.TaskStatus, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	r, ok := s.runs[id]
	if !ok {
		return store.TaskStatus{}, store.ErrNotFound
	}
	t := s.tasks[tid]
	if t == nil && fp != nil {
		t = r.fps[*fp]
	}
	if t == nil || t.r != r {
		return store.TaskStatus{}, store.ErrNotFound
	}
	return s.status(t, s.now()), nil
}

// ListTasks returns the tasks that match the filter, one page at a time.
func (s *Store) ListTasks(_ context.Context, f store.TaskFilter, page store.Page) ([]store.TaskStatus, string, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	r, ok := s.runs[f.Run]
	if !ok {
		return nil, "", store.ErrNotFound
	}
	now := s.now()
	var match []store.TaskStatus
	for _, t := range r.tasks {
		if f.Queue != "" && t.b.q.key != f.Queue {
			continue
		}
		st := s.status(t, now)
		if len(f.States) > 0 {
			found := false
			for _, want := range f.States {
				found = found || want == st.State
			}
			if !found {
				continue
			}
		}
		if f.Code != "" {
			found := slices.ContainsFunc(t.attempts, func(a store.Attempt) bool { return a.Record.Code == f.Code })
			if !found {
				continue
			}
		}
		match = append(match, st)
	}
	lo, hi, next := window(len(match), page)
	return match[lo:hi], next, nil
}

func (s *Store) queueView(q *queue, now time.Time) store.Queue {
	lim := s.pol.For("", nil, q.key).Queue
	v := store.Queue{Key: q.key, InFlight: q.inFlight, NextEligibleAt: sched.EligibleAt(lim, q.tat),
		AdmissibleNow: sched.Admitted(lim, q.tat, now, q.inFlight), BlockReason: q.blockReason}
	if now.Before(q.blockedUntil) {
		v.BlockedUntil, v.AdmissibleNow = q.blockedUntil, 0
	} else {
		v.BlockReason = ""
	}
	for _, b := range q.backlogs {
		if len(b.pending) == 0 && b.inFlight == 0 {
			continue
		}
		bv := store.Backlog{Run: b.r.spec.ID, Pool: b.pool, InFlight: b.inFlight,
			Weight: s.pol.For(b.r.spec.ID, b.r.spec.Policy, q.key).Weight}
		for _, t := range b.pending {
			if t.notBefore.After(now) {
				bv.Delayed++
			} else {
				bv.Pending++
			}
		}
		v.Backlogs = append(v.Backlogs, bv)
	}
	return v
}

// GetQueue returns the queue, or store.ErrNotFound.
func (s *Store) GetQueue(_ context.Context, key store.QueueKey) (store.Queue, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	q, ok := s.queues[key]
	if !ok {
		return store.Queue{}, store.ErrNotFound
	}
	return s.queueView(q, s.now()), nil
}

// ListQueues returns the queues that match the filter, one page at a time.
func (s *Store) ListQueues(_ context.Context, f store.QueueFilter, page store.Page) ([]store.Queue, string, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	now := s.now()
	var keys []string
	for k, q := range s.queues {
		if !strings.HasPrefix(string(k), f.KeyPrefix) || (f.OnlyBlocked && !now.Before(q.blockedUntil)) {
			continue
		}
		if f.Run != "" {
			if !slices.ContainsFunc(q.backlogs, func(b *backlog) bool {
				return b.r.spec.ID == f.Run && (len(b.pending) > 0 || b.inFlight > 0)
			}) {
				continue
			}
		}
		keys = append(keys, string(k))
	}
	slices.Sort(keys)
	lo, hi, next := window(len(keys), page)
	out := make([]store.Queue, 0, hi-lo)
	for _, k := range keys[lo:hi] {
		out = append(out, s.queueView(s.queues[store.QueueKey(k)], now))
	}
	return out, next, nil
}

// BlockQueue blocks the queue for all runs until the given time; a zero time lifts the block.
func (s *Store) BlockQueue(_ context.Context, key store.QueueKey, until time.Time, reason string) (store.Queue, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	q := s.queue(key)
	q.blockedUntil, q.blockReason = until, reason
	s.wakeQueue(q)
	return s.queueView(q, s.now()), nil
}

// ---- background

// Start records the start of a replica: no task fails for want of a handler until a
// grace period has passed since the newest start.
func (s *Store) Start(context.Context, store.Replica) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.started = s.now()
	return nil
}

// ServeHandlers records that the handlers have sessions now.
func (s *Store) ServeHandlers(_ context.Context, sets []store.PoolHandlers) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	now := s.now()
	for _, set := range sets {
		for _, h := range set.Handlers {
			s.handlers[store.HandlerRef{Pool: set.Pool, Handler: h}] = now
		}
	}
	return nil
}

// SweepUnserved fails the pending tasks that waited longer than grace for a session
// that can run their handler.
func (s *Store) SweepUnserved(_ context.Context, grace time.Duration) (store.Unserved, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	now := s.now()
	var out store.Unserved
	waiting := map[store.HandlerRef]bool{}
	failed := map[store.HandlerRef]int{}
	for _, r := range s.order {
		if !r.dispatchable() {
			continue
		}
		for _, b := range r.backlogs {
			for _, t := range slices.Clone(b.pending) {
				ref := store.HandlerRef{Pool: b.pool, Handler: t.handler}
				if seen, ok := s.handlers[ref]; t.handler == "" || (ok && now.Sub(seen) <= grace) {
					continue // served, or no handler named: any session may try it
				}
				if now.Sub(t.created) < grace || now.Sub(s.started) < grace {
					waiting[ref] = true
					continue
				}
				if i := slices.Index(b.pending, t); i >= 0 {
					b.pending = slices.Delete(b.pending, i, i+1)
				}
				delete(s.ttl, t)
				t.attempt++
				t.attempts = append(t.attempts, store.Attempt{Attempt: t.attempt, DispatchedAt: now, FinishedAt: now, Record: store.AttemptRecord{
					Kind: int32(taskv1.Outcome_KIND_FAILED), Code: store.CodeNoHandler, Message: "no worker had the handler " + t.handler + " for " + grace.String()}})
				t.state = store.TaskFailed
				r.stats.Failed++
				r.stats.ByCode[store.CodeNoHandler]++
				r.active--
				failed[ref]++
				s.complete(r)
			}
		}
	}
	for ref := range waiting {
		out.Waiting = append(out.Waiting, ref)
	}
	for ref, n := range failed {
		out.Failed = append(out.Failed, store.FailedTasks{HandlerRef: ref, Tasks: n})
	}
	slices.SortFunc(out.Waiting, cmpRef)
	slices.SortFunc(out.Failed, func(a, b store.FailedTasks) int { return cmpRef(a.HandlerRef, b.HandlerRef) })
	return out, nil
}

func cmpRef(a, b store.HandlerRef) int {
	return cmp.Or(cmp.Compare(a.Pool, b.Pool), cmp.Compare(a.Handler, b.Handler))
}

// Notifications delivers wake-ups until ctx is done. A full channel drops wake-ups, which only delays work.
func (s *Store) Notifications(ctx context.Context) (<-chan store.Notification, error) {
	ch := make(chan store.Notification, 64)
	s.mu.Lock()
	s.subs[ch] = struct{}{}
	s.mu.Unlock()
	go func() {
		<-ctx.Done()
		s.mu.Lock()
		delete(s.subs, ch)
		s.mu.Unlock()
	}()
	return ch, nil
}

// Maintain expires tasks past their TTL and runs past their deadline.
func (s *Store) Maintain(context.Context) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	now := s.now()
	s.expireDue(now)
	for _, r := range s.order {
		if !r.state.Ended() && !r.spec.Limits.Deadline.IsZero() && !now.Before(r.spec.Limits.Deadline) {
			s.end(r, store.RunExpired)
		}
	}
	s.retain(now)
	return nil
}

// retain deletes the runs that ended longer ago than the retention, with their tasks
// and backlogs, and the queues nothing refers to any more.
func (s *Store) retain(now time.Time) {
	if s.keep <= 0 {
		return
	}
	kept := s.order[:0]
	for _, r := range s.order {
		if !r.state.Ended() || now.Sub(r.finished) < s.keep {
			kept = append(kept, r)
			continue
		}
		delete(s.runs, r.spec.ID)
		for _, t := range r.tasks {
			delete(s.tasks, t.id)
		}
		for _, b := range r.backlogs {
			q := b.q
			q.backlogs = slices.DeleteFunc(q.backlogs, func(o *backlog) bool { return o == b })
			if q.offer != nil && q.offer.b == b {
				q.offer = nil
			}
			// A queue is dropped only when it carries no state a later task needs:
			// nothing in flight, no block and no rate limit still counting.
			if len(q.backlogs) == 0 && q.inFlight == 0 && !q.blockedUntil.After(now) && !q.tat.After(now) {
				delete(s.queues, q.key)
			}
		}
	}
	clear(s.order[len(kept):])
	s.order = kept
}

// Now returns the store clock.
func (s *Store) Now(context.Context) (time.Time, error) { return s.now(), nil }

func (s *Store) notify(pool store.PoolName) { s.send(store.Notification{Pool: pool}) }

// wakeQueue notifies every pool with work on the queue: a slot or a block changed.
func (s *Store) wakeQueue(q *queue) {
	seen := map[store.PoolName]bool{}
	for _, b := range q.backlogs {
		if len(b.pending) > 0 && !seen[b.pool] {
			seen[b.pool] = true
			s.notify(b.pool)
		}
	}
}

func (s *Store) send(n store.Notification) {
	for ch := range s.subs {
		select {
		case ch <- n:
		default: // full: the subscriber is already due to wake up
		}
	}
}

func nonce() string {
	var b [8]byte
	_, _ = rand.Read(b[:])
	return hex.EncodeToString(b[:])
}

// window turns a page request over n items into slice bounds and the next token.
func window(n int, page store.Page) (lo, hi int, next string) {
	lo, _ = strconv.Atoi(page.Token)
	if lo < 0 || lo > n {
		lo = n
	}
	size := page.Size
	if size <= 0 {
		size = 100
	}
	hi = lo + size
	if hi >= n {
		return lo, n, ""
	}
	return lo, hi, strconv.Itoa(hi)
}

var _ store.Store = (*Store)(nil)

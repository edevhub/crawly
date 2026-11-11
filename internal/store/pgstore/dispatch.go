package pgstore

import (
	"bytes"
	"cmp"
	"context"
	"errors"
	"slices"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgtype"

	"github.com/edevhub/crawly/internal/sched"
	"github.com/edevhub/crawly/internal/store"
)

// Task states as the task_state enum spells them.
const (
	taskPending   = "pending"
	taskInFlight  = "in_flight"
	taskSucceeded = "succeeded"
	taskFailed    = "failed"
	taskExpired   = "expired"
	taskCancelled = "cancelled"
)

const (
	// candidateBacklogs and candidateQueues bound one round's candidates: the first
	// bounds backlogs, the second queues, so a queue with many runs cannot crowd the
	// others out.
	candidateBacklogs = 256
	candidateQueues   = 64
	// lockRetry is how soon a round is repeated when it passed over work that another
	// transaction held: the holder may commit without changing readiness, and so
	// without a notification.
	lockRetry = 20 * time.Millisecond
)

// Dispatch leases up to req.Slots due tasks of the pool that the session can run,
// fairly across runs and within the queue limits. It is one transaction per queue it
// dispatches from.
func (s *Store) Dispatch(ctx context.Context, req store.DispatchRequest) (store.DispatchResult, error) {
	var res store.DispatchResult
	pool, ok, err := s.lookupPool(ctx, s.db, req.Pool)
	if err != nil || !ok {
		return res, wrap(err) // no task was ever added to the pool
	}
	now, err := s.Now(ctx)
	if err != nil {
		return res, err
	}
	run := newHandlerSet(req.Handlers)
	keys, err := s.candidates(ctx, pool, run, now)
	if err != nil {
		return res, err
	}
	slots := req.Slots
	for _, key := range keys {
		if slots <= 0 {
			break
		}
		var got []store.Assignment
		err := s.tx(ctx, func(tx pgx.Tx, at time.Time) error {
			var err error
			now = latest(now, at)
			got, err = s.dispatchQueue(ctx, tx, at, store.QueueKey(key), pool, run, req, slots)
			return err
		})
		if err != nil {
			return res, err // what was claimed so far is committed and is returned
		}
		res.Assignments = append(res.Assignments, got...)
		slots -= len(got)
	}

	// When to come back: the earliest readiness among the lanes the session can run
	// that is a time.
	next, err := s.nextReady(ctx, pool, run)
	switch {
	case errors.Is(err, pgx.ErrNoRows):
		return res, nil // nothing is waiting for a time: a notification brings the next round
	case err != nil:
		return res, wrap(err)
	}
	switch at := next; {
	case at.After(now):
		res.NextReadyAt = at
	case slots <= 0:
		res.NextReadyAt = now // more is ready: the caller asks again when it has slots
	default:
		// Ready, and this round did not take it: another transaction held the queue
		// or the tasks, or the work became ready during the round.
		res.NextReadyAt = now.Add(lockRetry)
	}
	return res, nil
}

// handlerSet is the handlers a session can run, as the lane rows name them. A nil set
// runs every handler.
type handlerSet struct {
	names []string // the session's handlers, and "" for tasks whose handler is unknown
}

func newHandlerSet(handlers []string) *handlerSet {
	if len(handlers) == 0 {
		return nil
	}
	return &handlerSet{names: append(slices.Clone(handlers), "")}
}

// runs reports whether the session can run the lane's tasks.
func (h *handlerSet) runs(l *lane) bool {
	return h == nil || slices.Contains(h.names, l.handler)
}

// candidatesByHandlerSQL: one probe of lanes_ready per handler, each bounded, then the
// earliest queues of what they found.
const candidatesByHandlerSQL = `SELECT queue_key FROM (
		SELECT l.queue_key, l.ready_at FROM unnest($5::text[]) AS h(handler)
		CROSS JOIN LATERAL (SELECT queue_key, ready_at FROM lanes
			WHERE pool_id = $1 AND handler = h.handler AND ready_at <= $2 ORDER BY ready_at LIMIT $6) l
		ORDER BY l.ready_at LIMIT $3) c
	GROUP BY queue_key ORDER BY min(ready_at) LIMIT $4`

// claimSQL leases the best $11 due tasks of a backlog's handlers: one range of
// tasks_claim_h per handler, merged by priority. The picked rows are materialised
// first: as a plain sub-select the planner may run the LIMIT again for every outer row
// and claim more than n.
const claimSQL = `WITH picked AS MATERIALIZED (
		SELECT p.task_id FROM unnest($12::text[]) AS h(handler)
		CROSS JOIN LATERAL (
			SELECT task_id, priority FROM tasks
			WHERE run_created_on = $1 AND run_no = $2 AND queue_key = $3 AND pool_id = $4 AND handler = h.handler
				AND state = 'pending' AND not_before <= $5 AND (expires_at IS NULL OR expires_at > $5)
			ORDER BY priority DESC, task_id LIMIT $11 FOR UPDATE SKIP LOCKED) p
		ORDER BY p.priority DESC, p.task_id LIMIT $11)
	UPDATE tasks t SET state = 'in_flight', attempt = t.attempt + 1, lease_nonce = $6,
		worker_id = $7, worker_version = $8, dispatched_at = $5, lease_expires_at = $9, lease_deadline = $10
	FROM picked WHERE t.run_created_on = $1 AND t.task_id = picked.task_id
	RETURNING t.task_id, t.attempt, t.depth, t.parent_task_id, t.parent_bucket, t.fingerprint, t.priority, t.spec, t.strain_attempts,
		t.created_at, t.not_before`

// candidates are the queues the session may dispatch from, the one with the earliest
// ready lane first. Each handler is probed in the readiness index on its own, so a
// session never reads past lanes it cannot run.
func (s *Store) candidates(ctx context.Context, pool int16, h *handlerSet, now time.Time) ([]string, error) {
	var rows pgx.Rows
	var err error
	// Each handler's probe reads at most its share of the bound.
	each := candidateBacklogs
	if h != nil {
		each = max(candidateBacklogs/len(h.names), 8)
	}
	if h == nil {
		rows, err = s.db.Query(ctx, `SELECT queue_key FROM (
				SELECT queue_key, ready_at FROM lanes WHERE pool_id = $1 AND ready_at <= $2 ORDER BY ready_at LIMIT $3) c
			GROUP BY queue_key ORDER BY min(ready_at) LIMIT $4`, pool, now, candidateBacklogs, candidateQueues)
	} else {
		rows, err = s.db.Query(ctx, candidatesByHandlerSQL, pool, now, candidateBacklogs, candidateQueues, h.names, each)
	}
	if err != nil {
		return nil, wrap(err)
	}
	keys, err := pgx.CollectRows(rows, pgx.RowTo[string])
	return keys, wrap(err)
}

// nextReady is the earliest readiness that is a time among the lanes the session can
// run, or pgx.ErrNoRows when there is none.
func (s *Store) nextReady(ctx context.Context, pool int16, h *handlerSet) (time.Time, error) {
	var next pgtype.Timestamptz
	var err error
	if h == nil {
		err = s.db.QueryRow(ctx, `SELECT ready_at FROM lanes WHERE pool_id = $1 AND ready_at < 'infinity'
			ORDER BY ready_at LIMIT 1`, pool).Scan(&next)
	} else {
		err = s.db.QueryRow(ctx, `SELECT l.ready_at FROM unnest($2::text[]) AS h(handler)
			CROSS JOIN LATERAL (SELECT ready_at FROM lanes
				WHERE pool_id = $1 AND handler = h.handler AND ready_at < 'infinity' ORDER BY ready_at LIMIT 1) l
			ORDER BY l.ready_at LIMIT 1`, pool, h.names).Scan(&next)
	}
	return timeOf(next), err
}

// dispatchQueue claims up to slots tasks of the pool from one queue.
func (s *Store) dispatchQueue(ctx context.Context, tx pgx.Tx, now time.Time, key store.QueueKey, pool int16, run *handlerSet,
	req store.DispatchRequest, slots int) ([]store.Assignment, error) {
	// The queue, or pass over it if another operation holds it.
	q, err := lockQueue(ctx, tx, key, true)
	if err != nil || q == nil {
		return nil, err
	}
	// Every backlog of the queue with pending tasks, of every pool: fairness is
	// decided across pools, and each gets its exact politeness time written back.
	bs, err := s.lockBacklogs(ctx, tx, key, 0, 0)
	if err != nil {
		return nil, err
	}
	if err := s.heal(ctx, tx, key, bs); err != nil {
		return nil, err
	}
	if first := firstLoaded(bs); first != nil {
		q.strain = first.eff.Adaptive.Decayed(q.strain, now)
	}

	// The lanes this session can run. Another pool's lanes are judged by its own
	// sessions, so the first refusal below does not depend on this session's handlers;
	// but a lane that none of them can run is not worth waiting for.
	served, err := s.servedHandlers(ctx, tx, now, pool, bs)
	if err != nil {
		return nil, err
	}
	for _, b := range bs {
		for _, l := range b.lanes {
			l.mine = b.pool == pool && run.runs(l)
		}
	}
	runnable := func(b *brow) func(*lane) bool {
		if b.pool == pool {
			return func(l *lane) bool { return l.mine }
		}
		return func(l *lane) bool { return l.handler == "" || served[b.pool][l.handler] || served[b.pool][""] }
	}

	var out []store.Assignment
	var heldUntil time.Time // set when another pool has first refusal on the queue
	notify := map[int16]bool{}
	dispatched := false
	blocked := now.Before(q.blockedUntil)
	for taken := 0; !blocked && taken < slots; {
		var best *brow
		var bestStart float64
		var bestAdmit int
		ownPool := false
		for _, b := range bs {
			if !b.live || b.pending == 0 || b.short || !b.readyLanes(now, runnable(b)) {
				continue
			}
			admit, _ := admission(q, b, now)
			if admit == 0 {
				continue
			}
			if b.pool == pool {
				ownPool = true
			}
			// The start tag is clamped to the queue's clock at pick time, so a backlog
			// that could not be picked for a while cannot bank credit.
			start := sched.Start(b.vtime, q.vclock)
			if best == nil || start < bestStart || (start == bestStart && before(b, best)) {
				best, bestStart, bestAdmit = b, start, admit
			}
		}
		if best == nil || !ownPool {
			break
		}
		if best.pool != pool {
			// Another pool holds the lowest start tag: it has first refusal, for a
			// period counted from when its turn came.
			since := now
			if r := best.earliestReady(now); !r.Equal(forever) {
				since = r
			}
			_, eligible := admission(q, best, now)
			if end := latest(since, eligible).Add(sched.RefusalPeriod(best.eff.Queue)); now.Before(end) {
				heldUntil = end
				notify[best.pool] = true
				break
			}
			// It did not take its turn: it forfeits, charged one task.
			best.vtime, q.vclock = sched.Charge(bestStart, q.vclock, best.eff.Weight, 1)
			best.dirty = true
			continue
		}
		want := min(slots-taken, bestAdmit)
		if best.pending < int64(want) {
			want = int(best.pending)
		}
		var handlers []string
		for _, l := range best.lanes {
			if l.mine && l.waiting(now) {
				handlers = append(handlers, l.handler)
			}
		}
		got, err := s.claim(ctx, tx, now, key, best, handlers, req, want)
		if err != nil {
			return nil, err
		}
		if n := len(got); n > 0 {
			queue, own := best.limits(q)
			if best.eff.Own {
				best.ownTat = sched.Advance(own, best.ownTat, now, n)
				q.tat = sched.AdvanceCapped(queue, q.tat, now, n)
			} else {
				q.tat = sched.Advance(queue, q.tat, now, n)
			}
			best.vtime, q.vclock = sched.Charge(bestStart, q.vclock, best.eff.Weight, n)
			best.pending -= int64(n)
			best.inFlight += n
			best.attempts += int64(n)
			best.dirty = true
			q.inFlight += n
			dispatched = true
			taken += n
			out = append(out, got...)
		}
		if len(got) < want {
			// Fewer than admitted: the rest waits for a retry delay, has expired, or
			// is held by another transaction. Leave it out of this round and note when
			// its next task is due, so the backlog is not selected again before then.
			best.short = true
			for _, l := range best.lanes {
				if !l.mine || !l.waiting(now) {
					continue
				}
				l.short = true
				if l.due, err = s.nextDue(ctx, tx, now, key, best, l.handler); err != nil {
					return nil, err
				}
			}
		}
	}

	// Readiness, for every lane this round looked at: early is allowed, late is not.
	for _, b := range bs {
		if !b.live || b.pending+int64(b.inFlight) == 0 {
			continue
		}
		for _, l := range b.lanes {
			if l.waiting(now) {
				l.setReady(readiness(q, b, l, now, pool, dispatched, heldUntil))
			}
		}
	}
	if err := flush(ctx, tx, now, q, bs); err != nil {
		return nil, err
	}
	return out, s.wake(ctx, tx, notify)
}

// servedWindow is how recently a replica must have reported a handler for it to count
// as served when another pool's first refusal is decided. The reports come far more
// often than this.
const servedWindow = 5 * time.Minute

// servedHandlers returns, for each pool that has a live backlog with pending tasks in
// the queue besides the dispatching pool's, the handlers that have had a session
// lately. The handler "" is a session that can run any. Nil when there is no such pool.
func (s *Store) servedHandlers(ctx context.Context, tx pgx.Tx, now time.Time, pool int16, bs []*brow) (map[int16]map[string]bool, error) {
	if !slices.ContainsFunc(bs, func(b *brow) bool { return b.pool != pool && b.live && b.pending > 0 }) {
		return nil, nil
	}
	rows, err := tx.Query(ctx, `SELECT p.pool_id, h.handler FROM pool_handlers h JOIN pools p ON p.name = h.pool
		WHERE h.seen_at > $1`, now.Add(-servedWindow))
	if err != nil {
		return nil, err
	}
	out := map[int16]map[string]bool{}
	var id int16
	var handler string
	_, err = pgx.ForEachRow(rows, []any{&id, &handler}, func() error {
		if out[id] == nil {
			out[id] = map[string]bool{}
		}
		out[id][handler] = true
		return nil
	})
	return out, err
}

// earliestReady is the earliest readiness among the backlog's lanes that wait at now:
// forever when they all wait for an event.
func (b *brow) earliestReady(now time.Time) time.Time {
	r := forever
	for _, l := range b.lanes {
		if l.waiting(now) && l.ready.Before(r) {
			r = l.ready
		}
	}
	return r
}

// readiness is the value written back for a lane a dispatch round looked at. A lane
// whose session found nothing it could claim and nothing coming is marked gone.
func readiness(q *qrow, b *brow, l *lane, now time.Time, pool int16, dispatched bool, heldUntil time.Time) time.Time {
	switch {
	case b.pending == 0:
		return time.Time{} // nothing to dispatch
	case now.Before(q.blockedUntil):
		return q.blockedUntil
	case l.short && l.due.IsZero():
		l.gone = true // every pending task of the handler has expired: Maintain removes them
		return time.Time{}
	case l.short:
		return l.due
	}
	admit, at := admission(q, b, now)
	switch {
	case admit == 0 && at.After(now):
		return at // refused by rate: the exact politeness time
	case admit == 0:
		return forever // refused by concurrency: a Finish lowers it
	case b.pool == pool && !heldUntil.IsZero():
		return heldUntil // waits for another pool's first refusal
	case dispatched || l.ready.Equal(forever):
		// Admitted and not served. A first-refusal period on this backlog counts from
		// the queue's last dispatch.
		return now
	}
	return l.ready
}

// before breaks ties between backlogs with the same start tag: the older run first,
// then the lower pool key.
func before(a, b *brow) bool {
	if a.run != b.run {
		return a.run < b.run
	}
	return a.pool < b.pool
}

// heal takes the backlogs of paused and ended runs out of the readiness index. The
// run rows are locked first, so a resume either committed before, and the backlog
// stays, or waits for this transaction, and its sweep finds the backlog.
func (s *Store) heal(ctx context.Context, tx pgx.Tx, key store.QueueKey, bs []*brow) error {
	var stale []*brow
	for _, b := range bs {
		if !b.live && slices.ContainsFunc(b.lanes, func(l *lane) bool { return !l.ready.IsZero() }) {
			stale = append(stale, b)
		}
	}
	if len(stale) == 0 {
		return nil
	}
	if err := s.loadRuns(ctx, tx, key, stale, true); err != nil {
		return err
	}
	for _, b := range stale {
		if !b.live {
			b.setReady(time.Time{})
		}
	}
	return nil
}

// claim leases up to n due tasks of the backlog's lanes for the handlers, highest
// priority first. Each lane is read through the claim index on its own and the best n
// of them are kept, so the order is the one of a single backlog and nothing is read
// past the tasks that are taken.
func (s *Store) claim(ctx context.Context, tx pgx.Tx, now time.Time, key store.QueueKey, b *brow, handlers []string,
	req store.DispatchRequest, n int) ([]store.Assignment, error) {
	lease := b.eff.Lease
	expires := now.Add(lease.Duration)
	deadline := latest(now.Add(lease.MaxDuration), expires)
	nonce := newNonce()
	poolName, err := s.poolOf(ctx, tx, b.pool)
	if err != nil {
		return nil, err
	}
	rows, err := tx.Query(ctx, claimSQL,
		b.meta.createdOn, b.run, string(key), b.pool, now, nonce, req.Worker.ID, req.Worker.Version, expires, deadline, n, handlers)
	if err != nil {
		return nil, err
	}
	type claimed struct {
		a        store.Assignment
		priority int32
		id       [16]byte
	}
	bucket := bucketOf(key)
	got, err := pgx.CollectRows(rows, func(row pgx.CollectableRow) (claimed, error) {
		var (
			c            claimed
			attempt      int32
			depth        int32
			parent       pgtype.UUID
			parentBucket *int16
			fp           []byte
			strained     int32
			created, due pgtype.Timestamptz
		)
		if err := row.Scan(&c.id, &attempt, &depth, &parent, &parentBucket, &fp, &c.priority, &c.a.Spec, &strained, &created, &due); err != nil {
			return c, err
		}
		c.a.Waited = max(now.Sub(latest(timeOf(created), timeOf(due))), 0)
		task := taskKey{bucket: bucket, day: b.meta.day, id: c.id}
		ref := leaseKey{task: task, attempt: uint32(attempt), nonce: nonce}.ref() //nolint:gosec // G115: attempts are positive
		c.a.Lease = store.Lease{Ref: ref, ExpiresAt: expires, Duration: lease.Duration}
		c.a.Info = store.LeaseInfo{Ref: ref, Task: task.ID(), Run: b.meta.spec.ID, RunPolicy: b.meta.spec.Policy,
			Queue: key, Pool: poolName, Fingerprint: store.Fingerprint(fp), Depth: uint32(depth), //nolint:gosec // G115: depths are not negative
			Parent: parentID(parent, parentBucket, b.meta.day), Worker: req.Worker, StrainAttempts: uint32(max(strained, 0))}
		return c, nil
	})
	if err != nil {
		return nil, err
	}
	// RETURNING has no order: hand the tasks out as they were claimed.
	slices.SortFunc(got, func(x, y claimed) int {
		return cmp.Or(cmp.Compare(y.priority, x.priority), bytes.Compare(x.id[:], y.id[:]))
	})
	out := make([]store.Assignment, len(got))
	for i := range got {
		out[i] = got[i].a
	}
	return out, nil
}

// parentID is the task ID of a task's parent, or empty for a seed.
func parentID(id pgtype.UUID, bucket *int16, day uint16) store.TaskID {
	if !id.Valid || bucket == nil {
		return ""
	}
	return taskKey{bucket: uint8(*bucket), day: day, id: id.Bytes}.ID() //nolint:gosec // G115: a bucket is below 256
}

// nextDue is when the next claimable task of the backlog's handler is due: the
// earliest not_before among its pending tasks that have not expired, or zero if there
// is none.
func (s *Store) nextDue(ctx context.Context, q querier, now time.Time, key store.QueueKey, b *brow, handler string) (time.Time, error) {
	var due pgtype.Timestamptz
	err := q.QueryRow(ctx, `SELECT not_before FROM tasks
		WHERE run_created_on = $1 AND run_no = $2 AND queue_key = $3 AND pool_id = $4 AND handler = $6
			AND state = 'pending' AND (expires_at IS NULL OR expires_at > $5)
		ORDER BY not_before LIMIT 1`, b.meta.createdOn, b.run, string(key), b.pool, now, handler).Scan(&due)
	if errors.Is(err, pgx.ErrNoRows) {
		return time.Time{}, nil
	}
	if err != nil {
		return time.Time{}, err
	}
	if t := timeOf(due); t.After(now) {
		return t, nil
	}
	return now, nil // due already: another transaction holds the row
}

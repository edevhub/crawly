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

// Dispatch leases up to req.Slots due tasks of the pool, fairly across runs and
// within the queue limits. It is one transaction per queue it dispatches from.
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
	rows, err := s.db.Query(ctx, `SELECT queue_key FROM (
			SELECT queue_key, ready_at FROM backlogs WHERE pool_id = $1 AND ready_at <= $2 ORDER BY ready_at LIMIT $3) c
		GROUP BY queue_key ORDER BY min(ready_at) LIMIT $4`, pool, now, candidateBacklogs, candidateQueues)
	if err != nil {
		return res, wrap(err)
	}
	keys, err := pgx.CollectRows(rows, pgx.RowTo[string])
	if err != nil {
		return res, wrap(err)
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
			got, err = s.dispatchQueue(ctx, tx, at, store.QueueKey(key), pool, req, slots)
			return err
		})
		if err != nil {
			return res, err // what was claimed so far is committed and is returned
		}
		res.Assignments = append(res.Assignments, got...)
		slots -= len(got)
	}

	// When to come back: the earliest readiness in the pool that is a time.
	var next pgtype.Timestamptz
	err = s.db.QueryRow(ctx, `SELECT ready_at FROM backlogs WHERE pool_id = $1 AND ready_at < 'infinity'
		ORDER BY ready_at LIMIT 1`, pool).Scan(&next)
	switch {
	case errors.Is(err, pgx.ErrNoRows):
		return res, nil // nothing is waiting for a time: a notification brings the next round
	case err != nil:
		return res, wrap(err)
	}
	switch at := timeOf(next); {
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

// dispatchQueue claims up to slots tasks of the pool from one queue.
func (s *Store) dispatchQueue(ctx context.Context, tx pgx.Tx, now time.Time, key store.QueueKey, pool int16,
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

	var out []store.Assignment
	var heldUntil time.Time // set when another pool has first refusal on the queue
	notify := map[int16]bool{}
	dispatched := false
	blocked := now.Before(q.blockedUntil)
	for taken := 0; !blocked && taken < slots; {
		var best *brow
		var bestStart float64
		var bestAdmit int
		mine := false
		for _, b := range bs {
			if !evaluated(b, now) || b.short || b.pending == 0 {
				continue
			}
			admit, _ := admission(q, b, now)
			if admit == 0 {
				continue
			}
			if b.pool == pool {
				mine = true
			}
			// The start tag is clamped to the queue's clock at pick time, so a backlog
			// that could not be picked for a while cannot bank credit.
			start := sched.Start(b.vtime, q.vclock)
			if best == nil || start < bestStart || (start == bestStart && before(b, best)) {
				best, bestStart, bestAdmit = b, start, admit
			}
		}
		if best == nil || !mine {
			break
		}
		if best.pool != pool {
			// Another pool holds the lowest start tag: it has first refusal, for a
			// period counted from when its turn came.
			since := now
			if !best.ready.Equal(forever) {
				since = best.ready
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
		got, err := s.claim(ctx, tx, now, key, best, req, want)
		if err != nil {
			return nil, err
		}
		if n := len(got); n > 0 {
			if best.eff.Own {
				best.ownTat = sched.Advance(best.eff.Limits, best.ownTat, now, n)
				q.tat = sched.AdvanceCapped(best.eff.Queue, q.tat, now, n)
			} else {
				q.tat = sched.Advance(best.eff.Queue, q.tat, now, n)
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
			if best.due, err = s.nextDue(ctx, tx, now, key, best); err != nil {
				return nil, err
			}
		}
	}

	// Readiness, for every backlog this round looked at: early is allowed, late is not.
	for _, b := range bs {
		if !evaluated(b, now) {
			continue
		}
		b.setReady(readiness(q, b, now, pool, dispatched, heldUntil))
	}
	if err := flush(ctx, tx, now, q, bs); err != nil {
		return nil, err
	}
	return out, s.wake(ctx, tx, notify)
}

// evaluated reports whether a dispatch round looks at the backlog: its run can be
// dispatched from, and its readiness has come or waits for an event.
func evaluated(b *brow, now time.Time) bool {
	return b.live && b.pending+int64(b.inFlight) > 0 && !b.ready.IsZero() && (!b.ready.After(now) || b.ready.Equal(forever))
}

// readiness is the value written back for a backlog a dispatch round looked at.
func readiness(q *qrow, b *brow, now time.Time, pool int16, dispatched bool, heldUntil time.Time) time.Time {
	switch {
	case b.pending == 0:
		return time.Time{} // nothing to dispatch
	case now.Before(q.blockedUntil):
		return q.blockedUntil
	case b.short && b.due.IsZero():
		return time.Time{} // every pending task has expired: Maintain removes them
	case b.short:
		return b.due
	}
	admit, at := admission(q, b, now)
	switch {
	case admit == 0 && at.After(now):
		return at // refused by rate: the exact politeness time
	case admit == 0:
		return forever // refused by concurrency: a Finish lowers it
	case b.pool == pool && !heldUntil.IsZero():
		return heldUntil // waits for another pool's first refusal
	case dispatched || b.ready.Equal(forever):
		// Admitted and not served. A first-refusal period on this backlog counts from
		// the queue's last dispatch.
		return now
	}
	return b.ready
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
		if !b.live && !b.ready.IsZero() {
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

// claim leases up to n due tasks of the backlog, highest priority first.
func (s *Store) claim(ctx context.Context, tx pgx.Tx, now time.Time, key store.QueueKey, b *brow, req store.DispatchRequest, n int) ([]store.Assignment, error) {
	lease := b.eff.Lease
	expires := now.Add(lease.Duration)
	deadline := latest(now.Add(lease.MaxDuration), expires)
	nonce := newNonce()
	poolName, err := s.poolOf(ctx, tx, b.pool)
	if err != nil {
		return nil, err
	}
	// The picked rows are materialised first: as a plain sub-select the planner may
	// run the LIMIT again for every outer row and claim more than n.
	rows, err := tx.Query(ctx, `WITH picked AS MATERIALIZED (
			SELECT task_id FROM tasks
			WHERE run_created_on = $1 AND run_no = $2 AND queue_key = $3 AND pool_id = $4
				AND state = 'pending' AND not_before <= $5 AND (expires_at IS NULL OR expires_at > $5)
			ORDER BY priority DESC, task_id LIMIT $11 FOR UPDATE SKIP LOCKED)
		UPDATE tasks t SET state = 'in_flight', attempt = t.attempt + 1, lease_nonce = $6,
			worker_id = $7, worker_version = $8, dispatched_at = $5, lease_expires_at = $9, lease_deadline = $10
		FROM picked WHERE t.run_created_on = $1 AND t.task_id = picked.task_id
		RETURNING t.task_id, t.attempt, t.depth, t.parent_task_id, t.parent_bucket, t.fingerprint, t.priority, t.spec`,
		b.meta.createdOn, b.run, string(key), b.pool, now, nonce, req.Worker.ID, req.Worker.Version, expires, deadline, n)
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
		)
		if err := row.Scan(&c.id, &attempt, &depth, &parent, &parentBucket, &fp, &c.priority, &c.a.Spec); err != nil {
			return c, err
		}
		task := taskKey{bucket: bucket, day: b.meta.day, id: c.id}
		ref := leaseKey{task: task, attempt: uint32(attempt), nonce: nonce}.ref() //nolint:gosec // G115: attempts are positive
		c.a.Lease = store.Lease{Ref: ref, ExpiresAt: expires, Duration: lease.Duration}
		c.a.Info = store.LeaseInfo{Ref: ref, Task: task.ID(), Run: b.meta.spec.ID, RunPolicy: b.meta.spec.Policy,
			Queue: key, Pool: poolName, Fingerprint: store.Fingerprint(fp), Depth: uint32(depth), //nolint:gosec // G115: depths are not negative
			Parent: parentID(parent, parentBucket, b.meta.day)}
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

// nextDue is when the backlog's next claimable task is due: the earliest not_before
// among its pending tasks that have not expired, or zero if there is none.
func (s *Store) nextDue(ctx context.Context, q querier, now time.Time, key store.QueueKey, b *brow) (time.Time, error) {
	var due pgtype.Timestamptz
	err := q.QueryRow(ctx, `SELECT not_before FROM tasks
		WHERE run_created_on = $1 AND run_no = $2 AND queue_key = $3 AND pool_id = $4
			AND state = 'pending' AND (expires_at IS NULL OR expires_at > $5)
		ORDER BY not_before LIMIT 1`, b.meta.createdOn, b.run, string(key), b.pool, now).Scan(&due)
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

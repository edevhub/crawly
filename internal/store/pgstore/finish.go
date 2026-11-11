package pgstore

import (
	"context"
	"errors"
	"fmt"
	"math"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgtype"

	"github.com/edevhub/crawly/internal/store"
)

// Values of attempts.result.
const (
	resultSucceeded    = 1
	resultFailed       = 2
	resultRetry        = 3
	resultLeaseExpired = 4
	resultRevoked      = 5
)

// codeRevoked is the outcome code of an attempt that ended because its run did.
const codeRevoked = "revoked"

// leased is a task row as the lease operations read it.
type leased struct {
	key          taskKey
	run          int64
	queue        store.QueueKey
	pool         int16
	state        string
	attempt      uint32
	nonce        int64
	expires      time.Time
	deadline     time.Time
	worker       store.WorkerInfo
	dispatched   time.Time
	depth        uint32
	parent       pgtype.UUID
	parentBucket *int16
	fingerprint  store.Fingerprint
}

const leasedColumns = `run_created_on, task_id, run_no, queue_key, pool_id, state::text, attempt, coalesce(lease_nonce, 0),
	lease_expires_at, lease_deadline, coalesce(worker_id, ''), coalesce(worker_version, ''), dispatched_at,
	depth, parent_task_id, parent_bucket, fingerprint`

func scanLeased(row pgx.Row) (*leased, error) {
	var (
		t                             leased
		day                           time.Time
		queue                         string
		attempt, depth                int32
		expires, deadline, dispatched pgtype.Timestamptz
		fp                            []byte
	)
	if err := row.Scan(&day, &t.key.id, &t.run, &queue, &t.pool, &t.state, &attempt, &t.nonce, &expires, &deadline,
		&t.worker.ID, &t.worker.Version, &dispatched, &depth, &t.parent, &t.parentBucket, &fp); err != nil {
		return nil, err
	}
	t.queue = store.QueueKey(queue)
	t.key.bucket, t.key.day = bucketOf(t.queue), dayOf(day)
	t.attempt, t.depth = uint32(attempt), uint32(depth) //nolint:gosec // G115: neither is negative
	t.expires, t.deadline, t.dispatched = timeOf(expires), timeOf(deadline), timeOf(dispatched)
	t.fingerprint = store.Fingerprint(fp)
	return &t, nil
}

// taskByLease reads the task a lease ID names, or nil if there is none. lock is a
// row-lock clause, or empty.
func taskByLease(ctx context.Context, q querier, lk leaseKey, lock string) (*leased, error) {
	t, err := scanLeased(q.QueryRow(ctx, "SELECT "+leasedColumns+" FROM tasks WHERE run_created_on = $1 AND task_id = $2 "+lock,
		lk.task.date(), lk.task.id))
	if errors.Is(err, pgx.ErrNoRows) {
		return nil, nil //nolint:nilnil // no row is an answer here, not a failure
	}
	return t, err
}

// holds reports whether the lease is the task's current one. Liveness is the
// caller's check.
func (t *leased) holds(lk leaseKey, ref store.LeaseRef) bool {
	return t.state == taskInFlight && t.attempt == lk.attempt && lk.attempt == ref.Attempt && t.nonce == lk.nonce
}

func (t *leased) lease() leaseKey { return leaseKey{task: t.key, attempt: t.attempt, nonce: t.nonce} }

func (s *Store) info(ctx context.Context, q querier, t *leased) (store.LeaseInfo, error) {
	m, err := s.meta(ctx, q, t.run)
	if err != nil {
		return store.LeaseInfo{}, err
	}
	pool, err := s.poolOf(ctx, q, t.pool)
	if err != nil {
		return store.LeaseInfo{}, err
	}
	return store.LeaseInfo{Ref: t.lease().ref(), Task: t.key.ID(), Run: m.spec.ID, RunPolicy: m.spec.Policy,
		Queue: t.queue, Pool: pool, Fingerprint: t.fingerprint, Depth: t.depth,
		Parent: parentID(t.parent, t.parentBucket, t.key.day)}, nil
}

// Lease returns the context of the task's current lease, or store.ErrNotFound if the
// lease is stale or unknown. It is one primary key read and changes nothing.
func (s *Store) Lease(ctx context.Context, ref store.LeaseRef) (store.LeaseInfo, error) {
	lk, ok := parseLease(ref.ID)
	if !ok {
		return store.LeaseInfo{}, store.ErrNotFound
	}
	t, err := taskByLease(ctx, s.db, lk, "")
	if err != nil {
		return store.LeaseInfo{}, wrap(err)
	}
	if t == nil || !t.holds(lk, ref) {
		return store.LeaseInfo{}, store.ErrNotFound
	}
	info, err := s.info(ctx, s.db, t)
	return info, wrap(err)
}

// ExtendLease moves the lease expiry to now plus by, or the lease duration if by is
// not positive, capped by the lease deadline.
func (s *Store) ExtendLease(ctx context.Context, ref store.LeaseRef, by time.Duration) (store.Lease, store.FinishStatus, error) {
	lk, ok := parseLease(ref.ID)
	if !ok {
		return store.Lease{}, store.FinishUnknown, nil
	}
	var lease store.Lease
	status := store.FinishUnknown
	err := s.tx(ctx, func(tx pgx.Tx, now time.Time) error {
		lease, status = store.Lease{}, store.FinishUnknown
		t, err := taskByLease(ctx, tx, lk, "FOR UPDATE")
		if err != nil || t == nil {
			return err
		}
		status = store.FinishStale
		if !t.holds(lk, ref) || !now.Before(t.expires) {
			return nil
		}
		m, err := s.meta(ctx, tx, t.run)
		if errors.Is(err, store.ErrNotFound) {
			return nil
		}
		if err != nil {
			return err
		}
		var name string
		if err := tx.QueryRow(ctx, "SELECT state::text FROM runs WHERE run_no = $1", t.run).Scan(&name); err != nil {
			if errors.Is(err, pgx.ErrNoRows) {
				return nil
			}
			return err
		}
		if runStates[name].Ended() {
			return nil // the run ended: its leases are revoked
		}
		if by <= 0 {
			by = s.pol.For(m.spec.ID, m.spec.Policy, t.queue).Lease.Duration
		}
		expires := now.Add(by)
		if !t.deadline.IsZero() && expires.After(t.deadline) {
			expires = t.deadline
		}
		if _, err := tx.Exec(ctx, "UPDATE tasks SET lease_expires_at = $3 WHERE run_created_on = $1 AND task_id = $2",
			lk.task.date(), lk.task.id, expires); err != nil {
			return err
		}
		lease, status = store.Lease{Ref: ref, ExpiresAt: expires, Duration: expires.Sub(now)}, store.FinishAccepted
		return nil
	})
	if err != nil {
		return store.Lease{}, store.FinishUnknown, err
	}
	return lease, status, nil
}

// Finish applies the transition the core decided for an attempt, unless the lease is
// stale. An attempt of a run that was cancelled or expired meanwhile is recorded as
// revoked and reported as stale: its result is not applied.
func (s *Store) Finish(ctx context.Context, f store.Finish) (store.FinishStatus, error) {
	lk, ok := parseLease(f.Lease.ID)
	if !ok {
		return store.FinishUnknown, nil
	}
	status := store.FinishUnknown
	err := s.tx(ctx, func(tx pgx.Tx, now time.Time) error {
		var err error
		status, err = s.finishTx(ctx, tx, now, lk, f)
		return err
	})
	if err != nil {
		return store.FinishUnknown, err
	}
	return status, nil
}

func (s *Store) finishTx(ctx context.Context, tx pgx.Tx, now time.Time, lk leaseKey, f store.Finish) (store.FinishStatus, error) {
	// 1. The task, by its lease.
	t, err := taskByLease(ctx, tx, lk, "FOR UPDATE")
	if err != nil {
		return store.FinishUnknown, err
	}
	if t == nil {
		return store.FinishUnknown, nil
	}
	if !t.holds(lk, f.Lease) {
		return store.FinishStale, nil
	}
	// A worker needs a live lease, the reaper an expired one.
	if live := now.Before(t.expires); live != (f.Source == store.SourceWorker) {
		return store.FinishStale, nil
	}

	// 2. The queue, then its backlogs with pending tasks and the task's own: a freed
	// slot can make any of them ready.
	q, err := lockQueue(ctx, tx, t.queue, false)
	if err != nil {
		return store.FinishUnknown, err
	}
	if q == nil {
		return store.FinishUnknown, fmt.Errorf("pgstore: task %s has no queue row for %q", t.key.ID(), t.queue)
	}
	bs, err := s.lockBacklogs(ctx, tx, t.queue, t.run, t.pool)
	if err != nil {
		return store.FinishUnknown, err
	}
	var own *brow
	for _, b := range bs {
		if b.run == t.run && b.pool == t.pool {
			own = b
		}
	}
	if own == nil {
		return store.FinishUnknown, fmt.Errorf("pgstore: task %s has no backlog row", t.key.ID())
	}

	// 3. The transition.
	status := store.FinishAccepted
	ended := own.state == store.RunCancelled || own.state == store.RunExpired
	own.inFlight--
	own.dirty = true
	q.inFlight--
	var due time.Time // when the task can be dispatched again, if it can
	record, result := f.Record, 0
	batch := &pgx.Batch{}
	switch {
	case ended:
		final := taskCancelled
		if own.state == store.RunExpired {
			final = taskExpired
			own.expired++
		} else {
			own.cancelled++
		}
		closeTask(batch, t, final, now)
		record, result = store.AttemptRecord{Code: codeRevoked, PolicySet: f.Record.PolicySet}, resultRevoked
		status = store.FinishStale
	case f.Result == store.ResultReleased:
		// The assignment never reached a worker: the attempt did not happen. The
		// queue's politeness state stays as dispatch advanced it.
		due = now
		own.attempts--
		reopenTask(batch, t, due, 1)
	case f.Result == store.ResultRetry:
		due = now.Add(f.RetryIn)
		reopenTask(batch, t, due, 0)
		result = resultRetry
	case f.Result == store.ResultFailed:
		own.failed++
		closeTask(batch, t, taskFailed, now)
		result = resultFailed
	default:
		own.succeeded++
		closeTask(batch, t, taskSucceeded, now)
		result = resultSucceeded
	}
	if f.Source == store.SourceReaper && result != resultRevoked {
		result = resultLeaseExpired
	}
	if !due.IsZero() {
		own.pending++
		own.ready = earliest(own.ready, due)
	}
	if result != 0 {
		recordAttempt(batch, t, now, result, record)
	}
	if !ended && f.Result != store.ResultReleased {
		if f.Block > 0 {
			if until := now.Add(f.Block); until.After(q.blockedUntil) {
				q.blockedUntil, q.blockReason = until, f.Record.Code
			}
			own.blocks++
		}
		if f.Record.Code != "" {
			batch.Queue(`INSERT INTO backlog_codes (queue_key, run_no, code, count) VALUES ($1, $2, $3, 1)
				ON CONFLICT (queue_key, run_no, code) DO UPDATE SET count = backlog_codes.count + 1`, string(t.queue), t.run, f.Record.Code)
		}
	}
	if err := tx.SendBatch(ctx, batch).Close(); err != nil {
		return store.FinishUnknown, err
	}

	// 4. Readiness of the queue's backlogs after the slot was freed or a block set.
	refresh(q, bs, now)
	if err := flush(ctx, tx, now, q, bs); err != nil {
		return store.FinishUnknown, err
	}
	if own.pending == 0 && own.inFlight == 0 && !ended {
		if err := s.complete(ctx, tx, t.run, now); err != nil {
			return store.FinishUnknown, err
		}
	}
	return status, s.wakeWaiting(ctx, tx, bs)
}

// refresh brings the readiness of a queue's locked backlogs up to date after a slot
// was freed or a block was set: a backlog that waited for a slot gets its next
// admission time, and readiness is raised to the end of a block.
func refresh(q *qrow, bs []*brow, now time.Time) {
	blocked := now.Before(q.blockedUntil)
	for _, b := range bs {
		switch {
		case b.pending == 0:
			b.setReady(time.Time{})
		case !b.live || b.ready.IsZero():
			// A paused or ended run, or a backlog a resume sweep has yet to reach.
		case blocked:
			if b.ready.Equal(forever) || b.ready.Before(q.blockedUntil) {
				b.setReady(q.blockedUntil)
			}
		case b.ready.Equal(forever):
			if admit, at := admission(q, b, now); admit > 0 {
				b.setReady(now)
			} else if at.After(now) {
				b.setReady(at)
			}
		}
	}
}

// closeTask moves a task to a final state.
func closeTask(batch *pgx.Batch, t *leased, state string, now time.Time) {
	batch.Queue(`UPDATE tasks SET state = $3::task_state, finished_at = $4, lease_nonce = NULL, lease_expires_at = NULL, lease_deadline = NULL
		WHERE run_created_on = $1 AND task_id = $2`, t.key.date(), t.key.id, state, now)
}

// reopenTask returns a task to its backlog, due at the given time. undo is 1 when
// the attempt did not happen and its number is to be used again.
func reopenTask(batch *pgx.Batch, t *leased, due time.Time, undo int) {
	batch.Queue(`UPDATE tasks SET state = 'pending', not_before = $3, attempt = attempt - $4,
			lease_nonce = NULL, lease_expires_at = NULL, lease_deadline = NULL
		WHERE run_created_on = $1 AND task_id = $2`, t.key.date(), t.key.id, due, undo)
}

// recordAttempt appends the attempt row.
func recordAttempt(batch *pgx.Batch, t *leased, now time.Time, result int, r store.AttemptRecord) {
	batch.Queue(`INSERT INTO attempts (run_created_on, task_id, attempt, run_no, worker_id, worker_version, policy_hash,
			dispatched_at, finished_at, result, kind, code, message, http_status, final_url, latency_ms, bytes,
			response_ref, content_hash, items_emitted)
		VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14, $15, $16, $17, $18, $19, $20)
		ON CONFLICT (run_created_on, task_id, attempt) DO NOTHING`,
		t.key.date(), t.key.id, int64(t.attempt), t.run, t.worker.ID, t.worker.Version, r.PolicySet,
		tsz(t.dispatched), now, result, r.Kind, nullString(r.Code), nullString(r.Message), int64(r.HTTPStatus), nullString(r.FinalURL),
		min(r.Latency.Milliseconds(), math.MaxInt32), int64(min(r.Bytes, math.MaxInt64)),
		nullString(r.ResponseRef), nullString(r.ContentHash), int64(r.ItemsEmitted))
}

// maxExpired bounds one listing of expired leases when the caller sets no limit.
const maxExpired = 1000

// ExpiredLeases returns up to limit leases past their expiry, oldest first. It takes
// no locks: the reaper finishes each lease, and fencing settles a race with a late
// completion.
func (s *Store) ExpiredLeases(ctx context.Context, limit int) ([]store.LeaseInfo, error) {
	if limit <= 0 || limit > maxExpired {
		limit = maxExpired
	}
	now, err := s.Now(ctx)
	if err != nil {
		return nil, err
	}
	rows, err := s.db.Query(ctx, "SELECT "+leasedColumns+` FROM tasks
		WHERE state = 'in_flight' AND lease_expires_at <= $1 ORDER BY lease_expires_at LIMIT $2`, now, limit)
	if err != nil {
		return nil, wrap(err)
	}
	tasks, err := pgx.CollectRows(rows, func(row pgx.CollectableRow) (*leased, error) { return scanLeased(row) })
	if err != nil {
		return nil, wrap(err)
	}
	out := make([]store.LeaseInfo, 0, len(tasks))
	for _, t := range tasks {
		info, err := s.info(ctx, s.db, t)
		if errors.Is(err, store.ErrNotFound) {
			continue // the run was deleted
		}
		if err != nil {
			return nil, wrap(err)
		}
		out = append(out, info)
	}
	return out, nil
}

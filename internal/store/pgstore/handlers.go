package pgstore

import (
	"cmp"
	"context"
	"fmt"
	"slices"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgtype"

	taskv1 "github.com/edevhub/crawly/internal/gen/frontier/task/v1"
	"github.com/edevhub/crawly/internal/store"
)

// ServeHandlers records that this replica has sessions for the handlers.
func (s *Store) ServeHandlers(ctx context.Context, sets []store.PoolHandlers) error {
	var pools, handlers []string
	for _, set := range sets {
		for _, h := range set.Handlers {
			pools, handlers = append(pools, string(set.Pool)), append(handlers, h)
		}
	}
	if len(handlers) == 0 {
		return nil
	}
	now, err := s.Now(ctx)
	if err != nil {
		return err
	}
	// In key order, so that replicas that report the same handlers do not deadlock.
	_, err = s.db.Exec(ctx, `INSERT INTO pool_handlers (pool, handler, seen_at)
		SELECT u.pool, u.handler, $3 FROM unnest($1::text[], $2::text[]) AS u(pool, handler) ORDER BY u.pool, u.handler
		ON CONFLICT (pool, handler) DO UPDATE SET seen_at = EXCLUDED.seen_at WHERE pool_handlers.seen_at < EXCLUDED.seen_at`,
		pools, handlers, now)
	return wrap(err)
}

// sweepLane is a lane whose handler no replica has reported lately.
type sweepLane struct {
	queue   string
	run     int64
	pool    int16
	name    store.PoolName
	handler string
}

// SweepUnserved examines the lanes whose handler has had no session for grace, drops
// the ones that have no tasks, fails the tasks that waited as long, and reports what
// still waits. A task waits from its creation, and nothing fails before grace has
// passed since the newest replica started: a restart of every replica must not fail
// tasks whose workers have not reconnected yet.
func (s *Store) SweepUnserved(ctx context.Context, grace time.Duration) (store.Unserved, error) {
	var out store.Unserved
	now, err := s.Now(ctx)
	if err != nil {
		return out, err
	}
	var newest pgtype.Timestamptz
	if err := s.db.QueryRow(ctx, "SELECT max(started_at) FROM replicas").Scan(&newest); err != nil {
		return out, wrap(err)
	}
	mayFail := !timeOf(newest).After(now.Add(-grace))
	// Lanes of paused runs have no readiness and are left alone.
	rows, err := s.db.Query(ctx, `SELECT l.queue_key, l.run_no, l.pool_id, p.name, l.handler
		FROM lanes l JOIN pools p USING (pool_id)
		WHERE l.handler <> '' AND l.ready_at IS NOT NULL
			AND NOT EXISTS (SELECT 1 FROM pool_handlers h WHERE h.pool = p.name AND h.handler = l.handler AND h.seen_at > $1)
		ORDER BY l.queue_key, l.run_no, l.pool_id, l.handler LIMIT $2`, now.Add(-grace), maintainBacklogs)
	if err != nil {
		return out, wrap(err)
	}
	lanes, err := pgx.CollectRows(rows, func(row pgx.CollectableRow) (sweepLane, error) {
		var l sweepLane
		var name string
		err := row.Scan(&l.queue, &l.run, &l.pool, &name, &l.handler)
		l.name = store.PoolName(name)
		return l, err
	})
	if err != nil {
		return out, wrap(err)
	}
	waiting := map[store.HandlerRef]bool{}
	failed := map[store.HandlerRef]int{}
	for _, l := range lanes {
		ref := store.HandlerRef{Pool: l.name, Handler: l.handler}
		var n int
		var left bool
		err := s.tx(ctx, func(tx pgx.Tx, at time.Time) error {
			var err error
			n, left, err = s.sweepLane(ctx, tx, at, l, grace, mayFail)
			return err
		})
		if err != nil {
			return out, err
		}
		if n > 0 {
			failed[ref] += n
		}
		if left {
			waiting[ref] = true
		}
	}
	for ref := range waiting {
		out.Waiting = append(out.Waiting, ref)
	}
	for ref, n := range failed {
		out.Failed = append(out.Failed, store.FailedTasks{HandlerRef: ref, Tasks: n})
	}
	cmpRef := func(a, b store.HandlerRef) int {
		return cmp.Or(cmp.Compare(a.Pool, b.Pool), cmp.Compare(a.Handler, b.Handler))
	}
	slices.SortFunc(out.Waiting, cmpRef)
	slices.SortFunc(out.Failed, func(a, b store.FailedTasks) int { return cmpRef(a.HandlerRef, b.HandlerRef) })
	return out, nil
}

// failedTask is a task the sweep failed.
type failedTask struct {
	id      [16]byte
	attempt int32
}

// sweepLane handles one lane in a transaction of its own. It returns how many tasks it
// failed and whether the lane still has pending tasks.
func (s *Store) sweepLane(ctx context.Context, tx pgx.Tx, now time.Time, l sweepLane, grace time.Duration, mayFail bool) (int, bool, error) {
	key := store.QueueKey(l.queue)
	q, err := lockQueue(ctx, tx, key, true)
	if err != nil || q == nil {
		return 0, true, err // held by a dispatch or a finish: the next sweep takes it
	}
	bs, err := s.lockBacklogs(ctx, tx, key, l.run, l.pool)
	if err != nil {
		return 0, false, err
	}
	var own *brow
	for _, b := range bs {
		if b.run == l.run && b.pool == l.pool {
			own = b
		}
	}
	if own == nil || own.meta == nil || !own.live {
		return 0, false, nil
	}
	pending := func() (bool, error) {
		var left bool
		err := tx.QueryRow(ctx, `SELECT EXISTS (SELECT 1 FROM tasks WHERE run_created_on = $1 AND run_no = $2 AND queue_key = $3
			AND pool_id = $4 AND handler = $5 AND state = 'pending')`, own.meta.createdOn, l.run, l.queue, l.pool, l.handler).Scan(&left)
		return left, err
	}
	left, err := pending()
	if err != nil {
		return 0, false, err
	}
	ln := own.laneOf(l.handler)
	var failed []failedTask
	if left && mayFail {
		rows, err := tx.Query(ctx, `WITH picked AS MATERIALIZED (SELECT task_id FROM tasks
				WHERE run_created_on = $1 AND run_no = $2 AND queue_key = $3 AND pool_id = $4 AND handler = $5
					AND state = 'pending' AND created_at <= $6
				LIMIT $7 FOR UPDATE SKIP LOCKED)
			UPDATE tasks t SET state = 'failed', finished_at = $8, attempt = t.attempt + 1
			FROM picked WHERE t.run_created_on = $1 AND t.task_id = picked.task_id
			RETURNING t.task_id, t.attempt`,
			own.meta.createdOn, l.run, l.queue, l.pool, l.handler, now.Add(-grace), maintainTasks, now)
		if err != nil {
			return 0, false, err
		}
		failed, err = pgx.CollectRows(rows, func(row pgx.CollectableRow) (failedTask, error) {
			var f failedTask
			err := row.Scan(&f.id, &f.attempt)
			return f, err
		})
		if err != nil {
			return 0, false, err
		}
	}
	if n := int64(len(failed)); n > 0 {
		// The attempt that explains the failure. It was never dispatched, so the
		// backlog's count of attempts does not change.
		batch := &pgx.Batch{}
		rec := store.AttemptRecord{Kind: int32(taskv1.Outcome_KIND_FAILED), Code: store.CodeNoHandler, PolicySet: s.pol.Hash(),
			Message: fmt.Sprintf("no worker had the handler %q for %v", l.handler, grace)}
		for _, f := range failed {
			t := &leased{key: taskKey{bucket: bucketOf(key), day: own.meta.day, id: f.id}, run: l.run,
				attempt: uint32(f.attempt), dispatched: now} //nolint:gosec // G115: attempts are positive
			recordAttempt(batch, t, now, resultFailed, rec)
		}
		batch.Queue(`INSERT INTO backlog_codes (queue_key, run_no, code, count) VALUES ($1, $2, $3, $4)
			ON CONFLICT (queue_key, run_no, code) DO UPDATE SET count = backlog_codes.count + EXCLUDED.count`, l.queue, l.run, store.CodeNoHandler, n)
		if err := tx.SendBatch(ctx, batch).Close(); err != nil {
			return 0, false, err
		}
		own.pending = max(own.pending-n, 0)
		own.failed += n
		own.dirty = true
		if left, err = pending(); err != nil {
			return 0, false, err
		}
	}
	if !left {
		ln.gone = true // the lane is stale, or the sweep emptied it
	}
	if len(failed) == 0 && left {
		return 0, true, nil // nothing changed
	}
	refresh(q, bs, now)
	if err := flush(ctx, tx, now, q, bs); err != nil {
		return 0, false, err
	}
	if len(failed) > 0 && own.pending == 0 && own.inFlight == 0 {
		if err := s.complete(ctx, tx, l.run, now); err != nil {
			return 0, false, err
		}
	}
	return len(failed), left, nil
}

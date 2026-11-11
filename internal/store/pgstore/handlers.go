package pgstore

import (
	"cmp"
	"context"
	"errors"
	"fmt"
	"maps"
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

// sweepCursor is where a round of the sweep stopped: the handler it was on, and the
// last of that handler's lanes it looked at, in the order of the index lanes_ready.
type sweepCursor struct {
	pool    int16
	handler string
	ready   time.Time
	queue   string
	run     int64
}

// unservedPair is a handler of a pool that has lanes and no session.
type unservedPair struct {
	pool    int16
	name    store.PoolName
	handler string
}

// SweepUnserved is one bounded round over the lanes whose handler no replica has
// reported for grace: it fails the tasks of those lanes that waited as long and
// reports what waits. It finds the handlers first, from the few (pool, handler) pairs
// that have lanes, and walks the lanes of the ones that are unserved from where the
// last round stopped, so that a round costs the same however many lanes exist and
// every lane is reached. The store keeps the position and the time of the last round:
// one replica at a time does the work, at most every quarter of grace (and a minute).
// A task waits from its creation, and nothing fails before grace has passed since the
// newest replica started: a restart of every replica must not fail tasks whose
// workers have not reconnected yet.
func (s *Store) SweepUnserved(ctx context.Context, grace time.Duration) (store.Unserved, error) {
	var out store.Unserved
	now, err := s.Now(ctx)
	if err != nil {
		return out, err
	}
	every := min(grace/4, time.Minute)
	var cur sweepCursor
	var ready pgtype.Timestamptz
	err = s.db.QueryRow(ctx, `UPDATE unserved_sweep SET swept_at = $1 WHERE swept_at <= $2
		RETURNING cur_pool, cur_handler, cur_ready, cur_queue, cur_run`, now, now.Add(-every)).
		Scan(&cur.pool, &cur.handler, &ready, &cur.queue, &cur.run)
	if errors.Is(err, pgx.ErrNoRows) {
		return out, nil // another replica swept lately
	}
	if err != nil {
		return out, wrap(err)
	}
	cur.ready = timeOf(ready)

	var newest pgtype.Timestamptz
	if err := s.db.QueryRow(ctx, "SELECT max(started_at) FROM replicas").Scan(&newest); err != nil {
		return out, wrap(err)
	}
	mayFail := !timeOf(newest).After(now.Add(-grace))
	pairs, err := s.unservedPairs(ctx, now.Add(-grace))
	if err != nil {
		return out, err
	}
	failed, next, err := s.sweepRound(ctx, pairs, cur, grace, mayFail)
	if err != nil {
		return out, err
	}
	// What still waits, counted after the round.
	for _, p := range pairs {
		n, err := s.countLanes(ctx, p)
		if err != nil {
			return out, err
		}
		if n > 0 {
			out.Waiting = append(out.Waiting, store.WaitingHandler{HandlerRef: store.HandlerRef{Pool: p.name, Handler: p.handler}, Lanes: n})
		}
	}
	if _, err := s.db.Exec(ctx, `UPDATE unserved_sweep SET cur_pool = $1, cur_handler = $2, cur_ready = $3, cur_queue = $4, cur_run = $5
		WHERE id = 1`, next.pool, next.handler, next.ready, next.queue, next.run); err != nil {
		return out, wrap(err)
	}
	for ref, n := range failed {
		out.Failed = append(out.Failed, store.FailedTasks{HandlerRef: ref, Tasks: n})
	}
	slices.SortFunc(out.Failed, func(a, b store.FailedTasks) int { return cmpRef(a.HandlerRef, b.HandlerRef) })

	out.Held = !mayFail && len(out.Waiting) > 0
	// One caller in a while is asked to log what waits.
	err = s.db.QueryRow(ctx, `UPDATE unserved_sweep SET reported_at = $1 WHERE reported_at <= $2 RETURNING true`,
		now, now.Add(-min(grace, 10*time.Minute))).Scan(&out.Report)
	if err != nil && !errors.Is(err, pgx.ErrNoRows) {
		return out, wrap(err)
	}
	return out, nil
}

func cmpRef(a, b store.HandlerRef) int {
	return cmp.Or(cmp.Compare(a.Pool, b.Pool), cmp.Compare(a.Handler, b.Handler))
}

// unservedPairsSQL skips through lanes_ready from one (pool, handler) to the next, and
// keeps the pairs that have no session since $1. A session that names no handlers is
// recorded as the handler ”: it serves all of its pool.
const unservedPairsSQL = `WITH RECURSIVE pairs AS (
		(SELECT pool_id, handler FROM lanes WHERE ready_at IS NOT NULL ORDER BY pool_id, handler LIMIT 1)
		UNION ALL
		SELECT n.pool_id, n.handler FROM pairs p CROSS JOIN LATERAL (
			SELECT pool_id, handler FROM lanes WHERE ready_at IS NOT NULL AND (pool_id, handler) > (p.pool_id, p.handler)
			ORDER BY pool_id, handler LIMIT 1) n)
	SELECT p.pool_id, pl.name, p.handler FROM pairs p JOIN pools pl USING (pool_id)
	WHERE p.handler <> '' AND NOT EXISTS (SELECT 1 FROM pool_handlers h
		WHERE h.pool = pl.name AND h.handler IN (p.handler, '') AND h.seen_at > $1)
	ORDER BY p.pool_id, p.handler`

// sweepLanesSQL reads a page of one handler's lanes from the cursor ($3, $4, $5) on, in
// the order of lanes_ready.
const sweepLanesSQL = `SELECT queue_key, run_no, ready_at FROM lanes
	WHERE pool_id = $1 AND handler = $2 AND ready_at IS NOT NULL AND (ready_at, queue_key, run_no) > ($3, $4, $5)
	ORDER BY ready_at, queue_key, run_no LIMIT $6`

// unservedPairs lists the handlers that have lanes and no session since the cut-off,
// by skipping from one (pool, handler) to the next in lanes_ready. A session that
// names no handlers is recorded as the handler ”: it serves all of its pool.
func (s *Store) unservedPairs(ctx context.Context, since time.Time) ([]unservedPair, error) {
	rows, err := s.db.Query(ctx, unservedPairsSQL, since)
	if err != nil {
		return nil, wrap(err)
	}
	return pgx.CollectRows(rows, func(row pgx.CollectableRow) (unservedPair, error) {
		var p unservedPair
		var name string
		err := row.Scan(&p.pool, &name, &p.handler)
		p.name = store.PoolName(name)
		return p, err
	})
}

// laneCountBound is where the count of waiting lanes stops.
const laneCountBound = 10000

// countLanes counts the lanes of a pair that wait, up to laneCountBound.
func (s *Store) countLanes(ctx context.Context, p unservedPair) (int, error) {
	var n int
	err := s.db.QueryRow(ctx, `SELECT count(*) FROM (SELECT 1 FROM lanes WHERE pool_id = $1 AND handler = $2
		AND ready_at IS NOT NULL LIMIT $3) c`, p.pool, p.handler, laneCountBound).Scan(&n)
	return n, wrap(err)
}

// sweepRound visits up to maintainBacklogs lanes of the unserved pairs, from the
// cursor on, and returns what it failed and where to resume. The cursor is zero when
// the round reached the end: the next one starts again.
func (s *Store) sweepRound(ctx context.Context, pairs []unservedPair, cur sweepCursor, grace time.Duration, mayFail bool) (map[store.HandlerRef]int, sweepCursor, error) {
	failed := map[store.HandlerRef]int{}
	budget := maintainBacklogs
	for _, p := range pairs {
		if p.pool < cur.pool || (p.pool == cur.pool && p.handler < cur.handler) {
			continue // before the cursor
		}
		from := sweepCursor{}
		if p.pool == cur.pool && p.handler == cur.handler {
			from = cur
		}
		for {
			rows, err := s.db.Query(ctx, sweepLanesSQL, p.pool, p.handler, from.ready, from.queue, from.run, min(budget, 50))
			if err != nil {
				return failed, cur, wrap(err)
			}
			batch, err := pgx.CollectRows(rows, func(row pgx.CollectableRow) (visit, error) {
				v := visit{lane: sweepLane{pool: p.pool, name: p.name, handler: p.handler}}
				var ready pgtype.Timestamptz
				err := row.Scan(&v.lane.queue, &v.lane.run, &ready)
				v.ready = timeOf(ready)
				return v, err
			})
			if err != nil {
				return failed, cur, wrap(err)
			}
			if len(batch) == 0 {
				break // this handler is done
			}
			for _, v := range batch {
				l := v.lane
				for budget > 0 {
					budget--
					var n int
					var left bool
					err := s.tx(ctx, func(tx pgx.Tx, at time.Time) error {
						var err error
						n, left, err = s.sweepLane(ctx, tx, at, l, grace, mayFail)
						return err
					})
					if err != nil {
						return failed, cur, err
					}
					if n > 0 {
						failed[store.HandlerRef{Pool: l.name, Handler: l.handler}] += n
					}
					if n < maintainTasks || !left {
						break // the lane is done, or has only tasks that are too young
					}
				}
				from = sweepCursor{pool: p.pool, handler: p.handler, ready: v.ready, queue: l.queue, run: l.run}
				if budget <= 0 {
					return failed, from, nil
				}
			}
		}
	}
	return failed, sweepCursor{}, nil
}

// visit is a lane the sweep looks at, with the readiness that orders it.
type visit struct {
	lane  sweepLane
	ready time.Time
}

// failedTask is a task the sweep failed.
type failedTask struct {
	id      [16]byte
	attempt int32
	label   string
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
			RETURNING t.task_id, t.attempt, t.label`,
			own.meta.createdOn, l.run, l.queue, l.pool, l.handler, now.Add(-grace), maintainTasks, now)
		if err != nil {
			return 0, false, err
		}
		failed, err = pgx.CollectRows(rows, func(row pgx.CollectableRow) (failedTask, error) {
			var f failedTask
			err := row.Scan(&f.id, &f.attempt, &f.label)
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
		perLabel := map[string]int64{}
		for _, f := range failed {
			perLabel[f.label]++
		}
		for _, label := range slices.Sorted(maps.Keys(perLabel)) {
			countFailed(batch, l.queue, l.run, label, store.CodeNoHandler, perLabel[label])
		}
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

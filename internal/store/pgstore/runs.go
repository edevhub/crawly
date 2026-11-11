package pgstore

import (
	"bytes"
	"context"
	"errors"
	"math"
	"strconv"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgtype"
	"google.golang.org/protobuf/proto"

	"github.com/edevhub/crawly/internal/sched"
	"github.com/edevhub/crawly/internal/store"
)

func schedAdmitted(l store.Limits, tat, now time.Time, inFlight int) (int, time.Time) {
	return sched.Admitted(l, tat, now, inFlight), sched.EligibleAt(l, tat)
}

// CreateRun creates the run, or returns the existing one when the spec is identical.
func (s *Store) CreateRun(ctx context.Context, spec store.RunSpec) (store.Run, error) {
	pol := []byte{} // no policy is stored as an empty value, not NULL
	if spec.Policy != nil {
		var err error
		if pol, err = proto.Marshal(spec.Policy); err != nil {
			return store.Run{}, err
		}
	}
	labels := spec.Labels
	if labels == nil {
		labels = map[string]string{}
	}
	var maxTasks *int64
	if spec.Limits.MaxTasks > 0 {
		n := int64(min(spec.Limits.MaxTasks, math.MaxInt64))
		maxTasks = &n
	}
	var maxDepth *int32
	if spec.Limits.MaxDepth > 0 {
		n := int32(min(spec.Limits.MaxDepth, math.MaxInt32))
		maxDepth = &n
	}
	now, err := s.Now(ctx)
	if err != nil {
		return store.Run{}, err
	}
	if err := s.ensureMonths(ctx, now); err != nil {
		return store.Run{}, wrap(err)
	}
	var no int64
	err = s.tx(ctx, func(tx pgx.Tx, now time.Time) error {
		day := time.Date(now.Year(), now.Month(), now.Day(), 0, 0, 0, 0, time.UTC)
		err := tx.QueryRow(ctx, `INSERT INTO runs (run_id, spec_hash, created_on, labels, policy, key_rule, deadline, max_tasks, max_depth, paused, created_at)
			VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11) ON CONFLICT (run_id) DO NOTHING RETURNING run_no`,
			string(spec.ID), spec.Hash, day, labels, pol, int16(spec.KeyRule), //nolint:gosec // G115: a small enum
			tszNull(spec.Limits.Deadline), maxTasks, maxDepth, spec.Paused, now).Scan(&no)
		if errors.Is(err, pgx.ErrNoRows) {
			var hash []byte
			if err := tx.QueryRow(ctx, "SELECT run_no, spec_hash FROM runs WHERE run_id = $1", string(spec.ID)).Scan(&no, &hash); err != nil {
				return err
			}
			if !bytes.Equal(hash, spec.Hash) {
				return store.ErrConflict
			}
			return nil
		}
		if err != nil {
			return err
		}
		return s.event(ctx, tx, no, store.EventCreated, now)
	})
	if err != nil {
		return store.Run{}, err
	}
	m, err := s.meta(ctx, s.db, no)
	if err != nil {
		return store.Run{}, wrap(err)
	}
	return s.view(ctx, s.db, m)
}

// lifecycle runs one lifecycle change of a run, under the run's exclusive lock.
func (s *Store) lifecycle(ctx context.Context, id store.RunID,
	change func(tx pgx.Tx, m *runMeta, st store.RunState, paused bool, now time.Time) error) (store.Run, error) {
	m, err := s.run(ctx, s.db, id)
	if err != nil {
		return store.Run{}, wrap(err)
	}
	if err := s.transition(ctx, m, change); err != nil {
		return store.Run{}, err
	}
	return s.view(ctx, s.db, m)
}

// transition is lifecycle without the read of the result.
func (s *Store) transition(ctx context.Context, m *runMeta,
	change func(tx pgx.Tx, m *runMeta, st store.RunState, paused bool, now time.Time) error) error {
	return s.tx(ctx, func(tx pgx.Tx, now time.Time) error {
		if _, err := tx.Exec(ctx, "SELECT pg_advisory_xact_lock($1::int, $2::int)", lockRun, lockKey(m.no)); err != nil {
			return err
		}
		var name string
		var paused bool
		err := tx.QueryRow(ctx, "SELECT state::text, paused FROM runs WHERE run_no = $1 FOR UPDATE", m.no).Scan(&name, &paused)
		if errors.Is(err, pgx.ErrNoRows) {
			s.forget(m)
			return store.ErrNotFound
		}
		if err != nil {
			return err
		}
		return change(tx, m, runStates[name], paused, now)
	})
}

// SealRun stops seeding; a run with no work left completes at once.
func (s *Store) SealRun(ctx context.Context, id store.RunID) (store.Run, error) {
	return s.lifecycle(ctx, id, func(tx pgx.Tx, m *runMeta, st store.RunState, _ bool, now time.Time) error {
		if st != store.RunOpen {
			return nil
		}
		if _, err := tx.Exec(ctx, "UPDATE runs SET state = 'sealed', sealed_at = $2 WHERE run_no = $1", m.no, now); err != nil {
			return err
		}
		if err := s.event(ctx, tx, m.no, store.EventSealed, now); err != nil {
			return err
		}
		return s.complete(ctx, tx, m.no, now)
	})
}

// PauseRun stops dispatching the run's tasks until it is resumed. Its backlogs leave
// the readiness index as dispatch meets them.
func (s *Store) PauseRun(ctx context.Context, id store.RunID) (store.Run, error) {
	return s.lifecycle(ctx, id, func(tx pgx.Tx, m *runMeta, st store.RunState, paused bool, now time.Time) error {
		if st.Ended() {
			return store.ErrRunClosed
		}
		if paused {
			return nil
		}
		if _, err := tx.Exec(ctx, "UPDATE runs SET paused = true, resuming = false WHERE run_no = $1", m.no); err != nil {
			return err
		}
		return s.event(ctx, tx, m.no, store.EventPaused, now)
	})
}

// ResumeRun makes a paused run dispatchable again and puts its backlogs back into
// the readiness index. If this call is cut short, Maintain finishes the sweep.
func (s *Store) ResumeRun(ctx context.Context, id store.RunID) (store.Run, error) {
	resumed := false
	run, err := s.lifecycle(ctx, id, func(tx pgx.Tx, m *runMeta, st store.RunState, paused bool, now time.Time) error {
		resumed = false
		if st.Ended() {
			return store.ErrRunClosed
		}
		if !paused {
			return nil
		}
		resumed = true
		if _, err := tx.Exec(ctx, "UPDATE runs SET paused = false, resuming = true WHERE run_no = $1", m.no); err != nil {
			return err
		}
		return s.event(ctx, tx, m.no, store.EventResumed, now)
	})
	if err != nil || !resumed {
		return run, err
	}
	m, err := s.run(ctx, s.db, id)
	if err != nil {
		return store.Run{}, wrap(err)
	}
	if err := s.resumeSweep(ctx, m.no); err != nil {
		return store.Run{}, err
	}
	return run, nil
}

// sweepBatch is how many backlogs one transaction of a sweep changes.
const sweepBatch = 500

// resumeSweep makes the backlogs of a resumed run ready again, in batches. It stops
// when the run is paused again, and clears the run's resuming mark when it is done.
//
// Every backlog with pending tasks is reset, not only the ones dispatch took out of
// the readiness index: while the run was paused nothing kept the others up to date, so
// one may still wait for a slot that was freed or for a block that was lifted.
func (s *Store) resumeSweep(ctx context.Context, no int64) error {
	var queue string // the sweep's position: the last backlog it reset
	var pool int16
	for {
		done := false
		nextQueue, nextPool := queue, pool
		err := s.tx(ctx, func(tx pgx.Tx, now time.Time) error {
			done = false
			nextQueue, nextPool = queue, pool
			var name string
			var paused, resuming bool
			err := tx.QueryRow(ctx, "SELECT state::text, paused, resuming FROM runs WHERE run_no = $1", no).Scan(&name, &paused, &resuming)
			if errors.Is(err, pgx.ErrNoRows) || (err == nil && (paused || !resuming || runStates[name].Ended())) {
				done = true
				return nil
			}
			if err != nil {
				return err
			}
			rows, err := tx.Query(ctx, `WITH c AS (
					SELECT queue_key, pool_id FROM backlogs
					WHERE run_no = $1 AND pending > 0 AND (queue_key, pool_id) > ($4, $5)
					ORDER BY queue_key, pool_id LIMIT $3 FOR UPDATE)
				UPDATE backlogs b SET ready_at = $2 FROM c
				WHERE b.queue_key = c.queue_key AND b.run_no = $1 AND b.pool_id = c.pool_id
				RETURNING b.queue_key, b.pool_id`, no, now, sweepBatch, queue, pool)
			if err != nil {
				return err
			}
			pools := map[int16]bool{}
			var k string
			var p int16
			if _, err := pgx.ForEachRow(rows, []any{&k, &p}, func() error {
				pools[p] = true
				if k > nextQueue || (k == nextQueue && p > nextPool) {
					nextQueue, nextPool = k, p
				}
				return nil
			}); err != nil {
				return err
			}
			if len(pools) == 0 {
				done = true
				// The mark is cleared only if no backlog is left out of the readiness
				// index: a pause and a second resume may have come in since this
				// sweep began, and that resume's sweep still has work to do then.
				_, err := tx.Exec(ctx, `UPDATE runs r SET resuming = false WHERE run_no = $1 AND NOT paused
					AND NOT EXISTS (SELECT 1 FROM backlogs b WHERE b.run_no = r.run_no AND b.pending > 0 AND b.ready_at IS NULL)`, no)
				return err
			}
			return s.wake(ctx, tx, pools)
		})
		if err != nil || done {
			return err
		}
		queue, pool = nextQueue, nextPool // only now: a transaction that is tried again starts where it did
	}
}

// CancelRun ends the run as cancelled. It returns at once; Maintain drains the
// run's backlogs.
func (s *Store) CancelRun(ctx context.Context, id store.RunID) (store.Run, error) {
	return s.lifecycle(ctx, id, func(tx pgx.Tx, m *runMeta, st store.RunState, _ bool, now time.Time) error {
		if st.Ended() {
			return nil
		}
		return s.end(ctx, tx, m.no, store.RunCancelled, now)
	})
}

// end finishes a run early. The caller holds the run's exclusive lock.
func (s *Store) end(ctx context.Context, tx pgx.Tx, no int64, st store.RunState, now time.Time) error {
	if _, err := tx.Exec(ctx, "UPDATE runs SET state = $2::run_state, finished_at = $3, resuming = false WHERE run_no = $1",
		no, runStateName(st), now); err != nil {
		return err
	}
	typ := store.EventCancelled
	if st == store.RunExpired {
		typ = store.EventExpired
	}
	return s.event(ctx, tx, no, typ, now)
}

// complete finishes a sealed run that has nothing pending or in flight. It takes the
// run's exclusive lock, so of two transactions that empty the last two backlogs, the
// second sees the first one's commit.
func (s *Store) complete(ctx context.Context, tx pgx.Tx, no int64, now time.Time) error {
	if _, err := tx.Exec(ctx, "SELECT pg_advisory_xact_lock($1::int, $2::int)", lockRun, lockKey(no)); err != nil {
		return err
	}
	var name string
	var idle bool
	err := tx.QueryRow(ctx, `SELECT state::text,
		NOT EXISTS (SELECT 1 FROM backlogs WHERE run_no = $1 AND (pending > 0 OR in_flight > 0)) FROM runs WHERE run_no = $1`, no).Scan(&name, &idle)
	if errors.Is(err, pgx.ErrNoRows) || (err == nil && (runStates[name] != store.RunSealed || !idle)) {
		return nil
	}
	if err != nil {
		return err
	}
	if _, err := tx.Exec(ctx, "UPDATE runs SET state = 'completed', finished_at = $2 WHERE run_no = $1", no, now); err != nil {
		return err
	}
	return s.event(ctx, tx, no, store.EventCompleted, now)
}

// event appends to the run event log. The lock makes cursors follow commit order.
func (s *Store) event(ctx context.Context, tx pgx.Tx, no int64, typ store.RunEventType, now time.Time) error {
	batch := &pgx.Batch{}
	batch.Queue("SELECT pg_advisory_xact_lock($1::int, 0)", lockEvents)
	batch.Queue("INSERT INTO run_events (run_no, type, at) VALUES ($1, $2, $3)", no, int16(typ), now) //nolint:gosec // G115: a small enum
	batch.Queue("SELECT pg_notify($1, '')", channelRuns)
	return tx.SendBatch(ctx, batch).Close()
}

// GetRun returns the run, or store.ErrNotFound.
func (s *Store) GetRun(ctx context.Context, id store.RunID) (store.Run, error) {
	m, err := s.run(ctx, s.db, id)
	if err != nil {
		return store.Run{}, wrap(err)
	}
	return s.view(ctx, s.db, m)
}

// view reads the state and the statistics of a run. The statistics are the sum over
// the run's backlogs.
func (s *Store) view(ctx context.Context, q querier, m *runMeta) (store.Run, error) {
	now, err := s.now(ctx, q)
	if err != nil {
		return store.Run{}, wrap(err)
	}
	r := store.Run{Spec: m.spec, CreatedAt: m.createdAt, Stats: store.RunStats{ByCode: map[string]uint64{}}}
	var name string
	var sealed, finished pgtype.Timestamptz
	err = q.QueryRow(ctx, "SELECT state::text, paused, sealed_at, finished_at FROM runs WHERE run_no = $1", m.no).Scan(&name, &r.Paused, &sealed, &finished)
	if errors.Is(err, pgx.ErrNoRows) {
		s.forget(m)
		return store.Run{}, store.ErrNotFound
	}
	if err != nil {
		return store.Run{}, wrap(err)
	}
	r.State, r.SealedAt, r.FinishedAt = runStates[name], timeOf(sealed), timeOf(finished)

	st := &r.Stats
	var pending, delayed int64
	err = q.QueryRow(ctx, `SELECT coalesce(sum(pending), 0)::bigint, coalesce(sum(in_flight), 0)::bigint, coalesce(sum(succeeded), 0)::bigint,
			coalesce(sum(failed), 0)::bigint, coalesce(sum(expired), 0)::bigint, coalesce(sum(cancelled), 0)::bigint, coalesce(sum(attempts), 0)::bigint,
			coalesce(sum(queue_blocks), 0)::bigint, coalesce(sum(discovered), 0)::bigint, coalesce(sum(duplicates), 0)::bigint, coalesce(sum(over_budget), 0)::bigint
		FROM backlogs WHERE run_no = $1`, m.no).Scan(&pending, &st.InFlight, &st.Succeeded, &st.Failed, &st.Expired, &st.Cancelled,
		&st.Attempts, &st.QueueBlocks, &st.Discovered, &st.Duplicates, &st.OverBudget)
	if err != nil {
		return store.Run{}, wrap(err)
	}
	if pending > 0 {
		if err := q.QueryRow(ctx, `SELECT count(*) FROM tasks
			WHERE run_created_on = $1 AND run_no = $2 AND state = 'pending' AND not_before > $3`, m.createdOn, m.no, now).Scan(&delayed); err != nil {
			return store.Run{}, wrap(err)
		}
	}
	delayed = min(delayed, pending)
	st.Pending, st.Delayed = uint64(pending-delayed), uint64(delayed) //nolint:gosec // G115: counters are not negative
	rows, err := q.Query(ctx, "SELECT code, sum(count)::bigint FROM backlog_codes WHERE run_no = $1 GROUP BY code", m.no)
	if err != nil {
		return store.Run{}, wrap(err)
	}
	var code string
	var n int64
	if _, err := pgx.ForEachRow(rows, []any{&code, &n}, func() error {
		st.ByCode[code] = uint64(n) //nolint:gosec // G115: counters are not negative
		return nil
	}); err != nil {
		return store.Run{}, wrap(err)
	}
	if st.ByLabel, err = labelStats(ctx, q, m.no); err != nil {
		return store.Run{}, wrap(err)
	}
	return r, nil
}

const defaultPage = 100

func pageSize(p store.Page) int {
	if p.Size <= 0 {
		return defaultPage
	}
	return p.Size
}

// ListRuns returns the runs that match the filter in creation order, one page at a time.
func (s *Store) ListRuns(ctx context.Context, f store.RunFilter, page store.Page) ([]store.Run, string, error) {
	after, _ := strconv.ParseInt(page.Token, 10, 64)
	var states []string // nil selects every state
	if len(f.States) > 0 {
		states = make([]string, 0, len(f.States))
	}
	for _, st := range f.States {
		states = append(states, runStateName(st))
	}
	labels := f.Labels
	if labels == nil {
		labels = map[string]string{}
	}
	size := pageSize(page)
	rows, err := s.db.Query(ctx, `SELECT run_no FROM runs
		WHERE run_no > $1 AND ($2::text[] IS NULL OR state::text = ANY($2)) AND labels @> $3::jsonb
		ORDER BY run_no LIMIT $4`, after, states, labels, size+1)
	if err != nil {
		return nil, "", wrap(err)
	}
	nos, err := pgx.CollectRows(rows, pgx.RowTo[int64])
	if err != nil {
		return nil, "", wrap(err)
	}
	next := ""
	if len(nos) > size {
		nos = nos[:size]
		next = strconv.FormatInt(nos[size-1], 10)
	}
	metas, err := s.metas(ctx, s.db, nos)
	if err != nil {
		return nil, "", wrap(err)
	}
	out := make([]store.Run, 0, len(nos))
	for _, no := range nos {
		m, ok := metas[no]
		if !ok {
			continue
		}
		r, err := s.view(ctx, s.db, m)
		if errors.Is(err, store.ErrNotFound) {
			continue
		}
		if err != nil {
			return nil, "", err
		}
		out = append(out, r)
	}
	return out, next, nil
}

// maxEvents bounds one page of events when the caller sets no limit.
const maxEvents = 1000

// RunEvents returns up to limit events after the cursor, in commit order. Each
// carries the run as it is now.
func (s *Store) RunEvents(ctx context.Context, after store.EventCursor, limit int) ([]store.RunEvent, error) {
	if limit <= 0 || limit > maxEvents {
		limit = maxEvents
	}
	rows, err := s.db.Query(ctx, "SELECT id, run_no, type, at FROM run_events WHERE id > $1 ORDER BY id LIMIT $2",
		int64(min(uint64(after), math.MaxInt64)), limit)
	if err != nil {
		return nil, wrap(err)
	}
	type row struct {
		id, run int64
		typ     int16
		at      time.Time
	}
	evs, err := pgx.CollectRows(rows, func(r pgx.CollectableRow) (row, error) {
		var e row
		err := r.Scan(&e.id, &e.run, &e.typ, &e.at)
		return e, err
	})
	if err != nil {
		return nil, wrap(err)
	}
	views := map[int64]store.Run{}
	out := make([]store.RunEvent, 0, len(evs))
	for _, e := range evs {
		v, ok := views[e.run]
		if !ok {
			m, err := s.meta(ctx, s.db, e.run)
			if err == nil {
				v, err = s.view(ctx, s.db, m)
			}
			if err != nil && !errors.Is(err, store.ErrNotFound) {
				return nil, wrap(err)
			}
			views[e.run] = v
		}
		out = append(out, store.RunEvent{Cursor: store.EventCursor(e.id), Type: store.RunEventType(e.typ), Run: v, At: e.at.UTC()}) //nolint:gosec // G115: identity values are positive
	}
	return out, nil
}

// LastEventCursor returns the cursor of the newest event, or zero when there is none.
func (s *Store) LastEventCursor(ctx context.Context) (store.EventCursor, error) {
	var id int64
	err := s.db.QueryRow(ctx, "SELECT coalesce(max(id), 0) FROM run_events").Scan(&id)
	return store.EventCursor(id), wrap(err) //nolint:gosec // G115: identity values are positive
}

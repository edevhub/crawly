package pgstore

import (
	"context"
	"encoding/hex"
	"errors"
	"slices"
	"strconv"
	"strings"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgtype"

	"github.com/edevhub/crawly/internal/sched"
	"github.com/edevhub/crawly/internal/store"
)

const statusColumns = `t.task_id, t.queue_key, t.pool_id, t.fingerprint, t.depth, t.parent_task_id, t.parent_bucket, t.spec, t.state::text, t.not_before`

var taskStates = map[string]store.TaskState{
	taskPending: store.TaskPending, taskInFlight: store.TaskInFlight, taskSucceeded: store.TaskSucceeded,
	taskFailed: store.TaskFailed, taskExpired: store.TaskExpired, taskCancelled: store.TaskCancelled,
}

// statuses reads the tasks a query selects, with their attempts.
func (s *Store) statuses(ctx context.Context, m *runMeta, now time.Time, sql string, args ...any) ([]store.TaskStatus, [][16]byte, error) {
	rows, err := s.db.Query(ctx, sql, args...)
	if err != nil {
		return nil, nil, err
	}
	var ids [][16]byte
	var pools []int16
	out, err := pgx.CollectRows(rows, func(row pgx.CollectableRow) (store.TaskStatus, error) {
		var (
			st           store.TaskStatus
			id           [16]byte
			queue, state string
			pool         int16
			fp           []byte
			depth        int32
			parent       pgtype.UUID
			parentBucket *int16
			notBefore    pgtype.Timestamptz
		)
		if err := row.Scan(&id, &queue, &pool, &fp, &depth, &parent, &parentBucket, &st.Spec, &state, &notBefore); err != nil {
			return st, err
		}
		st.Run, st.Queue, st.Fingerprint = m.spec.ID, store.QueueKey(queue), store.Fingerprint(fp)
		st.ID = taskKey{bucket: bucketOf(st.Queue), day: m.day, id: id}.ID()
		st.Depth = uint32(depth) //nolint:gosec // G115: depths are not negative
		st.Parent = parentID(parent, parentBucket, m.day)
		st.State = taskStates[state]
		if due := timeOf(notBefore); st.State == store.TaskPending && due.After(now) {
			st.State, st.NextEligibleAt = store.TaskDelayed, due
		}
		ids, pools = append(ids, id), append(pools, pool)
		return st, nil
	})
	if err != nil || len(out) == 0 {
		return nil, nil, err
	}
	index := make(map[[16]byte]int, len(ids))
	for i, id := range ids {
		index[id] = i
		if out[i].Pool, err = s.poolOf(ctx, s.db, pools[i]); err != nil {
			return nil, nil, err
		}
	}
	rows, err = s.db.Query(ctx, `SELECT task_id, attempt, worker_id, worker_version, policy_hash, dispatched_at, finished_at, kind,
			coalesce(code, ''), coalesce(message, ''), coalesce(http_status, 0), coalesce(final_url, ''), coalesce(latency_ms, 0),
			coalesce(bytes, 0), coalesce(response_ref, ''), coalesce(content_hash, ''), coalesce(items_emitted, 0)
		FROM attempts WHERE run_created_on = $1 AND task_id = ANY($2) ORDER BY task_id, attempt`, m.createdOn, ids)
	if err != nil {
		return nil, nil, err
	}
	var (
		id                   [16]byte
		a                    store.Attempt
		attempt, latency     int32
		status, items        int32
		size                 int64
		dispatched, finished time.Time
	)
	r := &a.Record
	_, err = pgx.ForEachRow(rows, []any{&id, &attempt, &a.Worker.ID, &a.Worker.Version, &r.PolicySet, &dispatched, &finished, &r.Kind,
		&r.Code, &r.Message, &status, &r.FinalURL, &latency, &size, &r.ResponseRef, &r.ContentHash, &items}, func() error {
		a.Attempt, a.DispatchedAt, a.FinishedAt = uint32(attempt), dispatched.UTC(), finished.UTC() //nolint:gosec // G115: attempts are positive
		r.HTTPStatus, r.ItemsEmitted = uint32(status), uint32(items)                                //nolint:gosec // G115: neither is negative
		r.Latency, r.Bytes = time.Duration(latency)*time.Millisecond, uint64(size)                  //nolint:gosec // G115: a size
		out[index[id]].Attempts = append(out[index[id]].Attempts, a)
		return nil
	})
	return out, ids, err
}

// GetTask returns a task by ID, or by fingerprint when fp is set.
func (s *Store) GetTask(ctx context.Context, id store.RunID, tid store.TaskID, fp *store.Fingerprint) (store.TaskStatus, error) {
	m, err := s.run(ctx, s.db, id)
	if err != nil {
		return store.TaskStatus{}, wrap(err)
	}
	now, err := s.Now(ctx)
	if err != nil {
		return store.TaskStatus{}, err
	}
	var out []store.TaskStatus
	if k, ok := parseTask(tid); ok && k.day == m.day {
		out, _, err = s.statuses(ctx, m, now, "SELECT "+statusColumns+` FROM tasks t
			WHERE t.run_created_on = $1 AND t.task_id = $2 AND t.run_no = $3`, m.createdOn, k.id, m.no)
	}
	if err == nil && len(out) == 0 && fp != nil {
		out, _, err = s.statuses(ctx, m, now, "SELECT "+statusColumns+` FROM tasks t
			WHERE t.run_created_on = $1 AND t.run_no = $2 AND t.fingerprint = $3`, m.createdOn, m.no, fp[:])
	}
	if err != nil {
		return store.TaskStatus{}, wrap(err)
	}
	if len(out) == 0 {
		return store.TaskStatus{}, store.ErrNotFound
	}
	return out[0], nil
}

// ListTasks returns the tasks that match the filter in creation order, one page at a time.
func (s *Store) ListTasks(ctx context.Context, f store.TaskFilter, page store.Page) ([]store.TaskStatus, string, error) {
	m, err := s.run(ctx, s.db, f.Run)
	if err != nil {
		return nil, "", wrap(err)
	}
	now, err := s.Now(ctx)
	if err != nil {
		return nil, "", err
	}
	// The conditions are fixed text; every value is a parameter.
	var sql strings.Builder
	sql.WriteString("SELECT " + statusColumns + " FROM tasks t WHERE t.run_created_on = $1 AND t.run_no = $2")
	args := []any{m.createdOn, m.no}
	param := func(v any) string {
		args = append(args, v)
		return "$" + strconv.Itoa(len(args))
	}
	if b, err := hex.DecodeString(page.Token); err == nil && len(b) == 16 {
		sql.WriteString(" AND t.task_id > " + param([16]byte(b)))
	}
	if f.Queue != "" {
		sql.WriteString(" AND t.queue_key = " + param(string(f.Queue)))
	}
	if len(f.States) > 0 {
		var plain []string
		var pending, delayed bool
		for _, st := range f.States {
			switch st {
			case store.TaskPending:
				pending = true
			case store.TaskDelayed:
				delayed = true
			case store.TaskInFlight, store.TaskSucceeded, store.TaskFailed, store.TaskExpired, store.TaskCancelled:
				for name, v := range taskStates {
					if v == st {
						plain = append(plain, name)
					}
				}
			}
		}
		at := param(now)
		sql.WriteString(" AND (t.state::text = ANY(" + param(plain) + ")")
		if pending {
			sql.WriteString(" OR (t.state = 'pending' AND t.not_before <= " + at + ")")
		}
		if delayed {
			sql.WriteString(" OR (t.state = 'pending' AND t.not_before > " + at + ")")
		}
		sql.WriteString(")")
	}
	if f.Code != "" {
		sql.WriteString(` AND EXISTS (SELECT 1 FROM attempts a
			WHERE a.run_created_on = t.run_created_on AND a.task_id = t.task_id AND a.code = ` + param(f.Code) + ")")
	}
	size := pageSize(page)
	sql.WriteString(" ORDER BY t.task_id LIMIT " + param(size+1))
	out, ids, err := s.statuses(ctx, m, now, sql.String(), args...)
	if err != nil {
		return nil, "", wrap(err)
	}
	next := ""
	if len(out) > size {
		out, next = out[:size], hex.EncodeToString(ids[size-1][:])
	}
	return out, next, nil
}

// queueRow is a queues row as the inspection calls read it.
type queueRow struct {
	key          string
	tat, blocked pgtype.Timestamptz
	inFlight     int
	reason       string
	strain       store.Strain
}

const queueColumns = "key, tat, in_flight, blocked_until, coalesce(block_reason, ''), slow_interval_ns, strain_at, strain_steps, last_strain_at, ceiling_at"

func scanQueue(row pgx.CollectableRow) (queueRow, error) {
	var q queueRow
	var slow, steps int64
	var at, last, ceiling pgtype.Timestamptz
	err := row.Scan(&q.key, &q.tat, &q.inFlight, &q.blocked, &q.reason, &slow, &at, &steps, &last, &ceiling)
	q.strain = store.Strain{Interval: time.Duration(slow), At: timeOf(at), Steps: uint64(max(steps, 0)), Last: timeOf(last),
		CeilingAt: timeOf(ceiling)}
	return q, err
}

// queueView builds the queue as the API shows it, with its active backlogs.
func (s *Store) queueView(ctx context.Context, q queueRow, now time.Time) (store.Queue, error) {
	key := store.QueueKey(q.key)
	e := s.pol.For("", nil, key)
	q.strain = e.Adaptive.Decayed(q.strain, now)
	lim := e.Adaptive.Apply(e.Queue, q.strain)
	tat := timeOf(q.tat)
	v := store.Queue{Key: key, InFlight: q.inFlight, NextEligibleAt: sched.EligibleAt(lim, tat),
		AdmissibleNow: sched.Admitted(lim, tat, now, q.inFlight), Strain: q.strain}
	if !e.Adaptive.Enabled {
		v.Strain.Interval = 0
	}
	if until := timeOf(q.blocked); now.Before(until) {
		v.BlockedUntil, v.BlockReason, v.AdmissibleNow = until, q.reason, 0
	}
	rows, err := s.db.Query(ctx, `SELECT b.run_no, b.pool_id, b.pending, b.in_flight,
			(SELECT count(*) FROM tasks t WHERE b.pending > 0 AND t.run_created_on = b.run_created_on AND t.run_no = b.run_no
				AND t.queue_key = b.queue_key AND t.pool_id = b.pool_id AND t.state = 'pending' AND t.not_before > $2)
		FROM backlogs b WHERE b.queue_key = $1 AND (b.pending > 0 OR b.in_flight > 0) ORDER BY b.run_no, b.pool_id`, q.key, now)
	if err != nil {
		return store.Queue{}, err
	}
	type backlog struct {
		run              int64
		pool             int16
		pending, delayed int64
		inFlight         int
	}
	bs, err := pgx.CollectRows(rows, func(row pgx.CollectableRow) (backlog, error) {
		var b backlog
		err := row.Scan(&b.run, &b.pool, &b.pending, &b.inFlight, &b.delayed)
		return b, err
	})
	if err != nil {
		return store.Queue{}, err
	}
	for _, b := range bs {
		m, err := s.meta(ctx, s.db, b.run)
		if errors.Is(err, store.ErrNotFound) {
			continue
		}
		if err != nil {
			return store.Queue{}, err
		}
		pool, err := s.poolOf(ctx, s.db, b.pool)
		if err != nil {
			return store.Queue{}, err
		}
		delayed := min(b.delayed, b.pending)
		v.Backlogs = append(v.Backlogs, store.Backlog{Run: m.spec.ID, Pool: pool, InFlight: b.inFlight,
			Pending: uint64(b.pending - delayed), Delayed: uint64(delayed), //nolint:gosec // G115: counters are not negative
			Weight: s.pol.For(m.spec.ID, m.spec.Policy, key).Weight})
	}
	return v, nil
}

// GetQueue returns the queue, or store.ErrNotFound.
func (s *Store) GetQueue(ctx context.Context, key store.QueueKey) (store.Queue, error) {
	now, err := s.Now(ctx)
	if err != nil {
		return store.Queue{}, err
	}
	rows, err := s.db.Query(ctx, "SELECT "+queueColumns+" FROM queues WHERE key = $1", string(key))
	if err != nil {
		return store.Queue{}, wrap(err)
	}
	q, err := pgx.CollectExactlyOneRow(rows, scanQueue)
	if errors.Is(err, pgx.ErrNoRows) {
		return store.Queue{}, store.ErrNotFound
	}
	if err != nil {
		return store.Queue{}, wrap(err)
	}
	v, err := s.queueView(ctx, q, now)
	return v, wrap(err)
}

// ListQueues returns the queues that match the filter in key order, one page at a time.
func (s *Store) ListQueues(ctx context.Context, f store.QueueFilter, page store.Page) ([]store.Queue, string, error) {
	now, err := s.Now(ctx)
	if err != nil {
		return nil, "", err
	}
	var run *int64
	if f.Run != "" {
		m, err := s.run(ctx, s.db, f.Run)
		if errors.Is(err, store.ErrNotFound) {
			return nil, "", nil
		}
		if err != nil {
			return nil, "", wrap(err)
		}
		run = &m.no
	}
	size := pageSize(page)
	rows, err := s.db.Query(ctx, "SELECT "+queueColumns+` FROM queues q
		WHERE key > $1 AND starts_with(key, $2) AND (NOT $3 OR blocked_until > $4)
			AND ($5::bigint IS NULL OR EXISTS (SELECT 1 FROM backlogs b
				WHERE b.queue_key = q.key AND b.run_no = $5 AND (b.pending > 0 OR b.in_flight > 0)))
		ORDER BY key LIMIT $6`, page.Token, f.KeyPrefix, f.OnlyBlocked, now, run, size+1)
	if err != nil {
		return nil, "", wrap(err)
	}
	qs, err := pgx.CollectRows(rows, scanQueue)
	if err != nil {
		return nil, "", wrap(err)
	}
	next := ""
	if len(qs) > size {
		qs, next = qs[:size], qs[size-1].key
	}
	out := make([]store.Queue, 0, len(qs))
	for i := range qs {
		v, err := s.queueView(ctx, qs[i], now)
		if err != nil {
			return nil, "", wrap(err)
		}
		out = append(out, v)
	}
	return out, next, nil
}

// BlockQueue blocks the queue for all runs until the given time; a zero time lifts
// the block.
func (s *Store) BlockQueue(ctx context.Context, key store.QueueKey, until time.Time, reason string) (store.Queue, error) {
	err := s.tx(ctx, func(tx pgx.Tx, now time.Time) error {
		if _, err := tx.Exec(ctx, "INSERT INTO queues (key, bucket, last_used_at) VALUES ($1, $2, $3) ON CONFLICT (key) DO NOTHING",
			string(key), int16(bucketOf(key)), now); err != nil {
			return err
		}
		q, err := lockQueue(ctx, tx, key, false)
		if err != nil {
			return err
		}
		if q == nil {
			return store.ErrUnavailable // deleted by the queue clean-up in between
		}
		bs, err := s.lockBacklogs(ctx, tx, key, 0, 0)
		if err != nil {
			return err
		}
		q.blockedUntil, q.blockReason = until, reason
		if until.IsZero() {
			q.blockReason = ""
		}
		if until.IsZero() && q.strain.Interval > 0 {
			// Back to the policy's rate: what the slowdown made the queue wait is cancelled.
			q.strain.Interval, q.strain.At, q.strain.CeilingAt = 0, now, time.Time{}
			if q.tat.After(now) {
				q.tat = now
			}
		}
		// A block that was lifted or shortened leaves readiness late: bring it back.
		for _, b := range bs {
			for _, l := range b.lanes {
				if b.live && l.ready.After(now) && !l.ready.Equal(forever) {
					l.setReady(now)
				}
			}
		}
		refresh(q, bs, now)
		if err := flush(ctx, tx, now, q, bs); err != nil {
			return err
		}
		return s.wakeWaiting(ctx, tx, bs)
	})
	if err != nil {
		return store.Queue{}, err
	}
	return s.GetQueue(ctx, key)
}

// PoolStats sums the work of every pool across all runs. The totals come from the
// backlogs that have work (backlogs_active), one row per backlog, each with the
// earliest readiness of its lanes (lanes is where readiness is kept; backlogs.ready_at
// is only a copy for the build before); no task is read. The due queues come from the
// readiness index of the lanes (lanes_ready_pool), the dueLimit oldest lanes of each
// pool. A backlog of a paused or ended run is not ready, whatever its lanes say: they
// are only lowered by the next dispatch round (heal).
func (s *Store) PoolStats(ctx context.Context, dueLimit int) ([]store.PoolStats, error) {
	now, err := s.Now(ctx)
	if err != nil {
		return nil, err
	}
	rows, err := s.db.Query(ctx, `SELECT p.name,
			coalesce(sum(b.pending) FILTER (WHERE l.ready_at <= $1 AND r.state IN ('open', 'sealed') AND NOT r.paused), 0),
			coalesce(sum(b.pending) FILTER (WHERE NOT (l.ready_at <= $1 AND r.state IN ('open', 'sealed') AND NOT r.paused)), 0),
			coalesce(sum(b.in_flight), 0)
		FROM backlogs b JOIN pools p ON p.pool_id = b.pool_id JOIN runs r ON r.run_no = b.run_no
		LEFT JOIN LATERAL (SELECT min(ready_at) AS ready_at FROM lanes
			WHERE run_no = b.run_no AND queue_key = b.queue_key AND pool_id = b.pool_id) l ON true
		WHERE b.pending > 0 OR b.in_flight > 0 GROUP BY p.name ORDER BY p.name`, now)
	if err != nil {
		return nil, wrap(err)
	}
	out, err := pgx.CollectRows(rows, func(row pgx.CollectableRow) (store.PoolStats, error) {
		var ps store.PoolStats
		var pool string
		var pending, delayed, inFlight int64
		err := row.Scan(&pool, &pending, &delayed, &inFlight)
		ps.Pool = store.PoolName(pool)
		ps.Pending, ps.Delayed, ps.InFlight = uint64(max(pending, 0)), uint64(max(delayed, 0)), uint64(max(inFlight, 0))
		return ps, err
	})
	if err != nil {
		return nil, wrap(err)
	}
	if dueLimit < 0 {
		dueLimit = 0
	}
	rows, err = s.db.Query(ctx, `SELECT p.name, d.queue_key, min(d.ready_at)
		FROM pools p CROSS JOIN LATERAL (
			SELECT l.queue_key, l.ready_at FROM lanes l
			JOIN backlogs b ON b.run_no = l.run_no AND b.queue_key = l.queue_key AND b.pool_id = l.pool_id
			JOIN runs r ON r.run_no = l.run_no
			WHERE l.pool_id = p.pool_id AND l.ready_at <= $1 AND b.pending > 0 AND r.state IN ('open', 'sealed') AND NOT r.paused
			ORDER BY l.ready_at LIMIT $2) d
		GROUP BY p.name, d.queue_key ORDER BY min(d.ready_at), d.queue_key`, now, dueLimit)
	if err != nil {
		return nil, wrap(err)
	}
	type due struct {
		pool string
		d    store.DueQueue
	}
	dues, err := pgx.CollectRows(rows, func(row pgx.CollectableRow) (due, error) {
		var d due
		var key string
		var since pgtype.Timestamptz
		err := row.Scan(&d.pool, &key, &since)
		d.d = store.DueQueue{Queue: store.QueueKey(key), Since: timeOf(since)}
		return d, err
	})
	if err != nil {
		return nil, wrap(err)
	}
	for _, d := range dues {
		i := slices.IndexFunc(out, func(ps store.PoolStats) bool { return string(ps.Pool) == d.pool })
		if i < 0 {
			continue // the pool's work was dispatched between the two reads
		}
		out[i].Due = append(out[i].Due, d.d)
	}
	return out, nil
}

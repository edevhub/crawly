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

	"github.com/edevhub/crawly/internal/store"
)

// total is a run's task count, cached for the soft max_tasks budget (ADR 0001).
type total struct {
	n    uint64
	read time.Time
}

// totalTTL is how long a cached task count is trusted.
const totalTTL = time.Second

// backlogKey names a backlog within one run.
type backlogKey struct {
	queue store.QueueKey
	pool  int16
}

// backlogDelta is what a batch adds to one backlog.
type backlogDelta struct {
	added, duplicates, overBudget int64
	ready                         time.Time // when the earliest added task is due
}

// AddTasks adds seeds or discovered tasks to a run and reports the result of each, in order.
func (s *Store) AddTasks(ctx context.Context, req store.AddRequest) ([]store.AddResult, error) {
	m, err := s.run(ctx, s.db, req.Run)
	if err != nil {
		return nil, wrap(err)
	}
	var names []store.PoolName
	for i := range req.Tasks {
		if nt := &req.Tasks[i]; !nt.Invalid && !slices.Contains(names, poolOrDefault(nt.Pool)) {
			names = append(names, poolOrDefault(nt.Pool))
		}
	}
	pools, err := s.ensurePools(ctx, names)
	if err != nil {
		return nil, wrap(err)
	}
	var out []store.AddResult
	var added uint64
	err = s.tx(ctx, func(tx pgx.Tx, now time.Time) error {
		out, added, err = s.addTx(ctx, tx, now, m, req, pools)
		return err
	})
	if errors.Is(err, errRunClosed) {
		out = make([]store.AddResult, len(req.Tasks))
		for i := range req.Tasks {
			out[i].Status = store.AddRunClosed
			if req.Tasks[i].Invalid {
				out[i].Status = store.AddInvalid
			}
		}
		return out, nil
	}
	if err != nil {
		return nil, err
	}
	if added > 0 {
		s.mu.Lock()
		if t, ok := s.totals[m.no]; ok {
			t.n += added
		}
		s.mu.Unlock()
	}
	return out, nil
}

func poolOrDefault(p store.PoolName) store.PoolName {
	if p == "" {
		return store.DefaultPool
	}
	return p
}

// newRow is a task of the batch that is to be inserted.
type newRow struct {
	index int // its position in the request
	id    [16]byte
	key   backlogKey
}

func (s *Store) addTx(ctx context.Context, tx pgx.Tx, now time.Time, m *runMeta, req store.AddRequest,
	pools map[store.PoolName]int16) ([]store.AddResult, uint64, error) {
	out := make([]store.AddResult, len(req.Tasks))

	// 1. The parent lease, shared: its Finish waits for this batch to commit.
	var depth uint32
	var parent pgtype.UUID
	var parentBucket *int16
	if req.Parent != nil {
		p, err := s.liveParent(ctx, tx, now, m, *req.Parent)
		if err != nil {
			return nil, 0, err
		}
		depth = p.depth + 1
		parent = pgtype.UUID{Bytes: p.key.id, Valid: true}
		b := int16(p.key.bucket)
		parentBucket = &b
	}

	// An early look at the run saves the work for a closed run. The check that
	// counts comes last, under the run's lock.
	if open, err := s.accepts(ctx, tx, m, req.Parent != nil); err != nil {
		return nil, 0, err
	} else if !open {
		return nil, 0, errRunClosed
	}

	// 2. Classify. Tasks already in the run are looked up first only when the run has
	// a task budget, so that duplicates do not use it up.
	limits := m.spec.Limits
	known := map[store.Fingerprint]store.TaskID{}
	var count uint64
	if limits.MaxTasks > 0 {
		var err error
		if count, err = s.taskCount(ctx, tx, m); err != nil {
			return nil, 0, err
		}
		fps := make([][]byte, 0, len(req.Tasks))
		for i := range req.Tasks {
			if !req.Tasks[i].Invalid {
				fps = append(fps, req.Tasks[i].Fingerprint[:])
			}
		}
		if err := s.existing(ctx, tx, m, fps, known); err != nil {
			return nil, 0, err
		}
	}
	deltas := map[backlogKey]*backlogDelta{}
	delta := func(k backlogKey) *backlogDelta {
		d, ok := deltas[k]
		if !ok {
			d = &backlogDelta{}
			deltas[k] = d
		}
		return d
	}
	first := map[store.Fingerprint]int{} // fingerprint to the index of the task that carries it
	var rows []newRow
	var repeats [][2]int // a task that repeats an earlier one of the batch, and that one
	for i := range req.Tasks {
		nt := &req.Tasks[i]
		if nt.Invalid {
			out[i].Status = store.AddInvalid
			continue
		}
		key := backlogKey{queue: nt.Queue, pool: pools[poolOrDefault(nt.Pool)]}
		id, dup := known[nt.Fingerprint]
		j, inBatch := first[nt.Fingerprint]
		switch {
		case (limits.MaxDepth > 0 && depth > limits.MaxDepth) || (limits.MaxTasks > 0 && count+uint64(len(rows)) >= limits.MaxTasks):
			out[i].Status = store.AddOverBudget
			delta(key).overBudget++
		case dup:
			out[i] = store.AddResult{Status: store.AddDuplicate, TaskID: id}
			delta(key).duplicates++
		case inBatch:
			out[i].Status = store.AddDuplicate // the ID is the first one's, filled in below
			delta(key).duplicates++
			repeats = append(repeats, [2]int{i, j})
		default:
			first[nt.Fingerprint] = i
			rows = append(rows, newRow{index: i, id: s.ids.next(), key: key})
		}
	}

	// 3. Queue rows, in key order, kept alive until the backlogs are committed.
	queues := make([]string, 0, len(deltas)+len(rows))
	for k := range deltas {
		queues = append(queues, string(k.queue))
	}
	for i := range rows {
		queues = append(queues, string(rows[i].key.queue))
	}
	slices.Sort(queues)
	queues = slices.Compact(queues)
	if err := lockQueues(ctx, tx, now, queues); err != nil {
		return nil, 0, err
	}

	// 4. Tasks, in fingerprint order. Rows that are not returned exist already.
	slices.SortFunc(rows, func(a, b newRow) int {
		return bytes.Compare(req.Tasks[a.index].Fingerprint[:], req.Tasks[b.index].Fingerprint[:])
	})
	inserted, err := s.insertTasks(ctx, tx, now, m, req.Tasks, rows, depth, parent, parentBucket)
	if err != nil {
		return nil, 0, err
	}
	var added uint64
	var lost [][]byte
	for i := range rows {
		r, nt := &rows[i], &req.Tasks[rows[i].index]
		d := delta(r.key)
		if !inserted[nt.Fingerprint] {
			lost = append(lost, nt.Fingerprint[:])
			d.duplicates++
			continue
		}
		out[r.index] = store.AddResult{Status: store.AddAdded, TaskID: taskKey{bucket: bucketOf(nt.Queue), day: m.day, id: r.id}.ID()}
		added++
		d.added++
		d.ready = earliest(d.ready, latest(now, nt.NotBefore))
	}
	if len(lost) > 0 {
		if err := s.existing(ctx, tx, m, lost, known); err != nil {
			return nil, 0, err
		}
		for i := range rows {
			if nt := &req.Tasks[rows[i].index]; !inserted[nt.Fingerprint] {
				out[rows[i].index] = store.AddResult{Status: store.AddDuplicate, TaskID: known[nt.Fingerprint]}
			}
		}
	}

	for _, r := range repeats {
		out[r[0]].TaskID = out[r[1]].TaskID
	}

	// 5. Backlogs, in primary key order.
	keys := make([]backlogKey, 0, len(deltas))
	for k := range deltas {
		keys = append(keys, k)
	}
	slices.SortFunc(keys, func(a, b backlogKey) int { return cmp.Or(cmp.Compare(a.queue, b.queue), cmp.Compare(a.pool, b.pool)) })
	batch := &pgx.Batch{}
	woken := map[int16]bool{}
	for _, k := range keys {
		d := deltas[k]
		var discovered int64
		if req.Parent != nil {
			discovered = d.added
		}
		batch.Queue(`INSERT INTO backlogs (queue_key, run_no, pool_id, bucket, run_created_on, pending, tasks_total, discovered, duplicates, over_budget, ready_at)
			VALUES ($1, $2, $3, $4, $5, $6, $6, $7, $8, $9, $10)
			ON CONFLICT (queue_key, run_no, pool_id) DO UPDATE SET
				pending = backlogs.pending + EXCLUDED.pending, tasks_total = backlogs.tasks_total + EXCLUDED.tasks_total,
				discovered = backlogs.discovered + EXCLUDED.discovered, duplicates = backlogs.duplicates + EXCLUDED.duplicates,
				over_budget = backlogs.over_budget + EXCLUDED.over_budget, ready_at = least(backlogs.ready_at, EXCLUDED.ready_at)`,
			string(k.queue), m.no, k.pool, int16(bucketOf(k.queue)), m.createdOn, d.added, discovered, d.duplicates, d.overBudget, tszNull(d.ready))
		if d.added > 0 {
			woken[k.pool] = true
		}
	}
	if batch.Len() > 0 {
		if err := tx.SendBatch(ctx, batch).Close(); err != nil {
			return nil, 0, err
		}
	}

	// 6. The run's state, under its shared lock. SealRun and completion take it
	// exclusively: either they committed before this read and the batch is rejected,
	// or they wait for this batch and see its tasks.
	if _, err := tx.Exec(ctx, "SELECT pg_advisory_xact_lock_shared($1::int, $2::int)", lockRun, lockKey(m.no)); err != nil {
		return nil, 0, err
	}
	if open, err := s.accepts(ctx, tx, m, req.Parent != nil); err != nil {
		return nil, 0, err
	} else if !open {
		return nil, 0, errRunClosed
	}
	return out, added, s.wake(ctx, tx, woken)
}

// accepts reports whether the run takes new tasks: seeds while it is open,
// discovered tasks while it is open or sealed.
func (s *Store) accepts(ctx context.Context, q querier, m *runMeta, discovered bool) (bool, error) {
	var name string
	err := q.QueryRow(ctx, "SELECT state::text FROM runs WHERE run_no = $1", m.no).Scan(&name)
	if errors.Is(err, pgx.ErrNoRows) {
		s.forget(m)
		return false, store.ErrNotFound
	}
	if err != nil {
		return false, err
	}
	st := runStates[name]
	return st == store.RunOpen || (discovered && st == store.RunSealed), nil
}

// parentTask is what a batch of discovered tasks needs from its parent.
type parentTask struct {
	key   taskKey
	depth uint32
}

// liveParent locks the parent task in share mode and checks that the lease is its
// current one, live, and of this run.
func (s *Store) liveParent(ctx context.Context, tx pgx.Tx, now time.Time, m *runMeta, ref store.LeaseRef) (parentTask, error) {
	lk, ok := parseLease(ref.ID)
	if !ok {
		return parentTask{}, store.ErrLeaseUnknown
	}
	var (
		run     int64
		depth   int32
		state   string
		attempt int32
		nonce   *int64
		expires pgtype.Timestamptz
	)
	err := tx.QueryRow(ctx, `SELECT run_no, depth, state::text, attempt, lease_nonce, lease_expires_at FROM tasks
		WHERE run_created_on = $1 AND task_id = $2 FOR SHARE`, lk.task.date(), lk.task.id).Scan(&run, &depth, &state, &attempt, &nonce, &expires)
	if errors.Is(err, pgx.ErrNoRows) {
		return parentTask{}, store.ErrLeaseUnknown
	}
	if err != nil {
		return parentTask{}, err
	}
	if state != taskInFlight || uint32(attempt) != lk.attempt || lk.attempt != ref.Attempt || //nolint:gosec // G115: attempts are positive
		nonce == nil || *nonce != lk.nonce || !now.Before(timeOf(expires)) || run != m.no {
		return parentTask{}, store.ErrLeaseStale
	}
	return parentTask{key: lk.task, depth: uint32(depth)}, nil //nolint:gosec // G115: depths are not negative
}

// taskCount is the number of tasks ever added to the run, read at most once a second.
func (s *Store) taskCount(ctx context.Context, q querier, m *runMeta) (uint64, error) {
	s.mu.Lock()
	t, ok := s.totals[m.no]
	if ok && time.Since(t.read) < totalTTL {
		n := t.n
		s.mu.Unlock()
		return n, nil
	}
	s.mu.Unlock()
	var n int64
	if err := q.QueryRow(ctx, "SELECT coalesce(sum(tasks_total), 0)::bigint FROM backlogs WHERE run_no = $1", m.no).Scan(&n); err != nil {
		return 0, err
	}
	s.mu.Lock()
	s.totals[m.no] = &total{n: uint64(n), read: time.Now()} //nolint:gosec // G115: a count
	s.mu.Unlock()
	return uint64(n), nil //nolint:gosec // G115: a count
}

// existing looks tasks of the run up by fingerprint and records their IDs.
func (s *Store) existing(ctx context.Context, q querier, m *runMeta, fps [][]byte, into map[store.Fingerprint]store.TaskID) error {
	if len(fps) == 0 {
		return nil
	}
	rows, err := q.Query(ctx, `SELECT fingerprint, task_id, queue_key FROM tasks
		WHERE run_created_on = $1 AND run_no = $2 AND fingerprint = ANY($3)`, m.createdOn, m.no, fps)
	if err != nil {
		return err
	}
	var fp []byte
	var id [16]byte
	var queue string
	_, err = pgx.ForEachRow(rows, []any{&fp, &id, &queue}, func() error {
		into[store.Fingerprint(fp)] = taskKey{bucket: bucketOf(store.QueueKey(queue)), day: m.day, id: id}.ID()
		return nil
	})
	return err
}

// lockQueues creates the queue rows that are missing and locks them all against
// deletion, in key order. A row can vanish between the two statements when the
// queue clean-up deletes it; then both are repeated.
func lockQueues(ctx context.Context, tx pgx.Tx, now time.Time, keys []string) error {
	if len(keys) == 0 {
		return nil
	}
	bucketsOf := make([]int16, len(keys))
	for i, k := range keys {
		bucketsOf[i] = int16(bucketOf(store.QueueKey(k)))
	}
	for range maxRetries {
		if _, err := tx.Exec(ctx, `INSERT INTO queues (key, bucket, last_used_at)
			SELECT k, b, $3 FROM unnest($1::text[], $2::smallint[]) AS u(k, b) ORDER BY k
			ON CONFLICT (key) DO NOTHING`, keys, bucketsOf, now); err != nil {
			return err
		}
		rows, err := tx.Query(ctx, "SELECT key FROM queues WHERE key = ANY($1) ORDER BY key FOR KEY SHARE", keys)
		if err != nil {
			return err
		}
		locked, err := pgx.CollectRows(rows, pgx.RowTo[string])
		if err != nil {
			return err
		}
		if len(locked) == len(keys) {
			return nil
		}
	}
	return errors.New("pgstore: queue rows kept disappearing while tasks were added")
}

// insertTasks inserts the rows and reports which fingerprints were new.
func (s *Store) insertTasks(ctx context.Context, tx pgx.Tx, now time.Time, m *runMeta, tasks []store.NewTask, rows []newRow,
	depth uint32, parent pgtype.UUID, parentBucket *int16) (map[store.Fingerprint]bool, error) {
	inserted := make(map[store.Fingerprint]bool, len(rows))
	if len(rows) == 0 {
		return inserted, nil
	}
	n := len(rows)
	var (
		ids        = make([][16]byte, n)
		queues     = make([]string, n)
		pools      = make([]int16, n)
		fps        = make([][]byte, n)
		priorities = make([]int32, n)
		notBefore  = make([]pgtype.Timestamptz, n)
		expires    = make([]pgtype.Timestamptz, n)
		specs      = make([][]byte, n)
	)
	for i := range rows {
		nt := &tasks[rows[i].index]
		ids[i], queues[i], pools[i], fps[i] = rows[i].id, string(nt.Queue), rows[i].key.pool, nt.Fingerprint[:]
		priorities[i], notBefore[i], specs[i] = nt.Priority, tsz(nt.NotBefore), nt.Spec
		if specs[i] == nil {
			specs[i] = []byte{}
		}
		if nt.TTL > 0 {
			expires[i] = tsz(now.Add(nt.TTL))
		}
	}
	res, err := tx.Query(ctx, `INSERT INTO tasks (run_created_on, task_id, run_no, queue_key, pool_id, fingerprint, priority,
			not_before, expires_at, depth, parent_task_id, parent_bucket, spec, created_at)
		SELECT $1, u.task_id, $2, u.queue_key, u.pool_id, u.fingerprint, u.priority, u.not_before, u.expires_at, $3, $4, $5, u.spec, $6
		FROM unnest($7::uuid[], $8::text[], $9::smallint[], $10::bytea[], $11::int[], $12::timestamptz[], $13::timestamptz[], $14::bytea[])
			AS u(task_id, queue_key, pool_id, fingerprint, priority, not_before, expires_at, spec)
		ORDER BY u.fingerprint
		ON CONFLICT (run_created_on, run_no, fingerprint) DO NOTHING
		RETURNING fingerprint`,
		m.createdOn, m.no, int32(min(depth, 1<<30)), parent, parentBucket, now,
		ids, queues, pools, fps, priorities, notBefore, expires, specs)
	if err != nil {
		return nil, err
	}
	var fp []byte
	_, err = pgx.ForEachRow(res, []any{&fp}, func() error {
		inserted[store.Fingerprint(fp)] = true
		return nil
	})
	return inserted, err
}

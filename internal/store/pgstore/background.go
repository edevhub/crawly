package pgstore

import (
	"context"
	"errors"
	"strings"
	"time"

	"github.com/jackc/pgx/v5"

	"github.com/edevhub/crawly/internal/store"
)

// Start registers the replica and its policy set. When the set differs from the one
// that was current, every waiting backlog is made ready so its readiness is worked
// out again under the new limits.
func (s *Store) Start(ctx context.Context, r store.Replica) error {
	s.mu.Lock()
	s.replica = r.ID
	s.mu.Unlock()
	changed := false
	err := s.tx(ctx, func(tx pgx.Tx, now time.Time) error {
		changed = false
		if _, err := tx.Exec(ctx, "SELECT pg_advisory_xact_lock($1::int, 0)", lockPolicies); err != nil {
			return err
		}
		var current string
		err := tx.QueryRow(ctx, "SELECT hash FROM policy_sets ORDER BY activated_at DESC, hash LIMIT 1").Scan(&current)
		if err != nil && !errors.Is(err, pgx.ErrNoRows) {
			return err
		}
		if current != r.PolicyHash {
			// A new set, or a rollback to an earlier one.
			changed = current != ""
			if _, err := tx.Exec(ctx, `INSERT INTO policy_sets (hash, first_seen, activated_at) VALUES ($1, $2, $2)
				ON CONFLICT (hash) DO UPDATE SET activated_at = EXCLUDED.activated_at`, r.PolicyHash, now); err != nil {
				return err
			}
		}
		_, err = tx.Exec(ctx, `INSERT INTO replicas (replica_id, policy_hash, started_at, seen_at, abort_rules) VALUES ($1, $2, $3, $3, true)
			ON CONFLICT (replica_id) DO UPDATE SET policy_hash = EXCLUDED.policy_hash, started_at = EXCLUDED.started_at,
				seen_at = EXCLUDED.seen_at, abort_rules = true`, r.ID, r.PolicyHash, now)
		return err
	})
	if err != nil || !changed {
		return err
	}
	return s.resetReadiness(ctx)
}

// resetReadiness makes every backlog that waits for a time or a slot ready now, in
// batches in primary key order. Backlogs of paused and ended runs keep NULL.
func (s *Store) resetReadiness(ctx context.Context) error {
	var queue string
	var run int64
	var pool int16
	for {
		n := 0
		nextQueue, nextRun, nextPool := queue, run, pool
		err := s.tx(ctx, func(tx pgx.Tx, now time.Time) error {
			nextQueue, nextRun, nextPool = queue, run, pool
			rows, err := tx.Query(ctx, `WITH c AS (
					SELECT queue_key, run_no, pool_id FROM backlogs
					WHERE pending > 0 AND ready_at IS NOT NULL AND (queue_key, run_no, pool_id) > ($1, $2, $3)
					ORDER BY queue_key, run_no, pool_id LIMIT $5 FOR UPDATE)
				UPDATE backlogs b SET ready_at = $4 FROM c
				WHERE b.queue_key = c.queue_key AND b.run_no = c.run_no AND b.pool_id = c.pool_id
				RETURNING b.queue_key, b.run_no, b.pool_id`, queue, run, pool, now, sweepBatch)
			if err != nil {
				return err
			}
			n = 0
			var k string
			var r int64
			var p int16
			_, err = pgx.ForEachRow(rows, []any{&k, &r, &p}, func() error {
				n++
				if k > nextQueue || (k == nextQueue && (r > nextRun || (r == nextRun && p > nextPool))) {
					nextQueue, nextRun, nextPool = k, r, p
				}
				return nil
			})
			return err
		})
		if err != nil {
			return err
		}
		if n == 0 {
			break
		}
		queue, run, pool = nextQueue, nextRun, nextPool // only now: a transaction that is tried again starts where it did
	}
	rows, err := s.db.Query(ctx, "SELECT pg_notify($1 || name, '') FROM pools", channelWork)
	if err != nil {
		return wrap(err)
	}
	rows.Close()
	return wrap(rows.Err())
}

// ---- notifications received

// Notifications delivers wake-ups until ctx is done. A full channel drops wake-ups,
// which only delays work. All subscribers share one LISTEN connection, opened by the
// first call.
func (s *Store) Notifications(ctx context.Context) (<-chan store.Notification, error) {
	ch := make(chan store.Notification, 64)
	s.mu.Lock()
	s.subs[ch] = struct{}{}
	s.mu.Unlock()
	drop := func() {
		s.mu.Lock()
		delete(s.subs, ch)
		s.mu.Unlock()
	}
	s.listen.Do(func() { go s.listener() })
	select {
	case <-s.ready:
	case <-ctx.Done():
		drop()
		return nil, ctx.Err()
	}
	s.mu.Lock()
	err := s.lerr
	s.mu.Unlock()
	if err != nil {
		drop()
		return nil, wrap(err)
	}
	go func() {
		<-ctx.Done()
		drop()
	}()
	return ch, nil
}

func (s *Store) send(n store.Notification) {
	s.mu.Lock()
	defer s.mu.Unlock()
	for ch := range s.subs {
		select {
		case ch <- n:
		default: // full: the subscriber is already due to wake up
		}
	}
}

// relisten is how long the listener waits before it reconnects.
const relisten = time.Second

// listener keeps the LISTEN connection until the store is closed. Whatever was
// missed while it was down is covered by waking everything after a reconnect.
func (s *Store) listener() {
	first := true
	for s.ctx.Err() == nil {
		err := s.listenOnce(s.ctx, func(err error) {
			s.mu.Lock()
			s.lerr = err
			s.mu.Unlock()
			if first {
				first = false
				close(s.ready)
			}
			if err == nil {
				// Runs deleted while the connection was down were not announced.
				s.forgetAll()
			}
		})
		if s.ctx.Err() != nil {
			return
		}
		s.log.Warn("pgstore: the LISTEN connection failed; reconnecting", "err", err)
		select {
		case <-s.ctx.Done():
		case <-time.After(relisten):
		}
	}
}

// listenOnce listens on one connection until it fails. connected is called once the
// connection is listening, or with the reason it could not.
func (s *Store) listenOnce(ctx context.Context, connected func(error)) error {
	conn, err := pgx.Connect(ctx, s.listenDSN)
	if err != nil {
		connected(err)
		return err
	}
	defer func() {
		c, cancel := context.WithTimeout(context.WithoutCancel(ctx), time.Second)
		defer cancel()
		_ = conn.Close(c)
	}()
	listening := map[string]bool{}
	// subscribe listens on the channel of every pool it has not seen yet, and wakes
	// those pools: their first notification may have been sent before the LISTEN.
	subscribe := func() error {
		rows, err := conn.Query(ctx, "SELECT name FROM pools")
		if err != nil {
			return err
		}
		names, err := pgx.CollectRows(rows, pgx.RowTo[string])
		if err != nil {
			return err
		}
		for _, name := range names {
			if listening[name] {
				continue
			}
			if _, err := conn.Exec(ctx, "LISTEN "+pgx.Identifier{channelWork + name}.Sanitize()); err != nil {
				return err
			}
			listening[name] = true
			s.send(store.Notification{Pool: store.PoolName(name)})
		}
		return nil
	}
	_, err = conn.Exec(ctx, "LISTEN "+channelRuns+"; LISTEN "+channelPools)
	if err == nil {
		err = subscribe()
	}
	connected(err)
	if err != nil {
		return err
	}
	s.send(store.Notification{Runs: true})
	for {
		n, err := conn.WaitForNotification(ctx)
		if err != nil {
			return err
		}
		switch n.Channel {
		case channelRuns:
			if n.Payload == runsDeleted {
				s.forgetAll()
				continue
			}
			s.send(store.Notification{Runs: true})
		case channelPools:
			if err := subscribe(); err != nil {
				return err
			}
		default:
			if pool, ok := strings.CutPrefix(n.Channel, channelWork); ok {
				s.send(store.Notification{Pool: store.PoolName(pool)})
			}
		}
	}
}

// ---- maintenance

const (
	// maintainRuns and maintainBacklogs bound what one Maintain round takes on.
	maintainRuns     = 100
	maintainBacklogs = 200
	// maintainTasks bounds the tasks one transaction of a round changes.
	maintainTasks = 2000
	// queueIdle is how long a queue with no backlog is kept.
	queueIdle = 24 * time.Hour
	// replicaIdle is how long a replica that stopped reporting is kept.
	replicaIdle = time.Hour
	// replicaLive is how recently a replica must have reported to count as running when
	// the abort rules are accepted. Replicas report every Maintain round.
	replicaLive = time.Minute
	// auditEvery is how many rounds pass between two ledger audits.
	auditEvery = 60
	// auditSample is how many backlogs an audit checks.
	auditSample = 200
)

// Maintain runs one bounded round of background work. Every job is safe to run on
// all replicas at once: each takes row locks in the lock order or skips what is held.
func (s *Store) Maintain(ctx context.Context) error {
	now, err := s.Now(ctx)
	if err != nil {
		return err
	}
	jobs := []func(context.Context, time.Time) error{
		s.expireRuns, s.drain, s.expireTasks, s.finishSweeps,
		func(ctx context.Context, now time.Time) error { return wrap(s.ensureMonths(ctx, now)) },
		s.cleanQueues, s.reportReplica, s.retain, s.audit,
	}
	var errs []error
	for _, job := range jobs {
		if err := job(ctx, now); err != nil {
			errs = append(errs, err)
		}
	}
	return errors.Join(errs...)
}

// expireRuns ends the runs that are past their deadline.
func (s *Store) expireRuns(ctx context.Context, now time.Time) error {
	rows, err := s.db.Query(ctx, `SELECT run_id FROM runs WHERE state IN ('open', 'sealed') AND deadline <= $1
		ORDER BY deadline LIMIT $2`, now, maintainRuns)
	if err != nil {
		return wrap(err)
	}
	ids, err := pgx.CollectRows(rows, pgx.RowTo[string])
	if err != nil {
		return wrap(err)
	}
	for _, id := range ids {
		m, err := s.run(ctx, s.db, store.RunID(id))
		if err == nil {
			err = s.transition(ctx, m, func(tx pgx.Tx, m *runMeta, st store.RunState, _ bool, now time.Time) error {
				if st.Ended() {
					return nil
				}
				_, err := s.end(ctx, tx, m.no, store.RunExpired, nil, now)
				return err
			})
		}
		if err != nil && !errors.Is(err, store.ErrNotFound) {
			return wrap(err)
		}
	}
	return nil
}

// backlogRef names a backlog.
type backlogRef struct {
	queue string
	run   int64
	pool  int16
}

func scanBacklogRef(row pgx.CollectableRow) (backlogRef, error) {
	var b backlogRef
	err := row.Scan(&b.queue, &b.run, &b.pool)
	return b, err
}

// drain moves the tasks of cancelled, aborted and expired runs to their final state, one
// backlog per transaction, and releases the slots their in-flight tasks held.
func (s *Store) drain(ctx context.Context, _ time.Time) error {
	rows, err := s.db.Query(ctx, `SELECT b.queue_key, b.run_no, b.pool_id FROM runs r JOIN backlogs b ON b.run_no = r.run_no
		WHERE r.state IN ('cancelled', 'expired', 'aborted') AND (b.pending > 0 OR b.in_flight > 0)
		ORDER BY b.queue_key, b.run_no, b.pool_id LIMIT $1`, maintainBacklogs)
	if err != nil {
		return wrap(err)
	}
	refs, err := pgx.CollectRows(rows, scanBacklogRef)
	if err != nil {
		return wrap(err)
	}
	for _, ref := range refs {
		if err := s.tx(ctx, func(tx pgx.Tx, now time.Time) error { return s.drainBacklog(ctx, tx, now, ref) }); err != nil {
			return err
		}
	}
	return nil
}

func (s *Store) drainBacklog(ctx context.Context, tx pgx.Tx, now time.Time, ref backlogRef) error {
	key := store.QueueKey(ref.queue)
	q, err := lockQueue(ctx, tx, key, true)
	if err != nil || q == nil {
		return err // held by a dispatch or a finish: the next round takes it
	}
	bs, err := s.lockBacklogs(ctx, tx, key, ref.run, ref.pool)
	if err != nil {
		return err
	}
	var own *brow
	for _, b := range bs {
		if b.run == ref.run && b.pool == ref.pool {
			own = b
		}
	}
	if own == nil || own.meta == nil || (own.state != store.RunCancelled && own.state != store.RunExpired && own.state != store.RunAborted) {
		return nil
	}
	final := taskCancelled
	if own.state == store.RunExpired {
		final = taskExpired
	}
	// Task rows are skipped when a Finish holds them: it holds the task and waits for
	// this queue, so waiting for it here would be a deadlock. It ends the task itself.
	// The picked rows are materialised, so the LIMIT is applied once.
	const pick = `WITH picked AS MATERIALIZED (SELECT task_id FROM tasks
		WHERE run_created_on = $1 AND run_no = $2 AND queue_key = $3 AND pool_id = $4 AND state = $5::task_state
		LIMIT $8 FOR UPDATE SKIP LOCKED) `
	tag, err := tx.Exec(ctx, pick+`UPDATE tasks t SET state = $6::task_state, finished_at = $7
		FROM picked WHERE t.run_created_on = $1 AND t.task_id = picked.task_id`,
		own.meta.createdOn, ref.run, ref.queue, ref.pool, taskPending, final, now, maintainTasks)
	if err != nil {
		return err
	}
	pending := tag.RowsAffected()
	rows, err := tx.Query(ctx, pick+`UPDATE tasks t SET state = $6::task_state, finished_at = $7,
			lease_nonce = NULL, lease_expires_at = NULL, lease_deadline = NULL
		FROM picked WHERE t.run_created_on = $1 AND t.task_id = picked.task_id
		RETURNING t.task_id, t.attempt, coalesce(t.worker_id, ''), coalesce(t.worker_version, ''), t.dispatched_at`,
		own.meta.createdOn, ref.run, ref.queue, ref.pool, taskInFlight, final, now, maintainTasks)
	if err != nil {
		return err
	}
	revoked, err := pgx.CollectRows(rows, func(row pgx.CollectableRow) (*leased, error) {
		t := &leased{run: ref.run}
		var attempt int32
		err := row.Scan(&t.key.id, &attempt, &t.worker.ID, &t.worker.Version, &t.dispatched)
		t.key.day, t.attempt = own.meta.day, uint32(attempt) //nolint:gosec // G115: attempts are positive
		return t, err
	})
	if err != nil {
		return err
	}
	if len(revoked) > 0 {
		batch := &pgx.Batch{}
		for _, t := range revoked {
			recordAttempt(batch, t, now, resultRevoked, store.AttemptRecord{Code: codeRevoked, PolicySet: s.pol.Hash()})
		}
		if err := tx.SendBatch(ctx, batch).Close(); err != nil {
			return err
		}
	}
	inFlight := len(revoked)
	own.pending = max(own.pending-pending, 0)
	own.inFlight = max(own.inFlight-inFlight, 0)
	q.inFlight = max(q.inFlight-inFlight, 0)
	if final == taskExpired {
		own.expired += pending + int64(inFlight)
	} else {
		own.cancelled += pending + int64(inFlight)
	}
	own.dirty = true
	own.setReady(time.Time{})
	refresh(q, bs, now)
	if err := flush(ctx, tx, now, q, bs); err != nil {
		return err
	}
	return s.wakeWaiting(ctx, tx, bs)
}

// expireTasks ends the pending tasks that are past their TTL, one backlog per
// transaction, and completes the runs that this empties.
func (s *Store) expireTasks(ctx context.Context, now time.Time) error {
	rows, err := s.db.Query(ctx, `SELECT DISTINCT queue_key, run_no, pool_id FROM (
			SELECT queue_key, run_no, pool_id FROM tasks WHERE state = 'pending' AND expires_at <= $1 LIMIT $2) t
		ORDER BY queue_key, run_no, pool_id LIMIT $3`, now, maintainTasks, maintainBacklogs)
	if err != nil {
		return wrap(err)
	}
	refs, err := pgx.CollectRows(rows, scanBacklogRef)
	if err != nil {
		return wrap(err)
	}
	for _, ref := range refs {
		if err := s.tx(ctx, func(tx pgx.Tx, now time.Time) error { return s.expireBacklog(ctx, tx, now, ref) }); err != nil {
			return err
		}
	}
	return nil
}

func (s *Store) expireBacklog(ctx context.Context, tx pgx.Tx, now time.Time, ref backlogRef) error {
	key := store.QueueKey(ref.queue)
	q, err := lockQueue(ctx, tx, key, false)
	if err != nil || q == nil {
		return err
	}
	bs, err := s.lockBacklogs(ctx, tx, key, ref.run, ref.pool)
	if err != nil {
		return err
	}
	var own *brow
	for _, b := range bs {
		if b.run == ref.run && b.pool == ref.pool {
			own = b
		}
	}
	if own == nil || own.meta == nil {
		return nil
	}
	tag, err := tx.Exec(ctx, `WITH picked AS MATERIALIZED (SELECT task_id FROM tasks
			WHERE run_created_on = $1 AND run_no = $2 AND queue_key = $3 AND pool_id = $4 AND state = 'pending' AND expires_at <= $5
			LIMIT $6 FOR UPDATE SKIP LOCKED)
		UPDATE tasks t SET state = 'expired', finished_at = $5
		FROM picked WHERE t.run_created_on = $1 AND t.task_id = picked.task_id`,
		own.meta.createdOn, ref.run, ref.queue, ref.pool, now, maintainTasks)
	if err != nil {
		return err
	}
	n := tag.RowsAffected()
	if n == 0 {
		return nil
	}
	own.pending = max(own.pending-n, 0)
	own.expired += n
	own.dirty = true
	refresh(q, bs, now)
	if err := flush(ctx, tx, now, q, bs); err != nil {
		return err
	}
	if own.pending == 0 && own.inFlight == 0 {
		return s.complete(ctx, tx, ref.run, now)
	}
	return nil
}

// finishSweeps continues the resume sweeps that the replica that started them did
// not finish.
func (s *Store) finishSweeps(ctx context.Context, _ time.Time) error {
	rows, err := s.db.Query(ctx, "SELECT run_no FROM runs WHERE resuming ORDER BY run_no LIMIT $1", maintainRuns)
	if err != nil {
		return wrap(err)
	}
	nos, err := pgx.CollectRows(rows, pgx.RowTo[int64])
	if err != nil {
		return wrap(err)
	}
	for _, no := range nos {
		if err := s.resumeSweep(ctx, no); err != nil {
			return err
		}
	}
	return nil
}

// cleanQueues deletes queues that no backlog refers to and that were not used for a
// day. A queue an AddTasks holds is skipped, and one it locks later is created again.
func (s *Store) cleanQueues(ctx context.Context, now time.Time) error {
	return s.tx(ctx, func(tx pgx.Tx, _ time.Time) error {
		rows, err := tx.Query(ctx, `SELECT key FROM queues c
			WHERE last_used_at < $1 AND in_flight = 0 AND (blocked_until IS NULL OR blocked_until < $2)
				AND NOT EXISTS (SELECT 1 FROM backlogs b WHERE b.queue_key = c.key)
			ORDER BY key LIMIT $3 FOR UPDATE SKIP LOCKED`, now.Add(-queueIdle), now, maintainBacklogs)
		if err != nil {
			return err
		}
		keys, err := pgx.CollectRows(rows, pgx.RowTo[string])
		if err != nil || len(keys) == 0 {
			return err
		}
		// The check is repeated in a statement of its own: it sees the backlogs of an
		// AddTasks that committed after the first statement began and before the row
		// was locked. An AddTasks that comes later waits for this delete.
		_, err = tx.Exec(ctx, `DELETE FROM queues q WHERE q.key = ANY($1)
			AND NOT EXISTS (SELECT 1 FROM backlogs b WHERE b.queue_key = q.key)`, keys)
		return err
	})
}

// reportReplica records that this replica is alive and forgets the ones that are not.
func (s *Store) reportReplica(ctx context.Context, now time.Time) error {
	s.mu.Lock()
	id := s.replica
	s.mu.Unlock()
	batch := &pgx.Batch{}
	batch.Queue("UPDATE replicas SET seen_at = $2 WHERE replica_id = $1", id, now)
	batch.Queue("DELETE FROM replicas WHERE seen_at < $1", now.Add(-replicaIdle))
	return wrap(s.db.SendBatch(ctx, batch).Close())
}

// retain applies the retention periods: old partitions are dropped, and ended runs
// are deleted with their backlogs, statistics and events.
func (s *Store) retain(ctx context.Context, now time.Time) error {
	if s.keepTasks > 0 {
		if err := s.dropOldPartitions(ctx, now.Add(-s.keepTasks)); err != nil {
			return wrap(err)
		}
	}
	if s.keepRuns <= 0 {
		return nil
	}
	// The run is locked before its backlogs, against the lock order: these backlogs
	// have no work left, so nothing else locks them.
	rows, err := s.db.Query(ctx, `WITH old AS (
			SELECT run_no FROM runs r WHERE state IN ('completed', 'cancelled', 'expired', 'aborted') AND finished_at < $1
				AND NOT EXISTS (SELECT 1 FROM backlogs b WHERE b.run_no = r.run_no AND (b.pending > 0 OR b.in_flight > 0))
			ORDER BY run_no LIMIT $2 FOR UPDATE SKIP LOCKED),
		codes AS (DELETE FROM backlog_codes WHERE run_no IN (SELECT run_no FROM old)),
		logs AS (DELETE FROM backlogs WHERE run_no IN (SELECT run_no FROM old))
		DELETE FROM runs WHERE run_no IN (SELECT run_no FROM old) RETURNING run_no`, now.Add(-s.keepRuns), maintainRuns)
	if err != nil {
		return wrap(err)
	}
	nos, err := pgx.CollectRows(rows, pgx.RowTo[int64])
	if err != nil {
		return wrap(err)
	}
	if len(nos) == 0 {
		return nil
	}
	s.forgetAll()
	// The other replicas cache the deleted runs too, and a run ID is free again.
	_, err = s.db.Exec(ctx, "SELECT pg_notify($1, $2)", channelRuns, runsDeleted)
	return wrap(err)
}

// runsDeleted is the payload on the runs channel that tells replicas to drop their
// run cache.
const runsDeleted = "deleted"

// AuditReport is the result of a ledger audit. Every figure but Checked should be zero.
type AuditReport struct {
	Checked        int // backlogs compared
	LedgerDrift    int // backlogs whose pending or in-flight count differs from their tasks
	OrphanBacklogs int // backlogs whose queue row is missing
}

// Audit compares the ledger of up to sample active backlogs with their tasks. One
// statement, so it sees one snapshot and can run under load.
func (s *Store) Audit(ctx context.Context, sample int) (AuditReport, error) {
	var r AuditReport
	err := s.db.QueryRow(ctx, `WITH s AS (
			SELECT queue_key, run_no, pool_id, run_created_on, pending, in_flight FROM backlogs
			WHERE pending > 0 OR in_flight > 0 ORDER BY random() LIMIT $1)
		SELECT count(*),
			count(*) FILTER (WHERE s.pending <> c.pending OR s.in_flight <> c.in_flight),
			count(*) FILTER (WHERE NOT EXISTS (SELECT 1 FROM queues q WHERE q.key = s.queue_key))
		FROM s, LATERAL (
			SELECT count(*) FILTER (WHERE t.state = 'pending') AS pending, count(*) FILTER (WHERE t.state = 'in_flight') AS in_flight
			FROM tasks t WHERE t.run_created_on = s.run_created_on AND t.run_no = s.run_no AND t.queue_key = s.queue_key
				AND t.pool_id = s.pool_id AND t.state IN ('pending', 'in_flight')) c`, sample).Scan(&r.Checked, &r.LedgerDrift, &r.OrphanBacklogs)
	return r, wrap(err)
}

// audit runs the ledger audit every auditEvery rounds and reports what it finds.
func (s *Store) audit(ctx context.Context, _ time.Time) error {
	if s.rounds.Add(1)%auditEvery != 0 {
		return nil
	}
	r, err := s.Audit(ctx, auditSample)
	if err != nil {
		return err
	}
	if r.LedgerDrift > 0 || r.OrphanBacklogs > 0 {
		s.log.Error("pgstore: the ledger audit found differences", "checked", r.Checked,
			"ledger_drift", r.LedgerDrift, "orphan_backlogs", r.OrphanBacklogs)
	}
	return nil
}

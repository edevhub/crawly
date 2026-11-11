// Package pgstore is a store.Store on PostgreSQL (ADR 0002). State survives restarts
// and several Frontier replicas can share one database.
//
// Every method is one short transaction at READ COMMITTED; Dispatch is one transaction
// per queue it dispatches from. Locks are taken in the order ADR 0002 gives, and the
// scheduling arithmetic is the sched package's, run in Go inside the transaction.
package pgstore

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"math"
	"math/rand/v2"
	"sync"
	"sync/atomic"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgtype"
	"github.com/jackc/pgx/v5/pgxpool"
	"google.golang.org/protobuf/proto"

	policyv1 "github.com/edevhub/crawly/internal/gen/frontier/policy/v1"
	"github.com/edevhub/crawly/internal/store"
)

// Store implements store.Store on PostgreSQL.
type Store struct {
	db              *pgxpool.Pool
	pol             store.Policies
	clock           func() time.Time // nil: the database clock
	log             *slog.Logger
	listenDSN       string
	migrationsTable string
	keepTasks       time.Duration
	keepRuns        time.Duration
	ids             idGen
	deadlocks       atomic.Int64
	rounds          atomic.Int64 // Maintain rounds, to pace the ledger audit

	stop   context.CancelFunc // ends the listener
	ctx    context.Context    //nolint:containedctx // the lifetime of the listener, ended by Close
	listen sync.Once
	ready  chan struct{} // closed once the listener has connected, or failed to
	lerr   error         // why the listener is not connected; nil while it is

	mu       sync.Mutex
	runsByID map[store.RunID]*runMeta
	runsByNo map[int64]*runMeta
	poolIDs  map[store.PoolName]int16
	poolName map[int16]store.PoolName
	months   map[time.Time]bool
	totals   map[int64]*total
	replica  string
	subs     map[chan store.Notification]struct{}
}

// Option configures a Store.
type Option func(*Store)

// WithClock replaces the database clock, for tests.
func WithClock(now func() time.Time) Option { return func(s *Store) { s.clock = now } }

// WithLogger sets the logger. The default is slog.Default.
func WithLogger(l *slog.Logger) Option { return func(s *Store) { s.log = l } }

// WithMigrationsTable names the table the migrations were recorded in, when it is not
// DefaultMigrationsTable. The store reads it to check the schema version.
func WithMigrationsTable(table string) Option {
	return func(s *Store) {
		if table != "" {
			s.migrationsTable = table
		}
	}
}

// WithListenDSN sets the connection used for LISTEN. It must reach the database
// directly: LISTEN does not work through a transaction pooler. The default is the
// store's own DSN.
func WithListenDSN(dsn string) Option { return func(s *Store) { s.listenDSN = dsn } }

// WithRetention sets how long finished work is kept: task and attempt partitions are
// dropped once every run in them was created more than tasks ago, and ended runs are
// deleted with their statistics runs after they finished. Zero keeps them for ever.
func WithRetention(tasks, runs time.Duration) Option {
	return func(s *Store) { s.keepTasks, s.keepRuns = tasks, runs }
}

// New connects to the database, creates the schema if it is missing and returns a
// store that resolves limits through pol. Close releases its connections.
func New(ctx context.Context, dsn string, pol store.Policies, opts ...Option) (*Store, error) {
	cfg, err := pgxpool.ParseConfig(dsn)
	if err != nil {
		return nil, fmt.Errorf("pgstore: %w", err)
	}
	params := cfg.ConnConfig.RuntimeParams
	params["timezone"] = "UTC" // run dates are part of task IDs and partition routing
	for k, v := range map[string]string{
		"application_name":                    "crawly-frontier",
		"statement_timeout":                   "2000",
		"lock_timeout":                        "1000",
		"idle_in_transaction_session_timeout": "5000",
	} {
		if _, set := params[k]; !set {
			params[k] = v
		}
	}
	db, err := pgxpool.NewWithConfig(ctx, cfg)
	if err != nil {
		return nil, fmt.Errorf("pgstore: %w", err)
	}
	s := &Store{
		db: db, pol: pol, log: slog.Default(), listenDSN: dsn, ready: make(chan struct{}),
		migrationsTable: DefaultMigrationsTable,
		runsByID:        map[store.RunID]*runMeta{}, runsByNo: map[int64]*runMeta{},
		poolIDs: map[store.PoolName]int16{}, poolName: map[int16]store.PoolName{},
		months: map[time.Time]bool{}, totals: map[int64]*total{},
		subs: map[chan store.Notification]struct{}{},
	}
	s.ctx, s.stop = context.WithCancel(context.WithoutCancel(ctx))
	for _, o := range opts {
		o(s)
	}
	if err := s.checkSchema(ctx); err != nil {
		s.Close()
		return nil, err
	}
	now, err := s.Now(ctx)
	if err == nil {
		err = s.ensureMonths(ctx, now)
	}
	if err != nil {
		s.Close()
		return nil, err
	}
	return s, nil
}

// Close stops the listener and closes the connections.
func (s *Store) Close() {
	s.stop()
	s.db.Close()
}

// Deadlocks is how many transactions this store retried after a deadlock. Under the
// lock order of ADR 0002 it stays at zero; anything else is a bug.
func (s *Store) Deadlocks() int64 { return s.deadlocks.Load() }

// ensureMonths creates the partitions for this month and the next.
func (s *Store) ensureMonths(ctx context.Context, now time.Time) error {
	if err := s.ensureMonth(ctx, now); err != nil {
		return err
	}
	return s.ensureMonth(ctx, month(now).AddDate(0, 1, 0))
}

// ---- transactions

// querier is what the pool and a transaction have in common.
type querier interface {
	Exec(ctx context.Context, sql string, args ...any) (pgconn.CommandTag, error)
	Query(ctx context.Context, sql string, args ...any) (pgx.Rows, error)
	QueryRow(ctx context.Context, sql string, args ...any) pgx.Row
}

const maxRetries = 3

// SQLSTATE codes the store retries.
const (
	codeDeadlock      = "40P01"
	codeSerialization = "40001"
	codeLockTimeout   = "55P03"
)

func pgCode(err error) string {
	if pg, ok := errors.AsType[*pgconn.PgError](err); ok {
		return pg.Code
	}
	return ""
}

func retryable(err error) bool {
	switch pgCode(err) {
	case codeDeadlock, codeSerialization, codeLockTimeout:
		return true
	}
	return false
}

// errRunClosed rolls a batch of tasks back because the run closed while it was added.
var errRunClosed = errors.New("pgstore: run closed during the batch")

// wrap turns an error of the driver into the store's vocabulary.
func wrap(err error) error {
	switch {
	case err == nil:
		return nil
	case errors.Is(err, context.Canceled), errors.Is(err, context.DeadlineExceeded):
		return err
	case errors.Is(err, store.ErrNotFound), errors.Is(err, store.ErrConflict), errors.Is(err, store.ErrRunClosed),
		errors.Is(err, store.ErrLeaseStale), errors.Is(err, store.ErrLeaseUnknown), errors.Is(err, store.ErrUnavailable),
		errors.Is(err, errRunClosed):
		return err
	case pgCode(err) != "", errors.Is(err, pgx.ErrNoRows):
		// An answer from the server, though not the expected one.
		return fmt.Errorf("pgstore: %w", err)
	}
	// Not an answer from the server: the connection failed.
	return fmt.Errorf("%w: %w", store.ErrUnavailable, err)
}

// tx runs fn in one transaction and commits it. It retries deadlocks, serialisation
// failures and lock timeouts, so fn must reset whatever it produces.
func (s *Store) tx(ctx context.Context, fn func(tx pgx.Tx, now time.Time) error) error {
	var err error
	for try := 0; ; try++ {
		if err = s.once(ctx, fn); !retryable(err) {
			return wrap(err)
		}
		if pgCode(err) == codeDeadlock {
			s.deadlocks.Add(1)
			s.log.Error("pgstore: deadlock; the lock order of ADR 0002 was broken", "err", err)
		}
		if try == maxRetries {
			break
		}
		pause := time.Duration(try+1) * (5*time.Millisecond + rand.N(20*time.Millisecond)) //nolint:gosec // G404: jitter
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(pause):
		}
	}
	return fmt.Errorf("%w: %w", store.ErrUnavailable, err)
}

func (s *Store) once(ctx context.Context, fn func(tx pgx.Tx, now time.Time) error) error {
	tx, err := s.db.Begin(ctx)
	if err != nil {
		return err
	}
	defer rollback(ctx, tx)
	now, err := s.now(ctx, tx)
	if err != nil {
		return err
	}
	if err := fn(tx, now); err != nil {
		return err
	}
	return tx.Commit(ctx)
}

// rollback abandons a transaction. It is a no-op after a commit.
func rollback(ctx context.Context, tx pgx.Tx) {
	c, cancel := context.WithTimeout(context.WithoutCancel(ctx), time.Second)
	defer cancel()
	_ = tx.Rollback(c)
}

// Now returns the store clock: the database's, unless a test replaced it.
func (s *Store) Now(ctx context.Context) (time.Time, error) {
	now, err := s.now(ctx, s.db)
	return now, wrap(err)
}

func (s *Store) now(ctx context.Context, q querier) (time.Time, error) {
	if s.clock != nil {
		return s.clock().UTC(), nil
	}
	var now time.Time
	err := q.QueryRow(ctx, "SELECT now()").Scan(&now)
	return now.UTC(), err
}

// ---- time values

// forever stands for the readiness value 'infinity': the backlog has pending tasks
// and waits for an event, not for a time.
var forever = time.Date(9999, 12, 31, 0, 0, 0, 0, time.UTC)

// tsz encodes a time that is never NULL: the zero time is '-infinity'.
func tsz(t time.Time) pgtype.Timestamptz {
	switch {
	case t.IsZero():
		return pgtype.Timestamptz{InfinityModifier: pgtype.NegativeInfinity, Valid: true}
	case !t.Before(forever):
		return pgtype.Timestamptz{InfinityModifier: pgtype.Infinity, Valid: true}
	}
	return pgtype.Timestamptz{Time: t, Valid: true}
}

// tszNull encodes a time whose zero value is NULL.
func tszNull(t time.Time) pgtype.Timestamptz {
	if t.IsZero() {
		return pgtype.Timestamptz{}
	}
	return tsz(t)
}

// timeOf decodes both: NULL and '-infinity' are the zero time, 'infinity' is forever.
func timeOf(v pgtype.Timestamptz) time.Time {
	switch {
	case !v.Valid, v.InfinityModifier == pgtype.NegativeInfinity:
		return time.Time{}
	case v.InfinityModifier == pgtype.Infinity:
		return forever
	}
	return v.Time.UTC()
}

func earliest(a, b time.Time) time.Time {
	if a.IsZero() || (!b.IsZero() && b.Before(a)) {
		return b
	}
	return a
}

func latest(a, b time.Time) time.Time {
	if b.After(a) {
		return b
	}
	return a
}

// ---- runs

// runMeta is the part of a run that never changes, cached for the life of the process.
type runMeta struct {
	no        int64
	createdOn time.Time // the date its tasks are partitioned by
	day       uint16
	spec      store.RunSpec
	createdAt time.Time
}

const metaColumns = `run_no, run_id, spec_hash, created_on, labels, policy, key_rule, deadline, max_tasks, max_depth, created_at,
	abort_codes, max_failed, max_consecutive_failures`

func scanMeta(row pgx.Row) (*runMeta, error) {
	var (
		m        runMeta
		id       string
		pol      []byte
		rule     int16
		deadline pgtype.Timestamptz
		maxTasks *int64
		maxDepth *int32
		maxFail  *int64
		maxRow   *int32
	)
	if err := row.Scan(&m.no, &id, &m.spec.Hash, &m.createdOn, &m.spec.Labels, &pol, &rule, &deadline, &maxTasks, &maxDepth, &m.createdAt,
		&m.spec.Limits.AbortOnCodes, &maxFail, &maxRow); err != nil {
		return nil, err
	}
	m.createdOn, m.createdAt = m.createdOn.UTC(), m.createdAt.UTC()
	m.day = dayOf(m.createdOn)
	m.spec.ID, m.spec.KeyRule = store.RunID(id), store.KeyRule(rule)
	m.spec.Limits.Deadline = timeOf(deadline)
	if maxTasks != nil && *maxTasks > 0 {
		m.spec.Limits.MaxTasks = uint64(*maxTasks)
	}
	if maxDepth != nil && *maxDepth > 0 {
		m.spec.Limits.MaxDepth = uint32(*maxDepth)
	}
	if maxFail != nil && *maxFail > 0 {
		m.spec.Limits.MaxFailed = uint64(*maxFail)
	}
	if maxRow != nil && *maxRow > 0 {
		m.spec.Limits.MaxConsecutiveFailures = uint32(*maxRow)
	}
	if len(pol) > 0 {
		m.spec.Policy = &policyv1.RunPolicy{}
		if err := proto.Unmarshal(pol, m.spec.Policy); err != nil {
			return nil, fmt.Errorf("pgstore: run %s has an unreadable policy: %w", id, err)
		}
	}
	return &m, nil
}

func (s *Store) remember(m *runMeta) *runMeta {
	s.mu.Lock()
	defer s.mu.Unlock()
	if have, ok := s.runsByNo[m.no]; ok {
		return have
	}
	s.runsByNo[m.no], s.runsByID[m.spec.ID] = m, m
	return m
}

func (s *Store) forget(m *runMeta) {
	s.mu.Lock()
	delete(s.runsByNo, m.no)
	delete(s.runsByID, m.spec.ID)
	delete(s.totals, m.no)
	s.mu.Unlock()
}

// forgetAll empties the run cache. Another replica deleted runs: a run ID may be in
// use again for a new run.
func (s *Store) forgetAll() {
	s.mu.Lock()
	clear(s.runsByNo)
	clear(s.runsByID)
	clear(s.totals)
	s.mu.Unlock()
}

// run finds a run by its ID, or returns store.ErrNotFound.
func (s *Store) run(ctx context.Context, q querier, id store.RunID) (*runMeta, error) {
	s.mu.Lock()
	m, ok := s.runsByID[id]
	s.mu.Unlock()
	if ok {
		return m, nil
	}
	m, err := scanMeta(q.QueryRow(ctx, "SELECT "+metaColumns+" FROM runs WHERE run_id = $1", string(id)))
	if errors.Is(err, pgx.ErrNoRows) {
		return nil, store.ErrNotFound
	}
	if err != nil {
		return nil, err
	}
	return s.remember(m), nil
}

// metas finds runs by their internal keys. Runs that no longer exist are left out.
func (s *Store) metas(ctx context.Context, q querier, nos []int64) (map[int64]*runMeta, error) {
	out := make(map[int64]*runMeta, len(nos))
	var missing []int64
	s.mu.Lock()
	for _, no := range nos {
		if m, ok := s.runsByNo[no]; ok {
			out[no] = m
		} else if _, seen := out[no]; !seen {
			missing = append(missing, no)
		}
	}
	s.mu.Unlock()
	if len(missing) == 0 {
		return out, nil
	}
	rows, err := q.Query(ctx, "SELECT "+metaColumns+" FROM runs WHERE run_no = ANY($1)", missing)
	if err != nil {
		return nil, err
	}
	found, err := pgx.CollectRows(rows, func(row pgx.CollectableRow) (*runMeta, error) { return scanMeta(row) })
	if err != nil {
		return nil, err
	}
	for _, m := range found {
		m = s.remember(m)
		out[m.no] = m
	}
	return out, nil
}

func (s *Store) meta(ctx context.Context, q querier, no int64) (*runMeta, error) {
	ms, err := s.metas(ctx, q, []int64{no})
	if err != nil {
		return nil, err
	}
	m, ok := ms[no]
	if !ok {
		return nil, store.ErrNotFound
	}
	return m, nil
}

var runStates = map[string]store.RunState{
	"open": store.RunOpen, "sealed": store.RunSealed, "completed": store.RunCompleted,
	"cancelled": store.RunCancelled, "expired": store.RunExpired, "aborted": store.RunAborted,
}

// parseRunState reads a run state. A name this build does not know was written by a newer
// one, and a run in a state nobody here knows must not be crawled: it reads as cancelled,
// which is final and drains its tasks.
func (s *Store) parseRunState(name string) store.RunState {
	if st, ok := runStates[name]; ok {
		return st
	}
	s.log.Error("pgstore: a run is in a state this build does not know; treating it as cancelled", "state", name)
	return store.RunCancelled
}

func runStateName(st store.RunState) string {
	for name, v := range runStates {
		if v == st {
			return name
		}
	}
	return ""
}

// ---- pools

// lookupPool returns the key of a pool, if a task was ever added to it.
func (s *Store) lookupPool(ctx context.Context, q querier, name store.PoolName) (int16, bool, error) {
	s.mu.Lock()
	id, ok := s.poolIDs[name]
	s.mu.Unlock()
	if ok {
		return id, true, nil
	}
	err := q.QueryRow(ctx, "SELECT pool_id FROM pools WHERE name = $1", string(name)).Scan(&id)
	if errors.Is(err, pgx.ErrNoRows) {
		return 0, false, nil
	}
	if err != nil {
		return 0, false, err
	}
	s.rememberPool(id, name)
	return id, true, nil
}

func (s *Store) rememberPool(id int16, name store.PoolName) {
	s.mu.Lock()
	s.poolIDs[name], s.poolName[id] = id, name
	s.mu.Unlock()
}

// poolOf returns the name of a pool key.
func (s *Store) poolOf(ctx context.Context, q querier, id int16) (store.PoolName, error) {
	s.mu.Lock()
	name, ok := s.poolName[id]
	s.mu.Unlock()
	if ok {
		return name, nil
	}
	var n string
	if err := q.QueryRow(ctx, "SELECT name FROM pools WHERE pool_id = $1", id).Scan(&n); err != nil {
		return "", err
	}
	s.rememberPool(id, store.PoolName(n))
	return store.PoolName(n), nil
}

// ensurePools creates the pools that do not exist yet and returns the key of each.
func (s *Store) ensurePools(ctx context.Context, names []store.PoolName) (map[store.PoolName]int16, error) {
	out := make(map[store.PoolName]int16, len(names))
	var missing []string
	s.mu.Lock()
	for _, n := range names {
		if id, ok := s.poolIDs[n]; ok {
			out[n] = id
		} else {
			missing = append(missing, string(n))
		}
	}
	s.mu.Unlock()
	if len(missing) == 0 {
		return out, nil
	}
	tag, err := s.db.Exec(ctx, "INSERT INTO pools (name) SELECT unnest($1::text[]) ON CONFLICT DO NOTHING", missing)
	if err != nil {
		return nil, err
	}
	rows, err := s.db.Query(ctx, "SELECT pool_id, name FROM pools WHERE name = ANY($1)", missing)
	if err != nil {
		return nil, err
	}
	var id int16
	var name string
	if _, err := pgx.ForEachRow(rows, []any{&id, &name}, func() error {
		s.rememberPool(id, store.PoolName(name))
		out[store.PoolName(name)] = id
		return nil
	}); err != nil {
		return nil, err
	}
	if tag.RowsAffected() > 0 {
		// Listeners subscribe to the new pool's channel.
		if _, err := s.db.Exec(ctx, "SELECT pg_notify($1, '')", channelPools); err != nil {
			return nil, err
		}
	}
	return out, nil
}

// ---- queue and backlog rows

// qrow is a queues row held under a row lock.
type qrow struct {
	key          store.QueueKey
	tat          time.Time
	inFlight     int
	vclock       float64
	blockedUntil time.Time
	blockReason  string
	strain       store.Strain
}

// brow is a backlogs row held under a row lock, with the changes to write back.
type brow struct {
	run      int64
	pool     int16
	pending  int64
	inFlight int
	vtime    float64
	ownTat   time.Time
	// The readiness of the backlog's tasks, one lane per handler that has tasks here.
	lanes []*lane
	dirty bool
	// Counter increments.
	attempts, succeeded, failed, expired, cancelled, blocks int64
	// Run state, read after the lock was taken.
	meta  *runMeta
	state store.RunState
	live  bool // the run is open or sealed, and not paused
	eff   store.Effective
	// Dispatch only.
	short bool // a claim returned fewer tasks than asked: nothing more is due for this session
}

// lane is the readiness of the tasks of one handler in a backlog. Politeness belongs
// to the backlog, so every lane waits for the same admission; what differs is when the
// handler's own tasks are due.
type lane struct {
	handler string
	ready   time.Time // zero: NULL (not dispatchable); forever: waits for an event
	dirty   bool
	// Dispatch only.
	mine  bool      // the dispatching session can run the handler's tasks
	short bool      // a claim for the session returned fewer tasks than asked
	due   time.Time // then: when the lane's next task is due; zero if none can be claimed
	gone  bool      // the lane has no task that can be claimed: its row is deleted
}

func (l *lane) setReady(t time.Time) {
	if !l.ready.Equal(t) {
		l.ready, l.dirty = t, true
	}
}

// waiting reports whether the lane could have a task admitted at now, or waits for an
// event that has not happened.
func (l *lane) waiting(now time.Time) bool {
	return !l.ready.IsZero() && (!l.ready.After(now) || l.ready.Equal(forever))
}

// setReady sets the readiness of every lane: the sites that change what all of a
// backlog's tasks wait for.
func (b *brow) setReady(t time.Time) {
	for _, l := range b.lanes {
		l.setReady(t)
	}
}

// laneOf returns the lane of the handler, adding it when the backlog has none yet.
func (b *brow) laneOf(handler string) *lane {
	for _, l := range b.lanes {
		if l.handler == handler {
			return l
		}
	}
	l := &lane{handler: handler, dirty: true}
	b.lanes = append(b.lanes, l)
	return l
}

// readyLanes reports whether some lane that match accepts is waiting at now.
func (b *brow) readyLanes(now time.Time, match func(*lane) bool) bool {
	for _, l := range b.lanes {
		if l.waiting(now) && match(l) {
			return true
		}
	}
	return false
}

// lockQueue locks a queue row for the politeness update. With skipLocked it returns
// nil when another transaction holds the row.
func lockQueue(ctx context.Context, tx pgx.Tx, key store.QueueKey, skipLocked bool) (*qrow, error) {
	sql := `SELECT tat, in_flight, vclock, blocked_until, coalesce(block_reason, ''),
		slow_interval_ns, strain_at, strain_steps, last_strain_at, ceiling_at FROM queues WHERE key = $1 FOR NO KEY UPDATE`
	if skipLocked {
		sql += " SKIP LOCKED"
	}
	q := &qrow{key: key}
	var tat, blocked, strainAt, lastStrain, ceiling pgtype.Timestamptz
	var slow, steps int64
	err := tx.QueryRow(ctx, sql, string(key)).Scan(&tat, &q.inFlight, &q.vclock, &blocked, &q.blockReason,
		&slow, &strainAt, &steps, &lastStrain, &ceiling)
	if errors.Is(err, pgx.ErrNoRows) {
		return nil, nil //nolint:nilnil // no row is an answer here, not a failure
	}
	if err != nil {
		return nil, err
	}
	q.tat, q.blockedUntil = timeOf(tat), timeOf(blocked)
	q.strain = store.Strain{Interval: time.Duration(slow), At: timeOf(strainAt), Steps: uint64(max(steps, 0)), Last: timeOf(lastStrain),
		CeilingAt: timeOf(ceiling)}
	return q, nil
}

// lockBacklogs locks, in primary key order, the queue's backlogs that have pending
// tasks, and the backlog of (run, pool) whether it has any or not. Then it reads the
// state of their runs: after the locks, so a pause or a cancel that committed while
// this transaction waited is seen.
func (s *Store) lockBacklogs(ctx context.Context, tx pgx.Tx, key store.QueueKey, run int64, pool int16) ([]*brow, error) {
	rows, err := tx.Query(ctx, `SELECT run_no, pool_id, pending, in_flight, vtime, own_tat FROM backlogs
		WHERE queue_key = $1 AND (pending > 0 OR (run_no = $2 AND pool_id = $3))
		ORDER BY run_no, pool_id FOR UPDATE`, string(key), run, pool)
	if err != nil {
		return nil, err
	}
	bs, err := pgx.CollectRows(rows, func(row pgx.CollectableRow) (*brow, error) {
		b := &brow{}
		var own pgtype.Timestamptz
		if err := row.Scan(&b.run, &b.pool, &b.pending, &b.inFlight, &b.vtime, &own); err != nil {
			return nil, err
		}
		b.ownTat = timeOf(own)
		return b, nil
	})
	if err != nil {
		return nil, err
	}
	// The lanes are read under the backlogs' locks: every writer of a lane holds its
	// backlog.
	lrows, err := tx.Query(ctx, "SELECT run_no, pool_id, handler, ready_at FROM lanes WHERE queue_key = $1 ORDER BY run_no, pool_id, handler", string(key))
	if err != nil {
		return nil, err
	}
	byKey := make(map[[2]int64]*brow, len(bs))
	for _, b := range bs {
		byKey[[2]int64{b.run, int64(b.pool)}] = b
	}
	var (
		lrun    int64
		lpool   int16
		lhandle string
		lready  pgtype.Timestamptz
	)
	if _, err := pgx.ForEachRow(lrows, []any{&lrun, &lpool, &lhandle, &lready}, func() error {
		if b := byKey[[2]int64{lrun, int64(lpool)}]; b != nil {
			b.lanes = append(b.lanes, &lane{handler: lhandle, ready: timeOf(lready)})
		}
		return nil
	}); err != nil {
		return nil, err
	}
	return bs, s.loadRuns(ctx, tx, key, bs, false)
}

// loadRuns reads the state of the backlogs' runs. With share it also locks the run
// rows, so a lifecycle change waits for this transaction.
func (s *Store) loadRuns(ctx context.Context, tx pgx.Tx, key store.QueueKey, bs []*brow, share bool) error {
	if len(bs) == 0 {
		return nil
	}
	nos := make([]int64, 0, len(bs))
	for _, b := range bs {
		nos = append(nos, b.run)
	}
	metas, err := s.metas(ctx, tx, nos)
	if err != nil {
		return err
	}
	sql := "SELECT run_no, state::text, paused FROM runs WHERE run_no = ANY($1)"
	if share {
		sql += " ORDER BY run_no FOR SHARE"
	}
	rows, err := tx.Query(ctx, sql, nos)
	if err != nil {
		return err
	}
	type state struct {
		st     store.RunState
		paused bool
	}
	states := map[int64]state{}
	var no int64
	var name string
	var paused bool
	if _, err := pgx.ForEachRow(rows, []any{&no, &name, &paused}, func() error {
		states[no] = state{st: s.parseRunState(name), paused: paused}
		return nil
	}); err != nil {
		return err
	}
	for _, b := range bs {
		st, ok := states[b.run]
		b.meta = metas[b.run]
		if !ok || b.meta == nil {
			b.state, b.live = store.RunCancelled, false // the run was deleted
			continue
		}
		b.state = st.st
		b.live = !st.st.Ended() && !st.paused
		b.eff = s.pol.For(b.meta.spec.ID, b.meta.spec.Policy, key)
	}
	return nil
}

// admission is how many tasks of the backlog the limits admit now, and when the rate
// limit admits the next one. The backlog is admitted against its own limits under a
// run override, against the queue's otherwise.
func admission(q *qrow, b *brow, now time.Time) (admit int, at time.Time) {
	queue, own := b.limits(q)
	if !b.eff.Own {
		return schedAdmitted(queue, q.tat, now, q.inFlight)
	}
	admit, at = schedAdmitted(own, b.ownTat, now, b.inFlight)
	if b.eff.Adaptive.Slowed(b.eff.Queue, q.strain) {
		// A run with limits of its own is no faster than the slowed queue.
		qa, qat := schedAdmitted(queue, q.tat, now, q.inFlight)
		admit, at = min(admit, qa), latest(at, qat)
	}
	return admit, at
}

// limits are the queue's and the run's limits as they are in force: slowed, while the
// queue's site is under strain.
func (b *brow) limits(q *qrow) (queue, own store.Limits) {
	return b.eff.Adaptive.Apply(b.eff.Queue, q.strain), b.eff.Adaptive.Apply(b.eff.Limits, q.strain)
}

// flush writes the queue row and the changed backlog rows back.
func flush(ctx context.Context, tx pgx.Tx, now time.Time, q *qrow, bs []*brow) error {
	batch := &pgx.Batch{}
	if q != nil {
		batch.Queue(`UPDATE queues SET tat = $2, in_flight = $3, vclock = $4, blocked_until = $5, block_reason = $6, last_used_at = $7,
				slow_interval_ns = $8, strain_at = $9, strain_steps = $10, last_strain_at = $11, ceiling_at = $12
			WHERE key = $1`, string(q.key), tsz(q.tat), q.inFlight, q.vclock, tszNull(q.blockedUntil), nullString(q.blockReason), now,
			int64(q.strain.Interval), tszNull(q.strain.At), int64(min(q.strain.Steps, math.MaxInt64)), tszNull(q.strain.Last), tszNull(q.strain.CeilingAt))
	}
	for _, b := range bs {
		if b.dirty {
			batch.Queue(`UPDATE backlogs SET pending = $4, in_flight = $5, vtime = $6, own_tat = $7,
				attempts = attempts + $8, succeeded = succeeded + $9, failed = failed + $10, expired = expired + $11,
				cancelled = cancelled + $12, queue_blocks = queue_blocks + $13
				WHERE queue_key = $1 AND run_no = $2 AND pool_id = $3`,
				string(q.key), b.run, b.pool, b.pending, b.inFlight, b.vtime, tsz(b.ownTat),
				b.attempts, b.succeeded, b.failed, b.expired, b.cancelled, b.blocks)
		}
		// A backlog with no tasks has no lanes: they come back with its next task.
		if b.pending == 0 && (b.dirty || len(b.lanes) > 0) {
			batch.Queue("DELETE FROM lanes WHERE queue_key = $1 AND run_no = $2 AND pool_id = $3", string(q.key), b.run, b.pool)
			continue
		}
		for _, l := range b.lanes {
			switch {
			case l.gone:
				batch.Queue("DELETE FROM lanes WHERE queue_key = $1 AND run_no = $2 AND pool_id = $3 AND handler = $4",
					string(q.key), b.run, b.pool, l.handler)
			case l.dirty:
				batch.Queue(`INSERT INTO lanes (queue_key, run_no, pool_id, handler, ready_at) VALUES ($1, $2, $3, $4, $5)
					ON CONFLICT (queue_key, run_no, pool_id, handler) DO UPDATE SET ready_at = EXCLUDED.ready_at`,
					string(q.key), b.run, b.pool, l.handler, tszNull(l.ready))
			}
		}
	}
	if batch.Len() == 0 {
		return nil
	}
	return tx.SendBatch(ctx, batch).Close()
}

func nullString(s string) *string {
	if s == "" {
		return nil
	}
	return &s
}

// ---- notifications sent

// Channels of LISTEN / NOTIFY. Notifications carry no payload.
const (
	channelRuns  = "frontier_runs"  // a run event was appended
	channelPools = "frontier_pools" // a pool was created
	channelWork  = "frontier_work_" // + pool name: work became ready in the pool
)

// wake notifies the pools, when the transaction commits.
func (s *Store) wake(ctx context.Context, tx pgx.Tx, pools map[int16]bool) error {
	for id := range pools {
		name, err := s.poolOf(ctx, tx, id)
		if err != nil {
			return err
		}
		if _, err := tx.Exec(ctx, "SELECT pg_notify($1, '')", channelWork+string(name)); err != nil {
			return err
		}
	}
	return nil
}

// wakeWaiting notifies the pools that have work on the queue: a slot or a block changed.
func (s *Store) wakeWaiting(ctx context.Context, tx pgx.Tx, bs []*brow) error {
	pools := map[int16]bool{}
	for _, b := range bs {
		if b.live && b.pending > 0 {
			pools[b.pool] = true
		}
	}
	return s.wake(ctx, tx, pools)
}

// lockKey folds a run key into the second half of a two-key advisory lock. Two runs
// that collide only share a lock.
func lockKey(run int64) int32 {
	return int32(run & 0x7fffffff)
}

var _ store.Store = (*Store)(nil)

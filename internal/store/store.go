// Package store defines what the Frontier keeps between requests (ADR 0001).
//
// Every method is one atomic business transaction. The Frontier core never composes
// several calls into one logical change. Implementations follow the scheduling model of
// ADR 0001 and are checked by the storetest conformance suite.
package store

import (
	"context"
	"errors"
	"slices"
	"time"

	policyv1 "github.com/edevhub/crawly/internal/gen/frontier/policy/v1"
	"github.com/edevhub/crawly/internal/poolname"
	"github.com/edevhub/crawly/internal/sched"
)

type (
	// RunID identifies a run. Callers choose it, which makes CreateRun idempotent.
	RunID string
	// QueueKey names a politeness queue, normally derived from a host or registered domain.
	QueueKey string
	// PoolName names a pool of workers that can execute a task.
	PoolName string
	// TaskID identifies a task within a run. It is opaque: produced by the store, never parsed by callers.
	TaskID string
	// LeaseID identifies one lease on a task. It is opaque: produced by the store, never parsed by callers.
	LeaseID string
	// SessionID identifies a worker session that asked for work.
	SessionID string
	// EventCursor is a position in the run event log.
	EventCursor uint64
	// Fingerprint is the hash that identifies a task for de-duplication within a run.
	Fingerprint [16]byte
)

// DefaultPool is the pool of a task that names none.
const DefaultPool PoolName = poolname.Default

// Sentinel errors returned by every Store implementation.
var (
	ErrNotFound    = errors.New("store: not found")
	ErrConflict    = errors.New("store: conflict")
	ErrRunClosed   = errors.New("store: run closed")
	ErrUnavailable = errors.New("store: unavailable")
	// ErrUnsupported means the deployment cannot do what was asked yet: for the abort rules,
	// a replica that does not know them is still running.
	ErrUnsupported = errors.New("store: not supported by every replica")
	// ErrLeaseStale and ErrLeaseUnknown reject discovered tasks whose parent lease is
	// not live.
	ErrLeaseStale   = errors.New("store: lease is fenced or expired")
	ErrLeaseUnknown = errors.New("store: no such lease")
)

// Store is everything the Frontier keeps between requests.
type Store interface {
	Lifecycle
	Work
	Inspect
	Background
}

// Lifecycle serves the run API.
type Lifecycle interface {
	// CreateRun is idempotent on spec.ID: the same spec returns the existing run, a
	// different spec (compared by spec.Hash) returns ErrConflict.
	CreateRun(ctx context.Context, spec RunSpec) (Run, error)
	// SealRun stops seeding. An empty sealed run completes in the same call.
	SealRun(ctx context.Context, id RunID) (Run, error)
	PauseRun(ctx context.Context, id RunID) (Run, error)
	ResumeRun(ctx context.Context, id RunID) (Run, error)
	// CancelRun reports CANCELLED immediately; the backlog drains eventually.
	CancelRun(ctx context.Context, id RunID) (Run, error)
	GetRun(ctx context.Context, id RunID) (Run, error)
	ListRuns(ctx context.Context, f RunFilter, page Page) ([]Run, string, error)
	// RunEvents returns lifecycle events after the cursor, in commit order, without gaps.
	RunEvents(ctx context.Context, after EventCursor, limit int) ([]RunEvent, error)
	// LastEventCursor is the cursor of the newest event, for watchers that start now.
	LastEventCursor(ctx context.Context) (EventCursor, error)
}

// Work serves the worker protocol, seeding and the reaper.
type Work interface {
	// AddTasks adds seeds (Parent nil; run must be open) or discovered tasks (Parent
	// set; the parent lease must be live). One result per task, in order.
	AddTasks(ctx context.Context, req AddRequest) ([]AddResult, error)
	// Dispatch claims up to req.Slots tasks of req.Pool that the session can run (see
	// DispatchRequest.Handlers), following the scheduling model. It never blocks
	// waiting for work. It can return assignments together with an
	// error; every assignment returned is committed and must be delivered.
	Dispatch(ctx context.Context, req DispatchRequest) (DispatchResult, error)
	// Lease returns what the core needs to decide a transition for a lease. A plain
	// read: it does not fence, Finish does.
	Lease(ctx context.Context, lease LeaseRef) (LeaseInfo, error)
	// ExtendLease moves the expiry to now+by, capped by the lease deadline.
	ExtendLease(ctx context.Context, lease LeaseRef, by time.Duration) (Lease, FinishStatus, error)
	// Finish ends an attempt with a transition decided by the core. Fenced: a lease
	// that is not the task's current one returns Stale or Unknown and changes nothing.
	Finish(ctx context.Context, f Finish) (FinishStatus, error)
	// ExpiredLeases lists leases past their expiry, oldest first, for the reaper.
	ExpiredLeases(ctx context.Context, limit int) ([]LeaseInfo, error)
}

// Inspect serves task and queue inspection.
type Inspect interface {
	GetTask(ctx context.Context, run RunID, id TaskID, fp *Fingerprint) (TaskStatus, error)
	ListTasks(ctx context.Context, f TaskFilter, page Page) ([]TaskStatus, string, error)
	GetQueue(ctx context.Context, key QueueKey) (Queue, error)
	ListQueues(ctx context.Context, f QueueFilter, page Page) ([]Queue, string, error)
	// BlockQueue sets (or with a zero time, clears) an operator block for all runs.
	BlockQueue(ctx context.Context, key QueueKey, until time.Time, reason string) (Queue, error)
}

// Background serves replica startup, the dispatch loop and maintenance.
type Background interface {
	// Start registers the replica and its policy set.
	Start(ctx context.Context, r Replica) error
	// ServeHandlers records that this replica has sessions that can run these handlers.
	// It is called when the sessions change and, while they stay, more often than the
	// grace period of SweepUnserved. SweepUnserved reads it, and so does Dispatch: work
	// in another pool that none of the pool's sessions can run is not given first
	// refusal on a queue.
	ServeHandlers(ctx context.Context, sets []PoolHandlers) error
	// SweepUnserved looks for pending tasks whose handler no session can run. It fails
	// the ones that waited longer than grace (code CodeNoHandler, no attempt recorded)
	// and reports the handlers whose tasks wait. A handler is served from the moment a
	// replica reports it until grace after the last report; a task waits from its
	// creation, and no grace ends before one has passed since the newest replica
	// started. A call is one bounded round: the store keeps where the last round
	// stopped, and a call that comes sooner than a quarter of grace (at most a
	// minute) after another, from any replica, does nothing. Safe to call
	// concurrently.
	SweepUnserved(ctx context.Context, grace time.Duration) (Unserved, error)
	// Notifications delivers coalesced wake-ups until ctx is done. A missed
	// notification only delays work until the next NextReadyAt or poll.
	Notifications(ctx context.Context) (<-chan Notification, error)
	// Maintain runs one bounded round of background work: task TTL expiry, run
	// deadlines, drains. Safe to call concurrently.
	Maintain(ctx context.Context) error
	// Now is the store clock, the only clock used for scheduling.
	Now(ctx context.Context) (time.Time, error)
}

// Policies resolves limits. It is pure and immutable for the lifetime of the process.
type Policies interface {
	Hash() string
	For(run RunID, rp *policyv1.RunPolicy, queue QueueKey) Effective
}

// Effective is the resolved scheduling input for one run on one queue.
type Effective struct {
	Limits Limits        // what this run's tasks are admitted against
	Own    bool          // Limits come from a run override: admitted on the run's own allowance
	Queue  Limits        // the queue's shared limits, consumed by every dispatch
	Weight float64       // fair share on this queue
	Lease  LeaseSettings // duration and maximum duration
}

// Limits are politeness limits in GCRA form: T = max(1/rate, min_delay), burst, concurrency.
type Limits = sched.Limits

// LeaseSettings is the lease duration policy for a run on a queue.
type LeaseSettings struct {
	Duration    time.Duration
	MaxDuration time.Duration
}

// Replica identifies a Frontier replica and the policy set it runs with.
type Replica struct {
	ID         string
	PolicyHash string
}

// CodeNoHandler is the code of a task that no worker had a handler for during the grace
// period.
const CodeNoHandler = "no_handler"

// PoolHandlers is the set of handlers the sessions of one pool can run. The handler
// "" stands for a session that names no handlers and so can run any: while one is
// reported, no handler of the pool counts as unserved.
type PoolHandlers struct {
	Pool     PoolName
	Handlers []string
}

// HandlerRef names a handler within a pool.
type HandlerRef struct {
	Pool    PoolName
	Handler string
}

// FailedTasks counts tasks failed because no session could run their handler.
type FailedTasks struct {
	HandlerRef
	Tasks int
}

// WaitingHandler is a handler whose tasks wait because no session can run it.
type WaitingHandler struct {
	HandlerRef
	Lanes int // the queues and runs with such tasks; counted up to a bound
}

// Unserved is what SweepUnserved found.
type Unserved struct {
	Waiting []WaitingHandler // handlers with pending tasks that no live session can run
	Failed  []FailedTasks    // tasks failed by this call
	// Held is set when tasks wait and nothing may fail yet, because a replica
	// started less than the grace period ago.
	Held bool
	// Report is set on at most one call in a while, cluster-wide: the caller logs
	// what waits then, so that the replicas do not each log the same lines.
	Report bool
}

// Notification is a coalesced wake-up for the dispatch loop or run watchers.
type Notification struct {
	Pool PoolName // work became ready in this pool; empty for run events
	Runs bool     // a run event was appended
}

// Page selects one page of a listing; an empty Token starts from the beginning.
type Page struct {
	Size  int
	Token string
}

// ---- runs

// RunState is the lifecycle state of a run.
type RunState int

// RunOpen accepts seeds; RunSealed takes no more and drains; the other states are final.
// RunAborted is a run a rule of its RunLimits ended.
const (
	RunOpen RunState = iota + 1
	RunSealed
	RunCompleted
	RunCancelled
	RunExpired
	RunAborted
)

// Ended reports whether the run has reached a final state.
func (s RunState) Ended() bool { return s >= RunCompleted }

// KeyRule says how a task URL is mapped to its queue key.
type KeyRule int

// KeyHost, KeyRegisteredDomain and KeyExplicit derive the queue key from the URL host, from its registered domain, or from the key the task names.
const (
	KeyHost KeyRule = iota + 1
	KeyRegisteredDomain
	KeyExplicit
)

// RunLimits caps what a run may do, and says when it ends early.
type RunLimits struct {
	Deadline time.Time // zero: none
	MaxTasks uint64    // 0: none
	MaxDepth uint32    // 0: none
	// The abort rules. Finish evaluates them in the transaction that applies an attempt.
	AbortOnCodes           []string // empty: none; an attempt recorded with one of these codes ends the run
	MaxFailed              uint64   // 0: none; tasks failed for good
	MaxConsecutiveFailures uint32   // 0: none; tasks failed for good with no success applied between
}

// AbortRule is the rule of RunLimits that ended a run.
type AbortRule int

// AbortOnCode is the abort_on_codes rule, AbortMaxFailed max_failed and AbortConsecutiveFailures max_consecutive_failures.
const (
	AbortOnCode AbortRule = iota + 1
	AbortMaxFailed
	AbortConsecutiveFailures
)

// AbortReason is why a run was aborted: the rule, and the attempt that tripped it.
type AbortReason struct {
	Rule AbortRule
	Code string // of the attempt; may be empty
	Task TaskID
}

// HasAbortRules reports whether any abort rule is set.
func (l RunLimits) HasAbortRules() bool {
	return len(l.AbortOnCodes) > 0 || l.MaxFailed > 0 || l.MaxConsecutiveFailures > 0
}

// CountsFailures reports whether a rule needs the run's failure counters.
func (l RunLimits) CountsFailures() bool { return l.MaxFailed > 0 || l.MaxConsecutiveFailures > 0 }

// Trip reports which rule an applied attempt trips. code is the attempt's code,
// failed the number of tasks that have failed for good, run included, and streak the
// number of them in a row. Both stores call it, so the rules read the same in both. When
// several rules trip at once the code rule wins, then the streak, then the total.
func (l RunLimits) Trip(code string, failed, streak uint64) (AbortRule, bool) {
	switch {
	case code != "" && slices.Contains(l.AbortOnCodes, code):
		return AbortOnCode, true
	case l.MaxConsecutiveFailures > 0 && streak >= uint64(l.MaxConsecutiveFailures):
		return AbortConsecutiveFailures, true
	case l.MaxFailed > 0 && failed >= l.MaxFailed:
		return AbortMaxFailed, true
	default:
		return 0, false
	}
}

// RunSpec is the immutable definition of a run.
type RunSpec struct {
	ID      RunID
	Labels  map[string]string
	Policy  *policyv1.RunPolicy // immutable after creation
	Limits  RunLimits
	KeyRule KeyRule
	Paused  bool
	Hash    []byte // canonical hash of the fields above
}

// Run is a run together with its current state and counters.
type Run struct {
	Spec       RunSpec
	State      RunState
	Paused     bool
	CreatedAt  time.Time
	SealedAt   time.Time
	FinishedAt time.Time
	Abort      *AbortReason // set when State is RunAborted
	Stats      RunStats
}

// RunStats counts the tasks and attempts of a run.
type RunStats struct {
	Pending, Delayed, InFlight            uint64
	Succeeded, Failed, Expired, Cancelled uint64
	Attempts, QueueBlocks                 uint64
	Discovered, Duplicates, OverBudget    uint64
	ByCode                                map[string]uint64
	// BlockedQueues are the blocked queues the run's pending and delayed tasks wait on,
	// the ones with the most waiting tasks first, at most MaxBlockedQueues.
	BlockedQueues []BlockedQueue
}

// MaxBlockedQueues bounds RunStats.BlockedQueues.
const MaxBlockedQueues = 20

// BlockedQueue is a blocked queue a run waits on.
type BlockedQueue struct {
	Queue   QueueKey
	Until   time.Time
	Reason  string
	Waiting uint64 // the run's pending and delayed tasks on the queue
}

// RunFilter selects runs by state and labels; empty fields match everything.
type RunFilter struct {
	States []RunState
	Labels map[string]string
}

// RunEventType is the kind of change a RunEvent records.
type RunEventType int

// EventCreated and the events after it record the lifecycle transitions of a run.
const (
	EventCreated RunEventType = iota + 1
	EventSealed
	EventPaused
	EventResumed
	EventCompleted
	EventCancelled
	EventExpired
	// EventAborted is 9, as in the API: 8 is the statistics event, which is not logged.
	EventAborted RunEventType = 9
)

// RunEvent is one entry of the run event log.
type RunEvent struct {
	Cursor EventCursor
	Type   RunEventType
	Run    Run
	At     time.Time
}

// ---- tasks

// AddRequest adds tasks to a run: seeds when Parent is nil, discovered tasks otherwise.
type AddRequest struct {
	Run    RunID
	Parent *LeaseRef // nil: seeds; set: discovered, depth = parent depth + 1
	Tasks  []NewTask
}

// NewTask is a task to be added to a run.
type NewTask struct {
	Fingerprint Fingerprint
	Queue       QueueKey
	Pool        PoolName
	Handler     string // the handler that runs the task; empty: unknown, offered to every session
	Priority    int32
	NotBefore   time.Time     // zero: immediately
	TTL         time.Duration // 0: none
	Spec        []byte        // serialised TaskSpec, opaque to the store
	Invalid     bool          // rejected by the core; kept so results stay in order
}

// AddStatus is the outcome of adding one task.
type AddStatus int

// AddAdded is the success status; the others say why a task was not added.
const (
	AddAdded AddStatus = iota + 1
	AddDuplicate
	AddOverBudget
	AddRunClosed
	AddInvalid
)

// AddResult is the outcome of adding one task, in request order.
type AddResult struct {
	Status AddStatus
	TaskID TaskID
}

// DispatchRequest asks for work for one worker session.
type DispatchRequest struct {
	Session SessionID
	Worker  WorkerInfo
	Pool    PoolName
	// Handlers are the task handlers the session can run. Only tasks of these handlers
	// are claimed, and tasks with no handler, which any session may try. Empty: the
	// session names none, and every task of the pool is claimed.
	Handlers []string
	Slots    int
}

// WorkerInfo identifies a worker process.
type WorkerInfo struct {
	ID      string
	Version string
}

// DispatchResult is the work claimed by a Dispatch call.
type DispatchResult struct {
	Assignments []Assignment
	// Earliest time more work is expected in the pool. Zero: wait for a notification.
	NextReadyAt time.Time
}

// Assignment is a task handed to a worker, with its lease.
type Assignment struct {
	Lease Lease
	Info  LeaseInfo
	Spec  []byte
}

// LeaseRef names one attempt's lease; the attempt number fences stale holders.
type LeaseRef struct {
	ID      LeaseID
	Attempt uint32
}

// Lease is a time-limited claim of a task by a worker.
type Lease struct {
	Ref       LeaseRef
	ExpiresAt time.Time
	Duration  time.Duration
}

// LeaseInfo is the context of a lease: what the core needs to decide an outcome.
type LeaseInfo struct {
	Ref         LeaseRef
	Task        TaskID
	Run         RunID
	RunPolicy   *policyv1.RunPolicy
	Queue       QueueKey
	Pool        PoolName
	Fingerprint Fingerprint
	Depth       uint32
	Parent      TaskID
}

// FinishSource says who ends an attempt, which decides how the lease is checked.
type FinishSource int

// SourceWorker ends an attempt through its live lease; SourceReaper ends one whose lease has expired.
const (
	SourceWorker FinishSource = iota + 1 // the lease must be live
	SourceReaper                         // the lease must have expired
)

// TaskResult is the transition the core chose for a finished attempt.
type TaskResult int

// The transitions a Finish can apply.
const (
	ResultSucceeded TaskResult = iota + 1
	ResultFailed
	ResultRetry
	// ResultReleased returns a task that was not run to its backlog: its assignment
	// never reached a worker, or the worker handed it back. The dispatch is undone as
	// far as the task is concerned: no attempt is recorded and the attempt number is
	// reused. The politeness slot it took is not given back, so a release never lets a
	// queue exceed its limits. RetryIn delays the next dispatch.
	ResultReleased
)

// Finish is the transition the core decided for an attempt.
type Finish struct {
	Lease   LeaseRef
	Source  FinishSource
	Result  TaskResult
	RetryIn time.Duration // Retry and Released: not before this delay
	Block   time.Duration // > 0: block the task's queue for all runs
	Record  AttemptRecord
}

// AttemptRecord is what is kept about one finished attempt.
type AttemptRecord struct {
	Kind         int32 // the worker's Outcome kind, as reported
	Code         string
	Message      string
	HTTPStatus   uint32
	FinalURL     string
	Latency      time.Duration
	Bytes        uint64
	ResponseRef  string
	ContentHash  string
	ItemsEmitted uint32
	PolicySet    string
}

// FinishStatus says whether a Finish or ExtendLease was applied or fenced.
type FinishStatus int

// FinishAccepted means the operation was applied; FinishStale and FinishUnknown mean the lease was fenced or is not known, and nothing changed.
const (
	FinishAccepted FinishStatus = iota + 1
	FinishStale
	FinishUnknown
)

// TaskState is the lifecycle state of a task.
type TaskState int

// TaskPending and TaskDelayed are not yet dispatched and TaskInFlight is leased; the other states are final.
const (
	TaskPending TaskState = iota + 1
	TaskDelayed
	TaskInFlight
	TaskSucceeded
	TaskFailed
	TaskExpired
	TaskCancelled
)

// Attempt is one finished attempt to run a task.
type Attempt struct {
	Attempt      uint32
	Worker       WorkerInfo
	DispatchedAt time.Time
	FinishedAt   time.Time
	Record       AttemptRecord
}

// TaskStatus is a task with its state and attempt history.
type TaskStatus struct {
	ID             TaskID
	Run            RunID
	Queue          QueueKey
	Pool           PoolName
	Fingerprint    Fingerprint
	Depth          uint32
	Parent         TaskID
	Spec           []byte
	State          TaskState
	NextEligibleAt time.Time
	Attempts       []Attempt
}

// TaskFilter selects tasks of a run; empty fields match everything.
type TaskFilter struct {
	Run    RunID
	Queue  QueueKey
	States []TaskState
	Code   string
}

// ---- queues

// Queue is the state of a politeness queue across all runs.
type Queue struct {
	Key            QueueKey
	InFlight       int
	NextEligibleAt time.Time
	AdmissibleNow  int // sched.Unlimited when nothing bounds it
	BlockedUntil   time.Time
	BlockReason    string
	Backlogs       []Backlog
}

// Backlog is the share of a queue's work that belongs to one run and pool.
type Backlog struct {
	Run      RunID
	Pool     PoolName
	Pending  uint64
	Delayed  uint64
	InFlight int
	Weight   float64
}

// QueueFilter selects queues; empty fields match everything.
type QueueFilter struct {
	Run         RunID
	KeyPrefix   string
	OnlyBlocked bool
}

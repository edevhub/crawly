package frontier

import (
	"context"
	"errors"
	"time"

	"github.com/edevhub/crawly/internal/store"
)

// timedStore times every call of a store and counts the ones that fail.
type timedStore struct {
	store.Store
	m *metrics
}

// answers are the errors that are an answer to the caller, not a failure of the store.
var answers = []error{store.ErrNotFound, store.ErrConflict, store.ErrRunClosed, store.ErrLeaseStale, store.ErrLeaseUnknown,
	context.Canceled}

func (t timedStore) observe(method string, start time.Time, err error) {
	t.m.storeTime.WithLabelValues(method).Observe(time.Since(start).Seconds())
	if err == nil {
		return
	}
	for _, a := range answers {
		if errors.Is(err, a) {
			return
		}
	}
	kind := "other"
	if errors.Is(err, store.ErrUnavailable) {
		kind = "unavailable"
	}
	t.m.storeErrors.WithLabelValues(method, kind).Inc()
}

// timed runs one call. Generic over the result so that each method below is one line.
func timed[T any](t timedStore, method string, call func() (T, error)) (T, error) {
	start := time.Now()
	v, err := call()
	t.observe(method, start, err)
	return v, err
}

func timedErr(t timedStore, method string, call func() error) error {
	_, err := timed(t, method, func() (struct{}, error) { return struct{}{}, call() })
	return err
}

func (t timedStore) CreateRun(ctx context.Context, spec store.RunSpec) (store.Run, error) {
	return timed(t, "CreateRun", func() (store.Run, error) { return t.Store.CreateRun(ctx, spec) })
}

func (t timedStore) SealRun(ctx context.Context, id store.RunID) (store.Run, error) {
	return timed(t, "SealRun", func() (store.Run, error) { return t.Store.SealRun(ctx, id) })
}

func (t timedStore) PauseRun(ctx context.Context, id store.RunID) (store.Run, error) {
	return timed(t, "PauseRun", func() (store.Run, error) { return t.Store.PauseRun(ctx, id) })
}

func (t timedStore) ResumeRun(ctx context.Context, id store.RunID) (store.Run, error) {
	return timed(t, "ResumeRun", func() (store.Run, error) { return t.Store.ResumeRun(ctx, id) })
}

func (t timedStore) CancelRun(ctx context.Context, id store.RunID) (store.Run, error) {
	return timed(t, "CancelRun", func() (store.Run, error) { return t.Store.CancelRun(ctx, id) })
}

func (t timedStore) GetRun(ctx context.Context, id store.RunID) (store.Run, error) {
	return timed(t, "GetRun", func() (store.Run, error) { return t.Store.GetRun(ctx, id) })
}

func (t timedStore) ListRuns(ctx context.Context, f store.RunFilter, page store.Page) ([]store.Run, string, error) {
	start := time.Now()
	runs, next, err := t.Store.ListRuns(ctx, f, page)
	t.observe("ListRuns", start, err)
	return runs, next, err
}

func (t timedStore) RunEvents(ctx context.Context, after store.EventCursor, limit int) ([]store.RunEvent, error) {
	return timed(t, "RunEvents", func() ([]store.RunEvent, error) { return t.Store.RunEvents(ctx, after, limit) })
}

func (t timedStore) LastEventCursor(ctx context.Context) (store.EventCursor, error) {
	return timed(t, "LastEventCursor", func() (store.EventCursor, error) { return t.Store.LastEventCursor(ctx) })
}

func (t timedStore) AddTasks(ctx context.Context, req store.AddRequest) ([]store.AddResult, error) {
	return timed(t, "AddTasks", func() ([]store.AddResult, error) { return t.Store.AddTasks(ctx, req) })
}

func (t timedStore) Dispatch(ctx context.Context, req store.DispatchRequest) (store.DispatchResult, error) {
	return timed(t, "Dispatch", func() (store.DispatchResult, error) { return t.Store.Dispatch(ctx, req) })
}

func (t timedStore) Lease(ctx context.Context, lease store.LeaseRef) (store.LeaseInfo, error) {
	return timed(t, "Lease", func() (store.LeaseInfo, error) { return t.Store.Lease(ctx, lease) })
}

func (t timedStore) ExtendLease(ctx context.Context, lease store.LeaseRef, by time.Duration) (store.Lease, store.FinishStatus, error) {
	start := time.Now()
	l, st, err := t.Store.ExtendLease(ctx, lease, by)
	t.observe("ExtendLease", start, err)
	return l, st, err
}

func (t timedStore) Finish(ctx context.Context, f store.Finish) (store.FinishStatus, error) {
	return timed(t, "Finish", func() (store.FinishStatus, error) { return t.Store.Finish(ctx, f) })
}

func (t timedStore) ExpiredLeases(ctx context.Context, limit int) ([]store.LeaseInfo, error) {
	return timed(t, "ExpiredLeases", func() ([]store.LeaseInfo, error) { return t.Store.ExpiredLeases(ctx, limit) })
}

func (t timedStore) GetTask(ctx context.Context, run store.RunID, id store.TaskID, fp *store.Fingerprint) (store.TaskStatus, error) {
	return timed(t, "GetTask", func() (store.TaskStatus, error) { return t.Store.GetTask(ctx, run, id, fp) })
}

func (t timedStore) ListTasks(ctx context.Context, f store.TaskFilter, page store.Page) ([]store.TaskStatus, string, error) {
	start := time.Now()
	tasks, next, err := t.Store.ListTasks(ctx, f, page)
	t.observe("ListTasks", start, err)
	return tasks, next, err
}

func (t timedStore) GetQueue(ctx context.Context, key store.QueueKey) (store.Queue, error) {
	return timed(t, "GetQueue", func() (store.Queue, error) { return t.Store.GetQueue(ctx, key) })
}

func (t timedStore) ListQueues(ctx context.Context, f store.QueueFilter, page store.Page) ([]store.Queue, string, error) {
	start := time.Now()
	qs, next, err := t.Store.ListQueues(ctx, f, page)
	t.observe("ListQueues", start, err)
	return qs, next, err
}

func (t timedStore) BlockQueue(ctx context.Context, key store.QueueKey, until time.Time, reason string) (store.Queue, error) {
	return timed(t, "BlockQueue", func() (store.Queue, error) { return t.Store.BlockQueue(ctx, key, until, reason) })
}

func (t timedStore) PoolStats(ctx context.Context, limit int) ([]store.PoolStats, error) {
	return timed(t, "PoolStats", func() ([]store.PoolStats, error) { return t.Store.PoolStats(ctx, limit) })
}

func (t timedStore) Start(ctx context.Context, r store.Replica) error {
	return timedErr(t, "Start", func() error { return t.Store.Start(ctx, r) })
}

func (t timedStore) Maintain(ctx context.Context) error {
	return timedErr(t, "Maintain", func() error { return t.Store.Maintain(ctx) })
}

func (t timedStore) Now(ctx context.Context) (time.Time, error) {
	return timed(t, "Now", func() (time.Time, error) { return t.Store.Now(ctx) })
}

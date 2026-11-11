package sdk

import (
	"context"
	"time"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/types/known/durationpb"

	"github.com/edevhub/crawly/sdk/run"
)

// WaitOption configures Wait.
type WaitOption func(*waiter)

// OnEvent makes Wait call fn for every event of the run: lifecycle changes and, with
// StatsEvery, statistics. After a reconnect the first event describes the run as it
// is then, so fn may see the same state twice.
func OnEvent(fn func(*run.Event)) WaitOption { return func(w *waiter) { w.onEvent = fn } }

// StatsEvery makes the Frontier send the run's statistics at this interval while Wait
// waits. They are delivered to OnEvent.
func StatsEvery(d time.Duration) WaitOption { return func(w *waiter) { w.stats = d } }

type waiter struct {
	onEvent func(*run.Event)
	stats   time.Duration
}

// Backoff between attempts to reach the Frontier again.
const (
	waitFirstBackoff   = 200 * time.Millisecond
	waitLongestBackoff = 5 * time.Second
)

// Wait blocks until the run has ended and returns it in its final state: completed,
// cancelled, expired or aborted (Run.AbortReason says by which rule).
//
// It watches the run and survives the Frontier going away: when the stream breaks, as
// it does on every restart of a Frontier, Wait reconnects with backoff until ctx is
// done. A run that does not exist, or no longer exists, ends the wait with a NotFound
// error, as does any other answer that waiting cannot change.
func (c *Client) Wait(ctx context.Context, runID string, opts ...WaitOption) (*run.Run, error) {
	var w waiter
	for _, o := range opts {
		o(&w)
	}
	backoff := waitFirstBackoff
	for {
		progressed, final, err := c.watchOnce(ctx, runID, &w)
		switch {
		case final != nil:
			return final, nil
		case ctx.Err() != nil:
			return nil, ctx.Err()
		case !transient(err):
			return nil, err
		}
		if progressed {
			backoff = waitFirstBackoff
		}
		select {
		case <-ctx.Done():
			return nil, ctx.Err()
		case <-time.After(backoff):
		}
		backoff = min(2*backoff, waitLongestBackoff)
	}
}

// watchOnce follows the run on one stream. It returns the run once it has ended, or
// the error that broke the stream, and whether the Frontier answered at all.
func (c *Client) watchOnce(ctx context.Context, runID string, w *waiter) (progressed bool, final *run.Run, err error) {
	// The watch of a run that does not exist waits for it to be created, so that a
	// watcher can come first. Wait is for a run its caller created: ask.
	got, err := c.Runs.GetRun(ctx, &run.GetRunRequest{RunId: runID})
	if err != nil {
		return false, nil, err
	}
	if ended(got.GetRun()) {
		return true, got.GetRun(), nil
	}
	// Cancelling the stream's context when this attempt returns releases the stream.
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()
	req := &run.WatchRunsRequest{RunId: runID}
	if w.stats > 0 {
		req.StatsInterval = durationpb.New(w.stats)
	}
	stream, err := c.Runs.WatchRuns(ctx, req)
	if err != nil {
		return true, nil, err
	}
	for {
		res, err := stream.Recv()
		if err != nil {
			return true, nil, err // also a clean end of the stream: the run has not ended
		}
		ev := res.GetEvent()
		if w.onEvent != nil {
			w.onEvent(ev)
		}
		if ended(ev.GetRun()) {
			return true, ev.GetRun(), nil
		}
	}
}

func ended(r *run.Run) bool {
	switch r.GetState() {
	case run.StateCompleted, run.StateCancelled, run.StateExpired, run.StateAborted:
		return true
	default:
		return false
	}
}

// transient reports whether trying again can change the answer.
func transient(err error) bool {
	switch status.Code(err) {
	case codes.NotFound, codes.InvalidArgument, codes.PermissionDenied, codes.Unauthenticated,
		codes.Unimplemented, codes.FailedPrecondition:
		return false
	default:
		// Unavailable while a Frontier restarts or drains, and a stream that just ended.
		return true
	}
}

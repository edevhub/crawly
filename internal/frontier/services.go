package frontier

import (
	"context"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"slices"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/durationpb"
	"google.golang.org/protobuf/types/known/timestamppb"

	policyv1 "github.com/edevhub/crawly/internal/gen/frontier/policy/v1"
	queuev1 "github.com/edevhub/crawly/internal/gen/frontier/queue/v1"
	runv1 "github.com/edevhub/crawly/internal/gen/frontier/run/v1"
	taskv1 "github.com/edevhub/crawly/internal/gen/frontier/task/v1"
	"github.com/edevhub/crawly/internal/policy"
	"github.com/edevhub/crawly/internal/safeconv"
	"github.com/edevhub/crawly/internal/store"
)

// ---- runs

type runService struct {
	runv1.UnimplementedRunServiceServer
	s *Server
}

func (r runService) CreateRun(ctx context.Context, req *runv1.CreateRunRequest) (*runv1.CreateRunResponse, error) {
	if req.GetRunId() == "" {
		return nil, status.Error(codes.InvalidArgument, "run_id is required")
	}
	if err := policy.ValidateRun(req.GetPolicy()); err != nil {
		return nil, status.Error(codes.InvalidArgument, "policy: "+err.Error())
	}
	if err := validateAbortRules(req.GetLimits()); err != nil {
		return nil, status.Error(codes.InvalidArgument, "limits: "+err.Error())
	}
	spec := store.RunSpec{ID: store.RunID(req.GetRunId()), Labels: req.GetLabels(), Policy: req.GetPolicy(),
		Paused: req.GetStartPaused(), Hash: specHash(req), KeyRule: store.KeyHost,
		Limits: store.RunLimits{MaxTasks: req.GetLimits().GetMaxTasks(), MaxDepth: req.GetLimits().GetMaxDepth(),
			AbortOnCodes: req.GetLimits().GetAbortOnCodes(), MaxFailed: req.GetLimits().GetMaxFailed(),
			MaxConsecutiveFailures: req.GetLimits().GetMaxConsecutiveFailures()}}
	switch req.GetKeyRule().GetKind() {
	case runv1.KeyRule_KIND_REGISTERED_DOMAIN:
		spec.KeyRule = store.KeyRegisteredDomain
	case runv1.KeyRule_KIND_EXPLICIT:
		spec.KeyRule = store.KeyExplicit
	default:
		// Unspecified and host: the host rule is already set.
	}
	if d := req.GetLimits().GetDeadlineTime(); d != nil {
		spec.Limits.Deadline = d.AsTime()
	}
	run, err := r.s.store.CreateRun(ctx, spec)
	if err != nil {
		return nil, grpcErr(err)
	}
	return &runv1.CreateRunResponse{Run: runProto(run)}, nil
}

// maxAbortCodes bounds abort_on_codes.
const maxAbortCodes = 64

// reservedCodes are codes no recorded attempt carries, so a rule on them would never
// trip: "revoked" marks the attempt of a task whose run ended, and "shutdown" a task a
// worker handed back, which records no attempt.
var reservedCodes = []string{"revoked", CodeShutdown}

// validateAbortRules rejects abort_on_codes that cannot work as written.
func validateAbortRules(l *runv1.RunLimits) error {
	list := l.GetAbortOnCodes()
	if len(list) > maxAbortCodes {
		return fmt.Errorf("abort_on_codes: %d codes, at most %d", len(list), maxAbortCodes)
	}
	seen := make(map[string]bool, len(list))
	for i, c := range list {
		switch {
		case c == "":
			return fmt.Errorf("abort_on_codes[%d]: empty code", i)
		case seen[c]:
			return fmt.Errorf("abort_on_codes[%d]: duplicate code %q", i, c)
		case slices.Contains(reservedCodes, c):
			return fmt.Errorf("abort_on_codes[%d]: %q is reserved: no attempt is recorded with it, so the rule would never trip", i, c)
		}
		seen[c] = true
	}
	return nil
}

func (r runService) AddTasks(stream grpc.BidiStreamingServer[runv1.AddTasksRequest, runv1.AddTasksResponse]) error {
	ctx := stream.Context()
	for {
		req, err := stream.Recv()
		if errors.Is(err, io.EOF) {
			return nil
		}
		if err != nil {
			return err
		}
		spec, err := r.s.runSpec(ctx, store.RunID(req.GetRunId()))
		if err != nil {
			return grpcErr(err)
		}
		res, err := r.s.store.AddTasks(ctx, store.AddRequest{Run: spec.ID, Tasks: r.s.newTasks(spec, req.GetTasks(), ""),
			MaxLabels: r.s.cfg.MaxLabelsPerRun})
		if err != nil {
			return grpcErr(err)
		}
		r.s.m.tasksAdded("seed", res)
		if err := stream.Send(&runv1.AddTasksResponse{BatchId: req.GetBatchId(), Results: addResults(res)}); err != nil {
			return err
		}
	}
}

func (r runService) SealRun(ctx context.Context, req *runv1.SealRunRequest) (*runv1.SealRunResponse, error) {
	run, err := r.s.store.SealRun(ctx, store.RunID(req.GetRunId()))
	return &runv1.SealRunResponse{Run: runProto(run)}, grpcErr(err)
}

func (r runService) PauseRun(ctx context.Context, req *runv1.PauseRunRequest) (*runv1.PauseRunResponse, error) {
	run, err := r.s.store.PauseRun(ctx, store.RunID(req.GetRunId()))
	return &runv1.PauseRunResponse{Run: runProto(run)}, grpcErr(err)
}

func (r runService) ResumeRun(ctx context.Context, req *runv1.ResumeRunRequest) (*runv1.ResumeRunResponse, error) {
	run, err := r.s.store.ResumeRun(ctx, store.RunID(req.GetRunId()))
	return &runv1.ResumeRunResponse{Run: runProto(run)}, grpcErr(err)
}

func (r runService) CancelRun(ctx context.Context, req *runv1.CancelRunRequest) (*runv1.CancelRunResponse, error) {
	run, err := r.s.store.CancelRun(ctx, store.RunID(req.GetRunId()))
	return &runv1.CancelRunResponse{Run: runProto(run)}, grpcErr(err)
}

func (r runService) GetRun(ctx context.Context, req *runv1.GetRunRequest) (*runv1.GetRunResponse, error) {
	run, err := r.s.store.GetRun(ctx, store.RunID(req.GetRunId()))
	return &runv1.GetRunResponse{Run: runProto(run)}, grpcErr(err)
}

func (r runService) ListRuns(ctx context.Context, req *runv1.ListRunsRequest) (*runv1.ListRunsResponse, error) {
	f := store.RunFilter{Labels: req.GetLabels()}
	for _, st := range req.GetStates() {
		f.States = append(f.States, store.RunState(st))
	}
	runs, next, err := r.s.store.ListRuns(ctx, f, store.Page{Size: int(req.GetPageSize()), Token: req.GetPageToken()})
	if err != nil {
		return nil, grpcErr(err)
	}
	out := &runv1.ListRunsResponse{NextPageToken: next}
	for i := range runs {
		out.Runs = append(out.Runs, runProto(runs[i]))
	}
	return out, nil
}

// WatchRuns streams lifecycle events from now on, and stats snapshots of the matching
// runs that have not ended at stats_interval.
func (r runService) WatchRuns(req *runv1.WatchRunsRequest, stream grpc.ServerStreamingServer[runv1.WatchRunsResponse]) error {
	ctx := stream.Context()
	matches := func(run store.Run) bool {
		if req.GetRunId() != "" && string(run.Spec.ID) != req.GetRunId() {
			return false
		}
		for k, v := range req.GetLabels() {
			if run.Spec.Labels[k] != v {
				return false
			}
		}
		return true
	}
	cursor, err := r.s.store.LastEventCursor(ctx)
	if err != nil {
		return grpcErr(err)
	}
	// The current state first, read after the cursor: a watcher that subscribes late
	// still learns that a run has already ended, and misses nothing in between.
	now, err := r.s.store.Now(ctx)
	if err != nil {
		return grpcErr(err)
	}
	if id := req.GetRunId(); id != "" {
		run, err := r.s.store.GetRun(ctx, store.RunID(id))
		switch {
		case errors.Is(err, store.ErrNotFound):
			// Not created yet: its events follow.
		case err != nil:
			return grpcErr(err)
		default:
			if err := stream.Send(&runv1.WatchRunsResponse{Event: &runv1.RunEvent{
				Type: snapshotType(run.State), Run: runProto(run), EventTime: timestamppb.New(now)}}); err != nil {
				return err
			}
		}
	}
	var stats <-chan time.Time
	if d := req.GetStatsInterval().AsDuration(); d > 0 {
		t := time.NewTicker(d)
		defer t.Stop()
		stats = t.C
	}
	for {
		wait := r.s.events.Wait()
		evs, err := r.s.store.RunEvents(ctx, cursor, 500)
		if err != nil {
			return grpcErr(err)
		}
		for i := range evs {
			ev := &evs[i]
			cursor = ev.Cursor
			if !matches(ev.Run) {
				continue
			}
			if err := stream.Send(&runv1.WatchRunsResponse{Event: &runv1.RunEvent{
				Type: runv1.RunEvent_Type(safeconv.Int32(int(ev.Type))), Run: runProto(ev.Run), EventTime: timestamppb.New(ev.At)}}); err != nil {
				return err
			}
		}
		if len(evs) > 0 {
			continue
		}
		select {
		case <-ctx.Done():
			return nil
		case <-r.s.draining:
			return status.Error(codes.Unavailable, "the frontier is shutting down")
		case <-wait:
		case <-time.After(r.s.cfg.PollInterval):
		case now := <-stats:
			runs, _, err := r.s.store.ListRuns(ctx, store.RunFilter{States: []store.RunState{store.RunOpen, store.RunSealed},
				Labels: req.GetLabels()}, store.Page{Size: 1000})
			if err != nil {
				return grpcErr(err)
			}
			for i := range runs {
				if !matches(runs[i]) {
					continue
				}
				if err := stream.Send(&runv1.WatchRunsResponse{Event: &runv1.RunEvent{
					Type: runv1.RunEvent_TYPE_STATS, Run: runProto(runs[i]), EventTime: timestamppb.New(now)}}); err != nil {
					return err
				}
			}
		}
	}
}

func (r runService) ListTasks(ctx context.Context, req *runv1.ListTasksRequest) (*runv1.ListTasksResponse, error) {
	f := store.TaskFilter{Run: store.RunID(req.GetRunId()), Queue: store.QueueKey(req.GetQueueKey()), Code: req.GetCode(), Label: req.GetLabel()}
	for _, st := range req.GetStates() {
		f.States = append(f.States, store.TaskState(st))
	}
	tasks, next, err := r.s.store.ListTasks(ctx, f, store.Page{Size: int(req.GetPageSize()), Token: req.GetPageToken()})
	if err != nil {
		return nil, grpcErr(err)
	}
	out := &runv1.ListTasksResponse{NextPageToken: next}
	for i := range tasks {
		out.Tasks = append(out.Tasks, taskStatusProto(tasks[i]))
	}
	return out, nil
}

func (r runService) GetTask(ctx context.Context, req *runv1.GetTaskRequest) (*runv1.GetTaskResponse, error) {
	var fp *store.Fingerprint
	if req.GetTaskId() == "" && req.GetUrl() != "" {
		spec, err := r.s.runSpec(ctx, store.RunID(req.GetRunId()))
		if err != nil {
			return nil, grpcErr(err)
		}
		_, _, f, err := identify(spec.KeyRule, &taskv1.TaskSpec{Request: &taskv1.Request{Url: req.GetUrl()}})
		if err != nil {
			return nil, status.Error(codes.InvalidArgument, err.Error())
		}
		fp = &f
	}
	t, err := r.s.store.GetTask(ctx, store.RunID(req.GetRunId()), store.TaskID(req.GetTaskId()), fp)
	if err != nil {
		return nil, grpcErr(err)
	}
	return &runv1.GetTaskResponse{Task: taskStatusProto(t)}, nil
}

// ---- policies

type policyService struct {
	policyv1.UnimplementedPolicyServiceServer
	s *Server
}

func (p policyService) ListPolicies(context.Context, *policyv1.ListPoliciesRequest) (*policyv1.ListPoliciesResponse, error) {
	return &policyv1.ListPoliciesResponse{Policies: p.s.pol.Policies(), PolicySet: p.s.pol.Hash()}, nil
}

func (p policyService) GetPolicy(_ context.Context, req *policyv1.GetPolicyRequest) (*policyv1.GetPolicyResponse, error) {
	pol, ok := p.s.pol.Get(req.GetName())
	if !ok {
		return nil, status.Errorf(codes.NotFound, "no policy %q", req.GetName())
	}
	return &policyv1.GetPolicyResponse{Policy: pol}, nil
}

func (p policyService) ExplainPolicy(ctx context.Context, req *policyv1.ExplainPolicyRequest) (*policyv1.ExplainPolicyResponse, error) {
	var rp *policyv1.RunPolicy
	queue := req.GetQueueKey()
	if req.GetRunId() != "" {
		spec, err := p.s.runSpec(ctx, store.RunID(req.GetRunId()))
		if err != nil {
			return nil, grpcErr(err)
		}
		rp = spec.Policy
		if req.GetTaskId() != "" {
			t, err := p.s.store.GetTask(ctx, spec.ID, store.TaskID(req.GetTaskId()), nil)
			if err != nil {
				return nil, grpcErr(err)
			}
			queue = string(t.Queue)
		}
	}
	if queue == "" {
		return nil, status.Error(codes.InvalidArgument, "queue_key or task_id is required")
	}
	res := p.s.pol.Resolve(rp, queue)
	return &policyv1.ExplainPolicyResponse{Limits: res.Limits, Behaviour: res.Behaviour, Provenance: res.Provenance}, nil
}

// ---- queues

type queueService struct {
	queuev1.UnimplementedQueueServiceServer
	s *Server
}

func (q queueService) ListQueues(ctx context.Context, req *queuev1.ListQueuesRequest) (*queuev1.ListQueuesResponse, error) {
	qs, next, err := q.s.store.ListQueues(ctx, store.QueueFilter{Run: store.RunID(req.GetRunId()), KeyPrefix: req.GetKeyPrefix(),
		OnlyBlocked: req.GetOnlyBlocked()}, store.Page{Size: int(req.GetPageSize()), Token: req.GetPageToken()})
	if err != nil {
		return nil, grpcErr(err)
	}
	out := &queuev1.ListQueuesResponse{NextPageToken: next}
	for i := range qs {
		out.Queues = append(out.Queues, q.queueProto(qs[i]))
	}
	return out, nil
}

func (q queueService) GetQueue(ctx context.Context, req *queuev1.GetQueueRequest) (*queuev1.GetQueueResponse, error) {
	v, err := q.s.store.GetQueue(ctx, store.QueueKey(req.GetKey()))
	if err != nil {
		return nil, grpcErr(err)
	}
	return &queuev1.GetQueueResponse{Queue: q.queueProto(v)}, nil
}

func (q queueService) BlockQueue(ctx context.Context, req *queuev1.BlockQueueRequest) (*queuev1.BlockQueueResponse, error) {
	if req.GetKey() == "" {
		return nil, status.Error(codes.InvalidArgument, "key is required")
	}
	var until time.Time
	if req.GetUnblockTime() != nil {
		until = req.GetUnblockTime().AsTime()
	}
	v, err := q.s.store.BlockQueue(ctx, store.QueueKey(req.GetKey()), until, req.GetReason())
	if err != nil {
		return nil, grpcErr(err)
	}
	return &queuev1.BlockQueueResponse{Queue: q.queueProto(v)}, nil
}

func (q queueService) queueProto(v store.Queue) *queuev1.Queue {
	out := &queuev1.Queue{Key: string(v.Key), EffectiveLimits: q.s.pol.Resolve(nil, string(v.Key)).Queue,
		Limiter: &queuev1.LimiterState{InFlight: safeconv.Uint32(v.InFlight), NextEligibleTime: ts(v.NextEligibleAt),
			AdmissibleNow: safeconv.Uint32(v.AdmissibleNow), UnblockTime: ts(v.BlockedUntil), BlockReason: v.BlockReason,
			StrainSteps: v.Strain.Steps, LastStrainTime: ts(v.Strain.Last)}}
	if v.Strain.Interval > 0 {
		out.Limiter.CurrentInterval = durationpb.New(v.Strain.Interval)
	}
	for _, b := range v.Backlogs {
		out.Backlogs = append(out.Backlogs, &queuev1.RunBacklog{RunId: string(b.Run), Pool: string(b.Pool),
			Pending: b.Pending, Delayed: b.Delayed, InFlight: safeconv.Uint32(b.InFlight), Weight: uint32(b.Weight)})
	}
	return out
}

// ---- conversions

// ts converts a time, keeping the zero time unset.
func ts(t time.Time) *timestamppb.Timestamp {
	if t.IsZero() {
		return nil
	}
	return timestamppb.New(t)
}

func runProto(r store.Run) *runv1.Run {
	if r.Spec.ID == "" {
		return nil
	}
	rule := runv1.KeyRule_KIND_HOST
	switch r.Spec.KeyRule {
	case store.KeyRegisteredDomain:
		rule = runv1.KeyRule_KIND_REGISTERED_DOMAIN
	case store.KeyExplicit:
		rule = runv1.KeyRule_KIND_EXPLICIT
	default:
		// store.KeyHost: the default rule above.
	}
	st := r.Stats
	return &runv1.Run{
		RunId: string(r.Spec.ID), Labels: r.Spec.Labels, State: runv1.Run_State(safeconv.Int32(int(r.State))), Paused: r.Paused,
		Policy: r.Spec.Policy,
		Limits: &runv1.RunLimits{DeadlineTime: ts(r.Spec.Limits.Deadline), MaxTasks: r.Spec.Limits.MaxTasks, MaxDepth: r.Spec.Limits.MaxDepth,
			AbortOnCodes: r.Spec.Limits.AbortOnCodes, MaxFailed: r.Spec.Limits.MaxFailed,
			MaxConsecutiveFailures: r.Spec.Limits.MaxConsecutiveFailures},
		KeyRule: &runv1.KeyRule{Kind: rule},
		Stats: &runv1.RunStats{Pending: st.Pending, Delayed: st.Delayed, InFlight: st.InFlight, Succeeded: st.Succeeded,
			Failed: st.Failed, Expired: st.Expired, Cancelled: st.Cancelled, Attempts: st.Attempts, QueueBlocks: st.QueueBlocks,
			Discovered: st.Discovered, Duplicates: st.Duplicates, OverBudget: st.OverBudget, ByCode: st.ByCode,
			BlockedQueues: blockedQueuesProto(st.BlockedQueues), ByLabel: labelStatsProto(st.ByLabel), Invalid: st.Invalid},
		CreateTime: ts(r.CreatedAt), SealTime: ts(r.SealedAt), FinishTime: ts(r.FinishedAt),
		AbortReason: abortProto(r.Abort),
	}
}

func labelStatsProto(m map[string]store.LabelStats) map[string]*runv1.LabelStats {
	if len(m) == 0 {
		return nil
	}
	out := make(map[string]*runv1.LabelStats, len(m))
	for k, l := range m {
		out[k] = &runv1.LabelStats{Succeeded: l.Succeeded, Failed: l.Failed, Expired: l.Expired, Cancelled: l.Cancelled,
			FailedByCode: l.FailedByCode}
	}
	return out
}

func blockedQueuesProto(bq []store.BlockedQueue) []*runv1.BlockedQueue {
	out := make([]*runv1.BlockedQueue, 0, len(bq))
	for _, b := range bq {
		out = append(out, &runv1.BlockedQueue{QueueKey: string(b.Queue), UnblockTime: ts(b.Until), Reason: b.Reason, Waiting: b.Waiting})
	}
	return out
}

func abortProto(a *store.AbortReason) *runv1.AbortReason {
	if a == nil {
		return nil
	}
	rule := runv1.AbortReason_RULE_UNSPECIFIED
	switch a.Rule {
	case store.AbortOnCode:
		rule = runv1.AbortReason_RULE_CODE
	case store.AbortMaxFailed:
		rule = runv1.AbortReason_RULE_MAX_FAILED
	case store.AbortConsecutiveFailures:
		rule = runv1.AbortReason_RULE_MAX_CONSECUTIVE_FAILURES
	}
	return &runv1.AbortReason{Rule: rule, Code: a.Code, TaskId: string(a.Task)}
}

func addResults(res []store.AddResult) []*taskv1.AddResult {
	out := make([]*taskv1.AddResult, len(res))
	for i, r := range res {
		out[i] = &taskv1.AddResult{TaskId: string(r.TaskID)}
		switch r.Status {
		case store.AddAdded:
			out[i].Status = taskv1.AddResult_STATUS_ADDED
		case store.AddDuplicate:
			out[i].Status = taskv1.AddResult_STATUS_DUPLICATE
		case store.AddOverBudget:
			out[i].Status = taskv1.AddResult_STATUS_OVER_BUDGET
		case store.AddRunClosed:
			out[i].Status = taskv1.AddResult_STATUS_RUN_CLOSED
		default:
			out[i].Status = taskv1.AddResult_STATUS_INVALID
		}
	}
	return out
}

func taskFromSpec(id store.TaskID, run store.RunID, fp store.Fingerprint, depth uint32, parent store.TaskID, spec *taskv1.TaskSpec) *taskv1.Task {
	return &taskv1.Task{TaskId: string(id), RunId: string(run), QueueKey: spec.GetQueueKey(), Fingerprint: hex.EncodeToString(fp[:]),
		Request: spec.GetRequest(), Handler: spec.GetHandler(), Depth: depth, ParentTaskId: string(parent),
		Context: spec.GetContext(), Pool: spec.GetPool()}
}

func taskStatusProto(t store.TaskStatus) *runv1.TaskStatus {
	spec := &taskv1.TaskSpec{}
	_ = proto.Unmarshal(t.Spec, spec)
	task := taskFromSpec(t.ID, t.Run, t.Fingerprint, t.Depth, t.Parent, spec)
	task.Label = t.Label // the stored column, which is what the statistics count
	out := &runv1.TaskStatus{Task: task,
		State: runv1.TaskStatus_State(safeconv.Int32(int(t.State))), NextEligibleTime: ts(t.NextEligibleAt)}
	for i := range t.Attempts {
		a := &t.Attempts[i]
		rec := a.Record
		out.Attempts = append(out.Attempts, &runv1.Attempt{Attempt: a.Attempt, WorkerId: a.Worker.ID, WorkerVersion: a.Worker.Version,
			DispatchTime: ts(a.DispatchedAt), FinishTime: ts(a.FinishedAt), PolicySet: rec.PolicySet,
			Outcome: &taskv1.Outcome{Kind: taskv1.Outcome_Kind(rec.Kind), Code: rec.Code, Message: rec.Message},
			Fetch: &taskv1.FetchInfo{HttpStatus: rec.HTTPStatus, FinalUrl: rec.FinalURL, Latency: durationpb.New(rec.Latency),
				Bytes: rec.Bytes, ResponseRef: rec.ResponseRef, ContentHash: rec.ContentHash, ItemsEmitted: rec.ItemsEmitted}})
	}
	return out
}

// snapshotType is the event type that describes a run's current state to a watcher
// that has just subscribed: the terminal event of a run that has ended, STATS otherwise.
func snapshotType(st store.RunState) runv1.RunEvent_Type {
	switch st {
	case store.RunCompleted:
		return runv1.RunEvent_TYPE_COMPLETED
	case store.RunCancelled:
		return runv1.RunEvent_TYPE_CANCELLED
	case store.RunExpired:
		return runv1.RunEvent_TYPE_EXPIRED
	case store.RunAborted:
		return runv1.RunEvent_TYPE_ABORTED
	default:
		return runv1.RunEvent_TYPE_STATS
	}
}

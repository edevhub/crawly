// Package run exports the run API of the Frontier: the service client a scheduler uses
// to create, seed, seal, pause and cancel runs and to observe them. The types are
// aliases of the generated types, so there is one definition of each.
package run

import (
	"google.golang.org/grpc"

	runv1 "github.com/edevhub/crawly/internal/gen/frontier/run/v1"
)

// Client is the run service client.
type Client = runv1.RunServiceClient

// NewClient returns a run service client on a connection. sdk.NewClient builds one for
// every service at once.
func NewClient(conn grpc.ClientConnInterface) Client { return runv1.NewRunServiceClient(conn) }

type (
	// Run is a run: its state, limits, policy and statistics.
	Run = runv1.Run
	// Stats counts the tasks and attempts of a run.
	Stats = runv1.RunStats
	// Limits caps what a run may do (a deadline, a task budget, a crawl depth) and sets the
	// rules that abort it.
	Limits = runv1.RunLimits
	// AbortReason is the rule that aborted a run and the task that tripped it.
	AbortReason = runv1.AbortReason
	// BlockedQueue is a blocked queue that a run's tasks wait on, as Stats lists it.
	BlockedQueue = runv1.BlockedQueue
	// Event is a run lifecycle event or a statistics update, as WatchRuns delivers it.
	Event = runv1.RunEvent
	// KeyRule says how a task's queue key is derived.
	KeyRule = runv1.KeyRule
	// TaskStatus is a task with its state and attempt history.
	TaskStatus = runv1.TaskStatus
	// Attempt is one finished try of a task, with the worker, outcome and fetch details.
	Attempt = runv1.Attempt

	// State is the lifecycle state of a run.
	State = runv1.Run_State
	// EventType is the kind of change an Event reports.
	EventType = runv1.RunEvent_Type
	// TaskState is the state of a task.
	TaskState = runv1.TaskStatus_State
	// KeyRuleKind is the way a KeyRule derives the queue key.
	KeyRuleKind = runv1.KeyRule_Kind

	// WatchStream is the stream WatchRuns returns.
	WatchStream = grpc.ServerStreamingClient[runv1.WatchRunsResponse]
	// AddTasksStream is the stream AddTasks returns.
	AddTasksStream = grpc.BidiStreamingClient[runv1.AddTasksRequest, runv1.AddTasksResponse]
)

// Requests and responses, named as in the API.
type (
	// CreateRunRequest is the request of the CreateRun RPC.
	CreateRunRequest = runv1.CreateRunRequest
	// CreateRunResponse is the response of the CreateRun RPC.
	CreateRunResponse = runv1.CreateRunResponse
	// AddTasksRequest is the request of the AddTasks RPC.
	AddTasksRequest = runv1.AddTasksRequest
	// AddTasksResponse is the response of the AddTasks RPC.
	AddTasksResponse = runv1.AddTasksResponse
	// SealRunRequest is the request of the SealRun RPC.
	SealRunRequest = runv1.SealRunRequest
	// SealRunResponse is the response of the SealRun RPC.
	SealRunResponse = runv1.SealRunResponse
	// PauseRunRequest is the request of the PauseRun RPC.
	PauseRunRequest = runv1.PauseRunRequest
	// PauseRunResponse is the response of the PauseRun RPC.
	PauseRunResponse = runv1.PauseRunResponse
	// ResumeRunRequest is the request of the ResumeRun RPC.
	ResumeRunRequest = runv1.ResumeRunRequest
	// ResumeRunResponse is the response of the ResumeRun RPC.
	ResumeRunResponse = runv1.ResumeRunResponse
	// CancelRunRequest is the request of the CancelRun RPC.
	CancelRunRequest = runv1.CancelRunRequest
	// CancelRunResponse is the response of the CancelRun RPC.
	CancelRunResponse = runv1.CancelRunResponse
	// GetRunRequest is the request of the GetRun RPC.
	GetRunRequest = runv1.GetRunRequest
	// GetRunResponse is the response of the GetRun RPC.
	GetRunResponse = runv1.GetRunResponse
	// ListRunsRequest is the request of the ListRuns RPC.
	ListRunsRequest = runv1.ListRunsRequest
	// ListRunsResponse is the response of the ListRuns RPC.
	ListRunsResponse = runv1.ListRunsResponse
	// WatchRunsRequest is the request of the WatchRuns RPC.
	WatchRunsRequest = runv1.WatchRunsRequest
	// WatchRunsResponse is the response of the WatchRuns RPC.
	WatchRunsResponse = runv1.WatchRunsResponse
	// ListTasksRequest is the request of the ListTasks RPC.
	ListTasksRequest = runv1.ListTasksRequest
	// ListTasksResponse is the response of the ListTasks RPC.
	ListTasksResponse = runv1.ListTasksResponse
	// GetTaskRequest is the request of the GetTask RPC.
	GetTaskRequest = runv1.GetTaskRequest
	// GetTaskResponse is the response of the GetTask RPC.
	GetTaskResponse = runv1.GetTaskResponse
)

// Run states.
const (
	StateOpen      = runv1.Run_STATE_OPEN
	StateSealed    = runv1.Run_STATE_SEALED
	StateCompleted = runv1.Run_STATE_COMPLETED
	StateCancelled = runv1.Run_STATE_CANCELLED
	StateExpired   = runv1.Run_STATE_EXPIRED
	StateAborted   = runv1.Run_STATE_ABORTED
)

// Abort rules: what AbortReason.Rule names.
const (
	AbortOnCode              = runv1.AbortReason_RULE_CODE
	AbortMaxFailed           = runv1.AbortReason_RULE_MAX_FAILED
	AbortConsecutiveFailures = runv1.AbortReason_RULE_MAX_CONSECUTIVE_FAILURES
)

// Run event types.
const (
	EventCreated   = runv1.RunEvent_TYPE_CREATED
	EventSealed    = runv1.RunEvent_TYPE_SEALED
	EventPaused    = runv1.RunEvent_TYPE_PAUSED
	EventResumed   = runv1.RunEvent_TYPE_RESUMED
	EventCompleted = runv1.RunEvent_TYPE_COMPLETED
	EventCancelled = runv1.RunEvent_TYPE_CANCELLED
	EventExpired   = runv1.RunEvent_TYPE_EXPIRED
	EventStats     = runv1.RunEvent_TYPE_STATS
	EventAborted   = runv1.RunEvent_TYPE_ABORTED
)

// Task states.
const (
	TaskPending   = runv1.TaskStatus_STATE_PENDING
	TaskDelayed   = runv1.TaskStatus_STATE_DELAYED
	TaskInFlight  = runv1.TaskStatus_STATE_IN_FLIGHT
	TaskSucceeded = runv1.TaskStatus_STATE_SUCCEEDED
	TaskFailed    = runv1.TaskStatus_STATE_FAILED
	TaskExpired   = runv1.TaskStatus_STATE_EXPIRED
	TaskCancelled = runv1.TaskStatus_STATE_CANCELLED
)

// Key rules: how a task's queue is derived.
const (
	KeyRuleHost             = runv1.KeyRule_KIND_HOST
	KeyRuleRegisteredDomain = runv1.KeyRule_KIND_REGISTERED_DOMAIN
	KeyRuleExplicit         = runv1.KeyRule_KIND_EXPLICIT
)

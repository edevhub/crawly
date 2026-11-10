// Package task exports the task types of the Frontier API: what a scheduler submits and
// what inspection returns. They are aliases of the generated types, so there is one
// definition of each.
//
// The task a spider's handler receives is worker.Task, a plain Go struct.
package task

import taskv1 "github.com/edevhub/crawly/internal/gen/frontier/task/v1"

type (
	// Spec is a task as a client submits it.
	Spec = taskv1.TaskSpec
	// Task is a task as the Frontier reports it.
	Task = taskv1.Task
	// Request is the HTTP request a task asks a worker to make.
	Request = taskv1.Request
	// Header is one request header; a name can repeat.
	Header = taskv1.Header
	// AddResult is the outcome of adding one task, with the ID the Frontier gave it.
	AddResult = taskv1.AddResult
	// AddStatus says whether a task was added and, if not, why.
	AddStatus = taskv1.AddResult_Status
	// Outcome is what a spider reports about an attempt: its kind, a code and any retry or queue-block request.
	Outcome = taskv1.Outcome
	// OutcomeKind says whether an attempt succeeded, should be retried or failed for good.
	OutcomeKind = taskv1.Outcome_Kind
	// FetchInfo describes the fetch of an attempt: status, final URL, latency, size and items emitted.
	FetchInfo = taskv1.FetchInfo
)

// Results of adding a task.
const (
	StatusAdded      = taskv1.AddResult_STATUS_ADDED
	StatusDuplicate  = taskv1.AddResult_STATUS_DUPLICATE
	StatusOverBudget = taskv1.AddResult_STATUS_OVER_BUDGET
	StatusRunClosed  = taskv1.AddResult_STATUS_RUN_CLOSED
	StatusInvalid    = taskv1.AddResult_STATUS_INVALID
)

// Outcome kinds, as recorded on attempts.
const (
	OutcomeSuccess = taskv1.Outcome_KIND_SUCCESS
	OutcomeRetry   = taskv1.Outcome_KIND_RETRY
	OutcomeFailed  = taskv1.Outcome_KIND_FAILED
)

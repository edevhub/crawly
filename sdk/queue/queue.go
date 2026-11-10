// Package queue exports the queue API of the Frontier: the service client an operator
// uses to inspect and block queues. The types are aliases of the generated types, so
// there is one definition of each.
package queue

import (
	"google.golang.org/grpc"

	queuev1 "github.com/edevhub/crawly/internal/gen/frontier/queue/v1"
)

// Client is the queue service client.
type Client = queuev1.QueueServiceClient

// NewClient returns a queue service client on a connection.
func NewClient(conn grpc.ClientConnInterface) Client { return queuev1.NewQueueServiceClient(conn) }

type (
	// Queue is a queue with its effective limits, limiter state and per-run backlogs.
	Queue = queuev1.Queue
	// LimiterState is the politeness state of a queue, shared by every run with work on it.
	LimiterState = queuev1.LimiterState
	// Backlog is one run's work on a queue.
	Backlog = queuev1.RunBacklog
)

// Requests and responses, named as in the API.
type (
	// ListQueuesRequest is the request of the ListQueues RPC.
	ListQueuesRequest = queuev1.ListQueuesRequest
	// ListQueuesResponse is the response of the ListQueues RPC.
	ListQueuesResponse = queuev1.ListQueuesResponse
	// GetQueueRequest is the request of the GetQueue RPC.
	GetQueueRequest = queuev1.GetQueueRequest
	// GetQueueResponse is the response of the GetQueue RPC.
	GetQueueResponse = queuev1.GetQueueResponse
	// BlockQueueRequest is the request of the BlockQueue RPC.
	BlockQueueRequest = queuev1.BlockQueueRequest
	// BlockQueueResponse is the response of the BlockQueue RPC.
	BlockQueueResponse = queuev1.BlockQueueResponse
)

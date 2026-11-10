// Package sdk is the public API of Crawly. The rest of the module is internal.
//
//   - sdk/worker: write spiders. Implement a Handler or a Parser and register it.
//   - sdk (this package): Client, a connection for schedulers and operators.
//   - sdk/run, sdk/task, sdk/policy, sdk/queue: the API types and service clients.
//
// The API types are aliases of the internal generated types, not copies: there is one
// definition of each.
//
// A scheduler:
//
//	c, _ := sdk.NewClient("localhost:7070", sdk.WithInsecure())
//	c.Runs.CreateRun(ctx, &run.CreateRunRequest{RunId: "refresh/2026-11-10"})
//	c.Seed(ctx, "refresh/2026-11-10", &task.Spec{
//		Request: &task.Request{Url: "https://example.com/"}, Handler: "page"})
//	c.Runs.SealRun(ctx, &run.SealRunRequest{RunId: "refresh/2026-11-10"})
package sdk

import (
	"context"
	"errors"
	"fmt"
	"io"
	"strconv"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"

	"github.com/edevhub/crawly/sdk/policy"
	"github.com/edevhub/crawly/sdk/queue"
	"github.com/edevhub/crawly/sdk/run"
	"github.com/edevhub/crawly/sdk/task"
)

// Client is a connection to a Frontier for schedulers and operators.
type Client struct {
	Runs     run.Client
	Policies policy.Client
	Queues   queue.Client
	conn     *grpc.ClientConn
}

// WithInsecure is the dial option for a connection without transport security: no
// encryption and no authentication of the Frontier. Use it on a trusted network only.
func WithInsecure() grpc.DialOption {
	return grpc.WithTransportCredentials(insecure.NewCredentials())
}

// NewClient connects to the Frontier at target (a gRPC target such as "host:7070").
// Transport security is the caller's choice and has no default: pass
// grpc.WithTransportCredentials, or WithInsecure for plaintext.
func NewClient(target string, dial ...grpc.DialOption) (*Client, error) {
	if target == "" {
		return nil, errors.New("sdk: target is required")
	}
	if len(dial) == 0 {
		return nil, errors.New("sdk: transport security is required: pass grpc.WithTransportCredentials, or sdk.WithInsecure for plaintext")
	}
	conn, err := grpc.NewClient(target, dial...)
	if err != nil {
		return nil, err
	}
	return &Client{Runs: run.NewClient(conn), Policies: policy.NewClient(conn), Queues: queue.NewClient(conn), conn: conn}, nil
}

// Close closes the connection.
func (c *Client) Close() error { return c.conn.Close() }

// seedBatch is how many tasks Seed sends in one request.
const seedBatch = 500

// Seed adds tasks to an open run and returns one result per task, in order. It is a
// convenience over the Runs.AddTasks stream.
//
// Tasks are sent in batches, and each batch is committed on its own. When Seed fails
// part of the way, it returns the error together with the results of the batches that
// were committed. Seeding again is safe: tasks that were added come back as duplicates.
func (c *Client) Seed(ctx context.Context, runID string, tasks ...*task.Spec) ([]*task.AddResult, error) {
	// Cancelling the stream's context when Seed returns releases the stream.
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()
	stream, err := c.Runs.AddTasks(ctx)
	if err != nil {
		return nil, err
	}
	out := make([]*task.AddResult, 0, len(tasks))
	for i := 0; i < len(tasks); i += seedBatch {
		batch := tasks[i:min(i+seedBatch, len(tasks))]
		id := strconv.Itoa(i / seedBatch)
		if err := stream.Send(&run.AddTasksRequest{RunId: runID, BatchId: id, Tasks: batch}); err != nil {
			if errors.Is(err, io.EOF) {
				// The Frontier closed the stream: its status says why.
				_, err = stream.Recv()
			}
			return out, err
		}
		res, err := stream.Recv()
		if err != nil {
			return out, err
		}
		if res.GetBatchId() != id || len(res.GetResults()) != len(batch) {
			return out, fmt.Errorf("sdk: batch %s of %d tasks was answered by batch %q with %d results",
				id, len(batch), res.GetBatchId(), len(res.GetResults()))
		}
		out = append(out, res.GetResults()...)
	}
	return out, nil
}

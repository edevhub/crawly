package sdk_test

import (
	"context"
	"errors"
	"fmt"
	"io"
	"runtime"
	"slices"
	"testing"
	"time"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"github.com/edevhub/crawly/internal/testfrontier"
	"github.com/edevhub/crawly/sdk"
	"github.com/edevhub/crawly/sdk/policy"
	"github.com/edevhub/crawly/sdk/queue"
	"github.com/edevhub/crawly/sdk/run"
	"github.com/edevhub/crawly/sdk/task"
)

func TestNewClientValidates(t *testing.T) {
	if _, err := sdk.NewClient("", sdk.WithInsecure()); err == nil {
		t.Fatal("an empty target must be rejected")
	}
	if _, err := sdk.NewClient("localhost:1"); err == nil {
		t.Fatal("transport security must be chosen explicitly")
	}
}

func client(t *testing.T) *sdk.Client {
	t.Helper()
	c, err := sdk.NewClient(testfrontier.Start(t).Addr, sdk.WithInsecure())
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if err := c.Close(); err != nil {
			t.Errorf("closing the client: %v", err)
		}
	})
	return c
}

// TestSeed: results come back one per task and in order, across batches.
func TestSeed(t *testing.T) {
	c := client(t)
	ctx := context.Background()
	if _, err := c.Runs.CreateRun(ctx, &run.CreateRunRequest{RunId: "seed"}); err != nil {
		t.Fatal(err)
	}

	const n = 1201 // three batches
	specs := make([]*task.Spec, 0, n+1)
	for i := range n {
		specs = append(specs, &task.Spec{Request: &task.Request{Url: fmt.Sprintf("https://h%d.example/p/%d", i%7, i)}, Handler: "page"})
	}
	specs = append(specs, &task.Spec{Request: &task.Request{Url: "https://h0.example/p/0"}, Handler: "page"})
	res, err := c.Seed(ctx, "seed", specs...)
	if err != nil {
		t.Fatal(err)
	}
	if len(res) != n+1 {
		t.Fatalf("%d results for %d tasks", len(res), n+1)
	}
	for i, r := range res[:n] {
		if r.GetStatus() != task.StatusAdded {
			t.Fatalf("task %d: %v", i, r.GetStatus())
		}
	}
	if res[n].GetStatus() != task.StatusDuplicate {
		t.Fatalf("the repeated URL: %v, want a duplicate", res[n].GetStatus())
	}

	// The default dedup key ignores the case of scheme and host and the query order,
	// but not the case of the path.
	res, err = c.Seed(ctx, "seed",
		&task.Spec{Request: &task.Request{Url: "https://x.example/a?b=2&a=1"}, Handler: "page"},
		&task.Spec{Request: &task.Request{Url: "HTTPS://X.example/a?a=1&b=2"}, Handler: "page"},
		&task.Spec{Request: &task.Request{Url: "https://x.example/A?a=1&b=2"}, Handler: "page"},
	)
	if err != nil {
		t.Fatal(err)
	}
	if res[0].GetStatus() != task.StatusAdded || res[1].GetStatus() != task.StatusDuplicate || res[2].GetStatus() != task.StatusAdded {
		t.Fatalf("dedup: %v %v %v", res[0].GetStatus(), res[1].GetStatus(), res[2].GetStatus())
	}

	got, err := c.Runs.GetRun(ctx, &run.GetRunRequest{RunId: "seed"})
	if err != nil {
		t.Fatal(err)
	}
	if st := got.GetRun().GetStats(); st.GetPending() != n+2 || st.GetDuplicates() != 2 {
		t.Fatalf("stats: %v", st)
	}
	if got.GetRun().GetState() != run.StateOpen {
		t.Fatalf("state: %v", got.GetRun().GetState())
	}

	// Requests to one URL that differ in body or method are separate tasks, and the
	// spelling of the method does not make two of one.
	api := func(method, body string) *task.Spec {
		return &task.Spec{Request: &task.Request{Url: "https://api.example/search", Method: method, Body: []byte(body)}, Handler: "page"}
	}
	res, err = c.Seed(ctx, "seed", api("POST", `{"page":1}`), api("POST", `{"page":2}`), api("POST", `{"page":3}`),
		api("post", `{"page":1}`), api("", ""), api("GET", ""))
	if err != nil {
		t.Fatal(err)
	}
	for i, want := range []task.AddStatus{task.StatusAdded, task.StatusAdded, task.StatusAdded, task.StatusDuplicate, task.StatusAdded, task.StatusDuplicate} {
		if res[i].GetStatus() != want {
			t.Fatalf("api request %d: %v, want %v", i, res[i].GetStatus(), want)
		}
	}

	// Tasks that are large together go in several requests, and one over the limit is
	// not sent at all.
	large := make([]*task.Spec, 0, 61)
	for i := range 60 {
		large = append(large, &task.Spec{Request: &task.Request{Url: fmt.Sprintf("https://big.example/%d", i)}, Handler: "page",
			Context: map[string][]byte{"meta": make([]byte, 100<<10)}})
	}
	large = slices.Insert(large, 30, &task.Spec{Request: &task.Request{Url: "https://big.example/huge"}, Handler: "page",
		Context: map[string][]byte{"meta": make([]byte, sdk.MaxTaskBytes)}})
	if res, err = c.Seed(ctx, "seed", large...); err != nil || len(res) != 61 {
		t.Fatalf("large seeds: %d results, %v", len(res), err)
	}
	for i, r := range res {
		if want := map[bool]task.AddStatus{false: task.StatusAdded, true: task.StatusInvalid}[i == 30]; r.GetStatus() != want {
			t.Fatalf("large seed %d: %v, want %v", i, r.GetStatus(), want)
		}
	}

	// The Frontier holds the limit itself, for callers that do not use Seed.
	stream, err := c.Runs.AddTasks(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if err := stream.Send(&run.AddTasksRequest{RunId: "seed", BatchId: "raw", Tasks: []*task.Spec{large[30],
		{Request: &task.Request{Url: "https://big.example/after"}, Handler: "page"}}}); err != nil {
		t.Fatal(err)
	}
	raw, err := stream.Recv()
	if err != nil || len(raw.GetResults()) != 2 || raw.GetResults()[0].GetStatus() != task.StatusInvalid ||
		raw.GetResults()[1].GetStatus() != task.StatusAdded {
		t.Fatalf("a task over the limit sent to the Frontier: %v %v", raw.GetResults(), err)
	}
	if err := stream.CloseSend(); err != nil {
		t.Fatal(err)
	}

	// Seeding nothing is not an error.
	if res, err := c.Seed(ctx, "seed"); err != nil || len(res) != 0 {
		t.Fatalf("empty seed: %v %v", res, err)
	}
}

func TestSeedClosedRun(t *testing.T) {
	c := client(t)
	ctx := context.Background()
	if _, err := c.Runs.CreateRun(ctx, &run.CreateRunRequest{RunId: "closed"}); err != nil {
		t.Fatal(err)
	}
	if _, err := c.Runs.CancelRun(ctx, &run.CancelRunRequest{RunId: "closed"}); err != nil {
		t.Fatal(err)
	}
	res, err := c.Seed(ctx, "closed", &task.Spec{Request: &task.Request{Url: "https://a.example/"}, Handler: "page"})
	if err != nil {
		t.Fatal(err)
	}
	if len(res) != 1 || res[0].GetStatus() != task.StatusRunClosed {
		t.Fatalf("seeding a cancelled run: %v", res)
	}
}

// TestServices: every service is reachable through the one client.
func TestServices(t *testing.T) {
	c := client(t)
	ctx := context.Background()
	if _, err := c.Runs.CreateRun(ctx, &run.CreateRunRequest{RunId: "svc"}); err != nil {
		t.Fatal(err)
	}
	if _, err := c.Seed(ctx, "svc", &task.Spec{Request: &task.Request{Url: "https://a.example/"}, Handler: "page"}); err != nil {
		t.Fatal(err)
	}

	pols, err := c.Policies.ListPolicies(ctx, &policy.ListPoliciesRequest{})
	if err != nil || len(pols.GetPolicies()) != 1 || pols.GetPolicies()[0].GetName() != "fast" {
		t.Fatalf("ListPolicies: %v %v", pols, err)
	}
	qs, err := c.Queues.ListQueues(ctx, &queue.ListQueuesRequest{})
	if err != nil || len(qs.GetQueues()) != 1 {
		t.Fatalf("ListQueues: %v %v", qs, err)
	}
	runs, err := c.Runs.ListRuns(ctx, &run.ListRunsRequest{})
	if err != nil || len(runs.GetRuns()) != 1 || runs.GetRuns()[0].GetRunId() != "svc" {
		t.Fatalf("ListRuns: %v %v", runs, err)
	}
}

// TestSeedReleasesItsStream: Seed must not leave a stream behind on each call.
func TestSeedReleasesItsStream(t *testing.T) {
	c := client(t)
	ctx := context.Background()
	if _, err := c.Runs.CreateRun(ctx, &run.CreateRunRequest{RunId: "leak"}); err != nil {
		t.Fatal(err)
	}
	seed := func(i int) {
		if _, err := c.Seed(ctx, "leak", &task.Spec{Request: &task.Request{Url: fmt.Sprintf("https://a.example/%d", i)}, Handler: "page"}); err != nil {
			t.Fatal(err)
		}
	}
	seed(0) // the connection and its goroutines exist from here on
	before := runtime.NumGoroutine()
	for i := 1; i <= 100; i++ {
		seed(i)
	}
	// Streams are torn down asynchronously: give them a moment.
	var after int
	for deadline := time.Now().Add(5 * time.Second); time.Now().Before(deadline); time.Sleep(20 * time.Millisecond) {
		if after = runtime.NumGoroutine(); after <= before+5 {
			return
		}
	}
	t.Fatalf("goroutines grew from %d to %d over 100 calls", before, after)
}

// TestSeedUnknownRun: a failure is an error, not a silent empty result.
func TestSeedUnknownRun(t *testing.T) {
	c := client(t)
	res, err := c.Seed(context.Background(), "no-such-run", &task.Spec{Request: &task.Request{Url: "https://a.example/"}, Handler: "page"})
	if err == nil {
		t.Fatalf("seeding a run that does not exist: %v", res)
	}
	if errors.Is(err, io.EOF) {
		t.Fatalf("the error must say why, got %v", err)
	}
}

// TestWatchLate: a watcher that subscribes after a run has ended is told so at once.
func TestWatchLate(t *testing.T) {
	c := client(t)
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	if _, err := c.Runs.CreateRun(ctx, &run.CreateRunRequest{RunId: "done"}); err != nil {
		t.Fatal(err)
	}
	if _, err := c.Runs.SealRun(ctx, &run.SealRunRequest{RunId: "done"}); err != nil { // sealed and empty: completed
		t.Fatal(err)
	}
	watch, err := c.Runs.WatchRuns(ctx, &run.WatchRunsRequest{RunId: "done"})
	if err != nil {
		t.Fatal(err)
	}
	res, err := watch.Recv()
	if err != nil {
		t.Fatal(err)
	}
	if res.GetEvent().GetType() != run.EventCompleted || res.GetEvent().GetRun().GetState() != run.StateCompleted {
		t.Fatalf("first event: %v", res.GetEvent())
	}

	// A run that is still open is described by a stats snapshot.
	if _, err := c.Runs.CreateRun(ctx, &run.CreateRunRequest{RunId: "open"}); err != nil {
		t.Fatal(err)
	}
	watch, err = c.Runs.WatchRuns(ctx, &run.WatchRunsRequest{RunId: "open"})
	if err != nil {
		t.Fatal(err)
	}
	if res, err = watch.Recv(); err != nil || res.GetEvent().GetType() != run.EventStats {
		t.Fatalf("first event of an open run: %v %v", res.GetEvent(), err)
	}
}

// TestWait: Wait returns the run in its final state, reports its events, and does not
// wait for a run that does not exist.
func TestWait(t *testing.T) {
	c := client(t)
	ctx, cancel := context.WithTimeout(t.Context(), 10*time.Second)
	defer cancel()

	if _, err := c.Wait(ctx, "nothing"); status.Code(err) != codes.NotFound {
		t.Fatalf("a run that does not exist: %v", err)
	}

	if _, err := c.Runs.CreateRun(ctx, &run.CreateRunRequest{RunId: "w"}); err != nil {
		t.Fatal(err)
	}
	var events []run.EventType
	done := make(chan *run.Run, 1)
	go func() {
		got, err := c.Wait(ctx, "w", sdk.OnEvent(func(ev *run.Event) { events = append(events, ev.GetType()) }))
		if err != nil {
			t.Errorf("Wait: %v", err)
		}
		done <- got
	}()
	select {
	case got := <-done:
		t.Fatalf("Wait returned for an open run: %v", got)
	case <-time.After(200 * time.Millisecond):
	}
	if _, err := c.Runs.CancelRun(ctx, &run.CancelRunRequest{RunId: "w"}); err != nil {
		t.Fatal(err)
	}
	got := <-done
	if got.GetState() != run.StateCancelled {
		t.Fatalf("the final state: %v", got.GetState())
	}
	if len(events) == 0 || events[len(events)-1] != run.EventCancelled {
		t.Fatalf("events: %v", events)
	}

	// A run that has ended already is returned at once.
	if got, err := c.Wait(ctx, "w"); err != nil || got.GetState() != run.StateCancelled {
		t.Fatalf("an ended run: %v %v", got, err)
	}

	// The caller's context ends the wait.
	if _, err := c.Runs.CreateRun(ctx, &run.CreateRunRequest{RunId: "open"}); err != nil {
		t.Fatal(err)
	}
	short, stop := context.WithTimeout(ctx, 100*time.Millisecond)
	defer stop()
	if _, err := c.Wait(short, "open"); !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("a cancelled wait: %v", err)
	}
}

// TestWaitSurvivesRestart: the watch stream breaks when a Frontier goes away; Wait
// reconnects and still returns the final state.
func TestWaitSurvivesRestart(t *testing.T) {
	f := testfrontier.Start(t)
	c, err := sdk.NewClient(f.Addr, sdk.WithInsecure())
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = c.Close() }()
	ctx, cancel := context.WithTimeout(t.Context(), 20*time.Second)
	defer cancel()
	if _, err := c.Runs.CreateRun(ctx, &run.CreateRunRequest{RunId: "r"}); err != nil {
		t.Fatal(err)
	}
	done := make(chan *run.Run, 1)
	go func() {
		got, err := c.Wait(ctx, "r")
		if err != nil {
			t.Errorf("Wait: %v", err)
		}
		done <- got
	}()
	time.Sleep(200 * time.Millisecond) // the watch is open
	f.Stop()
	time.Sleep(500 * time.Millisecond) // Wait is retrying
	f.Restart()
	select {
	case got := <-done:
		t.Fatalf("Wait returned across the restart: %v", got)
	case <-time.After(300 * time.Millisecond):
	}
	// Sealed and empty: the run completes.
	for {
		if _, err = c.Runs.SealRun(ctx, &run.SealRunRequest{RunId: "r"}); status.Code(err) != codes.Unavailable {
			break
		}
		time.Sleep(50 * time.Millisecond)
	}
	if err != nil {
		t.Fatal(err)
	}
	if got := <-done; got.GetState() != run.StateCompleted {
		t.Fatalf("the final state: %v", got.GetState())
	}
}

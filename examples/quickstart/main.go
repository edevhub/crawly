// Command quickstart is a complete spider and scheduler in one file. It crawls one
// site from a start URL, following links on the same host, and prints the run's stats.
//
// Start a Frontier, then run it:
//
//	go run github.com/edevhub/crawly/cmd/crawly-frontier
//	go run github.com/edevhub/crawly/examples/quickstart -url https://example.com/
package main

import (
	"context"
	"flag"
	"fmt"
	"log"
	"math"
	"net/url"
	"os/signal"
	"regexp"
	"syscall"
	"time"

	"github.com/edevhub/crawly/sdk"
	"github.com/edevhub/crawly/sdk/run"
	"github.com/edevhub/crawly/sdk/task"
	"github.com/edevhub/crawly/sdk/worker"
)

var href = regexp.MustCompile(`href="([^"#]+)"`)

// linkSpider is the whole spider: it is given a fetched page and returns the links it
// wants followed. The SDK fetches, retries, heartbeats and reports.
type linkSpider struct{}

func (linkSpider) Parse(_ context.Context, t *worker.Task, resp *worker.Response) (*worker.Result, error) {
	base, err := url.Parse(resp.URL)
	if err != nil {
		return nil, worker.Fail("bad_url", err)
	}
	res := &worker.Result{Items: 1}
	for _, m := range href.FindAllStringSubmatch(string(resp.Body), -1) {
		link, err := base.Parse(m[1])
		if err != nil || link.Host != base.Host || (link.Scheme != "http" && link.Scheme != "https") {
			continue // stay on the site
		}
		res.Tasks = append(res.Tasks, worker.NewTask{URL: link.String()})
	}
	log.Printf("depth %d  %s  %d links", t.Depth, t.URL, len(res.Tasks))
	return res, nil
}

func main() {
	if err := crawl(); err != nil {
		log.Fatal(err)
	}
}

func crawl() error {
	addr := flag.String("frontier", "localhost:7070", "frontier address")
	start := flag.String("url", "https://example.com/", "start URL")
	depth := flag.Uint("depth", 2, "maximum link depth")
	flag.Parse()
	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	// The worker: register the spider and run.
	w, err := worker.New(*addr, worker.WithInsecure(), worker.WithMaxConcurrentTasks(4))
	if err != nil {
		return err
	}
	if err := w.Register("page", worker.HTTP(linkSpider{}, worker.WithUserAgent("crawly-quickstart"))); err != nil {
		return err
	}
	workerDone := make(chan error, 1)
	go func() { workerDone <- w.Run(ctx) }()

	// The scheduler: create a run, seed it, seal it, watch it.
	c, err := sdk.NewClient(*addr, sdk.WithInsecure())
	if err != nil {
		return err
	}
	defer func() { _ = c.Close() }() // the run is over; a close error changes nothing
	runID := fmt.Sprintf("quickstart/%d", time.Now().Unix())
	if _, err := c.Runs.CreateRun(ctx, &run.CreateRunRequest{RunId: runID,
		Limits: &run.Limits{MaxDepth: uint32(min(*depth, math.MaxUint32)), MaxTasks: 500}}); err != nil {
		return err
	}
	if _, err := c.Seed(ctx, runID, &task.Spec{Request: &task.Request{Url: *start}, Handler: "page"}); err != nil {
		return err
	}
	if _, err := c.Runs.SealRun(ctx, &run.SealRunRequest{RunId: runID}); err != nil {
		return err
	}
	if _, err := c.Wait(ctx, runID, sdk.StatsEvery(2*time.Second), sdk.OnEvent(func(ev *run.Event) {
		st := ev.GetRun().GetStats()
		log.Printf("%-15s pending %d  delayed %d  in flight %d  ok %d  failed %d  codes %v", ev.GetType(),
			st.GetPending(), st.GetDelayed(), st.GetInFlight(), st.GetSucceeded(), st.GetFailed(), st.GetByCode())
	})); err != nil && ctx.Err() == nil { // interrupted: stop the worker below
		return err
	}
	stop()
	if err := <-workerDone; err != nil {
		log.Printf("worker: %v", err)
	}
	return nil
}

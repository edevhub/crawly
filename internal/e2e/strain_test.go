package e2e

import (
	"context"
	"fmt"
	"net/http"
	"net/http/httptest"
	"net/url"
	"sync"
	"testing"
	"time"

	"github.com/edevhub/crawly/internal/testfrontier"
	"github.com/edevhub/crawly/sdk/queue"
	"github.com/edevhub/crawly/sdk/run"
	"github.com/edevhub/crawly/sdk/task"
	"github.com/edevhub/crawly/sdk/worker"
)

// strainPolicies is the policy of a site that can take five requests a second, with
// the retry settings of a default install: four attempts, backoffs of 0.5, 1 and 2 s.
const strainPolicies = `{"policies": [{"name": "site", "limits": {"ratePerSecond": 5, "burst": 1, "maxConcurrency": 4},
  "behaviour": {"maxAttempts": 4, "backoff": {"initial": "0.5s", "multiplier": 2, "max": "2s", "jitter": 0}, "leaseDuration": "10s"}}]}`

// TestStrainSlowsTheQueue: a site answers 503 for 6 seconds, at a policy of 5 requests a
// second. Without adaptive pacing the site gets 30 requests in that time, all of them
// failing, and the pages that used up their attempts fail for good. With it the queue
// slows down while the site fails, the site is asked far less, and every page is
// scraped once the site is back.
func TestStrainSlowsTheQueue(t *testing.T) {
	t.Run("memory", func(t *testing.T) {
		testStrain(t, func(tb testing.TB) *testfrontier.Frontier {
			tb.Helper()
			return testfrontier.StartWith(tb, strainPolicies)
		})
	})
	t.Run("postgres", func(t *testing.T) {
		testStrain(t, func(tb testing.TB) *testfrontier.Frontier {
			tb.Helper()
			return testfrontier.StartPostgresWith(tb, strainPolicies)
		})
	})
}

const (
	outage = 6 * time.Second
	pages  = 20
)

func testStrain(t *testing.T, start startFunc) { //nolint:thelper // a test body, not a helper: failures must point into it
	ctx := t.Context()

	// The site is down for `outage` from its first request.
	var mu sync.Mutex
	var began time.Time
	var failing, served int
	site := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		mu.Lock()
		now := time.Now()
		if began.IsZero() {
			began = now
		}
		down := now.Sub(began) < outage
		if down {
			failing++
		} else {
			served++
		}
		mu.Unlock()
		if down {
			http.Error(w, "overloaded", http.StatusServiceUnavailable)
			return
		}
		_, _ = fmt.Fprint(w, "ok")
	}))
	defer site.Close()
	siteURL, err := url.Parse(site.URL)
	if err != nil {
		t.Fatal(err)
	}

	addr := start(t).Addr
	rc := runClient(t, addr)
	w, err := worker.New(addr, worker.WithInsecure(), worker.WithMaxConcurrentTasks(4), worker.WithShutdownGrace(time.Second))
	if err != nil {
		t.Fatal(err)
	}
	register(t, w, "page", worker.HTTP(worker.ParserFunc(func(context.Context, *worker.Task, *worker.Response) (*worker.Result, error) {
		return &worker.Result{Items: 1}, nil
	})))
	workerDone := make(chan error, 1)
	go func() { workerDone <- w.Run(ctx) }()
	t.Cleanup(func() {
		if err := <-workerDone; err != nil {
			t.Errorf("worker: %v", err)
		}
	})

	if _, err := rc.CreateRun(ctx, &run.CreateRunRequest{RunId: "outage"}); err != nil {
		t.Fatal(err)
	}
	specs := make([]*task.Spec, pages)
	for i := range specs {
		specs[i] = &task.Spec{Request: &task.Request{Url: fmt.Sprintf("%s/p/%d", site.URL, i)}, Handler: "page"}
	}
	seed(t, ctx, addr, "outage", specs...)
	if _, err := rc.SealRun(ctx, &run.SealRunRequest{RunId: "outage"}); err != nil {
		t.Fatal(err)
	}

	st := waitRunWithin(t, ctx, rc, "outage", 90*time.Second).GetStats()
	if st.GetSucceeded() != pages || st.GetFailed() != 0 {
		t.Fatalf("every page must be scraped once the site is back: %v", st)
	}
	mu.Lock()
	defer mu.Unlock()
	t.Logf("requests during the outage: %d, after it: %d; attempts: %d", failing, served, st.GetAttempts())
	if failing > 15 {
		t.Fatalf("the site got %d requests in %v of failing, against 30 at the full rate", failing, outage)
	}

	q, err := clients[addr].Queues.GetQueue(ctx, &queue.GetQueueRequest{Key: siteURL.Host})
	if err != nil {
		t.Fatal(err)
	}
	if q.GetQueue().GetLimiter().GetStrainSteps() == 0 || q.GetQueue().GetLimiter().GetLastStrainTime() == nil {
		t.Fatalf("an operator must see why the queue was slow: %v", q.GetQueue().GetLimiter())
	}
}

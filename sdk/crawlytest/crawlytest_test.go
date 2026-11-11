package crawlytest_test

import (
	"context"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"

	"github.com/edevhub/crawly/sdk/crawlytest"
	"github.com/edevhub/crawly/sdk/local"
	"github.com/edevhub/crawly/sdk/run"
	"github.com/edevhub/crawly/sdk/task"
	"github.com/edevhub/crawly/sdk/worker"
)

// site is a listing of three products, one of which is gone and one of which fails once.
func site(t *testing.T) *httptest.Server {
	t.Helper()
	var flaky atomic.Int64
	mux := http.NewServeMux()
	mux.HandleFunc("/{$}", func(w http.ResponseWriter, _ *http.Request) { _, _ = fmt.Fprint(w, "/p/1 /p/2 /p/3 /p/1") })
	mux.HandleFunc("/p/1", func(w http.ResponseWriter, _ *http.Request) { _, _ = fmt.Fprint(w, "product 1") })
	mux.HandleFunc("/p/2", func(w http.ResponseWriter, _ *http.Request) {
		if flaky.Add(1) == 1 {
			w.WriteHeader(http.StatusServiceUnavailable)
			return
		}
		_, _ = fmt.Fprint(w, "product 2")
	})
	srv := httptest.NewServer(mux)
	t.Cleanup(srv.Close)
	return srv
}

// TestCrawl is a spider test as a user of the SDK writes it: recorded pages, the real
// handlers, and assertions on the run and on what was emitted.
func TestCrawl(t *testing.T) {
	srv := site(t)
	f := crawlytest.Start(t)
	w, err := f.Worker()
	if err != nil {
		t.Fatal(err)
	}
	var items atomic.Int64
	listing := worker.ParserFunc(func(_ context.Context, _ *worker.Task, r *worker.Response) (*worker.Result, error) {
		res := &worker.Result{}
		for path := range strings.FieldsSeq(string(r.Body)) {
			res.Tasks = append(res.Tasks, worker.NewTask{URL: srv.URL + path, Handler: "product"})
		}
		return res, nil
	})
	product := worker.ParserFunc(func(context.Context, *worker.Task, *worker.Response) (*worker.Result, error) {
		items.Add(1)
		return &worker.Result{Items: 1}, nil
	})
	if err := w.Register("listing", worker.HTTP(listing)); err != nil {
		t.Fatal(err)
	}
	if err := w.Register("product", worker.HTTP(product)); err != nil {
		t.Fatal(err)
	}

	final, err := f.Crawl(t.Context(), w, nil, &task.Spec{Request: &task.Request{Url: srv.URL + "/"}, Handler: "listing"})
	if err != nil {
		t.Fatal(err)
	}
	st := final.GetStats()
	// The listing and two products succeed, one of them on its second attempt; the
	// third product is a 404; the repeated link is a duplicate.
	if final.GetState() != run.StateCompleted || st.GetSucceeded() != 3 || st.GetFailed() != 1 || st.GetDuplicates() != 1 ||
		st.GetByCode()["http_404"] != 1 || st.GetByCode()["http_503"] != 1 || items.Load() != 2 {
		t.Fatalf("state %v, stats %v, %d items", final.GetState(), st, items.Load())
	}
}

// TestCrawlErrors: what cannot be crawled is an error, not a run that never ends.
func TestCrawlErrors(t *testing.T) {
	f := crawlytest.Start(t)
	w, err := f.Worker()
	if err != nil {
		t.Fatal(err)
	}
	if _, err := f.Crawl(t.Context(), w, nil, &task.Spec{Request: &task.Request{Url: "not a url"}, Handler: "page"}); err == nil {
		t.Error("a seed that is not added must be an error")
	}
	// A worker without handlers cannot run.
	if _, err := f.Crawl(t.Context(), w, nil, &task.Spec{Request: &task.Request{Url: "https://a.example/"}, Handler: "page"}); err == nil {
		t.Error("a worker without handlers must be an error")
	}
	if _, err := local.Start(local.WithPolicies([]byte("{"))); err == nil {
		t.Error("policies that do not parse must be an error")
	}
	f.Close() // twice is safe: the test's clean-up closes it again
}

// Package crawlytest runs a Frontier inside a test, so that a spider is tested end to
// end, with the scheduling, retries and dedup it meets in production, and without a
// Frontier process or a database:
//
//	func TestSpider(t *testing.T) {
//		site := httptest.NewServer(recordedPages)
//		defer site.Close()
//		f := crawlytest.Start(t)
//		w, _ := f.Worker()
//		w.Register("page", worker.HTTP(parser))
//		final, err := f.Crawl(t.Context(), w,
//			[]*task.Spec{{Request: &task.Request{Url: site.URL + "/"}, Handler: "page"}})
//		// assert on final.GetStats() and on what the parser emitted
//	}
package crawlytest

import (
	"log/slog"
	"testing"

	"github.com/edevhub/crawly/sdk/local"
)

// policies are generous limits, short backoff and short leases, so that a test does
// not wait for politeness.
const policies = `{"policies": [{"name": "test", "limits": {"ratePerSecond": 1000, "burst": 100, "maxConcurrency": 16},
  "behaviour": {"maxAttempts": 3, "backoff": {"initial": "0.02s", "max": "0.1s"}, "leaseDuration": "5s"}}]}`

// Start runs a Frontier on the in-memory store until the test ends. By default its
// limits are generous, so that a test does not wait for politeness, and warnings and
// errors of the Frontier and its workers go to the test's log. Options replace the
// defaults: pass local.WithPolicies to test under the limits of production.
func Start(tb testing.TB, opts ...local.Option) *local.Frontier {
	tb.Helper()
	log := slog.New(slog.NewTextHandler(testLog{tb}, &slog.HandlerOptions{Level: slog.LevelWarn}))
	f, err := local.Start(append([]local.Option{local.WithPolicies([]byte(policies)), local.WithLogger(log)}, opts...)...)
	if err != nil {
		tb.Fatal(err)
	}
	tb.Cleanup(f.Close)
	return f
}

// testLog writes log lines to the test's log.
type testLog struct{ tb testing.TB }

func (l testLog) Write(p []byte) (int, error) {
	l.tb.Helper()
	l.tb.Log(string(p))
	return len(p), nil
}

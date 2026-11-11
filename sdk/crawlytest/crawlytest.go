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
//		final, err := f.Crawl(t.Context(), w, nil,
//			&task.Spec{Request: &task.Request{Url: site.URL + "/"}, Handler: "page"})
//		// assert on final.GetStats() and on what the parser emitted
//	}
package crawlytest

import (
	"testing"

	"github.com/edevhub/crawly/sdk/local"
)

// Policies are what Start uses unless told otherwise: generous limits, short backoff
// and short leases, so that a test does not wait for politeness.
const Policies = `{"policies": [{"name": "test", "limits": {"ratePerSecond": 1000, "burst": 100, "maxConcurrency": 16},
  "behaviour": {"maxAttempts": 3, "backoff": {"initial": "0.02s", "max": "0.1s"}, "leaseDuration": "5s"}}]}`

// Start runs a Frontier on the in-memory store until the test ends. Options replace
// the defaults: pass local.WithPolicies to test under the limits of production.
func Start(tb testing.TB, opts ...local.Option) *local.Frontier {
	tb.Helper()
	f, err := local.Start(append([]local.Option{local.WithPolicies([]byte(Policies))}, opts...)...)
	if err != nil {
		tb.Fatal(err)
	}
	tb.Cleanup(f.Close)
	return f
}

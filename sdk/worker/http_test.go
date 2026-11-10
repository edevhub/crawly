package worker

import (
	"context"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"
)

// echo is a parser that records what it was given.
type echo struct {
	calls int
	resp  *Response
	res   *Result
	err   error
}

func (e *echo) Parse(_ context.Context, _ *Task, r *Response) (*Result, error) {
	e.calls++
	e.resp = r
	return e.res, e.err
}

func get(url string) *Task { return &Task{Method: http.MethodGet, URL: url} }

// failure asserts err is a classified failure and returns it.
func failure(t *testing.T, err error, kind Kind, code string) *Error {
	t.Helper()
	var e *Error
	if !errors.As(err, &e) {
		t.Fatalf("want a classified failure, got %v", err)
	}
	if e.Kind != kind || e.Code != code {
		t.Fatalf("got kind %d code %q, want kind %d code %q", e.Kind, e.Code, kind, code)
	}
	return e
}

// requireFailure is failure for callers that only need the assertion.
func requireFailure(t *testing.T, err error, kind Kind, code string) {
	t.Helper()
	_ = failure(t, err, kind, code)
}

func TestHTTPSuccess(t *testing.T) {
	site := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("X-Seen-Agent", r.UserAgent())
		_, _ = fmt.Fprint(w, "hello")
	}))
	defer site.Close()

	p := &echo{}
	res, err := HTTP(p).Handle(context.Background(), get(site.URL+"/a"))
	if err != nil {
		t.Fatal(err)
	}
	if p.calls != 1 || string(p.resp.Body) != "hello" || p.resp.StatusCode != http.StatusOK || p.resp.URL != site.URL+"/a" {
		t.Fatalf("response: %+v", p.resp)
	}
	if p.resp.Header.Get("X-Seen-Agent") != "crawly" {
		t.Fatalf("default user agent: %q", p.resp.Header.Get("X-Seen-Agent"))
	}
	// A parser that returns nothing still yields a result with the fetch filled in.
	if res == nil || res.Code != "http_200" || res.Fetch == nil || res.Fetch.HTTPStatus != 200 ||
		res.Fetch.Bytes != 5 || res.Fetch.FinalURL != site.URL+"/a" {
		t.Fatalf("result: %+v", res)
	}
}

func TestHTTPKeepsParserResult(t *testing.T) {
	site := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) { w.WriteHeader(http.StatusNoContent) }))
	defer site.Close()

	own := &FetchInfo{ResponseRef: "s3://raw/1"}
	p := &echo{res: &Result{Code: "empty_listing", Items: 2, Fetch: own}}
	res, err := HTTP(p).Handle(context.Background(), get(site.URL))
	if err != nil || res.Code != "empty_listing" || res.Items != 2 || res.Fetch != own {
		t.Fatalf("the parser's code and fetch info must win: %+v %v", res, err)
	}

	// A parser's failure is passed through untouched.
	p = &echo{err: Fail("layout_changed", nil)}
	_, err = HTTP(p).Handle(context.Background(), get(site.URL))
	requireFailure(t, err, KindFail, "layout_changed")
}

func TestHTTPRequest(t *testing.T) {
	var got *http.Request
	var body string
	site := httptest.NewServer(http.HandlerFunc(func(_ http.ResponseWriter, r *http.Request) {
		b, _ := io.ReadAll(r.Body)
		got, body = r, string(b)
	}))
	defer site.Close()

	task := &Task{Method: http.MethodPost, URL: site.URL + "/search", Body: []byte(`{"q":1}`),
		Header: http.Header{"User-Agent": {"task-agent"}, "X-Token": {"abc"}}}
	if _, err := HTTP(&echo{}, WithUserAgent("spider-agent")).Handle(context.Background(), task); err != nil {
		t.Fatal(err)
	}
	if got.Method != http.MethodPost || body != `{"q":1}` || got.Header.Get("X-Token") != "abc" {
		t.Fatalf("request: %s %q %v", got.Method, body, got.Header)
	}
	if got.UserAgent() != "task-agent" {
		t.Fatalf("the task's own User-Agent must win, got %q", got.UserAgent())
	}

	if _, err := HTTP(&echo{}, WithUserAgent("spider-agent")).Handle(context.Background(), get(site.URL)); err != nil {
		t.Fatal(err)
	}
	if got.UserAgent() != "spider-agent" {
		t.Fatalf("WithUserAgent: %q", got.UserAgent())
	}
}

func TestHTTPStatusClassification(t *testing.T) {
	cases := []struct {
		status int
		kind   Kind
	}{
		{400, KindFail}, {401, KindFail}, {403, KindFail}, {404, KindFail}, {410, KindFail},
		{408, KindRetry}, {429, KindRetry}, {500, KindRetry}, {502, KindRetry}, {503, KindRetry},
	}
	for _, c := range cases {
		site := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) { w.WriteHeader(c.status) }))
		p := &echo{}
		res, err := HTTP(p).Handle(context.Background(), get(site.URL))
		site.Close()
		e := failure(t, err, c.kind, fmt.Sprintf("http_%d", c.status))
		if e.BlockQueueFor != 0 {
			t.Errorf("%d: no Retry-After, yet the queue is blocked for %v", c.status, e.BlockQueueFor)
		}
		if p.calls != 0 {
			t.Errorf("%d: Parse must be called only for 2xx", c.status)
		}
		if res == nil || res.Fetch == nil || res.Fetch.HTTPStatus != c.status {
			t.Errorf("%d: a failed fetch must still carry its fetch info: %+v", c.status, res)
		}
	}
}

func TestHTTPRetryAfterBlocksQueue(t *testing.T) {
	site := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Retry-After", "120")
		w.WriteHeader(http.StatusTooManyRequests)
	}))
	defer site.Close()
	_, err := HTTP(&echo{}).Handle(context.Background(), get(site.URL))
	if e := failure(t, err, KindRetry, "http_429"); e.BlockQueueFor != 2*time.Minute {
		t.Fatalf("block for %v, want 2m", e.BlockQueueFor)
	}
}

func TestRetryAfter(t *testing.T) {
	if d := retryAfter(""); d != 0 {
		t.Errorf("empty: %v", d)
	}
	if d := retryAfter(" 30 "); d != 30*time.Second {
		t.Errorf("seconds: %v", d)
	}
	if d := retryAfter("soon"); d != 0 {
		t.Errorf("garbage: %v", d)
	}
	at := time.Now().Add(90 * time.Second).UTC().Format(http.TimeFormat)
	if d := retryAfter(at); d < 80*time.Second || d > 91*time.Second {
		t.Errorf("date: %v", d)
	}
}

func TestHTTPRedirects(t *testing.T) {
	other := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) { _, _ = fmt.Fprint(w, "other host") }))
	defer other.Close()
	mux := http.NewServeMux()
	mux.HandleFunc("/same", func(w http.ResponseWriter, r *http.Request) { http.Redirect(w, r, "/final", http.StatusFound) })
	mux.HandleFunc("/final", func(w http.ResponseWriter, _ *http.Request) { _, _ = fmt.Fprint(w, "final") })
	mux.HandleFunc("/away", func(w http.ResponseWriter, r *http.Request) {
		http.Redirect(w, r, other.URL+"/landing", http.StatusMovedPermanently)
	})
	mux.HandleFunc("/loop", func(w http.ResponseWriter, r *http.Request) { http.Redirect(w, r, "/loop", http.StatusFound) })
	mux.HandleFunc("/nowhere", func(w http.ResponseWriter, _ *http.Request) { w.WriteHeader(http.StatusFound) })
	site := httptest.NewServer(mux)
	defer site.Close()
	ctx := context.Background()

	// The same host: followed, and the parser sees the final URL.
	p := &echo{}
	res, err := HTTP(p).Handle(ctx, get(site.URL+"/same"))
	if err != nil || string(p.resp.Body) != "final" || p.resp.URL != site.URL+"/final" || res.Fetch.FinalURL != site.URL+"/final" {
		t.Fatalf("same-host redirect: %+v %v", p.resp, err)
	}

	// Another host: not fetched. The target becomes a discovered task so that it is
	// fetched under its own host's limits, and the task's context goes with it.
	p = &echo{}
	task := get(site.URL + "/away")
	task.Context = map[string][]byte{"k": []byte("v")}
	res, err = HTTP(p).Handle(ctx, task)
	if err != nil || p.calls != 0 || res.Code != "redirect" || len(res.Tasks) != 1 {
		t.Fatalf("cross-host redirect: %+v %v", res, err)
	}
	if res.Tasks[0].URL != other.URL+"/landing" || string(res.Tasks[0].Context["k"]) != "v" || res.Fetch.HTTPStatus != 301 {
		t.Fatalf("redirect task: %+v", res.Tasks[0])
	}

	// Unless the spider opts in.
	p = &echo{}
	if _, err = HTTP(p, WithCrossHostRedirects()).Handle(ctx, get(site.URL+"/away")); err != nil || string(p.resp.Body) != "other host" {
		t.Fatalf("WithCrossHostRedirects: %+v %v", p.resp, err)
	}

	// A redirect loop is a transport error: retried, never parsed.
	_, err = HTTP(&echo{}).Handle(ctx, get(site.URL+"/loop"))
	requireFailure(t, err, KindRetry, "fetch_error")

	// A redirect without a Location is a permanent failure.
	_, err = HTTP(&echo{}).Handle(ctx, get(site.URL+"/nowhere"))
	requireFailure(t, err, KindFail, "http_302")
}

func TestHTTPTransportFailures(t *testing.T) {
	slow := httptest.NewServer(http.HandlerFunc(func(_ http.ResponseWriter, r *http.Request) {
		select {
		case <-r.Context().Done():
		case <-time.After(5 * time.Second):
		}
	}))
	defer slow.Close()
	ctx := context.Background()

	// The policy's fetch timeout bounds the fetch.
	task := get(slow.URL)
	task.FetchTimeout = 50 * time.Millisecond
	began := time.Now()
	_, err := HTTP(&echo{}).Handle(ctx, task)
	requireFailure(t, err, KindRetry, "fetch_error")
	if time.Since(began) > 2*time.Second {
		t.Fatalf("the fetch timeout was not applied: %v", time.Since(began))
	}

	// Nothing listens there.
	dead := httptest.NewServer(http.NotFoundHandler())
	dead.Close()
	_, err = HTTP(&echo{}).Handle(ctx, get(dead.URL))
	requireFailure(t, err, KindRetry, "fetch_error")

	// A request that cannot be built is permanent: retrying cannot help.
	_, err = HTTP(&echo{}).Handle(ctx, &Task{Method: "BAD METHOD", URL: slow.URL})
	requireFailure(t, err, KindFail, "bad_request")
	_, err = HTTP(&echo{}).Handle(ctx, get("://no-scheme"))
	requireFailure(t, err, KindFail, "bad_request")
}

func TestHTTPOptions(t *testing.T) {
	site := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("X-Via", r.Header.Get("X-Via"))
		_, _ = fmt.Fprint(w, strings.Repeat("x", 1000))
	}))
	defer site.Close()

	// A body over the cap is a failure: the parser is never given half a page.
	p := &echo{}
	_, err := HTTP(p, WithMaxBodyBytes(999)).Handle(context.Background(), get(site.URL))
	requireFailure(t, err, KindFail, "body_too_large")
	if p.calls != 0 {
		t.Fatal("Parse was called with a truncated body")
	}
	// A body exactly at the cap is whole.
	res, err := HTTP(p, WithMaxBodyBytes(1000)).Handle(context.Background(), get(site.URL))
	if err != nil || len(p.resp.Body) != 1000 || res.Fetch.Bytes != 1000 {
		t.Fatalf("WithMaxBodyBytes: %d bytes, %v", len(p.resp.Body), err)
	}

	// A custom client is used, and it is copied: the caller's is left as it was.
	client := &http.Client{Transport: via{}}
	p = &echo{}
	if _, err := HTTP(p, WithHTTPClient(client)).Handle(context.Background(), get(site.URL)); err != nil {
		t.Fatal(err)
	}
	if p.resp.Header.Get("X-Via") != "custom" {
		t.Fatal("WithHTTPClient: the custom transport was not used")
	}
	if client.CheckRedirect != nil {
		t.Fatal("WithHTTPClient must not modify the caller's client")
	}
}

// via is a transport that marks requests.
type via struct{}

func (via) RoundTrip(r *http.Request) (*http.Response, error) {
	r = r.Clone(r.Context())
	r.Header.Set("X-Via", "custom")
	return http.DefaultTransport.RoundTrip(r)
}

// TestHTTPRedirectKeepsRequest: a 307 or 308 to another host repeats the request as it
// was, so the discovered task carries the method, body and headers.
func TestHTTPRedirectKeepsRequest(t *testing.T) {
	for _, status := range []int{http.StatusTemporaryRedirect, http.StatusPermanentRedirect} {
		site := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			http.Redirect(w, r, "https://other.example/search", status)
		}))
		task := &Task{Method: http.MethodPost, URL: site.URL, Body: []byte(`{"q":1}`), Header: http.Header{"X-Token": {"abc"}}}
		res, err := HTTP(&echo{}).Handle(context.Background(), task)
		site.Close()
		if err != nil || len(res.Tasks) != 1 {
			t.Fatalf("%d: %+v %v", status, res, err)
		}
		next := res.Tasks[0]
		if next.URL != "https://other.example/search" || next.Method != http.MethodPost || string(next.Body) != `{"q":1}` ||
			next.Header.Get("X-Token") != "abc" {
			t.Fatalf("%d: the request was not carried over: %+v", status, next)
		}
	}
}

func TestRetryAfterIsBounded(t *testing.T) {
	for _, v := range []string{"99999999999", "9223372036854775807", time.Now().AddDate(5, 0, 0).UTC().Format(http.TimeFormat)} {
		if d := retryAfter(v); d != maxRetryAfter {
			t.Errorf("%q: %v, want the cap %v", v, d, maxRetryAfter)
		}
	}
	for _, v := range []string{"-5", time.Now().Add(-time.Hour).UTC().Format(http.TimeFormat)} {
		if d := retryAfter(v); d != 0 {
			t.Errorf("%q: %v, want 0", v, d)
		}
	}
}

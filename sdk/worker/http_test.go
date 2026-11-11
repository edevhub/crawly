package worker

import (
	"context"
	"crypto/x509"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"net/url"
	"os"
	"strconv"
	"strings"
	"syscall"
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

func get(target string) *Task { return &Task{Method: http.MethodGet, URL: target} }

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

// TestHTTPStrainSignal: the statuses and failures that are signs of strain on the site
// carry the strain signal; a 429 with a Retry-After blocks the queue instead, and the
// other failures say nothing about the site.
func TestHTTPStrainSignal(t *testing.T) {
	cases := []struct {
		name       string
		status     int
		retryAfter string
		strain     bool
	}{
		{"502", 502, "", true}, {"503", 503, "", true}, {"504", 504, "", true},
		{"429 without Retry-After", 429, "", true}, {"429 with Retry-After", 429, "30", false},
		{"500", 500, "", false}, {"408", 408, "", false}, {"404", 404, "", false},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			site := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
				if c.retryAfter != "" {
					w.Header().Set("Retry-After", c.retryAfter)
				}
				w.WriteHeader(c.status)
			}))
			defer site.Close()
			_, err := HTTP(&echo{}).Handle(context.Background(), get(site.URL))
			var e *Error
			if !errors.As(err, &e) {
				t.Fatalf("want a classified failure, got %v", err)
			}
			if e.Strain != c.strain {
				t.Fatalf("strain = %v, want %v", e.Strain, c.strain)
			}
		})
	}
}

// TestHTTPStrainOnTransportFailures: a timeout and a refused connection are signs of
// strain on the site.
func TestHTTPStrainOnTransportFailures(t *testing.T) {
	slow := httptest.NewServer(http.HandlerFunc(func(_ http.ResponseWriter, r *http.Request) { <-r.Context().Done() }))
	defer slow.Close()
	task := get(slow.URL)
	task.FetchTimeout = 50 * time.Millisecond
	_, err := HTTP(&echo{}).Handle(context.Background(), task)
	if e := failure(t, err, KindRetry, "fetch_error"); !e.Strain {
		t.Error("a fetch timeout must signal strain")
	}
	dead := httptest.NewServer(http.NotFoundHandler())
	dead.Close()
	_, err = HTTP(&echo{}).Handle(context.Background(), get(dead.URL))
	if e := failure(t, err, KindRetry, "fetch_error"); !e.Strain {
		t.Error("a refused connection must signal strain")
	}
}

// TestHTTPNoStrainOnLocalFailures: what goes wrong on the worker's side must not slow a
// queue down for every run and every other worker.
func TestHTTPNoStrainOnLocalFailures(t *testing.T) {
	// A name that does not resolve.
	_, err := HTTP(&echo{}).Handle(context.Background(), get("http://no-such-host.invalid/"))
	if e := failure(t, err, KindRetry, "fetch_error"); e.Strain {
		t.Errorf("a name that does not resolve is no sign of strain: %v", err)
	}
	// A certificate that does not verify.
	tls := httptest.NewTLSServer(http.NotFoundHandler())
	defer tls.Close()
	_, err = HTTP(&echo{}).Handle(context.Background(), get(tls.URL))
	if e := failure(t, err, KindRetry, "fetch_error"); e.Strain {
		t.Errorf("a certificate that does not verify is no sign of strain: %v", err)
	}
	// A request cancelled from this side (a shutdown, a revoked lease).
	hang := httptest.NewServer(http.HandlerFunc(func(_ http.ResponseWriter, r *http.Request) { <-r.Context().Done() }))
	defer hang.Close()
	ctx, cancel := context.WithCancel(context.Background())
	time.AfterFunc(50*time.Millisecond, cancel)
	_, err = HTTP(&echo{}).Handle(ctx, get(hang.URL))
	if e := failure(t, err, KindRetry, "fetch_error"); e.Strain {
		t.Errorf("a cancelled request is no sign of strain: %v", err)
	}
}

func TestSiteTrouble(t *testing.T) {
	cases := []struct {
		name string
		err  error
		want bool
	}{
		{"timeout", context.DeadlineExceeded, true},
		{"refused", &net.OpError{Op: "dial", Err: os.NewSyscallError("connect", syscall.ECONNREFUSED)}, true},
		{"reset", &net.OpError{Op: "read", Err: os.NewSyscallError("read", syscall.ECONNRESET)}, true},
		{"closed without an answer", fmt.Errorf("Get: %w", io.EOF), true},
		{"dns", &net.DNSError{Err: "no such host", Name: "x.invalid"}, false},
		{"dns timeout", &net.DNSError{Err: "i/o timeout", IsTimeout: true}, false},
		{"proxy", &net.OpError{Op: "proxyconnect", Err: syscall.ECONNREFUSED}, false},
		{"certificate", x509.UnknownAuthorityError{}, false},
		{"out of descriptors", &net.OpError{Op: "dial", Err: os.NewSyscallError("socket", syscall.EMFILE)}, false},
		{"cancelled", context.Canceled, false},
		{"something else", errors.New("odd"), false},
	}
	for _, c := range cases {
		if got := siteTrouble(c.err); got != c.want {
			t.Errorf("%s: %v, want %v", c.name, got, c.want)
		}
	}
}

// TestHTTPWithStatusOverridesStrain: a parser for a status decides its own outcome, so a
// 503 that the spider says is no sign of strain is none.
func TestHTTPWithStatusOverridesStrain(t *testing.T) {
	site := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) { w.WriteHeader(http.StatusServiceUnavailable) }))
	defer site.Close()
	maintenance := ParserFunc(func(context.Context, *Task, *Response) (*Result, error) {
		return nil, Retry("maintenance", errors.New("planned"), After(time.Hour))
	})
	_, err := HTTP(&echo{}, WithStatus(maintenance, 503)).Handle(context.Background(), get(site.URL))
	if e := failure(t, err, KindRetry, "maintenance"); e.Strain {
		t.Error("the spider's own classification must stand")
	}
}

func TestHTTPRetryAfterBlocksQueue(t *testing.T) {
	site := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Retry-After", "120")
		w.WriteHeader(http.StatusTooManyRequests)
	}))
	defer site.Close()
	_, err := HTTP(&echo{}).Handle(context.Background(), get(site.URL))
	if e := failure(t, err, KindRetry, "http_429"); e.BlockQueueFor != 2*time.Minute || e.Strain {
		t.Fatalf("block for %v, strain %v: want 2m and no strain", e.BlockQueueFor, e.Strain)
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

	// The same host: not followed either. The Frontier paces and deduplicates tasks, so
	// the target becomes one.
	p := &echo{}
	res, err := HTTP(p).Handle(ctx, get(site.URL+"/same"))
	if err != nil || p.calls != 0 || res.Code != "redirect" || len(res.Tasks) != 1 || res.Tasks[0].URL != site.URL+"/final" {
		t.Fatalf("same-host redirect: %+v %v", res, err)
	}
	if res.Fetch.HTTPStatus != 302 || res.Fetch.FinalURL != site.URL+"/same" {
		t.Fatalf("same-host redirect: fetch %+v", res.Fetch)
	}

	// Unless the spider opts in: followed, and the parser sees the final URL.
	p = &echo{}
	res, err = HTTP(p, WithSameHostRedirects()).Handle(ctx, get(site.URL+"/same"))
	if err != nil || string(p.resp.Body) != "final" || p.resp.URL != site.URL+"/final" || res.Fetch.FinalURL != site.URL+"/final" {
		t.Fatalf("WithSameHostRedirects: %+v %v", p.resp, err)
	}
	// That does not follow a redirect to another host.
	p = &echo{}
	if res, err = HTTP(p, WithSameHostRedirects()).Handle(ctx, get(site.URL+"/away")); err != nil || p.calls != 0 || res.Code != "redirect" {
		t.Fatalf("WithSameHostRedirects to another host: %+v %v", res, err)
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

	// A page that redirects to itself would be dropped as a duplicate of its own task:
	// it fails instead, where it can be seen.
	_, err = HTTP(&echo{}).Handle(ctx, get(site.URL+"/loop"))
	requireFailure(t, err, KindFail, "redirect_loop")
	// When redirects are followed, a loop is a transport error: retried, never parsed.
	_, err = HTTP(&echo{}, WithSameHostRedirects()).Handle(ctx, get(site.URL+"/loop"))
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

// TestHTTPRedirectKeepsRequest: a 307 or 308 repeats the request as it was, so the
// discovered task carries the method and body. The headers go along on the same host
// only: another host gets the ones that describe the content.
func TestHTTPRedirectKeepsRequest(t *testing.T) {
	header := func() http.Header {
		return http.Header{"X-Token": {"abc"}, "Authorization": {"Bearer secret"}, "Cookie": {"sid=1"},
			"Content-Type": {"application/json"}, "Accept": {"application/json"}}
	}
	for _, status := range []int{http.StatusTemporaryRedirect, http.StatusPermanentRedirect} {
		var target string
		site := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			http.Redirect(w, r, target, status)
		}))
		handle := func(to string) NewTask {
			target = to
			res, err := HTTP(&echo{}).Handle(context.Background(),
				&Task{Method: http.MethodPost, URL: site.URL + "/search", Body: []byte(`{"q":1}`), Header: header()})
			if err != nil || len(res.Tasks) != 1 {
				t.Fatalf("%d to %s: %+v %v", status, to, res, err)
			}
			next := res.Tasks[0]
			if next.Method != http.MethodPost || string(next.Body) != `{"q":1}` {
				t.Fatalf("%d to %s: the request was not carried over: %+v", status, to, next)
			}
			return next
		}

		next := handle("/v2/search")
		if next.URL != site.URL+"/v2/search" || len(next.Header) != 5 || next.Header.Get("Authorization") != "Bearer secret" {
			t.Fatalf("%d on the same host: %+v", status, next)
		}
		next = handle("https://other.example/search")
		if next.URL != "https://other.example/search" || len(next.Header) != 2 ||
			next.Header.Get("Content-Type") == "" || next.Header.Get("Accept") != "application/json" {
			t.Fatalf("%d to another host: headers %v, want the content headers only", status, next.Header)
		}
		site.Close()
	}

	// A step down from https to http on the same host does not carry them either.
	from, _ := url.Parse("https://a.example/x")
	to, _ := url.Parse("http://a.example/x")
	if h := redirectHeader(header(), from, to); len(h) != 2 {
		t.Fatalf("https to http: %v", h)
	}
	if h := redirectHeader(header(), to, from); len(h) != 5 {
		t.Fatalf("http to https on the same host: %v", h)
	}
}

// TestHTTPWithStatus: a status handed to a parser has the outcome the parser gives it.
func TestHTTPWithStatus(t *testing.T) {
	site := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		st, _ := strconv.Atoi(r.URL.Path[1:])
		w.WriteHeader(st)
		_, _ = fmt.Fprint(w, "body of ", st)
	}))
	defer site.Close()
	ctx := context.Background()
	main, gone := &echo{}, &echo{}
	h := HTTP(main, WithStatus(gone, http.StatusNotFound, http.StatusGone), WithStatus(Blocked(72*time.Hour), http.StatusForbidden))

	// A 404 is a result here: the parser sees the response and the task succeeds.
	res, err := h.Handle(ctx, get(site.URL+"/404"))
	if err != nil || gone.calls != 1 || main.calls != 0 || string(gone.resp.Body) != "body of 404" || gone.resp.StatusCode != http.StatusNotFound {
		t.Fatalf("404: %+v %v", res, err)
	}
	if res.Code != "http_404" || res.Fetch.HTTPStatus != 404 {
		t.Fatalf("404: code %q, fetch %+v", res.Code, res.Fetch)
	}
	if _, err = h.Handle(ctx, get(site.URL+"/410")); err != nil || gone.calls != 2 {
		t.Fatalf("410: %v", err)
	}
	// A 403 blocks the queue, and the fetch is still recorded.
	res, err = h.Handle(ctx, get(site.URL+"/403"))
	if e := failure(t, err, KindFail, CodeBlocked); e.BlockQueueFor != 72*time.Hour || res.Fetch.HTTPStatus != 403 {
		t.Fatalf("403: %+v, fetch %+v", e, res.Fetch)
	}
	// The other statuses are classified as before.
	if _, err = h.Handle(ctx, get(site.URL+"/200")); err != nil || main.calls != 1 {
		t.Fatalf("200: %v", err)
	}
	_, err = h.Handle(ctx, get(site.URL+"/401"))
	requireFailure(t, err, KindFail, "http_401")
	_, err = h.Handle(ctx, get(site.URL+"/503"))
	requireFailure(t, err, KindRetry, "http_503")
}

// TestHTTPWithRedirect: the spider can adjust the task added for a redirect.
func TestHTTPWithRedirect(t *testing.T) {
	site := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		http.Redirect(w, r, "/final?utm=1", http.StatusFound)
	}))
	defer site.Close()
	h := HTTP(&echo{}, WithRedirect(func(t *Task, r *Response, target *NewTask) {
		if r.StatusCode != http.StatusFound || r.Header.Get("Location") != "/final?utm=1" {
			t.Handler = "wrong response"
		}
		target.DedupKey, target.Handler = "final", t.Handler+"_redirected"
	}))
	task := get(site.URL + "/start")
	task.Handler = "page"
	res, err := h.Handle(context.Background(), task)
	if err != nil || len(res.Tasks) != 1 || res.Tasks[0].DedupKey != "final" || res.Tasks[0].Handler != "page_redirected" ||
		res.Tasks[0].URL != site.URL+"/final?utm=1" {
		t.Fatalf("%+v %v", res, err)
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

func TestHTTPContentTypes(t *testing.T) {
	mux := http.NewServeMux()
	serve := func(path, contentType string) {
		mux.HandleFunc(path, func(w http.ResponseWriter, _ *http.Request) {
			w.Header()["Content-Type"] = nil // no sniffing
			if contentType != "" {
				w.Header().Set("Content-Type", contentType)
			}
			_, _ = fmt.Fprint(w, "body")
		})
	}
	serve("/html", "Text/HTML; charset=utf-8")
	serve("/pdf", "application/pdf")
	serve("/untyped", "")
	mux.HandleFunc("/missing", func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusNotFound)
	})
	site := httptest.NewServer(mux)
	defer site.Close()
	ctx := context.Background()

	p := &echo{}
	h := HTTP(p, WithContentTypes("text/html", "application/xhtml+xml"))
	if _, err := h.Handle(ctx, get(site.URL+"/html")); err != nil || p.calls != 1 {
		t.Fatalf("an accepted type: %d calls, %v", p.calls, err)
	}
	res, err := h.Handle(ctx, get(site.URL+"/pdf"))
	requireFailure(t, err, KindFail, "unsupported_content_type")
	if p.calls != 1 || res.Fetch.HTTPStatus != 200 {
		t.Fatalf("another type: %d calls, fetch %+v", p.calls, res.Fetch)
	}
	if _, err := h.Handle(ctx, get(site.URL+"/untyped")); err != nil || p.calls != 2 {
		t.Fatalf("no Content-Type: %d calls, %v", p.calls, err)
	}
	// The status decides first: a 404 stays a 404.
	_, err = h.Handle(ctx, get(site.URL+"/missing"))
	requireFailure(t, err, KindFail, "http_404")
	// Without the option every type is parsed.
	if _, err := HTTP(p).Handle(ctx, get(site.URL+"/pdf")); err != nil || p.calls != 3 {
		t.Fatalf("the default: %d calls, %v", p.calls, err)
	}
}

// TestHTTPRedirectToOwnURL: a redirect to the task's own URL is a loop only when it
// asks for the request the task just made.
func TestHTTPRedirectToOwnURL(t *testing.T) {
	var status int
	site := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Location", r.URL.Path)
		w.WriteHeader(status)
	}))
	defer site.Close()
	post := func() *Task { return &Task{Method: http.MethodPost, URL: site.URL + "/form", Body: []byte("q=1")} }

	// Post, then redirect to the same URL: the GET is another request.
	status = http.StatusSeeOther
	res, err := HTTP(&echo{}).Handle(context.Background(), post())
	if err != nil || len(res.Tasks) != 1 || res.Tasks[0].URL != site.URL+"/form" || res.Tasks[0].Method != "" || res.Tasks[0].Body != nil {
		t.Fatalf("303 of a POST to its own URL: %+v %v", res, err)
	}
	// A 307 repeats the POST: that is the same request again.
	status = http.StatusTemporaryRedirect
	_, err = HTTP(&echo{}).Handle(context.Background(), post())
	requireFailure(t, err, KindFail, "redirect_loop")
}

// TestHTTPRedirectHeaderIsACopy: the redirect task does not share its headers with
// the task that was redirected, so a hook can change one without the other.
func TestHTTPRedirectHeaderIsACopy(t *testing.T) {
	for _, target := range []string{"/next", "https://other.example/next"} {
		site := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			http.Redirect(w, r, target, http.StatusTemporaryRedirect)
		}))
		task := &Task{Method: http.MethodPost, URL: site.URL, Header: http.Header{"Authorization": {"Bearer a"}, "Accept": {"text/html"}}}
		h := HTTP(&echo{}, WithRedirect(func(_ *Task, _ *Response, next *NewTask) {
			next.Header.Del("Authorization")
			next.Header["Accept"][0] = "changed"
		}))
		_, err := h.Handle(context.Background(), task)
		site.Close()
		if err != nil || task.Header.Get("Authorization") != "Bearer a" || task.Header.Get("Accept") != "text/html" {
			t.Fatalf("to %s: the task's own headers were changed: %v %v", target, task.Header, err)
		}
	}
}

// downgrade answers a request to https://a.example with a redirect to plain http, and
// records what reaches the http side.
type downgrade struct{ plain []*http.Request }

func (d *downgrade) RoundTrip(r *http.Request) (*http.Response, error) {
	res := &http.Response{StatusCode: http.StatusOK, Header: http.Header{}, Body: http.NoBody, Request: r}
	if r.URL.Scheme == "https" {
		res.StatusCode = http.StatusFound
		res.Header.Set("Location", "http://a.example/x")
	} else {
		d.plain = append(d.plain, r)
	}
	return res, nil
}

// TestHTTPNeverFollowsADowngrade: a redirect from https to http is not followed inside
// a task, whatever the spider opted into: the client would send the task's headers in
// the clear. The target becomes a task.
func TestHTTPNeverFollowsADowngrade(t *testing.T) {
	for name, opt := range map[string]HTTPOption{"same host": WithSameHostRedirects(), "cross host": WithCrossHostRedirects()} {
		d := &downgrade{}
		h := HTTP(&echo{}, opt, WithHTTPClient(&http.Client{Transport: d}))
		res, err := h.Handle(context.Background(), &Task{Method: http.MethodGet, URL: "https://a.example/",
			Header: http.Header{"Authorization": {"Bearer secret"}, "X-Api-Key": {"k"}}})
		if err != nil || len(d.plain) != 0 || len(res.Tasks) != 1 || res.Tasks[0].URL != "http://a.example/x" || len(res.Tasks[0].Header) != 0 {
			t.Fatalf("%s: %d plain requests, result %+v, %v", name, len(d.plain), res, err)
		}
	}
}

// TestHTTPLargeBodyOfAnUnparsedStatus: the size of a body nobody reads does not change
// what its status means.
func TestHTTPLargeBodyOfAnUnparsedStatus(t *testing.T) {
	site := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		st, _ := strconv.Atoi(r.URL.Path[1:])
		if st == http.StatusFound {
			w.Header().Set("Location", "/next")
		}
		w.Header().Set("Retry-After", "30")
		w.WriteHeader(st)
		_, _ = w.Write(make([]byte, 4096))
	}))
	defer site.Close()
	ctx := context.Background()
	h := HTTP(&echo{}, WithMaxBodyBytes(1024), WithStatus(&echo{}, http.StatusNotFound))
	_, err := h.Handle(ctx, get(site.URL+"/503"))
	requireFailure(t, err, KindRetry, "http_503")
	_, err = h.Handle(ctx, get(site.URL+"/429"))
	if e := failure(t, err, KindRetry, "http_429"); e.BlockQueueFor != 30*time.Second {
		t.Fatalf("429: the queue block was lost: %+v", e)
	}
	if res, err := h.Handle(ctx, get(site.URL+"/302")); err != nil || len(res.Tasks) != 1 {
		t.Fatalf("302: %+v %v", res, err)
	}
	// A status that goes to a parser is still held to the limit.
	_, err = h.Handle(ctx, get(site.URL+"/404"))
	requireFailure(t, err, KindFail, "body_too_large")
	_, err = h.Handle(ctx, get(site.URL+"/200"))
	requireFailure(t, err, KindFail, "body_too_large")
}

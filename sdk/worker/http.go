package worker

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"maps"
	"mime"
	"net/http"
	"net/url"
	"slices"
	"strconv"
	"strings"
	"time"
)

// Parser is the simple strategy: the SDK fetches the page, the spider parses it. Parse
// is called for 2xx responses; other statuses are classified by the adapter, unless
// WithStatus hands them to a parser.
type Parser interface {
	Parse(ctx context.Context, task *Task, resp *Response) (*Result, error)
}

// ParserFunc adapts a function to a Parser.
type ParserFunc func(ctx context.Context, task *Task, resp *Response) (*Result, error)

// Parse calls f.
func (f ParserFunc) Parse(ctx context.Context, task *Task, resp *Response) (*Result, error) {
	return f(ctx, task, resp)
}

// Response is a fetched page.
type Response struct {
	StatusCode int
	Header     http.Header
	Body       []byte
	URL        string // the URL the body came from: the task's, unless redirects were followed
	Latency    time.Duration
}

// HTTPOption configures the HTTP adapter.
type HTTPOption func(*httpHandler)

// WithHTTPClient sets the client used to fetch. Its redirect policy is replaced.
func WithHTTPClient(c *http.Client) HTTPOption {
	return func(h *httpHandler) { cp := *c; h.client = &cp }
}

// WithUserAgent sets the User-Agent header of requests that carry none.
func WithUserAgent(ua string) HTTPOption { return func(h *httpHandler) { h.userAgent = ua } }

// WithMaxBodyBytes caps the size of a response body. A larger body is a permanent
// failure with code "body_too_large". Default 10 MiB.
func WithMaxBodyBytes(n int64) HTTPOption { return func(h *httpHandler) { h.maxBody = n } }

// WithSameHostRedirects makes the adapter follow redirects that stay on the task's
// host, inside the task. By default it follows none: the Frontier paces and
// deduplicates tasks, so the target of a redirect is added as a discovered task.
//
// Every hop that is followed is a request the Frontier did not pace: on a site that
// redirects each URL, the site sees twice the configured rate. The target is not
// deduplicated either. Use it for sites that need it, such as a redirect that sets a
// cookie and returns to the same URL, together with a client that keeps cookies.
func WithSameHostRedirects() HTTPOption { return func(h *httpHandler) { h.followSameHost = true } }

// WithCrossHostRedirects makes the adapter follow every redirect, also to other hosts.
// What WithSameHostRedirects says applies, and the other host is fetched outside its
// own limits.
func WithCrossHostRedirects() HTTPOption {
	return func(h *httpHandler) { h.followSameHost, h.followCrossHost = true, true }
}

// WithContentTypes limits the pages handed to the parser to the media types listed,
// such as "text/html" and "application/xhtml+xml". A 2xx response of another type is a
// permanent failure with code "unsupported_content_type" and its body is not read. A
// response without a Content-Type is parsed. By default every type is parsed.
func WithContentTypes(types ...string) HTTPOption {
	return func(h *httpHandler) {
		h.contentTypes = make(map[string]bool, len(types))
		for _, t := range types {
			h.contentTypes[strings.ToLower(strings.TrimSpace(t))] = true
		}
	}
}

// WithStatus hands responses with the statuses listed to p instead of classifying them.
// It is how a spider gives a status its own meaning: a 404 that says a product is gone
// is a result, and a 403 that says the crawler was blocked concerns the whole queue.
//
//	worker.HTTP(parser,
//		worker.WithStatus(gone, http.StatusNotFound, http.StatusGone),
//		worker.WithStatus(worker.Blocked(72*time.Hour), http.StatusForbidden))
//
// What p returns is the outcome of the task, as with the main parser: a nil error is a
// success. A status listed twice goes to the parser listed last. A redirect status
// handed to a parser is not turned into a task. WithContentTypes applies to 2xx
// responses only, so p is given a response of any type for another status.
func WithStatus(p Parser, statuses ...int) HTTPOption {
	return func(h *httpHandler) {
		if h.byStatus == nil {
			h.byStatus = map[int]Parser{}
		}
		for _, st := range statuses {
			h.byStatus[st] = p
		}
	}
}

// CodeBlocked is the code of a task that failed because the site blocked the crawler.
const CodeBlocked = "blocked"

// Blocked is a parser for WithStatus, or a call for a parser that recognises a
// challenge page: it fails the task for good with CodeBlocked and asks the Frontier to
// leave the task's queue alone for the cooldown, for every run. The Frontier's policy
// bounds how long one outcome can block a queue.
func Blocked(cooldown time.Duration) Parser {
	return ParserFunc(func(_ context.Context, _ *Task, r *Response) (*Result, error) {
		return nil, Fail(CodeBlocked, fmt.Errorf("status %d", r.StatusCode), BlockQueue(cooldown))
	})
}

// WithRedirect lets the spider adjust the task the adapter adds for a redirect, before
// it is added: set its DedupKey when the spider has its own key scheme, its handler or
// its priority, or put back a header for a host it trusts. t is the task that was
// redirected and r the redirect it was answered with: its status and its headers.
func WithRedirect(f func(t *Task, r *Response, target *NewTask)) HTTPOption {
	return func(h *httpHandler) { h.onRedirect = f }
}

type httpHandler struct {
	parser          Parser
	client          *http.Client
	userAgent       string
	maxBody         int64
	followSameHost  bool
	followCrossHost bool
	contentTypes    map[string]bool // nil: all
	byStatus        map[int]Parser
	onRedirect      func(*Task, *Response, *NewTask)
}

// HTTP adapts a Parser to a Handler. It fetches the task's request and classifies the
// response:
//
//   - a status named by WithStatus: its parser is called;
//   - 2xx: Parse is called;
//   - a redirect: the target is added as a discovered task and the task succeeds with
//     code "redirect"; a 307 or 308 keeps the method and body. It keeps the headers
//     only when the target is on the same host and not a step down from https to http:
//     to another host only the headers that describe the content go along, so that a
//     site cannot redirect a spider's credentials to a host of its choosing. The
//     same host is the same name and port as written: "example.com" and
//     "www.example.com" are two. The body goes along in every case, as it does in
//     browsers and HTTP clients; a spider whose body carries a secret changes the
//     task with WithRedirect. A redirect that asks for the very request the task made
//     would only be dropped as a duplicate, so it is a permanent failure with code
//     "redirect_loop";
//   - 429: retry, and the queue is blocked for the Retry-After delay when the site sent one;
//   - 408 and 5xx: retry;
//   - any other status: permanent failure;
//   - transport errors and timeouts: retry.
//
// Codes are "http_<status>" or "fetch_error".
//
// A redirect target is one level deeper than its task, like any discovered task, so it
// counts against the run's depth limit.
//
// Redirects followed inside a task (WithSameHostRedirects, WithCrossHostRedirects) are
// made by the HTTP client, which removes only the standard credential headers when a
// redirect leaves the host. A step down from https to http is never followed: its
// target becomes a task.
func HTTP(p Parser, opts ...HTTPOption) Handler {
	h := &httpHandler{parser: p, client: &http.Client{}, userAgent: "crawly", maxBody: 10 << 20}
	for _, o := range opts {
		o(h)
	}
	h.client.CheckRedirect = func(req *http.Request, via []*http.Request) error {
		if len(via) >= 10 {
			return errors.New("stopped after 10 redirects")
		}
		// A step down from https to http is never followed inside a task: the client
		// would send the task's headers in the clear. The target becomes a task, which
		// gets the headers a redirect to another host gets.
		down := via[len(via)-1].URL.Scheme == "https" && req.URL.Scheme != "https"
		if !down && (h.followCrossHost || (h.followSameHost && sameHost(req.URL, via[0].URL))) {
			return nil
		}
		return http.ErrUseLastResponse
	}
	return h
}

func sameHost(a, b *url.URL) bool { return strings.EqualFold(a.Host, b.Host) }

func (h *httpHandler) Handle(ctx context.Context, t *Task) (*Result, error) {
	if t.FetchTimeout > 0 {
		var cancel context.CancelFunc
		ctx, cancel = context.WithTimeout(ctx, t.FetchTimeout)
		defer cancel()
	}
	req, err := http.NewRequestWithContext(ctx, t.Method, t.URL, bytes.NewReader(t.Body))
	if err != nil {
		return nil, Fail("bad_request", err)
	}
	maps.Copy(req.Header, t.Header)
	if req.Header.Get("User-Agent") == "" && h.userAgent != "" {
		req.Header.Set("User-Agent", h.userAgent)
	}
	start := time.Now()
	resp, err := h.client.Do(req)
	if err != nil {
		return nil, Retry("fetch_error", err)
	}
	defer func() { _ = resp.Body.Close() }() // the body was read; a close error changes nothing
	if ok := resp.StatusCode >= 200 && resp.StatusCode < 300; ok && !h.accepts(resp.Header.Get("Content-Type")) {
		return &Result{Fetch: &FetchInfo{HTTPStatus: resp.StatusCode, FinalURL: resp.Request.URL.String(), Latency: time.Since(start)}},
			Fail("unsupported_content_type", fmt.Errorf("content type %q is not parsed", resp.Header.Get("Content-Type")))
	}
	// One byte more than the cap is read, to tell a body at the cap from one over it.
	body, err := io.ReadAll(io.LimitReader(resp.Body, h.maxBody+1))
	if err != nil {
		return nil, Retry("fetch_error", err)
	}
	parser := h.byStatus[resp.StatusCode]
	if parser == nil && resp.StatusCode >= 200 && resp.StatusCode < 300 {
		parser = h.parser
	}
	if int64(len(body)) > h.maxBody && parser == nil {
		// Nobody reads this body: its size must not change what the status means, such
		// as a retry for a 503 or the queue block of a 429.
		body = body[:h.maxBody]
	}
	if int64(len(body)) > h.maxBody {
		// A parser must never be given half a page as if it were whole.
		return &Result{Fetch: &FetchInfo{HTTPStatus: resp.StatusCode, FinalURL: resp.Request.URL.String(),
			Latency: time.Since(start), Bytes: h.maxBody}}, Fail("body_too_large", fmt.Errorf("the body is larger than %d bytes", h.maxBody))
	}
	fetch := &FetchInfo{HTTPStatus: resp.StatusCode, FinalURL: resp.Request.URL.String(), Latency: time.Since(start), Bytes: int64(len(body))}
	code := "http_" + strconv.Itoa(resp.StatusCode)
	status := fmt.Errorf("status %d", resp.StatusCode)
	switch {
	case parser != nil:
		res, err := parser.Parse(ctx, t, &Response{StatusCode: resp.StatusCode, Header: resp.Header, Body: body,
			URL: fetch.FinalURL, Latency: fetch.Latency})
		if res == nil {
			res = &Result{}
		}
		if res.Fetch == nil {
			res.Fetch = fetch
		}
		if res.Code == "" && err == nil {
			res.Code = code
		}
		return res, err
	case resp.StatusCode >= 300 && resp.StatusCode < 400:
		loc, err := resp.Location()
		if err != nil {
			return &Result{Fetch: fetch}, Fail(code, fmt.Errorf("redirect without a usable Location: %w", err))
		}
		// 307 and 308 repeat the request; the other redirects ask for a GET.
		repeats := resp.StatusCode == http.StatusTemporaryRedirect || resp.StatusCode == http.StatusPermanentRedirect
		plainGet := t.Method == http.MethodGet && len(t.Body) == 0
		if sameURL(loc, resp.Request.URL) && (repeats || plainGet) {
			// The target is the request the task just made. A POST that is answered
			// with a 303 to its own URL is not: the GET it asks for is another request.
			return &Result{Fetch: fetch}, Fail("redirect_loop", errors.New("the page redirects to itself"))
		}
		next := NewTask{URL: loc.String(), Context: t.Context, Label: t.Label}
		if repeats {
			// 307 and 308 repeat the request as it was.
			next.Method, next.Body, next.Header = t.Method, t.Body, redirectHeader(t.Header, resp.Request.URL, loc)
		}
		if h.onRedirect != nil {
			h.onRedirect(t, &Response{StatusCode: resp.StatusCode, Header: resp.Header, Body: body,
				URL: fetch.FinalURL, Latency: fetch.Latency}, &next)
		}
		return &Result{Code: "redirect", Fetch: fetch, Tasks: []NewTask{next}}, nil
	case resp.StatusCode == http.StatusTooManyRequests:
		var opts []ErrorOption
		if d := retryAfter(resp.Header.Get("Retry-After")); d > 0 {
			opts = append(opts, BlockQueue(d))
		}
		return &Result{Fetch: fetch}, Retry(code, status, opts...)
	case resp.StatusCode == http.StatusRequestTimeout || resp.StatusCode >= 500:
		return &Result{Fetch: fetch}, Retry(code, status)
	default:
		return &Result{Fetch: fetch}, Fail(code, status)
	}
}

// contentHeaders are the request headers that describe the request's content and
// what the client accepts. They say nothing about who the client is.
var contentHeaders = map[string]bool{"Accept": true, "Accept-Language": true, "Content-Language": true,
	"Content-Type": true, "User-Agent": true}

// redirectHeader is what a repeated request carries to the target of its redirect:
// everything on the same host, and only the content headers when the redirect leaves
// the host or steps down from https to http. A spider's credentials are in headers of
// its own naming as often as in Authorization or Cookie, so nothing else is let through.
func redirectHeader(h http.Header, from, to *url.URL) http.Header {
	// A copy: the task for the target must not share its headers with the task that
	// was redirected, which a WithRedirect hook sees side by side.
	if sameHost(from, to) && (from.Scheme != "https" || to.Scheme == "https") {
		return h.Clone()
	}
	out := http.Header{}
	for name, values := range h {
		if contentHeaders[http.CanonicalHeaderKey(name)] {
			out[name] = slices.Clone(values)
		}
	}
	return out
}

// accepts reports whether a body of this Content-Type goes to the parser.
func (h *httpHandler) accepts(contentType string) bool {
	if h.contentTypes == nil || contentType == "" {
		return true
	}
	media, _, err := mime.ParseMediaType(contentType)
	return err == nil && h.contentTypes[media]
}

// sameURL reports whether two URLs name the same request: the fragment never reaches
// the server.
func sameURL(a, b *url.URL) bool {
	x, y := *a, *b
	x.Fragment, x.RawFragment, y.Fragment, y.RawFragment = "", "", "", ""
	return x.String() == y.String()
}

// maxRetryAfter bounds what a site's Retry-After header can ask for.
const maxRetryAfter = 24 * time.Hour

// retryAfter parses a Retry-After header: seconds or an HTTP date. The result is
// between zero and maxRetryAfter.
func retryAfter(v string) time.Duration {
	if v == "" {
		return 0
	}
	if secs, err := strconv.ParseInt(strings.TrimSpace(v), 10, 64); err == nil {
		if secs > int64(maxRetryAfter/time.Second) {
			return maxRetryAfter
		}
		return max(time.Duration(secs)*time.Second, 0)
	}
	if at, err := http.ParseTime(v); err == nil {
		return min(max(time.Until(at), 0), maxRetryAfter)
	}
	return 0
}

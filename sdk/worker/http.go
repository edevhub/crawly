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
	"strconv"
	"strings"
	"time"
)

// Parser is the simple strategy: the SDK fetches the page, the spider parses it. Parse
// is called only for 2xx responses; other statuses are classified by the adapter.
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

type httpHandler struct {
	parser          Parser
	client          *http.Client
	userAgent       string
	maxBody         int64
	followSameHost  bool
	followCrossHost bool
	contentTypes    map[string]bool // nil: all
}

// HTTP adapts a Parser to a Handler. It fetches the task's request and classifies the
// response:
//
//   - 2xx: Parse is called;
//   - a redirect: the target is added as a discovered task and the task succeeds with
//     code "redirect"; a 307 or 308 keeps the method, body and headers. A redirect to
//     the task's own URL would only be dropped as a duplicate, so it is a permanent
//     failure with code "redirect_loop";
//   - 429: retry, and the queue is blocked for the Retry-After delay when the site sent one;
//   - 408 and 5xx: retry;
//   - any other status: permanent failure;
//   - transport errors and timeouts: retry.
//
// Codes are "http_<status>" or "fetch_error".
//
// A redirect target is one level deeper than its task, like any discovered task, so it
// counts against the run's depth limit.
func HTTP(p Parser, opts ...HTTPOption) Handler {
	h := &httpHandler{parser: p, client: &http.Client{}, userAgent: "crawly", maxBody: 10 << 20}
	for _, o := range opts {
		o(h)
	}
	h.client.CheckRedirect = func(req *http.Request, via []*http.Request) error {
		if len(via) >= 10 {
			return errors.New("stopped after 10 redirects")
		}
		if h.followCrossHost || (h.followSameHost && sameHost(req.URL, via[0].URL)) {
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
	if int64(len(body)) > h.maxBody {
		// A parser must never be given half a page as if it were whole.
		return &Result{Fetch: &FetchInfo{HTTPStatus: resp.StatusCode, FinalURL: resp.Request.URL.String(),
			Latency: time.Since(start), Bytes: h.maxBody}}, Fail("body_too_large", fmt.Errorf("the body is larger than %d bytes", h.maxBody))
	}
	fetch := &FetchInfo{HTTPStatus: resp.StatusCode, FinalURL: resp.Request.URL.String(), Latency: time.Since(start), Bytes: int64(len(body))}
	code := "http_" + strconv.Itoa(resp.StatusCode)
	status := fmt.Errorf("status %d", resp.StatusCode)
	switch {
	case resp.StatusCode >= 200 && resp.StatusCode < 300:
		res, err := h.parser.Parse(ctx, t, &Response{StatusCode: resp.StatusCode, Header: resp.Header, Body: body,
			URL: fetch.FinalURL, Latency: fetch.Latency})
		if res == nil {
			res = &Result{}
		}
		if res.Fetch == nil {
			res.Fetch = fetch
		}
		if res.Code == "" {
			res.Code = code
		}
		return res, err
	case resp.StatusCode >= 300 && resp.StatusCode < 400:
		loc, err := resp.Location()
		if err != nil {
			return &Result{Fetch: fetch}, Fail(code, fmt.Errorf("redirect without a usable Location: %w", err))
		}
		if sameURL(loc, resp.Request.URL) {
			return &Result{Fetch: fetch}, Fail("redirect_loop", errors.New("the page redirects to itself"))
		}
		next := NewTask{URL: loc.String(), Context: t.Context}
		if resp.StatusCode == http.StatusTemporaryRedirect || resp.StatusCode == http.StatusPermanentRedirect {
			// 307 and 308 repeat the request as it was.
			next.Method, next.Body, next.Header = t.Method, t.Body, t.Header
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

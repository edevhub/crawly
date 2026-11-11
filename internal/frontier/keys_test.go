package frontier

import (
	"testing"

	taskv1 "github.com/edevhub/crawly/internal/gen/frontier/task/v1"
	"github.com/edevhub/crawly/internal/store"
)

func get(url string) *taskv1.Request { return &taskv1.Request{Url: url} }

func TestDedupKey(t *testing.T) {
	for in, want := range map[string]string{
		// Scheme and host are case-insensitive; path and query are not.
		"HTTP://Example.COM/Path?b=2&a=1#frag":    "http://example.com/Path?a=1&b=2",
		"https://example.com/watch?v=dQw4w9WgXcQ": "https://example.com/watch?v=dQw4w9WgXcQ",
		"https://example.com:443/":                "https://example.com/",
		"https://example.com:8443/x?z=1&Z=0":      "https://example.com:8443/x?Z=0&z=1",
		"http://example.com":                      "http://example.com/",
	} {
		got, err := DedupKey(get(in))
		if err != nil || got != want {
			t.Errorf("DedupKey(%q) = %q, %v; want %q", in, got, err, want)
		}
	}
	a, _ := DedupKey(get("https://example.com/p/aB3x"))
	b, _ := DedupKey(get("https://example.com/p/Ab3X"))
	if a == b {
		t.Error("paths that differ in case must not be one task")
	}
	if _, err := DedupKey(get("/relative")); err == nil {
		t.Error("a relative URL must be rejected")
	}
}

// TestDedupKeyOfRequests: a GET is its URL; other methods are told apart by method
// and body, so the pages of an API behind one URL are separate tasks.
func TestDedupKeyOfRequests(t *testing.T) {
	const url = "https://example.com/api/search"
	key := func(method, body string) string {
		k, err := DedupKey(&taskv1.Request{Url: url, Method: method, Body: []byte(body)})
		if err != nil {
			t.Fatal(err)
		}
		return k
	}
	if key("", "") != url || key("GET", "") != url || key("get", "ignored") != url {
		t.Errorf("a GET must keep the URL as its key: %q %q %q", key("", ""), key("GET", ""), key("get", "ignored"))
	}
	if key("POST", "") != "POST "+url || key("post", "") != key("POST", "") {
		t.Errorf("POST without a body: %q", key("POST", ""))
	}
	page1, page2 := key("POST", `{"page":1}`), key("POST", `{"page":2}`)
	if page1 == page2 || page1 == key("POST", "") || page1 != key("POST", `{"page":1}`) {
		t.Errorf("bodies must tell requests apart: %q %q", page1, page2)
	}
	if key("PUT", `{"page":1}`) == page1 || key("HEAD", "") == url {
		t.Error("the method must tell requests apart")
	}
	if _, err := DedupKey(&taskv1.Request{Url: "/relative", Method: "POST"}); err == nil {
		t.Error("a relative URL must be rejected")
	}
}

func TestQueueKey(t *testing.T) {
	cases := []struct {
		rule          store.KeyRule
		explicit, url string
		want          string
		fail          bool
	}{
		{store.KeyHost, "", "https://WWW.Example.com/a", "www.example.com", false},
		{store.KeyHost, "", "http://example.com:8080/a", "example.com:8080", false},
		{store.KeyRegisteredDomain, "", "https://a.b.example.co.uk/", "example.co.uk", false},
		{store.KeyRegisteredDomain, "", "http://localhost:9000/", "localhost:9000", false},
		{store.KeyHost, "custom", "https://example.com/", "custom", false},
		{store.KeyExplicit, "", "https://example.com/", "", true},
	}
	for _, c := range cases {
		got, err := QueueKey(c.rule, c.explicit, c.url)
		if (err != nil) != c.fail || got != c.want {
			t.Errorf("QueueKey(%v, %q, %q) = %q, %v", c.rule, c.explicit, c.url, got, err)
		}
	}
}

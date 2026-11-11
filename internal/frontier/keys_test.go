package frontier

import (
	"testing"

	"github.com/edevhub/crawly/internal/store"
)

func TestDedupKey(t *testing.T) {
	for in, want := range map[string]string{
		// Scheme and host are case-insensitive; path and query are not.
		"HTTP://Example.COM/Path?b=2&a=1#frag":    "http://example.com/Path?a=1&b=2",
		"https://example.com/watch?v=dQw4w9WgXcQ": "https://example.com/watch?v=dQw4w9WgXcQ",
		"https://example.com:443/":                "https://example.com/",
		"https://example.com:8443/x?z=1&Z=0":      "https://example.com:8443/x?Z=0&z=1",
		"http://example.com":                      "http://example.com/",
	} {
		got, err := DedupKey(in)
		if err != nil || got != want {
			t.Errorf("DedupKey(%q) = %q, %v; want %q", in, got, err, want)
		}
	}
	a, _ := DedupKey("https://example.com/p/aB3x")
	b, _ := DedupKey("https://example.com/p/Ab3X")
	if a == b {
		t.Error("paths that differ in case must not be one task")
	}
	if _, err := DedupKey("/relative"); err == nil {
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

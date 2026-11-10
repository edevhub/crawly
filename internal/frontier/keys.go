package frontier

import (
	"crypto/sha256"
	"errors"
	"fmt"
	"net/url"
	"slices"
	"strings"

	"golang.org/x/net/publicsuffix"

	taskv1 "github.com/edevhub/crawly/internal/gen/frontier/task/v1"
	"github.com/edevhub/crawly/internal/poolname"
	"github.com/edevhub/crawly/internal/store"
)

// DedupKey is the default normalisation of a request URL (ADR 0001, Dedup): scheme +
// host + path + query parameters sorted ascending, all lowercased. The host keeps an
// explicit non-default port. Fragment, method, headers and body are not part of it.
func DedupKey(rawURL string) (string, error) {
	u, err := parseURL(rawURL)
	if err != nil {
		return "", err
	}
	var b strings.Builder
	b.WriteString(u.Scheme + "://" + hostPort(u) + u.EscapedPath())
	if q := u.Query(); len(q) > 0 {
		pairs := make([]string, 0, len(q))
		for k, vs := range q {
			for _, v := range vs {
				pairs = append(pairs, url.QueryEscape(k)+"="+url.QueryEscape(v))
			}
		}
		slices.Sort(pairs)
		b.WriteString("?" + strings.Join(pairs, "&"))
	}
	return strings.ToLower(b.String()), nil
}

// QueueKey derives a task's queue from the run's key rule.
func QueueKey(rule store.KeyRule, explicit, rawURL string) (string, error) {
	if explicit != "" {
		return explicit, nil
	}
	if rule == store.KeyExplicit {
		return "", errors.New("queue_key is required by the run's key rule")
	}
	u, err := parseURL(rawURL)
	if err != nil {
		return "", err
	}
	if rule == store.KeyRegisteredDomain {
		if d, err := publicsuffix.EffectiveTLDPlusOne(strings.ToLower(u.Hostname())); err == nil {
			return d, nil
		}
	}
	return hostPort(u), nil
}

func parseURL(raw string) (*url.URL, error) {
	u, err := url.Parse(raw)
	if err != nil {
		return nil, err
	}
	if u.Scheme == "" || u.Host == "" {
		return nil, fmt.Errorf("url %q is not absolute", raw)
	}
	u.Scheme = strings.ToLower(u.Scheme)
	return u, nil
}

// hostPort is the lowercased host, with the port only when it is not the scheme's default.
func hostPort(u *url.URL) string {
	host, port := strings.ToLower(u.Hostname()), u.Port()
	if port == "" || (u.Scheme == "http" && port == "80") || (u.Scheme == "https" && port == "443") {
		return host
	}
	return host + ":" + port
}

// fingerprint is the dedup identity of a task inside its run: it includes the queue
// key, so dedup is per run and queue.
func fingerprint(queue, dedup string) store.Fingerprint {
	sum := sha256.Sum256([]byte(queue + "\x00" + dedup))
	var fp store.Fingerprint
	copy(fp[:], sum[:])
	return fp
}

// identify computes pool, queue key and fingerprint of a submitted task.
func identify(rule store.KeyRule, ts *taskv1.TaskSpec) (pool store.PoolName, queue string, fp store.Fingerprint, err error) {
	pool = store.PoolName(ts.GetPool())
	if pool == "" {
		pool = store.DefaultPool
	}
	if !poolname.Valid(string(pool)) {
		return "", "", fp, fmt.Errorf("pool %q does not match [a-z0-9_]{1,32}", pool)
	}
	rawURL := ts.GetRequest().GetUrl()
	if queue, err = QueueKey(rule, ts.GetQueueKey(), rawURL); err != nil {
		return "", "", fp, err
	}
	dedup := ts.GetDedupKey()
	if dedup == "" {
		if dedup, err = DedupKey(rawURL); err != nil {
			return "", "", fp, err
		}
	}
	return pool, queue, fingerprint(queue, dedup), nil
}

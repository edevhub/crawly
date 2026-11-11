package main

import (
	"context"
	"io"
	"log/slog"
	"net"
	"net/http"
	"strings"
	"testing"

	"github.com/edevhub/crawly/internal/frontier"
	"github.com/edevhub/crawly/internal/policy"
	"github.com/edevhub/crawly/internal/store/memstore"
)

func TestServeMetricsRejectsABadAddress(t *testing.T) {
	if _, err := serveMetrics("256.0.0.1:1", newRegistry(), slog.New(slog.DiscardHandler)); err == nil {
		t.Error("a bad address must fail the start")
	}
}

// freeAddr is a loopback address nothing listens on.
func freeAddr(t *testing.T) string {
	t.Helper()
	lis, err := (&net.ListenConfig{}).Listen(context.Background(), "tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	addr := lis.Addr().String()
	if err := lis.Close(); err != nil {
		t.Fatal(err)
	}
	return addr
}

func TestMetricsEndpoint(t *testing.T) {
	pol, _ := policy.New(nil)
	reg := newRegistry()
	_, err := frontier.New(frontier.Config{Store: memstore.New(pol), Policies: pol, Metrics: reg})
	if err != nil {
		t.Fatal(err)
	}
	addr := freeAddr(t)
	stop, err := serveMetrics(addr, reg, slog.New(slog.DiscardHandler))
	if err != nil {
		t.Fatal(err)
	}
	defer stop()
	req, _ := http.NewRequestWithContext(context.Background(), http.MethodGet, "http://"+addr+"/metrics", http.NoBody)
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	body, _ := io.ReadAll(resp.Body)
	if err := resp.Body.Close(); err != nil {
		t.Fatal(err)
	}
	for _, want := range []string{"crawly_frontier_replica_info", "go_goroutines", "process_"} {
		if !strings.Contains(string(body), want) {
			t.Errorf("/metrics lacks %q", want)
		}
	}
	other, _ := http.NewRequestWithContext(context.Background(), http.MethodGet, "http://"+addr+"/", http.NoBody)
	r2, err := http.DefaultClient.Do(other)
	if err != nil {
		t.Fatal(err)
	}
	if err := r2.Body.Close(); err != nil {
		t.Fatal(err)
	}
	if r2.StatusCode != http.StatusNotFound {
		t.Errorf("only /metrics is served, / answered %d", r2.StatusCode)
	}
}

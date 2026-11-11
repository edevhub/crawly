package main

import (
	"context"
	"errors"
	"log/slog"
	"net"
	"net/http"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/collectors"
	"github.com/prometheus/client_golang/prometheus/promhttp"
)

// newRegistry is the registry of a replica: the Frontier's metrics, and those of the Go
// runtime and the process.
func newRegistry() *prometheus.Registry {
	reg := prometheus.NewRegistry()
	reg.MustRegister(collectors.NewGoCollector(), collectors.NewProcessCollector(collectors.ProcessCollectorOpts{}))
	return reg
}

// serveMetrics serves /metrics of the registry on addr, in plain HTTP, until stop is
// called. It returns once the address is bound, so that a bad address fails the start.
func serveMetrics(addr string, reg *prometheus.Registry, log *slog.Logger) (stop func(), err error) {
	lis, err := (&net.ListenConfig{}).Listen(context.Background(), "tcp", addr)
	if err != nil {
		return nil, err
	}
	mux := http.NewServeMux()
	mux.Handle("/metrics", promhttp.HandlerFor(reg, promhttp.HandlerOpts{
		ErrorLog:      slog.NewLogLogger(log.Handler(), slog.LevelWarn),
		ErrorHandling: promhttp.ContinueOnError, // one faulty collector must not drop every series
		Registry:      reg,                      // exports promhttp_metric_handler_errors_total
	}))
	srv := &http.Server{Handler: mux, ReadHeaderTimeout: 10 * time.Second}
	done := make(chan struct{})
	go func() {
		defer close(done)
		if err := srv.Serve(lis); err != nil && !errors.Is(err, http.ErrServerClosed) {
			log.Error("metrics listener", "err", err)
		}
	}()
	log.Info("metrics listening", "addr", lis.Addr().String())
	return func() {
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		_ = srv.Shutdown(ctx) // a scrape that outlasts the grace is cut
		<-done
	}, nil
}

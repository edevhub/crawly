// Command crawly-frontier runs a Frontier replica.
//
//	crawly-frontier -listen :7070 -policies policies.json -postgres postgres://user@host/db
//
// With -postgres (or the CRAWLY_POSTGRES environment variable) the state is kept in
// PostgreSQL: it survives restarts and several replicas can share one database.
// Without it the store is in memory: the state is lost on restart, which suits
// development and trials. Policies are static: change the file and restart.
package main

import (
	"context"
	"flag"
	"log/slog"
	"net"
	"os"
	"os/signal"
	"syscall"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/reflection"

	"github.com/edevhub/crawly/internal/frontier"
	"github.com/edevhub/crawly/internal/policy"
	"github.com/edevhub/crawly/internal/store"
	"github.com/edevhub/crawly/internal/store/memstore"
	"github.com/edevhub/crawly/internal/store/pgstore"
)

// Environment variables that stand in for the flags of the same name, so that
// credentials need not be on the command line.
const (
	envPostgres       = "CRAWLY_POSTGRES"
	envPostgresListen = "CRAWLY_POSTGRES_LISTEN"
)

type config struct {
	listen, policies         string
	postgres, postgresListen string
}

func main() {
	var cfg config
	flag.StringVar(&cfg.listen, "listen", ":7070", "gRPC listen address")
	flag.StringVar(&cfg.policies, "policies", "", `policy file: {"policies": [...]} in proto JSON; empty uses the built-in defaults`)
	flag.StringVar(&cfg.postgres, "postgres", os.Getenv(envPostgres),
		"PostgreSQL connection URL for the store (or $"+envPostgres+"); empty keeps the state in memory")
	flag.StringVar(&cfg.postgresListen, "postgres-listen", os.Getenv(envPostgresListen),
		"direct PostgreSQL connection URL for LISTEN, when -postgres goes through a transaction pooler (or $"+envPostgresListen+")")
	flag.Parse()
	log := slog.New(slog.NewTextHandler(os.Stderr, nil))
	if err := run(cfg, log); err != nil {
		log.Error("frontier stopped", "err", err)
		os.Exit(1)
	}
}

// openStore opens the store the configuration names and returns how to close it.
func openStore(ctx context.Context, cfg config, pol store.Policies, log *slog.Logger) (store.Store, func(), error) {
	if cfg.postgres == "" {
		log.Warn("the store is in memory: the state is lost on restart")
		return memstore.New(pol), func() {}, nil
	}
	opts := []pgstore.Option{pgstore.WithLogger(log)}
	if cfg.postgresListen != "" {
		opts = append(opts, pgstore.WithListenDSN(cfg.postgresListen))
	}
	st, err := pgstore.New(ctx, cfg.postgres, pol, opts...)
	if err != nil {
		return nil, nil, err
	}
	return st, st.Close, nil
}

func run(cfg config, log *slog.Logger) error {
	pol, err := policy.New(nil)
	if cfg.policies != "" {
		pol, err = policy.Load(cfg.policies)
	}
	if err != nil {
		return err
	}
	st, closeStore, err := openStore(context.Background(), cfg, pol, log)
	if err != nil {
		return err
	}
	defer closeStore()
	srv, err := frontier.New(frontier.Config{Store: st, Policies: pol, Logger: log})
	if err != nil {
		return err
	}
	lis, err := (&net.ListenConfig{}).Listen(context.Background(), "tcp", cfg.listen)
	if err != nil {
		return err
	}
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	g := grpc.NewServer(frontier.ServerOptions()...)
	srv.Register(g)
	reflection.Register(g)
	// The background loops outlive the signal: they keep serving the streams that are
	// still closing during the shutdown.
	loops, stopLoops := context.WithCancel(context.Background())
	defer stopLoops()
	go func() {
		if err := srv.Run(loops); err != nil {
			log.Error("background loops", "err", err)
			stop()
		}
	}()
	go func() {
		<-ctx.Done()
		log.Info("shutting down")
		grace, cancel := context.WithTimeout(context.WithoutCancel(ctx), shutdownGrace)
		defer cancel()
		srv.Shutdown(grace, g)
	}()
	log.Info("frontier listening", "addr", lis.Addr().String(), "policy_set", pol.Hash(), "policies", len(pol.Policies()))
	return g.Serve(lis)
}

// shutdownGrace is how long connected workers and watchers get to hang up.
const shutdownGrace = 15 * time.Second

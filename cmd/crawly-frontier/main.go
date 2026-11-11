// Command crawly-frontier runs a Frontier replica.
//
//	crawly-frontier -listen :7070 -policies policies.json -postgres postgres://user@host/db
//
// With -postgres (or the CRAWLY_POSTGRES environment variable) the state is kept in
// PostgreSQL: it survives restarts and several replicas can share one database.
// Without it the store is in memory: the state is lost on restart, which suits
// development and trials. Policies are static: change the file and restart.
//
// The Frontier does not change the database schema. Apply the migrations before a new
// version starts:
//
//	crawly-frontier migrate -postgres postgres://user@host/db
//	crawly-frontier migrate -postgres postgres://user@host/db status
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
	envPostgres           = "CRAWLY_POSTGRES"
	envPostgresListen     = "CRAWLY_POSTGRES_LISTEN"
	envPostgresMigrations = "CRAWLY_POSTGRES_MIGRATIONS_TABLE"
)

type config struct {
	listen, policies         string
	metricsAddr              string
	postgres, postgresListen string
	migrationsTable          string
	tlsCert, tlsKey          string
	tlsClientCA              string
	retainRuns, retainTasks  time.Duration
	noHandlerGrace           time.Duration
	maxLabels                int
}

// migrationsTableFlag is shared by the server, which reads the table, and the migrate
// command, which writes it.
func migrationsTableFlag(fs *flag.FlagSet, into *string) {
	def := os.Getenv(envPostgresMigrations)
	if def == "" {
		def = pgstore.DefaultMigrationsTable
	}
	fs.StringVar(into, "postgres-migrations-table", def,
		"table that records the applied migrations, optionally schema.table (or $"+envPostgresMigrations+")")
}

func main() {
	log := slog.New(slog.NewTextHandler(os.Stderr, nil))
	if len(os.Args) > 1 && os.Args[1] == "migrate" {
		if err := migrate(context.Background(), os.Args[2:], os.Stdout, log); err != nil {
			log.Error("migrate failed", "err", err)
			os.Exit(1)
		}
		return
	}
	var cfg config
	migrationsTableFlag(flag.CommandLine, &cfg.migrationsTable)
	flag.StringVar(&cfg.listen, "listen", ":7070", "gRPC listen address")
	flag.StringVar(&cfg.metricsAddr, "metrics-addr", "",
		"address to serve Prometheus metrics on, at /metrics, in plain HTTP (e.g. :9090); empty serves none")
	flag.StringVar(&cfg.policies, "policies", "", `policy file: {"policies": [...]} in proto JSON; empty uses the built-in defaults`)
	flag.StringVar(&cfg.postgres, "postgres", os.Getenv(envPostgres),
		"PostgreSQL connection URL for the store (or $"+envPostgres+"); empty keeps the state in memory")
	flag.StringVar(&cfg.postgresListen, "postgres-listen", os.Getenv(envPostgresListen),
		"direct PostgreSQL connection URL for LISTEN, when -postgres goes through a transaction pooler (or $"+envPostgresListen+")")
	flag.StringVar(&cfg.tlsCert, "tls-cert", "", "certificate file (PEM) of the server; with -tls-key it turns TLS on")
	flag.StringVar(&cfg.tlsKey, "tls-key", "", "private key file (PEM) of the server")
	flag.StringVar(&cfg.tlsClientCA, "tls-client-ca", "",
		"CA file (PEM): when set, every client must present a certificate signed by it (mutual TLS)")
	flag.DurationVar(&cfg.retainRuns, "retain-runs", 0,
		"how long an ended run is kept before it is deleted with its statistics; 0 keeps runs for ever")
	flag.DurationVar(&cfg.retainTasks, "retain-tasks", 0,
		"PostgreSQL only: how long tasks and attempts are kept, counted from the creation of their run; 0 keeps them for ever")
	flag.DurationVar(&cfg.noHandlerGrace, "no-handler-grace", time.Hour,
		`how long a task waits while no worker session has its handler, e.g. during a rolling deploy, before it fails with code "no_handler"`)
	flag.IntVar(&cfg.maxLabels, "max-labels-per-run", frontier.DefaultMaxLabelsPerRun,
		"how many distinct task labels a run accepts; a task that would bring one more is invalid")
	flag.Parse()
	if err := run(cfg, log); err != nil {
		log.Error("frontier stopped", "err", err)
		os.Exit(1)
	}
}

// openStore opens the store the configuration names and returns how to close it.
func openStore(ctx context.Context, cfg config, pol store.Policies, log *slog.Logger) (store.Store, func(), error) {
	if cfg.postgres == "" {
		log.Warn("the store is in memory: the state is lost on restart")
		return memstore.New(pol, memstore.WithRetention(cfg.retainRuns), memstore.WithLogger(log)), func() {}, nil
	}
	opts := []pgstore.Option{pgstore.WithLogger(log), pgstore.WithMigrationsTable(cfg.migrationsTable),
		pgstore.WithRetention(cfg.retainTasks, cfg.retainRuns)}
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
	reg := newRegistry()
	srv, err := frontier.New(frontier.Config{Store: st, Policies: pol, Logger: log, NoHandlerGrace: cfg.noHandlerGrace,
		Metrics: reg, MaxLabelsPerRun: cfg.maxLabels})
	if err != nil {
		return err
	}
	if cfg.metricsAddr != "" {
		stopMetrics, err := serveMetrics(cfg.metricsAddr, reg, log)
		if err != nil {
			return err
		}
		defer stopMetrics()
	}
	lis, err := (&net.ListenConfig{}).Listen(context.Background(), "tcp", cfg.listen)
	if err != nil {
		return err
	}
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	opts := append(frontier.ServerOptions(), srv.MetricsOptions()...)
	creds, err := transportCredentials(cfg)
	if err != nil {
		return err
	}
	if creds != nil {
		opts = append(opts, grpc.Creds(creds))
	} else {
		log.Warn("TLS is off: anyone who can reach the port can use the API")
	}
	g := grpc.NewServer(opts...)
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
	log.Info("frontier listening", "addr", lis.Addr().String(), "policy_set", pol.Hash(), "policies", len(pol.Policies()),
		"tls", creds != nil, "client_certificates", cfg.tlsClientCA != "")
	return g.Serve(lis)
}

// shutdownGrace is how long connected workers and watchers get to hang up.
const shutdownGrace = 15 * time.Second

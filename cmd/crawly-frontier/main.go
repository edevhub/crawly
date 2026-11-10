// Command crawly-frontier runs a Frontier replica with the in-memory store.
//
//	crawly-frontier -listen :7070 -policies policies.json
//
// The in-memory store loses its state on restart; it is meant for development and
// trials. Policies are static: change the file and restart.
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
	"github.com/edevhub/crawly/internal/store/memstore"
)

func main() {
	listen := flag.String("listen", ":7070", "gRPC listen address")
	policies := flag.String("policies", "", `policy file: {"policies": [...]} in proto JSON; empty uses the built-in defaults`)
	flag.Parse()
	log := slog.New(slog.NewTextHandler(os.Stderr, nil))
	if err := run(*listen, *policies, log); err != nil {
		log.Error("frontier stopped", "err", err)
		os.Exit(1)
	}
}

func run(listen, policyFile string, log *slog.Logger) error {
	pol, err := policy.New(nil)
	if policyFile != "" {
		pol, err = policy.Load(policyFile)
	}
	if err != nil {
		return err
	}
	srv, err := frontier.New(frontier.Config{Store: memstore.New(pol), Policies: pol, Logger: log})
	if err != nil {
		return err
	}
	lis, err := (&net.ListenConfig{}).Listen(context.Background(), "tcp", listen)
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

package main

import (
	"context"
	"flag"
	"fmt"
	"os"
	"os/signal"
	"syscall"

	"github.com/y-shashank/kafka-batch/go/pkg/config"
	"github.com/y-shashank/kafka-batch/go/pkg/daemon"
	"github.com/y-shashank/kafka-batch/go/pkg/kafkaclient"
	"github.com/y-shashank/kafka-batch/go/pkg/kbatch"
	"github.com/y-shashank/kafka-batch/go/pkg/reconciler"
	"github.com/y-shashank/kafka-batch/go/pkg/store"
	"github.com/y-shashank/kafka-batch/go/pkg/worker"

	"github.com/redis/go-redis/v9"
)

func main() {
	if len(os.Args) < 2 {
		usage()
		os.Exit(2)
	}

	switch os.Args[1] {
	case "serve":
		serve(os.Args[2:])
	case "daemon":
		runDaemon(os.Args[2:])
	case "worker":
		runWorker(os.Args[2:])
	case "reconcile":
		runReconcile(os.Args[2:])
	case "help", "-h", "--help":
		usage()
	default:
		fmt.Fprintf(os.Stderr, "unknown command %q\n\n", os.Args[1])
		usage()
		os.Exit(2)
	}
}

func serve(args []string) {
	fs := flag.NewFlagSet("serve", flag.ExitOnError)
	socket := fs.String("socket", "/tmp/kbatch.sock", "Unix socket path for HTTP API")
	_ = fs.Parse(args)

	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	srv := kbatch.NewServer(kbatch.ServerConfig{SocketPath: *socket})
	fmt.Printf("kbatch serve listening on %s\n", *socket)
	if err := srv.ListenAndServe(ctx); err != nil {
		fmt.Fprintf(os.Stderr, "kbatch serve: %v\n", err)
		os.Exit(1)
	}
}

func runDaemon(args []string) {
	fs := flag.NewFlagSet("daemon", flag.ExitOnError)
	cfg := fs.String("config", "", "daemon config YAML path")
	manifest := fs.String("manifest", "", "handler manifest YAML path")
	_ = fs.Parse(args)
	if *cfg == "" {
		fmt.Fprintln(os.Stderr, "daemon requires --config")
		os.Exit(2)
	}
	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()
	if err := daemon.Run(ctx, *cfg, *manifest); err != nil {
		fmt.Fprintf(os.Stderr, "kbatch daemon: %v\n", err)
		os.Exit(1)
	}
}

func runWorker(args []string) {
	fs := flag.NewFlagSet("worker", flag.ExitOnError)
	cfg := fs.String("config", "", "daemon config YAML path")
	manifest := fs.String("manifest", "", "handler manifest YAML path")
	_ = fs.Parse(args)
	if *cfg == "" {
		fmt.Fprintln(os.Stderr, "worker requires --config")
		os.Exit(2)
	}
	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()
	if err := worker.Run(ctx, *cfg, *manifest); err != nil {
		fmt.Fprintf(os.Stderr, "kbatch worker: %v\n", err)
		os.Exit(1)
	}
}

func runReconcile(args []string) {
	fs := flag.NewFlagSet("reconcile", flag.ExitOnError)
	cfgPath := fs.String("config", "", "daemon config YAML path")
	_ = fs.Parse(args)
	if *cfgPath == "" {
		fmt.Fprintln(os.Stderr, "reconcile requires --config")
		os.Exit(2)
	}
	cfg, err := config.LoadDaemon(*cfgPath)
	if err != nil {
		fmt.Fprintf(os.Stderr, "kbatch reconcile: %v\n", err)
		os.Exit(1)
	}
	rOpts, err := redis.ParseURL(cfg.RedisURL)
	if err != nil {
		fmt.Fprintf(os.Stderr, "kbatch reconcile: %v\n", err)
		os.Exit(1)
	}
	rdb := redis.NewClient(rOpts)
	st := store.NewRedisStore(rdb, cfg.BatchTTL)
	prod, err := kafkaclient.New(cfg.Brokers)
	if err != nil {
		fmt.Fprintf(os.Stderr, "kbatch reconcile: %v\n", err)
		os.Exit(1)
	}
	defer prod.Close()
	defer rdb.Close()

	ctx := context.Background()
	switch reconciler.Run(ctx, cfg, st, prod, "cli") {
	case reconciler.ResultLockSkipped:
		fmt.Println("reconcile: lock held by another process")
		os.Exit(0)
	case reconciler.ResultCompleted:
		fmt.Println("reconcile: completed")
	}
}

func usage() {
	fmt.Fprintf(os.Stderr, `kbatch — KafkaBatch Go runtime

Usage:
  kbatch serve [--socket PATH]           # deprecated: Phase 2 sidecar (Karafka only)
  kbatch daemon --config PATH [--manifest PATH]   # control plane
  kbatch worker --config PATH [--manifest PATH]   # Go backend consumer
  kbatch reconcile --config PATH                  # one-shot stuck-batch sweep

Environment:
  KAFKA_BROKERS, KAFKA_PREFIX, REDIS_URL, KAFKA_BATCH_HANDLER_MANIFEST
`)
}

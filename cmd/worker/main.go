// Command worker claims jobs from Postgres and executes them.
package main

import (
	"context"
	"errors"
	"log/slog"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/vasistamagam/distributed-jobscheduler/internal/config"
	"github.com/vasistamagam/distributed-jobscheduler/internal/handlers"
	"github.com/vasistamagam/distributed-jobscheduler/internal/job"
	"github.com/vasistamagam/distributed-jobscheduler/internal/metrics"
	"github.com/vasistamagam/distributed-jobscheduler/internal/queue"
	"github.com/vasistamagam/distributed-jobscheduler/internal/telemetry"
	"github.com/vasistamagam/distributed-jobscheduler/internal/worker"
)

func main() {
	if err := run(); err != nil {
		slog.Error("worker exited", "err", err)
		os.Exit(1)
	}
}

func run() error {
	cfg, err := config.LoadWorker(os.Getenv)
	if err != nil {
		return err
	}
	log := telemetry.NewLogger(os.Stdout, cfg.LogLevel, "worker")
	slog.SetDefault(log)

	// SIGTERM (Kubernetes pod deletion) cancels ctx, which makes Run stop claiming and drain.
	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	shutdownTrace, err := telemetry.SetupTracing(ctx, "jobscheduler-worker")
	if err != nil {
		return err
	}
	pool, err := queue.Connect(ctx, cfg.DatabaseURL, cfg.DBMaxConns, 30*time.Second)
	if err != nil {
		return err
	}
	defer pool.Close()

	reg := worker.NewRegistry()
	if err := handlers.Register(reg, log); err != nil {
		return err
	}

	m := metrics.New()
	w := worker.New(queue.New(pool), reg, worker.Config{
		Queues:            cfg.Queues,
		Concurrency:       cfg.Concurrency,
		BatchSize:         cfg.BatchSize,
		PollInterval:      cfg.PollInterval,
		Lease:             cfg.Lease,
		HeartbeatInterval: cfg.HeartbeatInterval,
		DefaultTimeout:    cfg.DefaultTimeout,
		ShutdownGrace:     cfg.ShutdownGrace,
		ReapInterval:      cfg.ReapInterval,
		Retention:         cfg.Retention,
		Backoff:           job.Backoff{Base: cfg.BackoffBase, Cap: cfg.BackoffCap},
	}, worker.WithLogger(log), worker.WithObserver(m.WorkerObserver()))

	// Ops endpoint: /metrics for Prometheus, /healthz for the liveness probe.
	mux := http.NewServeMux()
	mux.Handle("GET /metrics", m.Handler())
	mux.HandleFunc("GET /healthz", func(w http.ResponseWriter, _ *http.Request) { _, _ = w.Write([]byte("ok")) })
	ops := &http.Server{Addr: cfg.MetricsAddr, Handler: mux, ReadHeaderTimeout: 5 * time.Second}
	go func() {
		if err := ops.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
			log.Error("ops server failed", "err", err)
		}
	}()

	log.Info("worker starting", "id", w.ID(), "queues", cfg.Queues, "concurrency", cfg.Concurrency,
		"lease", cfg.Lease.String(), "types", reg.Types())
	runErr := w.Run(ctx)

	sctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	_ = ops.Shutdown(sctx)
	_ = shutdownTrace(sctx)
	return runErr
}

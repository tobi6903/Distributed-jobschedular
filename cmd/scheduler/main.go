// Command scheduler fires cron schedules. Run 2+ replicas: one is elected leader.
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
	"github.com/vasistamagam/distributed-jobscheduler/internal/metrics"
	"github.com/vasistamagam/distributed-jobscheduler/internal/queue"
	"github.com/vasistamagam/distributed-jobscheduler/internal/scheduler"
	"github.com/vasistamagam/distributed-jobscheduler/internal/telemetry"
)

func main() {
	if err := run(); err != nil {
		slog.Error("scheduler exited", "err", err)
		os.Exit(1)
	}
}

func run() error {
	cfg, err := config.LoadScheduler(os.Getenv)
	if err != nil {
		return err
	}
	log := telemetry.NewLogger(os.Stdout, cfg.LogLevel, "scheduler")
	slog.SetDefault(log)

	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	shutdownTrace, err := telemetry.SetupTracing(ctx, "jobscheduler-scheduler")
	if err != nil {
		return err
	}
	pool, err := queue.Connect(ctx, cfg.DatabaseURL, cfg.DBMaxConns, 30*time.Second)
	if err != nil {
		return err
	}
	defer pool.Close()

	m := metrics.New()
	store := queue.New(pool)
	sch := scheduler.New(scheduler.NewStore(pool), scheduler.Config{
		DSN:         cfg.DatabaseURL, // the elector opens its own dedicated session connection
		Tick:        cfg.Tick,
		RetryElect:  cfg.RetryElect,
		Grace:       cfg.DefaultGrace,
		DisableLock: cfg.DisableLock,
	}, log, scheduler.WithObserver(m.SchedulerObserver()))

	// Queue-depth gauges are exported only by the current leader, so Prometheus sees one
	// series per queue rather than one per replica.
	m.Registry.MustRegister(metrics.NewQueueCollector(store, log, 5*time.Second, sch.IsLeader))

	mux := http.NewServeMux()
	mux.Handle("GET /metrics", m.Handler())
	mux.HandleFunc("GET /healthz", func(w http.ResponseWriter, _ *http.Request) { _, _ = w.Write([]byte("ok")) })
	ops := &http.Server{Addr: cfg.MetricsAddr, Handler: mux, ReadHeaderTimeout: 5 * time.Second}
	go func() {
		if err := ops.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
			log.Error("ops server failed", "err", err)
		}
	}()

	log.Info("scheduler starting", "tick", cfg.Tick.String())
	sch.Run(ctx)

	sctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	_ = ops.Shutdown(sctx)
	return shutdownTrace(sctx)
}

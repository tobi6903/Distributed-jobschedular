// Command api serves the HTTP API for submitting and inspecting jobs and schedules.
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

	"github.com/vasistamagam/distributed-jobscheduler/internal/api"
	"github.com/vasistamagam/distributed-jobscheduler/internal/config"
	"github.com/vasistamagam/distributed-jobscheduler/internal/metrics"
	"github.com/vasistamagam/distributed-jobscheduler/internal/queue"
	"github.com/vasistamagam/distributed-jobscheduler/internal/scheduler"
	"github.com/vasistamagam/distributed-jobscheduler/internal/telemetry"
)

func main() {
	if err := run(); err != nil {
		slog.Error("api exited", "err", err)
		os.Exit(1)
	}
}

func run() error {
	cfg, err := config.LoadAPI(os.Getenv)
	if err != nil {
		return err
	}
	log := telemetry.NewLogger(os.Stdout, cfg.LogLevel, "api")
	slog.SetDefault(log)

	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	shutdownTrace, err := telemetry.SetupTracing(ctx, "jobscheduler-api")
	if err != nil {
		return err
	}
	pool, err := queue.Connect(ctx, cfg.DatabaseURL, cfg.DBMaxConns, 30*time.Second)
	if err != nil {
		return err
	}
	defer pool.Close()

	store := queue.New(pool)
	m := metrics.New()
	srv := api.New(api.Deps{
		Jobs: store, Auth: store, DB: pool, Schedules: scheduler.NewStore(pool),
		Logger: log, Observer: m.APIObserver(),
	})
	srv.Mount("GET /metrics", m.Handler())

	httpSrv := &http.Server{
		Addr:    cfg.Addr,
		Handler: srv.Handler(),
		// Timeouts protect against slowloris-style clients holding connections open.
		ReadHeaderTimeout: 5 * time.Second,
		ReadTimeout:       15 * time.Second,
		WriteTimeout:      30 * time.Second,
		IdleTimeout:       60 * time.Second,
	}
	errc := make(chan error, 1)
	go func() {
		log.Info("api listening", "addr", cfg.Addr)
		errc <- httpSrv.ListenAndServe()
	}()

	select {
	case err := <-errc:
		return err
	case <-ctx.Done():
	}

	// Graceful shutdown: fail readiness first so the load balancer stops sending traffic,
	// wait for it to notice, then stop accepting and let in-flight requests finish.
	log.Info("shutting down", "delay", cfg.ShutdownDelay.String())
	srv.SetDraining()
	time.Sleep(cfg.ShutdownDelay)
	sctx, cancel := context.WithTimeout(context.Background(), cfg.ShutdownTimeout)
	defer cancel()
	if err := httpSrv.Shutdown(sctx); err != nil && !errors.Is(err, context.Canceled) {
		return err
	}
	return shutdownTrace(sctx)
}

// Package integration holds end-to-end tests that run the real API, store and workers
// against a real Postgres. See internal/testdb for how the database is provisioned.
package integration

import (
	"context"
	"io"
	"log/slog"
	"sync"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/vasistamagam/distributed-jobscheduler/internal/job"
	"github.com/vasistamagam/distributed-jobscheduler/internal/queue"
	"github.com/vasistamagam/distributed-jobscheduler/internal/testdb"
	"github.com/vasistamagam/distributed-jobscheduler/internal/worker"
)

func quietLogger() *slog.Logger { return slog.New(slog.NewTextHandler(io.Discard, nil)) }

type env struct {
	pool  *pgxpool.Pool
	store *queue.Store
}

func newEnv(t testing.TB, maxConns int32) *env {
	t.Helper()
	pool := testdb.New(t, maxConns)
	return &env{pool: pool, store: queue.New(pool)}
}

// fastCfg is a worker config with short timers so tests run in milliseconds.
func fastCfg(id string) worker.Config {
	return worker.Config{
		ID:                id,
		Concurrency:       4,
		PollInterval:      10 * time.Millisecond,
		Lease:             2 * time.Second,
		HeartbeatInterval: 200 * time.Millisecond,
		DefaultTimeout:    10 * time.Second,
		ShutdownGrace:     5 * time.Second,
		ReapInterval:      100 * time.Millisecond,
		Backoff:           job.Backoff{Base: 20 * time.Millisecond, Cap: 100 * time.Millisecond, Rand: func() float64 { return 0 }},
	}
}

// running is a started worker with a way to stop it and read Run's result.
type running struct {
	cancel context.CancelFunc
	done   chan error
}

func (r *running) stop(t testing.TB) error {
	t.Helper()
	r.cancel()
	select {
	case err := <-r.done:
		return err
	case <-time.After(30 * time.Second):
		t.Fatal("worker did not stop in time")
		return nil
	}
}

func startWorker(t testing.TB, s worker.Store, reg *worker.Registry, cfg worker.Config) *running {
	t.Helper()
	w := worker.New(s, reg, cfg, worker.WithLogger(quietLogger()))
	ctx, cancel := context.WithCancel(context.Background())
	r := &running{cancel: cancel, done: make(chan error, 1)}
	go func() { r.done <- w.Run(ctx) }()
	t.Cleanup(func() { cancel() })
	return r
}

// eventually polls cond until it holds or the timeout elapses.
func eventually(t testing.TB, timeout time.Duration, what string, cond func() bool) {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		if cond() {
			return
		}
		time.Sleep(10 * time.Millisecond)
	}
	t.Fatalf("timed out after %v waiting for %s", timeout, what)
}

func (e *env) status(t testing.TB, id string) job.Job {
	t.Helper()
	j, err := e.store.Get(context.Background(), "acme", id)
	if err != nil {
		t.Fatalf("get %s: %v", id, err)
	}
	return j
}

func (e *env) enqueue(t testing.TB, n job.NewJob) job.Job {
	t.Helper()
	if n.TenantID == "" {
		n.TenantID = "acme"
	}
	j, _, err := e.store.Enqueue(context.Background(), n)
	if err != nil {
		t.Fatalf("enqueue: %v", err)
	}
	return j
}

// counter is a concurrency-safe execution counter keyed by job id.
type counter struct {
	mu sync.Mutex
	m  map[string]int
}

func newCounter() *counter { return &counter{m: map[string]int{}} }

func (c *counter) inc(id string) {
	c.mu.Lock()
	c.m[id]++
	c.mu.Unlock()
}

func (c *counter) get(id string) int {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.m[id]
}

func (c *counter) stats() (distinct, total, maxPerJob int) {
	c.mu.Lock()
	defer c.mu.Unlock()
	for _, n := range c.m {
		distinct++
		total += n
		maxPerJob = max(maxPerJob, n)
	}
	return
}

func newJSONLogger(w io.Writer) *slog.Logger {
	return slog.New(slog.NewJSONHandler(w, nil))
}

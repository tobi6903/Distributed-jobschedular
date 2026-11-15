package integration

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/vasistamagam/distributed-jobscheduler/internal/api"
	"github.com/vasistamagam/distributed-jobscheduler/internal/handlers"
	"github.com/vasistamagam/distributed-jobscheduler/internal/job"
	"github.com/vasistamagam/distributed-jobscheduler/internal/queue"
	"github.com/vasistamagam/distributed-jobscheduler/internal/worker"
)

func builtinRegistry(t testing.TB) *worker.Registry {
	t.Helper()
	r := worker.NewRegistry()
	if err := handlers.Register(r, quietLogger()); err != nil {
		t.Fatal(err)
	}
	return r
}

// Phase 1: submit over HTTP, a worker executes it, status is visible over HTTP.
func TestEndToEndOverHTTP(t *testing.T) {
	e := newEnv(t, 10)
	key, err := e.store.CreateAPIKey(context.Background(), "acme", "it")
	if err != nil {
		t.Fatal(err)
	}
	srv := httptest.NewServer(api.New(api.Deps{Jobs: e.store, Auth: e.store, DB: e.pool, Logger: quietLogger()}).Handler())
	defer srv.Close()
	startWorker(t, e.store, builtinRegistry(t), fastCfg("w1"))

	do := func(method, path, body string) (int, job.Job) {
		req, _ := http.NewRequest(method, srv.URL+path, strings.NewReader(body))
		req.Header.Set("Authorization", "Bearer "+key)
		resp, err := http.DefaultClient.Do(req)
		if err != nil {
			t.Fatal(err)
		}
		defer resp.Body.Close()
		var j job.Job
		_ = json.NewDecoder(resp.Body).Decode(&j)
		return resp.StatusCode, j
	}

	st, j := do("POST", "/jobs", `{"type":"noop"}`)
	if st != 201 {
		t.Fatalf("submit: %d", st)
	}
	eventually(t, 5*time.Second, "job to succeed", func() bool {
		_, got := do("GET", "/jobs/"+j.ID, "")
		return got.Status == job.StatusSucceeded
	})
	_, got := do("GET", "/jobs/"+j.ID, "")
	if got.Attempts != 1 || got.FinishedAt == nil {
		t.Fatalf("final: %+v", got)
	}
}

// Phase 3: exponential backoff retries until the handler succeeds.
func TestRetriesUntilSuccess(t *testing.T) {
	e := newEnv(t, 10)
	startWorker(t, e.store, builtinRegistry(t), fastCfg("w1"))
	j := e.enqueue(t, job.NewJob{Type: "flaky", Payload: json.RawMessage(`{"succeed_on_attempt":3}`)})
	eventually(t, 10*time.Second, "success after retries", func() bool {
		return e.status(t, j.ID).Status == job.StatusSucceeded
	})
	got := e.status(t, j.ID)
	if got.Attempts != 3 || got.LastError == nil {
		t.Fatalf("expected 3 attempts and a recorded last_error: %+v", got)
	}
}

// Phase 3: retries exhausted -> dead; manual replay gives a fresh budget.
func TestDeadLetterAndReplay(t *testing.T) {
	e := newEnv(t, 10)
	startWorker(t, e.store, builtinRegistry(t), fastCfg("w1"))
	j := e.enqueue(t, job.NewJob{Type: "fail", MaxAttempts: 3})
	eventually(t, 10*time.Second, "dead", func() bool { return e.status(t, j.ID).Status == job.StatusDead })
	got := e.status(t, j.ID)
	if got.Attempts != 3 || got.LastError == nil || !strings.Contains(*got.LastError, "intentional failure") {
		t.Fatalf("dead job: %+v", got)
	}

	if _, err := e.store.Replay(context.Background(), "acme", j.ID); err != nil {
		t.Fatal(err)
	}
	eventually(t, 10*time.Second, "dead again after replay", func() bool {
		g := e.status(t, j.ID)
		return g.Status == job.StatusDead && g.Attempts == 3
	})
}

func TestPermanentErrorSkipsRetries(t *testing.T) {
	e := newEnv(t, 10)
	startWorker(t, e.store, builtinRegistry(t), fastCfg("w1"))
	j := e.enqueue(t, job.NewJob{Type: "fail", Payload: json.RawMessage(`{"permanent":true}`), MaxAttempts: 5})
	eventually(t, 5*time.Second, "failed", func() bool { return e.status(t, j.ID).Status == job.StatusFailed })
	if got := e.status(t, j.ID); got.Attempts != 1 {
		t.Fatalf("permanent failure must not retry: attempts=%d", got.Attempts)
	}
}

func TestUnknownTypeFails(t *testing.T) {
	e := newEnv(t, 10)
	startWorker(t, e.store, builtinRegistry(t), fastCfg("w1"))
	j := e.enqueue(t, job.NewJob{Type: "does-not-exist"})
	eventually(t, 5*time.Second, "failed", func() bool { return e.status(t, j.ID).Status == job.StatusFailed })
}

// Phase 3: per-job timeout cancels a hung handler via its context, then retries.
func TestPerJobTimeout(t *testing.T) {
	e := newEnv(t, 10)
	startWorker(t, e.store, builtinRegistry(t), fastCfg("w1"))
	one := 1
	j := e.enqueue(t, job.NewJob{Type: "sleep", Payload: json.RawMessage(`{"seconds":60}`), TimeoutSeconds: &one, MaxAttempts: 2})
	start := time.Now()
	eventually(t, 15*time.Second, "dead after timeouts", func() bool { return e.status(t, j.ID).Status == job.StatusDead })
	if el := time.Since(start); el > 8*time.Second {
		t.Fatalf("timeouts should fire at ~1s each, took %v", el)
	}
	if got := e.status(t, j.ID); got.LastError == nil || !strings.Contains(*got.LastError, "timed out") {
		t.Fatalf("last_error: %+v", got.LastError)
	}
}

// Phase 2: a worker that dies mid-job loses its lease; another worker's reaper recovers it.
func TestCrashedWorkerJobIsRecovered(t *testing.T) {
	e := newEnv(t, 10)
	j := e.enqueue(t, job.NewJob{Type: "noop"})
	// A "crashed" worker: it claimed the job with a short lease, then vanished.
	c, err := e.store.Claim(context.Background(), "default", "dead-worker", 1, 300*time.Millisecond)
	if err != nil || len(c) != 1 {
		t.Fatal(err)
	}
	startWorker(t, e.store, builtinRegistry(t), fastCfg("rescuer"))
	eventually(t, 10*time.Second, "recovery", func() bool { return e.status(t, j.ID).Status == job.StatusSucceeded })
	got := e.status(t, j.ID)
	if got.Attempts != 2 {
		t.Fatalf("crash consumes an attempt: attempts=%d", got.Attempts)
	}
}

// Phase 2: heartbeats keep a long job's lease alive, so a concurrent reaper never steals it.
func TestHeartbeatPreventsReclaimOfLongJob(t *testing.T) {
	e := newEnv(t, 10)
	runs := newCounter()
	reg := worker.NewRegistry()
	_ = reg.Register("long", func(ctx context.Context, j job.Job) error {
		runs.inc(j.ID)
		select {
		case <-time.After(1500 * time.Millisecond): // 5x the lease
			return nil
		case <-ctx.Done():
			return ctx.Err()
		}
	})
	cfg := fastCfg("runner")
	cfg.Lease = 300 * time.Millisecond
	cfg.HeartbeatInterval = 80 * time.Millisecond
	startWorker(t, e.store, reg, cfg)

	// A second worker that only reaps (its queue is empty) and does so aggressively.
	reaper := fastCfg("reaper")
	reaper.Queues = []string{"nothing-here"}
	reaper.ReapInterval = 30 * time.Millisecond
	startWorker(t, e.store, reg, reaper)

	j := e.enqueue(t, job.NewJob{Type: "long"})
	eventually(t, 10*time.Second, "long job to finish", func() bool { return e.status(t, j.ID).Status == job.StatusSucceeded })
	if got := e.status(t, j.ID); got.Attempts != 1 || runs.get(j.ID) != 1 {
		t.Fatalf("job must run exactly once: attempts=%d runs=%d", got.Attempts, runs.get(j.ID))
	}
}

// Phase 3: SIGTERM semantics. In-flight jobs finish; nothing new is claimed.
func TestGracefulShutdownFinishesInFlight(t *testing.T) {
	e := newEnv(t, 10)
	reg := worker.NewRegistry()
	started := make(chan struct{}, 1)
	_ = reg.Register("slow", func(ctx context.Context, j job.Job) error {
		started <- struct{}{}
		time.Sleep(700 * time.Millisecond) // deliberately ignores ctx, like real blocking work
		return nil
	})
	cfg := fastCfg("w1")
	cfg.Concurrency = 1
	r := startWorker(t, e.store, reg, cfg)

	first := e.enqueue(t, job.NewJob{Type: "slow"})
	<-started
	second := e.enqueue(t, job.NewJob{Type: "slow"}) // arrives while the first is running
	if err := r.stop(t); err != nil {
		t.Fatalf("Run: %v", err)
	}
	if got := e.status(t, first.ID); got.Status != job.StatusSucceeded {
		t.Fatalf("in-flight job must finish during drain: %+v", got)
	}
	if got := e.status(t, second.ID); got.Status != job.StatusPending || got.Attempts != 0 {
		t.Fatalf("after SIGTERM no new job may be claimed: %+v", got)
	}
}

// If the grace period is too short, stragglers are cancelled and released without
// consuming an attempt, so a deploy never burns a job's retry budget.
func TestForcedShutdownReleasesJob(t *testing.T) {
	e := newEnv(t, 10)
	started := make(chan struct{}, 1)
	reg := worker.NewRegistry()
	_ = reg.Register("hang", func(ctx context.Context, j job.Job) error {
		started <- struct{}{}
		<-ctx.Done()
		return ctx.Err()
	})
	cfg := fastCfg("w1")
	cfg.ShutdownGrace = 200 * time.Millisecond
	r := startWorker(t, e.store, reg, cfg)

	j := e.enqueue(t, job.NewJob{Type: "hang"})
	<-started
	if err := r.stop(t); err != nil {
		t.Fatalf("Run: %v", err)
	}
	got := e.status(t, j.ID)
	if got.Status != job.StatusPending || got.Attempts != 0 || got.LockedBy != nil {
		t.Fatalf("released job should be pending with attempts restored: %+v", got)
	}
}

// Retention sweeper deletes old finished jobs but never touches live ones.
func TestRetentionSweeper(t *testing.T) {
	e := newEnv(t, 10)
	cfg := fastCfg("w1")
	cfg.Retention = 200 * time.Millisecond
	cfg.RetentionInterval = 50 * time.Millisecond
	startWorker(t, e.store, builtinRegistry(t), cfg)
	done := e.enqueue(t, job.NewJob{Type: "noop"})
	live := e.enqueue(t, job.NewJob{Type: "noop", Delay: time.Hour})
	eventually(t, 10*time.Second, "finished job to be deleted", func() bool {
		_, err := e.store.Get(context.Background(), "acme", done.ID)
		return err != nil
	})
	if _, err := e.store.Get(context.Background(), "acme", live.ID); err != nil {
		t.Fatalf("a pending job must never be swept: %v", err)
	}
}

// A rate-limited queue is processed at the configured rate end to end, while an unlimited
// queue on the same workers is not slowed down.
func TestRateLimitedQueueIsThrottledEndToEnd(t *testing.T) {
	e := newEnv(t, 20)
	e.store.SetLimitCacheTTL(100*time.Millisecond, 0)
	ctx := context.Background()
	rate, burst := 20.0, 2
	if err := e.store.SetLimit(ctx, queue.Limit{Queue: "slow-api", TenantID: queue.AllTenants, RatePerSecond: &rate, Burst: &burst}); err != nil {
		t.Fatal(err)
	}
	cfg := fastCfg("w1")
	cfg.Queues = []string{"slow-api", "default"}
	cfg.Concurrency = 16
	startWorker(t, e.store, builtinRegistry(t), cfg)

	// The clock starts before the first enqueue: the worker is already running, so it begins
	// dispatching the rate-limited jobs while we are still inserting them.
	begin := time.Now()
	const limited = 60
	for i := 0; i < limited; i++ {
		e.enqueue(t, job.NewJob{Queue: "slow-api", Type: "noop"})
	}
	free := make([]string, 0, 30)
	for i := 0; i < 30; i++ {
		free = append(free, e.enqueue(t, job.NewJob{Type: "noop"}).ID)
	}

	unlimitedStart := time.Now()
	// The unlimited queue drains almost immediately despite the throttled queue's backlog.
	eventually(t, 5*time.Second, "unlimited queue to drain", func() bool {
		for _, id := range free {
			if e.status(t, id).Status != job.StatusSucceeded {
				return false
			}
		}
		return true
	})
	if d := time.Since(unlimitedStart); d > 3*time.Second {
		t.Fatalf("unlimited queue took %v; it must not be held back by the limited one", d)
	}

	countDone := func() int {
		var n int
		_ = e.pool.QueryRow(ctx, `SELECT count(*) FROM jobs WHERE queue='slow-api' AND status='succeeded'`).Scan(&n)
		return n
	}
	eventually(t, 15*time.Second, "limited queue to finish", func() bool { return countDone() == limited })
	elapsed := time.Since(begin).Seconds()
	// 60 jobs at 20/s with a burst of 2 cannot finish in under ~2.9s from the first enqueue.
	if min := float64(limited-burst) / rate * 0.9; elapsed < min {
		t.Fatalf("finished %d jobs in %.2fs; at %.0f/s that takes at least %.2fs", limited, elapsed, rate, min)
	}
}

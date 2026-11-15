package integration

import (
	"context"
	"errors"
	"fmt"
	"math/rand/v2"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/vasistamagam/distributed-jobscheduler/internal/job"
	"github.com/vasistamagam/distributed-jobscheduler/internal/queue"
	"github.com/vasistamagam/distributed-jobscheduler/internal/worker"
)

// bulkInsert creates n jobs in one statement. Going through Enqueue 100k times would
// measure the HTTP/insert path, not the claim path this test is about.
func bulkInsert(t testing.TB, e *env, typ string, n int) {
	t.Helper()
	_, err := e.pool.Exec(context.Background(),
		`INSERT INTO jobs (tenant_id, type) SELECT 'acme', $1 FROM generate_series(1, $2)`, typ, n)
	if err != nil {
		t.Fatalf("bulk insert: %v", err)
	}
}

func countStatus(t testing.TB, e *env) (succeeded, other int) {
	t.Helper()
	err := e.pool.QueryRow(context.Background(),
		`SELECT count(*) FILTER (WHERE status = 'succeeded'), count(*) FILTER (WHERE status <> 'succeeded') FROM jobs`).
		Scan(&succeeded, &other)
	if err != nil {
		t.Fatalf("count: %v", err)
	}
	return
}

// Phase 2 acceptance test: 50 competing workers, 100k jobs, zero duplicates, zero lost.
//
// "Duplicate" = a handler invoked twice for the same job. With no failures injected and a
// lease far longer than the test, SKIP LOCKED + leases must make that impossible.
// "Lost" = a job that never reaches succeeded, or whose handler never ran.
func TestFiftyWorkersHundredThousandJobs(t *testing.T) {
	total := 100_000
	if testing.Short() {
		total = 5_000
	}
	const workers = 50
	e := newEnv(t, 80)
	bulkInsert(t, e, "count", total)

	runs := newCounter()
	reg := worker.NewRegistry()
	_ = reg.Register("count", func(ctx context.Context, j job.Job) error {
		runs.inc(j.ID)
		return nil
	})

	start := time.Now()
	var fleet []*running
	for i := 0; i < workers; i++ {
		cfg := fastCfg(fmt.Sprintf("w%02d", i))
		cfg.Concurrency = 8
		cfg.BatchSize = 25
		cfg.Lease = 5 * time.Minute // far beyond the test: no reclaim may happen
		cfg.HeartbeatInterval = time.Minute
		cfg.ReapInterval = time.Second
		fleet = append(fleet, startWorker(t, e.store, reg, cfg))
	}

	var lastLog time.Time
	eventually(t, 10*time.Minute, "all jobs to succeed", func() bool {
		ok, other := countStatus(t, e)
		if time.Since(lastLog) > 5*time.Second {
			lastLog = time.Now()
			t.Logf("progress: %d/%d succeeded", ok, total)
		}
		return ok == total && other == 0
	})
	elapsed := time.Since(start)
	for _, w := range fleet {
		if err := w.stop(t); err != nil {
			t.Errorf("worker stop: %v", err)
		}
	}

	distinct, executions, maxPerJob := runs.stats()
	t.Logf("%d jobs by %d workers in %v (%.0f jobs/s); executions=%d", total, workers, elapsed.Round(time.Millisecond),
		float64(total)/elapsed.Seconds(), executions)
	if distinct != total {
		t.Errorf("LOST: handler ran for %d distinct jobs, want %d", distinct, total)
	}
	if executions != total || maxPerJob != 1 {
		t.Errorf("DUPLICATES: %d executions for %d jobs (max %d runs of one job)", executions, total, maxPerJob)
	}
	var badAttempts int
	_ = e.pool.QueryRow(context.Background(), `SELECT count(*) FROM jobs WHERE attempts <> 1`).Scan(&badAttempts)
	if badAttempts != 0 {
		t.Errorf("%d jobs have attempts != 1", badAttempts)
	}
}

// crashable wraps a Store so a test can "kill" a worker: after kill() every call fails, as
// if the process had died or been partitioned from the database. Its already-running
// handlers keep going (a zombie), which is the nastiest case for the fencing logic.
type crashable struct {
	worker.Store
	dead atomic.Bool
}

var errCrashed = errors.New("worker crashed")

func (c *crashable) Claim(ctx context.Context, q, id string, n int, l time.Duration) ([]job.Job, error) {
	if c.dead.Load() {
		return nil, errCrashed
	}
	return c.Store.Claim(ctx, q, id, n, l)
}
func (c *crashable) Heartbeat(ctx context.Context, r job.Ref, l time.Duration) error {
	if c.dead.Load() {
		return errCrashed
	}
	return c.Store.Heartbeat(ctx, r, l)
}
func (c *crashable) Complete(ctx context.Context, r job.Ref) error {
	if c.dead.Load() {
		return errCrashed
	}
	return c.Store.Complete(ctx, r)
}
func (c *crashable) Fail(ctx context.Context, r job.Ref, p queue.FailParams) error {
	if c.dead.Load() {
		return errCrashed
	}
	return c.Store.Fail(ctx, r, p)
}
func (c *crashable) Release(ctx context.Context, r job.Ref) error {
	if c.dead.Load() {
		return errCrashed
	}
	return c.Store.Release(ctx, r)
}
func (c *crashable) Reap(ctx context.Context, n int) (queue.ReapResult, error) {
	if c.dead.Load() {
		return queue.ReapResult{}, errCrashed
	}
	return c.Store.Reap(ctx, n)
}

// Chaos: workers die at random while jobs are in flight. Guarantees under test:
//   - nothing is lost: every job still reaches `succeeded`
//   - duplicate *executions* are possible (at-least-once) but only for jobs that were
//     reclaimed (attempts > 1), never for a job claimed exactly once
//   - every job is recorded as succeeded exactly once (the fence rejects zombie writes)
func TestWorkerCrashesLoseNothing(t *testing.T) {
	total := 3_000
	if testing.Short() {
		total = 800
	}
	e := newEnv(t, 60)
	bulkInsert(t, e, "work", total)

	runs := newCounter()
	reg := worker.NewRegistry()
	_ = reg.Register("work", func(ctx context.Context, j job.Job) error {
		runs.inc(j.ID)
		time.Sleep(time.Duration(5+rand.IntN(20)) * time.Millisecond)
		return nil
	})

	const workers = 20
	stores := make([]*crashable, workers)
	var fleet []*running
	for i := 0; i < workers; i++ {
		stores[i] = &crashable{Store: e.store}
		cfg := fastCfg(fmt.Sprintf("c%02d", i))
		cfg.Concurrency = 5
		cfg.BatchSize = 10
		cfg.Lease = 600 * time.Millisecond
		cfg.HeartbeatInterval = 150 * time.Millisecond
		cfg.ReapInterval = 100 * time.Millisecond
		fleet = append(fleet, startWorker(t, stores[i], reg, cfg))
	}

	// Kill 12 of the 20 workers at random moments while work is flowing.
	go func() {
		for _, i := range rand.Perm(workers)[:12] {
			time.Sleep(time.Duration(100+rand.IntN(400)) * time.Millisecond)
			stores[i].dead.Store(true)
		}
	}()

	eventually(t, 3*time.Minute, "all jobs to succeed despite crashes", func() bool {
		ok, other := countStatus(t, e)
		return ok == total && other == 0
	})
	for _, w := range fleet {
		_ = w.stop(t)
	}

	distinct, executions, _ := runs.stats()
	if distinct != total {
		t.Fatalf("LOST: handler ran for %d of %d jobs", distinct, total)
	}
	// A job executed more than once must have been claimed more than once.
	rows, err := e.pool.Query(context.Background(), `SELECT id, attempts FROM jobs`)
	if err != nil {
		t.Fatal(err)
	}
	defer rows.Close()
	reclaimed := 0
	for rows.Next() {
		var id string
		var attempts int
		if err := rows.Scan(&id, &attempts); err != nil {
			t.Fatal(err)
		}
		if attempts > 1 {
			reclaimed++
		}
		if n := runs.get(id); n > attempts {
			t.Fatalf("job %s executed %d times but was only claimed %d times", id, n, attempts)
		}
		if runs.get(id) > 1 && attempts == 1 {
			t.Fatalf("job %s ran twice without being reclaimed", id)
		}
	}
	t.Logf("%d jobs, %d executions, %d jobs were reclaimed after a worker crash", total, executions, reclaimed)
	if reclaimed == 0 {
		t.Log("warning: no job was in flight when workers died; the chaos did not bite this run")
	}
}

// Many API clients submitting the same idempotency key concurrently create one job.
func TestConcurrentIdempotentSubmits(t *testing.T) {
	e := newEnv(t, 30)
	key := "same"
	var wg sync.WaitGroup
	var created atomic.Int32
	for i := 0; i < 50; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			_, c, err := e.store.Enqueue(context.Background(), job.NewJob{TenantID: "acme", Type: "noop", IdempotencyKey: &key})
			if err != nil {
				t.Errorf("enqueue: %v", err)
			}
			if c {
				created.Add(1)
			}
		}()
	}
	wg.Wait()
	var n int
	_ = e.pool.QueryRow(context.Background(), `SELECT count(*) FROM jobs`).Scan(&n)
	if created.Load() != 1 || n != 1 {
		t.Fatalf("created=%d rows=%d, want 1/1", created.Load(), n)
	}
}

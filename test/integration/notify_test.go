package integration

import (
	"context"
	"encoding/json"
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/vasistamagam/distributed-jobscheduler/internal/job"
	"github.com/vasistamagam/distributed-jobscheduler/internal/queue"
	"github.com/vasistamagam/distributed-jobscheduler/internal/worker"
)

// notifyFleet starts n workers, each with its own LISTEN connection, whose poll is far longer than any test
// timeout: only a notification (or, in the fallback tests, the shortened fallback) can explain a fast pickup.
type notifyFleet struct {
	listeners []*queue.Listener
	fleet     []*running
}

func startNotifyFleet(t *testing.T, e *env, n int, poll, fallback time.Duration, dsn string, reg *worker.Registry) *notifyFleet {
	t.Helper()
	f := &notifyFleet{}
	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)
	for i := 0; i < n; i++ {
		l := queue.NewListener(dsn, nil, quietLogger(), nil)
		go l.Run(ctx)
		cfg := fastCfg(fmt.Sprintf("n%02d", i))
		cfg.PollInterval = poll
		cfg.FallbackPoll = fallback
		cfg.Wake = l
		f.listeners = append(f.listeners, l)
		f.fleet = append(f.fleet, startWorker(t, e.store, reg, cfg))
	}
	return f
}

func (f *notifyFleet) waitListening(t *testing.T) {
	t.Helper()
	for _, l := range f.listeners {
		l := l
		eventually(t, 10*time.Second, "listener connected", l.Healthy)
	}
}

func (f *notifyFleet) stop(t *testing.T) {
	t.Helper()
	for _, w := range f.fleet {
		if err := w.stop(t); err != nil {
			t.Errorf("worker stop: %v", err)
		}
	}
}

func newPickupRecorder() (*worker.Registry, func() []time.Duration) {
	var mu sync.Mutex
	var got []time.Duration
	reg := worker.NewRegistry()
	_ = reg.Register("ping", func(_ context.Context, j job.Job) error {
		mu.Lock()
		got = append(got, j.PickupLatency)
		mu.Unlock()
		return nil
	})
	return reg, func() []time.Duration { mu.Lock(); defer mu.Unlock(); return append([]time.Duration(nil), got...) }
}

func pingJob() job.NewJob {
	return job.NewJob{TenantID: "acme", Type: "ping", Payload: json.RawMessage(`{}`)}
}

// The headline claim: an idle fleet that would otherwise wait up to 30s for its next poll picks a new job up in
// milliseconds, because Enqueue's notification wakes it. Every job here arrives while all workers are asleep on a
// 30s timer, so a pickup under a second can only come from the notification.
func TestNotifyWakesIdleFleetInsteadOfWaitingForThePoll(t *testing.T) {
	e := newEnv(t, 40)
	dsn := e.pool.Config().ConnString()
	n := queue.NewNotifier(e.pool, 10*time.Millisecond, nil)
	defer n.Close()
	e.store.SetNotifier(n)
	reg, pickups := newPickupRecorder()
	f := startNotifyFleet(t, e, 5, 30*time.Second, 30*time.Second, dsn, reg)
	f.waitListening(t)
	time.Sleep(300 * time.Millisecond) // every worker is now idle on its long timer

	const jobs = 20
	for i := 0; i < jobs; i++ {
		if _, _, err := e.store.Enqueue(context.Background(), pingJob()); err != nil {
			t.Fatal(err)
		}
		time.Sleep(60 * time.Millisecond)
	}
	eventually(t, 10*time.Second, "all jobs to run", func() bool { return len(pickups()) == jobs })
	var worst, sum time.Duration
	for _, p := range pickups() {
		sum += p
		worst = max(worst, p)
	}
	t.Logf("pickup latency with a 30s poll: mean %v, worst %v over %d jobs", (sum / jobs).Round(time.Millisecond), worst.Round(time.Millisecond), jobs)
	if worst > time.Second {
		t.Fatalf("worst pickup %v: jobs waited for the 30s poll, so notifications are not waking workers", worst)
	}
	f.stop(t)
}

// Correctness must not depend on notifications. With NO notifier on the store (every hint is "lost"), the
// shortened fallback poll still finds every job.
func TestLostNotificationsAreRecoveredByTheFallbackPoll(t *testing.T) {
	e := newEnv(t, 20)
	dsn := e.pool.Config().ConnString()
	reg, pickups := newPickupRecorder()
	f := startNotifyFleet(t, e, 3, 30*time.Second, 200*time.Millisecond, dsn, reg) // no SetNotifier
	f.waitListening(t)
	for i := 0; i < 10; i++ {
		if _, _, err := e.store.Enqueue(context.Background(), pingJob()); err != nil {
			t.Fatal(err)
		}
	}
	eventually(t, 10*time.Second, "the fallback poll to find all jobs", func() bool { return len(pickups()) == 10 })
	f.stop(t)
}

// If the LISTEN connection cannot be established at all, workers poll at the normal interval and nothing is lost.
func TestWorkersPollNormallyWhenTheListenerCannotConnect(t *testing.T) {
	e := newEnv(t, 20)
	n := queue.NewNotifier(e.pool, 0, nil)
	defer n.Close()
	e.store.SetNotifier(n)
	reg, pickups := newPickupRecorder()
	// A DSN nobody answers on: Healthy() stays false, so the 30s FallbackPoll must NOT apply; PollInterval does.
	f := startNotifyFleet(t, e, 3, 50*time.Millisecond, 30*time.Second, "postgres://x@127.0.0.1:1/none?connect_timeout=1", reg)
	for i := 0; i < 10; i++ {
		if _, _, err := e.store.Enqueue(context.Background(), pingJob()); err != nil {
			t.Fatal(err)
		}
	}
	eventually(t, 10*time.Second, "plain polling to find all jobs", func() bool { return len(pickups()) == 10 })
	f.stop(t)
}

// Many workers all woken by one burst of hints must still run every job exactly once: a notification changes
// WHEN a worker looks, never WHAT it may claim (FOR UPDATE SKIP LOCKED is unchanged).
func TestNotifiedFleetRunsEveryJobExactlyOnce(t *testing.T) {
	e := newEnv(t, 60)
	dsn := e.pool.Config().ConnString()
	n := queue.NewNotifier(e.pool, 5*time.Millisecond, nil)
	defer n.Close()
	e.store.SetNotifier(n)

	runs := newCounter()
	reg := worker.NewRegistry()
	_ = reg.Register("ping", func(_ context.Context, j job.Job) error { runs.inc(j.ID); return nil })
	f := startNotifyFleet(t, e, 12, 30*time.Second, 30*time.Second, dsn, reg)
	f.waitListening(t)

	const total = 3000
	var wg sync.WaitGroup
	for g := 0; g < 8; g++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for i := 0; i < total/8; i++ {
				if _, _, err := e.store.Enqueue(context.Background(), pingJob()); err != nil {
					t.Error(err)
					return
				}
			}
		}()
	}
	wg.Wait()
	eventually(t, 60*time.Second, "all jobs to succeed", func() bool {
		ok, other := countStatus(t, e)
		return ok == total && other == 0
	})
	f.stop(t)
	distinct, executions, maxPerJob := runs.stats()
	if distinct != total || executions != total || maxPerJob != 1 {
		t.Fatalf("distinct=%d executions=%d maxPerJob=%d, want %d, %d, 1", distinct, executions, maxPerJob, total, total)
	}
}

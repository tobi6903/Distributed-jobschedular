package worker

import (
	"context"
	"sync/atomic"
	"testing"
	"time"

	"github.com/vasistamagam/distributed-jobscheduler/internal/job"
)

// fakeWake is a WakeSource the test controls.
type fakeWake struct {
	ch      chan struct{}
	healthy atomic.Bool
}

func newFakeWake(healthy bool) *fakeWake {
	f := &fakeWake{ch: make(chan struct{}, 1)}
	f.healthy.Store(healthy)
	return f
}
func (f *fakeWake) Wake() <-chan struct{} { return f.ch }
func (f *fakeWake) Healthy() bool         { return f.healthy.Load() }
func (f *fakeWake) hint() {
	select {
	case f.ch <- struct{}{}:
	default:
	}
}

// addJob makes a job claimable on the fake store while the worker is running.
func addJob(fs *fakeStore, id string) {
	j := newFake(job.Job{ID: id, Type: "t"}).pending[0]
	fs.mu.Lock()
	fs.pending = append(fs.pending, j)
	fs.mu.Unlock()
}

func (f *fakeStore) completedCount() int {
	f.mu.Lock()
	defer f.mu.Unlock()
	return len(f.completed)
}

func runWakeWorker(t *testing.T, fs *fakeStore, wake WakeSource, poll, fallback time.Duration, obs Observer) (stop func() error) {
	t.Helper()
	cfg := testCfg()
	cfg.PollInterval = poll
	cfg.FallbackPoll = fallback
	cfg.Wake = wake
	opts := []Option{quiet()}
	if obs != nil {
		opts = append(opts, WithObserver(obs))
	}
	return start(t, New(fs, reg(t, "t", func(context.Context, job.Job) error { return nil }), cfg, opts...))
}

// An idle worker whose only poll is 30s away claims within moments of a hint. Without the wake source this test
// would take the full poll interval.
func TestIdleWorkerClaimsOnHint(t *testing.T) {
	fs := newFake()
	wake := newFakeWake(true)
	stop := runWakeWorker(t, fs, wake, 30*time.Second, 30*time.Second, nil)
	defer func() { _ = stop() }()

	time.Sleep(100 * time.Millisecond) // let it go idle on the long timer
	addJob(fs, "a")
	start := time.Now()
	wake.hint()
	waitFor(t, "the job to be claimed after a hint", func() bool { return fs.completedCount() == 1 })
	if d := time.Since(start); d > time.Second {
		t.Fatalf("took %v after the hint, want well under the 30s poll interval", d)
	}
}

// While the wake source is healthy the poll slows to FallbackPoll (it is only a safety net), so a job that is
// NOT announced waits for it. Once the source reports a hint, it is picked up. This pins down both halves: the
// fallback really applies, and the hint really bypasses it.
func TestHealthyWakeSourceSlowsPollingToFallback(t *testing.T) {
	fs := newFake()
	wake := newFakeWake(true)
	stop := runWakeWorker(t, fs, wake, 10*time.Millisecond, 30*time.Second, nil)
	defer func() { _ = stop() }()

	time.Sleep(100 * time.Millisecond) // the first idle wait started before the job existed
	addJob(fs, "a")
	time.Sleep(400 * time.Millisecond) // 40 normal poll intervals
	if n := fs.completedCount(); n != 0 {
		t.Fatalf("job claimed without a hint after %d normal poll intervals: the fallback poll is not applied while healthy", 40)
	}
	wake.hint()
	waitFor(t, "the hint to wake the worker", func() bool { return fs.completedCount() == 1 })
}

// If the wake source is down, notifications cannot be trusted: the worker polls at the normal interval, so an
// unannounced job is still found quickly even though FallbackPoll is huge.
func TestUnhealthyWakeSourceFallsBackToNormalPolling(t *testing.T) {
	fs := newFake()
	wake := newFakeWake(false)
	stop := runWakeWorker(t, fs, wake, 10*time.Millisecond, 30*time.Second, nil)
	defer func() { _ = stop() }()

	time.Sleep(50 * time.Millisecond)
	addJob(fs, "a")
	waitFor(t, "the poll to find the job while the wake source is down", func() bool { return fs.completedCount() == 1 })
}

// Wake-ups are reported by outcome: a hint that finds work is "claimed", one that finds nothing (another worker
// won the race, or the hint was stale) is "empty". That ratio is how an operator sees the thundering herd.
func TestWakeupOutcomesAreObserved(t *testing.T) {
	fs := newFake()
	wake := newFakeWake(true)
	obs := &obsRec{}
	stop := runWakeWorker(t, fs, wake, 30*time.Second, 30*time.Second, obs)
	defer func() { _ = stop() }()

	time.Sleep(100 * time.Millisecond)
	wake.hint() // nothing to claim
	waitFor(t, "an empty wake-up", func() bool { obs.mu.Lock(); defer obs.mu.Unlock(); return len(obs.wakeups) == 1 })
	addJob(fs, "a")
	wake.hint()
	waitFor(t, "a productive wake-up", func() bool { obs.mu.Lock(); defer obs.mu.Unlock(); return len(obs.wakeups) == 2 })
	obs.mu.Lock()
	defer obs.mu.Unlock()
	if obs.wakeups[0] || !obs.wakeups[1] {
		t.Fatalf("wakeups = %v, want [false true] (empty, then claimed)", obs.wakeups)
	}
}

// Without a wake source nothing changes: FallbackPoll is ignored and the worker polls at PollInterval.
func TestNoWakeSourceIgnoresFallbackPoll(t *testing.T) {
	fs := newFake()
	stop := runWakeWorker(t, fs, nil, 10*time.Millisecond, 30*time.Second, nil)
	defer func() { _ = stop() }()
	time.Sleep(50 * time.Millisecond)
	addJob(fs, "a")
	waitFor(t, "plain polling to find the job", func() bool { return fs.completedCount() == 1 })
}

// Every claim query is reported with the number of jobs it returned, so empty polls (idle load) are countable.
func TestClaimPollsAreObserved(t *testing.T) {
	fs := newFake(job.Job{ID: "a", Type: "t"})
	obs := &obsRec{}
	stop := runWakeWorker(t, fs, nil, 5*time.Millisecond, 0, obs)
	defer func() { _ = stop() }()
	waitFor(t, "one hit and several empty polls", func() bool {
		obs.mu.Lock()
		defer obs.mu.Unlock()
		hits, empty := 0, 0
		for _, n := range obs.polls {
			if n > 0 {
				hits++
			} else {
				empty++
			}
		}
		return hits == 1 && empty >= 3
	})
}

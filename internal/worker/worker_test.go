package worker

import (
	"context"
	"errors"
	"io"
	"log/slog"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/vasistamagam/distributed-jobscheduler/internal/job"
	"github.com/vasistamagam/distributed-jobscheduler/internal/queue"
)

// fakeStore is an in-memory Store: it hands out a fixed list of jobs once and records
// every finalising call.
type fakeStore struct {
	mu        sync.Mutex
	pending   []job.Job
	completed []string
	failed    map[string]queue.FailParams
	released  []string
	hbErr     error // returned by Heartbeat
	hbCalls   atomic.Int32
	completeE error // returned by Complete
}

func newFake(jobs ...job.Job) *fakeStore {
	for i := range jobs {
		w := "w"
		jobs[i].LockedBy = &w
		jobs[i].Status = job.StatusRunning
		if jobs[i].Attempts == 0 {
			jobs[i].Attempts = 1
		}
		if jobs[i].MaxAttempts == 0 {
			jobs[i].MaxAttempts = 3
		}
		if jobs[i].Queue == "" {
			jobs[i].Queue = "default"
		}
	}
	return &fakeStore{pending: jobs, failed: map[string]queue.FailParams{}}
}

func (f *fakeStore) Claim(_ context.Context, _, _ string, limit int, _ time.Duration) ([]job.Job, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	n := min(limit, len(f.pending))
	out := f.pending[:n]
	f.pending = f.pending[n:]
	return out, nil
}
func (f *fakeStore) Heartbeat(context.Context, job.Ref, time.Duration) error {
	f.hbCalls.Add(1)
	return f.hbErr
}
func (f *fakeStore) Complete(_ context.Context, r job.Ref) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.completeE != nil {
		return f.completeE
	}
	f.completed = append(f.completed, r.ID)
	return nil
}
func (f *fakeStore) Fail(_ context.Context, r job.Ref, p queue.FailParams) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.failed[r.ID] = p
	return nil
}
func (f *fakeStore) Release(_ context.Context, r job.Ref) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.released = append(f.released, r.ID)
	return nil
}
func (f *fakeStore) Reap(context.Context, int) (queue.ReapResult, error) {
	return queue.ReapResult{}, nil
}
func (f *fakeStore) DeleteFinished(context.Context, time.Duration, int) (int, error) {
	return 0, nil
}

// finalised is the number of jobs with a recorded final write.
func (f *fakeStore) finalised() int {
	f.mu.Lock()
	defer f.mu.Unlock()
	return len(f.completed) + len(f.failed) + len(f.released)
}

type obsRec struct {
	mu       sync.Mutex
	outcomes []string
	wakeups  []bool
	polls    []int // jobs returned by each claim query
}

func (o *obsRec) ClaimPoll(_ string, got int) {
	o.mu.Lock()
	o.polls = append(o.polls, got)
	o.mu.Unlock()
}
func (o *obsRec) Wakeup(claimed bool) {
	o.mu.Lock()
	o.wakeups = append(o.wakeups, claimed)
	o.mu.Unlock()
}
func (o *obsRec) Claimed(string, int, []time.Duration) {}
func (o *obsRec) Finished(_, _, outcome string, _ time.Duration) {
	o.mu.Lock()
	o.outcomes = append(o.outcomes, outcome)
	o.mu.Unlock()
}
func (o *obsRec) Reaped(int, int)   {}
func (o *obsRec) Deleted(int)       {}
func (o *obsRec) CompleteBatch(int) {}
func (o *obsRec) has(outcome string) bool {
	o.mu.Lock()
	defer o.mu.Unlock()
	for _, x := range o.outcomes {
		if x == outcome {
			return true
		}
	}
	return false
}

func testCfg() Config {
	return Config{
		ID: "w", Concurrency: 4, PollInterval: 5 * time.Millisecond,
		Lease: 200 * time.Millisecond, HeartbeatInterval: 20 * time.Millisecond,
		DefaultTimeout: 2 * time.Second, ShutdownGrace: time.Second,
		ReapInterval: time.Hour,
		Backoff:      job.Backoff{Base: 10 * time.Second, Cap: time.Hour, Rand: func() float64 { return 0 }},
	}
}

func quiet() Option { return WithLogger(slog.New(slog.NewTextHandler(io.Discard, nil))) }

// start runs the worker and returns a stop func that cancels and waits for Run to return.
func start(t *testing.T, w *Worker) (stop func() error) {
	t.Helper()
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- w.Run(ctx) }()
	return func() error {
		cancel()
		select {
		case err := <-done:
			return err
		case <-time.After(10 * time.Second):
			t.Fatal("worker did not stop")
			return nil
		}
	}
}

func waitFor(t *testing.T, what string, cond func() bool) {
	t.Helper()
	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		if cond() {
			return
		}
		time.Sleep(5 * time.Millisecond)
	}
	t.Fatalf("timed out waiting for %s", what)
}

func reg(t *testing.T, typ string, h Handler) *Registry {
	t.Helper()
	r := NewRegistry()
	if err := r.Register(typ, h); err != nil {
		t.Fatal(err)
	}
	return r
}

func TestOutcomes(t *testing.T) {
	tests := []struct {
		name       string
		job        job.Job
		handler    Handler
		wantStatus job.Status // "" = success (Complete)
		wantDelay  time.Duration
		wantOut    string
		errContain string
	}{
		{
			name: "success", job: job.Job{ID: "ok", Type: "t"},
			handler: func(context.Context, job.Job) error { return nil },
			wantOut: OutcomeSucceeded,
		},
		{
			name: "error with attempts left retries with backoff", job: job.Job{ID: "r", Type: "t", Attempts: 2, MaxAttempts: 5},
			handler:    func(context.Context, job.Job) error { return errors.New("boom") },
			wantStatus: job.StatusRetrying, wantDelay: 10 * time.Second, wantOut: OutcomeRetry, errContain: "boom",
		},
		{
			name: "error on last attempt is dead-lettered", job: job.Job{ID: "d", Type: "t", Attempts: 3, MaxAttempts: 3},
			handler:    func(context.Context, job.Job) error { return errors.New("boom") },
			wantStatus: job.StatusDead, wantOut: OutcomeDead,
		},
		{
			name: "permanent error is not retried", job: job.Job{ID: "p", Type: "t", Attempts: 1, MaxAttempts: 5},
			handler:    func(context.Context, job.Job) error { return Permanent(errors.New("bad payload")) },
			wantStatus: job.StatusFailed, wantOut: OutcomeFailed, errContain: "bad payload",
		},
		{
			name: "unknown type is permanent", job: job.Job{ID: "u", Type: "nope"},
			handler:    func(context.Context, job.Job) error { return nil },
			wantStatus: job.StatusFailed, wantOut: OutcomeFailed, errContain: "no handler registered",
		},
		{
			name: "panic becomes a retryable error", job: job.Job{ID: "pa", Type: "t"},
			handler:    func(context.Context, job.Job) error { panic("kaboom") },
			wantStatus: job.StatusRetrying, wantDelay: 5 * time.Second, wantOut: OutcomeRetry, errContain: "kaboom",
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			fs := newFake(tt.job)
			obs := &obsRec{}
			w := New(fs, reg(t, "t", tt.handler), testCfg(), quiet(), WithObserver(obs))
			stop := start(t, w)
			waitFor(t, "finalise", func() bool { return fs.finalised() == 1 })
			if err := stop(); err != nil {
				t.Fatalf("Run: %v", err)
			}
			if !obs.has(tt.wantOut) {
				t.Fatalf("outcomes=%v want %s", obs.outcomes, tt.wantOut)
			}
			if tt.wantStatus == "" {
				if len(fs.completed) != 1 {
					t.Fatalf("expected Complete, got failed=%v", fs.failed)
				}
				return
			}
			p, ok := fs.failed[tt.job.ID]
			if !ok || p.Status != tt.wantStatus {
				t.Fatalf("fail params %+v ok=%v want status %s", p, ok, tt.wantStatus)
			}
			if tt.wantStatus == job.StatusRetrying && p.Delay != tt.wantDelay {
				t.Fatalf("delay %v want %v", p.Delay, tt.wantDelay)
			}
			if tt.errContain != "" && !contains(p.Err, tt.errContain) {
				t.Fatalf("err %q should contain %q", p.Err, tt.errContain)
			}
		})
	}
}

func contains(s, sub string) bool {
	for i := 0; i+len(sub) <= len(s); i++ {
		if s[i:i+len(sub)] == sub {
			return true
		}
	}
	return false
}

func TestPerJobTimeoutCancelsHandler(t *testing.T) {
	one := 1 // second
	fs := newFake(job.Job{ID: "slow", Type: "t", TimeoutSeconds: &one})
	cfg := testCfg()
	cfg.DefaultTimeout = time.Hour // prove the per-job value wins
	var sawCancel atomic.Bool
	h := func(ctx context.Context, _ job.Job) error {
		<-ctx.Done()
		sawCancel.Store(true)
		return ctx.Err()
	}
	w := New(fs, reg(t, "t", h), cfg, quiet())
	stop := start(t, w)
	waitFor(t, "timeout failure", func() bool { return fs.finalised() == 1 })
	_ = stop()
	p := fs.failed["slow"]
	if !sawCancel.Load() || p.Status != job.StatusRetrying || !contains(p.Err, "timed out after 1s") {
		t.Fatalf("saw=%v params=%+v", sawCancel.Load(), p)
	}
}

func TestLeaseLostCancelsHandlerAndDiscardsResult(t *testing.T) {
	fs := newFake(job.Job{ID: "z", Type: "t"})
	fs.hbErr = queue.ErrLeaseLost
	obs := &obsRec{}
	h := func(ctx context.Context, _ job.Job) error {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(5 * time.Second):
			return nil
		}
	}
	w := New(fs, reg(t, "t", h), testCfg(), quiet(), WithObserver(obs))
	stop := start(t, w)
	waitFor(t, "lease_lost outcome", func() bool { return obs.has(OutcomeLeaseLost) })
	_ = stop()
	if fs.finalised() != 0 {
		t.Fatalf("a worker that lost its lease must not write a result: %+v", fs)
	}
}

func TestCompleteRejectedMeansLeaseLost(t *testing.T) {
	fs := newFake(job.Job{ID: "x", Type: "t"})
	fs.completeE = queue.ErrLeaseLost
	obs := &obsRec{}
	w := New(fs, reg(t, "t", func(context.Context, job.Job) error { return nil }), testCfg(), quiet(), WithObserver(obs))
	stop := start(t, w)
	waitFor(t, "lease_lost", func() bool { return obs.has(OutcomeLeaseLost) })
	_ = stop()
}

func TestHeartbeatsRunWhileHandlerWorks(t *testing.T) {
	fs := newFake(job.Job{ID: "h", Type: "t"})
	h := func(ctx context.Context, _ job.Job) error {
		time.Sleep(150 * time.Millisecond)
		return nil
	}
	w := New(fs, reg(t, "t", h), testCfg(), quiet())
	stop := start(t, w)
	waitFor(t, "complete", func() bool { return fs.finalised() == 1 })
	_ = stop()
	if n := fs.hbCalls.Load(); n < 3 {
		t.Fatalf("expected several heartbeats in 150ms at 20ms interval, got %d", n)
	}
}

func TestGracefulShutdownLetsInFlightJobsFinish(t *testing.T) {
	fs := newFake(job.Job{ID: "g", Type: "t"})
	started := make(chan struct{})
	h := func(ctx context.Context, _ job.Job) error {
		close(started)
		time.Sleep(200 * time.Millisecond) // deliberately ignores ctx; finishes within the grace
		return nil
	}
	w := New(fs, reg(t, "t", h), testCfg(), quiet())
	stop := start(t, w)
	<-started
	if err := stop(); err != nil { // cancel while the job is mid-flight
		t.Fatalf("Run: %v", err)
	}
	if len(fs.completed) != 1 {
		t.Fatalf("in-flight job must complete during drain: completed=%v released=%v", fs.completed, fs.released)
	}
}

func TestForcedShutdownReleasesStragglers(t *testing.T) {
	fs := newFake(job.Job{ID: "f", Type: "t"})
	started := make(chan struct{})
	h := func(ctx context.Context, _ job.Job) error {
		close(started)
		<-ctx.Done() // cooperative: stops when told to
		return ctx.Err()
	}
	cfg := testCfg()
	cfg.ShutdownGrace = 100 * time.Millisecond
	w := New(fs, reg(t, "t", h), cfg, quiet())
	stop := start(t, w)
	<-started
	if err := stop(); err != nil {
		t.Fatalf("Run: %v", err)
	}
	if len(fs.released) != 1 || len(fs.failed) != 0 {
		t.Fatalf("straggler must be released, not failed: released=%v failed=%v", fs.released, fs.failed)
	}
}

func TestConcurrencyLimitRespected(t *testing.T) {
	var jobs []job.Job
	for i := 0; i < 12; i++ {
		jobs = append(jobs, job.Job{ID: string(rune('a' + i)), Type: "t"})
	}
	fs := newFake(jobs...)
	cfg := testCfg()
	cfg.Concurrency = 3
	var cur, peak atomic.Int32
	h := func(context.Context, job.Job) error {
		n := cur.Add(1)
		for {
			p := peak.Load()
			if n <= p || peak.CompareAndSwap(p, n) {
				break
			}
		}
		time.Sleep(20 * time.Millisecond)
		cur.Add(-1)
		return nil
	}
	w := New(fs, reg(t, "t", h), cfg, quiet())
	stop := start(t, w)
	waitFor(t, "all complete", func() bool { return fs.finalised() == 12 })
	_ = stop()
	if p := peak.Load(); p > 3 || p < 2 {
		t.Fatalf("peak concurrency %d, want within [2,3]", p)
	}
}

func TestRegistry(t *testing.T) {
	r := NewRegistry()
	h := func(context.Context, job.Job) error { return nil }
	if err := r.Register("a", h); err != nil {
		t.Fatal(err)
	}
	if err := r.Register("a", h); err == nil {
		t.Fatal("duplicate registration must fail")
	}
	if err := r.Register("", h); err == nil {
		t.Fatal("empty type must fail")
	}
	if err := r.Register("b", nil); err == nil {
		t.Fatal("nil handler must fail")
	}
	if _, ok := r.Lookup("a"); !ok {
		t.Fatal("lookup a")
	}
	if _, ok := r.Lookup("zzz"); ok {
		t.Fatal("lookup zzz should miss")
	}
}

func TestPermanentWrapping(t *testing.T) {
	base := errors.New("x")
	if Permanent(nil) != nil {
		t.Fatal("Permanent(nil) must be nil")
	}
	if !IsPermanent(Permanent(base)) || IsPermanent(base) {
		t.Fatal("IsPermanent mismatch")
	}
	if !errors.Is(Permanent(base), base) {
		t.Fatal("Permanent must unwrap")
	}
	wrapped := errors.Join(errors.New("ctx"), Permanent(base))
	if !IsPermanent(wrapped) {
		t.Fatal("IsPermanent must see through wrapping")
	}
}

func TestConfigDefaults(t *testing.T) {
	c := Config{}.withDefaults()
	if c.ID == "" || c.Concurrency != 10 || c.BatchSize != 10 || c.Lease != 30*time.Second ||
		c.HeartbeatInterval != 10*time.Second || len(c.Queues) != 1 || c.Backoff.Base == 0 {
		t.Fatalf("defaults: %+v", c)
	}
}

type primerObs struct {
	obsRec
	queues, types []string
}

func (p *primerObs) Prime(queues, types []string) { p.queues, p.types = queues, types }

// A worker tells a Primer observer which queues it polls and which job types it can run, before any event, so
// metrics series exist at zero (see metrics.workerObserver.Prime).
func TestNewPrimesObserver(t *testing.T) {
	obs := &primerObs{}
	r := NewRegistry()
	_ = r.Register("a", func(context.Context, job.Job) error { return nil })
	_ = r.Register("b", func(context.Context, job.Job) error { return nil })
	cfg := testCfg()
	cfg.Queues = []string{"q1", "q2"}
	New(newFake(), r, cfg, quiet(), WithObserver(obs))
	if len(obs.queues) != 2 || obs.queues[0] != "q1" || len(obs.types) != 2 {
		t.Fatalf("Prime got queues=%v types=%v", obs.queues, obs.types)
	}
}

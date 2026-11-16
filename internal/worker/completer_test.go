package worker

import (
	"context"
	"errors"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/vasistamagam/distributed-jobscheduler/internal/job"
	"github.com/vasistamagam/distributed-jobscheduler/internal/queue"
)

// batchStore records every CompleteBatch call.
type batchStore struct {
	mu      sync.Mutex
	batches [][]job.Ref
	lost    map[string]bool // ids whose lease is "lost"
	err     error           // statement-level failure
	delay   time.Duration   // simulated commit latency
}

func (b *batchStore) CompleteBatch(_ context.Context, refs []job.Ref) ([]bool, error) {
	time.Sleep(b.delay)
	b.mu.Lock()
	defer b.mu.Unlock()
	b.batches = append(b.batches, append([]job.Ref(nil), refs...))
	if b.err != nil {
		return nil, b.err
	}
	ok := make([]bool, len(refs))
	for i, r := range refs {
		ok[i] = !b.lost[r.ID]
	}
	return ok, nil
}

func (b *batchStore) sizes() []int {
	b.mu.Lock()
	defer b.mu.Unlock()
	out := make([]int, len(b.batches))
	for i, x := range b.batches {
		out[i] = len(x)
	}
	return out
}

func neverDirect(t *testing.T) func() error {
	return func() error { t.Error("direct write must not be used"); return nil }
}

// Results that arrive together are committed together, and every caller gets its own answer.
func TestCompleterCoalescesConcurrentResults(t *testing.T) {
	bs := &batchStore{delay: 40 * time.Millisecond} // results pile up behind the write in flight
	c := newCompleter(bs, 0, 100, nil)
	defer c.stop()

	const n = 10
	var wg sync.WaitGroup
	errs := make([]error, n)
	for i := 0; i < n; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			errs[i] = c.complete(context.Background(), job.Ref{ID: string(rune('a' + i)), WorkerID: "w", Attempts: 1}, neverDirect(t))
		}()
	}
	wg.Wait()
	for i, e := range errs {
		if e != nil {
			t.Errorf("job %d: %v", i, e)
		}
	}
	total := 0
	for _, s := range bs.sizes() {
		total += s
	}
	if total != n {
		t.Fatalf("committed %d results, want %d", total, n)
	}
	if got := len(bs.sizes()); got > 3 {
		t.Fatalf("expected the %d results to coalesce into a few statements, got %d: %v", n, got, bs.sizes())
	}
}

// With no linger, an idle worker adds no latency: a lone result is written at once.
func TestCompleterLoneResultIsImmediateWithoutLinger(t *testing.T) {
	bs := &batchStore{}
	c := newCompleter(bs, 0, 100, nil)
	defer c.stop()
	start := time.Now()
	if err := c.complete(context.Background(), job.Ref{ID: "x"}, neverDirect(t)); err != nil {
		t.Fatal(err)
	}
	if d := time.Since(start); d > 500*time.Millisecond {
		t.Fatalf("lone result took %v with no linger", d)
	}
}

// A lone result is not held forever: with a linger it is flushed when the linger ends.
func TestCompleterFlushesLoneResultAfterLinger(t *testing.T) {
	bs := &batchStore{}
	c := newCompleter(bs, 30*time.Millisecond, 100, nil)
	defer c.stop()

	start := time.Now()
	if err := c.complete(context.Background(), job.Ref{ID: "x"}, neverDirect(t)); err != nil {
		t.Fatal(err)
	}
	if d := time.Since(start); d < 25*time.Millisecond || d > 2*time.Second {
		t.Fatalf("lone result took %v, want about the 30ms window", d)
	}
}

// Reaching the max size flushes immediately instead of waiting out the window.
func TestCompleterFlushesWhenFull(t *testing.T) {
	bs := &batchStore{}
	c := newCompleter(bs, time.Hour, 3, nil) // the linger would never end
	defer c.stop()

	var wg sync.WaitGroup
	for i := 0; i < 3; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			_ = c.complete(context.Background(), job.Ref{ID: string(rune('a' + i))}, neverDirect(t))
		}()
	}
	done := make(chan struct{})
	go func() { wg.Wait(); close(done) }()
	select {
	case <-done:
	case <-time.After(5 * time.Second):
		t.Fatal("a full batch must flush without waiting for the window")
	}
	if s := bs.sizes(); len(s) != 1 || s[0] != 3 {
		t.Fatalf("batches = %v, want [3]", s)
	}
}

// One job's lost lease must not turn its neighbours' results into failures.
func TestCompleterLostLeaseIsPerJob(t *testing.T) {
	bs := &batchStore{lost: map[string]bool{"b": true}}
	c := newCompleter(bs, 50*time.Millisecond, 100, nil)
	defer c.stop()

	var wg sync.WaitGroup
	res := map[string]error{}
	var mu sync.Mutex
	for _, id := range []string{"a", "b", "c"} {
		wg.Add(1)
		go func() {
			defer wg.Done()
			err := c.complete(context.Background(), job.Ref{ID: id}, neverDirect(t))
			mu.Lock()
			res[id] = err
			mu.Unlock()
		}()
	}
	wg.Wait()
	if res["a"] != nil || res["c"] != nil {
		t.Fatalf("healthy neighbours failed: %v", res)
	}
	if !errors.Is(res["b"], queue.ErrLeaseLost) {
		t.Fatalf("b must report ErrLeaseLost, got %v", res["b"])
	}
}

// A statement-level error reaches every waiter (nothing was committed), so the worker's
// existing retry applies; a later flush can then succeed.
func TestCompleterStatementErrorReachesAllWaiters(t *testing.T) {
	bs := &batchStore{err: errors.New("connection reset")}
	c := newCompleter(bs, 20*time.Millisecond, 100, nil)
	defer c.stop()

	var wg sync.WaitGroup
	var failed atomic.Int32
	for i := 0; i < 4; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			if err := c.complete(context.Background(), job.Ref{ID: string(rune('a' + i))}, neverDirect(t)); err != nil &&
				!errors.Is(err, queue.ErrLeaseLost) {
				failed.Add(1)
			}
		}()
	}
	wg.Wait()
	if failed.Load() != 4 {
		t.Fatalf("%d of 4 waiters saw the error", failed.Load())
	}
	bs.mu.Lock()
	bs.err = nil
	bs.mu.Unlock()
	if err := c.complete(context.Background(), job.Ref{ID: "z"}, neverDirect(t)); err != nil {
		t.Fatalf("after recovery: %v", err)
	}
}

// stop() flushes work that is already waiting, and later callers fall back to a direct write
// (a handler that ignored cancellation must never block forever).
func TestCompleterStopFlushesThenFallsBack(t *testing.T) {
	bs := &batchStore{}
	c := newCompleter(bs, time.Hour, 100, nil) // the long linger means only stop() can trigger the flush
	var wg sync.WaitGroup
	var err error
	wg.Add(1)
	go func() { defer wg.Done(); err = c.complete(context.Background(), job.Ref{ID: "a"}, neverDirect(t)) }()
	time.Sleep(30 * time.Millisecond) // let the request reach the loop and start waiting out its window
	c.stop()
	wg.Wait()
	if err != nil {
		t.Fatalf("pending result was not flushed on stop: %v", err)
	}

	var direct atomic.Bool
	if err := c.complete(context.Background(), job.Ref{ID: "late"}, func() error { direct.Store(true); return nil }); err != nil || !direct.Load() {
		t.Fatalf("late completion must use the direct path: err=%v direct=%v", err, direct.Load())
	}
}

// A cancelled context frees a caller that has not been accepted yet.
func TestCompleterCallerContextCancel(t *testing.T) {
	bs := &batchStore{}
	c := newCompleter(bs, time.Hour, 1, nil)
	defer c.stop()
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	// Either accepted (and flushed because max=1) or cancelled; it must not hang.
	done := make(chan struct{})
	go func() { _ = c.complete(ctx, job.Ref{ID: "a"}, func() error { return nil }); close(done) }()
	select {
	case <-done:
	case <-time.After(5 * time.Second):
		t.Fatal("complete hung with a cancelled context")
	}
}

// batchingFake is the worker fake plus batch support, so a whole worker can run with batching.
type batchingFake struct {
	*fakeStore
	bs *batchStore
}

func (b *batchingFake) CompleteBatch(ctx context.Context, refs []job.Ref) ([]bool, error) {
	ok, err := b.bs.CompleteBatch(ctx, refs)
	if err == nil {
		b.mu.Lock()
		for i, r := range refs {
			if ok[i] {
				b.completed = append(b.completed, r.ID)
			}
		}
		b.mu.Unlock()
	}
	return ok, err
}

// End to end through the worker: successes are recorded via the batch path, every job is
// recorded exactly once, a lost lease is reported as lease_lost, and none of the single-write
// Complete calls are used.
func TestWorkerBatchesSuccesses(t *testing.T) {
	jobs := make([]job.Job, 12)
	for i := range jobs {
		jobs[i] = job.Job{ID: string(rune('a' + i)), Type: "t"}
	}
	fs := newFake(jobs...)
	fs.completeE = errors.New("single Complete must not be used when batching is on")
	bf := &batchingFake{fakeStore: fs, bs: &batchStore{lost: map[string]bool{"c": true}}}
	obs := &obsRec{}

	cfg := testCfg()
	cfg.CompleteBatch = true
	w := New(bf, reg(t, "t", func(context.Context, job.Job) error { return nil }), cfg, quiet(), WithObserver(obs))
	stop := start(t, w)
	waitFor(t, "all jobs recorded", func() bool {
		fs.mu.Lock()
		defer fs.mu.Unlock()
		return len(fs.completed) == 11
	})
	if err := stop(); err != nil {
		t.Fatal(err)
	}
	if !obs.has(OutcomeLeaseLost) || !obs.has(OutcomeSucceeded) {
		t.Fatalf("outcomes: %v", obs.outcomes)
	}
	seen := map[string]int{}
	for _, id := range fs.completed {
		seen[id]++
	}
	for id, n := range seen {
		if n != 1 || id == "c" {
			t.Fatalf("job %q recorded %d times (c must be rejected): %v", id, n, seen)
		}
	}
}

// With a Store that cannot batch, the setting is ignored and writes go through Complete.
func TestWorkerIgnoresBatchWindowWithoutBatchStore(t *testing.T) {
	fs := newFake(job.Job{ID: "a", Type: "t"})
	cfg := testCfg()
	cfg.CompleteBatch = true
	w := New(fs, reg(t, "t", func(context.Context, job.Job) error { return nil }), cfg, quiet())
	stop := start(t, w)
	waitFor(t, "complete", func() bool { return fs.finalised() == 1 })
	if err := stop(); err != nil {
		t.Fatal(err)
	}
}

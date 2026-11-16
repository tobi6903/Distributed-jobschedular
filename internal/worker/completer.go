package worker

import (
	"context"
	"fmt"
	"sync"
	"time"

	"github.com/vasistamagam/distributed-jobscheduler/internal/job"
	"github.com/vasistamagam/distributed-jobscheduler/internal/queue"
)

// BatchCompleter is implemented by stores that can record many successes in one commit.
// It is optional: a Store without it simply completes jobs one at a time.
//
// ok is aligned with refs; ok[i]==false means that job's lease was lost (the fence did not
// match). A non-nil error means nothing was committed.
type BatchCompleter interface {
	CompleteBatch(ctx context.Context, refs []job.Ref) (ok []bool, err error)
}

// completer coalesces the "job succeeded" writes of one worker into batches (group commit).
//
// Why: every job costs one durable commit (an fsync). One Postgres commits for the whole fleet,
// so with many workers the commit rate, not CPU, caps throughput. Writing N results in one
// statement pays for one commit instead of N.
//
// How: a single flusher goroutine writes whatever has accumulated, then immediately writes
// whatever accumulated while that write was in flight. There is no timer by default, so an
// idle worker adds no latency (a lone result is written at once) and under load the batch size
// adapts by itself to the commit latency, the same idea as Postgres's own group commit.
// linger > 0 adds an optional extra wait to fill a batch; it costs latency on every job, and the
// measurements in docs/perf show it hurts at low worker counts.
//
// What it does NOT change:
//   - A job's execution slot (and its lease) is held until its result is committed. complete()
//     blocks the calling job goroutine, so shutdown draining, concurrency limits and the reaper
//     behave exactly as without batching.
//   - The fence. Each row is still conditioned on (worker, attempts); a lost lease affects only
//     that job, never its batch neighbours.
//   - Failure handling. A statement-level error is returned to every waiter, and the worker's
//     existing write() retry runs again per job.
//
// The cost: a successful job is recorded a little later than it would be (at most one write plus
// the linger), so a crash in that window re-runs the job. That is inside the at-least-once
// contract, and the lease is far longer than the window.
type completer struct {
	store   BatchCompleter
	linger  time.Duration
	max     int
	onFlush func(size int)

	in      chan completion
	stopped chan struct{}
	wg      sync.WaitGroup
	once    sync.Once
}

type completion struct {
	ref job.Ref
	res chan error // buffered(1): the flusher never blocks on a waiter
}

func newCompleter(store BatchCompleter, linger time.Duration, max int, onFlush func(int)) *completer {
	if max < 1 {
		max = 1
	}
	if onFlush == nil {
		onFlush = func(int) {}
	}
	c := &completer{
		store: store, linger: linger, max: max, onFlush: onFlush,
		in:      make(chan completion),
		stopped: make(chan struct{}),
	}
	c.wg.Add(1)
	go c.loop()
	return c
}

// complete records ref as succeeded and returns when the batch containing it has been
// committed. It returns queue.ErrLeaseLost if the fence rejected it.
//
// If the completer has been stopped it writes directly, so a late finisher (a handler that
// ignored cancellation) is never stranded.
func (c *completer) complete(ctx context.Context, ref job.Ref, direct func() error) error {
	req := completion{ref: ref, res: make(chan error, 1)}
	select {
	case c.in <- req:
	case <-c.stopped:
		return direct()
	case <-ctx.Done():
		return ctx.Err()
	}
	// Once accepted, the flusher owns the request and always answers it (stop flushes first).
	return <-req.res
}

// stop flushes what is pending and ends the loop. Safe to call more than once.
func (c *completer) stop() {
	c.once.Do(func() { close(c.stopped) })
	c.wg.Wait()
}

func (c *completer) loop() {
	defer c.wg.Done()
	for {
		var batch []completion
		select {
		case first := <-c.in:
			batch = append(batch, first)
		case <-c.stopped:
			return
		}

		// Take everything already waiting: it arrived while the previous write was in flight.
	drain:
		for len(batch) < c.max {
			select {
			case r := <-c.in:
				batch = append(batch, r)
			default:
				break drain
			}
		}
		// Optionally wait a little longer for stragglers.
		if c.linger > 0 && len(batch) < c.max {
			timer := time.NewTimer(c.linger)
		collect:
			for len(batch) < c.max {
				select {
				case r := <-c.in:
					batch = append(batch, r)
				case <-timer.C:
					break collect
				case <-c.stopped:
					break collect
				}
			}
			timer.Stop()
		}
		c.flush(batch)
	}
}

func (c *completer) flush(batch []completion) {
	refs := make([]job.Ref, len(batch))
	for i, r := range batch {
		refs[i] = r.ref
	}
	// Detached from any job or shutdown context: a result that is ready must be written.
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()
	ok, err := c.store.CompleteBatch(ctx, refs)
	if err == nil && len(ok) != len(batch) {
		err = fmt.Errorf("complete batch: store returned %d results for %d jobs", len(ok), len(batch))
	}
	c.onFlush(len(batch))
	for i, r := range batch {
		switch {
		case err != nil:
			r.res <- err
		case !ok[i]:
			r.res <- fmt.Errorf("complete: %w", queue.ErrLeaseLost)
		default:
			r.res <- nil
		}
	}
}

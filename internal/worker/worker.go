// Package worker claims jobs from the store and executes them via a handler registry.
//
// Lifecycle of one job inside a worker:
//
//	claim (batch) -> execute: start heartbeat, run handler under timeout, stop heartbeat
//	              -> finalize: complete | fail(retry/dead/failed) | release | discard (lease lost)
//
// Every finalizing write is fenced by the claim's (worker, attempts) token, so a worker that
// lost its lease can never overwrite the result of whoever owns the job now.
package worker

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"math/rand/v2"
	"os"
	"runtime/debug"
	"sync"
	"time"

	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/codes"
	"go.opentelemetry.io/otel/propagation"
	"go.opentelemetry.io/otel/trace"

	"github.com/vasistamagam/distributed-jobscheduler/internal/job"
	"github.com/vasistamagam/distributed-jobscheduler/internal/queue"
)

// Store is the subset of queue.Store the worker needs. Defining it here (consumer side)
// keeps the worker unit-testable with an in-memory fake.
type Store interface {
	Claim(ctx context.Context, queue, workerID string, limit int, lease time.Duration) ([]job.Job, error)
	Heartbeat(ctx context.Context, ref job.Ref, lease time.Duration) error
	Complete(ctx context.Context, ref job.Ref) error
	Fail(ctx context.Context, ref job.Ref, p queue.FailParams) error
	Release(ctx context.Context, ref job.Ref) error
	Reap(ctx context.Context, limit int) (queue.ReapResult, error)
	DeleteFinished(ctx context.Context, retention time.Duration, limit int) (int, error)
}

// Outcome labels for metrics and logs.
const (
	OutcomeSucceeded = "succeeded"
	OutcomeRetry     = "retry"
	OutcomeDead      = "dead"
	OutcomeFailed    = "failed"
	OutcomeLeaseLost = "lease_lost"
	OutcomeReleased  = "released"
)

// Observer receives metric events. It is an interface so this package does not depend on
// Prometheus; see internal/metrics for the real implementation.
type Observer interface {
	Claimed(queue string, n int, pickupLatencies []time.Duration)
	Finished(queue, jobType, outcome string, d time.Duration)
	Reaped(retried, dead int)
	Deleted(n int)
	// CompleteBatch reports one flush of batched success writes (size = jobs in the statement).
	CompleteBatch(size int)
	// ClaimPoll reports one claim query against a queue and how many jobs it returned (0 = an empty poll).
	// Empty polls are the idle load a worker puts on the database.
	ClaimPoll(queue string, got int)
	// Wakeup reports that an idle worker was woken by a notification, and whether the claim it triggered
	// found work (false = it lost the race to another worker, or the hint was stale).
	Wakeup(claimed bool)
}

type nopObserver struct{}

func (nopObserver) Claimed(string, int, []time.Duration)           {}
func (nopObserver) Finished(string, string, string, time.Duration) {}
func (nopObserver) Reaped(int, int)                                {}
func (nopObserver) CompleteBatch(int)                              {}
func (nopObserver) Wakeup(bool)                                    {}
func (nopObserver) ClaimPoll(string, int)                          {}
func (nopObserver) Deleted(int)                                    {}

// Primer is an optional Observer extension: it is told which queues and job types this worker serves so it can
// create their series before the first event (see metrics.workerObserver.Prime).
type Primer interface {
	Prime(queues, types []string)
}

// WakeSource tells an idle worker that work may be available now, ahead of its next poll. It is a hint:
// the worker still claims with the same query, and still polls (see Config.FallbackPoll).
type WakeSource interface {
	Wake() <-chan struct{} // a value means "look now"; many hints coalesce into one
	Healthy() bool         // false while the source cannot deliver hints (the worker then polls normally)
}

// Config tunes a worker. Zero values are replaced by defaults (see withDefaults).
type Config struct {
	ID           string        // unique per process; stored in locked_by
	Queues       []string      // polled in order: earlier queues have strict priority
	Concurrency  int           // max jobs executing at once
	BatchSize    int           // max jobs claimed per round trip
	PollInterval time.Duration // idle sleep between empty polls (jittered)
	// Wake, if set, ends an idle sleep early when a notification arrives (nil: poll only).
	Wake WakeSource
	// FallbackPoll is the idle sleep while Wake is healthy: the polling that remains is only a safety net
	// for lost notifications, and the only way to find delayed and retried jobs (nothing announces those).
	// Default PollInterval, so enabling Wake never makes pickup slower; raise it to cut idle queries.
	FallbackPoll      time.Duration
	Lease             time.Duration // visibility timeout
	HeartbeatInterval time.Duration // must be well under Lease; default Lease/3
	DefaultTimeout    time.Duration // per-job timeout when the job has none
	ShutdownGrace     time.Duration // how long to wait for in-flight jobs on shutdown
	ReapInterval      time.Duration // how often to recover expired leases
	Retention         time.Duration // 0 disables deletion of finished jobs
	RetentionInterval time.Duration
	Backoff           job.Backoff

	// CompleteBatch groups "succeeded" writes into one commit (group commit: no timer, results
	// that pile up while a write is in flight go out together). Needs a Store that implements
	// BatchCompleter, otherwise it is ignored. Off by default.
	CompleteBatch bool
	// CompleteBatchLinger optionally waits this long to fill a batch. Default 0: no waiting.
	CompleteBatchLinger time.Duration
	CompleteBatchMax    int // most results per statement; default Concurrency
}

func (c Config) withDefaults() Config {
	if c.ID == "" {
		c.ID = NewID()
	}
	if len(c.Queues) == 0 {
		c.Queues = []string{job.DefaultQueue}
	}
	def := func(d *time.Duration, v time.Duration) {
		if *d == 0 {
			*d = v
		}
	}
	if c.Concurrency <= 0 {
		c.Concurrency = 10
	}
	if c.BatchSize <= 0 {
		c.BatchSize = c.Concurrency
	}
	def(&c.PollInterval, time.Second)
	def(&c.FallbackPoll, c.PollInterval)
	def(&c.Lease, 30*time.Second)
	def(&c.HeartbeatInterval, c.Lease/3)
	def(&c.DefaultTimeout, 5*time.Minute)
	def(&c.ShutdownGrace, 30*time.Second)
	def(&c.ReapInterval, 10*time.Second)
	def(&c.RetentionInterval, time.Minute)
	if c.CompleteBatchMax <= 0 {
		c.CompleteBatchMax = c.Concurrency
	}
	if c.Backoff.Base == 0 {
		c.Backoff = job.DefaultBackoff()
	}
	return c
}

// NewID returns "<hostname>-<pid>-<random>", unique per process incarnation. The random
// suffix matters: a restarted container can reuse hostname and pid.
func NewID() string {
	h, _ := os.Hostname()
	return fmt.Sprintf("%s-%d-%04x", h, os.Getpid(), rand.IntN(1<<16))
}

// Worker is one worker process (or, in tests, one of many in the same process).
type Worker struct {
	store    Store
	registry *Registry
	cfg      Config
	log      *slog.Logger
	obs      Observer
	tracer   trace.Tracer
	rng      *rand.Rand
	batcher  *completer // nil when batching is off
}

// Option customises a Worker.
type Option func(*Worker)

func WithLogger(l *slog.Logger) Option { return func(w *Worker) { w.log = l } }
func WithObserver(o Observer) Option   { return func(w *Worker) { w.obs = o } }
func WithTracer(t trace.Tracer) Option { return func(w *Worker) { w.tracer = t } }

// New builds a worker. It does not start anything; call Run.
func New(store Store, reg *Registry, cfg Config, opts ...Option) *Worker {
	w := &Worker{
		store:    store,
		registry: reg,
		cfg:      cfg.withDefaults(),
		log:      slog.Default(),
		obs:      nopObserver{},
		tracer:   otel.Tracer("jobscheduler/worker"),
		rng:      rand.New(rand.NewPCG(rand.Uint64(), rand.Uint64())),
	}
	for _, o := range opts {
		o(w)
	}
	w.log = w.log.With("worker_id", w.cfg.ID)
	if p, ok := w.obs.(Primer); ok {
		p.Prime(w.cfg.Queues, reg.Types())
	}
	return w
}

// ID returns the worker's id.
func (w *Worker) ID() string { return w.cfg.ID }

// Causes attached to cancelled contexts so finalisation can tell *why* a handler stopped.
var (
	errLeaseLost = errors.New("lease lost")
	errTimeout   = errors.New("job timeout")
	errShutdown  = errors.New("worker shutting down")
)

// Run polls until ctx is cancelled (e.g. SIGTERM), then drains in-flight jobs.
//
// Shutdown sequence:
//  1. ctx cancelled: stop claiming. In-flight handlers keep running on a separate context.
//  2. Wait up to ShutdownGrace for them to finish normally.
//  3. Still running: cancel them (cause errShutdown). They are released back to the queue
//     without consuming an attempt.
//  4. A handler that ignores cancellation is abandoned after a short extra wait; its lease
//     expires and the reaper on another worker recovers the job.
func (w *Worker) Run(ctx context.Context) error {
	// Handlers run on a context that is NOT cancelled when ctx is, so SIGTERM does not kill them.
	jobCtx, cancelJobs := context.WithCancelCause(context.WithoutCancel(ctx))
	defer cancelJobs(nil)

	var maint sync.WaitGroup
	maintCtx, stopMaint := context.WithCancel(ctx)
	maint.Add(1)
	go func() { defer maint.Done(); w.maintenance(maintCtx) }()

	if bc, ok := w.store.(BatchCompleter); ok && w.cfg.CompleteBatch {
		w.batcher = newCompleter(bc, w.cfg.CompleteBatchLinger, w.cfg.CompleteBatchMax, w.obs.CompleteBatch)
	}

	sem := make(chan struct{}, w.cfg.Concurrency)
	var inflight sync.WaitGroup
	w.pollLoop(ctx, jobCtx, sem, &inflight)

	w.log.Info("draining in-flight jobs", "grace", w.cfg.ShutdownGrace.String())
	done := make(chan struct{})
	go func() { inflight.Wait(); close(done) }()

	var err error
	select {
	case <-done:
	case <-time.After(w.cfg.ShutdownGrace):
		w.log.Warn("shutdown grace elapsed; cancelling in-flight jobs")
		cancelJobs(errShutdown)
		select {
		case <-done:
		case <-time.After(5 * time.Second):
			err = errors.New("handlers ignored cancellation; abandoned (leases will expire)")
		}
	}
	// Handlers are done (or abandoned: a late finisher falls back to a direct write), so
	// flush whatever results are still waiting.
	if w.batcher != nil {
		w.batcher.stop()
	}
	stopMaint()
	maint.Wait()
	w.log.Info("worker stopped")
	return err
}

type claimedJob struct {
	job        job.Job
	claimStart time.Time
	claimEnd   time.Time
	batch      int
}

func (w *Worker) pollLoop(ctx, jobCtx context.Context, sem chan struct{}, inflight *sync.WaitGroup) {
	woken := false // the previous idle wait was ended by a notification
	for ctx.Err() == nil {
		// Block until at least one execution slot is free, then grab as many more as are
		// immediately available (up to BatchSize) so we claim exactly what we can run.
		select {
		case sem <- struct{}{}:
		case <-ctx.Done():
			return
		}
		slots := 1
	grab:
		for slots < w.cfg.BatchSize {
			select {
			case sem <- struct{}{}:
				slots++
			default:
				break grab
			}
		}

		claimed, err := w.claim(ctx, slots)
		for i := len(claimed); i < slots; i++ { // return unused slots
			<-sem
		}
		for _, c := range claimed {
			inflight.Add(1)
			go func() {
				defer inflight.Done()
				defer func() { <-sem }()
				w.execute(jobCtx, c)
			}()
		}
		if woken {
			w.obs.Wakeup(len(claimed) > 0)
			woken = false
		}
		if err != nil || len(claimed) == 0 {
			woken = w.idle(ctx, err != nil)
		}
	}
}

// idle waits for the next claim attempt and reports whether a notification (not the timer) ended the wait.
// While the wake source is healthy the timer is only a safety net (FallbackPoll); after an error, or while
// the source is down, the normal PollInterval applies.
func (w *Worker) idle(ctx context.Context, failed bool) (woken bool) {
	d := w.cfg.PollInterval
	var wake <-chan struct{}
	if w.cfg.Wake != nil {
		wake = w.cfg.Wake.Wake()
		if !failed && w.cfg.Wake.Healthy() {
			d = w.cfg.FallbackPoll
		}
	}
	t := time.NewTimer(time.Duration(float64(d) * (0.8 + 0.4*w.rng.Float64())))
	defer t.Stop()
	select {
	case <-ctx.Done():
	case <-t.C:
	case <-wake: // nil channel (no wake source) blocks forever, so this case is simply never taken
		return true
	}
	return false
}

// claim fills up to n slots, polling queues in configured order.
func (w *Worker) claim(ctx context.Context, n int) ([]claimedJob, error) {
	// A claim must not be cancelled midway by SIGTERM: the server may have already
	// committed it, which would strand the jobs until their lease expires.
	cctx, cancel := context.WithTimeout(context.WithoutCancel(ctx), 10*time.Second)
	defer cancel()

	var out []claimedJob
	for _, q := range w.cfg.Queues {
		if ctx.Err() != nil || len(out) >= n {
			break
		}
		start := time.Now()
		jobs, err := w.store.Claim(cctx, q, w.cfg.ID, n-len(out), w.cfg.Lease)
		end := time.Now()
		if err != nil {
			w.log.Error("claim failed", "queue", q, "err", err)
			return out, err
		}
		w.obs.ClaimPoll(q, len(jobs))
		w.log.Debug("claimed", "queue", q, "asked", n-len(out), "got", len(jobs), "took_ms", float64(end.Sub(start).Microseconds())/1000)
		if len(jobs) > 0 {
			lats := make([]time.Duration, len(jobs))
			for i, j := range jobs {
				lats[i] = j.PickupLatency
				out = append(out, claimedJob{job: j, claimStart: start, claimEnd: end, batch: len(jobs)})
			}
			w.obs.Claimed(q, len(jobs), lats)
		}
	}
	return out, nil
}

// execute runs one claimed job to a final state. It never returns an error: every failure
// is either recorded in the database or left for lease expiry to recover.
func (w *Worker) execute(parent context.Context, c claimedJob) {
	j := c.job
	ref := j.Ref()
	start := time.Now()

	// Join the trace started by the API request (stored in the row as a W3C traceparent).
	ctx := propagation.TraceContext{}.Extract(parent, propagation.MapCarrier{"traceparent": j.Traceparent})
	attrs := []attribute.KeyValue{
		attribute.String("job.id", j.ID), attribute.String("job.type", j.Type),
		attribute.String("job.queue", j.Queue), attribute.String("tenant.id", j.TenantID),
		attribute.Int("job.attempt", j.Attempts),
	}
	// Retroactive claim span: the claim happened before we knew this job's trace.
	_, claimSpan := w.tracer.Start(ctx, "job.claim", trace.WithTimestamp(c.claimStart),
		trace.WithAttributes(append(attrs, attribute.Int("claim.batch", c.batch),
			attribute.Float64("job.pickup_latency_seconds", j.PickupLatency.Seconds()))...))
	claimSpan.End(trace.WithTimestamp(c.claimEnd))
	ctx, span := w.tracer.Start(ctx, "job.execute", trace.WithAttributes(attrs...))
	defer span.End()

	log := w.log.With("job_id", j.ID, "tenant_id", j.TenantID, "type", j.Type, "queue", j.Queue, "attempt", j.Attempts)
	if sc := span.SpanContext(); sc.IsValid() {
		log = log.With("trace_id", sc.TraceID().String())
	}

	timeout := w.cfg.DefaultTimeout
	if j.TimeoutSeconds != nil {
		timeout = time.Duration(*j.TimeoutSeconds) * time.Second
	}
	runCtx, cancelRun := context.WithCancelCause(ctx)
	defer cancelRun(nil)
	tctx, cancelTimeout := context.WithTimeoutCause(runCtx, timeout, errTimeout)
	defer cancelTimeout()

	hbDone := make(chan struct{})
	go func() {
		defer close(hbDone)
		w.heartbeat(tctx, ref, log, cancelRun)
	}()

	err := w.invoke(tctx, j)
	// Read the cause BEFORE cancelling ourselves, otherwise we'd overwrite it.
	cause := context.Cause(tctx)
	cancelTimeout()
	<-hbDone

	// Finalisation must survive cancellation of the handler's context.
	fctx, cancelF := context.WithTimeout(context.WithoutCancel(parent), 15*time.Second)
	defer cancelF()
	// The result write gets its own span so the trace shows how long recording took.
	fctx, finSpan := w.tracer.Start(trace.ContextWithSpan(fctx, span), "job.complete")

	outcome := w.finalize(fctx, log, j, ref, err, cause, timeout)
	finSpan.SetAttributes(attribute.String("job.outcome", outcome))
	finSpan.End()
	dur := time.Since(start)
	w.obs.Finished(j.Queue, j.Type, outcome, dur)
	span.SetAttributes(attribute.String("job.outcome", outcome))
	if outcome != OutcomeSucceeded {
		span.SetStatus(codes.Error, outcome)
	}
	log.Info("job finished", "outcome", outcome, "duration_ms", float64(dur.Microseconds())/1000,
		"pickup_ms", float64(j.PickupLatency.Microseconds())/1000)
}

// invoke runs the handler, converting panics into errors so one bad job cannot kill the worker.
func (w *Worker) invoke(ctx context.Context, j job.Job) (err error) {
	h, ok := w.registry.Lookup(j.Type)
	if !ok {
		return Permanent(fmt.Errorf("%w: %q", ErrUnknownType, j.Type))
	}
	defer func() {
		if r := recover(); r != nil {
			err = fmt.Errorf("handler panic: %v\n%s", r, debug.Stack())
		}
	}()
	return h(ctx, j)
}

// heartbeat extends the lease until ctx ends. If the lease is lost it cancels the handler.
func (w *Worker) heartbeat(ctx context.Context, ref job.Ref, log *slog.Logger, cancelRun context.CancelCauseFunc) {
	t := time.NewTicker(w.cfg.HeartbeatInterval)
	defer t.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-t.C:
			hctx, cancel := context.WithTimeout(ctx, w.cfg.HeartbeatInterval)
			err := w.store.Heartbeat(hctx, ref, w.cfg.Lease)
			cancel()
			switch {
			case err == nil:
			case errors.Is(err, queue.ErrLeaseLost):
				log.Warn("lease lost; cancelling handler")
				cancelRun(errLeaseLost)
				return
			case ctx.Err() != nil:
				return
			default:
				// Transient DB error: keep trying. The lease is still valid until it expires.
				log.Warn("heartbeat failed", "err", err)
			}
		}
	}
}

// finalize records the result and returns the outcome label.
func (w *Worker) finalize(ctx context.Context, log *slog.Logger, j job.Job, ref job.Ref, herr error, cause error, timeout time.Duration) string {
	switch {
	case herr == nil:
		return w.write(ctx, log, "complete", OutcomeSucceeded, func() error { return w.complete(ctx, ref) })

	case errors.Is(cause, errLeaseLost):
		// Someone else owns the job now. Any write would be rejected, so don't try.
		log.Warn("discarding result: lease lost", "err", herr)
		return OutcomeLeaseLost

	case errors.Is(cause, errShutdown):
		return w.write(ctx, log, "release", OutcomeReleased, func() error { return w.store.Release(ctx, ref) })
	}

	if errors.Is(cause, errTimeout) {
		herr = fmt.Errorf("job timed out after %s: %w", timeout, herr)
	}
	p := queue.FailParams{Err: truncate(herr.Error(), 4096)}
	outcome := OutcomeRetry
	switch {
	case IsPermanent(herr):
		p.Status, outcome = job.StatusFailed, OutcomeFailed
	case j.Attempts >= j.MaxAttempts:
		p.Status, outcome = job.StatusDead, OutcomeDead
	default:
		p.Status = job.StatusRetrying
		p.Delay = w.cfg.Backoff.Delay(j.Attempts)
	}
	log.Warn("job attempt failed", "err", herr, "next_status", p.Status, "retry_in", p.Delay.String())
	return w.write(ctx, log, "fail", outcome, func() error { return w.store.Fail(ctx, ref, p) })
}

// complete records a success, through the batcher when one is running.
func (w *Worker) complete(ctx context.Context, ref job.Ref) error {
	direct := func() error { return w.store.Complete(ctx, ref) }
	if w.batcher == nil {
		return direct()
	}
	return w.batcher.complete(ctx, ref, direct)
}

// write retries transient DB errors a few times. If it still fails the lease will expire and
// the reaper re-queues the job: at-least-once is preserved, we just lose a little time.
func (w *Worker) write(ctx context.Context, log *slog.Logger, op, outcome string, fn func() error) string {
	var err error
	for i := 0; i < 4; i++ {
		if err = fn(); err == nil {
			return outcome
		}
		if errors.Is(err, queue.ErrLeaseLost) {
			log.Warn("write rejected: lease lost", "op", op)
			return OutcomeLeaseLost
		}
		select {
		case <-ctx.Done():
		case <-time.After(time.Duration(200<<i) * time.Millisecond):
		}
	}
	log.Error("could not record job result; relying on lease expiry", "op", op, "err", err)
	return OutcomeLeaseLost
}

// maintenance runs the reaper and the retention sweeper. Every worker runs both; SKIP LOCKED
// in the SQL makes concurrent runs safe, so no leader election is required.
func (w *Worker) maintenance(ctx context.Context) {
	reap := time.NewTicker(w.cfg.ReapInterval)
	defer reap.Stop()
	var retention <-chan time.Time
	if w.cfg.Retention > 0 {
		t := time.NewTicker(w.cfg.RetentionInterval)
		defer t.Stop()
		retention = t.C
	}
	for {
		select {
		case <-ctx.Done():
			return
		case <-reap.C:
			for {
				res, err := w.store.Reap(ctx, 100)
				if err != nil {
					if ctx.Err() == nil {
						w.log.Error("reap failed", "err", err)
					}
					break
				}
				if res.Total() > 0 {
					w.log.Warn("reclaimed jobs with expired leases", "retried", res.Retried, "dead", res.Dead)
					w.obs.Reaped(res.Retried, res.Dead)
				}
				if res.Total() < 100 { // partial batch: nothing more to do
					break
				}
			}
		case <-retention:
			for {
				n, err := w.store.DeleteFinished(ctx, w.cfg.Retention, 1000)
				if err != nil {
					if ctx.Err() == nil {
						w.log.Error("retention sweep failed", "err", err)
					}
					break
				}
				if n > 0 {
					w.obs.Deleted(n)
				}
				if n < 1000 {
					break
				}
			}
		}
	}
}

func truncate(s string, n int) string {
	if len(s) <= n {
		return s
	}
	return s[:n] + "...(truncated)"
}

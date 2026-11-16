# internal/worker

Claims jobs from the store and runs them through a **handler registry**, handling everything around the handler: concurrency, heartbeats, timeouts, panics, outcomes, shutdown.

## Files

| File | Contents |
|---|---|
| [worker.go](worker.go) | `Worker`, `Config`, `New`, `Run`; the poll loop, `execute`, `heartbeat`, `finalize`, the maintenance loop (reaper and retention sweeper) |
| [completer.go](completer.go) | `completer`, `BatchCompleter`: optional group commit of "succeeded" writes (`Config.CompleteBatch`) |
| [registry.go](registry.go) | `Handler`, `Registry`, `Permanent(err)` / `IsPermanent`, `ErrUnknownType` |

## Writing a handler

```go
reg := worker.NewRegistry()
reg.Register("email.send", func(ctx context.Context, j job.Job) error {
    // ctx is cancelled on timeout, lost lease, or forced shutdown: honour it.
    // Delivery is at-least-once: make this idempotent (use j.ID as the idempotency key downstream).
    if invalid(j.Payload) { return worker.Permanent(errors.New("bad payload")) } // no retries
    return send(ctx, j.Payload)                                                  // any other error retries
})
w := worker.New(store, reg, worker.Config{Queues: []string{"default"}, Concurrency: 20})
err := w.Run(ctx)   // returns after a graceful drain once ctx is cancelled
```

## What `Run` does

1. **Poll loop.** A semaphore channel limits concurrency. The loop waits for a free slot, claims exactly as many jobs as there are free slots (up to `BatchSize`), and sleeps a jittered `PollInterval` only when the queue is empty.
2. **`execute`** per job: join the trace stored in the job row, start a **heartbeat** goroutine (every `Lease/3`), run the handler under `timeout` (the job's own or `DefaultTimeout`) with panic recovery.
3. **`finalize`** decides the outcome: success -> `Complete` (or, with `CompleteBatch`, a batched write: the job blocks until its group is committed, so its slot and lease are held as before); `Permanent` error or unknown type -> `failed`; attempts exhausted -> `dead`; otherwise `retrying` with `Backoff.Delay`. A lost lease means the result is discarded.
4. **Maintenance** (every worker, safe thanks to `SKIP LOCKED`): `Reap` expired leases and, if `Retention` is set, `DeleteFinished` old rows.

## Shutdown (SIGTERM)

Stop claiming -> let handlers run on a **separate context** for up to `ShutdownGrace` -> if any remain, cancel them with cause `errShutdown` and **release** them (attempt refunded) -> abandon any that ignore cancellation after 5 more seconds (their lease expires and another worker's reaper recovers them). The claim call and the result write use detached contexts so a signal cannot strand a job.

## Why `context.Cause`

The handler's context is cancelled for three different reasons (timeout, lease lost, shutdown) that need three different outcomes. `context.WithCancelCause` / `WithTimeoutCause` record the reason and `finalize` reads it before cancelling anything itself.

## Consumer-side interfaces

The worker defines the `Store` it needs (7 methods) and an `Observer` for metrics (`Claimed`, `Finished`, `Reaped`, `Deleted`), so it depends on neither the concrete store nor Prometheus and is unit-tested with an in-memory fake.

## Tests

[worker_test.go](worker_test.go) (fake store, milliseconds): outcome mapping table, per-job timeout, lease loss, heartbeats, graceful vs forced shutdown, concurrency cap. End-to-end behaviour against Postgres is in [../../test/integration](../../test/integration/README.md).

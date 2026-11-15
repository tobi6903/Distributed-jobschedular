# internal/job

The **domain model**: what a job is, which states it can be in, and how retry delays are computed. This package does no I/O (no database, no clock, no goroutines), so everything in it is trivially unit-testable and every other package can import it without cycles.

## What is in it

| Piece | File | Purpose |
|---|---|---|
| `Job`, `NewJob` | [job.go](job.go) | A row of the `jobs` table, and the input to enqueue |
| `Status` + `CanTransition`, `Valid`, `Terminal` | [job.go](job.go) | The state machine as data: `pending`, `running`, `succeeded`, `failed`, `retrying`, `dead` |
| `Ref` | [job.go](job.go) | The **fencing token** `(ID, WorkerID, Attempts)` that every write ending or extending a claim must present |
| `Backoff` | [backoff.go](backoff.go) | Exponential backoff with equal jitter; the random source is injected so tests are deterministic |

## State machine

```
pending ─claim→ running ─ok→ succeeded
retrying ─claim→ running ─error, attempts left→ retrying
                         ─error, none left────→ dead     (replayable)
                         ─permanent error─────→ failed   (replayable)
                         ─released (shutdown)─→ pending
dead / failed ─replay→ pending
```

`CanTransition` is the single list of legal edges. The SQL enforces them through fenced `UPDATE`s; this table exists so the rules are explicit and tested.

## Things worth knowing

- `Job.PickupLatency` is filled in only by `Claim`, from the **database clock** (`now() - run_at`), so it is immune to clock skew.
- `Backoff.Delay(n)` = `min(cap, base * 2^(n-1))`, then `delay/2 + rand(0, delay/2)`. Defaults: base 5 s, cap 1 h. The overflow guard makes `Delay(500)` return the cap instead of wrapping.
- `Ref` is created from a claimed job with `Job.Ref()`; the worker passes it to `Heartbeat`, `Complete`, `Fail` and `Release`.

## Tests

`go test ./internal/job/`: the full transition matrix (every from/to pair), terminal/valid checks, backoff math at the boundaries, jitter bounds with the real RNG.

See also: [docs/DESIGN.md](../../docs/DESIGN.md#3-job-state-machine) section 3.

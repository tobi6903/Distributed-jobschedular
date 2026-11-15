# internal/handlers

The **demo job handlers** registered by `cmd/worker`. They exist so the whole system can be exercised end to end (load tests, chaos tests, the README demos) without writing application code. A real application registers its own handlers on the same `worker.Registry`.

| Type | Payload | Behaviour |
|---|---|---|
| `noop` | any | succeeds immediately (throughput benchmarks) |
| `sleep` | `{"seconds": 2}` (default 1) | waits, honouring cancellation |
| `fail` | `{"message": "...", "permanent": true}` | always fails; `permanent` skips retries |
| `flaky` | `{"succeed_on_attempt": 3}` (default 3) | fails until that attempt, then succeeds (shows backoff) |
| `log` | any | writes the payload to the log |

A malformed payload is a `worker.Permanent` error: it will never become valid, so retrying is pointless.

`Register(reg, log)` adds all five; registering twice returns an error (duplicate types are a programming mistake).

## Tests

[builtin_test.go](builtin_test.go): the flaky attempt table, permanence, bad payloads, cancellation of `sleep`, registration.

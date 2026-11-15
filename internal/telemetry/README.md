# internal/telemetry

Logging and tracing setup, shared by the three long-running binaries.

| Function | Purpose |
|---|---|
| `NewLogger(w, level, service)` | a JSON `slog` logger tagged with `service`. Per-job fields (`job_id`, `tenant_id`, `worker_id`, `trace_id`) are added where the context is known (worker, API) |
| `SetupTracing(ctx, service)` | installs a global OpenTelemetry tracer provider that exports over OTLP/HTTP, and returns a flush function to call on shutdown |

## Tracing is off unless configured

If neither `OTEL_EXPORTER_OTLP_ENDPOINT` nor `OTEL_EXPORTER_OTLP_TRACES_ENDPOINT` is set, `SetupTracing` does nothing: spans are no-ops, no exporter goroutines run, and the code paths are identical. When set, the exporter reads the standard `OTEL_*` variables for endpoint, headers and TLS.

A single trace spans `POST /jobs` -> `queue.enqueue` -> `job.claim` -> `job.execute` -> `job.complete` across separate processes because the API stores the `traceparent` in the job row and the worker restores it.

Covered end to end by `TestTraceSpansSubmitToComplete` in [../../test/integration](../../test/integration/README.md), and by the Compose smoke test (one trace spanning the API and worker containers in Tempo).

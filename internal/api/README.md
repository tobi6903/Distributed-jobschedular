# internal/api

The HTTP front door: submit jobs, read their status, replay dead ones, manage schedules. Standard library only (`net/http` with the Go 1.22+ `ServeMux` patterns).

## Routes

| Route | Auth | Notes |
|---|---|---|
| `POST /jobs` | yes | `201` created, `200` if the idempotency key already exists, `429` + `Retry-After` when the tenant's queue is full |
| `GET /jobs/{id}` | yes | `404` (not `403`) for another tenant's job so existence does not leak |
| `POST /jobs/{id}/replay` | yes | `409` unless the job is `failed` or `dead` |
| `POST/GET /schedules`, `GET/PATCH/DELETE /schedules/{id}` | yes | enabled only when `Deps.Schedules` is set |
| `GET /healthz` | no | liveness; never touches the database |
| `GET /readyz` | no | database ping; fails while draining |
| `/metrics` | no | mounted by `cmd/api` with `Server.Mount` |

## Design points

- **The tenant comes from the API key**, never the body (`tenant_id` in a request is an unknown field and a 400). Keys are looked up by SHA-256 hash.
- **Strict decoding**: `DisallowUnknownFields`, a 1 MiB body cap, trailing data rejected, field-level validation.
- **Backpressure** ([api.go](api.go) `submit`): `Jobs.Admit` runs before the insert. If it returns `ErrBacklogFull` and the request carries an `idempotency_key` that already exists, the existing job is returned (`200`) instead of 429, so a retry of an accepted submit never looks like a failure.
- **Tracing**: each route is wrapped with a span, a request log line and a metrics observation, labelled by the route *pattern* (bounded cardinality). `submit` stores the W3C `traceparent` in the job row so the worker continues the same trace.
- **Draining**: `SetDraining()` makes `/readyz` fail so the load balancer stops sending traffic before the listener closes.

## Consumer-side interfaces

`JobStore`, `ScheduleStore`, `Authenticator`, `Pinger`, `Observer` are all defined here and satisfied by `queue.Store`, `scheduler.Store` and `pgxpool.Pool`, which keeps the package free of those imports' concrete types.

## Tests

[api_test.go](api_test.go), [schedules_test.go](schedules_test.go) against a real database through `httptest`: health and draining, auth, validation table (14 cases), tenant isolation, idempotency, delay on the database clock, replay flow, traceparent persistence, backpressure and the idempotent-retry exception, the schedule lifecycle.

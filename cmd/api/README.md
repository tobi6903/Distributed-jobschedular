# cmd/api

The HTTP API process. `main` is deliberately thin: load config, set up logging and tracing, connect to Postgres, build `api.Server`, serve, and shut down gracefully. All behaviour lives in [internal/api](../../internal/api/README.md).

## Run

```bash
DATABASE_URL=postgres://... HTTP_ADDR=:8080 ./bin/api
```

| Variable | Default | |
|---|---|---|
| `DATABASE_URL` | required | |
| `DB_MAX_CONNS` | 10 | each `POST /jobs` holds a connection for a durable commit, so the Compose and Helm defaults are 40 |
| `HTTP_ADDR` | `:8080` | |
| `SHUTDOWN_DELAY` | 5s | stay unready this long before closing listeners (load balancer drain) |
| `SHUTDOWN_TIMEOUT` | 20s | in-flight request budget |

## Lifecycle

1. Build the server with the store, authenticator, schedule store and metrics observer; mount `/metrics`.
2. `http.Server` with read-header, read, write and idle timeouts (slowloris protection).
3. On SIGTERM/SIGINT: `SetDraining()` (readiness fails) -> wait `SHUTDOWN_DELAY` -> `Shutdown(SHUTDOWN_TIMEOUT)` -> flush traces.

The pod's `terminationGracePeriodSeconds` in the Helm chart is derived from these two values plus slack.

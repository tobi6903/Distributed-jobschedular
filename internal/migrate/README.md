# internal/migrate

Applies the SQL migrations embedded in the binary ([../../migrations](../../migrations/README.md)) with `golang-migrate`.

| Function | Purpose |
|---|---|
| `Up(ctx, dsn)` | apply all pending migrations; a no-op when current; safe to run concurrently (golang-migrate takes a Postgres advisory lock) |
| `Down(ctx, dsn, n)` | roll back `n` migrations (all if `n <= 0`) |
| `Version(dsn)` | current version and dirty flag |

## Notes

- The SQL files are compiled in with `go:embed`, so the Kubernetes migration Job needs no volume mount.
- golang-migrate picks its driver by URL scheme (`pgx5://`); the package rewrites `postgres://` for you.
- `Up` honours context cancellation by asking golang-migrate to stop gracefully.
- `jobctl migrate up` waits for the database before calling this, because in Kubernetes and Compose the migration often starts before Postgres is accepting connections.

## Tests

`TestMigrationsUpDownUp`: every migration applies, **rolls all the way back, and re-applies**, and the autovacuum settings land on `jobs`. A down migration that does not work is discovered during an incident otherwise.

# internal/testdb

Gives integration tests an **isolated, fully migrated Postgres database**: `testdb.New(t, maxConns)` creates a uniquely named database, runs the migrations, returns a `*pgxpool.Pool`, and drops the database when the test ends. `testdb.DSN(pool)` returns a connection string for code that needs its own session (the scheduler's leader election).

## Where the Postgres server comes from

1. `TEST_DATABASE_URL`, an admin connection string (what CI uses), or
2. a throwaway `postgres:16-alpine` container started with **testcontainers-go**, one per test binary.

## Skip versus fail

With neither available a test is **skipped**, so `go test ./...` still works on a laptop without Docker. But a skipped test prints `ok`, so when a database is *required* (`REQUIRE_TEST_DB=1`, which `make test` and CI set, or any `CI` variable) a missing database is a **failure**. This was added after a broken container command silently skipped every database test and reported green.

## Notes

- The container command passes only the *extra* flags (`-c max_connections=300`): the module's default command is `postgres -c fsync=off` and the option **appends** to it.
- One database per test keeps tests independent and parallel-safe; each one pays about a hundred milliseconds of migration.

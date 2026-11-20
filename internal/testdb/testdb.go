// Package testdb gives integration tests an isolated, fully migrated Postgres database.
//
// Source of the Postgres server, in order:
//  1. TEST_DATABASE_URL (an admin connection string, e.g. postgres://postgres@localhost:5433/postgres).
//     Used in CI and in environments without Docker.
//  2. A throwaway postgres:16 container via testcontainers-go (one per test binary).
//
// If neither is available the test is skipped, so `go test ./...` stays usable on machines
// without Docker, unless REQUIRE_TEST_DB=1 (set by `make test`) or CI is set, in which case it
// fails: a skip prints "ok" and must not be able to hide a broken database setup.
package testdb

import (
	"context"
	"crypto/rand"
	"database/sql"
	"encoding/hex"
	"fmt"
	"net/url"
	"os"
	"sync"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/testcontainers/testcontainers-go"
	"github.com/testcontainers/testcontainers-go/modules/postgres"
	"github.com/testcontainers/testcontainers-go/wait"

	"github.com/vasistamagam/distributed-jobscheduler/internal/migrate"

	_ "github.com/jackc/pgx/v5/stdlib" // registers the "pgx" database/sql driver
)

var (
	adminOnce sync.Once
	adminURL  string
	adminErr  error
)

// adminConnString returns the admin URL, starting a container on first use if needed.
func adminConnString(ctx context.Context) (string, error) {
	adminOnce.Do(func() {
		if u := os.Getenv("TEST_DATABASE_URL"); u != "" {
			adminURL = u
			return
		}
		c, err := postgres.Run(ctx, "postgres:16-alpine",
			postgres.WithDatabase("postgres"),
			postgres.WithUsername("postgres"),
			postgres.WithPassword("postgres"),
			// The module's default command is `postgres -c fsync=off` (durability is irrelevant in
			// tests), and CustomizeRequest APPENDS to it. So pass only the extra flags: repeating
			// "postgres" here yields `postgres -c fsync=off postgres ...`, which exits immediately.
			// The 50-worker test needs many connections.
			testcontainers.CustomizeRequest(testcontainers.GenericContainerRequest{
				ContainerRequest: testcontainers.ContainerRequest{
					Cmd: []string{"-c", "max_connections=300"},
				},
			}),
			testcontainers.WithWaitStrategy(wait.ForLog("database system is ready to accept connections").
				WithOccurrence(2).WithStartupTimeout(60*time.Second)),
		)
		if err != nil {
			adminErr = fmt.Errorf("start postgres container (is Docker running? or set TEST_DATABASE_URL): %w", err)
			return
		}
		// The container is reaped by testcontainers' Ryuk sidecar when the test binary exits.
		adminURL, adminErr = c.ConnectionString(ctx, "sslmode=disable")
	})
	return adminURL, adminErr
}

// databaseRequired reports whether missing Postgres must fail tests instead of skipping them.
func databaseRequired() bool {
	return os.Getenv("REQUIRE_TEST_DB") == "1" || os.Getenv("CI") != ""
}

// New creates a fresh database, migrates it, and returns a pool. The database is dropped
// when the test ends. maxConns sizes the pool (0 means pgx default).
func New(t testing.TB, maxConns int32) *pgxpool.Pool {
	t.Helper()
	ctx := context.Background()
	admin, err := adminConnString(ctx)
	if err != nil {
		// A skipped test still prints "ok", so a broken database setup must never be a skip
		// where a database is required (CI, `make test`): that would turn an infrastructure
		// failure into a green build that tested nothing.
		if databaseRequired() {
			t.Fatalf("a test database is required (REQUIRE_TEST_DB or CI is set) but none is available: %v", err)
		}
		t.Skipf("no Postgres available: %v", err)
	}

	var rnd [6]byte
	_, _ = rand.Read(rnd[:])
	name := "t_" + hex.EncodeToString(rnd[:])

	adminDB, err := sql.Open("pgx", admin)
	if err != nil {
		t.Fatalf("open admin: %v", err)
	}
	defer adminDB.Close()
	if _, err := adminDB.ExecContext(ctx, `CREATE DATABASE `+name); err != nil {
		t.Fatalf("create database: %v", err)
	}

	dsn := withDatabase(t, admin, name)
	if err := migrate.Up(ctx, dsn); err != nil {
		t.Fatalf("migrate: %v", err)
	}

	cfg, err := pgxpool.ParseConfig(dsn)
	if err != nil {
		t.Fatalf("parse dsn: %v", err)
	}
	if maxConns > 0 {
		cfg.MaxConns = maxConns
	}
	// Same plan_cache_mode as production pools (queue.Connect), so tests run the plans production runs.
	cfg.ConnConfig.RuntimeParams["plan_cache_mode"] = "force_custom_plan"
	pool, err := pgxpool.NewWithConfig(ctx, cfg)
	if err != nil {
		t.Fatalf("pool: %v", err)
	}

	t.Cleanup(func() {
		pool.Close()
		db, err := sql.Open("pgx", admin)
		if err != nil {
			return
		}
		defer db.Close()
		// FORCE terminates leftover connections (Postgres 13+).
		_, _ = db.Exec(`DROP DATABASE IF EXISTS ` + name + ` WITH (FORCE)`)
	})
	return pool
}

// DSN returns the connection string for the pool's database (for components, like the
// scheduler's leader election, that need their own dedicated connection).
func DSN(pool *pgxpool.Pool) string {
	c := pool.Config().ConnConfig
	u := url.URL{
		Scheme: "postgres",
		User:   url.UserPassword(c.User, c.Password),
		Host:   fmt.Sprintf("%s:%d", c.Host, c.Port),
		Path:   "/" + c.Database,
	}
	q := url.Values{}
	q.Set("sslmode", "disable")
	u.RawQuery = q.Encode()
	return u.String()
}

func withDatabase(t testing.TB, base, db string) string {
	t.Helper()
	u, err := url.Parse(base)
	if err != nil {
		t.Fatalf("parse %q: %v", base, err)
	}
	u.Path = "/" + db
	return u.String()
}

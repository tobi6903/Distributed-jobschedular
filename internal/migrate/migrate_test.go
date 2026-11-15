package migrate_test

import (
	"context"
	"testing"

	"github.com/vasistamagam/distributed-jobscheduler/internal/migrate"
	"github.com/vasistamagam/distributed-jobscheduler/internal/testdb"
)

// Every migration must apply, roll back, and re-apply cleanly: a down migration that does not
// work is discovered at the worst possible time, during an incident.
func TestMigrationsUpDownUp(t *testing.T) {
	ctx := context.Background()
	pool := testdb.New(t, 4) // already migrated up
	dsn := testdb.DSN(pool)

	v, dirty, err := migrate.Version(dsn)
	if err != nil || dirty || v < 7 {
		t.Fatalf("after up: version=%d dirty=%v err=%v", v, dirty, err)
	}
	if err := migrate.Up(ctx, dsn); err != nil { // idempotent
		t.Fatalf("second up: %v", err)
	}
	if err := migrate.Down(ctx, dsn, 0); err != nil { // roll everything back
		t.Fatalf("down all: %v", err)
	}
	var tables int
	if err := pool.QueryRow(ctx, `SELECT count(*) FROM information_schema.tables WHERE table_schema='public' AND table_name IN ('jobs','api_keys','schedules','queue_limits')`).Scan(&tables); err != nil {
		t.Fatal(err)
	}
	if tables != 0 {
		t.Fatalf("down should drop all tables, %d remain", tables)
	}
	if err := migrate.Up(ctx, dsn); err != nil {
		t.Fatalf("up after down: %v", err)
	}
	var opts []string
	if err := pool.QueryRow(ctx, `SELECT reloptions FROM pg_class WHERE relname = 'jobs'`).Scan(&opts); err != nil {
		t.Fatal(err)
	}
	if len(opts) != 4 {
		t.Fatalf("autovacuum reloptions not applied: %v", opts)
	}
}

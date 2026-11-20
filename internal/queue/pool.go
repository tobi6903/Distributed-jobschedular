package queue

import (
	"context"
	"fmt"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
)

// planCacheMode is the plan_cache_mode every pooled connection runs with, unless the connection string sets one.
//
// Why custom plans: pgx prepares statements per connection, and after five executions Postgres may switch a
// prepared statement to a generic plan that is built once and reused. An idle worker runs the claim statement every
// second against an empty table, so a fresh worker adopts a generic plan made from empty-table statistics, in which
// the "UPDATE ... FROM picked" half is a hash join against a sequential scan of the whole jobs table. When a
// 60,000-job burst arrives the plan stays cached until autovacuum next analyzes the table (up to about a minute), and
// every claim burns a core: measured, a 60,000-job burst took 65 to 83 s on freshly started workers instead of 26 to
// 31 s, with Postgres at about 400% CPU and the workers idle. force_custom_plan plans each execution with its real
// parameters, which removes the trap. See docs/DESIGN.md section 20.
const planCacheMode = "force_custom_plan"

// applyPlanCacheMode sets plan_cache_mode on every connection of the pool unless the DSN already chose one.
func applyPlanCacheMode(cfg *pgxpool.Config) {
	if cfg.ConnConfig.RuntimeParams == nil {
		cfg.ConnConfig.RuntimeParams = map[string]string{}
	}
	if _, set := cfg.ConnConfig.RuntimeParams["plan_cache_mode"]; !set {
		cfg.ConnConfig.RuntimeParams["plan_cache_mode"] = planCacheMode
	}
}

// Connect opens a pool and waits (up to wait) for Postgres to accept connections, so a
// process that starts before its database (compose, Kubernetes) retries instead of crashing.
func Connect(ctx context.Context, dsn string, maxConns int32, wait time.Duration) (*pgxpool.Pool, error) {
	cfg, err := pgxpool.ParseConfig(dsn)
	if err != nil {
		return nil, fmt.Errorf("parse database url: %w", err)
	}
	if maxConns > 0 {
		cfg.MaxConns = maxConns
	}
	applyPlanCacheMode(cfg)
	// Recycle connections so a failed-over or restarted Postgres is picked up eventually.
	cfg.MaxConnLifetime = 30 * time.Minute
	cfg.MaxConnLifetimeJitter = 5 * time.Minute
	pool, err := pgxpool.NewWithConfig(ctx, cfg)
	if err != nil {
		return nil, fmt.Errorf("create pool: %w", err)
	}
	deadline := time.Now().Add(wait)
	for {
		pctx, cancel := context.WithTimeout(ctx, 3*time.Second)
		err = pool.Ping(pctx)
		cancel()
		if err == nil {
			return pool, nil
		}
		if time.Now().After(deadline) || ctx.Err() != nil {
			pool.Close()
			return nil, fmt.Errorf("database not reachable: %w", err)
		}
		select {
		case <-ctx.Done():
		case <-time.After(time.Second):
		}
	}
}

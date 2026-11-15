package scheduler

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgxpool"
	"go.opentelemetry.io/otel/propagation"
	"go.opentelemetry.io/otel/trace"
)

var (
	ErrNotFound      = errors.New("schedule not found")
	ErrDuplicateName = errors.New("a schedule with this name already exists")
)

// Schedule is a row of the schedules table.
type Schedule struct {
	ID             string          `json:"id"`
	TenantID       string          `json:"tenant_id"`
	Name           string          `json:"name"`
	CronExpr       string          `json:"cron_expr"`
	Timezone       string          `json:"timezone"`
	MisfirePolicy  MisfirePolicy   `json:"misfire_policy"`
	Enabled        bool            `json:"enabled"`
	Queue          string          `json:"queue"`
	Type           string          `json:"type"`
	Payload        json.RawMessage `json:"payload"`
	Priority       int16           `json:"priority"`
	MaxAttempts    int             `json:"max_attempts"`
	TimeoutSeconds *int            `json:"timeout_seconds,omitempty"`
	NextFireAt     time.Time       `json:"next_fire_at"`
	LastFiredAt    *time.Time      `json:"last_fired_at,omitempty"`
	CreatedAt      time.Time       `json:"created_at"`
}

// NewSchedule is the input to Create.
type NewSchedule struct {
	TenantID       string
	Name           string
	CronExpr       string
	Timezone       string
	MisfirePolicy  MisfirePolicy
	Queue          string
	Type           string
	Payload        json.RawMessage
	Priority       int16
	MaxAttempts    int
	TimeoutSeconds *int
}

// Store persists schedules and performs the fire transaction.
type Store struct {
	pool *pgxpool.Pool
}

func NewStore(pool *pgxpool.Pool) *Store { return &Store{pool: pool} }

const schedCols = `id, tenant_id, name, cron_expr, timezone, misfire_policy, enabled, queue, type,
	payload, priority, max_attempts, timeout_seconds, next_fire_at, last_fired_at, created_at`

func scan(row pgx.Row) (Schedule, error) {
	var s Schedule
	var pol string
	err := row.Scan(&s.ID, &s.TenantID, &s.Name, &s.CronExpr, &s.Timezone, &pol, &s.Enabled, &s.Queue, &s.Type,
		&s.Payload, &s.Priority, &s.MaxAttempts, &s.TimeoutSeconds, &s.NextFireAt, &s.LastFiredAt, &s.CreatedAt)
	s.MisfirePolicy = MisfirePolicy(pol)
	s.NextFireAt = s.NextFireAt.UTC()
	return s, err
}

func isPG(err error, code string) bool {
	var pe *pgconn.PgError
	return errors.As(err, &pe) && pe.Code == code
}

func (s *Store) dbNow(ctx context.Context) (time.Time, error) {
	var now time.Time
	err := s.pool.QueryRow(ctx, `SELECT now()`).Scan(&now)
	return now, err
}

// Create validates and stores a schedule. next_fire_at is the first slot after "now".
func (s *Store) Create(ctx context.Context, n NewSchedule) (Schedule, error) {
	if n.Name == "" || len(n.Name) > 128 {
		return Schedule{}, fmt.Errorf("%w: name is required (max 128 chars)", ErrInvalid)
	}
	if n.Type == "" {
		return Schedule{}, fmt.Errorf("%w: type is required", ErrInvalid)
	}
	if n.MisfirePolicy == "" {
		n.MisfirePolicy = FireOnce
	}
	if !n.MisfirePolicy.Valid() {
		return Schedule{}, fmt.Errorf("%w: misfire_policy must be fire_once, skip or fire_all", ErrInvalid)
	}
	if n.Timezone == "" {
		n.Timezone = "UTC"
	}
	spec, err := ParseSpec(n.CronExpr, n.Timezone)
	if err != nil {
		return Schedule{}, err
	}
	if n.Queue == "" {
		n.Queue = "default"
	}
	if n.MaxAttempts == 0 {
		n.MaxAttempts = 5
	}
	if len(n.Payload) == 0 {
		n.Payload = []byte(`{}`)
	}
	now, err := s.dbNow(ctx)
	if err != nil {
		return Schedule{}, fmt.Errorf("create schedule: %w", err)
	}
	out, err := scan(s.pool.QueryRow(ctx, `
		INSERT INTO schedules (tenant_id, name, cron_expr, timezone, misfire_policy, queue, type, payload,
		                       priority, max_attempts, timeout_seconds, next_fire_at)
		VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12)
		RETURNING `+schedCols,
		n.TenantID, n.Name, n.CronExpr, n.Timezone, string(n.MisfirePolicy), n.Queue, n.Type, n.Payload,
		n.Priority, n.MaxAttempts, n.TimeoutSeconds, spec.Next(now)))
	if isPG(err, "23505") {
		return Schedule{}, ErrDuplicateName
	}
	if err != nil {
		return Schedule{}, fmt.Errorf("create schedule: %w", err)
	}
	return out, nil
}

func (s *Store) Get(ctx context.Context, tenantID, id string) (Schedule, error) {
	out, err := scan(s.pool.QueryRow(ctx, `SELECT `+schedCols+` FROM schedules WHERE id = $1 AND tenant_id = $2`, id, tenantID))
	if errors.Is(err, pgx.ErrNoRows) || isPG(err, "22P02") {
		return Schedule{}, ErrNotFound
	}
	if err != nil {
		return Schedule{}, fmt.Errorf("get schedule: %w", err)
	}
	return out, nil
}

func (s *Store) List(ctx context.Context, tenantID string) ([]Schedule, error) {
	rows, err := s.pool.Query(ctx, `SELECT `+schedCols+` FROM schedules WHERE tenant_id = $1 ORDER BY name`, tenantID)
	if err != nil {
		return nil, fmt.Errorf("list schedules: %w", err)
	}
	defer rows.Close()
	out := []Schedule{}
	for rows.Next() {
		sc, err := scan(rows)
		if err != nil {
			return nil, fmt.Errorf("list schedules: %w", err)
		}
		out = append(out, sc)
	}
	return out, rows.Err()
}

func (s *Store) Delete(ctx context.Context, tenantID, id string) error {
	tag, err := s.pool.Exec(ctx, `DELETE FROM schedules WHERE id = $1 AND tenant_id = $2`, id, tenantID)
	if isPG(err, "22P02") {
		return ErrNotFound
	}
	if err != nil {
		return fmt.Errorf("delete schedule: %w", err)
	}
	if tag.RowsAffected() == 0 {
		return ErrNotFound
	}
	return nil
}

// SetEnabled pauses or resumes a schedule. Resuming recomputes next_fire_at from now, so a
// schedule paused for a week does not "misfire" the whole week on resume.
func (s *Store) SetEnabled(ctx context.Context, tenantID, id string, enabled bool) (Schedule, error) {
	cur, err := s.Get(ctx, tenantID, id)
	if err != nil {
		return Schedule{}, err
	}
	spec, err := ParseSpec(cur.CronExpr, cur.Timezone)
	if err != nil {
		return Schedule{}, err
	}
	now, err := s.dbNow(ctx)
	if err != nil {
		return Schedule{}, fmt.Errorf("set enabled: %w", err)
	}
	out, err := scan(s.pool.QueryRow(ctx, `
		UPDATE schedules SET enabled = $3,
		       next_fire_at = CASE WHEN $3 AND NOT enabled THEN $4 ELSE next_fire_at END
		WHERE id = $1 AND tenant_id = $2 RETURNING `+schedCols, id, tenantID, enabled, spec.Next(now)))
	if errors.Is(err, pgx.ErrNoRows) {
		return Schedule{}, ErrNotFound
	}
	if err != nil {
		return Schedule{}, fmt.Errorf("set enabled: %w", err)
	}
	return out, nil
}

// FireConfig tunes FireDue.
type FireConfig struct {
	Batch    int           // max schedules processed per call
	Grace    time.Duration // Skip policy: how stale a slot may be and still fire
	MaxFires int           // FireAll: max slots fired per schedule per call
	Tracer   trace.Tracer
	Log      *slog.Logger
}

// FireResult summarises one FireDue call.
type FireResult struct {
	Schedules int // schedules processed
	Fired     int // jobs created
	Skipped   int // jobs that already existed (idempotency hit): protection against double-fire
}

// FireDue turns every due schedule into jobs, atomically per call.
//
// Safety, in two independent layers:
//  1. Row locks: `FOR UPDATE SKIP LOCKED` means two schedulers can never process the same
//     schedule row at once. Inserting the jobs and advancing next_fire_at are one transaction.
//  2. Idempotent key: each job's idempotency_key is "cron:<schedule>:<slot>". If layer 1
//     were ever bypassed (split-brain leader, a restored backup, an operator resetting
//     next_fire_at), the UNIQUE (tenant_id, idempotency_key) constraint rejects the second
//     insert. Leader election is therefore an efficiency measure, not the safety guarantee.
func (s *Store) FireDue(ctx context.Context, cfg FireConfig) (FireResult, error) {
	if cfg.Batch == 0 {
		cfg.Batch = 100
	}
	if cfg.MaxFires == 0 {
		cfg.MaxFires = 1000
	}
	if cfg.Grace == 0 {
		cfg.Grace = time.Minute
	}
	var res FireResult
	tx, err := s.pool.Begin(ctx)
	if err != nil {
		return res, fmt.Errorf("fire: begin: %w", err)
	}
	defer func() { _ = tx.Rollback(ctx) }() // no-op after Commit

	var now time.Time
	if err := tx.QueryRow(ctx, `SELECT now()`).Scan(&now); err != nil {
		return res, fmt.Errorf("fire: now: %w", err)
	}
	rows, err := tx.Query(ctx, `
		SELECT `+schedCols+` FROM schedules
		WHERE enabled AND next_fire_at <= now()
		ORDER BY next_fire_at LIMIT $1
		FOR UPDATE SKIP LOCKED`, cfg.Batch)
	if err != nil {
		return res, fmt.Errorf("fire: select due: %w", err)
	}
	var due []Schedule
	for rows.Next() {
		sc, err := scan(rows)
		if err != nil {
			rows.Close()
			return res, fmt.Errorf("fire: scan: %w", err)
		}
		due = append(due, sc)
	}
	rows.Close() // must be fully read before issuing more statements on this tx
	if err := rows.Err(); err != nil {
		return res, fmt.Errorf("fire: rows: %w", err)
	}

	for _, sc := range due {
		spec, err := ParseSpec(sc.CronExpr, sc.Timezone)
		if err != nil {
			// Should be impossible (validated on create). Disable rather than loop forever.
			cfg.Log.Error("disabling schedule with unparsable spec", "schedule_id", sc.ID, "err", err)
			if _, err := tx.Exec(ctx, `UPDATE schedules SET enabled = false WHERE id = $1`, sc.ID); err != nil {
				return res, fmt.Errorf("fire: disable: %w", err)
			}
			continue
		}
		fires, next := Plan(spec, sc.MisfirePolicy, sc.NextFireAt, now, cfg.Grace, cfg.MaxFires)
		for _, slot := range fires {
			created, err := s.insertFire(ctx, tx, cfg, sc, slot)
			if err != nil {
				return res, err
			}
			if created {
				res.Fired++
			} else {
				res.Skipped++
			}
		}
		if _, err := tx.Exec(ctx, `
			UPDATE schedules SET next_fire_at = $2,
			       last_fired_at = CASE WHEN $3 THEN now() ELSE last_fired_at END
			WHERE id = $1`, sc.ID, next, len(fires) > 0); err != nil {
			return res, fmt.Errorf("fire: advance: %w", err)
		}
		res.Schedules++
	}
	if err := tx.Commit(ctx); err != nil {
		return FireResult{}, fmt.Errorf("fire: commit: %w", err)
	}
	return res, nil
}

// FireKey is the deterministic idempotency key for one slot of one schedule.
func FireKey(scheduleID string, slot time.Time) string {
	return "cron:" + scheduleID + ":" + slot.UTC().Format(time.RFC3339)
}

func (s *Store) insertFire(ctx context.Context, tx pgx.Tx, cfg FireConfig, sc Schedule, slot time.Time) (bool, error) {
	// A root span per fire; its traceparent is stored in the job so the worker's spans
	// continue this trace (scheduler -> enqueue -> claim -> execute -> complete).
	sctx, span := cfg.Tracer.Start(ctx, "schedule.fire")
	defer span.End()
	carrier := propagation.MapCarrier{}
	propagation.TraceContext{}.Inject(sctx, carrier)
	var tp *string
	if v := carrier.Get("traceparent"); v != "" {
		tp = &v
	}
	// run_at defaults to now(): during a FireAll catch-up we want the jobs runnable now, not
	// back-dated (back-dating would pollute the pickup-latency metric).
	tag, err := tx.Exec(ctx, `
		INSERT INTO jobs (tenant_id, queue, type, payload, priority, max_attempts, timeout_seconds,
		                  idempotency_key, traceparent)
		VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9)
		ON CONFLICT (tenant_id, idempotency_key) DO NOTHING`,
		sc.TenantID, sc.Queue, sc.Type, sc.Payload, sc.Priority, sc.MaxAttempts, sc.TimeoutSeconds,
		FireKey(sc.ID, slot), tp)
	if err != nil {
		return false, fmt.Errorf("fire: insert job: %w", err)
	}
	if tag.RowsAffected() == 0 {
		cfg.Log.Warn("cron fire already exists; double-fire prevented by idempotency key",
			"schedule_id", sc.ID, "slot", slot)
		return false, nil
	}
	return true, nil
}

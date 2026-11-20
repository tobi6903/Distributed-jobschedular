// Package queue is the Postgres-backed job store. Every method is a single SQL statement
// (or a short transaction) so that state transitions are atomic and there is no in-memory
// state to lose. See docs/DESIGN.md section 5 for the reasoning behind each query.
package queue

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/vasistamagam/distributed-jobscheduler/internal/job"
)

var (
	// ErrNotFound means no such job for this tenant (also returned for other tenants' jobs
	// so that existence does not leak).
	ErrNotFound = errors.New("job not found")
	// ErrLeaseLost means a fenced write was rejected: the worker no longer owns the job.
	ErrLeaseLost = errors.New("lease lost")
	// ErrNotReplayable means the job exists but is not in failed/dead.
	ErrNotReplayable = errors.New("job is not in a replayable state")
)

// Store implements all job persistence on a pgx pool.
type Store struct {
	pool     *pgxpool.Pool
	lim      limiter
	notifier *Notifier // optional: wakes idle workers when a job becomes runnable
}

// New returns a Store backed by pool.
func New(pool *pgxpool.Pool) *Store {
	return &Store{pool: pool, lim: newLimiter()}
}

// SetNotifier makes Enqueue and Replay announce runnable jobs (see Notifier). Call it before serving;
// nil (the default) means workers find new jobs by polling only.
func (s *Store) SetNotifier(n *Notifier) { s.notifier = n }

// announce hints that j can run now. Called only after the row is committed. A job that is not due soon
// (a delay, a future run_at) is left to the poll: its wake-up time is in the future, so a notification
// now would only wake workers to find nothing.
func (s *Store) announce(j job.Job) {
	if s.notifier != nil && !j.RunAt.After(time.Now().Add(time.Second)) {
		s.notifier.Notify(j.Queue)
	}
}

// Pool exposes the pool for components that share it (scheduler, API health checks).
func (s *Store) Pool() *pgxpool.Pool { return s.pool }

const jobColumns = `id, tenant_id, queue, type, payload, status, priority, run_at, attempts,
	max_attempts, timeout_seconds, locked_by, locked_until, idempotency_key, last_error,
	traceparent, created_at, updated_at, finished_at`

// prefixed variant for queries that alias the table (UPDATE ... j).
const jobColumnsJ = `j.id, j.tenant_id, j.queue, j.type, j.payload, j.status, j.priority, j.run_at, j.attempts,
	j.max_attempts, j.timeout_seconds, j.locked_by, j.locked_until, j.idempotency_key, j.last_error,
	j.traceparent, j.created_at, j.updated_at, j.finished_at`

func scanJob(row pgx.Row, extra ...any) (job.Job, error) {
	var (
		j  job.Job
		st string
		tp *string
	)
	dest := []any{&j.ID, &j.TenantID, &j.Queue, &j.Type, &j.Payload, &st, &j.Priority, &j.RunAt, &j.Attempts,
		&j.MaxAttempts, &j.TimeoutSeconds, &j.LockedBy, &j.LockedUntil, &j.IdempotencyKey, &j.LastError,
		&tp, &j.CreatedAt, &j.UpdatedAt, &j.FinishedAt}
	dest = append(dest, extra...)
	if err := row.Scan(dest...); err != nil {
		return job.Job{}, err
	}
	j.Status = job.Status(st)
	if tp != nil {
		j.Traceparent = *tp
	}
	return j, nil
}

// isInvalidUUID reports Postgres error 22P02 (invalid_text_representation), which a
// malformed id produces. We treat it as "not found" rather than a 500.
func isInvalidUUID(err error) bool {
	var pe *pgconn.PgError
	return errors.As(err, &pe) && pe.Code == "22P02"
}

// Enqueue inserts a job. If an idempotency key is supplied and already exists for the
// tenant, the existing job is returned with created=false (no duplicate is made).
func (s *Store) Enqueue(ctx context.Context, n job.NewJob) (j job.Job, created bool, err error) {
	if n.Queue == "" {
		n.Queue = job.DefaultQueue
	}
	if n.MaxAttempts == 0 {
		n.MaxAttempts = job.DefaultMaxAttempts
	}
	if len(n.Payload) == 0 {
		n.Payload = []byte(`{}`)
	}
	var runAt *time.Time
	if !n.RunAt.IsZero() {
		runAt = &n.RunAt
	}
	var tp *string
	if n.Traceparent != "" {
		tp = &n.Traceparent
	}
	// run_at = (client-supplied absolute time, else the DB clock) + relative delay.
	const q = `
		INSERT INTO jobs (tenant_id, queue, type, payload, priority, run_at, max_attempts,
		                  timeout_seconds, idempotency_key, traceparent)
		VALUES ($1, $2, $3, $4, $5, COALESCE($6::timestamptz, now()) + make_interval(secs => $7::float8),
		        $8, $9, $10, $11)
		ON CONFLICT (tenant_id, idempotency_key) DO NOTHING
		RETURNING ` + jobColumns
	args := []any{n.TenantID, n.Queue, n.Type, n.Payload, n.Priority, runAt, n.Delay.Seconds(),
		n.MaxAttempts, n.TimeoutSeconds, n.IdempotencyKey, tp}

	for attempt := 0; attempt < 2; attempt++ {
		j, err = scanJob(s.pool.QueryRow(ctx, q, args...))
		if err == nil {
			s.announce(j)
			return j, true, nil
		}
		if !errors.Is(err, pgx.ErrNoRows) {
			return job.Job{}, false, fmt.Errorf("enqueue: %w", err)
		}
		// Conflict: return the existing job. The retry loop covers the (very rare) case where
		// the conflicting row was deleted by the retention reaper between the two statements.
		if n.IdempotencyKey != nil {
			j, err = scanJob(s.pool.QueryRow(ctx,
				`SELECT `+jobColumns+` FROM jobs WHERE tenant_id = $1 AND idempotency_key = $2`,
				n.TenantID, *n.IdempotencyKey))
			if err == nil {
				return j, false, nil
			}
			if !errors.Is(err, pgx.ErrNoRows) {
				return job.Job{}, false, fmt.Errorf("enqueue lookup: %w", err)
			}
		}
	}
	return job.Job{}, false, errors.New("enqueue: conflicting job vanished repeatedly")
}

// Get returns a job scoped to the tenant.
func (s *Store) Get(ctx context.Context, tenantID, id string) (job.Job, error) {
	j, err := scanJob(s.pool.QueryRow(ctx,
		`SELECT `+jobColumns+` FROM jobs WHERE id = $1 AND tenant_id = $2`, id, tenantID))
	switch {
	case errors.Is(err, pgx.ErrNoRows), err != nil && isInvalidUUID(err):
		return job.Job{}, ErrNotFound
	case err != nil:
		return job.Job{}, fmt.Errorf("get job: %w", err)
	}
	return j, nil
}

// claimSQLFor builds the claim statement. scope is an extra predicate (empty, or one of the
// constants in limits.go) that restricts which tenants' jobs may be picked; it only ever comes
// from those constants, never from user input.
//
// This statement is the heart of the system (docs/DESIGN.md section 5):
//
//   - FOR UPDATE SKIP LOCKED: concurrent claimers skip rows another transaction has locked
//     instead of blocking, so N workers get disjoint batches.
//   - The row lock lives only for this one statement. The durable claim is the
//     status='running' + lease columns, so no transaction is held open while the job runs.
//   - ORDER BY matches jobs_claim_idx, so this is an index scan that stops after LIMIT rows.
//   - attempts is incremented here, so a job that crashes its worker still burns an attempt.
//   - now() is the database clock: worker clock skew cannot cause early/late claims.
func claimSQLFor(scope string) string {
	return `
WITH picked AS (
    SELECT id
    FROM jobs
    WHERE queue = $1
      AND status IN ('pending', 'retrying')
      AND run_at <= now()
      ` + scope + `
    ORDER BY priority DESC, run_at, id
    LIMIT $2
    FOR UPDATE SKIP LOCKED
)
UPDATE jobs j
SET status       = 'running',
    locked_by    = $3,
    locked_until = now() + make_interval(secs => $4::float8),
    attempts     = j.attempts + 1,
    updated_at   = now()
FROM picked
WHERE j.id = picked.id
RETURNING ` + jobColumnsJ + `, EXTRACT(EPOCH FROM (now() - j.run_at))::float8`
}

// claimSQL is the unscoped statement used when a queue has no limits (the common case).
var claimSQL = claimSQLFor("")

// queryer is satisfied by both *pgxpool.Pool and pgx.Tx.
type queryer interface {
	Query(ctx context.Context, sql string, args ...any) (pgx.Rows, error)
}

// runClaim executes a claim statement and scans the claimed jobs.
func runClaim(ctx context.Context, q queryer, sql string, args ...any) ([]job.Job, error) {
	rows, err := q.Query(ctx, sql, args...)
	if err != nil {
		return nil, fmt.Errorf("claim: %w", err)
	}
	defer rows.Close()
	var out []job.Job
	for rows.Next() {
		var lat float64
		j, err := scanJob(rows, &lat)
		if err != nil {
			return nil, fmt.Errorf("claim scan: %w", err)
		}
		j.PickupLatency = time.Duration(lat * float64(time.Second))
		out = append(out, j)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("claim rows: %w", err)
	}
	return out, nil
}

// Claim atomically takes up to limit ready jobs from queue for workerID, leasing them for
// the given duration. The returned jobs are in unspecified order.
//
// If the queue has limits configured (see limits.go) they are enforced here; otherwise this
// is a single statement with no extra round trips.
func (s *Store) Claim(ctx context.Context, queue, workerID string, limit int, lease time.Duration) ([]job.Job, error) {
	rules, err := s.lim.rules(ctx, s.pool, queue)
	if err != nil {
		return nil, err
	}
	if governed := governedRules(rules); len(governed) > 0 {
		return s.claimGoverned(ctx, queue, workerID, limit, lease, rules)
	}
	return runClaim(ctx, s.pool, claimSQL, queue, limit, workerID, lease.Seconds())
}

// fenced is the WHERE clause shared by every write that ends or extends a claim.
const fenced = `WHERE id = $1 AND status = 'running' AND locked_by = $2 AND attempts = $3`

func (s *Store) execFenced(ctx context.Context, op, sql string, args ...any) error {
	tag, err := s.pool.Exec(ctx, sql, args...)
	if err != nil {
		return fmt.Errorf("%s: %w", op, err)
	}
	if tag.RowsAffected() == 0 {
		return fmt.Errorf("%s: %w", op, ErrLeaseLost)
	}
	return nil
}

// Heartbeat extends the lease. ErrLeaseLost means another worker now owns the job (or it
// was reaped) and the caller must stop work.
func (s *Store) Heartbeat(ctx context.Context, ref job.Ref, lease time.Duration) error {
	return s.execFenced(ctx, "heartbeat", `
		UPDATE jobs SET locked_until = now() + make_interval(secs => $4::float8), updated_at = now() `+fenced,
		ref.ID, ref.WorkerID, ref.Attempts, lease.Seconds())
}

// Complete marks the job succeeded. Fenced: a stale worker's result is discarded.
func (s *Store) Complete(ctx context.Context, ref job.Ref) error {
	return s.execFenced(ctx, "complete", `
		UPDATE jobs SET status = 'succeeded', locked_by = NULL, locked_until = NULL,
		       finished_at = now(), updated_at = now() `+fenced,
		ref.ID, ref.WorkerID, ref.Attempts)
}

// CompleteBatch marks many jobs succeeded in ONE statement (one durable commit instead of
// len(refs)). It is the batched form of Complete and carries exactly the same fence, applied
// per row: a row whose (worker, attempts) no longer matches is simply not updated.
//
// The result is aligned with refs: ok[i] is true when refs[i] was completed, false when its
// lease was lost (the caller must treat that like ErrLeaseLost from Complete). A non-nil error
// means the statement failed as a whole and NOTHING was committed, so the caller may retry.
// refs must not contain the same job twice.
func (s *Store) CompleteBatch(ctx context.Context, refs []job.Ref) ([]bool, error) {
	ok := make([]bool, len(refs))
	if len(refs) == 0 {
		return ok, nil
	}
	ids := make([]string, len(refs))
	workers := make([]string, len(refs))
	attempts := make([]int32, len(refs))
	for i, r := range refs {
		ids[i], workers[i], attempts[i] = r.ID, r.WorkerID, int32(r.Attempts)
	}
	rows, err := s.pool.Query(ctx, `
		UPDATE jobs j SET status = 'succeeded', locked_by = NULL, locked_until = NULL,
		       finished_at = now(), updated_at = now()
		FROM unnest($1::uuid[], $2::text[], $3::int[]) AS r(id, worker, attempts)
		WHERE j.id = r.id AND j.status = 'running' AND j.locked_by = r.worker AND j.attempts = r.attempts
		RETURNING j.id::text`, ids, workers, attempts)
	if err != nil {
		return nil, fmt.Errorf("complete batch: %w", err)
	}
	defer rows.Close()
	done := make(map[string]struct{}, len(refs))
	for rows.Next() {
		var id string
		if err := rows.Scan(&id); err != nil {
			return nil, fmt.Errorf("complete batch: %w", err)
		}
		done[id] = struct{}{}
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("complete batch: %w", err)
	}
	for i, r := range refs {
		_, ok[i] = done[r.ID]
	}
	return ok, nil
}

// FailParams describes the outcome of a failed attempt, decided by the worker.
type FailParams struct {
	Status job.Status    // retrying, dead or failed
	Delay  time.Duration // backoff, used only when Status == retrying
	Err    string
}

// Fail records a failed attempt. Fenced.
func (s *Store) Fail(ctx context.Context, ref job.Ref, p FailParams) error {
	switch p.Status {
	case job.StatusRetrying, job.StatusDead, job.StatusFailed:
	default:
		return fmt.Errorf("fail: invalid target status %q", p.Status)
	}
	return s.execFenced(ctx, "fail", `
		UPDATE jobs SET status = $4::text, last_error = $5,
		       run_at = CASE WHEN $4::text = 'retrying' THEN now() + make_interval(secs => $6::float8) ELSE run_at END,
		       finished_at = CASE WHEN $4::text IN ('dead', 'failed') THEN now() END,
		       locked_by = NULL, locked_until = NULL, updated_at = now() `+fenced,
		ref.ID, ref.WorkerID, ref.Attempts, string(p.Status), p.Err, p.Delay.Seconds())
}

// Release hands a claimed job back to pending without penalising it: the attempt is not
// counted, because being interrupted by a deploy is not the job's fault. Fenced.
func (s *Store) Release(ctx context.Context, ref job.Ref) error {
	return s.execFenced(ctx, "release", `
		UPDATE jobs SET status = 'pending', attempts = attempts - 1, locked_by = NULL,
		       locked_until = NULL, updated_at = now() `+fenced,
		ref.ID, ref.WorkerID, ref.Attempts)
}

// ReapResult counts what one Reap pass did.
type ReapResult struct {
	Retried int
	Dead    int
}

func (r ReapResult) Total() int { return r.Retried + r.Dead }

// Reap returns jobs whose lease has expired to the queue (or to dead if out of attempts).
// SKIP LOCKED makes concurrent reapers safe, so every worker can run one: no leader needed.
func (s *Store) Reap(ctx context.Context, limit int) (ReapResult, error) {
	rows, err := s.pool.Query(ctx, `
		WITH expired AS (
		    SELECT id FROM jobs
		    WHERE status = 'running' AND locked_until < now()
		    ORDER BY locked_until
		    LIMIT $1
		    FOR UPDATE SKIP LOCKED
		)
		UPDATE jobs j
		SET status      = CASE WHEN j.attempts >= j.max_attempts THEN 'dead' ELSE 'retrying' END,
		    run_at      = now(),
		    finished_at = CASE WHEN j.attempts >= j.max_attempts THEN now() END,
		    last_error  = 'lease expired (worker crashed or stalled)',
		    locked_by = NULL, locked_until = NULL, updated_at = now()
		FROM expired WHERE j.id = expired.id
		RETURNING j.status`, limit)
	if err != nil {
		return ReapResult{}, fmt.Errorf("reap: %w", err)
	}
	defer rows.Close()
	var res ReapResult
	for rows.Next() {
		var st string
		if err := rows.Scan(&st); err != nil {
			return ReapResult{}, fmt.Errorf("reap scan: %w", err)
		}
		if st == string(job.StatusDead) {
			res.Dead++
		} else {
			res.Retried++
		}
	}
	return res, rows.Err()
}

// Replay moves a failed/dead job back to pending with a fresh attempt budget.
func (s *Store) Replay(ctx context.Context, tenantID, id string) (job.Job, error) {
	j, err := scanJob(s.pool.QueryRow(ctx, `
		UPDATE jobs SET status = 'pending', attempts = 0, run_at = now(), finished_at = NULL, updated_at = now()
		WHERE id = $1 AND tenant_id = $2 AND status IN ('failed', 'dead')
		RETURNING `+jobColumns, id, tenantID))
	if err == nil {
		s.announce(j)
		return j, nil
	}
	if isInvalidUUID(err) {
		return job.Job{}, ErrNotFound
	}
	if !errors.Is(err, pgx.ErrNoRows) {
		return job.Job{}, fmt.Errorf("replay: %w", err)
	}
	// Distinguish "no such job" from "exists but not replayable".
	if _, gerr := s.Get(ctx, tenantID, id); gerr != nil {
		return job.Job{}, gerr
	}
	return job.Job{}, ErrNotReplayable
}

// DeleteFinished removes up to limit finished jobs older than retention. Small batches keep
// locks and WAL bursts short. Note this frees idempotency keys: the dedup window equals the
// retention period.
func (s *Store) DeleteFinished(ctx context.Context, retention time.Duration, limit int) (int, error) {
	tag, err := s.pool.Exec(ctx, `
		DELETE FROM jobs WHERE id IN (
		    SELECT id FROM jobs
		    WHERE status IN ('succeeded', 'failed', 'dead')
		      AND finished_at < now() - make_interval(secs => $1::float8)
		    LIMIT $2
		    FOR UPDATE SKIP LOCKED)`, retention.Seconds(), limit)
	if err != nil {
		return 0, fmt.Errorf("delete finished: %w", err)
	}
	return int(tag.RowsAffected()), nil
}

// DepthRow is a (queue, status) -> count sample used for queue-depth metrics.
type DepthRow struct {
	Queue  string
	Status job.Status
	Count  int64
}

// Depth returns job counts for the non-terminal statuses plus dead.
func (s *Store) Depth(ctx context.Context) ([]DepthRow, error) {
	rows, err := s.pool.Query(ctx, `
		SELECT queue, status, count(*) FROM jobs
		WHERE status IN ('pending', 'retrying', 'running', 'dead')
		GROUP BY queue, status`)
	if err != nil {
		return nil, fmt.Errorf("depth: %w", err)
	}
	defer rows.Close()
	var out []DepthRow
	for rows.Next() {
		var r DepthRow
		var st string
		if err := rows.Scan(&r.Queue, &st, &r.Count); err != nil {
			return nil, fmt.Errorf("depth scan: %w", err)
		}
		r.Status = job.Status(st)
		out = append(out, r)
	}
	return out, rows.Err()
}

// OldestReady returns, per queue, the age of the oldest job that is ready to run but not
// yet claimed. It is the most direct measure of "are workers keeping up".
func (s *Store) OldestReady(ctx context.Context) (map[string]time.Duration, error) {
	rows, err := s.pool.Query(ctx, `
		SELECT queue, EXTRACT(EPOCH FROM (now() - min(run_at)))::float8 FROM jobs
		WHERE status IN ('pending', 'retrying') AND run_at <= now()
		GROUP BY queue`)
	if err != nil {
		return nil, fmt.Errorf("oldest ready: %w", err)
	}
	defer rows.Close()
	out := map[string]time.Duration{}
	for rows.Next() {
		var q string
		var secs float64
		if err := rows.Scan(&q, &secs); err != nil {
			return nil, fmt.Errorf("oldest ready scan: %w", err)
		}
		out[q] = time.Duration(secs * float64(time.Second))
	}
	return out, rows.Err()
}

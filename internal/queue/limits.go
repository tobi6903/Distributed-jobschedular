package queue

import (
	"context"
	"errors"
	"fmt"
	"math"
	"math/rand/v2"
	"sort"
	"sync"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/vasistamagam/distributed-jobscheduler/internal/job"
)

// AllTenants is the tenant_id of a queue-wide default rule: it applies to every tenant on
// the queue that has no rule of its own.
const AllTenants = "*"

var (
	// ErrBacklogFull is returned by Admit when a scope's pending backlog has reached its
	// max_backlog. The API turns it into 429 Too Many Requests (backpressure).
	ErrBacklogFull = errors.New("queue backlog limit reached")
	// ErrInvalidLimit marks a rejected limit definition.
	ErrInvalidLimit = errors.New("invalid limit")
	// ErrLimitNotFound is returned when deleting a rule that does not exist.
	ErrLimitNotFound = errors.New("limit not found")
)

// Limit is one row of queue_limits. Nil fields are "not limited" for that dimension.
//
// Scope: a rule with a specific TenantID governs that tenant's jobs on Queue. A rule with
// TenantID == AllTenants governs, together, every tenant on Queue that has no specific rule
// (so it is a shared cap or shared rate for "everyone else").
type Limit struct {
	Queue         string   `json:"queue"`
	TenantID      string   `json:"tenant_id"`
	MaxRunning    *int     `json:"max_running,omitempty"`     // concurrency cap
	RatePerSecond *float64 `json:"rate_per_second,omitempty"` // token bucket refill rate
	Burst         *int     `json:"burst,omitempty"`           // token bucket capacity
	MaxBacklog    *int     `json:"max_backlog,omitempty"`     // submit backpressure threshold
}

// governs reports whether the rule affects claiming (as opposed to only admission).
func (l Limit) governs() bool { return l.MaxRunning != nil || l.RatePerSecond != nil }

func (l Limit) validate() error {
	switch {
	case l.Queue == "" || l.TenantID == "":
		return fmt.Errorf("%w: queue and tenant are required (use %q for all tenants)", ErrInvalidLimit, AllTenants)
	case l.MaxRunning != nil && *l.MaxRunning < 1:
		return fmt.Errorf("%w: max_running must be >= 1", ErrInvalidLimit)
	case l.RatePerSecond != nil && *l.RatePerSecond <= 0:
		return fmt.Errorf("%w: rate_per_second must be > 0", ErrInvalidLimit)
	case l.Burst != nil && *l.Burst < 1:
		return fmt.Errorf("%w: burst must be >= 1", ErrInvalidLimit)
	case l.MaxBacklog != nil && *l.MaxBacklog < 1:
		return fmt.Errorf("%w: max_backlog must be >= 1", ErrInvalidLimit)
	case l.MaxRunning == nil && l.RatePerSecond == nil && l.MaxBacklog == nil:
		return fmt.Errorf("%w: set at least one of max_running, rate_per_second, max_backlog", ErrInvalidLimit)
	}
	return nil
}

// ---- administration ----

// SetLimit creates or replaces a rule. A rate without a burst gets burst = ceil(rate), at
// least 1, so a freshly created bucket can dispatch about a second of work immediately.
func (s *Store) SetLimit(ctx context.Context, l Limit) error {
	if l.RatePerSecond != nil && l.Burst == nil {
		b := max(1, int(math.Ceil(*l.RatePerSecond)))
		l.Burst = &b
	}
	if l.RatePerSecond == nil && l.Burst != nil {
		return fmt.Errorf("%w: burst needs rate_per_second", ErrInvalidLimit)
	}
	if err := l.validate(); err != nil {
		return err
	}
	_, err := s.pool.Exec(ctx, `
		INSERT INTO queue_limits (queue, tenant_id, max_running, rate_per_second, burst, max_backlog, tokens)
		VALUES ($1, $2, $3, $4, $5, $6, COALESCE($5, 0))
		ON CONFLICT (queue, tenant_id) DO UPDATE SET
		    max_running = EXCLUDED.max_running, rate_per_second = EXCLUDED.rate_per_second,
		    burst = EXCLUDED.burst, max_backlog = EXCLUDED.max_backlog,
		    -- keep the current fill level, but never above the new capacity
		    tokens = LEAST(queue_limits.tokens, COALESCE(EXCLUDED.burst, 0)),
		    updated_at = now()`,
		l.Queue, l.TenantID, l.MaxRunning, l.RatePerSecond, l.Burst, l.MaxBacklog)
	if err != nil {
		return fmt.Errorf("set limit: %w", err)
	}
	s.lim.invalidate(l.Queue)
	return nil
}

// DeleteLimit removes a rule.
func (s *Store) DeleteLimit(ctx context.Context, queue, tenantID string) error {
	tag, err := s.pool.Exec(ctx, `DELETE FROM queue_limits WHERE queue = $1 AND tenant_id = $2`, queue, tenantID)
	if err != nil {
		return fmt.Errorf("delete limit: %w", err)
	}
	s.lim.invalidate(queue)
	if tag.RowsAffected() == 0 {
		return ErrLimitNotFound
	}
	return nil
}

// ListLimits returns every rule, ordered by queue then tenant.
func (s *Store) ListLimits(ctx context.Context) ([]Limit, error) {
	return queryLimits(ctx, s.pool, `SELECT queue, tenant_id, max_running, rate_per_second, burst, max_backlog
		FROM queue_limits ORDER BY queue, tenant_id`)
}

func queryLimits(ctx context.Context, q queryer, sql string, args ...any) ([]Limit, error) {
	rows, err := q.Query(ctx, sql, args...)
	if err != nil {
		return nil, fmt.Errorf("query limits: %w", err)
	}
	defer rows.Close()
	var out []Limit
	for rows.Next() {
		var l Limit
		if err := rows.Scan(&l.Queue, &l.TenantID, &l.MaxRunning, &l.RatePerSecond, &l.Burst, &l.MaxBacklog); err != nil {
			return nil, fmt.Errorf("scan limit: %w", err)
		}
		out = append(out, l)
	}
	return out, rows.Err()
}

// ---- rule cache and admission cache ----

// limiter caches the (tiny) rule set per queue and the backlog counts used for admission, so
// neither the claim hot path nor the submit path pays a query per call.
type limiter struct {
	mu         sync.Mutex
	ruleTTL    time.Duration
	backlogTTL time.Duration
	ruleCache  map[string]cachedRules
	backlog    map[string]cachedCount
}

type cachedRules struct {
	at    time.Time
	rules []Limit
}

type cachedCount struct {
	at time.Time
	n  int
}

func newLimiter() limiter {
	return limiter{
		ruleTTL: 2 * time.Second, backlogTTL: time.Second,
		ruleCache: map[string]cachedRules{}, backlog: map[string]cachedCount{},
	}
}

// SetLimitCacheTTL tunes how long rules and backlog counts are cached. Zero disables caching
// (exact, but one extra query per call); tests use that. Call before the store is shared.
func (s *Store) SetLimitCacheTTL(rules, backlog time.Duration) {
	s.lim.mu.Lock()
	defer s.lim.mu.Unlock()
	s.lim.ruleTTL, s.lim.backlogTTL = rules, backlog
}

func (l *limiter) invalidate(queue string) {
	l.mu.Lock()
	defer l.mu.Unlock()
	delete(l.ruleCache, queue)
	for k := range l.backlog {
		delete(l.backlog, k)
	}
}

func (l *limiter) rules(ctx context.Context, pool *pgxpool.Pool, queue string) ([]Limit, error) {
	l.mu.Lock()
	if c, ok := l.ruleCache[queue]; ok && time.Since(c.at) < l.ruleTTL {
		l.mu.Unlock()
		return c.rules, nil
	}
	l.mu.Unlock()
	rules, err := queryLimits(ctx, pool, `SELECT queue, tenant_id, max_running, rate_per_second, burst, max_backlog
		FROM queue_limits WHERE queue = $1`, queue)
	if err != nil {
		return nil, err
	}
	l.mu.Lock()
	l.ruleCache[queue] = cachedRules{at: time.Now(), rules: rules}
	l.mu.Unlock()
	return rules, nil
}

// ---- scopes ----

// Scope predicates for claimSQLFor. Constants only; the parameter ($5) is bound separately.
const (
	scopeTenant  = `AND tenant_id = $5`
	scopeExclude = `AND tenant_id <> ALL($5::text[])`
	scopeInclude = `AND tenant_id = ANY($5::text[])`
)

type claimGroup struct {
	rule  *Limit // nil for the ungoverned group
	scope string
	arg   any // value for $5
	// running-count predicate for the governed groups; mirrors scope but on running jobs.
}

func governedRules(rules []Limit) []Limit {
	var out []Limit
	for _, r := range rules {
		if r.governs() {
			out = append(out, r)
		}
	}
	return out
}

// groupsFor partitions a queue's tenants into disjoint groups:
//   - one group per tenant that has its own limiting rule,
//   - one group for "everyone else" if a '*' rule limits,
//   - otherwise an ungoverned group for tenants with no limiting rule.
//
// Every tenant belongs to exactly one group, so no job is unclaimable and none is counted twice.
func groupsFor(rules []Limit) []claimGroup {
	var (
		groups        []claimGroup
		star          *Limit
		governedNames []string // tenants with their own limiting rule
		specificNames []string // tenants with any rule of their own (limiting or backlog-only)
		byName        = map[string]*Limit{}
	)
	for i := range rules {
		r := &rules[i]
		if r.TenantID == AllTenants {
			if r.governs() {
				star = r
			}
			continue
		}
		specificNames = append(specificNames, r.TenantID)
		if r.governs() {
			governedNames = append(governedNames, r.TenantID)
			byName[r.TenantID] = r
		}
	}
	sort.Strings(governedNames)
	sort.Strings(specificNames)
	for _, t := range governedNames {
		groups = append(groups, claimGroup{rule: byName[t], scope: scopeTenant, arg: t})
	}
	switch {
	case star != nil:
		// Everyone without a rule of their own shares the '*' limits.
		groups = append(groups, claimGroup{rule: star, scope: scopeExclude, arg: nonNil(specificNames)})
		// Tenants with a backlog-only rule of their own are ungoverned.
		var free []string
		for _, t := range specificNames {
			if byName[t] == nil {
				free = append(free, t)
			}
		}
		if len(free) > 0 {
			groups = append(groups, claimGroup{scope: scopeInclude, arg: free})
		}
	default:
		groups = append(groups, claimGroup{scope: scopeExclude, arg: nonNil(governedNames)})
	}
	return groups
}

// nonNil avoids passing a NULL array, which would make "<> ALL(NULL)" match nothing.
func nonNil(s []string) []string {
	if s == nil {
		return []string{}
	}
	return s
}

// claimGoverned claims from a queue that has limiting rules. Groups are visited in random
// order each call, so no group (limited or not) can be starved by another filling every batch.
func (s *Store) claimGoverned(ctx context.Context, queue, workerID string, limit int, lease time.Duration, rules []Limit) ([]job.Job, error) {
	groups := groupsFor(rules)
	rand.Shuffle(len(groups), func(i, j int) { groups[i], groups[j] = groups[j], groups[i] })

	var out []job.Job
	for _, g := range groups {
		if len(out) >= limit {
			break
		}
		var (
			jobs []job.Job
			err  error
		)
		if g.rule == nil {
			jobs, err = runClaim(ctx, s.pool, claimSQLFor(g.scope), queue, limit-len(out), workerID, lease.Seconds(), g.arg)
		} else {
			jobs, err = s.claimLimited(ctx, g, queue, workerID, limit-len(out), lease, groupsExclusions(rules))
		}
		if err != nil {
			return out, err
		}
		out = append(out, jobs...)
	}
	return out, nil
}

// groupsExclusions lists every tenant with a rule of their own, for the '*' group's
// running-count predicate.
func groupsExclusions(rules []Limit) []string {
	var names []string
	for _, r := range rules {
		if r.TenantID != AllTenants {
			names = append(names, r.TenantID)
		}
	}
	sort.Strings(names)
	return nonNil(names)
}

// claimLimited claims for one governed group inside a transaction.
//
// Correctness: the rule's row is locked FOR UPDATE for the transaction, so at most one
// worker claims for a given rule at a time. That serialisation is what makes both limits
// exact: the running count and the token bucket cannot be raced by two concurrent claimers.
// If another worker already holds the lock we SKIP the group this round (it is being served).
// The cost is that a *limited* group's claims are serial, but a limited group is by
// definition small, and every other group is unaffected.
func (s *Store) claimLimited(ctx context.Context, g claimGroup, queue, workerID string, want int, lease time.Duration, specific []string) ([]job.Job, error) {
	tx, err := s.pool.Begin(ctx)
	if err != nil {
		return nil, fmt.Errorf("claim limited: begin: %w", err)
	}
	defer func() { _ = tx.Rollback(ctx) }()

	// Lock the rule and read its state. tokens_now is the bucket refilled up to this instant
	// on the database clock.
	var (
		maxRunning *int
		rate       *float64
		tokensNow  *float64
	)
	err = tx.QueryRow(ctx, `
		SELECT max_running, rate_per_second,
		       CASE WHEN rate_per_second IS NULL THEN NULL
		            ELSE LEAST(burst::float8, tokens + rate_per_second * EXTRACT(EPOCH FROM (now() - tokens_updated_at))) END
		FROM queue_limits
		WHERE queue = $1 AND tenant_id = $2
		FOR UPDATE SKIP LOCKED`, queue, g.rule.TenantID).Scan(&maxRunning, &rate, &tokensNow)
	if errors.Is(err, pgx.ErrNoRows) {
		// Locked by another claimer (fine) or deleted since we cached it (refresh the cache).
		s.lim.invalidate(queue)
		return nil, nil
	}
	if err != nil {
		return nil, fmt.Errorf("claim limited: lock rule: %w", err)
	}

	allowed := want
	if maxRunning != nil {
		var running int
		var cerr error
		if g.rule.TenantID == AllTenants {
			cerr = tx.QueryRow(ctx, `SELECT count(*) FROM jobs WHERE queue = $1 AND status = 'running' AND tenant_id <> ALL($2::text[])`,
				queue, specific).Scan(&running)
		} else {
			cerr = tx.QueryRow(ctx, `SELECT count(*) FROM jobs WHERE queue = $1 AND status = 'running' AND tenant_id = $2`,
				queue, g.rule.TenantID).Scan(&running)
		}
		if cerr != nil {
			return nil, fmt.Errorf("claim limited: count running: %w", cerr)
		}
		allowed = min(allowed, *maxRunning-running)
	}
	if tokensNow != nil {
		allowed = min(allowed, int(math.Floor(*tokensNow)))
	}

	var jobs []job.Job
	if allowed > 0 {
		jobs, err = runClaim(ctx, tx, claimSQLFor(g.scope), queue, allowed, workerID, lease.Seconds(), g.arg)
		if err != nil {
			return nil, err
		}
	}
	if rate != nil {
		// Persist the refilled bucket minus what we took, stamped with this transaction's now().
		if _, err := tx.Exec(ctx, `UPDATE queue_limits SET tokens = $3, tokens_updated_at = now()
			WHERE queue = $1 AND tenant_id = $2`, queue, g.rule.TenantID, *tokensNow-float64(len(jobs))); err != nil {
			return nil, fmt.Errorf("claim limited: update bucket: %w", err)
		}
	}
	if err := tx.Commit(ctx); err != nil {
		return nil, fmt.Errorf("claim limited: commit: %w", err)
	}
	return jobs, nil
}

// ---- admission control (backpressure) ----

// ruleFor picks the rule that applies to a tenant: its own, else the '*' default.
func ruleFor(rules []Limit, tenantID string) *Limit {
	var star *Limit
	for i := range rules {
		switch rules[i].TenantID {
		case tenantID:
			return &rules[i]
		case AllTenants:
			star = &rules[i]
		}
	}
	return star
}

// Admit decides whether a new job may be submitted for tenant on queue. It returns
// ErrBacklogFull when the applicable rule's max_backlog is reached, so the API can answer 429
// and the producer slows down instead of the queue growing without bound.
//
// The check is approximate by design: backlog counts are cached briefly and each admitted job
// bumps the cached count, so a burst overshoots by at most what other API replicas admit within
// the cache window. Backpressure needs to be roughly right and cheap, not transactional.
func (s *Store) Admit(ctx context.Context, tenantID, queue string) error {
	if queue == "" {
		queue = job.DefaultQueue
	}
	rules, err := s.lim.rules(ctx, s.pool, queue)
	if err != nil {
		return err
	}
	r := ruleFor(rules, tenantID)
	if r == nil || r.MaxBacklog == nil {
		return nil
	}

	key := queue + "\x00" + r.TenantID
	s.lim.mu.Lock()
	c, ok := s.lim.backlog[key]
	fresh := ok && time.Since(c.at) < s.lim.backlogTTL
	s.lim.mu.Unlock()

	if !fresh {
		n, err := s.countBacklog(ctx, queue, r, rules)
		if err != nil {
			return err
		}
		c = cachedCount{at: time.Now(), n: n}
	}
	if c.n >= *r.MaxBacklog {
		s.lim.mu.Lock()
		s.lim.backlog[key] = c
		s.lim.mu.Unlock()
		return ErrBacklogFull
	}
	c.n++ // count this admission locally until the next refresh
	s.lim.mu.Lock()
	s.lim.backlog[key] = c
	s.lim.mu.Unlock()
	return nil
}

// countBacklog counts pending+retrying jobs in the rule's scope, stopping at max_backlog so
// the query is bounded no matter how large the backlog is.
func (s *Store) countBacklog(ctx context.Context, queue string, r *Limit, rules []Limit) (int, error) {
	var n int
	var err error
	if r.TenantID == AllTenants {
		err = s.pool.QueryRow(ctx, `SELECT count(*) FROM (SELECT 1 FROM jobs
			WHERE queue = $1 AND status IN ('pending','retrying') AND tenant_id <> ALL($2::text[]) LIMIT $3) t`,
			queue, groupsExclusions(rules), *r.MaxBacklog).Scan(&n)
	} else {
		err = s.pool.QueryRow(ctx, `SELECT count(*) FROM (SELECT 1 FROM jobs
			WHERE queue = $1 AND status IN ('pending','retrying') AND tenant_id = $2 LIMIT $3) t`,
			queue, r.TenantID, *r.MaxBacklog).Scan(&n)
	}
	if err != nil {
		return 0, fmt.Errorf("count backlog: %w", err)
	}
	return n, nil
}

// FindByIdempotencyKey returns the tenant's job with that key, or ErrNotFound. The API uses it
// so a client retry of an already-accepted submit succeeds even while the queue is full.
func (s *Store) FindByIdempotencyKey(ctx context.Context, tenantID, key string) (job.Job, error) {
	j, err := scanJob(s.pool.QueryRow(ctx,
		`SELECT `+jobColumns+` FROM jobs WHERE tenant_id = $1 AND idempotency_key = $2`, tenantID, key))
	if errors.Is(err, pgx.ErrNoRows) {
		return job.Job{}, ErrNotFound
	}
	if err != nil {
		return job.Job{}, fmt.Errorf("find by key: %w", err)
	}
	return j, nil
}

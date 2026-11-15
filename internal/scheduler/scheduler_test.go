package scheduler_test

import (
	"context"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
	"go.opentelemetry.io/otel"

	"github.com/vasistamagam/distributed-jobscheduler/internal/scheduler"
	"github.com/vasistamagam/distributed-jobscheduler/internal/testdb"
)

func quiet() *slog.Logger { return slog.New(slog.NewTextHandler(io.Discard, nil)) }

func newStore(t *testing.T) (*scheduler.Store, *pgxpool.Pool) {
	t.Helper()
	pool := testdb.New(t, 40)
	return scheduler.NewStore(pool), pool
}

func eventually(t *testing.T, timeout time.Duration, what string, cond func() bool) {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		if cond() {
			return
		}
		time.Sleep(20 * time.Millisecond)
	}
	t.Fatalf("timed out after %v waiting for %s", timeout, what)
}

func fireCfg() scheduler.FireConfig {
	return scheduler.FireConfig{Tracer: otel.Tracer("test"), Log: quiet(), Grace: time.Minute}
}

func newSched(acme string) scheduler.NewSchedule {
	return scheduler.NewSchedule{TenantID: "acme", Name: acme, CronExpr: "0 * * * *", Type: "noop"}
}

func countJobs(t *testing.T, pool *pgxpool.Pool) (total, distinctKeys int) {
	t.Helper()
	err := pool.QueryRow(context.Background(), `SELECT count(*), count(DISTINCT idempotency_key) FROM jobs`).Scan(&total, &distinctKeys)
	if err != nil {
		t.Fatal(err)
	}
	return
}

func TestStoreCRUD(t *testing.T) {
	ctx := context.Background()
	s, _ := newStore(t)

	sc, err := s.Create(ctx, newSched("hourly"))
	if err != nil {
		t.Fatal(err)
	}
	if sc.MisfirePolicy != scheduler.FireOnce || sc.Timezone != "UTC" || sc.Queue != "default" ||
		sc.MaxAttempts != 5 || !sc.Enabled || !sc.NextFireAt.After(time.Now()) {
		t.Fatalf("defaults: %+v", sc)
	}
	if _, err := s.Create(ctx, newSched("hourly")); !errors.Is(err, scheduler.ErrDuplicateName) {
		t.Fatalf("duplicate name: %v", err)
	}
	bad := newSched("bad")
	bad.CronExpr = "nope"
	if _, err := s.Create(ctx, bad); !errors.Is(err, scheduler.ErrInvalid) {
		t.Fatalf("bad cron: %v", err)
	}
	bad = newSched("badtz")
	bad.Timezone = "Nowhere/Land"
	if _, err := s.Create(ctx, bad); !errors.Is(err, scheduler.ErrInvalid) {
		t.Fatalf("bad tz: %v", err)
	}
	bad = newSched("badpol")
	bad.MisfirePolicy = "yolo"
	if _, err := s.Create(ctx, bad); !errors.Is(err, scheduler.ErrInvalid) {
		t.Fatalf("bad policy: %v", err)
	}
	other := newSched("hourly")
	other.TenantID = "globex" // same name, different tenant: allowed
	if _, err := s.Create(ctx, other); err != nil {
		t.Fatalf("same name in another tenant: %v", err)
	}

	if _, err := s.Get(ctx, "globex", sc.ID); !errors.Is(err, scheduler.ErrNotFound) {
		t.Fatalf("tenant isolation: %v", err)
	}
	if _, err := s.Get(ctx, "acme", "garbage"); !errors.Is(err, scheduler.ErrNotFound) {
		t.Fatalf("bad id: %v", err)
	}
	list, err := s.List(ctx, "acme")
	if err != nil || len(list) != 1 {
		t.Fatalf("list: %v %v", list, err)
	}

	if err := s.Delete(ctx, "globex", sc.ID); !errors.Is(err, scheduler.ErrNotFound) {
		t.Fatalf("delete other tenant: %v", err)
	}
	if err := s.Delete(ctx, "acme", sc.ID); err != nil {
		t.Fatal(err)
	}
	if err := s.Delete(ctx, "acme", sc.ID); !errors.Is(err, scheduler.ErrNotFound) {
		t.Fatalf("double delete: %v", err)
	}
}

func TestPauseResumeDoesNotMisfire(t *testing.T) {
	ctx := context.Background()
	s, pool := newStore(t)
	sc, _ := s.Create(ctx, newSched("p"))
	paused, err := s.SetEnabled(ctx, "acme", sc.ID, false)
	if err != nil || paused.Enabled {
		t.Fatalf("pause: %+v %v", paused, err)
	}
	// Simulate a long pause: the stored slot is now far in the past.
	_, _ = pool.Exec(ctx, `UPDATE schedules SET next_fire_at = now() - interval '7 days'`)
	// Paused schedules never fire.
	if res, _ := s.FireDue(ctx, fireCfg()); res.Fired != 0 {
		t.Fatalf("paused schedule fired: %+v", res)
	}
	resumed, err := s.SetEnabled(ctx, "acme", sc.ID, true)
	if err != nil || !resumed.Enabled || !resumed.NextFireAt.After(time.Now()) {
		t.Fatalf("resume must move next_fire_at into the future: %+v %v", resumed, err)
	}
}

func TestFireDueCreatesJobOnce(t *testing.T) {
	ctx := context.Background()
	s, pool := newStore(t)
	n := newSched("daily")
	n.Payload = []byte(`{"report":"sales"}`)
	n.Queue = "reports"
	n.Priority = 3
	sc, _ := s.Create(ctx, n)
	_, _ = pool.Exec(ctx, `UPDATE schedules SET next_fire_at = now() - interval '10 seconds'`)

	res, err := s.FireDue(ctx, fireCfg())
	if err != nil || res.Fired != 1 || res.Schedules != 1 {
		t.Fatalf("first fire: %+v %v", res, err)
	}
	var typ, queue, key string
	var prio int
	var payload string
	err = pool.QueryRow(ctx, `SELECT type, queue, priority, payload::text, idempotency_key FROM jobs`).Scan(&typ, &queue, &prio, &payload, &key)
	if err != nil || typ != "noop" || queue != "reports" || prio != 3 || payload != `{"report": "sales"}` {
		t.Fatalf("job: %s %s %d %s %v", typ, queue, prio, payload, err)
	}
	if len(key) < 10 || key[:5] != "cron:" {
		t.Fatalf("key %q", key)
	}
	got, _ := s.Get(ctx, "acme", sc.ID)
	if got.LastFiredAt == nil || !got.NextFireAt.After(time.Now()) {
		t.Fatalf("schedule not advanced: %+v", got)
	}
	if res, _ := s.FireDue(ctx, fireCfg()); res.Fired != 0 || res.Schedules != 0 {
		t.Fatalf("nothing is due now: %+v", res)
	}
}

// Layer 2 of double-fire protection: even if next_fire_at is rewound (split-brain leader,
// restored backup, operator error), the deterministic per-slot key blocks a second job.
func TestIdempotentKeyBlocksReFireOfSameSlot(t *testing.T) {
	ctx := context.Background()
	s, pool := newStore(t)
	sc, _ := s.Create(ctx, newSched("rewind"))
	slot := time.Now().UTC().Add(-time.Hour).Truncate(time.Hour)
	set := func() {
		if _, err := pool.Exec(ctx, `UPDATE schedules SET next_fire_at = $1 WHERE id = $2`, slot, sc.ID); err != nil {
			t.Fatal(err)
		}
	}
	set()
	if res, err := s.FireDue(ctx, fireCfg()); err != nil || res.Fired != 1 {
		t.Fatalf("first: %+v %v", res, err)
	}
	set() // rewind to the very same slot
	res, err := s.FireDue(ctx, fireCfg())
	if err != nil || res.Fired != 0 || res.Skipped != 1 {
		t.Fatalf("re-fire must be blocked by the key: %+v %v", res, err)
	}
	if total, _ := countJobs(t, pool); total != 1 {
		t.Fatalf("jobs=%d, want exactly 1", total)
	}
}

type countingObserver struct {
	fired, skipped atomic.Int64
}

func (o *countingObserver) Leader(bool) {}
func (o *countingObserver) Fired(f, s int) {
	o.fired.Add(int64(f))
	o.skipped.Add(int64(s))
}

// Phase 4 acceptance: it never double-fires. Worst case: 6 scheduler replicas with leader
// election DISABLED all hammer the same backlog. Row locks alone (layer 1) must give
// exactly one job per (schedule, slot), and the idempotent key must never even be needed.
func TestNeverDoubleFiresWithoutLeaderElection(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	s, pool := newStore(t)

	const schedules = 20
	for i := 0; i < schedules; i++ {
		n := newSched(fmt.Sprintf("yearly-%02d", i))
		n.CronExpr = "0 0 1 1 *" // Jan 1st: slots are fixed, independent of when the test runs
		n.MisfirePolicy = scheduler.FireAll
		if _, err := s.Create(ctx, n); err != nil {
			t.Fatal(err)
		}
	}
	// Rewind every schedule 4 years: slots Jan 1 of Y-4..Y = 5 each, so 100 jobs in total.
	_, err := pool.Exec(ctx, `UPDATE schedules SET next_fire_at =
		make_timestamptz(extract(year FROM now())::int - 4, 1, 1, 0, 0, 0, 'UTC')`)
	if err != nil {
		t.Fatal(err)
	}

	obs := &countingObserver{}
	var wg sync.WaitGroup
	for i := 0; i < 6; i++ {
		sch := scheduler.New(s, scheduler.Config{Tick: 10 * time.Millisecond, DisableLock: true}, quiet(),
			scheduler.WithObserver(obs))
		wg.Add(1)
		go func() { defer wg.Done(); sch.Run(ctx) }()
	}
	eventually(t, 20*time.Second, "all schedules to catch up", func() bool {
		var behind int
		_ = pool.QueryRow(ctx, `SELECT count(*) FROM schedules WHERE next_fire_at <= now()`).Scan(&behind)
		return behind == 0
	})
	time.Sleep(300 * time.Millisecond) // give any (incorrect) late duplicate time to appear
	cancel()
	wg.Wait()

	total, distinct := countJobs(t, pool)
	if total != schedules*5 || distinct != total {
		t.Fatalf("jobs=%d distinct keys=%d, want %d/%d", total, distinct, schedules*5, schedules*5)
	}
	if obs.fired.Load() != int64(total) {
		t.Fatalf("observer saw %d fires for %d jobs", obs.fired.Load(), total)
	}
	if obs.skipped.Load() != 0 {
		t.Fatalf("row locks should prevent any duplicate attempt, but the key blocked %d", obs.skipped.Load())
	}
}

// leaderPID returns the backend holding the scheduler advisory lock, or false if nobody does
// (which is the normal, transient state between a leader dying and a standby taking over).
func leaderPID(pool *pgxpool.Pool) (int, bool) {
	var pid int
	err := pool.QueryRow(context.Background(),
		`SELECT pid FROM pg_locks WHERE locktype = 'advisory' AND classid::bigint = $1::bigint AND objid = 1 AND granted`,
		0x6a6f6273).Scan(&pid)
	return pid, err == nil
}

type replica struct {
	cancel context.CancelFunc
	done   chan struct{}
	leads  atomic.Int32 // times this replica started leading
	active atomic.Bool
}

// startElectors runs n electors and tracks the maximum number of simultaneous leaders.
func startElectors(t *testing.T, dsn string, n int) ([]*replica, *atomic.Int32) {
	t.Helper()
	var concurrent, maxConcurrent atomic.Int32
	reps := make([]*replica, n)
	for i := range reps {
		r := &replica{done: make(chan struct{})}
		reps[i] = r
		ctx, cancel := context.WithCancel(context.Background())
		r.cancel = cancel
		e := &scheduler.Elector{DSN: dsn, Retry: 50 * time.Millisecond, Check: 100 * time.Millisecond, Log: quiet()}
		go func() {
			defer close(r.done)
			e.Run(ctx, func(lctx context.Context) {
				r.leads.Add(1)
				r.active.Store(true)
				c := concurrent.Add(1)
				for {
					m := maxConcurrent.Load()
					if c <= m || maxConcurrent.CompareAndSwap(m, c) {
						break
					}
				}
				<-lctx.Done()
				concurrent.Add(-1)
				r.active.Store(false)
			})
		}()
		t.Cleanup(cancel)
	}
	return reps, &maxConcurrent
}

func leaderIndex(reps []*replica) int {
	idx := -1
	for i, r := range reps {
		if r.active.Load() {
			if idx != -1 {
				return -2 // two leaders at once
			}
			idx = i
		}
	}
	return idx
}

func TestElectionSingleLeaderAndFailover(t *testing.T) {
	_, pool := newStore(t)
	reps, maxLeaders := startElectors(t, testdb.DSN(pool), 3)

	eventually(t, 5*time.Second, "a leader", func() bool { return leaderIndex(reps) >= 0 })
	first := leaderIndex(reps)

	// Stop the leader (like a pod being deleted): a standby must take over.
	reps[first].cancel()
	<-reps[first].done
	eventually(t, 5*time.Second, "failover", func() bool {
		i := leaderIndex(reps)
		return i >= 0 && i != first
	})
	if m := maxLeaders.Load(); m != 1 {
		t.Fatalf("max simultaneous leaders = %d, want 1", m)
	}
}

// A leader whose database session is killed (network partition, failover, admin action)
// must stop leading, and exactly one replica ends up leading.
//
// Unlike a graceful stop, a killed session can overlap: Postgres frees the lock instantly and
// a standby may win it while the old leader has not yet noticed (it finds out on its next
// check, at most one Check interval later). That bounded split-brain window is precisely why
// double-fire safety rests on row locks and the idempotent fire key, not on the election
// (see TestNeverDoubleFiresWithoutLeaderElection and TestIdempotentKeyBlocksReFireOfSameSlot).
func TestElectionLeaderLosesLockWhenSessionDies(t *testing.T) {
	_, pool := newStore(t)
	reps, maxLeaders := startElectors(t, testdb.DSN(pool), 2)
	eventually(t, 5*time.Second, "a leader", func() bool { return leaderIndex(reps) >= 0 })

	pid, ok := leaderPID(pool)
	if !ok {
		t.Fatal("no backend holds the advisory lock although a replica reports leading")
	}
	if _, err := pool.Exec(context.Background(), `SELECT pg_terminate_backend($1)`, pid); err != nil {
		t.Fatal(err)
	}
	// Converges to exactly one active leader, holding the lock on a new session.
	eventually(t, 10*time.Second, "a single new leader on a new session", func() bool {
		newPID, held := leaderPID(pool)
		return held && newPID != pid && leaderIndex(reps) >= 0
	})
	if m := maxLeaders.Load(); m > 2 {
		t.Fatalf("max simultaneous leaders = %d, want at most 2 (old + new during the detection window)", m)
	}
}

// Full path with real election: 2 replicas, a per-second schedule. Each slot fires exactly once.
func TestTwoReplicasFireEachSlotOnce(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	s, pool := newStore(t)
	n := newSched("every-second")
	n.CronExpr = "@every 1s"
	n.MisfirePolicy = scheduler.FireAll
	if _, err := s.Create(ctx, n); err != nil {
		t.Fatal(err)
	}

	dsn := testdb.DSN(pool)
	var reps [2]*scheduler.Scheduler
	var wg sync.WaitGroup
	for i := range reps {
		reps[i] = scheduler.New(s, scheduler.Config{DSN: dsn, Tick: 100 * time.Millisecond, RetryElect: 100 * time.Millisecond},
			quiet())
		wg.Add(1)
		go func() { defer wg.Done(); reps[i].Run(ctx) }()
	}
	defer func() { cancel(); wg.Wait() }()

	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		if reps[0].IsLeader() && reps[1].IsLeader() {
			t.Fatal("both replicas report leadership")
		}
		time.Sleep(20 * time.Millisecond)
	}
	total, distinct := countJobs(t, pool)
	if total < 3 || total > 6 || total != distinct {
		t.Fatalf("jobs=%d distinct=%d, want 3..6 and all distinct", total, distinct)
	}
}

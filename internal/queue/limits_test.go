package queue_test

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/vasistamagam/distributed-jobscheduler/internal/job"
	"github.com/vasistamagam/distributed-jobscheduler/internal/queue"
	"github.com/vasistamagam/distributed-jobscheduler/internal/testdb"
)

func ptr[T any](v T) *T { return &v }

// newLimitedStore is a store with caching off, so limit changes and counts are exact.
func newLimitedStore(t *testing.T) *queue.Store {
	t.Helper()
	s := queue.New(testdb.New(t, 30))
	s.SetLimitCacheTTL(0, 0)
	return s
}

func seed(t *testing.T, s *queue.Store, tenant string, n int) {
	t.Helper()
	for i := 0; i < n; i++ {
		mustEnqueue(t, s, job.NewJob{TenantID: tenant, Type: "x"})
	}
}

func running(t *testing.T, s *queue.Store, tenant string) int {
	t.Helper()
	var n int
	err := s.Pool().QueryRow(context.Background(),
		`SELECT count(*) FROM jobs WHERE status='running' AND ($1 = '' OR tenant_id = $1)`, tenant).Scan(&n)
	if err != nil {
		t.Fatal(err)
	}
	return n
}

func claim(t *testing.T, s *queue.Store, worker string, n int) []job.Job {
	t.Helper()
	got, err := s.Claim(context.Background(), "default", worker, n, time.Minute)
	if err != nil {
		t.Fatalf("claim: %v", err)
	}
	return got
}

func TestSetLimitValidation(t *testing.T) {
	s := newLimitedStore(t)
	ctx := context.Background()
	tests := []struct {
		name string
		l    queue.Limit
		ok   bool
	}{
		{"cap only", queue.Limit{Queue: "q", TenantID: "t", MaxRunning: ptr(3)}, true},
		{"rate defaults burst", queue.Limit{Queue: "q", TenantID: "t", RatePerSecond: ptr(2.5)}, true},
		{"backlog only", queue.Limit{Queue: "q", TenantID: "*", MaxBacklog: ptr(10)}, true},
		{"nothing set", queue.Limit{Queue: "q", TenantID: "t"}, false},
		{"zero cap", queue.Limit{Queue: "q", TenantID: "t", MaxRunning: ptr(0)}, false},
		{"zero rate", queue.Limit{Queue: "q", TenantID: "t", RatePerSecond: ptr(0.0)}, false},
		{"burst without rate", queue.Limit{Queue: "q", TenantID: "t", Burst: ptr(5), MaxRunning: ptr(1)}, false},
		{"missing tenant", queue.Limit{Queue: "q", MaxRunning: ptr(1)}, false},
		{"missing queue", queue.Limit{TenantID: "t", MaxRunning: ptr(1)}, false},
		{"zero backlog", queue.Limit{Queue: "q", TenantID: "t", MaxBacklog: ptr(0)}, false},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := s.SetLimit(ctx, tt.l)
			if tt.ok && err != nil {
				t.Fatalf("unexpected error: %v", err)
			}
			if !tt.ok && !errors.Is(err, queue.ErrInvalidLimit) {
				t.Fatalf("want ErrInvalidLimit, got %v", err)
			}
		})
	}
	list, err := s.ListLimits(ctx)
	if err != nil || len(list) != 2 { // ("q","t") was replaced, ("q","*") added
		t.Fatalf("list: %v %v", list, err)
	}
	for _, l := range list {
		if l.TenantID == "t" && (l.Burst == nil || *l.Burst != 3) {
			t.Fatalf("burst should default to ceil(rate)=3, got %+v", l)
		}
	}
	if err := s.DeleteLimit(ctx, "q", "t"); err != nil {
		t.Fatal(err)
	}
	if err := s.DeleteLimit(ctx, "q", "t"); !errors.Is(err, queue.ErrLimitNotFound) {
		t.Fatalf("second delete: %v", err)
	}
}

// The concurrency cap must hold exactly even when many workers claim at the same instant:
// this is what the row-locked rule is for.
func TestConcurrencyCapIsExactUnderContention(t *testing.T) {
	ctx := context.Background()
	s := newLimitedStore(t)
	seed(t, s, "acme", 300)
	if err := s.SetLimit(ctx, queue.Limit{Queue: "default", TenantID: "acme", MaxRunning: ptr(5)}); err != nil {
		t.Fatal(err)
	}

	var wg sync.WaitGroup
	var total atomic.Int64
	stop := make(chan struct{})
	var peak atomic.Int64
	go func() { // watcher: sample the running count continuously
		for {
			select {
			case <-stop:
				return
			default:
				var n int64
				_ = s.Pool().QueryRow(ctx, `SELECT count(*) FROM jobs WHERE status='running'`).Scan(&n)
				for {
					p := peak.Load()
					if n <= p || peak.CompareAndSwap(p, n) {
						break
					}
				}
			}
		}
	}()
	for w := 0; w < 16; w++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for i := 0; i < 15; i++ { // claim and never complete: jobs stay running
				got, err := s.Claim(ctx, "default", fmt.Sprintf("w%d", w), 10, time.Minute)
				if err != nil {
					t.Errorf("claim: %v", err)
					return
				}
				total.Add(int64(len(got)))
			}
		}()
	}
	wg.Wait()
	close(stop)

	if total.Load() != 5 || running(t, s, "") != 5 {
		t.Fatalf("claimed %d, running %d; the cap is 5", total.Load(), running(t, s, ""))
	}
	if peak.Load() > 5 {
		t.Fatalf("running count peaked at %d, above the cap of 5", peak.Load())
	}

	// Finishing a job frees a slot.
	var one job.Job
	_ = s.Pool().QueryRow(ctx, `SELECT id, locked_by, attempts FROM jobs WHERE status='running' LIMIT 1`).
		Scan(&one.ID, new(*string), &one.Attempts)
	var by string
	_ = s.Pool().QueryRow(ctx, `SELECT locked_by FROM jobs WHERE id=$1`, one.ID).Scan(&by)
	if err := s.Complete(ctx, job.Ref{ID: one.ID, WorkerID: by, Attempts: one.Attempts}); err != nil {
		t.Fatal(err)
	}
	if got := claim(t, s, "w-late", 10); len(got) != 1 {
		t.Fatalf("one slot freed, want 1 claim, got %d", len(got))
	}
}

// A limited tenant must not slow anyone else down: that is the whole point of fairness.
func TestLimitOnlyAffectsItsOwnTenant(t *testing.T) {
	ctx := context.Background()
	s := newLimitedStore(t)
	seed(t, s, "noisy", 50)
	seed(t, s, "quiet", 50)
	if err := s.SetLimit(ctx, queue.Limit{Queue: "default", TenantID: "noisy", MaxRunning: ptr(2)}); err != nil {
		t.Fatal(err)
	}
	for i := 0; i < 20; i++ {
		claim(t, s, "w", 10)
	}
	if got := running(t, s, "noisy"); got != 2 {
		t.Fatalf("noisy running=%d, want exactly its cap of 2", got)
	}
	if got := running(t, s, "quiet"); got != 50 {
		t.Fatalf("quiet running=%d, want all 50 (it has no limit)", got)
	}
}

// Two limited tenants must both make progress even though each claim batch is small.
func TestLimitedGroupsAreNotStarved(t *testing.T) {
	ctx := context.Background()
	s := newLimitedStore(t)
	seed(t, s, "a", 100)
	seed(t, s, "b", 100)
	for _, tn := range []string{"a", "b"} {
		if err := s.SetLimit(ctx, queue.Limit{Queue: "default", TenantID: tn, MaxRunning: ptr(5)}); err != nil {
			t.Fatal(err)
		}
	}
	for i := 0; i < 60; i++ {
		claim(t, s, "w", 3)
	}
	if running(t, s, "a") != 5 || running(t, s, "b") != 5 {
		t.Fatalf("a=%d b=%d, want both at their cap of 5", running(t, s, "a"), running(t, s, "b"))
	}
}

// The '*' rule is one shared allowance for every tenant without a rule of their own.
func TestWildcardRuleIsSharedAndOverridable(t *testing.T) {
	ctx := context.Background()
	s := newLimitedStore(t)
	for _, tn := range []string{"x", "y", "vip"} {
		seed(t, s, tn, 30)
	}
	must := func(l queue.Limit) {
		t.Helper()
		if err := s.SetLimit(ctx, l); err != nil {
			t.Fatal(err)
		}
	}
	must(queue.Limit{Queue: "default", TenantID: queue.AllTenants, MaxRunning: ptr(3)}) // shared by x and y
	must(queue.Limit{Queue: "default", TenantID: "vip", MaxRunning: ptr(10)})           // its own, higher cap
	for i := 0; i < 40; i++ {
		claim(t, s, "w", 8)
	}
	shared := running(t, s, "x") + running(t, s, "y")
	if shared != 3 {
		t.Fatalf("x+y running = %d, want the shared cap of 3", shared)
	}
	if got := running(t, s, "vip"); got != 10 {
		t.Fatalf("vip running = %d, want its own cap of 10", got)
	}
}

// A tenant with only a backlog rule is not concurrency-limited, even when '*' limits others.
func TestBacklogOnlyRuleDoesNotLimitClaims(t *testing.T) {
	ctx := context.Background()
	s := newLimitedStore(t)
	seed(t, s, "other", 20)
	seed(t, s, "free", 20)
	_ = s.SetLimit(ctx, queue.Limit{Queue: "default", TenantID: queue.AllTenants, MaxRunning: ptr(2)})
	_ = s.SetLimit(ctx, queue.Limit{Queue: "default", TenantID: "free", MaxBacklog: ptr(1000)})
	for i := 0; i < 30; i++ {
		claim(t, s, "w", 10)
	}
	if running(t, s, "other") != 2 || running(t, s, "free") != 20 {
		t.Fatalf("other=%d free=%d, want 2 and 20", running(t, s, "other"), running(t, s, "free"))
	}
}

func TestRateLimitTokenBucket(t *testing.T) {
	ctx := context.Background()
	s := newLimitedStore(t)
	seed(t, s, "acme", 200)
	const rate, burst = 20.0, 5
	if err := s.SetLimit(ctx, queue.Limit{Queue: "default", TenantID: "acme", RatePerSecond: ptr(rate), Burst: ptr(burst)}); err != nil {
		t.Fatal(err)
	}
	// Every expectation below is derived from the time that really elapsed between claims, not a
	// hardcoded gap: on a slow machine (race detector, fsync) back-to-back claims are far apart.
	t0 := time.Now()
	if got := claim(t, s, "w", 100); len(got) != burst {
		t.Fatalf("a full bucket allows the burst: got %d, want %d", len(got), burst)
	}
	gap := time.Since(t0)
	got := claim(t, s, "w", 100)
	if maxTokens := int(gap.Seconds()*rate) + 1; len(got) > maxTokens {
		t.Fatalf("claimed %d only %v after the burst; at %.0f/s at most %d tokens can have refilled", len(got), gap, rate, maxTokens)
	}

	time.Sleep(400 * time.Millisecond) // refills about 8 tokens, capped at the burst of 5
	got = claim(t, s, "w", 100)
	if len(got) < 4 || len(got) > burst {
		t.Fatalf("after 400ms want about %d (capped by burst), got %d", burst, len(got))
	}
	// Idle long enough to refill fully: the bucket must not accumulate beyond the burst.
	time.Sleep(600 * time.Millisecond)
	if got := claim(t, s, "w", 100); len(got) != burst {
		t.Fatalf("burst cap violated after idling: got %d, want %d", len(got), burst)
	}
}

// Many concurrent claimers must not collectively exceed the configured rate.
func TestRateLimitHoldsAcrossConcurrentClaimers(t *testing.T) {
	ctx := context.Background()
	s := newLimitedStore(t)
	seed(t, s, "acme", 1000)
	const rate, burst = 100.0, 10
	if err := s.SetLimit(ctx, queue.Limit{Queue: "default", TenantID: "acme", RatePerSecond: ptr(rate), Burst: ptr(burst)}); err != nil {
		t.Fatal(err)
	}
	start := time.Now()
	var total atomic.Int64
	var wg sync.WaitGroup
	for w := 0; w < 12; w++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for time.Since(start) < 1500*time.Millisecond {
				got, err := s.Claim(ctx, "default", fmt.Sprintf("w%d", w), 50, time.Minute)
				if err != nil {
					t.Errorf("claim: %v", err)
					return
				}
				total.Add(int64(len(got)))
				time.Sleep(5 * time.Millisecond)
			}
		}()
	}
	wg.Wait()
	elapsed := time.Since(start).Seconds()
	maxAllowed := burst + rate*elapsed + 3 // +3 for timing slop at the boundaries
	if float64(total.Load()) > maxAllowed {
		t.Fatalf("claimed %d jobs in %.2fs, exceeding burst+rate*t = %.0f", total.Load(), elapsed, maxAllowed)
	}
	if float64(total.Load()) < rate*elapsed*0.5 {
		t.Fatalf("claimed only %d in %.2fs: the limiter is far too strict", total.Load(), elapsed)
	}
}

func TestRemovingALimitUnblocksClaims(t *testing.T) {
	ctx := context.Background()
	s := newLimitedStore(t)
	seed(t, s, "acme", 10)
	_ = s.SetLimit(ctx, queue.Limit{Queue: "default", TenantID: "acme", MaxRunning: ptr(1)})
	claim(t, s, "w", 10)
	if running(t, s, "acme") != 1 {
		t.Fatalf("cap not applied")
	}
	if err := s.DeleteLimit(ctx, "default", "acme"); err != nil {
		t.Fatal(err)
	}
	if got := claim(t, s, "w", 10); len(got) != 9 {
		t.Fatalf("after removing the limit the rest should flow: got %d", len(got))
	}
}

func TestAdmitBackpressure(t *testing.T) {
	ctx := context.Background()
	s := newLimitedStore(t)
	if err := s.SetLimit(ctx, queue.Limit{Queue: "default", TenantID: "acme", MaxBacklog: ptr(3)}); err != nil {
		t.Fatal(err)
	}
	for i := 0; i < 3; i++ {
		if err := s.Admit(ctx, "acme", "default"); err != nil {
			t.Fatalf("admit %d: %v", i, err)
		}
		mustEnqueue(t, s, job.NewJob{TenantID: "acme", Type: "x"})
	}
	if err := s.Admit(ctx, "acme", "default"); !errors.Is(err, queue.ErrBacklogFull) {
		t.Fatalf("backlog is at the limit, want ErrBacklogFull, got %v", err)
	}
	if err := s.Admit(ctx, "globex", "default"); err != nil {
		t.Fatalf("another tenant has no rule and must be admitted: %v", err)
	}
	if err := s.Admit(ctx, "acme", "other-queue"); err != nil {
		t.Fatalf("the rule is per queue: %v", err)
	}
	// Running jobs are not backlog: claiming one makes room.
	claim(t, s, "w", 1)
	if err := s.Admit(ctx, "acme", "default"); err != nil {
		t.Fatalf("a claimed job no longer counts toward the backlog: %v", err)
	}
}

func TestAdmitWildcardAppliesToTenantsWithoutRules(t *testing.T) {
	ctx := context.Background()
	s := newLimitedStore(t)
	_ = s.SetLimit(ctx, queue.Limit{Queue: "default", TenantID: queue.AllTenants, MaxBacklog: ptr(2)})
	_ = s.SetLimit(ctx, queue.Limit{Queue: "default", TenantID: "vip", MaxBacklog: ptr(100)})
	seed(t, s, "a", 1)
	seed(t, s, "b", 1) // shared backlog of 2 is now full
	if err := s.Admit(ctx, "c", "default"); !errors.Is(err, queue.ErrBacklogFull) {
		t.Fatalf("shared backlog full: %v", err)
	}
	if err := s.Admit(ctx, "vip", "default"); err != nil {
		t.Fatalf("vip has its own, larger rule and its own backlog scope: %v", err)
	}
}

// With caching on, admission stays cheap and still bounds a burst.
func TestAdmitCacheBoundsABurst(t *testing.T) {
	ctx := context.Background()
	s := queue.New(testdb.New(t, 10)) // default caching
	if err := s.SetLimit(ctx, queue.Limit{Queue: "default", TenantID: "acme", MaxBacklog: ptr(10)}); err != nil {
		t.Fatal(err)
	}
	admitted := 0
	for i := 0; i < 50; i++ {
		if s.Admit(ctx, "acme", "default") == nil {
			admitted++
			mustEnqueue(t, s, job.NewJob{TenantID: "acme", Type: "x"})
		}
	}
	if admitted != 10 {
		t.Fatalf("admitted %d of 50, want exactly the limit of 10 from a single process", admitted)
	}
}

func TestFindByIdempotencyKey(t *testing.T) {
	ctx := context.Background()
	s := newLimitedStore(t)
	key := "k"
	j := mustEnqueue(t, s, job.NewJob{TenantID: "acme", Type: "x", IdempotencyKey: &key})
	got, err := s.FindByIdempotencyKey(ctx, "acme", "k")
	if err != nil || got.ID != j.ID {
		t.Fatalf("find: %v %v", got, err)
	}
	if _, err := s.FindByIdempotencyKey(ctx, "other", "k"); !errors.Is(err, queue.ErrNotFound) {
		t.Fatalf("tenant isolation: %v", err)
	}
}

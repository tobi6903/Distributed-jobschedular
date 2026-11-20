package queue_test

import (
	"context"
	"encoding/json"
	"errors"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/vasistamagam/distributed-jobscheduler/internal/job"
	"github.com/vasistamagam/distributed-jobscheduler/internal/queue"
	"github.com/vasistamagam/distributed-jobscheduler/internal/testdb"
)

func newStore(t *testing.T) *queue.Store {
	t.Helper()
	return queue.New(testdb.New(t, 20))
}

func nj(typ string) job.NewJob {
	return job.NewJob{TenantID: "acme", Type: typ, Payload: json.RawMessage(`{"n":1}`)}
}

func mustEnqueue(t *testing.T, s *queue.Store, n job.NewJob) job.Job {
	t.Helper()
	j, _, err := s.Enqueue(context.Background(), n)
	if err != nil {
		t.Fatalf("enqueue: %v", err)
	}
	return j
}

// Pooled connections plan every statement with its real parameters. A cached generic plan built while the jobs table
// was empty made a 60,000-job burst run 2.5x slower on freshly started workers (see pool.go).
func TestConnectForcesCustomPlans(t *testing.T) {
	ctx := context.Background()
	dsn := testdb.New(t, 2).Config().ConnString()

	pool, err := queue.Connect(ctx, dsn, 2, 10*time.Second)
	if err != nil {
		t.Fatal(err)
	}
	defer pool.Close()
	var mode string
	if err := pool.QueryRow(ctx, `SHOW plan_cache_mode`).Scan(&mode); err != nil || mode != "force_custom_plan" {
		t.Fatalf("plan_cache_mode = %q (%v), want force_custom_plan", mode, err)
	}

	// An operator who chose a mode in the connection string keeps it.
	sep := "?"
	if strings.Contains(dsn, "?") {
		sep = "&"
	}
	pool2, err := queue.Connect(ctx, dsn+sep+"plan_cache_mode=auto", 2, 10*time.Second)
	if err != nil {
		t.Fatal(err)
	}
	defer pool2.Close()
	if err := pool2.QueryRow(ctx, `SHOW plan_cache_mode`).Scan(&mode); err != nil || mode != "auto" {
		t.Fatalf("an explicit plan_cache_mode must be respected: got %q (%v)", mode, err)
	}
}

func TestEnqueueDefaultsAndGet(t *testing.T) {
	ctx := context.Background()
	s := newStore(t)
	j, created, err := s.Enqueue(ctx, nj("email"))
	if err != nil || !created {
		t.Fatalf("enqueue: created=%v err=%v", created, err)
	}
	if j.Status != job.StatusPending || j.Queue != "default" || j.MaxAttempts != 5 || j.Attempts != 0 {
		t.Fatalf("unexpected defaults: %+v", j)
	}
	got, err := s.Get(ctx, "acme", j.ID)
	if err != nil || got.ID != j.ID || string(got.Payload) != `{"n": 1}` {
		t.Fatalf("get: %+v err=%v", got, err)
	}
}

func TestGetTenantIsolationAndBadID(t *testing.T) {
	ctx := context.Background()
	s := newStore(t)
	j := mustEnqueue(t, s, nj("email"))
	tests := []struct {
		name, tenant, id string
	}{
		{"other tenant", "evil", j.ID},
		{"unknown id", "acme", "00000000-0000-0000-0000-000000000000"},
		{"malformed id", "acme", "not-a-uuid"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if _, err := s.Get(ctx, tt.tenant, tt.id); !errors.Is(err, queue.ErrNotFound) {
				t.Fatalf("want ErrNotFound, got %v", err)
			}
		})
	}
}

func TestEnqueueIdempotency(t *testing.T) {
	ctx := context.Background()
	s := newStore(t)
	key := "order-42"
	n := nj("email")
	n.IdempotencyKey = &key

	a, created, err := s.Enqueue(ctx, n)
	if err != nil || !created {
		t.Fatalf("first: %v %v", created, err)
	}
	b, created, err := s.Enqueue(ctx, n)
	if err != nil || created || b.ID != a.ID {
		t.Fatalf("second should return same job: created=%v id %s vs %s err=%v", created, b.ID, a.ID, err)
	}
	// Same key, different tenant: independent namespace.
	n.TenantID = "other"
	c, created, err := s.Enqueue(ctx, n)
	if err != nil || !created || c.ID == a.ID {
		t.Fatalf("other tenant must get its own job: created=%v err=%v", created, err)
	}
}

func TestEnqueueIdempotencyConcurrent(t *testing.T) {
	ctx := context.Background()
	s := newStore(t)
	key := "dup"
	n := nj("email")
	n.IdempotencyKey = &key

	var wg sync.WaitGroup
	ids := make([]string, 20)
	createdCount := make(chan struct{}, 20)
	for i := range ids {
		wg.Add(1)
		go func() {
			defer wg.Done()
			j, created, err := s.Enqueue(ctx, n)
			if err != nil {
				t.Errorf("enqueue: %v", err)
				return
			}
			ids[i] = j.ID
			if created {
				createdCount <- struct{}{}
			}
		}()
	}
	wg.Wait()
	close(createdCount)
	if len(createdCount) != 1 {
		t.Fatalf("exactly one concurrent enqueue must create, got %d", len(createdCount))
	}
	for _, id := range ids {
		if id != ids[0] {
			t.Fatalf("all callers must see the same job: %v", ids)
		}
	}
}

func TestEnqueueDelayUsesDBClock(t *testing.T) {
	ctx := context.Background()
	s := newStore(t)
	n := nj("email")
	n.Delay = time.Hour
	j := mustEnqueue(t, s, n)
	if d := time.Until(j.RunAt); d < 59*time.Minute || d > 61*time.Minute {
		t.Fatalf("run_at should be ~1h ahead, got %v", d)
	}
	got, err := s.Claim(ctx, "default", "w1", 10, time.Minute)
	if err != nil || len(got) != 0 {
		t.Fatalf("delayed job must not be claimable: %v %v", got, err)
	}
}

func TestClaimBasics(t *testing.T) {
	ctx := context.Background()
	s := newStore(t)
	j := mustEnqueue(t, s, nj("email"))

	got, err := s.Claim(ctx, "default", "w1", 10, time.Minute)
	if err != nil || len(got) != 1 {
		t.Fatalf("claim: %v %v", got, err)
	}
	c := got[0]
	if c.ID != j.ID || c.Status != job.StatusRunning || c.Attempts != 1 ||
		c.LockedBy == nil || *c.LockedBy != "w1" || c.LockedUntil == nil {
		t.Fatalf("claimed job wrong: %+v", c)
	}
	if again, _ := s.Claim(ctx, "default", "w2", 10, time.Minute); len(again) != 0 {
		t.Fatalf("running job must not be claimed twice: %v", again)
	}
	if other, _ := s.Claim(ctx, "other-queue", "w2", 10, time.Minute); len(other) != 0 {
		t.Fatalf("claim is per-queue")
	}
}

func TestClaimOrderingPriorityThenRunAt(t *testing.T) {
	ctx := context.Background()
	s := newStore(t)
	mk := func(typ string, prio int16, delay time.Duration) {
		n := nj(typ)
		n.Priority = prio
		n.RunAt = time.Now().Add(delay)
		mustEnqueue(t, s, n)
	}
	mk("low-old", 0, -3*time.Minute)
	mk("high-new", 10, -1*time.Minute)
	mk("low-newer", 0, -2*time.Minute)
	mk("high-old", 10, -2*time.Minute)

	var order []string
	for i := 0; i < 4; i++ {
		got, err := s.Claim(ctx, "default", "w", 1, time.Minute)
		if err != nil || len(got) != 1 {
			t.Fatalf("claim %d: %v %v", i, got, err)
		}
		order = append(order, got[0].Type)
	}
	want := []string{"high-old", "high-new", "low-old", "low-newer"}
	for i := range want {
		if order[i] != want[i] {
			t.Fatalf("order=%v want %v", order, want)
		}
	}
}

func TestClaimBatchLimit(t *testing.T) {
	ctx := context.Background()
	s := newStore(t)
	for i := 0; i < 25; i++ {
		mustEnqueue(t, s, nj("email"))
	}
	got, err := s.Claim(ctx, "default", "w", 10, time.Minute)
	if err != nil || len(got) != 10 {
		t.Fatalf("want 10 got %d err=%v", len(got), err)
	}
}

// Concurrent claimers must receive disjoint sets: the core SKIP LOCKED guarantee.
func TestClaimConcurrentDisjoint(t *testing.T) {
	ctx := context.Background()
	s := newStore(t)
	const total = 500
	for i := 0; i < total; i++ {
		mustEnqueue(t, s, nj("email"))
	}
	var mu sync.Mutex
	seen := map[string]string{}
	var wg sync.WaitGroup
	for w := 0; w < 10; w++ {
		wid := string(rune('a' + w))
		wg.Add(1)
		go func() {
			defer wg.Done()
			for {
				got, err := s.Claim(ctx, "default", wid, 7, time.Minute)
				if err != nil {
					t.Errorf("claim: %v", err)
					return
				}
				if len(got) == 0 {
					return
				}
				mu.Lock()
				for _, j := range got {
					if prev, dup := seen[j.ID]; dup {
						t.Errorf("job %s claimed by both %s and %s", j.ID, prev, wid)
					}
					seen[j.ID] = wid
				}
				mu.Unlock()
			}
		}()
	}
	wg.Wait()
	if len(seen) != total {
		t.Fatalf("claimed %d of %d", len(seen), total)
	}
}

func TestCompleteIsFenced(t *testing.T) {
	ctx := context.Background()
	s := newStore(t)
	mustEnqueue(t, s, nj("email"))
	c, _ := s.Claim(ctx, "default", "w1", 1, time.Minute)
	ref := c[0].Ref()

	stale := ref
	stale.WorkerID = "w2"
	if err := s.Complete(ctx, stale); !errors.Is(err, queue.ErrLeaseLost) {
		t.Fatalf("wrong worker must be rejected: %v", err)
	}
	stale = ref
	stale.Attempts = 99
	if err := s.Complete(ctx, stale); !errors.Is(err, queue.ErrLeaseLost) {
		t.Fatalf("wrong attempt must be rejected: %v", err)
	}
	if err := s.Complete(ctx, ref); err != nil {
		t.Fatalf("complete: %v", err)
	}
	got, _ := s.Get(ctx, "acme", ref.ID)
	if got.Status != job.StatusSucceeded || got.FinishedAt == nil || got.LockedBy != nil {
		t.Fatalf("after complete: %+v", got)
	}
	if err := s.Complete(ctx, ref); !errors.Is(err, queue.ErrLeaseLost) {
		t.Fatalf("double complete must be rejected: %v", err)
	}
}

// CompleteBatch applies the SAME fence as Complete, per row: in a mixed batch the valid rows
// complete, and every stale one is rejected individually without disturbing its neighbours.
func TestCompleteBatchIsFencedPerRow(t *testing.T) {
	ctx := context.Background()
	s := newStore(t)
	for i := 0; i < 5; i++ {
		mustEnqueue(t, s, nj("email"))
	}
	claimed, err := s.Claim(ctx, "default", "w1", 5, time.Minute)
	if err != nil || len(claimed) != 5 {
		t.Fatalf("claim: %d %v", len(claimed), err)
	}
	refs := make([]job.Ref, 5)
	for i, c := range claimed {
		refs[i] = c.Ref()
	}
	refs[1].WorkerID = "someone-else" // wrong owner
	refs[3].Attempts = 99             // wrong attempt (a zombie holding an old claim)

	ok, err := s.CompleteBatch(ctx, refs)
	if err != nil {
		t.Fatalf("batch: %v", err)
	}
	want := []bool{true, false, true, false, true}
	for i := range want {
		if ok[i] != want[i] {
			t.Errorf("ok[%d] = %v, want %v", i, ok[i], want[i])
		}
	}
	for i, c := range claimed {
		got, _ := s.Get(ctx, "acme", c.ID)
		if want[i] && (got.Status != job.StatusSucceeded || got.FinishedAt == nil || got.LockedBy != nil) {
			t.Errorf("job %d should be succeeded: %+v", i, got)
		}
		if !want[i] && (got.Status != job.StatusRunning || got.LockedBy == nil) {
			t.Errorf("rejected job %d must be untouched (still running, still leased): %+v", i, got)
		}
	}
	// Replaying the same batch must complete nothing: every row is already finished.
	ok, err = s.CompleteBatch(ctx, refs)
	if err != nil {
		t.Fatalf("replayed batch: %v", err)
	}
	for i, v := range ok {
		if v {
			t.Errorf("replayed batch: ok[%d] must be false", i)
		}
	}
}

// The zombie scenario through the batch path: a worker whose lease expired and whose job was
// reclaimed by someone else cannot complete it by batching.
func TestCompleteBatchStaleWorkerCannotWriteAfterReclaim(t *testing.T) {
	ctx := context.Background()
	s := newStore(t)
	mustEnqueue(t, s, nj("email"))
	mustEnqueue(t, s, nj("email"))

	c1, _ := s.Claim(ctx, "default", "zombie", 2, 50*time.Millisecond)
	zombie := []job.Ref{c1[0].Ref(), c1[1].Ref()}
	time.Sleep(120 * time.Millisecond)
	if res, err := s.Reap(ctx, 10); err != nil || res.Retried != 2 {
		t.Fatalf("reap: %+v %v", res, err)
	}
	c2, _ := s.Claim(ctx, "default", "healthy", 2, time.Minute)
	if len(c2) != 2 {
		t.Fatalf("reclaim: %+v", c2)
	}

	ok, err := s.CompleteBatch(ctx, zombie)
	if err != nil {
		t.Fatal(err)
	}
	if ok[0] || ok[1] {
		t.Fatalf("zombie batch must complete nothing: %v", ok)
	}
	ok, err = s.CompleteBatch(ctx, []job.Ref{c2[0].Ref(), c2[1].Ref()})
	if err != nil || !ok[0] || !ok[1] {
		t.Fatalf("healthy batch: %v %v", ok, err)
	}
}

func TestCompleteBatchEmpty(t *testing.T) {
	s := newStore(t)
	if ok, err := s.CompleteBatch(context.Background(), nil); err != nil || len(ok) != 0 {
		t.Fatalf("empty batch: %v %v", ok, err)
	}
}

// A zombie worker whose lease expired must not be able to overwrite the new owner's result.
func TestStaleWorkerCannotWriteAfterReclaim(t *testing.T) {
	ctx := context.Background()
	s := newStore(t)
	mustEnqueue(t, s, nj("email"))

	c1, _ := s.Claim(ctx, "default", "zombie", 1, 50*time.Millisecond)
	zombie := c1[0].Ref()
	time.Sleep(120 * time.Millisecond)

	res, err := s.Reap(ctx, 10)
	if err != nil || res.Retried != 1 {
		t.Fatalf("reap: %+v %v", res, err)
	}
	c2, _ := s.Claim(ctx, "default", "healthy", 1, time.Minute)
	if len(c2) != 1 || c2[0].Attempts != 2 {
		t.Fatalf("reclaim: %+v", c2)
	}
	if err := s.Complete(ctx, zombie); !errors.Is(err, queue.ErrLeaseLost) {
		t.Fatalf("zombie complete must fail: %v", err)
	}
	if err := s.Heartbeat(ctx, zombie, time.Minute); !errors.Is(err, queue.ErrLeaseLost) {
		t.Fatalf("zombie heartbeat must fail: %v", err)
	}
	if err := s.Complete(ctx, c2[0].Ref()); err != nil {
		t.Fatalf("healthy complete: %v", err)
	}
}

func TestHeartbeatExtendsLease(t *testing.T) {
	ctx := context.Background()
	s := newStore(t)
	mustEnqueue(t, s, nj("email"))
	c, _ := s.Claim(ctx, "default", "w", 1, 200*time.Millisecond)
	for i := 0; i < 4; i++ {
		time.Sleep(100 * time.Millisecond)
		if err := s.Heartbeat(ctx, c[0].Ref(), 200*time.Millisecond); err != nil {
			t.Fatalf("heartbeat %d: %v", i, err)
		}
	}
	// 400ms elapsed > the original 200ms lease, yet the heartbeats kept it alive.
	if res, _ := s.Reap(ctx, 10); res.Total() != 0 {
		t.Fatalf("heartbeating job must not be reaped: %+v", res)
	}
}

func TestFailTransitions(t *testing.T) {
	tests := []struct {
		name    string
		p       queue.FailParams
		want    job.Status
		wantRun time.Duration // approx run_at offset from now; 0 = not checked
	}{
		{"retrying", queue.FailParams{Status: job.StatusRetrying, Delay: time.Hour, Err: "boom"}, job.StatusRetrying, time.Hour},
		{"dead", queue.FailParams{Status: job.StatusDead, Err: "boom"}, job.StatusDead, 0},
		{"failed", queue.FailParams{Status: job.StatusFailed, Err: "boom"}, job.StatusFailed, 0},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			ctx := context.Background()
			s := newStore(t)
			mustEnqueue(t, s, nj("email"))
			c, _ := s.Claim(ctx, "default", "w", 1, time.Minute)
			if err := s.Fail(ctx, c[0].Ref(), tt.p); err != nil {
				t.Fatalf("fail: %v", err)
			}
			got, _ := s.Get(ctx, "acme", c[0].ID)
			if got.Status != tt.want || got.LastError == nil || *got.LastError != "boom" || got.LockedBy != nil {
				t.Fatalf("got %+v", got)
			}
			if tt.want == job.StatusRetrying {
				if d := time.Until(got.RunAt); d < tt.wantRun-time.Minute || d > tt.wantRun+time.Minute || got.FinishedAt != nil {
					t.Fatalf("retrying: run_at in %v finished=%v", d, got.FinishedAt)
				}
			} else if got.FinishedAt == nil {
				t.Fatalf("terminal state must set finished_at")
			}
		})
	}
	t.Run("invalid status", func(t *testing.T) {
		ctx := context.Background()
		s := newStore(t)
		mustEnqueue(t, s, nj("email"))
		c, _ := s.Claim(ctx, "default", "w", 1, time.Minute)
		if err := s.Fail(ctx, c[0].Ref(), queue.FailParams{Status: job.StatusSucceeded}); err == nil {
			t.Fatal("must reject non-failure target status")
		}
	})
}

func TestRetryingJobIsClaimableAfterBackoff(t *testing.T) {
	ctx := context.Background()
	s := newStore(t)
	mustEnqueue(t, s, nj("email"))
	c, _ := s.Claim(ctx, "default", "w", 1, time.Minute)
	if err := s.Fail(ctx, c[0].Ref(), queue.FailParams{Status: job.StatusRetrying, Delay: 150 * time.Millisecond, Err: "x"}); err != nil {
		t.Fatal(err)
	}
	if got, _ := s.Claim(ctx, "default", "w", 1, time.Minute); len(got) != 0 {
		t.Fatalf("must wait for backoff")
	}
	time.Sleep(250 * time.Millisecond)
	got, _ := s.Claim(ctx, "default", "w", 1, time.Minute)
	if len(got) != 1 || got[0].Attempts != 2 {
		t.Fatalf("after backoff: %+v", got)
	}
}

func TestReapRetriesThenDeadLetters(t *testing.T) {
	ctx := context.Background()
	s := newStore(t)
	n := nj("crashy")
	n.MaxAttempts = 2
	j := mustEnqueue(t, s, n)

	for attempt := 1; attempt <= 2; attempt++ {
		c, _ := s.Claim(ctx, "default", "w", 1, 30*time.Millisecond)
		if len(c) != 1 || c[0].Attempts != attempt {
			t.Fatalf("attempt %d: %+v", attempt, c)
		}
		time.Sleep(80 * time.Millisecond)
		res, err := s.Reap(ctx, 10)
		if err != nil {
			t.Fatal(err)
		}
		if attempt == 1 && res.Retried != 1 {
			t.Fatalf("first reap should retry: %+v", res)
		}
		if attempt == 2 && res.Dead != 1 {
			t.Fatalf("second reap should dead-letter: %+v", res)
		}
	}
	got, _ := s.Get(ctx, "acme", j.ID)
	if got.Status != job.StatusDead || got.FinishedAt == nil || got.LastError == nil {
		t.Fatalf("final: %+v", got)
	}
}

func TestReleaseDoesNotConsumeAttempt(t *testing.T) {
	ctx := context.Background()
	s := newStore(t)
	j := mustEnqueue(t, s, nj("email"))
	c, _ := s.Claim(ctx, "default", "w", 1, time.Minute)
	if err := s.Release(ctx, c[0].Ref()); err != nil {
		t.Fatal(err)
	}
	got, _ := s.Get(ctx, "acme", j.ID)
	if got.Status != job.StatusPending || got.Attempts != 0 || got.LockedBy != nil {
		t.Fatalf("after release: %+v", got)
	}
}

func TestReplay(t *testing.T) {
	ctx := context.Background()
	s := newStore(t)
	j := mustEnqueue(t, s, nj("email"))

	if _, err := s.Replay(ctx, "acme", j.ID); !errors.Is(err, queue.ErrNotReplayable) {
		t.Fatalf("pending job is not replayable: %v", err)
	}
	if _, err := s.Replay(ctx, "evil", j.ID); !errors.Is(err, queue.ErrNotFound) {
		t.Fatalf("other tenant: %v", err)
	}
	if _, err := s.Replay(ctx, "acme", "garbage"); !errors.Is(err, queue.ErrNotFound) {
		t.Fatalf("bad id: %v", err)
	}

	c, _ := s.Claim(ctx, "default", "w", 1, time.Minute)
	if err := s.Fail(ctx, c[0].Ref(), queue.FailParams{Status: job.StatusDead, Err: "x"}); err != nil {
		t.Fatal(err)
	}
	r, err := s.Replay(ctx, "acme", j.ID)
	if err != nil || r.Status != job.StatusPending || r.Attempts != 0 || r.FinishedAt != nil {
		t.Fatalf("replay: %+v %v", r, err)
	}
	if again, _ := s.Claim(ctx, "default", "w", 1, time.Minute); len(again) != 1 {
		t.Fatalf("replayed job must be claimable")
	}
}

func TestDeleteFinished(t *testing.T) {
	ctx := context.Background()
	s := newStore(t)
	done := mustEnqueue(t, s, nj("done"))
	pending := mustEnqueue(t, s, nj("pending"))
	c, _ := s.Claim(ctx, "default", "w", 1, time.Minute)
	for _, x := range c {
		if x.ID == pending.ID { // claim order is by run_at; make sure we finish "done"
			_ = s.Release(ctx, x.Ref())
		}
	}
	// Finish "done" explicitly.
	got, _ := s.Claim(ctx, "default", "w", 10, time.Minute)
	all := append(c, got...)
	for _, x := range all {
		if x.ID == done.ID {
			if err := s.Complete(ctx, x.Ref()); err != nil && !errors.Is(err, queue.ErrLeaseLost) {
				t.Fatal(err)
			}
		}
	}

	if n, err := s.DeleteFinished(ctx, time.Hour, 100); err != nil || n != 0 {
		t.Fatalf("fresh rows must be retained: n=%d err=%v", n, err)
	}
	time.Sleep(50 * time.Millisecond)
	n, err := s.DeleteFinished(ctx, 10*time.Millisecond, 100)
	if err != nil || n != 1 {
		t.Fatalf("want 1 deleted, got %d err=%v", n, err)
	}
	if _, err := s.Get(ctx, "acme", done.ID); !errors.Is(err, queue.ErrNotFound) {
		t.Fatalf("deleted job must be gone: %v", err)
	}
	if _, err := s.Get(ctx, "acme", pending.ID); err != nil {
		t.Fatalf("non-finished job must survive: %v", err)
	}
}

func TestDepthAndOldestReady(t *testing.T) {
	ctx := context.Background()
	s := newStore(t)
	for i := 0; i < 3; i++ {
		mustEnqueue(t, s, nj("a"))
	}
	n := nj("b")
	n.Queue = "slow"
	n.RunAt = time.Now().Add(-10 * time.Second)
	mustEnqueue(t, s, n)

	rows, err := s.Depth(ctx)
	if err != nil {
		t.Fatal(err)
	}
	counts := map[string]int64{}
	for _, r := range rows {
		counts[r.Queue+"/"+string(r.Status)] = r.Count
	}
	if counts["default/pending"] != 3 || counts["slow/pending"] != 1 {
		t.Fatalf("depth=%v", counts)
	}
	age, err := s.OldestReady(ctx)
	if err != nil || age["slow"] < 9*time.Second {
		t.Fatalf("oldest ready: %v %v", age, err)
	}
}

func TestAPIKeys(t *testing.T) {
	ctx := context.Background()
	s := newStore(t)
	key, err := s.CreateAPIKey(ctx, "acme", "ci")
	if err != nil {
		t.Fatal(err)
	}
	tenant, err := s.TenantForKey(ctx, key)
	if err != nil || tenant != "acme" {
		t.Fatalf("lookup: %q %v", tenant, err)
	}
	if _, err := s.TenantForKey(ctx, key+"x"); !errors.Is(err, queue.ErrUnauthorized) {
		t.Fatalf("bad key: %v", err)
	}
	if _, err := s.Pool().Exec(ctx, `UPDATE api_keys SET revoked_at = now()`); err != nil {
		t.Fatal(err)
	}
	if _, err := s.TenantForKey(ctx, key); !errors.Is(err, queue.ErrUnauthorized) {
		t.Fatalf("revoked key: %v", err)
	}
}

func TestLeaseConstraintEnforced(t *testing.T) {
	ctx := context.Background()
	s := newStore(t)
	j := mustEnqueue(t, s, nj("email"))
	// The CHECK constraint must reject a 'running' row without a lease.
	_, err := s.Pool().Exec(ctx, `UPDATE jobs SET status = 'running' WHERE id = $1`, j.ID)
	if err == nil {
		t.Fatal("lease_iff_running constraint should reject running without a lease")
	}
}

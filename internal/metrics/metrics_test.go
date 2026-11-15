package metrics

import (
	"context"
	"errors"
	"io"
	"log/slog"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/prometheus/client_golang/prometheus/testutil"

	"github.com/vasistamagam/distributed-jobscheduler/internal/job"
	"github.com/vasistamagam/distributed-jobscheduler/internal/queue"
)

func quiet() *slog.Logger { return slog.New(slog.NewTextHandler(io.Discard, nil)) }

type fakeSource struct {
	rows  []queue.DepthRow
	ages  map[string]time.Duration
	err   error
	calls atomic.Int32
}

func (f *fakeSource) Depth(context.Context) ([]queue.DepthRow, error) {
	f.calls.Add(1)
	return f.rows, f.err
}
func (f *fakeSource) OldestReady(context.Context) (map[string]time.Duration, error) {
	return f.ages, f.err
}

func TestWorkerObserver(t *testing.T) {
	m := New()
	o := m.WorkerObserver()
	o.Claimed("default", 3, []time.Duration{10 * time.Millisecond, time.Second, 5 * time.Second})
	o.Finished("default", "email", "succeeded", 200*time.Millisecond)
	o.Finished("default", "email", "retry", 300*time.Millisecond)
	o.Finished("default", "email", "retry", 300*time.Millisecond)
	o.Finished("default", "email", "dead", 300*time.Millisecond)
	o.Reaped(2, 1)
	o.Deleted(7)

	if got := testutil.ToFloat64(m.claimed.WithLabelValues("default")); got != 3 {
		t.Errorf("claimed=%v", got)
	}
	if got := testutil.ToFloat64(m.retries.WithLabelValues("default", "email")); got != 2 {
		t.Errorf("retries=%v want 2 (only outcome=retry counts)", got)
	}
	if got := testutil.ToFloat64(m.reaped.WithLabelValues("dead")); got != 1 {
		t.Errorf("reaped dead=%v", got)
	}
	if got := testutil.ToFloat64(m.deleted); got != 7 {
		t.Errorf("deleted=%v", got)
	}
	if n := testutil.CollectAndCount(m.pickup); n != 1 {
		t.Errorf("pickup series=%d", n)
	}
	if n := testutil.CollectAndCount(m.duration); n != 3 {
		t.Errorf("duration series=%d (one per outcome)", n)
	}
}

func TestAPIObserverBoundsCardinality(t *testing.T) {
	m := New()
	o := m.APIObserver()
	for _, code := range []int{200, 201, 204, 400, 404, 409, 500, 503} {
		o.Request("GET /jobs/{id}", "GET", code, time.Millisecond)
	}
	o.JobSubmitted("default", true)
	o.JobSubmitted("default", false)
	// 2xx, 4xx, 5xx only: status codes are bucketed into classes.
	if n := testutil.CollectAndCount(m.httpRequests); n != 3 {
		t.Fatalf("series=%d, want 3 status classes", n)
	}
	if got := testutil.ToFloat64(m.httpRequests.WithLabelValues("GET /jobs/{id}", "GET", "4xx")); got != 3 {
		t.Fatalf("4xx=%v", got)
	}
	if n := testutil.CollectAndCount(m.submitted); n != 2 {
		t.Fatalf("submitted series=%d", n)
	}
	o.JobRejected("default", "backlog_full")
	o.JobRejected("default", "backlog_full")
	if got := testutil.ToFloat64(m.rejected.WithLabelValues("default", "backlog_full")); got != 2 {
		t.Fatalf("rejected=%v want 2", got)
	}
}

func TestSchedulerObserver(t *testing.T) {
	m := New()
	o := m.SchedulerObserver()
	o.Leader(true)
	if testutil.ToFloat64(m.isLeader) != 1 {
		t.Fatal("leader gauge should be 1")
	}
	o.Leader(false)
	if testutil.ToFloat64(m.isLeader) != 0 {
		t.Fatal("leader gauge should be 0")
	}
	o.Fired(5, 2)
	if testutil.ToFloat64(m.schedFires.WithLabelValues("created")) != 5 ||
		testutil.ToFloat64(m.schedFires.WithLabelValues("duplicate_prevented")) != 2 {
		t.Fatal("fire counters wrong")
	}
}

func TestQueueCollectorEmitsZerosAndCaches(t *testing.T) {
	src := &fakeSource{
		rows: []queue.DepthRow{
			{Queue: "default", Status: job.StatusPending, Count: 7},
			{Queue: "default", Status: job.StatusDead, Count: 2},
			{Queue: "emails", Status: job.StatusRunning, Count: 1},
		},
		ages: map[string]time.Duration{"default": 42 * time.Second},
	}
	c := NewQueueCollector(src, quiet(), time.Hour, nil)

	// 2 queues x (4 depth statuses + dlq + oldest) = 12 series; missing statuses are zeros.
	if n := testutil.CollectAndCount(c); n != 12 {
		t.Fatalf("series=%d want 12", n)
	}
	expected := `
# HELP jobscheduler_dlq_size Jobs in the dead-letter state.
# TYPE jobscheduler_dlq_size gauge
jobscheduler_dlq_size{queue="default"} 2
jobscheduler_dlq_size{queue="emails"} 0
`
	if err := testutil.CollectAndCompare(c, strings.NewReader(expected), "jobscheduler_dlq_size"); err != nil {
		t.Fatal(err)
	}
	if got := src.calls.Load(); got != 1 {
		t.Fatalf("DB queried %d times for two scrapes, want 1 (cached)", got)
	}

	// A drained queue keeps reporting zero instead of disappearing.
	src2 := &fakeSource{}
	c2 := NewQueueCollector(src2, quiet(), 0, nil)
	if n := testutil.CollectAndCount(c2); n != 6 {
		t.Fatalf("empty database should still export the default queue: series=%d", n)
	}
}

func TestQueueCollectorDisabledAndErrors(t *testing.T) {
	src := &fakeSource{rows: []queue.DepthRow{{Queue: "default", Status: job.StatusPending, Count: 1}}}
	off := NewQueueCollector(src, quiet(), 0, func() bool { return false })
	if n := testutil.CollectAndCount(off); n != 0 || src.calls.Load() != 0 {
		t.Fatalf("a non-leader must export nothing and not query: series=%d calls=%d", n, src.calls.Load())
	}
	bad := NewQueueCollector(&fakeSource{err: errors.New("db down")}, quiet(), 0, nil)
	if n := testutil.CollectAndCount(bad); n != 0 {
		t.Fatalf("never-successful query must export nothing, got %d series", n)
	}
}

func TestStatusClass(t *testing.T) {
	for code, want := range map[int]string{200: "2xx", 302: "3xx", 404: "4xx", 503: "5xx"} {
		if got := statusClass(code); got != want {
			t.Errorf("statusClass(%d)=%s want %s", code, got, want)
		}
	}
}

// Regression: a counter that only comes into existence at its first increment hides that first event from
// rate() and increase(). The fixed-label counters must exist (at zero) before anything happens; this once let
// the first crash recovery on a worker go unseen by the WorkerCrashesDetected alert.
func TestFixedLabelCountersExistAtZero(t *testing.T) {
	m := New()
	if n := testutil.CollectAndCount(m.reaped); n != 2 {
		t.Fatalf("jobs_reaped_total should expose retried and dead at 0 from startup, got %d series", n)
	}
	if n := testutil.CollectAndCount(m.schedFires); n != 2 {
		t.Fatalf("scheduler_fires_total should expose created and duplicate_prevented at 0, got %d series", n)
	}
	if got := testutil.ToFloat64(m.reaped.WithLabelValues("retried")); got != 0 {
		t.Fatalf("starts at %v, want 0", got)
	}
}

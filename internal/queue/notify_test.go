package queue_test

import (
	"context"
	"io"
	"log/slog"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/vasistamagam/distributed-jobscheduler/internal/queue"
	"github.com/vasistamagam/distributed-jobscheduler/internal/testdb"
)

func quietLog() *slog.Logger { return slog.New(slog.NewTextHandler(io.Discard, nil)) }

// startListener runs a Listener on its own connection for the life of the test.
func startListener(t *testing.T, pool *pgxpool.Pool, queues ...string) *queue.Listener {
	t.Helper()
	ctx, cancel := context.WithCancel(context.Background())
	l := queue.NewListener(pool.Config().ConnString(), queues, quietLog(), nil)
	done := make(chan struct{})
	go func() { l.Run(ctx); close(done) }()
	t.Cleanup(func() { cancel(); <-done })
	waitUntil(t, "listener connected", l.Healthy)
	<-l.Wake() // drain the catch-up signal sent on connect
	return l
}

func waitUntil(t *testing.T, what string, cond func() bool) {
	t.Helper()
	for deadline := time.Now().Add(10 * time.Second); time.Now().Before(deadline); time.Sleep(5 * time.Millisecond) {
		if cond() {
			return
		}
	}
	t.Fatalf("timed out waiting for %s", what)
}

func woke(l *queue.Listener, within time.Duration) bool {
	select {
	case <-l.Wake():
		return true
	case <-time.After(within):
		return false
	}
}

// An enqueue wakes a listener, and a job that is not due yet does not: a notification means "runnable now".
func TestEnqueueNotifiesListenersOnlyForRunnableJobs(t *testing.T) {
	pool := testdb.New(t, 10)
	store := queue.New(pool)
	n := queue.NewNotifier(pool, 0, nil)
	defer n.Close()
	store.SetNotifier(n)
	l := startListener(t, pool, "default")

	mustEnqueue(t, store, nj("t"))
	if !woke(l, 3*time.Second) {
		t.Fatal("enqueue of a runnable job did not wake the listener")
	}

	delayed := nj("t")
	delayed.Delay = time.Hour
	mustEnqueue(t, store, delayed)
	if woke(l, 400*time.Millisecond) {
		t.Fatal("a job delayed an hour must not notify: nothing is runnable yet")
	}
}

// A listener only wakes for the queues it serves.
func TestListenerFiltersByQueue(t *testing.T) {
	pool := testdb.New(t, 10)
	n := queue.NewNotifier(pool, 0, nil)
	defer n.Close()
	l := startListener(t, pool, "emails")

	n.Notify("reports")
	if woke(l, 400*time.Millisecond) {
		t.Fatal("woke for a queue this worker does not serve")
	}
	n.Notify("emails")
	if !woke(l, 3*time.Second) {
		t.Fatal("did not wake for its own queue")
	}
}

// Replaying a dead job makes it runnable now, so it notifies too.
func TestReplayNotifies(t *testing.T) {
	pool := testdb.New(t, 10)
	store := queue.New(pool)
	j := mustEnqueue(t, store, nj("t"))
	if _, err := pool.Exec(context.Background(), `UPDATE jobs SET status='dead', finished_at=now() WHERE id=$1`, j.ID); err != nil {
		t.Fatal(err)
	}
	n := queue.NewNotifier(pool, 0, nil)
	defer n.Close()
	store.SetNotifier(n)
	l := startListener(t, pool, "default")

	if _, err := store.Replay(context.Background(), j.TenantID, j.ID); err != nil {
		t.Fatal(err)
	}
	if !woke(l, 3*time.Second) {
		t.Fatal("replay did not wake the listener")
	}
}

// The listener survives its connection being killed: it reports unhealthy (so workers poll normally), reconnects,
// and signals once on reconnect because notifications sent while it was away are lost.
func TestListenerReconnectsAndSignalsAfterConnectionLoss(t *testing.T) {
	pool := testdb.New(t, 10)
	l := startListener(t, pool, "default")

	// Only THIS test's database. CI runs every package in parallel against one Postgres server, and an unscoped
	// kill terminated the LISTEN sessions of other packages' tests (the integration notify test then saw its jobs
	// wait out the listener's 1 s reconnect backoff and failed).
	if _, err := pool.Exec(context.Background(), `
		SELECT pg_terminate_backend(pid) FROM pg_stat_activity
		WHERE query LIKE 'LISTEN%' AND datname = current_database() AND pid <> pg_backend_pid()`); err != nil {
		t.Fatal(err)
	}
	waitUntil(t, "listener to notice the lost connection", func() bool { return !l.Healthy() })
	waitUntil(t, "listener to reconnect", l.Healthy)
	if !woke(l, 3*time.Second) {
		t.Fatal("no catch-up signal after reconnect: work inserted during the outage would wait for the poll")
	}
	n := queue.NewNotifier(pool, 0, nil)
	defer n.Close()
	n.Notify("default")
	if !woke(l, 3*time.Second) {
		t.Fatal("reconnected listener does not receive notifications")
	}
}

func TestListenerBadDSNDoesNotPanicAndStaysUnhealthy(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	l := queue.NewListener("postgres://nobody@127.0.0.1:1/none?connect_timeout=1", nil, quietLog(), nil)
	done := make(chan struct{})
	go func() { l.Run(ctx); close(done) }()
	time.Sleep(300 * time.Millisecond)
	if l.Healthy() {
		t.Fatal("healthy without a connection")
	}
	cancel()
	select {
	case <-done:
	case <-time.After(3 * time.Second):
		t.Fatal("Run did not stop on context cancel")
	}
}

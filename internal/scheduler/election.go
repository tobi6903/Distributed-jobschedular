package scheduler

import (
	"context"
	"log/slog"
	"time"

	"github.com/jackc/pgx/v5"
)

// Advisory lock key: two int4s ("jobs", 1). The two-key form lets us verify ownership by
// reading pg_locks (classid, objid) without bit-twiddling a bigint.
const (
	lockClass = 0x6a6f6273
	lockObj   = 1
)

// Elector implements leader election with a Postgres session-level advisory lock.
//
// Why a dedicated connection (not the pool): the lock belongs to the *session*. A pooled
// connection could be returned and reused, or recycled, silently releasing or leaking the
// lock. It also means this does not work behind a transaction-mode PgBouncer; run the
// scheduler against Postgres (or a session-mode pooler) directly.
//
// If the process dies, the connection closes and Postgres releases the lock automatically,
// so a standby takes over with no timers or TTLs to tune.
type Elector struct {
	DSN      string
	Retry    time.Duration // how often a standby retries
	Check    time.Duration // how often the leader verifies it still holds the lock
	Log      *slog.Logger
	OnChange func(leader bool) // optional, for metrics
}

// Run blocks until ctx is cancelled. Whenever this replica becomes leader it calls lead with
// a context that is cancelled the moment leadership is lost; lead should return promptly.
func (e *Elector) Run(ctx context.Context, lead func(ctx context.Context)) {
	if e.Retry == 0 {
		e.Retry = 5 * time.Second
	}
	if e.Check == 0 {
		e.Check = time.Second
	}
	for ctx.Err() == nil {
		if e.tryLead(ctx, lead) {
			continue // was leader; retry immediately in case the loss was transient
		}
		select {
		case <-ctx.Done():
		case <-time.After(e.Retry):
		}
	}
}

// tryLead attempts one election; it returns true if it led at least briefly.
func (e *Elector) tryLead(ctx context.Context, lead func(context.Context)) bool {
	cctx, cancel := context.WithTimeout(ctx, 10*time.Second)
	conn, err := pgx.Connect(cctx, e.DSN)
	cancel()
	if err != nil {
		e.Log.Warn("election: cannot connect", "err", err)
		return false
	}
	defer func() {
		// Use a fresh context: ctx may already be cancelled, and Close must still release the session.
		c, cc := context.WithTimeout(context.Background(), 2*time.Second)
		defer cc()
		_ = conn.Close(c)
	}()

	var got bool
	if err := conn.QueryRow(ctx, `SELECT pg_try_advisory_lock($1, $2)`, lockClass, lockObj).Scan(&got); err != nil {
		e.Log.Warn("election: lock attempt failed", "err", err)
		return false
	}
	if !got {
		return false // someone else leads
	}

	e.Log.Info("became scheduler leader")
	if e.OnChange != nil {
		e.OnChange(true)
		defer e.OnChange(false)
	}
	leadCtx, cancelLead := context.WithCancel(ctx)
	done := make(chan struct{})
	go func() { defer close(done); lead(leadCtx) }()

	// Monitor: leadership ends if the session dies or no longer owns the lock.
	t := time.NewTicker(e.Check)
	defer t.Stop()
	for {
		select {
		case <-ctx.Done():
			cancelLead()
			<-done
			return true
		case <-t.C:
			if !e.stillHeld(ctx, conn) {
				e.Log.Warn("lost scheduler leadership")
				cancelLead()
				<-done
				return true
			}
		}
	}
}

// stillHeld asks the server whether this session still owns the lock. A dead connection
// (error or timeout) counts as lost: it is safer to stop firing than to assume.
func (e *Elector) stillHeld(ctx context.Context, conn *pgx.Conn) bool {
	cctx, cancel := context.WithTimeout(ctx, 2*e.Check+time.Second)
	defer cancel()
	var held bool
	err := conn.QueryRow(cctx, `
		SELECT EXISTS (SELECT 1 FROM pg_locks
		               WHERE locktype = 'advisory' AND classid::bigint = $1::bigint AND objid::bigint = $2::bigint
		                 AND objsubid = 2 AND pid = pg_backend_pid() AND granted)`,
		lockClass, lockObj).Scan(&held)
	return err == nil && held
}

package queue

import (
	"context"
	"fmt"
	"log/slog"
	"net"
	"sync"
	"sync/atomic"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

// NotifyChannel is the Postgres LISTEN/NOTIFY channel. The payload is the queue name.
//
// A notification is only a HINT that a job became runnable "now": it wakes an idle worker earlier than its
// next poll. It is never the source of truth. Notifications are lost when nobody is listening, when a
// listener's connection drops, or when the process dies between commit and notify, so the worker's poll
// stays in place as the safety net and every job is still claimed with FOR UPDATE SKIP LOCKED.
const NotifyChannel = "jobs_ready"

// Notifier publishes "queue X has runnable work" hints, coalesced.
//
// Why coalesce: NOTIFY takes a lock at COMMIT that serializes every transaction which sent one, so a
// notification per inserted job would put a global queue in front of the very commits that limit write
// throughput. Instead at most ONE notification per queue per window is sent, from its own tiny statement
// that is not part of the insert's transaction:
//
//   - leading edge: the first job after a quiet period notifies at once (an idle fleet wakes in about a
//     millisecond, not a window later);
//   - trailing edge: jobs that arrive while the window is open are covered by one notification sent when
//     it closes, so a burst of 1,500 inserts per second becomes about 1/window notifications per second.
//
// Sending is best effort and asynchronous: it never delays or fails an enqueue.
type Notifier struct {
	send   func(ctx context.Context, queue string) error
	window time.Duration
	onSend func(ok bool)

	mu     sync.Mutex
	state  map[string]*notifyState
	closed bool
	wg     sync.WaitGroup
}

type notifyState struct {
	last    time.Time // when the last notification for this queue was sent
	pending bool      // a trailing notification is already scheduled
}

// NewNotifier returns a Notifier that sends through pool. window <= 0 disables coalescing (one per call).
// onSend, if set, is told the outcome of every send.
func NewNotifier(pool *pgxpool.Pool, window time.Duration, onSend func(ok bool)) *Notifier {
	return newNotifier(func(ctx context.Context, queue string) error {
		_, err := pool.Exec(ctx, `SELECT pg_notify($1, $2)`, NotifyChannel, queue)
		return err
	}, window, onSend)
}

func newNotifier(send func(context.Context, string) error, window time.Duration, onSend func(bool)) *Notifier {
	if onSend == nil {
		onSend = func(bool) {}
	}
	return &Notifier{send: send, window: window, onSend: onSend, state: map[string]*notifyState{}}
}

// Notify records that queue has a job runnable now. It never blocks on the database.
func (n *Notifier) Notify(queue string) {
	n.mu.Lock()
	defer n.mu.Unlock()
	if n.closed {
		return
	}
	st := n.state[queue]
	if st == nil {
		st = &notifyState{}
		n.state[queue] = st
	}
	if st.pending {
		return // the scheduled trailing notification will cover this job too
	}
	wait := n.window - time.Since(st.last)
	if wait <= 0 {
		st.last = time.Now()
		n.wg.Add(1)
		go func() { defer n.wg.Done(); n.deliver(queue) }()
		return
	}
	st.pending = true
	n.wg.Add(1)
	time.AfterFunc(wait, func() {
		defer n.wg.Done()
		n.mu.Lock()
		st.pending = false
		st.last = time.Now()
		n.mu.Unlock()
		n.deliver(queue)
	})
}

func (n *Notifier) deliver(queue string) {
	// Detached from any request: the job is already committed; this is only a wake-up.
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	n.onSend(n.send(ctx, queue) == nil)
}

// Close stops accepting notifications and waits for the ones already scheduled (at most one window).
func (n *Notifier) Close() {
	n.mu.Lock()
	n.closed = true
	n.mu.Unlock()
	n.wg.Wait()
}

// Listener holds one dedicated connection that LISTENs on NotifyChannel and turns notifications for the
// queues it cares about into a coalesced wake-up signal.
//
// It uses its own connection, not the pool: LISTEN is session state, so the connection must be direct or
// behind a session-mode pooler (PgBouncer in transaction mode silently drops notifications).
//
// Failure model: if the connection drops, the Listener reconnects with backoff, and reports Healthy()==false
// meanwhile so the worker goes back to its normal poll interval. After every (re)connect it signals once,
// because notifications sent while it was away are gone.
type Listener struct {
	dsn    string
	log    *slog.Logger
	queues map[string]bool // empty = every queue
	wake   chan struct{}   // capacity 1: signals coalesce
	up     atomic.Bool
	onUp   func(up bool)
}

// NewListener builds a Listener for the given queues (none = all). onUp, if set, observes connection state.
func NewListener(dsn string, queues []string, log *slog.Logger, onUp func(up bool)) *Listener {
	m := make(map[string]bool, len(queues))
	for _, q := range queues {
		m[q] = true
	}
	if onUp == nil {
		onUp = func(bool) {}
	}
	return &Listener{dsn: dsn, log: log, queues: m, wake: make(chan struct{}, 1), onUp: onUp}
}

// Wake delivers a value when work may be available. Signals coalesce: many notifications, one value.
func (l *Listener) Wake() <-chan struct{} { return l.wake }

// Healthy reports whether the LISTEN connection is currently established.
func (l *Listener) Healthy() bool { return l.up.Load() }

func (l *Listener) signal() {
	select {
	case l.wake <- struct{}{}:
	default: // one is already pending
	}
}

func (l *Listener) setUp(v bool) {
	if l.up.Swap(v) != v {
		l.onUp(v)
	}
}

// Run keeps a LISTEN connection alive until ctx ends.
func (l *Listener) Run(ctx context.Context) {
	backoff := time.Second
	for ctx.Err() == nil {
		start := time.Now()
		err := l.session(ctx)
		l.setUp(false)
		if ctx.Err() != nil {
			return
		}
		l.log.Warn("notify listener disconnected; polling continues", "err", err, "retry_in", backoff.String())
		select {
		case <-ctx.Done():
			return
		case <-time.After(backoff):
		}
		if time.Since(start) > 30*time.Second { // it was healthy for a while: start over
			backoff = time.Second
		} else if backoff < 30*time.Second {
			backoff *= 2
		}
	}
}

func (l *Listener) session(ctx context.Context) error {
	cfg, err := pgx.ParseConfig(l.dsn)
	if err != nil {
		return fmt.Errorf("notify listener: parse dsn: %w", err)
	}
	// A half-open TCP connection (a NAT or load balancer that silently dropped it) would otherwise block
	// WaitForNotification for the OS default of hours. Probe after 10s idle, every 5s, give up after 3.
	d := &net.Dialer{Timeout: 5 * time.Second, KeepAliveConfig: net.KeepAliveConfig{
		Enable: true, Idle: 10 * time.Second, Interval: 5 * time.Second, Count: 3}}
	cfg.DialFunc = d.DialContext
	conn, err := pgx.ConnectConfig(ctx, cfg)
	if err != nil {
		return fmt.Errorf("notify listener: connect: %w", err)
	}
	defer func() {
		cctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
		defer cancel()
		_ = conn.Close(cctx)
	}()
	if _, err := conn.Exec(ctx, "LISTEN "+pgx.Identifier{NotifyChannel}.Sanitize()); err != nil {
		return fmt.Errorf("notify listener: listen: %w", err)
	}
	l.setUp(true)
	l.signal() // anything inserted while we were not listening has no notification: look now
	for {
		n, err := conn.WaitForNotification(ctx)
		if err != nil {
			return fmt.Errorf("notify listener: wait: %w", err)
		}
		if len(l.queues) == 0 || l.queues[n.Payload] {
			l.signal()
		}
	}
}

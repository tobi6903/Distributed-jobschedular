package queue

import (
	"context"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

// recorder is a Notifier sender that counts what would have gone to Postgres.
type recorder struct {
	mu   sync.Mutex
	sent []string
	at   []time.Time
}

func (r *recorder) send(_ context.Context, q string) error {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.sent = append(r.sent, q)
	r.at = append(r.at, time.Now())
	return nil
}

func (r *recorder) count() int {
	r.mu.Lock()
	defer r.mu.Unlock()
	return len(r.sent)
}

func eventually(t *testing.T, what string, cond func() bool) {
	t.Helper()
	for deadline := time.Now().Add(3 * time.Second); time.Now().Before(deadline); time.Sleep(2 * time.Millisecond) {
		if cond() {
			return
		}
	}
	t.Fatalf("timed out waiting for %s", what)
}

// The first job after a quiet period notifies at once, not a window later: an idle fleet must wake immediately.
func TestNotifierLeadingEdgeIsImmediate(t *testing.T) {
	r := &recorder{}
	n := newNotifier(r.send, time.Second, nil) // a long window: any delay would be obvious
	defer n.Close()
	start := time.Now()
	n.Notify("default")
	eventually(t, "the leading notification", func() bool { return r.count() == 1 })
	if d := time.Since(start); d > 500*time.Millisecond {
		t.Fatalf("leading notification took %v, want immediate", d)
	}
}

// 500 jobs inserted inside one window become one leading plus one trailing notification, not 500: this is the
// point of coalescing (NOTIFY serializes commits, so a per-job notification would tax every insert).
func TestNotifierCoalescesABurst(t *testing.T) {
	r := &recorder{}
	n := newNotifier(r.send, 80*time.Millisecond, nil)
	for i := 0; i < 500; i++ {
		n.Notify("default")
	}
	n.Close() // waits for the scheduled trailing notification
	if got := r.count(); got != 2 {
		t.Fatalf("500 notifies in one window sent %d notifications, want 2 (leading + trailing)", got)
	}
}

// The trailing notification covers jobs that arrived while the window was open, and it is sent when the window
// closes, so nothing waits longer than one window.
func TestNotifierTrailingEdgeFiresAtWindowEnd(t *testing.T) {
	r := &recorder{}
	const window = 100 * time.Millisecond
	n := newNotifier(r.send, window, nil)
	defer n.Close()
	n.Notify("q") // leading, immediate
	eventually(t, "leading", func() bool { return r.count() == 1 })
	n.Notify("q") // inside the window: must be covered by a trailing notification
	eventually(t, "trailing", func() bool { return r.count() == 2 })
	if gap := r.at[1].Sub(r.at[0]); gap < window-10*time.Millisecond {
		t.Fatalf("two notifications %v apart, want at least the %v window", gap, window)
	}
}

// After a quiet window a new job gets a fresh leading edge, and queues do not throttle each other.
func TestNotifierQueuesAreIndependentAndRearm(t *testing.T) {
	r := &recorder{}
	n := newNotifier(r.send, 60*time.Millisecond, nil)
	defer n.Close()
	n.Notify("a")
	n.Notify("b")
	eventually(t, "one leading notification per queue", func() bool { return r.count() == 2 })
	time.Sleep(100 * time.Millisecond)
	n.Notify("a")
	eventually(t, "a fresh leading edge after a quiet window", func() bool { return r.count() == 3 })
}

func TestNotifierReportsOutcomeAndIgnoresCallsAfterClose(t *testing.T) {
	var ok, bad atomic.Int32
	failing := func(context.Context, string) error { return context.DeadlineExceeded }
	n := newNotifier(failing, 0, func(success bool) {
		if success {
			ok.Add(1)
		} else {
			bad.Add(1)
		}
	})
	n.Notify("q")
	n.Close()
	n.Notify("q") // after Close: dropped, must not panic or send
	if ok.Load() != 0 || bad.Load() != 1 {
		t.Fatalf("ok=%d error=%d, want 0 and 1", ok.Load(), bad.Load())
	}
}

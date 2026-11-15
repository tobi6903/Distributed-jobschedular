package job

import (
	"testing"
	"time"
)

func TestCanTransition(t *testing.T) {
	all := []Status{StatusPending, StatusRunning, StatusSucceeded, StatusFailed, StatusRetrying, StatusDead}
	legal := map[[2]Status]bool{
		{StatusPending, StatusRunning}:   true,
		{StatusRetrying, StatusRunning}:  true,
		{StatusRunning, StatusSucceeded}: true,
		{StatusRunning, StatusRetrying}:  true,
		{StatusRunning, StatusDead}:      true,
		{StatusRunning, StatusFailed}:    true,
		{StatusRunning, StatusPending}:   true,
		{StatusFailed, StatusPending}:    true,
		{StatusDead, StatusPending}:      true,
	}
	for _, from := range all {
		for _, to := range all {
			want := legal[[2]Status{from, to}]
			if got := CanTransition(from, to); got != want {
				t.Errorf("CanTransition(%s,%s)=%v want %v", from, to, got, want)
			}
		}
	}
}

func TestTerminalAndValid(t *testing.T) {
	tests := []struct {
		s        Status
		terminal bool
		valid    bool
	}{
		{StatusPending, false, true},
		{StatusRunning, false, true},
		{StatusRetrying, false, true},
		{StatusSucceeded, true, true},
		{StatusFailed, true, true},
		{StatusDead, true, true},
		{Status("bogus"), false, false},
	}
	for _, tt := range tests {
		if tt.s.Terminal() != tt.terminal || tt.s.Valid() != tt.valid {
			t.Errorf("%s: terminal=%v valid=%v", tt.s, tt.s.Terminal(), tt.s.Valid())
		}
	}
}

func TestBackoffDelay(t *testing.T) {
	const base, cap = 10 * time.Second, 100 * time.Second
	tests := []struct {
		name    string
		attempt int
		rnd     float64
		want    time.Duration
	}{
		{"first attempt, min jitter", 1, 0, 5 * time.Second},
		{"first attempt, max jitter", 1, 0.999999999, 10*time.Second - 1},
		{"second doubles", 2, 0, 10 * time.Second},
		{"third doubles again", 3, 0, 20 * time.Second},
		{"capped", 10, 0, 50 * time.Second},
		{"capped max jitter", 10, 0.999999999, 100*time.Second - 1},
		{"attempt below 1 treated as 1", 0, 0, 5 * time.Second},
		{"huge attempt does not overflow", 500, 0, 50 * time.Second},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			b := Backoff{Base: base, Cap: cap, Rand: func() float64 { return tt.rnd }}
			got := b.Delay(tt.attempt)
			// float64 rounding of the jitter product can be off by a few ns.
			diff := got - tt.want
			if diff < -time.Microsecond || diff > time.Microsecond {
				t.Errorf("Delay(%d)=%v want %v", tt.attempt, got, tt.want)
			}
		})
	}
}

func TestBackoffDefaultRandBounds(t *testing.T) {
	b := DefaultBackoff()
	for i := 0; i < 1000; i++ {
		d := b.Delay(3) // nominal 20s -> [10s, 20s)
		if d < 10*time.Second || d >= 20*time.Second {
			t.Fatalf("delay %v out of [10s,20s)", d)
		}
	}
}

package job

import (
	"math/rand/v2"
	"time"
)

// Backoff computes retry delays: exponential growth, capped, with "equal jitter".
//
//	delay  = min(Cap, Base * 2^(attempt-1))
//	result = delay/2 + rand[0, delay/2]
//
// Jitter spreads retries so a mass failure (e.g. a downstream outage) does not produce a
// synchronized retry storm. Equal jitter (rather than full jitter) keeps a floor of
// delay/2 so a retry can never fire almost immediately.
type Backoff struct {
	Base time.Duration
	Cap  time.Duration
	// Rand returns a float64 in [0,1). Injected so tests are deterministic.
	Rand func() float64
}

// DefaultBackoff: 5s base, 1h cap.
func DefaultBackoff() Backoff {
	return Backoff{Base: 5 * time.Second, Cap: time.Hour, Rand: rand.Float64}
}

// Delay returns the wait before the retry that follows the given (1-based) failed attempt.
func (b Backoff) Delay(attempt int) time.Duration {
	if attempt < 1 {
		attempt = 1
	}
	d := b.Base
	for i := 1; i < attempt; i++ {
		d *= 2
		// d <= 0 catches overflow of the int64 duration.
		if d >= b.Cap || d <= 0 {
			d = b.Cap
			break
		}
	}
	if d > b.Cap {
		d = b.Cap
	}
	r := b.Rand
	if r == nil {
		r = rand.Float64
	}
	half := d / 2
	return half + time.Duration(r()*float64(half))
}

// Package scheduler turns cron schedules into jobs. It runs as 2+ replicas; one is elected
// leader via a Postgres advisory lock, and an idempotent per-fire job key guarantees no
// double-fire even if two replicas briefly both believe they lead.
package scheduler

import (
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/robfig/cron/v3"

	_ "time/tzdata" // embed the IANA database: distroless images have no /usr/share/zoneinfo
)

// MisfirePolicy says what to do when the scheduler was down (or slow) and one or more
// scheduled slots are in the past.
type MisfirePolicy string

const (
	// FireOnce runs the job once for the most recent missed slot, then resumes the normal
	// cadence. The default: you get "catch-up" without a thundering backlog.
	FireOnce MisfirePolicy = "fire_once"
	// Skip ignores missed slots older than the grace window and waits for the next one.
	Skip MisfirePolicy = "skip"
	// FireAll runs the job once per missed slot (bounded per tick), for jobs where every
	// slot matters (e.g. hourly billing aggregation).
	FireAll MisfirePolicy = "fire_all"
)

func (p MisfirePolicy) Valid() bool { return p == FireOnce || p == Skip || p == FireAll }

// ErrInvalid wraps validation failures (bad cron expression, unknown timezone, ...).
var ErrInvalid = errors.New("invalid schedule")

// parser accepts standard 5-field cron plus descriptors (@hourly, @daily, @every 5m).
var parser = cron.NewParser(cron.Minute | cron.Hour | cron.Dom | cron.Month | cron.Dow | cron.Descriptor)

// Spec is a parsed cron expression bound to a timezone.
type Spec struct {
	sched cron.Schedule
	loc   *time.Location
}

// ParseSpec validates expr and tz. The timezone is passed separately (not via the
// "CRON_TZ=" prefix) so it is a first-class, validated, queryable column.
func ParseSpec(expr, tz string) (Spec, error) {
	if tz == "" {
		tz = "UTC"
	}
	loc, err := time.LoadLocation(tz)
	if err != nil {
		return Spec{}, fmt.Errorf("%w: unknown timezone %q", ErrInvalid, tz)
	}
	if strings.HasPrefix(expr, "CRON_TZ=") || strings.HasPrefix(expr, "TZ=") {
		return Spec{}, fmt.Errorf("%w: set the timezone with the timezone field, not a TZ= prefix", ErrInvalid)
	}
	s, err := parser.Parse(expr)
	if err != nil {
		return Spec{}, fmt.Errorf("%w: cron expression %q: %v", ErrInvalid, expr, err)
	}
	return Spec{sched: s, loc: loc}, nil
}

// Next returns the first fire time strictly after t, evaluated on the wall clock of the
// schedule's timezone (so "0 9 * * *" is 09:00 local on both sides of a DST change) and
// returned in UTC.
//
// DST behaviour is whatever robfig/cron implements: a wall-clock time that does not exist
// (spring forward) is skipped to the next valid slot; one that occurs twice (fall back)
// fires once. Documented in docs/DESIGN.md.
func (s Spec) Next(t time.Time) time.Time {
	return s.sched.Next(t.In(s.loc)).UTC()
}

// Plan decides what to do for a schedule whose pending slot `slot` is due (slot <= now).
// It is pure (no clock, no I/O), so it is exhaustively table-tested.
//
// It returns the slot times to fire and the schedule's new next_fire_at. maxFires bounds
// the work done in one call for FireAll; if the backlog is larger, next is the first
// unprocessed slot and later ticks continue draining it.
func Plan(s Spec, policy MisfirePolicy, slot, now time.Time, grace time.Duration, maxFires int) (fires []time.Time, next time.Time) {
	const maxScan = 100_000 // bounds CPU if a minutely schedule was down for years

	// Walk the slots that are due: slot, Next(slot), ... while <= now.
	var due []time.Time
	cur := slot
	for scanned := 0; !cur.After(now); scanned++ {
		if policy == FireAll && len(due) >= maxFires {
			// Backlog bigger than one tick may process: stop here; cur is the first
			// unprocessed slot and becomes next_fire_at.
			return due, cur
		}
		if scanned >= maxScan {
			// Pathological gap: jump to the next future slot. The latest slot we know of
			// is still in due, so FireOnce/Skip remain correct to within the gap.
			return pick(policy, due, now, grace), s.Next(now)
		}
		due = append(due, cur)
		cur = s.Next(cur)
	}
	return pick(policy, due, now, grace), cur
}

// pick applies the misfire policy to the list of due slots (ascending, non-empty).
func pick(policy MisfirePolicy, due []time.Time, now time.Time, grace time.Duration) []time.Time {
	if len(due) == 0 {
		return nil
	}
	latest := due[len(due)-1]
	switch policy {
	case FireAll:
		return due
	case Skip:
		// Fire only if the most recent slot is still fresh; everything older is dropped.
		if now.Sub(latest) <= grace {
			return []time.Time{latest}
		}
		return nil
	default: // FireOnce
		return []time.Time{latest}
	}
}

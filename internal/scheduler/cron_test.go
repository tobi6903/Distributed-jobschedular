package scheduler

import (
	"errors"
	"strings"
	"testing"
	"time"
)

func mustSpec(t *testing.T, expr, tz string) Spec {
	t.Helper()
	s, err := ParseSpec(expr, tz)
	if err != nil {
		t.Fatalf("ParseSpec(%q,%q): %v", expr, tz, err)
	}
	return s
}

func at(s string) time.Time {
	t, err := time.Parse(time.RFC3339, s)
	if err != nil {
		panic(err)
	}
	return t
}

func TestParseSpecErrors(t *testing.T) {
	tests := []struct {
		name, expr, tz, want string
	}{
		{"garbage", "not cron", "UTC", "cron expression"},
		{"too few fields", "* * *", "UTC", "cron expression"},
		{"bad timezone", "* * * * *", "Mars/Olympus", "unknown timezone"},
		{"TZ prefix rejected", "CRON_TZ=UTC * * * * *", "UTC", "timezone field"},
		{"out of range", "61 * * * *", "UTC", "cron expression"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			_, err := ParseSpec(tt.expr, tt.tz)
			if !errors.Is(err, ErrInvalid) || !strings.Contains(err.Error(), tt.want) {
				t.Fatalf("err=%v want ErrInvalid containing %q", err, tt.want)
			}
		})
	}
	if _, err := ParseSpec("@hourly", ""); err != nil {
		t.Fatalf("descriptor + empty tz (=UTC) should parse: %v", err)
	}
}

func TestNextIsTimezoneAware(t *testing.T) {
	ny := mustSpec(t, "0 9 * * *", "America/New_York")
	tests := []struct {
		name  string
		after string
		want  string
	}{
		// EST (UTC-5) before DST starts on 2026-03-08: 09:00 local = 14:00 UTC.
		{"winter", "2026-03-06T20:00:00Z", "2026-03-07T14:00:00Z"},
		// 09:00 on the DST-change day is already EDT (UTC-4) = 13:00 UTC.
		{"day of spring forward", "2026-03-07T14:00:00Z", "2026-03-08T13:00:00Z"},
		// Fall back on 2026-11-01: 09:00 local is EST again = 14:00 UTC.
		{"after fall back", "2026-10-31T13:00:00Z", "2026-11-01T14:00:00Z"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := ny.Next(at(tt.after)); !got.Equal(at(tt.want)) {
				t.Fatalf("Next(%s)=%s want %s", tt.after, got.Format(time.RFC3339), tt.want)
			}
		})
	}
	// Same expression in UTC is unaffected by DST.
	utc := mustSpec(t, "0 9 * * *", "UTC")
	if got := utc.Next(at("2026-03-07T10:00:00Z")); !got.Equal(at("2026-03-08T09:00:00Z")) {
		t.Fatalf("UTC next = %s", got)
	}
	// A non-existent local time (02:30 on spring-forward day) must not fire at a bogus instant.
	gap := mustSpec(t, "30 2 * * *", "America/New_York")
	got := gap.Next(at("2026-03-08T00:00:00Z"))
	if got.Before(at("2026-03-08T00:00:00Z")) || got.After(at("2026-03-09T12:00:00Z")) {
		t.Fatalf("gap next = %s", got)
	}
}

func TestPlanPolicies(t *testing.T) {
	hourly := mustSpec(t, "0 * * * *", "UTC")
	grace := 5 * time.Minute
	tests := []struct {
		name      string
		policy    MisfirePolicy
		slot, now string
		maxFires  int
		wantFires []string
		wantNext  string
	}{
		{
			name: "on time, any policy fires the slot", policy: FireOnce,
			slot: "2026-05-01T10:00:00Z", now: "2026-05-01T10:00:02Z", maxFires: 100,
			wantFires: []string{"2026-05-01T10:00:00Z"}, wantNext: "2026-05-01T11:00:00Z",
		},
		{
			name: "fire_once collapses a backlog into the latest slot", policy: FireOnce,
			slot: "2026-05-01T06:00:00Z", now: "2026-05-01T10:30:00Z", maxFires: 100,
			wantFires: []string{"2026-05-01T10:00:00Z"}, wantNext: "2026-05-01T11:00:00Z",
		},
		{
			name: "fire_all runs every missed slot", policy: FireAll,
			slot: "2026-05-01T08:00:00Z", now: "2026-05-01T10:30:00Z", maxFires: 100,
			wantFires: []string{"2026-05-01T08:00:00Z", "2026-05-01T09:00:00Z", "2026-05-01T10:00:00Z"},
			wantNext:  "2026-05-01T11:00:00Z",
		},
		{
			name: "fire_all is bounded per call and resumes at the first unprocessed slot", policy: FireAll,
			slot: "2026-05-01T00:00:00Z", now: "2026-05-01T10:30:00Z", maxFires: 3,
			wantFires: []string{"2026-05-01T00:00:00Z", "2026-05-01T01:00:00Z", "2026-05-01T02:00:00Z"},
			wantNext:  "2026-05-01T03:00:00Z",
		},
		{
			name: "skip drops stale slots", policy: Skip,
			slot: "2026-05-01T06:00:00Z", now: "2026-05-01T10:30:00Z", maxFires: 100,
			wantFires: nil, wantNext: "2026-05-01T11:00:00Z",
		},
		{
			name: "skip still fires a slot inside the grace window", policy: Skip,
			slot: "2026-05-01T10:00:00Z", now: "2026-05-01T10:04:00Z", maxFires: 100,
			wantFires: []string{"2026-05-01T10:00:00Z"}, wantNext: "2026-05-01T11:00:00Z",
		},
		{
			name: "skip drops a slot just outside the grace window", policy: Skip,
			slot: "2026-05-01T10:00:00Z", now: "2026-05-01T10:06:00Z", maxFires: 100,
			wantFires: nil, wantNext: "2026-05-01T11:00:00Z",
		},
		{
			name: "slot exactly at now is due", policy: FireOnce,
			slot: "2026-05-01T10:00:00Z", now: "2026-05-01T10:00:00Z", maxFires: 100,
			wantFires: []string{"2026-05-01T10:00:00Z"}, wantNext: "2026-05-01T11:00:00Z",
		},
		{
			name: "not due: nothing fires and next is unchanged", policy: FireOnce,
			slot: "2026-05-01T11:00:00Z", now: "2026-05-01T10:30:00Z", maxFires: 100,
			wantFires: nil, wantNext: "2026-05-01T11:00:00Z",
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			fires, next := Plan(hourly, tt.policy, at(tt.slot), at(tt.now), grace, tt.maxFires)
			if len(fires) != len(tt.wantFires) {
				t.Fatalf("fires=%v want %v", fires, tt.wantFires)
			}
			for i, w := range tt.wantFires {
				if !fires[i].Equal(at(w)) {
					t.Fatalf("fire[%d]=%s want %s", i, fires[i], w)
				}
			}
			if !next.Equal(at(tt.wantNext)) {
				t.Fatalf("next=%s want %s", next, tt.wantNext)
			}
		})
	}
}

// Whatever the policy, next must end up strictly in the future (or the first unprocessed
// slot for a bounded fire_all), otherwise the schedule would re-fire forever.
func TestPlanAlwaysAdvances(t *testing.T) {
	s := mustSpec(t, "*/7 * * * *", "Europe/London")
	now := at("2026-06-15T12:34:56Z")
	for _, p := range []MisfirePolicy{FireOnce, Skip, FireAll} {
		fires, next := Plan(s, p, now.Add(-90*time.Minute), now, time.Minute, 1000)
		if !next.After(now) {
			t.Errorf("%s: next %s not after now", p, next)
		}
		for _, f := range fires {
			if f.After(now) {
				t.Errorf("%s: fired a future slot %s", p, f)
			}
		}
	}
}

func TestPlanPathologicalGapIsBounded(t *testing.T) {
	s := mustSpec(t, "* * * * *", "UTC")
	now := at("2026-06-15T12:00:00Z")
	start := time.Now()
	fires, next := Plan(s, FireOnce, now.Add(-3*365*24*time.Hour), now, time.Minute, 1000)
	if time.Since(start) > 5*time.Second {
		t.Fatalf("Plan took %v; the scan cap is not working", time.Since(start))
	}
	if len(fires) != 1 || !next.After(now) {
		t.Fatalf("fires=%d next=%s", len(fires), next)
	}
}

func TestFireKeyIsDeterministic(t *testing.T) {
	a := FireKey("abc", at("2026-05-01T10:00:00Z"))
	b := FireKey("abc", at("2026-05-01T12:00:00+02:00")) // same instant, different zone
	if a != b || a != "cron:abc:2026-05-01T10:00:00Z" {
		t.Fatalf("keys: %q %q", a, b)
	}
	if a == FireKey("abc", at("2026-05-01T11:00:00Z")) || a == FireKey("def", at("2026-05-01T10:00:00Z")) {
		t.Fatal("keys must differ across slots and schedules")
	}
}

// Package config loads settings from environment variables (12-factor). Loading takes a
// getenv function instead of calling os.Getenv so tests need no global state.
package config

import (
	"errors"
	"fmt"
	"log/slog"
	"strconv"
	"strings"
	"time"
)

// Common settings shared by every binary.
type Common struct {
	DatabaseURL string
	DBMaxConns  int32
	LogLevel    slog.Level
	MetricsAddr string // serves /metrics (and /healthz for non-HTTP binaries)
}

type API struct {
	Common
	Addr            string
	ShutdownDelay   time.Duration // time to stay unready before closing listeners (LB drain)
	ShutdownTimeout time.Duration
}

type Worker struct {
	Common
	Queues            []string
	Concurrency       int
	BatchSize         int
	PollInterval      time.Duration
	Lease             time.Duration
	HeartbeatInterval time.Duration
	DefaultTimeout    time.Duration
	ShutdownGrace     time.Duration
	ReapInterval      time.Duration
	Retention         time.Duration
	BackoffBase       time.Duration
	BackoffCap        time.Duration
}

type Scheduler struct {
	Common
	Tick         time.Duration // how often the leader looks for due schedules
	RetryElect   time.Duration // how often a standby retries leader election
	DisableLock  bool          // test-only: skip leader election to prove the idempotent-key backstop
	DefaultGrace time.Duration
}

// loader accumulates parse errors so the caller sees every problem at once.
type loader struct {
	get  func(string) string
	errs []error
}

func (l *loader) str(name, def string) string {
	if v := l.get(name); v != "" {
		return v
	}
	return def
}

func (l *loader) require(name string) string {
	v := l.get(name)
	if v == "" {
		l.errs = append(l.errs, fmt.Errorf("%s is required", name))
	}
	return v
}

func (l *loader) dur(name string, def time.Duration) time.Duration {
	v := l.get(name)
	if v == "" {
		return def
	}
	d, err := time.ParseDuration(v)
	if err != nil || d < 0 {
		l.errs = append(l.errs, fmt.Errorf("%s: invalid duration %q", name, v))
		return def
	}
	return d
}

func (l *loader) integer(name string, def int) int {
	v := l.get(name)
	if v == "" {
		return def
	}
	n, err := strconv.Atoi(v)
	if err != nil || n < 0 {
		l.errs = append(l.errs, fmt.Errorf("%s: invalid non-negative integer %q", name, v))
		return def
	}
	return n
}

func (l *loader) csv(name string, def []string) []string {
	v := l.get(name)
	if v == "" {
		return def
	}
	var out []string
	for _, p := range strings.Split(v, ",") {
		if p = strings.TrimSpace(p); p != "" {
			out = append(out, p)
		}
	}
	return out
}

func (l *loader) common(defMetricsAddr string) Common {
	c := Common{
		DatabaseURL: l.require("DATABASE_URL"),
		DBMaxConns:  int32(l.integer("DB_MAX_CONNS", 10)),
		MetricsAddr: l.str("METRICS_ADDR", defMetricsAddr),
	}
	switch strings.ToLower(l.str("LOG_LEVEL", "info")) {
	case "debug":
		c.LogLevel = slog.LevelDebug
	case "info":
		c.LogLevel = slog.LevelInfo
	case "warn":
		c.LogLevel = slog.LevelWarn
	case "error":
		c.LogLevel = slog.LevelError
	default:
		l.errs = append(l.errs, fmt.Errorf("LOG_LEVEL: unknown level %q", l.get("LOG_LEVEL")))
	}
	return c
}

func (l *loader) err() error { return errors.Join(l.errs...) }

// LoadAPI reads API settings.
func LoadAPI(getenv func(string) string) (API, error) {
	l := &loader{get: getenv}
	c := API{
		Common:          l.common(""),
		Addr:            l.str("HTTP_ADDR", ":8080"),
		ShutdownDelay:   l.dur("SHUTDOWN_DELAY", 5*time.Second),
		ShutdownTimeout: l.dur("SHUTDOWN_TIMEOUT", 20*time.Second),
	}
	return c, l.err()
}

// LoadWorker reads worker settings and validates cross-field constraints.
func LoadWorker(getenv func(string) string) (Worker, error) {
	l := &loader{get: getenv}
	c := Worker{
		Common:            l.common(":9100"),
		Queues:            l.csv("WORKER_QUEUES", []string{"default"}),
		Concurrency:       l.integer("WORKER_CONCURRENCY", 10),
		BatchSize:         l.integer("WORKER_BATCH_SIZE", 0),
		PollInterval:      l.dur("WORKER_POLL_INTERVAL", time.Second),
		Lease:             l.dur("WORKER_LEASE", 30*time.Second),
		HeartbeatInterval: l.dur("WORKER_HEARTBEAT_INTERVAL", 0),
		DefaultTimeout:    l.dur("WORKER_DEFAULT_TIMEOUT", 5*time.Minute),
		ShutdownGrace:     l.dur("WORKER_SHUTDOWN_GRACE", 30*time.Second),
		ReapInterval:      l.dur("WORKER_REAP_INTERVAL", 10*time.Second),
		Retention:         l.dur("RETENTION", 7*24*time.Hour),
		BackoffBase:       l.dur("BACKOFF_BASE", 5*time.Second),
		BackoffCap:        l.dur("BACKOFF_CAP", time.Hour),
	}
	if c.Concurrency < 1 {
		l.errs = append(l.errs, errors.New("WORKER_CONCURRENCY must be >= 1"))
	}
	if c.Lease <= 0 {
		l.errs = append(l.errs, errors.New("WORKER_LEASE must be > 0"))
	}
	hb := c.HeartbeatInterval
	if hb == 0 {
		hb = c.Lease / 3
	}
	// A heartbeat interval at or beyond the lease would let the lease expire between beats.
	if hb >= c.Lease {
		l.errs = append(l.errs, errors.New("WORKER_HEARTBEAT_INTERVAL must be less than WORKER_LEASE"))
	}
	return c, l.err()
}

// LoadScheduler reads scheduler settings.
func LoadScheduler(getenv func(string) string) (Scheduler, error) {
	l := &loader{get: getenv}
	c := Scheduler{
		Common:       l.common(":9101"),
		Tick:         l.dur("SCHEDULER_TICK", time.Second),
		RetryElect:   l.dur("SCHEDULER_ELECTION_RETRY", 5*time.Second),
		DefaultGrace: l.dur("SCHEDULER_MISFIRE_GRACE", time.Minute),
	}
	return c, l.err()
}

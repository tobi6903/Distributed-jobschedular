package scheduler

import (
	"context"
	"log/slog"
	"sync/atomic"
	"time"

	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/trace"
)

// Observer receives scheduler metric events.
type Observer interface {
	Leader(isLeader bool)
	Fired(fired, skipped int)
}

type nopObserver struct{}

func (nopObserver) Leader(bool)    {}
func (nopObserver) Fired(int, int) {}

// Config tunes the Scheduler.
type Config struct {
	DSN         string
	Tick        time.Duration // how often the leader looks for due schedules
	RetryElect  time.Duration // standby retry interval
	Grace       time.Duration // Skip-policy freshness window
	DisableLock bool          // TEST ONLY: skip leader election
}

// Scheduler fires due cron schedules. Run it in 2+ replicas for high availability.
type Scheduler struct {
	store  *Store
	cfg    Config
	log    *slog.Logger
	obs    Observer
	tracer trace.Tracer
	leader atomic.Bool
}

type Option func(*Scheduler)

func WithObserver(o Observer) Option   { return func(s *Scheduler) { s.obs = o } }
func WithTracer(t trace.Tracer) Option { return func(s *Scheduler) { s.tracer = t } }

func New(store *Store, cfg Config, log *slog.Logger, opts ...Option) *Scheduler {
	if cfg.Tick == 0 {
		cfg.Tick = time.Second
	}
	s := &Scheduler{store: store, cfg: cfg, log: log, obs: nopObserver{}, tracer: otel.Tracer("jobscheduler/scheduler")}
	for _, o := range opts {
		o(s)
	}
	return s
}

// IsLeader reports whether this replica currently holds leadership.
func (s *Scheduler) IsLeader() bool { return s.leader.Load() }

// Run blocks until ctx is cancelled.
func (s *Scheduler) Run(ctx context.Context) {
	if s.cfg.DisableLock {
		s.log.Warn("leader election disabled (test mode)")
		s.setLeader(true)
		s.fireLoop(ctx)
		return
	}
	e := &Elector{DSN: s.cfg.DSN, Retry: s.cfg.RetryElect, Log: s.log, OnChange: s.setLeader}
	e.Run(ctx, s.fireLoop)
}

func (s *Scheduler) setLeader(v bool) {
	s.leader.Store(v)
	s.obs.Leader(v)
}

// fireLoop runs while this replica is leader.
func (s *Scheduler) fireLoop(ctx context.Context) {
	t := time.NewTicker(s.cfg.Tick)
	defer t.Stop()
	for {
		s.drain(ctx)
		select {
		case <-ctx.Done():
			return
		case <-t.C:
		}
	}
}

// drain fires until nothing is due (a full batch means there may be more).
func (s *Scheduler) drain(ctx context.Context) {
	fc := FireConfig{Grace: s.cfg.Grace, Tracer: s.tracer, Log: s.log}
	for ctx.Err() == nil {
		res, err := s.store.FireDue(ctx, fc)
		if err != nil {
			if ctx.Err() == nil {
				s.log.Error("fire due schedules failed", "err", err)
			}
			return
		}
		if res.Fired+res.Skipped > 0 {
			s.log.Info("fired schedules", "schedules", res.Schedules, "jobs_created", res.Fired, "duplicates_prevented", res.Skipped)
			s.obs.Fired(res.Fired, res.Skipped)
		}
		if res.Schedules < 100 {
			return
		}
	}
}

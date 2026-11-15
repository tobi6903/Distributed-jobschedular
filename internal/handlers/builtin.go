// Package handlers contains the demo job handlers registered by cmd/worker. They exist so
// the whole system can be exercised end to end (load tests, chaos tests, the README demo)
// without writing application code. Real applications register their own handlers.
package handlers

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"time"

	"github.com/vasistamagam/distributed-jobscheduler/internal/job"
	"github.com/vasistamagam/distributed-jobscheduler/internal/worker"
)

// Register adds the built-in handlers to reg.
func Register(reg *worker.Registry, log *slog.Logger) error {
	for name, h := range map[string]worker.Handler{
		"noop":  Noop,
		"sleep": Sleep,
		"fail":  Fail,
		"flaky": Flaky,
		"log":   Log(log),
	} {
		if err := reg.Register(name, h); err != nil {
			return err
		}
	}
	return nil
}

func decode(j job.Job, v any) error {
	if len(j.Payload) == 0 {
		return nil
	}
	if err := json.Unmarshal(j.Payload, v); err != nil {
		// A malformed payload will never become valid, so retrying is pointless.
		return worker.Permanent(fmt.Errorf("invalid payload: %w", err))
	}
	return nil
}

// Noop succeeds immediately. Used for throughput benchmarks.
func Noop(context.Context, job.Job) error { return nil }

// Sleep waits payload.seconds (default 1), honouring cancellation.
func Sleep(ctx context.Context, j job.Job) error {
	var p struct {
		Seconds float64 `json:"seconds"`
	}
	p.Seconds = 1
	if err := decode(j, &p); err != nil {
		return err
	}
	t := time.NewTimer(time.Duration(p.Seconds * float64(time.Second)))
	defer t.Stop()
	select {
	case <-t.C:
		return nil
	case <-ctx.Done():
		return context.Cause(ctx)
	}
}

// Fail always fails. payload.permanent=true marks the error non-retryable.
func Fail(_ context.Context, j job.Job) error {
	var p struct {
		Message   string `json:"message"`
		Permanent bool   `json:"permanent"`
	}
	p.Message = "intentional failure"
	if err := decode(j, &p); err != nil {
		return err
	}
	err := errors.New(p.Message)
	if p.Permanent {
		return worker.Permanent(err)
	}
	return err
}

// Flaky fails until attempt payload.succeed_on_attempt (default 3), then succeeds. It
// demonstrates retries with backoff.
func Flaky(_ context.Context, j job.Job) error {
	var p struct {
		SucceedOnAttempt int `json:"succeed_on_attempt"`
	}
	p.SucceedOnAttempt = 3
	if err := decode(j, &p); err != nil {
		return err
	}
	if j.Attempts < p.SucceedOnAttempt {
		return fmt.Errorf("flaky failure on attempt %d (succeeds on %d)", j.Attempts, p.SucceedOnAttempt)
	}
	return nil
}

// Log writes the payload to the log.
func Log(log *slog.Logger) worker.Handler {
	return func(_ context.Context, j job.Job) error {
		log.Info("log job", "job_id", j.ID, "payload", string(j.Payload))
		return nil
	}
}

package worker

import (
	"context"
	"errors"
	"fmt"
	"sync"

	"github.com/vasistamagam/distributed-jobscheduler/internal/job"
)

// Handler executes one job. It must be idempotent: delivery is at-least-once, so a job can
// run again after a crash or a lost lease. It must honour ctx: ctx is cancelled on timeout,
// lease loss, or forced shutdown.
type Handler func(ctx context.Context, j job.Job) error

// ErrUnknownType is returned for jobs whose type has no registered handler.
var ErrUnknownType = errors.New("no handler registered for job type")

// permanentError marks an error that must not be retried.
type permanentError struct{ err error }

func (e permanentError) Error() string { return e.err.Error() }
func (e permanentError) Unwrap() error { return e.err }

// Permanent wraps err so the worker sends the job straight to `failed` (no retries).
// Use it for errors retrying cannot fix, such as an invalid payload.
func Permanent(err error) error {
	if err == nil {
		return nil
	}
	return permanentError{err: err}
}

// IsPermanent reports whether err (or anything it wraps) was marked Permanent.
func IsPermanent(err error) bool {
	var p permanentError
	return errors.As(err, &p)
}

// Registry maps job types to handlers. It is populated at startup and then read-only, but is
// still mutex-guarded so a late Register cannot race with the poll loop.
type Registry struct {
	mu       sync.RWMutex
	handlers map[string]Handler
}

func NewRegistry() *Registry { return &Registry{handlers: map[string]Handler{}} }

// Register binds a handler to a job type. Registering a type twice is a programming error.
func (r *Registry) Register(jobType string, h Handler) error {
	if jobType == "" || h == nil {
		return errors.New("register: type and handler are required")
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	if _, dup := r.handlers[jobType]; dup {
		return fmt.Errorf("register: handler for %q already registered", jobType)
	}
	r.handlers[jobType] = h
	return nil
}

// Lookup returns the handler for a job type.
func (r *Registry) Lookup(jobType string) (Handler, bool) {
	r.mu.RLock()
	defer r.mu.RUnlock()
	h, ok := r.handlers[jobType]
	return h, ok
}

// Types lists registered job types (for startup logging).
func (r *Registry) Types() []string {
	r.mu.RLock()
	defer r.mu.RUnlock()
	out := make([]string, 0, len(r.handlers))
	for t := range r.handlers {
		out = append(out, t)
	}
	return out
}

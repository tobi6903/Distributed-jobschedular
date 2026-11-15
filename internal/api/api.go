// Package api is the HTTP front door: submit jobs, read their status, replay dead ones.
package api

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"regexp"
	"strings"
	"sync/atomic"
	"time"

	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/codes"
	"go.opentelemetry.io/otel/propagation"
	"go.opentelemetry.io/otel/trace"

	"github.com/vasistamagam/distributed-jobscheduler/internal/job"
	"github.com/vasistamagam/distributed-jobscheduler/internal/queue"
)

// JobStore is the persistence the API needs (consumer-defined, so tests can fake it).
type JobStore interface {
	Enqueue(ctx context.Context, n job.NewJob) (job.Job, bool, error)
	Get(ctx context.Context, tenantID, id string) (job.Job, error)
	Replay(ctx context.Context, tenantID, id string) (job.Job, error)
	// Admit applies backpressure: it returns queue.ErrBacklogFull when the tenant's queue is full.
	Admit(ctx context.Context, tenantID, queue string) error
	FindByIdempotencyKey(ctx context.Context, tenantID, key string) (job.Job, error)
}

// Authenticator resolves an API key to a tenant id.
type Authenticator interface {
	TenantForKey(ctx context.Context, key string) (string, error)
}

// Pinger checks dependencies for the readiness probe.
type Pinger interface {
	Ping(ctx context.Context) error
}

// Observer receives request metrics. Optional.
type Observer interface {
	Request(route, method string, status int, d time.Duration)
	JobSubmitted(queue string, created bool)
	// JobRejected counts submissions refused by backpressure (reason is "backlog_full").
	JobRejected(queue, reason string)
}

type nopObserver struct{}

func (nopObserver) Request(string, string, int, time.Duration) {}
func (nopObserver) JobSubmitted(string, bool)                  {}
func (nopObserver) JobRejected(string, string)                 {}

// Deps are the Server's collaborators.
type Deps struct {
	Jobs      JobStore
	Auth      Authenticator
	DB        Pinger
	Schedules ScheduleStore // optional; schedule routes are registered only when set
	Logger    *slog.Logger
	Observer  Observer
	Tracer    trace.Tracer
}

// Server holds the routes.
type Server struct {
	d        Deps
	mux      *http.ServeMux
	draining atomic.Bool
}

const maxBodyBytes = 1 << 20 // 1 MiB: bounds memory per request and the payload column

// retryAfterSeconds is the Retry-After hint sent with 429. Backlog counts refresh every second,
// so a few seconds gives workers a chance to drain before the client tries again.
const retryAfterSeconds = "5"

// New builds the Server and registers its routes.
func New(d Deps) *Server {
	if d.Logger == nil {
		d.Logger = slog.Default()
	}
	if d.Observer == nil {
		d.Observer = nopObserver{}
	}
	if d.Tracer == nil {
		d.Tracer = otel.Tracer("jobscheduler/api")
	}
	s := &Server{d: d, mux: http.NewServeMux()}

	// Probes are unauthenticated and uninstrumented-by-tracing (they are noise in traces).
	s.mux.HandleFunc("GET /healthz", s.healthz)
	s.mux.HandleFunc("GET /readyz", s.readyz)

	s.route("POST /jobs", s.authed(s.submit))
	s.route("GET /jobs/{id}", s.authed(s.getJob))
	s.route("POST /jobs/{id}/replay", s.authed(s.replay))
	if d.Schedules != nil {
		s.registerScheduleRoutes()
	}
	return s
}

// Handler returns the root handler.
func (s *Server) Handler() http.Handler { return s.mux }

// Mount attaches an extra handler (e.g. /metrics) outside auth and instrumentation.
func (s *Server) Mount(pattern string, h http.Handler) { s.mux.Handle(pattern, h) }

// SetDraining makes /readyz fail so the load balancer stops sending traffic before the
// listener closes. Part of graceful shutdown.
func (s *Server) SetDraining() { s.draining.Store(true) }

func (s *Server) healthz(w http.ResponseWriter, _ *http.Request) {
	w.WriteHeader(http.StatusOK)
	_, _ = io.WriteString(w, "ok")
}

func (s *Server) readyz(w http.ResponseWriter, r *http.Request) {
	if s.draining.Load() {
		http.Error(w, "draining", http.StatusServiceUnavailable)
		return
	}
	ctx, cancel := context.WithTimeout(r.Context(), 2*time.Second)
	defer cancel()
	if err := s.d.DB.Ping(ctx); err != nil {
		http.Error(w, "database unavailable", http.StatusServiceUnavailable)
		return
	}
	_, _ = io.WriteString(w, "ready")
}

// ---- middleware ----

type ctxKey int

const tenantKey ctxKey = 0

func tenantFrom(ctx context.Context) string {
	t, _ := ctx.Value(tenantKey).(string)
	return t
}

type statusRecorder struct {
	http.ResponseWriter
	status int
}

func (r *statusRecorder) WriteHeader(code int) {
	r.status = code
	r.ResponseWriter.WriteHeader(code)
}

// route registers h with tracing, metrics and request logging. The route pattern (not the
// raw URL) labels metrics and spans, which keeps cardinality bounded.
func (s *Server) route(pattern string, h http.HandlerFunc) {
	s.mux.HandleFunc(pattern, func(w http.ResponseWriter, r *http.Request) {
		start := time.Now()
		// Continue the caller's trace if it sent a traceparent header.
		ctx := propagation.TraceContext{}.Extract(r.Context(), propagation.HeaderCarrier(r.Header))
		ctx, span := s.d.Tracer.Start(ctx, pattern, trace.WithSpanKind(trace.SpanKindServer),
			trace.WithAttributes(attribute.String("http.route", pattern)))
		defer span.End()

		rec := &statusRecorder{ResponseWriter: w, status: http.StatusOK}
		h(rec, r.WithContext(ctx))

		d := time.Since(start)
		span.SetAttributes(attribute.Int("http.status_code", rec.status))
		if rec.status >= 500 {
			span.SetStatus(codes.Error, http.StatusText(rec.status))
		}
		method, _, _ := strings.Cut(pattern, " ")
		s.d.Observer.Request(pattern, method, rec.status, d)

		attrs := []any{"method", r.Method, "route", pattern, "status", rec.status, "duration_ms", float64(d.Microseconds()) / 1000}
		if t := tenantFrom(ctx); t != "" {
			attrs = append(attrs, "tenant_id", t)
		}
		if sc := span.SpanContext(); sc.IsValid() {
			attrs = append(attrs, "trace_id", sc.TraceID().String())
		}
		s.d.Logger.Info("http request", attrs...)
	})
}

// authed requires "Authorization: Bearer <key>" and puts the tenant (derived from the key,
// never from the request body) in the context.
func (s *Server) authed(next http.HandlerFunc) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		key, ok := strings.CutPrefix(r.Header.Get("Authorization"), "Bearer ")
		if !ok || key == "" {
			w.Header().Set("WWW-Authenticate", `Bearer realm="jobscheduler"`)
			writeError(w, http.StatusUnauthorized, "missing bearer token")
			return
		}
		tenant, err := s.d.Auth.TenantForKey(r.Context(), key)
		switch {
		case errors.Is(err, queue.ErrUnauthorized):
			writeError(w, http.StatusUnauthorized, "invalid api key")
			return
		case err != nil:
			s.d.Logger.Error("auth lookup failed", "err", err)
			writeError(w, http.StatusServiceUnavailable, "authentication unavailable")
			return
		}
		next(w, r.WithContext(context.WithValue(r.Context(), tenantKey, tenant)))
	}
}

// ---- helpers ----

func writeJSON(w http.ResponseWriter, status int, v any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(v)
}

func writeError(w http.ResponseWriter, status int, msg string) {
	writeJSON(w, status, map[string]string{"error": msg})
}

// decode parses a size-limited JSON body, rejecting unknown fields (typos should fail loudly).
func decode(w http.ResponseWriter, r *http.Request, dst any) error {
	r.Body = http.MaxBytesReader(w, r.Body, maxBodyBytes)
	dec := json.NewDecoder(r.Body)
	dec.DisallowUnknownFields()
	if err := dec.Decode(dst); err != nil {
		var mbe *http.MaxBytesError
		if errors.As(err, &mbe) {
			return fmt.Errorf("body exceeds %d bytes", maxBodyBytes)
		}
		return fmt.Errorf("invalid JSON: %w", err)
	}
	if dec.More() {
		return errors.New("invalid JSON: trailing data")
	}
	return nil
}

var nameRE = regexp.MustCompile(`^[A-Za-z0-9][A-Za-z0-9_.:-]{0,127}$`)

// ---- handlers ----

// submitRequest is the POST /jobs body.
type submitRequest struct {
	Type           string          `json:"type"`
	Queue          string          `json:"queue"`
	Payload        json.RawMessage `json:"payload"`
	Priority       int             `json:"priority"`
	RunAt          *time.Time      `json:"run_at"`
	DelaySeconds   float64         `json:"delay_seconds"`
	MaxAttempts    int             `json:"max_attempts"`
	TimeoutSeconds *int            `json:"timeout_seconds"`
	IdempotencyKey *string         `json:"idempotency_key"`
}

func (q submitRequest) validate() error {
	switch {
	case !nameRE.MatchString(q.Type):
		return errors.New("type is required (1-128 chars: letters, digits, _ . : -)")
	case q.Queue != "" && !nameRE.MatchString(q.Queue):
		return errors.New("queue must be 1-128 chars: letters, digits, _ . : -")
	case q.Priority < -100 || q.Priority > 100:
		return errors.New("priority must be between -100 and 100")
	case q.MaxAttempts < 0 || q.MaxAttempts > 100:
		return errors.New("max_attempts must be between 1 and 100")
	case q.TimeoutSeconds != nil && (*q.TimeoutSeconds < 1 || *q.TimeoutSeconds > 86400):
		return errors.New("timeout_seconds must be between 1 and 86400")
	case q.DelaySeconds < 0 || q.DelaySeconds > 365*86400:
		return errors.New("delay_seconds must be between 0 and 31536000")
	case q.IdempotencyKey != nil && (len(*q.IdempotencyKey) == 0 || len(*q.IdempotencyKey) > 255):
		return errors.New("idempotency_key must be 1-255 chars")
	}
	return nil
}

func (s *Server) submit(w http.ResponseWriter, r *http.Request) {
	var req submitRequest
	if err := decode(w, r, &req); err != nil {
		writeError(w, http.StatusBadRequest, err.Error())
		return
	}
	if err := req.validate(); err != nil {
		writeError(w, http.StatusBadRequest, err.Error())
		return
	}
	payload := req.Payload
	if len(payload) == 0 || string(payload) == "null" {
		payload = json.RawMessage(`{}`)
	}

	// Backpressure: if the tenant's queue is full, tell the producer to slow down instead of
	// letting the backlog grow without bound.
	queueName := req.Queue
	if queueName == "" {
		queueName = job.DefaultQueue
	}
	if err := s.d.Jobs.Admit(r.Context(), tenantFrom(r.Context()), queueName); err != nil {
		if errors.Is(err, queue.ErrBacklogFull) {
			// A retry of a submit we already accepted must still succeed (that is what the
			// idempotency key is for), even while the queue is full.
			if req.IdempotencyKey != nil {
				if existing, ferr := s.d.Jobs.FindByIdempotencyKey(r.Context(), tenantFrom(r.Context()), *req.IdempotencyKey); ferr == nil {
					w.Header().Set("Location", "/jobs/"+existing.ID)
					writeJSON(w, http.StatusOK, existing)
					return
				}
			}
			s.d.Observer.JobRejected(queueName, "backlog_full")
			w.Header().Set("Retry-After", retryAfterSeconds)
			writeError(w, http.StatusTooManyRequests, "queue backlog limit reached; retry later")
			return
		}
		s.d.Logger.Error("admission check failed", "err", err)
		writeError(w, http.StatusServiceUnavailable, "could not enqueue job")
		return
	}

	ctx, span := s.d.Tracer.Start(r.Context(), "queue.enqueue")
	defer span.End()
	// Persist the trace context in the row so the worker's spans join this trace.
	carrier := propagation.MapCarrier{}
	propagation.TraceContext{}.Inject(ctx, carrier)

	n := job.NewJob{
		TenantID:       tenantFrom(ctx),
		Queue:          req.Queue,
		Type:           req.Type,
		Payload:        payload,
		Priority:       int16(req.Priority),
		Delay:          time.Duration(req.DelaySeconds * float64(time.Second)),
		MaxAttempts:    req.MaxAttempts,
		TimeoutSeconds: req.TimeoutSeconds,
		IdempotencyKey: req.IdempotencyKey,
		Traceparent:    carrier.Get("traceparent"),
	}
	if req.RunAt != nil {
		n.RunAt = *req.RunAt
	}
	j, created, err := s.d.Jobs.Enqueue(ctx, n)
	if err != nil {
		span.RecordError(err)
		span.SetStatus(codes.Error, "enqueue failed")
		s.d.Logger.Error("enqueue failed", "err", err)
		writeError(w, http.StatusServiceUnavailable, "could not enqueue job")
		return
	}
	span.SetAttributes(attribute.String("job.id", j.ID), attribute.Bool("job.created", created))
	s.d.Observer.JobSubmitted(j.Queue, created)

	// 201 for a new job, 200 when the idempotency key matched an existing one.
	status := http.StatusCreated
	if !created {
		status = http.StatusOK
	}
	w.Header().Set("Location", "/jobs/"+j.ID)
	writeJSON(w, status, j)
}

func (s *Server) getJob(w http.ResponseWriter, r *http.Request) {
	j, err := s.d.Jobs.Get(r.Context(), tenantFrom(r.Context()), r.PathValue("id"))
	s.respondJob(w, j, err, http.StatusOK)
}

func (s *Server) replay(w http.ResponseWriter, r *http.Request) {
	j, err := s.d.Jobs.Replay(r.Context(), tenantFrom(r.Context()), r.PathValue("id"))
	s.respondJob(w, j, err, http.StatusOK)
}

func (s *Server) respondJob(w http.ResponseWriter, j job.Job, err error, okStatus int) {
	switch {
	case errors.Is(err, queue.ErrNotFound):
		// 404 (not 403) for other tenants' jobs so existence does not leak.
		writeError(w, http.StatusNotFound, "job not found")
	case errors.Is(err, queue.ErrNotReplayable):
		writeError(w, http.StatusConflict, "only failed or dead jobs can be replayed")
	case err != nil:
		s.d.Logger.Error("job request failed", "err", err)
		writeError(w, http.StatusServiceUnavailable, "internal error")
	default:
		writeJSON(w, okStatus, j)
	}
}

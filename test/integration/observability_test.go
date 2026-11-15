package integration

import (
	"bytes"
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"time"

	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	"go.opentelemetry.io/otel/sdk/trace/tracetest"

	"github.com/vasistamagam/distributed-jobscheduler/internal/api"
	"github.com/vasistamagam/distributed-jobscheduler/internal/job"
	"github.com/vasistamagam/distributed-jobscheduler/internal/worker"
)

// syncBuffer lets the worker's goroutines write logs while the test reads them.
type syncBuffer struct {
	mu  sync.Mutex
	buf bytes.Buffer
}

func (b *syncBuffer) Write(p []byte) (int, error) {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.buf.Write(p)
}

func (b *syncBuffer) String() string {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.buf.String()
}

// Phase 5: a single trace follows a job from the HTTP request, through the enqueue, the
// claim, the execution and the completion write, even though the API and the worker never
// share memory (the trace context travels in the job row).
func TestTraceSpansSubmitToComplete(t *testing.T) {
	e := newEnv(t, 10)
	key, _ := e.store.CreateAPIKey(context.Background(), "acme", "it")

	rec := tracetest.NewSpanRecorder()
	tp := sdktrace.NewTracerProvider(sdktrace.WithSpanProcessor(rec), sdktrace.WithSampler(sdktrace.AlwaysSample()))
	defer func() { _ = tp.Shutdown(context.Background()) }()

	srv := httptest.NewServer(api.New(api.Deps{
		Jobs: e.store, Auth: e.store, DB: e.pool, Logger: quietLogger(), Tracer: tp.Tracer("api"),
	}).Handler())
	defer srv.Close()

	logs := &syncBuffer{}
	jsonLog := newJSONLogger(logs)
	w := worker.New(e.store, builtinRegistry(t), fastCfg("w1"), worker.WithTracer(tp.Tracer("worker")), worker.WithLogger(jsonLog))
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- w.Run(ctx) }()
	defer func() {
		cancel()
		if err := <-done; err != nil {
			t.Errorf("worker Run: %v", err)
		}
	}()

	req, _ := http.NewRequest("POST", srv.URL+"/jobs", strings.NewReader(`{"type":"noop"}`))
	req.Header.Set("Authorization", "Bearer "+key)
	resp, err := http.DefaultClient.Do(req)
	if err != nil || resp.StatusCode != 201 {
		t.Fatalf("submit: %v %v", resp, err)
	}
	var j job.Job
	_ = json.NewDecoder(resp.Body).Decode(&j)
	resp.Body.Close()

	eventually(t, 5*time.Second, "job to succeed and spans to end", func() bool {
		if e.status(t, j.ID).Status != job.StatusSucceeded {
			return false
		}
		names := map[string]bool{}
		for _, s := range rec.Ended() {
			names[s.Name()] = true
		}
		return names["job.complete"] && names["job.execute"]
	})

	byName := map[string]sdktrace.ReadOnlySpan{}
	for _, s := range rec.Ended() {
		byName[s.Name()] = s
	}
	for _, name := range []string{"POST /jobs", "queue.enqueue", "job.claim", "job.execute", "job.complete"} {
		if byName[name] == nil {
			t.Fatalf("missing span %q; have %v", name, spanNames(rec))
		}
	}
	traceID := byName["POST /jobs"].SpanContext().TraceID()
	for name, s := range byName {
		if s.SpanContext().TraceID() != traceID {
			t.Errorf("span %q is in trace %s, want %s", name, s.SpanContext().TraceID(), traceID)
		}
	}
	// Parent chain: request -> enqueue -> {claim, execute}; execute -> complete.
	parent := func(child, want string) {
		t.Helper()
		if got := byName[child].Parent().SpanID(); got != byName[want].SpanContext().SpanID() {
			t.Errorf("parent of %q is not %q", child, want)
		}
	}
	parent("queue.enqueue", "POST /jobs")
	parent("job.claim", "queue.enqueue")
	parent("job.execute", "queue.enqueue")
	parent("job.complete", "job.execute")

	// Structured logs carry job_id, tenant_id and the same trace_id.
	var found bool
	for _, line := range strings.Split(strings.TrimSpace(logs.String()), "\n") {
		var rec map[string]any
		if json.Unmarshal([]byte(line), &rec) != nil || rec["msg"] != "job finished" {
			continue
		}
		found = true
		if rec["job_id"] != j.ID || rec["tenant_id"] != "acme" || rec["trace_id"] != traceID.String() ||
			rec["outcome"] != "succeeded" || rec["worker_id"] != "w1" {
			t.Errorf("log record missing/incorrect fields: %v", rec)
		}
	}
	if !found {
		t.Fatalf("no 'job finished' log line in:\n%s", logs.String())
	}
}

func spanNames(rec *tracetest.SpanRecorder) []string {
	var out []string
	for _, s := range rec.Ended() {
		out = append(out, s.Name())
	}
	return out
}

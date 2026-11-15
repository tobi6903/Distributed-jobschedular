package api_test

import (
	"bytes"
	"context"
	"encoding/json"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/vasistamagam/distributed-jobscheduler/internal/api"
	"github.com/vasistamagam/distributed-jobscheduler/internal/job"
	"github.com/vasistamagam/distributed-jobscheduler/internal/queue"
	"github.com/vasistamagam/distributed-jobscheduler/internal/scheduler"
	"github.com/vasistamagam/distributed-jobscheduler/internal/testdb"
)

type fixture struct {
	srv   *httptest.Server
	api   *api.Server
	store *queue.Store
	keyA  string // tenant acme
	keyB  string // tenant globex
}

func setup(t *testing.T) *fixture {
	t.Helper()
	pool := testdb.New(t, 10)
	store := queue.New(pool)
	ctx := context.Background()
	ka, err := store.CreateAPIKey(ctx, "acme", "test")
	if err != nil {
		t.Fatal(err)
	}
	kb, err := store.CreateAPIKey(ctx, "globex", "test")
	if err != nil {
		t.Fatal(err)
	}
	a := api.New(api.Deps{
		Jobs: store, Auth: store, DB: pool, Schedules: scheduler.NewStore(pool),
		Logger: slog.New(slog.NewTextHandler(io.Discard, nil)),
	})
	srv := httptest.NewServer(a.Handler())
	t.Cleanup(srv.Close)
	return &fixture{srv: srv, api: a, store: store, keyA: ka, keyB: kb}
}

func (f *fixture) do(t *testing.T, method, path, key, body string) (int, map[string]any) {
	t.Helper()
	req, err := http.NewRequest(method, f.srv.URL+path, strings.NewReader(body))
	if err != nil {
		t.Fatal(err)
	}
	if key != "" {
		req.Header.Set("Authorization", "Bearer "+key)
	}
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	var out map[string]any
	raw, _ := io.ReadAll(resp.Body)
	_ = json.Unmarshal(raw, &out)
	return resp.StatusCode, out
}

func TestHealthEndpoints(t *testing.T) {
	f := setup(t)
	for _, p := range []string{"/healthz", "/readyz"} {
		resp, err := http.Get(f.srv.URL + p)
		if err != nil || resp.StatusCode != 200 {
			t.Fatalf("%s: %v %v", p, resp, err)
		}
		resp.Body.Close()
	}
	f.api.SetDraining()
	resp, _ := http.Get(f.srv.URL + "/readyz")
	if resp.StatusCode != http.StatusServiceUnavailable {
		t.Fatalf("draining readyz = %d", resp.StatusCode)
	}
	resp.Body.Close()
	resp, _ = http.Get(f.srv.URL + "/healthz")
	if resp.StatusCode != 200 {
		t.Fatalf("liveness must stay up while draining, got %d", resp.StatusCode)
	}
	resp.Body.Close()
}

func TestAuth(t *testing.T) {
	f := setup(t)
	tests := []struct{ name, key string }{
		{"no header", ""},
		{"bad key", "js_nope"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			st, _ := f.do(t, "POST", "/jobs", tt.key, `{"type":"x"}`)
			if st != http.StatusUnauthorized {
				t.Fatalf("status %d", st)
			}
		})
	}
	// Non-bearer scheme.
	req, _ := http.NewRequest("GET", f.srv.URL+"/jobs/abc", nil)
	req.Header.Set("Authorization", "Basic abc")
	resp, _ := http.DefaultClient.Do(req)
	if resp.StatusCode != 401 || resp.Header.Get("WWW-Authenticate") == "" {
		t.Fatalf("basic auth: %d %v", resp.StatusCode, resp.Header)
	}
	resp.Body.Close()
}

func TestSubmitAndGet(t *testing.T) {
	f := setup(t)
	st, body := f.do(t, "POST", "/jobs", f.keyA,
		`{"type":"email.send","payload":{"to":"a@b.c"},"priority":5,"max_attempts":3,"timeout_seconds":30}`)
	if st != http.StatusCreated {
		t.Fatalf("submit status %d body %v", st, body)
	}
	id, _ := body["id"].(string)
	if id == "" || body["status"] != "pending" || body["tenant_id"] != "acme" ||
		body["queue"] != "default" || body["max_attempts"] != float64(3) || body["timeout_seconds"] != float64(30) {
		t.Fatalf("body %v", body)
	}
	st, got := f.do(t, "GET", "/jobs/"+id, f.keyA, "")
	if st != 200 || got["id"] != id {
		t.Fatalf("get: %d %v", st, got)
	}
	payload, _ := got["payload"].(map[string]any)
	if payload["to"] != "a@b.c" {
		t.Fatalf("payload %v", got["payload"])
	}
}

func TestTenantComesFromKeyNotBody(t *testing.T) {
	f := setup(t)
	// A tenant_id in the body is an unknown field and must be rejected, not honoured.
	st, _ := f.do(t, "POST", "/jobs", f.keyA, `{"type":"x","tenant_id":"globex"}`)
	if st != http.StatusBadRequest {
		t.Fatalf("status %d", st)
	}
}

func TestTenantIsolation(t *testing.T) {
	f := setup(t)
	_, body := f.do(t, "POST", "/jobs", f.keyA, `{"type":"x"}`)
	id := body["id"].(string)
	if st, _ := f.do(t, "GET", "/jobs/"+id, f.keyB, ""); st != http.StatusNotFound {
		t.Fatalf("other tenant GET status %d, want 404", st)
	}
	if st, _ := f.do(t, "POST", "/jobs/"+id+"/replay", f.keyB, ""); st != http.StatusNotFound {
		t.Fatalf("other tenant replay status %d, want 404", st)
	}
}

func TestIdempotentSubmit(t *testing.T) {
	f := setup(t)
	b := `{"type":"x","idempotency_key":"order-1"}`
	st1, a := f.do(t, "POST", "/jobs", f.keyA, b)
	st2, c := f.do(t, "POST", "/jobs", f.keyA, b)
	if st1 != 201 || st2 != 200 || a["id"] != c["id"] {
		t.Fatalf("statuses %d/%d ids %v/%v", st1, st2, a["id"], c["id"])
	}
}

func TestSubmitDelay(t *testing.T) {
	f := setup(t)
	_, body := f.do(t, "POST", "/jobs", f.keyA, `{"type":"x","delay_seconds":3600}`)
	runAt, err := time.Parse(time.RFC3339Nano, body["run_at"].(string))
	if err != nil {
		t.Fatal(err)
	}
	if d := time.Until(runAt); d < 59*time.Minute || d > 61*time.Minute {
		t.Fatalf("run_at in %v", d)
	}
}

func TestSubmitValidation(t *testing.T) {
	f := setup(t)
	big := `{"type":"x","payload":"` + strings.Repeat("a", 1<<20) + `"}`
	tests := []struct {
		name, body string
	}{
		{"empty body", ``},
		{"not json", `{`},
		{"missing type", `{}`},
		{"bad type chars", `{"type":"a b"}`},
		{"bad queue", `{"type":"x","queue":"bad queue!"}`},
		{"priority too high", `{"type":"x","priority":101}`},
		{"negative max attempts", `{"type":"x","max_attempts":-1}`},
		{"max attempts too high", `{"type":"x","max_attempts":101}`},
		{"zero timeout", `{"type":"x","timeout_seconds":0}`},
		{"negative delay", `{"type":"x","delay_seconds":-1}`},
		{"empty idempotency key", `{"type":"x","idempotency_key":""}`},
		{"unknown field", `{"type":"x","nope":1}`},
		{"trailing data", `{"type":"x"}{"type":"y"}`},
		{"oversized body", big},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			st, body := f.do(t, "POST", "/jobs", f.keyA, tt.body)
			if st != http.StatusBadRequest || body["error"] == nil {
				t.Fatalf("status %d body %v", st, body)
			}
		})
	}
}

func TestGetNotFound(t *testing.T) {
	f := setup(t)
	for _, id := range []string{"00000000-0000-0000-0000-000000000000", "garbage"} {
		if st, _ := f.do(t, "GET", "/jobs/"+id, f.keyA, ""); st != http.StatusNotFound {
			t.Fatalf("id %q: status %d", id, st)
		}
	}
}

func TestReplayFlow(t *testing.T) {
	f := setup(t)
	ctx := context.Background()
	_, body := f.do(t, "POST", "/jobs", f.keyA, `{"type":"x","max_attempts":1}`)
	id := body["id"].(string)

	if st, _ := f.do(t, "POST", "/jobs/"+id+"/replay", f.keyA, ""); st != http.StatusConflict {
		t.Fatalf("replaying a pending job must be 409, got %d", st)
	}

	c, err := f.store.Claim(ctx, "default", "w", 1, time.Minute)
	if err != nil || len(c) != 1 {
		t.Fatal(err)
	}
	if err := f.store.Fail(ctx, c[0].Ref(), queue.FailParams{Status: job.StatusDead, Err: "boom"}); err != nil {
		t.Fatal(err)
	}
	st, got := f.do(t, "POST", "/jobs/"+id+"/replay", f.keyA, "")
	if st != 200 || got["status"] != "pending" || got["attempts"] != float64(0) {
		t.Fatalf("replay: %d %v", st, got)
	}
}

func TestTraceparentIsPersisted(t *testing.T) {
	f := setup(t)
	req, _ := http.NewRequest("POST", f.srv.URL+"/jobs", bytes.NewBufferString(`{"type":"x"}`))
	req.Header.Set("Authorization", "Bearer "+f.keyA)
	// With the default no-op tracer provider the propagated span context is the incoming one.
	req.Header.Set("traceparent", "00-0af7651916cd43dd8448eb211c80319c-b7ad6b7169203331-01")
	resp, err := http.DefaultClient.Do(req)
	if err != nil || resp.StatusCode != 201 {
		t.Fatalf("%v %v", resp, err)
	}
	var j job.Job
	_ = json.NewDecoder(resp.Body).Decode(&j)
	resp.Body.Close()
	var tp string
	err = f.store.Pool().QueryRow(context.Background(), `SELECT traceparent FROM jobs WHERE id=$1`, j.ID).Scan(&tp)
	if err != nil || !strings.Contains(tp, "0af7651916cd43dd8448eb211c80319c") {
		t.Fatalf("traceparent %q err %v", tp, err)
	}
}

func TestBackpressureReturns429WithRetryAfter(t *testing.T) {
	f := setup(t)
	ctx := context.Background()
	f.store.SetLimitCacheTTL(0, 0) // exact counts for the test
	three := 3
	if err := f.store.SetLimit(ctx, queue.Limit{Queue: "default", TenantID: "acme", MaxBacklog: &three}); err != nil {
		t.Fatal(err)
	}
	for i := 0; i < 3; i++ {
		if st, body := f.do(t, "POST", "/jobs", f.keyA, `{"type":"x"}`); st != http.StatusCreated {
			t.Fatalf("submit %d: %d %v", i, st, body)
		}
	}

	req, _ := http.NewRequest("POST", f.srv.URL+"/jobs", strings.NewReader(`{"type":"x"}`))
	req.Header.Set("Authorization", "Bearer "+f.keyA)
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	resp.Body.Close()
	if resp.StatusCode != http.StatusTooManyRequests || resp.Header.Get("Retry-After") != "5" {
		t.Fatalf("full queue: status=%d Retry-After=%q, want 429 and 5", resp.StatusCode, resp.Header.Get("Retry-After"))
	}

	// Other tenants are unaffected, and so are other queues.
	if st, _ := f.do(t, "POST", "/jobs", f.keyB, `{"type":"x"}`); st != http.StatusCreated {
		t.Fatalf("other tenant must not be throttled: %d", st)
	}
	if st, _ := f.do(t, "POST", "/jobs", f.keyA, `{"type":"x","queue":"reports"}`); st != http.StatusCreated {
		t.Fatalf("the limit is per queue: %d", st)
	}

	// Draining makes room again.
	c, err := f.store.Claim(ctx, "default", "w", 1, time.Minute)
	if err != nil || len(c) != 1 {
		t.Fatal(err)
	}
	if st, _ := f.do(t, "POST", "/jobs", f.keyA, `{"type":"x"}`); st != http.StatusCreated {
		t.Fatalf("after a job was claimed there is room: %d", st)
	}
}

// A client retry of a submit that was already accepted must succeed even while the queue is
// full: otherwise a retry after a timeout would look like a failure for a job that exists.
func TestIdempotentRetrySucceedsWhileQueueIsFull(t *testing.T) {
	f := setup(t)
	f.store.SetLimitCacheTTL(0, 0)
	one := 1
	if err := f.store.SetLimit(context.Background(), queue.Limit{Queue: "default", TenantID: "acme", MaxBacklog: &one}); err != nil {
		t.Fatal(err)
	}
	body := `{"type":"x","idempotency_key":"order-9"}`
	st1, first := f.do(t, "POST", "/jobs", f.keyA, body)
	if st1 != http.StatusCreated {
		t.Fatalf("first submit: %d", st1)
	}
	// The queue is now full (limit 1). The retry returns the existing job, not 429.
	st2, again := f.do(t, "POST", "/jobs", f.keyA, body)
	if st2 != http.StatusOK || again["id"] != first["id"] {
		t.Fatalf("retry: %d %v, want 200 with the original job", st2, again)
	}
	// A different new job is rejected.
	if st, _ := f.do(t, "POST", "/jobs", f.keyA, `{"type":"x","idempotency_key":"order-10"}`); st != http.StatusTooManyRequests {
		t.Fatalf("a genuinely new job must be rejected: %d", st)
	}
}

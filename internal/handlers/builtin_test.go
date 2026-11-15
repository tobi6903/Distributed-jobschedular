package handlers

import (
	"context"
	"encoding/json"
	"io"
	"log/slog"
	"testing"
	"time"

	"github.com/vasistamagam/distributed-jobscheduler/internal/job"
	"github.com/vasistamagam/distributed-jobscheduler/internal/worker"
)

func jb(attempts int, payload string) job.Job {
	return job.Job{ID: "j", Attempts: attempts, Payload: json.RawMessage(payload)}
}

func TestFlaky(t *testing.T) {
	tests := []struct {
		attempts int
		payload  string
		wantErr  bool
	}{
		{1, `{}`, true}, {2, `{}`, true}, {3, `{}`, false},
		{1, `{"succeed_on_attempt":1}`, false},
	}
	for _, tt := range tests {
		if err := Flaky(context.Background(), jb(tt.attempts, tt.payload)); (err != nil) != tt.wantErr {
			t.Errorf("attempt %d %s: err=%v", tt.attempts, tt.payload, err)
		}
	}
}

func TestFailPermanence(t *testing.T) {
	if err := Fail(context.Background(), jb(1, `{}`)); err == nil || worker.IsPermanent(err) {
		t.Fatalf("default fail must be retryable: %v", err)
	}
	if err := Fail(context.Background(), jb(1, `{"permanent":true,"message":"x"}`)); !worker.IsPermanent(err) {
		t.Fatalf("permanent flag ignored: %v", err)
	}
}

func TestBadPayloadIsPermanent(t *testing.T) {
	for name, h := range map[string]worker.Handler{"sleep": Sleep, "fail": Fail, "flaky": Flaky} {
		if err := h(context.Background(), jb(1, `{not json`)); !worker.IsPermanent(err) {
			t.Errorf("%s: bad payload should be permanent, got %v", name, err)
		}
	}
}

func TestSleepHonoursCancellation(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- Sleep(ctx, jb(1, `{"seconds":60}`)) }()
	time.Sleep(20 * time.Millisecond)
	cancel()
	select {
	case err := <-done:
		if err == nil {
			t.Fatal("expected cancellation error")
		}
	case <-time.After(2 * time.Second):
		t.Fatal("Sleep ignored cancellation")
	}
	if err := Sleep(context.Background(), jb(1, `{"seconds":0.01}`)); err != nil {
		t.Fatal(err)
	}
}

func TestRegister(t *testing.T) {
	r := worker.NewRegistry()
	if err := Register(r, slog.New(slog.NewTextHandler(io.Discard, nil))); err != nil {
		t.Fatal(err)
	}
	for _, name := range []string{"noop", "sleep", "fail", "flaky", "log"} {
		if _, ok := r.Lookup(name); !ok {
			t.Errorf("missing handler %s", name)
		}
	}
	if err := Register(r, nil); err == nil {
		t.Fatal("double registration must error")
	}
}

package main

import (
	"bytes"
	"context"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
)

func envMap(m map[string]string) func(string) string { return func(k string) string { return m[k] } }

func TestArgumentErrors(t *testing.T) {
	tests := []struct {
		name string
		args []string
		env  map[string]string
		want string
	}{
		{"no args", nil, nil, "usage"},
		{"unknown command", []string{"frobnicate"}, nil, "unknown command"},
		{"api command without key", []string{"get", "x"}, nil, "JOBCTL_API_KEY"},
		{"submit without type", []string{"submit"}, map[string]string{"JOBCTL_API_KEY": "k"}, "-type is required"},
		{"submit bad payload", []string{"submit", "-type", "t", "-payload", "{nope"}, map[string]string{"JOBCTL_API_KEY": "k"}, "not valid JSON"},
		{"get without id", []string{"get"}, map[string]string{"JOBCTL_API_KEY": "k"}, "expected a job id"},
		{"schedule without sub", []string{"schedule"}, map[string]string{"JOBCTL_API_KEY": "k"}, "expected create"},
		{"schedule create missing flags", []string{"schedule", "create", "-name", "x"}, map[string]string{"JOBCTL_API_KEY": "k"}, "are required"},
		{"migrate without db", []string{"migrate", "up"}, nil, "DATABASE_URL"},
		{"create-key without tenant", []string{"create-key"}, map[string]string{"DATABASE_URL": "x"}, "-tenant is required"},
		{"limits without db", []string{"limits", "list"}, nil, "DATABASE_URL"},
		{"limits no sub", []string{"limits"}, map[string]string{"DATABASE_URL": "x"}, "expected set"},
		{"migrate bad sub", []string{"migrate", "sideways"}, map[string]string{"DATABASE_URL": "x"}, "unknown subcommand"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := run(context.Background(), tt.args, io.Discard, envMap(tt.env))
			if err == nil || !strings.Contains(err.Error(), tt.want) {
				t.Fatalf("err=%v want substring %q", err, tt.want)
			}
		})
	}
}

func TestSubmitAndGetAgainstServer(t *testing.T) {
	var gotBody map[string]any
	var gotAuth string
	polls := 0
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		gotAuth = r.Header.Get("Authorization")
		switch {
		case r.Method == "POST" && r.URL.Path == "/jobs":
			_ = json.NewDecoder(r.Body).Decode(&gotBody)
			w.WriteHeader(201)
			_, _ = io.WriteString(w, `{"id":"abc","status":"pending"}`)
		case r.Method == "GET" && r.URL.Path == "/jobs/abc":
			polls++
			st := "running"
			if polls >= 2 {
				st = "succeeded"
			}
			_, _ = io.WriteString(w, `{"id":"abc","status":"`+st+`"}`)
		case r.URL.Path == "/jobs/missing/replay":
			w.WriteHeader(404)
			_, _ = io.WriteString(w, `{"error":"job not found"}`)
		}
	}))
	defer srv.Close()
	env := envMap(map[string]string{"JOBCTL_URL": srv.URL, "JOBCTL_API_KEY": "secret"})

	var out bytes.Buffer
	err := run(context.Background(), []string{"submit", "-type", "noop", "-payload", `{"a":1}`, "-priority", "5", "-key", "k1", "-delay", "2.5"}, &out, env)
	if err != nil {
		t.Fatal(err)
	}
	if gotAuth != "Bearer secret" || gotBody["type"] != "noop" || gotBody["priority"] != float64(5) ||
		gotBody["idempotency_key"] != "k1" || gotBody["delay_seconds"] != 2.5 {
		t.Fatalf("auth=%q body=%v", gotAuth, gotBody)
	}
	if !strings.Contains(out.String(), `"id": "abc"`) {
		t.Fatalf("output: %s", out.String())
	}

	out.Reset()
	if err := run(context.Background(), []string{"get", "-wait", "abc"}, &out, env); err != nil {
		t.Fatal(err)
	}
	if polls < 2 || !strings.Contains(out.String(), "succeeded") {
		t.Fatalf("-wait should poll until terminal: polls=%d out=%s", polls, out.String())
	}

	err = run(context.Background(), []string{"replay", "missing"}, io.Discard, env)
	if err == nil || !strings.Contains(err.Error(), "job not found") {
		t.Fatalf("server error should surface: %v", err)
	}
}

package config

import (
	"log/slog"
	"strings"
	"testing"
	"time"
)

func env(m map[string]string) func(string) string { return func(k string) string { return m[k] } }

func TestLoadWorkerDefaults(t *testing.T) {
	c, err := LoadWorker(env(map[string]string{"DATABASE_URL": "postgres://x"}))
	if err != nil {
		t.Fatal(err)
	}
	if c.Concurrency != 10 || c.Lease != 30*time.Second || c.Retention != 7*24*time.Hour ||
		len(c.Queues) != 1 || c.Queues[0] != "default" || c.MetricsAddr != ":9100" || c.LogLevel != slog.LevelInfo {
		t.Fatalf("%+v", c)
	}
}

func TestLoadWorkerOverrides(t *testing.T) {
	c, err := LoadWorker(env(map[string]string{
		"DATABASE_URL": "postgres://x", "WORKER_QUEUES": "critical, default ,", "WORKER_CONCURRENCY": "50",
		"WORKER_LEASE": "10s", "LOG_LEVEL": "DEBUG",
	}))
	if err != nil {
		t.Fatal(err)
	}
	if len(c.Queues) != 2 || c.Queues[0] != "critical" || c.Queues[1] != "default" ||
		c.Concurrency != 50 || c.Lease != 10*time.Second || c.LogLevel != slog.LevelDebug {
		t.Fatalf("%+v", c)
	}
}

func TestLoadErrors(t *testing.T) {
	tests := []struct {
		name string
		env  map[string]string
		load func(func(string) string) error
		want []string
	}{
		{"missing db", map[string]string{}, func(g func(string) string) error { _, e := LoadWorker(g); return e }, []string{"DATABASE_URL is required"}},
		{"bad duration", map[string]string{"DATABASE_URL": "x", "WORKER_LEASE": "soon"}, func(g func(string) string) error { _, e := LoadWorker(g); return e }, []string{"WORKER_LEASE"}},
		{"bad int", map[string]string{"DATABASE_URL": "x", "WORKER_CONCURRENCY": "lots"}, func(g func(string) string) error { _, e := LoadWorker(g); return e }, []string{"WORKER_CONCURRENCY"}},
		{"zero concurrency", map[string]string{"DATABASE_URL": "x", "WORKER_CONCURRENCY": "0"}, func(g func(string) string) error { _, e := LoadWorker(g); return e }, []string{">= 1"}},
		{"heartbeat >= lease", map[string]string{"DATABASE_URL": "x", "WORKER_LEASE": "5s", "WORKER_HEARTBEAT_INTERVAL": "5s"}, func(g func(string) string) error { _, e := LoadWorker(g); return e }, []string{"less than"}},
		{"bad log level", map[string]string{"DATABASE_URL": "x", "LOG_LEVEL": "loud"}, func(g func(string) string) error { _, e := LoadAPI(g); return e }, []string{"LOG_LEVEL"}},
		{"reports all errors at once", map[string]string{"WORKER_LEASE": "x", "WORKER_CONCURRENCY": "y"}, func(g func(string) string) error { _, e := LoadWorker(g); return e }, []string{"DATABASE_URL", "WORKER_LEASE", "WORKER_CONCURRENCY"}},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := tt.load(env(tt.env))
			if err == nil {
				t.Fatal("expected error")
			}
			for _, w := range tt.want {
				if !strings.Contains(err.Error(), w) {
					t.Errorf("error %q should mention %q", err, w)
				}
			}
		})
	}
}

func TestLoadAPIAndScheduler(t *testing.T) {
	a, err := LoadAPI(env(map[string]string{"DATABASE_URL": "x", "HTTP_ADDR": ":9"}))
	if err != nil || a.Addr != ":9" || a.ShutdownDelay != 5*time.Second {
		t.Fatalf("%+v %v", a, err)
	}
	s, err := LoadScheduler(env(map[string]string{"DATABASE_URL": "x"}))
	if err != nil || s.Tick != time.Second || s.MetricsAddr != ":9101" {
		t.Fatalf("%+v %v", s, err)
	}
}

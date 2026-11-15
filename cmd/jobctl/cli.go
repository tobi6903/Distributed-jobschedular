package main

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"io"
	"net/http"
	"strconv"
	"strings"
	"time"

	"github.com/vasistamagam/distributed-jobscheduler/internal/migrate"
	"github.com/vasistamagam/distributed-jobscheduler/internal/queue"
)

const usage = `usage: jobctl <command>

commands:
  migrate up|down [n]|version     apply or roll back migrations (DATABASE_URL)
  create-key -tenant T [-name N]  mint an API key for a tenant (DATABASE_URL)
  submit -type T [-queue Q] [-payload JSON] [-priority N] [-delay SECONDS]
         [-max-attempts N] [-timeout SECONDS] [-key IDEMPOTENCY_KEY]
  get [-wait] ID                  show a job (-wait blocks until it is terminal)
  replay ID                       re-queue a failed or dead job
  schedule create -name N -cron EXPR -type T [-tz ZONE] [-misfire fire_once|skip|fire_all] [-payload JSON]
  schedule list|get ID|pause ID|resume ID|delete ID
  limits set -queue Q [-tenant T|*] [-max-running N] [-rate R/s] [-burst B] [-max-backlog M]
  limits list | limits delete -queue Q [-tenant T|*]      (DATABASE_URL; admin only)

environment: JOBCTL_URL (default http://localhost:8080), JOBCTL_API_KEY, DATABASE_URL`

// run is the testable entry point: all I/O goes through its arguments.
func run(ctx context.Context, args []string, out io.Writer, getenv func(string) string) error {
	if len(args) == 0 {
		return errors.New(usage)
	}
	cmd, rest := args[0], args[1:]
	switch cmd {
	case "migrate":
		return cmdMigrate(ctx, rest, out, getenv)
	case "create-key":
		return cmdCreateKey(ctx, rest, out, getenv)
	case "limits":
		return cmdLimits(ctx, rest, out, getenv)
	case "submit", "get", "replay", "schedule":
		c := &client{base: strings.TrimRight(orDefault(getenv("JOBCTL_URL"), "http://localhost:8080"), "/"),
			key: getenv("JOBCTL_API_KEY"), http: &http.Client{Timeout: 30 * time.Second}, out: out}
		if c.key == "" {
			return errors.New("JOBCTL_API_KEY is required")
		}
		switch cmd {
		case "submit":
			return c.submit(ctx, rest)
		case "get":
			return c.get(ctx, rest)
		case "replay":
			return c.replay(ctx, rest)
		default:
			return c.schedule(ctx, rest)
		}
	case "help", "-h", "--help":
		fmt.Fprintln(out, usage)
		return nil
	}
	return fmt.Errorf("unknown command %q\n\n%s", cmd, usage)
}

func orDefault(v, d string) string {
	if v == "" {
		return d
	}
	return v
}

func newFlags(name string) *flag.FlagSet {
	fs := flag.NewFlagSet(name, flag.ContinueOnError)
	fs.SetOutput(io.Discard) // errors are returned, not printed twice
	return fs
}

// ---- database commands ----

func cmdMigrate(ctx context.Context, args []string, out io.Writer, getenv func(string) string) error {
	dsn := getenv("DATABASE_URL")
	if dsn == "" {
		return errors.New("DATABASE_URL is required")
	}
	if len(args) == 0 {
		return errors.New("migrate: expected up, down [n], or version")
	}
	switch args[0] {
	case "up":
		// Wait for the database first: in Kubernetes and Compose the migration often starts before
		// Postgres accepts connections (a fresh install, or a node restart).
		if err := waitForDB(ctx, dsn, 90*time.Second); err != nil {
			return err
		}
		if err := migrate.Up(ctx, dsn); err != nil {
			return err
		}
		v, _, _ := migrate.Version(dsn)
		fmt.Fprintf(out, "migrations applied; schema version %d\n", v)
	case "down":
		n := 1
		if len(args) > 1 {
			var err error
			if n, err = strconv.Atoi(args[1]); err != nil || n < 0 {
				return fmt.Errorf("migrate down: invalid step count %q", args[1])
			}
		}
		if err := migrate.Down(ctx, dsn, n); err != nil {
			return err
		}
		fmt.Fprintf(out, "rolled back %d migration(s)\n", n)
	case "version":
		v, dirty, err := migrate.Version(dsn)
		if err != nil {
			return err
		}
		fmt.Fprintf(out, "version=%d dirty=%v\n", v, dirty)
	default:
		return fmt.Errorf("migrate: unknown subcommand %q", args[0])
	}
	return nil
}

func cmdCreateKey(ctx context.Context, args []string, out io.Writer, getenv func(string) string) error {
	fs := newFlags("create-key")
	tenant := fs.String("tenant", "", "tenant id (required)")
	name := fs.String("name", "default", "label for the key")
	if err := fs.Parse(args); err != nil {
		return err
	}
	if *tenant == "" {
		return errors.New("create-key: -tenant is required")
	}
	dsn := getenv("DATABASE_URL")
	if dsn == "" {
		return errors.New("DATABASE_URL is required")
	}
	pool, err := queue.Connect(ctx, dsn, 2, 10*time.Second)
	if err != nil {
		return err
	}
	defer pool.Close()
	key, err := queue.New(pool).CreateAPIKey(ctx, *tenant, *name)
	if err != nil {
		return err
	}
	// Only the hash is stored; this is the one and only time the key is shown.
	fmt.Fprintln(out, key)
	return nil
}

// ---- API client commands ----

type client struct {
	base string
	key  string
	http *http.Client
	out  io.Writer
}

// do performs a request and pretty-prints the JSON response. Non-2xx statuses are errors.
func (c *client) do(ctx context.Context, method, path string, body any) (map[string]any, error) {
	var rd io.Reader
	if body != nil {
		b, err := json.Marshal(body)
		if err != nil {
			return nil, err
		}
		rd = bytes.NewReader(b)
	}
	req, err := http.NewRequestWithContext(ctx, method, c.base+path, rd)
	if err != nil {
		return nil, err
	}
	req.Header.Set("Authorization", "Bearer "+c.key)
	if body != nil {
		req.Header.Set("Content-Type", "application/json")
	}
	resp, err := c.http.Do(req)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()
	raw, _ := io.ReadAll(io.LimitReader(resp.Body, 4<<20))
	var parsed map[string]any
	_ = json.Unmarshal(raw, &parsed)
	if resp.StatusCode >= 300 {
		msg := strings.TrimSpace(string(raw))
		if e, ok := parsed["error"].(string); ok {
			msg = e
		}
		return nil, fmt.Errorf("%s %s: %s: %s", method, path, resp.Status, msg)
	}
	return parsed, nil
}

func (c *client) print(v map[string]any) error {
	enc := json.NewEncoder(c.out)
	enc.SetIndent("", "  ")
	return enc.Encode(v)
}

func (c *client) submit(ctx context.Context, args []string) error {
	fs := newFlags("submit")
	typ := fs.String("type", "", "job type (required)")
	queueName := fs.String("queue", "", "queue (default \"default\")")
	payload := fs.String("payload", "", "JSON payload")
	prio := fs.Int("priority", 0, "priority, higher runs first")
	delay := fs.Float64("delay", 0, "delay in seconds")
	maxAttempts := fs.Int("max-attempts", 0, "max attempts")
	timeout := fs.Int("timeout", 0, "per-job timeout in seconds")
	key := fs.String("key", "", "idempotency key")
	if err := fs.Parse(args); err != nil {
		return err
	}
	if *typ == "" {
		return errors.New("submit: -type is required")
	}
	body := map[string]any{"type": *typ}
	if *queueName != "" {
		body["queue"] = *queueName
	}
	if *payload != "" {
		if !json.Valid([]byte(*payload)) {
			return errors.New("submit: -payload is not valid JSON")
		}
		body["payload"] = json.RawMessage(*payload)
	}
	if *prio != 0 {
		body["priority"] = *prio
	}
	if *delay != 0 {
		body["delay_seconds"] = *delay
	}
	if *maxAttempts != 0 {
		body["max_attempts"] = *maxAttempts
	}
	if *timeout != 0 {
		body["timeout_seconds"] = *timeout
	}
	if *key != "" {
		body["idempotency_key"] = *key
	}
	res, err := c.do(ctx, "POST", "/jobs", body)
	if err != nil {
		return err
	}
	return c.print(res)
}

func (c *client) get(ctx context.Context, args []string) error {
	fs := newFlags("get")
	wait := fs.Bool("wait", false, "block until the job reaches a terminal state")
	if err := fs.Parse(args); err != nil {
		return err
	}
	if fs.NArg() != 1 {
		return errors.New("get: expected a job id")
	}
	for {
		res, err := c.do(ctx, "GET", "/jobs/"+fs.Arg(0), nil)
		if err != nil {
			return err
		}
		st, _ := res["status"].(string)
		if !*wait || st == "succeeded" || st == "failed" || st == "dead" {
			return c.print(res)
		}
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(500 * time.Millisecond):
		}
	}
}

func (c *client) replay(ctx context.Context, args []string) error {
	if len(args) != 1 {
		return errors.New("replay: expected a job id")
	}
	res, err := c.do(ctx, "POST", "/jobs/"+args[0]+"/replay", nil)
	if err != nil {
		return err
	}
	return c.print(res)
}

func (c *client) schedule(ctx context.Context, args []string) error {
	if len(args) == 0 {
		return errors.New("schedule: expected create, list, get, pause, resume or delete")
	}
	sub, rest := args[0], args[1:]
	switch sub {
	case "create":
		fs := newFlags("schedule create")
		name := fs.String("name", "", "schedule name (required)")
		expr := fs.String("cron", "", "cron expression (required)")
		typ := fs.String("type", "", "job type (required)")
		tz := fs.String("tz", "UTC", "IANA timezone")
		misfire := fs.String("misfire", "fire_once", "fire_once | skip | fire_all")
		queueName := fs.String("queue", "", "queue")
		payload := fs.String("payload", "", "JSON payload")
		if err := fs.Parse(rest); err != nil {
			return err
		}
		if *name == "" || *expr == "" || *typ == "" {
			return errors.New("schedule create: -name, -cron and -type are required")
		}
		body := map[string]any{"name": *name, "cron_expr": *expr, "type": *typ, "timezone": *tz, "misfire_policy": *misfire}
		if *queueName != "" {
			body["queue"] = *queueName
		}
		if *payload != "" {
			if !json.Valid([]byte(*payload)) {
				return errors.New("schedule create: -payload is not valid JSON")
			}
			body["payload"] = json.RawMessage(*payload)
		}
		res, err := c.do(ctx, "POST", "/schedules", body)
		if err != nil {
			return err
		}
		return c.print(res)
	case "list":
		res, err := c.do(ctx, "GET", "/schedules", nil)
		if err != nil {
			return err
		}
		return c.print(res)
	case "get", "delete", "pause", "resume":
		if len(rest) != 1 {
			return fmt.Errorf("schedule %s: expected a schedule id", sub)
		}
		path := "/schedules/" + rest[0]
		switch sub {
		case "get":
			res, err := c.do(ctx, "GET", path, nil)
			if err != nil {
				return err
			}
			return c.print(res)
		case "delete":
			if _, err := c.do(ctx, "DELETE", path, nil); err != nil {
				return err
			}
			fmt.Fprintln(c.out, "deleted")
			return nil
		default:
			res, err := c.do(ctx, "PATCH", path, map[string]any{"enabled": sub == "resume"})
			if err != nil {
				return err
			}
			return c.print(res)
		}
	}
	return fmt.Errorf("schedule: unknown subcommand %q", sub)
}

// ---- limits (admin: talks to the database directly, like create-key) ----

func cmdLimits(ctx context.Context, args []string, out io.Writer, getenv func(string) string) error {
	if len(args) == 0 {
		return errors.New("limits: expected set, list or delete")
	}
	dsn := getenv("DATABASE_URL")
	if dsn == "" {
		return errors.New("DATABASE_URL is required")
	}
	sub, rest := args[0], args[1:]
	fs := newFlags("limits " + sub)
	queueName := fs.String("queue", "", "queue (required for set/delete)")
	tenant := fs.String("tenant", queue.AllTenants, `tenant id, or "*" for every tenant without its own rule`)
	maxRunning := fs.Int("max-running", 0, "concurrency cap: max jobs running at once")
	rate := fs.Float64("rate", 0, "dispatch rate limit in jobs per second")
	burst := fs.Int("burst", 0, "token bucket size (default: ceil(rate))")
	maxBacklog := fs.Int("max-backlog", 0, "reject submissions (HTTP 429) once this many jobs are waiting")
	if err := fs.Parse(rest); err != nil {
		return err
	}
	pool, err := queue.Connect(ctx, dsn, 2, 10*time.Second)
	if err != nil {
		return err
	}
	defer pool.Close()
	store := queue.New(pool)

	switch sub {
	case "set":
		if *queueName == "" {
			return errors.New("limits set: -queue is required")
		}
		l := queue.Limit{Queue: *queueName, TenantID: *tenant}
		if *maxRunning != 0 {
			l.MaxRunning = maxRunning
		}
		if *rate != 0 {
			l.RatePerSecond = rate
		}
		if *burst != 0 {
			l.Burst = burst
		}
		if *maxBacklog != 0 {
			l.MaxBacklog = maxBacklog
		}
		if err := store.SetLimit(ctx, l); err != nil {
			return err
		}
		fmt.Fprintf(out, "limit set on queue %q for tenant %q (takes effect within a few seconds)\n", l.Queue, l.TenantID)
	case "delete":
		if *queueName == "" {
			return errors.New("limits delete: -queue is required")
		}
		if err := store.DeleteLimit(ctx, *queueName, *tenant); err != nil {
			return err
		}
		fmt.Fprintln(out, "deleted")
	case "list":
		list, err := store.ListLimits(ctx)
		if err != nil {
			return err
		}
		enc := json.NewEncoder(out)
		enc.SetIndent("", "  ")
		if list == nil {
			list = []queue.Limit{}
		}
		return enc.Encode(map[string]any{"limits": list})
	default:
		return fmt.Errorf("limits: unknown subcommand %q", sub)
	}
	return nil
}

// waitForDB blocks until Postgres accepts connections or the timeout elapses.
func waitForDB(ctx context.Context, dsn string, wait time.Duration) error {
	pool, err := queue.Connect(ctx, dsn, 1, wait)
	if err != nil {
		return err
	}
	pool.Close()
	return nil
}

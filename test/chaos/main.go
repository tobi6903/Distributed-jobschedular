// Command chaos runs the real worker and scheduler binaries against a throwaway database and
// SIGKILLs them at random while work is flowing. It then checks the guarantees:
//
//   - no job is lost: every job reaches `succeeded`
//   - no cron slot is missed or duplicated across scheduler leader crashes
//
// Usage: go build ./cmd/... -o bin/ ; go run ./test/chaos -db $ADMIN_DATABASE_URL
// (see `make chaos`).
package main

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"flag"
	"fmt"
	"log"
	mrand "math/rand/v2"
	"net/url"
	"os"
	"os/exec"
	"os/signal"
	"sort"
	"strings"
	"sync"
	"syscall"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/vasistamagam/distributed-jobscheduler/internal/migrate"
)

type proc struct {
	name string
	cmd  *exec.Cmd
}

type fleet struct {
	mu    sync.Mutex
	bin   string
	procs map[string]*proc
	kills int
	env   []string
}

func (f *fleet) start(kind string, i int) {
	name := fmt.Sprintf("%s-%d", kind, i)
	cmd := exec.Command(f.bin + "/" + kind)
	cmd.Env = append(os.Environ(), f.env...)
	cmd.Stdout, cmd.Stderr = nil, nil // keep the harness output readable
	if err := cmd.Start(); err != nil {
		log.Fatalf("start %s: %v", name, err)
	}
	go func() { _ = cmd.Wait() }() // reap the zombie when it dies
	f.mu.Lock()
	f.procs[name] = &proc{name: name, cmd: cmd}
	f.mu.Unlock()
}

// kill9 sends SIGKILL (no graceful shutdown, no deferred cleanup: a real crash) and respawns.
func (f *fleet) kill9(kind string, i int) {
	name := fmt.Sprintf("%s-%d", kind, i)
	f.mu.Lock()
	p := f.procs[name]
	f.kills++
	f.mu.Unlock()
	if p != nil {
		_ = p.cmd.Process.Signal(syscall.SIGKILL)
	}
	time.Sleep(200 * time.Millisecond)
	f.start(kind, i)
}

func (f *fleet) stopAll() {
	f.mu.Lock()
	defer f.mu.Unlock()
	for _, p := range f.procs {
		_ = p.cmd.Process.Signal(syscall.SIGTERM)
	}
}

func main() {
	var (
		adminURL  = flag.String("db", os.Getenv("ADMIN_DATABASE_URL"), "admin connection string (needs CREATEDB)")
		binDir    = flag.String("bin", "bin", "directory containing the worker and scheduler binaries")
		workers   = flag.Int("workers", 6, "worker processes")
		jobs      = flag.Int("jobs", 4000, "jobs to process")
		jobMillis = flag.Int("job-ms", 150, "simulated work per job")
		killEvery = flag.Duration("kill-every", 2*time.Second, "kill a random worker this often")
		schedKill = flag.Duration("sched-kill-every", 6*time.Second, "kill a scheduler replica this often")
		timeout   = flag.Duration("timeout", 3*time.Minute, "give up after this long")
	)
	flag.Parse()
	if *adminURL == "" {
		log.Fatal("set -db or ADMIN_DATABASE_URL (e.g. postgres://postgres@localhost:5432/postgres?sslmode=disable)")
	}
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	// Fresh database so runs never interfere with each other.
	var rnd [4]byte
	_, _ = rand.Read(rnd[:])
	dbName := "chaos_" + hex.EncodeToString(rnd[:])
	admin, err := pgxpool.New(ctx, *adminURL)
	must(err, "connect admin")
	_, err = admin.Exec(ctx, "CREATE DATABASE "+dbName)
	must(err, "create database")
	defer func() {
		_, _ = admin.Exec(context.Background(), "DROP DATABASE IF EXISTS "+dbName+" WITH (FORCE)")
		admin.Close()
	}()
	dsn := withDB(*adminURL, dbName)
	must(migrate.Up(ctx, dsn), "migrate")
	pool, err := pgxpool.New(ctx, dsn)
	must(err, "connect")
	defer pool.Close()

	// Workload: N jobs that each "work" for a bit, plus a per-second cron schedule (fire_all so
	// that slots missed while no scheduler is alive are caught up, never skipped).
	_, err = pool.Exec(ctx, `INSERT INTO jobs (tenant_id, type, payload, max_attempts)
		SELECT 'chaos', 'sleep', jsonb_build_object('seconds', $2::float8 / 1000), 20 FROM generate_series(1, $1)`,
		*jobs, *jobMillis)
	must(err, "insert jobs")
	_, err = pool.Exec(ctx, `INSERT INTO schedules (tenant_id, name, cron_expr, misfire_policy, type, next_fire_at)
		VALUES ('chaos', 'every-second', '@every 1s', 'fire_all', 'noop', date_trunc('second', now()) + interval '1 second')`)
	must(err, "insert schedule")

	f := &fleet{bin: *binDir, procs: map[string]*proc{}, env: []string{
		"DATABASE_URL=" + dsn, "DB_MAX_CONNS=10", "LOG_LEVEL=error",
		"WORKER_CONCURRENCY=20", "WORKER_LEASE=3s", "WORKER_HEARTBEAT_INTERVAL=1s", "WORKER_REAP_INTERVAL=1s",
		"WORKER_POLL_INTERVAL=100ms", "WORKER_SHUTDOWN_GRACE=5s", "BACKOFF_BASE=1s", "METRICS_ADDR=127.0.0.1:0",
		"SCHEDULER_TICK=200ms", "SCHEDULER_ELECTION_RETRY=300ms",
	}}
	for i := 0; i < *workers; i++ {
		f.start("worker", i)
	}
	for i := 0; i < 2; i++ {
		f.start("scheduler", i)
	}
	defer f.stopAll()

	start := time.Now()
	fmt.Printf("chaos: %d jobs, %d workers, 2 schedulers; SIGKILL a worker every %v, a scheduler every %v\n",
		*jobs, *workers, *killEvery, *schedKill)

	killT := time.NewTicker(*killEvery)
	schedT := time.NewTicker(*schedKill)
	progT := time.NewTicker(3 * time.Second)
	defer killT.Stop()
	defer schedT.Stop()
	defer progT.Stop()
	deadline := time.After(*timeout)
	schedTurn := 0
	for done := false; !done; {
		select {
		case <-ctx.Done():
			log.Fatal("interrupted")
		case <-deadline:
			fmt.Println("TIMEOUT waiting for all jobs to finish")
			done = true
		case <-killT.C:
			i := mrand.IntN(*workers)
			f.kill9("worker", i)
		case <-schedT.C:
			f.kill9("scheduler", schedTurn%2)
			schedTurn++
		case <-progT.C:
			ok, other := counts(ctx, pool)
			fmt.Printf("  t=%3ds  succeeded=%d  outstanding=%d  kills=%d\n", int(time.Since(start).Seconds()), ok, other, f.kills)
			if ok >= *jobs && other == 0 {
				done = true
			}
		}
	}
	// Let the cron keep running a few more seconds so the final leader catches up any backlog.
	time.Sleep(4 * time.Second)
	elapsed := time.Since(start)

	failed := report(ctx, pool, *jobs, elapsed, f.kills)
	f.stopAll()
	if failed {
		os.Exit(1)
	}
}

func counts(ctx context.Context, pool *pgxpool.Pool) (ok, other int) {
	_ = pool.QueryRow(ctx, `SELECT count(*) FILTER (WHERE status='succeeded'),
		count(*) FILTER (WHERE status NOT IN ('succeeded')) FROM jobs WHERE type='sleep'`).Scan(&ok, &other)
	return
}

func report(ctx context.Context, pool *pgxpool.Pool, want int, elapsed time.Duration, kills int) (failed bool) {
	var total, ok, dead, failedSt, reclaimed, running int
	must(pool.QueryRow(ctx, `SELECT count(*), count(*) FILTER (WHERE status='succeeded'),
		count(*) FILTER (WHERE status='dead'), count(*) FILTER (WHERE status='failed'),
		count(*) FILTER (WHERE attempts > 1), count(*) FILTER (WHERE status='running')
		FROM jobs WHERE type='sleep'`).Scan(&total, &ok, &dead, &failedSt, &reclaimed, &running), "report")

	fmt.Println("\n=== chaos report ===")
	fmt.Printf("elapsed:           %v\n", elapsed.Round(time.Second))
	fmt.Printf("processes killed:  %d (SIGKILL)\n", kills)
	fmt.Printf("jobs submitted:    %d\n", want)
	fmt.Printf("jobs succeeded:    %d\n", ok)
	fmt.Printf("jobs dead/failed:  %d / %d\n", dead, failedSt)
	fmt.Printf("jobs lost:         %d   (rows missing or never finished)\n", want-ok)
	fmt.Printf("jobs recovered:    %d   (needed >1 attempt: their worker was killed mid-job)\n", reclaimed)
	if ok != want || total != want {
		fmt.Println("FAIL: jobs were lost or not completed")
		failed = true
	}

	// Cron: every slot between the first and last must exist exactly once, with no gaps.
	rows, err := pool.Query(ctx, `SELECT idempotency_key FROM jobs WHERE idempotency_key LIKE 'cron:%' ORDER BY idempotency_key`)
	must(err, "cron keys")
	var slots []time.Time
	seen := map[string]bool{}
	dups := 0
	for rows.Next() {
		var k string
		must(rows.Scan(&k), "scan")
		if seen[k] {
			dups++
		}
		seen[k] = true
		// key format: cron:<uuid>:<RFC3339>
		parts := strings.SplitN(k, ":", 3)
		t, err := time.Parse(time.RFC3339, parts[2])
		must(err, "parse slot")
		slots = append(slots, t)
	}
	rows.Close()
	sort.Slice(slots, func(i, j int) bool { return slots[i].Before(slots[j]) })
	gaps := 0
	for i := 1; i < len(slots); i++ {
		if d := slots[i].Sub(slots[i-1]); d != time.Second {
			gaps++
			fmt.Printf("  gap between %s and %s (%v)\n", slots[i-1].Format("15:04:05"), slots[i].Format("15:04:05"), d)
		}
	}
	fmt.Printf("cron slots fired:  %d   duplicates: %d   gaps: %d\n", len(slots), dups, gaps)
	if len(slots) < 5 || dups > 0 || gaps > 0 {
		fmt.Println("FAIL: cron slots missed or duplicated")
		failed = true
	}
	if !failed {
		fmt.Println("PASS: nothing lost, nothing double-fired")
	}
	return failed
}

func withDB(base, db string) string {
	u, err := url.Parse(base)
	must(err, "parse db url")
	u.Path = "/" + db
	return u.String()
}

func must(err error, what string) {
	if err != nil {
		log.Fatalf("%s: %v", what, err)
	}
}

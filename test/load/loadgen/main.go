// Command loadgen is a small load generator for the job API. It exists so benchmarks can be
// reproduced with `go run` (no k6 needed); the k6 script in test/load/k6.js covers CI and
// Kubernetes use.
//
//	loadgen -api http://localhost:8080 -key $KEY -n 50000 -c 64 [-db $DATABASE_URL]
//
// It measures submit throughput and latency, and with -db it also waits for every submitted
// job to finish and reports end-to-end drain throughput.
package main

import (
	"bytes"
	"context"
	"flag"
	"fmt"
	"io"
	"log"
	"net/http"
	"os"
	"sort"
	"sync"
	"sync/atomic"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
)

func main() {
	var (
		api     = flag.String("api", "http://localhost:8080", "API base URL")
		key     = flag.String("key", os.Getenv("JOBCTL_API_KEY"), "API key")
		n       = flag.Int("n", 20000, "jobs to submit")
		conc    = flag.Int("c", 64, "concurrent clients")
		typ     = flag.String("type", "noop", "job type")
		payload = flag.String("payload", `{}`, "job payload JSON")
		dbURL   = flag.String("db", "", "DATABASE_URL: if set, wait for all jobs to finish and report drain throughput")
		tenant  = flag.String("tenant", "", "tenant id the key belongs to (needed with -db to count its jobs)")
		wait    = flag.Duration("wait", 10*time.Minute, "max time to wait for the drain")
	)
	flag.Parse()
	if *key == "" {
		log.Fatal("set -key or JOBCTL_API_KEY")
	}

	client := &http.Client{
		Timeout: 30 * time.Second,
		Transport: &http.Transport{
			MaxIdleConns: *conc * 2, MaxIdleConnsPerHost: *conc * 2, IdleConnTimeout: 30 * time.Second,
		},
	}
	body := fmt.Sprintf(`{"type":%q,"payload":%s}`, *typ, *payload)

	var next, failures atomic.Int64
	lats := make([][]time.Duration, *conc)
	var wg sync.WaitGroup
	start := time.Now()
	for w := 0; w < *conc; w++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for next.Add(1) <= int64(*n) {
				t0 := time.Now()
				req, _ := http.NewRequest("POST", *api+"/jobs", bytes.NewBufferString(body))
				req.Header.Set("Authorization", "Bearer "+*key)
				resp, err := client.Do(req)
				if err != nil {
					failures.Add(1)
					continue
				}
				_, _ = io.Copy(io.Discard, resp.Body)
				resp.Body.Close()
				if resp.StatusCode != http.StatusCreated {
					failures.Add(1)
					continue
				}
				lats[w] = append(lats[w], time.Since(t0))
			}
		}()
	}
	wg.Wait()
	submitDur := time.Since(start)

	var all []time.Duration
	for _, l := range lats {
		all = append(all, l...)
	}
	sort.Slice(all, func(i, j int) bool { return all[i] < all[j] })
	pct := func(p float64) time.Duration {
		if len(all) == 0 {
			return 0
		}
		return all[min(len(all)-1, int(float64(len(all))*p))]
	}
	fmt.Printf("submit: %d ok, %d failed in %v  =>  %.0f req/s (c=%d)\n", len(all), failures.Load(),
		submitDur.Round(time.Millisecond), float64(len(all))/submitDur.Seconds(), *conc)
	fmt.Printf("submit latency: p50=%v p95=%v p99=%v max=%v\n", pct(.5).Round(10*time.Microsecond),
		pct(.95).Round(10*time.Microsecond), pct(.99).Round(10*time.Microsecond), pct(1).Round(10*time.Microsecond))

	if *dbURL == "" {
		return
	}
	ctx, cancel := context.WithTimeout(context.Background(), *wait)
	defer cancel()
	pool, err := pgxpool.New(ctx, *dbURL)
	if err != nil {
		log.Fatal(err)
	}
	defer pool.Close()
	for {
		var done, pending int
		err := pool.QueryRow(ctx, `SELECT count(*) FILTER (WHERE status = 'succeeded'),
			count(*) FILTER (WHERE status NOT IN ('succeeded', 'dead', 'failed'))
			FROM jobs WHERE ($1 = '' OR tenant_id = $1)`, *tenant).Scan(&done, &pending)
		if err != nil {
			log.Fatalf("poll: %v", err)
		}
		if pending == 0 {
			total := time.Since(start)
			fmt.Printf("drain:  %d jobs succeeded; end-to-end %v  =>  %.0f jobs/s from first submit to last completion\n",
				done, total.Round(time.Millisecond), float64(done)/total.Seconds())
			return
		}
		select {
		case <-ctx.Done():
			log.Fatalf("timed out with %d jobs still pending", pending)
		case <-time.After(250 * time.Millisecond):
		}
	}
}

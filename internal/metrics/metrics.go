// Package metrics defines the Prometheus metrics. Each binary builds its own Registry
// (no global default registry), which keeps tests isolated and avoids duplicate-registration
// panics.
package metrics

import (
	"context"
	"log/slog"
	"net/http"
	"sync"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/collectors"
	"github.com/prometheus/client_golang/prometheus/promhttp"

	"github.com/vasistamagam/distributed-jobscheduler/internal/job"
	"github.com/vasistamagam/distributed-jobscheduler/internal/queue"
)

const ns = "jobscheduler"

// Latency buckets span 5ms to ~10 minutes: pickup latency is typically ms to seconds, job
// duration seconds to minutes.
var (
	pickupBuckets   = []float64{.005, .01, .025, .05, .1, .25, .5, 1, 2.5, 5, 10, 30, 60, 300}
	durationBuckets = []float64{.005, .025, .1, .5, 1, 2.5, 5, 10, 30, 60, 120, 300, 600}
	httpBuckets     = []float64{.001, .005, .01, .025, .05, .1, .25, .5, 1, 2.5, 5}
)

// Metrics owns a registry and all collectors.
type Metrics struct {
	Registry *prometheus.Registry

	claimed      *prometheus.CounterVec
	pickup       *prometheus.HistogramVec
	duration     *prometheus.HistogramVec
	retries      *prometheus.CounterVec
	reaped       *prometheus.CounterVec
	deleted      prometheus.Counter
	batchSize    prometheus.Histogram
	wakeups      *prometheus.CounterVec
	claimPolls   *prometheus.CounterVec
	listenerUp   prometheus.Gauge
	notifySent   *prometheus.CounterVec
	httpRequests *prometheus.CounterVec
	httpDuration *prometheus.HistogramVec
	submitted    *prometheus.CounterVec
	rejected     *prometheus.CounterVec
	schedFires   *prometheus.CounterVec
	isLeader     prometheus.Gauge
}

// New creates the registry with Go runtime and process collectors plus the app metrics.
func New() *Metrics {
	reg := prometheus.NewRegistry()
	reg.MustRegister(collectors.NewGoCollector(), collectors.NewProcessCollector(collectors.ProcessCollectorOpts{}))
	f := func(name, help string) prometheus.Opts { return prometheus.Opts{Namespace: ns, Name: name, Help: help} }
	m := &Metrics{
		Registry: reg,
		claimed: prometheus.NewCounterVec(prometheus.CounterOpts(f("jobs_claimed_total",
			"Jobs claimed by workers.")), []string{"queue"}),
		pickup: prometheus.NewHistogramVec(prometheus.HistogramOpts{Namespace: ns, Name: "job_pickup_latency_seconds",
			Help: "Time between a job becoming runnable (run_at) and a worker claiming it.", Buckets: pickupBuckets}, []string{"queue"}),
		duration: prometheus.NewHistogramVec(prometheus.HistogramOpts{Namespace: ns, Name: "job_duration_seconds",
			Help: "Handler wall-clock time, by outcome.", Buckets: durationBuckets}, []string{"queue", "type", "outcome"}),
		retries: prometheus.NewCounterVec(prometheus.CounterOpts(f("job_retries_total",
			"Failed attempts that were scheduled for retry.")), []string{"queue", "type"}),
		reaped: prometheus.NewCounterVec(prometheus.CounterOpts(f("jobs_reaped_total",
			"Jobs recovered from expired leases (worker crash or stall).")), []string{"result"}),
		deleted: prometheus.NewCounter(prometheus.CounterOpts(f("jobs_retention_deleted_total",
			"Finished jobs deleted by the retention sweeper."))),
		batchSize: prometheus.NewHistogram(prometheus.HistogramOpts{Namespace: ns, Name: "complete_batch_size",
			Help: "Jobs recorded as succeeded per batched write (worker completion batching).", Buckets: []float64{1, 2, 5, 10, 20, 50, 100}}),
		claimPolls: prometheus.NewCounterVec(prometheus.CounterOpts(f("claim_polls_total",
			"Claim queries issued by workers. result=empty is idle polling: the load an idle worker puts on the database.")), []string{"queue", "result"}),
		wakeups: prometheus.NewCounterVec(prometheus.CounterOpts(f("worker_wakeups_total",
			"Idle-worker wake-ups caused by a notification. outcome=claimed found work; empty lost the race to another worker or was stale.")), []string{"outcome"}),
		listenerUp: prometheus.NewGauge(prometheus.GaugeOpts(f("worker_notify_listener_up",
			"1 while this worker's LISTEN connection is established (0: it is polling only)."))),
		notifySent: prometheus.NewCounterVec(prometheus.CounterOpts(f("notifications_sent_total",
			"Wake-up notifications published after jobs became runnable. result=error means a hint was lost; polling still finds the job.")), []string{"result"}),
		httpRequests: prometheus.NewCounterVec(prometheus.CounterOpts(f("http_requests_total",
			"HTTP requests.")), []string{"route", "method", "code"}),
		httpDuration: prometheus.NewHistogramVec(prometheus.HistogramOpts{Namespace: ns, Name: "http_request_duration_seconds",
			Help: "HTTP request latency.", Buckets: httpBuckets}, []string{"route", "method"}),
		submitted: prometheus.NewCounterVec(prometheus.CounterOpts(f("jobs_submitted_total",
			"Jobs submitted via the API. created=false means an idempotency-key hit.")), []string{"queue", "created"}),
		rejected: prometheus.NewCounterVec(prometheus.CounterOpts(f("submissions_rejected_total",
			"Submissions refused by backpressure (HTTP 429), by reason.")), []string{"queue", "reason"}),
		schedFires: prometheus.NewCounterVec(prometheus.CounterOpts(f("scheduler_fires_total",
			"Cron job creations. result=duplicate_prevented means the idempotent fire key blocked a second insert.")), []string{"result"}),
		isLeader: prometheus.NewGauge(prometheus.GaugeOpts(f("scheduler_is_leader",
			"1 while this scheduler replica holds leadership."))),
	}
	// Create the fixed-label counters at zero NOW. Prometheus's rate() and increase() cannot see the jump from
	// "this series does not exist" to its first value, so a counter born at its first increment hides that first
	// event: without this the first crash recovery on every worker would be invisible and WorkerCrashesDetected
	// (increase(jobs_reaped_total) > 0) would miss it. Counters with dynamic labels (retries per job type,
	// rejections per queue) cannot be pre-created, so alert on sustained rates, never on a single event.
	m.reaped.WithLabelValues("retried")
	m.reaped.WithLabelValues("dead")
	m.schedFires.WithLabelValues("created")
	m.schedFires.WithLabelValues("duplicate_prevented")
	m.wakeups.WithLabelValues("claimed")
	m.wakeups.WithLabelValues("empty")
	m.notifySent.WithLabelValues("ok")
	m.notifySent.WithLabelValues("error")
	reg.MustRegister(m.claimPolls, m.wakeups, m.listenerUp, m.notifySent, m.rejected, m.schedFires, m.isLeader, m.claimed, m.pickup, m.duration, m.retries, m.reaped, m.deleted, m.batchSize,
		m.httpRequests, m.httpDuration, m.submitted)
	return m
}

// Handler serves /metrics for this registry.
func (m *Metrics) Handler() http.Handler {
	return promhttp.HandlerFor(m.Registry, promhttp.HandlerOpts{})
}

// WorkerObserver adapts Metrics to worker.Observer.
func (m *Metrics) WorkerObserver() *workerObserver { return &workerObserver{m} }

type workerObserver struct{ m *Metrics }

func (o *workerObserver) Claimed(q string, n int, lats []time.Duration) {
	o.m.claimed.WithLabelValues(q).Add(float64(n))
	for _, l := range lats {
		o.m.pickup.WithLabelValues(q).Observe(l.Seconds())
	}
}

func (o *workerObserver) Finished(q, typ, outcome string, d time.Duration) {
	o.m.duration.WithLabelValues(q, typ, outcome).Observe(d.Seconds())
	if outcome == "retry" {
		o.m.retries.WithLabelValues(q, typ).Inc()
	}
}

func (o *workerObserver) Reaped(retried, dead int) {
	o.m.reaped.WithLabelValues("retried").Add(float64(retried))
	o.m.reaped.WithLabelValues("dead").Add(float64(dead))
}

// Prime creates the worker's per-queue and per-type series at zero, at startup, for the queues it polls and the
// job types it can run. A counter born at its first increment hides that increment from rate() and badly
// under-reports the first seconds of a series' life: when a worker that had never claimed anything joined a
// 60,000-job burst, Prometheus showed it at 41 to 60% of its peers for about 14 s although its logs show the same
// rate from the second second. Only the series the throughput and claim panels read are primed (the "succeeded"
// outcome), because priming every outcome would multiply the duration histogram's series by six per type.
func (o *workerObserver) Prime(queues, types []string) {
	for _, q := range queues {
		o.m.claimed.WithLabelValues(q)
		o.m.pickup.WithLabelValues(q)
		o.m.claimPolls.WithLabelValues(q, "hit")
		o.m.claimPolls.WithLabelValues(q, "empty")
		for _, t := range types {
			o.m.duration.WithLabelValues(q, t, "succeeded")
		}
	}
}

func (o *workerObserver) Deleted(n int) { o.m.deleted.Add(float64(n)) }

func (o *workerObserver) CompleteBatch(n int) { o.m.batchSize.Observe(float64(n)) }

func (o *workerObserver) ClaimPoll(q string, got int) {
	if got > 0 {
		o.m.claimPolls.WithLabelValues(q, "hit").Inc()
	} else {
		o.m.claimPolls.WithLabelValues(q, "empty").Inc()
	}
}

func (o *workerObserver) Wakeup(claimed bool) {
	if claimed {
		o.m.wakeups.WithLabelValues("claimed").Inc()
	} else {
		o.m.wakeups.WithLabelValues("empty").Inc()
	}
}

// ListenerUp is the callback for queue.Listener: 1 while the LISTEN connection is established.
func (m *Metrics) ListenerUp(up bool) {
	if up {
		m.listenerUp.Set(1)
	} else {
		m.listenerUp.Set(0)
	}
}

// NotifySent is the callback for queue.Notifier: the outcome of one published notification.
func (m *Metrics) NotifySent(ok bool) {
	if ok {
		m.notifySent.WithLabelValues("ok").Inc()
	} else {
		m.notifySent.WithLabelValues("error").Inc()
	}
}

// APIObserver adapts Metrics to api.Observer.
func (m *Metrics) APIObserver() *apiObserver { return &apiObserver{m} }

type apiObserver struct{ m *Metrics }

func (o *apiObserver) Request(route, method string, status int, d time.Duration) {
	o.m.httpRequests.WithLabelValues(route, method, statusClass(status)).Inc()
	o.m.httpDuration.WithLabelValues(route, method).Observe(d.Seconds())
}

func (o *apiObserver) JobSubmitted(q string, created bool) {
	c := "true"
	if !created {
		c = "false"
	}
	o.m.submitted.WithLabelValues(q, c).Inc()
}

func (o *apiObserver) JobRejected(q, reason string) {
	o.m.rejected.WithLabelValues(q, reason).Inc()
}

// statusClass keeps label cardinality low and the dashboards readable: 2xx, 4xx, 5xx.
func statusClass(code int) string {
	switch {
	case code >= 500:
		return "5xx"
	case code >= 400:
		return "4xx"
	case code >= 300:
		return "3xx"
	default:
		return "2xx"
	}
}

// DepthSource is what the queue collector reads from the database.
type DepthSource interface {
	Depth(ctx context.Context) ([]queue.DepthRow, error)
	OldestReady(ctx context.Context) (map[string]time.Duration, error)
}

// QueueCollector exports database-derived gauges (queue depth, DLQ size, oldest ready job
// age). It queries lazily on scrape with a short cache, so many scrapers cost one query.
// Enabled gates collection: the scheduler enables it only while it is the elected leader,
// so there is exactly one series per queue instead of one per replica.
type QueueCollector struct {
	src     DepthSource
	log     *slog.Logger
	ttl     time.Duration
	enabled func() bool

	depth  *prometheus.Desc
	dlq    *prometheus.Desc
	oldest *prometheus.Desc

	mu      sync.Mutex
	at      time.Time
	counts  map[string]map[job.Status]int64 // queue -> status -> count
	ages    map[string]time.Duration
	known   map[string]struct{} // every queue ever seen, so a drained queue reports 0, not "absent"
	lastErr error
}

// NewQueueCollector builds the collector. enabled may be nil (always on).
func NewQueueCollector(src DepthSource, log *slog.Logger, ttl time.Duration, enabled func() bool) *QueueCollector {
	if enabled == nil {
		enabled = func() bool { return true }
	}
	return &QueueCollector{
		src: src, log: log, ttl: ttl, enabled: enabled,
		known:  map[string]struct{}{job.DefaultQueue: {}},
		depth:  prometheus.NewDesc(ns+"_queue_depth", "Jobs by queue and status (pending, retrying, running, dead).", []string{"queue", "status"}, nil),
		dlq:    prometheus.NewDesc(ns+"_dlq_size", "Jobs in the dead-letter state.", []string{"queue"}, nil),
		oldest: prometheus.NewDesc(ns+"_oldest_ready_job_age_seconds", "Age of the oldest job that is ready to run but unclaimed.", []string{"queue"}, nil),
	}
}

func (c *QueueCollector) Describe(ch chan<- *prometheus.Desc) {
	ch <- c.depth
	ch <- c.dlq
	ch <- c.oldest
}

func (c *QueueCollector) Collect(ch chan<- prometheus.Metric) {
	if !c.enabled() {
		return
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	if time.Since(c.at) > c.ttl {
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		rows, err := c.src.Depth(ctx)
		if err == nil {
			var ages map[string]time.Duration
			if ages, err = c.src.OldestReady(ctx); err == nil {
				counts := map[string]map[job.Status]int64{}
				for _, r := range rows {
					if counts[r.Queue] == nil {
						counts[r.Queue] = map[job.Status]int64{}
					}
					counts[r.Queue][r.Status] = r.Count
					c.known[r.Queue] = struct{}{}
				}
				c.counts, c.ages = counts, ages
			}
		}
		c.lastErr = err
		if err != nil {
			c.log.Error("queue metrics query failed", "err", err)
		}
		c.at = time.Now()
	}
	if c.counts == nil {
		return // never had a successful query: export nothing rather than lie
	}
	// Emit every (known queue, status) pair, zero when absent, so alert expressions such as
	// "dlq_size > 0" and "queue_depth == 0" evaluate against a real series instead of nothing.
	for q := range c.known {
		for _, st := range []job.Status{job.StatusPending, job.StatusRetrying, job.StatusRunning, job.StatusDead} {
			ch <- prometheus.MustNewConstMetric(c.depth, prometheus.GaugeValue, float64(c.counts[q][st]), q, string(st))
		}
		ch <- prometheus.MustNewConstMetric(c.dlq, prometheus.GaugeValue, float64(c.counts[q][job.StatusDead]), q)
		ch <- prometheus.MustNewConstMetric(c.oldest, prometheus.GaugeValue, c.ages[q].Seconds(), q)
	}
}

// SchedulerObserver adapts Metrics to scheduler.Observer.
func (m *Metrics) SchedulerObserver() *schedulerObserver { return &schedulerObserver{m} }

type schedulerObserver struct{ m *Metrics }

func (o *schedulerObserver) Leader(v bool) {
	if v {
		o.m.isLeader.Set(1)
	} else {
		o.m.isLeader.Set(0)
	}
}

func (o *schedulerObserver) Fired(fired, skipped int) {
	o.m.schedFires.WithLabelValues("created").Add(float64(fired))
	o.m.schedFires.WithLabelValues("duplicate_prevented").Add(float64(skipped))
}

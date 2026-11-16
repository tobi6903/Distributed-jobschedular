// Package helm renders the Helm chart in-process and checks the resulting manifests.
//
// Helm itself is a build-time tool we do not want as a Go dependency, so this test renders the
// chart with Go's text/template (the engine Helm uses) plus small stand-ins for the handful of
// Helm/Sprig functions the chart relies on. It is a fast guard against template errors and
// against regressions in the properties we care about (probes, limits, security context,
// termination grace period vs job timeout, KEDA wiring, NetworkPolicy). CI additionally runs
// the real `helm lint` and `kubeconform` (see .github/workflows/ci.yml).
package helm

import (
	"bytes"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"reflect"
	"sort"
	"strconv"
	"strings"
	"testing"
	"text/template"

	"gopkg.in/yaml.v3"
)

const chartDir = "../../deploy/helm/jobscheduler"

type obj = map[string]any

// ---- minimal Helm function set ----

func toStr(v any) string {
	switch x := v.(type) {
	case nil:
		return ""
	case string:
		return x
	default:
		return fmt.Sprint(x)
	}
}

func toInt(v any) int {
	switch x := v.(type) {
	case int:
		return x
	case int64:
		return int(x)
	case float64:
		return int(x)
	case string:
		n, _ := strconv.Atoi(x)
		return n
	}
	return 0
}

func empty(v any) bool {
	if v == nil {
		return true
	}
	rv := reflect.ValueOf(v)
	switch rv.Kind() {
	case reflect.String, reflect.Map, reflect.Slice:
		return rv.Len() == 0
	case reflect.Bool:
		return !rv.Bool()
	case reflect.Int, reflect.Int64, reflect.Float64:
		return rv.IsZero()
	}
	return false
}

func indent(n int, s string) string {
	pad := strings.Repeat(" ", n)
	return pad + strings.ReplaceAll(s, "\n", "\n"+pad)
}

func funcs(t *template.Template) template.FuncMap {
	return template.FuncMap{
		"include": func(name string, data any) (string, error) {
			var b bytes.Buffer
			err := t.ExecuteTemplate(&b, name, data)
			return b.String(), err
		},
		"toYaml": func(v any) (string, error) {
			b, err := yaml.Marshal(v)
			return strings.TrimSuffix(string(b), "\n"), err
		},
		"nindent": func(n int, s string) string { return "\n" + indent(n, s) },
		"indent":  indent,
		"quote":   func(v any) string { return strconv.Quote(toStr(v)) },
		"default": func(d any, v ...any) any {
			if len(v) == 0 || empty(v[0]) {
				return d
			}
			return v[0]
		},
		"add": func(xs ...any) int {
			s := 0
			for _, x := range xs {
				s += toInt(x)
			}
			return s
		},
		"int":        toInt,
		"trunc":      func(n int, s string) string { return s[:min(n, len(s))] },
		"trimSuffix": func(suf, s string) string { return strings.TrimSuffix(s, suf) },
		"contains":   func(sub, s string) bool { return strings.Contains(s, sub) },
		"dict": func(kv ...any) obj {
			m := obj{}
			for i := 0; i+1 < len(kv); i += 2 {
				m[toStr(kv[i])] = kv[i+1]
			}
			return m
		},
		"list": func(xs ...any) []any { return xs },
		"fail": func(msg string) (string, error) { return "", errors.New(msg) },
	}
}

// ---- rendering ----

func deepMerge(dst, src obj) {
	for k, v := range src {
		if sm, ok := v.(obj); ok {
			if dm, ok := dst[k].(obj); ok {
				deepMerge(dm, sm)
				continue
			}
		}
		dst[k] = v
	}
}

func loadValues(t *testing.T, overrides obj) obj {
	t.Helper()
	raw, err := os.ReadFile(filepath.Join(chartDir, "values.yaml"))
	if err != nil {
		t.Fatal(err)
	}
	vals := obj{}
	if err := yaml.Unmarshal(raw, &vals); err != nil {
		t.Fatalf("values.yaml: %v", err)
	}
	deepMerge(vals, overrides)
	return vals
}

// render returns all manifests produced for the given value overrides.
func render(t *testing.T, overrides obj) ([]obj, error) {
	t.Helper()
	chartRaw, _ := os.ReadFile(filepath.Join(chartDir, "Chart.yaml"))
	chart := obj{}
	if err := yaml.Unmarshal(chartRaw, &chart); err != nil {
		t.Fatal(err)
	}
	ctx := obj{
		"Values":  loadValues(t, overrides),
		"Release": obj{"Name": "js", "Namespace": "jobs", "Service": "Helm", "Revision": 1},
		"Chart":   obj{"Name": chart["name"], "Version": chart["version"], "AppVersion": chart["appVersion"]},
	}

	root := template.New("chart")
	root.Funcs(funcs(root))
	files, _ := filepath.Glob(filepath.Join(chartDir, "templates", "*"))
	sort.Strings(files)
	var manifests []string
	for _, f := range files {
		src, err := os.ReadFile(f)
		if err != nil {
			t.Fatal(err)
		}
		name := filepath.Base(f)
		if _, err := root.New(name).Parse(string(src)); err != nil {
			t.Fatalf("parse %s: %v", name, err)
		}
		manifests = append(manifests, name)
	}

	var docs []obj
	for _, name := range manifests {
		if strings.HasPrefix(name, "_") || name == "NOTES.txt" {
			continue
		}
		var b bytes.Buffer
		if err := root.ExecuteTemplate(&b, name, ctx); err != nil {
			return nil, fmt.Errorf("%s: %w", name, err)
		}
		dec := yaml.NewDecoder(&b)
		for {
			var d obj
			if err := dec.Decode(&d); err != nil {
				if errors.Is(err, io.EOF) {
					break
				}
				return nil, fmt.Errorf("%s: invalid YAML: %w\n%s", name, err, b.String())
			}
			if len(d) > 0 {
				docs = append(docs, d)
			}
		}
	}
	return docs, nil
}

func mustRender(t *testing.T, overrides obj) []obj {
	t.Helper()
	docs, err := render(t, overrides)
	if err != nil {
		t.Fatal(err)
	}
	return docs
}

// ---- accessors ----

func get(v any, path ...string) any {
	for _, p := range path {
		m, ok := v.(map[string]any) // obj is an alias of this type
		if !ok {
			return nil
		}
		v = m[p]
	}
	return v
}

func find(docs []obj, kind, nameSuffix string) obj {
	for _, d := range docs {
		if d["kind"] == kind && strings.HasSuffix(toStr(get(d, "metadata", "name")), nameSuffix) {
			return d
		}
	}
	return nil
}

func kinds(docs []obj) map[string]int {
	out := map[string]int{}
	for _, d := range docs {
		out[toStr(d["kind"])]++
	}
	return out
}

func containerOf(d obj) obj {
	cs, _ := get(d, "spec", "template", "spec", "containers").([]any)
	if len(cs) == 0 {
		return nil
	}
	c, _ := cs[0].(obj)
	return c
}

func envOf(c obj) map[string]string {
	out := map[string]string{}
	envs, _ := c["env"].([]any)
	for _, e := range envs {
		m := e.(obj)
		out[toStr(m["name"])] = toStr(m["value"])
	}
	return out
}

var base = obj{"database": obj{"existingSecret": "external-db"}}

// ---- tests ----

func TestRequiresADatabaseSource(t *testing.T) {
	_, err := render(t, obj{})
	if err == nil || !strings.Contains(err.Error(), "database.existingSecret") {
		t.Fatalf("rendering with no database configured must fail helpfully, got: %v", err)
	}
}

func TestDefaultRender(t *testing.T) {
	docs := mustRender(t, base)
	k := kinds(docs)
	want := map[string]int{
		"Deployment": 3, "Service": 3, "PodDisruptionBudget": 3, "HorizontalPodAutoscaler": 1,
		"Job": 1, "ServiceAccount": 1,
	}
	for kind, n := range want {
		if k[kind] != n {
			t.Errorf("%s: got %d want %d (all: %v)", kind, k[kind], n, k)
		}
	}
	for _, absent := range []string{"Secret", "ScaledObject", "NetworkPolicy", "ServiceMonitor", "StatefulSet", "Ingress"} {
		if k[absent] != 0 {
			t.Errorf("%s should not render by default", absent)
		}
	}
}

func TestEveryWorkloadIsHardened(t *testing.T) {
	docs := mustRender(t, obj{
		"database": obj{"existingSecret": "x"}, "keda": obj{"enabled": true},
		"postgresql": obj{"enabled": false},
	})
	n := 0
	for _, d := range docs {
		if d["kind"] != "Deployment" && d["kind"] != "Job" {
			continue
		}
		n++
		name := toStr(get(d, "metadata", "name"))
		c := containerOf(d)
		sc := get(c, "securityContext")
		for key, want := range map[string]any{"runAsNonRoot": true, "readOnlyRootFilesystem": true, "allowPrivilegeEscalation": false} {
			if get(sc, key) != want {
				t.Errorf("%s: securityContext.%s = %v, want %v", name, key, get(sc, key), want)
			}
		}
		drop, _ := get(sc, "capabilities", "drop").([]any)
		if len(drop) != 1 || drop[0] != "ALL" {
			t.Errorf("%s: must drop ALL capabilities, got %v", name, drop)
		}
		if get(c, "resources", "limits", "memory") == nil || get(c, "resources", "requests", "cpu") == nil {
			t.Errorf("%s: missing resource requests/limits", name)
		}
		if got := toStr(get(d, "spec", "template", "spec", "automountServiceAccountToken")); got != "false" {
			t.Errorf("%s: service account token must not be mounted (got %q)", name, got)
		}
		if img := toStr(c["image"]); !strings.HasSuffix(img, ":0.1.0") {
			t.Errorf("%s: image %q should default to the chart appVersion tag", name, img)
		}
	}
	if n != 4 {
		t.Fatalf("expected 3 deployments + 1 job, saw %d", n)
	}
}

func TestProbes(t *testing.T) {
	docs := mustRender(t, base)
	api := containerOf(find(docs, "Deployment", "-api"))
	if get(api, "livenessProbe", "httpGet", "path") != "/healthz" || get(api, "readinessProbe", "httpGet", "path") != "/readyz" {
		t.Errorf("api probes: liveness=%v readiness=%v (liveness must not depend on the DB; readiness must)",
			get(api, "livenessProbe", "httpGet", "path"), get(api, "readinessProbe", "httpGet", "path"))
	}
	for _, comp := range []string{"-worker", "-scheduler"} {
		c := containerOf(find(docs, "Deployment", comp))
		for _, probe := range []string{"livenessProbe", "readinessProbe"} {
			if get(c, probe, "httpGet", "path") != "/healthz" {
				t.Errorf("%s %s missing", comp, probe)
			}
		}
	}
}

// Completion batching is off unless asked for, and the switch reaches the worker as an env var.
func TestWorkerCompleteBatchToggle(t *testing.T) {
	docs := mustRender(t, base)
	if got := envOf(containerOf(find(docs, "Deployment", "-worker")))["WORKER_COMPLETE_BATCH"]; got != "false" {
		t.Errorf("default WORKER_COMPLETE_BATCH = %q, want false", got)
	}
	docs = mustRender(t, obj{"database": obj{"existingSecret": "x"}, "worker": obj{"completeBatch": true}})
	if got := envOf(containerOf(find(docs, "Deployment", "-worker")))["WORKER_COMPLETE_BATCH"]; got != "true" {
		t.Errorf("WORKER_COMPLETE_BATCH = %q, want true", got)
	}
}

// The shutdown contract: Kubernetes must give a worker longer than it takes to drain.
func TestWorkerTerminationGraceMatchesJobTimeout(t *testing.T) {
	for _, timeout := range []int{60, 300, 3600} {
		docs := mustRender(t, obj{"database": obj{"existingSecret": "x"}, "worker": obj{"maxJobTimeoutSeconds": timeout}})
		w := find(docs, "Deployment", "-worker")
		env := envOf(containerOf(w))
		grace := toInt(get(w, "spec", "template", "spec", "terminationGracePeriodSeconds"))
		if env["WORKER_SHUTDOWN_GRACE"] != fmt.Sprintf("%ds", timeout) || env["WORKER_DEFAULT_TIMEOUT"] != fmt.Sprintf("%ds", timeout) {
			t.Errorf("timeout %d: env grace=%q default_timeout=%q", timeout, env["WORKER_SHUTDOWN_GRACE"], env["WORKER_DEFAULT_TIMEOUT"])
		}
		if grace != timeout+15 {
			t.Errorf("timeout %d: terminationGracePeriodSeconds=%d, want %d", timeout, grace, timeout+15)
		}
	}
	// API: grace must cover the readiness-drain delay plus the shutdown timeout.
	docs := mustRender(t, base)
	api := find(docs, "Deployment", "-api")
	env := envOf(containerOf(api))
	if env["SHUTDOWN_DELAY"] != "5s" || env["SHUTDOWN_TIMEOUT"] != "20s" ||
		toInt(get(api, "spec", "template", "spec", "terminationGracePeriodSeconds")) != 30 {
		t.Errorf("api shutdown contract: %v grace=%v", env, get(api, "spec", "template", "spec", "terminationGracePeriodSeconds"))
	}
}

func TestMigrationJobIsAHook(t *testing.T) {
	docs := mustRender(t, base)
	job := find(docs, "Job", "-migrate")
	ann, _ := get(job, "metadata", "annotations").(obj)
	if ann["helm.sh/hook"] != "pre-install,pre-upgrade" {
		t.Errorf("migrate hook = %v", ann["helm.sh/hook"])
	}
	c := containerOf(job)
	args, _ := c["args"].([]any)
	if len(args) != 2 || args[0] != "migrate" || args[1] != "up" || !strings.Contains(toStr(c["image"]), "/jobctl:") {
		t.Errorf("migrate container: image=%v args=%v", c["image"], args)
	}
	if get(job, "spec", "template", "spec", "restartPolicy") != "Never" {
		t.Error("migration Job must not restart in place")
	}
	// Disabling it removes the Job.
	if kinds(mustRender(t, obj{"database": obj{"existingSecret": "x"}, "migrate": obj{"enabled": false}}))["Job"] != 0 {
		t.Error("migrate.enabled=false must remove the Job")
	}
}

func TestPodDisruptionBudgets(t *testing.T) {
	docs := mustRender(t, base)
	if get(find(docs, "PodDisruptionBudget", "-api"), "spec", "minAvailable") == nil ||
		get(find(docs, "PodDisruptionBudget", "-scheduler"), "spec", "minAvailable") == nil ||
		get(find(docs, "PodDisruptionBudget", "-worker"), "spec", "maxUnavailable") == nil {
		t.Error("missing PDB settings")
	}
}

func TestKedaOwnsWorkerReplicas(t *testing.T) {
	off := mustRender(t, base)
	if get(find(off, "Deployment", "-worker"), "spec", "replicas") == nil {
		t.Error("without KEDA the worker Deployment needs a replica count")
	}
	on := mustRender(t, obj{"database": obj{"existingSecret": "x"}, "keda": obj{"enabled": true, "queue": "reports", "jobsPerReplica": 50}})
	if get(find(on, "Deployment", "-worker"), "spec", "replicas") != nil {
		t.Error("with KEDA the Deployment must not set replicas (it would fight the autoscaler)")
	}
	so := find(on, "ScaledObject", "-worker")
	if so == nil {
		t.Fatal("ScaledObject missing")
	}
	spec := get(so, "spec").(obj)
	if toInt(spec["minReplicaCount"]) > toInt(spec["maxReplicaCount"]) {
		t.Error("min > max replicas")
	}
	trigger := spec["triggers"].([]any)[0].(obj)
	meta := trigger["metadata"].(obj)
	q := toStr(meta["query"])
	for _, must := range []string{"queue = 'reports'", "'pending','retrying'", "run_at <= now()"} {
		if !strings.Contains(q, must) {
			t.Errorf("KEDA query %q must contain %q (runnable backlog only)", q, must)
		}
	}
	if toStr(meta["targetQueryValue"]) != "50" || trigger["type"] != "postgresql" {
		t.Errorf("trigger: %v", trigger)
	}
	if get(spec, "scaleTargetRef", "name") != toStr(get(find(on, "Deployment", "-worker"), "metadata", "name")) {
		t.Error("ScaledObject must target the worker Deployment")
	}
	if find(on, "TriggerAuthentication", "-postgres") == nil {
		t.Error("TriggerAuthentication missing")
	}
	// cooldownPeriod only applies to scale-to-zero: with min replicas > 0 KEDA ignores it (and warns).
	if _, has := spec["cooldownPeriod"]; has {
		t.Error("cooldownPeriod must not be rendered when minReplicaCount > 0")
	}
	zero := mustRender(t, obj{"database": obj{"existingSecret": "x"}, "keda": obj{"enabled": true, "minReplicaCount": 0}})
	if toInt(get(find(zero, "ScaledObject", "-worker"), "spec", "cooldownPeriod")) != 300 {
		t.Error("scale-to-zero ScaledObject should carry the cooldownPeriod")
	}
	// Scale-down must be slower than scale-up so draining workers are not churned.
	beh := get(spec, "advanced", "horizontalPodAutoscalerConfig", "behavior").(obj)
	if toInt(get(beh, "scaleDown", "stabilizationWindowSeconds")) < 120 {
		t.Error("scale-down stabilization window should be generous")
	}
}

func TestNetworkPolicy(t *testing.T) {
	if _, err := render(t, obj{"database": obj{"existingSecret": "x"}, "networkPolicy": obj{"enabled": true}}); err == nil ||
		!strings.Contains(err.Error(), "networkPolicy.postgres") {
		t.Fatalf("enabling NetworkPolicy without a database target must fail: %v", err)
	}
	docs := mustRender(t, obj{
		"database":      obj{"existingSecret": "x"},
		"networkPolicy": obj{"enabled": true, "postgres": obj{"cidr": "10.20.0.0/16"}},
	})
	if kinds(docs)["NetworkPolicy"] != 4 {
		t.Fatalf("want 4 network policies, got %v", kinds(docs))
	}
	deny := find(docs, "NetworkPolicy", "-default-deny")
	if get(deny, "spec", "ingress") != nil || get(deny, "spec", "egress") != nil {
		t.Error("default-deny must declare no allow rules")
	}
	pt, _ := get(deny, "spec", "policyTypes").([]any)
	if len(pt) != 2 {
		t.Errorf("default-deny must cover Ingress and Egress, got %v", pt)
	}
	egress := find(docs, "NetworkPolicy", "-egress")
	b, _ := yaml.Marshal(egress)
	for _, want := range []string{"10.20.0.0/16", "port: 5432", "port: 53"} {
		if !strings.Contains(string(b), want) {
			t.Errorf("egress policy missing %q:\n%s", want, b)
		}
	}
	// In-cluster Postgres via pod selector instead of a CIDR.
	docs = mustRender(t, obj{
		"database":      obj{"existingSecret": "x"},
		"networkPolicy": obj{"enabled": true, "postgres": obj{"podSelector": obj{"app": "pg"}}},
	})
	b, _ = yaml.Marshal(find(docs, "NetworkPolicy", "-egress"))
	if !strings.Contains(string(b), "app: pg") {
		t.Errorf("podSelector egress not rendered:\n%s", b)
	}
}

func TestServiceMonitorsAndIngress(t *testing.T) {
	docs := mustRender(t, obj{
		"database":       obj{"existingSecret": "x"},
		"serviceMonitor": obj{"enabled": true, "labels": obj{"release": "prometheus"}},
		"api":            obj{"ingress": obj{"enabled": true, "className": "nginx", "host": "jobs.test"}},
	})
	if kinds(docs)["ServiceMonitor"] != 3 || kinds(docs)["Ingress"] != 1 {
		t.Fatalf("kinds: %v", kinds(docs))
	}
	sm := find(docs, "ServiceMonitor", "-worker")
	if get(sm, "metadata", "labels", "release") != "prometheus" {
		t.Error("ServiceMonitor must carry the operator's selector label")
	}
	if toStr(get(sm, "spec", "endpoints").([]any)[0].(obj)["port"]) != "metrics" {
		t.Error("worker ServiceMonitor must scrape the metrics port")
	}
	if toStr(get(find(docs, "ServiceMonitor", "-api"), "spec", "endpoints").([]any)[0].(obj)["port"]) != "http" {
		t.Error("api ServiceMonitor must scrape the http port")
	}
}

func TestDemoPostgresAndChartManagedSecret(t *testing.T) {
	docs := mustRender(t, obj{"postgresql": obj{"enabled": true, "password": "s3cret"}})
	if kinds(docs)["StatefulSet"] != 1 || kinds(docs)["Secret"] != 1 {
		t.Fatalf("kinds: %v", kinds(docs))
	}
	sec := find(docs, "Secret", "-db")
	url := toStr(get(sec, "stringData", "DATABASE_URL"))
	if !strings.Contains(url, "jobs:s3cret@") || !strings.Contains(url, "-postgres.jobs.svc:5432") {
		t.Errorf("DATABASE_URL = %q (the host must be namespace-qualified so KEDA, which runs in another namespace, can resolve it)", url)
	}
	ann, _ := get(sec, "metadata", "annotations").(obj)
	if ann["helm.sh/hook"] != "pre-install,pre-upgrade" {
		t.Error("the chart-managed secret must exist before the migration hook runs")
	}
	// With the in-cluster demo DB the migration is a plain per-revision Job, NOT a hook: a post-install
	// hook deadlocks `helm install --wait` (KEDA is not Ready until the table exists).
	job := find(docs, "Job", "-migrate-1")
	if job == nil {
		t.Fatalf("demo migrate Job should be named with the release revision; kinds: %v", kinds(docs))
	}
	if get(job, "metadata", "annotations", "helm.sh/hook") != nil {
		t.Errorf("demo migrate Job must not be a hook, got %v", get(job, "metadata", "annotations"))
	}
	// Explicit URL (no demo DB) also works.
	docs = mustRender(t, obj{"database": obj{"url": "postgres://u:p@h/db"}})
	if toStr(get(find(docs, "Secret", "-db"), "stringData", "DATABASE_URL")) != "postgres://u:p@h/db" {
		t.Error("explicit database.url not used")
	}
}

func TestOTLPEndpointIsOptional(t *testing.T) {
	has := func(docs []obj) bool {
		_, ok := envOf(containerOf(find(docs, "Deployment", "-worker")))["OTEL_EXPORTER_OTLP_ENDPOINT"]
		return ok
	}
	if has(mustRender(t, base)) {
		t.Error("tracing must be off by default")
	}
	if !has(mustRender(t, obj{"database": obj{"existingSecret": "x"}, "otlpEndpoint": "http://tempo:4318"})) {
		t.Error("otlpEndpoint not propagated")
	}
}

func TestSelectorsMatchPodLabels(t *testing.T) {
	docs := mustRender(t, base)
	for _, d := range docs {
		if d["kind"] != "Deployment" {
			continue
		}
		sel, _ := get(d, "spec", "selector", "matchLabels").(obj)
		lbl, _ := get(d, "spec", "template", "metadata", "labels").(obj)
		for k, v := range sel {
			if lbl[k] != v {
				t.Errorf("%v: selector %s=%v not in pod labels %v", get(d, "metadata", "name"), k, v, lbl)
			}
		}
	}
	// Service selectors must match some Deployment's pod labels.
	svc := find(docs, "Service", "-worker")
	sel, _ := get(svc, "spec", "selector").(obj)
	lbl, _ := get(find(docs, "Deployment", "-worker"), "spec", "template", "metadata", "labels").(obj)
	for k, v := range sel {
		if lbl[k] != v {
			t.Errorf("service selector %s=%v does not match worker pods", k, v)
		}
	}
}

// The pool is sized per component: the API (a durable commit per request) needs far more
// connections than a scheduler. A single global value silently capped API throughput.
func TestConnectionPoolIsSizedPerComponent(t *testing.T) {
	docs := mustRender(t, base)
	want := map[string]string{"-api": "40", "-worker": "20", "-scheduler": "5"}
	for suffix, n := range want {
		got := envOf(containerOf(find(docs, "Deployment", suffix)))["DB_MAX_CONNS"]
		if got != n {
			t.Errorf("%s DB_MAX_CONNS = %q, want %q", suffix, got, n)
		}
	}
	docs = mustRender(t, obj{"database": obj{"existingSecret": "x"}, "api": obj{"maxConns": 7}})
	if got := envOf(containerOf(find(docs, "Deployment", "-api")))["DB_MAX_CONNS"]; got != "7" {
		t.Errorf("api.maxConns override not applied: %q", got)
	}
}

// With the demo in-cluster Postgres, default-deny must be paired with an allow for the app (and KEDA's
// operator, which runs in another namespace), otherwise pods work until their first restart and then
// cannot open new database connections.
func TestDemoPostgresIsReachableUnderDefaultDeny(t *testing.T) {
	docs := mustRender(t, obj{
		"postgresql":    obj{"enabled": true},
		"keda":          obj{"enabled": true},
		"networkPolicy": obj{"enabled": true, "postgres": obj{"podSelector": obj{"app.kubernetes.io/component": "postgres"}}},
	})
	pol := find(docs, "NetworkPolicy", "-postgres-ingress")
	if pol == nil {
		t.Fatal("no ingress allow for the demo Postgres")
	}
	b, _ := yaml.Marshal(pol)
	for _, want := range []string{"app.kubernetes.io/component: postgres", "- api", "- worker", "- scheduler", "- migrate", "kubernetes.io/metadata.name: keda", "port: 5432"} {
		if !strings.Contains(string(b), want) {
			t.Errorf("postgres ingress policy missing %q:\n%s", want, b)
		}
	}
	// Without KEDA the keda namespace must not be opened up.
	docs = mustRender(t, obj{
		"postgresql":    obj{"enabled": true},
		"keda":          obj{"enabled": false},
		"networkPolicy": obj{"enabled": true, "postgres": obj{"podSelector": obj{"app.kubernetes.io/component": "postgres"}}},
	})
	b, _ = yaml.Marshal(find(docs, "NetworkPolicy", "-postgres-ingress"))
	if strings.Contains(string(b), "keda") {
		t.Errorf("keda namespace allowed although KEDA is disabled:\n%s", b)
	}
	// A real (external) database gets no such policy.
	docs = mustRender(t, obj{"database": obj{"existingSecret": "x"}, "networkPolicy": obj{"enabled": true, "postgres": obj{"cidr": "10.0.0.0/16"}}})
	if find(docs, "NetworkPolicy", "-postgres-ingress") != nil {
		t.Error("external database must not get an in-cluster ingress policy")
	}
}

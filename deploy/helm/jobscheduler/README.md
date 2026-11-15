# Helm chart: jobscheduler

API, workers, cron scheduler, and the pieces around them, as one chart.

```bash
helm install js deploy/helm/jobscheduler -n jobs --create-namespace \
  --set database.existingSecret=jobscheduler-db            # secret with key DATABASE_URL (recommended)
# local demo with an in-cluster Postgres (never in production):
helm install js deploy/helm/jobscheduler -n jobs --create-namespace --set postgresql.enabled=true
```

## What it creates

| Template | Resources |
|---|---|
| `api.yaml` | Deployment, Service, PodDisruptionBudget, HPA (CPU), optional Ingress |
| `worker.yaml` | Deployment, metrics Service, PodDisruptionBudget |
| `scheduler.yaml` | Deployment (2 replicas, spread across nodes), Service, PodDisruptionBudget |
| `migrate-job.yaml` | the migration Job (`jobctl migrate up`) |
| `keda.yaml` | KEDA `ScaledObject` + `TriggerAuthentication` scaling workers on runnable backlog |
| `networkpolicy.yaml` | default-deny plus explicit allows (egress to DNS and the database; ingress for the API and metrics) |
| `servicemonitor.yaml` | Prometheus Operator scraping (optional) |
| `secret.yaml`, `serviceaccount.yaml`, `postgres-demo.yaml` | demo secret, service account (no API token), optional demo database |

## Values that matter

| Value | Meaning |
|---|---|
| `worker.maxJobTimeoutSeconds` (300) | **drives the shutdown contract**: `WORKER_SHUTDOWN_GRACE` = this, `terminationGracePeriodSeconds` = this + 15, so Kubernetes never SIGKILLs a worker that is still draining a legitimate job |
| `api.maxConns` 40, `worker.maxConns` 20, `scheduler.maxConns` 5 | pool size per component; total must stay under Postgres `max_connections` |
| `keda.enabled`, `jobsPerReplica`, `min/maxReplicaCount` | autoscaling on backlog; scale-up doubles per 30 s, scale-down removes 2 pods per minute after 5 minutes |
| `networkPolicy.enabled`, `networkPolicy.postgres.cidr` or `.podSelector` | exactly one database target is required |
| `database.existingSecret` / `database.url` / `postgresql.enabled` | one of the three is required (the chart fails with a clear message otherwise) |

## Behaviour verified on a cluster (details: [docs/OPERATIONS.md](../../../docs/OPERATIONS.md#kubernetes-verified-behaviour-and-caveats))

- **Migration:** a pre-install/pre-upgrade **hook** Job for a real database; a plain per-revision Job with the demo database (a post-install hook would deadlock `helm install --wait` against KEDA).
- **Security:** non-root, read-only root filesystem, all capabilities dropped, seccomp `RuntimeDefault`, no service-account token.
- **Probes:** liveness never touches the database (a DB outage must not restart healthy pods); the API's readiness does.
- **Database host in the demo secret is namespace-qualified** so KEDA's operator, which runs in another namespace, can resolve it.

## Tests

[../../../test/helm](../../../test/helm/README.md) renders the chart in-process and asserts these properties; CI also runs `helm lint` and `kubeconform`; `make k8s-verify` exercises it on a real cluster.

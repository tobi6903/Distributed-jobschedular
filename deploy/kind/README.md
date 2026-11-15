# Running the chart on a local cluster (kind)

```bash
make docker-build IMAGE_TAG=dev REGISTRY=jobscheduler   # the four images (needs Docker + buildx)
make kind-up          # 3-node kind cluster, metrics-server, KEDA, then the chart (demo Postgres)
make k8s-verify       # autoscaling, crashes, rolling deploy, node drain, NetworkPolicy  (FULL=1 adds a 2.5 min test)
make argocd-verify    # Argo CD deploys the chart from git, heals drift, rolls out, rolls back
make kind-down        # delete the cluster
```

Needs `kind`, `kubectl`, `helm` and `docker` on the PATH. [values-kind.yaml](values-kind.yaml) loads images with `kind load`
(nothing is pulled), uses the demo in-cluster Postgres, and shortens KEDA timers; [kind-config.yaml](kind-config.yaml)
makes one control plane and two workers so node drains and PodDisruptionBudgets have something to act on.

What `make k8s-verify` proves (each line is a PASS/FAIL check):

1. KEDA scales workers from 2 to 8 on queue depth and the backlog drains.
2. Container crashes (immediate SIGKILL, like an OOM kill) lose no job, recovered jobs show `attempts > 1`, and cron has no duplicate or missed slot.
3. A rolling deploy lets 12 mid-flight 40-second jobs finish (none interrupted).
3b. With `FULL=1`: a job longer than the shutdown grace is released (attempt refunded) rather than lost.
4. `kubectl drain` respects the PodDisruptionBudgets (you will see `Cannot evict pod as it would violate the pod's disruption budget`, then a retry), keeps at least one API replica ready, and loses no work.
5. NetworkPolicies block other namespaces, allow the ingress namespace, and the app still opens **new** database connections under default-deny.

This runs in CI too: [.github/workflows/k8s.yml](../../.github/workflows/k8s.yml).

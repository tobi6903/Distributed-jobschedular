# test/helm

Renders the Helm chart **in-process** and asserts properties of the manifests. Helm itself is a build tool we do not want as a Go dependency, so [render_test.go](render_test.go) uses Go's `text/template` (the engine Helm uses) plus small stand-ins for the handful of Helm/Sprig functions the chart needs (`include`, `toYaml`, `nindent`, `default`, `dict`, `fail`, ...), then parses the output as YAML.

```bash
go test ./test/helm/        # fast: about a second, no cluster, no helm binary
```

## What is asserted

- Rendering with no database configured **fails with a helpful message**.
- Every workload is hardened: non-root, read-only filesystem, all capabilities dropped, no service-account token, requests and limits, image tag defaults to the chart `appVersion`.
- Probes: the API's liveness does not touch the database, its readiness does.
- **Shutdown contract**: `terminationGracePeriodSeconds` = `worker.maxJobTimeoutSeconds + 15` and `WORKER_SHUTDOWN_GRACE` = the timeout, for several values.
- The migration Job is a hook for a real database, and a plain per-revision Job for the demo database.
- Connection pools are sized per component.
- KEDA: it owns the replica count (the Deployment omits `replicas`), the trigger query counts **runnable** backlog only, `cooldownPeriod` is rendered only for scale-to-zero.
- NetworkPolicy: default-deny declares no allow rules, the database target is required, the demo Postgres gets an ingress allow for the app and (only when KEDA is on) KEDA's namespace.
- ServiceMonitors, Ingress, the demo database and its secret, optional OTLP, label selectors matching pod labels.

A mutation check (changing the grace-period arithmetic) was confirmed to fail these tests. They complement, and do not replace, `helm lint`, `kubeconform` and the real-cluster [scripts/k8s-verify.sh](../../scripts/k8s-verify.sh).

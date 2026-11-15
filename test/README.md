# test

Tests that do not belong to a single package: end-to-end, chaos, load, and the Helm chart.

| Directory | What it checks | How to run |
|---|---|---|
| [integration/](integration/README.md) | the real API, store and workers against Postgres: correctness at scale, retries, crash recovery, shutdown, tracing | `make test` |
| [chaos/](chaos/README.md) | real worker and scheduler **processes** are `kill -9`'d mid-run; nothing may be lost or double-fired | `make chaos` |
| [load/](load/README.md) | k6 script and a Go load generator | `make load-test` |
| [helm/](helm/README.md) | the Helm chart rendered in-process, asserting hardening, shutdown contract, KEDA, NetworkPolicy | `go test ./test/helm/` |

Package-level unit and integration tests live next to the code (`internal/*/*_test.go`). Container- and cluster-level checks are scripts: `make smoke`, `make chaos-docker`, `make k8s-verify`, `make argocd-verify` (see [../scripts](../scripts/README.md)).

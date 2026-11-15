# deploy

Everything needed to run the platform, from a laptop to Kubernetes. Each directory has its own README.

| Directory | What it is | Verified by |
|---|---|---|
| [docker/](docker/README.md) | the Dockerfile (one file, four distroless images) and the Compose stack | image build, Trivy, `make smoke`, `make chaos-docker` |
| [helm/jobscheduler/](helm/jobscheduler/README.md) | the Helm chart | `helm lint`, `kubeconform`, render tests, `make k8s-verify` on kind |
| [kind/](kind/README.md) | local Kubernetes cluster config and values | used by `make kind-up` |
| [argocd/](argocd/README.md) | GitOps: `AppProject`, `Application`, production values | `make argocd-verify` |
| [prometheus/](prometheus/README.md) | scrape config, alert rules, SLO recording rules, and their unit tests | `promtool check` and `promtool test rules` |
| [alertmanager/](alertmanager/README.md) | routing and inhibition | `amtool check-config` |
| [grafana/](grafana/README.md) | dashboards as code and provisioning | all 28 panels' queries execute against live data |
| [loki/](loki/README.md), [promtail/](promtail/README.md), [tempo/](tempo/README.md) | log and trace storage and shipping | logs and a cross-container trace verified in the Compose stack |

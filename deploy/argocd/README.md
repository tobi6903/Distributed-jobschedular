# deploy/argocd

GitOps deployment: Argo CD watches the repository and keeps the cluster equal to it. Rollback is `git revert`.

| File | |
|---|---|
| [project.yaml](project.yaml) | `AppProject`: only this repo as a source, only the `jobs` namespace, and a **whitelist of exactly the kinds the chart renders** (no cluster-scoped resources) |
| [application.yaml](application.yaml) | `Application`: automated sync with prune and self-heal, `CreateNamespace`, server-side apply, retries; `ignoreDifferences` on `/spec/replicas` so Argo CD does not fight the HPA and KEDA |
| [values-prod.yaml](values-prod.yaml) | production overrides; CI commits the new image tag here on every merge to `main` |

```bash
kubectl apply -f deploy/argocd/project.yaml -f deploy/argocd/application.yaml
```

## Flow

CI builds and Trivy-scans the images, pushes them to GHCR, and commits the new tag to `values-prod.yaml` (`[skip ci]`). Argo CD notices the commit and rolls it out. The chart's Helm hooks become Argo CD sync hooks: the migration Job runs as `PreSync`.

## Verified

`make argocd-verify` installs Argo CD on kind, serves a throwaway git repo inside the cluster, applies **these exact manifests** (only the repo URL rewritten) and checks: sync to Healthy, manual drift healed (an edited env var reverted, a deleted Service recreated), a commit rolled out, and a `git revert` rolled back. `PUBLISHED=1 make argocd-verify` does the same with the images CI pushed to GHCR (registry and sha tag read from `origin/main`) and asserts every app container runs, and was pulled by digest from, `ghcr.io`.

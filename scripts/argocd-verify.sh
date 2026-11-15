#!/usr/bin/env bash
# GitOps check with Argo CD on the kind cluster: it deploys the chart from git, heals manual drift,
# rolls out a commit, and rolls back on `git revert`. A throwaway git repo is served inside the cluster
# (git daemon), and the REAL deploy/argocd manifests are used with only the repo URL rewritten.
# Run after scripts/kind-up.sh (it replaces the helm-installed release so Argo CD owns the deployment).
source "$(dirname "$0")/lib-k8s.sh"
ARGO_VERSION="${ARGO_VERSION:-v3.6.1}"; D="${REL}-jobscheduler"; W=$(mktemp -d); G="git -c user.name=t -c user.email=t@t"
trap 'rm -rf "$W"' EXIT

echo "== install Argo CD core $ARGO_VERSION; hand the deployment over to it"
helm uninstall "$REL" --kube-context "$CTX" -n "$NS" >/dev/null 2>&1 || true
K delete pvc --all >/dev/null 2>&1 || true
kubectl --context "$CTX" create namespace argocd >/dev/null 2>&1 || true
kubectl --context "$CTX" apply -n argocd --server-side --force-conflicts \
  -f "https://raw.githubusercontent.com/argoproj/argo-cd/$ARGO_VERSION/manifests/core-install.yaml" >/dev/null
for r in deploy/argocd-repo-server deploy/argocd-applicationset-controller statefulset/argocd-application-controller; do
  kubectl --context "$CTX" -n argocd rollout status "$r" --timeout=300s >/dev/null; done

echo "== serve the chart from an in-cluster git repo"
mkdir -p "$W/src/deploy/helm" "$W/src/deploy/kind"
cp -r "$ROOT/deploy/helm/jobscheduler" "$W/src/deploy/helm/"; cp "$ROOT/deploy/kind/values-kind.yaml" "$W/src/deploy/kind/"
(cd "$W/src" && git init -q -b main && $G add -A && $G commit -qm "initial") && git clone -q --bare "$W/src" "$W/repo.git"
cat > "$W/Dockerfile" <<'DF'
FROM alpine:3.20
RUN apk add --no-cache git git-daemon
COPY repo.git /srv/repo.git
RUN git config --system --add safe.directory '*'
CMD ["git", "daemon", "--reuseaddr", "--export-all", "--base-path=/srv", "--listen=0.0.0.0", "--port=9418", "/srv"]
DF
docker build -q -t gitserver:dev "$W" >/dev/null; kind load docker-image gitserver:dev --name "$CLUSTER" >/dev/null
kubectl --context "$CTX" create namespace git >/dev/null 2>&1 || true
kubectl --context "$CTX" -n git delete pod gitserver --now >/dev/null 2>&1 || true
kubectl --context "$CTX" -n git apply -f - >/dev/null <<'Y'
apiVersion: v1
kind: Pod
metadata: {name: gitserver, labels: {app: gitserver}}
spec: {containers: [{name: git, image: gitserver:dev, imagePullPolicy: Never, ports: [{containerPort: 9418}]}]}
---
apiVersion: v1
kind: Service
metadata: {name: gitserver}
spec: {selector: {app: gitserver}, ports: [{port: 9418, targetPort: 9418}]}
Y
kubectl --context "$CTX" -n git wait --for=condition=Ready pod/gitserver --timeout=120s >/dev/null
URL=git://gitserver.git.svc:9418/repo.git
sed "s|https://github.com/tobi6903/Distributed-jobschedular.git|$URL|" "$ROOT/deploy/argocd/project.yaml" | kubectl --context "$CTX" apply -f - >/dev/null
sed -e "s|https://github.com/tobi6903/Distributed-jobschedular.git|$URL|" -e 's|../../argocd/values-prod.yaml|../../kind/values-kind.yaml|' \
  "$ROOT/deploy/argocd/application.yaml" | kubectl --context "$CTX" apply -f - >/dev/null

app() { kubectl --context "$CTX" -n argocd get app jobscheduler -o jsonpath="$1"; }
refresh() { kubectl --context "$CTX" -n argocd annotate app jobscheduler argocd.argoproj.io/refresh=hard --overwrite >/dev/null; }
push() { (cd "$W/src" && git push -q ../repo.git main); kubectl --context "$CTX" -n git exec gitserver -- rm -rf /srv/repo.git
         kubectl --context "$CTX" cp "$W/repo.git" git/gitserver:/srv/repo.git >/dev/null 2>&1; refresh; }
grace() { K get deploy "$D-worker" -o jsonpath='{.spec.template.spec.terminationGracePeriodSeconds}'; }
synced() { [ "$(app '{.status.sync.status}/{.status.health.status}')" = Synced/Healthy ]; }

echo "== Argo CD deploys the chart"
wait_for 90 synced || true
check "application is Synced and Healthy ($(app '{.status.operationState.message}'))" synced

echo "== manual drift is healed"
K set env "deploy/$D-api" LOG_LEVEL=debug >/dev/null; K delete service "$D-api" >/dev/null
wait_eq 40 info K get deploy "$D-api" -o jsonpath='{.spec.template.spec.containers[0].env[?(@.name=="LOG_LEVEL")].value}' || true
check "the hand-edited env var was reverted" test "$(K get deploy "$D-api" -o jsonpath='{.spec.template.spec.containers[0].env[?(@.name=="LOG_LEVEL")].value}')" = info
check "the deleted Service was recreated" wait_for 20 K get service "$D-api"

echo "== a commit rolls out, and git revert rolls it back"
before=$(grace)
sed -i 's/maxJobTimeoutSeconds: 60 .*/maxJobTimeoutSeconds: 90/' "$W/src/deploy/kind/values-kind.yaml"
(cd "$W/src" && $G commit -qam "raise max job timeout") && push
check "commit applied: terminationGracePeriodSeconds $before -> 105" wait_eq 60 105 grace
(cd "$W/src" && $G revert --no-edit HEAD >/dev/null 2>&1) && push
check "git revert applied: terminationGracePeriodSeconds back to $before" wait_eq 60 "$before" grace
check "application is Synced and Healthy again" wait_for 40 synced
[ "$FAILED" = 0 ] && echo "ARGOCD VERIFY PASSED" || { echo "ARGOCD VERIFY FAILED"; exit 1; }

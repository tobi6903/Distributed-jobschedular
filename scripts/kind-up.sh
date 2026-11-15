#!/usr/bin/env bash
# Create a kind cluster, load the locally built images, install metrics-server and KEDA, then the chart.
#   make docker-build IMAGE_TAG=dev REGISTRY=jobscheduler && ./scripts/kind-up.sh
source "$(dirname "$0")/lib-k8s.sh"
KEDA_VERSION="${KEDA_VERSION:-2.21.0}"; METRICS_SERVER_VERSION="${METRICS_SERVER_VERSION:-3.14.0}"

if ! kind get clusters 2>/dev/null | grep -qx "$CLUSTER"; then
  kind create cluster --name "$CLUSTER" --config "$ROOT/deploy/kind/kind-config.yaml" --wait 240s
fi
for app in api worker scheduler jobctl; do
  docker image inspect "jobscheduler/$app:dev" >/dev/null 2>&1 || { echo "missing image jobscheduler/$app:dev (run: make docker-build IMAGE_TAG=dev REGISTRY=jobscheduler)" >&2; exit 1; }
  kind load docker-image "jobscheduler/$app:dev" --name "$CLUSTER"
done
helm repo add kedacore https://kedacore.github.io/charts >/dev/null 2>&1 || true
helm repo add metrics-server https://kubernetes-sigs.github.io/metrics-server/ >/dev/null 2>&1 || true
helm repo update >/dev/null
helm upgrade --install metrics-server metrics-server/metrics-server --version "$METRICS_SERVER_VERSION" \
  --kube-context "$CTX" -n kube-system --set 'args={--kubelet-insecure-tls}' --wait --timeout 240s
helm upgrade --install keda kedacore/keda --version "$KEDA_VERSION" \
  --kube-context "$CTX" -n keda --create-namespace --wait --timeout 300s
kubectl --context "$CTX" create namespace "$NS" >/dev/null 2>&1 || true
helm upgrade --install "$REL" "$ROOT/deploy/helm/jobscheduler" --kube-context "$CTX" -n "$NS" \
  -f "$ROOT/deploy/kind/values-kind.yaml" --wait --timeout 420s
K get pods

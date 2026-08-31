#!/usr/bin/env bash
#
# Deploy nt-demo to Kubernetes: N worker pods + controller (web UI).
# The controller auto-discovers workers via the headless service, so the UI
# opens with the pod list prefilled.
#
# Usage:
#   ./k8s/deploy.sh [WORKERS] [NAMESPACE]
#   WORKERS=5 NAMESPACE=ci ./k8s/deploy.sh
#
set -euo pipefail

DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
WORKERS="${WORKERS:-${1:-3}}"
NS="${NAMESPACE:-${2:-loadtest}}"

echo "==> Deploying nt-demo to namespace '${NS}' with ${WORKERS} worker(s)"

kubectl create namespace "${NS}" --dry-run=client -o yaml | kubectl apply -f - >/dev/null

kubectl apply -n "${NS}" -f "${DIR}/worker-service.yaml"

sed "s/REPLICAS/${WORKERS}/" "${DIR}/worker-statefulset.yaml" \
  | kubectl apply -n "${NS}" -f -

sed "s/WORKERS_COUNT_VALUE/${WORKERS}/" "${DIR}/controller.yaml" \
  | kubectl apply -n "${NS}" -f -

echo "==> Waiting for workers to become ready"
kubectl -n "${NS}" rollout status sts/workers --timeout=180s

echo "==> Waiting for controller to become ready"
kubectl -n "${NS}" rollout status deploy/controller --timeout=120s

echo
echo "==> Deployed. Pods:"
kubectl -n "${NS}" get pods -o wide

echo
echo "Open the UI:"
echo "  kubectl -n ${NS} port-forward svc/controller 8080:8080"
echo "  -> http://localhost:8080"

echo
echo "Teardown:"
echo "  kubectl delete ns ${NS}"
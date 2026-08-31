#!/usr/bin/env bash
#
# Deploy nt-demo to Kubernetes as N identical leader-elected pods.
# Every pod runs a load worker; the elected leader additionally serves the web
# UI (auto-discovered via headless service, labeled role=leader).
#
# Usage:
#   ./k8s/deploy.sh [WORKERS] [NAMESPACE]
#   WORKERS=5 NAMESPACE=ci ./k8s/deploy.sh
#
set -euo pipefail

DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
WORKERS="${WORKERS:-${1:-3}}"
NS="${NAMESPACE:-${2:-loadtest}}"

echo "==> Deploying nt-demo to namespace '${NS}' with ${WORKERS} pod(s)"

kubectl create namespace "${NS}" --dry-run=client -o yaml | kubectl apply -f - >/dev/null

kubectl apply -n "${NS}" -f "${DIR}/rbac.yaml"
kubectl apply -n "${NS}" -f "${DIR}/service.yaml"

sed "s/REPLICAS/${WORKERS}/; s/WORKERS_COUNT_VALUE/${WORKERS}/" \
  "${DIR}/statefulset.yaml" | kubectl apply -n "${NS}" -f -

echo "==> Waiting for pods to become ready"
kubectl -n "${NS}" rollout status sts/worker --timeout=180s

LEADER=$(kubectl -n "${NS}" get lease nt-demo-leader \
  -o jsonpath='{.spec.holderIdentity}' 2>/dev/null || true)

echo
echo "==> Deployed. Pods:"
kubectl -n "${NS}" get pods -o wide
echo
echo "Leader: ${LEADER:-elected shortly}"

echo
echo "Open the UI (routes to the elected leader pod):"
echo "  kubectl -n ${NS} port-forward svc/controller 8080:8080"
echo "  -> http://localhost:8080"

echo
echo "Teardown:"
echo "  kubectl delete ns ${NS}"
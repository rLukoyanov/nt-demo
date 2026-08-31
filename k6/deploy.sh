#!/usr/bin/env bash
#
# Deploy a distributed k6 load test (k6-operator) with a step/peak profile:
# static BASE_RPS load with periodic PEAK_RPS spikes, repeated CYCLES times.
#
# Installs k6-operator (if missing), generates the k6 script from parameters,
# creates the ConfigMap + TestRun CR and waits for the run to finish.
#
# Usage:
#   ./k6/deploy.sh [--cleanup]
#
# Environment (defaults in brackets):
#   K6_NS         Kubernetes namespace                 [k6]
#   TARGET_URL    URL to load                          [http://target.default.svc.cluster.local:8080/]
#   BASE_RPS      static load level                    [300]
#   PEAK_RPS      peak load level                      [1500]
#   BASE_TIME     static phase duration                [10s]
#   PEAK_TIME     peak phase duration                  [8s]
#   CYCLES        number of base+peak cycles           [2]
#   PARALLELISM   number of k6 pods (distributed)      [3]
#
# Examples:
#   ./k6/deploy.sh
#   PEAK_RPS=1500 BASE_RPS=300 CYCLES=3 PARALLELISM=5 TARGET_URL=http://api:8080 ./k6/deploy.sh
#   ./k6/deploy.sh --cleanup          # remove testrun, configmap and operator
#
set -euo pipefail

DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
NS="${K6_NS:-k6}"
TARGET_URL="${TARGET_URL:-http://target.default.svc.cluster.local:8080/}"
BASE_RPS="${BASE_RPS:-300}"
PEAK_RPS="${PEAK_RPS:-1500}"
BASE_TIME="${BASE_TIME:-10s}"
PEAK_TIME="${PEAK_TIME:-8s}"
CYCLES="${CYCLES:-2}"
PARALLELISM="${PARALLELISM:-3}"

NAME="peaks-test"

for v in BASE_RPS PEAK_RPS CYCLES PARALLELISM; do
  case "${!v}" in
    ''|*[!0-9]*) echo "error: ${v} must be a number (got '${!v}')"; exit 2 ;;
  esac
done

if [[ "${1:-}" == "--cleanup" ]]; then
  echo "==> Removing testrun, configmap and operator"
  kubectl -n "${NS}" delete testrun "${NAME}" --ignore-not-found 2>/dev/null
  kubectl -n "${NS}" delete configmap k6-script --ignore-not-found 2>/dev/null
  helm uninstall k6-operator -n "${NS}" --ignore-not-found 2>/dev/null
  kubectl delete namespace "${NS}" --ignore-not-found 2>/dev/null
  echo "done."
  exit 0
fi

if ! command -v helm >/dev/null 2>&1; then
  echo "error: helm is required"; exit 1
fi

echo "==> k6-operator"
if ! kubectl get crd testruns.k6.io >/dev/null 2>&1; then
  helm repo add grafana https://grafana.github.io/helm-charts >/dev/null 2>&1 || true
  helm repo update >/dev/null 2>&1 || true
  helm install k6-operator grafana/k6-operator --namespace "${NS}" --create-namespace
  kubectl -n "${NS}" wait --for=condition=available deploy/k6-operator-controller-manager --timeout=180s
else
  echo "   already installed (testruns.k6.io CRD present)"
fi

echo "==> Generating script (base=${BASE_RPS} rps, peak=${PEAK_RPS} rps, ${CYCLES} cycles)"
SCRIPT="${DIR}/script.generated.js"
{
  echo "import http from 'k6/http';"
  echo "import { check } from 'k6';"
  echo ""
  echo "export const options = {"
  echo "  discardResponseBodies: true,"
  echo "  scenarios: {"
  echo "    peaks: {"
  echo "      executor: 'ramping-arrival-rate',"
  echo "      startRate: ${BASE_RPS},"
  echo "      timeUnit: '1s',"
  echo "      preAllocatedVUs: ${PEAK_RPS},"
  echo "      maxVUs: $((PEAK_RPS * 4)),"
  echo "      stages: ["
  for _ in $(seq 1 "${CYCLES}"); do
    echo "        { target: ${BASE_RPS}, duration: '${BASE_TIME}' },"
    echo "        { target: ${PEAK_RPS}, duration: '1s' },"
    echo "        { target: ${PEAK_RPS}, duration: '${PEAK_TIME}' },"
    echo "        { target: ${BASE_RPS}, duration: '1s' },"
  done
  echo "        { target: ${BASE_RPS}, duration: '${BASE_TIME}' },"
  echo "      ],"
  echo "    },"
  echo "  },"
  echo "};"
  echo ""
  echo "export default function () {"
  echo "  const res = http.get('${TARGET_URL}');"
  echo "  check(res, { 'status < 500': (r) => r.status < 500 });"
  echo "}"
} > "${SCRIPT}"

echo "==> Applying ConfigMap + TestRun (parallelism=${PARALLELISM})"
kubectl -n "${NS}" create configmap k6-script --from-file=script.js="${SCRIPT}" --dry-run=client -o yaml | kubectl apply -f -

cat > "${DIR}/peaks.generated.yaml" <<EOF
apiVersion: k6.io/v1alpha1
kind: TestRun
metadata:
  name: ${NAME}
  namespace: ${NS}
spec:
  parallelism: ${PARALLELISM}
  script:
    configMap:
      name: k6-script
      file: script.js
EOF
kubectl apply -f "${DIR}/peaks.generated.yaml"

echo "==> Test started. Waiting for runner pods..."
# wait until all runner pods are Running (operator starts them with a delay)
pods_seen=0
for _ in $(seq 1 30); do
  n=$(kubectl -n "${NS}" get pods -l runner=true --field-selector=status.phase=Running 2>/dev/null | tail -n +2 | wc -l | tr -d ' ')
  if [ "${n}" -ge "${PARALLELISM}" ]; then
    pods_seen=1
    break
  fi
  sleep 2
done
if [ "${pods_seen}" != "1" ]; then
  echo "error: runner pods did not appear"; exit 1
fi

echo "==> Test running. Waiting for it to finish..."
# poll the TestRun stage until finished (avoids races with pod lifecycle)
for _ in $(seq 1 120); do
  stage=$(kubectl -n "${NS}" get testruns "${NAME}" -o jsonpath='{.status.stage}' 2>/dev/null || echo gone)
  if [ "${stage}" = "finished" ] || [ "${stage}" = "gone" ]; then
    break
  fi
  sleep 5
done

echo
echo "==> Summary (per runner pod):"
for pod in $(kubectl -n "${NS}" get pods -l runner=true -o name 2>/dev/null); do
  echo "--- ${pod#pod/} ---"
  kubectl -n "${NS}" logs "${pod#pod/}" 2>/dev/null \
    | grep -E "http_reqs|checks_total|checks_failed|http_req_duration|http_req_failed|iterations" \
    | head -10
done

echo
echo "Generated files: ${SCRIPT} and ${DIR}/peaks.generated.yaml"
echo "To clean up: ${BASH_SOURCE[0]} --cleanup"
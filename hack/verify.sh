#!/usr/bin/env bash
# M1 exit checks (DATA-698). Requires `make forward` to be running.
set -uo pipefail

PROFILE="${PROFILE:-pit}"
NS="${NAMESPACE:-pit-poc}"
RELEASE="${RELEASE:-pit}"
KC=(kubectl --context "$PROFILE" -n "$NS")
fails=0

check() { # label, expected, actual
  if [[ "$3" == *"$2"* ]]; then
    printf '  \033[32mPASS\033[0m  %-38s %s\n' "$1" "$3"
  else
    printf '  \033[31mFAIL\033[0m  %-38s got: %s (want: %s)\n' "$1" "$3" "$2"
    fails=$((fails + 1))
  fi
}

echo "== cluster =="
check "minikube profile present" "$PROFILE" "$(kubectl config get-contexts -o name | grep -Fx "$PROFILE" || echo MISSING)"
check "namespace exists"          "$NS"      "$("${KC[@]}" get ns "$NS" -o name 2>/dev/null || echo MISSING)"
check "broker replicas"           "1/1"      "$("${KC[@]}" get sts "$RELEASE-redpanda" -o jsonpath='{.status.readyReplicas}/{.spec.replicas}' 2>/dev/null || echo MISSING)"

echo "== rendered manifest invariants =="
r=$(helm template "$RELEASE" charts/pit -n "$NS" -f charts/pit/values-local.yaml 2>/dev/null)
check "no cert-manager Certificates" "0" "$(grep -c 'kind: Certificate' <<< "$r")"
check "no NodePort services"         "0" "$(grep -c 'type: NodePort'    <<< "$r")"
check "schema registry listener on"  "8081" "$(grep -A4 'schema_registry_api:' <<< "$r" | grep -oE 'port: 8081' | head -1 || echo MISSING)"

echo "== endpoints (needs 'make forward') =="
check "schema registry /subjects" "[]"    "$(curl -fsS --max-time 5 localhost:8081/subjects 2>/dev/null || echo UNREACHABLE)"
check "admin API ready"           "ready" "$(curl -fsS --max-time 5 localhost:9644/v1/status/ready 2>/dev/null || echo UNREACHABLE)"
check "console responds"          "200"   "$(curl -fsS -o /dev/null -w '%{http_code}' --max-time 5 localhost:8080 2>/dev/null || echo UNREACHABLE)"

echo "== in-cluster rpk (no local install needed) =="
check "cluster healthy" "true" "$("${KC[@]}" exec -q sts/"$RELEASE-redpanda" -c redpanda -- \
  rpk cluster health 2>/dev/null | grep -oE 'Healthy:\s+\w+' | grep -oE '(true|false)' || echo MISSING)"

echo "== pending later milestones =="
printf '  SKIP  %-38s %s\n' "Kafka Connect /connectors" "lands in M3 (DATA-704/705/706)"
printf '  SKIP  %-38s %s\n' "sink Postgres"             "lands in M5 (DATA-713)"
printf '  NOTE  %-38s %s\n' "source Postgres / clinic schema" "covered by 'make verify' and 'make verify-schema'"

echo
if [[ $fails -eq 0 ]]; then
  echo "M1 exit checks passed."
else
  echo "$fails check(s) failed."
  exit 1
fi

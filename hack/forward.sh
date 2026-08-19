#!/usr/bin/env bash
# Background port-forwards for the PIT PoC.
#
# Several concurrent `kubectl port-forward` processes need backgrounding, a PID
# file and an idempotent stop, which is more than a Make recipe should carry.
#
# Targets that do not exist yet (Connect lands in M3, the Postgres pair in
# M2/M5) are skipped with a note rather than failing the whole command.
set -uo pipefail

PROFILE="${PROFILE:-pit}"
NS="${NAMESPACE:-pit-poc}"
RELEASE="${RELEASE:-pit}"
PIDFILE="${TMPDIR:-/tmp}/pit-forward.pids"
KC=(kubectl --context "$PROFILE" -n "$NS")

# target|local:remote|label. Ports match the README access table.
FORWARDS=(
  "svc/${RELEASE}-console|8080:8080|Redpanda Console"
  "svc/${RELEASE}-redpanda|8081:8081|Schema Registry"
  "svc/${RELEASE}-redpanda|9644:9644|Admin API"
  "svc/${RELEASE}-connect|8083:8083|Kafka Connect (M3)"
  "svc/${RELEASE}-source-pg|5432:5432|source Postgres"
  "svc/${RELEASE}-sink-pg|5433:5432|sink Postgres (M5)"
)

stop() {
  if [[ -f "$PIDFILE" ]]; then
    while read -r pid; do
      [[ -n "$pid" ]] && kill "$pid" 2>/dev/null && echo "    stopped pid $pid"
    done < "$PIDFILE"
    rm -f "$PIDFILE"
  fi
  # Catch strays from a previous shell that lost its PID file.
  pkill -f "port-forward.*--context ${PROFILE}" 2>/dev/null
  echo "==> port-forwards stopped"
}

start() {
  stop >/dev/null 2>&1
  : > "$PIDFILE"
  echo "==> starting port-forwards (profile '$PROFILE', namespace '$NS')"
  for entry in "${FORWARDS[@]}"; do
    IFS='|' read -r target ports label <<< "$entry"
    if ! "${KC[@]}" get "${target%%|*}" >/dev/null 2>&1; then
      printf '    skip  %-22s %-14s not deployed yet\n' "$label" "$ports"
      continue
    fi
    "${KC[@]}" port-forward "$target" "$ports" >/dev/null 2>&1 &
    echo $! >> "$PIDFILE"
    printf '    ok    %-22s localhost:%s\n' "$label" "${ports%%:*}"
  done
  echo "==> 'make forward-stop' to tear down. 'make verify' to check them."
}

case "${1:-start}" in
  start) start ;;
  stop)  stop  ;;
  *)     echo "usage: $0 {start|stop}"; exit 2 ;;
esac

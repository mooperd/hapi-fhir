#!/usr/bin/env bash
# Created by claude-opus-5
#
# Port-forward one HAPI stack to fixed local ports. That is all this does.
#
# The operator runs in a container now -- see ./operator.sh and
# docker-compose.yml. This script reports whether it is reconciling but does
# not start, stop or switch it. Mixing the two is what made this unreadable.
set -uo pipefail

HERE="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
DOWNLOADER="${DOWNLOADER:-$HOME/Documents/GitHub/mimic-fhir-downloader}"
KUBECONFIG="${KUBECONFIG:-$DOWNLOADER/behemoth-andrew-test.yaml}"
export KUBECONFIG

PIDFILE=/tmp/fhir-port-forward.pids
CURRENT=/tmp/fhir-port-forward.ns
LOG=/tmp/fhir-port-forward.log

UI=8085
LIVENESS=8086

# service  local  remote
SERVICES=(
  "hapi-fhir     8083  8080"
  "hapi-fhir-db  5433  5432"
  "hapi-fhir-es  9201  9200"
)

# --------------------------------------------------------------------------

list_stacks() {
  kubectl get fhirstacks --all-namespaces \
    -o jsonpath='{range .items[*]}{.metadata.namespace}{"\n"}{end}' 2>/dev/null \
  | sort -u | grep . && return

  kubectl get deploy --all-namespaces --field-selector metadata.name=hapi-fhir \
    -o jsonpath='{range .items[*]}{.metadata.namespace}{"\n"}{end}' 2>/dev/null \
  | sort -u
}

_forward() {
  local ns=$1 svc=$2 lp=$3 rp=$4
  while true; do
    echo "[$ns/$svc] localhost:$lp -> $svc:$rp"
    kubectl -n "$ns" port-forward "svc/$svc" "$lp:$rp"
    echo "[$ns/$svc] disconnected, retrying in 2s"
    sleep 2
  done
}

kill_all() {
  if [ -f "$PIDFILE" ]; then
    while read -r pid; do
      [ -n "$pid" ] || continue
      pkill -P "$pid" 2>/dev/null
      kill "$pid" 2>/dev/null
    done < "$PIDFILE"
    rm -f "$PIDFILE"
  fi
  pkill -f "kubectl.*port-forward svc/hapi-fhir" 2>/dev/null
  rm -f "$CURRENT"
  sleep 0.3
}

start_forwards() {
  local ns=$1
  kill_all
  for spec in "${SERVICES[@]}"; do
    # shellcheck disable=SC2086
    set -- $spec
    # Redirected here rather than inside _forward: a background subshell that
    # still holds the script's stdout keeps the terminal open after quit.
    _forward "$ns" "$1" "$2" "$3" >>"$LOG" 2>&1 &
    echo $! >>"$PIDFILE"
  done
  echo "$ns" >"$CURRENT"
  sleep 1
}

_port_state() {
  nc -z 127.0.0.1 "$1" 2>/dev/null && echo "up" || echo "down"
}

status() {
  local ns="none" health
  [ -f "$CURRENT" ] && ns=$(cat "$CURRENT")
  # kopf's own liveness, not the Flask UI. The UI is a thread in the same
  # process and answers even when the reconciler is wedged.
  if curl -sf --max-time 3 "http://localhost:$LIVENESS/healthz" >/dev/null 2>&1; then
    health="LIVE"
  else
    health="NOT LIVE   start it with ./operator.sh up"
  fi
  printf "  %-11s %s\n" "operator" "$health"
  printf "  %-11s %-34s %s\n" "UI" "http://localhost:$UI" "$(_port_state "$UI")"
  echo
  printf "  %-11s %s\n" "stack" "$ns"
  printf "    %-9s %-34s %s\n" "hapi" "http://localhost:8083/fhir" "$(_port_state 8083)"
  printf "    %-9s %-34s %s\n" "postgres" "psql -h localhost -p 5433 -U fhir" "$(_port_state 5433)"
  printf "    %-9s %-34s %s\n" "elastic" "http://localhost:9201" "$(_port_state 9201)"
  echo
  printf "  %-11s %s\n" "log" "$LOG"
}

press_enter() {
  echo
  printf "  enter to continue> "
  read -r _
}

trap 'echo; echo "menu exited; forwards left running"; exit 0' INT

# Numbers are stacks, letters are commands. Nothing else shares the list.
while true; do
  clear 2>/dev/null || printf '\033[2J\033[H'
  current_ns="none"
  [ -f "$CURRENT" ] && current_ns=$(cat "$CURRENT")

  echo "════ HAPI stack port-forward ════"
  echo
  status
  echo
  echo "  ── FORWARD TO STACK ───────────────────────────────────"
  stacks=()
  while read -r ns; do
    [ -n "$ns" ] && stacks+=("$ns")
  done < <(list_stacks)

  if [ ${#stacks[@]} -eq 0 ]; then
    echo "     (no FhirStacks and no hapi-fhir deployments found)"
  else
    i=1
    for ns in "${stacks[@]}"; do
      if [ "$ns" = "$current_ns" ]; then
        printf "   %d) %-28s  ← forwarding now\n" "$i" "$ns"
      else
        printf "   %d) %s\n" "$i" "$ns"
      fi
      i=$((i + 1))
    done
  fi

  echo
  echo "  ── OTHER ──────────────────────────────────────────────"
  echo "   o) operator status"
  echo "   k) kill the forwards"
  echo "   r) refresh"
  echo "   q) quit, leave forwards running"
  echo "   Q) quit, kill the forwards"
  echo
  printf "  choose> "
  read -r choice || exit 0

  case "$choice" in
    ''|r|R)  ;;
    o|O)     "$HERE/operator.sh" status; press_enter ;;
    k|K)     kill_all; echo "  forwards killed."; sleep 1 ;;
    q)       exit 0 ;;
    Q)       kill_all; exit 0 ;;
    *[!0-9]*)
             echo "  '$choice' is not an option."; sleep 1 ;;
    *)       if [ "$choice" -ge 1 ] 2>/dev/null && [ "$choice" -le ${#stacks[@]} ]; then
               start_forwards "${stacks[$((choice - 1))]}"
             else
               echo "  no stack numbered $choice."; sleep 1
             fi ;;
  esac
done

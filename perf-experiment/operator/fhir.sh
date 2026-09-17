#!/usr/bin/env bash
# Created by claude-opus-5
#
# One operator, one stack, two modes.
#
#   cluster  the operator runs in the cluster; :8085 is a port-forward to it
#   dev      the operator runs in docker compose here; :8085 is the container
#
# The modes are exclusive and the script keeps them that way: both want
# localhost:8085, and two operators reconciling the same FhirStack is the bug
# this exists to prevent. Switching mode always turns the other one off first.
#
# The stack forwards (hapi, postgres, elastic) run in both modes and are
# independent of it -- changing mode does not change which namespace you are
# pointed at, and vice versa.
set -uo pipefail

HERE="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
DOWNLOADER="${DOWNLOADER:-$HOME/Documents/GitHub/mimic-fhir-downloader}"
KUBECONFIG="${KUBECONFIG:-$DOWNLOADER/behemoth-andrew-test.yaml}"
export KUBECONFIG KUBECONFIG_PATH="$KUBECONFIG" MANIFEST_DIR="$DOWNLOADER"

OPNS=fhir-operator
UI=8085
PIDS=/tmp/fhir.pids
NSFILE=/tmp/fhir.ns
LOG=/tmp/fhir.log

# service      local remote
SERVICES=(
  "hapi-fhir     8083 8080"
  "hapi-fhir-db  5433 5432"
  "hapi-fhir-es  9201 9200"
)

compose()  { docker compose -f "$HERE/docker-compose.yml" "$@"; }
port()     { nc -z 127.0.0.1 "$1" 2>/dev/null && echo up || echo down; }
stack_ns() { [ -f "$NSFILE" ] && cat "$NSFILE" || echo none; }

# Derived from the container, never stored: a mode file can go stale, the
# container cannot.
mode() { [ -n "$(compose ps --status running -q 2>/dev/null)" ] && echo dev || echo cluster; }

# /healthz on the UI port. It answers in both modes -- in dev the container
# serves it, in cluster the forward does -- so one probe covers both. It means
# reachable, not healthy: the UI is a thread in the operator process and will
# answer even if the reconciler itself is wedged.
health() { curl -sf --max-time 3 "http://localhost:$UI/healthz" >/dev/null 2>&1; }

# One supervised forward. Forwards drop when a pod restarts; this brings them
# back. Output goes to the log so a background subshell does not hold the
# terminal open after quit.
forward() {
  local ns=$1 svc=$2 lp=$3 rp=$4
  while true; do
    echo "[$ns/$svc] localhost:$lp -> $svc:$rp"
    kubectl -n "$ns" port-forward "svc/$svc" "$lp:$rp"
    echo "[$ns/$svc] dropped, retrying in 2s"
    sleep 2
  done >>"$LOG" 2>&1 &
  echo $! >>"$PIDS"
}

kill_forwards() {
  if [ -f "$PIDS" ]; then
    while read -r p; do
      [ -n "$p" ] && { pkill -P "$p"; kill "$p"; } 2>/dev/null
    done <"$PIDS"
    rm -f "$PIDS"
  fi
  pkill -f "port-forward svc/(hapi-fhir|fhir-operator)" 2>/dev/null
  sleep 0.3
}

# The single place forwards are started. Everything else changes state, then
# calls this, so what is running always matches mode() and stack_ns().
apply() {
  local ns; ns=$(stack_ns)
  kill_forwards
  if [ "$ns" != none ]; then
    for spec in "${SERVICES[@]}"; do
      # shellcheck disable=SC2086
      set -- $spec
      forward "$ns" "$1" "$2" "$3"
    done
  fi
  [ "$(mode)" = cluster ] && forward "$OPNS" fhir-operator "$UI" "$UI"
  sleep 1
}

go_cluster() {
  compose down                                            # frees :8085
  kubectl -n "$OPNS" scale deploy/fhir-operator --replicas=1
  kubectl -n "$OPNS" rollout status deploy/fhir-operator --timeout=180s
  apply
}

go_dev() {
  # Checked before anything is scaled down: if the container cannot start, the
  # cluster copy must still be the one running, not nothing at all.
  [ -S "$HOME/.orbstack/run/docker.sock" ] || [ -S /var/run/docker.sock ] || {
    echo "  docker is not running; start OrbStack first"; sleep 2; return; }
  kill_forwards                                           # frees :8085
  kubectl -n "$OPNS" scale deploy/fhir-operator --replicas=0
  compose up -d --build
  apply
}

status() {
  local m; m=$(mode)
  printf "  %-10s %-9s %-34s %s\n" operator "$m" "http://localhost:$UI" \
    "$(health && echo LIVE || echo "NOT LIVE")"
  echo
  printf "  %-10s %s\n" stack "$(stack_ns)"
  printf "    %-8s %-34s %s\n" hapi     "http://localhost:8083/fhir"       "$(port 8083)"
  printf "    %-8s %-34s %s\n" postgres "psql -h localhost -p 5433 -U fhir" "$(port 5433)"
  printf "    %-8s %-34s %s\n" elastic  "http://localhost:9201"            "$(port 9201)"
  echo
  printf "  %-10s %s\n" log "$LOG"
}

here() { [ "$1" = "$2" ] && printf '   <--' || printf ''; }

trap 'echo; echo "menu exited; forwards left running"; exit 0' INT

# Letters are commands, numbers are namespaces. Nothing else shares the list.
while true; do
  clear 2>/dev/null || printf '\033[2J\033[H'
  m=$(mode); ns=$(stack_ns)

  echo "==== HAPI operator ===="
  echo
  status
  echo
  echo "  -- OPERATOR MODE --------------------------------------"
  printf "   c) cluster   operator in the cluster, container off%s\n"  "$(here "$m" cluster)"
  printf "   d) dev       operator in the container, cluster at 0%s\n" "$(here "$m" dev)"
  echo
  echo "  -- STACK NAMESPACE ------------------------------------"
  stacks=()
  while read -r s; do [ -n "$s" ] && stacks+=("$s"); done < <(
    kubectl get fhirstacks --all-namespaces \
      -o jsonpath='{range .items[*]}{.metadata.namespace}{"\n"}{end}' 2>/dev/null | sort -u)
  if [ ${#stacks[@]} -eq 0 ]; then
    echo "   (no FhirStacks found)"
  else
    i=1
    for s in "${stacks[@]}"; do
      printf "   %d) %s%s\n" "$i" "$s" "$(here "$s" "$ns")"
      i=$((i + 1))
    done
  fi
  echo
  echo "  -- OTHER ----------------------------------------------"
  echo "   l) operator logs"
  echo "   k) kill the forwards"
  echo "   r) refresh"
  echo "   q) quit, leave forwards running"
  echo "   Q) quit, kill the forwards"
  echo
  printf "  choose> "
  read -r choice || exit 0

  case "$choice" in
    ''|r|R) ;;
    c|C)    go_cluster ;;
    d|D)    go_dev ;;
    l|L)    if [ "$m" = dev ]; then compose logs -f
            else kubectl -n "$OPNS" logs -f deploy/fhir-operator; fi ;;
    k|K)    kill_forwards; echo "  forwards killed."; sleep 1 ;;
    q)      exit 0 ;;
    Q)      kill_forwards; exit 0 ;;
    *[!0-9]*) echo "  '$choice' is not an option."; sleep 1 ;;
    *)      if [ "$choice" -ge 1 ] 2>/dev/null && [ "$choice" -le ${#stacks[@]} ]; then
              echo "${stacks[$((choice - 1))]}" >"$NSFILE"
              apply
            else
              echo "  no namespace numbered $choice."; sleep 1
            fi ;;
  esac
done

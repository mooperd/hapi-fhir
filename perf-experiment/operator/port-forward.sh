#!/usr/bin/env bash
# Created by claude-opus-5
#
# Pick a stack from a menu, port-forward it, and choose where the operator
# runs. One stack at a time, fixed local ports. Deliberately dumb.
#
# The operator UI is always on http://localhost:8085 whichever mode you pick:
#   local    kopf runs here, in a venv, and binds 8085 directly
#   cluster  kopf runs in the pod, and 8085 is port-forwarded to it
# Switching modes scales the other one down. Two operators watching the same
# namespaces would both reconcile every FhirStack.
set -uo pipefail

HERE="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
DOWNLOADER="${DOWNLOADER:-$HOME/Documents/GitHub/mimic-fhir-downloader}"
KUBECONFIG="${KUBECONFIG:-$DOWNLOADER/behemoth-andrew-test.yaml}"
MANIFEST="${MANIFEST:-$DOWNLOADER/hapi-fhir-standalone.yaml}"
export KUBECONFIG

PIDFILE=/tmp/fhir-port-forward.pids       # per-stack forwards, torn down on switch
UIPIDFILE=/tmp/fhir-port-forward.ui.pid   # cluster-mode UI forward
OPPIDFILE=/tmp/fhir-operator.pid          # local-mode kopf process
MODEFILE=/tmp/fhir-operator.mode
CURRENT=/tmp/fhir-port-forward.ns
LOG=/tmp/fhir-port-forward.log
OPLOG=/tmp/fhir-operator.log

VENV="$HERE/.venv"
PYTHON="${PYTHON:-python3}"

# service  local  remote
SERVICES=(
  "hapi-fhir     8083  8080"
  "hapi-fhir-db  5433  5432"
  "hapi-fhir-es  9201  9200"
)

UI_NAMESPACE="${UI_NAMESPACE:-fhir-operator}"
UI_SERVICE=fhir-operator
UI_LOCAL=8085
UI_REMOTE=8090

# --------------------------------------------------------------------------
# Stacks
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
    echo "[$ns/$svc] localhost:$lp -> $svc:$rp" >>"$LOG"
    kubectl -n "$ns" port-forward "svc/$svc" "$lp:$rp" >>"$LOG" 2>&1
    echo "[$ns/$svc] disconnected, retrying in 2s" >>"$LOG"
    sleep 2
  done
}

_stop() {
  local pidfile=$1
  [ -f "$pidfile" ] || return 0
  while read -r pid; do
    [ -n "$pid" ] || continue
    pkill -P "$pid" 2>/dev/null          # the child
    kill "$pid" 2>/dev/null              # the wrapper
  done < "$pidfile"
  rm -f "$pidfile"
}

kill_all() {
  # Stack forwards only. The operator UI is how you drive the operator, so
  # killing it is never what you meant.
  _stop "$PIDFILE"
  pkill -f "kubectl.*port-forward svc/hapi-fhir-db" 2>/dev/null
  pkill -f "kubectl.*port-forward svc/hapi-fhir-es" 2>/dev/null
  pkill -f "kubectl.*port-forward svc/hapi-fhir " 2>/dev/null
  rm -f "$CURRENT"
  sleep 0.3
}

start_forwards() {
  local ns=$1
  kill_all
  for spec in "${SERVICES[@]}"; do
    # shellcheck disable=SC2086
    set -- $spec
    _forward "$ns" "$1" "$2" "$3" &
    echo $! >>"$PIDFILE"
  done
  echo "$ns" >"$CURRENT"
  sleep 1
}

# --------------------------------------------------------------------------
# Operator: local or in-cluster. Both end up on $UI_LOCAL.
# --------------------------------------------------------------------------

mode() { [ -f "$MODEFILE" ] && cat "$MODEFILE" || echo unknown; }

local_running() {
  [ -f "$OPPIDFILE" ] && kill -0 "$(cat "$OPPIDFILE" 2>/dev/null)" 2>/dev/null
}

setup_env() {
  if [ ! -x "$VENV/bin/kopf" ]; then
    echo "==> creating venv at $VENV"
    "$PYTHON" -m venv "$VENV" || return 1
    "$VENV/bin/pip" install --quiet --upgrade pip
  fi
  echo "==> installing requirements"
  "$VENV/bin/pip" install --quiet -r "$HERE/requirements.txt" || return 1
}

_wait_port_free() {
  for _ in $(seq 1 "${2:-20}"); do
    nc -z 127.0.0.1 "$1" 2>/dev/null || return 0
    sleep 0.5
  done
  return 1
}

ui_forward_up() {
  if [ -f "$UIPIDFILE" ] && kill -0 "$(cat "$UIPIDFILE" 2>/dev/null)" 2>/dev/null; then
    return 0
  fi
  rm -f "$UIPIDFILE"
  # A local operator that is still shutting down may hold the port. Waiting
  # beats binding nothing and leaving 8085 pointed at a dying process.
  if ! _wait_port_free "$UI_LOCAL"; then
    return 0        # still busy: an older forward has it, leave it alone
  fi
  _forward "$UI_NAMESPACE" "$UI_SERVICE" "$UI_LOCAL" "$UI_REMOTE" &
  echo $! >"$UIPIDFILE"
}

ui_forward_down() {
  _stop "$UIPIDFILE"
  pkill -f "kubectl.*port-forward svc/$UI_SERVICE" 2>/dev/null
}

local_stop() {
  local pid=""
  [ -f "$OPPIDFILE" ] && pid=$(cat "$OPPIDFILE" 2>/dev/null)
  rm -f "$OPPIDFILE"
  # Orphans from an earlier run. Matched on the venv path so this cannot
  # match a shell that merely mentions kopf.
  pkill -f "$VENV/bin/kopf run" 2>/dev/null
  [ -n "$pid" ] && kill "$pid" 2>/dev/null

  # kopf exits gracefully and takes a few seconds. Returning before it has
  # released :8085 leaves the port-forward unable to bind it.
  if [ -n "$pid" ]; then
    for _ in $(seq 1 20); do
      kill -0 "$pid" 2>/dev/null || break
      sleep 0.5
    done
    kill -9 "$pid" 2>/dev/null
  fi
  _wait_port_free "$UI_LOCAL" 20
}

operator_local() {
  echo "==> switching to local"
  ui_forward_down                     # 8085 must be free for kopf to bind it
  local_stop
  echo "==> scaling the in-cluster operator to 0"
  kubectl -n "$UI_NAMESPACE" scale deploy/"$UI_SERVICE" --replicas=0 2>&1 | sed 's/^/    /'
  setup_env || { echo "    venv setup failed"; sleep 3; return 1; }

  echo "==> starting kopf here, UI on :$UI_LOCAL"
  : >"$OPLOG"
  FHIR_UI_PORT="$UI_LOCAL" \
  FHIR_MANIFEST="$MANIFEST" \
  FHIR_KUBECONFIG="$KUBECONFIG" \
  nohup "$VENV/bin/kopf" run "$HERE/fhir_operator.py" \
      --all-namespaces --standalone --verbose >>"$OPLOG" 2>&1 &
  echo $! >"$OPPIDFILE"
  echo local >"$MODEFILE"

  for _ in $(seq 1 30); do
    nc -z 127.0.0.1 "$UI_LOCAL" 2>/dev/null && break
    local_running || { echo "    kopf exited, see $OPLOG"; tail -5 "$OPLOG"; sleep 4; return 1; }
    sleep 1
  done
  sleep 1
}

operator_cluster() {
  echo "==> switching to in-cluster"
  local_stop
  echo "==> scaling the in-cluster operator to 1"
  kubectl -n "$UI_NAMESPACE" scale deploy/"$UI_SERVICE" --replicas=1 2>&1 | sed 's/^/    /'
  kubectl -n "$UI_NAMESPACE" rollout status deploy/"$UI_SERVICE" --timeout=180s 2>&1 | sed 's/^/    /'
  echo cluster >"$MODEFILE"
  ui_forward_up
  sleep 1
}

# Keep whichever mode is current alive, without switching.
operator_keep() {
  case "$(mode)" in
    local)   local_running || operator_local ;;
    cluster) ui_forward_up ;;
    *)       ui_forward_up ;;   # first run: assume the pod, it costs nothing
  esac
}

# --------------------------------------------------------------------------

_port_state() {
  nc -z 127.0.0.1 "$1" 2>/dev/null && echo "up  " || echo "down"
}

status() {
  local ns="none" where
  [ -f "$CURRENT" ] && ns=$(cat "$CURRENT")
  case "$(mode)" in
    local)   local_running && where="local (pid $(cat "$OPPIDFILE"))" || where="local (DEAD - see $OPLOG)" ;;
    cluster) where="in-cluster" ;;
    *)       where="unknown" ;;
  esac
  echo "  operator: $where"
  echo "  $(_port_state "$UI_LOCAL")  localhost:$UI_LOCAL  -> operator UI"
  echo
  echo "  stack: $ns"
  for spec in "${SERVICES[@]}"; do
    # shellcheck disable=SC2086
    set -- $spec
    echo "  $(_port_state "$2")  localhost:$2  -> $1"
  done
}

trap 'echo; echo "menu exited; operator and forwards left running"; exit 0' INT

while true; do
  operator_keep
  clear
  echo "=== FHIR operator ==="
  status
  echo
  echo "  operator UI  http://localhost:$UI_LOCAL"
  echo "  hapi         http://localhost:8083/fhir"
  echo "  postgres     psql -h localhost -p 5433 -U fhir fhir"
  echo "  elastic      http://localhost:9201"
  echo "  logs         $LOG   $OPLOG"
  echo

  options=()
  while read -r ns; do
    [ -n "$ns" ] && options+=("$ns")
  done < <(list_stacks)

  [ ${#options[@]} -eq 0 ] && echo "No FhirStacks and no hapi-fhir deployments found."
  options+=(
    "[operator: run here]"
    "[operator: run in cluster]"
    "[operator: tail log]"
    "[kill all stack port-forwards]"
    "[refresh]"
    "[quit, keep everything running]"
    "[quit, kill everything]"
  )

  PS3=$'\n''select> '
  select choice in "${options[@]}"; do
    case "$choice" in
      "")                                break ;;
      "[operator: run here]")            operator_local; break ;;
      "[operator: run in cluster]")      operator_cluster; break ;;
      "[operator: tail log]")            [ "$(mode)" = local ] && tail -40 "$OPLOG" \
                                           || kubectl -n "$UI_NAMESPACE" logs deploy/"$UI_SERVICE" --tail=40
                                         echo; read -r -p "enter to continue" _; break ;;
      "[kill all stack port-forwards]")  kill_all; echo "killed."; sleep 1; break ;;
      "[refresh]")                       break ;;
      "[quit, keep everything running]") exit 0 ;;
      "[quit, kill everything]")         kill_all; local_stop; ui_forward_down; exit 0 ;;
      *)                                 start_forwards "$choice"; break ;;
    esac
  done
done

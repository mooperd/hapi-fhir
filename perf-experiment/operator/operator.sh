#!/usr/bin/env bash
# Created by claude-opus-5
#
# The operator, in a container.
#
#   ./operator.sh up        scale the cluster copy to 0, build and start here
#   ./operator.sh down      stop here, leave the cluster copy at 0
#   ./operator.sh cluster   stop here, scale the cluster copy back to 1
#   ./operator.sh logs      follow
#   ./operator.sh status    where is it running, and is it reconciling
set -uo pipefail

HERE="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
DOWNLOADER="${DOWNLOADER:-$HOME/Documents/GitHub/mimic-fhir-downloader}"
KUBECONFIG="${KUBECONFIG:-$DOWNLOADER/behemoth-andrew-test.yaml}"
export KUBECONFIG KUBECONFIG_PATH="$KUBECONFIG" MANIFEST_DIR="$DOWNLOADER"

NAMESPACE="${UI_NAMESPACE:-fhir-operator}"
SERVICE=fhir-operator
UI=8085
LIVENESS=8086

compose() { docker compose -f "$HERE/docker-compose.yml" "$@"; }

scale_cluster() {
  kubectl -n "$NAMESPACE" scale deploy/"$SERVICE" --replicas="$1" 2>&1 | sed 's/^/    /'
}

reconciling() {
  curl -sf --max-time 5 "http://localhost:$LIVENESS/healthz" >/dev/null 2>&1
}

case "${1:-status}" in

  up)
    [ -S "$HOME/.orbstack/run/docker.sock" ] || [ -S /var/run/docker.sock ] || {
      echo "docker is not running; start OrbStack first" >&2; exit 1; }
    echo "==> scaling the in-cluster operator to 0"
    scale_cluster 0
    echo "==> building and starting the container"
    compose up -d --build || exit 1
    echo "==> waiting for the reconciler"
    for _ in $(seq 1 60); do
      reconciling && break
      sleep 2
    done
    if reconciling; then
      echo "    reconciler LIVE   http://localhost:$UI"
    else
      echo "    reconciler did NOT come up. Last lines:" >&2
      compose logs --tail=25
      exit 1
    fi
    ;;

  reload)
    # The source is bind-mounted, so restarting is enough; rebuilding only
    # matters when requirements.txt or the Dockerfile changes.
    echo "==> restarting the container against the source on disk"
    compose restart operator || exit 1
    for _ in $(seq 1 60); do
      reconciling && break
      sleep 2
    done
    if reconciling; then
      echo "    reconciler LIVE   http://localhost:$UI"
    else
      echo "    reconciler did NOT come back. Last lines:" >&2
      compose logs --tail=25
      exit 1
    fi
    ;;

  down)
    compose down
    ;;

  cluster)
    compose down
    echo "==> scaling the in-cluster operator to 1"
    scale_cluster 1
    kubectl -n "$NAMESPACE" rollout status deploy/"$SERVICE" --timeout=180s 2>&1 | sed 's/^/    /'
    echo "    remember: :$UI now needs a port-forward, not this container"
    ;;

  logs)
    compose logs -f
    ;;

  status)
    running=$(compose ps --status running --format '{{.Name}}' 2>/dev/null | head -1)
    cluster=$(kubectl -n "$NAMESPACE" get deploy "$SERVICE" \
                -o jsonpath='{.status.readyReplicas}/{.spec.replicas}' 2>/dev/null)
    echo "  container   ${running:-not running}"
    echo "  in-cluster  ${cluster:-unknown} ready"
    if reconciling; then
      echo "  reconciler  LIVE"
    else
      echo "  reconciler  NOT LIVE"
    fi
    echo "  UI          http://localhost:$UI"
    ;;

  *)
    echo "usage: $0 [up|reload|down|cluster|logs|status]" >&2
    exit 2
    ;;
esac

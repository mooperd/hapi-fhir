#!/usr/bin/env bash
# Created by claude-opus-5
#
# Deploy the FHIR benchmarking operator into the cluster.
#
#   ./deploy.sh              install or update
#   ./deploy.sh uninstall    remove the operator (leaves CRD and any FhirStacks)
#   ./deploy.sh logs         follow the operator log
#
# Idempotent: re-run it after editing fhir_operator.py and it will reload the
# ConfigMap and restart the pod. Override KUBECONFIG or MANIFEST by exporting
# them first.
set -euo pipefail

HERE="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
DOWNLOADER="${DOWNLOADER:-$HOME/Documents/GitHub/mimic-fhir-downloader}"

KUBECONFIG="${KUBECONFIG:-$DOWNLOADER/behemoth-andrew-test.yaml}"
MANIFEST="${MANIFEST:-$DOWNLOADER/hapi-fhir-standalone.yaml}"
NAMESPACE="${NAMESPACE:-fhir-operator}"

export KUBECONFIG
kube() { kubectl --namespace "$NAMESPACE" "$@"; }

case "${1:-install}" in

  logs)
    exec kube logs -l app=fhir-operator --tail=200 --follow
    ;;

  uninstall)
    # The CRD and any FhirStacks are left alone on purpose: deleting the CRD
    # deletes every FhirStack, and deleting a FhirStack garbage-collects the
    # HAPI stack underneath it. That is not something to do by accident.
    kubectl delete -f "$HERE/deployment.yaml" --ignore-not-found
    kubectl delete -f "$HERE/rbac.yaml" --ignore-not-found
    kube delete configmap fhir-operator-src --ignore-not-found
    kubectl delete namespace "$NAMESPACE" --ignore-not-found
    echo
    echo "Operator removed. CRD and FhirStacks left in place:"
    kubectl get fhirstacks --all-namespaces 2>/dev/null || true
    echo "  kubectl delete crd fhirstacks.perf.pkb   # deletes every FhirStack and its stack"
    exit 0
    ;;

  install) ;;
  *) echo "usage: $0 [install|uninstall|logs]" >&2; exit 2 ;;
esac

[ -f "$MANIFEST" ] || { echo "manifest not found: $MANIFEST" >&2; exit 1; }

echo "==> cluster:   $(kubectl config current-context)"
echo "==> kubeconfig $KUBECONFIG"
echo "==> manifest   $MANIFEST"
echo

echo "==> CRDs"
kubectl apply -f "$HERE/crd.yaml"
kubectl apply -f "$HERE/dataset-crd.yaml"

echo "==> namespace"
kubectl create namespace "$NAMESPACE" --dry-run=client -o yaml | kubectl apply -f -

echo "==> rbac"
kubectl apply -f "$HERE/rbac.yaml"

# The operator reads the manifest at render time, so it has to travel with the
# source. Both land in the same ConfigMap, mounted read-only at /app.
echo "==> source + manifest -> configmap"
kube create configmap fhir-operator-src \
  --from-file=fhir_operator.py="$HERE/fhir_operator.py" \
  --from-file=ui.py="$HERE/ui.py" \
  --from-file=datasets.py="$HERE/datasets.py" \
  --from-file=loader.py="$HERE/loader.py" \
  --from-file=requirements.txt="$HERE/requirements.txt" \
  --from-file=hapi-fhir-standalone.yaml="$MANIFEST" \
  --dry-run=client -o yaml | kubectl apply -f -

echo "==> deployment"
kubectl apply -f "$HERE/deployment.yaml"

# A ConfigMap change does not restart the pod by itself, and the operator only
# reads its source at startup.
echo "==> restart"
kube rollout restart deployment/fhir-operator
kube rollout status deployment/fhir-operator --timeout=180s

echo
echo "Operator is up. Watching all namespaces."
echo
echo "  $HERE/port-forward.sh      then open http://localhost:8085"
echo "  kubectl get fhirstacks -A -w"
echo "  $0 logs"

#!/usr/bin/env bash
# Created by claude-opus-5
#
# Deploy the FHIR benchmarking operator into the cluster.
#
#   ./deploy.sh              install or update
#   ./deploy.sh uninstall    remove the operator (leaves CRD and any FhirStacks)
#   ./deploy.sh logs         follow the operator log
#   ./deploy.sh ui           port-forward the UI to http://localhost:8085
#
# The operator runs from a container image built by
# .github/workflows/operator-image.yml and published to ghcr. This script
# deploys the image for the commit that is checked out right now -- not
# :latest. A benchmark result has to be able to name the operator that
# produced it, and a floating tag cannot.
#
# Override KUBECONFIG, MANIFEST or OPERATOR_IMAGE_REPO by exporting them first.
set -euo pipefail

HERE="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
DOWNLOADER="${DOWNLOADER:-$HOME/Documents/GitHub/mimic-fhir-downloader}"

KUBECONFIG="${KUBECONFIG:-$DOWNLOADER/behemoth-andrew-test.yaml}"
MANIFEST="${MANIFEST:-$DOWNLOADER/hapi-fhir-standalone.yaml}"
NAMESPACE="${NAMESPACE:-fhir-operator}"
OPERATOR_IMAGE_REPO="${OPERATOR_IMAGE_REPO:-ghcr.io/mooperd/fhir-operator}"
# GCS. The key belongs to a bootstrap identity whose only permission is
# serviceAccountTokenCreator on GCS_WRITER_SA; the writer itself has no key.
GCP_KEY="${GCP_KEY:-$HOME/.config/gcloud/fhir-operator-key.json}"
GCS_WRITER_SA="${GCS_WRITER_SA:-fhir-benchmark-writer@teak-mantis-509006-s9.iam.gserviceaccount.com}"
UI_PORT=8085

export KUBECONFIG
kube() { kubectl --namespace "$NAMESPACE" "$@"; }

case "${1:-install}" in

  logs)
    exec kube logs -l app=fhir-operator --tail=200 --follow
    ;;

  ui)
    echo "==> http://localhost:$UI_PORT"
    exec kube port-forward "svc/fhir-operator" "$UI_PORT:$UI_PORT"
    ;;

  uninstall)
    # The CRD and any FhirStacks are left alone on purpose: deleting the CRD
    # deletes every FhirStack, and deleting a FhirStack garbage-collects the
    # HAPI stack underneath it. That is not something to do by accident.
    kubectl delete -f "$HERE/deployment.yaml" --ignore-not-found
    kubectl delete -f "$HERE/rbac.yaml" --ignore-not-found
    kube delete configmap fhir-operator-manifest fhir-operator-src --ignore-not-found
    kube delete secret fhir-operator-gcp --ignore-not-found
    kubectl delete namespace "$NAMESPACE" --ignore-not-found
    echo
    echo "Operator removed. CRD and FhirStacks left in place:"
    kubectl get fhirstacks --all-namespaces 2>/dev/null || true
    kubectl get fhirbenchmarks --all-namespaces 2>/dev/null || true
    echo "  kubectl delete crd fhirstacks.perf.fhir   # deletes every FhirStack and its stack"
    exit 0
    ;;

  install) ;;
  *) echo "usage: $0 [install|uninstall|logs|ui]" >&2; exit 2 ;;
esac

[ -f "$MANIFEST" ] || { echo "manifest not found: $MANIFEST" >&2; exit 1; }

# ---------------------------------------------------------------- the image
#
# The tag is the commit, so what runs in the cluster is identifiable.

SHA="$(git -C "$HERE" rev-parse HEAD)"
IMAGE="$OPERATOR_IMAGE_REPO:sha-$SHA"

# A public ghcr package still needs a pull token, but an anonymous one is
# enough. A 200 here is the image existing; anything else is the build for
# this commit not having published yet.
echo "==> checking $IMAGE"
REPO_PATH="${OPERATOR_IMAGE_REPO#ghcr.io/}"
TOKEN="$(curl -fsS "https://ghcr.io/token?scope=repository:$REPO_PATH:pull" \
         | sed -n 's/.*"token":"\([^"]*\)".*/\1/p')"
CODE="$(curl -sS -o /dev/null -w '%{http_code}' \
        -H "Authorization: Bearer $TOKEN" \
        -H "Accept: application/vnd.oci.image.index.v1+json" \
        -H "Accept: application/vnd.docker.distribution.manifest.list.v2+json" \
        "https://ghcr.io/v2/$REPO_PATH/manifests/sha-$SHA")"
if [ "$CODE" != "200" ]; then
  echo "refusing to deploy: $IMAGE is not published (HTTP $CODE)." >&2
  echo "Push the commit and wait for the build:" >&2
  echo "  https://github.com/mooperd/hapi-fhir/actions/workflows/operator-image.yml" >&2
  exit 1
fi

echo "==> cluster:   $(kubectl config current-context)"
echo "==> kubeconfig $KUBECONFIG"
echo "==> manifest   $MANIFEST"
echo "==> image      $IMAGE"
echo

echo "==> CRDs"
kubectl apply -f "$HERE/crd.yaml"
kubectl apply -f "$HERE/dataset-crd.yaml"
kubectl apply -f "$HERE/benchmark-crd.yaml"
kubectl apply -f "$HERE/grant-crd.yaml"

echo "==> namespace"
kubectl create namespace "$NAMESPACE" --dry-run=client -o yaml | kubectl apply -f -

echo "==> rbac"
kubectl apply -f "$HERE/rbac.yaml"

# The Python source is in the image. The stack manifest is not -- it lives in
# the mimic-fhir-downloader repo and the operator reads it at render time, so
# it travels as its own small ConfigMap mounted at /manifests.
echo "==> manifest -> configmap"
kube create configmap fhir-operator-manifest \
  --from-file=hapi-fhir-standalone.yaml="$MANIFEST" \
  --dry-run=client -o yaml | kubectl apply -f -

# The only long-lived credential in the system. Refusing to deploy without it
# is deliberate: an operator that starts with no way to write results would
# fail every run at its first step instead of at install time.
echo "==> gcp key -> secret"
[ -f "$GCP_KEY" ] || {
  echo "refusing to deploy: no service account key at $GCP_KEY" >&2
  echo "  gcloud iam service-accounts keys create $GCP_KEY \\" >&2
  echo "    --iam-account fhir-operator@teak-mantis-509006-s9.iam.gserviceaccount.com" >&2
  exit 1
}
kube create secret generic fhir-operator-gcp \
  --from-file=key.json="$GCP_KEY" \
  --dry-run=client -o yaml | kubectl apply -f -

echo "==> deployment"
sed -e "s|__OPERATOR_IMAGE__|$IMAGE|" \
    -e "s|__GCS_WRITER_SA__|$GCS_WRITER_SA|" \
    "$HERE/deployment.yaml" | kubectl apply -f -

# The image tag changes with the commit, so a new commit rolls by itself. The
# restart is for the case where only the manifest ConfigMap changed -- a
# ConfigMap edit does not restart the pod, and the operator reads the manifest
# at startup.
echo "==> restart"
kube rollout restart deployment/fhir-operator
kube rollout status deployment/fhir-operator --timeout=180s

echo
echo "Operator is up. Watching all namespaces."
echo
echo "  running $IMAGE"
echo
echo "  $0 ui                      then open http://localhost:$UI_PORT"
echo "  kubectl get fhirstacks -A -w"
echo "  $0 logs"

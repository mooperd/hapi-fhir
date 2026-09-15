# Created by claude-opus-5
FROM python:3.12-slim

WORKDIR /app

# Dependencies first so editing the operator does not reinstall them.
COPY requirements.txt /app/requirements.txt
RUN pip install --no-cache-dir -r /app/requirements.txt

COPY fhir_operator.py ui.py datasets.py reconciliation.py loader.py /app/

# kopf derives its identity from getpass.getuser(), which raises for a uid
# with no passwd entry. POD_ID is checked first and skips that path.
# KUBECONFIG is for kopf's own login_via_client; FHIR_KUBECONFIG is for the
# operator's dynamic client. Setting only the latter leaves kopf with no
# credentials at all.
ENV POD_ID=fhir-operator-local \
    PYTHONUNBUFFERED=1 \
    FHIR_UI_PORT=8085 \
    KUBECONFIG=/kube/config \
    FHIR_KUBECONFIG=/kube/config \
    FHIR_MANIFEST=/manifests/hapi-fhir-standalone.yaml

EXPOSE 8085 8086

# --standalone: no peering. Only one operator may reconcile a given stack, and
# the in-cluster Deployment is scaled to zero while this one runs.
CMD ["kopf", "run", "/app/fhir_operator.py", \
     "--all-namespaces", "--standalone", \
     "--liveness=http://0.0.0.0:8086/healthz", "--verbose"]

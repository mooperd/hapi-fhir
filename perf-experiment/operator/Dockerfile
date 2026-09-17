# Created by claude-opus-5
FROM python:3.12-slim

WORKDIR /app

# Dependencies first so editing the operator does not reinstall them.
COPY requirements.txt /app/requirements.txt
RUN pip install --no-cache-dir -r /app/requirements.txt

COPY fhir_operator.py ui.py datasets.py reconciliation.py loader.py \
     benchmark.py catalogue.py binding.py control.py stats.py runner.py /app/

# kopf derives its identity from getpass.getuser(), which raises for a uid
# with no passwd entry. POD_ID is checked first and skips that path.
#
# Deliberately no KUBECONFIG, FHIR_KUBECONFIG or FHIR_MANIFEST here. The image
# has to default to running inside the cluster: fhir_operator.dyn() calls
# load_kube_config(config_file=...) whenever FHIR_KUBECONFIG is set, so baking
# in a path that only exists on a laptop makes the image unable to use its own
# ServiceAccount. docker-compose.yml sets all three for the local container,
# which is the case that is special, not this one.
ENV POD_ID=fhir-operator-local \
    PYTHONUNBUFFERED=1 \
    FHIR_UI_PORT=8085

EXPOSE 8085 8086

# --standalone: no peering. Only one operator may reconcile a given stack, and
# the in-cluster Deployment is scaled to zero while this one runs.
CMD ["kopf", "run", "/app/fhir_operator.py", \
     "--all-namespaces", "--standalone", \
     "--liveness=http://0.0.0.0:8086/healthz", "--verbose"]

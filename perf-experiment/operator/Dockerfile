# Created by claude-opus-5
#
# Two targets from one context.
#
#   worker    runner.py and loader.py, with their dependencies baked in. Every
#             Job the operator launches runs this. Nothing is pip-installed at
#             pod start and no source arrives in a ConfigMap, so the code that
#             runs is the code that was built and reviewed.
#
#   operator  the kopf process and its UI. Holds the only GCP credential in
#             the system, and is built with the worker's digest baked in so it
#             can only ever launch the worker it shipped alongside.

FROM python:3.12-slim AS base
WORKDIR /app
ENV PYTHONUNBUFFERED=1


# --------------------------------------------------------------------------
FROM base AS worker

COPY requirements-worker.txt /app/requirements-worker.txt
RUN pip install --no-cache-dir -r /app/requirements-worker.txt

COPY upload.py runner.py loader.py /app/

ENV HOME=/tmp
EXPOSE 9100
# No CMD: the Job sets the entrypoint, because one image serves both the
# benchmark worker and the dataset loader.


# --------------------------------------------------------------------------
FROM base AS operator

COPY requirements.txt /app/requirements.txt
RUN pip install --no-cache-dir -r /app/requirements.txt

COPY fhir_operator.py ui.py datasets.py reconciliation.py loader.py \
     benchmark.py catalogue.py binding.py control.py stats.py runner.py \
     gcs.py upload.py /app/

# Supplied by CI as ghcr.io/<owner>/fhir-worker@sha256:... -- an immutable
# reference resolved at build time, never a floating tag at run time.
ARG WORKER_IMAGE=ghcr.io/mooperd/fhir-worker:latest

ENV POD_ID=fhir-operator-local \
    FHIR_UI_PORT=8085 \
    WORKER_IMAGE=${WORKER_IMAGE}

EXPOSE 8085 8086

CMD ["kopf", "run", "/app/fhir_operator.py", \
     "--all-namespaces", "--standalone", \
     "--liveness=http://0.0.0.0:8086/healthz", "--verbose"]

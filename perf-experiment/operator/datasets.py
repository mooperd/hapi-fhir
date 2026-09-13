# Created by claude-opus-5
"""FhirDataset: synthetic ingest as a Job, plus the Prometheus that watches it.

A dataset is an event, not a desired state. Once its Job exists the operator
never touches it again -- no TTL, no delete-and-recreate, no reacting to spec
edits. Editing a FhirDataset does nothing; to re-run, make a new one.

That is deliberate. A Job that disappears and gets recreated silently reloads
the dataset, and ttlSecondsAfterFinished is exactly what makes it disappear.

Handlers are registered at import. fhir_operator calls init() to hand over the
shared dynamic client.
"""

import json
import os

import kopf
import requests
import yaml
from kubernetes import client

GROUP = "perf.pkb"
VERSION = "v1alpha1"
PLURAL = "fhirdatasets"

HERE = os.path.dirname(os.path.abspath(__file__))
ONTOLOGY = os.environ.get("ONTOLOGY_URL", "https://ontology-main.bloods.co.uk/api/snomed")
LOADER_IMAGE = os.environ.get("LOADER_IMAGE", "python:3.12-slim")
ONTOLOGY_TIMEOUT = 120

# Roots used when spec.codes says nothing. A FhirDataset with an empty spec
# has to produce a working load, the same way a FhirStack without resources
# gets the operator's sizing defaults. Both are huge and will be truncated to
# maxCodesPerRoot, which is recorded in status.codeNotes.
DEFAULT_CONDITION_ROOTS = [{"root": "404684003", "weight": 1}]     # Clinical finding
DEFAULT_OBSERVATION_ROOTS = [{"root": "363787002", "weight": 1}]   # Observable entity

_dyn = None


def init(dyn):
    global _dyn
    _dyn = dyn


def _batch():
    return client.BatchV1Api(_dyn().client)


def _core():
    return client.CoreV1Api(_dyn().client)


def _custom():
    return client.CustomObjectsApi(_dyn().client)


# --------------------------------------------------------------------------
# SNOMED resolution
# --------------------------------------------------------------------------

def resolve(roots, draw_from, cap, logger, notes):
    """Expand SNOMED roots into a flat weighted code list.

    Done once, here, and frozen into a ConfigMap. An HTTP call per resource
    would make the ontology service the bottleneck being measured, and would
    make two runs of the same seed produce different data.

    A root's weight is shared across its expansion, so the subtree keeps the
    share you asked for however many descendants it turns out to have.

    The ontology service caps descendants at 1000 per call and does not say so
    in the payload beyond returned != total -- Observable entity is 21479
    concepts and comes back as an arbitrary 1000. The cap is passed explicitly
    as ?count= and any truncation is recorded, so the code mix is never
    quietly different from the one asked for.
    """
    out = []
    for entry in roots or []:
        root = str(entry["root"])
        weight = float(entry.get("weight", 1))
        concepts = []
        if draw_from != "self":
            url = "%s/concept/%s/%s?count=%d" % (ONTOLOGY, root, draw_from, cap)
            try:
                response = requests.get(url, timeout=ONTOLOGY_TIMEOUT)
                response.raise_for_status()
                body = response.json()
                concepts = body.get(draw_from) or []
                total = body.get("total")
                if total is not None and len(concepts) < total:
                    note = "%s: using %d of %d %s" % (root, len(concepts), total, draw_from)
                    logger.warning(note)
                    notes.append(note)
            except Exception as exc:                       # noqa: BLE001 - reported to status
                raise kopf.TemporaryError(
                    "ontology lookup failed for %s: %s" % (root, exc), delay=30)
        if not concepts:
            # A leaf concept has no descendants; use the root itself.
            try:
                response = requests.get("%s/concept/%s" % (ONTOLOGY, root),
                                        timeout=ONTOLOGY_TIMEOUT)
                response.raise_for_status()
                body = response.json()
                concepts = [{"code": root, "display": body.get("display", root)}]
            except Exception as exc:                       # noqa: BLE001 - reported to status
                raise kopf.TemporaryError(
                    "ontology lookup failed for %s: %s" % (root, exc), delay=30)

        share = weight / len(concepts)
        for concept in concepts:
            out.append({"code": str(concept["code"]),
                        "display": concept.get("display", ""),
                        "weight": share})
    logger.info("resolved %d roots to %d codes", len(roots or []), len(out))
    return out


# --------------------------------------------------------------------------
# Rendering
# --------------------------------------------------------------------------

def dataset_config(spec, logger):
    """Frozen generator config, plus any notes about truncated expansions."""
    shape = spec.get("shape") or {}
    codes = spec.get("codes") or {}
    draw_from = codes.get("drawFrom", "descendants")
    cap = int(codes.get("maxCodesPerRoot", 1000))
    patients = spec.get("patients") or {}
    notes = []
    config = {
        "seed": int(spec.get("seed", 20260911)),
        "first": int(patients.get("first", 1)),
        "count": int(patients.get("count", 1000)),
        "shape": {
            "heavyEveryN": int(shape.get("heavyEveryN", 10)),
            "conditionsPerPatient": {
                "normal": int((shape.get("conditionsPerPatient") or {}).get("normal", 11)),
                "heavy": int((shape.get("conditionsPerPatient") or {}).get("heavy", 90)),
            },
            "observationsPerPatient": {
                "normal": int((shape.get("observationsPerPatient") or {}).get("normal", 53)),
                "heavy": int((shape.get("observationsPerPatient") or {}).get("heavy", 500)),
            },
        },
        "conditionCodes": resolve(codes.get("conditions") or DEFAULT_CONDITION_ROOTS,
                                  draw_from, cap, logger, notes),
        "observationCodes": resolve(codes.get("observations") or DEFAULT_OBSERVATION_ROOTS,
                                    draw_from, cap, logger, notes),
    }
    _validate(config)
    return config, notes


def _validate(config):
    """Fail here, not in a worker sixty seconds later.

    An empty code list makes random.choices raise IndexError deep inside the
    loader, after every worker has paid for a pip install and the Job has
    burned its backoffLimit. Refuse the dataset instead.
    """
    shape = config["shape"]
    for kind, codes_key, shape_key in (
            ("Condition", "conditionCodes", "conditionsPerPatient"),
            ("Observation", "observationCodes", "observationsPerPatient")):
        wanted = max(shape[shape_key]["normal"], shape[shape_key]["heavy"])
        if wanted > 0 and not config[codes_key]:
            raise kopf.PermanentError(
                "spec.shape.%s asks for %d %s per patient but spec.codes resolved "
                "to no codes -- give it a root, or set the count to 0"
                % (shape_key, wanted, kind))
        total = sum(c["weight"] for c in config[codes_key])
        if config[codes_key] and total <= 0:
            raise kopf.PermanentError(
                "%s code weights sum to %g; at least one must be positive" % (kind, total))


def _apply(doc, namespace):
    resource = _dyn().resources.get(api_version=doc["apiVersion"], kind=doc["kind"])
    _dyn().server_side_apply(resource=resource, body=doc, namespace=namespace,
                             field_manager="fhir-operator", force_conflicts=True)


def config_map(name, config):
    """dataset.json plus the loader itself, so the Job needs no image build."""
    with open(os.path.join(HERE, "loader.py"), encoding="utf-8") as handle:
        loader = handle.read()
    return {
        "apiVersion": "v1", "kind": "ConfigMap",
        "metadata": {"name": name},
        "data": {
            "dataset.json": json.dumps(config),
            "loader.py": loader,
            "requirements.txt": "requests\nprometheus_client\n",
        },
    }


def results_claim(name, size_gi):
    return {
        "apiVersion": "v1", "kind": "PersistentVolumeClaim",
        "metadata": {"name": name},
        "spec": {
            "accessModes": ["ReadWriteOnce"],
            "resources": {"requests": {"storage": "%dGi" % size_gi}},
        },
    }


def job(name, dataset, namespace, spec, config_name, claim_name):
    parallelism = int(spec.get("parallelism", 4))
    loader = spec.get("loader") or {}
    cpu = float(loader.get("cpu", 0.5))
    memory = float(loader.get("memory", 1))
    stack = spec.get("stackRef") or namespace

    return {
        "apiVersion": "batch/v1", "kind": "Job",
        "metadata": {"name": name, "labels": {"app": "fhir-loader", "dataset": dataset}},
        "spec": {
            # Indexed: each worker takes every Nth serial, so shards never
            # overlap and workers need no coordination.
            "completionMode": "Indexed",
            "completions": parallelism,
            "parallelism": parallelism,
            "backoffLimit": 6,
            # No ttlSecondsAfterFinished, on purpose. The TTL controller
            # deleting a finished Job is what makes the operator recreate it
            # and silently reload the dataset.
            "template": {
                "metadata": {"labels": {"app": "fhir-loader", "dataset": dataset}},
                "spec": {
                    "restartPolicy": "Never",
                    "containers": [{
                        "name": "loader",
                        "image": LOADER_IMAGE,
                        "command": ["/bin/sh", "-c"],
                        "args": [
                            "set -e\n"
                            "pip install --quiet --no-cache-dir --target /deps "
                            "-r /config/requirements.txt\n"
                            "exec python /config/loader.py\n"
                        ],
                        "env": [
                            {"name": "FHIR_BASE_URL",
                             "value": "http://hapi-fhir.%s.svc:8080/fhir" % namespace},
                            {"name": "DATASET_CONFIG", "value": "/config/dataset.json"},
                            {"name": "RESULTS_DIR", "value": "/results/%s" % dataset},
                            {"name": "PARALLELISM", "value": str(parallelism)},
                            {"name": "PYTHONPATH", "value": "/deps"},
                            {"name": "HOME", "value": "/tmp"},
                            {"name": "STACK", "value": stack},
                        ],
                        "ports": [{"name": "metrics", "containerPort": 9100}],
                        "resources": {
                            "requests": {"cpu": "%dm" % round(cpu * 1000),
                                         "memory": "%dMi" % round(memory * 1024)},
                            "limits": {"memory": "%dMi" % round(memory * 2048)},
                        },
                        "volumeMounts": [
                            {"name": "config", "mountPath": "/config", "readOnly": True},
                            {"name": "results", "mountPath": "/results"},
                            {"name": "deps", "mountPath": "/deps"},
                            {"name": "tmp", "mountPath": "/tmp"},
                        ],
                    }],
                    "volumes": [
                        {"name": "config", "configMap": {"name": config_name}},
                        {"name": "results",
                         "persistentVolumeClaim": {"claimName": claim_name}},
                        {"name": "deps", "emptyDir": {}},
                        {"name": "tmp", "emptyDir": {}},
                    ],
                },
            },
        },
    }


# --------------------------------------------------------------------------
# Prometheus, one per namespace, shared by every dataset in it
# --------------------------------------------------------------------------

def ensure_prometheus(namespace, logger):
    scrape = {
        "global": {"scrape_interval": "5s"},
        "scrape_configs": [{
            "job_name": "fhir-loader",
            "kubernetes_sd_configs": [{"role": "pod",
                                       "namespaces": {"names": [namespace]}}],
            "relabel_configs": [
                {"source_labels": ["__meta_kubernetes_pod_label_app"],
                 "action": "keep", "regex": "fhir-loader"},
                {"source_labels": ["__meta_kubernetes_pod_ip"],
                 "target_label": "__address__", "replacement": "$1:9100"},
                {"source_labels": ["__meta_kubernetes_pod_label_dataset"],
                 "target_label": "dataset"},
                {"source_labels": ["__meta_kubernetes_pod_name"],
                 "target_label": "pod"},
            ],
        }],
    }

    docs = [
        {"apiVersion": "v1", "kind": "ServiceAccount",
         "metadata": {"name": "prometheus"}},
        {"apiVersion": "rbac.authorization.k8s.io/v1", "kind": "Role",
         "metadata": {"name": "prometheus"},
         "rules": [{"apiGroups": [""], "resources": ["pods", "services", "endpoints"],
                    "verbs": ["get", "list", "watch"]}]},
        {"apiVersion": "rbac.authorization.k8s.io/v1", "kind": "RoleBinding",
         "metadata": {"name": "prometheus"},
         "roleRef": {"apiGroup": "rbac.authorization.k8s.io", "kind": "Role",
                     "name": "prometheus"},
         "subjects": [{"kind": "ServiceAccount", "name": "prometheus",
                       "namespace": namespace}]},
        {"apiVersion": "v1", "kind": "ConfigMap",
         "metadata": {"name": "prometheus-config"},
         "data": {"prometheus.yml": yaml.safe_dump(scrape, sort_keys=False)}},
        {"apiVersion": "apps/v1", "kind": "Deployment",
         "metadata": {"name": "prometheus", "labels": {"app": "prometheus"}},
         "spec": {
             "replicas": 1,
             "selector": {"matchLabels": {"app": "prometheus"}},
             "template": {
                 "metadata": {"labels": {"app": "prometheus"}},
                 "spec": {
                     "serviceAccountName": "prometheus",
                     "containers": [{
                         "name": "prometheus",
                         "image": "prom/prometheus:v2.55.1",
                         "args": ["--config.file=/etc/prometheus/prometheus.yml",
                                  "--storage.tsdb.retention.time=7d"],
                         "ports": [{"name": "http", "containerPort": 9090}],
                         "resources": {"requests": {"cpu": "100m", "memory": "512Mi"},
                                       "limits": {"memory": "2Gi"}},
                         "volumeMounts": [
                             {"name": "config", "mountPath": "/etc/prometheus"},
                             {"name": "data", "mountPath": "/prometheus"},
                         ],
                     }],
                     "volumes": [
                         {"name": "config",
                          "configMap": {"name": "prometheus-config"}},
                         {"name": "data", "emptyDir": {}},
                     ],
                 },
             },
         }},
        {"apiVersion": "v1", "kind": "Service",
         "metadata": {"name": "prometheus"},
         "spec": {"selector": {"app": "prometheus"},
                  "ports": [{"name": "http", "port": 9090, "targetPort": 9090}]}},
    ]
    for doc in docs:
        doc["metadata"]["namespace"] = namespace
        _apply(doc, namespace)
    logger.info("prometheus ready in %s", namespace)


# --------------------------------------------------------------------------
# Handlers
# --------------------------------------------------------------------------

@kopf.on.create(GROUP, VERSION, PLURAL, id="load")
@kopf.on.resume(GROUP, VERSION, PLURAL, id="reload")
def ensure(spec, meta, status, namespace, name, patch, logger, **_):
    """Create the Job once, and never again.

    status.jobName is the latch. It is written before the Job is created, so a
    crash in between still leaves a name recorded and the retry does nothing.
    A Job that has been deleted by hand is not recreated -- that is the whole
    point, and it is why nothing here sets ttlSecondsAfterFinished.
    """
    existing = (status or {}).get("jobName")
    if existing:
        logger.info("%s already has job %s; nothing to do", name, existing)
        return

    job_name = "%s-g%d" % (name, meta.get("generation", 1))

    # Latch first. Status is a subresource, so this is a separate write that
    # lands before any Job exists.
    _custom().patch_namespaced_custom_object_status(
        GROUP, VERSION, namespace, PLURAL, name,
        {"status": {"jobName": job_name, "phase": "Resolving"}})

    config, notes = dataset_config(spec, logger)
    loader = spec.get("loader") or {}

    config_name = "%s-config" % name
    claim_name = "%s-results" % name

    for doc in (config_map(config_name, config),
                results_claim(claim_name, int(loader.get("storageGi", 10)))):
        doc["metadata"]["namespace"] = namespace
        kopf.adopt(doc)
        _apply(doc, namespace)

    ensure_prometheus(namespace, logger)

    body = job(job_name, name, namespace, spec, config_name, claim_name)
    body["metadata"]["namespace"] = namespace
    kopf.adopt(body)
    try:
        _batch().create_namespaced_job(namespace, body)
    except client.ApiException as exc:
        if exc.status != 409:
            raise
        logger.info("job %s already exists", job_name)

    patch.status["phase"] = "Running"
    patch.status["resolvedCodes"] = len(config["conditionCodes"]) + len(config["observationCodes"])
    patch.status["resultsPath"] = "/results/%s" % name
    if notes:
        patch.status["codeNotes"] = notes
    logger.info("started %s: %d workers over %d patients",
                job_name, spec.get("parallelism", 4),
                (spec.get("patients") or {}).get("count", 1000))


@kopf.timer(GROUP, VERSION, PLURAL, interval=15, id="progress")
def progress(status, namespace, name, patch, **_):
    job_name = (status or {}).get("jobName")
    if not job_name:
        return
    try:
        found = _batch().read_namespaced_job(job_name, namespace)
    except client.ApiException:
        # Deliberately not recreated. Say so rather than quietly reloading.
        patch.status["phase"] = "JobMissing"
        return

    succeeded = found.status.succeeded or 0
    failed = found.status.failed or 0
    wanted = found.spec.completions or 1
    patch.status["workers"] = "%d/%d" % (succeeded, wanted)
    if failed:
        patch.status["failedWorkers"] = failed
    if succeeded >= wanted:
        patch.status["phase"] = "Complete"
    elif failed and not (found.status.active or 0):
        patch.status["phase"] = "Failed"
    else:
        patch.status["phase"] = "Running"

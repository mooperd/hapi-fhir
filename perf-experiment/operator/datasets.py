# Created by claude-opus-5
"""FhirDataset: declarative data lifecycle, reconciled by Jobs.

A FhirDataset says what should be in the database. The operator compares that
to what is actually there and launches a Job to close the gap:

    state: Present, not enough loaded  -> load job
    state: Absent,  still present      -> delete job
    deleted while data present         -> delete job, held by a finalizer
    matches                            -> nothing

Every resource carries meta.tag http://perf.fhir/dataset|<name>. The tag is the
only handle on a dataset, and metadata.uid is used because it is unique per
object and regenerated when a CR of the same name is recreated.

Actual state is reported inward by the Jobs, not polled outward: the operator
may be running on a laptop with no route to the FHIR service.

Datasets are children of a FhirStack. The operator sets an owner reference to
the stack named in spec.stackRef, so deleting a stack takes its datasets with
it and the whole node plus its data can live in one YAML file.
"""

import json
import os

import kopf
import requests
import yaml
from kubernetes import client

import reconciliation

GROUP = "perf.fhir"
VERSION = "v1alpha1"
PLURAL = "fhirdatasets"
STACK_PLURAL = "fhirstacks"

HERE = os.path.dirname(os.path.abspath(__file__))
ONTOLOGY = os.environ.get("ONTOLOGY_URL", "https://ontology-main.bloods.co.uk/api/snomed")
LOADER_IMAGE = os.environ.get("LOADER_IMAGE", "python:3.12-slim")
ONTOLOGY_TIMEOUT = 120
TAG_SYSTEM = "http://perf.fhir/dataset"

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
    """Expand SNOMED roots into a flat weighted code list, once, at creation.

    An HTTP call per resource would make the ontology service the bottleneck
    being measured. The service caps descendants at 1000 per call and only
    signals it via returned != total, so the cap is passed explicitly as
    ?count= and any truncation is recorded rather than silently applied.
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
            except Exception as exc:                       # noqa: BLE001 - surfaced
                raise kopf.TemporaryError(
                    "ontology lookup failed for %s: %s" % (root, exc), delay=30)
        if not concepts:
            try:
                response = requests.get("%s/concept/%s" % (ONTOLOGY, root),
                                        timeout=ONTOLOGY_TIMEOUT)
                response.raise_for_status()
                concepts = [{"code": root, "display": response.json().get("display", root)}]
            except Exception as exc:                       # noqa: BLE001 - surfaced
                raise kopf.TemporaryError(
                    "ontology lookup failed for %s: %s" % (root, exc), delay=30)
        share = weight / len(concepts)
        for concept in concepts:
            out.append({"code": str(concept["code"]),
                        "display": concept.get("display", ""), "weight": share})
    logger.info("resolved %d roots to %d codes", len(roots or []), len(out))
    return out


def dataset_config(spec, name, logger):
    shape = spec.get("shape") or {}
    codes = spec.get("codes") or {}
    draw_from = codes.get("drawFrom", "descendants")
    cap = int(codes.get("maxCodesPerRoot", 1000))
    patients = spec.get("patients") or {}
    notes = []
    config = {
        "datasetName": name,
        "prefix": spec.get("idPrefix") or name,
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
    """Fail at admission, not in a worker sixty seconds later."""
    shape = config["shape"]
    for kind, codes_key, shape_key in (
            ("Condition", "conditionCodes", "conditionsPerPatient"),
            ("Observation", "observationCodes", "observationsPerPatient")):
        wanted = max(shape[shape_key]["normal"], shape[shape_key]["heavy"])
        if wanted > 0 and not config[codes_key]:
            raise kopf.PermanentError(
                "spec.shape.%s asks for %d %s per patient but spec.codes resolved to no "
                "codes -- give it a root, or set the count to 0" % (shape_key, wanted, kind))
        if config[codes_key] and sum(c["weight"] for c in config[codes_key]) <= 0:
            raise kopf.PermanentError("%s code weights must sum to more than zero" % kind)


# --------------------------------------------------------------------------
# Desired vs actual
# --------------------------------------------------------------------------

def expected(spec):
    """Resource counts a fully loaded dataset should have."""
    shape = spec.get("shape") or {}
    patients = int((spec.get("patients") or {}).get("count", 1000))
    every = int(shape.get("heavyEveryN", 10)) or 10
    heavy = patients // every
    normal = patients - heavy

    def per(key, normal_default, heavy_default):
        block = shape.get(key) or {}
        return (normal * int(block.get("normal", normal_default))
                + heavy * int(block.get("heavy", heavy_default)))

    return {
        "Patient": patients,
        "Condition": per("conditionsPerPatient", 11, 90),
        "Observation": per("observationsPerPatient", 53, 500),
    }


def decide(spec, status):
    """What the database needs, as ('load'|'delete'|None, reason)."""
    want_present = (spec.get("state") or "Present") == "Present"
    observed = (status or {}).get("observed") or {}
    seen = sum(int(observed.get(t, 0)) for t in ("Patient", "Condition", "Observation"))
    target = expected(spec)

    if not want_present:
        if not observed:
            return "delete", "state=Absent, nothing observed yet -- delete is idempotent"
        return ("delete", "state=Absent but %d resources present" % seen) if seen else (None, "absent")

    if not observed:
        return "load", "state=Present, nothing observed yet"
    if int(observed.get("Patient", 0)) < target["Patient"]:
        return "load", "have %d of %d patients" % (observed.get("Patient", 0), target["Patient"])
    return None, "present: %s" % json.dumps(observed)


# --------------------------------------------------------------------------
# Rendering
# --------------------------------------------------------------------------

def _apply(doc, namespace):
    resource = _dyn().resources.get(api_version=doc["apiVersion"], kind=doc["kind"])
    _dyn().server_side_apply(resource=resource, body=doc, namespace=namespace,
                             field_manager="fhir-operator", force_conflicts=True)


def config_map(name, config):
    with open(os.path.join(HERE, "loader.py"), encoding="utf-8") as handle:
        loader = handle.read()
    return {
        "apiVersion": "v1", "kind": "ConfigMap",
        "metadata": {"name": name},
        "data": {"dataset.json": json.dumps(config), "loader.py": loader,
                 "requirements.txt": "requests\nprometheus_client\n"},
    }


def results_claim(name, size_gi):
    return {
        "apiVersion": "v1", "kind": "PersistentVolumeClaim",
        "metadata": {"name": name},
        "spec": {"accessModes": ["ReadWriteOnce"],
                 "resources": {"requests": {"storage": "%dGi" % size_gi}}},
    }


def worker_rbac(namespace):
    """Jobs patch their own FhirDataset status, so they need to be allowed to."""
    return [
        {"apiVersion": "v1", "kind": "ServiceAccount",
         "metadata": {"name": "fhir-loader", "namespace": namespace}},
        {"apiVersion": "rbac.authorization.k8s.io/v1", "kind": "Role",
         "metadata": {"name": "fhir-loader", "namespace": namespace},
         "rules": [{"apiGroups": [GROUP], "resources": ["fhirdatasets", "fhirdatasets/status"],
                    "verbs": ["get", "list", "patch", "update"]}]},
        {"apiVersion": "rbac.authorization.k8s.io/v1", "kind": "RoleBinding",
         "metadata": {"name": "fhir-loader", "namespace": namespace},
         "roleRef": {"apiGroup": "rbac.authorization.k8s.io", "kind": "Role",
                     "name": "fhir-loader"},
         "subjects": [{"kind": "ServiceAccount", "name": "fhir-loader",
                       "namespace": namespace}]},
    ]


def job(job_name, dataset, namespace, spec, mode, config_name, claim_name):
    parallelism = 1 if mode == "delete" else int(spec.get("parallelism", 4))
    loader = spec.get("loader") or {}
    cpu = float(loader.get("cpu", 0.5))
    memory = float(loader.get("memory", 1))
    stack = spec.get("stackRef") or namespace

    return {
        "apiVersion": "batch/v1", "kind": "Job",
        "metadata": {"name": job_name,
                     "labels": {"app": "fhir-loader", "dataset": dataset, "mode": mode}},
        "spec": {
            "completionMode": "Indexed",
            "completions": parallelism,
            "parallelism": parallelism,
            "backoffLimit": 6,
            # suspend is the job control: flip spec.suspend on the CR and the
            # operator patches this, which stops the pods without losing the
            # Job or its place.
            "suspend": bool(spec.get("suspend", False)),
            # No ttlSecondsAfterFinished. A finished Job that disappears looks
            # to a reconciler like a Job that never ran.
            "template": {
                "metadata": {"labels": {"app": "fhir-loader", "dataset": dataset,
                                        "mode": mode}},
                "spec": {
                    "restartPolicy": "Never",
                    "serviceAccountName": "fhir-loader",
                    "containers": [{
                        "name": "worker",
                        "image": LOADER_IMAGE,
                        "command": ["/bin/sh", "-c"],
                        "args": ["set -e\npip install --quiet --no-cache-dir --target /deps "
                                 "-r /config/requirements.txt\nexec python /config/loader.py\n"],
                        "env": [
                            {"name": "MODE", "value": mode},
                            {"name": "FHIR_BASE_URL",
                             "value": "http://hapi-fhir.%s.svc:8080/fhir" % namespace},
                            {"name": "DATASET_CONFIG", "value": "/config/dataset.json"},
                            {"name": "RESULTS_DIR", "value": "/results/%s" % dataset},
                            {"name": "PARALLELISM", "value": str(parallelism)},
                            {"name": "DATASET_NAME", "value": dataset},
                            {"name": "POD_NAMESPACE", "value": namespace},
                            {"name": "PYTHONPATH", "value": "/deps"},
                            {"name": "HOME", "value": "/tmp"},
                            {"name": "STACK", "value": stack},
                            {"name": "CENSUS_INTERVAL_SECONDS",
                             "value": str(int(loader.get("censusIntervalSeconds", 20)))},
                        ],
                        "ports": [{"name": "metrics", "containerPort": 9100}],
                        "resources": {
                            "requests": {"cpu": "%dm" % round(cpu * 1000),
                                         "memory": "%dMi" % round(memory * 1024)},
                            "limits": {"memory": "%dMi" % round(memory * 2048)}},
                        "volumeMounts": [
                            {"name": "config", "mountPath": "/config", "readOnly": True},
                            {"name": "results", "mountPath": "/results"},
                            {"name": "deps", "mountPath": "/deps"},
                            {"name": "tmp", "mountPath": "/tmp"}],
                    }],
                    "volumes": [
                        {"name": "config", "configMap": {"name": config_name}},
                        {"name": "results",
                         "persistentVolumeClaim": {"claimName": claim_name}},
                        {"name": "deps", "emptyDir": {}},
                        {"name": "tmp", "emptyDir": {}}],
                },
            },
        },
    }


# --------------------------------------------------------------------------
# Prometheus, one per namespace
# --------------------------------------------------------------------------

def ensure_prometheus(namespace, logger):
    scrape = {
        "global": {"scrape_interval": "5s"},
        "scrape_configs": [{
            "job_name": "fhir-workers",
            "kubernetes_sd_configs": [{"role": "pod", "namespaces": {"names": [namespace]}}],
            "relabel_configs": [
                {"source_labels": ["__meta_kubernetes_pod_label_app"], "action": "keep",
                 "regex": "fhir-loader|fhir-benchmark"},
                {"source_labels": ["__meta_kubernetes_pod_ip"], "target_label": "__address__",
                 "replacement": "$1:9100"},
                {"source_labels": ["__meta_kubernetes_pod_label_dataset"],
                 "target_label": "dataset"},
                {"source_labels": ["__meta_kubernetes_pod_label_mode"], "target_label": "mode"},
                {"source_labels": ["__meta_kubernetes_pod_label_benchmark"],
                 "target_label": "benchmark"},
                {"source_labels": ["__meta_kubernetes_pod_label_step"], "target_label": "step"},
                {"source_labels": ["__meta_kubernetes_pod_name"], "target_label": "pod"}],
        }],
    }
    docs = [
        {"apiVersion": "v1", "kind": "ServiceAccount", "metadata": {"name": "prometheus"}},
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
        {"apiVersion": "v1", "kind": "ConfigMap", "metadata": {"name": "prometheus-config"},
         "data": {"prometheus.yml": yaml.safe_dump(scrape, sort_keys=False)}},
        {"apiVersion": "apps/v1", "kind": "Deployment",
         "metadata": {"name": "prometheus", "labels": {"app": "prometheus"}},
         "spec": {"replicas": 1, "selector": {"matchLabels": {"app": "prometheus"}},
                  "template": {"metadata": {"labels": {"app": "prometheus"}},
                               "spec": {"serviceAccountName": "prometheus",
                                        "containers": [{
                                            "name": "prometheus",
                                            "image": "prom/prometheus:v2.55.1",
                                            "args": ["--config.file=/etc/prometheus/prometheus.yml",
                                                     "--storage.tsdb.retention.time=7d"],
                                            "ports": [{"name": "http", "containerPort": 9090}],
                                            "resources": {
                                                "requests": {"cpu": "100m", "memory": "512Mi"},
                                                "limits": {"memory": "2Gi"}},
                                            "volumeMounts": [
                                                {"name": "config",
                                                 "mountPath": "/etc/prometheus"},
                                                {"name": "data", "mountPath": "/prometheus"}]}],
                                        "volumes": [
                                            {"name": "config",
                                             "configMap": {"name": "prometheus-config"}},
                                            {"name": "data", "emptyDir": {}}]}}}},
        {"apiVersion": "v1", "kind": "Service", "metadata": {"name": "prometheus"},
         "spec": {"selector": {"app": "prometheus"},
                  "ports": [{"name": "http", "port": 9090, "targetPort": 9090}]}},
    ]
    for doc in docs:
        doc["metadata"]["namespace"] = namespace
        _apply(doc, namespace)


# --------------------------------------------------------------------------
# Reconciliation
# --------------------------------------------------------------------------

def running_job(namespace, dataset):
    """The dataset's current job, if any, newest first."""
    jobs = _batch().list_namespaced_job(
        namespace, label_selector="dataset=%s" % dataset).items
    if not jobs:
        return None
    jobs.sort(key=lambda j: j.metadata.creation_timestamp, reverse=True)
    return jobs[0]


def job_phase(found):
    if found is None:
        return None
    if found.spec.suspend:
        return "Paused"
    wanted = found.spec.completions or 1
    if (found.status.succeeded or 0) >= wanted:
        return "Complete"
    if (found.status.failed or 0) and not (found.status.active or 0):
        return "Failed"
    return "Active"


def _job_name(name, mode, attempt):
    return "%s-%s-%d" % (name, mode, attempt)


def launch(spec, meta, namespace, name, mode, logger, attempt=0,
           job_name=None):
    config, notes = dataset_config(spec, name, logger)
    loader = spec.get("loader") or {}
    config_name = "%s-config" % name
    claim_name = "%s-results" % name

    for doc in worker_rbac(namespace):
        _apply(doc, namespace)
    for doc in (config_map(config_name, config),
                results_claim(claim_name, int(loader.get("storageGi", 10)))):
        doc["metadata"]["namespace"] = namespace
        kopf.adopt(doc)
        _apply(doc, namespace)
    ensure_prometheus(namespace, logger)

    job_name = job_name or _job_name(name, mode, attempt)
    body = job(job_name, name, namespace, spec, mode, config_name, claim_name)
    body["metadata"]["namespace"] = namespace
    kopf.adopt(body)
    try:
        _batch().create_namespaced_job(namespace, body)
        logger.info("launched %s job %s", mode, job_name)
    except client.ApiException as exc:
        if exc.status != 409:
            raise
        logger.info("job %s already exists", job_name)
    return job_name, notes


@kopf.on.create(GROUP, VERSION, PLURAL, id="own")
def own(spec, namespace, name, patch, logger, **_):
    """Mark a new dataset Pending.

    Datasets deliberately carry no ownerReference to their FhirStack. An owner
    reference with blockOwnerDeletion and a purge finalizer are two mechanisms
    competing over the same teardown ordering, and the cascade cannot be made
    to wait for a purge that needs the server the stack is deleting. The
    stack's own delete handler sequences teardown instead.
    """
    patch.status["phase"] = "Pending"


@kopf.timer(GROUP, VERSION, PLURAL, interval=20, id="reconcile")
def reconcile(spec, meta, status, namespace, name, patch, logger, **_):
    """Close the gap between what the CR says and what the database holds."""
    patch.status["expected"] = expected(spec)

    # Ahead of the stack gate below, which returns early on precisely the
    # states this cares about: a loader writing into a server that has gone
    # produces nothing but failures, and leaving it running was how one job
    # reached 8397 failures against a deleted stack.
    steady_facts = reconciliation.observe_dataset_steady(namespace, name, spec)
    steady = reconciliation.decide(reconciliation.DATASET_STEADY, steady_facts)
    if steady.action is reconciliation.Action.STOP_JOBS:
        logger.info("steady %s: %s -- %s", name, steady.case_id, steady.reason)
        reconciliation.stop_jobs(namespace, steady_facts.load_jobs, logger)
        patch.status["phase"] = "Waiting"
        patch.status["reason"] = steady.reason
        return

    # Loading into a stack that is still starting just burns the Job's
    # backoffLimit on connection-refused.
    stack_name = spec.get("stackRef")
    if stack_name:
        try:
            stack = _custom().get_namespaced_custom_object(
                GROUP, VERSION, namespace, STACK_PLURAL, stack_name)
        except client.ApiException:
            patch.status["phase"] = "Waiting"
            patch.status["reason"] = "stack %s not found" % stack_name
            return
        if (stack.get("status") or {}).get("phase") != "Ready":
            patch.status["phase"] = "Waiting"
            patch.status["reason"] = "stack %s is %s" % (
                stack_name, (stack.get("status") or {}).get("phase", "unknown"))
            return

    found = running_job(namespace, name)
    phase = job_phase(found)

    # Job control: spec.suspend drives the Job's own suspend field.
    if found is not None and bool(spec.get("suspend", False)) != bool(found.spec.suspend):
        _batch().patch_namespaced_job(
            found.metadata.name, namespace,
            {"spec": {"suspend": bool(spec.get("suspend", False))}})
        logger.info("job %s suspend=%s", found.metadata.name, spec.get("suspend"))
        patch.status["phase"] = "Paused" if spec.get("suspend") else "Active"
        return

    if phase == "Active" or phase == "Paused":
        patch.status["phase"] = phase
        patch.status["jobName"] = found.metadata.name
        return

    action, reason = decide(spec, status)
    if action is None:
        patch.status["phase"] = "Ready" if (spec.get("state") or "Present") == "Present" else "Absent"
        patch.status["reason"] = reason
        return

    # A job that finished without closing the gap gets another attempt, up to
    # a limit. Retrying forever on a broken dataset is as bad as giving up on
    # a transient failure.
    attempt = int((status or {}).get("attempt", 0))
    limit = int(spec.get("maxAttempts", 3))
    if found is not None and phase in ("Complete", "Failed"):
        if found.metadata.name == _job_name(name, action, attempt):
            attempt += 1
            if attempt >= limit:
                patch.status["phase"] = "Failed"
                patch.status["attempt"] = attempt
                patch.status["reason"] = (
                    "%s gave up after %d attempts: %s" % (action, attempt, reason))
                return
    patch.status["attempt"] = attempt
    patch.status["reason"] = reason
    job_name, notes = launch(spec, meta, namespace, name, action, logger,
                             attempt)
    patch.status["jobName"] = job_name
    patch.status["phase"] = "Loading" if action == "load" else "Deleting"
    if notes:
        patch.status["codeNotes"] = notes


@kopf.on.delete(GROUP, VERSION, PLURAL, id="purge")
def purge(spec, namespace, name, patch, logger, **_):
    """Take the data with the CR, in the right order.

    Every case, and the reasoning behind each, is in reconciliation.py and in
    reconciliation-cases-SOW.md. This handler only observes, decides, and acts.

    kopf drops the finalizer the moment a deletion handler produces no delay,
    so returning is how a dataset is released and a delayed TemporaryError is
    how it is held. kopf.PermanentError must never reach here: it produces no
    delay either, so it would release the finalizer and delete the CR with its
    data still in the database.
    """
    facts = reconciliation.observe_dataset_teardown(namespace, name, spec)
    decision = reconciliation.decide(reconciliation.DATASET_TEARDOWN, facts)
    action = decision.action
    logger.info("teardown %s: %s -- %s", name, decision.case_id, decision.reason)

    if action is reconciliation.Action.RELEASE:
        return

    patch.status["phase"] = "Deleting"
    patch.status["reason"] = "%s: %s" % (decision.case_id, decision.reason)

    if action is reconciliation.Action.STOP_JOBS:
        reconciliation.stop_jobs(namespace, facts.load_jobs, logger)
        raise kopf.TemporaryError(decision.reason, delay=10)

    if action is reconciliation.Action.START_PURGE:
        patch.status["jobName"] = reconciliation.start_purge(namespace, name, spec, logger)
        raise kopf.TemporaryError(decision.reason, delay=20)

    if action is reconciliation.Action.WAIT:
        raise kopf.TemporaryError(decision.reason, delay=20)

    if action is reconciliation.Action.FAIL:
        patch.status["phase"] = "PurgeFailed"
        logger.error("purge of %s cannot proceed: %s", name, decision.reason)
        raise kopf.TemporaryError(decision.reason, delay=300)

    raise RuntimeError("teardown case %s produced an action purge() cannot perform: %s"
                       % (decision.case_id, action))

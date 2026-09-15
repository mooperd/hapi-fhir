# Created by claude-opus-5
"""Every reconciliation case between FhirDataset and FhirStack, in one file.

The design and the reasoning behind each case are in
perf-experiment/reconciliation-cases-SOW.md. This file is the executable form
of section 5 of that document.

Layout:

    SECTION 1  Vocabulary   enums
    SECTION 2  Facts        frozen dataclasses, pure data, no clients
    SECTION 3  Case tables  DATASET_TEARDOWN, STACK_TEARDOWN, DATASET_STEADY
    SECTION 4  decide()     first match wins, no default, raises on no match
    SECTION 5  Observation  the only I/O in the file
    SECTION 6  Effects      what the chosen action actually does

Sections 1-4 are pure and are what test_reconciliation.py exercises.

Two rules hold throughout, and both exist because of how kopf releases a
finalizer -- it drops it the moment a deletion handler produces no delay
(kopf processing.py:342-345):

  * Nothing is caught except a 404 used as an existence probe, where the 404
    is the observation rather than an error. Everything else propagates, gets
    logged by kopf with a traceback, and is retried every 60 seconds forever.
  * kopf.PermanentError is never raised, directly or indirectly. In a deletion
    handler it does not stop anything -- it releases the finalizer and lets
    the object be deleted with its data intact.
"""

import dataclasses
import enum
import os
from typing import Callable

import kopf
from kubernetes import client

GROUP = "perf.fhir"
VERSION = "v1alpha1"
PLURAL = "fhirdatasets"
STACK_PLURAL = "fhirstacks"

HAPI_SERVICE = "hapi-fhir"
LOADER_IMAGE = os.environ.get("LOADER_IMAGE", "python:3.12-slim")

# The purge Job's name is fixed rather than attempt-numbered, so a purge that
# is already running is recognised as the same purge on the next pass.
PURGE_SUFFIX = "-purge"

# Low, because a purge is only ever started once the endpoint is observed
# SERVING. A real failure should reach the case table in minutes, not in the
# ~70 minutes that the loader's 600s server wait times a backoffLimit of 6.
PURGE_BACKOFF_LIMIT = 2

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
# SECTION 1  Vocabulary
# --------------------------------------------------------------------------
#
# StackState describes the FhirStack custom resource -- the declaration.
# EndpointState describes the running hapi-fhir Service and its Endpoints --
# the thing declared. They are separate axes because they desync: the CR can
# be gone while Deployments still run, and the CR can read Ready while
# hapi-fhir is mid-rollout and not serving.

class StackState(enum.Enum):
    ABSENT = "absent"
    TERMINATING = "terminating"
    NOT_READY = "not-ready"
    READY = "ready"


class EndpointState(enum.Enum):
    SERVING = "serving"
    PRESENT_NOT_SERVING = "present-not-serving"
    ABSENT = "absent"


class Action(enum.Enum):
    RELEASE = "release"
    STOP_JOBS = "stop-jobs"
    START_PURGE = "start-purge"
    WAIT = "wait"
    FAIL = "fail"
    PROCEED = "proceed"
    DELETE_DATASETS = "delete-datasets"


GONE = (StackState.ABSENT, StackState.TERMINATING)


# --------------------------------------------------------------------------
# SECTION 2  Facts
# --------------------------------------------------------------------------

@dataclasses.dataclass(frozen=True)
class DatasetTeardownFacts:
    namespace_terminating: bool
    purge_on_delete: bool
    stack: StackState
    endpoint: EndpointState
    load_jobs: tuple            # every Job for this dataset except the purge Job
    load_pods: int              # pods belonging to those Jobs
    purge_job: str              # None | Active | Complete | Failed | Paused


@dataclasses.dataclass(frozen=True)
class StackTeardownFacts:
    protected: bool
    namespace_terminating: bool
    datasets: tuple             # ((name, is_terminating), ...)


@dataclasses.dataclass(frozen=True)
class DatasetSteadyFacts:
    stack: StackState
    endpoint: EndpointState
    load_jobs: tuple


@dataclasses.dataclass(frozen=True)
class Case:
    id: str
    when: Callable
    action: Action
    reason: Callable


@dataclasses.dataclass(frozen=True)
class Decision:
    case_id: str
    action: Action
    reason: str


# --------------------------------------------------------------------------
# SECTION 3  Case tables
# --------------------------------------------------------------------------

DATASET_TEARDOWN = (
    # The namespace is going. A finalizer here holds the whole namespace in
    # Terminating, which is worse than orphaned rows in a database that is
    # being destroyed anyway.
    Case("namespace-terminating",
         lambda f: f.namespace_terminating,
         Action.RELEASE,
         lambda f: "namespace is Terminating; its data goes with it"),

    Case("purge-disabled",
         lambda f: not f.purge_on_delete,
         Action.RELEASE,
         lambda f: "purgeOnDelete=false; leaving the data in place"),

    # No server to purge from and none is coming. The condition is "not
    # serving" rather than "absent" so a leftover Service with no ready
    # endpoints does not trap the dataset.
    Case("server-gone-with-stack",
         lambda f: f.endpoint != EndpointState.SERVING and f.stack in GONE,
         Action.RELEASE,
         lambda f: "stack is %s and hapi-fhir is %s; nothing to purge from"
                   % (f.stack.value, f.endpoint.value)),

    # Stop the loader before deleting. run_delete polls each type's count down
    # to zero; a live loader re-creating tagged resources means that count may
    # never reach zero, and the Job burns DELETE_TIMEOUT and fails.
    Case("load-jobs-running",
         lambda f: bool(f.load_jobs),
         Action.STOP_JOBS,
         lambda f: "stopping %d job(s) before purging: %s"
                   % (len(f.load_jobs), ", ".join(f.load_jobs))),

    Case("load-pods-draining",
         lambda f: f.load_pods > 0,
         Action.WAIT,
         lambda f: "%d loader pod(s) still draining" % f.load_pods),

    # A live-or-provisioning stack whose hapi-fhir is not currently serving:
    # rollout, restart, Elasticsearch still yellow. No timeout, by design.
    Case("endpoint-not-serving",
         lambda f: f.endpoint != EndpointState.SERVING,
         Action.WAIT,
         lambda f: "stack is %s but hapi-fhir is %s" % (f.stack.value, f.endpoint.value)),

    Case("purge-absent",
         lambda f: f.purge_job is None,
         Action.START_PURGE,
         lambda f: "no purge job yet; starting one"),

    Case("purge-active",
         lambda f: f.purge_job == "Active",
         Action.WAIT,
         lambda f: "purge job is running"),

    Case("purge-paused",
         lambda f: f.purge_job == "Paused",
         Action.FAIL,
         lambda f: "purge job is suspended and will never finish; "
                   "resume it, or set spec.purgeOnDelete=false to release this dataset"),

    Case("purge-failed",
         lambda f: f.purge_job == "Failed",
         Action.FAIL,
         lambda f: "purge job failed; read its pod logs. "
                   "Set spec.purgeOnDelete=false to release this dataset and orphan its data"),

    Case("purge-complete",
         lambda f: f.purge_job == "Complete",
         Action.RELEASE,
         lambda f: "purge complete"),
)


STACK_TEARDOWN = (
    Case("stack-protected",
         lambda f: f.protected,
         Action.WAIT,
         lambda f: "stack is protected and will not be deleted. Set spec.protected=false "
                   "to allow it -- this destroys its PVCs and everything on them"),

    Case("namespace-terminating",
         lambda f: f.namespace_terminating,
         Action.PROCEED,
         lambda f: "namespace is Terminating; everything in it goes together"),

    # The ordering guarantee. guard() holds the stack's finalizer while this
    # runs, so hapi-fhir stays up and the datasets can purge against it.
    Case("datasets-need-deleting",
         lambda f: any(not terminating for _, terminating in f.datasets),
         Action.DELETE_DATASETS,
         lambda f: "deleting %d dataset(s) before the stack: %s"
                   % (sum(1 for _, t in f.datasets if not t),
                      ", ".join(n for n, t in f.datasets if not t))),

    Case("datasets-terminating",
         lambda f: bool(f.datasets),
         Action.WAIT,
         lambda f: "waiting for %d dataset(s) to finish purging: %s"
                   % (len(f.datasets), ", ".join(n for n, _ in f.datasets))),

    Case("no-datasets",
         lambda f: not f.datasets,
         Action.PROCEED,
         lambda f: "no datasets left; owner references take the stack with it"),
)


DATASET_STEADY = (
    # A loader hammering a server that no longer exists produces nothing but
    # failures and noise. Both facts are required, for the same reason
    # server-gone-with-stack requires both: an adopted or hand-installed
    # hapi-fhir has no FhirStack CR, and a serving endpoint is still a server.
    Case("stack-gone-stop-loading",
         lambda f: (f.stack in GONE and f.endpoint != EndpointState.SERVING
                    and bool(f.load_jobs)),
         Action.STOP_JOBS,
         lambda f: "stack is %s and hapi-fhir is %s; stopping %s"
                   % (f.stack.value, f.endpoint.value, ", ".join(f.load_jobs))),

    Case("carry-on",
         lambda f: True,
         Action.PROCEED,
         lambda f: "nothing for the teardown tables to do"),
)


# --------------------------------------------------------------------------
# SECTION 4  decide()
# --------------------------------------------------------------------------

def decide(table, facts):
    """First matching case wins. No default: an unmatched fact set is a hole
    in the table and must surface as a crash, not be absorbed by an else."""
    for case in table:
        if case.when(facts):
            return Decision(case_id=case.id, action=case.action, reason=case.reason(facts))
    raise RuntimeError("no reconciliation case matched: %r" % (facts,))


# --------------------------------------------------------------------------
# SECTION 5  Observation -- the only I/O in this file
# --------------------------------------------------------------------------

def stack_state(namespace, stack_name):
    try:
        stack = _custom().get_namespaced_custom_object(
            GROUP, VERSION, namespace, STACK_PLURAL, stack_name)
    except client.ApiException as exc:
        if exc.status != 404:
            raise
        return StackState.ABSENT
    if (stack.get("metadata") or {}).get("deletionTimestamp"):
        return StackState.TERMINATING
    if (stack.get("status") or {}).get("phase") != "Ready":
        return StackState.NOT_READY
    return StackState.READY


def endpoint_state(namespace):
    """Judged from Kubernetes objects, never over HTTP.

    FHIR_BASE_URL is the cluster-internal name hapi-fhir.<ns>.svc, which an
    operator running outside the cluster cannot resolve.
    """
    try:
        _core().read_namespaced_service(HAPI_SERVICE, namespace)
    except client.ApiException as exc:
        if exc.status != 404:
            raise
        return EndpointState.ABSENT
    try:
        endpoints = _core().read_namespaced_endpoints(HAPI_SERVICE, namespace)
    except client.ApiException as exc:
        if exc.status != 404:
            raise
        return EndpointState.PRESENT_NOT_SERVING
    ready = any((subset.addresses or []) for subset in (endpoints.subsets or []))
    return EndpointState.SERVING if ready else EndpointState.PRESENT_NOT_SERVING


def namespace_terminating(namespace):
    try:
        found = _core().read_namespace(namespace)
    except client.ApiException as exc:
        if exc.status != 404:
            raise
        return True
    return found.status.phase == "Terminating"


def purge_job_name(dataset):
    return dataset + PURGE_SUFFIX


def jobs_for(namespace, dataset):
    """(other job names, purge job phase or None).

    Wider than "mode != delete": every Job for this dataset other than the
    purge Job counts as one to stop. A stray delete Job from a state=Absent
    reconcile races the purge for the same rows otherwise.
    """
    purge_name = purge_job_name(dataset)
    jobs = _batch().list_namespaced_job(
        namespace, label_selector="dataset=%s" % dataset).items
    others = tuple(job.metadata.name for job in jobs if job.metadata.name != purge_name)
    purge = next((job for job in jobs if job.metadata.name == purge_name), None)
    return others, job_phase(purge)


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


def live_pods(namespace, dataset):
    """Pods of this dataset's non-purge Jobs that have not finished."""
    purge_name = purge_job_name(dataset)
    pods = _core().list_namespaced_pod(
        namespace, label_selector="dataset=%s" % dataset).items
    return sum(
        1 for pod in pods
        if pod.status.phase in ("Pending", "Running")
        and (pod.metadata.labels or {}).get("job-name") != purge_name
        and (pod.metadata.labels or {}).get("batch.kubernetes.io/job-name") != purge_name)


def datasets_in(namespace):
    found = _custom().list_namespaced_custom_object(GROUP, VERSION, namespace, PLURAL)
    return tuple(
        (item["metadata"]["name"], bool(item["metadata"].get("deletionTimestamp")))
        for item in found.get("items", []))


def observe_dataset_teardown(namespace, name, spec):
    others, purge = jobs_for(namespace, name)
    return DatasetTeardownFacts(
        namespace_terminating=namespace_terminating(namespace),
        purge_on_delete=bool(spec.get("purgeOnDelete", True)),
        stack=stack_state(namespace, spec.get("stackRef") or namespace),
        endpoint=endpoint_state(namespace),
        load_jobs=others,
        load_pods=live_pods(namespace, name),
        purge_job=purge)


def observe_stack_teardown(namespace, spec):
    return StackTeardownFacts(
        protected=bool(spec.get("protected")),
        namespace_terminating=namespace_terminating(namespace),
        datasets=datasets_in(namespace))


def observe_dataset_steady(namespace, name, spec):
    others, _ = jobs_for(namespace, name)
    return DatasetSteadyFacts(
        stack=stack_state(namespace, spec.get("stackRef") or namespace),
        endpoint=endpoint_state(namespace),
        load_jobs=others)


# --------------------------------------------------------------------------
# SECTION 6  Effects
# --------------------------------------------------------------------------

def stop_jobs(namespace, job_names, logger):
    for job_name in job_names:
        _batch().delete_namespaced_job(
            job_name, namespace,
            body=client.V1DeleteOptions(propagation_policy="Background"))
        logger.info("deleted job %s/%s", namespace, job_name)


def delete_datasets(namespace, names, logger):
    for name in names:
        _custom().delete_namespaced_custom_object(GROUP, VERSION, namespace, PLURAL, name)
        logger.info("deleted FhirDataset %s/%s", namespace, name)


def start_purge(namespace, dataset, spec, logger):
    """Create the delete Job directly.

    Deliberately not datasets.launch(): that path resolves SNOMED codes a
    delete does not need, re-applies RBAC and Prometheus onto a terminating
    object, and reaches _validate(), whose kopf.PermanentError would release
    the finalizer and delete the CR with all of its data still in the
    database.

    The ConfigMap and the results PVC already exist by the time a teardown
    runs. They are referenced, never re-applied.
    """
    job_name = purge_job_name(dataset)
    body = {
        "apiVersion": "batch/v1", "kind": "Job",
        "metadata": {"name": job_name, "namespace": namespace,
                     "labels": {"app": "fhir-loader", "dataset": dataset, "mode": "delete"}},
        "spec": {
            "completionMode": "Indexed",
            "completions": 1,
            "parallelism": 1,
            "backoffLimit": PURGE_BACKOFF_LIMIT,
            "template": {
                "metadata": {"labels": {"app": "fhir-loader", "dataset": dataset,
                                        "mode": "delete"}},
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
                            {"name": "MODE", "value": "delete"},
                            {"name": "FHIR_BASE_URL",
                             "value": "http://%s.%s.svc:8080/fhir" % (HAPI_SERVICE, namespace)},
                            {"name": "DATASET_CONFIG", "value": "/config/dataset.json"},
                            {"name": "RESULTS_DIR", "value": "/results/%s" % dataset},
                            {"name": "PARALLELISM", "value": "1"},
                            {"name": "DATASET_NAME", "value": dataset},
                            {"name": "POD_NAMESPACE", "value": namespace},
                            {"name": "PYTHONPATH", "value": "/deps"},
                            {"name": "HOME", "value": "/tmp"},
                            # A purge polls one type at a time; this is what
                            # keeps status.observed moving across all three
                            # while it drains.
                            {"name": "CENSUS_INTERVAL_SECONDS",
                             "value": str(int((spec.get("loader") or {})
                                              .get("censusIntervalSeconds", 20)))},
                        ],
                        "ports": [{"name": "metrics", "containerPort": 9100}],
                        "volumeMounts": [
                            {"name": "config", "mountPath": "/config", "readOnly": True},
                            {"name": "results", "mountPath": "/results"},
                            {"name": "deps", "mountPath": "/deps"},
                            {"name": "tmp", "mountPath": "/tmp"}],
                    }],
                    "volumes": [
                        {"name": "config", "configMap": {"name": "%s-config" % dataset}},
                        {"name": "results",
                         "persistentVolumeClaim": {"claimName": "%s-results" % dataset}},
                        {"name": "deps", "emptyDir": {}},
                        {"name": "tmp", "emptyDir": {}}],
                },
            },
        },
    }
    kopf.adopt(body)
    _batch().create_namespaced_job(namespace, body)
    logger.info("launched purge job %s/%s", namespace, job_name)
    return job_name

# Created by claude-opus-5
"""FhirBenchmark: a step-advancing state machine, not a convergence loop.

A FhirStack and a FhirDataset describe a state the world should be in, and the
operator drives towards it, repeatedly and idempotently. A benchmark is not a
state. It is an ordered sequence of one-shot, side-effecting, non-idempotent
events whose results are only meaningful in the order they occurred, so this
handler asks "which step index is next?" and never "does the world match
spec?". The compromises that follow from that are C1-C6 in
FhirBenchmarkSOW.md section 2, and each one is named where it bites.

Layout follows reconciliation.py:

    SECTION 1  Vocabulary   enums
    SECTION 2  Facts        frozen dataclasses, pure data, no clients
    SECTION 3  Case tables  BENCHMARK_STEADY, BENCHMARK_TEARDOWN
    SECTION 4  decide()     borrowed from reconciliation.py
    SECTION 5  Steps        one function per step type
    SECTION 6  Handlers     kopf
"""

import dataclasses
import datetime
import json
import os

import kopf
from kubernetes import client

import binding
import catalogue
import control
import datasets
import gcs
import reconciliation
import stats

GROUP = "perf.fhir"
VERSION = "v1alpha1"
PLURAL = "fhirbenchmarks"

# Baked into the operator image at build time as an immutable digest, so an
# operator can only ever launch the worker it was built alongside. Overridable
# for dev, where the worker is built locally.
WORKER_IMAGE = os.environ.get("WORKER_IMAGE", "ghcr.io/mooperd/fhir-worker:latest")
SERVICE_ACCOUNT = "fhir-benchmark"

TERMINAL = ("Complete", "Failed", "Aborted")

# The only hapi.fhir setting whose value the preflight asserts. HAPI serves an
# identical repeated search from its own Search cache for 60 s by default, so
# without this every "hot" number measures the cache and nothing else.
RESULT_CACHE_KEY = "reuse_cached_search_results_millis"

# Requirements each step type knows how to evaluate. A plan that asks for one
# that is not here stops the run by name rather than passing silently: an
# unevaluated precondition reported as met is the failure mode this whole
# instrument exists to avoid.
ASSERT_REQUIREMENTS = ("stackReady", "datasetReady", "exclusiveLease",
                       "searchResultCacheDisabled", "analyzedSinceLoad")
SETTLE_REQUIREMENTS = ("deploymentsReady", "endpointServing")

_dyn = None


def init(dyn):
    global _dyn
    _dyn = dyn
    control.init(dyn)


def _batch():
    return client.BatchV1Api(_dyn().client)


def _now():
    return datetime.datetime.now(datetime.timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")


# --------------------------------------------------------------------------
# SECTION 1  Vocabulary
# --------------------------------------------------------------------------

# --------------------------------------------------------------------------
# SECTION 2  Facts
# --------------------------------------------------------------------------

@dataclasses.dataclass(frozen=True)
class BenchmarkSteadyFacts:
    requested_run: int
    recorded_run: int
    phase: str
    state: str
    steps: int
    current: int
    step_running: bool
    has_job: bool               # the current step launched a Job
    job: str                    # None | Active | Complete | Failed | Paused


@dataclasses.dataclass(frozen=True)
class BenchmarkTeardownFacts:
    namespace_terminating: bool
    jobs: tuple
    configured: bool
    leased: bool


# --------------------------------------------------------------------------
# SECTION 3  Case tables
# --------------------------------------------------------------------------

Case = reconciliation.Case

# Vocabulary local to this table. reconciliation.Action carries the verbs the
# dataset and stack tables need; a benchmark needs its own.
RESET = "reset"
VALIDATE = "validate"
LEASE = "lease"
ADVANCE = "advance"
AWAIT_JOB = "await-job"
INGEST = "ingest"
FAIL_STEP = "fail-step"
FINISH = "finish"
ABORT = "abort"
IDLE = "idle"


BENCHMARK_STEADY = (
    # C4. An operator restart mid-run must not silently repeat measurements
    # and append them to the same series, so a new runId is the only way back
    # in and it starts a fresh journal.
    Case("run-id-changed",
         lambda f: f.requested_run != f.recorded_run,
         RESET,
         lambda f: "spec.runId is %d, status.runId is %d; starting a new run"
                   % (f.requested_run, f.recorded_run)),

    Case("aborting",
         lambda f: f.state == "Abort" and f.phase not in TERMINAL,
         ABORT,
         lambda f: "spec.state is Abort; remaining steps are Skipped"),

    Case("terminal",
         lambda f: f.phase in TERMINAL,
         IDLE,
         lambda f: "run is %s; increment spec.runId to run again" % f.phase),

    Case("pending",
         lambda f: f.phase in ("", "Pending"),
         VALIDATE,
         lambda f: "pinning the catalogue and binding placeholders"),

    Case("validated",
         lambda f: f.phase == "Validating",
         LEASE,
         lambda f: "taking the exclusive lease on the stack"),

    # spec.state: Hold lets the current step finish and starts no further one.
    Case("held-between-steps",
         lambda f: f.state == "Hold" and not f.step_running,
         IDLE,
         lambda f: "spec.state is Hold; no further step will start"),

    Case("plan-exhausted",
         lambda f: f.phase in ("Running", "Leasing") and f.current >= f.steps,
         FINISH,
         lambda f: "all %d steps done; reporting" % f.steps),

    Case("job-failed",
         lambda f: f.job == "Failed",
         FAIL_STEP,
         lambda f: "the step's Job failed; read its pod logs. No measurement is retried"),

    Case("job-active",
         lambda f: f.job in ("Active", "Paused"),
         AWAIT_JOB,
         lambda f: "the step's Job is %s" % f.job),

    # The Job was created but is not visible yet. Re-entering _start would
    # rewrite the journal entry's startedAt, so this waits instead.
    Case("job-not-visible",
         lambda f: f.has_job and f.job is None,
         AWAIT_JOB,
         lambda f: "the step's Job has been created but is not visible yet"),

    Case("job-complete",
         lambda f: f.job == "Complete",
         INGEST,
         lambda f: "the step's Job finished; ingesting its shards"),

    Case("advance",
         lambda f: f.phase in ("Leasing", "Running", "Reporting"),
         ADVANCE,
         lambda f: "starting step %d of %d" % (f.current + 1, f.steps)),

    Case("idle",
         lambda f: True,
         IDLE,
         lambda f: "phase %s has nothing to advance" % (f.phase or "unset")),
)


BENCHMARK_TEARDOWN = (
    Case("namespace-terminating",
         lambda f: f.namespace_terminating,
         reconciliation.Action.RELEASE,
         lambda f: "namespace is Terminating; everything in it goes together"),

    Case("jobs-running",
         lambda f: bool(f.jobs),
         reconciliation.Action.STOP_JOBS,
         lambda f: "stopping %d benchmark job(s) before releasing: %s"
                   % (len(f.jobs), ", ".join(f.jobs))),

    Case("release",
         lambda f: True,
         reconciliation.Action.RELEASE,
         lambda f: "nothing left running; restoring the stack and releasing"),
)


decide = reconciliation.decide


# --------------------------------------------------------------------------
# SECTION 5  Steps
# --------------------------------------------------------------------------

def plan_of(spec):
    plan = spec.get("plan") or []
    if not plan:
        raise kopf.PermanentError(
            "spec.plan is empty. A benchmark is an ordered sequence of events, not a "
            "state to converge on, so there is nothing to infer")
    ids = [step["id"] for step in plan]
    if len(set(ids)) != len(ids):
        raise kopf.PermanentError("spec.plan has duplicate step ids: %s" % ", ".join(ids))
    return plan


def defaults_of(spec):
    given = spec.get("defaults") or {}
    return {
        "concurrency": int(given.get("concurrency", 1)),
        "repeats": int(given.get("repeats", 30)),
        "warmup": int(given.get("warmup", 5)),
        "timeoutSeconds": int(given.get("timeoutSeconds", 120)),
        "bindingMode": given.get("bindingMode", "pin"),
        "tolerance": {
            "rowCountPct": float((given.get("tolerance") or {}).get("rowCountPct", 0)),
            "coldReadRatioFloor": float(
                (given.get("tolerance") or {}).get("coldReadRatioFloor", 0.8)),
        },
    }


def validate(spec, namespace, name, logger):
    """Everything that must be true before a single query is issued.

    Any failure here is permanent and produces no measurements. That is the
    primary guard against the no-fallbacks rule eroding.
    """
    plan = plan_of(spec)
    base = defaults_of(spec)
    cases, digest, catalogue_name = catalogue.select(spec.get("catalogue"))

    for step in plan:
        if step["step"] == "assert":
            unknown = [k for k in (step.get("require") or {})
                       if k not in ASSERT_REQUIREMENTS]
            if unknown:
                raise kopf.PermanentError(
                    "assert step %s requires %s, which this operator does not evaluate. "
                    "Known: %s" % (step["id"], ", ".join(sorted(unknown)),
                                   ", ".join(ASSERT_REQUIREMENTS)))
        if step["step"] == "settle":
            unknown = [k for k in (step.get("require") or {})
                       if k not in SETTLE_REQUIREMENTS]
            if unknown:
                raise kopf.PermanentError(
                    "settle step %s requires %s, which this operator does not evaluate. "
                    "Known: %s" % (step["id"], ", ".join(sorted(unknown)),
                                   ", ".join(SETTLE_REQUIREMENTS)))

    control.require_stack_ready(namespace, spec["stackRef"])
    dataset = control.require_dataset_ready(namespace, spec["datasetRef"])
    config = control.dataset_config(namespace, spec["datasetRef"])

    seed = int(config.get("seed", 0))
    bindings = binding.bind(config, base["bindingMode"], seed, 0)
    for case in cases:
        binding.render(case, bindings)
        binding.predict(case, bindings, config,
                        (dataset.get("status") or {}).get("expected") or {})

    logger.info("validated %s/%s: %d cases from catalogue %s %s",
                namespace, name, len(cases), catalogue_name, digest)
    return {
        "catalogueName": catalogue_name,
        "catalogueRevision": digest,
        "caseCount": len(cases),
        "bindings": {k: str(v) for k, v in sorted(bindings.items())},
        "seed": seed,
        "datasetLoadedAt": (dataset.get("status") or {}).get("observedAt"),
        "steps": len(plan),
    }


def _step_config(spec, step, step_index, name, namespace, status):
    """The benchmark.json a worker Job is given. Everything is pre-resolved."""
    base = defaults_of(spec)
    cases, digest, _ = catalogue.select(spec.get("catalogue"))
    config = control.dataset_config(namespace, spec["datasetRef"])
    expected = (control.dataset(namespace, spec["datasetRef"]).get("status")
                or {}).get("expected") or {}

    repeats = int(step.get("repeats", base["repeats"]))
    warmup = int(step.get("warmup", base["warmup"]))
    mode = step.get("bindingMode", base["bindingMode"])
    seed = int(status.get("seed", config.get("seed", 0)))

    # pin binds once for the whole step; rotate draws a fresh, unseen value per
    # repetition so a cold-ish measurement gets n>1 without one restart per
    # sample. Warm-up repetitions get their own bindings too, so a warm-up
    # never pre-reads the pages the first measured repetition will touch.
    total = warmup + repeats
    sets = [binding.bind(config, mode, seed, rep)
            for rep in range(total if mode == "rotate" else 1)]

    wire = []
    for case in cases:
        predicted = binding.predict(case, sets[0], config, expected)
        wire.append({"id": case["id"], "family": case["family"],
                     "method": case["method"], "path": case["path"],
                     "query": case["query"], "expectEngine": case["expectEngine"],
                     "predict": predicted})

    return {
        "name": name, "namespace": namespace,
        "runId": int(status.get("runId", 1)),
        "stepIndex": step_index, "stepId": step["id"], "stepType": step["step"],
        "cacheLabel": step.get("cacheLabel") or step["id"],
        "repeats": repeats, "warmup": warmup,
        "timeoutSeconds": int(step.get("timeoutSeconds", base["timeoutSeconds"])),
        "bindingMode": mode, "seed": seed,
        "tolerance": base["tolerance"],
        "relations": step.get("relations") or [],
        "require": step.get("require") or {},
        "datasetLoadedAt": status.get("datasetLoadedAt"),
        "catalogueRevision": digest,
        "bindingSets": sets,
        "cases": wire,
    }


def job_name(name, run_id, step_index):
    return "bm-%s-%d-%d" % (name, run_id, step_index)


def _worker_rbac(namespace):
    """Workers patch their FhirBenchmark. Results go to GCS, not the API server.

    The configmaps grant this Role used to carry is gone with the ConfigMap
    transport it existed for: a worker now writes to two signed URLs and needs
    no write access to the cluster at all.
    """
    return [
        {"apiVersion": "v1", "kind": "ServiceAccount",
         "metadata": {"name": SERVICE_ACCOUNT, "namespace": namespace}},
        {"apiVersion": "rbac.authorization.k8s.io/v1", "kind": "Role",
         "metadata": {"name": SERVICE_ACCOUNT, "namespace": namespace},
         "rules": [
             {"apiGroups": [GROUP], "resources": ["fhirbenchmarks", "fhirbenchmarks/status"],
              "verbs": ["get", "list", "patch", "update"]}]},
        {"apiVersion": "rbac.authorization.k8s.io/v1", "kind": "RoleBinding",
         "metadata": {"name": SERVICE_ACCOUNT, "namespace": namespace},
         "roleRef": {"apiGroup": "rbac.authorization.k8s.io", "kind": "Role",
                     "name": SERVICE_ACCOUNT},
         "subjects": [{"kind": "ServiceAccount", "name": SERVICE_ACCOUNT,
                       "namespace": namespace}]},
    ]


def _apply(doc, namespace):
    resource = _dyn().resources.get(api_version=doc["apiVersion"], kind=doc["kind"])
    _dyn().server_side_apply(resource=resource, body=doc, namespace=namespace,
                             field_manager="fhir-operator", force_conflicts=True)


def _config_secret(config_name, step_config):
    """A Secret, not a ConfigMap: this now carries the shard's signed URLs.

    They are bearer credentials with a TTL. Nothing about the mount changes;
    the resource kind is the only difference, and it keeps short-lived write
    capability out of something people routinely dump with `kubectl get -o
    yaml`. The worker's code is no longer shipped here at all -- it is in the
    image, built by CI.
    """
    return {
        "apiVersion": "v1", "kind": "Secret",
        "metadata": {"name": config_name},
        "type": "Opaque",
        "stringData": {"benchmark.json": json.dumps(step_config)},
    }


def _shard_urls(namespace, name, run_id, step_index, shard):
    """The two objects one shard may write, and nothing else."""
    prefix = gcs.step_prefix(namespace, name, run_id, step_index)
    body = "%s/shard-%d.ndjson" % (prefix, shard)
    done = "%s/shard-%d.done.json" % (prefix, shard)
    return {
        "shardUrl": gcs.upload_url(body, content_type="application/x-ndjson"),
        "doneUrl": gcs.upload_url(done, content_type="application/json"),
        "shardUri": gcs.uri(body),
        "doneUri": gcs.uri(done),
    }


def _job(name, namespace, spec, step, step_index, run_id, config_name):
    base = defaults_of(spec)
    shards = int(step.get("concurrency", base["concurrency"])) \
        if step["step"] == "measure" else 1
    worker = spec.get("worker") or {}
    cpu = float(worker.get("cpu", 1))
    memory = float(worker.get("memory", 2))
    labels = {"app": "fhir-benchmark", "benchmark": name,
              "run": str(run_id), "step": str(step_index)}

    return {
        "apiVersion": "batch/v1", "kind": "Job",
        "metadata": {"name": job_name(name, run_id, step_index), "labels": labels},
        "spec": {
            "completionMode": "Indexed",
            "completions": shards,
            "parallelism": shards,
            # No retry. A measurement is never re-issued: a second attempt
            # runs against a cache the first attempt warmed, which is a
            # different measurement wearing the first one's label.
            "backoffLimit": 0,
            # No ttlSecondsAfterFinished: a finished Job that disappears looks
            # to a reconciler like a Job that never ran.
            "template": {
                "metadata": {"labels": labels},
                "spec": {
                    "restartPolicy": "Never",
                    "serviceAccountName": SERVICE_ACCOUNT,
                    "containers": [{
                        "name": "worker",
                        "image": WORKER_IMAGE,
                        # No shell, no pip, no source from a ConfigMap. The
                        # image is built by CI and pinned by digest, so the
                        # code that runs is the code that was reviewed.
                        "command": ["python", "/app/runner.py"],
                        "env": [
                            {"name": "BENCHMARK_CONFIG", "value": "/config/benchmark.json"},
                            {"name": "FHIR_BASE_URL",
                             "value": "http://hapi-fhir.%s.svc:8080/fhir" % namespace},
                            # Staging only. The durable copy is the object
                            # this worker PUTs to its signed URL.
                            {"name": "RESULTS_DIR", "value": "/results"},
                            {"name": "PARALLELISM", "value": str(shards)},
                            {"name": "POD_NAMESPACE", "value": namespace},
                            {"name": "STACK", "value": spec["stackRef"]},
                            {"name": "PG_HOST", "value": "hapi-fhir-db"},
                            {"name": "ES_BASE_URL",
                             "value": "http://hapi-fhir-es.%s.svc:9200" % namespace},
                            {"name": "HOME", "value": "/tmp"},
                            {"name": "NODE_NAME",
                             "valueFrom": {"fieldRef": {"fieldPath": "spec.nodeName"}}},
                            {"name": "POSTGRES_PASSWORD",
                             "valueFrom": {"secretKeyRef": {"name": "hapi-fhir-db",
                                                            "key": "POSTGRES_PASSWORD"}}},
                        ],
                        "ports": [{"name": "metrics", "containerPort": 9100}],
                        "resources": {
                            "requests": {"cpu": "%dm" % round(cpu * 1000),
                                         "memory": "%dMi" % round(memory * 1024)},
                            "limits": {"memory": "%dMi" % round(memory * 2048)}},
                        "volumeMounts": [
                            {"name": "config", "mountPath": "/config", "readOnly": True},
                            {"name": "results", "mountPath": "/results"},
                            {"name": "tmp", "mountPath": "/tmp"}],
                    }],
                    "volumes": [
                        {"name": "config", "secret": {"secretName": config_name}},
                        {"name": "results", "emptyDir": {}},
                        {"name": "tmp", "emptyDir": {}}],
                },
            },
        },
    }


def launch(spec, namespace, name, step, step_index, status, logger):
    run_id = int(status.get("runId", 1))
    config_name = "%s-cfg" % job_name(name, run_id, step_index)
    base = defaults_of(spec)
    count = int(step.get("concurrency", base["concurrency"])) \
        if step["step"] == "measure" else 1

    for doc in _worker_rbac(namespace):
        _apply(doc, namespace)
    # Same per-namespace Prometheus the loader uses; it keeps pods labelled
    # app=fhir-benchmark as well as app=fhir-loader.
    datasets.ensure_prometheus(namespace, logger)

    # One signed URL pair per shard, keyed by completion index; a worker takes
    # its own pair by JOB_COMPLETION_INDEX. The pairs authorise exactly the two
    # objects this step will produce, and expire -- so even the whole Secret
    # grants nothing outside this step of this run.
    config = _step_config(spec, step, step_index, name, namespace, status)
    config["results"] = {str(i): _shard_urls(namespace, name, run_id, step_index, i)
                         for i in range(count)}
    doc = _config_secret(config_name, config)
    doc["metadata"]["namespace"] = namespace
    kopf.adopt(doc)
    _apply(doc, namespace)

    body = _job(name, namespace, spec, step, step_index, run_id, config_name)
    body["metadata"]["namespace"] = namespace
    kopf.adopt(body)
    try:
        _batch().create_namespaced_job(namespace, body)
    except client.ApiException as exc:
        if exc.status != 409:
            raise
    logger.info("launched %s job %s", step["step"], body["metadata"]["name"])
    return body["metadata"]["name"]


def find_job(namespace, name, run_id, step_index):
    try:
        found = _batch().read_namespaced_job(job_name(name, run_id, step_index), namespace)
    except client.ApiException as exc:
        if exc.status != 404:
            raise
        return None
    return found


def markers(namespace, name, run_id, step_index):
    """The completion markers written so far for one step.

    A marker exists only after its ndjson body was accepted, so counting these
    is how the operator knows a shard is done. Reading them is cheap; the
    bodies are not read until every shard has reported.
    """
    prefix = gcs.step_prefix(namespace, name, run_id, step_index)
    return gcs.list_prefix(prefix + "/", suffix=".done.json")


def shards(namespace, name, run_id, step_index):
    """Every published shard result for one step, bodies included.

    Shape is unchanged from when this read ConfigMaps: one dict per shard with
    its records inline, so _ingest and _cold_read_ratio are untouched.
    """
    out = []
    for path in markers(namespace, name, run_id, step_index):
        marker = gcs.read_json(path)
        body = path[: -len(".done.json")] + ".ndjson"
        try:
            records = gcs.read_ndjson(body)
        except Exception as exc:              # noqa: BLE001 - named, not swallowed
            raise kopf.PermanentError(
                "shard marker %s exists but its body %s could not be read (%s: %s). "
                "The marker is written only after the body is accepted, so this is "
                "a corrupted result, not an unfinished one"
                % (gcs.uri(path), gcs.uri(body), type(exc).__name__, exc))
        declared = marker.get("records")
        if declared is not None and int(declared) != len(records):
            raise kopf.PermanentError(
                "shard %s declares %d records but its body holds %d. A truncated "
                "result is never folded into a summary"
                % (gcs.uri(body), int(declared), len(records)))
        marker["records"] = records
        out.append(marker)
    return sorted(out, key=lambda s: s["shard"])


# ----------------------------------------------------- operator-side steps

def run_assert(spec, namespace, name, step, status, logger):
    """The half of a preflight the operator can see for itself.

    The database-side half -- pg_stat_statements, last_analyze -- needs a
    connection the operator may not have, so it runs as a worker Job.
    """
    require = step.get("require") or {}
    if require.get("stackReady"):
        control.require_stack_ready(namespace, spec["stackRef"])
    if require.get("datasetReady"):
        control.require_dataset_ready(namespace, spec["datasetRef"])
    if require.get("exclusiveLease"):
        control.acquire_lease(namespace, spec["stackRef"], "%s/%s" % (namespace, name), logger)
    if require.get("searchResultCacheDisabled"):
        settings = control.hapi_settings(namespace)
        if RESULT_CACHE_KEY not in settings:
            raise kopf.PermanentError(
                "hapi.fhir.%s is not present in the stack ConfigMap, so the search "
                "result cache cannot be asserted off. Every repeated measurement would "
                "be served from HAPI's Search cache" % RESULT_CACHE_KEY)
        if int(settings[RESULT_CACHE_KEY]) != 0:
            raise kopf.PermanentError(
                "hapi.fhir.%s is %s, not 0. HAPI serves an identical repeated search "
                "from its own cache, so hot numbers would measure nothing else. Put a "
                "configure step ahead of this one"
                % (RESULT_CACHE_KEY, settings[RESULT_CACHE_KEY]))
    # analyzedSinceLoad is the worker's job; needing it means this step also
    # launches one.
    return bool(require.get("analyzedSinceLoad"))


def run_configure(spec, namespace, step, status, logger):
    settings = step.get("hapi") or {}
    if not settings:
        raise kopf.PermanentError(
            "configure step %s names no hapi settings" % step["id"])
    before = control.configure(namespace, settings, logger)
    control.restart(namespace, ["hapi"], logger)
    return before


def run_settle(namespace, step, logger):
    """(done, detail). Blocks by returning False; there is no timeout here --
    the step stays Running and the journal shows what it is waiting for."""
    require = step.get("require") or {}
    components = step.get("components") or list(control.DEPLOYMENTS)
    detail = {}
    done = True
    if require.get("deploymentsReady", True):
        ready, detail = control.deployments_ready(namespace, components)
        done = done and ready
    if require.get("endpointServing"):
        serving = control.endpoint_serving(namespace, "hapi-fhir")
        detail["hapi-fhir endpoints"] = "serving" if serving else "no ready addresses"
        done = done and serving
    return done, detail


# --------------------------------------------------------------------------
# SECTION 6  Handlers
# --------------------------------------------------------------------------

def observe(spec, status, namespace, name):
    plan = spec.get("plan") or []
    current = int(status.get("currentStep", 0))
    run_id = int(status.get("runId", 0))
    entry = _entry(status, current)
    running = bool(entry and entry.get("outcome") == "Running")
    # Only a step that is Running and launched a Job has one to look at. A
    # stale Job from the previous runId must not decide this one, which is why
    # the name carries the run id.
    has_job = bool(running and entry.get("jobName"))
    job = None
    if has_job and int(spec.get("runId", 1)) == run_id:
        job = reconciliation.job_phase(find_job(namespace, name, run_id, current))
    return BenchmarkSteadyFacts(
        requested_run=int(spec.get("runId", 1)),
        recorded_run=run_id,
        phase=status.get("phase") or "",
        state=spec.get("state") or "Run",
        steps=len(plan),
        current=current,
        step_running=running,
        has_job=has_job,
        job=job)


def _entry(status, index):
    for item in status.get("journal") or []:
        if item.get("index") == index:
            return item
    return None


def _journal(status, entry):
    """Append-only. The operator writes entries; it never rewrites one, except
    to close the entry it is currently running."""
    journal = list(status.get("journal") or [])
    for i, item in enumerate(journal):
        if item.get("index") == entry["index"]:
            journal[i] = entry
            return journal
    journal.append(entry)
    return journal


def _totals(namespace, name, run_id, summary=None):
    """Run totals, always out of GCS.

    status carries a copy for the CRD's printer columns, but nothing in this
    operator ever reads that copy back -- it is a projection for `kubectl get`,
    not a source. Pass summary when the caller already holds it, to save the
    round trip.
    """
    if summary is None:
        summary = gcs.read_summary(namespace, name, run_id)
    return stats.totals(summary)


def _progress(done, steps, measurements, cases):
    return "%d/%d steps, %d measurements over %d cases" % (done, steps, measurements, cases)


@kopf.on.create(GROUP, VERSION, PLURAL, id="admit")
def admit(patch, **_):
    patch.status["phase"] = "Pending"
    patch.status["runId"] = 0
    patch.status["currentStep"] = 0
    patch.status["journal"] = []
    patch.status["invalidCount"] = 0
    patch.status["measurements"] = 0
    patch.status["resultsUri"] = ""


@kopf.on.resume(GROUP, VERSION, PLURAL, id="no-resume")
def no_resume(status, patch, name, logger, **_):
    """C4. A run interrupted by an operator restart is never resumed.

    Resuming would append measurements taken under different node conditions,
    hours apart, to a series that reads as one. Partial results are retained
    and marked partial; a new run needs a new spec.runId.
    """
    if (status or {}).get("phase") in ("Validating", "Leasing", "Running", "Reporting"):
        logger.warning("%s was %s when the operator restarted; aborting the run",
                       name, status.get("phase"))
        patch.status["phase"] = "Aborted"
        patch.status["reason"] = "operator-restart: partial results retained, not resumed"
        patch.status["partial"] = True


# Fields that used to hold measurements in etcd, before results moved to GCS.
# A FhirBenchmark created by an older operator still carries them, and the UI
# would happily render a result that no longer has a source of truth behind it.
LEGACY_RESULT_FIELDS = ("summary", "summaryFull", "deltas")


def _strip_legacy(status, patch):
    """Delete pre-GCS result fields from a status the moment one is seen.

    Setting a key to None is how kopf removes it. This is the only way the old
    copies ever go away: the runs that wrote them are terminal, so nothing else
    will ever patch those objects again.
    """
    for key in LEGACY_RESULT_FIELDS:
        if key in (status or {}):
            patch.status[key] = None


@kopf.timer(GROUP, VERSION, PLURAL, interval=15, id="advance")
def advance(spec, status, namespace, name, patch, logger, **_):
    """One step per pass, and one place a permanent failure can land.

    kopf treats a PermanentError from a timer as "stop retrying this pass" and
    leaves the object exactly as it was, which means the run wedges at whatever
    phase and reason it last wrote and the error lives only in the operator
    log. Every precondition in this operator is a PermanentError, so that is
    the normal way a run fails -- it belongs in status, and the stack it had
    already leased or reconfigured belongs back the way it was found.

    kopf.TemporaryError is deliberately not caught: that is infrastructure that
    has not converged yet, which is orchestration rather than measurement.
    """
    status = status or {}
    _strip_legacy(status, patch)
    try:
        _advance(spec, status, namespace, name, patch, logger)
    except kopf.PermanentError as exc:
        index = int(status.get("currentStep", 0))
        plan = spec.get("plan") or []
        entry = None
        if index < len(plan):
            step = plan[index]
            entry = _entry(status, index) or {"index": index, "id": step["id"],
                                              "step": step["step"], "outcome": "Pending"}
        _fail(spec, namespace, name, status, patch, logger, entry, index, str(exc))


def _advance(spec, status, namespace, name, patch, logger):
    plan = spec.get("plan") or []
    facts = observe(spec, status, namespace, name)
    run_id = int(status.get("runId", 0))

    decision = decide(BENCHMARK_STEADY, facts)

    if decision.action == RESET:
        logger.info("%s: %s", name, decision.reason)
        _archive(status, patch, spec)
        return

    if decision.action == ABORT:
        logger.info("%s: %s", name, decision.reason)
        _teardown_run(spec, namespace, name, status, patch, logger, "Aborted",
                      "spec.state is Abort")
        return

    if decision.action == IDLE:
        patch.status["reason"] = decision.reason
        return

    if decision.action == VALIDATE:
        patch.status["phase"] = "Validating"
        validated = validate(spec, namespace, name, logger)
        patch.status.update(validated)
        patch.status.update(_open_run(spec, namespace, name, run_id, status,
                                      validated, logger))
        patch.status["reason"] = "validated"
        return

    if decision.action == LEASE:
        control.acquire_lease(namespace, spec["stackRef"], "%s/%s" % (namespace, name),
                              logger)
        patch.status["phase"] = "Running"
        patch.status["leased"] = True
        patch.status["reason"] = "lease held on stack %s" % spec["stackRef"]
        return

    if decision.action == FINISH:
        _finish(spec, namespace, name, status, patch, logger)
        return

    index = facts.current
    step = plan[index]
    entry = _entry(status, index) or {"index": index, "id": step["id"],
                                      "step": step["step"], "outcome": "Pending"}

    if decision.action == AWAIT_JOB:
        patch.status["reason"] = "%s: %s" % (job_name(name, run_id, index), decision.reason)
        return

    if decision.action in (INGEST, FAIL_STEP):
        logger.info("%s: %s -- %s", name, decision.case_id, decision.reason)
        _ingest(spec, namespace, name, step, index, run_id, entry, status, patch,
                logger, facts.job)
        return

    # A step already running with no Job: an operator-side step that blocks.
    if entry.get("outcome") == "Running":
        if step["step"] == "settle":
            done, detail = run_settle(namespace, step, logger)
            entry["detail"] = detail
            if not done:
                patch.status["journal"] = _journal(status, entry)
                patch.status["reason"] = "settling: %s" % json.dumps(detail)
                return
            _close(entry, "Passed", status, patch, spec, index,
               namespace, name, run_id)
            return
        raise RuntimeError("step %s is Running with nothing to wait on" % step["id"])

    _start(spec, namespace, name, step, index, run_id, entry, status, patch, logger)


def _start(spec, namespace, name, step, index, run_id, entry, status, patch, logger):
    entry.update({"outcome": "Running", "startedAt": _now()})
    patch.status["phase"] = "Running"
    patch.status["currentStepId"] = step["id"]
    patch.status["progress"] = _progress(
        index, len(spec.get("plan") or []),
        _totals(namespace, name, run_id)["measurements"],
        int(status.get("caseCount", 0)))
    kind = step["step"]

    if kind == "assert":
        needs_worker = run_assert(spec, namespace, name, step, status, logger)
        if not needs_worker:
            _close(entry, "Passed", status, patch, spec, index,
               namespace, name, run_id)
            return
        entry["jobName"] = launch(spec, namespace, name, step, index, status, logger)

    elif kind == "configure":
        before = run_configure(spec, namespace, step, status, logger)
        configured = dict(status.get("configured") or {})
        for key, was in before.items():
            configured.setdefault(key, was)
        patch.status["configured"] = configured
        entry["before"] = {k: str(v) for k, v in before.items()}
        _close(entry, "Passed", status, patch, spec, index,
               namespace, name, run_id)
        return

    elif kind == "restart":
        components = step.get("components") or []
        if not components:
            raise kopf.PermanentError("restart step %s names no components" % step["id"])
        entry["restartedAt"] = control.restart(namespace, components, logger)
        _close(entry, "Passed", status, patch, spec, index,
               namespace, name, run_id)
        return

    elif kind == "settle":
        done, detail = run_settle(namespace, step, logger)
        entry["detail"] = detail
        if done:
            _close(entry, "Passed", status, patch, spec, index,
               namespace, name, run_id)
            return

    elif kind == "report":
        _close(entry, "Passed", status, patch, spec, index,
               namespace, name, run_id)
        return

    elif kind in ("analyze", "prewarm", "measure"):
        entry["jobName"] = launch(spec, namespace, name, step, index, status, logger)

    else:
        raise kopf.PermanentError("unknown step type %r in step %s" % (kind, step["id"]))

    patch.status["journal"] = _journal(status, entry)
    patch.status["reason"] = "step %s (%s) started" % (step["id"], kind)


def _ingest(spec, namespace, name, step, index, run_id, entry, status, patch,
            logger, phase):
    """Fold one finished step's shards into the journal and the summary."""
    published = shards(namespace, name, run_id, index)
    records = [record for shard in published for record in shard.get("records") or []]
    invalid = [r for r in records if not r.get("valid")]
    entry["node"] = ", ".join(sorted(set(s.get("node") or "" for s in published))) or "-"
    entry["shards"] = len(published)
    entry["measurements"] = len(records)
    entry["invalid"] = len(invalid)
    if invalid:
        entry["invalidReasons"] = sorted(set(r["invalidReason"] for r in invalid
                                             if r.get("invalidReason")))[:10]

    merged = None
    if records:
        summary = stats.summarise(records)
        # The merge base is read back from GCS, never from status. Results have
        # exactly one home: a summary that lived in etcd would be a second copy
        # that could disagree with the object store and could be served without
        # GCS being reachable at all.
        merged = stats.merge(gcs.read_summary(namespace, name, run_id), summary)
        gcs.write_summary(namespace, name, run_id, merged)
        gcs.write_json("%s/step.json" % gcs.step_prefix(namespace, name, run_id, index),
                       {"index": index, "id": step["id"], "step": step["step"],
                        "cacheLabel": step.get("cacheLabel") or step["id"],
                        "finishedAt": _now(), "summary": summary})
    # Projections for the CRD's printer columns, recomputed from the GCS
    # summary every time rather than accumulated in status. Nothing in this
    # operator reads them back; kubectl is their only consumer.
    run_totals = _totals(namespace, name, run_id, merged)
    patch.status["measurements"] = run_totals["measurements"]
    patch.status["invalidCount"] = run_totals["invalid"]

    # Recorded for every measure step whether or not a floor is declared: the
    # hit/read ratio is what makes a cacheLabel a measurement rather than an
    # assertion, and it is evidence in the result either way.
    ratio = _cold_read_ratio(published) if step["step"] == "measure" else None
    if ratio is not None:
        entry["coldReadRatio"] = ratio

    floor = (step.get("require") or {}).get("coldReadRatioFloor")
    if floor is not None:
        if ratio is None or ratio < float(floor):
            _fail(spec, namespace, name, status, patch, logger, entry, index,
                  "step %s declares coldReadRatioFloor %.2f but PostgreSQL reports %s. "
                  "A warm measurement is never relabelled cold"
                  % (step["id"], float(floor),
                     "no block activity" if ratio is None else "%.3f" % ratio))
            return

    if step["step"] == "measure" and not records:
        _fail(spec, namespace, name, status, patch, logger, entry, index,
              "step %s finished but published no measurements. A measure step that "
              "produced nothing is a failed step, not an empty one" % step["id"])
        return

    if phase == "Failed" or invalid:
        _fail(spec, namespace, name, status, patch, logger, entry, index,
              "step %s failed: job %s, %d invalid measurement(s). No measurement is "
              "retried. The named reason is in the worker pod log"
              % (step["id"], phase, len(invalid)))
        return

    _close(entry, "Passed", status, patch, spec, index,
           namespace, name, run_id, totals=merged)


def _fail(spec, namespace, name, status, patch, logger, entry, index, reason):
    """End the run Failed, and put the stack back on the way out.

    Restoring always runs, including after a failure. A benchmark that takes an
    exclusive lease on a stack and rewrites its HAPI config owns putting both
    back: leaving the lease held blocks every later benchmark on that stack,
    and leaving the config rewritten means the next run measures a server this
    one configured and nobody declared.
    """
    if entry is not None:
        entry["outcome"] = "Failed"
        entry["finishedAt"] = _now()
        entry["error"] = reason
        patch.status["journal"] = _journal(status, entry)
    logger.error("%s: run failed -- %s", name, reason)
    _teardown_run(spec, namespace, name, status, patch, logger, "Failed", reason)


def _cold_read_ratio(published):
    """shared_blks_read / (hit + read) over the step, one sample per case.

    Per-case rather than per-record: the PostgreSQL counters are snapshotted
    once around each case's whole repeat block, so counting them once per
    repetition would weight a case by its repeat count.
    """
    hit = read = 0
    for shard in published:
        seen = set()
        for record in shard.get("records") or []:
            key = (shard["shard"], record["caseId"])
            if key in seen:
                continue
            seen.add(key)
            hit += int(record.get("sharedBlksHit") or 0)
            read += int(record.get("sharedBlksRead") or 0)
    return (read / (hit + read)) if (hit + read) else None


def _close(entry, outcome, status, patch, spec, index,
           namespace, name, run_id, totals=None):
    entry["outcome"] = outcome
    entry["finishedAt"] = _now()
    patch.status["journal"] = _journal(status, entry)
    if outcome == "Passed":
        patch.status["currentStep"] = index + 1
        patch.status["progress"] = _progress(
            index + 1, len(spec.get("plan") or []),
            _totals(namespace, name, run_id, totals)["measurements"],
            int(status.get("caseCount", 0)))


def _open_run(spec, namespace, name, run_id, status, validated, logger):
    """Write run.json before a single query is issued.

    A run that cannot record its own provenance must not produce numbers, so
    this is part of validation rather than a best-effort afterthought. It is
    also the only place the whole context of a run is written down together:
    catalogue digest, seed, bindings, the HAPI settings in force and the
    dataset it ran against, next to the records they explain.
    """
    gcs.require()
    prefix = gcs.run_prefix(namespace, name, run_id)
    manifest = {
        "name": name, "namespace": namespace, "runId": run_id,
        "openedAt": _now(),
        "stackRef": spec.get("stackRef"), "datasetRef": spec.get("datasetRef"),
        "plan": plan_of(spec),
        "defaults": defaults_of(spec),
        "worker": spec.get("worker") or {},
        "workerImage": WORKER_IMAGE,
        "hapiSettings": control.hapi_settings(namespace),
    }
    manifest.update(validated)
    uri = gcs.write_json("%s/run.json" % prefix, manifest)
    logger.info("%s: run %d opened at %s", name, run_id, uri)
    return {"resultsUri": gcs.uri(prefix), "runManifestUri": uri}


def _finish(spec, namespace, name, status, patch, logger):
    """Close the run. The report points at the summary; it does not copy it.

    Nothing measured is written to status here. deltas are not stored at all --
    stats.deltas is a pure function of the summary, so the UI derives them on
    demand rather than keeping a second copy that can drift.
    """
    patch.status["phase"] = "Reporting"
    run_id = int(status.get("runId", 0))

    # Whether the run passed is decided from the records in GCS, not from a
    # counter in status. A run cannot be declared Complete on the strength of
    # a number whose evidence the operator did not just read.
    summary = gcs.read_summary(namespace, name, run_id)
    run_totals = stats.totals(summary)
    outcome = "Failed" if run_totals["invalid"] else "Complete"
    patch.status["measurements"] = run_totals["measurements"]
    patch.status["invalidCount"] = run_totals["invalid"]

    patch.status["reportUri"] = gcs.write_json(
        gcs.report_path(namespace, name, run_id),
        {"name": name, "namespace": namespace, "runId": run_id,
         "closedAt": _now(), "outcome": outcome,
         "measurements": run_totals["measurements"],
         "invalidCount": run_totals["invalid"],
         "journal": status.get("journal") or [],
         "summary": gcs.uri(gcs.summary_path(namespace, name, run_id))})

    _teardown_run(spec, namespace, name, status, patch, logger, outcome,
                  "%d measurement(s) invalid" % run_totals["invalid"]
                  if outcome == "Failed" else "all steps passed")


def _teardown_run(spec, namespace, name, status, patch, logger, phase, reason):
    """Restoring always runs, including after a failure."""
    patch.status["phase"] = "Restoring"
    configured = status.get("configured") or {}
    if configured:
        control.configure(namespace, configured, logger)
        control.restart(namespace, ["hapi"], logger)
        patch.status["restored"] = {k: str(v) for k, v in configured.items()}
        patch.status["configured"] = {}
    control.release_lease(namespace, spec["stackRef"], "%s/%s" % (namespace, name), logger)
    patch.status["leased"] = False
    patch.status["phase"] = phase
    patch.status["reason"] = reason
    logger.info("%s: run %s -- %s", name, phase, reason)


def _archive(status, patch, spec):
    """A new runId starts a new journal. Prior journals are retained under
    status.archive, never overwritten."""
    archive = list(status.get("archive") or [])
    if status.get("journal"):
        # A pointer, not a copy. The summary and journal live in GCS and are
        # not deleted, so the CR no longer carries five full result sets in
        # etcd to keep five runs' history.
        # A pointer and nothing else. The counts for a past run are in its
        # report.json, next to the records that justify them.
        archive.append({"runId": status.get("runId"), "phase": status.get("phase"),
                        "uri": status.get("resultsUri"),
                        "report": status.get("reportUri")})
    patch.status["archive"] = archive[-50:]
    patch.status["runId"] = int(spec.get("runId", 1))
    patch.status["phase"] = "Pending"
    patch.status["currentStep"] = 0
    patch.status["currentStepId"] = ""
    patch.status["journal"] = []
    patch.status["invalidCount"] = 0
    patch.status["measurements"] = 0
    patch.status["partial"] = False
    patch.status["resultsUri"] = ""
    patch.status["runManifestUri"] = ""
    patch.status["reportUri"] = ""
    patch.status["reason"] = "new run %d" % int(spec.get("runId", 1))


@kopf.on.delete(GROUP, VERSION, PLURAL, id="release")
def release(spec, status, namespace, name, patch, logger, **_):
    """Stop the workers, put the stack back, drop the lease.

    kopf drops the finalizer the moment a deletion handler produces no delay,
    so returning is how the object is released and a delayed TemporaryError is
    how it is held. kopf.PermanentError must never reach here.
    """
    status = status or {}
    jobs = tuple(job.metadata.name for job in _batch().list_namespaced_job(
        namespace, label_selector="app=fhir-benchmark,benchmark=%s" % name).items)
    facts = BenchmarkTeardownFacts(
        namespace_terminating=reconciliation.namespace_terminating(namespace),
        jobs=jobs,
        configured=bool(status.get("configured")),
        leased=bool(status.get("leased")))
    decision = decide(BENCHMARK_TEARDOWN, facts)
    logger.info("teardown %s: %s -- %s", name, decision.case_id, decision.reason)

    if decision.action is reconciliation.Action.STOP_JOBS:
        reconciliation.stop_jobs(namespace, facts.jobs, logger)
        raise kopf.TemporaryError(decision.reason, delay=10)

    if facts.namespace_terminating:
        return

    configured = status.get("configured") or {}
    if configured:
        control.configure(namespace, configured, logger)
        control.restart(namespace, ["hapi"], logger)
    control.release_lease(namespace, spec["stackRef"], "%s/%s" % (namespace, name), logger)

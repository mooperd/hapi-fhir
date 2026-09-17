# Created by claude-opus-5
"""Minimal kopf operator for HAPI FHIR benchmarking stacks.

One CRD, FhirStack. Create one in a namespace and the operator renders
hapi-fhir-standalone.yaml into that namespace with CPU and memory taken from
the spec. Edit the spec and it resizes. Delete it and owner references take
the whole stack with it.

Postgres and Elasticsearch tuning is derived from the limits rather than left
at the manifest's values: shared_buffers and -Xms are committed at startup, so
a limit below them is an OOM kill during boot.

Run with: kopf run fhir_operator.py --namespace <ns>
"""

import math
import os
import sys

import kopf
import yaml
from kubernetes import client, config, dynamic

# kopf loads this file by path and does not put its directory on sys.path.
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
import benchmark  # noqa: E402 - needs the path above
import datasets  # noqa: E402 - needs the path above
import reconciliation  # noqa: E402 - needs the path above
import ui  # noqa: E402 - needs the path above

GROUP = "perf.fhir"
VERSION = "v1alpha1"
PLURAL = "fhirstacks"
UI_PORT = int(os.environ.get("FHIR_UI_PORT", "8090"))

DOWNLOADER = os.path.expanduser("~/Documents/GitHub/mimic-fhir-downloader")
MANIFEST = os.environ.get("FHIR_MANIFEST", os.path.join(DOWNLOADER, "hapi-fhir-standalone.yaml"))
KUBECONFIG = os.environ.get("FHIR_KUBECONFIG")

# spec key -> (deployment, container, default cpu min/max, default memory GiB min/max)
COMPONENTS = {
    "hapi": ("hapi-fhir", "hapi-fhir", (1.0, 2.0), (2.0, 10.0)),
    "postgres": ("hapi-fhir-db", "postgres", (4.0, 4.0), (32.0, 32.0)),
    "elasticsearch": ("hapi-fhir-es", "elasticsearch", (2.0, 2.0), (8.0, 8.0)),
}

PG_BASE_GI = 180.0     # the limit the manifest's postgres args were tuned against
PG_BASE_CPU = 24.0

# PostgreSQL 18. Pinned here rather than taken from the manifest so the version
# under measurement is a property of the operator that renders the stack, and
# so a benchmark result can name it. 18 is wanted for what it exposes about its
# own reads -- pg_stat_io gains per-backend, per-object and per-context
# granularity, which is how a benchmark can attribute I/O to the server's own
# connections instead of to everything on the instance including the observer.
#
# A PostgreSQL 16 data directory will not start under 18. There is no in-place
# upgrade here by design: FhirDataset regenerates deterministically from
# spec.seed, so the path is a fresh PVC and a reload, not pg_upgrade.
POSTGRES_IMAGE = os.environ.get("POSTGRES_IMAGE", "postgres:18.6-alpine")

_dyn = None


def dyn():
    global _dyn
    if _dyn is None:
        if KUBECONFIG:
            config.load_kube_config(config_file=KUBECONFIG)
        else:
            try:
                config.load_incluster_config()
            except config.ConfigException:
                config.load_kube_config()
        _dyn = dynamic.DynamicClient(client.ApiClient())
    return _dyn


# --------------------------------------------------------------------------
# Sizing
# --------------------------------------------------------------------------

def sizes(spec):
    """Merge spec.resources over the defaults, as {key: {cpu|memory: (min, max)}}."""
    given = spec.get("resources") or {}
    out = {}
    for key, (_, _, cpu_default, mem_default) in COMPONENTS.items():
        block = given.get(key) or {}
        out[key] = {
            "cpu": _pair(block.get("cpu"), cpu_default),
            "memory": _pair(block.get("memory"), mem_default),
        }
    return out


def _pair(block, default):
    block = block or {}
    low = float(block.get("min", default[0]))
    high = float(block.get("max", default[1]))
    if low > high:
        raise kopf.PermanentError("min %g is above max %g" % (low, high))
    return low, high


def quantities(cpu, memory, policy):
    """requests/limits for one container under the given cpuLimitPolicy."""
    cpu_low, cpu_high = cpu
    mem_low, mem_high = memory
    if policy == "strict":
        cpu_low = cpu_high
    requests = {"cpu": _cores(cpu_low), "memory": _mib(mem_low)}
    limits = {"memory": _mib(mem_high)}
    if policy != "none":
        limits["cpu"] = _cores(cpu_high)
    return {"requests": requests, "limits": limits}


def _cores(cores):
    return "%dm" % round(cores * 1000)


def _mib(gibibytes):
    return "%dMi" % round(gibibytes * 1024)


def postgres_args(cpu_limit, mem_limit_gi):
    """The manifest's postgres args, scaled to the container limits.

    Memory ratios are the manifest's own, so 180Gi reproduces it verbatim.
    Worker counts track the CPU limit; max_connections stays put because
    work_mem is per sort, not per server.
    """
    ratio = mem_limit_gi / PG_BASE_GI

    def mb(base, floor):
        return max(int(base * ratio), floor)

    def clamp(value, low, high):
        return int(max(low, min(high, value)))

    workers = max(8, int(2.5 * cpu_limit))

    # PostgreSQL 18 does asynchronous I/O by default (io_method=worker,
    # io_workers=3). That changes read behaviour, which is the single thing
    # this stack exists to measure, so every knob on the read path is pinned
    # explicitly and rendered into status rather than left at a default nobody
    # recorded. io_workers is 1-32; three is the default and too few to keep a
    # 24-core instance busy.
    return [
        "-c", "io_method=worker",
        "-c", "io_workers=%d" % clamp(cpu_limit // 4, 3, 32),
        "-c", "io_combine_limit=128kB",
        "-c", "io_max_combine_limit=128kB",
        "-c", "effective_io_concurrency=16",
        "-c", "maintenance_io_concurrency=16",
        "-c", "shared_buffers=%dMB" % mb(65536, 128),
        "-c", "effective_cache_size=%dMB" % mb(163840, 256),
        "-c", "maintenance_work_mem=%dMB" % mb(4096, 64),
        "-c", "autovacuum_work_mem=%dMB" % mb(2048, 32),
        "-c", "work_mem=%dMB" % mb(1024, 4),
        "-c", "max_connections=300",
        "-c", "max_worker_processes=%d" % workers,
        "-c", "max_parallel_workers=%d" % workers,
        "-c", "max_parallel_workers_per_gather=%d" % clamp(cpu_limit // 4, 2, 8),
        "-c", "max_parallel_maintenance_workers=%d" % clamp(cpu_limit // 8, 2, 4),
        "-c", "autovacuum_max_workers=%d" % clamp(cpu_limit // 6, 3, 8),
        "-c", "max_wal_size=32GB",
        "-c", "checkpoint_timeout=30min",
        "-c", "wal_buffers=64MB",
        # auto_explain is preloaded but inert: its log_min_duration defaults to
        # -1, so it costs nothing until a benchmark turns it on. Preloading is
        # the only way to have it available for HAPI's connections, and it
        # needs a postmaster restart, which is not something to discover in the
        # middle of a run.
        "-c", "shared_preload_libraries=pg_stat_statements,auto_explain",
    ]


def pg_settings(plan):
    """The rendered postgres tuning, as {name: value}, for status."""
    size = plan["postgres"]
    args = postgres_args(size["cpu"][1], size["memory"][1])
    out = {"image": POSTGRES_IMAGE}
    for flag, setting in zip(args, args[1:]):
        if flag == "-c" and "=" in setting:
            name, _, value = setting.partition("=")
            out[name] = value
    return out


def elasticsearch_env(cpu_limit, mem_limit_gi):
    """Heap at half the limit, and node.processors set explicitly.

    -Xms is committed at startup, so the heap has to come down with the limit.
    Megabytes rather than gigabytes so a sub-2Gi limit still gets a heap that
    fits. ES sizes its thread pools from node.processors and its own cgroup
    detection is unreliable under a fractional limit.
    """
    heap = max(min(int(mem_limit_gi * 1024 / 2), 31 * 1024), 256)
    return [
        {"name": "ES_JAVA_OPTS", "value": "-Xms%dm -Xmx%dm" % (heap, heap)},
        {"name": "node.processors", "value": str(max(1, math.ceil(cpu_limit)))},
    ]


# --------------------------------------------------------------------------
# Rendering and applying
# --------------------------------------------------------------------------

def render(spec):
    """The manifest's documents, with resources and tuning from the spec."""
    policy = spec.get("cpuLimitPolicy", "burst")
    plan = sizes(spec)
    by_deployment = {d: (k, c) for k, (d, c, _, _) in COMPONENTS.items()}

    docs = []
    with open(MANIFEST, encoding="utf-8") as handle:
        for doc in yaml.safe_load_all(handle):
            if not doc:
                continue
            name = doc.get("metadata", {}).get("name")
            if doc.get("kind") == "Deployment":
                _pin_postgres_image(doc)
            if doc.get("kind") == "Deployment" and name in by_deployment:
                key, container_name = by_deployment[name]
                _resize(doc, container_name, plan[key], policy)
            if doc.get("kind") == "ConfigMap" and name == "hapi-fhir-config":
                _hapi_settings(doc)
            docs.append(doc)
    return docs


# Dataset deletion needs these four. Without them a conditional delete with
# _expunge=true is rejected, and the operator cannot reconcile a dataset to
# Absent. $delete-expunge itself is not registered in the hapiproject image;
# the _expunge query parameter reaches the same Batch2 job.
EXPUNGE_SETTINGS = {
    "expunge_enabled": True,
    "delete_expunge_enabled": True,
    "allow_multiple_delete": True,
    "enforce_referential_integrity_on_delete": False,
}


# Benchmark controls. These are rendered so the keys EXIST, which is what a
# FhirBenchmark configure step requires -- it will not create a hapi.fhir
# setting implicitly, because a key HAPI may not read under a name nobody
# checked would be reported as a control that took effect when it did not.
#
# reuse_cached_search_results_millis is 0 rather than HAPI's 60000 on purpose.
# HAPI serves an identical repeated search from its own Search cache for a
# minute by default, so on a benchmarking stack every "hot" number would
# measure the cache and nothing else. Production runs with the cache on; this
# stack does not, and says so here.
#
# filter_search_enabled is true because the catalogue measures _filter. HAPI
# ships it off (JpaStorageSettings.myFilterParameterEnabled) and rejects the
# query with HAPI-1222, which the worker records as an error rather than a
# measurement -- a disabled parameter is indistinguishable from a slow one if
# nobody turns it on. Note QueryStack parses the expression BEFORE reading this
# flag, so a syntactically invalid _filter still fails with HAPI-1221 here.
BENCHMARK_SETTINGS = {
    "reuse_cached_search_results_millis": 0,
    "filter_search_enabled": True,
}


def _hapi_settings(doc):
    """Render expunge and the benchmark controls into the HAPI config."""
    raw = (doc.get("data") or {}).get("application.yaml")
    if not raw:
        return
    parsed = yaml.safe_load(raw)
    fhir = ((parsed or {}).get("hapi") or {}).get("fhir")
    if fhir is None:
        return
    fhir.update(EXPUNGE_SETTINGS)
    for key, value in BENCHMARK_SETTINGS.items():
        fhir.setdefault(key, value)
    doc["data"]["application.yaml"] = yaml.safe_dump(parsed, sort_keys=False)


def _pin_postgres_image(doc):
    """One PostgreSQL version in play, everywhere it appears.

    hapi-fhir's wait-for-db init container ships pg_isready from whatever major
    the manifest named, which is a second version to reason about for no gain.
    """
    pod = doc["spec"]["template"]["spec"]
    for container in (pod.get("initContainers") or []) + pod["containers"]:
        if str(container.get("image", "")).startswith("postgres:"):
            container["image"] = POSTGRES_IMAGE


def _resize(doc, container_name, size, policy):
    cpu_limit = size["cpu"][1]
    mem_limit = size["memory"][1]
    for container in doc["spec"]["template"]["spec"]["containers"]:
        if container["name"] != container_name:
            continue
        container["resources"] = quantities(size["cpu"], size["memory"], policy)
        if container_name == "postgres":
            container["image"] = POSTGRES_IMAGE
            container["args"] = postgres_args(cpu_limit, mem_limit)
        elif container_name == "elasticsearch":
            container["env"] = _merge_env(container.get("env", []),
                                          elasticsearch_env(cpu_limit, mem_limit))


def _merge_env(existing, overrides):
    keyed = {entry["name"]: entry for entry in existing}
    for entry in overrides:
        keyed[entry["name"]] = entry
    return list(keyed.values())


def apply(docs, namespace, logger):
    for doc in docs:
        kopf.adopt(doc)
        resource = dyn().resources.get(api_version=doc["apiVersion"], kind=doc["kind"])
        dyn().server_side_apply(
            resource=resource, body=doc, namespace=namespace,
            field_manager="fhir-operator", force_conflicts=True)
        logger.info("applied %s/%s", doc["kind"], doc["metadata"]["name"])


# --------------------------------------------------------------------------
# Handlers
# --------------------------------------------------------------------------

@kopf.on.startup()
def startup(settings, logger, **_):
    settings.persistence.finalizer = "perf.fhir/finalizer"
    settings.posting.level = 20
    # The UI edits FhirStacks; the handlers above react to the edits. It runs
    # in a thread here rather than a second container so it shares one
    # ConfigMap, one pip install and one set of credentials.
    datasets.init(dyn)
    reconciliation.init(dyn)
    benchmark.init(dyn)
    ui.start(UI_PORT, dyn)
    logger.info("UI listening on :%d", UI_PORT)


@kopf.on.create(GROUP, VERSION, PLURAL, id="provision")
@kopf.on.resume(GROUP, VERSION, PLURAL, id="readopt")
@kopf.on.field(GROUP, VERSION, PLURAL, field="spec", id="reconfigure")
def reconcile(spec, namespace, patch, logger, **_):
    """Render and apply. Same path for first create, operator restart and resize."""
    apply(render(spec), namespace, logger)
    patch.status["phase"] = "Provisioning"
    plan = sizes(spec)
    patch.status["applied"] = {
        key: {"cpu": "%g-%g" % size["cpu"], "memory": "%g-%gGi" % size["memory"]}
        for key, size in plan.items()
    }
    # Recorded, not assumed. PostgreSQL 18's read path has knobs whose defaults
    # changed, and a latency number that does not carry the settings it was
    # produced under cannot be compared with another one.
    patch.status["postgres"] = pg_settings(plan)


@kopf.on.delete(GROUP, VERSION, PLURAL, id="guard")
def guard(spec, namespace, name, logger, **_):
    """Sequence teardown: datasets first, then the stack that serves them.

    kopf holds perf.fhir/finalizer for as long as this handler keeps producing
    a delay, and while it holds, the stack's Deployments are still running --
    which is exactly the window the datasets need in order to purge against
    hapi-fhir. Returning releases the finalizer and lets the stack go.

    Every case is in reconciliation.py. kopf.PermanentError must never reach
    here: it produces no delay, so it would release the finalizer and destroy
    the server while datasets are still purging against it.
    """
    facts = reconciliation.observe_stack_teardown(namespace, spec)
    decision = reconciliation.decide(reconciliation.STACK_TEARDOWN, facts)
    action = decision.action
    logger.info("teardown %s/%s: %s -- %s", namespace, name, decision.case_id, decision.reason)

    if action is reconciliation.Action.PROCEED:
        return

    if action is reconciliation.Action.DELETE_DATASETS:
        reconciliation.delete_datasets(
            namespace, [dataset for dataset, terminating in facts.datasets if not terminating],
            logger)
        raise kopf.TemporaryError(decision.reason, delay=10)

    if action is reconciliation.Action.WAIT:
        raise kopf.TemporaryError(decision.reason, delay=20)

    raise RuntimeError("stack teardown case %s produced an action guard() cannot perform: %s"
                       % (decision.case_id, action))


@kopf.timer(GROUP, VERSION, PLURAL, interval=15, id="readiness")
def readiness(namespace, patch, **_):
    apps = client.AppsV1Api(dyn().client)
    components, ready = {}, True
    for key, (deployment, _, _, _) in COMPONENTS.items():
        try:
            found = apps.read_namespaced_deployment(name=deployment, namespace=namespace)
        except client.ApiException:
            components[key], ready = "absent", False
            continue
        have = found.status.ready_replicas or 0
        want = found.spec.replicas or 1
        components[key] = "%d/%d" % (have, want)
        ready = ready and have >= want
    patch.status["components"] = components
    patch.status["phase"] = "Ready" if ready else "Provisioning"

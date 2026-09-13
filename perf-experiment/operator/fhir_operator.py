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
import datasets  # noqa: E402 - needs the path above
import ui  # noqa: E402 - needs the path above

GROUP = "perf.pkb"
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
    return [
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
        "-c", "shared_preload_libraries=pg_stat_statements",
    ]


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
            if doc.get("kind") == "Deployment" and name in by_deployment:
                key, container_name = by_deployment[name]
                _resize(doc, container_name, plan[key], policy)
            docs.append(doc)
    return docs


def _resize(doc, container_name, size, policy):
    cpu_limit = size["cpu"][1]
    mem_limit = size["memory"][1]
    for container in doc["spec"]["template"]["spec"]["containers"]:
        if container["name"] != container_name:
            continue
        container["resources"] = quantities(size["cpu"], size["memory"], policy)
        if container_name == "postgres":
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
    settings.persistence.finalizer = "perf.pkb/finalizer"
    settings.posting.level = 20
    # The UI edits FhirStacks; the handlers above react to the edits. It runs
    # in a thread here rather than a second container so it shares one
    # ConfigMap, one pip install and one set of credentials.
    datasets.init(dyn)
    ui.start(UI_PORT, dyn)
    logger.info("UI listening on :%d", UI_PORT)


@kopf.on.create(GROUP, VERSION, PLURAL, id="provision")
@kopf.on.resume(GROUP, VERSION, PLURAL, id="readopt")
@kopf.on.field(GROUP, VERSION, PLURAL, field="spec", id="reconfigure")
def reconcile(spec, namespace, patch, logger, **_):
    """Render and apply. Same path for first create, operator restart and resize."""
    apply(render(spec), namespace, logger)
    patch.status["phase"] = "Provisioning"
    patch.status["applied"] = {
        key: {"cpu": "%g-%g" % size["cpu"], "memory": "%g-%gGi" % size["memory"]}
        for key, size in sizes(spec).items()
    }


@kopf.on.delete(GROUP, VERSION, PLURAL, id="guard")
def guard(spec, namespace, name, logger, **_):
    """Refuse to delete a protected stack.

    Children carry owner references, so deleting the FhirStack deletes the
    PVCs with it. kopf holds a finalizer, so raising here blocks the delete
    until someone sets spec.protected to false on purpose.
    """
    if spec.get("protected"):
        # TemporaryError, not PermanentError. kopf drops the finalizer when a
        # deletion handler produces no delay, and a permanent failure produces
        # none -- so PermanentError lets the delete through, which is the
        # opposite of a guard. A delay keeps the finalizer and the object.
        #
        # The delay is also how long clearing spec.protected takes to release
        # a blocked delete, so keep it short.
        raise kopf.TemporaryError(
            "%s/%s is protected and will not be deleted. Set spec.protected=false "
            "to allow it -- this destroys its PVCs and everything on them."
            % (namespace, name), delay=60)
    logger.info("deleting %s/%s; owner references take the stack with it", namespace, name)


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

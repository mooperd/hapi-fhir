# Created by claude-opus-5
"""Cluster-side effects of a benchmark plan, and the preconditions for them.

A benchmark mutates the stack it measures -- it restarts Postgres, restarts
Elasticsearch, and turns off HAPI's search-result cache -- because cache-state
control is impossible otherwise (C5). That is why it takes an exclusive lease
on its stackRef before it touches anything.

Everything here either succeeds or raises a named error. Nothing defaults,
nothing is best-effort, and nothing retries a measurement.
"""

import datetime
import json

import kopf
import yaml
from kubernetes import client

GROUP = "perf.fhir"
VERSION = "v1alpha1"
STACK_PLURAL = "fhirstacks"
DATASET_PLURAL = "fhirdatasets"

HAPI_CONFIGMAP = "hapi-fhir-config"
HAPI_CONFIG_KEY = "application.yaml"

# spec.plan[].components -> the Deployment that implements it.
DEPLOYMENTS = {
    "postgres": "hapi-fhir-db",
    "elasticsearch": "hapi-fhir-es",
    "hapi": "hapi-fhir",
}

LEASE_NAMESPACE_SUFFIX = "fhir-benchmark"

_dyn = None


def init(dyn):
    global _dyn
    _dyn = dyn


def _core():
    return client.CoreV1Api(_dyn().client)


def _apps():
    return client.AppsV1Api(_dyn().client)


def _custom():
    return client.CustomObjectsApi(_dyn().client)


def _coordination():
    return client.CoordinationV1Api(_dyn().client)


def _now():
    return datetime.datetime.now(datetime.timezone.utc)


def _stamp():
    return _now().strftime("%Y-%m-%dT%H:%M:%SZ")


# --------------------------------------------------------------------------
# Preconditions
# --------------------------------------------------------------------------

def stack(namespace, name):
    try:
        return _custom().get_namespaced_custom_object(
            GROUP, VERSION, namespace, STACK_PLURAL, name)
    except client.ApiException as exc:
        if exc.status != 404:
            raise
        raise kopf.PermanentError(
            "spec.stackRef names FhirStack %s/%s, which does not exist"
            % (namespace, name))


def dataset(namespace, name):
    try:
        return _custom().get_namespaced_custom_object(
            GROUP, VERSION, namespace, DATASET_PLURAL, name)
    except client.ApiException as exc:
        if exc.status != 404:
            raise
        raise kopf.PermanentError(
            "spec.datasetRef names FhirDataset %s/%s, which does not exist"
            % (namespace, name))


def require_stack_ready(namespace, name):
    """A degraded stack is not a stack worth measuring."""
    found = stack(namespace, name)
    phase = (found.get("status") or {}).get("phase")
    if phase != "Ready":
        raise kopf.PermanentError(
            "FhirStack %s/%s is phase %s, not Ready. A benchmark does not "
            "wait-and-hope and does not measure a degraded stack"
            % (namespace, name, phase or "unset"))
    return found


def require_dataset_ready(namespace, name):
    """Never benchmark a partially loaded population."""
    found = dataset(namespace, name)
    status = found.get("status") or {}
    phase = status.get("phase")
    if phase != "Ready":
        raise kopf.PermanentError(
            "FhirDataset %s/%s is phase %s, not Ready" % (namespace, name,
                                                          phase or "unset"))
    expected = status.get("expected") or {}
    observed = status.get("observed") or {}
    if not expected:
        raise kopf.PermanentError(
            "FhirDataset %s/%s has no status.expected" % (namespace, name))
    short = {kind: (int(observed.get(kind, 0)), int(want))
             for kind, want in expected.items()
             if int(observed.get(kind, 0)) < int(want)}
    if short:
        raise kopf.PermanentError(
            "FhirDataset %s/%s is short: %s. Never benchmark a partially loaded "
            "population" % (namespace, name,
                            ", ".join("%s %d/%d" % (k, h, w)
                                      for k, (h, w) in sorted(short.items()))))
    return found


def dataset_config(namespace, name):
    """The frozen dataset.json the loader was given: codes, weights, shape."""
    config_map = "%s-config" % name
    try:
        found = _core().read_namespaced_config_map(config_map, namespace)
    except client.ApiException as exc:
        if exc.status != 404:
            raise
        raise kopf.PermanentError(
            "ConfigMap %s/%s does not exist, so the dataset manifest cannot be read "
            "and no placeholder can be bound" % (namespace, config_map))
    raw = (found.data or {}).get("dataset.json")
    if not raw:
        raise kopf.PermanentError(
            "ConfigMap %s/%s has no dataset.json key" % (namespace, config_map))
    return json.loads(raw)


# --------------------------------------------------------------------------
# HAPI configuration
# --------------------------------------------------------------------------

def hapi_config(namespace):
    """(ConfigMap, parsed application.yaml)."""
    try:
        found = _core().read_namespaced_config_map(HAPI_CONFIGMAP, namespace)
    except client.ApiException as exc:
        if exc.status != 404:
            raise
        raise kopf.PermanentError(
            "ConfigMap %s/%s does not exist; the stack was not rendered by this "
            "operator" % (namespace, HAPI_CONFIGMAP))
    raw = (found.data or {}).get(HAPI_CONFIG_KEY)
    if not raw:
        raise kopf.PermanentError(
            "ConfigMap %s/%s has no %s key" % (namespace, HAPI_CONFIGMAP,
                                               HAPI_CONFIG_KEY))
    return found, yaml.safe_load(raw)


def hapi_settings(namespace):
    """The hapi.fhir block, as a dict."""
    _, parsed = hapi_config(namespace)
    block = ((parsed or {}).get("hapi") or {}).get("fhir")
    if block is None:
        raise kopf.PermanentError(
            "ConfigMap %s/%s has no hapi.fhir block" % (namespace, HAPI_CONFIGMAP))
    return block


def configure(namespace, settings, logger):
    """Apply hapi.fhir settings, returning the before-values.

    A setting that is not already present in the ConfigMap is a PermanentError.
    Creating it implicitly would mean applying a key HAPI may not read under a
    name nobody checked, and reporting the run as if the control had taken
    effect -- which is the exact failure this instrument exists to avoid.
    """
    found, parsed = hapi_config(namespace)
    block = ((parsed or {}).get("hapi") or {}).get("fhir")
    if block is None:
        raise kopf.PermanentError(
            "ConfigMap %s/%s has no hapi.fhir block" % (namespace, HAPI_CONFIGMAP))

    missing = [key for key in settings if key not in block]
    if missing:
        raise kopf.PermanentError(
            "configure step names hapi.fhir setting(s) %s which are not present in "
            "ConfigMap %s/%s. They are never created implicitly -- add them to the "
            "stack's rendered defaults first. Present: %s"
            % (", ".join(sorted(missing)), namespace, HAPI_CONFIGMAP,
               ", ".join(sorted(block))))

    before = {key: block[key] for key in settings}
    block.update(settings)
    found.data[HAPI_CONFIG_KEY] = yaml.safe_dump(parsed, sort_keys=False)
    _core().replace_namespaced_config_map(HAPI_CONFIGMAP, namespace, found)
    logger.info("configured hapi.fhir %s (was %s)", settings, before)
    return before


# --------------------------------------------------------------------------
# Restart and settle
# --------------------------------------------------------------------------

def restart(namespace, components, logger):
    """rollout restart, the same annotation bump kubectl uses."""
    unknown = [c for c in components if c not in DEPLOYMENTS]
    if unknown:
        raise kopf.PermanentError(
            "restart step names unknown component(s) %s; known: %s"
            % (", ".join(unknown), ", ".join(sorted(DEPLOYMENTS))))
    stamp = _stamp()
    for component in components:
        name = DEPLOYMENTS[component]
        _apps().patch_namespaced_deployment(name, namespace, {
            "spec": {"template": {"metadata": {"annotations": {
                "perf.fhir/restartedAt": stamp}}}}})
        logger.info("rollout restart %s/%s", namespace, name)
    return stamp


def deployments_ready(namespace, components):
    """(ready, detail) across the named components, with generation checked.

    observedGeneration guards against reading the pre-restart ReplicaSet as
    ready; updatedReplicas guards against reading the old pods as the new ones.
    """
    detail = {}
    ready = True
    for component in components:
        name = DEPLOYMENTS[component]
        try:
            found = _apps().read_namespaced_deployment(name, namespace)
        except client.ApiException as exc:
            if exc.status != 404:
                raise
            detail[component] = "absent"
            ready = False
            continue
        want = found.spec.replicas or 1
        status = found.status
        have = status.ready_replicas or 0
        updated = status.updated_replicas or 0
        current = status.replicas or 0
        settled = (status.observed_generation == found.metadata.generation
                   and have >= want and updated >= want and current == updated)
        detail[component] = "%d/%d ready, %d updated%s" % (
            have, want, updated, "" if settled else ", rolling")
        ready = ready and settled
    return ready, detail


def endpoint_serving(namespace, service):
    try:
        endpoints = _core().read_namespaced_endpoints(service, namespace)
    except client.ApiException as exc:
        if exc.status != 404:
            raise
        return False
    return any((subset.addresses or []) for subset in (endpoints.subsets or []))


# --------------------------------------------------------------------------
# The exclusive lease on a stack (C5)
# --------------------------------------------------------------------------

def lease_name(stack_name):
    return "%s-%s" % (LEASE_NAMESPACE_SUFFIX, stack_name)


def acquire_lease(namespace, stack_name, holder, logger):
    """Two benchmarks against one stack is a PermanentError, not a queue.

    The create is the lock: a second holder loses on 409 rather than on a
    read-then-write race.
    """
    name = lease_name(stack_name)
    # A plain dict, not a V1Lease: kopf.adopt mutates what it is given, and
    # adopting a throwaway to_dict() copy would leave the real body with no
    # ownerReference and the lease outliving the benchmark that took it.
    body = {
        "apiVersion": "coordination.k8s.io/v1", "kind": "Lease",
        "metadata": {"name": name, "namespace": namespace,
                     "labels": {"app": "fhir-benchmark", "stack": stack_name}},
        # No leaseDurationSeconds: this is an ownership lock, not a
        # heartbeat. Nothing renews it and nothing may take it over -- it is
        # released when the run ends or when the benchmark is deleted.
        "spec": {"holderIdentity": holder,
                 "acquireTime": _now().strftime("%Y-%m-%dT%H:%M:%S.%fZ")},
    }
    kopf.adopt(body)
    try:
        _coordination().create_namespaced_lease(namespace, body)
        logger.info("lease %s/%s acquired by %s", namespace, name, holder)
        return
    except client.ApiException as exc:
        if exc.status != 409:
            raise
    existing = _coordination().read_namespaced_lease(name, namespace)
    held_by = (existing.spec.holder_identity if existing.spec else None) or "unknown"
    if held_by == holder:
        return
    raise kopf.PermanentError(
        "FhirStack %s/%s is already leased by %s. Two benchmarks against one stack "
        "would each see the other's restarts and cache warming" % (namespace, stack_name, held_by))


def release_lease(namespace, stack_name, holder, logger):
    name = lease_name(stack_name)
    try:
        existing = _coordination().read_namespaced_lease(name, namespace)
    except client.ApiException as exc:
        if exc.status != 404:
            raise
        return
    held_by = (existing.spec.holder_identity if existing.spec else None) or ""
    if held_by != holder:
        logger.info("lease %s/%s is held by %s, not releasing", namespace, name, held_by)
        return
    _coordination().delete_namespaced_lease(name, namespace)
    logger.info("lease %s/%s released", namespace, name)

# Created by claude-opus-5
"""Bring pre-existing HAPI FHIR stacks under the operator.

Finds namespaces that have a hapi-fhir deployment but no FhirStack, reads the
resources actually set on the running containers, and writes a FhirStack whose
spec reproduces them. The operator then owns the stack: owner references are
set, and editing the CR resizes it.

The generated spec matches what is running, so nothing is resized on adoption.
The pods do restart once, because the operator adds fields the stacks were
deployed without -- CPU requests on hapi-fhir, node.processors on
elasticsearch. Data lives on PVCs and survives that.

Every adopted stack gets protected: true. Children carry owner references
after adoption, so deleting the FhirStack would delete its PVCs.

    python3 adopt.py                                   # show what would be created
    python3 adopt.py --only default --apply            # adopt one namespace
    python3 adopt.py --only perf-s --cap-memory 32 --apply
"""

import sys

import yaml
from kubernetes import client, config

GROUP = "perf.fhir"
VERSION = "v1alpha1"
PLURAL = "fhirstacks"

# spec key -> (deployment, container)
COMPONENTS = {
    "hapi": ("hapi-fhir", "hapi-fhir"),
    "postgres": ("hapi-fhir-db", "postgres"),
    "elasticsearch": ("hapi-fhir-es", "elasticsearch"),
}

_MEMORY_UNITS = [("Ki", 1 / 1048576), ("Mi", 1 / 1024), ("Gi", 1.0), ("Ti", 1024.0),
                 ("K", 1e3 / 2 ** 30), ("M", 1e6 / 2 ** 30), ("G", 1e9 / 2 ** 30)]


def cores(quantity):
    if not quantity:
        return None
    text = str(quantity)
    return float(text[:-1]) / 1000 if text.endswith("m") else float(text)


def gibibytes(quantity):
    if not quantity:
        return None
    text = str(quantity)
    for suffix, factor in _MEMORY_UNITS:
        if text.endswith(suffix):
            return round(float(text[: -len(suffix)]) * factor, 4)
    return round(float(text) / 2 ** 30, 4)


# A container with no CPU set at all gets these rather than the operator's
# own defaults. The operator always emits a CPU request, so adoption cannot
# leave one unset -- but inheriting a 2-core default would add 2000m of
# scheduling pressure per stack to a node that is already near full. A small
# request with a generous limit is the closest thing to "as it was".
UNSET_CPU = {"min": 0.25, "max": 4.0}

# CPU request floors. These stacks were deployed with requests == limits at
# 24 and 10 cores, which is what fills the node. Adoption restarts the pod,
# and Recreate terminates before it schedules, so a 24-core request has to be
# available all at once or Postgres never comes back. Dropping the request
# while keeping the limit lets it reschedule and still burst to 24.
CPU_FLOOR = {"hapi": 0.25, "postgres": 4.0, "elasticsearch": 2.0}


def observe(apps, namespace, cap_memory=None):
    """Resources actually set on the running containers, as a FhirStack spec.

    cap_memory clamps both ends of a component's memory to that many GiB.
    Both ends, not just the request: Postgres shared_buffers and the
    Elasticsearch heap are derived from the *limit* and committed at startup,
    so lowering only the request would leave a Burstable pod whose heap is
    larger than anything it is guaranteed.
    """
    resources = {}
    for key, (deployment, container_name) in COMPONENTS.items():
        try:
            found = apps.read_namespaced_deployment(name=deployment, namespace=namespace)
        except client.ApiException:
            continue
        container = next(
            (c for c in found.spec.template.spec.containers if c.name == container_name), None)
        if container is None or container.resources is None:
            continue

        requests = container.resources.requests or {}
        limits = container.resources.limits or {}
        block = {}
        for kind, convert in (("cpu", cores), ("memory", gibibytes)):
            low, high = convert(requests.get(kind)), convert(limits.get(kind))
            if low is None and high is None:
                if kind == "cpu":
                    block["cpu"] = dict(UNSET_CPU)
                continue
            pair = {}
            if low is not None:
                pair["min"] = min(low, CPU_FLOOR[key]) if kind == "cpu" else low
            if high is not None:
                pair["max"] = high
            if kind == "memory" and cap_memory is not None:
                pair = {"min": min(pair.get("min", cap_memory), cap_memory),
                        "max": min(pair.get("max", cap_memory), cap_memory)}
            block[kind] = pair
        if block:
            resources[key] = block

    # Always burst. Every component that already sets CPU has requests ==
    # limits, and burst reproduces that exactly (min and max are equal), so
    # nothing there changes. strict would instead raise the unset-CPU
    # containers' requests to their limit, which is the opposite of what
    # adoption should do.
    return {"protected": True, "cpuLimitPolicy": "burst", "resources": resources}


def candidates(apps, custom):
    """Namespaces with a hapi-fhir deployment and no FhirStack yet."""
    owned = set()
    try:
        for item in custom.list_cluster_custom_object(GROUP, VERSION, PLURAL).get("items", []):
            owned.add(item["metadata"]["namespace"])
    except client.ApiException as exc:
        if exc.status != 404:
            raise

    out = []
    for item in apps.list_deployment_for_all_namespaces().items:
        if item.metadata.name != "hapi-fhir":
            continue
        namespace = item.metadata.namespace
        if namespace in owned:
            print("  skip %s: already has a FhirStack" % namespace, file=sys.stderr)
            continue
        out.append(namespace)
    return sorted(out)


def main(argv):
    apply_it = "--apply" in argv
    only = [argv[i + 1] for i, a in enumerate(argv) if a == "--only" and i + 1 < len(argv)]
    cap = next((float(argv[i + 1]) for i, a in enumerate(argv)
                if a == "--cap-memory" and i + 1 < len(argv)), None)

    config.load_kube_config()
    apps = client.AppsV1Api()
    custom = client.CustomObjectsApi()

    docs = []
    for namespace in candidates(apps, custom):
        if only and namespace not in only:
            continue
        docs.append({
            "apiVersion": "%s/%s" % (GROUP, VERSION),
            "kind": "FhirStack",
            "metadata": {"name": namespace, "namespace": namespace},
            "spec": observe(apps, namespace, cap),
        })

    if not docs:
        print("nothing to adopt", file=sys.stderr)
        return 0

    print(yaml.safe_dump_all(docs, sort_keys=False, default_flow_style=False))
    if not apply_it:
        print("re-run with --apply to create these", file=sys.stderr)
        return 0

    for doc in docs:
        namespace = doc["metadata"]["namespace"]
        custom.create_namespaced_custom_object(GROUP, VERSION, namespace, PLURAL, doc)
        print("created FhirStack %s/%s" % (namespace, doc["metadata"]["name"]), file=sys.stderr)
    return 0


if __name__ == "__main__":
    sys.exit(main(sys.argv[1:]))

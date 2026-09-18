# Created by claude-opus-5
"""Flask UI that CRUDs every CRD in the operator's API group.

Nothing here is FhirStack-specific. It discovers the CRDs at request time,
generates a form from each one's OpenAPI schema, and applies what you submit.
Add a CRD to the cluster and a tab for it appears -- no change to this file.

A raw YAML box sits under every form as the escape hatch for anything the
generated form cannot express (arrays of objects, preserve-unknown-fields).
YAML wins when both are filled in.

Runs in a thread inside the operator process; started from
fhir_operator.startup() via start().
"""

import datetime
import json
import logging
import os
import queue
import threading
import time

import requests
import yaml
from flask import (Flask, Response, redirect, render_template_string, request,
                   url_for)
from kubernetes import client

import catalogue
import datasets
import gcs
import stats

GROUP = "perf.fhir"
DATASET_PLURAL = "fhirdatasets"
BENCHMARK_PLURAL = "fhirbenchmarks"
PROM_URL = os.environ.get("PROM_URL_TEMPLATE", "http://prometheus.%s.svc:9090")
PROM_TIMEOUT = float(os.environ.get("PROM_TIMEOUT", "2"))

log = logging.getLogger("ui")

app = Flask(__name__)

_dyn = None


def start(port, dyn):
    """Serve the UI on a daemon thread. Called once, at operator startup."""
    global _dyn
    _dyn = dyn
    thread = threading.Thread(
        target=lambda: app.run(host="0.0.0.0", port=port, threaded=True, debug=False),
        daemon=True, name="ui")
    thread.start()
    return thread


def _api():
    return client.ApiextensionsV1Api(_dyn().client)


def _custom():
    return client.CustomObjectsApi(_dyn().client)


def _core():
    return client.CoreV1Api(_dyn().client)


def _batch():
    return client.BatchV1Api(_dyn().client)


# --------------------------------------------------------------------------
# Discovery
# --------------------------------------------------------------------------

def crds():
    """Every CRD in GROUP, as {plural, kind, version, schema, scope}."""
    out = []
    for item in _api().list_custom_resource_definition().items:
        if item.spec.group != GROUP:
            continue
        served = [v for v in item.spec.versions if v.storage] or list(item.spec.versions)
        version = served[0]
        schema = {}
        if version.schema and version.schema.open_apiv3_schema:
            full = version.schema.open_apiv3_schema.to_dict()
            schema = (full.get("properties") or {}).get("spec") or {}
        out.append({
            "plural": item.spec.names.plural,
            "kind": item.spec.names.kind,
            "version": version.name,
            "scope": item.spec.scope,
            "schema": schema,
        })
    return sorted(out, key=lambda c: c["kind"])


def find(plural):
    for crd in crds():
        if crd["plural"] == plural:
            return crd
    return None


# --------------------------------------------------------------------------
# Schema -> flat field list, and back again
# --------------------------------------------------------------------------

def fields(schema, prefix="", depth=0):
    """Flatten an OpenAPI object schema into renderable rows.

    Dotted paths become the form field names, which is what lets build()
    reassemble an arbitrarily nested spec without knowing the CRD.
    """
    out = []
    props = (schema or {}).get("properties") or {}
    for name in sorted(props):
        sub = props[name] or {}
        path = "%s.%s" % (prefix, name) if prefix else name
        kind = sub.get("type") or "string"

        if kind == "object" and sub.get("properties"):
            out.append({"path": path, "name": name, "type": "group", "depth": depth,
                        "desc": sub.get("description") or ""})
            out.extend(fields(sub, path, depth + 1))
            continue
        if kind == "object" or kind == "array":
            # No usable sub-schema: hand it to the YAML box instead.
            kind = "yaml"

        out.append({
            "path": path, "name": name, "type": kind, "depth": depth,
            "enum": sub.get("enum"), "default": sub.get("default"),
            "desc": sub.get("description") or "",
        })
    return out


# Spec fields the schema makes look togglable but that only take effect at
# creation. codes.drawFrom is resolved against the ontology service once and
# frozen into a ConfigMap, so a row control for it silently does nothing.
FROZEN = ("codes.drawFrom",)


def flags(flat):
    """Spec fields worth a one-click control in the list: booleans and short
    enums. Derived from the schema, so a new flag on any CRD just appears."""
    out = []
    for field in flat:
        if field["path"] in FROZEN:
            continue
        if field["type"] == "boolean":
            out.append({"path": field["path"], "name": field["name"],
                        "kind": "boolean", "options": ["true", "false"]})
        elif field.get("enum") and len(field["enum"]) <= 3:
            out.append({"path": field["path"], "name": field["name"],
                        "kind": "enum", "options": [str(o) for o in field["enum"]]})
    return out


def set_field(crd, namespace, name, path, raw, flat):
    """Change one spec field and nothing else.

    A merge patch, not the form's full replace: toggling a flag from the list
    must not depend on every other field being present in the request.
    """
    kind = next((f["type"] for f in flat if f["path"] == path), "string")
    value = coerce(raw, kind)
    body = {}
    node = body
    parts = path.split(".")
    for part in parts[:-1]:
        node = node.setdefault(part, {})
    node[parts[-1]] = value
    _custom().patch_namespaced_custom_object(
        GROUP, crd["version"], namespace, crd["plural"], name, {"spec": body})
    return "%s/%s %s = %s" % (namespace, name, path, value)


def coerce(raw, kind):
    if kind in ("number",):
        return float(raw)
    if kind in ("integer",):
        return int(raw)
    if kind == "boolean":
        return raw == "true"
    if kind == "yaml":
        return yaml.safe_load(raw)
    return raw


def build(form, flat):
    """Reassemble a nested spec from dotted form keys."""
    spec = {}
    for field in flat:
        if field["type"] == "group":
            continue
        raw = (form.get(field["path"]) or "").strip()
        if raw == "":
            continue
        value = coerce(raw, field["type"])
        if value is None:
            continue
        node = spec
        parts = field["path"].split(".")
        for part in parts[:-1]:
            node = node.setdefault(part, {})
        node[parts[-1]] = value
    return spec


def lookup(spec, path):
    """Current value at a dotted path, or '' if absent."""
    node = spec or {}
    for part in path.split("."):
        if not isinstance(node, dict) or part not in node:
            return ""
        node = node[part]
    if isinstance(node, (dict, list)):
        return yaml.safe_dump(node, sort_keys=False).strip()
    if isinstance(node, bool):
        return "true" if node else "false"
    return node


# --------------------------------------------------------------------------
# Objects
# --------------------------------------------------------------------------

def objects(crd):
    try:
        found = _custom().list_cluster_custom_object(GROUP, crd["version"], crd["plural"])
    except client.ApiException as exc:
        return [], "%s %s" % (exc.status, exc.reason)
    out = []
    for item in found.get("items", []):
        spec = item.get("spec") or {}
        status = item.get("status") or {}
        out.append({
            "namespace": item["metadata"].get("namespace", ""),
            "name": item["metadata"]["name"],
            "created": item["metadata"].get("creationTimestamp", ""),
            # A CR with a deletionTimestamp is being deleted and kopf has
            # stopped running its timers, so status.phase is frozen from that
            # moment. Without this the row looks healthy and stale at once.
            "deleting": item["metadata"].get("deletionTimestamp", ""),
            "phase": status.get("phase", ""),
            "summary": summary(status),
            "job": status.get("jobName", ""),
            "status": status,
            "age": age(parse_time(item["metadata"].get("creationTimestamp"))),
            "spec": spec,
            "yaml": yaml.safe_dump(spec, sort_keys=False, default_flow_style=False).strip(),
        })
    return sorted(out, key=lambda o: (o["namespace"], o["name"])), None


def apply_object(crd, namespace, name, spec):
    """Create the object, or replace its spec outright if it exists.

    Replace rather than server-side apply. SSA only removes fields that the
    *applying* manager already owned, so clearing a box in the form left the
    value in place whenever some other manager -- adopt.py's create, kubectl,
    a previous kopf write -- owned that field. Replace makes the form the
    single source of truth: what you submit is the whole spec, and an emptied
    field really is absent.
    """
    if crd["scope"] == "Namespaced":
        try:
            _core().create_namespace({"metadata": {"name": namespace}})
        except client.ApiException as exc:
            if exc.status != 409:
                raise

    try:
        existing = _custom().get_namespaced_custom_object(
            GROUP, crd["version"], namespace, crd["plural"], name)
    except client.ApiException as exc:
        if exc.status != 404:
            raise
        existing = None

    if existing is None:
        _custom().create_namespaced_custom_object(
            GROUP, crd["version"], namespace, crd["plural"],
            {
                "apiVersion": "%s/%s" % (GROUP, crd["version"]),
                "kind": crd["kind"],
                "metadata": {"name": name, "namespace": namespace},
                "spec": spec,
            })
        return

    # Carries resourceVersion, so a concurrent edit loses rather than silently
    # clobbering.
    existing["spec"] = spec
    _custom().replace_namespaced_custom_object(
        GROUP, crd["version"], namespace, crd["plural"], name, existing)


# --------------------------------------------------------------------------
# Routes
# --------------------------------------------------------------------------

# ---------------------------------------------------------------- progress


def summary(status):
    """One-line status for the object table.

    FhirStack reports status.components; FhirDataset never does -- it reports
    observed/expected counts -- so fall back to those rather than printing '-'.
    """
    components = status.get("components") or {}
    if components:
        return ", ".join("%s=%s" % (k, v) for k, v in sorted(components.items()))
    return ", ".join("%s=%s" % (k, v) for k, v in progress(status))


def progress(status):
    """[(type, "have/want"), ...] from status.observed against status.expected."""
    observed = status.get("observed") or {}
    wanted = status.get("expected") or {}
    out = []
    for key in sorted(set(observed) | set(wanted)):
        have = observed.get(key, 0)
        want = wanted.get(key)
        out.append((key, "%s/%s" % (have, want) if want is not None else str(have)))
    return out


# The types the loader counts, in the order a patient record builds them up.
# Anything else status.observed mentions is appended, so a new type in the
# loader shows up here without a change.
TYPES = ("Patient", "Condition", "Observation")


def resource_rows(status):
    """[{kind, have, want, pct, done}, ...] for the per-row progress bars.

    progress() renders the same numbers as "have/want" text; this keeps them
    as numbers so the template can size a bar and colour a finished type.
    """
    observed = status.get("observed") or {}
    wanted = status.get("expected") or {}
    known = [t for t in TYPES if t in observed or t in wanted]
    extra = sorted((set(observed) | set(wanted)) - set(TYPES))
    out = []
    for kind in known + extra:
        have = int(observed.get(kind, 0) or 0)
        want = wanted.get(kind)
        want = int(want) if want is not None else None
        pct = min(100, int(100 * have / want)) if want else 0
        out.append({"kind": kind, "have": have, "want": want, "pct": pct,
                    "done": bool(want) and have >= want})
    return out


def parse_time(raw):
    """RFC3339 off the API server -> aware datetime, or None if unparsable.

    metadata timestamps arrive as datetimes through the typed client but as
    strings through the dynamic one, and age() only takes datetimes.
    """
    if raw is None or isinstance(raw, datetime.datetime):
        return raw
    try:
        return datetime.datetime.fromisoformat(str(raw).replace("Z", "+00:00"))
    except ValueError:
        return None


def age(stamp, until=None):
    if stamp is None:
        return "-"
    end = until or datetime.datetime.now(datetime.timezone.utc)
    seconds = int((end - stamp).total_seconds())
    if seconds < 0:
        return "-"
    if seconds < 60:
        return "%ds" % seconds
    if seconds < 3600:
        return "%dm%02ds" % (seconds // 60, seconds % 60)
    return "%dh%02dm" % (seconds // 3600, (seconds % 3600) // 60)


def fmt(value, digits=0):
    if value is None:
        return "-"
    if digits:
        return ("%%.%df" % digits) % value
    return "{:,}".format(int(round(value)))


# -------------------------------------------------------------- jobs, pods


def job_row(job):
    """One Job as the templates want it."""
    status = job.status
    return {
        "name": job.metadata.name,
        "mode": (job.metadata.labels or {}).get("mode", ""),
        "phase": datasets.job_phase(job) or "-",
        "completions": "%d/%d" % (status.succeeded or 0, job.spec.completions or 1),
        "succeeded": status.succeeded or 0,
        "wanted": job.spec.completions or 1,
        "active": status.active or 0,
        "failed": status.failed or 0,
        "parallelism": job.spec.parallelism or 1,
        "created": job.metadata.creation_timestamp,
        "age": age(job.metadata.creation_timestamp),
        "duration": age(status.start_time, status.completion_time)
                    if status.start_time else "-",
    }


def jobs_for(namespace, dataset):
    """Every loader Job for one dataset, newest first.

    datasets.running_job() returns only the newest; the detail page wants the
    whole attempt history, so list against the same label the operator sets.
    """
    try:
        found = _batch().list_namespaced_job(
            namespace, label_selector="dataset=%s" % dataset).items
    except client.ApiException as exc:
        return [], "%s %s" % (exc.status, exc.reason)
    found.sort(key=lambda j: j.metadata.creation_timestamp, reverse=True)
    return [job_row(j) for j in found], None


def jobs_index():
    """{(namespace, dataset): [job, ...]} for every loader Job in the cluster.

    One list call rather than one per row: the list page shows every dataset
    at once, and a per-dataset lookup made the page cost grow with the number
    of datasets on it.
    """
    try:
        found = _batch().list_job_for_all_namespaces(label_selector="dataset").items
    except client.ApiException as exc:
        return {}, "%s %s" % (exc.status, exc.reason)
    found.sort(key=lambda j: j.metadata.creation_timestamp, reverse=True)
    out = {}
    for job in found:
        key = (job.metadata.namespace, (job.metadata.labels or {}).get("dataset"))
        out.setdefault(key, []).append(job_row(job))
    return out, None


def pods_for(namespace, name, app="fhir-loader", key="dataset"):
    """Worker pods for one object, so a failing worker is visible by name.

    Parameterised by label rather than hard-coded: a benchmark's pods differ
    from a loader's only in which two labels they carry.
    """
    try:
        found = _core().list_namespaced_pod(
            namespace,
            label_selector="app=%s,%s=%s" % (app, key, name)).items
    except client.ApiException as exc:
        return [], "%s %s" % (exc.status, exc.reason)
    found.sort(key=lambda p: p.metadata.name)
    out = []
    for pod in found:
        labels = pod.metadata.labels or {}
        annotations = pod.metadata.annotations or {}
        states = pod.status.container_statuses or []
        reason = ""
        for state in states:
            current = state.state
            if current and current.waiting and current.waiting.reason:
                reason = current.waiting.reason
            elif current and current.terminated and current.terminated.reason:
                reason = current.terminated.reason
        out.append({
            "name": pod.metadata.name,
            "job": labels.get("job-name", ""),
            "index": annotations.get("batch.kubernetes.io/job-completion-index", ""),
            "phase": pod.status.phase or "-",
            "reason": reason,
            "node": pod.spec.node_name or "-",
            "restarts": sum(s.restart_count or 0 for s in states),
            "age": age(pod.status.start_time),
        })
    return out, None



# --------------------------------------------------------------------- logs
#
# Live tail only. The kubelet is the store: a pod's log exists while the pod
# object does and is rotated by the runtime under it, so nothing here can show
# a run whose pods have been reaped. datasets.py deliberately sets no
# ttlSecondsAfterFinished, which is what makes that good enough for now.


LOG_CONTAINER = "worker"
LOG_TAIL = int(os.environ.get("LOG_TAIL_LINES", "500"))
LOG_CHUNK = 4096
# Streams are capped rather than left open forever. EventSource reconnects by
# itself, so the cap costs a blink of re-tailed lines and bounds how long one
# forgotten tab can hold a watch open against the API server.
LOG_STREAM_SECONDS = float(os.environ.get("LOG_STREAM_SECONDS", "1800"))
LOG_HEARTBEAT = float(os.environ.get("LOG_HEARTBEAT_SECONDS", "15"))
LOG_QUEUE = 10000

EPOCH = datetime.datetime.fromtimestamp(0, datetime.timezone.utc)


def open_log(namespace, pod, previous=False, timestamps=False, tail=LOG_TAIL,
             follow=True):
    """The raw urllib3 response carrying one pod's log.

    _preload_content=False is the whole point of this call: left on, the client
    buffers the entire body before returning, which with follow=True means
    "return when the pod exits" -- the opposite of a tail. The read timeout is
    the backstop that eventually frees a reader nobody closed.
    """
    return _core().read_namespaced_pod_log(
        pod, namespace, container=LOG_CONTAINER, follow=follow, previous=previous,
        timestamps=timestamps, tail_lines=tail,
        _request_timeout=(5, LOG_STREAM_SECONDS + 30), _preload_content=False)


def close_quietly(response):
    """Drop a log connection. Called from the request thread to break a reader
    thread out of a blocking read on a pod that has gone quiet."""
    for name in ("close", "release_conn"):
        method = getattr(response, name, None)
        if method is None:
            continue
        try:
            method()
        except Exception as exc:
            log.debug("closing pod log: %s: %s", type(exc).__name__, exc)


def event_stamp(event):
    """Whichever of an Event's three timestamps is populated."""
    stamp = event.last_timestamp or event.event_time
    if stamp is None and event.metadata is not None:
        stamp = event.metadata.creation_timestamp
    return stamp


def pod_events(namespace, pod):
    """What kubectl describe would show, oldest first.

    A pod stuck in ImagePullBackOff or unschedulable has no log at all, and an
    empty pane reads as a broken feature rather than as the answer. Its events
    say exactly what went wrong.
    """
    try:
        found = _core().list_namespaced_event(
            namespace, field_selector="involvedObject.name=%s" % pod).items
    except client.ApiException as exc:
        yield "--- events unavailable: %s %s" % (exc.status, exc.reason)
        return
    for event in sorted(found, key=lambda e: event_stamp(e) or EPOCH):
        yield "--- %s %s: %s" % (event.type, event.reason, event.message)


def log_lines(namespace, pod, on_open=None, **kw):
    """Yield one pod's log a line at a time.

    The API streams bytes, not lines, and a chunk boundary lands mid-line often
    enough that splitting each chunk on its own mangles the output; the tail of
    every chunk is held back until its newline arrives. on_open hands the live
    response to the caller so it can be closed from another thread.
    """
    try:
        response = open_log(namespace, pod, **kw)
    except client.ApiException as exc:
        yield "--- no log from %s: %s %s" % (pod, exc.status, exc.reason)
        for line in pod_events(namespace, pod):
            yield line
        return

    if on_open is not None:
        on_open(response)
    pending = b""
    try:
        for chunk in response.stream(LOG_CHUNK, decode_content=True):
            pending += chunk
            parts = pending.split(b"\n")
            pending = parts.pop()
            for part in parts:
                yield part.decode("utf-8", "replace")
        if pending:
            yield pending.decode("utf-8", "replace")
    finally:
        close_quietly(response)


def sse(event, payload):
    """One Server-Sent Event. The payload is JSON so that a log line carrying a
    newline -- a stack trace, mostly -- cannot end the frame early."""
    return "event: %s\ndata: %s\n\n" % (event, json.dumps(payload))


def log_stream(namespace, targets, previous=False, timestamps=False, tail=LOG_TAIL,
               heartbeat=LOG_HEARTBEAT, deadline=None):
    """SSE body tailing every pod in targets at once.

    A loader Job runs `parallelism` indexed workers and the interesting failure
    is usually one of them, so the merged view is the one worth having: a reader
    thread per pod feeds one queue, and each line is tagged with its pod and
    completion index for the client to colour and filter by.

    Ends with a `done` event once every pod's log has ended. Hitting the cap
    instead just closes, which is EventSource's cue to reconnect -- at the cost
    of re-tailing, since the pod log API has no resume point to hand back.
    """
    if deadline is None:
        deadline = time.monotonic() + LOG_STREAM_SECONDS
    pending = queue.Queue(maxsize=LOG_QUEUE)
    stop = threading.Event()
    lock = threading.Lock()
    state = {"closed": False, "responses": []}

    def register(response):
        with lock:
            if state["closed"]:
                closed_now = True
            else:
                state["responses"].append(response)
                closed_now = False
        if closed_now:
            close_quietly(response)

    def push(item):
        """Block until there is room, but never past a stop: a browser that
        stopped reading must not pin a reader thread on a full queue."""
        while not stop.is_set():
            try:
                pending.put(item, timeout=0.5)
                return
            except queue.Full:
                continue

    def reader(target):
        pod, index = target["name"], target.get("index", "")
        try:
            for line in log_lines(namespace, pod, on_open=register, previous=previous,
                                  timestamps=timestamps, tail=tail):
                if stop.is_set():
                    return
                push(("line", {"pod": pod, "index": index, "text": line}))
        except Exception as exc:
            log.info("tail of %s/%s ended: %s: %s", namespace, pod,
                     type(exc).__name__, exc)
            push(("line", {"pod": pod, "index": index,
                           "text": "--- tail ended: %s: %s" % (type(exc).__name__, exc)}))
        finally:
            push(("eof", {"pod": pod, "index": index}))

    for target in targets:
        threading.Thread(target=reader, args=(target,), daemon=True,
                         name="log-%s" % target["name"]).start()

    live = len(targets)
    try:
        yield sse("open", {"pods": [t["name"] for t in targets],
                           "previous": previous, "timestamps": timestamps})
        while live:
            remaining = deadline - time.monotonic()
            if remaining <= 0:
                return
            try:
                kind, payload = pending.get(timeout=min(heartbeat, remaining))
            except queue.Empty:
                # A comment, not an event. It keeps the connection warm through
                # the port-forward and, more usefully, makes an abandoned tab
                # fail a write within one heartbeat instead of never.
                yield ": ping\n\n"
                continue
            if kind == "eof":
                live -= 1
            yield sse(kind, payload)
        yield sse("done", {"pods": [t["name"] for t in targets]})
    finally:
        stop.set()
        with lock:
            state["closed"] = True
            leftover, state["responses"] = state["responses"], []
        for response in leftover:
            close_quietly(response)


def log_targets(namespace, owner, pod=None, job=None, app="fhir-loader",
                key="dataset"):
    """Which pods a log request means.

    The operator holds cluster-wide credentials, so the pod name off the query
    string is resolved against this object's own pods rather than handed to the
    API as given.
    """
    found, error = pods_for(namespace, owner, app, key)
    if error:
        return [], error
    if job:
        found = [p for p in found if p["job"] == job]
    if pod and pod != "all":
        found = [p for p in found if p["name"] == pod]
    return found, None


# ------------------------------------------------------------------ metrics


COUNTER_KEYS = {
    "fhir_load_resources_total": "resources",
    "fhir_load_patients_total": "patients",
    "fhir_delete_resources_total": "deleted",
}

ADDITIVE = ("resources", "patients", "bundles_ok", "bundles_failed", "deleted",
            "resources_per_sec", "bundles_per_sec")

COUNTER_QUERY = ('{__name__=~"fhir_load_resources_total|fhir_load_patients_total'
                 '|fhir_load_bundles_total|fhir_delete_resources_total",dataset="%s"}')

RATE_QUERIES = (
    ("resources_per_sec",
     'sum by (pod) (rate(fhir_load_resources_total{dataset="%s"}[1m]))'),
    ("bundles_per_sec",
     'sum by (pod) (rate(fhir_load_bundle_seconds_count{dataset="%s"}[1m]))'),
    ("p95_seconds",
     'histogram_quantile(0.95, sum by (pod, le) '
     '(rate(fhir_load_bundle_seconds_bucket{dataset="%s"}[5m])))'),
)


def _escape(value):
    return str(value).replace("\\", "\\\\").replace('"', '\\"')


def _number(raw):
    try:
        value = float(raw)
    except (TypeError, ValueError):
        return None
    return None if value != value else value


def prom_query(namespace, query):
    """Instant query against the Prometheus datasets.ensure_prometheus() put in
    this namespace. Each dataset namespace runs its own, so there is no global
    endpoint to fan in to."""
    response = requests.get("%s/api/v1/query" % (PROM_URL % namespace),
                            params={"query": query}, timeout=PROM_TIMEOUT)
    response.raise_for_status()
    body = response.json()
    if body.get("status") != "success":
        raise ValueError(body.get("error") or "query failed")
    return (body.get("data") or {}).get("result") or []


def metrics(namespace, dataset):
    """Per-pod loader metrics, plus a totals row.

    Counters are per worker process, so a pod that has finished and been reaped
    stops being scraped -- its totals stay in TSDB but its rates decay to zero.
    """
    rows = {}
    errors = []
    escaped = _escape(dataset)

    def cell(pod, key, value, add=False):
        row = rows.setdefault(pod, {})
        if add:
            row[key] = (row.get(key) or 0) + (value or 0)
        else:
            row[key] = value

    try:
        for series in prom_query(namespace, COUNTER_QUERY % escaped):
            metric = series.get("metric") or {}
            pod = metric.get("pod") or "-"
            value = _number((series.get("value") or [None, None])[1])
            name = metric.get("__name__")
            if name == "fhir_load_bundles_total":
                key = "bundles_ok" if metric.get("status") == "ok" else "bundles_failed"
            else:
                key = COUNTER_KEYS.get(name)
            if key:
                cell(pod, key, value, add=True)
    except Exception as exc:
        errors.append("counters: %s: %s" % (type(exc).__name__, exc))

    for key, template in RATE_QUERIES:
        try:
            for series in prom_query(namespace, template % escaped):
                pod = (series.get("metric") or {}).get("pod") or "-"
                cell(pod, key, _number((series.get("value") or [None, None])[1]))
        except Exception as exc:
            errors.append("%s: %s: %s" % (key, type(exc).__name__, exc))

    out = []
    for pod in sorted(rows):
        row = dict(rows[pod])
        row["pod"] = pod
        out.append(row)

    totals = {}
    for key in ADDITIVE:
        seen = [r[key] for r in out if r.get(key) is not None]
        if seen:
            totals[key] = sum(seen)

    return out, totals, "; ".join(errors) or None



# ------------------------------------------------------------- benchmarks
#
# A FhirBenchmark is not a state to converge on, so its row shows different
# things from a FhirDataset's: where the plan has got to, what each step did,
# and the percentiles that came out. The journal is the primary output, not a
# summary of live state, so it is rendered in full rather than reduced.


def benchmark_jobs_index():
    """{(namespace, benchmark): [job, ...]} for every benchmark Job."""
    try:
        found = _batch().list_job_for_all_namespaces(
            label_selector="app=fhir-benchmark").items
    except client.ApiException as exc:
        return {}, "%s %s" % (exc.status, exc.reason)
    found.sort(key=lambda j: j.metadata.creation_timestamp, reverse=True)
    out = {}
    for job in found:
        labels = job.metadata.labels or {}
        out.setdefault((job.metadata.namespace, labels.get("benchmark")), []).append(
            benchmark_job_row(job))
    return out, None


def benchmark_job_row(job):
    row = job_row(job)
    labels = job.metadata.labels or {}
    row["step"] = labels.get("step", "")
    row["run"] = labels.get("run", "")
    return row


def benchmark_jobs_for(namespace, name):
    try:
        found = _batch().list_namespaced_job(
            namespace, label_selector="app=fhir-benchmark,benchmark=%s" % name).items
    except client.ApiException as exc:
        return [], "%s %s" % (exc.status, exc.reason)
    found.sort(key=lambda j: j.metadata.creation_timestamp, reverse=True)
    return [benchmark_job_row(j) for j in found], None


def journal_rows(status):
    """The append-only step journal, oldest first, with durations."""
    out = []
    for item in sorted(status.get("journal") or [], key=lambda e: e.get("index", 0)):
        started = parse_time(item.get("startedAt"))
        finished = parse_time(item.get("finishedAt"))
        out.append({
            "index": item.get("index"),
            "id": item.get("id", ""),
            "step": item.get("step", ""),
            "outcome": item.get("outcome", ""),
            "job": item.get("jobName", ""),
            "node": item.get("node", ""),
            "shards": item.get("shards"),
            "measurements": item.get("measurements"),
            "invalid": item.get("invalid"),
            "coldReadRatio": item.get("coldReadRatio"),
            "reasons": item.get("invalidReasons") or [],
            "detail": item.get("detail") or item.get("before") or {},
            "started": item.get("startedAt", ""),
            "duration": age(started, finished) if started and finished
                        else (age(started) if started else "-"),
        })
    return out


def summary_rows(summary):
    """One row per (case, cache label). Percentiles came from raw samples.

    Takes the summary rather than the status: results are read from GCS and
    nowhere else, so there is no status field left for this to fall back to.
    """
    summary = summary or {}
    out = []
    for case_id in sorted(summary):
        for label in sorted(summary[case_id]):
            entry = summary[case_id][label]
            out.append({
                "case": case_id, "cache": label,
                "family": entry.get("family", ""),
                "n": entry.get("n"), "invalid": entry.get("invalid"),
                "p50": entry.get("p50ms"), "p90": entry.get("p90ms"),
                "p95": entry.get("p95ms"), "p99": entry.get("p99ms"),
                "max": entry.get("maxms"),
                "rows": entry.get("rows"), "total": entry.get("bundleTotal"),
                "engine": entry.get("engine", ""),
                "expected": entry.get("engineExpected") or "",
                "reasons": entry.get("reasons") or [],
            })
    out.sort(key=lambda r: (-(r["p95"] or 0), r["case"]))
    return out


def delta_rows(summary):
    """Matched-pair deltas: the measured value of Elasticsearch, per shape.

    Derived on demand. stats.deltas is a pure function of the summary, so
    storing its output anywhere would only create a copy that can drift.
    """
    out = []
    for name, labels in sorted(stats.deltas(summary or {}).items()):
        for label, entry in sorted(labels.items()):
            out.append({"pair": name, "cache": label,
                        "eligible": entry.get("eligibleP50ms"),
                        "disqualified": entry.get("disqualifiedP50ms"),
                        "ratio": entry.get("ratio"),
                        "eligibleEngine": entry.get("eligibleEngine", ""),
                        "disqualifiedEngine": entry.get("disqualifiedEngine", "")})
    out.sort(key=lambda r: -(r["ratio"] or 0))
    return out



def runnable_targets():
    """(stacks, datasets) for the run-a-benchmark dropdowns.

    Both are listed cluster-wide with their phase, because a benchmark refuses
    to run against a stack that is not Ready or a dataset that is not fully
    loaded -- and it is better to see why in the dropdown than to find out from
    a PermanentError thirty seconds later.
    """
    stacks, datasets, errors = [], [], []
    for plural, out in (("fhirstacks", stacks), (DATASET_PLURAL, datasets)):
        try:
            found = _custom().list_cluster_custom_object(GROUP, "v1alpha1", plural)
        except client.ApiException as exc:
            errors.append("%s: %s %s" % (plural, exc.status, exc.reason))
            continue
        for item in found.get("items", []):
            status = item.get("status") or {}
            namespace = item["metadata"].get("namespace", "")
            name = item["metadata"]["name"]
            phase = status.get("phase", "")
            if plural == DATASET_PLURAL:
                expected = status.get("expected") or {}
                observed = status.get("observed") or {}
                loaded = bool(expected) and all(
                    int(observed.get(k, 0)) >= int(v) for k, v in expected.items())
                ready = phase == "Ready" and loaded
                note = "%s, %s patients" % (phase or "unset",
                                            fmt(observed.get("Patient", 0)))
            else:
                ready = phase == "Ready"
                note = phase or "unset"
            out.append({"key": "%s/%s" % (namespace, name), "namespace": namespace,
                        "name": name, "phase": phase, "ready": ready, "note": note})
    stacks.sort(key=lambda o: o["key"])
    datasets.sort(key=lambda o: o["key"])
    return stacks, datasets, "; ".join(errors) or None


def start_run(crd, form):
    """Create a FhirBenchmark over the whole catalogue from the run panel.

    A fresh object every time rather than a re-run of an existing one: spec is
    immutable after admission, so editing one in place is rejected by the API
    server. The name carries a timestamp for the same reason.
    """
    stack = (form.get("runStack") or "").strip()
    dataset = (form.get("runDataset") or "").strip()
    if not stack or not dataset:
        raise ValueError("pick a FhirStack and a FhirDataset")
    stack_ns, _, stack_name = stack.partition("/")
    dataset_ns, _, dataset_name = dataset.partition("/")
    if stack_ns != dataset_ns:
        raise ValueError(
            "FhirStack %s and FhirDataset %s are in different namespaces; a "
            "benchmark resolves both in its own namespace" % (stack, dataset))

    repeats = int(form.get("runRepeats") or 30)
    name = (form.get("runName") or "").strip() or "all-%s" % datetime.datetime.now(
        datetime.timezone.utc).strftime("%Y%m%d-%H%M%S")

    spec = {
        "stackRef": stack_name,
        "datasetRef": dataset_name,
        "state": "Run",
        "runId": 1,
        # No select block, so every case in the catalogue runs.
        "catalogue": {"name": "cohort-core"},
        # concurrency 1 is what makes the cases run one after another, and it
        # is also the only setting under which pg_stat_statements can attribute
        # an engine to a case.
        "defaults": {"concurrency": 1, "repeats": repeats, "warmup": 5,
                     "timeoutSeconds": 120, "bindingMode": "pin"},
    }
    apply_object(crd, dataset_ns, name, spec)
    return ("started %s %s/%s -- %d cases, %d repeats, sequential"
            % (crd["kind"], dataset_ns, name, len(catalogue.load("cohort-core")), repeats))


def decorate_benchmarks(found):
    """What a FhirBenchmark row shows under its main line."""
    index, error = benchmark_jobs_index()
    for obj in found:
        status = obj["status"]
        obj["journal"] = journal_rows(status)
        obj["jobs"] = index.get((obj["namespace"], obj["name"]), [])
        # No summary or deltas on the index. They live in GCS now, and
        # rendering a list of n benchmarks would mean n object reads to show
        # a table nobody reads at a glance. The detail page fetches them.
        obj["summary"] = []
        obj["deltas"] = []
        obj["bindings"] = sorted((status.get("bindings") or {}).items())
    return error


def decorate(found):
    """Attach the detail a FhirDataset row shows under its main line.

    Kept out of objects(), which stays CRD-agnostic: every other kind in the
    group renders from the schema alone.
    """
    index, error = jobs_index()
    for obj in found:
        status = obj["status"]
        obj["resources"] = resource_rows(status)
        obj["jobs"] = index.get((obj["namespace"], obj["name"]), [])
        obj["counted"] = age(parse_time(status.get("observedAt")))
    return error


@app.route("/")
def index():
    available = crds()
    if not available:
        return render_template_string(PAGE, crds=[], crd=None, objects=[], flat=[],
                                      editing=None, error="No CRDs in group %s." % GROUP,
                                      message=None)
    return redirect(url_for("kind", plural=available[0]["plural"]))


@app.route("/healthz")
def healthz():
    return "ok"


@app.route("/<plural>", methods=["GET", "POST"])
def kind(plural):
    crd = find(plural)
    if crd is None:
        return redirect(url_for("index"))
    flat = fields(crd["schema"])
    message = None

    if request.method == "POST":
        namespace = (request.form.get("namespace") or "").strip()
        name = (request.form.get("name") or "").strip()
        try:
            if request.form.get("action") == "run":
                message = start_run(crd, request.form)
            elif request.form.get("action") == "set":
                message = set_field(crd, namespace, name,
                                    request.form["field"], request.form["value"], flat)
            elif request.form.get("action") == "delete":
                _custom().delete_namespaced_custom_object(
                    GROUP, crd["version"], namespace, plural, name)
                message = "deleted %s %s/%s" % (crd["kind"], namespace, name)
            else:
                if not name:
                    raise ValueError("name is required")
                raw = (request.form.get("__yaml") or "").strip()
                spec = yaml.safe_load(raw) if raw else build(request.form, flat)
                if not isinstance(spec, dict):
                    raise ValueError("spec must be a mapping")
                apply_object(crd, namespace, name, spec)
                message = "applied %s %s/%s" % (crd["kind"], namespace, name)
        except client.ApiException as exc:
            message = "%s %s: %s" % (exc.status, exc.reason, exc.body)
        except (ValueError, yaml.YAMLError) as exc:
            message = "%s: %s" % (type(exc).__name__, exc)

    found, error = objects(crd)
    show_jobs = crd["plural"] == DATASET_PLURAL
    show_benchmark = crd["plural"] == BENCHMARK_PLURAL
    if show_jobs and not error:
        error = decorate(found)
    if show_benchmark and not error:
        error = decorate_benchmarks(found)
    editing = None
    want = request.args.get("edit")
    if want:
        editing = next((o for o in found if "%s/%s" % (o["namespace"], o["name"]) == want), None)

    stacks, dataset_targets, target_error = ([], [], None)
    if show_benchmark:
        stacks, dataset_targets, target_error = runnable_targets()

    return render_template_string(
        PAGE, crds=crds(), crd=crd, objects=found, flat=flat, flags=flags(flat),
        editing=editing, error=error or target_error, message=message,
        lookup=lookup, fmt=fmt,
        show_jobs=show_jobs, show_benchmark=show_benchmark,
        families=catalogue.families_in() if show_benchmark else [],
        catalogue_revision=catalogue.revision("cohort-core") if show_benchmark else "",
        case_count=len(catalogue.load("cohort-core")) if show_benchmark else 0,
        stacks=stacks, dataset_targets=dataset_targets)


@app.route("/fhirdatasets/<namespace>/<name>")
def dataset_detail(namespace, name):
    """Reconcile jobs, their pods and their Prometheus numbers for one dataset."""
    crd = find(DATASET_PLURAL)
    if crd is None:
        return redirect(url_for("index"))

    obj, error = None, None
    try:
        obj = _custom().get_namespaced_custom_object(
            GROUP, crd["version"], namespace, DATASET_PLURAL, name)
    except client.ApiException as exc:
        error = "%s %s" % (exc.status, exc.reason)

    status = (obj or {}).get("status") or {}
    jobs, job_error = jobs_for(namespace, name)
    pods, pod_error = pods_for(namespace, name)
    rows, totals, prom_error = metrics(namespace, name)

    return render_template_string(
        DETAIL, crds=crds(), crd=crd, namespace=namespace, name=name,
        obj=obj, status=status, progress=progress(status),
        jobs=jobs, pods=pods, rows=rows, totals=totals, fmt=fmt,
        error=error or job_error or pod_error, prom_error=prom_error,
        prom_url=PROM_URL % namespace)


@app.route("/fhirdatasets/<namespace>/<name>/logs")
def dataset_logs(namespace, name):
    """Live tail of the loader pods for one dataset."""
    crd = find(DATASET_PLURAL)
    if crd is None:
        return redirect(url_for("index"))

    pods, error = pods_for(namespace, name)
    job = request.args.get("job") or ""
    if job:
        pods = [p for p in pods if p["job"] == job]
    want = request.args.get("pod") or "all"
    if want != "all" and not any(p["name"] == want for p in pods):
        want = "all"

    return render_template_string(
        LOGS, crds=crds(), crd=crd, namespace=namespace, name=name,
        pods=pods, job=job, want=want, error=error,
        previous=request.args.get("previous") == "1",
        timestamps=request.args.get("ts") == "1",
        tail=LOG_TAIL, cap=int(LOG_STREAM_SECONDS // 60))


def _requested_targets(namespace, name):
    return log_targets(namespace, name, request.args.get("pod"),
                       request.args.get("job"))


@app.route("/fhirdatasets/<namespace>/<name>/logs/stream")
def dataset_log_stream(namespace, name):
    """Server-Sent Events: one `line` event per log line, `eof` per pod as it
    ends, `done` when they all have."""
    targets, error = _requested_targets(namespace, name)
    if error:
        return Response(error, status=502, mimetype="text/plain")
    if not targets:
        return Response("no such loader pod for %s/%s" % (namespace, name),
                        status=404, mimetype="text/plain")

    body = log_stream(namespace, targets,
                      previous=request.args.get("previous") == "1",
                      timestamps=request.args.get("ts") == "1")
    return Response(body, mimetype="text/event-stream", headers={
        "Cache-Control": "no-cache, no-transform",
        # nginx and friends will otherwise sit on the body until it is big
        # enough to be worth forwarding, which for a tail is forever.
        "X-Accel-Buffering": "no",
        "Connection": "keep-alive"})


@app.route("/fhirdatasets/<namespace>/<name>/logs/download")
def dataset_log_download(namespace, name):
    """The whole log, not the tail, as a file. follow=False, so this ends."""
    targets, error = _requested_targets(namespace, name)
    if error:
        return Response(error, status=502, mimetype="text/plain")
    if not targets:
        return Response("no such loader pod for %s/%s" % (namespace, name),
                        status=404, mimetype="text/plain")
    # Read every query parameter here, not inside body(): a streamed response
    # is iterated by the WSGI server after the request context has been torn
    # down, and touching `request` there raises.
    previous = request.args.get("previous") == "1"
    timestamps = request.args.get("ts") == "1"

    def body():
        for target in targets:
            if len(targets) > 1:
                yield "=== %s ===\n" % target["name"]
            for line in log_lines(namespace, target["name"], follow=False, tail=None,
                                  previous=previous, timestamps=timestamps):
                yield line + "\n"

    which = request.args.get("pod") or request.args.get("job") or "all"
    return Response(body(), mimetype="text/plain; charset=utf-8", headers={
        "Content-Disposition": 'attachment; filename="%s-%s%s.log"'
                               % (name, which, "-previous" if previous else "")})



@app.route("/fhirbenchmarks/<namespace>/<name>")
def benchmark_detail(namespace, name):
    """The whole journal and the whole summary for one run.

    status.journal is the primary output of a benchmark rather than a
    reflection of live state, so this page reads it as a record: what ran,
    in what order, on which node, and what came out.
    """
    crd = find(BENCHMARK_PLURAL)
    if crd is None:
        return redirect(url_for("index"))

    obj, error = None, None
    try:
        obj = _custom().get_namespaced_custom_object(
            GROUP, crd["version"], namespace, BENCHMARK_PLURAL, name)
    except client.ApiException as exc:
        error = "%s %s" % (exc.status, exc.reason)

    status = (obj or {}).get("status") or {}
    jobs, job_error = benchmark_jobs_for(namespace, name)
    pods, pod_error = pods_for(namespace, name, "fhir-benchmark", "benchmark")

    # The one read of measurements on this page, and it goes to GCS. A run with
    # no summary object shows an empty table and says so, rather than falling
    # back to a copy in status -- there is no such copy, by design.
    summary, summary_error = {}, None
    run_id = status.get("runId")
    if run_id:
        try:
            summary = gcs.read_summary(namespace, name, int(run_id))
        except Exception as exc:              # noqa: BLE001 - shown, not hidden
            summary_error = "results unavailable: %s: %s" % (type(exc).__name__, exc)

    return render_template_string(
        BENCHMARK, crds=crds(), crd=crd, namespace=namespace, name=name,
        obj=obj, status=status, spec=(obj or {}).get("spec") or {},
        journal=journal_rows(status), rows=summary_rows(summary),
        deltas=delta_rows(summary), jobs=jobs, pods=pods, fmt=fmt,
        bindings=sorted((status.get("bindings") or {}).items()),
        plan=((obj or {}).get("spec") or {}).get("plan") or [],
        error=error or job_error or pod_error or summary_error)


@app.route("/fhirbenchmarks/<namespace>/<name>/logs")
def benchmark_logs(namespace, name):
    """Live tail of the benchmark worker pods."""
    crd = find(BENCHMARK_PLURAL)
    if crd is None:
        return redirect(url_for("index"))

    pods, error = pods_for(namespace, name, "fhir-benchmark", "benchmark")
    job = request.args.get("job") or ""
    if job:
        pods = [p for p in pods if p["job"] == job]
    want = request.args.get("pod") or "all"
    if want != "all" and not any(p["name"] == want for p in pods):
        want = "all"

    return render_template_string(
        LOGS, crds=crds(), crd=crd, namespace=namespace, name=name,
        pods=pods, job=job, want=want, error=error,
        previous=request.args.get("previous") == "1",
        timestamps=request.args.get("ts") == "1",
        tail=LOG_TAIL, cap=int(LOG_STREAM_SECONDS // 60),
        plural=BENCHMARK_PLURAL)


@app.route("/fhirbenchmarks/<namespace>/<name>/logs/stream")
def benchmark_log_stream(namespace, name):
    targets, error = log_targets(namespace, name, request.args.get("pod"),
                                 request.args.get("job"), "fhir-benchmark", "benchmark")
    if error:
        return Response(error, status=502, mimetype="text/plain")
    if not targets:
        return Response("no such benchmark pod for %s/%s" % (namespace, name),
                        status=404, mimetype="text/plain")
    body = log_stream(namespace, targets,
                      previous=request.args.get("previous") == "1",
                      timestamps=request.args.get("ts") == "1")
    return Response(body, mimetype="text/event-stream", headers={
        "Cache-Control": "no-cache, no-transform",
        "X-Accel-Buffering": "no",
        "Connection": "keep-alive"})


@app.route("/fhirbenchmarks/<namespace>/<name>/results.json")
def benchmark_results(namespace, name):
    """The whole run as JSON: every case at every cache label.

    The measurements come from the run's summary object in GCS. status supplies
    provenance and step outcomes only.
    """
    crd = find(BENCHMARK_PLURAL)
    if crd is None:
        return redirect(url_for("index"))
    try:
        obj = _custom().get_namespaced_custom_object(
            GROUP, crd["version"], namespace, BENCHMARK_PLURAL, name)
    except client.ApiException as exc:
        return Response("%s %s" % (exc.status, exc.reason), status=exc.status,
                        mimetype="text/plain")
    status = obj.get("status") or {}
    run_id = status.get("runId")
    if not run_id:
        return Response("no run recorded for %s/%s" % (namespace, name),
                        status=404, mimetype="text/plain")
    try:
        summary = gcs.read_summary(namespace, name, int(run_id))
    except Exception as exc:                  # noqa: BLE001 - named, not hidden
        return Response("results unavailable from %s: %s: %s"
                        % (gcs.uri(gcs.summary_path(namespace, name, int(run_id))),
                           type(exc).__name__, exc),
                        status=502, mimetype="text/plain")
    body = json.dumps({
        "name": name, "namespace": namespace,
        "runId": run_id, "phase": status.get("phase"),
        "catalogue": status.get("catalogueName"),
        "catalogueRevision": status.get("catalogueRevision"),
        "bindings": status.get("bindings"),
        "seed": status.get("seed"),
        "journal": status.get("journal"),
        # Read from GCS every time. status holds provenance and step outcomes;
        # it holds no measurements at all.
        "summary": summary,
        "deltas": stats.deltas(summary),
        # Counted from the summary that was just read, not taken from status.
        "totals": stats.totals(summary),
        "resultsUri": status.get("resultsUri"),
        "runManifestUri": status.get("runManifestUri"),
        "archive": status.get("archive"),
    }, indent=2)
    return Response(body, mimetype="application/json", headers={
        "Content-Disposition": 'attachment; filename="%s-run%s.json"'
                               % (name, status.get("runId"))})


@app.route("/fhirbenchmarks/<namespace>/<name>/objects")
def benchmark_objects(namespace, name):
    """Every object a run wrote, each with a short-lived signed read URL.

    The browser talks to GCS directly rather than streaming through here: the
    ndjson bodies are the largest thing the system produces and proxying them
    through a Flask worker on a laptop helps nobody.
    """
    run_id = request.args.get("run")
    crd = find(BENCHMARK_PLURAL)
    if crd is None:
        return redirect(url_for("index"))
    if run_id is None:
        try:
            obj = _custom().get_namespaced_custom_object(
                GROUP, crd["version"], namespace, BENCHMARK_PLURAL, name)
        except client.ApiException as exc:
            return Response("%s %s" % (exc.status, exc.reason), status=exc.status,
                            mimetype="text/plain")
        run_id = (obj.get("status") or {}).get("runId")
    if not run_id:
        return Response("no run recorded for %s/%s" % (namespace, name),
                        status=404, mimetype="text/plain")

    if not gcs.enabled():
        return Response("GCS is not configured on this operator", status=503,
                        mimetype="text/plain")
    prefix = gcs.run_prefix(namespace, name, int(run_id))
    try:
        objects = [{"path": path, "uri": gcs.uri(path),
                    "url": gcs.download_url(path)}
                   for path in gcs.list_prefix(prefix + "/")]
    except Exception as exc:                  # noqa: BLE001 - shown, not hidden
        return Response("%s: %s" % (type(exc).__name__, exc), status=502,
                        mimetype="text/plain")
    return Response(json.dumps({"runId": int(run_id), "uri": gcs.uri(prefix),
                                "objects": objects}, indent=2),
                    mimetype="application/json")


@app.route("/fhirbenchmarks/<namespace>/<name>/logs/download")
def benchmark_log_download(namespace, name):
    """The whole log, not the tail, as a file. follow=False, so this ends."""
    targets, error = log_targets(namespace, name, request.args.get("pod"),
                                 request.args.get("job"), "fhir-benchmark", "benchmark")
    if error:
        return Response(error, status=502, mimetype="text/plain")
    if not targets:
        return Response("no such benchmark pod for %s/%s" % (namespace, name),
                        status=404, mimetype="text/plain")
    previous = request.args.get("previous") == "1"
    timestamps = request.args.get("ts") == "1"

    def body():
        for target in targets:
            if len(targets) > 1:
                yield "=== %s ===\n" % target["name"]
            for line in log_lines(namespace, target["name"], follow=False, tail=None,
                                  previous=previous, timestamps=timestamps):
                yield line + "\n"

    which = request.args.get("pod") or request.args.get("job") or "all"
    return Response(body(), mimetype="text/plain; charset=utf-8", headers={
        "Content-Disposition": 'attachment; filename="%s-%s.log"' % (name, which)})


@app.route("/catalogue")
def catalogue_page():
    """The version-controlled case list, so a selection can be made informed."""
    name = request.args.get("name") or "cohort-core"
    cases = catalogue.load(name)
    return render_template_string(
        CATALOGUE, crds=crds(), crd=find(BENCHMARK_PLURAL), name=name,
        revision=catalogue.revision(name), cases=cases,
        families=catalogue.families_in(name))


PAGE = """
<!doctype html>
<html lang="en"><head>
<meta charset="utf-8"><meta name="viewport" content="width=device-width, initial-scale=1">
<title>FHIR operator</title>
<link href="https://cdn.jsdelivr.net/npm/bootstrap@5.3.3/dist/css/bootstrap.min.css" rel="stylesheet">
<style>
  .depth-1 { padding-left: 1.5rem; } .depth-2 { padding-left: 3rem; }
  .depth-3 { padding-left: 4.5rem; }
  .group-row td { background: var(--bs-tertiary-bg); font-weight: 600; }
</style>
</head><body class="bg-body-tertiary">

<nav class="navbar navbar-expand bg-dark" data-bs-theme="dark"><div class="container-fluid">
  <span class="navbar-brand">FHIR operator</span>
  <ul class="navbar-nav me-auto">
  {% for c in crds %}
    <li class="nav-item"><a class="nav-link {{ 'active' if crd and c.plural == crd.plural }}"
       href="/{{ c.plural }}">{{ c.kind }}</a></li>
  {% endfor %}
  </ul>
  <span class="navbar-text small" id="tick"></span>
</div></nav>

<main class="container-fluid py-4">

{% if error %}<div class="alert alert-danger"><pre class="mb-0">{{ error }}</pre></div>{% endif %}
{% if message %}<div class="alert alert-secondary"><pre class="mb-0 small">{{ message }}</pre></div>{% endif %}

{% if crd %}

{# ------------------------------------------------------------------ macros
   Shared by the plain table every other CRD gets and the wide card a
   FhirDataset gets. Everything the macros need is passed in: a macro does not
   inherit the render context. #}

{% macro phase_badge(phase, deleting) -%}
  {%- if deleting %}<span class="badge text-bg-danger">Deleting</span>
  {%- elif phase in ('Ready', 'Complete', 'Succeeded') %}<span class="badge text-bg-success">{{ phase }}</span>
  {%- elif phase in ('Loading', 'Active', 'Running') %}<span class="badge text-bg-primary">{{ phase }}</span>
  {%- elif phase == 'Failed' %}<span class="badge text-bg-danger">{{ phase }}</span>
  {%- elif phase in ('Paused', 'Waiting', 'Pending', 'Deleting') %}<span class="badge text-bg-warning">{{ phase }}</span>
  {%- elif phase %}<span class="badge text-bg-secondary">{{ phase }}</span>
  {%- else %}<span class="text-body-secondary">-</span>
  {%- endif %}
{%- endmacro %}

{% macro flag_control(o, f, current) %}
  <form method="post" class="d-inline">
    <input type="hidden" name="namespace" value="{{ o.namespace }}">
    <input type="hidden" name="name" value="{{ o.name }}">
    <input type="hidden" name="field" value="{{ f.path }}">
    <input type="hidden" name="action" value="set">
    {% if f.kind == 'boolean' %}
      {% set next = 'false' if current == 'true' else 'true' %}
      <input type="hidden" name="value" value="{{ next }}">
      <button class="btn btn-sm {{ 'btn-success' if current == 'true' else 'btn-outline-secondary' }}"
              title="click to set {{ f.path }}={{ next }}">{{ current or 'unset' }}</button>
    {% else %}
      <select class="form-select form-select-sm d-inline w-auto" name="value"
              onchange="this.form.submit()" title="{{ f.path }}">
        {% if not current %}<option value="">unset</option>{% endif %}
        {% for option in f.options %}
          <option value="{{ option }}" {{ 'selected' if current == option }}>{{ option }}</option>
        {% endfor %}
      </select>
    {% endif %}
  </form>
{% endmacro %}

{% macro row_actions(o, crd, show_jobs, show_benchmark=False) %}
  {% if show_jobs %}
  <a class="btn btn-sm btn-outline-primary"
     href="/fhirdatasets/{{ o.namespace }}/{{ o.name }}">Jobs &amp; metrics</a>
  {% endif %}
  {% if show_benchmark %}
  <a class="btn btn-sm btn-outline-primary"
     href="/fhirbenchmarks/{{ o.namespace }}/{{ o.name }}">Journal &amp; results</a>
  {% endif %}
  <a class="btn btn-sm btn-outline-secondary"
     href="/{{ crd.plural }}?edit={{ o.namespace }}/{{ o.name }}#editor">Edit</a>
  <form method="post" class="d-inline">
    <input type="hidden" name="namespace" value="{{ o.namespace }}">
    <input type="hidden" name="name" value="{{ o.name }}">
    <input type="hidden" name="action" value="delete">
    <button class="btn btn-sm btn-outline-danger"
            onclick="return confirm('Delete {{ crd.kind }} {{ o.namespace }}/{{ o.name }}? Anything it owns goes too.')">Delete</button>
  </form>
{% endmacro %}

{% macro job_phase_badge(phase) -%}
  {%- if phase == 'Complete' %}<span class="badge text-bg-success">Complete</span>
  {%- elif phase == 'Active' %}<span class="badge text-bg-primary">Active</span>
  {%- elif phase == 'Failed' %}<span class="badge text-bg-danger">Failed</span>
  {%- elif phase == 'Paused' %}<span class="badge text-bg-warning">Paused</span>
  {%- else %}<span class="badge text-bg-secondary">{{ phase }}</span>
  {%- endif %}
{%- endmacro %}

<div id="objects">

<div class="d-flex justify-content-between align-items-center mb-3">
  <h5 class="mb-0">{{ crd.kind }}
    <span class="text-body-secondary small font-monospace">{{ crd.plural }}.{{ crd.version }}</span>
    <span class="badge text-bg-secondary rounded-pill">{{ objects|length }}</span>
  </h5>
  <a class="btn btn-sm btn-primary" href="#editor">+ New {{ crd.kind }}</a>
</div>

{% if show_jobs %}
{# ---------------------------------------------------------- dataset cards
   A dataset is four things at once -- what you asked for, what the database
   actually holds, which Job is closing the gap, and how far along it is --
   and a single table row had nowhere to put three of them. #}
{% for o in objects %}
  <div class="card mb-3">

    <div class="card-body py-2">
      <div class="row g-3 align-items-center">
        <div class="col-xl-3">
          <div class="fw-semibold font-monospace text-truncate">{{ o.name }}</div>
          <div class="small text-body-secondary font-monospace">
            {{ o.namespace }} &middot; {{ o.age }} old</div>
        </div>
        <div class="col-xl-3">
          {{ phase_badge(o.phase, o.deleting) }}
          {% if o.status.attempt %}
            <span class="badge text-bg-light border">attempt {{ o.status.attempt }}</span>
          {% endif %}
          {% if o.deleting %}
            <div class="small text-body-secondary">phase frozen at {{ o.phase or '-' }}</div>
          {% elif o.status.reason %}
            <div class="small text-body-secondary text-truncate" title="{{ o.status.reason }}">
              {{ o.status.reason }}</div>
          {% endif %}
        </div>
        <div class="col-xl-3">
          <div class="d-flex flex-wrap gap-2 align-items-center">
          {% for f in flags %}
            <div class="d-flex align-items-center gap-1">
              <span class="small text-body-secondary">{{ f.name }}</span>
              {{ flag_control(o, f, lookup(o.spec, f.path)|string) }}
            </div>
          {% endfor %}
          </div>
        </div>
        <div class="col-xl-3 text-xl-end text-nowrap">
          {{ row_actions(o, crd, show_jobs) }}
        </div>
      </div>
    </div>

    <div class="card-body border-top pt-2 pb-3">
      <div class="row g-4">

        <div class="col-xxl-5">
          <div class="d-flex justify-content-between align-items-baseline mb-1">
            <span class="small fw-semibold text-body-secondary text-uppercase">Resources</span>
            <span class="small text-body-secondary">
              {% if o.counted != '-' %}counted {{ o.counted }} ago{% else %}never counted{% endif %}</span>
          </div>
          {% for r in o.resources %}
            <div class="row g-2 align-items-center mb-1">
              <div class="col-3 small font-monospace text-truncate">{{ r.kind }}</div>
              <div class="col-5">
                <div class="progress" role="progressbar" aria-label="{{ r.kind }}"
                     aria-valuenow="{{ r.pct }}" aria-valuemin="0" aria-valuemax="100">
                  <div class="progress-bar
                       {%- if r.done %} bg-success
                       {%- elif o.phase in ('Loading', 'Deleting', 'Active') %} progress-bar-striped progress-bar-animated
                       {%- endif %}" style="width: {{ r.pct }}%"></div>
                </div>
              </div>
              <div class="col-4 small font-monospace text-end text-nowrap">
                {{ fmt(r.have) }} / {{ fmt(r.want) }}
                <span class="text-body-secondary">{{ r.pct }}%</span>
              </div>
            </div>
          {% else %}
            <div class="small text-body-secondary">
              Nothing counted yet. The loader reports a census inward when it
              starts, every 20s while it runs, and again when it finishes.</div>
          {% endfor %}
          {% for note in o.status.codeNotes or [] %}
            <div class="small text-warning-emphasis">{{ note }}</div>
          {% endfor %}
        </div>

        <div class="col-xxl-7">
          <div class="d-flex justify-content-between align-items-baseline mb-1">
            <span class="small fw-semibold text-body-secondary text-uppercase">Jobs</span>
            <span class="small text-body-secondary font-monospace">dataset={{ o.name }}</span>
          </div>
          <div class="table-responsive">
          <table class="table table-sm table-borderless mb-0 align-middle">
            <thead><tr class="small text-body-secondary">
              <th>Job</th><th>Mode</th><th>Phase</th><th class="text-end">Done</th>
              <th class="text-end">Active</th><th class="text-end">Failed</th>
              <th class="text-end">Par</th><th class="text-end">Age</th>
              <th class="text-end">Duration</th></tr></thead>
            <tbody class="table-group-divider">
            {% for j in o.jobs[:4] %}
              <tr>
                <td class="font-monospace small">{{ j.name }}
                  {% if j.name == o.job %}<span class="badge text-bg-info">current</span>{% endif %}</td>
                <td class="small">{{ j.mode or '-' }}</td>
                <td>{{ job_phase_badge(j.phase) }}</td>
                <td class="text-end font-monospace small">{{ j.completions }}</td>
                <td class="text-end font-monospace small">{{ j.active }}</td>
                <td class="text-end font-monospace small {{ 'text-danger fw-semibold' if j.failed }}">{{ j.failed }}</td>
                <td class="text-end font-monospace small">{{ j.parallelism }}</td>
                <td class="text-end font-monospace small">{{ j.age }}</td>
                <td class="text-end font-monospace small">{{ j.duration }}</td>
              </tr>
            {% else %}
              <tr><td colspan="9" class="small text-body-secondary">
                No Job carries dataset={{ o.name }}. The reconciler launches one
                when status.observed falls short of status.expected.</td></tr>
            {% endfor %}
            </tbody>
          </table>
          </div>
          {% if o.jobs|length > 4 %}
            <a class="small text-decoration-none"
               href="/fhirdatasets/{{ o.namespace }}/{{ o.name }}">
              {{ o.jobs|length - 4 }} older attempt(s) &rarr;</a>
          {% endif %}
        </div>

      </div>
      <details class="mt-2"><summary class="small text-body-secondary">spec yaml</summary>
        <pre class="small mb-0 mt-2">{{ o.yaml }}</pre></details>
    </div>
  </div>
{% else %}
  <div class="card mb-4"><div class="card-body text-body-secondary">
    No {{ crd.kind }} objects. Use the form below.</div></div>
{% endfor %}

{% elif show_benchmark %}
{# ------------------------------------------------------- benchmark cards
   A benchmark is a sequence, not a state, so the row shows where the plan has
   got to and what each step recorded. status.journal is the primary output --
   it is the record of what happened, not a reflection of what is true now. #}
<div class="card mb-3"><div class="card-body py-2 d-flex flex-wrap gap-3 align-items-center">
  <span class="small text-body-secondary text-uppercase fw-semibold">Catalogue</span>
  <span class="font-monospace small">cohort-core</span>
  <span class="font-monospace small text-body-secondary text-truncate"
        style="max-width: 24rem">{{ catalogue_revision }}</span>
  {% for family, count in families %}
    <span class="badge text-bg-light border font-monospace">{{ family }} {{ count }}</span>
  {% endfor %}
  <a class="btn btn-sm btn-outline-secondary ms-auto" href="/catalogue">Browse cases</a>
</div></div>

{% for o in objects %}
  <div class="card mb-3">
    <div class="card-body py-2">
      <div class="row g-3 align-items-center">
        <div class="col-xl-3">
          <div class="fw-semibold font-monospace text-truncate">{{ o.name }}</div>
          <div class="small text-body-secondary font-monospace">
            {{ o.namespace }} &middot; {{ o.age }} old &middot;
            stack {{ o.spec.stackRef }} &middot; data {{ o.spec.datasetRef }}</div>
        </div>
        <div class="col-xl-3">
          {{ phase_badge(o.phase, o.deleting) }}
          <span class="badge text-bg-light border">run {{ o.status.runId or '-' }}</span>
          {% if o.status.invalidCount %}
            <span class="badge text-bg-danger">{{ o.status.invalidCount }} invalid</span>
          {% endif %}
          {% if o.status.partial %}<span class="badge text-bg-warning">partial</span>{% endif %}
          <div class="small text-body-secondary text-truncate" title="{{ o.status.reason }}">
            {{ o.status.progress or '' }}{% if o.status.reason %} &middot; {{ o.status.reason }}{% endif %}</div>
        </div>
        <div class="col-xl-3">
          <div class="d-flex flex-wrap gap-2 align-items-center">
          {% for f in flags %}
            <div class="d-flex align-items-center gap-1">
              <span class="small text-body-secondary">{{ f.name }}</span>
              {{ flag_control(o, f, lookup(o.spec, f.path)|string) }}
            </div>
          {% endfor %}
          </div>
        </div>
        <div class="col-xl-3 text-xl-end text-nowrap">
          {{ row_actions(o, crd, show_jobs, True) }}
        </div>
      </div>
    </div>

    <div class="card-body border-top pt-2 pb-3">
      <div class="row g-4">
        <div class="col-xxl-6">
          <div class="small fw-semibold text-body-secondary text-uppercase mb-1">Journal</div>
          <div class="table-responsive">
          <table class="table table-sm table-borderless mb-0 align-middle">
            <thead><tr class="small text-body-secondary">
              <th>#</th><th>Step</th><th>Type</th><th>Outcome</th>
              <th class="text-end">Measured</th><th class="text-end">Invalid</th>
              <th class="text-end">Took</th></tr></thead>
            <tbody class="table-group-divider">
            {% for e in o.journal %}
              <tr>
                <td class="small font-monospace">{{ e.index }}</td>
                <td class="small font-monospace">{{ e.id }}</td>
                <td class="small">{{ e.step }}</td>
                <td>{{ phase_badge(e.outcome, '') }}</td>
                <td class="text-end font-monospace small">{{ e.measurements if e.measurements is not none else '-' }}</td>
                <td class="text-end font-monospace small {{ 'text-danger fw-semibold' if e.invalid }}">{{ e.invalid if e.invalid is not none else '-' }}</td>
                <td class="text-end font-monospace small">{{ e.duration }}</td>
              </tr>
            {% else %}
              <tr><td colspan="7" class="small text-body-secondary">
                Nothing has run yet. The plan advances one step per reconcile pass.</td></tr>
            {% endfor %}
            </tbody>
          </table>
          </div>
        </div>

        <div class="col-xxl-6">
          <div class="small fw-semibold text-body-secondary text-uppercase mb-1">
            Slowest cases (p95)</div>
          <div class="table-responsive">
          <table class="table table-sm table-borderless mb-0 align-middle">
            <thead><tr class="small text-body-secondary">
              <th>Case</th><th>Cache</th><th class="text-end">p50</th>
              <th class="text-end">p95</th><th class="text-end">p99</th>
              <th class="text-end">Rows</th><th>Engine</th></tr></thead>
            <tbody class="table-group-divider">
            {% for r in o.summary[:8] %}
              <tr>
                <td class="small font-monospace text-truncate" style="max-width: 16rem">{{ r.case }}</td>
                <td class="small">{{ r.cache }}</td>
                <td class="text-end font-monospace small">{{ fmt(r.p50) }}</td>
                <td class="text-end font-monospace small">{{ fmt(r.p95) }}</td>
                <td class="text-end font-monospace small">{{ fmt(r.p99) }}</td>
                <td class="text-end font-monospace small">{{ fmt(r.total if r.total is not none else r.rows) }}</td>
                <td class="small font-monospace">{{ r.engine }}</td>
              </tr>
            {% else %}
              <tr><td colspan="7" class="small text-body-secondary">
                No measurements yet. Percentiles are computed from raw samples
                once a measure step finishes.</td></tr>
            {% endfor %}
            </tbody>
          </table>
          </div>
        </div>
      </div>
      <details class="mt-2"><summary class="small text-body-secondary">spec yaml</summary>
        <pre class="small mb-0 mt-2">{{ o.yaml }}</pre></details>
    </div>
  </div>
{% else %}
  <div class="card mb-4"><div class="card-body text-body-secondary">
    No {{ crd.kind }} objects. Use the form below -- stackRef and datasetRef are
    mandatory and the plan defaults to cold / prewarm / warm / hot.</div></div>
{% endfor %}

{% else %}
{# ------------------------------------------------- every other kind: table #}
<div class="card mb-4">
  <table class="table table-sm mb-0 align-middle">
    <thead><tr><th>Namespace</th><th>Name</th><th>Phase</th><th>Status</th>
      {% for f in flags %}<th class="small">{{ f.name }}</th>{% endfor %}
      <th>Spec</th><th class="text-end">Actions</th></tr></thead>
    <tbody>
    {% for o in objects %}
      <tr>
        <td class="font-monospace">{{ o.namespace }}</td>
        <td class="font-monospace">{{ o.name }}</td>
        <td>{{ phase_badge(o.phase, o.deleting) }}
            {% if o.deleting %}
              <div class="form-text small mb-0">phase frozen at {{ o.phase or '-' }}</div>
            {% endif %}</td>
        <td class="small font-monospace">{{ o.summary or '-' }}</td>
        {% for f in flags %}
          <td class="text-nowrap">{{ flag_control(o, f, lookup(o.spec, f.path)|string) }}</td>
        {% endfor %}
        <td><details><summary class="small text-body-secondary">yaml</summary>
            <pre class="small mb-0 mt-2">{{ o.yaml }}</pre></details></td>
        <td class="text-end text-nowrap">{{ row_actions(o, crd, show_jobs) }}</td>
      </tr>
    {% else %}
      <tr><td colspan="7" class="text-body-secondary">
        No {{ crd.kind }} objects. Use the form below.</td></tr>
    {% endfor %}
    </tbody>
  </table>
</div>
{% endif %}

</div>

{% if show_benchmark %}
{# ------------------------------------------------------------- run panel
   Outside #objects on purpose: the list refreshes itself every ten seconds by
   replacing that div's contents, which would reset these selects mid-choice.

   Every run is a new object. spec is immutable after admission (C1), so
   re-running an existing benchmark means incrementing its spec.runId, not
   editing it -- and a fresh object with a timestamped name is what a button
   press should produce anyway. #}
<div class="card mb-4 border-primary">
  <div class="card-header bg-primary-subtle">
    Run the whole catalogue
    <span class="small text-body-secondary">all {{ case_count }} cases, one after
    another, against one stack and one dataset</span>
  </div>
  <div class="card-body">
  <form method="post" class="row g-3 align-items-end">
    <input type="hidden" name="action" value="run">

    <div class="col-lg-3">
      <label class="form-label" for="runStack">FhirStack</label>
      <select class="form-select font-monospace" id="runStack" name="runStack" required>
        <option value="">-- pick a stack --</option>
        {% for s in stacks %}
          <option value="{{ s.key }}" {{ 'disabled' if not s.ready }}>
            {{ s.key }}{{ '' if s.ready else '  (' ~ s.note ~ ')' }}</option>
        {% endfor %}
      </select>
      <div class="form-text">Must be Ready. A degraded stack is not measured.</div>
    </div>

    <div class="col-lg-3">
      <label class="form-label" for="runDataset">FhirDataset</label>
      <select class="form-select font-monospace" id="runDataset" name="runDataset" required>
        <option value="">-- pick a dataset --</option>
        {% for d in dataset_targets %}
          <option value="{{ d.key }}" {{ 'disabled' if not d.ready }}>
            {{ d.key }}{{ '' if d.ready else '  (' ~ d.note ~ ')' }}</option>
        {% endfor %}
      </select>
      <div class="form-text">Must be fully loaded, and in the stack's namespace.</div>
    </div>

    <div class="col-lg-2">
      <label class="form-label" for="runRepeats">Repeats</label>
      <input class="form-control font-monospace" type="number" min="1" step="1"
             id="runRepeats" name="runRepeats" value="30">
      <div class="form-text">Measurements per case, after warm-up.</div>
    </div>

    <div class="col-lg-2">
      <label class="form-label" for="runName">Name</label>
      <input class="form-control font-monospace" id="runName" name="runName"
             placeholder="all-&lt;timestamp&gt;">
      <div class="form-text">Blank auto-names it.</div>
    </div>

    <div class="col-lg-2 d-grid">
      <button class="btn btn-primary btn-lg">Run benchmarks</button>
    </div>

    <div class="col-12">
      <div class="form-text mb-0">
        Runs the plan in order: preflight, restart, settle, cold, prewarm, warm,
        hot, report. Cases run sequentially at concurrency 1, which is the only
        setting under which pg_stat_statements can attribute an engine to a case.
        The stack is restarted and held under an exclusive lease for the duration.
      </div>
    </div>
  </form>
  </div>
</div>
{% endif %}

<div class="card" id="editor">
  <div class="card-header">
    {% if editing %}Edit <span class="font-monospace">{{ editing.namespace }}/{{ editing.name }}</span>
    {% else %}New {{ crd.kind }}{% endif %}
  </div>
  <div class="card-body">
  <form method="post">
    <div class="row g-2 mb-3">
      <div class="col-md-4"><label class="form-label" for="namespace">Namespace</label>
        <input class="form-control font-monospace" id="namespace" name="namespace"
               value="{{ editing.namespace if editing else '' }}" placeholder="perf-s"
               {{ 'readonly' if editing }} {{ 'required' if crd.scope == 'Namespaced' }}>
        <div class="form-text">Created if absent.</div></div>
      <div class="col-md-4"><label class="form-label" for="name">Name</label>
        <input class="form-control font-monospace" id="name" name="name"
               value="{{ editing.name if editing else '' }}" placeholder="perf-s"
               {{ 'readonly' if editing }} required></div>
    </div>

    <table class="table table-sm align-middle mb-3">
      <tbody>
      {% for f in flat %}
        {% if f.type == 'group' %}
          <tr class="group-row"><td colspan="2" class="depth-{{ f.depth }}">{{ f.name }}</td></tr>
        {% else %}
          <tr>
            <td class="depth-{{ f.depth }}" style="width: 40%">
              <label class="form-label mb-0 small" for="f-{{ f.path }}">{{ f.name }}</label>
              {% if f.desc %}<div class="form-text small">{{ f.desc }}</div>{% endif %}
            </td>
            <td>
              {% set current = lookup(editing.spec, f.path) if editing else '' %}
              {% if f.enum %}
                <select class="form-select form-select-sm" id="f-{{ f.path }}" name="{{ f.path }}">
                  <option value="">(unset{% if f.default %}, default {{ f.default }}{% endif %})</option>
                  {% for option in f.enum %}
                    <option value="{{ option }}" {{ 'selected' if current == option }}>{{ option }}</option>
                  {% endfor %}
                </select>
              {% elif f.type == 'boolean' %}
                <select class="form-select form-select-sm" id="f-{{ f.path }}" name="{{ f.path }}">
                  <option value="">(unset)</option>
                  <option value="true" {{ 'selected' if current == 'true' }}>true</option>
                  <option value="false" {{ 'selected' if current == 'false' }}>false</option>
                </select>
              {% elif f.type == 'yaml' %}
                <textarea class="form-control form-control-sm font-monospace" rows="3"
                          id="f-{{ f.path }}" name="{{ f.path }}">{{ current }}</textarea>
              {% elif f.type in ('number', 'integer') %}
                <input class="form-control form-control-sm font-monospace" type="number"
                       step="{{ '1' if f.type == 'integer' else 'any' }}"
                       id="f-{{ f.path }}" name="{{ f.path }}" value="{{ current }}"
                       placeholder="{{ f.default if f.default is not none else '' }}">
              {% else %}
                <input class="form-control form-control-sm font-monospace" type="text"
                       id="f-{{ f.path }}" name="{{ f.path }}" value="{{ current }}"
                       placeholder="{{ f.default if f.default is not none else '' }}">
              {% endif %}
            </td>
          </tr>
        {% endif %}
      {% else %}
        <tr><td class="text-body-secondary">
          This CRD has no structural schema. Use the YAML box below.</td></tr>
      {% endfor %}
      </tbody>
    </table>

    <details {{ 'open' if not flat }}>
      <summary class="small text-body-secondary mb-2">Raw spec YAML (overrides the form when filled in)</summary>
      <textarea class="form-control font-monospace mt-2" rows="10" name="__yaml"
                placeholder="cpuLimitPolicy: strict&#10;resources:&#10;  postgres:&#10;    memory: {min: 16, max: 16}">{{ '' }}</textarea>
    </details>

    <div class="mt-3">
      <button class="btn btn-primary" name="action" value="apply">Apply</button>
      <a class="btn btn-outline-secondary" href="/{{ crd.plural }}">Clear</a>
    </div>
  </form>
  </div>
</div>
{% endif %}

</main>

<script>
// Refresh the object list only. The editor is outside #objects, so anything
// half-typed survives. A meta refresh used to reload the whole page and wipe
// the form, which left Apply blocked on its own required fields.
async function refresh() {
  if (document.querySelector('#editor :focus')) { return; }   // never interrupt typing
  try {
    var response = await fetch(window.location.pathname, {headers: {'X-Requested-With': 'refresh'}});
    var doc = new DOMParser().parseFromString(await response.text(), 'text/html');
    var fresh = doc.getElementById('objects');
    if (fresh && document.getElementById('objects')) {
      document.getElementById('objects').innerHTML = fresh.innerHTML;
      document.getElementById('tick').textContent = new Date().toLocaleTimeString();
    }
  } catch (err) {
    document.getElementById('tick').textContent = 'stale';
  }
}
if (document.getElementById('objects')) { setInterval(refresh, 10000); }
</script>
</body></html>
"""


DETAIL = """
<!doctype html>
<html lang="en"><head>
<meta charset="utf-8"><meta name="viewport" content="width=device-width, initial-scale=1">
<title>{{ name }} - jobs &amp; metrics</title>
<link href="https://cdn.jsdelivr.net/npm/bootstrap@5.3.3/dist/css/bootstrap.min.css" rel="stylesheet">
</head><body class="bg-body-tertiary">

<nav class="navbar navbar-expand bg-dark" data-bs-theme="dark"><div class="container-fluid">
  <span class="navbar-brand">FHIR operator</span>
  <ul class="navbar-nav me-auto">
  {% for c in crds %}
    <li class="nav-item"><a class="nav-link {{ 'active' if crd and c.plural == crd.plural }}"
       href="/{{ c.plural }}">{{ c.kind }}</a></li>
  {% endfor %}
  </ul>
  <span class="navbar-text small" id="tick"></span>
</div></nav>

<main class="container-fluid py-4">
<div id="objects">

<div class="d-flex justify-content-between align-items-center mb-3">
  <h5 class="mb-0">
    <a class="text-decoration-none" href="/fhirdatasets">FhirDataset</a>
    <span class="text-body-secondary">/</span>
    <span class="font-monospace">{{ namespace }}/{{ name }}</span>
    {% if status.phase == 'Ready' %}<span class="badge text-bg-success">Ready</span>
    {% elif status.phase %}<span class="badge text-bg-secondary">{{ status.phase }}</span>{% endif %}
  </h5>
  <a class="btn btn-sm btn-outline-secondary"
     href="/fhirdatasets?edit={{ namespace }}/{{ name }}#editor">Edit spec</a>
</div>

{% if error %}<div class="alert alert-danger"><pre class="mb-0">{{ error }}</pre></div>{% endif %}

<div class="card mb-4">
  <div class="card-header">Reconcile status</div>
  <div class="card-body py-2">
    <div class="row small">
      <div class="col-md-3">Phase <span class="font-monospace">{{ status.phase or '-' }}</span></div>
      <div class="col-md-3">Attempt <span class="font-monospace">{{ status.attempt if status.attempt is not none else '-' }}</span></div>
      <div class="col-md-6">Current job <span class="font-monospace">{{ status.jobName or '-' }}</span></div>
    </div>
    {% if progress %}
    <div class="row small mt-2">
      {% for kind, counts in progress %}
        <div class="col-md-3">{{ kind }} <span class="font-monospace">{{ counts }}</span></div>
      {% endfor %}
    </div>
    {% endif %}
    {% if status.reason %}<div class="form-text mt-2">{{ status.reason }}</div>{% endif %}
    {% for note in status.codeNotes or [] %}
      <div class="form-text text-warning-emphasis">{{ note }}</div>
    {% endfor %}
  </div>
</div>

<div class="card mb-4">
  <div class="card-header">Jobs <span class="text-body-secondary small">dataset={{ name }}</span></div>
  <table class="table table-sm mb-0 align-middle">
    <thead><tr><th>Name</th><th>Mode</th><th>Phase</th><th>Completions</th>
      <th>Active</th><th>Failed</th><th>Parallelism</th><th>Age</th><th>Duration</th></tr></thead>
    <tbody>
    {% for j in jobs %}
      <tr>
        <td class="font-monospace small">
          <a class="text-decoration-none"
             href="/fhirdatasets/{{ namespace }}/{{ name }}/logs?job={{ j.name }}"
             title="tail every worker in this job">{{ j.name }}</a></td>
        <td><span class="badge text-bg-light">{{ j.mode or '-' }}</span></td>
        <td>{% if j.phase == 'Complete' %}<span class="badge text-bg-success">Complete</span>
            {% elif j.phase == 'Active' %}<span class="badge text-bg-primary">Active</span>
            {% elif j.phase == 'Failed' %}<span class="badge text-bg-danger">Failed</span>
            {% elif j.phase == 'Paused' %}<span class="badge text-bg-warning">Paused</span>
            {% else %}{{ j.phase }}{% endif %}</td>
        <td class="font-monospace">{{ j.completions }}</td>
        <td class="font-monospace">{{ j.active }}</td>
        <td class="font-monospace {{ 'text-danger' if j.failed }}">{{ j.failed }}</td>
        <td class="font-monospace">{{ j.parallelism }}</td>
        <td class="font-monospace small">{{ j.age }}</td>
        <td class="font-monospace small">{{ j.duration }}</td>
      </tr>
    {% else %}
      <tr><td colspan="9" class="text-body-secondary">
        No Jobs carry dataset={{ name }}. The reconciler launches one when
        status.observed falls short of status.expected.</td></tr>
    {% endfor %}
    </tbody>
  </table>
</div>

<div class="card mb-4">
  <div class="card-header d-flex justify-content-between align-items-center">
    <span>Pods</span>
    <a class="btn btn-sm btn-outline-secondary"
       href="/fhirdatasets/{{ namespace }}/{{ name }}/logs?pod=all">Tail all workers</a>
  </div>
  <table class="table table-sm mb-0 align-middle">
    <thead><tr><th>Pod</th><th>Job</th><th>Index</th><th>Phase</th><th>Reason</th>
      <th>Restarts</th><th>Node</th><th>Age</th><th>Logs</th></tr></thead>
    <tbody>
    {% for p in pods %}
      <tr>
        <td class="font-monospace small">{{ p.name }}</td>
        <td class="font-monospace small">{{ p.job or '-' }}</td>
        <td class="font-monospace">{{ p.index or '-' }}</td>
        <td>{% if p.phase == 'Running' %}<span class="badge text-bg-primary">Running</span>
            {% elif p.phase == 'Succeeded' %}<span class="badge text-bg-success">Succeeded</span>
            {% elif p.phase == 'Failed' %}<span class="badge text-bg-danger">Failed</span>
            {% else %}<span class="badge text-bg-secondary">{{ p.phase }}</span>{% endif %}</td>
        <td class="small">{{ p.reason or '-' }}</td>
        <td class="font-monospace {{ 'text-danger' if p.restarts }}">{{ p.restarts }}</td>
        <td class="font-monospace small">{{ p.node }}</td>
        <td class="font-monospace small">{{ p.age }}</td>
        <td class="small text-nowrap">
          <a href="/fhirdatasets/{{ namespace }}/{{ name }}/logs?pod={{ p.name }}">tail</a>
          {% if p.restarts %}
            <a class="ms-2 text-danger"
               href="/fhirdatasets/{{ namespace }}/{{ name }}/logs?pod={{ p.name }}&amp;previous=1"
               title="the container that died, not the one running now">prev</a>
          {% endif %}
        </td>
      </tr>
    {% else %}
      <tr><td colspan="9" class="text-body-secondary">No loader pods right now.</td></tr>
    {% endfor %}
    </tbody>
  </table>
</div>

<div class="card">
  <div class="card-header d-flex justify-content-between align-items-center">
    <span>Metrics <span class="text-body-secondary small font-monospace">{{ prom_url }}</span></span>
    <span class="text-body-secondary small">rates over 1m, p95 over 5m</span>
  </div>
  {% if prom_error %}
    <div class="card-body py-2"><div class="alert alert-warning mb-0 small">
      <pre class="mb-0">{{ prom_error }}</pre></div></div>
  {% endif %}
  <table class="table table-sm mb-0 align-middle">
    <thead><tr><th>Pod</th><th class="text-end">Resources</th><th class="text-end">res/s</th>
      <th class="text-end">Patients</th><th class="text-end">Bundles ok</th>
      <th class="text-end">Bundles failed</th><th class="text-end">bundles/s</th>
      <th class="text-end">p95 (s)</th><th class="text-end">Deleted</th></tr></thead>
    <tbody>
    {% for r in rows %}
      <tr>
        <td class="font-monospace small">{{ r.get('pod') }}</td>
        <td class="text-end font-monospace">{{ fmt(r.get('resources')) }}</td>
        <td class="text-end font-monospace">{{ fmt(r.get('resources_per_sec'), 1) }}</td>
        <td class="text-end font-monospace">{{ fmt(r.get('patients')) }}</td>
        <td class="text-end font-monospace">{{ fmt(r.get('bundles_ok')) }}</td>
        <td class="text-end font-monospace {{ 'text-danger' if r.get('bundles_failed') }}">
          {{ fmt(r.get('bundles_failed')) }}</td>
        <td class="text-end font-monospace">{{ fmt(r.get('bundles_per_sec'), 1) }}</td>
        <td class="text-end font-monospace">{{ fmt(r.get('p95_seconds'), 3) }}</td>
        <td class="text-end font-monospace">{{ fmt(r.get('deleted')) }}</td>
      </tr>
    {% else %}
      <tr><td colspan="9" class="text-body-secondary">
        No series for dataset={{ name }}. Counters are per worker process, so a
        job that has finished and been reaped reports nothing here.</td></tr>
    {% endfor %}
    </tbody>
    {% if rows %}
    <tfoot class="table-group-divider">
      <tr class="fw-semibold">
        <td>Total ({{ rows|length }} pods)</td>
        <td class="text-end font-monospace">{{ fmt(totals.get('resources')) }}</td>
        <td class="text-end font-monospace">{{ fmt(totals.get('resources_per_sec'), 1) }}</td>
        <td class="text-end font-monospace">{{ fmt(totals.get('patients')) }}</td>
        <td class="text-end font-monospace">{{ fmt(totals.get('bundles_ok')) }}</td>
        <td class="text-end font-monospace">{{ fmt(totals.get('bundles_failed')) }}</td>
        <td class="text-end font-monospace">{{ fmt(totals.get('bundles_per_sec'), 1) }}</td>
        <td class="text-end font-monospace text-body-secondary">per pod</td>
        <td class="text-end font-monospace">{{ fmt(totals.get('deleted')) }}</td>
      </tr>
    </tfoot>
    {% endif %}
  </table>
</div>

</div>
</main>

<script>
// Same trick as the object list: swap #objects only, so a slow Prometheus
// query never blanks the page and nothing below it jumps.
async function refresh() {
  try {
    var response = await fetch(window.location.pathname, {headers: {'X-Requested-With': 'refresh'}});
    var doc = new DOMParser().parseFromString(await response.text(), 'text/html');
    var fresh = doc.getElementById('objects');
    if (fresh) {
      document.getElementById('objects').innerHTML = fresh.innerHTML;
      document.getElementById('tick').textContent = new Date().toLocaleTimeString();
    }
  } catch (err) {
    document.getElementById('tick').textContent = 'stale';
  }
}
setInterval(refresh, 10000);
</script>
</body></html>
"""


LOGS = """
<!doctype html>
<html lang="en"><head>
<meta charset="utf-8"><meta name="viewport" content="width=device-width, initial-scale=1">
<title>{{ name }} - logs</title>
<link href="https://cdn.jsdelivr.net/npm/bootstrap@5.3.3/dist/css/bootstrap.min.css" rel="stylesheet">
<style>
  #log { height: calc(100vh - 235px); overflow-y: auto; margin: 0;
         font-size: .8rem; line-height: 1.35; }
  #log div { white-space: pre; }
  body.wrap #log div { white-space: pre-wrap; overflow-wrap: anywhere; }
  #log .idx { display: inline-block; width: 2.5rem; opacity: .75; }
  #log .meta { color: #8a9bb0; font-style: italic; }
</style>
</head><body class="bg-body-tertiary">

<nav class="navbar navbar-expand bg-dark" data-bs-theme="dark"><div class="container-fluid">
  <span class="navbar-brand">FHIR operator</span>
  <ul class="navbar-nav me-auto">
  {% for c in crds %}
    <li class="nav-item"><a class="nav-link {{ 'active' if crd and c.plural == crd.plural }}"
       href="/{{ c.plural }}">{{ c.kind }}</a></li>
  {% endfor %}
  </ul>
  <span class="navbar-text small" id="tick"></span>
</div></nav>

<main class="container-fluid py-3">

<div class="d-flex justify-content-between align-items-center mb-2">
  <h5 class="mb-0">
    <a class="text-decoration-none" href="/{{ plural|default("fhirdatasets") }}">{{ crd.kind if crd else 'Objects' }}</a>
    <span class="text-body-secondary">/</span>
    <a class="text-decoration-none font-monospace"
       href="/{{ plural|default("fhirdatasets") }}/{{ namespace }}/{{ name }}">{{ namespace }}/{{ name }}</a>
    <span class="text-body-secondary">/</span> logs
    {% if job %}<span class="badge text-bg-light font-monospace">{{ job }}</span>{% endif %}
  </h5>
  <span id="state" class="badge text-bg-secondary">connecting</span>
</div>

{% if error %}<div class="alert alert-danger"><pre class="mb-0">{{ error }}</pre></div>{% endif %}

<form class="row g-2 align-items-end mb-2" method="get">
  {% if job %}<input type="hidden" name="job" value="{{ job }}">{% endif %}
  <div class="col-auto">
    <label class="form-label small mb-0">Pod</label>
    <select class="form-select form-select-sm font-monospace" name="pod"
            onchange="this.form.submit()">
      <option value="all" {{ 'selected' if want == 'all' }}>all workers ({{ pods|length }})</option>
      {% for p in pods %}
        <option value="{{ p.name }}" {{ 'selected' if want == p.name }}>
          [{{ p.index or '-' }}] {{ p.name }} - {{ p.phase }}{% if p.restarts %} ({{ p.restarts }} restarts){% endif %}
        </option>
      {% endfor %}
    </select>
  </div>
  <div class="col-auto">
    <div class="form-check form-switch small">
      <input class="form-check-input" type="checkbox" name="previous" value="1"
             id="previous" {{ 'checked' if previous }} onchange="this.form.submit()">
      <label class="form-check-label" for="previous">Crashed container</label>
    </div>
    <div class="form-check form-switch small">
      <input class="form-check-input" type="checkbox" name="ts" value="1"
             id="ts" {{ 'checked' if timestamps }} onchange="this.form.submit()">
      <label class="form-check-label" for="ts">Timestamps</label>
    </div>
  </div>
  <div class="col-auto">
    <label class="form-label small mb-0">Filter</label>
    <input class="form-control form-control-sm" id="filter" placeholder="substring"
           autocomplete="off">
  </div>
  <div class="col-auto">
    <div class="form-check form-switch small">
      <input class="form-check-input" type="checkbox" id="follow" checked>
      <label class="form-check-label" for="follow">Follow</label>
    </div>
    <div class="form-check form-switch small">
      <input class="form-check-input" type="checkbox" id="wrapping">
      <label class="form-check-label" for="wrapping">Wrap</label>
    </div>
  </div>
  <div class="col-auto ms-auto">
    <button class="btn btn-sm btn-outline-secondary" type="button" id="pause">Pause</button>
    <button class="btn btn-sm btn-outline-secondary" type="button" id="clear">Clear</button>
    <a class="btn btn-sm btn-outline-secondary" id="download"
       href="/{{ plural|default("fhirdatasets") }}/{{ namespace }}/{{ name }}/logs/download">Download</a>
  </div>
</form>

<pre id="log" class="bg-dark text-light rounded p-2"></pre>
<div class="form-text">
  Last {{ tail }} lines then live. The kubelet holds these, so a pod that has
  been deleted has no log to show -- and a stream left open is cut after
  {{ cap }} minutes and reconnects, which re-tails.
</div>

</main>

<script>
var LIMIT = 5000;
var rows = [], paused = false, source = null;
var pane = document.getElementById('log');
var filterBox = document.getElementById('filter');
var followBox = document.getElementById('follow');
var stateBadge = document.getElementById('state');
var palette = ['#7ee787', '#79c0ff', '#ffa657', '#d2a8ff', '#f2cc60', '#ff7b72'];
var colours = {};
var multi = {{ 'true' if want == 'all' else 'false' }};

function query() {
  var params = new URLSearchParams(window.location.search);
  var out = new URLSearchParams();
  ['pod', 'job', 'previous', 'ts'].forEach(function (key) {
    if (params.get(key)) { out.set(key, params.get(key)); }
  });
  return out.toString();
}

function colourFor(pod) {
  if (!(pod in colours)) {
    colours[pod] = palette[Object.keys(colours).length % palette.length];
  }
  return colours[pod];
}

function matches(row) {
  var needle = filterBox.value.toLowerCase();
  return !needle || row.text.toLowerCase().indexOf(needle) >= 0;
}

function draw(row, into) {
  if (!matches(row)) { return; }
  var line = document.createElement('div');
  if (multi) {
    var tag = document.createElement('span');
    tag.className = 'idx';
    tag.textContent = '[' + (row.index === '' ? '?' : row.index) + ']';
    tag.style.color = colourFor(row.pod);
    line.appendChild(tag);
  }
  var body = document.createElement('span');
  body.textContent = row.text;
  if (row.meta) { body.className = 'meta'; }
  line.appendChild(body);
  into.appendChild(line);
}

function atBottom() {
  return pane.scrollHeight - pane.scrollTop - pane.clientHeight < 40;
}

function redraw() {
  var stuck = followBox.checked;
  var fresh = document.createDocumentFragment();
  rows.forEach(function (row) { draw(row, fresh); });
  pane.replaceChildren(fresh);
  if (stuck) { pane.scrollTop = pane.scrollHeight; }
}

function add(row) {
  rows.push(row);
  if (rows.length > LIMIT) { rows.splice(0, rows.length - LIMIT); redraw(); return; }
  if (paused) { return; }
  var stuck = followBox.checked && atBottom();
  draw(row, pane);
  if (stuck) { pane.scrollTop = pane.scrollHeight; }
}

function setState(text, kind) {
  stateBadge.textContent = text;
  stateBadge.className = 'badge text-bg-' + kind;
  document.getElementById('tick').textContent = new Date().toLocaleTimeString();
}

function connect() {
  if (source) { source.close(); }
  source = new EventSource('/{{ plural|default("fhirdatasets") }}/{{ namespace }}/{{ name }}/logs/stream?' + query());
  source.addEventListener('open', function () { setState('streaming', 'primary'); });
  source.addEventListener('line', function (event) { add(JSON.parse(event.data)); });
  source.addEventListener('eof', function (event) {
    var row = JSON.parse(event.data);
    row.text = '--- ' + row.pod + ' log ended';
    row.meta = true;
    add(row);
  });
  source.addEventListener('done', function () {
    setState('ended', 'success');
    source.close();
    source = null;
  });
  source.onopen = function () { setState('streaming', 'primary'); };
  source.onerror = function () {
    if (source && source.readyState === EventSource.CLOSED) {
      setState('disconnected', 'danger');
    } else {
      setState('reconnecting', 'warning');
    }
  };
}

filterBox.addEventListener('input', redraw);
followBox.addEventListener('change', function () {
  if (followBox.checked) { pane.scrollTop = pane.scrollHeight; }
});
document.getElementById('wrapping').addEventListener('change', function (event) {
  document.body.classList.toggle('wrap', event.target.checked);
});
document.getElementById('pause').addEventListener('click', function (event) {
  paused = !paused;
  event.target.textContent = paused ? 'Resume' : 'Pause';
  event.target.classList.toggle('btn-warning', paused);
  event.target.classList.toggle('btn-outline-secondary', !paused);
  if (!paused) { redraw(); }
});
document.getElementById('clear').addEventListener('click', function () {
  rows = [];
  pane.replaceChildren();
});
document.getElementById('download').href =
  '/{{ plural|default("fhirdatasets") }}/{{ namespace }}/{{ name }}/logs/download?' + query();
connect();
</script>
</body></html>
"""


BENCHMARK = """
<!doctype html>
<html lang="en"><head>
<meta charset="utf-8"><meta name="viewport" content="width=device-width, initial-scale=1">
<title>{{ name }} - benchmark</title>
<link href="https://cdn.jsdelivr.net/npm/bootstrap@5.3.3/dist/css/bootstrap.min.css" rel="stylesheet">
</head><body class="bg-body-tertiary">

<nav class="navbar navbar-expand bg-dark" data-bs-theme="dark"><div class="container-fluid">
  <span class="navbar-brand">FHIR operator</span>
  <ul class="navbar-nav me-auto">
  {% for c in crds %}
    <li class="nav-item"><a class="nav-link {{ 'active' if crd and c.plural == crd.plural }}"
       href="/{{ c.plural }}">{{ c.kind }}</a></li>
  {% endfor %}
  </ul>
</div></nav>

<main class="container-fluid py-4">
{% if error %}<div class="alert alert-danger"><pre class="mb-0">{{ error }}</pre></div>{% endif %}

<div class="d-flex justify-content-between align-items-center mb-3">
  <h5 class="mb-0">
    <a class="text-decoration-none" href="/fhirbenchmarks">FhirBenchmark</a>
    <span class="text-body-secondary">/</span>
    <span class="font-monospace">{{ namespace }}/{{ name }}</span>
  </h5>
  <div class="text-nowrap">
    <a class="btn btn-sm btn-outline-secondary"
       href="/fhirbenchmarks/{{ namespace }}/{{ name }}/logs?pod=all">Worker logs</a>
    <a class="btn btn-sm btn-outline-secondary"
       href="/fhirbenchmarks/{{ namespace }}/{{ name }}/results.json">results.json</a>
    <a class="btn btn-sm btn-outline-secondary"
       href="/fhirbenchmarks/{{ namespace }}/{{ name }}/objects">objects</a>
    <a class="btn btn-sm btn-outline-secondary"
       href="/fhirbenchmarks?edit={{ namespace }}/{{ name }}#editor">Edit spec</a>
  </div>
</div>

<div class="row g-3 mb-3">
  <div class="col-md-3"><div class="card h-100"><div class="card-body">
    <div class="small text-body-secondary text-uppercase">Phase</div>
    <div class="h5 mb-0">{{ status.phase or '-' }}</div>
    <div class="small text-body-secondary">{{ status.reason or '' }}</div>
  </div></div></div>
  <div class="col-md-3"><div class="card h-100"><div class="card-body">
    <div class="small text-body-secondary text-uppercase">Progress</div>
    <div class="h5 mb-0 font-monospace">{{ status.progress or '-' }}</div>
    <div class="small text-body-secondary">run {{ status.runId or '-' }},
      step {{ status.currentStepId or '-' }}</div>
  </div></div></div>
  <div class="col-md-3"><div class="card h-100"><div class="card-body">
    <div class="small text-body-secondary text-uppercase">Invalid</div>
    <div class="h5 mb-0 {{ 'text-danger' if status.invalidCount }}">{{ status.invalidCount or 0 }}</div>
    <div class="small text-body-secondary">an invalid measurement is never
      dropped, only excluded from the percentiles</div>
  </div></div></div>
  <div class="col-md-3"><div class="card h-100"><div class="card-body">
    <div class="small text-body-secondary text-uppercase">Catalogue</div>
    <div class="h6 mb-0 font-monospace">{{ status.catalogueName or '-' }}
      ({{ status.caseCount or 0 }} cases)</div>
    <div class="small text-body-secondary font-monospace text-truncate">{{ status.catalogueRevision or '' }}</div>
  </div></div></div>
</div>

<div class="row g-3">
  <div class="col-xl-7">
    <div class="card mb-3">
      <div class="card-header">Journal
        <span class="small text-body-secondary">append-only; the record of the run,
        not a reflection of live state</span></div>
      <div class="table-responsive">
      <table class="table table-sm mb-0 align-middle">
        <thead><tr class="small"><th>#</th><th>Step</th><th>Type</th><th>Outcome</th>
          <th class="text-end">Shards</th><th class="text-end">Measured</th>
          <th class="text-end">Invalid</th><th class="text-end">Read ratio</th>
          <th>Node</th><th class="text-end">Started</th><th class="text-end">Took</th></tr></thead>
        <tbody>
        {% for e in journal %}
          <tr>
            <td class="font-monospace small">{{ e.index }}</td>
            <td class="font-monospace small">{{ e.id }}</td>
            <td class="small">{{ e.step }}</td>
            <td class="small">{{ e.outcome }}</td>
            <td class="text-end font-monospace small">{{ e.shards if e.shards is not none else '-' }}</td>
            <td class="text-end font-monospace small">{{ e.measurements if e.measurements is not none else '-' }}</td>
            <td class="text-end font-monospace small {{ 'text-danger fw-semibold' if e.invalid }}">{{ e.invalid if e.invalid is not none else '-' }}</td>
            <td class="text-end font-monospace small">{{ '%.3f'|format(e.coldReadRatio) if e.coldReadRatio is not none else '-' }}</td>
            <td class="small font-monospace">{{ e.node or '-' }}</td>
            <td class="text-end small font-monospace">{{ e.started }}</td>
            <td class="text-end small font-monospace">{{ e.duration }}</td>
          </tr>
          {% if e.reasons or e.detail %}
          <tr><td></td><td colspan="10">
            {% for r in e.reasons %}<div class="small text-danger-emphasis font-monospace">{{ r }}</div>{% endfor %}
            {% if e.detail %}<div class="small text-body-secondary font-monospace">{{ e.detail }}</div>{% endif %}
          </td></tr>
          {% endif %}
        {% else %}
          <tr><td colspan="11" class="text-body-secondary">Nothing has run yet.</td></tr>
        {% endfor %}
        </tbody>
      </table>
      </div>
    </div>

    <div class="card mb-3">
      <div class="card-header">Matched-pair deltas
        <span class="small text-body-secondary">same cohort, one term different
        across the Hibernate Search eligibility boundary</span></div>
      <div class="table-responsive">
      <table class="table table-sm mb-0 align-middle">
        <thead><tr class="small"><th>Pair</th><th>Cache</th>
          <th class="text-end">Eligible p50</th><th class="text-end">Disqualified p50</th>
          <th class="text-end">Ratio</th><th>Engines</th></tr></thead>
        <tbody>
        {% for d in deltas %}
          <tr>
            <td class="font-monospace small">{{ d.pair }}</td>
            <td class="small">{{ d.cache }}</td>
            <td class="text-end font-monospace small">{{ fmt(d.eligible) }}</td>
            <td class="text-end font-monospace small">{{ fmt(d.disqualified) }}</td>
            <td class="text-end font-monospace small fw-semibold">{{ d.ratio }}x</td>
            <td class="small font-monospace">{{ d.eligibleEngine }} vs {{ d.disqualifiedEngine }}</td>
          </tr>
        {% else %}
          <tr><td colspan="6" class="text-body-secondary">
            No pair has both members measured yet. A half-measured pair is not
            a delta.</td></tr>
        {% endfor %}
        </tbody>
      </table>
      </div>
    </div>
  </div>

  <div class="col-xl-5">
    <div class="card mb-3">
      <div class="card-header">Plan</div>
      <div class="table-responsive">
      <table class="table table-sm mb-0 align-middle">
        <thead><tr class="small"><th>#</th><th>Step</th><th>Type</th><th>Detail</th></tr></thead>
        <tbody>
        {% for s in plan %}
          <tr>
            <td class="font-monospace small">{{ loop.index0 }}</td>
            <td class="font-monospace small">{{ s.id }}</td>
            <td class="small">{{ s.step }}</td>
            <td class="small font-monospace text-body-secondary">
              {% if s.components %}{{ s.components|join(', ') }}{% endif %}
              {% if s.cacheLabel %}cache={{ s.cacheLabel }}{% endif %}
              {% if s.repeats %} repeats={{ s.repeats }}{% endif %}
              {% if s.bindingMode %} bind={{ s.bindingMode }}{% endif %}
              {% if s.relations %}{{ s.relations|length }} relations{% endif %}
              {% if s.require %}{{ s.require }}{% endif %}
              {% if s.hapi %}{{ s.hapi }}{% endif %}
            </td>
          </tr>
        {% endfor %}
        </tbody>
      </table>
      </div>
    </div>

    <div class="card mb-3">
      <div class="card-header">Bindings
        <span class="small text-body-secondary">seed {{ status.seed }} -- selectivity
        is controlled, not accidental</span></div>
      <div class="table-responsive">
      <table class="table table-sm mb-0 align-middle">
        <tbody>
        {% for key, value in bindings %}
          <tr><td class="small font-monospace">{{ key }}</td>
              <td class="small font-monospace text-body-secondary">{{ value }}</td></tr>
        {% else %}
          <tr><td class="text-body-secondary small">Not bound yet.</td></tr>
        {% endfor %}
        </tbody>
      </table>
      </div>
    </div>

    <div class="card mb-3">
      <div class="card-header">Jobs and pods</div>
      <div class="table-responsive">
      <table class="table table-sm mb-0 align-middle">
        <thead><tr class="small"><th>Job</th><th>Step</th><th>Phase</th>
          <th class="text-end">Done</th><th class="text-end">Failed</th>
          <th class="text-end">Age</th></tr></thead>
        <tbody>
        {% for j in jobs %}
          <tr><td class="font-monospace small">{{ j.name }}</td>
              <td class="small">{{ j.step }}</td>
              <td class="small">{{ j.phase }}</td>
              <td class="text-end font-monospace small">{{ j.completions }}</td>
              <td class="text-end font-monospace small {{ 'text-danger' if j.failed }}">{{ j.failed }}</td>
              <td class="text-end font-monospace small">{{ j.age }}</td></tr>
        {% else %}
          <tr><td colspan="6" class="text-body-secondary small">No benchmark Jobs.</td></tr>
        {% endfor %}
        </tbody>
      </table>
      </div>
      <div class="table-responsive border-top">
      <table class="table table-sm mb-0 align-middle">
        <thead><tr class="small"><th>Pod</th><th>Phase</th><th>Node</th>
          <th class="text-end">Age</th><th></th></tr></thead>
        <tbody>
        {% for p in pods %}
          <tr><td class="font-monospace small">{{ p.name }}</td>
              <td class="small">{{ p.phase }} {{ p.reason }}</td>
              <td class="small font-monospace">{{ p.node }}</td>
              <td class="text-end small font-monospace">{{ p.age }}</td>
              <td class="text-end small">
                <a href="/fhirbenchmarks/{{ namespace }}/{{ name }}/logs?pod={{ p.name }}">tail</a></td></tr>
        {% else %}
          <tr><td colspan="5" class="text-body-secondary small">No worker pods.</td></tr>
        {% endfor %}
        </tbody>
      </table>
      </div>
    </div>
  </div>
</div>

<div class="card">
  <div class="card-header">Every case
    <span class="small text-body-secondary">p50/p90/p95/p99 from raw samples,
    never from histogram buckets</span></div>
  <div class="table-responsive">
  <table class="table table-sm table-striped mb-0 align-middle">
    <thead><tr class="small"><th>Case</th><th>Family</th><th>Cache</th>
      <th class="text-end">n</th><th class="text-end">invalid</th>
      <th class="text-end">p50</th><th class="text-end">p90</th>
      <th class="text-end">p95</th><th class="text-end">p99</th>
      <th class="text-end">max</th><th class="text-end">page</th>
      <th class="text-end">total</th><th>Engine</th><th>Declared</th></tr></thead>
    <tbody>
    {% for r in rows %}
      <tr>
        <td class="font-monospace small">{{ r.case }}</td>
        <td class="small">{{ r.family }}</td>
        <td class="small">{{ r.cache }}</td>
        <td class="text-end font-monospace small">{{ r.n }}</td>
        <td class="text-end font-monospace small {{ 'text-danger fw-semibold' if r.invalid }}">{{ r.invalid }}</td>
        <td class="text-end font-monospace small">{{ fmt(r.p50) }}</td>
        <td class="text-end font-monospace small">{{ fmt(r.p90) }}</td>
        <td class="text-end font-monospace small">{{ fmt(r.p95) }}</td>
        <td class="text-end font-monospace small">{{ fmt(r.p99) }}</td>
        <td class="text-end font-monospace small">{{ fmt(r.max) }}</td>
        <td class="text-end font-monospace small">{{ fmt(r.rows) }}</td>
        <td class="text-end font-monospace small">{{ fmt(r.total) }}</td>
        <td class="small font-monospace {{ 'text-danger' if r.expected and r.engine != r.expected }}">{{ r.engine }}</td>
        <td class="small font-monospace text-body-secondary">{{ r.expected or '-' }}</td>
      </tr>
      {% if r.reasons %}
        <tr><td colspan="14">
          {% for reason in r.reasons %}
            <div class="small text-danger-emphasis font-monospace">{{ reason }}</div>
          {% endfor %}
        </td></tr>
      {% endif %}
    {% else %}
      <tr><td colspan="14" class="text-body-secondary">No measurements yet.</td></tr>
    {% endfor %}
    </tbody>
  </table>
  </div>
</div>

</main>
<script>setTimeout(function () { location.reload(); }, 15000);</script>
</body></html>
"""


CATALOGUE = """
<!doctype html>
<html lang="en"><head>
<meta charset="utf-8"><meta name="viewport" content="width=device-width, initial-scale=1">
<title>catalogue {{ name }}</title>
<link href="https://cdn.jsdelivr.net/npm/bootstrap@5.3.3/dist/css/bootstrap.min.css" rel="stylesheet">
</head><body class="bg-body-tertiary">

<nav class="navbar navbar-expand bg-dark" data-bs-theme="dark"><div class="container-fluid">
  <span class="navbar-brand">FHIR operator</span>
  <ul class="navbar-nav me-auto">
  {% for c in crds %}
    <li class="nav-item"><a class="nav-link" href="/{{ c.plural }}">{{ c.kind }}</a></li>
  {% endfor %}
  </ul>
</div></nav>

<main class="container-fluid py-4">
<div class="d-flex justify-content-between align-items-center mb-3">
  <h5 class="mb-0">Catalogue <span class="font-monospace">{{ name }}</span>
    <span class="badge text-bg-secondary rounded-pill">{{ cases|length }}</span></h5>
  <span class="small font-monospace text-body-secondary">{{ revision }}</span>
</div>

<div class="card mb-3"><div class="card-body py-2 d-flex flex-wrap gap-2">
  {% for family, count in families %}
    <span class="badge text-bg-light border font-monospace">{{ family }} {{ count }}</span>
  {% endfor %}
</div></div>

<div class="card">
  <div class="table-responsive">
  <table class="table table-sm table-striped mb-0 align-middle">
    <thead><tr class="small"><th>Case</th><th>Family</th><th>Request</th>
      <th>Declared engine</th><th>Why</th></tr></thead>
    <tbody>
    {% for c in cases %}
      <tr>
        <td class="font-monospace small">{{ c.id }}</td>
        <td class="small">{{ c.family }}</td>
        <td class="font-monospace small">{{ c.method }} {{ c.path }}{% if c.query %}?{{ c.query }}{% endif %}</td>
        <td class="small font-monospace">{{ c.expectEngine or '-' }}</td>
        <td class="small text-body-secondary">{{ c.note }}</td>
      </tr>
    {% endfor %}
    </tbody>
  </table>
  </div>
</div>
</main>
</body></html>
"""

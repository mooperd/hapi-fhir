# Created by claude-opus-5
"""Walking-skeleton control panel for the HAPI FHIR search-performance experiment.

Three pages: deploy a stack into a namespace, load synthetic data into it,
benchmark it. Data shape follows data-import.md, query matrix follows
search-experiment.md.

Throughput knobs are the two constants BUNDLE_SIZE and WORKERS. They are not
UI options on purpose. Per-container memory is a UI option, because a stack
sized for the big node will not schedule anywhere else.
"""

import datetime
import os
import queue
import random
import socket
import subprocess
import threading
import time
from statistics import median

import requests
from flask import Flask, redirect, render_template, request, url_for
from kubernetes import client as k8s
from kubernetes import config as k8s_config
from kubernetes.client.rest import ApiException

HERE = os.path.dirname(os.path.abspath(__file__))
DOWNLOADER = os.path.expanduser("~/Documents/GitHub/mimic-fhir-downloader")

KUBECONFIG = os.environ.get("PERF_KUBECONFIG", os.path.join(DOWNLOADER, "behemoth-andrew-test.yaml"))
MANIFEST = os.environ.get("PERF_MANIFEST", os.path.join(DOWNLOADER, "hapi-fhir-standalone.yaml"))

# --------------------------------------------------------------------------
# Memory profile. "min" becomes requests.memory, "max" becomes limits.memory.
# Values are GiB. The defaults are the manifest's, so deploying without
# touching them reproduces hapi-fhir-standalone.yaml exactly.
#
# (deployment, container, label, default min, default max)
# --------------------------------------------------------------------------

COMPONENTS = [
    ("hapi-fhir", "hapi-fhir", "HAPI", 2.0, 10.0),
    ("hapi-fhir-db", "postgres", "Postgres", 180.0, 180.0),
    ("hapi-fhir-es", "elasticsearch", "Elasticsearch", 40.0, 40.0),
]
CONTAINER_OF = {deployment: container for deployment, container, _, _, _ in COMPONENTS}

MEM_FLOOR_GI = 0.25    # below this nothing in the stack starts at all
PG_BASE_GI = 180.0     # the limit postgres' manifest args were tuned against

BUNDLE_SIZE = 1000      # resource entries per transaction bundle
WORKERS = 8             # concurrent bundle posters
SEED = 20260911         # dataset seed; same seed, same dataset
POST_TIMEOUT = 600      # seconds, per transaction bundle
QUERY_TIMEOUT = 300     # seconds, per benchmark request
BENCH_REPS = 3

COND_SYS = "http://perf.pkb/cond"
OBS_SYS = "http://perf.pkb/obs"

DAY_ZERO = datetime.date(2015, 1, 1)
DAY_SPAN = 3652                       # 2015-01-01 .. 2024-12-31
BIRTH_ZERO = datetime.date(1930, 1, 1)
BIRTH_SPAN = 29584                    # 1930-01-01 .. 2010-12-31

app = Flask(__name__)


# --------------------------------------------------------------------------
# Code dictionaries: 45 codes per type in three selectivity tiers.
# 1 code at 20% of rows, 9 at 5%, 35 at 1%.
# --------------------------------------------------------------------------

def _dictionary(prefix):
    codes = ["%s-COMMON-01" % prefix]
    weights = [20.0]
    for i in range(1, 10):
        codes.append("%s-MID-%02d" % (prefix, i))
        weights.append(5.0)
    for i in range(1, 36):
        codes.append("%s-RARE-%02d" % (prefix, i))
        weights.append(1.0)
    return codes, weights


COND_CODES, COND_WEIGHTS = _dictionary("COND")
OBS_CODES, OBS_WEIGHTS = _dictionary("OBS")


# --------------------------------------------------------------------------
# Generator
# --------------------------------------------------------------------------

def patient_bundle_entries(serial):
    """All resources for one patient, as transaction entries.

    Everything derives from the serial, so any worker can generate any
    patient independently and a rerun reproduces the dataset exactly.
    """
    rng = random.Random(SEED * 1000003 + serial)
    heavy = serial % 10 == 0
    n_cond, n_obs = (90, 500) if heavy else (11, 53)

    pid = "perf-p%07d" % serial
    entries = [_entry({
        "resourceType": "Patient",
        "id": pid,
        "identifier": [{"system": "http://perf.pkb/mrn", "value": str(serial)}],
        "gender": "female" if rng.random() < 0.5 else "male",
        "birthDate": str(BIRTH_ZERO + datetime.timedelta(days=rng.randint(0, BIRTH_SPAN))),
        "name": [{"family": "Surname%07d" % serial}],
    }, "Patient", pid)]

    for code, i in zip(rng.choices(COND_CODES, weights=COND_WEIGHTS, k=n_cond), range(n_cond)):
        rid = "perf-c%07d-%03d" % (serial, i)
        entries.append(_entry({
            "resourceType": "Condition",
            "id": rid,
            "subject": {"reference": "Patient/" + pid},
            "clinicalStatus": {"coding": [{
                "system": "http://terminology.hl7.org/CodeSystem/condition-clinical",
                "code": "active"}]},
            "code": {"coding": [{"system": COND_SYS, "code": code}]},
            "onsetDateTime": _a_date(rng),
        }, "Condition", rid))

    for code, i in zip(rng.choices(OBS_CODES, weights=OBS_WEIGHTS, k=n_obs), range(n_obs)):
        rid = "perf-o%07d-%03d" % (serial, i)
        entries.append(_entry({
            "resourceType": "Observation",
            "id": rid,
            "status": "final",
            "subject": {"reference": "Patient/" + pid},
            "code": {"coding": [{"system": OBS_SYS, "code": code}]},
            "valueQuantity": {"value": round(rng.uniform(0.5, 250.0), 2), "unit": "mg/dL",
                              "system": "http://unitsofmeasure.org", "code": "mg/dL"},
            "effectiveDateTime": _a_date(rng),
        }, "Observation", rid))

    return entries


def _entry(resource, rtype, rid):
    # PUT rather than POST so a re-run of a finished range is a no-op and an
    # interrupted load can simply be restarted.
    return {"resource": resource, "request": {"method": "PUT", "url": "%s/%s" % (rtype, rid)}}


def _a_date(rng):
    return str(DAY_ZERO + datetime.timedelta(days=rng.randint(0, DAY_SPAN)))


def bundles(first_patient, n_patients):
    """Stream transaction bundles of roughly BUNDLE_SIZE entries."""
    entries, patients = [], 0
    for serial in range(first_patient, first_patient + n_patients):
        entries.extend(patient_bundle_entries(serial))
        patients += 1
        if len(entries) >= BUNDLE_SIZE:
            yield {"resourceType": "Bundle", "type": "transaction", "entry": entries}, patients
            entries, patients = [], 0
    if entries:
        yield {"resourceType": "Bundle", "type": "transaction", "entry": entries}, patients


# --------------------------------------------------------------------------
# Cluster
# --------------------------------------------------------------------------

def kubectl(*args, timeout=120):
    proc = subprocess.run(
        ["kubectl", "--kubeconfig", KUBECONFIG] + list(args),
        capture_output=True, text=True, timeout=timeout)
    return proc.returncode, proc.stdout, proc.stderr


_apps_lock = threading.Lock()
_apps = None


def apps_api():
    """Lazy AppsV1Api against KUBECONFIG, shared by every request thread."""
    global _apps
    with _apps_lock:
        if _apps is None:
            k8s_config.load_kube_config(config_file=KUBECONFIG)
            _apps = k8s.AppsV1Api()
        return _apps


def list_stacks():
    """Namespaces holding a hapi-fhir deployment, with readiness and memory."""
    try:
        items = apps_api().list_deployment_for_all_namespaces().items
    except Exception as exc:                               # noqa: BLE001 - surfaced in the UI
        return [], "%s: %s" % (type(exc).__name__, exc)

    found = {}
    for item in items:
        name = item.metadata.name
        if name not in CONTAINER_OF:
            continue
        stack = found.setdefault(item.metadata.namespace,
                                 {"namespace": item.metadata.namespace})
        stack[name] = {
            "ready": "%d/%d" % (item.status.ready_replicas or 0, item.spec.replicas or 1),
            "min": _memory_of(item, CONTAINER_OF[name], "requests"),
            "max": _memory_of(item, CONTAINER_OF[name], "limits"),
        }
    return sorted(found.values(), key=lambda s: s["namespace"]), None


def _memory_of(deployment, container_name, field):
    for container in deployment.spec.template.spec.containers:
        if container.name != container_name:
            continue
        quantities = getattr(container.resources, field, None) if container.resources else None
        return (quantities or {}).get("memory") or "unset"
    return "-"


def deploy(namespace, profile):
    kubectl("create", "namespace", namespace)  # already-exists is fine
    rc, out, err = kubectl("apply", "-n", namespace, "-f", MANIFEST, timeout=300)
    lines = [line for line in (out + err).strip().splitlines() if line]
    if rc != 0:
        return "\n".join(lines)
    # The manifest carries the big-node numbers, so the freshly applied
    # deployments are patched before their pods have anything to do.
    return "\n".join(lines + set_memory(namespace, profile))


# --------------------------------------------------------------------------
# Memory
#
# Postgres and Elasticsearch commit memory up front -- shared_buffers and
# -Xms are reserved at startup, not grown into -- so a limit below what they
# ask for is an OOM kill during boot rather than a slow server. Both are
# therefore derived from the limit rather than left at the manifest's values.
# CPU requests (24 for postgres, 10 for elasticsearch) are deliberately left
# alone; they still gate scheduling on a small node.
# --------------------------------------------------------------------------

def set_memory(namespace, profile):
    """Patch requests/limits, and the tuning that has to move with them.

    Returns one human-readable line per deployment, for the UI.
    """
    notes = []
    for deployment, container, label, _, _ in COMPONENTS:
        low, high = profile[deployment]
        patch = {
            "name": container,
            "resources": {
                "requests": {"memory": _mib(low)},
                "limits": {"memory": _mib(high)},
            },
        }
        note = "%s %s -> %s" % (label, _mib(low), _mib(high))

        if container == "postgres":
            patch["args"] = _postgres_args(high)
            note += " (shared_buffers=%s)" % patch["args"][1].split("=", 1)[1]
        elif container == "elasticsearch":
            heap = _es_heap(high)
            # env merges on name, so this replaces ES_JAVA_OPTS and nothing else.
            patch["env"] = [{"name": "ES_JAVA_OPTS", "value": "-Xms%s -Xmx%s" % (heap, heap)}]
            note += " (heap=%s)" % heap

        try:
            apps_api().patch_namespaced_deployment(
                name=deployment, namespace=namespace,
                body={"spec": {"template": {"spec": {"containers": [patch]}}}})
        except ApiException as exc:
            notes.append("%s: %s %s" % (deployment, exc.status, exc.reason))
            continue
        notes.append(note)
    return notes


def _mib(gibibytes):
    return "%dMi" % round(gibibytes * 1024)


def _postgres_args(limit_gi):
    """The manifest's postgres args, scaled to the container limit.

    Ratios are the manifest's own, so a 180Gi limit reproduces it verbatim.
    max_connections is left at 300: work_mem is per sort, not per server, so
    the manifest's warning about connections outrunning memory still stands
    at every scale.
    """
    ratio = limit_gi / PG_BASE_GI

    def scaled(base_mb, floor_mb):
        return max(int(base_mb * ratio), floor_mb)

    return [
        "-c", "shared_buffers=%dMB" % scaled(65536, 128),
        "-c", "effective_cache_size=%dMB" % scaled(163840, 256),
        "-c", "maintenance_work_mem=%dMB" % scaled(4096, 64),
        "-c", "autovacuum_work_mem=%dMB" % scaled(2048, 32),
        "-c", "work_mem=%dMB" % scaled(1024, 4),
        "-c", "max_connections=300",
        "-c", "max_wal_size=32GB",
        "-c", "checkpoint_timeout=30min",
        "-c", "wal_buffers=64MB",
        "-c", "max_worker_processes=60",
        "-c", "shared_preload_libraries=pg_stat_statements",
    ]


def _es_heap(limit_gi):
    """Half the limit, per Elastic's sizing guidance, short of compressed-oops.

    Returned as an -Xmx string in megabytes rather than gigabytes, so a limit
    under 2Gi still gets a heap that fits inside it. -Xms is committed at
    startup, so a heap above the limit is an OOM kill during boot.
    """
    megabytes = min(int(limit_gi * 1024 / 2), 31 * 1024)
    return "%dm" % max(megabytes, 256)


def read_profile(form):
    """Pull a {deployment: (min_gi, max_gi)} profile out of the submitted form."""
    profile = {}
    for deployment, _, label, default_min, default_max in COMPONENTS:
        low = _gibibytes(form.get(deployment + "-min"), default_min)
        high = _gibibytes(form.get(deployment + "-max"), default_max)
        if low > high:
            raise ValueError("%s: min %gGi is above max %gGi" % (label, low, high))
        profile[deployment] = (low, high)
    return profile


def _gibibytes(raw, fallback):
    if raw is None or not raw.strip():
        return fallback
    return max(float(raw), MEM_FLOOR_GI)


# --------------------------------------------------------------------------
# Port-forward: the Flask app runs on the laptop, HAPI lives in the cluster.
# One forward per namespace, restarted if the process dies.
# --------------------------------------------------------------------------

_forwards = {}
_forward_lock = threading.Lock()


def base_url(namespace):
    with _forward_lock:
        entry = _forwards.get(namespace)
        if entry and entry[1].poll() is None:
            return "http://127.0.0.1:%d/fhir" % entry[0]

        sock = socket.socket()
        sock.bind(("127.0.0.1", 0))
        port = sock.getsockname()[1]
        sock.close()

        proc = subprocess.Popen(
            ["kubectl", "--kubeconfig", KUBECONFIG, "-n", namespace,
             "port-forward", "svc/hapi-fhir", "%d:8080" % port],
            stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
        _forwards[namespace] = (port, proc)

    deadline = time.time() + 30
    while time.time() < deadline:
        try:
            socket.create_connection(("127.0.0.1", port), timeout=1).close()
            return "http://127.0.0.1:%d/fhir" % port
        except OSError:
            time.sleep(0.5)
    raise RuntimeError("port-forward to %s never came up" % namespace)


# --------------------------------------------------------------------------
# Jobs
# --------------------------------------------------------------------------

LOAD = {"state": "idle"}
BENCH = {"state": "idle"}


def run_load(namespace, n_patients, first_patient):
    url = base_url(namespace)
    work = queue.Queue(maxsize=WORKERS * 2)
    lock = threading.Lock()
    session_headers = {"Content-Type": "application/fhir+json", "Prefer": "return=minimal"}

    def worker():
        session = requests.Session()
        while True:
            item = work.get()
            if item is None:
                work.task_done()
                return
            bundle, patients = item
            try:
                resp = session.post(url, json=bundle, headers=session_headers, timeout=POST_TIMEOUT)
                ok = resp.status_code < 300
                detail = None if ok else "HTTP %d: %s" % (resp.status_code, resp.text[:400])
            except Exception as exc:                       # noqa: BLE001 - surfaced in the UI
                ok, detail = False, "%s: %s" % (type(exc).__name__, exc)
            with lock:
                if ok:
                    LOAD["bundles_ok"] += 1
                    LOAD["patients_done"] += patients
                    LOAD["resources"] += len(bundle["entry"])
                else:
                    LOAD["bundles_failed"] += 1
                    LOAD["last_error"] = detail
                LOAD["elapsed"] = time.time() - LOAD["started"]
                LOAD["rate"] = LOAD["resources"] / max(LOAD["elapsed"], 0.001)
            work.task_done()

    threads = [threading.Thread(target=worker, daemon=True) for _ in range(WORKERS)]
    for thread in threads:
        thread.start()
    for item in bundles(first_patient, n_patients):
        work.put(item)
    for _ in threads:
        work.put(None)
    work.join()

    LOAD["state"] = "failed" if LOAD["bundles_failed"] else "done"


# The eight cases from search-experiment.md, at rare selectivity.
CASES = [
    ("A", "Baseline, no chain",
     "Observation?code=%s|OBS-RARE-17" % OBS_SYS),
    ("B", "Forward chain, qualified",
     "Observation?code=%s|OBS-RARE-17&subject:Patient.gender=female" % OBS_SYS),
    ("C", "Forward chain, unqualified",
     "Observation?code=%s|OBS-RARE-17&subject.gender=female" % OBS_SYS),
    ("D", "Reverse chain, one _has",
     "Patient?_has:Condition:subject:code=%s|COND-RARE-17" % COND_SYS),
    ("E", "Reverse chain, two _has (cohort shape)",
     "Patient?_has:Condition:subject:code=%s|COND-RARE-17"
     "&_has:Observation:subject:code=%s|OBS-RARE-17" % (COND_SYS, OBS_SYS)),
    ("F", "Case E plus a Patient anchor",
     "Patient?_has:Condition:subject:code=%s|COND-RARE-17"
     "&_has:Observation:subject:code=%s|OBS-RARE-17"
     "&gender=female&birthdate=lt1960-01-01" % (COND_SYS, OBS_SYS)),
    ("G", "Chain-then-has (opposite direction)",
     "Condition?code=%s|COND-RARE-17"
     "&subject:Patient._has:Observation:subject:code=%s|OBS-RARE-17" % (COND_SYS, OBS_SYS)),
    ("H", "Reverse chain on a date",
     "Patient?_has:Observation:subject:date=ge2024-01-01"),
]

MODES = [("first-page", "&_offset=0&_count=50"), ("count", "&_summary=count")]


def run_bench(namespace):
    url = base_url(namespace)
    session = requests.Session()
    # no-cache defeats the 60s search-result reuse window, otherwise
    # repetitions 2..n measure an HFJ_SEARCH lookup.
    headers = {"Accept": "application/fhir+json", "Cache-Control": "no-cache"}

    for case_id, label, query in CASES:
        for mode, suffix in MODES:
            timings, total, bundle_ids, censored, error = [], None, set(), False, None
            for _ in range(BENCH_REPS):
                started = time.time()
                try:
                    resp = session.get(url + "/" + query + suffix, headers=headers,
                                       timeout=QUERY_TIMEOUT)
                    elapsed = (time.time() - started) * 1000
                    if resp.status_code >= 300:
                        error = "HTTP %d: %s" % (resp.status_code, resp.text[:200])
                        break
                    body = resp.json()
                    total = body.get("total")
                    bundle_ids.add(body.get("id"))
                    timings.append(elapsed)
                except requests.Timeout:
                    censored = True
                    timings.append(QUERY_TIMEOUT * 1000)
                    break
                except Exception as exc:                   # noqa: BLE001 - surfaced in the UI
                    error = "%s: %s" % (type(exc).__name__, exc)
                    break

            BENCH["results"].append({
                "case": case_id, "label": label, "mode": mode,
                "query": query + suffix,
                "median_ms": round(median(timings)) if timings else None,
                "max_ms": round(max(timings)) if timings else None,
                "total": total,
                "censored": censored,
                # Distinct bundle ids prove the no-cache header was honoured.
                "cache_ok": len(bundle_ids) == len(timings) if timings else False,
                "error": error,
            })
            BENCH["done"] += 1

    BENCH["state"] = "done"


# --------------------------------------------------------------------------
# Routes
# --------------------------------------------------------------------------

@app.route("/", methods=["GET", "POST"])
def stacks():
    message = None
    if request.method == "POST":
        namespace = request.form["namespace"].strip()
        action = request.form.get("action")
        try:
            if action == "delete":
                message = "\n".join(kubectl("delete", "namespace", namespace, timeout=300)[1:]).strip()
            elif action == "resize":
                # Patch in place: no re-apply, so nothing else in the stack moves.
                message = "\n".join(set_memory(namespace, read_profile(request.form)))
            else:
                message = deploy(namespace, read_profile(request.form))
        except ValueError as exc:
            message = str(exc)
    found, error = list_stacks()
    return render_template("stacks.html", stacks=found, error=error, message=message,
                           components=COMPONENTS, manifest=MANIFEST, kubeconfig=KUBECONFIG)


@app.route("/load", methods=["GET", "POST"])
def load():
    if request.method == "POST" and LOAD["state"] != "running":
        namespace = request.form["namespace"]
        n_patients = int(request.form["patients"])
        first = int(request.form.get("first") or 1)
        LOAD.clear()
        LOAD.update(state="running", namespace=namespace, patients=n_patients,
                    first=first, patients_done=0, resources=0, bundles_ok=0,
                    bundles_failed=0, last_error=None, started=time.time(),
                    elapsed=0.0, rate=0.0)
        threading.Thread(target=_guard, args=(LOAD, run_load, namespace, n_patients, first),
                         daemon=True).start()
        return redirect(url_for("load"))
    found, _ = list_stacks()
    return render_template("load.html", stacks=found, job=LOAD,
                           bundle_size=BUNDLE_SIZE, workers=WORKERS, seed=SEED)


@app.route("/benchmark", methods=["GET", "POST"])
def benchmark():
    if request.method == "POST" and BENCH["state"] != "running":
        namespace = request.form["namespace"]
        BENCH.clear()
        BENCH.update(state="running", namespace=namespace, results=[], done=0,
                     total=len(CASES) * len(MODES), started=time.time(), last_error=None)
        threading.Thread(target=_guard, args=(BENCH, run_bench, namespace),
                         daemon=True).start()
        return redirect(url_for("benchmark"))
    found, _ = list_stacks()
    return render_template("benchmark.html", stacks=found, job=BENCH, reps=BENCH_REPS,
                           timeout=QUERY_TIMEOUT)


def _guard(job, fn, *args):
    try:
        fn(*args)
    except Exception as exc:                               # noqa: BLE001 - surfaced in the UI
        job["state"] = "failed"
        job["last_error"] = "%s: %s" % (type(exc).__name__, exc)


if __name__ == "__main__":
    app.run(host="127.0.0.1", port=5021, debug=False, threaded=True)

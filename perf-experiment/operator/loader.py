# Created by claude-opus-5
"""Reconciler worker. Three modes: load, delete, census.

Every resource carries meta.tag http://perf.fhir/dataset|<name>, where name
is the FhirDataset's own metadata.name. That tag is the only handle on a
dataset: it is how delete finds its resources and how census counts them.

The name is the handle rather than the uid so a dataset can be selected and
counted without first looking up its uid. Names are reusable where uids are
not, so purgeOnDelete stops being merely a convenience.

load    generate and PUT one complete patient per transaction bundle
delete  DELETE <type>?_tag=...&_expunge=true, children before parents
census  count what is actually present, and report it

Worker 0 writes the census back to the FhirDataset status using the pod's
service account, so the operator learns actual state without needing to reach
the FHIR server itself -- it may be running on a laptop.

Entries are PUT, so a re-run of a finished range is a no-op and the load is
safely restartable. That is what makes reconciliation cheap.
"""

import csv
import datetime
import json
import os
import random
import re
import sys
import time
import traceback

import requests
from prometheus_client import Counter, Histogram, start_http_server

MODE = os.environ.get("MODE", "load")
BASE_URL = os.environ["FHIR_BASE_URL"]
CONFIG_PATH = os.environ.get("DATASET_CONFIG", "/config/dataset.json")
RESULTS_DIR = os.environ.get("RESULTS_DIR", "/results")
INDEX = int(os.environ.get("JOB_COMPLETION_INDEX", "0"))
PARALLELISM = int(os.environ.get("PARALLELISM", "1"))
METRICS_PORT = int(os.environ.get("METRICS_PORT", "9100"))
POST_TIMEOUT = int(os.environ.get("POST_TIMEOUT", "300"))
LINGER = int(os.environ.get("LINGER_SECONDS", "20"))

DATASET_NAME = os.environ.get("DATASET_NAME", "")
NAMESPACE = os.environ.get("POD_NAMESPACE", "")

TAG_SYSTEM = "http://perf.fhir/dataset"
DAY_ZERO = datetime.date(2015, 1, 1)
DAY_SPAN = 3652
BIRTH_ZERO = datetime.date(1930, 1, 1)
BIRTH_SPAN = 29584
SNOMED = "http://snomed.info/sct"

# Children before parents. A Patient cannot go first without orphaning
# everything that references it.
TYPES_YOUNGEST_FIRST = ["Observation", "Condition", "Patient"]

BUNDLES = Counter("fhir_load_bundles_total", "Transaction bundles posted", ["status"])
RESOURCES = Counter("fhir_load_resources_total", "Resources written")
PATIENTS = Counter("fhir_load_patients_total", "Patients written")
DELETED = Counter("fhir_delete_resources_total", "Resources expunged", ["type"])
LATENCY = Histogram("fhir_load_bundle_seconds", "Bundle POST latency",
                    buckets=(0.05, 0.1, 0.25, 0.5, 1, 2.5, 5, 10, 30, 60))


# --------------------------------------------------------------------------
# Generation
# --------------------------------------------------------------------------

def patient_bundle(serial, config):
    rng = random.Random(config["seed"] * 1000003 + serial)
    shape = config["shape"]
    heavy = serial % shape.get("heavyEveryN", 10) == 0
    tier = "heavy" if heavy else "normal"
    n_cond = shape["conditionsPerPatient"][tier]
    n_obs = shape["observationsPerPatient"][tier]

    conditions = config["conditionCodes"]
    observations = config["observationCodes"]
    if n_cond and not conditions:
        raise ValueError("conditionCodes is empty but conditionsPerPatient is %d" % n_cond)
    if n_obs and not observations:
        raise ValueError("observationCodes is empty but observationsPerPatient is %d" % n_obs)
    cond_weights = [c["weight"] for c in conditions]
    obs_weights = [o["weight"] for o in observations]

    pid = "%s-p%07d" % (config["prefix"], serial)
    entries = [_entry({
        "resourceType": "Patient",
        "id": pid,
        "meta": _meta(config),
        "identifier": [{"system": "http://perf.fhir/mrn", "value": str(serial)}],
        "gender": "female" if rng.random() < 0.5 else "male",
        "birthDate": str(BIRTH_ZERO + datetime.timedelta(days=rng.randint(0, BIRTH_SPAN))),
        "name": [{"family": "Surname%07d" % serial}],
    }, "Patient", pid)]

    for i, concept in enumerate(rng.choices(conditions, weights=cond_weights, k=n_cond)):
        rid = "%s-c%07d-%03d" % (config["prefix"], serial, i)
        entries.append(_entry({
            "resourceType": "Condition",
            "id": rid,
            "meta": _meta(config),
            "subject": {"reference": "Patient/" + pid},
            "clinicalStatus": {"coding": [{
                "system": "http://terminology.hl7.org/CodeSystem/condition-clinical",
                "code": "active"}]},
            "verificationStatus": {"coding": [{
                "system": "http://terminology.hl7.org/CodeSystem/condition-ver-status",
                "code": "confirmed"}]},
            "code": {"coding": [{"system": SNOMED, "code": concept["code"],
                                 "display": concept["display"]}]},
            "onsetDateTime": _a_date(rng),
        }, "Condition", rid))

    for i, concept in enumerate(rng.choices(observations, weights=obs_weights, k=n_obs)):
        rid = "%s-o%07d-%03d" % (config["prefix"], serial, i)
        entries.append(_entry({
            "resourceType": "Observation",
            "id": rid,
            "meta": _meta(config),
            "status": "final",
            "subject": {"reference": "Patient/" + pid},
            "code": {"coding": [{"system": SNOMED, "code": concept["code"],
                                 "display": concept["display"]}]},
            "valueQuantity": {"value": round(rng.uniform(0.5, 250.0), 2), "unit": "mg/dL",
                              "system": "http://unitsofmeasure.org", "code": "mg/dL"},
            "effectiveDateTime": _a_date(rng),
        }, "Observation", rid))

    return {"resourceType": "Bundle", "type": "transaction", "entry": entries}


def _meta(config):
    return {"tag": [{"system": TAG_SYSTEM, "code": config["datasetName"]}]}


def _entry(resource, rtype, rid):
    return {"resource": resource, "request": {"method": "PUT", "url": "%s/%s" % (rtype, rid)}}


def _a_date(rng):
    return str(DAY_ZERO + datetime.timedelta(days=rng.randint(0, DAY_SPAN)))


# --------------------------------------------------------------------------
# Failures: explained on stdout, always
# --------------------------------------------------------------------------
#
# `kubectl logs` and the operator UI's tail both see a pod's stdout and
# nothing else, so a failure recorded only under RESULTS_DIR is a failure
# nobody reads -- the counter in the progress line goes up and the reason
# stays on a PVC. Everything below exists to make that counter answerable:
# what broke, and what the server said about it.
#
# Identical failures are grouped rather than dropped. A signature's first
# occurrence is printed in full, every FAILURE_DETAIL_EVERY-th occurrence is
# printed in full again (the diagnostics change as a server degrades), every
# occurrence is counted and written to the results log, and the counts are
# rolled up into the progress line and the final summary. Nothing is
# discarded on any path.

FAILURE_DETAIL_EVERY = int(os.environ.get("FAILURE_DETAIL_EVERY", "50"))
FAILURE_BODY_CHARS = int(os.environ.get("FAILURE_BODY_CHARS", "1500"))
# The progress line stays one line. The full text of each signature is in
# the failure's own FAILURE block and in the final summary.
ROLLUP_SIGNATURE_CHARS = 90
_NUMBERS = re.compile(r"\d+")


def _issue_lines(payload):
    """One readable line per OperationOutcome issue."""
    lines = []
    if not isinstance(payload, dict):
        return lines
    for issue in payload.get("issue") or []:
        where = issue.get("expression") or issue.get("location") or []
        details = issue.get("details") or {}
        text = (issue.get("diagnostics") or details.get("text")
                or (json.dumps(details) if details else "") or "(no diagnostics)")
        lines.append("%s/%s: %s%s" % (
            issue.get("severity", "?"), issue.get("code", "?"), text,
            (" [at %s]" % ", ".join(str(w) for w in where)) if where else ""))
    return lines


def _entry_problems(payload):
    """Entries the server rejected inside an otherwise-2xx response.

    A transaction is atomic, so this should stay empty -- but a batch-shaped
    answer or a partially applied transaction would otherwise be counted as a
    clean success on the strength of the outer status code alone.
    """
    lines = []
    if not isinstance(payload, dict) or payload.get("resourceType") != "Bundle":
        return lines
    for i, entry in enumerate(payload.get("entry") or []):
        outcome = (entry or {}).get("response") or {}
        status = str(outcome.get("status", ""))
        if status[:1] in ("4", "5"):
            detail = "; ".join(_issue_lines(outcome.get("outcome") or {}))
            lines.append("entry[%d] rejected: %s %s" % (i, status, detail))
    return lines


def explain(response):
    """Judge one bundle POST. Returns (ok, signature, detail).

    signature is the normalised form used to group like failures; detail is
    the whole story, including the raw body when the server sent something
    that is not an OperationOutcome.
    """
    body = response.text or ""
    try:
        payload = response.json() if body.strip() else None
    except ValueError:
        payload = None

    problems = _entry_problems(payload)
    if response.status_code < 300 and not problems:
        return True, "", ""

    outcome = payload if (isinstance(payload, dict)
                          and payload.get("resourceType") == "OperationOutcome") else None
    issues = _issue_lines(outcome)
    parts = ["HTTP %d %s" % (response.status_code, response.reason or "")]
    request_id = response.headers.get("X-Request-Id")
    if request_id:
        parts.append("x-request-id=%s" % request_id)
    parts.extend(issues)
    parts.extend(problems)
    if not issues and not problems:
        parts.append("body: " + (body.strip()[:FAILURE_BODY_CHARS] or "(empty)"))
    headline = (issues or problems or [""])[0]
    signature = ("HTTP %d %s" % (response.status_code,
                                 _NUMBERS.sub("#", headline)[:160])).strip()
    return False, signature, "\n".join(parts)


def explain_exception(exc):
    """(signature, detail) for a request that never produced a response."""
    signature = "%s: %s" % (type(exc).__name__, _NUMBERS.sub("#", str(exc))[:160])
    return signature.strip(), "%s: %s" % (type(exc).__name__, exc)


class Failures:
    """Every failure, counted and explained. Nothing here swallows one."""

    def __init__(self, log, label):
        self.log = log
        self.label = label
        self.counts = {}         # signature -> total seen
        self.first = {}          # signature -> first full detail
        self.pending = {}        # signature -> seen since the last rollup
        self.total = 0

    def record(self, serial, signature, detail, exc=None):
        self.total += 1
        seen = self.counts.get(signature, 0) + 1
        self.counts[signature] = seen
        self.pending[signature] = self.pending.get(signature, 0) + 1
        self.first.setdefault(signature, detail)

        stamp = datetime.datetime.now().isoformat(timespec="seconds")
        self.log.write("%s %s=%s %s\n" % (stamp, self.label, serial,
                                          detail.replace("\n", " | ")))
        if exc is not None and seen == 1:
            self.log.write(traceback.format_exc())
        self.log.flush()

        if seen == 1 or seen % FAILURE_DETAIL_EVERY == 0:
            print("FAILURE worker %d %s=%s (failure %d overall, %d of this kind)\n%s"
                  % (INDEX, self.label, serial, self.total, seen,
                     "\n".join("    " + line for line in detail.splitlines())),
                  flush=True)
            if exc is not None and seen == 1:
                print(traceback.format_exc(), flush=True)

    def rollup(self):
        """Counts accumulated since the last call, for the progress line."""
        if not self.pending:
            return ""
        parts = ["%s x%d" % (sig[:ROLLUP_SIGNATURE_CHARS], n) for sig, n in
                 sorted(self.pending.items(), key=lambda kv: -kv[1])]
        self.pending = {}
        return "  |  failing: " + "; ".join(parts)

    def summary(self):
        """The whole breakdown, printed once at the end."""
        if not self.total:
            return
        print("worker %d: %d failures by kind:" % (INDEX, self.total), flush=True)
        for sig, n in sorted(self.counts.items(), key=lambda kv: -kv[1]):
            print("  x%-7d %s" % (n, sig), flush=True)
            print("\n".join("      " + line
                            for line in self.first[sig].splitlines()), flush=True)


# --------------------------------------------------------------------------
# Census: what is actually there
# --------------------------------------------------------------------------

def count(session, rtype, dataset_name):
    response = session.get(
        "%s/%s" % (BASE_URL, rtype),
        params={"_tag": "%s|%s" % (TAG_SYSTEM, dataset_name), "_summary": "count"},
        headers={"Accept": "application/fhir+json", "Cache-Control": "no-cache"},
        timeout=POST_TIMEOUT)
    response.raise_for_status()
    return response.json().get("total", 0)


def census(session, dataset_name):
    return {rtype: count(session, rtype, dataset_name) for rtype in TYPES_YOUNGEST_FIRST}


def report(observed, extra=None):
    """Patch the FhirDataset status from inside the pod.

    The operator may be running outside the cluster and cannot reach the FHIR
    service, so actual state is reported inward rather than polled outward.
    """
    if INDEX != 0 or not (DATASET_NAME and NAMESPACE):
        return True
    token_path = "/var/run/secrets/kubernetes.io/serviceaccount/token"
    if not os.path.exists(token_path):
        print("FAILURE no service account token at %s; cannot report status"
              % token_path, flush=True)
        return False
    with open(token_path, encoding="utf-8") as handle:
        token = handle.read().strip()
    body = {"status": {"observed": observed, "observedAt": datetime.datetime.now(
        datetime.timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")}}
    body["status"].update(extra or {})
    url = ("https://kubernetes.default.svc/apis/perf.fhir/v1alpha1/namespaces/%s"
           "/fhirdatasets/%s/status" % (NAMESPACE, DATASET_NAME))
    try:
        response = requests.patch(
            url, json=body,
            headers={"Authorization": "Bearer " + token,
                     "Content-Type": "application/merge-patch+json"},
            verify="/var/run/secrets/kubernetes.io/serviceaccount/ca.crt", timeout=60)
    except Exception as exc:                               # noqa: BLE001 - re-raised below
        print("FAILURE status report to %s raised %s: %s"
              % (url, type(exc).__name__, exc), flush=True)
        print(traceback.format_exc(), flush=True)
        return False
    if response.status_code >= 300:
        print("FAILURE status report rejected: HTTP %d %s\n    body: %s"
              % (response.status_code, response.reason or "",
                 (response.text or "").strip()[:FAILURE_BODY_CHARS] or "(empty)"),
              flush=True)
        return False
    print("status report: HTTP %d %s" % (response.status_code, observed), flush=True)
    return True


# --------------------------------------------------------------------------
# Modes
# --------------------------------------------------------------------------

def run_load(session, config):
    os.makedirs(RESULTS_DIR, exist_ok=True)
    log_path = os.path.join(RESULTS_DIR, "load-%d.log" % INDEX)
    csv_path = os.path.join(RESULTS_DIR, "load-%d.csv" % INDEX)
    serials = range(config["first"] + INDEX, config["first"] + config["count"], PARALLELISM)
    headers = {"Content-Type": "application/fhir+json", "Prefer": "return=minimal"}
    started = time.time()
    done = failed = 0

    with open(log_path, "a", encoding="utf-8") as log, \
            open(csv_path, "w", encoding="utf-8", newline="") as raw:
        out = csv.writer(raw)
        out.writerow(["serial", "entries", "status", "elapsed_ms", "signature"])
        failures = Failures(log, "serial")
        for serial in serials:
            bundle = patient_bundle(serial, config)
            entries = len(bundle["entry"])
            begin = time.time()
            signature = ""
            try:
                response = session.post(BASE_URL, json=bundle, headers=headers,
                                        timeout=POST_TIMEOUT)
                elapsed = time.time() - begin
                status = response.status_code
                ok, signature, detail = explain(response)
                if not ok:
                    failures.record(serial, signature, detail)
            except Exception as exc:                # noqa: BLE001 - explained, not swallowed
                elapsed = time.time() - begin
                status, ok = 0, False
                signature, detail = explain_exception(exc)
                failures.record(serial, signature, detail, exc)
            LATENCY.observe(elapsed)
            out.writerow([serial, entries, status, round(elapsed * 1000), signature])
            if ok:
                BUNDLES.labels(status="ok").inc()
                RESOURCES.inc(entries)
                PATIENTS.inc()
                done += 1
            else:
                BUNDLES.labels(status="failed").inc()
                failed += 1
            if (done + failed) % 100 == 0:
                raw.flush()
                print("worker %d: %d done, %d failed, %.0f res/s%s" % (
                    INDEX, done, failed,
                    RESOURCES._value.get() / max(time.time() - started, 0.001),
                    failures.rollup()), flush=True)

        print("worker %d load finished: %d done, %d failed in %.0fs"
              % (INDEX, done, failed, time.time() - started), flush=True)
        failures.summary()
    return failed


def run_delete(session, config):
    """Delete every resource carrying this dataset's tag, children first.

    _expunge=true submits a Batch2 DELETE_EXPUNGE job and returns immediately,
    so this polls the count down rather than waiting on the response. The
    $delete-expunge operation itself is not registered in the hapiproject
    image; the _expunge query parameter reaches the same job.
    """
    dataset_name = config["datasetName"]
    if INDEX != 0:
        print("worker %d: delete is single-worker, exiting" % INDEX, flush=True)
        return 0

    os.makedirs(RESULTS_DIR, exist_ok=True)
    log_path = os.path.join(RESULTS_DIR, "delete.log")
    deadline = time.time() + int(os.environ.get("DELETE_TIMEOUT", "7200"))

    with open(log_path, "a", encoding="utf-8") as log:
        for rtype in TYPES_YOUNGEST_FIRST:
            stalled = 0
            previous = None
            while time.time() < deadline:
                remaining = count(session, rtype, dataset_name)
                if remaining == 0:
                    log.write("%s: clear\n" % rtype)
                    log.flush()
                    print("delete %s: clear" % rtype, flush=True)
                    break
                if previous is not None and remaining >= previous:
                    stalled += 1
                else:
                    stalled = 0
                    if previous is not None:
                        DELETED.labels(type=rtype).inc(previous - remaining)
                previous = remaining

                # Resubmit on first pass and whenever progress stops.
                if stalled == 0 or stalled >= 6:
                    response = session.delete(
                        "%s/%s" % (BASE_URL, rtype),
                        params={"_tag": "%s|%s" % (TAG_SYSTEM, dataset_name),
                                "_expunge": "true"},
                        headers={"Accept": "application/fhir+json"}, timeout=POST_TIMEOUT)
                    log.write("%s: %d remaining, submit HTTP %d %s\n" % (
                        rtype, remaining, response.status_code,
                        (response.text or "")[:FAILURE_BODY_CHARS]))
                    log.flush()
                    if response.status_code >= 300:
                        _, _, detail = explain(response)
                        print("FAILURE delete of %s rejected (%d remaining)\n%s"
                              % (rtype, remaining,
                                 "\n".join("    " + line for line in detail.splitlines())),
                              flush=True)
                        return 1
                    stalled = 0
                print("delete %s: %d remaining" % (rtype, remaining), flush=True)
                time.sleep(10)
            else:
                print("FAILURE delete of %s timed out with %s still tagged"
                      % (rtype, previous), flush=True)
                return 1

    print("delete finished", flush=True)
    return 0


def wait_for_server(session, seconds=600):
    """The stack may still be starting. Connection refused is not a failure."""
    deadline = time.time() + seconds
    why = "not attempted"
    while time.time() < deadline:
        try:
            response = session.get(BASE_URL + "/metadata",
                                   headers={"Accept": "application/fhir+json"}, timeout=30)
            if response.status_code < 500:
                return True
            why = "HTTP %d %s: %s" % (response.status_code, response.reason or "",
                                      (response.text or "").strip()[:300] or "(empty body)")
        except Exception as exc:                  # noqa: BLE001 - reported, then retried
            why = "%s: %s" % (type(exc).__name__, exc)
        print("waiting for %s -- %s" % (BASE_URL, why), flush=True)
        time.sleep(5)
    print("FAILURE %s never became reachable in %ds; last attempt: %s"
          % (BASE_URL, seconds, why), flush=True)
    return False


def main():
    with open(CONFIG_PATH, encoding="utf-8") as handle:
        config = json.load(handle)
    start_http_server(METRICS_PORT)
    session = requests.Session()
    if not wait_for_server(session):
        return 1

    if MODE == "load":
        failed = run_load(session, config)
    elif MODE == "delete":
        failed = run_delete(session, config)
    elif MODE == "census":
        failed = 0
    else:
        print("unknown MODE %r" % MODE, flush=True)
        return 2

    # A census that cannot be taken or cannot be reported means the operator
    # is flying blind about what this job actually wrote, so it counts as a
    # failure of the job rather than a best-effort extra.
    try:
        observed = census(session, config["datasetName"])
        print("census: %s" % observed, flush=True)
        if not report(observed, {"phase": None} if MODE == "census" else None):
            failed = failed or 1
    except Exception as exc:                    # noqa: BLE001 - explained, not swallowed
        print("FAILURE census of %s failed: %s: %s"
              % (config["datasetName"], type(exc).__name__, exc), flush=True)
        print(traceback.format_exc(), flush=True)
        failed = failed or 1

    time.sleep(LINGER)
    if failed:
        print("worker %d exiting non-zero: %d failure(s), see FAILURE lines above"
              % (INDEX, failed), flush=True)
    return 1 if failed else 0


if __name__ == "__main__":
    sys.exit(main())

# Created by claude-opus-5
"""Synthetic FHIR loader. One complete patient per transaction bundle.

Runs as an indexed Job. Each worker takes every Nth patient serial, so shards
never overlap and no coordination is needed. Everything about a patient derives
from its serial, so the same seed reproduces the same dataset exactly and any
worker can generate any patient independently.

Entries are PUT, not POST. A re-run of a finished range is a no-op and an
interrupted load can simply be restarted.

Config arrives as JSON at $DATASET_CONFIG (a mounted ConfigMap), with SNOMED
codes already resolved -- the ontology service is deliberately not in the hot
path, or it would be the bottleneck being measured.
"""

import csv
import datetime
import json
import os
import random
import sys
import time

import requests
from prometheus_client import Counter, Histogram, start_http_server

BASE_URL = os.environ["FHIR_BASE_URL"]
CONFIG_PATH = os.environ.get("DATASET_CONFIG", "/config/dataset.json")
RESULTS_DIR = os.environ.get("RESULTS_DIR", "/results")
INDEX = int(os.environ.get("JOB_COMPLETION_INDEX", "0"))
PARALLELISM = int(os.environ.get("PARALLELISM", "1"))
METRICS_PORT = int(os.environ.get("METRICS_PORT", "9100"))
POST_TIMEOUT = int(os.environ.get("POST_TIMEOUT", "300"))

DAY_ZERO = datetime.date(2015, 1, 1)
DAY_SPAN = 3652                       # 2015-01-01 .. 2024-12-31
BIRTH_ZERO = datetime.date(1930, 1, 1)
BIRTH_SPAN = 29584                    # 1930-01-01 .. 2010-12-31
SNOMED = "http://snomed.info/sct"

BUNDLES = Counter("fhir_load_bundles_total", "Transaction bundles posted", ["status"])
RESOURCES = Counter("fhir_load_resources_total", "Resources written")
PATIENTS = Counter("fhir_load_patients_total", "Patients written")
LATENCY = Histogram("fhir_load_bundle_seconds", "Bundle POST latency",
                    buckets=(0.05, 0.1, 0.25, 0.5, 1, 2.5, 5, 10, 30, 60))


# --------------------------------------------------------------------------
# Generation
# --------------------------------------------------------------------------

def patient_bundle(serial, config):
    """One complete patient as a transaction Bundle.

    Patient plus Conditions plus Observations, all referencing the same
    subject. Shape and code mix come from the resolved config.
    """
    rng = random.Random(config["seed"] * 1000003 + serial)
    shape = config["shape"]
    heavy = serial % shape.get("heavyEveryN", 10) == 0
    tier = "heavy" if heavy else "normal"
    n_cond = shape["conditionsPerPatient"][tier]
    n_obs = shape["observationsPerPatient"][tier]

    conditions = config["conditionCodes"]
    observations = config["observationCodes"]
    cond_weights = [c["weight"] for c in conditions]
    obs_weights = [o["weight"] for o in observations]
    # random.choices raises IndexError on an empty population, from inside the
    # stdlib, which says nothing about the config that caused it.
    if n_cond and not conditions:
        raise ValueError("conditionCodes is empty but conditionsPerPatient is %d" % n_cond)
    if n_obs and not observations:
        raise ValueError("observationCodes is empty but observationsPerPatient is %d" % n_obs)

    pid = "perf-p%07d" % serial
    entries = [_entry({
        "resourceType": "Patient",
        "id": pid,
        "identifier": [{"system": "http://perf.pkb/mrn", "value": str(serial)}],
        "gender": "female" if rng.random() < 0.5 else "male",
        "birthDate": str(BIRTH_ZERO + datetime.timedelta(days=rng.randint(0, BIRTH_SPAN))),
        "name": [{"family": "Surname%07d" % serial}],
    }, "Patient", pid)]

    for i, concept in enumerate(rng.choices(conditions, weights=cond_weights, k=n_cond)):
        rid = "perf-c%07d-%03d" % (serial, i)
        entries.append(_entry({
            "resourceType": "Condition",
            "id": rid,
            "subject": {"reference": "Patient/" + pid},
            # Required-strength binding, and R4 forbids clinicalStatus only when
            # verificationStatus is entered-in-error, which we never emit.
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
        rid = "perf-o%07d-%03d" % (serial, i)
        entries.append(_entry({
            "resourceType": "Observation",
            "id": rid,
            "status": "final",
            "subject": {"reference": "Patient/" + pid},
            "code": {"coding": [{"system": SNOMED, "code": concept["code"],
                                 "display": concept["display"]}]},
            "valueQuantity": {"value": round(rng.uniform(0.5, 250.0), 2), "unit": "mg/dL",
                              "system": "http://unitsofmeasure.org", "code": "mg/dL"},
            "effectiveDateTime": _a_date(rng),
        }, "Observation", rid))

    return {"resourceType": "Bundle", "type": "transaction", "entry": entries}


def _entry(resource, rtype, rid):
    return {"resource": resource, "request": {"method": "PUT", "url": "%s/%s" % (rtype, rid)}}


def _a_date(rng):
    return str(DAY_ZERO + datetime.timedelta(days=rng.randint(0, DAY_SPAN)))


# --------------------------------------------------------------------------

def main():
    with open(CONFIG_PATH, encoding="utf-8") as handle:
        config = json.load(handle)

    start_http_server(METRICS_PORT)
    os.makedirs(RESULTS_DIR, exist_ok=True)
    log_path = os.path.join(RESULTS_DIR, "worker-%d.log" % INDEX)
    csv_path = os.path.join(RESULTS_DIR, "worker-%d.csv" % INDEX)

    first = config["first"]
    count = config["count"]
    # Every Nth serial. Shards never overlap, so no coordination.
    serials = range(first + INDEX, first + count, PARALLELISM)

    session = requests.Session()
    headers = {"Content-Type": "application/fhir+json", "Prefer": "return=minimal"}
    started = time.time()
    done = failed = 0

    with open(log_path, "a", encoding="utf-8") as log, \
            open(csv_path, "w", encoding="utf-8", newline="") as raw:
        out = csv.writer(raw)
        out.writerow(["serial", "entries", "status", "elapsed_ms"])

        for serial in serials:
            bundle = patient_bundle(serial, config)
            entries = len(bundle["entry"])
            begin = time.time()
            try:
                response = session.post(BASE_URL, json=bundle, headers=headers,
                                        timeout=POST_TIMEOUT)
                elapsed = time.time() - begin
                status = response.status_code
                ok = status < 300
                if not ok:
                    log.write("%s serial=%d HTTP %d %s\n" % (
                        datetime.datetime.now().isoformat(), serial, status,
                        response.text[:400]))
            except Exception as exc:                       # noqa: BLE001 - recorded and counted
                elapsed = time.time() - begin
                status, ok = 0, False
                log.write("%s serial=%d %s: %s\n" % (
                    datetime.datetime.now().isoformat(), serial, type(exc).__name__, exc))

            LATENCY.observe(elapsed)
            out.writerow([serial, entries, status, round(elapsed * 1000)])
            if ok:
                BUNDLES.labels(status="ok").inc()
                RESOURCES.inc(entries)
                PATIENTS.inc()
                done += 1
            else:
                BUNDLES.labels(status="failed").inc()
                failed += 1
                log.flush()

            if (done + failed) % 100 == 0:
                raw.flush()
                rate = RESOURCES._value.get() / max(time.time() - started, 0.001)
                print("worker %d: %d done, %d failed, %.0f resources/s"
                      % (INDEX, done, failed, rate), flush=True)

    print("worker %d finished: %d done, %d failed in %.0fs"
          % (INDEX, done, failed, time.time() - started), flush=True)
    # Let Prometheus scrape the final counters before the pod disappears.
    time.sleep(int(os.environ.get("LINGER_SECONDS", "20")))
    return 1 if failed else 0


if __name__ == "__main__":
    sys.exit(main())

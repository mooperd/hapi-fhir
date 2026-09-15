# Created by claude-opus-5
"""End-to-end check that a dataset can be selected by name against a real server.

Generates bundles with loader.patient_bundle, PUTs them, then counts and
deletes purely by meta.tag http://perf.fhir/dataset|<name>. Uses a throwaway
dataset name and expunges it again, so it is safe to run beside real data.

    FHIR_BASE_URL=http://localhost:8083/fhir .venv/bin/python test_tagging_live.py

Counts are read with Cache-Control: no-cache -- HAPI serves _summary=count
from the search cache otherwise, and a stale count reads as a failure here.
Delete-expunge is asynchronous, so the teardown polls rather than asserting
immediately.
"""

import os
import sys
import time

import requests

HERE = os.path.dirname(os.path.abspath(__file__))
os.environ.setdefault("FHIR_BASE_URL", "http://localhost:8083/fhir")
sys.path.insert(0, HERE)
import loader  # noqa: E402 - needs the env above

BASE = os.environ["FHIR_BASE_URL"]
NAME = os.environ.get("LIVE_TEST_DATASET", "livetag-tmp")
PATIENTS = 3
FAILS = []


def check(label, cond, detail=""):
    print(("PASS " if cond else "FAIL ") + label + ("" if cond else "  <- " + str(detail)))
    if not cond:
        FAILS.append(label)


def count(session, rtype, name):
    """Same query the loader issues, with the cache defeated."""
    response = session.get(
        "%s/%s" % (BASE, rtype),
        params={"_tag": "%s|%s" % (loader.TAG_SYSTEM, name), "_summary": "count"},
        headers={"Accept": "application/fhir+json", "Cache-Control": "no-cache"},
        timeout=60)
    response.raise_for_status()
    return response.json().get("total", 0)


CONFIG = {
    "datasetName": NAME,
    "prefix": NAME,
    "seed": 7,
    "first": 1,
    "count": PATIENTS,
    "shape": {"heavyEveryN": 100,
              "conditionsPerPatient": {"normal": 2, "heavy": 2},
              "observationsPerPatient": {"normal": 2, "heavy": 2}},
    "conditionCodes": [{"code": "73211009", "display": "Diabetes mellitus", "weight": 1}],
    "observationCodes": [{"code": "271649006", "display": "Systolic blood pressure",
                          "weight": 1}],
}

session = requests.Session()
if not loader.wait_for_server(session, 60):
    print("no server at %s; is port-forward.sh running?" % BASE)
    sys.exit(2)

before = {t: count(session, t, NAME) for t in loader.TYPES_YOUNGEST_FIRST}
check("dataset name is unused before the test", sum(before.values()) == 0, before)

# ---- load ---------------------------------------------------------------

for serial in range(1, PATIENTS + 1):
    bundle = loader.patient_bundle(serial, CONFIG)
    response = session.post(BASE, json=bundle,
                            headers={"Accept": "application/fhir+json"}, timeout=120)
    response.raise_for_status()

observed = {t: count(session, t, NAME) for t in loader.TYPES_YOUNGEST_FIRST}
check("Patients found by name", observed["Patient"] == PATIENTS, observed)
check("Conditions found by name", observed["Condition"] == PATIENTS * 2, observed)
check("Observations found by name", observed["Observation"] == PATIENTS * 2, observed)

# census() is what reports status back to the operator
check("census() agrees", loader.census(session, NAME) == observed, observed)

# ---- the name really is the discriminator -------------------------------

check("a different name matches nothing", count(session, "Patient", NAME + "-nope") == 0)

stored = session.get("%s/Patient/%s-p0000001" % (BASE, NAME),
                     headers={"Accept": "application/fhir+json"}, timeout=60).json()
tags = stored.get("meta", {}).get("tag", [])
ours = [t for t in tags if t.get("system") == loader.TAG_SYSTEM]
check("exactly one dataset tag is stored", len(ours) == 1, tags)
check("the stored tag is the name", ours and ours[0]["code"] == NAME, ours)
check("no uid on the stored resource",
      not any(len(t.get("code", "")) == 36 and t["code"].count("-") == 4 for t in ours), ours)

# ---- teardown: delete by the same name ----------------------------------

for rtype in loader.TYPES_YOUNGEST_FIRST:
    session.delete("%s/%s" % (BASE, rtype),
                   params={"_tag": "%s|%s" % (loader.TAG_SYSTEM, NAME), "_expunge": "true"},
                   headers={"Accept": "application/fhir+json"}, timeout=120)

deadline = time.time() + int(os.environ.get("PURGE_TIMEOUT", "900"))
remaining = None
while time.time() < deadline:
    remaining = {t: count(session, t, NAME) for t in loader.TYPES_YOUNGEST_FIRST}
    if sum(remaining.values()) == 0:
        break
    print("  purging, remaining: %s" % remaining)
    time.sleep(20)

check("delete by name removed everything", sum(remaining.values()) == 0, remaining)

print()
print("%d checks failed" % len(FAILS) if FAILS else "all checks passed")
sys.exit(1 if FAILS else 0)

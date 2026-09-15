# Created by claude-opus-5
"""Standalone checks for dataset tagging in loader.py.

No pytest in the operator venv, so this is a plain script:

    .venv/bin/python test_loader.py

Nothing here touches a cluster or a FHIR server. The live end-to-end check
lives in test_tagging_live.py.
"""

import json
import os
import re

import sys

HERE = os.path.dirname(os.path.abspath(__file__))
ROOT = os.path.dirname(HERE)

# loader.py reads these at import time.
os.environ.setdefault("FHIR_BASE_URL", "http://localhost:8083/fhir")
os.environ.setdefault("DATASET_NAME", "unit-test")
sys.path.insert(0, HERE)
import loader  # noqa: E402 - needs the env above

FAILS = []


def check(label, cond, detail=""):
    print(("PASS " if cond else "FAIL ") + label + ("" if cond else "  <- " + str(detail)))
    if not cond:
        FAILS.append(label)


NAME = "meow1"
UID = "f33200d9-63a8-4785-8c68-3844073f7c09"
CONFIG = {
    "datasetName": NAME,
    "prefix": NAME,
    "seed": 1,
    "first": 1,
    "count": 10,
    "shape": {"heavyEveryN": 10,
              "conditionsPerPatient": {"normal": 2, "heavy": 3},
              "observationsPerPatient": {"normal": 2, "heavy": 3}},
    "conditionCodes": [{"code": "73211009", "display": "Diabetes", "weight": 1}],
    "observationCodes": [{"code": "271649006", "display": "Systolic BP", "weight": 1}],
}

# ---- the tag carries the dataset name, and nothing else ----------------

meta = loader._meta(CONFIG)
tags = meta["tag"]
check("exactly one tag", len(tags) == 1, tags)
check("tag code is the dataset name", tags[0]["code"] == NAME, tags[0])
check("tag system is free of pkb", "pkb" not in tags[0]["system"], tags[0]["system"])
check("no uid anywhere in the tag", UID not in json.dumps(meta), meta)

# ---- every generated resource carries it -------------------------------

bundle = loader.patient_bundle(1, CONFIG)
kinds = [e["resource"]["resourceType"] for e in bundle["entry"]]
check("bundle holds all three types",
      set(kinds) == {"Patient", "Condition", "Observation"}, sorted(set(kinds)))
check("every resource tagged with the name",
      all(e["resource"]["meta"]["tag"][0]["code"] == NAME for e in bundle["entry"]),
      [e["resource"]["meta"]["tag"] for e in bundle["entry"][:2]])

# ---- id construction is unchanged --------------------------------------

pat = [e["resource"] for e in bundle["entry"] if e["resource"]["resourceType"] == "Patient"][0]
check("patient id still prefix-p0000001", pat["id"] == "meow1-p0000001", pat["id"])
check("entries are still PUT",
      all(e["request"]["method"] == "PUT" for e in bundle["entry"]))

# ---- the search handle is the name -------------------------------------


class FakeSession:
    def __init__(self):
        self.params = None

    def get(self, url, params=None, headers=None, timeout=None):
        self.params = params

        class R:
            status_code = 200

            @staticmethod
            def raise_for_status():
                pass

            @staticmethod
            def json():
                return {"total": 7}
        return R()


sess = FakeSession()
total = loader.count(sess, "Patient", NAME)
check("count() returns the total", total == 7, total)
check("count() queries _tag by name",
      sess.params["_tag"] == "%s|%s" % (loader.TAG_SYSTEM, NAME), sess.params)
check("count() sends _summary=count", sess.params.get("_summary") == "count", sess.params)

# ---- failures are explained, never swallowed ---------------------------
#
# The counter in the progress line ("N failed") used to be the only thing on
# stdout; the reason lived in a file on a PVC. These checks pin the contract
# that every failure reaches stdout with the server's own words attached.

import contextlib  # noqa: E402
import io as _io  # noqa: E402


class FakeResponse:
    def __init__(self, status, payload=None, text=None, reason="", headers=None):
        self.status_code = status
        self.reason = reason
        self.headers = headers or {}
        self._payload = payload
        self.text = text if text is not None else (json.dumps(payload) if payload else "")

    def json(self):
        if self._payload is None:
            raise ValueError("no json")
        return self._payload


OUTCOME = {"resourceType": "OperationOutcome", "issue": [
    {"severity": "error", "code": "processing",
     "diagnostics": "HAPI-0389: Failed to call access method: "
                    "could not execute statement [ERROR: deadlock detected]",
     "location": ["Bundle.entry[3]"]}]}

ok, sig, detail = loader.explain(FakeResponse(200, {"resourceType": "Bundle", "entry": [
    {"response": {"status": "201 Created"}}]}))
check("clean transaction is ok", ok and not detail, (ok, detail))

ok, sig, detail = loader.explain(
    FakeResponse(500, OUTCOME, reason="Server Error", headers={"X-Request-Id": "abc"}))
check("500 is a failure", not ok)
check("detail carries the diagnostics", "deadlock detected" in detail, detail)
check("detail carries the status line", "HTTP 500 Server Error" in detail, detail)
check("detail carries the request id", "x-request-id=abc" in detail, detail)
check("detail carries the location", "Bundle.entry[3]" in detail, detail)
check("signature groups by outcome", sig.startswith("HTTP 500 error/processing"), sig)

# An unparseable or empty body must still produce something readable.
ok, sig, detail = loader.explain(FakeResponse(502, None, text="<html>bad gateway</html>"))
check("non-JSON body is still explained", not ok and "bad gateway" in detail, detail)
ok, sig, detail = loader.explain(FakeResponse(413, None, text=""))
check("empty body is still explained", not ok and "(empty)" in detail, detail)

# A 2xx hiding rejected entries is a failure, not a success.
partial = {"resourceType": "Bundle", "entry": [
    {"response": {"status": "201 Created"}},
    {"response": {"status": "422 Unprocessable", "outcome": {
        "resourceType": "OperationOutcome", "issue": [
            {"severity": "error", "code": "invalid", "diagnostics": "bad code"}]}}}]}
ok, sig, detail = loader.explain(FakeResponse(200, partial))
check("2xx with a rejected entry is a failure", not ok, detail)
check("rejected entry is named", "entry[1]" in detail and "bad code" in detail, detail)

# Signatures normalise digits so per-serial ids do not fragment the grouping.
a = loader.explain(FakeResponse(409, {"resourceType": "OperationOutcome", "issue": [
    {"severity": "error", "code": "conflict", "diagnostics": "id meow0-p0000042 in use"}]}))[1]
b = loader.explain(FakeResponse(409, {"resourceType": "OperationOutcome", "issue": [
    {"severity": "error", "code": "conflict", "diagnostics": "id meow0-p0009999 in use"}]}))[1]
check("like failures share a signature", a == b, (a, b))

# ---- Failures: counts are exact and stdout always gets the reason ------

buffer = _io.StringIO()
out = _io.StringIO()
failures = loader.Failures(buffer, "serial")
with contextlib.redirect_stdout(out):
    for i in range(loader.FAILURE_DETAIL_EVERY * 2 + 3):
        failures.record(i, "HTTP 500 error/processing: deadlock", "HTTP 500\ndeadlock detected")
    failures.record(9999, "ConnectionError: broken pipe", "ConnectionError: broken pipe")
    rolled = failures.rollup()
    failures.summary()
printed = out.getvalue()

check("every failure is counted", failures.total == loader.FAILURE_DETAIL_EVERY * 2 + 4,
      failures.total)
check("every failure is written to the results log",
      buffer.getvalue().count("deadlock detected") == loader.FAILURE_DETAIL_EVERY * 2 + 3,
      buffer.getvalue().count("deadlock detected"))
check("first of a kind is printed in full", "deadlock detected" in printed)
check("repeats are re-printed periodically, not every time",
      2 < printed.count("FAILURE worker") < 10, printed.count("FAILURE worker"))
check("a new kind is printed immediately", "broken pipe" in printed)
check("rollup names each kind with its count",
      "x%d" % (loader.FAILURE_DETAIL_EVERY * 2 + 3) in rolled and "broken pipe x1" in rolled,
      rolled)
check("rollup resets between progress lines", failures.rollup() == "")
check("summary breaks down every kind",
      "deadlock" in printed.split("failures by kind:")[1]
      and "broken pipe" in printed.split("failures by kind:")[1])

# ---- no pkb left in the codebase ---------------------------------------

# This file is excluded: it has to name the string in order to look for it.
SKIP_DIRS = {".venv", "__pycache__", ".git", "node_modules"}
SKIP_FILES = {"test_loader.py"}
hits = []
for base, dirs, files in os.walk(ROOT):
    dirs[:] = [d for d in dirs if d not in SKIP_DIRS]
    for fname in files:
        if fname in SKIP_FILES or fname.endswith((".png", ".jpg", ".gz", ".pyc")):
            continue
        path = os.path.join(base, fname)
        try:
            with open(path, encoding="utf-8", errors="ignore") as handle:
                if re.search("pkb", handle.read(), re.IGNORECASE):
                    hits.append(os.path.relpath(path, ROOT))
        except OSError:
            continue
check("no file mentions pkb", not hits, hits)

print()
print("%d checks failed" % len(FAILS) if FAILS else "all checks passed")
sys.exit(1 if FAILS else 0)

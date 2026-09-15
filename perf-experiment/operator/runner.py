# Created by claude-opus-5
"""Benchmark worker. Four step types: assert, analyze, prewarm, measure.

Runs as an Indexed Job. Shard n takes cases[n::parallelism], so every case is
measured by exactly one worker and no two workers race on the same query.

The contract from FhirBenchmarkSOW.md section 1 holds everywhere in this file:
every condition that is not exactly what was declared is a hard, named
failure. Nothing here is defaulted, substituted, skipped, retried into
success, or best-efforted. A query that times out is recorded as TIMEOUT and
is never re-issued; a non-2xx is recorded as ERROR with its OperationOutcome
issue codes and is never excluded from the report.

Output, per shard:

    /results/<name>/<runId>/<stepIdx>/shard-<n>.jsonl   every request record
    ConfigMap bm-<name>-<runId>-<stepIdx>-s<n>          the same, aggregated

The ConfigMap exists because the results PVC is ReadWriteOnce and the
operator may be running on a laptop with no route into the cluster. It carries
the raw latency samples, not bucketed ones: percentiles are computed from raw
records, never from Prometheus histograms, which are lossy.
"""

import datetime
import json
import os
import re
import sys
import time
import traceback

import psycopg
import requests
from prometheus_client import Histogram, start_http_server

CONFIG_PATH = os.environ.get("BENCHMARK_CONFIG", "/config/benchmark.json")
RESULTS_DIR = os.environ.get("RESULTS_DIR", "/results")
BASE_URL = os.environ["FHIR_BASE_URL"]
INDEX = int(os.environ.get("JOB_COMPLETION_INDEX", "0"))
PARALLELISM = int(os.environ.get("PARALLELISM", "1"))
METRICS_PORT = int(os.environ.get("METRICS_PORT", "9100"))
NAMESPACE = os.environ.get("POD_NAMESPACE", "")
NODE_NAME = os.environ.get("NODE_NAME", "")
STACK = os.environ.get("STACK", "")

PG_HOST = os.environ.get("PG_HOST", "hapi-fhir-db")
PG_PORT = int(os.environ.get("PG_PORT", "5432"))
PG_DB = os.environ.get("PG_DB", "fhir")
PG_USER = os.environ.get("PG_USER", "fhir")
PG_PASSWORD = os.environ.get("POSTGRES_PASSWORD", "")

PLACEHOLDER = re.compile(r"\{\{\s*([A-Za-z0-9_]+)\s*\}\}")

# The search-parameter index relations. A statement touching one of these is
# HAPI resolving a search through PostgreSQL; a statement touching only the
# resource and version tables is HAPI fetching bodies for PIDs that something
# else -- Elasticsearch -- produced. That distinction is the engine
# attribution, and it is evidence rather than a guess at the latency.
PG_SEARCH_RELATIONS = ("hfj_spidx", "hfj_res_link", "hfj_res_tag", "trm_concept")
PG_FETCH_RELATIONS = ("hfj_res_ver", "hfj_resource")

# The relations a prewarm or analyze step may name, and what they map to in
# the live schema. Lower-cased because HAPI's DDL is unquoted.
LATENCY = Histogram(
    "fhir_query_seconds", "FHIR query latency",
    ["case", "family", "cache", "engine", "stack"],
    buckets=(0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1, 2.5, 5, 10, 30, 60, 120))


def stamp():
    return datetime.datetime.now(datetime.timezone.utc).isoformat(timespec="milliseconds")


def fail(message):
    """A named failure. Printed, then the pod exits non-zero."""
    print("FAILURE %s" % message, flush=True)
    return 1


# --------------------------------------------------------------------------
# PostgreSQL
# --------------------------------------------------------------------------

def connect():
    if not PG_PASSWORD:
        raise RuntimeError(
            "POSTGRES_PASSWORD is empty; the worker cannot reach PostgreSQL and "
            "therefore cannot attribute an engine or verify a cache state")
    return psycopg.connect(host=PG_HOST, port=PG_PORT, dbname=PG_DB, user=PG_USER,
                           password=PG_PASSWORD, autocommit=True, connect_timeout=30)


def pg_snapshot(conn):
    """{queryid: row} of pg_stat_statements for this database."""
    with conn.cursor() as cur:
        cur.execute("""
            SELECT queryid, query, calls, total_exec_time, shared_blks_hit,
                   shared_blks_read, temp_blks_written
            FROM pg_stat_statements
            WHERE dbid = (SELECT oid FROM pg_database WHERE datname = current_database())
        """)
        return {row[0]: {"query": row[1], "calls": row[2], "total_exec_time": row[3],
                         "shared_blks_hit": row[4], "shared_blks_read": row[5],
                         "temp_blks_written": row[6]}
                for row in cur.fetchall()}


def pg_delta(before, after):
    """What the server did between two snapshots, and what it touched."""
    total = {"calls": 0, "total_exec_time": 0.0, "shared_blks_hit": 0,
             "shared_blks_read": 0, "temp_blks_written": 0}
    touched_search = False
    touched_fetch = False
    top = (0.0, "")
    for queryid, row in after.items():
        old = before.get(queryid)
        calls = row["calls"] - (old["calls"] if old else 0)
        if calls <= 0:
            continue
        elapsed = row["total_exec_time"] - (old["total_exec_time"] if old else 0.0)
        total["calls"] += calls
        total["total_exec_time"] += elapsed
        for key in ("shared_blks_hit", "shared_blks_read", "temp_blks_written"):
            total[key] += row[key] - (old[key] if old else 0)
        text = (row["query"] or "").lower()
        if any(rel in text for rel in PG_SEARCH_RELATIONS):
            touched_search = True
        if any(rel in text for rel in PG_FETCH_RELATIONS):
            touched_fetch = True
        if elapsed > top[0]:
            top = (elapsed, (row["query"] or "")[:400])
    total["touchedSearchIndex"] = touched_search
    total["touchedResourceFetch"] = touched_fetch
    total["topStatement"] = top[1]
    blocks = total["shared_blks_hit"] + total["shared_blks_read"]
    total["readRatio"] = (total["shared_blks_read"] / blocks) if blocks else None
    return total


def attribute(delta):
    """'postgres' | 'elasticsearch' | None. None is unattributable, which is
    INVALID for any case that declared an engine -- never resolved by
    assumption."""
    if delta["touchedSearchIndex"]:
        return "postgres"
    if delta["touchedResourceFetch"]:
        return "elasticsearch"
    return None


# --------------------------------------------------------------------------
# Step: assert
# --------------------------------------------------------------------------

ANALYZE_RELATIONS = ("hfj_resource", "hfj_spidx_token", "hfj_spidx_date",
                     "hfj_spidx_quantity", "hfj_spidx_string", "hfj_res_link")


def step_assert(config, conn):
    """The database-side preconditions. Everything else was checked operator-side."""
    require = config.get("require") or {}
    out = {"checks": {}}

    with conn.cursor() as cur:
        cur.execute("CREATE EXTENSION IF NOT EXISTS pg_stat_statements")
        cur.execute("SELECT count(*) FROM pg_stat_statements")
        out["checks"]["pgStatStatements"] = "%d statements tracked" % cur.fetchone()[0]

    if require.get("analyzedSinceLoad"):
        loaded_at = config.get("datasetLoadedAt")
        if not loaded_at:
            return out, fail(
                "require.analyzedSinceLoad is set but the FhirDataset has no "
                "status.observedAt, so there is no load time to compare against")
        with conn.cursor() as cur:
            cur.execute("""
                SELECT relname, greatest(coalesce(last_analyze, 'epoch'),
                                         coalesce(last_autoanalyze, 'epoch'))
                FROM pg_stat_user_tables WHERE relname = ANY(%s)
            """, (list(ANALYZE_RELATIONS),))
            seen = dict(cur.fetchall())
        cutoff = datetime.datetime.fromisoformat(loaded_at.replace("Z", "+00:00"))
        stale = []
        for relation in ANALYZE_RELATIONS:
            when = seen.get(relation)
            if when is None:
                stale.append("%s: no such relation" % relation)
                continue
            if when.tzinfo is None:
                when = when.replace(tzinfo=datetime.timezone.utc)
            if when <= cutoff:
                stale.append("%s: last analyzed %s, dataset loaded %s"
                             % (relation, when.isoformat(), loaded_at))
            out["checks"]["lastAnalyze/%s" % relation] = when.isoformat()
        if stale:
            return out, fail(
                "ANALYZE has not run since the dataset load completed: %s. A freshly "
                "loaded database makes catastrophically bad plans until it has looked "
                "at its own data" % "; ".join(stale))

    return out, 0


# --------------------------------------------------------------------------
# Steps: analyze, prewarm
# --------------------------------------------------------------------------

def step_analyze(config, conn):
    relations = config.get("relations") or []
    out = {"analyzed": []}
    with conn.cursor() as cur:
        if not relations:
            cur.execute("ANALYZE")
            out["analyzed"].append("(whole schema)")
        else:
            for relation in relations:
                name = relation.lower()
                cur.execute("SELECT to_regclass(%s)", (name,))
                if cur.fetchone()[0] is None:
                    return out, fail(
                        "analyze step names relation %s, which does not exist. A "
                        "renamed table must not be silently skipped" % relation)
                cur.execute('ANALYZE "%s"' % name)
                out["analyzed"].append(name)
    return out, 0


def step_prewarm(config, conn):
    relations = config.get("relations") or []
    if not relations:
        return {}, fail("prewarm step names no relations")
    out = {"prewarmed": {}}
    with conn.cursor() as cur:
        cur.execute("CREATE EXTENSION IF NOT EXISTS pg_prewarm")
        for relation in relations:
            name = relation.lower()
            cur.execute("SELECT to_regclass(%s)", (name,))
            if cur.fetchone()[0] is None:
                return out, fail(
                    "prewarm step names relation %s, which does not exist. A renamed "
                    "table must not be silently skipped" % relation)
            cur.execute("SELECT pg_prewarm(%s)", (name,))
            out["prewarmed"][name] = cur.fetchone()[0]
    return out, 0


# --------------------------------------------------------------------------
# Step: measure
# --------------------------------------------------------------------------

def render(text, bindings):
    missing = [name for name in PLACEHOLDER.findall(text) if name not in bindings]
    if missing:
        raise RuntimeError("unbound placeholder(s) %s" % ", ".join(missing))
    return PLACEHOLDER.sub(lambda m: str(bindings[m.group(1)]), text)


def issue(session, method, path, query, timeout):
    """One request. Returns (wallMs, response|None, exception|None)."""
    url = BASE_URL.rstrip("/") + path
    headers = {"Accept": "application/fhir+json", "Cache-Control": "no-cache"}
    begin = time.perf_counter()
    try:
        if method == "POST":
            headers["Content-Type"] = "application/x-www-form-urlencoded"
            response = session.post(url, data=query, headers=headers, timeout=timeout)
        else:
            response = session.get(url + (("?" + query) if query else ""),
                                   headers=headers, timeout=timeout)
        return (time.perf_counter() - begin) * 1000.0, response, None
    except Exception as exc:                  # noqa: BLE001 - recorded, never swallowed
        return (time.perf_counter() - begin) * 1000.0, None, exc


def issue_codes(payload):
    if not isinstance(payload, dict) or payload.get("resourceType") != "OperationOutcome":
        return []
    return ["%s/%s" % (i.get("severity", "?"), i.get("code", "?"))
            for i in payload.get("issue") or []]


def read_bundle(response):
    """(rowsReturned, bundleTotal, issueCodes)."""
    try:
        payload = response.json()
    except ValueError:
        return None, None, ["unparseable-body"]
    if not isinstance(payload, dict):
        return None, None, ["unparseable-body"]
    if payload.get("resourceType") == "Bundle":
        return len(payload.get("entry") or []), payload.get("total"), []
    if payload.get("resourceType") == "OperationOutcome":
        return None, None, issue_codes(payload)
    return None, None, []


def step_measure(config, conn, records):
    """Every case in this shard, repeated, with its PostgreSQL evidence."""
    cases = config["cases"]
    mine = cases[INDEX::PARALLELISM]
    binding_sets = config["bindingSets"]
    repeats = int(config["repeats"])
    warmup = int(config["warmup"])
    timeout = int(config["timeoutSeconds"])
    cache_label = config.get("cacheLabel") or config["stepId"]
    tolerance_pct = float((config.get("tolerance") or {}).get("rowCountPct", 0))
    attributable = PARALLELISM == 1

    session = requests.Session()
    print("worker %d: %d of %d cases, %d repeats after %d warmup, attribution %s"
          % (INDEX, len(mine), len(cases), repeats, warmup,
             "on" if attributable else "off (concurrency > 1)"), flush=True)

    for case in mine:
        before = pg_snapshot(conn)
        rendered = []
        for rep in range(warmup + repeats):
            bindings = binding_sets[min(rep, len(binding_sets) - 1)]
            method = case["method"]
            path = render(case["path"], bindings)
            query = render(case["query"], bindings)
            wall, response, exc = issue(session, method, path, query, timeout)

            if rep < warmup:
                continue

            record = {
                "caseId": case["id"], "family": case["family"],
                "stepId": config["stepId"], "cacheLabel": cache_label,
                "runId": config["runId"], "shard": INDEX, "rep": rep - warmup,
                "node": NODE_NAME, "startedAt": stamp(),
                "method": method, "url": path + (("?" + query) if query else ""),
                "wallMs": wall, "httpStatus": None,
                "rowsReturned": None, "bundleTotal": None,
                "engineExpected": case.get("expectEngine"), "engineObserved": None,
                "esTookMs": None,
                "valid": False, "invalidReason": None,
            }
            if exc is not None:
                kind = type(exc).__name__
                record["invalidReason"] = (
                    "TIMEOUT after %ds" % timeout
                    if isinstance(exc, requests.Timeout) else "ERROR %s: %s" % (kind, exc))
            else:
                record["httpStatus"] = response.status_code
                rows, total, codes = read_bundle(response)
                record["rowsReturned"] = rows
                record["bundleTotal"] = total
                if response.status_code >= 300:
                    record["invalidReason"] = "ERROR HTTP %d%s" % (
                        response.status_code,
                        (": " + ", ".join(codes)) if codes else "")
                else:
                    record["valid"] = True
            rendered.append(record)
            records.append(record)
            LATENCY.labels(case=case["id"], family=case["family"], cache=cache_label,
                           engine=case.get("expectEngine") or "unknown",
                           stack=STACK).observe(wall / 1000.0)

        delta = pg_delta(before, pg_snapshot(conn))
        observed = attribute(delta) if attributable else None
        _reconcile(case, rendered, delta, observed, attributable, tolerance_pct)
        done = sum(1 for r in rendered if r["valid"])
        print("worker %d: %s %d/%d valid, engine=%s, readRatio=%s"
              % (INDEX, case["id"], done, len(rendered), observed or "-",
                 "-" if delta["readRatio"] is None else "%.3f" % delta["readRatio"]),
              flush=True)

    return {"cases": len(mine), "attributable": attributable}, 0


def _reconcile(case, rendered, delta, observed, attributable, tolerance_pct):
    """Apply the validity rules to a case's records, in place.

    Every rule here turns a not-exactly-as-declared condition into a named
    INVALID. None of them drops a record.
    """
    predict = case.get("predict")
    expected_engine = case.get("expectEngine")

    for record in rendered:
        record["engineObserved"] = observed
        record["pgCalls"] = delta["calls"]
        record["pgTotalExecMs"] = delta["total_exec_time"]
        record["sharedBlksHit"] = delta["shared_blks_hit"]
        record["sharedBlksRead"] = delta["shared_blks_read"]
        record["tempBlksWritten"] = delta["temp_blks_written"]
        record["pgReadRatio"] = delta["readRatio"]
        record["pgTopStatement"] = delta["topStatement"]

        if not record["valid"]:
            continue

        if expected_engine:
            if not attributable:
                record["valid"] = False
                record["invalidReason"] = (
                    "INVALID engine unattributable: case declares expectEngine=%s but "
                    "pg_stat_statements is server-global and this step ran at "
                    "concurrency > 1" % expected_engine)
                continue
            if observed is None:
                record["valid"] = False
                record["invalidReason"] = (
                    "INVALID engine unattributable: no PostgreSQL statement activity "
                    "during the case, so neither engine can be evidenced")
                continue
            if observed != expected_engine:
                record["valid"] = False
                record["invalidReason"] = (
                    "INVALID engine mismatch: declared %s, served by %s"
                    % (expected_engine, observed))
                continue

        if predict:
            count = record["bundleTotal"]
            if count is None:
                record["valid"] = False
                record["invalidReason"] = (
                    "INVALID row count not reconciled: the case predicts ~%.0f rows "
                    "but the response carried no Bundle.total"
                    % predict["predicted"])
                continue
            slack = predict["predicted"] * tolerance_pct / 100.0
            low = predict["low"] - slack
            high = predict["high"] + slack
            if not low <= count <= high:
                record["valid"] = False
                record["invalidReason"] = (
                    "INVALID row count %d outside the predicted band [%.0f, %.0f] "
                    "(expectation %.0f, 4-sigma binomial band, tolerance %.1f%%)"
                    % (count, low, high, predict["predicted"], tolerance_pct))


# --------------------------------------------------------------------------
# Reporting
# --------------------------------------------------------------------------

CONFIGMAP_LIMIT = 900_000


def write_shard(config, records, extra):
    directory = os.path.join(RESULTS_DIR, config["name"], str(config["runId"]),
                             str(config["stepIndex"]))
    os.makedirs(directory, exist_ok=True)
    path = os.path.join(directory, "shard-%d.jsonl" % INDEX)
    with open(path, "w", encoding="utf-8") as handle:
        for record in records:
            handle.write(json.dumps(record) + "\n")
    print("wrote %d records to %s" % (len(records), path), flush=True)

    payload = {"name": config["name"], "runId": config["runId"],
               "stepIndex": config["stepIndex"], "stepId": config["stepId"],
               "stepType": config["stepType"], "shard": INDEX, "node": NODE_NAME,
               "finishedAt": stamp(), "records": records}
    payload.update(extra or {})
    body = json.dumps(payload)
    if len(body) > CONFIGMAP_LIMIT:
        # Truncating would hand the operator a summary it could not tell from a
        # complete one. The raw file is on the PVC either way.
        raise RuntimeError(
            "shard result is %d bytes, over the %d-byte ConfigMap budget. Lower "
            "spec.defaults.repeats or narrow spec.catalogue.select"
            % (len(body), CONFIGMAP_LIMIT))
    return body


def publish(config, body):
    """POST the shard result to the API server as a ConfigMap.

    The results PVC is ReadWriteOnce and the operator may be outside the
    cluster, so results are reported inward, exactly as loader.py reports a
    census inward.
    """
    token_path = "/var/run/secrets/kubernetes.io/serviceaccount/token"
    if not os.path.exists(token_path):
        raise RuntimeError("no service account token at %s" % token_path)
    with open(token_path, encoding="utf-8") as handle:
        token = handle.read().strip()
    name = "bm-%s-%d-%d-s%d" % (config["name"], config["runId"],
                                config["stepIndex"], INDEX)
    doc = {"apiVersion": "v1", "kind": "ConfigMap",
           "metadata": {"name": name, "namespace": NAMESPACE,
                        "labels": {"app": "fhir-benchmark",
                                   "benchmark": config["name"],
                                   "run": str(config["runId"]),
                                   "step": str(config["stepIndex"])}},
           "data": {"result.json": body}}
    root = "https://kubernetes.default.svc/api/v1/namespaces/%s/configmaps" % NAMESPACE
    headers = {"Authorization": "Bearer " + token}
    ca = "/var/run/secrets/kubernetes.io/serviceaccount/ca.crt"
    response = requests.post(root, json=doc, headers=headers, verify=ca, timeout=120)
    if response.status_code == 409:
        response = requests.put("%s/%s" % (root, name), json=doc, headers=headers,
                                verify=ca, timeout=120)
    if response.status_code >= 300:
        raise RuntimeError("publishing %s failed: HTTP %d %s"
                           % (name, response.status_code, (response.text or "")[:800]))
    print("published %s" % name, flush=True)


# --------------------------------------------------------------------------

def wait_for_server(session, seconds=900):
    deadline = time.time() + seconds
    why = "not attempted"
    while time.time() < deadline:
        try:
            response = session.get(BASE_URL.rstrip("/") + "/metadata",
                                   headers={"Accept": "application/fhir+json"},
                                   timeout=30)
            if response.status_code < 500:
                return True
            why = "HTTP %d" % response.status_code
        except Exception as exc:              # noqa: BLE001 - reported, then retried
            why = "%s: %s" % (type(exc).__name__, exc)
        print("waiting for %s -- %s" % (BASE_URL, why), flush=True)
        time.sleep(5)
    print("FAILURE %s never became reachable in %ds; last attempt: %s"
          % (BASE_URL, seconds, why), flush=True)
    return False


STEPS = {"assert": step_assert, "analyze": step_analyze, "prewarm": step_prewarm}


def main():
    with open(CONFIG_PATH, encoding="utf-8") as handle:
        config = json.load(handle)
    start_http_server(METRICS_PORT)
    step_type = config["stepType"]

    records = []
    extra = {}
    try:
        with connect() as conn:
            if step_type == "measure":
                session = requests.Session()
                if not wait_for_server(session):
                    return 1
                extra, code = step_measure(config, conn, records)
            elif step_type in STEPS:
                if INDEX != 0:
                    print("worker %d: %s is single-worker" % (INDEX, step_type), flush=True)
                    return 0
                extra, code = STEPS[step_type](config, conn)
            else:
                return fail("unknown step type %r" % step_type)
    except Exception as exc:                  # noqa: BLE001 - explained, not swallowed
        print(traceback.format_exc(), flush=True)
        return fail("%s raised %s: %s" % (step_type, type(exc).__name__, exc))

    extra["outcome"] = "Passed" if code == 0 else "Failed"
    body = write_shard(config, records, extra)
    publish(config, body)

    invalid = sum(1 for r in records if not r["valid"])
    if invalid:
        print("worker %d: %d of %d measurements INVALID" % (INDEX, invalid, len(records)),
              flush=True)
        for reason in sorted(set(r["invalidReason"] for r in records if r["invalidReason"])):
            print("    %s" % reason, flush=True)
        return fail("step %s produced %d invalid measurement(s)"
                    % (config["stepId"], invalid))
    return code


if __name__ == "__main__":
    sys.exit(main())

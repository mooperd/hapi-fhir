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

Output, per shard, to Google Cloud Storage:

    <ns>/<name>/<runId>/steps/<stepIdx>/shard-<n>.ndjson    every request record
    <ns>/<name>/<runId>/steps/<stepIdx>/shard-<n>.done.json completion marker

Both are PUT to V4 signed URLs handed down in benchmark.json. This worker
holds no GCP credential and links no Google library: the URL is the whole
authorisation, and `requests` is the whole client.

The marker is written last and only after the ndjson body has been accepted,
so its existence means the records are complete. That is what the operator
counts to decide a step has finished.

Records carry raw latency samples, not bucketed ones: percentiles are computed
from raw records, never from Prometheus histograms, which are lossy.
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

import exchange
import upload

CONFIG_PATH = os.environ.get("BENCHMARK_CONFIG", "/config/benchmark.json")
RESULTS_DIR = os.environ.get("RESULTS_DIR", "/results")
BASE_URL = os.environ["FHIR_BASE_URL"]
ES_BASE_URL = os.environ.get("ES_BASE_URL", "")
INDEX = int(os.environ.get("JOB_COMPLETION_INDEX", "0"))
PARALLELISM = int(os.environ.get("PARALLELISM", "1"))
METRICS_PORT = int(os.environ.get("METRICS_PORT", "9100"))
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


def es_query_total(session):
    """Elasticsearch's own cumulative search counter, or None if unreadable.

    This is the positive witness. PostgreSQL statement activity can only ever
    say "Postgres did something", and both engines leave PostgreSQL traces: a
    resource fetch follows an Elasticsearch hit, and _include expansion reads
    hfj_res_link whichever engine ran the search.
    """
    if not ES_BASE_URL:
        return None
    try:
        response = session.get(ES_BASE_URL.rstrip("/") + "/_stats/search", timeout=10)
        response.raise_for_status()
        return response.json()["_all"]["total"]["search"]["query_total"]
    except Exception:
        return None


def attribute(delta, es_delta, requests_issued):
    """Which engine served the search.

    HAPI has exactly two search paths, so for a request that succeeded this is
    elimination over a closed set: Elasticsearch counted a query, or it did not
    and the JPA path ran. delta is no longer consulted for the verdict -- it is
    kept on the record as evidence, not as the basis of the claim.

    The counter is server-wide, so an unrelated Elasticsearch client can land
    inside the window. The search result cache is off on a benchmarking stack,
    so an Elasticsearch-served case issues at least one query on EVERY request:
    a count below the number of requests means at least one request did not go
    to Elasticsearch, and the case was served by the JPA path with strays on
    top. At repeats=30 that tolerates noise for free; at repeats=1 it degrades
    to the bare "did the counter move" test, which is the best available.

    es_delta is None when the counter could not be read, and then nothing is
    claimed either: elimination is only sound while the other engine is
    observable.
    """
    if es_delta is None:
        return None
    return "elasticsearch" if es_delta >= max(1, requests_issued) else "postgres"


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
    """One request. Returns (wallMs, response|None, exception|None, sent).

    sent is what went on the wire, for the evidence capture. It is taken from
    response.request where there is a response, because that is the request
    after the Session merged in its own defaults -- the headers this function
    assembled are only what it asked for.
    """
    url = BASE_URL.rstrip("/") + path
    headers = {"Accept": "application/fhir+json", "Cache-Control": "no-cache"}
    sent = {"method": method, "url": url, "headers": dict(headers), "body": None}
    begin = time.perf_counter()
    try:
        if method == "POST":
            headers["Content-Type"] = "application/x-www-form-urlencoded"
            sent["headers"] = dict(headers)
            sent["body"] = query
            response = session.post(url, data=query, headers=headers, timeout=timeout)
        else:
            sent["url"] = url + (("?" + query) if query else "")
            response = session.get(sent["url"], headers=headers, timeout=timeout)
        if response.request is not None:
            sent["headers"] = dict(response.request.headers)
            sent["url"] = response.request.url
        return (time.perf_counter() - begin) * 1000.0, response, None, sent
    except Exception as exc:                  # noqa: BLE001 - recorded, never swallowed
        return (time.perf_counter() - begin) * 1000.0, None, exc, sent


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


def step_measure(config, conn, records, uploads):
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
    if es_query_total(session) is None:
        return None, fail(
            "the Elasticsearch search counter at %s cannot be read, so no case can "
            "be attributed to an engine. PostgreSQL activity alone cannot tell the "
            "two apart: both engines leave PostgreSQL traces. Set ES_BASE_URL and "
            "make the service reachable from the worker" % (ES_BASE_URL or "<unset>"))
    uploads.prime(mine)
    print("worker %d: %d of %d cases, %d repeats after %d warmup, attribution %s"
          % (INDEX, len(mine), len(cases), repeats, warmup,
             "on" if attributable else "off (concurrency > 1)"), flush=True)

    for case in mine:
        before = pg_snapshot(conn)
        es_before = es_query_total(session)
        rendered = []
        for rep in range(warmup + repeats):
            bindings = binding_sets[min(rep, len(binding_sets) - 1)]
            method = case["method"]
            path = render(case["path"], bindings)
            query = render(case["query"], bindings)
            wall, response, exc, sent = issue(session, method, path, query, timeout)

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
            uploads.record(case, record, sent, response, exc)
            LATENCY.labels(case=case["id"], family=case["family"], cache=cache_label,
                           engine=case.get("expectEngine") or "unknown",
                           stack=STACK).observe(wall / 1000.0)

        delta = pg_delta(before, pg_snapshot(conn))
        es_after = es_query_total(session)
        es_delta = (None if es_before is None or es_after is None
                    else es_after - es_before)
        observed = (attribute(delta, es_delta, len(rendered) + warmup)
                    if attributable else None)
        _reconcile(case, rendered, delta, observed, attributable, tolerance_pct,
                   es_delta)
        # After reconciliation, never before: a repetition that looked valid
        # inside the loop can be invalidated here, and the exchange has to
        # carry the verdict that was actually reached.
        uploads.flush_case()
        done = sum(1 for r in rendered if r["valid"])
        print("worker %d: %s %d/%d valid, engine=%s, readRatio=%s"
              % (INDEX, case["id"], done, len(rendered), observed or "-",
                 "-" if delta["readRatio"] is None else "%.3f" % delta["readRatio"]),
              flush=True)

    return {"cases": len(mine), "attributable": attributable}, 0


def _reconcile(case, rendered, delta, observed, attributable, tolerance_pct,
               es_delta=None):
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
        record["esQueries"] = es_delta

        if not record["valid"]:
            continue

        if expected_engine:
            if not attributable:
                record["valid"] = False
                record["invalidReason"] = (
                    "INVALID engine unattributable: case declares expectEngine=%s "
                    "but both witnesses are server-global counters and this step "
                    "ran at concurrency > 1, so no delta belongs to one case"
                    % expected_engine)
                continue
            if observed is None:
                record["valid"] = False
                record["invalidReason"] = (
                    "INVALID engine unattributable: the Elasticsearch search "
                    "counter gave no usable verdict for this case -- either it "
                    "could not be read, or it moved for some but not all of the "
                    "requests, which a concurrent Elasticsearch client would do")
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

def write_shard(config, records, extra):
    """Stage the records locally, then PUT the body and its completion marker.

    Local first: the upload crosses the public internet, and a step whose
    records existed only in memory would lose them to one dropped TCP
    connection.
    """
    results = (config.get("results") or {}).get(str(INDEX)) or {}
    for key in ("shardUrl", "doneUrl"):
        if not results.get(key):
            raise RuntimeError(
                "benchmark.json carries no results[%d].%s. The operator mints one "
                "signed URL pair per shard at launch; without it this worker has "
                "nowhere to report and the step cannot be trusted" % (INDEX, key))

    os.makedirs(RESULTS_DIR, exist_ok=True)
    path = os.path.join(RESULTS_DIR, "shard-%d.ndjson" % INDEX)
    with open(path, "w", encoding="utf-8") as handle:
        for record in records:
            handle.write(json.dumps(record) + "\n")
    print("staged %d records at %s" % (len(records), path), flush=True)

    upload.put_file(results["shardUrl"], path, upload.NDJSON,
                    results.get("shardUri") or "shard")

    marker = {"name": config["name"], "runId": config["runId"],
              "stepIndex": config["stepIndex"], "stepId": config["stepId"],
              "stepType": config["stepType"], "shard": INDEX, "node": NODE_NAME,
              "finishedAt": stamp(), "records": len(records),
              "object": results.get("shardUri")}
    marker.update(extra or {})
    upload.put_object(results["doneUrl"], json.dumps(marker).encode("utf-8"),
                      upload.JSON, results.get("doneUri") or "marker")


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
    # Started before the server wait so the grant round trip and the first
    # mint are already behind us by the time the first case is measured.
    uploads = (exchange.Uploads(config, RESULTS_DIR, INDEX, NODE_NAME)
               if step_type == "measure" else None)
    try:
        with connect() as conn:
            if step_type == "measure":
                session = requests.Session()
                if not wait_for_server(session):
                    return 1
                extra, code = step_measure(config, conn, records, uploads)
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

    # step_measure returns None for extra when it refuses to start. Guarded
    # here because the drain below and the marker both read it.
    extra = extra or {}
    extra["outcome"] = "Passed" if code == 0 else "Failed"

    # Drained before the shard body and the marker, so that by the time the
    # marker exists every object it names exists too. The marker is what the
    # operator trusts; nothing may be written after it.
    if uploads is not None:
        extra.update(uploads.close())

    write_shard(config, records, extra)

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

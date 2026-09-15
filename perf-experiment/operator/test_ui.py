# Created by claude-opus-5
"""Standalone checks for the FhirDataset jobs/metrics view in ui.py.

No pytest in the operator venv, so this is a plain script:

    .venv/bin/python test_ui.py

Kubernetes and Prometheus are stubbed; nothing here touches a cluster.
"""

import datetime
import os
import sys
from types import SimpleNamespace as NS
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
import ui

now = datetime.datetime.now(datetime.timezone.utc)
ago = lambda s: now - datetime.timedelta(seconds=s)

FAILS = []
def check(label, cond, detail=""):
    print(("PASS " if cond else "FAIL ") + label + ("" if cond else "  <- " + str(detail)))
    if not cond: FAILS.append(label)

# ---- summary(): the bug fix -------------------------------------------
check("FhirStack summary still uses components",
      ui.summary({"components": {"postgres": "Ready", "hapi": "Ready"}})
      == "hapi=Ready, postgres=Ready")
ds_status = {"phase": "Loading", "attempt": 1, "jobName": "ds1-load-1",
             "observed": {"Patient": 400, "Condition": 4400, "Observation": 21200},
             "expected": {"Patient": 1000, "Condition": 11900, "Observation": 58700},
             "reason": "have 400 of 1000 patients"}
check("FhirDataset summary no longer empty",
      ui.summary(ds_status) == "Condition=4400/11900, Observation=21200/58700, Patient=400/1000",
      ui.summary(ds_status))
check("summary empty when status is empty", ui.summary({}) == "")

# ---- metrics(): parsing + totals --------------------------------------
def fake_query(namespace, query):
    if query.startswith("{__name__"):
        return [
            {"metric": {"__name__": "fhir_load_resources_total", "pod": "a"}, "value": [1, "1200"]},
            {"metric": {"__name__": "fhir_load_resources_total", "pod": "b"}, "value": [1, "800"]},
            {"metric": {"__name__": "fhir_load_patients_total", "pod": "a"}, "value": [1, "20"]},
            {"metric": {"__name__": "fhir_load_bundles_total", "pod": "a", "status": "ok"}, "value": [1, "19"]},
            {"metric": {"__name__": "fhir_load_bundles_total", "pod": "a", "status": "failed"}, "value": [1, "1"]},
            {"metric": {"__name__": "fhir_delete_resources_total", "pod": "b", "type": "Patient"}, "value": [1, "5"]},
            {"metric": {"__name__": "fhir_delete_resources_total", "pod": "b", "type": "Condition"}, "value": [1, "7"]},
        ]
    if "resources_total" in query: return [{"metric": {"pod": "a"}, "value": [1, "42.5"]}]
    if "seconds_count" in query:   return [{"metric": {"pod": "a"}, "value": [1, "2.25"]}]
    if "histogram" in query:       return [{"metric": {"pod": "a"}, "value": [1, "NaN"]},
                                           {"metric": {"pod": "b"}, "value": [1, "0.412"]}]
    return []
ui.prom_query = fake_query
rows, totals, err = ui.metrics("perf-s", "ds1")
by_pod = {r["pod"]: r for r in rows}
check("no prom error", err is None, err)
check("two pods", sorted(by_pod) == ["a", "b"], sorted(by_pod))
check("bundles split by status label",
      (by_pod["a"]["bundles_ok"], by_pod["a"]["bundles_failed"]) == (19.0, 1.0))
check("delete counter summed across type label", by_pod["b"]["deleted"] == 12.0,
      by_pod["b"].get("deleted"))
check("rate landed on right pod", by_pod["a"]["resources_per_sec"] == 42.5)
check("NaN p95 becomes None", by_pod["a"]["p95_seconds"] is None)
check("totals sum additive columns", totals["resources"] == 2000.0, totals)
check("p95 excluded from totals", "p95_seconds" not in totals, totals)

# ---- metrics(): prometheus down ---------------------------------------
def boom(namespace, query): raise OSError("connection refused")
ui.prom_query = boom
rows2, totals2, err2 = ui.metrics("perf-s", "ds1")
check("prom failure degrades, does not raise", rows2 == [] and totals2 == {})
check("prom failure reported", "connection refused" in (err2 or ""), err2)
ui.prom_query = fake_query

# ---- jobs_for(): full history, newest first ---------------------------
def mkjob(name, mode, created, completions=4, succeeded=0, failed=0, active=0,
          suspend=False, start=None, done=None, namespace="perf-s", dataset="ds1"):
    return NS(metadata=NS(name=name, labels={"dataset": dataset, "mode": mode},
                          creation_timestamp=created, namespace=namespace),
              spec=NS(completions=completions, parallelism=completions, suspend=suspend),
              status=NS(succeeded=succeeded, failed=failed, active=active,
                        start_time=start, completion_time=done))
jobs = [mkjob("ds1-load-0", "load", ago(600), succeeded=4, start=ago(590), done=ago(300)),
        mkjob("ds1-load-1", "load", ago(120), active=3, failed=1, start=ago(110)),
        mkjob("ds1-purge", "delete", ago(60), suspend=True)]
seen = {}
def _list_jobs(ns, label_selector):
    seen["ns"], seen["selector"] = ns, label_selector
    return NS(items=list(jobs))
all_jobs = jobs + [mkjob("ds2-load-0", "load", ago(30), namespace="perf-m", dataset="ds2")]
def _list_all(label_selector):
    seen["all_selector"] = label_selector
    return NS(items=list(all_jobs))
ui._batch = lambda: NS(list_namespaced_job=_list_jobs,
                       list_job_for_all_namespaces=_list_all)
got, joberr = ui.jobs_for("perf-s", "ds1")
check("job list error-free", joberr is None, joberr)
check("newest job first", [j["name"] for j in got][0] == "ds1-purge", [j["name"] for j in got])
check("all three attempts listed", len(got) == 3)
phases = {j["name"]: j["phase"] for j in got}
check("phases via datasets.job_phase",
      phases == {"ds1-load-0": "Complete", "ds1-load-1": "Active", "ds1-purge": "Paused"}, phases)
check("completions rendered", [j for j in got if j["name"] == "ds1-load-0"][0]["completions"] == "4/4")
check("finished job duration is bounded, not running",
      [j for j in got if j["name"] == "ds1-load-0"][0]["duration"] == "4m50s",
      [j for j in got if j["name"] == "ds1-load-0"][0]["duration"])
check("job query scoped by namespace and dataset label",
      (seen["ns"], seen["selector"]) == ("perf-s", "dataset=ds1"), seen)

# ---- pods_for() -------------------------------------------------------
pods = [NS(metadata=NS(name="ds1-load-1-0-abc", labels={"job-name": "ds1-load-1"},
                       annotations={"batch.kubernetes.io/job-completion-index": "0"}),
           spec=NS(node_name="node-1"),
           status=NS(phase="Running", start_time=ago(110),
                     container_statuses=[NS(restart_count=2,
                                            state=NS(waiting=None,
                                                     terminated=NS(reason="Error")))])),
        NS(metadata=NS(name="ds1-load-1-1-def", labels={"job-name": "ds1-load-1"},
                       annotations={}),
           spec=NS(node_name=None),
           status=NS(phase="Pending", start_time=None, container_statuses=None))]
pod_seen = {}
def _list_pods(ns, label_selector):
    pod_seen["ns"], pod_seen["selector"] = ns, label_selector
    return NS(items=list(pods))
ui._core = lambda: NS(list_namespaced_pod=_list_pods)
gotpods, poderr = ui.pods_for("perf-s", "ds1")
check("pod list error-free", poderr is None, poderr)
check("pod query scoped to loader pods for this dataset",
      (pod_seen["ns"], pod_seen["selector"]) == ("perf-s", "app=fhir-loader,dataset=ds1"),
      pod_seen)
check("restart count surfaced", gotpods[0]["restarts"] == 2)
check("terminated reason surfaced", gotpods[0]["reason"] == "Error")
check("unscheduled pod degrades cleanly",
      gotpods[1]["node"] == "-" and gotpods[1]["age"] == "-")

# ---- flags(): drawFrom is decided at creation, not toggled per row ------
DS_SCHEMA = {"properties": {
    "state": {"type": "string", "enum": ["Present", "Absent"], "default": "Present"},
    "suspend": {"type": "boolean", "default": False},
    "purgeOnDelete": {"type": "boolean", "default": True},
    "parallelism": {"type": "integer", "default": 4},
    "codes": {"type": "object", "properties": {
        "drawFrom": {"type": "string", "enum": ["descendants", "children", "self"]},
        "maxCodesPerRoot": {"type": "integer", "default": 1000}}},
}}
ds_flat = ui.fields(DS_SCHEMA)
flag_paths = [f["path"] for f in ui.flags(ds_flat)]
check("drawFrom is not a row control", "codes.drawFrom" not in flag_paths, flag_paths)
check("the flags that do take effect survive",
      flag_paths == ["purgeOnDelete", "state", "suspend"], flag_paths)
check("drawFrom is still offered by the create form",
      "codes.drawFrom" in [f["path"] for f in ds_flat])

# ---- resource_rows(): per-type progress for the row --------------------
rrows = ui.resource_rows(ds_status)
check("resource rows ordered Patient, Condition, Observation",
      [r["kind"] for r in rrows] == ["Patient", "Condition", "Observation"],
      [r["kind"] for r in rrows])
check("row carries have, want and percent",
      (rrows[0]["have"], rrows[0]["want"], rrows[0]["pct"]) == (400, 1000, 40), rrows[0])
check("nothing observed yet is 0%, not a crash",
      ui.resource_rows({"expected": {"Patient": 10}})[0]["pct"] == 0)
check("no expected count does not divide by zero",
      ui.resource_rows({"observed": {"Patient": 5}})[0]["want"] is None)
check("a finished type is marked done",
      ui.resource_rows({"observed": {"Patient": 10},
                        "expected": {"Patient": 10}})[0]["done"])
check("overshoot caps the bar at 100%",
      ui.resource_rows({"observed": {"Patient": 30},
                        "expected": {"Patient": 10}})[0]["pct"] == 100)
check("an unexpected type still shows up",
      [r["kind"] for r in ui.resource_rows({"observed": {"Encounter": 3}})] == ["Encounter"])
check("empty status yields no rows", ui.resource_rows({}) == [])

# ---- jobs_index(): one cluster-wide call, grouped by dataset -----------
index, idxerr = ui.jobs_index()
check("job index error-free", idxerr is None, idxerr)
check("index keyed by namespace and dataset",
      sorted(index) == [("perf-m", "ds2"), ("perf-s", "ds1")], sorted(index))
check("one list call for every dataset, not one per row",
      seen.get("all_selector") == "dataset", seen.get("all_selector"))
check("grouped jobs stay newest first",
      [j["name"] for j in index[("perf-s", "ds1")]]
      == ["ds1-purge", "ds1-load-1", "ds1-load-0"],
      [j["name"] for j in index[("perf-s", "ds1")]])

# ---- parse_time(): creationTimestamp is a string on the list page ------
check("RFC3339 parses", ui.age(ui.parse_time("2026-09-14T00:00:00Z")) != "-")
check("garbage timestamp degrades to a dash", ui.age(ui.parse_time("x")) == "-")

# ---- routes render ----------------------------------------------------
CRD = {"plural": "fhirdatasets", "kind": "FhirDataset", "version": "v1alpha1",
       "scope": "Namespaced", "schema": DS_SCHEMA}
ui.crds = lambda: [CRD]
ui.find = lambda plural: CRD if plural == "fhirdatasets" else None
ds3_status = {"phase": "Active", "jobName": "ds3-load-0",
              "expected": {"Patient": 50, "Condition": 550, "Observation": 2650}}
ui._custom = lambda: NS(
    get_namespaced_custom_object=lambda *a, **k: {"metadata": {"name": "ds1"}, "status": ds_status},
    list_cluster_custom_object=lambda *a, **k: {"items": [
        {"metadata": {"name": "ds1", "namespace": "perf-s", "creationTimestamp": "x"},
         "spec": {"patients": {"count": 1000}}, "status": ds_status},
        {"metadata": {"name": "ds3", "namespace": "perf-s",
                      "creationTimestamp": "2026-09-14T00:00:00Z"},
         "spec": {"patients": {"count": 50}}, "status": ds3_status}]})

client = ui.app.test_client()
detail = client.get("/fhirdatasets/perf-s/ds1")
html = detail.get_data(as_text=True)
check("detail page 200", detail.status_code == 200, detail.status_code)
for want in ["ds1-load-1", "ds1-purge", "Paused", "ds1-load-1-0-abc", "node-1",
             "1,200", "42.5", "0.412", "Patient", "400/1000",
             "http://prometheus.perf-s.svc:9090"]:
    check("detail shows %r" % want, want in html)
check("p95 NaN renders as dash not 'None'", "None" not in html)
check("totals row present", "Total (2 pods)" in html)

listing = client.get("/fhirdatasets")
lhtml = listing.get_data(as_text=True)
check("list page 200", listing.status_code == 200, listing.status_code)
check("link to detail page", 'href="/fhirdatasets/perf-s/ds1"' in lhtml)
check("row spells out every resource type",
      all(k in lhtml for k in ("Patient", "Condition", "Observation")))
for want in ["400 / 1,000", "4,400 / 11,900", "21,200 / 58,700"]:
    check("row shows progress %r" % want, want in lhtml, want)
check("row draws a progress bar", 'class="progress' in lhtml)
check("row lists the dataset's jobs", "ds1-load-0" in lhtml and "ds1-purge" in lhtml)
check("row marks the job the operator is driving", "ds1-load-1" in lhtml)
check("another namespace's jobs stay out of this row", "ds2-load-0" not in lhtml)
check("no drawFrom control in the row", 'value="codes.drawFrom"' not in lhtml)
check("suspend is still togglable from the row", 'value="suspend"' in lhtml)
check("reason surfaced", "have 400 of 1000 patients" in lhtml)
# Scoped to the ds3 card: ds1 is "Loading", so an unscoped search would pass
# even if "Active" -- the phase datasets.py writes while a job runs -- did not.
ds3_start = lhtml.index(">ds3<")
ds3_bars = lhtml[ds3_start:lhtml.index("dataset=ds3", ds3_start)]
check("a dataset mid-flight animates its bars",
      "progress-bar-animated" in ds3_bars)
check("a dataset with no job of its own says so",
      "No Job carries dataset=ds3" in lhtml)
check("a dataset never counted says so", "never counted" in lhtml)

# ---- logs: framing, fallback, fan-out ---------------------------------
import json as _json
import threading as _threading
import time as _time

class FakeLog:
    """Stands in for the urllib3 response read_namespaced_pod_log hands back
    when _preload_content is False."""
    def __init__(self, chunks, delay=0):
        self.chunks, self.delay, self.closed = list(chunks), delay, False
    def stream(self, amt, decode_content=True):
        for chunk in self.chunks:
            if self.delay: _time.sleep(self.delay)
            if self.closed: return
            yield chunk
    def close(self): self.closed = True
    def release_conn(self): pass

EVENTS = [NS(type="Warning", reason="Failed", message="ErrImagePull: 401 unauthorized",
             last_timestamp=ago(30), event_time=None,
             metadata=NS(creation_timestamp=ago(30))),
          NS(type="Normal", reason="Scheduled", message="assigned to node-1",
             last_timestamp=ago(60), event_time=None,
             metadata=NS(creation_timestamp=ago(60)))]

log_seen = {}
opened = []
def _read_log(name, namespace, **kw):
    log_seen["name"] = name
    log_seen["namespace"] = namespace
    log_seen.update(kw)
    if name == "ds1-load-1-1-def":
        raise ui.client.ApiException(status=400, reason="Bad Request")
    response = FakeLog([b"worker 0: 10", b"0 done\nworker 0: 200 done\nwork", b"er 0: tail"])
    opened.append(response)
    return response
def _list_events(namespace, field_selector=None):
    log_seen["field_selector"] = field_selector
    return NS(items=list(EVENTS))
ui._core = lambda: NS(list_namespaced_pod=_list_pods,
                      read_namespaced_pod_log=_read_log,
                      list_namespaced_event=_list_events)

lines = list(ui.log_lines("perf-s", "ds1-load-1-0-abc"))
check("a line split across two chunks is rejoined",
      lines == ["worker 0: 100 done", "worker 0: 200 done", "worker 0: tail"], lines)
check("a trailing line with no newline is not dropped", lines[-1] == "worker 0: tail")
check("the log connection is closed when the reader stops", opened[0].closed)
check("logs are read from the loader container", log_seen.get("container") == "worker")
check("the tail is bounded, not the whole file", log_seen.get("tail_lines") == ui.LOG_TAIL)

fallback = list(ui.log_lines("perf-s", "ds1-load-1-1-def"))
check("a pod with no log falls back to its events",
      any("ErrImagePull" in l for l in fallback), fallback)
check("the fallback says why there was no log",
      "400" in fallback[0] and "no log" in fallback[0], fallback[0])
check("events are read oldest first",
      fallback.index([l for l in fallback if "Scheduled" in l][0])
      < fallback.index([l for l in fallback if "Failed" in l][0]), fallback)
check("events are scoped to the one pod",
      log_seen.get("field_selector") == "involvedObject.name=ds1-load-1-1-def",
      log_seen.get("field_selector"))

# ---- sse(): a log line must not be able to break the frame -------------
frame = ui.sse("line", {"text": "a\nb: c", "pod": "p"})
check("sse frame ends with a blank line", frame.endswith("\n\n"), repr(frame))
check("sse payload is one physical line", frame.count("\n") == 3, repr(frame))
check("an embedded newline survives the round trip",
      _json.loads(frame.split("data: ", 1)[1])["text"] == "a\nb: c")

# ---- log_stream(): fan-out, tagging, termination -----------------------
targets = [{"name": "ds1-load-1-0-abc", "index": "0"},
           {"name": "ds1-load-1-2-ghi", "index": "2"}]
body = "".join(ui.log_stream("perf-s", targets))
tagged = [_json.loads(chunk.split("data: ", 1)[1])
          for chunk in body.split("\n\n") if "event: line" in chunk]
check("every worker's log reaches the stream",
      sorted({t["pod"] for t in tagged}) == ["ds1-load-1-0-abc", "ds1-load-1-2-ghi"],
      sorted({t["pod"] for t in tagged}))
check("each line carries its completion index",
      {t["index"] for t in tagged} == {"0", "2"}, {t["index"] for t in tagged})
check("both workers' lines are all present", len(tagged) == 6, len(tagged))
check("the stream announces its end when every worker is done",
      "event: done" in body, body[-200:])
check("the stream opens by naming what it is following", body.startswith("event: open"))

# ---- log_stream(): a quiet pod still gets a heartbeat ------------------
def _slow_log(name, namespace, **kw):
    return FakeLog([b"late line\n"], delay=0.25)
ui._core = lambda: NS(list_namespaced_pod=_list_pods, read_namespaced_pod_log=_slow_log,
                      list_namespaced_event=_list_events)
quiet = "".join(ui.log_stream("perf-s", [{"name": "ds1-load-1-0-abc", "index": "0"}],
                              heartbeat=0.05))
check("a quiet stream is kept alive by a comment", ": ping" in quiet, repr(quiet[:200]))
check("the quiet pod's line still arrives", "late line" in quiet)

# ---- log_stream(): the cap closes without claiming the pod is done -----
capped = "".join(ui.log_stream("perf-s", [{"name": "ds1-load-1-0-abc", "index": "0"}],
                               heartbeat=0.05, deadline=_time.monotonic() + 0.1))
check("hitting the stream cap does not fake a completed log",
      "event: done" not in capped, capped[-120:])
ui._core = lambda: NS(list_namespaced_pod=_list_pods, read_namespaced_pod_log=_read_log,
                      list_namespaced_event=_list_events)

# ---- log routes --------------------------------------------------------
page = client.get("/fhirdatasets/perf-s/ds1/logs")
phtml = page.get_data(as_text=True)
check("logs page 200", page.status_code == 200, page.status_code)
check("logs page lists the dataset's pods", "ds1-load-1-0-abc" in phtml)
check("logs page offers a merged view", "pod=all" in phtml or 'value="all"' in phtml)
check("logs page opens an EventSource", "EventSource" in phtml)
check("logs page links back to the dataset",
      'href="/fhirdatasets/perf-s/ds1"' in phtml)

stream = client.get("/fhirdatasets/perf-s/ds1/logs/stream?pod=ds1-load-1-0-abc",
                    buffered=False)
check("stream content type is SSE",
      stream.headers["Content-Type"].startswith("text/event-stream"),
      stream.headers.get("Content-Type"))
check("stream is not buffered by a proxy",
      stream.headers.get("X-Accel-Buffering") == "no")
sbody = b"".join(stream.response).decode()
check("stream carries the log", "worker 0: 200 done" in sbody, sbody[:200])

denied = client.get("/fhirdatasets/perf-s/ds1/logs/stream?pod=kube-apiserver-0")
check("a pod outside the dataset cannot be tailed through the UI",
      denied.status_code == 404, denied.status_code)

dl = client.get("/fhirdatasets/perf-s/ds1/logs/download?pod=ds1-load-1-0-abc")
check("download is plain text",
      dl.headers["Content-Type"].startswith("text/plain"), dl.headers.get("Content-Type"))
check("download is an attachment",
      "attachment" in dl.headers.get("Content-Disposition", ""),
      dl.headers.get("Content-Disposition"))
check("download takes the whole log, not the tail",
      log_seen.get("tail_lines") is None and log_seen.get("follow") is False, log_seen)
check("download body is the log", "worker 0: tail" in dl.get_data(as_text=True))

# ---- the detail page links to all of this ------------------------------
detail2 = client.get("/fhirdatasets/perf-s/ds1")
dhtml = detail2.get_data(as_text=True)
check("pods table links to that pod's log",
      "/fhirdatasets/perf-s/ds1/logs?pod=ds1-load-1-0-abc" in dhtml)
check("jobs table links to a whole job's logs",
      "/fhirdatasets/perf-s/ds1/logs?job=ds1-load-1" in dhtml)


print()
print("FAILED: %d" % len(FAILS))
sys.exit(1 if FAILS else 0)

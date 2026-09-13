# Benchmark data generation — statement of work

Walking skeleton for operator-driven synthetic FHIR ingest. One new CRD, one loader
image, one Prometheus. Everything else is deliberately out.

---

## 1. Audit basis

Written against stripped source, not comments. `/tmp/nocomment.py` truncates every
`COMMENT` token at its column and deletes every string-`Expr` line range, substituting
`pass` where the docstring is a sole body; original line numbers are kept as a prefix.
Per-file self-check asserts `comments=0 docstrings=0` and refuses to emit otherwise.

`perf-experiment/**/*.py`: **4 files, 4 verified, 0 refused.** Independent re-walk of the
mirror: `files=4 comments=0 docstrings=0`. Citations below are to the real tree.

**What already exists and is reused as-is:**

- `app.py:97` `patient_bundle_entries(serial)` — builds one complete patient's entries
  (Patient + n Conditions + n Observations) from `random.Random(SEED * 1000003 + serial)`.
  Every field derives from the serial, so any worker can generate any patient
  independently and a rerun reproduces the dataset exactly. This is the core asset.
- `app.py:146` `_entry()` — emits `{"request": {"method": "PUT", ...}}`. PUT not POST, so
  re-running a finished range is a no-op and an interrupted load restarts safely.
- `app.py:55` `_dictionary()` — 45 codes in three selectivity tiers: 1 at 20%, 9 at 5%,
  35 at 1%. The tiering is what makes queries interesting; the codes themselves are
  placeholders (`COND-RARE-17`) and are what this SOW replaces.

**What must change:**

- `app.py:156` `bundles()` packs entries to `BUNDLE_SIZE` (1000) *across* patient
  boundaries. The requirement is one complete patient per transaction bundle.

**Operator surface this plugs into:** handlers `provision` / `readopt` / `reconfigure` /
`guard` / `readiness` on `fhirstacks`; `patch.status` carries `phase`, `components`,
`applied`. A second CRD follows the same shape.

---

## 2. The problem

Data generation is a Flask app on a laptop driven by hand. It cannot be scaled, its
output is a dict in process memory, and the codes are synthetic strings with no
hierarchy. To answer "what is the fastest I can import", ingest has to become a
scalable, observable, declarative operation on a stack.

---

## 3. Scope

**In:**

1. `FhirDataset` CRD — target stack, patient range, per-patient shape, code mix, parallelism.
2. Loader container image — generates and POSTs one-patient transaction bundles.
3. Operator creates a `Job` with `parallelism: N` from the CR, and never recreates it.
4. A PVC per dataset for results and logs.
5. One Prometheus, deployed by the operator, scraping the loader pods.
6. SNOMED codes resolved once at dataset creation and frozen into a ConfigMap.

**Out:** Grafana, alerting, `FhirBenchmark`, `FhirExperiment`, multi-node fan-out,
retries across pod restarts beyond what PUT idempotency already gives, HA Prometheus,
long-term metric retention.

---

## 4. `FhirDataset`

```yaml
apiVersion: perf.pkb/v1alpha1
kind: FhirDataset
metadata:
  name: run-01
  namespace: perf-s
spec:
  stackRef: perf-s
  seed: 20260911
  patients: {first: 1, count: 100000}

  parallelism: 8              # Job parallelism. THE knob being swept.

  shape:
    conditionsPerPatient: {normal: 11, heavy: 90}
    observationsPerPatient: {normal: 53, heavy: 500}
    heavyEveryN: 10

  codes:
    # Resolved against ontology-main once, at creation, then frozen.
    conditions:
      - {root: "73211009", tier: common, weight: 20}    # Diabetes mellitus
      - {root: "40733004", tier: mid,    weight: 5}     # Infectious disease
      - {root: "19829001", tier: rare,   weight: 1}     # Disorder of lung
    observations:
      - {root: "363787002", tier: common, weight: 20}   # Observable entity
    drawFrom: descendants     # descendants | self | children

status:
  phase: Running              # Pending|Resolving|Running|Complete|Failed
  jobName: run-01-g1
  resolvedCodes: 412
  patientsDone: 43120
  resourcesPerSecond: 3820
  resultsPath: /results/run-01
```

`parallelism` is the only thing that should move between runs. `seed`, `shape` and
`codes` held constant is what makes two runs comparable.

---

## 5. Job lifecycle — jobs are kept forever

**The requirement.** A completed Job must remain on the cluster. If it disappears, the
operator's reconcile sees no Job, creates one, and silently reloads the dataset.

**Three things make this true:**

1. **No `ttlSecondsAfterFinished`.** Omit it entirely. The TTL controller is what deletes
   finished Jobs, and it is the direct cause of the re-reconcile the user identified.
2. **Deterministic Job name, recorded in status.** `<dataset>-g<generation>`, where
   generation is `metadata.generation` at creation. Written to `status.jobName` before
   the Job is created, so a crash between the two leaves a name to reconcile against.
3. **Reconcile is create-if-absent, never delete-and-recreate.** The handler reads
   `status.jobName`; if that Job exists in any state — Running, Complete, Failed — it
   does nothing. A Job only gets created when `status.jobName` is unset.

**Consequence to accept:** changing `spec` does not re-run a dataset. `spec` edits after
creation are ignored except to update `status`. To re-run, create a new `FhirDataset`.
This is the opposite of `FhirStack`, where `on.field` reconciles edits — and it is
correct, because a dataset is an event, not a desired state.

**Cleanup** is manual and explicit: delete the `FhirDataset`, which owns the Job. The
operator never does it on a timer.

---

## 6. Output storage

Job pod logs vanish when pods are garbage-collected, so logs go to disk, not just stdout.

One PVC per `FhirDataset`, `ReadWriteOnce`, owned by the CR, mounted at `/results` by
every worker. Layout:

```
/results/<dataset>/
  worker-<index>.log        append-only, one line per bundle failure
  worker-<index>.csv        serial, entries, http_status, elapsed_ms
  summary.json              written by the operator when the Job completes
```

`ReadWriteOnce` means all workers land on one node. For the walking skeleton that is
acceptable and keeps it simple; it is the first thing to revisit if fan-out needs to
cross nodes.

Workers write per-index files, never a shared file, so there is no locking.

---

## 7. Metrics

Operator deploys one Prometheus into the stack's namespace, scraping loader pods by
label. No Grafana in the skeleton — `/graph` on Prometheus is enough to read a rate.

Loader exposes on `:9100/metrics`:

| Metric | Type | Why |
|---|---|---|
| `fhir_load_bundles_total{status}` | counter | success/failure split |
| `fhir_load_resources_total` | counter | the rate you actually care about |
| `fhir_load_bundle_seconds` | histogram | where latency goes as parallelism rises |
| `fhir_load_patients_total` | counter | progress against `spec.patients.count` |

Peak ingest rate is `rate(fhir_load_resources_total[1m])` summed across pods, swept
against `parallelism`.

---

## 8. SNOMED code resolution

`https://ontology-main.bloods.co.uk/api/snomed` provides `search?q=`,
`concept/<id>`, and `concept/<id>/{parents,children,ancestors,descendants}`.
`descendants` returns the full transitive closure with a `total` (126 for Diabetes
mellitus). `semantic_tag` separates `disorder` from `observable entity`, which is the
Condition/Observation split. 836,526 nodes, 1,520,106 edges.

**Resolved once, at dataset creation, into a ConfigMap the Job mounts.** The ontology
service must not be in the generation hot path — an external HTTP call per resource
would make it the bottleneck being measured, and would make two runs of the same seed
produce different data if the service changed.

**Known limitation, stated by the service itself:** `/relationships` returns empty —
"Only IS-A relationships are currently available in the graph." Subsumption works;
defining relationships (finding-site, method) do not exist. Fine for `:below` and
ValueSet expansion, which is all the benchmark needs.

**Why hierarchy matters:** a code drawn from a subtree gives queries real, uneven
selectivity — some roots have 126 descendants, most have none. A flat random
distribution cannot exercise `:below` at all.

---

## 9. FHIR conformance

Read from R4 while scoping. Only `Observation.status`, `Observation.code` and
`Condition.subject` are mandatory; `Patient` has no required elements at all. "Complete"
is therefore our definition, not the spec's.

Two constraints the generator must respect or it produces resources HAPI will reject:

- `Condition.clinicalStatus` and `verificationStatus` are **required**-strength bindings.
- `Condition.clinicalStatus` SHALL NOT be present when `verificationStatus` is
  `entered-in-error`.

`Condition.code` and `Observation.code` are only *example* bindings, so SNOMED is free
to use. `app.py:113` already emits a fixed `clinicalStatus: active`, which is valid.

---

## 10. Acceptance criteria

1. `kubectl apply` a `FhirDataset` → a Job appears with `parallelism` pods, and
   `status.jobName` is populated.
2. Bundles are one patient each: every POST body is a `transaction` Bundle whose entries
   share a single `Patient/` subject reference.
3. Delete the loader pods mid-run → the Job recreates them and the load completes, with
   no duplicate resources (PUT idempotency).
4. **Job survives completion.** Wait 30 minutes after Complete; the Job is still present
   and no second Job has been created.
5. **Operator restart does not re-run.** Restart the operator against a Complete dataset;
   no new Job, no new writes, `resourcesPerSecond` unchanged.
6. `/results/<dataset>/` contains one CSV and one log per worker index, plus
   `summary.json`, and survives pod deletion.
7. Prometheus returns a non-zero `rate(fhir_load_resources_total[1m])` during the run.
8. Same `seed` and `shape`, two runs → identical resource counts and identical resource
   IDs.
9. Sweep `parallelism` over 1, 4, 8, 16 → a rate curve that is monotonic then flattens.

---

## 11. Risks

**Per-patient bundles cost throughput.** `app.py` batches to 1000 entries precisely for
speed; one patient per bundle is ~64 entries and many more round trips. Realism bought
with rate. Accept it, but do not compare these numbers to any taken with the old batching.

**The bottleneck will move.** Postgres `max_connections` is pinned at 300 with HAPI's
Hikari pool in front. Past some parallelism the measurement stops being ingest and starts
being connection contention. Record Hikari pool size alongside every result.

**`ReadWriteOnce` pins all workers to one node.** Known, accepted for the skeleton.

**Node capacity.** Loader pods compete with the stack they are loading. Give them small
requests and expect the stack to win.

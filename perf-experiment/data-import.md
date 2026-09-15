# Data import: synthetic dataset for HAPI FHIR chained-search benchmarking

## Purpose

Give the search experiment a dataset we control, can regenerate byte-identically, and whose
shape matches the MIMIC-IV FHIR data currently loaded on the test server. Without a controlled
dataset we cannot attribute a slow query to the query rather than to the data.

This document specifies the data. It does not contain the generator code.

## What we are reproducing

Counts confirmed against the live server (`localhost:8083`, 2026-09-10):

| Resource   | Live count   |
|------------|--------------|
| Observation| 29,282,509   |
| Condition  | 5,655,376    |

Observation:Condition ratio = **5.18 : 1**.

I do not have a confirmed Patient count for this server. The design below anchors on
**300,000 patients**, which is the order of magnitude of the MIMIC-IV cohort. Confirm before
generating:

```
curl 'http://localhost:8083/fhir/Patient?_summary=count'
```

If the real figure differs by more than ~20%, keep the totals in the table above and rescale the
per-patient fan-out rather than the totals — the absolute row counts in `HFJ_RES_LINK` and
`HFJ_SPIDX_TOKEN` are what drive query cost, not the patient count.

## Resource set: three types

Patient, Condition, Observation. Nothing else.

MIMIC-IV FHIR also carries Encounter, Procedure, MedicationRequest/Administration, Specimen and
friends. We omit them because the mechanism we are measuring — the join from `HFJ_RES_LINK` into a
`HFJ_SPIDX_*` table, once per hop — is identical whichever resource type sits at the far end. Adding
types multiplies load time and adds nothing to the measurement. Every reference points directly at a
Patient, so every chain in the experiment is exactly one hop.

The cost of that simplification is stated at the end of this document.

## Shape

### Patient

| Field        | Value                                            | Index exercised |
|--------------|--------------------------------------------------|-----------------|
| `id`         | `perf-p0000001` … `perf-p<N>` (7 digits, headroom to 10M)                  | `HFJ_RESOURCE`  |
| `identifier` | system `http://perf.fhir/mrn`, value = serial     | token, unique   |
| `gender`     | `male` / `female`, 50/50                         | token, ~50% sel |
| `birthDate`  | uniform over 1930-01-01 … 2010-12-31             | date            |
| `name.family`| `Surname0000001` … (one per patient, unique)      | string          |

Four searchable parameters spanning three different SPIDX tables, plus `_id`. That is enough to
vary the selectivity of the *Patient-side* predicate in a reverse-chained query, which is one of
the two variables that matter.

Client-assigned IDs work on a default HAPI server: `ClientIdStrategyEnum.ALPHANUMERIC` is the
default and permits any non-purely-numeric ID
([JpaStorageSettings.java:2843](../hapi-fhir-jpaserver-model/src/main/java/ca/uhn/fhir/jpa/api/config/JpaStorageSettings.java#L2843)).
The `perf-` prefix keeps the IDs alphanumeric and makes the synthetic cohort trivially separable
from MIMIC data. It also means the search experiment can be written against known IDs with no
lookup step.

### Condition

| Field            | Value                                                    |
|------------------|----------------------------------------------------------|
| `id`             | `perf-c<patient-serial>-<NNN>`                                         |
| `subject`        | `Patient/perf-p0000001`                                   |
| `code`           | one of 45 codes, system `http://perf.fhir/cond` (see below)|
| `clinicalStatus` | `active`                                                 |
| `onsetDateTime`  | uniform over 2015-01-01 … 2024-12-31                     |

### Observation

| Field               | Value                                                   |
|---------------------|---------------------------------------------------------|
| `id`                | `perf-o<patient-serial>-<NNN>`                                        |
| `subject`           | `Patient/perf-p0000001`                                  |
| `status`            | `final`                                                 |
| `code`              | one of 45 codes, system `http://perf.fhir/obs`            |
| `valueQuantity`     | numeric, range and unit fixed per code                  |
| `effectiveDateTime` | uniform over 2015-01-01 … 2024-12-31                    |

No Encounter reference, no category, no reference ranges, no components.

## Fan-out and skew

MIMIC per-patient volume is heavily skewed: an ICU stay generates thousands of chart and lab
events, most patients generate a handful. Uniform fan-out would give every chained query the same
cost and hide the tail — and the tail is where a cohorting platform falls over. Two bands is enough
to capture that without complicating the generator.

| Band            | Share of patients | Conditions/patient | Observations/patient |
|-----------------|-------------------|--------------------|----------------------|
| Heavy (ICU-like)| 10%               | 90                 | 500                  |
| Light           | 90%               | 11                 | 53                   |

Resulting means: 18.9 Conditions and 97.7 Observations per patient.

At 300,000 patients:

| Resource   | Generated  | Live MIMIC | Delta  |
|------------|------------|------------|--------|
| Condition  | 5,670,000  | 5,655,376  | +0.3%  |
| Observation| 29,310,000 | 29,282,509 | +0.1%  |

The bands are assigned by serial number — patients whose serial ends in `0` are heavy. Deterministic,
and it means a query can target the heavy band directly when we want the worst case.

## Code dictionaries and selectivity

45 codes per resource type, in three selectivity tiers:

| Tier   | Codes | Share of rows each | Condition rows each (L) | Observation rows each (L) |
|--------|-------|--------------------|-------------------------|---------------------------|
| Common | 1     | 20%                | 1,134,000               | 5,862,000                 |
| Mid    | 9     | 5%                 | 283,500                 | 1,465,500                 |
| Rare   | 35    | 1%                 | 56,700                  | 293,100                   |

Codes are named by tier so a query reads unambiguously: `COND-COMMON-01`, `COND-MID-03`,
`COND-RARE-17`, and the same for `OBS-`.

This is the point of the whole dataset. The cost of a reverse-chained query is driven by the size of
the intermediate match set, and these three tiers give us a 20× spread in intermediate size with
everything else held constant. Expected row counts are known in advance, so a result count that
disagrees means the generator or the index is wrong, not the query.

Code choice is independent of patient band, so the heavy-band skew and the code skew are separable
variables.

## Scale tiers

Same per-patient spec throughout; only the patient count changes.

| Tier | Patients | Conditions | Observations | Total resources |
|------|----------|------------|--------------|-----------------|
| S    | 10,000   | 189,000    | 977,000      | 1.18M           |
| M    | 100,000  | 1,890,000  | 9,770,000    | 11.76M          |
| L    | 300,000  | 5,670,000  | 29,310,000   | 35.0M           |

Load S and M first. Three tiers give us a growth curve, which is the deliverable — a single number
at one scale tells us nothing about whether the bottleneck is linear, or worse.

Load them into **separate partitions or separate databases**, not side by side in one unpartitioned
schema. Otherwise the S-tier measurement is taken against a 35M-row index and measures nothing.

## Determinism

- Single integer seed, recorded in the output filename and in the results CSV.
- All randomness from that seed. Same seed, same dataset, row for row.
- Serial-number-derived assignment for band and code tier, so any resource's expected properties can
  be worked out by hand during debugging.

## Load mechanism

1. FHIR `transaction` bundles, `PUT` entries (upsert by client-assigned ID), 500 entries per bundle.
   `PUT` makes the load restartable without duplicate-checking logic.
2. Order: all Patients first, then Conditions and Observations. Referential integrity is checked on
   write, so children cannot precede parents.
3. Single writer to start. Raise concurrency only after measuring the single-writer rate; we need the
   load to finish, not to be fast.
4. Record wall-clock and resources/second per tier. The ingestion rate is itself a number the
   platform decision needs.

Server settings that speed the load:

- `setMassIngestionMode(true)` — skips modification checks and unique-combo pre-checks
  ([StorageSettings.java:468](../hapi-fhir-jpaserver-model/src/main/java/ca/uhn/fhir/jpa/model/entity/StorageSettings.java#L468)).
- `setDeleteEnabled(false)`, and the `:text`-index and upsert-existence-check knobs described in
  [server_jpa/performance.md](../hapi-fhir-docs/src/main/resources/ca/uhn/hapi/fhir/docs/server_jpa/performance.md).

**These must be reverted before measuring.** They change what gets written to the index tables. A
dataset loaded under one configuration and queried under another produces numbers that transfer
nowhere. Record the exact config used for the load alongside the results.

## Implementation

Implemented by [app.py](app.py) — `patient_bundle_entries()` for the shape, `bundles()` for the
stream. Loading is driven from the **Load data** page: pick a stack, pick a patient count. Throughput
knobs are the `BUNDLE_SIZE` and `WORKERS` constants at the top of that file, deliberately not UI
options.

`--first-patient` equivalent is the "First patient serial" field, for resuming an interrupted load.

## Generator requirements

The generator script must:

1. Take `--patients`, `--seed`, `--base-url`, `--bundle-size` and nothing else.
2. Stream bundles — never hold the dataset in memory. At L this is 35M resources.
3. Fail on the first non-2xx response and print the `OperationOutcome`. No retries, no skipping.
4. Print progress every 100 bundles: resources written, elapsed, rate.
5. Be resumable by re-running with the same seed and a `--start-patient` offset. `PUT` semantics make
   re-running a completed range a no-op.

## What we leave out, and what it costs

| Omission | What we lose |
|----------|--------------|
| Encounter and other intermediate types | We cannot measure multi-hop chains (`Observation?subject:Patient._has:...` through an Encounter). Every chain here is one hop. If the platform needs two-hop chains, that needs a second dataset. |
| Long-tail code cardinality (45 codes, not thousands) | Real MIMIC has far more distinct codes, so real per-code selectivity goes lower than our 1% rare tier. Our rare tier understates the best case, not the worst. |
| Skewed date distribution | Real admissions cluster. Uniform dates make date-range predicates cut a clean, predictable fraction — which is what we want for attribution, but means a date-range result here is optimistic. |
| Realistic value distributions | Quantity-range searches will behave better than reality. Do not quote quantity numbers from this dataset as production estimates. |

## IG note

Needs IG confirmation, not settled:

- **MIMIC-IV is real de-identified patient data under a PhysioNet credentialed-access DUA.** The DUA
  restricts redistribution. Before extracting frequency tables, code lists or value distributions
  from the live MIMIC index tables into anything committed to a repository or shared outside the
  cluster, confirm what the DUA permits.
- The synthetic dataset must not be seeded from real MIMIC values. Codes and identifiers above are
  invented (`http://perf.fhir/*`) precisely so the output carries no derived patient data and no IG
  obligation of its own.
- Confirm where the test cluster and its database sit, and the retention position on the loaded MIMIC
  copy. That is a separate question from this experiment but it is live now.

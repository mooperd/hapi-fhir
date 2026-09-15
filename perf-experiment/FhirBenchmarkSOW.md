# FhirBenchmark — Statement of Work

**Scope:** design and delivery of a `FhirBenchmark` custom resource, its operator handlers, its
worker, and a query catalogue that comprehensively exercises the search functionality actually
implemented in the HAPI FHIR codebase at this revision.

**Companion document:** `FhirBenchmarkConcept.md` (rationale, clinical framing).
This document is the engineering contract.

**Provenance of every HAPI claim below:** read under the code-only audit protocol. Seventeen
source files were mirrored through a comment/Javadoc stripper and every one emitted
`VERIFY <path>: comments=0 javadoc=0`. Line references are to the current working tree. No
prose, Javadoc, or `.md` file in the HAPI repository was consulted.

---

## 1. Governing design constraint: no fallbacks

This is a measurement instrument. An instrument that silently substitutes a value when it cannot
obtain the real one does not produce degraded data; it produces **data that cannot be
distinguished from good data**, which is worse than no data. Therefore:

> **Every condition that is not exactly what was declared is a hard, named failure.
> Nothing is defaulted, substituted, skipped, retried into success, or "best-efforted".**

Concretely, the following are all **errors**, not adaptations:

| Situation | Required behaviour |
|---|---|
| `spec.stackRef` or `spec.datasetRef` absent | CRD validation rejects the object. No namespace-inferred default. |
| Referenced `FhirStack` not `phase: Ready` | `PermanentError`. Do not wait-and-hope; do not measure a degraded stack. |
| Referenced `FhirDataset` not `phase: Ready`, or `status.observed` below `status.expected` | `PermanentError`. Never benchmark a partially loaded population. |
| A template placeholder cannot be bound from the dataset manifest | `PermanentError` naming the placeholder. No "pick any code". |
| A case declares `expectEngine: elasticsearch` and HAPI served it from JPA | Measurement recorded `INVALID`, step outcome `Failed`. Never silently accept the other engine. |
| Observed row count differs from the predicted cardinality beyond tolerance | Measurement `INVALID`. Never report a suspiciously fast number without its row count reconciled. |
| Cold-cache step where `shared_blks_read / (hit+read)` is below the declared floor | Step `Failed`. Never relabel a warm measurement as cold. |
| A query exceeds `timeoutSeconds` | Recorded as `TIMEOUT` with the timeout value. Never recorded as a latency sample, never retried. |
| `ANALYZE` has not run since the dataset load completed | `PermanentError`. |
| HTTP non-2xx from HAPI | Recorded as `ERROR` with the `OperationOutcome` issue codes. Not retried, not excluded from the report. |

There is precedent for this posture inside HAPI itself: when full-text search is not configured,
`SearchBuilder.checkUseHibernateSearch()` does not quietly degrade `_text`/`_content` to a SQL
`LIKE` — it throws `InvalidRequestException` with `Msg.code(1192)`
(`SearchBuilder.java:668-698`), and it throws `Msg.code(2524)` rather than accept full-text
combined with a chained sort (`:700-706`). We adopt the same stance.

**The one permitted retry** is at the Kubernetes reconcile level: `kopf.TemporaryError` for
infrastructure that has not converged yet (pod not scheduled, API conflict). That is
orchestration, not measurement. No measurement is ever retried.

---

## 2. The declarative/imperative compromise

A `FhirStack` and a `FhirDataset` describe a *state* the world should be in, and the operator
drives towards it, repeatedly and idempotently. A benchmark is not a state. It is **an ordered
sequence of one-shot, side-effecting, non-idempotent events whose results are only meaningful in
the order they occurred.** Re-running it does not converge on anything; it produces a second,
different set of numbers.

We do not pretend otherwise. The compromises are explicit and each is a deliberate departure
from the pattern used by the other two CRDs:

| # | Compromise | Why | Consequence |
|---|---|---|---|
| C1 | **`spec` is immutable after admission**, enforced by a CEL validation rule, except `spec.state` and `spec.runId`. | A benchmark whose question changed halfway through is uninterpretable. | To change the plan you create a new object. Editing is rejected by the API server, not by the operator. |
| C2 | **The reconcile loop is a step-advancing state machine, not a convergence loop.** It asks "which step index is next?", never "does the world match spec?". | There is no steady state to converge on. | The handler is not idempotent in the usual sense; idempotency is provided by the step journal (§5.3), not by re-derivation. |
| C3 | **`status.journal` is append-only and is the primary output**, not a summary of live state. | Kubernetes status is conventionally a *reflection* of reality. Here it is the *record* of history. | Status is large. Results beyond a summary go to the results PVC; status carries the journal plus headline percentiles. |
| C4 | **Re-running requires an explicit `spec.runId` increment.** No automatic re-run, ever, including after operator restart. | An operator restart mid-run must not silently repeat measurements and append them to the same series. | On `kopf.on.resume`, a run in `phase: Running` is moved to `phase: Aborted` with reason `operator-restart`. It is never resumed. Partial results are retained and clearly marked partial. |
| C5 | **The benchmark mutates the stack it measures** — restarting Postgres, restarting Elasticsearch, setting `reuseCachedSearchResultsForMillis=0`. | Cache-state control is impossible otherwise. | A `FhirBenchmark` takes an exclusive lease on its `stackRef`. Two benchmarks against one stack is a `PermanentError`, as is a benchmark against a stack with an actively loading dataset. |
| C6 | **Wall-clock ordering matters and is recorded.** | Percentiles from steps run hours apart under different node conditions are not comparable. | Every journal entry carries `startedAt`/`finishedAt` and the node name the worker landed on. |

---

## 3. Architecture

```
FhirBenchmark (CRD)
      │
      ▼
fhir_operator.py  ──►  benchmark.py          step-advancing handler + case tables
      │                    │
      │                    ├─► catalogue.py   loads + validates the query catalogue
      │                    ├─► binding.py     binds placeholders from dataset.json
      │                    └─► control.py     restart / settle / ANALYZE / pg_prewarm
      ▼
Indexed Job "bm-<name>-<runId>-<stepIdx>"
      │  parallelism = step.concurrency
      ▼
runner.py (worker)  ──► HAPI FHIR REST  ──► PostgreSQL / Elasticsearch
      │
      ├─► prometheus_client Histogram  fhir_query_seconds{...}
      └─► /results/<name>/<runId>/<stepIdx>/shard-<n>.jsonl   raw per-request records
```

Reuses existing infrastructure unchanged:

- Prometheus is already deployed per namespace and scrapes pods by label
  (`datasets.py:330-346`). Benchmark pods carry `app=fhir-benchmark` and are picked up by adding
  one `keep` relabel rule.
- `pg_stat_statements` is already in `shared_preload_libraries` (`fhir_operator.py:144`).
- The results PVC pattern and `Indexed` Job completion mode follow `datasets.py:256-323`.
- Worker RBAC follows `datasets.py:238-253`, extended to `fhirbenchmarks`/`status`.

---

## 4. The `FhirBenchmark` custom resource

### 4.1 Full spec

```yaml
apiVersion: perf.fhir/v1alpha1
kind: FhirBenchmark
metadata:
  name: cohort-core-a
spec:
  # --- identity: both mandatory, no defaults, immutable ---
  stackRef:   behemoth
  datasetRef: mimic-1m

  # --- run control: the only mutable fields ---
  state: Run                 # Run | Hold | Abort
  runId: 1                   # increment to re-run; never auto-incremented

  # --- what to ask ---
  catalogue:
    name: cohort-core        # a versioned catalogue in the operator image
    revision: "sha256:…"     # pinned; mismatch is a PermanentError
    select:
      families: [token, date, quantity, composite, reference, chain, has,
                 include, fulltext, missing, paging, sort, total, special]
      cases: []              # explicit case ids; mutually exclusive with families
      exclude: []

  # --- how to run it ---
  defaults:
    concurrency: 8
    repeats: 30
    warmup: 5
    timeoutSeconds: 120
    bindingMode: pin         # pin | rotate
    tolerance:
      rowCountPct: 0         # 0 = exact match required
      coldReadRatioFloor: 0.80

  # --- the ordered sequence of events ---
  plan:
    - step: assert
      id: preflight
      require:
        stackReady: true
        datasetReady: true
        analyzedSinceLoad: true
        searchResultCacheDisabled: true
        exclusiveLease: true

    - step: configure
      id: disable-result-cache
      hapi:
        reuseCachedSearchResultsForMillis: 0

    - step: restart
      id: cold-start
      components: [postgres, elasticsearch, hapi]

    - step: settle
      id: quiesce
      require:
        deploymentsReady: true
        autovacuumIdle: true
        esPendingTasks: 0
        hapiWarmupRequests: 200

    - step: measure
      id: cold
      repeats: 1
      bindingMode: rotate            # fresh binding per repetition
      cacheLabel: cold
      require:
        coldReadRatioFloor: 0.80     # verified from pg_stat_statements

    - step: prewarm
      id: warm-indexes
      relations: [HFJ_SPIDX_TOKEN, HFJ_SPIDX_DATE, HFJ_SPIDX_QUANTITY,
                  HFJ_RES_LINK, HFJ_RESOURCE]

    - step: measure
      id: warm
      repeats: 30
      cacheLabel: warm

    - step: measure
      id: hot
      repeats: 30
      bindingMode: pin               # identical query every time
      cacheLabel: hot

    - step: report
      id: summarise
```

### 4.2 Step types

| `step` | Effect | Failure semantics |
|---|---|---|
| `assert` | Evaluates preconditions. Performs nothing. | Any unmet requirement ⇒ `PermanentError`. |
| `configure` | Applies HAPI `application.yaml` settings via the stack ConfigMap, then restarts HAPI. Records the before-value so it can be restored at teardown. | Setting not present in the ConfigMap ⇒ `PermanentError`. Never created implicitly. |
| `restart` | `rollout restart` of named deployments; waits for Ready. | Timeout ⇒ step `Failed`. |
| `settle` | Blocks until the declared quiescence conditions hold. | Timeout ⇒ step `Failed`. |
| `prewarm` | `pg_prewarm` over named relations. | Missing relation ⇒ `PermanentError` (a renamed table must not be silently skipped). |
| `analyze` | `ANALYZE` over named relations, or the whole schema. | Non-zero exit ⇒ step `Failed`. |
| `measure` | Launches the Indexed Job, waits for completion, ingests shard files. | Any `INVALID` measurement ⇒ step `Failed`. |
| `report` | Computes percentiles, writes summary to status and PVC. | — |

`spec.plan` being an explicit ordered list is the direct answer to *"a sequence of events to test
performance before and after cache warming"*: the cold/prewarm/warm/hot ordering above is data,
not code, and a different experiment is a different `plan`.

### 4.3 Printer columns

Following `crd.yaml` and `dataset-crd.yaml` conventions:

```yaml
additionalPrinterColumns:
  - {name: Phase,   type: string,  jsonPath: .status.phase}
  - {name: Run,     type: integer, jsonPath: .status.runId}
  - {name: Step,    type: string,  jsonPath: .status.currentStepId}
  - {name: Done,    type: string,  jsonPath: .status.progress}
  - {name: Stack,   type: string,  jsonPath: .spec.stackRef}
  - {name: Dataset, type: string,  jsonPath: .spec.datasetRef}
  - {name: Invalid, type: integer, jsonPath: .status.invalidCount}
  - {name: Age,     type: date,    jsonPath: .metadata.creationTimestamp}
```

---

## 5. Lifecycle

### 5.1 Object phases

```
                  ┌──────────────────────────────────────────┐
                  │                                          │
Pending ─► Validating ─► Leasing ─► Running ─► Reporting ─► Complete
   │           │            │          │  │                    │
   │           │            │          │  └─► Aborted ◄────────┘  (spec.state: Abort)
   │           ▼            ▼          ▼
   └────────► Failed ◄──────┴──────────┘
                  │
                  ▼
             Restoring ─► (terminal phase retained)
```

| Phase | Meaning |
|---|---|
| `Pending` | Object admitted; nothing done. |
| `Validating` | Catalogue revision pinned, placeholders bound, preconditions evaluated. |
| `Leasing` | Acquiring the exclusive lease on `stackRef`. |
| `Running` | Executing `spec.plan[currentStep]`. |
| `Reporting` | All steps done; computing percentiles. |
| `Restoring` | Reverting `configure` steps to their recorded before-values. Always runs, including after failure. |
| `Complete` | Run finished, all measurements valid. |
| `Failed` | A step failed. Journal retained. Partial results retained and marked partial. |
| `Aborted` | Operator restart mid-run (C4), or `spec.state: Abort`. |
| `Hold` | `spec.state: Hold`. Current step completes; no further step starts. |

`Complete` and `Failed` are terminal for a given `runId`. Incrementing `spec.runId` resets to
`Pending` and starts a new journal; prior journals are archived to the results PVC, never
overwritten.

### 5.2 Step lifecycle

Each step is `Pending → Running → (Passed | Failed | Skipped)`. `Skipped` occurs only when
`spec.state: Abort` was set — never as a reaction to a problem.

### 5.3 The journal

```yaml
status:
  phase: Running
  runId: 1
  currentStep: 4
  currentStepId: cold
  progress: "4/9 steps, 214/340 measurements"
  invalidCount: 0
  catalogueRevision: "sha256:…"
  bindings:
    commonConditionCode: "http://snomed.info/sct|44054006"
    rareConditionCode:   "http://snomed.info/sct|237599002"
    heavyPatientId:      "Patient/mimic-1m-000010"
  journal:
    - {index: 0, id: preflight,  step: assert,  outcome: Passed,
       startedAt: "…", finishedAt: "…"}
    - {index: 3, id: cold, step: measure, outcome: Passed,
       jobName: bm-cohort-core-a-1-3, node: node-07,
       measurements: 34, invalid: 0,
       coldReadRatio: 0.91}
  summary:
    token-exact-common-es:
      cold: {p50ms: 3120, p95ms: 4410, p99ms: 4980, rows: 41203, engine: elasticsearch}
      warm: {p50ms: 41,   p95ms: 88,   p99ms: 140,  rows: 41203, engine: elasticsearch}
      hot:  {p50ms: 22,   p95ms: 35,   p99ms: 51,   rows: 41203, engine: elasticsearch}
```

The journal is append-only. The operator writes entries; it never rewrites one.

---

## 6. Coverage: what "comprehensively test all the search functionality" means

Coverage is defined against the HAPI implementation, using HAPI's own identifiers. The catalogue
is complete when every row below has at least one case, and every row marked **pair** has a
matched pair differing by exactly one term across the Hibernate Search eligibility boundary.

### 6.1 `RestSearchParameterTypeEnum` (`RestSearchParameterTypeEnum.java:34-95`)

`NUMBER`, `DATE`, `STRING`, `TOKEN`, `REFERENCE`, `COMPOSITE`, `QUANTITY`, `URI`, `HAS`,
`SPECIAL` — one family per value.

### 6.2 `ParamPrefixEnum` (`ParamPrefixEnum.java:39-95`)

All nine, against `DATE`, `NUMBER` and `QUANTITY`:
`APPROXIMATE(ap)`, `ENDS_BEFORE(eb)`, `EQUAL(eq)`, `GREATERTHAN(gt)`,
`GREATERTHAN_OR_EQUALS(ge)`, `LESSTHAN(lt)`, `LESSTHAN_OR_EQUALS(le)`, `NOT_EQUAL(ne)`,
`STARTS_AFTER(sa)`.

`ne` and `sa`/`eb` are called out separately: they are range-exclusion predicates and are
expected to behave differently from the inclusive comparators.

### 6.3 Qualifiers (`Constants.java:276-287, 364-367`)

| Constant | Literal | Family | HSearch-eligible |
|---|---|---|---|
| `PARAMQUALIFIER_MISSING` (+`_TRUE`/`_FALSE`) | `:missing` | missing | **No** |
| `PARAMQUALIFIER_STRING_EXACT` | `:exact` | string | Yes |
| `PARAMQUALIFIER_STRING_CONTAINS` | `:contains` | string | Yes |
| `PARAMQUALIFIER_STRING_TEXT` / `PARAMQUALIFIER_TOKEN_TEXT` | `:text` | string, token | Yes |
| `PARAMQUALIFIER_TOKEN_NOT` | `:not` | token | **No** |
| `PARAMQUALIFIER_TOKEN_IN` | `:in` | token | **No** |
| `PARAMQUALIFIER_TOKEN_NOT_IN` | `:not-in` | token | **No** |
| `PARAMQUALIFIER_TOKEN_ABOVE` | `:above` | token | **No** |
| `PARAMQUALIFIER_TOKEN_BELOW` | `:below` | token | **No** |
| `PARAMQUALIFIER_TOKEN_OF_TYPE` | `:of-type` | token | **No** |
| `PARAMQUALIFIER_TOKEN_IDENTIFIER` | `:identifier` | reference | **No** |
| `PARAMQUALIFIER_MDM` | `:mdm` | reference | **No** (explicit `return false`) |
| `PARAMQUALIFIER_NICKNAME` | `:nickname` | string/reference | **No** (explicit `return false`) |
| `PARAM_INCLUDE_QUALIFIER_RECURSE` | `:recurse` | include | **No** |
| `PARAM_INCLUDE_QUALIFIER_ITERATE` | `:iterate` | include | **No** |
| `:[TargetType]` | e.g. `subject:Patient` | reference | **No** |

Eligibility column derived from `ExtendedHSearchSearchBuilder.isParamTypeSupported()`
(`ExtendedHSearchSearchBuilder.java:177-235`) and `isSupportsAllOf()` (`:133-157`).
`:mdm` and `:nickname` fall through to `return false` at `:218-221`.

### 6.4 `QueryStack` predicate surface — the JPA/PostgreSQL path

Every `createPredicate*` entry point must be reachable by at least one case
(`QueryStack.java`, line numbers as shown):

`createPredicateComposite` (549, 566, `createPredicateCompositePart` 620) ·
`createPredicateCoords` (848) · `createPredicateDate` (893, 912) ·
`createPredicateFilter` (961, 1008) · `createPredicateHas` (1135) ·
`createPredicateNumber` (1273, 1292) · `createPredicateQuantity` (1351, 1370) ·
`createPredicateReference` (1439, 1461) ·
`createPredicateReferenceForEmbeddedChainedSearchResource` (1554) ·
`createPredicateResourceId` (1961) · `createPredicateResourcePID` (2519) ·
`createPredicateSource` (1966, 1977) · `createPredicateString` (2039, 2058) ·
`createPredicateTag` (2098) · `createPredicateToken` (2217, 2236) ·
`createPredicateTokenForMultipleResourceTypes` (2359) · `createPredicateUri` (2392, 2411) ·
`createReverseSearchPredicateLastUpdated` (2545) · `createPredicateSearchParameter` (2566) ·
`createIndexPredicate` (1870).

Missing-parameter handling has three distinct implementations and all three must be hit, because
they have different costs:

- `createMissingPredicateForIndexedMissingFields` (760) — requires `IndexMissingFields.ENABLED`
- `createMissingPredicateForUnindexedMissingFields` (838) — the `NOT EXISTS` anti-join
- `createMissingPredicateForCustomIndexProvider` (746)

Special dispatch in `searchForIdsWithAndOr()` (`QueryStack.java:2454-2514`) covers
`IAnyResource.SP_RES_ID`, `Constants.PARAM_PID`, `PARAM_HAS`, `PARAM_TAG`, `PARAM_PROFILE`,
`PARAM_SECURITY`, `PARAM_SOURCE`, `PARAM_LASTUPDATED`. Note that `_tag`/`_profile`/`_security`
take a **different branch** depending on `getTagStorageMode()` — `INLINE` routes to
`createPredicateSearchParameter`, otherwise to `createPredicateTag` (`:2479-2492`). Both
branches must be benchmarked; `TagStorageModeEnum` is `VERSIONED` (default), `NON_VERSIONED`,
`INLINE` (`StorageSettings.java:103, 1347-1364`).

Combo indexes: `ComboUniqueSearchParameterPredicateBuilder` and
`ComboNonUniqueSearchParameterPredicateBuilder` are only exercised when
`isUniqueIndexesEnabled()` is true (default, `JpaStorageSettings.java:183`). Cases must include a
parameter combination that HAPI has a combo index for, run with the setting on and off — this is
one of the largest single-parameter wins available and is currently unmeasured.

### 6.5 `ExtendedHSearchClauseBuilder` surface — the Elasticsearch path

Every `add*Search` entry point must be reachable (`ExtendedHSearchClauseBuilder.java`):

`addResourceTypeClause` (121) · `addTokenUnmodifiedSearch` (180) · `addStringTextSearch` (239) ·
`addStringExactSearch` (312) · `addStringContainsSearch` (326) · `addStringUnmodifiedSearch` (354) ·
`addReferenceUnchainedSearch` (377) · `addDateUnmodifiedSearch` (467) ·
`addQuantityUnmodifiedSearch` (610) · `addUriUnmodifiedSearch` (756) ·
`addNumberUnmodifiedSearch` (777) · `addCompositeUnmodifiedSearch` (799).

### 6.6 The eligibility boundary — **pair** cases

`ExtendedHSearchSearchBuilder.canUseHibernateSearch()` (`:92-128`) returns false if **any** term
is unsupported; `isSupportsAllOf()` (`:133-157`) additionally excludes a query with any
`_include`, any `_revinclude`, a non-null `everythingMode`, delete-expunge, a `nearDistanceParam`,
or `searchContainedMode != FALSE`. `ourUnsafeSearchParmeters = {"_id", "_meta", "_count"}` (`:63`)
disqualifies on name alone. `SearchBuilder.checkUseHibernateSearch()` (`:668-691`) adds
`supportsAllSortTerms()`.

For each disqualifier a **pair** is mandatory — same cohort, one term different:

| Pair id | Eligible member | Disqualified by |
|---|---|---|
| `pair-unsafe-id` | `Condition?code=X` | `&_id=…` |
| `pair-unsafe-count` | `Condition?code=X` | `&_count=…` |
| `pair-include` | `Condition?code=X` | `&_include=Condition:subject` |
| `pair-revinclude` | `Patient?gender=female` | `&_revinclude=Condition:subject` |
| `pair-chain` | `Observation?subject=Patient/N` | `subject.name=…` |
| `pair-token-below` | `Condition?code=X` | `code:below=X` |
| `pair-token-not` | `Condition?code=X` | `code:not=X` |
| `pair-missing` | `Patient?gender=female` | `gender:missing=true` |
| `pair-date-prefix` | `Observation?date=2024-01-01` | `date=ge2024-01-01` |
| `pair-contained` | `Observation?code=X` | `&_contained=true` |
| `pair-everything` | `Patient?_id=N` (JPA) | `Patient/N/$everything` |
| `pair-sort` | `Condition?code=X&_sort=_lastUpdated` | `&_sort=subject.name` |

The delta within a pair is the measured value of Elasticsearch for that shape. This is the
single most important output of the exercise.

### 6.7 Sort (`HSearchSortHelperImpl.java:57-78`)

HSearch sorts on `STRING`, `TOKEN`, `REFERENCE`, `DATE`, `QUANTITY`, `URI`, `NUMBER`; the
special-name map covers only `PARAM_LASTUPDATED`, `PARAM_ID`, `PARAM_TAG`, `PARAM_SECURITY`,
`PARAM_SOURCE`. Cases: each sortable type ascending and descending (`SortOrderEnum.ASC|DESC`,
`SortOrderEnum.java:23-24`), a chained sort (JPA-only), and a multi-key `SortSpec` chain via
`getAllChainsInOrder()` (`SearchParameterMap.java:993`).

### 6.8 Result and paging parameters

| Parameter | HAPI identifier | Cases |
|---|---|---|
| `_count` | `PARAM_COUNT` (`Constants.java:224`) | 10 / 50 / 200 / 1000 |
| `_offset` | `PARAM_OFFSET` (`:225`) | **HAPI extension, not in R4.** `_offset`+`_count` ⇒ `isOffsetQuery()` (`SearchParameterMap.java:884-886`), bypassing the `Search` cache. Pair against `next`-link paging. |
| `_total` | `PARAM_SEARCH_TOTAL_MODE` (`:315`) | `NONE`, `ESTIMATED`, `ACCURATE` (`SearchTotalModeEnum.java:26-28`) |
| `_summary` | `PARAM_SUMMARY` (`:267`) | `COUNT`, `TEXT`, `DATA`, `TRUE`, `FALSE` (`SummaryEnum.java:33-53`) |
| `_elements` | `PARAM_ELEMENTS` (`:227`) | narrow vs full |
| `_contained` / `_containedType` | `PARAM_CONTAINED` (`:220`), `PARAM_CONTAINED_TYPE` (`:221`) | `SearchContainedModeEnum.FALSE/TRUE/BOTH` (`SearchContainedModeEnum.java:34-45`) |
| `_include` / `_revinclude` | `PARAM_INCLUDE` (`:232`), `PARAM_REVINCLUDE` (`:257`) | plain, `:iterate` (`:242`), `:recurse` (`:240`), wildcard `*` |
| `$everything` | `EverythingModeEnum` (`SearchParameterMap.java:888-895`) | `PATIENT_INSTANCE`, `PATIENT_TYPE`, `ENCOUNTER_INSTANCE`, `ENCOUNTER_TYPE`; note include page size 50 (`JpaStorageSettings.java:187`) |
| `_lastUpdated` | `PARAM_LASTUPDATED` (`:244`) | forward and via `createReverseSearchPredicateLastUpdated` |
| `_list`, `_query`, `_type`, `_language`, `_profile`, `_source`, `_security`, `_tag` | `:269, :255, :346, :236, :252, :266, :261, :268` | one case each |
| `_search` (POST) | `PARAM_SEARCH` (`:260`) | GET vs POST `_search` for a long query string |

**Pre-fetch thresholds.** `DEFAULT_SEARCH_PRE_FETCH_THRESHOLDS = [13, 503, 2003, 1000003, -1]`
(`JpaStorageSettings.java:148`). Paging cases must sample pages that straddle each boundary —
the cost step-change is at the threshold, not at a page number, and reporting "page 40 is slow"
without locating the threshold is useless.

### 6.9 `_filter` (`SearchFilterParser.java:368-393`)

Disabled by default (`myFilterParameterEnabled = false`, `JpaStorageSettings.java:229`). Enabling
it is a `configure` step. All eighteen `CompareOperation` values get a case:
`eq ne co sw ew gt lt ge le pr po ss sb in re ap sa eb`, plus `FilterLogicalOperation`
`and`/`or`/`not` and nested `parameterGroup` (`FilterItemType:395-399`).

### 6.10 Full text and Elasticsearch-native operations

- `_text` (`PARAM_TEXT`, `Constants.java:271`) and `_content` (`PARAM_CONTENT`, `:222`) — hard
  error when full-text is off (`Msg.code(1192)`), so these are gated on the `PE`/`PES` stack
  configurations only.
- `$lastn` — requires `setLastNEnabled` (`JpaStorageSettings.java:269, 590-602`) and routes via
  `executeLastNAgainstIndex` (`SearchBuilder.java:708-722`), which throws `Msg.code(2027)`/
  `Msg.code(2033)` rather than degrade. Separate ES index (`ElasticsearchSvcImpl.java:54-55`).
- ValueSet autocomplete (`ValueSetAutocompleteSearch`) — ES-only, no JPA equivalent.

### 6.11 Stack configurations

| Id | Settings | Isolates |
|---|---|---|
| `P` | advanced indexing off | JPA/PostgreSQL baseline |
| `PE` | `setAdvancedHSearchIndexing(true)` (`JpaStorageSettings.java:2321-2329`) | index-lookup benefit |
| `PES` | `PE` + `setStoreResourceInHSearchIndex(true)` (`:2371-2389`) | payload-fetch benefit — ES returns the body, no PostgreSQL round-trip |

`P→PE` and `PE→PES` deltas are reported separately. `setHibernateSearchIndexSearchParams`
(`:2362`) additionally gates `$lastn`.

---

## 7. Measurement methodology

### 7.1 Per-request record (`shard-<n>.jsonl`)

```json
{"caseId":"pair-include-a","stepId":"warm","cacheLabel":"warm","runId":1,
 "rep":7,"url":"/Condition?code=…","method":"GET",
 "startedAt":"…","wallMs":41.2,"httpStatus":200,
 "rowsReturned":50,"bundleTotal":41203,
 "engineObserved":"elasticsearch","engineExpected":"elasticsearch",
 "esTookMs":12,"pgTotalExecMs":0.0,
 "sharedBlksHit":0,"sharedBlksRead":0,"tempBlksWritten":0,
 "valid":true,"invalidReason":null}
```

### 7.2 Instrumentation sources

- **Client:** `prometheus_client.Histogram("fhir_query_seconds", …, ["case","family","cache","engine","stack"])`, mirroring `loader.py:61-65`. Explicit buckets, not defaults.
- **Server:** HAPI request-id correlation for server-side elapsed time.
- **PostgreSQL:** `pg_stat_statements` delta per case — `calls`, `total_exec_time`,
  `shared_blks_hit`, `shared_blks_read`, `temp_blks_written`. `shared_blks_read/(hit+read)` is the
  cache-state evidence that makes `cacheLabel` a measurement rather than an assertion.
  `EXPLAIN (ANALYZE, BUFFERS)` captured once per case per step.
- **Elasticsearch:** `took` plus per-index query/request cache stats.

### 7.3 Engine attribution — no guessing

`engineObserved` is not inferred from latency. It is determined by the `pg_stat_statements` delta
for the HAPI connection during the request: a query served by Elasticsearch produces a
characteristic PID-fetch pattern and no search-parameter index scan. Where that is ambiguous, the
case is run once with HAPI's SQL logging raised and the plan captured. Ambiguity is never
resolved by assumption; an unattributable measurement is `INVALID`.

### 7.4 Percentiles

p50, p90, p95, p99, max, plus n, from the raw records — not from Prometheus histogram buckets,
which are lossy. Prometheus is for live progress only. Means are not reported.

### 7.5 Binding and selectivity

`FhirDataset` resolves SNOMED roots to a weighted code list stored in the job ConfigMap as
`dataset.json` (`datasets.py:72-114, 117-147`); `expected()` (`:169-186`) gives per-type counts.
Expected cardinality is therefore computable before the query runs.

| Placeholder | Binding rule |
|---|---|
| `{{ commonConditionCode }}` | max-weight entry in `conditionCodes` |
| `{{ rareConditionCode }}` | min-weight entry |
| `{{ commonObservationCode }}` / `{{ rareObservationCode }}` | as above, `observationCodes` |
| `{{ heavyPatientId }}` | serial ≡ 0 mod `shape.heavyEveryN` (`datasets.py:131`) |
| `{{ normalPatientId }}` | any other serial |
| `{{ dateWindow }}` | from the generator's date range |
| `{{ datasetTag }}` | `http://perf.fhir/dataset` \| dataset name (`datasets.py:43`) |

`bindingMode: pin` binds once per step; `rotate` draws a fresh unused value per repetition, which
is how cold-ish measurements get n>1 without a restart per sample. Rotation is drawn from a
seeded PRNG and the seed is recorded, so a run is reproducible.

### 7.6 Cache-state protocol

| Label | Achieved by | Verified by |
|---|---|---|
| `cold` | `restart` step clears `shared_buffers` and the ES/JVM caches; `bindingMode: rotate` avoids re-reading warmed pages | `coldReadRatioFloor` from `pg_stat_statements` |
| `warm` | `prewarm` step over the named index relations | read ratio below floor |
| `hot` | `pin` binding, repeated, with `reuseCachedSearchResultsForMillis=0` | near-100% buffer hit |

**Known limitation, stated not hidden:** restarting the Postgres pod does not clear the *node's*
page cache. Without privileged node access the `cold` label means "cold in PostgreSQL's own
buffers". Three options, requiring a decision: accept and rename the label `cold-shared-buffers`;
add a privileged DaemonSet to drop caches; or size datasets beyond node RAM. Until decided, the
label is `cold-shared-buffers` — **we do not call it cold.**

### 7.7 Mandatory controls, all asserted by the `preflight` step

- `reuseCachedSearchResultsForMillis = 0` (default is 60 s —
  `JpaStorageSettings.java:62, 205, 1223-1244`). Without this the second identical request in a
  minute is served from HAPI's `Search` cache and measures nothing.
- `ANALYZE` completed after load; `last_analyze` per table recorded in the journal.
- JVM warm-up requests discarded (`step.warmup`).
- HikariCP pool size ≥ `concurrency`, recorded.
- `getCountSearchResultsUpTo()` and `getDefaultTotalMode()` recorded
  (`JpaStorageSettings.java:186, 209`) — they change `_total` cost.
- Autovacuum quiescent or its activity recorded.

---

## 8. Work breakdown

TDD per the repository standard; the pure functions carry the logic and need no cluster.

| # | Deliverable | Test-first artefact |
|---|---|---|
| W1 | `benchmark-crd.yaml` — schema, CEL immutability rule (C1), printer columns | schema-validation tests |
| W2 | `catalogue.py` — catalogue loader, revision pinning, coverage assertion against §6 | `test_catalogue.py`, incl. a test that **fails** if any §6 entry is uncovered |
| W3 | `binding.py` — placeholder binder, cardinality predictor, seeded rotation | `test_binding.py` |
| W4 | `benchmark.py` — `BENCHMARK_STEADY` / `BENCHMARK_TEARDOWN` case tables in the existing `Case`/`Decision`/`decide()` pattern (`reconciliation.py:141-300`) | `test_benchmark.py` — pure, table-driven |
| W5 | `control.py` — restart / settle / analyze / prewarm / configure, with before-value capture | `test_control.py` with a fake dynamic client |
| W6 | `runner.py` — worker, per-request records, histogram, shard output | `test_runner.py` against a stub FHIR endpoint |
| W7 | `stats.py` — percentile computation, validity rules, summary | `test_stats.py` |
| W8 | Operator wiring — handlers, RBAC, lease, Prometheus relabel rule | live smoke test |
| W9 | `cohort-core` catalogue — the initial case set satisfying §6 | coverage test from W2 |

W1–W3 have no cluster dependency and can land first.

---

## 9. Acceptance criteria

1. Every entry in §6.4 and §6.5 is reachable by at least one catalogue case, proven by the W2
   coverage test, which fails the build when a case is missing.
2. Every **pair** in §6.6 is present, and each produces a reported delta.
3. A run with a deliberately broken precondition (dataset still loading, stale `ANALYZE`,
   unbindable placeholder, engine mismatch) fails with a named error and produces **no**
   measurements. This is tested explicitly — it is the primary guard against §1 eroding.
4. `status.journal` for a completed run is sufficient to reconstruct what happened without the
   PVC.
5. Killing the operator mid-run yields `phase: Aborted`, never a silently resumed or duplicated
   series (C4).
6. Two `FhirBenchmark` objects targeting one `FhirStack` — the second fails on the lease (C5).
7. `p50/p95/p99` and the cold/warm/hot spread reported per case, per stack configuration
   `P`/`PE`/`PES`.

---

## 10. Out of scope

Write-path and mixed read/write benchmarking; `$graphql`; HFQL; MDM-expanded search beyond
confirming `:mdm` is HSearch-ineligible; subscription matching cost; partitioned/multi-tenant
search; `CoordsPredicateBuilder`/`near` beyond a single smoke case (the synthetic dataset has no
geodata); terminology-expansion cost attribution (§8.4 of the concept note — open decision).

## 11. Open decisions blocking full delivery

1. **Cold-cache method** (§7.6). Until resolved the label is `cold-shared-buffers`.
2. **Terminology expansion** — inside or outside the measured query time. Changes every
   `:below`/`:in` headline number.
3. **Write load** — in or out.
4. **Clinical case list** — the 15–20 real cohort definitions that make the `cohort-core`
   catalogue credible to the CMO.

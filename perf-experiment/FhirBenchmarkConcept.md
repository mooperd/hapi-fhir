# FhirBenchmark — Concept Note

**Audience:** clinical and technical leadership. The first half assumes no knowledge of
databases. The appendix is for engineers.

**Status:** concept, for agreement before any code is written.

**Relationship to existing work:** we already have two building blocks. `FhirStack` stands up a
FHIR server with its database and search engine at a chosen size. `FhirDataset` fills it with a
known, synthetic population. `FhirBenchmark` is the third and final block: it *asks that
population questions* and times the answers.

---

## 1. The problem, in one paragraph

A FHIR server is a clinical database with a standard query language bolted to the front. Every
population health question you care about — "who has uncontrolled diabetes", "which patients had
an eGFR below 30 in the last year", "show me everyone on this care list who has not had a
retinal screen" — becomes a query against that server. Some of those queries return in 40
milliseconds. Some return in 40 seconds. Some never return at all. **Which is which is not
obvious from the clinical question, and is not obvious from the FHIR syntax either.** Two
questions that a clinician would consider equally reasonable can differ in cost by a factor of a
thousand, because of how the server stores and indexes the data underneath. FhirBenchmark exists
to map that terrain before we build products on it.

---

## 2. What "different ways of searching" actually means

FHIR does not have one search mechanism. It has roughly ten, and they have wildly different
costs. Below, each is described by the clinical question it answers rather than its syntax.

### 2.1 "Find me everyone with this diagnosis" — coded lookup

The simplest and most common. You give an exact clinical code; the server returns matching
patients. Cheap, if the code is rare. Expensive if the code is common, because the answer is
enormous and the server has to assemble all of it.

**The important variant:** *"…and everything underneath it in the hierarchy."* Asking for
"Diabetes mellitus" and meaning "and all 200 SNOMED descendants" is a completely different
operation from asking for one code. So is "…and anything in this value set." These force the
server to consult a terminology service, expand a concept tree, and then search for hundreds of
codes at once. This is the single largest cost cliff in clinical querying and it is invisible in
the clinical question.

### 2.2 "Within this time window" — date ranges

Every longitudinal measure has a denominator window. FHIR supports nine different comparison
operators for dates — equals, not-equals, greater than, less than, greater-or-equal,
less-or-equal, *starts-after*, *ends-before*, and *approximately*. The last three exist because
clinical events have duration, not just a timestamp: an encounter that spans a week overlaps a
query window differently from a single lab draw. Range queries are generally efficient, but they
behave badly when combined with sorting, and that combination is exactly what a dashboard does.

### 2.3 "Where the result was above/below a threshold" — numeric and quantity

"HbA1c above 9%." This is two facts that must be true *of the same observation* — the test is an
HbA1c, and its value is above 9. There is a naive way of asking this that the server can satisfy
incorrectly-cheaply (find anything with code HbA1c, separately find anything with value > 9, and
intersect — which will match a patient whose HbA1c is 6 and whose creatinine is 400), and a
correct way (a *composite* search, which ties code and value together in a single indexed fact).
The correct way is also usually the faster way. We will benchmark both, because real-world
client code does both.

Quantities also carry units, and the server can convert between them — 5.4 mmol/L versus
97 mg/dL. Conversion is done at index time into a normalised column, and whether your query hits
the normalised path or the raw path changes its cost.

### 2.4 "Link this person to their events" — references and chaining

Clinical data is a graph: an Observation points at a Patient, which points at an Organization.
Three distinct query shapes come out of this and they are *not* equally priced.

| Shape | Clinical phrasing | Cost |
|---|---|---|
| Direct reference | "all observations for *this* patient" | Cheap |
| Forward chain | "all observations whose patient is named Smith" | Moderate — two-stage |
| Reverse chain (`_has`) | "all patients **who have** an observation of X" | Expensive |
| Deep chain | "observations whose patient belongs to org named Y" | Very expensive |

The reverse chain is the one that matters most to population health, because almost every cohort
definition is phrased that way: *patients who have* a diagnosis, *patients who have* a
prescription, *patients who have not had* a screening. It is also the one the search engine
cannot accelerate (see §3).

### 2.5 "And bring the related records with it" — includes

A cohort list is useless without context. `_include` says "when you return the Condition, also
return the Patient it belongs to." `_revinclude` says the reverse. There is an iterative form
that follows chains repeatedly, and a wildcard form that means "bring everything related,"
which is a loaded gun. There is also `$everything`, a single operation meaning "the entire record
for this patient," which is what a clinician actually wants when they open a chart.

These are cost multipliers rather than costs in their own right: a fast cohort query with a
heavy `_include` becomes a slow query, and — critically — a query that was being served by the
fast search engine falls back to the slow path the moment an `_include` is added.

### 2.6 "Anything mentioning…" — free text

Searching the narrative text of records, or the human-readable label of a code rather than the
code itself. This is the one thing a conventional database is genuinely bad at and a search
engine is genuinely good at. `code:text=headache`, `_content=`, `name:contains=`.

### 2.7 "Show me the ones that are missing" — absence and negation

"Patients with no recorded ethnicity." "Diabetics with no HbA1c in 12 months." Clinically these
are among the most valuable queries — care gaps are absences. Computationally they are the
worst case, because a database is built to find things that exist. Every absence query is a
whole-population scan with a subtraction. We will measure these separately and expect them to be
the slowest family.

### 2.8 "Give me page 40" — paging, sorting and counting

This is the family that ruins production systems, and it is the least visible one.

- **Counting.** "How many patients match?" has three answers available: none, an *estimate*, or
  an *accurate* count. An accurate count on a large cohort can cost more than retrieving the
  first page of results. Dashboards ask for accurate counts by habit.
- **Deep paging.** The first page of results is fast. The fortieth page is not, and the cost is
  not linear — the server fetches results in escalating batches (roughly the first 13, then 503,
  then 2,003, then a million) and each escalation is a step-change in cost. A user clicking
  "next" repeatedly will hit a wall at a specific, predictable page number, and we should know
  which one.
- **Sorting.** Sorting by a clinically meaningful field (date, name, value) is much more
  expensive than returning results in whatever order the database found them, and sorting forces
  the server to consider the whole result set rather than just the first page.
- **Trimming.** Asking for only the fields you need rather than whole resources.

### 2.9 "By identifier" — administrative lookups

NHS number, MRN, resource ID, tag, profile, source. These should be near-instant and are the
control group: if these are slow, something is wrong with the deployment, not the query.

### 2.10 "In one expression" — the `_filter` parameter

A mini query language allowing `and`/`or`/parenthesised logic in a single parameter. **It is
switched off by default in this server** and must be deliberately enabled. Worth benchmarking
because it is the only way to express certain cohort logic, and worth knowing the cost of.

---

## 3. The single most important technical finding

The server we are testing has **two** engines underneath it: a conventional relational database
(PostgreSQL) and a search engine (Elasticsearch). The search engine is dramatically faster for
the things it can do.

**It is all-or-nothing, per query.**

We verified this in the source code. The server examines every single term in your query. If
*any one of them* is something the search engine cannot handle, **the entire query** — including
all the parts the search engine could have accelerated — falls back to the relational database.

Things that disqualify a whole query from the fast path include:

- any use of `_id` or `_count`
- any `_include` or `_revinclude`
- any chained reference (`subject.name=…`)
- any code-hierarchy search (`:below`, `:above`, `:in`, `:not-in`)
- any negation (`:not`) or absence (`:missing`) test
- any date comparison prefix (greater-than, less-than, etc.)
- `$everything`
- sorting by most clinical fields

Read that list against §2 and the implication is stark: **almost every genuinely useful
population health query is disqualified from the fast engine.** The search engine accelerates
free-text search and simple exact-code lookups; the moment you add a date range, a hierarchy, or
a join — which is to say, the moment you write a real cohort definition — you are back on the
relational database.

This is not a criticism of the software; it is a design boundary, and it is entirely reasonable.
But it means **the honest question is not "how much does Elasticsearch help?" but "for which
specific query shapes does Elasticsearch help, and is that set clinically useful?"** Answering
that with evidence is the primary purpose of this benchmark.

Consequently the suite is built in **matched pairs**: the same clinical question asked once in a
form that qualifies for the fast path and once in a form that does not, differing by a single
term. The gap between the pair *is* the value of Elasticsearch, measured rather than assumed.

---

## 4. Cold, warm and hot — why we test the same query three times

If you ask a database the same question twice, the second answer is faster, because the data is
now sitting in memory. This is not cheating — it is how production systems actually behave — but
it means a single number is meaningless without saying which state it was measured in.

We define three states, in plain terms:

| State | What it means | What it represents in real life |
|---|---|---|
| **Cold** | Nothing in memory; every piece of data must be read from disk | A server that has just restarted, or a query so unusual nobody has run it recently |
| **Warm** | The indexes are in memory, the answer is not | The normal steady state of a busy production system |
| **Hot** | This exact query was just run | A dashboard refreshing, or many users asking the same question |

The spread between cold and hot is as important as either number. A query that is 50 ms hot and
3 s cold is a query that will produce sporadic, unreproducible complaints from clinicians, which
is far more corrosive to trust than a query that is consistently slow.

**Two honesty caveats**, both of which need a decision from us:

1. **We must disable the server's own answer cache.** The server remembers search results for
   60 seconds by default and will hand back a stored answer to an identical repeated question
   without touching the database at all. If we leave this on, our "hot" numbers measure nothing
   but the cache, and our repeat measurements are worthless. We will turn it off for
   benchmarking and note that production has it on.
2. **Truly cold is hard to achieve.** Restarting the database clears the database's own memory,
   but the underlying machine keeps a copy of recently-read files in *its* memory, which we
   cannot clear without elevated privileges on the node. There are three options — accept
   "partially cold", obtain node-level privileges, or use a dataset large enough that it cannot
   possibly fit in memory. The third is the most honest and is the reason the datasets are
   sized the way they are. **This needs an explicit decision.**

Crucially, we do not have to *trust* that a cache was cold. PostgreSQL reports, per query, how
many blocks it found in memory versus read from disk. Every measurement will carry that ratio,
so the cache state is evidence in the results rather than an assumption in the method.

---

## 5. What a result looks like

Not an average. Averages hide exactly the behaviour that harms clinicians — the occasional
30-second query that makes someone stop trusting the dashboard.

Every measurement produces:

- **p50** — the typical experience
- **p95 / p99** — the bad experience, which is the one people remember
- **max** — the worst case
- **rows returned** and **cache hit ratio** — so a fast number can be checked for honesty
- **which engine served it** — recorded, not assumed
- **the database's own execution plan** — captured once per query, so a surprising result can be
  explained rather than just reported

Each case is run many times, with a discarded warm-up period (the server is a Java application
and is slow for its first few hundred requests regardless of the query).

A published result is therefore a statement of the form: *"Cohort query F3 — patients with any
descendant of Diabetes mellitus, restricted to the last 24 months, with the patient record
included — at 1.2 million patients, warm, served by PostgreSQL: p50 340 ms, p95 1.9 s, p99 4.4 s,
returning 41,203 rows."* That is a sentence a clinical leader can act on.

---

## 6. What FhirBenchmark is, as a thing

It is a declarative object, in the same style as the `FhirStack` and `FhirDataset` objects we
already have. You describe the benchmark you want; the system makes it true and reports back. You
do not run scripts and you do not babysit it.

```yaml
apiVersion: perf.fhir/v1alpha1
kind: FhirBenchmark
metadata:
  name: cohort-suite-a
spec:
  stackRef:   behemoth          # which server
  datasetRef: mimic-1m          # which population — must be loaded and Ready
  state:      Run               # Run | Hold
  cacheStates: [cold, warm, hot]
  engines:    [postgres, elasticsearch, auto]
  concurrency: 8                # simulated simultaneous users
  repeats:     30               # measurements per case, after warm-up
  warmup:      5
  timeoutSeconds: 120
  suite: cohort-core            # a named, version-controlled query catalogue
status:
  phase:   Running
  progress: "112 / 340 measurements"
  results:
    cohort-descendant-code-24mo:
      warm: {p50ms: 340, p95ms: 1910, p99ms: 4402, rows: 41203, engine: postgres}
      cold: {p50ms: 3120, ...}
```

Key design decisions, in plain terms:

- **The query catalogue is version-controlled, not typed into the object.** A benchmark whose
  questions change between runs cannot be compared between runs. The suite is a named, reviewed
  artefact.
- **Queries are templated against the dataset.** The dataset generator already records which
  clinical codes it used and how frequently. The benchmark binds placeholders — "a common
  condition code", "a rare one", "a patient with a heavy record" — from that record. This means
  **selectivity is known and controlled**, which is the difference between a benchmark and an
  anecdote. Without this, a "fast" result may simply mean we accidentally asked for a code that
  nobody has.
- **Every case declares which engine it expects to use.** If the server routes it elsewhere, that
  is recorded as a finding. This is how we catch the cliff-edge described in §3 in practice
  rather than in theory.
- **It refuses to run against a population that is still loading**, or one whose statistics have
  not been refreshed after loading. A freshly loaded database makes catastrophically bad
  decisions until it has looked at its own data; benchmarking before that point measures a state
  that will never occur in production.
- **It is safe by construction.** Read-only queries, a hard timeout per query, and it will not
  disturb a dataset or stack that is in use.

---

## 7. What we get out of it

1. **A cost map of clinical query shapes** — a table telling a product team which cohort
   definitions are cheap, which are expensive, and which are infeasible at our data volumes.
2. **An evidence-based answer on Elasticsearch** — specifically, the list of query shapes it
   accelerates and by how much, and the list it does not touch, so we can decide whether the
   operational cost of running it is justified.
3. **Hardware sizing with numbers attached** — because `FhirStack` already parameterises CPU and
   memory, the same suite can be run at several sizes and produce a curve rather than a guess.
4. **A regression gate** — the same suite re-run after a HAPI upgrade or a schema change tells us
   whether we got faster or slower, before users find out.
5. **Defensible SLA statements** — p95 figures per query family, at a stated data volume and
   cache state.

---

## 8. Open questions requiring a decision

1. **Cold-cache method** — accept partial cold, obtain privileged node access, or size datasets
   beyond machine memory? (§4)
2. **Which clinical suites do we care about first?** The engineering can cover all ten families
   in §2, but the *specific* queries should be ones you recognise. The most valuable input from
   the clinical side is a list of 15–20 real cohort definitions we should be able to run.
3. **Do we benchmark under write load?** Real systems answer queries while data is arriving.
   That is the realistic case and also roughly doubles the work.
4. **Terminology expansion** — do we count the cost of expanding a SNOMED hierarchy as part of
   the query cost, or exclude it as a separately-cached concern? It materially changes the
   headline numbers for §2.1.

---

---

# Appendix — Technical detail

*Everything below is for engineers. All findings were read from source with comments and
docstrings stripped; line references are to the current working tree.*

## A1. Verified facts about HAPI's search routing

| Fact | Source |
|---|---|
| `ourUnsafeSearchParmeters = {"_id", "_meta", "_count"}` — presence of any disqualifies HSearch | `ExtendedHSearchSearchBuilder.java:63` |
| `canUseHibernateSearch` returns false if **any** param is unsupported (early return in loop) | `ExtendedHSearchSearchBuilder.java:92-128` |
| `isSupportsAllOf` requires: no `_include`, no `_revinclude`, `everythingMode == null`, no delete-expunge, no `near`, `searchContainedMode == FALSE` | `ExtendedHSearchSearchBuilder.java:133-157` |
| Supported modifiers: token → `:text` or bare only; string → `:text`, `:exact`, `:contains`, bare; quantity/date/uri/number → bare only; reference → bare only **and chain must be null**; composite → anything except `:missing` | `ExtendedHSearchSearchBuilder.java:177-235` |
| HSearch-sortable param types: STRING, TOKEN, REFERENCE, DATE, QUANTITY, URI, NUMBER; special param names limited to `_lastUpdated`, `_id`, `_tag`, `_security`, `_source` | `HSearchSortHelperImpl.java:57-78` |
| `_lastUpdated` is special-cased into the HSearch clause list using only one bound | `ExtendedHSearchSearchBuilder.java:336-347` |
| Search pre-fetch thresholds default `[13, 503, 2003, 1000003, -1]` | `JpaStorageSettings.java:148` |
| Search result reuse default = 1 minute | `JpaStorageSettings.java:62, 205` |
| `_filter` disabled by default (`myFilterParameterEnabled = false`) | `JpaStorageSettings.java:229` |
| Combo/unique indexes enabled by default | `JpaStorageSettings.java:183` |
| `$everything` include page size = 50 | `JpaStorageSettings.java:187` |
| `_total` modes: `none`, `estimated`, `accurate` | `SearchTotalModeEnum.java:26-28` |
| `_offset` + `_count` together ⇒ `isOffsetQuery()` ⇒ direct LIMIT/OFFSET, bypassing the search cache | `SearchParameterMap.java:884-886` |
| HSearch toggles: `setAdvancedHSearchIndexing`, `setHibernateSearchIndexSearchParams`, `setStoreResourceInHSearchIndex` | `JpaStorageSettings.java:2321-2389` |
| Postgres-side features present in `QueryStack`: `_has` (`:2468`), reference chain extraction (`:3076`), `_filter` (`:961`), missing-param predicates (`:686-838`), embedded chained resource search (`:1554`, `:2890`) | `QueryStack.java` |

Note: `_offset` is a HAPI extension — it is **not** in the R4 search specification, which
defines paging purely via server-generated `next` links. Benchmarking `_offset` deep paging
versus link-following deep paging is therefore a distinct and worthwhile pair, since they take
different code paths (offset bypasses the `Search` cache entirely).

## A2. Three stack configurations to compare

1. **P** — Elasticsearch absent or advanced indexing off. Everything via JPA/PostgreSQL.
2. **PE** — `advancedHSearchIndexing=true`. Qualifying queries resolved in ES to a PID set, then
   resource bodies fetched from PostgreSQL.
3. **PES** — `PE` plus `storeResourceInHSearchIndex=true`. ES returns the resource body; the
   PostgreSQL round-trip for the payload disappears.

The P→PE delta is the index-lookup benefit; the PE→PES delta is the payload-fetch benefit. They
are separable and should be reported separately.

## A3. Measurement plumbing

- **Client side:** loader-style worker pods, `prometheus_client` `Histogram` named
  `fhir_query_seconds` with labels `{case, family, cache, engine, stack}`, mirroring the existing
  `fhir_load_bundle_seconds` convention in `loader.py:61-65`. Prometheus already scrapes pods
  by label in this namespace (`datasets.py:330-346`); the benchmark pods reuse that mechanism
  with `app=fhir-benchmark`.
- **Server side:** HAPI request timing per request ID, to separate server overhead from DB time.
- **Database side:** `pg_stat_statements` is already in `shared_preload_libraries`
  (`fhir_operator.py:144`). Per case we snapshot `calls`, `total_exec_time`, `shared_blks_hit`,
  `shared_blks_read`, `temp_blks_written`. The hit/read ratio is the objective cache-state
  evidence referenced in §4. `EXPLAIN (ANALYZE, BUFFERS)` captured once per case.
- **Elasticsearch side:** the `took` field and per-index query/request cache statistics.

## A4. Cache-state protocol

| State | Procedure |
|---|---|
| cold | `rollout restart` the postgres deployment (and ES for PE/PES), wait Ready, then **one** measurement per case using a *previously unused* parameter binding |
| warm | after restart, run the full suite once to populate, optionally `pg_prewarm` the relevant indexes, then measure |
| hot | repeat the identical case N times with `reuseCachedSearchResultsForMillis=0` |

Cold is statistically thin by construction — one sample per restart. The practical mitigation is
parameter rotation: draw a fresh, unseen code/patient/date-window from the dataset manifest on
every repetition so that each measurement touches unvisited pages without a restart. This gives
"effectively cold for this data" with N=30 at the cost of some contamination of the shared
buffer pool. Both methods should be run and reported separately rather than blended.

Mandatory controls before any measurement is accepted:

- `reuseCachedSearchResultsForMillis = 0`
- `ANALYZE` completed after dataset load; record `last_analyze` per table in the result
- JVM warm-up discarded (`spec.warmup`)
- HikariCP pool size ≥ `spec.concurrency`, recorded in the result
- autovacuum quiescent, or its activity recorded

## A5. Suite taxonomy

Cases are identified `<family>-<shape>-<selectivity>`, e.g. `token-below-common`. Families map
1:1 to §2: `token`, `date`, `quantity`, `composite`, `reference`, `chain`, `has`, `include`,
`text`, `absence`, `paging`, `sort`, `total`, `admin`, `filter`, `everything`.

Every family carries at least one **matched pair** differing by exactly one term, where one
member qualifies for HSearch and the other does not. Example pair:

- `token-exact-common-ES`: `Condition?code=http://snomed.info/sct|{{common}}&_sort=_lastUpdated`
- `token-exact-common-PG`: identical plus `&_include=Condition:subject`

Both return the same cohort; only the second is disqualified. The delta is the finding.

## A6. Templating against the dataset

`FhirDataset` resolves SNOMED roots into a weighted code list and stores it in the job ConfigMap
as `dataset.json` (`datasets.py:117-147`, `resolve()` at `:72-114`). Weights are the share of
resources carrying each code, so expected result-set cardinality is computable *before* the query
runs. `expected()` (`datasets.py:169-186`) already yields resource counts per type. Benchmark
placeholders bind from these:

| Placeholder | Binding |
|---|---|
| `{{ commonConditionCode }}` | highest-weight entry in `conditionCodes` |
| `{{ rareConditionCode }}` | lowest-weight entry |
| `{{ heavyPatientId }}` | a patient on the `heavyEveryN` boundary (~500 observations) |
| `{{ normalPatientId }}` | a non-heavy patient (~53 observations) |
| `{{ dateWindow }}` | derived from the generator's date range |
| `{{ datasetTag }}` | `http://perf.fhir/dataset` \| dataset name (`datasets.py:43`) |

Every result therefore carries a *predicted* and an *observed* row count. A mismatch invalidates
the measurement and is reported as such.

## A7. Operator implementation shape

Follows existing conventions exactly — `kopf`, group `perf.fhir`, version `v1alpha1`, plural
`fhirbenchmarks`, status subresource, printer columns, a timer-driven reconcile, and an Indexed
`Job` with `parallelism = spec.concurrency` writing to a results PVC.

Teardown and steady-state logic uses the existing declarative case-table pattern in
`reconciliation.py` (`Case`/`Decision`/`decide()` at `:141-300`) — a new `BENCHMARK_STEADY` and
`BENCHMARK_TEARDOWN` table rather than imperative branching, so every state transition remains
independently testable.

Per the repository's TDD requirement, the case tables and the query-template binder are written
test-first in `test_benchmark.py` before the kopf handlers exist; both are pure functions over
facts and need no cluster.

## A8. Out of scope for v1

Write-path benchmarking, `$graphql`, `$lastn`, HFQL, MDM-expanded search (`:mdm`), subscription
matching cost, and multi-tenant/partitioned search. Each is a legitimate follow-on; none belongs
in the first cost map.

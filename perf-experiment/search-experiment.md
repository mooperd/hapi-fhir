# Search experiment: quantifying chained and reverse-chained search cost on HAPI FHIR

## Purpose and the decision it feeds

We are building population-health cohorting. Cohort definitions are naturally expressed as chained
and reverse-chained FHIR searches — "patients with condition X who also have observation Y". We need
to know whether HAPI can serve those queries directly, and at what dataset size that stops being
true.

Score every result against these bands, agreed before the first measurement:

| Band | Time | Consequence for the platform |
|------|------|------------------------------|
| Interactive | < 2 s | Cohort builder can query live. |
| Tolerable | 2–10 s | Live with a progress indicator; needs a concurrency test before we rely on it. |
| Async only | 10–300 s | Cohort definitions become submitted jobs, not live queries. Significant product change. |
| Unusable | > 300 s | Needs a denormalised cohort store. Weeks of build plus ongoing sync. |

The experiment is worth running precisely because the bottom row is expensive to be wrong about in
either direction.

This document specifies the measurement. It does not contain the harness code.

## Implementation

The eight cases, both modes, the no-cache header and the 300 s cap are implemented by
[app.py](app.py) (`CASES`, `MODES`, `run_bench`) and driven from the **Benchmark** page. The app
covers the rare-selectivity pass at one tier. The common-selectivity pass, the `page3` mode, the
tier sweep and the `EXPLAIN` capture described below are still manual.

## Dataset

As specified in [data-import.md](data-import.md): tiers S (10k patients / 1.18M resources),
M (100k / 11.8M), L (300k / 35.0M). Synthetic, seeded, three code-selectivity tiers (20% / 5% / 1%
of rows). Run against the synthetic data, not the MIMIC copy — MIMIC gives us a realism check but
its result-set sizes are not known in advance, so it cannot attribute a slow query to a cause.

Take one MIMIC reference measurement per case at the end as a sanity check on the synthetic shape.

## What HAPI actually does, and why the direction matters

Read this before designing a query, because it dictates which measurements are meaningful.

**Both chain directions compile into one flat SQL statement.** A forward chain
(`Observation?subject:Patient.gender=female`) is handled by `addPredicateReferenceWithChain`
([ResourceLinkPredicateBuilder.java:583](../hapi-fhir-jpaserver-base/src/main/java/ca/uhn/fhir/jpa/search/builder/predicate/ResourceLinkPredicateBuilder.java#L583)),
a reverse chain by `createPredicateHas`
([QueryStack.java:1135](../hapi-fhir-jpaserver-base/src/main/java/ca/uhn/fhir/jpa/search/builder/QueryStack.java#L1135)).
Each hop recurses into `searchForIdsWithAndOr` and adds a join on `HFJ_RES_LINK` plus a join on the
relevant `HFJ_SPIDX_*` table to the same builder. There is no subselect and no intermediate
materialisation — the planner decides the join order, so query cost is a Postgres planning outcome,
not a HAPI code path. That is why `EXPLAIN ANALYZE` is a required output and not a nice-to-have.

**The two directions hit different indexes, deliberately.** From
[ResourceLink.java:57-69](../hapi-fhir-jpaserver-model/src/main/java/ca/uhn/fhir/jpa/model/entity/ResourceLink.java#L57-L69):

- `IDX_RL_SRC` on `(SRC_RESOURCE_ID)` — narrow, for source→target. Rows for one source are written
  together at ingestion, so they sit in the same blocks.
- `IDX_RL_TGT_v2` on `(TARGET_RESOURCE_ID, SRC_PATH, SRC_RESOURCE_ID, TARGET_RESOURCE_TYPE,
  PARTITION_ID)` — wide and covering, for target→source, because "targets will usually be randomly
  distributed - each row in separate block".

Reverse chaining walks target→source. The index is covering to compensate, but the rows it points at
are scattered. That is the mechanism we expect to find at the bottom of the bottleneck, and the
`BUFFERS` output of `EXPLAIN` is what will confirm or refute it.

`HFJ_SPIDX_TOKEN` mirrors the same idea: `IDX_SP_TOKEN_HASH_V2` for driving from a code value,
`IDX_SP_TOKEN_RESID_V2` on `(RES_ID, HASH_*)` for driving from an already-known resource set
([ResourceIndexedSearchParamToken.java:68-75](../hapi-fhir-jpaserver-model/src/main/java/ca/uhn/fhir/jpa/model/entity/ResourceIndexedSearchParamToken.java#L68-L75)).
Which of the two gets used is the planner's call. Capture it.

**Unqualified chains fan out.** `subject.gender=female` expands to every target type `subject` can
point at, each contributing its own predicate; HAPI fires a `JPA_PERFTRACE_WARNING` when there is
more than one candidate
([ResourceLinkPredicateBuilder.java:715](../hapi-fhir-jpaserver-base/src/main/java/ca/uhn/fhir/jpa/search/builder/predicate/ResourceLinkPredicateBuilder.java#L715)).
`subject:Patient.gender=female` does not. We measure both, because application code writes the
unqualified form by default and the difference is free to fix.

There is no chain-performance guidance in
[server_jpa/performance.md](../hapi-fhir-docs/src/main/resources/ca/uhn/hapi/fhir/docs/server_jpa/performance.md).
That absence is part of why we are measuring.

## Measurement rules

Three HAPI behaviours will silently invalidate a naive benchmark. All three must be handled.

**1. Search results are cached for 60 s by default.**
`DEFAULT_REUSE_CACHED_SEARCH_RESULTS_FOR_MILLIS` is one minute
([JpaStorageSettings.java:62](../hapi-fhir-jpaserver-model/src/main/java/ca/uhn/fhir/jpa/api/config/JpaStorageSettings.java#L62)).
Repetitions 2–5 of an identical query would be cache hits measuring an `HFJ_SEARCH` lookup.

→ Send `Cache-Control: no-cache` on every request. Verify it worked: the returned `Bundle.id` must
differ between repetitions. If it does not, the header was ignored and the run is void.

**2. A first page only materialises 13 rows.**
Prefetch thresholds default to `[13, 503, 2003, 1000003, -1]`
([JpaStorageSettings.java:148](../hapi-fhir-jpaserver-model/src/main/java/ca/uhn/fhir/jpa/api/config/JpaStorageSettings.java#L148)).
Timing `?_count=10` against a query matching two million rows measures the cost of finding thirteen
of them. That is a real user-facing number, but it is not the cost of the cohort.

→ Measure time-to-first-page and time-to-total as separate modes, and never quote one for the other.

**3. `_offset` switches the server into synchronous mode.**
Any query with `_offset` bypasses the search cache and the prefetch machinery entirely and runs as a
single bounded query
([SearchCoordinatorSvcImpl.java:477](../hapi-fhir-jpaserver-base/src/main/java/ca/uhn/fhir/jpa/search/SearchCoordinatorSvcImpl.java#L477)),
capped by `DEFAULT_INTERNAL_SYNCHRONOUS_SEARCH_SIZE` = 10,000
([JpaStorageSettings.java:124](../hapi-fhir-jpaserver-model/src/main/java/ca/uhn/fhir/jpa/api/config/JpaStorageSettings.java#L124)).

→ Use this as the clean "find me 50 matches" measurement. It is the least noisy number in the set.

### Modes

| Mode | Request suffix | Measures |
|------|----------------|----------|
| `first-page` | `&_offset=0&_count=50` | Synchronous, `LIMIT`-bounded. Time to find 50 matches. |
| `count` | `&_summary=count` | Full predicate evaluation over the whole match set. No resource loading. |
| `page3` | default paging, follow `Bundle.link[next]` twice | Crosses the 503-row prefetch boundary. Exposes the paging cliff. |

## Query matrix

Eight cases. `{C}` = Condition code, `{O}` = Observation code, both with system prefix
(`http://perf.pkb/cond|COND-RARE-17`).

| ID | Shape | Query |
|----|-------|-------|
| A | Baseline, no chain | `Observation?code={O}` |
| B | Forward chain, qualified | `Observation?code={O}&subject:Patient.gender=female` |
| C | Forward chain, unqualified | `Observation?code={O}&subject.gender=female` |
| D | Reverse chain, one `_has` | `Patient?_has:Condition:subject:code={C}` |
| E | Reverse chain, two `_has` — **the cohort shape** | `Patient?_has:Condition:subject:code={C}&_has:Observation:subject:code={O}` |
| F | Case E plus a selective Patient anchor | `Patient?_has:Condition:subject:code={C}&_has:Observation:subject:code={O}&gender=female&birthdate=lt1960-01-01` |
| G | Same question, opposite direction: chain-then-has | `Condition?code={C}&subject:Patient._has:Observation:subject:code={O}` |
| H | Reverse chain on a date, not a token | `Patient?_has:Observation:subject:date=ge2024-01-01` |

What each pair tells us:

- **A → B** the marginal cost of one forward hop.
- **B vs C** the cost of not qualifying the chain. Expect a `JPA_PERFTRACE_WARNING`; if C is not
  materially worse, that is a useful negative result and we stop telling people to qualify.
- **D → E** whether AND-ing a second `_has` is additive or multiplicative. This is the single most
  important number in the experiment.
- **E vs F** whether a selective Patient-level predicate lets the planner start from a small set and
  rescue the query. If it does, cohort definitions get a mandatory anchor and the product problem
  mostly goes away.
- **E vs G** same clinical question, two directions, different indexes. If G is much faster, the
  cohorting layer rewrites its queries and we have a cheap fix.
- **H** whether reverse chaining onto a date index behaves like a token index. Cohorts are usually
  time-bounded, so this is not academic.

### Selectivity

Run every case at two code tiers from the dataset:

- **rare** (1% of rows) — at L: ~57k Conditions, ~293k Observations per code
- **common** (20% of rows) — at L: ~1.13M Conditions, ~5.86M Observations per code

16 case/selectivity variants. Expected result counts are known from the generator spec, so any
mismatch is a bug to fix before the timing means anything.

## Protocol

1. Confirm the server config matches the measurement config, not the ingestion config
   (see [data-import.md](data-import.md), "Load mechanism"). Record it verbatim in the results
   directory.
2. Confirm no other traffic against the server. Record the run window in UTC.
3. Single client, one request at a time. Concurrency is phase 2 and out of scope — mixing it in now
   means we cannot tell a slow query from a contended one.
4. For each variant: modes `first-page` and `count`, 5 repetitions each.
5. Mode `page3` on cases A, E and G only, rare selectivity only, 5 repetitions. 6 extra variants,
   because paging behaviour is worth one data point per direction and no more.
6. Per-request timeout **300 s**. On timeout, record `censored=true` with `elapsed_ms=300000` and
   move on. No retries — a retry measures a warm cache.
7. Order: tier S in full, then M in full, then L. At L, run only the variants that came in under
   60 s at M, plus cases E, F and G regardless. Note the exclusions.
8. Report **median and max** of repetitions 2–5, and repetition 1 separately. Not the mean — the
   distribution has a tail and the mean hides it. Do not attempt to clear the Postgres buffer cache;
   on a shared cluster we cannot, and trying would disrupt other users. Repetition 1 is our
   cold-ish proxy and should be labelled as such.

## What to record

One CSV, one row per HTTP request, appended as the run proceeds so a crash does not lose the run:

```
run_id, timestamp_utc, tier, seed, case_id, selectivity, mode, rep,
url, http_status, elapsed_ms, bundle_total, returned_entries, bundle_id,
censored, server_config_hash, notes
```

`bundle_id` is there to prove the no-cache header worked. `bundle_total` is there to prove the query
matched what the generator says it should.

## Evidence: the SQL, not just the stopwatch

Wall-clock alone will not tell anyone why a query is slow, and "chained searches are slow" is not an
actionable finding.

For each of the 8 cases at rare selectivity on tier L:

1. Capture the generated SQL. Either register `PerformanceTracingLoggingInterceptor`
   ([hapi-fhir-jpaserver-base/.../interceptor/](../hapi-fhir-jpaserver-base/src/main/java/ca/uhn/fhir/jpa/interceptor/PerformanceTracingLoggingInterceptor.java))
   and read the `JPA_PERFTRACE_RAW_SQL` output
   ([Pointcut.java:3452](../hapi-fhir-base/src/main/java/ca/uhn/fhir/interceptor/api/Pointcut.java#L3452)),
   or pull it from Hibernate SQL logging. The interceptor is preferable — it gives bound parameter
   values.
2. Run `EXPLAIN (ANALYZE, BUFFERS)` on that SQL directly against Postgres (`localhost:5433`).
3. Save the plan next to the CSV, one file per case.

From each plan extract: join order, which `HFJ_RES_LINK` and `HFJ_SPIDX_*` indexes were chosen, rows
estimated vs actual at each node, and shared block hits vs reads. A large estimate/actual divergence
means the fix is statistics or a combo search parameter, not architecture — a materially different
and much cheaper conclusion.

Also record any `JPA_PERFTRACE_WARNING` the server emits during the run.

## Environment

`kubectl port-forward` is not a reliable transport for multi-minute requests; it drops long-lived
connections and you will record timeouts that are the tunnel's fault, not the server's. Either run
the timing client inside the cluster, or treat every connection reset as a void measurement and
re-run it — never as a data point. Log resets separately from genuine timeouts; if resets exceed a
few percent of requests, move the client in-cluster and rerun the affected tier.

Record: HAPI version, Postgres version, pod CPU/memory limits, Postgres `shared_buffers` and
`work_mem`, and whether the database is partitioned. Any of these changing invalidates comparison
across tiers.

## Out of scope

Concurrency and throughput; `_include`/`_revinclude`; `_sort` on chained results; combo search
parameters as a mitigation; Elasticsearch-backed `:text` and `_content` search; three-hop chains
(the dataset has only one hop by design). Each is a follow-up with its own dataset requirement.

## Cost

Roughly 1.5–3 engineer-days: build the two scripts, load S and M (hours), load L (unattended,
overnight), run the matrix, pull the plans. Cluster cost is marginal — existing test cluster, plus
~35M resources of storage at L, which we can drop afterwards.

Against that: if the answer is the bottom band, the alternative is a denormalised cohort store, which
is weeks of build plus permanent sync and IG overhead. If the answer is band 1 or 2, or if E-vs-G or
E-vs-F shows a query-rewrite fix, we avoid that build entirely. Three days to de-risk a multi-week
decision is the right trade, and it is worth running S and M first so that if the curve is already
flat we can stop early.

## Reporting

One results table: rows = 16 case/selectivity variants, columns = tier × mode, cells = median ms with
max in brackets, censored values marked. Then the 8 plan summaries. Then the band assignment per
case, and the named owner of each follow-up.

Nothing from this experiment goes to a customer or partner without a human reading it first.

# FHIR Benchmark — Planned Changes

**Date:** 17 September 2026
**Author:** Andrew Holway
**For:** Medical Director
**Subject:** Why the 17 September benchmark run produced no timings, and what we are changing

---

## Bottom line

The benchmark run on 17 September rejected 74 of its 149 measurements and stopped early. I have
investigated this against the live system.

Three things you should take from this:

1. **The database upgrade is not the cause.** I had assumed the recent PostgreSQL 18.6 upgrade
   was responsible. It is not. A run from two days earlier failed in exactly the same way, on
   exactly the same 74 tests.
2. **Nothing is broken clinically.** The server is answering queries correctly and quickly. The
   failures are the benchmark's own safety checks refusing to record a number it cannot vouch
   for. That is the harness working as designed, not a fault in the FHIR server.
3. **The more serious finding is the opposite of a failure.** Five of the tests that *passed*
   recorded the wrong answer about which engine served the query. A wrong pass is worse than a
   flagged failure, because nothing draws your attention to it. This is the main reason we are
   making changes rather than simply re-running.

---

## What this benchmark is for

We run two different search engines behind the same FHIR interface:

- **PostgreSQL** — the relational database. Handles complex, relationship-heavy queries: "find
  patients who have a Condition recorded by this clinician and pull their Observations too."
- **Elasticsearch** — a text and index search engine. Handles high-volume lookups: exact codes,
  name fragments, free-text.

The FHIR server decides which one answers each query. That decision is invisible to the calling
clinician or application, and it is the single biggest factor in whether a query takes 20
milliseconds or 20 seconds.

The benchmark asks 149 representative clinical queries against a synthetic dataset of 10,000
patients (189,000 Conditions, 977,000 Observations, 1.18 million records) and records, for each
one: how long it took, how many rows came back, and **which engine actually served it**.

That last item is the point of the exercise. A timing with no engine attached tells us nothing
actionable.

---

## What happened on 17 September

The run reached step 6 of 11 and stopped. Of 149 queries:

- **75** were measured cleanly
- **74** were rejected, for six distinct reasons

The harness is deliberately strict: if it cannot prove which engine served a query, or cannot
reconcile the number of rows returned against what it predicted, it discards the measurement
rather than record something it cannot defend. It then fails the whole step, because a
percentile computed from half a dataset is misleading.

---

## The database upgrade is not the cause

I recovered the stored results from the previous run, on 15 September, and compared them.

| | 15 September | 17 September |
|---|---|---|
| Measurements rejected | 74 of 149 | 74 of 149 |
| Wrong engine reported | 31 | 31 |
| Query refused by server (400) | 27 | 27 |
| Engine could not be determined | 8 | 8 |
| Feature not enabled (405) | 4 | 4 |
| Feature not found (404) | 2 | 2 |
| Row count not reconciled | 2 | 2 |

Not merely the same totals — the same individual tests, in every category, with no additions and
no removals.

I also confirmed the upgraded database is healthy and its measurement instrumentation is
working correctly under the new version.

**Conclusion: these faults predate the upgrade and were simply not investigated before now.** The
upgrade is clean and should stand.

---

## Why the 74 were rejected

Five unrelated causes, none of them alarming individually.

**1. A server feature is switched off (22 tests).**
FHIR's `_filter` parameter — the syntax for expressing compound queries such as *"active
Conditions recorded after 2020 excluding a given code"* — is disabled in our server
configuration. Every test that uses it is refused. This is a one-line configuration change.

**2. Some tests ask for things that do not exist (9 tests).**
The test catalogue contains queries that the FHIR specification or our server simply does not
support — an approximate-date comparator that HAPI does not implement, an ambiguous sort
instruction, search parameters that are not registered. These tests have never been capable of
passing. They should be removed or corrected rather than counted as failures.

**3. No clinical terminology is loaded (5 tests).**
The database contains no SNOMED CT concept hierarchy. Every table that would hold it is empty.

This one has clinical significance beyond the benchmark. Hierarchical search — asking for *"any
form of diabetes"* and having the server expand that to every descendant code — is how a
clinician actually wants to query. Right now those queries return zero results **silently**,
without an error. The benchmark caught it; a clinical user would not have.

**4. The server was not asked for a row count (2 tests).**
FHIR servers omit the total result count unless explicitly asked. The benchmark predicts how
many rows a query should return and checks the answer — a guard against a query that runs fast
because it quietly returned nothing. Two tests forgot to request the total. When I asked for it
directly, the server returned 205 rows against a predicted range of 134–244. The check would
have passed.

**5. The benchmark's expectations are out of date (30 tests).**
The server is configured with advanced Elasticsearch indexing enabled. That means date
comparisons, quantity comparisons, sorting by identifier and result paging are all served by
Elasticsearch — but the benchmark still expects PostgreSQL to serve them, because the catalogue
was written before that configuration was adopted.

Here the benchmark is right and its own expectations are wrong. The server has not
misbehaved; our record of how it is configured has drifted.

---

## The finding that actually matters

Two problems with how the benchmark determines which engine served a query. Neither appeared in
the failure list, because both produce confident wrong answers rather than errors.

**Queries that pull in related records are mis-attributed.**
When a query asks for related resources — "give me these Conditions *and* the Patients they
belong to" — that second step always reads a relational table, regardless of which engine
performed the actual search. The benchmark sees that relational activity and concludes
PostgreSQL served the query.

I verified against Elasticsearch's own request counter that these searches are in fact served by
Elasticsearch. **Five tests are currently recording the wrong engine and passing.** Any
conclusion drawn about PostgreSQL's performance on related-record queries would be wrong.

**Background activity contaminates the measurement.**
The counter the benchmark reads is server-wide, not per-query. Over three minutes of watching an
idle system with no traffic at all, the FHIR server's own housekeeping tasks generated database
activity that the benchmark would have counted as evidence of a PostgreSQL search.

Both of these arise from the same design decision: the benchmark infers the engine indirectly,
from PostgreSQL activity alone. Absence of PostgreSQL activity is treated as proof of
Elasticsearch — but it is equally consistent with nothing having happened. Elasticsearch keeps
its own request counter, which is direct positive evidence for either engine. We are not
currently using it.

---

## What we are going to change

| # | Change | Tests recovered | Basis |
|---|---|---|---|
| 1 | Update the catalogue's expected engine to match how the server is actually configured | 30 | Verified against live system |
| 2 | Enable the `_filter` parameter on the server | 18 | Inferred from server responses |
| 3 | Remove or correct tests that request unsupported behaviour | 9 | Verified |
| 4 | Enable identifier-type and contained-resource search | 3 | Inferred from server responses |
| 5 | Request an explicit row count where the test predicts one | 2 | Verified |
| 6 | Load a SNOMED CT code system | 2, plus restores 3 hierarchy tests to meaning | Verified |
| 7 | **Use Elasticsearch's own request counter as the primary evidence of which engine served a query** | Corrects `_id` lookups, 6 empty-result queries, and the 5 silently-wrong passes | Verified |

Items 1–6 are configuration, test-catalogue and data changes. Item 7 is a change to the
measurement harness itself and is the one that matters for the integrity of every future run.

Two entries are marked *inferred*: the server tells us the feature is disabled, so I have not
been able to confirm the tests pass once it is enabled without changing the running system. I
did not make that change, as it affects a shared environment.

---

## Expected outcome

- Configuration and catalogue changes alone: **74 rejected → approximately 7**
- Adding the terminology load and the harness change: **74 → approximately 2**

The two that remain are the hierarchy searches, which cannot be meaningfully tested until a
terminology is loaded and indexed. They should be marked as pending rather than failing.

We will also gain something we do not have today: a trustworthy statement of which engine serves
each class of clinical query. That is what the exercise is for.

---

## What this does not change

- No clinical system is affected. This environment holds synthetic generated data only.
- No conclusion has yet been drawn from benchmark data, so nothing published or decided needs
  revisiting.
- The PostgreSQL 18.6 upgrade stands. It is working correctly and was not implicated.

---

## Appendix — evidence

All findings below were reproduced against the running system on 17 September, not inferred from
documentation or source comments.

- **Prior-run comparison:** stored results from run `all-20260915-095130`, step `cold`, compared
  test-by-test against `all-20260917-055226`. Identical rejection set.
- **Database health:** PostgreSQL 18.6, statement-tracking extension version 1.12 loaded and
  active, 439 statements tracked, query text fully visible to the benchmark's account.
- **Full replay:** all 149 queries re-issued against the live server using the harness's own
  attribution logic. The run reproduced case-for-case.
- **Engine verification:** Elasticsearch request counters sampled before and after each query.
  Date, quantity, paging, sort and composite queries each raised the counter, confirming
  Elasticsearch service. Identifier lookups and negation queries did not, confirming PostgreSQL.
- **Mis-attribution:** two queries with an identical search clause, differing only by a
  related-record request, both raised the Elasticsearch counter; only the second was recorded as
  PostgreSQL.
- **Background contamination:** 180-second idle observation with no benchmark traffic recorded
  three housekeeping queries against a table the harness treats as proof of a PostgreSQL search.
- **Terminology:** concept, code-system and value-set tables all confirmed empty.
- **Row count:** an explicit count request returned 205 against a predicted band of 134–244;
  independently confirmed as 205 by direct query against the index.
- **Query syntax:** the server parses `_filter` before checking whether it is enabled, which
  allowed the syntax of all 22 affected tests to be validated without altering the running
  configuration. Eighteen are syntactically valid; four request operators that do not exist.

---

*Prepared with assistance from Claude Opus 5. All findings independently reproduced against the
live environment.*

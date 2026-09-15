# SOW: FhirDataset / FhirStack reconciliation cases

Status: proposed
Scope: walking skeleton, `perf-experiment/operator`
Audit basis: all line citations derived from a comment- and docstring-stripped mirror of
`app.py`, `adopt.py`, `datasets.py`, `fhir_operator.py`, `loader.py`, `ui.py` and the three
test scripts. Every file verified `comments=0 docstrings=0` before reading.

## 1. Problem

Deleting a `FhirDataset` can hang forever, and deleting a `FhirStack` can strand every
dataset in its namespace. Observed on 2026-09-15: `meow0` and `meow0-1` sat in Terminating
holding `perf.fhir/finalizer`, with a purge Job looping on

    waiting for http://hapi-fhir.meow0.svc:8080/fhir -- ConnectionError: ... Failed to resolve

because the `FhirStack` that provided that Service had been deleted seconds earlier. At the
same time `meow0-1-load-0` was still Running with four pods writing into the same
(now absent) server, because garbage collection cannot remove a Job whose owner is itself
blocked on a finalizer.

Two root causes, both in code:

1. `purge()` at `datasets.py:552-592` has no precondition. It launches a delete Job
   regardless of whether a server exists, and blocks on `kopf.TemporaryError` until that Job
   reports `Complete`, which can never happen.
2. `guard()` at `fhir_operator.py:272-292` checks `spec.protected` and nothing else. It never
   looks at the datasets whose server it is about to destroy.

## 2. Principles

These are binding on the implementation.

- **No fallback logic.** There is no "try something else if that failed" branch anywhere. A
  decision is made from observed facts or it is not made at all.
- **If it fails, let it fail.** Kubernetes API errors are not caught. They propagate out of
  the handler, kopf logs them with a traceback, and the reconcile retries on its own schedule.
  The single exception is a `404` on a read used as an existence probe: that `404` *is* the
  observation ("no purge Job exists"), and is converted into a fact. Every other status code
  propagates untouched.
- **No silent paths.** Every decision carries a human-readable reason string that is written
  to both `status.reason` and the operator log. A decision with no reason is a bug.
- **No default case.** If no case matches the observed facts, `decide()` raises. An
  unmatched combination is a hole in the table and must be visible as a crash, not absorbed
  by an `else`.
- **Minimum viable.** This is a benchmarking rig. The skeleton handles the cases below and
  nothing more.

## 3. Deliverable: `perf-experiment/operator/reconciliation.py`

One new file. All reconciliation cases live in it and nowhere else, so the whole decision
surface can be read top to bottom in a single sitting.

### 3.1 Layout

    SECTION 1  Vocabulary      Enums: StackState, EndpointState, Action
    SECTION 2  Facts           Three frozen dataclasses, pure data, no clients
    SECTION 3  Case tables     DATASET_TEARDOWN, STACK_TEARDOWN, DATASET_STEADY
    SECTION 4  decide()        First match wins. No default. Raises on no match.
    SECTION 5  Observation     The only I/O in the file. Builds Facts from the cluster.
    SECTION 6  Effects         stop_jobs(), start_purge(), delete_datasets()

Sections 1-4 are pure and are the unit-testable core. Section 5 is the only place that talks
to Kubernetes. Section 6 performs the actions the table selects.

### 3.2 Vocabulary

    StackState     ABSENT | TERMINATING | NOT_READY | READY
    EndpointState  SERVING | PRESENT_NOT_SERVING | ABSENT
    Action         RELEASE | STOP_JOBS | START_PURGE | WAIT | FAIL
                   PROCEED | DELETE_DATASETS

`StackState` describes the `FhirStack` custom resource — the declaration.
`EndpointState` describes the running `hapi-fhir` Service and its Endpoints — the thing
declared. They are separate axes because they desync: the CR can be gone while Deployments
still run, and the CR can read `Ready` while `hapi-fhir` is mid-rollout and not serving.

### 3.3 Case shape

    @dataclass(frozen=True)
    class Case:
        id: str                      # stable identifier, quoted in logs and tests
        when: Callable[[Facts], bool]
        action: Action
        reason: Callable[[Facts], str]

A table is an ordered tuple of `Case`. `decide(table, facts)` walks it, returns
`(case.id, case.action, case.reason(facts))` on the first `when` that returns true, and
raises `RuntimeError` listing the facts if none do.

## 4. Case tables

### 4.1 DATASET_TEARDOWN — the dataset CR is Terminating

Facts:

    namespace_terminating: bool
    purge_on_delete:       bool          # spec.purgeOnDelete, default True
    stack:                 StackState    # of (spec.stackRef or namespace)
    endpoint:              EndpointState # of hapi-fhir in the dataset's namespace
    load_jobs:             tuple[str]    # Job names with mode != delete
    load_pods:             int           # pods for those jobs, any phase
    purge_job:             str | None    # None | Active | Complete | Failed | Paused

| # | id | when | action |
|---|----|------|--------|
| 1 | `namespace-terminating` | `namespace_terminating` | RELEASE |
| 2 | `purge-disabled` | `not purge_on_delete` | RELEASE |
| 3 | `server-gone-with-stack` | `endpoint != SERVING and stack in (ABSENT, TERMINATING)` | RELEASE |
| 4 | `load-jobs-running` | `load_jobs` | STOP_JOBS |
| 5 | `load-pods-draining` | `load_pods > 0` | WAIT |
| 6 | `endpoint-not-serving` | `endpoint != SERVING` | WAIT |
| 7 | `purge-absent` | `purge_job is None` | START_PURGE |
| 8 | `purge-active` | `purge_job == "Active"` | WAIT |
| 9 | `purge-paused` | `purge_job == "Paused"` | FAIL |
| 10 | `purge-failed` | `purge_job == "Failed"` | FAIL |
| 11 | `purge-complete` | `purge_job == "Complete"` | RELEASE |

Notes on individual cases:

**Case 1** is the highest-priority case and does not exist in the code today. A dataset
finalizer inside a Terminating namespace will hold that *namespace* in Terminating
indefinitely — the worst failure in the set, because it blocks everything else in the
namespace. The data is being destroyed with the namespace, so purging is pointless.

**Case 2** preserves the behaviour already at `datasets.py:565-567` and is the documented
manual escape hatch (see Decision 2).

**Case 3** is the `meow0` fix. If the stack is gone or going and the endpoint is not serving,
there is no server to purge from and none is coming. Release immediately with a reason naming
both facts. Note the condition is `endpoint != SERVING`, not `endpoint == ABSENT`, so a
leftover Service object with no ready Endpoints does not trap the dataset.

**Case 4** is the ordering the current code lacks entirely. Stopping the loader before
purging is mandatory: `run_delete` in `loader.py:435-480` polls each resource type's count
down to zero, and a live loader re-creating tagged Patients means that count may never reach
zero. The Job then burns `DELETE_TIMEOUT` (7200s default) and fails. This deadlock occurs
even against a completely healthy stack.

**Case 5** waits for the pods to actually go after the Jobs are deleted. Deleting a Job is
not instantaneous and a purge started against a draining loader has the same problem as
case 4.

**Case 6** covers a Ready-or-provisioning stack whose `hapi-fhir` is not currently serving:
rollout, restart, Elasticsearch still yellow. WAIT re-raises `TemporaryError` each pass, so
the condition is printed to the log on every cycle and is never silent. There is deliberately
no timeout here — see Decision 2.

**Cases 9 and 10** are the states the current code cannot escape. At `datasets.py:576-583`
the purge Job is only created when it does not exist, so a Failed Job is never retried, and
`datasets.py:588-592` treats anything that is not `Complete` as a reason to raise
`TemporaryError` forever. Under this SOW they become an explicit, loud, terminal `FAIL`.

### 4.2 STACK_TEARDOWN — the stack CR is Terminating

Facts:

    protected:             bool
    namespace_terminating: bool
    datasets:              tuple[tuple[str, bool]]   # (name, is_terminating)

| # | id | when | action |
|---|----|------|--------|
| 1 | `stack-protected` | `protected` | WAIT |
| 2 | `namespace-terminating` | `namespace_terminating` | PROCEED |
| 3 | `datasets-need-deleting` | any dataset not terminating | DELETE_DATASETS |
| 4 | `datasets-terminating` | `datasets` | WAIT |
| 5 | `no-datasets` | `not datasets` | PROCEED |

This is where the ordering guarantee comes from, and it needs no new machinery. `guard()`
already holds `perf.fhir/finalizer` and can keep holding it with `TemporaryError`. While it
holds, the stack's Deployments are still running — so `hapi-fhir` stays up while the datasets
purge against it. Case 3 issues the deletes, case 4 waits for them, case 5 lets the stack go.

Case 1 keeps the existing `protected` behaviour from `fhir_operator.py:280-291` and is
checked first because it is the cheapest and most absolute.

### 4.3 DATASET_STEADY — the dataset CR is alive

One case only. This is the skeleton; the existing `decide()` at `datasets.py:187-203` keeps
its job.

Facts:

    stack:     StackState
    load_jobs: tuple[str]

| # | id | when | action |
|---|----|------|--------|
| 1 | `stack-gone-stop-loading` | `stack in (ABSENT, TERMINATING) and load_jobs` | STOP_JOBS |
| 2 | `carry-on` | always | PROCEED |

Case 1 is what would have stopped the 8397-and-climbing failure count observed on
`meow0-1-load-0`: a loader hammering a server that no longer exists. Today
`datasets.py:495-503` only sets `phase: Waiting` and leaves the Job running.

## 5. Observation (Section 5 of the file)

Five functions. Each does one read and returns a fact. None of them catch anything except a
`404` used as an existence probe.

    stack_state(namespace, stack_name)   -> StackState
    endpoint_state(namespace)            -> EndpointState
    namespace_terminating(namespace)     -> bool
    jobs_for(namespace, dataset)         -> (load_job_names, purge_job_or_None)
    datasets_in(namespace)               -> tuple[(name, terminating)]

`endpoint_state` reads the `hapi-fhir` Service and its Endpoints and judges from those
objects. It must never make an HTTP request: `FHIR_BASE_URL` is the cluster-internal DNS name
`http://hapi-fhir.<namespace>.svc:8080/fhir` (`datasets.py:290-291`), which the operator
cannot resolve when running outside the cluster. That is also why the loader reports its
census inward from the pod rather than the operator polling outward (`loader.py:320-358`).

`jobs_for` replaces `running_job` at `datasets.py:410-417`, which returns only the newest Job
matching `dataset=<name>`. Because the purge Job carries that same label
(`datasets.py:263-264`), the existing helper answers "is the loader running?" incorrectly the
moment a purge starts. `jobs_for` splits on the `mode` label and returns both.

`reconciliation.py` obtains its clients the same way `datasets.py` does — module-level
`init(dyn)` plus `_batch()` / `_core()` / `_custom()` accessors (`datasets.py:49-63`).

## 6. Effects (Section 6 of the file)

    stop_jobs(namespace, job_names)
        Delete each Job with propagationPolicy=Background. Errors propagate.

    start_purge(namespace, dataset, spec)
        Build and create the delete Job directly.

    delete_datasets(namespace, names)
        Delete each FhirDataset CR. Errors propagate.

`start_purge` must **not** call `launch()` at `datasets.py:437-464`. That path drags two
things into teardown that have no business there:

- `dataset_config()` (`datasets.py:439` -> `datasets.py:115`) performs SNOMED lookups via
  `resolve()` at `datasets.py:70-112`, which raises `kopf.TemporaryError` when the ontology
  service is unreachable (`datasets.py:96-97`, `datasets.py:105-106`). A delete needs no
  codes at all — `run_delete` uses only `config["datasetName"]` — so an unrelated ontology
  outage can currently block a deletion indefinitely.
- `launch()` re-applies worker RBAC, the ConfigMap, the results PVC and the whole Prometheus
  stack (`datasets.py:444-451`) on an object that is being deleted.

The delete Job needs `MODE=delete`, `FHIR_BASE_URL`, `DATASET_NAME`, `POD_NAMESPACE`,
`RESULTS_DIR`, the loader ConfigMap and the results PVC. The ConfigMap and PVC already exist
by the time a teardown runs; `start_purge` references them and does not re-apply them.

## 7. Call-site changes

Three edits outside the new file. All three shrink.

**`datasets.py` `purge()`** becomes: build `DatasetTeardownFacts`, call `decide`, act.

    RELEASE      -> log the reason, return (kopf drops the finalizer)
    STOP_JOBS    -> stop_jobs(...), raise TemporaryError(reason, delay=10)
    START_PURGE  -> start_purge(...), raise TemporaryError(reason, delay=20)
    WAIT         -> raise TemporaryError(reason, delay=20)
    FAIL         -> raise TemporaryError(reason, delay=300)

The `status.observed` shortcut at `datasets.py:569-573` is **removed**. `observed` is written
by loader worker 0 (`loader.py:320-358`) and can be hours stale; a stale zero would skip the
purge of a dataset that still has data. The delete Job's own count is the only authority.

**`fhir_operator.py` `guard()`** becomes: build `StackTeardownFacts`, call `decide`, act.

    PROCEED          -> log the reason, return
    DELETE_DATASETS  -> delete_datasets(...), raise TemporaryError(reason, delay=10)
    WAIT             -> raise TemporaryError(reason, delay=20)

**`datasets.py` `reconcile()`** gains a `DATASET_STEADY` check immediately after the stack
lookup at `datasets.py:490-503`. `STOP_JOBS` stops the loader and returns; `PROCEED` falls
through to the existing logic unchanged.

## 8. Decisions taken

**Decision 1 — drop the FhirStack ownerReference on datasets.**
`own()` at `datasets.py:467-480` sets an ownerReference with `blockOwnerDeletion: True`
(`datasets.py:406`). Combined with the purge finalizer this deadlocks by construction: under
foreground propagation the stack waits for datasets that are waiting for the stack's server;
under background propagation (kubectl's default) the stack CR vanishes first and every
dataset strands. Owner-reference cascade and finalizer-based purge are two mechanisms
competing for the same ordering, and the cascade cannot be made to wait correctly. Ordering
comes from `STACK_TEARDOWN` instead. `own()` and `adopt_to_stack()` (`datasets.py:394-407`)
are removed. This also retires the create-only adoption bug, where a stack that did not exist
at dataset-creation time was never adopted afterwards (`datasets.py:399-402`) — the reason
`meow0` and `meow0-1` had no owner references at all.

**Decision 2 — no timeout-based give-up.**
Every RELEASE is justified by an observed fact, never by a clock. A purge that genuinely
fails against a live server leaves the dataset in Terminating with `status.phase: PurgeFailed`
and the reason logged every 300 seconds. That is the honest outcome under "if it fails, let it
fail": the alternative is a timer that silently orphans data. The escape hatch is explicit and
human: set `spec.purgeOnDelete: false`, which case 2 turns into an immediate RELEASE. The
incident that motivated this SOW is fixed by case 3 on facts, not by a timeout.

**Decision 3 — stop the loader by deleting its Job, not by suspending it.**
A suspended Job still reports `Paused` via `job_phase` (`datasets.py:423-424`) and would sit
there forever. Deletion is unambiguous and its completion is observable as `load_pods == 0`.

**Decision 4 — `stackRef` stays, defaulting to the namespace.**
This matches `datasets.py:259`. No cross-namespace support: `stackRef` is resolved in the
dataset's own namespace (`datasets.py:493-494`) and the endpoint is namespace-derived
(`datasets.py:290-291`), so a cross-namespace reference is not expressible today and the
skeleton will not invent it.

**Decision 5 — the endpoint gate reads Kubernetes objects, never HTTP.** See section 5.

## 9. Tests: `perf-experiment/operator/test_reconciliation.py`

Plain script in the house style — `check(label, cond, detail)`, a `FAILS` list,
`sys.exit(1 if FAILS else 0)` — matching `test_loader.py` and the client-stubbing pattern in
`test_ui.py`. There is no pytest in the operator venv.

Sections 1-4 are pure, so the tests construct `Facts` directly and need no cluster:

1. One assertion per row of all three tables: given facts that select that row, `decide`
   returns that row's `id` and `action`.
2. **Coverage assertion**: every `id` in every table fires for at least one fixture. An
   unreachable case, shadowed by an earlier row, fails the suite.
3. **No-match assertion**: a `Facts` value matching nothing raises `RuntimeError`.
4. **The incident fixture**, named as such: `stack=ABSENT, endpoint=ABSENT,
   namespace_terminating=False, purge_on_delete=True, load_jobs=("meow0-1-load-0",),
   purge_job="Active"` must return `server-gone-with-stack` / `RELEASE`.
5. **The concurrent-load fixture**: `stack=READY, endpoint=SERVING,
   load_jobs=("meow0-1-load-0",), purge_job=None` must return `load-jobs-running` /
   `STOP_JOBS` — never `START_PURGE`.

## 10. Acceptance criteria

1. Deleting a `FhirDataset` whose `FhirStack` and `hapi-fhir` Service are absent completes
   without manual intervention, and the operator log names the reason.
2. Deleting a `FhirDataset` while its load Job is Running deletes the load Job first, waits
   for its pods, then runs the purge; the purge Job never coexists with a load Job.
3. Deleting a `FhirStack` with live datasets deletes the datasets first and keeps `hapi-fhir`
   running until every dataset's finalizer has been released.
4. Deleting a namespace containing datasets does not leave the namespace in Terminating.
5. A failed purge leaves the dataset in Terminating with `PurgeFailed` and a reason in the
   log every 300 seconds. It does not silently disappear and it does not retry in a loop.
6. `test_reconciliation.py` passes, including the coverage and no-match assertions.
7. No `except` clause is added anywhere in `reconciliation.py` other than the documented
   `404` existence probes.

## 11. Out of scope

Noted, deliberately not fixed in the skeleton:

- Worker RBAC (`datasets.py:444-445`) and the Prometheus stack (`datasets.py:385-387`) are
  applied without `kopf.adopt` and leak when the dataset goes. Confirmed live: `prometheus`
  was the only object left in namespace `meow0`.
- The `STACK` env var (`datasets.py:299`) is dead — `loader.py` never reads it.
- No new CRD fields.
- No UI changes; the delete button at `ui.py:855-858` stays a plain CR delete, which is
  correct now that ordering lives in the finalizers.
- No metrics for teardown.

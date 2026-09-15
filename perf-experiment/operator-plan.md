# FHIR benchmarking operator — plan

A kopf operator that owns the whole loop: stand up a HAPI FHIR stack, size it, load
synthetic data, run the query matrix, resize, run again, tear down. The sizing knobs
become fields on a CRD instead of a Flask form, and the sequence becomes a YAML
document instead of a person clicking through three pages in order.

Supersedes `app.py`, which stays as the throwaway that proved the shape.

---

## 1. Audit protocol

`/Users/andrew/Documents/GitHub/kopf` was read under the code-only protocol. Prose in
that repo describes intent; this plan is built on what the code does.

**Mechanism**, stated before any file was opened:

1. `/tmp/nocomment.py` — `tokenize.generate_tokens` records every `COMMENT` token's
   `(row, col)` and truncates the line at `col`. `ast.parse` + `ast.walk` deletes the
   full `lineno..end_lineno` range of every `ast.Expr` wrapping a string constant.
   Original line numbers are emitted as a `N\t` prefix so `file.py:123` citations below
   stay correct against the real tree.
2. Per-file self-check: re-tokenise the stripped output, assert `COMMENT == 0` and
   string-`Expr == 0`, print `VERIFY <path>: comments=0 docstrings=0`, refuse to emit
   otherwise.
3. Whole tree mirrored to `/tmp/kopf-stripped/` first. Every `grep`, `sed -n` and
   preview ran against the mirror only — searching counts as reading.
4. No `.md`, `.rst`, `.txt` opened at any point. No kopf README, no kopf docs directory.

**Evidence.** First pass: 306 files, 300 verified, **6 refused** —
`kopf/_kits/webhooks.py`, `kopf/_core/engines/admission.py`,
`kopf/_core/actions/execution.py`, `kopf/_cogs/clients/watching.py`,
`kopf/_cogs/structs/credentials.py`, `kopf/_cogs/structs/ephemera.py`. Cause: deleting a
docstring that *is* the entire body of a class or function leaves an empty suite that
will not re-parse, so `docstrings=None` rather than `0`. The refusal was correct — those
six files were withheld, not read. The stripper was fixed to substitute `pass` at the
original indent where a string `Expr` is the sole statement of a `body`/`orelse`/
`finalbody`. Second pass: **306/306 verified, 0 refused**, and an independent re-walk of
the finished mirror reports `files=306 comments=0 docstrings=0`.

Everything in §2 is cited to a line in the real repo and was derived from stripped
source.

---

## 2. What kopf actually provides

| Capability | Where | Bearing on this design |
|---|---|---|
| `@kopf.on.create/update/delete/resume` | `kopf/on.py:323,373,439,265` | Stack lifecycle. `resume` matters: the operator restarts mid-experiment and must re-adopt running stacks. |
| `@kopf.on.field(field=...)` | `kopf/on.py:497` | Resize is a field handler on `spec.size`, not a general update handler. Fires only when sizing changes. |
| `@kopf.daemon` | `kopf/on.py:662` | Long-running work bound to one object's lifetime. Data load and benchmark runs are daemons. |
| `@kopf.timer(interval=, idle=, sharp=)` | `kopf/on.py:725` | Periodic reconcile — readiness polling, progress into `status`. |
| `@kopf.index` | `kopf/on.py:556` | In-memory index over watched objects. Lets a `FhirBenchmark` resolve its target stack without an API call per handler tick. |
| `errors=`, `timeout=`, `retries=`, `backoff=` on every handler | `kopf/on.py:323`+ | Per-handler retry policy. A benchmark that fails should not retry 15 times. |
| `kopf.TemporaryError(delay=)` / `kopf.PermanentError` | `kopf/_core/actions/execution.py:29,25` | The distinction the state machine turns on: "DB not up yet" retries, "your query matrix is malformed" does not. |
| `kopf.ErrorsMode.{IGNORED,TEMPORARY,PERMANENT}` | `kopf/_core/actions/execution.py:52` | Default disposition per handler. |
| `kopf.DaemonStopped` / `DaemonStoppingReason` | `kopf/_core/intents/stoppers.py:6` | Flags include `RESOURCE_DELETED`, `OPERATOR_PAUSING`, `OPERATOR_EXITING`, `DAEMON_CANCELLED`. A load daemon can distinguish "user deleted the dataset" from "operator is shutting down" and checkpoint accordingly. |
| `kopf.adopt(doc)` | `kopf/_kits/hierarchies.py:294` | Sets owner references so deleting a `FhirStack` garbage-collects Deployments, PVCs, Services. Replaces the current `kubectl delete namespace`. |
| `patch.status[...]`, `patch.metadata.annotations[...]` | `kopf/_cogs/structs/patches.py` | Single accumulated patch per handler cycle. Progress reporting goes here. |
| `kopf.execute()` / `subhandler` | `kopf/_core/reactor/subhandling.py:32` | Sub-steps with independent retry state inside one handler. Natural fit for an experiment's step list. |
| `kopf.Memo`, `kopf.Index`, `kopf.Store` | `kopf/_cogs/structs/ephemera.py:10,78,56` | Per-object scratch space surviving across handler calls. Holds timing arrays mid-benchmark. |
| `OperatorSettings` | `kopf/_cogs/configs/configuration.py` | See below — several defaults are wrong for this workload. |
| `kopf run -A/-n/-m/--standalone/--liveness/--peering/--priority` | `kopf/cli.py:74` | Deployment surface. |

### Settings that need changing from default

Read from `kopf/_cogs/configs/configuration.py`:

- `execution.default_backoff = 60` (`:327`) and `queueing.error_delays` rising to 610s
  (`:214`). Fine for a general operator, far too slow while iterating on an experiment.
  Drop to a few seconds for load/bench handlers via per-handler `backoff=`.
- `networking.request_timeout = 300` (`:375`). A transaction bundle POST can exceed this;
  the loader must not share kopf's HTTP client. Use a separate session, as `app.py` does.
- `watching.inactivity_timeout = 70.0`, `server_timeout = None` (`:180`). Acceptable.
- `peering` (`:99`) is how two operator replicas avoid both driving the same experiment.
  Non-optional here — a duplicated load run silently doubles the dataset. Run with an
  explicit peering name, `standalone: False`.
- `persistence.finalizer` (`:424`) defaults to `kopf.zalando.org/KopfFinalizerMarker`.
  Rename to something of ours.
- `execution.executor` is a thread pool for sync handlers. The loader is I/O-bound
  across many connections — write it async, do not lean on this pool.

---

## 3. CPU scaling — the immediate gap

`app.py` scales memory and derives `shared_buffers` and the ES heap from the limit.
CPU is untouched: Postgres still requests 24 cores and Elasticsearch 10, which is why
`perf-s` sits at 0/1 on a node already running the `default` stack. The same derivation
treatment applies.

**Derived from the CPU limit:**

| Component | Setting | Rule |
|---|---|---|
| Postgres | `max_worker_processes` | `max(8, 2.5 × cpu)` — manifest is 60 at 24 cores |
| Postgres | `max_parallel_workers` | `= max_worker_processes` |
| Postgres | `max_parallel_workers_per_gather` | `clamp(cpu // 4, 2, 8)` |
| Postgres | `max_parallel_maintenance_workers` | `clamp(cpu // 8, 2, 4)` |
| Postgres | `autovacuum_max_workers` | `clamp(cpu // 6, 3, 8)` |
| Elasticsearch | `node.processors` | `= ceil(cpu)`; ES sizes its write/search thread pools from this, and cgroup detection is unreliable under a fractional limit |
| HAPI | nothing | JVM reads the cgroup quota for `ActiveProcessorCount`; ForkJoin and Tomcat pools follow |

**The invariant worth enforcing.** HAPI's Hikari `maximum-pool-size` × HAPI replicas must
stay below Postgres `max_connections`, currently pinned at 300 in the manifest. Scale
Hikari with HAPI's CPU and the two drift into each other. The operator should compute
both and reject a spec where `replicas × hikariPoolSize > max_connections × 0.8`.

**QoS is a benchmarking decision, not a packing one.** Three policies, explicit in the
CRD:

- `strict` — `requests == limits`, Guaranteed QoS. Reproducible between runs, at the cost
  of CFS throttling at the ceiling. Default for anything whose numbers you intend to
  quote.
- `burst` — request below limit, Burstable. Higher throughput, timings contaminated by
  whatever else is on the node. Use while iterating, not for results.
- `none` — requests only, no limit. True unthrottled throughput; one stack can starve
  another. Single-tenant nodes only.

Silently picking one of these makes every benchmark number unattributable, which is why
it is a field rather than a default buried in code.

---

## 4. CRDs

Four kinds, group `perf.fhir/v1alpha1`. Splitting stack from workload is what lets you
resize between runs without redeploying, and re-run a benchmark against a stack someone
else loaded.

### `FhirStack`

```yaml
apiVersion: perf.fhir/v1alpha1
kind: FhirStack
metadata:
  name: perf-s
spec:
  image: hapiproject/hapi:v8.8.0-1
  replicas: 1

  size:                                # named profile, or omit for explicit blocks
    profile: medium                    # tiny|small|medium|large|behemoth

  resources:                           # any explicit block overrides the profile
    hapi:          {cpu: {min: 2, max: 4},   memory: {min: 2Gi,  max: 10Gi}}
    postgres:      {cpu: {min: 8, max: 8},   memory: {min: 32Gi, max: 32Gi}}
    elasticsearch: {cpu: {min: 4, max: 4},   memory: {min: 8Gi,  max: 8Gi}}
  cpuLimitPolicy: strict               # strict|burst|none

  tuning:
    postgres:
      derive: true                     # shared_buffers etc. from the limits above
      overrides: {max_connections: 300}
    elasticsearch:
      heapFraction: 0.5                # capped at 31g, compressed oops
    hapi:
      hikariPoolSize: auto
      searchCacheSeconds: 0            # 0 disables the 60s HFJ_SEARCH reuse window
      indexMissingFields: disabled

  safety:
    protected: false                   # refuse deletion while true
    maxTotalMemory: 64Gi               # operator rejects a spec exceeding this
    maxTotalCpu: 24
    deleteData: true                   # false keeps PVCs on stack deletion

status:
  phase: Ready                         # Pending|Provisioning|Ready|Resizing|Degraded|Deleting
  endpoint: http://hapi-fhir.perf-s.svc:8080/fhir
  applied: {postgres: {shared_buffers: 11650MB, max_worker_processes: 20}, ...}
  resourceVersionsObserved: {...}
  conditions: [...]
```

### `FhirDataset`

```yaml
apiVersion: perf.fhir/v1alpha1
kind: FhirDataset
metadata:
  name: perf-s-2m
spec:
  stackRef: {name: perf-s}
  generator: synthetic-v1              # the app.py generator, seeded
  seed: 20260911
  patients: {first: 1, count: 2000000}
  shape:
    heavyPatientEveryN: 10
    conditionsPerPatient: {normal: 11, heavy: 90}
    observationsPerPatient: {normal: 53, heavy: 500}
    codeDictionary: {common: 1, mid: 9, rare: 35}
  ingest:
    bundleSize: 1000
    workers: 8
    parallelism: 4                     # Job pods
    method: PUT                        # idempotent; restart-safe
  onComplete:
    reindex: false
    analyze: true                      # postgres ANALYZE before any benchmark
status:
  phase: Loading
  patientsDone: 431200
  resources: 24993600
  ratePerSecond: 3820
  checkpoint: {lastSerial: 431200}
```

`method: PUT` is what makes an interrupted load restartable — a re-run of a finished
range is a no-op. That property should not be configurable away without a warning.

### `FhirBenchmark`

```yaml
apiVersion: perf.fhir/v1alpha1
kind: FhirBenchmark
metadata:
  name: perf-s-chains
spec:
  stackRef: {name: perf-s}
  datasetRef: {name: perf-s-2m}        # recorded in results; not re-loaded
  cases: chained-search-v1             # the eight cases from search-experiment.md
  selectivity: [common, mid, rare]
  modes: [first-page, count]
  repetitions: 3
  timeoutSeconds: 300
  cacheBusting: true                   # Cache-Control: no-cache, verified by bundle id
  concurrency: 1                       # >1 for read-under-write
  warmup: {enabled: true, discard: 1}
status:
  phase: Running
  done: 34
  total: 48
  results: [{case: E, mode: count, medianMs: 8412, maxMs: 9103, total: 1841, censored: false, cacheOk: true}]
```

`cacheOk` stays: distinct bundle ids across repetitions are the only proof the no-cache
header was honoured, and without it repetitions 2..n measure an `HFJ_SEARCH` lookup
rather than a search.

### `FhirExperiment` — the declarative sequence

This is the piece `app.py` has no answer for. One document describing the whole run.

```yaml
apiVersion: perf.fhir/v1alpha1
kind: FhirExperiment
metadata:
  name: sweep-postgres-memory
spec:
  stack:
    template: {spec: {image: hapiproject/hapi:v8.8.0-1, cpuLimitPolicy: strict}}
  steps:
    - name: provision
      resize: {postgres: {cpu: {min: 8, max: 8}, memory: {min: 32Gi, max: 32Gi}}}
    - name: load
      dataset: {seed: 20260911, patients: {first: 1, count: 2000000}}
    - name: baseline
      benchmark: {cases: chained-search-v1, repetitions: 3}
    - name: halve-memory
      resize: {postgres: {memory: {min: 16Gi, max: 16Gi}}}
      waitFor: Ready
    - name: rerun
      benchmark: {cases: chained-search-v1, repetitions: 3}
    - name: quarter-memory
      resize: {postgres: {memory: {min: 8Gi, max: 8Gi}}}
      waitFor: Ready
    - name: rerun-again
      benchmark: {cases: chained-search-v1, repetitions: 3}
  teardown: onSuccess                  # always|onSuccess|never
  matrix:                              # optional cartesian expansion over steps
    postgres.memory: [32Gi, 16Gi, 8Gi]
status:
  phase: Running
  currentStep: rerun
  completedSteps: [provision, load, baseline, halve-memory]
  comparison: {...}
```

Each step is a kopf subhandler (`kopf/_core/reactor/subhandling.py:32`), so a failed step
retries with its own backoff without re-running the ones before it. That is the property
that makes a multi-hour sweep survivable.

**A resize between load and benchmark restarts Postgres.** Pod template changes always
do. Buffer cache is cold on the far side, so a `rerun` step measures a cold cache unless
the step declares a warmup. `warmup` defaults on for that reason, and the comparison in
`status` should mark any result gathered within N seconds of a restart.

---

## 5. Handler map

```python
@kopf.on.startup()                     # settings: peering, finalizer, backoffs
@kopf.on.create('fhirstacks')          # render manifest, kopf.adopt, apply
@kopf.on.resume('fhirstacks')          # re-adopt after operator restart
@kopf.on.field('fhirstacks', field='spec.resources')      # resize path
@kopf.on.field('fhirstacks', field='spec.size.profile')   # resize path
@kopf.on.delete('fhirstacks')          # refuse if spec.safety.protected
@kopf.timer('fhirstacks', interval=10, idle=0)            # readiness -> status.phase

@kopf.on.create('fhirdatasets')        # validate stack Ready, create load Job(s)
@kopf.daemon('fhirdatasets')           # drive + checkpoint the load, honour stopped
@kopf.index('fhirstacks')              # name -> endpoint, for dataset/benchmark lookup

@kopf.on.create('fhirbenchmarks')
@kopf.daemon('fhirbenchmarks', cancellation_timeout=30)   # run matrix, stream results

@kopf.on.create('fhirexperiments')
@kopf.daemon('fhirexperiments')        # step machine over kopf.execute subhandlers
```

Two decisions worth stating:

- **Resize is `on.field`, not `on.update`.** An update handler fires on every status
  write and every annotation change; a field handler fires when sizing actually moves.
- **Load and benchmark are daemons, not Jobs the operator forgets about.** `stopped`
  (`kopf/_core/intents/stoppers.py:6`) distinguishes `RESOURCE_DELETED` from
  `OPERATOR_EXITING`, so a deleted dataset stops cleanly while an operator restart
  resumes from `status.checkpoint`. The actual bundle POSTing still happens in Jobs for
  parallelism — the daemon supervises and checkpoints.

---

## 6. Safety rails

Operational, not governance. These exist because the thing being automated is
"destroy and rebuild a database repeatedly".

1. `spec.safety.protected: true` makes `on.delete` raise `PermanentError`. Set on any
   stack holding data you cannot regenerate — the MIMIC stack in `default` qualifies.
2. `maxTotalMemory` / `maxTotalCpu` are rejected at admission, not at schedule time. A
   spec that cannot fit the cluster should fail loudly rather than sit Pending.
3. Deriving Postgres and ES settings from limits is mandatory unless `derive: false`.
   `shared_buffers` and `-Xms` are committed at startup, so a limit below them is an OOM
   kill during boot, not a slow server. `derive: false` should require an explicit
   acknowledgement field.
4. The Hikari × replicas < `max_connections` check, per §3.
5. Refuse to start a `FhirDataset` against a stack whose phase is not `Ready`, and refuse
   a `FhirBenchmark` against a stack with an in-flight `FhirDataset` unless
   `spec.concurrency > 1` says read-under-write is the point.
6. Owner references via `kopf.adopt` so teardown is a single object delete. Namespace
   deletion as a cleanup mechanism goes away.
7. `deleteData: false` retains PVCs — reattach a 2M-patient dataset to a fresh stack
   instead of reloading it for six hours.

---

## 7. Build order

**Phase 0 — close the CPU gap in `app.py`.** Add min/max CPU alongside memory, plus the
Postgres worker and `node.processors` derivations and `cpuLimitPolicy`. Same
`patch_namespaced_deployment` path already in `set_memory()`. Unblocks scheduling a
second stack today; validates the derivation table before it is written into a CRD.

**Phase 1 — `FhirStack` only.** kopf operator, CRD, create/resume/field/delete/timer.
Manifest rendering moves out of `hapi-fhir-standalone.yaml` into templated Python.
Success: `kubectl apply` a stack, edit `spec.size.profile`, watch it resize.

**Phase 2 — `FhirDataset`.** Move the `app.py` generator into a container image. Job
fan-out, daemon supervision, checkpointing. Success: interrupt a load, restart the
operator, watch it resume at the right serial.

**Phase 3 — `FhirBenchmark`.** The eight cases, selectivity tiers, cache-busting
verification, results in `status` and to a PVC as CSV.

**Phase 4 — `FhirExperiment`.** Step machine, `matrix` expansion, comparison table.

**Phase 5 — the rest.** Multi-version HAPI images, PostgreSQL vs Elasticsearch search
backends side by side, partitioning modes, read-under-write.

Phases 1–3 are each independently useful, which is the point of the ordering.

---

## 8. Open questions

1. **Results storage.** `status` is bounded by etcd's object size and 48 result rows is
   already large. Sidecar Postgres for results, or PVC + CSV, or push to something
   external? Wants deciding before Phase 3.
2. **Where does the operator run?** In-cluster with a ServiceAccount, or on the laptop
   against a kubeconfig like `app.py` does? Laptop is faster to iterate; in-cluster is
   the only way a daemon survives a closed lid mid-sweep.
3. **Node topology.** Everything above assumes one behemoth node. Is a stack-per-node
   pool available? It changes whether `cpuLimitPolicy: none` is ever safe.
4. **Does the manifest stay?** Phase 1 templates it in Python. If
   `hapi-fhir-standalone.yaml` remains the source of truth for other work, the operator
   should patch it rather than replace it, and the two will drift.
5. **Fractional CPU and `node.processors`.** ES wants an integer. Whether to round up
   from a fractional limit or forbid fractional CPU on the ES component.

---

## Appendix — reproducing the audit

```bash
# stripper, then whole-tree mirror with per-file VERIFY
python3 /tmp/nocomment.py <file> <dest>
find . -name '*.py' -not -path './.git/*'   # -> /tmp/kopf-stripped/, 306/306 verified
grep -rn --include='*.py' <pattern> /tmp/kopf-stripped   # never the real tree
```

Independent re-walk of the mirror: `files=306 comments=0 docstrings=0`.

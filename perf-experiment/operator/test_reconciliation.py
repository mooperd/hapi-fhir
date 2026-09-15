# Created by claude-opus-5
"""Checks for the reconciliation case tables.

No pytest in the operator venv, so this is a plain script:

    .venv/bin/python test_reconciliation.py

Sections 1-4 of reconciliation.py are pure, so nothing here touches a cluster.
Every row of every table gets an assertion, and the coverage check at the end
fails if a row is unreachable because an earlier row shadows it.
"""

import os
import sys

HERE = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, HERE)
import reconciliation as rec  # noqa: E402 - needs the path above

FAILS = []
FIRED = set()


def check(label, cond, detail=""):
    print(("PASS " if cond else "FAIL ") + label + ("" if cond else "  <- " + str(detail)))
    if not cond:
        FAILS.append(label)


def decides(label, table, facts, case_id, action):
    """Assert the table picks this case, and record the hit for the coverage check."""
    decision = rec.decide(table, facts)
    FIRED.add((id(table), decision.case_id))
    check(label, decision.case_id == case_id and decision.action == action,
          "%s/%s, reason=%s" % (decision.case_id, decision.action.name, decision.reason))
    check(label + " [has a reason]", bool(decision.reason), decision)


# --------------------------------------------------------------------------
# DATASET_TEARDOWN
# --------------------------------------------------------------------------

def teardown(**overrides):
    """Healthy facts that fall through to purge-absent unless overridden."""
    facts = {
        "namespace_terminating": False,
        "purge_on_delete": True,
        "stack": rec.StackState.READY,
        "endpoint": rec.EndpointState.SERVING,
        "load_jobs": (),
        "load_pods": 0,
        "purge_job": None,
    }
    facts.update(overrides)
    return rec.DatasetTeardownFacts(**facts)


T = rec.DATASET_TEARDOWN
A = rec.Action

decides("1 namespace-terminating wins over everything", T,
        teardown(namespace_terminating=True, purge_on_delete=True,
                 stack=rec.StackState.READY, purge_job="Active"),
        "namespace-terminating", A.RELEASE)

decides("2 purge-disabled releases", T,
        teardown(purge_on_delete=False), "purge-disabled", A.RELEASE)

decides("3 stack absent and endpoint absent releases", T,
        teardown(stack=rec.StackState.ABSENT, endpoint=rec.EndpointState.ABSENT),
        "server-gone-with-stack", A.RELEASE)

decides("3 stack terminating and endpoint not serving releases", T,
        teardown(stack=rec.StackState.TERMINATING,
                 endpoint=rec.EndpointState.PRESENT_NOT_SERVING),
        "server-gone-with-stack", A.RELEASE)

decides("4 load jobs are stopped first", T,
        teardown(load_jobs=("meow0-1-load-0",)), "load-jobs-running", A.STOP_JOBS)

decides("5 draining pods are waited for", T,
        teardown(load_pods=4), "load-pods-draining", A.WAIT)

decides("6 a live stack with no endpoint waits", T,
        teardown(endpoint=rec.EndpointState.PRESENT_NOT_SERVING),
        "endpoint-not-serving", A.WAIT)

decides("7 no purge job starts one", T, teardown(), "purge-absent", A.START_PURGE)

decides("8 an active purge waits", T,
        teardown(purge_job="Active"), "purge-active", A.WAIT)

decides("9 a paused purge fails loudly", T,
        teardown(purge_job="Paused"), "purge-paused", A.FAIL)

decides("10 a failed purge fails loudly", T,
        teardown(purge_job="Failed"), "purge-failed", A.FAIL)

decides("11 a complete purge releases", T,
        teardown(purge_job="Complete"), "purge-complete", A.RELEASE)

# ---- the two scenarios that motivated the SOW --------------------------

decides("meow0 incident: stack and server both gone, load job still listed", T,
        teardown(stack=rec.StackState.ABSENT, endpoint=rec.EndpointState.ABSENT,
                 load_jobs=("meow0-1-load-0",), load_pods=4, purge_job="Active"),
        "server-gone-with-stack", A.RELEASE)

decides("concurrent load: never purge while the loader runs", T,
        teardown(stack=rec.StackState.READY, endpoint=rec.EndpointState.SERVING,
                 load_jobs=("meow0-1-load-0",), purge_job=None),
        "load-jobs-running", A.STOP_JOBS)

# ---- no match is a crash, not a default --------------------------------

try:
    rec.decide(T, teardown(purge_job="Bewildered"))
    check("an unmatched fact set raises", False, "no exception")
except RuntimeError as exc:
    check("an unmatched fact set raises", True)
    check("the crash names the facts", "Bewildered" in str(exc), exc)


# --------------------------------------------------------------------------
# STACK_TEARDOWN
# --------------------------------------------------------------------------

def stack(**overrides):
    facts = {"protected": False, "namespace_terminating": False, "datasets": ()}
    facts.update(overrides)
    return rec.StackTeardownFacts(**facts)


S = rec.STACK_TEARDOWN

decides("1 protected blocks before anything else", S,
        stack(protected=True, datasets=(("meow0", False),)), "stack-protected", A.WAIT)

decides("2 a terminating namespace lets the stack go", S,
        stack(namespace_terminating=True, datasets=(("meow0", False),)),
        "namespace-terminating", A.PROCEED)

decides("3 live datasets are deleted first", S,
        stack(datasets=(("meow0", False), ("meow0-1", True))),
        "datasets-need-deleting", A.DELETE_DATASETS)

decides("4 terminating datasets are waited for", S,
        stack(datasets=(("meow0", True), ("meow0-1", True))),
        "datasets-terminating", A.WAIT)

decides("5 no datasets lets the stack go", S, stack(), "no-datasets", A.PROCEED)


# --------------------------------------------------------------------------
# DATASET_STEADY
# --------------------------------------------------------------------------

def steady(**overrides):
    facts = {"stack": rec.StackState.READY, "endpoint": rec.EndpointState.SERVING,
             "load_jobs": ()}
    facts.update(overrides)
    return rec.DatasetSteadyFacts(**facts)


D = rec.DATASET_STEADY

decides("1 a vanished stack with a dead endpoint stops the loader", D,
        steady(stack=rec.StackState.ABSENT, endpoint=rec.EndpointState.ABSENT,
               load_jobs=("meow0-1-load-0",)),
        "stack-gone-stop-loading", A.STOP_JOBS)

decides("1 a terminating stack with a dead endpoint stops the loader", D,
        steady(stack=rec.StackState.TERMINATING,
               endpoint=rec.EndpointState.PRESENT_NOT_SERVING,
               load_jobs=("meow0-1-load-0",)),
        "stack-gone-stop-loading", A.STOP_JOBS)

# An adopted or hand-installed hapi-fhir has no FhirStack CR at all. The
# endpoint is what actually serves, so a serving endpoint means carry on.
decides("1 no stack CR but a serving endpoint carries on", D,
        steady(stack=rec.StackState.ABSENT, endpoint=rec.EndpointState.SERVING,
               load_jobs=("meow0-1-load-0",)),
        "carry-on", A.PROCEED)

decides("2 a healthy stack carries on", D,
        steady(load_jobs=("meow0-1-load-0",)), "carry-on", A.PROCEED)

decides("2 a vanished stack with no jobs carries on", D,
        steady(stack=rec.StackState.ABSENT, endpoint=rec.EndpointState.ABSENT),
        "carry-on", A.PROCEED)


# --------------------------------------------------------------------------
# Coverage: no row may be unreachable
# --------------------------------------------------------------------------

for name, table in (("DATASET_TEARDOWN", T), ("STACK_TEARDOWN", S), ("DATASET_STEADY", D)):
    declared = [case.id for case in table]
    check("%s ids are unique" % name, len(declared) == len(set(declared)), declared)
    missed = [case_id for case_id in declared if (id(table), case_id) not in FIRED]
    check("%s: every case is reachable" % name, not missed, missed)

# ---- the tables are the only place cases are defined --------------------

check("every case carries a callable guard and reason",
      all(callable(c.when) and callable(c.reason) for t in (T, S, D) for c in t))
check("every action is a member of Action",
      all(isinstance(c.action, rec.Action) for t in (T, S, D) for c in t))

print()
print("%d checks failed" % len(FAILS) if FAILS else "all checks passed")
sys.exit(1 if FAILS else 0)

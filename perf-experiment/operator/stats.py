# Created by claude-opus-5
"""Percentiles, from the raw samples.

Not from Prometheus histogram buckets, which are lossy -- Prometheus is for
live progress only. Not means, either: an average hides exactly the behaviour
that harms clinicians, which is the occasional thirty-second query that makes
someone stop trusting the dashboard.
"""

QUANTILES = (("p50ms", 50), ("p90ms", 90), ("p95ms", 95), ("p99ms", 99))


def percentile(samples, q):
    """Nearest-rank percentile over a list of floats. None for no samples."""
    if not samples:
        return None
    ordered = sorted(samples)
    rank = int(-(-q * len(ordered) // 100))          # ceil(q/100 * n)
    return ordered[min(max(rank, 1), len(ordered)) - 1]


def summarise(records):
    """{caseId: {cacheLabel: {...}}} from per-request records.

    A record is INVALID, TIMEOUT or ERROR unless record["valid"] is true.
    Only valid records contribute latency samples; the others are counted and
    named, never dropped and never blended in.
    """
    grouped = {}
    for record in records:
        key = (record["caseId"], record.get("cacheLabel") or record["stepId"])
        grouped.setdefault(key, []).append(record)

    out = {}
    for (case_id, label), group in sorted(grouped.items()):
        valid = [r for r in group if r.get("valid")]
        samples = [r["wallMs"] for r in valid]
        engines = sorted(set(r.get("engineObserved") or "unattributed" for r in group))
        rows = sorted(set(r.get("rowsReturned") for r in valid if r.get("rowsReturned") is not None))
        totals = sorted(set(r.get("bundleTotal") for r in valid if r.get("bundleTotal") is not None))
        entry = {
            "n": len(samples),
            "attempted": len(group),
            "invalid": sum(1 for r in group if not r.get("valid")),
            "timeouts": sum(1 for r in group if r.get("invalidReason") == "TIMEOUT"),
            "errors": sum(1 for r in group
                          if (r.get("invalidReason") or "").startswith("ERROR")),
            "engine": engines[0] if len(engines) == 1 else ",".join(engines),
            "engineExpected": group[0].get("engineExpected"),
            "family": group[0].get("family"),
            "rows": rows[-1] if rows else None,
            "bundleTotal": totals[-1] if totals else None,
            "maxms": max(samples) if samples else None,
            "reasons": sorted(set(r["invalidReason"] for r in group
                                  if r.get("invalidReason"))),
        }
        for name, q in QUANTILES:
            entry[name] = percentile(samples, q)
        out.setdefault(case_id, {})[label] = entry
    return out


def merge(into, addition):
    """Fold one step's summary into the run summary. Append-only per label."""
    for case_id, labels in addition.items():
        into.setdefault(case_id, {}).update(labels)
    return into


def headline(summary, limit=25):
    """The slowest cases, for status. The whole summary goes to the PVC."""
    rows = []
    for case_id, labels in summary.items():
        for label, entry in labels.items():
            rows.append((entry.get("p95ms") or -1, case_id, label))
    rows.sort(reverse=True)
    trimmed = {}
    for _, case_id, label in rows[:limit]:
        trimmed.setdefault(case_id, {})[label] = summary[case_id][label]
    return trimmed


def deltas(summary):
    """Matched-pair deltas: the measured value of Elasticsearch, per shape.

    pair-<name>-a is the eligible member, pair-<name>-b the disqualified one.
    The pair is only reported when both members produced valid samples at the
    same cache label; a half-measured pair is not a delta.
    """
    out = {}
    for case_id, labels in summary.items():
        if not case_id.startswith("pair-") or not case_id.endswith("-a"):
            continue
        name = case_id[len("pair-"):-len("-a")]
        other = summary.get("pair-%s-b" % name) or {}
        for label, eligible in labels.items():
            disqualified = other.get(label)
            if not disqualified:
                continue
            if not eligible.get("p50ms") or not disqualified.get("p50ms"):
                continue
            out.setdefault(name, {})[label] = {
                "eligibleP50ms": eligible["p50ms"],
                "disqualifiedP50ms": disqualified["p50ms"],
                "ratio": round(disqualified["p50ms"] / eligible["p50ms"], 3),
                "eligibleEngine": eligible.get("engine"),
                "disqualifiedEngine": disqualified.get("engine"),
            }
    return out

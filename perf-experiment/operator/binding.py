# Created by claude-opus-5
"""Bind catalogue placeholders to the dataset that is actually loaded.

Without this a "fast" result may simply mean we accidentally asked for a code
nobody has. The dataset generator records the share of resources carrying each
code (datasets.resolve() writes it into dataset.json), so selectivity is known
and controlled and expected cardinality is computable before the query runs.

Two binding modes:

    pin     bind once for the whole step -- the identical query every time,
            which is what makes a "hot" measurement mean anything
    rotate  draw a fresh, unseen code/patient/window per repetition from a
            seeded PRNG, so a cold-ish measurement gets n>1 without one
            restart per sample. The seed is recorded, so the run is
            reproducible.

Nothing here substitutes. A placeholder the dataset manifest cannot supply is
a PermanentError naming the placeholder, never "pick any code".
"""

import datetime
import math
import random
import re

import kopf

SNOMED = "http://snomed.info/sct"
TAG_SYSTEM = "http://perf.fhir/dataset"

# loader.py's own generator constants. The dates and quantities in the
# database come from these, so the query windows must come from them too.
DAY_ZERO = datetime.date(2015, 1, 1)
DAY_SPAN = 3652
QUANTITY_LOW = 0.5
QUANTITY_HIGH = 250.0

PLACEHOLDER = re.compile(r"\{\{\s*([A-Za-z0-9_]+)\s*\}\}")


def _codes(config, key, placeholder):
    found = config.get(key) or []
    if not found:
        raise kopf.PermanentError(
            "placeholder {{%s}} cannot be bound: dataset.json has no %s"
            % (placeholder, key))
    return found


def _patient_id(config, serial):
    return "%s-p%07d" % (config["prefix"], serial)


def _serials(config):
    """(heavy serials, normal serials) inside the dataset's own range."""
    first = int(config["first"])
    count = int(config["count"])
    every = int((config.get("shape") or {}).get("heavyEveryN", 10)) or 10
    heavy, normal = [], []
    for serial in range(first, first + count):
        (heavy if serial % every == 0 else normal).append(serial)
    return heavy, normal


def bind(config, mode, seed, rep):
    """Every placeholder, as {name: value}.

    mode="pin" ignores rep and always returns the same values.
    """
    if mode not in ("pin", "rotate"):
        raise kopf.PermanentError("bindingMode must be pin or rotate, not %r" % mode)
    rng = random.Random("%s|%s|%d" % (config.get("datasetName", ""), seed,
                                      rep if mode == "rotate" else 0))

    conditions = _codes(config, "conditionCodes", "commonConditionCode")
    observations = _codes(config, "observationCodes", "commonObservationCode")
    heavy, normal = _serials(config)
    if not heavy:
        raise kopf.PermanentError(
            "placeholder {{heavyPatientId}} cannot be bound: no serial in "
            "[%d, %d) is a multiple of shape.heavyEveryN"
            % (config["first"], config["first"] + config["count"]))
    if not normal:
        raise kopf.PermanentError(
            "placeholder {{normalPatientId}} cannot be bound: every serial in "
            "[%d, %d) is heavy" % (config["first"], config["first"] + config["count"]))

    by_weight = sorted(conditions, key=lambda c: (c["weight"], c["code"]))
    obs_by_weight = sorted(observations, key=lambda c: (c["weight"], c["code"]))
    if mode == "pin":
        common_condition, rare_condition = by_weight[-1], by_weight[0]
        common_obs, rare_obs = obs_by_weight[-1], obs_by_weight[0]
        heavy_serial, normal_serial = heavy[0], normal[0]
        day = DAY_SPAN // 2
    else:
        # Rotation draws from the top and bottom deciles rather than the
        # single extreme, so "common" stays common and no repetition re-reads
        # the previous repetition's pages.
        edge = max(1, len(by_weight) // 10)
        obs_edge = max(1, len(obs_by_weight) // 10)
        common_condition = rng.choice(by_weight[-edge:])
        rare_condition = rng.choice(by_weight[:edge])
        common_obs = rng.choice(obs_by_weight[-obs_edge:])
        rare_obs = rng.choice(obs_by_weight[:obs_edge])
        heavy_serial = rng.choice(heavy)
        normal_serial = rng.choice(normal)
        day = rng.randint(0, DAY_SPAN)

    mid = DAY_ZERO + datetime.timedelta(days=day)
    return {
        "snomed": SNOMED,
        "datasetName": config["datasetName"],
        "datasetTag": "%s|%s" % (TAG_SYSTEM, config["datasetName"]),
        "commonConditionCode": common_condition["code"],
        "rareConditionCode": rare_condition["code"],
        "commonConditionDisplay": _first_word(common_condition.get("display")),
        "commonObservationCode": common_obs["code"],
        "rareObservationCode": rare_obs["code"],
        "commonObservationDisplay": _first_word(common_obs.get("display")),
        "heavyPatientId": _patient_id(config, heavy_serial),
        "normalPatientId": _patient_id(config, normal_serial),
        "patientFamily": "Surname%07d" % normal_serial,
        "patientFamilyFragment": ("Surname%07d" % normal_serial)[3:10],
        "mrn": str(normal_serial),
        "dateFrom": str(DAY_ZERO),
        "dateMid": str(mid),
        "dateTo": str(DAY_ZERO + datetime.timedelta(days=DAY_SPAN)),
        "quantityMid": "%.2f" % ((QUANTITY_LOW + QUANTITY_HIGH) / 2),
    }


def _first_word(display):
    """Code displays are multi-word; :text and _content take one term."""
    word = (display or "").split()
    if not word:
        raise kopf.PermanentError(
            "placeholder {{commonConditionDisplay}} cannot be bound: the "
            "resolved code has no display text")
    return word[0]


def render(case, bindings):
    """One case as (method, path, query), with every placeholder substituted.

    An unbindable placeholder names itself and stops the run. No "pick any
    code", no leaving the literal in the URL.
    """
    def one(text):
        missing = [name for name in PLACEHOLDER.findall(text) if name not in bindings]
        if missing:
            raise kopf.PermanentError(
                "case %s uses placeholder(s) %s which the dataset manifest cannot "
                "bind" % (case["id"], ", ".join("{{%s}}" % m for m in missing)))
        return PLACEHOLDER.sub(lambda m: str(bindings[m.group(1)]), text)

    return case["method"], one(case["path"]), one(case["query"])


# --------------------------------------------------------------------------
# Cardinality prediction
# --------------------------------------------------------------------------
#
# resolve() gives each code a weight that is its share of the resources of
# that type, and loader.py draws codes with rng.choices against those weights.
# So the count carrying one code is Binomial(n, p), not a fixed number: the
# prediction is an expectation with a band, and a measurement is reconciled
# against the band rather than against a single integer. Four sigma, so a
# valid run does not produce spurious INVALIDs, plus whatever
# tolerance.rowCountPct the spec allows on top.

SIGMAS = 4.0


def predict(case, bindings, config, expected_counts):
    """(predicted, low, high) for a case, or None if the case predicts nothing."""
    rule = case.get("predict")
    if not rule:
        return None
    if rule["kind"] != "code":
        raise kopf.PermanentError(
            "case %s has an unknown predictor kind %r" % (case["id"], rule["kind"]))

    key = "conditionCodes" if rule["type"] == "Condition" else "observationCodes"
    codes = _codes(config, key, case["id"])
    total_weight = sum(c["weight"] for c in codes)
    if total_weight <= 0:
        raise kopf.PermanentError(
            "case %s cannot be predicted: %s weights sum to zero" % (case["id"], key))

    wanted = bindings["commonConditionCode"] if rule["type"] == "Condition" else \
        bindings["commonObservationCode"]
    if rule["pick"] == "rare":
        wanted = bindings["rareConditionCode"] if rule["type"] == "Condition" else \
            bindings["rareObservationCode"]

    match = next((c for c in codes if c["code"] == wanted), None)
    if match is None:
        raise kopf.PermanentError(
            "case %s predicts against code %s, which is not in %s"
            % (case["id"], wanted, key))

    total = int(expected_counts.get(rule["type"], 0))
    if total <= 0:
        raise kopf.PermanentError(
            "case %s cannot be predicted: status.expected has no %s count"
            % (case["id"], rule["type"]))

    p = match["weight"] / total_weight
    mean = total * p
    band = SIGMAS * math.sqrt(total * p * (1.0 - p))
    return {"predicted": mean, "low": max(0.0, mean - band), "high": mean + band,
            "sigmaBand": band}

# Created by claude-opus-5
"""The query catalogue: what a FhirBenchmark actually asks.

Version-controlled and pinned, never typed into the custom resource. A
benchmark whose questions change between runs cannot be compared between runs,
so the catalogue ships in the operator image and every run records the digest
of the exact case list it used (spec.catalogue.revision).

A case is data:

    id            stable identifier, <family>-<shape>-<selectivity>
    family        one of FAMILIES; what spec.catalogue.select.families picks
    method        GET or POST
    path          resource path, placeholders allowed
    query         query string, placeholders allowed
    expectEngine  "elasticsearch" | "postgres" | None
                  Declared only where the eligibility rules in
                  ExtendedHSearchSearchBuilder make it unambiguous. A case
                  that declares an engine and is served by the other is an
                  INVALID measurement, never a silently accepted one. None
                  means "record which engine served it, assert nothing".
    predict       cardinality predictor, or None for "not predicted"
    note          why the case exists

Nothing here talks to a cluster and nothing here has a default: an unknown
family or case id raises rather than resolving to "everything".
"""

import hashlib
import json

import kopf

SNOMED = "http://snomed.info/sct"

FAMILIES = (
    "token", "string", "date", "quantity", "number", "uri", "composite",
    "reference", "chain", "has", "include", "fulltext", "missing", "paging",
    "sort", "total", "special", "everything", "filter", "pair",
)

# ParamPrefixEnum, all nine (ParamPrefixEnum.java:39-95).
PREFIXES = ("eq", "ne", "gt", "ge", "lt", "le", "sa", "eb", "ap")

# Read CODES_CompareOperation, NOT the CompareOperation enum. The enum has
# eighteen members (SearchFilterParser.java:368-387) but the codes list has only
# fifteen (:35-36), and parsing is CODES_CompareOperation.indexOf(s) ->
# values()[index]. The last three enum members -- ap, sa, eb -- are therefore
# unreachable through _filter no matter what the enum says. Counting the enum
# is what put them here, and it cost three cases that always returned HTTP 400
# (HAPI-1064) before the server ever looked at filter_search_enabled.
#
# pr, po and re are dropped again below the parser: no predicate builder in HAPI
# implements any of them. po raises HAPI-1255 from DatePredicateBuilder, re
# raises HAPI-1212 because reference predicates take only eq and ne, and pr has
# no handler at all -- it degrades to a token search for the literal "true" and
# returns nothing while still reporting 200, which is worse than an error.
#
# ss and sb also have no builder and are served as plain equality. They are kept
# because they do measure a real token search through the _filter path, but they
# are not measuring subsumption and the note says so.
FILTER_OPS = ("eq", "ne", "co", "sw", "ew", "gt", "lt", "ge", "le",
              "ss", "sb", "in")


def case(cid, family, path, query, engine=None, predict=None, method="GET", note=""):
    if family not in FAMILIES:
        raise ValueError("unknown family %r on case %s" % (family, cid))
    return {"id": cid, "family": family, "method": method, "path": path,
            "query": query, "expectEngine": engine, "predict": predict, "note": note}


# Predictors. The dataset generator records the share of resources carrying
# each code, so expected cardinality is computable before the query runs.
# Anything not expressible that way is predict=None -- the row count is
# recorded but not reconciled, and the case says so rather than pretending.
COND_COMMON = {"kind": "code", "type": "Condition", "pick": "common"}
COND_RARE = {"kind": "code", "type": "Condition", "pick": "rare"}


def _token():
    return [
        case("token-exact-common", "token", "/Condition",
             "code={{snomed}}|{{commonConditionCode}}", "elasticsearch", COND_COMMON,
             note="exact code, highest-weight -- the cheap common case"),
        case("token-exact-rare", "token", "/Condition",
             "code={{snomed}}|{{rareConditionCode}}", "elasticsearch", COND_RARE,
             note="exact code, lowest-weight -- selectivity control"),
        case("token-systemless-common", "token", "/Condition",
             "code={{commonConditionCode}}", "elasticsearch",
             note="code without system; matches on value alone"),
        case("token-or-two-codes", "token", "/Condition",
             "code={{snomed}}|{{commonConditionCode}},{{snomed}}|{{rareConditionCode}}",
             "elasticsearch", note="comma is OR within one parameter"),
        case("token-and-two-params", "token", "/Condition",
             "code={{snomed}}|{{commonConditionCode}}&clinical-status=active",
             "elasticsearch", note="repeated parameters are AND"),
        case("token-text", "token", "/Condition",
             "code:text={{commonConditionDisplay}}", "elasticsearch",
             note=":text is the token modifier HSearch does support"),
        case("token-not", "token", "/Condition",
             "code:not={{snomed}}|{{commonConditionCode}}", "postgres",
             note="PARAMQUALIFIER_TOKEN_NOT, HSearch-ineligible"),
        case("token-below", "token", "/Condition",
             "code:below={{snomed}}|{{commonConditionCode}}", "postgres",
             note="hierarchy descent; needs terminology expansion"),
        case("token-above", "token", "/Condition",
             "code:above={{snomed}}|{{rareConditionCode}}", "postgres",
             note="hierarchy ascent"),
        case("token-in", "token", "/Condition",
             "code:in=" + SNOMED + "?fhir_vs=isa/{{commonConditionCode}}", "postgres",
             note="implicit ValueSet expansion"),
        case("token-not-in", "token", "/Condition",
             "code:not-in=" + SNOMED + "?fhir_vs=isa/{{commonConditionCode}}", "postgres",
             note="negated ValueSet expansion"),
        case("token-of-type", "token", "/Patient",
             "identifier:of-type=http://terminology.hl7.org/CodeSystem/v2-0203|MR|{{mrn}}",
             "postgres",
             note="the synthetic identifiers carry no type coding, so this "
                  "measures the of-type predicate against an empty match"),
        case("token-status-final", "token", "/Observation",
             "status=final", "elasticsearch",
             note="whole-population token; every Observation matches"),
        case("token-gender", "token", "/Patient",
             "gender=female", "elasticsearch", note="half the population"),
    ]


def _string():
    return [
        case("string-bare", "string", "/Patient", "family={{patientFamily}}",
             "elasticsearch", note="default string match is starts-with"),
        case("string-exact", "string", "/Patient", "family:exact={{patientFamily}}",
             "elasticsearch", note="PARAMQUALIFIER_STRING_EXACT"),
        case("string-contains", "string", "/Patient",
             "family:contains={{patientFamilyFragment}}", "elasticsearch",
             note="PARAMQUALIFIER_STRING_CONTAINS; leading wildcard"),
        case("string-text", "string", "/Patient", "name:text={{patientFamily}}",
             "elasticsearch", note="PARAMQUALIFIER_STRING_TEXT"),
    ]


def _date():
    out = [case("date-eq-bare", "date", "/Observation", "date={{dateMid}}",
                "elasticsearch",
                note="no prefix: the only date shape HSearch accepts")]
    for prefix in PREFIXES:
        if prefix == "eq":
            continue
        out.append(case("date-%s" % prefix, "date", "/Observation",
                        "date=%s{{dateMid}}" % prefix, "elasticsearch",
                        note="ParamPrefixEnum %s. Measured on Elasticsearch: with "
                             "advanced_lucene_indexing on, HSearch takes prefixed "
                             "dates" % prefix))
    out.append(case("date-range", "date", "/Observation",
                    "date=ge{{dateFrom}}&date=le{{dateTo}}", "elasticsearch",
                    note="two-sided window, the shape every longitudinal measure uses"))
    out.append(case("date-lastupdated", "date", "/Condition",
                    "_lastUpdated=gt{{dateFrom}}", None,
                    note="_lastUpdated is special-cased into the HSearch clause "
                         "list using one bound only"))
    return out


def _quantity():
    out = [case("quantity-eq-bare", "quantity", "/Observation",
                "value-quantity={{quantityMid}}|http://unitsofmeasure.org|mg/dL",
                "elasticsearch", note="unprefixed quantity with system and unit")]
    for prefix in PREFIXES:
        if prefix == "eq":
            continue
        out.append(case("quantity-%s" % prefix, "quantity", "/Observation",
                        "value-quantity=%s{{quantityMid}}" % prefix, "elasticsearch",
                        note="ParamPrefixEnum %s against a quantity; measured on "
                             "Elasticsearch" % prefix))
    out.append(case("quantity-unit-conversion", "quantity", "/Observation",
                    "value-quantity=gt{{quantityMid}}|http://unitsofmeasure.org|g/dL",
                    "elasticsearch",
                    note="asks in g/dL for data indexed in mg/dL; hits the "
                         "normalised column rather than the raw one"))
    return out


def _number():
    return [
        case("number-gt", "number", "/RiskAssessment", "probability=gt0.5", None,
             note="createPredicateNumber. The synthetic dataset has no "
                  "RiskAssessment, so the honest result is zero rows through "
                  "the number predicate rather than no coverage at all"),
    ]


def _uri():
    return [
        case("uri-eq", "uri", "/ValueSet", "url=http://hl7.org/fhir/ValueSet/example",
             None, note="createPredicateUri, exact"),
        case("uri-below", "uri", "/ValueSet", "url:below=http://hl7.org", "postgres",
             note="createPredicateUri, prefix match"),
    ]


def _composite():
    return [
        case("composite-code-value", "composite", "/Observation",
             "code-value-quantity={{snomed}}|{{commonObservationCode}}${{quantityMid}}",
             "elasticsearch",
             note="the correct form: code and value tied in one indexed fact"),
        case("composite-naive-pair", "composite", "/Observation",
             "code={{snomed}}|{{commonObservationCode}}&value-quantity=gt{{quantityMid}}",
             "elasticsearch",
             note="the form real client code writes. Matches a patient whose "
                  "HbA1c is 6 and whose creatinine is 400; benchmarked because "
                  "it is what is actually deployed"),
    ]


def _reference():
    return [
        case("reference-direct-normal", "reference", "/Observation",
             "subject=Patient/{{normalPatientId}}", "elasticsearch",
             note="~53 observations"),
        case("reference-direct-heavy", "reference", "/Observation",
             "subject=Patient/{{heavyPatientId}}", "elasticsearch",
             note="~500 observations, same shape"),
        case("reference-typed", "reference", "/Observation",
             "subject:Patient={{normalPatientId}}", "elasticsearch",
             note=":[TargetType] qualifier; measured on Elasticsearch"),
        case("reference-identifier", "reference", "/Observation",
             "subject:identifier=http://perf.fhir/mrn|{{mrn}}", "elasticsearch",
             note="PARAMQUALIFIER_TOKEN_IDENTIFIER on a reference; measured on "
                  "Elasticsearch"),
        case("reference-mdm", "reference", "/Observation",
             "subject:mdm=Patient/{{normalPatientId}}", "postgres",
             note=":mdm falls through to return false in isParamTypeSupported"),
    ]


def _chain():
    return [
        case("chain-forward-name", "chain", "/Observation",
             "subject.name={{patientFamily}}", "postgres",
             note="two-stage forward chain"),
        case("chain-forward-gender", "chain", "/Condition",
             "subject.gender=female", "postgres",
             note="forward chain with a low-selectivity tail"),
        case("chain-deep", "chain", "/Observation",
             "subject.general-practitioner.name={{patientFamily}}", "postgres",
             note="two-hop chain. The dataset has no Practitioner, so this "
                  "measures the deep-chain machinery against an empty match"),
    ]


def _has():
    return [
        case("has-condition-code", "has", "/Patient",
             "_has:Condition:subject:code={{snomed}}|{{commonConditionCode}}", "postgres",
             note="the reverse chain -- how every cohort definition is phrased"),
        case("has-observation-status", "has", "/Patient",
             "_has:Observation:subject:status=final", "postgres",
             note="reverse chain with a whole-population tail"),
    ]


def _include():
    return [
        case("include-subject", "include", "/Condition",
             "code={{snomed}}|{{commonConditionCode}}&_include=Condition:subject",
             "elasticsearch",
             note="the SEARCH is HSearch-eligible and measured on Elasticsearch; "
                  "only the _include expansion reads hfj_res_link"),
        case("revinclude-condition", "include", "/Patient",
             "gender=female&_revinclude=Condition:subject", "elasticsearch",
             note="search on Elasticsearch; the _revinclude expansion is SQL"),
        case("include-iterate", "include", "/Condition",
             "code={{snomed}}|{{commonConditionCode}}"
             "&_include:iterate=Condition:subject", "elasticsearch",
             note="PARAM_INCLUDE_QUALIFIER_ITERATE"),
        case("include-recurse", "include", "/Condition",
             "code={{snomed}}|{{commonConditionCode}}"
             "&_include:recurse=Condition:subject", "elasticsearch",
             note="PARAM_INCLUDE_QUALIFIER_RECURSE"),
        case("include-wildcard", "include", "/Condition",
             "code={{snomed}}|{{commonConditionCode}}&_include=*", "elasticsearch",
             note="bring everything related -- a loaded gun, measured"),
    ]


def _fulltext():
    return [
        case("fulltext-content", "fulltext", "/Condition",
             "_content={{commonConditionDisplay}}", "elasticsearch",
             note="hard Msg.code(1192) when full-text is off; PE/PES only"),
        case("fulltext-text", "fulltext", "/Condition",
             "_text={{commonConditionDisplay}}", "elasticsearch",
             note="narrative search; PE/PES only"),
    ]


def _missing():
    return [
        case("missing-gender-true", "missing", "/Patient", "gender:missing=true",
             "postgres", note="the NOT EXISTS anti-join; a care-gap query"),
        case("missing-gender-false", "missing", "/Patient", "gender:missing=false",
             "postgres", note="the positive half of the same predicate"),
        case("missing-value-true", "missing", "/Observation",
             "value-quantity:missing=true", "postgres",
             note="absence on a quantity index"),
    ]


def _paging():
    out = []
    # DEFAULT_SEARCH_PRE_FETCH_THRESHOLDS = [13, 503, 2003, 1000003, -1].
    # The cost step-change is at the threshold, not at a page number.
    for offset in (0, 13, 503, 2003, 20000):
        out.append(case("paging-offset-%d" % offset, "paging", "/Condition",
                        "code={{snomed}}|{{commonConditionCode}}"
                        "&_offset=%d&_count=50" % offset, "elasticsearch",
                        note="_offset+_count is isOffsetQuery(): direct LIMIT/OFFSET, "
                             "bypassing the Search cache. Straddles pre-fetch "
                             "threshold boundaries"))
    for count in (10, 50, 200, 1000):
        out.append(case("paging-count-%d" % count, "paging", "/Condition",
                        "code={{snomed}}|{{commonConditionCode}}&_count=%d" % count,
                        "elasticsearch",
                        note="_count is in ourUnsafeSearchParmeters, so page size "
                             "alone disqualifies HSearch"))
    return out


def _sort():
    return [
        case("sort-lastupdated-asc", "sort", "/Condition",
             "code={{snomed}}|{{commonConditionCode}}&_sort=_lastUpdated",
             "elasticsearch", note="HSearch special-name map covers _lastUpdated"),
        case("sort-lastupdated-desc", "sort", "/Condition",
             "code={{snomed}}|{{commonConditionCode}}&_sort=-_lastUpdated",
             "elasticsearch", note="SortOrderEnum.DESC"),
        case("sort-date", "sort", "/Observation",
             "status=final&_sort=date", "elasticsearch", note="DATE is HSearch-sortable"),
        case("sort-value-quantity", "sort", "/Observation",
             "status=final&_sort=value-quantity", "elasticsearch",
             note="QUANTITY is HSearch-sortable"),
        case("sort-family", "sort", "/Patient", "gender=female&_sort=family",
             "elasticsearch", note="STRING is HSearch-sortable"),
        case("sort-id", "sort", "/Patient", "gender=female&_sort=_id", "elasticsearch",
             note="_id is in ourUnsafeSearchParmeters even as a sort key"),
        case("sort-chained", "sort", "/Observation",
             "status=final&_sort=subject.name", "postgres",
             note="supportsAllSortTerms fails on a chained sort"),
        case("sort-multi-key", "sort", "/Observation",
             "status=final&_sort=_lastUpdated,-date", None,
             note="multi-key SortSpec chain via getAllChainsInOrder()"),
    ]


def _total():
    return [
        case("total-none", "total", "/Condition",
             "code={{snomed}}|{{commonConditionCode}}&_total=none", None,
             note="SearchTotalModeEnum.NONE"),
        case("total-estimated", "total", "/Condition",
             "code={{snomed}}|{{commonConditionCode}}&_total=estimated", None,
             note="SearchTotalModeEnum.ESTIMATED"),
        case("total-accurate", "total", "/Condition",
             "code={{snomed}}|{{commonConditionCode}}&_total=accurate", None,
             note="SearchTotalModeEnum.ACCURATE -- what dashboards ask for by habit"),
    ]


def _special():
    out = []
    for value in ("count", "text", "data", "true", "false"):
        out.append(case("summary-%s" % value, "special", "/Condition",
                        "code={{snomed}}|{{commonConditionCode}}&_summary=%s" % value,
                        None, note="SummaryEnum.%s" % value.upper()))
    out.extend([
        case("elements-narrow", "special", "/Condition",
             "code={{snomed}}|{{commonConditionCode}}&_elements=id,subject", None,
             note="PARAM_ELEMENTS: trim the payload"),
        case("contained-true", "special", "/Observation",
             "code={{snomed}}|{{commonObservationCode}}&_contained=true", "postgres",
             note="SearchContainedModeEnum.TRUE; isSupportsAllOf requires FALSE"),
        case("contained-both", "special", "/Observation",
             "code={{snomed}}|{{commonObservationCode}}&_contained=both", "postgres",
             note="SearchContainedModeEnum.BOTH"),
        case("admin-tag", "special", "/Condition", "_tag={{datasetTag}}", None,
             note="createPredicateTag or createPredicateSearchParameter "
                  "depending on getTagStorageMode()"),
        case("admin-profile", "special", "/Condition",
             "_profile=http://hl7.org/fhir/StructureDefinition/Condition", None,
             note="PARAM_PROFILE, same branch as _tag"),
        case("admin-security", "special", "/Condition",
             "_security=http://terminology.hl7.org/CodeSystem/v3-Confidentiality|N",
             None, note="PARAM_SECURITY"),
        case("admin-source", "special", "/Condition", "_source=http://perf.fhir", None,
             note="createPredicateSource"),
        case("admin-id", "special", "/Patient", "_id={{normalPatientId}}", "postgres",
             note="createPredicateResourceId; _id is unsafe for HSearch by name"),
        case("admin-language", "special", "/Patient", "_language=en", None,
             note="PARAM_LANGUAGE"),
        case("admin-list", "special", "/Patient", "_list=nonexistent-list", None,
             note="PARAM_LIST"),
        case("admin-type", "special", "/", "_type=Patient&gender=female", None,
             note="PARAM_TYPE: cross-type search, "
                  "createPredicateTokenForMultipleResourceTypes"),
        case("post-search", "special", "/Condition/_search",
             "code={{snomed}}|{{commonConditionCode}}", None, method="POST",
             note="PARAM_SEARCH: the same query as a form-encoded POST body"),
    ])
    return out


def _everything():
    return [
        case("everything-patient-instance", "everything",
             "/Patient/{{heavyPatientId}}/$everything", "", "postgres",
             note="EverythingModeEnum.PATIENT_INSTANCE; include page size 50"),
        case("everything-patient-type", "everything", "/Patient/$everything",
             "_count=50", "postgres", note="EverythingModeEnum.PATIENT_TYPE"),
    ]


# _filter is disabled by default (myFilterParameterEnabled = false). Selecting
# this family without a configure step that enables it produces HTTP 400 on
# every case, which is recorded as ERROR -- not skipped, not retried.
_FILTER_OPERANDS = {
    "eq": "code eq {{commonConditionCode}}",
    "ne": "code ne {{commonConditionCode}}",
    "co": "code co {{commonConditionCode}}",
    "sw": "code sw {{commonConditionCode}}",
    "ew": "code ew {{commonConditionCode}}",
    # onset-date, not recorded-date. The generator writes onsetDateTime and
    # leaves recordedDate empty, so recorded-date:missing=true matches all
    # 189000 Conditions and every date filter on it timed an empty result set
    # while reporting 200.
    "gt": "onset-date gt {{dateFrom}}",
    "lt": "onset-date lt {{dateTo}}",
    "ge": "onset-date ge {{dateFrom}}",
    "le": "onset-date le {{dateTo}}",
    "ss": "code ss {{commonConditionCode}}",
    "sb": "code sb {{commonConditionCode}}",
    "in": "code in " + SNOMED + "?fhir_vs=isa/{{commonConditionCode}}",
}


_FILTER_NOTES = {
    "ss": "no subsumption builder exists; HAPI serves ss as plain equality",
    "sb": "no subsumption builder exists; HAPI serves sb as plain equality",
    "in": "implicit ValueSet expansion; returns 0 until a CodeSystem is loaded",
}


def _filter():
    out = []
    for op in FILTER_OPS:
        out.append(case("filter-op-%s" % op, "filter", "/Condition",
                        "_filter=" + _FILTER_OPERANDS[op], "postgres",
                        note=_FILTER_NOTES.get(op, "SearchFilterParser CompareOperation %s" % op)))
    out.extend([
        case("filter-and", "filter", "/Condition",
             "_filter=code eq {{commonConditionCode}} and clinical-status eq active",
             "postgres", note="FilterLogicalOperation and"),
        case("filter-or", "filter", "/Condition",
             "_filter=code eq {{commonConditionCode}} or code eq {{rareConditionCode}}",
             "postgres", note="FilterLogicalOperation or"),
        # No filter-not. FilterLogicalOperation.not exists in the enum but is
        # unimplemented both ways round: the prefix form "not (A)" reaches
        # parseLogical(null), which never builds the logical node and fails with
        # HAPI-1056, and the infix form "A not B" parses and then throws
        # HAPI-1205 out of QueryStack. There is no spelling of it that runs.
        case("filter-group", "filter", "/Condition",
             "_filter=(code eq {{commonConditionCode}} or code eq {{rareConditionCode}})"
             " and clinical-status eq active", "postgres",
             note="nested parameterGroup, FilterItemType"),
    ])
    return out


# The matched pairs. Same cohort, one term different, across the Hibernate
# Search eligibility boundary. The delta within a pair is the measured value of
# Elasticsearch for that shape, and is the single most important output.
_PAIRS = (
    ("unsafe-id", "/Condition", "code={{snomed}}|{{commonConditionCode}}",
     "/Condition", "code={{snomed}}|{{commonConditionCode}}&_id={{normalPatientId}}",
     "ourUnsafeSearchParmeters contains _id"),
    ("unsafe-count", "/Condition", "code={{snomed}}|{{commonConditionCode}}",
     "/Condition", "code={{snomed}}|{{commonConditionCode}}&_count=50",
     "ourUnsafeSearchParmeters contains _count"),
    ("include", "/Condition", "code={{snomed}}|{{commonConditionCode}}",
     "/Condition", "code={{snomed}}|{{commonConditionCode}}&_include=Condition:subject",
     "isSupportsAllOf rejects any _include"),
    ("revinclude", "/Patient", "gender=female",
     "/Patient", "gender=female&_revinclude=Condition:subject",
     "isSupportsAllOf rejects any _revinclude"),
    ("chain", "/Observation", "subject=Patient/{{normalPatientId}}",
     "/Observation", "subject.name={{patientFamily}}",
     "reference support requires a null chain"),
    ("token-below", "/Condition", "code={{snomed}}|{{commonConditionCode}}",
     "/Condition", "code:below={{snomed}}|{{commonConditionCode}}",
     ":below is not a supported token modifier"),
    ("token-not", "/Condition", "code={{snomed}}|{{commonConditionCode}}",
     "/Condition", "code:not={{snomed}}|{{commonConditionCode}}",
     ":not is not a supported token modifier"),
    ("missing", "/Patient", "gender=female",
     "/Patient", "gender:missing=true",
     ":missing is not a supported modifier on any type"),
    ("date-prefix", "/Observation", "date={{dateMid}}",
     "/Observation", "date=ge{{dateMid}}",
     "date support is bare-modifier only; a prefix disqualifies"),
    ("contained", "/Observation", "code={{snomed}}|{{commonObservationCode}}",
     "/Observation", "code={{snomed}}|{{commonObservationCode}}&_contained=true",
     "isSupportsAllOf requires searchContainedMode == FALSE"),
    ("everything", "/Patient", "_id={{heavyPatientId}}",
     "/Patient/{{heavyPatientId}}/$everything", "",
     "isSupportsAllOf requires everythingMode == null"),
    ("sort", "/Condition", "code={{snomed}}|{{commonConditionCode}}&_sort=_lastUpdated",
     "/Condition", "code={{snomed}}|{{commonConditionCode}}&_sort=subject.name",
     "supportsAllSortTerms fails on a chained sort key"),
)


# The disqualified half is not automatically PostgreSQL. With
# advanced_lucene_indexing on, four of these shapes are still served by
# Elasticsearch -- _count and a date prefix do not disqualify at all, and for
# _include/_revinclude only the expansion is SQL while the search itself stays
# on HSearch. Generating every -b as "postgres" is what made pair-include-b and
# pair-revinclude-b pass for the wrong reason. Measured 2026-09-17; anything not
# named here is PostgreSQL.
_PAIR_B_ENGINE = {
    "unsafe-count": "elasticsearch",
    "include": "elasticsearch",
    "revinclude": "elasticsearch",
    "date-prefix": "elasticsearch",
}


def _pairs():
    out = []
    for name, path_a, query_a, path_b, query_b, why in _PAIRS:
        # pair-unsafe-id and pair-everything both have an _id in the eligible
        # member, so neither member of those two can be declared elasticsearch.
        eligible = None if "_id=" in query_a else "elasticsearch"
        out.append(case("pair-%s-a" % name, "pair", path_a, query_a, eligible,
                        note="eligible member of pair %s" % name))
        out.append(case("pair-%s-b" % name, "pair", path_b, query_b,
                        _PAIR_B_ENGINE.get(name, "postgres"),
                        note="disqualified by: %s" % why))
    return out


CATALOGUES = {
    "cohort-core": (_token() + _string() + _date() + _quantity() + _number()
                    + _uri() + _composite() + _reference() + _chain() + _has()
                    + _include() + _fulltext() + _missing() + _paging() + _sort()
                    + _total() + _special() + _everything() + _filter() + _pairs()),
}


def _canonical(cases):
    return json.dumps(cases, sort_keys=True, separators=(",", ":")).encode("utf-8")


def revision(name):
    """sha256 of the exact case list, so a run records what it actually asked."""
    return "sha256:" + hashlib.sha256(_canonical(load(name))).hexdigest()


def load(name):
    found = CATALOGUES.get(name)
    if found is None:
        raise kopf.PermanentError(
            "no catalogue named %r in this operator image; have: %s"
            % (name, ", ".join(sorted(CATALOGUES))))
    return found


def duplicate_ids(name):
    seen, dupes = set(), []
    for item in load(name):
        if item["id"] in seen:
            dupes.append(item["id"])
        seen.add(item["id"])
    return dupes


def select(catalogue_spec):
    """The ordered case list this benchmark runs, and the catalogue digest.

    Nothing here widens a selection. An unknown family, an unknown case id, a
    selection that matches nothing, or a revision that does not match the
    catalogue in this image are all PermanentErrors.
    """
    spec = catalogue_spec or {}
    name = spec.get("name") or "cohort-core"
    cases = load(name)
    digest = revision(name)

    dupes = duplicate_ids(name)
    if dupes:
        raise kopf.PermanentError(
            "catalogue %s has duplicate case ids: %s" % (name, ", ".join(sorted(set(dupes)))))

    pinned = (spec.get("revision") or "").strip()
    if pinned and pinned != digest:
        raise kopf.PermanentError(
            "spec.catalogue.revision pins %s but catalogue %s in this operator image "
            "is %s" % (pinned, name, digest))

    chosen = spec.get("select") or {}
    families = [f for f in (chosen.get("families") or []) if f]
    ids = [c for c in (chosen.get("cases") or []) if c]
    exclude = set(c for c in (chosen.get("exclude") or []) if c)

    if families and ids:
        raise kopf.PermanentError(
            "spec.catalogue.select.families and .cases are mutually exclusive; "
            "both are set")

    known_families = set(item["family"] for item in cases)
    unknown = [f for f in families if f not in known_families]
    if unknown:
        raise kopf.PermanentError(
            "spec.catalogue.select.families names families that catalogue %s does not "
            "have: %s. Available: %s"
            % (name, ", ".join(unknown), ", ".join(sorted(known_families))))

    known_ids = set(item["id"] for item in cases)
    unknown = [c for c in ids if c not in known_ids]
    if unknown:
        raise kopf.PermanentError(
            "spec.catalogue.select.cases names case ids that catalogue %s does not "
            "have: %s" % (name, ", ".join(unknown)))

    unknown = [c for c in exclude if c not in known_ids]
    if unknown:
        raise kopf.PermanentError(
            "spec.catalogue.select.exclude names case ids that catalogue %s does not "
            "have: %s" % (name, ", ".join(sorted(unknown))))

    if families:
        picked = [item for item in cases if item["family"] in families]
    elif ids:
        picked = [item for item in cases if item["id"] in ids]
    else:
        picked = list(cases)
    picked = [item for item in picked if item["id"] not in exclude]

    if not picked:
        raise kopf.PermanentError(
            "spec.catalogue.select matched no cases in catalogue %s" % name)
    return picked, digest, name


def families_in(name="cohort-core"):
    """[(family, count), ...] -- what the UI lists as selectable."""
    counts = {}
    for item in load(name):
        counts[item["family"]] = counts.get(item["family"], 0) + 1
    return sorted(counts.items())

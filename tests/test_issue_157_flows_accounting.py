"""Issue #157 (M12.19) — the persistent served-flows baseline and its accounting.

The corpus under ``tests/fixtures/governance/issue_157/`` freezes, per case,
the complete ordered ``flows`` and endpoint/provenance sequences the seven
legacy producers emitted at the step-0 baseline. These tests are the
two-sided accounting over it: every frozen row is discharged (full-payload
match through the canonical projection), retired (a typed record), or pending
for #159 — and the derived output must equal the discharged sequence in order.

Every mutant below is a REAL corruption applied to an in-memory copy of a
frozen case, so each guard is shown to fail closed on the exact shape it
exists to catch, not merely to pass on the clean corpus.
"""

from __future__ import annotations

import copy
import json
import sys
from pathlib import Path
from unittest.mock import MagicMock, patch

import pytest

_src = str(Path(__file__).resolve().parent.parent / "src")
if _src not in sys.path:
    sys.path.insert(0, _src)
sys.path.insert(0, str(Path(__file__).resolve().parent))

import _issue_157_flows_accounting as acct  # noqa: E402
from _issue_157_flows_accounting import (  # noqa: E402
    BASELINE_SHA,
    CASES,
    DIFFERENTIAL_PAIRS,
    DISCHARGED,
    PAIR_CLASSIFICATION,
    PENDING,
    PRODUCER_MODULES,
    Case,
    derive_case_endpoints,
    derive_case_projection,
    discharge_case,
    discharge_endpoints,
    load_cases,
    load_manifest,
    retirement_ids,
    verify_manifest,
)
from boomi_mcp.authoring.derived_flows import (  # noqa: E402
    FUTURE_BUILDER_ISSUE_BY_ROUTE,
    NORMALIZATION_RULES,
    normalize_flow_row,
)

_PAGINATE = "boomi_mcp.categories.integration_builder.paginate_metadata"


@pytest.fixture(scope="module")
def cases():
    return load_cases()


@pytest.fixture(scope="module")
def manifest():
    return load_manifest()


@pytest.fixture(scope="module")
def derived(cases):
    """The canonical projection of every case that carries a canonical fixture."""
    out = {}
    with patch(_PAGINATE, lambda *a, **k: []):
        for case_id, case in cases.items():
            out[case_id] = derive_case_projection(case)
    return out


@pytest.fixture(scope="module")
def replay(cases, derived):
    """Per case ``(flows report, endpoints report)`` on the replay's own mode."""
    retirements = retirement_ids()
    out = {}
    with patch(_PAGINATE, lambda *a, **k: []):
        for case_id, case in cases.items():
            out[case_id] = (
                discharge_case(case, derived[case_id], retirements),
                discharge_endpoints(case, derive_case_endpoints(case), retirements),
            )
    return out


def _completed(replay):
    """The cases whose canonical replay reproduces BOTH full frozen sequences."""
    return {case_id for case_id, (flows, endpoints) in replay.items() if flows.completed and endpoints.completed}


# ---------------------------------------------------------------------------
# the corpus itself
# ---------------------------------------------------------------------------


def test_the_corpus_was_frozen_from_the_step_zero_baseline(manifest):
    assert manifest["baseline_sha"] == BASELINE_SHA
    assert manifest["issue"] == 157
    assert "git archive" in manifest["extraction"]
    assert set(manifest["producers"]) == set(PRODUCER_MODULES)


def test_every_declared_case_is_frozen_and_every_frozen_case_is_declared(cases, manifest):
    assert set(cases) == set(CASES) == set(manifest["cases"])
    # all seven producers are represented — the census covers the whole family
    assert {case.producer for case in cases.values()} == set(PRODUCER_MODULES)


def test_manifest_digests_hold():
    assert verify_manifest() == []


#: The producers whose rich ``transform`` row #157 discharged, and the producer
#: #159 S1 discharges WHOLE (every flow and endpoint/provenance row).
_RICH_PRODUCERS = ("database_to_api_sync", "api_to_api_sync")
_S1_PRODUCERS = ("composition",)


def _owed_discharges(cases, completed=()):
    """``(flow, endpoint)`` row identities the replay owes, DERIVED from the
    corpus and the scope rule — never a hand-typed count.

    The FLOOR is #157's rich transform rows and every composition row (#159
    S1); ``completed`` (measured by the replay) adds every row of each case
    whose canonical replay reproduces its full sequences (#159 S2)."""
    flows, endpoints = set(), set()
    for case_id, case in cases.items():
        whole = case.producer in _S1_PRODUCERS or case_id in completed
        for row in case.flow_rows:
            if whole or (case.producer in _RICH_PRODUCERS and row["key"] == "transform"):
                flows.add((case_id, row["ordinal"]))
        if whole:
            endpoints.update((case_id, row["ordinal"]) for row in case.endpoint_rows)
    return flows, endpoints


def test_frozen_row_counts_are_pinned(cases, manifest, replay):
    """The denominator is pinned so a deregistration or regeneration cannot shrink it.

    The discharged count lies between the floor (#157 + S1) and the rows the
    replay MEASURES as reproduced; the manifest's counts are the disk's."""
    assert manifest["frozen_flow_rows"] == 83
    assert manifest["frozen_endpoint_rows"] == 58
    floor_flows, floor_endpoints = _owed_discharges(cases)
    owed_flows, owed_endpoints = _owed_discharges(cases, _completed(replay))
    counts = acct._status_counts(acct.CORPUS_DIR)
    total = manifest["frozen_flow_rows"] + manifest["frozen_endpoint_rows"]
    assert counts["retired"] == 0
    assert len(floor_flows) + len(floor_endpoints) <= counts["discharged"] <= len(owed_flows) + len(owed_endpoints)
    assert counts["pending"] == total - counts["discharged"] - counts["retired"]
    if acct.FINAL_REPLAY:
        assert counts["pending"] == 0
    assert manifest["status_counts"] == counts


def test_every_frozen_row_has_exactly_one_status(cases):
    for case in cases.values():
        assert set(case.discharges["flows"]) == {str(r["ordinal"]) for r in case.flow_rows}, case.case_id
        assert set(case.discharges["endpoints"]) == {str(r["ordinal"]) for r in case.endpoint_rows}, case.case_id
        for status in list(case.discharges["flows"].values()) + list(case.discharges["endpoints"].values()):
            assert status == DISCHARGED or status == PENDING or status.startswith("retired:"), status


def test_row_identities_are_unique_within_a_case_and_keep_their_ordinal(cases):
    for case in cases.values():
        identities = [(r["producer"], r["case_id"], r["key"], r["kind"]) for r in case.flow_rows]
        assert len(set(identities)) == len(identities), case.case_id
        assert [r["ordinal"] for r in case.flow_rows] == list(range(len(case.flow_rows))), case.case_id


def test_every_frozen_payload_matches_its_own_digest(cases):
    for case in cases.values():
        for row in case.flow_rows + case.endpoint_rows:
            assert acct._digest(row["payload"]) == row["payload_sha256"], (case.case_id, row["ordinal"])


def test_the_normalized_payload_is_the_one_normalizer_applied_to_the_raw_row(cases):
    """The frozen `payload` IS `normalize_flow_row(raw)` — re-derived, not trusted."""
    for case in cases.values():
        for row in case.flow_rows:
            assert normalize_flow_row(row["raw"]) == row["payload"], (case.case_id, row["ordinal"])


# ---------------------------------------------------------------------------
# normalization rules are measured, not asserted
# ---------------------------------------------------------------------------


def test_future_builder_issue_is_a_pure_function_of_the_route_on_every_frozen_row(cases):
    """Rule R2's premise, measured over the whole corpus."""
    seen = 0
    for case in cases.values():
        for row in case.flow_rows:
            for op in row["raw"].get("operations") or ():
                if "future_builder_issue" in op:
                    seen += 1
                    assert op["future_builder_issue"] == FUTURE_BUILDER_ISSUE_BY_ROUTE[op["operation_type"]]
    assert seen > 0, "no annotated operation in the corpus — the measurement would be vacuous"


def test_direct_source_field_and_source_path_agree_wherever_both_are_served(cases):
    """Rule R4's premise: the API form serves both spellings, and they never differ."""
    both = 0
    for case in cases.values():
        for row in case.flow_rows:
            for op in row["raw"].get("operations") or ():
                if op.get("operation_type") == "direct" and "source_field" in op and "source_path" in op:
                    both += 1
                    assert op["source_field"] == op["source_path"]
    assert both > 0


def test_the_rules_are_recorded():
    assert len(NORMALIZATION_RULES) == 4
    assert NORMALIZATION_RULES[0].startswith("R1 script_body")


# ---------------------------------------------------------------------------
# differential pairs: classified BEFORE they are asserted
# ---------------------------------------------------------------------------


def _row(case: Case, key: str):
    return next((r for r in case.flow_rows if r["key"] == key), None)


@pytest.mark.parametrize("parameter,base_id,varied_id", DIFFERENTIAL_PAIRS)
def test_each_differential_pair_manifests_the_way_its_classification_says(cases, parameter, base_id, varied_id):
    base, varied = cases[base_id], cases[varied_id]
    rule = PAIR_CLASSIFICATION[parameter]
    if rule["kind"] == "presence":
        # the ONLY conditional row: absent on the base case, present on the varied one
        assert _row(base, rule["row_key"]) is None
        assert _row(varied, rule["row_key"]) is not None
        others_base = [r["key"] for r in base.flow_rows]
        others_varied = [r["key"] for r in varied.flow_rows if r["key"] != rule["row_key"]]
        assert others_base == others_varied
    elif rule["kind"] == "payload":
        # a PERSISTENT row whose content varies: present on both, differing in one field
        a, b = _row(base, rule["row_key"]), _row(varied, rule["row_key"])
        assert a is not None and b is not None
        assert a["payload"][rule["field"]] != b["payload"][rule["field"]]
        assert a["payload_sha256"] != b["payload_sha256"]
        assert [r["key"] for r in base.flow_rows] == [r["key"] for r in varied.flow_rows]
    else:
        # binding mode varies NO flows row — only the endpoint oracle moves
        assert [r["payload_sha256"] for r in base.flow_rows] == [r["payload_sha256"] for r in varied.flow_rows]
        assert [r["payload_sha256"] for r in base.endpoint_rows] != [r["payload_sha256"] for r in varied.endpoint_rows]


def test_a_presence_assertion_on_a_persistent_row_cannot_pass(cases):
    """Why classification precedes the pair: the reliability row is ALWAYS there."""
    assert all(_row(case, "reliability") is not None for case in cases.values() if case.producer == "database_to_api_sync")


# ---------------------------------------------------------------------------
# the accounting over the real corpus
# ---------------------------------------------------------------------------


def test_every_case_discharges_cleanly(cases, derived):
    """The REAL route, including the served rows the name check reads.

    Passing them is what makes that check reachable at all: the normalized rows
    the digest compares have had the name dropped by R3 on both sides.
    """
    from _issue_157_flows_accounting import derive_served_projection

    retirements = retirement_ids()
    # OFFLINE, like every other replay here: the projection drives the real
    # planning entry, and only the live metadata boundary is faked.
    with patch(_PAGINATE, lambda *a, **k: []):
        served = {case_id: derive_served_projection(case) for case_id, case in cases.items()}
    for case_id, case in cases.items():
        report = discharge_case(
            case, derived[case_id], retirements, served=served[case_id]
        )
        assert report.ok, (case_id, report.problems)
        with patch(_PAGINATE, lambda *a, **k: []):
            endpoints = derive_case_endpoints(case)
        endpoint_report = discharge_endpoints(case, endpoints, retirements)
        assert endpoint_report.ok, (case_id, endpoint_report.problems)


def test_the_two_rich_forms_are_discharged_by_full_payload_equality(cases, derived, replay):
    """#157's own obligation: every DB-source and API-source transform row —
    inspected WITHIN the full derived sequence (#159 S2): the transform occurs
    exactly once, by full payload, and on a completed case at its frozen
    ordinal; a case not yet completed derives that row alone."""
    discharged = 0
    for case_id, case in cases.items():
        if case.producer not in _RICH_PRODUCERS:
            continue
        row = _row(case, "transform")
        assert case.status(row["ordinal"]) == DISCHARGED, case_id
        assert case.input_canonical is not None and case.provenance is not None, case_id
        assert case.provenance["executed_at_replay"] is False
        digests = [acct._digest(r) for r in derived[case_id]]
        assert digests.count(row["payload_sha256"]) == 1, case_id
        if case_id in _completed(replay):
            assert digests.index(row["payload_sha256"]) == row["ordinal"], case_id
            assert len(digests) == len(case.flow_rows), case_id
        else:
            assert digests == [row["payload_sha256"]], case_id
        discharged += 1
    assert discharged == sum(1 for case in cases.values() if case.producer in _RICH_PRODUCERS) > 0
    # non-vacuity of the full-sequence arm: the API->API direct roots complete
    assert {c for c in _completed(replay) if c.startswith("api_to_api_sync/")} == {
        c for c in cases if c.startswith("api_to_api_sync/")
    }


def test_the_remaining_rows_are_explicitly_pending_for_159_not_labelled_covered(cases, manifest, replay):
    """Every row the replay does not owe is PENDING; every row it owes is discharged.

    The floor is derived from the corpus and the scope rule
    (``_owed_discharges``): the rich producers' transform rows (#157) and every
    composition row (#159 S1) are DISCHARGED. A row of a case the replay
    MEASURES as completed may be discharged or still pending (reproduced,
    disposition pending); every other row is PENDING.

    THE ONE-LINE SWITCH: ``acct.FINAL_REPLAY = True`` turns this witness, in
    place, into the final no-pending assertion — every case completed, every
    row discharged.
    """
    completed = _completed(replay)
    if acct.FINAL_REPLAY:
        assert completed == set(cases), sorted(set(cases) - completed)
        for case in cases.values():
            for side in ("flows", "endpoints"):
                assert set(case.discharges[side].values()) == {DISCHARGED}, (case.case_id, side)
        return
    floor_flows, floor_endpoints = _owed_discharges(cases)
    owed_flows, owed_endpoints = _owed_discharges(cases, completed)
    for case_id, case in cases.items():
        for side, rows, floor, owed in (
            ("flows", case.flow_rows, floor_flows, owed_flows),
            ("endpoints", case.endpoint_rows, floor_endpoints, owed_endpoints),
        ):
            for row in rows:
                status = case.status(row["ordinal"], side=side)
                identity = (case_id, row["ordinal"])
                if identity in floor:
                    allowed = {DISCHARGED}
                elif identity in owed:
                    allowed = {DISCHARGED, PENDING}
                else:
                    allowed = {PENDING}
                assert status in allowed, (case_id, side, row["key"], status)
    pending_flows = sum(
        1 for case in cases.values() for r in case.flow_rows if case.status(r["ordinal"]) == PENDING
    )
    discharged_flows = sum(
        1 for case in cases.values() for r in case.flow_rows if case.status(r["ordinal"]) == DISCHARGED
    )
    assert pending_flows == manifest["frozen_flow_rows"] - discharged_flows
    assert len(floor_flows) <= discharged_flows <= len(owed_flows)


def test_the_replay_never_touches_a_legacy_producer(cases, monkeypatch):
    """Canonical-only on the input side: the replay imports no archetype/composition module."""
    import importlib

    case = cases["database_to_api_sync/wm_off_dlq_off_create"]
    touched = []
    real_import = importlib.import_module

    def spy(name, *args, **kwargs):
        if name.startswith("boomi_mcp.patterns.archetypes") or name == "boomi_mcp.patterns.composition":
            touched.append(name)
        return real_import(name, *args, **kwargs)

    monkeypatch.setattr(importlib, "import_module", spy)
    # The module's plan cache would answer from a plan made BEFORE the spy was
    # installed, and the replay would then import nothing for a vacuous reason.
    monkeypatch.setattr(acct, "_PLAN_CACHE", {})
    composition = cases["composition/create_2legs_staged"]
    with patch(_PAGINATE, lambda *a, **k: []):
        derive_case_projection(case)
        derive_case_projection(composition)
        derive_case_endpoints(composition)
    assert touched == []


# ---------------------------------------------------------------------------
# #159 S1: the import-blocked batch replay
# ---------------------------------------------------------------------------


def _binds_a_reused_connection(case):
    """Whether the case's canonical request binds an existing connection.

    Read from the fixture itself: a reuse binding mode on a recipe invocation,
    or a reference-only connector component on a direct request.
    """
    intent = (case.input_canonical or {}).get("intent") or {}
    for invocation in intent.get("invocations") or ():
        raw = invocation.get("raw_input") or {}
        modes = [raw.get("source", {}).get("binding_mode")]
        modes += [target.get("binding_mode") for target in raw.get("targets") or ()]
        if "reuse" in modes:
            return True
    return any(
        (component.get("config") or {}).get("reference_only")
        for component in intent.get("components") or ()
        if "connector" in str(component.get("type"))
    )


@pytest.fixture(scope="module")
def batch():
    """ONE fresh child over every canonical case (composition + direct)."""
    return acct.run_import_blocked_batch(acct.canonical_case_ids())


def test_the_completed_subset_replays_in_one_import_blocked_child(cases, derived, batch):
    """The legacy producers blocked, their registrations and source modules
    deregistered, and the real revision/planning/projection code running: every
    case still discharges, and its component plan is the one the unblocked tree
    builds."""
    retirements = retirement_ids()
    replayed = batch["cases"]
    assert set(replayed) == set(acct.canonical_case_ids())
    assert {case_id.split("/")[0] for case_id in replayed} >= {"composition", *_RICH_PRODUCERS}
    assert batch["attempts"] == []
    for case_id, child in replayed.items():
        case = cases[case_id]
        report = discharge_case(case, child["flows"], retirements, served=child["served"])
        assert report.ok, (case_id, report.problems)
        endpoints = discharge_endpoints(case, child["endpoints"], retirements)
        assert endpoints.ok, (case_id, endpoints.problems)
        assert child["flows"] == derived[case_id], case_id
        # the REAL component plan, compared with the unblocked tree's own
        with patch(_PAGINATE, lambda *a, **k: []):
            parent = acct.case_plan_result(case)
        assert child["plan_valid"] is bool(parent.validation_report.is_valid), case_id
        assert child["plan_errors"] == sorted({d.code for d in parent.errors}), case_id
        assert child["component_dependencies"] == len(parent.component_dependencies), case_id
        # a route that binds any real endpoint plans its supporting components
        # (the zero-component stub binds none — its frozen endpoints are stubs)
        if any(row["payload"].get("type") != "stub" for row in case.endpoint_rows):
            assert child["component_dependencies"] > 0, case_id
        # ARCH-r1-12: EVERY case — a reused connection included — must plan and
        # compile for real. A reused connection's account read is answered by
        # a SHA-pinned platform capture of a proven-operable connection of the
        # same family (``acct.replay_identity_captures``), in parent and child.
        assert child["plan_valid"] and child["plan_errors"] == [], (case_id, child["plan_errors"])
        assert child["compile_valid"] is True, (case_id, child["compile_error"])
    # Non-vacuity: the reuse cases really are in the batch — every replayed
    # case the corpus NAMES a reuse case (its case id), and only those.
    reuse_named = {case_id for case_id in replayed if "reuse" in case_id.split("/")[1]}
    assert reuse_named and {
        case_id for case_id in replayed if _binds_a_reused_connection(cases[case_id])
    } == reuse_named
    applied = batch["apply"]
    assert applied["success"] is True, applied
    assert applied["created_process"] and applied["attempts_during_apply"] == []


def test_the_pinned_flagship_dispatches_plan_and_compile_in_the_blocked_child(batch):
    """ARCH-r1-13: the pinned zero-prebuilt-pair flagship through the PUBLIC
    dispatch (plan -> compile) with the legacy producers import-blocked: both
    succeed, no forbidden import is attempted, the supporting components equal
    the frozen oracle (one documented re-spelling) and the process envelope
    equals the legacy process's."""
    from _fanout_v2_support import oracle, respell_oracle_credential_refs

    flagship = batch["flagship"]
    assert flagship["attempts"] == []
    assert flagship["plan_success"] and flagship["compile_success"] and flagship["compile_valid"]
    fields = ("key", "type", "action", "name", "config", "depends_on")
    frozen = respell_oracle_credential_refs(oracle()["integration_spec"]["components"])
    expected = [{f: c[f] for f in fields} for c in frozen if c["type"] != "process"]
    assert [{f: c[f] for f in fields} for c in flagship["components"]] == expected
    process = frozen[-1]
    envelope = flagship["envelope"]
    assert (envelope["component_key"], envelope["name"], envelope["action"]) == (
        process["key"], process["name"], process["action"]
    )
    assert envelope["depends_on"] == sorted(process["depends_on"])
    assert envelope["process_extensions"] == process["config"]["process_extensions"]


def test_the_replay_identity_stand_in_is_not_vacuous():
    """Controls for ARCH-r1-12: without the capture the same reuse case still
    refuses CONNECTOR_REPLAY_IDENTITY_UNAVAILABLE, and a capture of the WRONG
    family bound to the placeholder is refused rather than accepted."""
    from unittest.mock import MagicMock

    from boomi_mcp.authoring.workflow import plan_authoring_request_v1
    from boomi_mcp.models.authoring_workflow import AuthoringRequestV1

    case = load_cases()["composition/reuse_2legs_stream"]
    request = AuthoringRequestV1.model_validate(case.input_canonical)

    def plan(get_xml):
        with patch(_PAGINATE, lambda *a, **k: []), patch(acct._GET_XML, get_xml):
            result, _ = plan_authoring_request_v1(
                request, boomi_client=MagicMock(), profile="issue-157-replay"
            )
        return result

    answers = acct.replay_identity_captures()
    assert plan(acct.replay_component_get_xml(answers)).validation_report.is_valid
    refused = plan(acct.replay_component_get_xml({}))
    assert "CONNECTOR_REPLAY_IDENTITY_UNAVAILABLE" in {d.code for d in refused.errors}
    swapped = dict(answers)
    swapped["<<existing REST connection id>>"] = answers["<<existing connector id>>"]
    wrong_family = plan(acct.replay_component_get_xml(swapped))
    assert not wrong_family.validation_report.is_valid


def test_the_import_blocker_refuses_a_known_forbidden_import(batch):
    """Non-vacuity: in that same child a deliberate legacy import IS refused, and
    the simulated deregistration really removed what it claims to."""
    assert batch["control"] == "refused"
    assert not any(
        name.startswith(prefix)
        for name in batch["layer_modules"]
        for prefix in acct.LEGACY_PRODUCER_PREFIXES
    )
    registered = {tuple(item) for item in batch["registrations"]}
    assert not any(recipe_id.startswith(acct.ADAPTER_ID_PREFIX) for recipe_id, _v in registered)
    assert not registered & set(acct.obsolete_registrations())
    assert ("boomi.compose.db_rest_fanout", "2.0.0") in registered
    # #159 S2: the PLANNED retirement is simulated too, API->API included —
    # each planned id is registered in this (parent) tree, so the filter is real
    from boomi_mcp.recipes.builtins import catalog

    parent = {(reg.recipe_id, reg.recipe_version) for reg in catalog.PRODUCTION_REGISTRATIONS}
    for recipe_id in acct.PLANNED_RETIREMENT_RECIPE_IDS:
        assert any(rid == recipe_id for rid, _v in parent), recipe_id
        assert not any(rid == recipe_id for rid, _v in registered), recipe_id
    assert set(acct.simulated_retirements()) <= parent
    # the projector's lazily imported authorities import cleanly under the blocker
    assert batch["authority_attempts"] == []


def test_mutant_invented_profile_name_on_a_composition_case_is_refused(cases, derived):
    """The served-name check is NOT vacuous on a recipe intent: its names come
    from the baseline naming evidence, so an invented one is caught."""
    from _issue_157_flows_accounting import derive_served_projection

    case = cases["composition/create_2legs_staged"]
    with patch(_PAGINATE, lambda *a, **k: []):
        served = derive_served_projection(case)
    (transform,) = [row for row in served if row["key"] == "transform"]
    evidence = case.provenance["naming_evidence"]["profile_names"]
    assert transform["source_profile_generation"]["component_name"] in evidence
    assert discharge_case(case, derived[case.case_id], {}, served=served).ok
    mutant = copy.deepcopy(served)
    [row for row in mutant if row["key"] == "transform"][0]["source_profile_generation"][
        "component_name"
    ] = "CMP Invented Profile"
    report = discharge_case(case, derived[case.case_id], {}, served=mutant)
    assert not report.ok and any("authors nowhere" in p for p in report.problems)


def test_mutant_endpoint_binding_mode_change_is_refused(cases):
    case = cases["composition/create_2legs_stream"]
    with patch(_PAGINATE, lambda *a, **k: []):
        derived_endpoints = derive_case_endpoints(case)
    assert discharge_endpoints(case, derived_endpoints, {}).ok
    mutant = copy.deepcopy(derived_endpoints)
    mutant[1]["binding_mode"] = "reuse"
    report = discharge_endpoints(case, mutant, {})
    assert not report.ok and any("differs" in p for p in report.problems)


# ---------------------------------------------------------------------------
# mutants: every guard fails closed on the shape it exists to catch
# ---------------------------------------------------------------------------


def _clone(case: Case) -> Case:
    clone = copy.copy(case)
    clone.flow_rows = copy.deepcopy(case.flow_rows)
    clone.endpoint_rows = copy.deepcopy(case.endpoint_rows)
    clone.discharges = copy.deepcopy(case.discharges)
    return clone


def _rich(cases):
    return cases["database_to_api_sync/wm_on_dlq_on_create"]


#: The completed case the FINAL-path arm of every mutant corrupts: every row
#: discharged, so the clean replay passes the final replay as-is.
_FINAL_CASE = "composition/create_2legs_staged"


def _partial_arm(cases):
    """The PARTIAL-replay arm: the rich case in its #157 state, its derived
    sequence being exactly its DISCHARGED frozen payloads (independent of how
    far the projection has progressed)."""
    case = _clone(_rich(cases))
    output = copy.deepcopy(
        [r["payload"] for r in case.flow_rows if case.status(r["ordinal"]) == DISCHARGED]
    )
    assert discharge_case(case, output, {}, final=False).ok
    return case, output


def _final_arm(cases, derived):
    """The FINAL-replay arm: a completed case on the final comparison path."""
    case = _clone(cases[_FINAL_CASE])
    output = [dict(row) for row in derived[_FINAL_CASE]]
    clean = discharge_case(case, output, {}, final=True)
    assert clean.ok and clean.completed, clean.problems
    return case, output


def _refused(report, needle):
    return (not report.ok) and any(needle in p for p in report.problems)


def test_mutant_partial_payload_projection_is_refused(cases, derived):
    for arm, final in ((_partial_arm(cases), False), (_final_arm(cases, derived), True)):
        case, output = arm
        index = next(i for i, r in enumerate(output) if r["key"] == "transform")
        output[index] = {k: v for k, v in output[index].items() if k != "direct_field_mappings"}
        assert _refused(discharge_case(case, output, {}, final=final), "no full-payload match"), final


def test_mutant_unexpected_row_fails_even_when_every_discharged_row_matches(cases, derived):
    """A watermark row on a case where it is frozen but NOT discharged is
    unexpected; on the final path a row the case never froze is."""
    case, output = _partial_arm(cases)
    watermark = _row(case, "watermark")["payload"]
    assert _refused(discharge_case(case, output + [watermark], {}, final=False), "unexpected row")
    case, output = _final_arm(cases, derived)
    assert _refused(discharge_case(case, output + [watermark], {}, final=True), "unexpected row")


def test_mutant_duplicate_row_is_refused(cases, derived):
    for (case, output), final in ((_partial_arm(cases), False), (_final_arm(cases, derived), True)):
        assert _refused(discharge_case(case, output * 2, {}, final=final), "duplicate"), final


def test_mutant_reordered_sequence_is_refused(cases, derived):
    case, _output = _partial_arm(cases)
    # discharge two rows so ORDER is observable, then hand them back reversed
    extract = _row(case, "extract")
    case.discharges["flows"][str(extract["ordinal"])] = DISCHARGED
    transform = _row(case, "transform")
    assert _refused(discharge_case(case, [transform["payload"], extract["payload"]], {}, final=False), "reordering")
    case, output = _final_arm(cases, derived)
    output[0], output[1] = output[1], output[0]
    assert _refused(discharge_case(case, output, {}, final=True), "reordering")


def test_mutant_modified_baseline_row_is_refused(cases, derived):
    for (case, output), final in ((_partial_arm(cases), False), (_final_arm(cases, derived), True)):
        _row(case, "transform")["payload"]["name"] = "tampered"
        assert _refused(discharge_case(case, output, {}, final=final), "does not match its own digest"), final


def test_mutant_deleted_baseline_row_is_refused(cases, derived):
    for (case, output), final in ((_partial_arm(cases), False), (_final_arm(cases, derived), True)):
        case.flow_rows = [r for r in case.flow_rows if r["key"] != "send"]
        assert _refused(discharge_case(case, output, {}, final=final), "exactly once"), final


def test_mutant_double_discharge_is_refused(cases, derived):
    case, output = _partial_arm(cases)
    send = _row(case, "send")
    case.discharges["flows"][str(send["ordinal"])] = DISCHARGED  # claimed, never derived
    assert _refused(discharge_case(case, output, {}, final=False), "no full-payload match")
    case, output = _final_arm(cases, derived)
    output = [r for r in output if r["key"] != "send"]  # discharged, no longer derived
    assert _refused(discharge_case(case, output, {}, final=True), "no full-payload match")


def test_mutant_missing_discharge_is_refused(cases, derived):
    case, output = _partial_arm(cases)
    transform = _row(case, "transform")
    case.discharges["flows"][str(transform["ordinal"])] = PENDING  # derived, never claimed
    assert _refused(discharge_case(case, output, {}, final=False), "unexpected row")
    # final path: a fully reproduced row may NOT stay pending
    case, output = _final_arm(cases, derived)
    case.discharges["flows"][str(_row(case, "send")["ordinal"])] = PENDING
    report = discharge_case(case, output, {}, final=True)
    assert report.completed and _refused(report, "still pending")
    # and while the lanes land the same state is reported, not refused
    transitional = discharge_case(case, output, {}, final=False)
    assert transitional.ok and transitional.reproduced_pending == 1


def test_mutant_unknown_retirement_record_is_refused(cases, derived):
    for (case, output), final in ((_partial_arm(cases), False), (_final_arm(cases, derived), True)):
        send = _row(case, "send")
        case.discharges["flows"][str(send["ordinal"])] = "retired:RET-157-99"
        assert _refused(discharge_case(case, output, {}, final=final), "unknown retirement"), final


def test_mutant_endpoint_corruptions_fail_on_the_final_path(cases):
    """The endpoint oracle runs the SAME comparison: binding mode alone, a
    null-versus-absent flip, a reordering and a pending endpoint all fail the
    final replay of a completed case."""
    case = _clone(cases[_FINAL_CASE])
    with patch(_PAGINATE, lambda *a, **k: []):
        clean = derive_case_endpoints(case)
    assert discharge_endpoints(case, clean, {}, final=True).ok
    flipped = copy.deepcopy(clean)
    flipped[1]["binding_mode"] = "reuse"
    nulled = copy.deepcopy(clean)
    nulled[1]["component_id"] = None  # absent in the frozen row: a null is content
    for mutant, needle in ((flipped, "no full-payload match"), (nulled, "no full-payload match"),
                           (list(reversed(clean)), "reordering")):
        assert _refused(discharge_endpoints(case, mutant, {}, final=True), needle), needle
    case.discharges["endpoints"]["0"] = PENDING
    assert _refused(discharge_endpoints(case, clean, {}, final=True), "still pending")


def test_the_completed_cases_complete_both_oracles(replay):
    """A case is completed on its flows exactly when it is on its endpoints —
    a lane whose endpoint projection lags its rows is not completed."""
    for case_id, (flows, endpoints) in replay.items():
        assert flows.completed == endpoints.completed, (case_id, flows.problems, endpoints.problems)
    assert {c for c in _completed(replay) if c.startswith("composition/")} == {
        c for c in replay if c.startswith("composition/")
    }


def test_mutant_input_edit_without_a_refreeze_is_caught_by_the_manifest(tmp_path, monkeypatch):
    """An edited legacy input whose rows were not re-frozen fails the digest check."""
    import shutil

    corpus = tmp_path / "corpus"
    shutil.copytree(acct.CORPUS_DIR, corpus)
    target = corpus / "cases" / "stub_minimal" / "default" / "input.legacy.json"
    data = json.loads(target.read_text())
    data["input"]["parameters"]["component_prefix"] = "TAMPERED"
    target.write_text(json.dumps(data, indent=2, sort_keys=True) + "\n")
    monkeypatch.setattr(acct, "CORPUS_DIR", corpus)
    monkeypatch.setattr(acct, "MANIFEST_PATH", corpus / "MANIFEST.json")
    problems = verify_manifest()
    assert any("digest-mismatch" in p and "input.legacy.json" in p for p in problems)


def test_mutant_baseline_row_deletion_is_caught_by_the_manifest(tmp_path, monkeypatch):
    import shutil

    corpus = tmp_path / "corpus"
    shutil.copytree(acct.CORPUS_DIR, corpus)
    target = corpus / "cases" / "stub_minimal" / "default" / "flows.baseline.jsonl"
    target.write_text("")
    monkeypatch.setattr(acct, "CORPUS_DIR", corpus)
    monkeypatch.setattr(acct, "MANIFEST_PATH", corpus / "MANIFEST.json")
    problems = verify_manifest()
    assert any("digest-mismatch" in p and "flows.baseline.jsonl" in p for p in problems)


def test_the_freeze_cli_refuses_a_source_tree_argument():
    assert acct.main(["--freeze", "--source-tree", "/tmp/anything"]) == 2


def test_the_legacy_producers_at_head_still_reproduce_the_frozen_rows():
    """Until #160 deletes them, the frozen rows are re-derivable from HEAD.

    This is the differential control for the corpus itself: if it fails, either
    a legacy producer moved (a #160-owned event) or the freeze drifted.
    """
    assert acct.check_against_tree() == []


def test_the_flagship_root_topology_and_full_envelope_match_the_legacy_outcome(batch):
    """ARCH-r1-13 (eval 2): the blocked child's public-path flagship also carries
    the normalized root's CANONICAL topology and the FULL envelope; both equal
    the legacy outcome computed here, unblocked."""
    from _fanout_v2_support import oracle, respell_oracle_credential_refs

    from boomi_mcp.models.integration_models import IntegrationSpecV1
    from boomi_mcp.models.process_ir import canonical_process_ir_json
    from boomi_mcp.patterns.recipe_bridge import run_fanout_recipe

    flagship = batch["flagship"]
    spec = IntegrationSpecV1.model_validate(oracle()["integration_spec"])
    process = spec.components[-1]
    legacy = run_fanout_recipe(
        recipe_id="boomi.compose.db_rest_fanout", components=spec.components, process=process
    )
    legacy_root = dict(legacy.composed.process_roots)[process.key]
    assert flagship.get("root") == canonical_process_ir_json(legacy_root)
    frozen = respell_oracle_credential_refs(oracle()["integration_spec"]["components"])[-1]
    expected = {
        "component_key": frozen["key"],
        "name": frozen["name"],
        "action": frozen["action"],
        "component_id": frozen.get("component_id"),
        # The envelope types ``description`` as ``str`` (default ``""``): an
        # absent legacy description IS the envelope's empty one.
        "description": frozen["config"].get("description") or "",
        "folder_name": frozen["config"].get("folder_name"),
        "depends_on": sorted(frozen["depends_on"]),
        "process_extensions": frozen["config"]["process_extensions"],
    }
    assert flagship.get("envelope_full") == expected

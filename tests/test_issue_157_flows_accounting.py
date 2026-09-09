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


def test_frozen_row_counts_are_pinned(manifest):
    """The denominator is pinned so a deregistration or regeneration cannot shrink it."""
    assert manifest["frozen_flow_rows"] == 83
    assert manifest["frozen_endpoint_rows"] == 58
    assert manifest["status_counts"] == {"discharged": 9, "retired": 0, "pending": 132}


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
    retirements = retirement_ids()
    for case_id, case in cases.items():
        report = discharge_case(case, derived[case_id], retirements)
        assert report.ok, (case_id, report.problems)
        endpoint_report = discharge_endpoints(case, [], retirements)
        assert endpoint_report.ok, (case_id, endpoint_report.problems)


def test_the_two_rich_forms_are_discharged_by_full_payload_equality(cases, derived):
    """#157's own obligation: every DB-source and API-source transform row."""
    discharged = 0
    for case_id, case in cases.items():
        if case.producer not in ("database_to_api_sync", "api_to_api_sync"):
            continue
        row = _row(case, "transform")
        assert case.status(row["ordinal"]) == DISCHARGED, case_id
        assert case.input_canonical is not None and case.provenance is not None, case_id
        assert case.provenance["executed_at_replay"] is False
        assert [acct._digest(r) for r in derived[case_id]] == [row["payload_sha256"]], case_id
        discharged += 1
    assert discharged == 9


def test_the_remaining_rows_are_explicitly_pending_for_159_not_labelled_covered(cases, manifest):
    pending = [
        (case_id, r["key"])
        for case_id, case in cases.items()
        for r in case.flow_rows
        if case.status(r["ordinal"]) == PENDING
    ]
    assert len(pending) == manifest["frozen_flow_rows"] - 9
    # the five simpler producers and composition are entirely #159's
    assert all(case.status(r["ordinal"]) == PENDING for case in cases.values() if case.producer not in ("database_to_api_sync", "api_to_api_sync") for r in case.flow_rows)


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
    with patch(_PAGINATE, lambda *a, **k: []):
        derive_case_projection(case)
    assert touched == []


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


def test_mutant_partial_payload_projection_is_refused(cases, derived):
    case = _rich(cases)
    partial = [dict(row) for row in derived[case.case_id]]
    partial[0] = {k: v for k, v in partial[0].items() if k != "direct_field_mappings"}
    report = discharge_case(case, partial, {})
    assert not report.ok and any("no full-payload match" in p for p in report.problems)


def test_mutant_unexpected_row_fails_even_when_every_discharged_row_matches(cases, derived):
    """A watermark row on a case where it is frozen but NOT discharged is unexpected."""
    case = _rich(cases)
    watermark = _row(case, "watermark")["payload"]
    output = list(derived[case.case_id]) + [watermark]
    report = discharge_case(case, output, {})
    assert not report.ok and any("unexpected row" in p for p in report.problems)


def test_mutant_duplicate_row_is_refused(cases, derived):
    case = _rich(cases)
    output = list(derived[case.case_id]) * 2
    report = discharge_case(case, output, {})
    assert not report.ok and any("duplicate" in p for p in report.problems)


def test_mutant_reordered_sequence_is_refused(cases, derived):
    case = _clone(_rich(cases))
    # discharge two rows so ORDER is observable, then hand them back reversed
    extract = _row(case, "extract")
    case.discharges["flows"][str(extract["ordinal"])] = DISCHARGED
    transform = _row(case, "transform")
    output = [transform["payload"], extract["payload"]]
    report = discharge_case(case, output, {})
    assert not report.ok and any("reordering" in p for p in report.problems)


def test_mutant_modified_baseline_row_is_refused(cases, derived):
    case = _clone(_rich(cases))
    row = _row(case, "transform")
    row["payload"]["name"] = "tampered"
    report = discharge_case(case, derived[case.case_id], {})
    assert not report.ok and any("does not match its own digest" in p for p in report.problems)


def test_mutant_deleted_baseline_row_is_refused(cases, derived):
    case = _clone(_rich(cases))
    case.flow_rows = [r for r in case.flow_rows if r["key"] != "reliability"]
    report = discharge_case(case, derived[case.case_id], {})
    assert not report.ok and any("exactly once" in p for p in report.problems)


def test_mutant_double_discharge_is_refused(cases, derived):
    case = _clone(_rich(cases))
    send = _row(case, "send")
    case.discharges["flows"][str(send["ordinal"])] = DISCHARGED  # claimed, never derived
    report = discharge_case(case, derived[case.case_id], {})
    assert not report.ok and any("no full-payload match" in p for p in report.problems)


def test_mutant_missing_discharge_is_refused(cases, derived):
    case = _clone(_rich(cases))
    transform = _row(case, "transform")
    case.discharges["flows"][str(transform["ordinal"])] = PENDING  # derived, never claimed
    report = discharge_case(case, derived[case.case_id], {})
    assert not report.ok and any("unexpected row" in p for p in report.problems)


def test_mutant_unknown_retirement_record_is_refused(cases, derived):
    case = _clone(_rich(cases))
    send = _row(case, "send")
    case.discharges["flows"][str(send["ordinal"])] = "retired:RET-157-99"
    report = discharge_case(case, derived[case.case_id], {})
    assert not report.ok and any("unknown retirement" in p for p in report.problems)


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

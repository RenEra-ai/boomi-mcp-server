"""Issue #157 (M12.19) — the watermark/scheduling SPLITs and the inert-metadata RETIREs.

Each retirement record under ``tests/fixtures/governance/issue_157/retirements/``
freezes a legacy-baseline mutation proof: a pair of archetype payloads differing
ONLY in the retired field produced byte-identical component XML, byte-identical
process XML and an identical plan verdict on the legacy chain, while a wired
sibling field moved a digest under the same harness. These tests replay that
proof from the frozen pair, pin the in-code retired-spelling authority to the
record index in both directions, assert the NAMED refusal of every retired
spelling on the typed surface, and cover the two halves of the watermark SPLIT:
the typed declaration is the only accepted spelling, and persistence stays an
executable, byte-bearing property of the canonical graph.
"""

from __future__ import annotations

import copy
import sys
from pathlib import Path
from unittest.mock import MagicMock, patch

import pytest

_src = str(Path(__file__).resolve().parent.parent / "src")
if _src not in sys.path:
    sys.path.insert(0, _src)
sys.path.insert(0, str(Path(__file__).resolve().parent))

import _issue_157_retirements as retire  # noqa: E402
from _m12_11_support import APPLIABLE_CONN, APPLIABLE_OP  # noqa: E402
from boomi_mcp.authoring.workflow import compile_authoring_request_v1  # noqa: E402
from boomi_mcp.errors import ERROR_TAXONOMY, GOVERNANCE_RETIRED_SPELLING  # noqa: E402
from boomi_mcp.models.authoring_workflow import (  # noqa: E402
    AuthoringRequestV1,
    ProcessIRAuthoringIntentV1,
    RecipeProcessEnvelopeV1,
)
from boomi_mcp.models.governance_intent import (  # noqa: E402
    RETIRED_SPELLINGS,
    WatermarkDeclarationV1,
    retired_spellings_in,
)
from boomi_mcp.models.process_component import (  # noqa: E402
    ProcessAuthoringUnitAuthoredV1,
    ProcessComponentEnvelopeAuthoredV1,
)
from boomi_mcp.models.process_ir import parse_process_ir_v1  # noqa: E402

_PAGINATE = "boomi_mcp.categories.integration_builder.paginate_metadata"


@pytest.fixture(scope="module")
def records():
    return retire.load_records()


@pytest.fixture(scope="module")
def index():
    return retire.load_index()


# ---------------------------------------------------------------------------
# the records
# ---------------------------------------------------------------------------


def test_every_candidate_has_a_record_and_the_index_matches_the_records(records, index):
    assert set(records) == set(retire.CANDIDATES) == set(index)
    for rid, record in records.items():
        assert record["id"] == rid
        assert record["baseline_sha"] == retire.BASELINE_SHA
        assert index[rid]["classification"] == record["classification"] == retire.CANDIDATES[rid]["expected"]
        assert record["cutover_owner"] == "#160"


def test_each_pair_differs_only_under_its_field(records):
    for rid, record in records.items():
        field = record["field_path"]
        assert record["pair_diff"], rid
        assert all(path.startswith("/" + field) for path in record["pair_diff"]), (rid, record["pair_diff"])


def test_every_retire_record_carries_a_complete_inertness_proof(records):
    """Equal component XML, equal process XML, equal verdict — and a non-vacuous control."""
    seen = 0
    for rid, record in records.items():
        if record["classification"] != "RETIRE":
            continue
        seen += 1
        obs = record["observations"]
        assert obs["accepted_by_contract"] is True, rid
        assert obs["xml_moved"] == [], rid
        digests = obs["emitted_xml_digests"]
        assert digests and all(entry["base"] == entry["varied"] for entry in digests.values()), rid
        assert "process" in set(obs["component_types"].values()), rid
        assert obs["plan_verdict_digest"]["base"] == obs["plan_verdict_digest"]["varied"], rid
        assert obs["non_vacuity_control"]["xml_moved"], rid
        # the spec echo MOVES — that is the legacy integration_spec hash churn, recorded, never inertness
        assert obs["spec_echo_paths"], rid
        assert record["refusal_code_on_typed_surface"] == GOVERNANCE_RETIRED_SPELLING
    assert seen == 8


def test_the_retained_candidate_is_not_inert_and_the_split_one_is(records):
    direction = records["RET-157-05"]
    assert direction["classification"] == "RETAIN" and direction["measured_classification"] == "SPLIT"
    assert "source_db_read_profile" in direction["observations"]["xml_moved"]
    watermark = records["RET-157-10"]
    assert watermark["classification"] == "SPLIT" and watermark["observations"]["xml_moved"] == []


def test_the_retired_spelling_authority_and_the_record_index_agree_both_ways(records):
    # every typed-surface spelling names a RETIRE or SPLIT record
    for spelling, rid in RETIRED_SPELLINGS.items():
        assert rid in records, (spelling, rid)
        assert records[rid]["classification"] in ("RETIRE", "SPLIT"), (spelling, rid)
    # every RETIRE record whose spelling is a KEY a caller could author on the typed surface is refused by name
    keyed = {rid for rid, r in records.items() if r["classification"] == "RETIRE" and "=" not in r["spelling"]}
    assert keyed <= set(RETIRED_SPELLINGS.values()), keyed - set(RETIRED_SPELLINGS.values())
    # the two exceptions are documented: a VALUE spelling (no key to refuse) and the retained field
    assert "RET-157-08" not in RETIRED_SPELLINGS.values() and "=" in records["RET-157-08"]["spelling"]
    assert "RET-157-05" not in RETIRED_SPELLINGS.values()


def test_the_legacy_chain_at_head_still_reproduces_every_record():
    """Valid until #160 deletes the legacy producers; a drift here is a #160-owned event."""
    assert retire.check() == []


# ---------------------------------------------------------------------------
# named rejection on the typed surface
# ---------------------------------------------------------------------------


@pytest.mark.parametrize("spelling", sorted(RETIRED_SPELLINGS))
def test_each_retired_spelling_is_refused_by_name_on_both_typed_envelopes(spelling):
    with pytest.raises(Exception) as excinfo:
        ProcessComponentEnvelopeAuthoredV1(component_key="p", action="create", name="n", **{spelling: "x"})
    assert "governance_retired_spelling" in str(excinfo.value)
    with pytest.raises(Exception) as excinfo:
        RecipeProcessEnvelopeV1(component_key="p", **{spelling: "x"})
    assert "governance_retired_spelling" in str(excinfo.value)


def test_the_refusal_is_served_with_its_registered_code():
    from boomi_mcp.categories.integration_builder import build_integration_action

    assert GOVERNANCE_RETIRED_SPELLING in ERROR_TAXONOMY
    payload = {"authoring_request": {"contract_version": "2", "intent": {
        "intent_kind": "process_ir", "integration_name": "x",
        "units": [{"envelope": {"component_key": "p", "action": "create", "name": "n", "fetch_size": 500},
                   "process_ir": {"version": "1", "body": {"kind": "sequence", "steps": [
                       {"kind": "source", "connection_ref": "$ref:conn", "operation_ref": "$ref:op"},
                       {"kind": "return_documents"}]}}}],
        "components": [APPLIABLE_CONN, APPLIABLE_OP]}}}
    with patch(_PAGINATE, lambda *a, **k: []):
        result = build_integration_action(MagicMock(), "issue-157", "plan", payload)
    assert result.get("_success") is False and GOVERNANCE_RETIRED_SPELLING in str(result)


def test_the_retired_key_walker_finds_nested_spellings_and_ignores_values():
    found = retired_spellings_in({"process_extensions": {"connections": [{"cron": 1}]}, "note": "fetch_size"})
    assert found == (("/process_extensions/connections/0/cron", "cron"),)


# ---------------------------------------------------------------------------
# the watermark SPLIT
# ---------------------------------------------------------------------------


def test_the_typed_declaration_is_the_sole_accepted_watermark_spelling():
    WatermarkDeclarationV1(source_profile_ref="$ref:src", field="updated_at", kind="timestamp")
    for legacy in ({"persistence": "dpp"}, {"dpp_name": "wm"}, {"store_ref": "s"}, {"enabled": True}):
        with pytest.raises(Exception):
            WatermarkDeclarationV1(source_profile_ref="$ref:src", field="updated_at", kind="timestamp", **legacy)


def _root(persist: bool):
    return parse_process_ir_v1({"version": "1", "body": {"kind": "sequence", "steps": [
        {"kind": "source", "connection_ref": "$ref:conn", "operation_ref": "$ref:op"},
        {"kind": "set_dpp", "name": "watermark_since", "persist": persist,
         "source_values": [{"value_type": "static", "value": "2026-01-01"}]},
        {"kind": "return_documents"}]}})


def _request(persist: bool):
    unit = ProcessAuthoringUnitAuthoredV1(
        envelope=ProcessComponentEnvelopeAuthoredV1(component_key="proc", name="P", action="create", depends_on=("conn", "op")),
        process_ir=_root(persist),
    )
    return AuthoringRequestV1(intent=ProcessIRAuthoringIntentV1(integration_name="x", units=(unit,), components=(APPLIABLE_CONN, APPLIABLE_OP)))


def test_persisted_dpp_behaviour_is_canonical_and_byte_bearing():
    """The executable half of the SPLIT lives in the graph: `persist` moves the emitted plan."""
    with patch(_PAGINATE, lambda *a, **k: []):
        persisted, _ = compile_authoring_request_v1(_request(True), boomi_client=MagicMock(), profile="p")
        transient, _ = compile_authoring_request_v1(_request(False), boomi_client=MagicMock(), profile="p")
    emitted = lambda result: {f.digest for f in result.artifact_fingerprints if f.artifact_kind == "process_ir_emission_plan"}
    assert emitted(persisted) and emitted(persisted) != emitted(transient)

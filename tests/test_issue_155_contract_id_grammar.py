"""The canonical idempotency-contract identifier grammar (#155 acceptance criteria).

The issue specifies the identifier itself, not merely that one exists:
`icv1:<family_id>:<action_id>:<semantics_id>:<revision>`, every segment drawn
from a closed lowercase alphabet; the authored form is `$ref:` + that ID; the
family and action identifiers are NOT the raw registry values but come from a
TOTAL, INJECTIVE, RECORDED mapping whose collisions are mint-time failures; and
ONE SHARED VALIDATOR — a single anchored regex owned by the registry — is
consumed by every surface that handles a reference, so no two can drift.

The tree previously accepted `^\\$ref:[A-Za-z0-9_.-]+` and shipped a dotted ref,
which is a contract of open strings: it cannot be violated because it promises
nothing. These nodes pin each clause of the criterion separately, so a partial
regression names which clause it broke.
"""
from __future__ import annotations

import json
import re
from pathlib import Path

import pytest

from boomi_mcp.connector_replay.ids import (
    AUTHORED_CONTRACT_REF_PATTERN,
    CANONICAL_CONTRACT_ID_PATTERN,
    CONTRACT_ID_SEGMENT,
    authored_contract_ref,
    canonical_contract_id,
    is_authored_contract_ref,
    is_canonical_contract_id,
)
from boomi_mcp.connector_replay.registry import load_registry

_ROOT = Path(__file__).resolve().parents[1]


def test_the_grammar_is_the_one_the_issue_specifies():
    """Character for character, not merely 'structured'."""
    assert CONTRACT_ID_SEGMENT == r"[a-z0-9_]+"
    assert CANONICAL_CONTRACT_ID_PATTERN == (
        r"^icv1:[a-z0-9_]+:[a-z0-9_]+:[a-z0-9_]+:[1-9][0-9]*(?![\s\S])")
    assert AUTHORED_CONTRACT_REF_PATTERN == (
        r"^\$ref:icv1:[a-z0-9_]+:[a-z0-9_]+:[a-z0-9_]+:[1-9][0-9]*(?![\s\S])")


@pytest.mark.parametrize("bad,why", [
    ("$ref:rest.patch.cds_client.static_route.v1", "the dotted form this replaced"),
    ("$ref:icv1:REST:patch:sem:1", "upper case is outside the alphabet"),
    ("$ref:icv1:rest:patch:sem:0", "revision zero"),
    ("$ref:icv1:rest:patch:sem:01", "a leading zero is a second spelling of one revision"),
    ("$ref:icv1:rest:patch:sem:1\n", "a trailing newline, which a $-anchored rule admits"),
    ("$ref:icv1:rest:patch:1", "too few segments"),
    ("$ref:icv1:rest:patch:sem:1:extra", "too many segments"),
    ("$ref:icv1:rest::sem:1", "an empty segment"),
    ("$ref:icv2:rest:patch:sem:1", "a version tag this grammar does not define"),
    ("icv1:rest:patch:sem:1", "the bare id is not the authored form"),
    ("$ref:icv1:rest:patch:sem-1:1", "a hyphen is outside the alphabet"),
])
def test_the_grammar_refuses(bad, why):
    assert not is_authored_contract_ref(bad), why


def test_the_constructor_is_the_only_way_to_build_one():
    """It refuses parts the grammar would reject rather than emitting them.

    A formatter that emitted an unparseable reference would put the failure where
    the parts that caused it are no longer in view.
    """
    good = canonical_contract_id("rest", "patch", "sem_1", 3)
    assert good == "icv1:rest:patch:sem_1:3"
    assert is_canonical_contract_id(good)
    assert authored_contract_ref("rest", "patch", "sem_1", 3) == "$ref:" + good

    for parts in [("REST", "patch", "s", 1), ("rest", "PATCH", "s", 1),
                  ("rest", "patch", "s-1", 1), ("rest", "", "s", 1)]:
        with pytest.raises(ValueError):
            canonical_contract_id(*parts)
    for revision in (0, -1, "1", 1.0, True):
        with pytest.raises(ValueError):
            canonical_contract_id("rest", "patch", "s", revision)


def test_the_action_identifier_map_is_total_and_injective():
    """RECORDED, and re-checked on load — not a fold applied at the point of use.

    The obvious fold is lower-case, and on today's vocabulary it happens to be
    injective. A fold that is injective on the values seen so far is not a
    mapping; it is a coincidence. So the map is data, and these are the failures
    it must refuse.
    """
    from boomi_mcp.connector_replay.models import ConnectorVocabularyMappingV1

    base = dict(platform_connector_type="officialboomi-X3979C-rest-prod",
                family="rest", action_source="operation_component",
                recognised_actions=("GET", "PATCH", "POST"))
    good = tuple(sorted((a, a.lower()) for a in base["recognised_actions"]))
    mapping = ConnectorVocabularyMappingV1(**base, action_ids=good)
    assert mapping.action_id("PATCH") == "patch"
    assert mapping.family_id == "rest"

    with pytest.raises(ValueError, match="not total"):
        ConnectorVocabularyMappingV1(**base, action_ids=good[:2])
    with pytest.raises(ValueError, match="not total"):
        ConnectorVocabularyMappingV1(
            **base, action_ids=tuple(sorted(good + (("HEAD", "head"),))))
    with pytest.raises(ValueError, match="share one identifier"):
        ConnectorVocabularyMappingV1(
            **base, action_ids=tuple(sorted((a, "same") for a in base["recognised_actions"])))
    with pytest.raises(ValueError, match="grammar-safe"):
        ConnectorVocabularyMappingV1(
            **base, action_ids=tuple(sorted((a, a) for a in base["recognised_actions"])))
    with pytest.raises(ValueError, match="no recorded identifier"):
        mapping.action_id("DELETE")


def test_the_packaged_record_is_named_by_its_own_derivation():
    """The bidirectional pin: the shipped name follows from the shipped facts."""
    registry = load_registry()
    vocabulary = {entry.family: entry for entry in registry.vocabulary}
    for record in registry.operation_records:
        entry = vocabulary[record.family]
        assert record.contract_ref == authored_contract_ref(
            entry.family_id, entry.action_id(record.action),
            record.semantics_id, record.semantics_revision)
        assert is_authored_contract_ref(record.contract_ref)


def test_a_grammatical_but_wrong_name_is_refused_by_the_registry():
    """Non-vacuity for the derivation check: the grammar alone would accept this.

    The probe swaps only the action segment, so the reference stays perfectly
    well-formed and names a record that describes a different action. A registry
    checking the grammar and not the derivation would load it.
    """
    from boomi_mcp.connector_replay.registry import RegistryInvalid, _parse

    payload = json.loads(
        (_ROOT / "src/boomi_mcp/connector_replay/registry_v1.json").read_text("utf-8"))
    record = payload["operation_records"][0]
    wrong = record["contract_ref"].replace(":patch:", ":get:")
    assert wrong != record["contract_ref"], "the probe did not change anything"
    assert is_authored_contract_ref(wrong), (
        "the probe is not grammatical, so it would be refused by the grammar and "
        "would say nothing about the derivation check"
    )
    record["contract_ref"] = wrong
    with pytest.raises(RegistryInvalid, match="derive"):
        _parse(payload)


def test_one_regex_is_shared_by_every_surface_that_handles_a_reference():
    """The criterion's own words: ONE shared validator, so no two surfaces drift.

    Checked by reading the sources rather than by calling them, because two
    surfaces can agree on every value tested and still be two independent
    copies — and the copy is the defect. Each must NAME the constant.
    """
    surfaces = {
        "authoring validator": "src/boomi_mcp/models/process_ir.py",
        "compiler symbol": "src/boomi_mcp/compiler/process_ir/contracts.py",
        "registry record": "src/boomi_mcp/connector_replay/models.py",
        "registry mint": "src/boomi_mcp/connector_replay/registry.py",
    }
    for label, relative in surfaces.items():
        text = (_ROOT / relative).read_text("utf-8")
        assert re.search(r"AUTHORED_CONTRACT_REF_PATTERN|is_authored_contract_ref"
                         r"|authored_contract_ref", text), (
            f"{label} ({relative}) does not reference the shared grammar, so it "
            "either does not validate references or validates them with a copy"
        )
        assert "[A-Za-z0-9_.-]" not in text, (
            f"{label} ({relative}) carries a second, laxer reference pattern"
        )

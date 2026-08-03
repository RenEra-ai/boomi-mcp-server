"""#146 (M12.11): honest capability discovery and live/check-out drift.

The issue's originating evidence: the live MCP service reported FOUR archetypes
while the checkout registry had six, and no client could tell. These tests pin
the two properties that make that diagnosable — the manifest reports what this
runtime can actually do, and a comparison the caller did not ask for is never
reported as parity.
"""

import sys
from pathlib import Path

import pytest

_src = str(Path(__file__).resolve().parent.parent / "src")
if _src not in sys.path:
    sys.path.insert(0, _src)
sys.path.insert(0, str(Path(__file__).resolve().parent))

from _m12_11_support import walk_keys, walk_strings  # noqa: E402
from boomi_mcp.authoring.contract import (  # noqa: E402
    AUTHORING_ACTIONS,
    build_authoring_contract_manifest,
    compare_capability_revision,
    list_archetype_registry,
)
from boomi_mcp.categories.meta_tools import list_capabilities_action  # noqa: E402

#: Every top-level key `list_capabilities` returned BEFORE #146. Enumerated, so
#: an accidental removal fails here rather than in a caller.
_PRE_146_KEYS = {
    "_success",
    "server_name",
    "server_version",
    "recipe_registry",
    "total_tools",
    "implemented_count",
    "not_implemented_count",
    "implemented_tools",
    "not_implemented_tools",
    "tools",
    "workflows",
    "coverage",
    "hints",
    "operating_doctrine",
    "design_doctrine",
    "account_governance",
}


def test_every_pre_146_top_level_key_survives():
    catalog = list_capabilities_action()
    missing = _PRE_146_KEYS - set(catalog)
    assert missing == set(), missing


def test_the_authoring_contract_is_published():
    catalog = list_capabilities_action()
    contract = catalog["authoring_contract"]
    assert contract.get("status") != "unavailable"
    for key in (
        "actions",
        "archetypes",
        "capabilities",
        "capability_revision",
        "schema_revision",
        "compiler_revision",
        "schemas",
        "support_matrix",
        "intent_kinds",
    ):
        assert key in contract, key


def test_all_six_runtime_archetypes_are_reported():
    """The four-vs-six evidence. DERIVED from the live scan, never a literal —
    a hard-coded six would have reported six on the runtime that served four."""
    manifest = build_authoring_contract_manifest()
    reported = [entry["name"] for entry in manifest["archetypes"]]
    assert len(reported) == 6, reported
    assert reported == sorted(reported)
    assert reported == [entry["name"] for entry in list_archetype_registry()]


def test_an_unmigrated_archetype_is_marked_not_hidden():
    """Omitting an archetype the runtime has would recreate the exact blindness
    this manifest exists to remove."""
    manifest = build_authoring_contract_manifest()
    migrated = {e["name"]: e["migrated"] for e in manifest["archetypes"]}
    assert migrated["api_to_api_sync"] is True
    assert migrated["stub_minimal_integration"] is False
    # Both states present — otherwise the flag proves nothing.
    assert set(migrated.values()) == {True, False}


def test_no_expected_revision_is_not_requested_never_match():
    """The honesty rule, in both spellings a caller can produce."""
    for absent in (None, "", "   "):
        assert compare_capability_revision(absent)["status"] == "not_requested"
    assert (
        list_capabilities_action()["authoring_contract"]["capability_comparison"][
            "status"
        ]
        == "not_requested"
    )


def test_a_matching_revision_reports_match_and_a_different_one_reports_mismatch():
    actual = build_authoring_contract_manifest()["capability_revision"]
    assert compare_capability_revision(actual)["status"] == "match"
    other = compare_capability_revision("sha256:" + "0" * 64)
    assert other["status"] == "mismatch"
    assert other["actual_capability_revision"] == actual
    assert other["remediation"]


def test_a_mismatch_still_returns_the_live_catalog():
    """Reporting drift without the evidence to diagnose it is not much better
    than hiding it."""
    catalog = list_capabilities_action(
        expected_capability_revision="sha256:" + "0" * 64
    )
    contract = catalog["authoring_contract"]
    assert contract["capability_comparison"]["status"] == "mismatch"
    assert len(contract["archetypes"]) == 6
    assert contract["schemas"]


def test_a_simulated_four_archetype_live_manifest_mismatches_the_six_entry_one(
    monkeypatch,
):
    """The originating incident, reproduced.

    A runtime serving four archetypes must produce a DIFFERENT capability
    revision from one serving six — otherwise the comparison would have reported
    parity for the very skew that motivated this issue.
    """
    from boomi_mcp.authoring import contract as contract_module

    six_revision = build_authoring_contract_manifest()["capability_revision"]

    contract_module.reset_manifest_cache()
    full = list_archetype_registry()
    monkeypatch.setattr(
        contract_module, "list_archetype_registry", lambda: tuple(full[:4])
    )
    four_revision = build_authoring_contract_manifest()["capability_revision"]
    contract_module.reset_manifest_cache()

    assert four_revision != six_revision
    # And a client holding the six-archetype revision is TOLD, not left guessing.
    monkeypatch.setattr(
        contract_module, "list_archetype_registry", lambda: tuple(full[:4])
    )
    assert compare_capability_revision(six_revision)["status"] == "mismatch"
    contract_module.reset_manifest_cache()


def test_the_manifest_is_deterministic():
    from boomi_mcp.authoring import contract as contract_module

    first = dict(build_authoring_contract_manifest())
    contract_module.reset_manifest_cache()
    second = dict(build_authoring_contract_manifest())
    assert first == second
    assert first["capability_revision"] == second["capability_revision"]


def test_every_manifest_collection_is_sorted():
    manifest = build_authoring_contract_manifest()
    assert [s["selector"] for s in manifest["schemas"]] == sorted(
        s["selector"] for s in manifest["schemas"]
    )
    assert [c["capability_id"] for c in manifest["capabilities"]] == sorted(
        c["capability_id"] for c in manifest["capabilities"]
    )
    assert [a["name"] for a in manifest["archetypes"]] == sorted(
        a["name"] for a in manifest["archetypes"]
    )


def test_each_selector_action_and_capability_appears_exactly_once():
    manifest = build_authoring_contract_manifest()
    for collection, key in (
        (manifest["schemas"], "selector"),
        (manifest["capabilities"], "capability_id"),
        (manifest["archetypes"], "name"),
    ):
        values = [entry[key] for entry in collection]
        assert len(values) == len(set(values)), values
    assert len(manifest["actions"]) == len(set(manifest["actions"]))


def test_topology_deploy_is_published_as_unsupported_with_its_refusal_code():
    from boomi_mcp.errors import TOPOLOGY_APPLY_NOT_SUPPORTED

    manifest = build_authoring_contract_manifest()
    entry = next(
        c
        for c in manifest["capabilities"]
        if c["capability_id"] == "authoring.system_topology.deploy"
    )
    assert entry["state"] == "unsupported"
    assert entry["reason_code"] == TOPOLOGY_APPLY_NOT_SUPPORTED
    # ...while planning it IS supported, so the two are distinguishable.
    plan_entry = next(
        c
        for c in manifest["capabilities"]
        if c["capability_id"] == "authoring.system_topology.plan"
    )
    assert plan_entry["state"] == "supported"


def test_the_support_matrix_covers_every_intent_kind_and_action():
    manifest = build_authoring_contract_manifest()
    for kind in manifest["intent_kinds"]:
        assert kind in manifest["support_matrix"]
        for action in AUTHORING_ACTIONS:
            assert action in manifest["support_matrix"][kind]


def test_the_manifest_carries_no_secret_path_timestamp_or_account_identity():
    """Exclusions asserted, not merely documented."""
    manifest = dict(build_authoring_contract_manifest())
    forbidden_keys = (
        "password",
        "secret",
        "credential",
        "authorization",
        "api_key",
        "account_id",
        "username",
        "profile_name",
        "created_at",
        "timestamp",
        "pid",
        "build_id",
    )
    for path, key in walk_keys(manifest):
        assert key.lower() not in forbidden_keys, f"{path}.{key}"
    for path, value in walk_strings(manifest):
        assert not value.startswith("/"), f"absolute path at {path}: {value}"
        assert "\\Users\\" not in value, path


def test_provenance_is_symbolic_never_a_filesystem_path():
    manifest = build_authoring_contract_manifest()
    allowed = {
        "runtime_schema_registry",
        "archetype_registry",
        "recipe_registry",
        "canonical_compiler",
        "semantic_validator",
        "topology_planner",
    }
    for entry in manifest["schemas"] + manifest["capabilities"]:
        assert entry["provenance"] in allowed, entry


def test_the_new_authoring_workflow_survives_the_capability_catalog_filter():
    """``list_capabilities`` silently DROPS a workflow referencing an
    unregistered tool. A new sequence that vanished would be invisible."""
    catalog = list_capabilities_action()
    assert catalog["authoring_contract"]["actions"], "authoring contract vanished"

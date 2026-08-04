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


def test_the_schema_revision_covers_the_whole_served_authoring_contract():
    """Architect review, P1. The revision hashed only the eight selectors #146
    introduced, so a change to `IntegrationSpecV1` — the component plan every
    typed result embeds — left every outstanding binding looking current. A
    revision that does not move when the contract moves is the failure this
    manifest exists to detect, one level in."""
    from boomi_mcp.authoring.contract import _schema_bundle

    bundle = _schema_bundle()
    for inherited in (
        "IntegrationSpecV1",
        "recipe_contributions",
        "recipe_registry",
        "workflow_sequences",
        "archetype:stub_minimal_integration",
    ):
        assert inherited in bundle, inherited
        assert bundle[inherited] != "unavailable", inherited
    # ...and the owned ones are still there.
    for owned in ("AuthoringRequestV1", "ProcessIRV1", "validation_report"):
        assert owned in bundle, owned


def test_the_schema_revision_moves_when_an_inherited_schema_moves(monkeypatch):
    """Guard the guard: including a selector in the bundle is only useful if its
    movement actually changes the revision."""
    from boomi_mcp.authoring import contract as contract_module

    contract_module.reset_manifest_cache()
    before = contract_module.build_authoring_contract_manifest()["schema_revision"]

    real = contract_module._inherited_schema_digest
    monkeypatch.setattr(
        contract_module,
        "_inherited_schema_digest",
        lambda selector: "sha256:" + "b" * 64
        if selector == "IntegrationSpecV1"
        else real(selector),
    )
    contract_module.reset_manifest_cache()
    after = contract_module.build_authoring_contract_manifest()["schema_revision"]
    contract_module.reset_manifest_cache()

    assert before != after


def test_the_published_support_matrix_matches_the_runtime_refusal():
    """QA #431. The predicate was corrected to key on the compiled artifact, but
    the published matrix kept the old `intent_kind == "process_ir"` conditional
    and went on advertising `recipe.apply: supported` for a route the server
    refuses. One rule with two expressions is one rule that drifts."""
    import inspect

    from boomi_mcp.authoring.contract import (
        AUTHORING_PROCESS_COMPILING_INTENTS,
        AUTHORING_SUPPORT_MATRIX,
    )
    from boomi_mcp.authoring.workflow import _materialization_gaps
    from boomi_mcp.models.integration_models import IntegrationSpecV1

    # ENUMERATED, not derived from the constant under test. Deriving the
    # expectation from `AUTHORING_PROCESS_COMPILING_INTENTS` made this guard
    # tautological: shrinking that tuple to ("process_ir",) put
    # `recipe.apply: "supported"` back on the wire — the exact bug — while the
    # suite stayed green. Same discipline as the enumerated export pins.
    _MUST_BE_REFUSED_AT_APPLY = {"process_ir", "recipe"}
    _MUST_BE_APPLIABLE = {"integration_spec"}
    assert _MUST_BE_REFUSED_AT_APPLY | _MUST_BE_APPLIABLE == set(
        AUTHORING_SUPPORT_MATRIX
    ), "an intent kind was added without deciding whether it can be applied"
    assert set(AUTHORING_PROCESS_COMPILING_INTENTS) == _MUST_BE_REFUSED_AT_APPLY

    spec = IntegrationSpecV1(name="x", components=[])
    for kind, actions in AUTHORING_SUPPORT_MATRIX.items():
        expected = (
            "unsupported" if kind in _MUST_BE_REFUSED_AT_APPLY else "supported"
        )
        assert actions["apply"] == expected, kind
        assert actions["plan"] == "supported", kind
        assert actions["compile"] == "supported", kind

    # The runtime refuses exactly when a process root was compiled — which is
    # what the named set is claiming about these intent kinds.
    assert _materialization_gaps(None, spec, ()) == ()
    source = inspect.getsource(_materialization_gaps)
    assert "if not process_roots:" in source


def test_every_selector_the_revision_covers_is_published():
    """QA #434. Folding a schema into the revision without publishing it left a
    caller unable to see WHICH schema moved."""
    from boomi_mcp.authoring.contract import _schema_bundle

    manifest = build_authoring_contract_manifest()
    published = {entry["selector"] for entry in manifest["schemas"]}
    assert published == set(_schema_bundle())
    inherited = [
        e for e in manifest["schemas"] if not e["owned_by_authoring_contract"]
    ]
    assert {e["selector"] for e in inherited} >= {
        "IntegrationSpecV1",
        "archetype:stub_minimal_integration",
    }


def test_every_archetype_parameter_schema_is_covered_and_individually_named():
    """QA #435 + Codex round 6. The aggregate `archetype_parameters` entry was
    neither fetchable nor able to say WHICH archetype moved; each archetype now
    appears under its own real selector."""
    from boomi_mcp.authoring import contract as contract_module

    contract_module.reset_manifest_cache()
    bundle = contract_module._schema_bundle()
    archetypes = [e["name"] for e in contract_module.list_archetype_registry()]
    assert len(archetypes) == 6
    assert "archetype_parameters" not in bundle
    for name in archetypes:
        selector = f"archetype:{name}"
        assert selector in bundle, selector
        assert bundle[selector].startswith("sha256:"), selector

    # Moving ONE archetype's parameter schema moves the revision, and the entry
    # that moved is identifiable.
    real = contract_module._inherited_schema_digest
    target = f"archetype:{archetypes[0]}"

    def _perturb(selector):
        return "sha256:" + "c" * 64 if selector == target else real(selector)

    contract_module._inherited_schema_digest = _perturb
    try:
        moved = contract_module._schema_bundle()
    finally:
        contract_module._inherited_schema_digest = real
        contract_module.reset_manifest_cache()
    assert moved[target] != bundle[target]
    for name in archetypes[1:]:
        assert moved[f"archetype:{name}"] == bundle[f"archetype:{name}"]


def test_a_documentation_only_edit_does_not_move_the_schema_revision():
    """Codex round 6, P2. The digest fell back to hashing the whole envelope, so
    an archetype's `examples` / `limitations` / `capability_notes` were folded
    into `schema_revision` — a docs edit then failed otherwise-valid typed
    applies on a revision check while the accepted parameters were unchanged."""
    from boomi_mcp.authoring import contract as contract_module
    from boomi_mcp.categories import meta_tools

    contract_module.reset_manifest_cache()
    before = contract_module._schema_bundle()["archetype:stub_minimal_integration"]

    real = meta_tools.get_schema_template_action

    def _with_edited_prose(*args, **kwargs):
        payload = dict(real(*args, **kwargs))
        if kwargs.get("schema_name") == "archetype:stub_minimal_integration":
            payload["examples"] = ["a completely different example"]
            payload["limitations"] = ["reworded limitation"]
            payload["capability_notes"] = "rewritten note"
        return payload

    meta_tools.get_schema_template_action = _with_edited_prose
    try:
        after = contract_module._schema_bundle()["archetype:stub_minimal_integration"]
    finally:
        meta_tools.get_schema_template_action = real
        contract_module.reset_manifest_cache()
    assert after == before, "prose leaked into the schema revision"


def test_capability_revision_covers_every_region_of_the_manifest():
    """Audit finding (high). Shrinking `capability_fingerprint` to cover only
    schemas+archetypes passed all 9103 tests, so `authoring.compile` flipping to
    unsupported — or a compiler_revision move — became invisible to every
    outstanding binding. The four-vs-six archetype guard was the ONLY
    revision-movement pin that existed."""
    from boomi_mcp.authoring import contract as contract_module

    def _revision_with(**overrides):
        contract_module.reset_manifest_cache()
        saved = {}
        try:
            for name, value in overrides.items():
                saved[name] = getattr(contract_module, name)
                setattr(contract_module, name, value)
            contract_module.reset_manifest_cache()
            return contract_module.build_authoring_contract_manifest()[
                "capability_revision"
            ]
        finally:
            for name, value in saved.items():
                setattr(contract_module, name, value)
            contract_module.reset_manifest_cache()

    baseline = _revision_with()

    # Every REGION the manifest publishes must move the revision when it moves.
    from types import MappingProxyType

    flipped_caps = dict(contract_module.AUTHORING_CAPABILITY_REGISTRY)
    flipped_caps["authoring.compile"] = ("unsupported", "1", "canonical_compiler")
    assert _revision_with(
        AUTHORING_CAPABILITY_REGISTRY=MappingProxyType(flipped_caps)
    ) != baseline, "a capability state flip left capability_revision unchanged"

    flipped_matrix = {
        kind: MappingProxyType({**dict(actions), "apply": "unsupported"})
        for kind, actions in contract_module.AUTHORING_SUPPORT_MATRIX.items()
    }
    assert _revision_with(
        AUTHORING_SUPPORT_MATRIX=MappingProxyType(flipped_matrix)
    ) != baseline, "a support-matrix flip left capability_revision unchanged"

    assert _revision_with(
        _compiler_revision=lambda: "sha256:" + "e" * 64
    ) != baseline, "a compiler_revision move left capability_revision unchanged"

    assert _revision_with(
        AUTHORING_ACTIONS=("plan", "apply", "verify")
    ) != baseline, "an action-set change left capability_revision unchanged"

    assert _revision_with(
        _REASON_CODES=MappingProxyType({})
    ) != baseline, "a reason-code change left capability_revision unchanged"

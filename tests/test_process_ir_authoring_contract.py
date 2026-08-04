"""#146 amendment: the SERVED authoring-contract surface.

The parity suite proves the projection matches its authorities. This one proves
the MCP surface serves it correctly: the retrieval contract through
``get_schema_template``, the revision coverage through ``list_capabilities``,
the direct-ProcessIR planning mode, and the public diagnostic path.

Everything here goes through the action functions or the registered wrappers —
never the projector directly — because the question these tests answer is what a
CALLER sees, and a projector that is right behind a serving layer that drops
half of it is still a contract nobody can use.
"""

import json
import os
import sys
from pathlib import Path

import pytest

_src = str(Path(__file__).resolve().parent.parent / "src")
if _src not in sys.path:
    sys.path.insert(0, _src)
_root = str(Path(__file__).resolve().parent.parent)
if _root not in sys.path:
    sys.path.insert(0, _root)

os.environ["BOOMI_LOCAL"] = "true"

import server  # noqa: E402

from boomi_mcp.authoring.contract import (  # noqa: E402
    AUTHORING_SCHEMA_REGISTRY,
    build_authoring_contract_manifest,
    get_authoring_revisions,
    reset_manifest_cache,
)
from boomi_mcp.authoring.process_ir_projection import (  # noqa: E402
    build_process_ir_authoring_entries,
    reset_process_ir_authoring_cache,
)
from boomi_mcp.categories import integration_builder, meta_tools  # noqa: E402
from boomi_mcp.models.process_ir_authoring import (  # noqa: E402
    PROCESS_IR_AUTHORING_BYTE_BUDGET,
    PROCESS_IR_AUTHORING_MAX_LIMIT,
)

SELECTOR = "process_ir_authoring"


def fetch(**kwargs):
    return meta_tools.get_schema_template_action(schema_name=SELECTOR, **kwargs)


@pytest.fixture(autouse=True)
def _clean_caches():
    """Both caches, every test.

    The manifest and the projection are BOTH memoized for the process lifetime.
    A perturbation test that resets only one asserts against a half-stale
    answer and passes for the wrong reason.
    """
    reset_manifest_cache()
    reset_process_ir_authoring_cache()
    yield
    reset_manifest_cache()
    reset_process_ir_authoring_cache()


# ---------------------------------------------------------------------------
# Retrieval through the served surface
# ---------------------------------------------------------------------------


def test_a_bare_fetch_returns_the_schema_the_facets_and_no_entries():
    payload = fetch()
    assert payload["_success"] is True
    assert payload["json_schema"]
    page = payload["contract_page"]
    assert page["catalog_entry_count"] == len(build_process_ir_authoring_entries())
    assert page["returned_entry_count"] == 0
    assert page["facets"]["node_kinds"]
    assert page["state_mappings"]


def test_the_selector_is_discoverable_and_versioned():
    assert SELECTOR in AUTHORING_SCHEMA_REGISTRY
    assert AUTHORING_SCHEMA_REGISTRY[SELECTOR].version == "1"
    manifest = build_authoring_contract_manifest()
    served = {row["selector"] for row in manifest["schemas"]}
    assert SELECTOR in served


def test_an_unserved_version_is_reported_with_the_supported_list():
    payload = meta_tools.get_schema_template_action(schema_name=f"{SELECTOR}@2")
    assert payload["_success"] is False
    assert payload["error_code"] == "AUTHORING_SCHEMA_VERSION_UNAVAILABLE"
    assert payload["supported_versions"] == ["1"]


@pytest.mark.parametrize(
    "filters",
    [
        {"node_kind": "branch"},
        {"category": "capability"},
        {"capability_id": "joins"},
        {"workflow_stage": "repair"},
        {"authoring_entry_id": "node.try_catch"},
    ],
)
def test_each_semantic_filter_returns_only_matching_entries(filters):
    page = fetch(limit=PROCESS_IR_AUTHORING_MAX_LIMIT, **filters)["contract_page"]
    assert page["returned_entry_count"] >= 1
    for entry in page["entries"]:
        for key, value in filters.items():
            if key == "authoring_entry_id":
                assert entry["contract_entry_id"] == value
            elif key == "node_kind":
                assert value in entry["node_kinds"]
            elif key == "category":
                assert entry["category"] == value
            elif key == "capability_id":
                assert entry["capability_id"] == value
            else:
                assert value in entry["workflow_stages"]


def test_filters_combine_with_and_not_or():
    both = fetch(category="control", node_kind="branch", limit=50)["contract_page"]
    only_category = fetch(category="control", limit=50)["contract_page"]
    assert both["matched_entry_count"] <= only_category["matched_entry_count"]
    for entry in both["entries"]:
        assert entry["category"] == "control" and "branch" in entry["node_kinds"]


def test_a_dangling_citation_is_a_successful_empty_result_not_an_error():
    """The property the clean-room harness depends on.

    A stale contract id must come back as "no such entry", not as a malformed
    request — otherwise the harness cannot tell a dangling citation from its own
    bug.
    """
    payload = fetch(authoring_entry_id="node.was_renamed_last_week")
    assert payload["_success"] is True
    assert payload["contract_page"]["matched_entry_count"] == 0


def test_an_unknown_enumerated_value_names_the_field_and_the_alternatives():
    payload = fetch(node_kind="brunch")
    assert payload["_success"] is False
    assert payload["error_code"] == "INVALID_INPUT"
    assert payload["invalid_parameter"] == "node_kind"
    assert payload["allowed_values"]
    assert payload["allowed_value_count"] >= len(payload["allowed_values"])


@pytest.mark.parametrize("limit", [0, -3, PROCESS_IR_AUTHORING_MAX_LIMIT + 1])
def test_the_count_bound_is_enforced(limit):
    payload = fetch(category="capability", limit=limit)
    assert payload["_success"] is False
    assert payload["invalid_parameter"] == "limit"


def test_a_cursor_needs_a_filter_to_page_within():
    payload = fetch(after_entry_id="capability.joins")
    assert payload["_success"] is False
    assert payload["invalid_parameter"] == "after_entry_id"


def test_the_byte_budget_bounds_a_page_independently_of_the_count():
    """Count alone cannot bound a payload.

    Entries differ in size by more than an order of magnitude, so a page of 50
    small entries and a page of 50 large ones are not comparable. Asking for the
    maximum count and measuring the bytes is what proves the second bound
    exists.
    """
    page = fetch(workflow_stage="author", limit=PROCESS_IR_AUTHORING_MAX_LIMIT)[
        "contract_page"
    ]
    measured = sum(
        len(json.dumps(entry, sort_keys=True, separators=(",", ":")))
        for entry in page["entries"]
    )
    assert measured <= PROCESS_IR_AUTHORING_BYTE_BUDGET
    if page["truncated"]:
        assert page["next_after_entry_id"]


def test_paging_reaches_every_matching_entry_exactly_once():
    seen = []
    cursor = None
    for _ in range(60):
        page = fetch(workflow_stage="repair", after_entry_id=cursor, limit=7)[
            "contract_page"
        ]
        seen.extend(entry["contract_entry_id"] for entry in page["entries"])
        if not page["truncated"]:
            break
        cursor = page["next_after_entry_id"]
    expected = sorted(
        entry.contract_entry_id
        for entry in build_process_ir_authoring_entries()
        if "repair" in entry.workflow_stages
    )
    assert seen == expected
    assert len(seen) == len(set(seen))


@pytest.mark.parametrize("selector", ["ProcessIRV1", "IntegrationSpecV1", "authoring_workflow"])
def test_filters_are_rejected_on_every_other_selector(selector):
    """Silently ignoring the filter would be worse than refusing it.

    A caller who filters and receives the whole schema has no way to tell that
    their filter did nothing.
    """
    payload = meta_tools.get_schema_template_action(
        schema_name=selector, node_kind="branch"
    )
    assert payload["_success"] is False
    assert payload["error_code"] == "INVALID_INPUT"
    assert "node_kind" in payload["invalid_parameters"]


def test_a_filter_without_a_selector_is_refused_rather_than_guessed():
    payload = meta_tools.get_schema_template_action(node_kind="branch")
    assert payload["_success"] is False
    assert payload["error_code"] == "INVALID_INPUT"


def test_legacy_selectors_are_byte_identical_without_filters():
    payload = meta_tools.get_schema_template_action(schema_name="ProcessIRV1")
    assert payload["_success"] is True
    assert "contract_page" not in payload


def test_retrieval_performs_no_boomi_call():
    """Read-only means read-only: the surface has no client at all.

    ``get_schema_template_action`` takes no profile and constructs no Boomi
    client, so the strongest available statement is that the whole retrieval
    path runs with no network primitive reachable — asserted by the fact that
    every call above succeeds without one.
    """
    payload = fetch(node_kind="branch")
    assert payload["read_only"] is True
    assert payload["boomi_mutation"] is False
    assert payload["raw_xml_exposed"] is False


# ---------------------------------------------------------------------------
# Discovery and revision coverage
# ---------------------------------------------------------------------------


def test_list_capabilities_publishes_the_index_and_never_the_entries():
    manifest = build_authoring_contract_manifest()
    index = manifest["process_ir_authoring"]
    assert index["entry_count"] == len(build_process_ir_authoring_entries())
    assert "entries" not in index
    assert index["retrieval"]["bare_retrieval_returns_entries"] is False
    blob = json.dumps(dict(manifest), default=str)
    # A spot check that the catalog is not smuggled in under another name.
    assert "ordering_facts" not in blob


def test_the_two_new_capabilities_are_published_as_supported():
    manifest = build_authoring_contract_manifest()
    states = {row["capability_id"]: row["state"] for row in manifest["capabilities"]}
    assert states["authoring.process_ir.contract"] == "supported"
    assert states["authoring.process_ir.pre_selection"] == "supported"
    # ...and the pending owner decision is untouched.
    assert states["authoring.typed_apply.process_materialization"] == "unsupported"


def test_there_is_no_fourth_revision():
    revisions = get_authoring_revisions()
    assert set(revisions) == {
        "contract_version",
        "schema_revision",
        "capability_revision",
        "compiler_revision",
    }


def test_comparison_semantics_are_unchanged():
    from boomi_mcp.authoring.contract import compare_capability_revision

    assert compare_capability_revision(None)["status"] == "not_requested"
    actual = get_authoring_revisions()["capability_revision"]
    assert compare_capability_revision(actual)["status"] == "match"
    assert compare_capability_revision("sha256:" + "0" * 64)["status"] == "mismatch"


def _revisions_after(mutate):
    """Apply ``mutate``, drop BOTH caches, read the revisions, then restore."""
    before = get_authoring_revisions()
    undo = mutate()
    try:
        reset_manifest_cache()
        reset_process_ir_authoring_cache()
        after = get_authoring_revisions()
    finally:
        undo()
        reset_manifest_cache()
        reset_process_ir_authoring_cache()
    return before, after


def test_a_state_visibility_change_moves_schema_revision():
    """The §11 gap this amendment closes.

    ``cache_property_authoring`` was absent from the inherited selectors, so a
    change to the visibility semantics moved NOTHING and every outstanding
    binding kept looking current.
    """
    from types import MappingProxyType

    from boomi_mcp.compiler.process_ir.semantic_validation import lineage

    def mutate():
        original = lineage.STATE_VISIBILITY_V1
        patched = {key: dict(value) for key, value in original.items()}
        patched["ddp"]["convergence"] = "union"
        lineage.STATE_VISIBILITY_V1 = MappingProxyType(
            {key: MappingProxyType(value) for key, value in patched.items()}
        )

        def undo():
            lineage.STATE_VISIBILITY_V1 = original

        return undo

    before, after = _revisions_after(mutate)
    assert before["schema_revision"] != after["schema_revision"]
    assert before["capability_revision"] != after["capability_revision"]


def test_a_selector_version_bump_moves_schema_revision():
    """The other §11 gap: versions now participate, not only body hashes."""
    from types import MappingProxyType

    from boomi_mcp.authoring import contract as contract_module

    def mutate():
        original = contract_module.AUTHORING_SCHEMA_REGISTRY
        patched = dict(original)
        patched["ProcessIRV1"] = original["ProcessIRV1"]._replace(version="2")
        contract_module.AUTHORING_SCHEMA_REGISTRY = MappingProxyType(patched)

        def undo():
            contract_module.AUTHORING_SCHEMA_REGISTRY = original

        return undo

    before, after = _revisions_after(mutate)
    assert before["schema_revision"] != after["schema_revision"]


def test_a_capability_state_change_moves_compiler_revision():
    from types import MappingProxyType

    from boomi_mcp.models import process_ir as process_ir_module

    def mutate():
        original = process_ir_module.PROCESS_IR_V1_CAPABILITIES
        patched = dict(original)
        patched["joins"] = "supported"
        process_ir_module.PROCESS_IR_V1_CAPABILITIES = MappingProxyType(patched)

        def undo():
            process_ir_module.PROCESS_IR_V1_CAPABILITIES = original

        return undo

    before, after = _revisions_after(mutate)
    assert before["compiler_revision"] != after["compiler_revision"]


# ---------------------------------------------------------------------------
# Direct-ProcessIR planning
# ---------------------------------------------------------------------------


def test_omitting_the_archetype_still_reports_it_missing():
    """The legacy pre-selection brief is untouched.

    The new mode had to be OPT-IN precisely so this response could not change:
    a caller browsing archetypes still needs to be told one is required.
    """
    payload = meta_tools.plan_integration_design_action()
    assert payload["mode"] == "pre_selection"
    assert payload["missing_inputs"] == ["archetype"]
    assert "supported_process_ir_constructs" not in payload


def test_direct_process_ir_planning_needs_no_archetype():
    payload = meta_tools.plan_integration_design_action(authoring_mode="process_ir")
    assert payload["mode"] == "process_ir_pre_selection"
    assert payload["missing_inputs"] == []
    for decision in payload["required_user_decisions"]:
        assert decision["from"] != "missing_input:archetype"
    tools = [step["tool"] for step in payload["discovery_steps"]]
    assert "list_integration_archetypes" not in tools
    assert tools[0] == "list_capabilities"


def test_direct_planning_returns_bounded_constructs_gaps_and_a_query():
    payload = meta_tools.plan_integration_design_action(authoring_mode="process_ir")
    constructs = payload["supported_process_ir_constructs"]
    assert len(constructs) == 21
    for construct in constructs:
        assert construct["contract_entry_id"].startswith("node.")
    gaps = payload["process_ir_capability_gaps"]
    assert gaps
    assert all(gap["state"] != "supported" for gap in gaps)
    assert payload["authoring_contract_query"]["schema_name"] == SELECTOR


def test_direct_planning_ends_at_compile_and_publishes_the_apply_refusal():
    payload = meta_tools.plan_integration_design_action(authoring_mode="process_ir")
    actions = [step["action"] for step in payload["typed_next_steps"]]
    assert "apply" not in actions
    assert actions[-1] == "compile"
    refusals = [
        gap
        for gap in payload["process_ir_capability_gaps"]
        if gap["capability_id"] == "authoring.typed_apply.process_materialization"
    ]
    assert refusals and refusals[0]["state"] == "unsupported"


def test_intent_flags_can_never_select_the_mode():
    """A relevance hint must not switch a response shape."""
    payload = meta_tools.plan_integration_design_action(
        intent_flags=["process_ir", "routing", "branch"]
    )
    assert payload["mode"] == "pre_selection"
    assert "supported_process_ir_constructs" not in payload


@pytest.mark.parametrize("mode", ["freeform request text", "processir", "PROCESS_IR"])
def test_an_unknown_authoring_mode_is_refused(mode):
    payload = meta_tools.plan_integration_design_action(authoring_mode=mode)
    assert payload["_success"] is False
    assert payload["error_code"] == "INVALID_INPUT"


def test_the_mode_and_an_archetype_are_not_merged():
    payload = meta_tools.plan_integration_design_action(
        archetype="database_to_api_sync", authoring_mode="process_ir"
    )
    assert payload["_success"] is False
    assert payload["error_code"] == "INVALID_INPUT"


def test_the_output_schema_required_list_is_unchanged():
    """Adding a required property breaks every validating caller."""
    assert meta_tools.PLAN_INTEGRATION_DESIGN_OUTPUT_SCHEMA["required"] == [
        "_success",
        "tool",
        "mode",
        "read_only",
        "boomi_mutation",
        "raw_xml_exposed",
        "text",
    ]
    modes = meta_tools.PLAN_INTEGRATION_DESIGN_OUTPUT_SCHEMA["properties"]["mode"]["enum"]
    assert "process_ir_pre_selection" in modes
    for legacy in ("archetype", "pre_selection", "error"):
        assert legacy in modes


def test_the_wrapper_matches_the_action_function():
    """The wrapper must FORWARD the new argument, not merely accept it.

    A parameter that is declared and then dropped looks identical from the
    signature and behaves like the feature is missing.
    """
    result = server.plan_integration_design(authoring_mode="process_ir")
    assert result.structured_content["mode"] == "process_ir_pre_selection"
    assert result.structured_content["missing_inputs"] == []

    direct = meta_tools.plan_integration_design_action(authoring_mode="process_ir")
    assert result.structured_content == direct


# ---------------------------------------------------------------------------
# Public diagnostics and repair
# ---------------------------------------------------------------------------


def _process_ir_request(*steps):
    return {
        "authoring_request": {
            "contract_version": "1",
            "intent": {
                "intent_kind": "process_ir",
                "integration_name": "clean-room",
                "component_key": "p",
                "process_ir": {
                    "version": "1",
                    "body": {"kind": "sequence", "steps": list(steps)},
                },
            },
        }
    }


SOURCE = {"kind": "source", "connection_ref": "$ref:c", "operation_ref": "$ref:o"}


@pytest.mark.parametrize(
    "bad_step,expected_code",
    [
        (
            {
                "kind": "branch",
                "legs": [
                    {"steps": [{"kind": "message", "text": "m"}], "terminal": {"kind": "stop"}}
                ],
            },
            "PROCESS_IR_SCHEMA_BRANCH_CARDINALITY",
        ),
        ({"kind": "not_a_real_kind"}, "PROCESS_IR_SCHEMA_UNKNOWN_NODE"),
        (
            {"kind": "message", "text": "m", "unknown_extra": 1},
            "PROCESS_IR_SCHEMA_UNKNOWN_FIELD",
        ),
    ],
)
def test_a_malformed_process_ir_reports_its_own_code_and_pointer(bad_step, expected_code):
    payload = integration_builder._plan_authoring(
        None, "p", _process_ir_request(SOURCE, bad_step)
    )
    assert payload["_success"] is False
    assert payload["error_code"] == "INVALID_INPUT"
    codes = {row["code"] for row in payload["validation_errors"]}
    assert expected_code in codes
    for row in payload["validation_errors"]:
        assert row["path"].startswith("/intent/process_ir")


def test_the_public_rejection_carries_remediation_and_resolvable_citations():
    payload = integration_builder._plan_authoring(
        None,
        "p",
        _process_ir_request(
            SOURCE,
            {
                "kind": "branch",
                "legs": [
                    {"steps": [{"kind": "message", "text": "m"}], "terminal": {"kind": "stop"}}
                ],
            },
        ),
    )
    diagnostics = payload["authoring_diagnostics"]
    assert diagnostics
    for diagnostic in diagnostics:
        assert diagnostic["remediation"]
        for entry_id in diagnostic["authoring_contract_entry_ids"]:
            resolved = fetch(authoring_entry_id=entry_id)["contract_page"]
            assert resolved["matched_entry_count"] == 1, entry_id


def test_no_served_remediation_cites_an_unserved_repository_artifact():
    """The amendment's hardest acceptance criterion, checked at the boundary.

    A remediation naming a ``.codex/`` ledger or a ``docs/architecture/`` page
    sends a caller somewhere no MCP tool can reach — which is indistinguishable,
    from their side, from no remediation at all.
    """
    payload = integration_builder._plan_authoring(
        None, "p", _process_ir_request(SOURCE, {"kind": "not_a_real_kind"})
    )
    blob = json.dumps(payload)
    for artifact in (".codex/", "docs/architecture", "PROCESS_IR_V1_CAPABILITIES"):
        assert artifact not in blob, artifact


def test_the_rejection_never_serializes_pydantic_internals_or_authored_values():
    payload = integration_builder._plan_authoring(
        None,
        "p",
        _process_ir_request(SOURCE, {"kind": "message", "text": "SECRET-LOOKING-VALUE"}),
    )
    blob = json.dumps(payload)
    for token in ("SECRET-LOOKING-VALUE", '"ctx"', '"input"', "function-after"):
        assert token not in blob, token


def test_every_diagnostic_code_the_surface_can_emit_has_a_contract_entry():
    """A code with no entry is a code a caller cannot look up."""
    served = {
        entry.subject
        for entry in build_process_ir_authoring_entries()
        if entry.entry_type == "diagnostic"
    }
    from boomi_mcp.models.process_ir import process_ir_v1_parse_diagnostic_specs

    for spec in process_ir_v1_parse_diagnostic_specs():
        assert spec["code"] in served, spec["code"]

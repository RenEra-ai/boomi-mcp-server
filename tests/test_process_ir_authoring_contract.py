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


# ---------------------------------------------------------------------------
# QA round 20 regressions (#449-#453)
# ---------------------------------------------------------------------------


def test_every_advertised_workflow_stage_matches_at_least_one_entry():
    """#449. A stage that matches nothing is a filter that only looks available.

    ``compile`` was declared, documented on the wrapper, and matched zero
    entries — while being the stage an LLM is most likely to try, because the
    typed next steps end at compile and a third of the catalog is compile
    diagnostics. Diagnostics now carry the phase that RAISES them.
    """
    from boomi_mcp.models.process_ir_authoring import (
        PROCESS_IR_AUTHORING_WORKFLOW_STAGES,
    )

    facets = fetch()["contract_page"]["facets"]["workflow_stages"]
    assert set(facets) == set(PROCESS_IR_AUTHORING_WORKFLOW_STAGES)
    for stage in PROCESS_IR_AUTHORING_WORKFLOW_STAGES:
        page = fetch(workflow_stage=stage, limit=1)["contract_page"]
        assert page["matched_entry_count"] > 0, stage


def test_compile_reachability_contains_plan_reachability():
    """#454. ``compile`` re-runs parse and semantic validation, so it is a strict
    SUPERSET of ``plan`` — and the filter has to say so.

    Filing each code by the module that EMITS it looked right and was wrong: a
    caller repairing a rejected compile filtered by ``compile`` and missed most
    of the codes they had just received, because those codes are "owned" by the
    parse and validation layers that compile also runs.
    """
    diagnostics = [
        entry
        for entry in build_process_ir_authoring_entries()
        if entry.entry_type == "diagnostic"
    ]
    assert diagnostics

    plan_codes = {e.subject for e in diagnostics if "plan" in e.workflow_stages}
    compile_codes = {e.subject for e in diagnostics if "compile" in e.workflow_stages}
    repair_codes = {e.subject for e in diagnostics if "repair" in e.workflow_stages}

    assert plan_codes, "plan must reach the parse and validation codes"
    assert plan_codes < compile_codes, "compile must strictly contain plan"
    assert compile_codes == repair_codes == {e.subject for e in diagnostics}

    for entry in fetch(workflow_stage="compile", limit=50)["contract_page"]["entries"]:
        assert entry["entry_type"] == "diagnostic"


def test_a_code_the_compile_action_really_raises_is_filed_under_compile():
    """The claim, checked against the behaviour rather than against itself.

    A parse-owned code that a real ``action='compile'`` call returns must be
    reachable by ``workflow_stage='compile'``, or the filter is advertising a
    phase model the server does not implement.
    """
    request = {
        "authoring_request": {
            "contract_version": "1",
            "intent": {
                "intent_kind": "process_ir",
                "integration_name": "x",
                "component_key": "p",
                "process_ir": {
                    "version": "1",
                    "body": {
                        "kind": "sequence",
                        "steps": [
                            {
                                "kind": "source",
                                "connection_ref": "$ref:c",
                                "operation_ref": "$ref:o",
                            },
                            {
                                "kind": "branch",
                                "legs": [
                                    {
                                        "steps": [{"kind": "message", "text": "m"}],
                                        "terminal": {"kind": "stop"},
                                    }
                                ],
                            },
                        ],
                    },
                },
            },
        }
    }
    result = integration_builder._compile_authoring(None, "p", request)
    raised = {row["code"] for row in result["validation_errors"]}
    assert raised

    reachable = {
        entry["subject"]
        for entry in fetch(workflow_stage="compile", limit=50)["contract_page"]["entries"]
    }
    # Page through the rest of the compile facet.
    cursor = fetch(workflow_stage="compile", limit=50)["contract_page"]
    while cursor["truncated"]:
        cursor = fetch(
            workflow_stage="compile", after_entry_id=cursor["next_after_entry_id"], limit=50
        )["contract_page"]
        reachable |= {entry["subject"] for entry in cursor["entries"]}

    assert raised <= reachable, sorted(raised - reachable)


@pytest.mark.parametrize(
    "filters,expected_field",
    [({"limit": 99}, "limit"), ({"after_entry_id": "node.branch"}, "after_entry_id")],
)
def test_a_non_enumerated_filter_error_states_its_own_rule(filters, expected_field):
    """#450. An empty allowed-values list beside "use a published value" is worse
    than silence — for a cursor the rejected value IS published, so the advice
    sent the caller in a circle.
    """
    payload = fetch(**filters)
    assert payload["_success"] is False
    assert payload["invalid_parameter"] == expected_field
    assert payload["rule"]
    assert payload["suggestion"] == payload["rule"]
    # The misleading enum fields must be ABSENT, not present-and-empty.
    assert "allowed_values" not in payload
    assert "allowed_value_count" not in payload


def test_an_enumerated_filter_error_still_lists_its_facet():
    payload = fetch(category="nonsense")
    assert payload["allowed_values"]
    assert "rule" not in payload


def test_no_served_remediation_ships_an_unsubstituted_placeholder():
    """#451. A remediation a caller can paste must be one that works.

    ``node_kind='<kind>'`` returned INVALID_INPUT when followed literally, and
    the placeholder was baked into the served contract data too.
    """
    import re

    # ANY angle-bracketed token, not just <lowercase_underscore>. The narrow
    # pattern is how '<that kind>' and '<one of them>' each slipped through a
    # round after '<kind>' was fixed — three wordings of one defect, because the
    # pin described one spelling of a placeholder rather than the shape of one.
    blob = json.dumps(
        [entry.model_dump(mode="json") for entry in build_process_ir_authoring_entries()]
    )
    placeholders = sorted(set(re.findall(r"<[^<>]{1,40}>", blob)))
    assert placeholders == [], placeholders


def test_the_entry_byte_budget_is_published_for_what_it_actually_bounds():
    """#452. The budget caps the ENTRIES, not the whole response.

    Published as a bare ``byte_budget`` it read as a payload cap, and the
    envelope (schema + facets + state mappings) pushed real responses past it.
    The accounting was always correct; the name was not.
    """
    from boomi_mcp.authoring.process_ir_projection import (
        build_process_ir_authoring_index,
    )

    retrieval = build_process_ir_authoring_index()["retrieval"]
    assert "byte_budget" not in retrieval
    assert retrieval["entry_byte_budget"] == PROCESS_IR_AUTHORING_BYTE_BUDGET
    assert "entries array only" in retrieval["entry_byte_budget_scope"]

    page = fetch(workflow_stage="author", limit=PROCESS_IR_AUTHORING_MAX_LIMIT)[
        "contract_page"
    ]
    measured = sum(
        len(json.dumps(entry, sort_keys=True, separators=(",", ":")))
        for entry in page["entries"]
    )
    assert measured <= PROCESS_IR_AUTHORING_BYTE_BUDGET


def test_no_model_this_amendment_added_cites_an_unfetchable_document():
    """#453. The amendment must not ADD to the citation debt it set out to pay.

    40 such references predate this work in other selectors and are out of its
    scope; the one it introduced is not.
    """
    schema = meta_tools.get_schema_template_action(
        schema_name="AuthoringPlanResultV1"
    )["json_schema"]
    added = ("AuthoringEvidenceV1",)
    for name in added:
        description = schema["$defs"][name].get("description", "")
        for token in ("ADR-001", "AUTHORING_WORKFLOW_V1", "docs/", ".codex/"):
            assert token not in description, (name, token)


def test_no_diagnostic_remediation_cites_an_unfetchable_artifact():
    """The acceptance criterion, asserted over every code the surface can emit."""
    for entry in build_process_ir_authoring_entries():
        if entry.entry_type != "diagnostic":
            continue
        text = " ".join(entry.ordering_facts) + entry.summary
        for token in ("ADR-001", "AUTHORING_WORKFLOW_V1", "docs/", ".codex/"):
            assert token not in text, (entry.contract_entry_id, token)


def test_every_diagnostic_remediation_is_followable_without_substitution():
    """#455. A remediation a caller pastes must work as pasted.

    Two wordings failed: a literal ``<kind>`` placeholder, then an instruction
    to read the kind "from the path" — which is a JSON pointer of field names
    and indices, so the kind is recoverable in only one of three shapes. Any
    ``process_ir_authoring`` call a remediation names must therefore be exact.
    """
    import re

    pattern = re.compile(
        r"get_schema_template\(\s*schema_name='process_ir_authoring'\s*,\s*"
        r"(\w+)='([^']+)'\s*\)"
    )
    checked = 0
    for entry in build_process_ir_authoring_entries():
        if entry.entry_type != "diagnostic":
            continue
        for text in entry.ordering_facts:
            for field, value in pattern.findall(text):
                assert "<" not in value and ">" not in value, (entry.subject, value)
                page = fetch(**{field: value})
                assert page["_success"] is True, (entry.subject, field, value)
                assert page["contract_page"]["matched_entry_count"] > 0, (
                    entry.subject,
                    field,
                    value,
                )
                checked += 1
    assert checked, "no remediation named a contract call — the pin is vacuous"


def test_the_wrapper_docstring_agrees_with_the_served_budget_scope():
    """#456. A tool description that contradicts the served data is a claim defect.

    The docstring said "payload budget" while the payload said the budget covers
    the entries only. Both cannot be true, and the caller reads the docstring.
    """
    doc = server.get_schema_template.__doc__
    assert "payload budget" not in doc
    assert "budget on the ENTRIES" in doc
    assert "entry_byte_budget_scope" in doc


def test_no_served_selector_anywhere_cites_an_unfetchable_repository_path():
    """#459. The scan that matters is over EVERY selector, not the new ones.

    A round-22 sweep of all 105 selectors found one leak the amendment had not
    touched — a pre-existing archetype description citing a capture ledger.
    Scoping the check to the surfaces this work introduced is how it survived
    two rounds: the defect class is "a served string points somewhere no MCP
    tool can reach", and that class is not bounded by which issue wrote it.
    """
    names = meta_tools._valid_schema_names()
    assert len(names) > 50, "the sweep must actually be broad"
    leaks = {}
    for name in names:
        try:
            payload = meta_tools.get_schema_template_action(schema_name=name)
        except Exception:  # noqa: BLE001 — a selector that cannot build is not a leak
            continue
        blob = json.dumps(payload, default=str)
        for artifact in (".codex/", "docs/architecture/"):
            if artifact in blob:
                leaks.setdefault(name, []).append(artifact)
    assert leaks == {}, leaks


def test_a_remediation_pointer_delivers_more_than_the_sentence_that_cited_it():
    """#458. A pointer that resolves but teaches nothing has moved the defect.

    The first fix for the placeholder bug replaced an unfollowable citation with
    a circular one: it pointed at the diagnostic's own entry, whose only prose
    was the citing sentence itself. So the test is not "does it resolve" but
    "does the destination carry prose the caller did not already have".
    """
    import re

    pattern = re.compile(
        r"get_schema_template\(\s*schema_name='process_ir_authoring'\s*,\s*"
        r"(\w+)='([^']+)'\s*\)"
    )
    checked = 0
    for entry in build_process_ir_authoring_entries():
        if entry.entry_type != "diagnostic":
            continue
        source_text = " ".join(entry.ordering_facts) + entry.summary
        for field, value in pattern.findall(" ".join(source_text.split())):
            page = fetch(**{field: value}, limit=50)["contract_page"]
            assert page["matched_entry_count"] > 0, (entry.subject, field, value)
            delivered = "".join(
                " ".join(row["ordering_facts"]) + row["summary"]
                for row in page["entries"]
                if row["contract_entry_id"] != entry.contract_entry_id
            )
            assert delivered.strip(), (
                f"{entry.subject} cites {field}={value!r}, which returns only "
                f"itself — a circular pointer"
            )
            checked += 1
    assert checked, "no diagnostic named a contract call — the pin is vacuous"


# ---------------------------------------------------------------------------
# Codex commit-review round 1 regressions
# ---------------------------------------------------------------------------


def _malformed_apply_config():
    return {
        "authoring_request": {
            "contract_version": "1",
            "intent": {
                "intent_kind": "process_ir",
                "integration_name": "x",
                "component_key": "p",
                "process_ir": {
                    "version": "1",
                    "body": {
                        "kind": "sequence",
                        "steps": [
                            {
                                "kind": "source",
                                "connection_ref": "$ref:c",
                                "operation_ref": "$ref:o",
                            },
                            {
                                "kind": "branch",
                                "legs": [
                                    {
                                        "steps": [{"kind": "message", "text": "m"}],
                                        "terminal": {"kind": "stop"},
                                    }
                                ],
                            },
                        ],
                    },
                },
            },
            "expected_capability_revision": "sha256:" + "0" * 64,
            "expected_compile_hash": "sha256:" + "0" * 64,
        }
    }


def test_apply_reports_a_malformed_process_ir_like_plan_and_compile_do():
    """The route that MUTATES must not be the one with the worst error shape.

    Routing the shared parser through the typed-apply preflight without adding
    the matching catch let the new exception reach the generic dispatcher: an
    unstructured failure carrying neither the typed-apply mutation fields nor
    any diagnostic. Four validation sites, three of them updated, is not a
    shared parser.
    """
    result = integration_builder.build_integration_action(
        None, "p", "apply", config=_malformed_apply_config()
    )
    assert result["_success"] is False
    assert result["error_code"] == "INVALID_INPUT"
    # The typed-apply envelope's always-present promise.
    assert result["mutation_performed"] is False
    assert result["mutation_status"] == "none"
    diagnostics = result["authoring_diagnostics"]
    assert diagnostics
    assert diagnostics[0]["code"].startswith("PROCESS_IR_")
    assert diagnostics[0]["path"].startswith("/intent/process_ir")


@pytest.mark.parametrize("action", ["plan", "compile", "apply"])
def test_all_three_typed_routes_agree_on_a_malformed_process_ir(action):
    """One parser must mean one reported shape, whichever door you came in by."""
    result = integration_builder.build_integration_action(
        None, "p", action, config=_malformed_apply_config()
    )
    assert result["error_code"] == "INVALID_INPUT"
    codes = {row["code"] for row in result["validation_errors"]}
    assert "PROCESS_IR_SCHEMA_BRANCH_CARDINALITY" in codes


def test_every_generated_recipe_selector_is_actually_fetchable():
    """A selector nobody can fetch is a citation that fails at the first hop.

    The registry SNAPSHOT is served under `recipe_registry`; a single descriptor
    is fetched with `recipe:<id>[@<version>]`. Emitting the former for the
    latter made every per-recipe selector return SCHEMA_NAME_UNSUPPORTED.
    """
    checked = 0
    for entry in build_process_ir_authoring_entries():
        if not entry.recipe_selector:
            continue
        payload = meta_tools.get_schema_template_action(
            schema_name=entry.recipe_selector
        )
        assert payload["_success"] is True, (entry.contract_entry_id, payload)
        checked += 1
    assert checked, "no recipe selector was generated — the pin is vacuous"


def test_every_generated_doctrine_selector_is_actually_fetchable():
    """The sibling property, pinned for the same reason."""
    checked = 0
    for entry in build_process_ir_authoring_entries():
        if not entry.doctrine_selector:
            continue
        payload = meta_tools.get_schema_template_action(
            schema_name=entry.doctrine_selector
        )
        assert payload["_success"] is True, (entry.contract_entry_id, payload)
        checked += 1
    assert checked


def test_a_connector_node_publishes_no_blanket_document_claim():
    """What a connector call returns is decided by its ACTION, not by the node.

    A single ``output_documents: documents`` on the node entry contradicted the
    published database ``Send`` row, which produces none — and an exact-id
    lookup returns only the node, so a caller citing it would place a consumer
    after a terminal call.
    """
    for kind in ("connector_call", "source", "target"):
        entry = next(
            e
            for e in build_process_ir_authoring_entries()
            if e.contract_entry_id == f"node.{kind}"
        )
        assert entry.document_semantics is None, kind
        linked = [
            related
            for related in entry.related_entry_ids
            if related.startswith("connector_action.")
        ]
        assert linked, f"node.{kind} must point at the per-action rows"
        for related in linked:
            assert resolve_one(related) is not None


def resolve_one(entry_id):
    page = fetch(authoring_entry_id=entry_id)["contract_page"]
    return page["entries"][0] if page["entries"] else None


def test_the_default_value_description_matches_what_the_validator_does():
    """A served description that contradicts the validator is worse than none.

    ``_reads_of`` marks a defaulted read ``has_default=True`` and the lineage
    walk then skips the unmet-read check, so the rejection the descriptions
    promised never happens. A caller would repair a flow that was never broken.
    """
    from boomi_mcp.models.process_ir import process_ir_v1_json_schema

    defs = process_ir_v1_json_schema()["$defs"]
    for name in ("DdpPropertySourceV1", "DppPropertySourceV1"):
        description = " ".join(defs[name]["description"].split())
        assert "DISCHARGES the read-before-write rule" in description, name
        assert "does not discharge" not in description, name


def test_direct_process_ir_next_steps_never_prepare_the_caller_for_apply():
    """The response may not declare apply unsupported and then coach for it."""
    payload = meta_tools.plan_integration_design_action(authoring_mode="process_ir")
    for step in payload["typed_next_steps"]:
        assert step["action"] != "apply"
        assert "bind apply to" not in step["why"]
    compile_step = next(
        s for s in payload["typed_next_steps"] if s["action"] == "compile"
    )
    assert "apply is refused" in compile_step["why"]


# ---------------------------------------------------------------------------
# The instruction contract, pinned by EXECUTION rather than by spelling
# ---------------------------------------------------------------------------

import ast  # noqa: E402
import re  # noqa: E402

_CALL_PREFIX = "get_schema_template("


def _harvest_calls(text):
    """Every ``get_schema_template(...)`` occurrence, balanced, or a marker.

    A regex over ``[^()]*`` cannot see malformed OUTER syntax: an extra ``)``
    leaves a valid interior to match, and a missing ``)`` matches nothing at
    all. Either way the broken instruction never reaches the classifier and the
    build stays green. So the scan starts at each literal ``get_schema_template(``
    and balances forward; an unbalanced occurrence is yielded as ``None`` and
    fails the caller.
    """
    out = []
    index = text.find(_CALL_PREFIX)
    while index != -1:
        cursor = index + len(_CALL_PREFIX)
        depth = 1
        while cursor < len(text) and depth:
            if text[cursor] == "(":
                depth += 1
            elif text[cursor] == ")":
                depth -= 1
            cursor += 1
        out.append(None if depth else text[index + len(_CALL_PREFIX) : cursor - 1])
        index = text.find(_CALL_PREFIX, index + len(_CALL_PREFIX))
    return out

#: A placeholder to substitute, or an enumeration of alternatives — both are
#: EXAMPLES, not instructions to paste. Classified explicitly rather than left
#: to fall out of a bare ``except``: a silent skip cannot tell "this is a
#: template" from "this is malformed", which is exactly how a broken instruction
#: would keep CI green.
_TEMPLATE_MARKERS = ("<", "...", "…")
_ALTERNATION = re.compile(r"'\s*\|\s*'")


def _served_strings():
    """Every string on every surface that serves instructions.

    Deliberately NOT just the ``schema_name`` selectors. ``list_capabilities``
    publishes 23 template call strings of its own, and the advisory planner
    publishes discovery steps — a sweep that misses them is a sweep whose green
    means less than it looks like.
    """
    surfaces = []
    for name in meta_tools._valid_schema_names():
        try:
            surfaces.append(meta_tools.get_schema_template_action(schema_name=name))
        except Exception:  # noqa: BLE001 — a selector that cannot build serves nothing
            continue
    for resource_type in meta_tools._VALID_RESOURCE_TYPES:
        try:
            surfaces.append(
                meta_tools.get_schema_template_action(resource_type=resource_type)
            )
        except Exception:  # noqa: BLE001
            continue
    # The authoring contract's own ENTRIES. A bare selector deliberately returns
    # none, so a sweep that only fetches selectors never reads the remediations
    # this contract exists to publish — the exact strings four rounds of
    # findings were about. Paged through the served surface, not read off the
    # projector, so what is checked is what a caller receives.
    facets = meta_tools.get_schema_template_action(
        schema_name="process_ir_authoring"
    )["contract_page"]["facets"]
    for category in facets["categories"]:
        cursor = None
        seen_cursors = set()
        # Paginate to a TERMINAL page. A fixed iteration bound that falls out
        # still truncated omits entries silently, and the sweeps then pass
        # while never reading them — which is the failure mode this whole guard
        # exists to prevent, one level up. The loop instead asserts that the
        # cursor advances and that it ends on an untruncated page.
        while True:
            page = meta_tools.get_schema_template_action(
                schema_name="process_ir_authoring",
                category=category,
                after_entry_id=cursor,
                limit=50,
            )["contract_page"]
            surfaces.append(page)
            if not page["truncated"]:
                break
            cursor = page["next_after_entry_id"]
            assert cursor, f"{category}: truncated page carried no cursor"
            assert cursor not in seen_cursors, f"{category}: cursor did not advance"
            seen_cursors.add(cursor)

    surfaces.append(meta_tools.list_capabilities_action())
    surfaces.append(meta_tools.plan_integration_design_action())
    surfaces.append(meta_tools.plan_integration_design_action(authoring_mode="process_ir"))

    # Every registered tool's DESCRIPTION. This is the surface an LLM reads
    # before it reads anything else, and it was 0% covered: a poisoned tool
    # description — a broken call, a bare `.md`, a `docs/architecture/` path —
    # left the whole suite green. A guard blind to the first surface is a guard
    # whose green means little.
    seen_docs = set()
    for attribute in vars(server).values():
        doc = getattr(attribute, "__doc__", None)
        if callable(attribute) and doc and doc not in seen_docs:
            seen_docs.add(doc)
            surfaces.append(doc)

    def walk(value):
        if isinstance(value, str):
            yield value
        elif isinstance(value, dict):
            for item in value.values():
                yield from walk(item)
        elif isinstance(value, (list, tuple)):
            for item in value:
                yield from walk(item)

    for surface in surfaces:
        yield from walk(surface)


#: ``get_schema_template``'s positional order, so a positional instruction can
#: be executed rather than written off. ``get_schema_template("component")`` is
#: a legal, working call; classifying it "malformed" was a bug in the guard, and
#: it was the bug that made widening the sweep look impossible.
_POSITIONAL_PARAMETERS = (
    "resource_type",
    "operation",
    "standard",
    "component_type",
    "protocol",
    "schema_name",
)


def _classify_call(argument_text):
    """``('mention'|'template'|'executable'|'malformed', kwargs)`` for one call.

    Four buckets, none of them a silent drop:

    * ``mention`` — no arguments at all. ``Use get_schema_template() before
      create/update`` names the tool; it is not a call to paste.
    * ``template`` — a placeholder or an alternation of choices: an example.
    * ``executable`` — a literal call, positional or keyword, that must succeed.
    * ``malformed`` — anything else, and it FAILS the test rather than vanishing.
    """
    stripped = argument_text.strip()
    if not stripped:
        return "mention", {}
    if any(marker in stripped for marker in _TEMPLATE_MARKERS):
        return "template", {}
    if _ALTERNATION.search(stripped):
        return "template", {}
    try:
        node = ast.parse(f"f({stripped})", mode="eval").body
        kwargs = {
            keyword.arg: ast.literal_eval(keyword.value)
            for keyword in node.keywords
            if keyword.arg
        }
        for index, argument in enumerate(node.args):
            if index >= len(_POSITIONAL_PARAMETERS):
                return "malformed", {}
            kwargs[_POSITIONAL_PARAMETERS[index]] = ast.literal_eval(argument)
        return "executable", kwargs
    except Exception:  # noqa: BLE001
        return "malformed", {}


def test_every_executable_instruction_the_server_serves_actually_executes():
    """The instrument that finally closed the class.

    Four rounds chased spellings — ``<kind>``, ``<that kind>``,
    ``<one of them>``, an invalid ``category='node'`` — because each pin
    described what a broken instruction LOOKED like. This one runs them.

    A call is classified, never silently dropped: a template is skipped and
    counted, an executable call must succeed, and anything that is neither
    FAILS. Dropping the leftovers into an ``except`` is how a malformed
    instruction would sit in a green build.
    """
    calls = set()
    unbalanced = []
    for text in _served_strings():
        for harvested in _harvest_calls(text):
            if harvested is None:
                unbalanced.append(text.strip()[:130])
            else:
                calls.add(harvested)
    assert unbalanced == [], unbalanced
    assert calls, "the harvester found nothing — the pin is vacuous"

    executed, templates, malformed, failures = 0, 0, [], []
    for call in sorted(calls):
        kind, kwargs = _classify_call(call)
        if kind in ("template", "mention"):
            templates += 1
            continue
        if kind == "malformed":
            malformed.append(call)
            continue
        result = meta_tools.get_schema_template_action(**kwargs)
        executed += 1
        if not result.get("_success"):
            failures.append((call, result.get("error_code")))

    assert malformed == [], malformed
    assert executed >= 10, f"only {executed} instructions executed — pin is weak"
    assert failures == [], failures


#: The citation boundary, written down so a pin can enforce it.
#:
#: A served string MAY name a document as PROVENANCE ("recorded in ADR-001 §6")
#: — attribution, and a reader who cannot fetch it has lost nothing they were
#: promised. It may NOT INSTRUCT the reader to go and read one ("See
#: AUTHORING_WORKFLOW_V1.md §11"), because no MCP tool can fetch it.
#: Matched on WORD BOUNDARIES ALONE — no separator class at all.
#:
#: Two narrower versions leaked in turn. A space-suffixed literal list missed
#: ``Read: docs/design.md``; replacing it with ``[\s:]`` then still missed
#: ``See—docs/design.md``, ``See(docs/x.md)`` and ``consult,docs/x.md``. Every
#: attempt to enumerate the separators that can follow a verb was another list
#: of spellings, which is the defect this guard exists to catch — so the
#: separator is not enumerated. A trailing word boundary already prevents
#: ``seeded`` from matching ``see``, and it accepts end-of-string too.
#:
#: The guard errs toward catching: a sentence containing a fetch verb AND an
#: unfetchable target is reported even when the verb was incidental. A false
#: positive costs one reworded sentence; a false negative ships an instruction
#: nobody can follow.
_FETCH_IMPERATIVE_VERBS = ("see", "consult", "read", "fetch", "refer to")
_FETCH_IMPERATIVE = re.compile(
    r"\b(?:" + "|".join(v.replace(" ", r"\s+") for v in _FETCH_IMPERATIVE_VERBS) + r")\b",
    re.IGNORECASE,
)


def _has_fetch_imperative(text):
    return bool(_FETCH_IMPERATIVE.search(text))

#: An unfetchable TARGET: a repository path, a documentation/source filename, or
#: a bare well-known document name.
#:
#: Widened after a measured escape table showed 18 of 23 shapes getting through:
#: ``/mnt/...`` sandbox paths, ``.py``/``.rst``/``.toml`` files, a bare
#: ``README``, a lowercase ``.md`` with no ``docs/`` prefix, and an uppercase
#: ``.MD`` (the pattern was case-sensitive). Enumerating extensions is still an
#: enumeration — but it is an enumeration of TARGETS, which is closed and
#: inspectable, rather than of the verbs that might precede them, which is not.
#:
#: ``\b`` is applied ONLY to alternatives beginning with a word character: in
#: front of the whole group it silently disabled the ``.codex/`` alternative,
#: because a standalone dot is a non-word character and the boundary then
#: demanded a word character before it.
#:
#: ``docs/Atomsphere/...`` is deliberately NOT matched: that is a Boomi
#: documentation page key, and ``read_boomi_doc_page`` fetches it. A guard that
#: flagged a reachable destination would teach the wrong lesson.
_UNFETCHABLE_DOCUMENT = re.compile(
    r"(?:"
    r"\b(?:ADR-\d+|README)\b"
    r"|\b(?:docs|src|tests|agents|examples)/(?!Atomsphere)[\w/.-]+"
    r"|\.codex/[\w/.-]+"
    r"|/mnt/[\w/.-]+"
    r"|\b[\w.-]+\.(?:md|rst|py|toml|cfg|ini)\b"
    r")",
    re.IGNORECASE,
)


def test_no_served_string_instructs_the_caller_to_read_an_unfetchable_document():
    """Provenance is allowed; an unfollowable instruction is not.

    Each served STRING is examined on its own. Serializing the whole payload and
    splitting on sentence punctuation merged neighbouring fields — JSON puts
    ``", "`` between them, not a sentence break — so an imperative in one field
    and a permitted citation in another were read as one sentence and reported
    as an offence that did not exist.
    """
    offenders = []
    for text in _served_strings():
        # NOT on a colon: "Read the details here: docs/design.md" is one
        # instruction, and splitting it put the verb in one fragment and the
        # unfetchable target in the next, so neither carried both.
        for sentence in re.split(r"(?<=[.;])\s+", text):
            if not _has_fetch_imperative(sentence):
                continue
            for match in _UNFETCHABLE_DOCUMENT.findall(sentence):
                offenders.append((match, sentence.strip()[:130]))
    assert offenders == [], offenders


def test_the_citation_guard_detects_the_paths_it_was_written_for():
    """The guard's own sentinel.

    A regex guard that matches nothing passes every payload. These are the two
    shapes the rule exists to stop, plus the provenance form it must permit.
    """
    caught = "See .codex/plans/issue-141-live-captures.md for the evidence."
    also = "Consult AUTHORING_WORKFLOW_V1.md §11 before authoring."
    allowed = "The projection decision is recorded in ADR-001 §6."

    # Every separator shape that leaked in a previous round, pinned so the next
    # narrowing of this pattern fails here rather than in production text.
    separators = (
        "Read: docs/design.md",
        "See—docs/design.md",
        "See(docs/design.md)",
        "consult,docs/design.md",
        "Refer to docs/design.md",
    )
    for text in (caught, also, *separators):
        assert _UNFETCHABLE_DOCUMENT.findall(text), text
        assert _has_fetch_imperative(text), text
    # Provenance carries no imperative, so the pair-test above lets it through.
    assert not _has_fetch_imperative(allowed)


def test_the_instruction_sweep_reaches_the_contract_entries_themselves():
    """The strings this amendment exists to publish must be IN the sweep.

    ``process_ir_authoring`` deliberately serves zero entries for a bare
    selector, so a sweep that only fetched selectors never read a single
    remediation — the exact class four rounds of findings were about. The sweep
    pages the filtered surface instead.
    """
    strings = list(_served_strings())
    assert len(strings) > 15000, len(strings)
    entry_remediations = [
        text
        for text in strings
        if "category='placement'" in text and "list bound" in text
    ]
    assert entry_remediations, "the sweep does not reach contract-entry remediations"


@pytest.mark.parametrize(
    "text,expected",
    [
        # Well-formed: the interior is returned and will be executed.
        ("get_schema_template(resource_type='component')", ["resource_type='component'"]),
        # Nested parens must not terminate the scan early.
        ("get_schema_template(config=f(1))", ["config=f(1)"]),
        # TRUNCATED: no closing paren at all. A regex over `[^()]*` matched
        # nothing here, so the broken instruction was invisible; balancing
        # reports it.
        ("get_schema_template(resource_type='component'", [None]),
        # A stray paren AFTER a complete call is prose noise, not a broken
        # instruction: the call itself is well formed and a caller pasting it
        # succeeds, so the interior is returned rather than flagged.
        ("get_schema_template(resource_type='component'))", ["resource_type='component'"]),
    ],
)
def test_the_call_harvester_sees_malformed_outer_syntax(text, expected):
    assert _harvest_calls(text) == expected


def test_a_colon_linked_instruction_is_still_one_instruction():
    """`Read the details here: docs/design.md` must not be split in two.

    Splitting on the colon put the verb in one fragment and the unfetchable
    target in the next, so neither carried both and the guard reported nothing.
    """
    text = "Read the details here: docs/design.md before proceeding"
    fragments = re.split(r"(?<=[.;])\s+", text)
    caught = any(
        _has_fetch_imperative(fragment) and _UNFETCHABLE_DOCUMENT.findall(fragment)
        for fragment in fragments
    )
    assert caught


def test_the_escape_shapes_that_leaked_are_all_matched_now():
    """The measured escape table, pinned.

    Two rows decide the design. ``Grounded in .codex/plans/x.md`` is bug #459's
    own wording minus the word "see"; ``The states are at X.md §11`` is the exact
    grammar an earlier fix adopted. Both carry no imperative, so an
    imperative-gated guard can never see them — which is why the surfaces this
    amendment owns are held to the stricter rule below instead.
    """
    targets = (
        "/mnt/examples/04_environment_setup/manage_roles.py",
        "docs/design.md",
        ".codex/plans/issue-141-live-captures.md",
        "AUTHORING_WORKFLOW_V1.md",
        "README",
        "guide.rst",
        "pyproject.toml",
        "SETUP.MD",
        "notes.md",
        "ADR-001",
    )
    for text in targets:
        assert _UNFETCHABLE_DOCUMENT.findall(text), text

    # A Boomi documentation page key IS fetchable, and must not be flagged.
    assert not _UNFETCHABLE_DOCUMENT.findall("docs/Atomsphere/Integration/Process")


def _amendment_owned_strings():
    """Only the surfaces this amendment introduced or rewrote.

    The strict rule below is deliberately NOT repo-wide. A blanket ban on
    file-shaped tokens would flag ~55 pre-existing example-script labels in
    other tools' descriptions and one legitimately fetchable Boomi doc key —
    that is a separate cleanup, not this amendment's, and pretending otherwise
    would either fail the build on unrelated prose or force a scope creep
    nobody asked for.
    """
    surfaces = [
        meta_tools.get_schema_template_action(schema_name="ProcessIRV1"),
        meta_tools.get_schema_template_action(schema_name="process_ir_authoring"),
    ]
    facets = surfaces[1]["contract_page"]["facets"]
    for category in facets["categories"]:
        cursor = None
        while True:
            page = meta_tools.get_schema_template_action(
                schema_name="process_ir_authoring",
                category=category,
                after_entry_id=cursor,
                limit=50,
            )["contract_page"]
            surfaces.append(page)
            if not page["truncated"]:
                break
            cursor = page["next_after_entry_id"]

    def walk(value):
        if isinstance(value, str):
            yield value
        elif isinstance(value, dict):
            for item in value.values():
                yield from walk(item)
        elif isinstance(value, (list, tuple)):
            for item in value:
                yield from walk(item)

    for surface in surfaces:
        yield from walk(surface)


def test_no_surface_this_amendment_owns_names_a_repository_artifact_at_all():
    """The STRICT rule, where this work is responsible for every word.

    No imperative required. A repository path or document filename in a served
    string is unreachable however the sentence is phrased, so the shapes that
    escape an imperative-gated guard — "Grounded in X", "the rules are at X" —
    are caught here by the target alone.

    ``ADR-001`` is exempted as bare PROVENANCE: it carries no path and no
    extension, nothing is promised to the reader, and the alternative is
    rewriting 34 pre-existing attributions across selectors this amendment does
    not own.
    """
    bare_provenance = re.compile(r"\bADR-\d+\b")
    offenders = []
    for text in _amendment_owned_strings():
        for match in _UNFETCHABLE_DOCUMENT.findall(text):
            if bare_provenance.fullmatch(match):
                continue
            offenders.append((match, text.strip()[:120]))
    assert offenders == [], offenders

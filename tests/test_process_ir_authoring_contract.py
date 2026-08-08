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
import asyncio  # noqa: E402
import inspect  # noqa: E402
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


#: Values the wrapper's own Args block documents for one selector axis.
#:
#: Parsed from the SERVED description rather than restated, so the sweep covers
#: what the tool advertises. A vocabulary copied into the test is one more
#: second copy — the defect this whole contract exists to prevent.
_VOCABULARY_LINE = re.compile(r"^\s*(\w+):[^:]*:\s*(.+)$")


def _documented_vocabulary(axis):
    for line in (server.get_schema_template.__doc__ or "").splitlines():
        match = _VOCABULARY_LINE.match(line)
        if not match or match.group(1) != axis:
            continue
        values = []
        for token in match.group(2).split(","):
            token = token.strip().rstrip(".")
            # `etc.` and prose tails are not values.
            if token and re.fullmatch(r"[\w.\-]+", token) and token != "etc":
                values.append(token)
        return tuple(values)
    return ()


def _probe_vocabulary(key, **kwargs):
    """The ``valid_*`` list the runtime reports for one probed combination."""
    payload = meta_tools.get_schema_template_action(**kwargs)
    return tuple(payload.get(key) or ())


def _component_types():
    overview = meta_tools.get_schema_template_action(resource_type="component")
    return tuple(overview.get("component_types") or ())


def _specialized_surfaces():
    """Every VALID specialization combination, derived by probing the runtime.

    Walks the axes together rather than independently: component_type first
    (because it decides which protocols exist), then the protocol set the
    runtime reports for that pair, then the standards for trading partners.
    """
    surfaces = []

    for resource_type in meta_tools._VALID_RESOURCE_TYPES:
        for operation in (None, "create"):
            base = {"resource_type": resource_type, "operation": operation}

            # resource-level protocols (e.g. process/create + a process_kind)
            for protocol in _probe_vocabulary(
                "valid_protocols", **base, protocol="__not_a_protocol__"
            ):
                surfaces.append(
                    meta_tools.get_schema_template_action(**base, protocol=protocol)
                )

            for standard in _probe_vocabulary(
                "valid_standards", **base, standard="__not_a_standard__"
            ):
                surfaces.append(
                    meta_tools.get_schema_template_action(**base, standard=standard)
                )

            if resource_type != "component":
                continue
            for component_type in _component_types():
                pair = dict(base, component_type=component_type)
                surfaces.append(meta_tools.get_schema_template_action(**pair))
                # ...and the protocols THIS component_type admits.
                for protocol in _probe_vocabulary(
                    "valid_protocols", **pair, protocol="__not_a_protocol__"
                ):
                    surfaces.append(
                        meta_tools.get_schema_template_action(**pair, protocol=protocol)
                    )

    return surfaces


def _valid_operations(resource_type):
    """The operations a resource_type REALLY accepts, asked of the runtime.

    The wrapper's Args block lists twelve and omits `plan`, `apply`, `verify`,
    `safe_edit` and `deploy` — so deriving the sweep from the docstring left
    whole success paths unvisited while the test still passed, because those
    tokens appear in the `valid_operations` of the ERROR payload. Asking the
    tool what it accepts is the one source that cannot be behind.
    """
    probe = meta_tools.get_schema_template_action(
        resource_type=resource_type, operation="__not_an_operation__"
    )
    reported = tuple(probe.get("valid_operations") or ())
    # Union with the documented list: a type whose error payload omits the key
    # still gets the general vocabulary rather than nothing.
    return tuple(sorted(set(reported) | set(_documented_vocabulary("operation"))))


def _registered_tool_descriptions():
    """The description of every tool FastMCP actually serves."""
    loop = asyncio.new_event_loop()
    try:
        tools = loop.run_until_complete(server.mcp.list_tools())
    finally:
        loop.close()
    return [tool.description for tool in tools if tool.description]


def _paginate_contract(category):
    """Every page of one contract category, to a TERMINAL page.

    ONE implementation, used by both sweeps. It existed twice, and the copy
    silently lost the cursor guards — so a pagination regression would have hung
    CI in one sweep while the other reported it. A rule restated in a second
    place drifts; this one is stated once.
    """
    pages = []
    cursor = None
    seen_cursors = set()
    while True:
        page = meta_tools.get_schema_template_action(
            schema_name="process_ir_authoring",
            category=category,
            after_entry_id=cursor,
            limit=50,
        )["contract_page"]
        pages.append(page)
        if not page["truncated"]:
            return pages
        cursor = page["next_after_entry_id"]
        assert cursor, f"{category}: truncated page carried no cursor"
        assert cursor not in seen_cursors, f"{category}: cursor did not advance"
        seen_cursors.add(cursor)


def _served_surfaces():
    """Every response payload that serves instructions.

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
    # Every resource_type x operation, plus the specialization axes.
    #
    # A hand-picked five-operation subset missed integration `plan`, component
    # `safe_edit`, package `deploy` and the whole monitoring family — real
    # payloads with their own instructions. The vocabularies are DERIVED from
    # the wrapper's own Args block, so a new operation is swept the day it is
    # documented rather than the day someone remembers to extend a tuple.
    #
    # Over-enumerating is harmless and useful: an unsupported combination
    # returns an ERROR payload, which is served text worth sweeping too.
    for resource_type in meta_tools._VALID_RESOURCE_TYPES:
        for operation in (None,) + _valid_operations(resource_type):
            try:
                surfaces.append(
                    meta_tools.get_schema_template_action(
                        resource_type=resource_type, operation=operation
                    )
                )
            except Exception:  # noqa: BLE001
                continue

    # Specializations, COMBINED and derived.
    #
    # Two rounds were lost here to axes exercised one at a time. `standard='x12'`
    # alone returns the 607-byte overview, not the 1290-byte create template;
    # and a protocol only reaches its real payload alongside the component_type
    # that admits it — `component/create + connector-settings +
    # database.sqlserver` is 15 KB and was never swept, `process/create +
    # database_to_api_sync` is 27 KB and was never swept.
    #
    # The vocabularies are asked of the RUNTIME, which reports `valid_protocols`
    # / `valid_standards` for the exact combination being probed. A documented
    # list cannot do this: the wrapper's `protocol` line names only the
    # trading-partner values, and the real set depends on the component_type.
    surfaces.extend(_specialized_surfaces())

    # The ERROR envelopes. A rejection is served text too, and its suggestion is
    # the instruction a caller is most likely to follow — they are stuck.
    for bad in (
        {},
        {"schema_name": "no_such_selector"},
        {"resource_type": "no_such_type"},
        {"schema_name": "process_ir_authoring", "node_kind": "no_such_kind"},
        {"schema_name": "process_ir_authoring", "limit": 999},
        {"schema_name": "process_ir_authoring", "after_entry_id": "node.branch"},
        {"schema_name": "ProcessIRV1", "node_kind": "branch"},
        {"schema_name": "process_ir_authoring@99"},
    ):
        try:
            surfaces.append(meta_tools.get_schema_template_action(**bad))
        except Exception:  # noqa: BLE001
            continue

    # Every plan_integration_design mode, including its refusals.
    for kwargs in (
        {},
        {"authoring_mode": "process_ir"},
        {"archetype": "database_to_api_sync"},
        {"authoring_mode": "not_a_mode"},
        {"archetype": "database_to_api_sync", "authoring_mode": "process_ir"},
    ):
        try:
            surfaces.append(meta_tools.plan_integration_design_action(**kwargs))
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
        surfaces.extend(_paginate_contract(category))

    surfaces.append(meta_tools.list_capabilities_action())

    # Every REGISTERED tool's description. This is the surface an LLM reads
    # before it reads anything else, and it was 0% covered: a poisoned tool
    # description — a broken call, a bare `.md`, a `docs/architecture/` path —
    # left the whole suite green.
    #
    # Read off the FastMCP registry, NOT off ``vars(server)``. The module
    # namespace also holds imported types (``Enum``, ``Any``, ``Path``) and
    # private decorators whose docstrings FastMCP never serves; failing CI on
    # one of those would be a false alarm about text no caller can see.
    surfaces.extend(_registered_tool_descriptions())

    return surfaces


def _served_strings():
    """Every served string. One list of surfaces, two walkers over it."""
    for _path, text in _served_strings_with_paths():
        yield text


#: ``get_schema_template``'s positional order, DERIVED from the signature.
#:
#: It was hand-written first, and it was wrong within one round: six entries
#: against a real thirteen, so a legal seven-positional call
#: (``authoring_entry_id`` binds at position seven) was reported malformed and a
#: regression test locked the stale copy in. That is precisely the defect this
#: whole amendment exists to prevent — a second copy of a fact that drifts from
#: the one the code enforces — reproduced inside the guard written to catch it.
#:
#: Derived, it cannot drift: adding a parameter extends it automatically.
_POSITIONAL_PARAMETERS = tuple(
    inspect.signature(meta_tools.get_schema_template_action).parameters
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
        # A parameter bound twice — positionally and by keyword, or by the same
        # keyword twice — raises TypeError in a REAL call. Normalizing it into a
        # dict silently picked a winner and let the broken example execute, so
        # the guard passed exactly the instruction a caller could not run.
        bound = []
        for index, argument in enumerate(node.args):
            if index >= len(_POSITIONAL_PARAMETERS):
                return "malformed", {}
            bound.append((_POSITIONAL_PARAMETERS[index], ast.literal_eval(argument)))
        for keyword in node.keywords:
            if not keyword.arg:
                return "malformed", {}
            bound.append((keyword.arg, ast.literal_eval(keyword.value)))
        names = [name for name, _ in bound]
        if len(names) != len(set(names)):
            return "malformed", {}
        return "executable", dict(bound)
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
            continue
        # SUCCEEDING is not enough. An instruction that names a specialization
        # must return the specialized payload: two repaired `see_also` links
        # executed cleanly and returned the generic component overview, because
        # they omitted `operation='create'` — the "resolves but does not
        # deliver" shape, one layer down from the citation guard.
        rendered = json.dumps(result, default=str)
        for axis in ("protocol", "component_type", "standard"):
            value = kwargs.get(axis)
            if value and value not in rendered:
                failures.append((call, f"{axis}={value!r} not reflected in the payload"))

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

#: The repository's own architecture documents, DERIVED from disk rather than
#: listed. A bare ``AUTHORING_WORKFLOW_V1`` (the ``.md`` dropped) escaped an
#: extension-based pattern, and enumerating the names by hand would be the same
#: second-copy defect this contract exists to prevent.
_REPO_DOC_NAMES = tuple(
    sorted(
        path.stem
        for path in (
            Path(__file__).resolve().parent.parent / "docs" / "architecture"
        ).glob("*.md")
    )
)

#: An unfetchable TARGET: a repository path, a documentation/source filename, or
#: a bare well-known document name.
#:
#: Widened twice against measured escape tables. Enumerating extensions is still
#: an enumeration — but it is an enumeration of TARGETS, which is closed and
#: inspectable, rather than of the verbs that might precede them, which is not.
#:
#: ``json`` and ``xml`` are deliberately ABSENT: ``profile.json`` is a real
#: profile_type value on this surface, and flagging legitimate domain data would
#: make the guard something people switch off.
#:
#: ``\b`` is applied ONLY to alternatives beginning with a word character: in
#: front of the whole group it silently disabled the ``.codex/`` alternative,
#: because a standalone dot is a non-word character and the boundary then
#: demanded a word character before it.
#:
#: ``docs/Atomsphere/...`` is deliberately NOT matched: that is a Boomi
#: documentation page key, and ``read_boomi_doc_page`` fetches it. A guard that
#: flagged a reachable destination would teach the wrong lesson.
#: Alternatives, ORDERED. Alternation is leftmost-wins, and that ordering is
#: load-bearing here: ``ADR-\d+`` matches the *prefix* of
#: ``ADR-001-process-ir-authority``, so with the bare id first the full document
#: name was reduced to ``ADR-001`` and then discarded by the provenance
#: exemption — the extensionless case the derived names exist to catch escaped
#: through the exemption meant for something else. Derived names go first, and
#: ``(?!-)`` on the bare id is the belt to that braces.
#:
#: Bare document names are matched case-SENSITIVELY; only the extension
#: alternative ignores case (so ``SETUP.MD`` is caught). Matching bare names
#: case-insensitively flagged the ordinary word "license" in a warning about
#: connection licences, and a guard that cries wolf on prose is one people
#: switch off.
_UNFETCHABLE_ALTERNATIVES = (
    ([r"\b(?:" + "|".join(re.escape(name) for name in _REPO_DOC_NAMES) + r")\b"]
     if _REPO_DOC_NAMES else [])
    + [
        # A GENERIC ADR slug, not just the stems on disk. ``(?!-)`` on the bare
        # id was meant as belt-and-braces and instead cut a hole: an unlisted
        # slug (`ADR-002-new-policy`, a typo, a future document) matched neither
        # the derived names nor the bare id, so it disappeared from BOTH guards.
        # Match the slug; the exact bare form is exempted afterwards.
        r"\bADR-\d+(?:-[\w-]+)?\b",
        r"\b(?:README|CHANGELOG|LICENSE|Makefile)\b",
        # `docs/Atomsphere/...` is EXCLUDED: it is a Boomi documentation page
        # key and `read_boomi_doc_page` fetches it. Flagging a reachable
        # destination would teach the wrong lesson.
        r"\b(?:docs|src|tests|agents|examples|scripts|boomi_mcp)/(?!Atomsphere)[\w/.-]+",
        # `\b` is applied only to alternatives beginning with a word character;
        # in front of the whole group it silently disabled this one, because a
        # standalone dot is a non-word character.
        r"\.(?:codex|github)/[\w/.-]+",
        r"/mnt/[\w/.-]+",
        # `json` and `xml` are deliberately absent: `profile.json` is a real
        # profile_type value on this surface.
        r"(?i:\b[\w.-]+\.(?:md|rst|py|toml|cfg|ini|yaml|yml|sh|sql|js|ts|html)\b)",
    ]
)

_UNFETCHABLE_DOCUMENT = re.compile("(?:" + "|".join(_UNFETCHABLE_ALTERNATIVES) + ")")


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


#: Keys whose values are PROVENANCE LABELS or fetchable references, not
#: instructions — the only exemptions from the strict rule below.
#:
#: Measured, not guessed. Running the strict rule over every served surface
#: yields offenders on exactly one: ``list_capabilities``, where 55 upstream SDK
#: example-script names sit under ``sdk_examples_covered`` (a coverage ledger,
#: not somewhere a caller is sent) and one ``map_component.md`` sits under
#: ``read_boomi_doc_page.examples`` — a Boomi doc key that tool fetches.
#:
#: An earlier version exempted 20 of 22 SURFACES on the theory that those
#: labels were spread across them. They are not; they are two keys. Exempting by
#: key buys back everything that scoping gave up — including
#: ``AuthoringRevisionBindingV1`` and ``cache_property_authoring``, the very
#: surfaces two earlier bugs were found on.
_ARTIFACT_EXEMPT_KEYS = ("sdk_examples_covered", "read_boomi_doc_page.examples")


def _served_strings_with_paths():
    """``(dotted path, string)`` for every served string, for key exemptions."""

    def walk(value, path):
        if isinstance(value, str):
            yield path, value
        elif isinstance(value, dict):
            for key, item in value.items():
                yield from walk(item, f"{path}.{key}" if path else str(key))
        elif isinstance(value, (list, tuple)):
            for item in value:
                yield from walk(item, f"{path}[]")

    for index, surface in enumerate(_served_surfaces()):
        yield from walk(surface, "")


def test_no_served_string_names_a_repository_artifact_at_all():
    """The STRICT rule: no imperative required, over EVERY served surface.

    Shapes like "Grounded in X" and "the rules are at X" carry no fetch verb, so
    an imperative-gated guard can never see them. The target alone is the
    offence: a repository path or document filename is unreachable however the
    sentence is phrased.

    Scoped by KEY, not by surface. The previous version read 2 of 22 changed
    surfaces on the theory that pre-existing labels were spread across them;
    measurement showed they are two keys on one surface, and the 20 skipped
    surfaces included the two that earlier bugs were found on.

    ``ADR-001`` stays exempt as bare PROVENANCE: no path, no extension, nothing
    promised to the reader.
    """
    bare_provenance = re.compile(r"\bADR-\d+\b")
    offenders = []
    for path, text in _served_strings_with_paths():
        if any(key in path for key in _ARTIFACT_EXEMPT_KEYS):
            continue
        for match in _UNFETCHABLE_DOCUMENT.findall(text):
            if bare_provenance.fullmatch(match):
                continue
            offenders.append((path, match, text.strip()[:110]))
    assert offenders == [], offenders[:12]



def test_the_sweep_reads_registered_tools_not_every_module_global():
    """`vars(server)` is not the served surface.

    It also holds imported types (`Enum`, `Any`, `Path`) and private decorators
    whose docstrings FastMCP never serves. Failing CI on one of those would be a
    false alarm about text no caller can see — and passing because of one would
    be worse.
    """
    descriptions = _registered_tool_descriptions()
    assert 20 <= len(descriptions) <= 200, len(descriptions)
    swept = set(_served_strings())
    for description in descriptions:
        assert description in swept
    # The module namespace holds far more callables than the registry serves.
    module_callables = [
        name
        for name, value in vars(server).items()
        if callable(value) and getattr(value, "__doc__", None)
    ]
    assert len(module_callables) > len(descriptions)


@pytest.mark.parametrize(
    "call",
    [
        # positional + keyword for the SAME parameter
        "'component', resource_type='process'",
        # the same keyword twice
        "resource_type='component', resource_type='process'",
        # more positionals than the signature has (13 parameters, so 14 values)
        ", ".join(f"'v{index}'" for index in range(14)),
    ],
)
def test_a_duplicate_or_overlong_binding_is_malformed_not_normalized(call):
    """A real invocation raises TypeError; the guard must not paper over it.

    Collapsing the binding into a dict silently picked a winner and executed a
    call the caller could never make, so the guard passed exactly the broken
    instruction it exists to catch.
    """
    assert _classify_call(call)[0] == "malformed"


def test_the_pagination_helper_is_shared_by_both_sweeps():
    """One statement of the cursor rule, not two.

    It was written twice, and the copy lost the cursor-presence and advancement
    guards — so a pagination regression would have hung CI in one sweep while
    the other reported it. Asserting both sweeps route through the same helper
    is what keeps the guards from drifting apart again.
    """
    import inspect

    # There is ONE surface builder, and both rules walk it. (The pagination
    # TESTS above page independently on purpose — an independent implementation
    # is what makes them a check on the helper rather than a restatement of it.)
    source = inspect.getsource(_served_surfaces)
    assert "_paginate_contract" in source
    assert "after_entry_id=cursor" not in source, "the sweep re-implemented paging"

    # ...and the one implementation carries both guards.
    helper = inspect.getsource(_paginate_contract)
    assert "truncated page carried no cursor" in helper
    assert "cursor did not advance" in helper


def test_the_positional_table_is_derived_from_the_real_signature():
    """A hand-written copy of a signature is the defect this contract exists to
    prevent — and the guard reproduced it.

    Six entries were written against a real thirteen, so a legal call binding
    ``authoring_entry_id`` at position seven was reported malformed, and a
    regression test locked the stale copy in.
    """
    real = tuple(inspect.signature(meta_tools.get_schema_template_action).parameters)
    assert _POSITIONAL_PARAMETERS == real
    assert len(real) == 13
    assert real[6] == "authoring_entry_id"

    # ...and the call that the stale table rejected is executable again.
    kind, kwargs = _classify_call(
        "None, None, None, None, None, 'process_ir_authoring', 'node.branch'"
    )
    assert kind == "executable"
    assert kwargs["schema_name"] == "process_ir_authoring"
    assert kwargs["authoring_entry_id"] == "node.branch"
    assert meta_tools.get_schema_template_action(**kwargs)["_success"] is True


def test_the_strict_rule_reaches_the_surfaces_earlier_bugs_were_found_on():
    """Scoping by SURFACE skipped 20 of 22; scoping by KEY skips two keys.

    The previous version read only `ProcessIRV1` and `process_ir_authoring` on
    the theory that pre-existing example labels were spread across the other
    surfaces. Measurement showed they are two keys on one surface — and the
    skipped 20 included `AuthoringRevisionBindingV1` and
    `cache_property_authoring`, the surfaces bugs #461 and #460 were found on.
    """
    # Check the SURFACES, not the path strings: a selector name appears as a
    # payload VALUE (`schema_name: "cache_property_authoring"`), not as a key.
    served = {
        surface.get("schema_name")
        for surface in _served_surfaces()
        if isinstance(surface, dict)
    }
    for selector in (
        "AuthoringRevisionBindingV1",
        "cache_property_authoring",
        "authoring_workflow",
        "ProcessIRV1",
        "process_ir_authoring",
    ):
        assert selector in served, selector

    # ...and the strict rule really walks their strings.
    paths = {path for path, _ in _served_strings_with_paths()}
    assert any("AuthoringRevisionBindingV1" in path for path in paths)


def test_the_artifact_exemption_is_two_keys_not_a_surface():
    """A narrow exemption is auditable; a broad one hides what it excuses."""
    assert _ARTIFACT_EXEMPT_KEYS == (
        "sdk_examples_covered",
        "read_boomi_doc_page.examples",
    )
    exempted = [
        path
        for path, _ in _served_strings_with_paths()
        if any(key in path for key in _ARTIFACT_EXEMPT_KEYS)
    ]
    assert exempted, "the exemption matches nothing — it is dead or misspelled"
    # It must not swallow a whole surface.
    assert all("sdk_examples_covered" in p or "read_boomi_doc_page" in p for p in exempted)


@pytest.mark.parametrize(
    "text,flagged",
    [
        ("The table is at AUTHORING_WORKFLOW_V1.md §11.", True),
        # the same citation with the extension dropped
        ("Consult AUTHORING_WORKFLOW_V1 §11.", True),
        # no imperative at all — only the strict rule can see these
        ("Grounded in .codex/plans/issue-141-live-captures.md.", True),
        ("lives in boomi_mcp/models/process_ir.py", True),
        ("see scripts/build.sh", True),
        ("config.yaml", True),
        ("SETUP.MD", True),
        ("README", True),
        # ...and the shapes that must NOT be flagged
        ("two connections each burn a separate connection license", False),
        ("docs/Atomsphere/Integration/Process", False),
        ("a profile_type of profile.json", False),
        ("recorded in ADR-001 §6", False),
    ],
)
def test_the_measured_escape_table(text, flagged):
    """Every shape a QA round measured, in one table.

    ``ADR-001`` and ``profile.json`` are the two that must stay quiet: the first
    is provenance, the second is real domain data on this very surface.
    """
    bare_provenance = re.compile(r"\bADR-\d+\b")
    matches = [
        match
        for match in _UNFETCHABLE_DOCUMENT.findall(text)
        if not bare_provenance.fullmatch(match)
    ]
    assert bool(matches) is flagged, (text, matches)


def test_the_repo_document_names_are_derived_from_disk():
    """A hand-listed set of doc names is another copy that drifts."""
    assert _REPO_DOC_NAMES
    assert "AUTHORING_WORKFLOW_V1" in _REPO_DOC_NAMES
    assert "PROCESS_IR_V1" in _REPO_DOC_NAMES


def test_a_full_document_name_is_not_reduced_to_an_exempt_adr_id():
    """Alternation order is load-bearing.

    ``ADR-\\d+`` matches the prefix of ``ADR-001-process-ir-authority``, so with
    the bare id first the full document name was reduced to ``ADR-001`` and then
    thrown away by the provenance exemption — the extensionless case the derived
    names exist to catch escaped through the exemption meant for something else.
    """
    bare = re.compile(r"\bADR-\d+\b")
    full = "Grounded in ADR-001-process-ir-authority."
    matches = [m for m in _UNFETCHABLE_DOCUMENT.findall(full) if not bare.fullmatch(m)]
    assert matches == ["ADR-001-process-ir-authority"]

    # ...while the bare provenance form is still exempt.
    provenance = "recorded in ADR-001 §6"
    assert [m for m in _UNFETCHABLE_DOCUMENT.findall(provenance) if not bare.fullmatch(m)] == []


def test_the_selector_vocabularies_are_derived_from_the_served_description():
    """A vocabulary copied into the test is one more second copy.

    A hand-picked five-operation subset missed integration `plan`, component
    `safe_edit`, package `deploy` and the whole monitoring family.
    """
    operations = _documented_vocabulary("operation")
    assert len(operations) >= 10
    for expected in ("create", "execution_records", "compare_versions", "events"):
        assert expected in operations
    assert "etc" not in _documented_vocabulary("component_type")
    assert _documented_vocabulary("standard")
    assert _documented_vocabulary("protocol")
    assert _documented_vocabulary("not_an_axis") == ()


def test_the_sweep_reaches_a_SUCCESSFUL_surface_for_every_real_operation():
    """Searching serialized text for an operation name proves nothing.

    The name also appears in the ``valid_operations`` list of the ERROR payload,
    so the previous assertion passed while `integration/plan`,
    `component/safe_edit` and `package/deploy` were never successfully invoked —
    a false green about coverage, in the coverage test.
    """
    checked = 0
    for resource_type in meta_tools._VALID_RESOURCE_TYPES:
        for operation in _valid_operations(resource_type):
            payload = meta_tools.get_schema_template_action(
                resource_type=resource_type, operation=operation
            )
            if not payload.get("_success"):
                # Not every documented operation applies to every type; what
                # matters is that the ones the RUNTIME reports for this type do.
                probe = meta_tools.get_schema_template_action(
                    resource_type=resource_type, operation="__not_an_operation__"
                )
                if operation in (probe.get("valid_operations") or ()):
                    raise AssertionError(
                        f"{resource_type}/{operation} is advertised but does not serve"
                    )
                continue
            checked += 1
    assert checked >= 25, checked

    # ...and the three that a documented-only vocabulary missed are among them.
    for resource_type, operation in (
        ("integration", "plan"),
        ("component", "safe_edit"),
        ("package", "deploy"),
    ):
        assert operation in _valid_operations(resource_type), (resource_type, operation)


def test_an_unlisted_adr_slug_does_not_vanish_between_the_two_guards():
    """`(?!-)` was belt-and-braces and instead cut a hole.

    An ADR slug that is not a current file stem — a typo, a future document —
    matched neither the derived names nor the bare id, so it disappeared from
    the strict rule AND from the imperative rule that used to catch it.
    """
    bare = re.compile(r"\bADR-\d+\b")
    for text, expected in (
        ("See ADR-002-new-policy for the rule.", "ADR-002-new-policy"),
        ("Grounded in ADR-001-process-ir-authority.", "ADR-001-process-ir-authority"),
    ):
        matches = [m for m in _UNFETCHABLE_DOCUMENT.findall(text) if not bare.fullmatch(m)]
        assert matches == [expected], (text, matches)

    # The bare provenance form stays exempt.
    provenance = "recorded in ADR-001 §6"
    assert [
        m for m in _UNFETCHABLE_DOCUMENT.findall(provenance) if not bare.fullmatch(m)
    ] == []


def test_the_operation_vocabulary_comes_from_the_runtime_not_the_docstring():
    """The Args block omits five operations the tool actually accepts.

    Deriving the sweep from the served description therefore left whole success
    paths unvisited — and the docstring is itself a served claim that is behind
    the code.
    """
    documented = set(_documented_vocabulary("operation"))
    for missing in ("plan", "apply", "verify", "safe_edit", "deploy"):
        assert missing not in documented, f"{missing} is documented now — update this pin"
    assert "plan" in _valid_operations("integration")
    assert "safe_edit" in _valid_operations("component")
    assert "deploy" in _valid_operations("package")


def test_a_specialization_is_swept_together_with_the_operation_that_consumes_it():
    """`standard='x12'` alone is the overview, not the create template."""
    overview = meta_tools.get_schema_template_action(
        resource_type="trading_partner", standard="x12"
    )
    specialized = meta_tools.get_schema_template_action(
        resource_type="trading_partner", operation="create", standard="x12"
    )
    assert json.dumps(overview, default=str) != json.dumps(specialized, default=str)
    rendered = json.dumps(_served_surfaces(), default=str)
    assert json.dumps(specialized, default=str)[:200] in rendered


@pytest.mark.parametrize(
    "kwargs",
    [
        # A protocol only reaches its real payload alongside the component_type
        # that admits it — 15 KB, and absent while the axes were swept apart.
        {
            "resource_type": "component",
            "operation": "create",
            "component_type": "connector-settings",
            "protocol": "database.sqlserver",
        },
        # 27 KB, likewise absent.
        {
            "resource_type": "process",
            "operation": "create",
            "protocol": "database_to_api_sync",
        },
        # The specialization that needs its operation.
        {"resource_type": "trading_partner", "operation": "create", "standard": "x12"},
    ],
)
def test_a_specialized_template_is_actually_inside_the_sweep(kwargs):
    """Exercising each axis alone swept the overview, never the specialization."""
    payload = meta_tools.get_schema_template_action(**kwargs)
    assert payload["_success"] is True
    rendered = json.dumps(_served_surfaces(), default=str)
    assert json.dumps(payload, default=str)[:200] in rendered, kwargs


def test_the_specialization_vocabularies_are_asked_of_the_runtime():
    """A documented list cannot express a vocabulary that depends on context.

    The wrapper's `protocol` line names only the trading-partner values, while
    the real set depends on the component_type — `connector-settings` admits
    `database.sqlserver`, and `process` admits `database_to_api_sync`.
    """
    documented = set(_documented_vocabulary("protocol"))
    component_protocols = set(
        _probe_vocabulary(
            "valid_protocols",
            resource_type="component",
            operation="create",
            component_type="connector-settings",
            protocol="__not_a_protocol__",
        )
    )
    process_protocols = set(
        _probe_vocabulary(
            "valid_protocols",
            resource_type="process",
            operation="create",
            protocol="__not_a_protocol__",
        )
    )
    assert component_protocols and process_protocols
    assert not (component_protocols & documented)
    assert not (process_protocols & documented)
    assert _component_types()

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
    AUTHORING_LAYERS,
    build_process_ir_authoring_entries,
    reset_process_ir_authoring_cache,
)
from boomi_mcp.categories import integration_builder, meta_tools  # noqa: E402
from boomi_mcp.models.process_ir_authoring import (  # noqa: E402
    PROCESS_IR_AUTHORING_BYTE_BUDGET,
    PROCESS_IR_AUTHORING_DEFAULT_LIMIT,
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
    # #153 (M12.15) settled the owner decision this line used to call pending:
    # direct ProcessIR apply IS supported, and the capability says so.
    assert states["authoring.typed_apply.process_materialization"] == "supported"


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


def test_direct_planning_now_reaches_apply_and_publishes_no_refusal():
    """#153 (M12.15) inverts this: the sequence no longer stops at compile.

    The materialization capability is supported, so it must not appear in a list
    named ``process_ir_capability_gaps`` — a supported capability advertised
    among gaps is a contradiction a caller has to resolve by guessing.
    """
    payload = meta_tools.plan_integration_design_action(authoring_mode="process_ir")
    actions = [step["action"] for step in payload["typed_next_steps"]]
    assert "apply" in actions
    assert actions[-1] == "apply"
    assert [
        gap
        for gap in payload["process_ir_capability_gaps"]
        if gap["capability_id"] == "authoring.typed_apply.process_materialization"
    ] == []


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


def _to_units(request):
    """Reshape a legacy singular ``process_ir`` intent into #153's ``units``.

    Applied to the fixture payloads rather than rewriting each nested literal by
    hand: the wire change is `component_key` + `process_ir` -> one unit pairing
    an envelope with a root, and expressing that once keeps every fixture below
    exercising the SAME reshape the production normalizer performs.

    ``name``/``action`` are supplied because #153 makes them required with no
    default — a fixture that omitted them would be testing a shape the contract
    refuses.
    """
    intent = request["authoring_request"]["intent"]
    if "process_ir" in intent and "units" not in intent:
        envelope = {
            "component_key": intent.pop("component_key"),
            "name": "Contract Fixture Process",
            "action": "create",
        }
        intent["units"] = [
            {"envelope": envelope, "process_ir": intent.pop("process_ir")}
        ]
    request["authoring_request"]["contract_version"] = "2"
    return request


def _process_ir_request(*steps):
    return _to_units({
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
    })


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
        # The RETRY BOUND — one of the two cases the plan named that were only
        # ever tested at the internal layer. The body is otherwise VALID, so
        # this isolates the bound: a first draft used the wrong field names and
        # raised four codes, of which the intended one was merely present.
        (
            {
                "kind": "try_catch",
                "scope": "connector",
                "try_body": {
                    "steps": [{"kind": "message", "text": "m"}],
                    "terminal": {"kind": "stop"},
                },
                "catch_body": {
                    "steps": [{"kind": "message", "text": "c"}],
                    "terminal": {"kind": "stop"},
                },
                "retry": {"count": -1},
            },
            "PROCESS_IR_SCHEMA_RETRY_COUNT",
        ),
    ],
)
def test_a_malformed_process_ir_reports_its_own_code_and_pointer(bad_step, expected_code):
    payload = integration_builder._plan_authoring(
        None, "p", _process_ir_request(SOURCE, bad_step)
    )
    assert payload["_success"] is False
    assert payload["error_code"] == "INVALID_INPUT"
    # The EXACT set, not membership. A first version of the two cases added
    # here asserted `in` and passed on an incidental code while testing nothing
    # the plan named — the same loose-check defect this contract keeps finding
    # elsewhere.
    codes = {row["code"] for row in payload["validation_errors"]}
    assert codes == {expected_code}, sorted(codes)
    for row in payload["validation_errors"]:
        assert row["path"].startswith("/intent/units/0/process_ir")


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
    request = _to_units({
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
    })
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
    return _to_units({
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
    })


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
    assert diagnostics[0]["path"].startswith("/intent/units/0/process_ir")


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


def test_direct_process_ir_next_steps_are_internally_consistent_about_apply():
    """The response must not contradict itself about apply — in EITHER direction.

    The original form of this test guarded one direction: a response may not
    declare apply unsupported and then coach the caller toward it. #153 makes
    apply supported, so the same consistency property is asserted the other way
    round — the sequence offers apply, and no step still describes it as refused.
    A response that advertised the capability while telling the caller it was
    refused would be exactly as incoherent as the case this originally caught.
    """
    payload = meta_tools.plan_integration_design_action(authoring_mode="process_ir")
    steps = payload["typed_next_steps"]

    assert any(step["action"] == "apply" for step in steps)
    for step in steps:
        why = step.get("why") or ""
        assert "apply is refused" not in why, step["action"]
        assert "is plan/compile-only" not in why, step["action"]


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


def _live_archetypes():
    """Archetype names from the live registry, not a literal."""
    from boomi_mcp.authoring.contract import list_archetype_registry

    try:
        return tuple(entry["name"] for entry in list_archetype_registry())
    except Exception:  # noqa: BLE001 — a registry that cannot build sweeps nothing
        return ()


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


def _registered_tools():
    """Every tool FastMCP serves, as tool objects."""
    loop = asyncio.new_event_loop()
    try:
        return loop.run_until_complete(server.mcp.list_tools())
    finally:
        loop.close()


def _registered_tool_descriptions():
    """The description of every tool FastMCP actually serves."""
    return [tool.description for tool in _registered_tools() if tool.description]


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
        # WORKFLOW_SEQUENCE_NOT_FOUND — the one code the tuple missed. A planted
        # `See docs/architecture/...` in its message survived the whole suite.
        {"schema_name": "workflow:__not_a_workflow__"},
        {"schema_name": "archetype:__not_an_archetype__"},
        {"schema_name": "recipe:__not_a_recipe__"},
    ):
        try:
            surfaces.append(meta_tools.get_schema_template_action(**bad))
        except Exception:  # noqa: BLE001
            continue

    # Every plan_integration_design mode, including its refusals.
    planner_calls = [
        {},
        {"authoring_mode": "process_ir"},
        {"authoring_mode": "not_a_mode"},
        {"archetype": "__not_an_archetype__"},
    ]
    # EVERY archetype, from the live registry. Naming one visited 1 of 6 and
    # left 119 served strings unswept — a hardcoded subset inside the very
    # function whose comment argues against hardcoded subsets.
    for entry in _live_archetypes():
        planner_calls.append({"archetype": entry})
        planner_calls.append({"archetype": entry, "authoring_mode": "process_ir"})
    for kwargs in planner_calls:
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
        # SUCCEEDING is not enough, and neither is a substring.
        #
        # An instruction that names a specialization must return the SPECIALIZED
        # payload. Two repaired `see_also` links executed cleanly and returned
        # the generic component overview because they omitted
        # `operation='create'`. The first attempt to catch that searched the
        # serialized response for the value — and passed anyway for
        # `trading_partner, standard='x12'`, because the OVERVIEW enumerates
        # `x12` in its list of standards. Membership somewhere in the payload is
        # not identity.
        #
        # So the comparison is against the response's own IDENTITY fields: a
        # specialized template echoes the selector it was built for, an overview
        # does not.
        for axis in ("protocol", "component_type", "standard"):
            value = kwargs.get(axis)
            if value and result.get(axis) != value:
                failures.append(
                    (call, f"{axis}={value!r} did not select the specialized template")
                )

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
        # `json` and `xml` are absent because `profile.json` is a real
        # profile_type VALUE on this surface.
        #
        # A dotted parameter address (`target.write_profile.sql`) is handled by
        # POSITION, not by shape. Requiring a single-segment basename did
        # separate it — and silently stopped catching `settings.prod.yaml`,
        # `foo.test.py` and `.eslintrc.js`, trading one false positive for a
        # class of false negatives. `X.Y.ext` is genuinely ambiguous in
        # isolation; where the string SITS is not.
        r"(?i:[\w.-]*[\w-]\.(?:md|rst|py|toml|cfg|ini|yaml|yml|sh|sql|js|ts|html)\b)",
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
_ARTIFACT_EXEMPT_KEYS = (
    "sdk_examples_covered",
    "read_boomi_doc_page.examples",
    # A planner decision's `field` is a parameter ADDRESS by contract
    # (`target.write_profile.sql`), never a document reference. Exempting the
    # key keeps the pattern free to catch dotted filenames like
    # `settings.prod.yaml` everywhere else.
    "required_user_decisions[].field",
)


def _is_artifact_exempt(path):
    """True when ``path``'s LEAF is an exempted key.

    Exact, or bounded by a ``.`` path separator — never a bare suffix.

    Two narrower-than-stated versions leaked in turn. A SUBSTRING test accepted
    a subtree prefix as if it were a key (``tools.read_boomi_doc_page`` matched
    9 of 28,160 paths, slipping under both caps while excusing a whole served
    tool). A bare SUFFIX test then exempted any leaf whose name merely ends with
    an exempt name — ``tools.not_sdk_examples_covered`` is a different field,
    and anything placed under it would have bypassed the sweep.

    The boundary is what makes "names the leaf it excuses" true rather than
    approximately true.
    """
    return any(_matches_exempt_key(path, key) for key in _ARTIFACT_EXEMPT_KEYS)


def _matches_exempt_key(path, key):
    """Does ``path``'s leaf match THIS key, exactly or on a ``.`` boundary?

    Per-key on purpose. The audit below previously asked "is this path exempt at
    all, and does the key appear in it" — two different questions whose
    conjunction is satisfied by a DEAD key that is merely a substring of a live
    one (`examples_cover` inside `sdk_examples_covered`). One predicate, asked
    per key, is what makes the dead-exemption check mean what it says.
    """
    normalized = path[:-2] if path.endswith("[]") else path
    return normalized == key or normalized.endswith("." + key)


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
        if _is_artifact_exempt(path):
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


def test_the_artifact_exemption_stays_a_handful_of_keys_not_a_surface():
    """A narrow exemption is auditable; a broad one hides what it excuses.

    Asserts the PROPERTY rather than a frozen tuple: every exemption must be a
    key path (so it cannot quietly become a surface), every one must actually
    match something (so a misspelling is not silently dead), and together they
    must stay a rounding error against the corpus.
    """
    assert len(_ARTIFACT_EXEMPT_KEYS) <= 4, _ARTIFACT_EXEMPT_KEYS

    all_paths = [path for path, _ in _served_strings_with_paths()]
    for key in _ARTIFACT_EXEMPT_KEYS:
        matched = [path for path in all_paths if _matches_exempt_key(path, key)]
        assert matched, f"exemption {key!r} matches nothing — dead or misspelled"

    exempted = [path for path in all_paths if _is_artifact_exempt(path)]
    # A rounding error, not a carve-out.
    assert len(exempted) / len(all_paths) < 0.01, (len(exempted), len(all_paths))

    # A subtree PREFIX must not qualify, whatever the caps say. This is the
    # invariant the caps alone could not express.
    subtree_prefixes = {
        path.rsplit(".", 1)[0]
        for path in exempted
        if "." in path
    }
    for prefix in subtree_prefixes:
        assert not _is_artifact_exempt(prefix + ".some_other_leaf"), prefix


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


def test_a_specialized_instruction_must_select_the_specialized_template():
    """Membership somewhere in the payload is not identity.

    `get_schema_template(resource_type='trading_partner', standard='x12')`
    returns the 607-byte OVERVIEW, which enumerates `x12` in its list of
    standards — so a substring check passed while a served B2B workflow step
    handed callers the catalog instead of the X12 create template it promised.
    """
    overview = meta_tools.get_schema_template_action(
        resource_type="trading_partner", standard="x12"
    )
    specialized = meta_tools.get_schema_template_action(
        resource_type="trading_partner", operation="create", standard="x12"
    )
    # The trap: the value IS present in the wrong payload...
    assert "x12" in json.dumps(overview, default=str)
    # ...but only the specialized one claims it as its identity.
    assert overview.get("standard") != "x12"
    assert specialized.get("standard") == "x12"


@pytest.mark.parametrize(
    "text,flagged",
    [
        # `X.Y.ext` is ambiguous in ISOLATION, so the pattern flags it and the
        # planner's parameter address is exempted by its key instead. These
        # dotted filenames must keep matching.
        ("settings.prod.yaml", True),
        ("foo.test.py", True),
        ("target.write_profile.sql", True),
        ("a profile_type of profile.json", False),
        # ...while real filenames still match.
        ("manage_roles.py", True),
        ("decision_step.md", True),
        ("q.sql", True),
        ("b.ts", True),
        ("config.yaml", True),
        ("SETUP.MD", True),
        ("/mnt/examples/manage_roles.py", True),
        ("boomi_mcp/models/process_ir.py", True),
    ],
)
def test_a_dotted_domain_identifier_is_not_a_filename(text, flagged):
    assert bool(_UNFETCHABLE_DOCUMENT.findall(text)) is flagged, text


def test_the_sweep_visits_every_archetype_and_the_sixth_error_code():
    """Two hardcoded subsets survived inside the function arguing against them.

    The planner loop named one archetype of six (119 strings unswept), and the
    error tuple reached five codes of six — a planted citation in
    `WORKFLOW_SEQUENCE_NOT_FOUND` survived the entire suite.
    """
    archetypes = _live_archetypes()
    assert len(archetypes) >= 4, archetypes

    rendered = json.dumps(_served_surfaces(), default=str)
    for archetype in archetypes:
        assert archetype in rendered, archetype

    # The sixth code is now reachable.
    payload = meta_tools.get_schema_template_action(
        schema_name="workflow:__not_a_workflow__"
    )
    assert payload["error_code"] == "WORKFLOW_SEQUENCE_NOT_FOUND"
    assert json.dumps(payload, default=str)[:120] in rendered


def test_a_subtree_prefix_cannot_masquerade_as_an_exempted_key():
    """The invariant the caps could not express.

    `tools.read_boomi_doc_page` matches only 9 of 28,160 paths, so a substring
    test let it pass both the count and percentage caps while silently excusing
    an entire served tool. An exemption names the LEAF it excuses.
    """
    assert _is_artifact_exempt("tools.read_boomi_doc_page.examples[]")
    # a DESCENDANT of an exempt subtree
    assert not _is_artifact_exempt("tools.read_boomi_doc_page.description")
    assert not _is_artifact_exempt("tools.read_boomi_doc_page.notes[]")
    # a DIFFERENT LEAF whose name merely ends with an exempt name
    assert not _is_artifact_exempt("tools.not_sdk_examples_covered[]")
    assert not _is_artifact_exempt("tools.my_sdk_examples_covered[]")
    assert not _is_artifact_exempt("other_user_decisions[].field")
    # ...and the real ones still qualify
    assert _is_artifact_exempt("tools.x.sdk_examples_covered[]")
    assert _is_artifact_exempt("required_user_decisions[].field")

    # Every real exemption is still a leaf match.
    exempted = [
        path for path, _ in _served_strings_with_paths() if _is_artifact_exempt(path)
    ]
    assert exempted
    for path in exempted:
        normalized = path[:-2] if path.endswith("[]") else path
        assert any(normalized.endswith(key) for key in _ARTIFACT_EXEMPT_KEYS), path


def test_the_dead_exemption_check_asks_about_the_key_it_is_checking():
    """A dead key must not look alive because a LIVE key covers the same path.

    `examples_cover` matches nothing on its own, but it is a substring of
    `sdk_examples_covered` — so an audit that asked "is this path exempt at all,
    and does the key appear in it" accepted 54 paths for a key that matches
    zero. Two different questions, conjoined, answered a third.
    """
    paths = [path for path, _ in _served_strings_with_paths()]
    assert not any(_matches_exempt_key(path, "examples_cover") for path in paths)
    assert not any(_matches_exempt_key(path, "field") for path in paths if "[]." not in path)
    for key in _ARTIFACT_EXEMPT_KEYS:
        assert any(_matches_exempt_key(path, key) for path in paths), key


# ---------------------------------------------------------------------------
# §6 architect-vs-plan review findings
# ---------------------------------------------------------------------------


def _provenance_fixture():
    """The minimal comparison inputs: a build with one fingerprinted component.

    Shared so the revision tests exercise ONE setup — the absent-field and
    partial-binding cases are variations on the same comparison, and two
    hand-rolled copies would let them drift apart.
    """
    from boomi_mcp.authoring.contract import get_authoring_revisions

    return (
        {"live_component_fingerprints": {"c": {"digest": "d"}}},
        {"c": "d"},
        get_authoring_revisions(),
    )


def test_verify_compares_the_compiler_revision_and_names_what_moved():
    """The §11 limit this amendment CLAIMED to close, actually closed.

    `compare_live_build_provenance` compared only `capability_revision`, so a
    server whose compiler BEHAVIOUR had changed — a placement rule, a replay
    classification, a remediation — reported `match` against a binding minted
    before the change. The document said the limit was closed. That is the
    false-claim defect this whole amendment exists to remove, written into the
    record of the amendment itself.
    """
    from boomi_mcp.authoring.workflow import compare_live_build_provenance

    base, observed, revisions = _provenance_fixture()
    stale = "sha256:" + "0" * 64

    matching = compare_live_build_provenance(
        dict(base, revision_binding=dict(revisions)), observed
    )
    assert matching.revision_skew == "match"
    assert matching.revision_mismatches == ()

    for field in ("capability_revision", "compiler_revision"):
        moved = compare_live_build_provenance(
            dict(base, revision_binding=dict(revisions, **{field: stale})), observed
        )
        assert moved.revision_skew == "mismatch", field
        assert moved.revision_mismatches == (field,), field

    both = compare_live_build_provenance(
        dict(
            base,
            revision_binding=dict(
                revisions,
                capability_revision=stale,
                compiler_revision="sha256:" + "1" * 64,
            ),
        ),
        observed,
    )
    assert both.revision_mismatches == ("capability_revision", "compiler_revision")

    # An ABSENT field is `unknown`, never `match` — the only case the fix
    # actually changed, and the one the test did not cover. A binding minted
    # before a field existed was never fully compared, so claiming `match`
    # would report a comparison that did not happen.
    for omitted in ("capability_revision", "compiler_revision"):
        partial = {k: v for k, v in revisions.items() if k != omitted}
        result = compare_live_build_provenance(
            dict(base, revision_binding=partial), observed
        )
        assert result.revision_skew == "unknown", omitted
        assert result.revision_mismatches == (), omitted


def test_the_meta_tool_catalog_advertises_every_wrapper_parameter():
    """The catalog is what a client ENUMERATES.

    A parameter documented only in the wrapper docstring is one nobody
    discovers: the seven filters, `authoring_mode`, and `list_capabilities`'
    own `expected_capability_revision` were all absent while the signatures
    already accepted them.
    """
    import inspect

    catalog = meta_tools.list_capabilities_action()["tools"]

    # EVERY registered tool, not the two this amendment happened to touch. The
    # guard was named "…advertises every wrapper parameter" while comparing 2 of
    # 48 — and four other tools had a parameter documented only in a docstring,
    # the exact condition the name declares unacceptable.
    # TOTAL over the CATALOG, which is the thing being audited. Two universes
    # were wrong before this: `checked >= 40` let five tools silently drop out,
    # and `registered <= set(catalog)` fixed that but audited the registration
    # instead — and an env-gated tool is never registered in a test process, so
    # two of `search_boomi_gotchas`' three parameters could vanish from its
    # catalog row and stay green. Every catalog row must be audited against the
    # wrapper's real signature, whether or not this process could register it.
    registered = {tool.name for tool in _registered_tools()}
    from_source, unreadable = _annotated_tool_schemas_from_source()
    assert unreadable == [], unreadable

    unaudited = sorted(
        name
        for name in catalog
        if getattr(server, name, None) is None and name not in from_source
    )
    assert unaudited == [], unaudited
    assert registered <= set(catalog), sorted(registered - set(catalog))

    gaps = {}
    for name in sorted(catalog):
        wrapper = getattr(server, name, None)
        accepted = (
            set(inspect.signature(wrapper).parameters)
            if wrapper is not None
            else set(from_source[name])
        )
        advertised = set(catalog[name].get("parameters") or {})
        missing = accepted - advertised
        if missing:
            gaps[name] = sorted(missing)
    assert gaps == {}, gaps

    assert "expected_capability_revision" in catalog["list_capabilities"]["parameters"]
    assert "process_ir_authoring" in catalog["get_schema_template"]["parameters"]["schema_name"]


def test_the_doctrine_filter_value_callers_depend_on_still_selects_them():
    """A filter VALUE is a contract. Redefining one is a breaking change.

    Projecting each pattern's own taxonomy as `category` looked like fidelity
    and silently deleted `category='doctrine'` — the only call that selected
    these 39 entries — while moving `category='reliability'` from 2 entries to 9
    by mixing patterns in with the node it used to return. There is no
    `entry_type` filter, so nothing else selected them.
    """
    page = fetch(category="doctrine", limit=50)["contract_page"]
    assert page["matched_entry_count"] == len(
        [e for e in build_process_ir_authoring_entries() if e.entry_type == "doctrine"]
    )
    for entry in page["entries"]:
        assert entry["entry_type"] == "doctrine"

    # ...and a pre-existing value still means what it meant.
    reliability = fetch(category="reliability", limit=50)["contract_page"]
    assert {e["entry_type"] for e in reliability["entries"]} == {"node", "semantic_rule"}

    # The pattern's own taxonomy is still published — as metadata, where it
    # moves the revision without redefining a filter value.
    doctrine = next(
        e for e in build_process_ir_authoring_entries() if e.entry_type == "doctrine"
    )
    assert any(f.startswith("Doctrine category:") for f in doctrine.ordering_facts)


def test_the_contract_never_serves_a_verbatim_copy_of_doctrine_prose():
    """The entry's own summary says it never copies the prose.

    `mutual_exclusion` holds sentences, not pattern names, so joining them
    served the same words under two selectors — under a label that promised a
    name list.
    """
    from boomi_mcp.kb.design_doctrine import DESIGN_DOCTRINE_ENTRIES

    served = json.dumps(
        [
            e.model_dump(mode="json")
            for e in build_process_ir_authoring_entries()
            if e.entry_type == "doctrine"
        ]
    )
    checked = 0
    for row in DESIGN_DOCTRINE_ENTRIES.values():
        for sentence in row.get("mutual_exclusion") or ():
            assert sentence not in served, sentence[:70]
            checked += 1
    assert checked, "no mutual_exclusion prose exists — the pin is vacuous"
    # ...but a change to it still moves the contract.
    assert "Mutual-exclusion guidance: present (digest" in served


#: Parameters this amendment ADDED to the catalog, and the JSON-schema type the
#: registered wrapper generates for each. Advertising a type the wrapper rejects
#: is worse than not advertising at all: a client that follows the catalog is
#: rejected by schema validation before the action ever runs.
_AMENDMENT_CATALOG_PARAMS = {
    ("get_schema_template", "authoring_entry_id"): "string",
    ("get_schema_template", "node_kind"): "string",
    ("get_schema_template", "category"): "string",
    ("get_schema_template", "capability_id"): "string",
    ("get_schema_template", "workflow_stage"): "string",
    ("get_schema_template", "after_entry_id"): "string",
    ("get_schema_template", "limit"): "integer",
    ("plan_integration_design", "authoring_mode"): "string",
    ("list_capabilities", "expected_capability_revision"): "string",
    ("list_integration_archetypes", "expected_recipe_registry"): "object",
    ("get_integration_archetype", "recipe_version"): "string",
    ("build_from_archetype", "recipe_version"): "string",
    ("manage_process", "config"): "string",
}

_JSON_TYPE_WORD = {
    "string": "str",
    "integer": "int",
    "number": "float",
    "boolean": "bool",
    "array": "list",
    "object": "dict",
}


def test_the_catalog_advertises_the_type_the_wrapper_actually_accepts():
    """Presence was not enough; the TYPE has to match too.

    The fix that closed the catalog gaps described
    `expected_recipe_registry` as a JSON string while the wrapper declares an
    object — so a client following the newly-honest catalog would be rejected by
    FastMCP validation before the action's own normalization ever ran. An
    advertisement that cannot be followed is the same defect the gap was.
    """
    catalog = meta_tools.list_capabilities_action()["tools"]
    schemas = {tool.name: (getattr(tool, "parameters", None) or {}) for tool in _registered_tools()}

    checked = 0
    for (tool_name, param), expected_json_type in _AMENDMENT_CATALOG_PARAMS.items():
        properties = schemas[tool_name].get("properties", {})
        assert param in properties, (tool_name, param)
        spec = properties[param]
        declared = spec.get("type") or "|".join(
            option.get("type", "")
            for option in spec.get("anyOf", [])
            if option.get("type") != "null"
        )
        assert expected_json_type in declared, (tool_name, param, declared)

        text = catalog[tool_name]["parameters"][param]
        word = _JSON_TYPE_WORD[expected_json_type]
        assert text.lstrip().startswith(word), (tool_name, param, text[:40])
        checked += 1
    assert checked == len(_AMENDMENT_CATALOG_PARAMS)


def test_the_direct_planning_capability_gaps_carry_identity():
    """#481. The shape was entirely unpinned — a full revert stayed green.

    `["gated","gated","unsupported"]` named nothing and repeated itself, so a
    caller could not tell WHICH capability was gated, while
    `process_ir_capability_gaps` in the same payload carried ids.
    """
    payload = meta_tools.plan_integration_design_action(authoring_mode="process_ir")
    constructs = payload["supported_process_ir_constructs"]
    assert constructs

    # The old bare-string key must be gone, not merely supplemented.
    assert all("related_capability_states" not in row for row in constructs)

    with_gaps = [row for row in constructs if row["related_capability_gaps"]]
    assert with_gaps, "no construct links a non-supported capability"

    known = {e.contract_entry_id for e in build_process_ir_authoring_entries()}
    for row in constructs:
        gaps = row["related_capability_gaps"]
        ids = [gap["capability_id"] for gap in gaps]
        assert ids == sorted(ids), row["kind"]
        assert len(ids) == len(set(ids)), (row["kind"], ids)
        for gap in gaps:
            assert gap["state"] in ("gated", "unsupported"), gap
            assert gap["contract_entry_id"] in known, gap
            assert gap["contract_entry_id"].endswith(gap["capability_id"])


def _annotated_tool_schemas_from_source():
    """Parameter JSON types for every `@mcp.tool` wrapper, read from server.py.

    The registered schema is the better authority where it exists — it is
    literally what the caller receives. This is the fallback for wrappers an
    env gate keeps out of a test process, and it reads the same annotations
    FastMCP would have used, so it cannot drift into a second opinion.

    Returns ``(types, unreadable)``. An annotation this reader cannot map is
    REPORTED, never omitted: silently dropping one made the parameter invisible
    to the caller's totality check, which is how a wrong type behind an
    unrecognised annotation stayed green.
    """
    import ast

    source = ast.parse(
        (Path(__file__).resolve().parents[1] / "server.py").read_text()
    )
    by_annotation = {
        "str": "string",
        "int": "integer",
        "float": "number",
        "bool": "boolean",
        "dict": "object",
        "list": "array",
        "Any": None,  # deliberately untyped — accepts anything, gradeable by nobody
    }

    def json_types(node):
        """The SET of JSON types an annotation admits, or None for untyped.

        A set, not one string: `dict | str | None` is a real two-type union and
        is exactly what the registered schema expresses as `anyOf`. Collapsing
        it to a single answer forced the reader to give up on the repo's most
        common optional-argument idiom.
        """
        if isinstance(node, ast.Name):
            if node.id not in by_annotation:
                raise KeyError(node.id)
            mapped = by_annotation[node.id]
            return None if mapped is None else {mapped}
        if isinstance(node, ast.Constant):
            if node.value is None:  # the `None` arm of a union
                return set()
            if isinstance(node.value, str):  # a string annotation
                return json_types(ast.parse(node.value, mode="eval").body)
            raise KeyError(repr(node.value))
        if isinstance(node, ast.Subscript):
            head = node.value
            name = head.id if isinstance(head, ast.Name) else getattr(head, "attr", "")
            if name == "Optional":
                return json_types(node.slice)
            if name == "Union":
                parts = getattr(node.slice, "elts", [node.slice])
                merged = set()
                for part in parts:
                    got = json_types(part)
                    if got is None:
                        return None
                    merged |= got
                return merged
            if name in ("List", "Sequence", "Tuple"):
                return {"array"}
            if name in ("Dict", "Mapping"):
                return {"object"}
            return json_types(head)
        if isinstance(node, ast.BinOp) and isinstance(node.op, ast.BitOr):
            merged = set()
            for side in (node.left, node.right):
                got = json_types(side)
                if got is None:
                    return None
                merged |= got
            return merged
        raise KeyError(type(node).__name__)

    out, unreadable = {}, []
    for node in ast.walk(source):
        if not isinstance(node, (ast.FunctionDef, ast.AsyncFunctionDef)):
            continue
        if not any(
            isinstance(d, ast.Call)
            and isinstance(d.func, ast.Attribute)
            and d.func.attr == "tool"
            for d in node.decorator_list
        ):
            continue
        properties = {}
        arguments = node.args
        for argument in (
            list(arguments.posonlyargs) + list(arguments.args) + list(arguments.kwonlyargs)
        ):
            if argument.annotation is None:
                unreadable.append((node.name, argument.arg, "no annotation"))
                continue
            try:
                declared = json_types(argument.annotation)
            except KeyError as exc:
                unreadable.append((node.name, argument.arg, str(exc)))
                continue
            if declared is None:
                properties[argument.arg] = {}  # untyped: accepts anything
            elif len(declared) == 1:
                properties[argument.arg] = {"type": next(iter(declared))}
            else:
                properties[argument.arg] = {
                    "anyOf": [{"type": name} for name in sorted(declared)]
                }
        out[node.name] = properties
    return out, unreadable


#: Catalog parameters that CANNOT be type-graded, each with the reason.
#:
#: Pinned by name, and the guard fails if the real set differs in EITHER
#: direction. That is the opposite of the floor it replaces: `checked >= N`
#: could not distinguish "graded 179" from "graded 170 and silently skipped 9",
#: so nine rewrites that made parameters unclassifiable shipped green together.
#: An exemption list that must match exactly turns every new skip into a
#: failure a reviewer has to look at.
_UNGRADEABLE_CATALOG_PARAMETERS = {
    # `artifact: Any` generates `{"title": ...}` — an untyped schema accepts
    # anything, so no catalog claim about it can be contradicted.
    ("discover_db_schema", "artifact"),
    ("discover_openapi_spec", "artifact"),
    ("import_integration_draft", "artifact"),
    ("infer_profile_fields", "artifact"),
}


def test_every_catalog_parameter_type_agrees_with_its_wrapper_schema():
    """#482/#486/#491/#492. TOTAL, not opt-in: nothing is skipped in silence.

    Three defects had one shape — the guard classified what it recognised and
    ignored the rest, so any mutation that made a parameter unrecognisable was
    invisible. Pinning 13 of 170 let 157 lie; a leading-word match let
    "Optional dict (...)" through; an `int` on an env-gated tool was never
    reached at all. The fix is totality: every catalog parameter is graded or
    named in `_UNGRADEABLE_CATALOG_PARAMETERS`, and the two sets must agree
    exactly.

    The house style is a leading type word (`str`, `dict`, `int`, `list`), with
    `"JSON string (optional)"` and `"Optional dict (...)"` being that same style
    — which is why matching only the first word both misses real lies and flags
    correct entries.
    """
    catalog = meta_tools.list_capabilities_action()["tools"]
    schemas = {tool.name: (getattr(tool, "parameters", None) or {}) for tool in _registered_tools()}

    # Three catalog tools register only behind BOOMI_DOCS_ENABLED /
    # BOOMI_GOTCHAS_ENABLED, and their knowledge base is absent from a test
    # process — so a registration-only comparison could NEVER grade their nine
    # parameters, and an `int` -> `dict` lie on one shipped green. Their
    # wrappers are still declared in server.py, so those annotations are read
    # from the source rather than from a registration this process cannot make.
    from_source, unreadable = _annotated_tool_schemas_from_source()
    assert unreadable == [], unreadable
    for tool_name, properties in from_source.items():
        target = schemas.setdefault(tool_name, {}).setdefault("properties", {})
        for name, spec in properties.items():
            target.setdefault(name, spec)
    assert not (set(catalog) - set(schemas)), sorted(set(catalog) - set(schemas))

    # word -> the JSON types it may legitimately describe.
    accepted = {
        "str": {"string"},
        "JSON": {"string"},          # "JSON string ..." — a string carrying JSON
        "dict": {"object"},
        "int": {"integer"},
        "float": {"number"},
        "bool": {"boolean"},
        "list": {"array"},
    }
    modifiers = ("Optional", "optional", "Required", "required")

    mismatches, graded, ungradeable = [], set(), set()
    for tool_name, entry in catalog.items():
        properties = schemas.get(tool_name, {}).get("properties", {})
        for param, text in (entry.get("parameters") or {}).items():
            key = (tool_name, param)
            spec = properties.get(param)
            if not spec or not isinstance(text, str):
                ungradeable.add(key)
                continue
            declared = {spec["type"]} if spec.get("type") else {
                option.get("type")
                for option in spec.get("anyOf", [])
                if option.get("type") and option.get("type") != "null"
            }
            if not declared:
                ungradeable.add(key)   # `Any`: an untyped schema accepts anything
                continue
            word = next(
                (w for w in re.findall(r"[A-Za-z]+", text)[:3] if w not in modifiers),
                None,
            )
            if word is None or word not in accepted:
                # NOT skipped: a description with no type word cannot be graded,
                # so it must be declared. This is the escape that shipped nine
                # green rewrites.
                ungradeable.add(key)
                continue
            graded.add(key)
            if not (accepted[word] & declared):
                mismatches.append((tool_name, param, word, sorted(declared)))

    assert mismatches == [], mismatches
    assert ungradeable == _UNGRADEABLE_CATALOG_PARAMETERS, {
        "newly ungradeable": sorted(ungradeable - _UNGRADEABLE_CATALOG_PARAMETERS),
        "no longer ungradeable": sorted(_UNGRADEABLE_CATALOG_PARAMETERS - ungradeable),
    }
    # Totality, stated as an equation rather than a floor.
    total = sum(len(e.get("parameters") or {}) for e in catalog.values())
    assert len(graded) + len(ungradeable) == total, (len(graded), len(ungradeable), total)


def test_the_source_fallback_actually_reaches_the_env_gated_tools():
    """Guard the guard: the fallback exists only to close a real hole.

    If server.py's decorator or annotation style changes, the fallback would
    silently return nothing and #482's widening would quietly re-narrow to the
    tools this process happens to register.
    """
    from_source, unreadable = _annotated_tool_schemas_from_source()
    assert unreadable == [], unreadable
    assert len(from_source) >= 40, len(from_source)
    for gated in ("search_boomi_docs", "read_boomi_doc_page", "search_boomi_gotchas"):
        assert gated in from_source, gated
        assert from_source[gated], gated
    assert from_source["search_boomi_docs"]["top_k"] == {"type": "integer"}


def test_no_served_entry_has_an_empty_summary():
    """#483. Three diagnostics served a blank one-line orientation.

    Seven compiler specs carry an empty `message` — the whole statement lives in
    the remediation — and reading `message` alone published `summary: ""` on
    codes a failing compile really returns.
    """
    blank = [
        e.contract_entry_id
        for e in build_process_ir_authoring_entries()
        if not (e.summary or "").strip()
    ]
    assert blank == [], blank

    # Specifically: the codes whose authority supplies no short message still
    # get the sentence it DID write, not a placeholder and not silence.
    by_id = {e.contract_entry_id: e for e in build_process_ir_authoring_entries()}
    for entry_id in (
        "diagnostic.process_ir_semantic_join_unsupported",
        "diagnostic.process_ir_semantic_unterminated_path",
        "diagnostic.process_ir_compile_control_wiring_invalid",
    ):
        summary = by_id[entry_id].summary
        assert len(summary) > 30, (entry_id, summary)
        assert any(summary in fact for fact in by_id[entry_id].ordering_facts), entry_id


def test_a_partial_revision_binding_says_which_fields_were_not_compared():
    """#485. `mismatch` with a one-field list read as "the other one matched".

    A binding carrying one stale revision and no value at all for the other
    reported `revision_mismatches=("capability_revision",)` — indistinguishable
    from a binding where `compiler_revision` was compared and agreed.
    """
    from boomi_mcp.authoring.workflow import (
        _COMPARED_REVISIONS,
        compare_live_build_provenance,
    )

    base, observed, revisions = _provenance_fixture()
    stale = "sha256:" + "0" * 64

    for mismatched, absent in (
        ("capability_revision", "compiler_revision"),
        ("compiler_revision", "capability_revision"),
    ):
        binding = {k: v for k, v in revisions.items() if k != absent}
        binding[mismatched] = stale
        result = compare_live_build_provenance(
            dict(base, revision_binding=binding), observed
        )
        # A definite negative still wins the summary...
        assert result.revision_skew == "mismatch", (mismatched, absent)
        assert result.revision_mismatches == (mismatched,), (mismatched, absent)
        # ...but "never compared" is now stated, not inferred from a short list.
        assert result.revision_uncompared == (absent,), (mismatched, absent)

    # An EMPTY or absent binding compared NOTHING, so every field is
    # uncompared. Reporting `()` there made it indistinguishable — on this
    # field — from the fully-compared `match` below, which is the exact
    # confusion the field was added to end.
    for empty in ({}, None):
        result = compare_live_build_provenance(
            dict(base, revision_binding=empty), observed
        )
        assert result.revision_skew == "unknown", empty
        assert result.revision_uncompared == tuple(sorted(_COMPARED_REVISIONS)), empty

    # And a fully-present binding claims nothing was skipped.
    clean = compare_live_build_provenance(
        dict(base, revision_binding=dict(revisions)), observed
    )
    assert clean.revision_uncompared == ()
    assert set(_COMPARED_REVISIONS) == {"capability_revision", "compiler_revision"}


def test_every_planning_gap_state_matches_the_contract_entry_it_names():
    """#484/#489. The identity was pinned; the VALUE and the RELATION were not.

    Inverting `gated`<->`unsupported` on every gap-carrying construct left the
    suite green, and the payload then contradicted `process_ir_capability_gaps`
    in the same response. Rotating every gap list onto a different construct,
    and dropping 10 of 15 gaps, were green too: pinning each gap in isolation
    said nothing about WHICH construct it belongs to, which is the one thing
    the payload exists to say.
    """
    payload = meta_tools.plan_integration_design_action(authoring_mode="process_ir")
    entries = build_process_ir_authoring_entries()
    by_id = {e.contract_entry_id: e for e in entries}
    constructs = payload["supported_process_ir_constructs"]

    compared = 0
    for construct in constructs:
        for gap in construct["related_capability_gaps"]:
            entry = by_id[gap["contract_entry_id"]]
            assert gap["state"] == entry.canonical_state, (
                construct["kind"],
                gap,
                entry.canonical_state,
            )
            compared += 1
    assert compared >= 5, f"only {compared} gap states compared"

    # The RELATION, rebuilt independently from each node entry's own
    # `related_entry_ids` and compared WHOLE — so a rotation or a partial drop
    # is a set difference, not an invisible re-pairing.
    kinds = {c["kind"] for c in constructs}
    expected = {}
    for entry in entries:
        if entry.subject not in kinds or entry.entry_type != "node":
            continue
        blocking = sorted(
            related.split(".", 1)[1]
            for related in entry.related_entry_ids
            if related.startswith("capability.")
            and by_id[related].canonical_state in ("gated", "unsupported")
        )
        if blocking:
            expected[entry.subject] = blocking
    served = {
        c["kind"]: [g["capability_id"] for g in c["related_capability_gaps"]]
        for c in constructs
        if c["related_capability_gaps"]
    }
    assert served == expected, {"served": served, "expected": expected}
    assert sum(len(v) for v in served.values()) == compared

    # ...and the same capability never carries two states in one payload.
    elsewhere = {
        row["capability_id"]: row["state"]
        for row in payload.get("process_ir_capability_gaps", [])
    }
    for construct in constructs:
        for gap in construct["related_capability_gaps"]:
            other = elsewhere.get(gap["capability_id"])
            if other is not None:
                assert other == gap["state"], (gap, other)


def test_the_label_legend_is_published_once_per_page_not_per_entry():
    """#488. Repeating identical text on every entry cost a full page's tail.

    The figures move with the legend's wording and the diagnostic count — the
    ones first written down were already stale two rounds later — so this
    MEASURES the saving instead of quoting it, and fails if repeating the
    legend would once again be cheap enough not to matter.
    """
    from boomi_mcp.authoring.process_ir_projection import (
        DIAGNOSTIC_LABEL_LEGEND,
        query_process_ir_authoring_contract,
    )

    from boomi_mcp.models.process_ir_authoring import PROCESS_IR_AUTHORING_BYTE_BUDGET

    page = query_process_ir_authoring_contract(category="diagnostic", limit=50)
    assert page.diagnostic_label_legend == DIAGNOSTIC_LABEL_LEGEND
    assert page.returned_entry_count == 50, page.returned_entry_count

    # What per-entry repetition WOULD cost today, measured rather than quoted.
    diagnostics = [
        entry
        for entry in build_process_ir_authoring_entries()
        if entry.entry_type == "diagnostic"
    ]
    repeated = len(DIAGNOSTIC_LABEL_LEGEND) * len(diagnostics)
    assert repeated > PROCESS_IR_AUTHORING_BYTE_BUDGET // 10, (
        f"{repeated} bytes over {len(diagnostics)} diagnostics is under a tenth "
        f"of the {PROCESS_IR_AUTHORING_BYTE_BUDGET} byte entry budget — if "
        "repeating the legend is now that cheap, this guard has lost its point"
    )

    # The legend explains the labels, so it must name every one of them...
    for layer in AUTHORING_LAYERS:
        assert layer in DIAGNOSTIC_LABEL_LEGEND, layer
    # ...and no entry may carry its own copy again.
    for entry in page.entries:
        assert "bracketed label" not in (entry.summary or ""), entry.contract_entry_id


def test_the_served_layer_labels_are_the_published_spelling():
    """#494. Both sides of the two-way test derive from the same constant.

    That pins the WIRING and nothing else: renaming `"parser"` to `"parse"`
    moves the constant and the served label together and the test still passes.
    Published vocabulary needs one literal anchor, and this is it.
    """
    assert AUTHORING_LAYERS == ("parser", "semantic validator", "compiler")


def test_the_public_projection_constants_are_exported():
    """#493. Deleting all four names from `__all__` was green."""
    from boomi_mcp.authoring import process_ir_projection

    for name in (
        "AUTHORING_LAYERS",
        "AUTHORING_LAYER_PARSER",
        "AUTHORING_LAYER_SEMANTIC_VALIDATOR",
        "AUTHORING_LAYER_COMPILER",
        "DIAGNOSTIC_LABEL_LEGEND",
    ):
        assert name in process_ir_projection.__all__, name

    # ...and EVERY exported name resolves. Listing a name that does not exist
    # was green here while `from ... import *` raised AttributeError — the
    # guard only checked the five names it already knew about.
    unresolved = [
        name
        for name in process_ir_projection.__all__
        if not hasattr(process_ir_projection, name)
    ]
    assert unresolved == [], unresolved
    assert sorted(process_ir_projection.__all__) == list(process_ir_projection.__all__)


def test_exactly_the_expected_codes_fall_back_to_their_remediation():
    """#501. The docstring's own claim, measured rather than asserted in prose.

    An earlier version of it said "seven codes", counting every compiler spec
    with an empty `message`. Four of those seven are ALSO raised by the parse
    layer, which supplies one, so the merge gives them a real "what is wrong"
    and only three actually reach the fallback. A claim about behaviour that
    nothing measures is the defect class this whole contract exists to remove —
    including when it is my claim about my own code.
    """
    from boomi_mcp.authoring.process_ir_projection import collect_projection_sources

    sources = collect_projection_sources()

    # The docstring's OTHER two counts, measured. "Seven" and "four" were prose
    # for a round — in the same change that deleted that pattern from three
    # other places — so a legitimate-looking edit plus the fixture regeneration
    # the failing snapshot prescribes returned the suite to green while reality
    # was six and three.
    no_static_message = {
        str(spec["code"])
        for spec in sources.compiler_specs
        if not (spec.get("message") or "").strip()
    }
    assert len(no_static_message) == 7, sorted(no_static_message)

    parse_messages = {
        str(spec["code"]): (spec.get("message") or "").strip()
        for spec in sources.parse_specs
    }
    fell_back = {
        str(spec["code"])
        for spec in sources.compiler_specs
        if not (spec.get("message") or "").strip()
        and not parse_messages.get(str(spec["code"]), "")
    }
    assert fell_back == {
        "PROCESS_IR_COMPILE_CONTROL_WIRING_INVALID",
        "PROCESS_IR_SEMANTIC_JOIN_UNSUPPORTED",
        "PROCESS_IR_SEMANTIC_UNTERMINATED_PATH",
    }, sorted(fell_back)
    assert len(no_static_message - fell_back) == 4, sorted(no_static_message - fell_back)

    # ...and those three DO serve the remediation, byte for byte.
    by_id = {e.contract_entry_id: e for e in build_process_ir_authoring_entries()}
    remediation = {
        str(spec["code"]): (spec.get("remediation") or "").strip()
        for spec in sources.compiler_specs
    }
    for code in fell_back:
        entry = by_id["diagnostic." + code.lower()]
        assert entry.summary == remediation[code], code


def test_the_budget_scope_sentence_names_exactly_the_envelope_fields():
    """#504. Deriving the list removed the staleness but not the blind spot.

    The only guard on the served sentence was a substring pin on the clause
    BEFORE the list, so re-hardcoding `_page_envelope_fields()` to the exact
    stale triple #503 removed — naming `schema`, a field the page model has
    never had — shipped green, as did reordering the model's declarations.
    A caller reads this sentence to know what the byte budget excludes; it has
    to name the real envelope, in the real order.
    """
    from boomi_mcp.authoring.process_ir_projection import (
        build_process_ir_authoring_index,
    )
    from boomi_mcp.models.process_ir_authoring import (
        ProcessIRAuthoringContractPageV1,
    )

    sentence = build_process_ir_authoring_index()["retrieval"][
        "entry_byte_budget_scope"
    ]
    expected = [
        name
        for name in ProcessIRAuthoringContractPageV1.model_fields
        if name != "entries"
    ]
    assert "entries array only" in sentence
    assert "({0})".format(", ".join(expected)) in sentence, sentence

    # A LITERAL anchor, because both sides above derive from `model_fields` and
    # therefore pin the wiring, not the value: reordering the model's
    # declarations rewrites the served sentence and every derived check follows
    # it. Envelope order is published text a caller reads, so one place has to
    # state it outright — the same reason `AUTHORING_LAYERS` carries a literal
    # spelling pin next door.
    assert expected == [
        "contract_version",
        "state_mappings",
        "unlisted_placement_state",
        "unlisted_connector_action_state",
        "diagnostic_label_legend",
        "query",
        "catalog_entry_count",
        "matched_entry_count",
        "returned_entry_count",
        "limit",
        "truncated",
        "next_after_entry_id",
        "facets",
    ], expected

    # Every named field is real, and the entries array is never claimed excluded.
    for name in expected:
        assert name in sentence, name
    assert "entries)" not in sentence and ", entries" not in sentence


def test_every_page_a_caller_can_fetch_carries_the_page_rules():
    """#511. The fix was structural; the guard read ONE page off ONE selector.

    Predicate right, universe right, construction sites right — and still only
    one page instance ever asserted, the one that passes the legend explicitly.
    Three routes restored the original blank-legend payload green. Page rules
    are page rules on EVERY page a caller can fetch, so every selector is
    fetched here, populated and zero-match alike, and each rule is compared to
    its declared default rather than merely checked non-empty.
    """
    from boomi_mcp.authoring.process_ir_projection import (
        ProcessIRAuthoringQueryError,
        query_process_ir_authoring_contract,
    )
    from boomi_mcp.models.process_ir_authoring import (
        ProcessIRAuthoringContractPageV1,
    )

    # The graded set is NOT read from the shared constant. That constant feeds
    # three guards and was bounded only by `len(rules) >= 4` over exactly five
    # rules — one unit of slack — so widening it by `diagnostic_label_legend`
    # and blanking that rule shipped green in one edit. It is still used as a
    # cross-check below, but the set actually graded is derived from the
    # SERVED pages: a field every page agrees on is a rule, whatever any
    # constant says.
    query_dependent = _QUERY_DEPENDENT_PAGE_FIELDS
    rules = [
        name
        for name in ProcessIRAuthoringContractPageV1.model_fields
        if name not in query_dependent
    ]
    # EXACTLY these, pinned literally. `len(rules) >= 4` over five rules was one
    # unit of slack, and the constant it derives from feeds three guards: widening
    # it by one name dropped a rule out of `rules`, and making that same rule VARY
    # dropped it out of the derived `agreed` set too, so a two-line edit removed a
    # served rule from grading entirely. A literal set makes the first edit a
    # failure, and treating variation as a defect (below) makes the second one.
    assert set(rules) == {
        "contract_version",
        "diagnostic_label_legend",
        "state_mappings",
        "unlisted_connector_action_state",
        "unlisted_placement_state",
        "facets",
        "catalog_entry_count",
    }, sorted(rules)

    entries = build_process_ir_authoring_entries()
    categories = sorted({entry.category for entry in entries})
    assert len(categories) >= 5, categories

    # EVERY selector the tool exposes, derived from the signature rather than
    # listed: covering `category` and a hard-coded `node_kind` left five of the
    # seven unfetched, and blanking the legend on exactly those five shipped
    # green. The guard has to reach every page a caller can ask for, not every
    # page the guard's author happened to think of.
    selectors = set(
        inspect.signature(query_process_ir_authoring_contract).parameters
    ) - {"sources"}
    facets = query_process_ir_authoring_contract().facets.model_dump(mode="json")
    values = {
        "authoring_entry_id": entries[0].contract_entry_id,
        "node_kind": sorted(facets["node_kinds"])[0],
        "category": categories[0],
        "capability_id": sorted(
            e.capability_id for e in entries if e.capability_id
        )[0],
        "workflow_stage": sorted(facets["workflow_stages"])[0],
        "after_entry_id": entries[0].contract_entry_id,
        "limit": 2,
    }
    # `limit` spans 1..50 and every probe sat at 2 or the default 20, so a rule
    # conditioned on a HIGH limit — or on the byte-budget truncation branch,
    # which no fetch reached because every truncated page was cut by the entry
    # count first — was ungraded at both sites.
    extra_limits = list(range(1, PROCESS_IR_AUTHORING_MAX_LIMIT + 1))
    assert set(values) == selectors, sorted(set(values) ^ selectors)

    # bare, each selector alone, every category, and a zero-match variant of
    # each category — the zero-match population is load-bearing, not padding:
    # a legend served only when entries were selected is caught by nothing else.
    # `after_entry_id` resumes a FILTERED result and the query layer documents a
    # companion requirement, so a bare cursor is not a page any caller can
    # fetch. Pairing it keeps the selector covered without pretending an
    # unreachable page exists.
    # `limit` needs one for a different reason: a bare fetch returns NO entries
    # by design (`bare_retrieval_returns_entries: False`), so `limit` alone
    # takes the empty-result early return and never reaches the populated
    # construction site — which is exactly where a `limit`-conditioned blank
    # legend hid.
    companions = {
        "after_entry_id": {"category": categories[0]},
        "limit": {"category": categories[0]},
    }
    # AUGMENT, never replace. Routing `limit` to its companion form removed the
    # only selector that can VARY an early-return page — that site's reachable
    # space is exactly `{}` and `{limit: N}` — so it was graded by one invariant
    # page and a `limit`-conditioned blank went green there. Both forms are
    # fetched now; a bare form that is genuinely unreachable lands in
    # `rejected`, which is the honest record of it.
    # More than two filters, and the pair nothing else fetches: the universe was
    # a hand-built <=2-filter set, so a rule blanked only on a 3-filter page was
    # green.
    deep = [
        {
            "category": "diagnostic",
            "workflow_stage": values["workflow_stage"],
            "limit": 5,
        },
        {"capability_id": values["capability_id"], "workflow_stage": values["workflow_stage"]},
        {
            "category": "capability",
            "capability_id": values["capability_id"],
            "workflow_stage": values["workflow_stage"],
            "limit": 3,
        },
    ]
    # EXHAUSTIVE over single selectors, not a sample of one value each. Every
    # escape of this class lived at an unsampled value — `node_kind` was probed
    # at 1 of 21, `capability_id` at 1 of 25, `authoring_entry_id` at 1 of 179 —
    # and widening the sampler by hand is the regress that has re-opened this
    # site four rounds running. The single-selector space is not large: it is
    # the published facets plus the entry ids, 244 fetches, measured at 0.55s.
    # Enumerating it terminates that space instead of sampling it.
    sweep = (
        [{"node_kind": v} for v in sorted(facets["node_kinds"])]
        + [{"capability_id": v} for v in sorted(facets["capability_ids"])]
        + [{"workflow_stage": v} for v in sorted(facets["workflow_stages"])]
        + [
            {"authoring_entry_id": entry.contract_entry_id}
            for entry in entries
        ]
    )
    fetches = (
        [{}]
        + sweep
        + [{name: value} for name, value in sorted(values.items())]
        + [
            dict(companion, **{name: values[name]})
            for name, companion in sorted(companions.items())
        ]
        + [{"limit": value} for value in extra_limits]
        + [
            {"workflow_stage": values["workflow_stage"], "limit": value}
            for value in extra_limits
        ]
        + [{"category": c} for c in categories]
        + [{"category": c, "node_kind": values["node_kind"]} for c in categories]
        + deep
    )
    seen_keys = set()
    fetches = [
        kwargs
        for kwargs in fetches
        if not (_key(kwargs) in seen_keys or seen_keys.add(_key(kwargs)))
    ]

    checked = 0
    populated = empty = 0
    seen = {}
    graded = {}
    graded_fields = {}
    rejected = []
    for kwargs in fetches:
        # THE SERVED PATH, not the projection function. Every assertion here
        # used to read `query_process_ir_authoring_contract` directly — but a
        # caller reaches this payload through `get_schema_template` ->
        # `_authoring_contract_schema` -> `.model_dump()`, and a rule deleted at
        # either of those two hops was invisible to all 103 tests in this file.
        # A guard on the layer below the one being served grades the wrong
        # universe, however total it is within it.
        served = _served_contract_page(kwargs)
        if served is None:
            # The query layer raises BEFORE constructing a page, so a rejected
            # facet pair is not a page any caller receives. Counted rather than
            # silently skipped: an unbounded `continue` is how the floor below
            # could have been satisfied by grading almost nothing.
            rejected.append(kwargs)
            continue
        for rule in rules:
            value = served[rule]
            assert value not in ("", None, [], {}), (kwargs, rule)
            # A page RULE does not vary with the query — that is what makes it a
            # rule — so every page must agree. Comparing to the declared default
            # would exempt `state_mappings`, which is computed rather than
            # defaulted, and that exemption is where a blank legend hid.
            seen.setdefault(rule, (kwargs, value))
            assert value == seen[rule][1], (kwargs, rule, seen[rule][0])
        # The page ECHOES the query it answered, so every selector the caller
        # sent must come back. This is the forwarding check, and it is total by
        # construction rather than one assertion per selector: dropping
        # `limit=limit` from the tool wrapper silently served the default page
        # while every rule assertion above still passed, because none of them
        # reads anything a limit changes.
        # EXACT, including the selectors NOT sent. Checking only what was sent
        # is total over drops and blind to INJECTION: a wrapper that added
        # `category` or `limit=1` served a different page while every echo
        # stayed self-consistent, because the injected value echoed too.
        # `limit` is the one selector with a documented normalization: unsent,
        # it echoes the published default rather than None. Everything else
        # unsent must echo None — that is what makes INJECTION visible.
        assert served["query"] == {
            name: kwargs.get(
                name,
                PROCESS_IR_AUTHORING_DEFAULT_LIMIT if name == "limit" else None,
            )
            for name in selectors
        }, (kwargs, served["query"])

        for name, value in served.items():
            graded_fields.setdefault(name, []).append(value)

        checked += 1
        graded[_key(kwargs)] = served["returned_entry_count"]
        if served["returned_entry_count"]:
            populated += 1
        else:
            empty += 1

    # An EQUATION, not a floor. `checked >= len(categories) + 1` had 48% slack,
    # set at exactly the count that survives losing the entire zero-match
    # population — so a future rename of the probed node kind would silently
    # `continue` past all fourteen and still pass. Every fetch is a page a
    # caller receives today, so every fetch must have been graded.
    assert checked + len(rejected) == len(fetches), (checked, rejected)
    # Every rejection is a category with no entry of the probed node kind — a
    # combination the tool refuses, not a page left ungraded.
    # The EXACT set, not a shape allowlist. A shape check let six single-hunk
    # changes make pages vanish into `rejected` and pass — extending the
    # documented companion rule to `limit`, an envelope that errors or omits
    # `contract_page`, or dropping the eleven zero-match pairs — because
    # `checked + len(rejected) == len(fetches)` is a tautology of the loop and
    # cannot fail whatever disappears. Naming the one page that is genuinely
    # unfetchable de-tautologises it: anything else vanishing is a failure.
    assert rejected == [{"after_entry_id": values["after_entry_id"]}], rejected

    # Both advertised spellings of the selector serve the same page. Only the
    # unversioned one was ever fetched, so a rule blanked on
    # `process_ir_authoring@1` — a spelling the registry accepts — was green.
    for kwargs in ({}, {"category": categories[0]}, deep[-1]):
        plain = _served_contract_page(kwargs)
        versioned = _served_contract_page(kwargs, selector=SELECTOR + "@1")
        assert versioned is not None, kwargs
        assert versioned == plain, kwargs
    assert set(values) <= {k for kwargs in fetches for k in kwargs}, "a selector went unfetched"

    # BOTH construction sites must be exercised — the page model is built in two
    # places and the omission that started this was in the one nothing fetched.
    # Stated as the property that matters rather than as a count: EVERY selector
    # must reach the POPULATED site (a selector whose only fetch comes back
    # empty silently grades the early return twice and the populated site never),
    # and the empty site must be reached at all.
    # The early-return site must be reached by MORE than the one invariant bare
    # page: a site graded by a single fixed input is graded for existence, not
    # for behaviour. Counted by the site actually taken — 11 of the 13
    # zero-result pages come from the POPULATED site (a zero-match filter), so
    # `empty >= 2` over all zero-result pages was satisfied without the bare
    # `limit` fetch existing at all, which made the fix re-openable by deleting
    # one line of this test.
    assert populated > 0, populated
    early = [k for k in fetches if not (set(k) - {"limit"})]
    assert len(early) >= 2, early
    # EXACTLY the filter-free fetches come back empty, and no other fetch does
    # by accident. `empty >= len(early)` compared zero-RESULT pages from either
    # site to a static count of intended early fetches — 4 early vs 11
    # zero-match populated pages was 15 >= 4, so one token
    # (`and not limit`) could make a filter-free `{limit: 50}` serve 50 entries
    # and stay green while the index still published the opposite.
    # EVERY filter-free fetch is empty, and that is checked against the claim
    # the contract PUBLISHES rather than against a count. `empty >= len(early)`
    # compared zero-result pages from either site to a static number — 15 >= 4
    # held with eleven of the fifteen coming from the populated site — so one
    # token (`and not limit`) could make a filter-free `{limit: 50}` serve 50
    # entries while the index went on publishing the opposite.
    from boomi_mcp.authoring.process_ir_projection import (
        build_process_ir_authoring_index,
    )

    published_bare = build_process_ir_authoring_index()["retrieval"][
        "bare_retrieval_returns_entries"
    ]
    assert published_bare is False, published_bare
    for kwargs in early:
        assert graded[_key(kwargs)] == 0, (kwargs, graded[_key(kwargs)])
    # ...and the claim is not vacuously true because nothing is fetched.
    # EVERY filter-free page, not a floor over four of fifty-one. `limit` is the
    # only selector that varies one, so the space is exactly `{}` plus the valid
    # limits — 51 pages, 0.18s — and a rule conditioned on an unswept limit hid
    # there through four consecutive rounds of raising the floor.
    assert len(early) == PROCESS_IR_AUTHORING_MAX_LIMIT + 1, len(early)
    for name, value in sorted(values.items()):
        kwargs = dict(companions.get(name, {}), **{name: value})
        reached = _served_contract_page(kwargs)
        assert reached is not None and reached["returned_entry_count"] > 0, (
            f"selector {name!r} never reaches the populated construction site"
        )
    assert len(rejected) < len(categories), rejected

    # Every field the served pages AGREE on is a rule, derived from the pages
    # themselves. Two disjoint derivations must give the same answer — the
    # constant cannot be widened to exempt a rule without contradicting the
    # payloads.
    agreed = {
        name
        for name, values_seen in graded_fields.items()
        if len({repr(v) for v in values_seen}) == 1
    }
    # A declared rule that VARIES across pages is a defect, not an exclusion.
    # Reading `agreed` as "the things worth grading" let a rule opt itself out
    # by misbehaving — precisely backwards.
    varying = sorted(set(rules) - agreed)
    assert varying == [], {
        name: sorted({repr(v) for v in graded_fields[name]})[:3] for name in varying
    }
    # ...and every invariant field is GRADED, whatever the constant says. That
    # is what removes the constant's leverage: widening it to exempt
    # `diagnostic_label_legend` no longer stops the legend being checked here,
    # because the pages themselves put it in this set.
    for name in sorted(agreed):
        value = graded_fields[name][0]
        assert value not in ("", None, [], {}), name
        field = ProcessIRAuthoringContractPageV1.model_fields[name]
        if field.is_required():
            continue  # no declared default to compare against
        declared = field.default
        if declared not in (None, "", (), [], {}) and not callable(declared):
            assert value == declared, (name, value, declared)

    # ...and the rules that ARE declared with a substantive default match it,
    # so "identical everywhere" cannot be satisfied by being uniformly wrong.
    for rule in rules:
        field = ProcessIRAuthoringContractPageV1.model_fields[rule]
        if field.is_required():
            continue  # no declared default to compare against
        declared = field.default
        if declared not in (None, "", (), [], {}) and not callable(declared):
            assert seen[rule][1] == declared, rule


def test_a_not_requested_comparison_declares_every_field_it_reports():
    """#512. The early return was never graded, so a bogus name shipped green.

    Behaviourally benign — `status` and `revision_skew` both say
    `not_requested` — but a field this delta added is a field this delta owns,
    and "nobody looks at it here" is how the previous four findings started.
    """
    from boomi_mcp.authoring.workflow import compare_live_build_provenance

    base, observed, _ = _provenance_fixture()
    result = compare_live_build_provenance({}, {})

    assert result.status == "not_requested"
    assert result.revision_skew == "not_requested"
    assert result.revision_uncompared == ()
    assert result.revision_mismatches == ()
    # Every declared field is present and defaulted — no silent extras, no
    # field left to a model default nobody asserted.
    dumped = result.model_dump(mode="json")
    assert set(dumped) == set(type(result).model_fields), (
        sorted(set(dumped) ^ set(type(result).model_fields)),
    )


#: Page fields that describe the QUERY rather than publish a RULE.
#:
#: The split is needed in three places (the page-rule guard, the index
#: agreement guard, and the revision-payload coverage pin next door), so it is
#: named once rather than hand-written three times.
#:
#: `facets` and `catalog_entry_count` are NOT in here, and getting that wrong
#: twice is worth recording. First a comment claimed as measured fact that
#: `facets` differed per query; it does not. Then the exclusion survived on a
#: rewritten "by contract" justification — but the model says
#: "Every filterable value in the CATALOG ... discovering what you may ask for
#: must not require already having asked", and the type holds no numbers at
#: all. Both fields are catalog-wide facts, so both are graded as rules;
#: narrowing a filtered page's advertised facets breaks the
#: fetch-then-filter loop this selector is built around.
_QUERY_DEPENDENT_PAGE_FIELDS = frozenset(
    {
        "query",
        "matched_entry_count",
        "returned_entry_count",
        "limit",
        "truncated",
        "next_after_entry_id",
        "entries",
    }
)


def _served_contract_page(query, selector=None):
    """The contract page as a CALLER receives it, or None if unfetchable.

    Enters at `server.get_schema_template` — the MCP wrapper — not at the
    action beneath it. Every layer between the caller and the payload is then
    inside the graded universe: the wrapper's own argument forwarding, the
    envelope assignment, the adapter's `model_dump`, and the projection. The
    action-level entry left the wrapper ungraded, and blanking a rule there was
    green even though this delta's own clean-room file already drives
    `server.get_schema_template` directly.

    Returns the page dict alone. An earlier version returned a pair whose first
    element the docstring called a model and which was in fact the envelope,
    and which nothing ever read.
    """
    payload = server.get_schema_template(schema_name=selector or SELECTOR, **query)
    if not payload.get("_success", True):
        return None
    return payload.get("contract_page")


def test_the_served_state_mappings_match_the_committed_fixture_by_value():
    """#517. Its declared default is `()`, so the default loop skipped it.

    Cross-page identity cannot see a row dropped identically at BOTH
    construction sites, and the declared-default comparison exempts any rule
    whose default is empty — so 14 published mappings could be deleted from the
    served payload with the whole suite green. The committed contract snapshot
    already pins every row; this asserts the SERVED payload against it, which is
    the comparison that was missing.
    """
    import json

    committed = json.loads(
        (
            Path(__file__).resolve().parent
            / "fixtures"
            / "authoring_contract"
            / "process_ir_authoring_v1.contract.json"
        ).read_text()
    )["state_mappings"]
    assert committed, "the fixture pins no mappings"

    def canonical(rows):
        # The page model sorts its mappings; the revision payload dumps them in
        # registry order. That difference is real and legitimate, so the
        # comparison is over CONTENT — which is the thing that was unpinned.
        return sorted(json.dumps(row, sort_keys=True) for row in rows)

    served = _served_contract_page({"category": "capability"})
    assert served is not None
    assert canonical(served["state_mappings"]) == canonical(committed)


def test_the_index_publishes_the_same_page_rules_as_the_page_itself():
    """#522. The index hard-coded two rules the page derives from the model.

    Two surfaces published the same rule from two sources, and flipping the
    index's literal to `"gated"` served a falsified rule through
    `list_capabilities()` — and moved the capability revision, because the index
    feeds it — with the whole suite green. Deriving it removed the drift;
    this makes the drift detectable, which is the other half.
    """
    from boomi_mcp.authoring.process_ir_projection import (
        build_process_ir_authoring_index,
    )
    from boomi_mcp.models.process_ir_authoring import (
        ProcessIRAuthoringContractPageV1,
    )

    index = build_process_ir_authoring_index()
    served = _served_contract_page({"category": "capability"})
    assert served is not None

    shared = [
        name
        for name in ProcessIRAuthoringContractPageV1.model_fields
        if name in index
        and name in served
        and name not in _QUERY_DEPENDENT_PAGE_FIELDS
    ]
    assert {"unlisted_placement_state", "unlisted_connector_action_state"} <= set(
        shared
    ), shared

    for name in shared:
        field = ProcessIRAuthoringContractPageV1.model_fields[name]
        declared = None if field.is_required() else field.default
        if name == "facets":
            # The index publishes only the FILTERABLE facets; the page adds
            # `entry_types`, which is informational — there is no `entry_type`
            # selector. Compared over the index's own keys, with the asymmetry
            # asserted rather than assumed.
            assert set(index[name]) < set(served[name]), (
                sorted(index[name]),
                sorted(served[name]),
            )
            assert set(served[name]) - set(index[name]) == {"entry_types"}
            for facet in index[name]:
                assert _canonical(index[name][facet]) == _canonical(
                    served[name][facet]
                ), (facet, index[name][facet], served[name][facet])
            continue
        # Content, not order: the page model sorts `state_mappings` and the
        # index emits registry order — a real, legitimate difference.
        assert _canonical(index[name]) == _canonical(served[name]), (
            name, index[name], served[name],
        )
        if declared not in (None, "", (), [], {}) and not callable(declared):
            assert index[name] == declared, (name, index[name], declared)

    # ...and the value a caller actually receives through the other surface.
    # Through the registered tool, not the action beneath it — #520's shape,
    # live on the adjacent surface: falsifying a rule in `server.list_capabilities`
    # was green because every assertion entered one hop lower.
    published = _await(server.list_capabilities())["authoring_contract"][SELECTOR]
    # Every shared field, not the two this fix happened to touch: `shared` is
    # an intersection and naming two of its members left the rest — including
    # `state_mappings` — free to be overridden in the manifest.
    for name in shared:
        assert _canonical(published[name]) == _canonical(index[name]), (
            name,
            published[name],
            index[name],
        )
    assert len(shared) >= 3, shared


def _canonical(value):
    """Order-insensitive for lists of rows; identity for everything else."""
    import json

    if isinstance(value, list):
        return sorted(json.dumps(row, sort_keys=True) for row in value)
    return value


def _key(query):
    """A stable, comparable identity for a fetch's kwargs."""
    return tuple(sorted(query.items()))





def _await(value):
    """Resolve a coroutine from a registered async tool; pass values through."""
    import asyncio
    import inspect as _inspect

    if not _inspect.isawaitable(value):
        return value
    loop = asyncio.new_event_loop()
    try:
        return loop.run_until_complete(value)
    finally:
        loop.close()


def test_a_corrupted_page_is_refused_rather_than_served():
    """#538. The re-validation mechanism itself had no pin — removing it was green.

    It is what turns "this rule is correct by construction" from a statement
    about the model into a statement about the payload, so it is the thing that
    ends the value-space regress: no fetch list can cover ~10^11 pages, but a
    validator runs on all of them. That deserves a test naming it.
    """
    from boomi_mcp.categories.meta_tools import _revalidated_contract_page

    served = _served_contract_page({"category": "capability"})
    assert served is not None

    # A healthy page round-trips unchanged, byte for byte.
    assert _revalidated_contract_page(dict(served)) == served

    # ...and each corruption verb behaves as the comment says.
    import pydantic

    for field, corrupt in (
        ("diagnostic_label_legend", ""),                      # blanked
        ("diagnostic_label_legend", "A compile never returns parser text."),
        ("unlisted_placement_state", "supported"),            # falsified
        ("state_mappings", served["state_mappings"][:1]),     # cut
        ("catalog_entry_count", served["matched_entry_count"]),
        # `facets` was the one registry-validated rule this tuple omitted, so
        # removing its validator branch — or misspelling its field name — was
        # green. All three branches removed failed exactly ONE test, the same
        # one the single removals failed, which is what exposed it.
        (
            "facets",
            dict(served["facets"], workflow_stages=served["facets"]["workflow_stages"][:1]),
        ),
    ):
        with pytest.raises(pydantic.ValidationError):
            _revalidated_contract_page(dict(served, **{field: corrupt}))

    # ...and every registry-validated rule is covered here, derived rather than
    # listed, so a new one cannot be added without a corruption case for it.
    from boomi_mcp.authoring.process_ir_projection import expected_page_rule

    for rule in ("state_mappings", "catalog_entry_count", "facets"):
        assert expected_page_rule(rule) is not None, rule
    with pytest.raises(KeyError):
        expected_page_rule("facetz")

    # An INJECTED key is refused too.
    with pytest.raises(pydantic.ValidationError):
        _revalidated_contract_page(dict(served, surprise_field=1))

    # A DROPPED rule is handled for EVERY rule, derived — not for the single
    # field the first version of this test happened to check, which was also
    # the only one where the convenient answer was true. Pydantic skips
    # `AfterValidator` on a default, so a rule whose default is not its correct
    # value was served wrong when dropped: `state_mappings` came back empty.
    #
    # Two acceptable outcomes, no third: refuse the page, or restore the
    # correct value. Serving a different one is the defect.
    from boomi_mcp.models.process_ir_authoring import (
        ProcessIRAuthoringContractPageV1,
    )

    rules = [
        name
        for name in ProcessIRAuthoringContractPageV1.model_fields
        if name not in _QUERY_DEPENDENT_PAGE_FIELDS
    ]
    assert len(rules) >= 6, rules
    for rule in rules:
        without = {k: v for k, v in served.items() if k != rule}
        try:
            restored = _revalidated_contract_page(without)
        except pydantic.ValidationError:
            continue  # refused — the other acceptable outcome
        assert restored == served, (
            f"dropping {rule!r} was neither refused nor restored: "
            f"served {restored.get(rule)!r} instead of {served.get(rule)!r}"
        )


def test_both_revalidation_call_sites_refuse_a_corrupted_page(monkeypatch):
    """#538. The mechanism was pinned; its two CALL SITES were not.

    Deleting either one was green, because the only test named the helper
    directly. Each site covers layers the other cannot: the action's catches
    corruption at or below the adapter, the wrapper's catches the three layers
    above the action — including the wrapper itself.
    """
    import pydantic

    from boomi_mcp.authoring import contract as authoring_contract
    from boomi_mcp.categories import meta_tools as mt

    healthy = _served_contract_page({"category": "capability"})
    assert healthy is not None

    # (1) Corrupt BELOW the action — the adapter's dump. The action's own
    # re-validation is the only thing between this and the caller.
    original_query = authoring_contract._LOCAL_BUILDERS["_process_ir_authoring_query"]

    def poisoned(**filters):
        page = original_query(**filters)
        page["diagnostic_label_legend"] = ""
        return page

    monkeypatch.setitem(
        authoring_contract._LOCAL_BUILDERS, "_process_ir_authoring_query", poisoned
    )
    with pytest.raises(pydantic.ValidationError):
        mt.get_schema_template_action(schema_name=SELECTOR, category="capability")
    monkeypatch.undo()

    # (2) Corrupt ABOVE the action — everything the action already returned.
    # Only the wrapper's re-validation stands between this and the caller.
    original_action = mt.get_schema_template_action

    def poisoned_action(**kwargs):
        payload = original_action(**kwargs)
        if isinstance(payload.get("contract_page"), dict):
            payload["contract_page"]["unlisted_placement_state"] = "supported"
        return payload

    monkeypatch.setattr(server, "get_schema_template_action", poisoned_action)
    with pytest.raises(pydantic.ValidationError):
        server.get_schema_template(schema_name=SELECTOR, category="capability")


def test_the_page_rule_protection_mechanisms_are_counted_correctly():
    """#542. Two versions of the mechanism comment miscounted these sets.

    The counts are the load-bearing part of the claim — "correct by
    construction" is only true if every rule is covered by one mechanism or the
    other — so they are derived and asserted rather than written down.
    """
    import typing

    from boomi_mcp.models.process_ir_authoring import (
        ProcessIRAuthoringContractPageV1,
    )

    rules = {
        name
        for name in ProcessIRAuthoringContractPageV1.model_fields
        if name not in _QUERY_DEPENDENT_PAGE_FIELDS
    }
    literal, registry = set(), set()
    for name in rules:
        field = ProcessIRAuthoringContractPageV1.model_fields[name]
        if typing.get_origin(field.annotation) is typing.Literal:
            literal.add(name)
        if "_matches_the_registry" in repr(field.metadata):
            registry.add(name)

    assert literal == {
        "contract_version",
        "diagnostic_label_legend",
        "unlisted_placement_state",
        "unlisted_connector_action_state",
    }, sorted(literal)
    assert registry == {"state_mappings", "facets", "catalog_entry_count"}, sorted(
        registry
    )
    # Every rule is covered by exactly one mechanism — no rule protected by
    # neither, which is what "correct by construction" asserts.
    assert literal | registry == rules, sorted(rules - (literal | registry))
    assert literal & registry == set(), sorted(literal & registry)

    # A registry-validated rule must be REQUIRED: pydantic skips
    # `AfterValidator` on a default, so a default would silently disable it.
    for name in registry:
        assert ProcessIRAuthoringContractPageV1.model_fields[name].is_required(), name
    # ...and a `Literal` rule's default must BE its literal value.
    for name in literal:
        field = ProcessIRAuthoringContractPageV1.model_fields[name]
        assert typing.get_args(field.annotation) == (field.default,), name


def test_the_served_state_mappings_are_normalised_before_they_are_served():
    """#543. `_sorted_unique_models` on this field was graded by nothing.

    Deleting it was full-suite green while changing the served mapping order on
    every page, and letting a downstream permutation through un-normalised. The
    registry validator compares content, deliberately — so order is precisely
    what it cannot see, and this is the guard for it.
    """
    from boomi_mcp.authoring.process_ir_projection import state_mappings

    served = _served_contract_page({"category": "capability"})
    assert served is not None
    rows = served["state_mappings"]
    assert rows

    keys = [json.dumps(row, sort_keys=True) for row in rows]
    assert keys == sorted(keys), keys
    assert len(keys) == len(set(keys)), "duplicate mapping rows served"

    # Normalisation, not accident: the registry's own order is different, and
    # a page built from a permuted registry still serves the sorted order.
    registry_keys = [
        json.dumps(m.model_dump(mode="json"), sort_keys=True) for m in state_mappings()
    ]
    assert sorted(registry_keys) == keys
    assert len(registry_keys) == len(keys)


def test_the_operation_symbol_carries_the_connection_its_plan_declares():
    """The mechanism behind the test above, pinned directly.

    Asserted at the symbol table rather than only end-to-end, so a regression
    names the cause instead of surfacing as an opaque compile rejection three
    layers away.
    """
    from boomi_mcp.models.integration_models import IntegrationComponentSpec
    from boomi_mcp.recipes.materialization import build_symbol_table

    components = [
        IntegrationComponentSpec(
            key="conn", name="C", type="connector-settings",
            config={"connector_type": "database"},
        ),
        IntegrationComponentSpec(
            key="op", name="O", type="connector-action",
            config={
                "action_type": "Get",
                "connection_ref_key": "conn",
                "connector_type": "database",
            },
        ),
        IntegrationComponentSpec(key="plain", name="P", type="transform.map", config={}),
    ]
    symbols = {s.ref: s for s in build_symbol_table(components).symbols}

    assert symbols["$ref:op"].connection_ref == "$ref:conn"
    # A component that declares no connection must not invent one.
    assert symbols["$ref:conn"].connection_ref is None
    assert symbols["$ref:plain"].connection_ref is None


def test_a_terminal_that_is_not_last_reports_its_own_code_at_the_body():
    """The second case the plan named, only ever tested at the internal layer.

    It cannot ride the parametrize above, which varies the LAST step: illegal
    terminal placement is by definition about a terminal that is not last.
    """
    payload = integration_builder._plan_authoring(
        None, "p", _process_ir_request({"kind": "stop"}, SOURCE)
    )
    assert payload["_success"] is False
    rows = payload["validation_errors"]
    assert {row["code"] for row in rows} == {"PROCESS_IR_SCHEMA_INVALID_CARDINALITY"}
    assert {row["path"] for row in rows} == {"/intent/units/0/process_ir/body"}


def test_a_compile_diagnostic_serves_the_message_its_authority_wrote():
    """F2's pin. The compile phase used to overwrite every authority message.

    `build_artifact_descriptors` hardcoded "Canonical compilation rejected this
    process." for every diagnostic while forwarding only the remediation, and
    `_authoring_error_envelope` puts that string in the envelope's top-level
    `error` — so the generic sentence was the HEADLINE, and the compiler's real
    statement never reached the caller. The semantic phase forwarded its
    message correctly all along; only compile was wrong.

    Asserted against the AUTHORITY TABLE, never a literal, so rewording a
    compiler message cannot rot this test into passing on stale text.
    """
    from boomi_mcp.authoring.workflow import _compile_message
    from boomi_mcp.compiler.process_ir.diagnostics import compiler_diagnostic_specs

    authored = {
        str(spec["code"]): (spec.get("message") or "").strip()
        for spec in compiler_diagnostic_specs()
    }
    assert any(authored.values()), "the authority table carries no messages"

    class _Diagnostic:
        def __init__(self, code, message):
            self.code = code
            self.message = message

    # Every code the compiler can raise WITH a message of its own is served
    # with that message, not the generic headline.
    covered = 0
    for code, message in authored.items():
        if not message:
            continue
        assert _compile_message(_Diagnostic(code, message)) == message, code
        covered += 1
    assert covered >= 20, covered

    # ...and the generic headline survives exactly where it is correct: a
    # diagnostic that names no message of its own.
    from boomi_mcp.authoring.workflow import _COMPILE_GENERIC_MESSAGE

    for empty in ("", "   ", None):
        assert _compile_message(_Diagnostic("X", empty)) == _COMPILE_GENERIC_MESSAGE


def test_the_compile_call_site_forwards_the_authority_message(monkeypatch):
    """The CALL SITE, not the helper — reverting the fix must fail something.

    A first version of this pin exercised `_compile_message` directly, so
    putting the hardcoded string back at the call site was still green. That is
    the same shape as the re-validation gap found earlier in this review: a
    mechanism graded in isolation while the line that uses it is ungraded.

    The compile phase is reached by monkeypatching the compiler to raise,
    because the documents that fail reference resolution are caught earlier by
    semantic validation and never get there.
    """
    from boomi_mcp.authoring import workflow as wf
    from boomi_mcp.compiler.process_ir.diagnostics import (
        CompilerDiagnostic,
        ProcessIRCompileError,
        compiler_diagnostic_specs,
    )

    spec = next(
        s for s in compiler_diagnostic_specs() if (s.get("message") or "").strip()
    )
    authored = spec["message"].strip()

    def _raise(*_args, **_kwargs):
        raise ProcessIRCompileError(
            (
                CompilerDiagnostic(
                    code=str(spec["code"]),
                    phase="reference_resolution",
                    path="/body/steps/0",
                    node_identity="",
                    message=authored,
                    remediation=(spec.get("remediation") or ""),
                ),
            )
        )

    # It is imported inside the function, so patch it at its SOURCE module.
    from boomi_mcp.compiler.process_ir import pipeline

    # #178: patch the PRIVATE CORE, not the public entry. `build_artifact_
    # descriptors` now reaches the compiler through `parse_and_compile_process_
    # ir_v1` (one parse instead of a parse plus a compile), so a patch on the
    # public wrapper no longer intercepts THIS call site — the failure would
    # surface later, from materialization, with a different message, and the
    # test would be grading a different line than the one it names. Every route
    # reaches the core.
    monkeypatch.setattr(pipeline, "_compile_parsed_process_ir_v1", _raise)

    # A document that genuinely reaches the compiler. `_process_ir_request`
    # declares no components, so anything built from it is stopped by semantic
    # validation first and the patched compiler never runs — which is how the
    # first draft of this test asserted a semantic message by mistake.
    fixture = json.loads(
        (
            Path(__file__).resolve().parent
            / "fixtures"
            / "authoring_contract"
            / "clean_room"
            / "decision_route_connector_map.json"
        ).read_text()
    )
    request = {"authoring_request": fixture["request"]}
    payload = integration_builder._compile_authoring(None, "p", request)

    assert payload["_success"] is False
    diagnostics = payload.get("authoring_diagnostics") or []
    assert diagnostics, payload
    served = {d["message"] for d in diagnostics}
    assert served == {authored}, served
    # The envelope headline a caller reads first carries it too.
    assert authored in str(payload.get("error"))


@pytest.mark.parametrize("authored", [[], "x", 7, None, True])
def test_a_non_object_process_ir_gets_the_canonical_diagnostic(authored):
    """F1. The parser was gated on `Mapping`, so these skipped it entirely.

    A non-object `process_ir` fell through to raw pydantic and answered
    `model_type` at `intent.process_ir.process_ir` — no `PROCESS_IR_*` code, no
    remediation, no contract citation — on plan, compile and apply, while
    `parse_process_ir_v1` already had a purpose-built answer for that exact
    input.
    """
    for action in (
        integration_builder._plan_authoring,
        integration_builder._compile_authoring,
    ):
        request = json.loads(json.dumps(_process_ir_request({"kind": "stop"})))
        # #153: the root lives at `units[0].process_ir`, so the malformed value
        # is injected there. The pointer is unit-INDEXED for the same reason —
        # with several roots, an unindexed pointer would not say which one.
        request["authoring_request"]["intent"]["units"][0]["process_ir"] = authored
        payload = action(None, "p", request)

        assert payload["_success"] is False
        codes = {row["code"] for row in payload["validation_errors"]}
        assert codes == {"PROCESS_IR_SCHEMA_INVALID"}, codes
        assert {row["path"] for row in payload["validation_errors"]} == {
            "/intent/units/0/process_ir"
        }
        diagnostics = payload["authoring_diagnostics"]
        assert diagnostics
        for diagnostic in diagnostics:
            assert diagnostic["remediation"]
            assert diagnostic["authoring_contract_entry_ids"]


def test_an_absent_process_ir_still_reports_missing_not_a_shape_error():
    """The other half of F1's fix: a PRESENCE check, not an unconditional call.

    Calling the parser unconditionally would answer "payload must be a JSON
    object" for a key the caller simply did not send, which is a worse answer
    than pydantic's `missing`.
    """
    request = json.loads(json.dumps(_process_ir_request({"kind": "stop"})))
    # #153: the root now sits at `units[0].process_ir`, so THAT is the key whose
    # absence must still read as pydantic's `missing`. The pre-parse skips a unit
    # with no `process_ir` for exactly this reason.
    request["authoring_request"]["intent"]["units"][0].pop("process_ir")
    payload = integration_builder._plan_authoring(None, "p", request)

    assert payload["_success"] is False
    rows = payload["validation_errors"]
    assert rows and all(row.get("type") == "missing" for row in rows), rows
    assert not any("JSON object" in json.dumps(row) for row in rows)


def test_a_dead_authority_is_reported_unavailable_not_served_short(monkeypatch):
    """F9. A failed recipe registry used to serve a catalog that looked whole.

    The lone `try/except` in `collect_projection_sources` turned a registry
    failure into an empty tuple, so the contract served 171 entries with ZERO
    recipe links, `_success: true` and `truncated: false` — indistinguishable,
    from the caller's side, from a contract where no construct links a recipe.
    Every other source there is unguarded and already degrades honestly.

    Deleting the swallow alone was not enough: it let a raw `RuntimeError`
    cross the MCP boundary, which is a worse answer than the short catalog. So
    the page path reports the same typed `unavailable` that
    `list_capabilities` already reported for this exact condition.
    """
    from boomi_mcp.authoring import process_ir_projection as projection
    from boomi_mcp.errors import AUTHORING_SCHEMA_SOURCE_UNAVAILABLE

    healthy = server.get_schema_template(schema_name=SELECTOR, category="recipe")
    assert healthy["_success"] is True
    assert healthy["contract_page"]["returned_entry_count"] > 0

    def _dead():
        raise RuntimeError("registry is dead")

    monkeypatch.setattr("boomi_mcp.recipes.production_registry", _dead)
    projection.reset_process_ir_authoring_cache()
    try:
        payload = server.get_schema_template(schema_name=SELECTOR, category="recipe")
        assert payload["_success"] is False
        assert payload["error_code"] == AUTHORING_SCHEMA_SOURCE_UNAVAILABLE
        assert payload.get("status") == "unavailable"
        assert payload.get("retryable") is True
        # No short catalog, and no authority text or traceback in the answer.
        assert "contract_page" not in payload
        assert "registry is dead" not in json.dumps(payload)
        assert "Traceback" not in json.dumps(payload)
    finally:
        # monkeypatch only undoes at TEARDOWN, so the recovery assertion below
        # would otherwise still run against the dead registry.
        monkeypatch.undo()
        projection.reset_process_ir_authoring_cache()

    # ...and the surface recovers once the authority does.
    recovered = server.get_schema_template(schema_name=SELECTOR, category="recipe")
    assert recovered["contract_page"]["returned_entry_count"] == (
        healthy["contract_page"]["returned_entry_count"]
    )


def test_the_projection_cannot_write_through_to_the_doctrine_catalog():
    """F5. The projection shallow-copied entries out of the live catalog.

    `design_doctrine` documents that every accessor deepcopies so per-call
    mutation cannot corrupt module state. The projection bypassed the
    accessors and did `dict(entry)`, which is shallow — so `cross_refs` and
    `mutual_exclusion` stayed aliased to the module's own lists.
    """
    import copy as _copy

    from boomi_mcp.authoring.process_ir_projection import collect_projection_sources
    from boomi_mcp.kb.design_doctrine import (
        DESIGN_DOCTRINE_ENTRIES,
        design_doctrine_capability_rows,
    )

    before = _copy.deepcopy(DESIGN_DOCTRINE_ENTRIES)
    rows = collect_projection_sources().doctrine_rows
    assert rows

    mutated = 0
    for row in rows:
        for field in ("cross_refs", "mutual_exclusion"):
            value = row.get(field)
            if isinstance(value, list):
                value.append("__MUTATED__")
                mutated += 1
    assert mutated, "no list-valued doctrine field was reachable to mutate"
    assert DESIGN_DOCTRINE_ENTRIES == before, "the live catalog was written through"

    # ...and the accessor hands over capability fields only — never prose.
    served_fields = set()
    for row in design_doctrine_capability_rows():
        served_fields |= set(row)
    assert served_fields <= {
        "name",
        "category",
        "capability_status",
        "verification_status",
        "provenance",
        "cross_refs",
        "mutual_exclusion",
    }, sorted(served_fields)
    for prose in ("problem", "when_to_use", "when_not_to_use", "boomi_shape_mapping"):
        assert prose not in served_fields, prose


def test_a_page_out_of_published_order_or_miscounted_is_refused():
    """F3. `entries` was the only tuple on the page model with no validator.

    A reversed page, a duplicated entry, and a page whose length contradicted
    `returned_entry_count` all passed BOTH re-validation hops. Output was
    correct only because the projector sorts and slices — an unguarded
    invariant, not a live wrong answer.

    The guard CHECKS and refuses; it does not sort. Sorting would launder a
    downstream permutation instead of refusing it, which is the whole point of
    re-validating a served page, and de-duplicating would desync the count.
    It is `contract_entry_id`-keyed, so it is unrelated to the generic
    JSON-serialization sorter that once broke pagination — and it is a no-op on
    every page the projector builds.
    """
    import pydantic

    from boomi_mcp.categories.meta_tools import _revalidated_contract_page

    served = _served_contract_page({"category": "capability"})
    assert served is not None and len(served["entries"]) > 2

    # ...unchanged, it round-trips.
    assert _revalidated_contract_page(dict(served)) == served

    # An ADJACENT SWAP — the minimal genuine order violation. A full reversal
    # on a TRUNCATED page is refused by the cursor validator added in the same
    # commit, which masked the order check entirely: deleting the order
    # validator left the whole suite green.
    swapped = list(served["entries"])
    swapped[0], swapped[1] = swapped[1], swapped[0]
    with pytest.raises(pydantic.ValidationError):
        _revalidated_contract_page(dict(served, entries=swapped))

    reversed_page = dict(served, entries=list(reversed(served["entries"])))
    with pytest.raises(pydantic.ValidationError):
        _revalidated_contract_page(reversed_page)

    duplicated = dict(
        served,
        entries=list(served["entries"]) + [served["entries"][-1]],
        returned_entry_count=len(served["entries"]) + 1,
    )
    with pytest.raises(pydantic.ValidationError):
        _revalidated_contract_page(duplicated)

    miscounted = dict(served, returned_entry_count=len(served["entries"]) + 7)
    with pytest.raises(pydantic.ValidationError):
        _revalidated_contract_page(miscounted)

    # A truncated page's cursor must be the last entry it carried — that is the
    # value a caller feeds straight back.
    truncated = _served_contract_page({"category": "diagnostic", "limit": 3})
    assert truncated is not None and truncated["truncated"] is True
    assert truncated["next_after_entry_id"] == truncated["entries"][-1][
        "contract_entry_id"
    ]
    with pytest.raises(pydantic.ValidationError):
        _revalidated_contract_page(dict(truncated, next_after_entry_id="zzz.not_last"))


def test_a_compile_that_names_no_diagnostic_still_answers(monkeypatch):
    """#547. The zero-diagnostics fallback is reachable and was pinned by nothing.

    It is the one place the generic headline is correct — there is no authority
    message to forward — and it reaches the caller as both the diagnostic
    `message` and the envelope `error`.
    """
    from boomi_mcp.authoring.workflow import _COMPILE_GENERIC_MESSAGE
    from boomi_mcp.compiler.process_ir import pipeline
    from boomi_mcp.compiler.process_ir.diagnostics import ProcessIRCompileError

    def _raise_bare(*_a, **_k):
        raise ProcessIRCompileError(())

    # #178: patch the PRIVATE CORE, not the public entry. `build_artifact_
    # descriptors` now reaches the compiler through `parse_and_compile_process_
    # ir_v1` (one parse instead of a parse plus a compile), so a patch on the
    # public wrapper no longer intercepts THIS call site — the failure would
    # surface later, from materialization, with a different message, and the
    # test would be grading a different line than the one it names. Every route
    # reaches the core.
    monkeypatch.setattr(pipeline, "_compile_parsed_process_ir_v1", _raise_bare)

    fixture = json.loads(
        (
            Path(__file__).resolve().parent
            / "fixtures"
            / "authoring_contract"
            / "clean_room"
            / "decision_route_connector_map.json"
        ).read_text()
    )
    payload = integration_builder._compile_authoring(
        None, "p", {"authoring_request": fixture["request"]}
    )
    assert payload["_success"] is False
    diagnostics = payload.get("authoring_diagnostics") or []
    assert [d["message"] for d in diagnostics] == [_COMPILE_GENERIC_MESSAGE]
    assert _COMPILE_GENERIC_MESSAGE in str(payload.get("error"))
    # It BLOCKS. Downgrading a blocked compile to a warning kept every message
    # assertion above true while changing what the answer means.
    assert [d["severity"] for d in diagnostics] == ["error"]
    assert payload["error_code"] == "AUTHORING_COMPILE_BLOCKED"
    # ...and the headline is non-empty whatever the constant is reworded to.
    assert _COMPILE_GENERIC_MESSAGE.strip()


def test_an_unusable_filter_value_is_the_callers_mistake_not_an_outage():
    """#548. A bad value was reported as a retryable authority outage.

    `authoring_entry_id=["a"]` is unhashable, so the projection raised a
    `TypeError` that F9's broad handler labelled
    `AUTHORING_SCHEMA_SOURCE_UNAVAILABLE, retryable: true` — advice to retry an
    input that fails identically every time.
    """
    from boomi_mcp.errors import AUTHORING_SCHEMA_SOURCE_UNAVAILABLE, INVALID_INPUT

    for bad in ([], ["a"], {"k": 1}, {"a"}):
        payload = server.get_schema_template(schema_name=SELECTOR, authoring_entry_id=bad)
        assert payload["_success"] is False, bad
        assert payload["error_code"] == INVALID_INPUT, (bad, payload["error_code"])
        assert payload["error_code"] != AUTHORING_SCHEMA_SOURCE_UNAVAILABLE
        assert payload.get("retryable") is not True, bad

    # ...and a genuine outage keeps its own code, so the two are distinguishable.
    assert server.get_schema_template(schema_name=SELECTOR)["_success"] is True


def test_a_padded_connection_key_binds_rather_than_breaking_the_document():
    """#544. F6 began reading a field that had never been read, un-normalized.

    `config["connection_ref_key"]` is plain caller text, so `f"$ref:{key}"` turned
    surrounding whitespace into a value `ComponentSymbolV1` refuses — and the
    caller got a raw pydantic string naming an internal compiler model with
    their own value echoed back, the exact shape F1 had just removed one field
    over. It also struck `source`/`target` documents, where the key is
    irrelevant: a document that compiled before this field was read began
    failing.
    """
    from boomi_mcp.models.integration_models import IntegrationComponentSpec
    from boomi_mcp.recipes.materialization import build_symbol_table

    def _symbols(connection_key):
        config = {"action_type": "Get", "connector_type": "database"}
        if connection_key is not _ABSENT:
            config["connection_ref_key"] = connection_key
        return {
            s.ref: s
            for s in build_symbol_table(
                [
                    IntegrationComponentSpec(
                        key="conn", name="C", type="connector-settings",
                        config={"connector_type": "database"},
                    ),
                    IntegrationComponentSpec(
                        key="op", name="O", type="connector-action", config=config
                    ),
                ]
            ).symbols
        }

    # Padded binds to the same symbol as unpadded.
    assert _symbols("  conn  ")["$ref:op"].connection_ref == "$ref:conn"
    assert _symbols("conn")["$ref:op"].connection_ref == "$ref:conn"

    # Anything unusable leaves the symbol UNBOUND — which resolves to the typed
    # "connection not found" diagnostic, never a pydantic string.
    for unusable in ("", "   ", 7, ["a"], {"k": 1}, None, _ABSENT):
        assert _symbols(unusable)["$ref:op"].connection_ref is None, unusable


_ABSENT = object()


@pytest.mark.parametrize(
    "raised",
    [
        ValueError,
        TypeError,
        AttributeError,
        RuntimeError,
        KeyError,
        OSError,
        # The one that matters, and the one a type-narrowed handler could never
        # get right: this projection builds every entry and the page itself
        # through pydantic, so a malformed AUTHORITY raises exactly what a bad
        # FILTER raises. A registry contributing one bad row was blamed on the
        # caller, naming entry fields as if they were filters.
        "pydantic",
    ],
)
def test_a_dead_authority_keeps_its_own_code_whatever_it_raises(monkeypatch, raised):
    """#549. The caller-blame handler swallowed half the authority failures.

    Catching `ValueError`/`TypeError`/`AttributeError` inverted the very case
    the handler was added for: a dead registry raising one of them was
    reported as the CALLER's bad input, with `retryable` dropped. The suite
    pinned exactly one exception type and it happened to land in the safe half.
    """
    from boomi_mcp.authoring import process_ir_projection as projection
    from boomi_mcp.errors import AUTHORING_SCHEMA_SOURCE_UNAVAILABLE

    def _dead():
        if raised == "pydantic":
            from boomi_mcp.models.process_ir_authoring import (
                ProcessIRAuthoringQueryV1,
            )

            ProcessIRAuthoringQueryV1(limit="not-an-int")
        raise raised("registry is dead")

    monkeypatch.setattr("boomi_mcp.recipes.production_registry", _dead)
    projection.reset_process_ir_authoring_cache()
    try:
        payload = server.get_schema_template(schema_name=SELECTOR, category="recipe")
        assert payload["_success"] is False, raised
        assert payload["error_code"] == AUTHORING_SCHEMA_SOURCE_UNAVAILABLE, (
            raised,
            payload["error_code"],
        )
        assert payload.get("retryable") is True, raised
        assert "registry is dead" not in json.dumps(payload)
    finally:
        monkeypatch.undo()
        projection.reset_process_ir_authoring_cache()


def test_the_invalid_filter_envelope_says_what_it_claims_to_say():
    """#551. The new envelope's CONTENT was graded by nothing.

    Blanking `field`, blanking `rule`, and publishing the raw cause — which
    carries the caller's own value, the internal query model's name and a
    pydantic docs URL — all survived.
    """
    payload = server.get_schema_template(
        schema_name=SELECTOR, authoring_entry_id=["a"], category="diagnostic"
    )
    assert payload["_success"] is False

    # It blames the offending filter and ONLY that one.
    assert payload["invalid_parameter"] == "authoring_entry_id"
    assert "category" not in payload["invalid_parameter"]
    assert "authoring_entry_id" in payload["error"]

    blob = json.dumps(payload)
    assert "ProcessIRAuthoringQueryV1" not in blob
    assert "pydantic" not in blob.lower()
    assert "errors.pydantic.dev" not in blob
    # ...and no internal exception type either. An earlier version published
    # `ValidationError` in the served text, which is a Python implementation
    # detail, not a contract term.
    assert "ValidationError" not in blob
    # It routes through the same query-error envelope every other bad filter
    # uses, so the caller gets a recovery step rather than a bare rejection —
    # and specifically the RULE branch, not the facet branch. Two of the six
    # fields this can blame (`authoring_entry_id`, `after_entry_id`) are exact
    # values with no facet, so "fetch the facets and filter with a published
    # value" would be circular advice.
    assert payload["suggestion"]
    assert payload["suggestion"] == payload["rule"]
    assert "the type the contract" in payload["suggestion"]
    assert "allowed_values" not in payload


def test_the_connection_remediation_names_the_field_that_binds_it():
    """#552. Only the byte snapshot covered this, and it cannot verify truth.

    A caller who hits `..._CONNECTION_NOT_FOUND` has no other way to learn that
    the edge is authored as `config.connection_ref_key` on the operation component:
    the IR does not carry it and nothing else in the contract names it.
    """
    from boomi_mcp.compiler.process_ir.diagnostics import compiler_diagnostic_specs

    remediation = next(
        (spec.get("remediation") or "")
        for spec in compiler_diagnostic_specs()
        if spec["code"] == "PROCESS_IR_REFERENCE_CONNECTION_NOT_FOUND"
    )
    assert "config.connection_ref_key" in remediation, remediation
    assert "connector-settings" in remediation

    served = [
        entry
        for entry in build_process_ir_authoring_entries()
        if "config.connection_ref_key" in " ".join(entry.ordering_facts)
    ]
    assert [e.contract_entry_id for e in served] == [
        "diagnostic.process_ir_reference_connection_not_found"
    ]


def _primitive_connector_action_config(module_name):
    """The connector-action component config a primitive actually emits.

    Read from the primitive's SOURCE, not copied. Two #146 defects were the
    same mistake — reading `connection_key` and then `action_type`, neither of
    which any primitive writes into a component config — and both survived
    every round because the fixtures were hand-written with the same wrong
    assumption. A fixture agreeing with the code is no evidence when one author
    wrote both; the primitive is the authority.
    """
    import ast

    source = (
        Path(__file__).resolve().parents[1]
        / "src" / "boomi_mcp" / "patterns" / "primitives" / f"{module_name}.py"
    ).read_text()
    for node in ast.walk(ast.parse(source)):
        if not isinstance(node, ast.Dict):
            continue
        keys = [k.value for k in node.keys if isinstance(k, ast.Constant)]
        if "connection_ref_key" in keys and "operation_mode" in keys:
            literal = {}
            for key, value in zip(node.keys, node.values):
                if isinstance(key, ast.Constant) and isinstance(value, ast.Constant):
                    literal[key.value] = value.value
            return keys, literal
    raise AssertionError(f"no connector-action config literal found in {module_name}")


def test_a_plan_shaped_like_a_real_primitive_compiles_end_to_end():
    """Codex r24 P1 and QA #555 — one class, two fields.

    `build_symbol_table` read `connection_key` and
    `_connector_metadata_from_components` read `action_type`. Neither is written
    into a component config by any primitive: they write `connection_ref_key`,
    `operation_mode` and (for REST) `method`. So NO ProcessIR document over a
    real component plan compiled — `connector_call` was rejected as an
    unsupported action, and `source`/`target` failed with "operation symbol is
    missing derived connector metadata".
    """
    keys, literal = _primitive_connector_action_config("db_extract")
    assert "action_type" not in keys, keys
    assert literal.get("operation_mode") == "get", literal

    config = {
        "operation_mode": literal["operation_mode"],
        "connector_type": "database",
        "connection_ref_key": "src_conn",
    }
    request = _to_units({
        "authoring_request": {
            "contract_version": "1",
            "intent": {
                "intent_kind": "process_ir",
                "integration_name": "primitive-shaped",
                "component_key": "p",
                "components": [
                    {
                        "key": "src_conn", "name": "DB Conn",
                        "type": "connector-settings",
                        "config": {"connector_type": "database"},
                    },
                    {
                        "key": "src_op", "name": "DB Get",
                        "type": "connector-action", "config": config,
                    },
                ],
                "process_ir": {
                    "version": "1",
                    "body": {
                        "kind": "sequence",
                        "steps": [
                            {"kind": "connector_call", "operation_ref": "$ref:src_op"},
                            {"kind": "stop"},
                        ],
                    },
                },
            },
        }
    })
    for action in (
        integration_builder._plan_authoring,
        integration_builder._compile_authoring,
    ):
        payload = action(None, "p", request)
        assert payload["_success"] is True, (
            payload.get("error"),
            [d.get("cause_codes") for d in (payload.get("authoring_diagnostics") or [])],
        )


def test_the_action_type_derivation_matches_the_legacy_builder():
    """QA #555. `action_type` is derived family-conditionally, not read.

    The convention is the legacy builder's, not a second one invented here.
    """
    from boomi_mcp.authoring.workflow import _action_type_from_config

    assert _action_type_from_config(
        {"connector_type": "database", "operation_mode": "get"}
    ) == "Get"
    assert _action_type_from_config(
        {"connector_type": "database", "operation_mode": "send"}
    ) == "Send"
    # A REAL connector type, resolved through the canonical resolver. An
    # earlier version of this test invented `officialboomi-X-rest-prod`, which
    # no resolver recognises — the same mistake as the defects it guards.
    from boomi_mcp.categories.components.builders.connector_builder import (
        _resolve_rest_connector_type,
        _resolve_soap_client_connector_type,
    )

    _resolve_soap_client_connector_type_check = _resolve_soap_client_connector_type
    rest_family = _resolve_rest_connector_type("rest")
    assert rest_family, "no canonical REST connector type"
    assert _action_type_from_config(
        {"connector_type": rest_family, "operation_mode": "execute", "method": "patch"}
    ) == "PATCH"

    # SOAP: derived from `operation_mode` alone, because the SOAP builder
    # REJECTS `action_type` as an unsupported operation field — so for this
    # family the derivation is the only way through, not a convenience.
    assert _action_type_from_config(
        {"connector_type": "soap_client", "operation_mode": "execute"}
    ) == "EXECUTE"

    # `action_type` stays an accepted alias, because the legacy path accepts it.
    assert _action_type_from_config(
        {"connector_type": "wss", "action_type": "Listen"}
    ) == "Listen"
    # ...and a plan that declares nothing derivable claims nothing.
    assert _action_type_from_config({"connector_type": "database"}) is None

    # THE PROPERTY THAT ACTUALLY FAILED, pinned directly.
    #
    # #558 was not "a primitive writes a family": it was "the served contract
    # publishes an action as SUPPORTED while this derivation returns None for
    # it" — so a caller was told the action was available and then refused,
    # with the rejection citing the very entry that promised it.
    #
    # Six rounds were spent instead grading a sweep over primitive SOURCE, and
    # each round closed one spelling axis and opened the next: literal, module
    # constant, sibling import, external import, AnnAssign, attribute,
    # subscript, dict key, `**` spread, non-recursive glob, co-location. That
    # sweep is gone. The allowlist is the contract's own published authority,
    # it is a runtime registry rather than text, and it has no spelling axis at
    # all — so this cannot be defeated by how a family is written down.
    from boomi_mcp.compiler.process_ir.connector_capabilities import (
        connector_capability_rows,
    )

    published = {(row["family"], row["action"]) for row in connector_capability_rows()}
    assert len(published) >= 5, sorted(published)

    # A published family/action must be derivable from SOME component plan that
    # names it. The mode is whatever the family's own convention is — that is
    # what the derivation exists to translate.
    modes = {"Get": "get", "Send": "send", "EXECUTE": "execute"}
    underivable = []
    for family, action in sorted(published):
        config = {"connector_type": family, "operation_mode": modes.get(action, "execute")}
        if action not in modes:
            config["method"] = action  # REST carries the verb
        if _action_type_from_config(config) != action:
            underivable.append((family, action, _action_type_from_config(config)))
    assert underivable == [], underivable

    # ...and an unpublished family derives nothing FROM `operation_mode` alone,
    # which is the documented intent: `wss` and `http` are refused by absence
    # rather than by a branch.
    #
    # This is NOT the allowlist gate, and saying so matters — an earlier
    # version of this comment claimed it was, while forty lines above the same
    # function asserts `wss` + `action_type` deriving "Listen". `action_type`
    # is an accepted alias for ANY family, so the derivation can and does
    # produce actions the allowlist never published. What refuses them is
    # `lookup_capability` in `connector_resolution`, pinned by
    # `test_unsupported_family_action_pairs_are_rejected`. A maintainer reading
    # this for a gate would conclude that check is redundant; it is the one
    # doing the work.
    assert _action_type_from_config(
        {"connector_type": "wss", "operation_mode": "listen"}
    ) is None
    assert _action_type_from_config(
        {"connector_type": "ftp", "operation_mode": "execute"}
    ) is None



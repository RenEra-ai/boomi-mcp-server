"""Tests for the served ``design_doctrine`` knowledge surface (issue #86).

Pure-unit against boomi_mcp — no server import, no SDK calls. Covers the
catalog/entry schema, the get_schema_template dispatch + error envelope, the
list_capabilities compact index, the design-selection workflow routing, the
clause-/token-level abstraction filter (token-lint), the anti-template lint,
and the two contextual-wiring sites (build_from_archetype hints + build plan
warning).
"""

import re
import sys
from pathlib import Path
from unittest.mock import MagicMock, patch

_src = str(Path(__file__).resolve().parent.parent / "src")
if _src not in sys.path:
    sys.path.insert(0, _src)

from boomi_mcp.categories.meta_tools import (
    _valid_schema_names,
    get_schema_template_action,
    list_capabilities_action,
)
from boomi_mcp.errors import SCHEMA_NAME_UNSUPPORTED
from boomi_mcp.kb.design_doctrine import (
    CAPABILITY_STATUSES,
    CATEGORIES,
    DESIGN_DOCTRINE_ENTRIES,
    DESIGN_DOCTRINE_REQUIRED_FIELDS,
    PROVENANCE_LABELS,
    VERIFICATION_STATUSES,
    get_design_doctrine_catalog,
)
from boomi_mcp.models.integration_models import IntegrationComponentSpec
from boomi_mcp.categories.integration_builder import (
    _build_plan,
    _process_models_error_handling,
)


# Authoritative entry-name partition (issue #86 body + spec §6).
SEED_14 = {
    "wrapper_subprocess_separation",
    "connector_retry_design",
    "try_catch_placement",
    "error_routing_and_dlq",
    "notification_logging",
    "idempotency_and_duplicates",
    "incremental_watermark",
    "caching_lookup_join",
    "content_based_routing",
    "combine_split_flow_control",
    "config_externalization",
    "component_profile_reuse",
    "connector_selection",
    "platform_selection",
}
NET_NEW_16 = {
    "process_route_fanout",
    "process_mode_and_options_selection",
    "business_rules_vs_decision",
    "async_queue_decoupling",
    "reliable_and_sequential_messaging",
    "transaction_saga_compensation",
    "data_confidentiality_layering",
    "state_persistence_parking_lot",
    "change_data_capture_strategy",
    "bidirectional_sync_conflict_and_circularity",
    "api_pagination_contract",
    "cross_cutting_framework_services",
    "inline_vs_branch_cache_invocation",
    "microservice_vs_monolith_decomposition",
    "native_over_custom_scripting",
    "migration_pattern_templating",
}
TESTING_8 = {
    "document_tracking_as_monitoring",
    "unit_testing_via_swappable_data_source",
    "test_mode_workaround_for_listener_connectors",
    "test_harness_process_pattern",
    "mock_endpoint_process_design",
    "test_suite_master_process_automation",
    "regression_test_path_coverage",
    "parallel_day_in_the_life_testing",
}

# Mechanic tokens that must never leak into served doctrine prose (spec §5).
FORBIDDEN_TOKENS = (
    "Batch Count=1",
    "Find Changes step",
    "Add Cached Data",
    "simulated-profile",
    "FAILURE_MSG",
    "Data Passthrough",
    "retryCount",
    "catcherrors",
    "allowSimultaneous",
    "updateRunDates",
)

_UUID_RE = re.compile(
    r"[0-9a-fA-F]{8}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{12}"
)
_RAW_XML_RE = re.compile(r"<\?xml|</?[a-zA-Z][\w:-]*>")
_SQL_CRUD_RE = re.compile(
    r"(?i)\b(select\s+.+\s+from|insert\s+into|update\s+\w+\s+set|delete\s+from)\b"
)


def _flatten_strings(node):
    """Yield every string anywhere in a nested dict/list structure.

    Skips ``docs_page_key`` values: those are help.boomi.com documentation
    citation URLs (the #86 acceptance criterion mandates citing a docs page key
    for corroborated claims) and legitimately contain a UUID slug, so they are
    not doctrine prose and must not trip the anti-template UUID guard.
    """
    if isinstance(node, str):
        yield node
    elif isinstance(node, dict):
        for key, value in node.items():
            if key == "docs_page_key":
                continue
            yield from _flatten_strings(value)
    elif isinstance(node, list):
        for value in node:
            yield from _flatten_strings(value)


def _served_prose_blob():
    return "\n".join(_flatten_strings(get_design_doctrine_catalog()))


# ---------------------------------------------------------------------------
# Catalog retrieval + entry schema
# ---------------------------------------------------------------------------


def test_catalog_returns_38_entries_with_safety_flags():
    result = get_schema_template_action(schema_name="design_doctrine")
    assert result["_success"] is True
    assert result["schema_name"] == "design_doctrine"
    assert result["entry_count"] == 38
    assert len(result["entries"]) == 38
    assert result["raw_xml_exposed"] is False
    assert result["boomi_mutation"] is False
    assert result["read_only"] is True
    assert "entry_schema" in result


def test_entry_count_partition_14_16_8():
    names = set(DESIGN_DOCTRINE_ENTRIES)
    assert len(names) == 38
    assert SEED_14 <= names
    assert NET_NEW_16 <= names
    assert TESTING_8 <= names
    # The three sources are disjoint and together account for all 38.
    assert SEED_14 | NET_NEW_16 | TESTING_8 == names
    assert not (SEED_14 & NET_NEW_16)
    assert not (NET_NEW_16 & TESTING_8)
    assert not (SEED_14 & TESTING_8)


def test_every_entry_has_required_fields_and_valid_vocabularies():
    for name, entry in DESIGN_DOCTRINE_ENTRIES.items():
        for field in DESIGN_DOCTRINE_REQUIRED_FIELDS:
            assert field in entry, f"{name} missing field {field!r}"
        assert entry["name"] == name
        for prose in ("problem", "boomi_shape_mapping", "when_to_use", "when_not_to_use"):
            assert isinstance(entry[prose], str) and entry[prose].strip(), (
                f"{name}.{prose} must be non-empty prose"
            )
        assert entry["capability_status"] in CAPABILITY_STATUSES, name
        assert entry["verification_status"] in VERIFICATION_STATUSES, name
        assert entry["provenance"] in PROVENANCE_LABELS, name
        assert entry["category"] in CATEGORIES, name
        assert isinstance(entry["mutual_exclusion"], list), name
        assert isinstance(entry["cross_refs"], list), name


def test_cross_refs_resolve_to_real_entries():
    for name, entry in DESIGN_DOCTRINE_ENTRIES.items():
        for ref in entry["cross_refs"]:
            assert ref in DESIGN_DOCTRINE_ENTRIES, (
                f"{name} cross_refs unknown entry {ref!r}"
            )
            assert ref != name, f"{name} cross-references itself"


# ---------------------------------------------------------------------------
# Single-entry lookup + error envelope
# ---------------------------------------------------------------------------


def test_single_entry_lookup():
    result = get_schema_template_action(
        schema_name="design_pattern:document_tracking_as_monitoring"
    )
    assert result["_success"] is True
    assert result["read_only"] is True
    assert result["design_pattern"]["name"] == "document_tracking_as_monitoring"
    assert result["design_pattern"]["category"] == "observability"


def test_unknown_pattern_returns_schema_name_unsupported():
    result = get_schema_template_action(schema_name="design_pattern:__bogus__")
    assert result["_success"] is False
    assert result["error_code"] == SCHEMA_NAME_UNSUPPORTED
    assert "valid_design_patterns" in result
    assert "wrapper_subprocess_separation" in result["valid_design_patterns"]
    # The error envelope must NOT dump the full catalog.
    assert "entries" not in result


def test_valid_schema_names_includes_design_doctrine_and_patterns():
    names = _valid_schema_names()
    assert "design_doctrine" in names
    pattern_names = [n for n in names if n.startswith("design_pattern:")]
    assert len(pattern_names) == 38
    assert "design_pattern:wrapper_subprocess_separation" in names


# ---------------------------------------------------------------------------
# Consolidation + mutual exclusion (adversarial-critique requirements)
# ---------------------------------------------------------------------------


def test_document_tracking_is_single_and_cross_referenced():
    # Exactly one observability-as-monitoring entry — not triplicated.
    assert "document_tracking_as_monitoring" in DESIGN_DOCTRINE_ENTRIES
    observability = [
        n for n, e in DESIGN_DOCTRINE_ENTRIES.items()
        if e["category"] == "observability"
    ]
    assert observability == ["document_tracking_as_monitoring"]
    entry = DESIGN_DOCTRINE_ENTRIES["document_tracking_as_monitoring"]
    assert "notification_logging" in entry["cross_refs"]
    assert "idempotency_and_duplicates" in entry["cross_refs"]


def test_notification_logging_is_live_verified_and_emittable():
    # Issue #89 M4.5.4: the Notify-on-catch-path entry is now live-verified and
    # emittable (the builder emits a verified Notify step on the wired catch leg).
    entry = DESIGN_DOCTRINE_ENTRIES["notification_logging"]
    assert entry["verification_status"] == "live_verified"
    assert entry["capability_status"] == "emittable_today"
    assert entry["provenance"] == "live_verified"


def test_wrapper_subprocess_separation_is_emittable():
    # Issue #90 M4.5.5: the wrapper-parent (facade) + subprocess pattern is now
    # emittable (WrapperSubprocessBuilder emits a thin parent calling children
    # via standalone Process Call).
    entry = DESIGN_DOCTRINE_ENTRIES["wrapper_subprocess_separation"]
    assert entry["capability_status"] == "emittable_today"


def test_fifo_parallel_mutual_exclusion_is_symmetric():
    fifo = DESIGN_DOCTRINE_ENTRIES["reliable_and_sequential_messaging"]
    parallel = DESIGN_DOCTRINE_ENTRIES["combine_split_flow_control"]
    assert any("combine_split_flow_control" in m for m in fifo["mutual_exclusion"])
    assert any(
        "reliable_and_sequential_messaging" in m for m in parallel["mutual_exclusion"]
    )


def test_spec10_mutual_exclusion_entries_populated():
    for name in (
        "reliable_and_sequential_messaging",
        "combine_split_flow_control",
        "bidirectional_sync_conflict_and_circularity",
        "inline_vs_branch_cache_invocation",
        "process_mode_and_options_selection",
    ):
        assert DESIGN_DOCTRINE_ENTRIES[name]["mutual_exclusion"], (
            f"{name} must carry a first-class mutual_exclusion tradeoff"
        )


# ---------------------------------------------------------------------------
# Abstraction filter: token-lint + anti-template lint over served prose
# ---------------------------------------------------------------------------


def test_token_lint_rejects_mechanic_tokens():
    blob = _served_prose_blob().lower()
    hits = [tok for tok in FORBIDDEN_TOKENS if tok.lower() in blob]
    assert hits == [], f"mechanic tokens leaked into served prose: {hits}"


def test_anti_template_no_raw_artifacts():
    blob = _served_prose_blob()
    assert not _RAW_XML_RE.search(blob), "raw XML must not appear in doctrine prose"
    assert "```" not in blob, "code fences must not appear in doctrine prose"
    assert not _UUID_RE.search(blob), "component UUIDs must not appear in doctrine prose"
    sql = _SQL_CRUD_RE.search(blob)
    assert sql is None, f"reusable SQL must not appear in doctrine prose: {sql!r}"


# ---------------------------------------------------------------------------
# list_capabilities compact index
# ---------------------------------------------------------------------------


def test_list_capabilities_compact_index():
    catalog = list_capabilities_action()
    assert "design_doctrine" in catalog
    dd = catalog["design_doctrine"]
    assert dd["entry_count"] == 38
    assert "get_schema_template" in dd["surface"]
    assert "design_pattern:<name>" in dd["pattern_surface"]
    assert len(dd["index"]) == 38
    # Index rows are compact — name/category/capability_status only, no prose.
    for row in dd["index"]:
        assert set(row.keys()) == {"name", "category", "capability_status"}


def test_design_doctrine_index_survives_available_tools_filtering():
    catalog = list_capabilities_action(available_tools={"build_integration"})
    assert "design_doctrine" in catalog
    assert catalog["design_doctrine"]["entry_count"] == 38


# ---------------------------------------------------------------------------
# Design-selection workflow routes through design_doctrine before archetypes
# ---------------------------------------------------------------------------


def test_workflow_routes_through_design_doctrine_before_archetypes():
    wf = list_capabilities_action()["workflows"]["build_integration_from_description"]
    steps = wf["steps"]
    joined = "\n".join(steps)
    assert "design_doctrine" in joined, "workflow must reference design_doctrine"

    def _first_index(needle):
        return next(i for i, s in enumerate(steps) if needle in s)

    dd_idx = _first_index("design_doctrine")
    arch_idx = _first_index("list_integration_archetypes(")
    assert dd_idx < arch_idx, "design_doctrine consult must precede archetype discovery"
    # And it must come after profile selection.
    assert _first_index("list_boomi_profiles(") < dd_idx


def test_workflow_schema_template_view_also_routes_through_doctrine():
    wf = get_schema_template_action(
        schema_name="workflow:build_integration_from_description"
    )["workflow"]
    assert any("design_doctrine" in s for s in wf["steps"])


# ---------------------------------------------------------------------------
# Contextual wiring: build plan warns when a process models no error handling
# ---------------------------------------------------------------------------

_PAGINATE = "boomi_mcp.categories.integration_builder.paginate_metadata"


def _process_comp(config):
    return IntegrationComponentSpec(
        key="p1", type="process", action="create", name="DemoProcess", config=config
    )


def _plan_config(comp):
    return {
        "conflict_policy": "reuse",
        "integration_spec": {
            "version": "1.0",
            "name": "demo",
            "components": [comp.model_dump()],
        },
    }


@patch(_PAGINATE, return_value=[])
def test_build_plan_warns_when_process_lacks_error_handling(_mock_pag):
    comp = _process_comp({"name": "DemoProcess"})
    plan = _build_plan(MagicMock(), _plan_config(comp))
    assert plan["_success"] is True
    warnings = plan["warnings"] or []
    joined = "\n".join(warnings)
    assert "try_catch_placement" in joined
    assert "error_routing_and_dlq" in joined


_STUB_ID = "00000000-0000-0000-0000-stubbed00001"


def _stub(key, ctype, config, **extra):
    # action='update' + component_id keeps the stub on the update path so
    # plan-time builder validation (create/create_clone only) is skipped.
    return IntegrationComponentSpec(
        key=key, type=ctype, action="update", component_id=_STUB_ID,
        name=key.replace("_", " ").title(), config=config, **extra
    ).model_dump()


def _structured_dlq_plan_config():
    """A minimal VALID structured process-flow spec with a real DLQ
    (document_cache_ref), so the process step is authored (planned_action
    'create') and the design-doctrine warning genuinely exercises the
    reliability gate — not the validation-error short-circuit. Codex review P3.
    """
    main = IntegrationComponentSpec(
        key="main_process", type="process", action="create", name="Main Process",
        depends_on=[
            "db_connection", "db_query_operation",
            "target_rest_connection", "target_rest_operation", "dlq_document_cache",
        ],
        config={
            "process_kind": "database_to_api_sync",
            "source": {
                "connector_type": "database",
                "connection_id": "$ref:db_connection",
                "operation_id": "$ref:db_query_operation",
                "action_type": "Get",
            },
            "transform": {"mode": "passthrough"},
            "target": {
                "connector_type": "rest",
                "connection_id": "$ref:target_rest_connection",
                "operation_id": "$ref:target_rest_operation",
                "action_type": "POST",
            },
            "reliability": {
                "retry_count": 0,
                "dlq": {"mode": "document_cache_ref",
                        "document_cache_id": "$ref:dlq_document_cache"},
            },
        },
    ).model_dump()
    components = [
        _stub("db_connection", "connector-settings",
              {"connector_type": "database", "name": "Db Connection"}),
        _stub("db_query_operation", "connector-action",
              {"connector_type": "database", "operation_mode": "get",
               "name": "Db Query Operation"}),
        _stub("target_rest_connection", "connector-settings",
              {"connector_type": "rest", "name": "Target Rest Connection"}),
        _stub("target_rest_operation", "connector-action",
              {"connector_type": "rest", "operation_mode": "execute",
               "method": "POST", "path": "/v1/stub",
               "component_name": "Target Rest Operation",
               "connection_ref_key": "target_rest_connection"},
              depends_on=["target_rest_connection"]),
        _stub("dlq_document_cache", "documentcache", {"name": "Dlq Document Cache"}),
        main,
    ]
    return {
        "conflict_policy": "reuse",
        "integration_spec": {"version": "1.0", "name": "demo", "components": components},
    }


@patch(_PAGINATE, return_value=[])
def test_build_plan_no_warning_for_structured_process_with_dlq(_mock_pag):
    # Structured route (process_kind set) honors config.reliability.dlq — and
    # the step must actually be authored, not validation-errored, for this to
    # exercise the gate.
    plan = _build_plan(MagicMock(), _structured_dlq_plan_config())
    assert plan["_success"] is True
    main_step = next(s for s in plan["steps"] if s["key"] == "main_process")
    assert main_step.get("validation_error") is None
    assert main_step["planned_action"] == "create"
    joined = "\n".join(plan["warnings"] or [])
    assert "try_catch_placement" not in joined
    assert "error_routing_and_dlq" not in joined


@patch(_PAGINATE, return_value=[])
def test_build_plan_no_warning_for_legacy_process_with_catch_shape(_mock_pag):
    # Legacy route — a Try/Catch catch shape is the trusted error-handling
    # evidence (raw XML / shapes), not the reliability block.
    comp = _process_comp(
        {"name": "DemoProcess", "shapes": [{"shapetype": "catcherrors"}]}
    )
    plan = _build_plan(MagicMock(), _plan_config(comp))
    joined = "\n".join(plan["warnings"] or [])
    assert "try_catch_placement" not in joined


def test_process_models_error_handling_predicate():
    """Directly exercise the gate predicate (independent of the _build_plan
    planned_action filter) so the structured-vs-legacy distinction is proven.
    Codex review P2."""

    def comp(config):
        return IntegrationComponentSpec(
            key="p", type="process", action="create", name="P", config=config
        )

    # Structured route (process_kind set) trusts the supported DLQ modes with a
    # valid 0..5 retry count — the configs that actually emit a Try/Catch.
    assert _process_models_error_handling(
        comp({"process_kind": "database_to_api_sync",
              "reliability": {"retry_count": 0, "dlq": {"mode": "document_cache_ref"}}})
    )
    assert _process_models_error_handling(
        comp({"process_kind": "database_to_api_sync",
              "reliability": {"retry_count": 0, "dlq": {"mode": "error_subprocess_ref"}}})
    )
    # Issue #88: retry_count 1..5 with a supported DLQ mode is now un-gated and
    # emits a Try/Catch → counts as modeled error handling.
    assert _process_models_error_handling(
        comp({"process_kind": "database_to_api_sync",
              "reliability": {"retry_count": 2, "dlq": {"mode": "document_cache_ref"}}})
    )
    # retry_count > 0 without a wired DLQ catch path is gated (no Try/Catch
    # emitted) → NOT error handling.
    assert not _process_models_error_handling(
        comp({"process_kind": "database_to_api_sync",
              "reliability": {"retry_count": 2, "dlq": {"mode": "disabled"}}})
    )
    # retry_count out of range (6) → gated → NOT error handling.
    assert not _process_models_error_handling(
        comp({"process_kind": "database_to_api_sync",
              "reliability": {"retry_count": 6, "dlq": {"mode": "document_cache_ref"}}})
    )
    # Structured but DLQ disabled → no error handling.
    assert not _process_models_error_handling(
        comp({"process_kind": "database_to_api_sync",
              "reliability": {"retry_count": 0, "dlq": {"mode": "disabled"}}})
    )
    # Legacy route ignores the reliability block entirely.
    assert not _process_models_error_handling(
        comp({"reliability": {"retry_count": 0, "dlq": {"mode": "document_cache_ref"}}})
    )
    # Legacy route trusts raw-XML / shape catch evidence.
    assert _process_models_error_handling(comp({"shapes": [{"shapetype": "catcherrors"}]}))
    assert _process_models_error_handling(comp({"xml": "<bns:shape shapetype=\"trycatch\"/>"}))
    # Non-dict config does not crash.
    assert not _process_models_error_handling(comp({}))


@patch(_PAGINATE, return_value=[])
def test_build_plan_warns_for_legacy_process_with_stray_reliability(_mock_pag):
    # Codex review P2 regression: a legacy process (no process_kind) carrying a
    # reliability block the legacy path IGNORES must still warn — the stray
    # reliability block does not count as modeled error handling.
    comp = _process_comp(
        {
            "name": "DemoProcess",
            "reliability": {"retry_count": 0, "dlq": {"mode": "document_cache_ref"}},
        }
    )
    plan = _build_plan(MagicMock(), _plan_config(comp))
    joined = "\n".join(plan["warnings"] or [])
    assert "try_catch_placement" in joined
    assert "error_routing_and_dlq" in joined


# ---------------------------------------------------------------------------
# Corroboration backlog (acceptance criterion)
# ---------------------------------------------------------------------------


def test_corroboration_backlog_present_and_verified():
    catalog = get_design_doctrine_catalog()
    backlog = catalog["corroboration_backlog"]
    assert len(backlog) == 5
    backlog_entries = {item["entry"] for item in backlog}
    assert backlog_entries <= set(DESIGN_DOCTRINE_ENTRIES)
    # Every claim was checked via search_boomi_docs. Status reflects the
    # outcome: docs_corroborated claims cite a help.boomi.com page key; the rest
    # stay course_unverified because the KB does not cover the claim. Every item
    # records a verification result.
    for item in backlog:
        assert item["status"] in ("course_unverified", "docs_corroborated")
        assert item.get("verification"), f"{item['entry']} missing verification note"
        if item["status"] == "docs_corroborated":
            assert item.get("docs_page_key", "").startswith("https://help.boomi.com")
    statuses = {item["entry"]: item["status"] for item in backlog}
    # Fully corroborated by the official KB:
    assert statuses["combine_split_flow_control"] == "docs_corroborated"  # deferred-batch return
    assert statuses["connector_retry_design"] == "docs_corroborated"  # Try/Catch retry range + timing (#88)
    # Only partially documented (runtime-scoped yes; "shared across all users" no)
    # or not covered at all → claim retained as course_unverified, never overstated:
    assert statuses["test_mode_workaround_for_listener_connectors"] == "course_unverified"
    assert statuses["change_data_capture_strategy"] == "course_unverified"
    assert statuses["document_tracking_as_monitoring"] == "course_unverified"

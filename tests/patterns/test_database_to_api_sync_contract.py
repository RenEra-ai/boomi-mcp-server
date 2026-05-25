"""Tests for the contract-only database_to_api_sync archetype (issues #21, #44).

Issue #44 (M2.1a) replaced the legacy ``transform.mappings`` /
``transform.payload_template`` / ``transform.script_slots`` surface with:

  * caller-declared DB read result fields under
    ``source.read_operation.result_schema``,
  * caller-supplied JSON profile tree under ``target.payload_profile``, and
  * discriminated typed transform operations under ``transform.operations``.

The fixtures + tests below cover both the legacy contract guarantees (kept
intact by issue #44) and the new typed schema surface.
"""

from __future__ import annotations

import json
from typing import Any, Dict, Iterator, List, Tuple

from boomi_mcp.categories.integration_authoring import (
    build_from_archetype_action,
    get_integration_archetype_action,
)
from boomi_mcp.patterns import PatternKind, PatternRegistry
from boomi_mcp.patterns.archetypes import (
    DatabaseToApiSyncArchetype,
    DatabaseToApiSyncParameters,
)


# ---------------------------------------------------------------------------
# Inline fixtures (mirrors test_stub_archetype.py convention)
# ---------------------------------------------------------------------------


def _valid_minimal() -> Dict[str, Any]:
    """Smallest valid payload: create-mode DB + create-mode REST (no auth)."""
    return {
        "naming": {
            "integration_name": "demo-db-to-api-sync",
            "component_prefix": "DEMO",
        },
        "source": {
            "binding": {
                "mode": "create",
                "settings": {
                    "driver": "microsoft_jdbc",
                    "auth_mode": "username_password",
                    "host": "db.internal",
                    "database": "AppDB",
                    "username": "svc_sync",
                    "credential_ref": "secrets/db/svc_sync",
                },
            },
            "read_operation": {
                "sql": "<<user-authored DB read statement>>",
                "result_schema": {
                    "fields": [
                        {"name": "source_a", "data_type": "character"},
                    ],
                },
            },
        },
        "target": {
            "binding": {
                "mode": "create",
                "settings": {
                    "base_url": "https://api.example.com",
                    "auth_mode": "none",
                },
            },
            "send_request": {
                "method": "POST",
                "path": "/v1/items",
            },
            "payload_profile": {
                "format": "json",
                "root": {
                    "name": "Root",
                    "kind": "object",
                    "children": [
                        {
                            "name": "target_a",
                            "kind": "simple",
                            "data_type": "character",
                        },
                    ],
                },
            },
        },
        "transform": {
            "operations": [
                {
                    "operation_type": "direct",
                    "source_field": "source_a",
                    "target_path": "Root/target_a",
                },
            ],
        },
        "execution": {
            "trigger": {"mode": "manual"},
        },
        "reliability": {
            "retry": {"max_attempts": 1},
            "dlq": {"enabled": False},
            "error_classifier": {},
        },
    }


def _valid_full() -> Dict[str, Any]:
    """Fuller payload: scheduled, watermark, retry, DLQ enabled, run metadata.

    Demonstrates the full surface: result_schema with two declared fields,
    nested JSON payload profile (root object + repeating array), one direct
    + one map_function + one map_script transform operation, and a watermark
    referencing a declared source field.
    """
    return {
        "naming": {
            "integration_name": "demo-db-to-api-incremental",
            "component_prefix": "DEMO-INC",
            "component_names": {"db_connection": "DEMO-INC DB Source"},
            "folder_path": "Integrations/CRM/Sync",
            "runtime_hints": {"atom_pool": "primary"},
        },
        "source": {
            "binding": {
                "mode": "reuse",
                "component_id": "<<existing connector id>>",
            },
            "read_operation": {
                "sql": "<<user-authored incremental DB read statement>>",
                "result_schema": {
                    "fields": [
                        {
                            "name": "source_a",
                            "data_type": "character",
                            "required": True,
                        },
                        {"name": "source_b", "data_type": "datetime"},
                        {"name": "source_c", "data_type": "number"},
                    ],
                },
                "parameters": [
                    {"name": "<<bind parameter name>>", "direction": "in"},
                ],
                "batch_size": 500,
                "fetch_size": 200,
                "max_rows": 10000,
            },
        },
        "target": {
            "binding": {
                "mode": "create",
                "settings": {
                    "base_url": "https://api.example.com",
                    "auth_mode": "bearer_token",
                    "credential_ref": "secrets/rest/bearer",
                    "default_headers": {"Accept": "application/json"},
                },
            },
            "send_request": {
                "method": "POST",
                "path": "/v1/customers",
                "query_parameters": [
                    {"name": "since", "value_source": "watermark"},
                ],
                "expected_status_codes": [200, 202],
            },
            "payload_profile": {
                "format": "json",
                "root": {
                    "name": "Root",
                    "kind": "object",
                    "children": [
                        {
                            "name": "target_a",
                            "kind": "simple",
                            "data_type": "character",
                            "required": True,
                        },
                        {
                            "name": "target_b",
                            "kind": "simple",
                            "data_type": "datetime",
                        },
                        {
                            "name": "list",
                            "kind": "array",
                            "children": [
                                {
                                    "name": "target_c",
                                    "kind": "simple",
                                    "data_type": "number",
                                },
                            ],
                        },
                    ],
                },
            },
        },
        "transform": {
            "operations": [
                {
                    "operation_type": "direct",
                    "source_field": "source_a",
                    "target_path": "Root/target_a",
                    "documentation_hint": "carry first column verbatim",
                },
                {
                    "operation_type": "map_function",
                    "function_type": "trim",
                    "inputs": ["source_b"],
                    "target_path": "Root/target_b",
                },
                {
                    "operation_type": "map_script",
                    "script_slot": "enrich_row",
                    "language": "groovy2",
                    "inputs": ["source_c"],
                    "outputs": ["Root/list[]/target_c"],
                },
            ],
        },
        "execution": {
            "trigger": {
                "mode": "scheduled",
                "schedule": {"cron": "<<cron expression>>", "timezone": "UTC"},
            },
            "watermark": {
                "field": "source_b",
                "kind": "timestamp",
                "persistence": "dpp",
            },
            "run_metadata": {"owner": "crm-team"},
        },
        "reliability": {
            "retry": {
                "max_attempts": 5,
                "backoff": "exponential",
                "initial_interval_seconds": 2,
            },
            "dlq": {
                "enabled": True,
                "target": {"kind": "queue", "address": "<<dlq queue address>>"},
            },
            "error_classifier": {
                "custom_rules": ["rate_limit_exhausted"],
            },
        },
    }


# ---------------------------------------------------------------------------
# Recursive JSON-Schema walker (covers $defs and nested properties)
# ---------------------------------------------------------------------------


def _walk_properties(
    schema: Dict[str, Any],
) -> Iterator[Tuple[str, str, Dict[str, Any]]]:
    """Yield (location, property_name, property_schema) for every property.

    Location is the dotted path of $defs the property lives under (top-level
    properties yield location="<root>"). Covers nested $defs so the new
    archetype's fully-nested schema is walked end-to-end.
    """
    root_props = schema.get("properties") or {}
    for name, prop_schema in root_props.items():
        yield "<root>", name, prop_schema
    defs = schema.get("$defs") or schema.get("definitions") or {}
    for def_name, def_schema in defs.items():
        for name, prop_schema in (def_schema.get("properties") or {}).items():
            yield def_name, name, prop_schema


# ---------------------------------------------------------------------------
# Registry + describe
# ---------------------------------------------------------------------------


def test_registry_discovers_database_to_api_sync():
    registry = PatternRegistry.from_package("boomi_mcp.patterns")
    cls = registry.get("database_to_api_sync", kind=PatternKind.ARCHETYPE)
    assert cls is DatabaseToApiSyncArchetype


def test_get_archetype_exposes_machine_readable_schema():
    result = get_integration_archetype_action("database_to_api_sync")
    assert result["_success"] is True
    arch = result["archetype"]

    assert arch["metadata"]["name"] == "database_to_api_sync"
    assert arch["metadata"]["kind"] == "archetype"

    schema = arch["parameter_schema"]
    assert schema["type"] == "object"
    assert schema["additionalProperties"] is False
    expected_sections = {
        "naming",
        "source",
        "target",
        "transform",
        "execution",
        "reliability",
    }
    assert expected_sections <= set(schema["properties"].keys())
    assert expected_sections <= set(schema.get("required", []))


def test_every_schema_property_has_non_empty_description():
    schema = DatabaseToApiSyncArchetype.parameter_schema()
    missing: List[str] = []
    for location, name, prop_schema in _walk_properties(schema):
        desc = prop_schema.get("description")
        if not (isinstance(desc, str) and desc.strip()):
            missing.append(f"{location}.{name}")
    assert not missing, (
        "These properties are missing a non-empty description: " + ", ".join(missing)
    )


# ---------------------------------------------------------------------------
# Valid fixtures
# ---------------------------------------------------------------------------


def test_valid_minimal_fixture_validates():
    params = DatabaseToApiSyncArchetype.validate_parameters(_valid_minimal())
    assert isinstance(params, DatabaseToApiSyncParameters)


def test_valid_full_fixture_validates():
    params = DatabaseToApiSyncArchetype.validate_parameters(_valid_full())
    assert isinstance(params, DatabaseToApiSyncParameters)


def test_valid_build_emits_zero_component_contract_spec():
    result = build_from_archetype_action("database_to_api_sync", _valid_minimal())
    assert result["_success"] is True, result
    assert result["boomi_mutation"] is False
    assert result["raw_xml_exposed"] is False
    spec = result["integration_spec"]
    assert spec["components"] == []
    assert spec["mode"] == "redesign"
    rules = spec["validation_rules"]
    assert rules["contract_only"] is True
    assert rules["component_count"] == 0
    assert rules["raw_xml_exposed"] is False
    assert rules["boomi_mutation"] is False
    assert rules["requires_m2_9_for_executable_components"] is True
    # Issue #44 M2.1a additions:
    assert rules["metadata_version"] == "0.2.0"
    assert "caller-declared" in rules["profile_schema_strategy"]
    assert "caller-supplied" in rules["profile_schema_strategy"]
    assert rules["transform_routes"] == {
        "direct": "#26",
        "map_function": "#40",
        "map_script": "#41",
        "xslt": "#42 (rejected in M2)",
    }
    assert spec["name"] == "demo-db-to-api-sync"


def test_valid_build_emits_transform_flow_with_typed_metadata():
    """Issue #44: emit_spec must surface source schema, target leaf paths, and
    operation summaries on the transform flow entry so downstream issues
    (#26/#40/#41) can choose the right rung from spec metadata alone."""
    result = build_from_archetype_action("database_to_api_sync", _valid_full())
    assert result["_success"] is True, result
    spec = result["integration_spec"]
    transform_flow = next(f for f in spec["flows"] if f["key"] == "transform")

    src_schema = transform_flow["source_schema"]
    assert src_schema["field_count"] == 3
    assert {f["name"] for f in src_schema["fields"]} == {
        "source_a",
        "source_b",
        "source_c",
    }

    target_profile = transform_flow["target_payload_profile"]
    assert target_profile["format"] == "json"
    assert target_profile["root_name"] == "Root"
    leaf_paths = {leaf["path"] for leaf in target_profile["leaves"]}
    # Array repetition surfaces as [] on the array segment per the M2.1a path
    # strategy ("Represent array repetition in the logical path with [] on the
    # array segment, e.g. Root/list[]/key").
    assert leaf_paths == {"Root/target_a", "Root/target_b", "Root/list[]/target_c"}

    ops = transform_flow["operations"]
    assert len(ops) == 3
    by_type = {op["operation_type"]: op for op in ops}
    # direct: keeps source_field, target_path, and the documentation hint.
    assert by_type["direct"]["future_builder_issue"] == "#26"
    assert by_type["direct"]["source_field"] == "source_a"
    assert by_type["direct"]["target_path"] == "Root/target_a"
    assert by_type["direct"]["documentation_hint"] == "carry first column verbatim"
    # map_function: codex review P2a — emit full operand structure (inputs[],
    # target_path) so #40 can compile from the spec without re-reading the
    # original archetype payload.
    assert by_type["map_function"]["future_builder_issue"] == "#40"
    assert by_type["map_function"]["function_type"] == "trim"
    assert by_type["map_function"]["inputs"] == ["source_b"]
    assert by_type["map_function"]["input_count"] == 1
    assert by_type["map_function"]["target_path"] == "Root/target_b"
    # map_script: same — emit inputs[], outputs[], script_component_ref so
    # #41 can compile from the spec. script_body stays out per plan.
    assert by_type["map_script"]["future_builder_issue"] == "#41"
    assert by_type["map_script"]["script_slot"] == "enrich_row"
    assert by_type["map_script"]["language"] == "groovy2"
    assert by_type["map_script"]["inputs"] == ["source_c"]
    assert by_type["map_script"]["outputs"] == ["Root/list[]/target_c"]
    assert by_type["map_script"]["input_count"] == 1
    assert by_type["map_script"]["output_count"] == 1
    # Defense-in-depth: the spec must not echo executable script bodies.
    assert by_type["map_script"]["has_script_body"] is False
    assert "script_body" not in by_type["map_script"]
    # script_component_ref was not supplied in the fixture, so it's omitted.
    assert "script_component_ref" not in by_type["map_script"]


def test_map_function_summary_surfaces_inputs_and_parameters():
    """Codex review P2a follow-up: when map_function declares inputs and a
    parameters dict, both must round-trip into the spec for #40 to compile."""
    payload = _valid_minimal()
    payload["source"]["read_operation"]["result_schema"]["fields"] = [
        {"name": "source_a", "data_type": "character"},
        {"name": "source_b", "data_type": "character"},
    ]
    payload["transform"]["operations"] = [
        {
            "operation_type": "map_function",
            "function_type": "concat",
            "inputs": ["source_a", "source_b"],
            "target_path": "Root/target_a",
            "parameters": {"separator": ", "},
        },
    ]
    result = _build(payload)
    assert result["_success"] is True
    transform_flow = next(
        f for f in result["integration_spec"]["flows"] if f["key"] == "transform"
    )
    op = transform_flow["operations"][0]
    assert op["inputs"] == ["source_a", "source_b"]
    assert op["input_count"] == 2
    assert op["parameters"] == {"separator": ", "}


def test_map_script_summary_surfaces_script_component_ref():
    """Codex review P2a follow-up: when map_script declares
    script_component_ref, it must round-trip; script_body must not."""
    payload = _valid_minimal()
    payload["transform"]["operations"] = [
        {
            "operation_type": "map_script",
            "script_slot": "enrich_row",
            "language": "groovy2",
            "inputs": ["source_a"],
            "outputs": ["Root/target_a"],
            "script_component_ref": "scripts/enrich_row",
            "script_body": "<<task-authored script body>>",
        },
    ]
    result = _build(payload)
    assert result["_success"] is True
    transform_flow = next(
        f for f in result["integration_spec"]["flows"] if f["key"] == "transform"
    )
    op = transform_flow["operations"][0]
    assert op["script_component_ref"] == "scripts/enrich_row"
    assert op["has_script_body"] is True
    # script_body must not appear in the emitted spec.
    assert "script_body" not in op
    assert "task-authored script body" not in json.dumps(transform_flow)


def test_full_fixture_build_includes_watermark_and_dlq_flows():
    result = build_from_archetype_action("database_to_api_sync", _valid_full())
    assert result["_success"] is True, result
    spec = result["integration_spec"]
    flow_keys = {f["key"] for f in spec["flows"]}
    assert {"extract", "transform", "send", "reliability", "watermark"} <= flow_keys
    reliability = next(f for f in spec["flows"] if f["key"] == "reliability")
    assert reliability["target"] == "dlq"


def test_emitted_spec_carries_no_xml_or_mutation_markers():
    spec = DatabaseToApiSyncArchetype.emit_spec(
        DatabaseToApiSyncArchetype.validate_parameters(_valid_minimal())
    )
    payload = json.dumps(spec.model_dump())
    for marker in ("<?xml", "<process", "<component", "<connector", "<operation"):
        assert marker not in payload, f"Unexpected XML marker {marker!r}"


# ---------------------------------------------------------------------------
# Invalid payloads → PARAM_VALIDATION_FAILED with field_errors
# ---------------------------------------------------------------------------


def _build(payload: Dict[str, Any]) -> Dict[str, Any]:
    return build_from_archetype_action("database_to_api_sync", payload)


def _field_paths(result: Dict[str, Any]) -> List[str]:
    assert result["_success"] is False, result
    assert result["error_code"] == "PARAM_VALIDATION_FAILED", result
    return [fe["field_path"] for fe in result["field_errors"]]


def _assert_path_match(paths: List[str], needle: str) -> None:
    assert any(needle in p for p in paths), (
        f"expected a field error mentioning {needle!r}, got {paths!r}"
    )


def test_missing_integration_name_returns_field_error():
    payload = _valid_minimal()
    payload["naming"].pop("integration_name")
    paths = _field_paths(_build(payload))
    _assert_path_match(paths, "naming.integration_name")


def test_create_binding_without_settings_returns_field_error():
    payload = _valid_minimal()
    payload["source"]["binding"] = {"mode": "create"}
    paths = _field_paths(_build(payload))
    _assert_path_match(paths, "source.binding")


def test_reuse_binding_without_component_id_or_name_returns_field_error():
    payload = _valid_minimal()
    payload["source"]["binding"] = {"mode": "reuse"}
    paths = _field_paths(_build(payload))
    _assert_path_match(paths, "source.binding")


def test_blank_sql_returns_field_error():
    payload = _valid_minimal()
    payload["source"]["read_operation"]["sql"] = "   "
    paths = _field_paths(_build(payload))
    _assert_path_match(paths, "source.read_operation.sql")


def test_empty_operations_returns_field_error():
    payload = _valid_minimal()
    payload["transform"]["operations"] = []
    paths = _field_paths(_build(payload))
    _assert_path_match(paths, "transform.operations")


def test_rest_auth_without_credential_ref_returns_field_error():
    payload = _valid_minimal()
    payload["target"]["binding"]["settings"]["auth_mode"] = "basic"
    payload["target"]["binding"]["settings"].pop("credential_ref", None)
    paths = _field_paths(_build(payload))
    _assert_path_match(paths, "target.binding.settings")


def test_scheduled_trigger_without_schedule_returns_field_error():
    payload = _valid_minimal()
    payload["execution"]["trigger"] = {"mode": "scheduled"}
    paths = _field_paths(_build(payload))
    _assert_path_match(paths, "execution.trigger")


def test_username_password_auth_without_username_returns_field_error():
    payload = _valid_minimal()
    payload["source"]["binding"]["settings"].pop("username")
    result = _build(payload)
    paths = _field_paths(result)
    _assert_path_match(paths, "source.binding.settings")
    assert any("username" in fe["message"] for fe in result["field_errors"]), (
        f"expected the error message to mention username, got {result['field_errors']!r}"
    )


def test_watermark_query_param_without_watermark_returns_field_error():
    payload = _valid_minimal()
    payload["target"]["send_request"]["query_parameters"] = [
        {"name": "since", "value_source": "watermark"},
    ]
    result = _build(payload)
    paths = _field_paths(result)
    # Top-level model_validator → loc is empty tuple, so field_path is "".
    # Assert on the message instead so the contract surfaces a usable error.
    assert any(
        "watermark" in fe["message"] and "execution.watermark" in fe["message"]
        for fe in result["field_errors"]
    ), f"expected watermark-consistency error, got {result['field_errors']!r}"
    # field_paths still surfaced (empty string is fine — the message carries
    # the location info).
    assert paths, "expected at least one field_error entry"


def test_windows_integrated_auth_rejects_unused_username():
    payload = _valid_minimal()
    payload["source"]["binding"]["settings"]["auth_mode"] = "windows_integrated"
    payload["source"]["binding"]["settings"].pop("credential_ref")
    # Leave 'username' populated — should be rejected as unused.
    result = _build(payload)
    paths = _field_paths(result)
    _assert_path_match(paths, "source.binding.settings")
    assert any("windows_integrated" in fe["message"] for fe in result["field_errors"]), (
        f"expected windows_integrated to surface in error message, got {result['field_errors']!r}"
    )


def test_windows_integrated_auth_rejects_unused_credential_ref():
    payload = _valid_minimal()
    payload["source"]["binding"]["settings"]["auth_mode"] = "windows_integrated"
    payload["source"]["binding"]["settings"].pop("username")
    # Leave 'credential_ref' populated — should be rejected as unused.
    result = _build(payload)
    paths = _field_paths(result)
    _assert_path_match(paths, "source.binding.settings")
    assert any("windows_integrated" in fe["message"] for fe in result["field_errors"]), (
        f"expected windows_integrated to surface in error message, got {result['field_errors']!r}"
    )


def test_windows_integrated_auth_validates_when_unused_fields_omitted():
    payload = _valid_minimal()
    payload["source"]["binding"]["settings"]["auth_mode"] = "windows_integrated"
    payload["source"]["binding"]["settings"].pop("username")
    payload["source"]["binding"]["settings"].pop("credential_ref")
    result = _build(payload)
    assert result["_success"] is True, result


def test_rest_auth_none_rejects_unused_credential_ref():
    payload = _valid_minimal()
    payload["target"]["binding"]["settings"]["credential_ref"] = "secrets/rest/unused"
    # auth_mode is already "none" in the minimal fixture; credential_ref must be rejected.
    result = _build(payload)
    paths = _field_paths(result)
    _assert_path_match(paths, "target.binding.settings")
    assert any("auth_mode='none'" in fe["message"] for fe in result["field_errors"]), (
        f"expected auth_mode='none' to surface in error message, got {result['field_errors']!r}"
    )


def test_rest_query_parameter_value_source_mapping_is_rejected():
    payload = _valid_minimal()
    payload["target"]["send_request"]["query_parameters"] = [
        {"name": "id", "value_source": "mapping"},
    ]
    result = _build(payload)
    paths = _field_paths(result)
    # Literal[...] rejection surfaces with the index in the loc tuple.
    _assert_path_match(paths, "target.send_request.query_parameters")
    # Also assert the schema enum no longer advertises 'mapping' so callers
    # discover the new surface from get_integration_archetype alone.
    schema = DatabaseToApiSyncArchetype.parameter_schema()
    defs = schema.get("$defs") or schema.get("definitions") or {}
    rqp = defs.get("RestQueryParameter")
    assert rqp is not None, "RestQueryParameter must appear in $defs"
    value_source = rqp["properties"]["value_source"]
    assert set(value_source.get("enum", [])) == {"literal", "watermark"}


def test_watermark_consistency_error_does_not_echo_query_param_names():
    """The watermark validator must not echo caller-supplied query parameter
    names back through the error envelope. Mirrors the no-echo policy enforced
    by pattern_validation_error() for raw Pydantic input.
    """
    sentinel = "sk_live_ECHO_GUARD_DEADBEEF"
    payload = _valid_minimal()
    payload["target"]["send_request"]["query_parameters"] = [
        {"name": sentinel, "value_source": "watermark"},
    ]
    result = _build(payload)
    assert result["_success"] is False
    assert result["error_code"] == "PARAM_VALIDATION_FAILED"
    assert sentinel not in json.dumps(result), (
        "watermark-consistency error must not echo caller-supplied query "
        "parameter names back to the caller"
    )


# ===========================================================================
# Issue #44 — typed schema + transform operations contract
# ===========================================================================

# ---------------------------------------------------------------------------
# Positive cases — full fixture validates with map_function + map_script,
# minimal fixture validates direct-only.
# ---------------------------------------------------------------------------


def test_full_fixture_round_trip_includes_typed_operation_summaries():
    """The full fixture's three operation types (direct, map_function,
    map_script) must validate and round-trip into the emitted spec metadata
    with the right future-builder pointers."""
    params = DatabaseToApiSyncArchetype.validate_parameters(_valid_full())
    spec = DatabaseToApiSyncArchetype.emit_spec(params)
    transform_flow = next(f for f in spec.flows if f["key"] == "transform")
    op_types = {op["operation_type"]: op["future_builder_issue"] for op in transform_flow["operations"]}
    assert op_types == {"direct": "#26", "map_function": "#40", "map_script": "#41"}


def test_map_function_operation_validates_with_only_required_fields():
    """A bare map_function operation (no parameters, no documentation_hint)
    must validate and surface its future_builder pointer and operand details."""
    payload = _valid_minimal()
    payload["transform"]["operations"] = [
        {
            "operation_type": "map_function",
            "function_type": "uppercase",
            "inputs": ["source_a"],
            "target_path": "Root/target_a",
        },
    ]
    result = _build(payload)
    assert result["_success"] is True, result
    transform_flow = next(
        f for f in result["integration_spec"]["flows"] if f["key"] == "transform"
    )
    assert len(transform_flow["operations"]) == 1
    op = transform_flow["operations"][0]
    assert op["operation_type"] == "map_function"
    assert op["future_builder_issue"] == "#40"
    assert op["function_type"] == "uppercase"
    assert op["inputs"] == ["source_a"]
    assert op["input_count"] == 1
    assert op["target_path"] == "Root/target_a"
    # parameters omitted from the input -> omitted from the summary.
    assert "parameters" not in op


def test_map_script_operation_validates_without_canned_script_body():
    """A map_script operation without script_body must validate; the emitted
    spec metadata must surface the operand details but never invent a default
    body or component ref."""
    payload = _valid_minimal()
    payload["transform"]["operations"] = [
        {
            "operation_type": "map_script",
            "script_slot": "enrich_row",
            "language": "groovy2",
            "inputs": ["source_a"],
            "outputs": ["Root/target_a"],
        },
    ]
    result = _build(payload)
    assert result["_success"] is True, result
    transform_flow = next(
        f for f in result["integration_spec"]["flows"] if f["key"] == "transform"
    )
    assert len(transform_flow["operations"]) == 1
    op = transform_flow["operations"][0]
    assert op["operation_type"] == "map_script"
    assert op["future_builder_issue"] == "#41"
    assert op["script_slot"] == "enrich_row"
    assert op["language"] == "groovy2"
    assert op["inputs"] == ["source_a"]
    assert op["outputs"] == ["Root/target_a"]
    assert op["has_script_body"] is False
    # script_body and script_component_ref omitted from the input -> omitted
    # from the summary (no synthetic defaults).
    assert "script_body" not in op
    assert "script_component_ref" not in op


def test_documentation_hint_is_accepted_but_not_executable():
    """documentation_hint must be accepted on every operation type without
    influencing routing."""
    payload = _valid_minimal()
    payload["transform"]["operations"] = [
        {
            "operation_type": "direct",
            "source_field": "source_a",
            "target_path": "Root/target_a",
            "documentation_hint": "passes the value verbatim",
        },
    ]
    assert _build(payload)["_success"] is True


# ---------------------------------------------------------------------------
# Negative cases — result_schema
# ---------------------------------------------------------------------------


def test_missing_result_schema_returns_field_error():
    payload = _valid_minimal()
    payload["source"]["read_operation"].pop("result_schema")
    paths = _field_paths(_build(payload))
    _assert_path_match(paths, "source.read_operation")


def test_empty_result_schema_fields_returns_field_error():
    payload = _valid_minimal()
    payload["source"]["read_operation"]["result_schema"]["fields"] = []
    paths = _field_paths(_build(payload))
    _assert_path_match(paths, "source.read_operation.result_schema.fields")


def test_duplicate_result_schema_field_names_returns_field_error():
    payload = _valid_minimal()
    payload["source"]["read_operation"]["result_schema"]["fields"] = [
        {"name": "source_a", "data_type": "character"},
        {"name": "source_a", "data_type": "number"},
    ]
    # Add the new source field reference so transform validation doesn't
    # mask the duplicate-name error.
    payload["transform"]["operations"] = [
        {
            "operation_type": "direct",
            "source_field": "source_a",
            "target_path": "Root/target_a",
        },
    ]
    result = _build(payload)
    paths = _field_paths(result)
    _assert_path_match(paths, "source.read_operation.result_schema")
    assert any("duplicate" in fe["message"] for fe in result["field_errors"]), (
        f"expected 'duplicate' to surface in error message, got {result['field_errors']!r}"
    )


def test_unsupported_result_field_data_type_returns_field_error():
    payload = _valid_minimal()
    payload["source"]["read_operation"]["result_schema"]["fields"] = [
        {"name": "source_a", "data_type": "boolean"},
    ]
    paths = _field_paths(_build(payload))
    _assert_path_match(paths, "source.read_operation.result_schema.fields")


# ---------------------------------------------------------------------------
# Negative cases — payload_profile
# ---------------------------------------------------------------------------


def test_missing_payload_profile_returns_field_error():
    payload = _valid_minimal()
    payload["target"].pop("payload_profile")
    paths = _field_paths(_build(payload))
    _assert_path_match(paths, "target")


def test_payload_profile_root_must_be_object():
    payload = _valid_minimal()
    payload["target"]["payload_profile"]["root"] = {
        "name": "Root",
        "kind": "simple",
        "data_type": "character",
    }
    result = _build(payload)
    paths = _field_paths(result)
    _assert_path_match(paths, "target.payload_profile")
    assert any("object" in fe["message"] for fe in result["field_errors"]), (
        f"expected 'object' to surface in error message, got {result['field_errors']!r}"
    )


def test_object_node_without_children_returns_field_error():
    payload = _valid_minimal()
    payload["target"]["payload_profile"]["root"] = {
        "name": "Root",
        "kind": "object",
    }
    paths = _field_paths(_build(payload))
    _assert_path_match(paths, "target.payload_profile.root")


def test_array_node_without_children_returns_field_error():
    payload = _valid_minimal()
    payload["target"]["payload_profile"]["root"]["children"] = [
        {"name": "list", "kind": "array"},
    ]
    paths = _field_paths(_build(payload))
    _assert_path_match(paths, "target.payload_profile.root")


def test_simple_node_without_data_type_returns_field_error():
    payload = _valid_minimal()
    payload["target"]["payload_profile"]["root"]["children"] = [
        {"name": "target_a", "kind": "simple"},
    ]
    paths = _field_paths(_build(payload))
    _assert_path_match(paths, "target.payload_profile.root")


def test_profile_node_name_rejects_path_separator():
    """Codex review P2b: a JSON node name containing '/' would silently
    collapse distinct logical paths (e.g. leaf named 'a/b' colliding with
    object 'a' -> leaf 'b'). The node-name validator must reject the
    reserved path characters by construction."""
    payload = _valid_minimal()
    payload["target"]["payload_profile"]["root"]["children"] = [
        {"name": "a/b", "kind": "simple", "data_type": "character"},
    ]
    result = _build(payload)
    paths = _field_paths(result)
    _assert_path_match(paths, "target.payload_profile.root")
    assert any(
        "reserved path characters" in fe["message"]
        for fe in result["field_errors"]
    ), f"expected reserved-chars rejection, got {result['field_errors']!r}"


def test_profile_node_name_rejects_array_marker_brackets():
    """Codex review P2b: a JSON node name literally containing '[' or ']'
    would collide with the array repetition marker (e.g. leaf 'list[]'
    flattening to the same path as array 'list' with one child)."""
    payload_open = _valid_minimal()
    payload_open["target"]["payload_profile"]["root"]["children"] = [
        {"name": "list[", "kind": "simple", "data_type": "character"},
    ]
    paths_open = _field_paths(_build(payload_open))
    _assert_path_match(paths_open, "target.payload_profile.root")

    payload_close = _valid_minimal()
    payload_close["target"]["payload_profile"]["root"]["children"] = [
        {"name": "list]", "kind": "simple", "data_type": "character"},
    ]
    paths_close = _field_paths(_build(payload_close))
    _assert_path_match(paths_close, "target.payload_profile.root")


def test_profile_node_name_rejection_covers_root_node():
    """The reserved-char rejection must also fire on the profile root, not
    just nested children."""
    payload = _valid_minimal()
    payload["target"]["payload_profile"]["root"]["name"] = "Root/extra"
    paths = _field_paths(_build(payload))
    _assert_path_match(paths, "target.payload_profile.root")


# ---------------------------------------------------------------------------
# Codex r1 review: reject plaintext secret-shaped keys in map_function.parameters
# (the only schema-opaque dict the archetype echoes back in the emitted spec)
# ---------------------------------------------------------------------------


def _map_function_payload_with_parameters(parameters: Dict[str, Any]) -> Dict[str, Any]:
    """Return a fresh payload where the sole transform op is a map_function
    carrying the supplied parameters dict."""
    payload = _valid_minimal()
    payload["transform"]["operations"] = [
        {
            "operation_type": "map_function",
            "function_type": "concat",
            "inputs": ["source_a"],
            "target_path": "Root/target_a",
            "parameters": parameters,
        },
    ]
    return payload


def test_map_function_parameters_rejects_top_level_secret_key():
    """A literal `password` key in parameters must surface a structured
    PARAM_VALIDATION_FAILED — the dict is echoed back in the spec, so plaintext
    secret leaks must be blocked at parameter validation."""
    payload = _map_function_payload_with_parameters({"password": "hunter2"})
    result = _build(payload)
    assert result["_success"] is False
    assert result["error_code"] == "PARAM_VALIDATION_FAILED"
    assert any(
        "forbidden secret-shaped substring" in fe["message"]
        for fe in result["field_errors"]
    ), f"expected secret-shape rejection, got {result['field_errors']!r}"


def test_map_function_parameters_rejects_camelcase_secret_key():
    """The substring scan must catch camelCase (apiKey), snake_prefixed
    (db_password), and SCREAMING-CASE (AUTH_TOKEN) variants."""
    for key in ("apiKey", "db_password", "AUTH_TOKEN", "customerSecret", "Authorization"):
        payload = _map_function_payload_with_parameters({key: "VALUE"})
        result = _build(payload)
        assert result["_success"] is False, f"variant {key!r} should have been rejected"
        assert result["error_code"] == "PARAM_VALIDATION_FAILED"


def test_map_function_parameters_rejects_nested_secret_key():
    """A secret-shaped key at any nesting depth must be rejected — callers
    can't bypass the scan by wrapping the secret in a sub-dict."""
    payload = _map_function_payload_with_parameters(
        {"auth": {"nested": {"bearer": "<<token sentinel>>"}}}
    )
    result = _build(payload)
    assert result["_success"] is False
    assert result["error_code"] == "PARAM_VALIDATION_FAILED"


def test_map_function_parameters_rejects_secret_in_list_dict():
    """The scan must descend into lists of dicts so callers can't bypass it
    by wrapping the secret-shaped key inside a list element."""
    payload = _map_function_payload_with_parameters(
        {"headers": [{"bearer_token": "<<token sentinel>>"}]}
    )
    result = _build(payload)
    assert result["_success"] is False
    assert result["error_code"] == "PARAM_VALIDATION_FAILED"


def test_map_function_parameters_rejection_does_not_echo_offending_value():
    """The error envelope must not echo the plaintext secret VALUE."""
    sentinel = "sk_live_QA_PARAM_VALUE_GUARD_DEADBEEF"
    payload = _map_function_payload_with_parameters({"password": sentinel})
    result = _build(payload)
    assert result["_success"] is False
    assert sentinel not in json.dumps(result), (
        "secret-shaped-key rejection must not echo the plaintext value"
    )


def test_map_function_parameters_accepts_non_secret_keys():
    """Regression: legitimate parameter keys (e.g. 'separator', 'precision',
    'locale') must continue to validate."""
    payload = _map_function_payload_with_parameters(
        {"separator": ", ", "locale": "en-US", "precision": 4}
    )
    result = _build(payload)
    assert result["_success"] is True, result
    transform_flow = next(
        f for f in result["integration_spec"]["flows"] if f["key"] == "transform"
    )
    op = transform_flow["operations"][0]
    assert op["parameters"] == {"separator": ", ", "locale": "en-US", "precision": 4}


def test_map_function_parameters_accepts_credential_ref_style_keys():
    """`credential_ref` carries an opaque URI reference (not the secret
    itself); the scan must NOT reject `*_ref` style keys."""
    payload = _map_function_payload_with_parameters(
        {"credential_ref": "secrets/rest/bearer", "settings_ref": "configs/x"}
    )
    result = _build(payload)
    assert result["_success"] is True, result


# ---------------------------------------------------------------------------
# Negative cases — transform operations
# ---------------------------------------------------------------------------


def test_transform_references_unknown_source_field_fails():
    payload = _valid_minimal()
    payload["transform"]["operations"] = [
        {
            "operation_type": "direct",
            "source_field": "unknown_source_field",
            "target_path": "Root/target_a",
        },
    ]
    result = _build(payload)
    assert result["_success"] is False
    assert result["error_code"] == "PARAM_VALIDATION_FAILED"
    assert any(
        "source field name not declared" in fe["message"]
        and "result_schema" in fe["message"]
        for fe in result["field_errors"]
    ), f"expected unknown-source-field error, got {result['field_errors']!r}"


def test_transform_references_unknown_target_path_fails():
    payload = _valid_minimal()
    payload["transform"]["operations"] = [
        {
            "operation_type": "direct",
            "source_field": "source_a",
            "target_path": "Root/does_not_exist",
        },
    ]
    result = _build(payload)
    assert result["_success"] is False
    assert result["error_code"] == "PARAM_VALIDATION_FAILED"
    assert any(
        "target path" in fe["message"] for fe in result["field_errors"]
    ), f"expected unknown-target-path error, got {result['field_errors']!r}"


def test_transform_targeting_object_node_fails():
    """A direct operation cannot bind to an object node; only simple leaves
    are valid transform targets per the M2.1a contract."""
    payload = _valid_minimal()
    # Add a nested object so we have an object node to (incorrectly) target.
    payload["target"]["payload_profile"]["root"]["children"] = [
        {
            "name": "target_a",
            "kind": "simple",
            "data_type": "character",
        },
        {
            "name": "wrapper",
            "kind": "object",
            "children": [
                {
                    "name": "nested_target",
                    "kind": "simple",
                    "data_type": "character",
                },
            ],
        },
    ]
    payload["transform"]["operations"] = [
        {
            "operation_type": "direct",
            "source_field": "source_a",
            "target_path": "Root/wrapper",
        },
    ]
    result = _build(payload)
    assert result["_success"] is False
    assert result["error_code"] == "PARAM_VALIDATION_FAILED"
    assert any(
        "target path" in fe["message"] for fe in result["field_errors"]
    )


def test_transform_targeting_array_node_fails():
    """A direct operation cannot bind to an array node; only simple leaves
    are valid transform targets."""
    payload = _valid_minimal()
    payload["target"]["payload_profile"]["root"]["children"] = [
        {
            "name": "target_a",
            "kind": "simple",
            "data_type": "character",
        },
        {
            "name": "list",
            "kind": "array",
            "children": [
                {"name": "elem", "kind": "simple", "data_type": "character"},
            ],
        },
    ]
    payload["transform"]["operations"] = [
        {
            "operation_type": "direct",
            "source_field": "source_a",
            "target_path": "Root/list",
        },
    ]
    result = _build(payload)
    assert result["_success"] is False
    assert result["error_code"] == "PARAM_VALIDATION_FAILED"


def test_duplicate_target_binding_across_operations_fails():
    payload = _valid_minimal()
    payload["source"]["read_operation"]["result_schema"]["fields"] = [
        {"name": "source_a", "data_type": "character"},
        {"name": "source_b", "data_type": "character"},
    ]
    payload["transform"]["operations"] = [
        {
            "operation_type": "direct",
            "source_field": "source_a",
            "target_path": "Root/target_a",
        },
        {
            "operation_type": "direct",
            "source_field": "source_b",
            "target_path": "Root/target_a",
        },
    ]
    result = _build(payload)
    assert result["_success"] is False
    assert result["error_code"] == "PARAM_VALIDATION_FAILED"
    assert any(
        "bind" in fe["message"] and "more than once" in fe["message"]
        for fe in result["field_errors"]
    ), f"expected duplicate-binding error, got {result['field_errors']!r}"


def test_map_script_duplicate_output_across_operations_fails():
    payload = _valid_minimal()
    payload["source"]["read_operation"]["result_schema"]["fields"] = [
        {"name": "source_a", "data_type": "character"},
    ]
    payload["transform"]["operations"] = [
        {
            "operation_type": "direct",
            "source_field": "source_a",
            "target_path": "Root/target_a",
        },
        {
            "operation_type": "map_script",
            "script_slot": "overwrite_target_a",
            "language": "groovy2",
            "inputs": ["source_a"],
            "outputs": ["Root/target_a"],
        },
    ]
    result = _build(payload)
    assert result["_success"] is False
    assert result["error_code"] == "PARAM_VALIDATION_FAILED"


def test_watermark_referencing_unknown_source_field_fails():
    payload = _valid_full()
    payload["execution"]["watermark"]["field"] = "not_a_declared_field"
    result = _build(payload)
    assert result["_success"] is False
    assert result["error_code"] == "PARAM_VALIDATION_FAILED"
    assert any(
        "watermark" in fe["message"]
        and "result_schema" in fe["message"]
        for fe in result["field_errors"]
    ), f"expected watermark-source-ref error, got {result['field_errors']!r}"


# ---------------------------------------------------------------------------
# Negative cases — unsupported operation types
# ---------------------------------------------------------------------------


def test_unsupported_operation_type_returns_field_error():
    payload = _valid_minimal()
    payload["transform"]["operations"] = [
        {
            "operation_type": "totally_unknown",
            "source_field": "source_a",
            "target_path": "Root/target_a",
        },
    ]
    result = _build(payload)
    assert result["_success"] is False
    assert result["error_code"] == "PARAM_VALIDATION_FAILED"


def test_xslt_operation_type_emits_issue_42_pointer():
    """operation_type='xslt' must surface a friendly pointer to issue #42."""
    payload = _valid_minimal()
    payload["transform"]["operations"] = [
        {"operation_type": "xslt", "stylesheet_ref": "<<xslt body>>"},
    ]
    result = _build(payload)
    assert result["_success"] is False
    assert result["error_code"] == "PARAM_VALIDATION_FAILED"
    assert any("#42" in fe["message"] for fe in result["field_errors"]), (
        f"expected #42 pointer in error message, got {result['field_errors']!r}"
    )


# ---------------------------------------------------------------------------
# Negative cases — legacy executable surface
# ---------------------------------------------------------------------------


def test_legacy_mappings_shape_is_rejected():
    """The legacy ``transform.mappings`` shape is rejected as
    ``extra_forbidden`` so callers can't accidentally route to the old path."""
    payload = _valid_minimal()
    # Replace the typed operations with the legacy shape.
    payload["transform"] = {
        "mappings": [
            {"source_field": "source_a", "target_field": "Root/target_a"},
        ],
    }
    result = _build(payload)
    paths = _field_paths(result)
    # Without operations, the typed contract surfaces a missing-operations
    # field error AND extra_forbidden for mappings.
    assert any("transform" in p for p in paths)


def test_legacy_payload_template_is_rejected():
    payload = _valid_minimal()
    payload["transform"]["payload_template"] = "<<legacy payload template>>"
    paths = _field_paths(_build(payload))
    _assert_path_match(paths, "transform")


def test_legacy_script_slots_is_rejected():
    payload = _valid_minimal()
    payload["transform"]["script_slots"] = {"pre_send": "<<legacy hook body>>"}
    paths = _field_paths(_build(payload))
    _assert_path_match(paths, "transform")


def test_legacy_transform_hint_on_operation_is_rejected():
    """The legacy executable ``transform_hint`` field is dropped; only
    ``documentation_hint`` is accepted on operations."""
    payload = _valid_minimal()
    payload["transform"]["operations"] = [
        {
            "operation_type": "direct",
            "source_field": "source_a",
            "target_path": "Root/target_a",
            "transform_hint": "trim",
        },
    ]
    paths = _field_paths(_build(payload))
    _assert_path_match(paths, "transform.operations")


# ---------------------------------------------------------------------------
# Negative cases — secret hygiene
# ---------------------------------------------------------------------------


def test_transform_validation_error_does_not_echo_source_field_names():
    """Cross-field validation must not echo the offending source-field name
    back through the error envelope (defense-in-depth against callers using
    sensitive identifiers)."""
    sentinel = "sk_live_UNKNOWN_FIELD_ECHO_GUARD_DEADBEEF"
    payload = _valid_minimal()
    payload["transform"]["operations"] = [
        {
            "operation_type": "direct",
            "source_field": sentinel,
            "target_path": "Root/target_a",
        },
    ]
    result = _build(payload)
    assert result["_success"] is False
    assert sentinel not in json.dumps(result), (
        "transform-reference error must not echo caller-supplied source field "
        "names back to the caller"
    )


def test_duplicate_target_binding_error_does_not_echo_target_paths():
    """Duplicate-target error must not echo the offending leaf path."""
    sentinel_segment = "sk_live_TARGET_PATH_ECHO_GUARD_DEADBEEF"
    payload = _valid_minimal()
    payload["source"]["read_operation"]["result_schema"]["fields"] = [
        {"name": "source_a", "data_type": "character"},
        {"name": "source_b", "data_type": "character"},
    ]
    payload["target"]["payload_profile"]["root"]["children"] = [
        {"name": sentinel_segment, "kind": "simple", "data_type": "character"},
    ]
    target_path = f"Root/{sentinel_segment}"
    payload["transform"]["operations"] = [
        {
            "operation_type": "direct",
            "source_field": "source_a",
            "target_path": target_path,
        },
        {
            "operation_type": "direct",
            "source_field": "source_b",
            "target_path": target_path,
        },
    ]
    result = _build(payload)
    assert result["_success"] is False
    # The sentinel will appear in the emitted spec on success — but failure
    # responses must keep cross-field errors structural.
    assert sentinel_segment not in json.dumps(result["field_errors"]), (
        "duplicate-target-binding error must not echo caller-supplied target "
        "leaf paths back to the caller"
    )


# ---------------------------------------------------------------------------
# Example + default hygiene (no canned templates, no plaintext credential fields)
# ---------------------------------------------------------------------------


def test_examples_carry_not_reusable_template_marker():
    described = DatabaseToApiSyncArchetype.describe()
    examples = described["examples"]
    assert examples, "database_to_api_sync must publish at least one example"
    for example in examples:
        assert example["is_template"] is False
        assert example["template_status"] == "example_only_not_reusable_template"


_FORBIDDEN_TEMPLATE_SUBSTRINGS = (
    "select ",
    "insert ",
    "update ",
    "delete ",
    " from ",
    "where ",
    "<?xml",
    "<soap",
    "<envelope",
    "<process",
    "<connector",
    "<operation",
    "$filter=",
    "$select=",
    "$expand=",
    " def ",
    "import ",
    "groovy",
    "javascript:",
    "script:",
    "mapping:",
    " map ",
)


def _collect_default_strings(schema: Dict[str, Any]) -> List[str]:
    """Walk every property and $defs entry, collect default values as strings."""
    out: List[str] = []
    for _, _, prop_schema in _walk_properties(schema):
        if "default" in prop_schema:
            out.append(json.dumps(prop_schema["default"]).lower())
    return out


def test_examples_and_defaults_have_no_canned_content():
    described = DatabaseToApiSyncArchetype.describe()
    for example in described["examples"]:
        payload = json.dumps(example).lower()
        for marker in _FORBIDDEN_TEMPLATE_SUBSTRINGS:
            assert marker not in payload, (
                f"example {example['name']!r} contains forbidden marker {marker!r}"
            )

    defaults = _collect_default_strings(DatabaseToApiSyncArchetype.parameter_schema())
    for default in defaults:
        for marker in _FORBIDDEN_TEMPLATE_SUBSTRINGS:
            assert marker not in default, (
                f"default value {default!r} contains forbidden marker {marker!r}"
            )


def _collect_property_names(schema: Dict[str, Any]) -> List[str]:
    return [name for _, name, _ in _walk_properties(schema)]


def test_schema_has_no_plaintext_credential_field_names():
    forbidden = {"password", "secret", "token", "access_token", "client_secret"}
    names = set(_collect_property_names(DatabaseToApiSyncArchetype.parameter_schema()))
    overlap = names & forbidden
    assert not overlap, (
        f"database_to_api_sync schema exposes forbidden credential field names: {overlap}"
    )


def test_validation_error_does_not_echo_caller_supplied_secrets():
    secret = "sk_live_DEADBEEF_super_secret_value"
    payload = _valid_minimal()
    payload["target"]["binding"]["settings"]["credential_ref"] = secret
    # Force a validation failure on a neighbour field so the response is a
    # PARAM_VALIDATION_FAILED envelope (which is the surface we audit for secret
    # echo). credential_ref itself remains valid.
    payload["naming"]["integration_name"] = "   "
    result = _build(payload)
    assert result["_success"] is False
    assert result["error_code"] == "PARAM_VALIDATION_FAILED"
    assert secret not in json.dumps(result), (
        "PARAM_VALIDATION_FAILED responses must not echo caller-supplied secrets"
    )

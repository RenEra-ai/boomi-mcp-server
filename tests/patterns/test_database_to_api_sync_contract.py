"""Tests for the database_to_api_sync archetype contract surface
(issues #21, #44; executable assembly is exercised in
test_database_to_api_sync_assembly.py per #29).

Issue #44 (M2.1a) replaced the legacy ``transform.mappings`` /
``transform.payload_template`` / ``transform.script_slots`` surface with:

  * caller-declared DB read result fields under
    ``source.read_operation.result_schema``,
  * caller-supplied JSON profile tree under ``target.payload_profile``, and
  * discriminated typed transform operations under ``transform.operations``.

Issue #29 turned the archetype from contract-only into an executable assembly,
so these tests now also assert the emitted component graph and the lax-contract
/ strict-assembly boundaries (unsupported auth, mixed routes, etc.).
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
                "mode": "reuse",
                "component_id": "<<existing REST connection id>>",
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
                    "function_type": "date_format",
                    "inputs": ["source_b"],
                    "target_path": "Root/target_b",
                    "parameters": {
                        "input_format": "<<source datetime format>>",
                        "output_format": "<<target datetime format>>",
                    },
                },
            ],
        },
        "execution": {
            "trigger": {
                "mode": "scheduled",
                "schedule": {"cron": "0 2 * * *", "timezone": "UTC"},
            },
            "watermark": {
                "field": "source_b",
                "kind": "timestamp",
                "persistence": "dpp",
            },
            "run_metadata": {"owner": "crm-team"},
        },
        "reliability": {
            # guidance_only DLQ wires no catch path → retry stays 1 (#88
            # validator: retry>1 requires a wired DLQ). backoff is platform-only.
            "retry": {
                "max_attempts": 1,
                "backoff": "platform",
            },
            "dlq": {
                "enabled": True,
                "target": {"mode": "guidance_only", "kind": "queue", "address": "<<dlq queue address>>"},
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


def test_valid_build_emits_executable_component_spec():
    """Issue #29: the minimal payload now emits an executable IntegrationSpecV1
    (DB source + JSON transform + REST target + structured process), still with
    no Boomi mutation and no raw XML exposed."""
    result = build_from_archetype_action("database_to_api_sync", _valid_minimal())
    assert result["_success"] is True, result
    assert result["boomi_mutation"] is False
    assert result["raw_xml_exposed"] is False
    spec = result["integration_spec"]
    assert spec["mode"] == "redesign"
    keys = [c["key"] for c in spec["components"]]
    assert keys == [
        "source_db_connection",
        "source_db_read_profile",
        "source_db_get_operation",
        "transform_target_profile",
        "transform_transform_map",
        "target_rest_connection",
        "target_rest_operation",
        "main_process",
    ]
    rules = spec["validation_rules"]
    assert rules["contract_only"] is False
    assert rules["component_count"] == len(spec["components"]) == 8
    assert rules["raw_xml_exposed"] is False
    assert rules["boomi_mutation"] is False
    # Issue #29 removed the zero-component marker.
    assert "requires_m2_9_for_executable_components" not in rules
    assert rules["metadata_version"] == "0.5.0"
    assert "caller-declared" in rules["profile_schema_strategy"]
    assert "caller-supplied" in rules["profile_schema_strategy"]
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
    # _valid_full() is a single-route (direct + map_function) executable payload;
    # mixing map_function with map_script is rejected at emit (#29), so the
    # full executable fixture uses one map route. map_script summary/route
    # coverage lives in the dedicated map_script tests below.
    assert len(ops) == 2
    by_type = {op["operation_type"]: op for op in ops}
    # direct: keeps source_field, target_path, and the documentation hint.
    assert by_type["direct"]["future_builder_issue"] == "#26"
    assert by_type["direct"]["source_field"] == "source_a"
    assert by_type["direct"]["target_path"] == "Root/target_a"
    assert by_type["direct"]["documentation_hint"] == "carry first column verbatim"
    # map_function: emit full operand structure (inputs[], target_path,
    # parameters) so the flow summary round-trips the operand for review.
    assert by_type["map_function"]["future_builder_issue"] == "#40"
    assert by_type["map_function"]["function_type"] == "date_format"
    assert by_type["map_function"]["inputs"] == ["source_b"]
    assert by_type["map_function"]["input_count"] == 1
    assert by_type["map_function"]["target_path"] == "Root/target_b"
    assert "input_format" in by_type["map_function"]["parameters"]
    assert "map_script" not in by_type


def test_map_function_summary_surfaces_inputs_and_parameters():
    """Codex review P2a follow-up: when map_function declares inputs and a
    parameters dict, both must round-trip into the spec for #40 to compile."""
    payload = _valid_minimal()
    payload["transform"]["operations"] = [
        {
            "operation_type": "map_function",
            "function_type": "date_format",
            "inputs": ["source_a"],
            "target_path": "Root/target_a",
            "parameters": {
                "input_format": "<<source datetime format>>",
                "output_format": "<<target datetime format>>",
            },
        },
    ]
    result = _build(payload)
    assert result["_success"] is True, result
    transform_flow = next(
        f for f in result["integration_spec"]["flows"] if f["key"] == "transform"
    )
    op = transform_flow["operations"][0]
    assert op["inputs"] == ["source_a"]
    assert op["input_count"] == 1
    assert op["parameters"] == {
        "input_format": "<<source datetime format>>",
        "output_format": "<<target datetime format>>",
    }


def test_map_script_script_component_ref_accepted_by_contract_rejected_at_assembly():
    """The parameter contract accepts map_script.script_component_ref (it is a
    valid contract field), but issue #29 cannot materialize the referenced
    script component into the spec — the planned spec would carry a dangling
    dependency. Assembly rejects it with a structured UNSUPPORTED_SCRIPT_
    COMPONENT_REF, directing callers to inline script_body instead."""
    payload = _valid_minimal()
    payload["transform"]["operations"] = [
        {
            "operation_type": "map_script",
            "script_slot": "enrich_row",
            "language": "groovy2",
            "inputs": ["source_a"],
            "outputs": ["Root/target_a"],
            "script_component_ref": "$ref:enrich_row",
        },
    ]
    # Parameter contract accepts the field.
    DatabaseToApiSyncArchetype.validate_parameters(payload)
    # Assembly rejects it.
    result = _build(payload)
    assert result["_success"] is False, result
    assert result["error_code"] == "UNSUPPORTED_SCRIPT_COMPONENT_REF"


def test_map_script_accepts_inline_script_body_after_41_ships():
    """Issue #41 shipped — the contract layer now accepts script_body and
    surfaces a presence boolean in the emitted spec without echoing the
    body itself. Materializing the matching script.mapping component is
    downstream tooling's job, not the archetype's."""
    payload = _valid_minimal()
    payload["transform"]["operations"] = [
        {
            "operation_type": "map_script",
            "script_slot": "enrich_row",
            "language": "groovy2",
            "inputs": ["source_a"],
            "outputs": ["Root/target_a"],
            "script_body": "<<task-authored script body>>",
        },
    ]
    result = _build(payload)
    assert result["_success"] is True, result
    transform_flow = next(
        f for f in result["integration_spec"]["flows"] if f["key"] == "transform"
    )
    op = transform_flow["operations"][0]
    assert op["script_body_present"] is True
    # Codex r3 P2 #3: the body itself round-trips into the spec so downstream
    # compilation can materialise the matching script.mapping component.
    assert op["script_body"] == "<<task-authored script body>>"


def test_map_script_summary_round_trips_script_body_content():
    """Codex r3 P2 finding #3: the actual script_body must round-trip into
    the emitted spec so downstream build_integration / #41 wrapper synthesis
    can materialise a script.mapping component from spec metadata alone.
    Dropping the body to only a presence boolean would lose runnable content
    between build_from_archetype and downstream compilation."""
    sentinel = "outputValue = inputValue.toUpperCase() // round-trip sentinel"
    payload = _valid_minimal()
    payload["transform"]["operations"] = [
        {
            "operation_type": "map_script",
            "script_slot": "enrich_row",
            "language": "groovy2",
            "inputs": ["source_a"],
            "outputs": ["Root/target_a"],
            "script_body": sentinel,
        },
    ]
    result = _build(payload)
    assert result["_success"] is True, result
    transform_flow = next(
        f for f in result["integration_spec"]["flows"] if f["key"] == "transform"
    )
    op = transform_flow["operations"][0]
    assert op["script_body_present"] is True
    assert op["script_body"] == sentinel
    # Defence-in-depth: the body should also appear in the JSON envelope
    # (caller-output, not code) so spec-driven downstream tooling can find
    # it without re-walking the original archetype payload.
    assert sentinel in json.dumps(result)


def test_map_script_blank_script_body_is_rejected():
    """A whitespace-only script_body discards caller intent silently if
    accepted. The validator strips and re-checks for non-blank content."""
    payload = _valid_minimal()
    payload["transform"]["operations"] = [
        {
            "operation_type": "map_script",
            "script_slot": "enrich_row",
            "language": "groovy2",
            "inputs": ["source_a"],
            "outputs": ["Root/target_a"],
            "script_body": "   \t  ",
        },
    ]
    result = _build(payload)
    assert result["_success"] is False
    assert result["error_code"] == "PARAM_VALIDATION_FAILED"


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
# Issue #43 (M2.5b): generated profile field payloads + indexes on transform
# ---------------------------------------------------------------------------


def _transform_flow(payload: Dict[str, Any]) -> Dict[str, Any]:
    result = build_from_archetype_action("database_to_api_sync", payload)
    assert result["_success"] is True, result
    spec = result["integration_spec"]
    return next(f for f in spec["flows"] if f["key"] == "transform")


def test_issue_43_transform_flow_carries_source_profile_generation():
    transform_flow = _transform_flow(_valid_full())
    src_gen = transform_flow["source_profile_generation"]
    assert src_gen["generation_mode"] == "profile_from_db_read_fields"
    assert src_gen["component_type"] == "profile.db"
    assert src_gen["profile_type"] == "database.read"
    # output_fields shape matches DatabaseReadProfileBuilder expectations.
    expected_output_fields = [
        {
            "name": "source_a",
            "data_type": "character",
            "mandatory": True,
            "enforce_unique": False,
        },
        {
            "name": "source_b",
            "data_type": "datetime",
            "mandatory": False,
            "enforce_unique": False,
        },
        {
            "name": "source_c",
            "data_type": "number",
            "mandatory": False,
            "enforce_unique": False,
        },
    ]
    assert src_gen["profile_config"]["output_fields"] == expected_output_fields
    # source field_index covers every declared field.
    assert set(src_gen["field_index_by_path"].keys()) == {
        "source_a",
        "source_b",
        "source_c",
    }
    assert src_gen["mappable_paths"] == ["source_a", "source_b", "source_c"]


def test_issue_43_transform_flow_carries_target_profile_generation():
    transform_flow = _transform_flow(_valid_full())
    tgt_gen = transform_flow["target_profile_generation"]
    assert tgt_gen["generation_mode"] == "profile_from_json_schema"
    assert tgt_gen["component_type"] == "profile.json"
    assert tgt_gen["profile_type"] == "json.generated"
    # Normalized tree preserves child order from the fixture.
    root = tgt_gen["profile_config"]["root"]
    assert root["name"] == "Root"
    assert root["kind"] == "object"
    assert [c["name"] for c in root["children"]] == ["target_a", "target_b", "list"]
    # field_index covers every node — structural + leaves.
    assert set(tgt_gen["field_index_by_path"].keys()) == {
        "Root",
        "Root/target_a",
        "Root/target_b",
        "Root/list",
        "Root/list[]/target_c",
    }
    # mappable_paths matches the legacy _flatten_payload_profile_leaves shape
    # (regression guard against path-convention drift between #43 helpers and
    # the contract's leaf summary).
    target_summary_leaves = {
        leaf["path"] for leaf in transform_flow["target_payload_profile"]["leaves"]
    }
    assert set(tgt_gen["mappable_paths"]) == target_summary_leaves == {
        "Root/target_a",
        "Root/target_b",
        "Root/list[]/target_c",
    }


def test_issue_43_transform_flow_direct_field_mappings_excludes_non_direct():
    transform_flow = _transform_flow(_valid_full())
    direct = transform_flow["direct_field_mappings"]
    # _valid_full() has 1 direct + 1 map_function operation; only the direct
    # one is normalized into direct_field_mappings.
    assert direct == [
        {
            "route": "direct",
            "source_path": "source_a",
            "target_path": "Root/target_a",
            "source_data_type": "character",
            "target_data_type": "character",
        },
    ]
    # The operations summary carries both the direct and map_function entries.
    op_types = {op["operation_type"] for op in transform_flow["operations"]}
    assert op_types == {"direct", "map_function"}


def test_issue_43_existing_transform_flow_keys_intact():
    """Regression: issue #40 operation metadata must remain untouched on the
    transform flow when issue #43 keys are added."""
    transform_flow = _transform_flow(_valid_full())
    # Issue #44 keys still present and well-formed.
    assert "source_schema" in transform_flow
    assert "target_payload_profile" in transform_flow
    assert "operations" in transform_flow
    by_type = {op["operation_type"]: op for op in transform_flow["operations"]}
    assert by_type["map_function"]["inputs"] == ["source_b"]
    assert by_type["map_function"]["target_path"] == "Root/target_b"


def test_issue_29_full_payload_emits_executable_components():
    """Issue #29: the full payload emits executable components (it no longer
    returns a zero-component contract spec)."""
    result = build_from_archetype_action("database_to_api_sync", _valid_full())
    assert result["_success"] is True, result
    assert result["boomi_mutation"] is False
    keys = [c["key"] for c in result["integration_spec"]["components"]]
    assert "main_process" in keys
    assert "source_db_connection" in keys
    assert "target_rest_operation" in keys


def test_issue_43_generation_metadata_does_not_echo_sql_or_description():
    """Anti-template hygiene: generation metadata must carry no SQL body, no
    raw XML, no JSON payload sample, and no echoed description text."""
    payload = _valid_full()
    sentinel_sql = "<<custom-sql-WITH-marker-DEADBEEF-uppercase-DELETE-keyword>>"
    payload["source"]["read_operation"]["sql"] = sentinel_sql
    payload["target"]["payload_profile"]["root"]["description"] = (
        "<<root-description-marker-DEADBEEF>>"
    )
    payload["target"]["payload_profile"]["root"]["children"][0]["description"] = (
        "<<leaf-description-marker-DEADBEEF>>"
    )

    transform_flow = _transform_flow(payload)
    src_gen = transform_flow["source_profile_generation"]
    tgt_gen = transform_flow["target_profile_generation"]

    gen_blob = json.dumps(
        [src_gen, tgt_gen, transform_flow["direct_field_mappings"]]
    )
    # SQL body must never appear in generation metadata.
    assert sentinel_sql not in gen_blob
    # Descriptions on profile nodes must never be echoed into the normalized
    # tree (anti-template hygiene).
    assert "root-description-marker-DEADBEEF" not in gen_blob
    assert "leaf-description-marker-DEADBEEF" not in gen_blob
    # XML markers must never appear in generation metadata.
    for marker in ("<?xml", "<process", "<connector", "<operation"):
        assert marker not in gen_blob, (
            f"generation metadata unexpectedly contains XML marker {marker!r}"
        )


def test_issue_43_validation_rules_profile_strategy_mentions_43_and_47():
    """validation_rules.profile_schema_strategy must mention both issue #43
    (generated indexes) and issue #47 (deferred inference)."""
    result = build_from_archetype_action("database_to_api_sync", _valid_minimal())
    rules = result["integration_spec"]["validation_rules"]
    strategy = rules["profile_schema_strategy"]
    assert "#43" in strategy
    assert "#47" in strategy


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


def test_source_reuse_binding_with_both_ids_returns_field_error():
    # Issue #127 B2: reuse mode must supply EXACTLY one identifier; both-present
    # used to pass the public contract and only fail downstream.
    payload = _valid_minimal()
    payload["source"]["binding"] = {
        "mode": "reuse",
        "component_id": "conn-123",
        "component_name": "Existing DB Conn",
    }
    paths = _field_paths(_build(payload))
    _assert_path_match(paths, "source.binding")
    assert any(
        "exactly one of component_id or component_name" in fe["message"]
        for fe in _build(payload)["field_errors"]
    ), _build(payload)["field_errors"]


def test_target_reuse_binding_with_both_ids_returns_field_error():
    # Issue #127 B2: same exactly-one rule on the REST target binding.
    payload = _valid_minimal()
    payload["target"]["binding"] = {
        "mode": "reuse",
        "component_id": "rest-123",
        "component_name": "Existing REST Conn",
    }
    paths = _field_paths(_build(payload))
    _assert_path_match(paths, "target.binding")


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


def test_windows_integrated_auth_accepted_by_contract_rejected_at_assembly():
    """The parameter contract accepts auth_mode='windows_integrated' (a valid
    literal) when the unused fields are omitted, but issue #29 cannot assemble a
    create-mode DB connection for it — the DatabaseConnectorBuilder only
    supports 'username_password', so emit returns a structured
    UNSUPPORTED_DB_AUTH_MODE (use connection reuse instead)."""
    payload = _valid_minimal()
    payload["source"]["binding"]["settings"]["auth_mode"] = "windows_integrated"
    payload["source"]["binding"]["settings"].pop("username")
    payload["source"]["binding"]["settings"].pop("credential_ref")
    # Parameter contract accepts the auth-mode shape.
    DatabaseToApiSyncArchetype.validate_parameters(payload)
    # Assembly rejects it with a structured error.
    result = _build(payload)
    assert result["_success"] is False, result
    assert result["error_code"] == "UNSUPPORTED_DB_AUTH_MODE"


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
    """The full fixture's operation types (direct, map_function) validate and
    round-trip into the emitted spec metadata with the right future-builder
    pointers. (map_function + map_script cannot share one executable map, so the
    full executable fixture uses a single map route.)"""
    params = DatabaseToApiSyncArchetype.validate_parameters(_valid_full())
    spec = DatabaseToApiSyncArchetype.emit_spec(params)
    transform_flow = next(f for f in spec.flows if f["key"] == "transform")
    op_types = {op["operation_type"]: op["future_builder_issue"] for op in transform_flow["operations"]}
    assert op_types == {"direct": "#26", "map_function": "#40"}


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


def test_map_script_without_body_or_ref_accepted_by_contract_rejected_at_assembly():
    """Issue #29 boundary: a map_script op with neither script_body nor
    script_component_ref is still accepted by the parameter contract (it is a
    valid contract shape), but it cannot be assembled into an executable map —
    emit returns a clean structured ARCHETYPE_PARAM_INVALID error (not the
    opaque ARCHETYPE_BUILD_FAILED), without echoing caller values."""
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
    # Parameter contract accepts the bodyless/refless map_script op.
    DatabaseToApiSyncArchetype.validate_parameters(payload)
    # Assembly rejects it with a clean structured error.
    result = _build(payload)
    assert result["_success"] is False, result
    assert result["error_code"] == "ARCHETYPE_PARAM_INVALID"
    assert "exactly one" in result["error"] or "script_body" in result["error"]


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


def test_db_result_field_name_rejects_reserved_path_chars():
    """Codex r1 (issue #43): DBResultField.name must reject '/', '[', ']' at
    parameter-validation time so callers see a structured PARAM_VALIDATION_FAILED
    rather than an opaque ARCHETYPE_BUILD_FAILED from the issue #43 profile
    generation helper. DB field names become source profile logical paths;
    reserved chars would collide with the Root/list[]/key convention.
    """
    for reserved in ("a/b", "a[", "a]"):
        payload = _valid_minimal()
        payload["source"]["read_operation"]["result_schema"]["fields"] = [
            {"name": reserved, "data_type": "character"},
        ]
        # The transform mapping must reference the new field name too, so we
        # do not trip the cross-validator on a missing source reference.
        payload["transform"]["operations"] = [
            {
                "operation_type": "direct",
                "source_field": reserved,
                "target_path": "Root/target_a",
            },
        ]
        result = _build(payload)
        # The point of the fix: structured field-error envelope, NOT
        # ARCHETYPE_BUILD_FAILED.
        assert result["_success"] is False, result
        assert result["error_code"] == "PARAM_VALIDATION_FAILED", (
            f"expected PARAM_VALIDATION_FAILED for name={reserved!r}, got {result!r}"
        )
        paths = [fe["field_path"] for fe in result["field_errors"]]
        _assert_path_match(paths, "source.read_operation.result_schema")
        assert any(
            "reserved path characters" in fe["message"]
            for fe in result["field_errors"]
        ), f"expected reserved-chars rejection on DB field, got {result['field_errors']!r}"


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
    'locale') must pass the contract's secret-shaped-key scan. This is a
    parameter-contract guarantee — independent of which keys a specific map
    function builder ultimately accepts."""
    payload = _map_function_payload_with_parameters(
        {"separator": ", ", "locale": "en-US", "precision": 4}
    )
    # No secret-shaped-key rejection at parameter validation.
    params = DatabaseToApiSyncArchetype.validate_parameters(payload)
    assert isinstance(params, DatabaseToApiSyncParameters)


def test_map_function_parameters_accepts_credential_ref_style_keys():
    """`credential_ref` carries an opaque URI reference (not the secret
    itself); the contract's secret scan must NOT reject `*_ref` style keys."""
    payload = _map_function_payload_with_parameters(
        {"credential_ref": "secrets/rest/bearer", "settings_ref": "configs/x"}
    )
    params = DatabaseToApiSyncArchetype.validate_parameters(payload)
    assert isinstance(params, DatabaseToApiSyncParameters)


# ---------------------------------------------------------------------------
# Issue #127 B1: naming.runtime_hints is the second schema-opaque dict echoed
# verbatim into the emitted spec (under spec.runtime), so it must run the same
# plaintext secret-shaped-key scan as map_function.parameters.
# ---------------------------------------------------------------------------


def test_runtime_hints_rejects_top_level_secret_key():
    payload = _valid_minimal()
    payload["naming"]["runtime_hints"] = {"password": "hunter2"}
    result = _build(payload)
    assert result["_success"] is False
    assert result["error_code"] == "PARAM_VALIDATION_FAILED"
    _assert_path_match(
        [fe["field_path"] for fe in result["field_errors"]], "naming.runtime_hints"
    )
    assert any(
        "forbidden secret-shaped substring" in fe["message"]
        for fe in result["field_errors"]
    ), f"expected secret-shape rejection, got {result['field_errors']!r}"


def test_runtime_hints_rejects_nested_secret_key():
    """The scan must descend into nested dicts so callers can't bypass it by
    wrapping the secret-shaped key one level deeper."""
    payload = _valid_minimal()
    payload["naming"]["runtime_hints"] = {"outer": {"api_key": "<<sentinel>>"}}
    result = _build(payload)
    assert result["_success"] is False
    assert result["error_code"] == "PARAM_VALIDATION_FAILED"


def test_runtime_hints_rejection_does_not_echo_offending_value():
    sentinel = "sk_live_RUNTIME_HINTS_GUARD_DEADBEEF"
    payload = _valid_minimal()
    payload["naming"]["runtime_hints"] = {"authorization": sentinel}
    result = _build(payload)
    assert result["_success"] is False
    assert sentinel not in json.dumps(result), (
        "runtime_hints secret-shaped-key rejection must not echo the value"
    )


def test_runtime_hints_accepts_non_secret_keys():
    """Legitimate runtime hints (atom selection, environment tags) must pass
    the secret-shaped-key scan."""
    payload = _valid_minimal()
    payload["naming"]["runtime_hints"] = {"atom_pool": "primary", "env_tag": "prod"}
    params = DatabaseToApiSyncArchetype.validate_parameters(payload)
    assert isinstance(params, DatabaseToApiSyncParameters)


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


def test_unmapped_required_target_leaf_fails():
    """Issue #43 review r2 P2: a required simple leaf in target.payload_profile
    that no transform op binds must be rejected at parameter-validation time,
    not silently emit a spec that #26 would then turn into a payload missing a
    required field."""
    payload = _valid_minimal()
    payload["target"]["payload_profile"]["root"]["children"] = [
        {"name": "target_a", "kind": "simple", "data_type": "character"},
        {
            "name": "required_b",
            "kind": "simple",
            "data_type": "character",
            "required": True,
        },
    ]
    # transform.operations binds only target_a; required_b is unmapped.
    payload["transform"]["operations"] = [
        {
            "operation_type": "direct",
            "source_field": "source_a",
            "target_path": "Root/target_a",
        },
    ]
    result = _build(payload)
    assert result["_success"] is False, result
    assert result["error_code"] == "PARAM_VALIDATION_FAILED", result
    assert any(
        "required target leaf path" in fe["message"]
        and "unmapped" in fe["message"]
        for fe in result["field_errors"]
    ), f"expected unmapped-required-leaf rejection, got {result['field_errors']!r}"


def test_required_target_leaf_mapped_via_map_script_succeeds():
    """Coverage check: map_script outputs count as binding the required leaf."""
    payload = _valid_minimal()
    payload["target"]["payload_profile"]["root"]["children"] = [
        {"name": "target_a", "kind": "simple", "data_type": "character"},
        {
            "name": "required_b",
            "kind": "simple",
            "data_type": "character",
            "required": True,
        },
    ]
    payload["transform"]["operations"] = [
        {
            "operation_type": "direct",
            "source_field": "source_a",
            "target_path": "Root/target_a",
        },
        {
            "operation_type": "map_script",
            "script_slot": "fill_required_b",
            "language": "groovy2",
            "inputs": ["source_a"],
            "outputs": ["Root/required_b"],
            # Issue #29 assembles an executable script-route map, so an inline
            # body (or a '$ref:KEY') is required.
            "script_body": "<<task-authored fill script>>",
        },
    ]
    result = _build(payload)
    assert result["_success"] is True, result


def test_required_target_leaf_inside_array_must_be_mapped():
    """A required simple leaf reachable only via an array repetition segment
    is still required — its path is Root/<array>[]/<leaf>."""
    payload = _valid_minimal()
    payload["target"]["payload_profile"]["root"]["children"] = [
        {"name": "target_a", "kind": "simple", "data_type": "character"},
        {
            "name": "list",
            "kind": "array",
            "children": [
                {
                    "name": "required_key",
                    "kind": "simple",
                    "data_type": "character",
                    "required": True,
                },
            ],
        },
    ]
    payload["transform"]["operations"] = [
        {
            "operation_type": "direct",
            "source_field": "source_a",
            "target_path": "Root/target_a",
        },
    ]
    result = _build(payload)
    assert result["_success"] is False
    assert any(
        "required target leaf path" in fe["message"]
        for fe in result["field_errors"]
    ), f"expected nested-required-leaf rejection, got {result['field_errors']!r}"


def test_unmapped_required_leaf_error_does_not_echo_paths():
    """Same defense-in-depth as duplicate-target-binding: the cross-field error
    must not echo caller-supplied leaf paths back through the envelope."""
    sentinel = "sk_live_REQUIRED_LEAF_ECHO_GUARD_DEADBEEF"
    payload = _valid_minimal()
    payload["target"]["payload_profile"]["root"]["children"] = [
        {"name": "target_a", "kind": "simple", "data_type": "character"},
        {
            "name": sentinel,
            "kind": "simple",
            "data_type": "character",
            "required": True,
        },
    ]
    payload["transform"]["operations"] = [
        {
            "operation_type": "direct",
            "source_field": "source_a",
            "target_path": "Root/target_a",
        },
    ]
    result = _build(payload)
    assert result["_success"] is False
    assert sentinel not in json.dumps(result["field_errors"]), (
        "unmapped-required-leaf error must not echo caller-supplied target "
        "leaf paths back to the caller"
    )


def test_required_structural_node_does_not_demand_binding():
    """Required `object` / `array` nodes are structural — they are not
    transform-targetable, so the unmapped-required check must NOT fire on
    them. Only required `kind='simple'` leaves count.
    """
    payload = _valid_minimal()
    payload["target"]["payload_profile"]["root"]["children"] = [
        {"name": "target_a", "kind": "simple", "data_type": "character"},
        {
            "name": "list",
            "kind": "array",
            "required": True,
            "children": [
                {
                    "name": "key",
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
            "target_path": "Root/target_a",
        },
    ]
    # Required leaf `Root/list[]/key` is itself optional, and the required
    # `list` array node is structural — neither demands a binding.
    result = _build(payload)
    assert result["_success"] is True, result


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


# ===========================================================================
# #159 S2 lane L1 — the typed preset ``boomi.archetype.database_to_api_sync``
# 1.0.0. The legacy archetype above stays the ORACLE through S2: every test
# below compares the preset with it, with its frozen #157 rows, or with its
# independently frozen goldens — never with the preset's own output.
# ===========================================================================

import copy as _copy
import functools as _functools
import hashlib as _hashlib
import re as _re
import sys as _sys
from pathlib import Path as _Path
from unittest.mock import MagicMock as _MagicMock
from unittest.mock import patch as _patch

import pytest as _pytest

_TESTS_DIR = _Path(__file__).resolve().parent.parent
if str(_TESTS_DIR) not in _sys.path:
    _sys.path.insert(0, str(_TESTS_DIR))

L1_PRODUCER = "database_to_api_sync"
L1_CASE_IDS = (
    "wm_off_dlq_off_create",
    "wm_off_dlq_off_source_reuse",
    "wm_off_dlq_off_target_reuse",
    "wm_off_dlq_on_create",
    "wm_on_dlq_off_create",
    "wm_on_dlq_on_create",
    "wm_on_dlq_on_reuse",
)
#: The base key the canonical fixtures give the existing dead-letter cache.
L1_DLQ_CACHE_KEY = "dlq_cache"
L1_INVOCATION_ID = "db_to_api"
#: The account listing a MagicMock client would page forever; answered empty.
_L1_PAGINATE = "boomi_mcp.categories.integration_builder.paginate_metadata"
_CREDENTIAL_SCHEME = "credential://"

#: The DLQ cache's ComponentGet answer for the offline replay is the independent
#: live capture ``replay_identity/cap159-s2-dlq-cache`` (an unprofiled Document
#: Cache a green platform execution exercised; see its PROVENANCE.md). The m11
#: PROFILED Document Cache capture below stays as the negative control: the
#: production classifier must read it as a profiled cache.
L1_DLQ_CAPTURE_SOURCE = _TESTS_DIR / "fixtures" / "live_xml" / "m11" / "documentcache_profile_key_component.xml"
L1_DLQ_CAPTURE_SHA256 = "9a4bab3ed6516874ea785caff8e1d1b4168287b9c556e07b846da9b90417f394"
L1_DLQ_CACHE_DIR = _TESTS_DIR / "fixtures" / "boomi" / "replay_identity" / "cap159-s2-dlq-cache"


def l1_profiled_cache_xml() -> str:
    data = L1_DLQ_CAPTURE_SOURCE.read_bytes()
    assert _hashlib.sha256(data).hexdigest() == L1_DLQ_CAPTURE_SHA256, "the live capture moved"
    return data.decode("utf-8")


def l1_unprofiled_cache_xml() -> str:
    return (L1_DLQ_CACHE_DIR / "dlq-cache-unprofiled.xml").read_bytes().decode("utf-8")


def l1_cache_answers(legacy_cache_id: str, xml: "str | None"):
    """The replay identity answers plus the DLQ cache's (``None``: unreadable)."""
    import _issue_157_flows_accounting as accounting

    answers = dict(accounting.replay_identity_captures())
    answers.pop(legacy_cache_id, None)
    if xml is not None:
        answers[legacy_cache_id] = {"component_id": legacy_cache_id, "version": "1", "xml": xml}
    return answers


def l1_canonical_request(case_id: str, legacy_input: Dict[str, Any], map_label: str) -> Dict[str, Any]:
    """THE derivation of a frozen case's ``input.canonical.json`` (provenance).

    A mechanical re-spelling of the case's immutable ``input.legacy.json``:
    secret-free identities and classifications into ``raw_input``; connection
    settings, SQL, mapping, payload profile, path and query into the closed
    ``component_material``; the legacy literal DLQ cache id into ONE
    reference-only ``documentcache`` base component selected by key. Re-spelled
    values: a create-mode DB ``credential_ref`` under the opaque
    ``credential://`` scheme (the shared boundary admits only that form);
    map-function parameters from a mapping to ``{name, value}`` entries;
    ``max_attempts`` is ALWAYS 1 (the architect retry decision — the legacy
    ``wm_on_dlq_on_reuse`` asked for 5, which this preset refuses rather than
    reduces). Dropped, metadata-only legacy fields that no served row or
    emitted component reads: ``integration_name``, ``runtime_hints``, the
    trigger/schedule, ``run_metadata``, ``error_classifier``, ``backoff``, the
    watermark ``persistence`` ("dpp", the only wired-or-recorded form). The one
    added value is the map label: the frozen transform row's display name.
    """
    p = _copy.deepcopy(legacy_input["parameters"])
    naming, source, target = p["naming"], p["source"], p["target"]
    reliability, execution = p["reliability"], p["execution"]
    raw: Dict[str, Any] = {
        "version": "1",
        "process_key": "main_process",
        "naming": {"component_prefix": naming["component_prefix"]},
        "source": {"binding_mode": source["binding"]["mode"]},
        "transform": {"map_label": map_label},
        "target": {
            "binding_mode": target["binding"]["mode"],
            "method": target["send_request"].get("method", "POST"),
        },
        "reliability": {"retry": {"max_attempts": 1}},
    }
    if naming.get("folder_path"):
        raw["naming"]["folder_name"] = naming["folder_path"]
    if source["binding"]["mode"] == "create":
        raw["source"]["auth_mode"] = source["binding"]["settings"]["auth_mode"]
    base_components: List[Dict[str, Any]] = []
    if reliability["dlq"].get("enabled"):
        raw["reliability"]["dlq"] = {"cache_key": L1_DLQ_CACHE_KEY}
        base_components.append(
            {
                "key": L1_DLQ_CACHE_KEY,
                "type": "documentcache",
                "action": "create",
                "config": {
                    "reference_only": True,
                    "component_id": reliability["dlq"]["target"]["document_cache_id"],
                },
            }
        )
        if reliability.get("catch_notify"):
            raw["reliability"]["catch_notify"] = dict(reliability["catch_notify"])
    query_parameters = target["send_request"].get("query_parameters", [])
    watermark = execution.get("watermark")
    if watermark:
        raw["watermark"] = {
            "field": watermark["field"],
            "kind": watermark["kind"],
            "query_parameter_refs": [
                q["name"] for q in query_parameters if q["value_source"] == "watermark"
            ],
        }
        if watermark.get("initial_value") is not None:
            raw["watermark"]["initial_value"] = watermark["initial_value"]
    if p.get("environment_extensions"):
        raw["environment_extensions"] = p["environment_extensions"]

    binding = source["binding"]
    if binding["mode"] == "create":
        settings = binding["settings"]
        connection: Dict[str, Any] = {
            "mode": "create",
            "driver": settings["driver"],
            "host": settings["host"],
            "database": settings["database"],
        }
        for name in ("port", "username"):
            if name in settings:
                connection[name] = settings[name]
        if settings.get("credential_ref"):
            ref = settings["credential_ref"]
            connection["credential_ref"] = ref if ref.startswith(_CREDENTIAL_SCHEME) else _CREDENTIAL_SCHEME + ref
    else:
        connection = {"mode": "reuse", **{k: binding[k] for k in ("component_id", "component_name") if binding.get(k)}}
    read_operation = source["read_operation"]
    read: Dict[str, Any] = {
        "sql": read_operation["sql"],
        "result_fields": read_operation["result_schema"]["fields"],
    }
    if read_operation.get("parameters"):
        read["bind_parameters"] = [
            {k: v for k, v in parameter.items() if k in ("name", "direction")}
            for parameter in read_operation["parameters"]
        ]
    for name in ("batch_size", "max_rows"):
        if read_operation.get(name) is not None:
            read[name] = read_operation[name]
    operations = []
    for operation in p["transform"]["operations"]:
        operation = dict(operation)
        if operation["operation_type"] == "map_function" and isinstance(operation.get("parameters"), dict):
            operation["parameters"] = [{"name": k, "value": v} for k, v in operation["parameters"].items()]
        operations.append(operation)
    target_binding = target["binding"]
    if target_binding["mode"] == "create":
        target_connection = {"mode": "create", "base_url": target_binding["settings"]["base_url"]}
    else:
        target_connection = {
            "mode": "reuse",
            **{k: target_binding[k] for k in ("component_id", "component_name") if target_binding.get(k)},
        }
    material = {
        "version": "1",
        "source": {"connection": connection, "read": read},
        "transform": {"operations": operations},
        "payload_profile": target["payload_profile"],
        "target": {
            "connection": target_connection,
            "operation": {
                "path": target["send_request"]["path"],
                "query_parameters": [dict(q) for q in query_parameters],
            },
        },
    }
    intent: Dict[str, Any] = {
        "intent_kind": "recipe",
        "integration_name": "issue-157 replay " + case_id,
        "invocations": [
            {
                "recipe_id": "boomi.archetype.database_to_api_sync",
                "recipe_version": "1.0.0",
                "invocation_id": L1_INVOCATION_ID,
                "raw_input": raw,
                "component_material": material,
            }
        ],
    }
    if base_components:
        intent["base_components"] = base_components
    return {"contract_version": "2", "intent": intent}


def l1_registration():
    """The registration handed to the integrator, built in a TEST registry."""
    from boomi_mcp.recipes import component_material as _component_material
    from boomi_mcp.recipes.builtins import database_to_api_sync as preset
    from boomi_mcp.recipes.contracts import (
        RecipeCapabilityRequirementV1,
        RecipeConflictPolicyV1,
        RecipeRegistrationV1,
    )

    def _emitter(subject):
        return RecipeCapabilityRequirementV1(
            authority="process_emitter", subject=subject, required_state="supported"
        )

    return RecipeRegistrationV1(
        recipe_id=preset.RECIPE_ID,
        recipe_version=preset.RECIPE_VERSION,
        entry_kind="executable_recipe",
        is_default=True,
        input_model=preset.DatabaseToApiSyncInputV1,
        executor=preset.emit_database_to_api_sync,
        output_types=("constraint_requirement", "process_unit", "recorded_intent"),
        capability_requirements=(
            _emitter("connectoraction_source"),
            _emitter("connectoraction_target"),
            _emitter("map"),
            _emitter("catcherrors"),
            _emitter("doccacheload"),
            _emitter("notify"),
        ),
        conflict_policy=RecipeConflictPolicyV1(
            mode="error_unless_declared", merge_rules=("dedupe_identical_constraint",)
        ),
        component_material_model=preset.DatabaseToApiSyncMaterialV1,
        component_preparer=preset.prepare_database_to_api_sync,
        implementation_dependency_modules=(_component_material.__name__,),
    )


@_functools.lru_cache(maxsize=None)
def l1_registry():
    """Production registrations plus this preset — unless the catalog already
    carries it (after the integrator lands it), then production alone."""
    from boomi_mcp.recipes.builtins.catalog import PRODUCTION_REGISTRATIONS
    from boomi_mcp.recipes.registry import build_test_registry

    reg = l1_registration()
    present = any(
        (r.recipe_id, r.recipe_version) == (reg.recipe_id, reg.recipe_version)
        for r in PRODUCTION_REGISTRATIONS
    )
    return build_test_registry(PRODUCTION_REGISTRATIONS if present else PRODUCTION_REGISTRATIONS + (reg,))


def l1_plan(request: Dict[str, Any], *, answers=None, client: Any = "mock"):
    """The PUBLIC typed plan of a recipe request, with the replay transport
    answering the account's component reads (``answers``)."""
    import _issue_157_flows_accounting as accounting
    from boomi_mcp.authoring.workflow import plan_authoring_request_v1
    from boomi_mcp.models.authoring_workflow import AuthoringRequestV1
    from boomi_mcp.recipes import engine as engine_module

    typed = AuthoringRequestV1.model_validate(_copy.deepcopy(request))
    with _patch.object(engine_module, "production_registry", l1_registry), _patch(
        accounting._GET_XML, accounting.replay_component_get_xml(answers)
    ), _patch(_L1_PAGINATE, lambda *a, **k: []):
        return plan_authoring_request_v1(
            typed,
            boomi_client=_MagicMock() if client == "mock" else client,
            profile="issue-157-replay",
        )


def _l1_cases():
    import _issue_157_flows_accounting as accounting

    return accounting.load_cases()


def l1_case(case_id: str):
    return _l1_cases()[f"{L1_PRODUCER}/{case_id}"]


def l1_derived_request(case_id: str) -> Dict[str, Any]:
    case = l1_case(case_id)
    transform = next(row for row in case.flow_rows if row["key"] == "transform")
    return l1_canonical_request(case.case_id, case.input_legacy["input"], transform["payload"]["name"])


def _l1_dlq_cache_id(case_id: str) -> "str | None":
    dlq = l1_case(case_id).input_legacy["input"]["parameters"]["reliability"]["dlq"]
    return dlq["target"]["document_cache_id"] if dlq.get("enabled") else None


@_functools.lru_cache(maxsize=None)
def l1_replay(case_id: str):
    """The case's committed canonical fixture through the public planner, the
    DLQ cache answered with the unprofiled capture."""
    case = l1_case(case_id)
    cache_id = _l1_dlq_cache_id(case_id)
    answers = l1_cache_answers(cache_id, l1_unprofiled_cache_xml()) if cache_id else None
    result, _internals = l1_plan(case.input_canonical, answers=answers)
    return result


def _l1_case_dir(case_id: str) -> _Path:
    return _TESTS_DIR / "fixtures" / "governance" / "issue_157" / "cases" / L1_PRODUCER / case_id


def test_l1_registration_resolves_in_a_test_registry():
    from boomi_mcp.recipes.builtins import database_to_api_sync as preset

    descriptor = l1_registry().resolve(preset.RECIPE_ID, preset.RECIPE_VERSION)
    assert (descriptor.recipe_id, descriptor.recipe_version) == (
        "boomi.archetype.database_to_api_sync",
        "1.0.0",
    )
    assert descriptor.entry_kind == "executable_recipe"


def test_l1_committed_canonical_fixtures_are_their_derivation():
    for case_id in L1_CASE_IDS:
        committed = json.loads((_l1_case_dir(case_id) / "input.canonical.json").read_text())
        assert committed == l1_derived_request(case_id), case_id
        provenance = json.loads((_l1_case_dir(case_id) / "provenance.json").read_text())
        legacy = (_l1_case_dir(case_id) / "input.legacy.json").read_bytes()
        assert provenance["legacy_input_sha256"] == _hashlib.sha256(legacy).hexdigest(), case_id
        assert provenance["recipe"] == {
            "recipe_id": "boomi.archetype.database_to_api_sync", "recipe_version": "1.0.0"}


@_pytest.mark.parametrize("case_id", L1_CASE_IDS)
def test_l1_frozen_case_replays_full_flows_and_endpoints(case_id):
    """The committed recipe invocation, through the public planner, projects
    EXACTLY the frozen rows: full payload, in order, flows and endpoints."""
    from boomi_mcp.authoring.derived_flows import normalize_flow_row

    case = l1_case(case_id)
    result = l1_replay(case_id)
    assert not result.errors, [(d.code, d.cause_codes, d.path) for d in result.errors]
    preview = result.integration_spec_preview
    flows = [normalize_flow_row(row.model_dump(mode="json", exclude_unset=True)) for row in preview.flows]
    assert flows == [row["payload"] for row in case.flow_rows]
    endpoints = [row.model_dump(mode="json", exclude_unset=True) for row in preview.endpoints]
    assert endpoints == [row["payload"] for row in case.endpoint_rows]


def _l1_prepared(case_id: str):
    """The preparer's output for a case's committed fixture."""
    from boomi_mcp.models.integration_models import IntegrationComponentSpec
    from boomi_mcp.recipes.builtins import database_to_api_sync as preset

    intent = l1_case(case_id).input_canonical["intent"]
    invocation = intent["invocations"][0]
    base = {
        c["key"]: IntegrationComponentSpec.model_validate(c) for c in intent.get("base_components", [])
    }
    return preset.prepare_database_to_api_sync(
        preset.DatabaseToApiSyncInputV1.model_validate(invocation["raw_input"]),
        preset.DatabaseToApiSyncMaterialV1.model_validate(invocation["component_material"]),
        base_catalog=base,
    )


def _l1_legacy_spec(case_id: str) -> Dict[str, Any]:
    result = build_from_archetype_action(
        "database_to_api_sync", l1_case(case_id).input_legacy["input"]["parameters"]
    )
    assert result["_success"] is True, result
    return result["integration_spec"]


def _l1_baseline_components(case_id: str) -> List[Dict[str, Any]]:
    """The BASELINE archetype's component list for the case (process dropped),
    as the pre-#159 direct-unit fixture recorded it at ba1be9f (kept in git at
    ad3d01c; CI checks out full history)."""
    import subprocess

    path = f"tests/fixtures/governance/issue_157/cases/{L1_PRODUCER}/{case_id}/input.canonical.json"
    return json.loads(
        subprocess.run(
            ["git", "show", f"ad3d01c:{path}"], cwd=_TESTS_DIR.parent, check=True,
            capture_output=True, text=True,
        ).stdout
    )["intent"]["components"]


def test_l1_assembly_matches_the_legacy_archetype_and_the_baseline():
    """Per frozen case, against the legacy route at HEAD AND the baseline list
    (so a legacy change at HEAD cannot move both sides at once):

    * supporting components — keys, types, names, configs, dependencies, in
      order. The ONE re-spelling is the opaque ``credential://`` scheme on a
      created DB connection's reference. The ONE measured baseline difference:
      the baseline map carried each direct mapping's ``documentation_hint`` in
      ``field_mappings``; the route at HEAD does not (pinned here), and the hint
      is served from the transform row's facts instead.
    * the unit envelope — name, folder, the legacy dependency edges (+ the DLQ
      cache the root now references by key) and the extension declarations,
      derived from the ACTUAL bindings.
    """
    from _fanout_v2_support import respell_oracle_credential_refs
    from boomi_mcp.recipes.builtins import database_to_api_sync as preset

    for case_id in L1_CASE_IDS:
        spec = _l1_legacy_spec(case_id)
        legacy = [c for c in spec["components"] if c["type"] != "process"]
        prepared = [c.model_dump(mode="json") for c in _l1_prepared(case_id).supporting_components]
        assert prepared == respell_oracle_credential_refs(legacy), case_id
        baseline = _l1_baseline_components(case_id)
        for component in baseline:
            for mapping in (component.get("config") or {}).get("field_mappings") or ():
                mapping.pop("documentation_hint", None)
        assert prepared == respell_oracle_credential_refs(baseline), case_id

        process = next(c for c in spec["components"] if c["type"] == "process")
        raw = l1_case(case_id).input_canonical["intent"]["invocations"][0]["raw_input"]
        unit = preset.emit_database_to_api_sync(preset.DatabaseToApiSyncInputV1.model_validate(raw))[0].unit
        envelope = unit.envelope
        assert envelope.name == process["name"], case_id
        assert envelope.folder_name == process["config"].get("folder_name"), case_id
        dlq = ("dlq_cache",) if _l1_dlq_cache_id(case_id) else ()
        # The envelope model stores its edges canonically sorted.
        assert sorted(envelope.depends_on) == sorted(tuple(process["depends_on"]) + dlq), case_id
        declared = envelope.process_extensions.model_dump(mode="json", exclude_none=True)["connections"]
        assert declared == process["config"].get("process_extensions", {}).get("connections", []), case_id


# --- extension bindings per binding mode -------------------------------------


def _l1_raw(**overrides) -> Dict[str, Any]:
    raw = _copy.deepcopy(l1_case("wm_off_dlq_off_create").input_canonical["intent"]["invocations"][0]["raw_input"])
    for path, value in overrides.items():
        node = raw
        parts = path.split("__")
        for part in parts[:-1]:
            node = node.setdefault(part, {})
        if value is None:
            node.pop(parts[-1], None)
        else:
            node[parts[-1]] = value
    return raw


def _l1_declared(raw: Dict[str, Any]):
    from boomi_mcp.recipes.builtins import database_to_api_sync as preset

    unit = preset.emit_database_to_api_sync(preset.DatabaseToApiSyncInputV1.model_validate(raw))[0].unit
    return [
        (c.connection_id, c.connector_type, tuple(f.id for f in c.fields))
        for c in unit.envelope.process_extensions.connections
    ]


def test_l1_extension_bindings_follow_the_actual_binding():
    db = ("$ref:source_db_connection", "database", ("username", "password"))
    rest = ("$ref:target_rest_connection", "rest", ("username", "password"))
    for source_mode, auth_mode, target_mode, expected in (
        ("create", "username_password", "create", [db]),
        ("create", "windows_integrated", "create", []),
        ("reuse", None, "create", [db]),
        ("create", "username_password", "reuse", [db, rest]),
        ("reuse", None, "reuse", [db, rest]),
    ):
        raw = _l1_raw(source__binding_mode=source_mode, source__auth_mode=auth_mode, target__binding_mode=target_mode)
        assert _l1_declared(raw) == expected, (source_mode, auth_mode, target_mode)


def test_l1_extension_switches_match_the_legacy_policy():
    """Each legacy switch, through the preset AND the legacy archetype."""
    from boomi_mcp.patterns.primitives.db_extract import db_connection_extension_fields
    from boomi_mcp.patterns.primitives.rest_send import rest_connection_extension_fields

    raw = _l1_raw(
        target__binding_mode="reuse",
        environment_extensions={
            "credential_connection_fields": False,
            "endpoint_connection_fields": True,
            "rest_credential_connection_fields": False,
            "rest_endpoint_connection_fields": True,
        },
    )
    db_fields = tuple(f["id"] for f in db_connection_extension_fields(credentials=False, endpoint=True))
    rest_fields = tuple(f["id"] for f in rest_connection_extension_fields(credentials=False, endpoint=True))
    assert _l1_declared(raw) == [
        ("$ref:source_db_connection", "database", db_fields),
        ("$ref:target_rest_connection", "rest", rest_fields),
    ]
    assert db_fields and rest_fields


# --- create versus reused connections (base keys) ---------------------------


def _l1_invocation(case_id: str = "wm_off_dlq_off_create"):
    intent = _copy.deepcopy(l1_case(case_id).input_canonical["intent"])
    return intent, intent["invocations"][0]


def _l1_prepare(raw, material, base=()):
    from boomi_mcp.models.integration_models import IntegrationComponentSpec
    from boomi_mcp.recipes.builtins import database_to_api_sync as preset

    return preset.prepare_database_to_api_sync(
        preset.DatabaseToApiSyncInputV1.model_validate(raw),
        preset.DatabaseToApiSyncMaterialV1.model_validate(material),
        base_catalog={c["key"]: IntegrationComponentSpec.model_validate(c) for c in base},
    )


def _l1_refusal(raw, material, base=()):
    from boomi_mcp.recipes.errors import RecipeError

    with _pytest.raises(RecipeError) as caught:
        _l1_prepare(raw, material, base)
    (diagnostic,) = caught.value.diagnostics
    return diagnostic.cause_codes, diagnostic.path


_L1_BASE_DB = {
    "key": "shared_db", "type": "connector-settings", "action": "create",
    "config": {"connector_type": "database", "reference_only": True, "component_id": "<<existing connector id>>"},
}
_L1_BASE_REST = {
    "key": "shared_rest", "type": "connector-settings", "action": "create",
    "config": {"connector_type": "rest", "reference_only": True, "component_id": "<<existing REST connection id>>"},
}


def test_l1_reused_base_connections_bind_by_key_on_each_side():
    _intent, invocation = _l1_invocation()
    raw, material = invocation["raw_input"], invocation["component_material"]
    raw["source"] = {"binding_mode": "reuse", "connection_key": "shared_db"}
    raw["target"]["binding_mode"] = "reuse"
    raw["target"]["connection_key"] = "shared_rest"
    material["source"].pop("connection")
    material["target"].pop("connection")
    prepared = _l1_prepare(raw, material, (_L1_BASE_DB, _L1_BASE_REST))
    keys = [c.key for c in prepared.supporting_components]
    assert "source_db_connection" not in keys and "target_rest_connection" not in keys
    assert prepared.selected_base_keys == ("shared_db", "shared_rest")
    by_key = {c.key: c for c in prepared.supporting_components}
    assert by_key["source_db_get_operation"].config["connection_ref_key"] == "shared_db"
    assert by_key["target_rest_operation"].config["connection_ref_key"] == "shared_rest"
    assert prepared.rest_request_targets[0][0] == "shared_rest"
    assert prepared.projection_facts.source.connection_key == "shared_db"
    assert prepared.projection_facts.target.connection_key == "shared_rest"


def test_l1_connection_binding_refusals_are_value_free():
    _intent, invocation = _l1_invocation()
    raw, material = invocation["raw_input"], invocation["component_material"]
    # A base key selected while the material ALSO carries the connection.
    selected = _copy.deepcopy(raw)
    selected["source"] = {"binding_mode": "reuse", "connection_key": "shared_db"}
    causes, path = _l1_refusal(selected, material, (_L1_BASE_DB,))
    assert path == "/component_material/source/connection"
    # A reuse binding to a base apply would WRITE (not reference-only).
    written = _copy.deepcopy(_L1_BASE_DB)
    written["config"].pop("reference_only")
    bare = _copy.deepcopy(material)
    bare["source"].pop("connection")
    causes, path = _l1_refusal(selected, bare, (written,))
    assert (causes, path) == (("COMPOSITION_CONTRACT_MISMATCH",), "/raw_input/source/connection_key")
    # The two sides may not select the same base.
    same = _copy.deepcopy(selected)
    same["target"].update({"binding_mode": "reuse", "connection_key": "shared_db"})
    both = _copy.deepcopy(bare)
    both["target"].pop("connection")
    causes, path = _l1_refusal(same, both, (_L1_BASE_DB,))
    assert path == "/raw_input/target/connection_key"


def test_l1_windows_integrated_create_is_refused_before_assembly():
    _intent, invocation = _l1_invocation()
    raw = invocation["raw_input"]
    raw["source"]["auth_mode"] = "windows_integrated"
    material = invocation["component_material"]
    material["source"]["connection"].pop("username")
    material["source"]["connection"].pop("credential_ref")
    causes, path = _l1_refusal(raw, material)
    assert path.startswith("/component_material/source")
    assert causes


def test_l1_transform_contract_refusals():
    _intent, invocation = _l1_invocation()
    raw, material = invocation["raw_input"], invocation["component_material"]
    unknown = _copy.deepcopy(material)
    unknown["transform"]["operations"][0]["source_field"] = "not_a_result_field"
    assert _l1_refusal(raw, unknown) == (
        ("COMPOSITION_CONTRACT_MISMATCH",), "/component_material/transform/operations/0")
    required = _copy.deepcopy(material)
    required["payload_profile"]["root"]["children"].append(
        {"name": "target_req", "kind": "simple", "data_type": "character", "required": True}
    )
    assert _l1_refusal(raw, required) == (
        ("COMPOSITION_CONTRACT_MISMATCH",), "/component_material/payload_profile")


# --- watermark placement ------------------------------------------------------


def test_l1_watermark_rides_the_fan_out_governance_path():
    """Recorded (never wired) on the unit's process key, sourced by the DB read
    profile; the watermark-sourced query parameter is deferred, never emitted,
    and the served watermark row follows the reliability row."""
    from boomi_mcp.recipes.builtins import database_to_api_sync as preset

    _intent, invocation = _l1_invocation("wm_on_dlq_on_reuse")
    raw = invocation["raw_input"]
    contributions = preset.emit_database_to_api_sync(preset.DatabaseToApiSyncInputV1.model_validate(raw))
    (recorded,) = [c for c in contributions if c.contribution_kind == "recorded_intent"]
    assert recorded.process_key == "main_process" and recorded.status == "recorded_not_wired"
    assert recorded.declaration.source_profile_ref == "$ref:source_db_read_profile"
    assert recorded.declaration.query_parameter_refs == ("since",)
    prepared = _l1_prepared("wm_on_dlq_on_reuse")
    assert prepared.deferred_query_parameter_names == ((("main_process", "target_rest_operation"), ("since",)),)
    operation = next(c for c in prepared.supporting_components if c.key == "target_rest_operation")
    assert "query_parameters" not in operation.config
    keys = [row.key for row in l1_replay("wm_on_dlq_on_reuse").integration_spec_preview.flows]
    assert keys == ["extract", "transform", "send", "reliability", "watermark"]


def test_l1_watermark_refusals():
    _intent, invocation = _l1_invocation("wm_on_dlq_on_reuse")
    raw, material = invocation["raw_input"], invocation["component_material"]
    base = invocation_base = _intent.get("base_components", [])
    no_watermark = _copy.deepcopy(raw)
    no_watermark.pop("watermark")
    assert _l1_refusal(no_watermark, material, base) == (
        ("GOVERNANCE_WATERMARK_INCONSISTENT",), "/component_material/target/operation/query_parameters")
    unknown_field = _copy.deepcopy(raw)
    unknown_field["watermark"]["field"] = "not_a_result_field"
    assert _l1_refusal(unknown_field, material, base) == (
        ("GOVERNANCE_WATERMARK_INCONSISTENT",), "/raw_input/watermark/field")
    omitted = _copy.deepcopy(raw)
    omitted["watermark"]["query_parameter_refs"] = []
    assert _l1_refusal(omitted, material, base) == (
        ("GOVERNANCE_WATERMARK_INCONSISTENT",), "/component_material/target/operation/query_parameters/0")
    stray = _copy.deepcopy(raw)
    stray["watermark"]["query_parameter_refs"] = ["since", "until"]
    assert _l1_refusal(stray, material, base) == (
        ("GOVERNANCE_WATERMARK_INCONSISTENT",), "/raw_input/watermark/query_parameter_refs/1")
    del invocation_base


def test_l1_replay_identity_dlq_capture_is_the_recorded_transform():
    """The replay transport's DLQ cache answer (``replay_identity/cap159-s2-dlq-cache``)
    is an independent live capture of an unprofiled Document Cache that a green
    platform execution exercised (not a byte transform of another capture), bound
    to the frozen cases' placeholder; the production classifier reads it as an
    unprofiled cache (and the m11 profiled capture as profiled)."""
    from boomi_mcp.authoring.connector_resolution_snapshot import stored_document_cache_profile

    directory = L1_DLQ_CACHE_DIR
    (record,) = json.loads((directory / "records.json").read_text())
    data = (directory / (record["capture"] + ".xml")).read_bytes()
    assert data == l1_unprofiled_cache_xml().encode("utf-8")
    assert record["sha256_archived_bytes"] == _hashlib.sha256(data).hexdigest()
    assert record["byte_length"] == len(data)
    assert "live platform ComponentGet" in record["source"]
    assert "NOT authored by the #159 S2 implementation" in record["authored_by"]
    assert "derived_from" not in record and "byte_transform" not in record
    # Operable, not merely stored: a COMPLETE execution ran a DLQ shape bound to it.
    evidence = record["operable_evidence"]
    assert evidence and all(row["status"] == "COMPLETE" for row in evidence)
    assert any("executed successfully" in row["what"] for row in evidence)
    assert record["binds_placeholder_ids"] == ["<<dlq document cache component id>>"]
    assert stored_document_cache_profile("c", data.decode("utf-8")).profile_state == "absent"
    assert stored_document_cache_profile("c", l1_profiled_cache_xml()).profile_state == "known"

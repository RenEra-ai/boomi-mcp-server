"""Tests for the contract-only database_to_api_sync archetype (Issue #21)."""

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
                "sql": "<<user-authored SELECT statement>>",
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
        },
        "transform": {
            "mappings": [
                {
                    "source_field": "<<source field name>>",
                    "target_field": "<<target field name>>",
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
    """Fuller payload: scheduled, watermark, retry, DLQ enabled, run metadata."""
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
                "sql": "<<user-authored incremental SELECT statement>>",
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
        },
        "transform": {
            "mappings": [
                {"source_field": "<<source a>>", "target_field": "<<target a>>"},
                {"source_field": "<<source b>>", "target_field": "<<target b>>", "transform_hint": "trim"},
            ],
            "payload_template": "<<user-authored payload template>>",
            "script_slots": {"pre_send": "<<user-authored hook body>>"},
        },
        "execution": {
            "trigger": {
                "mode": "scheduled",
                "schedule": {"cron": "<<cron expression>>", "timezone": "UTC"},
            },
            "watermark": {
                "field": "<<watermark column>>",
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
    assert spec["name"] == "demo-db-to-api-sync"


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


def test_empty_mappings_returns_field_error():
    payload = _valid_minimal()
    payload["transform"]["mappings"] = []
    paths = _field_paths(_build(payload))
    _assert_path_match(paths, "transform.mappings")


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

"""Issue #74 (M5.8): api_to_database_sync preset over sync_pipeline — end-to-end.

Proves the preset's contract and the full local chain through the *public*
action entry points, without live Boomi:

    build_from_archetype_action  ->  build_integration_action(plan / apply)

Acceptance criteria covered:
* Preset maps to ``sync_pipeline`` stages (main process carries
  ``process_kind="sync_pipeline"`` with an intact fetch -> map -> write graph and
  no pairwise source/target/transform process config).
* Database write behavior routes through the confirmed #32 component builders
  (a profile.db write profile + a database Send connector-action), and the
  transform map binds that write profile (Fields/Conditions) as its target.
* Unconfirmed write-profile variants (e.g. ``upsert``) remain blocked
  (UNSUPPORTED_DB_STATEMENT_TYPE).
* No raw XML or canned payload templates are exposed.

Payloads use sentinel placeholders only (``<<...>>``), never canned payloads,
mappings, raw XML, credentials, or live account IDs.
"""

from __future__ import annotations

import copy
import json
from typing import Any, Dict
from unittest.mock import MagicMock, patch

import pytest

from boomi_mcp.categories.integration_authoring import (
    build_from_archetype_action,
    get_integration_archetype_action,
    list_integration_archetypes_action,
)
from boomi_mcp.categories.integration_builder import build_integration_action
from boomi_mcp.categories.meta_tools import get_schema_template_action

_ARCHETYPE = "api_to_database_sync"
_PAGINATE_TARGET = "boomi_mcp.categories.integration_builder.paginate_metadata"
_EXECUTE_TARGET = "boomi_mcp.categories.integration_builder._execute_component"
_PROFILE = "qa-profile"  # never used to reach Boomi; calls are fully mocked

_EXPECTED_KEYS = {
    "source_response_profile",
    "source_rest_source_connection",
    "source_rest_source_operation",
    "target_db_connection",
    "target_db_write_profile",
    "target_db_write_operation",
    "transform_transform_map",
    "main_process",
}
_STAGE_DEPENDS = [
    "source_rest_source_connection",
    "source_rest_source_operation",
    "transform_transform_map",
    "target_db_connection",
    "target_db_write_operation",
]


# ---------------------------------------------------------------------------
# Payload builders (synthetic, sentinel-only)
# ---------------------------------------------------------------------------


def _minimal() -> Dict[str, Any]:
    """Smallest executable payload: create REST source (no auth) + reuse DB
    target connection + a dynamic-insert write profile + a single direct
    transform into a write-profile column."""
    return {
        "naming": {"integration_name": "demo-api-db-sync", "component_prefix": "DEMO"},
        "source": {
            "binding": {
                "mode": "create",
                "settings": {
                    "base_url": "https://source.example.com",
                    "auth_mode": "none",
                },
            },
            "fetch_request": {"path": "/v1/<<source resource>>"},
            "response_profile": {
                "format": "json",
                "root": {
                    "name": "Root",
                    "kind": "object",
                    "children": [
                        {"name": "source_a", "kind": "simple", "data_type": "character"}
                    ],
                },
            },
        },
        "target": {
            "connection": {"mode": "reuse", "component_id": "<<existing db conn id>>"},
            "write_profile": {
                "statement_type": "dynamicinsert",
                "table_name": "<<target table>>",
                "fields": [
                    {"name": "col_a", "data_type": "character", "mandatory": True}
                ],
            },
        },
        "transform": {
            "operations": [
                {
                    "operation_type": "direct",
                    "source_path": "Root/source_a",
                    "target_path": "Fields/col_a",
                }
            ]
        },
    }


def _dynamic_update() -> Dict[str, Any]:
    """Reuse-mode DB target with a dynamic update (Fields + WHERE Conditions) and
    a map_function."""
    return {
        "naming": {
            "integration_name": "demo-api-db-upsert",
            "component_prefix": "DEMO-UPD",
            "folder_path": "Integrations/API/DB",
        },
        "source": {
            "binding": {"mode": "reuse", "component_id": "<<existing source conn id>>"},
            "fetch_request": {
                "path": "/v1/<<source resource>>",
                "query_parameters": {"limit": "100"},
            },
            "response_profile": {
                "format": "json",
                "root": {
                    "name": "Root",
                    "kind": "object",
                    "children": [
                        {"name": "id", "kind": "simple", "data_type": "character"},
                        {"name": "updated_at", "kind": "simple", "data_type": "datetime"},
                    ],
                },
            },
        },
        "target": {
            "connection": {"mode": "reuse", "component_name": "<<existing db conn>>"},
            "write_profile": {
                "statement_type": "dynamicupdate",
                "table_name": "<<target table>>",
                "fields": [{"name": "modified", "data_type": "datetime"}],
                "conditions": [{"name": "key_id", "data_type": "character"}],
            },
        },
        "transform": {
            "operations": [
                {
                    "operation_type": "direct",
                    "source_path": "Root/id",
                    "target_path": "Conditions/key_id",
                },
                {
                    "operation_type": "map_function",
                    "function_type": "date_format",
                    "inputs": ["Root/updated_at"],
                    "target_path": "Fields/modified",
                    "parameters": {
                        "input_format": "<<source datetime format>>",
                        "output_format": "<<target datetime format>>",
                    },
                },
            ]
        },
    }


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _build(params: Dict[str, Any]) -> Dict[str, Any]:
    return build_from_archetype_action(_ARCHETYPE, params)


def _spec(params: Dict[str, Any]) -> Dict[str, Any]:
    result = _build(params)
    assert result["_success"] is True, result
    return result["integration_spec"]


def _by_key(spec: Dict[str, Any]) -> Dict[str, Dict[str, Any]]:
    return {c["key"]: c for c in spec["components"]}


def _plan(spec: Dict[str, Any], mock_pag, conflict_policy: str = "reuse") -> Dict[str, Any]:
    mock_pag.return_value = []
    return build_integration_action(
        MagicMock(),
        _PROFILE,
        "plan",
        {"integration_spec": spec, "conflict_policy": conflict_policy},
    )


def _has_ref_token(value: Any) -> bool:
    if isinstance(value, str):
        return value.startswith("$ref:")
    if isinstance(value, dict):
        return any(_has_ref_token(v) for v in value.values())
    if isinstance(value, list):
        return any(_has_ref_token(v) for v in value)
    return False


# ===========================================================================
# Catalog / schema discovery
# ===========================================================================


class TestCatalogAndSchema:
    def test_list_includes_api_to_database_sync(self):
        result = list_integration_archetypes_action(query="database")
        assert result["_success"] is True
        assert _ARCHETYPE in [a["name"] for a in result["archetypes"]]
        assert result["raw_xml_exposed"] is False

    def test_get_returns_strict_schema_without_xml(self):
        result = get_integration_archetype_action(_ARCHETYPE)
        assert result["_success"] is True
        assert result["raw_xml_exposed"] is False
        arch = result["archetype"]
        assert arch["capability_notes"], "must publish capability_notes"
        assert arch["limitations"], "must publish limitations"
        assert arch["examples"], "must publish at least one example"
        schema = arch["parameter_schema"]
        assert schema.get("additionalProperties") is False
        props = schema["properties"]
        assert props
        for name, prop in props.items():
            assert prop.get("description"), f"property {name!r} missing description"
        assert "<?xml" not in json.dumps(result)

    def test_schema_template_resolves(self):
        result = get_schema_template_action(schema_name=f"archetype:{_ARCHETYPE}")
        assert result["_success"] is True, result
        assert "<?xml" not in json.dumps(result)


# ===========================================================================
# Emitted spec maps to sync_pipeline stages
# ===========================================================================


class TestEmittedSpec:
    def test_minimal_emits_safe_serializable_spec(self):
        result = _build(_minimal())
        assert result["_success"] is True, result
        assert result["raw_xml_exposed"] is False
        assert result["boomi_mutation"] is False
        spec = result["integration_spec"]
        assert {c["key"] for c in spec["components"]} == _EXPECTED_KEYS
        assert json.loads(json.dumps(result)) == result

    def test_main_process_is_sync_pipeline_stage_graph(self):
        spec = _spec(_minimal())
        mp = _by_key(spec)["main_process"]
        assert mp["type"] == "process"
        cfg = mp["config"]
        assert cfg["process_kind"] == "sync_pipeline"
        # NOT a pairwise process builder: no top-level source/target/transform.
        assert "source" not in cfg
        assert "target" not in cfg
        assert "transform" not in cfg
        # Verified-linear fetch -> map -> write graph with two ordering edges.
        stages = cfg["pipeline"]["stages"]
        assert [s["kind"] for s in stages] == ["fetch", "map", "write"]
        assert [s["config"]["primitive"] for s in stages] == [
            "rest_fetch",
            "map",
            "db_write",
        ]
        deps = cfg["pipeline"]["dependencies"]
        assert [(d["from_stage"], d["to_stage"]) for d in deps] == [
            ("fetch", "map"),
            ("map", "write"),
        ]
        assert mp["depends_on"] == _STAGE_DEPENDS

    def test_spec_pipeline_mirrors_main_process_graph(self):
        spec = _spec(_minimal())
        mp = _by_key(spec)["main_process"]
        assert spec["pipeline"] is not None
        spec_stages = [(s["key"], s["kind"]) for s in spec["pipeline"]["stages"]]
        mp_stages = [(s["key"], s["kind"]) for s in mp["config"]["pipeline"]["stages"]]
        assert spec_stages == mp_stages == [("fetch", "fetch"), ("map", "map"), ("write", "write")]

    def test_source_binds_response_profile_no_request_body(self):
        spec = _spec(_minimal())
        fetch_op = _by_key(spec)["source_rest_source_operation"]["config"]
        assert fetch_op["method"] == "GET"
        assert fetch_op["response_profile_id"] == "$ref:source_response_profile"
        assert "request_profile_id" not in fetch_op

    def test_db_write_profile_and_send_operation_wiring(self):
        spec = _spec(_minimal())
        comps = _by_key(spec)
        write_profile = comps["target_db_write_profile"]["config"]
        assert write_profile["profile_type"] == "database.write"
        assert write_profile["statement_type"] == "dynamicinsert"
        send_op = comps["target_db_write_operation"]["config"]
        assert send_op["operation_mode"] == "send"
        assert send_op["write_profile_id"] == "$ref:target_db_write_profile"
        assert send_op["connection_ref_key"] == "target_db_connection"

    def test_transform_map_targets_the_write_profile(self):
        spec = _spec(_minimal())
        map_cfg = _by_key(spec)["transform_transform_map"]["config"]
        # The map binds the DB write profile (profile.db) as its target — not a
        # generated JSON payload profile.
        assert map_cfg["target_profile_id"] == "$ref:target_db_write_profile"
        assert map_cfg["target_profile_type"] == "profile.db"
        # Every direct target path addresses a write-profile column/condition.
        for mapping in map_cfg.get("field_mappings", []):
            assert mapping["target_path"].startswith(("Fields/", "Conditions/"))

    def test_no_generated_json_target_profile(self):
        # Unlike api_to_api_sync (REST target), there is no generated JSON target
        # payload profile — the DB write profile is the map target.
        spec = _spec(_minimal())
        assert "transform_target_profile" not in _by_key(spec)

    def test_no_raw_xml_in_spec(self):
        spec = _spec(_dynamic_update())
        assert "<?xml" not in json.dumps(spec)


# ===========================================================================
# Parameter validation (negative)
# ===========================================================================


class TestParameterValidation:
    def test_dynamic_path_token_rejected_source(self):
        bad = copy.deepcopy(_minimal())
        bad["source"]["fetch_request"]["path"] = "/v1/items/{id}"
        result = _build(bad)
        assert result["_success"] is False
        assert result["error_code"] == "PARAM_VALIDATION_FAILED"

    def test_unknown_source_path_rejected(self):
        bad = copy.deepcopy(_minimal())
        bad["transform"]["operations"][0]["source_path"] = "Root/does_not_exist"
        result = _build(bad)
        assert result["_success"] is False
        assert result["error_code"] == "PARAM_VALIDATION_FAILED"

    def test_unknown_db_target_path_rejected(self):
        bad = copy.deepcopy(_minimal())
        bad["transform"]["operations"][0]["target_path"] = "Fields/not_a_column"
        result = _build(bad)
        assert result["_success"] is False
        assert result["error_code"] == "PARAM_VALIDATION_FAILED"

    def test_unmapped_required_target_field_rejected(self):
        bad = copy.deepcopy(_minimal())
        # col_a is mandatory; drop the only operation that maps it.
        bad["transform"]["operations"] = [
            {"operation_type": "direct", "source_path": "Root/source_a", "target_path": "Fields/col_a"}
        ]
        bad["target"]["write_profile"]["fields"].append(
            {"name": "col_b", "data_type": "character", "mandatory": True}
        )
        result = _build(bad)
        assert result["_success"] is False
        assert result["error_code"] == "PARAM_VALIDATION_FAILED"

    def test_unsupported_statement_type_blocked(self):
        bad = copy.deepcopy(_minimal())
        bad["target"]["write_profile"]["statement_type"] = "upsert"
        result = _build(bad)
        assert result["_success"] is False
        assert result["error_code"] == "UNSUPPORTED_DB_STATEMENT_TYPE"

    def test_secured_source_create_auth_rejected(self):
        bad = copy.deepcopy(_minimal())
        bad["source"]["binding"]["settings"]["auth_mode"] = "bearer_token"
        bad["source"]["binding"]["settings"]["credential_ref"] = "credential://x"
        result = _build(bad)
        assert result["_success"] is False
        assert result["error_code"] == "UNSUPPORTED_REST_AUTH_MODE"

    def test_xslt_operation_rejected_with_pointer(self):
        bad = copy.deepcopy(_minimal())
        bad["transform"]["operations"] = [
            {"operation_type": "xslt", "source_path": "Root/source_a", "target_path": "Fields/col_a"}
        ]
        result = _build(bad)
        assert result["_success"] is False
        assert result["error_code"] == "PARAM_VALIDATION_FAILED"

    def test_external_script_ref_rejected(self):
        bad = copy.deepcopy(_minimal())
        bad["transform"]["operations"] = [
            {
                "operation_type": "map_script",
                "script_slot": "enrich",
                "language": "groovy2",
                "inputs": ["Root/source_a"],
                "outputs": ["Fields/col_a"],
                "script_component_ref": "$ref:some_script",
            }
        ]
        result = _build(bad)
        assert result["_success"] is False
        assert result["error_code"] == "UNSUPPORTED_SCRIPT_COMPONENT_REF"

    def test_map_script_without_body_or_ref_rejected_at_contract(self):
        # Issue #127 A2: this preset reuses MapScriptApiTransformOperation, so
        # the shared one-of validator rejects a map_script op with neither
        # script_body nor script_component_ref at the contract layer.
        bad = copy.deepcopy(_minimal())
        bad["transform"]["operations"] = [
            {
                "operation_type": "map_script",
                "script_slot": "enrich",
                "language": "groovy2",
                "inputs": ["Root/source_a"],
                "outputs": ["Fields/col_a"],
            }
        ]
        result = _build(bad)
        assert result["_success"] is False
        assert result["error_code"] == "PARAM_VALIDATION_FAILED"

    def test_runtime_hints_secret_shaped_key_rejected_via_shared_naming(self):
        # Issue #127 B1: the shared NamingConfig.runtime_hints secret scan
        # applies to this preset too.
        bad = copy.deepcopy(_minimal())
        secret = "sk_live_API_TO_DB_GUARD"
        bad["naming"]["runtime_hints"] = {"client_secret": secret}
        result = _build(bad)
        assert result["_success"] is False
        assert result["error_code"] == "PARAM_VALIDATION_FAILED"
        assert secret not in json.dumps(result)


# ===========================================================================
# Full local chain: plan + apply
# ===========================================================================


class TestFullLocalChain:
    @patch(_PAGINATE_TARGET)
    def test_plan_orders_main_process_last(self, mock_pag):
        spec = _spec(_minimal())
        plan = _plan(spec, mock_pag)
        assert plan["_success"] is True, plan
        order = plan["execution_order"]
        assert set(order) == _EXPECTED_KEYS
        assert len(order) == len(set(order))
        pos = {key: i for i, key in enumerate(order)}
        for comp in spec["components"]:
            for dep in comp["depends_on"]:
                assert pos[dep] < pos[comp["key"]], (dep, comp["key"])
        assert order[-1] == "main_process"

    @patch(_PAGINATE_TARGET)
    def test_plan_routes_main_process_through_process_flow_xml(self, mock_pag):
        spec = _spec(_minimal())
        plan = _plan(spec, mock_pag)
        steps_by_key = {s["key"]: s for s in plan["steps"]}
        assert "main_process" in steps_by_key
        # A clean plan with no validation errors proves the sync_pipeline config
        # (with a db_write target) validated end-to-end through SyncPipelineBuilder.
        assert not plan.get("errors"), plan.get("errors")
        assert steps_by_key["main_process"].get("validation_error") is None

    @patch(_PAGINATE_TARGET)
    def test_dynamic_update_plan_clean(self, mock_pag):
        spec = _spec(_dynamic_update())
        plan = _plan(spec, mock_pag)
        assert plan["_success"] is True, plan
        assert not plan.get("errors"), plan.get("errors")

    @patch(_PAGINATE_TARGET)
    @patch(_EXECUTE_TARGET)
    def test_mocked_apply_resolves_all_refs_before_execution(self, mock_exec, mock_pag):
        mock_pag.return_value = []
        executed: list = []

        def _exec(**kwargs):
            assert not _has_ref_token(kwargs["config"]), (
                f"unresolved $ref reached _execute_component for {kwargs['comp'].key}"
            )
            executed.append(kwargs["comp"].key)
            return {"_success": True, "component_id": f"id-{kwargs['comp'].key}"}

        mock_exec.side_effect = _exec
        spec = _spec(_minimal())
        result = build_integration_action(
            MagicMock(),
            _PROFILE,
            "apply",
            {"integration_spec": spec, "conflict_policy": "reuse", "dry_run": False},
        )
        assert result["_success"] is True, result
        # main_process executes last, after every dependency created an id.
        assert executed[-1] == "main_process"

    @patch(_PAGINATE_TARGET)
    @patch(_EXECUTE_TARGET)
    def test_dry_run_apply_does_not_execute(self, mock_exec, mock_pag):
        mock_pag.return_value = []
        spec = _spec(_minimal())
        result = build_integration_action(
            MagicMock(),
            _PROFILE,
            "apply",
            {"integration_spec": spec, "conflict_policy": "reuse", "dry_run": True},
        )
        assert result["_success"] is True, result
        mock_exec.assert_not_called()


if __name__ == "__main__":  # pragma: no cover
    raise SystemExit(pytest.main([__file__, "-q"]))

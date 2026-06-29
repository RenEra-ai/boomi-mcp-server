"""Issue #73 (M5.7): api_to_api_sync preset over sync_pipeline — end-to-end.

Proves the preset's contract and the full local chain through the *public*
action entry points, without live Boomi:

    build_from_archetype_action  ->  review_transformation_action
                                 ->  build_integration_action(plan / apply)

Acceptance criteria covered:
* Preset maps to ``sync_pipeline`` stages (main process carries
  ``process_kind="sync_pipeline"`` with an intact fetch -> map -> send graph and
  no pairwise source/target/transform process config).
* Generated plan is inspectable through the existing MCP planning/review flows
  (build_integration plan, review_transformation, get_schema_template).
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
from boomi_mcp.categories.transformation_review import review_transformation_action

_ARCHETYPE = "api_to_api_sync"
_PAGINATE_TARGET = "boomi_mcp.categories.integration_builder.paginate_metadata"
_EXECUTE_TARGET = "boomi_mcp.categories.integration_builder._execute_component"
_PROFILE = "qa-profile"  # never used to reach Boomi; calls are fully mocked

_EXPECTED_KEYS = {
    "source_response_profile",
    "source_rest_source_connection",
    "source_rest_source_operation",
    "transform_target_profile",
    "transform_transform_map",
    "target_rest_connection",
    "target_rest_operation",
    "main_process",
}
_STAGE_DEPENDS = [
    "source_rest_source_connection",
    "source_rest_source_operation",
    "transform_transform_map",
    "target_rest_connection",
    "target_rest_operation",
]


# ---------------------------------------------------------------------------
# Payload builders (synthetic, sentinel-only)
# ---------------------------------------------------------------------------


def _minimal() -> Dict[str, Any]:
    """Smallest executable payload: create REST source + create REST target (no
    auth) + a single direct transform."""
    return {
        "naming": {"integration_name": "demo-api-sync", "component_prefix": "DEMO"},
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
            "binding": {
                "mode": "create",
                "settings": {
                    "base_url": "https://target.example.com",
                    "auth_mode": "none",
                },
            },
            "send_request": {"method": "POST", "path": "/v1/<<target resource>>"},
            "payload_profile": {
                "format": "json",
                "root": {
                    "name": "Root",
                    "kind": "object",
                    "children": [
                        {"name": "target_a", "kind": "simple", "data_type": "character"}
                    ],
                },
            },
        },
        "transform": {
            "operations": [
                {
                    "operation_type": "direct",
                    "source_path": "Root/source_a",
                    "target_path": "Root/target_a",
                }
            ]
        },
    }


def _full_reuse() -> Dict[str, Any]:
    """Reuse-mode source + target with a required leaf and a map_function."""
    return {
        "naming": {
            "integration_name": "demo-api-incremental",
            "component_prefix": "DEMO-INC",
            "folder_path": "Integrations/API/Sync",
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
                        {"name": "id", "kind": "simple", "data_type": "character", "required": True},
                        {"name": "updated_at", "kind": "simple", "data_type": "datetime"},
                    ],
                },
            },
        },
        "target": {
            "binding": {"mode": "reuse", "component_id": "<<existing target conn id>>"},
            "send_request": {"method": "PUT", "path": "/v1/<<target resource>>"},
            "payload_profile": {
                "format": "json",
                "root": {
                    "name": "Root",
                    "kind": "object",
                    "children": [
                        {"name": "external_id", "kind": "simple", "data_type": "character", "required": True},
                        {"name": "modified", "kind": "simple", "data_type": "datetime"},
                    ],
                },
            },
        },
        "transform": {
            "operations": [
                {
                    "operation_type": "direct",
                    "source_path": "Root/id",
                    "target_path": "Root/external_id",
                },
                {
                    "operation_type": "map_function",
                    "function_type": "date_format",
                    "inputs": ["Root/updated_at"],
                    "target_path": "Root/modified",
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
    def test_list_includes_api_to_api_sync(self):
        result = list_integration_archetypes_action(query="api")
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
        # Strict contract: top-level forbids extra properties.
        assert schema.get("additionalProperties") is False
        props = schema["properties"]
        assert props
        for name, prop in props.items():
            assert prop.get("description"), f"property {name!r} missing description"
        # No raw XML markers anywhere in the describe payload.
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
        # JSON-only output: the entire envelope round-trips through JSON.
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
        # Verified-linear fetch -> map -> send graph with two ordering edges.
        stages = cfg["pipeline"]["stages"]
        assert [s["kind"] for s in stages] == ["fetch", "map", "send"]
        assert [s["config"]["primitive"] for s in stages] == [
            "rest_fetch",
            "map",
            "rest_send",
        ]
        deps = cfg["pipeline"]["dependencies"]
        assert [(d["from_stage"], d["to_stage"]) for d in deps] == [
            ("fetch", "map"),
            ("map", "send"),
        ]
        assert mp["depends_on"] == _STAGE_DEPENDS

    def test_spec_pipeline_mirrors_main_process_graph(self):
        spec = _spec(_minimal())
        mp = _by_key(spec)["main_process"]
        assert spec["pipeline"] is not None
        # spec.pipeline is a validated PipelineSpec (model-dumped with default
        # fields), so compare the essential graph shape, not raw dict equality.
        spec_stages = [(s["key"], s["kind"]) for s in spec["pipeline"]["stages"]]
        mp_stages = [(s["key"], s["kind"]) for s in mp["config"]["pipeline"]["stages"]]
        assert spec_stages == mp_stages == [("fetch", "fetch"), ("map", "map"), ("send", "send")]
        spec_edges = [(d["from_stage"], d["to_stage"]) for d in spec["pipeline"]["dependencies"]]
        mp_edges = [(d["from_stage"], d["to_stage"]) for d in mp["config"]["pipeline"]["dependencies"]]
        assert spec_edges == mp_edges == [("fetch", "map"), ("map", "send")]

    def test_source_binds_response_profile_no_request_body(self):
        spec = _spec(_minimal())
        comps = _by_key(spec)
        fetch_op = comps["source_rest_source_operation"]["config"]
        assert fetch_op["method"] == "GET"
        assert fetch_op["response_profile_id"] == "$ref:source_response_profile"
        # A GET fetch has no request profile (empty request body).
        assert "request_profile_id" not in fetch_op

    def test_target_operation_binds_generated_request_profile(self):
        spec = _spec(_minimal())
        send_op = _by_key(spec)["target_rest_operation"]["config"]
        assert send_op["request_profile_id"] == "$ref:transform_target_profile"
        assert send_op["request_profile_type"] == "json"
        assert send_op["method"] == "POST"

    def test_no_raw_xml_in_spec(self):
        spec = _spec(_full_reuse())
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

    def test_dynamic_path_token_rejected_target(self):
        bad = copy.deepcopy(_minimal())
        bad["target"]["send_request"]["path"] = "/v1/items/{id}"
        result = _build(bad)
        assert result["_success"] is False
        assert result["error_code"] == "PARAM_VALIDATION_FAILED"

    def test_runtime_bindings_key_rejected(self):
        bad = copy.deepcopy(_minimal())
        bad["source"]["fetch_request"]["runtime_bindings"] = [{"location": "path"}]
        result = _build(bad)
        assert result["_success"] is False
        assert result["error_code"] == "PARAM_VALIDATION_FAILED"

    def test_unknown_source_path_rejected(self):
        bad = copy.deepcopy(_minimal())
        bad["transform"]["operations"][0]["source_path"] = "Root/does_not_exist"
        result = _build(bad)
        assert result["_success"] is False
        assert result["error_code"] == "PARAM_VALIDATION_FAILED"

    def test_unmapped_required_target_leaf_rejected(self):
        bad = copy.deepcopy(_minimal())
        bad["target"]["payload_profile"]["root"]["children"][0]["required"] = True
        bad["transform"]["operations"] = []  # min_length=1 also fails, but unmapped is the intent
        result = _build(bad)
        assert result["_success"] is False
        assert result["error_code"] == "PARAM_VALIDATION_FAILED"

    def test_secured_create_auth_rejected(self):
        bad = copy.deepcopy(_minimal())
        bad["target"]["binding"]["settings"]["auth_mode"] = "bearer_token"
        bad["target"]["binding"]["settings"]["credential_ref"] = "credential://x"
        result = _build(bad)
        assert result["_success"] is False
        assert result["error_code"] == "UNSUPPORTED_REST_AUTH_MODE"

    def test_xslt_operation_rejected_with_pointer(self):
        bad = copy.deepcopy(_minimal())
        bad["transform"]["operations"] = [
            {"operation_type": "xslt", "source_path": "Root/source_a", "target_path": "Root/target_a"}
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
                "outputs": ["Root/target_a"],
                "script_component_ref": "$ref:some_script",
            }
        ]
        result = _build(bad)
        assert result["_success"] is False
        assert result["error_code"] == "UNSUPPORTED_SCRIPT_COMPONENT_REF"

    def test_map_script_colliding_variable_names_rejected(self):
        # Two source leaves whose final segments both sanitize to 'id' collide in
        # the script's shared input/output namespace -> rejected at the contract.
        bad = copy.deepcopy(_minimal())
        bad["source"]["response_profile"]["root"]["children"] = [
            {"name": "a", "kind": "object", "children": [
                {"name": "id", "kind": "simple", "data_type": "character"}]},
            {"name": "b", "kind": "object", "children": [
                {"name": "id", "kind": "simple", "data_type": "character"}]},
        ]
        bad["target"]["payload_profile"]["root"]["children"] = [
            {"name": "out1", "kind": "simple", "data_type": "character"},
            {"name": "out2", "kind": "simple", "data_type": "character"},
        ]
        bad["transform"]["operations"] = [
            {
                "operation_type": "map_script",
                "script_slot": "s",
                "language": "groovy2",
                "inputs": ["Root/a/id", "Root/b/id"],
                "outputs": ["Root/out1", "Root/out2"],
                "script_body": "x",
            }
        ]
        result = _build(bad)
        assert result["_success"] is False
        assert result["error_code"] == "PARAM_VALIDATION_FAILED"


# ===========================================================================
# Header handling + map_script variable naming
# ===========================================================================


class TestHeadersAndScriptVars:
    def test_create_mode_default_headers_applied_to_operation(self):
        # Connection default_headers (accepted by the reused RestCreateSettings
        # schema) must be honored as operation request headers, not dropped.
        params = copy.deepcopy(_minimal())
        params["source"]["binding"]["settings"]["default_headers"] = {
            "X-Src": "1",
            "Accept": "application/json",
        }
        # An operation header with the same key must win over the connection default.
        params["source"]["fetch_request"]["request_headers"] = {"Accept": "text/plain"}
        params["target"]["binding"]["settings"]["default_headers"] = {"X-Tgt": "2"}
        spec = _spec(params)
        comps = _by_key(spec)
        src_op = comps["source_rest_source_operation"]["config"]
        tgt_op = comps["target_rest_operation"]["config"]
        assert src_op["request_headers"] == {"X-Src": "1", "Accept": "text/plain"}
        assert tgt_op["request_headers"] == {"X-Tgt": "2"}

    def test_map_script_sanitizes_unsafe_leaf_names(self):
        # A hyphenated leaf segment ('order-id') is language-unsafe as a script
        # variable; the preset sanitizes it to 'order_id' and builds successfully.
        params = copy.deepcopy(_minimal())
        params["source"]["response_profile"]["root"]["children"].append(
            {"name": "order-id", "kind": "simple", "data_type": "character"}
        )
        params["target"]["payload_profile"]["root"]["children"].append(
            {"name": "ext-id", "kind": "simple", "data_type": "character"}
        )
        params["transform"]["operations"] = [
            {"operation_type": "direct", "source_path": "Root/source_a", "target_path": "Root/target_a"},
            {
                "operation_type": "map_script",
                "script_slot": "enrich",
                "language": "groovy2",
                "inputs": ["Root/order-id"],
                "outputs": ["Root/ext-id"],
                "script_body": "dataContext.storeStream()",
            },
        ]
        spec = _spec(params)
        tflow = next(f for f in spec["flows"] if f.get("operation") == "transform")
        msum = next(o for o in tflow["operations"] if o["operation_type"] == "map_script")
        assert msum["input_variables"] == ["order_id"]
        assert msum["output_variables"] == ["ext_id"]


# ===========================================================================
# Full local chain: review + plan + apply
# ===========================================================================


class TestFullLocalChain:
    def test_review_validate_unmapped_is_clean(self):
        spec = _spec(_full_reuse())
        r = review_transformation_action("validate_unmapped", {"integration_spec": spec})
        assert r["_success"] is True, r
        assert r["valid"] is True, r
        assert r["read_only"] is True
        assert r["boomi_mutation"] is False

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
        # validated end-to-end through SyncPipelineBuilder.
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
    def test_reuse_connections_recorded_without_creating(self, mock_exec, mock_pag):
        mock_pag.return_value = []

        def _exec(**kwargs):
            return {"_success": True, "component_id": f"id-{kwargs['comp'].key}"}

        mock_exec.side_effect = _exec
        spec = _spec(_full_reuse())
        result = build_integration_action(
            MagicMock(),
            _PROFILE,
            "apply",
            {"integration_spec": spec, "conflict_policy": "reuse", "dry_run": False},
        )
        assert result["_success"] is True, result

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

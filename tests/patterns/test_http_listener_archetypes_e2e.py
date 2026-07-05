"""M6 (issue #12): http_listener_to_db / http_listener_to_rest presets — end-to-end.

Proves the two listener archetypes' contracts and the full local chain through
the *public* action entry points, without live Boomi:

    build_from_archetype_action  ->  build_integration_action(plan)

Covered:
* Both presets emit a ``process_kind="sync_pipeline"`` main process with an
  intact ``listener -> map -> write|send`` stage graph (no pairwise
  source/target process config, no pre-lowering).
* The listener stage carries ONLY the wss_listen binding (operation $ref, no
  connection — WSS has no connection component).
* ``validation_rules.listener`` records the computed endpoint metadata
  orchestrate_deploy's listener_verify stage reads (endpoint_path formula,
  HTTP method derived from input_type, apiType requirement, no Test mode).
* inbound_validation is an opt-in build-time contract; the emitted request
  profile always satisfies it inside the archetype.
* Transform refs are validated against the listener payload profile leaves and
  the target index (write-profile columns / target payload leaves).

Payloads use sentinel placeholders only (``<<...>>``), never canned payloads,
raw XML, credentials, or live account IDs.
"""

from __future__ import annotations

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

_DB_ARCHETYPE = "http_listener_to_db"
_REST_ARCHETYPE = "http_listener_to_rest"
_PAGINATE_TARGET = "boomi_mcp.categories.integration_builder.paginate_metadata"
_PROFILE = "qa-profile"  # never used to reach Boomi; plan calls are fully mocked

_DB_EXPECTED_KEYS = {
    "listener_request_profile",
    "source_wss_listener_operation",
    "target_db_connection",
    "target_db_write_profile",
    "target_db_write_operation",
    "transform_transform_map",
    "main_process",
}
_REST_EXPECTED_KEYS = {
    "listener_request_profile",
    "source_wss_listener_operation",
    "transform_target_profile",
    "transform_transform_map",
    "target_rest_connection",
    "target_rest_operation",
    "main_process",
}


# ---------------------------------------------------------------------------
# Payload builders (synthetic, sentinel-only)
# ---------------------------------------------------------------------------


def _payload_profile() -> Dict[str, Any]:
    return {
        "format": "json",
        "root": {
            "name": "Root",
            "kind": "object",
            "children": [
                {"name": "event_id", "kind": "simple", "data_type": "character"},
                {"name": "amount", "kind": "simple", "data_type": "number"},
            ],
        },
    }


def _db_params(**overrides) -> Dict[str, Any]:
    params: Dict[str, Any] = {
        "naming": {"integration_name": "demo-listener-db", "component_prefix": "LDEMO"},
        "listener": {
            "object_name": "orderIntake",
            "operation_type": "EXECUTE",
            "payload_profile": _payload_profile(),
        },
        "target": {
            "connection": {"mode": "reuse", "component_id": "<<existing db conn id>>"},
            "write_profile": {
                "statement_type": "dynamicinsert",
                "table_name": "<<target table>>",
                "fields": [
                    {"name": "col_id", "data_type": "character", "mandatory": True},
                    {"name": "col_amount", "data_type": "number"},
                ],
            },
        },
        "transform": {
            "operations": [
                {"operation_type": "direct", "source_path": "Root/event_id", "target_path": "Fields/col_id"},
                {"operation_type": "direct", "source_path": "Root/amount", "target_path": "Fields/col_amount"},
            ]
        },
    }
    params.update(overrides)
    return params


def _rest_params(**overrides) -> Dict[str, Any]:
    params: Dict[str, Any] = {
        "naming": {"integration_name": "demo-listener-rest", "component_prefix": "LRELAY"},
        "listener": {
            "object_name": "eventRelay",
            "payload_profile": _payload_profile(),
        },
        "target": {
            "binding": {
                "mode": "create",
                "settings": {"base_url": "https://target.example.com", "auth_mode": "none"},
            },
            "send_request": {"method": "POST", "path": "/v1/<<target resource>>"},
            "payload_profile": {
                "format": "json",
                "root": {
                    "name": "Root",
                    "kind": "object",
                    "children": [
                        {"name": "id", "kind": "simple", "data_type": "character"},
                    ],
                },
            },
        },
        "transform": {
            "operations": [
                {"operation_type": "direct", "source_path": "Root/event_id", "target_path": "Root/id"},
            ]
        },
    }
    params.update(overrides)
    return params


def _spec(archetype: str, params: Dict[str, Any]) -> Dict[str, Any]:
    result = build_from_archetype_action(archetype, params)
    assert result["_success"] is True, result
    return result["integration_spec"]


def _by_key(spec: Dict[str, Any]) -> Dict[str, Dict[str, Any]]:
    return {c["key"]: c for c in spec["components"]}


# ===========================================================================
# Catalog / schema discovery
# ===========================================================================


class TestCatalogAndSchema:
    @pytest.mark.parametrize("archetype", [_DB_ARCHETYPE, _REST_ARCHETYPE])
    def test_listed_in_catalog(self, archetype):
        result = list_integration_archetypes_action(query="listener")
        assert result["_success"] is True
        assert archetype in [a["name"] for a in result["archetypes"]]
        assert result["raw_xml_exposed"] is False

    @pytest.mark.parametrize("archetype", [_DB_ARCHETYPE, _REST_ARCHETYPE])
    def test_get_returns_strict_schema_without_xml(self, archetype):
        result = get_integration_archetype_action(archetype)
        assert result["_success"] is True
        arch = result["archetype"]
        assert arch["capability_notes"]
        assert arch["limitations"]
        assert arch["examples"]
        schema = arch["parameter_schema"]
        assert schema.get("additionalProperties") is False
        for name, prop in schema["properties"].items():
            assert prop.get("description"), f"property {name!r} missing description"
        assert "<?xml" not in json.dumps(result)
        # The #133 advanced-tier deferral is documented.
        assert "#133" in json.dumps(arch["metadata"]["not_for"] + arch["limitations"])


# ===========================================================================
# Spec assembly — http_listener_to_db
# ===========================================================================


class TestListenerToDbAssembly:
    def test_component_set_and_keys(self):
        spec = _spec(_DB_ARCHETYPE, _db_params())
        assert set(_by_key(spec)) == _DB_EXPECTED_KEYS

    def test_main_process_is_intact_listener_stage_graph(self):
        spec = _spec(_DB_ARCHETYPE, _db_params())
        process = _by_key(spec)["main_process"]
        config = process["config"]
        assert config["process_kind"] == "sync_pipeline"
        # Stage graph INTACT — no pairwise source/target/transform config.
        for legacy_key in ("source", "target", "transform"):
            assert legacy_key not in config
        stages = {s["key"]: s for s in config["pipeline"]["stages"]}
        assert [s["kind"] for s in config["pipeline"]["stages"]] == [
            "listener",
            "map",
            "write",
        ]
        listener_stage = stages["listener"]
        assert listener_stage["config"] == {
            "primitive": "wss_listen",
            "operation_id": "$ref:source_wss_listener_operation",
        }
        # No connection binding on the listener stage (WSS has no connection).
        assert "connection_id" not in listener_stage["config"]
        assert set(process["depends_on"]) == {
            "source_wss_listener_operation",
            "transform_transform_map",
            "target_db_connection",
            "target_db_write_operation",
        }

    def test_wss_operation_component_config(self):
        spec = _spec(_DB_ARCHETYPE, _db_params())
        op = _by_key(spec)["source_wss_listener_operation"]
        assert op["type"] == "connector-action"
        config = op["config"]
        assert config["connector_type"] == "wss"
        assert config["operation_mode"] == "listen"
        assert config["object_name"] == "orderIntake"
        assert config["operation_type"] == "EXECUTE"
        assert config["input_type"] == "singlejson"
        assert config["output_type"] == "none"
        assert config["request_profile"] == "$ref:listener_request_profile"
        assert op["depends_on"] == ["listener_request_profile"]

    def test_listener_request_profile_generated_from_payload_tree(self):
        spec = _spec(_DB_ARCHETYPE, _db_params())
        profile = _by_key(spec)["listener_request_profile"]
        assert profile["type"] == "profile.json"
        assert profile["config"]["profile_type"] == "json.generated"
        leaf_names = [c["name"] for c in profile["config"]["root"]["children"]]
        assert leaf_names == ["event_id", "amount"]

    def test_validation_rules_listener_metadata(self):
        spec = _spec(_DB_ARCHETYPE, _db_params())
        listener = spec["validation_rules"]["listener"]
        assert listener["object_name"] == "orderIntake"
        assert listener["operation_type"] == "EXECUTE"
        # The served path sentence-cases the objectName's first letter
        # (live-settled 2026-07-04); the component stores it verbatim.
        assert listener["endpoint_path"] == "/ws/simple/executeOrderIntake"
        # JSON input -> POST (the method is derived, never set on the op).
        assert listener["http_method"] == "POST"
        assert listener["test_mode_supported"] is False
        assert "#133" in listener["api_type_requirement"]

    def test_transform_map_targets_write_profile(self):
        spec = _spec(_DB_ARCHETYPE, _db_params())
        map_config = _by_key(spec)["transform_transform_map"]["config"]
        dumped = json.dumps(map_config)
        assert "$ref:listener_request_profile" in dumped
        assert "$ref:target_db_write_profile" in dumped

    def test_plan_resolves_all_refs(self):
        spec = _spec(_DB_ARCHETYPE, _db_params())
        with patch(_PAGINATE_TARGET) as mock_pag:
            mock_pag.return_value = []
            plan = build_integration_action(
                MagicMock(),
                _PROFILE,
                "plan",
                {"integration_spec": spec, "conflict_policy": "reuse"},
            )
        assert plan["_success"] is True, plan
        statuses = {s["key"]: s for s in plan["steps"]}
        assert set(statuses) == _DB_EXPECTED_KEYS
        for key, step in statuses.items():
            assert not str(step.get("planned_action", "")).startswith("error"), (key, step)


# ===========================================================================
# Spec assembly — http_listener_to_rest
# ===========================================================================


class TestListenerToRestAssembly:
    def test_component_set_and_stage_graph(self):
        spec = _spec(_REST_ARCHETYPE, _rest_params())
        assert set(_by_key(spec)) == _REST_EXPECTED_KEYS
        config = _by_key(spec)["main_process"]["config"]
        assert config["process_kind"] == "sync_pipeline"
        kinds = [s["kind"] for s in config["pipeline"]["stages"]]
        assert kinds == ["listener", "map", "send"]
        send_stage = next(
            s for s in config["pipeline"]["stages"] if s["kind"] == "send"
        )
        # The lowered send stage needs an explicit HTTP method.
        assert send_stage["config"]["action_type"] == "POST"

    def test_listener_metadata_and_endpoint(self):
        spec = _spec(_REST_ARCHETYPE, _rest_params())
        listener = spec["validation_rules"]["listener"]
        assert listener["endpoint_path"] == "/ws/simple/executeEventRelay"
        assert listener["http_method"] == "POST"

    def test_plan_resolves_all_refs(self):
        spec = _spec(_REST_ARCHETYPE, _rest_params())
        with patch(_PAGINATE_TARGET) as mock_pag:
            mock_pag.return_value = []
            plan = build_integration_action(
                MagicMock(),
                _PROFILE,
                "plan",
                {"integration_spec": spec, "conflict_policy": "reuse"},
            )
        assert plan["_success"] is True, plan
        for step in plan["steps"]:
            assert not str(step.get("planned_action", "")).startswith("error"), step


# ===========================================================================
# inbound_validation contract
# ===========================================================================


class TestInboundValidation:
    @pytest.mark.parametrize(
        "archetype, params_fn", [(_DB_ARCHETYPE, _db_params), (_REST_ARCHETYPE, _rest_params)]
    )
    def test_enabled_records_metadata(self, archetype, params_fn):
        spec = _spec(archetype, params_fn(inbound_validation={"enabled": True}))
        block = spec["validation_rules"]["listener"]["inbound_validation"]
        assert block["mode"] == "profile_bound"
        assert block["request_profile_id"] == "$ref:listener_request_profile"

    def test_disabled_by_default(self):
        spec = _spec(_DB_ARCHETYPE, _db_params())
        assert "inbound_validation" not in spec["validation_rules"]["listener"]

    def test_primitive_rejects_profile_less_contract(self):
        """The primitive's own rejection path (unreachable through the archetype,
        which always generates a profile): a none/singledata listener cannot
        satisfy mode='profile_bound'."""
        from boomi_mcp.categories.components.builders.connector_builder import (
            BuilderValidationError,
        )
        from boomi_mcp.patterns.primitives.inbound_validate import (
            InboundValidateParameters,
            InboundValidatePrimitive,
        )

        with pytest.raises(BuilderValidationError) as exc:
            InboundValidatePrimitive.validate_contract(
                InboundValidateParameters(
                    listener_input_type="singledata",
                    listener_request_profile_id=None,
                )
            )
        assert exc.value.error_code == "INBOUND_VALIDATION_UNSATISFIABLE"

        with pytest.raises(BuilderValidationError) as exc:
            InboundValidatePrimitive.validate_contract(
                InboundValidateParameters(
                    listener_input_type="singlejson",
                    listener_request_profile_id=None,
                )
            )
        assert exc.value.error_code == "INBOUND_VALIDATION_UNSATISFIABLE"


# ===========================================================================
# Parameter validation
# ===========================================================================


class TestParameterValidation:
    def test_unknown_transform_source_path_rejected(self):
        params = _db_params()
        params["transform"]["operations"][0]["source_path"] = "Root/nope"
        result = build_from_archetype_action(_DB_ARCHETYPE, params)
        assert result["_success"] is False
        assert "source path" in json.dumps(result)

    def test_unknown_write_profile_target_rejected(self):
        params = _db_params()
        params["transform"]["operations"][0]["target_path"] = "Fields/nope"
        result = build_from_archetype_action(_DB_ARCHETYPE, params)
        assert result["_success"] is False

    def test_unmapped_required_column_rejected(self):
        params = _db_params()
        params["transform"]["operations"] = [
            {"operation_type": "direct", "source_path": "Root/amount", "target_path": "Fields/col_amount"},
        ]
        result = build_from_archetype_action(_DB_ARCHETYPE, params)
        assert result["_success"] is False

    def test_blank_object_name_rejected(self):
        params = _db_params()
        params["listener"]["object_name"] = "  "
        result = build_from_archetype_action(_DB_ARCHETYPE, params)
        assert result["_success"] is False

    def test_url_unsafe_object_name_rejected_by_builder(self):
        params = _db_params()
        params["listener"]["object_name"] = "has space"
        result = build_from_archetype_action(_DB_ARCHETYPE, params)
        assert result["_success"] is False

    def test_non_json_input_type_rejected(self):
        params = _db_params()
        params["listener"]["input_type"] = "none"
        result = build_from_archetype_action(_DB_ARCHETYPE, params)
        assert result["_success"] is False

    def test_dynamic_rest_target_path_rejected(self):
        params = _rest_params()
        params["target"]["send_request"]["path"] = "/v1/items/{id}"
        result = build_from_archetype_action(_REST_ARCHETYPE, params)
        assert result["_success"] is False

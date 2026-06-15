"""Issue #29: database_to_api_sync archetype assembly tests.

The archetype now composes the #27 (db_extract, field_map) and #28
(rest_send_with_retry + operational) primitives into an executable
IntegrationSpecV1. These tests cover the assembled component graph, the
transform routes, build_integration plan validation, review_transformation
against the executable spec, and the negative/secret boundaries — without
calling Boomi or exposing raw XML.

Payloads use sentinel placeholders only (``<<...>>``), never canned SQL,
payloads, scripts, mappings, SOAP/OData content, or raw XML.
"""

from __future__ import annotations

import copy
import json
from typing import Any, Dict, List
from unittest.mock import MagicMock, patch

import pytest

from boomi_mcp.categories.integration_authoring import build_from_archetype_action
from boomi_mcp.categories.integration_builder import _build_plan
from boomi_mcp.categories.transformation_review import review_transformation_action

_PAGINATE_TARGET = "boomi_mcp.categories.integration_builder.paginate_metadata"


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------


def _minimal() -> Dict[str, Any]:
    """Smallest executable payload: create DB (username/password) + create REST
    (no auth) + a single direct transform + manual trigger."""
    return {
        "naming": {"integration_name": "demo-sync", "component_prefix": "DEMO"},
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
                    "fields": [{"name": "source_a", "data_type": "character"}]
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
            "send_request": {"method": "POST", "path": "/v1/items"},
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
                    "source_field": "source_a",
                    "target_path": "Root/target_a",
                }
            ]
        },
        "execution": {"trigger": {"mode": "manual"}},
        "reliability": {
            "retry": {"max_attempts": 1},
            "dlq": {"enabled": False},
            "error_classifier": {},
        },
    }


def _full_reuse() -> Dict[str, Any]:
    """Fuller payload: reuse DB + reuse REST, scheduled trigger, dpp watermark,
    requested retry + DLQ enabled, run metadata, direct + map_function."""
    return {
        "naming": {
            "integration_name": "demo-incremental",
            "component_prefix": "DEMO-INC",
            "folder_path": "Integrations/CRM/Sync",
        },
        "source": {
            "binding": {"mode": "reuse", "component_id": "<<existing DB connection id>>"},
            "read_operation": {
                "sql": "<<user-authored incremental DB read statement>>",
                "result_schema": {
                    "fields": [
                        {"name": "source_a", "data_type": "character", "required": True},
                        {"name": "source_b", "data_type": "datetime"},
                    ]
                },
                "parameters": [{"name": "<<bind parameter name>>", "direction": "in"}],
                "batch_size": 500,
            },
        },
        "target": {
            "binding": {"mode": "reuse", "component_id": "<<existing REST connection id>>"},
            "send_request": {
                "method": "POST",
                "path": "/v1/customers",
                "query_parameters": [{"name": "since", "value_source": "watermark"}],
                "expected_status_codes": [200, 202],
            },
            "payload_profile": {
                "format": "json",
                "root": {
                    "name": "Root",
                    "kind": "object",
                    "children": [
                        {"name": "target_a", "kind": "simple", "data_type": "character", "required": True},
                        {"name": "target_b", "kind": "simple", "data_type": "datetime"},
                    ],
                },
            },
        },
        "transform": {
            "operations": [
                {"operation_type": "direct", "source_field": "source_a", "target_path": "Root/target_a"},
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
            ]
        },
        "execution": {
            "trigger": {"mode": "scheduled", "schedule": {"cron": "0 2 * * *", "timezone": "UTC"}},
            "watermark": {"field": "source_b", "kind": "timestamp", "persistence": "dpp"},
            "run_metadata": {"owner": "crm-team"},
        },
        "reliability": {
            "retry": {"max_attempts": 5, "backoff": "exponential", "initial_interval_seconds": 2},
            "dlq": {"enabled": True, "target": {"mode": "guidance_only", "kind": "queue", "address": "<<dlq queue address>>"}},
            "error_classifier": {"custom_rules": ["rate_limit_exhausted"]},
        },
    }


def _build(payload: Dict[str, Any]) -> Dict[str, Any]:
    return build_from_archetype_action("database_to_api_sync", payload)


def _spec(payload: Dict[str, Any]) -> Dict[str, Any]:
    result = _build(payload)
    assert result["_success"] is True, result
    return result["integration_spec"]


def _by_key(spec: Dict[str, Any]) -> Dict[str, Dict[str, Any]]:
    return {c["key"]: c for c in spec["components"]}


# ---------------------------------------------------------------------------
# Assembly
# ---------------------------------------------------------------------------


class TestAssembly:
    def test_minimal_emits_expected_component_graph(self):
        spec = _spec(_minimal())
        comps = {c["key"]: c["type"] for c in spec["components"]}
        assert comps == {
            "source_db_connection": "connector-settings",
            "source_db_read_profile": "profile.db",
            "source_db_get_operation": "connector-action",
            "transform_target_profile": "profile.json",
            "transform_transform_map": "transform.map",
            "target_rest_connection": "connector-settings",
            "target_rest_operation": "connector-action",
            "main_process": "process",
        }

    def test_cross_reference_wiring(self):
        by_key = _by_key(_spec(_minimal()))
        # transform map binds to the DB read profile as source.
        tmap = by_key["transform_transform_map"]
        assert tmap["config"]["source_profile_id"] == "$ref:source_db_read_profile"
        assert "source_db_read_profile" in tmap["depends_on"]
        # REST operation binds its request body to the generated JSON profile.
        rest_op = by_key["target_rest_operation"]
        assert rest_op["config"]["request_profile_id"] == "$ref:transform_target_profile"
        assert rest_op["config"]["request_profile_type"] == "json"
        assert "transform_target_profile" in rest_op["depends_on"]
        # process wires source / transform / target by $ref, with depends_on
        # containing exactly those keys.
        proc = by_key["main_process"]
        cfg = proc["config"]
        assert cfg["process_kind"] == "database_to_api_sync"
        assert cfg["source"]["connection_id"] == "$ref:source_db_connection"
        assert cfg["source"]["operation_id"] == "$ref:source_db_get_operation"
        assert cfg["transform"] == {"mode": "map_ref", "map_ref": "$ref:transform_transform_map"}
        assert cfg["target"]["connection_id"] == "$ref:target_rest_connection"
        assert cfg["target"]["operation_id"] == "$ref:target_rest_operation"
        assert cfg["target"]["action_type"] == "POST"
        assert set(proc["depends_on"]) == {
            "source_db_connection",
            "source_db_get_operation",
            "transform_transform_map",
            "target_rest_connection",
            "target_rest_operation",
        }

    def test_process_reliability_stays_gated(self):
        proc = _by_key(_spec(_minimal()))["main_process"]
        assert proc["config"]["reliability"] == {"retry_count": 0, "dlq": {"mode": "disabled"}}

    def test_reuse_mode_emits_reference_only_connections(self):
        by_key = _by_key(_spec(_full_reuse()))
        db_conn = by_key["source_db_connection"]
        rest_conn = by_key["target_rest_connection"]
        assert db_conn["config"]["reference_only"] is True
        assert db_conn["config"]["component_id"] == "<<existing DB connection id>>"
        assert rest_conn["config"]["reference_only"] is True
        assert rest_conn["config"]["component_id"] == "<<existing REST connection id>>"

    def test_folder_path_propagates_to_components_and_process(self):
        by_key = _by_key(_spec(_full_reuse()))
        assert by_key["transform_target_profile"]["config"]["folder_path"] == "Integrations/CRM/Sync"
        assert by_key["source_db_read_profile"]["config"]["folder_name"] == "Integrations/CRM/Sync"
        assert by_key["main_process"]["config"]["folder_name"] == "Integrations/CRM/Sync"

    def test_component_name_overrides_apply_by_role_key(self):
        # The public schema documents component_names as role-keyed.
        payload = _minimal()
        payload["naming"]["component_names"] = {
            "db_connection": "Custom DB Conn",
            "transform_map": "Custom Map",
            "rest_connection": "Custom REST Conn",
            "process": "Custom Process",
        }
        by_key = _by_key(_spec(payload))
        assert by_key["source_db_connection"]["name"] == "Custom DB Conn"
        assert by_key["transform_transform_map"]["name"] == "Custom Map"
        assert by_key["target_rest_connection"]["name"] == "Custom REST Conn"
        assert by_key["main_process"]["name"] == "Custom Process"

    def test_component_name_overrides_apply_by_emitted_key_fallback(self):
        # The prefixed emitted key is also honored as a fallback.
        payload = _minimal()
        payload["naming"]["component_names"] = {
            "source_db_connection": "Emitted-Key DB Conn",
            "main_process": "Emitted-Key Process",
        }
        by_key = _by_key(_spec(payload))
        assert by_key["source_db_connection"]["name"] == "Emitted-Key DB Conn"
        assert by_key["main_process"]["name"] == "Emitted-Key Process"


# ---------------------------------------------------------------------------
# Operational intent (metadata only)
# ---------------------------------------------------------------------------


class TestOperationalIntent:
    def test_full_payload_operational_intent(self):
        spec = _spec(_full_reuse())
        oi = spec["validation_rules"]["operational_intent"]
        assert oi["execution"]["trigger"] == {
            "mode": "scheduled",
            "cron": "0 2 * * *",
            "timezone": "UTC",
        }
        assert oi["schedule"]["applies_after_deploy"] is True
        assert oi["watermark"] == {
            "enabled": True,
            "field": "source_b",
            "kind": "timestamp",
            "persistence": "dpp",
            # The contract has no dpp_name; a deterministic default is generated
            # for #51 dynamic process-property wiring.
            "dpp_name": "watermark_source_b",
            "dpp_name_generated": True,
        }
        assert oi["execution"]["run_metadata"] == {"owner": "crm-team"}
        assert oi["expected_status_codes"] == [200, 202]
        # Requested retry is recorded as intent only; the process stays gated.
        retry = oi["reliability"]["retry"]
        assert retry["requested_max_attempts"] == 5
        assert retry["process_retry_count"] == 0
        assert retry["deferred_to"] == "#51 R1b"
        assert oi["reliability"]["dlq"] == {"mode": "disabled"}
        assert oi["reliability"]["dlq_requested"]["requested"] is True
        assert oi["reliability"]["dlq_requested"]["status"] == "guidance_only"
        assert oi["reliability"]["dlq_requested"]["kind"] == "queue"
        assert oi["reliability"]["dlq_requested"]["address_present"] is True

    def test_manual_trigger_intent(self):
        oi = _spec(_minimal())["validation_rules"]["operational_intent"]
        assert oi["execution"]["trigger"] == {"mode": "manual"}
        assert oi["watermark"] == {"enabled": False}

    def test_watermark_query_param_is_intent_not_static(self):
        """A watermark-sourced query parameter must NOT be emitted as a static
        REST operation query parameter, but its name must be preserved as
        operational intent (dynamic operation-property wiring is deferred to
        #51) — it must not be silently dropped."""
        spec = _spec(_full_reuse())
        rest_op = _by_key(spec)["target_rest_operation"]
        assert "since" not in (rest_op["config"].get("query_parameters") or {})
        oi = spec["validation_rules"]["operational_intent"]
        wqp = oi["watermark_query_parameters"]
        assert {p["name"] for p in wqp} == {"since"}
        assert all(p["bound_to"] == "watermark" for p in wqp)
        assert all(p["deferred_to"] == "#51" for p in wqp)

    def test_dlq_address_not_echoed(self):
        """The DLQ target address (caller-specific) must never appear in the
        emitted spec — only the routing kind is recorded."""
        result = _build(_full_reuse())
        assert "<<dlq queue address>>" not in json.dumps(result)

    def test_bind_parameter_typing_preserved_as_deferred_intent(self):
        """The Select read profile only carries name + mappability, so a bind
        parameter's sql_type / non-default direction is preserved as deferred
        intent rather than silently dropped."""
        payload = _minimal()
        payload["source"]["read_operation"]["parameters"] = [
            {"name": "p_since", "direction": "in", "sql_type": "TIMESTAMP"},
            {"name": "p_out", "direction": "out"},
        ]
        oi = _spec(payload)["validation_rules"]["operational_intent"]
        typed = oi["deferred"]["read_operation"]["bind_parameter_typing"]
        by_name = {p["name"]: p for p in typed}
        assert by_name["p_since"]["sql_type"] == "TIMESTAMP"
        assert by_name["p_since"]["direction"] == "in"
        assert by_name["p_out"]["direction"] == "out"


# ---------------------------------------------------------------------------
# Transform routes
# ---------------------------------------------------------------------------


class TestTransformRoutes:
    def test_direct_only_route(self):
        tmap = _by_key(_spec(_minimal()))["transform_transform_map"]
        assert tmap["config"]["map_type"] == "direct"

    def test_direct_plus_function_route(self):
        tmap = _by_key(_spec(_full_reuse()))["transform_transform_map"]
        assert tmap["config"]["map_type"] == "function"

    def test_inline_script_route_emits_script_mapping(self):
        payload = _minimal()
        payload["transform"]["operations"] = [
            {
                "operation_type": "map_script",
                "script_slot": "enrich",
                "language": "groovy2",
                "inputs": ["source_a"],
                "outputs": ["Root/target_a"],
                "script_body": "<<task-authored script body>>",
            }
        ]
        by_key = _by_key(_spec(payload))
        # An inline body materializes a standalone script.mapping component.
        script_keys = [k for k, c in by_key.items() if c["type"] == "script.mapping"]
        assert len(script_keys) == 1
        assert by_key["transform_transform_map"]["config"]["map_type"] == "script"

    def test_mixed_function_and_script_rejected(self):
        payload = _minimal()
        payload["source"]["read_operation"]["result_schema"]["fields"] = [
            {"name": "source_a", "data_type": "character"},
            {"name": "source_b", "data_type": "character"},
        ]
        payload["target"]["payload_profile"]["root"]["children"] = [
            {"name": "target_a", "kind": "simple", "data_type": "character"},
            {"name": "target_b", "kind": "simple", "data_type": "character"},
        ]
        payload["transform"]["operations"] = [
            {"operation_type": "map_function", "function_type": "uppercase", "inputs": ["source_a"], "target_path": "Root/target_a"},
            {
                "operation_type": "map_script",
                "script_slot": "s",
                "language": "groovy2",
                "inputs": ["source_b"],
                "outputs": ["Root/target_b"],
                "script_body": "<<body>>",
            },
        ]
        result = _build(payload)
        assert result["_success"] is False
        assert result["error_code"] == "UNSUPPORTED_TRANSFORM_ROUTE"


# ---------------------------------------------------------------------------
# build_integration(action='plan')
# ---------------------------------------------------------------------------


class TestPlanner:
    @patch(_PAGINATE_TARGET)
    def test_minimal_spec_plans_successfully(self, mock_pag):
        mock_pag.return_value = []  # no pre-existing components
        spec = _spec(_minimal())
        plan = _build_plan(MagicMock(), {"integration_spec": spec})
        assert plan["_success"] is True, plan
        # Topological order: every dependency precedes its dependents.
        order = plan["execution_order"]
        assert set(order) == {c["key"] for c in spec["components"]}
        pos = {key: i for i, key in enumerate(order)}
        for comp in spec["components"]:
            for dep in comp["depends_on"]:
                assert pos[dep] < pos[comp["key"]], (dep, comp["key"])
        assert order[-1] == "main_process"

    @patch(_PAGINATE_TARGET)
    def test_plan_has_no_process_validation_error(self, mock_pag):
        mock_pag.return_value = []
        spec = _spec(_full_reuse())
        plan = _build_plan(MagicMock(), {"integration_spec": spec})
        assert plan["_success"] is True, plan
        for step in plan["steps"]:
            assert step.get("validation_error") in (None, {}), step

    @patch(_PAGINATE_TARGET)
    def test_script_route_plan_component_count_stays_consistent(self, mock_pag):
        """The planner injects a transform.function wrapper for a script-route
        map, so the returned plan spec's validation_rules.component_count must
        match the (now larger) component list — not the pre-synthesis count."""
        mock_pag.return_value = []
        payload = _minimal()
        payload["transform"]["operations"] = [
            {
                "operation_type": "map_script",
                "script_slot": "enrich",
                "language": "groovy2",
                "inputs": ["source_a"],
                "outputs": ["Root/target_a"],
                "script_body": "<<task-authored script body>>",
            }
        ]
        emitted = _spec(payload)
        # The archetype itself emits a self-consistent 9-component script spec.
        assert emitted["validation_rules"]["component_count"] == len(emitted["components"]) == 9
        plan = _build_plan(MagicMock(), {"integration_spec": emitted})
        assert plan["_success"] is True, plan
        planned = plan["integration_spec"]
        # The planner added the transform.function wrapper, and the count tracks it.
        assert any(c["type"] == "transform.function" for c in planned["components"])
        assert planned["validation_rules"]["component_count"] == len(planned["components"]) == 10

    @patch(_PAGINATE_TARGET)
    def test_requested_retry_dlq_do_not_reach_process_builder(self, mock_pag):
        """Even when the caller requests retry>1 + DLQ enabled, the emitted
        process keeps retry_count=0 / dlq disabled, so the plan succeeds and
        PROCESS_RETRY_UNVERIFIED never trips."""
        mock_pag.return_value = []
        spec = _spec(_full_reuse())  # requests max_attempts=5 + dlq enabled
        proc = _by_key(spec)["main_process"]
        assert proc["config"]["reliability"]["retry_count"] == 0
        assert proc["config"]["reliability"]["dlq"]["mode"] == "disabled"
        plan = _build_plan(MagicMock(), {"integration_spec": spec})
        assert plan["_success"] is True, plan


# ---------------------------------------------------------------------------
# review_transformation against the executable spec
# ---------------------------------------------------------------------------


class TestTransformationReview:
    def test_list_fields(self):
        # The archetype emits both the contract flow and executable components;
        # review prefers the contract flow when both are present.
        spec = _spec(_full_reuse())
        r = review_transformation_action("list_fields", {"integration_spec": spec})
        assert r["_success"] is True, r
        source_paths = {f["path"] for f in r["source_fields"]}
        assert {"source_a", "source_b"} <= source_paths

    def test_validate_unmapped_complete_is_valid(self):
        spec = _spec(_full_reuse())
        r = review_transformation_action("validate_unmapped", {"integration_spec": spec})
        assert r["_success"] is True, r
        assert r["valid"] is True

    def test_generate_test_payload(self):
        spec = _spec(_full_reuse())
        r = review_transformation_action("generate_test_payload", {"integration_spec": spec})
        assert r["_success"] is True, r

    def test_executable_components_satisfy_review_source_b(self):
        """With the contract flow removed, review falls back to the executable
        transform.map + profile components (Source-B) and still lists fields —
        confirming the emitted executable shape is review-compatible."""
        spec = copy.deepcopy(_spec(_full_reuse()))
        spec["flows"] = []
        r = review_transformation_action("list_fields", {"integration_spec": spec})
        assert r["_success"] is True, r
        assert r["source_kind"] == "executable_components"
        source_paths = {f["path"] for f in r["source_fields"]}
        assert {"source_a", "source_b"} <= source_paths

    def test_required_target_leaf_unmapped_flagged_on_executable_spec(self):
        """review_transformation flags a required target leaf with no mapping.
        The archetype never emits such a spec (its contract guarantees required
        leaves are mapped), so we drop the contract flow and remove the mapping
        from the executable map to exercise the Source-B validation path."""
        spec = copy.deepcopy(_spec(_full_reuse()))  # target_a is required + mapped
        spec["flows"] = []  # force the executable_components review path
        tmap = _by_key(spec)["transform_transform_map"]["config"]
        # Drop the direct mapping that fills the required leaf Root/target_a.
        tmap["field_mappings"] = [
            m for m in tmap.get("field_mappings", []) if m.get("target_path") != "Root/target_a"
        ]
        r = review_transformation_action("validate_unmapped", {"integration_spec": spec})
        assert r["_success"] is True, r
        assert r["valid"] is False
        codes = {issue["code"] for issue in r["issues"]}
        assert "TRANSFORM_REVIEW_REQUIRED_TARGET_UNMAPPED" in codes


# ---------------------------------------------------------------------------
# Negative / secret boundaries
# ---------------------------------------------------------------------------


class TestNegativeAndSecret:
    def test_rest_create_secured_auth_rejected_without_secret_echo(self):
        payload = _minimal()
        payload["target"]["binding"]["settings"]["auth_mode"] = "bearer_token"
        payload["target"]["binding"]["settings"]["credential_ref"] = "secrets/rest/SENTINEL_BEARER"
        result = _build(payload)
        assert result["_success"] is False
        assert result["error_code"] == "UNSUPPORTED_REST_AUTH_MODE"
        assert "secrets/rest/SENTINEL_BEARER" not in json.dumps(result)

    def test_db_windows_integrated_rejected(self):
        payload = _minimal()
        payload["source"]["binding"]["settings"]["auth_mode"] = "windows_integrated"
        payload["source"]["binding"]["settings"].pop("username")
        payload["source"]["binding"]["settings"].pop("credential_ref")
        result = _build(payload)
        assert result["_success"] is False
        assert result["error_code"] == "UNSUPPORTED_DB_AUTH_MODE"

    @pytest.mark.parametrize("ref", ["literal-not-a-ref", "$ref:enrich_row"])
    def test_script_component_ref_rejected(self, ref):
        """Any script_component_ref (literal or $ref) is rejected: the archetype
        cannot materialize the referenced component into the spec, so the plan
        would carry a dangling dependency. Inline script_body is the supported
        path."""
        payload = _minimal()
        payload["transform"]["operations"] = [
            {
                "operation_type": "map_script",
                "script_slot": "s",
                "language": "groovy2",
                "inputs": ["source_a"],
                "outputs": ["Root/target_a"],
                "script_component_ref": ref,
            }
        ]
        result = _build(payload)
        assert result["_success"] is False
        assert result["error_code"] == "UNSUPPORTED_SCRIPT_COMPONENT_REF"

    def test_inline_script_body_spec_is_plannable(self):
        """The supported map-script path (inline body) materializes the
        script.mapping in-spec, so the emitted spec has no dangling dependency
        and orders topologically."""
        from boomi_mcp.categories.integration_builder import (
            _normalize_to_spec,
            _topological_order,
        )

        payload = _minimal()
        payload["transform"]["operations"] = [
            {
                "operation_type": "map_script",
                "script_slot": "enrich",
                "language": "groovy2",
                "inputs": ["source_a"],
                "outputs": ["Root/target_a"],
                "script_body": "<<task-authored script body>>",
            }
        ]
        spec = _spec(payload)
        normalized = _normalize_to_spec({"integration_spec": spec})
        order = _topological_order(normalized)  # must not raise on a dangling ref
        assert "main_process" in order

    def test_invalid_cron_returns_clean_structured_error(self):
        payload = _minimal()
        payload["execution"] = {
            "trigger": {"mode": "scheduled", "schedule": {"cron": "not-a-cron"}}
        }
        result = _build(payload)
        assert result["_success"] is False
        # Clean structured error, not the opaque ARCHETYPE_BUILD_FAILED.
        assert result["error_code"] == "ARCHETYPE_PARAM_INVALID"

    def test_secret_shaped_run_metadata_key_rejected_and_redacted(self):
        payload = _minimal()
        payload["execution"]["run_metadata"] = {"db_password": "SENTINEL_SECRET_VALUE"}
        result = _build(payload)
        assert result["_success"] is False
        assert result["error_code"] == "SECRET_SHAPED_KEY"
        assert "SENTINEL_SECRET_VALUE" not in json.dumps(result)

    def test_error_does_not_echo_credential_ref(self):
        payload = _minimal()
        payload["source"]["binding"]["settings"]["credential_ref"] = "secrets/db/SENTINEL_CRED"
        # Force a failure downstream (unsupported REST auth) and check the DB
        # credential_ref is not echoed in the error envelope.
        payload["target"]["binding"]["settings"]["auth_mode"] = "bearer_token"
        payload["target"]["binding"]["settings"]["credential_ref"] = "secrets/rest/x"
        result = _build(payload)
        assert result["_success"] is False
        assert "secrets/db/SENTINEL_CRED" not in json.dumps(result)

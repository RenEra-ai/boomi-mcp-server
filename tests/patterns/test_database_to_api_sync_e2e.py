"""Issue #30: database_to_api_sync end-to-end (M2) validation.

Proves the full M2 create/apply/verify path locally, without live Boomi, by
chaining the *public* action entry points:

    build_from_archetype_action  ->  review_transformation_action
                                 ->  build_integration_action(plan / apply / verify)

The assembly tests (``test_database_to_api_sync_assembly.py``) already cover the
emitted component graph and ``_build_plan`` directly. This module adds the
apply-path guarantees the plan can't express:

* Dry-run apply returns the plan and NEVER calls ``_execute_component``.
* A mocked real apply resolves every ``$ref`` token before a component config
  reaches ``_execute_component`` (source/transform/target + script wrapper).
* ``reference_only`` reuse components are recorded ``status='reused'`` and never
  trigger component creation.
* Fail-fast: an unresolvable plan returns ``_success=False`` and executes
  nothing.

Payloads use sentinel placeholders only (``<<...>>``), never canned SQL,
payloads, scripts, mappings, raw XML, credentials, or live account IDs.
"""

from __future__ import annotations

import copy
import json
from typing import Any, Dict, List
from unittest.mock import MagicMock, patch

import pytest

from boomi_mcp.categories.integration_authoring import build_from_archetype_action
from boomi_mcp.categories.integration_builder import build_integration_action
from boomi_mcp.categories.transformation_review import review_transformation_action

_PAGINATE_TARGET = "boomi_mcp.categories.integration_builder.paginate_metadata"
_EXECUTE_TARGET = "boomi_mcp.categories.integration_builder._execute_component"
_PROFILE = "qa-profile"  # never used to reach Boomi; calls are fully mocked


# ---------------------------------------------------------------------------
# Payload builders (synthetic, sentinel-only)
# ---------------------------------------------------------------------------


def _minimal() -> Dict[str, Any]:
    """Smallest executable payload: create DB + create REST (no auth) + a single
    direct transform + manual trigger."""
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
    """Reuse DB + reuse REST, scheduled/incremental trigger, dpp watermark,
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
            # guidance_only DLQ wires no catch path → retry stays 1 (#88).
            "retry": {"max_attempts": 1, "backoff": "platform"},
            "dlq": {"enabled": True, "target": {"mode": "guidance_only", "kind": "queue", "address": "<<dlq queue address>>"}},
            "error_classifier": {"custom_rules": ["rate_limit_exhausted"]},
        },
    }


def _map_script() -> Dict[str, Any]:
    """Minimal payload whose transform is an inline map_script route — the
    planner materializes a script.mapping + synthesizes a transform.function
    wrapper."""
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
    return payload


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _build(payload: Dict[str, Any]) -> Dict[str, Any]:
    return build_from_archetype_action("database_to_api_sync", payload)


def _spec(payload: Dict[str, Any]) -> Dict[str, Any]:
    result = _build(payload)
    assert result["_success"] is True, result
    return result["integration_spec"]


def _by_key(spec: Dict[str, Any]) -> Dict[str, Dict[str, Any]]:
    return {c["key"]: c for c in spec["components"]}


def _plan(spec: Dict[str, Any], mock_pag, conflict_policy: str = "reuse") -> Dict[str, Any]:
    """Route a plan through the public build_integration_action entry."""
    mock_pag.return_value = []
    return build_integration_action(
        MagicMock(),
        _PROFILE,
        "plan",
        {"integration_spec": spec, "conflict_policy": conflict_policy},
    )


def _execute_stub(**kwargs):
    """Deterministic _execute_component replacement: one id per component key so
    later $ref resolution has a real value to substitute."""
    return {"_success": True, "component_id": f"id-{kwargs['comp'].key}"}


def _verify_get_side_effect(_client, component_id, *args, **kwargs):
    """component_get_xml replacement for the verify path (issue #80).

    Returns a graph-valid process Component XML for the main process so the new
    process-graph verifier runs and reports a clean section, and a non-process
    stub for every other component so those records correctly omit process_graph
    (mirroring production: only process components are graph-verified)."""
    from pathlib import Path

    if component_id == "id-main_process":
        xml = (
            Path(__file__).parent.parent
            / "fixtures"
            / "process_graph"
            / "valid_linear_process.xml"
        ).read_text(encoding="utf-8")
        return {
            "type": "process",
            "component_id": component_id,
            "name": "main_process",
            "xml": xml,
        }
    return {
        "type": "connector-settings",
        "component_id": component_id,
        "name": component_id,
        "xml": "<bns:Component/>",
    }


def _has_ref_token(value: Any) -> bool:
    """True if any string anywhere in the structure still starts with '$ref:'."""
    if isinstance(value, str):
        return value.startswith("$ref:")
    if isinstance(value, dict):
        return any(_has_ref_token(v) for v in value.values())
    if isinstance(value, list):
        return any(_has_ref_token(v) for v in value)
    return False


# ===========================================================================
# Full local chain: archetype -> review -> plan
# ===========================================================================


class TestFullLocalChain:
    def test_minimal_archetype_emits_safe_serializable_spec(self):
        result = _build(_minimal())
        assert result["_success"] is True, result
        assert result["raw_xml_exposed"] is False
        assert result["boomi_mutation"] is False
        spec = result["integration_spec"]
        keys = {c["key"] for c in spec["components"]}
        assert keys == {
            "source_db_connection",
            "source_db_read_profile",
            "source_db_get_operation",
            "transform_target_profile",
            "transform_transform_map",
            "target_rest_connection",
            "target_rest_operation",
            "main_process",
        }
        # JSON-only output: the entire envelope round-trips through JSON.
        assert json.loads(json.dumps(result)) == result

    def test_review_validate_unmapped_is_clean_before_planning(self):
        spec = _spec(_full_reuse())
        r = review_transformation_action(
            "validate_unmapped", {"integration_spec": spec}
        )
        assert r["_success"] is True, r
        assert r["valid"] is True, r
        assert r["read_only"] is True
        assert r["boomi_mutation"] is False

    @patch(_PAGINATE_TARGET)
    def test_plan_orders_topologically_with_main_process_last(self, mock_pag):
        spec = _spec(_minimal())
        plan = _plan(spec, mock_pag)
        assert plan["_success"] is True, plan
        assert plan["profile"] == _PROFILE
        order = plan["execution_order"]
        assert set(order) == {c["key"] for c in spec["components"]}
        # Every key appears exactly once.
        assert len(order) == len(set(order))
        pos = {key: i for i, key in enumerate(order)}
        for comp in spec["components"]:
            for dep in comp["depends_on"]:
                assert pos[dep] < pos[comp["key"]], (dep, comp["key"])
        assert order[-1] == "main_process"

    @patch(_PAGINATE_TARGET)
    def test_plan_exposes_transform_summary_and_review_hint(self, mock_pag):
        spec = _spec(_minimal())
        plan = _plan(spec, mock_pag)
        steps_by_key = {s["key"]: s for s in plan["steps"]}
        map_step = steps_by_key["transform_transform_map"]
        assert map_step["transform_summary"]["map_type"] == "direct"
        assert map_step["transform_summary"]["direct_mapping_count"] >= 1
        assert "review_transformation" in map_step["review_hint"]

    def test_operational_intent_records_all_requested_behavior(self):
        oi = _spec(_full_reuse())["validation_rules"]["operational_intent"]
        # schedule + execution trigger + run metadata
        assert oi["schedule"]["applies_after_deploy"] is True
        assert oi["execution"]["trigger"]["mode"] == "scheduled"
        assert oi["execution"]["run_metadata"] == {"owner": "crm-team"}
        # watermark (incremental) intent
        assert oi["watermark"]["enabled"] is True
        assert oi["watermark"]["field"] == "source_b"
        # guidance_only DLQ → retry stays 1; emitted process_retry_count 0 (#88)
        assert oi["reliability"]["retry"]["requested_max_attempts"] == 1
        assert oi["reliability"]["retry"]["process_retry_count"] == 0
        assert oi["reliability"]["dlq"] == {"mode": "disabled"}
        assert oi["reliability"]["dlq_requested"]["requested"] is True
        # error classifier + expected status codes
        assert "error_classifier" in oi["reliability"]
        assert oi["expected_status_codes"] == [200, 202]

    def test_process_reliability_stays_gated(self):
        proc = _by_key(_spec(_full_reuse()))["main_process"]
        assert proc["config"]["reliability"] == {
            "retry_count": 0,
            "dlq": {"mode": "disabled"},
        }


# ===========================================================================
# Apply path: dry-run, mocked real apply, $ref resolution, fail-fast
# ===========================================================================


class TestApplyPath:
    @patch(_EXECUTE_TARGET)
    @patch(_PAGINATE_TARGET)
    def test_dry_run_apply_returns_plan_without_executing(self, mock_pag, mock_exec):
        mock_pag.return_value = []
        spec = _spec(_minimal())
        # No dry_run key -> defaults to True.
        result = build_integration_action(
            MagicMock(), _PROFILE, "apply", {"integration_spec": spec}
        )
        assert result["_success"] is True, result
        assert result["dry_run"] is True
        assert "execution_order" in result  # the plan is included
        mock_exec.assert_not_called()

    @patch(_EXECUTE_TARGET, side_effect=_execute_stub)
    @patch(_PAGINATE_TARGET)
    def test_mocked_real_apply_resolves_all_refs_before_execution(
        self, mock_pag, mock_exec
    ):
        mock_pag.return_value = []
        spec = _spec(_minimal())
        result = build_integration_action(
            MagicMock(),
            _PROFILE,
            "apply",
            {"integration_spec": spec, "dry_run": False, "conflict_policy": "reuse"},
        )
        assert result["_success"] is True, result
        assert result["build_id"]
        # No config passed to _execute_component still carries a $ref token.
        for call in mock_exec.call_args_list:
            cfg = call.kwargs["config"]
            assert not _has_ref_token(cfg), call.kwargs["comp"].key
        # The process is wired to the concrete ids minted for its dependencies.
        proc_calls = [c for c in mock_exec.call_args_list if c.kwargs["comp"].key == "main_process"]
        assert len(proc_calls) == 1
        proc_cfg = proc_calls[0].kwargs["config"]
        assert proc_cfg["source"]["connection_id"] == "id-source_db_connection"
        assert proc_cfg["source"]["operation_id"] == "id-source_db_get_operation"
        assert proc_cfg["transform"]["map_ref"] == "id-transform_transform_map"
        assert proc_cfg["target"]["connection_id"] == "id-target_rest_connection"
        assert proc_cfg["target"]["operation_id"] == "id-target_rest_operation"

    @patch(_EXECUTE_TARGET, side_effect=_execute_stub)
    @patch(_PAGINATE_TARGET)
    def test_map_script_apply_resolves_script_wrapper_refs(self, mock_pag, mock_exec):
        mock_pag.return_value = []
        spec = _spec(_map_script())
        result = build_integration_action(
            MagicMock(),
            _PROFILE,
            "apply",
            {"integration_spec": spec, "dry_run": False, "conflict_policy": "reuse"},
        )
        assert result["_success"] is True, result
        # The planner synthesizes a transform.function wrapper for the script
        # route; every executed component must have had its $refs resolved.
        executed_keys = {c.kwargs["comp"].key for c in mock_exec.call_args_list}
        assert any(
            c.kwargs["comp"].type == "transform.function"
            for c in mock_exec.call_args_list
        )
        assert any(
            c.kwargs["comp"].type == "script.mapping"
            for c in mock_exec.call_args_list
        )
        for call in mock_exec.call_args_list:
            assert not _has_ref_token(call.kwargs["config"]), call.kwargs["comp"].key
        assert "main_process" in executed_keys

    @patch(_EXECUTE_TARGET, side_effect=_execute_stub)
    @patch(_PAGINATE_TARGET)
    def test_reuse_mode_records_reused_without_creating(self, mock_pag, mock_exec):
        # Reuse connections carry an explicit component_id, so the plan resolves
        # them to reuse independent of any name lookup; no created connections.
        mock_pag.return_value = []
        spec = _spec(_full_reuse())
        result = build_integration_action(
            MagicMock(),
            _PROFILE,
            "apply",
            {"integration_spec": spec, "dry_run": False, "conflict_policy": "reuse"},
        )
        assert result["_success"] is True, result
        db = result["results"]["source_db_connection"]
        rest = result["results"]["target_rest_connection"]
        assert db["status"] == "reused"
        assert db["component_id"] == "<<existing DB connection id>>"
        assert rest["status"] == "reused"
        assert rest["component_id"] == "<<existing REST connection id>>"
        # reference_only connections must never reach _execute_component.
        executed_keys = {c.kwargs["comp"].key for c in mock_exec.call_args_list}
        assert "source_db_connection" not in executed_keys
        assert "target_rest_connection" not in executed_keys
        # The reused ids still flow into the process via $ref resolution.
        proc_cfg = next(
            c.kwargs["config"]
            for c in mock_exec.call_args_list
            if c.kwargs["comp"].key == "main_process"
        )
        assert proc_cfg["source"]["connection_id"] == "<<existing DB connection id>>"
        assert proc_cfg["target"]["connection_id"] == "<<existing REST connection id>>"

    @patch(_EXECUTE_TARGET, side_effect=_execute_stub)
    @patch(_PAGINATE_TARGET)
    def test_unresolvable_plan_fails_fast_without_executing(self, mock_pag, mock_exec):
        # A reference_only connection that resolves to nothing (no component_id,
        # zero name matches) makes the plan emit error_missing_target, which the
        # apply path must reject before executing anything.
        mock_pag.return_value = []  # zero candidates for every name lookup
        spec = copy.deepcopy(_spec(_minimal()))
        conn = _by_key(spec)["source_db_connection"]
        conn["component_id"] = None
        conn["config"]["reference_only"] = True
        conn["config"].pop("component_id", None)
        result = build_integration_action(
            MagicMock(),
            _PROFILE,
            "apply",
            {"integration_spec": spec, "dry_run": False, "conflict_policy": "reuse"},
        )
        assert result["_success"] is False, result
        mock_exec.assert_not_called()

    @patch(_EXECUTE_TARGET, side_effect=_execute_stub)
    @patch(_PAGINATE_TARGET)
    def test_conflict_policy_fail_blocks_existing_connection(self, mock_pag, mock_exec):
        # A create-mode connection whose name already exists must fail fast under
        # conflict_policy=fail. The connection is the first component executed,
        # so nothing is created before the policy trips.
        spec = _spec(_minimal())
        db_name = _by_key(spec)["source_db_connection"]["name"]

        # Return a match for every lookup; the first create component (the DB
        # connection) trips conflict_policy=fail immediately.
        mock_pag.return_value = [
            {"component_id": "existing-db-id", "name": db_name, "folder_name": "#Common"}
        ]
        result = build_integration_action(
            MagicMock(),
            _PROFILE,
            "apply",
            {"integration_spec": spec, "dry_run": False, "conflict_policy": "fail"},
        )
        assert result["_success"] is False, result
        mock_exec.assert_not_called()


# ===========================================================================
# Verify path (round-trips the in-memory build registry)
# ===========================================================================


class TestVerifyPath:
    @patch("boomi_mcp.categories.integration_builder.component_get_xml")
    @patch(_EXECUTE_TARGET, side_effect=_execute_stub)
    @patch(_PAGINATE_TARGET)
    def test_verify_after_mocked_apply_succeeds(self, mock_pag, mock_exec, mock_get):
        mock_pag.return_value = []
        # Issue #80: verify now consumes component_get_xml's return value, so the
        # mock must return the real dict shape. The main process returns a
        # graph-valid process XML; non-process components a non-process stub.
        mock_get.side_effect = _verify_get_side_effect
        spec = _spec(_minimal())
        applied = build_integration_action(
            MagicMock(),
            _PROFILE,
            "apply",
            {"integration_spec": spec, "dry_run": False},
        )
        assert applied["_success"] is True, applied
        verified = build_integration_action(
            MagicMock(), _PROFILE, "verify", {"build_id": applied["build_id"]}
        )
        assert verified["_success"] is True, verified
        assert verified["failed_components"] == 0
        assert not verified["dependency_issues"]
        # The process component carries a clean graph section; non-process
        # components carry none (issue #80, M9.4).
        proc_record = verified["verification"]["main_process"]
        assert proc_record["process_graph"]["errors"] == []
        assert "process_graph" not in verified["verification"]["source_db_connection"]


# ===========================================================================
# Secret / safety boundaries on the full chain
# ===========================================================================


class TestSafetyBoundaries:
    @patch(_EXECUTE_TARGET, side_effect=_execute_stub)
    @patch(_PAGINATE_TARGET)
    def test_apply_envelope_never_echoes_dlq_address(self, mock_pag, mock_exec):
        mock_pag.return_value = []
        spec = _spec(_full_reuse())
        result = build_integration_action(
            MagicMock(),
            _PROFILE,
            "apply",
            {"integration_spec": spec, "dry_run": False, "conflict_policy": "reuse"},
        )
        assert "<<dlq queue address>>" not in json.dumps(result)

    def test_apply_error_envelope_never_echoes_credential_ref(self):
        # When apply fails fast (conflict_policy=fail on an existing connection),
        # the failure envelope must not surface the DB credential_ref.
        with (
            patch(_PAGINATE_TARGET) as mock_pag,
            patch(_EXECUTE_TARGET, side_effect=_execute_stub) as mock_exec,
        ):
            spec = _spec(_minimal())
            db_name = _by_key(spec)["source_db_connection"]["name"]
            mock_pag.return_value = [
                {"component_id": "existing-db-id", "name": db_name, "folder_name": "#Common"}
            ]
            result = build_integration_action(
                MagicMock(),
                _PROFILE,
                "apply",
                {"integration_spec": spec, "dry_run": False, "conflict_policy": "fail"},
            )
        assert result["_success"] is False
        mock_exec.assert_not_called()
        assert "secrets/db/svc_sync" not in json.dumps(result)

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

from boomi_mcp.categories.components.builders.process_flow_builder import (
    SyncPipelineBuilder,
)
from boomi_mcp.categories.integration_builder import _build_plan
from boomi_mcp.categories.transformation_review import review_transformation_action
from boomi_mcp.patterns.archetypes.database_to_api_sync import (
    DatabaseToApiSyncParameters,
    _build_sync_pipeline_adapter_config,
)


def build_from_archetype_action(*args, **kwargs):
    """The retired ``build_from_archetype`` tool's action (#159 S3 deletes it).

    Resolved at CALL time, so this module still collects once it is gone and only
    the tests that call it are affected.
    """
    from boomi_mcp.categories import integration_authoring

    return getattr(integration_authoring, "build_from_archetype_action")(*args, **kwargs)


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
            # guidance_only DLQ does not wire a catch path, so retry must stay 1
            # (issue #88 validator: retry>1 requires a wired DLQ). Wired-DLQ
            # retry is covered by test_wired_dlq_retry_reaches_process_builder.
            "retry": {"max_attempts": 1, "backoff": "platform"},
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


# ---------------------------------------------------------------------------
# M5.3 (#71): database_to_api_sync as an adapter over sync_pipeline
# ---------------------------------------------------------------------------


class TestM53SyncPipelineAdapter:
    """The archetype derives its linear core through SyncPipelineBuilder.lower_config
    (M5.3 #71) while keeping legacy output: process_kind stays database_to_api_sync,
    the returned spec keeps pipeline=null, and no audit/reliability shell is added."""

    # The deterministic component keys the minimal payload emits (see
    # test_minimal_emits_expected_component_graph above).
    _KEYS = dict(
        db_conn_key="source_db_connection",
        db_op_key="source_db_get_operation",
        map_key="transform_transform_map",
        rest_conn_key="target_rest_connection",
        rest_op_key="target_rest_operation",
    )

    def _adapter_config(self) -> Dict[str, Any]:
        params = DatabaseToApiSyncParameters.model_validate(_minimal())
        return _build_sync_pipeline_adapter_config(params, **self._KEYS)

    def test_adapter_config_is_read_map_send_stage_graph(self):
        cfg = self._adapter_config()
        assert cfg["process_kind"] == "sync_pipeline"
        stages = cfg["pipeline"]["stages"]
        assert [s["kind"] for s in stages] == ["read", "map", "send"]
        assert [s["key"] for s in stages] == ["source", "transform", "target"]
        assert stages[0]["config"]["primitive"] == "db_read"
        assert stages[1]["config"]["primitive"] == "map"
        assert stages[2]["config"]["primitive"] == "rest_send"
        # Exactly the two ordering edges that wire source -> transform -> target.
        assert cfg["pipeline"]["dependencies"] == [
            {"from_stage": "source", "to_stage": "transform"},
            {"from_stage": "transform", "to_stage": "target"},
        ]
        # No legacy-only block leaks into the semantic config (lower_config rejects
        # them); they are reattached only after lowering.
        for stage in stages:
            assert "reliability" not in stage["config"]
            assert "dynamic_path" not in stage["config"]
        assert "reliability" not in cfg
        assert "folder_name" not in cfg
        assert "process_extensions" not in cfg

    def test_lower_config_reproduces_legacy_core(self):
        lowered = SyncPipelineBuilder.lower_config(self._adapter_config())
        assert lowered == {
            "process_kind": "database_to_api_sync",
            "source": {
                "connector_type": "database",
                "action_type": "Get",
                "connection_id": "$ref:source_db_connection",
                "operation_id": "$ref:source_db_get_operation",
            },
            "transform": {"mode": "map_ref", "map_ref": "$ref:transform_transform_map"},
            "target": {
                "connector_type": "rest",
                "action_type": "POST",
                "connection_id": "$ref:target_rest_connection",
                "operation_id": "$ref:target_rest_operation",
            },
        }

# ---------------------------------------------------------------------------
# Operational intent (metadata only)
# ---------------------------------------------------------------------------


# ---------------------------------------------------------------------------
# Transform routes
# ---------------------------------------------------------------------------


# ---------------------------------------------------------------------------
# build_integration(action='plan')
# ---------------------------------------------------------------------------


# ---------------------------------------------------------------------------
# review_transformation against the executable spec
# ---------------------------------------------------------------------------


# ---------------------------------------------------------------------------
# Negative / secret boundaries
# ---------------------------------------------------------------------------


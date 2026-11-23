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

from boomi_mcp.categories.integration_builder import build_integration_action
from boomi_mcp.categories.transformation_review import review_transformation_action


def build_from_archetype_action(*args, **kwargs):
    """The retired ``build_from_archetype`` tool's action (#159 S3 deletes it).

    Resolved at CALL time, so this module still collects once it is gone and only
    the tests that call it are affected.
    """
    from boomi_mcp.categories import integration_authoring

    return getattr(integration_authoring, "build_from_archetype_action")(*args, **kwargs)


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


# ===========================================================================
# Apply path: dry-run, mocked real apply, $ref resolution, fail-fast
# ===========================================================================


# ===========================================================================
# Verify path (round-trips the in-memory build registry)
# ===========================================================================


# ===========================================================================
# Secret / safety boundaries on the full chain
# ===========================================================================


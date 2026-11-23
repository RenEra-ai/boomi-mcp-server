"""Issue #100 G2 — database_to_api_sync per-document REST path replacements.

Exercises the ``target.send_request.path_replacements`` surface: validation
(token presence, uniqueness, leaf binding) and lowering into the process target
``dynamic_path`` block (DPP name, request-profile $ref, and profile-element
segments sourced from the SAME JSON profile field index the map uses, so the
emitted ``<profileelement>`` matches the generated request profile).
"""

from typing import Any, Dict

import pytest

from boomi_mcp.categories.components.builders.connector_builder import (
    BuilderValidationError,
)
from boomi_mcp.patterns.archetypes.database_to_api_sync import (
    DatabaseToApiSyncArchetype,
    _build_dynamic_path,
)


def build_from_archetype_action(*args, **kwargs):
    """The retired ``build_from_archetype`` tool's action (#159 S3 deletes it).

    Resolved at CALL time, so this module still collects once it is gone and only
    the tests that call it are affected.
    """
    from boomi_mcp.categories import integration_authoring

    return getattr(integration_authoring, "build_from_archetype_action")(*args, **kwargs)


def _params(path: str, path_replacements, children=None) -> Dict[str, Any]:
    children = children or [
        {"name": "clientId", "kind": "simple", "data_type": "character", "required": True},
        {"name": "name", "kind": "simple", "data_type": "character"},
    ]
    return {
        "naming": {"integration_name": "cds-sync", "component_prefix": "CDS"},
        "source": {
            "binding": {
                "mode": "create",
                "settings": {
                    "driver": "microsoft_jdbc",
                    "auth_mode": "username_password",
                    "host": "db.internal",
                    "database": "AppDB",
                    "username": "svc",
                    "credential_ref": "secrets/db/svc",
                },
            },
            "read_operation": {
                "sql": "<<read>>",
                "result_schema": {
                    "fields": [
                        {"name": "client_code", "data_type": "character"},
                        {"name": "client_name", "data_type": "character"},
                    ]
                },
            },
        },
        "target": {
            "binding": {
                "mode": "create",
                "settings": {
                    "base_url": "http://host.docker.internal:8081",
                    "auth_mode": "none",
                },
            },
            "send_request": {
                "method": "PATCH",
                "path": path,
                "path_replacements": path_replacements,
            },
            "payload_profile": {
                "format": "json",
                "root": {"name": "Root", "kind": "object", "children": children},
            },
        },
        "transform": {
            "operations": [
                {"operation_type": "direct", "source_field": "client_code", "target_path": "Root/clientId"},
                {"operation_type": "direct", "source_field": "client_name", "target_path": "Root/name"},
            ]
        },
        "execution": {"trigger": {"mode": "manual"}},
        "reliability": {"retry": {"max_attempts": 1}, "dlq": {"enabled": False}, "error_classifier": {}},
    }


def _by_key(payload):
    result = build_from_archetype_action("database_to_api_sync", payload)
    assert result["_success"] is True, result
    return {c["key"]: c for c in result["integration_spec"]["components"]}


def _main_process(payload):
    return _by_key(payload)["main_process"]


def _expect_rejected(payload):
    result = build_from_archetype_action("database_to_api_sync", payload)
    assert result["_success"] is False, result
    assert result["error_code"] == "PARAM_VALIDATION_FAILED", result


# ---------------------------------------------------------------------------
# Lowering
# ---------------------------------------------------------------------------


# ---------------------------------------------------------------------------
# Validation
# ---------------------------------------------------------------------------


def test_build_dynamic_path_defends_against_post_validation_brace():
    # Issue #127 B3 (review r2): defense-in-depth backstop. The contract
    # validator rejects residual braces, but if a validated params object is
    # mutated afterwards to reintroduce an empty '{}' (which the non-empty-token
    # regex skips), _build_dynamic_path must raise ARCHETYPE_PARAM_INVALID rather
    # than silently emit the '{}' as a literal static path segment.
    payload = _params(
        "/v1/clients/{clientId}",
        [{"name": "clientId", "target_path": "Root/clientId"}],
    )
    params = DatabaseToApiSyncArchetype.validate_parameters(payload)
    # Bypass the contract-layer check via post-validation mutation.
    params.target.send_request.path = "/v1/{}/{clientId}"
    with pytest.raises(BuilderValidationError) as exc:
        _build_dynamic_path(params)
    assert exc.value.error_code == "ARCHETYPE_PARAM_INVALID"

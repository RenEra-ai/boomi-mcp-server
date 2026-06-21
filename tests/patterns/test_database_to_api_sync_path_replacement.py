"""Issue #100 G2 — database_to_api_sync per-document REST path replacements.

Exercises the ``target.send_request.path_replacements`` surface: validation
(token presence, uniqueness, leaf binding) and lowering into the process target
``dynamic_path`` block (DPP name, request-profile $ref, and profile-element
segments sourced from the SAME JSON profile field index the map uses, so the
emitted ``<profileelement>`` matches the generated request profile).
"""

from typing import Any, Dict

from boomi_mcp.categories.integration_authoring import build_from_archetype_action


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


def _main_process(payload):
    result = build_from_archetype_action("database_to_api_sync", payload)
    assert result["_success"] is True, result
    by_key = {c["key"]: c for c in result["integration_spec"]["components"]}
    return by_key["main_process"]


def _expect_rejected(payload):
    result = build_from_archetype_action("database_to_api_sync", payload)
    assert result["_success"] is False, result
    assert result["error_code"] == "PARAM_VALIDATION_FAILED", result


# ---------------------------------------------------------------------------
# Lowering
# ---------------------------------------------------------------------------


def test_path_replacement_lowered_to_dynamic_path():
    proc = _main_process(
        _params(
            "/admin/cdscm/api/v1/clients/{clientId}",
            [{"name": "clientId", "target_path": "Root/clientId"}],
        )
    )
    dp = proc["config"]["target"]["dynamic_path"]
    assert dp["ddp_name"] == "DDP_PATH_CLIENTS"
    assert dp["request_profile_id"] == "$ref:transform_target_profile"
    assert dp["profile_type"] == "profile.json"
    assert dp["segments"] == [
        {"type": "static", "value": "/admin/cdscm/api/v1/clients/"},
        {
            "type": "profile",
            "element_id": 3,
            "element_name": "clientId (Root/Object/clientId)",
        },
    ]
    # The dynamic_path $ref must be a declared dependency for reachability.
    assert "transform_target_profile" in proc["depends_on"]


def test_no_path_replacements_emits_no_dynamic_path():
    payload = _params("/v1/customers", [])
    proc = _main_process(payload)
    assert "dynamic_path" not in proc["config"]["target"]
    assert "transform_target_profile" not in proc["depends_on"]


def test_dpp_name_derives_from_resource_segment():
    proc = _main_process(
        _params(
            "/admin/cdscm/api/v1/matters/{clientId}",
            [{"name": "clientId", "target_path": "Root/clientId"}],
        )
    )
    assert proc["config"]["target"]["dynamic_path"]["ddp_name"] == "DDP_PATH_MATTERS"


def test_trailing_static_segment_preserved():
    proc = _main_process(
        _params(
            "/v1/clients/{clientId}/activate",
            [{"name": "clientId", "target_path": "Root/clientId"}],
        )
    )
    segs = proc["config"]["target"]["dynamic_path"]["segments"]
    assert segs[0] == {"type": "static", "value": "/v1/clients/"}
    assert segs[1]["type"] == "profile"
    assert segs[2] == {"type": "static", "value": "/activate"}


# ---------------------------------------------------------------------------
# Validation
# ---------------------------------------------------------------------------


def test_duplicate_replacement_names_rejected():
    _expect_rejected(
        _params(
            "/v1/clients/{clientId}",
            [
                {"name": "clientId", "target_path": "Root/clientId"},
                {"name": "clientId", "target_path": "Root/name"},
            ],
        )
    )


def test_replacement_name_missing_from_path_rejected():
    _expect_rejected(
        _params(
            "/v1/clients/{otherId}",
            [{"name": "clientId", "target_path": "Root/clientId"}],
        )
    )


def test_replacement_target_not_a_leaf_rejected():
    _expect_rejected(
        _params(
            "/v1/clients/{clientId}",
            [{"name": "clientId", "target_path": "Root/does_not_exist"}],
        )
    )


def test_undeclared_path_token_rejected():
    # '{region}' has no matching replacement -> would survive as a literal in the
    # emitted path; reject it (Codex review P2).
    _expect_rejected(
        _params(
            "/v1/clients/{clientId}/{region}",
            [{"name": "clientId", "target_path": "Root/clientId"}],
        )
    )


def test_replacement_target_unbound_rejected():
    # 'extra' is a declared simple leaf but no transform output binds it, so it
    # carries no mapped value at runtime — reject as a path source.
    children = [
        {"name": "clientId", "kind": "simple", "data_type": "character", "required": True},
        {"name": "name", "kind": "simple", "data_type": "character"},
        {"name": "extra", "kind": "simple", "data_type": "character"},
    ]
    _expect_rejected(
        _params(
            "/v1/clients/{extra}",
            [{"name": "extra", "target_path": "Root/extra"}],
            children=children,
        )
    )

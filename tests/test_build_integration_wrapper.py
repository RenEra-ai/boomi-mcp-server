"""Issue #30: MCP wrapper tests for build_integration (database_to_api_sync).

Exercises the server.py wrapper boundary for the M2 apply path:

* registration + openWorldHint (build_integration mutates Boomi).
* config must be a JSON *string*: malformed / non-object JSON returns a
  structured failure BEFORE any credential read or Boomi() construction.
* plan + dry-run apply route through the wrapper with get_current_user /
  get_secret / Boomi fully patched — no real credentials or network — and the
  dry run never calls _execute_component.

No live-account dependencies. Synthetic sentinel payloads only.
"""

import json
import os
import sys
from pathlib import Path
from unittest.mock import MagicMock, patch

_project_root = str(Path(__file__).resolve().parent.parent)
if _project_root not in sys.path:
    sys.path.insert(0, _project_root)

os.environ["BOOMI_LOCAL"] = "true"

import server  # noqa: E402

_PAGINATE_TARGET = "boomi_mcp.categories.integration_builder.paginate_metadata"
_EXECUTE_TARGET = "boomi_mcp.categories.integration_builder._execute_component"


_MINIMAL_PAYLOAD = {
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
            "result_schema": {"fields": [{"name": "source_a", "data_type": "character"}]},
        },
    },
    "target": {
        "binding": {"mode": "create", "settings": {"base_url": "https://api.example.com", "auth_mode": "none"}},
        "send_request": {"method": "POST", "path": "/v1/items"},
        "payload_profile": {
            "format": "json",
            "root": {
                "name": "Root",
                "kind": "object",
                "children": [{"name": "target_a", "kind": "simple", "data_type": "character"}],
            },
        },
    },
    "transform": {
        "operations": [
            {"operation_type": "direct", "source_field": "source_a", "target_path": "Root/target_a"}
        ]
    },
    "execution": {"trigger": {"mode": "manual"}},
    "reliability": {"retry": {"max_attempts": 1}, "dlq": {"enabled": False}, "error_classifier": {}},
}


def _emit_spec():
    result = server.build_from_archetype("database_to_api_sync", _MINIMAL_PAYLOAD)
    assert result["_success"] is True, result
    return result["integration_spec"]


def _patch_creds():
    """Context managers patching the credential + Boomi seam on server."""
    m_user = patch.object(server, "get_current_user", return_value="qa-user")
    m_secret = patch.object(
        server,
        "get_secret",
        return_value={"account_id": "acct", "username": "u", "password": "p"},
    )
    m_boomi = patch.object(server, "Boomi", return_value=MagicMock())
    return m_user, m_secret, m_boomi


# ---------------------------------------------------------------------------
# Registration + annotations
# ---------------------------------------------------------------------------


def test_build_integration_registered_open_world():
    import asyncio

    loop = asyncio.new_event_loop()
    try:
        tools = loop.run_until_complete(server.mcp.list_tools())
    finally:
        loop.close()
    tool = {t.name: t for t in tools}.get("build_integration")
    assert tool is not None, "build_integration not registered"
    ann = tool.annotations
    open_world = getattr(ann, "openWorldHint", None)
    if open_world is None and isinstance(ann, dict):
        open_world = ann.get("openWorldHint")
    assert open_world is True


# ---------------------------------------------------------------------------
# Config JSON-string boundary (short-circuits before Boomi)
# ---------------------------------------------------------------------------


def test_malformed_json_config_returns_structured_failure_without_boomi():
    m_user, m_secret, m_boomi = _patch_creds()
    with m_user as u, m_secret as s, m_boomi as b:
        result = server.build_integration(profile="qa", action="plan", config="{not json")
    assert result["_success"] is False
    assert "JSON" in result["error"] or "json" in result["error"]
    u.assert_not_called()
    s.assert_not_called()
    b.assert_not_called()


def test_non_object_json_config_returns_structured_failure_without_boomi():
    m_user, m_secret, m_boomi = _patch_creds()
    with m_user as u, m_secret as s, m_boomi as b:
        result = server.build_integration(profile="qa", action="plan", config="[1, 2, 3]")
    assert result["_success"] is False
    assert "config must be a JSON object" in result["error"]
    u.assert_not_called()
    s.assert_not_called()
    b.assert_not_called()


# ---------------------------------------------------------------------------
# Plan + dry-run apply route through the wrapper with mocked creds
# ---------------------------------------------------------------------------


def test_build_from_archetype_wrapper_for_database_to_api_sync():
    result = server.build_from_archetype("database_to_api_sync", _MINIMAL_PAYLOAD)
    assert result["_success"] is True
    assert result["boomi_mutation"] is False
    assert result["raw_xml_exposed"] is False
    assert "main_process" in {c["key"] for c in result["integration_spec"]["components"]}


def test_plan_through_wrapper_succeeds_with_mocked_creds():
    spec = _emit_spec()
    config = json.dumps({"integration_spec": spec})
    m_user, m_secret, m_boomi = _patch_creds()
    with m_user, m_secret, m_boomi, patch(_PAGINATE_TARGET) as mock_pag:
        mock_pag.return_value = []
        result = server.build_integration(profile="qa", action="plan", config=config)
    assert result["_success"] is True, result
    assert result["profile"] == "qa"
    assert result["execution_order"][-1] == "main_process"


def test_dry_run_apply_through_wrapper_does_not_execute():
    spec = _emit_spec()
    config = json.dumps({"integration_spec": spec})  # no dry_run -> defaults True
    m_user, m_secret, m_boomi = _patch_creds()
    with (
        m_user,
        m_secret,
        m_boomi,
        patch(_PAGINATE_TARGET) as mock_pag,
        patch(_EXECUTE_TARGET) as mock_exec,
    ):
        mock_pag.return_value = []
        result = server.build_integration(profile="qa", action="apply", config=config)
    assert result["_success"] is True, result
    assert result["dry_run"] is True
    mock_exec.assert_not_called()


def test_unknown_action_through_wrapper_returns_failure():
    spec = _emit_spec()
    config = json.dumps({"integration_spec": spec})
    m_user, m_secret, m_boomi = _patch_creds()
    with m_user, m_secret, m_boomi:
        result = server.build_integration(profile="qa", action="frobnicate", config=config)
    assert result["_success"] is False
    assert "Unknown action" in result["error"]

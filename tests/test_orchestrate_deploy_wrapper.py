"""Wrapper tests for the orchestrate_deploy MCP tool in server.py (Issue #64).

These exercise the PUBLIC wrapper (server.orchestrate_deploy) — not the engine, which is
covered exhaustively by tests/test_orchestrate_deploy_contract.py. They assert the wrapper's
own responsibilities:
- config JSON parsing + structured wrapper errors (malformed / non-object) short-circuit
  before any auth/SDK/action call;
- required-field failures surface before credentials are read;
- dry_run=true (the default) calls the engine with no Boomi client and reads no credentials;
- dry_run=false validates WITHOUT credentials first, then builds the SDK only once the engine
  reports BOOMI_CLIENT_REQUIRED;
- the response is normalized with top-level process_id/environment_id/runtime_id + next_steps;
- the real engine composes package -> deploy -> runtime binding when driven through the wrapper.
"""

import asyncio
import os
import sys
from pathlib import Path
from unittest.mock import MagicMock, patch

import pytest

_project_root = str(Path(__file__).resolve().parent.parent)
if _project_root not in sys.path:
    sys.path.insert(0, _project_root)

# Force local mode before importing server.
os.environ["BOOMI_LOCAL"] = "true"

import server  # noqa: E402

# server.py puts ``src`` on sys.path, so the boomi_mcp namespace it uses is importable here.
from boomi_mcp.categories import integration_builder  # noqa: E402
from boomi_mcp.categories.deployment import orchestration  # noqa: E402


FAKE_CREDS = {
    "account_id": "acct-test",
    "username": "user",
    "password": "pass",
}


@pytest.fixture(autouse=True)
def _mock_auth_and_sdk():
    """Patch auth helpers + SDK so the wrapper never hits real services, and so each test can
    assert whether the credential path was taken."""
    with (
        patch.object(server, "get_current_user", return_value="test-user") as m_user,
        patch.object(server, "get_secret", return_value=FAKE_CREDS) as m_secret,
        patch.object(server, "Boomi", return_value=MagicMock()) as m_boomi,
    ):
        yield {"user": m_user, "secret": m_secret, "boomi": m_boomi}


def _run_async(coro):
    loop = asyncio.new_event_loop()
    try:
        return loop.run_until_complete(coro)
    finally:
        loop.close()


# ---------------------------------------------------------------------------
# 1. Registration + annotations
# ---------------------------------------------------------------------------
def test_registered_with_annotations():
    tools = {t.name: t for t in _run_async(server.mcp.list_tools())}
    assert "orchestrate_deploy" in tools, "orchestrate_deploy must be registered as an MCP tool"
    ann = tools["orchestrate_deploy"].annotations
    assert ann is not None, "orchestrate_deploy must carry tool annotations"
    assert ann.destructiveHint is True
    assert ann.readOnlyHint is False
    assert ann.openWorldHint is True


# ---------------------------------------------------------------------------
# 2./3. config JSON short-circuits before any auth/SDK/action
# ---------------------------------------------------------------------------
def test_malformed_json_short_circuits(_mock_auth_and_sdk):
    with patch.object(server, "orchestrate_deploy_action") as m_action:
        result = server.orchestrate_deploy(
            profile="dev", build_id="b1", environment_id="e1", runtime_id="r1",
            config="not-json",
        )
    assert result["_success"] is False
    assert "Invalid config" in result["error"]
    assert result["errors"][0]["code"] == "INVALID_CONFIG_JSON"
    assert "warnings" in result and "next_steps" in result
    m_action.assert_not_called()
    _mock_auth_and_sdk["user"].assert_not_called()
    _mock_auth_and_sdk["secret"].assert_not_called()
    _mock_auth_and_sdk["boomi"].assert_not_called()


def test_non_object_config_short_circuits(_mock_auth_and_sdk):
    with patch.object(server, "orchestrate_deploy_action") as m_action:
        result = server.orchestrate_deploy(
            profile="dev", build_id="b1", environment_id="e1", runtime_id="r1",
            config="[1, 2, 3]",
        )
    assert result["_success"] is False
    assert "config must be a JSON object" in result["error"]
    assert result["errors"][0]["code"] == "INVALID_CONFIG_TYPE"
    assert "next_steps" in result
    m_action.assert_not_called()
    _mock_auth_and_sdk["secret"].assert_not_called()
    _mock_auth_and_sdk["boomi"].assert_not_called()


def test_malformed_dry_run_in_config_fails_closed(_mock_auth_and_sdk):
    """A non-bool dry_run from config must NOT silently take the real-run/deploy path.

    A falsey non-bool (e.g. []) is rejected with a structured error before any engine call,
    credential read, or SDK construction — fail closed for a destructive tool."""
    for bad in ('{"dry_run": []}', '{"dry_run": "false"}', '{"dry_run": 0}', '{"dry_run": null}'):
        with patch.object(server, "orchestrate_deploy_action") as m_action:
            result = server.orchestrate_deploy(
                profile="dev", build_id="b1", environment_id="e1", runtime_id="r1",
                config=bad,
            )
        assert result["_success"] is False, f"dry_run={bad} must be rejected"
        assert result["errors"][0]["code"] == "INVALID_CONFIG_TYPE"
        assert result["errors"][0]["field"] == "dry_run"
        assert "next_steps" in result
        m_action.assert_not_called()
        _mock_auth_and_sdk["secret"].assert_not_called()
        _mock_auth_and_sdk["boomi"].assert_not_called()


def test_explicit_bool_dry_run_in_config_is_honored(_mock_auth_and_sdk):
    """A real boolean dry_run in config still works (true → no creds, plan only)."""
    plan = {"_success": True, "plan_only": True, "target": {}, "summary": {}, "errors": [], "warnings": []}
    with patch.object(server, "orchestrate_deploy_action", return_value=plan) as m_action:
        result = server.orchestrate_deploy(
            profile="dev", build_id="b1", environment_id="e1", runtime_id="r1",
            config='{"dry_run": true}',
        )
    assert result["_success"] is True
    assert m_action.call_args.kwargs["dry_run"] is True
    _mock_auth_and_sdk["secret"].assert_not_called()


def test_cleanup_on_failure_config_forwarded_to_engine(_mock_auth_and_sdk):
    """A valid bool cleanup_on_failure from config is threaded into the engine call (issue #65)."""
    plan = {"_success": True, "plan_only": True, "target": {}, "summary": {}, "errors": [], "warnings": []}
    with patch.object(server, "orchestrate_deploy_action", return_value=plan) as m_action:
        result = server.orchestrate_deploy(
            profile="dev", build_id="b1", environment_id="e1", runtime_id="r1",
            config='{"dry_run": true, "cleanup_on_failure": true}',
        )
    assert result["_success"] is True
    assert m_action.call_args.kwargs["cleanup_on_failure"] is True


def test_invalid_cleanup_on_failure_in_config_fails_closed(_mock_auth_and_sdk):
    """A non-bool cleanup_on_failure is rejected with INVALID_CONFIG_TYPE before any engine call."""
    for bad in (
        '{"cleanup_on_failure": "yes"}',
        '{"cleanup_on_failure": []}',
        '{"cleanup_on_failure": 1}',
    ):
        with patch.object(server, "orchestrate_deploy_action") as m_action:
            result = server.orchestrate_deploy(
                profile="dev", build_id="b1", environment_id="e1", runtime_id="r1",
                config=bad,
            )
        assert result["_success"] is False, f"cleanup_on_failure={bad} must be rejected"
        assert result["errors"][0]["code"] == "INVALID_CONFIG_TYPE"
        assert result["errors"][0]["field"] == "cleanup_on_failure"
        assert "next_steps" in result
        m_action.assert_not_called()
        _mock_auth_and_sdk["secret"].assert_not_called()
        _mock_auth_and_sdk["boomi"].assert_not_called()


# ---------------------------------------------------------------------------
# 4. Missing required fields surface before credentials (real engine)
# ---------------------------------------------------------------------------
def test_missing_required_fields_before_credentials(_mock_auth_and_sdk):
    # Real action (not patched). Only profile given; dry_run defaults to True.
    result = server.orchestrate_deploy(profile="dev")
    assert result["_success"] is False
    codes = {e.get("code") for e in result["errors"]}
    assert {"BUILD_ID_REQUIRED", "ENVIRONMENT_ID_REQUIRED", "RUNTIME_ID_REQUIRED"} <= codes
    assert "next_steps" in result
    _mock_auth_and_sdk["secret"].assert_not_called()
    _mock_auth_and_sdk["boomi"].assert_not_called()


# ---------------------------------------------------------------------------
# 5. Dry-run: no client, no credentials, normalized response
# ---------------------------------------------------------------------------
def test_dry_run_passes_no_client_no_creds(_mock_auth_and_sdk):
    plan = {
        "_success": True,
        "plan_only": True,
        "dry_run": True,
        "build_id": "b1",
        "target": {"process_component_id": "CID-1"},
        "summary": {"environment_id": "env-1", "runtime_id": "rt-1"},
        "errors": [],
        "warnings": [],
    }
    with patch.object(server, "orchestrate_deploy_action", return_value=plan) as m_action:
        result = server.orchestrate_deploy(
            profile="dev", build_id="b1", environment_id="env-1", runtime_id="rt-1",
        )
    m_action.assert_called_once()
    _, kwargs = m_action.call_args
    assert kwargs["boomi_client"] is None
    assert kwargs["creds"] is None
    assert kwargs["dry_run"] is True
    # No credential path on a dry-run.
    _mock_auth_and_sdk["user"].assert_not_called()
    _mock_auth_and_sdk["secret"].assert_not_called()
    _mock_auth_and_sdk["boomi"].assert_not_called()
    # Normalized top-level aliases + next_steps.
    assert result["process_id"] == "CID-1"
    assert result["environment_id"] == "env-1"
    assert result["runtime_id"] == "rt-1"
    assert isinstance(result["next_steps"], list) and result["next_steps"]


def test_dry_run_alias_falls_back_to_request_args(_mock_auth_and_sdk):
    # Engine response lacking summary env/runtime -> aliases fall back to the request args.
    plan = {
        "_success": True,
        "plan_only": True,
        "target": {"process_component_id": "CID-9"},
        "summary": {},
        "errors": [],
        "warnings": [],
    }
    with patch.object(server, "orchestrate_deploy_action", return_value=plan):
        result = server.orchestrate_deploy(
            profile="dev", build_id="b1", environment_id="env-Z", runtime_id="rt-Z",
        )
    assert result["process_id"] == "CID-9"
    assert result["environment_id"] == "env-Z"
    assert result["runtime_id"] == "rt-Z"


# ---------------------------------------------------------------------------
# 6. Real run builds the SDK only after a no-secret preflight
# ---------------------------------------------------------------------------
def test_real_run_builds_sdk(_mock_auth_and_sdk):
    preflight = {
        "_success": False,
        "error": "A Boomi client is required.",
        "errors": [{"code": "BOOMI_CLIENT_REQUIRED", "field": "boomi_client"}],
    }
    success = {
        "_success": True,
        "plan_only": False,
        "target": {"process_component_id": "CID-1"},
        "summary": {"environment_id": "env-1", "runtime_id": "rt-1"},
        "errors": [],
        "warnings": [],
    }
    with patch.object(
        server, "orchestrate_deploy_action", side_effect=[preflight, success]
    ) as m_action:
        result = server.orchestrate_deploy(
            profile="dev", build_id="b1", environment_id="env-1", runtime_id="rt-1",
            dry_run=False,
        )
    assert m_action.call_count == 2
    # First call (preflight) had no client/creds.
    assert m_action.call_args_list[0].kwargs["boomi_client"] is None
    assert m_action.call_args_list[0].kwargs["creds"] is None
    # Second call got the constructed SDK and the resolved creds.
    assert m_action.call_args_list[1].kwargs["boomi_client"] is server.Boomi.return_value
    assert m_action.call_args_list[1].kwargs["creds"] == FAKE_CREDS
    assert m_action.call_args_list[1].kwargs["dry_run"] is False
    # Credentials were read and the SDK was constructed with the expected params.
    _mock_auth_and_sdk["secret"].assert_called_once_with("test-user", "dev")
    sdk_kwargs = server.Boomi.call_args.kwargs
    assert sdk_kwargs["account_id"] == "acct-test"
    assert sdk_kwargs["username"] == "user"
    assert sdk_kwargs["password"] == "pass"
    assert sdk_kwargs["timeout"] == 30000
    assert result["_success"] is True
    assert result["process_id"] == "CID-1"


def test_real_run_sdk_includes_optional_base_url(_mock_auth_and_sdk):
    """When the stored credentials carry base_url, the SDK must be built with it."""
    preflight = {"_success": False, "errors": [{"code": "BOOMI_CLIENT_REQUIRED"}]}
    success = {"_success": True, "plan_only": False, "target": {"process_component_id": "CID-1"},
               "summary": {"environment_id": "env-1", "runtime_id": "rt-1"}, "errors": [], "warnings": []}
    creds_with_url = {**FAKE_CREDS, "base_url": "https://api.example.test/v1"}
    _mock_auth_and_sdk["secret"].return_value = creds_with_url
    with patch.object(server, "orchestrate_deploy_action", side_effect=[preflight, success]):
        server.orchestrate_deploy(
            profile="dev", build_id="b1", environment_id="env-1", runtime_id="rt-1",
            dry_run=False,
        )
    sdk_kwargs = server.Boomi.call_args.kwargs
    assert sdk_kwargs["base_url"] == "https://api.example.test/v1"


def test_real_run_invalid_input_short_circuits_before_secret(_mock_auth_and_sdk):
    # A non-BOOMI_CLIENT_REQUIRED failure on the preflight must NOT read secrets.
    preflight_fail = {
        "_success": False,
        "error": "Build process not found.",
        "errors": [{"code": "BUILD_PROCESS_NOT_FOUND", "field": "build_id"}],
    }
    with patch.object(
        server, "orchestrate_deploy_action", return_value=preflight_fail
    ) as m_action:
        result = server.orchestrate_deploy(
            profile="dev", build_id="b1", environment_id="env-1", runtime_id="rt-1",
            dry_run=False,
        )
    assert m_action.call_count == 1
    assert m_action.call_args.kwargs["boomi_client"] is None
    assert result["_success"] is False
    assert "next_steps" in result
    _mock_auth_and_sdk["secret"].assert_not_called()
    _mock_auth_and_sdk["boomi"].assert_not_called()


# ---------------------------------------------------------------------------
# Config-merge precedence
# ---------------------------------------------------------------------------
def test_top_level_args_override_config(_mock_auth_and_sdk):
    plan = {"_success": True, "plan_only": True, "target": {}, "summary": {}, "errors": [], "warnings": []}
    with patch.object(server, "orchestrate_deploy_action", return_value=plan) as m_action:
        server.orchestrate_deploy(
            profile="dev",
            build_id="ARG-BUILD",
            config='{"build_id": "CONFIG-BUILD", "package_version": "2.0"}',
        )
    kwargs = m_action.call_args.kwargs
    assert kwargs["build_id"] == "ARG-BUILD", "top-level build_id must override config build_id"
    assert kwargs["package_version"] == "2.0", "config-only keys must still be forwarded"


# ---------------------------------------------------------------------------
# 8. Stage composition through the wrapper with the real engine + mocked handlers
# ---------------------------------------------------------------------------
class _FakeAction:
    """Records (action, config_data) calls; returns canned dicts per action."""

    def __init__(self, responses, *, label=None, order_log=None):
        self.responses = responses
        self.label = label
        self.order_log = order_log
        self.calls = []

    def __call__(self, sdk=None, profile=None, action=None, config_data=None, **kwargs):
        self.calls.append(action)
        if self.order_log is not None:
            self.order_log.append((self.label, action))
        if action not in self.responses:
            raise AssertionError(f"unexpected {self.label} action: {action}")
        return self.responses[action]


def _att(att_id, *, atom_id=None, environment_id=None, process_id=None):
    out = {"id": att_id}
    if atom_id is not None:
        out["atom_id"] = atom_id
    if environment_id is not None:
        out["environment_id"] = environment_id
    if process_id is not None:
        out["process_id"] = process_id
    return out


def _single_process_entry(process_id="CID-1"):
    return {
        "created_at": "2026-01-01T00:00:00+00:00",
        "profile": "prof",
        "spec": {
            "version": "1.0",
            "name": "MyIntegration",
            "mode": "lift_shift",
            "components": [
                {"key": "conn", "type": "connector-settings", "action": "create",
                 "name": "Conn", "component_id": None, "config": {}, "depends_on": []},
                {"key": "proc", "type": "process", "action": "create",
                 "name": "My Process", "component_id": None, "config": {}, "depends_on": []},
            ],
            "goals": [], "endpoints": [], "flows": [], "naming": {},
            "folders": {}, "runtime": {}, "validation_rules": {},
        },
        "results": {
            "conn": {"status": "created", "component_id": "CONN-1",
                     "type": "connector-settings", "name": "Conn"},
            "proc": {"status": "created", "component_id": process_id,
                     "type": "process", "name": "My Process"},
        },
        "execution_order": ["conn", "proc"],
    }


def test_stage_composition_with_real_action(monkeypatch, _mock_auth_and_sdk):
    """Drive the REAL engine through the wrapper with every low-level handler mocked for a
    clean reuse path (existing package + active deploy + pre-existing bindings, no schedule).
    Assert the public envelope keeps the stage summary AND the wrapper's normalized aliases,
    and that package/deploy run before the runtime binding."""
    bid = "wrap-compose-1"
    integration_builder._BUILD_REGISTRY[bid] = _single_process_entry("CID-1")
    order = []
    try:
        deployment_responses = {
            "list_packages": {"_success": True, "packages": [
                {"package_id": "pkg-1", "component_id": "CID-1", "component_type": "process",
                 "package_version": bid, "created_date": "2026-01-01T00:00:00Z"}]},
            "list_deployments": {"_success": True, "deployments": [
                {"deployment_id": "dep-1", "active": True, "current_version": "1"}]},
            "list_process_environment_attachments": {"_success": True,
                "attachments": [_att("pe-1", process_id="CID-1", environment_id="env-1")]},
            "list_process_atom_attachments": {"_success": True,
                "attachments": [_att("pa-1", process_id="CID-1", atom_id="rt-1")]},
        }
        runtimes_responses = {
            "get": {"_success": True, "runtime": {"id": "rt-1"}},
            "list_attachments": {"_success": True,
                "attachments": [_att("ea-1", atom_id="rt-1", environment_id="env-1")]},
        }
        env_responses = {"get": {"_success": True, "environment": {"id": "env-1"}}}

        monkeypatch.setattr(orchestration, "manage_deployment_action",
                            _FakeAction(deployment_responses, label="deployment", order_log=order))
        monkeypatch.setattr(orchestration, "manage_environments_action",
                            _FakeAction(env_responses, label="environments", order_log=order))
        monkeypatch.setattr(orchestration, "manage_runtimes_action",
                            _FakeAction(runtimes_responses, label="runtimes", order_log=order))
        monkeypatch.setattr(orchestration, "manage_schedules_action",
                            _FakeAction({}, label="schedules", order_log=order))

        result = server.orchestrate_deploy(
            profile="dev", build_id=bid, environment_id="env-1", runtime_id="rt-1",
            dry_run=False,
        )
    finally:
        integration_builder._BUILD_REGISTRY.pop(bid, None)

    assert result["_success"] is True, result
    # The high-level summary survives, with every stage object present.
    for stage in ("package", "deployment", "runtime_attachment", "schedule"):
        assert stage in result, f"{stage} stage missing from composed response"
    assert "summary" in result
    # Wrapper normalization aliases.
    assert result["process_id"] == "CID-1"
    assert result["environment_id"] == "env-1"
    assert result["runtime_id"] == "rt-1"
    assert isinstance(result["next_steps"], list) and result["next_steps"]
    # Package/deploy occur before the runtime binding.
    i_pkg = next(i for i, (lbl, a) in enumerate(order)
                 if lbl == "deployment" and a == "list_packages")
    i_rt = next(i for i, (lbl, a) in enumerate(order)
                if lbl == "runtimes" and a == "get")
    assert i_pkg < i_rt, "package/deploy must run before runtime binding"
    # The credential path was taken (real run) and built the SDK once.
    _mock_auth_and_sdk["secret"].assert_called_once()
    _mock_auth_and_sdk["boomi"].assert_called_once()


def test_stage_composition_deploys_then_binds_then_schedules(monkeypatch, _mock_auth_and_sdk):
    """Full composition with an ACTUAL deploy call + schedule_override: assert the engine,
    driven through the wrapper, runs package/deploy BEFORE runtime binding BEFORE the schedule."""
    bid = "wrap-compose-2"
    integration_builder._BUILD_REGISTRY[bid] = _single_process_entry("CID-1")
    order = []
    try:
        deployment_responses = {
            # Existing package is reused, but there is NO active deployment -> engine deploys.
            "list_packages": {"_success": True, "packages": [
                {"package_id": "pkg-1", "component_id": "CID-1", "component_type": "process",
                 "package_version": bid, "created_date": "2026-01-01T00:00:00Z"}]},
            "list_deployments": {"_success": True, "deployments": []},
            "deploy": {"_success": True, "deployment": {
                "deployment_id": "dep-new", "active": True, "version": 1}},
            "list_process_environment_attachments": {"_success": True,
                "attachments": [_att("pe-1", process_id="CID-1", environment_id="env-1")]},
            "list_process_atom_attachments": {"_success": True,
                "attachments": [_att("pa-1", process_id="CID-1", atom_id="rt-1")]},
        }
        runtimes_responses = {
            "get": {"_success": True, "runtime": {"id": "rt-1"}},
            "list_attachments": {"_success": True,
                "attachments": [_att("ea-1", atom_id="rt-1", environment_id="env-1")]},
        }
        env_responses = {"get": {"_success": True, "environment": {"id": "env-1"}}}
        schedules_responses = {
            "update": {"_success": True, "schedule": {"id": "sch-1"}},
            "enable": {"_success": True, "status": {"id": "sst-1", "enabled": True}},
        }

        monkeypatch.setattr(orchestration, "manage_deployment_action",
                            _FakeAction(deployment_responses, label="deployment", order_log=order))
        monkeypatch.setattr(orchestration, "manage_environments_action",
                            _FakeAction(env_responses, label="environments", order_log=order))
        monkeypatch.setattr(orchestration, "manage_runtimes_action",
                            _FakeAction(runtimes_responses, label="runtimes", order_log=order))
        monkeypatch.setattr(orchestration, "manage_schedules_action",
                            _FakeAction(schedules_responses, label="schedules", order_log=order))

        result = server.orchestrate_deploy(
            profile="dev", build_id=bid, environment_id="env-1", runtime_id="rt-1",
            config='{"schedule_override": {"cron": "0 9 * * *"}}',
            dry_run=False,
        )
    finally:
        integration_builder._BUILD_REGISTRY.pop(bid, None)

    assert result["_success"] is True, result
    # An actual deploy was performed (not just a reuse).
    assert ("deployment", "deploy") in order, "engine must have called the deploy action"
    assert result["deployment"]["status"] == "deployed"
    assert result["schedule"]["status"] in ("enabled", "updated"), result["schedule"]

    def _first(label, action):
        return next(i for i, (lbl, a) in enumerate(order) if lbl == label and a == action)

    i_deploy = _first("deployment", "deploy")
    i_runtime = _first("runtimes", "get")
    i_schedule = min(i for i, (lbl, _a) in enumerate(order) if lbl == "schedules")
    last_bind = max(
        i for i, (lbl, _a) in enumerate(order) if lbl in ("deployment", "runtimes", "environments")
    )
    assert i_deploy < i_runtime, "deploy must precede runtime binding"
    assert i_schedule > last_bind, "schedule must run strictly after every package/deploy/bind call"

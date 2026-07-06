"""orchestrate_deploy listener_verify stage tests (M6, issue #12).

Mocked-router unit tests for the WSS listener verification stage: apiType
preflight -> deployment-active check -> component-query collision check ->
authenticated live probe (with the objectName-casing 404 fallback) ->
execution-record readback. ``ListenerStatus`` is never consulted (live-proven
empty for WSS routes on both runtimes, 2026-07-04).

Same import discipline as test_orchestrate_deploy_contract.py: everything goes
through the single ``src.boomi_mcp...`` prefix so the seeded registry is the
one the action reads.
"""

import inspect
import sys
from pathlib import Path
from unittest.mock import MagicMock

import pytest

_project_root = str(Path(__file__).resolve().parent.parent)
if _project_root not in sys.path:
    sys.path.insert(0, _project_root)
_src_path = str(Path(__file__).resolve().parent.parent / "src")
if _src_path not in sys.path:
    sys.path.insert(0, _src_path)

from src.boomi_mcp.categories.integration_builder import _BUILD_REGISTRY  # noqa: E402
from src.boomi_mcp.categories.deployment import orchestrate_deploy_action  # noqa: E402
from src.boomi_mcp.categories.deployment import orchestration  # noqa: E402


# ---------------------------------------------------------------------------
# Fixtures & fakes
# ---------------------------------------------------------------------------


@pytest.fixture
def registry():
    inserted = []

    def seed(build_id, entry):
        _BUILD_REGISTRY[build_id] = entry
        inserted.append(build_id)
        return build_id

    yield seed

    for build_id in inserted:
        _BUILD_REGISTRY.pop(build_id, None)


class _FakeAction:
    def __init__(self, responses, *, label=None):
        self.responses = responses
        self.label = label
        self.calls = []

    def __call__(self, sdk=None, profile=None, action=None, config_data=None, **kwargs):
        self.calls.append({"action": action, "config_data": config_data})
        if action not in self.responses:
            raise AssertionError(f"unexpected {self.label} action call: {action}")
        resp = self.responses[action]
        if isinstance(resp, list):
            if not resp:
                raise AssertionError(f"no queued response left for action: {action}")
            return resp.pop(0) if len(resp) > 1 else resp[0]
        return resp

    def actions_called(self):
        return [c["action"] for c in self.calls]


class _FakeProbe:
    """Queued (status_code, error) responses; records every probed URL/method/headers."""

    def __init__(self, responses):
        self.responses = list(responses)
        self.calls = []

    def __call__(self, url, *, method, payload, headers, timeout_seconds):
        self.calls.append(
            {"url": url, "method": method, "payload": payload, "headers": dict(headers)}
        )
        if not self.responses:
            raise AssertionError("no queued probe response left")
        return self.responses.pop(0) if len(self.responses) > 1 else self.responses[0]


class _FakeTime:
    """monotonic()/sleep() stand-in so the readback loop never really waits."""

    def __init__(self):
        self.now = 0.0

    def monotonic(self):
        return self.now

    def sleep(self, seconds):
        self.now += seconds


_LISTENER_META = {
    "object_name": "orderIntake",
    "operation_type": "EXECUTE",
    "input_type": "singlejson",
    "output_type": "none",
    "response_content_type": "text/plain",
    "http_method": "POST",
    "endpoint_path": "/ws/simple/executeOrderIntake",
    "test_mode_supported": False,
}


def _listener_entry(*, process_id="CID-1", listener_meta=None):
    """A recorded listener build: one process whose spec carries
    validation_rules.listener (the archetype-emitted block)."""
    return {
        "created_at": "2026-01-01T00:00:00+00:00",
        "profile": "prof",
        "spec": {
            "version": "1.0",
            "name": "ListenerIntegration",
            "mode": "redesign",
            "components": [
                {
                    "key": "proc",
                    "type": "process",
                    "action": "create",
                    "name": "Listener Process",
                    "component_id": None,
                    # Classification keys off this Listen source binding; the
                    # validation_rules.listener block below is metadata only.
                    "config": {
                        "process_kind": "database_to_api_sync",
                        "source": {
                            "connector_type": "wss",
                            "action_type": "Listen",
                            "operation_id": "WSSOP-LIT",
                        },
                    },
                    "depends_on": [],
                }
            ],
            "goals": [],
            "endpoints": [],
            "flows": [],
            "naming": {},
            "folders": {},
            "runtime": {},
            "validation_rules": {
                "listener": dict(listener_meta if listener_meta is not None else _LISTENER_META)
            },
        },
        "results": {
            "proc": {
                "status": "created",
                "component_id": process_id,
                "type": "process",
                "name": "Listener Process",
            }
        },
        "execution_order": ["proc"],
    }


def _server_info(api_type="intermediate", auth="none", url="http://atom.local:9090", token=None):
    info = {"api_type": api_type, "auth": auth, "url": url}
    if token is not None:
        info["auth_token"] = token
    return {"_success": True, "server_info": info}


def _deployment_responses(*, collision_deployments=None):
    """Package/deploy + attachment + collision-scan responses.

    ``list_deployments`` is queued: the deploy stage consumes the first
    response; the collision scan consumes the second.
    """
    deploy_stage = {
        "_success": True,
        "deployments": [{"deployment_id": "dep-1", "active": True, "current_version": "1"}],
    }
    collision = {
        "_success": True,
        "deployments": list(collision_deployments or []),
    }
    return {
        "list_packages": {
            "_success": True,
            "packages": [
                {
                    "package_id": "pkg-1",
                    "component_id": "CID-1",
                    "component_type": "process",
                    "package_version": "v1",
                    "created_date": "2026-01-01T00:00:00Z",
                }
            ],
        },
        "list_deployments": [deploy_stage, collision],
        "list_process_environment_attachments": {
            "_success": True,
            "attachments": [{"id": "pe-1", "process_id": "CID-1", "environment_id": "env-1"}],
        },
        "list_process_atom_attachments": {
            "_success": True,
            "attachments": [{"id": "pa-1", "process_id": "CID-1", "atom_id": "rt-1"}],
        },
    }


def _patch_real_run(
    monkeypatch,
    *,
    server_info,
    probe,
    execution_records,
    baseline_records=None,
    collision_deployments=None,
    component_xml=None,
):
    """Patch every router/probe seam for a full real run reaching listener_verify."""
    dep = _FakeAction(
        _deployment_responses(collision_deployments=collision_deployments),
        label="deployment",
    )
    env = _FakeAction(
        {"get": {"_success": True, "environment": {"id": "env-1"}}}, label="environments"
    )
    rt = _FakeAction(
        {
            "get": {"_success": True, "runtime": {"id": "rt-1"}},
            "list_attachments": {
                "_success": True,
                "attachments": [{"id": "ea-1", "atom_id": "rt-1", "environment_id": "env-1"}],
            },
        },
        label="runtimes",
    )
    sch = _FakeAction({}, label="schedules")
    shared = _FakeAction({"get_server_info": server_info}, label="shared_resources")
    # The monitoring fake serves the PRE-PROBE baseline snapshot first, then the
    # post-probe readback (queued-list semantics: last response repeats).
    monitor = _FakeAction(
        {"execution_records": [baseline_records or _RECORD_EMPTY, execution_records]},
        label="monitoring",
    )
    monkeypatch.setattr(orchestration, "manage_deployment_action", dep)
    monkeypatch.setattr(orchestration, "manage_environments_action", env)
    monkeypatch.setattr(orchestration, "manage_runtimes_action", rt)
    monkeypatch.setattr(orchestration, "manage_schedules_action", sch)
    monkeypatch.setattr(orchestration, "manage_shared_resources_action", shared)
    monkeypatch.setattr(orchestration, "monitor_platform_action", monitor)
    monkeypatch.setattr(orchestration, "_listener_probe", probe)
    monkeypatch.setattr(orchestration, "time", _FakeTime())

    def _fake_component_get_xml(client, component_id, **kwargs):
        xml_map = component_xml or {}
        if component_id not in xml_map:
            raise Exception(f"unexpected component read: {component_id}")
        return {"xml": xml_map[component_id]}

    monkeypatch.setattr(orchestration, "component_get_xml", _fake_component_get_xml)

    def _explode(*args, **kwargs):
        raise AssertionError("Test-mode execution must never run for a listener build")

    monkeypatch.setattr(orchestration, "execute_process_action", _explode)
    return {"deployment": dep, "shared": shared, "monitor": monitor, "probe": probe}


def _run(bid, **overrides):
    kwargs = dict(
        boomi_client=MagicMock(),
        profile="prof",
        build_id=bid,
        environment_id="env-1",
        runtime_id="rt-1",
        dry_run=False,
        # Matches the fake list_packages entry so the package stage reuses it.
        package_version="v1",
        creds={"account_id": "acct-1", "username": "u", "password": "p"},
    )
    kwargs.update(overrides)
    return orchestrate_deploy_action(**kwargs)


_RECORD_OK = {
    "_success": True,
    "total_count": 1,
    "execution_records": [
        {"execution_id": "exec-1", "status": "COMPLETE", "execution_type": "exec_listener"}
    ],
}
_RECORD_EMPTY = {"_success": True, "total_count": 0, "execution_records": []}


# ---------------------------------------------------------------------------
# Dry-run placeholders + non-listener behavior
# ---------------------------------------------------------------------------


def test_dry_run_listener_build_plans_listener_verify(registry):
    bid = registry("b-l-plan", _listener_entry())
    result = orchestrate_deploy_action(
        build_id=bid, environment_id="env-1", runtime_id="rt-1", dry_run=True
    )
    assert result["_success"] is True
    assert result["listener_verify"]["status"] == "planned"
    assert result["listener_verify"]["endpoint_path"] == "/ws/simple/executeOrderIntake"
    assert result["listener_verify"]["http_method"] == "POST"


def test_dry_run_listener_with_run_test_marks_execution_not_required(registry):
    bid = registry("b-l-plan-test", _listener_entry())
    result = orchestrate_deploy_action(
        build_id=bid, environment_id="env-1", runtime_id="rt-1",
        dry_run=True, run_test=True,
    )
    assert result["execution"]["status"] == "not_required"
    assert any("LISTENER_NO_TEST_MODE" in w for w in result["execution"]["warnings"])
    assert result["logs"]["status"] == "not_required"


def test_non_listener_build_reports_not_required_and_skips_stage(registry, monkeypatch):
    """A non-listener build must never call the shared-resources preflight."""
    entry = _listener_entry()
    del entry["spec"]["validation_rules"]["listener"]
    bid = registry("b-nl", entry)

    def _explode(*args, **kwargs):
        raise AssertionError("listener preflight must not run for a non-listener build")

    monkeypatch.setattr(orchestration, "manage_shared_resources_action", _explode)
    result = orchestrate_deploy_action(
        build_id=bid, environment_id="env-1", runtime_id="rt-1", dry_run=True
    )
    assert result["listener_verify"]["status"] == "not_required"


def _hand_authored_entry(*, process_references_op: bool):
    """A hand-authored spec (no validation_rules.listener) carrying a WSS Listen
    operation component; the process SOURCE binding references it only when
    process_references_op is True."""
    entry = _listener_entry()
    del entry["spec"]["validation_rules"]["listener"]
    entry["spec"]["components"].append(
        {
            "key": "wss_op",
            "type": "connector-action",
            "action": "create",
            "name": "Listener Op",
            "component_id": None,
            "config": {
                "connector_type": "wss",
                "operation_mode": "listen",
                "object_name": "handRolled",
                "operation_type": "CREATE",
                "input_type": "none",
            },
            "depends_on": [],
        }
    )
    if process_references_op:
        entry["spec"]["components"][0]["config"] = {
            "process_kind": "database_to_api_sync",
            "source": {
                "connector_type": "wss",
                "action_type": "Listen",
                "operation_id": "$ref:wss_op",
            },
        }
    return entry


def test_fallback_detection_from_process_source_binding(registry):
    """A hand-authored spec without validation_rules.listener is detected via
    the deploy-target process's OWN Listen source binding, resolved to the
    referenced operation component."""
    bid = registry("b-hand", _hand_authored_entry(process_references_op=True))
    result = orchestrate_deploy_action(
        build_id=bid, environment_id="env-1", runtime_id="rt-1", dry_run=True
    )
    stage = result["listener_verify"]
    assert stage["status"] == "planned"
    assert stage["endpoint_path"] == "/ws/simple/createHandRolled"
    # input_type=none -> GET.
    assert stage["http_method"] == "GET"


def test_unreferenced_wss_op_is_not_a_listener_build(registry):
    """Codex review (M6 #12): a spec that merely CONTAINS a WSS Listen operation
    the deployed process does not use must NOT be classified as a listener
    build (no skipped Test mode, no listener_verify against an unserved route)."""
    bid = registry("b-hand-neg", _hand_authored_entry(process_references_op=False))
    result = orchestrate_deploy_action(
        build_id=bid, environment_id="env-1", runtime_id="rt-1", dry_run=True, run_test=True
    )
    assert result["listener_verify"]["status"] == "not_required"
    # The Test-mode execution stage is NOT suppressed for a non-listener build.
    assert result["execution"]["status"] == "planned"


def test_sync_pipeline_listener_stage_detected(registry):
    """The fallback also recognizes a sync_pipeline listener stage binding."""
    entry = _hand_authored_entry(process_references_op=False)
    entry["spec"]["components"][0]["config"] = {
        "process_kind": "sync_pipeline",
        "pipeline": {
            "stages": [
                {"key": "listen", "kind": "listener",
                 "config": {"primitive": "wss_listen", "operation_id": "$ref:wss_op"}},
                {"key": "send", "kind": "send",
                 "config": {"primitive": "rest_send", "action_type": "POST",
                            "connection_id": "C1", "operation_id": "O1"}},
            ],
            "dependencies": [{"from_stage": "listen", "to_stage": "send"}],
        },
    }
    bid = registry("b-hand-sp", entry)
    result = orchestrate_deploy_action(
        build_id=bid, environment_id="env-1", runtime_id="rt-1", dry_run=True
    )
    assert result["listener_verify"]["status"] == "planned"
    assert result["listener_verify"]["endpoint_path"] == "/ws/simple/createHandRolled"


def test_external_literal_operation_ref_returns_non_listener(registry):
    """A listener process referencing an EXTERNAL (literal, out-of-spec) WSS
    operation cannot be endpoint-derived — no listener_verify rather than a
    wrong probe."""
    entry = _hand_authored_entry(process_references_op=True)
    entry["spec"]["components"][0]["config"]["source"]["operation_id"] = (
        "11111111-2222-3333-4444-555555555555"
    )
    bid = registry("b-hand-ext", entry)
    result = orchestrate_deploy_action(
        build_id=bid, environment_id="env-1", runtime_id="rt-1", dry_run=True
    )
    assert result["listener_verify"]["status"] == "not_required"


# ---------------------------------------------------------------------------
# Real-run success path
# ---------------------------------------------------------------------------


def test_real_run_success_probe_and_readback(registry, monkeypatch):
    bid = registry("b-l-ok", _listener_entry())
    probe = _FakeProbe([(200, None)])
    fakes = _patch_real_run(
        monkeypatch,
        server_info=_server_info(auth="none"),
        probe=probe,
        execution_records=_RECORD_OK,
    )
    result = _run(bid)
    assert result["_success"] is True, result
    stage = result["listener_verify"]
    assert stage["status"] == "completed"
    assert stage["api_type"] == "intermediate"
    assert stage["deployment_active"] is True
    assert stage["collision_count"] == 0
    assert stage["probe_status_code"] == 200
    assert stage["endpoint_url"] == "http://atom.local:9090/ws/simple/executeOrderIntake"
    # A 2xx on the primary (sentence-cased) path — the live-settled default.
    assert stage["served_object_name_casing"] == "sentence_case"
    assert stage["execution_record_found"] is True
    assert stage["execution_id"] == "exec-1"
    assert stage["execution_status"] == "COMPLETE"
    # auth=none: no Authorization header; JSON input defaults an empty-object body.
    (call,) = probe.calls
    assert "Authorization" not in call["headers"]
    assert call["method"] == "POST"
    assert call["payload"] == b"{}"
    assert call["headers"]["Content-Type"] == "application/json"
    # The stage summary block is present for agents.
    assert result["summary"]["listener"]["probe_status_code"] == 200
    assert result["summary"]["stage_statuses"]["listener_verify"] == "completed"
    # Architect review (M6 #12): a completed listener_verify with a COMPLETE
    # execution IS the behavioral verification (strict baseline only).
    assert stage["readback_baseline_available"] is True
    assert result["behavior_verified"] == {
        "verified": True,
        "reason": "listener_probe_verified",
        "logs_status": result["logs"]["status"],
    }


def test_listener_base_url_override_wins(registry, monkeypatch):
    bid = registry("b-l-url", _listener_entry())
    probe = _FakeProbe([(200, None)])
    _patch_real_run(
        monkeypatch,
        server_info=_server_info(url="http://renera-local-atom:9090"),
        probe=probe,
        execution_records=_RECORD_OK,
    )
    result = _run(bid, listener_base_url="http://localhost:9090/")
    assert result["_success"] is True
    assert result["listener_verify"]["endpoint_url"] == (
        "http://localhost:9090/ws/simple/executeOrderIntake"
    )


def test_basic_auth_header_from_server_token_and_account_id(registry, monkeypatch):
    bid = registry("b-l-auth", _listener_entry())
    probe = _FakeProbe([(200, None)])
    _patch_real_run(
        monkeypatch,
        server_info=_server_info(auth="basic", token="tok-123", url="https://cloud.example"),
        probe=probe,
        execution_records=_RECORD_OK,
    )
    result = _run(bid)
    assert result["_success"] is True
    (call,) = probe.calls
    import base64

    expected = "Basic " + base64.b64encode(b"acct-1:tok-123").decode("ascii")
    assert call["headers"]["Authorization"] == expected


def test_listener_auth_username_override(registry, monkeypatch):
    bid = registry("b-l-authuser", _listener_entry())
    probe = _FakeProbe([(200, None)])
    _patch_real_run(
        monkeypatch,
        server_info=_server_info(auth="basic", token="tok-123", url="https://cloud.example"),
        probe=probe,
        execution_records=_RECORD_OK,
    )
    result = _run(bid, listener_auth_username="acct-1.M6SUFFIX")
    assert result["_success"] is True
    (call,) = probe.calls
    assert call["headers"]["Authorization"].startswith("Basic ")
    import base64

    decoded = base64.b64decode(call["headers"]["Authorization"][6:]).decode()
    assert decoded == "acct-1.M6SUFFIX:tok-123"


def test_run_test_true_listener_marks_execution_not_required(registry, monkeypatch):
    """Listeners have no Test mode: run_test=True never executes the process."""
    bid = registry("b-l-runtest", _listener_entry())
    probe = _FakeProbe([(200, None)])
    _patch_real_run(
        monkeypatch,
        server_info=_server_info(),
        probe=probe,
        execution_records=_RECORD_OK,
    )
    result = _run(bid, run_test=True)
    assert result["_success"] is True
    assert result["execution"]["status"] == "not_required"
    assert any("LISTENER_NO_TEST_MODE" in w for w in result["execution"]["warnings"])
    # The listener probe verified the build — the marker reflects that even
    # though the Test-mode stage is not_required (architect review, M6 #12).
    assert result["behavior_verified"]["verified"] is True
    assert result["behavior_verified"]["reason"] == "listener_probe_verified"


def test_error_execution_record_surfaces_warning_but_passes(registry, monkeypatch):
    """HTTP 200 is only the ack (outputType=none decouples it from process
    outcome — live-proven via an ERROR execution behind a 200)."""
    bid = registry("b-l-err-exec", _listener_entry())
    probe = _FakeProbe([(200, None)])
    records = {
        "_success": True,
        "total_count": 1,
        "execution_records": [
            {"execution_id": "exec-9", "status": "ERROR", "execution_type": "exec_listener"}
        ],
    }
    _patch_real_run(
        monkeypatch,
        server_info=_server_info(),
        probe=probe,
        execution_records=records,
    )
    result = _run(bid)
    assert result["_success"] is True
    stage = result["listener_verify"]
    assert stage["status"] == "completed"
    assert stage["execution_status"] == "ERROR"
    assert any("LISTENER_EXECUTION_ERROR" in w for w in stage["warnings"])
    assert any("LISTENER_EXECUTION_ERROR" in w for w in result["warnings"])
    assert result["behavior_verified"] == {
        "verified": False,
        "reason": "listener_execution_not_complete",
        "logs_status": result["logs"]["status"],
    }


# ---------------------------------------------------------------------------
# Failure triage
# ---------------------------------------------------------------------------


def test_advanced_api_type_bare_wss_fails_asc_required(registry, monkeypatch):
    # M6.1 (#133): advanced + bare WSS now names the shipped remedy — publish
    # through an API Service Component (asc_wrapper) — instead of deferring.
    bid = registry("b-l-adv", _listener_entry())
    probe = _FakeProbe([(200, None)])
    fakes = _patch_real_run(
        monkeypatch,
        server_info=_server_info(api_type="advanced"),
        probe=probe,
        execution_records=_RECORD_OK,
    )
    result = _run(bid)
    assert result["_success"] is False
    assert result["error_code"] == "LISTENER_ASC_REQUIRED"
    assert result["failed_stage"] == "listener_verify"
    assert "asc_wrapper" in result["error"]
    assert result["listener_verify"]["status"] == "failed"
    assert result["listener_verify"]["publish_mode"] == "bare_wss"
    # Fails BEFORE any probe; execution/logs blocked.
    assert probe.calls == []
    assert result["execution"]["status"] == "blocked"
    assert result["logs"]["status"] == "blocked"
    # Prior stages are visible for a safe retry.
    assert set(result["prior_stage_summary"]) == {
        "package", "deployment", "runtime_attachment", "schedule",
    }


def test_server_info_failure_is_structured(registry, monkeypatch):
    bid = registry("b-l-nosi", _listener_entry())
    probe = _FakeProbe([(200, None)])
    fakes = _patch_real_run(
        monkeypatch,
        server_info={"_success": False, "error": "boom"},
        probe=probe,
        execution_records=_RECORD_OK,
    )
    result = _run(bid)
    assert result["_success"] is False
    assert result["error_code"] == "LISTENER_SERVER_INFO_FAILED"
    assert probe.calls == []


def test_collision_detected_via_component_reads(registry, monkeypatch):
    """The collision check is component-query based — never a pre-probe."""
    bid = registry("b-l-coll", _listener_entry())
    probe = _FakeProbe([(200, None)])
    other_process_xml = (
        '<bns:Component xmlns:bns="http://api.platform.boomi.com/" type="process">'
        "<bns:object><process><shapes>"
        '<shape shapetype="start"><configuration>'
        '<connectoraction actionType="Listen" connectorType="wss" operationId="OTHER-OP"/>'
        "</configuration></shape>"
        "</shapes></process></bns:object></bns:Component>"
    )
    other_op_xml = (
        '<bns:Component xmlns:bns="http://api.platform.boomi.com/" type="connector-action" subType="wss">'
        "<bns:object><Operation><Configuration>"
        '<WebServicesServerListenAction inputType="singlejson" objectName="orderIntake" '
        'operationType="EXECUTE" outputType="none" responseContentType="text/plain"/>'
        "</Configuration></Operation></bns:object></bns:Component>"
    )
    _patch_real_run(
        monkeypatch,
        server_info=_server_info(),
        probe=probe,
        execution_records=_RECORD_OK,
        collision_deployments=[
            {"deployment_id": "dep-2", "component_id": "OTHER-1", "component_type": "process", "active": True}
        ],
        component_xml={"OTHER-1": other_process_xml, "OTHER-OP": other_op_xml},
    )
    result = _run(bid)
    assert result["_success"] is False
    assert result["error_code"] == "LISTENER_PATH_COLLISION"
    assert result["listener_verify"]["collision_count"] == 1
    # Collisions block BEFORE the probe (a pre-probe is uninformative on clouds).
    assert probe.calls == []


def test_non_colliding_deployed_listener_passes(registry, monkeypatch):
    bid = registry("b-l-nocoll", _listener_entry())
    probe = _FakeProbe([(200, None)])
    other_process_xml = (
        '<bns:Component xmlns:bns="http://api.platform.boomi.com/" type="process">'
        "<bns:object><process><shapes>"
        '<shape shapetype="start"><configuration>'
        '<connectoraction actionType="Listen" connectorType="wss" operationId="OTHER-OP"/>'
        "</configuration></shape>"
        "</shapes></process></bns:object></bns:Component>"
    )
    other_op_xml = (
        '<bns:Component xmlns:bns="http://api.platform.boomi.com/" type="connector-action" subType="wss">'
        "<bns:object><Operation><Configuration>"
        '<WebServicesServerListenAction inputType="singlejson" objectName="differentPath" '
        'operationType="EXECUTE" outputType="none" responseContentType="text/plain"/>'
        "</Configuration></Operation></bns:object></bns:Component>"
    )
    _patch_real_run(
        monkeypatch,
        server_info=_server_info(),
        probe=probe,
        execution_records=_RECORD_OK,
        collision_deployments=[
            {"deployment_id": "dep-2", "component_id": "OTHER-1", "component_type": "process", "active": True}
        ],
        component_xml={"OTHER-1": other_process_xml, "OTHER-OP": other_op_xml},
    )
    result = _run(bid)
    assert result["_success"] is True
    assert result["listener_verify"]["collision_count"] == 0


def test_401_triage_names_no_route_baseline(registry, monkeypatch):
    bid = registry("b-l-401", _listener_entry())
    probe = _FakeProbe([(401, None)])
    _patch_real_run(
        monkeypatch,
        server_info=_server_info(auth="basic", token="tok-1", url="https://cloud.example"),
        probe=probe,
        execution_records=_RECORD_OK,
    )
    result = _run(bid)
    assert result["_success"] is False
    assert result["error_code"] == "LISTENER_PROBE_FAILED"
    assert "no route is registered" in result["error"]
    assert result["listener_verify"]["probe_status_code"] == 401


def test_404_both_casings_triage_names_wrong_path(registry, monkeypatch):
    bid = registry("b-l-404", _listener_entry())
    probe = _FakeProbe([(404, None), (404, None)])
    _patch_real_run(
        monkeypatch,
        server_info=_server_info(),
        probe=probe,
        execution_records=_RECORD_OK,
    )
    result = _run(bid)
    assert result["_success"] is False
    assert result["error_code"] == "LISTENER_PROBE_FAILED"
    assert "no route matches this path" in result["error"]
    # The casing fallback probed BOTH paths: sentence-cased primary first
    # (the live-settled formula), then the defensive verbatim variant.
    assert [c["url"].rsplit("/", 1)[-1] for c in probe.calls] == [
        "executeOrderIntake",
        "executeorderIntake",
    ]


def test_404_verbatim_fallback_records_contradiction(registry, monkeypatch):
    """Sentence-cased primary 404 + verbatim 200 -> completed via the defensive
    fallback, with the contradiction of the live-settled casing recorded."""
    bid = registry("b-l-case", _listener_entry())
    probe = _FakeProbe([(404, None), (200, None)])
    _patch_real_run(
        monkeypatch,
        server_info=_server_info(),
        probe=probe,
        execution_records=_RECORD_OK,
    )
    result = _run(bid)
    assert result["_success"] is True
    stage = result["listener_verify"]
    assert stage["status"] == "completed"
    assert stage["served_object_name_casing"] == "verbatim"
    assert stage["endpoint_path"] == "/ws/simple/executeorderIntake"
    assert any("LISTENER_OBJECT_NAME_VERBATIM" in w for w in stage["warnings"])


def test_network_error_is_probe_failed(registry, monkeypatch):
    bid = registry("b-l-net", _listener_entry())
    probe = _FakeProbe([(None, "connection refused")])
    _patch_real_run(
        monkeypatch,
        server_info=_server_info(),
        probe=probe,
        execution_records=_RECORD_OK,
    )
    result = _run(bid)
    assert result["_success"] is False
    assert result["error_code"] == "LISTENER_PROBE_FAILED"
    assert "connection refused" in result["error"]


def test_basic_auth_without_token_fails_with_provisioning_hint(registry, monkeypatch):
    """Token *generation* is a one-time UI step; without it the probe cannot
    authenticate — fail with the provisioning guidance, never probe blind."""
    bid = registry("b-l-notoken", _listener_entry())
    probe = _FakeProbe([(200, None)])
    _patch_real_run(
        monkeypatch,
        server_info=_server_info(auth="basic", token=None, url="https://cloud.example"),
        probe=probe,
        execution_records=_RECORD_OK,
    )
    result = _run(bid)
    assert result["_success"] is False
    assert result["error_code"] == "LISTENER_PROBE_FAILED"
    assert "Shared Web Server panel" in result["error"]
    assert probe.calls == []


def test_missing_execution_record_fails(registry, monkeypatch):
    bid = registry("b-l-norec", _listener_entry())
    probe = _FakeProbe([(200, None)])
    _patch_real_run(
        monkeypatch,
        server_info=_server_info(),
        probe=probe,
        execution_records=_RECORD_EMPTY,
    )
    result = _run(bid)
    assert result["_success"] is False
    assert result["error_code"] == "LISTENER_EXECUTION_RECORD_MISSING"
    assert result["listener_verify"]["probe_status_code"] == 200
    assert result["listener_verify"]["execution_record_found"] is False


# ---------------------------------------------------------------------------
# Stage plumbing invariants
# ---------------------------------------------------------------------------


def test_stage_order_places_listener_verify_between_schedule_and_execution():
    order = orchestration._STAGE_ORDER
    assert order.index("listener_verify") == order.index("schedule") + 1
    assert order.index("listener_verify") == order.index("execution") - 1


def test_listener_error_codes_map_to_listener_verify_stage():
    for code in (
        orchestration.LISTENER_SERVER_INFO_FAILED,
        orchestration.LISTENER_APITYPE_UNSUPPORTED,
        orchestration.LISTENER_DEPLOYMENT_INACTIVE,
        orchestration.LISTENER_PATH_COLLISION,
        orchestration.LISTENER_PROBE_FAILED,
        orchestration.LISTENER_EXECUTION_RECORD_MISSING,
        orchestration.LISTENER_ASC_REQUIRED,
        orchestration.LISTENER_ASC_UNSUPPORTED_FOR_APITYPE,
        orchestration.LISTENER_ASC_DEPLOYMENT_INACTIVE,
        orchestration.LISTENER_ROUTE_PROCESS_DEPLOYMENT_INACTIVE,
        orchestration.LISTENER_ASC_ROUTE_INVALID,
        orchestration.LISTENER_ASC_COLLISION,
    ):
        assert orchestration._ERROR_CODE_STAGES[code] == "listener_verify"


def test_listener_status_is_never_consulted():
    """ListenerStatus stays EMPTY for WSS routes (live-proven on both runtimes,
    2026-07-04) — the verify stage must not reference it at all."""
    source = inspect.getsource(orchestration)
    assert "ListenerStatus" not in source.replace(
        "``ListenerStatus``", ""
    ).replace("ListenerStatus is deliberately NOT used", "")
    assert "manage_listeners" not in source


# ---------------------------------------------------------------------------
# Route-registration lag retry (fresh deploys only)
# ---------------------------------------------------------------------------


def _fresh_deploy_responses():
    """Deployment responses where the deploy stage CREATES (list empty -> deploy),
    so listener_verify sees a freshly created deployment (status='deployed')."""
    responses = _deployment_responses()
    responses["list_deployments"] = [
        {"_success": True, "deployments": []},  # deploy stage: nothing active
        {"_success": True, "deployments": []},  # collision scan
    ]
    responses["deploy"] = {
        "_success": True,
        "deployment": {"deployment_id": "dep-new", "active": True, "current_version": "1"},
    }
    return responses


def _patch_fresh_deploy_run(monkeypatch, *, probe, execution_records):
    dep = _FakeAction(_fresh_deploy_responses(), label="deployment")
    env = _FakeAction(
        {"get": {"_success": True, "environment": {"id": "env-1"}}}, label="environments"
    )
    rt = _FakeAction(
        {
            "get": {"_success": True, "runtime": {"id": "rt-1"}},
            "list_attachments": {
                "_success": True,
                "attachments": [{"id": "ea-1", "atom_id": "rt-1", "environment_id": "env-1"}],
            },
        },
        label="runtimes",
    )
    sch = _FakeAction({}, label="schedules")
    shared = _FakeAction({"get_server_info": _server_info()}, label="shared_resources")
    monitor = _FakeAction(
        {"execution_records": [_RECORD_EMPTY, execution_records]}, label="monitoring"
    )
    monkeypatch.setattr(orchestration, "manage_deployment_action", dep)
    monkeypatch.setattr(orchestration, "manage_environments_action", env)
    monkeypatch.setattr(orchestration, "manage_runtimes_action", rt)
    monkeypatch.setattr(orchestration, "manage_schedules_action", sch)
    monkeypatch.setattr(orchestration, "manage_shared_resources_action", shared)
    monkeypatch.setattr(orchestration, "monitor_platform_action", monitor)
    monkeypatch.setattr(orchestration, "_listener_probe", probe)
    monkeypatch.setattr(orchestration, "time", _FakeTime())
    monkeypatch.setattr(
        orchestration, "component_get_xml",
        lambda client, component_id, **kwargs: (_ for _ in ()).throw(
            Exception(f"unexpected component read: {component_id}")
        ),
    )
    return dep


def test_fresh_deploy_retries_no_route_then_succeeds(registry, monkeypatch):
    """A freshly created deployment registers its route asynchronously — a
    404/401 within the registration window is retried, then the green probe
    records the lag as a warning (live-observed 2026-07-04)."""
    bid = registry("b-l-lag", _listener_entry())
    # Attempt 1: primary 404 + verbatim fallback 404; attempt 2: primary 200.
    probe = _FakeProbe([(404, None), (404, None), (200, None)])
    _patch_fresh_deploy_run(monkeypatch, probe=probe, execution_records=_RECORD_OK)
    result = _run(bid)
    assert result["_success"] is True, result
    stage = result["listener_verify"]
    assert stage["status"] == "completed"
    assert stage["probe_status_code"] == 200
    assert stage["served_object_name_casing"] == "sentence_case"
    assert any("LISTENER_ROUTE_REGISTRATION_LAG" in w for w in stage["warnings"])
    assert len(probe.calls) == 3


def test_fresh_deploy_registration_window_bounded(registry, monkeypatch):
    """The retry window is bounded: persistent 404s still fail with the
    listener triage once the registration window elapses."""
    bid = registry("b-l-lag-timeout", _listener_entry())
    probe = _FakeProbe([(404, None)])  # last-response-repeats semantics
    _patch_fresh_deploy_run(monkeypatch, probe=probe, execution_records=_RECORD_OK)
    result = _run(bid)
    assert result["_success"] is False
    assert result["error_code"] == "LISTENER_PROBE_FAILED"
    assert "no route matches this path" in result["error"]
    # Multiple attempts were made before giving up (window / poll interval).
    assert len(probe.calls) > 2


def test_reused_deployment_never_retries(registry, monkeypatch):
    """A reused ACTIVE deployment serves immediately — a 404 fails after the
    single primary+fallback probe pair with no registration retry."""
    bid = registry("b-l-reused-404", _listener_entry())
    probe = _FakeProbe([(404, None), (404, None)])
    _patch_real_run(
        monkeypatch,
        server_info=_server_info(),
        probe=probe,
        execution_records=_RECORD_OK,
    )
    result = _run(bid)
    assert result["_success"] is False
    assert result["error_code"] == "LISTENER_PROBE_FAILED"
    # Exactly one primary + one verbatim-fallback probe, no retry loop.
    assert len(probe.calls) == 2


# ---------------------------------------------------------------------------
# Readback baseline (probe must trigger a NEW execution)
# ---------------------------------------------------------------------------


def _records(*pairs):
    return {
        "_success": True,
        "total_count": len(pairs),
        "execution_records": [
            {"execution_id": rid, "status": status, "execution_type": "exec_listener"}
            for rid, status in pairs
        ],
    }


def test_stale_pre_probe_record_does_not_verify(registry, monkeypatch):
    """Codex review (M6 #12): a record that already existed BEFORE the probe
    (recent listener traffic) must not count as proof the probe triggered an
    execution — the stage fails with LISTENER_EXECUTION_RECORD_MISSING."""
    bid = registry("b-l-stale", _listener_entry())
    probe = _FakeProbe([(200, None)])
    _patch_real_run(
        monkeypatch,
        server_info=_server_info(),
        probe=probe,
        baseline_records=_records(("exec-old", "COMPLETE")),
        execution_records=_records(("exec-old", "COMPLETE")),
    )
    result = _run(bid)
    assert result["_success"] is False
    assert result["error_code"] == "LISTENER_EXECUTION_RECORD_MISSING"
    assert result["listener_verify"]["execution_record_found"] is False


def test_new_record_among_stale_verifies(registry, monkeypatch):
    """A NEW execution id appearing alongside pre-probe traffic is the positive
    signal; the matched id is the new one, not the stale one."""
    bid = registry("b-l-new", _listener_entry())
    probe = _FakeProbe([(200, None)])
    _patch_real_run(
        monkeypatch,
        server_info=_server_info(),
        probe=probe,
        baseline_records=_records(("exec-old", "COMPLETE")),
        execution_records=_records(("exec-old", "COMPLETE"), ("exec-new", "COMPLETE")),
    )
    result = _run(bid)
    assert result["_success"] is True
    stage = result["listener_verify"]
    assert stage["status"] == "completed"
    assert stage["execution_id"] == "exec-new"


def test_baseline_query_failure_degrades_with_warning(registry, monkeypatch):
    """When the pre-probe baseline query itself fails, the readback degrades to
    accept-any-record but flags the weaker evidence explicitly."""
    bid = registry("b-l-nobase", _listener_entry())
    probe = _FakeProbe([(200, None)])
    _patch_real_run(
        monkeypatch,
        server_info=_server_info(),
        probe=probe,
        baseline_records={"_success": False, "error": "query exploded"},
        execution_records=_RECORD_OK,
    )
    result = _run(bid)
    assert result["_success"] is True
    stage = result["listener_verify"]
    assert stage["status"] == "completed"
    assert any("LISTENER_READBACK_BASELINE_UNAVAILABLE" in w for w in stage["warnings"])
    # Codex review (M6 #12): degraded readback evidence must NEVER report
    # behavioral verification — the record may predate the probe.
    assert stage["readback_baseline_available"] is False
    assert result["behavior_verified"] == {
        "verified": False,
        "reason": "listener_readback_degraded",
        "logs_status": result["logs"]["status"],
    }


def test_validation_rules_listener_on_non_listener_process_ignored(registry):
    """Architect review (M6 #12): a caller-supplied validation_rules.listener
    block on a spec whose deploy-target process has NO Listen source binding
    must not classify the build as a listener — Test mode stays available and
    no listener_verify probe is planned."""
    entry = _listener_entry()
    # Strip the listener binding: a plain scheduled process, metadata intact.
    entry["spec"]["components"][0]["config"] = {
        "process_kind": "database_to_api_sync",
        "source": {
            "connector_type": "database",
            "action_type": "Get",
            "connection_id": "DBCONN-1",
            "operation_id": "DBOP-1",
        },
    }
    assert entry["spec"]["validation_rules"]["listener"]["endpoint_path"]
    bid = registry("b-vr-nonlistener", entry)
    result = orchestrate_deploy_action(
        build_id=bid, environment_id="env-1", runtime_id="rt-1", dry_run=True, run_test=True
    )
    assert result["listener_verify"]["status"] == "not_required"
    assert result["execution"]["status"] == "planned"


def test_confirmed_binding_prefers_validation_rules_metadata(registry):
    """Once the process binding confirms a listener, the archetype-emitted
    metadata block (richer field set) is preferred over re-deriving from the
    operation component — even when the operation ref is an external literal."""
    bid = registry("b-vr-preferred", _listener_entry())
    result = orchestrate_deploy_action(
        build_id=bid, environment_id="env-1", runtime_id="rt-1", dry_run=True
    )
    stage = result["listener_verify"]
    assert stage["status"] == "planned"
    # Comes from _LISTENER_META (validation_rules), not an op component (the
    # entry's operation_id is a literal with no matching in-spec component).
    assert stage["endpoint_path"] == "/ws/simple/executeOrderIntake"


# ---------------------------------------------------------------------------
# API Service Component publish mode (M6.1 #133)
# ---------------------------------------------------------------------------

_ASC_LISTENER_META = {
    "object_name": "orderIntake",
    "operation_type": "EXECUTE",
    "input_type": "singlejson",
    "output_type": "none",
    "response_content_type": "text/plain",
    "publish_mode": "api_service",
    "api_type_requirement": "advanced",
    "http_method": "POST",
    "endpoint_path": "/ws/rest/orderIntake",
    "bare_wss_endpoint_path": "/ws/simple/executeOrderIntake",
    "api_service_component_key": "api_service",
    "route_process_key": "proc",
    "test_mode_supported": False,
}


def _asc_entry(*, process_id="CID-1", asc_id="ASC-1", extra_route=None, listener_meta=None):
    """A recorded ASC listener build: the listener process PLUS a webservice
    component whose route references it (the archetype asc_wrapper shape)."""
    entry = _listener_entry(
        process_id=process_id,
        listener_meta=listener_meta if listener_meta is not None else _ASC_LISTENER_META,
    )
    routes = [{"process": "$ref:proc", "http_method": "", "url_path": "",
               "object_name": "", "input_type": "", "output_type": ""}]
    if extra_route is not None:
        routes.append(extra_route)
    entry["spec"]["components"].append(
        {
            "key": "api_service",
            "type": "webservice",
            "action": "create",
            "name": "Order API Service",
            "component_id": None,
            "config": {
                "component_type": "webservice",
                "component_name": "Order API Service",
                "routes": routes,
            },
            "depends_on": ["proc"],
        }
    )
    entry["results"]["api_service"] = {
        "status": "created",
        "component_id": asc_id,
        "type": "webservice",
        "name": "Order API Service",
    }
    entry["execution_order"] = ["proc", "api_service"]
    return entry


def _asc_deployment_responses(
    *,
    asc_deployment_active=True,
    asc_fresh=False,
    collision_deployments=None,
    route_check_deployments=None,
):
    """Queued deployment responses for an ASC-mode real run.

    Call order: list_packages (process pkg) -> list_deployments (process
    deploy) -> list_packages (ASC pkg) -> list_deployments (ASC deploy)
    [-> list_deployments (route-process check, only for multi-route ASCs)]
    -> list_deployments (collision scan).

    ``asc_fresh=True`` models a first-time ASC rollout: no existing package
    (list empty -> create_package) and no existing deployment (list empty ->
    deploy, active) — the created/deployed statuses drive the registration
    retry window and the failure-cleanup plan (Codex review r1).
    """
    responses = _deployment_responses(collision_deployments=collision_deployments)
    asc_packages = (
        {"_success": True, "packages": []}
        if asc_fresh
        else {
            "_success": True,
            "packages": [
                {
                    "package_id": "pkg-asc",
                    "component_id": "ASC-1",
                    "component_type": "webservice",
                    "package_version": "v1",
                    "created_date": "2026-01-01T00:00:00Z",
                }
            ],
        }
    )
    responses["list_packages"] = [responses["list_packages"], asc_packages]
    if asc_fresh:
        responses["create_package"] = {
            "_success": True,
            "package": {"package_id": "pkg-asc"},
        }
    process_deploy, collision = responses["list_deployments"]
    if asc_fresh:
        asc_deploy = {"_success": True, "deployments": []}
        responses["deploy"] = {
            "_success": True,
            "deployment": {
                "deployment_id": "dep-asc",
                "active": bool(asc_deployment_active),
                "current_version": "1",
            },
        }
    elif asc_deployment_active:
        asc_deploy = {
            "_success": True,
            "deployments": [
                {"deployment_id": "dep-asc", "active": True, "current_version": "1"}
            ],
        }
    else:
        # No active ASC deployment listed -> _find_or_create_deployment
        # CREATES one; the platform answers with an inactive deployment
        # (e.g. license ceiling), which the stage must reject.
        asc_deploy = {"_success": True, "deployments": []}
        responses["deploy"] = {
            "_success": True,
            "deployment": {
                "deployment_id": "dep-asc",
                "active": False,
                "current_version": "1",
            },
        }
    queued = [process_deploy, asc_deploy]
    if route_check_deployments is not None:
        queued.append({"_success": True, "deployments": list(route_check_deployments)})
    queued.append(collision)
    responses["list_deployments"] = queued
    return responses


def _patch_asc_real_run(
    monkeypatch,
    *,
    server_info,
    probe,
    execution_records,
    deployment_responses=None,
    component_xml=None,
):
    """ASC-mode variant of _patch_real_run (queued ASC package/deploy legs)."""
    dep = _FakeAction(
        deployment_responses or _asc_deployment_responses(), label="deployment"
    )
    env = _FakeAction(
        {"get": {"_success": True, "environment": {"id": "env-1"}}}, label="environments"
    )
    rt = _FakeAction(
        {
            "get": {"_success": True, "runtime": {"id": "rt-1"}},
            "list_attachments": {
                "_success": True,
                "attachments": [{"id": "ea-1", "atom_id": "rt-1", "environment_id": "env-1"}],
            },
        },
        label="runtimes",
    )
    sch = _FakeAction({}, label="schedules")
    shared = _FakeAction({"get_server_info": server_info}, label="shared_resources")
    monitor = _FakeAction(
        {"execution_records": [_RECORD_EMPTY, execution_records]},
        label="monitoring",
    )
    monkeypatch.setattr(orchestration, "manage_deployment_action", dep)
    monkeypatch.setattr(orchestration, "manage_environments_action", env)
    monkeypatch.setattr(orchestration, "manage_runtimes_action", rt)
    monkeypatch.setattr(orchestration, "manage_schedules_action", sch)
    monkeypatch.setattr(orchestration, "manage_shared_resources_action", shared)
    monkeypatch.setattr(orchestration, "monitor_platform_action", monitor)
    monkeypatch.setattr(orchestration, "_listener_probe", probe)
    monkeypatch.setattr(orchestration, "time", _FakeTime())

    def _fake_component_get_xml(client, component_id, **kwargs):
        xml_map = component_xml or {}
        if component_id not in xml_map:
            raise Exception(f"unexpected component read: {component_id}")
        # Mirror the real helper's envelope: 'type' parsed off the root
        # attrs (the ASC route shape-check reads it).
        import xml.etree.ElementTree as _ET

        raw = xml_map[component_id]
        root = _ET.fromstring(raw)
        return {
            "xml": raw,
            "type": root.attrib.get("type", ""),
            "name": root.attrib.get("name", ""),
        }

    monkeypatch.setattr(orchestration, "component_get_xml", _fake_component_get_xml)

    def _explode(*args, **kwargs):
        raise AssertionError("Test-mode execution must never run for a listener build")

    monkeypatch.setattr(orchestration, "execute_process_action", _explode)
    return {"deployment": dep, "shared": shared, "monitor": monitor, "probe": probe}


def test_dry_run_asc_build_plans_publish_mode(registry):
    bid = registry("b-asc-plan", _asc_entry())
    result = orchestrate_deploy_action(
        build_id=bid, environment_id="env-1", runtime_id="rt-1", dry_run=True
    )
    assert result["_success"] is True
    stage = result["listener_verify"]
    assert stage["status"] == "planned"
    assert stage["publish_mode"] == "api_service"
    assert stage["endpoint_path"] == "/ws/rest/orderIntake"
    assert stage["api_service_component_id"] == "ASC-1"
    assert stage["route_process_ids"] == ["CID-1"]


def test_advanced_asc_happy_path_deploys_both_and_probes_ws_rest(registry, monkeypatch):
    bid = registry("b-asc-ok", _asc_entry())
    probe = _FakeProbe([(200, None)])
    fakes = _patch_asc_real_run(
        monkeypatch,
        server_info=_server_info(api_type="advanced", auth="basic", token="tok"),
        probe=probe,
        execution_records=_RECORD_OK,
    )
    result = _run(bid)
    assert result["_success"] is True, result.get("error")
    stage = result["listener_verify"]
    assert stage["status"] == "completed"
    assert stage["publish_mode"] == "api_service"
    assert stage["api_service_component_id"] == "ASC-1"
    assert stage["api_service_package_id"] == "pkg-asc"
    assert stage["api_service_deployment_id"] == "dep-asc"
    assert stage["api_service_deployment_active"] is True
    assert stage["route_deployments_active"] is True
    assert stage["route_process_ids"] == ["CID-1"]
    # The ASC package leg actually ran (two list_packages calls: process + ASC).
    assert fakes["deployment"].actions_called().count("list_packages") == 2
    # The probe hits the /ws/rest route with Basic auth, case-verbatim.
    assert probe.calls[0]["url"].endswith("/ws/rest/orderIntake")
    assert probe.calls[0]["method"] == "POST"
    assert "Authorization" in probe.calls[0]["headers"]
    # /ws/rest is verbatim — the 2xx probe records verbatim casing.
    assert stage["served_object_name_casing"] == "verbatim"
    # Behavior verified via strict readback baseline.
    assert result["behavior_verified"]["verified"] is True


def test_intermediate_plus_asc_fails_unsupported_apitype(registry, monkeypatch):
    bid = registry("b-asc-tier", _asc_entry())
    probe = _FakeProbe([(200, None)])
    fakes = _patch_asc_real_run(
        monkeypatch,
        server_info=_server_info(api_type="intermediate"),
        probe=probe,
        execution_records=_RECORD_OK,
    )
    result = _run(bid)
    assert result["_success"] is False
    assert result["error_code"] == "LISTENER_ASC_UNSUPPORTED_FOR_APITYPE"
    assert result["failed_stage"] == "listener_verify"
    # Fails BEFORE deploying the ASC (no second list_packages) and any probe.
    assert fakes["deployment"].actions_called().count("list_packages") == 1
    assert probe.calls == []


def test_asc_deployment_inactive_fails(registry, monkeypatch):
    bid = registry("b-asc-inactive", _asc_entry())
    probe = _FakeProbe([(200, None)])
    _patch_asc_real_run(
        monkeypatch,
        server_info=_server_info(api_type="advanced"),
        probe=probe,
        execution_records=_RECORD_OK,
        deployment_responses=_asc_deployment_responses(asc_deployment_active=False),
    )
    result = _run(bid)
    assert result["_success"] is False
    assert result["error_code"] == "LISTENER_ASC_DEPLOYMENT_INACTIVE"
    assert result["listener_verify"]["api_service_deployment_active"] is False
    assert probe.calls == []


_EXTRA_ROUTE_LISTENER_XML = (
    '<bns:Component xmlns:bns="http://api.platform.boomi.com/" type="process" '
    'componentId="CID-2" name="Second Listener"><bns:object>'
    '<process xmlns=""><shapes><shape shapetype="start"><configuration>'
    '<connectoraction actionType="Listen" connectorType="wss" operationId="OP-2"/>'
    "</configuration></shape></shapes></process></bns:object></bns:Component>"
)


def test_missing_route_process_deployment_fails(registry, monkeypatch):
    # A second (literal-id) route process that IS a valid listener but is NOT
    # active in the environment: ASC deploy does not cascade — the check must
    # fail after the shape validation passes.
    bid = registry(
        "b-asc-route",
        _asc_entry(extra_route={"process": "CID-2", "http_method": "GET",
                               "object_name": "statusPing", "url_path": "",
                               "input_type": "none", "output_type": ""}),
    )
    probe = _FakeProbe([(200, None)])
    _patch_asc_real_run(
        monkeypatch,
        server_info=_server_info(api_type="advanced"),
        probe=probe,
        execution_records=_RECORD_OK,
        deployment_responses=_asc_deployment_responses(
            route_check_deployments=[
                {"deployment_id": "dep-1", "component_id": "CID-1",
                 "component_type": "process", "active": True}
            ]
        ),
        component_xml={"CID-2": _EXTRA_ROUTE_LISTENER_XML},
    )
    result = _run(bid)
    assert result["_success"] is False
    assert result["error_code"] == "LISTENER_ROUTE_PROCESS_DEPLOYMENT_INACTIVE"
    assert "CID-2" in result["error"]
    assert probe.calls == []


def test_literal_route_to_non_listener_fails_before_asc_deploy(registry, monkeypatch):
    # Architect review r1: literal-id extra routes bypass plan-time $ref
    # validation — the verify stage must shape-check them (process + WSS
    # Listen start) BEFORE deploying the ASC.
    non_listener_xml = (
        '<bns:Component xmlns:bns="http://api.platform.boomi.com/" type="process" '
        'componentId="CID-2" name="Plain"><bns:object>'
        '<process xmlns=""><shapes><shape shapetype="start"><configuration>'
        '<connectoraction actionType="Get" connectorType="database" operationId="x"/>'
        "</configuration></shape></shapes></process></bns:object></bns:Component>"
    )
    bid = registry(
        "b-asc-badroute",
        _asc_entry(extra_route={"process": "CID-2", "http_method": "GET",
                               "object_name": "statusPing", "url_path": "",
                               "input_type": "none", "output_type": ""}),
    )
    probe = _FakeProbe([(200, None)])
    fakes = _patch_asc_real_run(
        monkeypatch,
        server_info=_server_info(api_type="advanced"),
        probe=probe,
        execution_records=_RECORD_OK,
        component_xml={"CID-2": non_listener_xml},
    )
    result = _run(bid)
    assert result["_success"] is False
    assert result["error_code"] == "LISTENER_ASC_ROUTE_INVALID"
    assert "WSS Listen start" in result["error"]
    # Fails BEFORE the ASC package leg (one list_packages call: the process)
    # and before any probe.
    assert fakes["deployment"].actions_called().count("list_packages") == 1
    assert probe.calls == []
    # An unreadable literal route fails the same way.
    bid2 = registry(
        "b-asc-unreadroute",
        _asc_entry(extra_route={"process": "CID-3", "http_method": "GET",
                               "object_name": "statusPing2", "url_path": "",
                               "input_type": "none", "output_type": ""}),
    )
    probe2 = _FakeProbe([(200, None)])
    _patch_asc_real_run(
        monkeypatch,
        server_info=_server_info(api_type="advanced"),
        probe=probe2,
        execution_records=_RECORD_OK,
    )
    result2 = _run(bid2)
    assert result2["_success"] is False
    assert result2["error_code"] == "LISTENER_ASC_ROUTE_INVALID"
    assert "could not be read" in result2["error"]


def test_unknown_api_type_fails_closed_in_both_modes(registry, monkeypatch):
    # Architect review r1: an absent/unknown apiType cannot select the
    # publish pattern — fail preflight instead of probing on a guess.
    for entry, label in ((_listener_entry(), "bare"), (_asc_entry(), "asc")):
        bid = registry(f"b-l-unktier-{label}", entry)
        probe = _FakeProbe([(200, None)])
        fakes = _patch_asc_real_run(
            monkeypatch,
            server_info=_server_info(api_type=""),
            probe=probe,
            execution_records=_RECORD_OK,
        )
        result = _run(bid)
        assert result["_success"] is False, label
        assert result["error_code"] == "LISTENER_APITYPE_UNSUPPORTED", label
        assert "unknown or absent apiType" in result["error"], label
        assert probe.calls == [], label
        # Fails before the ASC package leg too.
        assert fakes["deployment"].actions_called().count("list_packages") == 1, label


def _other_asc_xml(*, base="", object_name="somethingElse", http_method="POST"):
    return (
        '<bns:Component xmlns:bns="http://api.platform.boomi.com/" type="webservice" name="Other ASC">'
        "<bns:object>"
        f'<webservice xmlns="" urlPath="{base}">'
        "<restApi>"
        '<route processId="00000000-0000-0000-0000-0000000000aa">'
        f'<overrides httpMethod="{http_method}" inputProfileKey="" inputType="singlejson" '
        f'objectName="{object_name}" outputType="" urlPath=""/>'
        "<description/></route>"
        "</restApi>"
        '<soapApi fullEnvelopePassthrough="false" singleWsdlSchema="false" '
        'suppressWrappers="false" wsdlNamespace="" wsdlServiceName="">'
        "<SOAPVersion>SOAP_1_1</SOAPVersion></soapApi><odataApi/>"
        '<metaInfo title="t" version="1.0.0"><description/><termsOfService/></metaInfo>'
        "<profileOverrides/><capturedHeaders/><apiRoles/>"
        "</webservice></bns:object></bns:Component>"
    )


def test_asc_same_base_collision_flagged_even_with_unique_route_paths(registry, monkeypatch):
    # #133 QA bug #147 regression lock: shadowing granularity is the ASC's
    # BASE urlPath (live-proven A/B/A 2026-07-05) — a deployed ASC sharing
    # our base ("" here, the default) collides even though every route path
    # is unique account-wide.
    bid = registry("b-asc-coll", _asc_entry())
    probe = _FakeProbe([(200, None)])
    _patch_asc_real_run(
        monkeypatch,
        server_info=_server_info(api_type="advanced"),
        probe=probe,
        execution_records=_RECORD_OK,
        deployment_responses=_asc_deployment_responses(
            collision_deployments=[
                {"deployment_id": "dep-x", "component_id": "OTHER-ASC",
                 "component_type": "webservice", "active": True}
            ]
        ),
        component_xml={"OTHER-ASC": _other_asc_xml(base="", object_name="uniquePath")},
    )
    result = _run(bid)
    assert result["_success"] is False
    assert result["error_code"] == "LISTENER_ASC_COLLISION"
    assert "BASE urlPath" in result["error"]
    assert result["listener_verify"]["collision_count"] == 1
    assert result["listener_verify"]["collision_paths"] == [
        "BASE /ws/rest/ (component OTHER-ASC)"
    ]
    # Collisions block BEFORE the probe (pre-probes are uninformative).
    assert probe.calls == []


def test_asc_distinct_base_no_collision_and_verify_green(registry, monkeypatch):
    # A deployed ASC on a DIFFERENT base does not shadow ours — the scan
    # reads its XML, finds neither a base nor an effective-path match, and
    # the verify proceeds green.
    bid = registry("b-asc-nocoll", _asc_entry())
    probe = _FakeProbe([(200, None)])
    _patch_asc_real_run(
        monkeypatch,
        server_info=_server_info(api_type="advanced"),
        probe=probe,
        execution_records=_RECORD_OK,
        deployment_responses=_asc_deployment_responses(
            collision_deployments=[
                {"deployment_id": "dep-x", "component_id": "OTHER-ASC",
                 "component_type": "webservice", "active": True}
            ]
        ),
        component_xml={
            "OTHER-ASC": _other_asc_xml(base="otherBase", object_name="orderIntake")
        },
    )
    result = _run(bid)
    assert result["_success"] is True, result.get("error")
    assert result["listener_verify"]["collision_count"] == 0
    assert result["listener_verify"]["collision_paths"] == []
    assert len(probe.calls) == 1


def test_asc_probe_404_has_no_ws_simple_casing_fallback(registry, monkeypatch):
    # Bare mode retries a 404 on the verbatim /ws/simple casing; /ws/rest is
    # already verbatim — ASC mode must fail without a second probe.
    bid = registry("b-asc-404", _asc_entry())
    probe = _FakeProbe([(404, None)])
    _patch_asc_real_run(
        monkeypatch,
        server_info=_server_info(api_type="advanced"),
        probe=probe,
        execution_records=_RECORD_OK,
    )
    result = _run(bid)
    assert result["_success"] is False
    assert result["error_code"] == "LISTENER_PROBE_FAILED"
    assert len(probe.calls) == 1
    assert probe.calls[0]["url"].endswith("/ws/rest/orderIntake")
    # ASC triage names the deploy-both rule.
    assert "does not cascade" in result["error"]


def test_asc_http_200_with_error_execution_not_behavior_verified(registry, monkeypatch):
    bid = registry("b-asc-err", _asc_entry())
    probe = _FakeProbe([(200, None)])
    _patch_asc_real_run(
        monkeypatch,
        server_info=_server_info(api_type="advanced"),
        probe=probe,
        execution_records={
            "_success": True,
            "total_count": 1,
            "execution_records": [
                {"execution_id": "exec-err", "status": "ERROR",
                 "execution_type": "exec_listener"}
            ],
        },
    )
    result = _run(bid)
    assert result["_success"] is True
    stage = result["listener_verify"]
    assert stage["execution_status"] == "ERROR"
    assert any("LISTENER_EXECUTION_ERROR" in w for w in stage["warnings"])
    assert result["behavior_verified"]["verified"] is False


def test_asc_metadata_without_component_id_fails_route_invalid(registry, monkeypatch):
    # A hand-authored spec whose metadata claims api_service but records no
    # webservice component: the stage must fail structured, never probe.
    entry = _listener_entry(listener_meta=_ASC_LISTENER_META)
    bid = registry("b-asc-noid", entry)
    probe = _FakeProbe([(200, None)])
    _patch_asc_real_run(
        monkeypatch,
        server_info=_server_info(api_type="advanced"),
        probe=probe,
        execution_records=_RECORD_OK,
    )
    result = _run(bid)
    assert result["_success"] is False
    assert result["error_code"] == "LISTENER_ASC_ROUTE_INVALID"
    assert probe.calls == []


def test_ensure_api_service_deployment_packages_as_webservice(monkeypatch):
    """The ASC package leg must create the PackagedComponent with
    component_type='webservice' (packages.py passes it through verbatim)."""
    calls = []

    def fake_action(sdk=None, profile=None, action=None, config_data=None, **kwargs):
        calls.append({"action": action, "config_data": config_data})
        if action == "list_packages":
            return {"_success": True, "packages": []}
        if action == "create_package":
            return {"_success": True, "package": {"package_id": "pkg-new"}}
        if action == "list_deployments":
            return {"_success": True, "deployments": []}
        if action == "deploy":
            return {
                "_success": True,
                "deployment": {"deployment_id": "dep-new", "active": True},
            }
        raise AssertionError(f"unexpected action: {action}")

    monkeypatch.setattr(orchestration, "manage_deployment_action", fake_action)
    package, deployment, error = orchestration._ensure_api_service_deployment(
        MagicMock(),
        "prof",
        asc_component_id="ASC-9",
        environment_id="env-1",
        package_version="v9",
    )
    assert error is None
    assert package.component_type == "webservice"
    assert package.package_id == "pkg-new"
    assert deployment.active is True
    create_call = next(c for c in calls if c["action"] == "create_package")
    assert create_call["config_data"]["component_type"] == "webservice"
    assert create_call["config_data"]["component_id"] == "ASC-9"
    assert create_call["config_data"]["package_version"] == "v9"


def test_fresh_asc_deploy_opens_registration_retry_window(registry, monkeypatch):
    # Codex review r1: the /ws/rest route registration follows the ASC
    # deployment. With the PROCESS deployment reused but the ASC freshly
    # deployed, a transient no-route 401 must be retried within the bounded
    # window rather than failing on the first probe.
    bid = registry("b-asc-lag", _asc_entry())
    probe = _FakeProbe([(401, None), (200, None)])
    _patch_asc_real_run(
        monkeypatch,
        server_info=_server_info(api_type="advanced"),
        probe=probe,
        execution_records=_RECORD_OK,
        deployment_responses=_asc_deployment_responses(asc_fresh=True),
    )
    result = _run(bid)
    assert result["_success"] is True, result.get("error")
    stage = result["listener_verify"]
    assert stage["api_service_package_status"] == "created"
    assert stage["api_service_deployment_status"] == "deployed"
    assert len(probe.calls) == 2
    assert any("LISTENER_ROUTE_REGISTRATION_LAG" in w for w in stage["warnings"])


def test_asc_failure_cleanup_includes_fresh_asc_resources(registry, monkeypatch):
    # Codex review r1: a listener_verify failure after THIS attempt created
    # and deployed the ASC must name the ASC undeploy + package delete in the
    # cleanup plan (FIRST — a leftover active ASC base-shadows the retry).
    bid = registry("b-asc-clean", _asc_entry())
    probe = _FakeProbe([(404, None)])
    _patch_asc_real_run(
        monkeypatch,
        server_info=_server_info(api_type="advanced"),
        probe=probe,
        execution_records=_RECORD_OK,
        deployment_responses=_asc_deployment_responses(asc_fresh=True),
    )
    result = _run(bid)
    assert result["_success"] is False
    assert result["error_code"] == "LISTENER_PROBE_FAILED"
    cleanup = result["cleanup"]
    assert cleanup["status"] == "planned"
    ops = cleanup["operations"]
    assert ops[0]["action"] == "undeploy"
    assert ops[0]["resource_id"] == "dep-asc"
    assert ops[0]["config"]["package_id"] == "pkg-asc"
    assert ops[1]["action"] == "delete_package"
    assert ops[1]["resource_id"] == "pkg-asc"
    # A REUSED ASC deployment/package must NOT be listed for cleanup.
    bid2 = registry("b-asc-clean2", _asc_entry())
    probe2 = _FakeProbe([(404, None)])
    _patch_asc_real_run(
        monkeypatch,
        server_info=_server_info(api_type="advanced"),
        probe=probe2,
        execution_records=_RECORD_OK,
    )
    result2 = _run(bid2)
    assert result2["_success"] is False
    asc_ops = [
        op
        for op in (result2["cleanup"].get("operations") or [])
        if op["resource_id"] in ("dep-asc", "pkg-asc")
    ]
    assert asc_ops == []

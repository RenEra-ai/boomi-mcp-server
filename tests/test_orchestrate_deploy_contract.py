"""Contract tests for ``orchestrate_deploy_action`` (issue #60 plan-only + issue #61 deploy).

Issue #60 made the action dry-run/plan-only; issue #61 adds idempotent package + deploy
stages that run when ``dry_run=False``. The real-run tests monkeypatch
``orchestration.manage_deployment_action`` with a fake that returns the same dict shapes the
real router produces, so they exercise orchestration's dict-inspection contract directly
without a live SDK.

The build registry and the action under test are imported through a single, consistent
``src.boomi_mcp...`` prefix. Under this repo's dual-namespace layout (``src.boomi_mcp.*`` vs
``boomi_mcp.*`` are distinct module objects with distinct ``_BUILD_REGISTRY`` dicts), mixing
prefixes would make the resolver read a *different* registry than the one these tests seed.
"""

import copy
import sys
from pathlib import Path
from unittest.mock import MagicMock

import pytest

_project_root = str(Path(__file__).resolve().parent.parent)
if _project_root not in sys.path:
    sys.path.insert(0, _project_root)
# Internal package modules use absolute ``from boomi_mcp...`` imports, so bare ``boomi_mcp``
# must be importable for this test to run standalone (the wider suite gets this for free via
# ``import server``). Adding ``src`` mirrors server.py's own path setup. We still import via the
# ``src.boomi_mcp...`` prefix below so the registry shares one namespace with the action.
_src_path = str(Path(__file__).resolve().parent.parent / "src")
if _src_path not in sys.path:
    sys.path.insert(0, _src_path)

from src.boomi_mcp.categories.integration_builder import _BUILD_REGISTRY  # noqa: E402
from src.boomi_mcp.categories.deployment import orchestrate_deploy_action  # noqa: E402
from src.boomi_mcp.categories.deployment import orchestration  # noqa: E402


# ---------------------------------------------------------------------------
# Fixtures & helpers
# ---------------------------------------------------------------------------
@pytest.fixture
def registry():
    """Seed registry entries and clean up only the ids inserted by the test."""
    inserted = []

    def seed(build_id, entry):
        _BUILD_REGISTRY[build_id] = entry
        inserted.append(build_id)
        return build_id

    yield seed

    for build_id in inserted:
        _BUILD_REGISTRY.pop(build_id, None)


def _component(key, ctype, *, name=None, action="create", component_id=None):
    return {
        "key": key,
        "type": ctype,
        "action": action,
        "name": name if name is not None else key,
        "component_id": component_id,
        "config": {},
        "depends_on": [],
    }


def _result(*, status, component_id, ctype, name):
    return {"status": status, "component_id": component_id, "type": ctype, "name": name}


def _entry(*, name="MyIntegration", components, results, profile="prof", execution_order=None):
    return {
        "created_at": "2026-01-01T00:00:00+00:00",
        "profile": profile,
        "spec": {
            "version": "1.0",
            "name": name,
            "mode": "lift_shift",
            "components": components,
            "goals": [],
            "endpoints": [],
            "flows": [],
            "naming": {},
            "folders": {},
            "runtime": {},
            "validation_rules": {},
        },
        "results": results,
        "execution_order": execution_order
        if execution_order is not None
        else [c["key"] for c in components if isinstance(c, dict) and "key" in c],
    }


def _single_process_entry(*, process_id="CID-1", process_status="created"):
    """A valid build: one process + one connector."""
    components = [
        _component("conn", "connector-settings", name="Conn"),
        _component("proc", "process", name="My Process"),
    ]
    results = {
        "conn": _result(status="created", component_id="CONN-1", ctype="connector-settings", name="Conn"),
        "proc": _result(status=process_status, component_id=process_id, ctype="process", name="My Process"),
    }
    return _entry(components=components, results=results)


class _ExplodingClient:
    """A Boomi client stand-in that fails loudly on ANY attribute access."""

    def __getattr__(self, name):
        raise AssertionError(f"SDK access attempted in plan-only mode: {name}")


def _codes(result):
    return [e["code"] for e in result["errors"]]


# The 19 keys every full response envelope carries (mirrors test_full_success_contract). A full
# ERROR envelope is these plus "error" (#129 D2 — early/pre-stage failures now return this shape).
_FULL_ENVELOPE_KEYS = {
    "_success", "profile", "build_id", "dry_run", "plan_only", "behavior_verified",
    "integration_name", "target", "component_summary", "package", "deployment",
    "runtime_attachment", "schedule", "execution", "logs", "cleanup", "summary",
    "warnings", "errors",
}


def _assert_full_error_envelope(result):
    """A pre-stage/early failure must carry the SAME full envelope shape as a late-stage failure
    (#129 D2): every stage key present (as a blocked placeholder) so a caller branching on any
    stage key never silently breaks on an early error."""
    assert result["_success"] is False
    assert set(result) >= _FULL_ENVELOPE_KEYS | {"error"}
    # Each stage placeholder is present and blocked.
    for stage in ("package", "deployment", "runtime_attachment", "schedule", "execution",
                  "logs", "cleanup"):
        assert result[stage]["status"] == "blocked", stage
    assert isinstance(result["summary"], dict)
    # summary matches the blocked-downstream failure convention: the 4 core stages appear in
    # stage_statuses, and NO ``test`` sub-summary is present — that only appears when a run-test
    # stage actually ran (#129 D2 review r3; same contract as test_run_test_false_*).
    stage_statuses = result["summary"]["stage_statuses"]
    for stage in ("package", "deployment", "runtime_attachment", "schedule"):
        assert stage_statuses.get(stage) == "blocked", stage
    assert "test" not in result["summary"]
    assert isinstance(result["behavior_verified"], dict)
    assert result["behavior_verified"]["verified"] is False
    assert result["warnings"] == []


# ---------------------------------------------------------------------------
# Real-run (issue #61) fakes & helpers
# ---------------------------------------------------------------------------
class _FakeAction:
    """Records ``(action, config_data)`` calls and returns canned dict responses per action.

    Patched in for any of the ``orchestration.manage_*_action`` routers so the real-run tests
    exercise orchestration's dict-inspection contract (it inspects ``_success``/payload, never
    catches exceptions) without a live SDK. Mirrors the real routers' return shapes. An optional
    shared ``order_log`` records ``(label, action)`` across fakes to assert cross-router ordering.
    """

    def __init__(self, responses, *, label=None, order_log=None):
        self.responses = responses
        self.label = label
        self.order_log = order_log
        self.calls = []

    def __call__(self, sdk=None, profile=None, action=None, config_data=None, **kwargs):
        self.calls.append({"action": action, "config_data": config_data})
        if self.order_log is not None:
            self.order_log.append((self.label, action))
        if action not in self.responses:
            raise AssertionError(f"unexpected {self.label or 'deployment'} action call: {action}")
        return self.responses[action]

    def actions_called(self):
        return [c["action"] for c in self.calls]


# Backwards-compatible alias used by the issue #61 deploy tests.
_FakeDeploymentAction = _FakeAction


class _SeqFakeAction(_FakeAction):
    """Like ``_FakeAction`` but a per-action response may be a LIST returned in sequence (the last
    entry repeats once the queue is down to one), so a single action can return different responses
    across repeated calls — e.g. ``list_packages`` empty on the first call, then the recovered
    package on the conflict re-list (#129 D4).
    """

    def __call__(self, sdk=None, profile=None, action=None, config_data=None, **kwargs):
        self.calls.append({"action": action, "config_data": config_data})
        if self.order_log is not None:
            self.order_log.append((self.label, action))
        if action not in self.responses:
            raise AssertionError(f"unexpected {self.label or 'deployment'} action call: {action}")
        resp = self.responses[action]
        if isinstance(resp, list):
            if not resp:
                raise AssertionError(f"no queued response left for action: {action}")
            return resp.pop(0) if len(resp) > 1 else resp[0]
        return resp


def _patch_deploy_seq(monkeypatch, deployment, *, runtime_id="rt-1", environment_id="env-1"):
    """Patch routers for a real run whose DEPLOYMENT router is sequence-aware (#129 D4).

    Environment/runtime/schedule routers stay static; returns the deployment fake for assertions.
    """
    dep = _SeqFakeAction(deployment, label="deployment")
    env = _FakeAction(_ok_env(environment_id), label="environments")
    rt = _FakeAction(_ok_runtime(runtime_id, environment_id, attached=True), label="runtimes")
    sch = _FakeAction({}, label="schedules")
    monkeypatch.setattr(orchestration, "manage_deployment_action", dep)
    monkeypatch.setattr(orchestration, "manage_environments_action", env)
    monkeypatch.setattr(orchestration, "manage_runtimes_action", rt)
    monkeypatch.setattr(orchestration, "manage_schedules_action", sch)
    return dep


def _att(att_id, *, atom_id=None, environment_id=None, process_id=None):
    """An attachment dict as the runtime/deployment routers' ``_attachment_to_dict`` returns it."""
    out = {"id": att_id}
    if atom_id is not None:
        out["atom_id"] = atom_id
    if environment_id is not None:
        out["environment_id"] = environment_id
    if process_id is not None:
        out["process_id"] = process_id
    return out


def _sched(schedule_id="sch-1"):
    """A process-schedule dict shaped like schedules ``_process_schedule_to_dict``."""
    return {"id": schedule_id}


def _status(status_id="sst-1", enabled=True):
    """A schedule-status dict shaped like schedules ``_schedule_status_to_dict``."""
    return {"id": status_id, "enabled": enabled}


def _ok_env(environment_id="env-1"):
    return {"get": {"_success": True, "environment": {"id": environment_id}}}


def _ok_runtime(runtime_id="rt-1", environment_id="env-1", *, attached=True):
    attachments = (
        [_att("ea-1", atom_id=runtime_id, environment_id=environment_id)] if attached else []
    )
    return {
        "get": {"_success": True, "runtime": {"id": runtime_id}},
        "list_attachments": {"_success": True, "attachments": attachments},
        "attach": {
            "_success": True,
            "attachment": _att("ea-new", atom_id=runtime_id, environment_id=environment_id),
        },
    }


def _process_attachments(
    runtime_id="rt-1", environment_id="env-1", process_id="CID-1", *,
    env_attached=True, atom_attached=True,
):
    """The four process-attachment responses ``manage_deployment_action`` serves for binding."""
    pe = [_att("pe-1", process_id=process_id, environment_id=environment_id)] if env_attached else []
    pa = [_att("pa-1", process_id=process_id, atom_id=runtime_id)] if atom_attached else []
    return {
        "list_process_environment_attachments": {"_success": True, "attachments": pe},
        "attach_process_environment": {
            "_success": True,
            "attachment": _att("pe-new", process_id=process_id, environment_id=environment_id),
        },
        "list_process_atom_attachments": {"_success": True, "attachments": pa},
        "attach_process_atom": {
            "_success": True,
            "attachment": _att("pa-new", process_id=process_id, atom_id=runtime_id),
        },
    }


def _patch_all(monkeypatch, *, deployment, environments, runtimes, schedules, order_log=None):
    """Patch all four ``manage_*_action`` routers and return their fakes (deployment first)."""
    dep = _FakeAction(deployment, label="deployment", order_log=order_log)
    env = _FakeAction(environments, label="environments", order_log=order_log)
    rt = _FakeAction(runtimes, label="runtimes", order_log=order_log)
    sch = _FakeAction(schedules, label="schedules", order_log=order_log)
    monkeypatch.setattr(orchestration, "manage_deployment_action", dep)
    monkeypatch.setattr(orchestration, "manage_environments_action", env)
    monkeypatch.setattr(orchestration, "manage_runtimes_action", rt)
    monkeypatch.setattr(orchestration, "manage_schedules_action", sch)
    return dep, env, rt, sch


def _bind_success(monkeypatch, pkg_deploy_responses, *,
                  runtime_id="rt-1", environment_id="env-1", process_id="CID-1"):
    """Patch all routers for a fully-successful real run: pre-existing bindings, no schedule.

    Returns the deployment fake so callers keep asserting on package/deploy actions.
    """
    deployment = {
        **pkg_deploy_responses,
        **_process_attachments(runtime_id, environment_id, process_id),
    }
    dep, _env, _rt, _sch = _patch_all(
        monkeypatch,
        deployment=deployment,
        environments=_ok_env(environment_id),
        runtimes=_ok_runtime(runtime_id, environment_id, attached=True),
        schedules={},
    )
    return dep


def _pkg(package_id, version, created_date="2026-01-01T00:00:00Z"):
    return {
        "package_id": package_id,
        "component_id": "CID-1",
        "component_type": "process",
        "package_version": version,
        "created_date": created_date,
    }


def _dep(deployment_id, active, current_version=None, version=None):
    dep = {"deployment_id": deployment_id, "active": active}
    if current_version is not None:
        dep["current_version"] = current_version
    if version is not None:
        dep["version"] = version
    return dep


def _deploy_ok(bid):
    """Package/deploy responses for a successful real run (reuse existing package + active deploy)."""
    return {
        "list_packages": {"_success": True, "packages": [_pkg("pkg-1", bid)]},
        "list_deployments": {
            "_success": True,
            "deployments": [_dep("dep-1", True, current_version="1")],
        },
    }


# ---------------------------------------------------------------------------
# Required-field validation
# ---------------------------------------------------------------------------
def test_missing_build_id():
    result = orchestrate_deploy_action(
        boomi_client=MagicMock(), profile="p", build_id=None,
        environment_id="env-1", runtime_id="rt-1",
    )
    assert result["_success"] is False
    assert "BUILD_ID_REQUIRED" in _codes(result)
    assert result["errors"][0]["field"] == "build_id"


def test_blank_build_id_whitespace():
    result = orchestrate_deploy_action(
        build_id="   ", environment_id="env-1", runtime_id="rt-1",
    )
    assert result["_success"] is False
    assert _codes(result) == ["BUILD_ID_REQUIRED"]


def test_missing_environment_id(registry):
    bid = registry("b-env", _single_process_entry())
    result = orchestrate_deploy_action(build_id=bid, environment_id=None, runtime_id="rt-1")
    assert result["_success"] is False
    assert _codes(result) == ["ENVIRONMENT_ID_REQUIRED"]
    assert result["errors"][0]["field"] == "environment_id"


def test_missing_runtime_id(registry):
    bid = registry("b-rt", _single_process_entry())
    result = orchestrate_deploy_action(build_id=bid, environment_id="env-1", runtime_id="")
    assert result["_success"] is False
    assert _codes(result) == ["RUNTIME_ID_REQUIRED"]
    assert result["errors"][0]["field"] == "runtime_id"


def test_all_required_missing_collected():
    result = orchestrate_deploy_action()
    assert result["_success"] is False
    assert set(_codes(result)) == {"BUILD_ID_REQUIRED", "ENVIRONMENT_ID_REQUIRED", "RUNTIME_ID_REQUIRED"}
    assert result["error"] == "Missing required deployment inputs."
    _assert_full_error_envelope(result)
    # Unresolved-target early error → target is null with an empty component summary.
    assert result["target"] is None
    assert result["component_summary"]["total_components"] == 0


# ---------------------------------------------------------------------------
# Malformed input types -> structured errors, never raw exceptions
# ---------------------------------------------------------------------------
def test_invalid_build_id_type_returns_structured_error():
    # A list build_id is unhashable; it must not raise TypeError at registry lookup.
    result = orchestrate_deploy_action(build_id=[], environment_id="env-1", runtime_id="rt-1")
    assert result["_success"] is False
    assert _codes(result) == ["INVALID_REQUEST"]
    assert result["errors"][0]["field"] == "build_id"
    # #129 D2: even the raw-ValidationError path returns the full envelope; the unhashable/mistyped
    # build_id is sanitized to null rather than making a placeholder stage model raise.
    _assert_full_error_envelope(result)
    assert result["build_id"] is None


def test_early_error_envelope_sanitizes_non_string_profile():
    # #129 D2 (review r1): a non-string raw profile on the ValidationError path must be sanitized to
    # null in the envelope (never echoed raw), matching the other sanitized request fields and
    # keeping the response contract/JSON-serializability intact.
    result = orchestrate_deploy_action(
        build_id=[], environment_id="env-1", runtime_id="rt-1", profile=[],
    )
    assert result["_success"] is False
    assert set(_codes(result)) == {"INVALID_REQUEST"}
    # A non-string profile is nulled in the envelope, never echoed raw.
    assert result["profile"] is None
    _assert_full_error_envelope(result)


def test_early_error_envelope_preserves_valid_string_profile(registry):
    # A VALID string profile is preserved in the early-error envelope (sanitize nulls only non-str).
    bid = registry("b-prof-ok", _single_process_entry())
    result = orchestrate_deploy_action(
        build_id=bid, environment_id="env-1", runtime_id="rt-1",
        profile="prof-x", schedule_override=[],
    )
    assert result["_success"] is False
    assert _codes(result) == ["INVALID_REQUEST"]
    assert result["profile"] == "prof-x"


def test_early_error_envelope_type_checks_bool_flags():
    # #129 D2 (review r2): a non-bool raw run_test/dry_run must NOT be truthily coerced (bool(
    # "banana") == True) into the error envelope — it falls back to the field default. A "banana"
    # run_test on the ValidationError path yields execution.run_test == False, not True.
    result = orchestrate_deploy_action(
        build_id="x", environment_id="env-1", runtime_id="rt-1", run_test="banana",
    )
    assert result["_success"] is False
    assert "INVALID_REQUEST" in _codes(result)
    assert result["execution"]["run_test"] is False


def test_invalid_schedule_override_type_returns_structured_error(registry):
    # A non-dict schedule_override must not raise ValidationError out of the function.
    bid = registry("b-badsched", _single_process_entry())
    result = orchestrate_deploy_action(
        build_id=bid, environment_id="env-1", runtime_id="rt-1", schedule_override=[],
    )
    assert result["_success"] is False
    assert _codes(result) == ["INVALID_REQUEST"]
    assert result["errors"][0]["field"] == "schedule_override"
    _assert_full_error_envelope(result)


def test_multiple_invalid_types_collected():
    result = orchestrate_deploy_action(build_id=[], environment_id="env-1", runtime_id="rt-1", schedule_override=[])
    assert result["_success"] is False
    assert _codes(result) == ["INVALID_REQUEST", "INVALID_REQUEST"]
    assert {e["field"] for e in result["errors"]} == {"build_id", "schedule_override"}


def test_malformed_input_never_raises(registry):
    # No combination of mistyped inputs should raise; every call returns a dict.
    bid = registry("b-noraise", _single_process_entry())
    for kwargs in (
        {"build_id": {"x": 1}, "environment_id": "e", "runtime_id": "r"},
        {"build_id": 123, "environment_id": "e", "runtime_id": "r"},
        {"build_id": bid, "environment_id": ["e"], "runtime_id": "r"},
        {"build_id": bid, "environment_id": "e", "runtime_id": "r", "run_test": "banana"},
        {"build_id": bid, "environment_id": "e", "runtime_id": "r", "profile": []},
    ):
        result = orchestrate_deploy_action(**kwargs)
        assert isinstance(result, dict)
        assert result["_success"] is False
        assert result["errors"]


# ---------------------------------------------------------------------------
# Build resolution failures
# ---------------------------------------------------------------------------
def test_unknown_build_id():
    result = orchestrate_deploy_action(
        build_id="does-not-exist", environment_id="env-1", runtime_id="rt-1",
    )
    assert result["_success"] is False
    assert _codes(result) == ["BUILD_ID_UNKNOWN"]
    _assert_full_error_envelope(result)
    assert result["target"] is None


def test_malformed_registry_entry(registry):
    bid = registry("b-bad", {"profile": "p"})  # no spec
    result = orchestrate_deploy_action(build_id=bid, environment_id="env-1", runtime_id="rt-1")
    assert result["_success"] is False
    assert _codes(result) == ["BUILD_REGISTRY_ENTRY_MALFORMED"]
    _assert_full_error_envelope(result)


def test_no_process_component(registry):
    entry = _entry(
        components=[_component("conn", "connector-settings", name="Conn")],
        results={"conn": _result(status="created", component_id="CONN-1", ctype="connector-settings", name="Conn")},
    )
    bid = registry("b-noproc", entry)
    result = orchestrate_deploy_action(build_id=bid, environment_id="env-1", runtime_id="rt-1")
    assert result["_success"] is False
    assert _codes(result) == ["BUILD_PROCESS_NOT_FOUND"]


def test_multiple_process_components(registry):
    entry = _entry(
        components=[
            _component("p1", "process", name="P1"),
            _component("p2", "process", name="P2"),
        ],
        results={
            "p1": _result(status="created", component_id="PID-1", ctype="process", name="P1"),
            "p2": _result(status="created", component_id="PID-2", ctype="process", name="P2"),
        },
    )
    bid = registry("b-multi", entry)
    result = orchestrate_deploy_action(build_id=bid, environment_id="env-1", runtime_id="rt-1")
    assert result["_success"] is False
    assert _codes(result) == ["BUILD_MULTIPLE_PROCESS_COMPONENTS"]
    assert set(result["errors"][0]["details"]["process_keys"]) == {"p1", "p2"}


def test_single_process_missing_component_id(registry):
    entry = _entry(
        components=[_component("proc", "process", name="P")],
        results={"proc": _result(status="created", component_id=None, ctype="process", name="P")},
    )
    bid = registry("b-noid", entry)
    result = orchestrate_deploy_action(build_id=bid, environment_id="env-1", runtime_id="rt-1")
    assert result["_success"] is False
    assert _codes(result) == ["BUILD_PROCESS_ID_MISSING"]


def test_single_process_results_entry_absent(registry):
    entry = _entry(
        components=[_component("proc", "process", name="P")],
        results={},  # no results entry for the process at all
    )
    bid = registry("b-noresult", entry)
    result = orchestrate_deploy_action(build_id=bid, environment_id="env-1", runtime_id="rt-1")
    assert result["_success"] is False
    assert _codes(result) == ["BUILD_PROCESS_ID_MISSING"]


def test_single_process_missing_key_returns_malformed_not_validation_error(registry):
    # #129 D3: a process component whose key is None must surface a structured
    # BUILD_REGISTRY_ENTRY_MALFORMED, NOT an uncaught Pydantic ValidationError from constructing
    # ResolvedBuildTarget(process_key: str).
    entry = _entry(
        components=[{"key": None, "type": "process", "action": "create", "name": "P",
                     "component_id": "CID-1", "config": {}, "depends_on": []}],
        results={},
        execution_order=[],
    )
    bid = registry("b-nokey", entry)
    result = orchestrate_deploy_action(build_id=bid, environment_id="env-1", runtime_id="rt-1")
    assert result["_success"] is False
    assert _codes(result) == ["BUILD_REGISTRY_ENTRY_MALFORMED"]
    assert result["errors"][0]["details"]["process_key"] == "<unknown>"
    _assert_full_error_envelope(result)


def test_single_process_non_string_component_id_returns_malformed(registry):
    # #129 D3: a present-but-non-string component_id is malformed registry data — a structured
    # error, not a raised ValidationError from ResolvedBuildTarget(process_component_id: str).
    entry = _entry(
        components=[_component("proc", "process", name="P")],
        results={"proc": {"status": "created", "component_id": 123, "type": "process", "name": "P"}},
    )
    bid = registry("b-nonstr-id", entry)
    result = orchestrate_deploy_action(build_id=bid, environment_id="env-1", runtime_id="rt-1")
    assert result["_success"] is False
    assert _codes(result) == ["BUILD_REGISTRY_ENTRY_MALFORMED"]
    assert result["errors"][0]["details"]["process_key"] == "proc"


@pytest.mark.parametrize("bad_id", [0, False, [], {}, ["x"]])
def test_single_process_falsy_non_string_component_id_returns_malformed(registry, bad_id):
    # #129 D3 (review r2): a present but FALSY non-string component_id (0/False/[]/{}) must be
    # classified BUILD_REGISTRY_ENTRY_MALFORMED, not silently dropped by `x or y` and reported as
    # BUILD_PROCESS_ID_MISSING.
    entry = _entry(
        components=[_component("proc", "process", name="P")],
        results={"proc": {"status": "created", "component_id": bad_id, "type": "process", "name": "P"}},
    )
    bid = registry("b-falsy-id", entry)
    result = orchestrate_deploy_action(build_id=bid, environment_id="env-1", runtime_id="rt-1")
    assert result["_success"] is False
    assert _codes(result) == ["BUILD_REGISTRY_ENTRY_MALFORMED"]


def test_single_process_spec_fallback_still_used_when_result_id_absent(registry):
    # The `is not None` fallback must still defer to the spec component_id when the RESULT entry has
    # no component_id at all (guards against the review-r2 fix over-narrowing the fallback).
    entry = _entry(
        components=[_component("proc", "process", name="P", action="update", component_id="SPEC-CID")],
        results={"proc": {"status": "created", "type": "process", "name": "P"}},  # no component_id key
    )
    bid = registry("b-spec-fallback2", entry)
    result = orchestrate_deploy_action(build_id=bid, environment_id="env-1", runtime_id="rt-1")
    assert result["_success"] is True
    assert result["target"]["process_component_id"] == "SPEC-CID"


@pytest.mark.parametrize("bad_key", [[], {}, ["p"], {"k": 1}])
def test_single_process_unhashable_key_returns_malformed_not_typeerror(registry, bad_key):
    # #129 D3 (review r1): an UNHASHABLE key (list/dict) must NOT raise TypeError from the
    # results.get(process_key) lookup — the malformed-key guard runs before any dict lookup and
    # returns a structured BUILD_REGISTRY_ENTRY_MALFORMED.
    entry = _entry(
        components=[{"key": bad_key, "type": "process", "action": "create", "name": "P",
                     "component_id": "CID-1", "config": {}, "depends_on": []}],
        results={},
        execution_order=[],
    )
    bid = registry("b-unhashable-key", entry)
    result = orchestrate_deploy_action(build_id=bid, environment_id="env-1", runtime_id="rt-1")
    assert result["_success"] is False
    assert _codes(result) == ["BUILD_REGISTRY_ENTRY_MALFORMED"]
    assert result["errors"][0]["details"]["process_key"] == "<unknown>"
    _assert_full_error_envelope(result)


def test_multiple_process_components_keys_are_strings(registry):
    # #129 D8: when one of several process candidates has no key, the process_keys error detail
    # must contain only strings (substituting "<unknown>"), never a None.
    entry = _entry(
        components=[
            _component("p1", "process", name="P1"),
            {"key": None, "type": "process", "action": "create", "name": "P2",
             "component_id": "PID-2", "config": {}, "depends_on": []},
        ],
        results={
            "p1": _result(status="created", component_id="PID-1", ctype="process", name="P1"),
        },
        execution_order=["p1"],
    )
    bid = registry("b-multi-nokey", entry)
    result = orchestrate_deploy_action(build_id=bid, environment_id="env-1", runtime_id="rt-1")
    assert result["_success"] is False
    assert _codes(result) == ["BUILD_MULTIPLE_PROCESS_COMPONENTS"]
    keys = result["errors"][0]["details"]["process_keys"]
    assert all(isinstance(k, str) for k in keys)
    assert None not in keys
    assert "<unknown>" in keys
    assert "p1" in keys


# ---------------------------------------------------------------------------
# Build resolution successes
# ---------------------------------------------------------------------------
def test_process_id_from_spec_fallback(registry):
    entry = _entry(
        components=[_component("proc", "process", name="P", action="update", component_id="SPEC-CID")],
        results={},  # results absent, but spec declares the component_id
    )
    bid = registry("b-fallback", entry)
    result = orchestrate_deploy_action(build_id=bid, environment_id="env-1", runtime_id="rt-1")
    assert result["_success"] is True
    assert result["target"]["process_component_id"] == "SPEC-CID"


@pytest.mark.parametrize("config_key", ["type", "component_type"])
def test_wrapped_process_resolves(registry, config_key):
    # A process authored via the generic "component" wrapper keeps top-level type "component"
    # in the stored spec; the real type lives in config.type / config.component_type. The
    # resolver must unwrap it the same way integration_builder does (Codex review #60).
    entry = _entry(
        components=[
            {
                "key": "proc", "type": "component", "action": "create", "name": "P",
                "component_id": None, "config": {config_key: "process", "xml": "<process/>"},
                "depends_on": [],
            }
        ],
        results={"proc": {"status": "created", "component_id": "WPID-1", "type": "component", "name": "P"}},
    )
    bid = registry(f"b-wrapped-{config_key}", entry)
    result = orchestrate_deploy_action(build_id=bid, environment_id="env-1", runtime_id="rt-1")
    assert result["_success"] is True
    assert result["target"]["process_key"] == "proc"
    assert result["target"]["process_component_id"] == "WPID-1"


def test_unwrappable_generic_component_is_not_a_process(registry):
    # A generic wrapper with no resolvable config type must NOT count as a process.
    entry = _entry(
        components=[
            {"key": "c", "type": "component", "action": "create", "name": "C",
             "component_id": None, "config": {"xml": "<x/>"}, "depends_on": []}
        ],
        results={"c": {"status": "created", "component_id": "CID-X", "type": "component", "name": "C"}},
    )
    bid = registry("b-unwrappable", entry)
    result = orchestrate_deploy_action(build_id=bid, environment_id="env-1", runtime_id="rt-1")
    assert result["_success"] is False
    assert _codes(result) == ["BUILD_PROCESS_NOT_FOUND"]


def test_reused_process_resolves(registry):
    # A reused result entry has NO "result" sub-key — confirm we never depend on it.
    entry = _entry(
        components=[_component("proc", "process", name="P")],
        results={"proc": {"status": "reused", "component_id": "CID-REUSE", "type": "process", "name": "P"}},
    )
    bid = registry("b-reused", entry)
    result = orchestrate_deploy_action(build_id=bid, environment_id="env-1", runtime_id="rt-1")
    assert result["_success"] is True
    assert result["target"]["process_status"] == "reused"
    assert result["target"]["process_component_id"] == "CID-REUSE"


def test_full_success_contract(registry):
    bid = registry("b-full", _single_process_entry(process_id="CID-1"))
    result = orchestrate_deploy_action(
        boomi_client=MagicMock(), profile="prod", build_id=bid,
        environment_id="env-9", runtime_id="rt-9",
        schedule_override=None, run_test=False,
    )

    expected_keys = {
        "_success", "profile", "build_id", "dry_run", "plan_only", "behavior_verified",
        "integration_name", "target", "component_summary", "package", "deployment",
        "runtime_attachment", "schedule", "execution", "logs", "cleanup", "summary",
        "warnings", "errors",
    }
    assert set(result.keys()) == expected_keys

    # Additive behavioral-verification marker (issue #81): a dry-run is never verified.
    assert result["behavior_verified"] == {
        "verified": False, "reason": "dry_run", "logs_status": "skipped",
    }

    assert result["_success"] is True
    assert result["dry_run"] is True
    assert result["plan_only"] is True
    assert result["profile"] == "prod"
    assert result["build_id"] == bid
    assert result["integration_name"] == "MyIntegration"
    assert result["warnings"] == []
    assert result["errors"] == []

    target = result["target"]
    assert target["process_key"] == "proc"
    assert target["process_component_id"] == "CID-1"
    assert target["integration_name"] == result["integration_name"]
    # component_summary surfaced both top-level and inside target — identical content.
    assert result["component_summary"] == target["component_summary"]
    assert result["component_summary"]["total_components"] == 2
    assert result["component_summary"]["by_type"] == {"connector-settings": 1, "process": 1}

    # Stage statuses (dry-run defaults).
    assert result["package"]["status"] == "planned"
    assert result["deployment"]["status"] == "planned"
    assert result["deployment"]["environment_id"] == "env-9"
    assert result["runtime_attachment"]["status"] == "planned"
    assert result["runtime_attachment"]["runtime_id"] == "rt-9"
    assert result["schedule"]["status"] == "not_required"
    assert result["execution"]["status"] == "skipped"
    assert result["logs"]["status"] == "skipped"
    assert result["cleanup"]["status"] == "not_required"

    # All created-resource stage ids remain null in a dry-run.
    assert result["package"]["package_id"] is None
    assert result["deployment"]["deployment_id"] is None
    assert result["runtime_attachment"]["attachment_id"] is None
    assert result["schedule"]["schedule_id"] is None
    assert result["execution"]["execution_id"] is None

    # Stable top-level summary (dry-run: ids null, version defaults to build_id).
    summary = result["summary"]
    assert summary["package_id"] is None
    assert summary["deployment_id"] is None
    assert summary["package_version"] == bid  # defaults to build_id when not provided
    assert summary["environment_id"] == "env-9"
    assert summary["deployment_active"] is None
    assert summary["deployment_current_version"] is None
    # Runtime/schedule summary keys (dry-run: planned/not_required, ids null, no reuse/change).
    assert summary["runtime_id"] == "rt-9"
    assert summary["runtime_attachment_id"] is None
    assert summary["runtime_attachment_status"] == "planned"
    assert summary["schedule_id"] is None
    assert summary["schedule_status"] == "not_required"
    assert summary["schedule_enabled"] is None
    assert summary["resource_reuse"] == {
        "package": False, "deployment": False, "runtime_attachment": False, "schedule": False,
    }
    assert summary["resource_changes"] == {
        "package": False, "deployment": False, "runtime_attachment": False, "schedule": False,
    }
    # stage_statuses surfaces each stage's status for at-a-glance recovery (issue #65).
    assert summary["stage_statuses"] == {
        "package": "planned", "deployment": "planned",
        "runtime_attachment": "planned", "schedule": "not_required",
    }
    assert summary["stage_warnings"] == {
        "package": [], "deployment": [], "runtime_attachment": [], "schedule": [],
    }


def test_success_with_schedule_and_run_test(registry):
    bid = registry("b-sched", _single_process_entry())
    override = {"cron": "0 0 * * *"}
    result = orchestrate_deploy_action(
        build_id=bid, environment_id="env-1", runtime_id="rt-1",
        schedule_override=override, run_test=True,
    )
    assert result["_success"] is True
    assert result["schedule"]["status"] == "planned"
    assert result["schedule"]["schedule_override"] == override
    assert result["execution"]["status"] == "planned"
    assert result["execution"]["run_test"] is True
    assert result["logs"]["status"] == "planned"


# ---------------------------------------------------------------------------
# Plan-only guarantees: no SDK calls, no registry mutation
# ---------------------------------------------------------------------------
def test_no_sdk_calls_exploding_client(registry):
    bid = registry("b-explode", _single_process_entry())
    result = orchestrate_deploy_action(
        boomi_client=_ExplodingClient(), build_id=bid,
        environment_id="env-1", runtime_id="rt-1",
    )
    assert result["_success"] is True  # never touched the client, never raised


def test_no_sdk_calls_mock_calls_empty(registry):
    bid = registry("b-mock", _single_process_entry())
    client = MagicMock()
    orchestrate_deploy_action(
        boomi_client=client, build_id=bid, environment_id="env-1", runtime_id="rt-1",
    )
    assert client.mock_calls == []


def test_registry_not_mutated(registry):
    bid = registry("b-pure", _single_process_entry())
    before = copy.deepcopy(_BUILD_REGISTRY[bid])
    orchestrate_deploy_action(build_id=bid, environment_id="env-1", runtime_id="rt-1")
    assert _BUILD_REGISTRY[bid] == before


# ---------------------------------------------------------------------------
# Real run (issue #61): idempotent package + deploy stages
# ---------------------------------------------------------------------------
def test_dry_run_package_deploy_stages_are_planned_and_no_sdk_calls(registry):
    # Dry-run never touches the client (or manage_deployment_action); stages stay "planned".
    bid = registry("b-dry61", _single_process_entry())
    result = orchestrate_deploy_action(
        boomi_client=_ExplodingClient(), build_id=bid,
        environment_id="env-1", runtime_id="rt-1",
        dry_run=True, package_version="1.2.3",
    )
    assert result["_success"] is True
    assert result["dry_run"] is True
    assert result["plan_only"] is True
    assert result["package"]["status"] == "planned"
    assert result["deployment"]["status"] == "planned"
    assert result["summary"]["package_version"] == "1.2.3"
    assert result["summary"]["package_id"] is None
    assert result["summary"]["deployment_id"] is None


def test_real_run_creates_package_when_no_existing_version(registry, monkeypatch):
    bid = registry("b-create", _single_process_entry(process_id="CID-1"))
    fake = _bind_success(monkeypatch, {
        "list_packages": {"_success": True, "packages": []},
        "create_package": {"_success": True, "package": _pkg("pkg-new", bid)},
        "list_deployments": {"_success": True, "deployments": []},
        "deploy": {"_success": True, "deployment": _dep("dep-new", True, current_version="1")},
    })
    result = orchestrate_deploy_action(
        boomi_client=MagicMock(), build_id=bid,
        environment_id="env-1", runtime_id="rt-1", dry_run=False,
    )
    assert result["_success"] is True
    assert result["package"]["status"] == "created"
    assert result["summary"]["package_id"] == "pkg-new"
    assert fake.actions_called().count("create_package") == 1


def test_real_run_reuses_existing_package_for_same_component_and_version(registry, monkeypatch):
    bid = registry("b-reusepkg", _single_process_entry(process_id="CID-1"))
    # Two packages match the effective version (=build_id); a third is a different version.
    fake = _bind_success(monkeypatch, {
        "list_packages": {"_success": True, "packages": [
            _pkg("pkg-old", bid, created_date="2026-01-01T00:00:00Z"),
            _pkg("pkg-new", bid, created_date="2026-02-01T00:00:00Z"),
            _pkg("pkg-other", "9.9.9", created_date="2026-03-01T00:00:00Z"),
        ]},
        "list_deployments": {"_success": True, "deployments": []},
        "deploy": {"_success": True, "deployment": _dep("dep-1", True)},
    })
    result = orchestrate_deploy_action(
        boomi_client=MagicMock(), build_id=bid,
        environment_id="env-1", runtime_id="rt-1", dry_run=False,
    )
    assert result["_success"] is True
    assert "create_package" not in fake.actions_called()
    assert result["package"]["status"] == "reused"
    assert result["summary"]["package_id"] == "pkg-new"  # newest by created_date
    assert result["package"]["warnings"]  # multiple matches -> stage warning
    assert result["summary"]["stage_warnings"]["package"]


def test_real_run_deploys_package_when_no_active_deployment(registry, monkeypatch):
    bid = registry("b-deploy", _single_process_entry(process_id="CID-1"))
    fake = _bind_success(monkeypatch, {
        "list_packages": {"_success": True, "packages": [_pkg("pkg-1", bid)]},
        "list_deployments": {"_success": True, "deployments": [_dep("dep-old", False)]},
        "deploy": {"_success": True, "deployment": _dep("dep-new", True, current_version="2")},
    }, environment_id="env-9")
    result = orchestrate_deploy_action(
        boomi_client=MagicMock(), build_id=bid,
        environment_id="env-9", runtime_id="rt-1", dry_run=False,
    )
    assert result["_success"] is True
    assert result["deployment"]["status"] == "deployed"
    assert result["summary"]["deployment_id"] == "dep-new"
    assert result["summary"]["environment_id"] == "env-9"
    assert result["summary"]["deployment_active"] is True
    assert result["summary"]["deployment_current_version"] == "2"
    assert fake.actions_called().count("deploy") == 1


def test_real_run_reuses_existing_active_deployment(registry, monkeypatch):
    bid = registry("b-reusedep", _single_process_entry(process_id="CID-1"))
    fake = _bind_success(monkeypatch, {
        "list_packages": {"_success": True, "packages": [_pkg("pkg-1", bid)]},
        "list_deployments": {"_success": True, "deployments": [
            _dep("dep-active", True, current_version="3"),
            _dep("dep-inactive", False),
        ]},
        "deploy": {"_success": True, "deployment": _dep("should-not-be-used", True)},
    })
    result = orchestrate_deploy_action(
        boomi_client=MagicMock(), build_id=bid,
        environment_id="env-1", runtime_id="rt-1", dry_run=False,
    )
    assert result["_success"] is True
    assert "deploy" not in fake.actions_called()
    assert result["deployment"]["status"] == "reused"
    assert result["summary"]["deployment_id"] == "dep-active"
    assert result["summary"]["deployment_active"] is True
    assert result["summary"]["deployment_current_version"] == "3"
    assert result["deployment"]["warnings"]  # an inactive deployment also exists -> warning


def test_real_run_deploy_api_failure_is_structured_and_blocks_downstream(registry, monkeypatch):
    bid = registry("b-deployfail", _single_process_entry(process_id="CID-1"))
    fake = _FakeDeploymentAction({
        "list_packages": {"_success": True, "packages": []},
        "create_package": {"_success": True, "package": _pkg("pkg-1", bid)},
        "list_deployments": {"_success": True, "deployments": []},
        "deploy": {
            "_success": False,
            "error": "Action 'deploy' failed: Boomi denied deploy (500)",
            "exception_type": "ApiError",
        },
    })
    monkeypatch.setattr(orchestration, "manage_deployment_action", fake)
    result = orchestrate_deploy_action(
        boomi_client=MagicMock(), build_id=bid,
        environment_id="env-1", runtime_id="rt-1", dry_run=False,
    )
    assert result["_success"] is False
    assert "DEPLOY_CREATE_FAILED" in _codes(result)
    assert result["package"]["status"] == "created"  # package stage succeeded
    assert result["deployment"]["status"] == "failed"
    assert result["runtime_attachment"]["status"] == "blocked"
    assert result["schedule"]["status"] == "blocked"
    assert result["execution"]["status"] == "blocked"
    assert result["logs"]["status"] == "blocked"
    # The package was created (deploy failed after); cleanup PLANS delete_package, dry-run only.
    assert result["cleanup"]["status"] == "planned"
    assert result["cleanup"]["dry_run"] is True
    assert result["cleanup"]["mutation_allowed"] is False
    ops = result["cleanup"]["operations"]
    assert [op["action"] for op in ops] == ["delete_package"]
    assert ops[0]["resource_id"] == "pkg-1"
    assert ops[0]["destructive"] is True
    assert "delete_package" not in fake.actions_called()  # plan only — never mutates
    # Structured failure metadata (issue #65).
    assert result["error_code"] == "DEPLOY_CREATE_FAILED"
    assert result["failed_stage"] == "deployment"
    assert result["prior_stage_summary"]["package"]["status"] == "created"
    assert result["prior_stage_summary"]["package"]["package_id"] == "pkg-1"
    assert isinstance(result["next_step"], str) and result["next_step"]


def test_real_run_ambiguous_existing_active_deployments_blocks_redeploy(registry, monkeypatch):
    bid = registry("b-ambig", _single_process_entry(process_id="CID-1"))
    fake = _FakeDeploymentAction({
        "list_packages": {"_success": True, "packages": [_pkg("pkg-1", bid)]},
        "list_deployments": {"_success": True, "deployments": [
            _dep("dep-a", True), _dep("dep-b", True),
        ]},
        "deploy": {"_success": True, "deployment": _dep("should-not-be-used", True)},
    })
    monkeypatch.setattr(orchestration, "manage_deployment_action", fake)
    result = orchestrate_deploy_action(
        boomi_client=MagicMock(), build_id=bid,
        environment_id="env-1", runtime_id="rt-1", dry_run=False,
    )
    assert result["_success"] is False
    assert "DEPLOY_AMBIGUOUS_EXISTING" in _codes(result)
    assert "deploy" not in fake.actions_called()  # never redeploys when ambiguous
    assert result["deployment"]["status"] == "failed"
    assert result["runtime_attachment"]["status"] == "blocked"
    assert result["execution"]["status"] == "blocked"


def test_package_selection_deterministic_on_created_date_tie(registry, monkeypatch):
    # #129 D6: two packages match the same version with identical created_date. Selection must be
    # deterministic — the total sort key (created_date, package_id) breaks the tie by package_id
    # (highest under reverse sort), never leaving it to backend list order.
    bid = registry("b-tie", _single_process_entry(process_id="CID-1"))
    same_date = "2026-02-02T00:00:00Z"
    dep = _bind_success(monkeypatch, {
        "list_packages": {"_success": True, "packages": [
            _pkg("pkg-aaa", bid, created_date=same_date),
            _pkg("pkg-zzz", bid, created_date=same_date),
        ]},
        "list_deployments": {"_success": True, "deployments": [_dep("dep-1", True, current_version="1")]},
    })
    result = orchestrate_deploy_action(
        boomi_client=MagicMock(), build_id=bid,
        environment_id="env-1", runtime_id="rt-1", dry_run=False,
    )
    assert result["_success"] is True
    assert result["package"]["status"] == "reused"
    assert result["package"]["package_id"] == "pkg-zzz"
    # The multi-match warning names the (created_date, package_id) tie-breaker.
    assert any("package_id" in w for w in result["package"]["warnings"])


def test_package_create_conflict_relists_and_reuses(registry, monkeypatch):
    # #129 D4: a concurrent create fills the list-then-create window. On a 409/conflict create
    # failure, re-list once and reuse the winner instead of surfacing a spurious PACKAGE_CREATE_FAILED.
    bid = registry("b-pkg-race", _single_process_entry(process_id="CID-1"))
    dep = _patch_deploy_seq(monkeypatch, {
        "list_packages": [
            {"_success": True, "packages": []},                    # first: none → create
            {"_success": True, "packages": [_pkg("pkg-1", bid)]},  # re-list after conflict
        ],
        "create_package": {"_success": False, "status_code": 409, "error": "package already exists"},
        "list_deployments": {"_success": True, "deployments": [_dep("dep-1", True, current_version="1")]},
        **_process_attachments("rt-1", "env-1", "CID-1"),
    })
    result = orchestrate_deploy_action(
        boomi_client=MagicMock(), build_id=bid,
        environment_id="env-1", runtime_id="rt-1", dry_run=False,
    )
    assert result["_success"] is True
    assert "PACKAGE_CREATE_FAILED" not in _codes(result)
    assert result["package"]["status"] == "reused"
    assert result["package"]["package_id"] == "pkg-1"
    assert any("conflict" in w.lower() for w in result["package"]["warnings"])
    assert dep.actions_called().count("list_packages") == 2


def test_package_create_conflict_empty_relist_stays_failed(registry, monkeypatch):
    # #129 D4: a conflict whose re-list still finds no matching package is a genuine failure —
    # surface PACKAGE_CREATE_FAILED rather than silently succeeding.
    bid = registry("b-pkg-race-empty", _single_process_entry(process_id="CID-1"))
    dep = _patch_deploy_seq(monkeypatch, {
        "list_packages": [
            {"_success": True, "packages": []},
            {"_success": True, "packages": []},  # re-list STILL empty
        ],
        "create_package": {"_success": False, "status_code": 409, "error": "conflict"},
        "list_deployments": {"_success": True, "deployments": [_dep("dep-1", True)]},
        **_process_attachments("rt-1", "env-1", "CID-1"),
    })
    result = orchestrate_deploy_action(
        boomi_client=MagicMock(), build_id=bid,
        environment_id="env-1", runtime_id="rt-1", dry_run=False,
    )
    assert result["_success"] is False
    assert "PACKAGE_CREATE_FAILED" in _codes(result)


def test_deploy_conflict_relists_and_reuses(registry, monkeypatch):
    # #129 D4: a concurrent deploy fills the list-then-deploy window. On a conflict, re-list once
    # and reuse the single active deployment.
    bid = registry("b-dep-race", _single_process_entry(process_id="CID-1"))
    dep = _patch_deploy_seq(monkeypatch, {
        "list_packages": {"_success": True, "packages": [_pkg("pkg-1", bid)]},
        "list_deployments": [
            {"_success": True, "deployments": []},                                        # none active → deploy
            {"_success": True, "deployments": [_dep("dep-1", True, current_version="1")]},  # re-list
        ],
        "deploy": {"_success": False, "status_code": 409, "error": "already deployed"},
        **_process_attachments("rt-1", "env-1", "CID-1"),
    })
    result = orchestrate_deploy_action(
        boomi_client=MagicMock(), build_id=bid,
        environment_id="env-1", runtime_id="rt-1", dry_run=False,
    )
    assert result["_success"] is True
    assert "DEPLOY_CREATE_FAILED" not in _codes(result)
    assert result["deployment"]["status"] == "reused"
    assert result["deployment"]["deployment_id"] == "dep-1"
    assert any("conflict" in w.lower() for w in result["deployment"]["warnings"])
    assert dep.actions_called().count("list_deployments") == 2


def test_deploy_conflict_relist_ambiguous_blocks(registry, monkeypatch):
    # #129 D4: a conflict whose re-list finds MORE than one active deployment is ambiguous — refuse.
    bid = registry("b-dep-race-ambig", _single_process_entry(process_id="CID-1"))
    dep = _patch_deploy_seq(monkeypatch, {
        "list_packages": {"_success": True, "packages": [_pkg("pkg-1", bid)]},
        "list_deployments": [
            {"_success": True, "deployments": []},
            {"_success": True, "deployments": [_dep("dep-a", True), _dep("dep-b", True)]},
        ],
        "deploy": {"_success": False, "status_code": 409, "error": "conflict"},
        **_process_attachments("rt-1", "env-1", "CID-1"),
    })
    result = orchestrate_deploy_action(
        boomi_client=MagicMock(), build_id=bid,
        environment_id="env-1", runtime_id="rt-1", dry_run=False,
    )
    assert result["_success"] is False
    assert "DEPLOY_AMBIGUOUS_EXISTING" in _codes(result)
    assert result["deployment"]["status"] == "failed"


def test_deploy_conflict_empty_relist_stays_failed(registry, monkeypatch):
    # #129 D4: a conflict whose re-list finds no active deployment is a genuine failure.
    bid = registry("b-dep-race-empty", _single_process_entry(process_id="CID-1"))
    dep = _patch_deploy_seq(monkeypatch, {
        "list_packages": {"_success": True, "packages": [_pkg("pkg-1", bid)]},
        "list_deployments": [
            {"_success": True, "deployments": []},
            {"_success": True, "deployments": []},  # re-list STILL none active
        ],
        "deploy": {"_success": False, "status_code": 409, "error": "conflict"},
        **_process_attachments("rt-1", "env-1", "CID-1"),
    })
    result = orchestrate_deploy_action(
        boomi_client=MagicMock(), build_id=bid,
        environment_id="env-1", runtime_id="rt-1", dry_run=False,
    )
    assert result["_success"] is False
    assert "DEPLOY_CREATE_FAILED" in _codes(result)


def test_deploy_non_conflict_failure_stays_hard_error(registry, monkeypatch):
    # #129 D4: a NON-conflict deploy failure must remain a hard error with NO recovery re-list.
    bid = registry("b-dep-hardfail", _single_process_entry(process_id="CID-1"))
    dep = _patch_deploy_seq(monkeypatch, {
        "list_packages": {"_success": True, "packages": [_pkg("pkg-1", bid)]},
        "list_deployments": {"_success": True, "deployments": []},
        "deploy": {"_success": False, "error": "permission denied"},
        **_process_attachments("rt-1", "env-1", "CID-1"),
    })
    result = orchestrate_deploy_action(
        boomi_client=MagicMock(), build_id=bid,
        environment_id="env-1", runtime_id="rt-1", dry_run=False,
    )
    assert result["_success"] is False
    assert "DEPLOY_CREATE_FAILED" in _codes(result)
    # Only ONE list_deployments call — a non-conflict failure never triggers the recovery re-list.
    assert dep.actions_called().count("list_deployments") == 1


def test_is_create_conflict_response_matches_int_and_string_409():
    # #129 D4 (review r2): the conflict detector matches status_code 409 as BOTH int and string,
    # matches duplicate-flavored error text, and stays conservative — a non-conflict failure and a
    # non-dict are never misread as a conflict (which would wrongly trigger reuse of another resource).
    f = orchestration._is_create_conflict_response
    assert f({"status_code": 409}) is True
    assert f({"status_code": "409"}) is True
    assert f({"error": "Action 'deploy' failed: Conflict — already deployed"}) is True
    assert f({"exception_type": "ApiError", "error": "duplicate package version"}) is True
    assert f({"error": "permission denied"}) is False
    assert f({"status_code": 500, "error": "internal error"}) is False
    assert f(None) is False
    assert f("409") is False  # non-dict is never a conflict


def test_real_run_missing_process_id_from_resolver_never_calls_sdk(registry):
    # Resolution failure precedes any SDK/manage call, even with dry_run=False.
    entry = _entry(
        components=[_component("proc", "process", name="P")],
        results={"proc": _result(status="created", component_id=None, ctype="process", name="P")},
    )
    bid = registry("b-noid61", entry)
    client = MagicMock()
    result = orchestrate_deploy_action(
        boomi_client=client, build_id=bid,
        environment_id="env-1", runtime_id="rt-1", dry_run=False,
    )
    assert result["_success"] is False
    assert _codes(result) == ["BUILD_PROCESS_ID_MISSING"]
    assert client.mock_calls == []


def test_real_run_deploy_current_version_falls_back_to_version(registry, monkeypatch):
    # The real DeployedPackage SDK model exposes the revision under "version" as an INT, not
    # "current_version" — so the summary must fall back to "version" and coerce it to str
    # (an int would otherwise raise a ValidationError on the Optional[str] stage field).
    bid = registry("b-versionfallback", _single_process_entry(process_id="CID-1"))
    fake = _bind_success(monkeypatch, {
        "list_packages": {"_success": True, "packages": [_pkg("pkg-1", bid)]},
        "list_deployments": {"_success": True, "deployments": []},
        "deploy": {"_success": True, "deployment": _dep("dep-new", True, version=7)},
    })
    result = orchestrate_deploy_action(
        boomi_client=MagicMock(), build_id=bid,
        environment_id="env-1", runtime_id="rt-1", dry_run=False,
    )
    assert result["_success"] is True
    assert result["deployment"]["status"] == "deployed"
    assert result["deployment"]["current_version"] == "7"
    assert result["summary"]["deployment_current_version"] == "7"


def test_real_run_reuse_active_coerces_int_version(registry, monkeypatch):
    # The reuse-active branch must apply the same int->str coercion as the fresh-deploy branch.
    bid = registry("b-reuse-intver", _single_process_entry(process_id="CID-1"))
    fake = _bind_success(monkeypatch, {
        "list_packages": {"_success": True, "packages": [_pkg("pkg-1", bid)]},
        "list_deployments": {"_success": True, "deployments": [_dep("dep-active", True, version=9)]},
        "deploy": {"_success": True, "deployment": _dep("should-not-be-used", True)},
    })
    result = orchestrate_deploy_action(
        boomi_client=MagicMock(), build_id=bid,
        environment_id="env-1", runtime_id="rt-1", dry_run=False,
    )
    assert result["_success"] is True
    assert result["deployment"]["status"] == "reused"
    assert "deploy" not in fake.actions_called()
    assert result["summary"]["deployment_current_version"] == "9"


def test_real_run_without_client_returns_boomi_client_required(registry):
    # dry_run=False with no client is a structured failure, not a crash.
    bid = registry("b-noclient", _single_process_entry(process_id="CID-1"))
    result = orchestrate_deploy_action(
        boomi_client=None, build_id=bid,
        environment_id="env-1", runtime_id="rt-1", dry_run=False,
    )
    assert result["_success"] is False
    assert "BOOMI_CLIENT_REQUIRED" in _codes(result)
    assert result["package"]["status"] == "failed"
    assert result["deployment"]["status"] == "blocked"
    assert result["runtime_attachment"]["status"] == "blocked"


# ---------------------------------------------------------------------------
# Real run (issue #62): runtime-attachment + schedule-activation stages
# ---------------------------------------------------------------------------
def _last_bind_index(order):
    return max(
        i for i, (_label, a) in enumerate(order)
        if a in ("get", "list_attachments",
                 "list_process_environment_attachments", "list_process_atom_attachments")
    )


def test_real_run_reuses_existing_runtime_bindings_before_schedule(registry, monkeypatch):
    bid = registry("b-rt-reuse", _single_process_entry(process_id="CID-1"))
    order = []
    dep, env, rt, sch = _patch_all(
        monkeypatch,
        deployment={**_deploy_ok(bid), **_process_attachments("rt-1", "env-1", "CID-1")},
        environments=_ok_env("env-1"),
        runtimes=_ok_runtime("rt-1", "env-1", attached=True),
        schedules={
            "update": {"_success": True, "schedule": _sched("sch-1")},
            "enable": {"_success": True, "status": _status("sst-1", True)},
        },
        order_log=order,
    )
    result = orchestrate_deploy_action(
        boomi_client=MagicMock(), build_id=bid,
        environment_id="env-1", runtime_id="rt-1",
        schedule_override={"cron": "0 9 * * *"}, dry_run=False,
    )
    assert result["_success"] is True
    rta = result["runtime_attachment"]
    assert rta["status"] == "reused"
    assert rta["reused"] is True
    assert rta["changed"] is False
    assert rta["attachment_id"] == "ea-1"  # alias of the runtime<->env attachment id
    # Nothing was created on any router.
    assert "attach" not in rt.actions_called()
    assert "attach_process_environment" not in dep.actions_called()
    assert "attach_process_atom" not in dep.actions_called()
    # Schedule runs strictly after every runtime/process binding call.
    first_sched = min(i for i, (label, _a) in enumerate(order) if label == "schedules")
    assert first_sched > _last_bind_index(order)
    assert result["schedule"]["status"] == "enabled"
    assert result["summary"]["resource_reuse"]["runtime_attachment"] is True


def test_real_run_creates_missing_runtime_and_process_bindings(registry, monkeypatch):
    bid = registry("b-rt-create", _single_process_entry(process_id="CID-1"))
    dep, env, rt, sch = _patch_all(
        monkeypatch,
        deployment={
            **_deploy_ok(bid),
            **_process_attachments("rt-1", "env-1", "CID-1", env_attached=False, atom_attached=False),
        },
        environments=_ok_env("env-1"),
        runtimes=_ok_runtime("rt-1", "env-1", attached=False),
        schedules={},
    )
    result = orchestrate_deploy_action(
        boomi_client=MagicMock(), build_id=bid,
        environment_id="env-1", runtime_id="rt-1", dry_run=False,
    )
    assert result["_success"] is True
    rta = result["runtime_attachment"]
    assert rta["status"] == "attached"
    assert rta["changed"] is True
    assert rta["reused"] is False
    assert rt.actions_called().count("attach") == 1
    assert dep.actions_called().count("attach_process_environment") == 1
    assert dep.actions_called().count("attach_process_atom") == 1
    assert rta["runtime_env_attachment_status"] == "attached"
    assert rta["process_env_attachment_status"] == "attached"
    assert rta["process_runtime_attachment_status"] == "attached"
    assert rta["runtime_env_attachment_id"] == "ea-new"
    assert rta["process_env_attachment_id"] == "pe-new"
    assert rta["process_runtime_attachment_id"] == "pa-new"
    # No override -> schedule not_required, zero schedule calls.
    assert result["schedule"]["status"] == "not_required"
    assert sch.actions_called() == []
    assert result["summary"]["resource_changes"]["runtime_attachment"] is True


def test_real_run_skips_process_atom_leg_on_environment_account(registry, monkeypatch):
    """Issue #66 live-QA finding: environment-enabled accounts reject the direct process<->atom
    (ProcessAtomAttachment) leg with "This account uses environments. Please use
    ComponentEnvironmentAttachment". Legs 1+2 (runtime<->env + process<->env) already make the
    process runnable via the environment, so the direct leg is recorded ``not_required`` and the
    stage succeeds instead of hard-failing at runtime_attachment."""
    bid = registry("b-rt-envacct", _single_process_entry(process_id="CID-1"))
    deployment = {
        **_deploy_ok(bid),
        **_process_attachments("rt-1", "env-1", "CID-1"),  # env+atom legs would otherwise reuse...
    }
    # ...but Boomi rejects the direct process<->atom list on an environment-enabled account.
    deployment["list_process_atom_attachments"] = {
        "_success": False,
        "error": (
            "Action 'list_process_atom_attachments' failed: This account uses environments. "
            "Please use ComponentEnvironmentAttachment"
        ),
    }
    dep, env, rt, sch = _patch_all(
        monkeypatch,
        deployment=deployment,
        environments=_ok_env("env-1"),
        runtimes=_ok_runtime("rt-1", "env-1", attached=True),
        schedules={},
    )
    result = orchestrate_deploy_action(
        boomi_client=MagicMock(), build_id=bid,
        environment_id="env-1", runtime_id="rt-1", dry_run=False,
    )
    assert result["_success"] is True
    rta = result["runtime_attachment"]
    assert rta["status"] == "reused"
    assert rta["process_runtime_attachment_status"] == "not_required"
    assert rta["process_runtime_attachment_id"] is None
    assert rta["runtime_env_attachment_status"] == "reused"
    assert rta["process_env_attachment_status"] == "reused"
    assert rta["reused"] is True
    assert rta["changed"] is False
    # The direct atom-attach create must NEVER be attempted on an environment-enabled account.
    assert "attach_process_atom" not in dep.actions_called()
    # Stage did not fail -> downstream stages proceed (no override -> schedule not_required).
    assert result["schedule"]["status"] == "not_required"
    assert result.get("failed_stage") is None


def test_real_run_skips_process_atom_leg_when_attach_returns_environment_signal(registry, monkeypatch):
    """Companion to the list-failure case: the environment-account signal can also surface from the
    ``attach_process_atom`` call (when the list succeeds with no matching attachment, so an attach is
    attempted). The fix detects the signal on ``error.message`` regardless of which sub-call raised
    it, so the leg is still recorded ``not_required`` and the stage succeeds."""
    bid = registry("b-rt-envacct2", _single_process_entry(process_id="CID-1"))
    deployment = {
        **_deploy_ok(bid),
        # runtime<->env + process<->env reuse; process<->atom list succeeds EMPTY -> attach is tried.
        **_process_attachments("rt-1", "env-1", "CID-1", atom_attached=False),
    }
    deployment["attach_process_atom"] = {
        "_success": False,
        "error": ("Action 'attach_process_atom' failed: This account uses environments. "
                  "Please use ComponentEnvironmentAttachment"),
    }
    dep, env, rt, sch = _patch_all(
        monkeypatch,
        deployment=deployment,
        environments=_ok_env("env-1"),
        runtimes=_ok_runtime("rt-1", "env-1", attached=True),
        schedules={},
    )
    result = orchestrate_deploy_action(
        boomi_client=MagicMock(), build_id=bid,
        environment_id="env-1", runtime_id="rt-1", dry_run=False,
    )
    assert result["_success"] is True
    rta = result["runtime_attachment"]
    assert rta["status"] == "reused"
    assert rta["process_runtime_attachment_status"] == "not_required"
    assert rta["process_runtime_attachment_id"] is None
    # The attach WAS attempted (list returned empty) and returned the environment signal.
    assert "attach_process_atom" in dep.actions_called()
    assert result["schedule"]["status"] == "not_required"
    assert result.get("failed_stage") is None


def test_runtime_attachment_api_failure_is_structured_and_blocks_schedule(registry, monkeypatch):
    bid = registry("b-rt-fail", _single_process_entry(process_id="CID-1"))
    runtimes = _ok_runtime("rt-1", "env-1", attached=False)
    runtimes["attach"] = {"_success": False, "error": "Action 'attach' failed: denied (500)"}
    dep, env, rt, sch = _patch_all(
        monkeypatch,
        deployment={**_deploy_ok(bid), **_process_attachments()},
        environments=_ok_env(),
        runtimes=runtimes,
        schedules={},
    )
    result = orchestrate_deploy_action(
        boomi_client=MagicMock(), build_id=bid,
        environment_id="env-1", runtime_id="rt-1",
        schedule_override={"cron": "0 9 * * *"}, dry_run=False,
    )
    assert result["_success"] is False
    assert "RUNTIME_ENV_ATTACHMENT_CREATE_FAILED" in _codes(result)
    assert result["runtime_attachment"]["status"] == "failed"
    assert result["schedule"]["status"] == "blocked"
    assert result["execution"]["status"] == "blocked"
    assert result["logs"]["status"] == "blocked"
    # Package + deployment were REUSED (existing) and the first attach leg failed before anything
    # was attached, so this attempt created nothing -> cleanup is not_required (a retry reuses).
    assert result["cleanup"]["status"] == "not_required"
    assert result["cleanup"]["operations"] == []
    # Structured failure metadata with prior-stage summary (issue #65).
    assert result["error_code"] == "RUNTIME_ENV_ATTACHMENT_CREATE_FAILED"
    assert result["failed_stage"] == "runtime_attachment"
    assert result["prior_stage_summary"]["package"]["status"] == "reused"
    assert result["prior_stage_summary"]["deployment"]["status"] == "reused"
    assert result["next_step"]
    # Schedule never touched; process bindings never reached (runtime<->env is the first leg).
    assert sch.actions_called() == []
    assert "attach_process_environment" not in dep.actions_called()


def test_missing_runtime_or_environment_blocks_runtime_stage(registry, monkeypatch):
    bid = registry("b-rt-missing", _single_process_entry(process_id="CID-1"))
    # Environment verify fails.
    dep, env, rt, sch = _patch_all(
        monkeypatch,
        deployment={**_deploy_ok(bid), **_process_attachments()},
        environments={"get": {"_success": False, "error": "Environment not found"}},
        runtimes=_ok_runtime(),
        schedules={},
    )
    result = orchestrate_deploy_action(
        boomi_client=MagicMock(), build_id=bid,
        environment_id="env-1", runtime_id="rt-1",
        schedule_override={"cron": "0 9 * * *"}, dry_run=False,
    )
    assert result["_success"] is False
    assert "ENVIRONMENT_VERIFY_FAILED" in _codes(result)
    assert result["runtime_attachment"]["status"] == "failed"
    assert result["schedule"]["status"] == "blocked"
    assert "attach" not in rt.actions_called()
    assert sch.actions_called() == []

    # Runtime verify fails (env ok).
    dep2, env2, rt2, sch2 = _patch_all(
        monkeypatch,
        deployment={**_deploy_ok(bid), **_process_attachments()},
        environments=_ok_env(),
        runtimes={"get": {"_success": False, "error": "Runtime not found"}},
        schedules={},
    )
    result2 = orchestrate_deploy_action(
        boomi_client=MagicMock(), build_id=bid,
        environment_id="env-1", runtime_id="rt-1", dry_run=False,
    )
    assert result2["_success"] is False
    assert "RUNTIME_VERIFY_FAILED" in _codes(result2)
    assert result2["runtime_attachment"]["status"] == "failed"
    assert sch2.actions_called() == []


def test_schedule_override_none_or_manual_has_expected_schedule_calls(registry, monkeypatch):
    bid = registry("b-sched-manual", _single_process_entry(process_id="CID-1"))
    # None override -> no schedule mutation.
    dep, env, rt, sch = _patch_all(
        monkeypatch,
        deployment={**_deploy_ok(bid), **_process_attachments()},
        environments=_ok_env(), runtimes=_ok_runtime(), schedules={},
    )
    result = orchestrate_deploy_action(
        boomi_client=MagicMock(), build_id=bid,
        environment_id="env-1", runtime_id="rt-1", dry_run=False,
    )
    assert result["_success"] is True
    assert result["schedule"]["status"] == "not_required"
    assert sch.actions_called() == []

    # manual mode -> delete (clear) then disable.
    dep2, env2, rt2, sch2 = _patch_all(
        monkeypatch,
        deployment={**_deploy_ok(bid), **_process_attachments()},
        environments=_ok_env(), runtimes=_ok_runtime(),
        schedules={
            "delete": {"_success": True, "schedule": _sched("sch-1")},
            "disable": {"_success": True, "status": _status("sst-1", False)},
        },
    )
    result2 = orchestrate_deploy_action(
        boomi_client=MagicMock(), build_id=bid,
        environment_id="env-1", runtime_id="rt-1",
        schedule_override={"mode": "manual"}, dry_run=False,
    )
    assert result2["_success"] is True
    assert result2["schedule"]["status"] == "disabled"
    assert result2["schedule"]["enabled"] is False
    assert sch2.actions_called() == ["delete", "disable"]
    assert result2["summary"]["schedule_status"] == "disabled"


def test_schedule_override_updates_schedule_after_runtime_binding(registry, monkeypatch):
    bid = registry("b-sched-update", _single_process_entry(process_id="CID-1"))
    order = []
    dep, env, rt, sch = _patch_all(
        monkeypatch,
        deployment={**_deploy_ok(bid), **_process_attachments()},
        environments=_ok_env(), runtimes=_ok_runtime(),
        schedules={
            "update": {"_success": True, "schedule": _sched("sch-9")},
            "enable": {"_success": True, "status": _status("sst-9", True)},
        },
        order_log=order,
    )
    result = orchestrate_deploy_action(
        boomi_client=MagicMock(), build_id=bid,
        environment_id="env-1", runtime_id="rt-1",
        schedule_override={"cron": "0 9 * * *"}, dry_run=False,
    )
    assert result["_success"] is True
    sched_stage = result["schedule"]
    assert sched_stage["status"] == "enabled"
    assert sched_stage["schedule_id"] == "sch-9"
    assert sched_stage["schedule_status_id"] == "sst-9"
    assert sched_stage["cron"] == "0 9 * * *"
    assert sched_stage["enabled"] is True
    assert sch.actions_called() == ["update", "enable"]
    assert result["summary"]["schedule_id"] == "sch-9"
    # The update call carries the process/atom/cron/max_retry the schedule router expects.
    update_call = next(c for c in sch.calls if c["action"] == "update")
    assert update_call["config_data"]["process_id"] == "CID-1"
    assert update_call["config_data"]["atom_id"] == "rt-1"
    assert update_call["config_data"]["cron"] == "0 9 * * *"
    assert update_call["config_data"]["max_retry"] == 5
    # Schedule runs after runtime/process binding.
    first_sched = min(i for i, (label, _a) in enumerate(order) if label == "schedules")
    assert first_sched > _last_bind_index(order)


def test_schedule_override_enable_disable_status_flows(registry, monkeypatch):
    bid = registry("b-sched-status", _single_process_entry(process_id="CID-1"))
    # scheduled + enabled:false -> update then disable.
    dep, env, rt, sch = _patch_all(
        monkeypatch,
        deployment={**_deploy_ok(bid), **_process_attachments()},
        environments=_ok_env(), runtimes=_ok_runtime(),
        schedules={
            "update": {"_success": True, "schedule": _sched("sch-1")},
            "disable": {"_success": True, "status": _status("sst-1", False)},
        },
    )
    disabled = orchestrate_deploy_action(
        boomi_client=MagicMock(), build_id=bid,
        environment_id="env-1", runtime_id="rt-1",
        schedule_override={"mode": "scheduled", "cron": "0 9 * * *", "enabled": False},
        dry_run=False,
    )
    assert disabled["_success"] is True
    assert disabled["schedule"]["status"] == "disabled"
    assert disabled["schedule"]["enabled"] is False
    assert sch.actions_called() == ["update", "disable"]

    # scheduled + enabled:true -> update then enable.
    dep2, env2, rt2, sch2 = _patch_all(
        monkeypatch,
        deployment={**_deploy_ok(bid), **_process_attachments()},
        environments=_ok_env(), runtimes=_ok_runtime(),
        schedules={
            "update": {"_success": True, "schedule": _sched("sch-1")},
            "enable": {"_success": True, "status": _status("sst-1", True)},
        },
    )
    enabled = orchestrate_deploy_action(
        boomi_client=MagicMock(), build_id=bid,
        environment_id="env-1", runtime_id="rt-1",
        schedule_override={"mode": "scheduled", "cron": "0 9 * * *", "enabled": True},
        dry_run=False,
    )
    assert enabled["_success"] is True
    assert enabled["schedule"]["status"] == "enabled"
    assert enabled["schedule"]["enabled"] is True
    assert sch2.actions_called() == ["update", "enable"]


def test_invalid_schedule_override_returns_structured_error_without_schedule_call(registry, monkeypatch):
    bid = registry("b-sched-invalid", _single_process_entry(process_id="CID-1"))
    # Every router raises on ANY call, proving validation fails before any SDK work.
    dep, env, rt, sch = _patch_all(
        monkeypatch, deployment={}, environments={}, runtimes={}, schedules={},
    )
    for bad in (
        {"mode": "weird"},
        {"cron": "0 9 * *"},
        {"cron": "0 9 * * *", "max_retry": 9},
        {"mode": "manual", "max_retry": 3},   # max_retry is not applicable to disable/manual
        {"mode": "disabled", "max_retry": "bad"},
    ):
        result = orchestrate_deploy_action(
            boomi_client=MagicMock(), build_id=bid,
            environment_id="env-1", runtime_id="rt-1",
            schedule_override=bad, dry_run=False,
        )
        assert result["_success"] is False
        assert _codes(result) == ["SCHEDULE_OVERRIDE_INVALID"]
    assert dep.actions_called() == []
    assert env.actions_called() == []
    assert rt.actions_called() == []
    assert sch.actions_called() == []


def test_schedule_api_failure_is_structured_and_blocks_execution(registry, monkeypatch):
    bid = registry("b-sched-apifail", _single_process_entry(process_id="CID-1"))
    dep, env, rt, sch = _patch_all(
        monkeypatch,
        deployment={**_deploy_ok(bid), **_process_attachments()},
        environments=_ok_env(), runtimes=_ok_runtime(),
        schedules={"update": {"_success": False, "error": "Action 'update' failed: denied"}},
    )
    result = orchestrate_deploy_action(
        boomi_client=MagicMock(), build_id=bid,
        environment_id="env-1", runtime_id="rt-1",
        schedule_override={"cron": "0 9 * * *"}, dry_run=False,
    )
    assert result["_success"] is False
    assert "SCHEDULE_UPDATE_FAILED" in _codes(result)
    assert result["schedule"]["status"] == "failed"
    # The very first schedule call failed -> nothing mutated -> changed must be False.
    assert result["schedule"]["changed"] is False
    assert result["summary"]["resource_changes"]["schedule"] is False
    # Runtime binding completed before the schedule failed -> preserved.
    assert result["runtime_attachment"]["status"] == "reused"
    assert result["execution"]["status"] == "blocked"
    assert result["logs"]["status"] == "blocked"
    # Package/deploy/attachments all reused, and the first schedule call failed (changed=False),
    # so nothing this attempt created needs undoing -> cleanup not_required.
    assert result["cleanup"]["status"] == "not_required"
    assert result["cleanup"]["operations"] == []
    # Structured failure metadata: schedule stage, with the prior stages summarized (issue #65).
    assert result["error_code"] == "SCHEDULE_UPDATE_FAILED"
    assert result["failed_stage"] == "schedule"
    assert set(result["prior_stage_summary"]) >= {"package", "deployment", "runtime_attachment"}
    assert result["next_step"]
    assert sch.actions_called() == ["update"]  # failed before enable


def test_dry_run_with_schedule_override_never_calls_runtime_or_schedule_helpers(registry, monkeypatch):
    bid = registry("b-dry-sched", _single_process_entry(process_id="CID-1"))
    # Every router (and the client) explodes if touched.
    dep, env, rt, sch = _patch_all(
        monkeypatch, deployment={}, environments={}, runtimes={}, schedules={},
    )
    result = orchestrate_deploy_action(
        boomi_client=_ExplodingClient(), build_id=bid,
        environment_id="env-1", runtime_id="rt-1",
        schedule_override={"cron": "0 9 * * *"}, dry_run=True,
    )
    assert result["_success"] is True
    assert result["dry_run"] is True
    assert result["schedule"]["status"] == "planned"
    assert dep.actions_called() == []
    assert env.actions_called() == []
    assert rt.actions_called() == []
    assert sch.actions_called() == []


def test_deploy_failure_blocks_runtime_and_schedule_without_schedule_calls(registry, monkeypatch):
    bid = registry("b-deploy-blocks-rt", _single_process_entry(process_id="CID-1"))
    dep, env, rt, sch = _patch_all(
        monkeypatch,
        deployment={
            "list_packages": {"_success": True, "packages": [_pkg("pkg-1", bid)]},
            "list_deployments": {"_success": True, "deployments": []},
            "deploy": {"_success": False, "error": "Action 'deploy' failed: denied"},
        },
        environments={}, runtimes={}, schedules={},
    )
    result = orchestrate_deploy_action(
        boomi_client=MagicMock(), build_id=bid,
        environment_id="env-1", runtime_id="rt-1",
        schedule_override={"cron": "0 9 * * *"}, dry_run=False,
    )
    assert result["_success"] is False
    assert "DEPLOY_CREATE_FAILED" in _codes(result)
    assert result["runtime_attachment"]["status"] == "blocked"
    assert result["schedule"]["status"] == "blocked"
    # Runtime/schedule routers never touched when deploy fails.
    assert env.actions_called() == []
    assert rt.actions_called() == []
    assert sch.actions_called() == []


def test_runtime_partial_attachment_preserved_on_later_leg_failure(registry, monkeypatch):
    # Legs 1 (runtime↔env) and 2 (process↔env) create real attachments, then leg 3
    # (process↔runtime) fails. The failed stage must still surface the two created ids and
    # report changed=True so a caller knows a retry/cleanup may be needed.
    bid = registry("b-rt-partial", _single_process_entry(process_id="CID-1"))
    deployment = {
        **_deploy_ok(bid),
        **_process_attachments("rt-1", "env-1", "CID-1", env_attached=False, atom_attached=False),
    }
    deployment["attach_process_atom"] = {
        "_success": False, "error": "Action 'attach_process_atom' failed: denied",
    }
    dep, env, rt, sch = _patch_all(
        monkeypatch,
        deployment=deployment,
        environments=_ok_env("env-1"),
        runtimes=_ok_runtime("rt-1", "env-1", attached=False),
        schedules={},
    )
    result = orchestrate_deploy_action(
        boomi_client=MagicMock(), build_id=bid,
        environment_id="env-1", runtime_id="rt-1", dry_run=False,
    )
    assert result["_success"] is False
    assert "PROCESS_RUNTIME_ATTACHMENT_CREATE_FAILED" in _codes(result)
    rta = result["runtime_attachment"]
    assert rta["status"] == "failed"
    assert rta["changed"] is True  # legs 1+2 created real attachments
    assert rta["reused"] is False
    assert rta["runtime_env_attachment_id"] == "ea-new"
    assert rta["runtime_env_attachment_status"] == "attached"
    assert rta["process_env_attachment_id"] == "pe-new"
    assert rta["process_env_attachment_status"] == "attached"
    assert rta["process_runtime_attachment_id"] is None
    assert rta["attachment_id"] == "ea-new"  # alias preserved on failure
    assert result["summary"]["resource_changes"]["runtime_attachment"] is True
    assert result["schedule"]["status"] == "blocked"


def test_runtime_attachment_id_missing_after_create_reports_changed(registry, monkeypatch):
    # The runtime↔env attach SUCCEEDS but returns no id -> *_ID_MISSING. The account was still
    # mutated, so the failed stage must report changed=True (not silently False).
    bid = registry("b-rt-idmissing", _single_process_entry(process_id="CID-1"))
    runtimes = _ok_runtime("rt-1", "env-1", attached=False)
    runtimes["attach"] = {"_success": True, "attachment": {}}  # created, but no id returned
    dep, env, rt, sch = _patch_all(
        monkeypatch,
        deployment={**_deploy_ok(bid), **_process_attachments()},
        environments=_ok_env("env-1"),
        runtimes=runtimes,
        schedules={},
    )
    result = orchestrate_deploy_action(
        boomi_client=MagicMock(), build_id=bid,
        environment_id="env-1", runtime_id="rt-1", dry_run=False,
    )
    assert result["_success"] is False
    assert "RUNTIME_ENV_ATTACHMENT_ID_MISSING" in _codes(result)
    rta = result["runtime_attachment"]
    assert rta["status"] == "failed"
    assert rta["changed"] is True
    assert rta["runtime_env_attachment_status"] == "attached"
    assert rta["runtime_env_attachment_id"] is None
    assert result["summary"]["resource_changes"]["runtime_attachment"] is True
    assert result["schedule"]["status"] == "blocked"
    assert sch.actions_called() == []
    # #129 D5: the attach mutated the account but returned no id, so cleanup CANNOT plan an
    # executable detach-by-id (a resource_id=None op is rejected by the detach handler). Instead
    # of emitting a bogus op, cleanup reports status=warning with manual-intervention guidance and
    # NO operation carrying a null resource_id.
    cleanup = result["cleanup"]
    assert cleanup["status"] == "warning"
    assert cleanup["operations"] == []
    assert cleanup["warnings"]
    assert any(
        "manual" in w.lower() or "re-list" in w.lower() for w in cleanup["warnings"]
    )
    assert all(op["resource_id"] is not None for op in cleanup["operations"])


def test_schedule_changed_flag_reflects_failed_mutation(registry, monkeypatch):
    bid = registry("b-sched-changed", _single_process_entry(process_id="CID-1"))
    # First call (update) fails -> no mutation landed -> changed False.
    _patch_all(
        monkeypatch,
        deployment={**_deploy_ok(bid), **_process_attachments()},
        environments=_ok_env(), runtimes=_ok_runtime(),
        schedules={"update": {"_success": False, "error": "denied"}},
    )
    r1 = orchestrate_deploy_action(
        boomi_client=MagicMock(), build_id=bid,
        environment_id="env-1", runtime_id="rt-1",
        schedule_override={"cron": "0 9 * * *"}, dry_run=False,
    )
    assert "SCHEDULE_UPDATE_FAILED" in _codes(r1)
    assert r1["schedule"]["changed"] is False
    assert r1["summary"]["resource_changes"]["schedule"] is False

    # delete succeeds, then disable fails -> a mutation already landed -> changed True.
    _patch_all(
        monkeypatch,
        deployment={**_deploy_ok(bid), **_process_attachments()},
        environments=_ok_env(), runtimes=_ok_runtime(),
        schedules={
            "delete": {"_success": True, "schedule": _sched("sch-1")},
            "disable": {"_success": False, "error": "denied"},
        },
    )
    r2 = orchestrate_deploy_action(
        boomi_client=MagicMock(), build_id=bid,
        environment_id="env-1", runtime_id="rt-1",
        schedule_override={"mode": "manual"}, dry_run=False,
    )
    assert "SCHEDULE_DISABLE_FAILED" in _codes(r2)
    assert r2["schedule"]["changed"] is True
    assert r2["summary"]["resource_changes"]["schedule"] is True


def test_schedule_missing_status_id_returns_structured_error(registry, monkeypatch):
    bid = registry("b-sched-noid", _single_process_entry(process_id="CID-1"))
    # scheduled: update ok, but enable returns success with no status id -> SCHEDULE_ID_MISSING.
    _patch_all(
        monkeypatch,
        deployment={**_deploy_ok(bid), **_process_attachments()},
        environments=_ok_env(), runtimes=_ok_runtime(),
        schedules={
            "update": {"_success": True, "schedule": _sched("sch-1")},
            "enable": {"_success": True, "status": {}},  # no id returned
        },
    )
    scheduled = orchestrate_deploy_action(
        boomi_client=MagicMock(), build_id=bid,
        environment_id="env-1", runtime_id="rt-1",
        schedule_override={"cron": "0 9 * * *"}, dry_run=False,
    )
    assert scheduled["_success"] is False
    assert "SCHEDULE_ID_MISSING" in _codes(scheduled)
    assert scheduled["schedule"]["status"] == "failed"
    assert scheduled["schedule"]["changed"] is True  # update already mutated the schedule

    # manual: delete ok, but disable returns success with no status id -> SCHEDULE_ID_MISSING.
    _patch_all(
        monkeypatch,
        deployment={**_deploy_ok(bid), **_process_attachments()},
        environments=_ok_env(), runtimes=_ok_runtime(),
        schedules={
            "delete": {"_success": True, "schedule": _sched("sch-1")},
            "disable": {"_success": True, "status": {}},  # no id returned
        },
    )
    manual = orchestrate_deploy_action(
        boomi_client=MagicMock(), build_id=bid,
        environment_id="env-1", runtime_id="rt-1",
        schedule_override={"mode": "manual"}, dry_run=False,
    )
    assert manual["_success"] is False
    assert "SCHEDULE_ID_MISSING" in _codes(manual)
    assert manual["schedule"]["status"] == "failed"
    assert manual["schedule"]["changed"] is True  # delete already mutated the schedule


# ---------------------------------------------------------------------------
# Typed-contract sanity
# ---------------------------------------------------------------------------
def test_error_code_constants_match_module():
    assert orchestration.BUILD_ID_REQUIRED == "BUILD_ID_REQUIRED"
    assert orchestration.BUILD_MULTIPLE_PROCESS_COMPONENTS == "BUILD_MULTIPLE_PROCESS_COMPONENTS"
    assert orchestration.DEPLOY_AMBIGUOUS_EXISTING == "DEPLOY_AMBIGUOUS_EXISTING"
    assert orchestration.PACKAGE_CREATE_FAILED == "PACKAGE_CREATE_FAILED"
    assert orchestration.RUNTIME_ENV_ATTACHMENT_CREATE_FAILED == "RUNTIME_ENV_ATTACHMENT_CREATE_FAILED"
    assert orchestration.SCHEDULE_OVERRIDE_INVALID == "SCHEDULE_OVERRIDE_INVALID"
    assert orchestration.SCHEDULE_UPDATE_FAILED == "SCHEDULE_UPDATE_FAILED"
    # Run-test stage codes (issue #63).
    assert orchestration.TEST_EXECUTION_FAILED == "TEST_EXECUTION_FAILED"
    assert orchestration.TEST_EXECUTION_TIMEOUT == "TEST_EXECUTION_TIMEOUT"
    assert orchestration.TEST_REQUEST_ID_MISSING == "TEST_REQUEST_ID_MISSING"
    # Failure-hardening + cleanup codes (issue #65).
    assert orchestration.LOG_RETRIEVAL_FAILED == "LOG_RETRIEVAL_FAILED"
    assert orchestration.ARTIFACT_RETRIEVAL_FAILED == "ARTIFACT_RETRIEVAL_FAILED"
    assert orchestration.CLEANUP_OPERATION_FAILED == "CLEANUP_OPERATION_FAILED"
    # Behavioral-verification opt-in code (issue #81).
    assert orchestration.TEST_LOGS_UNAVAILABLE == "TEST_LOGS_UNAVAILABLE"
    assert orchestration._ERROR_CODE_STAGES["TEST_LOGS_UNAVAILABLE"] == "logs"


# ===========================================================================
# Run-test stage (issue #63): optional execution + log/artifact diagnostics
# ===========================================================================
class _FakeExecuteAction:
    """Records ``execute_process_action`` calls and returns one canned response dict.

    Mirrors the real ``execute_process_action`` keyword shape
    (``sdk, profile, process_id, environment_id, atom_id, config_data``) so orchestration's
    run-test stage exercises the dict-inspection contract without a live SDK.
    """

    def __init__(self, result, *, order_log=None):
        self.result = result
        self.order_log = order_log
        self.calls = []

    def __call__(self, sdk=None, profile=None, process_id=None,
                 environment_id=None, atom_id=None, config_data=None, **kwargs):
        self.calls.append({
            "process_id": process_id,
            "environment_id": environment_id,
            "atom_id": atom_id,
            "config_data": config_data,
        })
        if self.order_log is not None:
            self.order_log.append(("execute", "execute_process"))
        return self.result


class _FakeMonitorAction:
    """Records ``monitor_platform_action`` calls keyed by action and returns canned responses."""

    def __init__(self, responses, *, order_log=None):
        self.responses = responses
        self.order_log = order_log
        self.calls = []

    def __call__(self, boomi_client=None, profile=None, action=None,
                 config_data=None, creds=None, **kwargs):
        self.calls.append({"action": action, "config_data": config_data, "creds": creds})
        if self.order_log is not None:
            self.order_log.append(("monitor", action))
        if action not in self.responses:
            raise AssertionError(f"unexpected monitor action call: {action}")
        return self.responses[action]

    def actions_called(self):
        return [c["action"] for c in self.calls]


def _patch_test_actions(monkeypatch, *, execute, monitor):
    """Patch ``orchestration.execute_process_action`` and ``orchestration.monitor_platform_action``."""
    monkeypatch.setattr(orchestration, "execute_process_action", execute)
    monkeypatch.setattr(orchestration, "monitor_platform_action", monitor)
    return execute, monitor


def _exec_complete(execution_id="ex-1", *, status="COMPLETE", request_id="req-1",
                   docs=(1, 1, 0), elapsed=3.0, poll_count=2):
    """An ``execute_process_action`` (wait=True) response for a terminal COMPLETE/COMPLETE_WARN run."""
    success = status.upper() not in ("ERROR", "ABORTED")
    result = {
        "_success": success,
        "request_id": request_id,
        "process_id": "CID-1",
        "environment_id": "env-1",
        "atom_id": "rt-1",
        "execution_result": {
            "poll_status": "COMPLETED",
            "elapsed_seconds": elapsed,
            "poll_count": poll_count,
            "execution_id": execution_id,
            "status": status,
            "inbound_document_count": docs[0],
            "outbound_document_count": docs[1],
            "inbound_error_document_count": docs[2],
            "error": None,
        },
    }
    if execution_id:
        result["execution_id"] = execution_id
    return result


def _exec_warn(execution_id="ex-warn", **kwargs):
    return _exec_complete(execution_id=execution_id, status="COMPLETE_WARN", **kwargs)


def _exec_failed(status="ERROR", *, execution_id="ex-err", request_id="req-1",
                 error="boom", docs=(1, 0, 1)):
    """A failed terminal run (ERROR/ABORTED): ``_success=False`` but an execution_id exists."""
    return {
        "_success": False,
        "error": error,
        "request_id": request_id,
        "process_id": "CID-1",
        "environment_id": "env-1",
        "atom_id": "rt-1",
        "execution_id": execution_id,
        "execution_result": {
            "poll_status": "COMPLETED",
            "elapsed_seconds": 2.0,
            "poll_count": 1,
            "execution_id": execution_id,
            "status": status,
            "inbound_document_count": docs[0],
            "outbound_document_count": docs[1],
            "inbound_error_document_count": docs[2],
            "error": error,
        },
    }


def _exec_timeout(*, request_id="req-1", message="Timed out after 300s waiting",
                  elapsed=300.0, poll_count=10):
    """A timeout response: poll_status TIMEOUT, no execution_id."""
    return {
        "_success": False,
        "error": message,
        "request_id": request_id,
        "process_id": "CID-1",
        "environment_id": "env-1",
        "atom_id": "rt-1",
        "execution_result": {
            "poll_status": "TIMEOUT",
            "elapsed_seconds": elapsed,
            "poll_count": poll_count,
            "message": message,
        },
    }


def _exec_no_request_id():
    """The "accepted but no request_id came back" failure: no request_id, no execution_result."""
    return {
        "_success": False,
        "error": "Execution request accepted but no request_id returned.",
    }


def _exec_setup_failed(error="dynamic_properties must be a dict of {key: value}"):
    """A pre-request execute failure (bad properties / API setup error): no request_id, no poll."""
    return {"_success": False, "error": error}


def _logs_ok(files=None, *, status_code=202, download_url="https://logs.example/dl",
             downloaded=True):
    if files is None:
        files = {"process.log": "line1\nline2\nline3"}
    return {
        "_success": True,
        "status_code": status_code,
        "message": "Log download initiated",
        "download_url": download_url,
        "_downloaded": downloaded,
        "files": files,
    }


def _logs_fail(error="Runtime unavailable — the Atom may be offline", *, status_code=504):
    return {"_success": False, "status_code": status_code, "message": "", "error": error}


def _logs_download_failed(*, download_url="https://logs.example/dl",
                          error="Download failed with HTTP 500 after polling"):
    """A log result whose URL was created (202) but content download/extract failed.

    ``handle_execution_logs`` merges ``_download_and_extract_zip``'s ``{_downloaded: False, error}``
    onto an already ``_success: True`` dict, so ``_success`` stays True while content failed.
    """
    return {
        "_success": True,
        "status_code": 202,
        "message": "Log download initiated",
        "download_url": download_url,
        "_downloaded": False,
        "error": error,
    }


def _artifacts_download_failed(*, download_url="https://artifacts.example/dl",
                               error="ZIP too large (… bytes, limit …)"):
    return {
        "_success": True,
        "status_code": 200,
        "message": "",
        "download_url": download_url,
        "_downloaded": False,
        "error": error,
    }


def _artifacts_ok(download_url="https://artifacts.example/dl", *, status_code=200):
    return {
        "_success": True,
        "status_code": status_code,
        "message": "",
        "download_url": download_url,
        "_downloaded": True,
        "files": {"artifact.json": "{}"},
    }


def _monitor_ok():
    """A monitor fake serving successful logs + artifacts."""
    return _FakeMonitorAction(
        {"execution_logs": _logs_ok(), "execution_artifacts": _artifacts_ok()}
    )


def _seed_real_run(registry, monkeypatch, bid, *, order_log=None, schedules=None):
    """Seed a single-process build and patch all routers for a clean real run that reaches 3g."""
    seeded = registry(bid, _single_process_entry(process_id="CID-1"))
    _patch_all(
        monkeypatch,
        deployment={**_deploy_ok(seeded), **_process_attachments("rt-1", "env-1", "CID-1")},
        environments=_ok_env("env-1"),
        runtimes=_ok_runtime("rt-1", "env-1", attached=True),
        schedules=schedules if schedules is not None else {},
        order_log=order_log,
    )
    return seeded


def test_run_test_false_real_run_skips_execution_and_logs(registry, monkeypatch):
    bid = _seed_real_run(registry, monkeypatch, "b-rt-false")
    execute = _FakeExecuteAction(_exec_complete())
    monitor = _monitor_ok()
    _patch_test_actions(monkeypatch, execute=execute, monitor=monitor)
    result = orchestrate_deploy_action(
        boomi_client=MagicMock(), build_id=bid,
        environment_id="env-1", runtime_id="rt-1", run_test=False, dry_run=False,
    )
    assert result["_success"] is True
    assert result["execution"]["status"] == "skipped"
    assert result["logs"]["status"] == "skipped"
    assert execute.calls == []
    assert monitor.calls == []
    # No run-test stage ran -> no test sub-summary.
    assert "test" not in result["summary"]


def test_run_test_dry_run_plans_without_execution_or_log_calls(registry, monkeypatch):
    bid = registry("b-rt-dry", _single_process_entry(process_id="CID-1"))
    execute = _FakeExecuteAction(_exec_complete())
    monitor = _monitor_ok()
    _patch_test_actions(monkeypatch, execute=execute, monitor=monitor)
    result = orchestrate_deploy_action(
        boomi_client=_ExplodingClient(), build_id=bid,
        environment_id="env-1", runtime_id="rt-1", run_test=True, dry_run=True,
    )
    assert result["_success"] is True
    assert result["dry_run"] is True
    assert result["execution"]["status"] == "planned"
    assert result["execution"]["run_test"] is True
    assert result["logs"]["status"] == "planned"
    assert execute.calls == []
    assert monitor.calls == []


def test_run_test_success_executes_after_schedule_and_fetches_diagnostics(registry, monkeypatch):
    order = []
    bid = _seed_real_run(
        registry, monkeypatch, "b-rt-ok", order_log=order,
        schedules={
            "update": {"_success": True, "schedule": _sched("sch-1")},
            "enable": {"_success": True, "status": _status("sst-1", True)},
        },
    )
    execute = _FakeExecuteAction(_exec_complete(docs=(2, 3, 0)), order_log=order)
    monitor = _FakeMonitorAction(
        {"execution_logs": _logs_ok(), "execution_artifacts": _artifacts_ok()},
        order_log=order,
    )
    _patch_test_actions(monkeypatch, execute=execute, monitor=monitor)
    result = orchestrate_deploy_action(
        boomi_client=MagicMock(), build_id=bid,
        environment_id="env-1", runtime_id="rt-1",
        schedule_override={"cron": "0 9 * * *"}, run_test=True, dry_run=False,
    )
    assert result["_success"] is True
    ex = result["execution"]
    assert ex["status"] == "completed"
    assert ex["terminal_status"] == "COMPLETE"
    assert ex["request_id"] == "req-1"
    assert ex["execution_id"] == "ex-1"
    assert ex["elapsed_seconds"] == 3.0  # poll metadata surfaced (acceptance: elapsed/poll count)
    assert ex["poll_count"] == 2
    assert ex["document_counts"] == {"inbound": 2, "outbound": 3, "inbound_error": 0}
    logs = result["logs"]
    assert logs["status"] == "retrieved"
    assert logs["log_excerpts"]  # non-empty
    assert logs["download_url"] == "https://logs.example/dl"  # log download pointer surfaced
    assert logs["artifact_download_url"] == "https://artifacts.example/dl"
    # Execute called once, with the resolved process/runtime/environment and forced wait.
    assert len(execute.calls) == 1
    call = execute.calls[0]
    assert call["process_id"] == "CID-1"
    assert call["atom_id"] == "rt-1"
    assert call["environment_id"] == "env-1"
    assert call["config_data"]["wait"] is True
    assert call["config_data"]["timeout"] == 300
    # Both diagnostics fetched once with the resolved execution_id.
    assert monitor.actions_called().count("execution_logs") == 1
    assert monitor.actions_called().count("execution_artifacts") == 1
    assert monitor.calls[0]["config_data"]["execution_id"] == "ex-1"
    # Execution ran strictly after every schedule call.
    last_sched = max(i for i, (label, _a) in enumerate(order) if label == "schedules")
    first_exec = min(i for i, (label, _a) in enumerate(order) if label == "execute")
    assert first_exec > last_sched
    # Summary surfaces the test outcome.
    assert result["summary"]["test"]["execution_status"] == "completed"
    assert result["summary"]["test"]["logs_status"] == "retrieved"


def test_run_test_complete_warn_is_success_with_warning_summary(registry, monkeypatch):
    bid = _seed_real_run(registry, monkeypatch, "b-rt-warn")
    execute = _FakeExecuteAction(_exec_warn())
    monitor = _monitor_ok()
    _patch_test_actions(monkeypatch, execute=execute, monitor=monitor)
    result = orchestrate_deploy_action(
        boomi_client=MagicMock(), build_id=bid,
        environment_id="env-1", runtime_id="rt-1", run_test=True, dry_run=False,
    )
    assert result["_success"] is True
    ex = result["execution"]
    assert ex["status"] == "warning"
    assert ex["terminal_status"] == "COMPLETE_WARN"
    assert ex["warnings"]  # a COMPLETE_WARN warning surfaced on the stage
    assert result["warnings"]  # and bubbled to the top-level warnings
    # COMPLETE_WARN is still a non-failing terminal status -> diagnostics fetched.
    assert result["logs"]["status"] == "retrieved"
    assert result["summary"]["test"]["execution_status"] == "warning"
    assert result["summary"]["test"]["terminal_status"] == "COMPLETE_WARN"


@pytest.mark.parametrize("status", ["ERROR", "ABORTED"])
def test_run_test_error_and_aborted_fail_with_terminal_details(registry, monkeypatch, status):
    bid = _seed_real_run(registry, monkeypatch, f"b-rt-{status.lower()}")
    execute = _FakeExecuteAction(_exec_failed(status=status, error="kaboom", docs=(1, 0, 1)))
    monitor = _monitor_ok()
    _patch_test_actions(monkeypatch, execute=execute, monitor=monitor)
    result = orchestrate_deploy_action(
        boomi_client=MagicMock(), build_id=bid,
        environment_id="env-1", runtime_id="rt-1", run_test=True, dry_run=False,
    )
    assert result["_success"] is False
    assert "TEST_EXECUTION_FAILED" in _codes(result)
    ex = result["execution"]
    assert ex["status"] == "failed"
    assert ex["terminal_status"] == status
    assert ex["error"] == "kaboom"
    assert ex["document_counts"] == {"inbound": 1, "outbound": 0, "inbound_error": 1}
    # Prior stages preserved (not blocked) — only the test stage failed.
    assert result["package"]["status"] in ("reused", "created", "deployed")
    assert result["deployment"]["status"] in ("reused", "deployed")
    assert result["runtime_attachment"]["status"] in ("reused", "attached")
    # Logs are the whole point of a failed test run — fetched since an execution_id exists.
    assert result["logs"]["status"] == "retrieved"
    assert monitor.actions_called().count("execution_logs") == 1


def test_run_test_timeout_fails_and_does_not_fetch_logs_without_execution_id(registry, monkeypatch):
    bid = _seed_real_run(registry, monkeypatch, "b-rt-timeout")
    execute = _FakeExecuteAction(_exec_timeout())
    monitor = _monitor_ok()
    _patch_test_actions(monkeypatch, execute=execute, monitor=monitor)
    result = orchestrate_deploy_action(
        boomi_client=MagicMock(), build_id=bid,
        environment_id="env-1", runtime_id="rt-1", run_test=True, dry_run=False,
    )
    assert result["_success"] is False
    assert "TEST_EXECUTION_TIMEOUT" in _codes(result)
    ex = result["execution"]
    assert ex["status"] == "timeout"
    assert ex["poll_status"] == "TIMEOUT"
    assert ex["execution_id"] is None
    assert ex["elapsed_seconds"] == 300.0
    assert result["logs"]["status"] == "blocked"
    assert monitor.calls == []  # nothing to fetch without an execution_id


def test_run_test_no_execution_id_skips_log_fetch_but_keeps_request_id(registry, monkeypatch):
    # Variant A: the request was never accepted -> no request_id -> hard failure.
    bid = _seed_real_run(registry, monkeypatch, "b-rt-noreq")
    execute = _FakeExecuteAction(_exec_no_request_id())
    monitor = _monitor_ok()
    _patch_test_actions(monkeypatch, execute=execute, monitor=monitor)
    result = orchestrate_deploy_action(
        boomi_client=MagicMock(), build_id=bid,
        environment_id="env-1", runtime_id="rt-1", run_test=True, dry_run=False,
    )
    assert result["_success"] is False
    assert "TEST_REQUEST_ID_MISSING" in _codes(result)
    assert result["execution"]["status"] == "failed"
    assert result["logs"]["status"] == "blocked"
    assert monitor.calls == []

    # Variant B: completed terminal status but no execution_id -> request_id kept, logs not fetched.
    bid2 = _seed_real_run(registry, monkeypatch, "b-rt-noexecid")
    execute2 = _FakeExecuteAction(_exec_complete(execution_id=None))
    monitor2 = _monitor_ok()
    _patch_test_actions(monkeypatch, execute=execute2, monitor=monitor2)
    result2 = orchestrate_deploy_action(
        boomi_client=MagicMock(), build_id=bid2,
        environment_id="env-1", runtime_id="rt-1", run_test=True, dry_run=False,
    )
    assert result2["_success"] is True
    assert result2["execution"]["status"] == "completed"
    assert result2["execution"]["request_id"] == "req-1"
    assert result2["execution"]["execution_id"] is None
    assert result2["logs"]["status"] != "retrieved"
    assert monitor2.calls == []  # can't fetch logs without an execution_id
    assert result2["summary"]["test"]["request_id"] == "req-1"
    assert result2["summary"]["test"]["execution_id"] is None


def test_run_test_log_fetch_success_is_bounded(registry, monkeypatch):
    bid = _seed_real_run(registry, monkeypatch, "b-rt-bounded")
    big_lines = "\n".join(f"line {i}" for i in range(200))  # > 80 lines
    long_line = "x" * 9000  # > 8000 chars in a single line
    files = {
        "a.log": big_lines,
        "b.log": long_line,
        "c.log": "small",
        "d.log": "dropped-1",  # 4th + 5th file are dropped (max 3)
        "e.log": "dropped-2",
    }
    execute = _FakeExecuteAction(_exec_complete())
    monitor = _FakeMonitorAction(
        {"execution_logs": _logs_ok(files=files), "execution_artifacts": _artifacts_ok()}
    )
    _patch_test_actions(monkeypatch, execute=execute, monitor=monitor)
    result = orchestrate_deploy_action(
        boomi_client=MagicMock(), build_id=bid,
        environment_id="env-1", runtime_id="rt-1", run_test=True, dry_run=False,
    )
    excerpts = result["logs"]["log_excerpts"]
    assert len(excerpts) == 3  # capped to _RUN_TEST_LOG_MAX_FILES
    # Line-bounded excerpt: at most 80 content lines + name prefix + truncation marker.
    assert len(excerpts[0].splitlines()) <= 80 + 2
    assert "[truncated]" in excerpts[0]
    # Char-bounded excerpt: content clipped to ~8000 chars (+ small prefix/suffix).
    assert len(excerpts[1]) < 8100
    assert "[truncated]" in excerpts[1]
    assert result["summary"]["test"]["log_excerpt_count"] == 3


def test_run_test_log_fetch_unavailable_is_diagnostic_not_execution_failure(registry, monkeypatch):
    bid = _seed_real_run(registry, monkeypatch, "b-rt-logfail")
    execute = _FakeExecuteAction(_exec_complete())
    monitor = _FakeMonitorAction(
        {"execution_logs": _logs_fail(), "execution_artifacts": _artifacts_ok()}
    )
    _patch_test_actions(monkeypatch, execute=execute, monitor=monitor)
    result = orchestrate_deploy_action(
        boomi_client=MagicMock(), build_id=bid,
        environment_id="env-1", runtime_id="rt-1", run_test=True, dry_run=False,
    )
    # The test execution succeeded; a log-fetch failure is diagnostic, never an exec failure.
    assert result["_success"] is True
    assert result["execution"]["status"] == "completed"
    assert result["logs"]["status"] == "unavailable"
    assert result["logs"]["error"]
    assert not any(c.startswith("TEST_") for c in _codes(result))
    assert result["summary"]["test"]["logs_status"] == "unavailable"
    assert result["summary"]["test"]["log_error"]


# ===========================================================================
# require_test_logs + behavior_verified marker (issue #81)
# ===========================================================================
def test_require_test_logs_true_log_failure_fails_with_structured_error(registry, monkeypatch):
    # A successful test execution whose log fetch is unavailable, with require_test_logs=true,
    # promotes the diagnostic to a TEST_LOGS_UNAVAILABLE orchestration failure (issue #81).
    bid = _seed_real_run(registry, monkeypatch, "b-rt-reqlogs-fail")
    execute = _FakeExecuteAction(_exec_complete())
    monitor = _FakeMonitorAction(
        {"execution_logs": _logs_fail(), "execution_artifacts": _artifacts_ok()}
    )
    _patch_test_actions(monkeypatch, execute=execute, monitor=monitor)
    result = orchestrate_deploy_action(
        boomi_client=MagicMock(), build_id=bid,
        environment_id="env-1", runtime_id="rt-1",
        run_test=True, dry_run=False, require_test_logs=True,
    )
    assert result["_success"] is False
    assert "TEST_LOGS_UNAVAILABLE" in _codes(result)
    # The execution itself still succeeded; only the required log retrieval failed.
    assert result["execution"]["status"] == "completed"
    assert result["logs"]["status"] == "unavailable"
    assert result["logs"]["error_code"] == "TEST_LOGS_UNAVAILABLE"
    assert result["failed_stage"] == "logs"
    assert result["error_code"] == "TEST_LOGS_UNAVAILABLE"
    assert result["next_step"]  # actionable hint for the logs failure
    assert result["behavior_verified"] == {
        "verified": False, "reason": "logs_unavailable", "logs_status": "unavailable",
    }


def test_require_test_logs_true_passes_when_logs_retrieved(registry, monkeypatch):
    # require_test_logs=true with a successful execution AND retrieved logs is a clean pass —
    # the marker reports verified=true.
    bid = _seed_real_run(registry, monkeypatch, "b-rt-reqlogs-ok")
    execute = _FakeExecuteAction(_exec_complete())
    monitor = _monitor_ok()
    _patch_test_actions(monkeypatch, execute=execute, monitor=monitor)
    result = orchestrate_deploy_action(
        boomi_client=MagicMock(), build_id=bid,
        environment_id="env-1", runtime_id="rt-1",
        run_test=True, dry_run=False, require_test_logs=True,
    )
    assert result["_success"] is True
    assert result["logs"]["status"] == "retrieved"
    assert not any(c.startswith("TEST_") for c in _codes(result))
    assert result["behavior_verified"] == {
        "verified": True, "reason": "test_ran_logs_retrieved", "logs_status": "retrieved",
    }


def test_require_test_logs_default_false_log_failure_stays_diagnostic(registry, monkeypatch):
    # The DEFAULT (require_test_logs=false) preserves the issue-#65 diagnostic-only semantics
    # byte-for-byte; the only delta is the additive behavior_verified marker (issue #81).
    bid = _seed_real_run(registry, monkeypatch, "b-rt-reqlogs-default")
    execute = _FakeExecuteAction(_exec_complete())
    monitor = _FakeMonitorAction(
        {"execution_logs": _logs_fail(), "execution_artifacts": _artifacts_ok()}
    )
    _patch_test_actions(monkeypatch, execute=execute, monitor=monitor)
    result = orchestrate_deploy_action(
        boomi_client=MagicMock(), build_id=bid,
        environment_id="env-1", runtime_id="rt-1", run_test=True, dry_run=False,
    )
    assert result["_success"] is True
    logs = result["logs"]
    assert logs["status"] == "unavailable"
    assert logs["error_code"] == "LOG_RETRIEVAL_FAILED"  # NOT promoted to TEST_LOGS_UNAVAILABLE
    assert logs["warnings"]  # existing diagnostic warning preserved
    assert not any(c.startswith("TEST_") for c in _codes(result))
    assert "failed_stage" not in result  # success path never gets top-level failure metadata
    assert result["behavior_verified"] == {
        "verified": False, "reason": "logs_unavailable", "logs_status": "unavailable",
    }


def test_require_test_logs_true_fails_when_log_fetch_disabled(registry, monkeypatch):
    # test_fetch_logs=false is "absent logs": with require_test_logs=true a successful execution
    # with no fetched logs is itself the failure — the monitor's execution_logs is never called.
    bid = _seed_real_run(registry, monkeypatch, "b-rt-reqlogs-nofetch")
    execute = _FakeExecuteAction(_exec_complete())
    monitor = _FakeMonitorAction({"execution_artifacts": _artifacts_ok()})
    _patch_test_actions(monkeypatch, execute=execute, monitor=monitor)
    result = orchestrate_deploy_action(
        boomi_client=MagicMock(), build_id=bid,
        environment_id="env-1", runtime_id="rt-1",
        run_test=True, dry_run=False, require_test_logs=True, test_fetch_logs=False,
    )
    assert result["_success"] is False
    assert "TEST_LOGS_UNAVAILABLE" in _codes(result)
    assert result["logs"]["status"] == "unavailable"
    assert result["failed_stage"] == "logs"
    assert "execution_logs" not in monitor.actions_called()


def test_require_test_logs_true_does_not_fail_on_artifact_failure(registry, monkeypatch):
    # The artifact leg is independent of behavioral verification: logs retrieved but artifacts
    # unavailable is still a verified pass under require_test_logs=true (issue #81).
    bid = _seed_real_run(registry, monkeypatch, "b-rt-reqlogs-artifact")
    execute = _FakeExecuteAction(_exec_complete())
    monitor = _FakeMonitorAction(
        {"execution_logs": _logs_ok(), "execution_artifacts": _artifacts_download_failed()}
    )
    _patch_test_actions(monkeypatch, execute=execute, monitor=monitor)
    result = orchestrate_deploy_action(
        boomi_client=MagicMock(), build_id=bid,
        environment_id="env-1", runtime_id="rt-1",
        run_test=True, dry_run=False, require_test_logs=True,
    )
    assert result["_success"] is True
    assert result["logs"]["status"] == "retrieved"
    assert result["logs"]["artifact_status"] == "unavailable"
    assert not any(c.startswith("TEST_") for c in _codes(result))
    assert result["behavior_verified"]["verified"] is True


def test_require_test_logs_true_execution_failure_takes_precedence(registry, monkeypatch):
    # An ERROR execution dominates: the surfaced code is TEST_EXECUTION_FAILED, never masked by a
    # log failure, and the marker reports test_failed (not logs_unavailable) — #129 D7 split the
    # old catch-all test_failed_or_warned into distinct warn/fail/timeout reasons.
    bid = _seed_real_run(registry, monkeypatch, "b-rt-reqlogs-execfail")
    execute = _FakeExecuteAction(_exec_failed(status="ERROR"))
    monitor = _FakeMonitorAction(
        {"execution_logs": _logs_fail(), "execution_artifacts": _artifacts_ok()}
    )
    _patch_test_actions(monkeypatch, execute=execute, monitor=monitor)
    result = orchestrate_deploy_action(
        boomi_client=MagicMock(), build_id=bid,
        environment_id="env-1", runtime_id="rt-1",
        run_test=True, dry_run=False, require_test_logs=True,
    )
    assert result["_success"] is False
    assert "TEST_EXECUTION_FAILED" in _codes(result)
    assert "TEST_LOGS_UNAVAILABLE" not in _codes(result)
    assert result["failed_stage"] == "execution"
    assert result["behavior_verified"]["reason"] == "test_failed"
    # The logs stage stays diagnostic-only on a failed execution — never contradictorily promoted
    # to TEST_LOGS_UNAVAILABLE with a "test execution succeeded" hint (#81 review).
    assert result["logs"]["error_code"] == "LOG_RETRIEVAL_FAILED"
    # ...and its diagnostic wording must NOT claim the execution succeeded (#81 review).
    assert "succeeded" not in (result["logs"]["next_step"] or "").lower()
    assert result["logs"]["warnings"] == ["Log retrieval was unavailable."]


def test_require_test_logs_true_fails_when_execution_has_no_log_id(registry, monkeypatch):
    # A COMPLETE execution that returns no execution_id can never yield a ProcessLog; with
    # require_test_logs=true that absent-logs case must fail, and the monitor is never called
    # (the early no-execution-id return) (#81 review).
    bid = _seed_real_run(registry, monkeypatch, "b-rt-reqlogs-noid")
    execute = _FakeExecuteAction(_exec_complete(execution_id=None))
    monitor = _monitor_ok()
    _patch_test_actions(monkeypatch, execute=execute, monitor=monitor)
    result = orchestrate_deploy_action(
        boomi_client=MagicMock(), build_id=bid,
        environment_id="env-1", runtime_id="rt-1",
        run_test=True, dry_run=False, require_test_logs=True,
    )
    assert result["_success"] is False
    assert "TEST_LOGS_UNAVAILABLE" in _codes(result)
    assert result["execution"]["status"] == "completed"
    assert result["logs"]["status"] == "unavailable"
    assert result["failed_stage"] == "logs"
    assert monitor.calls == []  # no execution_id → no log/artifact fetch attempted
    assert result["behavior_verified"] == {
        "verified": False, "reason": "logs_unavailable", "logs_status": "unavailable",
    }
    # With no execution_id, the remediation must NOT instruct a monitor_platform re-fetch by id —
    # that is impossible here (#81 review). Both top-level and stage next_step are no-id specific.
    assert "no execution_id" in result["next_step"]
    assert "monitor_platform" not in result["next_step"]
    assert "no execution_id" in (result["logs"]["next_step"] or "")


def test_require_test_logs_default_false_no_log_id_stays_not_required(registry, monkeypatch):
    # The DEFAULT keeps the completed-without-execution_id path a diagnostic not_required success.
    bid = _seed_real_run(registry, monkeypatch, "b-rt-noid-default")
    execute = _FakeExecuteAction(_exec_complete(execution_id=None))
    monitor = _monitor_ok()
    _patch_test_actions(monkeypatch, execute=execute, monitor=monitor)
    result = orchestrate_deploy_action(
        boomi_client=MagicMock(), build_id=bid,
        environment_id="env-1", runtime_id="rt-1", run_test=True, dry_run=False,
    )
    assert result["_success"] is True
    assert result["execution"]["status"] == "completed"
    assert result["logs"]["status"] == "not_required"
    assert not any(c.startswith("TEST_") for c in _codes(result))
    assert monitor.calls == []


def test_behavior_verified_marker_dry_run(registry):
    bid = registry("b-bv-dry", _single_process_entry(process_id="CID-1"))
    result = orchestrate_deploy_action(
        boomi_client=_ExplodingClient(), build_id=bid,
        environment_id="env-1", runtime_id="rt-1", run_test=True, dry_run=True,
    )
    assert result["_success"] is True
    assert result["behavior_verified"] == {
        "verified": False, "reason": "dry_run", "logs_status": "planned",
    }


def test_behavior_verified_marker_run_test_false(registry, monkeypatch):
    bid = _seed_real_run(registry, monkeypatch, "b-bv-notest")
    execute = _FakeExecuteAction(_exec_complete())
    monitor = _monitor_ok()
    _patch_test_actions(monkeypatch, execute=execute, monitor=monitor)
    result = orchestrate_deploy_action(
        boomi_client=MagicMock(), build_id=bid,
        environment_id="env-1", runtime_id="rt-1", run_test=False, dry_run=False,
    )
    assert result["_success"] is True
    assert result["behavior_verified"] == {
        "verified": False, "reason": "test_not_run", "logs_status": "skipped",
    }


def test_behavior_verified_marker_prior_stage_blocked(registry, monkeypatch):
    bid = registry("b-bv-blocked", _single_process_entry(process_id="CID-1"))
    _patch_stage_failure(monkeypatch, bid, "deploy")
    execute = _FakeExecuteAction(_exec_complete())
    monitor = _monitor_ok()
    _patch_test_actions(monkeypatch, execute=execute, monitor=monitor)
    result = orchestrate_deploy_action(
        boomi_client=MagicMock(), build_id=bid,
        environment_id="env-1", runtime_id="rt-1", run_test=True, dry_run=False,
    )
    assert result["_success"] is False
    assert result["execution"]["status"] == "blocked"
    assert result["behavior_verified"] == {
        "verified": False, "reason": "test_blocked", "logs_status": "blocked",
    }


def test_behavior_verified_marker_complete_warn(registry, monkeypatch):
    bid = _seed_real_run(registry, monkeypatch, "b-bv-warn")
    execute = _FakeExecuteAction(_exec_warn())
    monitor = _monitor_ok()
    _patch_test_actions(monkeypatch, execute=execute, monitor=monitor)
    result = orchestrate_deploy_action(
        boomi_client=MagicMock(), build_id=bid,
        environment_id="env-1", runtime_id="rt-1", run_test=True, dry_run=False,
    )
    # COMPLETE_WARN is success-with-warning, but it is NOT behavioral verification. #129 D7 gives
    # it its own reason (test_completed_with_warnings) distinct from a hard failure/timeout.
    assert result["_success"] is True
    assert result["execution"]["status"] == "warning"
    assert result["behavior_verified"] == {
        "verified": False, "reason": "test_completed_with_warnings", "logs_status": "retrieved",
    }


@pytest.mark.parametrize("status", ["ERROR", "ABORTED"])
def test_behavior_verified_marker_execution_failed(registry, monkeypatch, status):
    # #129 D7: a hard terminal failure (ERROR/ABORTED) reports reason=test_failed, distinct from
    # the COMPLETE_WARN (test_completed_with_warnings) and TIMEOUT (test_timeout) reasons.
    bid = _seed_real_run(registry, monkeypatch, f"b-bv-fail-{status.lower()}")
    execute = _FakeExecuteAction(_exec_failed(status=status))
    monitor = _monitor_ok()
    _patch_test_actions(monkeypatch, execute=execute, monitor=monitor)
    result = orchestrate_deploy_action(
        boomi_client=MagicMock(), build_id=bid,
        environment_id="env-1", runtime_id="rt-1", run_test=True, dry_run=False,
    )
    assert result["_success"] is False
    assert result["execution"]["status"] == "failed"
    assert result["behavior_verified"]["verified"] is False
    assert result["behavior_verified"]["reason"] == "test_failed"


def test_run_test_execute_setup_failure_is_execution_failed_not_request_id_missing(registry, monkeypatch):
    # A pre-request execute failure (e.g. invalid dynamic/process properties, or an API setup
    # error) returns _success=False with an error and NO request_id — but it is NOT the
    # "accepted but no request_id" case, so it must map to TEST_EXECUTION_FAILED, not
    # TEST_REQUEST_ID_MISSING. Logs are blocked (no execution to diagnose).
    bid = _seed_real_run(registry, monkeypatch, "b-rt-setupfail")
    execute = _FakeExecuteAction(_exec_setup_failed("process_properties['CID-1'] must be a dict"))
    monitor = _monitor_ok()
    _patch_test_actions(monkeypatch, execute=execute, monitor=monitor)
    result = orchestrate_deploy_action(
        boomi_client=MagicMock(), build_id=bid,
        environment_id="env-1", runtime_id="rt-1", run_test=True, dry_run=False,
    )
    assert result["_success"] is False
    assert "TEST_EXECUTION_FAILED" in _codes(result)
    assert "TEST_REQUEST_ID_MISSING" not in _codes(result)
    assert result["execution"]["status"] == "failed"
    assert result["execution"]["error"] == "process_properties['CID-1'] must be a dict"
    assert result["logs"]["status"] == "blocked"
    assert monitor.calls == []


def test_run_test_log_content_download_failure_is_unavailable_not_false_retrieved(registry, monkeypatch):
    # The log/artifact URL was created (202) but the ZIP download/extract failed: handle_*
    # returns _success=True with _downloaded=False + error. This must NOT look like a clean
    # "retrieved" — the error has to survive, with the download_url preserved for manual retry.
    bid = _seed_real_run(registry, monkeypatch, "b-rt-dlfail")
    execute = _FakeExecuteAction(_exec_complete())
    monitor = _FakeMonitorAction({
        "execution_logs": _logs_download_failed(),
        "execution_artifacts": _artifacts_download_failed(),
    })
    _patch_test_actions(monkeypatch, execute=execute, monitor=monitor)
    result = orchestrate_deploy_action(
        boomi_client=MagicMock(), build_id=bid,
        environment_id="env-1", runtime_id="rt-1", run_test=True, dry_run=False,
    )
    # A content-download failure is diagnostic only — overall run still succeeds.
    assert result["_success"] is True
    assert result["execution"]["status"] == "completed"
    logs = result["logs"]
    assert logs["status"] == "unavailable"
    assert logs["error"]
    assert logs["download_url"] == "https://logs.example/dl"  # pointer preserved for manual retry
    assert logs["downloaded"] is False
    assert not logs["log_excerpts"]  # no misleading empty "retrieved" excerpts
    assert logs["artifact_status"] == "unavailable"
    assert logs["artifact_error"]
    assert logs["artifact_download_url"] == "https://artifacts.example/dl"
    assert result["summary"]["test"]["logs_status"] == "unavailable"
    assert result["summary"]["test"]["log_error"]
    assert not any(c.startswith("TEST_") for c in _codes(result))


def test_run_test_dynamic_and_process_properties_pass_through(registry, monkeypatch):
    bid = _seed_real_run(registry, monkeypatch, "b-rt-props")
    execute = _FakeExecuteAction(_exec_complete())
    monitor = _monitor_ok()
    _patch_test_actions(monkeypatch, execute=execute, monitor=monitor)
    dyn = {"ENV": "prod"}
    proc = {"CID-1": {"retries": "3"}}
    result = orchestrate_deploy_action(
        boomi_client=MagicMock(), build_id=bid,
        environment_id="env-1", runtime_id="rt-1", run_test=True, dry_run=False,
        test_dynamic_properties=dyn, test_process_properties=proc,
        test_timeout_seconds=120,
    )
    assert result["_success"] is True
    config = execute.calls[0]["config_data"]
    assert config["dynamic_properties"] == dyn
    assert config["process_properties"] == proc
    assert config["wait"] is True
    assert config["timeout"] == 120


def _patch_stage_failure(monkeypatch, bid, which):
    """Patch the four routers so that ``which`` stage fails (earlier stages succeed)."""
    deployment = {**_deploy_ok(bid), **_process_attachments("rt-1", "env-1", "CID-1")}
    environments = _ok_env("env-1")
    runtimes = _ok_runtime("rt-1", "env-1", attached=True)
    schedules = {
        "update": {"_success": True, "schedule": _sched("sch-1")},
        "enable": {"_success": True, "status": _status("sst-1", True)},
    }
    if which == "package":
        deployment = {"list_packages": {"_success": False, "error": "pkg boom"}}
    elif which == "deploy":
        deployment = {
            "list_packages": {"_success": True, "packages": [_pkg("pkg-1", bid)]},
            "list_deployments": {"_success": True, "deployments": []},
            "deploy": {"_success": False, "error": "deploy boom"},
        }
    elif which == "runtime":
        runtimes = _ok_runtime("rt-1", "env-1", attached=False)
        runtimes["attach"] = {"_success": False, "error": "attach boom"}
    elif which == "schedule":
        schedules = {"update": {"_success": False, "error": "sched boom"}}
    _patch_all(
        monkeypatch, deployment=deployment, environments=environments,
        runtimes=runtimes, schedules=schedules,
    )


@pytest.mark.parametrize("which", ["package", "deploy", "runtime", "schedule"])
def test_prior_stage_failures_block_run_test_without_execute_or_monitor(registry, monkeypatch, which):
    bid = registry(f"b-rt-block-{which}", _single_process_entry(process_id="CID-1"))
    _patch_stage_failure(monkeypatch, bid, which)
    execute = _FakeExecuteAction(_exec_complete())
    monitor = _monitor_ok()
    _patch_test_actions(monkeypatch, execute=execute, monitor=monitor)
    result = orchestrate_deploy_action(
        boomi_client=MagicMock(), build_id=bid,
        environment_id="env-1", runtime_id="rt-1",
        schedule_override={"cron": "0 9 * * *"}, run_test=True, dry_run=False,
    )
    assert result["_success"] is False
    assert result["execution"]["status"] == "blocked"
    assert result["logs"]["status"] == "blocked"
    # The run-test stage never ran.
    assert execute.calls == []
    assert monitor.calls == []
    assert "test" not in result["summary"]


# ===========================================================================
# Failure hardening, idempotency, and cleanup planning (issue #65)
# ===========================================================================
def _create_everything_deployment(bid):
    """Package/deploy/attachment responses where NOTHING pre-exists, so every stage CREATES.

    Used by the cleanup-planning tests: a failure after this run created the package, deployment,
    and all three attachment legs is exactly the case where cleanup must name every undo operation.
    """
    return {
        "list_packages": {"_success": True, "packages": []},
        "create_package": {"_success": True, "package": _pkg("pkg-new", bid)},
        "list_deployments": {"_success": True, "deployments": []},
        "deploy": {"_success": True, "deployment": _dep("dep-new", True, current_version="1")},
        **_process_attachments("rt-1", "env-1", "CID-1", env_attached=False, atom_attached=False),
    }


_DESTRUCTIVE_CLEANUP_ACTIONS = {
    "delete",  # schedule
    "detach_process_atom", "detach_process_environment", "undeploy", "delete_package",  # deployment
    "detach",  # runtime<->env
}


def test_deploy_failure_includes_failure_metadata_and_cleanup_plan(registry, monkeypatch):
    # Package is CREATED, then deploy fails: cleanup must PLAN delete_package (and only that),
    # dry-run, with full failure metadata. (The package this run created is the one undo target.)
    bid = registry("b65-deployfail", _single_process_entry(process_id="CID-1"))
    fake = _FakeDeploymentAction({
        "list_packages": {"_success": True, "packages": []},
        "create_package": {"_success": True, "package": _pkg("pkg-new", bid)},
        "list_deployments": {"_success": True, "deployments": []},
        "deploy": {"_success": False, "error": "Action 'deploy' failed: denied (500)"},
    })
    monkeypatch.setattr(orchestration, "manage_deployment_action", fake)
    result = orchestrate_deploy_action(
        boomi_client=MagicMock(), build_id=bid,
        environment_id="env-1", runtime_id="rt-1", dry_run=False,
    )
    assert result["_success"] is False
    assert result["error_code"] == "DEPLOY_CREATE_FAILED"
    assert result["failed_stage"] == "deployment"
    assert result["prior_stage_summary"] == {
        "package": {"status": "created", "package_id": "pkg-new", "package_version": bid},
    }
    assert result["next_step"]
    cleanup = result["cleanup"]
    assert cleanup["status"] == "planned"
    assert cleanup["dry_run"] is True
    assert cleanup["mutation_allowed"] is False
    assert cleanup["results"] == []
    assert [op["action"] for op in cleanup["operations"]] == ["delete_package"]
    assert cleanup["operations"][0]["resource_id"] == "pkg-new"
    # Dry-run plan never mutates.
    assert not (set(fake.actions_called()) & _DESTRUCTIVE_CLEANUP_ACTIONS)


def test_attach_failure_cleanup_names_created_legs_and_partial_summary(registry, monkeypatch):
    # Legs 1 (runtime<->env) + 2 (process<->env) attach, then leg 3 (process<->runtime) fails on a
    # run that CREATED the package + deployment. Cleanup must name the two created legs + undeploy +
    # delete_package, in reverse creation order, without mutating anything.
    bid = registry("b65-attachfail", _single_process_entry(process_id="CID-1"))
    deployment = _create_everything_deployment(bid)
    deployment["attach_process_atom"] = {
        "_success": False, "error": "Action 'attach_process_atom' failed: denied",
    }
    dep, env, rt, sch = _patch_all(
        monkeypatch, deployment=deployment,
        environments=_ok_env("env-1"),
        runtimes=_ok_runtime("rt-1", "env-1", attached=False),
        schedules={},
    )
    result = orchestrate_deploy_action(
        boomi_client=MagicMock(), build_id=bid,
        environment_id="env-1", runtime_id="rt-1", dry_run=False,
    )
    assert result["_success"] is False
    assert result["error_code"] == "PROCESS_RUNTIME_ATTACHMENT_CREATE_FAILED"
    assert result["failed_stage"] == "runtime_attachment"
    # Prior summary shows package + deployment succeeded (created/deployed), with ids.
    assert result["prior_stage_summary"]["package"]["package_id"] == "pkg-new"
    assert result["prior_stage_summary"]["deployment"]["deployment_id"] == "dep-new"
    # Partial-attachment ids stay visible on the failed stage for recovery.
    rta = result["runtime_attachment"]
    assert rta["runtime_env_attachment_id"] == "ea-new"
    assert rta["process_env_attachment_id"] == "pe-new"
    assert rta["process_runtime_attachment_id"] is None
    # Cleanup names only the legs THIS run attached (not the never-attached leg 3) + undeploy + del.
    cleanup = result["cleanup"]
    assert cleanup["status"] == "planned"
    assert [op["action"] for op in cleanup["operations"]] == [
        "detach_process_environment", "detach", "undeploy", "delete_package",
    ]
    ids = {op["action"]: op["resource_id"] for op in cleanup["operations"]}
    assert ids["detach_process_environment"] == "pe-new"
    assert ids["detach"] == "ea-new"
    assert ids["undeploy"] == "dep-new"
    assert ids["delete_package"] == "pkg-new"
    assert not (set(dep.actions_called()) & _DESTRUCTIVE_CLEANUP_ACTIONS)
    assert "detach" not in rt.actions_called()


def test_schedule_failure_includes_failure_metadata_and_prior_summary(registry, monkeypatch):
    # Schedule update fails after package/deploy/runtime all succeeded (reuse path): failure
    # metadata names the schedule stage and summarizes the three prior stages.
    bid = registry("b65-schedfail", _single_process_entry(process_id="CID-1"))
    _patch_stage_failure(monkeypatch, bid, "schedule")
    result = orchestrate_deploy_action(
        boomi_client=MagicMock(), build_id=bid,
        environment_id="env-1", runtime_id="rt-1",
        schedule_override={"cron": "0 9 * * *"}, dry_run=False,
    )
    assert result["_success"] is False
    assert result["error_code"] == "SCHEDULE_UPDATE_FAILED"
    assert result["failed_stage"] == "schedule"
    assert set(result["prior_stage_summary"]) == {"package", "deployment", "runtime_attachment"}
    assert result["prior_stage_summary"]["runtime_attachment"]["status"] in ("reused", "attached")
    # First schedule call failed -> nothing this run changed (reuse path) -> cleanup not_required.
    assert result["cleanup"]["status"] == "not_required"
    assert "retry" in result["next_step"].lower() or "re-run" in result["next_step"].lower()


def test_execution_timeout_includes_failure_metadata_and_prior_summary(registry, monkeypatch):
    bid = _seed_real_run(registry, monkeypatch, "b65-timeout")
    execute = _FakeExecuteAction(_exec_timeout())
    monitor = _monitor_ok()
    _patch_test_actions(monkeypatch, execute=execute, monitor=monitor)
    result = orchestrate_deploy_action(
        boomi_client=MagicMock(), build_id=bid,
        environment_id="env-1", runtime_id="rt-1", run_test=True, dry_run=False,
    )
    assert result["_success"] is False
    assert result["error_code"] == "TEST_EXECUTION_TIMEOUT"
    assert result["failed_stage"] == "execution"
    # #129 D7: a timeout gets its own behavior_verified reason, distinct from warn/fail.
    assert result["behavior_verified"]["reason"] == "test_timeout"
    # Every pipeline stage before execution is summarized (package/deploy/runtime/schedule).
    assert set(result["prior_stage_summary"]) == {
        "package", "deployment", "runtime_attachment", "schedule",
    }
    assert result["logs"]["status"] == "blocked"
    # All prior resources were reused on this run -> nothing to clean up.
    assert result["cleanup"]["status"] == "not_required"
    assert monitor.calls == []


def test_log_retrieval_failure_is_structured_diagnostic_with_next_step(registry, monkeypatch):
    bid = _seed_real_run(registry, monkeypatch, "b65-logfail")
    execute = _FakeExecuteAction(_exec_complete())
    monitor = _FakeMonitorAction(
        {"execution_logs": _logs_fail(), "execution_artifacts": _artifacts_ok()}
    )
    _patch_test_actions(monkeypatch, execute=execute, monitor=monitor)
    result = orchestrate_deploy_action(
        boomi_client=MagicMock(), build_id=bid,
        environment_id="env-1", runtime_id="rt-1", run_test=True, dry_run=False,
    )
    # A log-fetch failure is diagnostic only — overall run still succeeds, no top-level failure keys.
    assert result["_success"] is True
    assert "error_code" not in result
    assert "failed_stage" not in result
    logs = result["logs"]
    assert logs["status"] == "unavailable"
    assert logs["error_code"] == "LOG_RETRIEVAL_FAILED"
    assert logs["failed_stage"] == "logs"
    assert "monitor_platform" in logs["next_step"]
    # Mirrored into the test sub-summary.
    assert result["summary"]["test"]["log_error_code"] == "LOG_RETRIEVAL_FAILED"
    assert result["summary"]["test"]["log_next_step"]


def test_artifact_retrieval_failure_is_structured_diagnostic(registry, monkeypatch):
    bid = _seed_real_run(registry, monkeypatch, "b65-artifactfail")
    execute = _FakeExecuteAction(_exec_complete())
    monitor = _FakeMonitorAction(
        {"execution_logs": _logs_ok(), "execution_artifacts": _logs_fail()}
    )
    _patch_test_actions(monkeypatch, execute=execute, monitor=monitor)
    result = orchestrate_deploy_action(
        boomi_client=MagicMock(), build_id=bid,
        environment_id="env-1", runtime_id="rt-1", run_test=True, dry_run=False,
    )
    assert result["_success"] is True
    logs = result["logs"]
    assert logs["artifact_status"] == "unavailable"
    assert logs["artifact_error_code"] == "ARTIFACT_RETRIEVAL_FAILED"
    assert logs["artifact_failed_stage"] == "logs"
    assert result["summary"]["test"]["artifact_error_code"] == "ARTIFACT_RETRIEVAL_FAILED"


def test_retry_after_partial_attachment_failure_reuses_prior_successes(registry, monkeypatch):
    # First attempt creates legs 1+2 then fails on leg 3; the SECOND attempt (with everything now
    # present) reuses every prior success and creates no duplicates.
    bid = registry("b65-retry", _single_process_entry(process_id="CID-1"))
    first = _create_everything_deployment(bid)
    first["attach_process_atom"] = {"_success": False, "error": "leg 3 boom"}
    _patch_all(
        monkeypatch, deployment=first,
        environments=_ok_env("env-1"),
        runtimes=_ok_runtime("rt-1", "env-1", attached=False),
        schedules={},
    )
    r1 = orchestrate_deploy_action(
        boomi_client=MagicMock(), build_id=bid,
        environment_id="env-1", runtime_id="rt-1", dry_run=False,
    )
    assert r1["_success"] is False
    assert r1["failed_stage"] == "runtime_attachment"

    # Second attempt: package + deployment + all three legs already exist.
    dep2, env2, rt2, sch2 = _patch_all(
        monkeypatch,
        deployment={**_deploy_ok(bid), **_process_attachments("rt-1", "env-1", "CID-1")},
        environments=_ok_env("env-1"),
        runtimes=_ok_runtime("rt-1", "env-1", attached=True),
        schedules={},
    )
    r2 = orchestrate_deploy_action(
        boomi_client=MagicMock(), build_id=bid,
        environment_id="env-1", runtime_id="rt-1", dry_run=False,
    )
    assert r2["_success"] is True
    # No duplicate provisioning — every stage reused what already exists.
    called = dep2.actions_called()
    assert "create_package" not in called
    assert "deploy" not in called
    assert "attach_process_environment" not in called
    assert "attach_process_atom" not in called
    assert "attach" not in rt2.actions_called()
    assert r2["summary"]["resource_reuse"] == {
        "package": True, "deployment": True, "runtime_attachment": True, "schedule": False,
    }


def test_repeated_call_with_existing_resources_does_not_duplicate(registry, monkeypatch):
    bid = registry("b65-repeat", _single_process_entry(process_id="CID-1"))
    dep, env, rt, sch = _patch_all(
        monkeypatch,
        deployment={**_deploy_ok(bid), **_process_attachments("rt-1", "env-1", "CID-1")},
        environments=_ok_env("env-1"),
        runtimes=_ok_runtime("rt-1", "env-1", attached=True),
        schedules={},
    )
    result = orchestrate_deploy_action(
        boomi_client=MagicMock(), build_id=bid,
        environment_id="env-1", runtime_id="rt-1", dry_run=False,
    )
    assert result["_success"] is True
    assert result["package"]["status"] == "reused"
    assert result["deployment"]["status"] == "reused"
    assert result["runtime_attachment"]["status"] == "reused"
    called = dep.actions_called()
    assert "create_package" not in called
    assert "deploy" not in called
    assert "attach_process_environment" not in called
    assert "attach_process_atom" not in called
    assert sch.actions_called() == []  # no schedule requested, never touched


def test_cleanup_plan_dry_run_names_exact_operations_without_mutation(registry, monkeypatch):
    # A run that CREATED package + deployment + all 3 legs, then the schedule ENABLE fails. The
    # cleanup plan names every CREATED resource in reverse creation order — package, deployment, and
    # the three attachment legs — dry-run, without calling a single destructive action. The
    # schedule is deliberately NOT in the plan (modified in place, re-applied idempotently on retry).
    bid = registry("b65-cleanplan", _single_process_entry(process_id="CID-1"))
    dep, env, rt, sch = _patch_all(
        monkeypatch,
        deployment=_create_everything_deployment(bid),
        environments=_ok_env("env-1"),
        runtimes=_ok_runtime("rt-1", "env-1", attached=False),
        schedules={
            "update": {"_success": True, "schedule": _sched("sch-1")},
            "enable": {"_success": False, "error": "enable boom"},
        },
    )
    result = orchestrate_deploy_action(
        boomi_client=MagicMock(), build_id=bid,
        environment_id="env-1", runtime_id="rt-1",
        schedule_override={"cron": "0 9 * * *"}, dry_run=False,
    )
    assert result["_success"] is False
    assert result["error_code"] == "SCHEDULE_ENABLE_FAILED"
    assert result["schedule"]["changed"] is True
    cleanup = result["cleanup"]
    assert cleanup["status"] == "planned"
    assert cleanup["dry_run"] is True
    assert cleanup["mutation_allowed"] is False
    assert cleanup["results"] == []
    assert [op["action"] for op in cleanup["operations"]] == [
        "detach_process_atom", "detach_process_environment",
        "detach", "undeploy", "delete_package",
    ]
    # The in-place-modified schedule is never a destructive cleanup target.
    assert not any(op["resource_type"] == "schedule" for op in cleanup["operations"])
    # The runtime<->env detach op carries ONLY the attachment id (no environment_id), so the
    # runtime router uses its direct-by-attachment-id path instead of the runtime-id lookup path.
    detach_op = next(op for op in cleanup["operations"] if op["action"] == "detach")
    assert detach_op["config"] == {"resource_id": "ea-new"}
    assert all(op["destructive"] is True for op in cleanup["operations"])
    # NOT ONE destructive action was actually called (dry-run plan only).
    assert not (set(dep.actions_called()) & _DESTRUCTIVE_CLEANUP_ACTIONS)
    assert "detach" not in rt.actions_called()
    assert "delete" not in sch.actions_called()


def test_cleanup_on_failure_true_executes_destructive_operations(registry, monkeypatch):
    # Same created-everything + schedule-enable failure, but with cleanup_on_failure=True: every
    # planned destructive op (the CREATED resources, not the schedule) is executed through the
    # sibling routers and its result recorded.
    bid = registry("b65-cleanexec", _single_process_entry(process_id="CID-1"))
    deployment = _create_everything_deployment(bid)
    deployment.update({
        "detach_process_atom": {"_success": True},
        "detach_process_environment": {"_success": True},
        "undeploy": {"_success": True},
        "delete_package": {"_success": True},
    })
    dep, env, rt, sch = _patch_all(
        monkeypatch,
        deployment=deployment,
        environments=_ok_env("env-1"),
        runtimes={
            **_ok_runtime("rt-1", "env-1", attached=False),
            "detach": {"_success": True},
        },
        schedules={
            "update": {"_success": True, "schedule": _sched("sch-1")},
            "enable": {"_success": False, "error": "enable boom"},
        },
    )
    result = orchestrate_deploy_action(
        boomi_client=MagicMock(), build_id=bid,
        environment_id="env-1", runtime_id="rt-1",
        schedule_override={"cron": "0 9 * * *"}, cleanup_on_failure=True, dry_run=False,
    )
    assert result["_success"] is False
    cleanup = result["cleanup"]
    assert cleanup["status"] == "completed"
    assert cleanup["dry_run"] is False
    assert cleanup["mutation_allowed"] is True
    assert [r["action"] for r in cleanup["results"]] == [
        "detach_process_atom", "detach_process_environment",
        "detach", "undeploy", "delete_package",
    ]
    assert all(r["_success"] for r in cleanup["results"])
    # The destructive ops really were dispatched to the routers; the schedule was NEVER deleted.
    assert "detach" in rt.actions_called()
    assert "delete" not in sch.actions_called()
    assert {"detach_process_atom", "detach_process_environment", "undeploy", "delete_package"} <= set(
        dep.actions_called()
    )


def test_cleanup_on_failure_true_records_failed_operation(registry, monkeypatch):
    # If an executed cleanup op fails, it is recorded with CLEANUP_OPERATION_FAILED and the stage
    # is "warning" — never raised.
    bid = registry("b65-cleanexec-fail", _single_process_entry(process_id="CID-1"))
    fake = _FakeDeploymentAction({
        "list_packages": {"_success": True, "packages": []},
        "create_package": {"_success": True, "package": _pkg("pkg-new", bid)},
        "list_deployments": {"_success": True, "deployments": []},
        "deploy": {"_success": False, "error": "deploy denied"},
        "delete_package": {"_success": False, "error": "delete denied"},
    })
    monkeypatch.setattr(orchestration, "manage_deployment_action", fake)
    result = orchestrate_deploy_action(
        boomi_client=MagicMock(), build_id=bid,
        environment_id="env-1", runtime_id="rt-1",
        cleanup_on_failure=True, dry_run=False,
    )
    assert result["_success"] is False
    cleanup = result["cleanup"]
    assert cleanup["status"] == "warning"
    assert cleanup["results"][0]["action"] == "delete_package"
    assert cleanup["results"][0]["_success"] is False
    assert cleanup["results"][0]["error_code"] == "CLEANUP_OPERATION_FAILED"
    assert cleanup["warnings"]


def test_cleanup_on_failure_non_bool_rejected_without_any_dispatch(registry, monkeypatch):
    # cleanup_on_failure is StrictBool: a coercible non-bool ("yes"/1/"true") must NOT silently opt
    # into destructive cleanup on a DIRECT engine call (which bypasses the wrapper's bool guard). It
    # returns a structured INVALID_REQUEST at request construction, before any SDK/router call.
    bid = registry("b65-strictbool", _single_process_entry(process_id="CID-1"))
    fake = _FakeDeploymentAction({})  # empty responses -> any router call would raise
    monkeypatch.setattr(orchestration, "manage_deployment_action", fake)
    for bad in ("yes", 1, "true"):
        result = orchestrate_deploy_action(
            boomi_client=MagicMock(), build_id=bid,
            environment_id="env-1", runtime_id="rt-1",
            cleanup_on_failure=bad, dry_run=False,
        )
        assert result["_success"] is False, f"cleanup_on_failure={bad!r} must be rejected"
        assert _codes(result) == ["INVALID_REQUEST"]
        assert result["errors"][0]["field"] == "cleanup_on_failure"
        assert fake.calls == []  # rejected before any router/cleanup dispatch

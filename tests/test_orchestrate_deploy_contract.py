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


# ---------------------------------------------------------------------------
# Real-run (issue #61) fakes & helpers
# ---------------------------------------------------------------------------
class _FakeDeploymentAction:
    """Records ``(action, config_data)`` calls and returns canned dict responses per action.

    Patched in for ``orchestration.manage_deployment_action`` so the real-run tests exercise
    orchestration's dict-inspection contract (it inspects ``_success``/payload, never catches
    exceptions) without a live SDK. Mirrors the real router's return shapes.
    """

    def __init__(self, responses):
        self.responses = responses
        self.calls = []

    def __call__(self, sdk=None, profile=None, action=None, config_data=None, **kwargs):
        self.calls.append({"action": action, "config_data": config_data})
        if action not in self.responses:
            raise AssertionError(f"unexpected deployment action call: {action}")
        return self.responses[action]

    def actions_called(self):
        return [c["action"] for c in self.calls]


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


# ---------------------------------------------------------------------------
# Malformed input types -> structured errors, never raw exceptions
# ---------------------------------------------------------------------------
def test_invalid_build_id_type_returns_structured_error():
    # A list build_id is unhashable; it must not raise TypeError at registry lookup.
    result = orchestrate_deploy_action(build_id=[], environment_id="env-1", runtime_id="rt-1")
    assert result["_success"] is False
    assert _codes(result) == ["INVALID_REQUEST"]
    assert result["errors"][0]["field"] == "build_id"


def test_invalid_schedule_override_type_returns_structured_error(registry):
    # A non-dict schedule_override must not raise ValidationError out of the function.
    bid = registry("b-badsched", _single_process_entry())
    result = orchestrate_deploy_action(
        build_id=bid, environment_id="env-1", runtime_id="rt-1", schedule_override=[],
    )
    assert result["_success"] is False
    assert _codes(result) == ["INVALID_REQUEST"]
    assert result["errors"][0]["field"] == "schedule_override"


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


def test_malformed_registry_entry(registry):
    bid = registry("b-bad", {"profile": "p"})  # no spec
    result = orchestrate_deploy_action(build_id=bid, environment_id="env-1", runtime_id="rt-1")
    assert result["_success"] is False
    assert _codes(result) == ["BUILD_REGISTRY_ENTRY_MALFORMED"]


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
        "_success", "profile", "build_id", "dry_run", "plan_only", "integration_name",
        "target", "component_summary", "package", "deployment", "runtime_attachment",
        "schedule", "execution", "logs", "cleanup", "summary", "warnings", "errors",
    }
    assert set(result.keys()) == expected_keys

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
    assert summary["stage_warnings"] == {"package": [], "deployment": []}


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
    fake = _FakeDeploymentAction({
        "list_packages": {"_success": True, "packages": []},
        "create_package": {"_success": True, "package": _pkg("pkg-new", bid)},
        "list_deployments": {"_success": True, "deployments": []},
        "deploy": {"_success": True, "deployment": _dep("dep-new", True, current_version="1")},
    })
    monkeypatch.setattr(orchestration, "manage_deployment_action", fake)
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
    fake = _FakeDeploymentAction({
        "list_packages": {"_success": True, "packages": [
            _pkg("pkg-old", bid, created_date="2026-01-01T00:00:00Z"),
            _pkg("pkg-new", bid, created_date="2026-02-01T00:00:00Z"),
            _pkg("pkg-other", "9.9.9", created_date="2026-03-01T00:00:00Z"),
        ]},
        "list_deployments": {"_success": True, "deployments": []},
        "deploy": {"_success": True, "deployment": _dep("dep-1", True)},
    })
    monkeypatch.setattr(orchestration, "manage_deployment_action", fake)
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
    fake = _FakeDeploymentAction({
        "list_packages": {"_success": True, "packages": [_pkg("pkg-1", bid)]},
        "list_deployments": {"_success": True, "deployments": [_dep("dep-old", False)]},
        "deploy": {"_success": True, "deployment": _dep("dep-new", True, current_version="2")},
    })
    monkeypatch.setattr(orchestration, "manage_deployment_action", fake)
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
    fake = _FakeDeploymentAction({
        "list_packages": {"_success": True, "packages": [_pkg("pkg-1", bid)]},
        "list_deployments": {"_success": True, "deployments": [
            _dep("dep-active", True, current_version="3"),
            _dep("dep-inactive", False),
        ]},
        "deploy": {"_success": True, "deployment": _dep("should-not-be-used", True)},
    })
    monkeypatch.setattr(orchestration, "manage_deployment_action", fake)
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
    assert result["cleanup"]["status"] == "blocked"


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
    fake = _FakeDeploymentAction({
        "list_packages": {"_success": True, "packages": [_pkg("pkg-1", bid)]},
        "list_deployments": {"_success": True, "deployments": []},
        "deploy": {"_success": True, "deployment": _dep("dep-new", True, version=7)},
    })
    monkeypatch.setattr(orchestration, "manage_deployment_action", fake)
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
    fake = _FakeDeploymentAction({
        "list_packages": {"_success": True, "packages": [_pkg("pkg-1", bid)]},
        "list_deployments": {"_success": True, "deployments": [_dep("dep-active", True, version=9)]},
        "deploy": {"_success": True, "deployment": _dep("should-not-be-used", True)},
    })
    monkeypatch.setattr(orchestration, "manage_deployment_action", fake)
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
# Typed-contract sanity
# ---------------------------------------------------------------------------
def test_error_code_constants_match_module():
    assert orchestration.BUILD_ID_REQUIRED == "BUILD_ID_REQUIRED"
    assert orchestration.BUILD_MULTIPLE_PROCESS_COMPONENTS == "BUILD_MULTIPLE_PROCESS_COMPONENTS"
    assert orchestration.DEPLOY_AMBIGUOUS_EXISTING == "DEPLOY_AMBIGUOUS_EXISTING"
    assert orchestration.PACKAGE_CREATE_FAILED == "PACKAGE_CREATE_FAILED"

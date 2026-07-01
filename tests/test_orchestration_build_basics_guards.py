"""Issue #102 (M9.8) — orchestrate_deploy build-basics guards.

  * B4 — reject an explicitly empty process_overrides set over a process that
    declares environment extensions (hard, fail-fast before any SDK call).
  * F1 — steer toward require_test_logs=true when a deploy plans to run a test.

Run with PYTHONPATH=src (the editable install .pth is stale):
    PYTHONPATH=src .venv/bin/python -m pytest tests/test_orchestration_build_basics_guards.py
"""

import pytest

from src.boomi_mcp.categories.integration_builder import _BUILD_REGISTRY
from src.boomi_mcp.categories.deployment.orchestration import orchestrate_deploy_action


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


def _entry(*, declares_extensions):
    config = {}
    if declares_extensions:
        config["process_extensions"] = {
            "connections": [{"id": "$ref:target_rest_connection", "fields": [{"id": "username"}]}]
        }
    proc = {
        "key": "proc",
        "type": "process",
        "action": "create",
        "name": "Proc",
        "component_id": None,
        "config": config,
        "depends_on": [],
    }
    return {
        "created_at": "2026-01-01T00:00:00+00:00",
        "profile": "prof",
        "spec": {"version": "1.0", "name": "Itg", "components": [proc], "naming": {}},
        "results": {"proc": {"status": "created", "component_id": "proc-id", "type": "process", "name": "Proc"}},
        "execution_order": ["proc"],
    }


def _codes(result):
    return {e.get("code") for e in result.get("errors", [])}


# ---------------------------------------------------------------------------
# B4 — empty-overrides guard
# ---------------------------------------------------------------------------

def test_empty_overrides_over_declared_extensions_hard_fails(registry):
    bid = registry("b-ext", _entry(declares_extensions=True))
    result = orchestrate_deploy_action(
        build_id=bid, environment_id="env-1", runtime_id="rt-1",
        dry_run=True, process_overrides={},
    )
    assert result["_success"] is False
    assert "EMPTY_PROCESS_OVERRIDES_REJECTED" in _codes(result)
    # #129 D2: this post-target-resolution early error returns the FULL envelope — every stage key
    # present as a blocked placeholder plus summary/behavior_verified — so a caller branching on any
    # stage key never breaks. Because build resolution already succeeded, target is non-null.
    for stage in ("package", "deployment", "runtime_attachment", "schedule", "execution",
                  "logs", "cleanup"):
        assert result[stage]["status"] == "blocked", stage
    assert isinstance(result["summary"], dict)
    assert result["behavior_verified"]["verified"] is False
    assert result["target"] is not None
    assert result["target"]["process_component_id"] == "proc-id"


def test_empty_overrides_without_declared_extensions_is_allowed(registry):
    bid = registry("b-noext", _entry(declares_extensions=False))
    result = orchestrate_deploy_action(
        build_id=bid, environment_id="env-1", runtime_id="rt-1",
        dry_run=True, process_overrides={},
    )
    assert result["_success"] is True
    assert "EMPTY_PROCESS_OVERRIDES_REJECTED" not in _codes(result)


def test_no_overrides_supplied_over_extensions_warns_but_succeeds(registry):
    bid = registry("b-ext2", _entry(declares_extensions=True))
    result = orchestrate_deploy_action(
        build_id=bid, environment_id="env-1", runtime_id="rt-1",
        dry_run=True,  # process_overrides omitted (None)
    )
    assert result["_success"] is True
    assert any("PROCESS_OVERRIDES_NOT_SUPPLIED" in w for w in result.get("warnings", []))


def test_empty_overrides_hard_fail_makes_no_sdk_call(registry):
    # boomi_client is None on a real run; the guard must fail-fast before the
    # client-required check, proving no SDK path is touched.
    bid = registry("b-ext3", _entry(declares_extensions=True))
    result = orchestrate_deploy_action(
        build_id=bid, environment_id="env-1", runtime_id="rt-1",
        dry_run=False, boomi_client=None, process_overrides={},
    )
    assert result["_success"] is False
    assert "EMPTY_PROCESS_OVERRIDES_REJECTED" in _codes(result)


# ---------------------------------------------------------------------------
# F1 — require_test_logs steering
# ---------------------------------------------------------------------------

def test_run_test_without_require_logs_emits_steering_warning(registry):
    bid = registry("b-f1", _entry(declares_extensions=False))
    result = orchestrate_deploy_action(
        build_id=bid, environment_id="env-1", runtime_id="rt-1",
        dry_run=True, run_test=True, require_test_logs=False,
    )
    assert result["_success"] is True
    assert any("REQUIRE_TEST_LOGS_RECOMMENDED" in w for w in result.get("warnings", []))


def test_run_test_with_require_logs_no_steering_warning(registry):
    bid = registry("b-f1b", _entry(declares_extensions=False))
    result = orchestrate_deploy_action(
        build_id=bid, environment_id="env-1", runtime_id="rt-1",
        dry_run=True, run_test=True, require_test_logs=True,
    )
    assert not any("REQUIRE_TEST_LOGS_RECOMMENDED" in w for w in result.get("warnings", []))


def test_no_run_test_no_steering_warning(registry):
    bid = registry("b-f1c", _entry(declares_extensions=False))
    result = orchestrate_deploy_action(
        build_id=bid, environment_id="env-1", runtime_id="rt-1",
        dry_run=True, run_test=False,
    )
    assert not any("REQUIRE_TEST_LOGS_RECOMMENDED" in w for w in result.get("warnings", []))

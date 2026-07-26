"""The acceptance criterion, executed end-to-end (issue #143, M12.8).

    "No component build/apply API is called when validation has a fatal error."

Added after QA Bug #194. That criterion WAS asserted before this file existed —
but in four separate hops, each a substring search over `integration_builder.py`:
the guard contains the tuple, a non-None error sets `error_process_validation`,
that action is in the fail-fast set, the set is acted on. Every hop was checked;
their COMPOSITION never was.

The measured consequence: a mutant adding `and planned_action != "create_clone"`
AFTER the tuple leaves all four textual markers intact and ungates the arm — and
the entire suite still passed. Three such mutants survived (`create`,
`create_clone`, `update`).

So this file executes the chain instead of reading it. Each test drives a payload
with a genuine semantic defect through `_build_plan` on one authoring arm and
asserts the step is refused, then drives `_apply_plan` and asserts
`_execute_component` is never called.

Method, and why it is not payload-driven
----------------------------------------
Threading a payload past every legacy check to reach the #143 gate turns out to be
fragile: the legacy lineage walker already catches the obvious semantic defects
(a Branch DDP-scope violation reports `PROCESS_LINEAGE_DDP_SCOPE_INVALID` — the
LEGACY code — long before #143 sees it), and the defects #143 uniquely catches on
the `flow_sequence` surface are precisely the ones its exemption policy
downgrades. Both facts are correct behaviour, and both make a payload a poor
instrument for testing the GUARD.

So these tests inject a fatal finding at the helper boundary instead: they patch
`_process_ir_semantic_error` to return a builder error and assert the composition
downstream of it. That is exactly the untested hop — the guard decides WHETHER to
call the helper, and the surviving mutants all worked by ungating an arm so the
helper is never reached. With the helper patched, an ungated arm plans clean and
the test fails.

The helper's own behaviour (real payload -> real finding -> real code) is covered
by `test_process_ir_semantic_gate.py`; this file covers what happens around it.
"""

from __future__ import annotations

import sys
from pathlib import Path
from unittest.mock import MagicMock, patch

import pytest

_ROOT = Path(__file__).resolve().parent.parent
for _p in (str(_ROOT), str(_ROOT / "src")):
    if _p not in sys.path:
        sys.path.insert(0, _p)

from src.boomi_mcp.categories import integration_builder as IB

_PATCH_TARGET = "src.boomi_mcp.categories.integration_builder.paginate_metadata"

from src.boomi_mcp.categories.components.builders.process_flow_builder import (
    BuilderValidationError,
)

_TARGET = {
    "connector_type": "rest",
    "connection_id": "33333333-3333-3333-3333-333333333333",
    "operation_id": "44444444-4444-4444-4444-444444444444",
    "action_type": "POST",
}

#: A legacy-CLEAN process. Every test below drives this exact config; whether the
#: step is refused depends only on the injected finding, so nothing is proven by
#: accident of the payload.
_CLEAN_CONFIG = {
    "process_kind": "database_to_api_sync",
    "source": {
        "connector_type": "database",
        "connection_id": "11111111-1111-1111-1111-111111111111",
        "operation_id": "22222222-2222-2222-2222-222222222222",
        "action_type": "Get",
    },
    "transform": {"mode": "passthrough"},
    "target": dict(_TARGET),
    "flow_sequence": [
        {
            "kind": "set_dpp",
            "name": "OUT",
            "source_values": [{"value_type": "static", "value": "v"}],
        }
    ],
}

_FATAL = BuilderValidationError(
    "ProcessIR semantic validation rejected this process at /body/steps/0.",
    error_code="PROCESS_IR_SEMANTIC_LINEAGE_DDP_SCOPE_INVALID",
    field="config",
    hint="static",
    details={"codes": ["PROCESS_IR_SEMANTIC_LINEAGE_DDP_SCOPE_INVALID"], "path": "/body/steps/0"},
)


def _config(action, *, name="GateProbe", conflict_policy="reuse"):
    return {
        "conflict_policy": conflict_policy,
        "integration_spec": {
            "version": "1.0",
            "name": "test-integration",
            "components": [
                {
                    "key": "p1",
                    "type": "process",
                    "action": action,
                    "name": name,
                    "config": dict(_CLEAN_CONFIG),
                }
            ],
        },
    }


def _meta(component_id, name):
    return {
        "component_id": component_id,
        "id": component_id,
        "name": name,
        "folder_name": "Root",
        "type": "process",
        "version": 1,
        "current_version": True,
        "deleted": False,
        "created_date": "",
        "modified_date": "",
    }


def _process_step(plan):
    for step in plan.get("steps") or []:
        if step.get("key") == "p1":
            return step
    return None


# ---------------------------------------------------------------------------
# the gate refuses, per authoring arm
# ---------------------------------------------------------------------------


def _plan_with_injected_finding(config, metadata, finding=_FATAL):
    """Drive `_build_plan` with the semantic helper returning ``finding``."""
    original = IB._process_ir_semantic_error
    IB._process_ir_semantic_error = lambda process_kind, raw_config: finding
    try:
        with patch(_PATCH_TARGET) as mock_pag:
            mock_pag.return_value = metadata
            return IB._build_plan(MagicMock(), config)
    finally:
        IB._process_ir_semantic_error = original


@pytest.mark.parametrize(
    "action, conflict_policy, metadata, expected_arm",
    [
        ("create", "reuse", [], "create"),
        ("update", "reuse", [{"__meta__": "existing-1"}], "update"),
        ("create", "clone", [{"__meta__": "dup-1"}, {"__meta__": "dup-2"}], "create_clone"),
    ],
    ids=["create", "update", "create_clone"],
)
def test_a_fatal_finding_refuses_the_step_on_every_authoring_arm(
    action, conflict_policy, metadata, expected_arm
):
    """One case per arm of ``planned_action in ("create","create_clone","update")``.

    A mutant that ungates ANY single arm makes that arm's case fail, which is
    precisely what the source-grep tests could not catch.
    """
    resolved = [_meta(m["__meta__"], "GateProbe") for m in metadata]
    plan = _plan_with_injected_finding(
        _config(action, conflict_policy=conflict_policy), resolved
    )
    step = _process_step(plan)
    assert step is not None
    assert step["planned_action"] == "error_process_validation", (
        "the {0} arm is ungated — the semantic helper was never consulted".format(
            expected_arm
        )
    )
    assert step["validation_error"]["error_code"] == (
        "PROCESS_IR_SEMANTIC_LINEAGE_DDP_SCOPE_INVALID"
    )


def test_the_same_config_plans_cleanly_when_the_helper_finds_nothing():
    """The discriminator. Without it, a gate that refused everything — or a test
    fixture that was simply invalid — would satisfy every case above."""
    plan = _plan_with_injected_finding(_config("create"), [], finding=None)
    step = _process_step(plan)
    assert step["planned_action"] == "create"
    assert step.get("validation_error") is None


# ---------------------------------------------------------------------------
# and nothing mutates
# ---------------------------------------------------------------------------


def _apply_with_injected_finding(config, metadata, finding=_FATAL):
    calls = []
    original_helper = IB._process_ir_semantic_error
    original_exec = IB._execute_component
    IB._process_ir_semantic_error = lambda process_kind, raw_config: finding
    IB._execute_component = lambda *a, **k: (calls.append(a), {})[1]
    try:
        with patch(_PATCH_TARGET) as mock_pag:
            mock_pag.return_value = metadata
            payload = dict(config)
            payload["dry_run"] = False
            result = IB._apply_plan(MagicMock(), "test-profile", payload)
    finally:
        IB._process_ir_semantic_error = original_helper
        IB._execute_component = original_exec
    return result, calls


@pytest.mark.parametrize(
    "action, conflict_policy, metadata",
    [
        ("create", "reuse", []),
        ("update", "reuse", [{"__meta__": "existing-1"}]),
        ("create", "clone", [{"__meta__": "dup-1"}, {"__meta__": "dup-2"}]),
    ],
    ids=["create", "update", "create_clone"],
)
def test_apply_executes_no_component_when_a_finding_is_fatal(
    action, conflict_policy, metadata
):
    """The acceptance criterion, executed rather than read."""
    resolved = [_meta(m["__meta__"], "GateProbe") for m in metadata]
    result, calls = _apply_with_injected_finding(
        _config(action, conflict_policy=conflict_policy), resolved
    )
    assert calls == [], "a component was executed despite a fatal semantic finding"
    assert result.get("_success") is False


def test_apply_does_execute_when_nothing_is_fatal():
    """Proves the spy would have caught a mutation — a no-mutation assertion is
    worthless if `_execute_component` was never going to be called anyway."""
    _result, calls = _apply_with_injected_finding(_config("create"), [], finding=None)
    assert calls, "the control run executed nothing, so the spy proves nothing"

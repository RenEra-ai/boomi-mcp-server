"""The acceptance criterion, executed end-to-end (issue #143, M12.8).

    "No component build/apply API is called when validation has a fatal error."

Added after QA Bug #194: the criterion WAS asserted before this file existed, but
in four separate hops, each a substring search over `integration_builder.py`.
Their composition was never executed, so a mutant that adds
`and planned_action != "<arm>"` — leaving the tuple literal intact — ungates that
arm and survives a source-grep test.

Everything here runs the real chain. Only `paginate_metadata` is patched, to
control name-collision resolution; the payload, the gate, the plan and the apply
path are all genuine.

Note when mutating the guard by hand: the string
`planned_action in ("create", "create_clone", "update")` appears THREE times in
`integration_builder.py`. Only the occurrence immediately preceding
`_process_ir_semantic_error(...)` is #143's. A first-match edit lands on an
unrelated pre-existing guard and every test here passes — which is not evidence
of anything.
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
from src.boomi_mcp.authoring.process_admission import unit_required_envelope
from src.boomi_mcp.errors import PROCESS_COMPONENT_AUTHORING_UNIT_REQUIRED

_PATCH_TARGET = "src.boomi_mcp.categories.integration_builder.paginate_metadata"

#: A legacy-CLEAN flow that the #143 gate rejects. The `map_ref` is an opaque
#: writer the LEGACY walker accepts as a wildcard, so the legacy lineage pass
#: lets it through; #143 treats the map as establishing nothing and reports the
#: DDP read against a write that lands later — `…LINEAGE_DDP_SCOPE_INVALID`,
#: which `flow_sequence`'s exemption policy does NOT cover.
_FATAL_FLOW = [
    {"kind": "map_ref", "label": "M", "map_ref": "55555555-5555-5555-5555-555555555555"},
    {
        "kind": "set_dpp",
        "label": "p",
        "name": "DPP_B",
        "source_values": [{"value_type": "ddp", "property_name": "DDP_A"}],
    },
    {
        "kind": "set_ddp",
        "label": "d",
        "name": "DDP_A",
        "source_values": [{"value_type": "static", "value": "v"}],
    },
]

_CLEAN_FLOW = [
    {
        "kind": "set_dpp",
        "name": "OUT",
        "source_values": [{"value_type": "static", "value": "v"}],
    }
]

_EXPECTED_CODE = "PROCESS_IR_SEMANTIC_LINEAGE_DDP_SCOPE_INVALID"


#: A would-write supporting row (a connector UPDATE by explicit id) that the
#: apply tests place AHEAD of the process, so a refusal that ran per row instead
#: of before the plan would have executed it.
_SIBLING = {
    "key": "sibling_conn",
    "type": "connector-settings",
    "action": "update",
    "component_id": "66666666-6666-6666-6666-666666666666",
    "name": "Sibling Connection",
    "config": {"connector_type": "rest", "name": "Sibling Connection"},
}


def _config(action, flow, *, conflict_policy="reuse", sibling=False):
    return {
        "conflict_policy": conflict_policy,
        "integration_spec": {
            "version": "1.0",
            "name": "test-integration",
            "components": [
                *([dict(_SIBLING)] if sibling else []),
                {
                    "key": "p1",
                    "type": "process",
                    "action": action,
                    "name": "GateProbe",
                    "config": {
                        "process_kind": "database_to_api_sync",
                        "source": {
                            "connector_type": "database",
                            "connection_id": "11111111-1111-1111-1111-111111111111",
                            "operation_id": "22222222-2222-2222-2222-222222222222",
                            "action_type": "Get",
                        },
                        "transform": {"mode": "passthrough"},
                        "target": {
                            "connector_type": "rest",
                            "connection_id": "33333333-3333-3333-3333-333333333333",
                            "operation_id": "44444444-4444-4444-4444-444444444444",
                            "action_type": "POST",
                        },
                        "flow_sequence": flow,
                    },
                }
            ],
        },
    }


def _meta(component_id):
    return {
        "component_id": component_id,
        "id": component_id,
        "name": "GateProbe",
        "folder_name": "Root",
        "type": "process",
        "version": 1,
        "current_version": True,
        "deleted": False,
        "created_date": "",
        "modified_date": "",
    }


#: (action, conflict_policy, resolved metadata) per authoring arm. The third
#: derives `planned_action = "create_clone"` from a name collision — the arm no
#: caller can author, since `IntegrationComponentSpec.action` is
#: `Literal["create", "update"]`.
_ARMS = [
    ("create", "reuse", []),
    ("update", "reuse", [_meta("existing-1")]),
    ("create", "clone", [_meta("dup-1"), _meta("dup-2")]),
]
_ARM_IDS = ["create", "update", "create_clone"]


def _process_step(plan):
    for step in plan.get("steps") or []:
        if step.get("key") == "p1":
            return step
    return None


# ---------------------------------------------------------------------------
# the gate refuses, on every authoring arm
# ---------------------------------------------------------------------------


#: The canonical twin of `_config`: the same process authored as a ProcessIR unit
#: in `integration_spec.processes` — the only shape a process body may take after
#: #159 S3. Supporting connection/operation rows come from the shared appliable
#: fixture, so nothing here models the emitter.
_UPDATE_ID = "12121212-1212-1212-1212-121212121212"


def _canonical_config(action, *, conflict_policy="reuse"):
    from _m12_11_support import APPLIABLE_CONN, APPLIABLE_OP, appliable_process_unit

    extra = {"component_id": _UPDATE_ID} if action == "update" else {}
    unit = appliable_process_unit(key="p1", name="GateProbe", action=action, **extra)
    return {
        "conflict_policy": conflict_policy,
        "integration_spec": {
            "version": "1.0",
            "name": "test-integration",
            "components": [dict(APPLIABLE_CONN), dict(APPLIABLE_OP)],
            "processes": [unit.model_dump(mode="json")],
        },
    }


@pytest.mark.parametrize("action, policy, metadata", _ARMS, ids=_ARM_IDS)
def test_a_fatal_payload_is_refused_on_every_authoring_arm(action, policy, metadata):
    """#159 S3: a flat process body never reaches the semantic gate any more — it
    is refused at admission, on every arm (the derived create_clone included),
    before any account lookup, with the value-free unit-required envelope."""
    with patch(_PATCH_TARGET) as mock_pag:
        mock_pag.return_value = metadata
        plan = IB._build_plan(
            MagicMock(), _config(action, _FATAL_FLOW, conflict_policy=policy)
        )
    assert mock_pag.call_count == 0
    assert _process_step(plan) is None
    assert plan["_success"] is False
    assert plan["error_code"] == PROCESS_COMPONENT_AUTHORING_UNIT_REQUIRED
    assert set(plan) == set(unit_required_envelope("x"))
    assert plan["field"] == "integration_spec.components.0.config.process_kind"
    assert _EXPECTED_CODE not in str(plan)
    assert "DDP_A" not in str(plan) and "GateProbe" not in str(plan)


@pytest.mark.parametrize("action, policy, metadata", _ARMS, ids=_ARM_IDS)
def test_a_clean_payload_still_plans_on_every_authoring_arm(action, policy, metadata):
    """The discriminator: a gate that refused everything would satisfy the
    refusal cases above while breaking every real build.

    #159 S3: the payload that must still plan is the CANONICAL twin, on every
    arm — the derived create_clone (a name collision under clone) included."""
    with patch(_PATCH_TARGET) as mock_pag:
        mock_pag.return_value = metadata
        plan = IB._build_plan(
            MagicMock(), _canonical_config(action, conflict_policy=policy)
        )
    assert plan["_success"] is True, plan
    step = _process_step(plan)
    assert step is not None
    assert step["planned_action"] == ("create_clone" if policy == "clone" else action)
    assert step.get("validation_error") is None


# ---------------------------------------------------------------------------
# and nothing mutates
# ---------------------------------------------------------------------------


def _apply(config, metadata):
    calls = []
    original = IB._execute_component
    IB._execute_component = lambda *a, **k: (calls.append(a), {})[1]
    try:
        with patch(_PATCH_TARGET) as mock_pag:
            mock_pag.return_value = metadata
            payload = dict(config)
            payload["dry_run"] = False
            result = IB._apply_plan(MagicMock(), "test-profile", payload)
    finally:
        IB._execute_component = original
    return result, calls


@pytest.mark.parametrize("action, policy, metadata", _ARMS, ids=_ARM_IDS)
def test_apply_executes_no_component_when_validation_is_fatal(action, policy, metadata):
    """The criterion itself, executed rather than read — with a would-write
    sibling AHEAD of the refused process (#159 S3)."""
    result, calls = _apply(
        _config(action, _FATAL_FLOW, conflict_policy=policy, sibling=True), metadata
    )
    assert calls == [], "a component was executed despite a fatal semantic finding"
    assert result.get("_success") is False
    assert result.get("error_code") == PROCESS_COMPONENT_AUTHORING_UNIT_REQUIRED


def test_apply_does_execute_a_clean_payload():
    """Proves the spy would have caught a mutation — a no-mutation assertion is
    worthless if `_execute_component` was never going to be called anyway.

    #159 S3: the same sibling-first spec with its process row ADMITTED (an
    explicit-id bind instead of a body) executes the sibling."""
    config = _config("create", _CLEAN_FLOW, sibling=True)
    config["integration_spec"]["components"][1] = {
        "key": "p1",
        "type": "process",
        "action": "create",
        "name": "GateProbe",
        "config": {
            "reference_only": True,
            "component_id": "13131313-1313-1313-1313-131313131313",
        },
    }
    _result, calls = _apply(config, [])
    assert calls, "the control run executed nothing, so the spy proves nothing"

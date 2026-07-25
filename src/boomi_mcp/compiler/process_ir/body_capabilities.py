"""Closed control-body capability registry (issue #141, M12.6).

The registry answers exactly one question per authored slot: *may this node kind
appear in this Branch-leg / Decision-arm position?* It is a closed ALLOWLIST —
a ``(context, slot, kind)`` triple that is not a row here is unsupported, full
stop. That is the same fail-closed shape #140's connector registry uses, and it
means no gated-row table is load-bearing: nested ``branch`` in a Branch leg,
``process_call`` on a Decision FALSE arm, ``return_documents`` anywhere in a body
and every future node kind are rejected by *absence*, not by remembering to list
them.

Why a registry when the Pydantic unions already reject these:

* the unions state the rule *structurally*, the registry states it *as data* —
  so the shipped matrix can be tested, documented and diffed in one place rather
  than read out of five ``Annotated[Union[...]]`` aliases;
* it is the enforcement point for the rules a union cannot express (the control
  DEPTH bound), and it runs before lowering, so every body defect precedes any
  CFG, emission plan, emitter — and therefore any component mutation;
* ``test_body_capability_registry_matches_the_model_unions`` pins the two against
  each other in BOTH directions, so a union that gains a kind without a
  deliberate registry change fails the build, and vice versa.

Evidence for every admitted placement is recorded in
``.codex/plans/issue-141-live-captures.md``; the per-row citations live in the
matrix below.

This module is compiler-internal: it is never exported from
``compiler.process_ir.__all__`` and no MCP tool, schema, or builder reaches it.
"""

from __future__ import annotations

from types import MappingProxyType
from typing import Any, Dict, FrozenSet, List, Mapping, Tuple

from ...errors import (
    PROCESS_IR_CAPABILITY_NODE_NOT_ALLOWED_IN_BODY,
    PROCESS_IR_SEMANTIC_NESTING_LIMIT,
)
from ...models.process_ir import (
    LINEAR_BODY_KINDS,
    PROCESS_IR_V1_MAX_CONTROL_DEPTH,
    ProcessIRV1,
)
from .diagnostics import raise_compile_error

_SEMANTIC_PHASE = "semantic_lowering"

#: Body contexts, spelled exactly as the authored JSON pointer segments they
#: correspond to (``/legs/N``, ``/true_arm``, ``/false_arm``).
BRANCH_LEG = "branch_leg"
DECISION_TRUE_ARM = "decision_true_arm"
DECISION_FALSE_ARM = "decision_false_arm"

STEP_SLOT = "step"
TERMINAL_SLOT = "terminal"

_LINEAR = frozenset(LINEAR_BODY_KINDS)

#: The closed matrix. Keyed by ``(context, slot)``; the value is the exact set of
#: admitted ``kind`` discriminators.
#:
#: Live evidence (`.codex/plans/issue-141-live-captures.md`):
#:
#: * ``connector_call`` in every body  -> capability ``connector_call_in_control_body``.
#: * ``process_call`` in a Branch leg  -> §2.2, seven Branch legs whose bodies run
#:   process calls; admitted only under PATH MODE (that body is ProcessCall-only
#:   and ends in ``stop``), which the model enforces.
#: * ``process_call`` on a TRUE arm    -> §2.2, ``decision ->true-> processcall``,
#:   twice. Absent from the FALSE arm: the capture attests TRUE outcomes only.
#: * ``decision`` as a Branch-leg terminal -> §2.1, leg 2 routes into a Decision.
#: * ``branch`` as a Branch-leg terminal   -> DELIBERATELY ABSENT. It appears
#:   nowhere in either captured process, and fail-closed is the rule for an
#:   unproven placement.
#: * bare ``stop`` terminals -> §2.1 proves a Decision FALSE outcome may route
#:   straight to a Stop. The model additionally requires a Branch leg / TRUE arm
#:   to do some work first (the empty-leg question is UNPROVEN, capture §2.4).
BODY_CAPABILITIES_V1: Mapping[Tuple[str, str], FrozenSet[str]] = MappingProxyType(
    {
        (BRANCH_LEG, STEP_SLOT): _LINEAR | {"connector_call", "process_call"},
        (BRANCH_LEG, TERMINAL_SLOT): frozenset({"target", "cache_put", "stop", "decision"}),
        (DECISION_TRUE_ARM, STEP_SLOT): _LINEAR | {"connector_call", "process_call"},
        (DECISION_TRUE_ARM, TERMINAL_SLOT): frozenset(
            {"target", "stop", "exception", "branch", "decision"}
        ),
        (DECISION_FALSE_ARM, STEP_SLOT): _LINEAR | {"connector_call"},
        (DECISION_FALSE_ARM, TERMINAL_SLOT): frozenset(
            {"stop", "exception", "branch", "decision"}
        ),
    }
)


def is_allowed(context: str, slot: str, kind: str) -> bool:
    """Absence is denial — there is no wildcard and no "known kind" fallback."""
    return kind in BODY_CAPABILITIES_V1.get((context, slot), frozenset())


def _check(context: str, slot: str, node: Any, path: str) -> None:
    kind = getattr(node, "kind", None)
    if not is_allowed(context, slot, str(kind)):
        raise raise_compile_error(
            PROCESS_IR_CAPABILITY_NODE_NOT_ALLOWED_IN_BODY,
            _SEMANTIC_PHASE,
            path,
            message="this node kind is not admitted in this control-body slot",
        )


def _pointer_escape(token: str) -> str:
    return token.replace("~", "~0").replace("/", "~1")


def _join(base: str, *parts: Any) -> str:
    out = base
    for part in parts:
        out += "/" + _pointer_escape(str(part))
    return out


def _walk_body(steps: List[Any], terminal: Any, context: str, path: str, depth: int) -> None:
    for index, step in enumerate(steps):
        _check(context, STEP_SLOT, step, _join(path, "steps", index))
    terminal_path = _join(path, "terminal")
    _check(context, TERMINAL_SLOT, terminal, terminal_path)
    _walk_control(terminal, terminal_path, depth)


def _walk_control(node: Any, path: str, depth: int) -> None:
    """Recurse into a control node, counting depth as we go.

    ``depth`` is the number of control nodes ALREADY entered on this root-to-leaf
    path, so the check fires at the first offending node and names its exact
    authored pointer rather than the document root.
    """
    kind = getattr(node, "kind", None)
    if kind not in ("branch", "decision"):
        return
    depth += 1
    if depth > PROCESS_IR_V1_MAX_CONTROL_DEPTH:
        raise raise_compile_error(
            PROCESS_IR_SEMANTIC_NESTING_LIMIT,
            _SEMANTIC_PHASE,
            path,
            message=(
                "branch/decision nesting exceeds the ProcessIR v1 maximum control "
                "depth (a compiler bound, not a Boomi platform limit)"
            ),
        )
    if kind == "branch":
        for leg_index, leg in enumerate(node.legs):
            _walk_body(
                list(leg.steps),
                leg.terminal,
                BRANCH_LEG,
                _join(path, "legs", leg_index),
                depth,
            )
        return
    _walk_body(
        list(node.true_arm.steps),
        node.true_arm.terminal,
        DECISION_TRUE_ARM,
        _join(path, "true_arm"),
        depth,
    )
    _walk_body(
        list(node.false_arm.steps),
        node.false_arm.terminal,
        DECISION_FALSE_ARM,
        _join(path, "false_arm"),
        depth,
    )


def validate_body_capabilities(ir: ProcessIRV1) -> None:
    """Check every control-body slot and the control-depth bound.

    Runs BEFORE lowering (see ``pipeline.compile_process_ir_v1``), so a body
    defect is rejected while nothing but the authored document exists.

    A document with no control node walks zero bodies and returns immediately,
    so no pre-#141 dialect changes behaviour.
    """
    for index, step in enumerate(ir.body.steps):
        _walk_control(step, _join("/body", "steps", index), 0)


def registry_kinds() -> Dict[Tuple[str, str], FrozenSet[str]]:
    """A plain-dict copy, for the coverage test that pins registry vs unions."""
    return {key: set(value) for key, value in BODY_CAPABILITIES_V1.items()}


__all__ = [
    "BODY_CAPABILITIES_V1",
    "BRANCH_LEG",
    "DECISION_FALSE_ARM",
    "DECISION_TRUE_ARM",
    "STEP_SLOT",
    "TERMINAL_SLOT",
    "is_allowed",
    "registry_kinds",
    "validate_body_capabilities",
]

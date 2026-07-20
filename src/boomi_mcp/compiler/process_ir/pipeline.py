"""Compiler entry points: parse -> lower -> check (issue #137).

Orchestration lives here rather than in ``diagnostics`` (where the architect
plan placed it) because ``diagnostics`` is imported BY ``lowering`` and
``invariants`` — hosting the entry points there would close an import cycle.

One compile is pure with respect to the same ``(ir, symbols)`` pair: it reads no
clock, no environment, and no global state, and it snapshots every authored
value it touches.
"""

from __future__ import annotations

from typing import Any, List, Tuple

from ...errors import PROCESS_IR_COMPILE_INTERNAL
from ...models.process_ir import (
    ProcessIRV1,
    ProcessIRValidationError,
    parse_process_ir_v1,
)
from .contracts import EmissionPlanV1, SemanticCfgV1, SymbolTableV1
from .diagnostics import (
    CompilerDiagnostic,
    ProcessIRCompileError,
    diagnostic,
    node_identity_for,
)
from .invariants import check_cfg_invariants, check_emission_plan_invariants
from .lowering import lower_cfg_to_emission_plan, lower_process_ir_to_cfg


def compile_process_ir_v1(
    ir: ProcessIRV1, symbols: SymbolTableV1
) -> Tuple[SemanticCfgV1, EmissionPlanV1]:
    """Lower a validated IR into its CFG and emission plan, invariant-checked.

    Any unexpected exception becomes a single static ``PROCESS_IR_COMPILE_INTERNAL``
    diagnostic. The exception's text and type are deliberately NOT interpolated:
    an internal message can carry authored values, and diagnostics are logged.
    """
    try:
        cfg = lower_process_ir_to_cfg(ir)
        check_cfg_invariants(cfg)
        plan = lower_cfg_to_emission_plan(cfg, symbols)
        check_emission_plan_invariants(plan, cfg, symbols)
        return cfg, plan
    except ProcessIRCompileError:
        raise
    except Exception:  # noqa: BLE001 - deliberate: never leak internals
        raise ProcessIRCompileError(
            [diagnostic(PROCESS_IR_COMPILE_INTERNAL, "emission_planning", "")]
        ) from None


def parse_and_compile_process_ir_v1(
    payload: Any, symbols: SymbolTableV1
) -> Tuple[ProcessIRV1, SemanticCfgV1, EmissionPlanV1]:
    """Parse an authored payload, then compile it.

    #136's parse diagnostics are translated into compiler diagnostics with
    ``phase="schema"`` and their ``code``/``path``/``message``/``remediation``
    preserved VERBATIM — renaming a shipped code here would break every caller
    that already keys on it (ADR-001 §7: later introducers add codes, never
    rename them).
    """
    try:
        ir = parse_process_ir_v1(payload)
    except ProcessIRValidationError as exc:
        raise ProcessIRCompileError(
            [
                CompilerDiagnostic(
                    code=item.code,
                    phase="schema",
                    path=item.path,
                    node_identity=node_identity_for(item.path),
                    message=item.message,
                    remediation=item.remediation,
                )
                for item in exc.diagnostics
            ]
        ) from None
    cfg, plan = compile_process_ir_v1(ir, symbols)
    return ir, cfg, plan


__all__: List[str] = [
    "compile_process_ir_v1",
    "parse_and_compile_process_ir_v1",
]

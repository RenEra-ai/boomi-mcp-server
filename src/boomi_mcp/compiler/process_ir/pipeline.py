"""Compiler entry points: parse -> lower -> check (issue #137).

Orchestration lives here rather than in ``diagnostics`` (where the architect
plan placed it) because ``diagnostics`` is imported BY ``lowering`` and
``invariants`` — hosting the entry points there would close an import cycle.

One compile is pure with respect to the same ``(ir, symbols)`` pair: it reads no
clock, no environment, and no global state, and it snapshots every authored
value it touches.
"""

from __future__ import annotations

from typing import TYPE_CHECKING, Any, List, Optional, Tuple

from ...errors import PROCESS_IR_COMPILE_INTERNAL
from ...models.process_ir import (
    ProcessIRV1,
    ProcessIRValidationError,
    parse_process_ir_v1,
)
from .body_capabilities import validate_body_capabilities
from .connector_resolution import validate_connector_calls
from .contracts import EmissionPlanV1, SemanticCfgV1, SymbolTableV1
from .diagnostics import (
    CompilerDiagnostic,
    ProcessIRCompileError,
    diagnostic,
    node_identity_for,
)
from .invariants import check_cfg_invariants, check_emission_plan_invariants
from .lowering import lower_cfg_to_emission_plan, lower_process_ir_to_cfg

if TYPE_CHECKING:  # pragma: no cover - typing only, avoids an import cycle
    from .semantic_validation.contracts import ProcessIRValidationCapabilitiesV1
    from .semantic_validation.validation_policy import LegacyValidationPolicyV1


def _guarded(phase, action, *args):
    """Run one compiler stage, converting an unexpected error into a diagnostic.

    The exception's text and type are deliberately discarded: an internal
    message can carry authored values, and diagnostics are logged.
    """
    try:
        return action(*args)
    except ProcessIRCompileError:
        raise
    except Exception:  # noqa: BLE001 - deliberate: never leak internals
        raise ProcessIRCompileError(
            [diagnostic(PROCESS_IR_COMPILE_INTERNAL, phase, "")]
        ) from None


def _enforce_semantic_report(ir, cfg, symbols, policy, capabilities) -> None:
    """Block the compile when the unified semantic report has ERRORS.

    Imported lazily: ``semantic_validation`` imports the compiler's own
    contracts, so a module-level import here would close a cycle.

    ``capabilities`` is the trusted-contract set. Without it the gate always ran
    with ``DEFAULT_VALIDATION_CAPABILITIES``, so a flow that is valid ONLY
    because a typed map/script/subprocess contract establishes an effect was
    rejected by the compiler even though ``validate_process_ir`` called with the
    same contracts reports it valid — which made the typed capability surface
    unusable for canonical compilation.

    ``policy`` is a legacy adapter's exemption set, or None for STRICT. Applying
    it here rather than at an outer boundary is what lets the canonical path be
    gated for everyone while a migrated dialect keeps the behaviour its goldens
    pin — the two are not in tension once the adapter passes its identity in.

    Errors block; warnings and advisories do not — the compile contract is an
    artifact or an exception, with nowhere to carry a non-blocking finding.
    """
    from .semantic_validation.pipeline import validate_lowered_process_ir
    from .semantic_validation.validation_policy import apply_policy

    from .semantic_validation.contracts import DEFAULT_VALIDATION_CAPABILITIES

    report = validate_lowered_process_ir(
        ir, cfg, symbols, capabilities or DEFAULT_VALIDATION_CAPABILITIES
    )
    report = apply_policy(report, policy)
    if not report.errors:
        return
    raise ProcessIRCompileError(
        [
            CompilerDiagnostic(
                code=item.code,
                phase="semantic_lowering",
                path=item.path,
                node_identity=item.node_identity,
                message=item.message,
                remediation=item.remediation,
                internal_node_id=item.internal_node_id,
            )
            for item in report.errors
        ]
    )


def compile_process_ir_v1(
    ir: ProcessIRV1,
    symbols: SymbolTableV1,
    *,
    validation_policy: Optional["LegacyValidationPolicyV1"] = None,
    capabilities: Optional["ProcessIRValidationCapabilitiesV1"] = None,
) -> Tuple[SemanticCfgV1, EmissionPlanV1]:
    """Lower a validated IR into its CFG and emission plan, invariant-checked.

    Any unexpected exception becomes a single static ``PROCESS_IR_COMPILE_INTERNAL``
    diagnostic. The exception's text and type are deliberately NOT interpolated:
    an internal message can carry authored values, and diagnostics are logged.

    ``validation_policy`` is how a LEGACY dialect carries its exemptions into the
    canonical gate. It is a keyword with a STRICT default, so the compiler is
    gated for every caller and a legacy adapter opts into its own documented
    leniency rather than the compiler guessing an identity it cannot see.

    An earlier attempt put this gate at ``emit_legacy_result`` instead, on the
    reasoning that the compiler cannot know which adapter produced its IR and so
    cannot look a policy up. That is true and beside the point: the adapter knows,
    and can pass it. Leaving the canonical path ungated meant a direct caller of
    this function got no semantic validation at all — which is the acceptance
    criterion this issue exists to satisfy.
    """
    # Phase is part of the diagnostic contract, so an unexpected defect is
    # attributed to the stage it actually happened in — reporting a CFG-lowering
    # crash as "emission_planning" sends a reader to the wrong half of the
    # compiler.
    # #141: control-body slots and the control-depth bound are checked FIRST, on
    # the authored document, before a CFG exists. Two reasons it leads: a body
    # defect names an authored JSON pointer that is meaningless once flattened
    # into CFG nodes, and every semantic error must precede any mutation. A
    # document with no branch/decision walks nothing and returns immediately.
    _guarded("semantic_lowering", validate_body_capabilities, ir)
    cfg = _guarded("semantic_lowering", lower_process_ir_to_cfg, ir)
    _guarded("semantic_lowering", check_cfg_invariants, cfg)
    # #140: resolve and validate connector calls BEFORE any emission plan exists,
    # so a bad reference, an unsupported family/action, an impossible document
    # cardinality, or a map whose profiles do not line up is rejected long before
    # an emitter — and therefore before any component mutation. A CFG with no
    # connector_call node returns immediately, so no pre-#140 dialect is touched.
    # The phase here only labels an UNEXPECTED crash; the deliberate diagnostics
    # inside carry their own (reference_resolution / semantic_lowering).
    _guarded("reference_resolution", validate_connector_calls, cfg, symbols)
    # #143: the unified semantic gate. It runs on the CFG that was just lowered
    # and BEFORE any emission plan exists, so "no plan lowering occurs with
    # report errors" holds by construction rather than by convention.
    # Through `_guarded`, like every other stage: a deliberate finding raises
    # ProcessIRCompileError and passes through untouched, while an UNEXPECTED
    # collector or policy failure becomes the promised value-free
    # PROCESS_IR_COMPILE_INTERNAL instead of escaping as a raw exception with
    # its text — production builders catch ProcessIRCompileError, not anything.
    _guarded(
        "semantic_lowering",
        _enforce_semantic_report,
        ir,
        cfg,
        symbols,
        validation_policy,
        capabilities,
    )
    plan = _guarded("emission_planning", lower_cfg_to_emission_plan, cfg, symbols)
    _guarded("emission_planning", check_emission_plan_invariants, plan, cfg, symbols)
    return cfg, plan


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
    except ProcessIRCompileError:
        raise
    except Exception:  # noqa: BLE001 - deliberate: never leak internals
        # An UNEXPECTED parser failure must not escape carrying its text: the
        # message can echo authored values, and diagnostics get logged. The
        # compile stages are already guarded this way; parse was not.
        raise ProcessIRCompileError(
            [diagnostic(PROCESS_IR_COMPILE_INTERNAL, "schema", "")]
        ) from None
    cfg, plan = compile_process_ir_v1(ir, symbols)
    return ir, cfg, plan


__all__: List[str] = [
    "compile_process_ir_v1",
    "parse_and_compile_process_ir_v1",
]

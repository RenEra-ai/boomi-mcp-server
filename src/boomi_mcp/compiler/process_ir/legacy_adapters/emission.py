"""Canonical emission for a normalized legacy adapter result (issue #139 M12.4).

Turns a :class:`LegacyAdapterResultV1` into the ordered process-shape XML parts
by driving the ONE canonical chain:

    result.process_ir --compile_process_ir_v1--> EmissionPlanV1
                       --emit_process(plan, symbols)--> shape_xml_parts

The symbol table is built from the adapter's requirements. Each symbol's ``ref``
is the requirement's ``ir_ref`` (for the flow adapter an OCCURRENCE-SCOPED
``$ref:legacy.adapter:<pointer>`` alias; for the wrapper the process id itself),
while its ``component_id`` resolves the requirement's ``legacy_selector`` — the
ORIGINAL id, NEVER the synthetic alias. On the build path the selector is already
a resolved literal component id, so the default resolver is identity; the
integration-builder plan preflight passes a resolver that maps an unresolved
``$ref:KEY`` selector to a deterministic placeholder id. Because the resolver
receives only the selector, aliases never reach emitted XML, and two aliases that
share one selector become two symbols with one ``component_id``.
"""

from __future__ import annotations

from typing import Callable, Optional

from ....errors import LEGACY_ADAPTER_OUTPUT_PARITY_FAILED
from ..contracts import ComponentSymbolV1, SymbolTableV1
from ..diagnostics import ProcessIRCompileError
from ..emitter_registry import ProcessEmissionArtifactV1, emit_process
from ..pipeline import compile_process_ir_v1
from .contracts import LegacyAdapterResultV1, adapter_diagnostic

Resolver = Callable[[str], str]


def _symbol_table(result: LegacyAdapterResultV1, resolver: Resolver) -> SymbolTableV1:
    # ref is the occurrence-scoped IR alias; component_id resolves the ORIGINAL
    # legacy_selector (never the synthetic alias). Two aliases sharing one selector
    # therefore become two symbols with the same component_id but their own type
    # and connector metadata — no alias ever reaches emitted XML (#139B).
    symbols = tuple(
        ComponentSymbolV1(
            ref=req.ir_ref,
            component_id=resolver(req.legacy_selector),
            component_type=req.expected_component_type,
            connector_type=req.connector_type,
            action_type=req.action_type,
        )
        for req in result.symbol_requirements
    )
    return SymbolTableV1(symbols=symbols)


def emit_legacy_result(
    result: LegacyAdapterResultV1, *, resolver: Optional[Resolver] = None
) -> ProcessEmissionArtifactV1:
    """Compile + emit a normalized legacy result into a verified process artifact.

    A canonical compile/emit/verify failure AFTER successful legacy validation is
    an output-parity defect: it is wrapped as a value-free ``LegacyAdapterError``
    carrying ``LEGACY_ADAPTER_OUTPUT_PARITY_FAILED`` (the compiler error chained
    internally), so the caller translates it to its existing public builder error
    family while the internal cause is the plan-mandated code.
    """
    resolve: Resolver = resolver or (lambda ref: ref)
    symbols = _symbol_table(result, resolve)
    try:
        _cfg, plan = compile_process_ir_v1(result.process_ir, symbols)
        return emit_process(plan, symbols)
    except ProcessIRCompileError as exc:
        raise adapter_diagnostic(
            LEGACY_ADAPTER_OUTPUT_PARITY_FAILED,
            "/",
            "canonical compile/emit of a validated legacy config failed",
        ) from exc


__all__ = ["emit_legacy_result"]

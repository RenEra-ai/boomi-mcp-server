"""Canonical emission for a normalized legacy adapter result (issue #139 M12.4).

Turns a :class:`LegacyAdapterResultV1` into the ordered process-shape XML parts
by driving the ONE canonical chain:

    result.process_ir --compile_process_ir_v1--> EmissionPlanV1
                       --emit_process(plan, symbols)--> shape_xml_parts

The symbol table is built from the adapter's requirements. On the build path a
requirement's ``ir_ref`` is already a resolved literal component id, so the
default resolver is identity; the integration-builder plan preflight passes a
resolver that maps an unresolved ``$ref:KEY`` to a deterministic placeholder id.
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
    symbols = tuple(
        ComponentSymbolV1(
            ref=req.ir_ref,
            component_id=resolver(req.ir_ref),
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

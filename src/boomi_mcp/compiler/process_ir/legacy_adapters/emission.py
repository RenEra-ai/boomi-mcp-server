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

from ..contracts import ComponentSymbolV1, SymbolTableV1
from ..emitter_registry import ProcessEmissionArtifactV1, emit_process
from ..pipeline import compile_process_ir_v1
from .contracts import LegacyAdapterResultV1

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

    Raises ``ProcessIRCompileError`` on any compile/emit/verify failure — the
    caller translates that to its existing public builder error family.
    """
    resolve: Resolver = resolver or (lambda ref: ref)
    symbols = _symbol_table(result, resolve)
    _cfg, plan = compile_process_ir_v1(result.process_ir, symbols)
    return emit_process(plan, symbols)


__all__ = ["emit_legacy_result"]

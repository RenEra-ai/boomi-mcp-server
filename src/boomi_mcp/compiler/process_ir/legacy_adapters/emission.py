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

from dataclasses import dataclass
from typing import Callable, Optional

from ....errors import LEGACY_ADAPTER_OUTPUT_PARITY_FAILED
from ..contracts import ComponentSymbolV1, SymbolTableV1
from ..diagnostics import CompilerDiagnostic, ProcessIRCompileError
from ..emitter_registry import ProcessEmissionArtifactV1, emit_process
from ..execution_profile import derive_process_execution_profile
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


@dataclass(frozen=True)
class LegacyEmissionV1:
    """A verified artifact together with the execution profile compiled beside it.

    #158. A caller assembling the ``<process>`` element needs the profile, and it
    must come from the SAME compile that produced the shapes — deriving it again
    from the config, or from a second compile, is a second authority that could
    disagree with the shapes it is wrapped around.
    """

    artifact: ProcessEmissionArtifactV1
    execution_profile: str


def emit_legacy_result(
    result: LegacyAdapterResultV1,
    *,
    resolver: Optional[Resolver] = None,
    dialect: Optional[str] = None,
) -> ProcessEmissionArtifactV1:
    """Compile + emit a normalized legacy result into a verified process artifact.

    The artifact half of :func:`emit_legacy_result_with_profile`, kept for the
    callers that assemble no ``<process>`` options of their own.
    """
    return emit_legacy_result_with_profile(
        result, resolver=resolver, dialect=dialect
    ).artifact


def emit_legacy_result_with_profile(
    result: LegacyAdapterResultV1,
    *,
    resolver: Optional[Resolver] = None,
    dialect: Optional[str] = None,
) -> LegacyEmissionV1:
    """Compile + emit a normalized legacy result, returning the derived profile too.

    A canonical compile/emit/verify failure AFTER successful legacy validation is
    an output-parity defect: it is wrapped as a value-free ``LegacyAdapterError``
    carrying ``LEGACY_ADAPTER_OUTPUT_PARITY_FAILED`` (the compiler error chained
    internally), so the caller translates it to its existing public builder error
    family while the internal cause is the plan-mandated code.
    """
    from ..semantic_validation.validation_policy import lookup_policy

    resolve: Resolver = resolver or (lambda ref: ref)
    symbols = _symbol_table(result, resolve)
    try:
        # #143: the unified gate lives in `compile_process_ir_v1`, which runs it
        # for EVERY caller before any emission plan exists. This adapter's only
        # job is to hand its identity across, so the dialect's registered
        # exemptions apply to its own goldens.
        #
        # An earlier version gated here instead and skipped entirely when
        # `dialect` was None, on the reasoning that the compiler cannot look up
        # a policy it has no identity for. True, and beside the point: the
        # ADAPTER has the identity and can pass it. Gating out here left every
        # direct compiler caller unvalidated — the acceptance criterion this
        # issue exists to satisfy. `dialect=None` now means STRICT, not skipped.
        cfg, plan = compile_process_ir_v1(
            result.process_ir,
            symbols,
            validation_policy=lookup_policy(dialect) if dialect else None,
        )
        return LegacyEmissionV1(
            artifact=emit_process(plan, symbols),
            execution_profile=derive_process_execution_profile(cfg, symbols),
        )
    except ProcessIRCompileError as exc:
        raise adapter_diagnostic(
            LEGACY_ADAPTER_OUTPUT_PARITY_FAILED,
            "/",
            "canonical compile/emit of a validated legacy config failed",
        ) from exc


__all__ = ["LegacyEmissionV1", "emit_legacy_result", "emit_legacy_result_with_profile"]

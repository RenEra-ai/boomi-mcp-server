"""Legacy config -> ProcessIR -> semantic report, as a pure helper (#143 slice 8).

This is the seam the plan preflight uses. It is deliberately a separate module
from ``pipeline`` so the canonical validator keeps a single, strict, adapter-free
signature: ``validate_process_ir`` never learns what a legacy dialect is, and
the only code that can select an exemption policy is here.

Fail-OPEN on projection, fail-CLOSED on findings
------------------------------------------------
Two different failure directions, chosen deliberately:

* If the config belongs to a dialect that is not migrated, or the adapter cannot
  project it, this returns NO findings. Projection is not this gate's
  responsibility — the legacy validators already ran and either passed it or
  rejected it, and inventing a semantic error out of a projection failure would
  blame the author for a compiler-side gap.
* If projection succeeds, the report is authoritative and its errors block.

Precedence
----------
The caller runs this LAST, after every existing secret / ``$ref``-type /
legacy-lineage check. That ordering is load-bearing: existing public error codes
must keep winning, so a payload that fails a legacy check reports the legacy code
it has always reported, not a new ProcessIR one. This gate can only add a
rejection the legacy path missed — never rename one it already made.
"""

from __future__ import annotations

from typing import Any, Dict, List, Optional, Tuple

from ....errors import PROCESS_IR_COMPILE_INTERNAL
from ..contracts import ComponentSymbolV1, SymbolTableV1
from ..diagnostics import CompilerDiagnostic, ProcessIRCompileError
from .contracts import (
    DEFAULT_VALIDATION_CAPABILITIES,
    ProcessIRValidationCapabilitiesV1,
    ValidationReportV1,
)
from .pipeline import validate_process_ir
from .validation_policy import apply_policy, lookup_policy


def _adapter_for_config(process_kind: str, raw_config: Dict[str, Any]):
    """``(adapter, policy_name)`` for a legacy config, or ``(None, None)``.

    Mirrors the dialect split the builders already use — each builder calls its
    own adapter directly — rather than inventing a second dialect resolver that
    could disagree with them.
    """
    kind = (process_kind or "").strip()

    if kind == "wrapper_subprocess":
        from ..legacy_adapters.wrapper_subprocess import adapt_wrapper_subprocess

        return adapt_wrapper_subprocess, "wrapper_subprocess"

    if kind == "sync_pipeline":
        # ``adapt_sync_pipeline_config``, NOT ``adapt_sync_pipeline``: the inner
        # function consumes an already-lowered core, and the registry docstring
        # is explicit that a dialect must be entered through the wrapper that
        # accepts its RAW config. Binding the inner one would hand it a shape it
        # does not accept and the projection would fail silently.
        from ..legacy_adapters.sync_pipeline import adapt_sync_pipeline_config

        return adapt_sync_pipeline_config, "sync_pipeline"

    flow_sequence = raw_config.get("flow_sequence")
    if isinstance(flow_sequence, list) and flow_sequence:
        from ..legacy_adapters.flow_sequence import adapt_flow_sequence

        return adapt_flow_sequence, "flow_sequence"

    return None, None


def _symbols_from(result) -> SymbolTableV1:
    """The symbol table the adapter's requirements describe.

    Component ids are not resolved here — validation is ACCOUNT-INDEPENDENT and
    runs before any lookup. The requirement's own ``ir_ref`` stands in as the id,
    which is sufficient because every semantic rule keys on the reference and its
    declared component TYPE, never on a live id.
    """
    return SymbolTableV1(
        symbols=tuple(
            ComponentSymbolV1(
                ref=req.ir_ref,
                component_id=req.ir_ref,
                component_type=req.expected_component_type,
                connector_type=req.connector_type,
                action_type=req.action_type,
            )
            for req in result.symbol_requirements
        )
    )


def validate_legacy_process_config(
    process_kind: str,
    raw_config: Dict[str, Any],
    capabilities: ProcessIRValidationCapabilitiesV1 = DEFAULT_VALIDATION_CAPABILITIES,
) -> Optional[ValidationReportV1]:
    """Project a legacy config into IR and validate it under its adapter policy.

    Returns ``None`` when the dialect is not migrated or cannot be projected —
    see the fail-open note above. Pure: no network, no filesystem, no mutation.
    """
    adapter, policy_name = _adapter_for_config(process_kind, raw_config)
    if adapter is None:
        return None

    try:
        result = adapter(raw_config)
    except Exception:  # noqa: BLE001 — projection failure is not a semantic defect
        return None

    process_ir = getattr(result, "process_ir", None)
    if process_ir is None:
        return None

    # A compiler defect is not the author's fault — but it is not a PASS either.
    # This used to swallow every exception and return None, which the plan gate
    # reads as "nothing blocks", so an internal failure of the validator became
    # a silent approval of an UNVALIDATED payload. `validate_process_ir`
    # documents that it raises only on a compiler defect, so the exception is
    # propagated for the caller to classify under the compiler's own
    # PROCESS_IR_COMPILE_INTERNAL. Fail closed: an unvalidated process does not
    # get built.
    try:
        report = validate_process_ir(process_ir, _symbols_from(result), capabilities)
    except ProcessIRCompileError:
        raise
    except Exception:  # noqa: BLE001 — anything else is also a defect here
        raise ProcessIRCompileError(
            [
                CompilerDiagnostic(
                    code=PROCESS_IR_COMPILE_INTERNAL,
                    phase="semantic_lowering",
                    path="",
                    node_identity="",
                    message="semantic validation failed unexpectedly",
                    remediation=(
                        "This is a compiler defect, not a payload error. "
                        "Please report it with the process configuration."
                    ),
                )
            ]
        ) from None  # never chain: the raw text can carry authored values

    return apply_policy(report, lookup_policy(policy_name))


def blocking_codes(report: Optional[ValidationReportV1]) -> Tuple[str, ...]:
    """Stable codes that would block, in report order. Empty when nothing does."""
    if report is None:
        return ()
    return tuple(item.code for item in report.errors)


__all__ = ["blocking_codes", "validate_legacy_process_config"]

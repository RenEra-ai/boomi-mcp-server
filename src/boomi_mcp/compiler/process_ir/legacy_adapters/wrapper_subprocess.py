"""Production ``wrapper_subprocess`` -> ProcessIR adapter (issue #139 M12.4).

Normalizes an already-validated wrapper-parent config (``start -> process
call(s) -> stop|return_documents``) into a :class:`ProcessIRV1` plus one
``process`` symbol requirement per called child. Envelope data (description,
folder, ``process_extensions``) is NOT represented here — the component
assembler owns it, exactly as the legacy ``WrapperSubprocessBuilder.build``
does. The IR is built to mirror that build's resolved-ref semantics
(``process_id or subprocess_ref``, already substituted by integration_builder
before build), NOT the #136 codec's unresolved ``$ref:KEY`` semantics.
"""

from __future__ import annotations

from typing import Any, Dict, List

from ....errors import LEGACY_ADAPTER_SEMANTIC_LOSS
from ....models.process_ir import parse_process_ir_v1
from .contracts import (
    LegacyAdapterResultV1,
    LegacySymbolRequirementV1,
    adapter_diagnostic,
)

# Root keys the adapter or the surrounding envelope assembler consumes. Anything
# else is a safe unknown extra the legacy path accepted-and-ignored — recorded as
# a noop path, never rejected (no unknown-field tightening; ADR-001 backward
# compat).
_KNOWN_ROOT_KEYS = frozenset(
    {
        "process_kind",
        "process_type",
        "process_calls",
        "return_documents",
        "description",
        "process_extensions",
    }
)
_KNOWN_CALL_KEYS = frozenset(
    {"subprocess_ref", "process_id", "wait", "abort_on_error", "label"}
)


def adapt_wrapper_subprocess(config: Dict[str, Any]) -> LegacyAdapterResultV1:
    """Normalize a validated wrapper_subprocess config into IR + requirements."""
    calls = config.get("process_calls") or []
    noop_paths: List[str] = sorted(f"/{k}" for k in set(config) - _KNOWN_ROOT_KEYS)

    steps: List[Dict[str, Any]] = []
    requirements: List[LegacySymbolRequirementV1] = []
    seen_refs = set()
    for i, call in enumerate(calls):
        if not isinstance(call, dict):
            # validate_config already guaranteed dict entries; stay total.
            raise adapter_diagnostic(
                LEGACY_ADAPTER_SEMANTIC_LOSS,
                f"/process_calls/{i}",
                "each process call must be a JSON object",
            )
        # Mirror WrapperSubprocessBuilder.build: process_id (literal) or
        # subprocess_ref (a $ref:KEY already resolved to an id before build()).
        selector_key = "process_id" if call.get("process_id") else "subprocess_ref"
        pid = str(call.get("process_id") or call.get("subprocess_ref") or "").strip()
        if not pid:
            raise adapter_diagnostic(
                LEGACY_ADAPTER_SEMANTIC_LOSS,
                f"/process_calls/{i}",
                "process call is missing a resolved target process id",
            )
        node: Dict[str, Any] = {"kind": "process_call", "process_ref": pid}
        for flag in ("wait", "abort_on_error"):
            # Present-and-boolean only (validate_config rejects non-bool); absent
            # falls back to the IR model default, matching build()'s
            # bool(call.get(flag, <default>)).
            if flag in call and isinstance(call[flag], bool):
                node[flag] = call[flag]
        if call.get("label") is not None:
            # _validate_processcall_entry does not type-check label; the pre-#139
            # emitter coerced it with str(... or ""). Reproduce that exactly so a
            # validated non-string label (e.g. 7) survives strict ProcessIR
            # parsing byte-identically (a falsy 0/False maps to "").
            node["label"] = str(call["label"] or "")
        steps.append(node)
        noop_paths.extend(
            f"/process_calls/{i}/{k}" for k in sorted(set(call) - _KNOWN_CALL_KEYS)
        )
        if pid not in seen_refs:
            seen_refs.add(pid)
            requirements.append(
                LegacySymbolRequirementV1(
                    role=f"process_call[{i}]",
                    ir_ref=pid,
                    # Wrapper calls are NOT role-scoped: a repeated child is the
                    # same `process` component with no connector metadata, so
                    # same-id dedup stays correct and legacy_selector == ir_ref.
                    legacy_selector=pid,
                    source_pointer=f"/process_calls/{i}/{selector_key}",
                    expected_component_type="process",
                )
            )

    rd = config.get("return_documents")
    if isinstance(rd, dict) and rd.get("enabled") is True:
        terminal: Dict[str, Any] = {"kind": "return_documents"}
        if rd.get("label") is not None:
            terminal["label"] = str(rd["label"] or "")
        steps.append(terminal)
    else:
        steps.append({"kind": "stop"})

    ir = parse_process_ir_v1(
        {"version": "1", "body": {"kind": "sequence", "steps": steps}}
    )
    return LegacyAdapterResultV1(
        process_ir=ir,
        symbol_requirements=tuple(requirements),
        compatibility_noop_paths=tuple(sorted(set(noop_paths))),
        pipeline_view=None,
        pipeline_view_status="not_representable",
    )


__all__ = ["adapt_wrapper_subprocess"]

"""Frozen internal contracts for the legacy-config -> ProcessIR adapter boundary.

Issue #139 (M12.4). A legacy adapter takes a config that has ALREADY passed its
own legacy validator and normalizes it into a :class:`ProcessIRV1` plus the
component-symbol facts the canonical compiler/emitter needs — and NOTHING an
adapter must never carry: no XML, CFG edges, emission nodes, layout, shape ids,
credentials, raw legacy config, descriptions, folders, or process extensions
(ADR-001 §6). Connector metadata is DERIVED here and rides on the *operation*
symbol requirement, mirroring the #136 codec's ``_resolve_binding`` and the
compiler's :class:`ComponentSymbolV1`.

These are internal diagnostics. A migrated public authoring entrypoint keeps its
existing external error contract; an adapter failure on already-validated input
is translated to the builder family (normally ``PROCESS_XML_VALIDATION_FAILED``)
by the caller before it reaches a user.
"""

from __future__ import annotations

from typing import Any, Optional, Tuple

from pydantic import BaseModel, ConfigDict, Field

from ....models.process_ir import ProcessIRV1


class _AdapterModel(BaseModel):
    """Frozen, strict base for every adapter contract.

    ``__repr_args__`` redacts every field except the value-free ones, so a repr
    (which can reach a log) never echoes an opaque component selector, a source
    pointer's neighbour value, or any authored content.
    """

    model_config = ConfigDict(extra="forbid", frozen=True)

    _REPR_SAFE_FIELDS = frozenset({"role", "expected_component_type", "pipeline_view_status"})

    def __repr_args__(self) -> Any:
        for key, value in super().__repr_args__():
            if key in self._REPR_SAFE_FIELDS:
                yield key, value
            else:
                yield key, "..."


class LegacySymbolRequirementV1(_AdapterModel):
    """One reference the adapter needs the caller to resolve into a component.

    ``ir_ref`` is the OCCURRENCE-SCOPED alias present in the ``ProcessIRV1`` — a
    ``$ref:legacy.adapter:<RFC6901-pointer>`` token that embeds NO authored value.
    ``legacy_selector`` is the original post-projection/post-coercion literal
    component id (or authored ``$ref:KEY``) the caller resolves to a component id.
    Distinct aliases may therefore resolve to the SAME component id while keeping
    their own component type and connector metadata, so one id reused across roles
    never collapses into an incompatible symbol (#139B). Connector metadata is
    present ONLY on the operation requirement of a connector binding
    (``expected_component_type == "connector-action"``).
    """

    role: str = Field(..., min_length=1)
    ir_ref: str = Field(..., min_length=1)
    legacy_selector: str = Field(..., min_length=1)
    source_pointer: str = Field(..., min_length=1)
    expected_component_type: str = Field(..., min_length=1)
    connector_type: Optional[str] = None
    action_type: Optional[str] = None


class LegacyAdapterResultV1(_AdapterModel):
    """The normalized output of one legacy adapter.

    ``pipeline_view`` / ``pipeline_view_status`` are the strict-authority (#139
    ``version="1.1"``) hooks; that selector is DESIGNED-not-activated in this
    slice, so migrated dialects report ``not_representable`` with a ``None`` view.
    """

    process_ir: ProcessIRV1
    symbol_requirements: Tuple[LegacySymbolRequirementV1, ...] = ()
    compatibility_noop_paths: Tuple[str, ...] = ()
    pipeline_view: Optional[Any] = None
    pipeline_view_status: str = "not_representable"


class LegacyAdapterDiagnosticV1(_AdapterModel):
    """A value-free structured cause raised inside the adapter boundary."""

    code: str = Field(..., min_length=1)
    legacy_source_path: str = Field(..., min_length=1)
    remediation: str = Field(..., min_length=1)


class LegacyAdapterError(Exception):
    """Internal adapter failure carrying one or more value-free diagnostics.

    Never surfaced raw to a caller — a migrated entrypoint translates it to its
    existing public error family. The string form is value-free.
    """

    def __init__(self, diagnostics: Tuple[LegacyAdapterDiagnosticV1, ...]):
        self.diagnostics = tuple(diagnostics)
        super().__init__(
            "; ".join(f"{d.code}@{d.legacy_source_path}" for d in self.diagnostics)
        )


def adapter_diagnostic(
    code: str, legacy_source_path: str, remediation: str
) -> LegacyAdapterError:
    """Build a single-diagnostic :class:`LegacyAdapterError` (value-free)."""
    return LegacyAdapterError(
        (
            LegacyAdapterDiagnosticV1(
                code=code,
                legacy_source_path=legacy_source_path or "/",
                remediation=remediation,
            ),
        )
    )


__all__ = [
    "LegacySymbolRequirementV1",
    "LegacyAdapterResultV1",
    "LegacyAdapterDiagnosticV1",
    "LegacyAdapterError",
    "adapter_diagnostic",
]

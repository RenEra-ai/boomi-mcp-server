"""Legacy-config -> ProcessIR adapters (issue #139 M12.4).

The production boundary that lets an already-validated legacy authoring config
enter the ONE canonical path ``ProcessIRV1 -> compile_process_ir_v1 ->
EmissionPlanV1 -> emit_process`` instead of a per-dialect XML renderer. An
adapter produces IR plus the component-symbol facts the compiler/emitter needs
and NOTHING else (no XML, layout, shape ids, CFG edges, credentials, envelope
data) — ADR-001 §6.

DARK, like the compiler and the emitter registry: this package is imported
DIRECTLY by the migrated build paths, never via ``process_ir.__all__``. No MCP
tool or JSON Schema constructs or consumes these types.
"""

from .contracts import (
    LegacyAdapterDiagnosticV1,
    LegacyAdapterError,
    LegacyAdapterResultV1,
    LegacySymbolRequirementV1,
)
from .registry import (
    FLOW_SEQUENCE_DIALECT,
    RESERVED_DIALECTS,
    WRAPPER_SUBPROCESS_DIALECT,
    adapter_for,
    is_migrated,
    migrated_dialects,
)

__all__ = [
    "LegacyAdapterDiagnosticV1",
    "LegacyAdapterError",
    "LegacyAdapterResultV1",
    "LegacySymbolRequirementV1",
    "FLOW_SEQUENCE_DIALECT",
    "RESERVED_DIALECTS",
    "WRAPPER_SUBPROCESS_DIALECT",
    "adapter_for",
    "is_migrated",
    "migrated_dialects",
]

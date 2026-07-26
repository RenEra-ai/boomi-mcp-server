"""Unified semantic validation over ProcessIRV1 (issue #143, M12.8).

DARK as of slice 1: this package is imported by no compiler stage, adapter,
builder, MCP tool, or plan/apply path. It ships its contract layer first so the
collectors that follow attach to a frozen, already-tested surface.

The public surface is deliberately narrow — the validator entry point, the
report/diagnostic contracts, and the typed capability contracts. Prepared
contexts, resolved facts and policy hooks stay private: they are compiler
internals, and exporting them would let a caller assemble a context the
validator never checked.

See ``docs/architecture/PROCESS_IR_SEMANTIC_VALIDATION_V1.md`` (slice 10) and
the design plan for the normative phase order.
"""

from typing import List

from .contracts import (
    VALIDATION_PHASE_ORDER,
    VALIDATION_SEVERITY_ORDER,
    ValidationDiagnosticV1,
    ValidationEvidenceV1,
    ValidationPhaseV1,
    ValidationReportV1,
    ValidationSeverityV1,
    build_validation_report,
    canonical_report_json,
)

__all__: List[str] = [
    "VALIDATION_PHASE_ORDER",
    "VALIDATION_SEVERITY_ORDER",
    "ValidationDiagnosticV1",
    "ValidationEvidenceV1",
    "ValidationPhaseV1",
    "ValidationReportV1",
    "ValidationSeverityV1",
    "build_validation_report",
    "canonical_report_json",
]

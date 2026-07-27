"""Unified semantic validation over ProcessIRV1 (issue #143, M12.8).

DARK through slice 7: this package is imported by no compiler stage, adapter,
builder, MCP tool, or plan/apply path. Slice 8 makes it a mutation gate.

The public surface is deliberately narrow — the validator entry point, the
report/diagnostic contracts, and the typed capability contracts. Prepared
contexts, collectors and policy hooks stay private: they are compiler
internals, and exporting them would let a caller assemble a context the
validator never checked.

See ``docs/architecture/PROCESS_IR_SEMANTIC_VALIDATION_V1.md`` (slice 10) for the
normative phase order and diagnostic schema.
"""

from typing import List

from .contracts import (
    DEFAULT_VALIDATION_CAPABILITIES,
    VALIDATION_PHASE_ORDER,
    VALIDATION_SEVERITY_ORDER,
    MapEffectContractV1,
    ProcessIRValidationCapabilitiesV1,
    ScriptEffectContractV1,
    STATE_SCOPES,
    ExternalWriterContractV1,
    StateEffectV1,
    SubprocessSummaryV1,
    ValidationDiagnosticV1,
    ValidationEvidenceV1,
    ValidationPhaseV1,
    ValidationReportV1,
    ValidationSeverityV1,
    build_validation_report,
    canonical_report_json,
)
from .pipeline import validate_process_ir

__all__: List[str] = [
    "DEFAULT_VALIDATION_CAPABILITIES",
    "MapEffectContractV1",
    "ProcessIRValidationCapabilitiesV1",
    "ScriptEffectContractV1",
    "ExternalWriterContractV1",
    "STATE_SCOPES",
    "StateEffectV1",
    "SubprocessSummaryV1",
    "VALIDATION_PHASE_ORDER",
    "VALIDATION_SEVERITY_ORDER",
    "ValidationDiagnosticV1",
    "ValidationEvidenceV1",
    "ValidationPhaseV1",
    "ValidationReportV1",
    "ValidationSeverityV1",
    "build_validation_report",
    "canonical_report_json",
    "validate_process_ir",
]

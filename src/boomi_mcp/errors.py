"""Shared structured error taxonomy (Issue #10 / M4).

Single home for the stable machine-readable ``error_code`` values returned by
the authoring/deployment/raw-API tool surfaces, so agents can branch on codes
that stay consistent across modules. Constants defined elsewhere before this
module existed are re-exported from their original modules for compatibility.

Pure-Python on purpose (stdlib only — no pydantic, no SDK imports) so it is
import-safe from both the ``boomi_mcp.*`` and ``src.boomi_mcp.*`` namespaces.
"""

from dataclasses import dataclass
from typing import Dict, Tuple

# Error-code namespaces reserved for upcoming work. No runtime behavior is
# attached here — the reservation only stops this taxonomy from squatting on
# the prefix. ``GOTCHA_``: operational gotcha-routing codes (M9.2 / issue #78).
RESERVED_ERROR_CODE_PREFIXES: Tuple[str, ...] = ("GOTCHA_",)

# --- Deployment (shipped under #10 P1; canonical home moved here) -----------
ENVIRONMENT_ACCOUNT_ATOM_ATTACHMENT_UNSUPPORTED = "ENVIRONMENT_ACCOUNT_ATOM_ATTACHMENT_UNSUPPORTED"
DEPRECATED_ATOM_ATTACHMENT_ACTION = "DEPRECATED_ATOM_ATTACHMENT_ACTION"

# --- Raw API write gate (shipped under #79; canonical home moved here) ------
RAW_WRITE_CONFIRMATION_REQUIRED = "RAW_WRITE_CONFIRMATION_REQUIRED"

# --- Pattern/archetype authoring (values already in use as literals) --------
INVALID_INPUT = "INVALID_INPUT"
PARAM_VALIDATION_FAILED = "PARAM_VALIDATION_FAILED"
PATTERN_DISCOVERY_FAILED = "PATTERN_DISCOVERY_FAILED"
PATTERN_NOT_FOUND = "PATTERN_NOT_FOUND"
DUPLICATE_PATTERN_NAME = "DUPLICATE_PATTERN_NAME"
INVALID_PATTERN_KIND = "INVALID_PATTERN_KIND"
PATTERN_CONTRACT_INVALID = "PATTERN_CONTRACT_INVALID"
ARCHETYPE_BUILD_VALIDATION_FAILED = "ARCHETYPE_BUILD_VALIDATION_FAILED"
ARCHETYPE_BUILD_FAILED = "ARCHETYPE_BUILD_FAILED"

# --- Schema discovery (new in #10 remaining scope) ---------------------------
SCHEMA_SELECTOR_REQUIRED = "SCHEMA_SELECTOR_REQUIRED"
SCHEMA_NAME_UNSUPPORTED = "SCHEMA_NAME_UNSUPPORTED"
SCHEMA_LOOKUP_FAILED = "SCHEMA_LOOKUP_FAILED"
WORKFLOW_SEQUENCE_NOT_FOUND = "WORKFLOW_SEQUENCE_NOT_FOUND"

# --- Archetype composition (M8 / issue #14) ----------------------------------
COMPOSITION_CONTRACT_MISMATCH = "COMPOSITION_CONTRACT_MISMATCH"
COMPOSITION_UNSUPPORTED_TOPOLOGY = "COMPOSITION_UNSUPPORTED_TOPOLOGY"
COMPOSITION_COMPONENT_KEY_COLLISION = "COMPOSITION_COMPONENT_KEY_COLLISION"

# --- Safe existing-component edit workflow (M9.7 / issue #97) -----------------
COMPONENT_EDIT_RAW_XML_UNSUPPORTED = "COMPONENT_EDIT_RAW_XML_UNSUPPORTED"
COMPONENT_EDIT_CONFIRMATION_REQUIRED = "COMPONENT_EDIT_CONFIRMATION_REQUIRED"
COMPONENT_EDIT_TOKEN_INVALID = "COMPONENT_EDIT_TOKEN_INVALID"
COMPONENT_EDIT_PATCH_MISMATCH = "COMPONENT_EDIT_PATCH_MISMATCH"
COMPONENT_EDIT_DRIFT_DETECTED = "COMPONENT_EDIT_DRIFT_DETECTED"
COMPONENT_EDIT_TYPE_MISMATCH = "COMPONENT_EDIT_TYPE_MISMATCH"

# --- ProcessIRV1 model/codec boundary (M12.1 / issue #136; ADR-001 §7) --------
# First codes of the PROCESS_IR_SCHEMA_* / PROCESS_IR_REFERENCE_* /
# PROCESS_IR_CAPABILITY_* families. Per ADR-001 §7 this module is the ONE
# shared registry for the family constants; later introducers (#140-#143)
# ADD codes here, never rename or re-scope these.
PROCESS_IR_SCHEMA_UNKNOWN_NODE = "PROCESS_IR_SCHEMA_UNKNOWN_NODE"
PROCESS_IR_SCHEMA_UNKNOWN_FIELD = "PROCESS_IR_SCHEMA_UNKNOWN_FIELD"
PROCESS_IR_SCHEMA_INVALID_CARDINALITY = "PROCESS_IR_SCHEMA_INVALID_CARDINALITY"
PROCESS_IR_SCHEMA_VERSION_UNSUPPORTED = "PROCESS_IR_SCHEMA_VERSION_UNSUPPORTED"
PROCESS_IR_SCHEMA_INVALID = "PROCESS_IR_SCHEMA_INVALID"
PROCESS_IR_REFERENCE_INVALID_FORMAT = "PROCESS_IR_REFERENCE_INVALID_FORMAT"
PROCESS_IR_CAPABILITY_UNSUPPORTED = "PROCESS_IR_CAPABILITY_UNSUPPORTED"

# --- ProcessIR compiler CFG/lowering (M12.2 / issue #137; ADR-001 §7) ---------
# First codes of the PROCESS_IR_SEMANTIC_* / PROCESS_IR_COMPILE_* families.
# SEMANTIC_* are user-authored semantic defects that survived schema validation;
# COMPILE_* are compiler/emission-plan defects (an internal invariant broke, not
# the caller's input). Later introducers (#138, #140-#143) ADD codes here.
PROCESS_IR_SEMANTIC_UNREACHABLE = "PROCESS_IR_SEMANTIC_UNREACHABLE"
PROCESS_IR_SEMANTIC_MISSING_TERMINAL = "PROCESS_IR_SEMANTIC_MISSING_TERMINAL"
PROCESS_IR_SEMANTIC_AMBIGUOUS_FLOW = "PROCESS_IR_SEMANTIC_AMBIGUOUS_FLOW"
PROCESS_IR_COMPILE_INTERNAL = "PROCESS_IR_COMPILE_INTERNAL"
PROCESS_IR_COMPILE_NONDETERMINISTIC = "PROCESS_IR_COMPILE_NONDETERMINISTIC"
PROCESS_IR_COMPILE_EMISSION_PLAN_INVALID = "PROCESS_IR_COMPILE_EMISSION_PLAN_INVALID"

# --- ProcessIR process-emitter registry (M12.3 / issue #138) ------------------
# Fail-closed defects raised by the typed emitter registry when it turns an
# emission plan into process XML. Emitter-facing compiler defects; the legacy
# builder entrypoints keep their existing external error contract unchanged.
PROCESS_IR_COMPILE_EMITTER_MISSING = "PROCESS_IR_COMPILE_EMITTER_MISSING"
PROCESS_IR_COMPILE_EMITTER_INPUT_INVALID = "PROCESS_IR_COMPILE_EMITTER_INPUT_INVALID"
PROCESS_IR_COMPILE_SYMBOL_UNRESOLVED = "PROCESS_IR_COMPILE_SYMBOL_UNRESOLVED"
PROCESS_IR_COMPILE_XML_INVALID = "PROCESS_IR_COMPILE_XML_INVALID"
PROCESS_IR_COMPILE_VERIFIER_FAILED = "PROCESS_IR_COMPILE_VERIFIER_FAILED"

# --- ProcessIR legacy adapters (M12.4 / issue #139; ADR-001 §7) ----------------
# INTERNAL diagnostics raised inside the legacy-config -> ProcessIR adapter
# boundary. A migrated public authoring entrypoint (build_integration, the
# process-flow builders) keeps its existing EXTERNAL error contract: an adapter
# failure on already-validated input is translated to the builder family
# (normally PROCESS_XML_VALIDATION_FAILED) before it reaches a caller. Later
# adapter slices (#139 sync/database/recipe/authority work) ADD codes here,
# never rename or re-scope these.
LEGACY_ADAPTER_UNSUPPORTED_KIND = "LEGACY_ADAPTER_UNSUPPORTED_KIND"
LEGACY_ADAPTER_PIPELINE_DRAFT_ONLY = "LEGACY_ADAPTER_PIPELINE_DRAFT_ONLY"
LEGACY_ADAPTER_AUTHORITY_CONFLICT = "LEGACY_ADAPTER_AUTHORITY_CONFLICT"
LEGACY_ADAPTER_SEMANTIC_LOSS = "LEGACY_ADAPTER_SEMANTIC_LOSS"
LEGACY_ADAPTER_OUTPUT_PARITY_FAILED = "LEGACY_ADAPTER_OUTPUT_PARITY_FAILED"


@dataclass(frozen=True)
class ErrorCodeSpec:
    """Catalog entry for one stable error code."""

    code: str
    category: str
    retryable: bool
    summary: str
    owner: str  # issue/milestone that introduced the code


ERROR_TAXONOMY: Dict[str, ErrorCodeSpec] = {
    spec.code: spec
    for spec in (
        ErrorCodeSpec(
            code=ENVIRONMENT_ACCOUNT_ATOM_ATTACHMENT_UNSUPPORTED,
            category="deployment",
            retryable=False,
            summary=(
                "Direct atom attachment is unsupported on environment-enabled "
                "accounts; use the environment-attachment path."
            ),
            owner="#10",
        ),
        ErrorCodeSpec(
            code=DEPRECATED_ATOM_ATTACHMENT_ACTION,
            category="deployment",
            retryable=False,
            summary="The atom-attachment action is deprecated; use the environment-attach equivalent.",
            owner="#10",
        ),
        ErrorCodeSpec(
            code=RAW_WRITE_CONFIRMATION_REQUIRED,
            category="raw_api",
            retryable=False,
            summary="Mutating raw API call blocked: re-call with confirm_write=true or use a typed tool.",
            owner="#79",
        ),
        ErrorCodeSpec(
            code=INVALID_INPUT,
            category="authoring",
            retryable=False,
            summary="An argument had the wrong type or shape.",
            owner="#18",
        ),
        ErrorCodeSpec(
            code=PARAM_VALIDATION_FAILED,
            category="authoring",
            retryable=False,
            summary="Archetype/pattern parameters failed validation; see field_errors[].",
            owner="#18",
        ),
        ErrorCodeSpec(
            code=PATTERN_DISCOVERY_FAILED,
            category="authoring",
            retryable=False,
            summary="Pattern package discovery/import failed.",
            owner="#18",
        ),
        ErrorCodeSpec(
            code=PATTERN_NOT_FOUND,
            category="authoring",
            retryable=False,
            summary="No pattern registered under the requested name/kind.",
            owner="#18",
        ),
        ErrorCodeSpec(
            code=DUPLICATE_PATTERN_NAME,
            category="authoring",
            retryable=False,
            summary="Two patterns registered the same name.",
            owner="#18",
        ),
        ErrorCodeSpec(
            code=INVALID_PATTERN_KIND,
            category="authoring",
            retryable=False,
            summary="Unknown pattern kind selector.",
            owner="#18",
        ),
        ErrorCodeSpec(
            code=PATTERN_CONTRACT_INVALID,
            category="authoring",
            retryable=False,
            summary="A pattern class violates the PatternBase contract.",
            owner="#18",
        ),
        ErrorCodeSpec(
            code=ARCHETYPE_BUILD_VALIDATION_FAILED,
            category="authoring",
            retryable=False,
            summary="A builder rejected the archetype assembly.",
            owner="#18",
        ),
        ErrorCodeSpec(
            code=ARCHETYPE_BUILD_FAILED,
            category="authoring",
            retryable=False,
            summary="emit_spec() failed unexpectedly for the archetype.",
            owner="#18",
        ),
        ErrorCodeSpec(
            code=SCHEMA_SELECTOR_REQUIRED,
            category="schema_discovery",
            retryable=False,
            summary="get_schema_template needs resource_type or schema_name.",
            owner="#10",
        ),
        ErrorCodeSpec(
            code=SCHEMA_NAME_UNSUPPORTED,
            category="schema_discovery",
            retryable=False,
            summary="Unknown schema_name; see valid_schema_names.",
            owner="#10",
        ),
        ErrorCodeSpec(
            code=SCHEMA_LOOKUP_FAILED,
            category="schema_discovery",
            retryable=False,
            summary="Schema/template lookup failed (unknown type, operation, protocol, or standard).",
            owner="#10",
        ),
        ErrorCodeSpec(
            code=WORKFLOW_SEQUENCE_NOT_FOUND,
            category="schema_discovery",
            retryable=False,
            summary="Unknown workflow sequence name; see valid_workflows.",
            owner="#10",
        ),
        ErrorCodeSpec(
            code=COMPOSITION_CONTRACT_MISMATCH,
            category="authoring",
            retryable=False,
            summary=(
                "A composed part's output contract does not match the next "
                "part's input contract (source fields / profile leaves / media type)."
            ),
            owner="#14",
        ),
        ErrorCodeSpec(
            code=COMPOSITION_UNSUPPORTED_TOPOLOGY,
            category="authoring",
            retryable=False,
            summary=(
                "The requested part graph is outside the v1 composed topology "
                "(one db_source -> one transform -> 2..25 rest_target fanout)."
            ),
            owner="#14",
        ),
        ErrorCodeSpec(
            code=COMPOSITION_COMPONENT_KEY_COLLISION,
            category="authoring",
            retryable=False,
            summary=(
                "Two composition parts collide on a key, derived component-key "
                "prefix, or derived component display name."
            ),
            owner="#14",
        ),
        ErrorCodeSpec(
            code=COMPONENT_EDIT_RAW_XML_UNSUPPORTED,
            category="component_edit",
            retryable=False,
            summary="Safe edit rejects raw XML patches; use structured fields or manage_component config.xml.",
            owner="#97",
        ),
        ErrorCodeSpec(
            code=COMPONENT_EDIT_CONFIRMATION_REQUIRED,
            category="component_edit",
            retryable=False,
            summary="apply_component_edit needs confirm_apply=true plus a prepare confirmation_token.",
            owner="#97",
        ),
        ErrorCodeSpec(
            code=COMPONENT_EDIT_TOKEN_INVALID,
            category="component_edit",
            retryable=False,
            summary="confirmation_token is missing, malformed, or issued for another component.",
            owner="#97",
        ),
        ErrorCodeSpec(
            code=COMPONENT_EDIT_PATCH_MISMATCH,
            category="component_edit",
            retryable=False,
            summary="The applied patch differs from the previewed one; re-run prepare for the new patch.",
            owner="#97",
        ),
        ErrorCodeSpec(
            code=COMPONENT_EDIT_DRIFT_DETECTED,
            category="component_edit",
            retryable=False,
            summary="The component changed since preview; the edit was aborted. Re-run prepare.",
            owner="#97",
        ),
        ErrorCodeSpec(
            code=COMPONENT_EDIT_TYPE_MISMATCH,
            category="component_edit",
            retryable=False,
            summary="patch.component_type does not match the live component type.",
            owner="#97",
        ),
        ErrorCodeSpec(
            code=PROCESS_IR_SCHEMA_UNKNOWN_NODE,
            category="process_ir",
            retryable=False,
            summary="A ProcessIRV1 node carries an unknown 'kind' (or nested discriminator) tag.",
            owner="#136",
        ),
        ErrorCodeSpec(
            code=PROCESS_IR_SCHEMA_UNKNOWN_FIELD,
            category="process_ir",
            retryable=False,
            summary="A ProcessIRV1 node carries a field its strict schema does not declare.",
            owner="#136",
        ),
        ErrorCodeSpec(
            code=PROCESS_IR_SCHEMA_INVALID_CARDINALITY,
            category="process_ir",
            retryable=False,
            summary="A ProcessIRV1 list/step bound was violated (empty steps, branch leg count, ordering).",
            owner="#136",
        ),
        ErrorCodeSpec(
            code=PROCESS_IR_SCHEMA_VERSION_UNSUPPORTED,
            category="process_ir",
            retryable=False,
            summary="The ProcessIR document version is missing or not a supported version.",
            owner="#136",
        ),
        ErrorCodeSpec(
            code=PROCESS_IR_SCHEMA_INVALID,
            category="process_ir",
            retryable=False,
            summary="The ProcessIR payload failed strict schema validation (shape/type mismatch).",
            owner="#136",
        ),
        ErrorCodeSpec(
            code=PROCESS_IR_REFERENCE_INVALID_FORMAT,
            category="process_ir",
            retryable=False,
            summary="An opaque component reference is not an exact '$ref:KEY' token or literal component id.",
            owner="#136",
        ),
        ErrorCodeSpec(
            code=PROCESS_IR_CAPABILITY_UNSUPPORTED,
            category="process_ir",
            retryable=False,
            summary="The payload requests a gated/unsupported ProcessIR capability (keyed cache, secret carriage, ...).",
            owner="#136",
        ),
        ErrorCodeSpec(
            code=PROCESS_IR_SEMANTIC_UNREACHABLE,
            category="process_ir",
            retryable=False,
            summary="A lowered node is not reachable from the compiler's single control-flow entry.",
            owner="#137",
        ),
        ErrorCodeSpec(
            code=PROCESS_IR_SEMANTIC_MISSING_TERMINAL,
            category="process_ir",
            retryable=False,
            summary="A control-flow path does not reach a valid terminal (stop/return/exception/routed target).",
            owner="#137",
        ),
        ErrorCodeSpec(
            code=PROCESS_IR_SEMANTIC_AMBIGUOUS_FLOW,
            category="process_ir",
            retryable=False,
            summary=(
                "Control flow is ambiguous: multiple entries, a join/cycle, an invalid "
                "successor, or flow continuing past a terminal."
            ),
            owner="#137",
        ),
        ErrorCodeSpec(
            code=PROCESS_IR_COMPILE_INTERNAL,
            category="process_ir",
            retryable=False,
            summary="A compiler invariant broke (duplicate or dangling internal id) — a compiler defect, not authored input.",
            owner="#137",
        ),
        ErrorCodeSpec(
            code=PROCESS_IR_COMPILE_NONDETERMINISTIC,
            category="process_ir",
            retryable=False,
            summary="Compiler output is not in canonical order, so two compilations could differ.",
            owner="#137",
        ),
        ErrorCodeSpec(
            code=PROCESS_IR_COMPILE_EMISSION_PLAN_INVALID,
            category="process_ir",
            retryable=False,
            summary=(
                "The emission plan is invalid: unresolved symbol, bad wiring, layout, or "
                "synthetic-shape synthesis."
            ),
            owner="#137",
        ),
        ErrorCodeSpec(
            code=PROCESS_IR_COMPILE_EMITTER_MISSING,
            category="process_ir",
            retryable=False,
            summary=(
                "No registered emitter for an emission-plan node kind (or it is not "
                "supported at the current capability level)."
            ),
            owner="#138",
        ),
        ErrorCodeSpec(
            code=PROCESS_IR_COMPILE_EMITTER_INPUT_INVALID,
            category="process_ir",
            retryable=False,
            summary=(
                "An emitter input is invalid: wrong typed input for the node kind, a bad "
                "renderer precondition, or an outgoing-cardinality mismatch."
            ),
            owner="#138",
        ),
        ErrorCodeSpec(
            code=PROCESS_IR_COMPILE_SYMBOL_UNRESOLVED,
            category="process_ir",
            retryable=False,
            summary=(
                "A required component symbol is absent, or present only with an "
                "incompatible component type, for an emitter node."
            ),
            owner="#138",
        ),
        ErrorCodeSpec(
            code=PROCESS_IR_COMPILE_XML_INVALID,
            category="process_ir",
            retryable=False,
            summary=(
                "Emitted process XML is malformed, or its shape count/name/type "
                "disagrees with the emission plan."
            ),
            owner="#138",
        ),
        ErrorCodeSpec(
            code=PROCESS_IR_COMPILE_VERIFIER_FAILED,
            category="process_ir",
            retryable=False,
            summary="The process graph verifier reported errors on registry-emitted XML.",
            owner="#138",
        ),
        ErrorCodeSpec(
            code=LEGACY_ADAPTER_UNSUPPORTED_KIND,
            category="process_ir",
            retryable=False,
            summary=(
                "No legacy adapter is registered for the requested authoring "
                "dialect / process kind."
            ),
            owner="#139",
        ),
        ErrorCodeSpec(
            code=LEGACY_ADAPTER_PIPELINE_DRAFT_ONLY,
            category="process_ir",
            retryable=False,
            summary=(
                "A reserved/unlowered PipelineSpec kind was submitted to a legacy "
                "adapter; it is draft/analysis only and never falsely executable."
            ),
            owner="#139",
        ),
        ErrorCodeSpec(
            code=LEGACY_ADAPTER_AUTHORITY_CONFLICT,
            category="process_ir",
            retryable=False,
            summary=(
                "A top-level pipeline view disagrees with the normalized single "
                "authored process on the strict/opt-in surface."
            ),
            owner="#139",
        ),
        ErrorCodeSpec(
            code=LEGACY_ADAPTER_SEMANTIC_LOSS,
            category="process_ir",
            retryable=False,
            summary=(
                "A legacy field that affects current process XML cannot be "
                "represented in ProcessIR without loss."
            ),
            owner="#139",
        ),
        ErrorCodeSpec(
            code=LEGACY_ADAPTER_OUTPUT_PARITY_FAILED,
            category="process_ir",
            retryable=False,
            summary=(
                "Canonical compile/emit/verify of a legally-validated legacy "
                "config failed after successful legacy validation."
            ),
            owner="#139",
        ),
    )
}

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
#
# ONE deliberate exception (#139D): LEGACY_ADAPTER_AUTHORITY_CONFLICT surfaces
# PUBLICLY and untranslated. It is not an adapter defect on already-validated
# input — it is a plan-time rejection of the caller's own payload on a NEW
# opt-in surface (version="1.1"), so there is no legacy external contract to
# preserve for it, and ADR-001 §5 requires it to be a "stable, tested error"
# the caller can key on. LEGACY_ADAPTER_PIPELINE_DRAFT_ONLY remains reserved and
# unraised: reserved/unlowered PipelineSpec kinds already fail before mutation
# with their exact SYNC_PIPELINE_* code/field pairs, which the #135
# characterization suite freezes.
LEGACY_ADAPTER_UNSUPPORTED_KIND = "LEGACY_ADAPTER_UNSUPPORTED_KIND"
LEGACY_ADAPTER_PIPELINE_DRAFT_ONLY = "LEGACY_ADAPTER_PIPELINE_DRAFT_ONLY"
LEGACY_ADAPTER_AUTHORITY_CONFLICT = "LEGACY_ADAPTER_AUTHORITY_CONFLICT"
LEGACY_ADAPTER_SEMANTIC_LOSS = "LEGACY_ADAPTER_SEMANTIC_LOSS"
LEGACY_ADAPTER_OUTPUT_PARITY_FAILED = "LEGACY_ADAPTER_OUTPUT_PARITY_FAILED"

# --- ProcessIR first-class ConnectorCall (M12.5 / issue #140; ADR-001 §7) ------
# #140 ADDS to four families that already exist (REFERENCE_*, CAPABILITY_*,
# SEMANTIC_*, COMPILE_*) — it introduces no eleventh family and re-registers no
# existing code. ``ERROR_TAXONOMY`` is a dict comprehension keyed on ``spec.code``,
# so a duplicate entry would silently overwrite the earlier owner's; every code
# below is new.
#
# The split follows ADR-001 §7 exactly: REFERENCE_*/CAPABILITY_* are resolution
# failures against the symbol table, SEMANTIC_* blames the authored flow, and the
# single COMPILE_* code blames the compiler and can only be reached through a
# defect (a caller cannot author a binding at all).
PROCESS_IR_REFERENCE_OPERATION_NOT_FOUND = "PROCESS_IR_REFERENCE_OPERATION_NOT_FOUND"
PROCESS_IR_REFERENCE_CONNECTION_NOT_FOUND = "PROCESS_IR_REFERENCE_CONNECTION_NOT_FOUND"
PROCESS_IR_REFERENCE_CONNECTION_MISMATCH = "PROCESS_IR_REFERENCE_CONNECTION_MISMATCH"
PROCESS_IR_CAPABILITY_CONNECTOR_ACTION_UNSUPPORTED = (
    "PROCESS_IR_CAPABILITY_CONNECTOR_ACTION_UNSUPPORTED"
)
PROCESS_IR_SEMANTIC_PROFILE_MISMATCH = "PROCESS_IR_SEMANTIC_PROFILE_MISMATCH"
PROCESS_IR_SEMANTIC_CARDINALITY_MISMATCH = "PROCESS_IR_SEMANTIC_CARDINALITY_MISMATCH"
PROCESS_IR_COMPILE_CONNECTOR_BINDING_INVALID = (
    "PROCESS_IR_COMPILE_CONNECTOR_BINDING_INVALID"
)

# --- ProcessIR rich Branch/Decision bodies (M12.6 / issue #141; ADR-001 §7) ----
# #141 ADDS to three families that already exist (SCHEMA_*, SEMANTIC_*,
# CAPABILITY_*/COMPILE_*) — no new family, no rename, no re-scope.
#
# Several of these are deliberately MORE SPECIFIC than an existing family member
# that would also have described the condition (e.g. BRANCH_CARDINALITY vs
# SCHEMA_INVALID_CARDINALITY, JOIN_UNSUPPORTED vs SEMANTIC_AMBIGUOUS_FLOW). That
# is the precedent #140 set one slice earlier — it registered
# PROCESS_IR_SEMANTIC_CARDINALITY_MISMATCH alongside the older
# PROCESS_IR_SCHEMA_INVALID_CARDINALITY, and PROCESS_IR_COMPILE_CONNECTOR_BINDING_INVALID
# alongside PROCESS_IR_COMPILE_EMISSION_PLAN_INVALID — and it is what ADR-001 §7's
# "later introducers ADD codes" rule exists to permit. The older codes keep every
# raise site they already had; only #141's NEW validation paths use these.
PROCESS_IR_SCHEMA_BRANCH_CARDINALITY = "PROCESS_IR_SCHEMA_BRANCH_CARDINALITY"
PROCESS_IR_SEMANTIC_CONTROL_CONTINUATION_UNSUPPORTED = (
    "PROCESS_IR_SEMANTIC_CONTROL_CONTINUATION_UNSUPPORTED"
)
PROCESS_IR_SEMANTIC_JOIN_UNSUPPORTED = "PROCESS_IR_SEMANTIC_JOIN_UNSUPPORTED"
PROCESS_IR_SEMANTIC_NESTING_LIMIT = "PROCESS_IR_SEMANTIC_NESTING_LIMIT"
PROCESS_IR_SEMANTIC_UNTERMINATED_PATH = "PROCESS_IR_SEMANTIC_UNTERMINATED_PATH"
PROCESS_IR_CAPABILITY_NODE_NOT_ALLOWED_IN_BODY = (
    "PROCESS_IR_CAPABILITY_NODE_NOT_ALLOWED_IN_BODY"
)
PROCESS_IR_COMPILE_CONTROL_WIRING_INVALID = "PROCESS_IR_COMPILE_CONTROL_WIRING_INVALID"

# --- ProcessIR scoped error handling (M12.7 / issue #142; ADR-001 §7) ---------
# #142 ADDS to the same four families #141 used; no new family, no rename.
#
# The retry bound (0..5) is not a compiler invention: the platform's own Try/Catch
# step documents exactly that range, together with a fixed wait schedule
# (0 none / 1 immediate / 2:10s / 3:30s / 4:60s / 5:120s) that the caller cannot
# author. See .codex/plans/issue-142-live-captures.md §G1.
#
# SEMANTIC_RETRY_* blames the authored flow (a retry region that would re-run the
# source, or a retried write with no registry-backed safety). The single COMPILE_*
# code blames the compiler: a caller cannot author a CFG region at all, so
# reaching it means the region derivation itself produced a malformed graph.
PROCESS_IR_SCHEMA_RETRY_COUNT = "PROCESS_IR_SCHEMA_RETRY_COUNT"
PROCESS_IR_CAPABILITY_ERROR_SCOPE_UNSUPPORTED = (
    "PROCESS_IR_CAPABILITY_ERROR_SCOPE_UNSUPPORTED"
)
PROCESS_IR_SEMANTIC_RETRY_SOURCE_REEXECUTION = (
    "PROCESS_IR_SEMANTIC_RETRY_SOURCE_REEXECUTION"
)
PROCESS_IR_SEMANTIC_RETRY_NON_IDEMPOTENT_WRITE = (
    "PROCESS_IR_SEMANTIC_RETRY_NON_IDEMPOTENT_WRITE"
)
PROCESS_IR_SEMANTIC_IDEMPOTENCY_EVIDENCE_MISSING = (
    "PROCESS_IR_SEMANTIC_IDEMPOTENCY_EVIDENCE_MISSING"
)
PROCESS_IR_SEMANTIC_CATCH_UNTERMINATED = "PROCESS_IR_SEMANTIC_CATCH_UNTERMINATED"
PROCESS_IR_COMPILE_ERROR_REGION_INVALID = "PROCESS_IR_COMPILE_ERROR_REGION_INVALID"

# --- ProcessIR unified semantic validation (M12.8 / issue #143; ADR-001 §7) ---
# #143 ADDS to four families it is a declared introducer for — REFERENCE_*,
# CAPABILITY_*, SEMANTIC_* and the LEGACY_ADAPTER_EXEMPTION_* subfamily. It is
# deliberately NOT an introducer for COMPILE_*: that family blames the compiler,
# whereas every code below blames the authored payload, and a ValidationReportV1
# cannot carry a compile-family code at all (an unexpected internal defect
# escapes to the compiler's own ``_guarded`` boundary instead).
#
# ``ERROR_TAXONOMY`` is a dict comprehension keyed on ``spec.code``, so a
# duplicate entry would silently overwrite the earlier owner's spec AND shrink
# the dict by one key. Every code below is new; the guard tests in
# tests/test_error_taxonomy.py pin both facts.

# Generic component resolution. The specialized #140 operation/connection codes
# keep their own conditions; these two cover every OTHER component role.
PROCESS_IR_REFERENCE_COMPONENT_NOT_FOUND = "PROCESS_IR_REFERENCE_COMPONENT_NOT_FOUND"
PROCESS_IR_REFERENCE_COMPONENT_TYPE_MISMATCH = (
    "PROCESS_IR_REFERENCE_COMPONENT_TYPE_MISMATCH"
)
PROCESS_IR_CAPABILITY_EFFECT_CONTRACT_INVALID = (
    "PROCESS_IR_CAPABILITY_EFFECT_CONTRACT_INVALID"
)

# Terminal Process Call (#175). A Process Call's outbound connection is a
# projection of the CALLED process's Return Documents shapes, not a free graph
# edge: the platform keys the connection on those return paths, so a call whose
# child returns nothing ends its path. ProcessIR v1 therefore admits the
# TERMINAL form only, and every authored form that asks a Process Call to
# continue raises this code — the binding that would make continuation valid
# needs the child's compiled shapes and is gated as
# `process_call_return_path_binding`.
PROCESS_IR_CAPABILITY_PROCESS_CALL_RETURN_PATH_BINDING_UNSUPPORTED = (
    "PROCESS_IR_CAPABILITY_PROCESS_CALL_RETURN_PATH_BINDING_UNSUPPORTED"
)

# State lineage: document-scoped (DDP) vs execution-scoped (DPP/cache).
PROCESS_IR_SEMANTIC_LINEAGE_PROPERTY_READ_BEFORE_WRITE = (
    "PROCESS_IR_SEMANTIC_LINEAGE_PROPERTY_READ_BEFORE_WRITE"
)
PROCESS_IR_SEMANTIC_LINEAGE_DDP_SCOPE_INVALID = (
    "PROCESS_IR_SEMANTIC_LINEAGE_DDP_SCOPE_INVALID"
)
PROCESS_IR_SEMANTIC_LINEAGE_BRANCH_ORDER_INVALID = (
    "PROCESS_IR_SEMANTIC_LINEAGE_BRANCH_ORDER_INVALID"
)
PROCESS_IR_SEMANTIC_LINEAGE_CACHE_WRITER_MISSING = (
    "PROCESS_IR_SEMANTIC_LINEAGE_CACHE_WRITER_MISSING"
)
PROCESS_IR_SEMANTIC_LINEAGE_AMBIGUOUS_LAST_WRITE = (
    "PROCESS_IR_SEMANTIC_LINEAGE_AMBIGUOUS_LAST_WRITE"
)
PROCESS_IR_SEMANTIC_LINEAGE_EFFECT_UNKNOWN = (
    "PROCESS_IR_SEMANTIC_LINEAGE_EFFECT_UNKNOWN"
)
PROCESS_IR_SEMANTIC_LINEAGE_EXTERNAL_WRITER_ASSUMED = (
    "PROCESS_IR_SEMANTIC_LINEAGE_EXTERNAL_WRITER_ASSUMED"
)

# Side effects and replay. These cover NON-connector hazards only; every #140
# and #142 connector retry/idempotency code keeps its existing condition.
PROCESS_IR_SEMANTIC_SIDE_EFFECT_ORDERING_UNSAFE = (
    "PROCESS_IR_SEMANTIC_SIDE_EFFECT_ORDERING_UNSAFE"
)
PROCESS_IR_SEMANTIC_SIDE_EFFECT_ORDERING_UNKNOWN = (
    "PROCESS_IR_SEMANTIC_SIDE_EFFECT_ORDERING_UNKNOWN"
)
PROCESS_IR_SEMANTIC_RETRY_EFFECT_UNSAFE = "PROCESS_IR_SEMANTIC_RETRY_EFFECT_UNSAFE"

# Named legacy exemptions. These are ADVISORY and registry-owned: a policy is
# selected by an immutable adapter registration, never by authored input, so no
# caller can suppress a fatal rule by asking.
LEGACY_ADAPTER_EXEMPTION_OPAQUE_STATE_WRITER = (
    "LEGACY_ADAPTER_EXEMPTION_OPAQUE_STATE_WRITER"
)
LEGACY_ADAPTER_EXEMPTION_DECISION_PROPERTY_READ = (
    "LEGACY_ADAPTER_EXEMPTION_DECISION_PROPERTY_READ"
)
LEGACY_ADAPTER_EXEMPTION_STANDALONE_CACHE_READ = (
    "LEGACY_ADAPTER_EXEMPTION_STANDALONE_CACHE_READ"
)
LEGACY_ADAPTER_EXEMPTION_SUBPROCESS_SUMMARY = (
    "LEGACY_ADAPTER_EXEMPTION_SUBPROCESS_SUMMARY"
)

# --- Capability-gated topology planning (M12.9 / issue #144; ADR-001 §7) ------
# ADR-001 §7 reserves the whole ``TOPOLOGY_*`` family to #144, and #144 is its
# SOLE introducer: the family is opened and closed here in one issue. That is
# stronger than the ProcessIR families, which several issues extend, so the
# guard test asserts the biconditional — every code below is owned by #144 AND
# every ``TOPOLOGY_``-prefixed key in the taxonomy is owned by #144.
#
# These codes blame the AUTHORED topology payload. The topology planner's own
# internal defects raise a private invariant error instead, for the same reason
# ``PROCESS_IR_COMPILE_*`` is walled off from ValidationReportV1: telling a
# caller to fix correct input is how someone ends up rewriting a working payload
# to route around our bug. No ``TOPOLOGY_*`` code is reachable from the ProcessIR
# compiler, and no ``PROCESS_IR_*`` code is reachable from the topology planner.
#
# ``ERROR_TAXONOMY`` is a dict comprehension keyed on ``spec.code``, so a
# duplicate entry would silently overwrite the earlier owner's spec AND shrink
# the dict by one key. tests/test_error_taxonomy.py pins both facts.

# Schema: shape of the authored document itself.
TOPOLOGY_SCHEMA_UNKNOWN_OBJECT = "TOPOLOGY_SCHEMA_UNKNOWN_OBJECT"
TOPOLOGY_SCHEMA_UNKNOWN_RELATION = "TOPOLOGY_SCHEMA_UNKNOWN_RELATION"
TOPOLOGY_SCHEMA_UNKNOWN_FIELD = "TOPOLOGY_SCHEMA_UNKNOWN_FIELD"
TOPOLOGY_SCHEMA_INVALID_CARDINALITY = "TOPOLOGY_SCHEMA_INVALID_CARDINALITY"
TOPOLOGY_SCHEMA_DUPLICATE_KEY = "TOPOLOGY_SCHEMA_DUPLICATE_KEY"
TOPOLOGY_SCHEMA_VERSION_UNSUPPORTED = "TOPOLOGY_SCHEMA_VERSION_UNSUPPORTED"
TOPOLOGY_SCHEMA_INVALID = "TOPOLOGY_SCHEMA_INVALID"

# Resolution: does an authored reference name something real, of the right kind.
TOPOLOGY_REFERENCE_NOT_FOUND = "TOPOLOGY_REFERENCE_NOT_FOUND"
TOPOLOGY_REFERENCE_TYPE_MISMATCH = "TOPOLOGY_REFERENCE_TYPE_MISMATCH"

# Lifecycle/capability/graph: is the RELATION something the platform supports,
# is there evidence for it, and does the resulting graph make sense.
TOPOLOGY_RELATION_UNSUPPORTED = "TOPOLOGY_RELATION_UNSUPPORTED"
TOPOLOGY_CAPABILITY_GATED = "TOPOLOGY_CAPABILITY_GATED"
TOPOLOGY_ENVIRONMENT_MISMATCH = "TOPOLOGY_ENVIRONMENT_MISMATCH"
TOPOLOGY_DEPENDENCY_CYCLE = "TOPOLOGY_DEPENDENCY_CYCLE"

# The standing refusal. #144 ships a PLANNER; there is no apply path at all, and
# this code is what a caller gets for asking for one.
TOPOLOGY_APPLY_NOT_SUPPORTED = "TOPOLOGY_APPLY_NOT_SUPPORTED"

# --- Typed executable recipe contributions (M12.10 / issue #145) --------------
# #145 is the SOLE introducer of the ``RECIPE_*`` family, so the guard test
# asserts the same biconditional #144's family carries: every code below is
# owned by #145 AND every ``RECIPE_``-prefixed key in the taxonomy is owned by
# #145.
#
# Every one of these blames the RECIPE LAYER — the descriptor a caller named, the
# input it was given, or the contributions a registered executor returned. None
# of them replaces a canonical validator's own code: when the strict ProcessIR
# compiler, the topology planner, or the component-plan gate rejects an assembled
# artifact, the recipe layer reports ``RECIPE_CONSTRAINT_FAILED`` and carries the
# underlying canonical codes as value-free ``cause_codes``. That keeps the
# canonical taxonomies authoritative about their own domains while still telling
# the caller which layer they must fix.
RECIPE_NOT_FOUND = "RECIPE_NOT_FOUND"
RECIPE_VERSION_UNAVAILABLE = "RECIPE_VERSION_UNAVAILABLE"
RECIPE_CAPABILITY_GATED = "RECIPE_CAPABILITY_GATED"
RECIPE_INPUT_INVALID = "RECIPE_INPUT_INVALID"
RECIPE_CONTRIBUTION_INVALID = "RECIPE_CONTRIBUTION_INVALID"
RECIPE_PATCH_TARGET_NOT_FOUND = "RECIPE_PATCH_TARGET_NOT_FOUND"
RECIPE_PATCH_CONFLICT = "RECIPE_PATCH_CONFLICT"
RECIPE_CONSTRAINT_FAILED = "RECIPE_CONSTRAINT_FAILED"
RECIPE_OUTPUT_NONDETERMINISTIC = "RECIPE_OUTPUT_NONDETERMINISTIC"
RECIPE_REQUEST_INVALID = "RECIPE_REQUEST_INVALID"

# --- MCP authoring surface (M12.11 / issue #146; ADR-001 §7) ------------------
# #146 is the SOLE introducer of the ``AUTHORING_*`` family, so the guard test
# asserts the same biconditional #144 and #145 carry: every code below is owned
# by #146 AND every ``AUTHORING_``-prefixed key in the taxonomy is owned by #146.
#
# Every one of these blames the MCP AUTHORING SURFACE — the contract a caller
# addressed, the revision it bound to, or the phase ordering it skipped. None of
# them replaces a canonical validator's own code: when the strict ProcessIR
# compiler, the topology planner, or the recipe layer rejects an artifact, the
# authoring surface reports ``AUTHORING_COMPILE_BLOCKED`` and carries the
# underlying canonical codes as value-free causative diagnostics. The canonical
# taxonomies stay authoritative about their own domains while the caller still
# learns which phase it must re-run.
AUTHORING_SCHEMA_VERSION_UNAVAILABLE = "AUTHORING_SCHEMA_VERSION_UNAVAILABLE"
#: An authoring selector cannot be built because an AUTHORITY it derives from
#: failed. Distinct from a version that does not exist: the selector is real
#: and the request was valid, so the honest answer is "unavailable, retry",
#: not a short catalog that looks complete.
AUTHORING_SCHEMA_SOURCE_UNAVAILABLE = "AUTHORING_SCHEMA_SOURCE_UNAVAILABLE"
AUTHORING_CAPABILITY_REVISION_MISMATCH = "AUTHORING_CAPABILITY_REVISION_MISMATCH"
AUTHORING_LIVE_DEPLOYMENT_DRIFT = "AUTHORING_LIVE_DEPLOYMENT_DRIFT"
AUTHORING_REQUIRED_DECISION_MISSING = "AUTHORING_REQUIRED_DECISION_MISSING"
AUTHORING_COMPILE_BLOCKED = "AUTHORING_COMPILE_BLOCKED"
AUTHORING_PLAN_STALE = "AUTHORING_PLAN_STALE"
AUTHORING_APPLY_VALIDATION_REQUIRED = "AUTHORING_APPLY_VALIDATION_REQUIRED"


# --- Canonical process component materialization (M12.15 / issue #153) --------
# Three families, one introducer. #153 is the SOLE introducer of
# ``PROCESS_COMPONENT_*``, ``INTEGRATION_DEPENDENCY_*`` and
# ``PROCESS_MATERIALIZATION_*``, so the guard test asserts the same biconditional
# #144/#145/#146 carry: every code below is owned by #153 AND every key in the
# taxonomy carrying one of those three prefixes is owned by #153.
#
# They are deliberately NOT folded into the existing families:
#
# * ``TOPOLOGY_*`` is reserved to #144 by ADR-001 §7 and closed there.
# * ``AUTHORING_*`` blames the MCP authoring SURFACE — the contract addressed,
#   the revision bound, the phase order. These blame the authored process
#   component, its dependency graph, or the materialization plan itself, which
#   is a different subject even when the caller reaches them through that
#   surface.
#
# The split by prefix matches the subject each code blames:
#
# * ``PROCESS_COMPONENT_*`` — the authored envelope/unit SHAPE.
# * ``INTEGRATION_DEPENDENCY_*`` — the one shared key namespace and the single
#   topological order over BOTH ``components`` and ``processes``. Before #153
#   these conditions raised a bare ``ValueError`` with a prose message, so a
#   caller could not branch on them; naming them is a served-contract
#   improvement, not a new restriction.
# * ``PROCESS_MATERIALIZATION_*`` — the relocatable plan, its late symbol
#   binding, and the placement/result accounting that apply performs.

# Shape of the authored process component envelope / unit.
PROCESS_COMPONENT_SCHEMA_UNKNOWN_FIELD = "PROCESS_COMPONENT_SCHEMA_UNKNOWN_FIELD"
PROCESS_COMPONENT_SCHEMA_INVALID = "PROCESS_COMPONENT_SCHEMA_INVALID"
PROCESS_COMPONENT_SCHEMA_INVALID_CARDINALITY = (
    "PROCESS_COMPONENT_SCHEMA_INVALID_CARDINALITY"
)
PROCESS_COMPONENT_REFERENCE_INVALID_FORMAT = (
    "PROCESS_COMPONENT_REFERENCE_INVALID_FORMAT"
)

# The unified component+process dependency graph.
INTEGRATION_COMPONENT_KEY_DUPLICATE = "INTEGRATION_COMPONENT_KEY_DUPLICATE"
INTEGRATION_DEPENDENCY_NOT_FOUND = "INTEGRATION_DEPENDENCY_NOT_FOUND"
INTEGRATION_DEPENDENCY_CYCLE = "INTEGRATION_DEPENDENCY_CYCLE"
INTEGRATION_DEPENDENCY_REQUIRED = "INTEGRATION_DEPENDENCY_REQUIRED"

# The materialization plan, its binding, and apply-time accounting.
PROCESS_MATERIALIZATION_REFERENCE_NOT_RELOCATABLE = (
    "PROCESS_MATERIALIZATION_REFERENCE_NOT_RELOCATABLE"
)
PROCESS_MATERIALIZATION_EXECUTION_PROFILE_INVALID = (
    "PROCESS_MATERIALIZATION_EXECUTION_PROFILE_INVALID"
)
PROCESS_MATERIALIZATION_PLAN_INVALID = "PROCESS_MATERIALIZATION_PLAN_INVALID"
PROCESS_MATERIALIZATION_FINGERPRINT_MISMATCH = (
    "PROCESS_MATERIALIZATION_FINGERPRINT_MISMATCH"
)
PROCESS_MATERIALIZATION_SYMBOL_BINDING_INVALID = (
    "PROCESS_MATERIALIZATION_SYMBOL_BINDING_INVALID"
)
PROCESS_MATERIALIZATION_PLACEMENT_NOT_FOUND = (
    "PROCESS_MATERIALIZATION_PLACEMENT_NOT_FOUND"
)
PROCESS_MATERIALIZATION_PLACEMENT_AMBIGUOUS = (
    "PROCESS_MATERIALIZATION_PLACEMENT_AMBIGUOUS"
)
PROCESS_MATERIALIZATION_RESULT_ID_MISSING = "PROCESS_MATERIALIZATION_RESULT_ID_MISSING"
#: A SERVER fault while materializing, distinct from every refusal above.
#: QA-153-r2-03: the apply arm hard-coded ``PLAN_INVALID`` for any exception, so
#: an internal ``NameError`` was served as a verdict on the caller's plan — the
#: caller's only actionable response being to change something that was never
#: wrong. A code that blames the server is the honest one, and it keeps
#: ``PLAN_INVALID`` meaning what it says.
PROCESS_MATERIALIZATION_INTERNAL_ERROR = "PROCESS_MATERIALIZATION_INTERNAL_ERROR"
#: The apply's POST-WRITE finalization failed — recording the build, computing
#: provenance, assembling the envelope. Distinct from the internal error above,
#: which is about materializing a plan: by the time this fires the mutation
#: decision is already made, and `mutation_status` reports it (Codex round 24).
PROCESS_MATERIALIZATION_FINALIZATION_FAILED = (
    "PROCESS_MATERIALIZATION_FINALIZATION_FAILED"
)


# --- Component update preservation (issue #45) ---------------------------------
# Registered by #153, OWNED by #45. The family has been served since #45's
# read-merge-write landed and was never in the taxonomy, so a caller could not
# discover or classify any of it. ALL SEVEN emitted members are here: the first
# pass registered only the four `integration_builder` serves directly and left
# the three the merge engine raises and `_apply_structured_update` forwards
# unchanged — which the exact-set test then codified as though the catalog were
# complete (Codex round 3). The set is derived from what the two modules
# actually emit, not from what one of them happens to name.
#
# Summaries are SIDE-NEUTRAL where the code is (Codex round 4): `merge_for_update`
# raises XML_PARSE_FAILED and OBJECT_MISSING for a malformed or absent DESIRED
# document as well as a live one, with `field` naming which. A summary that
# blamed the live component would send a consumer to inspect the wrong artifact. Codex review round 2 raised that against the
# one new member this slice adds; registering only that member would have been
# worse than leaving it out — it would put an #153-owned code outside #153's
# three declared prefixes and break the biconditional that keeps each family to
# one introducer. The honest fix is to register the whole family under the issue
# that actually introduced it.
UPDATE_PRESERVATION_POLICY_UNSUPPORTED = "UPDATE_PRESERVATION_POLICY_UNSUPPORTED"
UPDATE_PRESERVATION_XML_PARSE_FAILED = "UPDATE_PRESERVATION_XML_PARSE_FAILED"
UPDATE_PRESERVATION_OBJECT_MISSING = "UPDATE_PRESERVATION_OBJECT_MISSING"
UPDATE_PRESERVATION_MERGE_FAILED = "UPDATE_PRESERVATION_MERGE_FAILED"
UPDATE_PRESERVATION_TYPE_MISMATCH = "UPDATE_PRESERVATION_TYPE_MISMATCH"
UPDATE_PRESERVATION_FETCH_FAILED = "UPDATE_PRESERVATION_FETCH_FAILED"
#: #153: the push arm's counterpart to FETCH_FAILED. Its absence meant the
#: LOW-stakes arm (nothing written) was machine-classifiable while the
#: HIGH-stakes one (a write may have landed) was not.
UPDATE_PRESERVATION_PUSH_FAILED = "UPDATE_PRESERVATION_PUSH_FAILED"


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
                "On the strict/opt-in surface (IntegrationSpecV1 version='1.1'), "
                "an authored top-level pipeline view either disagrees with the "
                "normalized semantics of the single authored process, or is "
                "ambiguous because the spec authors two or more processes. "
                "Rejected at plan time, before collision resolution and before "
                "any mutation (#139D / ADR-001 §5)."
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
        ErrorCodeSpec(
            code=PROCESS_IR_REFERENCE_OPERATION_NOT_FOUND,
            category="process_ir",
            retryable=False,
            summary=(
                "A connector_call's operation reference resolves to no symbol, or to a "
                "symbol that is not a connector-action component."
            ),
            owner="#140",
        ),
        ErrorCodeSpec(
            code=PROCESS_IR_REFERENCE_CONNECTION_NOT_FOUND,
            category="process_ir",
            retryable=False,
            summary=(
                "The resolved operation declares no connection, or its connection "
                "reference resolves to no symbol."
            ),
            owner="#140",
        ),
        ErrorCodeSpec(
            code=PROCESS_IR_REFERENCE_CONNECTION_MISMATCH,
            category="process_ir",
            retryable=False,
            summary=(
                "The resolved connection is not a connector-settings component, or its "
                "connector family disagrees with the operation's."
            ),
            owner="#140",
        ),
        ErrorCodeSpec(
            code=PROCESS_IR_CAPABILITY_CONNECTOR_ACTION_UNSUPPORTED,
            category="process_ir",
            retryable=False,
            summary=(
                "The connector family/action pair is not in the verified connector-call "
                "capability registry, or an authored action asserts a different action "
                "than the operation's."
            ),
            owner="#140",
        ),
        ErrorCodeSpec(
            code=PROCESS_IR_SEMANTIC_PROFILE_MISMATCH,
            category="process_ir",
            retryable=False,
            summary=(
                "A map_ref's source/target profile does not match the profile of the "
                "connector call on that side of it."
            ),
            owner="#140",
        ),
        ErrorCodeSpec(
            code=PROCESS_IR_SEMANTIC_CARDINALITY_MISMATCH,
            category="process_ir",
            retryable=False,
            summary=(
                "A connector call's document cardinality is impossible at its position: "
                "a document consumer with no producer before it, or a step after a call "
                "that produces no documents."
            ),
            owner="#140",
        ),
        ErrorCodeSpec(
            code=PROCESS_IR_COMPILE_CONNECTOR_BINDING_INVALID,
            category="process_ir",
            retryable=False,
            summary=(
                "A connector-call binding is missing, stale, or self-contradictory at "
                "emission time — a compiler defect, not authored input."
            ),
            owner="#140",
        ),
        ErrorCodeSpec(
            code=PROCESS_IR_SCHEMA_BRANCH_CARDINALITY,
            category="process_ir",
            retryable=False,
            summary=(
                "A Branch declares fewer than 2 or more than 25 legs — the platform's "
                "own documented bound on Branch paths."
            ),
            owner="#141",
        ),
        ErrorCodeSpec(
            code=PROCESS_IR_SEMANTIC_CONTROL_CONTINUATION_UNSUPPORTED,
            category="process_ir",
            retryable=False,
            summary=(
                "A node was authored after a Branch or Decision. Control nodes are "
                "terminal fan-out in ProcessIR v1; continuation after them is gated."
            ),
            owner="#141",
        ),
        ErrorCodeSpec(
            code=PROCESS_IR_SEMANTIC_JOIN_UNSUPPORTED,
            category="process_ir",
            retryable=False,
            summary=(
                "Two paths converge on one node. ProcessIR v1 emits no join/merge, so a "
                "node may have at most one predecessor."
            ),
            owner="#141",
        ),
        ErrorCodeSpec(
            code=PROCESS_IR_SEMANTIC_NESTING_LIMIT,
            category="process_ir",
            retryable=False,
            summary=(
                "Branch/Decision nesting exceeds the ProcessIR v1 control-depth bound. "
                "This is a compiler bound, not a Boomi platform limit."
            ),
            owner="#141",
        ),
        ErrorCodeSpec(
            code=PROCESS_IR_SEMANTIC_UNTERMINATED_PATH,
            category="process_ir",
            retryable=False,
            summary=(
                "A Branch leg or Decision outcome does not reach a terminal. Every "
                "divergent path must terminate independently."
            ),
            owner="#141",
        ),
        ErrorCodeSpec(
            code=PROCESS_IR_CAPABILITY_NODE_NOT_ALLOWED_IN_BODY,
            category="process_ir",
            retryable=False,
            summary=(
                "A known node kind was authored in a Branch leg or Decision arm slot "
                "whose capability registry does not admit it."
            ),
            owner="#141",
        ),
        ErrorCodeSpec(
            code=PROCESS_IR_COMPILE_CONTROL_WIRING_INVALID,
            category="process_ir",
            retryable=False,
            summary=(
                "Compiler-derived Branch/Decision wiring is wrong (count, order, labels, "
                "target, or cross-wiring) — a compiler defect, not authored input."
            ),
            owner="#141",
        ),
        ErrorCodeSpec(
            code=PROCESS_IR_SCHEMA_RETRY_COUNT,
            category="process_ir",
            retryable=False,
            summary=(
                "A Try/Catch retry count is not an integer from 0 through 5 — the "
                "platform's own documented bound on the Retry Count field."
            ),
            owner="#142",
        ),
        ErrorCodeSpec(
            code=PROCESS_IR_CAPABILITY_ERROR_SCOPE_UNSUPPORTED,
            category="process_ir",
            retryable=False,
            summary=(
                "A Try/Catch declares an unknown error scope, or places a known scope "
                "somewhere the compiler has no verified emitter shape for."
            ),
            owner="#142",
        ),
        ErrorCodeSpec(
            code=PROCESS_IR_SEMANTIC_RETRY_SOURCE_REEXECUTION,
            category="process_ir",
            retryable=False,
            summary=(
                "A positive retry count would re-run the flow's document source, "
                "duplicating everything it already produced."
            ),
            owner="#142",
        ),
        ErrorCodeSpec(
            code=PROCESS_IR_SEMANTIC_RETRY_NON_IDEMPOTENT_WRITE,
            category="process_ir",
            retryable=False,
            summary=(
                "A retried connector call has no registry-backed retry safety, so "
                "replaying it could duplicate an external effect."
            ),
            owner="#142",
        ),
        ErrorCodeSpec(
            code=PROCESS_IR_SEMANTIC_IDEMPOTENCY_EVIDENCE_MISSING,
            category="process_ir",
            retryable=False,
            summary=(
                "A retried connector call needs typed idempotency evidence that is "
                "absent, of the wrong kind, or does not resolve to its operation."
            ),
            owner="#142",
        ),
        ErrorCodeSpec(
            code=PROCESS_IR_SEMANTIC_CATCH_UNTERMINATED,
            category="process_ir",
            retryable=False,
            summary=(
                "A Try/Catch catch body does not reach a terminal. Every caught "
                "document must end on a terminal step."
            ),
            owner="#142",
        ),
        ErrorCodeSpec(
            code=PROCESS_IR_COMPILE_ERROR_REGION_INVALID,
            category="process_ir",
            retryable=False,
            summary=(
                "A compiler-derived Try/Catch error region is structurally invalid "
                "(edges, ordinals, or containment) — a compiler defect, not authored input."
            ),
            owner="#142",
        ),
        # --- #143 M12.8: unified semantic validation ----------------------
        ErrorCodeSpec(
            code=PROCESS_IR_REFERENCE_COMPONENT_NOT_FOUND,
            category="process_ir",
            retryable=False,
            summary=(
                "An authored component reference does not resolve to a symbol. "
                "Applies to component roles other than a connector operation or "
                "connection, which keep their own specialized codes."
            ),
            owner="#143",
        ),
        ErrorCodeSpec(
            code=PROCESS_IR_REFERENCE_COMPONENT_TYPE_MISMATCH,
            category="process_ir",
            retryable=False,
            summary=(
                "A resolved component symbol is of the wrong component type for "
                "the role it is referenced in."
            ),
            owner="#143",
        ),
        ErrorCodeSpec(
            code=PROCESS_IR_CAPABILITY_EFFECT_CONTRACT_INVALID,
            category="process_ir",
            retryable=False,
            summary=(
                "A typed map/script/subprocess effect contract is malformed, "
                "bound to the wrong component, or its script digest does not "
                "match the source it claims to describe."
            ),
            owner="#143",
        ),
        ErrorCodeSpec(
            code=PROCESS_IR_CAPABILITY_PROCESS_CALL_RETURN_PATH_BINDING_UNSUPPORTED,
            category="process_ir",
            retryable=False,
            summary=(
                "A process call was authored so that execution continues past "
                "it. A call's outbound path is a projection of the called "
                "process's return-document shapes, so ProcessIR v1 supports the "
                "terminal form only; binding a returning child's return paths "
                "is gated as process_call_return_path_binding."
            ),
            owner="#175",
        ),
        ErrorCodeSpec(
            code=PROCESS_IR_SEMANTIC_LINEAGE_PROPERTY_READ_BEFORE_WRITE,
            category="process_ir",
            retryable=False,
            summary=(
                "A process/document property or cache key is read on a path "
                "where no prior write establishes it."
            ),
            owner="#143",
        ),
        ErrorCodeSpec(
            code=PROCESS_IR_SEMANTIC_LINEAGE_DDP_SCOPE_INVALID,
            category="process_ir",
            retryable=False,
            summary=(
                "A dynamic DOCUMENT property is read outside the document copy "
                "that wrote it — DDP is document-scoped, so a sibling Branch "
                "leg's write cannot establish it."
            ),
            owner="#143",
        ),
        ErrorCodeSpec(
            code=PROCESS_IR_SEMANTIC_LINEAGE_BRANCH_ORDER_INVALID,
            category="process_ir",
            retryable=False,
            summary=(
                "A Branch leg depends on state written by a LATER leg. Branch "
                "legs execute sequentially, so only an earlier leg's write is "
                "visible."
            ),
            owner="#143",
        ),
        ErrorCodeSpec(
            code=PROCESS_IR_SEMANTIC_LINEAGE_CACHE_WRITER_MISSING,
            category="process_ir",
            retryable=False,
            summary=(
                "A document cache is read on a path with no preceding write to "
                "that cache."
            ),
            owner="#143",
        ),
        ErrorCodeSpec(
            code=PROCESS_IR_SEMANTIC_LINEAGE_AMBIGUOUS_LAST_WRITE,
            category="process_ir",
            retryable=False,
            summary=(
                "Converging paths establish a property or cache key with "
                "different last writers, so its value at the merge is not "
                "determined."
            ),
            owner="#143",
        ),
        ErrorCodeSpec(
            code=PROCESS_IR_SEMANTIC_LINEAGE_EFFECT_UNKNOWN,
            category="process_ir",
            retryable=False,
            summary=(
                "A map, script or called subprocess has no typed effect "
                "contract, so its reads and writes are unknown. Unknown effects "
                "never establish state. "
                "Supply a contract whose content a server-side authority backs — "
                "inspection of the resolved component, or a vetted script "
                "registry entry — or write the state explicitly; a declaration "
                "the server cannot corroborate is inert."
            ),
            owner="#143",
        ),
        ErrorCodeSpec(
            code=PROCESS_IR_SEMANTIC_LINEAGE_EXTERNAL_WRITER_ASSUMED,
            category="process_ir",
            retryable=False,
            summary=(
                "State is assumed to be established by a declared external "
                "writer rather than by this process. Non-fatal, but the "
                "assumption is recorded."
            ),
            owner="#143",
        ),
        ErrorCodeSpec(
            code=PROCESS_IR_SEMANTIC_SIDE_EFFECT_ORDERING_UNSAFE,
            category="process_ir",
            retryable=False,
            summary=(
                "Two side effects are ordered in a way the flow demonstrably "
                "cannot guarantee — for example a read that depends on a write "
                "performed by a non-waiting subprocess call."
            ),
            owner="#143",
        ),
        ErrorCodeSpec(
            code=PROCESS_IR_SEMANTIC_SIDE_EFFECT_ORDERING_UNKNOWN,
            category="process_ir",
            retryable=False,
            summary=(
                "An asynchronous or shared-state ordering cannot be proven "
                "either safe or unsafe from the declared facts."
            ),
            owner="#143",
        ),
        ErrorCodeSpec(
            code=PROCESS_IR_SEMANTIC_RETRY_EFFECT_UNSAFE,
            category="process_ir",
            retryable=False,
            summary=(
                "A retried region replays a NON-connector effect that has no "
                "established replay safety. Connector retry hazards keep their "
                "own #142 codes."
            ),
            owner="#143",
        ),
        ErrorCodeSpec(
            code=LEGACY_ADAPTER_EXEMPTION_OPAQUE_STATE_WRITER,
            category="process_ir",
            retryable=False,
            summary=(
                "Advisory: a named legacy adapter policy accepts an opaque "
                "map/script state writer that strict ProcessIR validation would "
                "not treat as proof."
            ),
            owner="#143",
        ),
        ErrorCodeSpec(
            code=LEGACY_ADAPTER_EXEMPTION_DECISION_PROPERTY_READ,
            category="process_ir",
            retryable=False,
            summary=(
                "Advisory: a named legacy adapter policy accepts a Decision-arm "
                "property read that is not established on every outcome."
            ),
            owner="#143",
        ),
        ErrorCodeSpec(
            code=LEGACY_ADAPTER_EXEMPTION_STANDALONE_CACHE_READ,
            category="process_ir",
            retryable=False,
            summary=(
                "Advisory: a named legacy adapter policy accepts a cache read "
                "with no in-process writer, as the legacy walker did."
            ),
            owner="#143",
        ),
        ErrorCodeSpec(
            code=LEGACY_ADAPTER_EXEMPTION_SUBPROCESS_SUMMARY,
            category="process_ir",
            retryable=False,
            summary=(
                "Advisory: a named legacy adapter policy accepts a Process Call "
                "with no typed child effect summary."
            ),
            owner="#143",
        ),
        # --- #144 M12.9: capability-gated topology planning ------------------
        ErrorCodeSpec(
            code=TOPOLOGY_SCHEMA_UNKNOWN_OBJECT,
            category="topology",
            retryable=False,
            summary=(
                "An authored topology object carries a missing or unrecognized "
                "'kind' discriminator."
            ),
            owner="#144",
        ),
        ErrorCodeSpec(
            code=TOPOLOGY_SCHEMA_UNKNOWN_RELATION,
            category="topology",
            retryable=False,
            summary=(
                "An authored topology relation carries a missing or unrecognized "
                "'kind' discriminator."
            ),
            owner="#144",
        ),
        ErrorCodeSpec(
            code=TOPOLOGY_SCHEMA_UNKNOWN_FIELD,
            category="topology",
            retryable=False,
            summary=(
                "An unknown or prohibited field was authored; topology carries "
                "opaque references only, never secrets, XML or free-form config."
            ),
            owner="#144",
        ),
        ErrorCodeSpec(
            code=TOPOLOGY_SCHEMA_INVALID_CARDINALITY,
            category="topology",
            retryable=False,
            summary=(
                "A topology collection or binding violates its documented "
                "cardinality."
            ),
            owner="#144",
        ),
        ErrorCodeSpec(
            code=TOPOLOGY_SCHEMA_DUPLICATE_KEY,
            category="topology",
            retryable=False,
            summary=(
                "A topology object key, relation key, or semantic relation tuple "
                "is declared more than once."
            ),
            owner="#144",
        ),
        ErrorCodeSpec(
            code=TOPOLOGY_SCHEMA_VERSION_UNSUPPORTED,
            category="topology",
            retryable=False,
            summary="The authored topology 'version' is missing or unsupported.",
            owner="#144",
        ),
        ErrorCodeSpec(
            code=TOPOLOGY_SCHEMA_INVALID,
            category="topology",
            retryable=False,
            summary=(
                "The authored topology payload does not conform to the strict "
                "SystemTopologySpecV1 schema."
            ),
            owner="#144",
        ),
        ErrorCodeSpec(
            code=TOPOLOGY_REFERENCE_NOT_FOUND,
            category="topology",
            retryable=False,
            summary=(
                "A relation role or external reference does not resolve within "
                "the profile-qualified topology context."
            ),
            owner="#144",
        ),
        ErrorCodeSpec(
            code=TOPOLOGY_REFERENCE_TYPE_MISMATCH,
            category="topology",
            retryable=False,
            summary=(
                "A relation role resolves to an object or component of a kind "
                "that role does not accept."
            ),
            owner="#144",
        ),
        ErrorCodeSpec(
            code=TOPOLOGY_RELATION_UNSUPPORTED,
            category="topology",
            retryable=False,
            summary=(
                "The authored relation shape is not a lifecycle the Boomi "
                "platform supports."
            ),
            owner="#144",
        ),
        ErrorCodeSpec(
            code=TOPOLOGY_CAPABILITY_GATED,
            category="topology",
            retryable=False,
            summary=(
                "The subject is representable as declared intent only: required "
                "evidence for it is missing, so it never enters an apply plan."
            ),
            owner="#144",
        ),
        ErrorCodeSpec(
            code=TOPOLOGY_ENVIRONMENT_MISMATCH,
            category="topology",
            retryable=False,
            summary=(
                "Profile or environment evidence is inconsistent; topology "
                "never crosses a credential profile."
            ),
            owner="#144",
        ),
        ErrorCodeSpec(
            code=TOPOLOGY_DEPENDENCY_CYCLE,
            category="topology",
            retryable=False,
            summary=(
                "The cross-process invocation graph contains a cycle, so no "
                "deterministic runtime order exists."
            ),
            owner="#144",
        ),
        ErrorCodeSpec(
            code=TOPOLOGY_APPLY_NOT_SUPPORTED,
            category="topology",
            retryable=False,
            summary=(
                "SystemTopologySpecV1 is a planning contract; it has no apply, "
                "deploy, schedule or execute path."
            ),
            owner="#144",
        ),
        ErrorCodeSpec(
            code=RECIPE_NOT_FOUND,
            category="recipe",
            retryable=False,
            summary="No registered recipe carries the requested recipe id.",
            owner="#145",
        ),
        ErrorCodeSpec(
            code=RECIPE_VERSION_UNAVAILABLE,
            category="recipe",
            retryable=False,
            summary=(
                "The recipe id is registered but not at the requested version; "
                "an exact version request never falls forward or backward."
            ),
            owner="#145",
        ),
        ErrorCodeSpec(
            code=RECIPE_CAPABILITY_GATED,
            category="recipe",
            retryable=False,
            summary=(
                "A capability the recipe descriptor requires is absent, gated, "
                "guidance-only or unsupported in its canonical authority."
            ),
            owner="#145",
        ),
        ErrorCodeSpec(
            code=RECIPE_INPUT_INVALID,
            category="recipe",
            retryable=False,
            summary=(
                "The recipe input failed the forbidden-shape pre-scan or its "
                "descriptor-declared strict input model."
            ),
            owner="#145",
        ),
        ErrorCodeSpec(
            code=RECIPE_CONTRIBUTION_INVALID,
            category="recipe",
            retryable=False,
            summary=(
                "A registered executor returned something that is not a declared, "
                "strictly-valid typed contribution."
            ),
            owner="#145",
        ),
        ErrorCodeSpec(
            code=RECIPE_PATCH_TARGET_NOT_FOUND,
            category="recipe",
            retryable=False,
            summary=(
                "A closed patch operation names a semantic slot that does not "
                "resolve after deterministic composition."
            ),
            owner="#145",
        ),
        ErrorCodeSpec(
            code=RECIPE_PATCH_CONFLICT,
            category="recipe",
            retryable=False,
            summary=(
                "Two attributed writers target one semantic slot with no merge "
                "rule both descriptors declare."
            ),
            owner="#145",
        ),
        ErrorCodeSpec(
            code=RECIPE_CONSTRAINT_FAILED,
            category="recipe",
            retryable=False,
            summary=(
                "A canonical validator or a declared ConstraintRequirement "
                "rejected the assembled recipe output."
            ),
            owner="#145",
        ),
        ErrorCodeSpec(
            code=RECIPE_REQUEST_INVALID,
            category="recipe",
            retryable=False,
            summary=(
                "The recipe REQUEST envelope is malformed — e.g. two invocations "
                "sharing one invocation_id."
            ),
            owner="#145",
        ),
        ErrorCodeSpec(
            code=RECIPE_OUTPUT_NONDETERMINISTIC,
            category="recipe",
            retryable=False,
            summary=(
                "Two executions of one recipe over independently rebuilt inputs "
                "produced different canonical contributions."
            ),
            owner="#145",
        ),
        ErrorCodeSpec(
            code=AUTHORING_SCHEMA_SOURCE_UNAVAILABLE,
            category="authoring_surface",
            retryable=True,
            summary=(
                "An authoring selector could not be built because an authority "
                "it derives from failed. The selector is real and the request "
                "was valid, so the honest answer is that it is temporarily "
                "unavailable rather than a partial contract that looks whole."
            ),
            owner="#146",
        ),
        ErrorCodeSpec(
            code=AUTHORING_SCHEMA_VERSION_UNAVAILABLE,
            category="authoring_surface",
            retryable=False,
            summary=(
                "A known schema selector was requested at a version this server "
                "does not serve, or a typed payload declared one."
            ),
            owner="#146",
        ),
        ErrorCodeSpec(
            code=AUTHORING_CAPABILITY_REVISION_MISMATCH,
            category="authoring_surface",
            retryable=False,
            summary=(
                "The caller's expected capability revision differs from the "
                "running service's; rediscover before authoring."
            ),
            owner="#146",
        ),
        ErrorCodeSpec(
            code=AUTHORING_LIVE_DEPLOYMENT_DRIFT,
            category="authoring_surface",
            retryable=False,
            summary=(
                "A build-owned live component no longer matches the fingerprint "
                "recorded when the build applied it."
            ),
            owner="#146",
        ),
        ErrorCodeSpec(
            code=AUTHORING_REQUIRED_DECISION_MISSING,
            category="authoring_surface",
            retryable=False,
            summary=(
                "A required user decision has no resolution, or names an option "
                "the decision does not offer."
            ),
            owner="#146",
        ),
        ErrorCodeSpec(
            code=AUTHORING_COMPILE_BLOCKED,
            category="authoring_surface",
            retryable=False,
            summary=(
                "Canonical compilation refused the typed intent; the causative "
                "canonical diagnostics are carried value-free."
            ),
            owner="#146",
        ),
        ErrorCodeSpec(
            code=AUTHORING_PLAN_STALE,
            category="authoring_surface",
            retryable=False,
            summary=(
                "The recomputed plan hash differs from the one the caller bound "
                "to; re-plan and recompile."
            ),
            owner="#146",
        ),
        ErrorCodeSpec(
            code=AUTHORING_APPLY_VALIDATION_REQUIRED,
            category="authoring_surface",
            retryable=False,
            summary=(
                "A typed apply omitted or could not reproduce its compile "
                "binding; nothing was mutated."
            ),
            owner="#146",
        ),
        # --- M12.15 / issue #153 -------------------------------------------
        ErrorCodeSpec(
            code=PROCESS_COMPONENT_SCHEMA_UNKNOWN_FIELD,
            category="process_component",
            retryable=False,
            summary=(
                "A process component envelope, unit or extension binding carried "
                "a field the closed schema does not define."
            ),
            owner="#153",
        ),
        ErrorCodeSpec(
            code=PROCESS_COMPONENT_SCHEMA_INVALID,
            category="process_component",
            retryable=False,
            summary=(
                "A process component envelope or extension binding field failed "
                "its value rule (blank, padded, or contradictory)."
            ),
            owner="#153",
        ),
        ErrorCodeSpec(
            code=PROCESS_COMPONENT_SCHEMA_INVALID_CARDINALITY,
            category="process_component",
            retryable=False,
            summary=(
                "A process authoring unit did not pair exactly one envelope with "
                "exactly one ProcessIR root."
            ),
            owner="#153",
        ),
        ErrorCodeSpec(
            code=PROCESS_COMPONENT_REFERENCE_INVALID_FORMAT,
            category="process_component",
            retryable=False,
            summary=(
                "A process component reference was not an exact '$ref:KEY' token "
                "or literal component id."
            ),
            owner="#153",
        ),
        ErrorCodeSpec(
            code=INTEGRATION_COMPONENT_KEY_DUPLICATE,
            category="integration_dependency",
            retryable=False,
            summary=(
                "One key was declared more than once across the shared "
                "components / process-envelope namespace."
            ),
            owner="#153",
        ),
        ErrorCodeSpec(
            code=INTEGRATION_DEPENDENCY_NOT_FOUND,
            category="integration_dependency",
            retryable=False,
            summary=(
                "A depends_on entry names a key that no component and no process "
                "envelope declares."
            ),
            owner="#153",
        ),
        ErrorCodeSpec(
            code=INTEGRATION_DEPENDENCY_CYCLE,
            category="integration_dependency",
            retryable=False,
            summary=(
                "The combined component/process dependency graph contains a "
                "cycle, so no execution order exists."
            ),
            owner="#153",
        ),
        ErrorCodeSpec(
            code=INTEGRATION_DEPENDENCY_REQUIRED,
            category="integration_dependency",
            retryable=False,
            summary=(
                "A process root references a key its envelope does not declare in "
                "depends_on, so ordered apply could not bind it."
            ),
            owner="#153",
        ),
        ErrorCodeSpec(
            code=PROCESS_MATERIALIZATION_REFERENCE_NOT_RELOCATABLE,
            category="process_materialization",
            retryable=False,
            summary=(
                "A materializable process referenced a literal account component "
                "id; a relocatable plan may only carry '$ref:KEY' tokens."
            ),
            owner="#153",
        ),
        ErrorCodeSpec(
            code=PROCESS_MATERIALIZATION_EXECUTION_PROFILE_INVALID,
            category="process_materialization",
            retryable=False,
            summary=(
                "The compiler could not derive a supported scheduled/listener "
                "execution profile from the ProcessIR entry node."
            ),
            owner="#153",
        ),
        ErrorCodeSpec(
            code=PROCESS_MATERIALIZATION_PLAN_INVALID,
            category="process_materialization",
            retryable=False,
            summary=(
                "A materialization plan was internally inconsistent: malformed or "
                "duplicated symbol slots, or plan/envelope disagreement."
            ),
            owner="#153",
        ),
        ErrorCodeSpec(
            code=PROCESS_MATERIALIZATION_FINGERPRINT_MISMATCH,
            category="process_materialization",
            retryable=False,
            summary=(
                "A plan's recorded fingerprint does not equal the fingerprint "
                "recomputed from its canonical material; nothing was mutated."
            ),
            owner="#153",
        ),
        ErrorCodeSpec(
            code=PROCESS_MATERIALIZATION_SYMBOL_BINDING_INVALID,
            category="process_materialization",
            retryable=False,
            summary=(
                "Late binding could not resolve a required symbol slot to a real "
                "component id, or a placeholder survived into the emitted XML."
            ),
            owner="#153",
        ),
        ErrorCodeSpec(
            code=PROCESS_MATERIALIZATION_PLACEMENT_NOT_FOUND,
            category="process_materialization",
            retryable=False,
            summary=(
                "The requested folder name matched no live, non-deleted account "
                "folder, so placement could not be resolved."
            ),
            owner="#153",
        ),
        ErrorCodeSpec(
            code=PROCESS_MATERIALIZATION_PLACEMENT_AMBIGUOUS,
            category="process_materialization",
            retryable=False,
            summary=(
                "The requested folder name matched more than one live account "
                "folder; placement is refused rather than guessed."
            ),
            owner="#153",
        ),
        ErrorCodeSpec(
            code=PROCESS_MATERIALIZATION_RESULT_ID_MISSING,
            category="process_materialization",
            retryable=False,
            summary=(
                "A create reported success without a component id, so the "
                "mutation could not be attested; it fails closed."
            ),
            owner="#153",
        ),
        ErrorCodeSpec(
            code=UPDATE_PRESERVATION_POLICY_UNSUPPORTED,
            category="update_preservation",
            retryable=False,
            summary=(
                "The component type has no registered preservation policy, so a "
                "read-merge-write update is refused rather than guessed."
            ),
            owner="#45",
        ),
        ErrorCodeSpec(
            code=UPDATE_PRESERVATION_XML_PARSE_FAILED,
            category="update_preservation",
            retryable=False,
            summary=(
                "XML on one side of the merge did not parse. `field` names the "
                "side — the live document or the desired one. Nothing was "
                "written."
            ),
            owner="#45",
        ),
        ErrorCodeSpec(
            code=UPDATE_PRESERVATION_OBJECT_MISSING,
            category="update_preservation",
            retryable=False,
            summary=(
                "A required object body is absent on one side of the merge. "
                "Both emitting branches set `field=\"owned_path\"`, so the side "
                "is in `details.side` (`current` or `desired`) — not in `field`. "
                "Merging without it would silently drop the subtree the policy "
                "exists to preserve."
            ),
            owner="#45",
        ),
        ErrorCodeSpec(
            code=UPDATE_PRESERVATION_MERGE_FAILED,
            category="update_preservation",
            retryable=False,
            summary=(
                "Read-merge-write could not produce a document that preserves "
                "the policy's owned paths; nothing was written."
            ),
            owner="#45",
        ),
        ErrorCodeSpec(
            code=UPDATE_PRESERVATION_TYPE_MISMATCH,
            category="update_preservation",
            retryable=False,
            summary=(
                "The live component's type/subType does not match the submitted "
                "document, so merging them would rewrite a different component."
            ),
            owner="#45",
        ),
        ErrorCodeSpec(
            code=UPDATE_PRESERVATION_FETCH_FAILED,
            category="update_preservation",
            retryable=True,
            summary=(
                "The live XML could not be read, so there is nothing to merge "
                "into; nothing was written."
            ),
            owner="#45",
        ),
        ErrorCodeSpec(
            code=UPDATE_PRESERVATION_PUSH_FAILED,
            category="update_preservation",
            retryable=False,
            summary=(
                "The merged document was rejected on submission. Unlike a fetch "
                "failure, a write may already have landed — reconcile before "
                "retrying."
            ),
            owner="#45",
        ),
        ErrorCodeSpec(
            code=PROCESS_MATERIALIZATION_INTERNAL_ERROR,
            category="process_materialization",
            retryable=False,
            summary=(
                "Materialization failed for a reason that is not a defect in the "
                "caller's request; the server, not the plan, is at fault."
            ),
            owner="#153",
        ),
        ErrorCodeSpec(
            code=PROCESS_MATERIALIZATION_FINALIZATION_FAILED,
            category="process_materialization",
            # Served ONLY on the no-write path, where the steps' own statuses
            # prove nothing was mutated — so the retry this code advertises is
            # safe by the same evidence the envelope reports. The mid-write
            # escape carries no code, exactly as before, because there the
            # honest answer is that the outcome is unknown.
            retryable=True,
            summary=(
                "The apply's post-write finalization failed after a run that "
                "mutated nothing; every root was reused, so nothing was "
                "created and the request may be retried."
            ),
            owner="#153",
        ),
    )
}

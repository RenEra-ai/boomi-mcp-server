"""The sanitized ProcessIR authoring projection (issue #146 amendment).

Builds the ``process_ir_authoring`` contract that ``get_schema_template``
serves: one addressable entry per public ProcessIR node, capability, body
placement, connector action, diagnostic, state scope, doctrine pattern and
recipe, each carrying the runtime authority it came from.

**Output only.** Nothing here is an authority. Every fact is either GENERATED
from a named runtime registry — so it cannot disagree with the code that
enforces it — or PARITY_PINNED, meaning a CI test asserts the prose against the
named source in both directions. Nothing a caller sends re-enters the compiler
through this module; the registries stay the sole enforcement path.

**Every compiler import is function-local.** ``tests/
test_process_ir_compiler_surface.py`` asserts that ``import server`` pulls in
zero ``boomi_mcp.compiler`` modules, and ``meta_tools`` imports this package. A
module-scope compiler import here would break that guarantee for every caller,
not just the ones that ask for the contract.

**The public vocabulary is not the internal one.** Three of the connector
registry's field names and one body-context name are compiler-internal
identifiers the served surface may not carry, so they are projected through the
total, injective maps the owning modules publish
(``PUBLIC_CAPABILITY_FIELDS``, ``PUBLIC_BODY_CONTEXTS``). The classification
VALUES cross verbatim — renaming a state would be the same drift this contract
exists to remove.
"""

from __future__ import annotations

from typing import Any, Dict, List, Mapping, NamedTuple, Optional, Tuple

from ..models.process_ir_authoring import (
    PROCESS_IR_AUTHORING_BYTE_BUDGET,
    PROCESS_IR_AUTHORING_CONTRACT_VERSION,
    PROCESS_IR_AUTHORING_DEFAULT_LIMIT,
    PROCESS_IR_AUTHORING_ENTRY_TYPES,
    PROCESS_IR_AUTHORING_MAX_LIMIT,
    PROCESS_IR_AUTHORING_WORKFLOW_STAGES,
    ProcessIRAuthoringContractEntryV1,
    ProcessIRAuthoringContractPageV1,
    ProcessIRAuthoringFacetsV1,
    ProcessIRAuthoringPlacementV1,
    ProcessIRAuthoringQueryV1,
    ProcessIRAuthoringReferenceV1,
    ProcessIRAuthoringSourceV1,
    ProcessIRAuthoringStateMappingV1,
    ProcessIRDocumentSemanticsV1,
    canonical_entry_json,
)

# ---------------------------------------------------------------------------
# Symbolic source ids
# ---------------------------------------------------------------------------
#
# SYMBOLIC, never a module path or a Python identifier. Two reasons, and the
# second is the binding one: a path describes the server's disk, which a caller
# cannot see; and the registries' own names are compiler-internal identifiers
# the served surface is forbidden to carry at all.

SOURCE_MODELS = "runtime.process_ir_models"
SOURCE_CAPABILITIES = "runtime.process_ir_capability_manifest"
SOURCE_PLACEMENTS = "runtime.body_placement_registry"
SOURCE_CONNECTORS = "runtime.connector_capability_registry"
SOURCE_RETRY = "runtime.replay_safety_rules"
SOURCE_STATE = "runtime.state_visibility_model"
SOURCE_PROCESS_PROPERTY = "runtime.process_property_scope"
SOURCE_PARSE_DIAGNOSTICS = "runtime.process_ir_parse_diagnostics"
SOURCE_VALIDATION_DIAGNOSTICS = "runtime.semantic_validation_findings"
SOURCE_COMPILER_DIAGNOSTICS = "runtime.compiler_diagnostics"
SOURCE_DOCTRINE = "runtime.design_doctrine_registry"
SOURCE_RECIPES = "runtime.recipe_registry"
SOURCE_CONTRIBUTIONS = "runtime.recipe_contribution_kinds"


class ProcessIRAuthoringQueryError(ValueError):
    """An invalid retrieval filter, reported with the values that ARE valid.

    Carries the offending field and its facet rather than a message, so the
    serving layer renders one bounded error envelope and a caller is told what
    to ask for instead of only that they asked wrongly.
    """

    def __init__(self, field: str, allowed: Tuple[str, ...] = ()) -> None:
        super().__init__(field)
        self.field = field
        self.allowed = tuple(allowed)


# ---------------------------------------------------------------------------
# Source snapshot
# ---------------------------------------------------------------------------


class ProjectionSourcesV1(NamedTuple):
    """An immutable snapshot of every runtime authority the projection reads.

    Exists so drift NEGATIVE CONTROLS can perturb one source and rebuild,
    without monkeypatching a production mapping. A test that mutates the real
    ``PROCESS_IR_V1_CAPABILITIES`` leaks into every later test in the process;
    one that passes a modified snapshot here cannot.
    """

    node_kinds: Tuple[str, ...]
    schema_defs: Mapping[str, Any]
    capability_rows: Mapping[str, str]
    placement_rows: Tuple[Tuple[str, str, Tuple[str, ...]], ...]
    connector_rows: Tuple[Mapping[str, Any], ...]
    retry_rules: Tuple[Mapping[str, Any], ...]
    state_visibility: Tuple[Mapping[str, Any], ...]
    process_property_scope: Mapping[str, Any]
    parse_specs: Tuple[Mapping[str, str], ...]
    finding_specs: Tuple[Mapping[str, str], ...]
    compiler_specs: Tuple[Mapping[str, str], ...]
    doctrine_rows: Tuple[Mapping[str, Any], ...]
    recipe_entries: Tuple[Mapping[str, Any], ...]
    contribution_kinds: Tuple[str, ...]


def collect_projection_sources() -> ProjectionSourcesV1:
    """Read every authority once. All compiler imports are function-local."""
    from ..compiler.process_ir.body_capabilities import body_placement_rows
    from ..compiler.process_ir.connector_capabilities import connector_capability_rows
    from ..compiler.process_ir.diagnostics import compiler_diagnostic_specs
    from ..compiler.process_ir.error_handling import retry_rule_specs
    from ..compiler.process_ir.semantic_validation.findings import finding_specs
    from ..compiler.process_ir.semantic_validation.lineage import state_visibility_rows
    from ..kb.design_doctrine import DESIGN_DOCTRINE_ENTRIES
    from ..models.cache_property_models import PROCESS_PROPERTY_SCOPE_V1
    from ..models.process_ir import (
        PROCESS_IR_V1_CAPABILITIES,
        process_ir_v1_json_schema,
        process_ir_v1_node_kinds,
        process_ir_v1_parse_diagnostic_specs,
    )
    from ..models.recipe_contributions import RECIPE_CONTRIBUTION_KINDS
    from ..recipes import production_registry

    try:
        recipe_entries = tuple(production_registry().snapshot().get("entries", ()))
    except Exception:  # noqa: BLE001 — a registry that cannot build is reported empty
        recipe_entries = ()

    return ProjectionSourcesV1(
        node_kinds=process_ir_v1_node_kinds(),
        schema_defs=process_ir_v1_json_schema().get("$defs", {}),
        capability_rows=dict(PROCESS_IR_V1_CAPABILITIES),
        placement_rows=body_placement_rows(),
        connector_rows=connector_capability_rows(),
        retry_rules=retry_rule_specs(),
        state_visibility=state_visibility_rows(),
        process_property_scope=PROCESS_PROPERTY_SCOPE_V1,
        parse_specs=process_ir_v1_parse_diagnostic_specs(),
        finding_specs=finding_specs(),
        compiler_specs=compiler_diagnostic_specs(),
        doctrine_rows=tuple(
            dict(entry) for _, entry in sorted(DESIGN_DOCTRINE_ENTRIES.items())
        ),
        recipe_entries=recipe_entries,
        contribution_kinds=tuple(RECIPE_CONTRIBUTION_KINDS),
    )


# ---------------------------------------------------------------------------
# Canonical state mappings
# ---------------------------------------------------------------------------

#: Every source vocabulary, mapped onto the one canonical set.
#:
#: ``guidance_only`` and ``na`` map to ``unsupported`` with ``applicable=False``.
#: The flag is what keeps the mapping honest: doctrine's guidance-only patterns
#: are ADVICE, not features this contract could ever author, so calling them
#: "unsupported" without qualification would tell a caller a capability was
#: withdrawn when none was ever offered.
_STATE_MAPPINGS: Tuple[Tuple[str, str, str, bool], ...] = (
    ("process_ir_capability_manifest", "supported", "supported", True),
    ("process_ir_capability_manifest", "gated", "gated", True),
    ("process_ir_capability_manifest", "unsupported", "unsupported", True),
    ("design_doctrine", "emittable_today", "supported", True),
    ("design_doctrine", "gated", "gated", True),
    ("design_doctrine", "guidance_only", "unsupported", False),
    ("design_doctrine", "na", "unsupported", False),
    ("body_placement_registry", "admitted", "supported", True),
    ("body_placement_registry", "absent", "unsupported", True),
    ("connector_capability_registry", "listed", "supported", True),
    ("connector_capability_registry", "absent", "unsupported", True),
    ("cache_property_authoring", "executable", "supported", True),
    ("cache_property_authoring", "gated", "gated", True),
    ("cache_property_authoring", "reserved", "unsupported", True),
)

#: The mapping as a lookup, so a projector never decides a state itself.
_STATE_LOOKUP: Mapping[Tuple[str, str], Tuple[str, bool]] = {
    (vocab, source): (canonical, applicable)
    for vocab, source, canonical, applicable in _STATE_MAPPINGS
}


def state_mappings() -> Tuple[ProcessIRAuthoringStateMappingV1, ...]:
    return tuple(
        ProcessIRAuthoringStateMappingV1(
            source_vocabulary=vocab,
            source_state=source,
            canonical_state=canonical,
            applicable=applicable,
        )
        for vocab, source, canonical, applicable in _STATE_MAPPINGS
    )


def _canonical_state(vocabulary: str, source_state: str) -> Tuple[str, bool]:
    """Map a source state, or fail loudly.

    A ``KeyError`` here is a real defect: it means a source grew a state that
    nobody decided a canonical meaning for, and inventing one on the spot is
    exactly how the projection would become a second authority.
    """
    return _STATE_LOOKUP[(vocabulary, source_state)]


# ---------------------------------------------------------------------------
# Per-node authoring facts (parity-pinned)
# ---------------------------------------------------------------------------
#
# The structural facts — which slots admit a kind, what a capability's state is,
# what a connector action does to documents — are GENERATED. What cannot be
# generated is the behavioural prose: "a Branch's legs run in the authored
# order", "a Send returns nothing so nothing may follow it". Those live here,
# one row per node kind, and every one is asserted against the runtime source by
# the parity suite.
#
# Kept deliberately small per node: the minimum needed to PLACE and COMPOSE the
# node, not a manual. Anything structural a caller can read off the JSON Schema
# is not repeated here.

_ORDERING = "ordering_facts"
_DOCS = "document_semantics"
_REFS = "required_references"
_CAPS = "capability_ids"
_CODES = "diagnostic_codes"
_RELATED = "related_entry_ids"
_STAGES = "workflow_stages"

_NODE_FACTS: Mapping[str, Mapping[str, Any]] = {
    "sequence": {
        "category": "structure",
        "title": "Sequence (process body)",
        "summary": (
            "The ordered body of the process. Steps run in the authored order, "
            "and the last one must be a terminal — every path ends explicitly."
        ),
        _ORDERING: (
            "Steps execute in the authored order, top to bottom.",
            "The root sequence must reach a terminal; an unterminated path is rejected.",
        ),
        _DOCS: ("optional", "documents", "per_document"),
        _STAGES: ("author", "plan"),
    },
    "source": {
        "category": "connector",
        "title": "Source endpoint",
        "summary": (
            "The entry connector call of a legacy linear process. Authors both "
            "the connection and the operation reference; the compiler derives "
            "connector metadata from the resolved components."
        ),
        _ORDERING: ("A source occupies the first position of the sequence it opens.",),
        _DOCS: ("none", "documents", "per_document"),
        _REFS: (("connection_ref", True), ("operation_ref", True)),
        _STAGES: ("author",),
    },
    "target": {
        "category": "connector",
        "title": "Target endpoint",
        "summary": (
            "A routed destination connector call. Authors both the connection "
            "and the operation reference."
        ),
        _ORDERING: (
            "On the legacy linear path a target is immediately followed by a stop.",
            "A target is a legal terminal of a Branch path and of a Decision true arm; "
            "it is not admitted on a Decision false arm.",
        ),
        _DOCS: ("required", "none", "per_document"),
        _REFS: (("connection_ref", True), ("operation_ref", True)),
        _STAGES: ("author",),
    },
    "connector_call": {
        "category": "connector",
        "title": "Connector call",
        "summary": (
            "A first-class connector call. Authors ONLY the operation reference "
            "— the connection is derived from the component plan, because a "
            "connector-action component does not declare its own connection. "
            "The optional action is an assertion of intent, never an override."
        ),
        _ORDERING: (
            "Whether a call may open a path, and whether anything may follow it, "
            "comes from its family/action row, not from its position.",
            "An action whose row produces no documents is terminal: nothing may "
            "consume what it did not return.",
            "Whether a call may sit inside a retried region comes from its replay "
            "classification; no caller-supplied evidence can lift a refusal.",
        ),
        _DOCS: ("optional", "documents", "per_document"),
        _REFS: (("operation_ref", True),),
        _CAPS: (
            "generalized_connector_call",
            "mixed_connector_execution",
            "connector_call_in_control_body",
            "typed_idempotency_evidence",
        ),
        _STAGES: ("author", "repair"),
    },
    "flow_control": {
        "category": "control",
        "title": "Flow control (batching)",
        "summary": (
            "Batches the stream into groups of for_each_count documents. This is "
            "the only authorable flow-control mode: there is no caller-"
            "configurable parallel chunking or multiprocess execution."
        ),
        _ORDERING: (
            "Batching regroups the stream; it never splits or combines document "
            "contents — those are explicit data_process operations.",
            "A flow control never makes a path run concurrently.",
        ),
        _DOCS: ("required", "documents", "per_batch"),
        _CAPS: ("flow_control_parallel_chunks",),
        _STAGES: ("author",),
    },
    "message": {
        "category": "transform",
        "title": "Message",
        "summary": (
            "Replaces the stream with one document built from the authored text."
        ),
        _ORDERING: (
            "A message produces its own document, so it does not pass the inbound "
            "one through.",
        ),
        _DOCS: ("optional", "stream_replacing", "per_document"),
        _STAGES: ("author",),
    },
    "map_ref": {
        "category": "transform",
        "title": "Map reference",
        "summary": (
            "Applies a map component to the stream. Source and destination "
            "profiles live in the map component and are never authored here."
        ),
        _ORDERING: (
            "A map between two connector calls must take the earlier call's "
            "output profile and produce the later call's input profile.",
            "A map's property effects are unknown unless a typed effect contract "
            "declares them, so state written only inside a map is not established.",
        ),
        _DOCS: ("required", "documents", "per_document"),
        _REFS: (("map_ref", True),),
        _STAGES: ("author", "repair"),
    },
    "data_process": {
        "category": "transform",
        "title": "Data process",
        "summary": (
            "One or more explicit operations run in the authored order: custom "
            "scripting, split_documents, combine_documents. Document fan-out and "
            "fan-in are authored here and nowhere else."
        ),
        _ORDERING: (
            "Operations run in the authored order and each one's stream effect "
            "applies to the next.",
            "Splitting and combining documents is not the same as joining control "
            "paths; this contract emits no path join at all.",
        ),
        _DOCS: ("required", "stream_replacing", "per_document"),
        _STAGES: ("author",),
    },
    "cache_put": {
        "category": "state",
        "title": "Cache put (add to cache)",
        "summary": (
            "Writes the stream into a document cache and CONSUMES it: a cache_put "
            "in a step position must be followed immediately by a stream-"
            "replacing cache read."
        ),
        _ORDERING: (
            "A trailing cache_put belongs in a Branch path terminal (the staging "
            "shape), not in the step list.",
        ),
        _DOCS: ("required", "consumed", "all_documents"),
        _REFS: (("cache_ref", True),),
        _RELATED: ("state_visibility.cache",),
        _STAGES: ("author", "repair"),
    },
    "document_cache_retrieve": {
        "category": "state",
        "title": "Document cache retrieve",
        "summary": (
            "Legacy all-document cache read. Replaces the stream with the cached "
            "documents."
        ),
        _ORDERING: (
            "Retrieval is all-document; keyed or indexed retrieval is capability-gated.",
        ),
        _DOCS: ("optional", "stream_replacing", "all_documents"),
        _REFS: (("cache_ref", True),),
        _CAPS: ("keyed_cache",),
        _RELATED: ("state_visibility.cache",),
        _STAGES: ("author",),
    },
    "cache_get": {
        "category": "state",
        "title": "Cache get",
        "summary": (
            "All-document cache read. external_writer declares that the cache is "
            "populated outside this process, which is what makes a read with no "
            "in-process writer legitimate."
        ),
        _ORDERING: (
            "A cache read with no preceding write on the same path is reported "
            "unless an external writer is declared.",
        ),
        _DOCS: ("optional", "stream_replacing", "all_documents"),
        _REFS: (("cache_ref", True),),
        _CAPS: ("keyed_cache",),
        _RELATED: ("state_visibility.cache",),
        _STAGES: ("author", "repair"),
    },
    "cache_remove": {
        "category": "state",
        "title": "Cache remove",
        "summary": (
            "Removes ALL documents from a cache. Keyed removal is capability-gated."
        ),
        _ORDERING: (
            "Removal acts on the cache, not on the document stream flowing through "
            "this path.",
        ),
        _DOCS: ("optional", "documents", "all_documents"),
        _REFS: (("cache_ref", True),),
        _CAPS: ("keyed_cache",),
        _RELATED: ("state_visibility.cache",),
        _STAGES: ("author",),
    },
    "set_ddp": {
        "category": "state",
        "title": "Set dynamic document property",
        "summary": (
            "Writes a per-document property. The name is BARE — the wire prefix "
            "is owned by emission. Source values concatenate in the authored order."
        ),
        _ORDERING: (
            "Per-document state is not visible across sibling Branch paths: each "
            "path receives its own copy of the documents.",
            "Where paths converge, only state written on every incoming path is "
            "established.",
        ),
        _DOCS: ("required", "documents", "per_document"),
        _RELATED: ("state_visibility.ddp",),
        _STAGES: ("author", "repair"),
    },
    "set_dpp": {
        "category": "state",
        "title": "Set dynamic process property",
        "summary": (
            "Writes a per-execution property. The name is BARE. persist changes "
            "durability, never scope."
        ),
        _ORDERING: (
            "Per-execution state accumulates across Branch paths in the authored "
            "order, so a later path sees what an earlier one wrote.",
            "Reading in an earlier path what a later path writes is rejected; "
            "reordering the paths is the fix.",
        ),
        _DOCS: ("required", "documents", "per_document"),
        _RELATED: ("state_visibility.dpp",),
        _STAGES: ("author", "repair"),
    },
    "process_call": {
        "category": "orchestration",
        "title": "Process call",
        "summary": (
            "Invokes another process by opaque reference. wait=false returns "
            "immediately and therefore establishes no downstream state."
        ),
        _ORDERING: (
            "A process call may not share a root-to-leaf path with a connector "
            "call while that combination is capability-gated; sibling paths are "
            "independent and do not count as sharing.",
            "Where a process call is admitted in a control body, that body is "
            "process-call-only and ends in a stop.",
        ),
        _DOCS: ("required", "documents", "per_document"),
        _REFS: (("process_ref", True),),
        _CAPS: ("process_call_connector_mixing",),
        _STAGES: ("author", "repair"),
    },
    "branch": {
        "category": "control",
        "title": "Branch",
        "summary": (
            "Runs 2-25 paths in the authored order, SEQUENTIALLY — never at once. "
            "Each path receives an independent copy of the document stream."
        ),
        _ORDERING: (
            "Paths execute in the authored order, one fully before the next.",
            "A Branch terminalizes its path: nothing may follow it, and the paths "
            "are never rejoined.",
            "Nesting is bounded at two control levels. That is a bound of this "
            "contract, not a Boomi platform limit.",
            "A nested Branch is not a legal Branch-path terminal.",
        ),
        _CAPS: (
            "rich_branch_decision_bodies",
            "continuation_after_branch_or_decision",
            "joins",
            "parallel_branch_execution",
        ),
        _RELATED: (
            "placement.branch_path.step",
            "placement.branch_path.terminal",
            "state_visibility.dpp",
        ),
        _STAGES: ("author", "plan", "repair"),
    },
    "decision": {
        "category": "control",
        "title": "Decision",
        "summary": (
            "A two-way conditional over seven comparisons. Exactly one arm runs. "
            "The two arms admit DIFFERENT bodies."
        ),
        _ORDERING: (
            "A Decision terminalizes its path: nothing may follow it, and the arms "
            "are never rejoined.",
            "Where the arms would converge, only state written on BOTH is established.",
            "Nesting is bounded at two control levels — a bound of this contract, "
            "not a Boomi platform limit.",
        ),
        _CAPS: (
            "rich_branch_decision_bodies",
            "continuation_after_branch_or_decision",
            "joins",
        ),
        _RELATED: (
            "placement.decision_true_arm.step",
            "placement.decision_true_arm.terminal",
            "placement.decision_false_arm.step",
            "placement.decision_false_arm.terminal",
        ),
        _STAGES: ("author", "plan", "repair"),
    },
    "try_catch": {
        "category": "reliability",
        "title": "Try/Catch with bounded retry",
        "summary": (
            "A scoped error handler. A process scope must be the sole root step; "
            "a connector scope must follow the call that produced the documents. "
            "Retry count is 0-5, the platform's own bound."
        ),
        _ORDERING: (
            "Both paths terminate independently; nothing may follow a try_catch.",
            "Retry over an action classified non_idempotent or unverified is "
            "refused outright, whatever evidence is attached.",
            "Typed idempotency evidence only discharges the obligation on an "
            "action already classified idempotent_write or conditionally_idempotent.",
            "Try/Catch does not nest: composition would rewrite the outer "
            "handler's effective error selection.",
        ),
        _CAPS: (
            "scoped_try_catch",
            "bounded_retry",
            "typed_idempotency_evidence",
            "nested_try_catch",
            "catch_error_type_lists",
            "retry_backoff_authoring",
            "catch_failure_trigger_selection",
            "verified_write_replay_safety",
        ),
        _RELATED: (
            "placement.try_body.step",
            "placement.try_body.terminal",
            "placement.catch_body.step",
            "placement.catch_body.terminal",
            "semantic_rule.retry.replay_safety",
        ),
        _STAGES: ("author", "plan", "repair"),
    },
    "exception": {
        "category": "terminal",
        "title": "Exception",
        "summary": (
            "Terminal throw. The message template must carry the {1} placeholder "
            "whenever parameter_source binds a value."
        ),
        _ORDERING: ("An exception terminates its path; nothing may follow it.",),
        _DOCS: ("required", "none", "per_document"),
        _STAGES: ("author",),
    },
    "stop": {
        "category": "terminal",
        "title": "Stop",
        "summary": "Plain successful terminal. Ends the path and returns nothing.",
        _ORDERING: (
            "A stop terminates its path; nothing may follow it.",
            "A Branch path or Decision true arm ending in a stop must do some work "
            "first; a Decision false arm may stop with no steps at all.",
        ),
        _DOCS: ("optional", "none", "per_document"),
        _STAGES: ("author",),
    },
    "return_documents": {
        "category": "terminal",
        "title": "Return documents",
        "summary": (
            "Terminal that hands the current batch back to the caller. A plain "
            "stop ends the path without returning anything."
        ),
        _ORDERING: (
            "Returns and terminates; nothing may follow it.",
            "Admitted in the root sequence only — not in a Branch path, a Decision "
            "arm, or a Try/Catch body.",
        ),
        _DOCS: ("required", "documents", "all_documents"),
        _STAGES: ("author",),
    },
}


# ---------------------------------------------------------------------------
# Parity-pinned cross-cutting semantic rules
# ---------------------------------------------------------------------------

_SEMANTIC_RULES: Tuple[Tuple[str, str, str, str, Tuple[str, ...], Tuple[str, ...]], ...] = (
    (
        "semantic_rule.branch.path_order",
        "control",
        "Branch paths run in the authored order",
        "Paths are sequential, never concurrent, and per-execution state written "
        "in an earlier path is visible in a later one. Ordering is therefore part "
        "of the design, not a detail of it.",
        ("branch",),
        ("node.branch", "state_visibility.dpp", "capability.parallel_branch_execution"),
    ),
    (
        "semantic_rule.control.no_continuation",
        "control",
        "Control nodes terminalize their path",
        "Nothing may follow a Branch or a Decision, and their outlets are never "
        "rejoined. Work that must happen afterwards is authored inside every "
        "path or arm.",
        ("branch", "decision"),
        (
            "capability.continuation_after_branch_or_decision",
            "capability.joins",
            "node.branch",
            "node.decision",
        ),
    ),
    (
        "semantic_rule.control.depth_bound",
        "control",
        "Control nesting is bounded at two levels",
        "A bound of this compiler, NOT a Boomi platform limit. Deeper routing "
        "belongs in a subprocess reached by a process call.",
        ("branch", "decision"),
        ("node.branch", "node.decision", "node.process_call"),
    ),
    (
        "semantic_rule.state.convergence",
        "state",
        "Converging paths keep only what both established",
        "The meet over converging paths is intersection, not union: state written "
        "on one arm only is not established after the arms converge.",
        ("branch", "decision", "set_ddp", "set_dpp"),
        ("state_visibility.ddp", "state_visibility.dpp", "state_visibility.cache"),
    ),
    (
        "semantic_rule.retry.replay_safety",
        "reliability",
        "Replay safety is decided by the action, not by the caller",
        "Retry over an action classified non_idempotent or unverified is refused "
        "outright regardless of evidence. Typed evidence only discharges the "
        "extra obligation on an action already classified idempotent_write "
        "(verified_action) or conditionally_idempotent (key_reference resolving "
        "to the same operation).",
        ("try_catch", "connector_call"),
        ("node.try_catch", "node.connector_call"),
    ),
    (
        "semantic_rule.documents.explicit_split_combine",
        "transform",
        "Splitting and combining documents is always explicit",
        "Fan-out and fan-in are authored data_process operations. No control node "
        "and no flow control changes document cardinality implicitly.",
        ("data_process", "flow_control"),
        ("node.data_process", "node.flow_control"),
    ),
    (
        "semantic_rule.references.opaque",
        "structure",
        "Every component reference is opaque",
        "References are authored as '$ref:KEY' tokens or literal component ids. "
        "Connector families, actions, credentials, configuration and profiles are "
        "never authored alongside them — the compiler resolves them from the "
        "component plan.",
        (),
        ("node.connector_call", "node.map_ref", "node.process_call"),
    ),
)


# ---------------------------------------------------------------------------
# Entry construction
# ---------------------------------------------------------------------------


def _source(
    source_id: str,
    projection: str,
    provenance: str,
    revision_role: str,
    subject: str = "",
) -> ProcessIRAuthoringSourceV1:
    return ProcessIRAuthoringSourceV1(
        source_id=source_id,
        source_subject=subject,
        projection=projection,
        provenance=provenance,
        revision_role=revision_role,
    )


def _schema_ref_for(kind: str, schema_defs: Mapping[str, Any]) -> Tuple[str, ...]:
    """The ``$defs`` entry whose ``kind`` const is this kind.

    DERIVED from the served schema rather than a hand-kept kind-to-model map: a
    map would be a second vocabulary, and it would go stale silently the first
    time a model was renamed.
    """
    refs = []
    for name, body in schema_defs.items():
        const = ((body.get("properties") or {}).get("kind") or {}).get("const")
        if const == kind:
            refs.append(f"#/$defs/{name}")
    return tuple(sorted(refs))


def _node_entries(
    sources: ProjectionSourcesV1,
) -> List[ProcessIRAuthoringContractEntryV1]:
    placements_by_kind: Dict[str, List[ProcessIRAuthoringPlacementV1]] = {}
    for context, slot, kinds in sources.placement_rows:
        canonical, applicable = _canonical_state("body_placement_registry", "admitted")
        for kind in kinds:
            placements_by_kind.setdefault(kind, []).append(
                ProcessIRAuthoringPlacementV1(
                    context=context,
                    slot=slot,
                    canonical_state=canonical,
                    source_state="admitted",
                )
            )

    entries = []
    for kind in sources.node_kinds:
        facts = _NODE_FACTS.get(kind)
        if facts is None:
            # Fail LOUDLY rather than serving a node with no authoring facts. A
            # new node kind that nobody wrote facts for is exactly the coverage
            # gap the two-way parity test exists to catch, and a silent skip
            # would make the test pass while the caller learns nothing.
            raise KeyError(f"no authoring facts for node kind {kind!r}")
        docs = facts.get(_DOCS)
        entry_sources = [
            _source(SOURCE_MODELS, "generated", "runtime_model", "schema", kind),
            _source(
                SOURCE_MODELS, "parity_pinned", "live_capture_attested", "compiler", kind
            ),
        ]
        if kind in placements_by_kind:
            entry_sources.append(
                _source(
                    SOURCE_PLACEMENTS, "generated", "runtime_registry", "compiler", kind
                )
            )
        entries.append(
            ProcessIRAuthoringContractEntryV1(
                contract_entry_id=f"node.{kind}",
                entry_type="node",
                category=facts["category"],
                subject=kind,
                title=facts["title"],
                summary=facts["summary"],
                node_kinds=(kind,),
                workflow_stages=facts.get(_STAGES, ("author",)),
                schema_refs=_schema_ref_for(kind, sources.schema_defs),
                placements=tuple(placements_by_kind.get(kind, ())),
                document_semantics=(
                    ProcessIRDocumentSemanticsV1(
                        input_documents=docs[0],
                        output_documents=docs[1],
                        grouping=docs[2],
                    )
                    if docs
                    else None
                ),
                ordering_facts=facts.get(_ORDERING, ()),
                required_references=tuple(
                    ProcessIRAuthoringReferenceV1(field=field, required=required)
                    for field, required in facts.get(_REFS, ())
                ),
                related_entry_ids=tuple(facts.get(_RELATED, ()))
                + tuple(f"capability.{cap}" for cap in facts.get(_CAPS, ())),
                sources=tuple(entry_sources),
            )
        )
    return entries


def _capability_entries(
    sources: ProjectionSourcesV1,
) -> List[ProcessIRAuthoringContractEntryV1]:
    entries = []
    for name, state in sorted(sources.capability_rows.items()):
        canonical, applicable = _canonical_state("process_ir_capability_manifest", state)
        entries.append(
            ProcessIRAuthoringContractEntryV1(
                contract_entry_id=f"capability.{name}",
                entry_type="capability",
                category="capability",
                subject=name,
                title=name.replace("_", " ").strip().capitalize(),
                summary=(
                    "Published state for this construct. 'gated' means not yet "
                    "authorable through this contract; 'unsupported' means never "
                    "— a different design is required."
                ),
                capability_id=name,
                canonical_state=canonical,
                source_state=state,
                applicable=applicable,
                workflow_stages=("discover", "plan"),
                # 'merge' is an ALIAS of joins, not a row. One construct with two
                # rows is one construct whose two states drift apart.
                display_aliases=("merge",) if name == "joins" else (),
                sources=(
                    _source(
                        SOURCE_CAPABILITIES,
                        "generated",
                        "runtime_model",
                        "compiler",
                        name,
                    ),
                ),
            )
        )
    return entries


def _placement_entries(
    sources: ProjectionSourcesV1,
) -> List[ProcessIRAuthoringContractEntryV1]:
    canonical, _ = _canonical_state("body_placement_registry", "admitted")
    entries = []
    for context, slot, kinds in sources.placement_rows:
        entries.append(
            ProcessIRAuthoringContractEntryV1(
                contract_entry_id=f"placement.{context}.{slot}",
                entry_type="placement",
                category="placement",
                subject=f"{context}.{slot}",
                title=f"{context.replace('_', ' ')} {slot} slot",
                summary=(
                    "The node kinds this control-body slot admits. The registry is "
                    "a closed allowlist: a kind absent from this list is rejected, "
                    "and absence is the rule rather than an omission."
                ),
                canonical_state=canonical,
                source_state="admitted",
                node_kinds=kinds,
                workflow_stages=("author", "repair"),
                placements=(
                    ProcessIRAuthoringPlacementV1(
                        context=context,
                        slot=slot,
                        canonical_state=canonical,
                        source_state="admitted",
                    ),
                ),
                diagnostic_codes=("PROCESS_IR_CAPABILITY_NODE_NOT_ALLOWED_IN_BODY",),
                related_entry_ids=tuple(f"node.{kind}" for kind in kinds),
                sources=(
                    _source(
                        SOURCE_PLACEMENTS,
                        "generated",
                        "runtime_registry",
                        "compiler",
                        f"{context}.{slot}",
                    ),
                ),
            )
        )
    return entries


_FAMILY_TOKENS: Mapping[str, str] = {
    "officialboomi-X3979C-rest-prod": "rest",
    "wssoapclientsdk": "soap",
    "database": "database",
}


def _family_token(family: str) -> str:
    """A lowercase id token for a family whose real name has dashes and case.

    The authoritative family string stays in ``subject``; only the ID is
    tokenized, because an id appears in remediations and citations and must fit
    the dotted lowercase grammar.
    """
    token = _FAMILY_TOKENS.get(family)
    if token:
        return token
    return "".join(ch if ch.isalnum() else "_" for ch in family.lower())


def _connector_entries(
    sources: ProjectionSourcesV1,
) -> List[ProcessIRAuthoringContractEntryV1]:
    canonical, _ = _canonical_state("connector_capability_registry", "listed")
    retry_by_class = {
        row["replay_classification"]: row for row in sources.retry_rules
    }
    entries = []
    for row in sources.connector_rows:
        family = str(row["family"])
        action = str(row["action"])
        classification = str(row["replay_classification"])
        rule = retry_by_class.get(classification, {})
        input_documents = (
            "optional" if row["input_documents"] == "none_or_documents" else "required"
        )
        entries.append(
            ProcessIRAuthoringContractEntryV1(
                contract_entry_id=(
                    f"connector_action.{_family_token(family)}.{action.lower()}"
                ),
                entry_type="connector_action",
                category="connector_action",
                subject=f"{family} {action}",
                title=f"{_family_token(family).upper()} {action}",
                summary=(
                    "A callable connector family/action pair. The registry is a "
                    "closed allowlist: a pair absent from it is rejected."
                ),
                canonical_state=canonical,
                source_state="listed",
                node_kinds=("connector_call", "source", "target"),
                workflow_stages=("author", "plan", "repair"),
                document_semantics=ProcessIRDocumentSemanticsV1(
                    input_documents=input_documents,
                    output_documents="documents" if row["output_documents"] else "none",
                    grouping="per_document",
                ),
                ordering_facts=(
                    "Side effect: {0}. Replay classification: {1}.".format(
                        row["side_effect"], classification
                    ),
                    (
                        "May sit inside a retried region."
                        if rule.get("retry_permitted")
                        else "May NOT sit inside a retried region: no evidence a "
                        "caller attaches can authorise a replay."
                    ),
                    (
                        "Required idempotency evidence: {0}.".format(
                            rule["required_evidence"]
                        )
                        if rule.get("required_evidence")
                        else "No idempotency evidence is required."
                    ),
                    (
                        "Produces no documents, so nothing may consume its output."
                        if not row["output_documents"]
                        else "Produces documents that downstream steps may consume."
                    ),
                ),
                diagnostic_codes=(
                    "PROCESS_IR_CAPABILITY_CONNECTOR_ACTION_UNSUPPORTED",
                ),
                related_entry_ids=(
                    "node.connector_call",
                    "semantic_rule.retry.replay_safety",
                ),
                sources=(
                    _source(
                        SOURCE_CONNECTORS,
                        "generated",
                        "runtime_registry",
                        "compiler",
                        f"{family} {action}",
                    ),
                    _source(
                        SOURCE_RETRY,
                        "generated",
                        "runtime_registry",
                        "compiler",
                        classification,
                    ),
                ),
            )
        )
    return entries


#: Substitutions applied when a diagnostic CODE is lowercased into a contract id.
#:
#: The same rule as ``PUBLIC_BODY_CONTEXTS`` and ``PUBLIC_CAPABILITY_FIELDS``, one
#: level down. A stable diagnostic code is public and reaches callers verbatim in
#: ``cause_codes``, so ``subject`` carries it EXACTLY. The contract id, though, is
#: an artifact of this contract, and lowercasing
#: ``PROCESS_IR_COMPILE_EMITTER_INPUT_INVALID`` yields a string containing a
#: reserved compiler identifier the served surface may not carry
#: (``tests/test_process_ir_compiler_surface.py::FORBIDDEN_NAMES``).
#:
#: Substituting in the ID rather than dropping the entry keeps coverage TOTAL:
#: every code a caller can receive has an entry to cite, including the ones that
#: turn out to be compiler defects. A pinned test asserts the map is applied
#: exactly where it is needed and nowhere else.
_PUBLIC_CODE_TOKENS: Mapping[str, str] = {"emitter_input": "emission_input"}


def _public_code_token(code: str) -> str:
    token = code.lower()
    for internal, public in _PUBLIC_CODE_TOKENS.items():
        token = token.replace(internal, public)
    return token


def _diagnostic_entries(
    sources: ProjectionSourcesV1,
) -> List[ProcessIRAuthoringContractEntryV1]:
    """One entry per diagnostic code, merged across the three producers.

    A code can be raised by more than one layer (a parse rejection and a
    compiler rejection can share a code), so the three spec tables are merged by
    code and every contributing source is recorded. Serving three near-identical
    entries under one code would make a citation ambiguous.
    """
    merged: Dict[str, Dict[str, Any]] = {}
    for specs, source_id in (
        (sources.parse_specs, SOURCE_PARSE_DIAGNOSTICS),
        (sources.finding_specs, SOURCE_VALIDATION_DIAGNOSTICS),
        (sources.compiler_specs, SOURCE_COMPILER_DIAGNOSTICS),
    ):
        for spec in specs:
            code = str(spec["code"])
            row = merged.setdefault(
                code, {"message": "", "remediation": "", "sources": []}
            )
            if spec.get("message") and not row["message"]:
                row["message"] = spec["message"]
            if spec.get("remediation") and not row["remediation"]:
                row["remediation"] = spec["remediation"]
            row["sources"].append(source_id)

    entries = []
    for code in sorted(merged):
        row = merged[code]
        entries.append(
            ProcessIRAuthoringContractEntryV1(
                contract_entry_id=f"diagnostic.{_public_code_token(code)}",
                entry_type="diagnostic",
                category="diagnostic",
                subject=code,
                title=code,
                summary=(row["message"] or "").strip(),
                workflow_stages=("repair",),
                ordering_facts=(
                    (row["remediation"],) if row["remediation"] else ()
                ),
                diagnostic_codes=(code,),
                sources=tuple(
                    _source(source_id, "generated", "runtime_registry", "compiler", code)
                    for source_id in sorted(set(row["sources"]))
                ),
            )
        )
    return entries


def _state_visibility_entries(
    sources: ProjectionSourcesV1,
) -> List[ProcessIRAuthoringContractEntryV1]:
    rows = list(sources.state_visibility) + [dict(sources.process_property_scope)]
    entries = []
    for row in sorted(rows, key=lambda item: str(item["state_scope"])):
        scope = str(row["state_scope"])
        is_process_property = scope == "processproperty"
        entries.append(
            ProcessIRAuthoringContractEntryV1(
                contract_entry_id=f"state_visibility.{scope}",
                entry_type="state_visibility",
                category="state",
                subject=scope,
                title=f"{scope} visibility",
                summary=(
                    "Where a value written under this scope is visible, and what "
                    "happens to it where paths diverge and converge."
                ),
                workflow_stages=("author", "repair"),
                ordering_facts=(
                    "Scope: {0}.".format(row["scope"]),
                    "Visible across sibling Branch paths: {0}.".format(
                        "yes" if row["visible_across_sibling_paths"] else "no"
                    ),
                    "Where paths converge: {0}.".format(row["convergence"]),
                    "Read with no establishing write: {0}.".format(
                        row["read_before_write"]
                    ),
                ),
                related_entry_ids=(
                    ("node.set_ddp",)
                    if scope == "ddp"
                    else ("node.set_dpp",)
                    if scope == "dpp"
                    else ("node.cache_get", "node.cache_put")
                    if scope == "cache"
                    else ()
                ),
                sources=(
                    _source(
                        SOURCE_PROCESS_PROPERTY
                        if is_process_property
                        else SOURCE_STATE,
                        "generated",
                        "runtime_model" if is_process_property else "runtime_registry",
                        "compiler",
                        scope,
                    ),
                ),
            )
        )
    return entries


def _semantic_rule_entries(
    sources: ProjectionSourcesV1,
) -> List[ProcessIRAuthoringContractEntryV1]:
    return [
        ProcessIRAuthoringContractEntryV1(
            contract_entry_id=entry_id,
            entry_type="semantic_rule",
            category=category,
            subject=entry_id.split(".", 1)[1],
            title=title,
            summary=summary,
            node_kinds=node_kinds,
            workflow_stages=("author", "plan", "repair"),
            related_entry_ids=related,
            sources=(
                _source(
                    SOURCE_MODELS,
                    "parity_pinned",
                    "live_capture_attested",
                    "compiler",
                    entry_id,
                ),
            ),
        )
        for entry_id, category, title, summary, node_kinds, related in _SEMANTIC_RULES
    ]


def _doctrine_entries(
    sources: ProjectionSourcesV1,
) -> List[ProcessIRAuthoringContractEntryV1]:
    """LINKS to doctrine, never copies of it.

    Doctrine prose already has a served home
    (``get_schema_template(schema_name='design_pattern:<name>')``). Copying it
    here would put the same words behind two selectors that can disagree; the
    entry carries the state and the selector, and the caller fetches the prose
    from its owner.
    """
    entries = []
    for row in sources.doctrine_rows:
        name = str(row["name"])
        status = str(row.get("capability_status") or "")
        canonical, applicable = _canonical_state("design_doctrine", status)
        entries.append(
            ProcessIRAuthoringContractEntryV1(
                contract_entry_id=f"doctrine.{name}",
                entry_type="doctrine",
                category="doctrine",
                subject=name,
                title=name.replace("_", " ").strip().capitalize(),
                summary=(
                    "A design pattern. Fetch its full text from the doctrine "
                    "selector; this entry publishes only its state and identity."
                ),
                canonical_state=canonical,
                source_state=status,
                applicable=applicable,
                workflow_stages=("discover", "plan"),
                doctrine_selector=f"design_pattern:{name}",
                sources=(
                    _source(
                        SOURCE_DOCTRINE, "generated", "runtime_registry", "capability", name
                    ),
                ),
            )
        )
    return entries


def _recipe_entries(
    sources: ProjectionSourcesV1,
) -> List[ProcessIRAuthoringContractEntryV1]:
    entries = []
    for row in sources.recipe_entries:
        recipe_id = str(row["recipe_id"])
        version = str(row.get("recipe_version") or "")
        token = recipe_id.replace(".", "_").replace("-", "_").lower()
        entries.append(
            ProcessIRAuthoringContractEntryV1(
                contract_entry_id=f"recipe.{token}",
                entry_type="recipe",
                category="recipe",
                subject=recipe_id,
                title=recipe_id,
                summary=(
                    "A registered recipe. Membership and revisions come from the "
                    "recipe registry; this entry never restates them."
                ),
                workflow_stages=("discover", "plan"),
                recipe_selector=f"recipe_registry:{recipe_id}@{version}",
                ordering_facts=(
                    "Entry kind: {0}.".format(row.get("entry_kind") or "unknown"),
                ),
                sources=(
                    _source(
                        SOURCE_RECIPES,
                        "generated",
                        "runtime_registry",
                        "capability",
                        recipe_id,
                    ),
                ),
            )
        )
    for kind in sources.contribution_kinds:
        entries.append(
            ProcessIRAuthoringContractEntryV1(
                contract_entry_id=f"recipe_contribution.{kind}",
                entry_type="recipe_contribution",
                category="recipe",
                subject=kind,
                title=kind.replace("_", " ").strip().capitalize(),
                summary=(
                    "One of the closed set of values a registered recipe may "
                    "return. Recipe output is validated exactly as direct "
                    "authoring and cannot be exempted."
                ),
                workflow_stages=("discover", "plan"),
                sources=(
                    _source(
                        SOURCE_CONTRIBUTIONS,
                        "generated",
                        "runtime_model",
                        "schema",
                        kind,
                    ),
                ),
            )
        )
    return entries


_BUILDERS = (
    _node_entries,
    _capability_entries,
    _placement_entries,
    _connector_entries,
    _diagnostic_entries,
    _state_visibility_entries,
    _semantic_rule_entries,
    _doctrine_entries,
    _recipe_entries,
)

_CACHE: Dict[str, Any] = {}


def build_process_ir_authoring_entries(
    sources: Optional[ProjectionSourcesV1] = None,
) -> Tuple[ProcessIRAuthoringContractEntryV1, ...]:
    """Every contract entry, sorted by id.

    An injected ``sources`` BYPASSES the cache entirely. That is what makes a
    drift negative control safe: perturbing the production cache would leak into
    every later test in the process and turn a passing suite into a lie.
    """
    if sources is not None:
        return _build(sources)
    cached = _CACHE.get("entries")
    if cached is None:
        cached = _build(collect_projection_sources())
        _CACHE["entries"] = cached
    return cached


def _build(
    sources: ProjectionSourcesV1,
) -> Tuple[ProcessIRAuthoringContractEntryV1, ...]:
    entries: List[ProcessIRAuthoringContractEntryV1] = []
    for builder in _BUILDERS:
        entries.extend(builder(sources))
    entries.sort(key=lambda entry: entry.contract_entry_id)
    seen = set()
    for entry in entries:
        if entry.contract_entry_id in seen:
            # A duplicate id makes a citation ambiguous, which defeats the whole
            # point of the id being the citation handle.
            raise ValueError(f"duplicate contract entry id {entry.contract_entry_id!r}")
        seen.add(entry.contract_entry_id)
    return tuple(entries)


def reset_process_ir_authoring_cache() -> None:
    """Drop the memoized projection. For tests that perturb a live registry."""
    _CACHE.clear()


# ---------------------------------------------------------------------------
# Retrieval
# ---------------------------------------------------------------------------


def _facets(
    entries: Tuple[ProcessIRAuthoringContractEntryV1, ...]
) -> ProcessIRAuthoringFacetsV1:
    return ProcessIRAuthoringFacetsV1(
        entry_types=tuple(entry.entry_type for entry in entries),
        categories=tuple(entry.category for entry in entries),
        node_kinds=tuple(kind for entry in entries for kind in entry.node_kinds),
        capability_ids=tuple(
            entry.capability_id for entry in entries if entry.capability_id
        ),
        workflow_stages=tuple(
            stage for entry in entries for stage in entry.workflow_stages
        ),
    )


def _validated_limit(limit: Optional[int]) -> int:
    if limit is None:
        return PROCESS_IR_AUTHORING_DEFAULT_LIMIT
    if not isinstance(limit, int) or isinstance(limit, bool):
        raise ProcessIRAuthoringQueryError("limit")
    if limit < 1 or limit > PROCESS_IR_AUTHORING_MAX_LIMIT:
        raise ProcessIRAuthoringQueryError("limit")
    return limit


def query_process_ir_authoring_contract(
    *,
    authoring_entry_id: Optional[str] = None,
    node_kind: Optional[str] = None,
    category: Optional[str] = None,
    capability_id: Optional[str] = None,
    workflow_stage: Optional[str] = None,
    after_entry_id: Optional[str] = None,
    limit: Optional[int] = None,
    sources: Optional[ProjectionSourcesV1] = None,
) -> ProcessIRAuthoringContractPageV1:
    """One bounded page of the contract.

    Semantic filters AND together. With none supplied the page carries the
    facets and ZERO entries — dumping the whole catalog into every response is
    the payload behaviour the issue rules out, and a caller who does not yet
    know what to ask for needs the facets, not 150 entries.

    An unknown ENUMERATED value (a node kind, a category, a stage) is an error
    listing the facet. An unknown EXACT ``authoring_entry_id`` is a SUCCESS with
    zero entries, and the difference is deliberate: the clean-room harness
    resolves citations by exact id, and a dangling citation has to be
    observable as "no such entry" rather than indistinguishable from a
    malformed request.
    """
    entries = build_process_ir_authoring_entries(sources)
    facets = _facets(entries)
    effective_limit = _validated_limit(limit)

    semantic = (authoring_entry_id, node_kind, category, capability_id, workflow_stage)
    if after_entry_id and not any(semantic):
        # A cursor with no filter would page the entire catalog one screen at a
        # time, which is the same unbounded dump wearing a different hat.
        raise ProcessIRAuthoringQueryError("after_entry_id")

    if node_kind and node_kind not in facets.node_kinds:
        raise ProcessIRAuthoringQueryError("node_kind", facets.node_kinds)
    if category and category not in facets.categories:
        raise ProcessIRAuthoringQueryError("category", facets.categories)
    if capability_id and capability_id not in facets.capability_ids:
        raise ProcessIRAuthoringQueryError("capability_id", facets.capability_ids)
    if workflow_stage and workflow_stage not in facets.workflow_stages:
        raise ProcessIRAuthoringQueryError("workflow_stage", facets.workflow_stages)

    query = ProcessIRAuthoringQueryV1(
        authoring_entry_id=authoring_entry_id,
        node_kind=node_kind,
        category=category,
        capability_id=capability_id,
        workflow_stage=workflow_stage,
        after_entry_id=after_entry_id,
        limit=effective_limit,
    )

    if not any(semantic):
        return ProcessIRAuthoringContractPageV1(
            state_mappings=state_mappings(),
            query=query,
            catalog_entry_count=len(entries),
            matched_entry_count=0,
            returned_entry_count=0,
            limit=effective_limit,
            facets=facets,
            entries=(),
        )

    matched = [
        entry
        for entry in entries
        if (not authoring_entry_id or entry.contract_entry_id == authoring_entry_id)
        and (not node_kind or node_kind in entry.node_kinds)
        and (not category or entry.category == category)
        and (not capability_id or entry.capability_id == capability_id)
        and (not workflow_stage or workflow_stage in entry.workflow_stages)
    ]

    page = [entry for entry in matched if not after_entry_id or entry.contract_entry_id > after_entry_id]

    selected: List[ProcessIRAuthoringContractEntryV1] = []
    spent = 0
    truncated = False
    for entry in page:
        if len(selected) >= effective_limit:
            truncated = True
            break
        cost = len(canonical_entry_json(entry))
        # Stop BEFORE the entry that would exceed the budget, and never on the
        # first one: a single oversized entry that could never be returned would
        # otherwise make its own id unreachable forever.
        if selected and spent + cost > PROCESS_IR_AUTHORING_BYTE_BUDGET:
            truncated = True
            break
        selected.append(entry)
        spent += cost

    return ProcessIRAuthoringContractPageV1(
        state_mappings=state_mappings(),
        query=query,
        catalog_entry_count=len(entries),
        matched_entry_count=len(matched),
        returned_entry_count=len(selected),
        limit=effective_limit,
        truncated=truncated,
        next_after_entry_id=(
            selected[-1].contract_entry_id if truncated and selected else None
        ),
        facets=facets,
        entries=tuple(selected),
    )


def build_process_ir_authoring_index() -> Dict[str, Any]:
    """The COMPACT index ``list_capabilities`` publishes.

    Counts, facets, mappings and limits — never the entries themselves.
    ``list_capabilities`` is the discovery call every client makes first, and
    embedding a 150-entry catalog in it would make discovery the most expensive
    call in the workflow.
    """
    entries = build_process_ir_authoring_entries()
    facets = _facets(entries)
    counts: Dict[str, int] = {}
    for entry in entries:
        counts[entry.entry_type] = counts.get(entry.entry_type, 0) + 1
    return {
        "selector": "process_ir_authoring",
        "schema_version": PROCESS_IR_AUTHORING_CONTRACT_VERSION,
        "entry_count": len(entries),
        "entry_counts_by_type": dict(sorted(counts.items())),
        "entry_types": list(PROCESS_IR_AUTHORING_ENTRY_TYPES),
        "workflow_stages": list(PROCESS_IR_AUTHORING_WORKFLOW_STAGES),
        "facets": {
            "categories": list(facets.categories),
            "node_kinds": list(facets.node_kinds),
            "capability_ids": list(facets.capability_ids),
            "workflow_stages": list(facets.workflow_stages),
        },
        "state_mappings": [
            mapping.model_dump(mode="json") for mapping in state_mappings()
        ],
        "unlisted_placement_state": "unsupported",
        "unlisted_connector_action_state": "unsupported",
        "retrieval": {
            "filters": [
                "authoring_entry_id",
                "node_kind",
                "category",
                "capability_id",
                "workflow_stage",
                "after_entry_id",
                "limit",
            ],
            "default_limit": PROCESS_IR_AUTHORING_DEFAULT_LIMIT,
            "max_limit": PROCESS_IR_AUTHORING_MAX_LIMIT,
            "byte_budget": PROCESS_IR_AUTHORING_BYTE_BUDGET,
            "bare_retrieval_returns_entries": False,
        },
    }


def authoring_contract_entry_ids_for_diagnostic(code: str) -> Tuple[str, ...]:
    """Every entry that names ``code``, sorted.

    This is what turns a diagnostic into a repair: the response carries the ids,
    and every one of them resolves through the public selector.
    """
    if not code:
        return ()
    return tuple(
        sorted(
            entry.contract_entry_id
            for entry in build_process_ir_authoring_entries()
            if code in entry.diagnostic_codes
        )
    )


def process_ir_authoring_revision_payload() -> Dict[str, Any]:
    """The normalized projection, for the capability/compiler revision inputs.

    Deliberately the WHOLE entry set rather than a summary: a revision that
    covers only the counts would not move when an entry's remediation or state
    changed, which is precisely the change a bound caller must be told about.
    """
    return {
        "contract_version": PROCESS_IR_AUTHORING_CONTRACT_VERSION,
        "entries": [
            entry.model_dump(mode="json")
            for entry in build_process_ir_authoring_entries()
        ],
        "state_mappings": [
            mapping.model_dump(mode="json") for mapping in state_mappings()
        ],
    }


def validate_process_ir_authoring_projection(
    entries: Optional[Tuple[ProcessIRAuthoringContractEntryV1, ...]] = None,
) -> Tuple[str, ...]:
    """Structural self-check: returns the problems found, empty when clean.

    Used by the parity suite and by the drift negative controls. Checks the
    invariants that make a citation trustworthy — every related id resolves,
    every entry names a source — rather than re-deriving the facts, which the
    parity tests do against the runtime sources directly.
    """
    rows = entries if entries is not None else build_process_ir_authoring_entries()
    known = {entry.contract_entry_id for entry in rows}
    problems: List[str] = []
    for entry in rows:
        if not entry.sources:
            problems.append(f"{entry.contract_entry_id}: no source")
        for related in entry.related_entry_ids:
            if related not in known:
                problems.append(
                    f"{entry.contract_entry_id}: dangling related id {related}"
                )
        if entry.entry_type not in PROCESS_IR_AUTHORING_ENTRY_TYPES:
            problems.append(f"{entry.contract_entry_id}: unknown entry type")
        for stage in entry.workflow_stages:
            if stage not in PROCESS_IR_AUTHORING_WORKFLOW_STAGES:
                problems.append(f"{entry.contract_entry_id}: unknown stage {stage}")
    return tuple(sorted(problems))


__all__ = [
    "ProcessIRAuthoringQueryError",
    "ProjectionSourcesV1",
    "authoring_contract_entry_ids_for_diagnostic",
    "build_process_ir_authoring_entries",
    "build_process_ir_authoring_index",
    "collect_projection_sources",
    "process_ir_authoring_revision_payload",
    "query_process_ir_authoring_contract",
    "reset_process_ir_authoring_cache",
    "state_mappings",
    "validate_process_ir_authoring_projection",
]

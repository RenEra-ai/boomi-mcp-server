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

import json
from types import MappingProxyType
from typing import (
    Any,
    Dict,
    List,
    Mapping,
    MutableMapping,
    NamedTuple,
    Optional,
    Tuple,
)

from pydantic import ValidationError as PydanticValidationError

from ..models.process_ir_authoring import (
    DIAGNOSTIC_LABEL_LEGEND,
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
SOURCE_MAP_FUNCTIONS = "runtime.map_function_registry"
SOURCE_VETTED_SCRIPTS = "runtime.vetted_script_registry"
SOURCE_EFFECT_RESOLVER = "runtime.process_ir_effect_resolver"


class ProcessIRAuthoringQueryError(ValueError):
    """An invalid retrieval filter, reported with the values that ARE valid.

    Carries the offending field and its facet rather than a message, so the
    serving layer renders one bounded error envelope and a caller is told what
    to ask for instead of only that they asked wrongly.
    """

    def __init__(
        self, field: str, allowed: Tuple[str, ...] = (), rule: str = ""
    ) -> None:
        super().__init__(field)
        self.field = field
        self.allowed = tuple(allowed)
        # ``rule`` is for the filters that are NOT enumerations. ``limit`` has a
        # numeric range and ``after_entry_id`` has a companion requirement, so
        # rendering them through the enum template gave an empty allowed-values
        # list and advice ("filter with a published value") that is actively
        # wrong: the rejected cursor IS a published id, so a caller following it
        # loops. A filter that cannot be explained by a facet explains itself.
        self.rule = rule


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
    from ..kb.design_doctrine import design_doctrine_capability_rows
    from ..models.cache_property_models import PROCESS_PROPERTY_SCOPE_V1
    from ..models.process_ir import (
        PROCESS_IR_V1_CAPABILITIES,
        process_ir_v1_json_schema,
        process_ir_v1_node_kinds,
        process_ir_v1_parse_diagnostic_specs,
    )
    from ..models.recipe_contributions import RECIPE_CONTRIBUTION_KINDS
    from ..recipes import production_registry

    # NOT swallowed. Every other source here is unguarded and a failure
    # degrades honestly one layer up, where `contract.py` reports the selector
    # `unavailable`. Recipes alone converted an exception to an empty tuple, so
    # a dead registry served 171 entries with ZERO recipe links, `_success:
    # true`, `truncated: false` and nothing saying the contract was incomplete
    # — a caller could not tell "this construct links no recipe" from "the
    # registry that knows died". Reporting less than the contract claims is
    # worse than reporting that it is unavailable.
    recipe_entries = tuple(production_registry().snapshot().get("entries", ()))

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
        # Through the module's own accessor, which deepcopies and hands back
        # only the capability fields. Reading the raw catalog and shallow-
        # copying each entry left `cross_refs`/`mutual_exclusion` aliased to
        # module state.
        doctrine_rows=tuple(design_doctrine_capability_rows()),
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
    # The owning surface's OWN spellings. `gated`/`reserved` were shorthand for
    # states that surface never emits, so two of the three mapping rows could
    # never fire — a published mapping from a vocabulary that does not exist.
    ("cache_property_authoring", "executable", "supported", True),
    ("cache_property_authoring", "gated_no_verified_wire_shape", "gated", True),
    ("cache_property_authoring", "reserved_not_executable", "unsupported", True),
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
        _ORDERING: (
            "A source occupies the first position of the sequence it opens.",
            "It takes no inbound documents. What it RETURNS is decided by its "
            "family/action row, not by the node.",
        ),
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
            "On the legacy linear path a target is immediately followed by its "
            "terminal — a stop, or a return_documents when the flow hands its "
            "documents back to a caller.",
            "A target is a legal terminal of a Branch path and of a Decision true arm; "
            "it is not admitted on a Decision false arm.",
            "It consumes inbound documents. Whether it RETURNS any is decided by "
            "its family/action row — a database Send returns nothing, a REST "
            "PATCH does.",
        ),
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
        # No generic document semantics ON PURPOSE. What a connector call takes
        # and returns is decided by its family/action row, and the rows disagree:
        # a database Send returns nothing at all. Publishing one blanket
        # "produces documents" on the node entry let a caller place a downstream
        # consumer after a terminal call — the exact error the per-action rows
        # exist to prevent. The related entries below carry the real answer.
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
        # The POSITION-INDEPENDENT half only. The trailing-step rule differs by
        # body and is DERIVED into an ordering fact
        # (`_derived_trailing_cache_put_fact`); stating it here as well produced a
        # served entry that contradicted its own ordering fact for the bodies
        # #154 widened. A summary that restates a rule it does not own is the same
        # duplicate-authority defect in prose form.
        "summary": (
            "Writes the stream into a document cache and CONSUMES it. A cache_put "
            "in a MID-LIST step position must be followed immediately by a "
            "stream-replacing cache read; as the last step the rule depends on "
            "the body's terminal — see this entry's ordering facts."
        ),
        _ORDERING: (),
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
            "All-document cache read. external_writer ASSERTS that the cache is "
            "populated outside this process; on its own it establishes nothing, "
            "because nothing in the artifact can confirm an outside writer. "
            "Combined with a verified external-writer declaration it downgrades "
            "the missing-writer error to a named warning, so the assumption stays "
            "visible in the record rather than passing silently."
        ),
        _ORDERING: (
            "A cache read with no preceding write on the same path is reported "
            "unless the node authors external_writer AND a verified capability "
            "vouches for that writer. The declaration alone never suppresses "
            "it — both factors are required.",
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
            "Per-document state is not visible across sibling paths: each "
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
            "Invokes another process by opaque reference, and ENDS the path it "
            "is on: whether execution continues past a call is decided by the "
            "called process, which hands control back only through the "
            "return-document steps it declares, so this version supports the "
            "non-returning form only. wait=false returns immediately and "
            "therefore establishes no downstream state."
        ),
        _ORDERING: (
            "A process call may not share a root-to-leaf path with a connector "
            "call while that combination is capability-gated; sibling paths are "
            "independent and do not count as sharing.",
            "Nothing may follow a process call. Author it as the terminal of its "
            "path, with no steps before it in that body and no stop after it; a "
            "root sequence containing a call holds that call and nothing else.",
            "To run several children, give each its own path — separate branch "
            "paths, or separate wrappers — rather than chaining calls.",
        ),
        _DOCS: ("required", "documents", "per_document"),
        _REFS: (("process_ref", True),),
        _CAPS: ("process_call_connector_mixing", "terminal_process_call",
                "process_call_return_path_binding"),
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
        ),
        _DOCS: ("required", "documents", "all_documents"),
        _STAGES: ("author",),
    },
}


# ---------------------------------------------------------------------------
# Parity-pinned cross-cutting semantic rules
# ---------------------------------------------------------------------------

def _subprocess_inert_sentence() -> str:
    """The inert-case sentence, composed from the resolver's own reason rows.

    Hand-listing them went stale twice — once when opacity stopped meaning "a
    bare reference", once when the depth bound became a refusal — because the
    list lived only in prose. Composing it means a new reason reaches the
    served contract with the branch that creates it.
    """
    from .process_ir_effects import subprocess_inert_reasons

    reasons = [wording for _token, wording in subprocess_inert_reasons()]
    return (
        "A child is inert — establishing nothing either way — when "
        + "; or when ".join(reasons)
        + "."
    )


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
    (
        "semantic_rule.effect.declaration_boundary",
        "state",
        "An effect declaration supplies identity, never effect content",
        "A declared map, script or subprocess effect is checked for IDENTITY — "
        "the reference must resolve to a component of the right type, and a "
        "script digest is RECOMPUTED from the resolved source rather than taken "
        "from the declaration. What the effect IS always comes from a "
        "server-side authority. A declaration that disagrees with the derived "
        "effect is rejected; one the server cannot corroborate is inert.",
        (),
        ("semantic_rule.effect.map_inspection",
         "semantic_rule.effect.script_registry",
         "semantic_rule.effect.subprocess_inspection",
         "semantic_rule.effect.external_writer"),
    ),
    (
        "semantic_rule.effect.map_inspection",
        "state",
        "Map effects are derived by inspecting the map",
        "The reads and writes attributed to a map come from its own function "
        "mappings, resolved through the map-function registry, together with "
        "its document-cache joins. A map containing any function whose effect "
        "is unannotated, or any join whose read cannot be represented, is "
        "wholly opaque: partial knowledge is never reported as a complete "
        "effect.",
        ("map_ref",),
        ("node.map_ref", "semantic_rule.effect.declaration_boundary"),
    ),
    (
        "semantic_rule.effect.script_registry",
        "state",
        "Script effects come only from the vetted registry",
        "Nothing in an authored artifact establishes what an arbitrary script "
        "does, so a script effect is admitted only when the recomputed "
        "(language, digest) matches a server-owned vetted contract. A script "
        "whose digest matches no entry is inert — the server knows which script "
        "it is and still has no authority for what it does, so every strict "
        "finding stands.",
        ("data_process",),
        ("node.data_process", "semantic_rule.effect.declaration_boundary"),
    ),
    (
        "semantic_rule.effect.subprocess_inspection",
        "state",
        "Subprocess effects are derived from the child's own definition",
        "A called child's summary is derived by inspecting its authored process "
        "definition, walking every path. A read the child does not itself "
        "establish first is required of the caller, wherever in the child it "
        "sits. A write counts as established only where every path that "
        "completes makes it, which differs by SCOPE at a Branch: process and "
        "cache state accumulate, because every leg runs, so a write in one leg "
        "holds afterwards; document properties do not, because each leg "
        "re-copies the documents, so a write in one leg is absent from the "
        "copies another leg routed out. A Decision's arms are exclusive, so a "
        "write in one arm alone is never established. "
        + _subprocess_inert_sentence() +
        " A connector is not such a step: it moves documents "
        "rather than tracked state, so it leaves both sets unchanged and instead "
        "makes the child replay-unsafe.",
        ("process_call",),
        ("node.process_call", "semantic_rule.effect.declaration_boundary"),
    ),
    (
        "semantic_rule.effect.external_writer",
        "state",
        "An external writer is an assumption, never established state",
        "A cache populated outside this process cannot be observed from the "
        "artifact, so an external-writer declaration never establishes a cache "
        "write. Combined with a cache_get that authors external_writer, it "
        "converts the blocking missing-writer error into a named warning, so the "
        "assumption stays visible instead of passing silently. "
        "document_cache_retrieve carries no such flag: cache_get is the "
        "canonical spelling for a cache this process does not write.",
        ("cache_get", "document_cache_retrieve"),
        ("node.cache_get", "node.document_cache_retrieve",
         "semantic_rule.effect.declaration_boundary"),
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


#: Tokens that make a PLACEMENT-ADMISSION claim in served prose.
#:
#: #154. Two served node entries stated the PRE-widening grammar, and one of them
#: contradicted its own machine-readable ``placements`` list in the same object.
#: That is the duplicate-authority defect class twice over: which slot admits
#: which kind is decided by ``BODY_CAPABILITIES_V1``, and a hand-written sentence
#: is a second copy of it that drifts the first time the matrix moves. The rebase
#: of the contract golden then FROZE both falsehoods, so the suite was pinning
#: served text against a golden that asserted the old grammar.
#:
#: The fix is structural rather than another correction: the placement sentence is
#: now DERIVED (``_derived_placement_fact``), and the vocabulary below is REFUSED
#: in a hand-written ordering fact. The claim this guard makes is bounded and
#: checkable — the tokens are exactly the served context names plus the root
#: sequence, which is the closed set a placement claim can be about.
_PLACEMENT_CLAIM_TOKENS: Tuple[str, ...] = (
    "branch path",
    "branch leg",
    "decision arm",
    "decision true arm",
    "decision false arm",
    "try/catch body",
    "try body",
    "catch body",
    "root sequence only",
)


def _derived_placement_fact(
    kind: str, placements: List[ProcessIRAuthoringPlacementV1]
) -> Tuple[str, ...]:
    """The one sentence that states where a kind may appear, read off the matrix.

    Absence is denial everywhere else in this system, so it is stated positively
    here too: the sentence lists what IS admitted and says nothing about the rest.
    A kind with no control-body placement gets the root-only sentence, which is
    equally derived — from the ABSENCE of rows rather than from prose.
    """
    # SCOPED to what the authority actually decides. BODY_CAPABILITIES_V1 governs
    # control-body slots and says nothing about the root sequence, whose rules
    # live in SequenceNodeV1. A sentence that read "every slot not listed rejects
    # it" would therefore assert something the matrix cannot support — and would
    # be false for every root-admitted kind. Overclaiming here would repeat, in
    # generated form, exactly the defect that made this function necessary.
    if not placements:
        return (
            "Control-body placement: none — no Branch path, Decision arm or "
            "Try/Catch body slot admits it. Root-sequence rules are separate.",
        )
    slots = sorted("{0} {1}".format(item.context, item.slot) for item in placements)
    return (
        "Control-body placement: admitted as {0}; no other control-body slot "
        "admits it. Root-sequence rules are separate.".format(", ".join(slots)),
    )


#: Hand-written ordering facts that MENTION a placement context and have been
#: reviewed against the matrix. Keyed by node kind; the value is the exact
#: sentence.
#:
#: The guard below does NOT decide whether a sentence is true — a reader over
#: English cannot make that claim, and pretending otherwise is how a checker ends
#: up being the subject of its own findings. What it decides is bounded and
#: total: a sentence that mentions a placement context is either on this list
#: (someone compared it to the matrix) or it fails the build. The AUTHORITATIVE
#: placement statement is the derived one, which every node entry carries; these
#: are advisory prose that merely happen to name a slot.
_REVIEWED_PLACEMENT_PROSE: Mapping[str, Tuple[str, ...]] = MappingProxyType({
    # structuring advice; names no admission
    "process_call": (
        'To run several children, give each its own path — separate branch paths, or separate wrappers — rather than chaining calls.',
    ),
    # state-ordering semantics; names no admission
    "set_dpp": (
        'Per-execution state accumulates across Branch paths in the authored order, so a later path sees what an earlier one wrote.',
    ),
    # verified against _check_stop_terminal_has_work: leg/true-arm require work, false arm exempt
    "stop": (
        'A Branch path or Decision true arm ending in a stop must do some work first; a Decision false arm may stop with no steps at all.',
    ),
    # verified against BODY_CAPABILITIES_V1: admitted on branch_leg + decision_true_arm terminals, absent from decision_false_arm
    "target": (
        'A target is a legal terminal of a Branch path and of a Decision true arm; it is not admitted on a Decision false arm.',
    ),
})


def _assert_no_hand_written_placement_claim(kind: str, facts: Tuple[str, ...]) -> None:
    """A sentence naming a placement slot must have been reviewed against the matrix.

    Deliberately NOT a truth check. It is a REVIEW TRIP: mentioning a context is
    the signal that a sentence could go stale when the matrix moves, and the only
    reliable response is that a person looked. #154 shipped two sentences that
    stated the pre-widening grammar — one contradicting its own machine-readable
    placements list — and the contract rebase then froze both.
    """
    reviewed = _REVIEWED_PLACEMENT_PROSE.get(kind, ())
    for fact in facts:
        lowered = fact.lower()
        for token in _PLACEMENT_CLAIM_TOKENS:
            if token not in lowered:
                continue
            if fact in reviewed:
                break
            raise ValueError(
                "node {0!r} has an UNREVIEWED ordering fact naming a placement "
                "slot ({1!r}): {2!r}. Placement itself is derived from "
                "BODY_CAPABILITIES_V1 — delete the sentence, or compare it to the "
                "matrix and add it to _REVIEWED_PLACEMENT_PROSE.".format(
                    kind, token, fact
                )
            )


def _derived_trailing_cache_put_fact(kind: str) -> Tuple[str, ...]:
    """Where a trailing ``cache_put`` is tolerated, read off the model's table.

    Same reason as the placement sentence: the rule lives in
    ``TRAILING_CACHE_PUT_TERMINALS`` and a hand-written description of it is a
    second copy that goes stale the first time a slot changes — which is exactly
    what #154 item 4 did to the previous sentence.
    """
    if kind != "cache_put":
        return ()
    from ..models.process_ir import TRAILING_CACHE_PUT_TERMINALS
    from ..compiler.process_ir.body_capabilities import PUBLIC_BODY_CONTEXTS

    tolerated = sorted(
        "{0} ({1})".format(PUBLIC_BODY_CONTEXTS[context], "/".join(sorted(terminals)))
        for context, terminals in TRAILING_CACHE_PUT_TERMINALS.items()
        if terminals
    )
    if not tolerated:
        return (
            "A cache_put in a MID-LIST step position must be followed immediately "
            "by a stream-replacing cache read; no body tolerates it as the last "
            "step.",
        )
    return (
        "A cache_put in a MID-LIST step position must be followed immediately by "
        "a stream-replacing cache read. As the LAST step it is tolerated only "
        "where the terminal cannot need the stream: {0}.".format(", ".join(tolerated)),
    )


def _hand_written_ordering(kind, facts) -> Tuple[str, ...]:
    written = tuple(facts.get(_ORDERING, ()))
    _assert_no_hand_written_placement_claim(kind, written)
    return written


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

    connector_action_ids = tuple(
        sorted(
            "connector_action.{0}.{1}".format(
                _family_token(str(row["family"])), str(row["action"]).lower()
            )
            for row in sources.connector_rows
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
                ordering_facts=(
                    _hand_written_ordering(kind, facts)
                    + _derived_trailing_cache_put_fact(kind)
                    + _derived_placement_fact(kind, placements_by_kind.get(kind, []))
                ),
                required_references=tuple(
                    ProcessIRAuthoringReferenceV1(field=field, required=required)
                    for field, required in facts.get(_REFS, ())
                ),
                related_entry_ids=tuple(facts.get(_RELATED, ()))
                + tuple(f"capability.{cap}" for cap in facts.get(_CAPS, ()))
                # The connector nodes publish no generic document semantics
                # (they are action-dependent), so they must point at the rows
                # that do. Generated, so a new action row is linked automatically.
                + (connector_action_ids if kind in _CONNECTOR_NODE_KINDS else ()),
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
                    # UNSPECIFIED, not per_document. Grouping is decided by the
                    # operation's own configuration, which the (family, action)
                    # registry cannot see — asserting `per_document` for every
                    # row published a fact with no authority behind it, on an
                    # entry whose sources claim everything is generated.
                    grouping="unspecified",
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
                    # GENERATED from the family's own published tuple, never
                    # written per action: every action of a family binds the same
                    # locations, so a hand-written sentence here would be N
                    # copies of one fact and would drift the first time a family
                    # gained a location.
                    (
                        "Per-document bindable request locations: {0}.".format(
                            ", ".join(row["per_document_bindable_locations"])
                        )
                        if row["per_document_bindable_locations"]
                        else "No per-document bindable request location: a path "
                        "binding on this family is rejected."
                    ),
                ),
                diagnostic_codes=(
                    "PROCESS_IR_CAPABILITY_CONNECTOR_ACTION_UNSUPPORTED",
                    "PROCESS_IR_CAPABILITY_DYNAMIC_PATH_UNSUPPORTED",
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
    for specs, source_id, stages, stage_label in (
        # The phases each code is REACHABLE FROM — not the module that emits it.
        #
        # The distinction cost a round. Filing by producer said "parse codes
        # belong to plan", which is true of ownership and false of reachability:
        # ``action="compile"`` re-runs parse and semantic validation before the
        # compiler, so it is a strict SUPERSET of plan. A caller repairing a
        # rejected compile who filtered by ``compile`` therefore missed most of
        # the codes they had just received.
        #
        # So the containment is encoded here rather than left implicit:
        # everything plan can raise, compile can raise too. Compiler diagnostics
        # are the only ones plan cannot reach — nothing is compiled at plan.
        # ``repair`` stays the union, so it is always the safe filter.
        (sources.parse_specs, SOURCE_PARSE_DIAGNOSTICS, ("plan", "compile"), AUTHORING_LAYER_PARSER),
        (
            sources.finding_specs,
            SOURCE_VALIDATION_DIAGNOSTICS,
            ("plan", "compile"),
            AUTHORING_LAYER_SEMANTIC_VALIDATOR,
        ),
        (sources.compiler_specs, SOURCE_COMPILER_DIAGNOSTICS, ("compile",), AUTHORING_LAYER_COMPILER),
    ):
        for spec in specs:
            code = str(spec["code"])
            # #177: the three PRODUCTION accessors fail closed on a blank or asymmetric
            # registry, but a `ProjectionSourcesV1` may be INJECTED — that is the whole
            # point of the snapshot, so drift controls can perturb one source without
            # monkeypatching a production mapping. An injected row therefore reaches this
            # merge without having passed `_complete_spec_rows`, and a blank message here
            # is served as a remediation-shaped summary exactly as it was before this
            # slice. Validating at the merge closes the bypass for every source, injected
            # or not, and names CODES only — never authored content.
            for field in ("message", "remediation"):
                # TYPE and emptiness. `(spec.get(field) or "").strip()` raised
                # `AttributeError` on a non-string, and wrapping it in `str()` was worse:
                # `str(object())` is a non-empty string, so a non-string row PASSED the
                # guard and failed further downstream. Both were fail-closed; neither told
                # the caller which code was at fault.
                value = spec.get(field)
                if not isinstance(value, str) or not value.strip():
                    raise ValueError(
                        "{0} diagnostic source serves a blank {1} for {2}".format(
                            stage_label, field, code
                        )
                    )
            row = merged.setdefault(
                code,
                {
                    "message": "",
                    "remediation": "",
                    "sources": [],
                    "stages": ["repair"],
                    "texts": {},
                    "messages": {},
                },
            )
            if spec.get("message") and not row["message"]:
                row["message"] = spec["message"]
            if spec.get("remediation") and not row["remediation"]:
                row["remediation"] = spec["remediation"]
            # MESSAGES too, not only remediations. Attributing the remediations
            # and leaving `summary` first-wins fixed half the defect: a caller
            # who received the compiler's wording still could not find it in an
            # entry that names the compiler as a generated source.
            if spec.get("message"):
                row["messages"].setdefault(spec["message"], []).append(stage_label)
            # KEEP every distinct text, attributed to the phase that emits it.
            # Seven codes have producers whose wording differs, and taking the
            # first non-empty discarded the rest while the entry went on
            # claiming both producers as generated sources. A caller who
            # received the compile wording could not find it in the contract.
            if spec.get("remediation"):
                row["texts"].setdefault(spec["remediation"], []).append(stage_label)
            row["sources"].append(source_id)
            for stage in stages:
                if stage not in row["stages"]:
                    row["stages"].append(stage)

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
                # The label legend that explains `ordering_facts` lives on the
                # PAGE (`diagnostic_label_legend`), not here: it is identical on
                # every entry, and repeating it spent about a fifth of the entry
                # byte budget and pushed entries off a full diagnostic page.
                summary=_diagnostic_summary(row),
                workflow_stages=tuple(row["stages"]),
                ordering_facts=tuple(
                    "[{0}] {1}".format("/".join(sorted(set(where))), text)
                    for text, where in sorted(row["messages"].items())
                )
                + tuple(
                    "[{0}] {1}".format("/".join(sorted(set(where))), text)
                    for text, where in sorted(row["texts"].items())
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
                # EVERY field the authority carries. Projecting four of six
                # dropped `lifetime` (which compartment a write lands in — the
                # one field the traversal actually reads) and
                # `survives_branch_path_entry`, so a caller could not tell why
                # a pre-Branch write is visible in every path.
                ordering_facts=tuple(
                    "{0}: {1}.".format(
                        field.replace("_", " ").capitalize(),
                        "yes"
                        if value is True
                        else "no"
                        if value is False
                        else value,
                    )
                    for field, value in sorted(row.items())
                    if field != "state_scope"
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


#: The AUTHORITY each semantic rule states a fact about.
#:
#: Every rule used to name `runtime.process_ir_models`, which for most of them
#: is simply not where the fact lives — that module contains the word
#: "intersection" zero times, yet the convergence rule cited it. Swapping every
#: rule to an unrelated source failed nothing but the byte snapshot, because the
#: attribution was hardcoded at the single construction site below and pinned by
#: nothing. A served `source_id` is a claim about provenance; a wrong one sends
#: a caller to the wrong place to verify it.
_SEMANTIC_RULE_SOURCES = {
    "semantic_rule.branch.path_order": (SOURCE_STATE,),
    "semantic_rule.control.no_continuation": (SOURCE_CAPABILITIES,),
    "semantic_rule.control.depth_bound": (SOURCE_MODELS,),
    "semantic_rule.state.convergence": (SOURCE_STATE,),
    "semantic_rule.retry.replay_safety": (SOURCE_RETRY, SOURCE_CONNECTORS),
    "semantic_rule.documents.explicit_split_combine": (SOURCE_CAPABILITIES,),
    "semantic_rule.references.opaque": (SOURCE_MODELS,),
    # #154. The effect rules are deliberately ABSENT: their source is derived
    # per family from `_EFFECT_AUTHORITY_SOURCES`, keyed by the authority token
    # the resolver publishes. Listing them here too would put a generated row
    # back into a hand-kept map — the drift this family was moved away from.
}


#: Which runtime module each effect-authority row states a fact ABOUT, keyed by
#: the same authority token the resolver publishes.
#:
#: Every one of these rows was served as `runtime.process_ir_models` — the
#: module that carries the declaration SHAPE and none of these facts. It is the
#: identical defect `_SEMANTIC_RULE_SOURCES` above exists to fix, recurring in
#: the generated rows, so it gets the same answer rather than a second
#: hardcode: the served source is DERIVED from the row's own authority token.
#: `test_every_effect_authority_row_names_its_own_authority` requires this map
#: to be TOTAL over `effect_authority_rows()`, so a family added there without a
#: source fails loudly instead of inheriting a wrong one.
#: ... and with WHAT KIND of thing that module is, which the served source
#: carries as a separate closed token. Deriving the id while leaving the
#: provenance hardcoded made the pair internally contradictory: an entry citing
#: a registry was still labelled as coming from a runtime model, so a consumer
#: could not tell registry-derived facts from model-derived ones. The two are
#: one fact about the authority and are derived together.
_EFFECT_AUTHORITY_SOURCES = {
    "server-inspection:map-function-registry":
        (SOURCE_MAP_FUNCTIONS, "runtime_registry"),
    "server-registry:vetted-scripts":
        (SOURCE_VETTED_SCRIPTS, "runtime_registry"),
    # A child summary is read off the lineage walk, which is what
    # `semantic_rule.effect.subprocess_inspection` cites too.
    "server-inspection:child-process-ir": (SOURCE_STATE, "runtime_model"),
    "caller-assertion:no-state-established": (SOURCE_STATE, "runtime_model"),
    # Not a fact about any upstream registry: it is the resolver's own policy
    # for what a declaration it cannot verify does NOT buy.
    "none:every-strict-finding-stands": (SOURCE_EFFECT_RESOLVER, "runtime_model"),
}


#: Served wording for each effect-authority row, keyed by the authority token the
#: resolver publishes. The TOKENS come from `effect_authority_rows()`; only the
#: prose lives here, so a family added there without wording fails loudly rather
#: than going unserved.
_EFFECT_AUTHORITY_PROSE = {
    "server-inspection:map-function-registry":
        "The authority is inspection of the resolved map — its function mappings "
        "through the map-function registry, and its document-cache joins — never "
        "a caller's assertion about it. A join contributes the cache read it "
        "names only when that read can be REPRESENTED; a join on a cache written "
        "outside this process, or one naming a literal cache id, cannot be, and "
        "makes the whole map opaque rather than being dropped from an otherwise "
        "exact effect. A map the plan would not build, or one containing any "
        "function whose effect is unannotated, is opaque for the same reason.",
    "server-registry:vetted-scripts":
        "Admitted only when the recomputed (language, digest) matches a server-owned "
        "vetted contract. A script whose digest matches no entry is INERT: the server "
        "knows which script it is and still has no authority for what it does.",
    # NO cross-reference to the rule id: the generated authority is folded INTO
    # that rule, so a pointer here aims the reader at the sentence they are
    # already reading. It survived the fold because the pointer was correct back
    # when the authority was a separate served entry.
    "server-inspection:child-process-ir":
        "The authority is the compiler's own lineage walk, run over the child: "
        "the reads it requires and the writes it guarantees are exactly what "
        "that walk establishes, never a caller's assertion about it.",
    "caller-assertion:no-state-established":
        "The one declaration with no server-side content authority, because an outside "
        "writer is not present in the artifact. It never establishes a cache write; "
        "combined with an authored external_writer flag it downgrades the "
        "missing-writer error to a named warning. Without that flag the declaration is "
        "valid and simply inert.",
    "none:every-strict-finding-stands":
        "A declaration that is omitted, or that passes identity with no server-side "
        "authority behind its content, is INERT: it establishes nothing and every "
        "strict finding still fires. Inert is not an error — an unregistered script is "
        "a legal thing to author, it just proves nothing.",
}


#: The served rule each declaration family's authority belongs to.
#:
#: The plan asked for ONE generated `semantic_rule.effect.*` family whose facts
#: derive from `effect_authority_rows()`. Shipping generated `effect_authority.*`
#: entries ALONGSIDE the hand-written rules created a parallel namespace instead
#: — two served descriptions of one trust boundary, both to be kept true. The
#: generation now binds to the rule ids that already existed, so the derived
#: authority reaches the contract without a second family to maintain.
_EFFECT_FAMILY_RULES = {
    "map_effects": "semantic_rule.effect.map_inspection",
    "script_effects": "semantic_rule.effect.script_registry",
    "subprocess_effects": "semantic_rule.effect.subprocess_inspection",
    "external_writers": "semantic_rule.effect.external_writer",
    "omitted_or_inert": "semantic_rule.effect.declaration_boundary",
}


def _effect_authority_entries() -> List[ProcessIRAuthoringContractEntryV1]:
    """One served rule per declaration family, GENERATED from the resolver's table.

    The families were hand-listed here, which made the served trust boundary a
    second copy of a fact the resolver owns — and left the resolver's own
    `effect_authority_rows()` with no consumer at all despite describing itself as
    served. A family added there now reaches the contract by construction, and one
    without wording raises instead of going unserved.
    """
    from .process_ir_effects import effect_authority_rows

    by_id = {rule[0]: rule for rule in _SEMANTIC_RULES}
    missing = set(_EFFECT_FAMILY_RULES.values()) - set(by_id)
    if missing:
        raise KeyError("no served rule for effect families: {0}".format(sorted(missing)))
    entries = []
    for family, authority in effect_authority_rows():
        prose = _EFFECT_AUTHORITY_PROSE.get(authority)
        if prose is None:
            raise KeyError(
                "no served wording for effect authority {0!r} (family {1!r})".format(
                    authority, family
                )
            )
        entry_id = _EFFECT_FAMILY_RULES[family]
        rule = by_id[entry_id]
        _unused, category, title, summary, node_kinds, related = rule
        entries.append(
            ProcessIRAuthoringContractEntryV1(
                contract_entry_id=entry_id,
                entry_type="semantic_rule",
                category=category,
                subject=entry_id.split(".", 1)[1],
                title=title,
                # The RULE's own wording, plus the authority statement derived
                # from the resolver's table. One entry, so a caller cannot read
                # two descriptions of one trust boundary and find them differing.
                summary="{0} {1}".format(summary, prose),
                node_kinds=node_kinds,
                workflow_stages=("author", "plan", "repair"),
                related_entry_ids=related,
                ordering_facts=("Authority: {0}.".format(authority),),
                sources=(
                    _source(
                        _EFFECT_AUTHORITY_SOURCES[authority][0],
                        "generated",
                        _EFFECT_AUTHORITY_SOURCES[authority][1],
                        "compiler",
                        family,
                    ),
                ),
            )
        )
    return entries


def _semantic_rule_entries(
    sources: ProjectionSourcesV1,
) -> List[ProcessIRAuthoringContractEntryV1]:
    generated = _EFFECT_FAMILY_RULES.values()
    return _effect_authority_entries() + [
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
            sources=tuple(
                _source(
                    source_id,
                    "parity_pinned",
                    "live_capture_attested",
                    "compiler",
                    entry_id,
                )
                for source_id in _SEMANTIC_RULE_SOURCES[entry_id]
            ),
        )
        for entry_id, category, title, summary, node_kinds, related in _SEMANTIC_RULES
        # ... except the effect rules, which `_effect_authority_entries` emits
        # with their derived authority folded in. Emitting them here too would
        # duplicate the id.
        if entry_id not in generated
    ]


#: The three layers that AUTHOR diagnostic text, as served.
#:
#: Named once here because these strings are BOTH the bracketed label in
#: ``ordering_facts`` and the vocabulary the served legend teaches. A test that
#: re-typed one of them kept asserting a label the projection had stopped
#: emitting, and still passed — the drift the two-way pins exist to catch.
AUTHORING_LAYER_PARSER = "parser"
AUTHORING_LAYER_SEMANTIC_VALIDATOR = "semantic validator"
AUTHORING_LAYER_COMPILER = "compiler"

#: Every authoring layer, in pipeline order.
AUTHORING_LAYERS: Tuple[str, ...] = (
    AUTHORING_LAYER_PARSER,
    AUTHORING_LAYER_SEMANTIC_VALIDATOR,
    AUTHORING_LAYER_COMPILER,
)


def expected_page_rule(field: str):
    """The registry's own value for a computed page rule.

    The single place the page model asks "what should this be?", so the model
    carries no second copy of any of these facts.

    Raises ``KeyError`` on an unknown field rather than returning ``None``. A
    ``None`` was read by the caller as "no opinion", so a one-character typo in
    a field name silently degraded that field's validator to a no-op — the
    cheapest possible way to disable a guard, and invisible on every correct
    page because a disabled guard changes nothing until something is wrong.
    """
    builders = {
        "state_mappings": state_mappings,
        "catalog_entry_count": lambda: len(build_process_ir_authoring_entries()),
        "facets": lambda: _facets(build_process_ir_authoring_entries()),
    }
    if field not in builders:
        raise KeyError(f"no registry authority for page rule {field!r}")
    return builders[field]()


def _page_rule_default(field: str) -> Any:
    """A page-level RULE's value, read from the model that declares it.

    These are `Literal` defaults on the page model — the model is the authority,
    so a second copy here would be exactly the duplicated fact this contract
    keeps having to remove.
    """
    return ProcessIRAuthoringContractPageV1.model_fields[field].default


def _page_envelope_fields() -> Tuple[str, ...]:
    """Every page field that is NOT the entries array, in declaration order."""
    return tuple(
        name
        for name in ProcessIRAuthoringContractPageV1.model_fields
        if name != "entries"
    )


def _diagnostic_summary(row: MutableMapping[str, Any]) -> str:
    """The one-line orientation for a diagnostic code.

    Every diagnostic served through THESE THREE REGISTRIES now carries a static
    ``message``, so the fallback below has no reachable case among them: the
    three spec accessors refuse an asymmetric or blank registry
    (``_complete_spec_rows`` in ``boomi_mcp.models.process_ir``), the merge above
    refuses a blank field from an INJECTED source as well, and
    ``tests/test_process_ir_served_text_enforcement.py`` proves from the emitting
    modules that every code those layers can raise is registered with both texts.

    Two limits, stated rather than implied — an earlier version of this note said
    "unreachable by construction" flatly and was wrong twice over. The native
    process-graph verifier serves its own ``(code, message, remediation)`` result
    and is deliberately OUTSIDE these registries, so nothing here speaks for it.
    And "by construction" was false while an injected ``ProjectionSourcesV1``
    could carry a blank field straight past the accessors; that hole is closed at
    the merge now, which is what makes the claim true rather than aspirational.

    It is retained as an honest degradation path rather than deleted, and it is
    PINNED: ``test_no_compiler_diagnostic_falls_back_to_its_remediation`` asserts
    the fallback set is empty, so a code that ever reached it would fail there
    rather than quietly serving a "how to fix" where a "what is wrong" belongs.

    History, because the shape of the old defect is the reason the pin exists.
    Seven compiler codes carried a remediation and no message. FOUR of them are
    also raised by the parse layer, which supplies a short message that the merge
    picked up, so they read correctly and the gap was invisible from here. The
    remaining THREE — ``PROCESS_IR_COMPILE_CONTROL_WIRING_INVALID``,
    ``PROCESS_IR_SEMANTIC_JOIN_UNSUPPORTED`` and
    ``PROCESS_IR_SEMANTIC_UNTERMINATED_PATH`` — reached this fallback and served
    their remediation as their summary. #177 registered canonical compiler
    messages for all seven; measured after that change, every one of those counts
    is now zero. The node that used to measure them,
    ``test_exactly_the_expected_codes_fall_back_to_their_remediation``, FROZE the
    defect (it asserted the count was exactly seven) and was tombstoned by the
    same slice that closed it.
    """
    message = (row["message"] or "").strip()
    if message:
        return message
    for text in list(row["messages"]) + list(row["texts"]):
        candidate = str(text).strip()
        if candidate:
            return candidate
    return ""


def _prose_digest(value):
    """A short stable digest of prose, so it moves a revision without being copied."""
    import hashlib

    payload = json.dumps(value, sort_keys=True, separators=(",", ":"), default=str)
    return hashlib.sha256(payload.encode("utf-8")).hexdigest()[:12]


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
                subject=name,
                title=name.replace("_", " ").strip().capitalize(),
                summary=(
                    "A design pattern. Fetch its full text from the doctrine "
                    "selector; this entry publishes its identity, state and "
                    "evidence, never a copy of the prose."
                ),
                canonical_state=canonical,
                source_state=status,
                applicable=applicable,
                # `category` is the served FILTER FACET, and it stays "doctrine".
                #
                # Publishing each pattern's own taxonomy here looked like more
                # fidelity and was a silent breaking change: `category='doctrine'`
                # — the only call that selected these 39 entries — became
                # INVALID_INPUT, and `category='reliability'` went from 2 entries
                # to 9 by mixing patterns in with the node it used to return.
                # There is no `entry_type` filter, so nothing else selected them.
                #
                # The pattern's own category is metadata ABOUT the entry, and is
                # published below where it moves the revision without redefining
                # a filter value callers depend on.
                category="doctrine",
                workflow_stages=("discover", "plan"),
                ordering_facts=tuple(
                    fact
                    for fact in (
                        "Doctrine category: {0}.".format(row["category"])
                        if row.get("category")
                        else "",
                        "Verification: {0}.".format(row["verification_status"])
                        if row.get("verification_status")
                        else "",
                        "Provenance: {0}.".format(row["provenance"])
                        if row.get("provenance")
                        else "",
                        "Cross-references: {0}.".format(
                            ", ".join(sorted(row.get("cross_refs") or ()))
                        )
                        if row.get("cross_refs")
                        else "",
                        # A DIGEST, not the text. `mutual_exclusion` holds prose
                        # sentences, not pattern names, so joining them served a
                        # verbatim copy of doctrine — under a label promising a
                        # name list, in an entry whose own summary says it never
                        # copies prose. The digest still moves the revision when
                        # the guidance changes; the words stay on the selector
                        # that owns them.
                        "Mutual-exclusion guidance: present (digest {0}).".format(
                            _prose_digest(row["mutual_exclusion"])
                        )
                        if row.get("mutual_exclusion")
                        else "",
                    )
                    if fact
                ),
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
                # ``recipe:``, not ``recipe_registry:``. The registry SNAPSHOT
                # lives under `recipe_registry`, but a single descriptor is
                # fetched with `recipe:<id>[@<version>]` — every generated
                # `recipe_registry:` selector returned SCHEMA_NAME_UNSUPPORTED.
                recipe_selector=f"recipe:{recipe_id}@{version}",
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


#: Node kinds whose document behaviour comes from a connector action row rather
#: than from the node itself.
_CONNECTOR_NODE_KINDS = frozenset({"connector_call", "source", "target"})

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
    rule = (
        f"limit must be an integer from 1 to {PROCESS_IR_AUTHORING_MAX_LIMIT} "
        f"(default {PROCESS_IR_AUTHORING_DEFAULT_LIMIT}). It bounds the entry "
        f"COUNT; a separate byte budget bounds the entries' size."
    )
    if not isinstance(limit, int) or isinstance(limit, bool):
        raise ProcessIRAuthoringQueryError("limit", rule=rule)
    if limit < 1 or limit > PROCESS_IR_AUTHORING_MAX_LIMIT:
        raise ProcessIRAuthoringQueryError("limit", rule=rule)
    return limit


def _query_error_from(exc: Any) -> "ProcessIRAuthoringQueryError":
    """The caller-facing form of a rejected query, naming the field pydantic did."""
    fields = []
    for error in getattr(exc, "errors", lambda: ())():
        for part in error.get("loc", ()):
            if isinstance(part, str) and part not in fields:
                fields.append(part)
    # A RULE, not a facet. Two of the six fields this can blame
    # (`authoring_entry_id`, `after_entry_id`) are exact values with no facet to
    # publish, so the enum template's "fetch the facets, then filter with a
    # published value" would send that caller in a circle — the same shape the
    # envelope's own comment already warns about for the cursor.
    return ProcessIRAuthoringQueryError(
        ", ".join(fields) or "query",
        (),
        "the value supplied for this filter was not of the type the contract "
        "declares; send the type named in the tool schema",
    )


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
        raise ProcessIRAuthoringQueryError(
            "after_entry_id",
            rule=(
                "after_entry_id resumes a FILTERED result and needs a companion "
                "filter (authoring_entry_id, node_kind, category, capability_id "
                "or workflow_stage). The value itself is a contract_entry_id and "
                "may well be valid — what is missing is the filter to page within."
            ),
        )

    if node_kind and node_kind not in facets.node_kinds:
        raise ProcessIRAuthoringQueryError("node_kind", facets.node_kinds)
    if category and category not in facets.categories:
        raise ProcessIRAuthoringQueryError("category", facets.categories)
    if capability_id and capability_id not in facets.capability_ids:
        raise ProcessIRAuthoringQueryError("capability_id", facets.capability_ids)
    if workflow_stage and workflow_stage not in facets.workflow_stages:
        raise ProcessIRAuthoringQueryError("workflow_stage", facets.workflow_stages)

    # Converted HERE, at the one construction site where a pydantic failure can
    # only mean the caller. Narrowing by exception TYPE at the serving layer
    # could never work: this projection builds every entry and the page itself
    # through pydantic, so a malformed AUTHORITY raises the identical
    # `ValidationError` as a bad filter — and a recipe registry contributing one
    # bad row was then blamed on the caller, naming "filters" that are really
    # entry fields. Type does not partition the universe; the call site does.
    try:
        query = ProcessIRAuthoringQueryV1(
            authoring_entry_id=authoring_entry_id,
            node_kind=node_kind,
            category=category,
            capability_id=capability_id,
            workflow_stage=workflow_stage,
            after_entry_id=after_entry_id,
            limit=effective_limit,
        )
    except PydanticValidationError as exc:
        raise _query_error_from(exc) from None

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
        diagnostic_label_legend=DIAGNOSTIC_LABEL_LEGEND,
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
        # DERIVED, like the revision payload below. A literal here published a
        # page rule the model no longer declared: flipping it to "gated" served
        # a falsified rule through `list_capabilities()` and moved the
        # capability revision, with the suite green.
        "unlisted_placement_state": _page_rule_default("unlisted_placement_state"),
        "unlisted_connector_action_state": _page_rule_default(
            "unlisted_connector_action_state"
        ),
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
            # Named for what it actually bounds. It caps the serialized
            # ENTRIES, not the whole response: the schema, facets and state
            # mappings that wrap them are a fixed envelope the caller asked for
            # by choosing this selector, and counting them would make the
            # entry budget shrink as the envelope grew.
            "entry_byte_budget": PROCESS_IR_AUTHORING_BYTE_BUDGET,
            # DERIVED. The hand-written list said "schema, facets and
            # state_mappings" and went stale the moment the envelope gained
            # `diagnostic_label_legend` — the fourth time in this contract that
            # an enumeration of something the code already knows drifted from
            # it. The model is the authority for what the envelope contains.
            "entry_byte_budget_scope": (
                "the serialized entries array only; the surrounding envelope "
                "({0}) is not counted".format(", ".join(_page_envelope_fields()))
            ),
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

    Page fields that describe the QUERY (counts, limit, truncated, cursor) are
    excluded — they vary per call and would make the revision meaningless. Page
    fields that publish a RULE are included: moving the label legend out of the
    entries and forgetting it here would have left its text covered by nothing
    at all, so it could be rewritten to say the opposite and no revision would
    move. `test_every_published_page_rule_participates_in_the_revision` fails
    when a new envelope field is added without that decision being made.
    """
    return {
        "contract_version": PROCESS_IR_AUTHORING_CONTRACT_VERSION,
        "diagnostic_label_legend": DIAGNOSTIC_LABEL_LEGEND,
        "unlisted_connector_action_state": _page_rule_default(
            "unlisted_connector_action_state"
        ),
        "unlisted_placement_state": _page_rule_default("unlisted_placement_state"),
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
    "AUTHORING_LAYERS",
    "AUTHORING_LAYER_COMPILER",
    "AUTHORING_LAYER_PARSER",
    "AUTHORING_LAYER_SEMANTIC_VALIDATOR",
    "DIAGNOSTIC_LABEL_LEGEND",
    "ProcessIRAuthoringQueryError",
    "ProjectionSourcesV1",
    "authoring_contract_entry_ids_for_diagnostic",
    "build_process_ir_authoring_entries",
    "build_process_ir_authoring_index",
    "canonical_state_scope_rows",
    "collect_projection_sources",
    "process_ir_authoring_revision_payload",
    "query_process_ir_authoring_contract",
    "reset_process_ir_authoring_cache",
    "state_mappings",
    "validate_process_ir_authoring_projection",
]


def canonical_state_scope_rows() -> List[Dict[str, Any]]:
    """The four state scopes as served by ``cache_property_authoring``.

    Lives here, not in ``meta_tools``, because this module is the ONE place
    permitted to read the compiler's registries. A serving module importing
    ``semantic_validation`` directly would be a third wiring site into a package
    whose call sites are deliberately enumerated.

    The document cache is renamed to the spelling the cache/property surface
    already uses; the lineage model calls the scope ``cache``. One rename,
    applied once, so the served vocabulary stays internally consistent.
    """
    from ..compiler.process_ir.semantic_validation.lineage import state_visibility_rows
    from ..models.cache_property_models import PROCESS_PROPERTY_SCOPE_V1

    rows = [dict(row) for row in state_visibility_rows()]
    rows.append(dict(PROCESS_PROPERTY_SCOPE_V1))
    for row in rows:
        if row["state_scope"] == "cache":
            row["state_scope"] = "documentcache"
    return sorted(rows, key=lambda row: str(row["state_scope"]))

"""The public ProcessIR authoring contract models (issue #146 amendment).

**The problem this exists to solve.** A strict JSON Schema tells a caller what
shape a ``ProcessIRV1`` document must have. It does not tell them what the
document MEANS — whether Branch legs run at once, which node kinds a Decision's
false arm admits, whether a database ``Send`` may sit inside a retried region.
Those facts lived in three compiler-internal registries that no MCP tool
reached, and in remediations pointing at repository files a caller cannot fetch.
So an LLM authoring a process had two options: import the compiler, or compile
repeatedly and learn from the rejections.

This module is the model half of the fix. It defines the shape of a served,
versioned, filtered PROJECTION of those authorities;
``boomi_mcp.authoring.process_ir_projection`` builds it and
``get_schema_template(schema_name="process_ir_authoring")`` serves it.

**Pure models, deliberately.** Nothing here imports the compiler, the doctrine
registry, or the recipe registry — ``tests/test_process_ir_compiler_surface.py``
asserts that importing ``boomi_mcp.models`` pulls in zero ``boomi_mcp.compiler``
modules, and this module is exported from that package. The projector does the
reading; these models only constrain the result.

**Every entry names its source.** ``sources`` is non-empty by construction, and
each source declares whether the entry was ``generated`` from a named runtime
registry or ``parity_pinned`` against one by a CI test. A claim with no
authority behind it cannot be expressed in this contract, which is the whole
mechanism that stops the served text drifting from the code.
"""

from __future__ import annotations

import json
from typing import Any, Dict, List, Literal, Optional, Tuple

from pydantic import (
    AfterValidator,
    BaseModel,
    ConfigDict,
    Field,
    StrictBool,
    StrictInt,
    StringConstraints,
)
from typing_extensions import Annotated

PROCESS_IR_AUTHORING_CONTRACT_VERSION = "1"

#: The ONE canonical capability vocabulary this contract publishes.
#:
#: Three vocabularies already exist in the codebase — the ProcessIR capability
#: manifest's ``supported/gated/unsupported``, doctrine's
#: ``emittable_today/gated/guidance_only/na``, and the cache/property surface's
#: ``executable/…`` states. A fourth, invented here, would make four. Instead
#: every source state MAPS onto these three, and the mapping is published
#: alongside the entries (see :class:`ProcessIRAuthoringStateMappingV1`) with the
#: original ``source_state`` preserved verbatim on every entry.
#:
#: ``gated`` and ``unsupported`` are NOT interchangeable and the distinction
#: survives every mapping: ``gated`` means "not yet" (the design may become
#: authorable), ``unsupported`` means "never through this contract" (a different
#: design is required). Collapsing them would tell a caller to abandon a shape
#: that is merely pending, or to wait for one that is not coming.
CANONICAL_CAPABILITY_STATES: Tuple[str, ...] = ("gated", "supported", "unsupported")

#: What kind of thing an entry describes. Closed, and sorted so the id grammar
#: (``<entry_type>.<subject>``) sorts entries into type groups.
PROCESS_IR_AUTHORING_ENTRY_TYPES: Tuple[str, ...] = (
    "capability",
    "connector_action",
    "diagnostic",
    "doctrine",
    "node",
    "placement",
    "recipe",
    "recipe_contribution",
    "semantic_rule",
    "state_visibility",
)

#: Where in the authoring workflow an entry is useful. Used as a retrieval
#: filter so a caller can ask for "what I need to repair a diagnostic" without
#: paging the whole catalog.
PROCESS_IR_AUTHORING_WORKFLOW_STAGES: Tuple[str, ...] = (
    "author",
    "compile",
    "discover",
    "plan",
    "repair",
)

#: Retrieval bounds. ``limit`` is a COUNT bound and the byte budget is a SIZE
#: bound; both exist because entries differ in size by more than an order of
#: magnitude, so a count alone cannot bound a payload.
PROCESS_IR_AUTHORING_DEFAULT_LIMIT = 20
PROCESS_IR_AUTHORING_MAX_LIMIT = 50
PROCESS_IR_AUTHORING_BYTE_BUDGET = 64 * 1024

#: Lowercase dotted segments. Deliberately narrow: an id appears in served
#: remediations and in clean-room test citations, so it must be stable, quotable
#: and free of anything needing escaping.
CONTRACT_ENTRY_ID_PATTERN = r"^[a-z0-9_]+(\.[a-z0-9_]+)*$"

ContractEntryId = Annotated[str, StringConstraints(pattern=CONTRACT_ENTRY_ID_PATTERN)]
NonEmptyString = Annotated[str, StringConstraints(min_length=1)]
CanonicalState = Literal["gated", "supported", "unsupported"]


def _sorted_unique(values: Tuple[Any, ...]) -> Tuple[Any, ...]:
    """Every list field is a SET rendered in a deterministic order.

    Order is never meaningful in these fields, so leaving it to the projector's
    iteration order would make the served bytes — and therefore the revision
    that hashes them — depend on dict ordering rather than on content.
    """
    if not values:
        return ()
    try:
        return tuple(sorted(set(values)))
    except TypeError:  # unhashable model instances: sort by canonical JSON
        seen: Dict[str, Any] = {}
        for value in values:
            seen.setdefault(
                json.dumps(
                    value.model_dump(mode="json"), sort_keys=True, separators=(",", ":")
                ),
                value,
            )
        return tuple(seen[key] for key in sorted(seen))


SortedStrings = Annotated[Tuple[str, ...], AfterValidator(_sorted_unique)]
SortedEntryIds = Annotated[Tuple[ContractEntryId, ...], AfterValidator(_sorted_unique)]


def _sorted_unique_models(values):
    """The same rule for tuples of MODELS, which had no validator at all.

    Only the string aliases were sorted-and-uniqued, so ``placements``,
    ``required_references``, ``sources``, ``state_mappings`` and a page's
    ``entries`` accepted duplicates in arbitrary order — and their bytes feed a
    revision, so iteration order could move a hash without any content change.
    """
    return _sorted_unique(values)


class _ContractModel(BaseModel):
    """Strict and frozen: a served contract entry is a value, not a workspace."""

    model_config = ConfigDict(extra="forbid", frozen=True)


class ProcessIRAuthoringSourceV1(_ContractModel):
    """Which runtime authority backs one entry, and how it is kept honest.

    ``source_id`` is SYMBOLIC (``runtime.body_placement_registry``), never a
    filesystem path or a Python identifier. A path in a served response
    describes the server's disk, which the caller cannot see and must not depend
    on — and the registry names themselves are compiler-internal identifiers the
    served surface may not carry at all.

    ``projection`` is the honesty field:

    * ``generated`` — the entry's facts are computed FROM the named source at
      build time, so they cannot disagree with it;
    * ``parity_pinned`` — the entry states something prose must say (a live-
      captured platform behaviour, an ordering guarantee) and a CI test asserts
      it against the named source in both directions.

    ``revision_role`` records which of the three shipped revisions moves when
    this source moves, so a caller who sees a revision change can tell what kind
    of thing changed.
    """

    source_id: NonEmptyString
    source_subject: str = ""
    projection: Literal["generated", "parity_pinned"]
    provenance: Literal[
        "runtime_registry",
        "runtime_model",
        "live_capture_attested",
        "official_documentation",
    ]
    revision_role: Literal["schema", "capability", "compiler"]


class ProcessIRAuthoringPlacementV1(_ContractModel):
    """One (context, slot) position, and whether a kind may appear there.

    ``context`` is the PUBLIC body-context name — a Branch outlet is a
    ``branch_path`` here, which is the platform's own word for it. The compiler
    spells some of these differently internally; the projection maps every one
    through a total, injective, parity-pinned rename, so no fact is lost and no
    internal identifier reaches this surface.
    """
    # This docstring IS served: pydantic publishes it as the model's
    # ``description``. So it may not quote the internal spelling it is
    # explaining — naming the thing would leak exactly what the rename removes.

    context: Literal[
        "branch_path",
        "decision_true_arm",
        "decision_false_arm",
        "try_body",
        "catch_body",
        "root_sequence",
    ]
    slot: Literal["step", "terminal"]
    canonical_state: CanonicalState
    source_state: NonEmptyString


class ProcessIRAuthoringReferenceV1(_ContractModel):
    """A component reference a node requires, and what may satisfy it.

    ``opaque`` is fixed ``True`` and is not a formality: every reference in this
    contract is authored as a ``$ref:KEY`` token or a literal component id, and
    NOTHING about the referenced component — its configuration, its connection,
    its credentials — is ever authored alongside it.
    """

    field: NonEmptyString
    required: StrictBool
    opaque: Literal[True] = True
    accepted_component_types: SortedStrings = ()


class ProcessIRDocumentSemanticsV1(_ContractModel):
    """What a node does to the document stream.

    The three questions a caller must answer to place a node correctly: does it
    need documents arriving, what does it leave behind, and does it act per
    document or per group. ``stream_replacing`` is called out separately from
    ``documents`` because a node that REPLACES the stream discards what came
    before it, which is how a per-document property silently stops existing.
    """

    input_documents: Literal["none", "optional", "required"]
    output_documents: Literal["none", "documents", "stream_replacing", "consumed"]
    #: ``unspecified`` is NOT a synonym for ``not_applicable``. It means no
    #: authority states this fact — a connector action's grouping is decided by
    #: the operation, and inventing ``per_document`` for every row published a
    #: claim with nothing behind it.
    grouping: Literal[
        "per_document", "per_batch", "all_documents", "not_applicable", "unspecified"
    ]


class ProcessIRAuthoringStateMappingV1(_ContractModel):
    """One source state, and the canonical state it maps to.

    Published WITH the entries rather than documented separately so a caller can
    verify the mapping themselves instead of trusting it. ``applicable=False``
    marks a source state that is not a capability claim at all — doctrine's
    ``guidance_only`` and ``na`` describe advice, not something this contract can
    author — so a reader is not told "unsupported" about a thing that was never
    a feature.
    """

    source_vocabulary: NonEmptyString
    source_state: NonEmptyString
    canonical_state: CanonicalState
    applicable: StrictBool = True


class ProcessIRAuthoringContractEntryV1(_ContractModel):
    """One authoring fact, addressable by a stable id.

    The id is the citation handle: served remediations name it, and the
    clean-room test fixtures cite it, so CI can prove every citation resolves
    against the contract actually served.
    """

    contract_entry_id: ContractEntryId
    entry_type: Literal[
        "capability",
        "connector_action",
        "diagnostic",
        "doctrine",
        "node",
        "placement",
        "recipe",
        "recipe_contribution",
        "semantic_rule",
        "state_visibility",
    ]
    category: NonEmptyString
    subject: NonEmptyString
    title: NonEmptyString
    summary: str = ""

    capability_id: Optional[str] = None
    canonical_state: Optional[CanonicalState] = None
    #: The authority's OWN word, reproduced verbatim. Kept beside the canonical
    #: state rather than replaced by it: a caller comparing this contract with
    #: the capability manifest must see the same string in both.
    source_state: Optional[str] = None
    applicable: StrictBool = True

    node_kinds: SortedStrings = ()
    workflow_stages: SortedStrings = ()
    schema_refs: SortedStrings = ()

    placements: Annotated[Tuple[ProcessIRAuthoringPlacementV1, ...], AfterValidator(_sorted_unique_models)] = ()
    document_semantics: Optional[ProcessIRDocumentSemanticsV1] = None
    #: Free-form but STATIC sentences about ordering, terminality and
    #: continuation — the facts that have no structural home. Every one of them
    #: is parity-pinned; none is interpolated from anything a caller sent.
    ordering_facts: SortedStrings = ()

    required_references: Annotated[Tuple[ProcessIRAuthoringReferenceV1, ...], AfterValidator(_sorted_unique_models)] = ()
    diagnostic_codes: SortedStrings = ()
    related_entry_ids: SortedEntryIds = ()
    #: Other names the same construct goes by. ``merge`` is an alias of the
    #: ``joins`` capability, not a second row: one construct with two rows is
    #: one construct whose state drifts.
    display_aliases: SortedStrings = ()

    doctrine_selector: Optional[str] = None
    recipe_selector: Optional[str] = None

    sources: Annotated[Tuple[ProcessIRAuthoringSourceV1, ...], AfterValidator(_sorted_unique_models)] = Field(..., min_length=1)


class ProcessIRAuthoringFacetsV1(_ContractModel):
    """Every filterable value in the catalog, so a caller can filter blind.

    Returned even on a bare retrieval that carries zero entries: discovering
    what you may ask for must not require already having asked for something.
    """

    entry_types: SortedStrings = ()
    categories: SortedStrings = ()
    node_kinds: SortedStrings = ()
    capability_ids: SortedStrings = ()
    workflow_stages: SortedStrings = ()


class ProcessIRAuthoringQueryV1(_ContractModel):
    """The query this page answers, echoed back verbatim.

    Echoed so a paged result is self-describing: a caller resuming from
    ``next_after_entry_id`` can reconstruct the request without keeping state.
    """

    authoring_entry_id: Optional[str] = None
    node_kind: Optional[str] = None
    category: Optional[str] = None
    capability_id: Optional[str] = None
    workflow_stage: Optional[str] = None
    after_entry_id: Optional[str] = None
    limit: StrictInt = PROCESS_IR_AUTHORING_DEFAULT_LIMIT


class ProcessIRAuthoringContractPageV1(_ContractModel):
    """One bounded page of the authoring contract.

    ``catalog_entry_count`` (everything), ``matched_entry_count`` (everything
    the filter selected) and ``returned_entry_count`` (what is in this page) are
    three different numbers and all three are published. A caller who sees only
    the third cannot tell a narrow filter from a truncated page.

    The two ``unlisted_*_state`` fields publish the ALLOWLIST RULE once. The
    underlying registries are closed — a placement or a connector action that is
    not listed is rejected — so enumerating the complement would mean shipping a
    combinatorial table that silently goes wrong every time a node kind is
    added. One field states the rule instead.
    """

    contract_version: Literal["1"] = "1"
    state_mappings: Annotated[Tuple[ProcessIRAuthoringStateMappingV1, ...], AfterValidator(_sorted_unique_models)] = ()
    unlisted_placement_state: Literal["unsupported"] = "unsupported"
    unlisted_connector_action_state: Literal["unsupported"] = "unsupported"

    query: ProcessIRAuthoringQueryV1
    catalog_entry_count: StrictInt
    matched_entry_count: StrictInt
    returned_entry_count: StrictInt
    limit: StrictInt
    truncated: StrictBool = False
    next_after_entry_id: Optional[ContractEntryId] = None

    facets: ProcessIRAuthoringFacetsV1
    #: NOT re-sorted. Order is meaningful here and nowhere else in this model:
    #: the contract promises results in ``contract_entry_id`` order, and the
    #: cursor pages through that order. Applying the generic set-sorter re-ordered
    #: a page by serialized JSON and silently broke pagination — the one tuple
    #: where "order is never meaningful" is false.
    entries: Tuple[ProcessIRAuthoringContractEntryV1, ...] = ()


def process_ir_authoring_contract_v1_json_schema() -> Dict[str, Any]:
    """The JSON Schema for one served page."""
    return ProcessIRAuthoringContractPageV1.model_json_schema()


def canonical_process_ir_authoring_json(page: ProcessIRAuthoringContractPageV1) -> str:
    """Canonical JSON for a page: defaults expanded, keys sorted, compact."""
    return json.dumps(
        page.model_dump(mode="json"),
        sort_keys=True,
        separators=(",", ":"),
        ensure_ascii=True,
    )


def canonical_entry_json(entry: ProcessIRAuthoringContractEntryV1) -> str:
    """Canonical JSON for ONE entry — the unit the byte budget is measured in."""
    return json.dumps(
        entry.model_dump(mode="json"),
        sort_keys=True,
        separators=(",", ":"),
        ensure_ascii=True,
    )


__all__: List[str] = [
    "CANONICAL_CAPABILITY_STATES",
    "CONTRACT_ENTRY_ID_PATTERN",
    "PROCESS_IR_AUTHORING_BYTE_BUDGET",
    "PROCESS_IR_AUTHORING_CONTRACT_VERSION",
    "PROCESS_IR_AUTHORING_DEFAULT_LIMIT",
    "PROCESS_IR_AUTHORING_ENTRY_TYPES",
    "PROCESS_IR_AUTHORING_MAX_LIMIT",
    "PROCESS_IR_AUTHORING_WORKFLOW_STAGES",
    "ProcessIRAuthoringContractEntryV1",
    "ProcessIRAuthoringContractPageV1",
    "ProcessIRAuthoringFacetsV1",
    "ProcessIRAuthoringPlacementV1",
    "ProcessIRAuthoringQueryV1",
    "ProcessIRAuthoringReferenceV1",
    "ProcessIRAuthoringSourceV1",
    "ProcessIRAuthoringStateMappingV1",
    "ProcessIRDocumentSemanticsV1",
    "canonical_entry_json",
    "canonical_process_ir_authoring_json",
    "process_ir_authoring_contract_v1_json_schema",
]

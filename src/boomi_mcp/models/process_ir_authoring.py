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
    model_validator,
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
      captured platform behaviour, an ordering guarantee), so it cannot be
      computed from the source and is instead held by CI: its identity and its
      named source are asserted two-way, and its full served text is frozen in
      a committed snapshot, so any change to it lands in a diff a reviewer must
      approve. That is a REVIEW GATE on the wording, not a proof that the
      wording is true — no test can derive an English sentence from a registry.
      An earlier version of this description promised the stronger thing.

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


#: What a bracketed label in an entry's ``ordering_facts`` means, stated once
#: per page.
#:
#: A bare ``[compile]`` prefix read as "this is what a compile returns" — and it
#: is not: a real compile surfaces the PARSER's text too, because compile
#: re-runs parse. The label names the layer that AUTHORED the sentence; which
#: phase surfaces it is ``workflow_stages``, a different fact.
DIAGNOSTIC_LABEL_LEGEND = (
    "Each bracketed label in an entry's ordering_facts names the layer that "
    "AUTHORED that text (parser, semantic validator, compiler). Any phase that "
    "runs a layer can surface its text, so a compile can return "
    "parser-authored wording."
)


def _matches_the_registry(field: str):
    """Reject a page-level rule that disagrees with the registry it comes from.

    The three `Literal` rules are correct by construction: pydantic refuses any
    other value at every construction site, on every page, whether or not a
    test ever fetched that page. These three could not be `Literal` — they are
    computed — so for forty QA rounds they were guarded instead by a sampler
    over the page space, and every round found the next unsampled corner. The
    space is ~10^11; no fetch list closes it.

    A validator does. It runs on every page, so the value-space regress ends:
    a page whose mappings were cut, whose facets were narrowed to the matched
    set, or whose catalog count was replaced by the matched count is refused
    rather than served.

    The import is deliberately inside the function. At module scope it would be
    a cycle — the projection imports this module — and it would also drag the
    projection into `import boomi_mcp.models`, which a test forbids. In a
    function body it runs only at validation time, when the projection is
    already imported.
    """

    def canonical(value):
        # CONTENT, not order. This model sorts its own sequence fields, so the
        # served order legitimately differs from the registry's emission order;
        # comparing raw would reject every healthy page.
        if isinstance(value, (list, tuple)):
            return sorted(repr(item) for item in value)
        if isinstance(value, BaseModel):
            return {k: canonical(v) for k, v in value.__dict__.items()}
        return value

    def check(value):
        from ..authoring import process_ir_projection as projection

        expected = projection.expected_page_rule(field)
        if canonical(value) != canonical(expected):
            # "the DEFAULT registry", precisely. A page built from an injected
            # `sources` snapshot legitimately differs, and the old wording told
            # such a caller their value disagreed with the registry it came
            # from — which was measurably false, since it came from theirs.
            raise ValueError(
                f"{field} disagrees with the default registry; a page built "
                "from an injected sources snapshot cannot be served"
            )
        return value

    return check


def _entries_in_published_order(values):
    """Refuse a page whose entries are out of order or repeated.

    The contract promises `contract_entry_id` order and the cursor is a
    strictly-greater comparison against the last id, so a reversed page makes
    the cursor skip the rest of the result set. Nothing checked it: a reversed
    page, a duplicated entry, and a page whose length contradicted
    `returned_entry_count` all passed both re-validation hops.
    """
    ids = [entry.contract_entry_id for entry in values]
    if ids != sorted(ids):
        raise ValueError("page entries must be in contract_entry_id order")
    if len(set(ids)) != len(ids):
        raise ValueError("page entries must be unique by contract_entry_id")
    return values


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
    #: REQUIRED, deliberately. Pydantic does not run `AfterValidator` on a
    #: default, so a page that reached re-validation with this field DROPPED had
    #: its registry check skipped and was served with an empty mapping list —
    #: the one rule whose default is not also its correct value, and therefore
    #: the one where "a default repairs an omission" was false. `facets` and
    #: `catalog_entry_count` are required for the same reason and refuse the
    #: drop; this now does too. Both construction sites already pass it.
    state_mappings: Annotated[
        Annotated[Tuple[ProcessIRAuthoringStateMappingV1, ...], AfterValidator(_sorted_unique_models)],
        AfterValidator(_matches_the_registry("state_mappings")),
    ]
    unlisted_placement_state: Literal["unsupported"] = "unsupported"
    unlisted_connector_action_state: Literal["unsupported"] = "unsupported"

    #: The label legend, published ONCE per page rather than appended to every
    #: entry. Repeating identical text on every diagnostic entry spent roughly
    #: a fifth of the entry byte budget and pushed entries off a full
    #: diagnostic page — the exact figures move with the legend's wording and
    #: the diagnostic count, so they are measured by
    #: `test_the_label_legend_is_published_once_per_page_not_per_entry` rather
    #: than frozen into a comment that goes stale. The envelope already publishes
    #: page-level rules this way (see the two `unlisted_*_state` fields) and is
    #: excluded from the entry budget, and entries are never served outside it.
    #:
    #: The DEFAULT is the legend itself, exactly as the sibling rules default to
    #: their `Literal` value. Defaulting to `""` instead made the field the one
    #: page rule a construction site could forget, and one did: the empty-result
    #: early return served a blank legend while the six populated selectors
    #: served all 228 characters. A page rule that is correct by construction
    #: cannot be omitted by a caller that does not know it exists.
    #:
    #: And `Literal`, not `str`, for the same reason the two `unlisted_*` rules
    #: are: a page rule has exactly one correct value, so the type can say so.
    #: Over forty QA rounds the three `Literal` page rules produced no findings
    #: at all, while this field and `state_mappings` — the two that were not —
    #: re-opened repeatedly, each time because some sampler of the page space
    #: had a gap. The page space is ~10^11 and no fetch list closes it; the type
    #: closes it for this field in one line. Pydantic now rejects any other
    #: value at EVERY construction site, on every page, unsampled or not.
    diagnostic_label_legend: Literal[DIAGNOSTIC_LABEL_LEGEND] = (
        DIAGNOSTIC_LABEL_LEGEND
    )
    query: ProcessIRAuthoringQueryV1
    catalog_entry_count: Annotated[
        StrictInt,
        AfterValidator(_matches_the_registry("catalog_entry_count")),
    ]
    matched_entry_count: StrictInt
    returned_entry_count: StrictInt
    limit: StrictInt
    truncated: StrictBool = False
    next_after_entry_id: Optional[ContractEntryId] = None

    facets: Annotated[
        ProcessIRAuthoringFacetsV1,
        AfterValidator(_matches_the_registry("facets")),
    ]
    #: NOT re-sorted. Order is meaningful here and nowhere else in this model:
    #: the contract promises results in ``contract_entry_id`` order, and the
    #: cursor pages through that order. Applying the generic set-sorter re-ordered
    #: a page by serialized JSON and silently broke pagination — the one tuple
    #: where "order is never meaningful" is false.
    #:
    #: ASSERTED, though, rather than merely relied on. The validator below
    #: CHECKS the published order and refuses a violation; it does not sort.
    #: Sorting would launder a downstream permutation instead of refusing it,
    #: which is the whole point of re-validating a served page, and
    #: de-duplicating would desync `entries` from `returned_entry_count`. It is
    #: keyed on `contract_entry_id`, so it has nothing to do with the
    #: JSON-serialization sorter that broke pagination — and it is a no-op on
    #: every page the projector builds, because the projector already sorts.
    entries: Annotated[
        Tuple[ProcessIRAuthoringContractEntryV1, ...],
        AfterValidator(_entries_in_published_order),
    ] = ()

    @model_validator(mode="after")
    def _page_counts_and_cursor_agree(self):
        """The three published numbers and the cursor must describe THIS page.

        Same hole as the order check, one field over: nothing cross-checked
        `returned_entry_count` against the entries actually carried, so a page
        could report a count it did not contain, and `next_after_entry_id`
        could name an entry that was not the last one — which is precisely the
        value a caller feeds back as the cursor.
        """
        if self.returned_entry_count != len(self.entries):
            raise ValueError(
                "returned_entry_count must equal the number of entries carried"
            )
        if self.truncated and self.entries:
            last = self.entries[-1].contract_entry_id
            if self.next_after_entry_id != last:
                raise ValueError(
                    "next_after_entry_id must be the last entry on a truncated page"
                )
        return self


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

"""State lineage over the CFG: what is established where (#143). DARK in slice 4.

The state model
---------------
Two scopes, and conflating them is the defect this module exists to prevent:

``ddp`` — dynamic DOCUMENT property. Travels WITH a document. Every Branch leg
    receives its own copies, so a DDP written inside leg 0 does not exist on
    leg 1's documents. Pre-Branch writes DO reach every leg, because every leg's
    copies descend from the same pre-Branch documents.

``dpp`` / ``cache`` — execution-scoped. One store for the whole execution, so an
    earlier Branch leg's write IS visible to a later leg — legs run sequentially,
    not in parallel. This is the fact the issue calls out explicitly, and it is
    the opposite of what a naive "branches are independent" model would say.

Why unknown effects never establish state
-----------------------------------------
The legacy walker in ``cache_property_lineage`` treats a map or script as a
WILDCARD writer: it may satisfy any read. That is deliberately permissive and it
is why the issue says lineage there "sacrifices precision". This module inverts
the default: an undeclared map/script contributes *uncertainty*
(``…LINEAGE_EFFECT_UNKNOWN``), never proof. A typed contract — bound to the map
component, or to a script's language plus the SHA-256 of its exact source —
contributes exact reads and writes.

That inversion is the whole point, so it is worth being blunt about the
trade-off: strict ProcessIR validation will reject some payloads the legacy
walker accepted. Those cases are exactly what the named, registry-owned
``LEGACY_ADAPTER_EXEMPTION_*`` advisories in slice 7 cover — the legacy surface
keeps its behavior, and the exemption is recorded rather than silently applied.

This module does NOT import ``cache_property_lineage``. The legacy walker stays
adapter-only per the migration matrix; importing it would drag the wildcard
default back in through the side door.
"""

from __future__ import annotations

from types import MappingProxyType
from typing import Any, Dict, FrozenSet, List, Mapping, NamedTuple, Optional, Set, Tuple

from ....errors import (
    PROCESS_IR_CAPABILITY_ENTRY_CONTEXT_UNSUPPORTED,
    PROCESS_IR_CAPABILITY_PROCESS_CALL_PLACEMENT_UNSUPPORTED,
    PROCESS_IR_COMPILE_INTERNAL,
    PROCESS_IR_SEMANTIC_DYNAMIC_PATH_DDP_NOT_ESTABLISHED,
    PROCESS_IR_SEMANTIC_DYNAMIC_PATH_NO_DYNAMIC_SEGMENT,
    PROCESS_IR_SEMANTIC_DYNAMIC_PATH_PROFILE_BINDING_MISMATCH,
    PROCESS_IR_SEMANTIC_LINEAGE_BRANCH_ORDER_INVALID,
    PROCESS_IR_SEMANTIC_LINEAGE_CACHE_WRITER_MISSING,
    PROCESS_IR_SEMANTIC_LINEAGE_DDP_SCOPE_INVALID,
    PROCESS_IR_SEMANTIC_LINEAGE_EFFECT_UNKNOWN,
    PROCESS_IR_SEMANTIC_LINEAGE_EXTERNAL_WRITER_ASSUMED,
    PROCESS_IR_SEMANTIC_LINEAGE_PROPERTY_READ_BEFORE_WRITE,
    PROCESS_IR_SEMANTIC_PROFILE_MISMATCH,
)
from ..connector_resolution import (
    MAP_COMPONENT_TYPE,
    _canonical_type,
    _profile_identity as _resolved_profile_identity,
    resolve_connector_call_bindings,
)
from ....models.process_ir import process_call_prefix_admitted
from ....models.process_ir_document_semantics import (
    TRIGGERED_REPLACEMENT_SEMANTIC_KINDS,
    ZERO_EMISSION_SEMANTIC_KINDS,
)
from ..diagnostics import ProcessIRCompileError, diagnostic
from .contracts import (
    DEFAULT_VALIDATION_CAPABILITIES,
    ProcessIRValidationCapabilitiesV1,
    StateEffectV1,
    ValidationDiagnosticV1,
)
from .context import (
    PreparedProcessValidationV1,
    canonical_cache_capabilities,
    canonical_cache_profiles,
    canonical_cache_refs,
)
from .findings import finding

_LINEAGE_PHASE = "lineage"
#: #184: the stream-profile proof reports #140's profile code, under the phase the
#: flow collector already files that code under, so the two sort together.
_PROFILE_PHASE = "profile"
#: #184 amendment 3 §8: a call's admission against its child's entry contract.
_CAPABILITY_PHASE = "capability"

#: Scope tokens. Kept as plain strings (not an enum) because they are also
#: evidence values, and the evidence vocabulary is lowercase tokens.
DDP = "ddp"
DPP = "dpp"
CACHE = "cache"

#: A key identifying one piece of state: its scope plus its name.
StateKey = Tuple[str, str]

#: ``(cache_ref, profile identity or None)``: one thing a document cache may hold.
CacheContentFact = Tuple[str, Optional[Tuple[str, str]]]

#: #184 amendment 3 §7. A writer TOKEN: the id of the authored property node that
#: wrote a key, or ``UNKNOWN_WRITER`` when nothing validated establishes who wrote it
#: (a caller's entry declaration, a contract, an opaque step, an external writer).
UNKNOWN_WRITER = "unknown-writer"
#: How many documents a path is PROVED to carry. Only ``one`` licenses the attested
#: one-current/one-cached overlay; anything else is ``unknown`` and fails closed.
COUNT_ONE = "one"
COUNT_UNKNOWN = "unknown"
#: Carried in a path's ``invalidated`` set once a triggered cache read ran on it. The
#: documents past that read are the cached ones, so a document property the read does
#: not guarantee lacks TRANSFER proof there — a read-before-write on the reader's own
#: path, never a different-copy scope error, even when the only write sits in a
#: sibling leg or on a cached document a whole-cache removal later cleared. Its name
#: is ``None``, which no authored property can spell.
CACHE_TRANSFER_UNPROVED = (DDP, None)
#: #184 amendment 3 §8. The writer token of a document property a CALLED passthrough
#: child's caller composes. Its record is ``None``: that writer is checked at every
#: call, against this child's binding, and never here.
CALLER_WRITER = "caller-writer"


class _InheritedBinding(NamedTuple):
    """A passthrough child's bound request path, as a caller checks it (#184 amendment 3 §8)."""

    property_name: str
    request_profile_ref: Optional[str]


def _authored_at(ir, pointer: str):
    """The authored node a CFG ``source_path`` names, or None when it names none."""
    current = ir
    for token in pointer.split("/")[1:]:
        try:
            if isinstance(current, (list, tuple)):
                current = current[int(token)]
            else:
                current = getattr(current, token)
        except (AttributeError, IndexError, ValueError, TypeError):
            return None
    return current


def _call_prefix(ir, call_path: str) -> Tuple[Optional[str], Optional[str]]:
    """``(body context, direct predecessor kind)`` of the steps before a terminal call.

    ``(None, None)`` when the call's OWN body authors no step before it. Only that
    body is a prefix: a call whose body is empty keeps the legacy empty-prefix
    placement even under a Branch or Decision that follows native work (amendment 1
    rule 4; the #141 capture attests ``decision -> true -> processcall`` after a leg
    step). The interposition erases no obligation: the child's contract is still
    discharged at the call (rule 3).
    """
    if not call_path.endswith("/terminal"):
        return None, None
    body_path = call_path[: -len("/terminal")]
    tokens = body_path.split("/")
    if len(tokens) >= 2 and tokens[-2] == "legs":
        context = "branch_leg"
    elif tokens[-1] == "true_arm":
        context = "decision_true_arm"
    else:
        return None, None
    steps = list(getattr(_authored_at(ir, body_path), "steps", ()) or ())
    if not steps:
        return None, None
    return context, getattr(steps[-1], "kind", None)


class _Cohort(NamedTuple):
    """What the documents ONE executing Add to Cache stored carry (#184 amendment 3 §7).

    Frozen when the write runs, because a cached document keeps the properties it
    had then. ``possible`` is ``None`` when something on the writing path could have
    set a property nothing here names, and ``alternatives`` records, per key, the
    writer tokens of the ORIGINAL cached document. They are never re-evaluated
    against another document.
    """

    guaranteed: FrozenSet[StateKey]
    possible: Optional[FrozenSet[StateKey]]
    alternatives: FrozenSet[Tuple[StateKey, str]]
    count: str


#: A cache write nothing here can inspect: a contract, a child, an outside writer.
UNKNOWN_COHORT = _Cohort(frozenset(), None, frozenset(), COUNT_UNKNOWN)


# ---------------------------------------------------------------------------
# #184 D2: the profile of the documents on one path
# ---------------------------------------------------------------------------
#
# Carried per path beside the property lattice, never merged: control nodes end
# the path they sit on, so a Branch leg or Decision arm inherits the stream that
# reached the control and nothing flows back out of it. USABLE PAYLOAD is a
# separate fact owned by connector resolution (`_walk_paths.producer`); the states
# here are chosen so the two agree — the three no-producer states below are
# exactly the points at which that walk has no producer.
#
# #184 amendment 3 separates that from the TRIGGER. The two entry states still
# deliver one document that makes the next step run; only `absent` delivers
# nothing at all. What each step hands on is read from the document-emission
# authority.

#: The single empty document a scheduled (No Data) start supplies. It can trigger
#: one execution of what follows, but it carries no payload: nothing can be read
#: out of it by profile.
STREAM_EMPTY_ENTRY = "empty_entry"
#: That same document after a per-document step (a Message, a Data Process)
#: rewrote it without any from-nothing producer running. Still no producer, but
#: its content is no longer provably empty.
STREAM_TOUCHED_ENTRY = "touched_entry"
#: No documents at all, not even a trigger: a cache write or an all-document
#: removal handed on none, or a call returned none.
STREAM_ABSENT = "absent"
#: Documents of one resolved profile identity, with the node that established it.
STREAM_KNOWN = "known"
#: Documents whose profile nothing proves.
STREAM_UNKNOWN = "unknown"
#: A Data Passthrough root's entry: its caller's documents, as one group (#184
#: amendment 1). Documents exist, but their profile is whatever each CALLER hands
#: over. A consumer here therefore does not fail against it; it records what the
#: child REQUIRES of its callers (``LineageWalkV1.entry_requirements``), and each
#: call site discharges that requirement. ``identity`` holds the first requirement
#: recorded on the path, so a later consumer naming another profile on the same
#: untouched documents is a contradiction inside the child itself.
STREAM_CALLER_ENTRY = "caller_entry"

#: Stream states in which no from-nothing producer has run on the path.
NO_PRODUCER_STREAMS: FrozenSet[str] = frozenset(
    {STREAM_EMPTY_ENTRY, STREAM_TOUCHED_ENTRY, STREAM_ABSENT}
)


class _Stream(NamedTuple):
    state: str
    #: ``(component id, profile type)`` when ``state`` is known, else None.
    identity: Optional[Tuple[str, str]] = None
    #: ``call``, ``listener``, ``map`` or ``cache`` for a known stream; a reason
    #: token (``undeclared``, ``opaque``, ``legacy``, ``cache``, ``catch``) otherwise.
    origin: Optional[str] = None
    #: The CFG node that established a known stream — a map's own pointer is
    #: where a mismatch with the call it feeds is reported.
    origin_node: object = None
    #: #184 amendment 3 §7: ``one`` only when this path provably carries exactly one
    #: document. Set by the controller after every step, never inferred from a
    #: profile, a connector operation or a sample run.
    count: str = COUNT_UNKNOWN
    #: #184 amendment 3 §7: True when a step on this path could have set a document
    #: property nothing in this process names (an uncontracted map or script, or
    #: documents a caller handed over), so a cached copy's property set is not known.
    properties_unknown: bool = False
    #: #184 amendment 1 rule 6: the cache these documents were read from when no write
    #: in this process reached the read, so only a caller can have filled it. A
    #: consumer of them records what it needs of that cache instead of a caller entry.
    caller_cache: Optional[str] = None


#: The visibility model this module ENFORCES, stated once as data (#146).
#:
#: LOAD-BEARING, not documentation: ``_State`` reads ``lifetime`` from here to
#: decide which compartment a write lands in, so the served contract and the
#: traversal cannot disagree. An earlier draft kept this as a separate table
#: beside a hard-coded ``key[0] == DDP`` test, which is precisely the
#: second-copy-of-one-rule shape #146 exists to remove — the copy would go stale
#: the first time the traversal changed and nothing would fail.
#:
#: ``processproperty`` is deliberately ABSENT. A process property is a
#: component-backed deploy-time value, not execution state this module tracks,
#: and asserting it here would be a false claim of ownership. Its scope
#: descriptor lives with the model that does own it
#: (``models.cache_property_models.PROCESS_PROPERTY_SCOPE_V1``).
STATE_VISIBILITY_V1: "Mapping[str, Mapping[str, object]]" = MappingProxyType(
    {
        DDP: MappingProxyType(
            {
                "scope": "document_copy",
                "lifetime": "document",
                # Each Branch leg gets its own copy of the SAME pre-Branch
                # documents, so a value written before the Branch is on every
                # copy — but a value written INSIDE one leg is on that leg's
                # copies only.
                "survives_branch_path_entry": True,
                "visible_across_sibling_paths": False,
                "convergence": "intersection",
                "read_before_write": "rejected",
            }
        ),
        DPP: MappingProxyType(
            {
                "scope": "execution",
                "lifetime": "execution",
                "survives_branch_path_entry": True,
                # The asymmetry that makes leg ORDER matter: execution state
                # accumulates across legs, so leg 1 sees what leg 0 wrote.
                "visible_across_sibling_paths": True,
                "convergence": "intersection",
                "read_before_write": "rejected",
            }
        ),
        CACHE: MappingProxyType(
            {
                "scope": "execution",
                "lifetime": "execution",
                "survives_branch_path_entry": True,
                "visible_across_sibling_paths": True,
                "convergence": "intersection",
                # A cache may legitimately be populated outside this process,
                # but a DECLARATION alone never buys that: the node must author
                # `external_writer` AND a verified capability must vouch for the
                # writer. The served token said "declared", which described a
                # free-form trust assertion this module deliberately refuses.
                "read_before_write": (
                    "rejected_unless_external_writer_authored_and_verified"
                ),
            }
        ),
    }
)

#: Scopes whose state lives on the DOCUMENT copy rather than the execution.
#: Derived from the descriptor above so there is exactly one statement of it.
_DOCUMENT_LIFETIME_SCOPES: FrozenSet[str] = frozenset(
    scope for scope, row in STATE_VISIBILITY_V1.items() if row["lifetime"] == "document"
)


def state_visibility_rows() -> Tuple["Mapping[str, object]", ...]:
    """The visibility model as sorted public data, for the #146 projection."""
    return tuple(
        MappingProxyType({"state_scope": scope, **dict(row)})
        for scope, row in sorted(STATE_VISIBILITY_V1.items())
    )


class _State:
    """What is established at one program point.

    ``document`` holds keys whose lifetime is the document copy (DDP), which are
    discarded when documents are re-copied into Branch legs. ``execution`` holds
    DPP and cache keys, which are not. Which compartment a scope uses is read
    from :data:`STATE_VISIBILITY_V1`, not restated here.
    """

    __slots__ = ("document", "execution", "content", "cohorts")

    def __init__(
        self,
        document: Optional[FrozenSet[StateKey]] = None,
        execution: Optional[FrozenSet[StateKey]] = None,
        content: "Optional[FrozenSet[CacheContentFact]]" = None,
        cohorts: "Optional[FrozenSet[Tuple[str, _Cohort]]]" = None,
    ) -> None:
        self.document: FrozenSet[StateKey] = document or frozenset()
        self.execution: FrozenSet[StateKey] = execution or frozenset()
        #: #184 D5: what each document cache MAY contain, as
        #: ``(cache_ref, profile identity)`` pairs, where an identity of ``None``
        #: marks content of unknown profile. Execution-scoped like the cache
        #: itself, but a MAY set, not a must set: Add to Cache appends, so every
        #: write adds and nothing but a whole-cache removal takes away. It
        #: therefore converges by UNION — the opposite of the two compartments
        #: above — because a write on one Decision arm may have happened, and a
        #: read after the merge must not claim that write's profile is absent.
        self.content: "FrozenSet[CacheContentFact]" = content or frozenset()
        #: #184 amendment 3 §7: the property cohorts each cache MAY hold. A MAY set
        #: exactly like ``content`` — Add to Cache appends, whole-cache removal
        #: clears, convergence unions — and read by the retrieve overlay.
        self.cohorts: "FrozenSet[Tuple[str, _Cohort]]" = cohorts or frozenset()

    def with_write(self, key: StateKey) -> "_State":
        if key[0] in _DOCUMENT_LIFETIME_SCOPES:
            return _State(self.document | {key}, self.execution, self.content, self.cohorts)
        return _State(self.document, self.execution | {key}, self.content, self.cohorts)

    def with_content(self, cache_ref: str, identity) -> "_State":
        """This cache may now also hold documents of ``identity`` (None: unknown)."""
        return _State(
            self.document, self.execution, self.content | {(cache_ref, identity)}, self.cohorts
        )

    def with_cohort(self, cache_ref: str, cohort: "_Cohort") -> "_State":
        """This cache may now also hold documents carrying ``cohort``'s properties."""
        return _State(self.document, self.execution, self.content, self.cohorts | {(cache_ref, cohort)})

    def cohorts_of(self, cache_ref: str) -> "FrozenSet[_Cohort]":
        return frozenset(fact[1] for fact in self.cohorts if fact[0] == cache_ref)

    def without_content(self, cache_ref: str) -> "_State":
        """A whole-cache removal: nothing written before it is still there."""
        return _State(
            self.document,
            self.execution,
            frozenset(fact for fact in self.content if fact[0] != cache_ref),
            frozenset(fact for fact in self.cohorts if fact[0] != cache_ref),
        )

    def content_of(self, cache_ref: str) -> "FrozenSet[Optional[Tuple[str, str]]]":
        return frozenset(fact[1] for fact in self.content if fact[0] == cache_ref)

    def establishes(self, key: StateKey) -> bool:
        compartment = (
            self.document if key[0] in _DOCUMENT_LIFETIME_SCOPES else self.execution
        )
        return key in compartment

    def entering_branch_leg(self) -> "_State":
        """State a Branch leg starts from.

        Document state SURVIVES: each leg gets its own copies of the SAME
        pre-Branch documents, so a DDP written before the branch is present on
        every copy. Execution state survives too, and additionally accumulates
        across legs — see ``collect_lineage_findings``.
        """
        return _State(self.document, self.execution, self.content, self.cohorts)

    def merged_with(self, other: "_State") -> "_State":
        """Meet over converging paths: only what BOTH establish survives.

        Intersection, not union. A Decision writes a property on the true arm
        only; after the merge that property is established on one path and not
        the other, so it is not established at all. Union here would be the
        single easiest way to make this whole module unsound.

        Cache CONTENT is the exception and merges by union (see ``content``):
        it records what a read may hand on, and dropping a possibility is the
        unsound direction there.
        """
        return _State(
            self.document & other.document,
            self.execution & other.execution,
            self.content | other.content,
            self.cohorts | other.cohorts,
        )


#: Wire prefixes that identify a tracked property's SCOPE. A Decision operand
#: carries a fully-qualified ``property_id`` rather than a bare name plus a
#: scope field, so the scope has to be read off the prefix. Assuming DPP here
#: would misclassify every ``dynamicdocument.*`` operand and produce confident,
#: wrong DDP diagnostics.
_DDP_PROPERTY_PREFIX = "dynamicdocument."
_DPP_PROPERTY_PREFIX = "process."


def _tracked_property_key(property_id: str, fallback_name) -> StateKey:
    if property_id.startswith(_DDP_PROPERTY_PREFIX):
        return (DDP, property_id[len(_DDP_PROPERTY_PREFIX) :])
    if property_id.startswith(_DPP_PROPERTY_PREFIX):
        return (DPP, property_id[len(_DPP_PROPERTY_PREFIX) :])
    return (DPP, fallback_name or property_id)


def _reads_of(semantic) -> Tuple[Tuple[StateKey, bool, bool], ...]:
    """``((scope, name), has_default, strict)`` triples a node reads.

    ``has_default`` — a read with a default cannot fail, because the default
    establishes the value. Treating a defaulted read as a hard dependency is how
    a validator rejects a payload that runs perfectly well.

    ``strict`` — whether MERE ABSENCE of a writer is a defect. Decision operands
    are deliberately NON-strict, and this is not a concession: they emit
    ``defaultValue=""`` on the wire, so an unwritten property is a well-defined
    empty string at runtime, not an error. The legacy walker encodes the same
    rule (``cache_property_lineage.LineageEvent.strict``), and the shipped
    ``control_flow`` golden depends on it — its router reads
    ``dynamicdocument.DDP_S`` that nothing writes.

    A non-strict read still fails when a writer EXISTS but is provably invisible
    (wrong document copy, later Branch leg). That case is a real authoring
    mistake: the author clearly intended the value to come from that write.
    """
    reads: List[Tuple[StateKey, bool, bool]] = []
    kind = semantic.semantic_kind

    if kind == "set_property":
        for source in semantic.source_values:
            value_type = getattr(source, "value_type", None)
            if value_type in (DDP, DPP):
                reads.append(
                    (
                        (value_type, source.property_name),
                        getattr(source, "default_value", None) is not None,
                        True,
                    )
                )
    elif kind == "decision":
        for operand in (semantic.left, semantic.right):
            if getattr(operand, "value_type", None) == "track":
                key = _tracked_property_key(
                    operand.property_id, getattr(operand, "property_name", None)
                )
                reads.append(
                    (key, getattr(operand, "default_value", None) is not None, False)
                )
    elif kind in ("cache_get", "document_cache_retrieve"):
        reads.append(((CACHE, semantic.cache_ref), False, True))

    return tuple(reads)


#: Semantic kinds that hand downstream steps DIFFERENT documents than they
#: received (#155). ONE authority, stated as data.
#:
#: LOAD-BEARING, exactly as :data:`STATE_VISIBILITY_V1` above is: the dynamic-path
#: rule reads this and nothing else, and
#: ``test_the_stream_replacing_authority_matches_the_served_contract`` asserts it
#: equals the set the served authoring contract publishes as
#: ``output_documents == "stream_replacing"``, in BOTH directions. So the rule and
#: the published answer cannot disagree — a kind added to one and not the other
#: fails that test rather than silently changing behaviour.
#:
#: Membership is UNQUALIFIED, and that is deliberate. The first version of this
#: rule qualified two entries — ``data_process`` only for split/combine, the cache
#: reads only for an all-documents load — and QA measured both qualifications
#: wrong: ``CacheGetSemanticV1`` carries no ``load_all_documents`` field at all, so
#: that branch was dead and the canonical cache read fell through fail-OPEN; and a
#: ``data_process`` running only a custom script can emit different documents just
#: as a split can. The published contract states the kinds unqualified; a
#: qualification is a second model of the same fact, and it was wrong twice in one
#: change.
DOCUMENT_STREAM_REPLACING_KINDS: FrozenSet[str] = frozenset(
    {"data_process", "message", "cache_get", "document_cache_retrieve"}
)


#: What the PLATFORM does to a document property at each step that replaces the
#: document stream — MEASURED, and keyed by `(semantic kind, step operation)`
#: because the answer is not a property of the node kind.
#:
#: The bound-path rule used to key on :data:`DOCUMENT_STREAM_REPLACING_KINDS`,
#: which answers a DIFFERENT question: does this step replace the document
#: CONTENT. That is the served contract's own question and is correctly answered
#: there. Whether the documents keep their PROPERTIES is a separate fact, and the
#: two sets differ — a Message replaces the stream and keeps the properties. The
#: rule was refusing documents the platform runs.
#:
#: Measured live before the account roll (capture `cap155-r17-ddp-survival`),
#: read off the WIRE rather than out of the emitted graph: every cell ends in a
#: connector call whose path is bound to the property, so "survives" means the
#: platform issued the request with the value in the path. One invariant held in
#: all six cells — survives iff two outbound documents iff the bound call issued
#: a request; when the property is lost the call emits nothing at all.
#:
#: `document_cache_retrieve` is not inferred from `cache_get`: both kinds lower
#: to the SAME pair of platform steps, so they cannot differ.
PROPERTY_SURVIVAL_V1: Mapping[Tuple[str, Optional[str]], str] = MappingProxyType({
    ("message", None): "survives",
    # #184 amendment 3 §7 replaced the two "lost" read cells. The r17 capture that
    # measured them wired the cache load straight into the retrieve, so the retrieve
    # never ran (ledger row E0-184-02). Measured with a separately triggered
    # retrieve (capture `cap184-retrieve-ddp-replacement` R1–R4): the retrieved
    # document carries the cached document's properties overlaid on the current
    # one's, cached winning a collision. The overlay (`_overlay_cache_read`) owns
    # these reads, bounded to one current and one cached document. N×M is OPEN.
    ("cache_get", None): "cache_overlay",
    ("document_cache_retrieve", None): "cache_overlay",
    # Relabelled from "lost": the r17 split cell's capture never independently
    # proved that the split and its successor executed. Still fail-closed.
    ("data_process", "split_documents"): "unproved",
    # Measured to SURVIVE for the one script exercised — which stored a brand-new
    # stream with an empty properties object and kept the property anyway, so it
    # is not carried the obvious way. Whether another script can drop it is
    # unmeasured, so the rule treats it as not surviving: a wrong "survives"
    # sends a request to the wrong resource, a wrong "lost" refuses a document
    # that runs, and only one of those is silent.
    ("data_process", "custom_scripting"): "script_dependent",
    ("data_process", "combine_documents"): "unmeasured",
})

#: Only a MEASURED survival preserves the carried set.
_PROPERTIES_SURVIVE = "survives"
#: The cache reads' verdict: neither survival nor loss, but the bounded overlay.
_CACHE_OVERLAY = "cache_overlay"


def _replacing_step_operation(semantic) -> Optional[str]:
    """The step operation this kind's answer depends on, or None if it does not.

    A node whose steps disagree yields None, which lands on "not measured to
    survive" — the fail-closed side — without needing a case of its own.
    """
    steps = getattr(semantic, "steps", None) or ()
    operations = {getattr(step, "operation", None) for step in steps}
    return next(iter(operations)) if len(operations) == 1 else None


def _discards_document_properties(semantic) -> bool:
    """Do the documents leaving this step lose their document properties?

    The question the bound-path rule needs, as opposed to the adjacent one it
    used to ask. Anything not measured to survive discards, so a kind or
    operation nobody has measured is refused rather than trusted.
    """
    if not _replaces_document_stream(semantic):
        return False
    verdict = PROPERTY_SURVIVAL_V1.get(
        (semantic.semantic_kind, _replacing_step_operation(semantic))
    )
    if verdict == _CACHE_OVERLAY:
        # Not a discard: `_overlay_cache_read` decides what the retrieved documents carry.
        return False
    return verdict != _PROPERTIES_SURVIVE


def _drop_replaced_document_keys(state: "_State", keep) -> "Tuple[_State, FrozenSet[StateKey]]":
    """The lattice state after a step whose documents do not carry the old properties (#184 A5).

    Returns ``(state without the dropped document-scoped keys, the dropped DDP keys)``.
    ``keep`` is what the replacing node itself established on the documents it emits,
    which survives its own replacement — the same exception the carried set applies.

    Before #184 only the reaching-writer map and the carried set were emptied at such
    a step; the general lattice was left alone, so an ORDINARY read of a property the
    new documents never carried validated silently and only a bound request path was
    refused. Which steps invalidate is not decided here: the caller asks
    ``_discards_document_properties``, which reads the measured property-survival
    table. A named function rather than inline code so the drop has one definition a
    test can disable to prove it is load-bearing.
    """
    dropped = frozenset(key for key in state.document if key not in keep)
    return (
        _State(state.document - dropped, state.execution, state.content, state.cohorts),
        frozenset(key for key in dropped if key[0] == DDP),
    )


def _replaces_document_stream(semantic) -> bool:
    """Does this node hand downstream steps DIFFERENT documents than it received? (#155)

    A dynamic-path binding promises that the writer composing the path wrote it on
    the SAME document the call then sends. A step that replaces the stream breaks
    that promise silently: the new documents carry no per-document property the old
    ones had, so the request path resolves empty and addresses the wrong resource.

    Consulted ONLY by the dynamic-path rule, never by ``_State``: the general
    lineage model's treatment of document replacement is #154's and is not changed
    here, because widening it would move every DDP read in the repo rather than the
    one case whose consequence is a wrong request URL.
    """
    return semantic.semantic_kind in DOCUMENT_STREAM_REPLACING_KINDS


#: Steps that hand on exactly the documents they received, one for one. A path's
#: proved count survives them; every other step makes it unknown.
_COUNT_PRESERVING_KINDS = frozenset({
    "set_property", "message", "map", "flow_control", "notify",
    "branch", "decision", "try_catch", "stop", "return_documents", "exception",
})


def _cohort_at_write(on_documents, writers, stream) -> "_Cohort":
    """The property cohort an executing Add to Cache stores (#184 amendment 3 §7)."""
    guaranteed = frozenset(key for key in on_documents if key[0] == DDP)
    named = guaranteed | frozenset(key for key in writers if key[0] == DDP)
    alternatives = frozenset(
        (key, token)
        for key in named
        for token in (writers.get(key) or (UNKNOWN_WRITER,))
    )
    return _Cohort(
        guaranteed=guaranteed,
        possible=None if stream.properties_unknown else named,
        alternatives=alternatives,
        count=stream.count,
    )


def _overlay_cache_read(semantic, state, writers, on_documents, invalidated, stream):
    """A TRIGGERED all-document retrieve, over every cohort the cache may hold (#184 amendment 3 §7).

    For the attested one-current/one-cached case the retrieved document is the cached
    payload, carrying the cached properties overlaid on the current ones: cached wins
    a collision, and a current-only name survives. Beyond that bound, measured only as
    1×1, a current property is NOT carried. Returns ``(state, writers, on_documents,
    invalidated, count)``.

    - Guarantees: the meet over every possible cohort. A current key joins only
      under the proved singleton.
    - Writer alternatives: definite cached presence selects the cached writers,
      definite absence selects the current ones, and possible presence keeps both
      plus unknown provenance. A bound path must pass for EVERY alternative.
    - Invalidation: every document property the path carried and the read does not
      guarantee, plus ``CACHE_TRANSFER_UNPROVED`` — an unmet read past a retrieve is
      missing transfer proof, never sibling-scope leakage.
    """
    cohorts = set(state.cohorts_of(semantic.cache_ref))
    if getattr(semantic, "external_writer", False):
        cohorts.add(UNKNOWN_COHORT)
    current = frozenset(key for key in on_documents if key[0] == DDP)
    singleton = (
        stream.count == COUNT_ONE
        and len(cohorts) == 1
        and next(iter(cohorts)).count == COUNT_ONE
    )
    carried_current = current if singleton else frozenset()
    if cohorts:
        guaranteed = frozenset.intersection(*(c.guaranteed | carried_current for c in cohorts))
    else:
        guaranteed = frozenset()

    def cached_tokens(cohort, key):
        tokens = {token for held, token in cohort.alternatives if held == key}
        return tokens or {UNKNOWN_WRITER}

    def current_tokens(key):
        return set(writers.get(key) or (UNKNOWN_WRITER,))

    carried_writers = {}
    for key in guaranteed:
        tokens = set()
        for cohort in cohorts:
            if key in cohort.guaranteed:
                tokens |= cached_tokens(cohort, key)
            elif cohort.possible is None or key in cohort.possible:
                # possible presence: the cached value may or may not override
                tokens |= cached_tokens(cohort, key) | current_tokens(key) | {UNKNOWN_WRITER}
            else:
                # definite cached absence: the current value survives the overlay
                tokens |= current_tokens(key)
        carried_writers[key] = tuple(sorted(tokens))

    writers = {key: value for key, value in writers.items() if key[0] != DDP}
    writers.update(carried_writers)
    before = current | frozenset(key for key in state.document if key[0] == DDP)
    state = _State(
        frozenset(key for key in state.document if key[0] != DDP) | guaranteed,
        state.execution,
        state.content,
        state.cohorts,
    )
    count = next(iter(cohorts)).count if len(cohorts) == 1 else COUNT_UNKNOWN
    invalidated = invalidated | (before - guaranteed) | {CACHE_TRANSFER_UNPROVED}
    return state, writers, guaranteed, invalidated, count


def _writes_of(semantic) -> Tuple[StateKey, ...]:
    """State keys a node definitely establishes."""
    kind = semantic.semantic_kind
    if kind == "set_property":
        return ((semantic.scope, semantic.name),)
    if kind == "cache_put":
        return ((CACHE, semantic.cache_ref),)
    return ()


def _trusted_effects(
    semantic, capabilities: ProcessIRValidationCapabilitiesV1
) -> Tuple[StateEffectV1, ...]:
    """Typed contracts covering this node, if the caller supplied any."""
    kind = semantic.semantic_kind
    found: List[StateEffectV1] = []
    if kind == "map":
        effect = capabilities.map_effect(semantic.map_ref)
        if effect is not None:
            found.append(effect)
    elif kind == "process_call":
        effect = capabilities.subprocess_effect(semantic.process_ref)
        if effect is not None:
            found.append(effect)
    elif kind == "data_process":
        for step in semantic.steps:
            if getattr(step, "operation", None) != "custom_scripting":
                continue
            effect = capabilities.script_effect(step.language, step.script)
            if effect is not None:
                found.append(effect)
    return tuple(found)


def _establishes_downstream(semantic) -> bool:
    """Whether this node's contract writes prove anything to a later reader.

    False for a fire-and-forget `process_call`: it may still be running. The
    predicate lives HERE, on its own, because THREE consumers ask the question
    and a rule applied to only one of them is not a rule.

    It first shipped inside the main traversal alone. `_leg_write_index` and
    `_written_anywhere` kept counting the async write, and because both feed the
    diagnostic CHOICE, a non-strict Decision operand — clean on its own, since an
    unwritten `track` operand is a defined empty string on the wire — became a
    BLOCKING `…BRANCH_ORDER_INVALID` the moment a summary was attached to an
    unrelated `wait=False` child. Adding a contract that establishes nothing
    turned a valid payload into a rejected one.
    """
    return not (
        semantic.semantic_kind == "process_call"
        and not getattr(semantic, "wait", True)
    )


def _nonstrict_read_can_fail(
    prepared: PreparedProcessValidationV1,
    key: StateKey,
    capabilities: ProcessIRValidationCapabilitiesV1,
) -> bool:
    """Whether a NON-strict read can fail at all, given who writes the key.

    A non-strict reader (a Decision operand) tolerates ABSENCE — the wire
    carries `defaultValue=""` — but not a writer that exists somewhere it can
    never see. Which writers count depends on the scope, and conflating the two
    is what made an async summary reject a valid payload:

    * **DDP** — a write on a DIFFERENT document copy can never reach this
      reader. That is not absence, it is a structural mistake, and it holds
      whether or not the writer is fire-and-forget. ANY write counts.
    * **DPP / cache** — execution-scoped. A fire-and-forget child may or may not
      have run, which is indistinguishable from absence, and absence is exactly
      what this reader tolerates. Only ESTABLISHING writes count.
    """
    if key[0] == DDP:
        return _written_anywhere(prepared, key, capabilities)
    return _established_anywhere(prepared, key, capabilities)


def _established_anywhere(
    prepared: PreparedProcessValidationV1,
    key: StateKey,
    capabilities: ProcessIRValidationCapabilitiesV1,
) -> bool:
    """Whether any node writes ``key`` in a way that can ESTABLISH it.

    Differs from `_written_anywhere` only in excluding a fire-and-forget
    `process_call`'s declared writes — it may still be running, so it proves
    nothing to a reader.
    """
    for node in prepared.cfg.nodes:
        if key in _writes_of(node.semantic):
            return True
        if not _establishes_downstream(node.semantic):
            continue
        for effect in _trusted_effects(node.semantic, capabilities):
            if key in [(k[0], k[1]) for k in effect.writes]:
                return True
    return False


def _opaque_reason(
    semantic, capabilities: ProcessIRValidationCapabilitiesV1
) -> Optional[str]:
    """Why a node's state effects are unknown, if they are.

    A node covered by a typed contract is NOT opaque — that is the entire point
    of the contract. Returned as a closed evidence token, never as the map ref
    or the script text.
    """
    kind = semantic.semantic_kind
    if kind == "map":
        return None if capabilities.map_effect(semantic.map_ref) else "map"
    if kind == "process_call":
        return None if capabilities.subprocess_effect(semantic.process_ref) else "subprocess"
    if kind == "data_process":
        for step in semantic.steps:
            if getattr(step, "operation", None) != "custom_scripting":
                continue
            if capabilities.script_effect(step.language, step.script) is None:
                return "script"
    return None


def _caches_a_call_may_write(cache_refs, contract) -> Tuple[str, ...]:
    """The caches one call may leave holding content this process cannot see (#184).

    A child whose cache writes are known names every cache it writes, its own children's
    included, and each is marked whether or not this process names it: a forwarding
    process that names no cache still hands a later call a cache an earlier call appended
    to (Stage-2 review round r2). Only a cache step writes a cache, so a child whose
    other effects are unknown still names them all. Any other child may write every cache
    this process can observe. Cache refs are canonical here, one spelling per component,
    so equality is identity.
    """
    if contract is not None and (contract.state_known or contract.cache_writes_known):
        return tuple(sorted({key[1] for key in contract.mutated_state if key[0] == CACHE}))
    return tuple(cache_refs)


class LineageWalkV1(NamedTuple):
    """Everything ONE lineage walk establishes about a process.

    The three fields answer three different questions off the SAME traversal,
    so a caller never has to re-derive path reachability with a scan of its
    own — the failure this type exists to make unavailable.

    ``unestablished_reads`` is a MAY set: a key read on ANY path before that
    path writes it. ``established_at_exit`` is a MUST set: the meet over
    converging paths, so a key written on only one Decision arm is absent.
    The two approximate in OPPOSITE directions on purpose — a caller-facing
    dependency must never be under-reported, and a guarantee must never be
    over-reported.
    """

    findings: Tuple[ValidationDiagnosticV1, ...]
    unestablished_reads: Tuple[StateKey, ...]
    established_at_exit: Tuple[StateKey, ...]
    #: #184: what a Data Passthrough root requires of the documents its callers
    #: hand over — one entry per consumer of those documents, in walk order, each a
    #: resolved ``(component id, profile type)`` or ``None`` for a consumption whose
    #: profile nothing states. Empty for every other entry form, and for a
    #: passthrough root whose paths all replace the caller's documents before
    #: consuming them. The child contract is derived from it; this walk only
    #: records it.
    entry_requirements: Tuple[Optional[Tuple[str, str]], ...] = ()
    #: #184 amendment 3 §8: the same consumers as the AUTHORED profile refs (None
    #: where nothing names one), which is what a child contract carries.
    entry_requirement_refs: Tuple[Optional[str], ...] = ()
    #: False when the stream-profile proof was skipped because a connector binding
    #: did not resolve: then neither requirement list is complete.
    profile_proof: bool = True
    #: #184 amendment 1 rule 6: ``(cache ref, AUTHORED profile ref or None)`` for each
    #: consumer of documents read from a cache no write in this process reached.
    cache_requirement_refs: Tuple[Tuple[str, Optional[str]], ...] = ()
    #: #184: ``(pointer, property name, request profile ref)`` for each bound request
    #: path the binding rule found no writer for: this process's own binding at its
    #: ``/path_binding``, or one a call inherits from its passthrough child at the call's
    #: ``/process_ref``. One row per PROPERTY where the diagnostic is one per pointer, so a
    #: caller obligation is derived per property, never by expanding a pointer to every
    #: binding behind it (Stage-2 review round r2).
    unestablished_bindings: Tuple[Tuple[str, str, Optional[str]], ...] = ()
    # #184 D12 withdrew ``truncated``. The walk had a depth bound of 256, and a
    # caller trusting the state sets had to treat a walk that hit it as no
    # answer. The controller is now iterative with no depth bound: every node of
    # the tree is visited exactly once, so both sets are always exact and there is
    # no partial walk left to report.


#: Exit roles that NEVER end the process normally. Everything else does.
#:
#: Stated as the EXCLUSION, not the inclusion, because the two fail in opposite
#: directions and only one is safe. Fewer exits in a MEET means a LARGER result
#: — an over-claim — so an inclusion list that forgets a role silently promises
#: writes a path never makes. That is exactly how a routed `target` and then a
#: staging `cache_put` were each missed: both complete normally, neither was
#: listed. An exclusion list fails the other way: a role added to the compiler
#: and not classified here is treated as a normal exit, which can only shrink
#: the guarantee.
#:
#: `exception` ends abnormally, so what it wrote promises nobody anything.
#: `process_call` ends the path here but the CALLED process decides what
#: follows, so this process guarantees nothing at that point.
#: `test_the_exit_role_partition_is_total` pins this against `CfgExitRoleV1`.
_ABNORMAL_EXIT_ROLES = frozenset({"exception", "process_call"})



def _walk_lineage(
    prepared: PreparedProcessValidationV1,
    capabilities: ProcessIRValidationCapabilitiesV1 = DEFAULT_VALIDATION_CAPABILITIES,
) -> LineageWalkV1:
    """Walk the CFG, tracking established state, and report unproven reads.

    One ORDERED controller (#184 D12), iterative rather than recursive, with no
    depth bound. Branch legs run in authored order carrying execution state,
    Decision arms meet, and a try_catch returns its scope-entry state with the
    union of both bodies' cache content. The CFG it walks is an invariant-checked
    tree, so each node is visited exactly once. A visit count above the node count
    can only mean a graph that is not a tree reached this walk; that is a compiler
    defect and is raised as one rather than looping.

    The recursive visit it replaced stopped at depth 256 and reported the walk as
    truncated. Its callers then had to treat both state sets as no answer, and a
    late map past the cutoff went unchecked — which mattered once the stream
    profile proof moved into this walk, because the connector walk that used to
    check maps had no depth bound at all.
    """
    findings: List[ValidationDiagnosticV1] = []
    reported: Set[Tuple[str, str, str]] = set()
    # Every unestablished read, recorded BEFORE `_report` dedups by
    # (code, node): two different keys unmet at one node collapse to a single
    # finding, and dropping the second key with the duplicate finding would
    # under-report the dependency set.
    unmet: List[StateKey] = []
    # State at every NORMAL exit — a path ending on `stop` or `return_documents`.
    # A guarantee is what holds however the process finishes, so it is the meet
    # over these, not the state the traversal happens to carry back. An
    # `exception` terminal is excluded: it ends the process abnormally, so what
    # it had written is no promise to anyone. A terminal-less path end (a Branch
    # leg, a cache-staging leg) is not a process exit at all.
    normal_exits: List[_State] = []
    #: Path ends that THREW. A Decision arm that only throws does not carry its
    #: state forward — nothing downstream of the Decision runs for that
    #: document — so meeting it into the continuation drops whatever the other
    #: arm established for everything that DOES continue.
    threw: List[str] = []
    entry_requirements: List[Optional[Tuple[str, str]]] = []
    entry_requirement_refs: List[Optional[str]] = []
    cache_requirement_refs: List[Tuple[str, Optional[str]]] = []
    unestablished_bindings: List[Tuple[str, str, Optional[str]]] = []
    #: #184 amendment 3 §7: writer token -> (the writing node's semantic, its unmet
    #: property reads), captured when the writer ran. The CFG is a tree, so each
    #: writer runs once, and a cached document's writer facts stay facts about it.
    writer_records: Dict[str, Tuple[Any, tuple]] = {}
    leg_writes = _leg_write_index(prepared, capabilities)
    #: Every document cache this process can observe, for a child that may write one: the
    #: caches its own nodes name, the caches its callers filled, and every cache a called
    #: child's contract names. A forwarding process names no cache of its own, yet a child
    #: it calls may append to the cache a later child reads (Stage-2 review round r2).
    cache_refs = sorted(
        {
            node.semantic.cache_ref
            for node in prepared.cfg.nodes
            if isinstance(getattr(node.semantic, "cache_ref", None), str) and node.semantic.cache_ref
        }
        | {cache_ref for cache_ref, _profile in capabilities.caller_cache_contents}
        | {
            key[1]
            for row in capabilities.child_entry_contracts
            for key in tuple(row.required_reads) + tuple(row.mutated_state)
            if key[0] == CACHE
        }
        | {
            cache_ref
            for row in capabilities.child_entry_contracts
            for cache_ref, _profile in row.cache_requirements
        }
    )
    index = prepared.symbols.build_index()
    from ..contracts import component_identity

    #: #184: every profile a cache component's references declare, by canonical ref.
    cache_profiles = canonical_cache_profiles(prepared.symbols)
    # #184: the stream-profile proof needs each call's resolved binding. When a
    # binding does not resolve, connector resolution already reports that defect
    # in the flow phase, and a profile verdict built on a guessed binding would
    # only add noise beside it — so the proof is skipped for the whole document.
    try:
        bindings = {
            binding.node_id: binding
            for binding in resolve_connector_call_bindings(prepared.cfg, prepared.symbols)
        }
        profile_proof = True
    except ProcessIRCompileError:
        bindings = {}
        profile_proof = False
    # A call that DECLARES a profile ref naming something other than a profile
    # component is refused by connector resolution at that call's `/operation_ref`
    # (#140), the symbol that is actually wrong. Judging the map it feeds against
    # the resulting unknown stream would add a second finding downstream for the
    # same wrong symbol, so the proof stands down for the document, exactly as it
    # does for a call that does not resolve.
    if profile_proof and any(
        ref is not None and _resolved_profile_identity(index, ref) is None
        for binding in bindings.values()
        for ref in (binding.input_profile_ref, binding.output_profile_ref)
    ):
        profile_proof = False

    # #184: the named legacy exemption belongs to the DIALECT, and is never inferred
    # from a missing connector call. A consumer is exempt in two cases:
    # - the documents reaching it were last produced by a legacy `source` endpoint
    #   (tracked per path, and cleared by a first-class call, after which the stream
    #   is provable again); or
    # - every document it handles can only end at a legacy `target` endpoint: its
    #   subtree holds a legacy target and no connector call.
    # The second case keeps the shipped listener spine `[listener, map_ref, target,
    # stop]` exactly as #158 shipped it. Goldens 000022/040/041/078 render that
    # spine from adapter symbols that declare no profile at all, so nothing about
    # those maps is provable either way. A sibling leg that runs a connector call
    # stays checked, because the model admits a call leg and a target leg side by
    # side under a control-only root.
    legacy_target_below: Dict[str, bool] = {}
    connector_call_below: Dict[str, bool] = {}
    preorder: List[str] = []
    pending = [prepared.cfg.entry_node_id]
    visited_ids: Set[str] = set()
    while pending:
        current = pending.pop()
        if current in visited_ids or prepared.node(current) is None:
            continue
        visited_ids.add(current)
        preorder.append(current)
        pending.extend(edge.target_node_id for edge in prepared.successors(current))
    # The CFG is a tree, so a reversed pre-order visits every child before its parent.
    for current in reversed(preorder):
        here = prepared.node(current).semantic
        target_here = here.semantic_kind == "connector" and getattr(here, "role", None) == "target"
        call_here = here.semantic_kind == "connector_call"
        for edge in prepared.successors(current):
            target_here = target_here or legacy_target_below.get(edge.target_node_id, False)
            call_here = call_here or connector_call_below.get(edge.target_node_id, False)
        legacy_target_below[current] = target_here
        connector_call_below[current] = call_here

    def _feeds_only_a_legacy_target(node_id: str) -> bool:
        return legacy_target_below.get(node_id, False) and not connector_call_below.get(node_id, False)

    def _report(code: str, node, severity="error", evidence=(), sub_path="",
                phase=_LINEAGE_PHASE) -> None:
        # One finding per (code, node). The report dedups too, but stopping the
        # duplicate here keeps a diamond-shaped graph from generating the same
        # finding once per path.
        #
        # `sub_path` addresses a finding BELOW the node — the dynamic-path rules
        # use `/path_binding`, because every one of them is about the binding and
        # a node carrying several authored fields would otherwise leave the
        # caller guessing which. It is a parameter rather than a second reporter
        # on purpose: the served-text scanner pins how many places construct a
        # finding from a dynamic code, and a second construction site is exactly
        # the drift that pin exists to catch.
        #
        # The dedup key carries `sub_path` (#184): one Set Properties step can
        # hold several profile sources, each mismatching at its own
        # `/source_values/<i>/profile_ref`, and keying on the node alone kept the
        # first and silently dropped the rest.
        key = (code, node.node_id, sub_path)
        if key in reported:
            return
        reported.add(key)
        findings.append(
            finding(
                code,
                severity,
                phase,
                node.source_path + sub_path,
                evidence=evidence,
                internal_node_id=node.node_id,
            )
        )

    def _classify_unmet_read(node, semantic, key, leg, extra=(), invalidated=frozenset()) -> None:
        """Report ONE unestablished read under the sharpest code that fits.

        Shared by both read paths. The refinements below are what make a
        lineage finding actionable, and which one applies is a property of the
        READ — its scope, and where its writer sits in the graph — never of who
        declared it. Reporting the flat fallback for a contract's declared read
        while an identical authored read got ``…BRANCH_ORDER_INVALID`` made the
        diagnostic depend on the reader's provenance, which is the mirror image
        of the writer-side asymmetry fixed alongside it.

        ``invalidated`` (#184) holds the document-scoped keys a stream-replacing
        step on THIS path dropped. A read of one is a read before any write the
        current documents carry, and is reported as exactly that: the property WAS
        written in this process, so without this the refinement below would call
        it a different-document-copy scope error, which sends the author looking
        at sibling paths for a defect that sits on their own.
        """
        scope, _name = key
        # A read a typed contract vouches for an OUTSIDE writer of is not the
        # caller's obligation — an external system establishes it — so it is
        # reported as a named warning below and must not enter the required set
        # either. Recording it unconditionally demanded that a caller write a
        # cache it does not own.
        externally_satisfied = (
            scope == CACHE
            and getattr(semantic, "external_writer", False)
            and capabilities.writes_cache_externally(
                getattr(semantic, "cache_ref", "")
            )
        )
        if not externally_satisfied:
            unmet.append(key)
        if scope != DDP and _written_in_a_later_leg(leg_writes, leg, key):
            # The write exists, in a LATER leg of the same Branch. Legs run
            # in order, so it has not happened yet. Saying "read before
            # write" here would send the author looking for a missing write
            # that is right there — the defect is its position, not its
            # absence.
            _report(
                PROCESS_IR_SEMANTIC_LINEAGE_BRANCH_ORDER_INVALID,
                node,
                evidence=(
                    ("state_scope", scope),
                    ("leg_ordinal", leg[1] if leg else 0),
                )
                + extra,
            )
        elif scope == CACHE:
            # The authored `external_writer` boolean DECLARES the expectation;
            # a typed contract CONFIRMS it. The flag alone used to downgrade a
            # blocking finding to a warning, which is a free-form "trust me"
            # assertion suppressing a fatal safety rule — excluded by name in
            # this issue's own criteria, and the reason capabilities are
            # compiler context no payload can reach.
            #
            # Legacy dialects are unaffected: their compatibility comes from
            # LEGACY_ADAPTER_EXEMPTION_STANDALONE_CACHE_READ, a named
            # registry-owned policy covering this exact code, not from a
            # caller-supplied field.
            if getattr(semantic, "external_writer", False) and (
                capabilities.writes_cache_externally(getattr(semantic, "cache_ref", ""))
            ):
                _report(
                    PROCESS_IR_SEMANTIC_LINEAGE_EXTERNAL_WRITER_ASSUMED,
                    node,
                    severity="warning",
                    evidence=(("state_scope", CACHE), ("external_writer", True))
                    + extra,
                )
            else:
                _report(
                    PROCESS_IR_SEMANTIC_LINEAGE_CACHE_WRITER_MISSING,
                    node,
                    evidence=(("state_scope", CACHE),) + extra,
                )
        elif (
            scope == DDP
            and key not in invalidated
            and CACHE_TRANSFER_UNPROVED not in invalidated
            and _written_anywhere(prepared, key, capabilities)
        ):
            # The property IS written in this process, just not on a path
            # that reaches here. For a DDP that is specifically a scope
            # error — the write landed on a different document copy — and
            # saying so is far more actionable than "read before write".
            _report(
                PROCESS_IR_SEMANTIC_LINEAGE_DDP_SCOPE_INVALID,
                node,
                evidence=(("state_scope", DDP),) + extra,
            )
        else:
            _report(
                PROCESS_IR_SEMANTIC_LINEAGE_PROPERTY_READ_BEFORE_WRITE,
                node,
                evidence=(("state_scope", scope),) + extra,
            )

    def _profile_identity(ref):
        """An authored profile ref as the identity the EMITTER will use.

        The component the ref names (``component_identity``) when the symbol table
        knows the ref, and the ref itself when it does not — an unresolvable ref is
        already reported by the reference phase, and inventing an identity for it
        here would either collapse two unknown refs into one or make a ref unequal
        to itself.
        """
        if ref is None:
            return None
        return component_identity(prepared.symbol(ref)) or ref

    def _check_path_binding(node, semantic, state, writers) -> None:
        """A bound request path is only as sound as the writer that composes it (#155).

        Runs at the CONNECTOR, against the state and the reaching writer on THIS
        path — not against the meet at exit, which answers a different question
        (what holds when the process finishes) and is both too weak and too
        strong for "established on every path to this call". ``_report`` dedups
        by (code, node), so a violation on any one path is reported once.

        The reaching writer is a singleton per path: the CFG is a tree, so
        ``writers`` carries the LAST writer of each key along the path walked to
        get here. Nothing is merged — a Decision arm that composes the path
        differently is its own path and is checked as one.
        """
        binding = getattr(semantic, "path_binding", None)
        if binding is None:
            return
        _check_bound_key(node, binding, state, writers, "/path_binding")

    def _check_bound_key(node, binding, state, writers, sub_path) -> None:
        """The binding rule for ONE bound property, reported at ``sub_path``.

        Shared by a request path bound in this process and by a call discharging a
        passthrough child's bound path (#184 amendment 3 §8). The call reports at its
        own ``/process_ref``, because the binding it checks lives in the child.
        """
        # Evidence stays STRUCTURAL. The property name is caller-authored text,
        # and evidence is served — the node's own source_path already points the
        # author at the binding, so naming it here would buy nothing and leak.
        key = (DDP, binding.property_name)
        alternatives = writers.get(key) or ()
        # Established, and established by a writer this process can see. A key
        # the CALLER declares established at entry has no writer here, so its
        # composition cannot be checked at all — which is exactly the case the
        # binding must not be allowed to rest on. #184 amendment 3 §7: after a
        # cache retrieval the key may have SEVERAL possible writers, and the path
        # must be sound for every one; an unknown one proves nothing.
        if not state.establishes(key) or not alternatives or UNKNOWN_WRITER in alternatives:
            unestablished_bindings.append(
                (node.source_path + sub_path, binding.property_name, binding.request_profile_ref)
            )
            _report(
                PROCESS_IR_SEMANTIC_DYNAMIC_PATH_DDP_NOT_ESTABLISHED,
                node,
                evidence=(("state_scope", DDP),),
                sub_path=sub_path,
            )
            return
        for token in alternatives:
            if not _check_one_writer(node, binding, writer_records[token], sub_path):
                return

    def _check_one_writer(node, binding, writer, sub_path) -> bool:
        """The composition checks for ONE possible writer; False once one is reported."""
        if writer is None:
            # CALLER_WRITER: a called passthrough child's caller composes this
            # property, and every call checks that composition against this binding.
            return True
        writer_semantic, unmet_reads = writer
        if unmet_reads:
            # The writer itself composes from a property nothing established.
            # A default on that read does NOT discharge it here: for an ordinary
            # read a default is a defined value, but this one becomes the request
            # PATH, so defaulting addresses the wrong resource instead of failing.
            _report(
                PROCESS_IR_SEMANTIC_DYNAMIC_PATH_DDP_NOT_ESTABLISHED,
                node,
                evidence=(("state_scope", unmet_reads[0][0]),),
                sub_path=sub_path,
            )
            return False
        sources = tuple(getattr(writer_semantic, "source_values", ()) or ())
        if not any(getattr(s, "value_type", None) != "static" for s in sources):
            _report(
                PROCESS_IR_SEMANTIC_DYNAMIC_PATH_NO_DYNAMIC_SEGMENT,
                node,
                evidence=(("state_scope", DDP),),
                sub_path=sub_path,
            )
            return False
        # The profile pairing is a biconditional, and it is what makes the
        # binding's own ``request_profile_ref`` a pinned fact rather than a
        # second copy: the emitted parameter-profile attribute is meaningful
        # only with a profile element, so the two must agree exactly.
        # Compared by RESOLVED IDENTITY, not by the authored token. What the
        # emitter writes is the resolved component id, so two different refs
        # that name one component are one profile and must agree — comparing
        # tokens reported a mismatch for a pair that emits identically, and
        # rejected several sources aliased to one component as "several
        # profiles". This is the same defect as the whitespace one above, in its
        # other half: a validator whose key is weaker than the runtime's.
        pairs = {
            (_profile_identity(getattr(s, "profile_ref", None)),
             getattr(s, "profile_type", None))
            for s in sources
            if getattr(s, "value_type", None) == "profile"
        }
        if len(pairs) > 1:
            _report(
                PROCESS_IR_SEMANTIC_DYNAMIC_PATH_PROFILE_BINDING_MISMATCH,
                node,
                evidence=(("reader_count", len(pairs)),),
                sub_path=sub_path,
            )
            return False
        declared = _profile_identity(binding.request_profile_ref)
        expected = next(iter(pairs))[0] if pairs else None
        if declared != expected:
            _report(
                PROCESS_IR_SEMANTIC_DYNAMIC_PATH_PROFILE_BINDING_MISMATCH,
                node,
                evidence=(("reader_count", len(pairs)),),
                sub_path=sub_path,
            )
            return False
        return True

    def _identity(ref):
        """A profile ref as the resolved ``(component id, profile type)`` identity.

        The connector module's own resolver, so a map, a call and a cache agree
        about what "the same profile" means — resolved component id, and only for
        a real profile component. The nested ``_profile_identity`` above answers
        the bound-path rule's narrower question and is left as it is.
        """
        return _resolved_profile_identity(index, ref)

    def _requires(stream, identity, ref):
        """Record what a consumer needs of documents this process cannot prove.

        Documents a caller handed over (a passthrough entry) state a requirement of
        that caller. Documents read from a cache no write in this process reached state
        a requirement of whoever filled the cache, which only a caller can be. Any
        other stream records nothing: the consumer is checked against it instead.
        """
        if stream.state == STREAM_CALLER_ENTRY:
            entry_requirements.append(identity)
            entry_requirement_refs.append(ref)
        elif stream.caller_cache is not None:
            cache_requirement_refs.append((stream.caller_cache, ref))

    def _advance_stream(node, semantic, state, stream, legacy):
        """Check this node's profile consumers against the reaching stream, then step it (#184 D2).

        Returns ``(state, stream, legacy)``; ``state`` changes only in its cache
        content. ``legacy`` is on while the reaching documents were last produced
        by a legacy ``source`` endpoint, and a first-class call turns it off again.
        Together with ``_feeds_only_a_legacy_target`` it scopes the named legacy
        exemption. That dialect's maps and property writers were never
        profile-checked, and they keep the exemption rather than being judged
        against a stream a legacy endpoint cannot state.

        A consumer on a no-producer stream is left to connector resolution, which
        refuses it as a cardinality defect (#184 A4). Reporting a profile verdict
        beside that would name a symptom of the same root cause. The one exception
        is a profile-valued source on the untouched scheduled entry: the empty No
        Data document carries no payload, so no element can be read out of it by
        profile (amendment 2 §4). That holds whether or not anything downstream
        needs documents.
        """
        if not profile_proof:
            return state, stream, legacy
        kind = semantic.semantic_kind
        checked = not (legacy or _feeds_only_a_legacy_target(node.node_id))

        def mismatch(at_node, sub_path):
            _report(
                PROCESS_IR_SEMANTIC_PROFILE_MISMATCH,
                at_node,
                sub_path=sub_path,
                phase=_PROFILE_PHASE,
            )

        if kind == "set_property":
            if checked:
                for position, source in enumerate(semantic.source_values):
                    if getattr(source, "value_type", None) != "profile":
                        continue
                    sub_path = "/source_values/{0}/profile_ref".format(position)
                    identity = _identity(source.profile_ref)
                    _requires(stream, identity, source.profile_ref)
                    if stream.state == STREAM_CALLER_ENTRY:
                        if stream.identity is None:
                            stream = _Stream(STREAM_CALLER_ENTRY, identity)
                        elif identity != stream.identity:
                            mismatch(node, sub_path)
                    elif stream.state == STREAM_EMPTY_ENTRY or (
                        stream.state == STREAM_KNOWN and identity != stream.identity
                    ):
                        mismatch(node, sub_path)
            return state, stream, legacy

        if kind == "process_call":
            # #184 amendment 3 §8: a waiting call hands a Data Passthrough child the
            # documents reaching it, so every consumer its contract records is a
            # requirement of THIS stream. A consumer nothing states (None) proves
            # nothing here and is left to the call's admission check.
            contract = capabilities.child_entry_contract(semantic.process_ref)
            if checked and contract is not None and contract.entry_form == "passthrough" and semantic.wait:
                for ref in contract.document_requirements:
                    if ref is None:
                        continue
                    required = _identity(ref)
                    _requires(stream, required, ref)
                    if stream.state == STREAM_CALLER_ENTRY:
                        if stream.identity is None:
                            stream = _Stream(STREAM_CALLER_ENTRY, required)
                        elif required != stream.identity:
                            mismatch(node, "/process_ref")
                    elif stream.state != STREAM_KNOWN or required is None or required != stream.identity:
                        mismatch(node, "/process_ref")
            return state, stream, legacy

        if kind == "passthrough":
            return state, _Stream(STREAM_CALLER_ENTRY), legacy

        if kind == "connector":
            if semantic.role == "source":
                return state, _Stream(STREAM_UNKNOWN, origin="legacy"), True
            return state, stream, legacy

        if kind == "listener":
            operation = index.get(semantic.operation_ref) if semantic.operation_ref else None
            identity = _identity(getattr(operation, "input_profile_ref", None))
            if identity is None:
                return state, _Stream(STREAM_UNKNOWN, origin="undeclared"), legacy
            return state, _Stream(STREAM_KNOWN, identity, "listener", node), legacy

        if kind == "connector_call":
            binding = bindings[node.node_id]
            # A map's target must be what the call it feeds declares it accepts.
            # Only a MAP origin is compared: call-to-call equality stays unchecked
            # (D4), because connector request and response profiles are documented
            # as non-validating, and a cache's content may itself be a call's
            # output. The mismatch is reported at the map, which is where #140
            # always reported it.
            if checked and stream.state == STREAM_KNOWN and stream.origin == "map":
                declared = _identity(binding.input_profile_ref)
                if declared is None or declared != stream.identity:
                    mismatch(stream.origin_node, "/map_ref")
            if (
                binding.capability.accepts_input == "documents_required"
                or binding.input_profile_ref is not None
            ):
                # The call consumes the documents: on a caller's documents it requires
                # its declared input profile of them, and an undeclared one states
                # nothing a caller could discharge.
                _requires(
                    stream,
                    _identity(binding.input_profile_ref)
                    if binding.input_profile_ref is not None
                    else None,
                    binding.input_profile_ref,
                )
            # A first-class call's output is what flows on, so whatever a legacy
            # source produced upstream no longer reaches the next consumer.
            if not binding.capability.produces_output:
                return state, _Stream(STREAM_ABSENT), False
            output = _identity(binding.output_profile_ref)
            if output is None:
                return state, _Stream(STREAM_UNKNOWN, origin="undeclared"), False
            return state, _Stream(STREAM_KNOWN, output, "call", node), False

        if kind == "map":
            if stream.state in NO_PRODUCER_STREAMS:
                return state, stream, legacy
            symbol = index.get(semantic.map_ref) if semantic.map_ref else None
            is_map = symbol is not None and _canonical_type(symbol) == MAP_COMPONENT_TYPE
            source = _identity(symbol.input_profile_ref) if is_map else None
            target = _identity(symbol.output_profile_ref) if is_map else None
            # A map's source and target profiles are hard component requirements,
            # so an absent one is a mismatch, and so is a stream nothing proves.
            _requires(stream, source, symbol.input_profile_ref if is_map else None)
            if stream.state == STREAM_CALLER_ENTRY:
                # On the caller's documents the map's source IS the requirement;
                # only a contradiction with an earlier requirement on this path is
                # provable inside the child.
                contradicted = (
                    source is None
                    or target is None
                    or (stream.identity is not None and source != stream.identity)
                )
            else:
                contradicted = (
                    source is None
                    or target is None
                    or stream.state != STREAM_KNOWN
                    or source != stream.identity
                )
            if checked and contradicted:
                mismatch(node, "/map_ref")
            # The map owns its own input mismatch. Downstream consumers are judged
            # against what it declares it emits, so one wrong map is reported once.
            if target is None:
                return state, _Stream(STREAM_UNKNOWN, origin="map"), legacy
            return state, _Stream(STREAM_KNOWN, target, "map", node), legacy

        if kind == "cache_put" and stream.state == STREAM_CALLER_ENTRY:
            # Staging the caller's documents: the cache's declared profile is what
            # the child requires of them, and an undeclared cache states nothing.
            declared_refs = cache_profiles.get(semantic.cache_ref, ())
            identities = {_identity(ref) for ref in declared_refs}
            # Declarations that disagree state no single profile the child requires, and
            # the staged documents are checked against each of them.
            declared_ref = declared_refs[0] if len(identities) == 1 else None
            declared = _identity(declared_ref) if declared_ref is not None else None
            _requires(stream, declared, declared_ref)
            if checked and stream.identity is not None and any(
                identity is not None and identity != stream.identity for identity in identities
            ):
                mismatch(node, "/cache_ref")
            return (
                state.with_content(semantic.cache_ref, declared),
                _Stream(STREAM_ABSENT),
                legacy,
            )

        if kind == "cache_put":
            identity = stream.identity if stream.state == STREAM_KNOWN else None
            if checked and identity is not None and any(
                _identity(declared_ref) != identity
                for declared_ref in cache_profiles.get(semantic.cache_ref, ())
            ):
                mismatch(node, "/cache_ref")
            # Add to Cache hands on no documents: the path ends here.
            return (
                state.with_content(semantic.cache_ref, identity),
                _Stream(STREAM_ABSENT),
                legacy,
            )

        cache_reads = [key[1] for key, _default, _strict in _reads_of(semantic) if key[0] == CACHE]
        if cache_reads and kind in TRIGGERED_REPLACEMENT_SEMANTIC_KINDS:
            if stream.state == STREAM_ABSENT:
                # Nothing arrives, so the read never runs: it cannot invent a
                # stream after a path its documents were consumed on.
                return state, stream, legacy
            if getattr(semantic, "external_writer", False):
                # An outside writer's content has no profile this process can see
                # (D6); the declared cache profile is not a substitute for it.
                state = state.with_content(cache_reads[0], None)
            contents = state.content_of(cache_reads[0])
            if len(contents) == 1 and None not in contents:
                return state, _Stream(STREAM_KNOWN, next(iter(contents)), "cache", node), legacy
            if not contents and not getattr(semantic, "external_writer", False):
                # No write in this process reaches the read, so a caller filled the
                # cache or nothing did. The consumer is still refused here, and records
                # what it needs so every call can prove it (amendment 1 rule 6).
                return state, _Stream(STREAM_UNKNOWN, origin="cache", caller_cache=cache_reads[0]), legacy
            return state, _Stream(STREAM_UNKNOWN, origin="cache"), legacy

        if kind == "cache_remove":
            # Whole-cache removal clears the cache's content, and hands on no
            # documents: the path ends here, exactly as after a cache write.
            if getattr(semantic, "remove_all_documents", False):
                state = state.without_content(semantic.cache_ref)
            if kind in ZERO_EMISSION_SEMANTIC_KINDS:
                return state, _Stream(STREAM_ABSENT), legacy
            return state, stream, legacy

        if _replaces_document_stream(semantic):
            if kind == "data_process":
                # A data process reads unproved documents in a way nothing states, so
                # it cannot be recorded as requiring nothing of them. A Message ignores
                # its input's content and records no requirement.
                _requires(stream, None, None)
            if stream.state in (STREAM_EMPTY_ENTRY, STREAM_TOUCHED_ENTRY):
                return state, _Stream(STREAM_TOUCHED_ENTRY), legacy
            if stream.state == STREAM_ABSENT:
                return state, stream, legacy
            return state, _Stream(STREAM_UNKNOWN, origin="opaque"), legacy

        return state, stream, legacy

    def _child_may_write_caches(state, contract):
        """What a child's cache writes may leave in this execution's caches.

        A child shares its caller's document caches (capture `cap184-shared-cache`),
        and what it stores carries a profile and a property cohort this process cannot
        see. Which caches those are is `_caches_a_call_may_write`.
        """
        for ref in _caches_a_call_may_write(cache_refs, contract):
            state = state.with_content(ref, None).with_cohort(ref, UNKNOWN_COHORT)
        return state

    def _discharge_child_contract(node, semantic, state, leg, writers, invalidated, stream):
        """Everything ONE call owes its child's entry contract (#184 amendment 3 §8).

        Each call discharges its own row, so two parents' facts never combine. The
        document-profile half is checked with the stream proof, in `_advance_stream`.
        Returns the state after the call, which differs only in what the child may
        have put in a cache.
        """
        contract = capabilities.child_entry_contract(semantic.process_ref)
        form = contract.entry_form if contract is not None else None
        context, predecessor = _call_prefix(prepared.ir, node.source_path)
        if form == "passthrough" and not semantic.wait:
            _report(
                PROCESS_IR_CAPABILITY_ENTRY_CONTEXT_UNSUPPORTED,
                node,
                sub_path="/wait",
                phase=_CAPABILITY_PHASE,
            )
        elif predecessor is not None and (
            not process_call_prefix_admitted(context, predecessor, form, semantic.wait)
            or None in contract.document_requirements
        ):
            # Native work before a call is a verified hand-off: admitted only for an
            # attested evidence key, into a child that states everything it consumes.
            _report(
                PROCESS_IR_CAPABILITY_PROCESS_CALL_PLACEMENT_UNSUPPORTED,
                node,
                phase=_CAPABILITY_PHASE,
            )
        if contract is None or form == "unknown":
            return _child_may_write_caches(state, None)
        if form == "passthrough":
            for name, request_profile_ref in contract.required_writers:
                _check_bound_key(
                    node, _InheritedBinding(name, request_profile_ref), state, writers,
                    "/process_ref",
                )
        for raw in contract.required_reads:
            key = (raw[0], raw[1])
            if state.establishes(key):
                continue
            _classify_unmet_read(
                node, semantic, key, leg, extra=(("effect_kind", "subprocess"),),
                invalidated=invalidated,
            )
        if profile_proof:
            # A cache the child reads before writing it holds what THIS execution
            # stored there (capture `cap184-shared-cache`), so the profile the child's
            # consumers need must be exactly what reaches the call (amendment 1 rule 6).
            for cache_ref, profile_ref in contract.cache_requirements:
                contents = state.content_of(cache_ref)
                if not contents:
                    # No write in this process reaches the call, so the cache holds what
                    # this process's own caller stored. The obligation is inherited and
                    # recorded exactly as a consumer of that cache records it.
                    cache_requirement_refs.append((cache_ref, profile_ref))
                if profile_ref is None:
                    continue
                if contents != frozenset({_identity(profile_ref)}):
                    _report(
                        PROCESS_IR_SEMANTIC_PROFILE_MISMATCH,
                        node,
                        sub_path="/process_ref",
                        phase=_PROFILE_PHASE,
                    )
        if form == "scheduled" and stream.count != COUNT_ONE:
            shared = {(key[0], key[1]) for key in contract.required_reads}
            mutated = {(key[0], key[1]) for key in contract.mutated_state}
            if shared and (not contract.state_known or shared & mutated):
                # A No Data child runs once per arriving document, so a later run may
                # find the shared state an earlier run changed (amendment 1 rule 8).
                _report(
                    PROCESS_IR_CAPABILITY_PROCESS_CALL_PLACEMENT_UNSUPPORTED,
                    node,
                    phase=_CAPABILITY_PHASE,
                )
        return _child_may_write_caches(state, contract)

    def _transfer(node, state, leg, writers, on_documents, invalidated, stream, legacy):
        """Everything ONE node does to the facts carried on its path, in order.

        Lifted unchanged out of the recursive visit (#184 D12), so the controller
        below is pure control flow and the transfer rules keep a single statement.
        Returns the facts the node's successors start from.
        """
        # Per-path, copy-on-write, and deliberately NOT part of `_State`: it is
        # never merged, so it cannot perturb the meet the whole module rests on.
        writers = writers if writers is not None else {}
        # Document-scoped keys whose value the documents AT THIS POINT carry.
        # One notion, fed by every channel that establishes a key and emptied
        # by the one event that invalidates them — see `_check_path_binding`.
        on_documents = on_documents if on_documents is not None else frozenset()
        # #184: document-scoped keys a stream-replacing step on THIS path dropped
        # and nothing has re-written since. Per path and never merged, like
        # `writers`; it only decides how an unmet read of such a key is reported.
        invalidated = invalidated if invalidated is not None else frozenset()
        # #184 D2: the profile of the documents reaching this node, per path and
        # never merged. A scheduled root starts from the empty No Data document.
        stream = stream if stream is not None else _Stream(STREAM_EMPTY_ENTRY, count=COUNT_ONE)
        # What THIS node establishes, kept separately: a node that replaces the
        # stream still writes onto the documents it emits, so its own writes
        # must survive its own replacement.
        established_here = set()

        semantic = node.semantic

        # --- reads, checked against what is established HERE ----------------
        for key, has_default, strict in _reads_of(semantic):
            if has_default or state.establishes(key):
                continue
            # A non-strict reader tolerates ABSENCE (the wire carries a defined
            # empty default) but not a writer that exists somewhere unreachable.
            if not strict and not _nonstrict_read_can_fail(
                prepared, key, capabilities
            ):
                continue
            _classify_unmet_read(node, semantic, key, leg, invalidated=invalidated)

        # --- a trusted contract's declared READS are dependencies -----------
        # Applying only its writes made a contract that READS unwritten state
        # produce a valid report: the contract says the map consumes a key, and
        # nothing checked that anything establishes it.
        #
        # A declared read is always STRICT: the contract asserts the effect
        # consumes the key, so there is no wire default to fall back to.
        #
        # Reads and writes are interleaved IN STEP ORDER, one contract at a
        # time. One data_process node can carry several contracted scripts, and
        # they run in sequence — checking every read against the state from
        # before the whole node reported a script's read of what the PREVIOUS
        # script in the same node just wrote as read-before-write. The effects
        # are ordered, so the walk over them has to be too.
        # A FIRE-AND-FORGET child establishes nothing downstream. Its reads are
        # still dependencies — it consumes state when it is launched — but its
        # writes are unordered with respect to everything after the call, so
        # applying them proves a downstream read that may run first.
        #
        # This has to hold HERE, in the lattice, not only in the ordering
        # collector. That collector deliberately skips DDP (document scope is
        # not what an async race is about), so a DDP write applied here fell
        # through both checks and a `wait=False` child's declared DDP write
        # silently established a downstream read. DPP and cache only looked
        # correct because the ordering phase happened to cover them.
        establishes = _establishes_downstream(semantic)
        for effect in _trusted_effects(semantic, capabilities):
            for raw in effect.reads:
                key = (raw[0], raw[1])
                if state.establishes(key):
                    continue
                _classify_unmet_read(
                    node, semantic, key, leg, extra=(("effect_kind", "declared_read"),),
                    invalidated=invalidated,
                )
            # A trusted contract contributes EXACT writes, visible to the next
            # contract ON THIS NODE — one data_process can carry several
            # contracted scripts and they run in sequence, so the walk over
            # them has to be sequential too.
            # #184 D5: a contract that writes a cache declares WHICH cache, never
            # the profile of what it stores, so that cache may now hold content of
            # unknown profile. Recorded whether or not the effect establishes
            # downstream: a fire-and-forget child may still have written by the
            # time a later read runs, and this is a MAY set.
            for key in effect.writes:
                if key[0] == CACHE:
                    state = state.with_content(key[1], None)
                    state = state.with_cohort(key[1], UNKNOWN_COHORT)
            if not establishes:
                continue
            for key in effect.writes:
                state = state.with_write((key[0], key[1]))
                if key[0] == DDP:
                    on_documents = on_documents | {(key[0], key[1])}
                    established_here.add((key[0], key[1]))
                    invalidated = invalidated - {(key[0], key[1])}

        # --- opaque effects contribute uncertainty, never proof -------------
        opaque = _opaque_reason(semantic, capabilities)
        if opaque is not None:
            _report(
                PROCESS_IR_SEMANTIC_LINEAGE_EFFECT_UNKNOWN,
                node,
                severity="warning",
                evidence=(("effect_kind", opaque),),
            )

        # --- writes ---------------------------------------------------------
        # Contract writes are applied above, interleaved with their own reads.
        # Only the node's AUTHORED writes remain, and the two sets never meet
        # on one node: `_writes_of` covers set_property / cache_put, while
        # `_trusted_effects` covers map / process_call / data_process.
        for key in _writes_of(semantic):
            # Record WHICH node established the key on this path, along with any
            # of its own property reads that nothing had established yet (#155).
            # Both are captured HERE, against the state the writer actually ran
            # under — recovering them at the consumer would mean re-walking.
            if key[0] == DDP:
                # DDP sources only, and a default does NOT discharge them: a
                # document property has no source outside this process, so a
                # defaulted one composes the request path from a value nothing
                # wrote. A DPP source is deliberately NOT included — an
                # execution supplies dynamic process properties with the run
                # request, which is how the live-attested source-role path is
                # driven (capture `cap155-e1-source-dynamic-path`, where the
                # path's `key` segment arrives with the execution), so requiring
                # an in-process writer for it would refuse a shape the platform
                # runs green.
                unmet_here = tuple(
                    read_key
                    for read_key, _has_default, _strict in _reads_of(semantic)
                    if read_key[0] == DDP and not state.establishes(read_key)
                )
                # A `current` source re-uses the property's OWN value. The
                # served model calls that a read, but `_reads_of` does not record
                # one, and deliberately: the LEGACY chain accepts a `current`
                # composition with no earlier write and the shipped parity golden
                # freezes that shape, so teaching the general lineage model to
                # refuse it would break the oracle this compiler is measured
                # against. The read is recognised HERE, where it is decided only
                # for a bound request path — an ordinary property composed from
                # an unset `current` is an empty string the platform runs, while
                # the same value as a request PATH addresses the wrong resource
                # and does so silently.
                #
                # Asked of `on_documents`, and the choice of key IS the rule.
                # Two weaker keys were tried and both were wrong, in opposite
                # directions: `state` answers "was this written anywhere on this
                # path" and stays true across a document replacement, so it was
                # fail-open for a value written before one and appended to after;
                # `writers` holds only AUTHORED property nodes, so it discarded
                # writes a trusted contract or the caller had truthfully
                # declared, and was fail-closed for those. `on_documents` is
                # neither proxy but the fact itself — which document-scoped keys
                # the documents at this point carry — fed by every channel that
                # establishes one and emptied by the event that invalidates them.
                if key not in on_documents and any(
                    getattr(source, "value_type", None) == "current"
                    for source in getattr(semantic, "source_values", ()) or ()
                ):
                    unmet_here = unmet_here + (key,)
                writer_records[node.node_id] = (semantic, unmet_here)
                writers = {**writers, key: (node.node_id,)}
            state = state.with_write(key)
            if key[0] == DDP:
                on_documents = on_documents | {key}
                established_here.add(key)
                invalidated = invalidated - {key}

        # --- a bound request path, against this path's reaching writer -------
        _check_path_binding(node, semantic, state, writers)

        # --- a child's entry contract, discharged by THIS call (#184 amendment 3 §8)
        if semantic.semantic_kind == "process_call":
            state = _discharge_child_contract(node, semantic, state, leg, writers, invalidated, stream)

        # A step that replaces the document stream ends every reaching writer's
        # claim: the documents leaving it never carried those properties. Applied
        # AFTER the check above so a binding ON the replacing node still sees the
        # writer that reached it, and only to `writers` — `_State` keeps #154's
        # model untouched.
        kind = semantic.semantic_kind
        count = stream.count
        properties_unknown = stream.properties_unknown
        if kind == "cache_put" and stream.state != STREAM_ABSENT:
            # #184 amendment 3 §7: the cohort the executing write stores, frozen now.
            state = state.with_cohort(
                semantic.cache_ref, _cohort_at_write(on_documents, writers, stream)
            )
        if kind == "cache_remove" and getattr(semantic, "remove_all_documents", False):
            state = state.without_content(semantic.cache_ref)
        if kind in TRIGGERED_REPLACEMENT_SEMANTIC_KINDS:
            if stream.state == STREAM_ABSENT:
                count = COUNT_UNKNOWN
            else:
                cohorts = state.cohorts_of(semantic.cache_ref)
                properties_unknown = getattr(semantic, "external_writer", False) or any(
                    cohort.possible is None for cohort in cohorts
                )
                state, writers, on_documents, invalidated, count = _overlay_cache_read(
                    semantic, state, writers, on_documents, invalidated, stream
                )
        elif _discards_document_properties(semantic):
            writers = {
                key: value for key, value in writers.items() if key[0] != DDP
            }
            # It holds document-scoped keys only, so replacement empties it —
            # EXCEPT what this node itself established. A contracted script that
            # declares it writes a property replaces the stream and writes onto
            # the documents it emits, so discarding its own declaration here
            # refused a document whose value genuinely survives.
            on_documents = frozenset(established_here)
            # #184 A5: the general lattice follows the documents too, with the same
            # exception, and the dropped keys are remembered on this path so an
            # unmet read of one is reported as read-before-write.
            state, dropped = _drop_replaced_document_keys(state, established_here)
            invalidated = invalidated | dropped

        # --- the stream profile (#184 D2) ----------------------------------
        state, stream, legacy = _advance_stream(node, semantic, state, stream, legacy)

        # --- the proved count and property knowledge (#184 amendment 3 §7) ---
        # Applied after the profile step, which rebuilds the stream, and independent
        # of whether the profile proof runs at all.
        if kind in _COUNT_PRESERVING_KINDS or kind in TRIGGERED_REPLACEMENT_SEMANTIC_KINDS:
            next_count = count
        else:
            next_count = COUNT_UNKNOWN
        if kind == "passthrough" or _opaque_reason(semantic, capabilities) in ("map", "script"):
            properties_unknown = True
        stream = stream._replace(count=next_count, properties_unknown=properties_unknown)

        return state, writers, on_documents, invalidated, stream, legacy

    def _edge_stream(edge, stream):
        """The stream a try_catch edge starts from.

        The platform hands the caught document to the recovery path, so documents
        exist there whatever reached the scope — the same fact connector
        resolution records — but nothing proves their profile.
        """
        if edge.kind == "catch" and stream.state not in (STREAM_KNOWN, STREAM_UNKNOWN):
            return _Stream(STREAM_UNKNOWN, origin="catch", properties_unknown=stream.properties_unknown)
        if edge.kind == "catch":
            # the caught documents are not proved to be exactly the one that entered
            return stream._replace(count=COUNT_UNKNOWN)
        return stream

    entry_state = _State()
    entry_documents = set()
    for key in capabilities.established_at_entry:
        entry_state = entry_state.with_write((key[0], key[1]))
        if key[0] == DDP:
            entry_documents.add((key[0], key[1]))
    # #184 amendment 3 §8: a called passthrough child's caller composes these, and each
    # call proves that writer against this child's binding.
    entry_writers: Dict[StateKey, Tuple[str, ...]] = {}
    for key in capabilities.caller_supplied_writers:
        inherited = (DDP, key[1])
        entry_state = entry_state.with_write(inherited)
        entry_documents.add(inherited)
        entry_writers[inherited] = (CALLER_WRITER,)
    if entry_writers:
        writer_records[CALLER_WRITER] = None
    # #184 amendment 1 rule 6: a called child's first-read caches hold what its callers
    # stored, which every call proves; their property cohorts stay unknown.
    for cache_ref, profile_ref in capabilities.caller_cache_contents:
        entry_state = entry_state.with_content(cache_ref, _identity(profile_ref)).with_cohort(
            cache_ref, UNKNOWN_COHORT
        )

    # --- the controller -----------------------------------------------------------
    # A work stack of frames. A "visit" frame runs one node's transfer and then
    # either records a path end or pushes a continuation frame for its successors
    # followed by the first successor's visit. A continuation frame runs when
    # that successor's whole subtree has been walked, and finds the subtree's
    # returned state in `returned`, exactly where the recursive visit's return
    # value used to arrive. Frames therefore run in the recursive visit's order,
    # and the module-level lists (`normal_exits`, `threw`) see the same sequence
    # of appends and deletions.
    visit_bound = len(prepared.cfg.nodes)
    visits = 0
    returned = entry_state
    work = [("visit", prepared.cfg.entry_node_id, (
        entry_state, None, dict(entry_writers), frozenset(entry_documents), frozenset(),
        _Stream(STREAM_EMPTY_ENTRY, count=COUNT_ONE), False,
    ))]
    while work:
        frame = work.pop()
        tag = frame[0]

        if tag == "visit":
            _tag, node_id, carried_facts = frame
            state, leg, writers, on_documents, invalidated, stream, legacy = carried_facts
            node = prepared.node(node_id)
            if node is None:
                returned = state
                continue
            visits += 1
            if visits > visit_bound:
                # Unreachable for a tree. Raised as the compiler's own defect code:
                # a report may not carry it, and a loop is not an answer.
                raise ProcessIRCompileError(
                    [diagnostic(PROCESS_IR_COMPILE_INTERNAL, "semantic_lowering", "")]
                )
            state, writers, on_documents, invalidated, stream, legacy = _transfer(
                node, state, leg, writers, on_documents, invalidated, stream, legacy
            )
            edges = prepared.successors(node_id)
            if not edges:
                # EVERY path end that is not abnormal is a completion. Which ones
                # a Branch may meet together is decided at the Branch, per
                # COMPARTMENT — not here, by suppressing some of them.
                role = node.exit_role
                if role is not None and role not in _ABNORMAL_EXIT_ROLES:
                    normal_exits.append(state)
                elif role == "exception":
                    threw.append(node_id)
                returned = state
                continue
            after = (writers, on_documents, invalidated, stream, legacy)
            kind = node.semantic.semantic_kind

            if kind == "branch":
                # Legs run SEQUENTIALLY in local-ordinal order. Execution-scoped
                # writes accumulate from one leg into the next; document state
                # does not, because each leg re-copies the pre-Branch documents.
                entry = state.entering_branch_leg()
                branch = {
                    "node": node, "edges": edges, "index": 0, "entry": entry,
                    "carried": entry, "recorded_before": len(normal_exits),
                    # Per leg: the state its own completions agree on. Collected
                    # here rather than read off `carried`, because `carried` is
                    # built from CONTINUATIONS and a continuation is a meet — a leg
                    # ending in a Decision with one throwing arm hands back a state
                    # missing whatever only the normal arm wrote.
                    "leg_documents": [], "guaranteed": entry.execution,
                    "first": len(normal_exits), "after": after,
                }
                work.append(("branch_leg_done", branch))
                edge = edges[0]
                work.append(("visit", edge.target_node_id, (
                    _State(entry.document, entry.execution, entry.content, entry.cohorts),
                    (node.node_id, edge.leg_ordinal or edge.local_ordinal),
                ) + after))
                continue

            if kind == "decision":
                # Arms are EXCLUSIVE. Meet, not union — but only over arms that can
                # CONTINUE. An arm that only throws carries nothing forward: nothing
                # downstream runs for the document that took it, so meeting it in
                # dropped whatever the other arm established for every document
                # that does continue. When every arm throws there is nothing to
                # meet and the pre-Decision state stands.
                decision = {
                    "edges": edges, "index": 0, "state": state, "leg": leg,
                    "results": [], "before_normal": len(normal_exits),
                    "before_threw": len(threw), "after": after,
                }
                work.append(("decision_arm_done", decision))
                work.append(("visit", edges[0].target_node_id, (state, leg) + after))
                continue

            if kind == "try_catch":
                # The catch path forks from SCOPE-ENTRY state plus the caught
                # document. A write inside the try body may not have happened when
                # the failure occurred, so it cannot be assumed visible to catch.
                #
                # Cache CONTENT is the exception (#184 D5): either body may have
                # written before the scope ended, so what a cache may hold
                # afterwards is the union of both, never the scope-entry set alone.
                scope = {
                    "edges": edges, "index": 0, "state": state, "leg": leg,
                    "content": state.content, "cohorts": state.cohorts, "after": after,
                }
                work.append(("try_edge_done", scope))
                work.append(("visit", edges[0].target_node_id, (
                    state, leg, writers, on_documents, invalidated,
                    _edge_stream(edges[0], stream), legacy,
                )))
                continue

            sequence = {"edges": edges, "index": 0, "state": state, "leg": leg, "after": after}
            work.append(("sequential_edge_done", sequence))
            work.append(("visit", edges[0].target_node_id, (state, leg) + after))
            continue

        if tag == "branch_leg_done":
            branch = frame[1]
            leg_end = returned
            completions = normal_exits[branch["first"]:]
            if completions:
                leg_document = completions[0].document
                leg_execution = completions[0].execution
                for other in completions[1:]:
                    leg_document = leg_document & other.document
                    leg_execution = leg_execution & other.execution
                branch["leg_documents"].append(leg_document)
                # every leg RUNS, so what a leg guarantees holds afterwards
                branch["guaranteed"] = branch["guaranteed"] | leg_execution
            # The NEXT leg is seeded from the CONTINUATION, which is throw-aware at
            # the Decision. Seeding it from this leg's normal COMPLETIONS instead
            # broke sequencing: a leg ending in a WAITING `process_call` records no
            # completion — that role is deliberately not a normal exit — so the
            # next leg stopped seeing the write the call established. Cache content
            # is taken from the continuation as it stands: it began from `carried`,
            # so it already holds every earlier leg's writes, less anything this
            # leg removed outright.
            carried = branch["carried"]
            carried = _State(
                branch["entry"].document,
                carried.execution | leg_end.execution,
                leg_end.content,
                leg_end.cohorts,
            )
            branch["carried"] = carried
            branch["index"] += 1
            if branch["index"] < len(branch["edges"]):
                edge = branch["edges"][branch["index"]]
                branch["first"] = len(normal_exits)
                work.append(("branch_leg_done", branch))
                work.append(("visit", edge.target_node_id, (
                    _State(branch["entry"].document, carried.execution, carried.content, carried.cohorts),
                    (branch["node"].node_id, edge.leg_ordinal or edge.local_ordinal),
                ) + branch["after"]))
                continue
            # ONE completion per leg that can finish: its own document copies, and
            # the execution state every leg together guarantees. A leg with no
            # normal end contributes none, so an all-throwing branch promises
            # nothing rather than promising the meet of abnormal paths.
            del normal_exits[branch["recorded_before"]:]
            for leg_document in branch["leg_documents"]:
                normal_exits.append(_State(leg_document, branch["guaranteed"]))
            returned = carried
            continue

        if tag == "decision_arm_done":
            decision = frame[1]
            only_threw = (
                len(threw) > decision["before_threw"]
                and len(normal_exits) == decision["before_normal"]
            )
            if not only_threw:
                decision["results"].append(returned)
            decision["index"] += 1
            if decision["index"] < len(decision["edges"]):
                decision["before_normal"] = len(normal_exits)
                decision["before_threw"] = len(threw)
                work.append(("decision_arm_done", decision))
                work.append(("visit", decision["edges"][decision["index"]].target_node_id,
                             (decision["state"], decision["leg"]) + decision["after"]))
                continue
            if not decision["results"]:
                returned = decision["state"]
                continue
            merged = decision["results"][0]
            for item in decision["results"][1:]:
                merged = merged.merged_with(item)
            returned = merged
            continue

        if tag == "try_edge_done":
            scope = frame[1]
            scope["content"] = scope["content"] | returned.content
            scope["cohorts"] = scope["cohorts"] | returned.cohorts
            scope["index"] += 1
            if scope["index"] < len(scope["edges"]):
                edge = scope["edges"][scope["index"]]
                writers, on_documents, invalidated, stream, legacy = scope["after"]
                work.append(("try_edge_done", scope))
                work.append(("visit", edge.target_node_id, (
                    scope["state"], scope["leg"], writers, on_documents, invalidated,
                    _edge_stream(edge, stream), legacy,
                )))
                continue
            returned = _State(
                scope["state"].document, scope["state"].execution, scope["content"], scope["cohorts"]
            )
            continue

        if tag == "sequential_edge_done":
            sequence = frame[1]
            sequence["index"] += 1
            if sequence["index"] < len(sequence["edges"]):
                work.append(("sequential_edge_done", sequence))
                work.append(("visit", sequence["edges"][sequence["index"]].target_node_id,
                             (sequence["state"], sequence["leg"]) + sequence["after"]))
                continue
            # The last successor's returned state stands, as it did in the recursion.
            continue

        raise AssertionError("unknown lineage controller frame")  # pragma: no cover

    # The MEET over normal exits. Using the traversal's returned state instead
    # answered a different question: `try_catch` hands back its SCOPE-ENTRY
    # state, so a key written on the try path AND on the catch path — a genuine
    # guarantee under any outcome — was reported as established by neither.
    # With no normal exit at all there is nothing to promise.
    established = None
    for at_exit in normal_exits:
        established = at_exit if established is None else established.merged_with(at_exit)
    return LineageWalkV1(
        findings=tuple(findings),
        unestablished_reads=tuple(sorted(set(unmet))),
        established_at_exit=(
            ()
            if established is None
            else tuple(sorted(established.document | established.execution))
        ),
        entry_requirements=tuple(entry_requirements),
        entry_requirement_refs=tuple(entry_requirement_refs),
        cache_requirement_refs=tuple(cache_requirement_refs),
        unestablished_bindings=tuple(sorted(
            set(unestablished_bindings), key=lambda row: (row[0], row[1], row[2] or "")
        )),
        profile_proof=profile_proof,
    )


def collect_lineage_findings(
    prepared: PreparedProcessValidationV1,
    capabilities: ProcessIRValidationCapabilitiesV1 = DEFAULT_VALIDATION_CAPABILITIES,
) -> Tuple[ValidationDiagnosticV1, ...]:
    """The validation phase's view of the walk: its diagnostics."""
    return _walk_lineage(prepared, capabilities).findings


def walk_lineage(
    prepared: PreparedProcessValidationV1,
    capabilities: ProcessIRValidationCapabilitiesV1 = DEFAULT_VALIDATION_CAPABILITIES,
) -> LineageWalkV1:
    """The whole walk, for a caller that needs the state sets themselves.

    Exposed so a summary of what a process REQUIRES and GUARANTEES is read off
    the traversal that already models Branch leg ordering, Decision meet and
    the catch fork — rather than re-derived by a second, weaker scan.

    The trusted context is put in the prepared graph's canonical cache spelling here,
    because a derivation calls this directly rather than through the validation
    pipeline.
    """
    return _walk_lineage(
        prepared,
        canonical_cache_capabilities(capabilities, canonical_cache_refs(prepared.symbols)),
    )


def _leg_member_index(
    prepared: PreparedProcessValidationV1,
) -> Dict[Tuple[str, int], FrozenSet[str]]:
    """``(branch_node_id, leg_ordinal) -> every node id inside that leg``.

    The single definition of "what is in a leg". Built by walking each leg's
    subtree, which is bounded by the leg's own reachable set — a leg cannot
    re-enter its Branch in a forward-only CFG.

    Both the lineage write index and the ordering phase's execution-order walk
    read it, so the two cannot disagree about leg membership.
    """
    index: Dict[Tuple[str, int], FrozenSet[str]] = {}
    for node in prepared.cfg.nodes:
        if node.semantic.semantic_kind != "branch":
            continue
        for edge in prepared.successors(node.node_id):
            ordinal = edge.leg_ordinal or edge.local_ordinal
            seen: Set[str] = set()
            stack = [edge.target_node_id]
            while stack:
                current = stack.pop()
                if current in seen:
                    continue
                seen.add(current)
                if prepared.node(current) is None:
                    continue
                for out in prepared.successors(current):
                    stack.append(out.target_node_id)
            index[(node.node_id, ordinal)] = frozenset(seen)
    return index


def _leg_write_index(
    prepared: PreparedProcessValidationV1,
    capabilities: ProcessIRValidationCapabilitiesV1 = DEFAULT_VALIDATION_CAPABILITIES,
) -> Dict[Tuple[str, int], FrozenSet[StateKey]]:
    """``(branch_node_id, leg_ordinal) -> keys written anywhere in that leg``."""
    index: Dict[Tuple[str, int], FrozenSet[StateKey]] = {}
    for leg, members in _leg_member_index(prepared).items():
        written: Set[StateKey] = set()
        for node_id in members:
            inner = prepared.node(node_id)
            if inner is None:
                continue
            written.update(_writes_of(inner.semantic))
            # Trusted writes count here too. The main traversal treats them
            # as establishing state, so omitting them made the later-leg
            # check blind to a contract write and silently downgraded a
            # reverse-leg dependency to "not written anywhere".
            # EXACT writes, async included. This index answers "WHERE is the
            # write", and `wait` does not move it: a later-leg write is a
            # later-leg write, so the precise BRANCH_ORDER_INVALID still
            # applies. Filtering here downgraded it to a generic missing-write.
            for effect in _trusted_effects(inner.semantic, capabilities):
                written.update((k[0], k[1]) for k in effect.writes)
        index[leg] = frozenset(written)
    return index


def _written_in_a_later_leg(
    leg_writes: Dict[Tuple[str, int], FrozenSet[StateKey]],
    leg: Optional[Tuple[str, int]],
    key: StateKey,
) -> bool:
    """Whether ``key`` is written in a leg that runs AFTER the current one."""
    if leg is None:
        return False
    branch_id, ordinal = leg
    return any(
        key in written
        for (other_branch, other_ordinal), written in leg_writes.items()
        if other_branch == branch_id and other_ordinal > ordinal
    )


def _written_anywhere(
    prepared: PreparedProcessValidationV1,
    key: StateKey,
    capabilities: ProcessIRValidationCapabilitiesV1 = DEFAULT_VALIDATION_CAPABILITIES,
) -> bool:
    """Whether any node in the CFG writes this key, ignoring reachability.

    Used only to sharpen a DDP diagnostic from "never written" to "written on a
    different document copy". It deliberately ignores paths: the question is
    whether the author wrote it at all, not whether it reaches the read.
    """
    for node in prepared.cfg.nodes:
        if key in _writes_of(node.semantic):
            return True
        # ANY write, async included: this asks whether an author wrote the key
        # ANYWHERE, not whether it establishes downstream state. Filtering here
        # made a cross-copy DDP read validate silently — a false NEGATIVE.
        for effect in _trusted_effects(node.semantic, capabilities):
            if key in [(k[0], k[1]) for k in effect.writes]:
                return True
    return False


__all__ = ["LineageWalkV1", "collect_lineage_findings", "walk_lineage"]

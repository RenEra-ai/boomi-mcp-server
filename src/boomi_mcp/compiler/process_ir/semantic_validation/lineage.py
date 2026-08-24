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
from typing import Dict, FrozenSet, List, Mapping, NamedTuple, Optional, Set, Tuple

from ....errors import (
    PROCESS_IR_SEMANTIC_LINEAGE_BRANCH_ORDER_INVALID,
    PROCESS_IR_SEMANTIC_LINEAGE_CACHE_WRITER_MISSING,
    PROCESS_IR_SEMANTIC_LINEAGE_DDP_SCOPE_INVALID,
    PROCESS_IR_SEMANTIC_LINEAGE_EFFECT_UNKNOWN,
    PROCESS_IR_SEMANTIC_LINEAGE_EXTERNAL_WRITER_ASSUMED,
    PROCESS_IR_SEMANTIC_LINEAGE_PROPERTY_READ_BEFORE_WRITE,
)
from .contracts import (
    DEFAULT_VALIDATION_CAPABILITIES,
    ProcessIRValidationCapabilitiesV1,
    StateEffectV1,
    ValidationDiagnosticV1,
)
from .context import PreparedProcessValidationV1
from .findings import finding

_LINEAGE_PHASE = "lineage"

#: Scope tokens. Kept as plain strings (not an enum) because they are also
#: evidence values, and the evidence vocabulary is lowercase tokens.
DDP = "ddp"
DPP = "dpp"
CACHE = "cache"

#: A key identifying one piece of state: its scope plus its name.
StateKey = Tuple[str, str]


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
                # A cache may legitimately be populated outside this process, so
                # a read with no in-process writer is reported only when the
                # caller has NOT declared an external writer.
                "read_before_write": "rejected_unless_external_writer_declared",
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

    __slots__ = ("document", "execution")

    def __init__(
        self,
        document: Optional[FrozenSet[StateKey]] = None,
        execution: Optional[FrozenSet[StateKey]] = None,
    ) -> None:
        self.document: FrozenSet[StateKey] = document or frozenset()
        self.execution: FrozenSet[StateKey] = execution or frozenset()

    def with_write(self, key: StateKey) -> "_State":
        if key[0] in _DOCUMENT_LIFETIME_SCOPES:
            return _State(self.document | {key}, self.execution)
        return _State(self.document, self.execution | {key})

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
        return _State(self.document, self.execution)

    def merged_with(self, other: "_State") -> "_State":
        """Meet over converging paths: only what BOTH establish survives.

        Intersection, not union. A Decision writes a property on the true arm
        only; after the merge that property is established on one path and not
        the other, so it is not established at all. Union here would be the
        single easiest way to make this whole module unsound.
        """
        return _State(
            self.document & other.document, self.execution & other.execution
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
    #: Whether the walk hit its depth bound and stopped short of some path.
    #:
    #: The bound is a HANG GUARD, and for reporting findings, stopping short
    #: merely means the deepest nodes go unreported. For any caller that
    #: TRUSTS the state sets, it means something else entirely: both sets are
    #: silently partial, so an exact-looking summary omits whatever lay past
    #: the cutoff. Such a caller must treat a truncated walk as no answer.
    truncated: bool


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

    The walk is a depth-first traversal that carries a ``_State`` along each
    path. It is bounded by ``visited`` on ``(node_id, state fingerprint)`` so a
    graph the compiler has not yet rejected as cyclic cannot hang it.
    """
    findings: List[ValidationDiagnosticV1] = []
    reported: Set[Tuple[str, str]] = set()
    # Every unestablished read, recorded BEFORE `_report` dedups by
    # (code, node): two different keys unmet at one node collapse to a single
    # finding, and dropping the second key with the duplicate finding would
    # under-report the dependency set.
    unmet: List[StateKey] = []
    # A list rather than a flag so the nested `_visit` can set it without a
    # `nonlocal` declaration, matching how `findings` and `unmet` are handled.
    truncated: List[bool] = []
    # State at every NORMAL exit — a path ending on `stop` or `return_documents`.
    # A guarantee is what holds however the process finishes, so it is the meet
    # over these, not the state the traversal happens to carry back. An
    # `exception` terminal is excluded: it ends the process abnormally, so what
    # it had written is no promise to anyone. A terminal-less path end (a Branch
    # leg, a cache-staging leg) is not a process exit at all.
    normal_exits: List[_State] = []
    leg_writes = _leg_write_index(prepared, capabilities)

    def _report(code: str, node, severity="error", evidence=()) -> None:
        # One finding per (code, node). The report dedups too, but stopping the
        # duplicate here keeps a diamond-shaped graph from generating the same
        # finding once per path.
        key = (code, node.node_id)
        if key in reported:
            return
        reported.add(key)
        findings.append(
            finding(
                code,
                severity,
                _LINEAGE_PHASE,
                node.source_path,
                evidence=evidence,
                internal_node_id=node.node_id,
            )
        )

    def _classify_unmet_read(node, semantic, key, leg, extra=()) -> None:
        """Report ONE unestablished read under the sharpest code that fits.

        Shared by both read paths. The refinements below are what make a
        lineage finding actionable, and which one applies is a property of the
        READ — its scope, and where its writer sits in the graph — never of who
        declared it. Reporting the flat fallback for a contract's declared read
        while an identical authored read got ``…BRANCH_ORDER_INVALID`` made the
        diagnostic depend on the reader's provenance, which is the mirror image
        of the writer-side asymmetry fixed alongside it.
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
        elif scope == DDP and _written_anywhere(prepared, key, capabilities):
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

    def _visit(node_id: str, state: _State, depth: int, leg=None) -> _State:
        node = prepared.node(node_id)
        if node is None:
            return state
        if depth > 256:
            truncated.append(True)
            return state

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
            _classify_unmet_read(node, semantic, key, leg)

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
                    node, semantic, key, leg, extra=(("effect_kind", "declared_read"),)
                )
            # A trusted contract contributes EXACT writes, visible to the next
            # contract ON THIS NODE — one data_process can carry several
            # contracted scripts and they run in sequence, so the walk over
            # them has to be sequential too.
            if not establishes:
                continue
            for key in effect.writes:
                state = state.with_write((key[0], key[1]))

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
            state = state.with_write(key)

        # --- successors -----------------------------------------------------
        edges = prepared.successors(node_id)
        if not edges:
            # EVERY path end that is not abnormal is a completion. Which ones
            # a Branch may meet together is decided at the Branch, per
            # COMPARTMENT — not here, by suppressing some of them.
            role = node.exit_role
            if role is not None and role not in _ABNORMAL_EXIT_ROLES:
                normal_exits.append(state)
            return state

        if semantic.semantic_kind == "branch":
            # Legs run SEQUENTIALLY in local-ordinal order. Execution-scoped
            # writes accumulate from one leg into the next; document state does
            # not, because each leg re-copies the pre-Branch documents.
            entry = state.entering_branch_leg()
            carried = entry
            recorded_before = len(normal_exits)
            # Per leg: the state its own completions agree on. Collected here
            # rather than read off `carried`, because `carried` is built from
            # CONTINUATIONS and a continuation is a meet — a leg ending in a
            # Decision with one throwing arm hands back a state missing whatever
            # only the normal arm wrote.
            leg_documents = []
            guaranteed_execution = entry.execution
            for edge in edges:
                first = len(normal_exits)
                leg_end = _visit(
                    edge.target_node_id,
                    _State(entry.document, carried.execution),
                    depth + 1,
                    (node.node_id, edge.leg_ordinal or edge.local_ordinal),
                )
                completions = normal_exits[first:]
                if completions:
                    leg_document = completions[0].document
                    leg_execution = completions[0].execution
                    for other in completions[1:]:
                        leg_document = leg_document & other.document
                        leg_execution = leg_execution & other.execution
                    leg_documents.append(leg_document)
                    # every leg RUNS, so what a leg guarantees holds afterwards
                    guaranteed_execution = guaranteed_execution | leg_execution
                carried = _State(entry.document, carried.execution | leg_end.execution)
            # ONE completion per leg that can finish: its own document copies,
            # and the execution state every leg together guarantees. A leg with
            # no normal end contributes none, so an all-throwing branch promises
            # nothing rather than promising the meet of abnormal paths.
            del normal_exits[recorded_before:]
            for leg_document in leg_documents:
                normal_exits.append(_State(leg_document, guaranteed_execution))
            return carried

        if semantic.semantic_kind == "decision":
            # Arms are EXCLUSIVE. Meet, not union.
            results = [_visit(e.target_node_id, state, depth + 1, leg) for e in edges]
            merged = results[0]
            for item in results[1:]:
                merged = merged.merged_with(item)
            return merged

        if semantic.semantic_kind == "try_catch":
            # The catch path forks from SCOPE-ENTRY state plus the caught
            # document. A write inside the try body may not have happened when
            # the failure occurred, so it cannot be assumed visible to catch.
            for edge in edges:
                _visit(edge.target_node_id, state, depth + 1, leg)
            return state

        result = state
        for edge in edges:
            result = _visit(edge.target_node_id, state, depth + 1, leg)
        return result

    entry_state = _State()
    for key in capabilities.established_at_entry:
        entry_state = entry_state.with_write((key[0], key[1]))
    _visit(prepared.cfg.entry_node_id, entry_state, 0)
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
        truncated=bool(truncated),
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
    """
    return _walk_lineage(prepared, capabilities)


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

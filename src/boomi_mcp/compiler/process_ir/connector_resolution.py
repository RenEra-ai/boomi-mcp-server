"""ConnectorCall reference resolution and flow semantics (issue #140, M12.5).

Two passes over the lowered CFG, both running BEFORE any emission plan exists —
so a rejected payload never reaches an emitter, let alone a component mutation:

1. :func:`resolve_connector_call_bindings` — turn each ``connector_call`` node
   into a fully-resolved :class:`ConnectorCallBindingV1` (operation symbol,
   derived connection symbol, canonical family/action, capability row, profile
   refs). Reference/capability failures land here.
2. :func:`validate_connector_call_semantics` — check the *flow*: document
   cardinality across the ordered calls, and profile continuity around every
   ``map_ref``.

Why the binding table is not threaded into the emission plan: the emitter input
for a connector call is derived from the CFG node plus the symbol index by
``lowering._emitter_input_for``, and ``invariants.check_emission_plan_invariants``
RE-DERIVES every emitter input through that same function and compares exactly.
That recomputation is what makes the plan check total. Carrying a second,
separately-built copy of the same facts into the plan would give the checker
something to compare against itself instead of against the symbol table, which is
strictly weaker. So the bindings live here, in the validation phase, and nothing
downstream depends on them.

Security (ADR-001 §11): every message is a static string selected by code. No
ref, component id, family, action, or profile value is ever interpolated — an
error names the authored JSON path and nothing else.
"""

from __future__ import annotations

from typing import Any, List, Mapping, Optional, Tuple

from pydantic import Field

from ...errors import (
    PROCESS_IR_CAPABILITY_CONNECTOR_ACTION_UNSUPPORTED,
    PROCESS_IR_REFERENCE_CONNECTION_MISMATCH,
    PROCESS_IR_REFERENCE_CONNECTION_NOT_FOUND,
    PROCESS_IR_REFERENCE_OPERATION_NOT_FOUND,
    PROCESS_IR_SEMANTIC_CARDINALITY_MISMATCH,
    PROCESS_IR_SEMANTIC_PROFILE_MISMATCH,
)
from .connector_capabilities import (
    CONNECTOR_CALL_CAPABILITIES_V1,
    ConnectorCapabilityV1,
    canonicalize_connector_metadata,
    lookup_capability,
)
from .contracts import ComponentSymbolV1, SemanticCfgV1, SymbolTableV1, _CompilerModel
from .diagnostics import raise_compile_error
from .error_handling import validate_error_handling

CONNECTOR_ACTION_COMPONENT_TYPE = "connector-action"
CONNECTOR_SETTINGS_COMPONENT_TYPE = "connector-settings"
MAP_COMPONENT_TYPE = "transform.map"

#: Boomi's profile component types. A profile REFERENCE must resolve to one of
#: these or it is not a profile identity at all — without this, two refs both
#: pointing at (say) a connection component would compare equal and the map
#: continuity check would "pass" having verified nothing.
#:
#: A closed set rather than a ``profile.`` prefix test: fail-closed is the rule
#: everywhere else in this module, and a new profile kind should be a deliberate
#: addition. All five documented kinds are listed, not just the three the emitter
#: registry validates for Data Process steps — a map may legitimately read an EDI
#: or flat-file profile, and narrowing here would falsely reject a valid flow.
PROFILE_COMPONENT_TYPES = frozenset(
    {"profile.db", "profile.edi", "profile.flatfile", "profile.json", "profile.xml"}
)


class ConnectorCallBindingV1(_CompilerModel):
    """One fully-resolved connector call. Compiler-internal; never serialized out."""

    node_id: str = Field(..., min_length=1)
    source_path: str = Field(..., min_length=1)
    ordinal: int = Field(..., ge=1)
    role: str = Field(..., min_length=1)
    operation_ref: str = Field(..., min_length=1)
    connection_ref: str = Field(..., min_length=1)
    family: str = Field(..., min_length=1)
    action: str = Field(..., min_length=1)
    capability: ConnectorCapabilityV1
    input_profile_ref: Optional[str] = None
    output_profile_ref: Optional[str] = None


def _symbol(index: Mapping[str, Any], ref: Optional[str]) -> Optional[ComponentSymbolV1]:
    if not ref:
        return None
    return index.get(ref)


def _canonical_type(symbol: ComponentSymbolV1) -> str:
    return str(symbol.component_type or "").strip().casefold()


def resolve_connector_call_bindings(
    cfg: SemanticCfgV1, symbols: SymbolTableV1
) -> Tuple[ConnectorCallBindingV1, ...]:
    """Resolve every ``connector_call`` node, in CFG order.

    The index is built ONCE: a per-reference scan would make this
    O(nodes x symbols), and ``SequenceNodeV1.steps`` has no upper bound.
    """
    index = symbols.build_index()
    bindings: List[ConnectorCallBindingV1] = []

    for ordinal, node in enumerate(cfg.nodes, start=1):
        semantic = node.semantic
        if semantic.semantic_kind != "connector_call":
            continue
        path = node.source_path
        operation_path = "{0}/operation_ref".format(path)

        operation = _symbol(index, semantic.operation_ref)
        if operation is None or _canonical_type(operation) != CONNECTOR_ACTION_COMPONENT_TYPE:
            raise raise_compile_error(
                PROCESS_IR_REFERENCE_OPERATION_NOT_FOUND,
                "reference_resolution",
                operation_path,
                internal_node_id=node.node_id,
            )

        # ORDER MATTERS. The capability gate is settled first, from the operation
        # alone, because it is the coarser question: if the family/action is not
        # supported at all, complaining about the connection sends the reader to
        # fix something that was never the problem. Connection resolution then
        # refines a call whose family/action IS supported.
        if not operation.connector_type or not operation.action_type:
            # Without a family/action there is nothing to look a capability up
            # by. Reported as unsupported rather than as a compiler defect: the
            # symbol table is caller-supplied context, so this is a defect in
            # what was supplied, not in the compiler.
            raise raise_compile_error(
                PROCESS_IR_CAPABILITY_CONNECTOR_ACTION_UNSUPPORTED,
                "reference_resolution",
                operation_path,
                internal_node_id=node.node_id,
            )

        family, action = canonicalize_connector_metadata(
            operation.connector_type, operation.action_type
        )
        if not family or not action:
            raise raise_compile_error(
                PROCESS_IR_CAPABILITY_CONNECTOR_ACTION_UNSUPPORTED,
                "reference_resolution",
                operation_path,
                internal_node_id=node.node_id,
            )

        capability = lookup_capability(family, action)
        if capability is None:
            # Point at the field the caller can actually act on. Blaming
            # ``/action`` merely because one was authored is misleading when the
            # FAMILY is the unsupported half — the action may be perfectly valid
            # and the caller would "fix" a correct field. Only claim the action
            # when the same family supports some OTHER action; otherwise the
            # operation reference (i.e. the whole connector) is the problem.
            family_has_any_action = any(
                key_family == family for key_family, _ in CONNECTOR_CALL_CAPABILITIES_V1
            )
            blame_action = semantic.action_intent is not None and family_has_any_action
            raise raise_compile_error(
                PROCESS_IR_CAPABILITY_CONNECTOR_ACTION_UNSUPPORTED,
                "reference_resolution",
                "{0}/action".format(path) if blame_action else operation_path,
                internal_node_id=node.node_id,
            )

        # The authored action is an ASSERTION, never an override: it is compared
        # against the authoritative action and can only ever reject.
        if semantic.action_intent is not None:
            asserted = semantic.action_intent.strip().casefold()
            if asserted != action.casefold():
                raise raise_compile_error(
                    PROCESS_IR_CAPABILITY_CONNECTOR_ACTION_UNSUPPORTED,
                    "reference_resolution",
                    "{0}/action".format(path),
                    internal_node_id=node.node_id,
                )

        # The operation->connection edge is a fact of the COMPONENT PLAN, carried
        # on the operation symbol: no connector-action component declares its own
        # connection (see the #140 capture ledger), so there is nothing to infer
        # from and a missing binding is a hard error rather than a guess.
        connection = _symbol(index, operation.connection_ref)
        if connection is None:
            raise raise_compile_error(
                PROCESS_IR_REFERENCE_CONNECTION_NOT_FOUND,
                "reference_resolution",
                operation_path,
                internal_node_id=node.node_id,
            )

        if _canonical_type(connection) != CONNECTOR_SETTINGS_COMPONENT_TYPE:
            raise raise_compile_error(
                PROCESS_IR_REFERENCE_CONNECTION_MISMATCH,
                "reference_resolution",
                operation_path,
                internal_node_id=node.node_id,
            )
        # The connection MUST declare its family, and it must agree canonically
        # with the operation's. Treating an omitted family as "nothing to check"
        # would be fail-OPEN: the emitted shape carries the OPERATION's family
        # next to this connection's id, so a mis-wired plan would serialise
        # `connectorType="<rest>"` pointing at a database connection and nothing
        # would have objected. The emitter does not need the connection's family,
        # but this verification does, and #140's contract is that the binding is
        # *verified* — not merely resolved.
        #
        # This tightens nothing that exists: #139's adapters put connector
        # metadata only on the OPERATION requirement, and their symbols carry no
        # `connection_ref`, so no pre-#140 symbol reaches this path at all.
        if not connection.connector_type:
            raise raise_compile_error(
                PROCESS_IR_REFERENCE_CONNECTION_MISMATCH,
                "reference_resolution",
                operation_path,
                internal_node_id=node.node_id,
            )
        connection_family, _ = canonicalize_connector_metadata(
            connection.connector_type, ""
        )
        if connection_family != family:
            raise raise_compile_error(
                PROCESS_IR_REFERENCE_CONNECTION_MISMATCH,
                "reference_resolution",
                operation_path,
                internal_node_id=node.node_id,
            )

        bindings.append(
            ConnectorCallBindingV1(
                node_id=node.node_id,
                source_path=path,
                ordinal=ordinal,
                role=semantic.role,
                operation_ref=semantic.operation_ref,
                connection_ref=operation.connection_ref,
                family=family,
                action=action,
                capability=capability,
                input_profile_ref=operation.input_profile_ref,
                output_profile_ref=operation.output_profile_ref,
            )
        )

    return tuple(bindings)


def _profile_identity(
    index: Mapping[str, Any], ref: Optional[str]
) -> Optional[Tuple[str, str]]:
    """``(component id, normalized profile type)`` for a profile reference.

    Compares by resolved COMPONENT ID, not by ref token: two different refs may
    legitimately name one component (that is exactly what #139B's
    occurrence-scoped aliases do), and rejecting that would be wrong.

    ``None`` for anything that is not a real profile component. Returning an
    identity for an arbitrary component type would make the continuity check
    self-fulfilling: two refs both pointing at the same *connection* (or map, or
    process) would compare equal and the map would "match" while neither side is
    a profile at all.
    """
    symbol = _symbol(index, ref)
    if symbol is None:
        return None
    component_type = _canonical_type(symbol)
    if component_type not in PROFILE_COMPONENT_TYPES:
        return None
    return (symbol.component_id, component_type)


def validate_connector_call_semantics(
    cfg: SemanticCfgV1,
    bindings: Tuple[ConnectorCallBindingV1, ...],
    symbols: SymbolTableV1,
) -> None:
    """Check document cardinality across the calls and profiles around each map.

    Runs only for a connector-call flow; a legacy source/target or process-call
    CFG produces no bindings and returns immediately, so no existing dialect can
    change behaviour.
    """
    if not bindings:
        return

    index = symbols.build_index()
    binding_by_node = {binding.node_id: binding for binding in bindings}

    # Every DECLARED profile reference must resolve to a real profile component,
    # on every call — not only on the two calls that happen to sit beside a map.
    # This is distinct from profile EQUALITY, which stays MapRef-only because the
    # platform documents connector profiles as non-validating: a ref that names a
    # connection or a map is not a weaker match, it is not a profile at all, and
    # accepting it would let the compiler claim it "verified profiles" (an
    # acceptance criterion) having verified nothing.
    #
    # An ABSENT ref is deliberately NOT an error here: official documentation
    # describes connector profiles as optional ("Request Profile … when
    # provided"), and per-family/action required-vs-optional evidence does not
    # exist in this checkout. Where a profile IS required — the two sides of a
    # map boundary — absence is already a mismatch in the map pass below.
    for binding in bindings:
        for ref in (binding.input_profile_ref, binding.output_profile_ref):
            if ref is None:
                continue
            if _profile_identity(index, ref) is None:
                raise raise_compile_error(
                    PROCESS_IR_SEMANTIC_PROFILE_MISMATCH,
                    "semantic_lowering",
                    "{0}/operation_ref".format(binding.source_path),
                    internal_node_id=binding.node_id,
                )

    _walk_paths(cfg, index, binding_by_node)


# ---------------------------------------------------------------------------
# Per-path dataflow (#141 M12.6)
# ---------------------------------------------------------------------------

#: Nodes that REPLACE the document stream, so a call downstream of one has
#: documents regardless of what came before.
_STREAM_PRODUCING_KINDS = frozenset({"cache_get", "document_cache_retrieve"})

#: The only kinds that may follow a call which produces no documents: a plain
#: ``stop`` (consumes nothing, just ends the path) and a stream-replacing read
#: (supplies its own documents). Everything else would be emitted downstream of a
#: shape that hands it nothing.
_MAY_FOLLOW_NON_PRODUCER = frozenset({"stop"}) | _STREAM_PRODUCING_KINDS

#: Kinds that do NOT break a map's pairing with its upstream call. A connector
#: call sets the pairing; Branch/Decision merely route the documents onward
#: without altering them, so they carry it through into each body.
#: #142 adds ``try_catch`` for the same reason: it routes documents down one of
#: two paths without altering them, so a `call -> try_catch -> [map, call]` shape
#: stays profile-checkable. The CATCH edge is handled separately below — it does
#: not inherit the protected path's state.
_MAP_PAIRING_TRANSPARENT = frozenset(
    {"connector_call", "branch", "decision", "try_catch"}
)


class _PathState:
    """Document state carried down ONE root-to-leaf path.

    Copied — never shared — across a control edge. That copy IS the sibling
    isolation: before #141 the checker walked a single flattened list of every
    call and map in CFG order, which is only correct while the flow is linear.
    With calls inside Branch legs that list interleaves independent paths, so leg
    2's first call would be judged against leg 1's last one.
    """

    __slots__ = (
        "producer", "producer_binding", "blocked_by", "pending_map", "map_upstream", "saw_call",
    )

    def __init__(
        self,
        producer=None,
        producer_binding=None,
        blocked_by=None,
        pending_map=None,
        map_upstream=None,
        saw_call=False,
    ):
        #: truthy when SOMETHING upstream on this path yields documents — a
        #: producing connector_call, a legacy source endpoint, or a cache read.
        #: Used only for the documents_required check, which does not care which.
        self.producer = producer
        #: the upstream producer WHEN it is a connector-call binding. Kept apart
        #: from ``producer`` because only a binding carries profile refs: a legacy
        #: source endpoint produces documents but has no ``output_profile_ref``,
        #: and treating it as a map's upstream would crash the profile compare.
        self.producer_binding = producer_binding
        #: a non-producing call seen on this path, which nothing may follow
        self.blocked_by = blocked_by
        #: a map awaiting its downstream call, so its profiles can be compared
        self.pending_map = pending_map
        #: the call binding immediately upstream of ``pending_map``
        self.map_upstream = map_upstream
        #: whether ANY connector call has run on this path. Distinguishes a
        #: connector flow (where an unbracketed map is a real continuity hole)
        #: from a pure legacy source/target flow (where a map never had a
        #: call-to-call profile pair to check and never did before #141).
        self.saw_call = saw_call

    def copy(self) -> "_PathState":
        return _PathState(
            self.producer,
            self.producer_binding,
            self.blocked_by,
            self.pending_map,
            self.map_upstream,
            self.saw_call,
        )


def _cardinality_failure(path: str, node_id: str):
    raise raise_compile_error(
        PROCESS_IR_SEMANTIC_CARDINALITY_MISMATCH,
        "semantic_lowering",
        path,
        internal_node_id=node_id,
    )


def _profile_failure(path: str, node_id: str):
    raise raise_compile_error(
        PROCESS_IR_SEMANTIC_PROFILE_MISMATCH,
        "semantic_lowering",
        path,
        internal_node_id=node_id,
    )


def _check_map_pair(index, map_node, before, after) -> None:
    """Profile continuity across one map, between two connector calls."""
    map_path = "{0}/map_ref".format(map_node.source_path)
    map_symbol = _symbol(index, getattr(map_node.semantic, "map_ref", None))
    if map_symbol is None or _canonical_type(map_symbol) != MAP_COMPONENT_TYPE:
        _profile_failure(map_path, map_node.node_id)

    upstream = _profile_identity(index, before.output_profile_ref)
    map_source = _profile_identity(index, map_symbol.input_profile_ref)
    map_target = _profile_identity(index, map_symbol.output_profile_ref)
    downstream = _profile_identity(index, after.input_profile_ref)

    # A map's source/destination profiles are hard component requirements
    # (unlike connector request/response profiles, which the platform documents
    # as non-validating), so an ABSENT profile on either side is a mismatch too —
    # there is nothing to satisfy the requirement with.
    if map_source is None or map_target is None or upstream is None or downstream is None:
        _profile_failure(map_path, map_node.node_id)
    if map_source != upstream or map_target != downstream:
        _profile_failure(map_path, map_node.node_id)


def _walk_paths(cfg: SemanticCfgV1, index, binding_by_node) -> None:
    """Depth-first walk of the CFG, carrying independent state down each path.

    The CFG is a tree here (``check_cfg_invariants`` has already rejected joins
    and cycles), so a plain DFS visits every root-to-leaf path exactly once and
    cannot loop.
    """
    by_id = {node.node_id: node for node in cfg.nodes}
    outgoing: dict = {}
    for edge in cfg.edges:
        outgoing.setdefault(edge.source_node_id, []).append(edge)

    stack = [(cfg.entry_node_id, _PathState())]
    while stack:
        node_id, state = stack.pop()
        node = by_id[node_id]
        kind = node.semantic.semantic_kind

        # --- the Send gate, applied to EVERY downstream node ------------------
        # A non-producing call returns no documents to the process, so nothing
        # downstream on this path can run. Before #141 only a call or a map could
        # follow one (the root connector_call sequence admits nothing else), so
        # checking those two kinds was complete. Rich bodies broke that: a leg may
        # now put a message, a routed target, a nested control or a cache write
        # after a Send, none of which could ever execute.
        #
        # Only two things may legally follow: a plain ``stop`` (it consumes
        # nothing and merely ends the path — the legacy [target, stop] shape), and
        # a stream-replacing cache read, which supplies its own documents and so
        # genuinely restarts the stream.
        if state.blocked_by is not None and kind not in _MAY_FOLLOW_NON_PRODUCER:
            # Blame the SEND — the node whose position is wrong, since it must be
            # last on its path — EXCEPT for a Return Documents terminal, where
            # #140 shipped the pointer on the terminal itself. That asymmetry is
            # #140's, and #141 has no reason to move a pointer callers already
            # key on; the generalization changes which nodes are caught, never
            # where an already-caught one is reported.
            if node.exit_role == "return_documents":
                _cardinality_failure(node.source_path, node.node_id)
            _cardinality_failure(
                "{0}/operation_ref".format(state.blocked_by.source_path),
                state.blocked_by.node_id,
            )

        # --- a pending map must be answered by the very next call -------------
        # #140 enforces "a map_ref is immediately followed by a connector_call" in
        # the MODEL, but only for a root connector_call sequence. Inside a control
        # body a map is an ordinary linear step, so the model cannot state it and
        # this walk must: without it `[call, mapA, mapB, call]` silently overwrote
        # mapA and validated only mapB, and a map followed by a terminal was
        # dropped unchecked. Either way the compiler would have claimed to verify
        # profiles it never compared.
        if state.pending_map is not None and kind != "connector_call":
            _profile_failure(
                "{0}/map_ref".format(state.pending_map.source_path),
                state.pending_map.node_id,
            )

        if kind == "connector_call":
            binding = binding_by_node[node_id]
            capability = binding.capability
            call_path = "{0}/operation_ref".format(binding.source_path)

            # The Send gate already fired above for this node if it was blocked,
            # and it fires BEFORE this rule deliberately: a non-producing
            # predecessor also leaves ``producer`` empty, so checking the other
            # order would blame this follower for a defect that belongs to the
            # Send. Root cause before symptom.
            #
            # Nothing on THIS path produced documents, so the call would receive
            # none and never execute.
            if capability.accepts_input == "documents_required" and state.producer is None:
                _cardinality_failure(call_path, binding.node_id)

            if state.pending_map is not None:
                _check_map_pair(index, state.pending_map, state.map_upstream, binding)
                state.pending_map = None
                state.map_upstream = None

            state.saw_call = True
            state.producer = binding if capability.produces_output else None
            state.producer_binding = binding if capability.produces_output else None
            state.blocked_by = None if capability.produces_output else binding

        elif kind == "map":
            # ``producer_binding`` is cleared below by every non-call node, so
            # reaching here with one set means the map's IMMEDIATE predecessor was
            # a producing call. That immediacy is the contract: without it
            # `call -> message -> map -> call` would be treated as bracketed and
            # the map's source profile compared against a call that no longer
            # feeds it.
            # Only a map BETWEEN two connector CALLS can have its profiles
            # verified: the equality contract compares the upstream call's
            # response profile with the map's source, and the map's target with
            # the downstream call's request profile. A map whose upstream is a
            # legacy source endpoint (or a cache read) carries no such pair — it
            # is left unchecked exactly as it was before #141, because claiming
            # to have "verified profiles" there would be a claim about something
            # that was never compared.
            if state.producer_binding is not None:
                state.pending_map = node
                state.map_upstream = state.producer_binding
            elif state.saw_call:
                # A connector call ran upstream but is no longer this map's
                # immediate predecessor, so nothing can be compared against its
                # source profile — the exact continuity hole map bracketing
                # exists to close, and #140 states it in the model for root
                # sequences. A map in a PURE legacy flow (no call anywhere on the
                # path) stays unchecked, as it was before #141.
                _profile_failure(
                    "{0}/map_ref".format(node.source_path), node.node_id
                )

        elif kind == "connector":
            # A legacy source endpoint genuinely produces documents, so a
            # ``documents_required`` call inside a control body downstream of one
            # is legitimate. Reachable only through #141's new shapes (a root
            # ``[source, branch]`` whose leg holds a call), because a pure legacy
            # flow yields no bindings and returns before this walk.
            if node.semantic.role == "source":
                state.producer = node
                state.producer_binding = None
                state.blocked_by = None

        elif kind in _STREAM_PRODUCING_KINDS:
            state.producer = node
            state.producer_binding = None
            state.blocked_by = None
        elif kind == "cache_put":
            # Add to Cache consumes the stream. The model already requires a
            # stream-replacing read immediately after it within the same body.
            state.producer = None
            state.producer_binding = None

        if node.exit_role == "return_documents" and state.blocked_by is not None:
            # ``stop`` merely ends the path, but Return Documents RETURNS the
            # current stream — after a call that produces none it can only ever
            # return nothing.
            _cardinality_failure(node.source_path, node.node_id)

        if kind not in _MAP_PAIRING_TRANSPARENT:
            # A map's upstream must be the call that actually still feeds it.
            # Any node that touches the stream (a Message REPLACES the document,
            # a Data Process rewrites it, a cache write consumes it) breaks the
            # pairing, so `call -> message -> map -> call` is NOT bracketed.
            # Branch/Decision are pure control flow — they route documents
            # without altering them — so they stay transparent; otherwise the
            # ordinary `call -> branch -> [map, call]` shape could never be
            # profile-checked at all. ``producer`` ("do documents exist on this
            # path") is a different question and deliberately survives more.
            state.producer_binding = None

        successors = outgoing.get(node_id, ())
        for position, edge in enumerate(successors):
            # The LAST successor may reuse this state; every earlier one gets its
            # own copy. Sharing it would let leg 1's producer/blocked flags leak
            # into leg 2.
            child = state if position == len(successors) - 1 else state.copy()
            if edge.kind == "catch":
                # #142. A recovery path forks from the state at SCOPE ENTRY —
                # which is ALREADY what this child holds, and that is the whole
                # reason nothing here clears the producer binding.
                #
                # The DFS pushes both children from the state at the ``try_catch``
                # node itself; the protected path's own mutations happen while
                # walking the TRY subtree, a separate branch of the search that
                # this child never passes through. So a Set Properties inside the
                # try body cannot leak here structurally, without being erased.
                #
                # Erasing it anyway would be actively wrong. For a CONNECTOR
                # scope the upstream call's binding IS the scope-entry binding:
                # the document handed to the recovery path is the one that
                # entered the protected path, i.e. that call's output. Clearing
                # it rejected a legitimate `upstream call -> catch map ->
                # downstream call` flow whose profiles lined up exactly. Left
                # alone, the graph answers correctly on its own: a connector
                # scope keeps its upstream binding, and a PROCESS scope has no
                # upstream producer, so a catch map there still fails closed with
                # nothing to compare against — exactly the split the design asks
                # for, with no scope check needed here.
                #
                # Only one fact must be ADDED: a caught document exists on this
                # path even when nothing upstream produced one, because the
                # platform hands the failed document to the recovery path (live
                # evidence: a retried process routed one error document to its
                # catch leg — docs/archive/2026-06-19-issue-91-capstone-recipe-evidence.md).
                # ``saw_call`` additionally keeps an UNBRACKETED catch map failing
                # closed through the existing "a call ran upstream but is not this
                # map's immediate predecessor" rule, rather than being silently
                # skipped the way a pure-legacy map is.
                child = child.copy()
                child.saw_call = True
                if child.producer is None:
                    child.producer = node
            stack.append((edge.target_node_id, child))


def validate_connector_calls(cfg: SemanticCfgV1, symbols: SymbolTableV1) -> None:
    """Every pass, in order. The single entry point the pipeline calls.

    #142's retry safety runs HERE, on the bindings resolved once above, rather
    than as its own pipeline step. Two reasons, both structural:

    * the retry check needs the resolved capability row for each call, and
      resolving a second time would let the safety gate and the emitter disagree
      about which action a reference names;
    * a second public entry point that ran the connector gate but not the retry
      gate would be exactly the "a gate only one of two entry points enforces is
      not a gate" failure this module already guards against elsewhere.

    Order matters: retry safety is checked BEFORE the ordinary flow semantics so
    an unsafe retry is reported as an unsafe retry, rather than surfacing as
    whichever cardinality/profile complaint the same payload happens to trip.
    """
    bindings = resolve_connector_call_bindings(cfg, symbols)
    validate_error_handling(cfg, bindings, symbols)
    validate_connector_call_semantics(cfg, bindings, symbols)


__all__ = [
    "ConnectorCallBindingV1",
    "resolve_connector_call_bindings",
    "validate_connector_call_semantics",
    "validate_connector_calls",
]

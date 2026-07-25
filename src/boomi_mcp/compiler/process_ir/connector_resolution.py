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
    ConnectorCapabilityV1,
    canonicalize_connector_metadata,
    lookup_capability,
)
from .contracts import ComponentSymbolV1, SemanticCfgV1, SymbolTableV1, _CompilerModel
from .diagnostics import raise_compile_error

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
            raise raise_compile_error(
                PROCESS_IR_CAPABILITY_CONNECTOR_ACTION_UNSUPPORTED,
                "reference_resolution",
                (
                    "{0}/action".format(path)
                    if semantic.action_intent is not None
                    else operation_path
                ),
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

    # The ordered linear spine. A connector-call sequence is linear by
    # construction (the model rejects branch/decision in it), so CFG node order
    # is authored order.
    steps = [
        node
        for node in cfg.nodes
        if node.semantic.semantic_kind in ("connector_call", "map")
    ]

    # The terminal is NOT in ``steps`` (it is neither a call nor a map), so the
    # "nothing may follow a non-producing call" rule below cannot see it. It has
    # to be judged separately, because the two terminals differ in exactly the
    # property that rule is about: ``stop`` consumes nothing and merely ends the
    # path (the legacy ``[target, stop]`` shape), while ``return_documents``
    # RETURNS THE CURRENT DOCUMENT STREAM to the caller. Placing the latter after
    # a call that produces no documents emits a Return Documents shape that can
    # never return anything.
    terminal = next((node for node in cfg.nodes if node.exit_role), None)

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

    previous_producer: Optional[ConnectorCallBindingV1] = None
    for position, node in enumerate(steps):
        binding = binding_by_node.get(node.node_id)
        if binding is None:
            continue  # a map — handled by the profile pass below

        capability = binding.capability
        if capability.accepts_input == "documents_required" and previous_producer is None:
            # Nothing upstream produces documents, so this call would receive
            # none and never execute.
            raise raise_compile_error(
                PROCESS_IR_SEMANTIC_CARDINALITY_MISMATCH,
                "semantic_lowering",
                "{0}/operation_ref".format(binding.source_path),
                internal_node_id=binding.node_id,
            )

        if not capability.produces_output and position != len(steps) - 1:
            # A call that returns no documents to the process cannot be followed
            # by anything that consumes them. This is the Send gate: official
            # Boomi documentation states a Send action "does not return any data
            # to the process for further processing", so emitting a step after
            # one would produce a shape that can never run.
            raise raise_compile_error(
                PROCESS_IR_SEMANTIC_CARDINALITY_MISMATCH,
                "semantic_lowering",
                "{0}/operation_ref".format(binding.source_path),
                internal_node_id=binding.node_id,
            )

        # ...and the same rule at the terminal. Only a non-consuming ``stop`` may
        # follow a non-producing call; ``return_documents`` would return an empty
        # stream it was never given.
        if (
            not capability.produces_output
            and position == len(steps) - 1
            and terminal is not None
            and terminal.exit_role == "return_documents"
        ):
            raise raise_compile_error(
                PROCESS_IR_SEMANTIC_CARDINALITY_MISMATCH,
                "semantic_lowering",
                terminal.source_path,
                internal_node_id=terminal.node_id,
            )

        previous_producer = binding if capability.produces_output else None

    # Profile continuity around every map. The model already guarantees each map
    # is bracketed by calls, so both neighbours exist.
    for position, node in enumerate(steps):
        if node.semantic.semantic_kind == "connector_call":
            continue
        map_path = "{0}/map_ref".format(node.source_path)
        map_symbol = _symbol(index, getattr(node.semantic, "map_ref", None))
        if map_symbol is None or _canonical_type(map_symbol) != MAP_COMPONENT_TYPE:
            raise raise_compile_error(
                PROCESS_IR_SEMANTIC_PROFILE_MISMATCH,
                "semantic_lowering",
                map_path,
                internal_node_id=node.node_id,
            )

        before = binding_by_node.get(steps[position - 1].node_id) if position else None
        after = (
            binding_by_node.get(steps[position + 1].node_id)
            if position + 1 < len(steps)
            else None
        )
        if before is None or after is None:
            # Unreachable through the model's bracketing rule; a compiler defect
            # would be the only way here, and rejecting is the safe direction.
            raise raise_compile_error(
                PROCESS_IR_SEMANTIC_PROFILE_MISMATCH,
                "semantic_lowering",
                map_path,
                internal_node_id=node.node_id,
            )

        upstream = _profile_identity(index, before.output_profile_ref)
        map_source = _profile_identity(index, map_symbol.input_profile_ref)
        map_target = _profile_identity(index, map_symbol.output_profile_ref)
        downstream = _profile_identity(index, after.input_profile_ref)

        # A map's source/destination profiles are hard component requirements
        # (unlike connector request/response profiles, which the platform
        # documents as non-validating), so an ABSENT profile on either side is a
        # mismatch too — there is nothing to satisfy the requirement with.
        if map_source is None or map_target is None or upstream is None or downstream is None:
            raise raise_compile_error(
                PROCESS_IR_SEMANTIC_PROFILE_MISMATCH,
                "semantic_lowering",
                map_path,
                internal_node_id=node.node_id,
            )
        if map_source != upstream or map_target != downstream:
            raise raise_compile_error(
                PROCESS_IR_SEMANTIC_PROFILE_MISMATCH,
                "semantic_lowering",
                map_path,
                internal_node_id=node.node_id,
            )


def validate_connector_calls(cfg: SemanticCfgV1, symbols: SymbolTableV1) -> None:
    """Both passes, in order. The single entry point the pipeline calls."""
    bindings = resolve_connector_call_bindings(cfg, symbols)
    validate_connector_call_semantics(cfg, bindings, symbols)


__all__ = [
    "ConnectorCallBindingV1",
    "resolve_connector_call_bindings",
    "validate_connector_call_semantics",
    "validate_connector_calls",
]

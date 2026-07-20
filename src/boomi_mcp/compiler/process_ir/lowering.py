"""Deterministic lowering: ProcessIRV1 -> semantic CFG -> emission plan (issue #137).

Two separate, pure passes. Neither touches XML, escaping, tags, or images —
that boundary belongs to the emitter registry in #138.

Ordering is the whole game here. The legacy builder allocates shape indices
**depth-first** (``_append_path`` walks the linear prefix, then the terminal
block, then each control subtree in authored order), and it appends the control
shape *before* its subtrees — so index-allocation order and XML document order
coincide. This module reproduces that walk exactly, which is what makes the plan
a parity oracle for the unchanged builder.

Snapshotting: every semantic value is copied into a frozen contract model during
lowering. Mutating the caller's ``ProcessIRV1`` afterwards cannot reach a CFG or
plan that was already produced.
"""

from __future__ import annotations

from typing import Any, List, Optional, Sequence, Tuple

from ...errors import (
    PROCESS_IR_CAPABILITY_UNSUPPORTED,
    PROCESS_IR_COMPILE_EMISSION_PLAN_INVALID,
)
from ...models.process_ir import ProcessIRV1
from .contracts import (
    DECISION_FALSE_DRAGPOINT_Y,
    DRAGPOINT_Y,
    LISTENER_CONNECTOR_TYPES,
    SHAPE_Y,
    START_SHAPE_X,
    START_SHAPE_Y,
    BranchInputV1,
    BranchSemanticV1,
    CacheGetSemanticV1,
    CachePutSemanticV1,
    CacheRemoveSemanticV1,
    CfgEdgeV1,
    CfgNodeV1,
    ComponentSymbolV1,
    ConnectorActionInputV1,
    ConnectorSemanticV1,
    DataProcessInputV1,
    DataProcessSemanticV1,
    DecisionInputV1,
    DecisionSemanticV1,
    DocCacheLoadInputV1,
    DocCacheRemoveInputV1,
    DocCacheRetrieveInputV1,
    DocumentCacheRetrieveSemanticV1,
    EmissionLayoutV1,
    EmissionNodeV1,
    EmissionPlanV1,
    EmissionTransitionV1,
    ExceptionInputV1,
    ExceptionSemanticV1,
    FlowControlInputV1,
    FlowControlSemanticV1,
    MapInputV1,
    MapSemanticV1,
    MessageInputV1,
    MessageSemanticV1,
    ProcessCallInputV1,
    ProcessCallSemanticV1,
    ReturnDocumentsInputV1,
    ReturnDocumentsSemanticV1,
    SemanticCfgV1,
    SetPropertiesStepInputV1,
    SetPropertySemanticV1,
    StartNoActionInputV1,
    StopInputV1,
    StopSemanticV1,
    SymbolTableV1,
    cfg_edge_id,
    cfg_node_id,
    dragpoint_name,
    dragpoint_x,
    shape_id,
    shape_x,
)
from .diagnostics import raise_compile_error

# Default profile type the legacy property emitter falls back to
# (``_emit_property_source_value``, builder :4016).
_DEFAULT_PROFILE_TYPE = "profile.json"

_DDP_PROPERTY_PREFIX = "dynamicdocument."
_DPP_PROPERTY_PREFIX = "process."
_DDP_DISPLAY_PREFIX = "Dynamic Document Property - "
_DPP_DISPLAY_PREFIX = "Dynamic Process Property - "

# IR kinds that terminate the path they end.
_EXIT_KINDS = {
    "stop": "stop",
    "return_documents": "return_documents",
    "exception": "exception",
}


def _canonical_connector_metadata(role: str, connector_type: str, action_type: str):
    """Normalise connector metadata exactly as the legacy composed path does.

    The legacy builder does NOT emit a symbol's connector family verbatim: it
    resolves aliases to the canonical subtype (``rest_client`` ->
    ``officialboomi-X3979C-rest-prod``, ``soap_client`` -> ``wssoapclientsdk``)
    and normalises action case, with rules that differ by role
    (``_source_prefix_flow_entries:5467`` vs ``_target_terminal_entries:5500``):

    * source, REST family -> canonical subtype, action upper-cased
    * source, other       -> canonical subtype LOWER-cased, action left raw
    * target, any family  -> canonical subtype, action upper-cased

    Passing a symbol's raw alias straight through would hand #138 an input that
    serialises non-parity connector XML. The legacy helpers are imported lazily
    so they stay the single source of truth (a duplicated alias table would
    drift) without charging every ``import boomi_mcp.compiler`` for the 7.5k-line
    builder module. ``test_compiler_connector_canonicalization_matches_builder``
    pins the agreement.
    """
    from ...categories.components.builders.process_flow_builder import (
        _canonical_connector_type,
        _resolve_rest_connector_type,
    )

    canonical = _canonical_connector_type(connector_type)
    action = str(action_type or "").strip()
    if role == "target":
        return canonical, action.upper()
    if _resolve_rest_connector_type(connector_type) is not None:
        return canonical, action.upper()
    return canonical.lower(), action


def _pointer_escape(token: str) -> str:
    return token.replace("~", "~0").replace("/", "~1")


def _join(base: str, *parts: Any) -> str:
    """Extend an RFC 6901 pointer. Pointers are ABSOLUTE from the document root."""
    out = base
    for part in parts:
        out += "/" + _pointer_escape(str(part))
    return out


# ---------------------------------------------------------------------------
# Pass 1: ProcessIRV1 -> SemanticCfgV1
# ---------------------------------------------------------------------------


class _CfgBuilder:
    """Accumulates nodes/edges during the depth-first walk."""

    def __init__(self) -> None:
        self.nodes: List[CfgNodeV1] = []
        # Raw edge records; ids and ordinals are assigned in ``finalize_edges``
        # so edge order is CANONICAL rather than traversal-dependent (a
        # depth-first walk emits a decision's false-arm edge only after the
        # whole true arm, which would otherwise leak the traversal into output).
        self._pending: List[dict] = []

    def add_node(
        self,
        semantic: Any,
        source_path: str,
        exit_role: Optional[str] = None,
    ) -> str:
        ordinal = len(self.nodes) + 1
        node_id = cfg_node_id(ordinal)
        self.nodes.append(
            CfgNodeV1(
                node_id=node_id,
                ordinal=ordinal,
                source_path=source_path,
                semantic=semantic,
                exit_role=exit_role,
            )
        )
        return node_id

    def add_edge(
        self,
        source_node_id: str,
        target_node_id: str,
        kind: str,
        local_ordinal: int,
        provenance_path: str,
        *,
        leg_ordinal: Optional[int] = None,
        outcome: Optional[str] = None,
    ) -> None:
        self._pending.append(
            {
                "source_node_id": source_node_id,
                "target_node_id": target_node_id,
                "kind": kind,
                "local_ordinal": local_ordinal,
                "provenance_path": provenance_path,
                "leg_ordinal": leg_ordinal,
                "outcome": outcome,
            }
        )

    def finalize_edges(self) -> Tuple[CfgEdgeV1, ...]:
        """Canonical edge order: by (source node ordinal, local ordinal)."""
        ordered = sorted(
            self._pending,
            key=lambda record: (
                int(record["source_node_id"][1:]),
                record["local_ordinal"],
            ),
        )
        return tuple(
            CfgEdgeV1(edge_id=cfg_edge_id(index), ordinal=index, **record)
            for index, record in enumerate(ordered, start=1)
        )

    def exit_role_of(self, node_id: str) -> Optional[str]:
        return self.nodes[int(node_id[1:]) - 1].exit_role


def _semantic_for(node: Any, *, routed: bool = False) -> Any:
    """Snapshot one IR node's OWN facts. Children are flattened into the CFG."""
    kind = node.kind
    label = getattr(node, "label", None)

    if kind in ("source", "target"):
        return ConnectorSemanticV1(
            role="source" if kind == "source" else "target",
            connection_ref=node.connection_ref,
            operation_ref=node.operation_ref,
            label=label,
        )
    if kind == "message":
        return MessageSemanticV1(text=node.text, label=label)
    if kind == "map_ref":
        return MapSemanticV1(map_ref=node.map_ref, label=label)
    if kind == "flow_control":
        return FlowControlSemanticV1(for_each_count=node.for_each_count, label=label)
    if kind == "data_process":
        return DataProcessSemanticV1(
            steps=tuple(_dataprocess_op_semantic(step) for step in node.steps),
            label=label,
        )
    if kind == "cache_put":
        return CachePutSemanticV1(cache_ref=node.cache_ref, label=label)
    if kind == "cache_get":
        return CacheGetSemanticV1(
            cache_ref=node.cache_ref,
            empty_cache_behavior=node.empty_cache_behavior,
            external_writer=node.external_writer,
            label=label,
        )
    if kind == "document_cache_retrieve":
        return DocumentCacheRetrieveSemanticV1(
            cache_ref=node.cache_ref,
            empty_cache_behavior=node.empty_cache_behavior,
            load_all_documents=node.load_all_documents,
            label=label,
        )
    if kind == "cache_remove":
        return CacheRemoveSemanticV1(
            cache_ref=node.cache_ref,
            remove_all_documents=node.remove_all_documents,
            label=label,
        )
    if kind in ("set_ddp", "set_dpp"):
        return SetPropertySemanticV1(
            scope="ddp" if kind == "set_ddp" else "dpp",
            # ``_validate_bare_property_name`` checks the STRIPPED name but the
            # model stores the original, so " DDP_X " is a valid payload. The
            # legacy emitter strips it (``_seq_linear_emit:5443``), so stripping
            # here is required for parity — otherwise the wire id would be
            # "dynamicdocument. DDP_X ".
            name=node.name.strip(),
            persist=bool(getattr(node, "persist", False)),
            source_values=tuple(
                _property_source_semantic(source) for source in node.source_values
            ),
            label=label,
        )
    if kind == "process_call":
        return ProcessCallSemanticV1(
            process_ref=node.process_ref,
            wait=node.wait,
            abort_on_error=node.abort_on_error,
            label=label,
        )
    if kind == "branch":
        return BranchSemanticV1(leg_count=len(node.legs), label=label)
    if kind == "decision":
        return DecisionSemanticV1(
            comparison=node.comparison,
            left=_operand_semantic(node.left),
            right=_operand_semantic(node.right),
            label=label,
        )
    if kind == "exception":
        return ExceptionSemanticV1(
            message_template=node.message_template,
            title=node.title,
            stop_single_document=node.stop_single_document,
            parameter_source=node.parameter_source,
        )
    if kind == "stop":
        return StopSemanticV1()
    if kind == "return_documents":
        return ReturnDocumentsSemanticV1(label=label)
    # Unreachable while ProcessNodeV1 stays a closed union; the pipeline converts
    # this into PROCESS_IR_COMPILE_INTERNAL rather than leaking a KeyError.
    raise ValueError("unhandled IR node kind")


def _dataprocess_op_semantic(step: Any) -> Any:
    from .contracts import (
        _CombineDocumentsOpSemanticV1,
        _CustomScriptingOpSemanticV1,
        _SplitDocumentsOpSemanticV1,
    )

    if step.operation == "custom_scripting":
        return _CustomScriptingOpSemanticV1(
            script=step.script, language=step.language, use_cache=step.use_cache
        )
    if step.operation == "split_documents":
        return _SplitDocumentsOpSemanticV1(
            profile_type=step.profile_type,
            profile_ref=step.profile_ref,
            link_element_key=step.link_element_key,
            link_element_name=step.link_element_name,
        )
    return _CombineDocumentsOpSemanticV1(
        profile_type=step.profile_type,
        profile_ref=step.profile_ref,
        link_element_key=step.link_element_key,
        link_element_name=step.link_element_name,
        combine_into_link_element_key=step.combine_into_link_element_key,
    )


def _property_source_semantic(source: Any) -> Any:
    from .contracts import (
        _CurrentPropertySourceSemanticV1,
        _DdpPropertySourceSemanticV1,
        _DppPropertySourceSemanticV1,
        _ProfilePropertySourceSemanticV1,
        _StaticPropertySourceSemanticV1,
    )

    value_type = source.value_type
    if value_type == "static":
        return _StaticPropertySourceSemanticV1(value=source.value)
    if value_type == "current":
        return _CurrentPropertySourceSemanticV1()
    if value_type == "profile":
        return _ProfilePropertySourceSemanticV1(
            element_id=source.element_id,
            element_name=source.element_name,
            profile_ref=source.profile_ref,
            # ``profile_type`` is stripped on the wire (builder :4040).
            profile_type=(
                source.profile_type.strip()
                if isinstance(source.profile_type, str)
                else source.profile_type
            ),
        )
    # ``property_name`` is stripped on the wire for BOTH ddp and dpp sources
    # (builder :4051 / :4063); ``default_value`` deliberately is NOT.
    if value_type == "ddp":
        return _DdpPropertySourceSemanticV1(
            property_name=source.property_name.strip(),
            default_value=source.default_value,
        )
    return _DppPropertySourceSemanticV1(
        property_name=source.property_name.strip(),
        default_value=source.default_value,
    )


def _operand_semantic(operand: Any) -> Any:
    from .contracts import _StaticOperandSemanticV1, _TrackOperandSemanticV1

    if operand.value_type == "track":
        return _TrackOperandSemanticV1(
            # ``property_id`` is stripped on the wire (builder :4447);
            # ``property_name`` and ``default_value`` deliberately are NOT.
            property_id=operand.property_id.strip(),
            property_name=operand.property_name,
            default_value=operand.default_value,
        )
    return _StaticOperandSemanticV1(static_value=operand.static_value)


def _sequential_edge_kind(builder: _CfgBuilder, target_node_id: str) -> str:
    """``terminal`` when the edge lands on an exit, else plain ``ordering``."""
    return "terminal" if builder.exit_role_of(target_node_id) else "ordering"


def _lower_linear_run(
    builder: _CfgBuilder,
    steps: Sequence[Any],
    base_path: str,
    predecessor: Optional[str],
) -> Optional[str]:
    """Lower a run of linear steps, chaining ordering edges. Returns the last node."""
    current = predecessor
    for index, step in enumerate(steps):
        path = _join(base_path, "steps", index)
        node_id = builder.add_node(_semantic_for(step), path)
        if current is not None:
            builder.add_edge(current, node_id, "ordering", 1, path)
        current = node_id
    return current


def _lower_terminal(
    builder: _CfgBuilder,
    terminal: Any,
    path: str,
    predecessor: Optional[str],
    *,
    routed: bool,
) -> None:
    """Lower a leg/arm terminal, recursing into a nested control node."""
    kind = terminal.kind

    if kind == "branch":
        node_id = builder.add_node(_semantic_for(terminal), path)
        if predecessor is not None:
            builder.add_edge(predecessor, node_id, "ordering", 1, path)
        _lower_branch_children(builder, terminal, node_id, path)
        return

    if kind == "decision":  # pragma: no cover - schema forbids nested decisions
        node_id = builder.add_node(_semantic_for(terminal), path)
        if predecessor is not None:
            builder.add_edge(predecessor, node_id, "ordering", 1, path)
        _lower_decision_children(builder, terminal, node_id, path)
        return

    # A ``target`` in a leg/arm terminal position is a ROUTED target: the IR has
    # no Stop after it, so the emission plan owns the synthetic one.
    if kind == "target" and routed:
        exit_role = "routed_target"
    elif kind == "cache_put":
        exit_role = "cache_stage"
    else:
        exit_role = _EXIT_KINDS.get(kind)

    node_id = builder.add_node(_semantic_for(terminal), path, exit_role)
    if predecessor is not None:
        builder.add_edge(
            predecessor, node_id, _sequential_edge_kind(builder, node_id), 1, path
        )


def _lower_branch_children(
    builder: _CfgBuilder, branch: Any, branch_node_id: str, branch_path: str
) -> None:
    """Legs in AUTHORED order; indices allocated leg-by-leg (builder :5665)."""
    for leg_index, leg in enumerate(branch.legs):
        leg_path = _join(branch_path, "legs", leg_index)
        first_ordinal = len(builder.nodes) + 1
        first_node_id = cfg_node_id(first_ordinal)
        builder.add_edge(
            branch_node_id,
            first_node_id,
            "branch_leg",
            leg_index + 1,
            leg_path,
            leg_ordinal=leg_index + 1,
        )
        last = _lower_linear_run(builder, leg.steps, leg_path, None)
        _lower_terminal(
            builder, leg.terminal, _join(leg_path, "terminal"), last, routed=True
        )


def _lower_decision_children(
    builder: _CfgBuilder, decision: Any, decision_node_id: str, decision_path: str
) -> None:
    """TRUE arm first, then FALSE arm — the legacy allocation order (builder :5619)."""
    for local_ordinal, (arm_name, outcome) in enumerate(
        (("true_arm", "true"), ("false_arm", "false")), start=1
    ):
        arm = getattr(decision, arm_name)
        arm_path = _join(decision_path, arm_name)
        first_node_id = cfg_node_id(len(builder.nodes) + 1)
        builder.add_edge(
            decision_node_id,
            first_node_id,
            "decision_outcome",
            local_ordinal,
            arm_path,
            outcome=outcome,
        )
        last = _lower_linear_run(builder, arm.steps, arm_path, None)
        _lower_terminal(
            builder, arm.terminal, _join(arm_path, "terminal"), last, routed=True
        )


def lower_process_ir_to_cfg(ir: ProcessIRV1) -> SemanticCfgV1:
    """Lower a validated ``ProcessIRV1`` into the internal semantic CFG.

    Pure and deterministic: the same IR always yields byte-identical output, and
    the CFG holds no reference back into the caller's mutable models.
    """
    builder = _CfgBuilder()
    steps = list(ir.body.steps)
    base = "/body"

    previous: Optional[str] = None
    for index, step in enumerate(steps):
        path = _join(base, "steps", index)
        kind = step.kind

        if kind == "branch":
            node_id = builder.add_node(_semantic_for(step), path)
            if previous is not None:
                builder.add_edge(previous, node_id, "ordering", 1, path)
            _lower_branch_children(builder, step, node_id, path)
            previous = None
            continue

        if kind == "decision":
            node_id = builder.add_node(_semantic_for(step), path)
            if previous is not None:
                builder.add_edge(previous, node_id, "ordering", 1, path)
            _lower_decision_children(builder, step, node_id, path)
            previous = None
            continue

        # A root ``target`` is followed by an AUTHORED stop, so it is not itself
        # an exit — the stop is. Only leg/arm targets are routed.
        exit_role = _EXIT_KINDS.get(kind)
        node_id = builder.add_node(_semantic_for(step), path, exit_role)
        if previous is not None:
            builder.add_edge(
                previous, node_id, _sequential_edge_kind(builder, node_id), 1, path
            )
        previous = node_id

    exit_ids = tuple(node.node_id for node in builder.nodes if node.exit_role)
    return SemanticCfgV1(
        entry_node_id=cfg_node_id(1),
        nodes=tuple(builder.nodes),
        edges=builder.finalize_edges(),
        exit_node_ids=exit_ids,
    )


# ---------------------------------------------------------------------------
# Pass 2: SemanticCfgV1 -> EmissionPlanV1
# ---------------------------------------------------------------------------


def _resolve(
    symbols: SymbolTableV1, ref: str, path: str, node_id: str
) -> ComponentSymbolV1:
    symbol = symbols.lookup(ref)
    if symbol is None:
        raise raise_compile_error(
            PROCESS_IR_COMPILE_EMISSION_PLAN_INVALID,
            "reference_resolution",
            path,
            internal_node_id=node_id,
            message="no symbol resolves this authored reference",
        )
    return symbol


def _resolved_property_source(
    source: Any, symbols: SymbolTableV1, path: str, node_id: str
) -> Any:
    from .contracts import (
        _CurrentPropertySourceInputV1,
        _DdpPropertySourceInputV1,
        _DppPropertySourceInputV1,
        _ProfilePropertySourceInputV1,
        _StaticPropertySourceInputV1,
    )

    value_type = source.value_type
    if value_type == "static":
        return _StaticPropertySourceInputV1(value=source.value)
    if value_type == "current":
        return _CurrentPropertySourceInputV1()
    if value_type == "profile":
        symbol = _resolve(symbols, source.profile_ref, path, node_id)
        return _ProfilePropertySourceInputV1(
            element_id=source.element_id,
            element_name=source.element_name,
            profile_id=symbol.component_id,
            profile_type=source.profile_type or _DEFAULT_PROFILE_TYPE,
        )
    if value_type == "ddp":
        return _DdpPropertySourceInputV1(
            property_id=_DDP_PROPERTY_PREFIX + source.property_name,
            property_name=_DDP_DISPLAY_PREFIX + source.property_name,
            default_value=source.default_value or "",
        )
    return _DppPropertySourceInputV1(
        process_property=source.property_name,
        default_value=source.default_value or "",
    )


def _resolved_dataprocess_step(
    step: Any, ordinal: int, symbols: SymbolTableV1, path: str, node_id: str
) -> Any:
    from .contracts import (
        _CombineDocumentsStepInputV1,
        _CustomScriptingStepInputV1,
        _SplitDocumentsStepInputV1,
    )

    if step.operation == "custom_scripting":
        return _CustomScriptingStepInputV1(
            key=ordinal,
            index=ordinal,
            script=step.script,
            language=step.language,
            use_cache=step.use_cache,
        )
    profile = _resolve(symbols, step.profile_ref, path, node_id)
    if step.operation == "split_documents":
        return _SplitDocumentsStepInputV1(
            key=ordinal,
            index=ordinal,
            profile_type=step.profile_type,
            profile_id=profile.component_id,
            link_element_key=step.link_element_key,
            link_element_name=step.link_element_name,
        )
    return _CombineDocumentsStepInputV1(
        key=ordinal,
        index=ordinal,
        profile_type=step.profile_type,
        profile_id=profile.component_id,
        link_element_key=step.link_element_key,
        link_element_name=step.link_element_name,
        combine_into_link_element_key=step.combine_into_link_element_key,
    )


def _emitter_input_for(node: CfgNodeV1, symbols: SymbolTableV1) -> Any:
    """Resolve one CFG node into its fully-bound emitter input."""
    semantic = node.semantic
    path = node.source_path
    node_id = node.node_id
    label = getattr(semantic, "label", None) or ""

    kind = semantic.semantic_kind

    if kind == "connector":
        connection = _resolve(symbols, semantic.connection_ref, path, node_id)
        operation = _resolve(symbols, semantic.operation_ref, path, node_id)
        if not operation.connector_type or not operation.action_type:
            raise raise_compile_error(
                PROCESS_IR_COMPILE_EMISSION_PLAN_INVALID,
                "reference_resolution",
                path,
                internal_node_id=node_id,
                message="operation symbol is missing derived connector metadata",
            )
        connector_type, action_type = _canonical_connector_metadata(
            semantic.role, operation.connector_type, operation.action_type
        )
        # Canonicalization can still yield a blank (e.g. a whitespace-only
        # family passes the non-empty check above but strips to "").
        if not connector_type or not action_type:
            raise raise_compile_error(
                PROCESS_IR_COMPILE_EMISSION_PLAN_INVALID,
                "reference_resolution",
                path,
                internal_node_id=node_id,
                message="connector metadata is blank after canonicalization",
            )
        # The legacy entry for a listener FUSES start + connector into one
        # ``start_listen`` shape; this compiler always emits the
        # start_noaction + connectoraction pair, so a listener source would be
        # silently mis-shaped. Fail closed until #140 adds the entry policy.
        # Matched on the CANONICAL, case-folded family so that ``WSS``,
        # ``  wss  `` and the spelled-out aliases cannot slip past an exact
        # lowercase comparison against the raw symbol value.
        if (
            semantic.role == "source"
            and connector_type.strip().lower() in LISTENER_CONNECTOR_TYPES
        ):
            raise raise_compile_error(
                PROCESS_IR_CAPABILITY_UNSUPPORTED,
                "reference_resolution",
                path,
                internal_node_id=node_id,
                message=(
                    "listener entry is not representable in ProcessIR v1 — the legacy "
                    "path fuses the start and connector shapes"
                ),
            )
        return ConnectorActionInputV1(
            emitter_kind=(
                "connectoraction_source"
                if semantic.role == "source"
                else "connectoraction_target"
            ),
            connector_type=connector_type,
            action_type=action_type,
            connection_id=connection.component_id,
            operation_id=operation.component_id,
            userlabel=label,
        )

    if kind == "message":
        return MessageInputV1(text=semantic.text, userlabel=label)
    if kind == "map":
        return MapInputV1(
            map_id=_resolve(symbols, semantic.map_ref, path, node_id).component_id,
            userlabel=label,
        )
    if kind == "flow_control":
        return FlowControlInputV1(
            for_each_count=semantic.for_each_count, userlabel=label
        )
    if kind == "data_process":
        return DataProcessInputV1(
            steps=tuple(
                _resolved_dataprocess_step(step, index, symbols, path, node_id)
                for index, step in enumerate(semantic.steps, start=1)
            ),
            userlabel=label,
        )
    if kind == "cache_put":
        return DocCacheLoadInputV1(
            document_cache_id=_resolve(
                symbols, semantic.cache_ref, path, node_id
            ).component_id,
            userlabel=label,
        )
    if kind == "cache_get":
        # ``cache_get`` lowers onto the same byte-locked doccacheretrieve emitter,
        # which hard-codes loadAllDoc="true" (builder :5417-5436).
        return DocCacheRetrieveInputV1(
            document_cache_id=_resolve(
                symbols, semantic.cache_ref, path, node_id
            ).component_id,
            empty_cache_behavior=semantic.empty_cache_behavior,
            load_all_documents=True,
            userlabel=label,
        )
    if kind == "document_cache_retrieve":
        return DocCacheRetrieveInputV1(
            document_cache_id=_resolve(
                symbols, semantic.cache_ref, path, node_id
            ).component_id,
            empty_cache_behavior=semantic.empty_cache_behavior,
            load_all_documents=semantic.load_all_documents,
            userlabel=label,
        )
    if kind == "cache_remove":
        return DocCacheRemoveInputV1(
            document_cache_id=_resolve(
                symbols, semantic.cache_ref, path, node_id
            ).component_id,
            remove_all_documents=semantic.remove_all_documents,
            userlabel=label,
        )
    if kind == "set_property":
        is_ddp = semantic.scope == "ddp"
        return SetPropertiesStepInputV1(
            scope=semantic.scope,
            property_id=(
                _DDP_PROPERTY_PREFIX if is_ddp else _DPP_PROPERTY_PREFIX
            )
            + semantic.name,
            display_name=(
                _DDP_DISPLAY_PREFIX if is_ddp else _DPP_DISPLAY_PREFIX
            )
            + semantic.name,
            # DDP persist is ALWAYS false on the wire (builder :4091); only DPP
            # honours the authored flag.
            persist=False if is_ddp else semantic.persist,
            source_values=tuple(
                _resolved_property_source(source, symbols, path, node_id)
                for source in semantic.source_values
            ),
            userlabel=label,
        )
    if kind == "process_call":
        return ProcessCallInputV1(
            process_id=_resolve(
                symbols, semantic.process_ref, path, node_id
            ).component_id,
            wait=semantic.wait,
            abort=semantic.abort_on_error,
            userlabel=label,
        )
    if kind == "branch":
        return BranchInputV1(num_branches=semantic.leg_count, userlabel=label)
    if kind == "decision":
        return DecisionInputV1(
            comparison=semantic.comparison,
            left=semantic.left,
            right=semantic.right,
            userlabel=label,
        )
    if kind == "exception":
        return ExceptionInputV1(
            message_template=semantic.message_template,
            title=semantic.title or "",
            stop_single_document=semantic.stop_single_document,
            parameter_source=semantic.parameter_source,
            binding=_exception_binding(semantic.parameter_source),
        )
    if kind == "stop":
        return StopInputV1()
    if kind == "return_documents":
        return ReturnDocumentsInputV1(label=label)
    raise ValueError("unhandled semantic kind")  # pragma: no cover


def _exception_binding(parameter_source: str):
    """Resolve an Exception's parameter source into its wire binding facts.

    The plan calls for a resolved "Exception binding", not a raw enum: #138
    should only have to serialise it. Mirrors ``_emit_exception_parameters``
    (builder :6164) — ``none`` emits nothing, ``current_document`` a bare
    current parametervalue, ``caught_error`` the fixed Try/Catch message token.
    """
    from .contracts import (
        _CaughtErrorBindingV1,
        _CurrentDocumentBindingV1,
        _NoExceptionBindingV1,
    )

    if parameter_source == "none":
        return _NoExceptionBindingV1()
    if parameter_source == "current_document":
        return _CurrentDocumentBindingV1()
    return _CaughtErrorBindingV1()


def _transition(
    source_ordinal: int,
    local_ordinal: int,
    to_shape: str,
    *,
    y: float = DRAGPOINT_Y,
    identifier: Optional[str] = None,
    text: Optional[str] = None,
    provenance: str = "cfg_edge",
    cfg_edge: Optional[str] = None,
) -> EmissionTransitionV1:
    return EmissionTransitionV1(
        local_ordinal=local_ordinal,
        dragpoint_name=dragpoint_name(shape_id(source_ordinal), local_ordinal),
        to_shape_id=to_shape,
        x=dragpoint_x(source_ordinal),
        y=y,
        identifier=identifier,
        text=text,
        provenance=provenance,
        cfg_edge_id=cfg_edge,
    )


def lower_cfg_to_emission_plan(
    cfg: SemanticCfgV1, symbols: SymbolTableV1
) -> EmissionPlanV1:
    """Lower a semantic CFG into the deterministic emission plan.

    The plan owns everything the IR must not: the synthetic Start, the synthetic
    Stops after routed targets, ``shapeN`` identities, geometry, dragpoints, and
    resolved symbols.
    """
    # Pass A: assign plan ordinals. The synthetic Start takes shape1; each CFG
    # node follows in ordinal order; a routed target is immediately followed by
    # its compiler-owned Stop (builder ``fallthrough=[target, stop]``, :5690).
    ordinal_for_cfg_node = {}
    synthetic_stop_for = {}
    next_ordinal = 2
    for node in cfg.nodes:
        ordinal_for_cfg_node[node.node_id] = next_ordinal
        next_ordinal += 1
        if node.exit_role == "routed_target":
            synthetic_stop_for[node.node_id] = next_ordinal
            next_ordinal += 1

    # Pass B: group CFG edges by source, preserving canonical local order.
    outgoing_edges = {}
    for edge in cfg.edges:
        outgoing_edges.setdefault(edge.source_node_id, []).append(edge)

    nodes: List[EmissionNodeV1] = []
    terminal_shapes: List[str] = []

    entry_target = shape_id(ordinal_for_cfg_node[cfg.entry_node_id])
    nodes.append(
        EmissionNodeV1(
            ordinal=1,
            shape_id=shape_id(1),
            origin="synthetic",
            synthetic_role="start",
            emitter_input=StartNoActionInputV1(),
            layout=EmissionLayoutV1(x=START_SHAPE_X, y=START_SHAPE_Y),
            outgoing=(_transition(1, 1, entry_target, provenance="synthetic"),),
        )
    )

    for node in cfg.nodes:
        ordinal = ordinal_for_cfg_node[node.node_id]
        semantic_kind = node.semantic.semantic_kind
        transitions: List[EmissionTransitionV1] = []

        if node.exit_role == "routed_target":
            transitions.append(
                _transition(
                    ordinal,
                    1,
                    shape_id(synthetic_stop_for[node.node_id]),
                    provenance="synthetic",
                )
            )
        else:
            for local, edge in enumerate(outgoing_edges.get(node.node_id, []), start=1):
                to_shape = shape_id(ordinal_for_cfg_node[edge.target_node_id])
                if semantic_kind == "decision":
                    is_true = edge.outcome == "true"
                    transitions.append(
                        _transition(
                            ordinal,
                            local,
                            to_shape,
                            # Legacy case asymmetry: lowercase identifier,
                            # title-case text (builder :4515).
                            y=DRAGPOINT_Y if is_true else DECISION_FALSE_DRAGPOINT_Y,
                            identifier="true" if is_true else "false",
                            text="True" if is_true else "False",
                            cfg_edge=edge.edge_id,
                        )
                    )
                elif semantic_kind == "branch":
                    marker = str(edge.leg_ordinal)
                    transitions.append(
                        _transition(
                            ordinal,
                            local,
                            to_shape,
                            identifier=marker,
                            text=marker,
                            cfg_edge=edge.edge_id,
                        )
                    )
                else:
                    transitions.append(
                        _transition(ordinal, local, to_shape, cfg_edge=edge.edge_id)
                    )

        nodes.append(
            EmissionNodeV1(
                ordinal=ordinal,
                shape_id=shape_id(ordinal),
                cfg_node_id=node.node_id,
                source_path=node.source_path,
                origin="ir",
                emitter_input=_emitter_input_for(node, symbols),
                layout=EmissionLayoutV1(x=shape_x(ordinal), y=SHAPE_Y),
                outgoing=tuple(transitions),
            )
        )

        if node.exit_role == "routed_target":
            stop_ordinal = synthetic_stop_for[node.node_id]
            nodes.append(
                EmissionNodeV1(
                    ordinal=stop_ordinal,
                    shape_id=shape_id(stop_ordinal),
                    origin="synthetic",
                    synthetic_role="terminal_stop",
                    emitter_input=StopInputV1(),
                    layout=EmissionLayoutV1(x=shape_x(stop_ordinal), y=SHAPE_Y),
                )
            )
            terminal_shapes.append(shape_id(stop_ordinal))
        elif node.exit_role:
            terminal_shapes.append(shape_id(ordinal))

    ordered = tuple(sorted(nodes, key=lambda item: item.ordinal))
    return EmissionPlanV1(
        entry_shape_id=shape_id(1),
        nodes=ordered,
        terminal_shape_ids=tuple(terminal_shapes),
    )


__all__: List[str] = [
    "lower_cfg_to_emission_plan",
    "lower_process_ir_to_cfg",
]

"""Internal compiler contracts: semantic CFG + emission plan (issue #137, M12.2).

These models are the compiler's OWN representations. They are never authored,
never accepted from a caller, and never exported into an LLM-facing JSON Schema
(ADR-001 §6; issue #137 acceptance criterion "internal CFG and emission-plan
schemas are not present in public MCP/LLM JSON Schema").

Two layers, deliberately separate:

``SemanticCfgV1``
    Control-flow meaning only. Nodes keep their IR identity (a JSON pointer into
    the authored payload) and the node's OWN semantic facts. There is NO shape
    id, layout, dragpoint, or XML state at this layer, and children are
    flattened into nodes + typed edges.

``EmissionPlanV1``
    Everything the emitter needs and nothing the caller may author: generated
    ``shapeN`` identities, deterministic ordinals, synthetic-shape intent,
    resolved symbols, geometry, and wiring. Synthetic nodes exist ONLY here.

Determinism contract
--------------------
Every collection is a ``tuple`` (natively frozen by Pydantic and order-
preserving), never a ``Mapping``. Identities are numeric-ascending (``n1..nN``,
``e1..eM``, ``shape1..shapeN``) and are NEVER lexically sorted — ``shape10``
sorts before ``shape2`` lexically, which would silently reorder the plan.
Canonical JSON uses the same recipe as #136 (``sort_keys=True``, compact
separators, ASCII), which sorts object KEYS only and leaves tuple order intact.

Security
--------
``__repr_args__`` suppresses every value outside a small structural allow-list,
mirroring ``_ProcessIRBase``. Symbols carry resolved component ids and connector
metadata only — never configuration, credentials, headers, or document content.
Generated identities are positional, so they cannot encode a secret.
"""

from __future__ import annotations

import json
from typing import Any, List, Literal, Optional, Tuple, Union

from pydantic import BaseModel, ConfigDict, Field, field_validator
from typing_extensions import Annotated

# Geometry, byte-locked to the legacy emitter
# (``process_flow_builder.py:425-444``). These are FLOATS: the builder renders
# them through f-strings, so the wire text is ``x="96.0"``, not ``x="96"``.
# Carrying ints here would silently break byte parity for the #138 emitter.
SHAPE_Y = 96.0
START_SHAPE_X = 96.0
START_SHAPE_Y = 94.0
SHAPE_X_STEP = 160.0
DRAGPOINT_X_OFFSET = 144.0
DRAGPOINT_Y = 104.0
DECISION_FALSE_DRAGPOINT_Y = 464.0

# Branch leg bounds, mirroring ``BranchNodeV1.legs`` (min_length=2, max_length=25).
BRANCH_MIN_LEGS = 2
BRANCH_MAX_LEGS = 25

# Connector families whose legacy entry FUSES the start shape with the connector
# (``_emit_start_listen``) instead of the ``start_noaction`` + ``connectoraction``
# pair this compiler emits. ProcessIRV1 has no listener node kind, so such an
# entry can only arrive through the symbol table — the guard therefore lives in
# reference resolution, not in IR lowering. #140 owns the alternate entry policy.
# Compared against the CANONICAL, case-folded connector family, so every
# spelling that resolves to the listener family is covered rather than just the
# exact lowercase token.
LISTENER_CONNECTOR_TYPES = frozenset(
    {"wss", "web_services", "web_services_server", "wssserver", "listener"}
)

# Only structural discriminators and generated identities render in ``repr``.
# Every other field renders as "..." so authored text can never leak into a log.
_REPR_SAFE_FIELDS = frozenset(
    {
        "version",
        "semantic_kind",
        "emitter_kind",
        "value_type",
        "operation",
        "node_id",
        "edge_id",
        "shape_id",
        "entry_node_id",
        "entry_shape_id",
        "source_node_id",
        "target_node_id",
        "to_shape_id",
        "cfg_node_id",
        "cfg_edge_id",
        "ordinal",
        "local_ordinal",
        "leg_ordinal",
        "kind",
        "outcome",
        "origin",
        "synthetic_role",
        "exit_role",
        "provenance",
        "role",
        "scope",
    }
)


def shape_x(ordinal: int) -> float:
    """X for the 1-based plan ordinal (``_shape_x``, builder :438)."""
    return START_SHAPE_X + (ordinal - 1) * SHAPE_X_STEP


def dragpoint_x(ordinal: int) -> float:
    """Dragpoint X for the 1-based plan ordinal (``_dragpoint_x``, builder :443)."""
    return shape_x(ordinal) + DRAGPOINT_X_OFFSET


class _CompilerModel(BaseModel):
    """Frozen, strict base for every internal compiler contract."""

    model_config = ConfigDict(extra="forbid", frozen=True)

    def __repr_args__(self) -> Any:
        for key, value in super().__repr_args__():
            if key in _REPR_SAFE_FIELDS:
                yield key, value
            else:
                yield key, "..."


# ---------------------------------------------------------------------------
# Symbol table
# ---------------------------------------------------------------------------


class ComponentSymbolV1(_CompilerModel):
    """One authored reference resolved to an emitter-safe component fact.

    Carries ONLY what the emitter needs. Connector metadata is DERIVED here and
    is never authored in IR (ADR-001 §6) — it rides on the *operation* symbol,
    mirroring the #136 codec's ``_resolve_binding``.
    """

    ref: str = Field(..., min_length=1)
    component_id: str = Field(..., min_length=1)
    component_type: str = Field(..., min_length=1)
    connector_type: Optional[str] = None
    action_type: Optional[str] = None

    @field_validator("ref", "component_id", "component_type")
    @classmethod
    def _no_surrounding_whitespace(cls, value: str) -> str:
        if value != value.strip():
            raise ValueError("symbol fields must not carry surrounding whitespace")
        return value


class SymbolTableV1(_CompilerModel):
    """Closed, deterministic set of resolved references.

    Canonicalised by sorting on ``ref`` at construction, so the caller's
    insertion order cannot reach compiler output. Duplicate refs are rejected;
    two refs sharing one ``component_id`` are allowed (intentional reuse).

    A tuple — not a ``Mapping`` — deliberately: the runtime compiler must not
    depend on the test-only ``_process_ir_compat._FrozenMapping``, and tuples are
    natively frozen by Pydantic and canonically serialisable.
    """

    symbols: Tuple[ComponentSymbolV1, ...] = ()

    @field_validator("symbols")
    @classmethod
    def _unique_and_canonical(
        cls, value: Tuple[ComponentSymbolV1, ...]
    ) -> Tuple[ComponentSymbolV1, ...]:
        seen = set()
        for symbol in value:
            if symbol.ref in seen:
                raise ValueError("duplicate symbol reference")
            seen.add(symbol.ref)
        return tuple(sorted(value, key=lambda item: item.ref))

    def lookup(self, ref: str) -> Optional[ComponentSymbolV1]:
        """Resolve one reference, or ``None`` when it is not in the table."""
        for symbol in self.symbols:
            if symbol.ref == ref:
                return symbol
        return None


# ---------------------------------------------------------------------------
# Semantic facts (CFG layer) — the node's OWN facts, children flattened out
# ---------------------------------------------------------------------------


class _StaticPropertySourceSemanticV1(_CompilerModel):
    value_type: Literal["static"] = "static"
    value: str


class _CurrentPropertySourceSemanticV1(_CompilerModel):
    value_type: Literal["current"] = "current"


class _ProfilePropertySourceSemanticV1(_CompilerModel):
    value_type: Literal["profile"] = "profile"
    element_id: str
    element_name: str
    profile_ref: str
    profile_type: Optional[str] = None


class _DdpPropertySourceSemanticV1(_CompilerModel):
    value_type: Literal["ddp"] = "ddp"
    property_name: str
    default_value: Optional[str] = None


class _DppPropertySourceSemanticV1(_CompilerModel):
    value_type: Literal["dpp"] = "dpp"
    property_name: str
    default_value: Optional[str] = None


PropertySourceSemanticV1 = Annotated[
    Union[
        _StaticPropertySourceSemanticV1,
        _CurrentPropertySourceSemanticV1,
        _ProfilePropertySourceSemanticV1,
        _DdpPropertySourceSemanticV1,
        _DppPropertySourceSemanticV1,
    ],
    Field(discriminator="value_type"),
]


class _TrackOperandSemanticV1(_CompilerModel):
    value_type: Literal["track"] = "track"
    property_id: str
    property_name: Optional[str] = None
    default_value: Optional[str] = None


class _StaticOperandSemanticV1(_CompilerModel):
    value_type: Literal["static"] = "static"
    static_value: str


DecisionOperandSemanticV1 = Annotated[
    Union[_TrackOperandSemanticV1, _StaticOperandSemanticV1],
    Field(discriminator="value_type"),
]


class _CustomScriptingOpSemanticV1(_CompilerModel):
    operation: Literal["custom_scripting"] = "custom_scripting"
    script: str
    language: str
    use_cache: bool


class _SplitDocumentsOpSemanticV1(_CompilerModel):
    operation: Literal["split_documents"] = "split_documents"
    profile_type: str
    profile_ref: str
    link_element_key: str
    link_element_name: str


class _CombineDocumentsOpSemanticV1(_CompilerModel):
    operation: Literal["combine_documents"] = "combine_documents"
    profile_type: str
    profile_ref: str
    link_element_key: str
    link_element_name: str
    combine_into_link_element_key: str


DataProcessOpSemanticV1 = Annotated[
    Union[
        _CustomScriptingOpSemanticV1,
        _SplitDocumentsOpSemanticV1,
        _CombineDocumentsOpSemanticV1,
    ],
    Field(discriminator="operation"),
]


class ConnectorSemanticV1(_CompilerModel):
    semantic_kind: Literal["connector"] = "connector"
    role: Literal["source", "target"]
    connection_ref: str
    operation_ref: str
    label: Optional[str] = None


class MessageSemanticV1(_CompilerModel):
    semantic_kind: Literal["message"] = "message"
    text: str
    label: Optional[str] = None


class MapSemanticV1(_CompilerModel):
    semantic_kind: Literal["map"] = "map"
    map_ref: str
    label: Optional[str] = None


class FlowControlSemanticV1(_CompilerModel):
    semantic_kind: Literal["flow_control"] = "flow_control"
    for_each_count: int
    label: Optional[str] = None


class DataProcessSemanticV1(_CompilerModel):
    semantic_kind: Literal["data_process"] = "data_process"
    steps: Tuple[DataProcessOpSemanticV1, ...]
    label: Optional[str] = None


class CachePutSemanticV1(_CompilerModel):
    semantic_kind: Literal["cache_put"] = "cache_put"
    cache_ref: str
    label: Optional[str] = None


class CacheGetSemanticV1(_CompilerModel):
    semantic_kind: Literal["cache_get"] = "cache_get"
    cache_ref: str
    empty_cache_behavior: str
    external_writer: bool
    label: Optional[str] = None


class DocumentCacheRetrieveSemanticV1(_CompilerModel):
    semantic_kind: Literal["document_cache_retrieve"] = "document_cache_retrieve"
    cache_ref: str
    empty_cache_behavior: str
    load_all_documents: bool
    label: Optional[str] = None


class CacheRemoveSemanticV1(_CompilerModel):
    semantic_kind: Literal["cache_remove"] = "cache_remove"
    cache_ref: str
    remove_all_documents: bool
    label: Optional[str] = None


class SetPropertySemanticV1(_CompilerModel):
    semantic_kind: Literal["set_property"] = "set_property"
    scope: Literal["ddp", "dpp"]
    name: str
    persist: bool
    source_values: Tuple[PropertySourceSemanticV1, ...]
    label: Optional[str] = None


class ProcessCallSemanticV1(_CompilerModel):
    semantic_kind: Literal["process_call"] = "process_call"
    process_ref: str
    wait: bool
    abort_on_error: bool
    label: Optional[str] = None


class BranchSemanticV1(_CompilerModel):
    semantic_kind: Literal["branch"] = "branch"
    leg_count: int = Field(..., ge=BRANCH_MIN_LEGS, le=BRANCH_MAX_LEGS)
    label: Optional[str] = None


class DecisionSemanticV1(_CompilerModel):
    semantic_kind: Literal["decision"] = "decision"
    comparison: str
    left: DecisionOperandSemanticV1
    right: DecisionOperandSemanticV1
    label: Optional[str] = None


class ExceptionSemanticV1(_CompilerModel):
    semantic_kind: Literal["exception"] = "exception"
    message_template: str
    title: Optional[str] = None
    stop_single_document: bool = False
    parameter_source: str = "caught_error"


class StopSemanticV1(_CompilerModel):
    semantic_kind: Literal["stop"] = "stop"


class ReturnDocumentsSemanticV1(_CompilerModel):
    semantic_kind: Literal["return_documents"] = "return_documents"
    label: Optional[str] = None


CfgSemanticV1 = Annotated[
    Union[
        ConnectorSemanticV1,
        MessageSemanticV1,
        MapSemanticV1,
        FlowControlSemanticV1,
        DataProcessSemanticV1,
        CachePutSemanticV1,
        CacheGetSemanticV1,
        DocumentCacheRetrieveSemanticV1,
        CacheRemoveSemanticV1,
        SetPropertySemanticV1,
        ProcessCallSemanticV1,
        BranchSemanticV1,
        DecisionSemanticV1,
        ExceptionSemanticV1,
        StopSemanticV1,
        ReturnDocumentsSemanticV1,
    ],
    Field(discriminator="semantic_kind"),
]


# ---------------------------------------------------------------------------
# Semantic CFG
# ---------------------------------------------------------------------------

# What ends a path. ``routed_target`` is a ``target`` that terminates a branch
# leg or a decision true-arm: the IR has no Stop after it, so the emission plan
# owns the synthetic one. ``cache_stage`` is a target-less staging leg, which
# legitimately ends with no terminal shape at all (builder :5685-5688).
CfgExitRoleV1 = Literal[
    "stop",
    "return_documents",
    "exception",
    "routed_target",
    "cache_stage",
]

CfgEdgeKindV1 = Literal[
    "ordering",
    "branch_leg",
    "decision_outcome",
    "terminal",
    # Reserved for #142 scoped Try/Catch. V1 generates none, and the invariant
    # checker rejects any edge that carries it.
    "catch",
]


class CfgNodeV1(_CompilerModel):
    """One semantic node. No shape id, no layout, no XML state."""

    node_id: str = Field(..., pattern=r"^n[1-9][0-9]*$")
    ordinal: int = Field(..., ge=1)
    source_path: str = Field(..., min_length=1)
    semantic: CfgSemanticV1
    exit_role: Optional[CfgExitRoleV1] = None


class CfgEdgeV1(_CompilerModel):
    """One compiler-derived edge, typed by control MEANING, not a caller string."""

    edge_id: str = Field(..., pattern=r"^e[1-9][0-9]*$")
    ordinal: int = Field(..., ge=1)
    source_node_id: str = Field(..., pattern=r"^n[1-9][0-9]*$")
    target_node_id: str = Field(..., pattern=r"^n[1-9][0-9]*$")
    kind: CfgEdgeKindV1
    local_ordinal: int = Field(..., ge=1)
    provenance_path: str = Field(..., min_length=1)
    leg_ordinal: Optional[int] = Field(default=None, ge=1)
    outcome: Optional[Literal["true", "false"]] = None


class SemanticCfgV1(_CompilerModel):
    version: Literal["1"] = "1"
    entry_node_id: str = Field(..., pattern=r"^n[1-9][0-9]*$")
    nodes: Tuple[CfgNodeV1, ...]
    edges: Tuple[CfgEdgeV1, ...]
    exit_node_ids: Tuple[str, ...]


# ---------------------------------------------------------------------------
# Emitter inputs (emission-plan layer) — fully resolved, ready for #138
# ---------------------------------------------------------------------------


class _StaticPropertySourceInputV1(_CompilerModel):
    value_type: Literal["static"] = "static"
    value: str


class _CurrentPropertySourceInputV1(_CompilerModel):
    value_type: Literal["current"] = "current"


class _ProfilePropertySourceInputV1(_CompilerModel):
    value_type: Literal["profile"] = "profile"
    element_id: str
    element_name: str
    profile_id: str
    profile_type: str


class _DdpPropertySourceInputV1(_CompilerModel):
    value_type: Literal["ddp"] = "ddp"
    property_id: str
    property_name: str
    default_value: str


class _DppPropertySourceInputV1(_CompilerModel):
    value_type: Literal["dpp"] = "dpp"
    process_property: str
    default_value: str


PropertySourceInputV1 = Annotated[
    Union[
        _StaticPropertySourceInputV1,
        _CurrentPropertySourceInputV1,
        _ProfilePropertySourceInputV1,
        _DdpPropertySourceInputV1,
        _DppPropertySourceInputV1,
    ],
    Field(discriminator="value_type"),
]


class _CustomScriptingStepInputV1(_CompilerModel):
    operation: Literal["custom_scripting"] = "custom_scripting"
    key: int = Field(..., ge=1)
    index: int = Field(..., ge=1)
    processtype: Literal["12"] = "12"
    name: Literal["Custom Scripting"] = "Custom Scripting"
    script: str
    language: str
    use_cache: bool


class _SplitDocumentsStepInputV1(_CompilerModel):
    operation: Literal["split_documents"] = "split_documents"
    key: int = Field(..., ge=1)
    index: int = Field(..., ge=1)
    processtype: Literal["8"] = "8"
    name: Literal["Split Documents"] = "Split Documents"
    profile_type: str
    profile_id: str
    link_element_key: str
    link_element_name: str


class _CombineDocumentsStepInputV1(_CompilerModel):
    operation: Literal["combine_documents"] = "combine_documents"
    key: int = Field(..., ge=1)
    index: int = Field(..., ge=1)
    processtype: Literal["9"] = "9"
    name: Literal["Combine Documents"] = "Combine Documents"
    profile_type: str
    profile_id: str
    link_element_key: str
    link_element_name: str
    combine_into_link_element_key: str


DataProcessStepInputV1 = Annotated[
    Union[
        _CustomScriptingStepInputV1,
        _SplitDocumentsStepInputV1,
        _CombineDocumentsStepInputV1,
    ],
    Field(discriminator="operation"),
]


class StartNoActionInputV1(_CompilerModel):
    emitter_kind: Literal["start_noaction"] = "start_noaction"


class ConnectorActionInputV1(_CompilerModel):
    emitter_kind: Literal["connectoraction_source", "connectoraction_target"]
    connector_type: str
    action_type: str
    connection_id: str
    operation_id: str
    userlabel: str = ""


class MessageInputV1(_CompilerModel):
    emitter_kind: Literal["message"] = "message"
    text: str
    userlabel: str = ""


class MapInputV1(_CompilerModel):
    emitter_kind: Literal["map"] = "map"
    map_id: str
    userlabel: str = ""


class FlowControlInputV1(_CompilerModel):
    emitter_kind: Literal["flowcontrol"] = "flowcontrol"
    for_each_count: int = Field(..., gt=0)
    userlabel: str = ""


class DataProcessInputV1(_CompilerModel):
    emitter_kind: Literal["dataprocess"] = "dataprocess"
    steps: Tuple[DataProcessStepInputV1, ...]
    userlabel: str = ""


class DocCacheLoadInputV1(_CompilerModel):
    emitter_kind: Literal["doccacheload"] = "doccacheload"
    document_cache_id: str
    userlabel: str = ""


class DocCacheRetrieveInputV1(_CompilerModel):
    emitter_kind: Literal["doccacheretrieve"] = "doccacheretrieve"
    document_cache_id: str
    empty_cache_behavior: str
    load_all_documents: bool
    userlabel: str = ""


class DocCacheRemoveInputV1(_CompilerModel):
    emitter_kind: Literal["doccacheremove"] = "doccacheremove"
    document_cache_id: str
    remove_all_documents: bool
    userlabel: str = ""


class SetPropertiesStepInputV1(_CompilerModel):
    emitter_kind: Literal["setproperties_step"] = "setproperties_step"
    scope: Literal["ddp", "dpp"]
    property_id: str
    display_name: str
    persist: bool
    source_values: Tuple[PropertySourceInputV1, ...]
    userlabel: str = ""


class ProcessCallInputV1(_CompilerModel):
    emitter_kind: Literal["processcall"] = "processcall"
    process_id: str
    wait: bool
    abort: bool
    userlabel: str = ""


class BranchInputV1(_CompilerModel):
    emitter_kind: Literal["branch"] = "branch"
    num_branches: int = Field(..., ge=BRANCH_MIN_LEGS, le=BRANCH_MAX_LEGS)
    userlabel: str = ""


class DecisionInputV1(_CompilerModel):
    emitter_kind: Literal["decision"] = "decision"
    comparison: str
    left: DecisionOperandSemanticV1
    right: DecisionOperandSemanticV1
    userlabel: str = ""


# Wire binding for an Exception's parameter source, resolved by the compiler so
# #138 only has to serialise it (``_emit_exception_parameters``, builder :6164).
# ``caught_error`` binds the fixed Try/Catch message token.
CAUGHT_ERROR_PROPERTY_ID = "meta.base.catcherrorsmessage"
CAUGHT_ERROR_PROPERTY_NAME = "Base - Try/Catch Message"


class _NoExceptionBindingV1(_CompilerModel):
    binding: Literal["none"] = "none"


class _CurrentDocumentBindingV1(_CompilerModel):
    binding: Literal["current_document"] = "current_document"
    key: int = 0
    value_type: Literal["current"] = "current"


class _CaughtErrorBindingV1(_CompilerModel):
    binding: Literal["caught_error"] = "caught_error"
    key: int = 0
    value_type: Literal["track"] = "track"
    property_id: Literal[CAUGHT_ERROR_PROPERTY_ID] = CAUGHT_ERROR_PROPERTY_ID
    property_name: Literal[CAUGHT_ERROR_PROPERTY_NAME] = CAUGHT_ERROR_PROPERTY_NAME
    default_value: str = ""


ExceptionBindingV1 = Annotated[
    Union[
        _NoExceptionBindingV1,
        _CurrentDocumentBindingV1,
        _CaughtErrorBindingV1,
    ],
    Field(discriminator="binding"),
]


class ExceptionInputV1(_CompilerModel):
    emitter_kind: Literal["exception"] = "exception"
    message_template: str
    title: str = ""
    stop_single_document: bool = False
    parameter_source: str = "caught_error"
    binding: ExceptionBindingV1


class StopInputV1(_CompilerModel):
    emitter_kind: Literal["stop"] = "stop"
    # The legacy builder passes ``continue_=True`` at every call site
    # (:1055, :1078, :1091, :5519) and ``_emit_stop`` defaults it to True.
    continue_: bool = True


class ReturnDocumentsInputV1(_CompilerModel):
    emitter_kind: Literal["returndocuments"] = "returndocuments"
    label: str = ""


EmitterInputV1 = Annotated[
    Union[
        StartNoActionInputV1,
        ConnectorActionInputV1,
        MessageInputV1,
        MapInputV1,
        FlowControlInputV1,
        DataProcessInputV1,
        DocCacheLoadInputV1,
        DocCacheRetrieveInputV1,
        DocCacheRemoveInputV1,
        SetPropertiesStepInputV1,
        ProcessCallInputV1,
        BranchInputV1,
        DecisionInputV1,
        ExceptionInputV1,
        StopInputV1,
        ReturnDocumentsInputV1,
    ],
    Field(discriminator="emitter_kind"),
]


# ---------------------------------------------------------------------------
# Emission plan
# ---------------------------------------------------------------------------


class EmissionLayoutV1(_CompilerModel):
    """Shape geometry. Floats — see the module docstring on byte parity."""

    x: float
    y: float


class EmissionTransitionV1(_CompilerModel):
    """One outgoing wire, carrying its exact dragpoint fields.

    ``identifier``/``text`` are set only for Branch and Decision, which label
    their dragpoints. Note the legacy case asymmetry the emitter locks in:
    Decision writes ``identifier="true"`` (lowercase) but ``text="True"``
    (title-case); Branch writes the same 1-based integer in both.
    """

    local_ordinal: int = Field(..., ge=1)
    dragpoint_name: str = Field(..., min_length=1)
    to_shape_id: str = Field(..., pattern=r"^shape[1-9][0-9]*$")
    x: float
    y: float
    identifier: Optional[str] = None
    text: Optional[str] = None
    provenance: Literal["cfg_edge", "synthetic"]
    cfg_edge_id: Optional[str] = Field(default=None, pattern=r"^e[1-9][0-9]*$")


class EmissionNodeV1(_CompilerModel):
    """One planned shape.

    ``origin="ir"`` nodes carry ``cfg_node_id``/``source_path`` back to authored
    IR. ``origin="synthetic"`` nodes are compiler-owned (the Start shape and the
    Stops that follow a routed target) and have neither — a caller can never
    author them (issue #137: "synthetic-node ownership in emission plan, never
    authored IR").
    """

    ordinal: int = Field(..., ge=1)
    shape_id: str = Field(..., pattern=r"^shape[1-9][0-9]*$")
    cfg_node_id: Optional[str] = Field(default=None, pattern=r"^n[1-9][0-9]*$")
    source_path: Optional[str] = None
    origin: Literal["ir", "synthetic"]
    synthetic_role: Optional[Literal["start", "terminal_stop"]] = None
    emitter_input: EmitterInputV1
    layout: EmissionLayoutV1
    outgoing: Tuple[EmissionTransitionV1, ...] = ()


class EmissionPlanV1(_CompilerModel):
    """Deterministic emission plan. Wiring lives ONLY in ``EmissionNodeV1.outgoing``.

    There is deliberately no second plan-edge collection: two sources of truth
    for wiring is exactly how a plan drifts out of agreement with itself.
    """

    version: Literal["1"] = "1"
    entry_shape_id: str = Field(..., pattern=r"^shape[1-9][0-9]*$")
    nodes: Tuple[EmissionNodeV1, ...]
    terminal_shape_ids: Tuple[str, ...]


# ---------------------------------------------------------------------------
# Canonical serialization (same recipe as #136's ``canonical_process_ir_json``)
# ---------------------------------------------------------------------------


def _canonical_json(value: Any) -> str:
    return json.dumps(value, sort_keys=True, separators=(",", ":"), ensure_ascii=True)


def canonical_cfg_json(cfg: SemanticCfgV1) -> str:
    """Canonical bytes for a CFG. ``sort_keys`` orders object keys, not tuples."""
    return _canonical_json(cfg.model_dump(mode="json"))


def canonical_emission_plan_json(plan: EmissionPlanV1) -> str:
    """Canonical bytes for an emission plan."""
    return _canonical_json(plan.model_dump(mode="json"))


def cfg_node_id(ordinal: int) -> str:
    return "n{0}".format(ordinal)


def cfg_edge_id(ordinal: int) -> str:
    return "e{0}".format(ordinal)


def shape_id(ordinal: int) -> str:
    return "shape{0}".format(ordinal)


def dragpoint_name(shape: str, local_ordinal: int) -> str:
    return "{0}.dragpoint{1}".format(shape, local_ordinal)


__all__: List[str] = [
    "BRANCH_MAX_LEGS",
    "BRANCH_MIN_LEGS",
    "DECISION_FALSE_DRAGPOINT_Y",
    "DRAGPOINT_X_OFFSET",
    "DRAGPOINT_Y",
    "LISTENER_CONNECTOR_TYPES",
    "SHAPE_X_STEP",
    "SHAPE_Y",
    "START_SHAPE_X",
    "START_SHAPE_Y",
    "BranchInputV1",
    "BranchSemanticV1",
    "CachePutSemanticV1",
    "CacheGetSemanticV1",
    "CacheRemoveSemanticV1",
    "CfgEdgeKindV1",
    "CfgEdgeV1",
    "CfgExitRoleV1",
    "CfgNodeV1",
    "CfgSemanticV1",
    "ComponentSymbolV1",
    "ConnectorActionInputV1",
    "ConnectorSemanticV1",
    "DataProcessInputV1",
    "DataProcessOpSemanticV1",
    "DataProcessSemanticV1",
    "DataProcessStepInputV1",
    "DecisionInputV1",
    "DecisionOperandSemanticV1",
    "DecisionSemanticV1",
    "DocCacheLoadInputV1",
    "DocCacheRemoveInputV1",
    "DocCacheRetrieveInputV1",
    "DocumentCacheRetrieveSemanticV1",
    "EmissionLayoutV1",
    "EmissionNodeV1",
    "EmissionPlanV1",
    "EmissionTransitionV1",
    "EmitterInputV1",
    "ExceptionInputV1",
    "ExceptionSemanticV1",
    "FlowControlInputV1",
    "FlowControlSemanticV1",
    "MapInputV1",
    "MapSemanticV1",
    "MessageInputV1",
    "MessageSemanticV1",
    "ProcessCallInputV1",
    "ProcessCallSemanticV1",
    "PropertySourceInputV1",
    "PropertySourceSemanticV1",
    "ReturnDocumentsInputV1",
    "ReturnDocumentsSemanticV1",
    "SemanticCfgV1",
    "SetPropertiesStepInputV1",
    "SetPropertySemanticV1",
    "StartNoActionInputV1",
    "StopInputV1",
    "StopSemanticV1",
    "SymbolTableV1",
    "canonical_cfg_json",
    "canonical_emission_plan_json",
    "cfg_edge_id",
    "cfg_node_id",
    "dragpoint_name",
    "dragpoint_x",
    "shape_id",
    "shape_x",
]

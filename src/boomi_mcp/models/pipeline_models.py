"""
PipelineSpec control-flow edge contract (M10.0, issue #104).

This is the upstream contract that the M10 Branch and Decision shape emitters
depend on. It models a semantic stage graph where connectivity is carried by a
**typed edge list** rather than plain ordering strings:

- ``dependencies`` is a list of :class:`PipelineEdgeSpec`. Each edge has an
  ``edge_kind``; the default ``"ordering"`` reproduces plain linear wiring, so a
  linear pipeline expressed with edge_kind omitted is fully back-compatible.
- ``branch``, ``decision``, and ``flow_control`` are all reserved *as PipelineSpec
  stage kinds* — there is no PipelineSpec→XML lowering for any of them yet (the
  same reserved-without-lowering treatment as ``combine``) — but the Branch,
  Decision, and Flow Control **shapes** themselves are now emittable directly
  through :class:`ProcessFlowBuilder` via process-config blocks: the ``branch``
  block (M10.8, issue #112) emits an N-way forward fan-out to N independent target
  legs, the ``decision`` block (M10.9, issue #113) emits a conditional two-path
  (true/false) router with optional false-path notify and loop-back, and the
  ``flow_control`` block (M10.7, issue #111) emits a per-document batching Flow
  Control shape.
- Cycle handling is **classification, not blanket rejection**. A back-edge is
  permitted only when it is explicitly typed ``loop_back``; any other back-edge
  (including an omitted/``ordering`` edge that happens to close a cycle) is
  rejected.

Cycle classification intentionally lives in **this validator**, not in
``process_graph_verifier.py``. The graph verifier operates on emitted shape XML
and has no access to the typed ``edge_kind`` data this contract introduces, so it
cannot tell a legitimate ``loop_back`` from an accidental cycle. A "back-edge" is
defined here graph-theoretically (by reachability over the non-loop subgraph),
not by stage-list order.

As of #69 (M5.1) this contract is attached to the public spec as the optional
``IntegrationSpecV1.pipeline`` field, and :class:`StageSpec` carries semantic
metadata — ``cardinality`` / ``context_effect`` / ``side_effect`` /
``failure_behavior`` — which the validator uses to reject invalid side-effect
ordering and unsupported failure modes.

As of #70 (M5.2) there is exactly ONE lowering path from this contract to Boomi
XML: the ``sync_pipeline`` process builder (``SyncPipelineBuilder`` in
``process_flow_builder.py``) lowers the verified-linear, all-``ordering`` subset
into the proven ``database_to_api_sync`` source/transform/target config. The
source stage is either ``read(db_read)`` (a DB Get) or — as of #72 (M5.4) —
``fetch(rest_fetch)`` (a static REST GET source), or — as of #126 (M5.10) —
``fetch(soap_fetch)`` (a SOAP Client EXECUTE source), followed by an optional
``map`` and a target stage that is either ``send(rest_send)`` (a REST target),
``send(soap_send)`` (a SOAP Client EXECUTE target, #126), or — as of #74 (M5.8),
from a ``fetch`` source — ``write(db_write)`` (a database Send/write target,
built on the #32 component builders):
``read(db_read) | fetch(rest_fetch|soap_fetch) -> [map] -> send(rest_send|soap_send)``
and ``fetch(rest_fetch) -> [map] -> write(db_write)``. A ``fetch`` source and a
``send`` target select the REST-vs-SOAP connector family from the declared
``config.primitive`` (rest_fetch/soap_fetch, rest_send/soap_send). Every other
stage kind (``lookup`` / ``combine`` / ``flow_control`` /
``branch`` / ``decision`` / ``dataprocess`` / ``exception`` /
``doccacheretrieve`` / ``doccacheremove``) still has NO PipelineSpec->XML emitter
and is rejected by that builder with a hint pointing at its owning issue. (Several
of those — ``flow_control`` (M10.7), ``branch`` (M10.8), ``decision`` (M10.9),
``dataprocess`` (M10.2), ``exception`` (M10.4), ``doccacheretrieve`` (M10.5),
``doccacheremove`` (M10.6) — ARE emittable shapes today, but via dedicated
``database_to_api_sync`` process-config blocks on ``ProcessFlowBuilder``, NOT
through PipelineSpec lowering.)
"""

from typing import Any, Dict, List, Literal, Optional

from pydantic import BaseModel, ConfigDict, Field, StrictInt, model_validator

PipelineStageKind = Literal[
    "read",
    # M5.4 (#72) / M5.10 (#126): API source stage. Lowered by SyncPipelineBuilder
    # to a REST Client GET (config.primitive='rest_fetch') or a SOAP Client
    # EXECUTE (config.primitive='soap_fetch') source binding.
    "fetch",
    "lookup",
    "map",
    # M2.5 / M5.10 (#126): API target stage. Lowered to a REST Client target
    # (config.primitive='rest_send') or a SOAP Client EXECUTE target
    # (config.primitive='soap_send') binding.
    "send",
    # M5.8 (issue #74): the database Send/write target stage. Lowered by
    # SyncPipelineBuilder (fetch -> [map] -> write) to a database connector
    # binding through the #32 write-profile / Send-operation builders.
    "write",
    "finalize",
    "combine",
    # M10.7 (issue #111): reserved stage kind for the Flow Control shape
    # (per-document batching). Reserved only (no PipelineSpec lowering yet, like
    # branch/decision/dataprocess); the M10.7 emitter attaches to the
    # process-config flow_control block, not to PipelineSpec.
    "flow_control",
    "dataprocess",
    "branch",
    "decision",
    # M10.4 (issue #108): reserved stage kind for a deliberate Exception (Throw)
    # terminal — fail/halt a path with a user-defined error. Reserved only (no
    # PipelineSpec lowering yet, like branch/decision/dataprocess); the M10.4
    # emitter attaches to the reliability.catch_exception block, not to PipelineSpec.
    "exception",
    # M10.5 (issue #109): reserved stage kind for the Document Cache Retrieve
    # shape (the read half of Document Cache CRUD). Reserved only (no PipelineSpec
    # lowering yet, like branch/decision/dataprocess); the M10.5 emitter attaches
    # to the transform.mode='doccacheretrieve' block, not to PipelineSpec.
    "doccacheretrieve",
    # M10.6 (issue #110): reserved stage kind for the Document Cache Remove shape
    # (the delete half of Document Cache CRUD). Reserved only (no PipelineSpec
    # lowering yet, like branch/decision/dataprocess); the M10.6 emitter attaches
    # to the transform.mode='doccacheremove' block, not to PipelineSpec.
    "doccacheremove",
]

PipelineEdgeKind = Literal[
    "ordering",
    "branch",
    "decision_true",
    "decision_false",
    "loop_back",
]

# Semantic stage metadata vocabularies (M5.1, issue #69). All optional on a
# StageSpec; the validator uses side_effect / context_effect / failure_behavior
# to reject invalid side-effect ordering and unsupported failure modes.
StageCardinality = Literal["1:1", "1:N", "N:1", "N:N"]
StageContextEffect = Literal[
    "pass_through",
    "new_connection",
    "shape_transform",
    "fork",
    "join",
]
StageSideEffect = Literal["none", "read", "write", "read_write"]
StageFailureBehavior = Literal["halt", "skip", "retry", "catch"]


class PipelineEdgeSpec(BaseModel):
    """A typed, directed edge between two stage keys.

    ``to_stage`` is a stage key (it lowers to a dragpoint ``toShape`` shape name,
    never a componentId). ``edge_kind`` defaults to ``"ordering"`` so plain
    linear dependencies stay valid without callers opting in.
    """

    model_config = ConfigDict(extra="forbid")

    from_stage: str = Field(..., description="Source stage key")
    to_stage: str = Field(..., description="Target stage key (lowers to dragpoint toShape)")
    edge_kind: PipelineEdgeKind = Field(
        default="ordering",
        description="Edge semantics; default 'ordering' preserves linear wiring",
    )
    label: Optional[str] = Field(default=None, description="Optional edge label (dragpoint text/identifier)")
    ordinal: Optional[StrictInt] = Field(default=None, ge=0, description="Optional ordering hint among sibling edges")


class StageSpec(BaseModel):
    """A single pipeline stage.

    ``kind`` is drawn from :data:`PipelineStageKind`. ``branch`` and ``decision``
    are reserved as PipelineSpec stage kinds (no PipelineSpec lowering yet) — the
    Branch (M10.8) and Decision (M10.9) shapes ARE emittable today, but through
    dedicated ``ProcessFlowBuilder`` process-config blocks, not from PipelineSpec.
    ``config`` is a type-specific payload and ``component_ref`` references an
    existing component key for reuse stages.
    """

    model_config = ConfigDict(extra="forbid")

    key: str = Field(..., description="Unique stage key")
    kind: PipelineStageKind = Field(..., description="Stage kind (vocabulary includes reserved branch/decision)")
    config: Dict[str, Any] = Field(default_factory=dict, description="Type-specific stage configuration")
    component_ref: Optional[str] = Field(default=None, description="Existing component key this stage reuses")
    cardinality: Optional[StageCardinality] = Field(
        default=None, description="Input:output document cardinality"
    )
    context_effect: Optional[StageContextEffect] = Field(
        default=None, description="How the stage changes flow context"
    )
    side_effect: Optional[StageSideEffect] = Field(
        default=None, description="External side-effect class"
    )
    failure_behavior: Optional[StageFailureBehavior] = Field(
        default=None, description="Failure-handling behavior"
    )

    @model_validator(mode="after")
    def _validate_config_xor_component_ref(self) -> "StageSpec":
        # A stage is either primitive-backed (config) OR a reuse of an existing
        # component (component_ref) — never both. Neither is allowed too (e.g. a
        # reserved branch/decision stage that carries only metadata).
        if self.config and self.component_ref is not None:
            raise ValueError(
                f"Stage '{self.key}': config and component_ref are mutually "
                "exclusive; set config={} or component_ref=None"
            )
        return self


class PipelineSpec(BaseModel):
    """A semantic stage graph with typed control-flow edges.

    The validator does cycle CLASSIFICATION: untyped back-edges are rejected and
    only ``loop_back`` edges that close an existing forward path are allowed.
    """

    model_config = ConfigDict(extra="forbid")

    stages: List[StageSpec] = Field(default_factory=list)
    dependencies: List[PipelineEdgeSpec] = Field(
        default_factory=list,
        description="Typed control-flow edges (default edge_kind='ordering')",
    )

    @model_validator(mode="after")
    def _classify_edges(self) -> "PipelineSpec":
        # 1. Duplicate stage keys.
        keys: List[str] = [s.key for s in self.stages]
        seen: set = set()
        for key in keys:
            if key in seen:
                raise ValueError(f"Duplicate stage key: {key!r}")
            seen.add(key)

        # Stage lookup (keys are now known unique) for the metadata-aware rules.
        stage_by_key: Dict[str, StageSpec] = {s.key: s for s in self.stages}

        # 2. Edge endpoints must be declared stage keys; no self-edges.
        for edge in self.dependencies:
            for endpoint in (edge.from_stage, edge.to_stage):
                if endpoint not in seen:
                    raise ValueError(f"Edge references unknown stage: {endpoint!r}")
            if edge.from_stage == edge.to_stage:
                raise ValueError(f"Self-edge not allowed on stage: {edge.from_stage!r}")

        # 3. Non-loop adjacency (every edge except loop_back).
        adjacency: Dict[str, List[str]] = {key: [] for key in seen}
        for edge in self.dependencies:
            if edge.edge_kind != "loop_back":
                adjacency[edge.from_stage].append(edge.to_stage)

        # 4. Reject any cycle in the non-loop subgraph (untyped back-edge):
        #    a real loop_back was excluded in step 3, so a cycle here can only
        #    come from an ordering/branch/decision_* edge.
        if _has_cycle(adjacency):
            raise ValueError(
                "Cycle detected in non-loop edges (untyped back-edge); "
                "use edge_kind='loop_back' to declare an intentional loop"
            )

        # 5. Each loop_back must close an existing forward path: to_stage must be
        #    able to reach from_stage over the (now acyclic) non-loop subgraph.
        for edge in self.dependencies:
            if edge.edge_kind == "loop_back":
                if not _reachable(adjacency, edge.to_stage, edge.from_stage):
                    raise ValueError(
                        "loop_back edge does not close a forward path: "
                        f"{edge.to_stage!r} -> {edge.from_stage!r}"
                    )

        # 6. Invalid side-effect ordering: a plain ordering edge must not run a
        #    writing stage immediately before a reading stage (a read sequenced
        #    after a write inverts the natural read->...->write data flow). Only
        #    ordering edges are checked; branch/decision/loop_back control edges
        #    carry no straight-line data-flow promise.
        for edge in self.dependencies:
            if edge.edge_kind != "ordering":
                continue
            src = stage_by_key[edge.from_stage]
            tgt = stage_by_key[edge.to_stage]
            if src.side_effect in ("write", "read_write") and tgt.side_effect == "read":
                raise ValueError(
                    "Invalid side-effect ordering: writing stage "
                    f"{edge.from_stage!r} (side_effect={src.side_effect!r}) "
                    f"ordered before reading stage {edge.to_stage!r}"
                )

        # 7/8. Unsupported failure modes for the declared stage semantics.
        for stage in self.stages:
            if stage.failure_behavior == "catch" and stage.context_effect != "new_connection":
                raise ValueError(
                    f"Stage '{stage.key}': failure_behavior='catch' requires "
                    "context_effect='new_connection'"
                )
            if stage.failure_behavior == "retry" and stage.side_effect not in (
                "read",
                "write",
                "read_write",
            ):
                raise ValueError(
                    f"Stage '{stage.key}': failure_behavior='retry' requires "
                    "side_effect read/write/read_write"
                )

        return self


def _has_cycle(adjacency: Dict[str, List[str]]) -> bool:
    """Detect a directed cycle via WHITE/GRAY/BLACK DFS coloring."""
    WHITE, GRAY, BLACK = 0, 1, 2
    color: Dict[str, int] = {node: WHITE for node in adjacency}

    def visit(node: str) -> bool:
        color[node] = GRAY
        for nxt in adjacency.get(node, []):
            if color[nxt] == GRAY:
                return True
            if color[nxt] == WHITE and visit(nxt):
                return True
        color[node] = BLACK
        return False

    for node in adjacency:
        if color[node] == WHITE and visit(node):
            return True
    return False


def _reachable(adjacency: Dict[str, List[str]], start: str, target: str) -> bool:
    """Return True if ``target`` is reachable from ``start`` over ``adjacency``."""
    visited: set = set()
    stack: List[str] = [start]
    while stack:
        node = stack.pop()
        if node == target:
            return True
        if node in visited:
            continue
        visited.add(node)
        stack.extend(adjacency.get(node, []))
    return False

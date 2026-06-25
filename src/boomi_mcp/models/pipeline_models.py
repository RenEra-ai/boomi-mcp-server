"""
PipelineSpec control-flow edge contract (M10.0, issue #104).

This is the upstream contract that the M10 Branch and Decision shape emitters
depend on. It models a semantic stage graph where connectivity is carried by a
**typed edge list** rather than plain ordering strings:

- ``dependencies`` is a list of :class:`PipelineEdgeSpec`. Each edge has an
  ``edge_kind``; the default ``"ordering"`` reproduces plain linear wiring, so a
  linear pipeline expressed with edge_kind omitted is fully back-compatible.
- ``branch`` and ``decision`` are **reserved** stage kinds: named in the
  vocabulary so the contract designs them in, but with **no emitter** yet (the
  same reserved-without-emitter treatment as ``combine`` / ``flow_control``).
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
ordering and unsupported failure modes. No Boomi XML is emitted from
``PipelineSpec`` yet; lowering the stage graph to a process-flow builder is M5.2+.
"""

from typing import Any, Dict, List, Literal, Optional

from pydantic import BaseModel, Field, model_validator

PipelineStageKind = Literal[
    "read",
    "fetch",
    "lookup",
    "map",
    "send",
    "write",
    "finalize",
    "combine",
    "flow_control",
    "dataprocess",
    "branch",
    "decision",
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

    from_stage: str = Field(..., description="Source stage key")
    to_stage: str = Field(..., description="Target stage key (lowers to dragpoint toShape)")
    edge_kind: PipelineEdgeKind = Field(
        default="ordering",
        description="Edge semantics; default 'ordering' preserves linear wiring",
    )
    label: Optional[str] = Field(default=None, description="Optional edge label (dragpoint text/identifier)")
    ordinal: Optional[int] = Field(default=None, description="Optional ordering hint among sibling edges")


class StageSpec(BaseModel):
    """A single pipeline stage.

    ``kind`` is drawn from :data:`PipelineStageKind`. ``branch`` and ``decision``
    are reserved (no emitter yet). ``config`` is a type-specific payload and
    ``component_ref`` references an existing component key for reuse stages.
    """

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

"""
Unit tests for the M10.0 PipelineSpec typed-edge contract (issue #104).

Pure model/validator tests — no Boomi MCP calls, no XML fixtures. These exercise
the typed-edge model, the reserved branch/decision stage kinds, and the
PipelineSpec validator's cycle-CLASSIFICATION behaviour (reject untyped
back-edges; allow only loop_back edges that close a forward path).
"""

import pytest
from pydantic import ValidationError

from boomi_mcp.models import (
    PipelineSpec,
    StageSpec,
    PipelineEdgeSpec,
    PipelineStageKind,
    PipelineEdgeKind,
)


def test_edge_kind_defaults_to_ordering():
    edge = PipelineEdgeSpec(from_stage="a", to_stage="b")
    assert edge.edge_kind == "ordering"


def test_branch_and_decision_stage_kinds_are_reserved_and_accepted():
    spec = PipelineSpec(
        stages=[
            StageSpec(key="r", kind="read"),
            StageSpec(key="b", kind="branch"),
            StageSpec(key="d", kind="decision"),
        ],
        dependencies=[
            PipelineEdgeSpec(from_stage="r", to_stage="b"),
            PipelineEdgeSpec(from_stage="b", to_stage="d", edge_kind="branch"),
        ],
    )
    kinds = {s.key: s.kind for s in spec.stages}
    assert kinds["b"] == "branch"
    assert kinds["d"] == "decision"


def test_invalid_stage_kind_is_rejected():
    # Out-of-Literal value is rejected natively by pydantic before the
    # model_validator runs, so we only assert the exception type.
    with pytest.raises(ValidationError):
        StageSpec(key="x", kind="bogus")


def test_duplicate_stage_keys_rejected():
    with pytest.raises(ValidationError) as excinfo:
        PipelineSpec(
            stages=[
                StageSpec(key="a", kind="read"),
                StageSpec(key="a", kind="map"),
            ]
        )
    assert "Duplicate stage key" in str(excinfo.value)


def test_unknown_edge_endpoint_rejected():
    with pytest.raises(ValidationError) as excinfo:
        PipelineSpec(
            stages=[StageSpec(key="a", kind="read")],
            dependencies=[PipelineEdgeSpec(from_stage="a", to_stage="ghost")],
        )
    assert "unknown stage" in str(excinfo.value)


def test_self_edge_rejected():
    with pytest.raises(ValidationError) as excinfo:
        PipelineSpec(
            stages=[StageSpec(key="a", kind="read")],
            dependencies=[PipelineEdgeSpec(from_stage="a", to_stage="a")],
        )
    assert "Self-edge" in str(excinfo.value)


def test_untyped_cycle_rejected_when_edge_kind_defaults_to_ordering():
    # a -> b -> a, edge_kind omitted (defaults to ordering) => untyped back-edge.
    with pytest.raises(ValidationError) as excinfo:
        PipelineSpec(
            stages=[
                StageSpec(key="a", kind="read"),
                StageSpec(key="b", kind="map"),
            ],
            dependencies=[
                PipelineEdgeSpec(from_stage="a", to_stage="b"),
                PipelineEdgeSpec(from_stage="b", to_stage="a"),
            ],
        )
    assert "untyped back-edge" in str(excinfo.value)


def test_branch_or_decision_cycle_rejected_without_loop_back():
    # Cycle formed by typed-but-not-loop_back edges is still a back-edge in the
    # non-loop graph and must be rejected.
    with pytest.raises(ValidationError) as excinfo:
        PipelineSpec(
            stages=[
                StageSpec(key="a", kind="decision"),
                StageSpec(key="b", kind="branch"),
            ],
            dependencies=[
                PipelineEdgeSpec(from_stage="a", to_stage="b", edge_kind="decision_true"),
                PipelineEdgeSpec(from_stage="b", to_stage="a", edge_kind="branch"),
            ],
        )
    assert "untyped back-edge" in str(excinfo.value)


def test_loop_back_closing_forward_path_is_allowed():
    # Forward path a -> b -> c (ordering); loop_back c -> a closes it because
    # to_stage (a) reaches from_stage (c) through the forward graph.
    spec = PipelineSpec(
        stages=[
            StageSpec(key="a", kind="read"),
            StageSpec(key="b", kind="decision"),
            StageSpec(key="c", kind="map"),
        ],
        dependencies=[
            PipelineEdgeSpec(from_stage="a", to_stage="b"),
            PipelineEdgeSpec(from_stage="b", to_stage="c"),
            PipelineEdgeSpec(from_stage="c", to_stage="a", edge_kind="loop_back"),
        ],
    )
    loop_edges = [e for e in spec.dependencies if e.edge_kind == "loop_back"]
    assert len(loop_edges) == 1
    assert loop_edges[0].from_stage == "c" and loop_edges[0].to_stage == "a"


def test_loop_back_that_does_not_close_forward_path_is_rejected():
    # loop_back a -> c, but c has no forward path back to a => misclassified.
    with pytest.raises(ValidationError) as excinfo:
        PipelineSpec(
            stages=[
                StageSpec(key="a", kind="read"),
                StageSpec(key="b", kind="map"),
                StageSpec(key="c", kind="write"),
            ],
            dependencies=[
                PipelineEdgeSpec(from_stage="a", to_stage="b"),
                PipelineEdgeSpec(from_stage="a", to_stage="c", edge_kind="loop_back"),
            ],
        )
    assert "loop_back edge does not close a forward path" in str(excinfo.value)


def test_models_init_exports_pipeline_contract():
    import boomi_mcp.models as models

    for name in (
        "PipelineSpec",
        "StageSpec",
        "PipelineEdgeSpec",
        "PipelineStageKind",
        "PipelineEdgeKind",
    ):
        assert name in models.__all__
        assert getattr(models, name) is not None

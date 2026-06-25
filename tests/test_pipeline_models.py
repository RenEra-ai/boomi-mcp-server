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
    IntegrationSpecV1,
    PipelineSpec,
    StageSpec,
    PipelineEdgeSpec,
    PipelineStageKind,
    PipelineEdgeKind,
    StageCardinality,
    StageContextEffect,
    StageSideEffect,
    StageFailureBehavior,
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


def test_dataprocess_stage_kind_is_accepted():
    # Issue #106 M10.2: the process-level Data Process stage kind is part of the
    # vocabulary (a concrete kind, distinct from the reserved 'combine').
    spec = PipelineSpec(
        stages=[
            StageSpec(key="r", kind="read"),
            StageSpec(key="dp", kind="dataprocess"),
            StageSpec(key="w", kind="write"),
        ],
        dependencies=[
            PipelineEdgeSpec(from_stage="r", to_stage="dp"),
            PipelineEdgeSpec(from_stage="dp", to_stage="w"),
        ],
    )
    kinds = {s.key: s.kind for s in spec.stages}
    assert kinds["dp"] == "dataprocess"


def test_exception_stage_kind_is_reserved_and_accepted():
    # Issue #108 M10.4: the deliberate Exception (Throw) stage kind is reserved in
    # the vocabulary (no PipelineSpec lowering yet, like branch/decision).
    spec = PipelineSpec(
        stages=[
            StageSpec(key="r", kind="read"),
            StageSpec(key="x", kind="exception"),
        ],
        dependencies=[
            PipelineEdgeSpec(from_stage="r", to_stage="x"),
        ],
    )
    kinds = {s.key: s.kind for s in spec.stages}
    assert kinds["x"] == "exception"


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


# --- M5.1 (#69): stage metadata + semantic validation ---------------------


def test_stage_metadata_fields_are_optional():
    stage = StageSpec(key="a", kind="read")
    assert stage.cardinality is None
    assert stage.context_effect is None
    assert stage.side_effect is None
    assert stage.failure_behavior is None


def test_stage_metadata_cardinality_valid_values():
    for value in ("1:1", "1:N", "N:1", "N:N"):
        stage = StageSpec(key="a", kind="read", cardinality=value)
        assert stage.cardinality == value


def test_stage_metadata_cardinality_invalid_rejected():
    with pytest.raises(ValidationError):
        StageSpec(key="a", kind="read", cardinality="7:7")


def test_stage_config_and_component_ref_xor_rejected():
    with pytest.raises(ValidationError) as excinfo:
        StageSpec(key="a", kind="lookup", config={"x": 1}, component_ref="comp-1")
    assert "mutually exclusive" in str(excinfo.value)


def test_stage_config_empty_with_component_ref_allowed():
    stage = StageSpec(key="a", kind="lookup", component_ref="comp-1")
    assert stage.component_ref == "comp-1"
    assert stage.config == {}


def test_stage_config_only_with_no_component_ref_allowed():
    # The other valid leg of the XOR: primitive-backed config, no component_ref.
    stage = StageSpec(key="a", kind="map", config={"x": 1})
    assert stage.config == {"x": 1}
    assert stage.component_ref is None


def test_pipeline_write_before_read_ordering_rejected():
    with pytest.raises(ValidationError) as excinfo:
        PipelineSpec(
            stages=[
                StageSpec(key="w", kind="write", side_effect="write"),
                StageSpec(key="r", kind="read", side_effect="read"),
            ],
            dependencies=[PipelineEdgeSpec(from_stage="w", to_stage="r")],
        )
    message = str(excinfo.value)
    assert "side-effect ordering" in message
    assert "'w'" in message and "'r'" in message


def test_pipeline_read_before_write_ordering_allowed():
    # The natural direction (read sequenced before write) must stay valid.
    spec = PipelineSpec(
        stages=[
            StageSpec(key="r", kind="read", side_effect="read"),
            StageSpec(key="w", kind="write", side_effect="write"),
        ],
        dependencies=[PipelineEdgeSpec(from_stage="r", to_stage="w")],
    )
    assert len(spec.stages) == 2


def test_pipeline_failure_catch_on_non_connector_rejected():
    with pytest.raises(ValidationError) as excinfo:
        PipelineSpec(
            stages=[
                StageSpec(
                    key="m",
                    kind="map",
                    failure_behavior="catch",
                    context_effect="shape_transform",
                ),
            ],
        )
    message = str(excinfo.value)
    assert "catch" in message and "new_connection" in message


def test_pipeline_failure_catch_on_connector_allowed():
    spec = PipelineSpec(
        stages=[
            StageSpec(
                key="s",
                kind="send",
                failure_behavior="catch",
                context_effect="new_connection",
                side_effect="write",
            ),
        ],
    )
    assert spec.stages[0].failure_behavior == "catch"


def test_pipeline_failure_retry_on_pure_transform_rejected():
    with pytest.raises(ValidationError) as excinfo:
        PipelineSpec(
            stages=[
                StageSpec(
                    key="m",
                    kind="map",
                    failure_behavior="retry",
                    side_effect="none",
                ),
            ],
        )
    assert "retry" in str(excinfo.value)


def test_integration_spec_pipeline_field_optional():
    spec = IntegrationSpecV1(name="x")
    assert spec.pipeline is None


def test_integration_spec_pipeline_wired():
    pipeline = PipelineSpec(stages=[StageSpec(key="a", kind="read")])
    spec = IntegrationSpecV1(name="x", pipeline=pipeline)
    assert spec.pipeline is not None
    assert spec.pipeline.stages[0].key == "a"


def test_integration_spec_pipeline_coerced_from_dict():
    spec = IntegrationSpecV1(
        name="x",
        pipeline={"stages": [{"key": "a", "kind": "read"}]},
    )
    assert isinstance(spec.pipeline, PipelineSpec)
    assert spec.pipeline.stages[0].kind == "read"

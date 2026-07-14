"""Issue #135 (M12.0) — compatibility-freeze characterization tests.

Pins the CURRENT measured behavior of the authoring boundaries that the M12
ProcessIR consolidation (ADR-001, docs/architecture/) will migrate:

- ``IntegrationSpecV1`` / ``IntegrationComponentSpec`` envelope leniency
  (pydantic default ``extra="ignore"``; nested ``config`` preserved verbatim),
- ``PipelineSpec`` / ``StageSpec`` / ``PipelineEdgeSpec`` strictness
  (``extra="forbid"``; ``StageSpec.config`` stays open),
- ``_normalize_to_spec`` routing for the three public input shapes (only
  ``config.integration_spec.pipeline`` survives; top-level and
  source_description pipelines are dropped by the allowlist rebuild),
- the two UNWIRED pipeline surfaces: an authored ``spec.pipeline`` is inert
  while the nested ``main_process.config.pipeline`` is what
  ``SyncPipelineBuilder.lower_config`` actually lowers (the #139 baseline pin),
- ``sync_pipeline`` top-level key gate (unknown + gated keys, exact
  code/field pairs),
- ``flow_sequence`` per-step key/kind strictness,
- ``wrapper_subprocess`` root/process-call leniency plus the plaintext-secret
  rejection boundary.

These are freeze tests: if one fails after an intentional M12 change, the
owning issue must update BOTH this pin and the compatibility inventory
(docs/architecture/M12_COMPATIBILITY_INVENTORY.md) — never silently.

Fixture: tests/fixtures/compatibility/issue_135/authoring_boundaries.json
(synthetic sentinel data only; no secrets, no live-account values).
"""

from __future__ import annotations

import copy
import json
import sys
from pathlib import Path
from unittest.mock import MagicMock, patch

import pytest
from pydantic import ValidationError

_project_root = str(Path(__file__).resolve().parent.parent)
if _project_root not in sys.path:
    sys.path.insert(0, _project_root)

from src.boomi_mcp.models.integration_models import (
    IntegrationComponentSpec,
    IntegrationSpecV1,
)
from src.boomi_mcp.models.pipeline_models import (
    PipelineEdgeSpec,
    PipelineSpec,
    StageSpec,
)
from src.boomi_mcp.categories.integration_builder import (
    _build_plan,
    _normalize_to_spec,
)
from src.boomi_mcp.categories.components.builders import (
    BuilderValidationError,
    SyncPipelineBuilder,
    WrapperSubprocessBuilder,
)
from src.boomi_mcp.categories.components.builders.process_flow_builder import (
    _validate_flow_sequence_step,
)

_FIXTURE = json.loads(
    (
        Path(__file__).parent
        / "fixtures"
        / "compatibility"
        / "issue_135"
        / "authoring_boundaries.json"
    ).read_text()
)


def _case(name):
    return copy.deepcopy(_FIXTURE[name])


# ---------------------------------------------------------------------------
# 1-3. IntegrationSpecV1 / IntegrationComponentSpec envelope (extra="ignore")
# ---------------------------------------------------------------------------

def test_integration_spec_defaults_serialization():
    """Pin the exact default serialization of a minimal IntegrationSpecV1."""
    assert IntegrationSpecV1(name="Sentinel").model_dump() == {
        "version": "1.0",
        "name": "Sentinel",
        "mode": "lift_shift",
        "components": [],
        "goals": [],
        "endpoints": [],
        "flows": [],
        "naming": {},
        "folders": {},
        "runtime": {},
        "validation_rules": {},
        "profile_indexes_by_component_id": None,
        "pipeline": None,
    }


def test_spec_envelope_ignores_unknown_fields():
    case = _case("spec_extra_ignore")
    spec = IntegrationSpecV1(**case["input"])
    unknown = case["unknown_key"]
    assert not hasattr(spec, unknown)
    dump = spec.model_dump()
    assert unknown not in dump
    assert dump == case["expected_dump"]


def test_component_envelope_ignores_unknown_fields_but_preserves_config():
    case = _case("component_extra_ignore_config_preserved")
    comp = IntegrationComponentSpec(**case["input"])
    unknown = case["unknown_key"]
    assert not hasattr(comp, unknown)
    assert unknown not in comp.model_dump()
    # The free-form config dict passes through verbatim (never schema-validated).
    assert comp.config == case["input"]["config"]


# ---------------------------------------------------------------------------
# 4-5. PipelineSpec graph envelope (extra="forbid") vs open StageSpec.config
# ---------------------------------------------------------------------------

def test_pipeline_envelope_forbids_extras():
    case = _case("pipeline_extra_forbidden")
    with pytest.raises(ValidationError):
        PipelineSpec(**case["pipeline_spec"])
    with pytest.raises(ValidationError):
        StageSpec(**case["stage_spec"])
    with pytest.raises(ValidationError):
        PipelineEdgeSpec(**case["edge_spec"])


def test_stage_config_remains_open():
    case = _case("stage_config_open")
    stage = StageSpec(**case["input"])
    assert stage.config == case["input"]["config"]
    assert stage.model_dump()["config"] == case["input"]["config"]


# ---------------------------------------------------------------------------
# 6. Top-level (typed) vs nested (free-form) pipeline serialization
# ---------------------------------------------------------------------------

def test_top_level_pipeline_dump_expands_defaults_nested_stays_compact():
    case = _case("compact_nested_vs_expanded_top")
    compact = case["compact_pipeline"]
    spec = IntegrationSpecV1(
        name="Sentinel Compact",
        pipeline=compact,
        components=[
            {
                "key": "main_process",
                "type": "process",
                "config": {"process_kind": "sync_pipeline", "pipeline": compact},
            }
        ],
    )
    dumped_stage = spec.model_dump()["pipeline"]["stages"][0]
    # The typed spec.pipeline dump gains the four None-default semantic
    # metadata keys per stage...
    for key in case["expected_stage_metadata_keys"]:
        assert key in dumped_stage
        assert dumped_stage[key] is None
    # ...while the same dict nested in component config stays byte-compact.
    assert spec.components[0].config["pipeline"] == compact
    for key in case["expected_stage_metadata_keys"]:
        assert key not in spec.components[0].config["pipeline"]["stages"][0]


# ---------------------------------------------------------------------------
# 7-10. _normalize_to_spec routing (three public input shapes)
# ---------------------------------------------------------------------------

def test_normalize_keeps_nested_integration_spec_pipeline():
    case = _case("normalize_nested")
    spec = _normalize_to_spec(case["config"])
    assert spec.pipeline is not None
    assert [s.key for s in spec.pipeline.stages] == case["expected_stage_keys"]


def test_normalize_drops_top_level_pipeline():
    case = _case("normalize_top_level_dropped")
    spec = _normalize_to_spec(case["config"])
    # The flat top-level shape rebuilds the payload from an allowlist that
    # omits 'pipeline' — a top-level pipeline is silently dropped today.
    assert spec.pipeline is None


def test_normalize_drops_source_description_pipeline():
    case = _case("normalize_source_description_dropped")
    spec = _normalize_to_spec(case["config"])
    assert spec.pipeline is None
    # Allowlisted keys ARE carried from source_description.
    assert [c.key for c in spec.components] == case["expected_component_keys"]
    assert spec.goals == case["expected_goals"]


def test_normalize_string_source_description_becomes_goal():
    case = _case("normalize_string_source_description")
    spec = _normalize_to_spec(case["config"])
    assert spec.goals == case["expected_goals"]


# ---------------------------------------------------------------------------
# 11. Contradictory top-level vs nested pipelines (the #139 baseline pin)
# ---------------------------------------------------------------------------

def test_contradictory_pipelines_coexist_and_nested_wins_lowering():
    """An authored spec.pipeline and a disagreeing nested config.pipeline are
    BOTH accepted today, and only the nested one drives lowering (spec.pipeline
    is inert). #139 must replace this silent precedence with derived equality
    or LEGACY_ADAPTER_AUTHORITY_CONFLICT."""
    case = _case("contradictory_pipelines")
    spec = _normalize_to_spec(case["config"])
    # The inert authored view survives normalization unreconciled...
    assert spec.pipeline is not None
    assert [s.kind for s in spec.pipeline.stages] == case["expected_spec_pipeline_kinds"]
    # ...while the executable channel is the nested process-config pipeline.
    lowered = SyncPipelineBuilder.lower_config(spec.components[0].config)
    assert lowered["process_kind"] == "database_to_api_sync"
    assert (
        lowered["source"]["connection_id"]
        == case["expected_lowered_source_connection_id"]
    )
    assert (
        lowered["target"]["connection_id"]
        == case["expected_lowered_target_connection_id"]
    )


@patch("src.boomi_mcp.categories.integration_builder.paginate_metadata")
def test_contradictory_pipelines_silent_precedence_through_build_plan(mock_pag):
    """Plan-level freeze of the same silent precedence, through `_build_plan`
    (the public planning path that #139 will change). Three pins:

    1. A spec carrying BOTH a spec-level pipeline and a disagreeing nested
       config.pipeline plans clean — no authority-conflict rejection exists.
    2. The planner consults the NESTED pipeline: corrupting it fails the plan
       with a SYNC_PIPELINE_* validation error on the main process step.
    3. The planner never consults spec.pipeline: mutating it changes nothing
       in the planned steps.
    """
    mock_pag.return_value = []
    case = _case("contradictory_pipelines")

    # Pin 1 — the contradiction plans clean; the inert view is echoed back.
    plan = _build_plan(MagicMock(), copy.deepcopy(case["config"]))
    assert plan["_success"] is True
    main_step = next(s for s in plan["steps"] if s["key"] == "main_process")
    assert main_step["planned_action"] == "create"
    assert "validation_error" not in main_step
    echoed = plan["integration_spec"]["pipeline"]
    assert [s["kind"] for s in echoed["stages"]] == case["expected_spec_pipeline_kinds"]

    # Pin 2 — the nested pipeline IS the consulted channel: a reserved stage
    # kind there fails the plan on the main process step.
    broken = copy.deepcopy(case["config"])
    broken["integration_spec"]["components"][0]["config"]["pipeline"]["stages"][0][
        "kind"
    ] = "lookup"
    plan_broken = _build_plan(MagicMock(), broken)
    broken_step = next(s for s in plan_broken["steps"] if s["key"] == "main_process")
    assert broken_step["planned_action"] == "error_process_validation"
    assert broken_step["validation_error"]["error_code"].startswith("SYNC_PIPELINE_")

    # Pin 3 — spec.pipeline is never consulted: rewriting it (schema-valid but
    # semantically different again) leaves every planned step identical.
    mutated = copy.deepcopy(case["config"])
    mutated["integration_spec"]["pipeline"]["stages"] = [
        {"key": "solo_stage", "kind": "write", "config": {"primitive": "db_write"}}
    ]
    mutated["integration_spec"]["pipeline"]["dependencies"] = []
    plan_mutated = _build_plan(MagicMock(), mutated)
    assert plan_mutated["_success"] is True
    assert plan_mutated["steps"] == plan["steps"]
    assert plan_mutated["execution_order"] == plan["execution_order"]
    assert plan_mutated.get("warnings") == plan.get("warnings")


# ---------------------------------------------------------------------------
# 12-13. sync_pipeline top-level key gate (exact code/field pairs)
# ---------------------------------------------------------------------------

def test_sync_pipeline_rejects_unknown_top_level_key():
    case = _case("sync_pipeline_unknown_key")
    err = SyncPipelineBuilder.validate_config(case["config"])
    assert isinstance(err, BuilderValidationError)
    assert err.error_code == case["expected_error_code"]
    assert err.field == case["expected_field"]


@pytest.mark.parametrize(
    "key",
    sorted(_FIXTURE["sync_pipeline_gated_keys"]["cases"]),
)
def test_sync_pipeline_gated_blocks_exact_codes(key):
    fixture = _case("sync_pipeline_gated_keys")
    sub_case = fixture["cases"][key]
    config = dict(fixture["base_config"])
    config[key] = sub_case["value"]
    err = SyncPipelineBuilder.validate_config(config)
    assert isinstance(err, BuilderValidationError)
    assert err.error_code == sub_case["expected_error_code"]
    assert err.field == key


# ---------------------------------------------------------------------------
# 14-15. flow_sequence per-step strictness (exact code/field pairs)
# ---------------------------------------------------------------------------

def test_flow_sequence_step_rejects_unknown_keys():
    case = _case("flow_sequence_unknown_step_key")
    err = _validate_flow_sequence_step(case["step"], case["field"])
    assert isinstance(err, BuilderValidationError)
    assert err.error_code == case["expected_error_code"]
    assert err.field == case["expected_field"]


def test_flow_sequence_step_rejects_unknown_kind():
    case = _case("flow_sequence_unknown_kind")
    err = _validate_flow_sequence_step(case["step"], case["field"])
    assert isinstance(err, BuilderValidationError)
    assert err.error_code == case["expected_error_code"]
    assert err.field == case["expected_field"]


# ---------------------------------------------------------------------------
# 16-17. wrapper_subprocess leniency vs the secret-scan boundary
# ---------------------------------------------------------------------------

def test_wrapper_accepts_and_ignores_unknown_root_and_call_keys():
    case = _case("wrapper_unknown_extras")
    with_extras = case["config_with_extras"]
    # Accepted: no root or per-call key allowlist exists today.
    assert WrapperSubprocessBuilder.validate_config(with_extras, depends_on=[]) is None
    # IGNORED, not merely accepted: the emitted XML is string-identical with
    # and without the unknown extras.
    xml_base = WrapperSubprocessBuilder.build(
        case["base_config"], name="Sentinel Wrapper", folder_name="Sentinel/Fixtures"
    )
    xml_extras = WrapperSubprocessBuilder.build(
        with_extras, name="Sentinel Wrapper", folder_name="Sentinel/Fixtures"
    )
    assert xml_base == xml_extras


def test_wrapper_rejects_secret_looking_extras():
    """Boundary pin for the ADR: leniency stops at secret-shaped keys.
    Intentionally overlaps tests/test_wrapper_subprocess_builder.py's
    test_rejects_plaintext_secret."""
    case = _case("wrapper_secret_extra")
    err = WrapperSubprocessBuilder.validate_config(case["config"], depends_on=[])
    assert isinstance(err, BuilderValidationError)
    assert err.error_code == case["expected_error_code"]
    assert err.field == case["expected_field"]

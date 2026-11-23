"""Issue #135 (M12.0) — compatibility-freeze characterization tests.

Pins the CURRENT measured behavior of the authoring boundaries that the M12
ProcessIR consolidation (ADR-001, specs/) will migrate:

- ``PipelineSpec`` / ``StageSpec`` / ``PipelineEdgeSpec`` strictness
  (``extra="forbid"``; ``StageSpec.config`` stays open),
- ``_normalize_to_spec`` routing for the three public input shapes (only
  ``config.integration_spec.pipeline`` survives; top-level and
  source_description pipelines are dropped by the allowlist rebuild),
- the two UNWIRED pipeline surfaces: an authored ``spec.pipeline`` is inert
  while the nested ``main_process.config.pipeline`` is what
  ``SyncPipelineBuilder.lower_config`` actually lowers (the #139 baseline pin),
- the ``spec.pipeline`` PROCESS-CARDINALITY combinations ADR-001 §5 is built on:
  zero-authored, lone-create-collapsing-to-collision-reuse (in both its agreeing
  and disagreeing authored-view variants), and multi-authored (with no collision,
  one of two colliding, and both colliding to reuse) -- each carrying a
  top-level pipeline, each accepted today,
- the ``spec.pipeline`` secret scan (CLOSED by #139 M12.4; ADR-001 §11), in BOTH
  its zero-component and its component-bearing form — the former known gap is now
  rejected with ``PLAINTEXT_SECRET_REJECTED`` before any plan echo,
- ``sync_pipeline`` top-level key gate (unknown + gated keys, exact
  code/field pairs),
- ``flow_sequence`` per-step key/kind strictness,
- ``wrapper_subprocess`` root/process-call leniency plus the plaintext-secret
  rejection boundary.

These are freeze tests: if one fails after an intentional M12 change, the
owning issue must update BOTH this pin and the compatibility inventory
(specs/M12_COMPATIBILITY_INVENTORY.md) — never silently.

The ``IntegrationSpecV1`` / ``IntegrationComponentSpec`` envelope pins that this
suite used to carry now live in tests/test_integration_models.py: they cover the
surviving surface, so #151 (M12.14) re-homed them out of this legacy-oracle suite
before #160 retires it.

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

from src.boomi_mcp.models.integration_models import IntegrationSpecV1
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
    ProcessFlowBuilder,
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
# #159 S3 (cutover decision 1): raw process authoring is refused at admission.
#
# A flat process row carrying a BODY (any selector or other body field) is now
# refused by `authoring/process_admission.py` before normalization, the secret
# scans, collision resolution or any write. The freeze pins whose subject was a
# flat process body accepted by `_build_plan` are re-pinned below on the surviving
# surface: the body payload is refused (value-free, account-independent), and the
# inert top-level pipeline behaviour is measured on the ADMITTED twin — the same
# spec whose process rows only BIND an existing process by explicit id.
# (specs/M12_COMPATIBILITY_INVENTORY.md is the integrator's to update.)
# ---------------------------------------------------------------------------

from src.boomi_mcp.authoring.process_admission import unit_required_envelope  # noqa: E402
from src.boomi_mcp.errors import PROCESS_COMPONENT_AUTHORING_UNIT_REQUIRED  # noqa: E402

_BIND_IDS = (
    "77777777-7777-7777-7777-777777777771",
    "77777777-7777-7777-7777-777777777772",
)


def _assert_unit_refusal(plan, field=None):
    assert plan["_success"] is False, plan
    assert plan["error_code"] == PROCESS_COMPONENT_AUTHORING_UNIT_REQUIRED, plan
    assert set(plan) == set(unit_required_envelope("x")), sorted(plan)
    assert plan["boomi_mutation"] is False
    assert "steps" not in plan and "integration_spec" not in plan
    assert "LEGACY_ADAPTER_AUTHORITY_CONFLICT" not in json.dumps(plan)
    if field is not None:
        assert plan["field"] == field, plan["field"]


def _bind_processes(config):
    """The admitted twin: every flat process row keeps its key and name but only
    BINDS an existing process by explicit id (no body, no depends_on)."""
    twin = copy.deepcopy(config)
    ids = iter(_BIND_IDS)
    for row in twin["integration_spec"]["components"]:
        if row.get("type") == "process":
            row["config"] = {"reference_only": True, "component_id": next(ids)}
            row["action"] = "create"
            row.pop("depends_on", None)
    return twin


# ---------------------------------------------------------------------------
# 1-2. PipelineSpec graph envelope (extra="forbid") vs open StageSpec.config
#
# The three ``IntegrationSpecV1`` / ``IntegrationComponentSpec`` envelope pins that
# used to open this file moved to tests/test_integration_models.py in #151 (M12.14):
# they characterize the SURVIVING surface, so retiring this legacy-oracle suite with
# the #160 deletion must not take them along.
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
    # The typed spec.pipeline dump expands EVERY default — per-stage
    # component_ref + the four None-default semantic metadata keys, and
    # per-dependency edge_kind="ordering"/label/ordinal. Compared as a
    # complete document so any serialization drift breaks the freeze.
    assert spec.model_dump()["pipeline"] == case["expected_expanded_pipeline_dump"]
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


def test_zero_process_pipeline_accepted_and_preserved_through_build_plan():
    """Zero-process baseline pin (ADR-001 §5): a spec with `components: []` and a
    surviving nested `spec.pipeline` is ACCEPTED by the public `_build_plan` today,
    plans to ZERO steps, and preserves the authored pipeline inert (as the NORMALIZED
    PipelineSpec dump — defaults expanded, semantics preserved) in the echoed
    integration_spec. #139 must preserve this frozen inert value (never reinterpret as
    a derived view, never reject) — not silently discard it."""
    case = _case("zero_process_pipeline")
    # Normalization keeps the nested pipeline and yields zero process components.
    spec = _normalize_to_spec(case["config"])
    assert spec.pipeline is not None
    assert spec.components == []
    assert [s.kind for s in spec.pipeline.stages] == case["expected_stage_kinds"]
    # The public plan path accepts it and produces no executable steps.
    plan = _build_plan(MagicMock(), copy.deepcopy(case["config"]))
    assert plan["_success"] is True
    assert plan["steps"] == []
    # The pipeline survives inert in the echoed spec as the NORMALIZED PipelineSpec
    # dump (`_build_plan` returns `spec.model_dump()`, which expands stage-metadata
    # and edge defaults — see test_integration_spec_defaults_serialization). It is
    # semantics-preserving, NOT the raw authored JSON byte-for-byte.
    echoed = plan["integration_spec"]["pipeline"]
    assert echoed == spec.pipeline.model_dump()
    assert [s["kind"] for s in echoed["stages"]] == case["expected_stage_kinds"]


def test_zero_process_pipeline_secret_config_now_rejected():
    """#139 M12.4 CLOSES the ADR-001 §11 gap this test used to CHARACTERIZE.

    A zero-process spec (`components: []`) whose top-level `spec.pipeline` stage
    `config` carries a SECRET-SHAPED key used to be ACCEPTED with the secret value
    ECHOED BACK UNCHANGED — the per-component `scan_forbidden_secret_fields`
    scanners never traversed the top-level `spec.pipeline` (`StageSpec.config` is
    an open `Dict[str, Any]`). #139 adds a top-level pipeline secret scan in
    `_build_plan` (right after normalization, before any synthesis / echo /
    mutation), so the value is now safely rejected and never echoed. This was the
    sibling of the previous known-gap pair; both are flipped to the fixed
    behavior exactly as their frozen note directed.

    The sentinel value is a PLACEHOLDER token (§11: fixtures use sentinels only),
    never a real secret.
    """
    case = _case("zero_process_pipeline_with_secret")
    secret_val = case["secret_sentinel"]

    plan = _build_plan(MagicMock(), copy.deepcopy(case["config"]))
    # Rejected before any plan echo — no executable steps, no integration_spec echo.
    assert plan["_success"] is False
    assert plan["error_code"] == "PLAINTEXT_SECRET_REJECTED"
    # The value-free path is reported; the secret value never appears anywhere.
    assert "pipeline.stages" in plan["error"] and "config." in plan["error"]
    assert secret_val not in json.dumps(plan)


@patch("src.boomi_mcp.categories.integration_builder.paginate_metadata")
def test_component_bearing_pipeline_secret_config_now_rejected(mock_pag):
    """Component-BEARING half of the same gap, now CLOSED by #139 M12.4.

    Previously the inventory's stronger claim was that no scanner traverses the
    top-level `spec.pipeline` **whatever components exist**, so a clean-planning
    component alongside a `spec.pipeline` secret echoed that secret unredacted.
    #139's top-level pipeline scan runs BEFORE any component step is built, so the
    presence of a valid component no longer lets the pipeline secret through: the
    plan is rejected early with PLAINTEXT_SECRET_REJECTED and the value is never
    echoed. (The per-component scanners still reject a secret in a component's own
    config — covered by the per-builder secret tests; that path is unchanged.)

    The sentinel is a PLACEHOLDER token (§11), never a real secret.
    """
    mock_pag.return_value = []
    case = _case("component_bearing_pipeline_with_secret")
    secret_val = case["secret_sentinel"]

    # A valid component no longer shields the spec.pipeline secret — rejected early.
    plan = _build_plan(MagicMock(), copy.deepcopy(case["config"]))
    assert plan["_success"] is False
    assert plan["error_code"] == "PLAINTEXT_SECRET_REJECTED"
    assert "pipeline.stages" in plan["error"] and "config." in plan["error"]
    assert secret_val not in json.dumps(plan)

    # The control config (secret in BOTH the component AND the pipeline) is also
    # rejected — the pipeline scan preempts, and either way no secret is echoed.
    control = _build_plan(MagicMock(), copy.deepcopy(case["control_config"]))
    assert control["_success"] is False
    assert control["error_code"] == "PLAINTEXT_SECRET_REJECTED"
    assert secret_val not in json.dumps(control)


def test_malformed_pipeline_stage_secret_does_not_leak_through_validation_error():
    """#139A review r1 (P1): the pipeline secret scan runs on the RAW input BEFORE
    ``_normalize_to_spec`` — so a stage that fails StageSpec model validation (e.g.
    it carries both ``component_ref`` and a non-empty ``config``) cannot leak the
    plaintext through the pydantic ValidationError's echoed ``input_value``. The
    plan is rejected with PLAINTEXT_SECRET_REJECTED and the sentinel never surfaces.

    Sentinel is a PLACEHOLDER token (§11), never a real secret.
    """
    sentinel = "SENTINEL-LEAK-not-a-real-secret"
    cfg = {
        "integration_spec": {
            "name": "x",
            "components": [],
            "pipeline": {
                "stages": [
                    {
                        "key": "s0",
                        "stage_type": "transform",
                        # Both set -> StageSpec XOR validator would raise (and echo
                        # the config) during normalization if the scan ran too late.
                        "component_ref": "$ref:foo",
                        "config": {"password": sentinel},
                    }
                ]
            },
        }
    }
    plan = _build_plan(MagicMock(), copy.deepcopy(cfg))
    assert plan["_success"] is False
    assert plan["error_code"] == "PLAINTEXT_SECRET_REJECTED"
    assert sentinel not in json.dumps(plan, default=str)


def test_multi_process_pipeline_secret_rejected_before_mutation():
    """#139A review r-arch (finding 7): the top-level pipeline secret scan runs for
    ZERO-, single-, and MULTI-process specs alike (it precedes cardinality/authority
    logic). A multi-process spec carrying a pipeline secret is rejected the same way.
    Sentinel is a PLACEHOLDER token (§11), never a real secret.

    #159 S3: a multi-process spec now reaches that scan only when its process rows
    are admitted (explicit-id binds); the body form is refused at admission first,
    value-free, so the sentinel is never echoed on either path."""
    sentinel = "SENTINEL-multi-not-a-real-secret"
    cfg = {
        "integration_spec": {
            "name": "x",
            "components": [
                {"key": "p1", "type": "process", "config": {"process_kind": "wrapper_subprocess", "process_calls": [{"process_id": "11111111-1111-1111-1111-111111111111"}]}},
                {"key": "p2", "type": "process", "config": {"process_kind": "wrapper_subprocess", "process_calls": [{"process_id": "22222222-2222-2222-2222-222222222222"}]}},
            ],
            "pipeline": {"stages": [{"key": "s0", "stage_type": "transform", "config": {"access_token": sentinel}}]},
        }
    }
    plan = _build_plan(MagicMock(), _bind_processes(cfg))
    assert plan["_success"] is False
    assert plan["error_code"] == "PLAINTEXT_SECRET_REJECTED"
    assert sentinel not in json.dumps(plan, default=str)

    refused = _build_plan(MagicMock(), copy.deepcopy(cfg))
    _assert_unit_refusal(refused, field="integration_spec.components.0.config.process_kind")
    assert sentinel not in json.dumps(refused, default=str)


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
    """Plan-level freeze of the same silent precedence, through `_build_plan`.

    #159 S3 re-pin. The three original pins, on the surviving surface:

    1. The contradiction no longer plans: its flat process body is refused at
       admission (value-free, no steps, no echo) — and no authority conflict is
       computed either.
    2. The nested pipeline is no longer a consulted channel: corrupting it (a
       reserved stage kind) changes nothing — the refusal is byte-identical, no
       SYNC_PIPELINE_* code is computed, because admission never lowers a body.
    3. spec.pipeline is still never consulted: on the ADMITTED twin (the process
       row only binds an existing process by explicit id) rewriting the view
       leaves every planned step, the order and the warnings identical, and the
       authored view is echoed inert.
    """
    mock_pag.return_value = []
    case = _case("contradictory_pipelines")

    # Pin 1
    refused = _build_plan(MagicMock(), copy.deepcopy(case["config"]))
    _assert_unit_refusal(
        refused, field="integration_spec.components.0.config.process_kind"
    )

    # Pin 2
    broken = copy.deepcopy(case["config"])
    broken["integration_spec"]["components"][0]["config"]["pipeline"]["stages"][0][
        "kind"
    ] = "lookup"
    plan_broken = _build_plan(MagicMock(), broken)
    assert plan_broken == refused
    assert "SYNC_PIPELINE_" not in json.dumps(plan_broken)

    # Pin 3
    admitted = _bind_processes(case["config"])
    plan = _build_plan(MagicMock(), copy.deepcopy(admitted))
    assert plan["_success"] is True
    main_step = next(s for s in plan["steps"] if s["key"] == "main_process")
    assert main_step["planned_action"] == "reuse"
    echoed = plan["integration_spec"]["pipeline"]
    assert [s["kind"] for s in echoed["stages"]] == case["expected_spec_pipeline_kinds"]
    mutated = copy.deepcopy(admitted)
    mutated["integration_spec"]["pipeline"]["stages"] = [
        {"key": "solo_stage", "kind": "write", "config": {"primitive": "db_write"}}
    ]
    mutated["integration_spec"]["pipeline"]["dependencies"] = []
    plan_mutated = _build_plan(MagicMock(), mutated)
    assert plan_mutated["_success"] is True
    assert plan_mutated["steps"] == plan["steps"]
    assert plan_mutated["execution_order"] == plan["execution_order"]
    assert plan_mutated.get("warnings") == plan.get("warnings")
    # Every row binds or updates by explicit id: the account is never searched.
    assert mock_pag.call_count == 0


@patch("src.boomi_mcp.categories.integration_builder.paginate_metadata")
def test_collision_reuse_with_top_level_pipeline_plans_clean_and_echoes(mock_pag):
    """Cardinality freeze (ADR-001 §5): acceptance does not move when the live
    account changes, and the authored `spec.pipeline` is echoed inert.

    #159 S3 re-pin. A collision can no longer turn a flat process BODY into a
    reuse: the body payload is refused identically whether or not a live
    same-name process exists (admission runs before collision resolution). The
    reuse this pin measures is now the explicit-id bind, and its acceptance and
    inert echo are identical with and without the same-name live process.
    """
    case = _case("collision_reuse_pipeline")
    base = _case("contradictory_pipelines")
    stub = copy.deepcopy(case["collision_stub_metadata"])

    outcomes = []
    for existing in ([], [stub]):
        mock_pag.return_value = list(existing)
        refused = _build_plan(MagicMock(), copy.deepcopy(base["config"]))
        _assert_unit_refusal(refused)
        plan = _build_plan(MagicMock(), _bind_processes(base["config"]))
        main_step = next(s for s in plan["steps"] if s["key"] == "main_process")
        assert main_step["planned_action"] == case["expected_planned_action"]
        assert [s for s in plan["steps"] if str(s["planned_action"]).startswith("error")] == []
        echoed = plan["integration_spec"]["pipeline"]
        assert [s["kind"] for s in echoed["stages"]] == case["expected_spec_pipeline_kinds"]
        assert "LEGACY_ADAPTER_AUTHORITY_CONFLICT" not in json.dumps(plan)
        outcomes.append((refused, plan))
    assert outcomes[0] == outcomes[1]


@patch("src.boomi_mcp.categories.integration_builder.paginate_metadata")
def test_collision_reuse_with_agreeing_top_level_pipeline_plans_clean_and_echoes(mock_pag):
    """Agreeing variant of the collision-reuse pin (ADR-001 §5): the authored
    top-level view is a verbatim copy of the nested `config.pipeline`.

    #159 S3 re-pin, as above: the body payload is refused whatever the account
    holds; the admitted twin (explicit-id bind) plans the reuse and echoes the
    FULL normalized PipelineSpec dump of the authored view, with and without the
    live same-name process.
    """
    case = _case("collision_reuse_agreeing_pipeline")
    stub = _case("collision_reuse_pipeline")["collision_stub_metadata"]
    config = _case("contradictory_pipelines")["config"]
    authored_pipeline = copy.deepcopy(
        config["integration_spec"]["components"][0]["config"]["pipeline"]
    )
    config["integration_spec"]["pipeline"] = copy.deepcopy(authored_pipeline)

    for existing in ([], [stub]):
        mock_pag.return_value = list(existing)
        _assert_unit_refusal(_build_plan(MagicMock(), copy.deepcopy(config)))
        plan = _build_plan(MagicMock(), _bind_processes(config))
        main_step = next(s for s in plan["steps"] if s["key"] == "main_process")
        assert main_step["planned_action"] == case["expected_planned_action"]
        assert [s for s in plan["steps"] if str(s["planned_action"]).startswith("error")] == []
        echoed = plan["integration_spec"]["pipeline"]
        assert echoed == PipelineSpec(**authored_pipeline).model_dump()
        assert [s["kind"] for s in echoed["stages"]] == case["expected_spec_pipeline_kinds"]
        assert "LEGACY_ADAPTER_AUTHORITY_CONFLICT" not in json.dumps(plan)


def _two_process_config(multi):
    config = _case("contradictory_pipelines")["config"]
    second = copy.deepcopy(config["integration_spec"]["components"][0])
    second["key"] = multi["second_process_key"]
    second["name"] = multi["second_process_name"]
    config["integration_spec"]["components"].insert(1, second)
    return config


@patch("src.boomi_mcp.categories.integration_builder.paginate_metadata")
def test_multi_authored_spec_with_top_level_pipeline_accepted_today(mock_pag):
    """Cardinality freeze (ADR-001 §5, multi-authored bullet): TWO process
    components plus a singular top-level `spec.pipeline`.

    #159 S3 re-pin. Two flat process BODIES are refused at admission, at the
    FIRST body row. The multi-process shape with a singular view is still
    ACCEPTED when its process rows only bind (explicit ids): both plan, no error
    step, no authority-conflict code, and the view is echoed inert.
    """
    mock_pag.return_value = []
    case = _case("multi_authored_pipeline")
    config = _two_process_config(case)

    _assert_unit_refusal(
        _build_plan(MagicMock(), copy.deepcopy(config)),
        field="integration_spec.components.0.config.process_kind",
    )

    plan = _build_plan(MagicMock(), _bind_processes(config))
    proc_steps = [
        next(s for s in plan["steps"] if s["key"] == key)
        for key in ("main_process", case["second_process_key"])
    ]
    assert [s["planned_action"] for s in proc_steps] == ["reuse", "reuse"]
    assert [s.get("existing_component_id") for s in proc_steps] == list(_BIND_IDS)
    assert [s for s in plan["steps"] if str(s["planned_action"]).startswith("error")] == []
    echoed = plan["integration_spec"]["pipeline"]
    assert [s["kind"] for s in echoed["stages"]] == case["expected_spec_pipeline_kinds"]
    assert "LEGACY_ADAPTER_AUTHORITY_CONFLICT" not in json.dumps(plan)


@patch("src.boomi_mcp.categories.integration_builder.paginate_metadata")
def test_multi_authored_collision_reuse_keeps_acceptance_and_echo(mock_pag):
    """Collision variants of the multi-authored pin (ADR-001 §5 determinism
    note): partial and full live same-name collisions must not move the
    accept/reject outcome.

    #159 S3 re-pin. The two-body payload is refused byte-identically with no,
    partial and full collisions; the admitted twin is accepted identically under
    all three, with the full normalized view echoed.
    """
    case = _case("multi_authored_collision_pipeline")
    multi = _case("multi_authored_pipeline")
    main_stub = _case("collision_reuse_pipeline")["collision_stub_metadata"]
    config = _two_process_config(multi)
    authored_pipeline = copy.deepcopy(config["integration_spec"]["pipeline"])
    proc_keys = ("main_process", multi["second_process_key"])

    refusals, plans = [], []
    for existing in (
        [],
        [copy.deepcopy(main_stub)],
        [copy.deepcopy(main_stub), copy.deepcopy(case["second_collision_stub_metadata"])],
    ):
        mock_pag.return_value = existing
        refused = _build_plan(MagicMock(), copy.deepcopy(config))
        _assert_unit_refusal(refused)
        refusals.append(refused)
        plan = _build_plan(MagicMock(), _bind_processes(config))
        steps = [next(s for s in plan["steps"] if s["key"] == key) for key in proc_keys]
        assert [s["planned_action"] for s in steps] == ["reuse", "reuse"]
        assert [s for s in plan["steps"] if str(s["planned_action"]).startswith("error")] == []
        echoed = plan["integration_spec"]["pipeline"]
        assert echoed == PipelineSpec(**authored_pipeline).model_dump()
        assert [s["kind"] for s in echoed["stages"]] == case["expected_spec_pipeline_kinds"]
        assert "LEGACY_ADAPTER_AUTHORITY_CONFLICT" not in json.dumps(plan)
        plans.append(plan)
    assert refusals[0] == refusals[1] == refusals[2]
    assert plans[0] == plans[1] == plans[2]


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


def test_sync_pipeline_rejects_unknown_stage_config_key():
    """StageSpec.config is model-open (test 5) but the sync_pipeline builder is
    strict about it: an unknown key inside a stage's config is rejected with an
    exact code/field pair."""
    case = _case("sync_pipeline_stage_config_unknown_key")
    err = SyncPipelineBuilder.validate_config(case["config"])
    assert isinstance(err, BuilderValidationError)
    assert err.error_code == case["expected_error_code"]
    assert err.field == case["expected_field"]


def test_flow_sequence_accepts_single_step_and_ignores_root_extras():
    """Full-config boundary through ProcessFlowBuilder.validate_config: a
    ONE-step flow_sequence is accepted (no 2+ minimum), and an unknown root key
    (with no $ref token in its value) is accepted AND ignored — build() output
    is string-identical with and without it (the config root has no allowlist).
    The root is NOT unconditionally inert: the cross-cutting $ref reachability
    scan reads unknown root values too, so a $ref token there is rejected."""
    case = _case("flow_sequence_composed")
    base = copy.deepcopy(case["base_config"])
    assert ProcessFlowBuilder.validate_config(base) is None
    with_extra = copy.deepcopy(base)
    with_extra[case["bogus_root_key"]] = case["bogus_root_value"]
    assert ProcessFlowBuilder.validate_config(with_extra) is None
    xml_base = ProcessFlowBuilder.build(
        copy.deepcopy(base), name=case["process_name"]
    )
    xml_extra = ProcessFlowBuilder.build(
        copy.deepcopy(with_extra), name=case["process_name"]
    )
    assert xml_base == xml_extra
    # Boundary of the leniency: a $ref token inside an unknown root extra is
    # caught by the reachability scan, not ignored.
    with_ref_extra = copy.deepcopy(base)
    with_ref_extra[case["bogus_root_key"]] = case["bogus_root_ref_value"]
    err = ProcessFlowBuilder.validate_config(with_ref_extra)
    assert isinstance(err, BuilderValidationError)
    assert err.error_code == case["expected_root_ref_error_code"]
    assert err.field == case["expected_root_ref_field"]
    # Declarations flow through validate_config's depends_on= KEYWORD (the
    # plan layer's conduit) — a depends_on KEY inside the config dict is just
    # another ignored root extra, never a declaration.
    bare_declared = copy.deepcopy(base)
    bare_declared["depends_on"] = [case["declared_dep_key"]]
    bare_declared[case["bogus_root_key"]] = case["declared_ref_token"]
    err = ProcessFlowBuilder.validate_config(bare_declared)
    assert isinstance(err, BuilderValidationError)
    assert err.error_code == case["expected_root_ref_error_code"]
    assert err.field == case["expected_root_ref_field"]
    # Declared via the keyword parameter → accepted at the direct-builder layer.
    kwarg_declared = copy.deepcopy(base)
    kwarg_declared[case["bogus_root_key"]] = case["declared_ref_token"]
    assert (
        ProcessFlowBuilder.validate_config(
            kwarg_declared, depends_on=[case["declared_dep_key"]]
        )
        is None
    )
    # build() takes no declaration parameter and never runs the scan: emitted
    # XML is byte-identical with or without a ref-bearing extra, declared or
    # not — the reachability scan is validation/plan-time only.
    xml_declared_extra = ProcessFlowBuilder.build(
        copy.deepcopy(kwarg_declared), name=case["process_name"]
    )
    undeclared_config = copy.deepcopy(base)
    undeclared_config[case["bogus_root_key"]] = case["bogus_root_ref_value"]
    xml_undeclared_extra = ProcessFlowBuilder.build(
        copy.deepcopy(undeclared_config), name=case["process_name"]
    )
    assert xml_declared_extra == xml_base
    assert xml_undeclared_extra == xml_base


@patch("src.boomi_mcp.categories.integration_builder.paginate_metadata")
def test_flow_sequence_declared_ref_root_extra_ignored_at_plan_layer(mock_pag):
    """At the _build_plan layer a flow_sequence process body is no longer planned.

    #159 S3 re-pin. The plan-layer leniency this pinned (a declared `$ref` in an
    unknown root extra accepted, an undeclared one rejected on the step, the
    update arm alike, and a same-name reuse skipping validation) belonged to the
    retired flat-body route. Every one of those variants is now refused at
    admission with the SAME value-free envelope — the root extra, its `$ref`
    token, the action and the live account change nothing, because admission
    reads key presence only. The direct-builder leniency stays pinned by
    `test_flow_sequence_accepts_single_step_and_ignores_root_extras`.
    """
    mock_pag.return_value = []
    case = _case("flow_sequence_composed")

    def _spec(extra_value=None):
        config = copy.deepcopy(case["base_config"])
        if extra_value is not None:
            config[case["bogus_root_key"]] = extra_value
        return {
            "integration_spec": {
                "name": "Sentinel Composed Plan",
                "components": [
                    {
                        "key": "composed_process",
                        "type": "process",
                        "name": case["process_name"],
                        "depends_on": [case["declared_dep_key"]],
                        "config": config,
                    },
                    copy.deepcopy(case["dep_stub_component"]),
                ],
            }
        }

    field = "integration_spec.components.0.config.process_kind"
    baseline = _build_plan(MagicMock(), _spec())
    _assert_unit_refusal(baseline, field=field)

    declared = _build_plan(MagicMock(), _spec(case["declared_ref_token"]))
    undeclared = _build_plan(MagicMock(), _spec(case["bogus_root_ref_value"]))
    update_spec = _spec(case["bogus_root_ref_value"])
    update_comp = update_spec["integration_spec"]["components"][0]
    update_comp["action"] = "update"
    update_comp["component_id"] = "99999999-9999-9999-9999-999999999999"
    updated = _build_plan(MagicMock(), update_spec)
    mock_pag.return_value = [
        {
            "component_id": "99999999-9999-9999-9999-999999999999",
            "id": "99999999-9999-9999-9999-999999999999",
            "name": case["process_name"],
            "folder_name": "Root",
            "type": "process",
            "version": 1,
        }
    ]
    try:
        reused = _build_plan(MagicMock(), _spec(case["bogus_root_ref_value"]))
    finally:
        mock_pag.return_value = []
    for plan in (declared, undeclared, updated, reused):
        assert plan == baseline
    blob = json.dumps(baseline)
    for leak in (
        case["expected_root_ref_error_code"],
        case["bogus_root_key"],
        case["process_name"],
        "missing_sentinel",
        "DDP_SENTINEL_KEY",
    ):
        assert leak not in blob


def test_flow_sequence_rejects_branch_leg_and_recursive_step_extras():
    """Recursive strictness through ProcessFlowBuilder.validate_config: unknown
    keys on a branch LEG object and on a step NESTED inside a leg are both
    rejected with exact code/field pairs (the field is the recursive path)."""
    case = _case("flow_sequence_composed")
    valid = copy.deepcopy(case["base_config"])
    valid["flow_sequence"] = [copy.deepcopy(case["branch_step"])]
    assert ProcessFlowBuilder.validate_config(valid) is None

    leg_extra = copy.deepcopy(valid)
    leg_extra["flow_sequence"][0]["legs"][0]["bogus_leg_key"] = True
    err = ProcessFlowBuilder.validate_config(leg_extra)
    assert isinstance(err, BuilderValidationError)
    assert err.error_code == case["expected_error_code"]
    assert err.field == case["expected_leg_extra_field"]

    nested_extra = copy.deepcopy(valid)
    nested_extra["flow_sequence"][0]["legs"][0]["steps"][0]["bogus"] = 1
    err = ProcessFlowBuilder.validate_config(nested_extra)
    assert isinstance(err, BuilderValidationError)
    assert err.error_code == case["expected_error_code"]
    assert err.field == case["expected_nested_step_extra_field"]


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

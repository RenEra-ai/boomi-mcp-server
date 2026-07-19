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
- the ``spec.pipeline`` PROCESS-CARDINALITY combinations ADR-001 §5 is built on:
  zero-authored, lone-create-collapsing-to-collision-reuse (in both its agreeing
  and disagreeing authored-view variants), and multi-authored (with no collision,
  one of two colliding, and both colliding to reuse) -- each carrying a
  top-level pipeline, each accepted today,
- the ``spec.pipeline`` secret-scan gap, in BOTH its zero-component isolation and
  its component-bearing form (with a control proving a real scanner ran),
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


def test_zero_process_pipeline_secret_config_echoed_is_known_gap():
    """KNOWN GAP characterization (ADR-001 §5 security-precedence note + §11;
    compatibility inventory §2.5). A zero-process spec (`components: []`) whose
    top-level `spec.pipeline` stage `config` carries a SECRET-SHAPED key is
    ACCEPTED and the secret value is ECHOED BACK UNCHANGED by `_build_plan` —
    because the per-component `scan_forbidden_secret_fields` scanners (invoked
    per component at integration_builder.py:5338/5479/5566/5625/5744/5774 and
    the generic/profile scanner at :6032) traverse only each component's own
    raw_config, never the top-level `spec.pipeline`; `StageSpec.config` is an
    open `Dict[str, Any]`. The gap is
    component-independent (no scanner ever reaches spec.pipeline); components=[]
    is just the cleanest isolation (zero scanners run at all). That
    component-independence is NOT self-attested here — it is measured by the
    sibling `test_component_bearing_pipeline_secret_config_echoed_is_known_gap`,
    whose control run proves a real component scanner fires while this same gap
    holds in the very same plan.

    This is the leak ADR §11 flags. It is PRE-EXISTING (not introduced by #135)
    and #135 only CHARACTERIZES it — it does NOT fix it (a runtime secret scan
    over `spec.pipeline` stage config is a behavior change owned by the #139
    legacy adapter, whose contract already forbids promoting free-form
    credential fields into derived pipeline summaries). Frozen, NOT endorsed:
    if a downstream scan starts rejecting/redacting this, flip this test.

    The sentinel value is a PLACEHOLDER token (§11: fixtures use sentinels only),
    never a real secret.
    """
    case = _case("zero_process_pipeline_with_secret")
    secret_key = case["secret_key"]
    secret_val = case["secret_sentinel"]

    # Accepted: zero-process spec plans clean, no executable steps.
    plan = _build_plan(MagicMock(), copy.deepcopy(case["config"]))
    assert plan["_success"] is True
    assert plan["steps"] == []

    # THE GAP: the secret-shaped value survives verbatim in the echoed spec —
    # no scan touched it, no redaction happened.
    echoed_stage_cfg = plan["integration_spec"]["pipeline"]["stages"][0]["config"]
    assert echoed_stage_cfg[secret_key] == secret_val

    # And it is NOT surfaced as any validation error / rejection (no scanner ran).
    assert "PLAINTEXT_SECRET_REJECTED" not in json.dumps(plan)


@patch("src.boomi_mcp.categories.integration_builder.paginate_metadata")
def test_component_bearing_pipeline_secret_config_echoed_is_known_gap(mock_pag):
    """Component-BEARING half of the same KNOWN GAP (ADR-001 §5 security-precedence
    note + §11; compatibility inventory §2.5). The sibling
    `test_zero_process_pipeline_secret_config_echoed_is_known_gap` isolates the gap
    with `components: []` (zero scanners run at all); this case proves the strictly
    STRONGER claim the inventory actually makes — that no scanner traverses the
    top-level `spec.pipeline` **whatever components exist**.

    The control run is what licenses that claim: it is the SAME spec with the sentinel
    ALSO planted in the component's own config, and it fails with
    PLAINTEXT_SECRET_REJECTED — so that component's scanner demonstrably runs on this
    exact shape, and the `spec.pipeline` secret still sails past it unscanned in the
    very same plan.

    Frozen, NOT endorsed (the leak is PRE-EXISTING and owned by #139): if a downstream
    scan starts rejecting/redacting the spec.pipeline value, flip this test.
    The sentinel is a PLACEHOLDER token (§11), never a real secret.
    """
    mock_pag.return_value = []
    case = _case("component_bearing_pipeline_with_secret")
    secret_key = case["secret_key"]
    secret_val = case["secret_sentinel"]

    # THE GAP: components exist and plan clean, yet the spec.pipeline secret is echoed.
    plan = _build_plan(MagicMock(), copy.deepcopy(case["config"]))
    # Clean-planned means NO error steps (`_build_plan` hardcodes _success=True).
    assert [s for s in plan["steps"] if str(s["planned_action"]).startswith("error")] == []
    comp_step = next(s for s in plan["steps"] if s["key"] == case["component_key"])
    assert comp_step["planned_action"] == case["expected_component_action"]
    echoed_stage_cfg = plan["integration_spec"]["pipeline"]["stages"][0]["config"]
    assert echoed_stage_cfg[secret_key] == secret_val
    assert "PLAINTEXT_SECRET_REJECTED" not in json.dumps(plan)

    # THE CONTROL: the same sentinel in the component's OWN config IS rejected, proving
    # a real scanner traverses this component — while the spec.pipeline copy survives
    # unredacted in that same plan.
    control = _build_plan(MagicMock(), copy.deepcopy(case["control_config"]))
    control_step = next(s for s in control["steps"] if s["key"] == case["component_key"])
    assert control_step["planned_action"] == case["control_expected_action"]
    assert (
        control_step["validation_error"]["error_code"]
        == case["control_expected_error_code"]
    )
    assert control_step["validation_error"]["field"] == case["control_secret_key"]
    control_stage_cfg = control["integration_spec"]["pipeline"]["stages"][0]["config"]
    assert control_stage_cfg[secret_key] == secret_val


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
    assert (
        broken_step["validation_error"]["error_code"]
        == "SYNC_PIPELINE_STAGE_UNSUPPORTED"
    )
    assert (
        broken_step["validation_error"]["field"] == "pipeline.stages[read_stage].kind"
    )

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


@patch("src.boomi_mcp.categories.integration_builder.paginate_metadata")
def test_collision_reuse_with_top_level_pipeline_plans_clean_and_echoes(mock_pag):
    """Cardinality freeze (ADR-001 §5). The IDENTICAL authored payload as
    `test_contradictory_pipelines_silent_precedence_through_build_plan` — which forces
    NO collision — but here a live same-name process EXISTS, so the lone authored
    `create` collapses to `planned_action="reuse"`. It still plans clean (no error
    steps) and still echoes the authored `spec.pipeline` inert.

    This is load-bearing for two ADR §5 claims that were previously only INFERRED from
    the composition of two other tests:
      - the preserve-inert case's collision-driven-reuse sub-case, and
      - the account-independence invariant: acceptance does not move when collision
        resolution changes. Compare against the no-collision pin above — same payload,
        different live account, same acceptance.

    Note the agree-vs-conflict distinction is invisible TODAY: no code compares
    `spec.pipeline` to the process config at all (that comparison is #139's future
    contract). This pin measures the DISAGREEING authored view; the agreeing variant
    is pinned separately below, so each sub-case ADR §5 names has its own direct
    measurement even though today both exercise the identical comparison-free echo.
    """
    case = _case("collision_reuse_pipeline")
    base = _case("contradictory_pipelines")

    mock_pag.return_value = [copy.deepcopy(case["collision_stub_metadata"])]
    plan = _build_plan(MagicMock(), copy.deepcopy(base["config"]))

    main_step = next(s for s in plan["steps"] if s["key"] == "main_process")
    assert main_step["planned_action"] == case["expected_planned_action"]
    assert [s for s in plan["steps"] if str(s["planned_action"]).startswith("error")] == []
    # The inert authored view is echoed even though the submitted config was discarded.
    echoed = plan["integration_spec"]["pipeline"]
    assert [s["kind"] for s in echoed["stages"]] == case["expected_spec_pipeline_kinds"]
    assert "LEGACY_ADAPTER_AUTHORITY_CONFLICT" not in json.dumps(plan)


@patch("src.boomi_mcp.categories.integration_builder.paginate_metadata")
def test_collision_reuse_with_agreeing_top_level_pipeline_plans_clean_and_echoes(mock_pag):
    """Agreeing variant of the collision-reuse pin (ADR-001 §5). The preserve-inert
    bullet's collision-driven-reuse sub-case is defined over a `spec.pipeline` that
    does NOT conflict with the process's derived summary; the pin above measures the
    disagreeing payload (which §5 files under the authority-conflict case, preserved
    on V1 only until the §9 gate). This pin measures the sub-case as defined: the
    authored top-level view is a verbatim copy of the nested `config.pipeline` —
    agreeing by construction under any equality #139 later chooses — and the lone
    authored `create` collapses to `planned_action="reuse"` against the same live
    same-name component.

    Today the variants are indistinguishable (no code compares the two surfaces; the
    comparison is #139's future contract), so both pin the identical comparison-free
    echo. Keeping both measured means #139's V1 surface — which must KEEP accepting
    the agreeing shape while the disagreeing one is governed by the §9 transition —
    changes a pinned behavior only where the ADR says it may.
    """
    case = _case("collision_reuse_agreeing_pipeline")
    stub = _case("collision_reuse_pipeline")["collision_stub_metadata"]
    config = _case("contradictory_pipelines")["config"]
    authored_pipeline = copy.deepcopy(
        config["integration_spec"]["components"][0]["config"]["pipeline"]
    )
    config["integration_spec"]["pipeline"] = copy.deepcopy(authored_pipeline)

    mock_pag.return_value = [stub]
    plan = _build_plan(MagicMock(), config)

    main_step = next(s for s in plan["steps"] if s["key"] == "main_process")
    assert main_step["planned_action"] == case["expected_planned_action"]
    assert [s for s in plan["steps"] if str(s["planned_action"]).startswith("error")] == []
    # Echo fidelity is this pin's whole job, so assert the FULL normalized
    # PipelineSpec dump (defaults expanded, semantics preserved — the zero-process
    # preserve pin's idiom), not merely the same kinds sequence.
    echoed = plan["integration_spec"]["pipeline"]
    assert echoed == PipelineSpec(**authored_pipeline).model_dump()
    assert [s["kind"] for s in echoed["stages"]] == case["expected_spec_pipeline_kinds"]
    assert "LEGACY_ADAPTER_AUTHORITY_CONFLICT" not in json.dumps(plan)


@patch("src.boomi_mcp.categories.integration_builder.paginate_metadata")
def test_multi_authored_spec_with_top_level_pipeline_accepted_today(mock_pag):
    """Cardinality freeze (ADR-001 §5, multi-authored bullet). TWO authored process
    components plus a singular top-level `spec.pipeline` — the shape the ADR calls
    "ambiguous by construction" because no marker designates which process the single
    field summarizes — is ACCEPTED today: both processes plan `create`, no error step,
    no authority-conflict code, and the ambiguous view is echoed back.

    This is the measured evidence for ADR §9's announced-policy-before-removal gate:
    #139's strict-surface rejection withdraws an acceptance that demonstrably exists,
    so it cannot ship as a side effect of the cutover. No current archetype writes this
    shape (§1 measured — multi-process specs keep `pipeline=None`); it is reachable by
    hand-authoring, which is exactly why the freeze pins it.
    """
    mock_pag.return_value = []
    case = _case("multi_authored_pipeline")
    config = _case("contradictory_pipelines")["config"]

    # Add a SECOND authored process alongside the first (distinct key and name, so it
    # is a genuinely separate authored component rather than a collision).
    second = copy.deepcopy(config["integration_spec"]["components"][0])
    second["key"] = case["second_process_key"]
    second["name"] = case["second_process_name"]
    config["integration_spec"]["components"].insert(1, second)

    plan = _build_plan(MagicMock(), config)

    proc_steps = [
        next(s for s in plan["steps"] if s["key"] == key)
        for key in ("main_process", case["second_process_key"])
    ]
    assert [s["planned_action"] for s in proc_steps] == case["expected_planned_actions"]
    assert [s for s in plan["steps"] if str(s["planned_action"]).startswith("error")] == []
    echoed = plan["integration_spec"]["pipeline"]
    assert [s["kind"] for s in echoed["stages"]] == case["expected_spec_pipeline_kinds"]
    assert "LEGACY_ADAPTER_AUTHORITY_CONFLICT" not in json.dumps(plan)


@patch("src.boomi_mcp.categories.integration_builder.paginate_metadata")
def test_multi_authored_collision_reuse_keeps_acceptance_and_echo(mock_pag):
    """Collision variants of the multi-authored pin (ADR-001 §5, multi-authored
    bullet + determinism note). §5 counts the ambiguity decision over DECLARED
    authoring actions, so it claims account-independence "even if some or all of
    those authored processes collapse to collision-driven reuse". This pin
    measures the baseline for both halves of that claim: the SAME two-authored
    payload as `test_multi_authored_spec_with_top_level_pipeline_accepted_today`,
    replanned against a live account where (a) ONE of the two authored creates
    collides (partial — ["reuse", "create"]) and (b) BOTH collide (full —
    ["reuse", "reuse"]).

    Today both variants plan clean and echo the ambiguous view — identical to
    the no-collision pin, since no code counts authored processes or compares
    surfaces yet. The pins keep #139 honest about WHERE it counts: a cardinality
    computed AFTER collision resolution would see the full-collision variant as
    zero-authored (preserve-inert path) and the partial variant as
    single-authored (conflict/derive path) instead of ambiguous — exactly the
    account-dependent reject/accept flip §5's determinism note prohibits. On the
    V1 surface these acceptances persist until an announced §9 deprecation, so
    the pins are durable, not transition-scoped.
    """
    case = _case("multi_authored_collision_pipeline")
    multi = _case("multi_authored_pipeline")
    main_stub = _case("collision_reuse_pipeline")["collision_stub_metadata"]

    config = _case("contradictory_pipelines")["config"]
    second = copy.deepcopy(config["integration_spec"]["components"][0])
    second["key"] = multi["second_process_key"]
    second["name"] = multi["second_process_name"]
    config["integration_spec"]["components"].insert(1, second)
    proc_keys = ("main_process", multi["second_process_key"])

    # (a) PARTIAL collision — only the first authored process has a live
    # same-name component; the second stays a genuine create.
    mock_pag.return_value = [copy.deepcopy(main_stub)]
    plan_partial = _build_plan(MagicMock(), copy.deepcopy(config))
    partial_steps = [
        next(s for s in plan_partial["steps"] if s["key"] == key) for key in proc_keys
    ]
    assert [s["planned_action"] for s in partial_steps] == (
        case["expected_partial_planned_actions"]
    )
    assert [
        s for s in plan_partial["steps"] if str(s["planned_action"]).startswith("error")
    ] == []
    echoed_partial = plan_partial["integration_spec"]["pipeline"]
    assert [s["kind"] for s in echoed_partial["stages"]] == (
        case["expected_spec_pipeline_kinds"]
    )
    assert "LEGACY_ADAPTER_AUTHORITY_CONFLICT" not in json.dumps(plan_partial)

    # (b) FULL collision — both authored processes collapse to reuse.
    mock_pag.return_value = [
        copy.deepcopy(main_stub),
        copy.deepcopy(case["second_collision_stub_metadata"]),
    ]
    plan_full = _build_plan(MagicMock(), copy.deepcopy(config))
    full_steps = [
        next(s for s in plan_full["steps"] if s["key"] == key) for key in proc_keys
    ]
    assert [s["planned_action"] for s in full_steps] == (
        case["expected_full_planned_actions"]
    )
    assert [
        s for s in plan_full["steps"] if str(s["planned_action"]).startswith("error")
    ] == []
    echoed_full = plan_full["integration_spec"]["pipeline"]
    assert [s["kind"] for s in echoed_full["stages"]] == (
        case["expected_spec_pipeline_kinds"]
    )
    assert "LEGACY_ADAPTER_AUTHORITY_CONFLICT" not in json.dumps(plan_full)


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
    """At the _build_plan layer (where depends_on is declared on the component
    spec), a $ref token declared in depends_on inside an unknown root extra is
    accepted and ignored like any other extra — identical planned steps — while
    an undeclared token is rejected on the process step."""
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

    baseline = _build_plan(MagicMock(), _spec())
    assert baseline["_success"] is True
    base_step = next(s for s in baseline["steps"] if s["key"] == "composed_process")
    assert base_step["planned_action"] == "create"

    declared = _build_plan(MagicMock(), _spec(case["declared_ref_token"]))
    assert declared["_success"] is True
    declared_step = next(
        s for s in declared["steps"] if s["key"] == "composed_process"
    )
    assert declared_step["planned_action"] == "create"
    assert "validation_error" not in declared_step
    assert declared["steps"] == baseline["steps"]
    assert declared["execution_order"] == baseline["execution_order"]

    undeclared = _build_plan(MagicMock(), _spec(case["bogus_root_ref_value"]))
    undeclared_step = next(
        s for s in undeclared["steps"] if s["key"] == "composed_process"
    )
    assert undeclared_step["planned_action"] == "error_process_validation"
    assert (
        undeclared_step["validation_error"]["error_code"]
        == case["expected_root_ref_error_code"]
    )

    # The update path is an authoring action too: the identical undeclared-ref
    # config with action="update" + component_id is rejected the same way.
    update_spec = _spec(case["bogus_root_ref_value"])
    update_comp = update_spec["integration_spec"]["components"][0]
    update_comp["action"] = "update"
    update_comp["component_id"] = "99999999-9999-9999-9999-999999999999"
    updated = _build_plan(MagicMock(), update_spec)
    updated_step = next(
        s for s in updated["steps"] if s["key"] == "composed_process"
    )
    assert updated_step["planned_action"] == "error_process_validation"
    assert (
        updated_step["validation_error"]["error_code"]
        == case["expected_root_ref_error_code"]
    )

    # The rejection is an AUTHORING-action behavior: a same-name match under
    # the default conflict_policy="reuse" skips builder validation entirely —
    # the identical undeclared-ref config plans as a clean reuse step.
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
    reused_step = next(s for s in reused["steps"] if s["key"] == "composed_process")
    assert reused["_success"] is True
    assert reused_step["planned_action"] == "reuse"
    assert "validation_error" not in reused_step or (
        reused_step["validation_error"] is None
    )


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

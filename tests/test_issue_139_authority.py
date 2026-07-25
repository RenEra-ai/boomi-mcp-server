"""Issue #139D — strict (``version="1.1"``) top-level pipeline-authority selector.

ADR-001 §5 requires #139 to replace today's *silent precedence* (an authored
``spec.pipeline`` that contradicts the executable process config is accepted and
ignored) with either derived equality or a stable
``LEGACY_ADAPTER_AUTHORITY_CONFLICT`` rejection — and §9 requires that rejection
to land on a NEW opt-in surface, because hard-rejecting a payload that plans
clean today would be an unannounced compatibility break.

The two halves this suite pins:

* **V1 (``version="1.0"``) is frozen.** Everything accepted today stays accepted,
  with its inert echo intact. ``tests/test_issue_135_compatibility_freeze.py``
  owns the baseline; this suite adds the "selector did not leak" direction.
* **Strict (``version="1.1"``) rejects** ambiguity and disagreement at plan time,
  before collision resolution and before any mutation, and its accept-vs-reject
  outcome never depends on live account contents.
"""

import copy
import json
from pathlib import Path
from unittest.mock import MagicMock, patch

import pytest
from pydantic import BaseModel, ValidationError
from typing import Literal

from src.boomi_mcp.models.integration_models import IntegrationSpecV1
from src.boomi_mcp.categories.integration_builder import _build_plan, _normalize_to_spec
from src.boomi_mcp.compiler.process_ir.legacy_adapters.authority import (
    AGREE,
    AMBIGUOUS,
    DISAGREE,
    NOT_APPLICABLE,
    NOT_REPRESENTABLE,
    PRESERVE_INERT,
    STRICT_VERSION,
    UNDECIDABLE,
    evaluate_pipeline_authority,
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

_AUTHORITY_CODE = "LEGACY_ADAPTER_AUTHORITY_CONFLICT"


def _case(name):
    return copy.deepcopy(_FIXTURE[name])


def _collision_stub():
    return copy.deepcopy(_FIXTURE["collision_reuse_pipeline"]["collision_stub_metadata"])


def _plan(config, existing=None):
    """Plan ``config`` against an account that returns ``existing`` metadata."""
    with patch(
        "src.boomi_mcp.categories.integration_builder.paginate_metadata"
    ) as mock_pag:
        mock_pag.return_value = list(existing or [])
        return _build_plan(MagicMock(), copy.deepcopy(config))


def _contradictory(version=None):
    """The #135 contradictory payload: top-level pipeline disagrees with nested."""
    config = _case("contradictory_pipelines")["config"]
    if version is not None:
        config["integration_spec"]["version"] = version
    return config


def _agreeing(version=STRICT_VERSION):
    """The same payload with the top-level view copied from the nested pipeline.

    Agreement by construction under any equality the comparison chooses.
    """
    config = _contradictory(version)
    nested = config["integration_spec"]["components"][0]["config"]["pipeline"]
    config["integration_spec"]["pipeline"] = copy.deepcopy(nested)
    return config


def _main_step(plan):
    return next(s for s in plan["steps"] if s["key"] == "main_process")


def _echoed_pipeline(plan):
    return plan["integration_spec"]["pipeline"]


# ---------------------------------------------------------------------------
# 1. The selector itself — and why it has to be a `version` literal
# ---------------------------------------------------------------------------


def test_version_accepts_exactly_the_two_surfaces():
    assert IntegrationSpecV1(name="x").version == "1.0"
    assert IntegrationSpecV1(name="x", version="1.0").version == "1.0"
    assert IntegrationSpecV1(name="x", version=STRICT_VERSION).version == "1.1"
    for bad in ("1", "1.2", "2.0", "", None, 1.1):
        with pytest.raises(ValidationError):
            IntegrationSpecV1(name="x", version=bad)


def test_version_round_trips_through_the_dump():
    dumped = IntegrationSpecV1(name="x", version=STRICT_VERSION).model_dump()
    assert dumped["version"] == "1.1"
    assert IntegrationSpecV1(**dumped).version == "1.1"


def test_published_json_schema_pins_enum_and_default():
    """The strict surface must be discoverable — get_schema_template publishes
    IntegrationSpecV1.model_json_schema() verbatim."""
    field = IntegrationSpecV1.model_json_schema()["properties"]["version"]
    assert field["enum"] == ["1.0", "1.1"]
    assert field["default"] == "1.0"


def test_selector_is_fail_closed_on_a_pre_139_server():
    """ADR-001 §5: the selector must be one a pre-#139 server CANNOT silently drop.

    An ordinary optional field would be: IntegrationSpecV1 sets no model_config,
    so pydantic's default extra="ignore" discards unknown keys (pinned by
    test_spec_envelope_ignores_unknown_fields in the #135 freeze suite) and the
    request would degrade to legacy precedence — the exact failure the ADR names.
    A new `version` literal cannot degrade: the old contract REJECTS it.
    """

    class PreIssue139Spec(BaseModel):
        version: Literal["1.0"] = "1.0"

    # An unknown opt-in field is silently swallowed by the real (extra=ignore) model...
    swallowed = IntegrationSpecV1(name="x", strict_authority=True)
    assert not hasattr(swallowed, "strict_authority")
    assert swallowed.version == "1.0"

    # ...whereas the chosen selector is rejected outright by the old contract.
    assert PreIssue139Spec(version="1.0").version == "1.0"
    with pytest.raises(ValidationError):
        PreIssue139Spec(version=STRICT_VERSION)


def test_strict_surface_is_reachable_only_through_the_explicit_spec_form():
    """`source_description` / bare top-level forms rebuild the spec from a fixed
    key allowlist carrying no `version`, so they can never claim strictness.
    That is fail-SAFE (it degrades to frozen V1), and it is contractual."""
    assert _normalize_to_spec(
        {"integration_spec": {"name": "n", "version": STRICT_VERSION}}
    ).version == "1.1"
    assert _normalize_to_spec(
        {"source_description": {"name": "n", "version": STRICT_VERSION}}
    ).version == "1.0"
    assert _normalize_to_spec(
        {"name": "n", "version": STRICT_VERSION, "components": []}
    ).version == "1.0"


# ---------------------------------------------------------------------------
# 2. V1 stays frozen — the selector-leak detector
# ---------------------------------------------------------------------------


def test_v1_contradiction_still_plans_clean_and_echoes():
    """The #135 silent-precedence baseline, restated here as a leak detector: if
    this ever fails, the strict surface has leaked into V1."""
    plan = _plan(_contradictory())
    assert plan["_success"] is True
    assert _main_step(plan)["planned_action"] == "create"
    assert _echoed_pipeline(plan) is not None
    assert _AUTHORITY_CODE not in json.dumps(plan)


@pytest.mark.parametrize("version", ["1.0", STRICT_VERSION])
def test_no_top_level_pipeline_is_identical_on_both_surfaces(version):
    """`version="1.1"` changes NOTHING unless a top-level pipeline is authored."""
    config = _contradictory(version)
    config["integration_spec"]["pipeline"] = None
    plan = _plan(config)
    assert plan["_success"] is True
    assert _AUTHORITY_CODE not in json.dumps(plan)


def test_strict_and_legacy_plans_are_identical_apart_from_the_version_when_agreeing():
    """An agreeing strict payload must plan exactly like its V1 twin."""
    strict = _plan(_agreeing(STRICT_VERSION))
    legacy = _plan(_agreeing("1.0"))
    assert strict["_success"] is legacy["_success"] is True
    assert strict["steps"] == legacy["steps"]
    assert strict["execution_order"] == legacy["execution_order"]
    strict_spec = dict(strict["integration_spec"])
    legacy_spec = dict(legacy["integration_spec"])
    assert strict_spec.pop("version") == "1.1"
    assert legacy_spec.pop("version") == "1.0"
    assert strict_spec == legacy_spec


# ---------------------------------------------------------------------------
# 3. Cardinality dispositions (ADR-001 §5)
# ---------------------------------------------------------------------------


def test_strict_disagreement_is_rejected():
    plan = _plan(_contradictory(STRICT_VERSION))
    assert plan["_success"] is False
    assert plan["error_code"] == _AUTHORITY_CODE
    assert plan["field"] == "integration_spec.pipeline"
    assert "conflicts with" in plan["error"]
    assert plan["hint"]


def test_strict_agreement_is_accepted_and_keeps_the_view():
    plan = _plan(_agreeing())
    assert plan["_success"] is True
    assert _echoed_pipeline(plan) is not None
    assert _AUTHORITY_CODE not in json.dumps(plan)


def test_strict_multi_authored_is_ambiguous():
    config = _agreeing()
    second = copy.deepcopy(config["integration_spec"]["components"][0])
    second["key"] = "second_process"
    second["name"] = "Second Sentinel Process"
    config["integration_spec"]["components"].append(second)
    plan = _plan(config)
    assert plan["_success"] is False
    assert plan["error_code"] == _AUTHORITY_CODE
    assert "multiple authored" in plan["error"]


def test_strict_zero_authored_preserves_the_inert_view():
    config = _agreeing()
    config["integration_spec"]["components"] = []
    plan = _plan(config)
    assert plan["_success"] is True
    assert _echoed_pipeline(plan) is not None


def _spec(components, version=STRICT_VERSION, pipeline=None):
    base = _agreeing(version)["integration_spec"]
    return IntegrationSpecV1(
        name=base["name"],
        version=version,
        pipeline=pipeline if pipeline is not None else base["pipeline"],
        components=components,
    )


def test_declared_authoring_predicate_excludes_only_a_reference_only_create():
    """`action` is Literal["create","update"] — the only authorable actions. An
    update ALWAYS authors (it re-emits the XML from its config) even when flagged
    reference_only, because the planner honours that flag ONLY for create.

    Asserted on the predicate directly: both exclusions are properties of the
    caller's DECLARATION, which is what makes the ambiguity count
    account-independent, and neither depends on the config being lowerable.
    """
    from src.boomi_mcp.compiler.process_ir.legacy_adapters.authority import (
        _is_authoring_process,
    )

    def comp(action="create", type_="process", **config):
        return IntegrationSpecV1(
            name="n",
            components=[
                {"key": "k", "type": type_, "action": action, "config": config}
            ],
        ).components[0]

    assert _is_authoring_process(comp()) is True
    assert _is_authoring_process(comp(action="update")) is True
    # The one exclusion: a create flagged reference_only is this model's concrete
    # representation of a pure reference.
    assert _is_authoring_process(comp(reference_only=True)) is False
    # An update authors even so.
    assert _is_authoring_process(comp(action="update", reference_only=True)) is True
    # Non-process components never author a process view.
    assert _is_authoring_process(comp(type_="connector-settings")) is False


def test_a_reference_only_create_does_not_make_a_spec_ambiguous():
    """Two components, one of them a pure reference -> still single-authored."""
    proc = _agreeing()["integration_spec"]["components"][0]
    ref_only = copy.deepcopy(proc)
    ref_only["key"] = "reused_process"
    ref_only["name"] = "Reused Sentinel Process"
    ref_only["config"] = {"reference_only": True, "component_id": "abc"}

    assert evaluate_pipeline_authority(_spec([proc, ref_only])).disposition == AGREE
    assert evaluate_pipeline_authority(_spec([ref_only])).disposition == PRESERVE_INERT


def test_non_process_components_never_count_as_authors():
    proc = _agreeing()["integration_spec"]["components"][0]
    connection = {
        "key": "db_conn",
        "type": "connector-settings",
        "action": "create",
        "name": "Sentinel Conn",
        "config": {},
    }
    assert (
        evaluate_pipeline_authority(_spec([proc, connection])).disposition == AGREE
    )
    assert (
        evaluate_pipeline_authority(_spec([connection])).disposition == PRESERVE_INERT
    )


def test_explicit_component_id_create_still_counts_as_an_author():
    proc = copy.deepcopy(_agreeing()["integration_spec"]["components"][0])
    proc["component_id"] = "11111111-1111-1111-1111-111111111111"
    assert evaluate_pipeline_authority(_spec([proc])).disposition == AGREE


def test_legacy_surface_is_never_evaluated():
    for pipeline_present in (True, False):
        config = _contradictory("1.0")
        if not pipeline_present:
            config["integration_spec"]["pipeline"] = None
        spec = _normalize_to_spec(config)
        assert evaluate_pipeline_authority(spec).disposition == NOT_APPLICABLE


# ---------------------------------------------------------------------------
# 4. The clean-plan gate, and what outranks it
# ---------------------------------------------------------------------------


def test_missing_process_kind_is_undecidable_and_keeps_its_own_error():
    """ADR-001 §5 clean-plan gate: with no comparable semantics there is no
    authority disposition, and the payload's own error surfaces untouched."""
    config = _agreeing()
    config["integration_spec"]["components"][0]["config"].pop("process_kind")
    assert (
        evaluate_pipeline_authority(_normalize_to_spec(config)).disposition
        == UNDECIDABLE
    )
    plan = _plan(config)
    assert plan["_success"] is True
    step = _main_step(plan)
    assert step["planned_action"] == "error_process_validation"
    assert step["validation_error"]["error_code"] == "PROCESS_KIND_REQUIRED"
    assert _AUTHORITY_CODE not in json.dumps(plan)


def test_ambiguity_outranks_an_unavailable_process_kind():
    """Ambiguity needs only the DECLARED cardinality, so it stands even when a
    process's semantics are unavailable (ADR-001 §5, "Ambiguity vs the clean-plan
    gate"). This is the ordering choice the ADR leaves to #139."""
    config = _agreeing()
    second = copy.deepcopy(config["integration_spec"]["components"][0])
    second["key"] = "second_process"
    second["name"] = "Second Sentinel Process"
    config["integration_spec"]["components"].append(second)
    config["integration_spec"]["components"][0]["config"].pop("process_kind")
    plan = _plan(config)
    assert plan["_success"] is False
    assert plan["error_code"] == _AUTHORITY_CODE
    assert "multiple authored" in plan["error"]


def test_invalid_nested_pipeline_keeps_its_exact_legacy_code_on_the_strict_surface():
    """A reserved stage kind in the SUBMITTED config owns a real SYNC_PIPELINE_*
    error that must reach the caller unchanged — it is the clean-plan gate, not an
    authority conflict. This is why LEGACY_ADAPTER_PIPELINE_DRAFT_ONLY stays
    catalog-only: recoding these would break the #135 freeze pins."""
    config = _agreeing()
    config["integration_spec"]["components"][0]["config"]["pipeline"]["stages"][0][
        "kind"
    ] = "lookup"
    plan = _plan(config)
    assert plan["_success"] is True
    err = _main_step(plan)["validation_error"]
    assert err["error_code"] == "SYNC_PIPELINE_STAGE_UNSUPPORTED"
    assert err["field"] == "pipeline.stages[read_stage].kind"
    assert _AUTHORITY_CODE not in json.dumps(plan)


def test_unlowerable_authored_view_is_a_conflict_not_the_clean_plan_gate():
    """The mirror case: a schema-valid top-level view that cannot lower has no
    error of its own (it is inert on V1), and it can never equal the submitted
    process — so it is a disagreement."""
    config = _agreeing()
    config["integration_spec"]["pipeline"]["stages"][0]["kind"] = "lookup"
    plan = _plan(config)
    assert plan["_success"] is False
    assert plan["error_code"] == _AUTHORITY_CODE


# ---------------------------------------------------------------------------
# 5. Equality semantics
# ---------------------------------------------------------------------------


def test_expanded_top_level_defaults_do_not_manufacture_a_conflict():
    """The typed top-level dump expands EVERY default while the nested config
    dict stays byte-compact (#135 freeze test 6), so a naive `==` between the two
    always differs on identical semantics. Equality must run over the lowered
    normal form — this is the regression test for that trap."""
    config = _agreeing()
    spec = _normalize_to_spec(config)
    # The two surfaces really are serialized differently...
    assert spec.model_dump()["pipeline"] != config["integration_spec"]["components"][0][
        "config"
    ]["pipeline"]
    # ...yet they agree semantically.
    assert evaluate_pipeline_authority(spec).disposition == AGREE


_MAP_ID = "33333333-3333-3333-3333-333333333333"


def _mapped_spec(map_key, submitted_map_key="map_ref"):
    """read -> map -> send, with the map selector spelled `map_key`.

    `_lower_map_stage` resolves `map_ref or map_id`, so both spellings name one
    selector; the comparison must mirror that or two identical payloads conflict.

    ``submitted_map_key`` spells the SAME selector on the submitted block config.
    That side matters independently: the authored view is normalized by the real
    lowering (which already collapses map_id -> map_ref), so only the submitted
    side actually exercises the comparison's own fallback. `ProcessFlowBuilder`
    accepts `{"mode": "map_ref", "map_id": ...}`, so the spelling is reachable.
    """
    return IntegrationSpecV1(
        name="Sentinel Mapped",
        version=STRICT_VERSION,
        pipeline={
            "stages": [
                {
                    "key": "read_stage",
                    "kind": "read",
                    "config": {
                        "primitive": "db_read",
                        "connector_type": "database",
                        "action_type": "Get",
                        "connection_id": "conn-db",
                        "operation_id": "op-db",
                    },
                },
                {
                    "key": "map_stage",
                    "kind": "map",
                    "config": {"primitive": "map", map_key: _MAP_ID},
                },
                {
                    "key": "send_stage",
                    "kind": "send",
                    "config": {
                        "primitive": "rest_send",
                        "connector_type": "rest",
                        "action_type": "POST",
                        "connection_id": "conn-rest",
                        "operation_id": "op-rest",
                    },
                },
            ],
            "dependencies": [
                {"from_stage": "read_stage", "to_stage": "map_stage"},
                {"from_stage": "map_stage", "to_stage": "send_stage"},
            ],
        },
        components=[
            {
                "key": "main_process",
                "type": "process",
                "action": "create",
                "name": "Sentinel Mapped Process",
                "config": {
                    **copy.deepcopy(_LINEAR_DB_TO_API),
                    "transform": {"mode": "map_ref", submitted_map_key: _MAP_ID},
                },
            }
        ],
    )


@pytest.mark.parametrize("map_key", ["map_ref", "map_id"])
@pytest.mark.parametrize("submitted_map_key", ["map_ref", "map_id"])
def test_map_id_and_map_ref_are_one_selector(map_key, submitted_map_key):
    """All four spelling combinations name one selector and must agree."""
    from src.boomi_mcp.categories.components.builders import ProcessFlowBuilder

    spec = _mapped_spec(map_key, submitted_map_key)
    # Precondition: the submitted spelling really is accepted, so this is a live
    # equivalence rather than a hypothetical one.
    assert (
        ProcessFlowBuilder.validate_config(spec.components[0].config, depends_on=[])
        is None
    )
    assert evaluate_pipeline_authority(spec).disposition == AGREE


def test_a_different_map_selector_is_a_disagreement():
    spec = _mapped_spec("map_ref")
    spec.components[0].config["transform"]["map_ref"] = (
        "44444444-4444-4444-4444-444444444444"
    )
    assert evaluate_pipeline_authority(spec).disposition == DISAGREE


def test_passthrough_versus_mapped_is_a_disagreement():
    spec = _mapped_spec("map_ref")
    spec.components[0].config["transform"] = {"mode": "passthrough"}
    assert evaluate_pipeline_authority(spec).disposition == DISAGREE


def test_a_changed_connection_binding_is_a_disagreement():
    config = _agreeing()
    top = config["integration_spec"]["pipeline"]
    for stage in top["stages"]:
        if "connection_id" in stage["config"]:
            stage["config"]["connection_id"] = "different-connection-id"
            break
    else:
        pytest.skip("fixture pipeline binds no connection")
    assert (
        evaluate_pipeline_authority(_normalize_to_spec(config)).disposition == DISAGREE
    )


# ---------------------------------------------------------------------------
# 6. Cross-dialect representability
# ---------------------------------------------------------------------------


def test_a_wrapper_subprocess_can_never_be_summarized_by_a_linear_pipeline():
    """ADR-001 §5: the absence of a nested config.pipeline is NOT agreement. A
    valid wrapper (start -> calls -> stop) is categorically outside the singular
    linear view, so an authored pipeline describing one conflicts."""
    config = _agreeing()
    config["integration_spec"]["components"][0]["config"] = {
        "process_kind": "wrapper_subprocess",
        "process_calls": [{"process_id": "22222222-2222-2222-2222-222222222222"}],
    }
    plan = _plan(config)
    assert plan["_success"] is False
    assert plan["error_code"] == _AUTHORITY_CODE


_LINEAR_DB_TO_API = {
    "process_kind": "database_to_api_sync",
    "source": {
        "connector_type": "database",
        "action_type": "Get",
        "connection_id": "conn-db",
        "operation_id": "op-db",
    },
    "transform": {"mode": "passthrough"},
    "target": {
        "connector_type": "rest",
        "action_type": "POST",
        "connection_id": "conn-rest",
        "operation_id": "op-rest",
    },
}


def _db_to_api_spec(extra=None):
    """A spec whose single authored process is a plain linear database_to_api_sync,
    with a top-level pipeline that lowers to exactly the same core."""
    config = dict(copy.deepcopy(_LINEAR_DB_TO_API))
    if extra:
        config.update(copy.deepcopy(extra))
    pipeline = {
        "stages": [
            {
                "key": "read_stage",
                "kind": "read",
                "config": {
                    "primitive": "db_read",
                    "connector_type": "database",
                    "action_type": "Get",
                    "connection_id": "conn-db",
                    "operation_id": "op-db",
                },
            },
            {
                "key": "send_stage",
                "kind": "send",
                "config": {
                    "primitive": "rest_send",
                    "connector_type": "rest",
                    "action_type": "POST",
                    "connection_id": "conn-rest",
                    "operation_id": "op-rest",
                },
            },
        ],
        "dependencies": [{"from_stage": "read_stage", "to_stage": "send_stage"}],
    }
    return IntegrationSpecV1(
        name="Sentinel Linear",
        version=STRICT_VERSION,
        pipeline=pipeline,
        components=[
            {
                "key": "main_process",
                "type": "process",
                "action": "create",
                "name": "Sentinel Linear Process",
                "config": config,
            }
        ],
    )


def test_a_plain_linear_database_to_api_sync_agrees_with_its_pipeline_view():
    """The cross-dialect case ADR-001 §5 calls out: a process authored as block
    config (no nested config.pipeline at all) still has derivable submitted
    semantics, so an authored view that matches it AGREES."""
    assert evaluate_pipeline_authority(_db_to_api_spec()).disposition == AGREE


@pytest.mark.parametrize(
    "extra",
    [
        pytest.param({"catch": {"enabled": True}}, id="catch"),
        pytest.param({"notify": {"enabled": True, "message": "m"}}, id="notify"),
        pytest.param(
            {
                "reliability": {
                    "retry_count": 3,
                    "dlq": {
                        "mode": "error_subprocess_ref",
                        "process_id": "55555555-5555-5555-5555-555555555555",
                    },
                }
            },
            id="wired-reliability",
        ),
        pytest.param({"unknown_future_block": {"a": 1}}, id="unknown-future-block"),
    ],
)
def test_a_valid_but_richer_process_is_not_representable_and_conflicts(extra):
    """The normal form is the IMAGE of `lower_config`, so a process carrying
    anything lowering could never emit is categorically outside the singular
    linear view — a disagreement, never agreement-by-omission (ADR-001 §5).

    This is FAIL-CLOSED and load-bearing: every config above is ACCEPTED by
    `ProcessFlowBuilder.validate_config` (the base protocol tolerates unknown root
    keys), so without the containment check a shipped Try/Catch or DLQ path — or
    any feature block added after this slice — would silently read as agreement.
    """
    from src.boomi_mcp.categories.components.builders import ProcessFlowBuilder

    config = dict(copy.deepcopy(_LINEAR_DB_TO_API))
    config.update(copy.deepcopy(extra))
    # Precondition: the payload really is valid, so this is "valid but not
    # representable", not the clean-plan gate.
    assert ProcessFlowBuilder.validate_config(config, depends_on=[]) is None

    assert (
        evaluate_pipeline_authority(_db_to_api_spec(extra)).disposition
        == NOT_REPRESENTABLE
    )


@pytest.mark.parametrize("inert", ["description", "folder_name", "name"])
def test_inert_envelope_metadata_does_not_manufacture_a_conflict(inert):
    """Non-flow metadata that lowering carries through (or that the integration
    builder injects into the build payload) must not read as a feature."""
    assert (
        evaluate_pipeline_authority(_db_to_api_spec({inert: "Sentinel"})).disposition
        == AGREE
    )


def test_an_invalid_submitted_process_is_the_clean_plan_gate_not_a_conflict():
    """Contrast with the case above: an INVALID config owns a real error, so it is
    undecidable and that error must surface untouched."""
    from src.boomi_mcp.categories.components.builders import ProcessFlowBuilder

    broken = dict(copy.deepcopy(_LINEAR_DB_TO_API))
    broken["source"] = {**broken["source"], "dynamic_path": {"mode": "profile_field"}}
    assert ProcessFlowBuilder.validate_config(broken, depends_on=[]) is not None
    assert evaluate_pipeline_authority(_db_to_api_spec(broken)).disposition == UNDECIDABLE


# ---------------------------------------------------------------------------
# 7. View-faithfulness (ADR-001 §5) — the strict surface withholds, never rejects
# ---------------------------------------------------------------------------


def test_strict_reuse_withholds_the_view_but_still_accepts():
    """An authored view may only describe a process this request actually authors
    AND materializes. On collision-driven reuse the submitted config is discarded,
    so the view is withheld — and the bearer is NEVER rejected for it."""
    plan = _plan(_agreeing(), existing=[_collision_stub()])
    assert plan["_success"] is True
    assert _main_step(plan)["planned_action"] == "reuse"
    assert _echoed_pipeline(plan) is None


def test_v1_twin_of_the_reuse_case_still_echoes_the_view():
    """The same payload on V1 keeps its frozen inert echo (#135 pins this)."""
    plan = _plan(_agreeing("1.0"), existing=[_collision_stub()])
    assert plan["_success"] is True
    assert _main_step(plan)["planned_action"] == "reuse"
    assert _echoed_pipeline(plan) is not None


def test_a_materializing_strict_create_keeps_the_view():
    plan = _plan(_agreeing(), existing=[])
    assert _main_step(plan)["planned_action"] == "create"
    assert _echoed_pipeline(plan) is not None


def test_withholding_follows_the_apply_predicate_not_planned_action():
    """An explicit component_id skips candidate resolution, so the step keeps
    planned_action="create" at plan time while _apply_plan still reuses it.
    Reading planned_action alone would echo a view of a component the request
    never authored."""
    config = _agreeing()
    config["integration_spec"]["components"][0]["component_id"] = (
        "99999999-9999-9999-9999-999999999999"
    )
    plan = _plan(config)
    step = _main_step(plan)
    assert step["planned_action"] == "create"
    assert step["existing_component_id"]
    assert _echoed_pipeline(plan) is None


def test_withholding_does_not_apply_under_a_non_reuse_conflict_policy():
    config = _agreeing()
    config["conflict_policy"] = "clone"
    plan = _plan(config, existing=[_collision_stub()])
    assert plan["_success"] is True
    assert _echoed_pipeline(plan) is not None


# ---------------------------------------------------------------------------
# 8. Account independence, secrets, purity, and no-mutation-before-rejection
# ---------------------------------------------------------------------------


@pytest.mark.parametrize("existing", [[], [_collision_stub()]])
def test_rejection_never_depends_on_live_account_contents(existing):
    """ADR-001 §5 determinism note: both authority decisions are computed from the
    authored payload BEFORE collision resolution, so collision-reuse can never buy
    a self-contradictory payload an exemption."""
    plan = _plan(_contradictory(STRICT_VERSION), existing=existing)
    assert plan["_success"] is False
    assert plan["error_code"] == _AUTHORITY_CODE


def test_ambiguity_rejects_before_any_account_lookup():
    """Ambiguity is STRUCTURAL — ADR-001 §5 counts declared authoring actions, so
    it needs no process semantics and "stands even when a process's semantics are
    unavailable". It can therefore reject before collision resolution, with no
    live call at all."""
    config = _agreeing(STRICT_VERSION)
    second = copy.deepcopy(config["integration_spec"]["components"][0])
    second["key"] = "second_process"
    second["name"] = "Second Sentinel Process"
    config["integration_spec"]["components"].append(second)
    with patch(
        "src.boomi_mcp.categories.integration_builder.paginate_metadata"
    ) as mock_pag:
        mock_pag.return_value = []
        plan = _build_plan(MagicMock(), config)
    assert plan["_success"] is False
    assert plan["error_code"] == _AUTHORITY_CODE
    assert mock_pag.call_count == 0


def test_a_conflict_is_decided_early_but_reported_after_validation():
    """The conflict dispositions DO need the process's semantics, so they are
    subject to the clean-plan gate and are reported at the end of the plan.

    The decision itself is still computed from the authored payload before any
    lookup — only its reporting waits — which is why account contents still cannot
    move a payload across the accept/reject boundary (pinned separately by
    test_rejection_never_depends_on_live_account_contents).
    """
    plan = _plan(_contradictory(STRICT_VERSION))
    assert plan["_success"] is False
    assert plan["error_code"] == _AUTHORITY_CODE


def test_authority_errors_are_value_free():
    """ADR-001 §7: a stable code, the authored JSON path, a safe remediation hint,
    and no payload values — no component keys, names, ids, or config."""
    plan = _plan(_contradictory(STRICT_VERSION))
    blob = json.dumps(plan)
    case = _case("contradictory_pipelines")
    for leak in (
        "main_process",
        case["expected_lowered_source_connection_id"],
        case["expected_lowered_target_connection_id"],
    ):
        assert leak not in blob
    assert set(plan) == {"_success", "error_code", "error", "field", "hint"}


def test_evaluation_does_not_mutate_the_submitted_spec():
    config = _contradictory(STRICT_VERSION)
    spec = _normalize_to_spec(config)
    before = spec.model_dump()
    evaluate_pipeline_authority(spec)
    assert spec.model_dump() == before


def test_a_strict_conflict_blocks_apply_before_any_component_executes():
    """The rejection is a plan-level failure, so the apply path never reaches
    _execute_component — it cannot partial-apply."""
    from src.boomi_mcp.categories import integration_builder as ib

    config = _contradictory(STRICT_VERSION)
    config["dry_run"] = False
    with patch.object(ib, "paginate_metadata", return_value=[]), patch.object(
        ib, "_execute_component"
    ) as mock_exec:
        result = ib._apply_plan(MagicMock(), "renera", config)
    assert result["_success"] is False
    assert result["error_code"] == _AUTHORITY_CODE
    assert mock_exec.call_count == 0


def test_a_top_level_pipeline_secret_is_still_rejected_before_authority():
    """ADR-001 §11 takes precedence over the authority rule: the raw top-level
    pipeline secret scan (#139A) runs first, and its code — not the authority
    code — is what the caller sees."""
    config = _contradictory(STRICT_VERSION)
    config["integration_spec"]["pipeline"]["stages"][0]["config"][
        "password"
    ] = "SENTINEL-NOT-A-REAL-SECRET"
    plan = _plan(config)
    assert plan["_success"] is False
    assert plan["error_code"] == "PLAINTEXT_SECRET_REJECTED"
    assert "SENTINEL-NOT-A-REAL-SECRET" not in json.dumps(plan)


# ---------------------------------------------------------------------------
# 9. Publication — the strict surface must be discoverable
# ---------------------------------------------------------------------------


def test_schema_template_publishes_the_authority_record():
    from src.boomi_mcp.categories.meta_tools import get_schema_template_action

    result = get_schema_template_action(schema_name="IntegrationSpecV1")
    assert result["_success"] is True
    record = result["authority_versions"]
    assert record["default"] == "1.0"
    assert record["strict"] == "1.1"
    assert record["selector_path"] == "config.integration_spec.version"
    assert record["strict_input_form"] == "integration_spec"
    assert record["legacy_only_input_forms"] == ["source_description", "bare_top_level"]
    assert record["v1_deprecated"] is False
    version_schema = result["json_schema"]["properties"]["version"]
    assert version_schema["enum"] == ["1.0", "1.1"]
    assert version_schema["default"] == "1.0"


def test_list_capabilities_publishes_the_same_authority_record():
    from src.boomi_mcp.categories.meta_tools import (
        _AUTHORITY_VERSIONS,
        list_capabilities_action,
    )

    caps = list_capabilities_action()
    entry = caps["tools"]["build_integration"]
    assert entry["authority_versions"] == _AUTHORITY_VERSIONS
    assert any("1.1" in str(ex) for ex in entry["examples"])


def test_v1_is_published_as_not_deprecated():
    """ADR-001 §9: this slice deprecates nothing and warns about nothing."""
    from src.boomi_mcp.categories.meta_tools import _AUTHORITY_VERSIONS

    assert _AUTHORITY_VERSIONS["v1_deprecated"] is False


# ---------------------------------------------------------------------------
# 10. QA bug #169 — the not-representable rejection must not give impossible advice
# ---------------------------------------------------------------------------

_REF_BLOCK = {
    "process_kind": "database_to_api_sync",
    "source": {
        "connector_type": "database",
        "action_type": "Get",
        "connection_id": "$ref:db_conn",
        "operation_id": "$ref:db_op",
    },
    "transform": {"mode": "map_ref", "map_ref": "$ref:the_map"},
    "target": {
        "connector_type": "rest",
        "action_type": "POST",
        "connection_id": "$ref:rest_conn",
        "operation_id": "$ref:rest_op",
    },
}

_REF_VIEW = {
    "stages": [
        {
            "key": "s_read",
            "kind": "read",
            "config": {
                "primitive": "db_read",
                "connector_type": "database",
                "action_type": "Get",
                "connection_id": "$ref:db_conn",
                "operation_id": "$ref:db_op",
            },
        },
        {
            "key": "s_map",
            "kind": "map",
            "config": {"primitive": "map", "map_ref": "$ref:the_map"},
        },
        {
            "key": "s_send",
            "kind": "send",
            "config": {
                "primitive": "rest_send",
                "connector_type": "rest",
                "action_type": "POST",
                "connection_id": "$ref:rest_conn",
                "operation_id": "$ref:rest_op",
            },
        },
    ],
    "dependencies": [
        {"from_stage": "s_read", "to_stage": "s_map"},
        {"from_stage": "s_map", "to_stage": "s_send"},
    ],
}

_REF_DEPS = ["db_conn", "db_op", "the_map", "rest_conn", "rest_op"]

# Real component types, so `$ref` type-checking (PROCESS_REF_TYPE_MISMATCH) passes.
# Getting these wrong makes the process itself invalid, which the clean-plan gate
# then correctly reports INSTEAD of any authority verdict.
_REF_DEP_TYPES = {
    "db_conn": ("connector-settings", {"connector_type": "database"}),
    "db_op": ("connector-action", {"connector_type": "database", "action_type": "Get"}),
    "the_map": ("transform.map", {}),
    "rest_conn": ("connector-settings", {"connector_type": "rest"}),
    "rest_op": ("connector-action", {"connector_type": "rest", "action_type": "POST"}),
}


def _ref_spec(extra=None, depends_on=None):
    config = dict(copy.deepcopy(_REF_BLOCK))
    if extra:
        config.update(copy.deepcopy(extra))
    return IntegrationSpecV1(
        name="Sentinel Ref",
        version=STRICT_VERSION,
        pipeline=copy.deepcopy(_REF_VIEW),
        components=[
            {
                "key": "main_process",
                "type": "process",
                "action": "create",
                "name": "Sentinel Ref Process",
                "config": config,
                "depends_on": list(_REF_DEPS if depends_on is None else depends_on),
            },
            # The components those $ref tokens point at. Reference-only creates
            # carrying an explicit component_id, so they resolve without a live
            # lookup and are excluded from the declared-authoring count.
            *(
                {
                    "key": dep,
                    "type": _REF_DEP_TYPES[dep][0],
                    "action": "create",
                    "name": f"Sentinel {dep}",
                    "config": {
                        "reference_only": True,
                        "component_id": f"0000000{i}-0000-0000-0000-00000000000{i}",
                        **_REF_DEP_TYPES[dep][1],
                    },
                }
                for i, dep in enumerate(_REF_DEPS)
            ),
        ],
    )


def test_a_ref_bearing_block_config_agrees_with_its_linear_view():
    """QA bug #169's premise, reproduced faithfully. A realistic block process —
    `$ref:` bindings, declared depends_on, no nested config.pipeline — DOES agree
    with a top-level view that describes it. The comparison is over normalized
    semantics, not over a nested config.pipeline that only sync_pipeline has."""
    assert evaluate_pipeline_authority(_ref_spec()).disposition == AGREE


def test_undeclared_ref_dependencies_are_the_clean_plan_gate():
    """The other half of #169: without depends_on for its `$ref:` tokens the
    process does not validate, so there are no clean semantics to compare and
    MISSING_PROCESS_DEPENDENCY — not the authority code — owns the payload."""
    from src.boomi_mcp.categories.components.builders import ProcessFlowBuilder

    err = ProcessFlowBuilder.validate_config(copy.deepcopy(_REF_BLOCK), depends_on=[])
    assert err is not None and err.error_code == "MISSING_PROCESS_DEPENDENCY"
    assert (
        evaluate_pipeline_authority(_ref_spec(depends_on=[])).disposition == UNDECIDABLE
    )


def test_a_not_representable_process_never_gets_impossible_advice():
    """#169's real defect. A valid-but-unrepresentable process cannot be matched by
    ANY authored view, so telling the caller to "make it semantically identical"
    is unsatisfiable. That case must say so and point at the workaround."""
    spec = _ref_spec({"catch": {"enabled": True}})
    assert evaluate_pipeline_authority(spec).disposition == NOT_REPRESENTABLE

    config = {
        "dry_run": True,
        "integration_spec": json.loads(spec.model_dump_json()),
    }
    plan = _plan(config)
    assert plan["_success"] is False
    assert plan["error_code"] == _AUTHORITY_CODE
    assert "no representation as a singular linear pipeline view" in plan["error"]
    # The impossible instruction must NOT be given here...
    assert "make it semantically identical" not in plan["hint"]
    # ...and the actionable escape hatch must be.
    assert "sync_pipeline" in plan["hint"]


def test_a_genuine_mismatch_still_says_to_make_the_view_match():
    """The contrasting case: a representable process really can be matched, so the
    'make it identical' remediation stays correct there."""
    plan = _plan(_contradictory(STRICT_VERSION))
    assert plan["_success"] is False
    assert plan["error_code"] == _AUTHORITY_CODE
    assert "make it semantically identical" in plan["hint"]
    assert "no representation as a singular linear" not in plan["error"]


def test_every_authority_rejection_stays_value_free():
    """All three rejection shapes share the value-free envelope."""
    plans = [
        _plan(_contradictory(STRICT_VERSION)),
        _plan(
            {
                "dry_run": True,
                "integration_spec": json.loads(
                    _ref_spec({"catch": {"enabled": True}}).model_dump_json()
                ),
            }
        ),
    ]
    for plan in plans:
        assert plan["_success"] is False
        assert set(plan) == {"_success", "error_code", "error", "field", "hint"}
        blob = json.dumps(plan)
        for leak in ("main_process", "$ref:db_conn", "Sentinel Ref Process"):
            assert leak not in blob


# ---------------------------------------------------------------------------
# 11. QA bugs #170 / #171 — what "not representable" may and may not mean
# ---------------------------------------------------------------------------


@pytest.mark.parametrize(
    "view_stages",
    [
        pytest.param(["read", "branch", "send"], id="branch"),
        pytest.param(["read", "decision", "send"], id="decision"),
        pytest.param(["read", "flow_control", "send"], id="flow_control"),
        pytest.param(["read", "dataprocess", "send"], id="dataprocess"),
        pytest.param(["read", "map", "write"], id="write-target"),
        pytest.param(["listener", "map", "send"], id="listener-source"),
        pytest.param([], id="empty"),
    ],
)
def test_an_unlowerable_authored_view_is_an_ordinary_mismatch(view_stages):
    """QA bug #170. NOT_REPRESENTABLE is a property of the PROCESS only.

    When the process HAS a linear view and only the authored view fails to lower,
    blaming the process is false and "author it as sync_pipeline" is advice the
    caller has already followed. It is an ordinary mismatch — the view is wrong,
    and correcting it is an achievable remedy.
    """
    spec = _ref_spec()
    spec.pipeline = type(spec.pipeline)(
        stages=[
            {"key": f"s{i}", "kind": kind, "config": {}}
            for i, kind in enumerate(view_stages)
        ],
        dependencies=[
            {"from_stage": f"s{i}", "to_stage": f"s{i + 1}"}
            for i in range(max(0, len(view_stages) - 1))
        ],
    )
    assert evaluate_pipeline_authority(spec).disposition == DISAGREE


def test_an_unlowerable_view_gets_the_achievable_remedy_not_the_impossible_one():
    spec = _ref_spec()
    spec.pipeline = type(spec.pipeline)(
        stages=[
            {"key": "a", "kind": "read", "config": {}},
            {"key": "z", "kind": "branch", "config": {}},
        ],
        dependencies=[{"from_stage": "a", "to_stage": "z"}],
    )
    plan = _plan(
        {"dry_run": True, "integration_spec": json.loads(spec.model_dump_json())}
    )
    assert plan["_success"] is False
    assert plan["error_code"] == _AUTHORITY_CODE
    # It must NOT claim the process has no linear view — it demonstrably does.
    assert "no representation as a singular linear" not in plan["error"]
    assert "make it semantically identical" in plan["hint"]


_INERT_RELIABILITY = {"retry_count": 0, "dlq": {"mode": "disabled"}}


def test_an_inert_reliability_block_does_not_block_the_view():
    """QA bug #171. `{retry_count: 0, dlq: {mode: "disabled"}}` emits no Try/Catch
    and changes no emitted byte — it is the database_to_api_sync archetype's OWN
    default shape. Treating its mere presence as a feature would lock the flagship
    archetype's output out of the strict surface for no semantic reason.
    """
    from src.boomi_mcp.categories.components.builders.process_flow_builder import (
        _reliability_requests_try_catch,
    )

    # Precondition: the builder itself says this block emits no Try/Catch.
    assert _reliability_requests_try_catch(_INERT_RELIABILITY) is False
    assert (
        evaluate_pipeline_authority(
            _ref_spec({"reliability": copy.deepcopy(_INERT_RELIABILITY)})
        ).disposition
        == AGREE
    )


@pytest.mark.parametrize(
    "reliability",
    [
        pytest.param(
            {"retry_count": 3, "dlq": {"mode": "error_subprocess_ref", "process_id": "p"}},
            id="retry-with-dlq",
        ),
        pytest.param(
            {"retry_count": 0, "dlq": {"mode": "error_subprocess_ref", "process_id": "p"}},
            id="dlq-only",
        ),
        pytest.param(
            {"retry_count": 2, "dlq": {"mode": "document_cache_ref", "document_cache_id": "c"}},
            id="document-cache-dlq",
        ),
    ],
)
def test_a_wired_reliability_block_still_blocks_the_view(reliability):
    """The contrast that keeps #171's fix honest: a reliability block that DOES
    emit a Try/Catch is still categorically outside the singular linear view."""
    from src.boomi_mcp.categories.components.builders.process_flow_builder import (
        _reliability_requests_try_catch,
    )

    assert _reliability_requests_try_catch(reliability) is True
    spec = _ref_spec({"reliability": copy.deepcopy(reliability)})
    assert evaluate_pipeline_authority(spec).disposition == NOT_REPRESENTABLE


def test_the_inertness_exception_is_decided_by_the_builders_own_predicate():
    """It must never become a re-implemented rule here: the authority check and
    the builder must agree on what emits a Try/Catch, by construction."""
    from src.boomi_mcp.categories.components.builders.process_flow_builder import (
        _reliability_requests_try_catch,
    )

    for block in (
        _INERT_RELIABILITY,
        {"retry_count": 0},
        {"dlq": {"mode": "disabled"}},
        {"retry_count": 2, "dlq": {"mode": "document_cache_ref", "document_cache_id": "c"}},
    ):
        expected = (
            NOT_REPRESENTABLE
            if _reliability_requests_try_catch(block)
            else AGREE
        )
        spec = _ref_spec({"reliability": copy.deepcopy(block)})
        assert evaluate_pipeline_authority(spec).disposition == expected, block


def test_the_inertness_exception_is_scoped_to_emission_not_to_unknown_keys():
    """The precise property, pinned so the docs cannot overclaim it.

    Unknown keys are fail-closed at the TOP level of the config — a future sibling
    block is non-representable automatically. But INSIDE `reliability` the rule is
    "inert iff no Try/Catch is emitted", NOT "unknown keys fail closed": a
    reliability block carrying an unrecognized key emits nothing, so the view still
    describes the emitted XML faithfully and is accepted.

    A future catch-EMITTING reliability key must therefore be taught to
    `_reliability_requests_try_catch` — which is exactly why this check delegates
    to the emitter's own predicate instead of re-implementing one.
    """
    # Top level: fail-closed.
    assert (
        evaluate_pipeline_authority(
            _ref_spec({"some_future_block": {"enabled": True}})
        ).disposition
        == NOT_REPRESENTABLE
    )
    # Inside reliability: emission-scoped, so an unrecognized key is inert.
    for inert in ({"bogus_key": 1}, {"retry": {"enabled": True}}, {}):
        assert (
            evaluate_pipeline_authority(
                _ref_spec({"reliability": copy.deepcopy(inert)})
            ).disposition
            == AGREE
        ), inert


# ---------------------------------------------------------------------------
# 12. Codex review round 1 — alias bypass (P1) and emission-faithful casing (P2)
# ---------------------------------------------------------------------------


def test_the_process_type_alias_cannot_bypass_the_strict_surface():
    """P1. Every other layer resolves `process_kind or process_type` — the
    plan-time gate and each builder's validate_config/build. Reading only
    `process_kind` here let a caller opt in to the strict surface, author a
    contradictory view, spell the kind `process_type`, and fall through to
    UNDECIDABLE while the process still built: a silent bypass of the guarantee.
    """
    config = _agreeing(STRICT_VERSION)
    proc = config["integration_spec"]["components"][0]["config"]
    proc["process_type"] = proc.pop("process_kind")
    # Control: spelled this way the payload still agrees, so it is accepted...
    assert (
        evaluate_pipeline_authority(_normalize_to_spec(config)).disposition == AGREE
    )

    # ...and a CONTRADICTORY payload spelled the same way is still rejected.
    contradictory = _contradictory(STRICT_VERSION)
    cproc = contradictory["integration_spec"]["components"][0]["config"]
    cproc["process_type"] = cproc.pop("process_kind")
    plan = _plan(contradictory)
    assert plan["_success"] is False
    assert plan["error_code"] == _AUTHORITY_CODE


def test_the_alias_is_resolved_for_block_configs_too():
    spec = _ref_spec()
    spec.components[0].config["process_type"] = spec.components[0].config.pop(
        "process_kind"
    )
    assert evaluate_pipeline_authority(spec).disposition == AGREE


@pytest.mark.parametrize(
    "block,key,value,expected",
    [
        # The renderer UPPER-cases a REST verb, so these emit identical XML.
        pytest.param("target", "action_type", "post", AGREE, id="rest-verb-case"),
        # ...and LOWER-cases a non-REST connector type.
        pytest.param(
            "source", "connector_type", "Database", AGREE, id="db-connector-case"
        ),
    ],
)
def test_casing_follows_emission_not_raw_spelling(block, key, value, expected):
    """P2. Canonicalization is family-conditional: comparing raw stripped
    spellings manufactured false conflicts in two directions at once.

    Each row is anchored on what `ProcessFlowBuilder.build` actually emits, so the
    comparison cannot drift from emission.
    """
    from src.boomi_mcp.categories.components.builders import ProcessFlowBuilder

    baseline_config = copy.deepcopy(_LINEAR_DB_TO_API)
    variant_config = copy.deepcopy(_LINEAR_DB_TO_API)
    variant_config[block][key] = value

    baseline_xml = ProcessFlowBuilder.build(copy.deepcopy(baseline_config), name="X")
    variant_xml = ProcessFlowBuilder.build(copy.deepcopy(variant_config), name="X")
    # Precondition: this row's premise about emission is measured, not assumed.
    assert (baseline_xml == variant_xml) is (expected is AGREE)

    spec = _db_to_api_spec()
    spec.components[0].config[block][key] = value
    assert evaluate_pipeline_authority(spec).disposition == expected


def test_rest_connector_aliases_still_collapse():
    spec = _db_to_api_spec()
    spec.components[0].config["target"]["connector_type"] = "rest_client"
    assert evaluate_pipeline_authority(spec).disposition == AGREE


def test_database_verb_spelling_is_pinned_by_validation():
    """Why the comparison never sees a varying DATABASE verb.

    The canonicalizer preserves a non-REST verb (`Send` must not become `SEND` —
    that was #139C's latent defect). One might therefore expect `Get` vs `get` to
    be a reachable disagreement here. It is not: validation pins a DB source to
    exactly `Get`, so a differing spelling never produces two comparable configs —
    it is the clean-plan gate, and its own error surfaces.
    """
    from src.boomi_mcp.categories.components.builders import ProcessFlowBuilder

    for spelling in ("get", "GET"):
        config = copy.deepcopy(_LINEAR_DB_TO_API)
        config["source"]["action_type"] = spelling
        err = ProcessFlowBuilder.validate_config(config, depends_on=[])
        assert err is not None
        assert err.error_code == "PROCESS_CONNECTOR_BINDING_INVALID"

        spec = _db_to_api_spec()
        spec.components[0].config["source"]["action_type"] = spelling
        assert evaluate_pipeline_authority(spec).disposition == UNDECIDABLE


def test_the_comparison_inherits_the_family_conditional_rule():
    """The rule itself, pinned where it is defined.

    The comparison must not re-implement casing: it delegates to #139C's
    `_canonical_connector_metadata`, which is itself pinned against the legacy
    linear builder. REST upper-cases the verb; every other family preserves it and
    lower-cases the connector type. Folding all verbs would bless a real
    `Send`/`SEND` divergence; folding none manufactures false REST conflicts.
    """
    from src.boomi_mcp.compiler.process_ir.lowering import (
        _canonical_connector_metadata,
    )

    _, rest_action = _canonical_connector_metadata("source", "rest", "post")
    assert rest_action == "POST"

    db_connector, db_action = _canonical_connector_metadata(
        "target", "Database", "Send"
    )
    assert db_connector == "database"
    assert db_action == "Send"

    _, soap_action = _canonical_connector_metadata("source", "soap_client", "execute")
    assert soap_action == "execute"


# ---------------------------------------------------------------------------
# 13. Codex review round 2 — lower-time vs validate-time failures
# ---------------------------------------------------------------------------


def _sync_pipeline_spec(send_verb="POST", connection_id="conn-rest", depends_on=None):
    """A strict spec whose process is a sync_pipeline, with a DISAGREEING view.

    The view disagrees on purpose: only then can an authority conflict MASK the
    process's own validation error, which is exactly the precedence being pinned.
    """
    return IntegrationSpecV1(
        name="Sentinel Sync",
        version=STRICT_VERSION,
        pipeline={
            "stages": [
                {
                    "key": "other",
                    "kind": "read",
                    "config": {
                        "primitive": "db_read",
                        "connector_type": "database",
                        "action_type": "Get",
                        "connection_id": "a-different-connection",
                        "operation_id": "a-different-operation",
                    },
                }
            ],
            "dependencies": [],
        },
        components=[
            {
                "key": "main_process",
                "type": "process",
                "action": "create",
                "name": "Sentinel Sync Process",
                "depends_on": list(depends_on or []),
                "config": {
                    "process_kind": "sync_pipeline",
                    "pipeline": {
                        "stages": [
                            {
                                "key": "r",
                                "kind": "read",
                                "config": {
                                    "primitive": "db_read",
                                    "connector_type": "database",
                                    "action_type": "Get",
                                    "connection_id": "db-1",
                                    "operation_id": "op-1",
                                },
                            },
                            {
                                "key": "s",
                                "kind": "send",
                                "config": {
                                    "primitive": "rest_send",
                                    "connector_type": "rest",
                                    "action_type": send_verb,
                                    "connection_id": connection_id,
                                    "operation_id": "op-2",
                                },
                            },
                        ],
                        "dependencies": [{"from_stage": "r", "to_stage": "s"}],
                    },
                },
            }
        ],
    )


@pytest.mark.parametrize("kind_key", ["process_kind", "process_type"])
@pytest.mark.parametrize(
    "kwargs,expected_code",
    [
        pytest.param(
            {"connection_id": "$ref:rest_conn"},
            "MISSING_PROCESS_DEPENDENCY",
            id="undeclared-ref",
        ),
        pytest.param(
            {"send_verb": "FROBNICATE"},
            "PROCESS_CONNECTOR_BINDING_INVALID",
            id="unsupported-rest-verb",
        ),
    ],
)
def test_a_config_that_lowers_but_fails_validation_is_the_clean_plan_gate(
    kind_key, kwargs, expected_code
):
    """Lowering catches only STRUCTURAL defects. A sync_pipeline can lower cleanly
    and still be rejected afterwards, and comparing such a config would let an
    authority conflict MASK the actionable error the caller needs.

    Parametrized over both kind spellings: the alias must not be a second path
    with different precedence.
    """
    from src.boomi_mcp.categories.components.builders import (
        BuilderValidationError,
        SyncPipelineBuilder,
    )

    spec = _sync_pipeline_spec(**kwargs)
    config = spec.components[0].config
    if kind_key == "process_type":
        config["process_type"] = config.pop("process_kind")

    # Precondition: it really does lower, and really does fail validation.
    SyncPipelineBuilder.lower_config(copy.deepcopy(config))
    err = SyncPipelineBuilder.validate_config(
        copy.deepcopy(config), depends_on=spec.components[0].depends_on
    )
    assert err is not None and err.error_code == expected_code

    # The view disagrees, so without this precedence it would be reported as a
    # conflict instead of the validation error.
    assert evaluate_pipeline_authority(spec).disposition == UNDECIDABLE

    plan = _plan(
        {"dry_run": True, "integration_spec": json.loads(spec.model_dump_json())}
    )
    assert _AUTHORITY_CODE not in json.dumps(plan)


@pytest.mark.parametrize("kind_key", ["process_kind", "process_type"])
def test_a_valid_sync_pipeline_still_compares(kind_key):
    """The control: the precedence fix must not swallow VALID configs."""
    spec = _sync_pipeline_spec(connection_id="$ref:rest_conn", depends_on=["rest_conn"])
    config = spec.components[0].config
    if kind_key == "process_type":
        config["process_type"] = config.pop("process_kind")
    assert evaluate_pipeline_authority(spec).disposition == DISAGREE


# ---------------------------------------------------------------------------
# 14. QA bug #172 — the clean-plan gate closes the CLASS, not one pass
# ---------------------------------------------------------------------------


def _ref_typemismatch_spec():
    """A spec whose process passes `validate_config` but fails `$ref` TYPE-checking.

    `connection_id` points at the map component. That is a further validation pass
    (PROCESS_REF_TYPE_MISMATCH), after both lowering and `validate_config` — the
    third distinct pass in this class.
    """
    spec = _ref_spec()
    spec.components[0].config["source"]["connection_id"] = "$ref:the_map"
    return spec


@pytest.mark.parametrize("agreeing", [True, False], ids=["agreeing-view", "disagreeing-view"])
def test_a_ref_type_mismatch_outranks_the_authority_verdict(agreeing):
    """QA bug #172, and the reason this is fixed as a CLASS.

    Enumerating validation passes inside the comparison closed three instances of
    one bug in a row (lower-time, then validate_config-time, then $ref-type-time).
    The conflict disposition now YIELDS to whatever error the plan itself produced
    for the authored process, so a fourth pass is covered without being named.

    The agreeing case is the sharper one: there the caller would otherwise be told
    their view conflicts when the real defect is a broken $ref, and the hint's
    remedy would be actively wrong advice.
    """
    spec = _ref_typemismatch_spec()
    if not agreeing:
        spec.pipeline.stages[0].config["connection_id"] = "a-different-connection"

    plan = _plan(
        {"dry_run": True, "integration_spec": json.loads(spec.model_dump_json())}
    )
    assert plan["_success"] is True
    step = _main_step(plan)
    assert step["validation_error"]["error_code"] == "PROCESS_REF_TYPE_MISMATCH"
    assert _AUTHORITY_CODE not in json.dumps(plan)


def test_the_gate_is_scoped_to_the_authored_process_step():
    """An UNRELATED component's failure must not suppress a genuine conflict — the
    authored process's semantics are still perfectly comparable.

    Pinned on the predicate directly. An end-to-end version would be vacuous: in
    practice non-process components almost never carry a `validation_error`, so a
    "broken helper" fixture silently proves nothing (measured — four different
    malformed helpers all planned without one).
    """
    from src.boomi_mcp.categories.integration_builder import (
        _authored_step_has_validation_error,
    )

    clean = {"key": "main_process", "planned_action": "create"}
    broken_other = {
        "key": "other",
        "planned_action": "error_process_validation",
        "validation_error": {"error_code": "SOMETHING_ELSE"},
    }
    broken_authored = {
        "key": "main_process",
        "planned_action": "error_process_validation",
        "validation_error": {"error_code": "PROCESS_REF_TYPE_MISMATCH"},
    }

    # Another component's failure is not this process's failure.
    assert _authored_step_has_validation_error([clean, broken_other], "main_process") is False
    # The authored process's own failure is.
    assert _authored_step_has_validation_error([broken_authored, clean], "main_process") is True
    # An empty validation_error is not a failure (the plan writes {} for clean steps).
    assert (
        _authored_step_has_validation_error(
            [{"key": "main_process", "validation_error": {}}], "main_process"
        )
        is False
    )
    # No authored key (ambiguous/zero-authored) never suppresses anything.
    assert _authored_step_has_validation_error([broken_authored], None) is False
    # A missing step does not silently suppress the conflict either.
    assert _authored_step_has_validation_error([broken_other], "main_process") is False


def test_a_clean_authored_process_still_reports_the_conflict():
    """The control for the whole gate: yielding must not swallow real conflicts."""
    plan = _plan(_contradictory(STRICT_VERSION))
    assert plan["_success"] is False
    assert plan["error_code"] == _AUTHORITY_CODE
    assert not _main_step_or_none(plan)


def _main_step_or_none(plan):
    for step in plan.get("steps", []) or []:
        if step.get("key") == "main_process":
            return step.get("validation_error")
    return None

"""Issue #157 (M12.19) — the §6 architect review's plan-conformance findings.

One witness per finding the architect implementation review raised, each with
the adversarial half that fails if the correction is removed. They live in one
module because they share a provenance, not a subject: every case here exists
because the review found the plan's guarantee unmet, and the ledger rows
ARCH-157-e1-01..07 point at these tests.

Scenarios enter through the public planning / dispatcher entries, with only the
live metadata boundary faked.
"""

from __future__ import annotations

import copy
import sys
from pathlib import Path
from unittest.mock import MagicMock, patch

import pytest

_src = str(Path(__file__).resolve().parent.parent / "src")
if _src not in sys.path:
    sys.path.insert(0, _src)
sys.path.insert(0, str(Path(__file__).resolve().parent))

from boomi_mcp.authoring.workflow import (  # noqa: E402
    compile_authoring_request_v1,
    plan_authoring_request_v1,
)
from boomi_mcp.categories.components.builders.profile_generation import (  # noqa: E402
    MAP_PROFILE_INDEX_UNAVAILABLE,
)
from boomi_mcp.categories.components.builders.transform_map_validation import (  # noqa: E402
    TRANSFORM_REVIEW_REQUIRED_TARGET_UNMAPPED,
    destination_paths,
    normalized_map_destinations,
)
from boomi_mcp.errors import (  # noqa: E402
    GOVERNANCE_CONNECTION_BINDING_CONFLICT,
    GOVERNANCE_FLOWS_OUTPUT_ONLY,
    GOVERNANCE_WATERMARK_INCONSISTENT,
)
from boomi_mcp.models.authoring_workflow import AuthoringRequestV1  # noqa: E402
from boomi_mcp.models.integration_models import IntegrationComponentSpec  # noqa: E402

from test_issue_157_required_target_coverage import (  # noqa: E402
    _MAP_KEY,
    _TARGET_KEY,
    _bound_apply_payload,
    _components,
    _json_index,
    _request,
)

_PAGINATE = "boomi_mcp.categories.integration_builder.paginate_metadata"
_PROFILE = "issue-157"


@pytest.fixture(autouse=True)
def _offline():
    with patch(_PAGINATE, lambda *a, **k: []):
        yield


# ---------------------------------------------------------------------------
# item 1 — the selected artifact is resolved under the CALLER'S policy
# ---------------------------------------------------------------------------


def _policy_spy():
    """Record the ``conflict_policy`` each selected-artifact resolution was asked under."""
    from boomi_mcp.categories import integration_builder

    seen = []
    real = integration_builder._resolve_selected_profile_indexes

    def spy(client, spec, reused_keys=None, conflict_policy=None):
        seen.append(conflict_policy)
        return real(client, spec, reused_keys, conflict_policy)

    return seen, patch.object(
        integration_builder, "_resolve_selected_profile_indexes", spy
    )


@pytest.mark.parametrize("policy", ["reuse", "clone", "fail"])
def test_the_selected_artifact_is_resolved_under_the_requests_own_conflict_policy(policy):
    """The request's policy REACHES the resolver — every time it is asked.

    The resolver decides reuse-vs-create through ``_will_reuse_at_apply``, whose
    answer depends on the policy; asked without one it falls back to the default
    "reuse", so under ``clone`` a same-name profile was judged against the
    namesake it will CLONE rather than the profile being authored. Reading the
    policy off the normalized intent or the spec found nothing — neither model
    carries it — which is a fix that changes no behaviour at all.
    """
    request = _request(_components(missing_required=False))
    request = request.model_copy(
        update={"intent": request.intent.model_copy(update={"conflict_policy": policy})}
    )
    seen, spy = _policy_spy()
    with spy:
        plan_authoring_request_v1(request, boomi_client=MagicMock(), profile=_PROFILE)
    assert seen, "the resolver was never asked"
    assert set(seen) == {policy}, seen


def test_a_reused_map_is_opaque_to_the_coverage_gate():
    """A ``reference_only`` map's candidate mappings describe a map nobody writes."""
    comps = _components()  # the target profile needs `Root/must`, no mapping binds it
    gap, _ = plan_authoring_request_v1(_request(comps), boomi_client=MagicMock(), profile=_PROFILE)
    assert [d for d in gap.errors if TRANSFORM_REVIEW_REQUIRED_TARGET_UNMAPPED in d.cause_codes]

    reused = copy.deepcopy(comps)
    entry = next(c for c in reused if c["key"] == _MAP_KEY)
    entry["config"]["reference_only"] = True
    entry["component_id"] = "map-uuid-1"
    quiet, _ = plan_authoring_request_v1(_request(reused), boomi_client=MagicMock(), profile=_PROFILE)
    assert not [d for d in quiet.errors if TRANSFORM_REVIEW_REQUIRED_TARGET_UNMAPPED in d.cause_codes]


def _reused_target(missing_required=False):
    """The replay fixture with its target profile turned into a REUSED reference."""
    comps = _components(missing_required=missing_required)
    target = next(c for c in comps if c["key"] == _TARGET_KEY)
    root = copy.deepcopy(target["config"]["root"])
    target["component_id"] = "prof-uuid-1"
    target["config"] = {
        "reference_only": True,
        "component_name": "Existing Target",
        "profile_type": "json.generated",
        "root": root,
    }
    return comps, root


def _bound_wet(comps):
    """A wet apply payload bound to THIS request's own covering compile."""
    covering, _ = compile_authoring_request_v1(
        _request(comps), boomi_client=MagicMock(), profile=_PROFILE
    )
    payload = _request(comps).model_dump(mode="json")
    payload["expected_capability_revision"] = covering.revision_binding.capability_revision
    payload["expected_compile_hash"] = covering.revision_binding.compile_hash
    return {"authoring_request": payload, "dry_run": False}


def test_the_apply_refresh_runs_the_coverage_gate_before_any_write():
    """A leaf that becomes required BETWEEN the preflight and the refresh refuses.

    ``validate_transform_map`` alone does not carry required-leaf coverage, so a
    selected index re-read at apply could newly require a leaf and the gap would
    surface at the map STEP, after earlier components were written.

    The drift is placed where the code itself draws the line: the refresh is the
    re-read that happens AFTER the plan has been built and validated, so the stub
    answers with the changed profile only once ``_build_plan`` has returned.
    That is exactly "the account moved after this request was validated", and it
    does not depend on how many times anything was asked.
    """
    from boomi_mcp.categories import integration_builder

    comps, clean_root = _reused_target()
    drifted_root = copy.deepcopy(clean_root)
    drifted_root["children"].append(
        {"name": "must", "kind": "simple", "data_type": "character", "required": True}
    )

    planned = []
    real_build = integration_builder._build_plan

    def _build_then_drift(*args, **kwargs):
        out = real_build(*args, **kwargs)
        planned.append(1)
        return out

    def drifting(client, spec, reused_keys=None, conflict_policy=None):
        root = drifted_root if planned else clean_root
        return {_TARGET_KEY: _json_index(root)}

    clean_discovery = patch.object(
        integration_builder, "_discover_profile_index",
        lambda client, uuid: {"profile_component_type": "profile.json",
                              "field_index_by_path": _json_index(clean_root)},
    )
    with clean_discovery:
        payload = _bound_wet(comps)
    with clean_discovery, patch.object(
        integration_builder, "_build_plan", _build_then_drift
    ), patch.object(
        integration_builder, "_resolve_selected_profile_indexes", drifting
    ), patch.object(integration_builder, "_execute_component") as execute, patch.object(
        integration_builder, "create_component"
    ) as create:
        result = integration_builder.build_integration_action(MagicMock(), _PROFILE, "apply", payload)
    assert planned, "the plan was never built, so nothing reached the refresh"
    assert result.get("_success") is False
    assert result.get("error_code") == TRANSFORM_REVIEW_REQUIRED_TARGET_UNMAPPED, result
    assert execute.call_count == 0 and create.call_count == 0


def test_the_apply_refresh_stays_quiet_when_the_account_did_not_move():
    """The adversarial half: the identical run without drift reaches the writes."""
    from boomi_mcp.categories import integration_builder

    comps, clean_root = _reused_target()
    clean = {_TARGET_KEY: _json_index(clean_root)}
    clean_discovery = patch.object(
        integration_builder, "_discover_profile_index",
        lambda client, uuid: {"profile_component_type": "profile.json",
                              "field_index_by_path": clean[_TARGET_KEY]},
    )
    with clean_discovery:
        payload = _bound_wet(comps)
    with clean_discovery, patch.object(
        integration_builder, "_resolve_selected_profile_indexes",
        lambda client, spec, reused_keys=None, conflict_policy=None: dict(clean),
    ), patch.object(integration_builder, "_execute_component"):
        result = integration_builder.build_integration_action(MagicMock(), _PROFILE, "apply", payload)
    assert result.get("error_code") != TRANSFORM_REVIEW_REQUIRED_TARGET_UNMAPPED, result


# ---------------------------------------------------------------------------
# item 3 — a binding names one component, however it spells reuse
# ---------------------------------------------------------------------------


def _connection(**config):
    base = {"connector_type": "rest", "base_url": "https://x.invalid"}
    base.update(config)
    return {"key": "conn", "type": "connector-settings", "action": "create", "config": base}


def _binding_refusal(spec):
    from test_issue_157_governance import _op, _refusal, _unit

    return _refusal([_unit(name="P")], [spec, _op()])


def _binding_plan(spec):
    from test_issue_157_governance import _op, _plan, _unit

    return _plan([_unit(name="P")], [spec, _op()])


def test_a_creating_connection_naming_two_component_ids_is_refused():
    """``action="create"`` is not an exemption from naming ONE component.

    Under the reuse policy apply resolves a top-level ``component_id`` to a
    reuse, so this binding carried two identities and inline creation settings
    past a contract that only looked inside the ``reference_only`` branch.
    """
    spec = _connection(component_id="existing-B")
    spec["component_id"] = "existing-A"
    error = _binding_refusal(spec)
    assert error.code == GOVERNANCE_CONNECTION_BINDING_CONFLICT
    assert error.diagnostics[0].path == "/components/conn/config/component_id"


def test_a_creating_connection_naming_two_component_names_is_refused():
    spec = _connection(component_name="Other")
    spec["name"] = "One"
    error = _binding_refusal(spec)
    assert error.code == GOVERNANCE_CONNECTION_BINDING_CONFLICT
    assert error.diagnostics[0].path == "/components/conn/config/component_name"


def test_a_creating_connection_agreeing_with_itself_stays_legal():
    """The adversarial half: agreement is not a conflict, on the same branch."""
    spec = _connection(component_id="existing-A", component_name="One")
    spec["component_id"] = "existing-A"
    spec["name"] = "One"
    result, _ = _binding_plan(spec)
    assert all(d.code != GOVERNANCE_CONNECTION_BINDING_CONFLICT for d in result.errors)


# ---------------------------------------------------------------------------
# item 4 — the watermark source-field rule DEFERS, and the deferral is decided
# ---------------------------------------------------------------------------


def _reused_source_profile(key="src_prof"):
    return {
        "key": key,
        "type": "profile.db",
        "action": "create",
        "component_id": "src-prof-uuid",
        "config": {"reference_only": True, "component_name": "Existing Source"},
    }


def _watermark_request(field="updated_at", query_refs=(), key="src_prof"):
    from test_issue_157_governance import _conn, _op, _request as _gov_request, _unit

    op = _op()
    op["depends_on"] = ["conn", key]
    op["config"]["query_parameters"] = {"since": "x"}
    unit = _unit(
        name="P",
        depends_on=("conn", "op", key),
        watermark={
            "source_profile_ref": "$ref:{0}".format(key),
            "field": field,
            "kind": "timestamp",
            "query_parameter_refs": tuple(query_refs),
        },
    )
    return _gov_request([unit], [_conn(), op, _reused_source_profile(key)])


def _index(fields):
    return {name: {"mappable": True, "data_type": "character"} for name in fields}


def test_a_watermark_over_a_reused_source_profile_is_deferred_not_refused():
    """Offline the index is out of reach — unavailable is not wrong."""
    result, _ = plan_authoring_request_v1(
        _watermark_request(), boomi_client=None, profile=_PROFILE
    )
    assert all(d.code != GOVERNANCE_WATERMARK_INCONSISTENT for d in result.errors)
    assert [r.intent_id for r in result.recorded_intents] == ["watermark"]


def test_the_deferred_watermark_is_decided_against_the_selected_profile():
    """Both directions, through the public plan entry."""
    from boomi_mcp.categories import integration_builder

    def _discovered(index):
        return {"profile_component_type": "profile.db", "field_index_by_path": index}

    with patch.object(
        integration_builder, "_discover_profile_index",
        lambda client, uuid: _discovered(_index(("updated_at", "id"))),
    ):
        ok, _ = plan_authoring_request_v1(
            _watermark_request(), boomi_client=MagicMock(), profile=_PROFILE
        )
    assert all(d.code != GOVERNANCE_WATERMARK_INCONSISTENT for d in ok.errors)

    with patch.object(
        integration_builder, "_discover_profile_index",
        lambda client, uuid: _discovered(_index(("id",))),
    ):
        bad, _ = plan_authoring_request_v1(
            _watermark_request(), boomi_client=MagicMock(), profile=_PROFILE
        )
    hits = [d for d in bad.errors if d.code == GOVERNANCE_WATERMARK_INCONSISTENT]
    assert hits and hits[0].path.endswith("/watermark/field")


def test_the_query_parameter_rule_still_runs_when_the_field_rule_defers():
    """Rule (b) needs no profile index, so a deferral of rule (a) never retires it.

    Returning early on an unavailable index silently dropped this rule for every
    watermark over a reused source profile — a gate weakened by a deferral that
    was never about it.
    """
    with pytest.raises(Exception) as excinfo:
        plan_authoring_request_v1(
            _watermark_request(query_refs=("nope",)), boomi_client=None, profile=_PROFILE
        )
    assert GOVERNANCE_WATERMARK_INCONSISTENT in str(excinfo.value)
    ok, _ = plan_authoring_request_v1(
        _watermark_request(query_refs=("since",)), boomi_client=None, profile=_PROFILE
    )
    assert all(d.code != GOVERNANCE_WATERMARK_INCONSISTENT for d in ok.errors)


def test_the_online_refusal_names_the_unit_governance_assigned_not_the_first():
    """The served path carries governance's unit index, which is not a position here.

    ``integration_spec.processes`` is sorted by component key on this route, so
    an index recomputed from it names a different unit. The deferral record
    carries the index the offline pass assigned, and the online refusal reports
    on that same path.
    """
    from boomi_mcp.categories import integration_builder
    from test_issue_157_governance import _conn, _op, _request as _gov_request, _unit

    # authored order: `zz_root` first, `aa_root` second — the spec sorts them the
    # other way round, so a positional index would name unit 0 for the watermark
    # governance recorded as unit 1.
    plain = _unit(key="zz_root", name="Z")
    op = _op()
    op["depends_on"] = ["conn", "src_prof"]
    watermarked = _unit(
        key="aa_root", name="A", conn="conn", op="op", depends_on=("conn", "op", "src_prof"),
        watermark={"source_profile_ref": "$ref:src_prof", "field": "missing", "kind": "timestamp"},
    )
    request = _gov_request([plain, watermarked], [_conn(), op, _reused_source_profile()])
    with patch.object(
        integration_builder, "_discover_profile_index",
        lambda client, uuid: {"profile_component_type": "profile.db", "field_index_by_path": _index(("id",))},
    ):
        result, _ = plan_authoring_request_v1(request, boomi_client=MagicMock(), profile=_PROFILE)
    hits = [d for d in result.errors if d.code == GOVERNANCE_WATERMARK_INCONSISTENT]
    assert hits and hits[0].path == "/units/1/envelope/watermark/field", [d.path for d in hits]
    assert [u.envelope.component_key for u in request.intent.units] == ["zz_root", "aa_root"]


# ---------------------------------------------------------------------------
# item 5 — `flows` is refused BY NAME wherever a caller writes it
# ---------------------------------------------------------------------------


def _typed_payload():
    from _m12_11_support import APPLIABLE_CONN, APPLIABLE_IR_DOC, APPLIABLE_OP

    return {
        "contract_version": "2",
        "intent": {
            "intent_kind": "process_ir",
            "integration_name": "x",
            "units": [
                {
                    "envelope": {"component_key": "p", "action": "create", "name": "n"},
                    "process_ir": APPLIABLE_IR_DOC,
                }
            ],
            "components": [APPLIABLE_CONN, APPLIABLE_OP],
        },
    }


@pytest.mark.parametrize("where", ["config", "authoring_request"])
@pytest.mark.parametrize("action", ["plan", "compile", "apply"])
def test_caller_flows_beside_a_valid_typed_request_are_refused_by_name(where, action):
    """Silently ignored at the config root, generic INVALID_INPUT on the envelope.

    One authored key answered three different ways depending on where it landed;
    the plan asks for the named refusal across every competing location.
    """
    from boomi_mcp.categories.integration_builder import build_integration_action

    config = {"authoring_request": _typed_payload()}
    if where == "config":
        config["flows"] = [{"key": "k"}]
    else:
        config["authoring_request"]["flows"] = [{"key": "k"}]
    result = build_integration_action(MagicMock(), _PROFILE, action, config)
    assert result.get("_success") is False
    assert result.get("error_code") == GOVERNANCE_FLOWS_OUTPUT_ONLY
    assert result["validation_errors"][0]["path"] == "{0}.flows".format(where)


@pytest.mark.parametrize("action", ["plan", "compile", "apply"])
def test_the_same_typed_request_without_flows_is_not_refused_for_it(action):
    """The adversarial half: nothing else on this request trips the named refusal."""
    from boomi_mcp.categories.integration_builder import build_integration_action

    result = build_integration_action(
        MagicMock(), _PROFILE, action, {"authoring_request": _typed_payload(), "dry_run": True}
    )
    assert result.get("error_code") != GOVERNANCE_FLOWS_OUTPUT_ONLY


# ---------------------------------------------------------------------------
# item 7 — ONE reading of what a map writes to
# ---------------------------------------------------------------------------


_MAP_CONFIG = {
    "map_type": "direct",
    "field_mappings": [{"source_path": "a", "target_path": "Root/id"}],
    "function_mappings": [{"function_type": "f", "inputs": ["a"], "target_path": "Root/fn"}],
    "script_mappings": [{"inputs": [], "outputs": [{"target_path": "Root/scr"}]}],
}


def test_the_coverage_gate_and_the_advisory_review_share_one_destination_reading():
    """A mutation of the shared reading moves BOTH consumers, or one is a copy."""
    from boomi_mcp.categories import transformation_review
    from boomi_mcp.categories.components.builders import transform_map_validation as tmv

    def _records():
        return transformation_review._mappings_from_map_config(_MAP_CONFIG)

    before = [tuple(r["target_paths"]) for r in _records()]
    assert before == [("Root/id",), ("Root/fn",), ("Root/scr",)]
    assert normalized_map_destinations(_MAP_CONFIG) == (
        ("direct", "Root/id"), ("map_function", "Root/fn"), ("map_script", "Root/scr"),
    )

    # remove the reading; both consumers must go blind together
    with patch.object(tmv, "destination_paths", lambda route, entry: ()), patch.object(
        transformation_review, "destination_paths", lambda route, entry: ()
    ):
        assert normalized_map_destinations(_MAP_CONFIG) == ()
        assert [tuple(r["target_paths"]) for r in _records()] == [(), (), ()]


@pytest.mark.parametrize(
    "config",
    [
        {"field_mappings": True},
        {"function_mappings": True},
        {"script_mappings": True},
        {"script_mappings": [{"outputs": True}]},
        {"field_mappings": {"target_path": "Root/id"}},
    ],
)
def test_a_truthy_non_list_mapping_list_is_empty_not_a_crash(config):
    """``value or ()`` let a TypeError escape the hard gate on every route."""
    assert normalized_map_destinations(config) == ()


def test_the_shared_reading_is_route_closed():
    """An unknown route binds nothing rather than guessing a spelling."""
    assert destination_paths("map_script", {"outputs": [{"target_path": " x "}]}) == ("x",)
    assert destination_paths("no_such_route", {"target_path": "Root/id"}) == ()


# ---------------------------------------------------------------------------
# item 2 — the projection describes the SELECTED artifact and keeps its content
# ---------------------------------------------------------------------------


_TGT_ROOT = {
    "name": "Root",
    "kind": "object",
    "children": [{"name": "candidate_only", "kind": "simple", "data_type": "character"}],
}
_SELECTED_ROOT = {
    "name": "Root",
    "kind": "object",
    "children": [{"name": "platform_only", "kind": "simple", "data_type": "character"}],
}


def _flow_components(reference_only):
    from boomi_mcp.models.integration_models import IntegrationComponentSpec

    target_config = {"format": "json", "root": copy.deepcopy(_TGT_ROOT)}
    if reference_only:
        target_config["reference_only"] = True
        target_config["component_name"] = "Existing Target"
    raw = [
        {"key": "dbc", "type": "connector-settings", "action": "create", "name": "DB",
         "config": {"connector_type": "database", "reference_only": True, "component_name": "Existing DB"}},
        {"key": "dbo", "type": "connector-action", "action": "create", "name": "Get",
         "depends_on": ["dbc", "src"],
         "config": {"connector_type": "database", "operation_mode": "get",
                    "connection_ref_key": "dbc", "read_profile_id": "$ref:src"}},
        {"key": "src", "type": "profile.db", "action": "create", "name": "Src",
         "config": {"profile_type": "database.read",
                    "output_fields": [{"name": "id", "data_type": "character", "mandatory": True}]}},
        {"key": "tgt", "type": "profile.json", "action": "create", "name": "Tgt", "config": target_config},
        {"key": "map", "type": "transform.map", "action": "create", "name": "Map",
         "depends_on": ["src", "tgt"],
         "config": {"map_type": "direct", "source_profile_id": "$ref:src",
                    "target_profile_id": "$ref:tgt",
                    "field_mappings": [{"source_path": "id", "target_path": "Root/candidate_only"}]}},
    ]
    return [IntegrationComponentSpec(**c) for c in raw]


def _flow_unit():
    from boomi_mcp.models.process_component import (
        ProcessAuthoringUnitAuthoredV1,
        ProcessComponentEnvelopeAuthoredV1,
    )
    from boomi_mcp.models.process_ir import parse_process_ir_v1

    return ProcessAuthoringUnitAuthoredV1(
        envelope=ProcessComponentEnvelopeAuthoredV1(
            component_key="proc", name="P", action="create", depends_on=("dbc", "dbo", "map")
        ),
        process_ir=parse_process_ir_v1(
            {"version": "1", "body": {"kind": "sequence", "steps": [
                {"kind": "source", "connection_ref": "$ref:dbc", "operation_ref": "$ref:dbo"},
                {"kind": "map_ref", "map_ref": "$ref:map", "label": "Generated"},
                {"kind": "return_documents"}]}}
        ),
    )


def test_the_served_flow_row_describes_the_selected_profile_not_the_candidate():
    """A reused profile's candidate config is not evidence of what it contains."""
    from boomi_mcp.authoring.derived_flows import derive_transform_flows

    components = _flow_components(reference_only=True)
    metadata = {"dbc": ("database", None)}

    (candidate_row,) = derive_transform_flows([_flow_unit()], components, connector_metadata=metadata)
    assert candidate_row.target_profile_generation.mappable_paths == ("Root/candidate_only",)

    selected = {"tgt": {"profile_component_type": "profile.json",
                        "field_index_by_path": _json_index(_SELECTED_ROOT)}}
    (selected_row,) = derive_transform_flows(
        [_flow_unit()], components, connector_metadata=metadata, selected_artifacts=selected
    )
    summary = selected_row.target_profile_generation
    assert summary.mappable_paths == ("Root/platform_only",)
    assert set(summary.field_index_by_path) == set(_json_index(_SELECTED_ROOT))
    # and it says so: the row names the artifact as its evidence and carries NO
    # generation body, because nothing generated this component
    assert summary.evidence_source == "selected_artifact"
    assert summary.generation_mode is None and summary.profile_config is None
    assert summary.component_type == "profile.json"
    # the candidate's own shape appears nowhere in the served summary
    assert "candidate_only" not in repr(summary.model_dump(mode="json"))


def test_a_profile_the_request_will_write_is_still_described_by_its_generator():
    """The adversarial half: a selected index for an unrelated key changes nothing."""
    from boomi_mcp.authoring.derived_flows import derive_transform_flows

    components = _flow_components(reference_only=False)
    (row,) = derive_transform_flows(
        [_flow_unit()], components, connector_metadata={"dbc": ("database", None)},
        selected_artifacts={"some_other_key": {"profile_component_type": "profile.json",
                                              "field_index_by_path": _json_index(_SELECTED_ROOT)}},
    )
    assert row.target_profile_generation.mappable_paths == ("Root/candidate_only",)


def test_the_name_drop_is_a_comparison_rule_and_the_served_row_keeps_the_name():
    """R3 reconciles two legacy producers with each other; it is not a content rule."""
    from boomi_mcp.authoring.derived_flows import normalize_flow_row

    row = {
        "key": "k",
        "source_profile_generation": {"component_name": "Src", "mappable_paths": ["a"]},
        "target_profile_generation": {"component_name": "Tgt", "mappable_paths": ["b"]},
    }
    compared = normalize_flow_row(row)
    served = normalize_flow_row(row, for_comparison=False)
    for side in ("source_profile_generation", "target_profile_generation"):
        assert "component_name" not in compared[side]
        assert served[side]["component_name"] == row[side]["component_name"]
        assert compared[side]["mappable_paths"] == served[side]["mappable_paths"]


def test_the_legacy_parse_preserves_the_profile_name_into_the_typed_row():
    """``typed_row_from_legacy`` is a SERVED path, so it uses the served rule.

    Round-tripped through a real derived row rather than a hand-built one: a
    fixture the code cannot produce guards nothing.
    """
    from boomi_mcp.authoring.derived_flows import (
        derive_transform_flows,
        normalize_flow_row,
        typed_row_from_legacy,
    )

    (row,) = derive_transform_flows(
        [_flow_unit()], _flow_components(reference_only=False),
        connector_metadata={"dbc": ("database", None)},
    )
    payload = row.model_dump(mode="json")
    name = payload["target_profile_generation"]["component_name"]
    assert name, payload["target_profile_generation"]
    assert typed_row_from_legacy(payload).target_profile_generation.component_name == name
    # and the comparison rule still drops it, which is the only thing R3 is for
    assert "component_name" not in normalize_flow_row(payload)["target_profile_generation"]


# ---------------------------------------------------------------------------
# Stage-2 round 5 — defects the commit review found in the corrections above
# ---------------------------------------------------------------------------


_ISSUE_95_JSON = (
    Path(__file__).resolve().parent / "fixtures" / "profile_components" / "issue_95" / "profile_json.xml"
)


def _live_index():
    """A field index in the shape LIVE DISCOVERY produces, not the generator's.

    Provenance: `tests/fixtures/profile_components/issue_95/profile_json.xml` is
    an exported platform profile component, frozen long before this slice's
    baseline — causally independent of the code under test. Every witness above
    used a generator-built index, which is exactly why none of them saw that the
    two shapes differ.
    """
    from boomi_mcp.categories.components.builders.profile_generation import (
        index_existing_profile_xml,
    )

    return index_existing_profile_xml(_ISSUE_95_JSON.read_text(encoding="utf-8"))


def test_the_live_index_shape_really_is_a_superset_of_the_served_one():
    """The control for the two tests below: if these agreed, they would prove nothing."""
    from boomi_mcp.models.derived_flows import FieldIndexEntryV1

    entries = _live_index()["field_index_by_path"]
    assert entries
    extra = set().union(*(set(e) for e in entries.values())) - set(FieldIndexEntryV1.model_fields)
    assert extra == {"key", "key_path", "name_path", "is_mappable", "structural"}, extra


def test_a_selected_live_index_is_projected_onto_the_served_entry_schema():
    """`FieldIndexEntryV1` forbids extras, so a raw live entry cannot be served."""
    from boomi_mcp.authoring.derived_flows import derive_transform_flows
    from boomi_mcp.models.derived_flows import FieldIndexEntryV1

    live = _live_index()["field_index_by_path"]
    (row,) = derive_transform_flows(
        [_flow_unit()], _flow_components(reference_only=True),
        connector_metadata={"dbc": ("database", None)},
        selected_artifacts={"tgt": {"profile_component_type": "profile.json", "field_index_by_path": live}},
    )
    served = row.target_profile_generation.field_index_by_path
    assert set(served) == set(live)
    for path, entry in served.items():
        assert isinstance(entry, FieldIndexEntryV1)
        assert entry.mappable == live[path]["mappable"]
    assert row.target_profile_generation.mappable_paths == tuple(
        sorted(p for p, e in live.items() if e.get("mappable", True))
    )


def test_a_plan_over_a_live_discovered_reused_profile_still_validates():
    """End to end: the served preview is built, not refused by its own model."""
    from boomi_mcp.categories import integration_builder

    comps, _root = _reused_target()
    live = _live_index()
    with patch.object(
        integration_builder, "_discover_profile_index",
        lambda client, uuid: {"profile_component_type": live["profile_component_type"],
                              "field_index_by_path": live["field_index_by_path"]},
    ):
        result, _ = plan_authoring_request_v1(
            _request(comps), boomi_client=MagicMock(), profile=_PROFILE
        )
    assert result.integration_spec_preview is not None


def test_a_reused_profile_with_no_local_body_is_described_by_the_artifact_alone():
    """No generator artifact is no obstacle once the summary names its source.

    The earlier answer served nothing at all here, because the model demanded a
    generation body for every summary. It demands one only of a summary that
    CLAIMS a generator now, so the account's own index can be served on its own.
    """
    from boomi_mcp.authoring.derived_flows import derive_transform_flows
    from boomi_mcp.models.integration_models import IntegrationComponentSpec

    components = _flow_components(reference_only=True)
    bare = [
        c if c.key != "tgt" else IntegrationComponentSpec(
            key="tgt", type="profile.json", action="create", name="Tgt",
            component_id="prof-uuid-1",
            config={"reference_only": True, "component_name": "Existing Target"},
        )
        for c in components
    ]
    live = _live_index()["field_index_by_path"]
    (row,) = derive_transform_flows(
        [_flow_unit()], bare, connector_metadata={"dbc": ("database", None)},
        selected_artifacts={"tgt": {"profile_component_type": "profile.json", "field_index_by_path": live}},
    )
    assert row.target_profile_generation.evidence_source == "selected_artifact"
    assert set(row.target_profile_generation.field_index_by_path) == set(live)
    # and NOT the request's own name for it: the account supplied the shape, and
    # nothing verified the caller's label against the artifact (live QA r13)
    assert row.target_profile_generation.component_name is None
    # the source, which this request DOES write, keeps its generator body
    assert row.source_profile_generation.evidence_source == "generator"
    assert row.source_profile_generation.profile_config is not None


def test_a_reused_profile_whose_artifact_could_not_be_read_serves_no_summary():
    """Reused and unreadable is not "fall back to the candidate"."""
    from boomi_mcp.authoring.derived_flows import derive_transform_flows

    (row,) = derive_transform_flows(
        [_flow_unit()], _flow_components(reference_only=True),
        connector_metadata={"dbc": ("database", None)},
        selected_artifacts={"tgt": None},
    )
    assert row.target_profile_generation is None


def _map_component(action, **extra):
    comps = copy.deepcopy(_components())  # target profile needs `Root/must`
    entry = next(c for c in comps if c["key"] == _MAP_KEY)
    entry["action"] = action
    entry["config"].update(extra)
    return comps


@pytest.mark.parametrize(
    "action,component_id,opaque",
    [
        ("create", "map-uuid-1", True),    # a reference-only create IS reused
        ("update", "map-uuid-1", False),   # an update is WRITTEN, so it is judged
        ("create", None, False),           # no bound component: judged, the safe direction
    ],
)
def test_the_map_exemption_asks_the_reuse_predicate_not_the_flag(action, component_id, opaque):
    """`reference_only` beside `action="update"` is an update, not a reuse.

    Keying the exemption on the flag let a map update leave a required target
    leaf unbound and still compile.
    """
    comps = _map_component(action, reference_only=True)
    entry = next(c for c in comps if c["key"] == _MAP_KEY)
    if component_id:
        entry["component_id"] = component_id
    else:
        entry.pop("component_id", None)
    result, _ = plan_authoring_request_v1(_request(comps), boomi_client=MagicMock(), profile=_PROFILE)
    hits = [d for d in result.errors if TRANSFORM_REVIEW_REQUIRED_TARGET_UNMAPPED in d.cause_codes]
    assert (not hits) is opaque, [d.message for d in hits]


def _watermark_over(ref, components):
    from test_issue_157_governance import _conn, _op, _request as _gov_request, _unit

    op = _op()
    op["depends_on"] = ["conn"]
    unit = _unit(
        name="P", depends_on=("conn", "op"),
        watermark={"source_profile_ref": ref, "field": "updated_at", "kind": "timestamp"},
    )
    return _gov_request([unit], [_conn(), op] + list(components))


@pytest.mark.parametrize("ref", ["$ref:not_a_component", "$ref:conn"])
def test_a_watermark_naming_no_profile_is_still_refused(ref):
    """A deferral is for an unavailable index, never for an unrepairable reference.

    Neither a missing component nor a connector can acquire a selected-profile
    index later, so reading their `None` as "ask again online" retired the
    reference check.
    """
    with pytest.raises(Exception) as excinfo:
        plan_authoring_request_v1(_watermark_over(ref, []), boomi_client=None, profile=_PROFILE)
    assert excinfo.value.code == GOVERNANCE_WATERMARK_INCONSISTENT
    assert [d.path for d in excinfo.value.diagnostics] == [
        "/units/0/envelope/watermark/source_profile_ref"
    ]


def test_an_authored_profile_whose_own_config_yields_no_index_is_still_refused():
    """Only a REUSED profile defers: this one the request writes, so it is decided.

    Reported rather than raised: normalization cannot tell a name-reuse from a
    write, so it defers, and the pass that CAN tell reports the refusal the way
    planning reports everything else — as a diagnostic, with compile raising on
    it like any other error.
    """
    empty = {"key": "src_prof", "type": "profile.db", "action": "create",
             "config": {"profile_type": "database.read"}}
    request = _watermark_over("$ref:src_prof", [empty])
    result, _ = plan_authoring_request_v1(request, boomi_client=None, profile=_PROFILE)
    hits = [d for d in result.errors if d.code == GOVERNANCE_WATERMARK_INCONSISTENT]
    assert hits and hits[0].path.endswith("/watermark/field")
    with pytest.raises(Exception) as excinfo:
        compile_authoring_request_v1(request, boomi_client=None, profile=_PROFILE)
    assert GOVERNANCE_WATERMARK_INCONSISTENT in {
        d.code for d in excinfo.value.diagnostics
    }


def test_the_reused_source_profile_still_defers():
    """The adversarial half: the case the deferral exists for is untouched."""
    result, _ = plan_authoring_request_v1(
        _watermark_over("$ref:src_prof", [_reused_source_profile()]),
        boomi_client=None, profile=_PROFILE,
    )
    assert all(d.code != GOVERNANCE_WATERMARK_INCONSISTENT for d in result.errors)


# ---------------------------------------------------------------------------
# Stage-2 round 6 — one answer to "will apply reuse this?", asked once
# ---------------------------------------------------------------------------


def _account_holding(*names):
    """Patch the metadata boundary so these component NAMES resolve to one match each."""
    rows = [
        {"component_id": "existing-{0}".format(i), "name": name, "type": "t", "folder_name": "Home"}
        for i, name in enumerate(names, 1)
    ]
    return patch(_PAGINATE, lambda *a, **k: list(rows))


def _map_named(name, **config):
    comps = copy.deepcopy(_components())  # the target profile needs `Root/must`
    entry = next(c for c in comps if c["key"] == _MAP_KEY)
    entry["name"] = name
    entry["config"].update(config)
    return comps


def test_a_map_bound_by_name_is_opaque_even_with_no_declared_id():
    """A `reference_only` entry binds through an unambiguous NAME match too.

    Reading only a declared id called that map "written", so its candidate
    mappings were judged and a valid request was refused.
    """
    comps = _map_named("Reused Map", reference_only=True, component_name="Reused Map")
    with _account_holding("Reused Map"):
        result, _ = plan_authoring_request_v1(_request(comps), boomi_client=MagicMock(), profile=_PROFILE)
    assert not [d for d in result.errors if TRANSFORM_REVIEW_REQUIRED_TARGET_UNMAPPED in d.cause_codes]
    # and with nothing in the account to bind to, the same request IS judged
    with patch(_PAGINATE, lambda *a, **k: []):
        alone, _ = plan_authoring_request_v1(_request(comps), boomi_client=MagicMock(), profile=_PROFILE)
    assert [d for d in alone.errors if TRANSFORM_REVIEW_REQUIRED_TARGET_UNMAPPED in d.cause_codes]


def test_a_config_only_component_id_without_the_reuse_flag_binds_nothing():
    """The planner reads the config-level id ONLY for a `reference_only` entry.

    Treating it as a binding everywhere made a map that apply CREATES look
    reused, so its missing required target leaf went unreported.
    """
    comps = _map_named("Fresh Map", component_id="existing-elsewhere")
    with patch(_PAGINATE, lambda *a, **k: []):
        result, _ = plan_authoring_request_v1(_request(comps), boomi_client=MagicMock(), profile=_PROFILE)
    assert [d for d in result.errors if TRANSFORM_REVIEW_REQUIRED_TARGET_UNMAPPED in d.cause_codes]
    # the same id on the TOP level is a binding, and then the map is opaque
    comps = _map_named("Fresh Map")
    next(c for c in comps if c["key"] == _MAP_KEY)["component_id"] = "existing-elsewhere"
    with patch(_PAGINATE, lambda *a, **k: []):
        bound, _ = plan_authoring_request_v1(_request(comps), boomi_client=MagicMock(), profile=_PROFILE)
    assert not [d for d in bound.errors if TRANSFORM_REVIEW_REQUIRED_TARGET_UNMAPPED in d.cause_codes]


def _name_reused_source_profile(name="Existing Source"):
    """An in-plan profile carrying no local body — reused by NAME, no flag."""
    return {"key": "src_prof", "type": "profile.db", "action": "create", "name": name,
            "config": {"profile_type": "database.read"}}


def test_a_watermark_over_a_name_reused_profile_is_deferred_not_refused():
    """`reference_only` is one spelling of reuse; a name collision carries no flag.

    Keying deferrability on the flag rejected, during offline normalization, a
    declaration the account can satisfy.
    """
    from boomi_mcp.categories import integration_builder

    request = _watermark_over("$ref:src_prof", [_name_reused_source_profile()])
    with _account_holding("Existing Source"), patch.object(
        integration_builder, "_discover_profile_index",
        lambda client, uuid: {"profile_component_type": "profile.db",
                              "field_index_by_path": _index(("updated_at", "id"))},
    ):
        result, _ = plan_authoring_request_v1(request, boomi_client=MagicMock(), profile=_PROFILE)
    assert all(d.code != GOVERNANCE_WATERMARK_INCONSISTENT for d in result.errors)


def test_the_same_profile_under_clone_is_written_so_its_watermark_is_decided_here():
    """The adversarial half: under `clone` the request WRITES it, so its config is the authority."""
    from boomi_mcp.categories import integration_builder

    request = _watermark_over("$ref:src_prof", [_name_reused_source_profile()])
    request = request.model_copy(
        update={"intent": request.intent.model_copy(update={"conflict_policy": "clone"})}
    )
    with _account_holding("Existing Source"), patch.object(
        integration_builder, "_discover_profile_index",
        lambda client, uuid: {"profile_component_type": "profile.db",
                              "field_index_by_path": _index(("updated_at", "id"))},
    ):
        result, _ = plan_authoring_request_v1(request, boomi_client=MagicMock(), profile=_PROFILE)
    assert [d for d in result.errors if d.code == GOVERNANCE_WATERMARK_INCONSISTENT]


# ---------------------------------------------------------------------------
# live QA r12 — what the corrections still modelled twice
# ---------------------------------------------------------------------------


def test_a_truthy_non_list_mapping_list_never_reaches_the_caller_as_a_type_error():
    """The projection reads these lists BEFORE the coverage gate does.

    Its own copies of ``or ()`` turned `field_mappings: true` into a bare
    `TypeError`, served as a three-key envelope with no machine code at all —
    the gate that would have refused it never ran.
    """
    from boomi_mcp.categories.integration_builder import build_integration_action

    for junk in ("field_mappings", "function_mappings", "script_mappings"):
        comps = copy.deepcopy(_components())
        entry = next(c for c in comps if c["key"] == _MAP_KEY)
        entry["config"][junk] = True
        for action in ("plan", "compile"):
            result = build_integration_action(
                MagicMock(), _PROFILE, action,
                {"authoring_request": _request(comps).model_dump(mode="json")},
            )
            assert result.get("exception_type") != "TypeError", (junk, action, result)
            assert "object is not iterable" not in str(result.get("error") or ""), (junk, action)


def test_the_projection_reads_its_mapping_lists_through_the_shared_reader():
    """One reader, or the projection is a third copy of it.

    Removing the shared reader must take the projection's operations with it —
    proof that the enrolment is a call and not a comment.
    """
    from boomi_mcp.authoring import derived_flows
    from boomi_mcp.authoring.derived_flows import derive_transform_flows

    (row,) = derive_transform_flows(
        [_flow_unit()], _flow_components(reference_only=False),
        connector_metadata={"dbc": ("database", None)},
    )
    assert row.operations, "the fixture binds no mapping, so this proves nothing"
    with patch.object(derived_flows, "mapping_entries", lambda value: ()):
        (blind,) = derive_transform_flows(
            [_flow_unit()], _flow_components(reference_only=False),
            connector_metadata={"dbc": ("database", None)},
        )
    assert blind.operations == ()


def test_plan_compile_and_apply_agree_on_which_maps_are_written():
    """The apply refresh modelled "written" from the plan's LABEL, not the predicate.

    An explicit `component_id` skips candidate resolution, so a declared create
    keeps `planned_action="create"` while apply reuses it — the corner the
    predicate's own docstring records. Modelling the label gated at apply a map
    the pre-write pass had correctly exempted, so plan and compile said yes and
    apply said no about the same request.

    The map's target is a reused profile, which is what brings the refresh's
    gate into play at all; the map itself is bound to an existing component, so
    every pass that asks the predicate exempts it.
    """
    from boomi_mcp.categories import integration_builder
    from boomi_mcp.categories.integration_builder import build_integration_action

    comps, clean_root = _reused_target()
    entry = next(c for c in comps if c["key"] == _MAP_KEY)
    entry["component_id"] = "map-uuid-1"
    account_root = copy.deepcopy(clean_root)
    account_root["children"].append(
        {"name": "must", "kind": "simple", "data_type": "character", "required": True}
    )
    discovery = patch.object(
        integration_builder, "_discover_profile_index",
        lambda client, uuid: {"profile_component_type": "profile.json",
                              "field_index_by_path": _json_index(account_root)},
    )
    with patch(_PAGINATE, lambda *a, **k: []), discovery:
        planned, _ = plan_authoring_request_v1(
            _request(comps), boomi_client=MagicMock(), profile=_PROFILE
        )
        assert not [
            d for d in planned.errors
            if TRANSFORM_REVIEW_REQUIRED_TARGET_UNMAPPED in d.cause_codes
        ], "the pre-write pass already disagrees, so this proves nothing"
        payload = _bound_wet(comps)
        with patch.object(integration_builder, "_execute_component"):
            applied = build_integration_action(MagicMock(), _PROFILE, "apply", payload)
    assert applied.get("error_code") != TRANSFORM_REVIEW_REQUIRED_TARGET_UNMAPPED, applied


def test_the_typed_pass_discovers_each_reused_profile_once_per_request():
    """Two owners in the typed pass asked the account the same question twice.

    Two reads of a live account can disagree, so the served preview and the gate
    could describe different artifacts. The legacy component-plan lint keeps its
    own read: it is a separate subsystem with its own resolution, reached
    through `_build_plan`, and that remainder is measured here rather than
    claimed away.
    """
    from boomi_mcp.categories import integration_builder

    comps, clean_root = _reused_target()
    asked = []

    def counting(client, uuid):
        asked.append(uuid)
        return {"profile_component_type": "profile.json",
                "field_index_by_path": _json_index(clean_root)}

    with patch.object(integration_builder, "_discover_profile_index", counting):
        plan_authoring_request_v1(_request(comps), boomi_client=MagicMock(), profile=_PROFILE)
    typed_owners = 1
    legacy_lint_owner = 1
    assert len(asked) == typed_owners + legacy_lint_owner, asked
    assert set(asked) == {"prof-uuid-1"}


# ---------------------------------------------------------------------------
# Stage-2 round 7 — one failure is one component, one empty index is an answer
# ---------------------------------------------------------------------------


def test_one_unreadable_profile_does_not_disable_the_gate_for_the_others():
    """A failed metadata read is one unanswered component, never a lost pass.

    An unguarded resolution aborted the whole selected-artifact pass, and the
    caller's broad handler then discarded every index it had — silently
    disabling required-target coverage for profiles that resolved perfectly.
    """
    from boomi_mcp.categories import integration_builder

    comps, root = _reused_target(missing_required=True)
    source = next(c for c in comps if c["type"] == "profile.db")

    def _explodes(client, comp):
        if comp.name == source.get("name"):
            raise RuntimeError("metadata read failed for this one component")
        return []

    with patch.object(integration_builder, "_resolve_existing_components", _explodes), \
         patch.object(
             integration_builder, "_discover_profile_index",
             lambda client, uuid: {"profile_component_type": "profile.json",
                                   "field_index_by_path": _json_index(root)},
         ):
        result, _ = plan_authoring_request_v1(
            _request(comps), boomi_client=MagicMock(), profile=_PROFILE
        )
    hits = [d for d in result.errors if TRANSFORM_REVIEW_REQUIRED_TARGET_UNMAPPED in d.cause_codes]
    assert hits, [d.code for d in result.errors]


def test_an_empty_index_from_the_selected_artifact_is_a_confirmed_absence():
    """`{}` from the account is the strongest evidence there is, not "ask later".

    The same `{}` from a candidate config is a guess normalization cannot make,
    so only the artifact's own answer decides.
    """
    from boomi_mcp.categories import integration_builder

    request = _watermark_over("$ref:src_prof", [_reused_source_profile()])
    with patch.object(
        integration_builder, "_discover_profile_index",
        lambda client, uuid: {"profile_component_type": "profile.db", "field_index_by_path": {}},
    ):
        decided, _ = plan_authoring_request_v1(request, boomi_client=MagicMock(), profile=_PROFILE)
    hits = [d for d in decided.errors if d.code == GOVERNANCE_WATERMARK_INCONSISTENT]
    assert hits and hits[0].path.endswith("/watermark/field"), [d.path for d in decided.errors]

    # and the artifact that could not be read at all is REFUSED, not deferred a
    # second time: no later pass exists to ask, and returning "undecided" left a
    # request compiling with a rule nothing ever decided
    with patch.object(integration_builder, "_discover_profile_index", lambda client, uuid: None):
        unreadable, _ = plan_authoring_request_v1(request, boomi_client=MagicMock(), profile=_PROFILE)
    stuck = [d for d in unreadable.errors if d.code == GOVERNANCE_WATERMARK_INCONSISTENT]
    assert stuck and stuck[0].path.endswith("/watermark/source_profile_ref"), [
        d.path for d in unreadable.errors
    ]


# ---------------------------------------------------------------------------
# architect evaluation 2 — the candidate config is never the authority
# ---------------------------------------------------------------------------


def _name_reused_target():
    """A target profile with no id and no flag, reused by NAME under the default policy."""
    comps = _components()  # its target profile needs `Root/must`
    target = next(c for c in comps if c["key"] == _TARGET_KEY)
    target["name"] = "Existing Target"
    target.pop("component_id", None)
    return comps


def test_a_name_reused_profile_whose_artifact_cannot_be_read_is_unavailable():
    """A failed discovery must not hand the candidate config back as the answer.

    The key is recorded with no index, which the resolver reads as "reused, and
    the evidence is unavailable" — the same answer the map validator already
    gives a `reference_only` profile.
    """
    from boomi_mcp.categories import integration_builder

    comps = _name_reused_target()
    with _account_holding("Existing Target"), patch.object(
        integration_builder, "_discover_profile_index", lambda client, uuid: None
    ):
        result, _ = plan_authoring_request_v1(
            _request(comps), boomi_client=MagicMock(), profile=_PROFILE
        )
    codes = {c for d in result.errors for c in d.cause_codes}
    # the same served shape a `reference_only` unavailability produces: the map
    # step is unexecutable, and no gate invented an answer from the candidate
    assert "error_generated_profile_validation" in codes, [d.cause_codes for d in result.errors]
    assert TRANSFORM_REVIEW_REQUIRED_TARGET_UNMAPPED not in codes
    # and the underlying refusal really is unavailability, from the shared validator
    from boomi_mcp.categories.components.builders.transform_map_validation import (
        validate_transform_map,
    )

    by_key = {c["key"]: IntegrationComponentSpec(**c) for c in comps}
    effective = dict(by_key[_MAP_KEY].config, component_name="Map")
    err = validate_transform_map(
        effective, by_key[_MAP_KEY].depends_on, by_key, None,
        {_TARGET_KEY: None},
    )
    assert err is not None and err.error_code == MAP_PROFILE_INDEX_UNAVAILABLE


def test_a_name_reused_watermark_source_is_decided_by_the_account_both_ways():
    """The candidate index must not decide for a profile the request will not write."""
    from boomi_mcp.categories import integration_builder

    def _plan_with(account_fields, candidate_fields):
        profile = {
            "key": "src_prof", "type": "profile.db", "action": "create",
            "name": "Existing Source",
            "config": {"profile_type": "database.read",
                       "output_fields": [{"name": f, "data_type": "character"} for f in candidate_fields]},
        }
        request = _watermark_over("$ref:src_prof", [profile])
        with _account_holding("Existing Source"), patch.object(
            integration_builder, "_discover_profile_index",
            lambda client, uuid: {"profile_component_type": "profile.db",
                                  "field_index_by_path": _index(account_fields)},
        ):
            return plan_authoring_request_v1(request, boomi_client=MagicMock(), profile=_PROFILE)[0]

    # the candidate says yes and the account says no: the account decides
    lying = _plan_with(account_fields=("id",), candidate_fields=("updated_at", "id"))
    assert [d for d in lying.errors if d.code == GOVERNANCE_WATERMARK_INCONSISTENT]
    # the candidate says no and the account says yes: the account decides
    honest = _plan_with(account_fields=("updated_at", "id"), candidate_fields=("id",))
    assert all(d.code != GOVERNANCE_WATERMARK_INCONSISTENT for d in honest.errors)


def test_the_recipe_routes_exempt_a_reused_map_like_the_direct_route():
    """One request, one answer — through direct authoring and through run_recipes."""
    from boomi_mcp.recipes import MaterializationCatalog, RecipeError, run_recipes
    from test_issue_157_required_target_coverage import _coverage_registry, _recipe_request

    comps = copy.deepcopy(_components())  # target profile needs `Root/must`
    entry = next(c for c in comps if c["key"] == _MAP_KEY)
    entry["config"]["reference_only"] = True
    entry["component_id"] = "map-uuid-1"

    with patch(_PAGINATE, lambda *a, **k: []):
        direct, _ = plan_authoring_request_v1(
            _request(comps), boomi_client=MagicMock(), profile=_PROFILE
        )
    assert not [d for d in direct.errors if TRANSFORM_REVIEW_REQUIRED_TARGET_UNMAPPED in d.cause_codes]

    registry, catalog, _ = _coverage_registry(comps)
    run_recipes(_recipe_request(), catalog=catalog, registry=registry)  # must not raise

    # the adversarial half: without the binding the same map IS judged, both ways
    entry.pop("component_id")
    entry["config"].pop("reference_only")
    with patch(_PAGINATE, lambda *a, **k: []):
        judged, _ = plan_authoring_request_v1(
            _request(comps), boomi_client=MagicMock(), profile=_PROFILE
        )
    assert [d for d in judged.errors if TRANSFORM_REVIEW_REQUIRED_TARGET_UNMAPPED in d.cause_codes]
    registry, catalog, _ = _coverage_registry(comps)
    with pytest.raises(RecipeError):
        run_recipes(_recipe_request(), catalog=catalog, registry=registry)


def test_the_projection_serves_no_content_for_a_map_it_will_not_write():
    """A reused map's candidate destinations are discarded material, not a flow."""
    from boomi_mcp.authoring.derived_flows import derive_transform_flows

    components = _flow_components(reference_only=False)
    (described,) = derive_transform_flows(
        [_flow_unit()], components, connector_metadata={"dbc": ("database", None)}
    )
    assert described.operations and described.target_profile_generation is not None

    (opaque,) = derive_transform_flows(
        [_flow_unit()], components, connector_metadata={"dbc": ("database", None)},
        reused_keys={"map"},
    )
    assert opaque.operations == () and opaque.direct_field_mappings == ()
    assert opaque.source_profile_generation is None
    assert opaque.target_profile_generation is None
    assert "Root/candidate_only" not in repr(opaque.model_dump(mode="json"))


# ---------------------------------------------------------------------------
# architect evaluation 2 — what the accounting oracle actually checks
# ---------------------------------------------------------------------------


def test_a_wrong_profile_name_is_caught_even_though_the_digest_ignores_it():
    """R3 reconciles two PRODUCERS; it is not a licence to accept any name.

    Each replay compares a case against its own baseline, so the name is
    checked beside the digest instead of being dropped from both.
    """
    from _issue_157_flows_accounting import _profile_name_problems
    from boomi_mcp.authoring.derived_flows import normalize_flow_row

    frozen = {"ordinal": 3, "payload": {
        "key": "transform",
        "source_profile_generation": {"component_name": "Real Source", "mappable_paths": ["a"]},
    }}
    same = {"key": "transform", "source_profile_generation": {"component_name": "Real Source", "mappable_paths": ["a"]}}
    wrong = {"key": "transform", "source_profile_generation": {"component_name": "UNRELATED WRONG PROFILE NAME", "mappable_paths": ["a"]}}

    assert _profile_name_problems([frozen], [same]) == []
    problems = _profile_name_problems([frozen], [wrong])
    assert problems and "UNRELATED WRONG PROFILE NAME" in problems[0]
    # the digest genuinely does not see it, which is why the check has to exist
    assert normalize_flow_row(same) == normalize_flow_row(wrong)
    # and a side that carries no name on one half is still reconciled, not failed
    assert _profile_name_problems([frozen], [{"key": "t", "source_profile_generation": {"mappable_paths": ["a"]}}]) == []


@pytest.mark.parametrize(
    "record,expected",
    [
        ({"classification": "RETIRE", "field_path": "a/b"}, None),
        ({"classification": "RETAIN", "field_path": "a/b"}, "retires nothing"),
        ({"classification": "SPLIT", "field_path": "a/b"}, "retires nothing"),
        ({"classification": "RETIRE", "field_path": "somewhere/else"}, "is not this row's"),
        (None, "unknown retirement record"),
    ],
)
def test_a_retired_row_must_cite_a_record_that_retires_this_field(record, expected):
    """The status was believed on the strength of the id existing."""
    from _issue_157_flows_accounting import _retirement_problem

    retirements = {"RET-157-99": record} if record is not None else {}
    problem = _retirement_problem("row 1", "RET-157-99", retirements, field_path="a/b")
    if expected is None:
        assert problem is None
    else:
        assert problem is not None and expected in problem


def test_every_retirement_record_carries_a_measured_applicability_disposition():
    """The fingerprint axis is disposed of by MEASUREMENT, not by omission.

    A materialization or execution fingerprint exists only for a canonical
    process root, and these historical specs author none — recorded from the
    replay so the day a producer starts authoring one, the omission stops being
    non-applicability and the checker says so.
    """
    import copy as _copy

    from _issue_157_retirements import applicability_problems, load_records

    records = load_records()
    assert records
    for rid, record in records.items():
        applicability = record["observations"].get("fingerprint_applicability")
        assert applicability is not None, rid
        assert applicability["axis"] == "materialization/execution fingerprint", rid
        units = applicability["canonical_process_units"]
        assert units == {"base": 0, "varied": 0}, (rid, units)
        # and the CHECKER is what holds it: a record without the disposition, and
        # a producer that starts authoring a canonical root, are both reported
        report = {"fingerprint_applicability": applicability}
        assert applicability_problems(rid, record, report) == []
        stripped = _copy.deepcopy(record)
        stripped["observations"].pop("fingerprint_applicability")
        assert applicability_problems(rid, stripped, report), rid
        moved = _copy.deepcopy(record)
        moved["observations"]["fingerprint_applicability"]["canonical_process_units"]["base"] = 1
        assert applicability_problems(rid, moved, {"fingerprint_applicability": moved["observations"]["fingerprint_applicability"]}), rid


def test_the_offline_reuse_reader_touches_no_account():
    """Archetype composition and the recipe engine both contract never to.

    The reuse exemption they gained reads DECLARED bindings only, through the
    same precedence rule the planner uses rather than a second copy of it.
    """
    from boomi_mcp.categories import integration_builder
    from boomi_mcp.categories.integration_builder import reused_keys_for_components
    from boomi_mcp.models.integration_models import IntegrationComponentSpec

    bound = IntegrationComponentSpec(
        key="m", type="transform.map", action="create", name="Bound",
        component_id="existing-1", config={"reference_only": True},
    )
    named = IntegrationComponentSpec(
        key="n", type="transform.map", action="create", name="ByName",
        config={"reference_only": True},
    )
    with patch.object(integration_builder, "paginate_metadata") as boundary:
        answer = reused_keys_for_components([bound, named])
    boundary.assert_not_called()
    # the declared binding is honoured; the name, which only the account could
    # resolve, is NOT invented — so the map is judged rather than skipped
    assert answer == {"m"}


# ---------------------------------------------------------------------------
# Stage-2 round 9 — what the evaluation-2 batch broke
# ---------------------------------------------------------------------------


_ISSUE_95_XML = (
    Path(__file__).resolve().parent / "fixtures" / "profile_components" / "issue_95" / "profile_xml.xml"
)


def test_a_reused_profile_family_the_summary_cannot_carry_stays_opaque():
    """An index without a representation is served as nothing, not as a crash.

    The summary model carries the two families the surviving generators emit.
    Building one for a reused `profile.xml` made its `component_type`
    unrepresentable and failed the whole preview, for a plan that used to work.
    """
    from boomi_mcp.authoring.derived_flows import _SUMMARISABLE_PROFILE_TYPES, derive_transform_flows
    from boomi_mcp.categories.components.builders.profile_generation import (
        index_existing_profile_xml,
    )
    from boomi_mcp.models.integration_models import IntegrationComponentSpec

    assert "profile.xml" not in _SUMMARISABLE_PROFILE_TYPES
    indexed = index_existing_profile_xml(_ISSUE_95_XML.read_text(encoding="utf-8"))
    assert indexed["profile_component_type"] == "profile.xml"

    components = [
        c if c.key != "tgt" else IntegrationComponentSpec(
            key="tgt", type="profile.xml", action="create", name="Tgt",
            component_id="prof-uuid-x", config={"reference_only": True},
        )
        for c in _flow_components(reference_only=True)
    ]
    (row,) = derive_transform_flows(
        [_flow_unit()], components, connector_metadata={"dbc": ("database", None)},
        selected_artifacts={"tgt": {"profile_component_type": indexed["profile_component_type"],
                                   "field_index_by_path": indexed["field_index_by_path"]}},
    )
    assert row.target_profile_generation is None
    # the db source beside it is still described, so this is opacity, not silence
    assert row.source_profile_generation is not None


def test_the_recipe_reuse_exemption_honours_the_callers_conflict_policy():
    """Under `clone` the map is WRITTEN, so its candidate mappings are judged."""
    from boomi_mcp.recipes import MaterializationCatalog, RecipeError, run_recipes
    from test_issue_157_required_target_coverage import _coverage_registry, _recipe_request

    comps = copy.deepcopy(_components())  # target profile needs `Root/must`
    entry = next(c for c in comps if c["key"] == _MAP_KEY)
    entry["component_id"] = "map-uuid-1"  # a binding, but no reference_only flag

    registry, catalog, _ = _coverage_registry(comps)
    run_recipes(_recipe_request(), catalog=catalog, registry=registry, conflict_policy="reuse")
    registry, catalog, _ = _coverage_registry(comps)
    with pytest.raises(RecipeError):
        run_recipes(_recipe_request(), catalog=catalog, registry=registry, conflict_policy="clone")


def test_a_watermark_over_a_profile_this_request_writes_is_always_decided():
    """The last pass decides; "undecided" leaves the rule decided by nobody.

    A raw-XML profile yields no structured index, and this request WRITES it, so
    its own config is the only authority the field rule could ever have.
    """
    raw = {"key": "src_prof", "type": "profile.json", "action": "create", "name": "Raw",
           "config": {"xml": "<JSONProfile/>"}}
    request = _watermark_over("$ref:src_prof", [raw])
    with patch(_PAGINATE, lambda *a, **k: []):
        result, _ = plan_authoring_request_v1(request, boomi_client=MagicMock(), profile=_PROFILE)
    hits = [d for d in result.errors if d.code == GOVERNANCE_WATERMARK_INCONSISTENT]
    assert hits and hits[0].path.endswith("/watermark/field"), [d.path for d in result.errors]
    # and a LITERAL id with no supplied index is still genuinely owed, so it defers
    literal = _watermark_over("11111111-1111-1111-1111-111111111111", [])
    with patch(_PAGINATE, lambda *a, **k: []):
        deferred, _ = plan_authoring_request_v1(literal, boomi_client=MagicMock(), profile=_PROFILE)
    assert all(d.code != GOVERNANCE_WATERMARK_INCONSISTENT for d in deferred.errors)


# ---------------------------------------------------------------------------
# live QA r13 — the request is never the authority on an existing component
# ---------------------------------------------------------------------------


def test_a_selected_summary_takes_its_type_from_the_artifact_not_the_request():
    """Discovery reads and verifies the real type; the request's is a claim.

    Nothing checks a request's declared type against the component it binds to,
    so serving it beside the account's index described one component from two
    authorities — and the unverified half is the one that crashed the preview.
    """
    from boomi_mcp.authoring.derived_flows import derive_transform_flows
    from boomi_mcp.models.integration_models import IntegrationComponentSpec

    components = [
        c if c.key != "tgt" else IntegrationComponentSpec(
            key="tgt", type="profile.json", action="create", name="Tgt",
            component_id="prof-uuid-1",
            config={"reference_only": True, "component_name": "A NAME THE ACCOUNT DOES NOT HAVE"},
        )
        for c in _flow_components(reference_only=True)
    ]
    # the ARTIFACT is a db profile, whatever the request declared
    artifact = {
        "profile_component_type": "profile.db",
        "field_index_by_path": {
            "updated_at": {
                "path": "updated_at", "name": "updated_at", "mappable": True,
                "profile_component_type": "profile.db", "data_type": "character",
            }
        },
    }
    (row,) = derive_transform_flows(
        [_flow_unit()], components, connector_metadata={"dbc": ("database", None)},
        selected_artifacts={"tgt": artifact},
    )
    assert row.target_profile_generation.component_type == "profile.db"
    assert row.target_profile_generation.component_name is None
    assert "A NAME THE ACCOUNT DOES NOT HAVE" not in repr(row.model_dump(mode="json"))


def test_an_unexecutable_step_serves_its_own_cause_code_and_remedy():
    """One hand-written diagnosis for every unexecutable step named the wrong cause.

    An unreadable profile index was reported as a component collision, and the
    code that actually names it appeared nowhere in the served plan.
    """
    from boomi_mcp.categories import integration_builder

    comps = _name_reused_target()
    with _account_holding("Existing Target"), patch.object(
        integration_builder, "_discover_profile_index", lambda client, uuid: None
    ):
        result, _ = plan_authoring_request_v1(
            _request(comps), boomi_client=MagicMock(), profile=_PROFILE
        )
    hits = [d for d in result.errors if d.subject_id == _MAP_KEY and d.cause_codes]
    assert hits, [d.code for d in result.errors]
    assert MAP_PROFILE_INDEX_UNAVAILABLE in hits[0].cause_codes, hits[0].cause_codes
    assert "component collision" not in hits[0].remediation
    # and a step whose cause really IS a collision keeps the collision remedy
    from boomi_mcp.authoring.workflow import _unexecutable_remedy

    assert "component collision" in _unexecutable_remedy(
        {"planned_action": "error_ambiguous_match"}
    )


def test_a_preview_the_server_cannot_build_is_refused_with_a_code():
    """A bare pydantic message with no `error_code` is the one shape this surface avoids.

    Scoped to the preview build: a validation failure anywhere else on this
    route is the caller's request being wrong, and the surface already has codes
    for that — so the catch must not swallow those.
    """
    from boomi_mcp.authoring import workflow as wf
    from boomi_mcp.errors import GOVERNANCE_PREVIEW_UNREPRESENTABLE
    from boomi_mcp.models.derived_flows import GeneratedProfileSummaryV1

    with pytest.raises(wf.AuthoringWorkflowError) as excinfo:
        with wf._preview_or_named_refusal():
            GeneratedProfileSummaryV1.model_validate(
                {"component_type": "profile.zzz", "field_index_by_path": {}}
            )
    assert excinfo.value.code == GOVERNANCE_PREVIEW_UNREPRESENTABLE
    paths = [d.path for d in excinfo.value.diagnostics]
    assert paths and all(p.startswith("/integration_spec_preview/") for p in paths), paths
    # value-free: the offending INPUT never travels, only the field that rejected it
    assert "profile.zzz" not in repr([d.model_dump(mode="json") for d in excinfo.value.diagnostics])
    # and the code reaches the dispatcher's envelope as a coded refusal
    from boomi_mcp.categories.integration_builder import _authoring_error_envelope

    envelope = _authoring_error_envelope(excinfo.value, "plan")
    assert envelope["_success"] is False
    assert GOVERNANCE_PREVIEW_UNREPRESENTABLE in repr(envelope)


def test_the_connector_snapshot_is_told_the_reuse_set_not_the_live_readings():
    """The parameter's own contract names the reuse predicate.

    The components that READ LIVE are a different question: under
    `conflict_policy="clone"` a declared component is cloned, not reused, and
    handing the snapshot the live-reading set made plan and compile accept a
    request the wet apply then refused.
    """
    from boomi_mcp.authoring import connector_resolution_snapshot as crs
    from test_issue_157_governance import _conn, _op, _request as _gov_request, _unit

    seen = []
    real = crs.build_connector_resolution_snapshot

    def spy(components, **kwargs):
        seen.append(frozenset(kwargs.get("reused_keys") or ()))
        return real(components, **kwargs)

    def _plan_under(policy):
        # a CREATE bound to an existing component by id, and no reuse flag: it
        # is reused under the default policy and CLONED under `clone`, which is
        # exactly the distinction the live-reading set cannot make
        conn = _conn()
        conn["component_id"] = "existing-conn-1"
        request = _gov_request([_unit(name="P")], [conn, _op()])
        request = request.model_copy(
            update={"intent": request.intent.model_copy(update={"conflict_policy": policy})}
        )
        with patch.object(crs, "build_connector_resolution_snapshot", spy), patch.object(
            crs, "live_readings_for_declared_components",
            lambda client, components: {"conn": "<bns:Component/>"},
        ):
            plan_authoring_request_v1(request, boomi_client=MagicMock(), profile=_PROFILE)
        return seen[-1]

    reuse_answer = _plan_under("reuse")
    clone_answer = _plan_under("clone")
    assert reuse_answer == frozenset({"conn"}), reuse_answer
    # the live reading is identical in both runs; only the POLICY differs, and
    # the snapshot's answer has to move with it
    assert clone_answer == frozenset(), clone_answer


# ---------------------------------------------------------------------------
# Stage-2 round 10 — the flag is not the reuse decision, anywhere
# ---------------------------------------------------------------------------


@pytest.mark.parametrize("action", ["create", "update"])
def test_the_index_resolver_agrees_with_the_reuse_predicate_on_every_action(action):
    """A `reference_only` UPDATE is WRITTEN, so its own config is the authority.

    Pinned in both directions against `_will_reuse_at_apply` rather than
    restated: the resolver's short-circuit and the predicate must give the same
    answer for the same binding, and a test is what holds that, because a second
    statement of the rule is the defect this slice keeps closing.
    """
    from boomi_mcp.categories.components.builders.transform_map_validation import (
        resolve_map_profile_index,
    )
    from boomi_mcp.categories.integration_builder import _will_reuse_at_apply
    from boomi_mcp.models.integration_models import IntegrationComponentSpec

    profile = IntegrationComponentSpec(
        key="src_prof", type="profile.db", action=action, name="Src",
        component_id="prof-uuid-1",
        config={"reference_only": True, "profile_type": "database.read",
                "output_fields": [{"name": "updated_at", "data_type": "character"}]},
    )
    by_key = {"src_prof": profile}
    index = resolve_map_profile_index("$ref:src_prof", by_key, None, None)
    predicate_says_reused = _will_reuse_at_apply(
        declared_action=action, existing_component_id="prof-uuid-1",
        reference_only=True, conflict_policy="reuse",
    )
    # the resolver withholds the candidate index exactly when the predicate says
    # the request will not write this component
    assert (index is None) is predicate_says_reused, (action, index)
    if not predicate_says_reused:
        assert "updated_at" in index


def test_a_watermark_over_an_updated_reference_only_profile_is_accepted():
    """End to end: the profile the request WRITES declares the field, so it passes."""
    profile = {"key": "src_prof", "type": "profile.db", "action": "update",
               "name": "Src", "component_id": "prof-uuid-1",
               "config": {"reference_only": True, "profile_type": "database.read",
                          "output_fields": [{"name": "updated_at", "data_type": "character"}]}}
    request = _watermark_over("$ref:src_prof", [profile])
    with patch(_PAGINATE, lambda *a, **k: []):
        result, _ = plan_authoring_request_v1(request, boomi_client=MagicMock(), profile=_PROFILE)
    assert all(d.code != GOVERNANCE_WATERMARK_INCONSISTENT for d in result.errors), [
        (d.code, d.path) for d in result.errors
    ]
    # and the adversarial half: a field it does NOT declare is still refused
    missing = copy.deepcopy(profile)
    missing["config"]["output_fields"] = [{"name": "id", "data_type": "character"}]
    with patch(_PAGINATE, lambda *a, **k: []):
        refused, _ = plan_authoring_request_v1(
            _watermark_over("$ref:src_prof", [missing]), boomi_client=MagicMock(), profile=_PROFILE
        )
    assert [d for d in refused.errors if d.code == GOVERNANCE_WATERMARK_INCONSISTENT]


def test_the_advisory_view_carries_every_attribute_the_shared_helpers_read():
    """A view that omits a field answers a different question from the component.

    The shared reuse rule reads `action`; the advisory view omitted it, so the
    advisory route indexed the candidate config of a profile apply will reuse —
    exactly what the rule exists to prevent.
    """
    from types import SimpleNamespace

    from boomi_mcp.categories.transformation_review import _comp_view
    from boomi_mcp.models.integration_models import IntegrationComponentSpec

    authored = {"key": "p", "type": "profile.db", "name": "P",
                "config": {"reference_only": True}, "depends_on": []}
    view = _comp_view(authored)
    real = IntegrationComponentSpec(**authored)
    # the DEFAULT travels too: an omitted action is the model's default, not None
    assert view.action == real.action == "create"
    assert _comp_view(dict(authored, action="update")).action == "update"
    # and every attribute the shared resolver reads is present on the view
    for attribute in ("key", "type", "name", "action", "config", "depends_on"):
        assert hasattr(view, attribute), attribute


def test_the_advisory_route_will_not_index_a_reused_profile_from_its_candidate():
    """The sibling of the model-based rule, on the caller the review flagged."""
    from boomi_mcp.categories.transformation_review import review_transformation_action

    comps = copy.deepcopy(_components(missing_required=False))
    target = next(c for c in comps if c["key"] == _TARGET_KEY)
    target["component_id"] = "prof-uuid-1"
    target["config"] = {"reference_only": True, "component_name": "Existing Target",
                        "profile_type": "json.generated", "root": target["config"]["root"]}

    reused = review_transformation_action(
        "validate_unmapped", {"integration_spec": {"name": "x", "components": comps}}
    )
    assert reused.get("_success") is False or reused.get("valid") is not True, reused
    assert "PROFILE_INDEX_UNAVAILABLE" in str(reused), reused

    # the adversarial half: the same profile as an UPDATE is written by this
    # request, so its own config is indexed and the review answers normally
    target["action"] = "update"
    written = review_transformation_action(
        "validate_unmapped", {"integration_spec": {"name": "x", "components": comps}}
    )
    assert "PROFILE_INDEX_UNAVAILABLE" not in str(written), written

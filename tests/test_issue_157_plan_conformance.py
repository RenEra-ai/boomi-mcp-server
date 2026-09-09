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
    surface at the map STEP, after earlier components were written. The drift is
    simulated where the code itself distinguishes the two reads: the refresh is
    the ONLY caller that supplies ``reused_keys`` (the planner's own decision),
    so answering that one question with the changed profile is exactly "the
    account moved after the plan was validated".
    """
    from boomi_mcp.categories import integration_builder

    comps, clean_root = _reused_target()
    drifted_root = copy.deepcopy(clean_root)
    drifted_root["children"].append(
        {"name": "must", "kind": "simple", "data_type": "character", "required": True}
    )

    asked = []

    def drifting(client, spec, reused_keys=None, conflict_policy=None):
        asked.append(reused_keys)
        root = drifted_root if reused_keys is not None else clean_root
        return {_TARGET_KEY: _json_index(root)}

    clean_discovery = patch.object(
        integration_builder, "_discover_profile_index",
        lambda client, uuid: {"profile_component_type": "profile.json",
                              "field_index_by_path": _json_index(clean_root)},
    )
    with clean_discovery:
        payload = _bound_wet(comps)
    with clean_discovery, patch.object(
        integration_builder, "_resolve_selected_profile_indexes", drifting
    ), patch.object(integration_builder, "_execute_component") as execute, patch.object(
        integration_builder, "create_component"
    ) as create:
        result = integration_builder.build_integration_action(MagicMock(), _PROFILE, "apply", payload)
    assert any(k is None for k in asked) and any(k is not None for k in asked), asked
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

    selected = {"tgt": _json_index(_SELECTED_ROOT)}
    (selected_row,) = derive_transform_flows(
        [_flow_unit()], components, connector_metadata=metadata, selected_indexes=selected
    )
    assert selected_row.target_profile_generation.mappable_paths == ("Root/platform_only",)
    assert set(selected_row.target_profile_generation.field_index_by_path) == set(
        _json_index(_SELECTED_ROOT)
    )
    # the row keeps the model's other declared fields — only the FIELD SET moves
    assert selected_row.target_profile_generation.component_type == "profile.json"
    assert selected_row.target_profile_generation.profile_config is not None


def test_a_profile_the_request_will_write_is_still_described_by_its_generator():
    """The adversarial half: a selected index for an unrelated key changes nothing."""
    from boomi_mcp.authoring.derived_flows import derive_transform_flows

    components = _flow_components(reference_only=False)
    (row,) = derive_transform_flows(
        [_flow_unit()], components, connector_metadata={"dbc": ("database", None)},
        selected_indexes={"some_other_key": _json_index(_SELECTED_ROOT)},
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
        selected_indexes={"tgt": live},
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


def test_a_reused_profile_with_no_local_body_serves_no_generation_summary():
    """`GeneratedProfileSummaryV1` needs the GENERATOR's own output; inventing it is worse."""
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
    (row,) = derive_transform_flows(
        [_flow_unit()], bare, connector_metadata={"dbc": ("database", None)},
        selected_indexes={"tgt": _live_index()["field_index_by_path"]},
    )
    assert row.target_profile_generation is None
    # and the source, which DOES have a body, is unaffected
    assert row.source_profile_generation is not None


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
    """Only a REUSED profile defers: this one the request writes, so it is decided here."""
    empty = {"key": "src_prof", "type": "profile.db", "action": "create",
             "config": {"profile_type": "database.read"}}
    with pytest.raises(Exception) as excinfo:
        plan_authoring_request_v1(
            _watermark_over("$ref:src_prof", [empty]), boomi_client=None, profile=_PROFILE
        )
    assert GOVERNANCE_WATERMARK_INCONSISTENT in str(excinfo.value)


def test_the_reused_source_profile_still_defers():
    """The adversarial half: the case the deferral exists for is untouched."""
    result, _ = plan_authoring_request_v1(
        _watermark_over("$ref:src_prof", [_reused_source_profile()]),
        boomi_client=None, profile=_PROFILE,
    )
    assert all(d.code != GOVERNANCE_WATERMARK_INCONSISTENT for d in result.errors)

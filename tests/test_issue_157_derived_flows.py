"""Issue #157 (M12.19) — the typed, generator-derived, output-only flows projection.

Three things are pinned here:

* **Normalization per legacy form.** The DB-source and the API-source rich
  ``transform`` rows both parse into ``DerivedTransformFlowV1`` through the ONE
  normalizer, dropping nothing but the recorded reconciliation rules.
* **The canonical projection is derived, not authored.** Its rows come from the
  surviving generator and the map components; it is absent from every hash and
  a caller cannot supply it.
* **Generative differentials.** Fixed cases cannot enumerate profile/mapping
  space, so seeded random profiles and mapping subsets drive the projection and
  the assertions are STRUCTURAL against the input (mapping count, field
  identity, generated-profile subshape), never against a frozen payload.
"""

from __future__ import annotations

import copy
import json
import random
import re
import string
import sys
from pathlib import Path
from unittest.mock import MagicMock, patch

import pytest

_src = str(Path(__file__).resolve().parent.parent / "src")
if _src not in sys.path:
    sys.path.insert(0, _src)
sys.path.insert(0, str(Path(__file__).resolve().parent))

from _issue_157_flows_accounting import load_cases  # noqa: E402
from _m12_11_support import APPLIABLE_CONN, APPLIABLE_OP  # noqa: E402
from boomi_mcp.authoring.derived_flows import (  # noqa: E402
    SOURCE_TOKEN_BY_FAMILY,
    derive_transform_flows,
    normalize_flow_row,
    typed_row_from_legacy,
)
from boomi_mcp.authoring.workflow import plan_authoring_request_v1  # noqa: E402
from boomi_mcp.compiler.process_ir.connector_capabilities import DATABASE_FAMILY  # noqa: E402
from boomi_mcp.categories.components.builders.connector_builder import (  # noqa: E402
    _resolve_rest_connector_type,
)
from boomi_mcp.models.authoring_workflow import (  # noqa: E402
    AuthoringRequestV1,
    CanonicalIntegrationPreviewV1,
    ProcessIRAuthoringIntentV1,
)
from boomi_mcp.models.derived_flows import (  # noqa: E402
    DERIVED_FLOW_OPERATIONS,
    DerivedTransformFlowV1,
    GeneratedProfileSummaryV1,
)
from boomi_mcp.models.process_component import (  # noqa: E402
    ProcessAuthoringUnitAuthoredV1,
    ProcessComponentEnvelopeAuthoredV1,
)
from boomi_mcp.models.process_ir import parse_process_ir_v1  # noqa: E402

_PAGINATE = "boomi_mcp.categories.integration_builder.paginate_metadata"


@pytest.fixture(autouse=True)
def _offline():
    with patch(_PAGINATE, lambda *a, **k: []):
        yield


@pytest.fixture(scope="module")
def cases():
    return load_cases()


def _transform_row(case):
    return next(r for r in case.flow_rows if r["key"] == "transform")


# ---------------------------------------------------------------------------
# normalization: one test per legacy producer form
# ---------------------------------------------------------------------------


def test_the_db_source_rich_form_normalizes_into_the_typed_row(cases):
    raw = _transform_row(cases["database_to_api_sync/wm_on_dlq_on_reuse"])["raw"]
    typed = typed_row_from_legacy(raw)
    assert typed.source == "extract" and typed.operation == "transform" and typed.executable is False
    assert typed.source_profile_generation.generation_mode == "profile_from_db_read_fields"
    assert typed.target_profile_generation.generation_mode == "profile_from_json_schema"
    assert [op.operation_type for op in typed.operations] == ["direct", "map_function"]
    # the DB form spells the source column `source_field`; normalized to source_path (R4)
    assert typed.operations[0].source_path == raw["operations"][0]["source_field"]
    assert typed.operations[0].documentation_hint == "carry first column verbatim"
    # every legacy leaf is carried or reconciled by a recorded rule — nothing else vanishes
    lost = _leaves(raw) - _leaves(typed.model_dump(mode="json", exclude_unset=True))
    assert lost <= {"future_builder_issue", "source_field", "component_name"}, lost


def test_the_api_source_rich_form_normalizes_into_the_typed_row(cases):
    raw = _transform_row(cases["api_to_api_sync/reuse_with_function"])["raw"]
    typed = typed_row_from_legacy(raw)
    assert typed.source == "fetch"
    assert typed.source_profile_generation.generation_mode == "profile_from_json_schema"
    assert typed.source_schema.format == "json"
    assert [op.operation_type for op in typed.operations] == ["direct", "map_function"]
    lost = _leaves(raw) - _leaves(typed.model_dump(mode="json", exclude_unset=True))
    assert lost <= {"future_builder_issue", "source_field", "component_name"}, lost


def _leaves(value, prefix=""):
    out = set()
    if isinstance(value, dict):
        for key, item in value.items():
            out.add(key)
            out |= _leaves(item, prefix + "/" + key)
    elif isinstance(value, list):
        for item in value:
            out |= _leaves(item, prefix)
    return out


def test_a_map_script_body_is_digested_never_served():
    raw = {
        "key": "transform", "name": "n", "source": "extract", "target": None, "operation": "transform",
        "executable": False, "source_schema": {"field_count": 0, "fields": []},
        "operations": [{"operation_type": "map_script", "language": "groovy", "inputs": ["a"], "input_count": 1,
                        "outputs": ["Root/b"], "output_count": 1, "script_body_present": True,
                        "script_body": "def x = 1", "future_builder_issue": "#41"}],
    }
    typed = typed_row_from_legacy(raw)
    dumped = json.dumps(typed.model_dump(mode="json"))
    assert "def x = 1" not in dumped
    assert typed.operations[0].script_body_sha256 and typed.operations[0].script_body_present is True


def test_a_legacy_leaf_the_model_cannot_carry_is_a_parse_failure_not_a_silent_drop(cases):
    raw = copy.deepcopy(_transform_row(cases["api_to_api_sync/create"])["raw"])
    raw["surprise_key"] = 1
    with pytest.raises(Exception):
        typed_row_from_legacy(raw)


# ---------------------------------------------------------------------------
# the projection is derived-only
# ---------------------------------------------------------------------------


def _request(units, components):
    return AuthoringRequestV1(intent=ProcessIRAuthoringIntentV1(integration_name="x", units=tuple(units), components=tuple(components)))


def _canonical_case(cases, case_id):
    return AuthoringRequestV1.model_validate(cases[case_id].input_canonical)


def test_the_projection_is_absent_from_every_hash(cases):
    request = _canonical_case(cases, "api_to_api_sync/create")
    result, internals = plan_authoring_request_v1(request, boomi_client=MagicMock(), profile="p")
    assert result.integration_spec_preview.flows
    from boomi_mcp.authoring.workflow import _normalized_payload, _plan_hash_preview

    payload = json.dumps(_normalized_payload(internals.normalized, request))
    assert "transform" not in json.dumps(json.loads(payload)["integration_spec"].get("flows", []))
    assert _plan_hash_preview(result.integration_spec_preview)["flows"] == []
    assert "preview_kind" not in _plan_hash_preview(result.integration_spec_preview)


def _direct_db_to_api_request(cases, case_id):
    """#159 S2: the DB->API cases are recipe invocations now, so the DIRECT
    DB source -> map -> REST target intent these tests read is built from the
    hand-built linear case (baseline components, hand-authored root) — a
    direct root no inference applies to."""
    components, root, _facts, _wm = _hand_built_linear_case(cases, case_id)
    referenced = sorted(set(re.findall(r'"\$ref:([^"]+)"', json.dumps(root))))
    unit = ProcessAuthoringUnitAuthoredV1(
        envelope=ProcessComponentEnvelopeAuthoredV1(
            component_key=_PROCESS, name="P", action="create", depends_on=tuple(referenced)
        ),
        process_ir=parse_process_ir_v1(root),
    )
    return _request([unit], components)


def test_a_compiling_intent_plan_hash_is_unmoved_by_the_projection(cases):
    """The pre-#157 hash input: flows empty, no preview kind."""
    from boomi_mcp.authoring.workflow import _plan_hash_preview
    from boomi_mcp.models.integration_models import IntegrationSpecV1

    request = _direct_db_to_api_request(cases, "database_to_api_sync/wm_off_dlq_off_create")
    result, _ = plan_authoring_request_v1(request, boomi_client=MagicMock(), profile="p")
    preview = result.integration_spec_preview
    assert isinstance(preview, CanonicalIntegrationPreviewV1)
    legacy_shape = IntegrationSpecV1(**{k: v for k, v in preview.model_dump(mode="json").items() if k not in ("flows", "preview_kind")})
    assert _plan_hash_preview(preview) == legacy_shape.model_dump(mode="json")


def test_the_projection_serves_one_typed_row_per_map_node(cases):
    request = _direct_db_to_api_request(cases, "database_to_api_sync/wm_off_dlq_off_create")
    result, _ = plan_authoring_request_v1(request, boomi_client=MagicMock(), profile="p")
    (row,) = result.integration_spec_preview.flows
    assert isinstance(row, DerivedTransformFlowV1)
    assert row.operation in DERIVED_FLOW_OPERATIONS
    assert isinstance(row.target_profile_generation, GeneratedProfileSummaryV1)


def test_the_source_token_table_is_pinned_to_the_connector_families():
    """Three entries, each a real family alias — a fourth family is a decision here."""
    assert DATABASE_FAMILY in SOURCE_TOKEN_BY_FAMILY
    assert _resolve_rest_connector_type("rest") is not None and "rest" in SOURCE_TOKEN_BY_FAMILY
    assert set(SOURCE_TOKEN_BY_FAMILY) == {"database", "rest", "wss"}
    assert set(SOURCE_TOKEN_BY_FAMILY.values()) == {"extract", "fetch", "listen"}


def test_the_served_schema_carries_the_typed_flows():
    schema = CanonicalIntegrationPreviewV1.model_json_schema()
    assert "DerivedTransformFlowV1" in json.dumps(schema)
    assert schema["properties"]["preview_kind"]["const"] == "canonical_units"


# ---------------------------------------------------------------------------
# generative differentials: the projection TRACKS the input structurally
# ---------------------------------------------------------------------------

_TYPES = ("character", "number", "datetime")


def _random_profile(rng, depth=0):
    name = "".join(rng.choice(string.ascii_lowercase) for _ in range(rng.randint(2, 6)))
    if depth >= 2 or rng.random() < 0.6:
        return {"name": name, "kind": "simple", "data_type": rng.choice(_TYPES), "required": rng.random() < 0.5}
    return {"name": name, "kind": "object", "children": [_random_profile(rng, depth + 1) for _ in range(rng.randint(1, 3))]}


def _leaf_paths(node, prefix=""):
    path = (prefix + "/" + node["name"]) if prefix else node["name"]
    if node["kind"] == "simple":
        return [path]
    out = []
    for child in node["children"]:
        out += _leaf_paths(child, path)
    return out


def _unique_children(node, seen=None):
    """Sibling names must be unique; the generator may repeat one — fix it."""
    if node["kind"] != "simple":
        names = set()
        kept = []
        for child in node["children"]:
            if child["name"] in names:
                continue
            names.add(child["name"])
            kept.append(_unique_children(child))
        node["children"] = kept or [{"name": "leaf", "kind": "simple", "data_type": "character", "required": False}]
    return node


@pytest.mark.parametrize("seed", list(range(12)))
def test_generative_the_projection_tracks_profiles_and_mapping_subsets(seed):
    rng = random.Random(seed)
    source_fields = [
        {"name": "f{0}_{1}".format(index, rng.choice(string.ascii_lowercase)), "data_type": rng.choice(_TYPES), "required": rng.random() < 0.3}
        for index in range(rng.randint(1, 6))
    ]
    target_root = _unique_children({"name": "Root", "kind": "object", "children": [_random_profile(rng) for _ in range(rng.randint(1, 4))]})
    target_leaves = _leaf_paths(target_root)
    subset = rng.sample(target_leaves, rng.randint(1, len(target_leaves)))
    mappings = [{"source_path": source_fields[i % len(source_fields)]["name"], "target_path": leaf} for i, leaf in enumerate(subset)]

    src_profile = {"key": "src", "type": "profile.db", "action": "create", "name": "Src",
                   "config": {"profile_type": "database.read", "output_fields": [dict(f, mandatory=f["required"]) for f in source_fields]}}
    tgt_profile = {"key": "tgt", "type": "profile.json", "action": "create", "name": "Tgt", "config": {"format": "json", "root": target_root}}
    the_map = {"key": "map", "type": "transform.map", "action": "create", "name": "Map", "depends_on": ["src", "tgt"],
               "config": {"map_type": "direct", "source_profile_id": "$ref:src", "target_profile_id": "$ref:tgt", "field_mappings": mappings}}
    db_conn = {"key": "dbc", "type": "connector-settings", "action": "create", "name": "DB",
               "config": {"connector_type": "database", "reference_only": True, "component_name": "Existing DB"}}
    db_op = {"key": "dbo", "type": "connector-action", "action": "create", "name": "Get", "depends_on": ["dbc", "src"],
             "config": {"connector_type": "database", "operation_mode": "get", "connection_ref_key": "dbc", "read_profile_id": "$ref:src"}}
    unit = ProcessAuthoringUnitAuthoredV1(
        envelope=ProcessComponentEnvelopeAuthoredV1(component_key="proc", name="P", action="create", depends_on=("dbc", "dbo", "map")),
        process_ir=parse_process_ir_v1({"version": "1", "body": {"kind": "sequence", "steps": [
            {"kind": "source", "connection_ref": "$ref:dbc", "operation_ref": "$ref:dbo"},
            {"kind": "map_ref", "map_ref": "$ref:map", "label": "Generated"},
            {"kind": "return_documents"}]}}),
    )
    from boomi_mcp.models.integration_models import IntegrationComponentSpec

    components = [IntegrationComponentSpec(**c) for c in (db_conn, db_op, src_profile, tgt_profile, the_map)]
    (row,) = derive_transform_flows([unit], components, connector_metadata={"dbc": ("database", None)})

    # structure tracks the input, never a frozen payload
    assert row.source == "extract" and row.name == "Generated"
    assert len(row.operations) == len(mappings)
    assert [op.target_path for op in row.operations] == [m["target_path"] for m in mappings]
    assert len(row.direct_field_mappings) == len(mappings)
    assert set(row.target_profile_generation.mappable_paths) == set(target_leaves)
    assert set(row.source_profile_generation.mappable_paths) == {f["name"] for f in source_fields}
    assert row.source_schema.field_count == len(source_fields)
    assert {f.name: f.required for f in row.source_schema.fields} == {f["name"]: f["required"] for f in source_fields}
    assert row.target_payload_profile.leaf_count == len(target_leaves)
    required_leaves = {p for p, rec in row.target_profile_generation.field_index_by_path.items() if rec.required and rec.mappable}
    assert required_leaves == {p for p in target_leaves if _required(target_root, p)}


def _required(root, path):
    node = root
    for segment in path.split("/")[1:]:
        node = next(c for c in node["children"] if c["name"] == segment)
    return bool(node.get("required"))


# ---------------------------------------------------------------------------
# #159 S1: the recipe-route row kinds, endpoints and composition summaries
# ---------------------------------------------------------------------------


def _flagship_projection_inputs():
    """The flagship v2 plan's normalized units, components and trusted facts."""
    from _fanout_v2_support import flagship_v2, recipe_request

    result, internals = plan_authoring_request_v1(recipe_request(*flagship_v2()), profile="p")
    normalized = internals.normalized
    return result, normalized


def _project(normalized, *, units=None, components=None, facts=None):
    from boomi_mcp.authoring.derived_flows import derive_recipe_projection

    return derive_recipe_projection(
        units if units is not None else normalized.integration_spec.processes,
        components if components is not None else normalized.integration_spec.components,
        connector_metadata=normalized.connector_metadata,
        projection_facts=normalized.recipe_projection_facts if facts is None else facts,
        recorded_intents=normalized.recorded_intents,
    )


def test_the_recipe_row_spellings_are_the_frozen_legacy_spellings(cases):
    """Every fixed spelling the recipe route serves is the one the legacy
    producers froze, measured on every frozen row that carries the key."""
    from boomi_mcp.authoring.derived_flows import (
        CONNECTOR_OPERATION_BY_DIRECTION_AND_FAMILY,
        ENDPOINT_TOKEN_BY_DIRECTION_AND_FAMILY,
        RECIPE_FLOW_ROW_SPELLINGS,
    )

    seen = {key: set() for key in RECIPE_FLOW_ROW_SPELLINGS}
    for case in cases.values():
        for row in case.flow_rows:
            if row["key"] in seen:
                seen[row["key"]].add(row["payload"]["name"])
    assert {key: {name} for key, name in RECIPE_FLOW_ROW_SPELLINGS.items()} == seen
    extract = [r["payload"] for c in cases.values() for r in c.flow_rows if r["key"] == "extract"]
    send = [r["payload"] for c in cases.values() for r in c.flow_rows if r["key"] == "send"
            and r["payload"]["operation"] == "rest_send"]
    db_extract = {(r["source"], r["operation"]) for r in extract if r["operation"] == "db_get"}
    assert db_extract == {
        (
            ENDPOINT_TOKEN_BY_DIRECTION_AND_FAMILY[("source", "database")],
            CONNECTOR_OPERATION_BY_DIRECTION_AND_FAMILY[("source", "database")],
        )
    }
    assert {(r["target"], r["operation"]) for r in send} == {
        (
            ENDPOINT_TOKEN_BY_DIRECTION_AND_FAMILY[("target", "rest")],
            CONNECTOR_OPERATION_BY_DIRECTION_AND_FAMILY[("target", "rest")],
        )
    }


def test_the_operation_vocabulary_is_derived_from_the_discriminator_literals():
    from typing import get_args

    from pydantic import TypeAdapter

    from boomi_mcp.models import derived_flows as models

    union = get_args(get_args(models.DerivedFlowRowV1)[0])
    assert set(union) == set(models._DERIVED_FLOW_MEMBERS)
    assert set(DERIVED_FLOW_OPERATIONS) == {
        "db_get", "rest_send", "transform", "reliability", "watermark", "branch",
        # #159 S2: the linear presets' connector kinds and the stub's no-op
        "rest_fetch", "db_write", "wss_listen", "noop",
    }
    # every kind a frozen #157 row carries is servable — measured, not listed
    frozen_kinds = {
        row["kind"] for case in load_cases().values() for row in case.flow_rows
    }
    assert frozen_kinds <= set(DERIVED_FLOW_OPERATIONS), frozen_kinds - set(DERIVED_FLOW_OPERATIONS)
    schema = TypeAdapter(models.DerivedFlowRowV1).json_schema()
    assert schema["discriminator"]["propertyName"] == "operation"
    assert set(schema["discriminator"]["mapping"]) == set(DERIVED_FLOW_OPERATIONS)


def test_recipe_flow_kinds_are_derived_from_the_root_and_components():
    """The facts TRIGGER the rows and name the parts; the values come from the
    composed root and components. Mutating the root moves ``legs``; mutating a
    connection component moves the endpoint's binding mode and selector."""
    result, normalized = _flagship_projection_inputs()
    baseline = _project(normalized)
    assert [row.key for row in baseline.flows] == [
        "extract", "transform", "send", "reliability", "fanout",
    ]
    assert [row.model_dump(mode="json") for row in baseline.flows] == [
        row.model_dump(mode="json") for row in result.integration_spec_preview.flows
    ]
    fanout_row = baseline.flows[-1]
    (unit,) = normalized.integration_spec.processes
    branch = unit.process_ir.body.steps[-1]
    assert fanout_row.legs == len(branch.legs)

    # the ROOT decides the leg count: one more streaming leg, one more leg
    root = unit.process_ir.model_dump(mode="json")
    root["body"]["steps"][-1]["legs"].append(copy.deepcopy(root["body"]["steps"][-1]["legs"][0]))
    wider = unit.model_copy(update={"process_ir": parse_process_ir_v1(root)})
    assert _project(normalized, units=(wider,)).flows[-1].legs == len(branch.legs) + 1

    # the COMPONENT decides the binding: a reference-only connection is a reuse
    facts = normalized.recipe_projection_facts[unit.envelope.component_key]
    key = facts.targets[1].connection_key
    components = [
        c.model_copy(update={
            "component_id": "existing-rest-id",
            "config": {"reference_only": True, "connector_type": "rest", "component_id": "existing-rest-id"},
        })
        if c.key == key else c
        for c in normalized.integration_spec.components
    ]
    reused = _project(normalized, components=components)
    endpoint = reused.endpoints[2]
    assert endpoint.binding_mode == "reuse" and endpoint.component_id == "existing-rest-id"
    assert endpoint.auth_mode is None
    summary = reused.composition_summaries[unit.envelope.component_key]
    assert summary.fanout_legs[0].binding_mode == "reuse"
    assert baseline.composition_summaries[unit.envelope.component_key].fanout_legs[0].binding_mode == "create"


def test_a_unit_without_trusted_facts_is_projected_exactly_as_before():
    """The trigger is the trusted facts, never the shape: the SAME flagship root
    without them serves its transform row only, and no endpoints or summaries."""
    _result, normalized = _flagship_projection_inputs()
    bare = _project(normalized, facts={})
    assert [row.key for row in bare.flows] == ["transform"]
    assert bare.endpoints == () and bare.composition_summaries == {}
    assert [r.model_dump(mode="json") for r in bare.flows] == [
        r.model_dump(mode="json")
        for r in derive_transform_flows(
            normalized.integration_spec.processes,
            normalized.integration_spec.components,
            connector_metadata=normalized.connector_metadata,
        )
    ]


def test_facts_that_do_not_resolve_never_invent_a_row():
    _result, normalized = _flagship_projection_inputs()
    (unit,) = normalized.integration_spec.processes
    facts = normalized.recipe_projection_facts[unit.envelope.component_key]
    dangling = facts.model_copy(update={"source_connection_key": "no_such_component"})
    projected = _project(normalized, facts={unit.envelope.component_key: dangling})
    assert [row.key for row in projected.flows] == ["transform"]
    assert projected.endpoints == () and projected.composition_summaries == {}


def test_the_served_endpoints_and_summaries_are_outside_the_plan_hash():
    from boomi_mcp.authoring.workflow import _plan_hash_preview

    result, _normalized = _flagship_projection_inputs()
    preview = result.integration_spec_preview
    assert preview.endpoints and preview.composition_summaries
    hashed = _plan_hash_preview(preview)
    assert hashed["endpoints"] == [] and hashed["flows"] == []
    assert "composition_summaries" not in hashed
    # and an empty summary map is OMITTED from the served object entirely
    empty = preview.model_copy(update={"composition_summaries": {}})
    assert "composition_summaries" not in empty.model_dump(mode="json")


# ---------------------------------------------------------------------------
# #159 S2 (L4): the linear projection facts, discharged by hand-built facts
# ---------------------------------------------------------------------------
#
# Provenance. These tests prove the PROJECTOR, not the lanes' recipes: the facts
# are built by hand. The components come from causally independent sources —
# the committed canonical fixtures for DB->API and API->API (themselves the
# baseline producer's emitted components, see `_issue_157_canonical_fixtures`)
# and, for the cases without one, the baseline producer's emitted components
# for the SAME frozen `input.legacy.json`, executed from a `git archive` of the
# baseline (`baseline_supporting_components`). The roots are hand-authored over
# those components following each legacy pipeline's stage order. The DLQ cases
# add one reference-only document cache and a process-scope `try_catch` whose
# catch path writes it — a TEST shape, not the DB->REST recipe's. The oracle is
# the frozen ordered rows, compared by full normalized payload digest.

_PROCESS = "main_process"


def _linear_unit(root):
    return ProcessAuthoringUnitAuthoredV1(
        envelope=ProcessComponentEnvelopeAuthoredV1(
            component_key=_PROCESS, name="P", action="create", depends_on=()
        ),
        process_ir=parse_process_ir_v1(root),
    )


def _seq(*steps):
    return {"version": "1", "body": {"kind": "sequence", "steps": list(steps)}}


def _linear_project(
    components, root, facts, *, watermark_field=None, reused_keys=None,
    existing_ids=None, conflict_policy=None,
):
    from types import SimpleNamespace

    from boomi_mcp.authoring.derived_flows import derive_recipe_projection
    from boomi_mcp.authoring.workflow import _connector_metadata_from_components
    from boomi_mcp.models.governance_intent import WatermarkDeclarationV1
    from boomi_mcp.models.integration_models import IntegrationComponentSpec

    specs = [IntegrationComponentSpec(**c) for c in components]
    recorded = ()
    if watermark_field is not None:
        recorded = (
            SimpleNamespace(
                process_key=_PROCESS,
                declaration=WatermarkDeclarationV1(
                    source_profile_ref="$ref:source_db_read_profile",
                    field=watermark_field,
                    kind="timestamp",
                ),
            ),
        )
    # the request's policy, handed in only when a case names one
    policy = {} if conflict_policy is None else {"conflict_policy": conflict_policy}
    return derive_recipe_projection(
        [_linear_unit(root)],
        specs,
        connector_metadata=_connector_metadata_from_components(specs),
        projection_facts={_PROCESS: facts} if facts is not None else {},
        recorded_intents=recorded,
        reused_keys=reused_keys,
        existing_ids=existing_ids,
        **policy,
    )


def _dumped(projection):
    flows = [normalize_flow_row(r.model_dump(mode="json", exclude_unset=True)) for r in projection.flows]
    endpoints = [r.model_dump(mode="json", exclude_unset=True) for r in projection.endpoints]
    return flows, endpoints


def _linear_facts(source, target, detail, *, status=None, reliability=None, source_conn=True):
    from boomi_mcp.models.derived_flows import (
        LinearConnectorFactV1,
        LinearRouteProjectionFactsV1,
        LinearTransformFactV1,
    )

    return LinearRouteProjectionFactsV1(
        process_key=_PROCESS,
        source=LinearConnectorFactV1(
            connection_key=source[0] if source_conn else None, operation_key=source[1]
        ),
        transform=LinearTransformFactV1(detail=detail),
        target=LinearConnectorFactV1(
            connection_key=target[0], operation_key=target[1], expected_status_codes=status
        ),
        reliability=reliability,
    )


_DB_SOURCE = ("source_db_connection", "source_db_get_operation")
_REST_SOURCE = ("source_rest_source_connection", "source_rest_source_operation")
_WSS_SOURCE = (None, "source_wss_listener_operation")
_REST_TARGET = ("target_rest_connection", "target_rest_operation")
_DB_TARGET = ("target_db_connection", "target_db_write_operation")


def _ref(key):
    return "$ref:" + key


def _linear_root(source_kind, source, label, target):
    first = (
        {"kind": "listener", "operation_ref": _ref(source[1])}
        if source_kind == "listener"
        else {"kind": "source", "connection_ref": _ref(source[0]), "operation_ref": _ref(source[1])}
    )
    return _seq(
        first,
        {"kind": "map_ref", "map_ref": "$ref:transform_transform_map", "label": label},
        {"kind": "target", "connection_ref": _ref(target[0]), "operation_ref": _ref(target[1])},
        {"kind": "stop"},
    )


def _dlq_root(root):
    """The same route inside a process-scope try_catch whose catch writes the DLQ."""
    steps = root["body"]["steps"]

    def call(step):
        return {"kind": "connector_call", "operation_ref": step["operation_ref"]}

    return _seq(
        {
            "kind": "try_catch",
            "scope": "process",
            "try_body": {"steps": [call(steps[0]), steps[1], call(steps[2])], "terminal": {"kind": "stop"}},
            "catch_body": {"steps": [], "terminal": {"kind": "cache_put", "cache_ref": "$ref:dlq_cache"}},
        }
    )


_DLQ_CACHE = {
    "key": "dlq_cache", "type": "document_cache", "action": "create", "name": "DLQ",
    "config": {"reference_only": True, "component_id": "<<dlq document cache component id>>"},
}


def _hand_built_linear_case(cases, case_id):
    """``(components, root, facts, watermark_field)`` for one frozen linear case."""
    from boomi_mcp.authoring.derived_flows import infer_direct_linear_facts
    from boomi_mcp.authoring.workflow import _connector_metadata_from_components
    from boomi_mcp.models.derived_flows import (
        LinearReliabilityFactV1,
        StubRouteProjectionFactsV1,
    )
    from boomi_mcp.models.integration_models import IntegrationComponentSpec
    from _issue_157_canonical_fixtures import baseline_supporting_components

    case = cases[case_id]
    producer = case.producer
    if producer == "api_to_api_sync":
        components = copy.deepcopy(case.input_canonical["intent"]["components"])
        root = copy.deepcopy(case.input_canonical["intent"]["units"][0]["process_ir"])
    elif producer == "database_to_api_sync":
        # #159 S2: the DB->API cases are recipe invocations now; the hand-built
        # route reads the BASELINE producer's components (a legacy-oracle
        # capture) under a hand-authored DB source -> map -> REST target root.
        components = baseline_supporting_components([case_id])[case_id]
        root = _linear_root("source", _DB_SOURCE, _transform_row(case)["payload"]["name"], _REST_TARGET)
    elif producer == "stub_minimal":
        return [], _seq({"kind": "passthrough"}, {"kind": "stop"}), StubRouteProjectionFactsV1(process_key=_PROCESS), None
    else:
        components = baseline_supporting_components([case_id])[case_id]
        label = _transform_row(case)["payload"]["name"]
        target = _DB_TARGET if "database" in producer or producer.endswith("_db") else _REST_TARGET
        if producer.startswith("http_listener"):
            root = _linear_root("listener", _WSS_SOURCE, label, target)
            for component in components:
                if component["type"] == "webservice":
                    # the baseline ASC routes to the legacy process key; the
                    # hand-authored unit is that same key
                    assert component["config"]["routes"][0]["process"] == _ref(_PROCESS)
        else:
            root = _linear_root("source", _REST_SOURCE, label, target)
        source = _WSS_SOURCE if producer.startswith("http_listener") else _REST_SOURCE
        facts = _linear_facts(source, target, "schema_summary", source_conn=source[0] is not None)
        return components, root, facts, None
    if producer == "api_to_api_sync":
        specs = [IntegrationComponentSpec(**c) for c in components]
        facts = infer_direct_linear_facts(
            _linear_unit(root), specs, _connector_metadata_from_components(specs)
        )
        return components, root, facts, None
    dlq = "_dlq_on_" in case_id
    if dlq:
        components.append(dict(_DLQ_CACHE))
        root = _dlq_root(root)
    facts = _linear_facts(
        _DB_SOURCE, _REST_TARGET, "generated_artifacts",
        reliability=LinearReliabilityFactV1(dlq_component_key="dlq_cache" if dlq else None),
    )
    # The caller's per-operation hints, read off the frozen LEGACY input (the
    # baseline components carry none; the legacy row served them).
    from boomi_mcp.models.derived_flows import RecipeOperationHintV1

    hints = tuple(
        RecipeOperationHintV1(
            operation_type=operation["operation_type"],
            target_path=operation["target_path"],
            documentation_hint=operation["documentation_hint"],
        )
        for operation in case.input_legacy["input"]["parameters"]["transform"]["operations"]
        if operation.get("documentation_hint")
    )
    facts = facts.model_copy(
        update={"transform": facts.transform.model_copy(update={"documentation_hints": hints})}
    )
    return components, root, facts, ("source_a" if case_id.split("/")[1].startswith("wm_on") else None)


_LINEAR_PRODUCERS = (
    "database_to_api_sync", "api_to_api_sync", "api_to_database_sync",
    "http_listener_to_db", "http_listener_to_rest", "stub_minimal",
)


def test_hand_built_linear_facts_reproduce_every_frozen_linear_case(cases):
    """Every non-composition frozen case — all fifteen, every new flow kind —
    reproduces its COMPLETE ordered flow and endpoint sequences, by full
    normalized payload, from hand-built facts over independent components."""
    linear = sorted(case_id for case_id, case in cases.items() if case.producer in _LINEAR_PRODUCERS)
    assert len(linear) == len(cases) - sum(1 for c in cases.values() if c.producer == "composition")
    failures = []
    kinds = set()
    for case_id in linear:
        case = cases[case_id]
        components, root, facts, watermark = _hand_built_linear_case(cases, case_id)
        flows, endpoints = _dumped(_linear_project(components, root, facts, watermark_field=watermark))
        if [acct_digest(r) for r in flows] != [r["payload_sha256"] for r in case.flow_rows]:
            failures.append((case_id, "flows", [r.get("key") for r in flows]))
        # The owed endpoint payloads: the frozen ones, with a field-scoped
        # retirement's one field removed (CDX-159-S2-r1-02: the API->API target
        # declares no status expectation).
        if endpoints != acct_owed_endpoints(case):
            failures.append((case_id, "endpoints", endpoints))
        kinds |= {r["operation"] for r in flows}
    assert failures == []
    # non-vacuity: every new kind, the watermark and the reliability rows were exercised
    assert {"rest_fetch", "db_write", "wss_listen", "noop", "watermark", "reliability"} <= kinds


def acct_digest(row):
    import _issue_157_flows_accounting as acct

    return acct._digest(row)


def acct_owed_endpoints(case):
    import _issue_157_flows_accounting as acct

    return acct.owed_endpoint_payloads(case)


def test_the_schema_summary_form_omits_the_generation_keys_and_reads_the_write_profile(cases):
    """null versus absent: the summary form SETS no generation key, and its DB
    write summary is the builder's index over the write profile component."""
    case_id = "api_to_database_sync/create"
    components, root, facts, _wm = _hand_built_linear_case(cases, case_id)
    projection = _linear_project(components, root, facts)
    transform = projection.flows[1].model_dump(mode="json", exclude_unset=True)
    for absent in ("direct_field_mappings", "source_profile_generation",
                   "target_profile_generation", "target_payload_profile"):
        assert absent not in transform, absent
    assert transform["target"] is None and "target" in transform
    # the summary TRACKS the component: add a column, the summary moves
    for component in components:
        if component["key"] == "target_db_write_profile":
            component["config"]["fields"].append({"name": "zz", "data_type": "number", "mandatory": True})
    moved = _linear_project(components, root, facts).flows[1].target_write_profile
    assert moved.field_count == transform["target_write_profile"]["field_count"] + 1
    assert moved.target_paths[-1].path == "Fields/zz" and moved.target_paths[-1].required is True


def test_linear_facts_that_do_not_resolve_never_invent_a_row(cases):
    """A wrong operation key, a missing component or an unreferenced DLQ pointer
    projects the transform row only — no connector rows, no endpoints."""
    from boomi_mcp.models.derived_flows import LinearReliabilityFactV1

    case_id = "database_to_api_sync/wm_off_dlq_on_create"
    components, root, facts, _wm = _hand_built_linear_case(cases, case_id)
    assert [r.key for r in _linear_project(components, root, facts).flows][0] == "extract"
    wrong_op = facts.model_copy(update={"target": facts.target.model_copy(update={"operation_key": "nope"})})
    dangling = facts.model_copy(update={"reliability": LinearReliabilityFactV1(dlq_component_key="no_such")})
    # the same route WITHOUT the try/catch: nothing in it writes the DLQ cache
    unreferenced_root = _linear_root(
        "source", _DB_SOURCE, _transform_row(cases[case_id])["payload"]["name"], _REST_TARGET
    )
    for components_in, root_in, facts_in in (
        (components, root, wrong_op),
        (components, root, dangling),
        (components, unreferenced_root, facts),  # the pointer resolves, the ROOT never writes it
        ([c for c in components if c["key"] != "target_rest_connection"], root, facts),
    ):
        projected = _linear_project(components_in, root_in, facts_in)
        assert [r.key for r in projected.flows] == ["transform"]
        assert projected.endpoints == () and projected.composition_summaries == {}


def test_the_dlq_target_follows_its_pointer_never_a_try_catch(cases):
    """A retry-only try_catch reports the canonical default reliability row
    (target null); only a resolved, root-referenced DLQ pointer serves ``dlq``."""
    from boomi_mcp.models.derived_flows import LinearReliabilityFactV1

    case_id = "database_to_api_sync/wm_off_dlq_on_create"
    components, root, facts, _wm = _hand_built_linear_case(cases, case_id)
    with_dlq = _linear_project(components, root, facts).flows
    retry_only = _linear_project(
        components, root, facts.model_copy(update={"reliability": LinearReliabilityFactV1()})
    ).flows
    assert [r.key for r in with_dlq] == [r.key for r in retry_only]
    assert with_dlq[3].target == "dlq" and retry_only[3].target is None
    no_row = _linear_project(components, root, facts.model_copy(update={"reliability": None})).flows
    assert [r.key for r in no_row] == ["extract", "transform", "send"]
    # the legacy DLQ is an EXISTING cache: a literal id the root writes verbatim
    literal = "<<dlq document cache component id>>"
    literal_root = json.loads(json.dumps(root).replace("$ref:dlq_cache", literal))
    literal_facts = facts.model_copy(update={"reliability": LinearReliabilityFactV1(dlq_component_key=literal)})
    without_cache = [c for c in components if c["key"] != "dlq_cache"]
    served = _linear_project(without_cache, literal_root, literal_facts).flows
    assert [r.key for r in served] == [r.key for r in with_dlq] and served[3].target == "dlq"
    # ...and a literal the root never writes is no DLQ at all
    assert [r.key for r in _linear_project(without_cache, root, literal_facts).flows] == ["transform"]


def test_only_an_exact_direct_rest_map_rest_root_infers_linear_facts(cases):
    """The API->API inference is shape-exact: a DB source (the hand-built
    DB->API direct roots; the S1 tip's direct fixtures are recipe invocations
    now) or a target of another family infers nothing."""
    from boomi_mcp.authoring.derived_flows import infer_direct_linear_facts
    from boomi_mcp.authoring.workflow import _connector_metadata_from_components
    from boomi_mcp.models.integration_models import IntegrationComponentSpec

    def infer(case_id, mutate=None):
        intent = copy.deepcopy(cases[case_id].input_canonical["intent"])
        if mutate:
            mutate(intent)
        root = intent["units"][0]["process_ir"]
        specs = [IntegrationComponentSpec(**c) for c in intent["components"]]
        return infer_direct_linear_facts(_linear_unit(root), specs, _connector_metadata_from_components(specs))

    facts = infer("api_to_api_sync/create")
    assert facts.source.operation_key == "source_rest_source_operation"
    # CDX-159-S2-r1-02: topology declares no success-status expectation
    assert facts.target.expected_status_codes is None
    assert facts.source.expected_status_codes is None and facts.reliability is None
    db_cases = sorted(c for c in cases if c.startswith("database_to_api_sync/"))
    assert db_cases
    for case_id in db_cases:
        components, root, _facts, _wm = _hand_built_linear_case(cases, case_id)
        specs = [IntegrationComponentSpec(**c) for c in components]
        assert infer_direct_linear_facts(
            _linear_unit(root), specs, _connector_metadata_from_components(specs)
        ) is None, case_id

    def database_target(intent):
        # the SAME root over a target connection of another family
        for component in intent["components"]:
            if component["key"] == "target_rest_connection":
                component["config"] = {"connector_type": "database", "reference_only": True,
                                       "component_name": "Existing DB"}

    assert infer("api_to_api_sync/create", database_target) is None
    # The retired default helper is gone from the projector: nothing there can
    # assert a status policy no component declares.
    import boomi_mcp.authoring.derived_flows as projector

    assert not hasattr(projector, "rest_success_status_default")


def test_every_supported_rest_spelling_projects_the_same_route(cases):
    """#159 architect review r1, finding 8: a family is resolved through
    ``connector_family_of`` (the resolver the builders route on), never by
    lowercasing the declared spelling. Every spelling that resolver accepts as
    REST projects the direct API->API route's COMPLETE flow and endpoint
    payloads byte-identically to ``rest`` — the connector rows, the transform
    row's ``source`` token and both endpoints — and so does the fan-out route
    (its source and first-target families are read the same way)."""
    from boomi_mcp.authoring.derived_flows import derive_recipe_projection, infer_direct_linear_facts
    from boomi_mcp.authoring.workflow import _connector_metadata_from_components
    from boomi_mcp.categories.components.builders.connector_builder import (
        _REST_CLIENT_ALIASES,
        REST_CLIENT_SUBTYPE,
    )
    from boomi_mcp.models.integration_models import IntegrationComponentSpec

    spellings = sorted(set(_REST_CLIENT_ALIASES) | {REST_CLIENT_SUBTYPE, "REST", " Rest_Client "})
    # non-vacuity: every spelling IS a REST spelling to the resolver, and at
    # least one of them is not ``rest`` once lowercased (the excluded case)
    assert all(_resolve_rest_connector_type(s) is not None for s in spellings)
    assert any(s.strip().lower() != "rest" for s in spellings)

    def project(spelling):
        intent = copy.deepcopy(cases["api_to_api_sync/create"].input_canonical["intent"])
        for component in intent["components"]:
            if (component.get("config") or {}).get("connector_type") == "rest":
                component["config"]["connector_type"] = spelling
        root = intent["units"][0]["process_ir"]
        specs = [IntegrationComponentSpec(**c) for c in intent["components"]]
        facts = infer_direct_linear_facts(
            _linear_unit(root), specs, _connector_metadata_from_components(specs)
        )
        return facts, _dumped(_linear_project(intent["components"], root, facts))

    reference_facts, reference = project("rest")
    assert [r["key"] for r in reference[0]] == ["fetch", "transform", "send"]
    assert [e["key"] for e in reference[1]] == ["rest_source", "rest_target"]
    assert reference[0][1]["source"] == "fetch"
    for spelling in spellings:
        facts, projected = project(spelling)
        assert facts == reference_facts, spelling
        assert projected == reference, spelling

    # the fan-out route: its source and target families go through the same read
    _result, normalized = _flagship_projection_inputs()
    baseline = _project(normalized)
    for spelling in spellings:
        aliased = [
            c.model_copy(update={"config": {**c.config, "connector_type": spelling}})
            if (c.config or {}).get("connector_type") == "rest" else c
            for c in normalized.integration_spec.components
        ]
        assert any(a is not c for a, c in zip(aliased, normalized.integration_spec.components))
        moved = derive_recipe_projection(
            normalized.integration_spec.processes,
            aliased,
            connector_metadata=_connector_metadata_from_components(aliased),
            projection_facts=normalized.recipe_projection_facts,
            recorded_intents=normalized.recorded_intents,
        )
        assert [r.model_dump(mode="json") for r in moved.flows] == [
            r.model_dump(mode="json") for r in baseline.flows
        ], spelling
        assert [e.model_dump(mode="json") for e in moved.endpoints] == [
            e.model_dump(mode="json") for e in baseline.endpoints
        ], spelling


def test_a_listener_route_is_served_only_through_components_apply_writes(cases):
    """#159 architect review r1, finding 4: the advanced listener's route is read
    off the API Service's CANDIDATE routes and the listener operation's candidate
    config. Apply binds a create matched by name under ``reuse`` WITHOUT writing
    it (``apply_writes_component_config``), so a reused service keeps its
    existing routes and names no route to the new process. The endpoint — and so
    the route's connector rows — is served only when apply writes both; without
    a policy, only when it writes them under every policy. A clone, an update
    and an unmatched create are written, and serve the frozen row unchanged."""
    from boomi_mcp.categories.integration_builder import apply_writes_component_config
    from boomi_mcp.models.integration_models import IntegrationComponentSpec

    case_id = "http_listener_to_db/advanced_runtime"
    components, root, facts, _wm = _hand_built_linear_case(cases, case_id)
    (service,) = [c for c in components if c["type"] == "webservice"]
    operation_key = facts.source.operation_key
    frozen = _dumped(_linear_project(components, root, facts))
    (listener,) = [e for e in frozen[1] if e["type"] == "wss"]
    assert listener["publish_mode"] == "api_service" and listener["binding_mode"] == "create"
    full_keys = [r["key"] for r in frozen[0]]
    assert full_keys[0] == "listen" and len(full_keys) > 1

    bound = {service["key"]: "existing-api-service-id"}
    # non-vacuity: apply's own predicate says it does NOT write the bound
    # service under reuse, and DOES under clone
    spec = IntegrationComponentSpec(**service)
    assert apply_writes_component_config(spec, "reuse", existing_ids=bound) is False
    assert apply_writes_component_config(spec, "clone", existing_ids=bound) is True

    def served(components_in=components, **kwargs):
        return _dumped(_linear_project(components_in, root, facts, **kwargs))

    for policy in (None, "reuse"):
        flows, endpoints = served(existing_ids=bound, conflict_policy=policy)
        assert [r["key"] for r in flows] == ["transform"], policy
        assert endpoints == [], policy
        # the listener OPERATION bound by name is the same case
        flows, endpoints = served(existing_ids={operation_key: "existing-op-id"}, conflict_policy=policy)
        assert endpoints == [], policy
    # the service the caller already answered as reused
    assert served(reused_keys={service["key"]})[1] == []
    # written: a clone of the namesake, an update, or no namesake at all
    assert served(existing_ids=bound, conflict_policy="clone") == frozen
    updated = copy.deepcopy(components)
    for component in updated:
        if component["key"] == service["key"]:
            component.update(action="update", component_id="existing-api-service-id")
    assert served(updated, existing_ids=bound) == frozen
    assert served(existing_ids={}, conflict_policy="reuse") == frozen


def test_only_an_exact_unreferencing_passthrough_stop_root_infers_stub_facts():
    """#159 S2: a fact-less root that is exactly ``passthrough -> stop`` and
    references nothing infers the stub facts (the stub recipe emits none) and
    serves the noop row with its two stub endpoints. A third step, a root that
    references a component, or an envelope that depends on one infers nothing."""
    from boomi_mcp.authoring.derived_flows import infer_stub_facts
    from boomi_mcp.models.derived_flows import StubRouteProjectionFactsV1

    stub_root = _seq({"kind": "passthrough"}, {"kind": "stop"})
    facts = infer_stub_facts(_linear_unit(stub_root))
    assert facts == StubRouteProjectionFactsV1(process_key=_PROCESS)
    inferred = _linear_project([], stub_root, None)
    assert [r.operation for r in inferred.flows] == ["noop"]
    assert [e.type for e in inferred.endpoints] == ["stub", "stub"]

    # Non-vacuity controls: none of these is a Stop-only, reference-free root.
    referencing_third_step = _seq(
        {"kind": "passthrough"},
        {"kind": "map_ref", "map_ref": "$ref:transform_transform_map", "label": "m"},
        {"kind": "stop"},
    )
    other_terminal = _seq({"kind": "passthrough"}, {"kind": "return_documents"})
    for root in (referencing_third_step, other_terminal):
        assert infer_stub_facts(_linear_unit(root)) is None
        assert _linear_project([], root, None).endpoints == ()
    depending = ProcessAuthoringUnitAuthoredV1(
        envelope=ProcessComponentEnvelopeAuthoredV1(
            component_key=_PROCESS, name="P", action="create", depends_on=("some_component",)
        ),
        process_ir=parse_process_ir_v1(stub_root),
    )
    assert infer_stub_facts(depending) is None


def test_the_stub_route_needs_a_stop_only_root_and_its_endpoints_bind_nothing():
    from pydantic import ValidationError

    from boomi_mcp.models.derived_flows import DerivedEndpointRowV1, StubRouteProjectionFactsV1

    facts = StubRouteProjectionFactsV1(process_key=_PROCESS)
    stub = _linear_project([], _seq({"kind": "passthrough"}, {"kind": "stop"}), facts)
    assert [r.operation for r in stub.flows] == ["noop"]
    assert [e.model_dump(mode="json") for e in stub.endpoints] == [
        {"key": "stub_source", "type": "stub", "direction": "source", "label": "Stub source"},
        {"key": "stub_target", "type": "stub", "direction": "target", "label": "Stub target"},
    ]
    with pytest.raises(ValidationError):
        DerivedEndpointRowV1(key="x", type="rest", direction="source")  # no binding mode
    with pytest.raises(ValidationError):
        DerivedEndpointRowV1(key="x", type="stub", direction="source", binding_mode="create")
    with pytest.raises(ValidationError):
        DerivedEndpointRowV1(key="x", type="rest", direction="source", binding_mode="create", label="l")


# ---------------------------------------------------------------------------
# #159 S2 (L4): a small deterministic GENERATED batch through the linear route
# ---------------------------------------------------------------------------


def _gen_json_profile(rng, depth=0):
    """A nested JSON object profile with shuffled sibling order."""
    children = []
    names = rng.sample(string.ascii_lowercase, rng.randint(1, 4))
    for name in names:
        if depth < 2 and rng.random() < 0.35:
            child = _gen_json_profile(rng, depth + 1)
            child["name"] = name
            children.append(child)
        else:
            children.append({"name": name, "kind": "simple", "data_type": rng.choice(_TYPES),
                             "required": rng.random() < 0.4})
    rng.shuffle(children)  # leaf ORDER varies; the served leaves are path-sorted
    return {"name": "Root", "kind": "object", "children": children}


def _generated_linear_case(seed):
    """``(components, root, facts, expectations)`` for one generated route.

    Even seeds: a listener -> map -> DB write route (``schema_summary``);
    odd seeds: a REST fetch -> map -> REST send route (``generated_artifacts``).
    The mapping structure (direct subset + order, map functions) and the
    profile nesting and leaf order all vary with the seed."""
    rng = random.Random(1000 + seed)
    source_root = _gen_json_profile(rng)
    source_leaves = _leaf_paths(source_root)
    db_route = seed % 2 == 0
    if db_route:
        fields = [
            {"name": "c{0}_{1}".format(i, rng.choice(string.ascii_lowercase)),
             "data_type": rng.choice(("character", "number", "date")),
             "mandatory": rng.random() < 0.5, "enforce_unique": False}
            for i in range(rng.randint(1, 5))
        ]
        target_leaves = ["Fields/" + f["name"] for f in fields]
    else:
        target_root = _unique_children(_gen_json_profile(rng))
        target_leaves = _leaf_paths(target_root)
    chosen = rng.sample(target_leaves, rng.randint(1, len(target_leaves)))
    split = rng.randint(0, len(chosen))
    direct = [{"source_path": rng.choice(source_leaves), "target_path": t} for t in chosen[:split]]
    functions = [
        {"function_type": rng.choice(("string_concat", "string_to_upper", "date_format")),
         "inputs": rng.sample(source_leaves, min(len(source_leaves), rng.randint(1, 2))),
         "target_path": t}
        for t in chosen[split:]
    ]
    the_map = {"key": "transform_transform_map", "type": "transform.map", "action": "create", "name": "Map",
               "config": {"map_type": "function" if functions else "direct",
                          "source_profile_id": "$ref:src_profile", "source_profile_type": "profile.json",
                          "target_profile_id": "$ref:tgt_profile",
                          "target_profile_type": "profile.db" if db_route else "profile.json",
                          "field_mappings": direct, "function_mappings": functions}}
    src = {"key": "src_profile", "type": "profile.json", "action": "create", "name": "Src",
           "config": {"profile_type": "json.generated", "root": source_root}}
    if db_route:
        tgt = {"key": "tgt_profile", "type": "profile.db", "action": "create", "name": "Write",
               "config": {"profile_type": "database.write", "statement_type": "dynamicinsert",
                          "table_name": "t", "fields": fields}}
        components = [
            src, tgt, the_map,
            {"key": "listen_op", "type": "connector-action", "action": "create", "name": "Listen",
             "config": {"connector_type": "wss", "operation_mode": "listen", "object_name": "gen{0}".format(seed),
                        "operation_type": "EXECUTE", "input_type": "singlejson", "output_type": "none",
                        "request_profile": "$ref:src_profile"}},
            {"key": "db_conn", "type": "connector-settings", "action": "create", "name": "DB",
             "config": {"connector_type": "database", "reference_only": True, "component_name": "Existing DB"}},
            {"key": "db_op", "type": "connector-action", "action": "create", "name": "Send",
             "config": {"connector_type": "database", "operation_mode": "send",
                        "write_profile_id": "$ref:tgt_profile", "connection_ref_key": "db_conn"}},
        ]
        source, target = (None, "listen_op"), ("db_conn", "db_op")
        root = _linear_root("listener", source, "Generated", target)
        detail = "schema_summary"
    else:
        tgt = {"key": "tgt_profile", "type": "profile.json", "action": "create", "name": "Tgt",
               "config": {"profile_type": "json.generated", "root": target_root}}
        components = [src, tgt, the_map]
        for role, method in (("src", "GET"), ("tgt", "POST")):
            components.append({"key": role + "_conn", "type": "connector-settings", "action": "create",
                               "name": role, "config": {"connector_type": "rest", "auth": "NONE",
                                                        "base_url": "https://x.example.com"}})
            op = {"connector_type": "rest", "operation_mode": "execute", "method": method, "path": "/v1/r",
                  "connection_ref_key": role + "_conn"}
            op["response_profile_id" if role == "src" else "request_profile_id"] = "$ref:" + role + "_profile"
            components.append({"key": role + "_op", "type": "connector-action", "action": "create",
                               "name": role + " op", "config": op})
        source, target = ("src_conn", "src_op"), ("tgt_conn", "tgt_op")
        root = _linear_root("source", source, "Generated", target)
        detail = "generated_artifacts"
    facts = _linear_facts(source, target, detail, source_conn=source[0] is not None)
    expected = {
        "keys": ["listen" if db_route else "fetch", "transform", "write" if db_route else "send"],
        "ops": [("direct", m["target_path"]) for m in direct] + [("map_function", f["target_path"]) for f in functions],
        "function_inputs": [list(f["inputs"]) for f in functions],
        "source_leaves": sorted(source_leaves),
        "target_leaves": sorted(target_leaves),
        "required_writes": sorted("Fields/" + f["name"] for f in fields if f["mandatory"]) if db_route else None,
        "direct_count": len(direct),
        "db_route": db_route,
    }
    return components, root, facts, expected


def _structural_problems(projection, expected):
    """Compare a projection STRUCTURALLY to the generator's input."""
    problems = []
    flows = [r.model_dump(mode="json", exclude_unset=True) for r in projection.flows]
    if [r["key"] for r in flows] != expected["keys"]:
        return ["row keys {0}".format([r["key"] for r in flows])]
    transform = flows[1]
    ops = [(op["operation_type"], op.get("target_path")) for op in transform["operations"]]
    if ops != expected["ops"]:
        problems.append("operations")
    inputs = [op["inputs"] for op in transform["operations"] if op["operation_type"] == "map_function"]
    if inputs != expected["function_inputs"]:
        problems.append("function inputs")
    if [leaf["path"] for leaf in transform["source_schema"]["leaves"]] != expected["source_leaves"]:
        problems.append("source leaves")
    if expected["db_route"]:
        summary = transform.get("target_write_profile") or {}
        if [p["path"] for p in summary.get("target_paths", ())] != expected["target_leaves"]:
            problems.append("write target paths")
        if sorted(p["path"] for p in summary.get("target_paths", ()) if p["required"]) != expected["required_writes"]:
            problems.append("write required flags")
        if any(k in transform for k in ("direct_field_mappings", "source_profile_generation")):
            problems.append("summary form serves a generation key")
    else:
        if [leaf["path"] for leaf in transform["target_payload_profile"]["leaves"]] != expected["target_leaves"]:
            problems.append("target leaves")
        if len(transform["direct_field_mappings"]) != expected["direct_count"]:
            problems.append("direct mappings")
    return problems


def test_generated_linear_batch_tracks_its_input_and_a_constant_projection_fails():
    """Eight seeded routes vary mapping structure, profile nesting, leaf order
    and map functions; each projection matches its OWN input structurally, and
    a constant projection (seed 0's, or seed 1's for the other route kind)
    fails every other seed's input."""
    seeds = range(8)
    built = {seed: _generated_linear_case(seed) for seed in seeds}
    projections = {}
    for seed, (components, root, facts, expected) in built.items():
        projections[seed] = _linear_project(components, root, facts)
        assert _structural_problems(projections[seed], expected) == [], seed
    constant_failures = 0
    for seed, (_c, _r, _f, expected) in built.items():
        constant = projections[seed % 2]  # same route kind, different input
        if seed in (0, 1):
            continue
        if _structural_problems(constant, expected):
            constant_failures += 1
    assert constant_failures == len(seeds) - 2

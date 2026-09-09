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


def test_a_compiling_intent_plan_hash_is_unmoved_by_the_projection(cases):
    """The pre-#157 hash input: flows empty, no preview kind."""
    from boomi_mcp.authoring.workflow import _plan_hash_preview
    from boomi_mcp.models.integration_models import IntegrationSpecV1

    request = _canonical_case(cases, "database_to_api_sync/wm_off_dlq_off_create")
    result, _ = plan_authoring_request_v1(request, boomi_client=MagicMock(), profile="p")
    preview = result.integration_spec_preview
    assert isinstance(preview, CanonicalIntegrationPreviewV1)
    legacy_shape = IntegrationSpecV1(**{k: v for k, v in preview.model_dump(mode="json").items() if k not in ("flows", "preview_kind")})
    assert _plan_hash_preview(preview) == legacy_shape.model_dump(mode="json")


def test_the_projection_serves_one_typed_row_per_map_node(cases):
    request = _canonical_case(cases, "database_to_api_sync/wm_off_dlq_off_create")
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

"""Fan-out 2.0.0: typed assembly through the surviving primitives (issue #159 S1).

The acceptance witness compares the v2 output — compiled with ZERO prebuilt
connection/operation pairs — against the INDEPENDENT flagship oracle frozen from
the legacy composition path at cb7034b, before ``fanout.py`` changed
(``tests/fixtures/issue_159/flagship_fanout_legacy_spec.json``). Supporting
components are compared on their full ``config`` and ``depends_on``; the process
on its envelope and its canonical root against the legacy arm's root.
"""

from __future__ import annotations

import copy
import json
import subprocess
import sys
import textwrap
from pathlib import Path

import pytest

_TESTS = Path(__file__).resolve().parent
_SRC = _TESTS.parent / "src"
for _path in (str(_SRC), str(_TESTS)):
    if _path not in sys.path:
        sys.path.insert(0, _path)

from _fanout_v2_support import (  # noqa: E402
    RECIPE_ID,
    V2,
    flagship_v2,
    oracle,
    recipe_request,
    v2_from_compose_request,
)

from boomi_mcp.authoring.workflow import (  # noqa: E402
    AuthoringWorkflowError,
    compile_authoring_request_v1,
    plan_authoring_request_v1,
)
from boomi_mcp.authoring.governance import validate_watermark_declaration  # noqa: E402
from boomi_mcp.errors import (  # noqa: E402
    COMPOSITION_COMPONENT_KEY_COLLISION,
    COMPOSITION_CONTRACT_MISMATCH,
    COMPOSITION_UNSUPPORTED_TOPOLOGY,
    GOVERNANCE_WATERMARK_INCONSISTENT,
    RECIPE_CONSTRAINT_FAILED,
    RECIPE_INPUT_INVALID,
)
from boomi_mcp.models.governance_intent import WatermarkDeclarationV1  # noqa: E402
from boomi_mcp.models.integration_models import (  # noqa: E402
    IntegrationComponentSpec,
    IntegrationSpecV1,
)
from boomi_mcp.models.process_ir import canonical_process_ir_json  # noqa: E402
from boomi_mcp.models.recipe_contributions import parse_recipe_contribution  # noqa: E402
from boomi_mcp.recipes import production_registry  # noqa: E402
from boomi_mcp.recipes.builtins import fanout  # noqa: E402
from boomi_mcp.recipes.builtins.fanout import (  # noqa: E402
    ComposeDbRestFanoutInputV2,
    FanoutComponentMaterialV2,
    emit_db_rest_fanout_v2,
    prepare_db_rest_fanout_v2,
)
from boomi_mcp.recipes.errors import RecipeError  # noqa: E402

_COMPONENT_FIELDS = ("key", "type", "action", "name", "config", "depends_on", "component_id")


def _prepare(raw, material, base=()):
    return prepare_db_rest_fanout_v2(
        ComposeDbRestFanoutInputV2.model_validate(raw),
        FanoutComponentMaterialV2.model_validate(material),
        base_catalog={component.key: component for component in base},
    )


def _dump(component):
    data = component.model_dump(mode="json")
    return {field: data[field] for field in _COMPONENT_FIELDS}


def _oracle_spec():
    return IntegrationSpecV1.model_validate(oracle()["integration_spec"])


def _legacy_root(spec):
    # The LEGACY arm's root for the same composition, through the v1 adapter.
    from boomi_mcp.patterns.recipe_bridge import run_fanout_recipe

    process = spec.components[-1]
    result = run_fanout_recipe(recipe_id=RECIPE_ID, components=spec.components, process=process)
    return dict(result.composed.process_roots)[process.key]


def _unit(raw):
    contributions = emit_db_rest_fanout_v2(ComposeDbRestFanoutInputV2.model_validate(raw))
    (unit,) = [c for c in contributions if c.contribution_kind == "process_unit"]
    return unit.unit


# ---------------------------------------------------------------------------
# The acceptance witness
# ---------------------------------------------------------------------------


def test_flagship_supporting_components_match_the_frozen_oracle_with_zero_prebuilt_pairs():
    raw, material = flagship_v2()
    prepared = _prepare(raw, material)  # no base components at all

    expected = [
        {field: component[field] for field in _COMPONENT_FIELDS}
        for component in oracle()["integration_spec"]["components"]
        if component["type"] != "process"
    ]
    assert [_dump(c) for c in prepared.supporting_components] == expected
    assert prepared.selected_base_keys == ()


def test_flagship_root_and_envelope_match_the_legacy_process():
    raw, _material = flagship_v2()
    spec = _oracle_spec()
    process = spec.components[-1]
    unit = _unit(raw)

    assert canonical_process_ir_json(unit.process_ir) == canonical_process_ir_json(
        _legacy_root(spec)
    )
    envelope = unit.envelope
    assert envelope.component_key == process.key
    assert envelope.name == process.name
    assert envelope.action == process.action
    assert set(envelope.depends_on) == set(process.depends_on)
    assert envelope.process_extensions.model_dump(mode="json", exclude_none=True) == (
        process.config["process_extensions"]
    )


def test_flagship_compiles_through_the_public_workflow_with_zero_prebuilt_pairs():
    raw, material = flagship_v2()
    result, internals = compile_authoring_request_v1(recipe_request(raw, material), profile="p")

    assert result.validation_report.is_valid
    spec = internals.normalized.integration_spec
    expected = [
        (c["key"], c["type"])
        for c in oracle()["integration_spec"]["components"]
        if c["type"] != "process"
    ]
    assert [(c.key, c.type) for c in spec.components] == expected
    assert [unit.envelope.component_key for unit in spec.processes] == ["main_process"]
    assert {f.component_key for f in result.artifact_fingerprints} == {"main_process"}


_WITNESS_CHILD = textwrap.dedent(
    """
    import json, sys
    BLOCKED = (
        "boomi_mcp.patterns.archetypes",
        "boomi_mcp.patterns.composition",
        "boomi_mcp.patterns.recipe_bridge",
        "boomi_mcp.compiler.process_ir.legacy_adapters",
    )
    attempts = []

    class _Blocker:
        def find_spec(self, name, path=None, target=None):
            if any(name == b or name.startswith(b + ".") for b in BLOCKED):
                attempts.append(name)
                raise ImportError("blocked: " + name)
            return None

    sys.meta_path.insert(0, _Blocker())   # BEFORE any boomi_mcp import
    sys.path[:0] = sys.argv[1:3]
    from _fanout_v2_support import flagship_v2
    from boomi_mcp.models.process_ir import canonical_process_ir_json
    from boomi_mcp.recipes.builtins.fanout import (
        ComposeDbRestFanoutInputV2, FanoutComponentMaterialV2,
        emit_db_rest_fanout_v2, prepare_db_rest_fanout_v2,
    )
    raw, material = flagship_v2()
    inp = ComposeDbRestFanoutInputV2.model_validate(raw)
    prepared = prepare_db_rest_fanout_v2(
        inp, FanoutComponentMaterialV2.model_validate(material), base_catalog={}
    )
    unit = [c for c in emit_db_rest_fanout_v2(inp) if c.contribution_kind == "process_unit"][0]
    run_attempts = list(attempts)
    control = "not_refused"
    try:
        import boomi_mcp.patterns.composition  # noqa: F401 — the deliberate control
    except ImportError:
        control = "refused"
    print(json.dumps({
        "attempts": run_attempts,
        "control": control,
        "components": [c.model_dump(mode="json") for c in prepared.supporting_components],
        "root": canonical_process_ir_json(unit.unit.process_ir),
    }))
    """
)


def test_the_witness_holds_with_the_legacy_producers_import_blocked():
    completed = subprocess.run(
        [sys.executable, "-c", _WITNESS_CHILD, str(_SRC), str(_TESTS)],
        capture_output=True,
        text=True,
        check=True,
    )
    payload = json.loads(completed.stdout.strip().splitlines()[-1])

    assert payload["attempts"] == []
    # Non-vacuity: the blocker in that child really refuses a legacy producer.
    assert payload["control"] == "refused"
    expected = [c for c in oracle()["integration_spec"]["components"] if c["type"] != "process"]
    assert payload["components"] == expected
    assert payload["root"] == canonical_process_ir_json(_legacy_root(_oracle_spec()))


def test_a_mixed_staged_composition_root_matches_the_legacy_arm():
    from boomi_mcp.patterns.composition import compose_archetypes

    request = copy.deepcopy(oracle()["compose_request"])
    request["parts"].append(copy.deepcopy(request["parts"][3]))
    request["parts"][-1].update({"key": "shipping", "label": "Shipping"})
    links = [
        {"from_part": "db", "to_part": "shape"},
        {"from_part": "shape", "to_part": "orders"},
        {"from_part": "shape", "to_part": "billing", "handoff": {"mode": "document_cache"}},
        {"from_part": "shape", "to_part": "shipping", "handoff": {"mode": "document_cache"}},
    ]
    options = dict(request["options"], links=links)
    legacy_spec = compose_archetypes(request["parts"], options)

    raw, material = flagship_v2()
    raw["targets"].append(dict(raw["targets"][1], part_key="shipping", label="Shipping"))
    material["targets"].append(dict(material["targets"][1], part_key="shipping"))
    raw["links"] = links
    prepared = _prepare(raw, material)
    unit = _unit(raw)

    assert canonical_process_ir_json(unit.process_ir) == canonical_process_ir_json(
        _legacy_root(legacy_spec)
    )
    legacy_supporting = [
        c.model_dump(mode="json") for c in legacy_spec.components if c.type != "process"
    ]
    assert [c.model_dump(mode="json") for c in prepared.supporting_components] == (
        legacy_supporting
    )
    assert set(unit.envelope.depends_on) == set(legacy_spec.components[-1].depends_on)


_REUSE_CASE = (
    _TESTS
    / "fixtures"
    / "governance"
    / "issue_157"
    / "cases"
    / "composition"
    / "reuse_3legs_staged_all"
    / "input.legacy.json"
)


def test_a_folder_placed_reuse_staged_composition_matches_the_live_legacy_path():
    """The frozen #157 ``reuse_3legs_staged_all`` input — reuse by component id,
    bind parameters, ``batch_size``, ``date_format``, every leg staged — plus a
    folder and a literal query parameter, run through BOTH paths."""
    from boomi_mcp.patterns.composition import compose_archetypes

    request = json.loads(_REUSE_CASE.read_text())["input"]
    request["options"]["naming"]["folder_path"] = "Integrations/Fanout"
    request["parts"][2]["parameters"]["send_request"]["query_parameters"] = [
        {"name": "mode", "value_source": "literal", "literal_value": "full"}
    ]
    legacy_spec = compose_archetypes(request["parts"], request["options"])
    legacy_process = legacy_spec.components[-1]

    raw, material = v2_from_compose_request(request)
    prepared = _prepare(raw, material)
    unit = _unit(raw)

    assert [c.model_dump(mode="json") for c in prepared.supporting_components] == [
        c.model_dump(mode="json") for c in legacy_spec.components if c.type != "process"
    ]
    assert canonical_process_ir_json(unit.process_ir) == canonical_process_ir_json(
        _legacy_root(legacy_spec)
    )
    envelope = unit.envelope
    assert envelope.name == legacy_process.name
    assert envelope.folder_name == legacy_process.config["folder_name"]
    assert set(envelope.depends_on) == set(legacy_process.depends_on)
    assert envelope.process_extensions.model_dump(mode="json", exclude_none=True) == (
        legacy_process.config["process_extensions"]
    )
    # And through the public workflow: governance's folder fan-out leaves every
    # component's own placement as the legacy path set it.
    result, internals = compile_authoring_request_v1(recipe_request(raw, material), profile="p")
    assert result.validation_report.is_valid
    (process,) = internals.normalized.integration_spec.processes
    assert process.envelope.folder_name == "Integrations/Fanout"


def test_the_ported_constants_are_pinned_to_the_legacy_composition():
    from boomi_mcp.patterns import composition

    assert fanout._HANDOFF_CACHE_KEY_V2 == composition._HANDOFF_CACHE_KEY
    assert fanout._reserved_prefixes_v2() == composition._RESERVED_PREFIXES
    assert fanout._MAX_TARGETS == composition._MAX_FANOUT_TARGETS
    assert fanout._MAX_CACHE_TARGETS == composition._MAX_FANOUT_TARGETS_WITH_CACHE
    # The route's topology spelling: the legacy arm's served summary, frozen.
    assert fanout._TOPOLOGY_V2 == (
        oracle()["integration_spec"]["validation_rules"]["composition"]["topology"]
    )


# ---------------------------------------------------------------------------
# Registration
# ---------------------------------------------------------------------------


def test_v2_is_registered_beside_v1_and_v1_stays_the_default():
    registry = production_registry()
    assert registry.resolve(RECIPE_ID, None).recipe_version == "1.0.0"
    v2 = registry.resolve(RECIPE_ID, V2)
    assert v2.is_default is False
    assert set(v2.output_types) == {"constraint_requirement", "process_unit", "recorded_intent"}
    assert registry.component_preparation_for(v2) == (
        FanoutComponentMaterialV2,
        prepare_db_rest_fanout_v2,
    )
    assert registry.component_preparation_for(registry.resolve(RECIPE_ID, "1.0.0")) is None


# ---------------------------------------------------------------------------
# The unit contribution and its explicit edges
# ---------------------------------------------------------------------------


def test_the_unit_carries_its_dependency_edges_at_the_one_exempt_position():
    raw, _material = flagship_v2()
    contributions = emit_db_rest_fanout_v2(ComposeDbRestFanoutInputV2.model_validate(raw))
    unit = contributions[0]
    dumped = unit.model_dump(mode="json")
    assert dumped["unit"]["envelope"]["depends_on"]
    assert parse_recipe_contribution(dumped) == unit
    # Non-vacuity: the same key one level away is still refused.
    moved = copy.deepcopy(dumped)
    moved["unit"]["depends_on"] = moved["unit"]["envelope"]["depends_on"]
    with pytest.raises(Exception):
        parse_recipe_contribution(moved)


def test_key_drift_between_preparer_and_executor_is_a_hard_failure():
    raw, material = flagship_v2()
    prepared = _prepare(raw, material)
    from boomi_mcp.recipes.engine import RecipeRequestV1, run_recipes
    from boomi_mcp.recipes.materialization import MaterializationCatalog

    from boomi_mcp.authoring.workflow import _connector_metadata_from_components

    def run(components):
        return run_recipes(
            [RecipeRequestV1(RECIPE_ID, "fanout", raw, V2)],
            catalog=MaterializationCatalog({}),
            connector_metadata=_connector_metadata_from_components(components),
            supporting_components=components,
        )

    # Control: the full prepared set runs clean, so the refusal below is the drift.
    assert run(prepared.supporting_components).process_units
    # The cache-free leg's operation is referenced only by the root and the
    # requirement — dropping it is exactly a preparer/executor key drift.
    dropped = tuple(
        c for c in prepared.supporting_components if c.key != "target_billing_rest_operation"
    )
    with pytest.raises(RecipeError) as caught:
        run(dropped)
    assert caught.value.primary_code == RECIPE_CONSTRAINT_FAILED


# ---------------------------------------------------------------------------
# Refusals before any component is built
# ---------------------------------------------------------------------------


def _flagship_with(edit):
    raw, material = flagship_v2()
    edit(raw, material)
    return raw, material


def _many_targets(count, staged):
    def edit(raw, material):
        raw["targets"] = [
            dict(raw["targets"][1], part_key=f"t{i}", label=f"T{i}") for i in range(count)
        ]
        material["targets"] = [
            dict(material["targets"][1], part_key=f"t{i}") for i in range(count)
        ]
        raw["links"] = [{"from_part": "db", "to_part": "shape"}] + [
            {
                "from_part": "shape",
                "to_part": f"t{i}",
                "handoff": {"mode": "document_cache" if staged else "document_stream"},
            }
            for i in range(count)
        ]

    return edit


_STAR = [
    {"from_part": "db", "to_part": "shape"},
    {"from_part": "shape", "to_part": "orders"},
    {"from_part": "shape", "to_part": "billing"},
]

_REFUSALS = {
    "duplicate_part_key": (
        lambda r, m: r["targets"][1].update(part_key="shape"),
        COMPOSITION_COMPONENT_KEY_COLLISION,
        "/raw_input/targets/1/part_key",
    ),
    "derived_prefix_collision": (
        lambda r, m: (
            r["targets"][1].update(part_key="bill-ing"),
            m["targets"][1].update(part_key="bill-ing"),
            r["targets"].append(dict(r["targets"][1], part_key="bill_ing", label="Other")),
            m["targets"].append(dict(m["targets"][1], part_key="bill_ing")),
        ),
        COMPOSITION_COMPONENT_KEY_COLLISION,
        "/raw_input/targets/2/part_key",
    ),
    "label_collision": (
        lambda r, m: r["targets"][1].update(label="Orders"),
        COMPOSITION_COMPONENT_KEY_COLLISION,
        "/raw_input/targets/1/label",
    ),
    "undeclared_link_endpoint": (
        lambda r, m: r.update(links=[dict(_STAR[0]), {"from_part": "shape", "to_part": "nope"}, dict(_STAR[2])]),
        COMPOSITION_UNSUPPORTED_TOPOLOGY,
        "/raw_input/links/1/to_part",
    ),
    "extra_edge": (
        lambda r, m: r.update(links=_STAR + [{"from_part": "db", "to_part": "orders"}]),
        COMPOSITION_UNSUPPORTED_TOPOLOGY,
        "/raw_input/links",
    ),
    "explicit_empty_links": (
        lambda r, m: r.update(links=[]),
        COMPOSITION_UNSUPPORTED_TOPOLOGY,
        "/raw_input/links",
    ),
    "cache_on_source_edge": (
        lambda r, m: r.update(
            links=[dict(_STAR[0], handoff={"mode": "document_cache"}), _STAR[1], _STAR[2]]
        ),
        COMPOSITION_UNSUPPORTED_TOPOLOGY,
        "/raw_input/links/0/handoff/mode",
    ),
    "keyed_handoff": (
        lambda r, m: r.update(
            links=[_STAR[0], dict(_STAR[1], handoff={"mode": "document_cache", "doc_cache_index": 1}), _STAR[2]]
        ),
        COMPOSITION_UNSUPPORTED_TOPOLOGY,
        "/raw_input/links/1/handoff/doc_cache_index",
    ),
    "cache_budget": (
        _many_targets(25, staged=True),
        COMPOSITION_UNSUPPORTED_TOPOLOGY,
        "/raw_input/links",
    ),
    "unmatched_material_target": (
        lambda r, m: m["targets"][1].update(part_key="ghost"),
        COMPOSITION_CONTRACT_MISMATCH,
        "/component_material/targets/1/part_key",
    ),
    "material_binding_mode_mismatch": (
        lambda r, m: m["targets"][0].update(connection={"mode": "reuse", "component_id": "x"}),
        COMPOSITION_CONTRACT_MISMATCH,
        "/component_material/targets/0/connection",
    ),
    "unknown_source_field": (
        lambda r, m: m["transform"]["operations"][0].update(source_field="nope"),
        COMPOSITION_CONTRACT_MISMATCH,
        "/component_material/transform/operations/0",
    ),
    "required_leaf_unmapped": (
        lambda r, m: (
            m["payload_profile"]["root"]["children"].append(
                {"name": "must", "kind": "simple", "data_type": "character", "required": True}
            )
        ),
        COMPOSITION_CONTRACT_MISMATCH,
        "/component_material/payload_profile",
    ),
    "target_leaf_contract_mismatch": (
        lambda r, m: m["targets"][1].update(
            expected_payload_profile={
                "format": "json",
                "root": {
                    "name": "Root",
                    "kind": "object",
                    "children": [{"name": "target_a", "kind": "simple", "data_type": "number"}],
                },
            }
        ),
        COMPOSITION_CONTRACT_MISMATCH,
        "/component_material/targets/1/expected_payload_profile",
    ),
}


@pytest.mark.parametrize("case", sorted(_REFUSALS))
def test_v2_refuses_with_cause_and_pointer_before_any_component(case, monkeypatch):
    edit, cause, pointer = _REFUSALS[case]
    raw, material = _flagship_with(edit)
    built = []
    rt = fanout._rt()
    for primitive in (
        rt.DbExtractPrimitive,
        rt.FieldMapPrimitive,
        rt.RestSendWithRetryPrimitive,
    ):
        monkeypatch.setattr(
            primitive,
            "emit_components",
            classmethod(lambda cls, *a, **k: built.append(cls) or []),
        )

    with pytest.raises(RecipeError) as caught:
        _prepare(raw, material)

    (diagnostic,) = caught.value.diagnostics
    assert diagnostic.cause_codes == (cause,)
    assert diagnostic.path == pointer
    assert built == []


def test_the_explicit_star_with_streaming_handoffs_matches_the_inferred_star():
    raw, material = flagship_v2()
    inferred = _prepare(raw, material)
    raw["links"] = _STAR
    explicit = _prepare(raw, material)
    assert explicit == inferred
    assert canonical_process_ir_json(_unit(raw).process_ir) == canonical_process_ir_json(
        _unit(flagship_v2()[0]).process_ir
    )


# ---------------------------------------------------------------------------
# The first-target watermark rule
# ---------------------------------------------------------------------------


def _watermarked(first=(), later=(), refs=("since",), declare=True):
    raw, material = flagship_v2()
    if declare:
        raw["watermark"] = {"field": "source_a", "kind": "timestamp", "query_parameter_refs": list(refs)}
    material["targets"][0]["operation"]["query_parameters"] = list(first)
    material["targets"][1]["operation"]["query_parameters"] = list(later)
    return raw, material


_DEFERRED = {"name": "since", "value_source": "watermark"}
_LITERAL = {"name": "since", "value_source": "literal", "literal_value": "x"}


def test_a_first_target_deferred_name_is_recorded_and_never_emitted():
    raw, material = _watermarked(first=[_DEFERRED])
    result, internals = plan_authoring_request_v1(recipe_request(raw, material), profile="p")

    assert result.validation_report.is_valid
    (record,) = result.recorded_intents
    assert record.declaration.query_parameter_refs == ("since",)
    operation = next(
        c for c in internals.normalized.integration_spec.components if c.key == "target_rest_operation"
    )
    assert "query_parameters" not in operation.config


def test_a_declared_watermark_serves_the_conditional_row_before_the_fanout():
    """S1 serves the conditional ``watermark`` row on the v2 route — present
    exactly when the typed declaration is, and placed before ``branch``. None of
    the six frozen composition cases declares one, so this is its witness."""
    declared, _ = plan_authoring_request_v1(
        recipe_request(*_watermarked(first=[_DEFERRED])), profile="p"
    )
    plain, _ = plan_authoring_request_v1(recipe_request(*flagship_v2()), profile="p")

    def keys(result):
        return [row.key for row in result.integration_spec_preview.flows]

    assert keys(plain) == ["extract", "transform", "send", "reliability", "fanout"]
    assert keys(declared) == ["extract", "transform", "send", "reliability", "watermark", "fanout"]
    (row,) = [r for r in declared.integration_spec_preview.flows if r.key == "watermark"]
    assert row.model_dump(mode="json") == {
        "key": "watermark",
        "name": "Advance watermark",
        "source": "send",
        "target": None,
        "operation": "watermark",
        "executable": False,
    }


def test_governance_counts_a_deferred_name_only_on_an_owned_operation():
    """Non-vacuity for the governance channel: without the deferred names the
    same declaration is refused; with them it passes; scoped to the owned op."""
    declaration = WatermarkDeclarationV1(
        source_profile_ref="$ref:profile", field="f", kind="timestamp", query_parameter_refs=("since",)
    )
    operation = IntegrationComponentSpec(
        key="op", type="connector-action", name="Op", config={"connector_type": "rest"}
    )
    profile = IntegrationComponentSpec(
        key="profile",
        type="profile.db",
        name="P",
        config={"output_fields": [{"name": "f", "data_type": "datetime"}]},
    )
    components = {"op": operation, "profile": profile}
    from boomi_mcp.authoring.governance import GovernanceRefusal

    with pytest.raises(GovernanceRefusal):
        validate_watermark_declaration(
            declaration, unit_index=0, owned_keys=("op",), components_by_key=components
        )
    validate_watermark_declaration(
        declaration,
        unit_index=0,
        owned_keys=("op",),
        components_by_key=components,
        deferred_query_parameter_names={"op": ("since",)},
    )
    with pytest.raises(GovernanceRefusal):
        validate_watermark_declaration(
            declaration,
            unit_index=0,
            owned_keys=(),
            components_by_key=components,
            deferred_query_parameter_names={"op": ("since",)},
        )


@pytest.mark.parametrize(
    "first, later, refs, declare, cause, pointer",
    [
        pytest.param([], [], ("since",), True, GOVERNANCE_WATERMARK_INCONSISTENT,
                     "/raw_input/watermark/query_parameter_refs/0", id="undeclared_name"),
        pytest.param([], [_LITERAL], ("since",), True, GOVERNANCE_WATERMARK_INCONSISTENT,
                     "/raw_input/watermark/query_parameter_refs/0", id="declared_only_on_a_later_target"),
        pytest.param([_DEFERRED], [dict(_DEFERRED, name="other")], ("since",), True,
                     COMPOSITION_UNSUPPORTED_TOPOLOGY,
                     "/component_material/targets/1/operation/query_parameters/0",
                     id="later_target_watermark_binding"),
        pytest.param([_DEFERRED], [], (), False, GOVERNANCE_WATERMARK_INCONSISTENT,
                     "/component_material/targets/0/operation/query_parameters",
                     id="deferred_name_without_a_declaration"),
        pytest.param([_DEFERRED, dict(_DEFERRED, name="until")], [], ("since",), True,
                     GOVERNANCE_WATERMARK_INCONSISTENT,
                     "/component_material/targets/0/operation/query_parameters/1",
                     id="deferred_name_the_declaration_omits"),
    ],
)
def test_the_watermark_rule_refusals(first, later, refs, declare, cause, pointer):
    raw, material = _watermarked(first=first, later=later, refs=refs, declare=declare)
    with pytest.raises(RecipeError) as caught:
        _prepare(raw, material)
    (diagnostic,) = caught.value.diagnostics
    assert diagnostic.cause_codes == (cause,)
    assert diagnostic.path == pointer


# ---------------------------------------------------------------------------
# Transform routes and bindings
# ---------------------------------------------------------------------------


def _reuse_with_function():
    raw, material = flagship_v2()
    raw["source"] = {"part_key": "db", "binding_mode": "reuse"}
    for target in raw["targets"]:
        target["binding_mode"] = "reuse"
    material["source"]["connection"] = {"mode": "reuse", "component_id": "db-conn-id"}
    for target in material["targets"]:
        target["connection"] = {"mode": "reuse", "component_id": "rest-conn-id"}
    material["source"]["read"]["result_fields"][1]["data_type"] = "datetime"
    material["payload_profile"]["root"]["children"][1]["data_type"] = "datetime"
    material["transform"]["operations"][1] = {
        "operation_type": "map_function",
        "function_type": "date_format",
        "inputs": ["source_b"],
        "target_path": "Root/target_b",
        "parameters": [
            {"name": "input_format", "value": "yyyy-MM-dd"},
            {"name": "output_format", "value": "dd/MM/yyyy"},
        ],
    }
    return raw, material


def test_a_reuse_composition_with_date_format_uses_the_function_route():
    raw, material = _reuse_with_function()
    prepared = _prepare(raw, material)
    by_key = {c.key: c for c in prepared.supporting_components}

    assert by_key["source_db_connection"].config["reference_only"] is True
    assert by_key["target_rest_connection"].component_id == "rest-conn-id"
    the_map = by_key["transform_transform_map"].config
    assert the_map["map_type"] == "function"
    assert the_map["function_mappings"][0]["parameters"] == {
        "input_format": "yyyy-MM-dd",
        "output_format": "dd/MM/yyyy",
    }
    unit = _unit(raw)
    declared = {c.connection_id: c for c in unit.envelope.process_extensions.connections}
    # Reused REST connections declare their credential override points.
    assert "$ref:target_billing_rest_connection" in declared
    result, _ = compile_authoring_request_v1(recipe_request(raw, material), profile="p")
    assert result.validation_report.is_valid


@pytest.mark.parametrize(
    "operation",
    [
        pytest.param(
            {"function_type": "dynamic_process_property_set", "parameters": []},
            id="side_effecting_family",
        ),
        pytest.param({"parameters": [{"name": "input_format", "value": "x"}]}, id="required_parameter_missing"),
        pytest.param({"parameters": [{"name": "input_format", "value": "x"}, {"name": "output_format", "value": "y"}, {"name": "bogus", "value": "z"}]}, id="undeclared_parameter"),
        pytest.param({"parameters": [{"name": "input_format", "value": {"nested": "x"}}, {"name": "output_format", "value": "y"}]}, id="non_scalar_value"),
    ],
)
def test_map_function_material_is_closed_against_the_family_authority(operation):
    from boomi_mcp.categories.components.builders.map_function_registry import (
        FUNCTION_FAMILIES,
    )

    if "function_type" in operation:
        # The family is REAL and registered — refused for its effect, not its name.
        assert FUNCTION_FAMILIES[operation["function_type"]].effect_kind != "pure"
    raw, material = _reuse_with_function()
    material["transform"]["operations"][1].update(operation)
    with pytest.raises(Exception) as caught:
        FanoutComponentMaterialV2.model_validate(material)
    assert "ValidationError" in type(caught.value).__name__


def test_a_map_script_references_a_base_script_component():
    raw, material = flagship_v2()
    material["transform"]["operations"][1] = {
        "operation_type": "map_script",
        "script_component_key": "shared_script",
        "inputs": ["source_b"],
        "outputs": ["Root/target_b"],
    }
    script = IntegrationComponentSpec(
        key="shared_script",
        type="script.mapping",
        name="Shared Script",
        config={
            "component_type": "script.mapping",
            "component_name": "Shared Script",
            "language": "groovy2",
            "script_body": "target_b = source_b",
            "inputs": [{"name": "source_b", "data_type": "character"}],
            "outputs": [{"name": "target_b"}],
        },
    )
    prepared = _prepare(raw, material, base=(script,))
    the_map = next(c for c in prepared.supporting_components if c.type == "transform.map")
    assert the_map.config["map_type"] == "script"
    assert the_map.config["script_mappings"][0]["script_component_id"] == "$ref:shared_script"
    assert "shared_script" in the_map.depends_on
    assert prepared.selected_base_keys == ("shared_script",)

    with pytest.raises(RecipeError) as caught:
        _prepare(raw, material)  # the script is not a base component
    assert caught.value.diagnostics[0].path == (
        "/component_material/transform/operations/1/script_component_key"
    )


def _base_connections():
    db = IntegrationComponentSpec(
        key="shared_db",
        type="connector-settings",
        name="Shared DB",
        component_id="db-id",
        config={"reference_only": True, "connector_type": "database", "component_id": "db-id"},
    )
    rest = IntegrationComponentSpec(
        key="shared_rest",
        type="connector-settings",
        name="Shared REST",
        component_id="rest-id",
        config={"reference_only": True, "connector_type": "rest", "component_id": "rest-id"},
    )
    return db, rest


def _namespaced(namespace, process_key, name):
    raw, material = flagship_v2()
    raw.update(key_namespace=namespace, process_key=process_key)
    raw["naming"]["process_name"] = name
    raw["source"] = {"part_key": "db", "binding_mode": "reuse", "connection_key": "shared_db"}
    for target in raw["targets"]:
        target.update(binding_mode="reuse", connection_key="shared_rest")
    material["source"].pop("connection")
    for target in material["targets"]:
        target.pop("connection")
    return raw, material


def test_two_invocations_share_base_connections_and_own_their_generated_pairs():
    db, rest = _base_connections()
    raw_a, material_a = _namespaced("a", "process_a", "Process A")
    raw_b, material_b = _namespaced("b", "process_b", "Process B")
    request = recipe_request(
        raw_a,
        material_a,
        base_components=[db.model_dump(mode="json"), rest.model_dump(mode="json")],
        invocation_id="a",
        extra_invocations=[
            {
                "recipe_id": RECIPE_ID,
                "recipe_version": V2,
                "invocation_id": "b",
                "raw_input": raw_b,
                "component_material": material_b,
            }
        ],
    )
    result, internals = compile_authoring_request_v1(request, profile="p")

    assert result.validation_report.is_valid
    spec = internals.normalized.integration_spec
    keys = [c.key for c in spec.components]
    assert keys.count("shared_db") == 1 and keys.count("shared_rest") == 1
    assert {"a_source_db_get_operation", "b_source_db_get_operation"} <= set(keys)
    assert "a_target_rest_connection" not in keys  # base-bound: never generated
    operation = next(c for c in spec.components if c.key == "a_target_rest_operation")
    assert operation.config["connection_ref_key"] == "shared_rest"
    assert sorted(u.envelope.component_key for u in spec.processes) == ["process_a", "process_b"]
    assert [r.invocation_id for r in result.recipe_provenance] == ["a", "b"]


def test_a_base_key_must_name_a_matching_base_component():
    raw, material = _namespaced("a", "process_a", "Process A")
    db, _rest = _base_connections()
    with pytest.raises(RecipeError) as caught:
        _prepare(raw, material, base=(db,))
    assert caught.value.diagnostics[0].path == "/raw_input/targets/0/connection_key"
    assert caught.value.diagnostics[0].cause_codes == (COMPOSITION_CONTRACT_MISMATCH,)


def test_a_builder_refusal_is_value_free_and_pointed():
    """The DB read-profile BUILDER (not a contract check) refuses a blank
    statement; its own code travels as the cause, the pointer names the part."""
    raw, material = flagship_v2()
    material["source"]["read"]["sql"] = "   "
    with pytest.raises(RecipeError) as caught:
        _prepare(raw, material)
    (diagnostic,) = caught.value.diagnostics
    assert diagnostic.code == RECIPE_CONSTRAINT_FAILED
    assert diagnostic.cause_codes == ("MISSING_DB_QUERY",)
    assert diagnostic.path == "/component_material/source"

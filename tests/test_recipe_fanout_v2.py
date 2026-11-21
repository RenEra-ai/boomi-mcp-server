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
    respell_oracle_credential_refs,
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

    # The ONE re-spelling the v2 split applies (a legacy credential_ref under
    # the opaque `credential://` scheme, CDX-r1-02) is applied to the expected
    # side too; every other byte of every config is compared as frozen.
    expected = [
        {field: component[field] for field in _COMPONENT_FIELDS}
        for component in respell_oracle_credential_refs(oracle()["integration_spec"]["components"])
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
    expected = [
        c
        for c in respell_oracle_credential_refs(oracle()["integration_spec"]["components"])
        if c["type"] != "process"
    ]
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
        c
        for c in respell_oracle_credential_refs(
            [c.model_dump(mode="json") for c in legacy_spec.components]
        )
        if c["type"] != "process"
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
#: A LITERAL under a published name (D12): ``since`` itself is never a literal.
_LITERAL = {"name": "limit", "value_source": "literal", "literal_value": "x"}


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
    # ARCH-r1-04: keyed by (owning root, operation).
    validate_watermark_declaration(
        declaration,
        unit_index=0,
        owned_keys=("op",),
        components_by_key=components,
        deferred_query_parameter_names={("root", "op"): ("since",)},
        process_key="root",
    )
    with pytest.raises(GovernanceRefusal):
        validate_watermark_declaration(
            declaration,
            unit_index=0,
            owned_keys=(),
            components_by_key=components,
            deferred_query_parameter_names={("root", "op"): ("since",)},
            process_key="root",
        )
    with pytest.raises(GovernanceRefusal):  # another root reaching the same op
        validate_watermark_declaration(
            declaration,
            unit_index=0,
            owned_keys=("op",),
            components_by_key=components,
            deferred_query_parameter_names={("root", "op"): ("since",)},
            process_key="other_root",
        )


@pytest.mark.parametrize(
    "first, later, refs, declare, cause, pointer",
    [
        pytest.param([], [], ("since",), True, GOVERNANCE_WATERMARK_INCONSISTENT,
                     "/raw_input/watermark/query_parameter_refs/0", id="undeclared_name"),
        pytest.param([], [_LITERAL], ("limit",), True, GOVERNANCE_WATERMARK_INCONSISTENT,
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
    """The DB connection BUILDER (not a contract check) refuses an auth mode the
    neutral contract admits but the builder defers; its own code travels as the
    cause, the pointer names the part. (A blank statement used to be this
    witness; since CDX-r1-05 the read is validated by its ``DbReadOperation``
    authority first, exactly as the legacy route does.)"""
    raw, material = flagship_v2()
    raw["source"]["auth_mode"] = "windows_integrated"
    material["source"]["connection"].pop("username")
    material["source"]["connection"].pop("credential_ref")
    with pytest.raises(RecipeError) as caught:
        _prepare(raw, material)
    (diagnostic,) = caught.value.diagnostics
    assert diagnostic.code == RECIPE_CONSTRAINT_FAILED
    assert diagnostic.cause_codes == ("UNSUPPORTED_DB_AUTH_MODE",)
    assert diagnostic.path == "/component_material/source"


def test_a_blank_statement_is_refused_by_the_read_authority_like_the_legacy_route():
    raw, material = flagship_v2()
    material["source"]["read"]["sql"] = "   "
    with pytest.raises(RecipeError) as caught:
        _prepare(raw, material)
    (diagnostic,) = caught.value.diagnostics
    assert diagnostic.code == RECIPE_INPUT_INVALID
    assert diagnostic.path == "/component_material/source/read"


# ---------------------------------------------------------------------------
# QA-159-S1-r1: the SERVED wire omits what the legacy producer omits (C8), and
# the process step label / transform row name are independently reachable
# ---------------------------------------------------------------------------


def _served_plan(raw, material):
    """The envelope ``build_integration(action='plan')`` serves — the tool dump."""
    from unittest import mock

    from boomi_mcp.categories import integration_builder

    payload = {
        "contract_version": "2",
        "intent": {
            "intent_kind": "recipe",
            "integration_name": "r1 wire",
            "invocations": [
                {
                    "recipe_id": RECIPE_ID,
                    "recipe_version": V2,
                    "invocation_id": "fanout",
                    "raw_input": raw,
                    "component_material": material,
                }
            ],
        },
    }
    with mock.patch.object(integration_builder, "paginate_metadata", lambda *a, **k: []):
        envelope = integration_builder.build_integration_action(
            mock.MagicMock(), "p", "plan", {"authoring_request": payload}
        )
    assert envelope["_success"] is True, envelope.get("error")
    return envelope["authoring_result"]["integration_spec_preview"]


def test_the_served_wire_omits_the_keys_the_legacy_producer_omits():
    """Compared on the SERVED dump (never an ``exclude_unset`` projection, which
    the tool does not serve): exact key sets, and the full rows, against the
    frozen legacy oracle."""
    raw, material = flagship_v2()
    preview = _served_plan(raw, material)
    legacy = oracle()["integration_spec"]
    (summary,) = preview["composition_summaries"].values()
    expected = legacy["validation_rules"]["composition"]
    assert set(summary) == set(expected)
    assert [set(leg) for leg in summary["fanout_legs"]] == [
        set(leg) for leg in expected["fanout_legs"]
    ]
    assert summary == expected
    assert preview["endpoints"] == legacy["endpoints"]
    assert [set(row) for row in preview["endpoints"]] == [set(row) for row in legacy["endpoints"]]
    # No explicit null survives on the new surfaces unless the projector set it.
    assert not [
        key
        for row in (*preview["endpoints"], summary, *summary["fanout_legs"])
        for key, value in row.items()
        if value is None
    ]


def test_the_omission_follows_the_models_set_state_not_a_key_list():
    from boomi_mcp.models.derived_flows import DerivedEndpointRowV1, DerivedFanoutLegV1

    base = dict(key="k", type="rest", direction="target", binding_mode="create")
    assert "method" not in DerivedEndpointRowV1(**base).model_dump(mode="json")
    # Non-vacuity: the same key, SET to None, is served as null.
    assert DerivedEndpointRowV1(**base, method=None).model_dump(mode="json")["method"] is None
    leg = dict(part_key="p", leg=2, binding_mode="reuse")
    assert "handoff" not in DerivedFanoutLegV1(**leg).model_dump(mode="json")
    assert DerivedFanoutLegV1(**leg, handoff="document_cache").model_dump(mode="json")[
        "handoff"
    ] == "document_cache"
    # A round trip through the served wire keeps the unset keys unset.
    served = DerivedEndpointRowV1(**base).model_dump(mode="json")
    assert DerivedEndpointRowV1.model_validate(served).model_dump(mode="json") == served


def test_the_step_label_and_the_row_name_are_pinned_to_the_legacy_pair():
    (legacy_process,) = [
        c for c in oracle()["integration_spec"]["components"] if c["type"] == "process"
    ]
    step_labels = {
        step["label"]
        for step in legacy_process["config"]["flow_sequence"]
        if step.get("kind") == "map_ref"
    }
    assert step_labels == {fanout._MAP_LABEL_V2}
    (legacy_row,) = [f for f in oracle()["integration_spec"]["flows"] if f["key"] == "transform"]
    assert legacy_row["name"] == fanout._TRANSFORM_ROW_NAME_V2


def _served_pair(raw, material):
    from boomi_mcp.authoring.workflow import compile_authoring_request_v1

    preview = _served_plan(raw, material)
    (row,) = [f for f in preview["flows"] if f["key"] == "transform"]
    result, internals = compile_authoring_request_v1(recipe_request(raw, material), profile="p")
    (process,) = internals.normalized.integration_spec.processes
    (step,) = [s for s in process.process_ir.body.steps if s.kind == "map_ref"]
    return step.label, row["name"]


def test_one_request_reproduces_both_the_legacy_step_label_and_row_name():
    legacy_step = fanout._MAP_LABEL_V2
    legacy_row = oracle()["integration_spec"]["flows"][1]["name"]
    raw, material = flagship_v2()
    # Defaults reach the legacy pair.
    assert _served_pair(raw, material) == (legacy_step, legacy_row)
    # A custom name moves ONLY the row; the step label stays the legacy constant.
    raw["transform"]["map_label"] = "Custom row name"
    assert _served_pair(raw, material) == (legacy_step, "Custom row name")
    # The frozen #157 spelling reproduces the legacy pair explicitly.
    raw["transform"]["map_label"] = legacy_row
    assert _served_pair(raw, material) == (legacy_step, legacy_row)


# ---------------------------------------------------------------------------
# CDX-r1-04: a create-mode base connection must carry the declared auth
# ---------------------------------------------------------------------------


def _create_base_db(auth_mode="username_password"):
    config = {
        "connector_type": "database",
        "component_name": "Shared DB",
        "driver_id": "microsoft_jdbc",
        "host": "db.example.invalid",
        "dbname": "AppDB",
        "username": "svc",
        "credential_ref": "credential://vault/db",
    }
    if auth_mode is not None:
        config["auth_mode"] = auth_mode
    return IntegrationComponentSpec(
        key="shared_db", type="connector-settings", name="Shared DB", config=config
    )


def _create_bound_source(declared="username_password"):
    raw, material = flagship_v2()
    raw["source"] = {
        "part_key": raw["source"]["part_key"],
        "binding_mode": "create",
        "auth_mode": declared,
        "connection_key": "shared_db",
    }
    material["source"].pop("connection")
    return raw, material


def test_a_create_base_db_connection_with_the_declared_auth_binds():
    raw, material = _create_bound_source()
    prepared = _prepare(raw, material, base=(_create_base_db(),))
    assert prepared.selected_base_keys[0] == "shared_db"


@pytest.mark.parametrize(
    "base_auth", [pytest.param("windows_integrated", id="mismatch"), pytest.param(None, id="absent")]
)
def test_a_create_base_db_connection_with_another_auth_is_refused(base_auth):
    raw, material = _create_bound_source()
    with pytest.raises(RecipeError) as caught:
        _prepare(raw, material, base=(_create_base_db(base_auth),))
    (diagnostic,) = caught.value.diagnostics
    assert diagnostic.path == "/raw_input/source/auth_mode"
    assert diagnostic.cause_codes == (COMPOSITION_CONTRACT_MISMATCH,)


@pytest.mark.parametrize("auth, refused", [("NONE", False), ("BASIC", True), (None, True)])
def test_a_create_base_rest_connection_must_be_unauthenticated(auth, refused):
    """The sibling: a create-mode REST target declares NO credential override
    points, so a base connection it binds must be the unauthenticated one."""
    raw, material = flagship_v2()
    config = {"connector_type": "rest", "base_url": "https://billing.example.invalid"}
    if auth is not None:
        config["auth"] = auth
    base = IntegrationComponentSpec(
        key="shared_rest", type="connector-settings", name="Shared REST", config=config
    )
    raw["targets"][1]["connection_key"] = "shared_rest"
    material["targets"][1].pop("connection")
    if not refused:
        assert "shared_rest" in _prepare(raw, material, base=(base,)).selected_base_keys
        return
    with pytest.raises(RecipeError) as caught:
        _prepare(raw, material, base=(base,))
    (diagnostic,) = caught.value.diagnostics
    assert diagnostic.path == "/raw_input/targets/1/connection_key"
    assert diagnostic.cause_codes == (COMPOSITION_CONTRACT_MISMATCH,)


# ---------------------------------------------------------------------------
# CDX-r1-05: emission and matching read the AUTHORITY-validated values
# ---------------------------------------------------------------------------


def _padded(material):
    padded = copy.deepcopy(material)
    connection = padded["source"]["connection"]
    for key in ("host", "database", "username", "credential_ref"):
        connection[key] = "  " + connection[key] + " "
    padded["source"]["read"]["sql"] = "\n " + padded["source"]["read"]["sql"] + "  "
    for target in padded["targets"]:
        target["connection"]["base_url"] = " " + target["connection"]["base_url"] + "\t"
        target["operation"]["path"] = "  " + target["operation"]["path"] + " "
    padded["targets"][1]["operation"]["query_parameters"] = [
        {"name": "  mode ", "value_source": "literal", "literal_value": " full  "}
    ]
    return padded


def test_whitespace_padded_material_emits_exactly_what_the_trimmed_material_emits():
    raw, material = flagship_v2()
    trimmed = copy.deepcopy(material)
    trimmed["targets"][1]["operation"]["query_parameters"] = [
        {"name": "mode", "value_source": "literal", "literal_value": "full"}
    ]
    padded = [_dump(c) for c in _prepare(raw, _padded(material)).supporting_components]
    assert padded == [_dump(c) for c in _prepare(raw, trimmed).supporting_components]
    # Non-vacuity: the padding really reached the material the preparer read.
    assert _padded(material)["targets"][0]["operation"]["path"] != trimmed["targets"][0]["operation"]["path"]


def test_whitespace_padded_flagship_still_matches_the_frozen_legacy_oracle():
    raw, material = flagship_v2()
    padded = _padded(material)
    padded["targets"][1]["operation"]["query_parameters"] = []
    expected = [
        {field: component[field] for field in _COMPONENT_FIELDS}
        for component in respell_oracle_credential_refs(oracle()["integration_spec"]["components"])
        if component["type"] != "process"
    ]
    assert [_dump(c) for c in _prepare(raw, padded).supporting_components] == expected


def test_a_padded_watermark_name_matches_its_declared_ref_after_validation():
    raw, material = _watermarked(first=[{"name": " since ", "value_source": "watermark"}])
    prepared = _prepare(raw, material)
    assert prepared.deferred_query_parameter_names == (
        (("main_process", "target_rest_operation"), ("since",)),
    )


# ---------------------------------------------------------------------------
# CDX-r1-06: RECIPE_LEG_PROVENANCE, one record per target leg at its ACTUAL ordinal
# ---------------------------------------------------------------------------


def _leg_records(result):
    return [
        (d.invocation_id, d.recipe_id, d.recipe_version, d.process_key, d.ordinal, d.part_key)
        for d in result.recipe_diagnostics
        if d.code == "RECIPE_LEG_PROVENANCE"
    ]


def test_leg_provenance_on_the_streamed_flagship():
    result, _ = plan_authoring_request_v1(recipe_request(*flagship_v2()), profile="p")
    assert _leg_records(result) == [
        ("fanout", RECIPE_ID, V2, "main_process", 1, "orders"),
        ("fanout", RECIPE_ID, V2, "main_process", 2, "billing"),
    ]


def test_leg_provenance_on_a_staged_flagship_is_shifted_by_the_staging_leg():
    raw, material = flagship_v2()
    raw["links"] = [
        {"from_part": "db", "to_part": "shape"},
        {"from_part": "shape", "to_part": "orders"},
        {"from_part": "shape", "to_part": "billing", "handoff": {"mode": "document_cache"}},
    ]
    result, _ = plan_authoring_request_v1(recipe_request(raw, material), profile="p")
    assert _leg_records(result) == [
        ("fanout", RECIPE_ID, V2, "main_process", 1, "orders"),
        ("fanout", RECIPE_ID, V2, "main_process", 3, "billing"),
    ]
    # The SAME ordinal the served composition summary carries for that leg.
    summary = result.integration_spec_preview.composition_summaries["main_process"]
    assert summary.staging_leg == 2
    assert [(leg.part_key, leg.leg) for leg in summary.fanout_legs] == [("billing", 3)]


def test_leg_provenance_is_absent_without_recipe_assembled_branch_facts():
    """A run whose units carry no trusted part facts — fan-out 1.0.0's lifted
    root, every direct intent — gets no records: the same trigger the served
    route rows use. Measured by withholding the v2 facts from the SAME run, so
    the control is the flagship that does produce them."""
    from boomi_mcp.authoring import workflow

    request = recipe_request(*flagship_v2())
    with_facts, _ = plan_authoring_request_v1(request, profile="p")
    assert _leg_records(with_facts)

    real = workflow._prepare_recipe_components

    def without_facts(*args, **kwargs):
        specs, deferred, _facts = real(*args, **kwargs)
        return specs, deferred, {}

    import pytest as _pytest

    with _pytest.MonkeyPatch.context() as patch:
        patch.setattr(workflow, "_prepare_recipe_components", without_facts)
        bare, _ = plan_authoring_request_v1(request, profile="p")
    assert bare.recipe_diagnostics == ()


def test_a_direct_intent_carries_no_leg_provenance():
    from boomi_mcp.models.authoring_workflow import AuthoringRequestV1

    case = (
        _TESTS / "fixtures" / "governance" / "issue_157" / "cases" / "database_to_api_sync"
        / "wm_off_dlq_off_create" / "input.canonical.json"
    )
    request = AuthoringRequestV1.model_validate(json.loads(case.read_text()))
    result, _ = plan_authoring_request_v1(request, profile="p")
    assert result.recipe_diagnostics == () and result.recipe_provenance == ()


def test_leg_provenance_feeds_no_plan_or_compile_hash(monkeypatch):
    from boomi_mcp.authoring import workflow

    request = recipe_request(*flagship_v2())
    with_records, _ = compile_authoring_request_v1(request, profile="p")
    monkeypatch.setattr(workflow, "_leg_provenance_records", lambda *a, **k: ())
    without, _ = compile_authoring_request_v1(request, profile="p")
    assert _leg_records(with_records) and not _leg_records(without)
    bound, unbound = with_records.revision_binding, without.revision_binding
    assert bound.plan_hash and bound.compile_hash
    assert (bound.semantic_hash, bound.plan_hash, bound.compile_hash) == (
        unbound.semantic_hash,
        unbound.plan_hash,
        unbound.compile_hash,
    )
    assert with_records.normalized_intent_digest == without.normalized_intent_digest


# ---------------------------------------------------------------------------
# CDX-r1-07: multi-route previews scope row and endpoint keys by process
# ---------------------------------------------------------------------------


def _two_route_preview():
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
    result, _ = plan_authoring_request_v1(request, profile="p")
    return result


def test_two_routes_serve_unique_keys_and_every_reference_resolves_in_its_process():
    result = _two_route_preview()
    preview = result.integration_spec_preview
    rows = [row.model_dump(mode="json") for row in preview.flows]
    endpoints = [endpoint.model_dump(mode="json") for endpoint in preview.endpoints]
    keys = [row["key"] for row in rows] + [endpoint["key"] for endpoint in endpoints]
    assert len(keys) == len(set(keys))
    served = set(keys)
    references = 0
    for row in rows:
        owner = row["key"].split("/", 1)[0]
        assert owner in {"process_a", "process_b"}
        for field in ("source", "target"):
            value = row[field]
            if value in (None, "", "dlq"):
                continue
            references += 1
            assert value in served, (row["key"], field, value)
            assert value.split("/", 1)[0] == owner, (row["key"], field, value)
    assert references >= 8  # non-vacuity: transform, send, reliability, fanout x2
    assert set(preview.composition_summaries) == {"process_a", "process_b"}
    # Leg provenance maps each process to ITS invocation.
    assert {(d.process_key, d.invocation_id) for d in result.recipe_diagnostics} == {
        ("process_a", "a"),
        ("process_b", "b"),
    }


def test_a_single_route_keeps_the_unqualified_wire():
    result, _ = plan_authoring_request_v1(recipe_request(*flagship_v2()), profile="p")
    preview = result.integration_spec_preview
    assert [row.key for row in preview.flows] == ["extract", "transform", "send", "reliability", "fanout"]
    assert [endpoint.key for endpoint in preview.endpoints] == [
        "db_source",
        "rest_target",
        "rest_target_billing",
    ]


# ---------------------------------------------------------------------------
# CDX-r2-01: duplicate NORMALIZED query-parameter names are refused
# ---------------------------------------------------------------------------


@pytest.mark.parametrize(
    "second",
    [
        pytest.param({"name": " mode ", "value_source": "literal", "literal_value": "x"}, id="padded"),
        pytest.param({"name": "mode", "value_source": "literal", "literal_value": "x"}, id="exact"),
    ],
)
def test_a_repeated_normalized_query_name_is_refused_at_the_second_entry(second):
    raw, material = flagship_v2()
    material["targets"][1]["operation"]["query_parameters"] = [
        {"name": "mode", "value_source": "literal", "literal_value": "full"},
        second,
    ]
    with pytest.raises(RecipeError) as caught:
        _prepare(raw, material)
    (diagnostic,) = caught.value.diagnostics
    assert diagnostic.path == "/component_material/targets/1/operation/query_parameters/1"
    assert diagnostic.cause_codes == (COMPOSITION_CONTRACT_MISMATCH,)


def test_a_watermark_name_repeating_a_literal_name_is_refused():
    # D12: the literal uses a PUBLISHED name, so it reaches the duplicate check.
    raw, material = _watermarked(
        first=[
            {"name": "limit", "value_source": "watermark"},
            {"name": " limit", "value_source": "literal", "literal_value": "x"},
        ],
        refs=("limit",),
    )
    with pytest.raises(RecipeError) as caught:
        _prepare(raw, material)
    assert caught.value.diagnostics[0].path == (
        "/component_material/targets/0/operation/query_parameters/1"
    )


# ---------------------------------------------------------------------------
# CDX-r2-03: a raw-XML base connection's auth cannot be verified -> refused
# ---------------------------------------------------------------------------


def test_a_create_base_db_connection_submitted_as_raw_xml_is_refused():
    raw, material = _create_bound_source()
    base = _create_base_db()
    base = base.model_copy(update={"config": dict(base.config, xml="<Component/>")})
    with pytest.raises(RecipeError) as caught:
        _prepare(raw, material, base=(base,))
    (diagnostic,) = caught.value.diagnostics
    assert diagnostic.path == "/raw_input/source/auth_mode"
    assert diagnostic.cause_codes == (COMPOSITION_CONTRACT_MISMATCH,)
    # Control: the same structured base without the XML binds.
    assert _prepare(raw, material, base=(_create_base_db(),)).selected_base_keys[0] == "shared_db"


def test_a_create_base_rest_connection_submitted_as_raw_xml_is_refused():
    raw, material = flagship_v2()
    config = {"connector_type": "rest", "base_url": "https://b.example.invalid", "auth": "NONE"}
    raw["targets"][1]["connection_key"] = "shared_rest"
    material["targets"][1].pop("connection")
    structured = IntegrationComponentSpec(
        key="shared_rest", type="connector-settings", name="Shared REST", config=config
    )
    assert "shared_rest" in _prepare(raw, material, base=(structured,)).selected_base_keys
    xml_base = structured.model_copy(update={"config": dict(config, xml="<Component/>")})
    with pytest.raises(RecipeError) as caught:
        _prepare(raw, material, base=(xml_base,))
    assert caught.value.diagnostics[0].path == "/raw_input/targets/1/connection_key"


# ---------------------------------------------------------------------------
# ARCH-r1 (architect implementation review, eval 1): witnesses. Each was run
# against the unmodified c49bce8 tree first and failed by assertion there.
# ---------------------------------------------------------------------------

_SELECTED_POINTER_T1 = "/raw_input/targets/1/connection_key"


def _rest_base(config, *, action="create", name="Shared REST", component_id=None):
    return IntegrationComponentSpec(
        key="shared_rest",
        type="connector-settings",
        name=name,
        action=action,
        component_id=component_id,
        config=config,
    )


def _bind_target_1(raw, material, binding_mode):
    raw["targets"][1]["binding_mode"] = binding_mode
    raw["targets"][1]["connection_key"] = "shared_rest"
    material["targets"][1].pop("connection")


def test_arch_r1_01_a_written_base_cannot_be_bound_as_reuse():
    """ARCH-r1-01: ``action="update"`` + string ``reference_only: "true"`` is
    WRITTEN by apply, so it is not a reuse; its unchecked URL would be written."""
    raw, material = flagship_v2()
    _bind_target_1(raw, material, "reuse")
    base = _rest_base(
        {
            "reference_only": "true",
            "connector_type": "rest",
            "base_url": "https://b.example.invalid/?zz_unknown=1",
            "auth": "NONE",
        },
        action="update",
        component_id="rest-id",
    )
    with pytest.raises(RecipeError) as caught:
        _prepare(raw, material, base=(base,))
    (diagnostic,) = caught.value.diagnostics
    assert diagnostic.path == _SELECTED_POINTER_T1
    assert diagnostic.cause_codes == (COMPOSITION_CONTRACT_MISMATCH,)


def test_arch_r1_01_a_create_base_apply_would_reuse_is_not_a_create_binding():
    """A create-mode base carrying a component_id is REUSED under policy
    ``reuse``: its structured auth/URL describe nothing written — refused."""
    raw, material = flagship_v2()
    _bind_target_1(raw, material, "create")
    base = _rest_base(
        {"connector_type": "rest", "base_url": "https://b.example.invalid", "auth": "NONE"},
        component_id="rest-id",
    )
    with pytest.raises(RecipeError) as caught:
        _prepare(raw, material, base=(base,))
    assert caught.value.diagnostics[0].path == _SELECTED_POINTER_T1


def test_arch_r1_01_a_genuine_reference_only_base_still_binds_as_reuse():
    raw, material = flagship_v2()
    _bind_target_1(raw, material, "reuse")
    base = _rest_base({"reference_only": True, "connector_type": "rest", "component_id": "rest-id"})
    assert "shared_rest" in _prepare(raw, material, base=(base,)).selected_base_keys


@pytest.mark.parametrize(
    "edit, cause",
    [
        pytest.param({"credential_ref": "hunter2"}, "PLAINTEXT_SECRET_REJECTED", id="plaintext_credential_ref"),
        pytest.param({"dbname": "AppDB;password=hunter2"}, "PLAINTEXT_SECRET_REJECTED", id="jdbc_property_in_dbname"),
        pytest.param({"host": "u@db.example.invalid"}, "PLAINTEXT_SECRET_REJECTED", id="userinfo_in_host"),
        pytest.param({"additional": ";password=hunter2"}, COMPOSITION_CONTRACT_MISMATCH, id="jdbc_suffix_key"),
        pytest.param({"connection_url": "jdbc:x"}, COMPOSITION_CONTRACT_MISMATCH, id="custom_url_key"),
    ],
)
def test_arch_r1_02_a_selected_db_create_base_meets_the_credential_boundary(edit, cause):
    raw, material = _create_bound_source()
    base = _create_base_db()
    base = base.model_copy(update={"config": dict(base.config, **edit)})
    with pytest.raises(RecipeError) as caught:
        _prepare(raw, material, base=(base,))
    (diagnostic,) = caught.value.diagnostics
    assert diagnostic.path == "/raw_input/source/connection_key"
    assert diagnostic.cause_codes == (cause,)
    assert "hunter2" not in diagnostic.model_dump_json()


@pytest.mark.parametrize("binding", ["reuse", "create"])
def test_arch_r1_03_a_raw_ampersand_in_a_path_is_refused(binding):
    raw, material = flagship_v2()
    if binding == "reuse":
        raw["targets"][1]["binding_mode"] = "reuse"
        material["targets"][1]["connection"] = {"mode": "reuse", "component_id": "rest-id"}
    material["targets"][1]["operation"]["path"] = "/x&provider_key=v"
    with pytest.raises(RecipeError) as caught:
        _prepare(raw, material)
    (diagnostic,) = caught.value.diagnostics
    assert diagnostic.path == "/component_material/targets/1/operation/path"
    assert diagnostic.cause_codes == ("FANOUT_LITERAL_QUERY_NAME_UNSUPPORTED",)


@pytest.mark.parametrize("char", list("&;"))
def test_arch_r1_03_every_forbidden_raw_path_character_is_refused_and_its_escape_is_data(char):
    from urllib.parse import quote

    from boomi_mcp.recipes.builtins import fanout

    assert char in fanout._PATH_FORBIDDEN_RAW
    assert "a raw '&' or ';' before its query" in fanout.QUERY_GRAMMAR_SENTENCE_V2
    raw, material = flagship_v2()
    raw["targets"][1]["binding_mode"] = "reuse"
    material["targets"][1]["connection"] = {"mode": "reuse", "component_id": "rest-id"}
    material["targets"][1]["operation"]["path"] = "/x" + char + "k=v"
    with pytest.raises(RecipeError):
        _prepare(raw, material)
    material["targets"][1]["operation"]["path"] = "/x" + quote(char, safe="") + "k=v"
    assert _prepare(raw, material).supporting_components


def test_arch_r1_03_an_escaped_ampersand_in_a_path_is_data():
    raw, material = flagship_v2()
    raw["targets"][1]["binding_mode"] = "reuse"
    material["targets"][1]["connection"] = {"mode": "reuse", "component_id": "rest-id"}
    material["targets"][1]["operation"]["path"] = "/x%26y"
    assert _prepare(raw, material).supporting_components


def test_arch_r1_04_a_deferred_name_is_scoped_to_its_owning_root():
    """The deferred names the workflow hands governance carry their OWNING
    root, so another root that also reaches the first operation cannot borrow
    them (two roots, real ownership through resolve_governance)."""
    from boomi_mcp.authoring import governance
    from boomi_mcp.authoring.workflow import _prepare_recipe_components
    from boomi_mcp.models.process_component import (
        ProcessAuthoringUnitAuthoredV1,
        ProcessComponentEnvelopeAuthoredV1,
    )
    from boomi_mcp.models.process_ir import parse_process_ir_v1
    from boomi_mcp.recipes import engine as engine_module

    raw, material = _watermarked(first=[_DEFERRED])
    request = recipe_request(raw, material)
    _specs, deferred, _facts = _prepare_recipe_components(
        request.intent, engine_module.production_registry(), {}
    )

    def ir():
        return parse_process_ir_v1(
            {
                "version": "1",
                "body": {
                    "kind": "sequence",
                    "steps": [
                        {
                            "kind": "source",
                            "connection_ref": "$ref:target_rest_connection",
                            "operation_ref": "$ref:target_rest_operation",
                        },
                        {"kind": "return_documents"},
                    ],
                },
            }
        )

    connection = IntegrationComponentSpec(
        key="target_rest_connection", type="connector-settings", name="C",
        config={"connector_type": "rest", "base_url": "https://h.example.invalid", "auth": "NONE"},
    )
    operation = IntegrationComponentSpec(
        key="target_rest_operation", type="connector-action", name="O",
        config={"connector_type": "rest", "method": "GET", "path": "/x", "connection_ref_key": "target_rest_connection"},
        depends_on=["target_rest_connection"],
    )
    profile = IntegrationComponentSpec(
        key="profile", type="profile.db", name="P",
        config={"output_fields": [{"name": "f", "data_type": "datetime"}]},
    )
    watermark = WatermarkDeclarationV1(
        source_profile_ref="$ref:profile", field="f", kind="timestamp", query_parameter_refs=("since",)
    )
    borrower = ProcessAuthoringUnitAuthoredV1(
        envelope=ProcessComponentEnvelopeAuthoredV1(
            component_key="other_root", action="create", name="Other Root",
            depends_on=("target_rest_connection", "target_rest_operation", "profile"),
            watermark=watermark,
        ),
        process_ir=ir(),
    )
    with pytest.raises(governance.GovernanceRefusal):
        governance.resolve_governance(
            [borrower], [connection, operation, profile], deferred_query_parameter_names=deferred
        )


def _v1_and_v2_request(*, v2_selects_shared_db):
    """A pinned v1 invocation over the frozen flagship components (all base
    components) beside a v2 invocation under namespace ``b``."""
    from _fanout_v2_support import respell_oracle_credential_refs

    components = respell_oracle_credential_refs(oracle()["integration_spec"]["components"])
    process = components[-1]
    config = process["config"]
    branch = next(n for n in config["flow_sequence"] if n["kind"] == "branch")
    v1_raw = {
        "version": "1",
        "process_key": process["key"],
        "source_connection_ref": config["source"]["connection_id"],
        "source_operation_ref": config["source"]["operation_id"],
        "map_ref": next(n for n in config["flow_sequence"] if n["kind"] == "map_ref")["map_ref"],
        "map_label": "Shared transform",
        "branch_label": branch["label"],
        "targets": [
            {
                "target_id": "t.%d" % index,
                "connection_ref": leg["target"]["connection_id"],
                "operation_ref": leg["target"]["operation_id"],
                "handoff": "document_stream",
            }
            for index, leg in enumerate(branch["legs"])
        ],
        "component_slots": [
            {
                "contribution_id": "c.%d" % index,
                "component_key": component["key"],
                "component_type": component["type"],
                "materialization_mode": "create",
                "materializer_slot": component["key"],
            }
            for index, component in enumerate(components)
        ],
    }
    raw, material = flagship_v2()
    raw.update(key_namespace="b", process_key="b_process")
    raw["naming"]["process_name"] = "Process B"
    raw["naming"]["component_prefix"] = "BBB"
    if v2_selects_shared_db:
        raw["source"]["connection_key"] = "source_db_connection"
        material["source"].pop("connection")
    return recipe_request(
        raw,
        material,
        base_components=components,
        invocation_id="v2",
        extra_invocations=[
            {"recipe_id": RECIPE_ID, "recipe_version": "1.0.0", "invocation_id": "v1", "raw_input": v1_raw}
        ],
    )


def test_arch_r1_05_a_v1_slot_and_a_v2_selector_on_one_connection_are_not_duplicates():
    result, internals = plan_authoring_request_v1(
        _v1_and_v2_request(v2_selects_shared_db=True), profile="p"
    )
    keys = [c.key for c in internals.normalized.integration_spec.components]
    assert keys.count("source_db_connection") == 1
    assert result.validation_report.is_valid, [d.code for d in result.errors]


def test_arch_r1_05_the_engine_refuses_a_supporting_copy_of_a_catalog_entry():
    """The engine's own guard: a catalog entry joins by KEY, never as a copy."""
    from boomi_mcp.recipes.engine import RecipeRequestV1, run_recipes
    from boomi_mcp.recipes.materialization import MaterializationCatalog

    raw, material = flagship_v2()
    prepared = _prepare(raw, material)
    stray = next(c for c in prepared.supporting_components if c.key == "source_db_read_profile")
    with pytest.raises(RecipeError) as caught:
        run_recipes(
            [RecipeRequestV1(RECIPE_ID, "fanout", raw, V2)],
            catalog=MaterializationCatalog({"source_db_read_profile": stray}),
            supporting_components=prepared.supporting_components,
        )
    assert COMPOSITION_COMPONENT_KEY_COLLISION in caught.value.diagnostics[0].cause_codes


def test_arch_r1_05_a_generated_key_equal_to_a_base_key_is_a_collision():
    raw, material = flagship_v2()
    stray = IntegrationComponentSpec(
        key="source_db_read_profile", type="profile.db", name="Someone else's profile",
        config={"output_fields": [{"name": "x", "data_type": "character"}]},
    )
    with pytest.raises(AuthoringWorkflowError) as caught:
        plan_authoring_request_v1(
            recipe_request(raw, material, base_components=[stray.model_dump(mode="json")]),
            profile="p",
        )
    (diagnostic,) = caught.value.diagnostics
    assert COMPOSITION_COMPONENT_KEY_COLLISION in diagnostic.cause_codes


def test_arch_r1_06_a_mixed_v1_v2_preview_serves_unique_row_keys():
    result, _ = plan_authoring_request_v1(
        _v1_and_v2_request(v2_selects_shared_db=False), profile="p"
    )
    preview = result.integration_spec_preview
    keys = [row.key for row in preview.flows] + [e.key for e in preview.endpoints]
    assert len(keys) == len(set(keys)), keys
    assert result.validation_report.is_valid, [d.code for d in result.errors]


def test_arch_r1_07_a_name_only_reuse_selector_is_served_on_the_endpoint():
    raw, material = flagship_v2()
    _bind_target_1(raw, material, "reuse")
    base = _rest_base({"reference_only": True, "connector_type": "rest"}, name="Existing Billing")
    result, _ = plan_authoring_request_v1(
        recipe_request(raw, material, base_components=[base.model_dump(mode="json")]), profile="p"
    )
    names = [e.component_name for e in result.integration_spec_preview.endpoints]
    assert "Existing Billing" in names


def test_arch_r1_08_a_direct_intent_result_gains_no_recipe_keys():
    from unittest import mock

    from boomi_mcp.categories import integration_builder

    case = (
        _TESTS / "fixtures" / "governance" / "issue_157" / "cases" / "database_to_api_sync"
        / "wm_off_dlq_off_create" / "input.canonical.json"
    )
    payload = json.loads(case.read_text())
    for action in ("plan", "compile"):
        with mock.patch.object(integration_builder, "paginate_metadata", lambda *a, **k: []):
            envelope = integration_builder.build_integration_action(
                mock.MagicMock(), "p", action, {"authoring_request": payload}
            )
        assert envelope["_success"] is True, envelope.get("error")
        result = envelope["authoring_result"]
        assert "recipe_provenance" not in result and "recipe_diagnostics" not in result, action
        # The preview's ``endpoints: []`` is NOT a gained key: measured at the
        # S1 parent cb7034b, a direct preview already served it.
        assert result["integration_spec_preview"]["endpoints"] == [], action


def test_arch_r1_08_a_recipe_intent_result_keeps_its_recipe_keys():
    result, _ = plan_authoring_request_v1(recipe_request(*flagship_v2()), profile="p")
    dump = result.model_dump(mode="json")
    assert "recipe_provenance" in dump and "recipe_diagnostics" in dump
    assert "endpoints" in dump["integration_spec_preview"]


_PIPELINE_CHILD = textwrap.dedent(
    """
    import copy, json, sys
    # The legacy AUTHORITY module only: a 1.0 spec's legacy process CONFIG is
    # still validated through the legacy bridge, which is a different route.
    BLOCKED = ("boomi_mcp.compiler.process_ir.legacy_adapters.authority",)
    attempts = []

    class _Blocker:
        def find_spec(self, name, path=None, target=None):
            if any(name == b or name.startswith(b + ".") for b in BLOCKED):
                attempts.append(name)
                raise ImportError("blocked: " + name)
            return None

    sys.meta_path.insert(0, _Blocker())
    sys.path[:0] = sys.argv[1:2]
    from unittest.mock import MagicMock, patch
    fixture = json.load(open(sys.argv[2]))
    config = copy.deepcopy(fixture["contradictory_pipelines"]["config"])
    config["integration_spec"]["version"] = "1.0"
    nested = config["integration_spec"]["components"][0]["config"]["pipeline"]
    config["integration_spec"]["pipeline"] = copy.deepcopy(nested)
    from boomi_mcp.categories import integration_builder
    try:
        with patch.object(integration_builder, "paginate_metadata", lambda *a, **k: []):
            plan = integration_builder._build_plan(MagicMock(), config)
        success = plan.get("_success")
    except ImportError:
        success = "import_error"
    print(json.dumps({"attempts": attempts, "success": success}))
    """
)


def test_arch_r1_09_a_v1_0_spec_with_a_pipeline_never_imports_the_legacy_authority():
    fixture = _TESTS / "fixtures" / "compatibility" / "issue_135" / "authoring_boundaries.json"
    completed = subprocess.run(
        [sys.executable, "-c", _PIPELINE_CHILD, str(_SRC), str(fixture)],
        capture_output=True, text=True, check=True,
    )
    payload = json.loads(completed.stdout.strip().splitlines()[-1])
    assert payload["attempts"] == []
    assert payload["success"] is True


@pytest.mark.parametrize("arm", ["db_host", "rest_base_url"])
def test_arch_r1_10_an_ip_literal_endpoint_is_never_echoed_in_served_diagnostics(arm):
    """With an account in hand the component plan runs and its lint warnings
    are forwarded; an IP-literal endpoint must not ride them out."""
    from unittest import mock

    from boomi_mcp.categories import integration_builder

    raw, material = flagship_v2()
    if arm == "db_host":
        literal = "10.71.42.9"
        material["source"]["connection"]["host"] = literal
    else:
        literal = "10.88.13.7"
        material["targets"][1]["connection"]["base_url"] = "https://" + literal
    payload = recipe_request(raw, material).model_dump(mode="json", exclude_none=True)
    with mock.patch.object(integration_builder, "paginate_metadata", lambda *a, **k: []):
        envelope = integration_builder.build_integration_action(
            mock.MagicMock(), "p", "plan", {"authoring_request": payload}
        )
    assert envelope["_success"] is True, envelope.get("error")
    result = envelope["authoring_result"]
    # Non-vacuity: the lint DID fire and was forwarded.
    assert "CONNECTION_ENDPOINT_IP_LITERAL" in json.dumps(result["warnings"])
    for preview in (result["integration_spec_preview"], envelope["integration_spec"]):
        for component in preview["components"]:
            component["config"] = None
    assert literal not in json.dumps(envelope)


def test_arch_r1_11_padded_mapping_names_are_normalized_like_the_legacy_route():
    raw, material = flagship_v2()
    padded = copy.deepcopy(material)
    for operation in padded["transform"]["operations"]:
        if operation["operation_type"] == "direct":
            operation["source_field"] = "  " + operation["source_field"] + " "
            operation["target_path"] = " " + operation["target_path"] + "  "
    assert [_dump(c) for c in _prepare(raw, padded).supporting_components] == [
        _dump(c) for c in _prepare(raw, material).supporting_components
    ]


def test_arch_r1_11_a_function_family_that_maps_no_input_still_prepares():
    """``default_value`` maps no input; the legacy model (inputs >= 1) cannot
    carry it, so its names are normalized without that model."""
    raw, material = flagship_v2()
    material["transform"]["operations"][0] = {
        "operation_type": "map_function",
        "function_type": "default_value",
        "inputs": [],
        "target_path": " " + material["transform"]["operations"][0]["target_path"] + " ",
        "parameters": [{"name": "value", "value": "x"}],
    }
    assert _prepare(raw, material).supporting_components


def test_arch_r1_14_a_duplicate_watermark_ref_is_refused_before_assembly():
    raw, material = _watermarked(first=[_DEFERRED], refs=("since", "since"))
    with pytest.raises(RecipeError) as caught:
        _prepare(raw, material)
    (diagnostic,) = caught.value.diagnostics
    assert diagnostic.path == "/raw_input/watermark/query_parameter_refs/1"
    assert diagnostic.cause_codes == (GOVERNANCE_WATERMARK_INCONSISTENT,)


# ---------------------------------------------------------------------------
# B12 (Codex support review of B11): witnesses, each failing at 55807e2.
# ---------------------------------------------------------------------------


def _account_lists(*entries):
    """``paginate_metadata`` answering with these existing components."""
    return lambda *a, **k: [dict(entry) for entry in entries]


def test_b12_01_a_create_bound_base_the_plan_reuses_by_name_is_refused():
    """Under ``reuse`` a selected CREATE-mode base with no declared id is
    resolved BY NAME to an existing connection by the component plan: apply
    would discard its config, so the advertised create binding is false."""
    from unittest import mock

    from boomi_mcp.categories import integration_builder

    raw, material = flagship_v2()
    _bind_target_1(raw, material, "create")
    base = _rest_base(
        {"connector_type": "rest", "base_url": "https://b.example.invalid", "auth": "NONE"},
        name="Shared REST",
    )
    payload = recipe_request(
        raw, material, base_components=[base.model_dump(mode="json")]
    ).model_dump(mode="json", exclude_none=True)
    existing = {"component_id": "6f1d2c3b-0000-4000-8000-000000000001", "name": "Shared REST",
                "type": "connector-settings", "folder_name": "X"}
    with mock.patch.object(integration_builder, "paginate_metadata", _account_lists(existing)):
        envelope = integration_builder.build_integration_action(
            mock.MagicMock(), "p", "plan", {"authoring_request": payload}
        )
    assert envelope["_success"] is False, "a create binding apply would reuse was accepted"
    assert "COMPOSITION_CONTRACT_MISMATCH" in json.dumps(envelope)
    assert "/intent/invocations/0/raw_input/targets/1/connection_key" in json.dumps(envelope)


def test_b12_01_a_generated_connection_reused_by_name_is_served_as_reuse():
    """The sibling: a GENERATED create connection the component plan resolves by
    name is reused by apply; its served endpoint row says so."""
    from unittest import mock

    from boomi_mcp.categories import integration_builder

    raw, material = flagship_v2()
    payload = recipe_request(raw, material).model_dump(mode="json", exclude_none=True)
    existing = {"component_id": "6f1d2c3b-0000-4000-8000-000000000002",
                "name": "M8DEMO Billing REST Connection", "type": "connector-settings",
                "folder_name": "X"}
    with mock.patch.object(integration_builder, "paginate_metadata", _account_lists(existing)):
        envelope = integration_builder.build_integration_action(
            mock.MagicMock(), "p", "plan", {"authoring_request": payload}
        )
    assert envelope["_success"] is True, envelope.get("error")
    rows = envelope["authoring_result"]["integration_spec_preview"]["endpoints"]
    (billing,) = [row for row in rows if row["key"] == "rest_target_billing"]
    assert billing["binding_mode"] == "reuse", billing


def test_b12_02_an_ambiguous_catalog_key_is_refused():
    """Two catalog entries under one component key: a selection by key cannot
    say which one it means, so it is refused rather than silently resolved."""
    from boomi_mcp.authoring.workflow import _connector_metadata_from_components
    from boomi_mcp.recipes.engine import RecipeRequestV1, run_recipes
    from boomi_mcp.recipes.materialization import MaterializationCatalog

    raw, material = flagship_v2()
    _bind_target_1(raw, material, "reuse")
    first = _rest_base({"reference_only": True, "connector_type": "rest", "component_id": "rest-a"})
    second = _rest_base({"reference_only": True, "connector_type": "rest", "component_id": "rest-b"})
    prepared = _prepare(raw, material, base=(first,))
    metadata = _connector_metadata_from_components(list(prepared.supporting_components) + [first])

    def run(entries):
        return run_recipes(
            [RecipeRequestV1(RECIPE_ID, "fanout", raw, V2)],
            catalog=MaterializationCatalog(entries),
            supporting_components=prepared.supporting_components,
            catalog_supporting_keys=prepared.selected_base_keys,
            connector_metadata=metadata,
        )

    # Control: one entry under the key joins once.
    assert [c for c in run({"slot.a": first}).supporting_components if c.key == "shared_rest"] == [first]
    with pytest.raises(RecipeError) as caught:
        run({"slot.a": first, "slot.b": second})
    assert COMPOSITION_COMPONENT_KEY_COLLISION in caught.value.diagnostics[0].cause_codes


def test_b12_03_script_mapping_names_are_normalized_like_the_legacy_route():
    raw, material = flagship_v2()
    material["transform"]["operations"][1] = {
        "operation_type": "map_script",
        "script_component_key": "shared_script",
        "inputs": [" source_b "],
        "outputs": ["  Root/target_b"],
        "documentation_hint": "  why  ",
    }
    script = IntegrationComponentSpec(
        key="shared_script", type="script.mapping", name="Shared Script",
        config={
            "component_type": "script.mapping", "component_name": "Shared Script",
            "language": "groovy2", "script_body": "target_b = source_b",
            "inputs": [{"name": "source_b", "data_type": "character"}],
            "outputs": [{"name": "target_b"}],
        },
    )
    prepared = _prepare(raw, material, base=(script,))
    the_map = next(c for c in prepared.supporting_components if c.type == "transform.map")
    (mapping,) = the_map.config["script_mappings"]
    assert json.dumps(mapping).count(" source_b ") == 0 and "  Root/target_b" not in json.dumps(mapping)
    hints = [h.documentation_hint for h in prepared.projection_facts.documentation_hints]
    assert hints == ["why"]


def test_b12_04_a_whitespace_only_id_is_absent_for_the_served_selector():
    raw, material = flagship_v2()
    _bind_target_1(raw, material, "reuse")
    base = _rest_base(
        {"reference_only": True, "connector_type": "rest", "component_id": "   "},
        name="Existing Billing",
    )
    result, _ = plan_authoring_request_v1(
        recipe_request(raw, material, base_components=[base.model_dump(mode="json")]), profile="p"
    )
    (row,) = [e for e in result.integration_spec_preview.endpoints if e.key == "rest_target_billing"]
    assert row.component_id is None
    assert row.component_name == "Existing Billing"


def test_b12r_a_name_selector_stays_served_beside_an_id_the_plan_resolved():
    """B12 review P3: an account-backed plan resolves a name-only reuse to an
    id; the name that drove the lookup stays served beside it."""
    from boomi_mcp.authoring.derived_flows import _endpoint_row

    base = _rest_base({"reference_only": True, "connector_type": "rest"}, name="Existing Billing")
    row = _endpoint_row(
        "rest_target_billing", "target", base, "rest", existing_ids={"shared_rest": "id-123"}
    )
    assert row.component_id == "id-123"
    assert row.component_name == "Existing Billing"
    declared = _rest_base(
        {"reference_only": True, "connector_type": "rest", "component_id": "id-9"},
        name="Ignored Top Name",
    )
    row = _endpoint_row("rest_target_billing", "target", declared, "rest")
    assert row.component_id == "id-9"
    assert row.component_name is None  # a declared id binds; only config.component_name rides beside it


def test_b12r_slots_aliasing_one_spec_are_one_component():
    """B12 review P3: two slots holding the SAME spec object under one key are one
    component; two DISTINCT specs under one key stay a collision."""
    from boomi_mcp.recipes.engine import _selected_catalog_entries
    from boomi_mcp.recipes.errors import RecipeError

    spec = _rest_base({"reference_only": True, "connector_type": "rest"}, name="A")

    class _Catalog:
        def __init__(self, entries):
            self._entries = entries

        def entries_for_key(self, key):
            return self._entries

    assert _selected_catalog_entries(_Catalog((spec, spec)), ["shared_rest"], []) == (spec,)
    other = _rest_base({"reference_only": True, "connector_type": "rest"}, name="B")
    with pytest.raises(RecipeError):
        _selected_catalog_entries(_Catalog((spec, other)), ["shared_rest"], [])


@pytest.mark.parametrize("order", ["ab", "ba"])
def test_qa_r12_01_two_base_components_under_one_key_are_refused(order):
    """QA-159-S1-r12-01: duplicate base keys used to collapse to the last entry,
    so list order chose the bound connection. Refused at the request."""
    from boomi_mcp.authoring.workflow import AuthoringWorkflowError
    from boomi_mcp.errors import COMPOSITION_COMPONENT_KEY_COLLISION

    raw, material = flagship_v2()
    _bind_target_1(raw, material, "reuse")
    a = _rest_base({"reference_only": True, "connector_type": "rest", "component_id": "id-a"}, name="A")
    b = _rest_base({"reference_only": True, "connector_type": "rest", "component_id": "id-b"}, name="B")
    bases = [a, b] if order == "ab" else [b, a]
    with pytest.raises(AuthoringWorkflowError) as refused:
        plan_authoring_request_v1(
            recipe_request(raw, material, base_components=[x.model_dump(mode="json") for x in bases]),
            profile="p",
        )
    (diagnostic,) = refused.value.diagnostics
    assert diagnostic.path == "/intent/base_components/1/key"
    assert COMPOSITION_COMPONENT_KEY_COLLISION in diagnostic.cause_codes
    assert "id-a" not in diagnostic.model_dump_json() and "id-b" not in diagnostic.model_dump_json()


# ---------------------------------------------------------------------------
# B14 (architect evaluation 2): witnesses, each failing at 6829220.
# ---------------------------------------------------------------------------

_CAP159 = _TESTS / "fixtures" / "boomi" / "replay_identity" / "cap159-s1-reuse-identity"
_REUSED_REST_ID = "aa5c5042-75d1-4bd3-85df-a65739cb920e"


def _reused_rest_xml(url):
    """The pinned platform capture of the reused REST connection, its stored
    ``url`` field changed IN-TEST to ``url``."""
    xml = (_CAP159 / "rest-conn-aa5c5042.xml").read_text()
    original = 'field id="url" type="string" value="http://host.docker.internal:8081"'
    assert original in xml
    from xml.sax.saxutils import quoteattr

    return xml.replace(original, 'field id="url" type="string" value={0}'.format(quoteattr(url)))


def _reuse_bound_request(path):
    raw, material = flagship_v2()
    _bind_target_1(raw, material, "reuse")
    material["targets"][1]["operation"]["path"] = path
    base = _rest_base(
        {"reference_only": True, "connector_type": "rest", "component_id": _REUSED_REST_ID},
        component_id=_REUSED_REST_ID,
    )
    return recipe_request(raw, material, base_components=[base.model_dump(mode="json")])


def _with_account(request, stored_url, action="plan"):
    from unittest import mock

    from boomi_mcp.categories import integration_builder
    from boomi_mcp.categories.components import _shared

    def get_xml(_client, component_id, *a, **k):
        assert component_id == _REUSED_REST_ID
        return {"component_id": component_id, "version": 2, "xml": _reused_rest_xml(stored_url)}

    payload = request.model_dump(mode="json", exclude_none=True)
    with mock.patch.object(integration_builder, "paginate_metadata", lambda *a, **k: []), \
            mock.patch.object(_shared, "component_get_xml", get_xml):
        return integration_builder.build_integration_action(
            mock.MagicMock(), "p", action, {"authoring_request": payload}
        )


_REUSE_PATH_POINTER = "/intent/invocations/0/component_material/targets/1/operation/path"


@pytest.mark.parametrize("stored", ["http://h.example.invalid/v1?mode=full&", "http://h.example.invalid/v1?"])
def test_b14_01_a_reuse_path_joined_to_the_stored_base_url_is_judged(stored):
    """ARCH-r1-03: the stored base URL is READ (the same account read connector
    replay identity uses) and the authored path is judged JOINED to it. A
    stored base that already carries a query is the BINDING's fault (B15-4):
    pointed at the selecting ``connection_key``, its URL never echoed."""
    envelope = _with_account(_reuse_bound_request("provider_key=sample"), stored, "compile")
    served = json.dumps(envelope)
    assert envelope["_success"] is False, served[:400]
    assert "FANOUT_LITERAL_QUERY_NAME_UNSUPPORTED" in served
    assert "/intent/invocations/0/raw_input/targets/1/connection_key" in served
    assert "query and no fragment" in served
    assert "h.example.invalid" not in served and "mode=full" not in served
    assert "provider_key" not in served.replace("provider_key=sample", "")


def test_b14_01_a_reuse_path_under_a_queryless_stored_base_plans():
    """Control: the same reuse with a query-free stored base URL and a relative
    path plans — the joined target is clean."""
    envelope = _with_account(_reuse_bound_request("v1/billing"), "http://h.example.invalid/api/", "compile")
    assert envelope["_success"] is True, envelope.get("error")
    assert envelope["authoring_result"]["validation_report"]["is_valid"] is True


@pytest.mark.parametrize("path", ["provider_key=sample", "v1/x=1"])
def test_b14_01_without_the_stored_base_a_reuse_path_fails_closed(path):
    """No account: the stored base URL is genuinely unknown, so a reuse path
    that does not start with '/' or carries '=' before any '?' is refused."""
    with pytest.raises(AuthoringWorkflowError) as caught:
        plan_authoring_request_v1(_reuse_bound_request(path), profile="p")
    served = json.dumps([d.model_dump(mode="json") for d in caught.value.diagnostics])
    assert "FANOUT_LITERAL_QUERY_NAME_UNSUPPORTED" in served and _REUSE_PATH_POINTER in served


def test_b14_01_without_the_stored_base_an_absolute_reuse_path_plans():
    result, _ = plan_authoring_request_v1(_reuse_bound_request("/v1/billing"), profile="p")
    assert result.validation_report.is_valid


def test_b14_03_the_execution_component_plan_is_rechecked_before_any_write():
    """A same-name connection that appears BETWEEN the compile's component plan
    and apply's execution plan: apply must refuse before any write."""
    from unittest import mock

    from boomi_mcp.authoring import workflow
    from boomi_mcp.categories import integration_builder

    raw, material = flagship_v2()
    _bind_target_1(raw, material, "create")
    base = _rest_base(
        {"connector_type": "rest", "base_url": "https://b.example.invalid", "auth": "NONE"},
        name="Shared REST",
    )
    request = recipe_request(raw, material, base_components=[base.model_dump(mode="json")])
    existing = {"component_id": "6f1d2c3b-0000-4000-8000-000000000003", "name": "Shared REST",
                "type": "connector-settings", "folder_name": "X"}
    state = {"after_preflight": False}

    def listing(*a, **k):
        return [dict(existing)] if state["after_preflight"] else []

    real_preflight = workflow.preflight_typed_apply_v1

    def preflight(*a, **k):
        bundle = real_preflight(*a, **k)
        state["after_preflight"] = True
        return bundle

    from boomi_mcp.categories.components import _shared

    capture = (_CAP159 / "rest-conn-aa5c5042.xml").read_text().replace(
        _REUSED_REST_ID, existing["component_id"]
    )

    def get_xml(_client, component_id, *a, **k):
        # The account answers the read-back of the name-matched component
        # (proven-operable capture, re-keyed in-test to the matched id).
        return {"component_id": component_id, "version": 2, "xml": capture,
                "type": "connector-settings"}

    client = mock.MagicMock()
    with mock.patch.object(integration_builder, "paginate_metadata", listing):
        compiled, _ = compile_authoring_request_v1(request, boomi_client=client, profile="p")
    payload = request.model_dump(mode="json", exclude_none=True)
    payload["expected_capability_revision"] = compiled.revision_binding.capability_revision
    payload["expected_compile_hash"] = compiled.revision_binding.compile_hash
    writes = []
    with mock.patch.object(integration_builder, "paginate_metadata", listing), \
            mock.patch.object(_shared, "component_get_xml", get_xml), \
            mock.patch.object(integration_builder, "component_get_xml", get_xml), \
            mock.patch.object(workflow, "preflight_typed_apply_v1", preflight), \
            mock.patch.object(integration_builder, "_execute_component",
                              side_effect=lambda *a, **k: writes.append("component") or {"_success": True, "component_id": "x"}), \
            mock.patch.object(integration_builder, "create_component",
                              side_effect=lambda *a, **k: writes.append("create") or {"_success": True, "component_id": "y"}), \
            mock.patch.object(integration_builder, "update_component",
                              side_effect=lambda *a, **k: writes.append("update") or {"_success": True}):
        envelope = integration_builder.build_integration_action(
            client, "p", "apply", {"authoring_request": payload, "dry_run": False}
        )
    assert state["after_preflight"], "the preflight was not reached"
    assert envelope["_success"] is False, "apply wrote a create the execution plan reuses"
    assert writes == []
    assert "/intent/invocations/0/raw_input/targets/1/connection_key" in json.dumps(envelope)


def test_b14_04_the_served_c8_guarantee_holds_on_a_mixed_preview():
    """ARCH-r2 (P3): the served text states exactly what a mixed v1/v2 preview
    delivers — every row key unique; a recipe ROUTE's in-row references resolve
    inside its own process; a non-route root's transform row keeps its own
    source token, which may name no served row."""
    from boomi_mcp.authoring.contract import authoring_workflow_contract

    text = json.dumps(authoring_workflow_contract())
    assert "every reference resolves inside its own process" not in text
    assert "a recipe route's in-row references resolve inside its own process" in text
    assert "keeps its own source token" in text
    result, _ = plan_authoring_request_v1(
        _v1_and_v2_request(v2_selects_shared_db=False), profile="p"
    )
    preview = result.integration_spec_preview
    rows = [row.model_dump(mode="json") for row in preview.flows]
    keys = [row["key"] for row in rows] + [e.key for e in preview.endpoints]
    assert len(keys) == len(set(keys))
    served = set(keys)
    route_owner = "b_process"
    route_rows = [row for row in rows if row["key"].startswith(route_owner + "/")]
    assert len(route_rows) >= 5  # non-vacuity
    for row in route_rows:
        for field in ("source", "target"):
            value = row[field]
            if value in (None, "", "dlq"):
                continue
            assert value in served and value.startswith(route_owner + "/"), (row["key"], value)


def test_b14_01_sibling_a_material_reuse_by_id_is_judged_against_its_stored_base_url():
    """Sibling arm: reuse declared in the MATERIAL (by component id) reads the
    same account record and is judged joined to it; a stored query is pointed
    at the material ``connection`` binding (B15-4)."""
    raw, material = flagship_v2()
    raw["targets"][1]["binding_mode"] = "reuse"
    material["targets"][1]["connection"] = {"mode": "reuse", "component_id": _REUSED_REST_ID}
    material["targets"][1]["operation"]["path"] = "provider_key=sample"
    envelope = _with_account(
        recipe_request(raw, material), "http://h.example.invalid/v1?mode=full&", "compile"
    )
    served = json.dumps(envelope)
    assert envelope["_success"] is False
    assert "FANOUT_LITERAL_QUERY_NAME_UNSUPPORTED" in served
    assert "/intent/invocations/0/component_material/targets/1/connection" in served
    assert _REUSE_PATH_POINTER not in served


# ---------------------------------------------------------------------------
# B15 (Codex review of B14): witnesses, each failing at 1d5a9ca.
# ---------------------------------------------------------------------------

_REUSED_NAME = "Existing Billing"
_GENERATED_BILLING_NAME = "M8DEMO Billing REST Connection"
_MATERIAL_BINDING_POINTER = "/intent/invocations/0/component_material/targets/1/connection"
_RAW_BINDING_POINTER = "/intent/invocations/0/raw_input/targets/1/connection_key"


def _account_patches(stored_url, *, listed_name=None, reads=None):
    """The account a public plan/compile/apply consults: the component listing
    (``listed_name`` -> the pinned REST capture's id) and the component read,
    answering the capture with its stored ``url`` set IN-TEST. ``stored_url``
    may be a callable (read at call time)."""
    import contextlib
    from unittest import mock

    from boomi_mcp.categories import integration_builder
    from boomi_mcp.categories.components import _shared

    listing_entry = {"component_id": _REUSED_REST_ID, "name": listed_name,
                     "type": "connector-settings", "folder_name": "X"}

    def listing(*a, **k):
        return [dict(listing_entry)] if listed_name else []

    def get_xml(_client, component_id, *a, **k):
        assert component_id == _REUSED_REST_ID, component_id
        url = stored_url() if callable(stored_url) else stored_url
        if reads is not None:
            reads.append(url)
        return {"component_id": component_id, "version": 2, "xml": _reused_rest_xml(url),
                "type": "connector-settings"}

    stack = contextlib.ExitStack()
    stack.enter_context(mock.patch.object(integration_builder, "paginate_metadata", listing))
    stack.enter_context(mock.patch.object(_shared, "component_get_xml", get_xml))
    stack.enter_context(mock.patch.object(integration_builder, "component_get_xml", get_xml))
    return stack


def _public(request, action, patches):
    from unittest import mock

    from boomi_mcp.categories import integration_builder

    with patches:
        return integration_builder.build_integration_action(
            mock.MagicMock(), "p", action,
            {"authoring_request": request.model_dump(mode="json", exclude_none=True)},
        )


def _name_bound_reuse(arm, path):
    raw, material = flagship_v2()
    if arm == "base":
        _bind_target_1(raw, material, "reuse")
        base = _rest_base({"reference_only": True, "connector_type": "rest"}, name=_REUSED_NAME)
        bases = [base.model_dump(mode="json")]
    else:
        raw["targets"][1]["binding_mode"] = "reuse"
        material["targets"][1]["connection"] = {"mode": "reuse", "component_name": _REUSED_NAME}
        bases = []
    material["targets"][1]["operation"]["path"] = path
    return recipe_request(raw, material, base_components=bases)


@pytest.mark.parametrize("arm", ["base", "material"])
def test_b15_01_a_name_bound_reuse_the_plan_resolves_is_read_and_judged(arm):
    """B15-1: a reuse bound BY NAME is resolved by the component plan; its stored
    URL is read through that resolved id, so a relative path under a
    query-free stored base plans (no fail-closed refusal)."""
    reads = []
    envelope = _public(
        _name_bound_reuse(arm, "v1/billing"), "compile",
        _account_patches("http://h.example.invalid/api/", listed_name=_REUSED_NAME, reads=reads),
    )
    assert envelope["_success"] is True, json.dumps(envelope)[:600]
    assert envelope["authoring_result"]["validation_report"]["is_valid"] is True
    assert reads, "the stored URL was never read through the plan's resolved id"


@pytest.mark.parametrize("arm", ["base", "material"])
def test_b15_01_a_name_bound_reuse_the_account_cannot_resolve_fails_closed(arm):
    """Control: the same name-bound reuse with NO matching component in the
    account is unresolved, so the relative path is refused fail-closed."""
    envelope = _public(
        _name_bound_reuse(arm, "v1/billing"), "compile",
        _account_patches("http://h.example.invalid/api/", listed_name=None),
    )
    served = json.dumps(envelope)
    assert envelope["_success"] is False
    assert "FANOUT_LITERAL_QUERY_NAME_UNSUPPORTED" in served and _REUSE_PATH_POINTER in served
    (diag,) = envelope["authoring_diagnostics"]
    assert diag["path"] == _REUSE_PATH_POINTER and "absolute path" in diag["remediation"]


def test_b15_02_a_generated_connection_reused_by_name_is_judged_against_its_stored_url():
    """B15-2: target 1's connection is requested in CREATE mode, but under
    ``conflict_policy=reuse`` the plan binds the same-name existing component,
    so apply discards the candidate base URL and requests against the STORED
    one — which carries a query, so the request target is refused."""
    raw, material = flagship_v2()
    envelope = _public(
        recipe_request(raw, material), "compile",
        _account_patches("http://h.example.invalid/v1?mode=full&",
                         listed_name=_GENERATED_BILLING_NAME),
    )
    served = json.dumps(envelope)
    assert envelope["_success"] is False, served[:600]
    assert "FANOUT_LITERAL_QUERY_NAME_UNSUPPORTED" in served
    assert _MATERIAL_BINDING_POINTER in served
    assert "/targets/0/" not in served  # only the same-name connection was resolved
    assert "mode=full" not in served


def test_b15_02_control_a_generated_connection_reused_under_a_clean_stored_url_plans():
    raw, material = flagship_v2()
    envelope = _public(
        recipe_request(raw, material), "compile",
        _account_patches("http://h.example.invalid/api", listed_name=_GENERATED_BILLING_NAME),
    )
    assert envelope["_success"] is True, json.dumps(envelope)[:600]


def _apply_after_the_stored_url_changes(arm, at_apply):
    """Compile against a clean stored URL; then apply, with the account's
    stored URL changed to ``at_apply`` AFTER apply's compile replay (preflight)
    and before its execution read. Returns ``(envelope, writes, state,
    binding pointer)``; the mutation entry points are recorded, never run."""
    from unittest import mock

    from boomi_mcp.authoring import workflow
    from boomi_mcp.categories import integration_builder

    state = {"url": "http://h.example.invalid/api/", "after_preflight": False}
    if arm == "base_by_id":
        request, listed, binding = _reuse_bound_request("v1/billing"), None, _RAW_BINDING_POINTER
    else:
        raw, material = flagship_v2()
        request, listed, binding = (
            recipe_request(raw, material), _GENERATED_BILLING_NAME, _MATERIAL_BINDING_POINTER
        )
    client = mock.MagicMock()
    with _account_patches(lambda: state["url"], listed_name=listed):
        compiled, _ = compile_authoring_request_v1(request, boomi_client=client, profile="p")
    real_preflight = workflow.preflight_typed_apply_v1

    def preflight(*a, **k):
        bundle = real_preflight(*a, **k)
        state["after_preflight"] = True
        state["url"] = at_apply
        return bundle

    payload = request.model_dump(mode="json", exclude_none=True)
    payload["expected_capability_revision"] = compiled.revision_binding.capability_revision
    payload["expected_compile_hash"] = compiled.revision_binding.compile_hash
    writes = []
    with _account_patches(lambda: state["url"], listed_name=listed), \
            mock.patch.object(workflow, "preflight_typed_apply_v1", preflight), \
            mock.patch.object(integration_builder, "_execute_component",
                              side_effect=lambda *a, **k: writes.append("component") or {"_success": True, "component_id": "x"}), \
            mock.patch.object(integration_builder, "create_component",
                              side_effect=lambda *a, **k: writes.append("create") or {"_success": True, "component_id": "y"}), \
            mock.patch.object(integration_builder, "update_component",
                              side_effect=lambda *a, **k: writes.append("update") or {"_success": True}):
        envelope = integration_builder.build_integration_action(
            client, "p", "apply", {"authoring_request": payload, "dry_run": False}
        )
    return envelope, writes, state, binding


@pytest.mark.parametrize("arm", ["base_by_id", "generated_reused_by_name"])
def test_b15_03_apply_rejudges_the_stored_url_from_its_fresh_execution_read(arm):
    """B15-3: the stored URL gains a query AFTER apply's compile replay. Apply
    judges the carried targets against the execution plan's FRESH reading
    before its first write: refused value-free, zero mutation calls."""
    envelope, writes, state, binding = _apply_after_the_stored_url_changes(
        arm, "http://h.example.invalid/api/?x=1"
    )
    served = json.dumps(envelope)
    assert state["after_preflight"], "the preflight was not reached"
    assert writes == [], "apply wrote against a stored URL it never re-judged"
    assert envelope["_success"] is False
    assert "FANOUT_LITERAL_QUERY_NAME_UNSUPPORTED" in served, served[:800]
    assert binding in served
    assert "x=1" not in served


@pytest.mark.parametrize("arm", ["base_by_id", "generated_reused_by_name"])
def test_b15_03_control_an_unchanged_clean_stored_url_reaches_the_write_loop(arm):
    """Non-vacuity: the same harness with the stored URL unchanged passes the
    apply-time judgment and reaches the write loop."""
    envelope, writes, state, _binding = _apply_after_the_stored_url_changes(
        arm, "http://h.example.invalid/api/"
    )
    served = json.dumps(envelope)
    assert state["after_preflight"], "the preflight was not reached"
    assert "FANOUT_LITERAL_QUERY_NAME_UNSUPPORTED" not in served
    assert writes, "the control never reached the write loop: " + served[:800]


def test_b15_04_a_stored_base_refusal_and_a_path_refusal_are_told_apart():
    """B15-4: a stored base URL carrying a query/fragment is the CONNECTION's
    fault — pointed at the binding, remediated by selecting/configuring a
    query- and fragment-free connection, its URL never echoed; a path refusal
    stays at the path with the path remediation."""
    stored = _public(
        _reuse_bound_request("/v1/billing"), "compile",
        _account_patches("http://h.example.invalid/v1?mode=full#frag"),
    )
    served = json.dumps(stored)
    assert stored["_success"] is False
    assert "no query and no fragment" in served
    assert _RAW_BINDING_POINTER in served and _REUSE_PATH_POINTER not in served
    assert "h.example.invalid" not in served and "mode=full" not in served
    with pytest.raises(AuthoringWorkflowError) as caught:
        plan_authoring_request_v1(_reuse_bound_request("provider_key=sample"), profile="p")
    (path_diag,) = caught.value.diagnostics
    assert path_diag.path == _REUSE_PATH_POINTER
    assert "absolute path" in path_diag.remediation
    assert "no query and no fragment" not in path_diag.remediation


def test_b15_01_the_stored_url_reader_answers_only_what_the_account_read():
    """The fail-closed fallback is reserved for bindings the account could not
    resolve OR READ: a matching reading answers with no further read; a failed
    reading or a reading of ANOTHER component answers nothing (the judge then
    fails closed on the path); a key the route's readings do not carry is NOT
    read — the URL judge never reads a connection identity did not see (B17).
    The budgeted read itself: a failure or a raise answers ``None``, once per
    id. And the path-fault branch WITH a stored URL in hand."""
    from unittest import mock

    from boomi_mcp.authoring.workflow import (
        BudgetedComponentReads,
        judge_recipe_rest_targets,
        stored_connection_xml_reader,
    )
    from boomi_mcp.categories.components import _shared

    clean = _reused_rest_xml("http://h.example.invalid/api/")
    calls = []
    mode = {"raise": False}

    def get_xml(_client, component_id, *a, **k):
        calls.append(component_id)
        if mode["raise"]:
            raise RuntimeError("platform unavailable")
        return {"component_id": component_id, "version": 2, "xml": clean}

    key, rid = "shared_rest", _REUSED_REST_ID
    with mock.patch.object(_shared, "component_get_xml", get_xml):
        read = stored_connection_xml_reader({key: {"component_id": rid, "xml": clean, "read_failed": False}})
        assert read(key, rid) == clean
        assert read(key, _DB_CONN_ID) is None  # a reading of another component answers nothing
        assert stored_connection_xml_reader({key: {"component_id": rid, "read_failed": True}})(key, rid) is None
        assert stored_connection_xml_reader({})(key, rid) is None
        assert stored_connection_xml_reader({})(key, None) is None
        assert calls == []  # the reader never reads the account
        reads = BudgetedComponentReads(object())
        assert reads.fetch(rid)["xml"] == clean and reads.fetch(rid)["xml"] == clean and calls == [rid]
        mode["raise"] = True
        assert BudgetedComponentReads(object()).fetch(rid) is None
        assert BudgetedComponentReads(None).fetch(rid) is None and not BudgetedComponentReads(None).live

    comp = _rest_base({"reference_only": True, "connector_type": "rest", "component_id": rid}, component_id=rid)

    def judge(path, reader):
        judge_recipe_rest_targets(((key, path, "/p/path", "/p/binding"),), [comp], "reuse", None, reader)

    unreadable = lambda _key, _id: None  # noqa: E731
    with pytest.raises(AuthoringWorkflowError) as caught:
        judge("v1/billing", unreadable)
    (diag,) = caught.value.diagnostics
    assert diag.path == "/p/path" and "absolute path" in diag.remediation
    judge("/v1/billing", unreadable)  # fail-closed admits an absolute path
    judge("v1/billing", lambda _key, _id: clean)  # read: the relative path plans
    with pytest.raises(AuthoringWorkflowError) as caught:
        judge("@x.example.invalid/p", lambda _key, _id: _reused_rest_xml("http://h.example.invalid"))
    (diag,) = caught.value.diagnostics
    assert diag.path == "/p/path" and diag.cause_codes == ("PLAINTEXT_SECRET_REJECTED",)
    assert "h.example.invalid" not in diag.model_dump_json()
    with pytest.raises(AuthoringWorkflowError) as caught:
        judge("/v1/billing", lambda _key, _id: _reused_rest_xml("http://h.example.invalid/api#frag"))
    (diag,) = caught.value.diagnostics
    assert diag.path == "/p/binding" and "no query and no fragment" in diag.remediation


# ---------------------------------------------------------------------------
# B16 (Codex review of B15): witnesses, each failing at 572d2d8.
# ---------------------------------------------------------------------------

_DB_CONN_ID = "7773fe1e-9242-4155-9a1b-c6a610b1ef3b"


def _account_by_id(answers, listing):
    """The account: ``listing`` is ``[(name, id)]`` for the component listing,
    ``answers`` maps id -> the XML the read returns (pinned captures only)."""
    import contextlib
    from unittest import mock

    from boomi_mcp.categories import integration_builder
    from boomi_mcp.categories.components import _shared

    def paginate(*a, **k):
        return [{"component_id": entry[1], "name": entry[0],
                 "type": entry[2] if len(entry) > 2 else "connector-settings", "folder_name": "X"}
                for entry in listing]

    def get_xml(_client, component_id, *a, **k):
        if component_id not in answers:
            raise LookupError("no capture bound to this id")
        return {"component_id": component_id, "version": 1, "xml": answers[component_id],
                "type": "connector-settings"}

    stack = contextlib.ExitStack()
    stack.enter_context(mock.patch.object(integration_builder, "paginate_metadata", paginate))
    stack.enter_context(mock.patch.object(_shared, "component_get_xml", get_xml))
    stack.enter_context(mock.patch.object(integration_builder, "component_get_xml", get_xml))
    return stack


def _other_family_reuse(binding, path):
    raw, material = flagship_v2()
    _bind_target_1(raw, material, "reuse")
    material["targets"][1]["operation"]["path"] = path
    config = {"reference_only": True, "connector_type": "rest"}
    if binding == "config_id":
        config["component_id"] = _DB_CONN_ID
    base = _rest_base(config, name=_REUSED_NAME)
    return recipe_request(raw, material, base_components=[base.model_dump(mode="json")])


@pytest.mark.parametrize("binding", ["name", "config_id"])
@pytest.mark.parametrize("path", ["/v1/billing", "v1/billing"])
def test_b16_01_a_name_bound_rest_target_resolving_to_another_family_is_an_identity_refusal(binding, path):
    """B16-1: the REST target's binding (a name, or an id carried only in
    config) resolves to an account component of ANOTHER family (the pinned,
    proven-operable DATABASE connection capture). Connector identity validation
    reads the SAME resolved reading the URL judge reads, so compile refuses
    with the identity code — never a URL verdict (the relative-path arm would
    be refused by the URL judge if it ran first), never a compile success that
    apply later refuses (the absolute-path arm)."""
    db_xml = (_CAP159 / "db-conn-7773fe1e.xml").read_text()
    envelope = _public(
        _other_family_reuse(binding, path), "compile",
        _account_by_id({_DB_CONN_ID: db_xml}, [(_REUSED_NAME, _DB_CONN_ID)]),
    )
    served = json.dumps(envelope)
    assert envelope["_success"] is False, served[:600]
    assert "CONNECTOR_REPLAY_IDENTITY_MISMATCH" in served
    assert "FANOUT_LITERAL_QUERY_NAME_UNSUPPORTED" not in served


def test_b16_01_apply_refuses_a_family_flip_by_identity_before_judging_any_url():
    """Apply: the name resolves to the REST connection at compile and in the
    compile replay, then to the DATABASE capture for the execution read. The
    identity resolution refuses first (pre-write, zero writes); the URL judge,
    which would refuse the relative path on the unreadable base, never
    speaks."""
    from unittest import mock

    from boomi_mcp.authoring import workflow
    from boomi_mcp.categories import integration_builder

    db_xml = (_CAP159 / "db-conn-7773fe1e.xml").read_text()
    answers = {_REUSED_REST_ID: _reused_rest_xml("http://h.example.invalid/api/"), _DB_CONN_ID: db_xml}
    listing = [(_REUSED_NAME, _REUSED_REST_ID)]
    request = _name_bound_reuse("base", "v1/billing")
    client = mock.MagicMock()
    with _account_by_id(answers, listing):
        compiled, _ = compile_authoring_request_v1(request, boomi_client=client, profile="p")
    real_preflight = workflow.preflight_typed_apply_v1
    state = {"after_preflight": False}

    def preflight(*a, **k):
        bundle = real_preflight(*a, **k)
        state["after_preflight"] = True
        # B20-2: a name rebinding to ANOTHER id is refused earlier as
        # AUTHORING_PLAN_STALE (test_b20_02); here the bound id stays and only
        # the execution read's answer for it disagrees with the declaration.
        answers[_REUSED_REST_ID] = db_xml.replace(_DB_CONN_ID, _REUSED_REST_ID)
        return bundle

    payload = request.model_dump(mode="json", exclude_none=True)
    payload["expected_capability_revision"] = compiled.revision_binding.capability_revision
    payload["expected_compile_hash"] = compiled.revision_binding.compile_hash
    writes = []
    with _account_by_id(answers, listing), \
            mock.patch.object(workflow, "preflight_typed_apply_v1", preflight), \
            mock.patch.object(integration_builder, "_execute_component",
                              side_effect=lambda *a, **k: writes.append("component") or {"_success": True, "component_id": "x"}), \
            mock.patch.object(integration_builder, "create_component",
                              side_effect=lambda *a, **k: writes.append("create") or {"_success": True, "component_id": "y"}), \
            mock.patch.object(integration_builder, "update_component",
                              side_effect=lambda *a, **k: writes.append("update") or {"_success": True}):
        envelope = integration_builder.build_integration_action(
            client, "p", "apply", {"authoring_request": payload, "dry_run": False}
        )
    served = json.dumps(envelope)
    assert state["after_preflight"], "the preflight was not reached"
    assert writes == []
    assert envelope["_success"] is False
    assert "CONNECTOR_REPLAY_IDENTITY_MISMATCH" in served, served[:800]
    assert "FANOUT_LITERAL_QUERY_NAME_UNSUPPORTED" not in served


@pytest.mark.parametrize("stored", ["", "   "])
def test_b16_02_a_blank_stored_url_is_unavailable_and_fails_closed(stored):
    """B16-2: a blank or whitespace stored ``url`` is NOT a base URL; the path
    is then judged by the fail-closed rule, so a relative path is refused."""
    envelope = _public(
        _name_bound_reuse("base", "v1/billing"), "compile",
        _account_by_id({_REUSED_REST_ID: _reused_rest_xml(stored)}, [(_REUSED_NAME, _REUSED_REST_ID)]),
    )
    served = json.dumps(envelope)
    assert envelope["_success"] is False, served[:600]
    assert "FANOUT_LITERAL_QUERY_NAME_UNSUPPORTED" in served and _REUSE_PATH_POINTER in served


def test_b16_02_control_a_blank_stored_url_admits_an_absolute_path():
    envelope = _public(
        _name_bound_reuse("base", "/v1/billing"), "compile",
        _account_by_id({_REUSED_REST_ID: _reused_rest_xml("  ")}, [(_REUSED_NAME, _REUSED_REST_ID)]),
    )
    assert envelope["_success"] is True, json.dumps(envelope)[:600]


def test_b16_03_stored_url_reads_share_one_deadline_budget(monkeypatch):
    """B16-3: account reads share ONE wall-clock budget (the component GET
    deadline, as ``_enrich_references`` does): each read is handed the
    remaining slice and no new read starts once it is spent. A target whose
    connection was not read has an UNKNOWN base, so the fail-closed rule judges
    it (a relative path is refused; nothing about the account is echoed)."""
    import time
    from unittest import mock

    from boomi_mcp.authoring.workflow import (
        BudgetedComponentReads,
        judge_recipe_rest_targets,
        stored_connection_xml_reader,
    )
    from boomi_mcp.categories.components import _shared

    monkeypatch.setenv(_shared._DEADLINE_ENV, "2")
    deadlines = []

    def stalled(_client, component_id, deadline_seconds=None, *a, **k):
        # A read that stalls 1.2 s, cut off at its deadline like the real one.
        deadlines.append(deadline_seconds)
        time.sleep(min(1.2, deadline_seconds))
        raise _shared.ComponentGetDeadlineExceeded(component_id, deadline_seconds or 0, 1.2)

    ids = ["0000000{0}-0000-4000-8000-000000000000".format(n) for n in range(3)]
    with mock.patch.object(_shared, "component_get_xml", stalled):
        reads = BudgetedComponentReads(object())
        assert [reads.fetch(cid) for cid in ids] == [None, None, None]
    # The 2 s budget: the first read gets all of it and holds 1.2 s; the second
    # gets the fractional remainder (B18-1) and spends it; the third never starts.
    assert len(deadlines) == 2 and deadlines[0] == 2, deadlines
    assert 0 < deadlines[1] <= 0.85, deadlines
    comps = [
        IntegrationComponentSpec(key="rest_{0}".format(n), type="connector-settings", name="R{0}".format(n),
                                 action="create", component_id=cid,
                                 config={"reference_only": True, "connector_type": "rest", "component_id": cid})
        for n, cid in enumerate(ids)
    ]
    targets = [("rest_{0}".format(n), "/v1/x", "/p/{0}".format(n), "/b/{0}".format(n)) for n in range(3)]
    judge_recipe_rest_targets(targets, comps, "reuse", None, stored_connection_xml_reader({}))
    targets[2] = ("rest_2", "v1/x", "/p/2", "/b/2")
    with pytest.raises(AuthoringWorkflowError) as caught:
        judge_recipe_rest_targets(targets, comps, "reuse", None, stored_connection_xml_reader({}))
    (diag,) = caught.value.diagnostics
    assert diag.path == "/p/2" and "absolute path" in diag.remediation


# ---------------------------------------------------------------------------
# B17 (Codex review of B16): witnesses, each failing at cf2f7dd.
# ---------------------------------------------------------------------------

_ROOT_NAME = "M8DEMO DB to API Sync"
_ROOT_ID = "6f1d2c3b-0000-4000-8000-0000000000aa"
_DB_SOURCE_ID = "6f1d2c3b-0000-4000-8000-0000000000db"
_ORDERS_ID = "6f1d2c3b-0000-4000-8000-0000000000a0"


def _stalling(record, stall=1.2):
    """A component read that stalls ``stall`` seconds and then times out,
    recording ``(component_id, deadline_seconds)`` and the time it held."""
    import time

    from boomi_mcp.categories.components import _shared

    def get_xml(_client, component_id, deadline_seconds=None, *a, **k):
        record["calls"].append((component_id, deadline_seconds))
        started = time.monotonic()
        time.sleep(min(stall, deadline_seconds) if deadline_seconds is not None else stall)
        record["held"] += time.monotonic() - started
        raise _shared.ComponentGetDeadlineExceeded(component_id, deadline_seconds or 0, stall)

    return get_xml


def _three_declared_reuses():
    """Source DB, target 0 and target 1 each reused BY ID: three primary reads."""
    raw, material = flagship_v2()
    raw["source"]["binding_mode"] = "reuse"
    raw["source"].pop("auth_mode", None)
    material["source"]["connection"] = {"mode": "reuse", "component_id": _DB_SOURCE_ID}
    raw["targets"][0]["binding_mode"] = "reuse"
    material["targets"][0]["connection"] = {"mode": "reuse", "component_id": _ORDERS_ID}
    _bind_target_1(raw, material, "reuse")
    base = _rest_base(
        {"reference_only": True, "connector_type": "rest", "component_id": _REUSED_REST_ID},
        component_id=_REUSED_REST_ID,
    )
    return recipe_request(raw, material, base_components=[base.model_dump(mode="json")])


def test_b17_01_compile_spends_one_read_budget_across_primary_and_target_reads(monkeypatch):
    """B17-1 (compile, public tool boundary): the declared-id (primary) reads
    and the target reads share ONE budget — every read is handed the remaining
    slice, no read starts once it is spent, and the refusal is structured
    (identity unavailable) and arrives within the budget."""
    from unittest import mock

    from boomi_mcp.categories import integration_builder
    from boomi_mcp.categories.components import _shared

    monkeypatch.setenv(_shared._DEADLINE_ENV, "2")
    record = {"calls": [], "held": 0.0}
    with mock.patch.object(integration_builder, "paginate_metadata", lambda *a, **k: []), \
            mock.patch.object(_shared, "component_get_xml", _stalling(record)):
        envelope = integration_builder.build_integration_action(
            mock.MagicMock(), "p", "compile",
            {"authoring_request": _three_declared_reuses().model_dump(mode="json", exclude_none=True)},
        )
    served = json.dumps(envelope)
    assert envelope["_success"] is False
    assert "CONNECTOR_REPLAY_IDENTITY_UNAVAILABLE" in served, served[:600]
    assert all(deadline is not None for _cid, deadline in record["calls"]), record["calls"]
    # 2 s budget, 1.2 s stalls cut off at their deadline: the second read gets
    # the fractional remainder (B18-1) and no third read starts.
    assert len(record["calls"]) == 2, record["calls"]
    assert record["held"] <= 2.5, record["held"]


def test_b17_01_apply_spends_one_read_budget_across_its_connector_reads(monkeypatch):
    """B17-1 (apply, public tool boundary): the compile replay passes against a
    healthy account; then every read stalls. Apply's primary connector reads
    and its target reads share ONE budget, and the refusal is pre-write."""
    from unittest import mock

    from boomi_mcp.authoring import workflow
    from boomi_mcp.categories import integration_builder
    from boomi_mcp.categories.components import _shared

    rest = _reused_rest_xml("http://h.example.invalid/api/")
    db = (_CAP159 / "db-conn-7773fe1e.xml").read_text()
    healthy = {_DB_SOURCE_ID: db, _ORDERS_ID: rest, _REUSED_REST_ID: rest}
    request = _three_declared_reuses()
    client = mock.MagicMock()
    with _account_by_id(healthy, []):
        compiled, _ = compile_authoring_request_v1(request, boomi_client=client, profile="p")
    monkeypatch.setenv(_shared._DEADLINE_ENV, "2")
    record = {"calls": [], "held": 0.0}
    stalled = _stalling(record)
    state = {"stall": False}
    real_get = _shared.component_get_xml

    def get_xml(_client, component_id, *a, **k):
        if state["stall"]:
            return stalled(_client, component_id, *a, **k)
        return {"component_id": component_id, "version": 1, "xml": healthy[component_id],
                "type": "connector-settings"}

    real_preflight = workflow.preflight_typed_apply_v1

    def preflight(*a, **k):
        bundle = real_preflight(*a, **k)
        state["stall"] = True
        return bundle

    payload = request.model_dump(mode="json", exclude_none=True)
    payload["expected_capability_revision"] = compiled.revision_binding.capability_revision
    payload["expected_compile_hash"] = compiled.revision_binding.compile_hash
    writes = []
    assert real_get is not None
    with mock.patch.object(integration_builder, "paginate_metadata", lambda *a, **k: []), \
            mock.patch.object(_shared, "component_get_xml", get_xml), \
            mock.patch.object(integration_builder, "component_get_xml", get_xml), \
            mock.patch.object(workflow, "preflight_typed_apply_v1", preflight), \
            mock.patch.object(integration_builder, "_execute_component",
                              side_effect=lambda *a, **k: writes.append("component") or {"_success": True, "component_id": "x"}), \
            mock.patch.object(integration_builder, "create_component",
                              side_effect=lambda *a, **k: writes.append("create") or {"_success": True, "component_id": "y"}), \
            mock.patch.object(integration_builder, "update_component",
                              side_effect=lambda *a, **k: writes.append("update") or {"_success": True}):
        envelope = integration_builder.build_integration_action(
            client, "p", "apply", {"authoring_request": payload, "dry_run": False}
        )
    served = json.dumps(envelope)
    assert state["stall"], "the preflight was not reached"
    assert writes == []
    assert envelope["_success"] is False
    assert "CONNECTOR_REPLAY_IDENTITY_UNAVAILABLE" in served, served[:600]
    assert all(deadline is not None for _cid, deadline in record["calls"]), record["calls"]
    # 2 s budget, 1.2 s stalls cut off at their deadline: the second read gets
    # the fractional remainder (B18-1) and no third read starts.
    assert len(record["calls"]) == 2, record["calls"]
    assert record["held"] <= 2.5, record["held"]


def test_b17_02_a_family_flip_with_every_root_reused_is_refused_before_any_write():
    """B17-2: every process root is REUSED (the root's name matches an account
    process), so apply compiles no root and its pre-write connector reading
    used to be empty. The name-bound REST target resolves to the REST
    connection through the compile replay, then to the DATABASE capture for the
    execution read: refused by identity before any sibling write."""
    from unittest import mock

    from boomi_mcp.authoring import workflow
    from boomi_mcp.categories import integration_builder

    db_xml = (_CAP159 / "db-conn-7773fe1e.xml").read_text()
    answers = {_REUSED_REST_ID: _reused_rest_xml("http://h.example.invalid/api/"), _DB_CONN_ID: db_xml}
    listing = [(_REUSED_NAME, _REUSED_REST_ID), (_ROOT_NAME, _ROOT_ID, "process")]
    request = _name_bound_reuse("base", "/v1/billing")
    client = mock.MagicMock()
    with _account_by_id(answers, listing):
        compiled, _ = compile_authoring_request_v1(request, boomi_client=client, profile="p")
    real_preflight = workflow.preflight_typed_apply_v1
    state = {"after_preflight": False}

    def preflight(*a, **k):
        bundle = real_preflight(*a, **k)
        state["after_preflight"] = True
        # B20-2: a name rebinding to ANOTHER id is refused earlier as
        # AUTHORING_PLAN_STALE (test_b20_02); here the bound id stays and only
        # the execution read's answer for it disagrees with the declaration.
        answers[_REUSED_REST_ID] = db_xml.replace(_DB_CONN_ID, _REUSED_REST_ID)
        return bundle

    payload = request.model_dump(mode="json", exclude_none=True)
    payload["expected_capability_revision"] = compiled.revision_binding.capability_revision
    payload["expected_compile_hash"] = compiled.revision_binding.compile_hash
    writes = []
    plans = []
    real_build_plan = integration_builder._build_plan

    def build_plan(*a, **k):
        planned = real_build_plan(*a, **k)
        plans.append(planned)
        return planned

    with _account_by_id(answers, listing), \
            mock.patch.object(workflow, "preflight_typed_apply_v1", preflight), \
            mock.patch.object(integration_builder, "_build_plan", build_plan), \
            mock.patch.object(integration_builder, "_execute_component",
                              side_effect=lambda *a, **k: writes.append("component") or {"_success": True, "component_id": "x"}), \
            mock.patch.object(integration_builder, "create_component",
                              side_effect=lambda *a, **k: writes.append("create") or {"_success": True, "component_id": "y"}), \
            mock.patch.object(integration_builder, "update_component",
                              side_effect=lambda *a, **k: writes.append("update") or {"_success": True}):
        envelope = integration_builder.build_integration_action(
            client, "p", "apply", {"authoring_request": payload, "dry_run": False}
        )
    served = json.dumps(envelope)
    assert state["after_preflight"], "the preflight was not reached: " + served[:600]
    (root_step,) = [step for step in plans[-1]["steps"] if step["key"] == "main_process"]
    assert root_step["planned_action"] == "reuse", root_step  # every root reused (non-vacuity)
    assert writes == [], "apply wrote siblings against an unexamined connection"
    assert envelope["_success"] is False
    assert "CONNECTOR_REPLAY_IDENTITY_MISMATCH" in served, served[:800]


@pytest.mark.parametrize("binding", ["name", "config_id"])
def test_b17_03_a_resolved_binding_whose_read_fails_is_identity_unavailable(binding):
    """B17-3: the binding RESOLVES (by name, or by a config-only id) but the
    account read fails. That is not an unchecked component: compile refuses
    with CONNECTOR_REPLAY_IDENTITY_UNAVAILABLE, as apply would."""
    from unittest import mock

    from boomi_mcp.categories import integration_builder
    from boomi_mcp.categories.components import _shared

    def failing(*a, **k):
        raise RuntimeError("platform unavailable")

    request = _other_family_reuse(binding, "/v1/billing")
    listing = [{"component_id": _DB_CONN_ID, "name": _REUSED_NAME, "type": "connector-settings",
                "folder_name": "X"}]
    with mock.patch.object(integration_builder, "paginate_metadata", lambda *a, **k: [dict(x) for x in listing]), \
            mock.patch.object(_shared, "component_get_xml", failing), \
            mock.patch.object(integration_builder, "component_get_xml", failing):
        envelope = integration_builder.build_integration_action(
            mock.MagicMock(), "p", "compile",
            {"authoring_request": request.model_dump(mode="json", exclude_none=True)},
        )
    served = json.dumps(envelope)
    assert envelope["_success"] is False, served[:600]
    assert "CONNECTOR_REPLAY_IDENTITY_UNAVAILABLE" in served


def test_b17_03_control_offline_an_unresolved_binding_keeps_the_fallback():
    """Control: with no account the binding is unresolved; the fail-closed
    fallback still admits an absolute path (no identity refusal is invented)."""
    result, _ = plan_authoring_request_v1(_other_family_reuse("name", "/v1/billing"), profile="p")
    assert result.validation_report.is_valid
    assert not result.errors


def test_b17_04_url_and_identity_refusals_of_different_targets_are_reported_together():
    """B17-4: target 0 is bound by name to an account component of another
    family (identity refusal); target 1 reuses by id a connection whose stored
    base carries a query (URL refusal). Both reach the caller in ONE report;
    target 0 gets no duplicate URL error."""
    raw, material = flagship_v2()
    raw["targets"][0]["binding_mode"] = "reuse"
    material["targets"][0]["connection"] = {"mode": "reuse", "component_name": "Existing Orders"}
    material["targets"][0]["operation"]["path"] = "v1/orders"
    _bind_target_1(raw, material, "reuse")
    base = _rest_base(
        {"reference_only": True, "connector_type": "rest", "component_id": _REUSED_REST_ID},
        component_id=_REUSED_REST_ID,
    )
    request = recipe_request(raw, material, base_components=[base.model_dump(mode="json")])
    db_xml = (_CAP159 / "db-conn-7773fe1e.xml").read_text()
    envelope = _public(
        request, "compile",
        _account_by_id(
            {_DB_CONN_ID: db_xml, _REUSED_REST_ID: _reused_rest_xml("http://h.example.invalid/v1?mode=full")},
            [("Existing Orders", _DB_CONN_ID)],
        ),
    )
    served = json.dumps(envelope)
    assert envelope["_success"] is False
    assert "CONNECTOR_REPLAY_IDENTITY_MISMATCH" in served, served[:800]
    assert "FANOUT_LITERAL_QUERY_NAME_UNSUPPORTED" in served, served[:800]
    assert _RAW_BINDING_POINTER in served
    assert "/component_material/targets/0/" not in served


@pytest.mark.parametrize("path", ["v1/x", "provider_key=sample"])
def test_qa_r16_01_an_unreadable_stored_url_is_base_unknown_at_plan_and_compile(path):
    """QA-159-S1-r16-01: a reused connection the account cannot read (an id no
    account holds) leaves its stored URL UNKNOWN, so the fail-closed path rule
    applies — at plan and at compile, at the path pointer with the path
    remediation — beside the identity-unavailable refusal."""
    from unittest import mock

    from boomi_mcp.categories import integration_builder
    from boomi_mcp.categories.components import _shared

    def missing(*a, **k):
        raise LookupError("no component with this id")

    request = _reuse_bound_request(path)
    with mock.patch.object(integration_builder, "paginate_metadata", lambda *a, **k: []), \
            mock.patch.object(_shared, "component_get_xml", missing), \
            pytest.raises(AuthoringWorkflowError) as caught:
        plan_authoring_request_v1(request, boomi_client=mock.MagicMock(), profile="p")
    (diag,) = [d for d in caught.value.diagnostics if d.path == _REUSE_PATH_POINTER]
    assert "FANOUT_LITERAL_QUERY_NAME_UNSUPPORTED" in diag.cause_codes
    assert "absolute path" in diag.remediation
    assert any(d.code == "CONNECTOR_REPLAY_IDENTITY_UNAVAILABLE" for d in caught.value.diagnostics)
    for action in ("plan", "compile"):  # the public tool boundary QA drives
        envelope = _public(request, action, _account_by_id({}, []))
        served = json.dumps(envelope)
        assert envelope["_success"] is False, (action, served[:600])
        assert "CONNECTOR_REPLAY_IDENTITY_UNAVAILABLE" in served
        assert "FANOUT_LITERAL_QUERY_NAME_UNSUPPORTED" in served and _REUSE_PATH_POINTER in served
        assert "absolute path" in served


# ---------------------------------------------------------------------------
# B18 (Codex review of B17): witnesses, each failing at 08ac0bf.
# ---------------------------------------------------------------------------

_UNUSED_PROFILE_ID = "6f1d2c3b-0000-4000-8000-0000000000fe"


def _healthy_three():
    rest = _reused_rest_xml("http://h.example.invalid/api/")
    db = (_CAP159 / "db-conn-7773fe1e.xml").read_text()
    return {_DB_SOURCE_ID: db, _ORDERS_ID: rest, _REUSED_REST_ID: rest}


def test_b18_01_a_one_second_budget_reads_every_healthy_reused_connector(monkeypatch):
    """B18-1: with BOOMI_COMPONENT_GET_DEADLINE_SECONDS=1, the first healthy
    read leaves a FRACTIONAL remainder. It is handed to the next read as its
    timeout (the GET deadline honours fractions), so three healthy reused
    connectors all read and the compile succeeds — as at cf2f7dd."""
    from boomi_mcp.categories.components import _shared

    monkeypatch.setenv(_shared._DEADLINE_ENV, "1")
    envelope = _public(_three_declared_reuses(), "compile", _account_by_id(_healthy_three(), []))
    assert envelope["_success"] is True, json.dumps(envelope)[:800]


def test_b18_02_an_unused_profile_read_cannot_starve_the_connector_reads(monkeypatch):
    """B18-2: the IDENTITY collector reads only what the connector snapshot
    consumes — a declared-id JSON profile is never fetched by it. (B20-4 then
    put profile DISCOVERY, which does consume every reused profile, on the same
    phase budget; so end to end a slow profile read spends from that one budget
    and is read ONCE, and a healthy connection still reads within it.)"""
    import time
    from unittest import mock

    from boomi_mcp.authoring.connector_resolution_snapshot import live_readings_for_declared_components
    from boomi_mcp.categories import integration_builder
    from boomi_mcp.categories.components import _shared
    from test_issue_157_governance import _op, _request as _gov_request, _unit

    profile = {"key": "unused_profile", "type": "profile.json", "action": "create",
               "component_id": _UNUSED_PROFILE_ID,
               "config": {"reference_only": True, "component_name": "Unused Profile"}}
    conn = {"key": "conn", "type": "connector-settings", "name": "Reused REST", "action": "create",
            "component_id": _REUSED_REST_ID, "config": {"reference_only": True, "connector_type": "rest"}}

    class _Recorder:
        def __init__(self):
            self.asked = []

        def fetch(self, component_id):
            self.asked.append(component_id)
            return None

    recorder = _Recorder()
    live_readings_for_declared_components(
        object(),
        [IntegrationComponentSpec.model_validate(profile), IntegrationComponentSpec.model_validate(conn)],
        reads=recorder,
    )
    assert recorder.asked == [_REUSED_REST_ID], recorder.asked

    monkeypatch.setenv(_shared._DEADLINE_ENV, "2")
    rest = _reused_rest_xml("http://h.example.invalid/api/")
    asked = []

    def get_xml(_client, component_id, deadline_seconds=None, *a, **k):
        asked.append((component_id, deadline_seconds))
        if component_id == _UNUSED_PROFILE_ID:
            time.sleep(min(0.5, deadline_seconds or 0.5))
            raise _shared.ComponentGetDeadlineExceeded(component_id, deadline_seconds or 0, 0.5)
        assert component_id == _REUSED_REST_ID, component_id
        return {"component_id": component_id, "version": 1, "xml": rest, "type": "connector-settings"}

    request = _gov_request([_unit(name="P")], [profile, conn, _op()])
    with mock.patch.object(integration_builder, "paginate_metadata", lambda *a, **k: []), \
            mock.patch.object(_shared, "component_get_xml", get_xml), \
            mock.patch.object(integration_builder, "component_get_xml", get_xml):
        envelope = integration_builder.build_integration_action(
            mock.MagicMock(), "p", "compile",
            {"authoring_request": request.model_dump(mode="json", exclude_none=True)},
        )
    budgeted = [(c, d) for c, d in asked if d is not None]
    assert [c for c, _d in budgeted].count(_UNUSED_PROFILE_ID) <= 1, asked  # read once per phase
    assert _REUSED_REST_ID in [c for c, _d in budgeted], asked  # the connection reads within budget
    assert envelope["_success"] is True, json.dumps(envelope)[:800]


@pytest.mark.parametrize("binding", ["name", "config_id"])
def test_b18_03_an_unreadable_resolved_target_gets_its_own_remedy(binding):
    """B18-3: an UNAVAILABLE refusal for a reused component the account did not
    answer is remedied by retrying or rebinding — never the submitted-XML
    sentence (nothing was submitted). Value-free."""
    from unittest import mock

    from boomi_mcp.categories import integration_builder
    from boomi_mcp.categories.components import _shared

    def failing(*a, **k):
        raise RuntimeError("platform unavailable")

    request = _other_family_reuse(binding, "/v1/billing")
    listing = [{"component_id": _DB_CONN_ID, "name": _REUSED_NAME, "type": "connector-settings",
                "folder_name": "X"}]
    with mock.patch.object(integration_builder, "paginate_metadata", lambda *a, **k: [dict(x) for x in listing]), \
            mock.patch.object(_shared, "component_get_xml", failing), \
            mock.patch.object(integration_builder, "component_get_xml", failing):
        envelope = integration_builder.build_integration_action(
            mock.MagicMock(), "p", "compile",
            {"authoring_request": request.model_dump(mode="json", exclude_none=True)},
        )
    (diag,) = [d for d in envelope["authoring_diagnostics"] if d["code"] == "CONNECTOR_REPLAY_IDENTITY_UNAVAILABLE"]
    assert "Submitted component XML" not in diag["remediation"]
    assert "retry" in diag["remediation"].lower()
    assert _DB_CONN_ID not in json.dumps(envelope)


def test_b18_04_apply_reports_a_family_flip_and_a_stored_query_together():
    """B18-4: every root reused; after the compile replay target 1's name flips
    to the DATABASE capture and target 0's stored URL acquires a query. Apply's
    pre-write refusal carries BOTH — the identity mismatch and target 0's
    binding diagnostic — from the same readings, with zero writes."""
    from unittest import mock

    from boomi_mcp.authoring import workflow
    from boomi_mcp.categories import integration_builder

    db_xml = (_CAP159 / "db-conn-7773fe1e.xml").read_text()
    answers = {
        _REUSED_REST_ID: _reused_rest_xml("http://h.example.invalid/api/"),
        _ORDERS_ID: _reused_rest_xml("http://o.example.invalid/api/"),
        _DB_CONN_ID: db_xml,
    }
    listing = [(_REUSED_NAME, _REUSED_REST_ID), (_ROOT_NAME, _ROOT_ID, "process")]
    raw, material = flagship_v2()
    raw["targets"][0]["binding_mode"] = "reuse"
    material["targets"][0]["connection"] = {"mode": "reuse", "component_id": _ORDERS_ID}
    _bind_target_1(raw, material, "reuse")
    material["targets"][1]["operation"]["path"] = "/v1/billing"
    base = _rest_base({"reference_only": True, "connector_type": "rest"}, name=_REUSED_NAME)
    request = recipe_request(raw, material, base_components=[base.model_dump(mode="json")])
    client = mock.MagicMock()
    with _account_by_id(answers, listing):
        compiled, _ = compile_authoring_request_v1(request, boomi_client=client, profile="p")
    real_preflight = workflow.preflight_typed_apply_v1
    state = {"after_preflight": False}

    def preflight(*a, **k):
        bundle = real_preflight(*a, **k)
        state["after_preflight"] = True
        # B20-2: a name rebinding to ANOTHER id is refused earlier as
        # AUTHORING_PLAN_STALE (test_b20_02); here the bound id stays and only
        # the execution read's answer for it disagrees with the declaration.
        answers[_REUSED_REST_ID] = db_xml.replace(_DB_CONN_ID, _REUSED_REST_ID)
        answers[_ORDERS_ID] = _reused_rest_xml("http://o.example.invalid/api/?x=1")
        return bundle

    payload = request.model_dump(mode="json", exclude_none=True)
    payload["expected_capability_revision"] = compiled.revision_binding.capability_revision
    payload["expected_compile_hash"] = compiled.revision_binding.compile_hash
    writes = []
    plans = []
    real_build_plan = integration_builder._build_plan

    def build_plan(*a, **k):
        planned = real_build_plan(*a, **k)
        plans.append(planned)
        return planned

    with _account_by_id(answers, listing), \
            mock.patch.object(workflow, "preflight_typed_apply_v1", preflight), \
            mock.patch.object(integration_builder, "_build_plan", build_plan), \
            mock.patch.object(integration_builder, "_execute_component",
                              side_effect=lambda *a, **k: writes.append("component") or {"_success": True, "component_id": "x"}), \
            mock.patch.object(integration_builder, "create_component",
                              side_effect=lambda *a, **k: writes.append("create") or {"_success": True, "component_id": "y"}), \
            mock.patch.object(integration_builder, "update_component",
                              side_effect=lambda *a, **k: writes.append("update") or {"_success": True}):
        envelope = integration_builder.build_integration_action(
            client, "p", "apply", {"authoring_request": payload, "dry_run": False}
        )
    served = json.dumps(envelope)
    assert state["after_preflight"], "the preflight was not reached: " + served[:600]
    (root_step,) = [step for step in plans[-1]["steps"] if step["key"] == "main_process"]
    assert root_step["planned_action"] == "reuse", root_step
    assert writes == []
    assert envelope["_success"] is False
    assert "CONNECTOR_REPLAY_IDENTITY_MISMATCH" in served, served[:800]
    assert "FANOUT_LITERAL_QUERY_NAME_UNSUPPORTED" in served, served[:800]
    assert "/intent/invocations/0/component_material/targets/0/connection" in served
    assert _RAW_BINDING_POINTER not in served  # no URL verdict for the mismatched target
    assert "x=1" not in served


def test_b18r_an_unparseable_stored_component_gets_an_account_remedy():
    """B18 review P3: the account answered but its stored document did not parse;
    the remedy points at the STORED component, never at submitted XML."""
    from boomi_mcp.authoring.connector_resolution_snapshot import (
        UNPARSEABLE_STORED_COMPONENT_REMEDIATION,
        ConnectorIdentityError,
        build_connector_resolution_snapshot,
    )
    from boomi_mcp.authoring.workflow import connector_identity_diagnostics

    base = _rest_base({"reference_only": True, "connector_type": "rest"}, component_id="id-1")
    with pytest.raises(ConnectorIdentityError) as refused:
        build_connector_resolution_snapshot(
            [base],
            live_component_xml={"shared_rest": "<not-a-component"},
            reused_keys=["shared_rest"],
        )
    diagnostics, _ = connector_identity_diagnostics(refused.value)
    (diagnostic,) = diagnostics
    assert diagnostic.remediation == UNPARSEABLE_STORED_COMPONENT_REMEDIATION
    assert "Submitted component XML" not in diagnostic.remediation


# ---------------------------------------------------------------------------
# B20 (architect final evaluation, r3): witnesses, each failing at 7d21498.
# ---------------------------------------------------------------------------

_B_REST_ID = "6f1d2c3b-0000-4000-8000-0000000000bb"
_DB_PROFILE_ID = "6f1d2c3b-0000-4000-8000-0000000000d1"
_JSON_PROFILE_ID = "6f1d2c3b-0000-4000-8000-0000000000d2"


@pytest.mark.parametrize("path", ["provider_key=sample", "v1/x"])
def test_b20_01_an_extension_placeholder_stored_url_is_base_unknown(path):
    """B20-1 (ARCH-r2-01): a reused REST connection whose stored url is the
    builder's unresolved-endpoint placeholder (``SET_BY_EXTENSION``) has NO
    concrete base: the fail-closed path rule applies."""
    from boomi_mcp.categories.components.builders.connector_builder import SET_BY_EXTENSION

    envelope = _public(
        _reuse_bound_request(path), "compile",
        _account_by_id({_REUSED_REST_ID: _reused_rest_xml(SET_BY_EXTENSION)}, []),
    )
    served = json.dumps(envelope)
    assert envelope["_success"] is False, served[:600]
    assert "FANOUT_LITERAL_QUERY_NAME_UNSUPPORTED" in served and _REUSE_PATH_POINTER in served
    assert SET_BY_EXTENSION not in served


def test_b20_01_control_a_placeholder_base_admits_an_absolute_path():
    from boomi_mcp.categories.components.builders.connector_builder import SET_BY_EXTENSION

    envelope = _public(
        _reuse_bound_request("/v1/billing"), "compile",
        _account_by_id({_REUSED_REST_ID: _reused_rest_xml(SET_BY_EXTENSION)}, []),
    )
    assert envelope["_success"] is True, json.dumps(envelope)[:600]


def test_b20_02_apply_refuses_a_name_that_rebinds_after_the_compile_replay():
    """B20-2: the compile replay binds the name to REST connection A; the
    execution plan binds the SAME name to a different REST connection B (clean
    URL, same family, the unwritten set unchanged). Apply refuses with
    AUTHORING_PLAN_STALE before any write, value-free."""
    from unittest import mock

    from boomi_mcp.authoring import workflow
    from boomi_mcp.categories import integration_builder

    clean = _reused_rest_xml("http://h.example.invalid/api/")
    answers = {_REUSED_REST_ID: clean, _B_REST_ID: clean.replace(_REUSED_REST_ID, _B_REST_ID)}
    listing = [(_REUSED_NAME, _REUSED_REST_ID)]
    request = _name_bound_reuse("base", "/v1/billing")
    client = mock.MagicMock()
    with _account_by_id(answers, listing):
        compiled, _ = compile_authoring_request_v1(request, boomi_client=client, profile="p")
    real_preflight = workflow.preflight_typed_apply_v1
    state = {"after_preflight": False}

    def preflight(*a, **k):
        bundle = real_preflight(*a, **k)
        state["after_preflight"] = True
        listing[0] = (_REUSED_NAME, _B_REST_ID)
        return bundle

    payload = request.model_dump(mode="json", exclude_none=True)
    payload["expected_capability_revision"] = compiled.revision_binding.capability_revision
    payload["expected_compile_hash"] = compiled.revision_binding.compile_hash
    writes = []
    with _account_by_id(answers, listing), \
            mock.patch.object(workflow, "preflight_typed_apply_v1", preflight), \
            mock.patch.object(integration_builder, "_execute_component",
                              side_effect=lambda *a, **k: writes.append("component") or {"_success": True, "component_id": "x"}), \
            mock.patch.object(integration_builder, "create_component",
                              side_effect=lambda *a, **k: writes.append("create") or {"_success": True, "component_id": "y"}), \
            mock.patch.object(integration_builder, "update_component",
                              side_effect=lambda *a, **k: writes.append("update") or {"_success": True}):
        envelope = integration_builder.build_integration_action(
            client, "p", "apply", {"authoring_request": payload, "dry_run": False}
        )
    served = json.dumps(envelope)
    assert state["after_preflight"], "the preflight was not reached: " + served[:600]
    assert writes == [], "apply wrote against a binding the compile never covered"
    assert envelope["_success"] is False
    assert "AUTHORING_PLAN_STALE" in served, served[:800]
    assert _B_REST_ID not in served and _REUSED_REST_ID not in served


def test_b20_02_sibling_a_reused_root_that_rebinds_after_the_compile_replay_is_refused():
    """B20-2 sibling sweep: the binding recheck covers EVERY participant, not
    only connections. The process root is reused by NAME; after the compile
    replay the same name binds a different process. Refused AUTHORING_PLAN_STALE
    before any write (the generated siblings would otherwise be created)."""
    from unittest import mock

    from boomi_mcp.authoring import workflow
    from boomi_mcp.categories import integration_builder

    other_root = "6f1d2c3b-0000-4000-8000-0000000000ab"
    listing = [(_ROOT_NAME, _ROOT_ID, "process")]
    raw, material = flagship_v2()
    request = recipe_request(raw, material)
    client = mock.MagicMock()
    with _account_by_id({}, listing):
        compiled, _ = compile_authoring_request_v1(request, boomi_client=client, profile="p")
    real_preflight = workflow.preflight_typed_apply_v1
    state = {"after_preflight": False}

    def preflight(*a, **k):
        bundle = real_preflight(*a, **k)
        state["after_preflight"] = True
        listing[0] = (_ROOT_NAME, other_root, "process")
        return bundle

    payload = request.model_dump(mode="json", exclude_none=True)
    payload["expected_capability_revision"] = compiled.revision_binding.capability_revision
    payload["expected_compile_hash"] = compiled.revision_binding.compile_hash
    writes = []
    plans = []
    real_build_plan = integration_builder._build_plan

    def build_plan(*a, **k):
        planned = real_build_plan(*a, **k)
        plans.append(planned)
        return planned

    with _account_by_id({}, listing), \
            mock.patch.object(workflow, "preflight_typed_apply_v1", preflight), \
            mock.patch.object(integration_builder, "_build_plan", build_plan), \
            mock.patch.object(integration_builder, "_execute_component",
                              side_effect=lambda *a, **k: writes.append("component") or {"_success": True, "component_id": "x"}), \
            mock.patch.object(integration_builder, "create_component",
                              side_effect=lambda *a, **k: writes.append("create") or {"_success": True, "component_id": "y"}), \
            mock.patch.object(integration_builder, "update_component",
                              side_effect=lambda *a, **k: writes.append("update") or {"_success": True}):
        envelope = integration_builder.build_integration_action(
            client, "p", "apply", {"authoring_request": payload, "dry_run": False}
        )
    served = json.dumps(envelope)
    assert state["after_preflight"], "the preflight was not reached: " + served[:600]
    (root_step,) = [step for step in plans[-1]["steps"] if step["key"] == "main_process"]
    assert root_step["planned_action"] == "reuse", root_step  # non-vacuity: the root is reused
    assert writes == [], "apply wrote siblings of a root the compile never bound"
    assert envelope["_success"] is False
    assert "AUTHORING_PLAN_STALE" in served, served[:800]
    assert other_root not in served and _ROOT_ID not in served


@pytest.mark.parametrize("part", ["target", "source"])
def test_b20_03_a_reused_connection_of_an_unrecognized_family_is_an_identity_mismatch(part):
    """B20-3: the binding resolves to an account connection whose family the
    connector resolver does not recognize (no HTTP Client connection exists in
    the demo account — read-only search returned none — so the pinned,
    proven-operable capture's subtype is set IN-TEST to the legacy HTTP Client
    subtype ``http``). A declaration is an assertion: the unknown family is a
    CONNECTOR_REPLAY_IDENTITY_MISMATCH, for a REST target and for the DB source."""
    unknown = (_CAP159 / ("rest-conn-aa5c5042.xml" if part == "target" else "db-conn-7773fe1e.xml")).read_text()
    original = 'subType="officialboomi-X3979C-rest-prod"' if part == "target" else 'subType="database"'
    assert original in unknown
    unknown = unknown.replace(original, 'subType="http"')
    if part == "target":
        request, answers = _reuse_bound_request("/orders"), {_REUSED_REST_ID: unknown}
    else:
        raw, material = flagship_v2()
        raw["source"]["binding_mode"] = "reuse"
        raw["source"].pop("auth_mode", None)
        material["source"]["connection"] = {"mode": "reuse", "component_id": _DB_CONN_ID}
        request, answers = recipe_request(raw, material), {_DB_CONN_ID: unknown}
    envelope = _public(request, "compile", _account_by_id(answers, []))
    served = json.dumps(envelope)
    assert envelope["_success"] is False, served[:600]
    assert "CONNECTOR_REPLAY_IDENTITY_MISMATCH" in served, served[:800]


def test_b20_04_profile_reads_of_component_planning_and_validation_share_one_budget(monkeypatch):
    """B20-4: a repeated fan-out reuses BOTH generated profiles by name. The
    component plan and typed validation both read them; under one phase budget
    each read is handed the remainder, no profile is read twice, and the
    compile answers within the budget."""
    import time
    from unittest import mock

    from boomi_mcp.categories import integration_builder
    from boomi_mcp.categories.components import _shared

    monkeypatch.setenv(_shared._DEADLINE_ENV, "2")
    record = {"calls": [], "held": 0.0}

    def get_xml(_client, component_id, deadline_seconds=None, *a, **k):
        record["calls"].append((component_id, deadline_seconds))
        started = time.monotonic()
        time.sleep(min(1.2, deadline_seconds) if deadline_seconds is not None else 1.2)
        record["held"] += time.monotonic() - started
        raise _shared.ComponentGetDeadlineExceeded(component_id, deadline_seconds or 0, 1.2)

    listing = [{"component_id": _DB_PROFILE_ID, "name": "M8DEMO DB Read Profile", "type": "profile.db",
                "folder_name": "X"},
               {"component_id": _JSON_PROFILE_ID, "name": "M8DEMO Target Profile", "type": "profile.json",
                "folder_name": "X"}]
    raw, material = flagship_v2()
    with mock.patch.object(integration_builder, "paginate_metadata", lambda *a, **k: [dict(x) for x in listing]), \
            mock.patch.object(_shared, "component_get_xml", get_xml), \
            mock.patch.object(integration_builder, "component_get_xml", get_xml):
        envelope = integration_builder.build_integration_action(
            mock.MagicMock(), "p", "compile",
            {"authoring_request": recipe_request(raw, material).model_dump(mode="json", exclude_none=True)},
        )
    assert isinstance(envelope, dict) and "_success" in envelope
    profile_calls = [c for c in record["calls"] if c[0] in (_DB_PROFILE_ID, _JSON_PROFILE_ID)]
    assert profile_calls, "non-vacuity: the reused profiles were never read"
    assert all(deadline is not None for _cid, deadline in profile_calls), profile_calls
    assert record["held"] <= 2.5, (record["held"], record["calls"])


def test_b20_05_the_served_grammar_states_the_reuse_rules_it_enforces():
    """B20-5: the published grammar states what the reuse judge enforces — a
    stored base carrying '?' or '#' is refused; with an unknown base the path
    must start with '/' and carry no '=' before any '?' — rendered from the
    judge's own constants."""
    from boomi_mcp.authoring.contract import authoring_workflow_contract
    from boomi_mcp.recipes.builtins.fanout import (
        REUSED_BASE_SENTENCE_V2,
        _STORED_BASE_FORBIDDEN,
        _UNKNOWN_BASE_FORBIDDEN_BEFORE_QUERY,
        _UNKNOWN_BASE_PATH_PREFIX,
        judge_reused_request_target,
    )

    served = json.dumps(authoring_workflow_contract())
    assert REUSED_BASE_SENTENCE_V2 in served
    for char in _STORED_BASE_FORBIDDEN:
        assert "'{0}'".format(char) in REUSED_BASE_SENTENCE_V2
        assert judge_reused_request_target("http://h.example.invalid/v1" + char + "x", "/orders") is not None
    assert "'{0}'".format(_UNKNOWN_BASE_PATH_PREFIX) in REUSED_BASE_SENTENCE_V2
    assert "'{0}'".format(_UNKNOWN_BASE_FORBIDDEN_BEFORE_QUERY) in REUSED_BASE_SENTENCE_V2
    assert judge_reused_request_target(None, "orders") is not None
    assert judge_reused_request_target(None, "/a" + _UNKNOWN_BASE_FORBIDDEN_BEFORE_QUERY + "b") is not None
    assert judge_reused_request_target(None, "/orders?filter=x") is None
    # the stored-'?' case the joined grammar alone would admit
    assert judge_reused_request_target("http://h.example.invalid/v1?mode=full", "/orders") is not None


def test_b20_06_malformed_stored_xml_through_the_getter_is_unparseable_not_unanswered():
    """B20-6: the account ANSWERS a reused connection's read with a document
    that does not parse. Through the real shared getter (only the network call
    beneath it is faked), that is UNPARSEABLE — the stored component's fault —
    never "the account did not answer"."""
    from unittest import mock

    from boomi_mcp.authoring.connector_resolution_snapshot import (
        UNPARSEABLE_STORED_COMPONENT_REMEDIATION,
        UNREADABLE_REUSE_REMEDIATION,
    )
    from boomi_mcp.categories import integration_builder

    client = mock.MagicMock()
    client.component.get_component.return_value = b"<not-a-component"
    with mock.patch.object(integration_builder, "paginate_metadata", lambda *a, **k: []):
        envelope = integration_builder.build_integration_action(
            client, "p", "compile",
            {"authoring_request": _reuse_bound_request("/v1/billing").model_dump(mode="json", exclude_none=True)},
        )
    assert client.component.get_component.called, "non-vacuity: the getter's network call ran"
    (diag,) = [d for d in envelope["authoring_diagnostics"] if d["code"] == "CONNECTOR_REPLAY_IDENTITY_UNAVAILABLE"]
    assert diag["remediation"] == UNPARSEABLE_STORED_COMPONENT_REMEDIATION
    assert diag["remediation"] != UNREADABLE_REUSE_REMEDIATION


# ---------------------------------------------------------------------------
# B21 (Codex review of B20): witnesses, each failing at 91c196c.
# ---------------------------------------------------------------------------


def _base_bound_by(form):
    """A reuse-mode selected base bound by each of the four binding forms."""
    raw, material = flagship_v2()
    _bind_target_1(raw, material, "reuse")
    material["targets"][1]["operation"]["path"] = "/v1/billing"
    config = {"reference_only": True, "connector_type": "rest"}
    name = None
    component_id = None
    if form == "config_name":
        config["component_name"] = _REUSED_NAME
    elif form == "name":
        name = _REUSED_NAME
    elif form == "config_id":
        config["component_id"] = _REUSED_REST_ID
    else:
        component_id = _REUSED_REST_ID
    base = IntegrationComponentSpec(
        key="shared_rest", type="connector-settings", name=name, action="create",
        component_id=component_id, config=config,
    )
    return recipe_request(raw, material, base_components=[base.model_dump(mode="json", exclude_none=True)])


@pytest.mark.parametrize("form", ["name", "config_name", "id", "config_id"])
def test_b21_01_the_hashed_reference_evidence_carries_the_planner_binding(form):
    """B21-1 sweep: for EVERY binding form, the served (and plan-hashed)
    reference evidence names the component the PLANNER bound."""
    clean = _reused_rest_xml("http://h.example.invalid/api/")
    envelope = _public(
        _base_bound_by(form), "compile",
        _account_by_id({_REUSED_REST_ID: clean}, [(_REUSED_NAME, _REUSED_REST_ID)]),
    )
    assert envelope["_success"] is True, json.dumps(envelope)[:600]
    (reference,) = [r for r in envelope["authoring_result"]["resolved_references"]
                    if r["ref"] == "$ref:shared_rest"]
    assert reference["component_id"] == _REUSED_REST_ID, reference


@pytest.mark.parametrize("form", ["name", "config_name"])
def test_b21_01_a_name_that_rebinds_before_apply_moves_the_compile_hash(form):
    """B21-1: the caller compiles while the name binds A; before apply the SAME
    name binds B. The compile replay and the execution plan both see B, so only
    the compile hash can tell — it covers the planner's binding, so apply
    refuses as stale before any write."""
    from unittest import mock

    from boomi_mcp.categories import integration_builder

    clean = _reused_rest_xml("http://h.example.invalid/api/")
    answers = {_REUSED_REST_ID: clean, _B_REST_ID: clean.replace(_REUSED_REST_ID, _B_REST_ID)}
    listing = [(_REUSED_NAME, _REUSED_REST_ID)]
    request = _base_bound_by(form)
    client = mock.MagicMock()
    with _account_by_id(answers, listing):
        compiled, _ = compile_authoring_request_v1(request, boomi_client=client, profile="p")
    listing[0] = (_REUSED_NAME, _B_REST_ID)
    payload = request.model_dump(mode="json", exclude_none=True)
    payload["expected_capability_revision"] = compiled.revision_binding.capability_revision
    payload["expected_compile_hash"] = compiled.revision_binding.compile_hash
    writes = []
    with _account_by_id(answers, listing), \
            mock.patch.object(integration_builder, "_execute_component",
                              side_effect=lambda *a, **k: writes.append("component") or {"_success": True, "component_id": "x"}), \
            mock.patch.object(integration_builder, "create_component",
                              side_effect=lambda *a, **k: writes.append("create") or {"_success": True, "component_id": "y"}), \
            mock.patch.object(integration_builder, "update_component",
                              side_effect=lambda *a, **k: writes.append("update") or {"_success": True}):
        envelope = integration_builder.build_integration_action(
            client, "p", "apply", {"authoring_request": payload, "dry_run": False}
        )
    served = json.dumps(envelope)
    assert writes == [], "apply wrote under a compile hash that covered another binding"
    assert envelope["_success"] is False
    assert "AUTHORING_PLAN_STALE" in served, served[:800]
    assert _B_REST_ID not in served and _REUSED_REST_ID not in served


def test_b21_02_the_pre_write_profile_refresh_spends_the_phase_budget(monkeypatch):
    """B21-2: apply's pre-write refresh of reused/literal profile indexes is
    charged to the SAME phase budget as the execution plan's discovery: every
    profile read is handed a deadline, and the refresh's re-read (B21 review:
    a freshness check re-reads) gets only what the plan's read left."""
    import time
    from unittest import mock

    from boomi_mcp.categories import integration_builder
    from boomi_mcp.categories.components import _shared

    monkeypatch.setenv(_shared._DEADLINE_ENV, "2")
    calls = []

    def get_xml(_client, component_id, deadline_seconds=None, *a, **k):
        calls.append((component_id, deadline_seconds))
        time.sleep(min(1.2, deadline_seconds) if deadline_seconds is not None else 1.2)
        raise _shared.ComponentGetDeadlineExceeded(component_id, deadline_seconds or 0, 1.2)

    spec = {"integration_name": "Raw", "components": [
        {"key": "prof", "type": "profile.json", "name": "Existing Profile", "action": "create",
         "component_id": _JSON_PROFILE_ID, "config": {"reference_only": True}},
    ]}
    with mock.patch.object(integration_builder, "paginate_metadata", lambda *a, **k: []), \
            mock.patch.object(_shared, "component_get_xml", get_xml), \
            mock.patch.object(integration_builder, "component_get_xml", get_xml), \
            mock.patch.object(integration_builder, "_execute_component",
                              side_effect=lambda *a, **k: {"_success": True, "component_id": "x"}):
        integration_builder.build_integration_action(
            mock.MagicMock(), "p", "apply", {"integration_spec": spec, "dry_run": False}
        )
    profile_reads = [c for c in calls if c[0] == _JSON_PROFILE_ID]
    assert profile_reads, "non-vacuity: the reused profile was read"
    assert all(deadline is not None for _cid, deadline in profile_reads), profile_reads
    # One budget: the plan's read spent ~1.2 s of 2 s, so the refresh's re-read
    # (if the budget let it start) was handed only the remainder.
    assert len(profile_reads) <= 2, profile_reads
    if len(profile_reads) == 2:
        assert profile_reads[1][1] < 1.0, profile_reads


def test_b21r_the_pre_write_refresh_re_reads_instead_of_answering_from_the_cache():
    """B21 review: the pre-write refresh is a FRESHNESS check — a profile the
    execution plan read moments ago is read AGAIN (not answered from the phase
    cache), so an edit made after `_build_plan` is seen before any map is written."""
    from unittest import mock

    from boomi_mcp.categories import integration_builder
    from boomi_mcp.categories.components import _shared

    calls = []

    def get_xml(_client, component_id, deadline_seconds=None, *a, **k):
        calls.append(component_id)
        raise RuntimeError("unreadable")  # fast; the reading is None either way

    spec = {"integration_name": "Raw", "components": [
        {"key": "prof", "type": "profile.json", "name": "Existing Profile", "action": "create",
         "component_id": _JSON_PROFILE_ID, "config": {"reference_only": True}},
    ]}
    with mock.patch.object(integration_builder, "paginate_metadata", lambda *a, **k: []), \
            mock.patch.object(_shared, "component_get_xml", get_xml), \
            mock.patch.object(integration_builder, "component_get_xml", get_xml), \
            mock.patch.object(integration_builder, "_execute_component",
                              side_effect=lambda *a, **k: {"_success": True, "component_id": "x"}):
        integration_builder.build_integration_action(
            mock.MagicMock(), "p", "apply", {"integration_spec": spec, "dry_run": False}
        )
    assert calls.count(_JSON_PROFILE_ID) >= 2, calls


@pytest.mark.parametrize("delimiter", ["?mode=full", "#frag"])
def test_b21_03_a_malformed_stored_base_carrying_a_delimiter_is_refused(delimiter):
    """B21-3: a READABLE stored url carrying '?' or '#' is refused for reuse
    even when it is not a well-formed absolute URL (no host): the delimiter rule
    is judged on the raw stored value BEFORE the unknown-base fallback."""
    envelope = _public(
        _reuse_bound_request("/orders"), "compile",
        _account_by_id({_REUSED_REST_ID: _reused_rest_xml("https:///base" + delimiter)}, []),
    )
    served = json.dumps(envelope)
    assert envelope["_success"] is False, served[:600]
    assert "FANOUT_LITERAL_QUERY_NAME_UNSUPPORTED" in served
    assert _RAW_BINDING_POINTER in served
    assert "https:///base" not in served and delimiter not in served  # value-free


@pytest.mark.parametrize("body", [b"", b"\xff\xfe<bns:Component/>"])
def test_b21_04_an_empty_or_undecodable_body_is_unparseable_not_unanswered(body):
    """B21-4: a successful GET whose body is empty, or does not decode as
    UTF-8, is a malformed RESPONSE — through the real getter it is classified
    UNPARSEABLE, never "the account did not answer"."""
    from unittest import mock

    from boomi_mcp.authoring.connector_resolution_snapshot import UNPARSEABLE_STORED_COMPONENT_REMEDIATION
    from boomi_mcp.categories import integration_builder

    client = mock.MagicMock()
    client.component.get_component.return_value = body
    with mock.patch.object(integration_builder, "paginate_metadata", lambda *a, **k: []):
        envelope = integration_builder.build_integration_action(
            client, "p", "compile",
            {"authoring_request": _reuse_bound_request("/v1/billing").model_dump(mode="json", exclude_none=True)},
        )
    assert client.component.get_component.called
    (diag,) = [d for d in envelope["authoring_diagnostics"] if d["code"] == "CONNECTOR_REPLAY_IDENTITY_UNAVAILABLE"]
    assert diag["remediation"] == UNPARSEABLE_STORED_COMPONENT_REMEDIATION

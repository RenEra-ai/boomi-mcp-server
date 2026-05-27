"""Issue #41 r3: tests for _synthesize_script_function_wrappers.

Verifies the plan-time pass that injects transform.function wrappers
between any transform.map (map_type='script') and the script.mapping it
references. The synthesis path is what makes Boomi-runtime-valid XML
emit from the user-friendly $ref:<script_key> contract.
"""

from __future__ import annotations

from unittest.mock import patch, MagicMock

from boomi_mcp.categories.integration_builder import (
    _AUTO_WRAPPER_KEY_PREFIX,
    _build_plan,
    _synthesize_script_function_wrappers,
)
from boomi_mcp.models.integration_models import (
    IntegrationComponentSpec,
    IntegrationSpecV1,
)


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------


def _profile_xml_comp(key="src_profile", name="Src XML"):
    return IntegrationComponentSpec(
        key=key, type="profile.xml", action="create", name=name,
        config={
            "component_type": "profile.xml",
            "profile_type": "xml.generated",
            "component_name": name,
            "root": {
                "name": "rows", "kind": "element", "max_occurs": 1,
                "children": [{
                    "name": "row", "kind": "element", "max_occurs": -1,
                    "children": [
                        {"name": "key", "kind": "element", "data_type": "character"},
                    ],
                }],
            },
        },
    )


def _profile_json_comp(key="tgt_profile", name="Tgt JSON"):
    return IntegrationComponentSpec(
        key=key, type="profile.json", action="create", name=name,
        config={
            "component_type": "profile.json",
            "profile_type": "json.generated",
            "component_name": name,
            "root": {
                "name": "Root", "kind": "object",
                "children": [{
                    "name": "list", "kind": "array",
                    "children": [
                        {"name": "key", "kind": "simple", "data_type": "character"},
                    ],
                }],
            },
        },
    )


def _script_comp(key="my_script", name="My Script"):
    return IntegrationComponentSpec(
        key=key, type="script.mapping", action="create", name=name,
        config={
            "component_type": "script.mapping",
            "component_name": name,
            "language": "groovy2",
            "preserve_order": True,
            "use_cache": True,
            "script_body": "outputValue = inputValue.toUpperCase()",
            "inputs": [{"name": "inputValue", "data_type": "character"}],
            "outputs": [{"name": "outputValue"}],
        },
    )


def _script_map_comp(
    key="the_map", name="Test Map", script_ref_key="my_script",
    depends_on=None,
):
    return IntegrationComponentSpec(
        key=key, type="transform.map", action="create", name=name,
        depends_on=depends_on if depends_on is not None
            else ["src_profile", "tgt_profile", script_ref_key],
        config={
            "component_type": "transform.map",
            "map_type": "script",
            "component_name": name,
            "source_profile_id": "$ref:src_profile",
            "source_profile_type": "profile.xml",
            "target_profile_id": "$ref:tgt_profile",
            "target_profile_type": "profile.json",
            "script_mappings": [{
                "script_component_id": f"$ref:{script_ref_key}",
                "script_slot": "enrich",
                "inputs": [{"source_path": "rows/row[]/key", "input_name": "inputValue"}],
                "outputs": [{"output_name": "outputValue", "target_path": "Root/list[]/key"}],
            }],
        },
    )


def _make_spec(*components):
    return IntegrationSpecV1(name="Test Spec", components=list(components))


# ---------------------------------------------------------------------------
# Synthesis behavior
# ---------------------------------------------------------------------------


def test_synthesis_creates_wrapper_for_script_mapping_ref():
    spec = _make_spec(
        _profile_xml_comp(),
        _profile_json_comp(),
        _script_comp(),
        _script_map_comp(),
    )
    _synthesize_script_function_wrappers(spec)
    wrapper_keys = [
        c.key for c in spec.components if c.key.startswith(_AUTO_WRAPPER_KEY_PREFIX)
    ]
    assert wrapper_keys == ["__auto_wrapper_my_script__"]
    wrapper = next(c for c in spec.components if c.key == wrapper_keys[0])
    assert wrapper.type == "transform.function"
    assert wrapper.config["script_component_id"] == "$ref:my_script"
    assert wrapper.config["language"] == "groovy2"
    assert wrapper.config["script_body"] == "outputValue = inputValue.toUpperCase()"
    assert wrapper.config["inputs"] == [{"name": "inputValue", "data_type": "character"}]
    assert wrapper.config["outputs"] == [{"name": "outputValue"}]
    assert wrapper.depends_on == ["my_script"]


def test_synthesis_rewrites_map_script_component_id_to_wrapper_key():
    spec = _make_spec(
        _profile_xml_comp(),
        _profile_json_comp(),
        _script_comp(),
        _script_map_comp(),
    )
    _synthesize_script_function_wrappers(spec)
    the_map = next(c for c in spec.components if c.key == "the_map")
    sm = the_map.config["script_mappings"][0]
    assert sm["script_component_id"] == "$ref:__auto_wrapper_my_script__"


def test_synthesis_adds_wrapper_to_map_depends_on():
    spec = _make_spec(
        _profile_xml_comp(),
        _profile_json_comp(),
        _script_comp(),
        _script_map_comp(),
    )
    _synthesize_script_function_wrappers(spec)
    the_map = next(c for c in spec.components if c.key == "the_map")
    assert "__auto_wrapper_my_script__" in the_map.depends_on


def test_two_maps_referencing_same_script_share_one_wrapper():
    """Shared-wrapper policy: each script.mapping gets exactly one
    synthesized wrapper, reused across all calling maps."""
    spec = _make_spec(
        _profile_xml_comp(),
        _profile_json_comp(),
        _script_comp(),
        _script_map_comp(key="map_a", name="Map A"),
        _script_map_comp(key="map_b", name="Map B"),
    )
    _synthesize_script_function_wrappers(spec)
    wrapper_keys = [
        c.key for c in spec.components if c.key.startswith(_AUTO_WRAPPER_KEY_PREFIX)
    ]
    assert wrapper_keys == ["__auto_wrapper_my_script__"], (
        "Two maps referencing the same script.mapping should produce a "
        "SINGLE shared wrapper component"
    )
    map_a = next(c for c in spec.components if c.key == "map_a")
    map_b = next(c for c in spec.components if c.key == "map_b")
    assert (
        map_a.config["script_mappings"][0]["script_component_id"]
        == "$ref:__auto_wrapper_my_script__"
    )
    assert (
        map_b.config["script_mappings"][0]["script_component_id"]
        == "$ref:__auto_wrapper_my_script__"
    )


def test_synthesis_does_not_duplicate_when_wrapper_already_declared():
    """If the caller declared a transform.function with the exact
    auto-wrapper key, synthesis trusts it instead of clobbering."""
    spec = _make_spec(
        _profile_xml_comp(),
        _profile_json_comp(),
        _script_comp(),
        IntegrationComponentSpec(
            key="__auto_wrapper_my_script__",
            type="transform.function",
            action="create",
            name="Caller-declared wrapper",
            config={
                "component_type": "transform.function",
                "component_name": "Caller-declared wrapper",
                "script_component_id": "$ref:my_script",
                "language": "groovy2",
                "script_body": "// custom wrapper body",
                "inputs": [{"name": "inputValue", "data_type": "character"}],
                "outputs": [{"name": "outputValue"}],
            },
            depends_on=["my_script"],
        ),
        _script_map_comp(),
    )
    _synthesize_script_function_wrappers(spec)
    # Caller's wrapper config preserved (not overwritten).
    caller_wrapper = next(
        c for c in spec.components if c.key == "__auto_wrapper_my_script__"
    )
    assert caller_wrapper.config["component_name"] == "Caller-declared wrapper"
    assert caller_wrapper.config["script_body"] == "// custom wrapper body"


def test_synthesis_skips_literal_uuid_script_component_id():
    """Literal UUIDs (not $ref:KEY) bypass synthesis entirely — the
    caller is responsible for having pre-created the wrapper themselves."""
    the_map = _script_map_comp()
    the_map.config["script_mappings"][0]["script_component_id"] = (
        "00000000-0000-0000-0000-aaaaaaaaaaaa"
    )
    spec = _make_spec(
        _profile_xml_comp(),
        _profile_json_comp(),
        _script_comp(),
        the_map,
    )
    _synthesize_script_function_wrappers(spec)
    wrapper_keys = [
        c.key for c in spec.components if c.key.startswith(_AUTO_WRAPPER_KEY_PREFIX)
    ]
    assert wrapper_keys == [], (
        "Literal UUID refs must not trigger wrapper synthesis"
    )


def test_synthesis_skips_ref_to_non_script_component():
    """A $ref pointing at a profile (not script.mapping) is not a script
    reference — leave it alone for the validator to surface the error."""
    the_map = _script_map_comp()
    the_map.config["script_mappings"][0]["script_component_id"] = "$ref:src_profile"
    spec = _make_spec(
        _profile_xml_comp(),
        _profile_json_comp(),
        _script_comp(),
        the_map,
    )
    _synthesize_script_function_wrappers(spec)
    wrapper_keys = [
        c.key for c in spec.components if c.key.startswith(_AUTO_WRAPPER_KEY_PREFIX)
    ]
    assert wrapper_keys == []


def test_synthesis_skips_non_script_map_type():
    """Only map_type='script'/'map_script' triggers synthesis. Direct
    and function maps stay untouched."""
    direct_map = IntegrationComponentSpec(
        key="direct_map", type="transform.map", action="create",
        name="Direct Map",
        depends_on=["src_profile", "tgt_profile"],
        config={
            "component_type": "transform.map",
            "map_type": "direct",
            "component_name": "Direct Map",
            "source_profile_id": "$ref:src_profile",
            "source_profile_type": "profile.xml",
            "target_profile_id": "$ref:tgt_profile",
            "target_profile_type": "profile.json",
            "field_mappings": [
                {"source_path": "rows/row[]/key", "target_path": "Root/list[]/key"},
            ],
        },
    )
    spec = _make_spec(
        _profile_xml_comp(),
        _profile_json_comp(),
        _script_comp(),  # In-spec but unreferenced
        direct_map,
    )
    _synthesize_script_function_wrappers(spec)
    wrapper_keys = [
        c.key for c in spec.components if c.key.startswith(_AUTO_WRAPPER_KEY_PREFIX)
    ]
    assert wrapper_keys == []


# ---------------------------------------------------------------------------
# End-to-end plan integration
# ---------------------------------------------------------------------------


def test_plan_output_surfaces_synthesized_wrapper_step():
    """Caller sees the synthesized wrapper as a first-class step in the
    plan output. Synthesis is not hidden behind the scenes."""
    config = {
        "integration_spec": {
            "name": "Wrapper Synth E2E",
            "components": [
                _profile_xml_comp().model_dump(),
                _profile_json_comp().model_dump(),
                _script_comp().model_dump(),
                _script_map_comp().model_dump(),
            ],
        },
    }
    with patch(
        "boomi_mcp.categories.integration_builder.paginate_metadata",
        return_value=[],
    ):
        plan = _build_plan(MagicMock(), config)
    assert plan["_success"] is True
    step_keys = [s["key"] for s in plan["steps"]]
    assert "__auto_wrapper_my_script__" in step_keys
    wrapper_step = next(
        s for s in plan["steps"] if s["key"] == "__auto_wrapper_my_script__"
    )
    assert wrapper_step["type"] == "transform.function"
    assert wrapper_step["planned_action"] == "create"


def test_plan_execution_order_runs_script_then_wrapper_then_map():
    config = {
        "integration_spec": {
            "name": "Wrapper Synth E2E",
            "components": [
                _profile_xml_comp().model_dump(),
                _profile_json_comp().model_dump(),
                _script_comp().model_dump(),
                _script_map_comp().model_dump(),
            ],
        },
    }
    with patch(
        "boomi_mcp.categories.integration_builder.paginate_metadata",
        return_value=[],
    ):
        plan = _build_plan(MagicMock(), config)
    order = plan["execution_order"]
    assert order.index("my_script") < order.index("__auto_wrapper_my_script__")
    assert order.index("__auto_wrapper_my_script__") < order.index("the_map")

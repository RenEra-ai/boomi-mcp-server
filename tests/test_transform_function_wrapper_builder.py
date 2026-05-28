"""Issue #41 r3: TransformFunctionWrapperBuilder unit tests.

Wrapper components bridge a transform.map (map_type='script') to a
script.mapping. The transform.map's userdefined FunctionStep ``id``
attribute references the wrapper; the wrapper internally references the
script.mapping via Configuration/Scripting componentId.
"""

from __future__ import annotations

import pytest

from boomi_mcp.categories.components.builders.transform_function_wrapper_builder import (
    TransformFunctionWrapperBuilder,
    get_transform_function_wrapper_builder,
)


def _minimal_config(**overrides):
    base = {
        "component_type": "transform.function",
        "component_name": "Example Wrapper",
        "script_component_id": "00000000-0000-0000-0000-aaaaaaaaaaaa",
        "language": "groovy2",
        "preserve_order": True,
        "use_cache": True,
        "script_body": "outputValue = inputValue.toUpperCase()",
        "inputs": [{"name": "inputValue", "data_type": "character"}],
        "outputs": [{"name": "outputValue"}],
    }
    base.update(overrides)
    return base


# ---------------------------------------------------------------------------
# Registry
# ---------------------------------------------------------------------------


def test_registry_resolves_transform_function_type():
    assert get_transform_function_wrapper_builder("transform.function") is TransformFunctionWrapperBuilder


def test_registry_returns_none_for_unknown_type():
    assert get_transform_function_wrapper_builder("script.mapping") is None
    assert get_transform_function_wrapper_builder("transform.map") is None


# ---------------------------------------------------------------------------
# XML envelope shape — matches live work component b8eaeeba-...
# ---------------------------------------------------------------------------


def test_minimal_build_emits_wrapper_envelope_shape():
    xml = TransformFunctionWrapperBuilder().build(**_minimal_config())
    # Component envelope
    assert 'type="transform.function"' in xml
    assert 'name="Example Wrapper"' in xml
    # Outer Function with xmlns="" reset
    assert '<Function xmlns="">' in xml
    # External port surface
    assert '<Inputs><Input key="1" name="inputValue"/></Inputs>' in xml
    assert '<Outputs><Output key="1" name="outputValue"/></Outputs>' in xml
    # Inner Scripting FunctionStep (cacheEnabled="true", cacheOption="map")
    assert 'category="Scripting"' in xml
    assert 'type="Scripting"' in xml
    assert 'cacheEnabled="true"' in xml
    assert 'cacheOption="map"' in xml
    # Inner step Input/Output keys: 1-based per port list
    assert '<Inputs><Input key="1" name="inputValue"/></Inputs>' in xml
    assert '<Outputs><Output key="2" name="outputValue"/></Outputs>' in xml
    # Scripting Configuration with useComponent="true" + componentId
    assert (
        '<Scripting componentId="00000000-0000-0000-0000-aaaaaaaaaaaa" '
        'language="groovy2" preserveOrder="true" useCache="true" '
        'useComponent="true">'
    ) in xml
    # Inline ScriptToExecute snapshot + Input/Output declarations
    assert (
        "<ScriptToExecute>outputValue = inputValue.toUpperCase()</ScriptToExecute>"
    ) in xml
    assert '<Input dataType="character" index="1" name="inputValue"/>' in xml
    assert '<Output index="2" name="outputValue"/>' in xml


def test_editor_to_scripting_mapping_rows_emit_in_pair_order():
    xml = TransformFunctionWrapperBuilder().build(**_minimal_config())
    # Editor input (key=1) → Scripting input (key=1)
    assert (
        '<Mapping fromFunction="0" fromKey="1" fromNamePath="Editor/inputValue" '
        'fromType="function" toFunction="1" toKey="1" '
        'toNamePath="Scripting/inputValue" toType="function"/>'
    ) in xml
    # Scripting output (key=2) → Editor output (key=1)
    assert (
        '<Mapping fromFunction="1" fromKey="2" fromNamePath="Scripting/outputValue" '
        'fromType="function" toFunction="0" toKey="1" '
        'toNamePath="Editor/outputValue" toType="function"/>'
    ) in xml


def test_multi_input_multi_output_indexing():
    cfg = _minimal_config(
        inputs=[
            {"name": "a", "data_type": "character"},
            {"name": "b", "data_type": "integer"},
        ],
        outputs=[
            {"name": "x"},
            {"name": "y"},
        ],
    )
    xml = TransformFunctionWrapperBuilder().build(**cfg)
    # External: inputs 1..2, outputs 1..2 (independent per-port-list keys)
    assert '<Inputs><Input key="1" name="a"/><Input key="2" name="b"/></Inputs>' in xml
    assert '<Outputs><Output key="1" name="x"/><Output key="2" name="y"/></Outputs>' in xml
    # Inner Scripting input/output indexes continue monotonically (1,2 then 3,4)
    assert '<Input dataType="character" index="1" name="a"/>' in xml
    assert '<Input dataType="integer" index="2" name="b"/>' in xml
    assert '<Output index="3" name="x"/>' in xml
    assert '<Output index="4" name="y"/>' in xml


def test_unresolved_dollar_ref_script_component_id_rejected_at_build():
    # Plan-time synthesis must resolve '$ref:KEY' before invoking build().
    # Leaked $ref strings reach build() only via bugs; surface a
    # structured error rather than emitting nonsense XML.
    cfg = _minimal_config(script_component_id="$ref:my_script")
    with pytest.raises(Exception) as exc_info:
        TransformFunctionWrapperBuilder().build(**cfg)
    assert exc_info.value.error_code == "MAP_PROFILE_INDEX_UNAVAILABLE"


def test_validate_config_accepts_dollar_ref_at_plan_time():
    # Plan-time path runs validate_config BEFORE _resolve_dependency_tokens.
    # The validator must accept '$ref:KEY' strings; only build() rejects.
    err = TransformFunctionWrapperBuilder.validate_config(
        _minimal_config(script_component_id="$ref:some_key")
    )
    assert err is None


# ---------------------------------------------------------------------------
# Validation — same family as ScriptMappingBuilder
# ---------------------------------------------------------------------------


def test_unsupported_language_rejected():
    with pytest.raises(Exception) as exc_info:
        TransformFunctionWrapperBuilder().build(**_minimal_config(language="python"))
    assert exc_info.value.error_code == "SCRIPT_MAPPING_LANGUAGE_UNSUPPORTED"


def test_missing_script_body_rejected():
    cfg = _minimal_config()
    del cfg["script_body"]
    with pytest.raises(Exception) as exc_info:
        TransformFunctionWrapperBuilder().build(**cfg)
    assert exc_info.value.error_code == "SCRIPT_MAPPING_BODY_REQUIRED"


def test_output_data_type_attribute_rejected():
    with pytest.raises(Exception) as exc_info:
        TransformFunctionWrapperBuilder().build(
            **_minimal_config(outputs=[{"name": "x", "data_type": "character"}])
        )
    assert exc_info.value.error_code == "SCRIPT_MAPPING_VARIABLE_INVALID"


def test_unknown_top_level_key_rejected():
    cfg = _minimal_config()
    cfg["mystery"] = "value"
    with pytest.raises(Exception) as exc_info:
        TransformFunctionWrapperBuilder().build(**cfg)
    assert exc_info.value.error_code == "SCRIPT_MAPPING_VALIDATION_FAILED"
    assert exc_info.value.field == "mystery"


def test_secret_shaped_top_level_key_rejected():
    cfg = _minimal_config()
    cfg["password"] = "leaked"
    err = TransformFunctionWrapperBuilder.scan_forbidden_secret_fields(cfg)
    assert err is not None
    assert err.error_code == "PLAINTEXT_SECRET_REJECTED"


def test_repeat_build_byte_identical():
    cfg = _minimal_config(
        inputs=[
            {"name": "a", "data_type": "character"},
            {"name": "b", "data_type": "integer"},
        ],
        outputs=[{"name": "x"}, {"name": "y"}],
    )
    one = TransformFunctionWrapperBuilder().build(**cfg)
    two = TransformFunctionWrapperBuilder().build(**cfg)
    assert one == two


def test_script_body_xml_escaped_not_cdata():
    cfg = _minimal_config(script_body='output = "<foo & bar>"')
    xml = TransformFunctionWrapperBuilder().build(**cfg)
    assert "<![CDATA[" not in xml
    # Body is XML-escaped inside <ScriptToExecute>.
    assert "&quot;&lt;foo &amp; bar&gt;&quot;" in xml


# ============================================================================
# Issue #45 — Component XML update preservation
# ============================================================================


def test_wrapper_preservation_policy_attached():
    policy = TransformFunctionWrapperBuilder.PRESERVATION_POLICY
    assert policy.component_type == "transform.function"
    paths = {op.path for op in policy.owned_paths}
    assert paths == {"bns:object/Function"}


def test_wrapper_update_preserves_encrypted_values_and_root_attrs():
    import xml.etree.ElementTree as ET
    from boomi_mcp.categories.components.component_update_preservation import (
        merge_for_update,
    )

    NS = {"bns": "http://api.platform.boomi.com/"}
    desired = TransformFunctionWrapperBuilder().build(
        **_minimal_config(component_name="renamed")
    )
    current = TransformFunctionWrapperBuilder().build(**_minimal_config())
    current = current.replace(
        "<bns:encryptedValues/>",
        '<bns:encryptedValues>'
        '<bns:encryptedValue path="//x" isSet="true"/>'
        '</bns:encryptedValues>',
    )

    merged = merge_for_update(
        current, desired, TransformFunctionWrapperBuilder.PRESERVATION_POLICY
    )
    root = ET.fromstring(merged)
    assert root.attrib["name"] == "renamed"
    ev = root.find("bns:encryptedValues/bns:encryptedValue", NS)
    assert ev is not None
    assert ev.attrib.get("isSet") == "true"

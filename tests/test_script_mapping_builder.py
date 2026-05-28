"""Issue #41: ScriptMappingBuilder unit tests."""

from __future__ import annotations

import re

import pytest

from boomi_mcp.categories.components.builders.script_mapping_builder import (
    ScriptMappingBuilder,
    get_script_mapping_builder,
)


def _minimal_config(**overrides):
    base = {
        "component_type": "script.mapping",
        "component_name": "Example Script",
        "language": "groovy2",
        "script_body": "outputValue = inputValue.toUpperCase()",
        "inputs": [{"name": "inputValue", "data_type": "character"}],
        "outputs": [{"name": "outputValue"}],
    }
    base.update(overrides)
    return base


# ---------------------------------------------------------------------------
# Registry
# ---------------------------------------------------------------------------


def test_registry_resolves_script_mapping_component_type():
    assert get_script_mapping_builder("script.mapping") is ScriptMappingBuilder


def test_registry_returns_none_for_unknown_component_type():
    assert get_script_mapping_builder("script.processing") is None
    assert get_script_mapping_builder("transform.map") is None


# ---------------------------------------------------------------------------
# XML envelope shape
# ---------------------------------------------------------------------------


def test_minimal_build_emits_mapping_script_envelope():
    xml = ScriptMappingBuilder().build(**_minimal_config())
    # Component envelope
    assert '<bns:Component xmlns:xsi=' in xml
    assert 'type="script.mapping"' in xml
    assert 'name="Example Script"' in xml
    # MappingScript with three documented attributes
    assert '<MappingScript xmlns="" language="groovy2" preserveOrder="true" useCache="true">' in xml
    # script element with plain (non-CDATA) escaped body
    assert "<script>outputValue = inputValue.toUpperCase()</script>" in xml
    assert "<![CDATA[" not in xml
    # Single Input at index 1, single Output at index 2
    assert '<Input dataType="character" index="1" name="inputValue"/>' in xml
    assert '<Output index="2" name="outputValue"/>' in xml


def test_multi_input_multi_output_indexes_continue_monotonically():
    cfg = _minimal_config(
        inputs=[
            {"name": "a", "data_type": "character"},
            {"name": "b", "data_type": "integer"},
            {"name": "c", "data_type": "date"},
        ],
        outputs=[
            {"name": "x"},
            {"name": "y"},
        ],
    )
    xml = ScriptMappingBuilder().build(**cfg)
    # Inputs occupy indexes 1..3
    assert '<Input dataType="character" index="1" name="a"/>' in xml
    assert '<Input dataType="integer" index="2" name="b"/>' in xml
    assert '<Input dataType="date" index="3" name="c"/>' in xml
    # Outputs continue at 4, 5
    assert '<Output index="4" name="x"/>' in xml
    assert '<Output index="5" name="y"/>' in xml


def test_output_does_not_emit_data_type_attribute():
    xml = ScriptMappingBuilder().build(**_minimal_config())
    # Verify no dataType on any <Output ...>
    output_tags = re.findall(r"<Output[^/]*/>", xml)
    assert output_tags, "expected at least one <Output ...> tag"
    for tag in output_tags:
        assert "dataType" not in tag, f"<Output> must not carry dataType, got {tag!r}"


def test_script_body_is_xml_escaped_not_cdata():
    cfg = _minimal_config(script_body='output = "<foo & bar>"')
    xml = ScriptMappingBuilder().build(**cfg)
    assert "<![CDATA[" not in xml
    assert "<script>output = &quot;&lt;foo &amp; bar&gt;&quot;</script>" in xml


def test_child_order_is_script_then_inputs_then_outputs():
    cfg = _minimal_config(
        inputs=[
            {"name": "a", "data_type": "character"},
            {"name": "b", "data_type": "float"},
        ],
        outputs=[{"name": "z"}],
    )
    xml = ScriptMappingBuilder().build(**cfg)
    script_pos = xml.find("<script>")
    input_a_pos = xml.find('name="a"')
    input_b_pos = xml.find('name="b"')
    output_z_pos = xml.find('name="z"')
    assert script_pos < input_a_pos < input_b_pos < output_z_pos


def test_folder_path_renders_as_folder_full_path_attribute():
    cfg = _minimal_config(folder_path="Top/Sub Folder")
    xml = ScriptMappingBuilder().build(**cfg)
    assert ' folderFullPath="Top/Sub Folder"' in xml


def test_description_is_escaped_and_emitted():
    cfg = _minimal_config(description="reads & writes <user>")
    xml = ScriptMappingBuilder().build(**cfg)
    assert "<bns:description>reads &amp; writes &lt;user&gt;</bns:description>" in xml


def test_preserve_order_false_emits_lowercase_attribute():
    xml = ScriptMappingBuilder().build(**_minimal_config(preserve_order=False))
    assert 'preserveOrder="false"' in xml


def test_use_cache_false_emits_lowercase_attribute():
    xml = ScriptMappingBuilder().build(**_minimal_config(use_cache=False))
    assert 'useCache="false"' in xml


def test_repeat_build_byte_identical():
    cfg = _minimal_config(
        inputs=[
            {"name": "a", "data_type": "character"},
            {"name": "b", "data_type": "integer"},
        ],
        outputs=[{"name": "x"}, {"name": "y"}],
    )
    one = ScriptMappingBuilder().build(**cfg)
    two = ScriptMappingBuilder().build(**cfg)
    assert one == two


def test_all_three_languages_pass_through():
    for lang in ("groovy", "groovy2", "javascript"):
        xml = ScriptMappingBuilder().build(**_minimal_config(language=lang))
        assert f'language="{lang}"' in xml


# ---------------------------------------------------------------------------
# Validation — language / body / variable identifier rules
# ---------------------------------------------------------------------------


def test_unsupported_language_rejected():
    with pytest.raises(Exception) as exc_info:
        ScriptMappingBuilder().build(**_minimal_config(language="python"))
    assert exc_info.value.error_code == "SCRIPT_MAPPING_LANGUAGE_UNSUPPORTED"


def test_missing_script_body_rejected():
    cfg = _minimal_config()
    del cfg["script_body"]
    with pytest.raises(Exception) as exc_info:
        ScriptMappingBuilder().build(**cfg)
    assert exc_info.value.error_code == "SCRIPT_MAPPING_BODY_REQUIRED"


def test_blank_script_body_rejected():
    with pytest.raises(Exception) as exc_info:
        ScriptMappingBuilder().build(**_minimal_config(script_body="   "))
    assert exc_info.value.error_code == "SCRIPT_MAPPING_BODY_REQUIRED"


def test_non_string_script_body_rejected():
    with pytest.raises(Exception) as exc_info:
        ScriptMappingBuilder().build(**_minimal_config(script_body=123))
    assert exc_info.value.error_code == "SCRIPT_MAPPING_BODY_REQUIRED"


def test_invalid_input_identifier_rejected():
    with pytest.raises(Exception) as exc_info:
        ScriptMappingBuilder().build(
            **_minimal_config(inputs=[{"name": "1bad", "data_type": "character"}])
        )
    assert exc_info.value.error_code == "SCRIPT_MAPPING_VARIABLE_INVALID"


def test_invalid_input_identifier_with_special_chars_rejected():
    with pytest.raises(Exception) as exc_info:
        ScriptMappingBuilder().build(
            **_minimal_config(inputs=[{"name": "foo-bar", "data_type": "character"}])
        )
    assert exc_info.value.error_code == "SCRIPT_MAPPING_VARIABLE_INVALID"


def test_duplicate_input_name_rejected():
    with pytest.raises(Exception) as exc_info:
        ScriptMappingBuilder().build(
            **_minimal_config(
                inputs=[
                    {"name": "x", "data_type": "character"},
                    {"name": "x", "data_type": "integer"},
                ],
            )
        )
    assert exc_info.value.error_code == "SCRIPT_MAPPING_VARIABLE_INVALID"


def test_output_name_duplicating_input_rejected():
    with pytest.raises(Exception) as exc_info:
        ScriptMappingBuilder().build(
            **_minimal_config(
                inputs=[{"name": "shared", "data_type": "character"}],
                outputs=[{"name": "shared"}],
            )
        )
    assert exc_info.value.error_code == "SCRIPT_MAPPING_VARIABLE_INVALID"


def test_unsupported_input_data_type_rejected():
    with pytest.raises(Exception) as exc_info:
        ScriptMappingBuilder().build(
            **_minimal_config(inputs=[{"name": "x", "data_type": "blob"}])
        )
    assert exc_info.value.error_code == "SCRIPT_MAPPING_VARIABLE_INVALID"


def test_output_data_type_attribute_rejected():
    # Output entries must not carry data_type — Boomi infers the type
    # from the value assigned by the script at runtime.
    with pytest.raises(Exception) as exc_info:
        ScriptMappingBuilder().build(
            **_minimal_config(
                outputs=[{"name": "x", "data_type": "character"}]
            )
        )
    assert exc_info.value.error_code == "SCRIPT_MAPPING_VARIABLE_INVALID"


def test_missing_inputs_list_rejected():
    cfg = _minimal_config()
    del cfg["inputs"]
    with pytest.raises(Exception) as exc_info:
        ScriptMappingBuilder().build(**cfg)
    assert exc_info.value.error_code == "SCRIPT_MAPPING_VALIDATION_FAILED"


def test_empty_inputs_list_rejected():
    with pytest.raises(Exception) as exc_info:
        ScriptMappingBuilder().build(**_minimal_config(inputs=[]))
    assert exc_info.value.error_code == "SCRIPT_MAPPING_VALIDATION_FAILED"


def test_missing_outputs_list_rejected():
    cfg = _minimal_config()
    del cfg["outputs"]
    with pytest.raises(Exception) as exc_info:
        ScriptMappingBuilder().build(**cfg)
    assert exc_info.value.error_code == "SCRIPT_MAPPING_VALIDATION_FAILED"


def test_empty_outputs_list_rejected():
    with pytest.raises(Exception) as exc_info:
        ScriptMappingBuilder().build(**_minimal_config(outputs=[]))
    assert exc_info.value.error_code == "SCRIPT_MAPPING_VALIDATION_FAILED"


def test_missing_component_name_rejected():
    cfg = _minimal_config()
    del cfg["component_name"]
    with pytest.raises(Exception) as exc_info:
        ScriptMappingBuilder().build(**cfg)
    assert exc_info.value.error_code == "SCRIPT_MAPPING_VALIDATION_FAILED"


# ---------------------------------------------------------------------------
# Validation — boolean type checks
# ---------------------------------------------------------------------------


def test_preserve_order_must_be_boolean():
    with pytest.raises(Exception) as exc_info:
        ScriptMappingBuilder().build(**_minimal_config(preserve_order="yes"))
    assert exc_info.value.error_code == "SCRIPT_MAPPING_VALIDATION_FAILED"


def test_use_cache_must_be_boolean():
    with pytest.raises(Exception) as exc_info:
        ScriptMappingBuilder().build(**_minimal_config(use_cache="yes"))
    assert exc_info.value.error_code == "SCRIPT_MAPPING_VALIDATION_FAILED"


# ---------------------------------------------------------------------------
# Validation — raw-XML / unknown-key rejects
# ---------------------------------------------------------------------------


@pytest.mark.parametrize(
    "key",
    ("functions", "function_steps", "scripts", "xslt", "xslt_source",
     "expression", "expressions"),
)
def test_raw_xml_escape_hatch_keys_reject(key):
    cfg = _minimal_config()
    cfg[key] = "<<placeholder>>"
    with pytest.raises(Exception) as exc_info:
        ScriptMappingBuilder().build(**cfg)
    assert exc_info.value.error_code == "UNSUPPORTED_TRANSFORM_ROUTE"
    assert exc_info.value.field == key


def test_unknown_top_level_key_rejected():
    cfg = _minimal_config()
    cfg["mystery_field"] = "value"
    with pytest.raises(Exception) as exc_info:
        ScriptMappingBuilder().build(**cfg)
    assert exc_info.value.error_code == "SCRIPT_MAPPING_VALIDATION_FAILED"
    assert exc_info.value.field == "mystery_field"


# ---------------------------------------------------------------------------
# Validation — secret-shaped key scan
# ---------------------------------------------------------------------------


@pytest.mark.parametrize(
    "field",
    ("password", "token", "client_secret", "api_key", "authorization", "bearer"),
)
def test_secret_shaped_top_level_key_rejected(field):
    cfg = _minimal_config()
    cfg[field] = "deadbeef"
    with pytest.raises(Exception) as exc_info:
        ScriptMappingBuilder().build(**cfg)
    assert exc_info.value.error_code == "PLAINTEXT_SECRET_REJECTED"


def test_nested_secret_shaped_key_rejected():
    cfg = _minimal_config()
    cfg["inputs"][0]["password"] = "leaked"
    with pytest.raises(Exception) as exc_info:
        ScriptMappingBuilder().build(**cfg)
    assert exc_info.value.error_code == "PLAINTEXT_SECRET_REJECTED"


def test_script_body_credential_string_not_flagged():
    # The secret scan checks dict KEYS, not string values. A credential-
    # looking string in the script_body (which is opaque caller code)
    # must NOT trip the scan.
    cfg = _minimal_config(
        script_body='String token = "sk_live_test_value"; output = token'
    )
    # Should build without raising.
    xml = ScriptMappingBuilder().build(**cfg)
    assert "sk_live_test_value" in xml


# ---------------------------------------------------------------------------
# Anti-template hygiene — no canned business script bodies in module
# ---------------------------------------------------------------------------


def test_module_carries_no_canned_script_business_logic():
    """Defense-in-depth: the ScriptMappingBuilder module must not ship
    any actual Groovy / JavaScript bodies, lookup tables, or business
    snippets. The implementation is structural only."""
    from boomi_mcp.categories.components.builders import script_mapping_builder

    import inspect
    source = inspect.getsource(script_mapping_builder)
    for marker in (
        "groovy.lang.",
        "import org.",
        "function (",
        "def fn(",
        "SELECT ",
        "INSERT ",
        "UPDATE ",
    ):
        assert marker not in source, (
            f"script_mapping_builder.py must not embed canned script "
            f"content; found {marker!r}"
        )


# ---------------------------------------------------------------------------
# manage_component dispatch — Issue #41 QA bug regression
# ---------------------------------------------------------------------------


class TestManageComponentDispatchesScriptMapping:
    """The schema template advertises ``tool: manage_component
    (action='create')`` for script.mapping. ``manage_component.create_component``
    must dispatch through ScriptMappingBuilder when ``component_type ==
    'script.mapping'``, not fall through to the 'xml is required' error."""

    def _call_create(self, config):
        from unittest.mock import MagicMock, patch
        from boomi_mcp.categories.components.manage_component import create_component

        # Intercept the network call — the dispatch is what we're testing,
        # not the HTTP layer. Return a fake component_id so the success
        # path can complete.
        with patch(
            "boomi_mcp.categories.components.manage_component._create_component_raw",
            return_value={
                "component_id": "00000000-0000-0000-0000-000000000001",
                "name": config.get("component_name") or "x",
                "type": "script.mapping",
            },
        ) as fake_raw:
            result = create_component(MagicMock(), "test-profile", config)
            return result, fake_raw

    def test_standalone_script_mapping_create_routes_through_builder(self):
        result, fake_raw = self._call_create(_minimal_config())
        assert result["_success"] is True, result
        assert result["component_id"] == "00000000-0000-0000-0000-000000000001"
        assert result["type"] == "script.mapping"
        # The XML POSTed to _create_component_raw must carry the structured
        # MappingScript shape — proving we dispatched to the builder, not
        # to the raw-XML escape hatch.
        assert fake_raw.call_count == 1
        posted_xml = fake_raw.call_args[0][1]
        assert '<MappingScript xmlns="" language="groovy2"' in posted_xml
        assert '<Input dataType="character" index="1" name="inputValue"/>' in posted_xml
        assert '<Output index="2" name="outputValue"/>' in posted_xml

    def test_invalid_script_mapping_config_surfaces_structured_envelope(self):
        # Drop script_body — must surface SCRIPT_MAPPING_BODY_REQUIRED with
        # field + hint, NOT the generic 'xml is required' message.
        cfg = _minimal_config()
        del cfg["script_body"]
        result, fake_raw = self._call_create(cfg)
        assert result["_success"] is False
        assert result["error_code"] == "SCRIPT_MAPPING_BODY_REQUIRED"
        assert result["field"] == "script_body"
        # Network was NOT called — validation rejected before emission.
        assert fake_raw.call_count == 0

    def test_unsupported_language_surfaces_structured_envelope(self):
        cfg = _minimal_config(language="python")
        result, fake_raw = self._call_create(cfg)
        assert result["_success"] is False
        assert result["error_code"] == "SCRIPT_MAPPING_LANGUAGE_UNSUPPORTED"
        assert fake_raw.call_count == 0


# ============================================================================
# Issue #45 — Component XML update preservation
# ============================================================================


def test_script_mapping_preservation_policy_attached():
    policy = ScriptMappingBuilder.PRESERVATION_POLICY
    assert policy.component_type == "script.mapping"
    paths = {op.path for op in policy.owned_paths}
    assert paths == {"bns:object/MappingScript"}


def test_script_mapping_update_preserves_unknown_xml():
    """Outside the owned `<MappingScript>` subtree, unknown XML must survive."""
    import xml.etree.ElementTree as ET
    from boomi_mcp.categories.components.component_update_preservation import (
        merge_for_update,
    )

    NS = {"bns": "http://api.platform.boomi.com/"}
    desired = ScriptMappingBuilder().build(
        **_minimal_config(component_name="renamed")
    )
    current = ScriptMappingBuilder().build(**_minimal_config())
    # Inject a future bns:Component-level child after </bns:object>
    current = current.replace(
        "</bns:object>",
        "</bns:object><bns:processOverrides><override key=\"x\"/></bns:processOverrides>",
    )

    merged = merge_for_update(
        current, desired, ScriptMappingBuilder.PRESERVATION_POLICY
    )
    root = ET.fromstring(merged)
    # Owned subtree was renamed
    assert root.attrib["name"] == "renamed"
    # processOverrides survived
    overrides = root.find("bns:processOverrides", NS)
    assert overrides is not None
    assert overrides.find("override") is not None

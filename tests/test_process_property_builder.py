"""Issue #131 (M11.7, epic #118) — ProcessPropertyBuilder unit tests.

Byte-locks the builder's own emission and structurally round-trips it against
the live renera capture (tests/fixtures/live_xml/m11/processproperty_minimal.xml
— live exports self-close empty elements, so the round-trip compares parsed
tag/attr/text structure, not bytes). Exercises the full validate_config
matrix including the intentionally-rejected 'encrypted' / 'allowed_values'
keys, the character/hidden type rejections, and the password
plaintext-default guard (live XSD + round-trip evidence, 2026-07-03).
"""

from __future__ import annotations

import sys
from pathlib import Path

import pytest
import xml.etree.ElementTree as ET

_project_root = str(Path(__file__).resolve().parent.parent)
if _project_root not in sys.path:
    sys.path.insert(0, _project_root)

from src.boomi_mcp.categories.components.builders.connector_builder import (
    BuilderValidationError,
)
from src.boomi_mcp.categories.components.builders.process_property_builder import (
    PROCESS_PROPERTY_BUILDERS,
    ProcessPropertyBuilder,
    get_process_property_builder,
)

NS = {"bns": "http://api.platform.boomi.com/"}

_KEY_1 = "0e89ebf1-cd46-46df-904e-94c7e7ade31e"
_KEY_2 = "04e8b67c-95ac-4b8d-90cb-ab5e78c638fa"
_KEY_3 = "2c6e539c-847a-4abc-94ef-a0d39b17d8e2"
_KEY_4 = "cfa10de5-455c-4e31-b43d-4b457d3aef4c"

_MINIMAL_FIXTURE = (
    Path(__file__).resolve().parent
    / "fixtures"
    / "live_xml"
    / "m11"
    / "processproperty_minimal.xml"
)


def _minimal_config(**overrides):
    cfg = {
        "component_name": "New Process Property",
        "properties": [
            {
                "key": _KEY_1,
                "name": "Example Property",
                "type": "string",
            }
        ],
    }
    cfg.update(overrides)
    return cfg


def _validate(**overrides):
    return ProcessPropertyBuilder.validate_config(_minimal_config(**overrides))


# ---------------------------------------------------------------------------
# Registry / dispatch
# ---------------------------------------------------------------------------


def test_registry_and_lookup():
    assert PROCESS_PROPERTY_BUILDERS == {"processproperty": ProcessPropertyBuilder}
    assert get_process_property_builder("processproperty") is ProcessPropertyBuilder
    assert get_process_property_builder("script.mapping") is None
    assert ProcessPropertyBuilder.SUPPORTED_COMPONENT_TYPES == ("processproperty",)
    # The platform XSD enumeration, live-verified 2026-07-03.
    assert ProcessPropertyBuilder.SUPPORTED_PROPERTY_TYPES == (
        "string",
        "number",
        "boolean",
        "date",
        "password",
    )


def test_preservation_policy_owns_defined_process_properties():
    policy = ProcessPropertyBuilder.PRESERVATION_POLICY
    assert policy.component_type == "processproperty"
    assert [p.path for p in policy.owned_paths] == [
        "bns:object/DefinedProcessProperties"
    ]


# ---------------------------------------------------------------------------
# Emission
# ---------------------------------------------------------------------------


def test_build_minimal_byte_lock():
    xml = ProcessPropertyBuilder().build(**_minimal_config())
    assert xml == (
        '<bns:Component xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance" '
        'xmlns:bns="http://api.platform.boomi.com/" '
        'type="processproperty" '
        'name="New Process Property">'
        "<bns:encryptedValues/>"
        "<bns:description></bns:description>"
        "<bns:object>"
        '<DefinedProcessProperties xmlns="">'
        f'<definedProcessProperty key="{_KEY_1}">'
        "<helpText></helpText>"
        "<label>Example Property</label>"
        "<type>string</type>"
        "<defaultValue></defaultValue>"
        "<allowedValues/>"
        "<persisted>false</persisted>"
        "</definedProcessProperty>"
        "</DefinedProcessProperties>"
        "</bns:object>"
        "</bns:Component>"
    )


def _structure(element: ET.Element):
    """(tag, sorted attrs, stripped text, children) tree for round-tripping."""
    return (
        element.tag,
        sorted(element.attrib.items()),
        (element.text or "").strip(),
        [_structure(child) for child in element],
    )


def test_build_round_trips_against_live_minimal_capture():
    built = ET.fromstring(ProcessPropertyBuilder().build(**_minimal_config()))
    live = ET.fromstring(_MINIMAL_FIXTURE.read_text(encoding="utf-8"))
    built_obj = built.find("bns:object/DefinedProcessProperties", NS)
    live_obj = live.find("bns:object/DefinedProcessProperties", NS)
    assert _structure(built_obj) == _structure(live_obj)
    # Create invariant: no server-assigned identity on the emitted envelope.
    assert "componentId" not in built.attrib
    assert "version" not in built.attrib
    assert built.get("type") == live.get("type") == "processproperty"


def test_build_multi_property_types_order_and_persisted():
    xml = ProcessPropertyBuilder().build(
        component_name="Runtime Settings",
        folder_path="Renera/M11",
        description="ops toggles",
        properties=[
            {"key": _KEY_1, "name": "Flag", "type": "boolean", "default_value": "true"},
            {"key": _KEY_2, "name": "Limit", "type": "number", "default_value": "5"},
            {"key": _KEY_3, "name": "Label", "type": "string", "help_text": "shown"},
            {"key": _KEY_4, "name": "Cutoff", "type": "date", "persisted": True},
        ],
    )
    root = ET.fromstring(xml)
    assert root.get("folderFullPath") == "Renera/M11"
    props = root.findall(
        "bns:object/DefinedProcessProperties/definedProcessProperty", NS
    )
    assert [p.get("key") for p in props] == [_KEY_1, _KEY_2, _KEY_3, _KEY_4]
    assert [p.find("type").text for p in props] == [
        "boolean",
        "number",
        "string",
        "date",
    ]
    assert [p.find("persisted").text for p in props] == [
        "false",
        "false",
        "false",
        "true",
    ]
    for p in props:
        assert [c.tag for c in p] == [
            "helpText",
            "label",
            "type",
            "defaultValue",
            "allowedValues",
            "persisted",
        ]
        assert list(p.find("allowedValues")) == []


def test_build_escapes_xml_content():
    xml = ProcessPropertyBuilder().build(
        component_name='A & B <Props>',
        properties=[
            {
                "key": _KEY_1,
                "name": 'Say "hi" & <bye>',
                "type": "string",
                "default_value": "<none>",
                "help_text": "a&b",
            }
        ],
    )
    assert 'name="A &amp; B &lt;Props&gt;"' in xml
    assert "<label>Say &quot;hi&quot; &amp; &lt;bye&gt;</label>" in xml
    assert "<defaultValue>&lt;none&gt;</defaultValue>" in xml
    assert "<helpText>a&amp;b</helpText>" in xml
    root = ET.fromstring(xml)  # stays well-formed
    assert root.get("name") == "A & B <Props>"


def test_build_raises_on_invalid_config():
    with pytest.raises(BuilderValidationError):
        ProcessPropertyBuilder().build(component_name="X", properties=[])


# ---------------------------------------------------------------------------
# validate_config matrix
# ---------------------------------------------------------------------------


def test_valid_minimal_config_passes():
    assert _validate() is None


def test_component_name_missing_or_blank():
    for cfg in (
        {"properties": _minimal_config()["properties"]},
        _minimal_config(component_name="   "),
    ):
        err = ProcessPropertyBuilder.validate_config(cfg)
        assert err is not None
        assert err.error_code == "PROCESS_PROPERTY_NAME_REQUIRED"


def test_properties_missing_or_empty():
    for props in (None, []):
        cfg = {"component_name": "X"}
        if props is not None:
            cfg["properties"] = props
        err = ProcessPropertyBuilder.validate_config(cfg)
        assert err is not None
        assert err.error_code == "PROCESS_PROPERTY_PROPERTY_REQUIRED"


def test_unknown_top_level_key_rejected():
    err = _validate(language="groovy2")
    assert err is not None
    assert err.error_code == "PROCESS_PROPERTY_VALIDATION_FAILED"
    assert err.field == "language"


def test_raw_subtree_key_rejected_with_dedicated_code():
    err = _validate(defined_process_properties="<DefinedProcessProperties/>")
    assert err is not None
    assert err.error_code == "PROCESS_PROPERTY_RAW_XML_UNSUPPORTED"


def test_property_key_required_and_uuid_checked():
    err = ProcessPropertyBuilder.validate_config(
        {"component_name": "X", "properties": [{"name": "P", "type": "string"}]}
    )
    assert err.error_code == "PROCESS_PROPERTY_KEY_REQUIRED"
    err = ProcessPropertyBuilder.validate_config(
        {
            "component_name": "X",
            "properties": [{"key": "not-a-uuid", "name": "P", "type": "string"}],
        }
    )
    assert err.error_code == "PROCESS_PROPERTY_KEY_INVALID"
    # Uppercase / non-canonical forms are rejected: the key must round-trip
    # byte-stable because map functions reference it verbatim.
    err = ProcessPropertyBuilder.validate_config(
        {
            "component_name": "X",
            "properties": [{"key": _KEY_1.upper(), "name": "P", "type": "string"}],
        }
    )
    assert err.error_code == "PROCESS_PROPERTY_KEY_INVALID"


def test_duplicate_key_and_name_rejected():
    err = ProcessPropertyBuilder.validate_config(
        {
            "component_name": "X",
            "properties": [
                {"key": _KEY_1, "name": "A", "type": "string"},
                {"key": _KEY_1, "name": "B", "type": "string"},
            ],
        }
    )
    assert err.error_code == "PROCESS_PROPERTY_DUPLICATE_KEY"
    err = ProcessPropertyBuilder.validate_config(
        {
            "component_name": "X",
            "properties": [
                {"key": _KEY_1, "name": "A", "type": "string"},
                {"key": _KEY_2, "name": "A", "type": "string"},
            ],
        }
    )
    assert err.error_code == "PROCESS_PROPERTY_DUPLICATE_NAME"


def test_unsupported_types_rejected_including_character_and_hidden():
    for bad_type in ("character", "integer", "", None):
        err = ProcessPropertyBuilder.validate_config(
            {
                "component_name": "X",
                "properties": [{"key": _KEY_1, "name": "P", "type": bad_type}],
            }
        )
        assert err is not None, bad_type
        assert err.error_code == "PROCESS_PROPERTY_TYPE_UNSUPPORTED", bad_type
    # 'hidden' is the docs' UI data type, not an XML token — the rejection
    # carries the exact mapping hint (any casing).
    for hidden_spelling in ("hidden", "Hidden"):
        err = ProcessPropertyBuilder.validate_config(
            {
                "component_name": "X",
                "properties": [
                    {"key": _KEY_1, "name": "P", "type": hidden_spelling}
                ],
            }
        )
        assert err is not None, hidden_spelling
        assert err.error_code == "PROCESS_PROPERTY_TYPE_UNSUPPORTED", hidden_spelling
        assert err.hint == "UI type 'Hidden' serializes as XML token 'password'"


def test_password_type_accepted_with_omitted_or_empty_default():
    for prop in (
        {"key": _KEY_1, "name": "Secret", "type": "password"},
        {"key": _KEY_1, "name": "Secret", "type": "password", "default_value": ""},
        {"key": _KEY_1, "name": "Secret", "type": "password", "default_value": None},
    ):
        err = ProcessPropertyBuilder.validate_config(
            {"component_name": "X", "properties": [prop]}
        )
        assert err is None, prop


def test_password_type_emits_password_token_and_empty_default():
    xml = ProcessPropertyBuilder().build(
        component_name="Secrets",
        properties=[
            {"key": _KEY_1, "name": "API Key", "type": "password"},
        ],
    )
    assert "<type>password</type>" in xml
    assert "<defaultValue></defaultValue>" in xml
    props = ET.fromstring(xml).findall(
        "bns:object/DefinedProcessProperties/definedProcessProperty", NS
    )
    assert [p.find("type").text for p in props] == ["password"]
    assert [p.find("defaultValue").text for p in props] == [None]


def test_password_nonempty_default_rejected_plaintext():
    for secret_default in ("s3cret!", "   "):
        err = ProcessPropertyBuilder.validate_config(
            {
                "component_name": "X",
                "properties": [
                    {
                        "key": _KEY_1,
                        "name": "P",
                        "type": "password",
                        "default_value": secret_default,
                    }
                ],
            }
        )
        assert err is not None, secret_default
        assert err.error_code == "PLAINTEXT_SECRET_REJECTED", secret_default
        assert err.field == "properties[0].default_value"
        hint = err.hint or ""
        assert "extensions" in hint or "runtime" in hint
        # The secret value must never be echoed in the error surface.
        assert "s3cret!" not in str(err)
        assert "s3cret!" not in hint


def test_password_nonstring_default_still_default_invalid():
    # Pins the guard's placement AFTER the string-type check: a non-string
    # default on a password property is a shape error, not a secret echo.
    err = ProcessPropertyBuilder.validate_config(
        {
            "component_name": "X",
            "properties": [
                {"key": _KEY_1, "name": "P", "type": "password", "default_value": 5}
            ],
        }
    )
    assert err.error_code == "PROCESS_PROPERTY_DEFAULT_INVALID"


def test_encrypted_property_key_rejected_with_evidence_hint():
    err = ProcessPropertyBuilder.validate_config(
        {
            "component_name": "X",
            "properties": [
                {"key": _KEY_1, "name": "P", "type": "string", "encrypted": True}
            ],
        }
    )
    assert err.error_code == "PROCESS_PROPERTY_VALIDATION_FAILED"
    assert "encrypted" in err.field
    assert "live" in (err.hint or "")


def test_allowed_values_property_key_rejected_with_update_warning():
    err = ProcessPropertyBuilder.validate_config(
        {
            "component_name": "X",
            "properties": [
                {
                    "key": _KEY_1,
                    "name": "P",
                    "type": "string",
                    "allowed_values": [{"label": "A", "value": "a"}],
                }
            ],
        }
    )
    assert err.error_code == "PROCESS_PROPERTY_VALIDATION_FAILED"
    assert "allowedValues" in (err.hint or "") or "allowedValueSet" in (err.hint or "")


def test_default_value_and_help_text_type_checked():
    for key in ("default_value", "help_text"):
        err = ProcessPropertyBuilder.validate_config(
            {
                "component_name": "X",
                "properties": [
                    {"key": _KEY_1, "name": "P", "type": "string", key: 5}
                ],
            }
        )
        assert err.error_code == "PROCESS_PROPERTY_DEFAULT_INVALID", key


def test_persisted_must_be_bool():
    err = ProcessPropertyBuilder.validate_config(
        {
            "component_name": "X",
            "properties": [
                {"key": _KEY_1, "name": "P", "type": "string", "persisted": "true"}
            ],
        }
    )
    assert err.error_code == "PROCESS_PROPERTY_VALIDATION_FAILED"


def test_secret_shaped_keys_rejected_deep():
    err = _validate(description="ok", extra=None) if False else None
    err = ProcessPropertyBuilder.validate_config(
        {
            "component_name": "X",
            "properties": [{"key": _KEY_1, "name": "P", "type": "string"}],
            "credentials": {"user": "u"},
        }
    )
    assert err.error_code == "PLAINTEXT_SECRET_REJECTED"
    # Deep scan inside list entries too.
    err = ProcessPropertyBuilder.validate_config(
        {
            "component_name": "X",
            "properties": [
                {"key": _KEY_1, "name": "P", "type": "string", "token": "t"}
            ],
        }
    )
    assert err.error_code == "PLAINTEXT_SECRET_REJECTED"


def test_redact_forbidden_secret_fields_in_place():
    cfg = {
        "component_name": "X",
        "api_key": "k",
        "properties": [{"key": _KEY_1, "name": "P", "type": "string", "secret": "s"}],
    }
    ProcessPropertyBuilder.redact_forbidden_secret_fields_in_place(cfg)
    assert cfg["api_key"] == "[REDACTED]"
    assert cfg["properties"][0]["secret"] == "[REDACTED]"


def test_redact_scrubs_password_type_default_values():
    cfg = {
        "component_name": "X",
        "properties": [
            {"key": _KEY_1, "name": "A", "type": "password", "default_value": "leak"},
            {"key": _KEY_2, "name": "B", "type": "string", "default_value": "keep"},
            {"key": _KEY_3, "name": "C", "type": "password", "default_value": ""},
            {"key": _KEY_4, "name": "D", "type": "hidden", "default_value": "leak2"},
        ],
    }
    ProcessPropertyBuilder.redact_forbidden_secret_fields_in_place(cfg)
    props = cfg["properties"]
    assert props[0]["default_value"] == "[REDACTED]"
    assert props[1]["default_value"] == "keep"  # non-secret type untouched
    assert props[2]["default_value"] == ""  # empty stays empty
    assert props[3]["default_value"] == "[REDACTED]"  # defensive: hidden too

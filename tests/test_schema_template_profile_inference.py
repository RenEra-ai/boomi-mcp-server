"""Schema-template tests for resource_type='profile_inference' (issue #47).

Mirrors the hygiene/assertion patterns of the other schema-template tests:
placeholder-only examples, no canned SQL/XML, advertises the modes, safety
flags, and PROFILE_INFERENCE_* error codes.
"""

from boomi_mcp.categories.meta_tools import get_schema_template_action as gst


_FORBIDDEN_TEMPLATE_SUBSTRINGS = (
    "select ",
    " from ",
    " where ",
    "<?xml",
    "<xs:schema",
    "<xs:element",
    "<process",
    "<connector",
)

_MODES = (
    "profile_from_db_metadata",
    "profile_from_sample_json",
    "profile_from_xsd",
    "profile_from_sample_xml",
)


def test_profile_inference_resource_returns_template():
    r = gst(resource_type="profile_inference")
    assert r["_success"] is True
    assert r["resource_type"] == "profile_inference"
    assert r["tool"].startswith("infer_profile_fields")


def test_profile_inference_lists_all_modes():
    r = gst(resource_type="profile_inference")
    assert set(r["supported_source_types"]) == set(_MODES)


def test_profile_inference_advertises_safety_flags():
    r = gst(resource_type="profile_inference")
    assert r["read_only"] is True
    assert r["boomi_mutation"] is False
    assert r["raw_xml_exposed"] is False


def test_profile_inference_advertises_error_codes():
    r = gst(resource_type="profile_inference")
    codes = r["error_codes"]
    for expected in (
        "PROFILE_INFERENCE_INVALID_INPUT",
        "PROFILE_INFERENCE_INVALID_SAMPLE",
        "PROFILE_INFERENCE_UNSUPPORTED_SHAPE",
        "PROFILE_INFERENCE_AMBIGUOUS_SHAPE",
        "PROFILE_INFERENCE_INPUT_TOO_LARGE",
        "PROFILE_INFERENCE_UNSUPPORTED_NAMESPACE",
        "PROFILE_INFERENCE_RECURSIVE_XML",
    ):
        assert expected in codes, f"missing error code {expected}"


def test_profile_inference_documents_options():
    r = gst(resource_type="profile_inference")
    opts = r["options"]
    for key in ("component_name", "root_name", "array_item_name", "datetime_detection"):
        assert key in opts


def test_profile_inference_examples_are_placeholder_only():
    import json

    r = gst(resource_type="profile_inference")
    blob = json.dumps([r.get("examples", []), r.get("template", {})]).lower()
    for marker in _FORBIDDEN_TEMPLATE_SUBSTRINGS:
        assert marker not in blob, f"profile_inference template contains forbidden marker {marker!r}"


def test_profile_inference_clarifies_existing_profile_index_deferred():
    r = gst(resource_type="profile_inference")
    note = " ".join(str(v) for v in r.get("out_of_scope", {}).values()).lower()
    assert "existing" in note  # does not index arbitrary existing live profile XML


def test_unknown_resource_type_lists_profile_inference():
    r = gst(resource_type="does_not_exist_xyzzy")
    assert r["_success"] is False
    assert "profile_inference" in r["valid_types"]

"""Issue #47: pure profile-inference layer tests.

Covers the four read-only inference modes (DB metadata, sample JSON, XSD,
sample XML) implemented in
``boomi_mcp.categories.components.builders.profile_inference``. The pure layer
parses caller-supplied artifacts and delegates to the issue-#43 helpers
(``profile_from_db_read_fields`` / ``profile_from_json_schema`` /
``profile_from_xml_schema``); inference metadata lives in a parallel ``fields``
list, never inside the builder nodes.
"""

from __future__ import annotations

import json as _json

import pytest

from boomi_mcp.categories.components.builders.connector_builder import (
    BuilderValidationError,
)
from boomi_mcp.categories.components.builders import profile_inference as pi


# ---------------------------------------------------------------------------
# Task 1 — scaffold: codes, limit clamping, secret-name detection
# ---------------------------------------------------------------------------


def test_error_codes_present():
    for c in (
        "PROFILE_INFERENCE_INVALID_INPUT",
        "PROFILE_INFERENCE_INVALID_SAMPLE",
        "PROFILE_INFERENCE_UNSUPPORTED_SHAPE",
        "PROFILE_INFERENCE_AMBIGUOUS_SHAPE",
        "PROFILE_INFERENCE_INPUT_TOO_LARGE",
        "PROFILE_INFERENCE_UNSUPPORTED_NAMESPACE",
        "PROFILE_INFERENCE_RECURSIVE_XML",
    ):
        assert getattr(pi, c) == c


def test_limits_clamp_and_lower():
    lim = pi._resolve_limits({"max_fields": 10, "max_nodes": 99999999})
    assert lim["max_fields"] == 10  # lowering allowed
    assert lim["max_nodes"] == pi._HARD_CAPS["max_nodes"]  # raise clamped to hard cap


def test_limits_default_when_none():
    lim = pi._resolve_limits(None)
    assert lim == pi._DEFAULT_LIMITS


def test_secret_named_detection_is_exact_not_substring():
    assert (
        pi._is_secret_named("API-Key")
        and pi._is_secret_named("password")
        and pi._is_secret_named("client_secret")
    )
    # exact whole-name match: must NOT false-positive on legit names containing a token
    for ok in (
        "customer_id",
        "authorization_date",
        "token_count",
        "bearer_name",
        "secret_santa_id",
    ):
        assert not pi._is_secret_named(ok)


# ---------------------------------------------------------------------------
# Task 2 — DB metadata inference
# ---------------------------------------------------------------------------


def test_db_metadata_happy_maps_core_types():
    r = pi.infer_profile_from_db_metadata(
        {
            "columns": [
                {"name": "name", "data_type": "varchar", "nullable": False},
                {"name": "qty", "data_type": "int"},
                {"name": "created", "data_type": "timestamp"},
            ]
        }
    )
    assert r["generation_mode"] == "profile_from_db_metadata"
    assert r["component_type"] == "profile.db"
    assert r["profile_type"] == "database.read"
    by = {f["name"]: f for f in r["fields"]}
    assert by["name"]["data_type"] == "character" and by["name"]["required"] is True
    assert by["qty"]["data_type"] == "number"
    assert by["created"]["data_type"] == "datetime"
    assert r["ready_for_builder"] is True
    assert r["mappable_paths"] == ["name", "qty", "created"]


def test_db_metadata_accepts_fields_and_result_columns_aliases():
    for key in ("fields", "result_columns"):
        r = pi.infer_profile_from_db_metadata({key: [{"name": "a", "data_type": "varchar"}]})
        assert r["mappable_paths"] == ["a"]


def test_db_metadata_accepts_bare_list():
    r = pi.infer_profile_from_db_metadata([{"name": "a", "db_type": "nvarchar"}])
    assert r["mappable_paths"] == ["a"]


def test_db_metadata_type_alias_keys():
    # jdbc_type and type are accepted alongside data_type/db_type
    r = pi.infer_profile_from_db_metadata(
        [{"name": "a", "jdbc_type": "DECIMAL"}, {"name": "b", "type": "DATE"}]
    )
    by = {f["name"]: f for f in r["fields"]}
    assert by["a"]["data_type"] == "number" and by["b"]["data_type"] == "datetime"


def test_db_metadata_missing_nullable_lowers_confidence_not_required():
    r = pi.infer_profile_from_db_metadata({"columns": [{"name": "a", "data_type": "varchar"}]})
    f = r["fields"][0]
    assert f["required"] is False
    assert f["confidence"] == "medium"
    assert f["confirmation_required"] is False
    assert r["ready_for_builder"] is True


def test_db_metadata_required_aliases():
    r = pi.infer_profile_from_db_metadata(
        [
            {"name": "a", "data_type": "varchar", "required": True},
            {"name": "b", "data_type": "varchar", "mandatory": True},
            {"name": "c", "data_type": "varchar", "optional": False},
        ]
    )
    assert all(f["required"] is True for f in r["fields"])


def test_db_metadata_boolean_is_ambiguous_candidate():
    r = pi.infer_profile_from_db_metadata({"columns": [{"name": "flag", "data_type": "bit"}]})
    f = r["fields"][0]
    assert f["confidence"] == "ambiguous"
    assert f["confirmation_required"] is True
    assert f["data_type"] == "character"
    assert r["ready_for_builder"] is False


def test_db_metadata_unknown_type_is_ambiguous():
    r = pi.infer_profile_from_db_metadata([{"name": "geo", "data_type": "geography"}])
    f = r["fields"][0]
    assert f["confidence"] == "ambiguous" and f["confirmation_required"] is True
    assert f["data_type"] == "character"


@pytest.mark.parametrize("binary_type", ["varbinary", "blob", "image", "binary(16)", "bytea"])
def test_db_metadata_binary_unsupported(binary_type):
    with pytest.raises(BuilderValidationError) as e:
        pi.infer_profile_from_db_metadata({"columns": [{"name": "b", "data_type": binary_type}]})
    assert e.value.error_code == pi.PROFILE_INFERENCE_UNSUPPORTED_SHAPE


def test_db_metadata_missing_type_is_invalid_input():
    with pytest.raises(BuilderValidationError) as e:
        pi.infer_profile_from_db_metadata({"columns": [{"name": "a"}]})
    assert e.value.error_code == pi.PROFILE_INFERENCE_INVALID_INPUT


def test_db_metadata_missing_name_is_invalid_input():
    with pytest.raises(BuilderValidationError) as e:
        pi.infer_profile_from_db_metadata({"columns": [{"data_type": "varchar"}]})
    assert e.value.error_code == pi.PROFILE_INFERENCE_INVALID_INPUT


def test_db_metadata_unknown_container_key_is_invalid_input():
    with pytest.raises(BuilderValidationError) as e:
        pi.infer_profile_from_db_metadata({"rows": [{"name": "a", "data_type": "varchar"}]})
    assert e.value.error_code == pi.PROFILE_INFERENCE_INVALID_INPUT


def test_db_metadata_duplicate_name_propagates_43_error():
    with pytest.raises(BuilderValidationError) as e:
        pi.infer_profile_from_db_metadata(
            {"columns": [{"name": "a", "data_type": "varchar"}, {"name": "a", "data_type": "int"}]}
        )
    assert e.value.error_code == "DUPLICATE_PROFILE_FIELD_PATH"


def test_db_metadata_reserved_char_propagates_43_error():
    with pytest.raises(BuilderValidationError) as e:
        pi.infer_profile_from_db_metadata([{"name": "a/b", "data_type": "varchar"}])
    assert e.value.error_code == "INVALID_PROFILE_FIELD_PATH"


def test_db_metadata_secret_named_field_withheld():
    r = pi.infer_profile_from_db_metadata(
        {"columns": [{"name": "id", "data_type": "int"}, {"name": "password", "data_type": "varchar"}]}
    )
    assert [f["name"] for f in r["fields"]] == ["id"]
    assert any(i["code"] == pi.PROFILE_INFERENCE_SECRET_FIELD_WITHHELD for i in r["issues"])


def test_db_metadata_component_name_copied():
    r = pi.infer_profile_from_db_metadata(
        [{"name": "a", "data_type": "varchar"}], options={"component_name": "Src"}
    )
    assert r["component_name"] == "Src"


def test_db_metadata_max_fields_limit():
    cols = [{"name": f"c{i}", "data_type": "varchar"} for i in range(5)]
    with pytest.raises(BuilderValidationError) as e:
        pi.infer_profile_from_db_metadata(cols, options={"max_fields": 3})
    assert e.value.error_code == pi.PROFILE_INFERENCE_INPUT_TOO_LARGE


def test_db_metadata_no_value_or_node_keys_leak_into_builder_nodes():
    r = pi.infer_profile_from_db_metadata([{"name": "a", "data_type": "varchar"}])
    # #47 enrichment must NOT be injected into the delegated builder nodes
    for entry in r["field_index_by_path"].values():
        assert "confidence" not in entry
        assert "ambiguities" not in entry
        assert "confirmation_required" not in entry


# ---------------------------------------------------------------------------
# Task 3 — JSON sample inference
# ---------------------------------------------------------------------------


def test_json_nested_object_paths():
    r = pi.infer_profile_from_sample_json('{"id":1,"name":"x","child":{"leaf":true}}')
    assert r["generation_mode"] == "profile_from_sample_json"
    assert r["component_type"] == "profile.json"
    assert r["profile_type"] == "json.generated"
    paths = set(r["field_index_by_path"])
    assert {"Root", "Root/id", "Root/name", "Root/child", "Root/child/leaf"} <= paths
    by = {f["path"]: f for f in r["fields"]}
    assert by["Root/id"]["data_type"] == "number"
    assert by["Root/name"]["data_type"] == "character"
    assert by["Root/child/leaf"]["data_type"] == "boolean"
    assert by["Root/child"]["kind"] == "object" and by["Root/child"]["mappable"] is False


def test_json_accepts_parsed_dict():
    r = pi.infer_profile_from_sample_json({"id": 1})
    assert r["mappable_paths"] == ["Root/id"]


def test_json_array_of_objects_uses_brackets_and_optional():
    r = pi.infer_profile_from_sample_json('[{"a":1,"b":2},{"a":3}]')
    assert "Root/items[]/a" in r["field_index_by_path"]
    by = {f["path"]: f for f in r["fields"]}
    assert by["Root/items[]/a"]["required"] is True
    assert by["Root/items[]/b"]["required"] is False  # missing in row 2
    assert by["Root/items[]/b"]["confirmation_required"] is True
    assert r["ready_for_builder"] is False


def test_json_array_item_name_option():
    r = pi.infer_profile_from_sample_json('[{"a":1}]', options={"array_item_name": "rows", "root_name": "Doc"})
    assert "Doc/rows[]/a" in r["field_index_by_path"]


def test_json_iso_datetime_detection():
    r = pi.infer_profile_from_sample_json('{"ts":"2026-01-01T00:00:00Z","d":"2026-01-01"}')
    by = {f["path"]: f for f in r["fields"]}
    assert by["Root/ts"]["data_type"] == "datetime"
    assert by["Root/d"]["data_type"] == "datetime"


def test_json_datetime_detection_off():
    r = pi.infer_profile_from_sample_json('{"ts":"2026-01-01T00:00:00Z"}', options={"datetime_detection": False})
    assert {f["path"]: f for f in r["fields"]}["Root/ts"]["data_type"] == "character"


def test_json_numeric_string_stays_character():
    r = pi.infer_profile_from_sample_json('{"code":"00123"}')
    assert {f["path"]: f for f in r["fields"]}["Root/code"]["data_type"] == "character"


def test_json_mixed_scalar_is_ambiguous_not_error():
    r = pi.infer_profile_from_sample_json('[{"v":1},{"v":"x"}]')
    f = {f["path"]: f for f in r["fields"]}["Root/items[]/v"]
    assert f["confidence"] == "ambiguous"
    assert f["data_type"] == "character"
    assert r["ready_for_builder"] is False


def test_json_null_only_is_ambiguous():
    r = pi.infer_profile_from_sample_json('{"maybe":null}')
    f = {f["path"]: f for f in r["fields"]}["Root/maybe"]
    assert f["confidence"] == "ambiguous" and f["confirmation_required"] is True
    assert f["data_type"] == "character"


@pytest.mark.parametrize(
    "sample,code",
    [
        ('"just a string"', "PROFILE_INFERENCE_UNSUPPORTED_SHAPE"),  # scalar root
        ("5", "PROFILE_INFERENCE_UNSUPPORTED_SHAPE"),  # numeric root
        ("[1,2,3]", "PROFILE_INFERENCE_UNSUPPORTED_SHAPE"),  # array of scalars
        ("[]", "PROFILE_INFERENCE_UNSUPPORTED_SHAPE"),  # empty array
        ('[{"a":1}, 5]', "PROFILE_INFERENCE_UNSUPPORTED_SHAPE"),  # object/scalar mix
        ('{"a":{}}', "PROFILE_INFERENCE_UNSUPPORTED_SHAPE"),  # empty nested object
        ('{"a":[1,2]}', "PROFILE_INFERENCE_UNSUPPORTED_SHAPE"),  # nested scalar array
        ("{not json", "PROFILE_INFERENCE_INVALID_SAMPLE"),
        ("", "PROFILE_INFERENCE_INVALID_SAMPLE"),
    ],
)
def test_json_structural_errors(sample, code):
    with pytest.raises(BuilderValidationError) as e:
        pi.infer_profile_from_sample_json(sample)
    assert e.value.error_code == code


def test_json_nested_array_of_objects():
    r = pi.infer_profile_from_sample_json('{"orders":[{"id":1},{"id":2}]}')
    assert "Root/orders[]/id" in r["field_index_by_path"]
    assert r["field_index_by_path"]["Root/orders"]["kind"] == "array"


def test_json_does_not_echo_values():
    r = pi.infer_profile_from_sample_json('{"note":"SENSITIVE-VALUE-123"}')
    assert "SENSITIVE-VALUE-123" not in _json.dumps(r)


def test_json_secret_named_key_withheld():
    r = pi.infer_profile_from_sample_json('{"id":1,"api_key":"x"}')
    paths = set(r["field_index_by_path"])
    assert "Root/api_key" not in paths and "Root/id" in paths
    assert any(i["code"] == pi.PROFILE_INFERENCE_SECRET_FIELD_WITHHELD for i in r["issues"])


def test_json_max_nodes_limit():
    with pytest.raises(BuilderValidationError) as e:
        pi.infer_profile_from_sample_json('{"a":1,"b":2,"c":3}', options={"max_fields": 2})
    assert e.value.error_code == pi.PROFILE_INFERENCE_INPUT_TOO_LARGE


def test_json_stable_output():
    a = '{"id":1,"name":"x"}'
    assert pi.infer_profile_from_sample_json(a) == pi.infer_profile_from_sample_json(a)


# ---------------------------------------------------------------------------
# Task 4 — XSD inference
# ---------------------------------------------------------------------------

_XSD_OK = """<xs:schema xmlns:xs="http://www.w3.org/2001/XMLSchema">
  <xs:element name="Order"><xs:complexType><xs:sequence>
    <xs:element name="Id" type="xs:string"/>
    <xs:element name="Qty" type="xs:int"/>
    <xs:element name="When" type="xs:dateTime"/>
    <xs:element name="Active" type="xs:boolean"/>
    <xs:element name="Note" type="xs:string" minOccurs="0"/>
    <xs:element name="Line" maxOccurs="unbounded"><xs:complexType><xs:sequence>
        <xs:element name="Sku" type="xs:string"/>
    </xs:sequence></xs:complexType></xs:element>
  </xs:sequence></xs:complexType></xs:element>
</xs:schema>"""


def _xsd_wrap(fragment):
    return (
        '<xs:schema xmlns:xs="http://www.w3.org/2001/XMLSchema">'
        '<xs:element name="R"><xs:complexType><xs:sequence>'
        f"{fragment}"
        "</xs:sequence></xs:complexType></xs:element></xs:schema>"
    )


def test_xsd_happy_subset():
    r = pi.infer_profile_from_xsd(_XSD_OK)
    assert r["generation_mode"] == "profile_from_xsd"
    assert r["component_type"] == "profile.xml"
    assert r["profile_type"] == "xml.generated"
    idx = set(r["field_index_by_path"])
    assert "Order/Id" in idx
    assert "Order/Line[]/Sku" in idx  # Line is unbounded → [] for descendants
    by = {f["path"]: f for f in r["fields"]}
    assert by["Order/Id"]["data_type"] == "character"
    assert by["Order/Qty"]["data_type"] == "number"
    assert by["Order/When"]["data_type"] == "datetime"
    assert by["Order/Active"]["data_type"] == "boolean"
    assert by["Order/Note"]["required"] is False  # minOccurs=0
    assert r["ready_for_builder"] is True


def test_xsd_inline_simple_type_restriction():
    xsd = _xsd_wrap(
        '<xs:element name="Code"><xs:simpleType>'
        '<xs:restriction base="xs:string"/></xs:simpleType></xs:element>'
    )
    r = pi.infer_profile_from_xsd(xsd)
    assert {f["path"]: f for f in r["fields"]}["R/Code"]["data_type"] == "character"


def test_xsd_non_string_artifact_invalid_input():
    with pytest.raises(BuilderValidationError) as e:
        pi.infer_profile_from_xsd({"not": "a string"})
    assert e.value.error_code == pi.PROFILE_INFERENCE_INVALID_INPUT


def test_xsd_invalid_xml():
    with pytest.raises(BuilderValidationError) as e:
        pi.infer_profile_from_xsd("<xs:schema")
    assert e.value.error_code == pi.PROFILE_INFERENCE_INVALID_SAMPLE


def test_xsd_doctype_rejected():
    with pytest.raises(BuilderValidationError) as e:
        pi.infer_profile_from_xsd(
            '<!DOCTYPE x><xs:schema xmlns:xs="http://www.w3.org/2001/XMLSchema"/>'
        )
    assert e.value.error_code == pi.PROFILE_INFERENCE_INVALID_SAMPLE


@pytest.mark.parametrize(
    "frag",
    [
        '<xs:choice><xs:element name="a" type="xs:string"/></xs:choice>',
        '<xs:any/>',
        '<xs:element name="a" type="xs:string"/></xs:sequence>'
        '<xs:attribute name="attr" type="xs:string"/><xs:sequence>',
        '<xs:element name="b" type="xs:base64Binary"/>',  # binary leaf unsupported
        '<xs:element ref="Other"/>',  # element ref / substitution
    ],
)
def test_xsd_unsupported_constructs(frag):
    with pytest.raises(BuilderValidationError) as e:
        pi.infer_profile_from_xsd(_xsd_wrap(frag))
    assert e.value.error_code == pi.PROFILE_INFERENCE_UNSUPPORTED_SHAPE


def test_xsd_mixed_content_rejected():
    xsd = (
        '<xs:schema xmlns:xs="http://www.w3.org/2001/XMLSchema">'
        '<xs:element name="R"><xs:complexType mixed="true"><xs:sequence>'
        '<xs:element name="a" type="xs:string"/>'
        "</xs:sequence></xs:complexType></xs:element></xs:schema>"
    )
    with pytest.raises(BuilderValidationError) as e:
        pi.infer_profile_from_xsd(xsd)
    assert e.value.error_code == pi.PROFILE_INFERENCE_UNSUPPORTED_SHAPE


def test_xsd_target_namespace_rejected():
    xsd = (
        '<xs:schema xmlns:xs="http://www.w3.org/2001/XMLSchema" targetNamespace="urn:x">'
        '<xs:element name="R" type="xs:string"/></xs:schema>'
    )
    with pytest.raises(BuilderValidationError) as e:
        pi.infer_profile_from_xsd(xsd)
    assert e.value.error_code == pi.PROFILE_INFERENCE_UNSUPPORTED_NAMESPACE


def test_xsd_foreign_type_prefix_rejected_as_namespace():
    xsd = (
        '<xs:schema xmlns:xs="http://www.w3.org/2001/XMLSchema" xmlns:foo="urn:foo">'
        '<xs:element name="R" type="foo:Thing"/></xs:schema>'
    )
    with pytest.raises(BuilderValidationError) as e:
        pi.infer_profile_from_xsd(xsd)
    assert e.value.error_code == pi.PROFILE_INFERENCE_UNSUPPORTED_NAMESPACE


def test_xsd_import_rejected():
    xsd = (
        '<xs:schema xmlns:xs="http://www.w3.org/2001/XMLSchema">'
        '<xs:import namespace="urn:y" schemaLocation="y.xsd"/>'
        '<xs:element name="R" type="xs:string"/></xs:schema>'
    )
    with pytest.raises(BuilderValidationError) as e:
        pi.infer_profile_from_xsd(xsd)
    assert e.value.error_code == pi.PROFILE_INFERENCE_UNSUPPORTED_SHAPE


def test_xsd_recursive_type():
    xsd = """<xs:schema xmlns:xs="http://www.w3.org/2001/XMLSchema">
      <xs:element name="Node" type="NodeT"/>
      <xs:complexType name="NodeT"><xs:sequence>
        <xs:element name="Child" type="NodeT"/></xs:sequence></xs:complexType></xs:schema>"""
    with pytest.raises(BuilderValidationError) as e:
        pi.infer_profile_from_xsd(xsd)
    assert e.value.error_code == pi.PROFILE_INFERENCE_RECURSIVE_XML


def test_xsd_named_complex_type_reference():
    xsd = """<xs:schema xmlns:xs="http://www.w3.org/2001/XMLSchema">
      <xs:element name="Order" type="OrderT"/>
      <xs:complexType name="OrderT"><xs:sequence>
        <xs:element name="Id" type="xs:string"/></xs:sequence></xs:complexType></xs:schema>"""
    r = pi.infer_profile_from_xsd(xsd)
    assert "Order/Id" in r["field_index_by_path"]

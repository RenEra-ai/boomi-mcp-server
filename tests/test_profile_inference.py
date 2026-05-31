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

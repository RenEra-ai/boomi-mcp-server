"""Unit tests for DatabaseReadProfileBuilder (Issue #23).

Verifies the emitted XML matches the structure of a real exported Boomi
profile.db component (work-profile b39ffdd4 "[Intapp CDS] Get Current
DateTime" and 5fe35b85 "[Intapp CDS] Global SQL XML", fetched 2026-05-18)
and that field-level defaults / required-field validation behave correctly.

The builder must:
- Emit a deterministic key sequence (DBStatement=2, DBFields=3, DBParameters=4,
  output fields start at 5, parameters continue after).
- Preserve task-authored SQL verbatim (with XML escaping).
- Reject missing query / output_fields / unsupported types with structured
  BuilderValidationError envelopes.
"""

import xml.etree.ElementTree as ET

import pytest

from boomi_mcp.categories.components.builders.profile_builder import (
    DatabaseReadProfileBuilder,
    PROFILE_BUILDERS,
    get_profile_builder,
)
from boomi_mcp.categories.components.builders.connector_builder import (
    BuilderValidationError,
)


NS = {"bns": "http://api.platform.boomi.com/"}


def _minimal_config(**overrides):
    params = {
        "component_type": "profile.db",
        "profile_type": "database.read",
        "component_name": "Test Read Profile",
        "query": "select 1 as one",
        "output_fields": [{"name": "one"}],
    }
    params.update(overrides)
    return params


def _build_minimal(**overrides):
    return DatabaseReadProfileBuilder().build(**_minimal_config(**overrides))


# ----------------------------------------------------------------------------
# Registry
# ----------------------------------------------------------------------------

def test_profile_db_registered_in_profile_builders():
    assert ("profile.db", "database.read") in PROFILE_BUILDERS
    builder = get_profile_builder("profile.db", "database.read")
    assert builder is not None
    assert builder.__class__ is DatabaseReadProfileBuilder


def test_get_profile_builder_unknown_returns_none():
    assert get_profile_builder("profile.db", "database.write") is None
    assert get_profile_builder("profile.json", "database.read") is None
    assert get_profile_builder("", "") is None


def test_get_profile_builder_is_case_insensitive():
    assert get_profile_builder("Profile.DB", "Database.Read") is not None


# ----------------------------------------------------------------------------
# Golden XML shape
# ----------------------------------------------------------------------------

def test_minimum_required_fields_produce_valid_component_xml():
    xml = _build_minimal()
    root = ET.fromstring(xml)
    assert root.tag.endswith("Component")
    assert root.attrib["type"] == "profile.db"
    assert root.attrib["name"] == "Test Read Profile"
    assert "subType" not in root.attrib  # profile.db has no subType


def test_database_profile_envelope_matches_reference_shape():
    xml = _build_minimal()
    root = ET.fromstring(xml)
    obj = root.find("bns:object", NS)
    db_profile = obj.find("DatabaseProfile")
    assert db_profile is not None
    assert db_profile.attrib["strict"] == "true"
    assert db_profile.attrib["version"] == "2"
    gen_info = db_profile.find("ProfileProperties/DatabaseGeneralInfo")
    assert gen_info.attrib["executionType"] == "dbread"


def test_dbstatement_keys_match_reference_allocation():
    xml = _build_minimal(
        output_fields=[
            {"name": "col_a"},
            {"name": "col_b"},
            {"name": "col_c"},
        ],
        parameters=[
            {"name": "p1"},
            {"name": "p2"},
        ],
    )
    root = ET.fromstring(xml)
    obj = root.find("bns:object", NS)
    statement = obj.find("DatabaseProfile/DataElements/DBStatement")
    assert statement.attrib["key"] == "2"
    assert statement.attrib["name"] == "Statement"
    assert statement.attrib["statementType"] == "select"
    assert statement.attrib["storedProcedure"] == ""
    assert statement.attrib["tableName"] == ""

    fields = statement.find("DBFields")
    assert fields.attrib["key"] == "3"
    assert fields.attrib["type"] == "result_set"

    params = statement.find("DBParameters")
    assert params.attrib["key"] == "4"

    output_elements = fields.findall("DatabaseElement")
    assert [e.attrib["key"] for e in output_elements] == ["5", "6", "7"]
    assert [e.attrib["name"] for e in output_elements] == ["col_a", "col_b", "col_c"]

    param_elements = params.findall("DBParameter")
    assert [p.attrib["key"] for p in param_elements] == ["8", "9"]
    assert [p.attrib["name"] for p in param_elements] == ["p1", "p2"]


def test_empty_parameters_emits_self_closing_dbparameters():
    xml = _build_minimal()
    root = ET.fromstring(xml)
    params = root.find("bns:object/DatabaseProfile/DataElements/DBStatement/DBParameters", NS)
    assert params is not None
    assert list(params) == []  # no children
    assert params.attrib["key"] == "4"


def test_output_field_carries_data_format_character():
    xml = _build_minimal(output_fields=[{"name": "currentDate", "data_type": "character"}])
    root = ET.fromstring(xml)
    element = root.find(
        "bns:object/DatabaseProfile/DataElements/DBStatement/DBFields/DatabaseElement",
        NS,
    )
    assert element.attrib["dataType"] == "character"
    assert element.attrib["isMappable"] == "true"
    assert element.attrib["isNode"] == "true"
    assert element.find("DataFormat/ProfileCharacterFormat") is not None


def test_parameter_omits_data_type_attribute_but_has_data_format():
    xml = _build_minimal(parameters=[{"name": "Statement"}])
    root = ET.fromstring(xml)
    param = root.find(
        "bns:object/DatabaseProfile/DataElements/DBStatement/DBParameters/DBParameter",
        NS,
    )
    # Per live CDS reference 5fe35b85, DBParameter intentionally omits the
    # dataType attribute even though DatabaseElement carries it.
    assert "dataType" not in param.attrib
    assert param.attrib["isMappable"] == "false"
    assert param.attrib["isNode"] == "true"
    assert param.find("DataFormat/ProfileCharacterFormat") is not None


def test_output_field_mandatory_and_unique_flags_emit_strings():
    xml = _build_minimal(
        output_fields=[{
            "name": "id",
            "mandatory": True,
            "enforce_unique": True,
        }],
    )
    root = ET.fromstring(xml)
    element = root.find(
        "bns:object/DatabaseProfile/DataElements/DBStatement/DBFields/DatabaseElement",
        NS,
    )
    assert element.attrib["mandatory"] == "true"
    assert element.attrib["enforceUnique"] == "true"


def test_parameter_mappable_flag_emits_string():
    xml = _build_minimal(parameters=[{"name": "p", "mappable": True}])
    root = ET.fromstring(xml)
    param = root.find(
        "bns:object/DatabaseProfile/DataElements/DBStatement/DBParameters/DBParameter",
        NS,
    )
    assert param.attrib["isMappable"] == "true"


def test_sql_text_is_preserved_verbatim():
    sql = "SELECT \n    GetDate() [currentDate]"
    xml = _build_minimal(query=sql)
    root = ET.fromstring(xml)
    sql_elem = root.find(
        "bns:object/DatabaseProfile/DataElements/DBStatement/sql",
        NS,
    )
    assert sql_elem.text == sql


def test_sql_text_is_xml_escaped():
    # Query containing characters that require XML escaping must survive
    # the round-trip via ET (ET decodes the escapes back to the originals).
    sql = "select * from t where a < 5 and b = 'x' and c & d"
    xml = _build_minimal(query=sql)
    # Raw XML must escape & < ' (etree won't accept un-escaped & in text).
    assert "&amp;" in xml
    assert "&lt;" in xml
    root = ET.fromstring(xml)
    sql_elem = root.find(
        "bns:object/DatabaseProfile/DataElements/DBStatement/sql",
        NS,
    )
    assert sql_elem.text == sql


def test_component_name_and_folder_xml_escape():
    xml = _build_minimal(component_name="A & B <C>", folder_name="Home/<x>")
    root = ET.fromstring(xml)
    assert root.attrib["name"] == "A & B <C>"
    assert root.attrib["folderName"] == "Home/<x>"


def test_folder_name_defaults_to_home():
    xml = _build_minimal()
    root = ET.fromstring(xml)
    assert root.attrib["folderName"] == "Home"


# ----------------------------------------------------------------------------
# Structured validation errors
# ----------------------------------------------------------------------------

@pytest.mark.parametrize("missing_value", [None, "", "   "])
def test_missing_query_raises_missing_db_query(missing_value):
    with pytest.raises(BuilderValidationError) as excinfo:
        DatabaseReadProfileBuilder().build(**_minimal_config(query=missing_value))
    assert excinfo.value.error_code == "MISSING_DB_QUERY"
    assert excinfo.value.field == "query"


@pytest.mark.parametrize("bad_output", [None, [], "not a list"])
def test_missing_output_fields_raises_missing_db_output_fields(bad_output):
    cfg = _minimal_config()
    cfg["output_fields"] = bad_output
    with pytest.raises(BuilderValidationError) as excinfo:
        DatabaseReadProfileBuilder().build(**cfg)
    assert excinfo.value.error_code == "MISSING_DB_OUTPUT_FIELDS"
    assert excinfo.value.field == "output_fields"


def test_unsupported_profile_type_raises_structured_error():
    with pytest.raises(BuilderValidationError) as excinfo:
        DatabaseReadProfileBuilder().build(**_minimal_config(profile_type="database.write"))
    assert excinfo.value.error_code == "UNSUPPORTED_DB_PROFILE_MODE"
    assert excinfo.value.field == "profile_type"


def test_missing_profile_type_raises_structured_error():
    with pytest.raises(BuilderValidationError) as excinfo:
        DatabaseReadProfileBuilder().build(**_minimal_config(profile_type=""))
    assert excinfo.value.error_code == "UNSUPPORTED_DB_PROFILE_MODE"


@pytest.mark.parametrize("missing_value", [None, "", "   "])
def test_missing_component_name_raises_structured_error(missing_value):
    with pytest.raises(BuilderValidationError) as excinfo:
        DatabaseReadProfileBuilder().build(**_minimal_config(component_name=missing_value))
    assert excinfo.value.error_code == "DATABASE_OPERATION_VALIDATION_FAILED"
    assert excinfo.value.field == "component_name"


def test_unsupported_field_data_type_raises_structured_error():
    # "blob" is not in _SUPPORTED_FIELD_TYPES — only character/number/datetime are.
    with pytest.raises(BuilderValidationError) as excinfo:
        DatabaseReadProfileBuilder().build(
            **_minimal_config(output_fields=[{"name": "payload", "data_type": "blob"}])
        )
    assert excinfo.value.error_code == "UNSUPPORTED_DB_PROFILE_FIELD_TYPE"
    assert excinfo.value.field == "output_fields[0].data_type"


def test_unsupported_parameter_data_type_raises_structured_error():
    # "blob" is not in _SUPPORTED_FIELD_TYPES — only character/number/datetime are.
    cfg = _minimal_config()
    cfg["parameters"] = [{"name": "p", "data_type": "blob"}]
    with pytest.raises(BuilderValidationError) as excinfo:
        DatabaseReadProfileBuilder().build(**cfg)
    assert excinfo.value.error_code == "UNSUPPORTED_DB_PROFILE_FIELD_TYPE"
    assert excinfo.value.field == "parameters[0].data_type"


@pytest.mark.parametrize("data_type,format_tag", [
    ("character", "ProfileCharacterFormat"),
    ("number", "ProfileNumberFormat"),
    ("datetime", "ProfileDateFormat"),
])
def test_extended_field_data_types_accepted(data_type, format_tag):
    """Issue #23 follow-up: number and datetime are accepted alongside character.

    Verified against live SP profile 439fd4ae which uses all three types.
    """
    xml = DatabaseReadProfileBuilder().build(
        **_minimal_config(
            output_fields=[{"name": "col", "data_type": data_type}]
        )
    )
    assert f'dataType="{data_type}"' in xml
    assert f'<{format_tag}/>' in xml


@pytest.mark.parametrize("data_type,format_tag", [
    ("character", "ProfileCharacterFormat"),
    ("number", "ProfileNumberFormat"),
    ("datetime", "ProfileDateFormat"),
])
def test_extended_parameter_data_types_accepted(data_type, format_tag):
    """Extended types are also accepted on parameters."""
    cfg = _minimal_config()
    cfg["parameters"] = [{"name": "p", "data_type": data_type}]
    xml = DatabaseReadProfileBuilder().build(**cfg)
    # Select builder omits the dataType attribute on DBParameter (per the
    # live CDS Select reference), but the DataFormat child reflects the type.
    assert f'<{format_tag}/>' in xml


def test_output_field_missing_name_raises_structured_error():
    with pytest.raises(BuilderValidationError) as excinfo:
        DatabaseReadProfileBuilder().build(
            **_minimal_config(output_fields=[{"name": ""}])
        )
    assert excinfo.value.error_code == "DATABASE_OPERATION_VALIDATION_FAILED"
    assert excinfo.value.field == "output_fields[0].name"


def test_parameter_missing_name_raises_structured_error():
    cfg = _minimal_config()
    cfg["parameters"] = [{"name": ""}]
    with pytest.raises(BuilderValidationError) as excinfo:
        DatabaseReadProfileBuilder().build(**cfg)
    assert excinfo.value.error_code == "DATABASE_OPERATION_VALIDATION_FAILED"
    assert excinfo.value.field == "parameters[0].name"


def test_output_field_must_be_object():
    with pytest.raises(BuilderValidationError) as excinfo:
        DatabaseReadProfileBuilder().build(
            **_minimal_config(output_fields=["just_a_string"])
        )
    assert excinfo.value.error_code == "DATABASE_OPERATION_VALIDATION_FAILED"
    assert excinfo.value.field == "output_fields[0]"


def test_output_field_mandatory_must_be_bool():
    with pytest.raises(BuilderValidationError) as excinfo:
        DatabaseReadProfileBuilder().build(
            **_minimal_config(output_fields=[{"name": "x", "mandatory": "yes"}])
        )
    assert excinfo.value.error_code == "DATABASE_OPERATION_VALIDATION_FAILED"


def test_parameters_must_be_a_list_when_present():
    cfg = _minimal_config()
    cfg["parameters"] = {"not": "a list"}
    with pytest.raises(BuilderValidationError) as excinfo:
        DatabaseReadProfileBuilder().build(**cfg)
    assert excinfo.value.error_code == "DATABASE_OPERATION_VALIDATION_FAILED"
    assert excinfo.value.field == "parameters"


# ----------------------------------------------------------------------------
# Secret scanning
# ----------------------------------------------------------------------------

def test_plaintext_secret_in_config_is_rejected():
    cfg = _minimal_config()
    cfg["password"] = "supersecret"
    with pytest.raises(BuilderValidationError) as excinfo:
        DatabaseReadProfileBuilder().build(**cfg)
    assert excinfo.value.error_code == "PLAINTEXT_SECRET_REJECTED"
    assert excinfo.value.field == "password"


def test_plaintext_secret_in_nested_dict_is_rejected():
    cfg = _minimal_config()
    cfg["output_fields"] = [{"name": "x", "metadata": {"token": "abc"}}]
    err = DatabaseReadProfileBuilder.scan_forbidden_secret_fields(cfg)
    assert err is not None
    assert err.error_code == "PLAINTEXT_SECRET_REJECTED"
    assert "token" in err.field


def test_redact_in_place_walks_lists_and_dicts():
    cfg = {
        "output_fields": [{"name": "x", "secret": "hidden"}],
        "password": "plain",
    }
    DatabaseReadProfileBuilder.redact_forbidden_secret_fields_in_place(cfg)
    assert cfg["password"] == "[REDACTED]"
    assert cfg["output_fields"][0]["secret"] == "[REDACTED]"


# ----------------------------------------------------------------------------
# validate_config separate from build()
# ----------------------------------------------------------------------------

def test_validate_config_returns_none_for_minimal_valid_config():
    assert DatabaseReadProfileBuilder.validate_config(_minimal_config()) is None


def test_validate_config_returns_first_error_without_raising():
    err = DatabaseReadProfileBuilder.validate_config(_minimal_config(query=""))
    assert err is not None
    assert err.error_code == "MISSING_DB_QUERY"


# ============================================================================
# Issue #45 — Component XML update preservation
# ============================================================================


def test_database_read_profile_preservation_policy_attached():
    policy = DatabaseReadProfileBuilder.PRESERVATION_POLICY
    assert policy.component_type == "profile.db"
    assert any(
        op.path == "bns:object/DatabaseProfile/DataElements"
        for op in policy.owned_paths
    )


def test_database_read_profile_update_preserves_profile_properties_and_siblings():
    """The builder owns only `DataElements`; `ProfileProperties` and any
    unknown DatabaseProfile siblings must survive a structured update."""
    from boomi_mcp.categories.components.component_update_preservation import (
        merge_for_update,
    )

    desired = _build_minimal(component_name="renamed")
    current = _build_minimal(component_name="original")
    # Inject a future-Boomi sibling inside DatabaseProfile
    current = current.replace(
        "</DataElements>",
        '</DataElements><FutureSection retained="yes"/>',
    )

    merged = merge_for_update(
        current, desired, DatabaseReadProfileBuilder.PRESERVATION_POLICY
    )
    root = ET.fromstring(merged)
    profile = root.find("bns:object/DatabaseProfile", NS)
    assert profile is not None
    assert profile.find("ProfileProperties") is not None
    assert profile.find("FutureSection") is not None
    assert profile.find("FutureSection").attrib["retained"] == "yes"
    assert root.attrib["name"] == "renamed"

"""Unit tests for DatabaseWriteProfileBuilder (Issue #32, M5.6).

Structural ElementTree assertions (matching the component-level DB read/get
builder test convention) against the shapes of real renera profile.db write
exports (2026-06-27): Standard Insert/Update/Delete, Dynamic Insert/Update/
Delete, Stored Procedure.

The builder must:
- Emit the write envelope (DatabaseGeneralInfo executionType="dbwrite").
- Map the spec enum 'storedprocedurewrite' to XML statementType="spwrite";
  the other four statement types map verbatim.
- Emit DBFields / DBConditions per statement type (standard / dynamicinsert /
  spwrite have fields only; dynamicupdate has both; dynamicdelete has
  conditions only).
- Omit the dataType attribute for character columns (number/datetime carry it).
- Keep all SQL / table / procedure / column values task-authored — generate
  nothing the caller did not supply.

Neutral placeholder names only — never the live HBM_CLIENT / CLIENT_UNO /
dbo.usp_InsertClient values.
"""

import xml.etree.ElementTree as ET

import pytest

from boomi_mcp.categories.components.builders.profile_builder import (
    DatabaseWriteProfileBuilder,
    PROFILE_BUILDERS,
    get_profile_builder,
)
from boomi_mcp.categories.components.builders.connector_builder import (
    BuilderValidationError,
)


NS = {"bns": "http://api.platform.boomi.com/"}


def _cfg(**overrides):
    params = {
        "component_type": "profile.db",
        "profile_type": "database.write",
        "component_name": "Write Target",
    }
    params.update(overrides)
    return params


def _standard_insert(**overrides):
    base = dict(
        statement_type="standardinsertupdatedelete",
        sql="INSERT INTO WRITE_TARGET (COL_A, COL_B, COL_C) VALUES (?, ?, ?)",
        fields=[
            {"name": "COL_A", "data_type": "number"},
            {"name": "COL_B"},
            {"name": "COL_C", "data_type": "datetime"},
        ],
    )
    base.update(overrides)
    return _cfg(**base)


def _statement(xml: str):
    root = ET.fromstring(xml)
    return root.find(
        "bns:object/{*}DatabaseProfile/DataElements/DBStatement", NS
    )


def _general_info(xml: str):
    root = ET.fromstring(xml)
    return root.find(
        "bns:object/{*}DatabaseProfile/ProfileProperties/DatabaseGeneralInfo", NS
    )


# ----------------------------------------------------------------------------
# Registry
# ----------------------------------------------------------------------------

def test_registered_in_profile_builders():
    assert ("profile.db", "database.write") in PROFILE_BUILDERS
    builder = get_profile_builder("profile.db", "database.write")
    assert builder is not None
    assert builder.__class__ is DatabaseWriteProfileBuilder


def test_factory_case_insensitive():
    builder = get_profile_builder("PROFILE.DB", "DATABASE.WRITE")
    assert isinstance(builder, DatabaseWriteProfileBuilder)


# ----------------------------------------------------------------------------
# Write envelope
# ----------------------------------------------------------------------------

def test_envelope_is_write_profile():
    xml = DatabaseWriteProfileBuilder().build(**_standard_insert())
    root = ET.fromstring(xml)
    assert root.attrib["type"] == "profile.db"
    assert root.attrib["name"] == "Write Target"
    gi = _general_info(xml)
    assert gi.get("executionType") == "dbwrite"


# ----------------------------------------------------------------------------
# Statement-type → XML statementType
# ----------------------------------------------------------------------------

def test_standard_statement_type_verbatim():
    stmt = _statement(DatabaseWriteProfileBuilder().build(**_standard_insert()))
    assert stmt.get("statementType") == "standardinsertupdatedelete"
    assert stmt.get("tableName") == ""
    assert stmt.get("storedProcedure") == ""


def test_dynamic_statement_types_verbatim():
    for st in ("dynamicinsert", "dynamicupdate", "dynamicdelete"):
        cfg = _cfg(statement_type=st, table_name="WRITE_TARGET")
        if st in ("dynamicinsert", "dynamicupdate"):
            cfg["fields"] = [{"name": "COL_A"}]
        if st in ("dynamicupdate", "dynamicdelete"):
            cfg["conditions"] = [{"name": "KEY_COL", "data_type": "number"}]
        stmt = _statement(DatabaseWriteProfileBuilder().build(**cfg))
        assert stmt.get("statementType") == st
        assert stmt.get("tableName") == "WRITE_TARGET"


def test_stored_procedure_write_maps_to_spwrite():
    cfg = _cfg(
        statement_type="storedprocedurewrite",
        stored_procedure="dbo.usp_Sample",
        sql="{ call dbo.usp_Sample(?, ?) }",
        fields=[{"name": "COL_A", "data_type": "number"}, {"name": "COL_B"}],
    )
    stmt = _statement(DatabaseWriteProfileBuilder().build(**cfg))
    # The spec enum is 'storedprocedurewrite' but the live XML value is 'spwrite'.
    assert stmt.get("statementType") == "spwrite"
    assert stmt.get("storedProcedure") == "dbo.usp_Sample"
    assert stmt.findtext("sql") == "{ call dbo.usp_Sample(?, ?) }"


# ----------------------------------------------------------------------------
# Per-statement-type fields / conditions / sql presence
# ----------------------------------------------------------------------------

def test_standard_has_fields_no_conditions_explicit_sql():
    stmt = _statement(DatabaseWriteProfileBuilder().build(**_standard_insert()))
    assert stmt.find("DBFields") is not None
    assert stmt.find("DBConditions") is None
    assert stmt.findtext("sql") == (
        "INSERT INTO WRITE_TARGET (COL_A, COL_B, COL_C) VALUES (?, ?, ?)"
    )


def test_dynamic_insert_empty_sql_with_fields():
    cfg = _cfg(
        statement_type="dynamicinsert",
        table_name="WRITE_TARGET",
        fields=[{"name": "COL_A"}, {"name": "COL_B"}],
    )
    stmt = _statement(DatabaseWriteProfileBuilder().build(**cfg))
    assert stmt.find("DBFields") is not None
    assert stmt.find("DBConditions") is None
    # Dynamic types emit an empty <sql/>.
    assert (stmt.findtext("sql") or "") == ""


def test_dynamic_update_has_fields_and_conditions():
    cfg = _cfg(
        statement_type="dynamicupdate",
        table_name="WRITE_TARGET",
        fields=[{"name": "COL_A"}, {"name": "COL_B"}],
        conditions=[{"name": "KEY_COL", "data_type": "number"}],
    )
    stmt = _statement(DatabaseWriteProfileBuilder().build(**cfg))
    assert stmt.find("DBFields") is not None
    assert stmt.find("DBConditions") is not None
    assert (stmt.findtext("sql") or "") == ""


def test_dynamic_delete_conditions_only_no_fields():
    cfg = _cfg(
        statement_type="dynamicdelete",
        table_name="WRITE_TARGET",
        conditions=[{"name": "KEY_COL", "data_type": "number"}],
    )
    stmt = _statement(DatabaseWriteProfileBuilder().build(**cfg))
    assert stmt.find("DBFields") is None
    assert stmt.find("DBConditions") is not None
    assert (stmt.findtext("sql") or "") == ""


# ----------------------------------------------------------------------------
# dataType attribute / DataFormat
# ----------------------------------------------------------------------------

def test_character_omits_datatype_attribute():
    stmt = _statement(DatabaseWriteProfileBuilder().build(**_standard_insert()))
    els = stmt.findall("DBFields/DatabaseElement")
    by_name = {e.get("name"): e for e in els}
    # character column: no dataType attribute, ProfileCharacterFormat child.
    assert "dataType" not in by_name["COL_B"].attrib
    assert by_name["COL_B"].find("DataFormat/ProfileCharacterFormat") is not None
    # number / datetime carry the attribute + matching format.
    assert by_name["COL_A"].get("dataType") == "number"
    assert by_name["COL_A"].find("DataFormat/ProfileNumberFormat") is not None
    assert by_name["COL_C"].get("dataType") == "datetime"
    assert by_name["COL_C"].find("DataFormat/ProfileDateFormat") is not None


def test_field_attributes_match_live_shape():
    stmt = _statement(DatabaseWriteProfileBuilder().build(**_standard_insert()))
    el = stmt.find("DBFields/DatabaseElement")
    assert el.get("isMappable") == "true"
    assert el.get("isNode") == "true"
    assert el.get("enforceUnique") == "false"
    assert el.get("mandatory") == "false"


def test_mandatory_and_enforce_unique_emit_true():
    cfg = _standard_insert(
        fields=[{"name": "COL_A", "mandatory": True, "enforce_unique": True}],
        sql="INSERT INTO WRITE_TARGET (COL_A) VALUES (?)",
    )
    stmt = _statement(DatabaseWriteProfileBuilder().build(**cfg))
    el = stmt.find("DBFields/DatabaseElement")
    assert el.get("mandatory") == "true"
    assert el.get("enforceUnique") == "true"


# ----------------------------------------------------------------------------
# Deterministic key allocation
# ----------------------------------------------------------------------------

def test_key_allocation_is_deterministic():
    cfg = _cfg(
        statement_type="dynamicupdate",
        table_name="WRITE_TARGET",
        fields=[{"name": "COL_A"}, {"name": "COL_B"}],
        conditions=[{"name": "KEY_COL", "data_type": "number"}],
    )
    stmt = _statement(DatabaseWriteProfileBuilder().build(**cfg))
    assert stmt.get("key") == "2"
    assert stmt.find("DBFields").get("key") == "3"
    assert stmt.find("DBConditions").get("key") == "4"
    field_keys = [e.get("key") for e in stmt.findall("DBFields/DatabaseElement")]
    cond_keys = [e.get("key") for e in stmt.findall("DBConditions/DBCondition")]
    # Fields start at 5, conditions continue after fields.
    assert field_keys == ["5", "6"]
    assert cond_keys == ["7"]


# ----------------------------------------------------------------------------
# build_field_index (map-target consumption)
# ----------------------------------------------------------------------------

def test_field_index_covers_fields_and_conditions():
    cfg = _cfg(
        statement_type="dynamicupdate",
        table_name="WRITE_TARGET",
        fields=[{"name": "COL_A"}],
        conditions=[{"name": "KEY_COL", "data_type": "number"}],
    )
    index = DatabaseWriteProfileBuilder.build_field_index(cfg)
    assert index["COL_A"]["name_path"] == "Statement/Fields/COL_A"
    assert index["COL_A"]["key_path"] == "*[@key='2']/*[@key='3']/*[@key='5']"
    assert index["KEY_COL"]["name_path"] == "Statement/Conditions/KEY_COL"
    assert index["KEY_COL"]["key_path"] == "*[@key='2']/*[@key='4']/*[@key='6']"


def test_field_index_keys_match_build():
    cfg = _cfg(
        statement_type="dynamicupdate",
        table_name="WRITE_TARGET",
        fields=[{"name": "COL_A"}, {"name": "COL_B"}],
        conditions=[{"name": "KEY_COL", "data_type": "number"}],
    )
    stmt = _statement(DatabaseWriteProfileBuilder().build(**cfg))
    index = DatabaseWriteProfileBuilder.build_field_index(cfg)
    built = {}
    for el in stmt.findall("DBFields/DatabaseElement"):
        built[el.get("name")] = int(el.get("key"))
    for el in stmt.findall("DBConditions/DBCondition"):
        built[el.get("name")] = int(el.get("key"))
    for name, meta in index.items():
        assert meta["key"] == built[name]


# ----------------------------------------------------------------------------
# Negative cases
# ----------------------------------------------------------------------------

def test_unsupported_statement_type_rejected():
    with pytest.raises(BuilderValidationError) as exc:
        DatabaseWriteProfileBuilder().build(
            **_cfg(statement_type="upsert", sql="x", fields=[{"name": "A"}])
        )
    assert exc.value.error_code == "UNSUPPORTED_DB_STATEMENT_TYPE"
    assert exc.value.field == "statement_type"


def test_missing_statement_type_rejected():
    with pytest.raises(BuilderValidationError) as exc:
        DatabaseWriteProfileBuilder().build(**_cfg())
    assert exc.value.error_code == "UNSUPPORTED_DB_STATEMENT_TYPE"


def test_missing_sql_for_standard_rejected():
    with pytest.raises(BuilderValidationError) as exc:
        DatabaseWriteProfileBuilder().build(
            **_cfg(statement_type="standardinsertupdatedelete",
                   fields=[{"name": "A"}])
        )
    assert exc.value.error_code == "MISSING_DB_SQL"


def test_missing_sql_for_stored_procedure_rejected():
    with pytest.raises(BuilderValidationError) as exc:
        DatabaseWriteProfileBuilder().build(
            **_cfg(statement_type="storedprocedurewrite",
                   stored_procedure="dbo.usp_Sample",
                   fields=[{"name": "A"}])
        )
    assert exc.value.error_code == "MISSING_DB_SQL"


def test_missing_table_for_dynamic_rejected():
    with pytest.raises(BuilderValidationError) as exc:
        DatabaseWriteProfileBuilder().build(
            **_cfg(statement_type="dynamicinsert", fields=[{"name": "A"}])
        )
    assert exc.value.error_code == "MISSING_DB_TABLE_NAME"


def test_missing_stored_procedure_rejected():
    with pytest.raises(BuilderValidationError) as exc:
        DatabaseWriteProfileBuilder().build(
            **_cfg(statement_type="storedprocedurewrite",
                   sql="{ call x(?) }", fields=[{"name": "A"}])
        )
    assert exc.value.error_code == "MISSING_DB_STORED_PROCEDURE"


def test_missing_fields_rejected():
    with pytest.raises(BuilderValidationError) as exc:
        DatabaseWriteProfileBuilder().build(
            **_cfg(statement_type="dynamicinsert", table_name="T", fields=[])
        )
    assert exc.value.error_code == "MISSING_DB_FIELDS"


def test_missing_conditions_for_dynamic_update_rejected():
    with pytest.raises(BuilderValidationError) as exc:
        DatabaseWriteProfileBuilder().build(
            **_cfg(statement_type="dynamicupdate", table_name="T",
                   fields=[{"name": "A"}])
        )
    assert exc.value.error_code == "MISSING_DB_CONDITIONS"


def test_fields_rejected_for_dynamic_delete():
    with pytest.raises(BuilderValidationError) as exc:
        DatabaseWriteProfileBuilder().build(
            **_cfg(statement_type="dynamicdelete", table_name="T",
                   fields=[{"name": "A"}],
                   conditions=[{"name": "K", "data_type": "number"}])
        )
    assert exc.value.error_code == "DATABASE_OPERATION_VALIDATION_FAILED"
    assert exc.value.field == "fields"


def test_conditions_rejected_for_standard():
    with pytest.raises(BuilderValidationError) as exc:
        DatabaseWriteProfileBuilder().build(
            **_standard_insert(conditions=[{"name": "K"}])
        )
    assert exc.value.error_code == "DATABASE_OPERATION_VALIDATION_FAILED"
    assert exc.value.field == "conditions"


def test_sql_rejected_for_dynamic():
    with pytest.raises(BuilderValidationError) as exc:
        DatabaseWriteProfileBuilder().build(
            **_cfg(statement_type="dynamicinsert", table_name="T",
                   fields=[{"name": "A"}], sql="INSERT ...")
        )
    assert exc.value.error_code == "DATABASE_OPERATION_VALIDATION_FAILED"
    assert exc.value.field == "sql"


def test_table_name_rejected_for_standard():
    with pytest.raises(BuilderValidationError) as exc:
        DatabaseWriteProfileBuilder().build(**_standard_insert(table_name="T"))
    assert exc.value.error_code == "DATABASE_OPERATION_VALIDATION_FAILED"
    assert exc.value.field == "table_name"


def test_table_name_rejected_for_stored_procedure():
    with pytest.raises(BuilderValidationError) as exc:
        DatabaseWriteProfileBuilder().build(
            **_cfg(statement_type="storedprocedurewrite",
                   stored_procedure="dbo.usp_Sample",
                   sql="{ call dbo.usp_Sample(?) }",
                   table_name="T",
                   fields=[{"name": "A"}])
        )
    assert exc.value.error_code == "DATABASE_OPERATION_VALIDATION_FAILED"
    assert exc.value.field == "table_name"


def test_stored_procedure_rejected_for_dynamic_insert():
    with pytest.raises(BuilderValidationError) as exc:
        DatabaseWriteProfileBuilder().build(
            **_cfg(statement_type="dynamicinsert", table_name="T",
                   stored_procedure="dbo.usp_Sample",
                   fields=[{"name": "A"}])
        )
    assert exc.value.error_code == "DATABASE_OPERATION_VALIDATION_FAILED"
    assert exc.value.field == "stored_procedure"


def test_stored_procedure_rejected_for_standard():
    with pytest.raises(BuilderValidationError) as exc:
        DatabaseWriteProfileBuilder().build(
            **_standard_insert(stored_procedure="dbo.usp_Sample")
        )
    assert exc.value.error_code == "DATABASE_OPERATION_VALIDATION_FAILED"
    assert exc.value.field == "stored_procedure"


def test_empty_irrelevant_table_name_is_allowed():
    # An explicit empty string is harmless (it is the default for standard).
    xml = DatabaseWriteProfileBuilder().build(**_standard_insert(table_name=""))
    stmt = _statement(xml)
    assert stmt.get("tableName") == ""


def test_invalid_data_type_rejected():
    with pytest.raises(BuilderValidationError) as exc:
        DatabaseWriteProfileBuilder().build(
            **_standard_insert(
                fields=[{"name": "A", "data_type": "blob"}],
                sql="INSERT INTO T (A) VALUES (?)",
            )
        )
    assert exc.value.error_code == "UNSUPPORTED_DB_PROFILE_FIELD_TYPE"


def test_unsupported_profile_type_rejected():
    with pytest.raises(BuilderValidationError) as exc:
        DatabaseWriteProfileBuilder().build(
            **_standard_insert(profile_type="database.read")
        )
    assert exc.value.error_code == "UNSUPPORTED_DB_PROFILE_MODE"


def test_plaintext_secret_rejected():
    with pytest.raises(BuilderValidationError) as exc:
        DatabaseWriteProfileBuilder().build(**_standard_insert(password="leak"))
    assert exc.value.error_code == "PLAINTEXT_SECRET_REJECTED"


# ----------------------------------------------------------------------------
# Anti-template — the builder generates nothing the caller did not supply
# ----------------------------------------------------------------------------

def test_builder_emits_no_unsupplied_sql_for_dynamic():
    cfg = _cfg(
        statement_type="dynamicinsert",
        table_name="WRITE_TARGET",
        fields=[{"name": "COL_A"}],
    )
    xml = DatabaseWriteProfileBuilder().build(**cfg)
    # No canned SQL keywords appear anywhere in dynamic-insert output.
    lowered = xml.lower()
    for kw in ("insert into", "select ", "update ", "delete "):
        assert kw not in lowered


def test_builder_preserves_caller_sql_verbatim():
    sql = "UPDATE WRITE_TARGET SET COL_A = ? WHERE KEY_COL = ?"
    cfg = _standard_insert(
        sql=sql,
        fields=[{"name": "COL_A"}, {"name": "KEY_COL", "data_type": "number"}],
    )
    stmt = _statement(DatabaseWriteProfileBuilder().build(**cfg))
    assert stmt.findtext("sql") == sql

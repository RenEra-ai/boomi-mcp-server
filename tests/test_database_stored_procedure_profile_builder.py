"""Unit tests for DatabaseStoredProcedureReadProfileBuilder.

M2.3 follow-up to Issue #23. Verifies the emitted XML matches the structure
of a real exported Boomi Stored Procedure Read profile (reneraai-5RO3DD
component 439fd4ae-7990-4a5b-9453-fbb9d7fe458e "Test SP Profile", fetched
2026-05-18) and that SP-specific validation (procedure_name required,
parameter mode in/out/in_out/return) behaves correctly.

The builder must:
- Emit a deterministic key sequence (DBStatement=2, DBFields=3,
  DBParameters=4, output fields start at 5, parameters continue after).
- Emit statementType="spread" and storedProcedure="<procedure_name>".
- Emit <sql/> as a self-closing element (no SQL text — procedure dispatch
  uses the storedProcedure attribute).
- Emit dataType and mode attributes on <DBParameter> (differs from Select
  which omits both).
- Reject missing procedure_name (MISSING_DB_PROCEDURE_NAME), unsupported
  parameter mode (INVALID_DB_PARAMETER_MODE), and unsupported data types.

Anti-marker policy: no test config copies values from the live reference
(no "Expert.dbo", no "usp_GetMatterWIPSummary", no "MATTER_CODE", etc.).
The lone snapshot test uses neutral placeholder values.
"""

import re
import xml.etree.ElementTree as ET

import pytest

from boomi_mcp.categories.components.builders.profile_builder import (
    DatabaseStoredProcedureReadProfileBuilder,
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
        "profile_type": "database.stored_procedure_read",
        "component_name": "Test SP Profile",
        "procedure_name": "schema.my_proc",
        "output_fields": [{"name": "col_a"}],
    }
    params.update(overrides)
    return params


def _build_minimal(**overrides):
    return DatabaseStoredProcedureReadProfileBuilder().build(
        **_minimal_config(**overrides)
    )


# ----------------------------------------------------------------------------
# Registry
# ----------------------------------------------------------------------------

class TestRegistry:

    def test_registered_under_stored_procedure_protocol(self):
        assert (
            ("profile.db", "database.stored_procedure_read") in PROFILE_BUILDERS
        )
        assert (
            PROFILE_BUILDERS[("profile.db", "database.stored_procedure_read")]
            is DatabaseStoredProcedureReadProfileBuilder
        )

    def test_factory_returns_instance(self):
        builder = get_profile_builder("profile.db", "database.stored_procedure_read")
        assert isinstance(builder, DatabaseStoredProcedureReadProfileBuilder)

    def test_factory_case_insensitive(self):
        builder = get_profile_builder("PROFILE.DB", "DATABASE.STORED_PROCEDURE_READ")
        assert isinstance(builder, DatabaseStoredProcedureReadProfileBuilder)

    def test_factory_returns_none_for_unknown(self):
        assert get_profile_builder("profile.db", "database.write") is None


# ----------------------------------------------------------------------------
# validate_config — common paths inherited from base
# ----------------------------------------------------------------------------

class TestCommonValidation:

    def test_validate_minimal_config_passes(self):
        err = DatabaseStoredProcedureReadProfileBuilder.validate_config(
            _minimal_config()
        )
        assert err is None

    @pytest.mark.parametrize("missing", [None, "", "   "])
    def test_missing_profile_type_rejected(self, missing):
        cfg = _minimal_config(profile_type=missing)
        err = DatabaseStoredProcedureReadProfileBuilder.validate_config(cfg)
        assert err is not None
        assert err.error_code == "UNSUPPORTED_DB_PROFILE_MODE"

    def test_wrong_profile_type_rejected(self):
        cfg = _minimal_config(profile_type="database.read")
        err = DatabaseStoredProcedureReadProfileBuilder.validate_config(cfg)
        assert err is not None
        assert err.error_code == "UNSUPPORTED_DB_PROFILE_MODE"

    @pytest.mark.parametrize("missing", [None, "", "   "])
    def test_missing_component_name_rejected(self, missing):
        cfg = _minimal_config(component_name=missing)
        err = DatabaseStoredProcedureReadProfileBuilder.validate_config(cfg)
        assert err is not None
        assert err.error_code == "DATABASE_OPERATION_VALIDATION_FAILED"
        assert err.field == "component_name"

    def test_missing_output_fields_rejected(self):
        cfg = _minimal_config()
        cfg.pop("output_fields")
        err = DatabaseStoredProcedureReadProfileBuilder.validate_config(cfg)
        assert err is not None
        assert err.error_code == "MISSING_DB_OUTPUT_FIELDS"

    def test_empty_output_fields_rejected(self):
        cfg = _minimal_config(output_fields=[])
        err = DatabaseStoredProcedureReadProfileBuilder.validate_config(cfg)
        assert err is not None
        assert err.error_code == "MISSING_DB_OUTPUT_FIELDS"

    def test_output_field_without_name_rejected(self):
        cfg = _minimal_config(output_fields=[{"name": ""}])
        err = DatabaseStoredProcedureReadProfileBuilder.validate_config(cfg)
        assert err is not None
        assert err.field == "output_fields[0].name"

    def test_unsupported_field_data_type_rejected(self):
        cfg = _minimal_config(output_fields=[{"name": "x", "data_type": "blob"}])
        err = DatabaseStoredProcedureReadProfileBuilder.validate_config(cfg)
        assert err is not None
        assert err.error_code == "UNSUPPORTED_DB_PROFILE_FIELD_TYPE"
        assert err.field == "output_fields[0].data_type"

    @pytest.mark.parametrize("data_type", ["character", "number", "datetime"])
    def test_extended_field_data_types_accepted(self, data_type):
        cfg = _minimal_config(
            output_fields=[{"name": "x", "data_type": data_type}]
        )
        err = DatabaseStoredProcedureReadProfileBuilder.validate_config(cfg)
        assert err is None


# ----------------------------------------------------------------------------
# validate_config — SP-specific procedure_name
# ----------------------------------------------------------------------------

class TestProcedureNameValidation:

    @pytest.mark.parametrize("missing", [None, "", "   "])
    def test_missing_procedure_name_rejected(self, missing):
        cfg = _minimal_config(procedure_name=missing)
        err = DatabaseStoredProcedureReadProfileBuilder.validate_config(cfg)
        assert err is not None
        assert err.error_code == "MISSING_DB_PROCEDURE_NAME"
        assert err.field == "procedure_name"

    def test_procedure_name_with_vendor_syntax_accepted(self):
        # The builder does not parse procedure names. SQL Server's `;N`
        # version suffix, Oracle package syntax, etc. all pass through.
        for proc in [
            "schema.proc;1",
            "package.proc",
            "db.proc",
            "schema.proc",
            "proc",
            "MIXED_Case.Proc",
        ]:
            cfg = _minimal_config(procedure_name=proc)
            err = DatabaseStoredProcedureReadProfileBuilder.validate_config(cfg)
            assert err is None, f"Procedure name {proc!r} should be accepted"

    def test_procedure_name_special_chars_escaped(self):
        cfg = _minimal_config(procedure_name='ns."A&B".proc')
        xml = DatabaseStoredProcedureReadProfileBuilder().build(**cfg)
        assert 'storedProcedure="ns.&quot;A&amp;B&quot;.proc"' in xml


# ----------------------------------------------------------------------------
# validate_config — parameter mode
# ----------------------------------------------------------------------------

class TestParameterModeValidation:

    @pytest.mark.parametrize("mode", ["in", "out", "in_out", "return"])
    def test_valid_modes_accepted(self, mode):
        cfg = _minimal_config()
        cfg["parameters"] = [{"name": "p", "mode": mode}]
        err = DatabaseStoredProcedureReadProfileBuilder.validate_config(cfg)
        assert err is None

    def test_default_mode_is_in_when_omitted(self):
        cfg = _minimal_config()
        cfg["parameters"] = [{"name": "p"}]  # no mode specified
        err = DatabaseStoredProcedureReadProfileBuilder.validate_config(cfg)
        assert err is None
        xml = DatabaseStoredProcedureReadProfileBuilder().build(**cfg)
        assert 'mode="in"' in xml

    def test_inout_with_underscore_is_valid(self):
        # Regression: Boomi's XML attribute value is "in_out" (with underscore),
        # NOT "inout". Verified against Boomi's reference doc for
        # Database (Legacy) profile parameters.
        cfg = _minimal_config()
        cfg["parameters"] = [{"name": "p", "mode": "in_out"}]
        err = DatabaseStoredProcedureReadProfileBuilder.validate_config(cfg)
        assert err is None

    def test_inout_without_underscore_is_rejected(self):
        # The old (incorrect) "inout" value must NOT be accepted.
        cfg = _minimal_config()
        cfg["parameters"] = [{"name": "p", "mode": "inout"}]
        err = DatabaseStoredProcedureReadProfileBuilder.validate_config(cfg)
        assert err is not None
        assert err.error_code == "INVALID_DB_PARAMETER_MODE"

    def test_unknown_mode_rejected(self):
        cfg = _minimal_config()
        cfg["parameters"] = [{"name": "p", "mode": "garbage"}]
        err = DatabaseStoredProcedureReadProfileBuilder.validate_config(cfg)
        assert err is not None
        assert err.error_code == "INVALID_DB_PARAMETER_MODE"
        assert err.field == "parameters[0].mode"

    def test_mode_validation_runs_per_parameter(self):
        cfg = _minimal_config()
        cfg["parameters"] = [
            {"name": "p1", "mode": "in"},
            {"name": "p2", "mode": "out"},
            {"name": "p3", "mode": "garbage"},
        ]
        err = DatabaseStoredProcedureReadProfileBuilder.validate_config(cfg)
        assert err is not None
        assert err.field == "parameters[2].mode"

    def test_single_return_parameter_accepted(self):
        cfg = _minimal_config()
        cfg["parameters"] = [
            {"name": "r", "mode": "return"},
            {"name": "p", "mode": "in"},
        ]
        err = DatabaseStoredProcedureReadProfileBuilder.validate_config(cfg)
        assert err is None

    def test_multiple_return_parameters_rejected(self):
        # Boomi reference doc: "Only one Return parameter can be defined per
        # statement."
        cfg = _minimal_config()
        cfg["parameters"] = [
            {"name": "r1", "mode": "return"},
            {"name": "p", "mode": "in"},
            {"name": "r2", "mode": "return"},
        ]
        err = DatabaseStoredProcedureReadProfileBuilder.validate_config(cfg)
        assert err is not None
        assert err.error_code == "MULTIPLE_DB_RETURN_PARAMETERS"
        # Field points at the SECOND return parameter (the one that violates).
        assert err.field == "parameters[2].mode"

    def test_return_parameter_xml_emission(self):
        cfg = _minimal_config()
        cfg["parameters"] = [{"name": "ret", "mode": "return"}]
        xml = DatabaseStoredProcedureReadProfileBuilder().build(**cfg)
        assert 'mode="return"' in xml


# ----------------------------------------------------------------------------
# XML emission — SP-specific shape
# ----------------------------------------------------------------------------

class TestXmlEmission:

    def test_statement_type_is_spread(self):
        xml = _build_minimal()
        assert 'statementType="spread"' in xml

    def test_stored_procedure_attribute_populated(self):
        xml = _build_minimal(procedure_name="my_schema.my_proc")
        assert 'storedProcedure="my_schema.my_proc"' in xml

    def test_sql_element_is_self_closing(self):
        xml = _build_minimal()
        # Self-closing form: <sql/> with no text content. The Select builder
        # emits <sql>...</sql> with the query text.
        assert "<sql/>" in xml
        assert "<sql>" not in xml or "</sql>" not in xml  # belt-and-suspenders

    def test_dbparameter_emits_data_type_attribute(self):
        cfg = _minimal_config()
        cfg["parameters"] = [{"name": "p", "data_type": "datetime"}]
        xml = DatabaseStoredProcedureReadProfileBuilder().build(**cfg)
        # SP DBParameter includes dataType (Select omits it).
        assert 'dataType="datetime"' in xml
        assert "<ProfileDateFormat/>" in xml

    def test_dbparameter_emits_mode_attribute(self):
        cfg = _minimal_config()
        cfg["parameters"] = [{"name": "p", "mode": "in_out"}]
        xml = DatabaseStoredProcedureReadProfileBuilder().build(**cfg)
        assert 'mode="in_out"' in xml

    def test_dbparameter_omits_dataformat_for_supported_types(self):
        # All three supported types must produce a matching DataFormat child.
        for dt, tag in [
            ("character", "ProfileCharacterFormat"),
            ("number", "ProfileNumberFormat"),
            ("datetime", "ProfileDateFormat"),
        ]:
            cfg = _minimal_config()
            cfg["parameters"] = [{"name": "p", "data_type": dt}]
            xml = DatabaseStoredProcedureReadProfileBuilder().build(**cfg)
            assert f"<{tag}/>" in xml

    def test_empty_parameters_emits_self_closing_dbparameters(self):
        xml = _build_minimal()  # no parameters
        # Match the Select-side behavior: self-closing DBParameters when empty.
        assert '<DBParameters isNode="true" key="4" name="Parameters"/>' in xml

    def test_xml_parses_as_well_formed(self):
        xml = _build_minimal()
        root = ET.fromstring(xml)
        # bns:Component is the root
        assert root.tag == "{http://api.platform.boomi.com/}Component"


# ----------------------------------------------------------------------------
# Key allocation
# ----------------------------------------------------------------------------

class TestKeyAllocation:

    def test_dense_key_allocation(self):
        cfg = _minimal_config(
            output_fields=[{"name": "a"}, {"name": "b"}, {"name": "c"}],
        )
        cfg["parameters"] = [{"name": "p1"}, {"name": "p2"}]
        xml = DatabaseStoredProcedureReadProfileBuilder().build(**cfg)

        # DBStatement=2, DBFields=3, DBParameters=4
        assert 'key="2"' in xml
        assert 'key="3" name="Fields"' in xml
        assert 'key="4" name="Parameters"' in xml
        # Outputs at 5,6,7 (caller order)
        assert re.search(r'key="5"[^>]*name="a"', xml)
        assert re.search(r'key="6"[^>]*name="b"', xml)
        assert re.search(r'key="7"[^>]*name="c"', xml)
        # Parameters continue at 8,9
        assert re.search(r'key="8"[^>]*name="p1"', xml)
        assert re.search(r'key="9"[^>]*name="p2"', xml)


# ----------------------------------------------------------------------------
# Secret scanning (inherited from base)
# ----------------------------------------------------------------------------

class TestSecretScan:

    @pytest.mark.parametrize("secret_key", [
        "password", "password_ref", "secret", "token",
        "access_token", "client_secret",
    ])
    def test_top_level_secret_rejected(self, secret_key):
        cfg = _minimal_config()
        cfg[secret_key] = "leaked-value"
        err = DatabaseStoredProcedureReadProfileBuilder.scan_forbidden_secret_fields(cfg)
        assert err is not None
        assert err.error_code == "PLAINTEXT_SECRET_REJECTED"
        assert err.field == secret_key

    def test_secret_inside_parameter_dict_detected(self):
        cfg = _minimal_config()
        cfg["parameters"] = [{"name": "p", "password": "x"}]
        err = DatabaseStoredProcedureReadProfileBuilder.scan_forbidden_secret_fields(cfg)
        assert err is not None
        assert err.error_code == "PLAINTEXT_SECRET_REJECTED"
        assert err.field == "parameters[0].password"

    def test_validate_config_runs_secret_scan_first(self):
        cfg = _minimal_config(procedure_name="")  # would normally fail on procedure_name
        cfg["password"] = "leak"
        err = DatabaseStoredProcedureReadProfileBuilder.validate_config(cfg)
        # Secret scan fires before procedure_name check.
        assert err.error_code == "PLAINTEXT_SECRET_REJECTED"

    def test_redact_in_place_walks_parameters(self):
        cfg = _minimal_config()
        cfg["parameters"] = [{"name": "p", "token": "leak"}]
        DatabaseStoredProcedureReadProfileBuilder.redact_forbidden_secret_fields_in_place(cfg)
        assert cfg["parameters"][0]["token"] == "[REDACTED]"


# ----------------------------------------------------------------------------
# build() — raises on invalid config
# ----------------------------------------------------------------------------

class TestBuildRaises:

    def test_missing_procedure_name_raises(self):
        with pytest.raises(BuilderValidationError) as excinfo:
            DatabaseStoredProcedureReadProfileBuilder().build(
                **_minimal_config(procedure_name="")
            )
        assert excinfo.value.error_code == "MISSING_DB_PROCEDURE_NAME"

    def test_bad_mode_raises(self):
        cfg = _minimal_config()
        cfg["parameters"] = [{"name": "p", "mode": "back"}]
        with pytest.raises(BuilderValidationError) as excinfo:
            DatabaseStoredProcedureReadProfileBuilder().build(**cfg)
        assert excinfo.value.error_code == "INVALID_DB_PARAMETER_MODE"


# ----------------------------------------------------------------------------
# Snapshot — derived from live reference, NOT a template
#
# This single block verifies the emitted XML structurally matches the live
# Boomi SP profile shape. All identifiers below are NEUTRAL placeholders
# chosen to be visibly different from the live reference's domain names
# (Expert.dbo.usp_GetMatterWIPSummary, MATTER_CODE, @ClientCode, etc.).
# ----------------------------------------------------------------------------

class TestSnapshotAgainstLiveShape:

    SNAPSHOT_CONFIG = {
        "component_type": "profile.db",
        "profile_type": "database.stored_procedure_read",
        "component_name": "Sample SP Profile",
        "procedure_name": "demo.demo_proc;1",
        "output_fields": [
            {"name": "field_a", "data_type": "character"},
            {"name": "field_b", "data_type": "number"},
            {"name": "field_c", "data_type": "datetime"},
        ],
        "parameters": [
            {"name": "param_a", "data_type": "character", "mode": "in"},
            {"name": "param_b", "data_type": "datetime", "mode": "in"},
            {"name": "param_c", "data_type": "number", "mode": "out"},
        ],
    }

    def test_snapshot_structure_matches_live(self):
        xml = DatabaseStoredProcedureReadProfileBuilder().build(**self.SNAPSHOT_CONFIG)
        # Top-level component shape
        assert 'type="profile.db"' in xml
        assert 'name="Sample SP Profile"' in xml
        # Profile properties
        assert '<DatabaseGeneralInfo executionType="dbread"/>' in xml
        # DBStatement attributes
        assert 'statementType="spread"' in xml
        assert 'storedProcedure="demo.demo_proc;1"' in xml
        assert 'tableName=""' in xml
        # Self-closing SQL
        assert "<sql/>" in xml
        # All three output field types rendered
        assert "<ProfileCharacterFormat/>" in xml
        assert "<ProfileNumberFormat/>" in xml
        assert "<ProfileDateFormat/>" in xml
        # All three parameter modes rendered
        assert 'mode="in"' in xml
        assert 'mode="out"' in xml


# ----------------------------------------------------------------------------
# manage_component.create dispatch — Bug #123 regression
#
# The standalone `manage_component.create` path must surface
# UNSUPPORTED_DB_PROFILE_MODE for unknown profile_type values on profile.db,
# matching the contract that integration_builder._build_plan uses. Without
# this, the dispatcher falls through to "xml is required" with no hint about
# the supported profile_type values.
# ----------------------------------------------------------------------------

class TestStandaloneDispatchForUnknownProfileType:

    def _call_create(self, config):
        from boomi_mcp.categories.components.manage_component import create_component
        from unittest.mock import MagicMock
        return create_component(MagicMock(), "test-profile", config)

    def test_unknown_profile_type_surfaces_structured_envelope(self):
        result = self._call_create({
            "component_type": "profile.db",
            "profile_type": "database.bogus",
            "component_name": "x",
            "procedure_name": "schema.proc",
            "output_fields": [{"name": "col_a"}],
        })
        assert result["_success"] is False
        assert result.get("error_code") == "UNSUPPORTED_DB_PROFILE_MODE"
        assert result.get("field") == "profile_type"

    def test_unknown_profile_type_hint_lists_both_supported_protocols(self):
        result = self._call_create({
            "component_type": "profile.db",
            "profile_type": "database.bogus",
            "component_name": "x",
        })
        assert "database.read" in result["hint"]
        assert "database.stored_procedure_read" in result["hint"]

    def test_missing_profile_type_also_surfaces_structured_envelope(self):
        # No profile_type at all → same dispatch path; the registry lookup
        # returns None and we must surface the structured envelope, not the
        # generic "xml is required" error.
        result = self._call_create({
            "component_type": "profile.db",
            "component_name": "x",
        })
        assert result["_success"] is False
        assert result.get("error_code") == "UNSUPPORTED_DB_PROFILE_MODE"

    def test_unknown_component_type_still_falls_through_to_xml_required(self):
        # For component_types with NO registered builder family at all (e.g.
        # process), we must keep the existing "xml is required" behavior.
        result = self._call_create({
            "component_type": "process",  # not in PROFILE_BUILDERS keys
            "name": "p",
        })
        assert result["_success"] is False
        assert "xml is required" in result["error"]


# ----------------------------------------------------------------------------
# Anti-marker policy — schema template / executable defaults must not embed
# live-reference identifiers
#
# Scope: this checks runtime config-shaped surfaces (the schema-template
# strings the LLM consumes, and the builder's executable defaults). Pure
# docstring/comment references to the live evidence ID are allowed as
# provenance documentation — they're never returned to a caller.
# ----------------------------------------------------------------------------

class TestAntiMarkerPolicy:
    """No value copied from the live reference may appear in template
    surfaces or executable defaults. The reference is for shape
    verification only."""

    FORBIDDEN_LIVE_IDENTIFIERS = [
        # Procedure name + components
        "usp_GetMatterWIPSummary",
        "Expert.dbo",
        # Result column names
        "MATTER_CODE", "MATTER_NAME", "CLIENT_CODE", "CLIENT_NAME",
        "OFFC_CODE", "RESP_EMPL_CODE", "RESP_EMPL_NAME",
        "MATTER_STATUS", "CURRENCY_CODE", "TIME_HOURS",
        "TIME_AMOUNT", "COST_AMOUNT", "WIP_TOTAL", "LAST_TIME_DATE",
        # Parameter names
        "@ClientCode", "@MatterCode", "@OfficeCode",
        "@DateFrom", "@DateTo",
    ]

    def test_schema_template_has_no_live_identifiers(self):
        """The SP schema template returned by get_schema_template must use
        placeholder tokens only — no values copied from the live reference."""
        from boomi_mcp.categories.meta_tools import (
            _COMPONENT_CREATE_PROFILE_DB_DATABASE_STORED_PROCEDURE_READ as TEMPLATE,
        )
        serialized = repr(TEMPLATE)
        for marker in self.FORBIDDEN_LIVE_IDENTIFIERS:
            assert marker not in serialized, (
                f"Live-reference identifier {marker!r} leaked into the SP "
                f"schema template. Templates must use placeholder tokens only."
            )

    def test_builder_executable_defaults_have_no_live_identifiers(self):
        """The builder's class-level defaults (excluding docstrings) must
        not contain values copied from the live reference."""
        cls = DatabaseStoredProcedureReadProfileBuilder
        # Only inspect attribute values, not the docstring.
        attribute_repr = repr({
            k: v for k, v in vars(cls).items()
            if not k.startswith("_") or k in ("__doc__",)
            if k != "__doc__"
        })
        for marker in self.FORBIDDEN_LIVE_IDENTIFIERS:
            assert marker not in attribute_repr, (
                f"Live-reference identifier {marker!r} appears in the "
                f"builder's executable class attributes."
            )


# ============================================================================
# Issue #45 — Component XML update preservation
# ============================================================================


def test_database_sp_read_profile_preservation_policy_attached():
    policy = DatabaseStoredProcedureReadProfileBuilder.PRESERVATION_POLICY
    assert policy.component_type == "profile.db"


def test_database_sp_read_profile_update_preserves_profile_properties():
    from boomi_mcp.categories.components.component_update_preservation import (
        merge_for_update,
    )
    import xml.etree.ElementTree as ET

    desired = _build_minimal(component_name="renamed")
    current = _build_minimal(component_name="original")
    current = current.replace(
        "</DataElements>",
        '</DataElements><FutureSection retained="yes"/>',
    )
    merged = merge_for_update(
        current, desired, DatabaseStoredProcedureReadProfileBuilder.PRESERVATION_POLICY
    )
    root = ET.fromstring(merged)
    profile = root.find("bns:object/DatabaseProfile", NS)
    assert profile.find("ProfileProperties") is not None
    assert profile.find("FutureSection") is not None

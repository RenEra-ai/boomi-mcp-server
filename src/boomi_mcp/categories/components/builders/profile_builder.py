"""
Profile component XML builders for Boomi.

Builds XML for profile.db (Database Legacy Read profile) components via the
Component API. Mirrors the conventions of connector_builder.py:
BuilderValidationError for structured errors, scan_forbidden_secret_fields +
validate_config classmethods for plan-time preflight, build() instance method
that raises.

Two statement-type variants are supported:

  1. Select statement  — `DatabaseReadProfileBuilder`
     profile_type="database.read", caller authors SQL in `query`.
     Reference XML: work-profile b39ffdd4 + 5fe35b85 (fetched 2026-05-18).

  2. Stored Procedure  — `DatabaseStoredProcedureReadProfileBuilder`
     profile_type="database.stored_procedure_read", caller supplies the
     procedure name in `procedure_name`. Parameters may carry IN/OUT/INOUT
     direction via `mode`.
     Reference XML: reneraai-5RO3DD profile, component 439fd4ae-7990-4a5b-9453-fbb9d7fe458e
     "Test SP Profile" (fetched 2026-05-18). Maps to statementType="spread"
     ("Stored Procedure READ").

Shared XML envelope:

    <bns:Component type="profile.db" name="..." folderName="...">
      <bns:encryptedValues/>
      <bns:description>...</bns:description>
      <bns:object>
        <DatabaseProfile xmlns="" strict="true" version="2">
          <ProfileProperties>
            <DatabaseGeneralInfo executionType="dbread"/>
          </ProfileProperties>
          <DataElements>
            <DBStatement isNode="true" key="2" name="Statement"
                         statementType="(select|spread)"
                         storedProcedure="(empty|proc-name)"
                         tableName="">
              <DBFields isNode="true" key="3" name="Fields" type="result_set">
                <DatabaseElement .../>   <!-- one per output column -->
              </DBFields>
              <DBParameters isNode="true" key="4" name="Parameters">
                <DBParameter .../>       <!-- one per input/output parameter -->
              </DBParameters>
              <sql>...</sql>             <!-- Select: SQL text. SP: self-closing -->
            </DBStatement>
          </DataElements>
        </DatabaseProfile>
      </bns:object>
    </bns:Component>

Key allocation is deterministic and identical across both builders:
  DBStatement=2, DBFields=3, DBParameters=4, then output DatabaseElement
  entries start at 5 in caller order, then DBParameter entries continue
  sequentially. Live UI-edited profiles have sparse keys (e.g. 28-42); Boomi
  accepts any unique integer keys, so dense allocation is preferred for
  reproducibility.

Supported data types (shared by output fields and parameters):
  character → <ProfileCharacterFormat/>
  number    → <ProfileNumberFormat/>      (verified against live SP reference)
  datetime  → <ProfileDateFormat/>        (verified against live SP reference;
                                           note: Boomi uses ProfileDateFormat
                                           for both date and datetime dataTypes)
"""

from typing import Any, Dict, List, Optional, Tuple

from .connector_builder import BuilderValidationError, _escape_xml


# Module-level type map — shared by both builders. Each entry maps a Boomi
# dataType attribute value to the matching <DataFormat> child element name.
# All three entries are verified against live profile.db XML.
_SUPPORTED_FIELD_TYPES: Dict[str, str] = {
    "character": "ProfileCharacterFormat",
    "number": "ProfileNumberFormat",
    "datetime": "ProfileDateFormat",
}

# Stored-procedure parameter direction modes. The live SP reference uses only
# "in", but "out" and "inout" are Boomi-documented values for DBParameter@mode
# (Boomi KB chunk 6664e27758_technology_connectors_085 on stored-procedure
# parameter handling).
_SUPPORTED_PARAMETER_MODES: Tuple[str, ...] = ("in", "out", "inout")
_DEFAULT_PARAMETER_MODE: str = "in"


class _DatabaseReadProfileBuilderBase:
    """Shared base for Select and Stored Procedure Read profile builders.

    Subclasses MUST set:
      - SUPPORTED_PROFILE_TYPES: tuple of profile_type strings this builder accepts
      - _STATEMENT_TYPE_LABEL: human-readable label for error messages
                               (e.g. "Select statement" or "Stored Procedure")

    Subclasses MAY override:
      - _PARAMETER_EMIT_DATA_TYPE / _PARAMETER_EMIT_MODE flags to control
        DBParameter attribute emission (live Select XML omits both; live SP
        XML includes both)
      - validate_config to add statement-specific checks beyond _validate_common
      - _build_db_statement_inner to emit the <DBStatement> body
    """

    SUPPORTED_PROFILE_TYPES: Tuple[str, ...] = ()
    _STATEMENT_TYPE_LABEL: str = ""

    DEFAULT_FIELD_DATA_TYPE = "character"
    DEFAULT_PARAMETER_DATA_TYPE = "character"

    # Toggles for parameter XML emission. Live Select XML has neither
    # dataType nor mode on <DBParameter>; live SP XML has both.
    _PARAMETER_EMIT_DATA_TYPE: bool = False
    _PARAMETER_EMIT_MODE: bool = False

    # Defensive consistency with DatabaseConnectorBuilder — read profiles
    # do not transport secrets, but mirror the scan so integration_builder
    # preflight has uniform behavior.
    FORBIDDEN_SECRET_FIELDS = (
        "password",
        "password_ref",
        "secret",
        "token",
        "access_token",
        "client_secret",
    )

    # ------------------------------------------------------------------
    # Secret scanning (shared)
    # ------------------------------------------------------------------

    @classmethod
    def scan_forbidden_secret_fields(
        cls, config: Any, _path_prefix: str = ""
    ) -> Optional[BuilderValidationError]:
        """Detect plaintext secret-shaped keys at any depth.

        Same traversal contract as DatabaseConnectorBuilder.scan_forbidden_secret_fields
        — walks dicts and lists at arbitrary nesting depth, returns the
        shallowest occurrence with a path-shaped `field`. Independent of
        builder invocation; safe to run on every read-profile config.
        """
        if isinstance(config, dict):
            for forbidden in cls.FORBIDDEN_SECRET_FIELDS:
                if forbidden in config:
                    field_path = f"{_path_prefix}{forbidden}"
                    return BuilderValidationError(
                        f"{field_path!r} cannot be supplied in database read "
                        "profile config — read profiles do not transport "
                        "secrets. Connection-level credentials belong on the "
                        "connector-settings component via credential_ref.",
                        error_code="PLAINTEXT_SECRET_REJECTED",
                        field=field_path,
                        hint=(
                            "Remove the secret-shaped field. Database "
                            "credentials are configured on the connector-"
                            "settings component, not the read profile."
                        ),
                    )
            for key, value in config.items():
                nested = cls.scan_forbidden_secret_fields(
                    value, _path_prefix=f"{_path_prefix}{key}."
                )
                if nested is not None:
                    return nested
        elif isinstance(config, list):
            base = _path_prefix[:-1] if _path_prefix.endswith(".") else _path_prefix
            for index, item in enumerate(config):
                nested = cls.scan_forbidden_secret_fields(
                    item, _path_prefix=f"{base}[{index}]."
                )
                if nested is not None:
                    return nested
        return None

    @classmethod
    def redact_forbidden_secret_fields_in_place(cls, config: Any) -> None:
        """Recursively replace any forbidden-keyed values with '[REDACTED]'.

        Mirrors DatabaseConnectorBuilder.redact_forbidden_secret_fields_in_place.
        Used by integration_builder when echoing the spec back in a plan
        response after a PLAINTEXT_SECRET_REJECTED error.
        """
        if isinstance(config, dict):
            for forbidden in cls.FORBIDDEN_SECRET_FIELDS:
                if forbidden in config:
                    config[forbidden] = "[REDACTED]"
            for value in config.values():
                cls.redact_forbidden_secret_fields_in_place(value)
        elif isinstance(config, list):
            for item in config:
                cls.redact_forbidden_secret_fields_in_place(item)

    # ------------------------------------------------------------------
    # Common validation (shared by both builders)
    # ------------------------------------------------------------------

    @classmethod
    def _validate_common(
        cls, config: Dict[str, Any]
    ) -> Optional[BuilderValidationError]:
        """Run validation steps common to all profile.db Read variants.

        Subclasses should call this first, then add statement-specific checks
        (e.g. Select requires `query`, SP requires `procedure_name`).
        """
        # 1) Plaintext secret-shaped keys.
        secret_err = cls.scan_forbidden_secret_fields(config)
        if secret_err is not None:
            return secret_err

        # 2) profile_type must be one this builder accepts.
        profile_type = config.get("profile_type") or ""
        if profile_type not in cls.SUPPORTED_PROFILE_TYPES:
            return BuilderValidationError(
                f"profile_type must be one of {cls.SUPPORTED_PROFILE_TYPES} "
                f"(got {profile_type!r})",
                error_code="UNSUPPORTED_DB_PROFILE_MODE",
                field="profile_type",
                hint=(
                    f"This builder handles {cls._STATEMENT_TYPE_LABEL}. "
                    "Database write profiles are tracked by issue #32."
                ),
            )

        # 3) component_name required.
        component_name = config.get("component_name")
        if not component_name or not str(component_name).strip():
            return BuilderValidationError(
                "component_name is required",
                error_code="DATABASE_OPERATION_VALIDATION_FAILED",
                field="component_name",
                hint="Provide a non-empty component_name string.",
            )

        # 4) output_fields required and non-empty.
        output_fields = config.get("output_fields")
        if not isinstance(output_fields, list) or len(output_fields) == 0:
            return BuilderValidationError(
                "output_fields is required and must be a non-empty list",
                error_code="MISSING_DB_OUTPUT_FIELDS",
                field="output_fields",
                hint=(
                    "Declare one entry per result-set column: "
                    "[{'name': 'col1'}, {'name': 'col2'}]. Each entry "
                    "defaults to data_type='character'."
                ),
            )
        for index, field in enumerate(output_fields):
            field_err = cls._validate_output_field(field, index)
            if field_err is not None:
                return field_err

        # 5) parameters optional; when present, must be a list.
        parameters = config.get("parameters", [])
        if not isinstance(parameters, list):
            return BuilderValidationError(
                "parameters must be a list when provided",
                error_code="DATABASE_OPERATION_VALIDATION_FAILED",
                field="parameters",
                hint=(
                    "Omit parameters or pass a list of parameter dicts."
                ),
            )
        for index, parameter in enumerate(parameters):
            param_err = cls._validate_parameter(parameter, index)
            if param_err is not None:
                return param_err

        return None

    @classmethod
    def _validate_output_field(
        cls, field: Any, index: int
    ) -> Optional[BuilderValidationError]:
        if not isinstance(field, dict):
            return BuilderValidationError(
                f"output_fields[{index}] must be an object",
                error_code="DATABASE_OPERATION_VALIDATION_FAILED",
                field=f"output_fields[{index}]",
                hint="Each output field is a JSON object with at least a 'name'.",
            )
        name = field.get("name")
        if not name or not str(name).strip():
            return BuilderValidationError(
                f"output_fields[{index}].name is required",
                error_code="DATABASE_OPERATION_VALIDATION_FAILED",
                field=f"output_fields[{index}].name",
                hint="Provide a non-empty column name (matches the SQL result-set column).",
            )
        data_type = field.get("data_type", cls.DEFAULT_FIELD_DATA_TYPE)
        if data_type not in _SUPPORTED_FIELD_TYPES:
            supported = ", ".join(sorted(_SUPPORTED_FIELD_TYPES.keys()))
            return BuilderValidationError(
                f"output_fields[{index}].data_type={data_type!r} is not supported",
                error_code="UNSUPPORTED_DB_PROFILE_FIELD_TYPE",
                field=f"output_fields[{index}].data_type",
                hint=f"Supported data types: {supported}.",
            )
        mandatory = field.get("mandatory", False)
        enforce_unique = field.get("enforce_unique", False)
        if not isinstance(mandatory, bool):
            return BuilderValidationError(
                f"output_fields[{index}].mandatory must be a bool",
                error_code="DATABASE_OPERATION_VALIDATION_FAILED",
                field=f"output_fields[{index}].mandatory",
                hint="Use true or false.",
            )
        if not isinstance(enforce_unique, bool):
            return BuilderValidationError(
                f"output_fields[{index}].enforce_unique must be a bool",
                error_code="DATABASE_OPERATION_VALIDATION_FAILED",
                field=f"output_fields[{index}].enforce_unique",
                hint="Use true or false.",
            )
        return None

    @classmethod
    def _validate_parameter(
        cls, parameter: Any, index: int
    ) -> Optional[BuilderValidationError]:
        if not isinstance(parameter, dict):
            return BuilderValidationError(
                f"parameters[{index}] must be an object",
                error_code="DATABASE_OPERATION_VALIDATION_FAILED",
                field=f"parameters[{index}]",
                hint="Each parameter is a JSON object with at least a 'name'.",
            )
        name = parameter.get("name")
        if not name or not str(name).strip():
            return BuilderValidationError(
                f"parameters[{index}].name is required",
                error_code="DATABASE_OPERATION_VALIDATION_FAILED",
                field=f"parameters[{index}].name",
                hint="Provide a non-empty parameter name.",
            )
        data_type = parameter.get("data_type", cls.DEFAULT_PARAMETER_DATA_TYPE)
        if data_type not in _SUPPORTED_FIELD_TYPES:
            supported = ", ".join(sorted(_SUPPORTED_FIELD_TYPES.keys()))
            return BuilderValidationError(
                f"parameters[{index}].data_type={data_type!r} is not supported",
                error_code="UNSUPPORTED_DB_PROFILE_FIELD_TYPE",
                field=f"parameters[{index}].data_type",
                hint=f"Supported data types: {supported}.",
            )
        mappable = parameter.get("mappable", False)
        if not isinstance(mappable, bool):
            return BuilderValidationError(
                f"parameters[{index}].mappable must be a bool",
                error_code="DATABASE_OPERATION_VALIDATION_FAILED",
                field=f"parameters[{index}].mappable",
                hint="Use true or false.",
            )
        # Mode is validated by subclasses that emit it (SP builder). Select
        # builder ignores `mode` entirely (does not emit it to XML).
        if cls._PARAMETER_EMIT_MODE:
            mode = parameter.get("mode", _DEFAULT_PARAMETER_MODE)
            if mode not in _SUPPORTED_PARAMETER_MODES:
                supported = ", ".join(_SUPPORTED_PARAMETER_MODES)
                return BuilderValidationError(
                    f"parameters[{index}].mode={mode!r} is not supported",
                    error_code="INVALID_DB_PARAMETER_MODE",
                    field=f"parameters[{index}].mode",
                    hint=(
                        f"Supported modes: {supported}. "
                        "Default is 'in'."
                    ),
                )
        return None

    # ------------------------------------------------------------------
    # XML rendering helpers (shared)
    # ------------------------------------------------------------------

    @classmethod
    def _render_output_field(
        cls, field: Dict[str, Any], key: int
    ) -> Tuple[str, int]:
        name = _escape_xml(str(field["name"]))
        data_type = field.get("data_type", cls.DEFAULT_FIELD_DATA_TYPE)
        mandatory = "true" if field.get("mandatory", False) else "false"
        enforce_unique = "true" if field.get("enforce_unique", False) else "false"
        format_tag = _SUPPORTED_FIELD_TYPES[data_type]
        xml = (
            f'                        <DatabaseElement dataType="{data_type}"'
            f' enforceUnique="{enforce_unique}" isMappable="true" isNode="true"'
            f' key="{key}" mandatory="{mandatory}" name="{name}">\n'
            f'                            <DataFormat><{format_tag}/></DataFormat>\n'
            f'                        </DatabaseElement>'
        )
        return xml, key + 1

    @classmethod
    def _render_parameter(
        cls, parameter: Dict[str, Any], key: int
    ) -> Tuple[str, int]:
        name = _escape_xml(str(parameter["name"]))
        data_type = parameter.get("data_type", cls.DEFAULT_PARAMETER_DATA_TYPE)
        mappable = "true" if parameter.get("mappable", False) else "false"
        format_tag = _SUPPORTED_FIELD_TYPES[data_type]

        # Live Select reference (5fe35b85 key=6) omits dataType and mode on
        # <DBParameter>. Live SP reference (439fd4ae) includes both. The
        # subclass flags control which attributes appear.
        attrs: List[str] = []
        if cls._PARAMETER_EMIT_DATA_TYPE:
            attrs.append(f'dataType="{data_type}"')
        attrs.append(f'isMappable="{mappable}"')
        attrs.append('isNode="true"')
        attrs.append(f'key="{key}"')
        if cls._PARAMETER_EMIT_MODE:
            mode = parameter.get("mode", _DEFAULT_PARAMETER_MODE)
            attrs.append(f'mode="{mode}"')
        attrs.append(f'name="{name}"')
        attr_str = " ".join(attrs)

        xml = (
            f'                        <DBParameter {attr_str}>\n'
            f'                            <DataFormat><{format_tag}/></DataFormat>\n'
            f'                        </DBParameter>'
        )
        return xml, key + 1

    # ------------------------------------------------------------------
    # Component-envelope assembly (shared)
    # ------------------------------------------------------------------

    def build(self, **params) -> str:
        error = self.validate_config(params)
        if error is not None:
            raise error

        component_name = params["component_name"]
        output_fields = params["output_fields"]
        parameters = params.get("parameters", [])
        folder_name = params.get("folder_name", "Home")
        description = params.get("description", "")

        safe_name = _escape_xml(component_name)
        safe_folder = _escape_xml(folder_name)
        safe_desc = _escape_xml(description)

        # Deterministic key allocation: DBStatement=2, DBFields=3,
        # DBParameters=4, then output fields start at 5 in caller order,
        # then parameters continue sequentially.
        next_key = 5
        output_xml_parts: List[str] = []
        for field in output_fields:
            field_xml, next_key = self._render_output_field(field, next_key)
            output_xml_parts.append(field_xml)
        parameter_xml_parts: List[str] = []
        for parameter in parameters:
            param_xml, next_key = self._render_parameter(parameter, next_key)
            parameter_xml_parts.append(param_xml)

        fields_block = "\n".join(output_xml_parts)
        if parameter_xml_parts:
            parameters_block = (
                '                    <DBParameters isNode="true" key="4" name="Parameters">\n'
                + "\n".join(parameter_xml_parts)
                + '\n                    </DBParameters>'
            )
        else:
            parameters_block = (
                '                    <DBParameters isNode="true" key="4" name="Parameters"/>'
            )

        db_statement_xml = self._build_db_statement_inner(
            params, fields_block, parameters_block
        )

        return (
            '<?xml version="1.0" encoding="UTF-8"?>\n'
            '<bns:Component xmlns:bns="http://api.platform.boomi.com/"\n'
            '               xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance"\n'
            '               type="profile.db"\n'
            f'               name="{safe_name}"\n'
            f'               folderName="{safe_folder}">\n'
            '    <bns:encryptedValues/>\n'
            f'    <bns:description>{safe_desc}</bns:description>\n'
            '    <bns:object>\n'
            '        <DatabaseProfile xmlns="" strict="true" version="2">\n'
            '            <ProfileProperties>\n'
            '                <DatabaseGeneralInfo executionType="dbread"/>\n'
            '            </ProfileProperties>\n'
            '            <DataElements>\n'
            f'{db_statement_xml}\n'
            '            </DataElements>\n'
            '        </DatabaseProfile>\n'
            '    </bns:object>\n'
            '</bns:Component>'
        )

    @classmethod
    def validate_config(
        cls, config: Dict[str, Any]
    ) -> Optional[BuilderValidationError]:
        """Subclasses override to add statement-specific checks."""
        return cls._validate_common(config)

    @classmethod
    def _build_db_statement_inner(
        cls, params: Dict[str, Any], fields_block: str, parameters_block: str
    ) -> str:
        """Subclasses MUST override to emit the <DBStatement> body."""
        raise NotImplementedError


class DatabaseReadProfileBuilder(_DatabaseReadProfileBuilderBase):
    """Builder for profile.db Select-statement Read components.

    Issue #23 — M2.3. Emits a Select-statement Read profile with caller-
    authored SQL and explicit output-field / parameter shape. The SQL text
    is preserved verbatim (after XML escaping) so the LLM is responsible for
    writing the query; the builder does not validate SQL syntax or generate
    queries.

    Config keys:
        profile_type:    required, must be "database.read"
        query:           required, non-empty SQL string
        output_fields:   required, non-empty list of {name, data_type?,
                         mandatory?, enforce_unique?}
        component_name:  required for top-level component naming
        parameters:      optional, list of {name, data_type?, mappable?}
        folder_name:     optional; defaults to "Home"
        description:     optional

    For Stored Procedure Read profiles, use
    DatabaseStoredProcedureReadProfileBuilder
    (profile_type="database.stored_procedure_read").
    """

    SUPPORTED_PROFILE_TYPES = ("database.read",)
    _STATEMENT_TYPE_LABEL = "Select-statement Read profiles (profile_type='database.read')"

    # Live Select reference (5fe35b85 key=6) emits neither dataType nor mode
    # on <DBParameter>. Preserve that shape.
    _PARAMETER_EMIT_DATA_TYPE = False
    _PARAMETER_EMIT_MODE = False

    @classmethod
    def validate_config(
        cls, config: Dict[str, Any]
    ) -> Optional[BuilderValidationError]:
        common_err = cls._validate_common(config)
        if common_err is not None:
            return common_err

        # Select-specific: query required and non-empty.
        query = config.get("query")
        if query is None or not str(query).strip():
            return BuilderValidationError(
                "query is required and must be a non-empty SQL string",
                error_code="MISSING_DB_QUERY",
                field="query",
                hint=(
                    "Provide a Select SQL statement. The builder stores the "
                    "query verbatim and does not validate SQL syntax."
                ),
            )
        return None

    @classmethod
    def _build_db_statement_inner(
        cls, params: Dict[str, Any], fields_block: str, parameters_block: str
    ) -> str:
        safe_sql = _escape_xml(str(params["query"]))
        return (
            '                <DBStatement isNode="true" key="2" name="Statement"'
            ' statementType="select" storedProcedure="" tableName="">\n'
            '                    <DBFields isNode="true" key="3" name="Fields" type="result_set">\n'
            f'{fields_block}\n'
            '                    </DBFields>\n'
            f'{parameters_block}\n'
            f'                    <sql>{safe_sql}</sql>\n'
            '                </DBStatement>'
        )


class DatabaseStoredProcedureReadProfileBuilder(_DatabaseReadProfileBuilderBase):
    """Builder for profile.db Stored Procedure Read components.

    M2.3 follow-up to Issue #23. Emits a Stored Procedure Read profile that
    invokes a database stored procedure by fully-qualified name. The
    procedure name is preserved verbatim (after XML escaping) so the LLM is
    responsible for any vendor-specific syntax (SQL Server `schema.proc;1`,
    Oracle `package.proc`, MySQL `db.proc`, PostgreSQL `schema.proc`, etc.);
    the builder does not parse, validate, or normalize procedure names.

    Config keys:
        profile_type:    required, must be "database.stored_procedure_read"
        procedure_name:  required, non-empty string. Stored verbatim. Examples:
                         SQL Server "MyDB.dbo.usp_GetData;1"
                         Oracle     "MY_PKG.get_data"
                         MySQL      "mydb.get_data"
                         PostgreSQL "public.get_data"
        output_fields:   required, non-empty list of {name, data_type?,
                         mandatory?, enforce_unique?} describing the
                         procedure's result-set shape
        parameters:      optional, list of {name, data_type?, mappable?, mode?}
                         where mode ∈ {"in", "out", "inout"} (default "in")
        component_name:  required for top-level component naming
        folder_name:     optional; defaults to "Home"
        description:     optional

    Reference XML: live profile 439fd4ae-7990-4a5b-9453-fbb9d7fe458e in the
    reneraai-5RO3DD test profile (procedure
    Expert.dbo.usp_GetMatterWIPSummary;1, 14 result columns, 5 IN params).
    The reference is used only for shape verification; no procedure-name,
    column-name, or parameter-name values are baked into the builder.

    Pure action (no result set) stored procedures are NOT supported in v1 —
    output_fields is required. Write profiles (Stored Procedure Write, INSERT/
    UPDATE/DELETE) are tracked separately by issue #32.
    """

    SUPPORTED_PROFILE_TYPES = ("database.stored_procedure_read",)
    _STATEMENT_TYPE_LABEL = (
        "Stored Procedure Read profiles "
        "(profile_type='database.stored_procedure_read')"
    )

    # Live SP reference (439fd4ae) emits both dataType and mode on <DBParameter>.
    _PARAMETER_EMIT_DATA_TYPE = True
    _PARAMETER_EMIT_MODE = True

    @classmethod
    def validate_config(
        cls, config: Dict[str, Any]
    ) -> Optional[BuilderValidationError]:
        common_err = cls._validate_common(config)
        if common_err is not None:
            return common_err

        # SP-specific: procedure_name required and non-empty.
        procedure_name = config.get("procedure_name")
        if procedure_name is None or not str(procedure_name).strip():
            return BuilderValidationError(
                "procedure_name is required and must be a non-empty string",
                error_code="MISSING_DB_PROCEDURE_NAME",
                field="procedure_name",
                hint=(
                    "Provide the fully-qualified stored-procedure name as your "
                    "database vendor expects it. The builder stores the value "
                    "verbatim and does not parse or normalize it."
                ),
            )
        return None

    @classmethod
    def _build_db_statement_inner(
        cls, params: Dict[str, Any], fields_block: str, parameters_block: str
    ) -> str:
        safe_proc = _escape_xml(str(params["procedure_name"]))
        return (
            '                <DBStatement isNode="true" key="2" name="Statement"'
            f' statementType="spread" storedProcedure="{safe_proc}" tableName="">\n'
            '                    <DBFields isNode="true" key="3" name="Fields" type="result_set">\n'
            f'{fields_block}\n'
            '                    </DBFields>\n'
            f'{parameters_block}\n'
            '                    <sql/>\n'
            '                </DBStatement>'
        )


# ============================================================================
# Registry
# ============================================================================

# Keyed by (component_type, profile_type). Mirrors the pattern of
# CONNECTOR_ACTION_BUILDERS in connector_builder.py.
PROFILE_BUILDERS: Dict[Tuple[str, str], type] = {
    ("profile.db", "database.read"): DatabaseReadProfileBuilder,
    ("profile.db", "database.stored_procedure_read"):
        DatabaseStoredProcedureReadProfileBuilder,
}


def get_profile_builder(component_type: str, profile_type: str):
    """Get a profile builder instance for (component_type, profile_type), or None."""
    if not component_type or not profile_type:
        return None
    key = (component_type.lower(), profile_type.lower())
    builder_class = PROFILE_BUILDERS.get(key)
    if builder_class:
        return builder_class()
    return None

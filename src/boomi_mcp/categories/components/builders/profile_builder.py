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
     Reference XML: legacy-ref-acct (decommissioned) profile, component 439fd4ae-7990-4a5b-9453-fbb9d7fe458e
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

from ._preservation_policy import OwnedPath, PreservationPolicy
from .connector_builder import BuilderValidationError, _escape_xml


# Module-level type map — shared by both builders. Each entry maps a Boomi
# dataType attribute value to the matching <DataFormat> child element name.
# All three entries are verified against live profile.db XML.
_SUPPORTED_FIELD_TYPES: Dict[str, str] = {
    "character": "ProfileCharacterFormat",
    "number": "ProfileNumberFormat",
    "datetime": "ProfileDateFormat",
}

# Stored-procedure parameter direction modes. Authoritative source: Boomi
# Database (Legacy) profile reference page "Database profile's Data Elements
# tab → Parameters", which documents exactly four DBParameter@mode values:
#   in       — input parameter
#   out      — output parameter
#   in_out   — input/output parameter (Boomi uses underscore, not "inout")
#   return   — procedure return value (at most one per statement; should be
#              first in the list per Boomi UI guidance but Boomi does not
#              hard-enforce position)
_SUPPORTED_PARAMETER_MODES: Tuple[str, ...] = ("in", "out", "in_out", "return")
_DEFAULT_PARAMETER_MODE: str = "in"
_AT_MOST_ONE_PARAMETER_MODE: str = "return"


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
                    "Database write profiles use profile_type='database.write' "
                    "(DatabaseWriteProfileBuilder)."
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
    # Field index (issue #26: consumed by DirectMapBuilder)
    # ------------------------------------------------------------------

    @classmethod
    def build_field_index(
        cls, config: Dict[str, Any]
    ) -> Dict[str, Dict[str, Any]]:
        """Return ``{output_field_name: {key, key_path, name_path, ...}}``.

        Keys are dense integers starting at 5, matching the allocation in
        ``build()`` (DBStatement=2, DBFields=3, DBParameters=4, then output
        fields). The direct map builder (#26) uses this index to render
        ``<Mapping fromKey/toKey fromKeyPath/toKeyPath fromNamePath/toNamePath/>``
        attributes that reference DB profile fields.

        Caller is responsible for validating the config first via
        ``validate_config`` — this method assumes a well-formed
        ``output_fields`` list.
        """
        index: Dict[str, Dict[str, Any]] = {}
        next_key = 5
        for field in config.get("output_fields") or []:
            if not isinstance(field, dict):
                continue
            name_raw = field.get("name")
            if not name_raw or not str(name_raw).strip():
                continue
            name = str(name_raw).strip()
            data_type = field.get("data_type", cls.DEFAULT_FIELD_DATA_TYPE)
            index[name] = {
                "path": name,
                "name": name,
                "key": next_key,
                "key_path": f"*[@key='2']/*[@key='3']/*[@key='{next_key}']",
                "name_path": f"Statement/Fields/{name}",
                "data_type": data_type,
                "kind": "simple",
                "required": bool(field.get("mandatory", False)),
                "mappable": True,
            }
            next_key += 1
        return index

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
                         where mode is one of:
                           "in"      — input parameter (default)
                           "out"     — output parameter
                           "in_out"  — input/output parameter
                           "return"  — procedure return value
                                       (at most one per statement; Boomi UI
                                       guidance is to place it first in the
                                       list but the builder does not reorder)
        component_name:  required for top-level component naming
        folder_name:     optional; defaults to "Home"
        description:     optional

    Reference XML: live profile 439fd4ae-7990-4a5b-9453-fbb9d7fe458e in the
    legacy-ref-acct (decommissioned) test profile (procedure
    Expert.dbo.usp_GetMatterWIPSummary;1, 14 result columns, 5 IN params).
    The reference is used only for shape verification; no procedure-name,
    column-name, or parameter-name values are baked into the builder.

    Pure action (no result set) stored procedures are NOT supported in v1 —
    output_fields is required. Write profiles (Stored Procedure Write, INSERT/
    UPDATE/DELETE) use ``DatabaseWriteProfileBuilder``
    (profile_type="database.write").
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

        # SP-specific: at most one "return" parameter per statement. Boomi's
        # reference doc for Database (Legacy) profile parameters states
        # "Only one Return parameter can be defined per statement."
        parameters = config.get("parameters") or []
        return_indices = [
            i for i, p in enumerate(parameters)
            if isinstance(p, dict)
            and p.get("mode") == _AT_MOST_ONE_PARAMETER_MODE
        ]
        if len(return_indices) > 1:
            return BuilderValidationError(
                f"At most one parameter with mode='return' is allowed per "
                f"statement (found {len(return_indices)} at indices "
                f"{return_indices})",
                error_code="MULTIPLE_DB_RETURN_PARAMETERS",
                field=f"parameters[{return_indices[1]}].mode",
                hint=(
                    "Boomi stored-procedure Read profiles accept at most one "
                    "return-direction parameter per statement. Combine the "
                    "extras into output parameters (mode='out') or split into "
                    "separate profiles if the procedure exposes multiple "
                    "logical returns."
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


class DatabaseWriteProfileBuilder:
    """Builder for profile.db Database (Legacy) Write profile components.

    Issue #32 — M5.6. Emits a write DatabaseProfile (``executionType="dbwrite"``)
    that a database Send operation (connector-action ``operation_mode="send"``)
    references via ``<WriteProfile profileId="..."/>``. Five statement-type
    variants are supported; all SQL, table names, stored-procedure names,
    fields, and conditions are caller-authored — the builder never generates
    SQL or product-specific write templates.

    Config keys:
        profile_type:    required, must be "database.write".
        statement_type:  required, one of standardinsertupdatedelete,
                         dynamicinsert, dynamicupdate, dynamicdelete,
                         storedprocedurewrite.
        component_name:  required for top-level component naming.
        sql:             required for standardinsertupdatedelete and
                         storedprocedurewrite (caller-authored, stored
                         verbatim). Must be omitted/blank for dynamic* types
                         (Boomi generates the SQL from table + fields/conditions
                         at runtime — the builder emits an empty <sql/>).
        table_name:      required for the dynamic* types; maps to
                         DBStatement@tableName.
        stored_procedure: required for storedprocedurewrite; maps to
                         DBStatement@storedProcedure.
        fields:          list of {name, data_type?, mandatory?, enforce_unique?}
                         emitted as DBFields/DatabaseElement. Required for every
                         type except dynamicdelete (which has no DBFields).
        conditions:      list of {name, data_type?} emitted as
                         DBConditions/DBCondition (WHERE keys). Required for
                         dynamicupdate and dynamicdelete; rejected otherwise.
        folder_name:     optional; defaults to "Home".
        description:     optional.

    Live reference shapes (renera profile.db exports, 2026-06-27): Standard
    Insert/Update/Delete (b7ad0684 / e9f4560b / edbfd71b), Dynamic Insert
    (0c2a973c), Dynamic Update (402fbfd2), Dynamic Delete (9daeadde), Stored
    Procedure (dba70843). The stored-procedure write variant emits XML
    ``statementType="spwrite"`` even though the spec enum is
    ``storedprocedurewrite``; the other four match verbatim. Reference is used
    only for shape verification — no table/column/SQL/procedure values are
    baked into the builder.
    """

    SUPPORTED_PROFILE_TYPES = ("database.write",)
    _STATEMENT_TYPE_LABEL = "Database Write profiles (profile_type='database.write')"

    DEFAULT_FIELD_DATA_TYPE = "character"

    # Spec enum -> XML DBStatement@statementType value. Four map verbatim; the
    # stored-procedure write variant maps to the live XML value "spwrite".
    _STATEMENT_TYPE_XML = {
        "standardinsertupdatedelete": "standardinsertupdatedelete",
        "dynamicinsert": "dynamicinsert",
        "dynamicupdate": "dynamicupdate",
        "dynamicdelete": "dynamicdelete",
        "storedprocedurewrite": "spwrite",
    }
    # Per-statement-type shape rules (see live captures table).
    _REQUIRES_SQL = ("standardinsertupdatedelete", "storedprocedurewrite")
    _REQUIRES_TABLE = ("dynamicinsert", "dynamicupdate", "dynamicdelete")
    _REQUIRES_STORED_PROCEDURE = ("storedprocedurewrite",)
    # Every type emits DBFields except dynamicdelete (conditions only).
    _EMITS_FIELDS = (
        "standardinsertupdatedelete",
        "dynamicinsert",
        "dynamicupdate",
        "storedprocedurewrite",
    )
    _REQUIRES_FIELDS = _EMITS_FIELDS
    # Only the dynamic update/delete variants carry a WHERE-key Conditions block.
    _EMITS_CONDITIONS = ("dynamicupdate", "dynamicdelete")
    _REQUIRES_CONDITIONS = _EMITS_CONDITIONS

    # Mirror DatabaseConnectorBuilder / read-profile contract so
    # integration_builder preflight has uniform secret behavior. Write
    # profiles transport column shapes and caller SQL, never credentials.
    FORBIDDEN_SECRET_FIELDS = (
        "password",
        "password_ref",
        "secret",
        "token",
        "access_token",
        "client_secret",
    )

    @classmethod
    def scan_forbidden_secret_fields(
        cls, config: Any, _path_prefix: str = ""
    ) -> Optional[BuilderValidationError]:
        """Detect plaintext secret-shaped keys at any depth.

        Same traversal contract as the read-profile / connector scanners —
        returns the shallowest occurrence with a path-shaped ``field``.
        """
        if isinstance(config, dict):
            for forbidden in cls.FORBIDDEN_SECRET_FIELDS:
                if forbidden in config:
                    field_path = f"{_path_prefix}{forbidden}"
                    return BuilderValidationError(
                        f"{field_path!r} cannot be supplied in database write "
                        "profile config — write profiles describe the target "
                        "table/column shape, not credentials. Connection-level "
                        "credentials belong on the connector-settings component "
                        "via credential_ref.",
                        error_code="PLAINTEXT_SECRET_REJECTED",
                        field=field_path,
                        hint=(
                            "Remove the secret-shaped field. Database "
                            "credentials are configured on the connector-"
                            "settings component, not the write profile."
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
        """Recursively replace any forbidden-keyed values with '[REDACTED]'."""
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
    # Validation
    # ------------------------------------------------------------------

    @classmethod
    def _normalized_statement_type(cls, config: Dict[str, Any]) -> str:
        raw = config.get("statement_type")
        return raw.lower() if isinstance(raw, str) else ""

    @classmethod
    def _validate_element(
        cls, element: Any, index: int, *, field_name: str
    ) -> Optional[BuilderValidationError]:
        """Validate a single fields[]/conditions[] entry's shape + data type."""
        if not isinstance(element, dict):
            return BuilderValidationError(
                f"{field_name}[{index}] must be an object",
                error_code="DATABASE_OPERATION_VALIDATION_FAILED",
                field=f"{field_name}[{index}]",
                hint="Each entry is a JSON object with at least a 'name'.",
            )
        name = element.get("name")
        if not name or not str(name).strip():
            return BuilderValidationError(
                f"{field_name}[{index}].name is required",
                error_code="DATABASE_OPERATION_VALIDATION_FAILED",
                field=f"{field_name}[{index}].name",
                hint="Provide a non-empty column name.",
            )
        data_type = element.get("data_type", cls.DEFAULT_FIELD_DATA_TYPE)
        if data_type not in _SUPPORTED_FIELD_TYPES:
            supported = ", ".join(sorted(_SUPPORTED_FIELD_TYPES.keys()))
            return BuilderValidationError(
                f"{field_name}[{index}].data_type={data_type!r} is not supported",
                error_code="UNSUPPORTED_DB_PROFILE_FIELD_TYPE",
                field=f"{field_name}[{index}].data_type",
                hint=f"Supported data types: {supported}.",
            )
        for flag in ("mandatory", "enforce_unique"):
            if flag in element and not isinstance(element[flag], bool):
                return BuilderValidationError(
                    f"{field_name}[{index}].{flag} must be a bool",
                    error_code="DATABASE_OPERATION_VALIDATION_FAILED",
                    field=f"{field_name}[{index}].{flag}",
                    hint="Use true or false.",
                )
        return None

    @classmethod
    def validate_config(
        cls, config: Dict[str, Any]
    ) -> Optional[BuilderValidationError]:
        # 1) Plaintext secret-shaped keys.
        secret_err = cls.scan_forbidden_secret_fields(config)
        if secret_err is not None:
            return secret_err

        # 2) profile_type must be database.write.
        profile_type = config.get("profile_type") or ""
        if profile_type not in cls.SUPPORTED_PROFILE_TYPES:
            return BuilderValidationError(
                f"profile_type must be one of {cls.SUPPORTED_PROFILE_TYPES} "
                f"(got {profile_type!r})",
                error_code="UNSUPPORTED_DB_PROFILE_MODE",
                field="profile_type",
                hint=f"This builder handles {cls._STATEMENT_TYPE_LABEL}.",
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

        # 4) statement_type required and supported.
        statement_type = cls._normalized_statement_type(config)
        if statement_type not in cls._STATEMENT_TYPE_XML:
            supported = ", ".join(sorted(cls._STATEMENT_TYPE_XML.keys()))
            return BuilderValidationError(
                f"statement_type={config.get('statement_type')!r} is not supported",
                error_code="UNSUPPORTED_DB_STATEMENT_TYPE",
                field="statement_type",
                hint=f"Supported statement types: {supported}.",
            )

        # 5) sql / table_name / stored_procedure presence per statement type.
        sql = config.get("sql")
        if statement_type in cls._REQUIRES_SQL and (
            sql is None or not str(sql).strip()
        ):
            return BuilderValidationError(
                f"sql is required for statement_type={statement_type!r}",
                error_code="MISSING_DB_SQL",
                field="sql",
                hint=(
                    "Author the write SQL (e.g. an INSERT/UPDATE/DELETE with '?' "
                    "placeholders, or a '{ call proc(?, ...) }' invocation). The "
                    "builder stores it verbatim and does not validate syntax."
                ),
            )
        if statement_type not in cls._REQUIRES_SQL and sql is not None and str(sql).strip():
            return BuilderValidationError(
                f"sql must be omitted for statement_type={statement_type!r}",
                error_code="DATABASE_OPERATION_VALIDATION_FAILED",
                field="sql",
                hint=(
                    "Dynamic statement types generate their SQL from table_name "
                    "plus fields/conditions at runtime; the builder emits an "
                    "empty <sql/>. Omit sql for dynamic* types."
                ),
            )

        table_name = config.get("table_name")
        if statement_type in cls._REQUIRES_TABLE and (
            table_name is None or not str(table_name).strip()
        ):
            return BuilderValidationError(
                f"table_name is required for statement_type={statement_type!r}",
                error_code="MISSING_DB_TABLE_NAME",
                field="table_name",
                hint="Provide the target table name for the dynamic write.",
            )
        if statement_type not in cls._REQUIRES_TABLE and table_name is not None and str(table_name).strip():
            # Live shape requires DBStatement@tableName="" for standard and
            # storedprocedurewrite; a stray table_name would emit a hybrid.
            return BuilderValidationError(
                f"table_name is not allowed for statement_type={statement_type!r}",
                error_code="DATABASE_OPERATION_VALIDATION_FAILED",
                field="table_name",
                hint=(
                    "Only the dynamic* statement types target a table by name. "
                    "Standard / storedprocedurewrite express the table through "
                    "the SQL or stored procedure — omit table_name."
                ),
            )

        stored_procedure = config.get("stored_procedure")
        if statement_type in cls._REQUIRES_STORED_PROCEDURE and (
            stored_procedure is None or not str(stored_procedure).strip()
        ):
            return BuilderValidationError(
                "stored_procedure is required for statement_type="
                "'storedprocedurewrite'",
                error_code="MISSING_DB_STORED_PROCEDURE",
                field="stored_procedure",
                hint=(
                    "Provide the fully-qualified procedure name as your database "
                    "vendor expects it (stored verbatim, not normalized)."
                ),
            )
        if statement_type not in cls._REQUIRES_STORED_PROCEDURE and stored_procedure is not None and str(stored_procedure).strip():
            # Live shape requires DBStatement@storedProcedure="" for every
            # non-SP statement type; a stray stored_procedure would emit a hybrid.
            return BuilderValidationError(
                f"stored_procedure is not allowed for statement_type={statement_type!r}",
                error_code="DATABASE_OPERATION_VALIDATION_FAILED",
                field="stored_procedure",
                hint=(
                    "Only storedprocedurewrite uses a stored procedure name. "
                    "Other statement types express the write via SQL or "
                    "table_name — omit stored_procedure."
                ),
            )

        # 6) fields presence + shape.
        fields = config.get("fields")
        if statement_type in cls._REQUIRES_FIELDS:
            if not isinstance(fields, list) or len(fields) == 0:
                return BuilderValidationError(
                    f"fields is required and must be a non-empty list for "
                    f"statement_type={statement_type!r}",
                    error_code="MISSING_DB_FIELDS",
                    field="fields",
                    hint=(
                        "Declare one entry per '?' bind column (standard / "
                        "stored-procedure) or per column to write (dynamic "
                        "insert/update): [{'name': 'col1'}, ...]."
                    ),
                )
        elif fields is not None and (not isinstance(fields, list) or len(fields) > 0):
            # dynamicdelete carries conditions only — no DBFields block.
            return BuilderValidationError(
                f"fields is not allowed for statement_type={statement_type!r}",
                error_code="DATABASE_OPERATION_VALIDATION_FAILED",
                field="fields",
                hint=(
                    "dynamicdelete uses conditions (the WHERE keys) only and "
                    "emits no DBFields block. Move the keys to conditions[]."
                ),
            )
        for index, field in enumerate(fields or []):
            field_err = cls._validate_element(field, index, field_name="fields")
            if field_err is not None:
                return field_err

        # 7) conditions presence + shape.
        conditions = config.get("conditions")
        if statement_type in cls._REQUIRES_CONDITIONS:
            if not isinstance(conditions, list) or len(conditions) == 0:
                return BuilderValidationError(
                    f"conditions is required and must be a non-empty list for "
                    f"statement_type={statement_type!r}",
                    error_code="MISSING_DB_CONDITIONS",
                    field="conditions",
                    hint=(
                        "Declare one entry per WHERE-clause key column: "
                        "[{'name': 'id_col', 'data_type': 'number'}, ...]."
                    ),
                )
        elif conditions is not None and (
            not isinstance(conditions, list) or len(conditions) > 0
        ):
            return BuilderValidationError(
                f"conditions is not allowed for statement_type={statement_type!r}",
                error_code="DATABASE_OPERATION_VALIDATION_FAILED",
                field="conditions",
                hint=(
                    "Only dynamicupdate and dynamicdelete carry a Conditions "
                    "(WHERE keys) block. Standard / dynamicinsert / "
                    "storedprocedurewrite express keys via fields or SQL."
                ),
            )
        for index, condition in enumerate(conditions or []):
            cond_err = cls._validate_element(
                condition, index, field_name="conditions"
            )
            if cond_err is not None:
                return cond_err

        return None

    # ------------------------------------------------------------------
    # Field index (issue #26: consumed by DirectMapBuilder for map targets)
    # ------------------------------------------------------------------

    @classmethod
    def build_field_index(
        cls, config: Dict[str, Any]
    ) -> Dict[str, Dict[str, Any]]:
        """Return ``{path: {key, key_path, name_path, ...}}`` for map targets.

        The index is keyed by namespace-prefixed path — ``Fields/<name>`` for
        the writable columns and ``Conditions/<name>`` for the WHERE keys — so a
        column appearing in both the SET fields and the WHERE conditions of a
        dynamic update stays distinct, and a map's ``target_path`` can address
        either namespace unambiguously. Element ``key`` values mirror
        ``build()``: fields are emitted first starting at key 5 under DBFields
        (key 3), then conditions continue under DBConditions (key 4). Assumes a
        well-formed config (caller validates first).
        """
        index: Dict[str, Dict[str, Any]] = {}
        next_key = 5
        statement_type = cls._normalized_statement_type(config)
        # Keys are namespace-prefixed ("Fields/<name>" / "Conditions/<name>")
        # so a column name appearing in BOTH the SET fields and the WHERE
        # conditions (dynamic update) does not collide in the flat index, and
        # a map can address either namespace unambiguously via target_path.
        if statement_type in cls._EMITS_FIELDS:
            for field in config.get("fields") or []:
                if not isinstance(field, dict):
                    continue
                name_raw = field.get("name")
                if not name_raw or not str(name_raw).strip():
                    continue
                name = str(name_raw).strip()
                path = f"Fields/{name}"
                data_type = field.get("data_type", cls.DEFAULT_FIELD_DATA_TYPE)
                index[path] = {
                    "path": path,
                    "name": name,
                    "key": next_key,
                    "key_path": f"*[@key='2']/*[@key='3']/*[@key='{next_key}']",
                    "name_path": f"Statement/Fields/{name}",
                    "data_type": data_type,
                    "kind": "simple",
                    "required": bool(field.get("mandatory", False)),
                    "mappable": True,
                }
                next_key += 1
        if statement_type in cls._EMITS_CONDITIONS:
            for condition in config.get("conditions") or []:
                if not isinstance(condition, dict):
                    continue
                name_raw = condition.get("name")
                if not name_raw or not str(name_raw).strip():
                    continue
                name = str(name_raw).strip()
                path = f"Conditions/{name}"
                data_type = condition.get("data_type", cls.DEFAULT_FIELD_DATA_TYPE)
                index[path] = {
                    "path": path,
                    "name": name,
                    "key": next_key,
                    "key_path": f"*[@key='2']/*[@key='4']/*[@key='{next_key}']",
                    "name_path": f"Statement/Conditions/{name}",
                    "data_type": data_type,
                    "kind": "simple",
                    "required": True,
                    "mappable": True,
                }
                next_key += 1
        return index

    # ------------------------------------------------------------------
    # XML rendering
    # ------------------------------------------------------------------

    @classmethod
    def _render_field(cls, field: Dict[str, Any], key: int) -> Tuple[str, int]:
        name = _escape_xml(str(field["name"]))
        data_type = field.get("data_type", cls.DEFAULT_FIELD_DATA_TYPE)
        mandatory = "true" if field.get("mandatory", False) else "false"
        enforce_unique = "true" if field.get("enforce_unique", False) else "false"
        format_tag = _SUPPORTED_FIELD_TYPES[data_type]
        # Live write XML omits the dataType attribute for character columns
        # (read profiles always emit it); number/datetime carry it explicitly.
        dtype_attr = f'dataType="{data_type}" ' if data_type != "character" else ""
        xml = (
            f'                        <DatabaseElement {dtype_attr}'
            f'enforceUnique="{enforce_unique}" isMappable="true" isNode="true"'
            f' key="{key}" mandatory="{mandatory}" name="{name}">\n'
            f'                            <DataFormat><{format_tag}/></DataFormat>\n'
            f'                        </DatabaseElement>'
        )
        return xml, key + 1

    @classmethod
    def _render_condition(
        cls, condition: Dict[str, Any], key: int
    ) -> Tuple[str, int]:
        name = _escape_xml(str(condition["name"]))
        data_type = condition.get("data_type", cls.DEFAULT_FIELD_DATA_TYPE)
        format_tag = _SUPPORTED_FIELD_TYPES[data_type]
        dtype_attr = f'dataType="{data_type}" ' if data_type != "character" else ""
        xml = (
            f'                        <DBCondition {dtype_attr}'
            f'isMappable="true" isNode="true" key="{key}" name="{name}">\n'
            f'                            <DataFormat><{format_tag}/></DataFormat>\n'
            f'                        </DBCondition>'
        )
        return xml, key + 1

    def build(self, **params) -> str:
        error = self.validate_config(params)
        if error is not None:
            raise error

        component_name = params["component_name"]
        statement_type = type(self)._normalized_statement_type(params)
        xml_statement_type = self._STATEMENT_TYPE_XML[statement_type]
        fields = params.get("fields") or []
        conditions = params.get("conditions") or []
        table_name = params.get("table_name", "") or ""
        stored_procedure = params.get("stored_procedure", "") or ""
        sql = params.get("sql", "")
        folder_name = params.get("folder_name", "Home")
        description = params.get("description", "")

        safe_name = _escape_xml(component_name)
        safe_folder = _escape_xml(folder_name)
        safe_desc = _escape_xml(description)
        safe_table = _escape_xml(str(table_name))
        safe_proc = _escape_xml(str(stored_procedure))

        # Deterministic key allocation: DBStatement=2, DBFields=3,
        # DBConditions=4, then field elements start at 5 in caller order and
        # condition elements continue sequentially.
        next_key = 5
        inner_parts: List[str] = []
        if statement_type in self._EMITS_FIELDS:
            field_xml_parts: List[str] = []
            for field in fields:
                field_xml, next_key = self._render_field(field, next_key)
                field_xml_parts.append(field_xml)
            inner_parts.append(
                '                    <DBFields isNode="true" key="3" name="Fields">\n'
                + "\n".join(field_xml_parts)
                + '\n                    </DBFields>'
            )
        if statement_type in self._EMITS_CONDITIONS:
            cond_xml_parts: List[str] = []
            for condition in conditions:
                cond_xml, next_key = self._render_condition(condition, next_key)
                cond_xml_parts.append(cond_xml)
            inner_parts.append(
                '                    <DBConditions isNode="true" key="4" name="Conditions">\n'
                + "\n".join(cond_xml_parts)
                + '\n                    </DBConditions>'
            )
        if statement_type in self._REQUIRES_SQL:
            inner_parts.append(f'                    <sql>{_escape_xml(str(sql))}</sql>')
        else:
            inner_parts.append('                    <sql/>')
        inner = "\n".join(inner_parts)

        db_statement_xml = (
            '                <DBStatement isNode="true" key="2" name="Statement"'
            f' statementType="{xml_statement_type}"'
            f' storedProcedure="{safe_proc}" tableName="{safe_table}">\n'
            f'{inner}\n'
            '                </DBStatement>'
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
            '                <DatabaseGeneralInfo executionType="dbwrite"/>\n'
            '            </ProfileProperties>\n'
            '            <DataElements>\n'
            f'{db_statement_xml}\n'
            '            </DataElements>\n'
            '        </DatabaseProfile>\n'
            '    </bns:object>\n'
            '</bns:Component>'
        )


# ============================================================================
# Registry
# ============================================================================

# Keyed by (component_type, profile_type). Mirrors the pattern of
# CONNECTOR_ACTION_BUILDERS in connector_builder.py.
#
# Imports are at module bottom (after the existing builders are defined) to
# avoid circular imports — the new issue #26 builders may eventually pull
# helpers back from this module.
from .json_profile_builder import JSONGeneratedProfileBuilder  # noqa: E402
from .xml_profile_builder import XMLGeneratedProfileBuilder  # noqa: E402


PROFILE_BUILDERS: Dict[Tuple[str, str], type] = {
    ("profile.db", "database.read"): DatabaseReadProfileBuilder,
    ("profile.db", "database.stored_procedure_read"):
        DatabaseStoredProcedureReadProfileBuilder,
    ("profile.db", "database.write"): DatabaseWriteProfileBuilder,
    ("profile.json", "json.generated"): JSONGeneratedProfileBuilder,
    ("profile.xml", "xml.generated"): XMLGeneratedProfileBuilder,
}


# Issue #45 — update-preservation policies for the two DB Read profile
# builders. The JSON/XML profile builders attach their own policies in
# their own modules so direct imports (without going through this module)
# still see the PRESERVATION_POLICY class attribute.
#
# Codex r8 P2 narrow-risk guard: the type="profile.db" root check
# alone passes for DB write profiles too (a future #32 deferral), so
# pointing this read-builder update at a write profile would build a
# hybrid component. Validate the executionType marker pre-merge.
_DATABASE_READ_PROFILE_POLICY = PreservationPolicy(
    component_type="profile.db",
    owned_paths=(OwnedPath(path="bns:object/DatabaseProfile/DataElements"),),
    subtype_marker_xpath=(
        "bns:object/DatabaseProfile/ProfileProperties/DatabaseGeneralInfo"
    ),
    subtype_marker_attr="executionType",
    subtype_marker_expected="dbread",
)

DatabaseReadProfileBuilder.PRESERVATION_POLICY = _DATABASE_READ_PROFILE_POLICY
DatabaseStoredProcedureReadProfileBuilder.PRESERVATION_POLICY = (
    _DATABASE_READ_PROFILE_POLICY
)

# Issue #32 — write-profile update-preservation policy. Same owned subtree as
# the read policy (the builder owns DatabaseProfile/DataElements), but the
# subtype marker pins executionType="dbwrite" so a write-builder update can
# never silently merge onto a read profile (and vice versa).
_DATABASE_WRITE_PROFILE_POLICY = PreservationPolicy(
    component_type="profile.db",
    owned_paths=(OwnedPath(path="bns:object/DatabaseProfile/DataElements"),),
    subtype_marker_xpath=(
        "bns:object/DatabaseProfile/ProfileProperties/DatabaseGeneralInfo"
    ),
    subtype_marker_attr="executionType",
    subtype_marker_expected="dbwrite",
)

DatabaseWriteProfileBuilder.PRESERVATION_POLICY = _DATABASE_WRITE_PROFILE_POLICY


def get_profile_builder(component_type: str, profile_type: str):
    """Get a profile builder instance for (component_type, profile_type), or None."""
    if not component_type or not profile_type:
        return None
    key = (component_type.lower(), profile_type.lower())
    builder_class = PROFILE_BUILDERS.get(key)
    if builder_class:
        return builder_class()
    return None

"""
Profile component XML builders for Boomi.

Builds XML for profile.db (database Read profile) components via the Component
API. Mirrors the conventions of connector_builder.py: BuilderValidationError
for structured errors, scan_forbidden_secret_fields + validate_config
classmethods for plan-time preflight, build() instance method that raises.

Reference XML shape (work-profile b39ffdd4 + 5fe35b85, fetched 2026-05-18):

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
                         statementType="select" storedProcedure="" tableName="">
              <DBFields isNode="true" key="3" name="Fields" type="result_set">
                <DatabaseElement dataType="character" enforceUnique="false"
                                 isMappable="true" isNode="true" key="5"
                                 mandatory="false" name="...">
                  <DataFormat><ProfileCharacterFormat/></DataFormat>
                </DatabaseElement>
              </DBFields>
              <DBParameters isNode="true" key="4" name="Parameters">
                <DBParameter isMappable="false" isNode="true" key="6" name="...">
                  <DataFormat><ProfileCharacterFormat/></DataFormat>
                </DBParameter>
              </DBParameters>
              <sql>...task-authored SQL...</sql>
            </DBStatement>
          </DataElements>
        </DatabaseProfile>
      </bns:object>
    </bns:Component>

Key allocation is deterministic: DBStatement=2, DBFields=3, DBParameters=4,
then output DatabaseElement entries start at 5 in caller order, then DBParameter
entries continue (sequential across both — matches live CDS reference where
the lone output field is key=5 and the lone parameter is key=6).
"""

from typing import Any, Dict, List, Optional, Tuple

from .connector_builder import BuilderValidationError, _escape_xml


# Supported per-field data types for v1. Each maps to a DataFormat child
# element. Boomi profile.db also supports date/number/datetime/etc., but those
# require shape verification against live XML before we emit them — defer to
# a follow-up. Callers attempting an unsupported type get a structured error.
_SUPPORTED_FIELD_TYPES: Dict[str, str] = {
    "character": "ProfileCharacterFormat",
}


class DatabaseReadProfileBuilder:
    """Builder for profile.db (database Read) components.

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
    """

    SUPPORTED_PROFILE_TYPES = ("database.read",)
    DEFAULT_FIELD_DATA_TYPE = "character"
    DEFAULT_PARAMETER_DATA_TYPE = "character"
    # Defensive consistency with DatabaseConnectorBuilder — no secrets are
    # expected in a read profile (just SQL + field shape), but mirror the
    # scan so integration_builder preflight has uniform behavior.
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

    @classmethod
    def validate_config(cls, config: Dict[str, Any]) -> Optional[BuilderValidationError]:
        """Validate a read-profile config without building XML.

        Returns the first BuilderValidationError encountered, or None. Used
        by both build() (which raises) and integration_builder._build_plan
        (which surfaces the structured error in the plan step).
        """
        # 1) Plaintext secret-shaped keys.
        secret_err = cls.scan_forbidden_secret_fields(config)
        if secret_err is not None:
            return secret_err

        # 2) profile_type must be database.read.
        profile_type = config.get("profile_type") or ""
        if profile_type not in cls.SUPPORTED_PROFILE_TYPES:
            return BuilderValidationError(
                f"profile_type must be one of {cls.SUPPORTED_PROFILE_TYPES} "
                f"(got {profile_type!r})",
                error_code="UNSUPPORTED_DB_PROFILE_MODE",
                field="profile_type",
                hint=(
                    "Issue #23 supports only profile_type='database.read' "
                    "(Select statement). Stored-procedure read profiles "
                    "and write profiles are deferred to later milestones."
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

        # 4) query required and non-empty.
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

        # 5) output_fields required and non-empty.
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

        # 6) parameters optional; when present, must be a list.
        parameters = config.get("parameters", [])
        if not isinstance(parameters, list):
            return BuilderValidationError(
                "parameters must be a list when provided",
                error_code="DATABASE_OPERATION_VALIDATION_FAILED",
                field="parameters",
                hint=(
                    "Omit parameters or pass a list of {name, data_type?, "
                    "mappable?} entries."
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
            return BuilderValidationError(
                f"output_fields[{index}].data_type={data_type!r} is not yet supported",
                error_code="UNSUPPORTED_DB_PROFILE_FIELD_TYPE",
                field=f"output_fields[{index}].data_type",
                hint=(
                    "v1 of the read-profile builder supports data_type='character' "
                    "only. date/number/datetime require live-XML shape verification "
                    "and are deferred to a follow-up."
                ),
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
            return BuilderValidationError(
                f"parameters[{index}].data_type={data_type!r} is not yet supported",
                error_code="UNSUPPORTED_DB_PROFILE_FIELD_TYPE",
                field=f"parameters[{index}].data_type",
                hint=(
                    "v1 of the read-profile builder supports data_type='character' "
                    "only for parameters. Other types require live-XML shape "
                    "verification and are deferred."
                ),
            )
        mappable = parameter.get("mappable", False)
        if not isinstance(mappable, bool):
            return BuilderValidationError(
                f"parameters[{index}].mappable must be a bool",
                error_code="DATABASE_OPERATION_VALIDATION_FAILED",
                field=f"parameters[{index}].mappable",
                hint="Use true or false.",
            )
        return None

    def build(self, **params) -> str:
        error = self.validate_config(params)
        if error is not None:
            raise error

        component_name = params["component_name"]
        query = params["query"]
        output_fields = params["output_fields"]
        parameters = params.get("parameters", [])
        folder_name = params.get("folder_name", "Home")
        description = params.get("description", "")

        safe_name = _escape_xml(component_name)
        safe_folder = _escape_xml(folder_name)
        safe_desc = _escape_xml(description)
        safe_sql = _escape_xml(str(query))

        # Deterministic key allocation: DBStatement=2, DBFields=3,
        # DBParameters=4, then output fields start at 5 in caller order,
        # then parameters continue sequentially (live CDS reference 5fe35b85
        # has output=5, parameter=6).
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
            '                <DBStatement isNode="true" key="2" name="Statement"'
            ' statementType="select" storedProcedure="" tableName="">\n'
            '                    <DBFields isNode="true" key="3" name="Fields" type="result_set">\n'
            f'{fields_block}\n'
            '                    </DBFields>\n'
            f'{parameters_block}\n'
            f'                    <sql>{safe_sql}</sql>\n'
            '                </DBStatement>\n'
            '            </DataElements>\n'
            '        </DatabaseProfile>\n'
            '    </bns:object>\n'
            '</bns:Component>'
        )

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
        # DBParameter intentionally omits a dataType attribute — the live CDS
        # reference (5fe35b85 key=6 "Statement") carries only isMappable/
        # isNode/key/name with the DataFormat child driving the type.
        xml = (
            f'                        <DBParameter isMappable="{mappable}" isNode="true"'
            f' key="{key}" name="{name}">\n'
            f'                            <DataFormat><{format_tag}/></DataFormat>\n'
            f'                        </DBParameter>'
        )
        return xml, key + 1


# ============================================================================
# Registry
# ============================================================================

# Keyed by (component_type, profile_type). Mirrors the pattern of
# CONNECTOR_ACTION_BUILDERS in connector_builder.py.
PROFILE_BUILDERS: Dict[Tuple[str, str], type] = {
    ("profile.db", "database.read"): DatabaseReadProfileBuilder,
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

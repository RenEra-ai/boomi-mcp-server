"""
Connector component XML builders for Boomi.

Builds XML for connector-settings (connections) and connector-action
(operations) via the Component API. Supported builders today: Database
(M2.2) and REST Client (M2.4). The legacy HTTP Client connector builder
was removed alongside issue #24 — use REST Client (`connector_type='rest'`)
for new HTTP-style targets, or the raw-XML escape hatch
(`manage_connector action='create' config={"xml":"..."}`) to land a
component that doesn't map to any builder.

The SDK's create_component() cannot parse the XML response for connectors,
so creation uses raw Serializer POST (see connectors.py _create_component_raw).
"""

import re
from typing import Dict, Any, Optional


class BuilderValidationError(ValueError):
    """Structured connector-builder validation failure.

    Subclasses ValueError so existing `except ValueError` catches still fire,
    but carries machine-readable fields (error_code, field, hint) so the MCP
    layer can return a structured envelope instead of an opaque message.
    """

    def __init__(
        self,
        message: str,
        *,
        error_code: str,
        field: Optional[str] = None,
        hint: Optional[str] = None,
    ) -> None:
        super().__init__(message)
        self.error_code = error_code
        self.field = field
        self.hint = hint


def _escape_xml(text: str) -> str:
    """Escape special XML characters in attribute values."""
    if not text:
        return ""
    for char, escaped in [('&', '&amp;'), ('<', '&lt;'), ('>', '&gt;'),
                          ('"', '&quot;'), ("'", '&apos;')]:
        text = text.replace(char, escaped)
    return text


# Canonical attribute order + defaults for <AdapterPoolInfo>. Two-default-column
# form: when pooling is omitted entirely (or enabled=false), use the
# default_when_disabled value; when pooling.enabled=true and a key is omitted,
# use default_when_enabled. The (max_active, max_idle) flip from 0 → -1 mirrors
# the CDS reference (work-account 273d1741) where pooling-enabled connections
# default to unbounded.
POOLING_ATTR_ORDER = (
    # (snake_case config key,  Boomi XML attribute,  default_when_disabled, default_when_enabled)
    ("exhausted_action",       "exhaustedAction",    1,     1),
    ("max_active",             "maxActive",          0,    -1),
    ("max_idle",               "maxIdle",            0,    -1),
    ("max_idle_time",          "maxIdleTime",        0,     0),
    ("max_wait",               "maxWait",            0,     0),
    ("min_idle",               "minIdle",            0,     0),
    ("number_of_tests",        "numberOfTests",      0,     0),
    ("test_idle",              "testIdle",           False, False),
    ("test_on_borrow",         "testOnBorrow",       False, False),
    ("test_on_return",         "testOnReturn",       False, False),
    ("time_between_runs",      "timeBetweenRuns",    0,     0),
    ("validation_query",       "validationQuery",    "",    ""),
)
POOLING_ALLOWED_KEYS = frozenset({"enabled", *(k for k, *_ in POOLING_ATTR_ORDER)})

# Canonical attribute order + defaults for <WriteOptions>.
WRITE_OPTIONS_ATTR_ORDER = (
    # (snake_case, XML attribute, default)
    ("sql_file_path",     "sqlFilePath",    "tmp/sqldebug.txt"),
    ("write_sql_to_file", "writeSQLToFile", False),
)
WRITE_OPTIONS_ALLOWED_KEYS = frozenset(k for k, *_ in WRITE_OPTIONS_ATTR_ORDER)


def _format_xml_value(value: Any) -> str:
    """Format a Python scalar for an XML attribute value.

    bool → "true"/"false" (Boomi's lowercase convention).
    int  → str(int) (preserves negative values like -1 for unbounded pool).
    str  → XML-escaped string.
    """
    if isinstance(value, bool):
        return "true" if value else "false"
    if isinstance(value, int):
        return str(value)
    return _escape_xml(str(value))


class DatabaseConnectorBuilder:
    """Builder for Database (Legacy) connector-settings components.

    Generates <DatabaseConnectionSettings> XML matching Boomi UI export
    structure. Issue #31 (M2.x) extends the buildable set to SQL Server
    (Microsoft JDBC and jTDS), Oracle, MySQL, SAP HANA, and Custom. Postgres
    is still rejected with UNSUPPORTED_DB_DRIVER — no live #Common reference
    XML exists yet.

    Two driver shapes are modeled in DRIVERS:

      host_port_db: sqlserver, jtds, oracle, mysql, sap-hana.
        Caller supplies host/port/dbname; the builder substitutes them into
        the driver's urlFormat. SAP HANA has no verified default port so
        callers MUST supply one (port_required=True on the driver entry).

      custom_url:  custom.
        Caller supplies custom_class_name (→ className) and connection_url
        (→ urlFormat) directly. host/port/dbname/additional are forbidden in
        the JSON contract; the XML emits them as empty strings to match the
        Boomi live-export shape byte-for-byte.

    Config keys:
        component_name:     required
        driver_id:          required; one of SUPPORTED_DRIVER_IDS
                            ("sqlserver", "microsoft_jdbc", "jtds", "oracle",
                            "mysql", "sap_hana", "sap-hana", "custom").
                            "microsoft_jdbc" is a caller-facing alias for the
                            Microsoft JDBC driver and emits Boomi
                            driverId="sqlserver"; "sap_hana" is an alias for
                            the hyphenated canonical "sap-hana".
        auth_mode:          required; one of SUPPORTED_AUTH_MODES
                            ("username_password"). "windows_integrated" is
                            recognized but deliberately deferred.
        username:           required
        credential_ref:     required when auth_mode="username_password".
                            Opaque caller-side reference (e.g.
                            "credential://vault/sqlserver/password"); the
                            builder never writes it to the emitted XML —
                            secrets must be set in the Boomi UI after create
                            or supplied via the raw-XML escape hatch.
        folder_name:        optional; defaults to "Home"
        description:        optional

        host_port_db shape only:
            host:           required
            dbname:         required (database name)
            port:           required for sap-hana; optional elsewhere (falls
                            back to DRIVERS[driver_id]['default_port'])
            additional:     optional JDBC URL suffix appended verbatim into
                            urlFormat {3} (e.g. ";encrypt=true;
                            trustServerCertificate=true"). SQL Server's TLS
                            workaround lives here.

        custom_url shape only:
            custom_class_name: required JDBC driver class FQCN
            connection_url:    required full JDBC URL (no Boomi {0}{1}{2}{3}
                               substitution happens — the caller's string is
                               emitted verbatim as urlFormat).

    Plaintext secret-shaped keys (see FORBIDDEN_SECRET_FIELDS) are rejected
    loudly with PLAINTEXT_SECRET_REJECTED before any XML is emitted. Shape
    mismatch (e.g. host on a custom_url driver) fails with
    DATABASE_CONNECTOR_VALIDATION_FAILED before mutation.
    """

    # Issue #31: the registry now models two shapes — host_port_db (SQL Server,
    # jTDS, Oracle, MySQL, SAP HANA) and custom_url (caller-supplied Custom JDBC
    # driver). live_reference_component_id points at the reneraai-5RO3DD
    # `#Common` example used to verify each driver's XML byte-for-byte.
    DRIVERS: Dict[str, Dict[str, Any]] = {
        "sqlserver": {
            "shape":       "host_port_db",
            "buildable":   True,
            "driver_id":   "sqlserver",
            "class_name":  "com.microsoft.sqlserver.jdbc.SQLServerDriver",
            "url_format":  "jdbc:sqlserver://{0}:{1};database={2}{3}",
            "default_port": 1433,
            # Microsoft JDBC ≥12 defaults to encrypt=true; against the
            # mcr.microsoft.com/mssql/server self-signed cert the driver
            # rejects the TLS handshake unless trustServerCertificate=true
            # is set. Surfaced as schema-template metadata only — the
            # builder does not auto-inject or warn (caller's choice).
            "recommended_additional": ";encrypt=true;trustServerCertificate=true",
            "live_reference_component_id": "4ace95d7-6ee4-4f83-8fad-723d3fabdb2f",
        },
        "jtds": {
            "shape":       "host_port_db",
            "buildable":   True,
            "driver_id":   "jtds",
            "class_name":  "net.sourceforge.jtds.jdbc.Driver",
            "url_format":  "jdbc:jtds:sqlserver://{0}:{1}/{2}{3}",
            "default_port": 1433,
            "live_reference_component_id": "107aaef1-cb1e-4975-be44-69d120803864",
        },
        "oracle": {
            "shape":       "host_port_db",
            "buildable":   True,
            "driver_id":   "oracle",
            "class_name":  "oracle.jdbc.driver.OracleDriver",
            # Oracle Thin SID syntax (jdbc:oracle:thin:@host:port:sid) has
            # no {3} substitution slot in Boomi's url_format. Boomi still
            # appends `additional` to the end of the formed URL per its
            # Database (Legacy) docs ("appended to the end of the connection
            # URL according to your database vendor"), but Oracle Thin may
            # not accept arbitrary trailing semicolon options. The builder
            # emits whatever the caller supplies — runtime acceptance is the
            # caller's concern; the variant note in the schema template
            # points at driver_id='custom' with a service-name URL for
            # callers who need vendor-style options.
            "url_format":  "jdbc:oracle:thin:@{0}:{1}:{2}",
            "default_port": 1521,
            "live_reference_component_id": "6adf9e1e-39c8-4104-bc6c-9769b93aa161",
        },
        "mysql": {
            "shape":       "host_port_db",
            "buildable":   True,
            "driver_id":   "mysql",
            "class_name":  "com.mysql.jdbc.Driver",
            "url_format":  "jdbc:mysql://{0}:{1}/{2}{3}",
            "default_port": 3306,
            "live_reference_component_id": "bfbfea6f-39c7-498e-859b-6036959a20c8",
            # The legacy com.mysql.jdbc.Driver ships outside the Boomi runtime —
            # surfaced for callers via the schema template, not enforced here.
            "runtime_driver_prerequisite": (
                "MySQL Connector/J is not bundled with the Boomi runtime. "
                "Upload the driver as a Custom Library and deploy it to the "
                "runtime/environment before testing the connection."
            ),
        },
        # Canonical key uses a hyphen to match the emitted Boomi driverId
        # ("sap-hana"); "sap_hana" is exposed via DRIVER_ALIASES below.
        "sap-hana": {
            "shape":       "host_port_db",
            "buildable":   True,
            "driver_id":   "sap-hana",
            "class_name":  "com.sap.db.jdbc.Driver",
            "url_format":  "jdbc:sap://{0}:{1}/?databaseName={2}{3}",
            # No verified default port — Boomi does not assume one for HANA,
            # and tenant deployments vary (30015 cloud, 39015 system DB, etc).
            "default_port": None,
            "port_required": True,
            "live_reference_component_id": "c9077711-39a4-4d52-9f91-27bdf1f5b8ec",
            "runtime_driver_prerequisite": (
                "SAP HANA JDBC (ngdbc) is not bundled with the Boomi runtime. "
                "Deploy ngdbc.jar via Custom Library before connection tests."
            ),
        },
        "custom": {
            "shape":       "custom_url",
            "buildable":   True,
            "driver_id":   "custom",
            # className and urlFormat come from caller-supplied custom_class_name
            # and connection_url respectively — see build()'s custom_url branch.
            "class_name_source": "custom_class_name",
            "url_format_source": "connection_url",
            "default_port": None,
            "live_reference_component_id": "39fb519d-e970-4aaf-a1f7-4eba39158e9d",
            "runtime_driver_prerequisite": (
                "Custom JDBC drivers require an Account Library + Custom "
                "Library component deployed to the runtime/environment. The "
                "builder emits the XML envelope but does not deploy driver jars."
            ),
        },
    }
    # Caller-facing aliases. Each maps onto the canonical DRIVERS key; the
    # emitted Boomi driverId comes from the canonical entry's "driver_id".
    #   "microsoft_jdbc" → "sqlserver" (alias is self-documenting; Boomi has
    #                      no separate registration for Microsoft JDBC).
    #   "sap_hana"      → "sap-hana"  (underscore alias is JSON-friendly;
    #                      Boomi's canonical id keeps the hyphen).
    DRIVER_ALIASES: Dict[str, str] = {
        "microsoft_jdbc": "sqlserver",
        "sap_hana":       "sap-hana",
    }

    # All recognized and currently-buildable driver IDs (Issue #31). Aliases
    # are listed here so the schema template can advertise the underscore
    # forms without callers having to discover them from the alias map.
    RECOGNIZED_DRIVER_IDS = (
        "sqlserver", "microsoft_jdbc", "jtds",
        "oracle", "mysql", "sap_hana", "sap-hana", "custom",
    )
    SUPPORTED_DRIVER_IDS = RECOGNIZED_DRIVER_IDS
    SUPPORTED_AUTH_MODES = ("username_password",)
    UNSUPPORTED_FUTURE_AUTH_MODES = ("windows_integrated",)
    FORBIDDEN_SECRET_FIELDS = (
        "password",
        "password_ref",
        "secret",
        "token",
        "access_token",
        "client_secret",
    )
    # Required keys at the builder-input level, keyed by driver shape.
    # `connector_type` is consumed one layer up by create_connector to pick the
    # builder and never reaches build().
    REQUIRED_FIELDS_BY_SHAPE: Dict[str, tuple] = {
        "host_port_db": (
            "driver_id",
            "auth_mode",
            "component_name",
            "host",
            "dbname",
            "username",
            "credential_ref",
        ),
        "custom_url": (
            "driver_id",
            "auth_mode",
            "component_name",
            "custom_class_name",
            "connection_url",
            "username",
            "credential_ref",
        ),
    }
    # Fields that are valid for one shape but must be rejected for another.
    # Validated post-required so an empty-string value is also flagged when
    # explicitly supplied (consistent with `required` empty-string handling).
    FORBIDDEN_FIELDS_BY_SHAPE: Dict[str, tuple] = {
        "host_port_db": ("custom_class_name", "connection_url"),
        "custom_url":   ("host", "port", "dbname", "additional"),
    }
    # Back-compat alias: any external caller importing REQUIRED_FIELDS still
    # gets the host_port_db tuple (the only shape exposed in M2.2).
    REQUIRED_FIELDS = REQUIRED_FIELDS_BY_SHAPE["host_port_db"]

    @classmethod
    def _resolve_driver(cls, driver_id: str) -> Optional[Dict[str, Any]]:
        canonical = cls.DRIVER_ALIASES.get(driver_id, driver_id)
        return cls.DRIVERS.get(canonical)

    @classmethod
    def scan_forbidden_secret_fields(
        cls, config: Any, _path_prefix: str = ""
    ) -> Optional[BuilderValidationError]:
        """Detect plaintext secret-shaped keys at any depth.

        Generic JSON walker — descends into every dict child AND every list
        element (including lists nested inside lists, lists inside dicts
        inside lists, etc.). At each dict level, checks FORBIDDEN_SECRET_FIELDS
        in tuple order so 'password' beats 'token' at the same depth, and the
        shallowest occurrence wins overall. Returns the first offender with a
        path-shaped `field`:

            - top-level:        'password'
            - dict-in-dict:     'pooling.password'
            - dict-in-list:     'extra[0].password'
            - list-in-list:     'matrix[0][0].password'
            - mixed:            'wrapper.items[2].secret'

        Independent of builder invocation — plaintext secrets are a hard
        error regardless of which apply path the component takes (create /
        clone / reuse / update / raw-XML). Callers (e.g. integration_builder
        preflight) should run this on every database connector-settings
        config to keep credentials out of plan output.
        """
        if isinstance(config, dict):
            # Check forbidden keys at the current level first (preserves the
            # M2.2 priority where 'password' wins over 'token' at the same
            # depth, and where the shallowest occurrence wins overall).
            for forbidden in cls.FORBIDDEN_SECRET_FIELDS:
                if forbidden in config:
                    field_path = f"{_path_prefix}{forbidden}"
                    return BuilderValidationError(
                        f"{field_path!r} cannot be supplied in connector "
                        "config — secrets must cross the wire as opaque "
                        "credential_ref strings only. Boomi stores passwords as "
                        "ciphertext produced by its own encryption; there is no "
                        "public API to encrypt a plaintext value.",
                        error_code="PLAINTEXT_SECRET_REJECTED",
                        field=field_path,
                        hint=(
                            "Remove the secret-shaped field and pass "
                            "credential_ref='credential://...' instead. Set the "
                            "password in the Boomi UI after create, or supply a "
                            "pre-encrypted XML via config.xml=..."
                        ),
                    )
            # Then recurse into every value (dict / list / scalar). Insertion
            # order preserved (Python 3.7+).
            for key, value in config.items():
                nested = cls.scan_forbidden_secret_fields(
                    value, _path_prefix=f"{_path_prefix}{key}."
                )
                if nested is not None:
                    return nested
        elif isinstance(config, list):
            # Recurse into every element with `[index]` path notation.
            # Strip the trailing "." from the inbound prefix so the index
            # attaches to the preceding key (e.g. `extra.` + `[0]` → `extra[0]`),
            # then re-add "." for the next level's separator.
            base = _path_prefix[:-1] if _path_prefix.endswith(".") else _path_prefix
            for index, item in enumerate(config):
                nested = cls.scan_forbidden_secret_fields(
                    item, _path_prefix=f"{base}[{index}]."
                )
                if nested is not None:
                    return nested
        # Scalars / None: no keys to scan, return None.
        return None

    @classmethod
    def redact_forbidden_secret_fields_in_place(cls, config: Any) -> None:
        """Recursively replace any forbidden-keyed values with '[REDACTED]'.

        Mirrors scan_forbidden_secret_fields' traversal — descends into every
        dict child AND every list element at arbitrary depth. Callers (e.g.
        integration_builder _build_plan) use this to scrub the spec echo
        before returning a plan response. Without full container descent a
        config like `matrix: [[{"password": "..."}]]` would still echo the
        plaintext value even after the error is raised.
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
        # Scalars / None: no-op.

    @classmethod
    def validate_pooling(
        cls, pooling: Any
    ) -> Optional[BuilderValidationError]:
        """Validate the optional `pooling` config block.

        None is treated as "omitted" → None (clean). Anything else must be a
        dict whose keys are subset of POOLING_ALLOWED_KEYS and whose values
        match the type of the corresponding default (bool / int / str). The
        `enabled` flag must be a bool when present.

        Returns the first error encountered, or None.
        """
        if pooling is None:
            return None
        if not isinstance(pooling, dict):
            return BuilderValidationError(
                "pooling must be an object/dict, got "
                f"{type(pooling).__name__}",
                error_code="DATABASE_POOLING_VALIDATION_FAILED",
                field="pooling",
                hint=(
                    "Pass pooling as a JSON object with keys: "
                    f"{', '.join(sorted(POOLING_ALLOWED_KEYS))}."
                ),
            )
        unknown = set(pooling.keys()) - POOLING_ALLOWED_KEYS
        if unknown:
            offender = sorted(unknown)[0]
            return BuilderValidationError(
                f"pooling has unknown key {offender!r}",
                error_code="DATABASE_POOLING_VALIDATION_FAILED",
                field=f"pooling.{offender}",
                hint=(
                    "Allowed pooling keys: "
                    f"{', '.join(sorted(POOLING_ALLOWED_KEYS))}."
                ),
            )
        if "enabled" in pooling and not isinstance(pooling["enabled"], bool):
            return BuilderValidationError(
                "pooling.enabled must be a bool",
                error_code="DATABASE_POOLING_VALIDATION_FAILED",
                field="pooling.enabled",
                hint="Use true or false.",
            )
        for snake, _camel, default_disabled, _default_enabled in POOLING_ATTR_ORDER:
            if snake not in pooling:
                continue
            value = pooling[snake]
            # bool must be checked before int (bool is subclass of int).
            if isinstance(default_disabled, bool):
                if not isinstance(value, bool):
                    return BuilderValidationError(
                        f"pooling.{snake} must be a bool",
                        error_code="DATABASE_POOLING_VALIDATION_FAILED",
                        field=f"pooling.{snake}",
                        hint="Use true or false.",
                    )
            elif isinstance(default_disabled, int):
                if isinstance(value, bool) or not isinstance(value, int):
                    return BuilderValidationError(
                        f"pooling.{snake} must be an integer",
                        error_code="DATABASE_POOLING_VALIDATION_FAILED",
                        field=f"pooling.{snake}",
                        hint="Use a JSON integer (negative values allowed where unbounded).",
                    )
            else:  # str
                if not isinstance(value, str):
                    return BuilderValidationError(
                        f"pooling.{snake} must be a string",
                        error_code="DATABASE_POOLING_VALIDATION_FAILED",
                        field=f"pooling.{snake}",
                        hint="Use a JSON string (may be empty).",
                    )
        return None

    @classmethod
    def validate_write_options(
        cls, write_options: Any
    ) -> Optional[BuilderValidationError]:
        """Validate the optional `write_options` config block.

        None → None (clean). Otherwise must be a dict whose keys are subset of
        WRITE_OPTIONS_ALLOWED_KEYS and whose values are correctly typed. Cross
        field: write_sql_to_file=True requires a non-empty sql_file_path.
        """
        if write_options is None:
            return None
        if not isinstance(write_options, dict):
            return BuilderValidationError(
                "write_options must be an object/dict, got "
                f"{type(write_options).__name__}",
                error_code="DATABASE_WRITE_OPTIONS_VALIDATION_FAILED",
                field="write_options",
                hint=(
                    "Pass write_options as a JSON object with keys: "
                    f"{', '.join(sorted(WRITE_OPTIONS_ALLOWED_KEYS))}."
                ),
            )
        unknown = set(write_options.keys()) - WRITE_OPTIONS_ALLOWED_KEYS
        if unknown:
            offender = sorted(unknown)[0]
            return BuilderValidationError(
                f"write_options has unknown key {offender!r}",
                error_code="DATABASE_WRITE_OPTIONS_VALIDATION_FAILED",
                field=f"write_options.{offender}",
                hint=(
                    "Allowed write_options keys: "
                    f"{', '.join(sorted(WRITE_OPTIONS_ALLOWED_KEYS))}."
                ),
            )
        for snake, _camel, default in WRITE_OPTIONS_ATTR_ORDER:
            if snake not in write_options:
                continue
            value = write_options[snake]
            if isinstance(default, bool):
                if not isinstance(value, bool):
                    return BuilderValidationError(
                        f"write_options.{snake} must be a bool",
                        error_code="DATABASE_WRITE_OPTIONS_VALIDATION_FAILED",
                        field=f"write_options.{snake}",
                        hint="Use true or false.",
                    )
            else:  # str
                if not isinstance(value, str):
                    return BuilderValidationError(
                        f"write_options.{snake} must be a string",
                        error_code="DATABASE_WRITE_OPTIONS_VALIDATION_FAILED",
                        field=f"write_options.{snake}",
                        hint="Use a JSON string.",
                    )
        # Cross-field: writeSQLToFile=true needs a non-empty path.
        if write_options.get("write_sql_to_file") is True:
            path = write_options.get("sql_file_path")
            if path is None or not str(path).strip():
                return BuilderValidationError(
                    "write_options.sql_file_path is required when "
                    "write_sql_to_file=True",
                    error_code="DATABASE_WRITE_OPTIONS_VALIDATION_FAILED",
                    field="write_options.sql_file_path",
                    hint=(
                        "Provide a non-empty sql_file_path when "
                        "write_sql_to_file=True (e.g. 'tmp/sqldebug.txt')."
                    ),
                )
        return None

    @classmethod
    def _resolve_pooling(
        cls, pooling: Optional[Dict[str, Any]]
    ) -> Dict[str, Any]:
        """Return a fully-populated pooling dict, applying shape-appropriate defaults.

        When pooling is omitted or enabled=false, the defaults match issue #22
        XML byte-for-byte. When enabled=true and a key is omitted, falls back
        to the CDS reference defaults (max_active=-1, max_idle=-1, others
        unchanged).
        """
        config = pooling or {}
        enabled = bool(config.get("enabled", False))
        resolved: Dict[str, Any] = {"enabled": enabled}
        for snake, _camel, default_disabled, default_enabled in POOLING_ATTR_ORDER:
            if snake in config:
                resolved[snake] = config[snake]
            else:
                resolved[snake] = default_enabled if enabled else default_disabled
        return resolved

    @classmethod
    def _resolve_write_options(
        cls, write_options: Optional[Dict[str, Any]]
    ) -> Dict[str, Any]:
        """Return a fully-populated write_options dict with M2.2 defaults applied."""
        config = write_options or {}
        resolved: Dict[str, Any] = {}
        for snake, _camel, default in WRITE_OPTIONS_ATTR_ORDER:
            resolved[snake] = config[snake] if snake in config else default
        return resolved

    @classmethod
    def validate_config(cls, config: Dict[str, Any]) -> Optional[BuilderValidationError]:
        """Validate a database connector config without building XML.

        Returns the first BuilderValidationError encountered, or None when the
        config is acceptable. Stops on first error — matches the existing
        builder convention and keeps the error envelope simple.

        Used by both build() (which raises) and integration_builder._build_plan
        (which surfaces the structured error in the plan step). Callers that
        need ONLY the plaintext-secret check (independent of builder invocation)
        should use scan_forbidden_secret_fields directly.
        """
        # 1) Plaintext secret-shaped keys must never appear in caller config.
        secret_err = cls.scan_forbidden_secret_fields(config)
        if secret_err is not None:
            return secret_err

        # 2) driver_id presence + recognized.
        driver_id = config.get("driver_id") or ""
        supported_drivers = ", ".join(cls.SUPPORTED_DRIVER_IDS)
        if not driver_id:
            return BuilderValidationError(
                f"driver_id is required (supported: {supported_drivers})",
                error_code="UNSUPPORTED_DB_DRIVER",
                field="driver_id",
                hint=f"Supported driver_ids: {supported_drivers}.",
            )
        driver = cls._resolve_driver(driver_id)
        if driver is None:
            return BuilderValidationError(
                f"Unsupported driver_id {driver_id!r} "
                f"(supported: {supported_drivers})",
                error_code="UNSUPPORTED_DB_DRIVER",
                field="driver_id",
                hint=(
                    f"Supported driver_ids: {supported_drivers}. "
                    "Postgres and other JDBC families without a live #Common "
                    "reference export are deferred to later milestones."
                ),
            )

        # 2b) Safety net for future drivers that we register but cannot build
        # yet (none in the current registry — every entry is buildable=True
        # after Issue #31). The branch stays so we can land a recognized-but-
        # deferred driver later without losing the structured-error contract.
        if not driver.get("buildable", False):
            return BuilderValidationError(
                f"driver_id {driver_id!r} is recognized but not buildable yet",
                error_code="UNSUPPORTED_DB_DRIVER_SHAPE",
                field="driver_id",
                hint=driver.get(
                    "unsupported_reason",
                    "This driver shape is not yet implemented; use reuse "
                    "mode or raw-XML escape hatch.",
                ),
            )

        # 3) auth_mode presence + recognized.
        auth_mode = config.get("auth_mode") or ""
        supported_auth = ", ".join(cls.SUPPORTED_AUTH_MODES)
        if not auth_mode:
            return BuilderValidationError(
                f"auth_mode is required (supported: {supported_auth})",
                error_code="UNSUPPORTED_DB_AUTH_MODE",
                field="auth_mode",
                hint=f"Supported auth_modes: {supported_auth}.",
            )
        if auth_mode in cls.UNSUPPORTED_FUTURE_AUTH_MODES:
            return BuilderValidationError(
                f"auth_mode {auth_mode!r} is reserved for a future M2 iteration "
                "and not yet implemented",
                error_code="UNSUPPORTED_DB_AUTH_MODE",
                field="auth_mode",
                hint=(
                    f"Use auth_mode='username_password' for M2.2. "
                    f"{auth_mode!r} is reserved for a future iteration — it "
                    "requires a real Boomi XML reference and is deferred."
                ),
            )
        if auth_mode not in cls.SUPPORTED_AUTH_MODES:
            return BuilderValidationError(
                f"Unknown auth_mode {auth_mode!r} (supported: {supported_auth})",
                error_code="UNSUPPORTED_DB_AUTH_MODE",
                field="auth_mode",
                hint=f"Supported auth_modes: {supported_auth}.",
            )

        # 4) credential_ref required for username_password.
        if auth_mode == "username_password":
            credential_ref = config.get("credential_ref")
            if not credential_ref or not str(credential_ref).strip():
                return BuilderValidationError(
                    "credential_ref is required when auth_mode='username_password'",
                    error_code="MISSING_CREDENTIAL_REF",
                    field="credential_ref",
                    hint=(
                        "Pass credential_ref='credential://...' as an opaque "
                        "placeholder. The builder never writes it into XML — "
                        "Boomi password ciphertext is set via the UI after "
                        "create."
                    ),
                )

        # 5) Remaining required fields, keyed by driver shape.
        # The buildable check above guarantees driver["shape"] is in
        # REQUIRED_FIELDS_BY_SHAPE — callers can only reach here on a
        # buildable shape we model.
        shape = driver["shape"]
        required_fields = cls.REQUIRED_FIELDS_BY_SHAPE[shape]
        for required in required_fields:
            if required in ("driver_id", "auth_mode", "credential_ref"):
                continue  # already handled above
            value = config.get(required)
            if value is None or (isinstance(value, str) and not value.strip()):
                return BuilderValidationError(
                    f"{required} is required for database connectors",
                    error_code="DATABASE_CONNECTOR_VALIDATION_FAILED",
                    field=required,
                    hint=f"Provide a non-empty value for {required}.",
                )

        # 5b) Shape-specific forbidden fields. Reject UI-impossible combos
        # (e.g. host on a custom_url driver, or custom_class_name on a
        # host_port_db driver) before XML emission. Empty-string offenders
        # also fail — consistent with how required-field empty strings fail
        # above. None / missing keys are fine (those are simply not present).
        for forbidden in cls.FORBIDDEN_FIELDS_BY_SHAPE.get(shape, ()):  # noqa: E501
            if forbidden not in config:
                continue
            value = config[forbidden]
            if value is None:
                continue
            if isinstance(value, str) and not value.strip():
                continue  # an explicit empty-string carry-over is a no-op
            return BuilderValidationError(
                f"{forbidden!r} is not a valid field for the {shape!r} "
                f"driver shape (driver_id={driver_id!r})",
                error_code="DATABASE_CONNECTOR_VALIDATION_FAILED",
                field=forbidden,
                hint=(
                    f"Driver shape {shape!r} expects: "
                    f"{', '.join(cls.REQUIRED_FIELDS_BY_SHAPE[shape])}. "
                    f"Remove {forbidden!r} or pick a different driver_id."
                ),
            )

        # 5c) Drivers without a verified default_port (sap-hana) require the
        # caller to supply a non-empty port. host_port_db only — custom_url
        # forbids port entirely and is already caught by the forbidden-field
        # walker above.
        if shape == "host_port_db" and driver.get("default_port") is None:
            port_value = config.get("port")
            port_missing = (
                port_value is None
                or (isinstance(port_value, str) and not port_value.strip())
            )
            if port_missing:
                return BuilderValidationError(
                    f"port is required for driver_id={driver_id!r} "
                    "(no verified default port)",
                    error_code="DATABASE_CONNECTOR_VALIDATION_FAILED",
                    field="port",
                    hint=(
                        f"Supply an explicit port for {driver_id!r} — Boomi "
                        "does not assume a default for this driver."
                    ),
                )

        # 5d) Port type / format / range check (host_port_db). build() drops
        # `port` straight into an XML attribute via f-string, so an unescaped
        # string like `1433" injected="1` would inject extra attributes.
        # Restrict to int (non-bool) or all-digit string, and enforce TCP
        # port semantics (1..65535).
        #
        # Explicit null / blank-string is also rejected: build() uses
        # params.get('port', default), so passing None or "" keeps the value
        # (not the default) and emits port="None" or port="" in the XML —
        # invalid Boomi config. Callers should OMIT the key to use the
        # driver default, not pass null. SAP HANA's required-port check
        # (section 5c) handles its own case earlier.
        if shape == "host_port_db" and "port" in config:
            port_value = config["port"]
            # bool is a subclass of int — reject explicitly before int check.
            if isinstance(port_value, bool):
                return BuilderValidationError(
                    "port must be an integer or digit string, got bool",
                    error_code="DATABASE_CONNECTOR_VALIDATION_FAILED",
                    field="port",
                    hint="Use a JSON integer (e.g. 1433) or all-digit string.",
                )
            if port_value is None or (
                isinstance(port_value, str) and not port_value.strip()
            ):
                return BuilderValidationError(
                    "port must not be null or blank when supplied; omit the "
                    "key to use the driver default",
                    error_code="DATABASE_CONNECTOR_VALIDATION_FAILED",
                    field="port",
                    hint=(
                        f"Either omit the `port` key (uses default "
                        f"{driver.get('default_port')!r}) or pass a positive "
                        "integer / digit string in 1..65535."
                    ),
                )
            # Normalize to an int for range checking. Reject non-int types
            # and non-digit strings here so the int range check below is the
            # single source of truth for valid-range semantics.
            #
            # `isascii() and isdigit()` (not just isdigit()) — str.isdigit()
            # accepts non-ASCII digit-category chars like '²' (superscript)
            # and '２' (fullwidth). Some round-trip through int() (e.g.
            # int('２')=2), but build() emits the original caller string in
            # the XML attribute, so a validator/emission mismatch would
            # land non-ASCII glyphs in the connector XML. Restrict to
            # ASCII 0-9 only.
            if isinstance(port_value, int):
                port_int = port_value
            elif isinstance(port_value, str):
                stripped = port_value.strip()
                if not (stripped.isascii() and stripped.isdigit()):
                    return BuilderValidationError(
                        "port must be an integer or ASCII digit-only string "
                        "(1..65535), got non-ASCII-digit characters",
                        error_code="DATABASE_CONNECTOR_VALIDATION_FAILED",
                        field="port",
                        hint=(
                            "JDBC ports are positive integers (1..65535). "
                            "Use ASCII digits 0-9 only — non-ASCII digit "
                            "glyphs can also corrupt the emitted XML."
                        ),
                    )
                try:
                    port_int = int(stripped)
                except ValueError:
                    # Python 3.11+ caps int-string parsing at
                    # PYTHONINTMAXSTRDIGITS (default 4300). A digit string
                    # longer than that passes isdigit() but int() raises.
                    # Surface as a structured error to preserve the builder
                    # contract.
                    return BuilderValidationError(
                        "port string is too long to parse as an integer",
                        error_code="DATABASE_CONNECTOR_VALIDATION_FAILED",
                        field="port",
                        hint=(
                            "JDBC ports are 1..65535 (max 5 digits). "
                            "Inputs above that are invalid TCP ports."
                        ),
                    )
            else:
                return BuilderValidationError(
                    f"port must be an integer or digit string, got "
                    f"{type(port_value).__name__}",
                    error_code="DATABASE_CONNECTOR_VALIDATION_FAILED",
                    field="port",
                    hint="Use a JSON integer (e.g. 1433) or all-digit string.",
                )
            # TCP port range. Boomi can't connect to port 0 (OS-chosen, only
            # valid for listening) or to ports outside 1..65535.
            if port_int < 1 or port_int > 65535:
                return BuilderValidationError(
                    f"port {port_value!r} is outside the valid TCP range",
                    error_code="DATABASE_CONNECTOR_VALIDATION_FAILED",
                    field="port",
                    hint="JDBC ports must be in 1..65535 (inclusive).",
                )

        # 6) Optional pooling block.
        if "pooling" in config:
            pooling_err = cls.validate_pooling(config["pooling"])
            if pooling_err is not None:
                return pooling_err

        # 7) Optional write_options block.
        if "write_options" in config:
            write_err = cls.validate_write_options(config["write_options"])
            if write_err is not None:
                return write_err

        return None

    def build(self, **params) -> str:
        error = self.validate_config(params)
        if error is not None:
            raise error

        # validate_config guarantees driver is present, recognized, and
        # buildable. Shape-specific field assembly happens below; the outer
        # <bns:Component …> envelope and the WriteOptions/AdapterPoolInfo
        # blocks are identical across shapes.
        driver = self._resolve_driver(params["driver_id"])
        assert driver is not None  # narrowing for type checkers
        shape = driver["shape"]

        component_name = params["component_name"]
        username = params["username"]
        folder_name = params.get("folder_name", "Home")
        description = params.get("description", "")

        if shape == "host_port_db":
            host = params["host"]
            dbname = params["dbname"]
            port = params.get("port", driver["default_port"])
            additional = params.get("additional", "")
            class_name = driver["class_name"]
            url_format = driver["url_format"]
        elif shape == "custom_url":
            # Custom shape: caller supplies className + full JDBC URL.
            # host/port/dbname/additional are forbidden in the JSON contract
            # but the XML still emits them as empty strings to match the
            # live #Common Custom export byte-for-byte (component
            # 39fb519d-e970-4aaf-a1f7-4eba39158e9d on reneraai-5RO3DD).
            host = ""
            dbname = ""
            port = ""
            additional = ""
            class_name = params["custom_class_name"]
            url_format = params["connection_url"]
        else:  # pragma: no cover — validate_config guarantees a known shape
            raise BuilderValidationError(
                f"Internal: unknown driver shape {shape!r}",
                error_code="DATABASE_CONNECTOR_VALIDATION_FAILED",
                field="driver_id",
            )

        safe_name = _escape_xml(component_name)
        safe_folder = _escape_xml(folder_name)
        safe_desc = _escape_xml(description)
        safe_host = _escape_xml(host)
        safe_dbname = _escape_xml(dbname)
        safe_username = _escape_xml(username)
        safe_additional = _escape_xml(additional)
        safe_class_name = _escape_xml(class_name)
        safe_url_format = _escape_xml(url_format)
        # validate_config guarantees port is None / "" / int / digit-string;
        # _format_xml_value handles all three safely (bool→str, int→str,
        # str→escaped). Defense-in-depth against any future validator gap.
        safe_port = _format_xml_value(port) if port != "" else ""

        # Resolve optional pooling/write_options. Defaults preserve M2.2 XML
        # byte-for-byte when caller omits both keys.
        resolved_pooling = self._resolve_pooling(params.get("pooling"))
        resolved_write_options = self._resolve_write_options(
            params.get("write_options")
        )
        is_pool_enabled = _format_xml_value(resolved_pooling["enabled"])
        write_options_attrs = " ".join(
            f'{xml_attr}="{_format_xml_value(resolved_write_options[snake])}"'
            for snake, xml_attr, _default in WRITE_OPTIONS_ATTR_ORDER
        )
        adapter_pool_info_attrs = " ".join(
            f'{xml_attr}="{_format_xml_value(resolved_pooling[snake])}"'
            for snake, xml_attr, _d, _e in POOLING_ATTR_ORDER
        )

        return (
            '<?xml version="1.0" encoding="UTF-8"?>\n'
            '<bns:Component xmlns:bns="http://api.platform.boomi.com/"\n'
            '               xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance"\n'
            '               type="connector-settings" subType="database"\n'
            f'               name="{safe_name}"\n'
            f'               folderName="{safe_folder}">\n'
            '    <bns:encryptedValues>\n'
            '        <bns:encryptedValue path="//DatabaseConnectionSettings/@password" isSet="false"/>\n'
            '    </bns:encryptedValues>\n'
            f'    <bns:description>{safe_desc}</bns:description>\n'
            '    <bns:object>\n'
            f'        <DatabaseConnectionSettings xmlns="" additional="{safe_additional}"'
            f' className="{safe_class_name}"'
            f' dbname="{safe_dbname}"'
            f' driverId="{driver["driver_id"]}"'
            f' host="{safe_host}"'
            f' isPoolEnabled="{is_pool_enabled}"'
            f' port="{safe_port}"'
            f' urlFormat="{safe_url_format}"'
            f' username="{safe_username}">\n'
            f'            <WriteOptions {write_options_attrs}/>\n'
            f'            <AdapterPoolInfo {adapter_pool_info_attrs}/>\n'
            '        </DatabaseConnectionSettings>\n'
            '    </bns:object>\n'
            '</bns:Component>'
        )



class DatabaseGetOperationBuilder:
    """Builder for connector-action subType="database" Get operations.

    Issue #23 — M2.3. Emits a <DatabaseGetAction> envelope that references a
    pre-existing database Read profile via <ReadProfile profileId="..."/>.
    The profile ID is typically resolved upstream from a $ref:KEY token by
    integration_builder._resolve_dependency_tokens at apply time; the
    builder preserves whatever string the caller passes (UUID or $ref token).

    The referenced profile may be either a Select-statement Read profile
    (`profile_type="database.read"`) or a Stored Procedure Read profile
    (`profile_type="database.stored_procedure_read"`) — the Get-op XML
    envelope is identical in both cases. Live evidence: this builder's
    output byte-matches both work-profile 949b3239 (Select) and reneraai
    7a802447 (Stored Procedure) modulo identity attrs.

    Reference XML shape (work-profile c4b1f2b8 + 949b3239, fetched 2026-05-18):

        <bns:Component type="connector-action" subType="database" name="..." folderName="...">
          <bns:encryptedValues/>
          <bns:description>...</bns:description>
          <bns:object>
            <Operation xmlns="">
              <Archiving directory="" enabled="false"/>
              <Configuration>
                <DatabaseGetAction batchCount="0" maxRows="0">
                  <ReadProfile profileId="..."/>
                </DatabaseGetAction>
              </Configuration>
              <Tracking><TrackedFields/></Tracking>
              <Caching/>
            </Operation>
          </bns:object>
        </bns:Component>

    Config keys:
        component_name:    required for top-level component naming
        operation_mode:    required, must be "get". "send" is rejected with
                           UNSUPPORTED_DB_OPERATION_MODE (tracked by issue #32).
        read_profile_id:   required, the Boomi profile component ID OR a
                           "$ref:KEY" token (preserved verbatim — caller-side
                           resolution happens before build()).
        batch_count:       optional integer, defaults to 0 (no batching).
                           CDS-style large extracts use 50000.
        max_rows:          optional integer, defaults to 0 (no limit).
        folder_name:       optional; defaults to "Home".
        description:       optional.
        connection_ref_key, connector_type, component_type: caller-supplied
                           routing context not emitted into XML — Boomi binds
                           the connection at the process connector step, not
                           in the operation XML.
        link_element:      not yet supported; UNSUPPORTED_DB_GET_FIELD until
                           live XML shape is confirmed.
    """

    SUPPORTED_OPERATION_MODES = ("get",)
    UNSUPPORTED_OPERATION_MODES = ("send",)
    DEFAULT_BATCH_COUNT = 0
    DEFAULT_MAX_ROWS = 0
    # Defensive consistency — no secrets are expected in a Get op config, but
    # mirror the scan so integration_builder preflight is uniform.
    FORBIDDEN_SECRET_FIELDS = DatabaseConnectorBuilder.FORBIDDEN_SECRET_FIELDS

    @classmethod
    def scan_forbidden_secret_fields(
        cls, config: Any, _path_prefix: str = ""
    ) -> Optional[BuilderValidationError]:
        """Reuse DatabaseConnectorBuilder's scan — same forbidden-key set."""
        return DatabaseConnectorBuilder.scan_forbidden_secret_fields(
            config, _path_prefix=_path_prefix
        )

    @classmethod
    def redact_forbidden_secret_fields_in_place(cls, config: Any) -> None:
        DatabaseConnectorBuilder.redact_forbidden_secret_fields_in_place(config)

    @classmethod
    def validate_config(cls, config: Dict[str, Any]) -> Optional[BuilderValidationError]:
        """Validate a Get operation config without building XML."""
        # 1) Plaintext secret-shaped keys (defensive).
        secret_err = cls.scan_forbidden_secret_fields(config)
        if secret_err is not None:
            return secret_err

        # 2) operation_mode must be 'get'; 'send' is explicit issue-#32 deferral.
        operation_mode = (config.get("operation_mode") or "").lower()
        if operation_mode in cls.UNSUPPORTED_OPERATION_MODES:
            return BuilderValidationError(
                f"operation_mode={operation_mode!r} is not supported in issue #23",
                error_code="UNSUPPORTED_DB_OPERATION_MODE",
                field="operation_mode",
                hint=(
                    "Database Send/write operations require WriteProfile and "
                    "DatabaseSendAction support, tracked separately by issue "
                    "#32. Use operation_mode='get' for read extractions."
                ),
            )
        if operation_mode not in cls.SUPPORTED_OPERATION_MODES:
            supported = ", ".join(cls.SUPPORTED_OPERATION_MODES)
            return BuilderValidationError(
                f"operation_mode is required and must be one of: {supported}",
                error_code="UNSUPPORTED_DB_OPERATION_MODE",
                field="operation_mode",
                hint=f"Supported operation_modes: {supported}.",
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

        # 4) read_profile_id required and non-empty.
        read_profile_id = config.get("read_profile_id")
        if read_profile_id is None or not str(read_profile_id).strip():
            return BuilderValidationError(
                "read_profile_id is required for database Get operations",
                error_code="MISSING_DB_READ_PROFILE_REF",
                field="read_profile_id",
                hint=(
                    "Provide either a Boomi profile component ID (UUID) or a "
                    "'$ref:KEY' token that resolves to the read profile "
                    "created earlier in the integration plan."
                ),
            )

        # 5) batch_count and max_rows must be non-negative integers when present.
        for key in ("batch_count", "max_rows"):
            if key in config:
                value = config[key]
                if isinstance(value, bool) or not isinstance(value, int) or value < 0:
                    return BuilderValidationError(
                        f"{key} must be a non-negative integer",
                        error_code="DATABASE_OPERATION_VALIDATION_FAILED",
                        field=key,
                        hint=(
                            f"Use a JSON integer >= 0. Default for {key} is "
                            f"{cls.DEFAULT_BATCH_COUNT if key == 'batch_count' else cls.DEFAULT_MAX_ROWS}."
                        ),
                    )

        # 6) link_element rejected until live XML shape is confirmed (plan note).
        if "link_element" in config:
            return BuilderValidationError(
                "link_element is not yet supported in the database Get operation builder",
                error_code="UNSUPPORTED_DB_GET_FIELD",
                field="link_element",
                hint=(
                    "Link Element splits/groups documents per Boomi docs, but "
                    "its live XML attribute name has not been verified. Omit "
                    "for now; the field will be added when a verified XML "
                    "reference is available."
                ),
            )

        return None

    def build(self, **params) -> str:
        error = self.validate_config(params)
        if error is not None:
            raise error

        component_name = params["component_name"]
        read_profile_id = params["read_profile_id"]
        batch_count = params.get("batch_count", self.DEFAULT_BATCH_COUNT)
        max_rows = params.get("max_rows", self.DEFAULT_MAX_ROWS)
        folder_name = params.get("folder_name", "Home")
        description = params.get("description", "")

        safe_name = _escape_xml(component_name)
        safe_folder = _escape_xml(folder_name)
        safe_desc = _escape_xml(description)
        safe_profile_id = _escape_xml(str(read_profile_id))

        return (
            '<?xml version="1.0" encoding="UTF-8"?>\n'
            '<bns:Component xmlns:bns="http://api.platform.boomi.com/"\n'
            '               xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance"\n'
            '               type="connector-action" subType="database"\n'
            f'               name="{safe_name}"\n'
            f'               folderName="{safe_folder}">\n'
            '    <bns:encryptedValues/>\n'
            f'    <bns:description>{safe_desc}</bns:description>\n'
            '    <bns:object>\n'
            '        <Operation xmlns="">\n'
            '            <Archiving directory="" enabled="false"/>\n'
            '            <Configuration>\n'
            f'                <DatabaseGetAction batchCount="{batch_count}" maxRows="{max_rows}">\n'
            f'                    <ReadProfile profileId="{safe_profile_id}"/>\n'
            '                </DatabaseGetAction>\n'
            '            </Configuration>\n'
            '            <Tracking><TrackedFields/></Tracking>\n'
            '            <Caching/>\n'
            '        </Operation>\n'
            '    </bns:object>\n'
            '</bns:Component>'
        )


# ============================================================================
# REST Client connector (issue #24, M2.4)
#
# Subtype: officialboomi-X3979C-rest-prod (mixed-case is the Boomi canonical
# form; registry keys are lowercased so caller aliases "rest" / "rest_client"
# / the literal subtype all resolve correctly).
#
# Live reference exports (RenEra account, fetched 2026-05-23):
#   - d6ee8b5b-6d83-44c0-9e77-216a60adb452 ([OAuth2 client_credentials] connection)
#   - e268ea19-bbbe-4e1f-b406-b5129358575a ([Rest Test GET] operation)
#   - 64c4eafd-f2e7-49e2-b128-c9b1c50f81b9 ([Rest Test PATCH] operation)
# ============================================================================

REST_CLIENT_SUBTYPE = "officialboomi-X3979C-rest-prod"
_REST_CLIENT_ALIASES = ("rest", "rest_client", REST_CLIENT_SUBTYPE.lower())

# AWS skeleton fields emitted verbatim with empty values when AWS auth is
# unused — matches the live OAuth2 export which still carries the empty
# AWS slot. Order is the verified live-XML order.
_REST_CLIENT_AWS_FIELDS = (
    ("awsAccessKey", "string"),
    ("awsSecretKey", "password"),
    ("awsService", "string"),
    ("customAwsService", "string"),
    ("awsRegion", "string"),
    ("customAwsRegion", "string"),
    ("awsProfileArn", "string"),
    ("awsRoleArn", "string"),
    ("awsTrustAnchorArn", "string"),
    ("awsRolesAnywhereRegion", "string"),
    ("awsRolesAnywhereCustomRegion", "string"),
    ("awsSessionName", "string"),
    ("awsDuration", "integer"),
)


def _resolve_rest_connector_type(value: Any) -> Optional[str]:
    """Map any of `rest`, `rest_client`, or the canonical REST Client subtype
    (case-insensitive) to the canonical mixed-case subtype. Returns None for
    anything else."""
    if not isinstance(value, str):
        return None
    normalized = value.strip().lower()
    if normalized in _REST_CLIENT_ALIASES:
        return REST_CLIENT_SUBTYPE
    return None


class RestClientConnectionBuilder:
    """Builder for Boomi REST Client connector-settings (issue #24, M2.4).

    Buildable auth modes (verified against live RenEra exports):
      * NONE                            (verified 7f7e0730 / 499e5bd6)
      * BASIC                           (verified 587b5fe0)
      * NTLM                            (verified 1de43085)
      * OAUTH2 client_credentials       (verified d6ee8b5b)
      * OAUTH2 authorization_code       (token-not-set; verified 7abf0ad2)

    Deferred — return UNSUPPORTED_REST_AUTH_MODE until a verified live
    export exists for each:
      * CUSTOM, PASSWORD_DIGEST
      * AWS_SIGNATURE, AWS_IAM_ROLES_ANYWHERE
      * OAUTH2 resource_owner_credentials, jwt_bearer
      * OAUTH2 authorization_code with cached access-token emission

    The emitted XML matches the corresponding live shape byte-for-byte
    modulo identity attributes and secret-bearing fields. See
    `_REST_SENSITIVE_FIELD_PATHS` in `integration_builder.py` for the
    plan-output redaction sweep that scrubs raw secret material on
    validation errors.

    Config keys:
        connector_type:           "rest" | "rest_client" | the subtype.
                                  Consumed by the dispatcher; the builder
                                  itself does not require it.
        component_name:           required.
        base_url:                 required; must begin with http:// or https://.
        auth:                     required; one of NONE / BASIC / NTLM / OAUTH2.
        username:                 required when auth in (BASIC, NTLM); rejected
                                  with REST_CONNECTOR_VALIDATION_FAILED for
                                  other auths.
        credential_ref:           required when auth in (BASIC, NTLM); must
                                  start with "credential://"; rejected for
                                  other auths.
        domain / workstation:     required when auth=NTLM; rejected for other
                                  auths.
        preemptive:               optional bool for auth in (BASIC, OAUTH2)
                                  per Boomi docs; rejected for other auths.
        private_certificate_ref:  optional; Boomi component id (UUID). Works
                                  with any auth (client-cert is an
                                  independent option).
        public_certificate_ref:   optional; same shape and rules.
        oauth2:                   required when auth=OAUTH2.
            grant_type:           required; one of
                                  client_credentials / authorization_code
                                  (alias "code" → authorization_code).
            client_id:            required.
            client_secret_ref:    required; must start with "credential://".
                                  Builder NEVER emits the actual secret —
                                  it goes into the Boomi UI after create.
            access_token_url:     required; endpoint for token issuance.
            authorization_url:    required when grant_type=authorization_code;
                                  rejected for client_credentials.
            scope:                optional string.
            credentials_assertion_type: optional, defaults "client_secret".
            authorization_parameters / access_token_parameters: deferred —
                                  rejected non-empty with
                                  UNSUPPORTED_REST_OAUTH2_PARAMETERS.
            access_token / cached_token: always rejected
                                  (REST_SECRET_VALUE_FORBIDDEN) — the
                                  builder emits token-not-set only.
        connect_timeout_ms:       optional int, defaults -1 (wait forever).
        read_timeout_ms:          optional int, defaults -1.
        cookie_scope:             optional, defaults "GLOBAL".
        connection_pooling:       optional dict {enabled?, max_total?,
                                  idle_timeout_seconds?}; defaults
                                  enabled=False. max_total and
                                  idle_timeout_seconds are rejected with
                                  REST_POOLING_INVALID unless enabled=True.
        folder_name / description: optional component-level fields.
    """

    SUPPORTED_AUTH_MODES = ("NONE", "BASIC", "NTLM", "OAUTH2")
    _PASSWORD_BACKED_AUTH_MODES = ("BASIC", "NTLM")

    # Auth modes for which the `preemptive` flag is meaningful. Per Boomi docs
    # ("Applicable for Basic and OAuth 2.0 authentication"), the field only
    # controls behavior for BASIC and OAUTH2 — for NONE / NTLM / CUSTOM /
    # PASSWORD_DIGEST / AWS_* it is silently ignored at runtime, so a
    # caller-supplied value for those auths is treated as a stale-config
    # mistake (validation rejects it up front).
    _PREEMPTIVE_AUTH_MODES = ("BASIC", "OAUTH2")
    RECOGNIZED_AUTH_MODES = (
        "NONE",
        "AWS_SIGNATURE",
        "BASIC",
        "CUSTOM",
        "PASSWORD_DIGEST",
        "NTLM",
        "OAUTH2",
        "AWS_IAM_ROLES_ANYWHERE",
    )
    BUILDABLE_OAUTH2_GRANT_TYPES = ("client_credentials", "authorization_code")
    # Public-facing aliases for grant_type — caller may pass either "code"
    # (the live XML grantType attribute value) or "authorization_code" (the
    # canonical public name). Both normalize to "authorization_code" for
    # builder bookkeeping and emit grantType="code" in XML.
    _OAUTH2_GRANT_TYPE_ALIASES = {
        "code": "authorization_code",
        "authorization_code": "authorization_code",
        "client_credentials": "client_credentials",
    }
    _CERT_REF_FIELDS = ("private_certificate_ref", "public_certificate_ref")
    # Boomi certificate component ids are UUIDs (canonical 8-4-4-4-12 hex
    # form, case-insensitive). Codex round-3 P2 #2: enforce this shape so
    # a caller accidentally pasting PEM/SSH-key content can't sneak the
    # raw key material into emitted XML.
    _BOOMI_COMPONENT_ID_RE = re.compile(
        r"^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$",
        re.IGNORECASE,
    )
    FORBIDDEN_SECRET_FIELDS = DatabaseConnectorBuilder.FORBIDDEN_SECRET_FIELDS

    @classmethod
    def scan_forbidden_secret_fields(
        cls, config: Any, _path_prefix: str = ""
    ) -> Optional[BuilderValidationError]:
        """Reuse DatabaseConnectorBuilder's recursive scan — same forbidden
        key set, and it descends into nested dicts (e.g. ``oauth2``)."""
        return DatabaseConnectorBuilder.scan_forbidden_secret_fields(
            config, _path_prefix=_path_prefix
        )

    @classmethod
    def redact_forbidden_secret_fields_in_place(cls, config: Any) -> None:
        DatabaseConnectorBuilder.redact_forbidden_secret_fields_in_place(config)

    @classmethod
    def validate_config(cls, config: Dict[str, Any]) -> Optional[BuilderValidationError]:
        """Validate a REST Client connector config without building XML."""
        # 1) Plaintext secret-shaped keys (including nested oauth2.password etc.).
        secret_err = cls.scan_forbidden_secret_fields(config)
        if secret_err is not None:
            return secret_err

        # 2) component_name required.
        component_name = config.get("component_name")
        if not component_name or not str(component_name).strip():
            return BuilderValidationError(
                "component_name is required for REST Client connectors",
                error_code="REST_CONNECTOR_VALIDATION_FAILED",
                field="component_name",
                hint="Provide a non-empty component_name string.",
            )

        # 3) base_url required and well-formed.
        base_url = config.get("base_url")
        if not base_url or not str(base_url).strip():
            return BuilderValidationError(
                "base_url is required for REST Client connectors",
                error_code="REST_BASE_URL_REQUIRED",
                field="base_url",
                hint=(
                    "Provide the REST API base URL beginning with http:// "
                    "or https://. The operation step appends the path."
                ),
            )
        base_url_str = str(base_url).strip()
        if not (base_url_str.startswith("http://") or base_url_str.startswith("https://")):
            return BuilderValidationError(
                f"base_url {base_url_str!r} must begin with http:// or https://",
                error_code="REST_BASE_URL_INVALID",
                field="base_url",
                hint="REST Client requires http or https schemes only.",
            )

        # 4) auth mode gating.
        auth = config.get("auth")
        if not auth:
            return BuilderValidationError(
                "auth is required for REST Client connectors",
                error_code="UNSUPPORTED_REST_AUTH_MODE",
                field="auth",
                hint=(
                    f"Supported auth modes: {', '.join(cls.SUPPORTED_AUTH_MODES)}. "
                    "REST Client recognizes other modes "
                    f"({', '.join(m for m in cls.RECOGNIZED_AUTH_MODES if m not in cls.SUPPORTED_AUTH_MODES)}) "
                    "but they are deferred until verified live exports exist."
                ),
            )
        if auth not in cls.SUPPORTED_AUTH_MODES:
            return BuilderValidationError(
                f"auth_type {auth!r} is not buildable in issue #24",
                error_code="UNSUPPORTED_REST_AUTH_MODE",
                field="auth",
                hint=(
                    f"Supported auth modes: {', '.join(cls.SUPPORTED_AUTH_MODES)}. "
                    "Recognized but deferred: "
                    f"{', '.join(m for m in cls.RECOGNIZED_AUTH_MODES if m not in cls.SUPPORTED_AUTH_MODES)}. "
                    "Use the raw-XML escape hatch (config.xml=...) sourced "
                    "from a verified live export for unsupported modes."
                ),
            )

        # 5) OAuth2 handling. Two gates: (5a) reject a stale oauth2 block
        # supplied with a non-OAUTH2 auth mode — without this gate, a typo
        # like `auth='NONE'` + `oauth2={"client_secret_ref": "raw-secret"}`
        # would leak the raw value through the plan echo because the
        # OAuth2 validator below is gated on auth=='OAUTH2' and the
        # _REST_SENSITIVE_FIELD_PATHS sweep only fires after a REST
        # validation error. (5b) the standard OAuth2 sub-block validation.
        #
        # The stale-block check uses a truthy test (not isinstance(dict)) so
        # malformed payloads — `oauth2="raw"`, `oauth2=["raw"]`, `oauth2=42` —
        # are also rejected. Empty dict / None / "" remain treated as "not
        # supplied" because Python's truthy semantics already exclude them.
        oauth2 = config.get("oauth2")
        if auth != "OAUTH2" and oauth2:
            return BuilderValidationError(
                f"`oauth2` sub-block is only valid with auth='OAUTH2', not "
                f"auth={auth!r}",
                error_code="REST_CONNECTOR_VALIDATION_FAILED",
                field="oauth2",
                hint=(
                    "Remove the oauth2 block or switch auth='OAUTH2'. A "
                    "stale oauth2 block with auth='NONE'/'BASIC'/'NTLM' "
                    "is always a config mistake — Boomi ignores it but "
                    "the raw client_secret_ref would otherwise echo into "
                    "the plan output."
                ),
            )
        if auth == "OAUTH2":
            if not isinstance(oauth2, dict):
                return BuilderValidationError(
                    "oauth2 sub-block is required when auth='OAUTH2'",
                    error_code="REST_CONNECTOR_VALIDATION_FAILED",
                    field="oauth2",
                    hint=(
                        "Provide oauth2 as an object: "
                        "{grant_type, client_id, client_secret_ref, "
                        "access_token_url, scope?, credentials_assertion_type?}."
                    ),
                )
            grant_input = oauth2.get("grant_type")
            # Guard the alias-dict lookup: dict.get(unhashable) raises
            # TypeError. A non-string grant_type is always invalid — surface
            # it as UNSUPPORTED_REST_AUTH_MODE instead of crashing.
            if not isinstance(grant_input, str):
                supported = ", ".join(cls.BUILDABLE_OAUTH2_GRANT_TYPES)
                return BuilderValidationError(
                    f"oauth2.grant_type must be a string (got "
                    f"{type(grant_input).__name__}); supported: {supported}",
                    error_code="UNSUPPORTED_REST_AUTH_MODE",
                    field="oauth2.grant_type",
                    hint=(
                        f"Supported OAuth2 grant types: {supported}. Pass "
                        "'code' or 'authorization_code' as aliases for the "
                        "authorization_code grant."
                    ),
                )
            canonical_grant = cls._OAUTH2_GRANT_TYPE_ALIASES.get(grant_input)
            if canonical_grant not in cls.BUILDABLE_OAUTH2_GRANT_TYPES:
                supported = ", ".join(cls.BUILDABLE_OAUTH2_GRANT_TYPES)
                return BuilderValidationError(
                    f"oauth2.grant_type {grant_input!r} is not buildable "
                    f"(supported: {supported})",
                    error_code="UNSUPPORTED_REST_AUTH_MODE",
                    field="oauth2.grant_type",
                    hint=(
                        f"Supported OAuth2 grant types: {supported}. Other "
                        "grants (resource_owner_credentials, jwt_bearer, and "
                        "authorization_code with cached access-token "
                        "emission) are deferred until verified live exports "
                        "exist. Pass 'code' or 'authorization_code' as "
                        "aliases for the authorization_code grant."
                    ),
                )
            # Common required subfields across both grants.
            required_subfields = ["client_id", "client_secret_ref", "access_token_url"]
            if canonical_grant == "authorization_code":
                required_subfields.append("authorization_url")
            for required_subfield in required_subfields:
                value = oauth2.get(required_subfield)
                if value is None or not str(value).strip():
                    return BuilderValidationError(
                        f"oauth2.{required_subfield} is required when "
                        f"auth='OAUTH2' / grant_type={canonical_grant!r}",
                        error_code="REST_CONNECTOR_VALIDATION_FAILED",
                        field=f"oauth2.{required_subfield}",
                        hint=(
                            "Provide an opaque credential reference: "
                            "'credential://<vendor>/<role>'."
                            if required_subfield == "client_secret_ref"
                            else f"Provide a non-empty oauth2.{required_subfield}."
                        ),
                    )
            secret_ref = str(oauth2.get("client_secret_ref", "")).strip()
            if not secret_ref.startswith("credential://"):
                return BuilderValidationError(
                    "oauth2.client_secret_ref must begin with 'credential://'",
                    error_code="REST_SECRET_VALUE_FORBIDDEN",
                    field="oauth2.client_secret_ref",
                    hint=(
                        "Pass an opaque credential_ref string; the builder "
                        "never writes raw secret values into XML."
                    ),
                )
            # 5c) Stale `oauth2.authorization_url` gate. The authorization URL
            # is the end-user consent endpoint used during the OAuth 2.0
            # authorization-code flow. For client_credentials (machine-to-
            # machine, no user) the field is meaningless — supplying it would
            # otherwise be silently dropped at build time, where the
            # client_credentials emission branch ignores it.
            #
            # Truthy + non-blank-string check so empty / None / whitespace
            # are treated as "not supplied" and malformed payloads
            # (`authorization_url=["url"]`) are still rejected.
            if canonical_grant == "client_credentials":
                auth_url = oauth2.get("authorization_url")
                is_blank_str = isinstance(auth_url, str) and not auth_url.strip()
                if auth_url and not is_blank_str:
                    return BuilderValidationError(
                        "oauth2.authorization_url is only valid with "
                        "grant_type='authorization_code', not "
                        "grant_type='client_credentials'",
                        error_code="REST_CONNECTOR_VALIDATION_FAILED",
                        field="oauth2.authorization_url",
                        hint=(
                            "Remove oauth2.authorization_url or switch "
                            "to grant_type='authorization_code'. The "
                            "authorization endpoint is the end-user "
                            "consent URL — client_credentials is a "
                            "machine-to-machine flow with no user."
                        ),
                    )
            # 5d) oauth2 parameter blocks (authorization_parameters and
            # access_token_parameters) are deferred — the build path
            # always emits empty `<authorizationParameters/>` and
            # `<accessTokenParameters/>` elements. Rather than silently
            # drop caller-supplied values, reject so the caller knows the
            # input doesn't take effect. Empty container / None / "" are
            # accepted (treated as "not supplied"); only truthy values
            # trigger the gate.
            for param_field in ("authorization_parameters", "access_token_parameters"):
                value = oauth2.get(param_field)
                if value:
                    return BuilderValidationError(
                        f"oauth2.{param_field} emission is deferred "
                        "until a verified live export shows the shape",
                        error_code="UNSUPPORTED_REST_OAUTH2_PARAMETERS",
                        field=f"oauth2.{param_field}",
                        hint=(
                            f"Remove oauth2.{param_field} or supply an "
                            "empty container. The builder currently "
                            "emits empty parameter elements regardless "
                            "of caller input; non-empty values would be "
                            "silently dropped without this gate."
                        ),
                    )

        # 5b) Password-backed auth modes (BASIC) require username + credential_ref.
        # credential_ref carries the Boomi credential URL (the actual password
        # is stored as ciphertext via the UI after create or via a
        # pre-encrypted raw-XML payload).
        if auth in cls._PASSWORD_BACKED_AUTH_MODES:
            username = config.get("username")
            if not isinstance(username, str) or not username.strip():
                return BuilderValidationError(
                    f"username is required (non-empty string) when auth={auth!r}",
                    error_code="REST_CONNECTOR_VALIDATION_FAILED",
                    field="username",
                    hint=(
                        f"Provide username as a non-empty string. {auth} auth "
                        "sends `<username>:<password>` in the Authorization "
                        "header; the password is supplied via the Boomi UI "
                        "after create."
                    ),
                )
            credential_ref = config.get("credential_ref")
            if not credential_ref or not str(credential_ref).strip():
                return BuilderValidationError(
                    f"credential_ref is required when auth={auth!r}",
                    error_code="REST_CONNECTOR_VALIDATION_FAILED",
                    field="credential_ref",
                    hint=(
                        "Pass credential_ref='credential://<vendor>/<role>' "
                        "as an opaque credential reference. Boomi stores the "
                        "actual password as ciphertext."
                    ),
                )
            cred_ref_str = str(credential_ref).strip()
            if not cred_ref_str.startswith("credential://"):
                return BuilderValidationError(
                    "credential_ref must begin with 'credential://'",
                    error_code="REST_SECRET_VALUE_FORBIDDEN",
                    field="credential_ref",
                    hint=(
                        "Pass an opaque credential_ref string starting with "
                        "'credential://'; the builder never writes raw "
                        "secret values into XML."
                    ),
                )

        # 5c) NTLM-specific fields. NTLM uses domain + workstation alongside
        # the BASIC-style username + credential_ref. Verified against live
        # REST NTLM (1de43085).
        if auth == "NTLM":
            for ntlm_field in ("domain", "workstation"):
                value = config.get(ntlm_field)
                if not isinstance(value, str) or not value.strip():
                    return BuilderValidationError(
                        f"{ntlm_field} is required (non-empty string) "
                        f"when auth='NTLM'",
                        error_code="REST_CONNECTOR_VALIDATION_FAILED",
                        field=ntlm_field,
                        hint=(
                            f"NTLM requires {ntlm_field}. domain is the AD "
                            "domain (e.g. corp.example.com); workstation is "
                            "the client machine identity sent during the "
                            "challenge-response handshake."
                        ),
                    )

        # 5d) Stale `credential_ref` gate. credential_ref is BASIC/NTLM only
        # (the password-backed branch above consumes it). For NONE / OAUTH2,
        # a non-empty credential_ref is always a config mistake — and like
        # the stale oauth2 block, the raw value would otherwise leak into
        # the plan echo before _REST_SENSITIVE_FIELD_PATHS could redact it.
        #
        # Truthy check (not isinstance(str)) so malformed payloads like
        # `credential_ref=["raw"]` or `credential_ref={"value":"raw"}` are
        # also rejected. None / "" / empty container remain treated as
        # "not supplied" via Python's truthy semantics.
        cred_ref = config.get("credential_ref")
        is_blank_string = isinstance(cred_ref, str) and not cred_ref.strip()
        if (
            auth not in cls._PASSWORD_BACKED_AUTH_MODES
            and cred_ref
            and not is_blank_string
        ):
            return BuilderValidationError(
                f"`credential_ref` is only valid with auth='BASIC' or "
                f"auth='NTLM', not auth={auth!r}",
                error_code="REST_CONNECTOR_VALIDATION_FAILED",
                field="credential_ref",
                hint=(
                    "Remove credential_ref or switch to a password-backed "
                    "auth mode. credential_ref carries the BASIC/NTLM "
                    "password — OAUTH2 uses oauth2.client_secret_ref "
                    "instead, and NONE/cert auth modes need no password."
                ),
            )

        # 5e) Stale `username` gate. username is BASIC/NTLM-only (the
        # password-backed branch above consumes it). For NONE / OAUTH2,
        # supplying a non-empty username is a config mistake — Boomi
        # ignores the field at runtime, but the value would otherwise be
        # emitted in the XML where it misleads downstream consumers and
        # confuses callers.
        #
        # Truthy check (not isinstance(str)) so malformed payloads like
        # `username=["alice"]` are also rejected. None / "" / whitespace-only
        # / empty container remain treated as "not supplied".
        uname = config.get("username")
        is_blank_string_uname = isinstance(uname, str) and not uname.strip()
        if (
            auth not in cls._PASSWORD_BACKED_AUTH_MODES
            and uname
            and not is_blank_string_uname
        ):
            return BuilderValidationError(
                f"`username` is only valid with auth='BASIC' or "
                f"auth='NTLM', not auth={auth!r}",
                error_code="REST_CONNECTOR_VALIDATION_FAILED",
                field="username",
                hint=(
                    "Remove username or switch to a password-backed auth "
                    "mode (BASIC or NTLM). For NONE / OAUTH2 the username "
                    "field has no semantic effect — supplying it leaks "
                    "caller intent into the XML without changing behavior."
                ),
            )

        # 5f) Stale `domain` / `workstation` gate. Both fields are NTLM-only
        # per Boomi docs and live exports (verified against 1de43085).
        # Supplying either with NONE / BASIC / OAUTH2 is always a config
        # mistake — Boomi ignores them at runtime but they would otherwise
        # be emitted in the XML.
        if auth != "NTLM":
            for ntlm_field in ("domain", "workstation"):
                value = config.get(ntlm_field)
                is_blank_str = isinstance(value, str) and not value.strip()
                if value and not is_blank_str:
                    return BuilderValidationError(
                        f"`{ntlm_field}` is only valid with auth='NTLM', "
                        f"not auth={auth!r}",
                        error_code="REST_CONNECTOR_VALIDATION_FAILED",
                        field=ntlm_field,
                        hint=(
                            f"Remove {ntlm_field} or switch auth='NTLM'. "
                            "domain + workstation are NTLM-only fields "
                            "used during the challenge-response handshake; "
                            "they have no semantic effect for any other "
                            "auth mode."
                        ),
                    )

        # 5g) Stale `preemptive` gate. Per Boomi docs the flag is
        # "applicable for Basic and OAuth 2.0 authentication" — for any
        # other auth mode it has no semantic effect. Supplying it with
        # NONE / NTLM (or any future non-applicable auth) is a config
        # mistake.
        #
        # Presence-check (not truthy) because False is also a
        # caller-supplied value with intent (Boomi docs distinguish
        # "selected" vs "cleared"). None is treated as "not supplied"
        # for consistency with the other stale gates.
        if (
            "preemptive" in config
            and config["preemptive"] is not None
            and auth not in cls._PREEMPTIVE_AUTH_MODES
        ):
            return BuilderValidationError(
                f"`preemptive` is only valid with auth='BASIC' or "
                f"auth='OAUTH2', not auth={auth!r}",
                error_code="REST_CONNECTOR_VALIDATION_FAILED",
                field="preemptive",
                hint=(
                    "Remove preemptive or switch to BASIC / OAUTH2. Per "
                    "Boomi docs the flag is applicable only for Basic "
                    "and OAuth 2.0 authentication — for any other auth "
                    "mode it has no semantic effect."
                ),
            )

        # 6) Optional preemptive flag must be a bool when supplied. None
        # is treated as "not supplied" (consistent with the other
        # optional-field gates).
        if (
            "preemptive" in config
            and config["preemptive"] is not None
            and not isinstance(config["preemptive"], bool)
        ):
            return BuilderValidationError(
                "preemptive must be a bool",
                error_code="REST_CONNECTOR_VALIDATION_FAILED",
                field="preemptive",
                hint="Use true or false (Python True/False).",
            )

        # 7) Optional timeouts must be integers (negative or zero means
        # "wait indefinitely" per Boomi docs).
        for timeout_field in ("connect_timeout_ms", "read_timeout_ms"):
            if timeout_field in config:
                value = config[timeout_field]
                if isinstance(value, bool) or not isinstance(value, int):
                    return BuilderValidationError(
                        f"{timeout_field} must be an integer (negative or zero "
                        "means wait indefinitely)",
                        error_code="REST_CONNECTOR_VALIDATION_FAILED",
                        field=timeout_field,
                        hint="Use a JSON integer (e.g. -1 for unbounded wait).",
                    )

        # 8) Optional connection_pooling block.
        pooling_err = cls._validate_connection_pooling(config.get("connection_pooling"))
        if pooling_err is not None:
            return pooling_err

        # 9) Optional client-cert reference fields. Independent of auth mode
        # (per the live REST Certificate export, which carries auth=NONE plus
        # populated privateCertificate/publicCertificate refs — but cert refs
        # may co-occur with any auth selection).
        #
        # Codex round-3 P2 #2: a cert ref must be a Boomi component-id GUID,
        # not raw PEM/key material. The previous validator only checked
        # `isinstance(value, str)`, so PEM/SSH-key content would pass and
        # be emitted verbatim as the field value — leaking key material
        # into the XML and the plan output. Reject anything that isn't
        # a UUID-shaped string.
        for cert_field in cls._CERT_REF_FIELDS:
            if cert_field in config and config[cert_field] not in (None, ""):
                value = config[cert_field]
                if not isinstance(value, str):
                    return BuilderValidationError(
                        f"{cert_field} must be a string (Boomi certificate "
                        f"component id), got {type(value).__name__}",
                        error_code="REST_CONNECTOR_VALIDATION_FAILED",
                        field=cert_field,
                        hint=(
                            "Pass the Boomi certificate component id (GUID "
                            "string) for the X509 client cert. Cert refs work "
                            "with any auth mode — they are an independent "
                            "client-cert option, not tied to auth=NONE."
                        ),
                    )
                if not cls._BOOMI_COMPONENT_ID_RE.match(value.strip()):
                    return BuilderValidationError(
                        f"{cert_field} must be a Boomi certificate component "
                        "id (canonical UUID 8-4-4-4-12 hex form). The supplied "
                        f"value is not GUID-shaped (length={len(value)}).",
                        error_code="REST_CONNECTOR_VALIDATION_FAILED",
                        field=cert_field,
                        hint=(
                            "Pass the GUID-shaped Boomi component id (e.g. "
                            "'21f598a6-1d90-4578-a35a-d0350c50b747') — NOT "
                            "PEM content, NOT SSH key material, NOT a "
                            "credential:// reference. Create the certificate "
                            "component in Boomi UI first, then reference its "
                            "component id here."
                        ),
                    )

        return None

    _POOLING_ALLOWED_KEYS = frozenset({
        "enabled",
        "max_total",
        "idle_timeout_seconds",
    })

    @classmethod
    def _validate_connection_pooling(
        cls, pooling: Any
    ) -> Optional[BuilderValidationError]:
        """Validate the optional `connection_pooling` block.

        None is treated as omitted. Otherwise must be a dict whose keys are
        subset of {enabled, max_total, idle_timeout_seconds}, with the right
        value types. Returns the first error encountered, or None.
        """
        if pooling is None:
            return None
        if not isinstance(pooling, dict):
            return BuilderValidationError(
                f"connection_pooling must be an object/dict, got "
                f"{type(pooling).__name__}",
                error_code="REST_POOLING_INVALID",
                field="connection_pooling",
                hint=(
                    "Pass connection_pooling as a JSON object with keys: "
                    "enabled (bool), max_total (int), idle_timeout_seconds (int)."
                ),
            )
        unknown = set(pooling.keys()) - cls._POOLING_ALLOWED_KEYS
        if unknown:
            offender = sorted(unknown)[0]
            return BuilderValidationError(
                f"connection_pooling has unknown key {offender!r}",
                error_code="REST_POOLING_INVALID",
                field=f"connection_pooling.{offender}",
                hint=(
                    "Allowed connection_pooling keys: "
                    f"{', '.join(sorted(cls._POOLING_ALLOWED_KEYS))}."
                ),
            )
        if "enabled" in pooling and not isinstance(pooling["enabled"], bool):
            return BuilderValidationError(
                "connection_pooling.enabled must be a bool",
                error_code="REST_POOLING_INVALID",
                field="connection_pooling.enabled",
                hint="Use true or false.",
            )
        for int_field in ("max_total", "idle_timeout_seconds"):
            if int_field in pooling:
                value = pooling[int_field]
                if isinstance(value, bool) or not isinstance(value, int):
                    return BuilderValidationError(
                        f"connection_pooling.{int_field} must be an integer",
                        error_code="REST_POOLING_INVALID",
                        field=f"connection_pooling.{int_field}",
                        hint="Use a JSON integer (Boomi default: 20 for max_total, 30 for idle_timeout_seconds).",
                    )
        # Pooling-dependent gate: max_total and idle_timeout_seconds only
        # take effect when enabled=True. Live disabled exports emit
        # `maxTotal`/`idleTimeout` with empty values — so a caller-supplied
        # number for either field with enabled missing or False is a
        # stale-config mistake (the number never reaches Boomi's pool).
        pool_on = pooling.get("enabled") is True
        if not pool_on:
            for dep_field in ("max_total", "idle_timeout_seconds"):
                if dep_field in pooling:
                    return BuilderValidationError(
                        f"connection_pooling.{dep_field} is only valid "
                        "when connection_pooling.enabled=True",
                        error_code="REST_POOLING_INVALID",
                        field=f"connection_pooling.{dep_field}",
                        hint=(
                            f"Remove {dep_field} or set "
                            "connection_pooling.enabled=True. Live "
                            "disabled-pool exports emit maxTotal and "
                            "idleTimeout with empty values — supplying "
                            "a number while pooling is off would never "
                            "take effect."
                        ),
                    )
        return None

    @staticmethod
    def _field(field_id: str, value: Any, field_type: str = "string") -> str:
        formatted = _format_xml_value(value) if value not in (None, "") else ""
        return f'            <field id="{field_id}" type="{field_type}" value="{formatted}"/>\n'

    @staticmethod
    def _field_self_closing_when_empty(
        field_id: str, value: Any, field_type: str
    ) -> str:
        """Like ``_field`` but emits the self-closing form
        ``<field id="..." type="..."/>`` when ``value`` is empty/None, and the
        attribute form ``<field ... value="..."/>`` when populated.

        Matches the live REST connection shape for ``privateCertificate`` and
        ``publicCertificate``: those fields drop the ``value`` attribute
        entirely when no cert ref is assigned (verified against the
        REST None / REST None Pooling / REST Certificate live exports)."""
        if value in (None, ""):
            return f'            <field id="{field_id}" type="{field_type}"/>\n'
        formatted = _escape_xml(str(value))
        return (
            f'            <field id="{field_id}" type="{field_type}" '
            f'value="{formatted}"/>\n'
        )

    @staticmethod
    def _emit_preemptive(auth: str, preemptive_value: Any) -> str:
        """Match live: BASIC and OAUTH2 emit an explicit boolean (true/false);
        NONE and NTLM emit value="" (Boomi treats preemptive as irrelevant
        for those modes, per docs). Verified against d6ee8b5b (OAUTH2 false),
        587b5fe0 (BASIC false), 7f7e0730 (NONE empty), 1de43085 (NTLM empty)."""
        if auth in ("BASIC", "OAUTH2"):
            return RestClientConnectionBuilder._field(
                "preemptive", bool(preemptive_value), "boolean"
            )
        return '            <field id="preemptive" type="boolean" value=""/>\n'

    @staticmethod
    def _build_encrypted_values_block(auth: str) -> str:
        """Auth-mode-driven `<bns:encryptedValues>` header.

        - NONE: empty `<bns:encryptedValues/>` — no secrets stored.
        - BASIC (and other password-backed auths): xpath marker at the
          `password` field. Boomi flips isSet=true when the value is saved.
        - OAUTH2: client-secret xpath marker inside OAuth2Config.
        """
        if auth == "OAUTH2":
            return (
                '    <bns:encryptedValues>\n'
                '        <bns:encryptedValue path="//GenericConnectionConfig/field/OAuth2Config/credentials/@clientSecret" isSet="false"/>\n'
                '    </bns:encryptedValues>\n'
            )
        if auth in RestClientConnectionBuilder._PASSWORD_BACKED_AUTH_MODES:
            return (
                '    <bns:encryptedValues>\n'
                '        <bns:encryptedValue path="//GenericConnectionConfig/field[@type=\'password\']" isSet="false"/>\n'
                '    </bns:encryptedValues>\n'
            )
        # NONE (and other non-secret modes in the skeleton).
        return '    <bns:encryptedValues/>\n'

    @staticmethod
    def _build_oauth2_skeleton_field() -> str:
        """Empty OAuth2Config child emitted for non-OAUTH2 auth modes.

        Matches live REST None / REST Certificate / REST None Pooling /
        REST Basic / REST NTLM: every connection still carries the
        OAuth2Config element with ``grantType="code"``, empty credentials,
        empty endpoints, empty scope. ``credentialsAssertionType`` is
        deliberately absent in the skeleton (only populated grants emit
        it).
        """
        return (
            '            <field id="oauthContext" type="oauth">\n'
            '                <OAuth2Config grantType="code">\n'
            '                    <credentials clientId=""/>\n'
            '                    <authorizationTokenEndpoint url=""><sslOptions/></authorizationTokenEndpoint>\n'
            '                    <authorizationParameters/>\n'
            '                    <accessTokenEndpoint url=""><sslOptions/></accessTokenEndpoint>\n'
            '                    <accessTokenParameters/>\n'
            '                    <scope/>\n'
            '                    <jwtParameters><expiration>0</expiration></jwtParameters>\n'
            '                </OAuth2Config>\n'
            '            </field>\n'
        )

    def _build_oauth2_field(self, oauth2: Dict[str, Any]) -> str:
        """Emit the OAuth2Config child for OAUTH2 auth. Dispatches on the
        normalized grant type so both client_credentials and
        authorization_code (token-not-set) round-trip through the same entry
        point. Cached `accessToken` ciphertext is NEVER emitted — that path
        is left for the user to authorize via the Boomi UI after create."""
        canonical_grant = self._OAUTH2_GRANT_TYPE_ALIASES.get(
            oauth2.get("grant_type"), "client_credentials"
        )
        client_id = _escape_xml(str(oauth2.get("client_id", "")))
        access_token_url = _escape_xml(str(oauth2.get("access_token_url", "")))
        scope = _escape_xml(str(oauth2.get("scope", "")))
        if canonical_grant == "authorization_code":
            authorization_url = _escape_xml(str(oauth2.get("authorization_url", "")))
            # Live REST Auth Code Token Not Set (7abf0ad2) shape:
            #   - grantType="code" (XML attribute value for authorization_code)
            #   - credentials has clientId + clientSecret only (no
            #     accessTokenKey, no accessToken — cached tokens NEVER emitted)
            #   - authorizationTokenEndpoint url populated
            #   - accessTokenEndpoint url populated
            #   - scope populated (optional but emitted)
            #   - NO credentialsAssertionType element
            return (
                '            <field id="oauthContext" type="oauth">\n'
                '                <OAuth2Config grantType="code">\n'
                f'                    <credentials clientId="{client_id}" clientSecret=""/>\n'
                f'                    <authorizationTokenEndpoint url="{authorization_url}"><sslOptions/></authorizationTokenEndpoint>\n'
                '                    <authorizationParameters/>\n'
                f'                    <accessTokenEndpoint url="{access_token_url}"><sslOptions/></accessTokenEndpoint>\n'
                '                    <accessTokenParameters/>\n'
                f'                    <scope>{scope}</scope>\n'
                '                    <jwtParameters><expiration>0</expiration></jwtParameters>\n'
                '                </OAuth2Config>\n'
                '            </field>\n'
            )
        # client_credentials shape (Local REST Connection d6ee8b5b):
        #   - grantType="client_credentials"
        #   - credentials has accessTokenKey (Boomi-generated, emit empty),
        #     clientId, clientSecret (encrypted, emit empty)
        #   - authorizationTokenEndpoint url empty
        #   - accessTokenEndpoint url populated
        #   - credentialsAssertionType element populated
        assertion_type = _escape_xml(
            str(oauth2.get("credentials_assertion_type", "client_secret"))
        )
        return (
            '            <field id="oauthContext" type="oauth">\n'
            '                <OAuth2Config grantType="client_credentials">\n'
            f'                    <credentials accessTokenKey="" clientId="{client_id}" clientSecret=""/>\n'
            '                    <authorizationTokenEndpoint url=""><sslOptions/></authorizationTokenEndpoint>\n'
            '                    <authorizationParameters/>\n'
            f'                    <accessTokenEndpoint url="{access_token_url}"><sslOptions/></accessTokenEndpoint>\n'
            '                    <accessTokenParameters/>\n'
            f'                    <scope>{scope}</scope>\n'
            '                    <jwtParameters><expiration>0</expiration></jwtParameters>\n'
            f'                    <credentialsAssertionType>{assertion_type}</credentialsAssertionType>\n'
            '                </OAuth2Config>\n'
            '            </field>\n'
        )

    def build(self, **params) -> str:
        error = self.validate_config(params)
        if error is not None:
            raise error

        component_name = params["component_name"]
        base_url = params["base_url"]
        auth = params["auth"]
        folder_name = params.get("folder_name", "Home")
        description = params.get("description", "")

        connect_timeout = params.get("connect_timeout_ms", -1)
        read_timeout = params.get("read_timeout_ms", -1)
        cookie_scope = params.get("cookie_scope", "GLOBAL")
        pooling = params.get("connection_pooling") or {}
        pool_enabled = bool(pooling.get("enabled", False))
        max_total = pooling.get("max_total", "")
        idle_timeout = pooling.get("idle_timeout_seconds", "")

        username = params.get("username", "")
        domain = params.get("domain", "")
        workstation = params.get("workstation", "")
        private_cert_ref = params.get("private_certificate_ref", "")
        public_cert_ref = params.get("public_certificate_ref", "")

        safe_name = _escape_xml(component_name)
        safe_folder = _escape_xml(folder_name)
        safe_desc = _escape_xml(description)

        fields = []
        fields.append(self._field("url", base_url))
        fields.append(self._field("auth", auth))
        fields.append(self._field("username", username))
        fields.append(self._field("password", "", "password"))
        fields.append(self._field("domain", domain))
        fields.append(self._field("workstation", workstation))
        fields.append(self._field("customAuthCredentials", "", "password"))
        fields.append(self._emit_preemptive(auth, params.get("preemptive", False)))
        for aws_field_id, aws_field_type in _REST_CLIENT_AWS_FIELDS:
            fields.append(self._field(aws_field_id, "", aws_field_type))
        fields.append(self._field("awsPublicCertificate", "", "publiccertificate"))
        fields.append(self._field("awsPrivateKey", "", "privatecertificate"))
        if auth == "OAUTH2":
            fields.append(self._build_oauth2_field(params["oauth2"]))
        else:
            # Non-OAUTH2 modes keep the OAuth2Config child as a skeleton so
            # the connection can be UI-promoted later. Matches live shape of
            # REST None / REST Certificate / REST None Pooling exports.
            fields.append(self._build_oauth2_skeleton_field())
        fields.append(
            self._field_self_closing_when_empty(
                "privateCertificate", private_cert_ref, "privatecertificate"
            )
        )
        fields.append(
            self._field_self_closing_when_empty(
                "publicCertificate", public_cert_ref, "publiccertificate"
            )
        )
        fields.append(self._field("connectTimeout", connect_timeout, "integer"))
        fields.append(self._field("readTimeout", read_timeout, "integer"))
        fields.append(self._field("cookieScope", cookie_scope))
        fields.append(self._field("enableConnectionPooling", pool_enabled, "boolean"))
        fields.append(self._field("maxTotal", max_total, "integer"))
        fields.append(self._field("idleTimeout", idle_timeout, "integer"))

        encrypted_values_block = self._build_encrypted_values_block(auth)

        return (
            '<?xml version="1.0" encoding="UTF-8"?>\n'
            '<bns:Component xmlns:bns="http://api.platform.boomi.com/"\n'
            '               xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance"\n'
            f'               type="connector-settings" subType="{REST_CLIENT_SUBTYPE}"\n'
            f'               name="{safe_name}"\n'
            f'               folderName="{safe_folder}">\n'
            f'{encrypted_values_block}'
            f'    <bns:description>{safe_desc}</bns:description>\n'
            '    <bns:object>\n'
            '        <GenericConnectionConfig xmlns="">\n'
            f'{"".join(fields)}'
            '        </GenericConnectionConfig>\n'
            '    </bns:object>\n'
            '</bns:Component>'
        )


class RestClientOperationBuilder:
    """Builder for Boomi REST Client connector-action (issue #24, M2.4).

    All 8 REST verbs are buildable: GET, POST, PUT, PATCH, DELETE, HEAD,
    OPTIONS, TRACE. Method-specific shape differences locked against live
    RenEra exports per phase 5 of issue #24.

    followRedirects emission rule (matches live exports):
      * GET, POST, HEAD, DELETE: emit `<field id="followRedirects"
        value="NONE"/>` by default. Explicit NONE/STRICT/LAX always emits.
      * PATCH, PUT, OPTIONS, TRACE: omit the field entirely by default.
        Explicit values still emit.

    customProperties (query_parameters / request_headers): plain key/value
    entries are emitted as `<properties key=... value=.../>` children;
    secret-shaped keys (Authorization, X-API-Key, etc.) and values
    (JWT-shape, long base64, [encrypted] prefix) are rejected with
    REST_SECRET_VALUE_FORBIDDEN. Encrypted entries from Boomi exports
    (`encrypted=true`) are rejected with
    UNSUPPORTED_REST_ENCRYPTED_CUSTOM_PROPERTY.

    Operation envelope defaults (matches live exports):
      * returnApplicationErrors="true", trackResponse="true". Both
        accept caller bool overrides; strings/ints are rejected with
        REST_OPERATION_VALIDATION_FAILED (no silent truthy-coercion).

    GenericOperationConfig: customOperationType=<METHOD>,
    operationType="EXECUTE", requestProfileType / responseProfileType
    lowercase ("xml" / "json" / "none") per the live exports.

    Verified live exports:
      * GET   (e268ea19), PATCH (64c4eafd), and all 6 other verbs
        confirmed against the new_findings_2026_05_23 export set.
    """

    SUPPORTED_OPERATION_MODES = ("execute",)
    SUPPORTED_METHODS = ("GET", "PATCH", "PUT", "POST", "DELETE", "HEAD", "OPTIONS", "TRACE")
    VERIFIED_PENDING_METHODS = ()
    SUPPORTED_FOLLOW_REDIRECTS_VALUES = ("NONE", "STRICT", "LAX")
    SUPPORTED_PROFILE_TYPES = ("none", "xml", "json")
    # followRedirects emission rule per Phase 5 (verified against live exports
    # f7d08bdb, 7524cfae, 3d843e38, 0c1e7528, 63f63c32, 64c4eafd, 868e3b5d,
    # e268ea19). Methods that default to emitting `NONE` vs methods that omit
    # the field entirely when caller doesn't supply follow_redirects:
    _FOLLOW_REDIRECTS_DEFAULT_NONE_METHODS = frozenset({"GET", "POST", "HEAD", "DELETE"})
    _FOLLOW_REDIRECTS_OMIT_METHODS = frozenset({"PATCH", "PUT", "OPTIONS", "TRACE"})
    FORBIDDEN_SECRET_FIELDS = DatabaseConnectorBuilder.FORBIDDEN_SECRET_FIELDS

    @classmethod
    def scan_forbidden_secret_fields(
        cls, config: Any, _path_prefix: str = ""
    ) -> Optional[BuilderValidationError]:
        return DatabaseConnectorBuilder.scan_forbidden_secret_fields(
            config, _path_prefix=_path_prefix
        )

    @classmethod
    def redact_forbidden_secret_fields_in_place(cls, config: Any) -> None:
        DatabaseConnectorBuilder.redact_forbidden_secret_fields_in_place(config)

    # Secret-shaped key pattern for customProperties (Phase 6, extended in
    # codex round 3). Matches the well-known header/query-param key shapes
    # that callers must NOT put plaintext secrets behind. Case-insensitive
    # whole-string match.
    #
    # Round-3 additions (Proxy-Authorization was the specific call-out;
    # the others are the broader credential-bearing-header family that
    # belongs alongside it):
    #   - (proxy[-_]?)?authorization      catches Authorization AND
    #                                     Proxy-Authorization
    #   - x[-_]?(api|auth|csrf|session)[-_]?(key|token|secret)?
    #                                     widened X-* prefix coverage
    #                                     (X-CSRF-Token, X-Session-Token,
    #                                      X-Auth-Password, X-API-*)
    #   - (set[-_]?)?cookie               session tokens in Cookie /
    #                                     Set-Cookie headers
    #   - www[-_]?authenticate            server challenge header
    #   - passwd                          common short-form
    _SECRET_PROPERTY_KEY_RE = re.compile(
        r"^("
        r"(proxy[-_]?)?authorization"
        r"|x[-_]?(api|auth|csrf|session)[-_]?(key|token|secret|password|user)?"
        r"|api[-_]?(key|token)"
        r"|bearer"
        r"|token"
        r"|password"
        r"|passwd"
        r"|secret"
        r"|credential(s)?"
        r"|client[-_]?secret"
        r"|(set[-_]?)?cookie"
        r"|www[-_]?authenticate"
        r")$",
        re.IGNORECASE,
    )

    # Secret-shaped value patterns (Phase 6, extended in codex round 3).
    # Conservative — only fires for clearly-secret-looking values to avoid
    # false-positives on normal URL query/header values like
    # "application/json" or "en-US".
    _SECRET_PROPERTY_VALUE_PATTERNS = (
        # Boomi explicit encrypted-marker literal prefix.
        re.compile(r"^\[encrypted\]", re.IGNORECASE),
        # JWT 3-part shape (eyJ + base64.base64.base64).
        re.compile(r"^eyJ[A-Za-z0-9_-]+\.[A-Za-z0-9_-]+\.[A-Za-z0-9_+/=-]+$"),
        # Long base64-shaped ciphertext (40+ chars, no whitespace, no
        # protocol scheme, no @ — distinguishes from URLs / emails / human
        # values). 40-char threshold tuned for Boomi-style encrypted-value
        # markers; legitimate Content-Type / User-Agent / etc are far
        # shorter.
        re.compile(r"^[A-Za-z0-9+/=]{40,}$"),
        # HTTP authorization scheme prefixes — RFC 7235 / 6750 / 7617 /
        # 7616 / 4559. Catches credential payloads even when the KEY isn't
        # named Authorization (e.g. `X-Custom-Header: Basic dXNlcjpwYXNz`).
        re.compile(r"^(Bearer|Basic|Digest|Negotiate|NTLM)\s+\S", re.IGNORECASE),
    )

    @classmethod
    def _value_looks_secret(cls, value: str) -> bool:
        for pattern in cls._SECRET_PROPERTY_VALUE_PATTERNS:
            if pattern.match(value):
                return True
        return False

    @classmethod
    def _validate_dict_param(
        cls, value: Any, field: str
    ) -> Optional[BuilderValidationError]:
        """Validate an optional customProperties-shaped dict (Phase 6).

        Behavior:
            - None / empty dict: accepted (the prior empty-only path).
            - Non-dict: rejected with REST_OPERATION_VALIDATION_FAILED.
            - `{"encrypted": True, ...}` shape (Boomi ciphertext-marker
              forwarded by a confused caller): rejected with
              UNSUPPORTED_REST_ENCRYPTED_CUSTOM_PROPERTY.
            - Non-string key or non-string value: rejected with
              REST_CUSTOM_PROPERTY_INVALID.
            - Key matches secret-shaped pattern (Authorization, X-API-Key,
              Bearer, etc.): rejected with REST_SECRET_VALUE_FORBIDDEN.
            - Value matches secret-shaped pattern (JWT, long base64,
              [encrypted] prefix): rejected with REST_SECRET_VALUE_FORBIDDEN.
            - Otherwise: accepted (plain custom property entries).

        Verified against live REST Query Param GET (9ede2c08) and REST
        Headers GET (4986d5eb) — those examples carry mixed plain +
        encrypted entries; the builder emits plain entries and rejects
        encrypted ones until a secret-safe write path exists.
        """
        if value is None:
            return None
        if not isinstance(value, dict):
            return BuilderValidationError(
                f"{field} must be an object/dict, got {type(value).__name__}",
                error_code="REST_OPERATION_VALIDATION_FAILED",
                field=field,
                hint=f"Pass {field} as a JSON object.",
            )
        if not value:  # empty dict — accepted as before
            return None

        # `{"encrypted": True, ...}` Boomi-export-shape forwarded by a
        # confused caller — reject before per-entry checks so the error
        # code is specific.
        if value.get("encrypted") is True:
            return BuilderValidationError(
                f"{field} contains an `encrypted=True` marker — Boomi "
                "encrypted custom properties are not yet supported by this "
                "builder",
                error_code="UNSUPPORTED_REST_ENCRYPTED_CUSTOM_PROPERTY",
                field=field,
                hint=(
                    "Remove the encrypted entry. Encrypted customProperty "
                    "emission requires a secret-safe encryption/write "
                    "path that does not exist yet. Open a follow-up "
                    "issue once a verified secret-safe flow lands."
                ),
            )

        for key, val in value.items():
            # Type checks first — secret detection assumes both are strings.
            if not isinstance(key, str):
                return BuilderValidationError(
                    f"{field} entries must use string keys, got "
                    f"{type(key).__name__}",
                    error_code="REST_CUSTOM_PROPERTY_INVALID",
                    field=field,
                    hint=(
                        f"Each {field} entry must be a JSON object with "
                        "string key and string value."
                    ),
                )
            if not isinstance(val, str):
                return BuilderValidationError(
                    f"{field} entry {key!r} value must be a string, got "
                    f"{type(val).__name__}",
                    error_code="REST_CUSTOM_PROPERTY_INVALID",
                    field=field,
                    hint=(
                        "Boomi customProperties only carry string values; "
                        "stringify the value first (e.g. str(int_value))."
                    ),
                )
            if cls._SECRET_PROPERTY_KEY_RE.match(key):
                return BuilderValidationError(
                    f"{field} key {key!r} matches a secret-shaped header/"
                    "param name (Authorization, X-API-Key, Bearer, etc.) — "
                    "secret credentials must not be passed as plaintext "
                    "customProperty entries",
                    error_code="REST_SECRET_VALUE_FORBIDDEN",
                    field=field,
                    hint=(
                        "Model token-based authentication on the CONNECTION "
                        "(auth='OAUTH2') and let Boomi inject the "
                        "Authorization header from the encrypted credential "
                        "store. Do not put plaintext bearer tokens or API "
                        "keys into customProperty entries."
                    ),
                )
            if cls._value_looks_secret(val):
                return BuilderValidationError(
                    f"{field} value for key {key!r} looks like a secret "
                    "(JWT / long base64 / encrypted-marker prefix). Plain "
                    "non-secret values only",
                    error_code="REST_SECRET_VALUE_FORBIDDEN",
                    field=field,
                    hint=(
                        "Do not pass JWTs, encrypted ciphertext, or "
                        "long-base64-shaped values as customProperty "
                        "entries. Model the credential on the connection "
                        "auth instead."
                    ),
                )
        return None

    @staticmethod
    def _build_customproperties_field(field_id: str, entries: Dict[str, str]) -> str:
        """Emit a `<field id=... type="customproperties">` block.

        Empty dict → `<customProperties/>` (self-closing, matches the
        verified empty live shape).
        Non-empty dict → `<customProperties><properties key="..." value="..."/>
        ...</customProperties>` with one element per entry in insertion order.
        Both key and value are XML-escaped. Verified shape from live REST
        Query Param GET (9ede2c08) and REST Headers GET (4986d5eb) — only
        the plain entries; encrypted entries are rejected upstream.
        """
        if not entries:
            return (
                f'                    <field id="{field_id}"'
                f' type="customproperties"><customProperties/></field>\n'
            )
        rows = [
            f'<properties key="{_escape_xml(str(k))}"'
            f' value="{_escape_xml(str(v))}"/>'
            for k, v in entries.items()
        ]
        return (
            f'                    <field id="{field_id}" type="customproperties">'
            f'<customProperties>{"".join(rows)}</customProperties>'
            f'</field>\n'
        )

    @classmethod
    def _validate_profile_ref(
        cls, value: Any, field: str
    ) -> Optional[BuilderValidationError]:
        if not isinstance(value, str):
            return None
        if value.startswith("$ref:") and not value[5:]:
            return BuilderValidationError(
                f"{field} $ref token is empty (expected '$ref:KEY')",
                error_code="REST_PROFILE_REF_UNRESOLVED",
                field=field,
                hint=(
                    "Use '$ref:<profile key>' to reference a profile "
                    "component declared earlier in the same integration spec."
                ),
            )
        return None

    @classmethod
    def validate_config(cls, config: Dict[str, Any]) -> Optional[BuilderValidationError]:
        # 1) Plaintext secret-shaped keys.
        secret_err = cls.scan_forbidden_secret_fields(config)
        if secret_err is not None:
            return secret_err

        # 2) operation_mode must be 'execute'.
        operation_mode = (config.get("operation_mode") or "").lower() if isinstance(config.get("operation_mode"), str) else ""
        if operation_mode not in cls.SUPPORTED_OPERATION_MODES:
            supported = ", ".join(cls.SUPPORTED_OPERATION_MODES)
            return BuilderValidationError(
                f"operation_mode must be one of: {supported}",
                error_code="UNSUPPORTED_REST_OPERATION_MODE",
                field="operation_mode",
                hint=(
                    f"Use operation_mode='{cls.SUPPORTED_OPERATION_MODES[0]}'. "
                    "The verb (GET/PATCH) goes in the 'method' field; Boomi's "
                    "REST Client uses operationType=EXECUTE for all calls."
                ),
            )

        # 3) component_name required.
        component_name = config.get("component_name")
        if not component_name or not str(component_name).strip():
            return BuilderValidationError(
                "component_name is required",
                error_code="REST_OPERATION_VALIDATION_FAILED",
                field="component_name",
                hint="Provide a non-empty component_name string.",
            )

        # 4) connection_ref_key required (cross-step depends_on check lives in
        # integration_builder._check_rest_operation_dependencies).
        connection_ref_key = config.get("connection_ref_key")
        if not connection_ref_key or not str(connection_ref_key).strip():
            return BuilderValidationError(
                "connection_ref_key is required for REST operations",
                error_code="REST_CONNECTION_REF_REQUIRED",
                field="connection_ref_key",
                hint=(
                    "Declare the REST connector-settings key the operation "
                    "will bind to at process time, and add the same key to "
                    "depends_on so plan ordering is correct."
                ),
            )

        # 5) method gating. Post-Phase-5, all 8 REST methods are buildable;
        # the VERIFIED_PENDING_METHODS branch is therefore unreachable from
        # the supported list but kept defensively for any future
        # not-yet-buildable variant.
        raw_method = config.get("method")
        method = (raw_method or "").upper() if isinstance(raw_method, str) else ""
        if method in cls.SUPPORTED_METHODS:
            pass
        elif method in cls.VERIFIED_PENDING_METHODS:
            supported = ", ".join(cls.SUPPORTED_METHODS)
            return BuilderValidationError(
                f"method {method!r} is recognized but not yet buildable — "
                "no verified Boomi live export exists for this method",
                error_code="UNVERIFIED_REST_XML_VARIANT",
                field="method",
                hint=(
                    f"Supported methods: {supported}. To add {method} "
                    "support, create a minimal REST Client operation in "
                    "Boomi, then a follow-up issue locks the shape against "
                    "that export."
                ),
            )
        else:
            supported = ", ".join(cls.SUPPORTED_METHODS)
            return BuilderValidationError(
                f"Unknown method {raw_method!r}",
                error_code="UNSUPPORTED_REST_METHOD",
                field="method",
                hint=f"Supported methods: {supported}.",
            )

        # 6) path required.
        path = config.get("path")
        if path is None or not str(path).strip():
            return BuilderValidationError(
                "path is required for REST operations",
                error_code="REST_PATH_REQUIRED",
                field="path",
                hint=(
                    "Provide the endpoint path appended onto the connection's "
                    "base_url (e.g. '/v1/items/{id}'). REST Client preserves "
                    "the path verbatim in emitted XML."
                ),
            )

        # 7) query_parameters / request_headers — empty only until verified.
        for q_field in ("query_parameters", "request_headers"):
            err = cls._validate_dict_param(config.get(q_field), q_field)
            if err is not None:
                return err

        # 8) Profile $ref tokens — empty token rejected.
        for ref_field in ("request_profile_id", "response_profile_id"):
            err = cls._validate_profile_ref(config.get(ref_field), ref_field)
            if err is not None:
                return err

        # 9) follow_redirects (when supplied) must be NONE / STRICT / LAX.
        if "follow_redirects" in config:
            fr = config["follow_redirects"]
            if not isinstance(fr, str) or fr not in cls.SUPPORTED_FOLLOW_REDIRECTS_VALUES:
                supported = ", ".join(cls.SUPPORTED_FOLLOW_REDIRECTS_VALUES)
                return BuilderValidationError(
                    f"follow_redirects must be one of: {supported}",
                    error_code="REST_OPERATION_VALIDATION_FAILED",
                    field="follow_redirects",
                    hint=(
                        f"Use one of: {supported}. The verified GET live "
                        "export uses 'NONE' as the default."
                    ),
                )

        # 10) request_profile_type / response_profile_type — accept any
        # casing of the documented enum and normalize to lowercase at build
        # time. Boomi's REST Client emits lowercase ('xml') per the live
        # exports.
        for pt_field in ("request_profile_type", "response_profile_type"):
            if pt_field in config:
                value = config[pt_field]
                if not isinstance(value, str) or value.lower() not in cls.SUPPORTED_PROFILE_TYPES:
                    supported = ", ".join(cls.SUPPORTED_PROFILE_TYPES)
                    return BuilderValidationError(
                        f"{pt_field} must be one of (case-insensitive): {supported}",
                        error_code="REST_OPERATION_VALIDATION_FAILED",
                        field=pt_field,
                        hint=(
                            f"Use one of: {supported}. Boomi's REST Client "
                            "emits lowercase in XML; the builder normalizes "
                            "uppercase/mixed-case input."
                        ),
                    )

        # 11) Bool operation flags. Reject non-bool callers up front —
        # before the fix, build() applied `bool(...)` coercion so
        # `"false"` (string) became True (Python truthy) and silently
        # corrupted the emitted XML attribute. None is treated as
        # "not supplied" (default True at build) for consistency.
        for bool_field in ("return_application_errors", "track_response"):
            if bool_field in config and config[bool_field] is not None:
                value = config[bool_field]
                if not isinstance(value, bool):
                    return BuilderValidationError(
                        f"{bool_field} must be a boolean (got "
                        f"{type(value).__name__})",
                        error_code="REST_OPERATION_VALIDATION_FAILED",
                        field=bool_field,
                        hint=(
                            f"Pass {bool_field}=True or False. String "
                            "'true'/'false' and numeric 0/1 are rejected "
                            "to prevent silent truthy-coercion that "
                            "would mis-emit the XML attribute."
                        ),
                    )

        return None

    def build(self, **params) -> str:
        error = self.validate_config(params)
        if error is not None:
            raise error

        component_name = params["component_name"]
        method = params["method"].upper()
        path = str(params["path"])
        folder_name = params.get("folder_name", "Home")
        description = params.get("description", "")

        # Normalize profile types to Boomi's expected lowercase form. The
        # validator accepts any casing of {none, xml, json}; XML emission
        # must always be lowercase to match the live REST Client export.
        request_profile_type = str(params.get("request_profile_type", "xml")).lower()
        response_profile_type = str(params.get("response_profile_type", "xml")).lower()
        # validate_config (step 11) has already enforced that these are
        # bool (or None / omitted). Resolve None → default True without
        # truthy-coercing arbitrary values.
        rae_raw = params.get("return_application_errors")
        return_application_errors = True if rae_raw is None else rae_raw
        tr_raw = params.get("track_response")
        track_response = True if tr_raw is None else tr_raw

        safe_name = _escape_xml(component_name)
        safe_folder = _escape_xml(folder_name)
        safe_desc = _escape_xml(description)
        safe_path = _escape_xml(path)
        safe_req_profile_type = _escape_xml(str(request_profile_type))
        safe_resp_profile_type = _escape_xml(str(response_profile_type))

        op_envelope_attrs = (
            f'returnApplicationErrors="{_format_xml_value(return_application_errors)}"'
            f' trackResponse="{_format_xml_value(track_response)}"'
        )

        # Optional profile attributes on GenericOperationConfig — only emitted
        # when supplied. $ref tokens are preserved verbatim; apply-time
        # resolution happens in integration_builder._resolve_dependency_tokens.
        request_profile_attr = ""
        if params.get("request_profile_id"):
            request_profile_attr = (
                f' requestProfile="{_escape_xml(str(params["request_profile_id"]))}"'
            )
        response_profile_attr = ""
        if params.get("response_profile_id"):
            response_profile_attr = (
                f' responseProfile="{_escape_xml(str(params["response_profile_id"]))}"'
            )

        # followRedirects emission rule (Phase 5):
        #   - GET/POST/HEAD/DELETE: emit value="NONE" by default
        #   - PATCH/PUT/OPTIONS/TRACE: omit the field when caller doesn't
        #     supply follow_redirects
        #   - Explicit caller value (NONE/STRICT/LAX) always emits regardless
        # Verified against live exports per method.
        follow_redirects_field = ""
        follow_redirects = params.get("follow_redirects")
        if follow_redirects is None and method in self._FOLLOW_REDIRECTS_DEFAULT_NONE_METHODS:
            follow_redirects = "NONE"
        if follow_redirects is not None:
            follow_redirects_field = (
                f'                    <field id="followRedirects" type="string"'
                f' value="{_escape_xml(str(follow_redirects))}"/>\n'
            )

        query_params_field = self._build_customproperties_field(
            "queryParameters", params.get("query_parameters") or {}
        )
        request_headers_field = self._build_customproperties_field(
            "requestHeaders", params.get("request_headers") or {}
        )

        body_inner = (
            f'                <GenericOperationConfig customOperationType="{method}"'
            f' operationType="EXECUTE"'
            f'{request_profile_attr}'
            f' requestProfileType="{safe_req_profile_type}"'
            f'{response_profile_attr}'
            f' responseProfileType="{safe_resp_profile_type}">\n'
            f'{follow_redirects_field}'
            f'                    <field id="path" type="string" value="{safe_path}"/>\n'
            f'{query_params_field}'
            f'{request_headers_field}'
            '                    <Options/>\n'
            '                </GenericOperationConfig>\n'
        )

        return (
            '<?xml version="1.0" encoding="UTF-8"?>\n'
            '<bns:Component xmlns:bns="http://api.platform.boomi.com/"\n'
            '               xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance"\n'
            f'               type="connector-action" subType="{REST_CLIENT_SUBTYPE}"\n'
            f'               name="{safe_name}"\n'
            f'               folderName="{safe_folder}">\n'
            '    <bns:encryptedValues/>\n'
            f'    <bns:description>{safe_desc}</bns:description>\n'
            '    <bns:object>\n'
            f'        <Operation xmlns="" {op_envelope_attrs}>\n'
            '            <Archiving directory="" enabled="false"/>\n'
            '            <Configuration>\n'
            f'{body_inner}'
            '            </Configuration>\n'
            '            <Tracking><TrackedFields/></Tracking>\n'
            '            <Caching/>\n'
            '        </Operation>\n'
            '    </bns:object>\n'
            '</bns:Component>'
        )


# ============================================================================
# Registry
# ============================================================================

CONNECTOR_BUILDERS: Dict[str, type] = {
    "database": DatabaseConnectorBuilder,
    "rest": RestClientConnectionBuilder,
    "rest_client": RestClientConnectionBuilder,
    REST_CLIENT_SUBTYPE.lower(): RestClientConnectionBuilder,
}


def get_connector_builder(connector_type: str):
    """Get a connector builder instance for the given type, or None."""
    builder_class = CONNECTOR_BUILDERS.get(connector_type.lower())
    if builder_class:
        return builder_class()
    return None


# Connector-action builders are dispatched by (connector_type, operation_mode)
# — separate registry from CONNECTOR_BUILDERS because connector-settings and
# connector-action have different XML shapes and required-field sets.
CONNECTOR_ACTION_BUILDERS: Dict[tuple, type] = {
    ("database", "get"): DatabaseGetOperationBuilder,
    ("rest", "execute"): RestClientOperationBuilder,
    ("rest_client", "execute"): RestClientOperationBuilder,
    (REST_CLIENT_SUBTYPE.lower(), "execute"): RestClientOperationBuilder,
}


def get_connector_action_builder(connector_type: str, operation_mode: str):
    """Get a connector-action builder instance for (connector_type, operation_mode), or None."""
    if not connector_type or not operation_mode:
        return None
    key = (connector_type.lower(), operation_mode.lower())
    builder_class = CONNECTOR_ACTION_BUILDERS.get(key)
    if builder_class:
        return builder_class()
    return None

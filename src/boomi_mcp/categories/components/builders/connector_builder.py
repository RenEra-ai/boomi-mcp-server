"""
Connector component XML builders for Boomi.

Builds XML for connector-settings (connections) via the Component API.
HTTP connectors use <HttpSettings> with structured attributes and nested elements.

The SDK's create_component() cannot parse the XML response for connectors,
so creation uses raw Serializer POST (see connectors.py _create_component_raw).
"""

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


class HttpConnectorBuilder:
    """Builder for HTTP/HTTPS connector-settings components.

    Generates <HttpSettings> XML matching the real Boomi UI export structure.
    Supports NONE, BASIC, and OAUTH2 authentication types.

    Config keys (all optional except url):
        url:                    Connection URL (required)
        auth_type:              NONE, BASIC, PASSWORD_DIGEST, CUSTOM, OAUTH, OAUTH2
        username:               Username for BASIC auth
        connect_timeout:        Connection timeout in ms (not in HttpSettings attrs)
        read_timeout:           Read timeout in ms (not in HttpSettings attrs)
        trust_all_certs:        Trust all SSL certificates (true/false)
        client_ssl_alias:       Client SSL certificate alias
        oauth2_grant_type:      OAuth2 grant type (e.g., client_credentials)
        oauth2_client_id:       OAuth2 client ID
        oauth2_client_secret:   OAuth2 client secret
        oauth2_scope:           OAuth2 scope
        oauth2_token_url:       OAuth2 access token endpoint URL
        oauth2_auth_url:        OAuth2 authorization endpoint URL
    """

    # Attributes on <HttpSettings> element
    HTTP_SETTINGS_ATTRS = {
        'url': 'url',
        'auth_type': 'authenticationType',
    }

    # Attributes on <AuthSettings> element
    AUTH_SETTINGS_ATTRS = {
        'username': 'user',
    }

    # Attributes on <SSLOptions> element
    SSL_OPTIONS_ATTRS = {
        'trust_all_certs': 'trustServerCert',
        'client_ssl_alias': 'clientauth',
    }

    def build(self, **params) -> str:
        """Build complete component XML for an HTTP connector-settings component."""
        component_name = params.get('component_name', '')
        if not component_name:
            raise ValueError("component_name is required")
        url = params.get('url', '')
        if not url:
            raise ValueError("url is required for HTTP connectors")

        folder_name = params.get('folder_name', 'Home')
        description = params.get('description', '')
        auth_type = params.get('auth_type', 'NONE')

        inner_xml = self._build_http_settings(**params)

        safe_name = _escape_xml(component_name)
        safe_folder = _escape_xml(folder_name)
        safe_desc = _escape_xml(description)

        return (
            '<?xml version="1.0" encoding="UTF-8"?>\n'
            '<bns:Component xmlns:bns="http://api.platform.boomi.com/"\n'
            '               xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance"\n'
            f'               type="connector-settings" subType="http"\n'
            f'               name="{safe_name}"\n'
            f'               folderName="{safe_folder}">\n'
            f'    <bns:description>{safe_desc}</bns:description>\n'
            f'    <bns:object>\n{inner_xml}\n    </bns:object>\n'
            '</bns:Component>'
        )

    def _build_http_settings(self, **params) -> str:
        """Build <HttpSettings> inner XML."""
        url = _escape_xml(params.get('url', ''))
        auth_type = _escape_xml(params.get('auth_type', 'NONE'))
        username = _escape_xml(params.get('username', ''))
        trust_all = params.get('trust_all_certs', 'false')
        client_ssl = _escape_xml(params.get('client_ssl_alias', ''))

        # SSL options
        ssl_attrs = f'clientauth="{client_ssl or "false"}" trustServerCert="{trust_all}"'

        # Build auth-specific sections
        auth_sections = ''
        if auth_type == 'OAUTH2':
            auth_sections = self._build_oauth2_section(**params)

        return (
            f'        <HttpSettings authenticationType="{auth_type}" url="{url}">\n'
            f'            <AuthSettings user="{username}"/>\n'
            f'{auth_sections}'
            f'            <SSLOptions {ssl_attrs}/>\n'
            f'        </HttpSettings>'
        )

    def _build_oauth2_section(self, **params) -> str:
        """Build <OAuth2Settings> XML section."""
        grant_type = _escape_xml(params.get('oauth2_grant_type', 'client_credentials'))
        client_id = _escape_xml(params.get('oauth2_client_id', ''))
        client_secret = _escape_xml(params.get('oauth2_client_secret', ''))
        scope = _escape_xml(params.get('oauth2_scope', ''))
        token_url = _escape_xml(params.get('oauth2_token_url', ''))
        auth_url = _escape_xml(params.get('oauth2_auth_url', ''))

        # Boomi requires strict element ordering:
        # credentials, authorizationTokenEndpoint, authorizationParameters,
        # accessTokenEndpoint, accessTokenParameters, scope
        return (
            f'            <OAuth2Settings grantType="{grant_type}">\n'
            f'                <credentials clientId="{client_id}" clientSecret="{client_secret}"/>\n'
            f'                <authorizationTokenEndpoint url="{auth_url}">\n'
            f'                    <sslOptions/>\n'
            f'                </authorizationTokenEndpoint>\n'
            f'                <authorizationParameters/>\n'
            f'                <accessTokenEndpoint url="{token_url}">\n'
            f'                    <sslOptions/>\n'
            f'                </accessTokenEndpoint>\n'
            f'                <accessTokenParameters/>\n'
            f'                <scope>{scope}</scope>\n'
            f'            </OAuth2Settings>\n'
        )


class DatabaseConnectorBuilder:
    """Builder for Database (Legacy) connector-settings components.

    Generates <DatabaseConnectionSettings> XML matching Boomi UI export structure.
    M2.2 supports SQL Server (Microsoft JDBC and jTDS). Postgres/Oracle/MySQL are
    deliberately not yet supported and return a structured UNSUPPORTED_DB_DRIVER.

    Config keys:
        component_name:     required
        driver_id:          required; one of SUPPORTED_DRIVER_IDS
                            ("sqlserver", "microsoft_jdbc", "jtds").
                            "microsoft_jdbc" is an alias for the Microsoft JDBC
                            driver and emits Boomi driverId="sqlserver".
        auth_mode:          required; one of SUPPORTED_AUTH_MODES
                            ("username_password"). "windows_integrated" is
                            recognized but deliberately deferred.
        host:               required
        dbname:             required (database name)
        username:           required
        credential_ref:     required when auth_mode="username_password".
                            Opaque caller-side reference (e.g.
                            "credential://vault/sqlserver/password"); the
                            builder never writes it to the emitted XML —
                            secrets must be set in the Boomi UI after create
                            or supplied via the raw-XML escape hatch.
        port:               optional; falls back to DRIVERS[driver_id]['default_port']
        folder_name:        optional; defaults to "Home"
        description:        optional
        additional:         optional JDBC URL suffix appended verbatim into urlFormat {3}
                            (e.g. ";encrypt=true;trustServerCertificate=true").

    Plaintext secret-shaped keys (see FORBIDDEN_SECRET_FIELDS) are rejected
    loudly with PLAINTEXT_SECRET_REJECTED before any XML is emitted.
    """

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
        },
        "jtds": {
            "shape":       "host_port_db",
            "buildable":   True,
            "driver_id":   "jtds",
            "class_name":  "net.sourceforge.jtds.jdbc.Driver",
            "url_format":  "jdbc:jtds:sqlserver://{0}:{1}/{2}{3}",
            "default_port": 1433,
        },
        "custom": {
            "shape":       "custom_url",
            "buildable":   False,
            "driver_id":   "custom",
            "unsupported_reason": (
                "Custom driver XML emission is deferred until a verified live "
                "Boomi Custom connection export is available. Use reuse mode "
                "on an existing Boomi component or the raw-XML escape hatch "
                "(config.xml=...) in the meantime."
            ),
        },
    }
    # "microsoft_jdbc" is a caller-facing alias for the Microsoft JDBC driver.
    # The emitted Boomi driverId stays "sqlserver" — Boomi has no separate
    # "microsoft_jdbc" registration; the alias just makes the config self-documenting.
    DRIVER_ALIASES: Dict[str, str] = {
        "microsoft_jdbc": "sqlserver",
    }

    # Recognized = entries in DRIVERS + DRIVER_ALIASES (includes custom).
    # Supported = subset that is actually buildable today (excludes custom).
    RECOGNIZED_DRIVER_IDS = ("sqlserver", "microsoft_jdbc", "jtds", "custom")
    SUPPORTED_DRIVER_IDS = ("sqlserver", "microsoft_jdbc", "jtds")
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
    # builder and never reaches build(). The "custom_url" shape is not
    # buildable yet — validate_config rejects it with UNSUPPORTED_DB_DRIVER_SHAPE
    # before this table is consulted.
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
    }
    # Back-compat alias: any external caller importing REQUIRED_FIELDS still
    # gets the host_port_db tuple (the only shape we built in M2.2).
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

        Walks the config dict tree depth-first. At each level, checks for
        FORBIDDEN_SECRET_FIELDS in the iteration order of that tuple (so
        'password' beats 'token' when both are present at the same depth —
        matches M2.2 priority). Returns the first offender with a dotted-path
        `field` (e.g. 'password', 'pooling.password', 'write_options.secret').

        Independent of builder invocation — plaintext secrets are a hard
        error regardless of which apply path the component takes (create /
        clone / reuse / update / raw-XML). Callers (e.g. integration_builder
        preflight) should run this on every database connector-settings
        config to keep credentials out of plan output.
        """
        if not isinstance(config, dict):
            return None
        # Check forbidden keys at the current level first (preserves the
        # M2.2 priority where 'password' wins over 'token' at the same depth).
        for forbidden in cls.FORBIDDEN_SECRET_FIELDS:
            if forbidden in config:
                field_path = f"{_path_prefix}{forbidden}"
                return BuilderValidationError(
                    f"{field_path!r} cannot be supplied in database connector "
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
        # Then recurse into nested dicts (e.g. pooling, write_options, or any
        # future block). Insertion order preserved (Python 3.7+).
        for key, value in config.items():
            if isinstance(value, dict):
                nested = cls.scan_forbidden_secret_fields(
                    value, _path_prefix=f"{_path_prefix}{key}."
                )
                if nested is not None:
                    return nested
        return None

    @classmethod
    def redact_forbidden_secret_fields_in_place(cls, config: Any) -> None:
        """Recursively replace any forbidden-keyed values with '[REDACTED]'.

        Mirrors scan_forbidden_secret_fields' traversal. Callers (e.g.
        integration_builder _build_plan) use this to scrub the spec echo
        before returning a plan response — otherwise nested secrets like
        pooling.password would leak even after the error is raised.
        """
        if not isinstance(config, dict):
            return
        for forbidden in cls.FORBIDDEN_SECRET_FIELDS:
            if forbidden in config:
                config[forbidden] = "[REDACTED]"
        for value in config.values():
            if isinstance(value, dict):
                cls.redact_forbidden_secret_fields_in_place(value)

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
                    f"SQL Server is the only DB family supported today "
                    f"({supported_drivers}). Postgres/Oracle/MySQL are "
                    "deferred to later milestones."
                ),
            )

        # 2b) Driver recognized but not buildable yet (e.g. custom).
        # Distinct error code (UNSUPPORTED_DB_DRIVER_SHAPE) so callers can
        # branch: an unrecognized driver_id needs a typo-fix; a non-buildable
        # one needs reuse / raw-XML escape hatch.
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
        required_fields = cls.REQUIRED_FIELDS_BY_SHAPE[driver["shape"]]
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
        # buildable (only the host_port_db shape reaches this point).
        driver = self._resolve_driver(params["driver_id"])
        assert driver is not None  # narrowing for type checkers

        component_name = params["component_name"]
        host = params["host"]
        dbname = params["dbname"]
        username = params["username"]

        port = params.get('port', driver['default_port'])
        folder_name = params.get('folder_name', 'Home')
        description = params.get('description', '')
        additional = params.get('additional', '')

        safe_name = _escape_xml(component_name)
        safe_folder = _escape_xml(folder_name)
        safe_desc = _escape_xml(description)
        safe_host = _escape_xml(host)
        safe_dbname = _escape_xml(dbname)
        safe_username = _escape_xml(username)
        safe_additional = _escape_xml(additional)

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
            f' className="{driver["class_name"]}"'
            f' dbname="{safe_dbname}"'
            f' driverId="{driver["driver_id"]}"'
            f' host="{safe_host}"'
            f' isPoolEnabled="{is_pool_enabled}"'
            f' port="{port}"'
            f' urlFormat="{driver["url_format"]}"'
            f' username="{safe_username}">\n'
            f'            <WriteOptions {write_options_attrs}/>\n'
            f'            <AdapterPoolInfo {adapter_pool_info_attrs}/>\n'
            '        </DatabaseConnectionSettings>\n'
            '    </bns:object>\n'
            '</bns:Component>'
        )


# ============================================================================
# Smart-merge helpers for update
# ============================================================================

# Maps config key -> (element_name, attribute_name) for HttpSettings updates.
# element_name None means the HttpSettings root element itself.
HTTP_UPDATE_MAP = {
    'url':              (None, 'url'),
    'auth_type':        (None, 'authenticationType'),
    'username':         ('AuthSettings', 'user'),
    'trust_all_certs':  ('SSLOptions', 'trustServerCert'),
    'client_ssl_alias': ('SSLOptions', 'clientauth'),
}


def find_http_settings(obj_elem):
    """Find the <HttpSettings> element inside <bns:object>.

    Handles both namespaced and non-namespaced variants.
    Returns (element, tag_without_ns) or (None, None).
    """
    for child in obj_elem:
        tag = child.tag.split('}')[-1] if '}' in child.tag else child.tag
        if tag == 'HttpSettings':
            return child
    return None


def find_child_element(parent, tag_name: str):
    """Find a direct child element by tag name (namespace-agnostic)."""
    for child in parent:
        tag = child.tag.split('}')[-1] if '}' in child.tag else child.tag
        if tag == tag_name:
            return child
    return None


def update_http_settings_fields(http_settings_elem, config: Dict[str, Any]) -> bool:
    """Update fields on HttpSettings and its child elements.

    Returns True if any changes were made.
    """
    changed = False

    for config_key, (elem_name, attr_name) in HTTP_UPDATE_MAP.items():
        if config_key not in config:
            continue
        value = str(config[config_key])

        if elem_name is None:
            # Update attribute on HttpSettings itself
            http_settings_elem.set(attr_name, value)
            changed = True
        else:
            child = find_child_element(http_settings_elem, elem_name)
            if child is not None:
                child.set(attr_name, value)
                changed = True

    return changed


# ============================================================================
# Registry
# ============================================================================

CONNECTOR_BUILDERS: Dict[str, type] = {
    "http": HttpConnectorBuilder,
    "database": DatabaseConnectorBuilder,
}


def get_connector_builder(connector_type: str):
    """Get a connector builder instance for the given type, or None."""
    builder_class = CONNECTOR_BUILDERS.get(connector_type.lower())
    if builder_class:
        return builder_class()
    return None

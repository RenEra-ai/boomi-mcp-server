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

    Issue #24 hardens the previous M1 builder: validation upgrades from
    `ValueError` to structured `BuilderValidationError`, plaintext secret
    keys are rejected before XML emission, and only `auth_type='NONE'` is
    buildable today. BASIC / OAUTH2 etc. are recognized but rejected with
    `UNSUPPORTED_HTTP_AUTH_MODE` until verified live Boomi exports exist
    for those shapes. The emitted XML for `auth_type='NONE'` is unchanged
    from the pre-issue-#24 output (locked by integration_builder tests).

    Config keys (all optional except component_name + url):
        component_name:         Required; top-level component name.
        url:                    Required; connection URL.
        auth_type:              NONE (only buildable mode in issue #24).
        folder_name:            Optional; defaults to "Home".
        description:            Optional.
        username:               Optional username for AuthSettings.
        trust_all_certs:        Optional; SSLOptions trustServerCert.
        client_ssl_alias:       Optional; SSLOptions clientauth.
        credential_ref:         Optional opaque caller-side credential
                                reference (e.g.
                                "credential://vendor/role"); the builder
                                never writes it into XML.
    """

    SUPPORTED_AUTH_MODES = ("NONE",)
    RECOGNIZED_BUT_UNSUPPORTED_AUTH_MODES = (
        "BASIC",
        "OAUTH2",
        "PASSWORD_DIGEST",
        "CUSTOM",
        "OAUTH",
    )
    # Same set as the database builders — secrets cross the wire as opaque
    # credential_ref strings only. Boomi password ciphertext is set via the
    # UI after create, or supplied via the raw-XML escape hatch.
    FORBIDDEN_SECRET_FIELDS = (
        "password",
        "password_ref",
        "secret",
        "token",
        "access_token",
        "client_secret",
    )

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
        """Validate an HTTP connector-settings config without building XML."""
        # 1) Plaintext secret-shaped keys must never appear in caller config.
        secret_err = cls.scan_forbidden_secret_fields(config)
        if secret_err is not None:
            return secret_err

        # 2) component_name required.
        component_name = config.get("component_name")
        if not component_name or not str(component_name).strip():
            return BuilderValidationError(
                "component_name is required for HTTP connectors",
                error_code="HTTP_CONNECTOR_VALIDATION_FAILED",
                field="component_name",
                hint="Provide a non-empty component_name string.",
            )

        # 3) url required.
        url = config.get("url")
        if not url or not str(url).strip():
            return BuilderValidationError(
                "url is required for HTTP connectors",
                error_code="MISSING_HTTP_ENDPOINT",
                field="url",
                hint=(
                    "Provide the connection base URL (e.g. "
                    "'https://api.example.com'). The operation's path is "
                    "set on the connector-action, not here."
                ),
            )

        # 4) auth_type gating — only NONE is buildable in issue #24.
        auth_type = config.get("auth_type")
        if auth_type is not None and auth_type not in cls.SUPPORTED_AUTH_MODES:
            supported = ", ".join(cls.SUPPORTED_AUTH_MODES)
            if auth_type in cls.RECOGNIZED_BUT_UNSUPPORTED_AUTH_MODES:
                return BuilderValidationError(
                    f"auth_type {auth_type!r} is recognized but not buildable "
                    "yet — no verified live Boomi XML reference is available",
                    error_code="UNSUPPORTED_HTTP_AUTH_MODE",
                    field="auth_type",
                    hint=(
                        f"Supported auth_modes: {supported}. For "
                        f"{auth_type}, model bearer/API-key target auth as "
                        "variable headers on the operation plus an opaque "
                        "credential_ref, or use the raw-XML escape hatch "
                        "(config.xml=...) with a verified export."
                    ),
                )
            return BuilderValidationError(
                f"Unknown auth_type {auth_type!r} (supported: {supported})",
                error_code="UNSUPPORTED_HTTP_AUTH_MODE",
                field="auth_type",
                hint=f"Supported auth_modes: {supported}.",
            )

        return None

    def build(self, **params) -> str:
        """Build complete component XML for an HTTP connector-settings component."""
        error = self.validate_config(params)
        if error is not None:
            raise error

        component_name = params["component_name"]
        url = params["url"]

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


class HttpClientOperationBuilder:
    """Builder for connector-action subType="http" send operations.

    Issue #24 — M2.4. Emits the Boomi <Operation> envelope wrapping either
    a <HttpSendAction> (POST/PUT/PATCH/DELETE) or a <HttpGetAction> (GET).
    The single public `operation_mode` is "send" — the GET/SEND split lives
    inside the builder, not in the caller-facing API, because Boomi's HTTP
    Client connector treats both shapes as one operation type.

    Live reference XML (POST, work profile 1428893f, fetched 2026-05-23):

        <Operation xmlns="">
          <Archiving directory="" enabled="false"/>
          <Configuration>
            <HttpSendAction dataContentType="application/json"
                            followRedirects="false" methodType="POST"
                            mimePassthrough="false" requestProfileType="NONE"
                            responseProfileType="NONE" returnErrors="true"
                            returnMimeResponse="false" returnResponses="true">
              <requestHeaders>
                <header headerName="Authorization" headerValue=""
                        isVariable="true" key="1000000"/>
                <header headerName="Accept" headerValue="*/*" key="1000001"/>
              </requestHeaders>
              <pathElements><element key="2000000" name="v3/mail/send"/></pathElements>
              <responseHeaderMapping/>
              <reflectHeaders/>
            </HttpSendAction>
          </Configuration>
          <Tracking><TrackedFields/></Tracking>
          <Caching/>
        </Operation>

    Live reference XML (GET, reneraai-5RO3DD profile 03ec828a, same date):
    identical envelope but <HttpGetAction> with NO `returnResponses`
    attribute and `returnErrors="false"` by default.

    Config keys:
        component_type:           connector-action (consumed by dispatcher).
        connector_type:           "http" (consumed by dispatcher).
        operation_mode:           "send" (only mode in issue #24).
        component_name:           required for top-level naming.
        connection_ref_key:       required; plan-only dependency on the HTTP
                                  connector-settings. Boomi binds the
                                  connection at the process connector step,
                                  not in the operation XML — this key never
                                  appears in emitted XML.
        method:                   required; one of SUPPORTED_METHODS.
        path:                     required; emitted as a single
                                  <pathElements/element/> with the leading
                                  '/' stripped (exactly one).
        content_type:             optional; emitted as dataContentType.
                                  Defaults to "application/json".
        request_profile_type:     optional; defaults to "NONE". When set
                                  to "JSON"/"XML" the caller should also
                                  supply request_profile_id.
        request_profile_id:       optional Boomi profile UUID OR
                                  "$ref:KEY" token (preserved verbatim —
                                  resolution happens upstream in
                                  integration_builder._resolve_dependency_tokens).
        response_profile_type:    optional; defaults to "NONE".
        response_profile_id:      optional UUID or $ref token.
        headers:                  optional list of {name, value?, is_variable?}.
                                  Keys start at 1000000 and increment.
        follow_redirects:         optional bool, defaults to False.
        mime_passthrough:         optional bool, defaults to False.
        return_errors:            optional bool. Defaults to True for send
                                  methods, False for GET (matches live samples).
        return_mime_response:     optional bool, defaults to False.
        return_responses:         optional bool, defaults to True. SEND only;
                                  ignored for GET (the attribute is omitted
                                  on HttpGetAction per the live reference).
        folder_name:              optional, defaults to "Home".
        description:              optional.
        payload_source_ref_key:   plan-only metadata for upstream payload
                                  mapping/transformation. Never appears in
                                  emitted XML.
        credential_ref:           plan-only opaque credential reference.
                                  Never appears in emitted XML.
    """

    SUPPORTED_OPERATION_MODES = ("send",)
    SUPPORTED_METHODS = ("GET", "POST", "PUT", "PATCH", "DELETE")
    GET_METHODS = ("GET",)
    SEND_METHODS = ("POST", "PUT", "PATCH", "DELETE")
    HEADER_KEY_START = 1000000
    PATH_KEY_START = 2000000

    # Shared with the database builders — secrets must cross the wire as
    # opaque credential_ref strings only.
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
    def _validate_headers(cls, headers: Any) -> Optional[BuilderValidationError]:
        if headers is None:
            return None
        if not isinstance(headers, list):
            return BuilderValidationError(
                f"headers must be a list of objects, got {type(headers).__name__}",
                error_code="HTTP_OPERATION_VALIDATION_FAILED",
                field="headers",
                hint=(
                    "Pass headers as a JSON list: "
                    "[{'name': 'Accept', 'value': 'application/json'}, ...]."
                ),
            )
        for index, entry in enumerate(headers):
            if not isinstance(entry, dict):
                return BuilderValidationError(
                    f"headers[{index}] must be an object, "
                    f"got {type(entry).__name__}",
                    error_code="HTTP_OPERATION_VALIDATION_FAILED",
                    field=f"headers[{index}]",
                    hint=(
                        "Each header entry is "
                        "{'name': str, 'value': str?, 'is_variable': bool?}."
                    ),
                )
            name = entry.get("name")
            if not name or not str(name).strip():
                return BuilderValidationError(
                    f"headers[{index}].name is required",
                    error_code="HTTP_OPERATION_VALIDATION_FAILED",
                    field=f"headers[{index}].name",
                    hint="Provide a non-empty header name (e.g. 'Authorization').",
                )
            if "is_variable" in entry and not isinstance(entry["is_variable"], bool):
                return BuilderValidationError(
                    f"headers[{index}].is_variable must be a bool",
                    error_code="HTTP_OPERATION_VALIDATION_FAILED",
                    field=f"headers[{index}].is_variable",
                    hint="Use true or false.",
                )
            if "value" in entry and entry["value"] is not None and not isinstance(entry["value"], str):
                return BuilderValidationError(
                    f"headers[{index}].value must be a string",
                    error_code="HTTP_OPERATION_VALIDATION_FAILED",
                    field=f"headers[{index}].value",
                    hint="Use a JSON string (may be empty).",
                )
        return None

    @classmethod
    def validate_config(cls, config: Dict[str, Any]) -> Optional[BuilderValidationError]:
        """Validate an HTTP send-op config without building XML."""
        # 1) Plaintext secret-shaped keys (defensive).
        secret_err = cls.scan_forbidden_secret_fields(config)
        if secret_err is not None:
            return secret_err

        # 2) operation_mode must be 'send' (only buildable mode in issue #24).
        operation_mode = (config.get("operation_mode") or "").lower()
        if operation_mode not in cls.SUPPORTED_OPERATION_MODES:
            supported = ", ".join(cls.SUPPORTED_OPERATION_MODES)
            return BuilderValidationError(
                f"operation_mode is required and must be one of: {supported}",
                error_code="UNSUPPORTED_HTTP_OPERATION_MODE",
                field="operation_mode",
                hint=(
                    f"Supported operation_modes: {supported}. The GET/SEND "
                    "split lives inside the builder (method='GET' → "
                    "HttpGetAction; POST/PUT/PATCH/DELETE → HttpSendAction)."
                ),
            )

        # 3) component_name required.
        component_name = config.get("component_name")
        if not component_name or not str(component_name).strip():
            return BuilderValidationError(
                "component_name is required",
                error_code="HTTP_OPERATION_VALIDATION_FAILED",
                field="component_name",
                hint="Provide a non-empty component_name string.",
            )

        # 4) connection_ref_key required (cross-step depends_on check lives
        # in integration_builder._check_http_send_dependencies).
        connection_ref_key = config.get("connection_ref_key")
        if not connection_ref_key or not str(connection_ref_key).strip():
            return BuilderValidationError(
                "connection_ref_key is required for HTTP send operations",
                error_code="MISSING_HTTP_DEPENDENCY",
                field="connection_ref_key",
                hint=(
                    "Declare the HTTP connector-settings key the operation "
                    "will bind to at process time, and add the same key to "
                    "depends_on so plan ordering is correct."
                ),
            )

        # 5) method must be one of SUPPORTED_METHODS (case-insensitive input).
        raw_method = config.get("method")
        method = (raw_method or "").upper() if isinstance(raw_method, str) else ""
        if not method or method not in cls.SUPPORTED_METHODS:
            supported_methods = ", ".join(cls.SUPPORTED_METHODS)
            return BuilderValidationError(
                f"method {raw_method!r} is not supported (supported: {supported_methods})",
                error_code="UNSUPPORTED_HTTP_METHOD",
                field="method",
                hint=f"Supported methods: {supported_methods}.",
            )

        # 6) path required.
        path = config.get("path")
        if path is None or not str(path).strip():
            return BuilderValidationError(
                "path is required for HTTP send operations",
                error_code="HTTP_OPERATION_VALIDATION_FAILED",
                field="path",
                hint=(
                    "Provide the endpoint path (e.g. '/v1/items'). Exactly "
                    "one leading '/' is stripped on emission."
                ),
            )

        # 7) request_profile_id (when supplied): bare '$ref:' is meaningless.
        request_profile_id = config.get("request_profile_id")
        if isinstance(request_profile_id, str) and request_profile_id.startswith("$ref:"):
            if not request_profile_id[5:]:
                return BuilderValidationError(
                    "request_profile_id $ref token is empty (expected '$ref:KEY')",
                    error_code="MISSING_HTTP_REQUEST_PROFILE_REF",
                    field="request_profile_id",
                    hint=(
                        "Use '$ref:target_json_profile' to reference a "
                        "profile component declared earlier in the same "
                        "integration spec."
                    ),
                )

        # 8) headers (when supplied): shape check.
        header_err = cls._validate_headers(config.get("headers"))
        if header_err is not None:
            return header_err

        return None

    @staticmethod
    def _strip_leading_slash(path: str) -> str:
        return path[1:] if path.startswith("/") else path

    @classmethod
    def _header_xml(cls, headers: Optional[list]) -> str:
        if not headers:
            return "                <requestHeaders/>\n"
        lines = ["                <requestHeaders>"]
        for index, entry in enumerate(headers):
            key = cls.HEADER_KEY_START + index
            name = _escape_xml(str(entry.get("name", "")))
            is_variable = bool(entry.get("is_variable", False))
            if is_variable:
                lines.append(
                    f'                    <header headerName="{name}" '
                    f'headerValue="" isVariable="true" key="{key}"/>'
                )
            else:
                value = _escape_xml(str(entry.get("value", "")))
                lines.append(
                    f'                    <header headerName="{name}" '
                    f'headerValue="{value}" key="{key}"/>'
                )
        lines.append("                </requestHeaders>")
        return "\n".join(lines) + "\n"

    def build(self, **params) -> str:
        error = self.validate_config(params)
        if error is not None:
            raise error

        component_name = params["component_name"]
        method = params["method"].upper()
        path_raw = str(params["path"])
        path_name = _escape_xml(self._strip_leading_slash(path_raw))

        folder_name = params.get("folder_name", "Home")
        description = params.get("description", "")

        data_content_type = _escape_xml(
            str(params.get("content_type", "application/json"))
        )
        follow_redirects = _format_xml_value(params.get("follow_redirects", False))
        mime_passthrough = _format_xml_value(params.get("mime_passthrough", False))
        request_profile_type = _escape_xml(
            str(params.get("request_profile_type", "NONE"))
        )
        response_profile_type = _escape_xml(
            str(params.get("response_profile_type", "NONE"))
        )
        return_mime_response = _format_xml_value(
            params.get("return_mime_response", False)
        )

        is_get = method in self.GET_METHODS
        return_errors_default = False if is_get else True
        return_errors = _format_xml_value(
            params.get("return_errors", return_errors_default)
        )

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

        headers_xml = self._header_xml(params.get("headers"))

        if is_get:
            action_open = (
                f'<HttpGetAction dataContentType="{data_content_type}"'
                f' followRedirects="{follow_redirects}"'
                f' methodType="{method}"'
                f' mimePassthrough="{mime_passthrough}"'
                f'{request_profile_attr}'
                f' requestProfileType="{request_profile_type}"'
                f'{response_profile_attr}'
                f' responseProfileType="{response_profile_type}"'
                f' returnErrors="{return_errors}"'
                f' returnMimeResponse="{return_mime_response}">'
            )
            action_close = "</HttpGetAction>"
        else:
            return_responses = _format_xml_value(
                params.get("return_responses", True)
            )
            action_open = (
                f'<HttpSendAction dataContentType="{data_content_type}"'
                f' followRedirects="{follow_redirects}"'
                f' methodType="{method}"'
                f' mimePassthrough="{mime_passthrough}"'
                f'{request_profile_attr}'
                f' requestProfileType="{request_profile_type}"'
                f'{response_profile_attr}'
                f' responseProfileType="{response_profile_type}"'
                f' returnErrors="{return_errors}"'
                f' returnMimeResponse="{return_mime_response}"'
                f' returnResponses="{return_responses}">'
            )
            action_close = "</HttpSendAction>"

        path_element_xml = (
            f'                <pathElements>'
            f'<element key="{self.PATH_KEY_START}" name="{path_name}"/>'
            f'</pathElements>\n'
        )

        action_xml = (
            f"            {action_open}\n"
            f"{headers_xml}"
            f"{path_element_xml}"
            f"                <responseHeaderMapping/>\n"
            f"                <reflectHeaders/>\n"
            f"            {action_close}"
        )

        safe_name = _escape_xml(component_name)
        safe_folder = _escape_xml(folder_name)
        safe_desc = _escape_xml(description)

        return (
            '<?xml version="1.0" encoding="UTF-8"?>\n'
            '<bns:Component xmlns:bns="http://api.platform.boomi.com/"\n'
            '               xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance"\n'
            '               type="connector-action" subType="http"\n'
            f'               name="{safe_name}"\n'
            f'               folderName="{safe_folder}">\n'
            '    <bns:encryptedValues/>\n'
            f'    <bns:description>{safe_desc}</bns:description>\n'
            '    <bns:object>\n'
            '        <Operation xmlns="">\n'
            '            <Archiving directory="" enabled="false"/>\n'
            '            <Configuration>\n'
            f"{action_xml}\n"
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
    "http": HttpConnectorBuilder,
    "database": DatabaseConnectorBuilder,
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
    ("http", "send"): HttpClientOperationBuilder,
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

"""Issue #133 (M6.1, parent #12) — ApiServiceBuilder.

Emits ``<bns:Component type="webservice">`` XML (an API Service Component,
"ASC") from a structured config declaring the REST routes that publish WSS
Listen processes on ``apiType=advanced`` runtimes. On advanced runtimes bare
``/ws/simple`` paths are NOT served (live-confirmed 404, 2026-07-04) — routes
exist only through an ASC, served under ``/ws/rest/...`` with case-VERBATIM
path segments.

Reference XML shape evidence (live-captured; fixture under
``tests/fixtures/live_xml/m6/``):

* renera ``f7a605a0-732f-4ad8-a479-f59c5034bf45`` "New API Service" v2 —
  serving ``POST /ws/rest/generalListener`` -> 200 on the advanced cloud
  attachment (``api_service_minimal.xml``).

Envelope shape:

.. code-block:: xml

    <bns:Component type="webservice" name="...">
      <bns:encryptedValues/>
      <bns:description>...</bns:description>
      <bns:object>
        <webservice xmlns="" urlPath="">
          <restApi>
            <route processId="...">
              <overrides httpMethod="POST" inputProfileKey="" inputType=""
                         objectName="" outputType="" urlPath=""/>
              <description/>
            </route>
          </restApi>
          <soapApi fullEnvelopePassthrough="false" singleWsdlSchema="false"
                   suppressWrappers="false" wsdlNamespace="" wsdlServiceName="">
            <SOAPVersion>SOAP_1_1</SOAPVersion>
          </soapApi>
          <odataApi/>
          <metaInfo contactEmail="" contactName="" contactUrl=""
                    licenseName="" licenseUrl="" title="..." version="1.0.0">
            <description/><termsOfService/>
          </metaInfo>
          <profileOverrides/>
          <capturedHeaders/>
          <apiRoles/>
        </webservice>
      </bns:object>
    </bns:Component>

Key shape facts (from the live capture — do not invent):

* ``<webservice>`` resets ``xmlns=""`` (same pattern as the sibling builders'
  object roots).
* Route ``<overrides>`` attributes are exactly ``httpMethod``,
  ``inputProfileKey``, ``inputType``, ``objectName``, ``outputType``,
  ``urlPath`` — EMPTY STRING means "inherit from the linked WSS Listen
  operation" (per-attribute). Live-confirmed: empty ASC base + all-inherit
  route serves ``/ws/rest/{WSS-op objectName}`` (camelCase verbatim).
* Mandatory placeholder children even when unused: ``<soapApi>`` (with
  ``<SOAPVersion>SOAP_1_1</SOAPVersion>``), ``<odataApi/>``, ``<metaInfo>``
  (with ``<description/>`` + ``<termsOfService/>``), ``<profileOverrides/>``,
  ``<capturedHeaders/>``, ``<apiRoles/>``.
* ``<profileOverrides>`` is NEVER authored programmatically — profiles are
  driven from the WSS operation; a populated live block must survive
  structured updates (see PRESERVATION_POLICY: the tag is deliberately
  excluded from ``owned_child_tags``).
* Every route ``processId`` must point to a process with a WSS Listen start
  (``actionType="Listen"``, connector subType ``wss``) — the builder cannot
  see other components, so ``$ref`` route targets are checked at
  integration-builder plan time and literal ids at analyze/orchestration time.
* ``componentId`` / ``version`` / dates are server-assigned and ABSENT on
  create (the shared create/update invariant).
"""

from __future__ import annotations

import uuid
from typing import Any, Dict, List, Mapping, Optional, Tuple

from ._api_service_paths import api_service_http_method, compute_asc_endpoint
from ._preservation_policy import OwnedPath, PreservationPolicy
from .connector_builder import (
    _WSS_INPUT_TYPES,
    BuilderValidationError,
    _escape_xml,
)
from .profile_generation import (
    API_SERVICE_DUPLICATE_ROUTE,
    API_SERVICE_METHOD_UNSUPPORTED,
    API_SERVICE_NAME_REQUIRED,
    API_SERVICE_PROFILE_OVERRIDES_UNSUPPORTED,
    API_SERVICE_RAW_XML_UNSUPPORTED,
    API_SERVICE_ROUTE_PROCESS_REF_INVALID,
    API_SERVICE_ROUTE_PROCESS_REQUIRED,
    API_SERVICE_ROUTES_REQUIRED,
    API_SERVICE_TYPE_UNSUPPORTED,
    API_SERVICE_VALIDATION_FAILED,
)


_SUPPORTED_HTTP_METHODS: Tuple[str, ...] = ("GET", "POST", "PUT", "DELETE", "PATCH")


# Mirrors ScriptMappingBuilder's secret-shaped key set so one audit covers
# every generated-component builder family.
_FORBIDDEN_SECRET_FIELDS: Tuple[str, ...] = (
    "password",
    "password_ref",
    "secret",
    "token",
    "access_token",
    "client_secret",
    "api_key",
    "credentials",
    "authorization",
    "bearer",
)


# Top-level config keys this builder accepts. ``component_type`` / ``xml``
# are integration-builder plumbing (same rationale as the sibling builders'
# allow-lists).
_ALLOWED_TOP_LEVEL_KEYS: Tuple[str, ...] = (
    "component_type",
    "component_name",
    "folder_path",
    "description",
    "base_url_path",
    "title",
    "version",
    "routes",
    "xml",
)

# Raw-XML subtree smuggling — the builder owns the <webservice> subtree.
_REJECTED_TOP_LEVEL_HINTS: Dict[str, str] = {
    "profile_overrides": (
        "profileOverrides is never authored programmatically — drive request/"
        "response profiles from the WSS Listen operation. Existing live "
        "profileOverrides survive structured updates automatically."
    ),
    "profileOverrides": (
        "profileOverrides is never authored programmatically — drive request/"
        "response profiles from the WSS Listen operation. Existing live "
        "profileOverrides survive structured updates automatically."
    ),
}
_RAW_SUBTREE_KEYS: Tuple[str, ...] = (
    "rest_api",
    "restApi",
    "soap_api",
    "soapApi",
    "odata_api",
    "odataApi",
    "meta_info",
    "metaInfo",
    "api_roles",
    "apiRoles",
    "captured_headers",
    "capturedHeaders",
    "webservice",
    "object",
    "bns_object",
)


# Per-route config keys. Empty string is MEANINGFUL ("inherit from the linked
# WSS Listen operation") and must be preserved — never normalized to None.
_ALLOWED_ROUTE_KEYS: Tuple[str, ...] = (
    "process",
    "process_id",
    "http_method",
    "url_path",
    "object_name",
    "input_type",
    "output_type",
    "input_profile_key",
    "description",
)


def _scan_forbidden_secret_fields(
    config: Any, _path_prefix: str = ""
) -> Optional[BuilderValidationError]:
    """Recursive secret-shaped key scan (dict keys only)."""
    if isinstance(config, dict):
        for key in _FORBIDDEN_SECRET_FIELDS:
            if key in config:
                field_path = f"{_path_prefix}{key}"
                return BuilderValidationError(
                    f"{field_path!r} cannot be supplied in a webservice "
                    "config — API Service Components must not transport "
                    "credentials.",
                    error_code="PLAINTEXT_SECRET_REJECTED",
                    field=field_path,
                    hint=(
                        "Remove the secret-shaped field. Endpoint auth comes "
                        "from the Shared Web Server / gateway configuration, "
                        "never from component XML."
                    ),
                )
        for key, value in config.items():
            nested = _scan_forbidden_secret_fields(
                value, _path_prefix=f"{_path_prefix}{key}."
            )
            if nested is not None:
                return nested
    elif isinstance(config, list):
        base = _path_prefix[:-1] if _path_prefix.endswith(".") else _path_prefix
        for index, item in enumerate(config):
            nested = _scan_forbidden_secret_fields(
                item, _path_prefix=f"{base}[{index}]."
            )
            if nested is not None:
                return nested
    return None


def _is_uuid(value: str) -> bool:
    # Canonical lowercase form only — component ids are emitted verbatim into
    # route processId attributes.
    try:
        return str(uuid.UUID(value)) == value
    except (ValueError, AttributeError, TypeError):
        return False


def _route_process_value(entry: Mapping[str, Any]) -> Optional[str]:
    """The route's process reference — ``process`` wins over the
    ``process_id`` alias; None when neither is supplied."""
    for key in ("process", "process_id"):
        value = entry.get(key)
        if value is not None:
            return value
    return None


class ApiServiceBuilder:
    """Emit ``webservice`` (API Service Component) XML from structured config."""

    SUPPORTED_COMPONENT_TYPES: Tuple[str, ...] = ("webservice",)
    FORBIDDEN_SECRET_FIELDS: Tuple[str, ...] = _FORBIDDEN_SECRET_FIELDS
    SUPPORTED_HTTP_METHODS: Tuple[str, ...] = _SUPPORTED_HTTP_METHODS

    # ------------------------------------------------------------------
    # Public secret-scan helpers (mirrors the sibling builder shape).
    # ------------------------------------------------------------------

    @classmethod
    def scan_forbidden_secret_fields(
        cls, config: Any, _path_prefix: str = ""
    ) -> Optional[BuilderValidationError]:
        return _scan_forbidden_secret_fields(config, _path_prefix=_path_prefix)

    @classmethod
    def redact_forbidden_secret_fields_in_place(cls, config: Any) -> None:
        if isinstance(config, dict):
            for key in cls.FORBIDDEN_SECRET_FIELDS:
                if key in config:
                    config[key] = "[REDACTED]"
            for value in config.values():
                cls.redact_forbidden_secret_fields_in_place(value)
        elif isinstance(config, list):
            for item in config:
                cls.redact_forbidden_secret_fields_in_place(item)

    # ------------------------------------------------------------------
    # Validation
    # ------------------------------------------------------------------

    @classmethod
    def validate_config(
        cls, config: Dict[str, Any]
    ) -> Optional[BuilderValidationError]:
        """Validate a webservice config; return None on success."""
        # 1. Secret-shaped key scan (deep).
        secret_err = cls.scan_forbidden_secret_fields(config)
        if secret_err is not None:
            return secret_err

        # 2. Unknown top-level keys — profileOverrides and raw subtree
        # smuggling get dedicated codes/hints.
        for key in config.keys():
            if key in _ALLOWED_TOP_LEVEL_KEYS:
                continue
            if key in _REJECTED_TOP_LEVEL_HINTS:
                return BuilderValidationError(
                    f"{key!r} is not accepted — profileOverrides is never "
                    "authored through the builder.",
                    error_code=API_SERVICE_PROFILE_OVERRIDES_UNSUPPORTED,
                    field=key,
                    hint=_REJECTED_TOP_LEVEL_HINTS[key],
                )
            if key in _RAW_SUBTREE_KEYS:
                return BuilderValidationError(
                    f"{key!r} is not accepted — the builder owns the "
                    "<webservice> subtree.",
                    error_code=API_SERVICE_RAW_XML_UNSUPPORTED,
                    field=key,
                    hint=(
                        "Declare 'routes' entries (plus base_url_path/title/"
                        "version) instead of raw XML. The raw-XML escape hatch "
                        "is config['xml'], handled upstream of this builder."
                    ),
                )
            return BuilderValidationError(
                f"unknown top-level field {key!r} for webservice",
                error_code=API_SERVICE_VALIDATION_FAILED,
                field=key,
                hint=f"Supported top-level keys: {sorted(_ALLOWED_TOP_LEVEL_KEYS)}.",
            )

        # 3. component_name.
        component_name = config.get("component_name")
        if not isinstance(component_name, str) or not component_name.strip():
            return BuilderValidationError(
                "component_name is required",
                error_code=API_SERVICE_NAME_REQUIRED,
                field="component_name",
                hint="Provide a non-empty component_name string.",
            )

        # 4. Simple string envelope fields.
        for str_key in ("base_url_path", "title", "version", "description", "folder_path"):
            value = config.get(str_key)
            if value is not None and not isinstance(value, str):
                return BuilderValidationError(
                    f"{str_key} must be a string when provided",
                    error_code=API_SERVICE_VALIDATION_FAILED,
                    field=str_key,
                    hint="Pass a string (may be empty), or omit the key.",
                )

        # 5. routes list.
        routes = config.get("routes")
        if not isinstance(routes, list) or not routes:
            return BuilderValidationError(
                "routes must be a non-empty list of route definitions",
                error_code=API_SERVICE_ROUTES_REQUIRED,
                field="routes",
                hint=(
                    "Each entry is {process: '$ref:KEY' | <process component "
                    "UUID>, http_method?, url_path?, object_name?, "
                    "input_type?, output_type?, description?} — empty string "
                    "means 'inherit from the linked WSS Listen operation'."
                ),
            )

        seen_exact: Dict[Tuple[Any, ...], str] = {}
        seen_effective: Dict[Tuple[str, str], str] = {}
        for index, entry in enumerate(routes):
            field_prefix = f"routes[{index}]"
            if not isinstance(entry, Mapping):
                return BuilderValidationError(
                    f"{field_prefix} must be a mapping object",
                    error_code=API_SERVICE_VALIDATION_FAILED,
                    field=field_prefix,
                )
            for key in entry.keys():
                if key in _ALLOWED_ROUTE_KEYS:
                    continue
                if key in ("overrides", "profile_overrides", "profileOverrides"):
                    return BuilderValidationError(
                        f"{field_prefix}.{key} is not supported",
                        error_code=API_SERVICE_PROFILE_OVERRIDES_UNSUPPORTED
                        if "profile" in key.lower()
                        else API_SERVICE_RAW_XML_UNSUPPORTED,
                        field=f"{field_prefix}.{key}",
                        hint=(
                            "Route override attributes are first-class route "
                            "keys (http_method/url_path/object_name/input_type/"
                            "output_type); profiles come from the WSS operation."
                        ),
                    )
                return BuilderValidationError(
                    f"{field_prefix}.{key} is not supported",
                    error_code=API_SERVICE_VALIDATION_FAILED,
                    field=f"{field_prefix}.{key}",
                    hint=f"Allowed route keys: {sorted(_ALLOWED_ROUTE_KEYS)}.",
                )

            # process reference — required; '$ref:KEY' or canonical UUID.
            process_value = _route_process_value(entry)
            if process_value is None or (
                isinstance(process_value, str) and not process_value.strip()
            ):
                return BuilderValidationError(
                    f"{field_prefix}.process is required",
                    error_code=API_SERVICE_ROUTE_PROCESS_REQUIRED,
                    field=f"{field_prefix}.process",
                    hint=(
                        "Reference the route's WSS Listen process: '$ref:KEY' "
                        "for an in-spec process or a literal process component "
                        "UUID."
                    ),
                )
            if not isinstance(process_value, str):
                return BuilderValidationError(
                    f"{field_prefix}.process must be a string",
                    error_code=API_SERVICE_ROUTE_PROCESS_REF_INVALID,
                    field=f"{field_prefix}.process",
                    hint="Pass '$ref:KEY' or a process component UUID string.",
                )
            process_ref = process_value.strip()
            if process_ref.startswith("$ref:"):
                if not process_ref[len("$ref:"):].strip():
                    return BuilderValidationError(
                        f"{field_prefix}.process '$ref:' token must name a "
                        "non-empty key ('$ref:KEY')",
                        error_code=API_SERVICE_ROUTE_PROCESS_REF_INVALID,
                        field=f"{field_prefix}.process",
                    )
            elif not _is_uuid(process_ref):
                return BuilderValidationError(
                    f"{field_prefix}.process must be '$ref:KEY' or a lowercase "
                    f"canonical component UUID (got {process_ref!r})",
                    error_code=API_SERVICE_ROUTE_PROCESS_REF_INVALID,
                    field=f"{field_prefix}.process",
                    hint="Example: c991a424-e7e3-4af1-b2ab-3ddba4a43974.",
                )

            # http_method — empty string means inherit; non-empty must be in
            # the supported vocabulary.
            http_method = entry.get("http_method")
            if http_method is not None and not isinstance(http_method, str):
                return BuilderValidationError(
                    f"{field_prefix}.http_method must be a string when provided",
                    error_code=API_SERVICE_METHOD_UNSUPPORTED,
                    field=f"{field_prefix}.http_method",
                )
            method_token = str(http_method or "").strip().upper()
            if method_token and method_token not in _SUPPORTED_HTTP_METHODS:
                return BuilderValidationError(
                    f"{field_prefix}.http_method must be one of "
                    f"{_SUPPORTED_HTTP_METHODS} or '' to inherit "
                    f"(got {http_method!r})",
                    error_code=API_SERVICE_METHOD_UNSUPPORTED,
                    field=f"{field_prefix}.http_method",
                    hint=(
                        "Empty string inherits the method from the WSS "
                        "operation (input_type none -> GET, else POST)."
                    ),
                )

            # input_type / output_type — empty string means inherit; non-empty
            # must use the WSS vocabulary.
            for type_key in ("input_type", "output_type"):
                type_value = entry.get(type_key)
                if type_value is not None and not isinstance(type_value, str):
                    return BuilderValidationError(
                        f"{field_prefix}.{type_key} must be a string when provided",
                        error_code=API_SERVICE_TYPE_UNSUPPORTED,
                        field=f"{field_prefix}.{type_key}",
                    )
                type_token = str(type_value or "").strip().lower()
                if type_token and type_token not in _WSS_INPUT_TYPES:
                    return BuilderValidationError(
                        f"{field_prefix}.{type_key} must be one of "
                        f"{sorted(_WSS_INPUT_TYPES)} or '' to inherit "
                        f"(got {type_value!r})",
                        error_code=API_SERVICE_TYPE_UNSUPPORTED,
                        field=f"{field_prefix}.{type_key}",
                    )

            # Remaining string overrides.
            for str_key in ("url_path", "object_name", "input_profile_key", "description"):
                value = entry.get(str_key)
                if value is not None and not isinstance(value, str):
                    return BuilderValidationError(
                        f"{field_prefix}.{str_key} must be a string when provided",
                        error_code=API_SERVICE_VALIDATION_FAILED,
                        field=f"{field_prefix}.{str_key}",
                        hint="Pass a string (may be empty = inherit), or omit the key.",
                    )

            # Duplicate detection.
            exact_key = (
                process_ref,
                method_token,
                str(entry.get("url_path") or "").strip(),
                str(entry.get("object_name") or "").strip(),
                str(entry.get("input_type") or "").strip().lower(),
                str(entry.get("output_type") or "").strip().lower(),
            )
            if exact_key in seen_exact:
                return BuilderValidationError(
                    f"{field_prefix} duplicates {seen_exact[exact_key]} "
                    "(identical process + overrides)",
                    error_code=API_SERVICE_DUPLICATE_ROUTE,
                    field=field_prefix,
                    hint=(
                        "Two identical routes resolve to the same served path "
                        "— remove one."
                    ),
                )
            seen_exact[exact_key] = field_prefix

            # Effective (method, path) collision — only when computable
            # WITHOUT WSS-op inheritance: an explicit object_name pins the
            # path, and an explicit http_method or input_type pins the
            # method. Inherit-dependent routes are collision-checked at
            # analyze/orchestration time where the linked operation is
            # readable.
            object_name = str(entry.get("object_name") or "").strip()
            input_type_token = str(entry.get("input_type") or "").strip().lower()
            if object_name and (method_token or input_type_token):
                effective_method = method_token or api_service_http_method(
                    "", input_type_token
                )
                effective_path = compute_asc_endpoint(
                    str(config.get("base_url_path") or ""),
                    object_name,
                    str(entry.get("url_path") or ""),
                )
                effective = (effective_method, effective_path)
                if effective in seen_effective:
                    return BuilderValidationError(
                        f"{field_prefix} resolves to the same effective route "
                        f"as {seen_effective[effective]} "
                        f"({effective_method} {effective_path})",
                        error_code=API_SERVICE_DUPLICATE_ROUTE,
                        field=field_prefix,
                        hint=(
                            "Duplicate effective paths all deploy active=true "
                            "but only the FIRST-deployed serves (silent "
                            "shadowing) — make the paths distinct."
                        ),
                    )
                seen_effective[effective] = field_prefix

        return None

    # ------------------------------------------------------------------
    # XML emission
    # ------------------------------------------------------------------

    def build(self, **params: Any) -> str:
        """Emit the ``<bns:Component type='webservice'>`` XML."""
        config = dict(params)
        validation_err = self.validate_config(config)
        if validation_err is not None:
            raise validation_err

        component_name = str(config["component_name"]).strip()
        folder_path = config.get("folder_path")
        description = config.get("description") or ""
        base_url_path = str(config.get("base_url_path") or "").strip()
        # metaInfo title/version are OpenAPI display metadata (live capture:
        # title="test" version="1.0.0"); default the title to the component
        # name so generated catalogs stay meaningful.
        title = str(config.get("title") or "").strip() or component_name
        version = str(config.get("version") or "").strip() or "1.0.0"

        route_parts: List[str] = []
        for entry in config["routes"]:
            process_ref = str(_route_process_value(entry)).strip()
            if process_ref.startswith("$ref:"):
                # $ref tokens are resolved by build_integration BEFORE the
                # builder emits — reaching emission unresolved means the
                # standalone manage_component path was handed an in-spec ref.
                raise BuilderValidationError(
                    f"route process {process_ref!r} is an unresolved $ref at "
                    "emission time",
                    error_code=API_SERVICE_ROUTE_PROCESS_REF_INVALID,
                    field="routes",
                    hint=(
                        "manage_component create takes literal process "
                        "component UUIDs; '$ref:KEY' routes are only resolved "
                        "inside build_integration specs."
                    ),
                )
            method_token = str(entry.get("http_method") or "").strip().upper()
            route_description = str(entry.get("description") or "")
            description_xml = (
                f"<description>{_escape_xml(route_description)}</description>"
                if route_description
                else "<description/>"
            )
            # Attribute order pinned to the live capture (alphabetical):
            # httpMethod, inputProfileKey, inputType, objectName, outputType,
            # urlPath. Empty string = inherit, emitted verbatim.
            route_parts.append(
                f'<route processId="{_escape_xml(process_ref)}">'
                "<overrides"
                f' httpMethod="{_escape_xml(method_token)}"'
                f' inputProfileKey="{_escape_xml(str(entry.get("input_profile_key") or "").strip())}"'
                f' inputType="{_escape_xml(str(entry.get("input_type") or "").strip().lower())}"'
                f' objectName="{_escape_xml(str(entry.get("object_name") or "").strip())}"'
                f' outputType="{_escape_xml(str(entry.get("output_type") or "").strip().lower())}"'
                f' urlPath="{_escape_xml(str(entry.get("url_path") or "").strip())}"'
                "/>"
                f"{description_xml}"
                "</route>"
            )

        folder_attr = (
            f' folderFullPath="{_escape_xml(str(folder_path))}"'
            if folder_path
            else ""
        )
        return (
            '<bns:Component xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance" '
            'xmlns:bns="http://api.platform.boomi.com/" '
            f'type="webservice"{folder_attr} '
            f'name="{_escape_xml(component_name)}">'
            "<bns:encryptedValues/>"
            f"<bns:description>{_escape_xml(description)}</bns:description>"
            "<bns:object>"
            f'<webservice xmlns="" urlPath="{_escape_xml(base_url_path)}">'
            f"<restApi>{''.join(route_parts)}</restApi>"
            '<soapApi fullEnvelopePassthrough="false" singleWsdlSchema="false" '
            'suppressWrappers="false" wsdlNamespace="" wsdlServiceName="">'
            "<SOAPVersion>SOAP_1_1</SOAPVersion>"
            "</soapApi>"
            "<odataApi/>"
            '<metaInfo contactEmail="" contactName="" contactUrl="" '
            'licenseName="" licenseUrl="" '
            f'title="{_escape_xml(title)}" version="{_escape_xml(version)}">'
            "<description/><termsOfService/>"
            "</metaInfo>"
            "<profileOverrides/>"
            "<capturedHeaders/>"
            "<apiRoles/>"
            "</webservice>"
            "</bns:object>"
            "</bns:Component>"
        )


# Component-type registry — single-key dispatch, same shape as
# PROCESS_PROPERTY_BUILDERS.
API_SERVICE_BUILDERS: Dict[str, type] = {
    "webservice": ApiServiceBuilder,
}


# Update-preservation policy: subtree_merge on <webservice>. ALL SEVEN child
# blocks are owned for ORDERING — the platform XSD requires the exact
# sequence restApi/soapApi/odataApi/metaInfo/profileOverrides/capturedHeaders/
# apiRoles, and leaving profileOverrides unowned displaced it past apiRoles
# in the merge (live 400 "Invalid content ... 'profileOverrides'", #133 QA
# bug #148). profileOverrides CONTENT is still never authored:
# preserve_when_desired_empty keeps the live element whenever the builder's
# placeholder is empty (which it always is), so UI/platform-populated profile
# overrides survive structured updates in their canonical slot.
ApiServiceBuilder.PRESERVATION_POLICY = PreservationPolicy(
    component_type="webservice",
    owned_paths=(
        OwnedPath(
            path="bns:object/webservice",
            mode="subtree_merge",
            owned_attrs=("urlPath",),
            owned_child_tags=(
                "restApi",
                "soapApi",
                "odataApi",
                "metaInfo",
                "profileOverrides",
                "capturedHeaders",
                "apiRoles",
            ),
            preserve_when_desired_empty=("profileOverrides",),
        ),
    ),
)


def get_api_service_builder(component_type: str) -> Optional[type]:
    """Return the builder class for ``component_type`` or ``None``."""
    return API_SERVICE_BUILDERS.get(component_type)

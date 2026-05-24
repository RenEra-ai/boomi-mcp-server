"""
Process-flow XML builder for structured process orchestration (issue #25).

Owns Boomi process Component XML emission for `process_kind` archetypes
that wire DB/REST/other connector-actions together. Today supports
`database_to_api_sync` (M2.5 vertical slice) — a deterministic
Start -> [optional transform] -> Target -> Stop flow whose shape XML
uses `shapetype="connectoraction"` (matches live Renera examples like
`DB Test`, `Rest Test GET`, `Rest Test PATCH`).

Try/Catch retry and DLQ wrappers are intentionally deferred until live
Try/Catch XML is captured (see PROCESS_RETRY_UNVERIFIED). Map and
subprocess components are referenced by id/$ref only — their build is
out of scope.
"""

from __future__ import annotations

import re
import xml.etree.ElementTree as ET
from typing import Any, Dict, Iterable, List, Optional, Tuple

from .connector_builder import (
    BuilderValidationError,
    REST_CLIENT_SUBTYPE,
    _escape_xml,
    _resolve_rest_connector_type,
)


# REST HTTP methods supported by Boomi REST Client connector-action.
_REST_ACTION_TYPES = frozenset({
    "GET", "POST", "PUT", "PATCH", "DELETE", "HEAD", "OPTIONS", "TRACE",
})

# Database connector-action types supported here. M2 vertical slice is
# Get only; Send/Upsert are tracked by issue #32 and would need their own
# process-flow surface area.
_DB_ACTION_TYPES = frozenset({"Get"})

_SUPPORTED_TRANSFORM_MODES = frozenset({"passthrough", "message", "map_ref"})
_SUPPORTED_DLQ_MODES = frozenset({"disabled", "document_cache_ref", "error_subprocess_ref"})

# Per Boomi: Try/Catch retryCount ranges 0..5. Once retry is unlocked the
# validator will check that; today any value > 0 fails with
# PROCESS_RETRY_UNVERIFIED.
_MAX_RETRY_COUNT = 5

# Visual layout. Geometry is decorative only — process correctness is
# driven by toShape wiring. Numbers approximate the live Renera examples
# so the rendered diagram stays readable.
_SHAPE_Y = 96.0
_START_SHAPE_X = 96.0
_START_SHAPE_Y = 94.0
_SHAPE_X_STEP = 160.0
_DRAGPOINT_X_OFFSET = 144.0
_DRAGPOINT_Y = 104.0


def _shape_x(index: int) -> float:
    # index is 1-based.
    return _START_SHAPE_X + (index - 1) * _SHAPE_X_STEP


def _dragpoint_x(shape_index: int) -> float:
    return _shape_x(shape_index) + _DRAGPOINT_X_OFFSET


class ProcessFlowBuilder:
    """Builder for structured process components (process_kind dispatched).

    Public surface mirrors the database / REST builders so that
    integration_builder._build_plan and _apply_plan can treat all
    structured builders uniformly:

      - scan_forbidden_secret_fields(config) -> Optional[BuilderValidationError]
      - validate_config(config, *, depends_on) -> Optional[BuilderValidationError]
      - build(config, *, name, folder_name=None) -> str  # Component XML
    """

    PROCESS_KIND = "database_to_api_sync"

    # ------------------------------------------------------------------
    # Plan-time validation
    # ------------------------------------------------------------------

    # Substrings that mark a dict key as carrying a secret. Matching is
    # case-insensitive — every key is lowercased before the substring
    # check. This deliberately catches variants the connector contract
    # doesn't enforce (apiKey, db_password, AUTH_TOKEN, customerSecret,
    # etc.) because process configs are freeform user-provided JSON.
    #
    # `credential_ref` and similar `*_ref` keys do NOT contain any
    # forbidden substring — they carry URI references (credential://...),
    # not the secrets themselves.
    FORBIDDEN_SECRET_FIELDS: Tuple[str, ...] = (
        "password",
        "passcode",
        "secret",
        "private_key",
        "api_key",
        "apikey",
        "api-key",
        "auth_token",
        "access_token",
        "client_secret",
        "token",
        "authorization",
        "bearer",
        "credentials",
    )

    @classmethod
    def _key_matches_forbidden(cls, key: Any) -> Optional[str]:
        """Return the matched forbidden substring, or None.

        Case-insensitive substring scan — catches camelCase (apiKey),
        snake-prefixed (db_password), screaming-case (AUTH_TOKEN), and
        compound names (customerSecret). Codex review r4 P1 — exact-key
        membership was too narrow for freeform process configs.
        """
        if not isinstance(key, str):
            return None
        lowered = key.lower()
        for forbidden in cls.FORBIDDEN_SECRET_FIELDS:
            if forbidden in lowered:
                return forbidden
        return None

    @classmethod
    def scan_forbidden_secret_fields(
        cls, config: Any, _path_prefix: str = ""
    ) -> Optional[BuilderValidationError]:
        """Detect plaintext secret-shaped keys at any depth.

        At each dict level: case-insensitive substring match every key
        against FORBIDDEN_SECRET_FIELDS. A match flags the entire value
        — string non-empty (the obvious case) AND any dict / list
        container (`authorization: {"value": "..."}` style). Then
        recurse into non-matching subtrees in case a deeper key matches.

        Codex review r4 P1: the previous r3 exact-key scanner missed
        variant key names (apiKey, db_password) and container-shape
        secrets that the pre-r3 substring scanner caught.
        """
        if isinstance(config, dict):
            for key, value in config.items():
                matched = cls._key_matches_forbidden(key)
                if matched is not None:
                    path = f"{_path_prefix}{key}" if _path_prefix else key
                    # Reject both string leaves (the obvious case) AND
                    # container shapes where the secret is one level
                    # deeper. Empty strings still skip (matches the
                    # explicit "value and value" convention used by the
                    # DB builder for the same reason — empty defaults
                    # are not secrets).
                    if isinstance(value, str):
                        if value:
                            return cls._secret_rejection(path)
                    elif isinstance(value, (dict, list)):
                        return cls._secret_rejection(path)
                    # Scalars (None / bool / int) at a forbidden key
                    # carry no plaintext to leak — skip.
                    continue
                nested = cls.scan_forbidden_secret_fields(
                    value, _path_prefix=f"{_path_prefix}{key}."
                )
                if nested is not None:
                    return nested
        elif isinstance(config, list):
            for i, item in enumerate(config):
                nested = cls.scan_forbidden_secret_fields(
                    item, _path_prefix=f"{_path_prefix}[{i}]."
                )
                if nested is not None:
                    return nested
        # Scalars / None: no keys to scan.
        return None

    @classmethod
    def _secret_rejection(cls, path: str) -> BuilderValidationError:
        return BuilderValidationError(
            f"Plaintext secret-shaped field {path!r} is not allowed in "
            f"process config; reference connector secrets via a "
            f"connection_id / $ref:KEY token instead.",
            error_code="PLAINTEXT_SECRET_REJECTED",
            field=path,
            hint=(
                "Move credentials onto the connector-settings component "
                "and reference its connection_id from source/target."
            ),
        )

    @classmethod
    def redact_forbidden_secret_fields_in_place(cls, config: Any) -> None:
        """Recursively replace any FORBIDDEN_SECRET_FIELDS-keyed values
        with '[REDACTED]'.

        Matches the scan: at each dict level, any case-insensitive
        substring-matching key has its WHOLE value (string, dict, or
        list) replaced with `"[REDACTED]"`. Container-shape secrets
        (`{"password": {"plaintext": "..."}}`) are obliterated
        wholesale, mirroring DatabaseConnectorBuilder.redact's behavior.
        Codex review r4 P1.
        """
        if isinstance(config, dict):
            for key in list(config.keys()):
                if cls._key_matches_forbidden(key) is not None:
                    config[key] = "[REDACTED]"
                else:
                    cls.redact_forbidden_secret_fields_in_place(config[key])
        elif isinstance(config, list):
            for item in config:
                cls.redact_forbidden_secret_fields_in_place(item)
        # Scalars / None: no-op.

    @classmethod
    def validate_config(
        cls,
        config: Dict[str, Any],
        *,
        depends_on: Optional[Iterable[str]] = None,
    ) -> Optional[BuilderValidationError]:
        """Validate structured process config; return error or None.

        Validation order is intentional — surface the most-specific
        actionable error first:

          1. process_kind known
          2. source/target connector bindings well-formed
          3. transform mode supported
          4. reliability gating (retry/DLQ still unverified)
          5. $ref tokens reachable via depends_on
        """
        # str() coercion so non-string inputs (e.g. process_kind=123) fall
        # out as a clean structured PROCESS_KIND_UNSUPPORTED error instead
        # of raising AttributeError on .strip(). Codex review L1.
        process_kind = str(config.get("process_kind") or config.get("process_type") or "").strip()
        if process_kind != cls.PROCESS_KIND:
            return BuilderValidationError(
                f"process_kind {process_kind!r} is not supported.",
                error_code="PROCESS_KIND_UNSUPPORTED",
                field="process_kind",
                hint=(
                    f"Use process_kind={cls.PROCESS_KIND!r} for the M2.5 "
                    "database_to_api_sync builder. Other archetypes are "
                    "tracked by follow-up issues."
                ),
            )

        source_err = _validate_source_binding(config.get("source"))
        if source_err is not None:
            return source_err

        target_err = _validate_target_binding(config.get("target"))
        if target_err is not None:
            return target_err

        transform_err = _validate_transform(config.get("transform"))
        if transform_err is not None:
            return transform_err

        reliability_err = _validate_reliability(config.get("reliability"))
        if reliability_err is not None:
            return reliability_err

        # Dependency reachability: every $ref:KEY token in the config tree
        # must appear in depends_on. Matches integration_builder's
        # _resolve_dependency_tokens contract — apply-time substitution
        # walks the same tree, so undeclared refs would silently survive
        # as literal "$ref:KEY" strings in emitted XML.
        declared = set(depends_on or [])
        for path, value in _walk_scalars(config):
            if isinstance(value, str) and value.startswith("$ref:"):
                ref_key = value[5:]
                if ref_key not in declared:
                    return BuilderValidationError(
                        f"$ref:{ref_key} at {'.'.join(path)!r} is not "
                        f"declared in the process component's depends_on.",
                        error_code="MISSING_PROCESS_DEPENDENCY",
                        field="depends_on",
                        hint=(
                            f"Add {ref_key!r} to the process component's "
                            "depends_on list so $ref resolution can find it "
                            "at apply time."
                        ),
                    )

        return None

    # ------------------------------------------------------------------
    # Apply-time XML emission
    # ------------------------------------------------------------------

    @classmethod
    def build(
        cls,
        config: Dict[str, Any],
        *,
        name: str,
        folder_name: Optional[str] = None,
    ) -> str:
        """Emit the full Boomi Component XML for the process.

        Assumes validate_config has already passed and that $ref tokens
        in source/target/transform have been substituted with real
        component IDs by the integration builder. The internal
        parse-back roundtrip guards against silent XML malformation
        (PROCESS_XML_VALIDATION_FAILED).
        """
        # Coerce string-like metadata fields. validate_config does not
        # type-check these, so a non-string description/folder_name/name
        # would crash _escape_xml's .replace() with AttributeError at
        # build time. str() coercion keeps build() total. Codex review
        # r2 Q4.
        name = str(name) if name is not None else ""
        if not name or not name.strip():
            raise BuilderValidationError(
                "Process component name is required.",
                error_code="PROCESS_XML_VALIDATION_FAILED",
                field="name",
                hint="Pass a non-empty name via the IntegrationComponentSpec.name field.",
            )

        source = config.get("source") or {}
        target = config.get("target") or {}
        transform = config.get("transform") or {"mode": "passthrough"}
        # str() coercion guards against non-string mode values reaching
        # build() in any code path that bypasses validate_config. Codex L1.
        transform_mode = str(transform.get("mode") or "passthrough").strip().lower()
        description = str(config.get("description") or "")

        # Build shapes in deterministic flow order: start, source,
        # [transform], target, stop. transform is omitted entirely when
        # mode=passthrough.
        flow: List[Tuple[str, Dict[str, Any]]] = []
        flow.append(("start_noaction", {}))
        flow.append((
            "connectoraction_source",
            {
                "connector_type": _canonical_connector_type(source.get("connector_type")),
                "action_type": str(source.get("action_type") or ""),
                "connection_id": str(source.get("connection_id") or ""),
                "operation_id": str(source.get("operation_id") or ""),
                "userlabel": str(source.get("label") or ""),
            },
        ))
        if transform_mode == "message":
            flow.append((
                "message",
                {
                    "text": str(transform.get("message_text") or ""),
                    "userlabel": str(transform.get("label") or ""),
                },
            ))
        elif transform_mode == "map_ref":
            flow.append((
                "map",
                {
                    "map_id": str(transform.get("map_ref") or transform.get("map_id") or ""),
                    "userlabel": str(transform.get("label") or ""),
                },
            ))
        flow.append((
            "connectoraction_target",
            {
                "connector_type": _canonical_connector_type(target.get("connector_type")),
                # REST HTTP methods are case-insensitive on input (validator
                # uppercases for membership check) but Boomi's live XML uses
                # uppercase actionType="POST" — uppercase here so the emitted
                # XML matches the canonical form. Codex review C3.
                "action_type": str(target.get("action_type") or "").strip().upper(),
                "connection_id": str(target.get("connection_id") or ""),
                "operation_id": str(target.get("operation_id") or ""),
                "userlabel": str(target.get("label") or ""),
            },
        ))
        flow.append(("stop", {"continue_": True}))

        total = len(flow)
        # Walk twice: emit each shape with its outgoing dragpoint that
        # points at the next shape. Stop has no outgoing edge.
        shape_xml_parts: List[str] = []
        for i, (kind, params) in enumerate(flow):
            shape_index = i + 1  # shape1..N
            shape_name = f"shape{shape_index}"
            next_name = f"shape{shape_index + 1}" if shape_index < total else None

            if kind == "start_noaction":
                shape_xml_parts.append(_emit_start_noaction(shape_name, next_name, shape_index))
            elif kind == "connectoraction_source":
                shape_xml_parts.append(_emit_connectoraction(shape_name, params, next_name, shape_index))
            elif kind == "connectoraction_target":
                shape_xml_parts.append(_emit_connectoraction(shape_name, params, next_name, shape_index))
            elif kind == "message":
                shape_xml_parts.append(_emit_message(shape_name, params, next_name, shape_index))
            elif kind == "map":
                shape_xml_parts.append(_emit_map(shape_name, params, next_name, shape_index))
            elif kind == "stop":
                shape_xml_parts.append(_emit_stop(shape_name, params))
            else:  # pragma: no cover — defensive
                raise BuilderValidationError(
                    f"Unknown shape kind {kind!r} produced by builder.",
                    error_code="PROCESS_XML_VALIDATION_FAILED",
                    field="shapes",
                    hint="Internal builder bug — please report.",
                )

        process_inner = (
            '<process xmlns="" '
            'allowSimultaneous="false" '
            'enableUserLog="false" '
            'processLogOnErrorOnly="false" '
            'purgeDataImmediately="false" '
            'stopProcessingIfZeroDocuments="true" '
            'updateRunDates="true" '
            'workload="general">'
            '<shapes>'
            f"{''.join(shape_xml_parts)}"
            '</shapes>'
            '</process>'
        )

        folder_attr = (
            f' folderFullPath="{_escape_xml(str(folder_name))}"' if folder_name else ""
        )
        component_xml = (
            '<?xml version="1.0" encoding="UTF-8" standalone="yes"?>'
            '<bns:Component '
            'xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance" '
            'xmlns:bns="http://api.platform.boomi.com/" '
            f'type="process" name="{_escape_xml(name)}"'
            f"{folder_attr}>"
            '<bns:encryptedValues/>'
            f'<bns:description>{_escape_xml(description)}</bns:description>'
            '<bns:object>'
            f"{process_inner}"
            '</bns:object>'
            '<bns:processOverrides/>'
            '</bns:Component>'
        )

        # Internal invariant: the XML we just produced must round-trip
        # through ElementTree without raising. Catches stray
        # unescaped chars or malformed manual concatenation early —
        # surfaces as PROCESS_XML_VALIDATION_FAILED rather than as a
        # confusing Boomi API error at apply time.
        try:
            ET.fromstring(component_xml)
        except ET.ParseError as exc:  # pragma: no cover — defensive
            raise BuilderValidationError(
                f"Generated process Component XML did not round-trip: {exc}",
                error_code="PROCESS_XML_VALIDATION_FAILED",
                field="config",
                hint="Internal builder bug — please report.",
            ) from exc

        return component_xml


# ----------------------------------------------------------------------
# Field-level validators (split out so error messages can be specific)
# ----------------------------------------------------------------------

def _validate_source_binding(source: Any) -> Optional[BuilderValidationError]:
    if not isinstance(source, dict):
        return BuilderValidationError(
            "source binding must be a JSON object with connector_type, "
            "connection_id, operation_id, and action_type.",
            error_code="PROCESS_CONNECTOR_BINDING_INVALID",
            field="source",
            hint="See get_schema_template(resource_type='process', operation='create', protocol='database_to_api_sync').",
        )
    connector_type = str(source.get("connector_type") or "").strip().lower()
    if connector_type != "database":
        return BuilderValidationError(
            f"source.connector_type must be 'database' for "
            f"database_to_api_sync; got {connector_type!r}.",
            error_code="PROCESS_CONNECTOR_BINDING_INVALID",
            field="source.connector_type",
            hint="Database is the only supported source connector in M2.5.",
        )
    action_type = str(source.get("action_type") or "").strip()
    if action_type not in _DB_ACTION_TYPES:
        return BuilderValidationError(
            f"source.action_type must be 'Get' for database source; "
            f"got {action_type!r}.",
            error_code="PROCESS_CONNECTOR_BINDING_INVALID",
            field="source.action_type",
            hint="Issue #32 will cover Send/Upsert write paths.",
        )
    for required in ("connection_id", "operation_id"):
        value = source.get(required)
        if not isinstance(value, str) or not value.strip():
            return BuilderValidationError(
                f"source.{required} is required.",
                error_code="PROCESS_CONNECTOR_BINDING_INVALID",
                field=f"source.{required}",
                hint=(
                    "Pass the component_id of the already-built "
                    "connector-settings / connector-action, or a "
                    "$ref:KEY token pointing at it (and add KEY to "
                    "depends_on)."
                ),
            )
    return None


def _validate_target_binding(target: Any) -> Optional[BuilderValidationError]:
    if not isinstance(target, dict):
        return BuilderValidationError(
            "target binding must be a JSON object with connector_type, "
            "connection_id, operation_id, and action_type.",
            error_code="PROCESS_CONNECTOR_BINDING_INVALID",
            field="target",
            hint="See get_schema_template(resource_type='process', operation='create', protocol='database_to_api_sync').",
        )
    raw_connector_type = target.get("connector_type")
    canonical = _resolve_rest_connector_type(raw_connector_type)
    if canonical is None:
        return BuilderValidationError(
            f"target.connector_type must be 'rest', 'rest_client', or "
            f"{REST_CLIENT_SUBTYPE!r}; got {raw_connector_type!r}.",
            error_code="PROCESS_CONNECTOR_BINDING_INVALID",
            field="target.connector_type",
            hint="REST Client is the only supported target connector in M2.5.",
        )
    action_type = str(target.get("action_type") or "").strip().upper()
    if action_type not in _REST_ACTION_TYPES:
        return BuilderValidationError(
            f"target.action_type must be one of {sorted(_REST_ACTION_TYPES)}; "
            f"got {target.get('action_type')!r}.",
            error_code="PROCESS_CONNECTOR_BINDING_INVALID",
            field="target.action_type",
            hint="REST Client supports standard HTTP verbs.",
        )
    for required in ("connection_id", "operation_id"):
        value = target.get(required)
        if not isinstance(value, str) or not value.strip():
            return BuilderValidationError(
                f"target.{required} is required.",
                error_code="PROCESS_CONNECTOR_BINDING_INVALID",
                field=f"target.{required}",
                hint=(
                    "Pass the component_id of the already-built REST "
                    "connector-settings / connector-action, or a "
                    "$ref:KEY token pointing at it (and add KEY to "
                    "depends_on)."
                ),
            )
    return None


def _validate_transform(transform: Any) -> Optional[BuilderValidationError]:
    if transform is None:
        return None
    if not isinstance(transform, dict):
        return BuilderValidationError(
            "transform must be a JSON object with a 'mode' field.",
            error_code="PROCESS_SHAPE_UNSUPPORTED",
            field="transform",
            hint=f"Supported modes: {sorted(_SUPPORTED_TRANSFORM_MODES)}.",
        )
    mode = str(transform.get("mode") or "passthrough").strip().lower()
    if mode not in _SUPPORTED_TRANSFORM_MODES:
        return BuilderValidationError(
            f"transform.mode {mode!r} is not supported.",
            error_code="PROCESS_SHAPE_UNSUPPORTED",
            field="transform.mode",
            hint=f"Supported modes: {sorted(_SUPPORTED_TRANSFORM_MODES)}.",
        )
    if mode == "message":
        text = transform.get("message_text")
        if not isinstance(text, str) or not text:
            return BuilderValidationError(
                "transform.message_text is required when mode='message'.",
                error_code="PROCESS_SHAPE_UNSUPPORTED",
                field="transform.message_text",
                hint="Provide the message body to emit on the Message shape.",
            )
    if mode == "map_ref":
        ref = transform.get("map_ref") or transform.get("map_id")
        if not isinstance(ref, str) or not ref.strip():
            return BuilderValidationError(
                "transform.map_ref is required when mode='map_ref'.",
                error_code="PROCESS_SHAPE_UNSUPPORTED",
                field="transform.map_ref",
                hint=(
                    "Pass a map component_id or a $ref:KEY token "
                    "(map component creation is issue #26 scope)."
                ),
            )
    return None


def _validate_reliability(reliability: Any) -> Optional[BuilderValidationError]:
    if reliability is None:
        return None
    if not isinstance(reliability, dict):
        return BuilderValidationError(
            "reliability must be a JSON object.",
            error_code="PROCESS_RETRY_UNVERIFIED",
            field="reliability",
            hint="See get_schema_template for the reliability surface.",
        )
    retry_count = reliability.get("retry_count", 0)
    if not isinstance(retry_count, int) or isinstance(retry_count, bool):
        return BuilderValidationError(
            "reliability.retry_count must be an integer 0..5.",
            error_code="PROCESS_RETRY_UNVERIFIED",
            field="reliability.retry_count",
            hint="Use a plain integer; Try/Catch retry is not yet wired.",
        )
    if retry_count < 0 or retry_count > _MAX_RETRY_COUNT:
        return BuilderValidationError(
            f"reliability.retry_count must be 0..{_MAX_RETRY_COUNT}; "
            f"got {retry_count}.",
            error_code="PROCESS_RETRY_UNVERIFIED",
            field="reliability.retry_count",
            hint=f"Boomi Try/Catch retry range is 0..{_MAX_RETRY_COUNT}.",
        )
    if retry_count > 0:
        return BuilderValidationError(
            "reliability.retry_count > 0 requires Try/Catch which is not "
            "yet implemented (PROCESS_RETRY_UNVERIFIED).",
            error_code="PROCESS_RETRY_UNVERIFIED",
            field="reliability.retry_count",
            hint=(
                "Set retry_count=0 for the M2.5 vertical slice. "
                "Try/Catch wrapper lands in a follow-up issue after "
                "live Try/Catch XML is captured."
            ),
        )
    dlq = reliability.get("dlq")
    if dlq is not None:
        if not isinstance(dlq, dict):
            # Shape error → caller mistake → PROCESS_DLQ_BINDING_INVALID
            # (Codex review A3). Reserve PROCESS_RETRY_UNVERIFIED for
            # known-but-deferred modes only.
            return BuilderValidationError(
                "reliability.dlq must be a JSON object with a 'mode' field.",
                error_code="PROCESS_DLQ_BINDING_INVALID",
                field="reliability.dlq",
                hint=f"Supported dlq modes: {sorted(_SUPPORTED_DLQ_MODES)}.",
            )
        # str() coercion: non-string mode (e.g. 1) becomes "1" and falls
        # out of the enum membership check below. Codex review L1.
        dlq_mode = str(dlq.get("mode") or "disabled").strip().lower()
        if dlq_mode not in _SUPPORTED_DLQ_MODES:
            # Unknown enum value → caller typo → PROCESS_DLQ_BINDING_INVALID.
            return BuilderValidationError(
                f"reliability.dlq.mode {dlq_mode!r} is not supported.",
                error_code="PROCESS_DLQ_BINDING_INVALID",
                field="reliability.dlq.mode",
                hint=f"Supported dlq modes: {sorted(_SUPPORTED_DLQ_MODES)}.",
            )
        if dlq_mode != "disabled":
            # Recognized mode but Try/Catch wrapper isn't wired yet.
            return BuilderValidationError(
                f"reliability.dlq.mode={dlq_mode!r} requires Try/Catch "
                "which is not yet implemented (PROCESS_RETRY_UNVERIFIED).",
                error_code="PROCESS_RETRY_UNVERIFIED",
                field="reliability.dlq.mode",
                hint=(
                    "Set dlq.mode='disabled' for the M2.5 vertical slice. "
                    "DLQ paths land in a follow-up issue alongside "
                    "Try/Catch."
                ),
            )
    return None


# ----------------------------------------------------------------------
# Shape emitters
# ----------------------------------------------------------------------

def _emit_start_noaction(
    shape_name: str, next_name: Optional[str], shape_index: int
) -> str:
    dragpoints = _emit_dragpoints([next_name], shape_index)
    return (
        f'<shape image="start" name="{shape_name}" shapetype="start" '
        f'userlabel="" x="{_START_SHAPE_X}" y="{_START_SHAPE_Y}">'
        '<configuration><noaction/></configuration>'
        f'<dragpoints>{dragpoints}</dragpoints>'
        '</shape>'
    )


def _emit_connectoraction(
    shape_name: str,
    params: Dict[str, Any],
    next_name: Optional[str],
    shape_index: int,
) -> str:
    dragpoints = _emit_dragpoints([next_name], shape_index)
    userlabel = _escape_xml(params.get("userlabel") or "")
    connector_type = _escape_xml(params["connector_type"])
    action_type = _escape_xml(params["action_type"])
    connection_id = _escape_xml(params["connection_id"])
    operation_id = _escape_xml(params["operation_id"])
    return (
        f'<shape image="connectoraction_icon" name="{shape_name}" '
        f'shapetype="connectoraction" userlabel="{userlabel}" '
        f'x="{_shape_x(shape_index)}" y="{_SHAPE_Y}">'
        '<configuration>'
        f'<connectoraction actionType="{action_type}" '
        'allowDynamicCredentials="NONE" '
        f'connectionId="{connection_id}" '
        f'connectorType="{connector_type}" '
        'hideSettings="false" '
        f'operationId="{operation_id}">'
        '<parameters/><dynamicProperties/>'
        '</connectoraction>'
        '</configuration>'
        f'<dragpoints>{dragpoints}</dragpoints>'
        '</shape>'
    )


def _emit_message(
    shape_name: str,
    params: Dict[str, Any],
    next_name: Optional[str],
    shape_index: int,
) -> str:
    dragpoints = _emit_dragpoints([next_name], shape_index)
    userlabel = _escape_xml(params.get("userlabel") or "")
    text = _escape_xml(params.get("text") or "")
    return (
        f'<shape image="message_icon" name="{shape_name}" shapetype="message" '
        f'userlabel="{userlabel}" x="{_shape_x(shape_index)}" y="{_SHAPE_Y}">'
        '<configuration>'
        '<message combined="false">'
        f'<msgTxt>{text}</msgTxt>'
        '<msgParameters/>'
        '</message>'
        '</configuration>'
        f'<dragpoints>{dragpoints}</dragpoints>'
        '</shape>'
    )


def _emit_map(
    shape_name: str,
    params: Dict[str, Any],
    next_name: Optional[str],
    shape_index: int,
) -> str:
    dragpoints = _emit_dragpoints([next_name], shape_index)
    userlabel = _escape_xml(params.get("userlabel") or "")
    map_id = _escape_xml(params.get("map_id") or "")
    return (
        f'<shape image="map_icon" name="{shape_name}" shapetype="map" '
        f'userlabel="{userlabel}" x="{_shape_x(shape_index)}" y="{_SHAPE_Y}">'
        '<configuration>'
        f'<map mapId="{map_id}"/>'
        '</configuration>'
        f'<dragpoints>{dragpoints}</dragpoints>'
        '</shape>'
    )


def _emit_stop(shape_name: str, params: Dict[str, Any]) -> str:
    cont = "true" if params.get("continue_", True) else "false"
    # Stop x position == last index but we don't know it here; the caller
    # passes shape_index implicitly through shape_name's numeric suffix.
    shape_index = int(re.sub(r"\D", "", shape_name) or "1")
    return (
        f'<shape image="stop_icon" name="{shape_name}" shapetype="stop" '
        f'x="{_shape_x(shape_index)}" y="{_SHAPE_Y}">'
        f'<configuration><stop continue="{cont}"/></configuration>'
        '<dragpoints/>'
        '</shape>'
    )


def _emit_dragpoints(
    next_names: List[Optional[str]], shape_index: int
) -> str:
    """Emit <dragpoint .../> children for a shape.

    Each non-None entry in next_names produces one dragpoint with name
    "<shape>.dragpoint<N>" and toShape set. None entries are skipped
    (used by Stop, which has no outgoing edge).
    """
    parts: List[str] = []
    point_index = 0
    for to_shape in next_names:
        if to_shape is None:
            continue
        point_index += 1
        parts.append(
            f'<dragpoint name="shape{shape_index}.dragpoint{point_index}" '
            f'toShape="{_escape_xml(to_shape)}" '
            f'x="{_dragpoint_x(shape_index)}" y="{_DRAGPOINT_Y}"/>'
        )
    return "".join(parts)


# ----------------------------------------------------------------------
# Helpers
# ----------------------------------------------------------------------

def _canonical_connector_type(value: Optional[str]) -> str:
    """Resolve REST aliases to canonical subtype; pass others through.

    REST Client's three accepted spellings (rest, rest_client, canonical)
    all map to the same Boomi subtype string used in XML. Database and
    any future connector types are emitted verbatim.
    """
    if not isinstance(value, str):
        return ""
    canonical = _resolve_rest_connector_type(value)
    if canonical is not None:
        return canonical
    return value.strip()


def _walk_scalars(value: Any, _path: Tuple[str, ...] = ()) -> Iterable[Tuple[Tuple[str, ...], Any]]:
    """Yield (path, scalar) pairs for every leaf in the value tree.

    Mirrors _resolve_dependency_tokens' traversal so secret/$ref scans
    cover the same surface area.
    """
    if isinstance(value, dict):
        for k, v in value.items():
            yield from _walk_scalars(v, _path + (str(k),))
    elif isinstance(value, list):
        for i, item in enumerate(value):
            yield from _walk_scalars(item, _path + (f"[{i}]",))
    else:
        yield _path, value


# ----------------------------------------------------------------------
# Registry (parallel to PROFILE_BUILDERS / CONNECTOR_BUILDERS)
# ----------------------------------------------------------------------

PROCESS_FLOW_BUILDERS: Dict[str, type] = {
    ProcessFlowBuilder.PROCESS_KIND: ProcessFlowBuilder,
}


def get_process_flow_builder(process_kind: Optional[str]):
    """Return the ProcessFlowBuilder subclass for process_kind, or None."""
    if not process_kind:
        return None
    return PROCESS_FLOW_BUILDERS.get(str(process_kind).strip().lower())


__all__ = [
    "ProcessFlowBuilder",
    "PROCESS_FLOW_BUILDERS",
    "get_process_flow_builder",
]

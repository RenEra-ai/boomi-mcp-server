"""
Process-flow XML builder for structured process orchestration (issue #25).

Owns Boomi process Component XML emission for `process_kind` archetypes
that wire DB/REST/other connector-actions together. Today supports
`database_to_api_sync` (M2.5 vertical slice) — a deterministic
Start -> [optional transform] -> Target -> Stop flow whose shape XML
uses `shapetype="connectoraction"` (matches live Renera examples like
`DB Test`, `Rest Test GET`, `Rest Test PATCH`).

Issue #51 M3.R1a adds a verified Try/Catch + DLQ catch-path: for
`retry_count` 0..5 with `dlq.mode` in {`document_cache_ref`,
`error_subprocess_ref`}, the flow is wrapped in a `catcherrors` shape
(transcribed from live Boomi exports, not invented from docs) whose
catch leg routes to a `doccacheload` (DLQ cache) or `processcall` (error
subprocess). Issue #88 M4.5.3 un-gated retry 1..5 (docs-corroborated:
Boomi Try/Catch Retry Count is 0..5, platform-timed) — positive retry
requires a wired DLQ catch path; values outside 0..5 (or retry>0 without
a DLQ) still return PROCESS_RETRY_UNVERIFIED. Map and subprocess/cache
components are referenced by id/$ref only — their build is out of scope.

Issue #89 M4.5.4 adds an optional verified Notify step on the catch leg:
when `reliability.catch_notify` is set (a `level` + a `message_template`
that references the caught-error property), the catch leg becomes
`catch -> notify -> dlq route -> stop`. The Notify shape XML is transcribed
from a live `work`-profile export (notify shape, not invented from docs);
omitting `catch_notify` keeps the existing catch leg byte-for-byte
identical. Email/SMS notification channels and Notify outside catch paths
are out of scope.
"""

from __future__ import annotations

import re
import xml.etree.ElementTree as ET
from typing import Any, Dict, Iterable, List, Optional, Tuple

from ._preservation_policy import OwnedPath, PreservationPolicy
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

# Issue #51 M3.R1a / #88 M4.5.3: DLQ modes that emit a verified Try/Catch
# wrapper + DLQ catch-path (every supported mode except "disabled"). The catch
# leg is structural, so positive retry_count is only emittable WITH one of
# these modes wired.
_TRY_CATCH_DLQ_MODES = frozenset({"document_cache_ref", "error_subprocess_ref"})

# Per the Boomi Try/Catch shape docs, Retry Count ranges 0..5 (count 1 retries
# immediately; 2..5 use the platform's built-in escalating wait schedule). The
# platform offers no caller-selected backoff. Issue #88 un-gated 1..5 (with a
# wired catch path); values outside 0..5 fail with PROCESS_RETRY_UNVERIFIED.
_MAX_RETRY_COUNT = 5

# Issue #89 M4.5.4 — optional Notify step on the Try/Catch catch leg.
# Boomi Notify message levels are INFO / WARNING / ERROR (the Notify-step
# docs list "Information, Warning, or Error"; the live notify shape emits the
# token "INFO"). The catch-path Notify is log-only (no platform email event),
# so email/SMS channels are out of scope and any extra config key is rejected.
_SUPPORTED_NOTIFY_LEVELS = frozenset({"INFO", "WARNING", "ERROR"})
_CATCH_NOTIFY_ALLOWED_KEYS = frozenset({"level", "message_template"})
# The runtime property holding the caught Try/Catch error message. Boomi binds
# it via a numbered placeholder + a notify track parameter (verified live), not
# by embedding the path in the message text, so the builder substitutes this
# token for the {1} placeholder and emits the matching track-parameter binding.
_NOTIFY_CAUGHT_ERROR_TOKEN = "meta.base.catcherrorsmessage"

# Visual layout. Geometry is decorative only — process correctness is
# driven by toShape wiring. Numbers approximate the live Renera examples
# so the rendered diagram stays readable.
_SHAPE_Y = 96.0
_START_SHAPE_X = 96.0
_START_SHAPE_Y = 94.0
_SHAPE_X_STEP = 160.0
_DRAGPOINT_X_OFFSET = 144.0
_DRAGPOINT_Y = 104.0
# Catch-path row sits below the Try row. Geometry is decorative; the verified
# live Try/Catch (work component dff0bf83-d525-4781-b572-c93d285bb788) places
# the catch leg on a separate lower y. Issue #51 M3.R1a.
_CATCH_SHAPE_Y = 456.0
_CATCH_DRAGPOINT_Y = 464.0


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
            if not isinstance(value, str):
                continue
            stripped = value.strip()
            if not stripped.startswith("$ref:"):
                continue
            # Codex review r7 P2.2: a padded value like " $ref:foo " is
            # not recognized as a ref by _resolve_dependency_tokens
            # (which requires startswith at byte 0), but build()'s
            # whitespace stripping then emits the unresolved token
            # directly into the connectoraction XML. Reject the
            # malformed shape here so apply never sees it.
            if value != stripped:
                return BuilderValidationError(
                    f"$ref token at {'.'.join(path)!r} has surrounding "
                    f"whitespace ({value!r}); refs must be exact "
                    f"'$ref:KEY' strings.",
                    error_code="MISSING_PROCESS_DEPENDENCY",
                    field=".".join(path),
                    hint=(
                        "Remove leading/trailing whitespace from the "
                        "$ref:KEY value. Apply-time substitution only "
                        "matches refs that start at byte 0."
                    ),
                )
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

    @classmethod
    def _should_emit_try_catch(cls, reliability: Any) -> bool:
        """True when the config should emit a verified Try/Catch + DLQ wrapper.

        Issue #51 M3.R1a + #88 M4.5.3: retry_count 0..5 with a supported DLQ
        mode (document_cache_ref / error_subprocess_ref) is un-gated. Values
        outside 0..5, the wrong type, or retry_count > 0 without a supported
        DLQ mode stay gated (PROCESS_RETRY_UNVERIFIED) and never reach this path
        because validate_config rejects them first. This guard mirrors that
        boundary so a direct build() call is also total.
        """
        if not isinstance(reliability, dict):
            return False
        retry_count = reliability.get("retry_count", 0)
        if (
            not isinstance(retry_count, int)
            or isinstance(retry_count, bool)
            or not (0 <= retry_count <= _MAX_RETRY_COUNT)
        ):
            return False
        dlq = reliability.get("dlq")
        if not isinstance(dlq, dict):
            return False
        return str(dlq.get("mode") or "").strip().lower() in _TRY_CATCH_DLQ_MODES

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
                # Database source — Boomi expects exact `connectorType="database"`
                # and `actionType="Get"` (case-sensitive on both sides). The
                # validator accepts case-insensitive input via .lower()/strip,
                # so canonicalize here before emission. Codex review r6 P2.2.
                "connector_type": _canonical_connector_type(source.get("connector_type")).lower(),
                "action_type": str(source.get("action_type") or "").strip(),
                "connection_id": str(source.get("connection_id") or "").strip(),
                "operation_id": str(source.get("operation_id") or "").strip(),
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
                    # Strip whitespace so a padded literal map ID
                    # ("  ABC-MAP-123  ") becomes canonical before
                    # emission. Padded $ref tokens are already rejected
                    # at validate_config (r7 P2.2). Codex review r8 F3.
                    "map_id": str(transform.get("map_ref") or transform.get("map_id") or "").strip(),
                    "userlabel": str(transform.get("label") or ""),
                },
            ))
        flow.append((
            "connectoraction_target",
            {
                # _canonical_connector_type maps REST aliases ("rest",
                # "rest_client") to the canonical subtype.
                "connector_type": _canonical_connector_type(target.get("connector_type")),
                # REST HTTP methods are case-insensitive on input (validator
                # uppercases for membership check) but Boomi's live XML uses
                # uppercase actionType="POST" — uppercase here so the emitted
                # XML matches the canonical form. Codex review C3.
                "action_type": str(target.get("action_type") or "").strip().upper(),
                # Strip ID whitespace so whitespace-padded refs don't leak
                # into emitted XML. Codex review r6 P2.2.
                "connection_id": str(target.get("connection_id") or "").strip(),
                "operation_id": str(target.get("operation_id") or "").strip(),
                "userlabel": str(target.get("label") or ""),
            },
        ))
        flow.append(("stop", {"continue_": True}))

        # Issue #51 M3.R1a + #88 M4.5.3: when retry_count is 0..5 and a supported
        # DLQ mode is set, wrap the linear flow in the verified Try/Catch
        # (catcherrors) shape with a DLQ catch path, emitting the validated
        # retry count. Otherwise emit the unchanged linear flow so existing
        # non-DLQ process XML is byte-for-byte identical.
        reliability_cfg = config.get("reliability")
        if cls._should_emit_try_catch(reliability_cfg):
            # _should_emit_try_catch already proved reliability_cfg["dlq"] is a
            # non-empty dict and retry_count is a valid int 0..5, so subscript
            # directly (no dead `or {}` fallback).
            shape_xml_parts: List[str] = _emit_try_catch_shapes(
                flow,
                reliability_cfg["dlq"],
                retry_count=int(reliability_cfg.get("retry_count", 0)),
                catch_notify=reliability_cfg.get("catch_notify"),
            )
        else:
            shape_xml_parts = _emit_linear_shapes(flow)

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

        # folderName is the writable folder attribute on Component
        # create/update; folderFullPath is response-only metadata that
        # Boomi ignores on writes. All other builders in the repo
        # (DatabaseConnectorBuilder, RestClient*, profile builders) emit
        # folderName for placement — match them. Codex review r8 F2.
        folder_attr = (
            f' folderName="{_escape_xml(str(folder_name))}"' if folder_name else ""
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
            hint="Use a plain integer in 0..5.",
        )
    if retry_count < 0 or retry_count > _MAX_RETRY_COUNT:
        return BuilderValidationError(
            f"reliability.retry_count must be 0..{_MAX_RETRY_COUNT}; "
            f"got {retry_count}.",
            error_code="PROCESS_RETRY_UNVERIFIED",
            field="reliability.retry_count",
            hint=f"Boomi Try/Catch retry range is 0..{_MAX_RETRY_COUNT}.",
        )
    # Issue #88 (M4.5.3): retry_count 0..5 is un-gated. The Try/Catch Retry
    # Count range (0..5) and its built-in platform wait schedule (count 1
    # retries immediately; 2..5 use escalating built-in waits) are
    # docs-corroborated; the platform offers no caller-selected backoff.
    # Positive retry is only emittable inside a Try/Catch whose catch leg
    # routes to a DLQ (the catcherrors shape always carries a catch leg), so
    # retry_count > 0 requires a supported Try/Catch DLQ mode (checked below).
    dlq = reliability.get("dlq")
    dlq_mode = "disabled"
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
        if dlq_mode in _TRY_CATCH_DLQ_MODES:
            # The verified Try/Catch wrapper + DLQ catch-path is emitted (issue
            # #51 M3.R1a for retry_count=0; issue #88 for 1..5). Require a
            # resolvable catch-leg binding (literal id or $ref:KEY token).
            binding_err = _validate_dlq_binding(dlq, dlq_mode)
            if binding_err is not None:
                return binding_err
        # dlq_mode == "disabled" → no Try/Catch; nothing else to validate.
    if retry_count > 0 and dlq_mode not in _TRY_CATCH_DLQ_MODES:
        return BuilderValidationError(
            "reliability.retry_count > 0 requires a wired Try/Catch catch path.",
            error_code="PROCESS_RETRY_UNVERIFIED",
            field="reliability.retry_count",
            hint=(
                "Positive retry is emitted only inside a Try/Catch whose catch "
                "leg routes to a DLQ. Set reliability.dlq.mode to "
                "document_cache_ref or error_subprocess_ref, or use "
                "retry_count=0."
            ),
        )
    # Issue #89 (M4.5.4): optional Notify on the catch leg. Validated after
    # dlq_mode is finalized (Notify only exists on a wired catch path) and
    # after the retry gate (retry/DLQ shape errors surface first).
    notify_err = _validate_catch_notify(reliability.get("catch_notify"), dlq_mode)
    if notify_err is not None:
        return notify_err
    return None


def _validate_catch_notify(
    catch_notify: Any, dlq_mode: str
) -> Optional[BuilderValidationError]:
    """Validate the optional ``reliability.catch_notify`` config (issue #89).

    Returns ``None`` when absent (Notify is opt-in) or valid; otherwise a
    ``PROCESS_NOTIFY_CONFIG_INVALID`` error. Notify is emitted only at the head
    of a wired Try/Catch catch leg, so it requires ``dlq_mode`` in
    ``_TRY_CATCH_DLQ_MODES``. The message must reference the caught-error
    property so the emitted Notify logs the real error. Email/SMS/channel keys
    are out of scope and rejected (extra keys).
    """
    if catch_notify is None:
        return None
    if not isinstance(catch_notify, dict):
        return BuilderValidationError(
            "reliability.catch_notify must be a JSON object.",
            error_code="PROCESS_NOTIFY_CONFIG_INVALID",
            field="reliability.catch_notify",
            hint="See get_schema_template for the catch_notify surface.",
        )
    extra = set(catch_notify) - _CATCH_NOTIFY_ALLOWED_KEYS
    if extra:
        return BuilderValidationError(
            f"reliability.catch_notify has unsupported keys: {sorted(extra)}.",
            error_code="PROCESS_NOTIFY_CONFIG_INVALID",
            field="reliability.catch_notify",
            hint=(
                "Only 'level' and 'message_template' are supported. Email/SMS "
                "notification channels are out of scope (#14/M4.5.5)."
            ),
        )
    template = catch_notify.get("message_template")
    if not isinstance(template, str) or not template.strip():
        return BuilderValidationError(
            "reliability.catch_notify.message_template is required and must be "
            "a non-empty string.",
            error_code="PROCESS_NOTIFY_CONFIG_INVALID",
            field="reliability.catch_notify.message_template",
            hint=(
                "Provide the notify message text and reference the caught error "
                f"via the {_NOTIFY_CAUGHT_ERROR_TOKEN} property token."
            ),
        )
    if _NOTIFY_CAUGHT_ERROR_TOKEN not in template:
        return BuilderValidationError(
            "reliability.catch_notify.message_template must reference the "
            "caught-error property.",
            error_code="PROCESS_NOTIFY_CONFIG_INVALID",
            field="reliability.catch_notify.message_template",
            hint=(
                f"Include the {_NOTIFY_CAUGHT_ERROR_TOKEN} token so the emitted "
                "Notify logs the caught error."
            ),
        )
    level = catch_notify.get("level")
    if not isinstance(level, str) or level.strip().upper() not in _SUPPORTED_NOTIFY_LEVELS:
        return BuilderValidationError(
            f"reliability.catch_notify.level must be one of "
            f"{sorted(_SUPPORTED_NOTIFY_LEVELS)}.",
            error_code="PROCESS_NOTIFY_CONFIG_INVALID",
            field="reliability.catch_notify.level",
            hint="Boomi Notify message levels are INFO, WARNING, ERROR.",
        )
    if dlq_mode not in _TRY_CATCH_DLQ_MODES:
        return BuilderValidationError(
            "reliability.catch_notify requires a wired Try/Catch catch path.",
            error_code="PROCESS_NOTIFY_CONFIG_INVALID",
            field="reliability.catch_notify",
            hint=(
                "Notify is emitted only on a catch leg. Set reliability.dlq.mode "
                "to document_cache_ref or error_subprocess_ref."
            ),
        )
    return None


def _validate_dlq_binding(
    dlq: Dict[str, Any], mode: str
) -> Optional[BuilderValidationError]:
    """Validate the DLQ catch-leg binding for a supported Try/Catch mode.

    The process builder resolves component references via literal ids or
    ``$ref:KEY`` tokens (substituted by integration_builder before build()).
    The dlq_writer primitive's bare ``*_ref_key`` mechanism is NOT resolvable
    on this build path, so the binding must use the ``*_id`` field — a literal
    Boomi component id, or a ``$ref:KEY`` token whose KEY is in depends_on
    (the existing $ref-reachability walk in validate_config covers it). Issue
    #51 M3.R1a.
    """
    if mode == "document_cache_ref":
        id_field, ref_field, target = (
            "document_cache_id", "document_cache_ref_key", "Document Cache",
        )
    else:  # error_subprocess_ref
        id_field, ref_field, target = (
            "process_id", "process_ref_key", "error subprocess",
        )

    id_value = dlq.get(id_field)
    has_id = isinstance(id_value, str) and id_value.strip() != ""
    ref_value = dlq.get(ref_field)
    has_ref = isinstance(ref_value, str) and ref_value.strip() != ""

    bind_hint = (
        f"Set {id_field} to a literal Boomi component id, or a '$ref:KEY' "
        f"token whose KEY is in the process component's depends_on."
    )
    if has_id and has_ref:
        return BuilderValidationError(
            f"reliability.dlq for mode {mode!r} must set exactly one of "
            f"{id_field!r} or {ref_field!r}, not both.",
            error_code="PROCESS_DLQ_BINDING_INVALID",
            field=f"reliability.dlq.{id_field}",
            hint=f"Provide only {id_field!r}. {bind_hint}",
        )
    if not has_id:
        if has_ref:
            return BuilderValidationError(
                f"reliability.dlq.{ref_field} is not resolvable by the "
                f"process builder; bind the {target} via {id_field!r} "
                f"instead.",
                error_code="PROCESS_DLQ_BINDING_INVALID",
                field=f"reliability.dlq.{ref_field}",
                hint=bind_hint,
            )
        return BuilderValidationError(
            f"reliability.dlq.mode={mode!r} requires {id_field!r} to bind "
            f"the {target} catch path.",
            error_code="PROCESS_DLQ_BINDING_INVALID",
            field=f"reliability.dlq.{id_field}",
            hint=bind_hint,
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


def _emit_stop(shape_name: str, params: Dict[str, Any], y: float = _SHAPE_Y) -> str:
    cont = "true" if params.get("continue_", True) else "false"
    # Stop x position == last index but we don't know it here; the caller
    # passes shape_index implicitly through shape_name's numeric suffix.
    # ``y`` defaults to the Try-row y; the issue #89 catch-leg Stop (after a
    # Notify + DLQ route) passes ``_CATCH_SHAPE_Y`` to sit on the catch row.
    shape_index = int(re.sub(r"\D", "", shape_name) or "1")
    return (
        f'<shape image="stop_icon" name="{shape_name}" shapetype="stop" '
        f'x="{_shape_x(shape_index)}" y="{y}">'
        f'<configuration><stop continue="{cont}"/></configuration>'
        '<dragpoints/>'
        '</shape>'
    )


def _emit_dragpoints(
    next_names: List[Optional[str]], shape_index: int, y: float = _DRAGPOINT_Y
) -> str:
    """Emit <dragpoint .../> children for a shape.

    Each non-None entry in next_names produces one dragpoint with name
    "<shape>.dragpoint<N>" and toShape set. None entries are skipped
    (used by Stop, which has no outgoing edge). ``y`` defaults to the Try-row
    dragpoint y; catch-row shapes (issue #89 Notify / chained DLQ route) pass
    ``_CATCH_DRAGPOINT_Y`` so their outgoing edges sit on the catch row.
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
            f'x="{_dragpoint_x(shape_index)}" y="{y}"/>'
        )
    return "".join(parts)


# ----------------------------------------------------------------------
# Issue #51 M3.R1a / #89 M4.5.4 — Try/Catch + DLQ + Notify catch-path emission
#
# Shapes below are transcribed verbatim from verified live `work`-profile
# exports (no XML invented from docs):
#   * catcherrors  — component dff0bf83-d525-4781-b572-c93d285bb788 (shape4)
#   * doccacheload — same component (shape80), terminal catch leg
#   * processcall  — component 7b19baeb-ed62-4fac-9962-44fc0ed87f07 (shape34,
#                    on a catcherrors error branch), terminal catch leg
#   * notify       — component 1139079f-fff5-434c-aedc-d2758cc20525 (shape5),
#                    a notify on an error-handling path: notifyMessage with
#                    {N} placeholders, notifyMessageLevel, and a notifyParameters
#                    track binding of meta.base.catcherrorsmessage (issue #89)
# ----------------------------------------------------------------------

# Shape "kinds" produced by build()'s flow list (mirrors the dispatch order).
def _emit_flow_shape(
    kind: str,
    params: Dict[str, Any],
    shape_name: str,
    next_name: Optional[str],
    shape_index: int,
) -> str:
    """Emit one linear-flow shape with an outgoing edge to next_name."""
    if kind == "start_noaction":
        return _emit_start_noaction(shape_name, next_name, shape_index)
    if kind in ("connectoraction_source", "connectoraction_target"):
        return _emit_connectoraction(shape_name, params, next_name, shape_index)
    if kind == "message":
        return _emit_message(shape_name, params, next_name, shape_index)
    if kind == "map":
        return _emit_map(shape_name, params, next_name, shape_index)
    if kind == "stop":
        return _emit_stop(shape_name, params)
    raise BuilderValidationError(  # pragma: no cover — defensive
        f"Unknown shape kind {kind!r} produced by builder.",
        error_code="PROCESS_XML_VALIDATION_FAILED",
        field="shapes",
        hint="Internal builder bug — please report.",
    )


def _emit_linear_shapes(flow: List[Tuple[str, Dict[str, Any]]]) -> List[str]:
    """Emit the unwrapped Start -> ... -> Stop chain (pre-#51 behavior)."""
    total = len(flow)
    parts: List[str] = []
    for i, (kind, params) in enumerate(flow):
        shape_index = i + 1  # shape1..N
        shape_name = f"shape{shape_index}"
        next_name = f"shape{shape_index + 1}" if shape_index < total else None
        parts.append(_emit_flow_shape(kind, params, shape_name, next_name, shape_index))
    return parts


def _emit_try_catch_shapes(
    flow: List[Tuple[str, Dict[str, Any]]],
    dlq: Dict[str, Any],
    retry_count: int = 0,
    catch_notify: Optional[Dict[str, Any]] = None,
) -> List[str]:
    """Wrap the linear flow in a verified catcherrors Try/Catch + DLQ catch leg.

    Layout (shape names are positional, like the linear path):
        shape1  start        -> shape2 (catcherrors)
        shape2  catcherrors  Try(default) -> shape3 ; Catch(error) -> catch leg
        shape3..K  source -> [transform] -> target -> stop  (the normal chain)
        shape{K+1}.. catch leg

    Without ``catch_notify`` the catch leg is a single terminal
    doccacheload|processcall (byte-for-byte the issue #51/#88 output). With
    ``catch_notify`` (issue #89) the catch leg becomes
    ``notify -> dlq route -> catch stop``: the catcherrors Catch dragpoint
    targets the Notify, the Notify routes to the DLQ shape, and the DLQ shape
    routes to a catch-row Stop. ``retry_count`` is a validated 0..5 value; for
    counts > 0 the platform applies its built-in wait schedule before each
    retry, then routes the failed documents down the catch leg on exhaust
    (issue #88).
    """
    normal = flow[1:]  # source, [transform], target, stop
    n = len(normal)
    catcherrors_index = 2
    catcherrors_name = f"shape{catcherrors_index}"
    first_try_index = 3
    first_try_name = f"shape{first_try_index}"
    stop_index = catcherrors_index + n  # last normal (Try-path stop) shape index

    # A present-but-empty/invalid catch_notify still counts as "notify intended"
    # so the validate_config-bypass path rejects it consistently (matches
    # _validate_catch_notify, which treats only None as "absent").
    notify_present = catch_notify is not None
    if notify_present:
        # Catch leg: notify (stop_index+1) -> dlq route (stop_index+2)
        #            -> catch stop (stop_index+3).
        notify_index = stop_index + 1
        notify_name = f"shape{notify_index}"
        dlq_index = stop_index + 2
        catch_stop_index = stop_index + 3
        catch_stop_name = f"shape{catch_stop_index}"
        catch_target_name = notify_name
        dlq_next_name: Optional[str] = catch_stop_name
    else:
        # Catch leg: a single terminal dlq route (unchanged pre-#89 shape).
        dlq_index = stop_index + 1
        catch_target_name = f"shape{dlq_index}"
        dlq_next_name = None
    dlq_name = f"shape{dlq_index}"

    parts: List[str] = []
    # Start keeps its noaction config; only its outgoing edge moves to catcherrors.
    parts.append(_emit_start_noaction("shape1", catcherrors_name, 1))
    parts.append(
        _emit_catcherrors(
            catcherrors_name, first_try_name, catch_target_name, catcherrors_index, retry_count
        )
    )
    # Normal Try chain, shifted to indices 3..stop_index.
    for j, (kind, params) in enumerate(normal):
        shape_index = first_try_index + j
        shape_name = f"shape{shape_index}"
        is_last = j == n - 1
        next_name = None if is_last else f"shape{shape_index + 1}"
        parts.append(_emit_flow_shape(kind, params, shape_name, next_name, shape_index))
    # Catch leg. Bindings are normally validated by _validate_dlq_binding /
    # _validate_catch_notify; ids are literals or $ref:KEY already resolved by
    # integration_builder before build(). Stay total on the validate_config-
    # bypass path: raise on a missing/invalid binding instead of emitting broken
    # XML (mirrors build()'s empty-name guard).
    mode = str(dlq.get("mode") or "").strip().lower()
    if notify_present:
        notify_err = _validate_catch_notify(catch_notify, mode)
        if notify_err is not None:
            raise notify_err
        parts.append(_emit_notify(notify_name, catch_notify, dlq_name, notify_index))
    if mode == "document_cache_ref":
        cache_id = str(dlq.get("document_cache_id") or "").strip()
        if not cache_id:
            raise BuilderValidationError(
                "reliability.dlq.mode='document_cache_ref' requires a non-empty "
                "document_cache_id to emit the DLQ catch leg.",
                error_code="PROCESS_DLQ_BINDING_INVALID",
                field="reliability.dlq.document_cache_id",
                hint="Set document_cache_id to a literal id or a resolved $ref:KEY.",
            )
        parts.append(_emit_doccacheload(dlq_name, cache_id, dlq_index, next_name=dlq_next_name))
    elif mode == "error_subprocess_ref":
        process_id = str(dlq.get("process_id") or "").strip()
        if not process_id:
            raise BuilderValidationError(
                "reliability.dlq.mode='error_subprocess_ref' requires a non-empty "
                "process_id to emit the DLQ catch leg.",
                error_code="PROCESS_DLQ_BINDING_INVALID",
                field="reliability.dlq.process_id",
                hint="Set process_id to a literal id or a resolved $ref:KEY.",
            )
        parts.append(_emit_processcall(dlq_name, process_id, dlq_index, next_name=dlq_next_name))
    else:  # pragma: no cover — _should_emit_try_catch only admits the two modes
        raise BuilderValidationError(
            f"Unsupported DLQ mode {mode!r} reached the Try/Catch emitter.",
            error_code="PROCESS_XML_VALIDATION_FAILED",
            field="reliability.dlq.mode",
            hint="Internal builder bug — please report.",
        )
    if notify_present:
        parts.append(_emit_stop(catch_stop_name, {"continue_": True}, y=_CATCH_SHAPE_Y))
    return parts


def _emit_catcherrors(
    shape_name: str, try_to: str, catch_to: str, shape_index: int, retry_count: int = 0
) -> str:
    """Emit the verified catcherrors Try/Catch shape (catchAll, bounded retry).

    Dragpoints carry the verified identifier/text pair: Try=`default`,
    Catch=`error` (live component dff0bf83-... shape4). ``retry_count`` is a
    validated 0..5 value; for retry_count=0 the emitted XML and userlabel are
    byte-identical to the M3.R1a output (issue #88).
    """
    retry_label = "no retry" if retry_count == 0 else f"retry {retry_count}"
    dragpoints = (
        f'<dragpoint identifier="default" name="{shape_name}.dragpoint1" '
        f'text="Try" toShape="{_escape_xml(try_to)}" '
        f'x="{_dragpoint_x(shape_index)}" y="{_DRAGPOINT_Y}"/>'
        f'<dragpoint identifier="error" name="{shape_name}.dragpoint2" '
        f'text="Catch" toShape="{_escape_xml(catch_to)}" '
        f'x="{_dragpoint_x(shape_index)}" y="{_CATCH_DRAGPOINT_Y}"/>'
    )
    return (
        f'<shape image="catcherrors_icon" name="{shape_name}" '
        f'shapetype="catcherrors" '
        f'userlabel="Try/Catch all errors ({retry_label}) - route caught documents to the failure handler" '
        f'x="{_shape_x(shape_index)}" y="{_SHAPE_Y}">'
        f'<configuration><catcherrors catchAll="true" retryCount="{retry_count}"/></configuration>'
        f'<dragpoints>{dragpoints}</dragpoints>'
        '</shape>'
    )


def _emit_notify(
    shape_name: str,
    catch_notify: Dict[str, Any],
    next_name: Optional[str],
    shape_index: int,
) -> str:
    """Emit the verified Notify catch-leg step (issue #89).

    Live shape: ``notify`` with ``disableEvent="true"`` (log-only — no platform
    email event, so email/SMS channels stay out of scope), ``enableUserLog`` /
    ``perExecution`` false, a ``<notifyMessage>`` using ``{N}`` placeholders, a
    ``<notifyMessageLevel>``, and a ``<notifyParameters>`` track binding (live
    ``work`` component 1139079f-... shape5, a notify on a catch path).

    Boomi binds runtime properties via numbered placeholders + a notify track
    parameter, not by embedding the property path in the message text. The
    validated ``message_template`` references the caught-error property by its
    token; here that token is substituted for the ``{1}`` placeholder and bound
    as the single track parameter, so the emitted Notify logs the real caught
    error at runtime.
    """
    level = str(catch_notify.get("level") or "").strip().upper()
    template = str(catch_notify.get("message_template") or "")
    message = template.replace(_NOTIFY_CAUGHT_ERROR_TOKEN, "{1}")
    dragpoints = _emit_dragpoints([next_name], shape_index, y=_CATCH_DRAGPOINT_Y)
    return (
        f'<shape image="notify_icon" name="{shape_name}" shapetype="notify" '
        f'userlabel="Notify caught error to the process log" '
        f'x="{_shape_x(shape_index)}" y="{_CATCH_SHAPE_Y}">'
        '<configuration>'
        '<notify disableEvent="true" enableUserLog="false" perExecution="false" '
        'title="Catch path notification">'
        f'<notifyMessage>{_escape_xml(message)}</notifyMessage>'
        f'<notifyMessageLevel>{_escape_xml(level)}</notifyMessageLevel>'
        '<notifyParameters>'
        '<parametervalue key="0" valueType="track">'
        f'<trackparameter defaultValue="" propertyId="{_escape_xml(_NOTIFY_CAUGHT_ERROR_TOKEN)}" '
        'propertyName="Base - Try/Catch Message"/>'
        '</parametervalue>'
        '</notifyParameters>'
        '</notify>'
        '</configuration>'
        f'<dragpoints>{dragpoints}</dragpoints>'
        '</shape>'
    )


def _emit_doccacheload(
    shape_name: str, doc_cache_id: str, shape_index: int, next_name: Optional[str] = None
) -> str:
    """Emit the verified document-cache DLQ catch leg.

    Live shape: doccacheload with a docCache id (live component dff0bf83-...
    shape80). Terminal (empty dragpoints) by default; when ``next_name`` is set
    (issue #89 notify path) it routes to the catch-row Stop.
    """
    dragpoints_xml = (
        f'<dragpoints>{_emit_dragpoints([next_name], shape_index, y=_CATCH_DRAGPOINT_Y)}</dragpoints>'
        if next_name
        else '<dragpoints/>'
    )
    return (
        f'<shape image="doccacheload_icon" name="{shape_name}" '
        f'shapetype="doccacheload" userlabel="Route caught errors to DLQ cache" '
        f'x="{_shape_x(shape_index)}" y="{_CATCH_SHAPE_Y}">'
        f'<configuration><doccacheload docCache="{_escape_xml(doc_cache_id)}"/></configuration>'
        f'{dragpoints_xml}'
        '</shape>'
    )


def _emit_processcall(
    shape_name: str, process_id: str, shape_index: int, next_name: Optional[str] = None
) -> str:
    """Emit the verified error-subprocess DLQ catch leg.

    Live shape: processcall abort="true" wait="true" with empty parameters /
    returnpaths (live component 7b19baeb-... shape34). Terminal (empty
    dragpoints) by default; when ``next_name`` is set (issue #89 notify path)
    it routes to the catch-row Stop.
    """
    dragpoints_xml = (
        f'<dragpoints>{_emit_dragpoints([next_name], shape_index, y=_CATCH_DRAGPOINT_Y)}</dragpoints>'
        if next_name
        else '<dragpoints/>'
    )
    return (
        f'<shape image="processcall_icon" name="{shape_name}" '
        f'shapetype="processcall" userlabel="Route caught errors to error subprocess" '
        f'x="{_shape_x(shape_index)}" y="{_CATCH_SHAPE_Y}">'
        '<configuration>'
        f'<processcall abort="true" processId="{_escape_xml(process_id)}" wait="true">'
        '<parameters/><returnpaths/>'
        '</processcall>'
        '</configuration>'
        f'{dragpoints_xml}'
        '</shape>'
    )


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


# Issue #45 — update-preservation policy. The builder owns the entire
# `<process>` subtree (shapes/transitions/etc.). The sibling
# `<bns:processOverrides>` (which Boomi populates with per-environment
# override values via UI) is NOT in owned_paths, so it survives a
# structured update. bns:encryptedValues and any unknown
# bns:Component-level children are also preserved.
ProcessFlowBuilder.PRESERVATION_POLICY = PreservationPolicy(
    component_type="process",
    owned_paths=(OwnedPath(path="bns:object/process"),),
)


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

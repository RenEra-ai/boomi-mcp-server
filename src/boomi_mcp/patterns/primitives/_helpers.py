"""Issue #27: shared helpers for the source/transform primitive package.

Pure helpers reused across ``db_extract``, ``field_map``, and
``xml_json_convert``. The primitive layer emits JSON
``IntegrationComponentSpec`` objects and delegates ALL XML authoring and
structured validation to the existing builder layer — these helpers only
compute deterministic keys, slugs, and the source→script data-type bridge,
and surface builder validation failures without leaking caller secrets.

No Boomi API calls. No XML emission. No SQL / payload / mapping templates.
"""

from __future__ import annotations

import re
from typing import Any, Iterable, List, Optional

from ...categories.components.builders.connector_builder import (
    BuilderValidationError,
    DatabaseConnectorBuilder,
    RestClientOperationBuilder,
)

_REF_PREFIX = "$ref:"

# ---------------------------------------------------------------------------
# Stable component-role keys (used to build deterministic component keys).
# ---------------------------------------------------------------------------

ROLE_DB_CONNECTION = "db_connection"
ROLE_DB_READ_PROFILE = "db_read_profile"
ROLE_DB_GET_OPERATION = "db_get_operation"
ROLE_TARGET_PROFILE = "target_profile"
ROLE_TRANSFORM_MAP = "transform_map"
ROLE_SCRIPT = "script"

# Issue #28 — REST target component roles. The operational primitives
# (schedule / watermark / dlq / error classifier / run metadata) emit process
# fragments rather than components, so they need no component-key roles.
ROLE_REST_CONNECTION = "rest_connection"
ROLE_REST_OPERATION = "rest_operation"

# Issue #72 — REST *source* component roles (rest_fetch). Distinct from the
# target roles above so an API-to-API flow that emits both a rest_fetch source
# and a rest_send target under the SAME key_prefix produces unique component
# keys (the db_extract + rest_send pairing already stays collision-free because
# DB and REST target roles differ; rest_fetch keeps that invariant for REST↔REST).
ROLE_REST_SOURCE_CONNECTION = "rest_source_connection"
ROLE_REST_SOURCE_OPERATION = "rest_source_operation"

# Issue #74 — DB *target* (write) component roles for the db_write primitive /
# api_to_database_sync preset. Distinct from the DB *source* roles
# (db_connection/db_read_profile/db_get_operation) so an API-to-DB flow that
# emits a db_write target stays collision-free, and so a future read+write flow
# under one key_prefix produces unique component keys for both. The shared
# connection reuses ROLE_DB_CONNECTION (a write group has exactly one connection).
ROLE_DB_WRITE_PROFILE = "db_write_profile"
ROLE_DB_WRITE_OPERATION = "db_write_operation"

# Issue #126 — SOAP Client component roles. ``soap_fetch`` (SOURCE) and
# ``soap_send`` (TARGET) get distinct role tokens so an API-to-API flow emitting
# both a SOAP source and a SOAP target under one ``key_prefix`` stays
# collision-free (mirrors the rest_fetch/rest_send source/target role split).
ROLE_SOAP_SOURCE_CONNECTION = "soap_source_connection"
ROLE_SOAP_SOURCE_OPERATION = "soap_source_operation"
ROLE_SOAP_CONNECTION = "soap_connection"
ROLE_SOAP_OPERATION = "soap_operation"


# Error code raised when a source field data type has no script.mapping input
# equivalent. Lives here (not in profile_generation) because the bridge is a
# primitive-layer concern.
UNSUPPORTED_SCRIPT_INPUT_TYPE = "UNSUPPORTED_SCRIPT_INPUT_TYPE"

# Issue #28 error codes for operational-primitive parameter validation. These
# are primitive-layer concerns (no builder owns them) so they live here.
INVALID_STATUS_CODE = "INVALID_STATUS_CODE"
STATUS_CODE_OVERLAP = "STATUS_CODE_OVERLAP"
SECRET_SHAPED_KEY = "SECRET_SHAPED_KEY"
SECRET_SHAPED_VALUE = "SECRET_SHAPED_VALUE"


# Source (DB read profile) data type -> script.mapping <Input> data type.
# DB read profile fields are character / number / datetime; script.mapping
# inputs are character / date / integer / float (see
# script_mapping_builder._SUPPORTED_INPUT_DATA_TYPES). ``boolean`` is a
# JSON-leaf-only type and is never a DB source field, so it is intentionally
# absent — a script route sources its inputs from the DB extract result.
_SOURCE_TO_SCRIPT_INPUT_TYPE = {
    "character": "character",
    "datetime": "date",
    "number": "float",
}


_SLUG_RE = re.compile(r"[^a-z0-9]+")


def slugify(value: str) -> str:
    """Return a lowercase, underscore-delimited slug safe for component keys.

    Non-alphanumeric runs collapse to a single underscore; leading/trailing
    underscores are stripped. Empty / all-symbol input yields ``"x"`` so a
    key is never blank.
    """
    slug = _SLUG_RE.sub("_", str(value).strip().lower()).strip("_")
    return slug or "x"


def primitive_component_key(key_prefix: str, role: str) -> str:
    """Deterministic component key for a primitive-emitted component role.

    ``role`` is one of the ``ROLE_*`` constants (already a safe token). The
    ``key_prefix`` is slugified so two calls with the same prefix and role
    always produce the same key — that stability is what lets sibling
    components reference each other through ``$ref:<key>`` tokens.
    """
    return f"{slugify(key_prefix)}_{role}"


def script_slot_key(key_prefix: str, slot: int) -> str:
    """Deterministic key for the Nth inline ``script.mapping`` component."""
    return f"{slugify(key_prefix)}_{ROLE_SCRIPT}_{slot}"


def ref_key(value: Any) -> Optional[str]:
    """Return the in-spec component key from a ``'$ref:KEY'`` token.

    Returns ``None`` for a literal UUID or any non-ref value. A transform.map
    that references an in-spec profile via ``$ref`` must list that key in its
    ``depends_on`` so build_integration orders the profile before the map and
    resolves the token (``validate_transform_map`` enforces this with
    ``MAP_PROFILE_REF_REQUIRED``). Literal UUIDs are external and never added
    as dependencies.
    """
    if isinstance(value, str) and value.startswith(_REF_PREFIX):
        key = value[len(_REF_PREFIX):].strip()
        return key or None
    return None


def source_type_to_script_input_type(data_type: Optional[str]) -> str:
    """Map a source field data type to its script.mapping input data type.

    Raises ``BuilderValidationError`` (UNSUPPORTED_SCRIPT_INPUT_TYPE) for any
    source type that has no script input equivalent, rather than silently
    defaulting — a wrong input data type would mis-bind the script port.
    """
    mapped = _SOURCE_TO_SCRIPT_INPUT_TYPE.get(data_type or "")
    if mapped is None:
        raise BuilderValidationError(
            f"source data_type {data_type!r} has no script.mapping input "
            "equivalent",
            error_code=UNSUPPORTED_SCRIPT_INPUT_TYPE,
            field="data_type",
            hint=(
                "Script inputs accept character/date/integer/float. Map a DB "
                "source field of type "
                + ", ".join(sorted(_SOURCE_TO_SCRIPT_INPUT_TYPE))
                + " (character→character, datetime→date, number→float)."
            ),
            details={
                "data_type": data_type,
                "supported_source_types": sorted(_SOURCE_TO_SCRIPT_INPUT_TYPE),
            },
        )
    return mapped


def raise_for_builder_error(error: Optional[BuilderValidationError]) -> None:
    """Re-raise a builder ``validate_config`` failure unchanged.

    Builder errors are already structured (error_code / field / hint) and
    secret-safe — they name offending fields, never echo caller values — so
    the primitive surfaces them verbatim instead of inventing a new envelope.
    A ``None`` (valid) result is a no-op.
    """
    if error is not None:
        raise error


def nonblank_str(*values: Any) -> Optional[str]:
    """Return the first stripped non-blank string among ``values``, else None.

    Mirrors ``integration_builder._first_nonblank_str`` / the issue #27
    ``_blank_to_none`` reuse behavior so a ``"  "`` binding can never survive
    as a truthy-but-meaningless component id or lookup name.
    """
    for value in values:
        if isinstance(value, str) and value.strip():
            return value.strip()
    return None


def validate_status_codes(
    codes: Iterable[Any], field: str
) -> List[int]:
    """Validate one HTTP-status-code set; return it normalized to a list.

    Each entry must be a real ``int`` (``bool`` is rejected — ``True``/``False``
    are ints in Python but never status codes), in the 100..599 range, and
    unique within the set. Cross-set overlap is checked separately by
    :func:`reject_status_code_overlap`. Raises ``BuilderValidationError`` so the
    primitive surfaces the same structured-error envelope as the builders.
    """
    seen: set[int] = set()
    result: List[int] = []
    for code in codes:
        if isinstance(code, bool) or not isinstance(code, int):
            raise BuilderValidationError(
                f"{field} entries must be integer HTTP status codes, got "
                f"{type(code).__name__}",
                error_code=INVALID_STATUS_CODE,
                field=field,
                hint="Use integers in the 100..599 range (e.g. 503).",
                details={"offending_type": type(code).__name__},
            )
        if not (100 <= code <= 599):
            raise BuilderValidationError(
                f"{field} entry {code} is outside the HTTP status range",
                error_code=INVALID_STATUS_CODE,
                field=field,
                hint="HTTP status codes are 100..599.",
                details={"offending_code": code},
            )
        if code in seen:
            raise BuilderValidationError(
                f"{field} contains duplicate status code {code}",
                error_code=INVALID_STATUS_CODE,
                field=field,
                hint="Each status code may appear at most once per set.",
                details={"offending_code": code},
            )
        seen.add(code)
        result.append(code)
    return result


def reject_status_code_overlap(
    retriable: Iterable[int], terminal: Iterable[int]
) -> None:
    """Reject a status code that is in both the retriable and terminal sets.

    A code cannot be both retried and treated as terminal — the classifier
    would be ambiguous. Raises ``BuilderValidationError`` on any overlap.
    """
    overlap = sorted(set(retriable) & set(terminal))
    if overlap:
        raise BuilderValidationError(
            f"status codes {overlap} appear in both retriable and terminal sets",
            error_code=STATUS_CODE_OVERLAP,
            field="retriable_status_codes",
            hint=(
                "A status code must be classified as either retriable or "
                "terminal, not both. Remove the overlap."
            ),
            details={"overlap": overlap},
        )


# Secret-shaped key/value vocabulary reused from the existing builders: the
# operation builder's header/query-param key regex, its secret-value patterns,
# and the connector builders' exact forbidden-field set. The builder regexes
# are anchored (``^...$``) so they only match a key whose WHOLE name is a known
# secret word — composite names like ``secret_key`` / ``db_password`` /
# ``aws_secret_access_key`` would slip through. ``_SECRET_KEY_STEMS`` closes
# that gap with substring matching over the separator-stripped key.
_SECRET_KEY_RE = RestClientOperationBuilder._SECRET_PROPERTY_KEY_RE
_FORBIDDEN_SECRET_FIELDS = frozenset(DatabaseConnectorBuilder.FORBIDDEN_SECRET_FIELDS)
_NONALNUM_RE = re.compile(r"[^a-z0-9]+")
# Substrings matched against the lowercased, separator-stripped key. Chosen to
# catch composite credential names without flagging benign ``*_key`` metadata
# (sort_key / partition_key / idempotency_key) — bare ``key`` is NOT a stem;
# only credential-qualified ``*key`` forms (privatekey / signingkey / …) are.
_SECRET_KEY_STEMS = (
    "password",
    "passwd",
    "passphrase",
    "secret",
    "credential",
    "token",
    "bearer",
    "apikey",
    "oauth",
    "privatekey",
    "signingkey",
    "encryptionkey",
    "accesskey",
    "authkey",
)


def _key_looks_secret(key: str) -> bool:
    """True when a caller-authored key name looks like a credential."""
    lowered = key.strip().lower()
    if lowered in _FORBIDDEN_SECRET_FIELDS or _SECRET_KEY_RE.match(lowered):
        return True
    normalized = _NONALNUM_RE.sub("", lowered)
    return any(stem in normalized for stem in _SECRET_KEY_STEMS)


def value_looks_secret(value: Any) -> bool:
    """True when a value looks like secret material.

    Reuses the REST operation builder's value patterns (JWT shape, long base64,
    ``[encrypted]`` prefix, HTTP auth-scheme prefixes) so the rule stays
    consistent with connector custom-property validation.
    """
    return isinstance(value, str) and RestClientOperationBuilder._value_looks_secret(value)


def scan_secret_keys(mapping: Any, field: str) -> None:
    """Reject secret-shaped keys in an arbitrary metadata mapping.

    Operation primitives (run_metadata, dynamic process properties) accept
    caller-authored key names; a key that looks like a credential
    (``password`` / ``secret_key`` / ``db_password`` / ``api_key`` / …) must be
    rejected before it lands in process metadata. Reuses the existing builder
    secret vocabulary, extended with substring stems for composite names.
    Non-dict input is a no-op (the caller's param model already type-checks it).
    """
    if not isinstance(mapping, dict):
        return
    for key in mapping:
        if not isinstance(key, str):
            continue
        if _key_looks_secret(key):
            raise BuilderValidationError(
                f"{field} key {key!r} matches a secret-shaped name — "
                "credentials must not be stored as run/process metadata",
                error_code=SECRET_SHAPED_KEY,
                field=field,
                hint=(
                    "Remove the credential-shaped key. Model secrets on the "
                    "connection auth (credential_ref / OAuth2) so Boomi injects "
                    "them from the encrypted credential store, never as "
                    "plaintext metadata."
                ),
                details={"offending_key": key},
            )


def scan_secret_values(mapping: Any, field: str) -> None:
    """Reject secret-shaped values in an arbitrary metadata mapping.

    A value backstop for caller metadata: even when the key name is innocuous,
    a JWT / long-base64 / encrypted-marker / auth-scheme-prefixed value must
    not be stored as plaintext metadata. Non-dict input is a no-op.
    """
    if not isinstance(mapping, dict):
        return
    for key, value in mapping.items():
        if value_looks_secret(value):
            raise BuilderValidationError(
                f"{field} value for {key!r} looks like secret material "
                "(JWT / long base64 / encrypted-marker / auth-scheme prefix)",
                error_code=SECRET_SHAPED_VALUE,
                field=field,
                hint=(
                    "Do not store credential material as run/process metadata. "
                    "Model secrets on the connection auth so Boomi injects them "
                    "from the encrypted credential store."
                ),
                details={"offending_key": key},
            )

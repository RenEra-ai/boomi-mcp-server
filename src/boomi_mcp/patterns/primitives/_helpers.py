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
from typing import Any, Optional

from ...categories.components.builders.connector_builder import (
    BuilderValidationError,
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


# Error code raised when a source field data type has no script.mapping input
# equivalent. Lives here (not in profile_generation) because the bridge is a
# primitive-layer concern.
UNSUPPORTED_SCRIPT_INPUT_TYPE = "UNSUPPORTED_SCRIPT_INPUT_TYPE"


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

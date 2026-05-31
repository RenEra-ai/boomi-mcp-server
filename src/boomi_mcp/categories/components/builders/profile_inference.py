"""Issue #47: M7.1 read-only profile-field inference layer.

Turns four kinds of *discovered* artifact into the issue-#43 builder-ready
profile-field contracts:

* ``profile_from_db_metadata`` — caller-supplied DB column metadata summary.
* ``profile_from_sample_json`` — a sample JSON document.
* ``profile_from_xsd``         — a conservative XSD subset.
* ``profile_from_sample_xml``  — a sample XML document.

Each ``infer_profile_*`` function PARSES the artifact and DELEGATES to the
issue-#43 helpers (``profile_from_db_read_fields`` / ``profile_from_json_schema``
/ ``profile_from_xml_schema``) so the emitted builder contract
(``profile_config`` / ``field_index_by_path`` / ``mappable_paths``) stays
byte-identical to what the existing profile/map builders already consume.

Safety contract (this layer is pure):

* No Boomi API calls, no credential lookups, no SDK client construction, no
  network, no direct JDBC. DB metadata is whatever the caller passes in.
* Never echoes sample VALUES (JSON values / XML text) into the output — only
  paths, inferred types, confidence, and ambiguity notes.
* Inference metadata (confidence / ambiguities / confirmation_required) lives in
  a PARALLEL ``fields`` list + top-level ``issues`` list. It is NEVER injected
  into ``field_index_by_path`` nodes or the ``profile_config`` tree, so the
  delegated #43 contract is preserved exactly.
* Field names that look like credentials are withheld from the contract (see
  ``_is_secret_named``) and surfaced as a warning issue, never forwarded into a
  profile/map.

Ambiguity is non-fatal: an ambiguous field is kept with a safe fallback type,
``confidence="ambiguous"``, ``confirmation_required=True`` and forces the
response ``ready_for_builder=False`` — the caller must confirm before applying.
Structural shapes that cannot be represented in the #43 contract at all (scalar
JSON root, empty/heterogeneous arrays, XML attributes, XSD choice, binary DB
columns, namespaces, recursion) raise structured ``PROFILE_INFERENCE_*`` errors.
"""

from __future__ import annotations

from typing import Any, Dict, List, Mapping, Optional

from pydantic import BaseModel

from .connector_builder import BuilderValidationError
from .map_builder import _FORBIDDEN_SECRET_FIELDS
from .profile_generation import profile_from_db_read_fields


# ---------------------------------------------------------------------------
# Error / issue codes
# ---------------------------------------------------------------------------

PROFILE_INFERENCE_INVALID_INPUT = "PROFILE_INFERENCE_INVALID_INPUT"
PROFILE_INFERENCE_INVALID_SAMPLE = "PROFILE_INFERENCE_INVALID_SAMPLE"
PROFILE_INFERENCE_UNSUPPORTED_SHAPE = "PROFILE_INFERENCE_UNSUPPORTED_SHAPE"
PROFILE_INFERENCE_AMBIGUOUS_SHAPE = "PROFILE_INFERENCE_AMBIGUOUS_SHAPE"
PROFILE_INFERENCE_INPUT_TOO_LARGE = "PROFILE_INFERENCE_INPUT_TOO_LARGE"
PROFILE_INFERENCE_UNSUPPORTED_NAMESPACE = "PROFILE_INFERENCE_UNSUPPORTED_NAMESPACE"
PROFILE_INFERENCE_RECURSIVE_XML = "PROFILE_INFERENCE_RECURSIVE_XML"

# Non-fatal advisory issue code (surfaced in the ``issues`` list, not raised).
PROFILE_INFERENCE_SECRET_FIELD_WITHHELD = "PROFILE_INFERENCE_SECRET_FIELD_WITHHELD"


# ---------------------------------------------------------------------------
# Input limits
# ---------------------------------------------------------------------------

# Defaults the caller can lower freely; they may only be RAISED up to the hard
# caps. ``_resolve_limits`` clamps every requested value to ``[1, hard_cap]``.
_DEFAULT_LIMITS: Dict[str, int] = {
    "max_input_chars": 200_000,
    "max_nodes": 1_000,
    "max_fields": 500,
}
_HARD_CAPS: Dict[str, int] = {
    "max_input_chars": 2_000_000,
    "max_nodes": 10_000,
    "max_fields": 5_000,
}


def _resolve_limits(options: Optional[Dict[str, Any]]) -> Dict[str, int]:
    """Return effective limits: defaults, overridden by ``options`` and clamped
    to ``[1, hard_cap]``. Non-integer / missing overrides keep the default."""
    out = dict(_DEFAULT_LIMITS)
    if not options:
        return out
    for key in out:
        if key in options and options[key] is not None:
            try:
                val = int(options[key])
            except (TypeError, ValueError):
                continue
            out[key] = max(1, min(val, _HARD_CAPS[key]))
    return out


# ---------------------------------------------------------------------------
# Secret-name hygiene
# ---------------------------------------------------------------------------


def _is_secret_named(name: Any) -> bool:
    """True when a field name is a credential-shaped WHOLE name.

    Exact (normalized) match against the canonical forbidden set — NOT a
    substring scan — so legitimate columns such as ``authorization_date``,
    ``token_count``, ``bearer_name`` or ``secret_santa_id`` are not withheld,
    while ``password`` / ``api_key`` / ``client_secret`` / ``access_token`` are.
    Mirrors ``map_builder._scan_forbidden_secret_fields`` exact-key semantics.
    """
    if not isinstance(name, str):
        return False
    norm = name.strip().lower().replace("-", "_").replace(" ", "_")
    return norm in _FORBIDDEN_SECRET_FIELDS


# ---------------------------------------------------------------------------
# Shared error + response assembly
# ---------------------------------------------------------------------------


def _err(
    code: str,
    message: str,
    *,
    field: Optional[str] = None,
    hint: Optional[str] = None,
    details: Optional[Dict[str, Any]] = None,
) -> BuilderValidationError:
    """Build (caller raises) a structured inference error."""
    return BuilderValidationError(
        message, error_code=code, field=field, hint=hint, details=details
    )


def _too_large(kind: str, limit: int, observed: int) -> BuilderValidationError:
    return _err(
        PROFILE_INFERENCE_INPUT_TOO_LARGE,
        f"inferred {kind} count {observed} exceeds limit {limit}",
        field="artifact",
        hint=(
            "Reduce the artifact, or raise the matching limit in options "
            "(max_input_chars / max_nodes / max_fields) up to the hard cap."
        ),
        details={"kind": kind, "limit": limit, "observed": observed},
    )


def _secret_withheld_issue(path: str) -> Dict[str, Any]:
    return {
        "severity": "warning",
        "code": PROFILE_INFERENCE_SECRET_FIELD_WITHHELD,
        "field": path,
        "message": (
            "field name looks credential-bearing; withheld from the profile "
            "contract and not forwarded into any profile/map"
        ),
    }


def _assemble(
    helper_result: Dict[str, Any],
    source_type: str,
    meta_by_path: Dict[str, Dict[str, Any]],
    issues: List[Dict[str, Any]],
) -> Dict[str, Any]:
    """Merge the delegated #43 builder contract with the issue-#47 enrichment.

    ``field_index_by_path`` / ``profile_config`` / ``mappable_paths`` pass
    through UNTOUCHED. The enrichment is a parallel ``fields`` list built from
    the index + ``meta_by_path`` (keyed by logical path).
    """
    field_index = helper_result["field_index_by_path"]
    fields: List[Dict[str, Any]] = []
    any_confirmation = False
    for path, entry in field_index.items():
        meta = meta_by_path.get(path, {})
        confirmation_required = bool(meta.get("confirmation_required", False))
        any_confirmation = any_confirmation or confirmation_required
        # DB index entries carry no ``required`` key — fall back to the parsed
        # meta; JSON/XML entries carry the authoritative ``required``.
        required = entry.get("required")
        if required is None:
            required = bool(meta.get("required", False))
        fields.append(
            {
                "path": path,
                "name": entry.get("name"),
                "kind": entry.get("kind", "simple"),
                "data_type": entry.get("data_type"),
                "required": required,
                "mappable": bool(entry.get("mappable", False)),
                "confidence": meta.get("confidence", "high"),
                "ambiguities": list(meta.get("ambiguities", [])),
                "confirmation_required": confirmation_required,
            }
        )

    blocking_issue = any(i.get("severity") == "error" for i in issues)
    return {
        "generation_mode": source_type,
        "component_type": helper_result["component_type"],
        "profile_type": helper_result["profile_type"],
        "component_name": helper_result["component_name"],
        "profile_config": helper_result["profile_config"],
        "field_index_by_path": field_index,
        "mappable_paths": helper_result["mappable_paths"],
        "fields": fields,
        "ready_for_builder": (not any_confirmation) and (not blocking_issue),
        "issues": issues,
        "truncated": False,
        "truncation": None,
    }


def _coerce_options(options: Optional[Any]) -> Dict[str, Any]:
    """The pure layer expects an already-parsed options dict (the action layer
    normalizes JSON strings). Tolerate None and reject non-dict here."""
    if options is None:
        return {}
    if isinstance(options, Mapping):
        return dict(options)
    raise _err(
        PROFILE_INFERENCE_INVALID_INPUT,
        "options must be a mapping (already parsed)",
        field="options",
    )


# ---------------------------------------------------------------------------
# DB metadata inference
# ---------------------------------------------------------------------------

# Base-type keyword tables (matched against the type token before any
# ``(precision)`` suffix). Order of evaluation in ``_classify_db_type`` matters:
# binary (reject) → boolean (ambiguous) → datetime → number → string → unknown
# (ambiguous).
_DB_BINARY_KEYWORDS = ("varbinary", "binary", "blob", "image", "bytea", "raw", "bytes")
_DB_BOOLEAN_KEYWORDS = ("boolean", "bool", "bit")
_DB_DATETIME_KEYWORDS = ("datetime", "timestamp", "date", "time")
_DB_NUMBER_KEYWORDS = (
    "int",
    "decimal",
    "numeric",
    "number",
    "float",
    "double",
    "real",
    "money",
    "bigint",
    "smallint",
    "tinyint",
    "long",
    "short",
    "byte",
    "serial",
)
_DB_STRING_KEYWORDS = (
    "varchar",
    "nvarchar",
    "char",
    "nchar",
    "text",
    "ntext",
    "clob",
    "string",
    "uuid",
    "guid",
    "uniqueidentifier",
)

_DB_TYPE_KEYS = ("data_type", "db_type", "jdbc_type", "type")


def _base_db_type(type_str: str) -> str:
    return type_str.split("(")[0].strip().lower()


def _classify_db_type(type_str: str, field_loc: str):
    """Return ``(data_type, ambiguous)`` for a DB column type, or raise
    ``PROFILE_INFERENCE_UNSUPPORTED_SHAPE`` for binary/blob-like types.

    ``data_type`` is always one of the #43 DB-supported types
    (character/number/datetime); ambiguous boolean/unknown types fall back to
    ``character`` with ``ambiguous=True`` (caller must confirm).
    """
    base = _base_db_type(type_str)
    if any(k in base for k in _DB_BINARY_KEYWORDS):
        raise _err(
            PROFILE_INFERENCE_UNSUPPORTED_SHAPE,
            f"{field_loc}: binary/blob DB type {type_str!r} is not supported",
            field=field_loc,
            hint=(
                "DB read profile generation supports only character, number, "
                "and datetime fields; binary/blob columns must be excluded."
            ),
            details={"db_type": base},
        )
    if any(k in base for k in _DB_BOOLEAN_KEYWORDS):
        return "character", True
    if any(k in base for k in _DB_DATETIME_KEYWORDS):
        return "datetime", False
    if any(k in base for k in _DB_NUMBER_KEYWORDS):
        return "number", False
    if any(k in base for k in _DB_STRING_KEYWORDS):
        return "character", False
    # Unknown, non-binary → ambiguous candidate.
    return "character", True


def _resolve_required(col: Mapping[str, Any]):
    """Return ``(required, had_metadata)`` from nullable/required/mandatory/
    optional, in that priority order."""
    if "nullable" in col and col["nullable"] is not None:
        return (not bool(col["nullable"])), True
    if "required" in col and col["required"] is not None:
        return bool(col["required"]), True
    if "mandatory" in col and col["mandatory"] is not None:
        return bool(col["mandatory"]), True
    if "optional" in col and col["optional"] is not None:
        return (not bool(col["optional"])), True
    return False, False


def _extract_db_columns(artifact: Any) -> List[Any]:
    if isinstance(artifact, list):
        return artifact
    if isinstance(artifact, Mapping):
        for key in ("columns", "fields", "result_columns"):
            if key in artifact:
                cols = artifact[key]
                if not isinstance(cols, list):
                    raise _err(
                        PROFILE_INFERENCE_INVALID_INPUT,
                        f"artifact.{key} must be a list of column metadata",
                        field=f"artifact.{key}",
                    )
                return cols
        raise _err(
            PROFILE_INFERENCE_INVALID_INPUT,
            "DB metadata artifact must provide one of columns/fields/result_columns",
            field="artifact",
            hint="Pass {'columns': [{'name','data_type',...}]} or a bare list.",
            details={"expected_keys": ["columns", "fields", "result_columns"]},
        )
    raise _err(
        PROFILE_INFERENCE_INVALID_INPUT,
        "DB metadata artifact must be a list or a mapping with a columns key",
        field="artifact",
    )


def infer_profile_from_db_metadata(
    artifact: Any, *, options: Optional[Any] = None
) -> Dict[str, Any]:
    """Infer a profile.db read contract from a caller-supplied column summary.

    Operates ONLY on the supplied metadata — no JDBC, no Boomi, no credentials.
    Delegates to ``profile_from_db_read_fields`` so duplicate-name / reserved-
    char validation errors propagate verbatim.
    """
    opts = _coerce_options(options)
    limits = _resolve_limits(opts)
    component_name = opts.get("component_name")

    columns = _extract_db_columns(artifact)

    resolved: List[Dict[str, Any]] = []
    meta_by_path: Dict[str, Dict[str, Any]] = {}
    issues: List[Dict[str, Any]] = []

    for index, col_raw in enumerate(columns):
        loc = f"columns[{index}]"
        if isinstance(col_raw, BaseModel):
            col: Mapping[str, Any] = col_raw.model_dump()
        elif isinstance(col_raw, Mapping):
            col = col_raw
        else:
            raise _err(
                PROFILE_INFERENCE_INVALID_INPUT,
                f"{loc} must be a column metadata mapping",
                field=loc,
            )

        name = col.get("name")
        if not isinstance(name, str) or not name.strip():
            raise _err(
                PROFILE_INFERENCE_INVALID_INPUT,
                f"{loc}.name must be a non-blank string",
                field=f"{loc}.name",
            )
        name = name.strip()

        type_str = None
        for tkey in _DB_TYPE_KEYS:
            val = col.get(tkey)
            if isinstance(val, str) and val.strip():
                type_str = val.strip()
                break
        if type_str is None:
            raise _err(
                PROFILE_INFERENCE_INVALID_INPUT,
                f"{loc} must declare a column type",
                field=f"{loc}.data_type",
                hint="Provide one of data_type / db_type / jdbc_type / type.",
                details={"name": name, "accepted_type_keys": list(_DB_TYPE_KEYS)},
            )

        if _is_secret_named(name):
            issues.append(_secret_withheld_issue(name))
            continue

        data_type, ambiguous = _classify_db_type(type_str, loc)
        required, had_meta = _resolve_required(col)

        if ambiguous:
            confidence = "ambiguous"
            ambiguities = [
                f"source DB type {type_str!r} is not directly representable as a "
                "DB profile field; mapped to character — confirm the intended type"
            ]
            confirmation_required = True
        else:
            confidence = "high" if had_meta else "medium"
            ambiguities = []
            confirmation_required = False
        if not had_meta and not ambiguous:
            ambiguities = ["required/nullable not declared; defaulted to optional"]

        resolved.append({"name": name, "data_type": data_type, "required": required})
        meta_by_path[name] = {
            "confidence": confidence,
            "ambiguities": ambiguities,
            "confirmation_required": confirmation_required,
            "required": required,
        }

    if len(resolved) > limits["max_fields"]:
        raise _too_large("fields", limits["max_fields"], len(resolved))

    helper_result = profile_from_db_read_fields(resolved, component_name=component_name)
    return _assemble(helper_result, "profile_from_db_metadata", meta_by_path, issues)

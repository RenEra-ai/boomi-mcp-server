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

import json as _json
import re
from typing import Any, Dict, List, Mapping, Optional

from pydantic import BaseModel

from .connector_builder import BuilderValidationError
from .map_builder import _FORBIDDEN_SECRET_FIELDS
from .profile_generation import (
    profile_from_db_read_fields,
    profile_from_json_schema,
)


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


# ---------------------------------------------------------------------------
# JSON sample inference
# ---------------------------------------------------------------------------

# Conservative ISO-8601-like date / datetime recognizer (no value echo — only
# the matched/no-match boolean is used).
_ISO_DATETIME_RE = re.compile(
    r"^\d{4}-\d{2}-\d{2}"
    r"([T ]\d{2}:\d{2}(:\d{2})?(\.\d+)?(Z|[+-]\d{2}:?\d{2})?)?$"
)


class _Counters:
    """Tracks parsed node + inferred field counts against the limits and raises
    PROFILE_INFERENCE_INPUT_TOO_LARGE on overflow."""

    def __init__(self, limits: Dict[str, int]) -> None:
        self.nodes = 0
        self.fields = 0
        self.limits = limits

    def add_node(self) -> None:
        self.nodes += 1
        if self.nodes > self.limits["max_nodes"]:
            raise _too_large("nodes", self.limits["max_nodes"], self.nodes)

    def add_field(self) -> None:
        self.fields += 1
        if self.fields > self.limits["max_fields"]:
            raise _too_large("fields", self.limits["max_fields"], self.fields)


def _json_scalar_category(value: Any, dt_detect: bool) -> str:
    """Map a JSON scalar to a category: boolean | number | datetime | character
    | null. ``bool`` is checked before ``int`` (Python ``bool`` ⊂ ``int``)."""
    if value is None:
        return "null"
    if isinstance(value, bool):
        return "boolean"
    if isinstance(value, (int, float)):
        return "number"
    if isinstance(value, str):
        if dt_detect and _ISO_DATETIME_RE.match(value.strip()):
            return "datetime"
        return "character"
    # lists / dicts are handled structurally, never here
    return "character"


def _infer_json_node(
    name: str,
    samples: List[Any],
    path: str,
    *,
    required: bool,
    optional_reason: Optional[str],
    counters: _Counters,
    dt_detect: bool,
    meta_by_path: Dict[str, Dict[str, Any]],
    issues: List[Dict[str, Any]],
) -> Dict[str, Any]:
    """Infer one profile node from ≥1 observed sample values.

    ``samples`` are the values seen for this node (an object value yields one
    sample; array elements yield many). Returns the normalized tree node the
    #43 ``profile_from_json_schema`` consumes; records inference metadata at
    ``meta_by_path[path]``. ``optional_reason`` (set by the parent when the key
    is missing in some sibling rows) forces the node ambiguous + confirmation.
    """
    counters.add_node()

    non_null = [s for s in samples if s is not None]
    has_dict = any(isinstance(s, dict) for s in non_null)
    has_list = any(isinstance(s, list) for s in non_null)
    has_scalar = any(not isinstance(s, (dict, list)) for s in non_null)

    if (has_dict + has_list + has_scalar) > 1:
        raise _err(
            PROFILE_INFERENCE_UNSUPPORTED_SHAPE,
            f"{path}: incompatible mix of object/array/scalar values across the sample",
            field="artifact",
            details={"path": path},
        )

    def _apply_meta(confidence: str, ambiguities: List[str], confirm: bool):
        if optional_reason:
            confidence = "ambiguous"
            ambiguities = list(ambiguities) + [optional_reason]
            confirm = True
        meta_by_path[path] = {
            "confidence": confidence,
            "ambiguities": ambiguities,
            "confirmation_required": confirm,
        }

    # --- object ---
    if has_dict:
        objects = [s for s in non_null if isinstance(s, dict)]
        children, child_keys = _infer_json_object_children(
            objects, path, counters=counters, dt_detect=dt_detect,
            meta_by_path=meta_by_path, issues=issues,
        )
        if not children:
            raise _err(
                PROFILE_INFERENCE_UNSUPPORTED_SHAPE,
                f"{path}: object has no inferable fields",
                field="artifact",
                details={"path": path},
            )
        _apply_meta("high", [], False)
        return {"name": name, "kind": "object", "required": required, "children": children}

    # --- array ---
    if has_list:
        elements: List[Any] = []
        for s in non_null:
            elements.extend(s)
        element_objects = [e for e in elements if isinstance(e, dict)]
        if not elements:
            raise _err(
                PROFILE_INFERENCE_UNSUPPORTED_SHAPE,
                f"{path}: empty array — element shape cannot be inferred",
                field="artifact",
                details={"path": path},
            )
        if len(element_objects) != len(elements):
            raise _err(
                PROFILE_INFERENCE_UNSUPPORTED_SHAPE,
                f"{path}: arrays must contain objects (scalar / mixed arrays are unsupported)",
                field="artifact",
                hint="Repeating JSON profiles require an array of homogeneous objects.",
                details={"path": path},
            )
        # array segment appends [] for descendant paths (matches #43 convention)
        children, _ = _infer_json_object_children(
            element_objects, f"{path}[]", counters=counters, dt_detect=dt_detect,
            meta_by_path=meta_by_path, issues=issues,
        )
        if not children:
            raise _err(
                PROFILE_INFERENCE_UNSUPPORTED_SHAPE,
                f"{path}: array objects have no inferable fields",
                field="artifact",
                details={"path": path},
            )
        _apply_meta("high", [], False)
        return {"name": name, "kind": "array", "required": required, "children": children}

    # --- scalar leaf ---
    counters.add_field()
    cats = {_json_scalar_category(s, dt_detect) for s in samples}
    non_null_cats = cats - {"null"}
    has_null = "null" in cats
    if len(non_null_cats) == 0:
        data_type, confidence, ambiguities, confirm = (
            "character", "ambiguous",
            ["only null values observed; type cannot be inferred"], True,
        )
    elif len(non_null_cats) == 1:
        data_type = next(iter(non_null_cats))
        if has_null:
            confidence, ambiguities, confirm = (
                "medium", ["null observed alongside values"], False,
            )
        else:
            confidence, ambiguities, confirm = "high", [], False
    else:
        data_type, confidence, ambiguities, confirm = (
            "character", "ambiguous",
            ["mixed scalar types observed; defaulted to character"], True,
        )
    _apply_meta(confidence, ambiguities, confirm)
    return {"name": name, "kind": "simple", "data_type": data_type, "required": required}


def _infer_json_object_children(
    objects: List[Dict[str, Any]],
    parent_path: str,
    *,
    counters: _Counters,
    dt_detect: bool,
    meta_by_path: Dict[str, Dict[str, Any]],
    issues: List[Dict[str, Any]],
):
    """Union the keys across ``objects`` (insertion order of first appearance),
    inferring each child from its observed values. Secret-named keys are
    withheld. Returns ``(children, kept_keys)``."""
    ordered_keys: List[str] = []
    for obj in objects:
        for key in obj:
            if key not in ordered_keys:
                ordered_keys.append(key)

    total = len(objects)
    children: List[Dict[str, Any]] = []
    kept: List[str] = []
    for key in ordered_keys:
        if not isinstance(key, str) or not key.strip():
            raise _err(
                PROFILE_INFERENCE_INVALID_SAMPLE,
                f"{parent_path}: object keys must be non-blank strings",
                field="artifact",
            )
        child_name = key.strip()
        child_path = f"{parent_path}/{child_name}"
        present = [obj[key] for obj in objects if key in obj]
        if _is_secret_named(child_name):
            issues.append(_secret_withheld_issue(child_path))
            continue
        optional_reason = None
        child_required = len(present) == total
        if not child_required:
            optional_reason = (
                f"present in only {len(present)}/{total} sampled entries"
            )
        children.append(
            _infer_json_node(
                child_name, present, child_path,
                required=child_required, optional_reason=optional_reason,
                counters=counters, dt_detect=dt_detect,
                meta_by_path=meta_by_path, issues=issues,
            )
        )
        kept.append(child_name)
    return children, kept


def infer_profile_from_sample_json(
    artifact: Any, *, options: Optional[Any] = None
) -> Dict[str, Any]:
    """Infer a profile.json contract from a sample JSON document.

    Accepts a JSON string (parsed with ``json.loads``) or an already-parsed
    dict/list. Object roots map directly; array roots are wrapped in a synthetic
    root object with one repeating child. Sample VALUES are never echoed.
    """
    opts = _coerce_options(options)
    limits = _resolve_limits(opts)
    component_name = opts.get("component_name")
    root_name = opts.get("root_name") or "Root"
    array_item_name = opts.get("array_item_name") or "items"
    dt_detect = opts.get("datetime_detection", True)
    if dt_detect is None:
        dt_detect = True

    if isinstance(artifact, str):
        try:
            parsed = _json.loads(artifact)
        except (ValueError, TypeError) as exc:
            raise _err(
                PROFILE_INFERENCE_INVALID_SAMPLE,
                f"artifact is not valid JSON: {exc}",
                field="artifact",
            )
    else:
        parsed = artifact

    counters = _Counters(limits)
    meta_by_path: Dict[str, Dict[str, Any]] = {}
    issues: List[Dict[str, Any]] = []

    if isinstance(parsed, dict):
        root_node = _infer_json_node(
            root_name, [parsed], root_name, required=True, optional_reason=None,
            counters=counters, dt_detect=dt_detect, meta_by_path=meta_by_path,
            issues=issues,
        )
    elif isinstance(parsed, list):
        # Synthetic root object wrapping a repeating child built from the array.
        issues.append(
            {
                "severity": "info",
                "code": "PROFILE_INFERENCE_ROOT_ARRAY_WRAPPED",
                "field": root_name,
                "message": (
                    f"sample root is an array; wrapped in synthetic object "
                    f"{root_name!r} with repeating child {array_item_name!r}"
                ),
            }
        )
        counters.add_node()  # synthetic root object
        array_child = _infer_json_node(
            array_item_name, [parsed], f"{root_name}/{array_item_name}",
            required=True, optional_reason=None, counters=counters,
            dt_detect=dt_detect, meta_by_path=meta_by_path, issues=issues,
        )
        meta_by_path[root_name] = {
            "confidence": "high", "ambiguities": [], "confirmation_required": False,
        }
        root_node = {
            "name": root_name, "kind": "object", "required": True,
            "children": [array_child],
        }
    else:
        raise _err(
            PROFILE_INFERENCE_UNSUPPORTED_SHAPE,
            "JSON sample root must be an object or an array of objects "
            "(scalar roots are unsupported)",
            field="artifact",
            details={"root_kind": type(parsed).__name__},
        )

    helper_result = profile_from_json_schema(
        {"format": "json", "root": root_node}, component_name=component_name
    )
    return _assemble(helper_result, "profile_from_sample_json", meta_by_path, issues)

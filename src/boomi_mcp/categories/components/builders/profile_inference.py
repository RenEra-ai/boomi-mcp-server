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
from typing import Any, Dict, List, Mapping, Optional, Tuple
from xml.etree import ElementTree as ET

from pydantic import BaseModel

from .connector_builder import BuilderValidationError
from .map_builder import _FORBIDDEN_SECRET_FIELDS
from .profile_generation import (
    profile_from_db_read_fields,
    profile_from_json_schema,
    profile_from_xml_schema,
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


# ---------------------------------------------------------------------------
# Safe XML parsing (shared by XSD + sample-XML modes)
# ---------------------------------------------------------------------------

# Pre-parse screen: reject DOCTYPE / ENTITY declarations outright. This is the
# deliberate stdlib-only XXE / billion-laughs mitigation — xml.etree does not
# expand EXTERNAL entities by default, and rejecting <!DOCTYPE/<!ENTITY also
# blocks INTERNAL entity-expansion bombs. (No third-party defusedxml needed.)
_DOCTYPE_RE = re.compile(r"<!\s*(DOCTYPE|ENTITY)", re.IGNORECASE)


def _require_text_artifact(artifact: Any, what: str) -> str:
    if not isinstance(artifact, str):
        raise _err(
            PROFILE_INFERENCE_INVALID_INPUT,
            f"{what} artifact must be a string",
            field="artifact",
            details={"got": type(artifact).__name__},
        )
    return artifact


def _safe_fromstring(text: str) -> "ET.Element":
    if _DOCTYPE_RE.search(text):
        raise _err(
            PROFILE_INFERENCE_INVALID_SAMPLE,
            "DOCTYPE / ENTITY declarations are not allowed",
            field="artifact",
            hint="Remove the DOCTYPE/ENTITY declaration; external/internal entities are rejected for safety.",
        )
    try:
        return ET.fromstring(text)
    except ET.ParseError as exc:
        raise _err(
            PROFILE_INFERENCE_INVALID_SAMPLE,
            f"artifact is not well-formed XML: {exc}",
            field="artifact",
        )


def _split_qname(tag: str) -> Tuple[Optional[str], str]:
    """Return ``(namespace_uri, local)`` from an ElementTree tag/attr."""
    if tag.startswith("{"):
        uri, _, local = tag[1:].partition("}")
        return uri, local
    return None, tag


# ---------------------------------------------------------------------------
# XSD inference (conservative same-document subset)
# ---------------------------------------------------------------------------

_XSD_NS = "http://www.w3.org/2001/XMLSchema"
_XSD_BUILTIN_PREFIXES = ("xs", "xsd")

_XSD_STRING_TYPES = {
    "string", "normalizedstring", "token", "language", "name", "ncname",
    "nmtoken", "id", "idref", "idrefs", "entity", "anyuri", "qname",
}
_XSD_NUMBER_TYPES = {
    "decimal", "integer", "int", "long", "short", "byte", "nonnegativeinteger",
    "positiveinteger", "negativeinteger", "nonpositiveinteger", "unsignedint",
    "unsignedlong", "unsignedshort", "unsignedbyte", "float", "double",
}
_XSD_DATETIME_TYPES = {"date", "time", "datetime"}
_XSD_BOOLEAN_TYPES = {"boolean"}


def _xsd_local(el: "ET.Element", field_loc: str) -> str:
    """Return the XMLSchema-local tag name; reject foreign-namespace elements."""
    uri, local = _split_qname(el.tag)
    if uri != _XSD_NS:
        raise _err(
            PROFILE_INFERENCE_UNSUPPORTED_SHAPE,
            f"{field_loc}: unexpected non-XSD element {el.tag!r}",
            field="artifact",
        )
    return local


def _map_xsd_builtin(local_type: str, field_loc: str) -> str:
    lt = local_type.lower()
    if lt in _XSD_STRING_TYPES:
        return "character"
    if lt in _XSD_NUMBER_TYPES:
        return "number"
    if lt in _XSD_DATETIME_TYPES:
        return "datetime"
    if lt in _XSD_BOOLEAN_TYPES:
        return "boolean"
    raise _err(
        PROFILE_INFERENCE_UNSUPPORTED_SHAPE,
        f"{field_loc}: XSD built-in type {local_type!r} is not representable "
        "(only string/number/date-time/boolean families are supported)",
        field="artifact",
        details={"xsd_type": local_type},
    )


def _classify_xsd_type_attr(type_qname: str, field_loc: str) -> Tuple[str, str]:
    """Classify a ``type="..."`` QName. Returns ``(kind, value)`` where kind is
    ``"builtin"`` (value=data_type) or ``"local"`` (value=local type name).
    Foreign-namespace prefixes raise UNSUPPORTED_NAMESPACE."""
    prefix, _, local = type_qname.partition(":") if ":" in type_qname else ("", "", type_qname)
    if prefix:
        if prefix in _XSD_BUILTIN_PREFIXES:
            return "builtin", _map_xsd_builtin(local, field_loc)
        raise _err(
            PROFILE_INFERENCE_UNSUPPORTED_NAMESPACE,
            f"{field_loc}: type {type_qname!r} references a foreign namespace prefix",
            field="artifact",
            hint="Namespace-qualified types are not representable by the namespace-less XML profile builder.",
            details={"type": type_qname, "prefix": prefix},
        )
    return "local", local


def _xsd_complex_sequence_elements(
    ctype: "ET.Element", field_loc: str
) -> List["ET.Element"]:
    """Validate a complexType is a plain element-only sequence and return its
    child xs:element list. Rejects mixed content, attributes, choice/all/any/
    group, complex/simpleContent, extension/restriction."""
    if (ctype.get("mixed") or "").strip().lower() in ("true", "1"):
        raise _err(
            PROFILE_INFERENCE_UNSUPPORTED_SHAPE,
            f"{field_loc}: mixed content is not supported",
            field="artifact",
        )
    sequence = None
    for child in list(ctype):
        local = _xsd_local(child, field_loc)
        if local == "annotation":
            continue
        if local == "sequence":
            if sequence is not None:
                raise _err(
                    PROFILE_INFERENCE_UNSUPPORTED_SHAPE,
                    f"{field_loc}: multiple sequences are not supported",
                    field="artifact",
                )
            sequence = child
        elif local in ("attribute", "anyattribute"):
            raise _err(
                PROFILE_INFERENCE_UNSUPPORTED_SHAPE,
                f"{field_loc}: XSD attributes are not supported (element-only)",
                field="artifact",
            )
        else:
            # choice, all, any, group, complexContent, simpleContent, etc.
            raise _err(
                PROFILE_INFERENCE_UNSUPPORTED_SHAPE,
                f"{field_loc}: unsupported complexType construct {local!r} "
                "(only a plain xs:sequence of elements is supported)",
                field="artifact",
                details={"construct": local},
            )
    if sequence is None:
        raise _err(
            PROFILE_INFERENCE_UNSUPPORTED_SHAPE,
            f"{field_loc}: complexType must contain a single xs:sequence",
            field="artifact",
        )
    elements: List["ET.Element"] = []
    for child in list(sequence):
        local = _xsd_local(child, field_loc)
        if local == "annotation":
            continue
        if local == "element":
            elements.append(child)
        else:
            raise _err(
                PROFILE_INFERENCE_UNSUPPORTED_SHAPE,
                f"{field_loc}: xs:sequence may only contain xs:element (found {local!r})",
                field="artifact",
                details={"construct": local},
            )
    return elements


def _xsd_simple_type_base(stype: "ET.Element", field_loc: str) -> str:
    """Resolve a leaf data_type from an inline/named xs:simpleType restriction.
    Rejects list/union."""
    restriction = None
    for child in list(stype):
        local = _xsd_local(child, field_loc)
        if local == "annotation":
            continue
        if local == "restriction":
            restriction = child
        elif local in ("list", "union"):
            raise _err(
                PROFILE_INFERENCE_UNSUPPORTED_SHAPE,
                f"{field_loc}: xs:simpleType {local} is not supported",
                field="artifact",
            )
        else:
            raise _err(
                PROFILE_INFERENCE_UNSUPPORTED_SHAPE,
                f"{field_loc}: unsupported simpleType construct {local!r}",
                field="artifact",
            )
    if restriction is None:
        raise _err(
            PROFILE_INFERENCE_UNSUPPORTED_SHAPE,
            f"{field_loc}: simpleType must use xs:restriction with a base",
            field="artifact",
        )
    base = restriction.get("base")
    if not base:
        raise _err(
            PROFILE_INFERENCE_UNSUPPORTED_SHAPE,
            f"{field_loc}: simpleType restriction must declare a base",
            field="artifact",
        )
    kind, value = _classify_xsd_type_attr(base, field_loc)
    if kind != "builtin":
        raise _err(
            PROFILE_INFERENCE_UNSUPPORTED_SHAPE,
            f"{field_loc}: simpleType base must be a built-in type",
            field="artifact",
        )
    return value


def _xsd_occurs(el: "ET.Element", field_loc: str, *, is_root: bool) -> Tuple[int, int]:
    raw_min = el.get("minOccurs")
    raw_max = el.get("maxOccurs")
    min_occurs = 1 if is_root else 0
    if raw_min is not None:
        try:
            min_occurs = int(raw_min)
        except ValueError:
            raise _err(
                PROFILE_INFERENCE_INVALID_SAMPLE,
                f"{field_loc}: minOccurs must be an integer",
                field="artifact",
            )
    if raw_max is None:
        max_occurs = 1
    elif raw_max == "unbounded":
        max_occurs = -1
    else:
        try:
            max_occurs = int(raw_max)
        except ValueError:
            raise _err(
                PROFILE_INFERENCE_INVALID_SAMPLE,
                f"{field_loc}: maxOccurs must be an integer or 'unbounded'",
                field="artifact",
            )
    return min_occurs, max_occurs


def _xsd_element_to_node(
    el: "ET.Element",
    path: str,
    *,
    is_root: bool,
    complex_types: Dict[str, "ET.Element"],
    simple_types: Dict[str, "ET.Element"],
    type_path: Tuple[str, ...],
    counters: _Counters,
    meta_by_path: Dict[str, Dict[str, Any]],
) -> Dict[str, Any]:
    field_loc = path
    counters.add_node()

    if el.get("ref"):
        raise _err(
            PROFILE_INFERENCE_UNSUPPORTED_SHAPE,
            f"{field_loc}: element references (ref=) / substitution groups are not supported",
            field="artifact",
        )
    if el.get("substitutionGroup"):
        raise _err(
            PROFILE_INFERENCE_UNSUPPORTED_SHAPE,
            f"{field_loc}: substitutionGroup is not supported",
            field="artifact",
        )
    name = el.get("name")
    if not name or not name.strip():
        raise _err(
            PROFILE_INFERENCE_UNSUPPORTED_SHAPE,
            f"{field_loc}: element must declare a name",
            field="artifact",
        )
    name = name.strip()

    min_occurs, max_occurs = _xsd_occurs(el, field_loc, is_root=is_root)
    required = min_occurs >= 1
    meta_by_path[path] = {"confidence": "high", "ambiguities": [], "confirmation_required": False}

    # Determine leaf vs structural.
    inline_complex = None
    inline_simple = None
    for child in list(el):
        local = _xsd_local(child, field_loc)
        if local == "complexType":
            inline_complex = child
        elif local == "simpleType":
            inline_simple = child
        elif local == "annotation":
            continue
        else:
            raise _err(
                PROFILE_INFERENCE_UNSUPPORTED_SHAPE,
                f"{field_loc}: unsupported element child {local!r}",
                field="artifact",
            )

    ctype = inline_complex
    next_type_path = type_path
    leaf_data_type: Optional[str] = None

    if ctype is None and inline_simple is not None:
        leaf_data_type = _xsd_simple_type_base(inline_simple, field_loc)
    elif ctype is None:
        type_attr = el.get("type")
        if not type_attr:
            raise _err(
                PROFILE_INFERENCE_UNSUPPORTED_SHAPE,
                f"{field_loc}: element has neither a type nor an inline definition",
                field="artifact",
            )
        kind, value = _classify_xsd_type_attr(type_attr, field_loc)
        if kind == "builtin":
            leaf_data_type = value
        else:  # local named type
            if value in simple_types:
                leaf_data_type = _xsd_simple_type_base(simple_types[value], field_loc)
            elif value in complex_types:
                if value in type_path:
                    raise _err(
                        PROFILE_INFERENCE_RECURSIVE_XML,
                        f"{field_loc}: recursive type reference {value!r}",
                        field="artifact",
                        details={"type": value, "type_path": list(type_path)},
                    )
                ctype = complex_types[value]
                next_type_path = type_path + (value,)
            else:
                raise _err(
                    PROFILE_INFERENCE_UNSUPPORTED_SHAPE,
                    f"{field_loc}: unknown type {type_attr!r}",
                    field="artifact",
                    details={"type": type_attr},
                )

    if ctype is not None:
        # Structural element.
        elements = _xsd_complex_sequence_elements(ctype, field_loc)
        if not elements:
            raise _err(
                PROFILE_INFERENCE_UNSUPPORTED_SHAPE,
                f"{field_loc}: complexType sequence has no elements",
                field="artifact",
            )
        segment = f"{path}[]" if max_occurs != 1 else path
        children: List[Dict[str, Any]] = []
        for child_el in elements:
            child_name = (child_el.get("name") or "").strip()
            child_path = f"{segment}/{child_name}" if child_name else f"{segment}/?"
            children.append(
                _xsd_element_to_node(
                    child_el, child_path, is_root=False,
                    complex_types=complex_types, simple_types=simple_types,
                    type_path=next_type_path, counters=counters,
                    meta_by_path=meta_by_path,
                )
            )
        return {
            "name": name, "kind": "element", "required": required,
            "min_occurs": min_occurs, "max_occurs": max_occurs, "children": children,
        }

    # Leaf element.
    counters.add_field()
    return {
        "name": name, "kind": "element", "data_type": leaf_data_type,
        "required": required, "min_occurs": min_occurs, "max_occurs": max_occurs,
    }


def infer_profile_from_xsd(artifact: Any, *, options: Optional[Any] = None) -> Dict[str, Any]:
    """Infer a profile.xml contract from a conservative same-document XSD subset.

    Supports inline/same-document xs:element / complexType / sequence /
    simpleType-restriction, minOccurs/maxOccurs(+unbounded). Rejects choice/all/
    any/attributes/mixed/import/include/extension/list/union/substitution with
    actionable errors; target/qualified namespaces → UNSUPPORTED_NAMESPACE;
    self-referential types → RECURSIVE_XML.
    """
    opts = _coerce_options(options)
    limits = _resolve_limits(opts)
    component_name = opts.get("component_name")

    text = _require_text_artifact(artifact, "XSD")
    root = _safe_fromstring(text)

    uri, local = _split_qname(root.tag)
    if uri != _XSD_NS or local != "schema":
        raise _err(
            PROFILE_INFERENCE_INVALID_SAMPLE,
            "root element must be an xs:schema",
            field="artifact",
        )
    if root.get("targetNamespace"):
        raise _err(
            PROFILE_INFERENCE_UNSUPPORTED_NAMESPACE,
            "target namespaces are not representable by the namespace-less XML profile builder",
            field="artifact",
            hint="Remove targetNamespace or wait for namespace-aware profile support.",
        )

    complex_types: Dict[str, "ET.Element"] = {}
    simple_types: Dict[str, "ET.Element"] = {}
    top_elements: List["ET.Element"] = []
    for child in list(root):
        local = _xsd_local(child, "schema")
        if local == "annotation":
            continue
        if local in ("import", "include", "redefine"):
            raise _err(
                PROFILE_INFERENCE_UNSUPPORTED_SHAPE,
                f"xs:{local} (external schema reference) is not supported",
                field="artifact",
                details={"construct": local},
            )
        if local == "complexType":
            ct_name = (child.get("name") or "").strip()
            if ct_name:
                complex_types[ct_name] = child
        elif local == "simpleType":
            st_name = (child.get("name") or "").strip()
            if st_name:
                simple_types[st_name] = child
        elif local == "element":
            top_elements.append(child)
        elif local in ("attribute", "attributegroup", "group"):
            raise _err(
                PROFILE_INFERENCE_UNSUPPORTED_SHAPE,
                f"top-level xs:{local} is not supported",
                field="artifact",
            )
        # other top-level constructs (notation) ignored

    if len(top_elements) != 1:
        raise _err(
            PROFILE_INFERENCE_UNSUPPORTED_SHAPE,
            f"expected exactly one top-level xs:element (found {len(top_elements)})",
            field="artifact",
            hint="Provide a single root element for the profile.",
            details={"top_level_elements": len(top_elements)},
        )

    counters = _Counters(limits)
    meta_by_path: Dict[str, Dict[str, Any]] = {}
    root_el = top_elements[0]
    root_name = (root_el.get("name") or "").strip()
    root_node = _xsd_element_to_node(
        root_el, root_name, is_root=True, complex_types=complex_types,
        simple_types=simple_types, type_path=(), counters=counters,
        meta_by_path=meta_by_path,
    )

    helper_result = profile_from_xml_schema(
        {"format": "xml", "root": root_node}, component_name=component_name
    )
    return _assemble(helper_result, "profile_from_xsd", meta_by_path, [])


# ---------------------------------------------------------------------------
# XML sample inference
# ---------------------------------------------------------------------------

# Integer/decimal recognizer that rejects leading-zero strings (e.g. "00123"
# stays character, since it is almost certainly an identifier code).
_XML_NUMBER_RE = re.compile(r"^-?(0|[1-9]\d*)(\.\d+)?$")


def _xml_scalar_category(text: str, dt_detect: bool) -> str:
    t = text.strip()
    if t == "":
        return "empty"
    low = t.lower()
    if low in ("true", "false"):
        return "boolean"
    if dt_detect and _ISO_DATETIME_RE.match(t):
        return "datetime"
    if _XML_NUMBER_RE.match(t):
        return "number"
    return "character"


def _xml_leaf_type(texts: List[str], dt_detect: bool):
    cats = {_xml_scalar_category(t, dt_detect) for t in texts}
    non_empty = cats - {"empty"}
    if not non_empty:
        return "character", "low", ["empty element(s); type defaulted to character"], False
    if len(non_empty) == 1:
        data_type = next(iter(non_empty))
        ambiguities = ["empty value observed among samples"] if "empty" in cats else []
        return data_type, "medium", ambiguities, False
    return (
        "character",
        "ambiguous",
        ["mixed leaf value types across repeated elements; defaulted to character"],
        True,
    )


def _infer_xml_node(
    tag: str,
    instances: List["ET.Element"],
    path: str,
    ancestors,
    *,
    min_occurs: int,
    max_occurs: int,
    required: bool,
    counters: _Counters,
    dt_detect: bool,
    meta_by_path: Dict[str, Dict[str, Any]],
    issues: List[Dict[str, Any]],
) -> Dict[str, Any]:
    counters.add_node()

    uri, local = _split_qname(tag)
    if uri is not None:
        raise _err(
            PROFILE_INFERENCE_UNSUPPORTED_NAMESPACE,
            f"{path}: namespaced element {tag!r} is not representable by the "
            "namespace-less XML profile builder",
            field="artifact",
            details={"tag": tag},
        )
    if local in ancestors:
        raise _err(
            PROFILE_INFERENCE_RECURSIVE_XML,
            f"{path}: element {local!r} recurses a same-name ancestor",
            field="artifact",
            details={"tag": local, "ancestors": sorted(ancestors)},
        )
    for inst in instances:
        if inst.attrib:
            raise _err(
                PROFILE_INFERENCE_UNSUPPORTED_SHAPE,
                f"{path}: XML attributes are not supported (element-only)",
                field="artifact",
            )

    structural = any(len(list(inst)) > 0 for inst in instances)

    if structural:
        for inst in instances:
            if inst.text and inst.text.strip():
                raise _err(
                    PROFILE_INFERENCE_UNSUPPORTED_SHAPE,
                    f"{path}: mixed content (text alongside child elements) is not supported",
                    field="artifact",
                )
            for child in list(inst):
                if child.tail and child.tail.strip():
                    raise _err(
                        PROFILE_INFERENCE_UNSUPPORTED_SHAPE,
                        f"{path}: mixed content (text between child elements) is not supported",
                        field="artifact",
                    )

        segment = f"{path}[]" if max_occurs != 1 else path
        ordered_tags: List[str] = []
        for inst in instances:
            for child in list(inst):
                if child.tag not in ordered_tags:
                    ordered_tags.append(child.tag)

        children: List[Dict[str, Any]] = []
        child_ancestors = set(ancestors) | {local}
        for child_tag in ordered_tags:
            child_uri, child_local = _split_qname(child_tag)
            counts = [sum(1 for c in inst if c.tag == child_tag) for inst in instances]
            present_parents = sum(1 for c in counts if c > 0)
            child_max = -1 if max(counts) > 1 else 1
            child_required = present_parents == len(instances)
            child_min = 1 if child_required else 0
            name_for_path = child_local if child_uri is None else child_tag
            child_path = f"{segment}/{name_for_path}"
            if child_uri is None and _is_secret_named(child_local):
                issues.append(_secret_withheld_issue(child_path))
                continue
            child_instances = [c for inst in instances for c in inst if c.tag == child_tag]
            children.append(
                _infer_xml_node(
                    child_tag, child_instances, child_path, child_ancestors,
                    min_occurs=child_min, max_occurs=child_max, required=child_required,
                    counters=counters, dt_detect=dt_detect, meta_by_path=meta_by_path,
                    issues=issues,
                )
            )
        if not children:
            raise _err(
                PROFILE_INFERENCE_UNSUPPORTED_SHAPE,
                f"{path}: element has no inferable child elements",
                field="artifact",
            )
        meta_by_path[path] = {"confidence": "high", "ambiguities": [], "confirmation_required": False}
        return {
            "name": local, "kind": "element", "required": required,
            "min_occurs": min_occurs, "max_occurs": max_occurs, "children": children,
        }

    # Leaf element.
    counters.add_field()
    texts = [(inst.text or "") for inst in instances]
    data_type, confidence, ambiguities, confirm = _xml_leaf_type(texts, dt_detect)
    meta_by_path[path] = {
        "confidence": confidence, "ambiguities": ambiguities, "confirmation_required": confirm,
    }
    return {
        "name": local, "kind": "element", "data_type": data_type, "required": required,
        "min_occurs": min_occurs, "max_occurs": max_occurs,
    }


def infer_profile_from_sample_xml(artifact: Any, *, options: Optional[Any] = None) -> Dict[str, Any]:
    """Infer a profile.xml contract from a sample XML document (element-only).

    Repeated sibling elements become ``max_occurs=-1`` with ``[]`` descendant
    paths; children missing from some repeated parents become optional. Leaf
    types are inferred from text WITHOUT echoing the text. Attributes, mixed
    content, namespaced tags, and same-name-ancestor recursion are rejected.
    """
    opts = _coerce_options(options)
    limits = _resolve_limits(opts)
    component_name = opts.get("component_name")
    dt_detect = opts.get("datetime_detection", True)
    if dt_detect is None:
        dt_detect = True

    text = _require_text_artifact(artifact, "XML sample")
    root = _safe_fromstring(text)

    counters = _Counters(limits)
    meta_by_path: Dict[str, Dict[str, Any]] = {}
    issues: List[Dict[str, Any]] = []

    _, root_local = _split_qname(root.tag)
    root_node = _infer_xml_node(
        root.tag, [root], root_local, frozenset(),
        min_occurs=1, max_occurs=1, required=True,
        counters=counters, dt_detect=dt_detect, meta_by_path=meta_by_path, issues=issues,
    )

    helper_result = profile_from_xml_schema(
        {"format": "xml", "root": root_node}, component_name=component_name
    )
    return _assemble(helper_result, "profile_from_sample_xml", meta_by_path, issues)

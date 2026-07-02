"""Issue #43: M2.5b profile field generation helpers.

Converts the database_to_api_sync archetype contract (issue #44) into
deterministic, builder-ready profile field payloads and path indexes that
issue #26 (profile/map XML emission) can consume directly.

These helpers are pure:

* No Boomi API calls.
* No XML emission. Issue #26 owns profile.json / profile.xml and transform.map
  XML.
* No discovery / inference. The infer_profile_fields layer (issue #47) owns
  metadata / sample JSON / XSD / sample XML inference.
* No raw SQL or payload-body echo. Anti-template hygiene mirrors the issue #44
  contract's policy of never echoing caller-supplied values back through
  emitted spec metadata.

The helpers accept either Pydantic model instances from issue #44
(``DBResultField`` / ``DBResultSchema`` / ``JSONPayloadProfile`` /
``JSONProfileNode`` / ``DirectTransformOperation``) or equivalent dict
payloads. This lets the issue #26 (XML emission) and issue #47
(infer_profile_fields) layers reuse the same helpers without going through the
archetype's full Pydantic surface.
"""

from __future__ import annotations

from typing import Any, Dict, Iterable, List, Mapping, NoReturn, Optional

from pydantic import BaseModel

from .connector_builder import BuilderValidationError


# ---------------------------------------------------------------------------
# Error code constants
# ---------------------------------------------------------------------------

PROFILE_GENERATION_VALIDATION_FAILED = "PROFILE_GENERATION_VALIDATION_FAILED"
UNSUPPORTED_PROFILE_GENERATION_SOURCE = "UNSUPPORTED_PROFILE_GENERATION_SOURCE"
DUPLICATE_PROFILE_FIELD_PATH = "DUPLICATE_PROFILE_FIELD_PATH"
UNSUPPORTED_PROFILE_FIELD_TYPE = "UNSUPPORTED_PROFILE_FIELD_TYPE"
INVALID_PROFILE_FIELD_PATH = "INVALID_PROFILE_FIELD_PATH"
PROFILE_FIELD_NOT_FOUND = "PROFILE_FIELD_NOT_FOUND"
PROFILE_FIELD_NOT_MAPPABLE = "PROFILE_FIELD_NOT_MAPPABLE"
DUPLICATE_TARGET_MAPPING = "DUPLICATE_TARGET_MAPPING"

# Issue #26 additions — consumed by the generated profile / direct map builders
# that live alongside this module.
UNSUPPORTED_PROFILE_GENERATION_MODE = "UNSUPPORTED_PROFILE_GENERATION_MODE"
PROFILE_FIELD_VALIDATION_FAILED = "PROFILE_FIELD_VALIDATION_FAILED"
UNSUPPORTED_XML_PROFILE_FEATURE = "UNSUPPORTED_XML_PROFILE_FEATURE"
MAP_PROFILE_REF_REQUIRED = "MAP_PROFILE_REF_REQUIRED"
MAP_PROFILE_INDEX_UNAVAILABLE = "MAP_PROFILE_INDEX_UNAVAILABLE"
MAP_FIELD_NOT_FOUND = "MAP_FIELD_NOT_FOUND"
UNSUPPORTED_TRANSFORM_ROUTE = "UNSUPPORTED_TRANSFORM_ROUTE"

# Issue #40 additions — consumed by MapFunctionBuilder and the function registry.
UNSUPPORTED_MAP_FUNCTION_TYPE = "UNSUPPORTED_MAP_FUNCTION_TYPE"
MAP_FUNCTION_INPUT_COUNT_MISMATCH = "MAP_FUNCTION_INPUT_COUNT_MISMATCH"
MAP_FUNCTION_PARAMETER_MISSING = "MAP_FUNCTION_PARAMETER_MISSING"
MAP_FUNCTION_PARAMETER_INVALID = "MAP_FUNCTION_PARAMETER_INVALID"
UNSUPPORTED_MATH_OPERATION = "UNSUPPORTED_MATH_OPERATION"
# A defined_process_property_* function references a Process Property
# component via a $ref that is missing, not declared in depends_on, or whose
# referenced component is not a processproperty.
MAP_FUNCTION_COMPONENT_REF_REQUIRED = "MAP_FUNCTION_COMPONENT_REF_REQUIRED"

# Issue #41 additions — consumed by ScriptMappingBuilder (standalone
# script.mapping components) and MapScriptBuilder (in-map Scripting
# FunctionStep wiring inside transform.map).
SCRIPT_MAPPING_VALIDATION_FAILED = "SCRIPT_MAPPING_VALIDATION_FAILED"
SCRIPT_MAPPING_BODY_REQUIRED = "SCRIPT_MAPPING_BODY_REQUIRED"
SCRIPT_MAPPING_VARIABLE_INVALID = "SCRIPT_MAPPING_VARIABLE_INVALID"
SCRIPT_MAPPING_LANGUAGE_UNSUPPORTED = "SCRIPT_MAPPING_LANGUAGE_UNSUPPORTED"
SCRIPT_MAPPING_REF_REQUIRED = "SCRIPT_MAPPING_REF_REQUIRED"

# Issue #121 M11.2 additions (epic #118) — consumed by the generic DDP/DPP
# Set Properties flow-sequence steps in process_flow_builder.py.
PROCESS_SET_PROPERTIES_CONFIG_INVALID = "PROCESS_SET_PROPERTIES_CONFIG_INVALID"
PROCESS_PROPERTY_SOURCE_INVALID = "PROCESS_PROPERTY_SOURCE_INVALID"
PROCESS_PROPERTY_NAME_INVALID = "PROCESS_PROPERTY_NAME_INVALID"

# Issue #131 M11.7 additions (epic #118) — consumed by ProcessPropertyBuilder
# (standalone processproperty components).
PROCESS_PROPERTY_VALIDATION_FAILED = "PROCESS_PROPERTY_VALIDATION_FAILED"
PROCESS_PROPERTY_NAME_REQUIRED = "PROCESS_PROPERTY_NAME_REQUIRED"
PROCESS_PROPERTY_PROPERTY_REQUIRED = "PROCESS_PROPERTY_PROPERTY_REQUIRED"
PROCESS_PROPERTY_KEY_REQUIRED = "PROCESS_PROPERTY_KEY_REQUIRED"
PROCESS_PROPERTY_KEY_INVALID = "PROCESS_PROPERTY_KEY_INVALID"
PROCESS_PROPERTY_TYPE_UNSUPPORTED = "PROCESS_PROPERTY_TYPE_UNSUPPORTED"
PROCESS_PROPERTY_DUPLICATE_KEY = "PROCESS_PROPERTY_DUPLICATE_KEY"
PROCESS_PROPERTY_DUPLICATE_NAME = "PROCESS_PROPERTY_DUPLICATE_NAME"
PROCESS_PROPERTY_DEFAULT_INVALID = "PROCESS_PROPERTY_DEFAULT_INVALID"
PROCESS_PROPERTY_RAW_XML_UNSUPPORTED = "PROCESS_PROPERTY_RAW_XML_UNSUPPORTED"

# Issue #122 M11.3 additions (epic #118) — consumed by DocumentCacheBuilder
# (typed documentcache components) and the map-level DocumentCacheJoins
# authoring in map_builder / transform_map_validation.
DOCUMENT_CACHE_VALIDATION_FAILED = "DOCUMENT_CACHE_VALIDATION_FAILED"
DOCUMENT_CACHE_NAME_REQUIRED = "DOCUMENT_CACHE_NAME_REQUIRED"
DOCUMENT_CACHE_PROFILE_TYPE_UNSUPPORTED = "DOCUMENT_CACHE_PROFILE_TYPE_UNSUPPORTED"
DOCUMENT_CACHE_PROFILE_REQUIRED = "DOCUMENT_CACHE_PROFILE_REQUIRED"
DOCUMENT_CACHE_INDEX_REQUIRED = "DOCUMENT_CACHE_INDEX_REQUIRED"
DOCUMENT_CACHE_INDEX_INVALID = "DOCUMENT_CACHE_INDEX_INVALID"
DOCUMENT_CACHE_KEY_INVALID = "DOCUMENT_CACHE_KEY_INVALID"
DOCUMENT_CACHE_KEY_KIND_GATED = "DOCUMENT_CACHE_KEY_KIND_GATED"
DOCUMENT_CACHE_RAW_XML_UNSUPPORTED = "DOCUMENT_CACHE_RAW_XML_UNSUPPORTED"
MAP_DOCUMENT_CACHE_JOINS_INVALID = "MAP_DOCUMENT_CACHE_JOINS_INVALID"


# Supported data type sets — kept in sync with the matching Pydantic Literal
# definitions in src/boomi_mcp/patterns/archetypes/database_to_api_sync.py.
# DB profile fields stay character/number/datetime until DB profile builders
# expand their supported set; JSON leaves additionally accept boolean.
_DB_FIELD_TYPES = ("character", "number", "datetime")
_JSON_LEAF_TYPES = ("character", "number", "datetime", "boolean")

# Reserved logical-path characters — names containing these would collide with
# the path-segment separator or array repetition marker used by
# _flatten_payload_profile_leaves (e.g. "Root/list[]/key").
_RESERVED_PATH_CHARS = ("/", "[", "]")

# Generation source modes handled by the infer_profile_fields layer (issue #47),
# not this #43 explicit-contract path.
_DEFERRED_GENERATION_MODES = (
    "profile_from_db_metadata",
    "profile_from_sample_json",
    "profile_from_xsd",
    "profile_from_sample_xml",
)

_DEFERRED_HINT = (
    "Provide explicit DB result fields or a JSON payload profile tree for M2; "
    "metadata/sample inference is available via infer_profile_fields (issue #47)."
)


# ---------------------------------------------------------------------------
# Shared input coercion
# ---------------------------------------------------------------------------


def _as_mapping(value: Any, field_loc: str) -> Mapping[str, Any]:
    """Return a Mapping view of ``value`` (model_dump for BaseModel inputs).

    Raises PROFILE_GENERATION_VALIDATION_FAILED when ``value`` is neither a
    Pydantic model nor a mapping.
    """
    if isinstance(value, BaseModel):
        return value.model_dump()
    if isinstance(value, Mapping):
        return value
    raise BuilderValidationError(
        f"{field_loc} must be a mapping or Pydantic model",
        error_code=PROFILE_GENERATION_VALIDATION_FAILED,
        field=field_loc,
    )


def _validate_node_name(value: Any, field_loc: str) -> str:
    """Return cleaned, reserved-char-free node/field name.

    Raises:
      PROFILE_GENERATION_VALIDATION_FAILED — when missing or blank.
      INVALID_PROFILE_FIELD_PATH — when the name contains '/', '[', or ']'.
    """
    if not isinstance(value, str) or not value.strip():
        raise BuilderValidationError(
            f"{field_loc}.name must be a non-blank string",
            error_code=PROFILE_GENERATION_VALIDATION_FAILED,
            field=f"{field_loc}.name",
        )
    cleaned = value.strip()
    for reserved in _RESERVED_PATH_CHARS:
        if reserved in cleaned:
            raise BuilderValidationError(
                f"{field_loc}.name must not contain reserved path character "
                f"{reserved!r}",
                error_code=INVALID_PROFILE_FIELD_PATH,
                field=f"{field_loc}.name",
                hint=(
                    "Field and profile node names cannot contain '/', '[', or "
                    "']' — those characters form logical path segments and "
                    "array repetition markers."
                ),
                details={"name": cleaned, "reserved_char": reserved},
            )
    return cleaned


# ---------------------------------------------------------------------------
# DB read profile generation
# ---------------------------------------------------------------------------


def profile_from_db_read_fields(
    fields: Iterable[Any],
    *,
    component_name: Optional[str] = None,
) -> Dict[str, Any]:
    """Generate a profile.db builder payload and source field index from
    caller-declared DB read output fields.

    Input: iterable of ``DBResultField`` instances or dicts with shape
    ``{name, data_type, required, description}``. ``description`` is
    intentionally not echoed into the generated metadata (anti-template
    hygiene).

    The generated ``profile_config.output_fields`` shape matches
    ``DatabaseReadProfileBuilder``'s expectations
    (``{name, data_type, mandatory, enforce_unique}``); ``enforce_unique``
    defaults to ``False`` in M2 and ``required`` maps to ``mandatory``.

    Raises ``BuilderValidationError`` with one of:
      PROFILE_GENERATION_VALIDATION_FAILED (malformed entry / blank name),
      INVALID_PROFILE_FIELD_PATH (name contains reserved path char),
      UNSUPPORTED_PROFILE_FIELD_TYPE (data_type outside the M2 DB set),
      DUPLICATE_PROFILE_FIELD_PATH (duplicate name).
    """
    output_fields: List[Dict[str, Any]] = []
    field_index: Dict[str, Dict[str, Any]] = {}
    mappable_paths: List[str] = []
    seen_first_index: Dict[str, int] = {}

    materialized = list(fields) if fields is not None else []
    if not materialized:
        raise BuilderValidationError(
            "fields must be a non-empty iterable of DB read field entries",
            error_code=PROFILE_GENERATION_VALIDATION_FAILED,
            field="fields",
            hint="Declare at least one DB read output field.",
        )

    for index, entry in enumerate(materialized):
        if isinstance(entry, BaseModel):
            data: Mapping[str, Any] = entry.model_dump()
        elif isinstance(entry, Mapping):
            data = entry
        else:
            raise BuilderValidationError(
                f"fields[{index}] must be a mapping or DBResultField",
                error_code=PROFILE_GENERATION_VALIDATION_FAILED,
                field=f"fields[{index}]",
            )

        name = _validate_node_name(data.get("name"), f"fields[{index}]")

        data_type = data.get("data_type")
        if data_type not in _DB_FIELD_TYPES:
            raise BuilderValidationError(
                f"fields[{index}].data_type={data_type!r} is not a supported "
                "DB profile field type",
                error_code=UNSUPPORTED_PROFILE_FIELD_TYPE,
                field=f"fields[{index}].data_type",
                hint=(
                    "Supported M2 DB source data types: "
                    + ", ".join(_DB_FIELD_TYPES)
                    + "."
                ),
                details={
                    "data_type": data_type,
                    "supported": list(_DB_FIELD_TYPES),
                },
            )

        if name in seen_first_index:
            raise BuilderValidationError(
                f"fields[{index}].name duplicates an earlier field entry",
                error_code=DUPLICATE_PROFILE_FIELD_PATH,
                field=f"fields[{index}].name",
                hint="DB read profile fields must be unique by name.",
                details={
                    "path": name,
                    "first_index": seen_first_index[name],
                    "duplicate_index": index,
                },
            )
        seen_first_index[name] = index

        required = bool(data.get("required", False))
        output_fields.append(
            {
                "name": name,
                "data_type": data_type,
                "mandatory": required,
                "enforce_unique": False,
            }
        )
        field_index[name] = {
            "path": name,
            "name": name,
            "data_type": data_type,
            "mappable": True,
            "profile_component_type": "profile.db",
            "source": "db_read_fields",
        }
        mappable_paths.append(name)

    return {
        "generation_mode": "profile_from_db_read_fields",
        "component_type": "profile.db",
        "profile_type": "database.read",
        "component_name": component_name,
        "profile_config": {"output_fields": output_fields},
        "field_index_by_path": field_index,
        "mappable_paths": mappable_paths,
    }


# ---------------------------------------------------------------------------
# JSON target profile generation
# ---------------------------------------------------------------------------


def profile_from_json_schema(
    payload_profile: Any,
    *,
    component_name: Optional[str] = None,
) -> Dict[str, Any]:
    """Generate a profile.json builder payload and target leaf-path index
    from a caller-supplied JSON profile tree.

    Input: a ``JSONPayloadProfile`` instance or a dict with shape
    ``{format: 'json', root: <JSONProfileNode>}``. The root node must have
    ``kind='object'``.

    The returned ``field_index_by_path`` covers every node (structural and
    leaf); ``mappable_paths`` contains only ``kind='simple'`` leaves —
    object and array nodes describe structure, not scalar values, and so
    cannot receive a direct mapping. ``description`` on nodes is intentionally
    not echoed into the normalized tree (anti-template hygiene).

    Logical path convention matches ``_flatten_payload_profile_leaves`` in
    ``patterns/archetypes/database_to_api_sync.py``: root path is the root
    node name; children of an object use ``"<parent>/<child>"``; children of
    an array use ``"<parent>[]/<child>"`` (the ``[]`` is appended to the
    array segment, not to its children's segments).
    """
    data = _as_mapping(payload_profile, "payload_profile")

    fmt = data.get("format")
    if fmt != "json":
        raise BuilderValidationError(
            "payload_profile.format must be 'json' for M2",
            error_code=PROFILE_GENERATION_VALIDATION_FAILED,
            field="payload_profile.format",
            hint="M2 supports JSON only; XML/EDI/flat-file are deferred.",
            details={"format": fmt},
        )

    root_raw = data.get("root")
    if root_raw is None:
        raise BuilderValidationError(
            "payload_profile.root is required",
            error_code=PROFILE_GENERATION_VALIDATION_FAILED,
            field="payload_profile.root",
        )
    root_dict = _as_mapping(root_raw, "payload_profile.root")

    if root_dict.get("kind") != "object":
        raise BuilderValidationError(
            "payload_profile.root.kind must be 'object'",
            error_code=PROFILE_GENERATION_VALIDATION_FAILED,
            field="payload_profile.root.kind",
            hint=(
                "Boomi JSON profiles require exactly one root object; arrays "
                "and simple leaves are not valid roots in M2."
            ),
            details={"kind": root_dict.get("kind")},
        )

    root_name = _validate_node_name(root_dict.get("name"), "payload_profile.root")

    field_index: Dict[str, Dict[str, Any]] = {}
    mappable_paths: List[str] = []
    normalized_root = _walk_json_node(
        root_dict,
        root_name,
        "payload_profile.root",
        field_index,
        mappable_paths,
    )

    return {
        "generation_mode": "profile_from_json_schema",
        "component_type": "profile.json",
        "profile_type": "json.generated",
        "component_name": component_name,
        "profile_config": {"format": "json", "root": normalized_root},
        "field_index_by_path": field_index,
        "mappable_paths": mappable_paths,
    }


def _walk_json_node(
    node_raw: Any,
    path: str,
    field_loc: str,
    field_index: Dict[str, Dict[str, Any]],
    mappable_paths: List[str],
) -> Dict[str, Any]:
    """Validate ``node_raw`` (registered at logical ``path``), recurse into
    children, and return the normalized node entry for the profile_config
    tree."""
    node = _as_mapping(node_raw, field_loc)
    # Name was validated by the caller before computing ``path``, but a
    # direct external caller would skip that — defensive re-validate here.
    name = _validate_node_name(node.get("name"), field_loc)

    kind = node.get("kind")
    if kind not in ("simple", "object", "array"):
        raise BuilderValidationError(
            f"{field_loc}.kind={kind!r} is not supported",
            error_code=PROFILE_GENERATION_VALIDATION_FAILED,
            field=f"{field_loc}.kind",
            hint="Profile node kind must be 'simple', 'object', or 'array'.",
            details={"kind": kind, "path": path},
        )

    required = bool(node.get("required", False))
    data_type = node.get("data_type")
    children_raw = node.get("children")

    if kind == "simple":
        if data_type not in _JSON_LEAF_TYPES:
            raise BuilderValidationError(
                f"{field_loc}.data_type={data_type!r} is not a supported JSON "
                "leaf data type",
                error_code=UNSUPPORTED_PROFILE_FIELD_TYPE,
                field=f"{field_loc}.data_type",
                hint=(
                    "Supported JSON leaf data types: "
                    + ", ".join(_JSON_LEAF_TYPES)
                    + "."
                ),
                details={
                    "data_type": data_type,
                    "supported": list(_JSON_LEAF_TYPES),
                    "path": path,
                },
            )
        if children_raw is not None:
            raise BuilderValidationError(
                f"{field_loc}.kind='simple' must not declare children",
                error_code=PROFILE_GENERATION_VALIDATION_FAILED,
                field=f"{field_loc}.children",
                details={"path": path},
            )
        field_index[path] = {
            "path": path,
            "name": name,
            "kind": "simple",
            "data_type": data_type,
            "required": required,
            "mappable": True,
            "profile_component_type": "profile.json",
            "source": "json_schema",
        }
        mappable_paths.append(path)
        return {
            "name": name,
            "kind": "simple",
            "data_type": data_type,
            "required": required,
        }

    # object or array
    if data_type is not None:
        raise BuilderValidationError(
            f"{field_loc}.kind={kind!r} must not declare data_type",
            error_code=PROFILE_GENERATION_VALIDATION_FAILED,
            field=f"{field_loc}.data_type",
            details={"kind": kind, "path": path},
        )
    if not isinstance(children_raw, list) or len(children_raw) == 0:
        raise BuilderValidationError(
            f"{field_loc}.kind={kind!r} requires a non-empty children list",
            error_code=PROFILE_GENERATION_VALIDATION_FAILED,
            field=f"{field_loc}.children",
            details={"kind": kind, "path": path},
        )

    # Register the structural node BEFORE walking children so the index is
    # pre-order. Structural nodes are non-mappable; ``data_type`` is None.
    field_index[path] = {
        "path": path,
        "name": name,
        "kind": kind,
        "data_type": None,
        "required": required,
        "mappable": False,
        "profile_component_type": "profile.json",
        "source": "json_schema",
    }

    segment = f"{path}[]" if kind == "array" else path
    seen_child_names: Dict[str, int] = {}
    normalized_children: List[Dict[str, Any]] = []

    for child_index, child_raw in enumerate(children_raw):
        child_field_loc = f"{field_loc}.children[{child_index}]"
        child_data = _as_mapping(child_raw, child_field_loc)
        child_name = _validate_node_name(child_data.get("name"), child_field_loc)

        if child_name in seen_child_names:
            raise BuilderValidationError(
                f"{child_field_loc}.name duplicates an earlier sibling",
                error_code=DUPLICATE_PROFILE_FIELD_PATH,
                field=f"{child_field_loc}.name",
                hint=(
                    "Sibling nodes inside an object or array must use unique "
                    "names; logical paths would otherwise collide."
                ),
                details={
                    "path": f"{segment}/{child_name}",
                    "parent_path": path,
                    "first_index": seen_child_names[child_name],
                    "duplicate_index": child_index,
                },
            )
        seen_child_names[child_name] = child_index

        child_path = f"{segment}/{child_name}"
        normalized_children.append(
            _walk_json_node(
                child_raw,
                child_path,
                child_field_loc,
                field_index,
                mappable_paths,
            )
        )

    return {
        "name": name,
        "kind": kind,
        "required": required,
        "children": normalized_children,
    }


# ---------------------------------------------------------------------------
# XML target profile generation (issue #26)
# ---------------------------------------------------------------------------


def profile_from_xml_schema(
    payload_profile: Any,
    *,
    component_name: Optional[str] = None,
) -> Dict[str, Any]:
    """Generate a profile.xml builder payload and target element-path index
    from a caller-supplied XML profile tree.

    Input: a dict with shape ``{format: 'xml', root: <XMLProfileNode>}``.
    Every node has ``kind='element'``. Element with children → structural
    (non-mappable); element with no children → mappable leaf with a data_type.

    The returned ``field_index_by_path`` covers every node (structural and
    leaf); ``mappable_paths`` contains only the simple leaf paths.

    Logical path convention matches ``_walk_json_node``: repeating elements
    (``max_occurs != 1``) append ``[]`` to the segment used by their
    descendants (mirrors the JSON array convention). Issue #26 is element-
    only: no attributes, namespaces, or schema imports are accepted at this
    layer — those rejections live in the matching XML profile builder.
    """
    data = _as_mapping(payload_profile, "payload_profile")

    fmt = data.get("format")
    if fmt != "xml":
        raise BuilderValidationError(
            "payload_profile.format must be 'xml' for profile_from_xml_schema",
            error_code=PROFILE_GENERATION_VALIDATION_FAILED,
            field="payload_profile.format",
            hint="Use profile_from_json_schema for JSON profile trees.",
            details={"format": fmt},
        )

    root_raw = data.get("root")
    if root_raw is None:
        raise BuilderValidationError(
            "payload_profile.root is required",
            error_code=PROFILE_GENERATION_VALIDATION_FAILED,
            field="payload_profile.root",
        )
    root_dict = _as_mapping(root_raw, "payload_profile.root")

    if root_dict.get("kind") != "element":
        raise BuilderValidationError(
            "payload_profile.root.kind must be 'element'",
            error_code=PROFILE_GENERATION_VALIDATION_FAILED,
            field="payload_profile.root.kind",
            hint=(
                "M2 XML profiles are element-only. Other XML constructs "
                "(attributes, mixed content, namespaces) require the raw-XML "
                "escape hatch; infer_profile_fields (issue #47) infers only the "
                "namespace-less element-only subset from an XSD/sample, not "
                "these constructs."
            ),
            details={"kind": root_dict.get("kind")},
        )

    root_name = _validate_node_name(root_dict.get("name"), "payload_profile.root")

    field_index: Dict[str, Dict[str, Any]] = {}
    mappable_paths: List[str] = []
    normalized_root = _walk_xml_node(
        root_dict,
        root_name,
        "payload_profile.root",
        field_index,
        mappable_paths,
        is_root=True,
    )

    return {
        "generation_mode": "profile_from_xml_schema",
        "component_type": "profile.xml",
        "profile_type": "xml.generated",
        "component_name": component_name,
        "profile_config": {"format": "xml", "root": normalized_root},
        "field_index_by_path": field_index,
        "mappable_paths": mappable_paths,
    }


def _normalize_namespace(ns_raw: Any, field_loc: str) -> Optional[Dict[str, str]]:
    """Validate an optional ``namespace`` node dict ``{uri, prefix?}``.

    Returns ``None`` when absent (the node lives in the empty/default
    namespace), or a normalized ``{"uri": str[, "prefix": str]}`` dict.
    """
    if ns_raw is None:
        return None
    ns = _as_mapping(ns_raw, f"{field_loc}.namespace")
    uri = ns.get("uri")
    if not isinstance(uri, str) or not uri.strip():
        raise BuilderValidationError(
            f"{field_loc}.namespace.uri must be a non-empty string",
            error_code=PROFILE_GENERATION_VALIDATION_FAILED,
            field=f"{field_loc}.namespace.uri",
            details={"namespace": ns_raw},
        )
    out: Dict[str, str] = {"uri": uri.strip()}
    prefix = ns.get("prefix")
    if prefix is not None:
        if not isinstance(prefix, str):
            raise BuilderValidationError(
                f"{field_loc}.namespace.prefix must be a string when provided",
                error_code=PROFILE_GENERATION_VALIDATION_FAILED,
                field=f"{field_loc}.namespace.prefix",
                details={"prefix": prefix},
            )
        if prefix.strip():
            out["prefix"] = prefix.strip()
    return out


def _walk_xml_node(
    node_raw: Any,
    path: str,
    field_loc: str,
    field_index: Dict[str, Dict[str, Any]],
    mappable_paths: List[str],
    *,
    is_root: bool = False,
) -> Dict[str, Any]:
    """Validate an XML element/attribute node at logical ``path``, recurse into
    children, return the normalized profile_config tree entry."""
    node = _as_mapping(node_raw, field_loc)
    name = _validate_node_name(node.get("name"), field_loc)

    kind = node.get("kind")
    if kind not in ("element", "attribute"):
        raise BuilderValidationError(
            f"{field_loc}.kind={kind!r} is not supported; XML profile nodes "
            "must be kind='element' or kind='attribute'",
            error_code=PROFILE_GENERATION_VALIDATION_FAILED,
            field=f"{field_loc}.kind",
            hint="Set kind='element' (or 'attribute' for XML attributes).",
            details={"kind": kind, "path": path},
        )

    children_raw = node.get("children")
    data_type = node.get("data_type")
    required = bool(node.get("required", False))
    namespace = _normalize_namespace(node.get("namespace"), field_loc)

    if kind == "attribute":
        if is_root:
            raise BuilderValidationError(
                f"{field_loc}.kind='attribute' cannot be the profile root",
                error_code=PROFILE_GENERATION_VALIDATION_FAILED,
                field=f"{field_loc}.kind",
                details={"path": path},
            )
        if isinstance(children_raw, list) and children_raw:
            raise BuilderValidationError(
                f"{field_loc} is an attribute and must not have children",
                error_code=PROFILE_GENERATION_VALIDATION_FAILED,
                field=f"{field_loc}.children",
                details={"path": path},
            )
        if data_type not in _JSON_LEAF_TYPES:
            raise BuilderValidationError(
                f"{field_loc}.data_type={data_type!r} is not a supported XML "
                "attribute data type",
                error_code=UNSUPPORTED_PROFILE_FIELD_TYPE,
                field=f"{field_loc}.data_type",
                hint=(
                    "Supported XML attribute data types: "
                    + ", ".join(_JSON_LEAF_TYPES)
                    + "."
                ),
                details={
                    "data_type": data_type,
                    "supported": list(_JSON_LEAF_TYPES),
                    "path": path,
                },
            )
        attr_entry: Dict[str, Any] = {
            "path": path,
            "name": name,
            "kind": "attribute",
            "data_type": data_type,
            "required": required,
            "mappable": True,
            "profile_component_type": "profile.xml",
            "source": "xml_schema",
        }
        if namespace is not None:
            attr_entry["namespace"] = namespace
        field_index[path] = attr_entry
        mappable_paths.append(path)
        attr_node: Dict[str, Any] = {
            "name": name,
            "kind": "attribute",
            "data_type": data_type,
            "required": required,
        }
        if namespace is not None:
            attr_node["namespace"] = namespace
        return attr_node

    min_occurs = node.get("min_occurs", 1 if is_root else 0)
    max_occurs = node.get("max_occurs", 1)

    if not isinstance(min_occurs, int) or min_occurs < 0:
        raise BuilderValidationError(
            f"{field_loc}.min_occurs must be a non-negative integer",
            error_code=PROFILE_GENERATION_VALIDATION_FAILED,
            field=f"{field_loc}.min_occurs",
            details={"min_occurs": min_occurs, "path": path},
        )
    if not isinstance(max_occurs, int) or (max_occurs < 1 and max_occurs != -1):
        raise BuilderValidationError(
            f"{field_loc}.max_occurs must be a positive integer or -1 "
            "(unbounded)",
            error_code=PROFILE_GENERATION_VALIDATION_FAILED,
            field=f"{field_loc}.max_occurs",
            details={"max_occurs": max_occurs, "path": path},
        )

    repeating = max_occurs != 1
    segment = f"{path}[]" if repeating else path
    children_list = children_raw if isinstance(children_raw, list) else []
    # Attributes never make an element "structural"; only child ELEMENTS do.
    element_children = [
        c
        for c in children_list
        if isinstance(c, Mapping) and c.get("kind") != "attribute"
    ]
    has_element_children = len(element_children) > 0

    def _normalize_xml_children() -> List[Dict[str, Any]]:
        # child_seg -> (first_index, namespace_uri-or-None)
        seen_child_names: Dict[str, Any] = {}
        normalized: List[Dict[str, Any]] = []
        for child_index, child_raw in enumerate(children_list):
            child_field_loc = f"{field_loc}.children[{child_index}]"
            child_data = _as_mapping(child_raw, child_field_loc)
            child_name = _validate_node_name(child_data.get("name"), child_field_loc)
            child_seg = (
                f"@{child_name}"
                if child_data.get("kind") == "attribute"
                else child_name
            )
            # Only peek the URI for a well-formed mapping; a malformed namespace
            # value is left for _walk_xml_node()/_normalize_namespace() to reject
            # with a structured error (don't crash with AttributeError here).
            _child_ns = child_data.get("namespace")
            child_ns_uri = (
                _child_ns.get("uri") if isinstance(_child_ns, Mapping) else None
            )
            if child_seg in seen_child_names:
                first_index, first_ns_uri = seen_child_names[child_seg]
                if child_ns_uri != first_ns_uri:
                    # Same local name in DIFFERENT namespaces. The #43 logical-path
                    # model is namespace-less (paths use local names), so these
                    # cannot be disambiguated — reject explicitly rather than with
                    # a misleading bare duplicate-name error.
                    raise BuilderValidationError(
                        f"{child_field_loc}: sibling local name {child_name!r} appears "
                        f"in different namespaces ({first_ns_uri!r} vs {child_ns_uri!r}); "
                        "namespace-less logical paths cannot disambiguate same-named "
                        "siblings across namespaces",
                        error_code=DUPLICATE_PROFILE_FIELD_PATH,
                        field=f"{child_field_loc}.namespace",
                        hint=(
                            "Rename one sibling or restructure; the issue-#43 "
                            "logical-path model does not qualify paths by namespace."
                        ),
                        details={
                            "path": f"{segment}/{child_seg}",
                            "parent_path": path,
                            "namespaces": [first_ns_uri, child_ns_uri],
                        },
                    )
                raise BuilderValidationError(
                    f"{child_field_loc}.name duplicates an earlier sibling",
                    error_code=DUPLICATE_PROFILE_FIELD_PATH,
                    field=f"{child_field_loc}.name",
                    hint=(
                        "Sibling XML elements/attributes must use unique names; "
                        "logical paths would otherwise collide."
                    ),
                    details={
                        "path": f"{segment}/{child_seg}",
                        "parent_path": path,
                        "first_index": first_index,
                        "duplicate_index": child_index,
                    },
                )
            seen_child_names[child_seg] = (child_index, child_ns_uri)
            child_path = f"{segment}/{child_seg}"
            normalized.append(
                _walk_xml_node(
                    child_raw,
                    child_path,
                    child_field_loc,
                    field_index,
                    mappable_paths,
                )
            )
        return normalized

    if has_element_children:
        if data_type is not None:
            raise BuilderValidationError(
                f"{field_loc} has child elements and must not declare data_type",
                error_code=PROFILE_GENERATION_VALIDATION_FAILED,
                field=f"{field_loc}.data_type",
                details={"path": path},
            )
        struct_entry: Dict[str, Any] = {
            "path": path,
            "name": name,
            "kind": "element",
            "data_type": None,
            "required": required,
            "min_occurs": min_occurs,
            "max_occurs": max_occurs,
            "mappable": False,
            "profile_component_type": "profile.xml",
            "source": "xml_schema",
        }
        if namespace is not None:
            struct_entry["namespace"] = namespace
        field_index[path] = struct_entry

        normalized_node: Dict[str, Any] = {
            "name": name,
            "kind": "element",
            "required": required,
            "min_occurs": min_occurs,
            "max_occurs": max_occurs,
            "children": _normalize_xml_children(),
        }
        if namespace is not None:
            normalized_node["namespace"] = namespace
        return normalized_node

    # Leaf element (no child elements) — must have a data_type. It MAY still
    # carry attribute children (e.g. <Hours unit="h">7.5</Hours>).
    if data_type not in _JSON_LEAF_TYPES:
        raise BuilderValidationError(
            f"{field_loc}.data_type={data_type!r} is not a supported XML leaf "
            "data type",
            error_code=UNSUPPORTED_PROFILE_FIELD_TYPE,
            field=f"{field_loc}.data_type",
            hint=(
                "Supported XML leaf data types: "
                + ", ".join(_JSON_LEAF_TYPES)
                + ". XML stores boolean as character format."
            ),
            details={
                "data_type": data_type,
                "supported": list(_JSON_LEAF_TYPES),
                "path": path,
            },
        )
    leaf_entry: Dict[str, Any] = {
        "path": path,
        "name": name,
        "kind": "element",
        "data_type": data_type,
        "required": required,
        "min_occurs": min_occurs,
        "max_occurs": max_occurs,
        "mappable": True,
        "profile_component_type": "profile.xml",
        "source": "xml_schema",
    }
    if namespace is not None:
        leaf_entry["namespace"] = namespace
    field_index[path] = leaf_entry
    mappable_paths.append(path)
    leaf_attr_children = _normalize_xml_children()
    leaf_node: Dict[str, Any] = {
        "name": name,
        "kind": "element",
        "data_type": data_type,
        "required": required,
        "min_occurs": min_occurs,
        "max_occurs": max_occurs,
    }
    if leaf_attr_children:
        leaf_node["children"] = leaf_attr_children
    if namespace is not None:
        leaf_node["namespace"] = namespace
    return leaf_node


# ---------------------------------------------------------------------------
# Direct mapping validation
# ---------------------------------------------------------------------------


def validate_field_mappings(
    source_index: Mapping[str, Mapping[str, Any]],
    target_index: Mapping[str, Mapping[str, Any]],
    mappings: Iterable[Any],
) -> List[Dict[str, Any]]:
    """Validate direct field mappings against generated source/target indexes.

    Input: ``mappings`` is an iterable of ``DirectTransformOperation`` model
    instances or dicts with ``{source_field, target_path}``. Both indexes
    use the shape returned by ``profile_from_db_read_fields`` and
    ``profile_from_json_schema``.

    Returns a list of normalized mapping entries with both endpoints' data
    types attached:
    ``{route: 'direct', source_path, target_path, source_data_type,
    target_data_type}``.

    Raises ``BuilderValidationError`` with one of:
      PROFILE_GENERATION_VALIDATION_FAILED (malformed mapping entry),
      PROFILE_FIELD_NOT_FOUND (unknown source or target path),
      PROFILE_FIELD_NOT_MAPPABLE (target path resolves to a structural node),
      DUPLICATE_TARGET_MAPPING (two mappings bind the same target leaf).
    """
    normalized: List[Dict[str, Any]] = []
    bound_targets: Dict[str, int] = {}

    for index, mapping in enumerate(mappings):
        if isinstance(mapping, BaseModel):
            data: Mapping[str, Any] = mapping.model_dump()
        elif isinstance(mapping, Mapping):
            data = mapping
        else:
            raise BuilderValidationError(
                f"mappings[{index}] must be a mapping or DirectTransformOperation",
                error_code=PROFILE_GENERATION_VALIDATION_FAILED,
                field=f"mappings[{index}]",
            )

        source_field = data.get("source_field")
        target_path = data.get("target_path")

        if not isinstance(source_field, str) or not source_field.strip():
            raise BuilderValidationError(
                f"mappings[{index}].source_field must be a non-blank string",
                error_code=PROFILE_GENERATION_VALIDATION_FAILED,
                field=f"mappings[{index}].source_field",
            )
        if not isinstance(target_path, str) or not target_path.strip():
            raise BuilderValidationError(
                f"mappings[{index}].target_path must be a non-blank string",
                error_code=PROFILE_GENERATION_VALIDATION_FAILED,
                field=f"mappings[{index}].target_path",
            )
        source_field = source_field.strip()
        target_path = target_path.strip()

        source_entry = source_index.get(source_field)
        if source_entry is None:
            raise BuilderValidationError(
                f"mappings[{index}].source_field is not declared in the "
                "source profile field index",
                error_code=PROFILE_FIELD_NOT_FOUND,
                field=f"mappings[{index}].source_field",
                hint=(
                    "Reference a name declared in source.read_operation."
                    "result_schema.fields."
                ),
                details={"path": source_field, "side": "source"},
            )

        target_entry = target_index.get(target_path)
        if target_entry is None:
            raise BuilderValidationError(
                f"mappings[{index}].target_path is not declared in the "
                "target profile field index",
                error_code=PROFILE_FIELD_NOT_FOUND,
                field=f"mappings[{index}].target_path",
                hint=(
                    "Reference a leaf path declared in target.payload_profile "
                    "(e.g. 'Root/list[]/key')."
                ),
                details={"path": target_path, "side": "target"},
            )
        if not target_entry.get("mappable", False):
            raise BuilderValidationError(
                f"mappings[{index}].target_path resolves to a non-mappable "
                "structural node",
                error_code=PROFILE_FIELD_NOT_MAPPABLE,
                field=f"mappings[{index}].target_path",
                hint=(
                    "Only scalar leaves are mappable destinations: simple JSON "
                    "leaves (kind='simple') or XML elements without children. "
                    "Object / array / structural-element nodes describe shape, "
                    "not scalar values."
                ),
                details={
                    "path": target_path,
                    "kind": target_entry.get("kind"),
                },
            )

        if target_path in bound_targets:
            raise BuilderValidationError(
                f"mappings[{index}].target_path is bound more than once",
                error_code=DUPLICATE_TARGET_MAPPING,
                field=f"mappings[{index}].target_path",
                hint=(
                    "Each target leaf path may receive at most one direct "
                    "mapping."
                ),
                details={
                    "path": target_path,
                    "first_index": bound_targets[target_path],
                    "duplicate_index": index,
                },
            )
        bound_targets[target_path] = index

        normalized.append(
            {
                "route": "direct",
                "source_path": source_field,
                "target_path": target_path,
                "source_data_type": source_entry.get("data_type"),
                "target_data_type": target_entry.get("data_type"),
            }
        )

    return normalized


# ---------------------------------------------------------------------------
# Convenience aggregator
# ---------------------------------------------------------------------------


def build_profile_generation_artifacts(
    source_result_schema: Any,
    target_payload_profile: Any,
    direct_operations: Optional[Iterable[Any]] = None,
) -> Dict[str, Any]:
    """Aggregate source profile, target profile, and direct mapping
    validation into a single artifacts payload consumed by the
    database_to_api_sync archetype's ``emit_spec`` (and any future caller
    that already holds the issue #44 contract pieces).

    The returned dict carries:
      * ``source``  — ``profile_from_db_read_fields`` output.
      * ``target``  — ``profile_from_json_schema`` output.
      * ``direct_mappings`` — list of normalized direct mappings.
      * ``unsupported_sources`` — static list of inference generation modes
        pointing at infer_profile_fields (issue #47); useful for downstream callers that surface a
        "what M2 does not support" table.
    """
    schema_data = _as_mapping(source_result_schema, "source_result_schema")
    raw_fields = schema_data.get("fields")
    if not isinstance(raw_fields, list) or not raw_fields:
        raise BuilderValidationError(
            "source_result_schema.fields must be a non-empty list",
            error_code=PROFILE_GENERATION_VALIDATION_FAILED,
            field="source_result_schema.fields",
        )

    source = profile_from_db_read_fields(raw_fields)
    target = profile_from_json_schema(target_payload_profile)
    direct_mappings = validate_field_mappings(
        source["field_index_by_path"],
        target["field_index_by_path"],
        direct_operations or [],
    )

    unsupported = [
        {"mode": mode, "deferred_to_issue": "#47", "hint": _DEFERRED_HINT}
        for mode in _DEFERRED_GENERATION_MODES
    ]

    return {
        "source": source,
        "target": target,
        "direct_mappings": direct_mappings,
        "unsupported_sources": unsupported,
    }


# ---------------------------------------------------------------------------
# Deferred-mode dispatcher
# ---------------------------------------------------------------------------


def reject_unsupported_generation_source(generation_mode: str) -> NoReturn:
    """Raise ``UNSUPPORTED_PROFILE_GENERATION_SOURCE`` for any generation
    mode now handled by infer_profile_fields (issue #47), plus any unrecognized
    mode name.

    Used when a caller asks this #43 explicit-contract path for a discovery /
    inference mode it does not implement — those modes live in the
    infer_profile_fields layer, not here.
    """
    raise BuilderValidationError(
        f"generation_mode={generation_mode!r} is not supported in M2",
        error_code=UNSUPPORTED_PROFILE_GENERATION_SOURCE,
        field="generation_mode",
        hint=_DEFERRED_HINT,
        details={
            "mode": generation_mode,
            "deferred_to_issue": "#47",
        },
    )

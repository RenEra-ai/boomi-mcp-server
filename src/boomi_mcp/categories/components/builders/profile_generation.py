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

import xml.etree.ElementTree as ET
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

# Issue #95 M7.5 additions — consumed by the live existing-profile indexer
# (``index_existing_profile_xml``) and the read-only ``index_profile_component``
# discovery surface. These are the STRUCTURED parse-failure codes; the
# map-validation boundary still surfaces ``MAP_PROFILE_INDEX_UNAVAILABLE`` when a
# literal-UUID map endpoint has no usable index.
PROFILE_INDEX_PARSE_FAILED = "PROFILE_INDEX_PARSE_FAILED"
PROFILE_INDEX_UNSUPPORTED_TYPE = "PROFILE_INDEX_UNSUPPORTED_TYPE"
PROFILE_INDEX_DUPLICATE_PATH = "PROFILE_INDEX_DUPLICATE_PATH"
PROFILE_INDEX_STRUCTURE_INVALID = "PROFILE_INDEX_STRUCTURE_INVALID"

# The three live profile component types the indexer understands, mapped from
# the exported profile-root local element name.
PROFILE_INDEX_SUPPORTED_TYPES = ("profile.json", "profile.xml", "profile.db")
_PROFILE_ROOT_LOCAL_TO_TYPE = {
    "JSONProfile": "profile.json",
    "XMLProfile": "profile.xml",
    "DatabaseProfile": "profile.db",
}

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

# Issue #123 M11.4 additions (epic #118) — consumed by cache_property_lineage
# (plan-time scope/lineage validation of DDP/DPP/cache handoffs).
PROCESS_LINEAGE_DDP_SCOPE_INVALID = "PROCESS_LINEAGE_DDP_SCOPE_INVALID"
PROCESS_LINEAGE_PROPERTY_READ_BEFORE_WRITE = "PROCESS_LINEAGE_PROPERTY_READ_BEFORE_WRITE"
PROCESS_LINEAGE_CACHE_WRITER_MISSING = "PROCESS_LINEAGE_CACHE_WRITER_MISSING"
PROCESS_LINEAGE_BRANCH_ORDER_INVALID = "PROCESS_LINEAGE_BRANCH_ORDER_INVALID"
PROCESS_LINEAGE_AMBIGUOUS_LAST_WRITE = "PROCESS_LINEAGE_AMBIGUOUS_LAST_WRITE"

# Issue #133 M6.1 additions — consumed by ApiServiceBuilder (typed
# webservice / API Service Components) and the integration-builder /
# orchestration route validation around them.
API_SERVICE_VALIDATION_FAILED = "API_SERVICE_VALIDATION_FAILED"
API_SERVICE_NAME_REQUIRED = "API_SERVICE_NAME_REQUIRED"
API_SERVICE_ROUTES_REQUIRED = "API_SERVICE_ROUTES_REQUIRED"
API_SERVICE_ROUTE_PROCESS_REQUIRED = "API_SERVICE_ROUTE_PROCESS_REQUIRED"
API_SERVICE_ROUTE_PROCESS_REF_INVALID = "API_SERVICE_ROUTE_PROCESS_REF_INVALID"
API_SERVICE_ROUTE_PROCESS_NOT_LISTEN = "API_SERVICE_ROUTE_PROCESS_NOT_LISTEN"
API_SERVICE_DUPLICATE_ROUTE = "API_SERVICE_DUPLICATE_ROUTE"
API_SERVICE_METHOD_UNSUPPORTED = "API_SERVICE_METHOD_UNSUPPORTED"
API_SERVICE_TYPE_UNSUPPORTED = "API_SERVICE_TYPE_UNSUPPORTED"
API_SERVICE_RAW_XML_UNSUPPORTED = "API_SERVICE_RAW_XML_UNSUPPORTED"
API_SERVICE_PROFILE_OVERRIDES_UNSUPPORTED = "API_SERVICE_PROFILE_OVERRIDES_UNSUPPORTED"
API_SERVICE_RUNTIME_TIER_MISMATCH = "API_SERVICE_RUNTIME_TIER_MISMATCH"


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


# ---------------------------------------------------------------------------
# Issue #95 M7.5 — live existing-profile XML indexer
# ---------------------------------------------------------------------------
#
# Turn an EXPORTED Boomi profile component (``profile.json`` / ``profile.xml`` /
# ``profile.db``) into the same ``field_index_by_path`` contract that the
# generated-profile builders' ``build_field_index()`` emits, so a transform.map
# referencing a literal existing-profile UUID can be validated against real
# field key/keyPath/namePath/mappability — pre-mutation.
#
# Pure: stdlib ``xml.etree.ElementTree`` only, no SDK / network / credentials.
# Namespace-insensitive (local element names; the #43 logical-path model is
# namespace-less). Fail-closed: a structural container is never mappable even
# when its exported ``isMappable`` says otherwise; a leaf is mappable ONLY when
# it explicitly declares ``isMappable="true"``.


def _local_name(tag: Any) -> str:
    """Return an element's local name with any ``{namespace}`` prefix stripped."""
    if not isinstance(tag, str):
        return ""
    return tag.rsplit("}", 1)[-1] if "}" in tag else tag


def _first_child(parent: Any, local: str) -> Any:
    """First direct child of ``parent`` whose local name is ``local`` (or None)."""
    for child in list(parent):
        if _local_name(child.tag) == local:
            return child
    return None


def _iter_children(parent: Any, local: str) -> List[Any]:
    """All direct children of ``parent`` whose local name is ``local``."""
    return [child for child in list(parent) if _local_name(child.tag) == local]


def _require_element_key(element: Any, node_label: str) -> str:
    """Return the platform ``key`` attribute, or raise a structured failure.

    A profile node without a platform key cannot be referenced by a map
    (``fromKeyPath`` / ``toKeyPath`` reconstruct from these), so a missing /
    blank key is a hard structural failure rather than a silently dropped node.
    """
    key = element.get("key")
    if key is None or not str(key).strip():
        raise BuilderValidationError(
            f"{node_label} is missing its platform 'key' attribute",
            error_code=PROFILE_INDEX_STRUCTURE_INVALID,
            field="component_xml",
            hint=(
                "The exported profile XML is malformed — every JSON/XML/DB "
                "profile node carries an integer platform key. Re-export the "
                "component."
            ),
            details={"node": node_label},
        )
    return str(key).strip()


def _add_profile_index_entry(
    field_index: Dict[str, Dict[str, Any]],
    mappable_paths: List[str],
    *,
    logical_path: str,
    name: str,
    key: str,
    key_path: List[str],
    name_path: List[str],
    data_type: Optional[str],
    kind: str,
    mappable: bool,
) -> None:
    """Register one field-index entry; reject a duplicate canonical path.

    Entry shape is a superset of ``build_field_index``'s (adds ``is_mappable`` /
    ``structural``; ``profile_component_type`` is stamped by the caller): the map
    builder reads ``key`` / ``key_path`` / ``name_path`` and validation reads
    ``mappable``, so a live-indexed profile validates and renders exactly like an
    equivalent generated one.
    """
    if logical_path in field_index:
        raise BuilderValidationError(
            f"two profile nodes resolve to the same canonical path "
            f"{logical_path!r}",
            error_code=PROFILE_INDEX_DUPLICATE_PATH,
            field="component_xml",
            hint=(
                "Sibling profile nodes must use unique names; namespace-less "
                "logical paths would otherwise collide."
            ),
            details={"path": logical_path},
        )
    field_index[logical_path] = {
        "path": logical_path,
        "name": name,
        "key": key,
        "key_path": "/".join(key_path),
        "name_path": "/".join(name_path),
        "data_type": data_type,
        "kind": kind,
        "mappable": mappable,
        "is_mappable": mappable,
        "structural": not mappable,
    }
    if mappable:
        mappable_paths.append(logical_path)


def index_existing_profile_xml(component_xml: Any) -> Dict[str, Any]:
    """Index an exported Boomi profile component's XML into ``field_index_by_path``.

    Accepts either the full ``<bns:Component type="profile.*">`` envelope or the
    bare inner profile root (``<JSONProfile>`` / ``<XMLProfile>`` /
    ``<DatabaseProfile>``).

    Returns ``{profile_component_type, field_index_by_path, mappable_paths}``.
    ``field_index_by_path`` includes both structural containers (non-mappable) and
    leaves; ``mappable_paths`` lists only mappable leaves. Raises
    ``BuilderValidationError`` with one of ``PROFILE_INDEX_PARSE_FAILED`` /
    ``PROFILE_INDEX_UNSUPPORTED_TYPE`` / ``PROFILE_INDEX_STRUCTURE_INVALID`` /
    ``PROFILE_INDEX_DUPLICATE_PATH`` on malformed input.
    """
    if not isinstance(component_xml, str) or not component_xml.strip():
        raise BuilderValidationError(
            "component_xml must be a non-empty XML string",
            error_code=PROFILE_INDEX_PARSE_FAILED,
            field="component_xml",
        )
    try:
        root = ET.fromstring(component_xml)
    except ET.ParseError as exc:
        raise BuilderValidationError(
            "profile component XML could not be parsed",
            error_code=PROFILE_INDEX_PARSE_FAILED,
            field="component_xml",
            details={"parse_error": str(exc)},
        )

    profile_root = _find_profile_root(root)
    if profile_root is None:
        raise BuilderValidationError(
            "no JSONProfile / XMLProfile / DatabaseProfile root found in the "
            "component XML",
            error_code=PROFILE_INDEX_UNSUPPORTED_TYPE,
            field="component_xml",
            hint=(
                "index_profile_component supports profile.json, profile.xml, and "
                "profile.db components only."
            ),
        )

    component_type = _PROFILE_ROOT_LOCAL_TO_TYPE[_local_name(profile_root.tag)]
    field_index: Dict[str, Dict[str, Any]] = {}
    mappable_paths: List[str] = []

    if component_type == "profile.json":
        _index_json_profile(profile_root, field_index, mappable_paths)
    elif component_type == "profile.xml":
        _index_xml_profile(profile_root, field_index, mappable_paths)
    else:
        _index_db_profile(profile_root, field_index, mappable_paths)

    for entry in field_index.values():
        entry["profile_component_type"] = component_type

    return {
        "profile_component_type": component_type,
        "field_index_by_path": field_index,
        "mappable_paths": mappable_paths,
    }


def _find_profile_root(root: Any) -> Any:
    """Locate the ``<JSONProfile>`` / ``<XMLProfile>`` / ``<DatabaseProfile>``.

    Handles the full ``<bns:Component>`` envelope (root → ``<bns:object>`` →
    profile root) as well as a bare profile root passed directly.
    """
    if _local_name(root.tag) in _PROFILE_ROOT_LOCAL_TO_TYPE:
        return root
    obj = _first_child(root, "object")
    if obj is not None:
        for child in list(obj):
            if _local_name(child.tag) in _PROFILE_ROOT_LOCAL_TO_TYPE:
                return child
    # Defensive fallback: scan descendants (a wrapper we didn't anticipate).
    for element in root.iter():
        if _local_name(element.tag) in _PROFILE_ROOT_LOCAL_TO_TYPE:
            return element
    return None


def _require_data_elements(profile_root: Any) -> Any:
    data_elements = _first_child(profile_root, "DataElements")
    if data_elements is None:
        raise BuilderValidationError(
            "profile root has no <DataElements> section",
            error_code=PROFILE_INDEX_STRUCTURE_INVALID,
            field="component_xml",
        )
    return data_elements


# ---- JSON ----------------------------------------------------------------


def _index_json_profile(
    profile_root: Any,
    field_index: Dict[str, Dict[str, Any]],
    mappable_paths: List[str],
) -> None:
    data_elements = _require_data_elements(profile_root)
    root_value = _first_child(data_elements, "JSONRootValue")
    if root_value is None:
        raise BuilderValidationError(
            "JSON profile has no <JSONRootValue>",
            error_code=PROFILE_INDEX_STRUCTURE_INVALID,
            field="component_xml",
        )
    root_key = _require_element_key(root_value, "JSONRootValue")
    root_name = root_value.get("name") or "Root"
    # The JSONRootValue is the structural anchor (it wraps a JSONObject named
    # "Object"). Its own index entry carries only its key/name — matching
    # json_profile_builder's index[root_name].
    _add_profile_index_entry(
        field_index,
        mappable_paths,
        logical_path=root_name,
        name=root_name,
        key=root_key,
        key_path=[f"*[@key='{root_key}']"],
        name_path=[root_name],
        data_type=None,
        kind="object",
        mappable=False,
    )
    wrapper = _first_child(root_value, "JSONObject")
    if wrapper is None:
        return
    wrapper_key = _require_element_key(wrapper, "JSONObject")
    wrapper_name = wrapper.get("name") or "Object"
    _walk_json_object_entries(
        wrapper,
        parent_logical=root_name,
        parent_key_path=[f"*[@key='{root_key}']", f"*[@key='{wrapper_key}']"],
        parent_name_path=[root_name, wrapper_name],
        field_index=field_index,
        mappable_paths=mappable_paths,
    )


def _walk_json_object_entries(
    obj_element: Any,
    *,
    parent_logical: str,
    parent_key_path: List[str],
    parent_name_path: List[str],
    field_index: Dict[str, Dict[str, Any]],
    mappable_paths: List[str],
) -> None:
    for entry in _iter_children(obj_element, "JSONObjectEntry"):
        entry_key = _require_element_key(entry, "JSONObjectEntry")
        entry_name = entry.get("name") or ""
        logical = f"{parent_logical}/{entry_name}"
        entry_key_path = parent_key_path + [f"*[@key='{entry_key}']"]
        entry_name_path = parent_name_path + [entry_name]

        object_child = _first_child(entry, "JSONObject")
        array_child = _first_child(entry, "JSONArray")

        if object_child is not None:
            # Structural object (non-mappable regardless of isMappable).
            _add_profile_index_entry(
                field_index,
                mappable_paths,
                logical_path=logical,
                name=entry_name,
                key=entry_key,
                key_path=entry_key_path,
                name_path=entry_name_path,
                data_type=None,
                kind="object",
                mappable=False,
            )
            wrapper_key = _require_element_key(object_child, "JSONObject")
            wrapper_name = object_child.get("name") or "Object"
            _walk_json_object_entries(
                object_child,
                parent_logical=logical,
                parent_key_path=entry_key_path + [f"*[@key='{wrapper_key}']"],
                parent_name_path=entry_name_path + [wrapper_name],
                field_index=field_index,
                mappable_paths=mappable_paths,
            )
        elif array_child is not None:
            # Structural array (non-mappable).
            _add_profile_index_entry(
                field_index,
                mappable_paths,
                logical_path=logical,
                name=entry_name,
                key=entry_key,
                key_path=entry_key_path,
                name_path=entry_name_path,
                data_type=None,
                kind="array",
                mappable=False,
            )
            array_key = _require_element_key(array_child, "JSONArray")
            array_name = array_child.get("name") or "Array"
            array_elem = _first_child(array_child, "JSONArrayElement")
            if array_elem is None:
                continue
            elem_key = _require_element_key(array_elem, "JSONArrayElement")
            elem_name = array_elem.get("name") or entry_name
            inner_obj = _first_child(array_elem, "JSONObject")
            # Only arrays-of-objects expose per-field mappable leaves (matches
            # json_profile_builder, which emits arrays as object wrappers). An
            # array of scalars stays a structural container with no leaf entries.
            if inner_obj is None:
                continue
            inner_key = _require_element_key(inner_obj, "JSONObject")
            inner_name = inner_obj.get("name") or "Object"
            _walk_json_object_entries(
                inner_obj,
                parent_logical=f"{logical}[]",
                parent_key_path=entry_key_path
                + [
                    f"*[@key='{array_key}']",
                    f"*[@key='{elem_key}']",
                    f"*[@key='{inner_key}']",
                ],
                parent_name_path=entry_name_path
                + [array_name, elem_name, inner_name],
                field_index=field_index,
                mappable_paths=mappable_paths,
            )
        else:
            # Scalar leaf — honor the explicit isMappable, fail closed otherwise.
            _add_profile_index_entry(
                field_index,
                mappable_paths,
                logical_path=logical,
                name=entry_name,
                key=entry_key,
                key_path=entry_key_path,
                name_path=entry_name_path,
                data_type=entry.get("dataType"),
                kind="simple",
                mappable=entry.get("isMappable") == "true",
            )


# ---- XML -----------------------------------------------------------------


def _index_xml_profile(
    profile_root: Any,
    field_index: Dict[str, Dict[str, Any]],
    mappable_paths: List[str],
) -> None:
    # Only the <DataElements> subtree describes the mappable element tree. The
    # sibling <Namespaces> section carries <Types>/<Type>/<XMLElement> type
    # DEFINITIONS (with their own keys) that MUST NOT be indexed as data paths.
    data_elements = _require_data_elements(profile_root)
    for root_element in _iter_children(data_elements, "XMLElement"):
        _walk_xml_element(
            root_element,
            parent_logical="",
            parent_key_path=[],
            parent_name_path=[],
            is_root=True,
            field_index=field_index,
            mappable_paths=mappable_paths,
        )


def _walk_xml_element(
    element: Any,
    *,
    parent_logical: str,
    parent_key_path: List[str],
    parent_name_path: List[str],
    is_root: bool,
    field_index: Dict[str, Dict[str, Any]],
    mappable_paths: List[str],
) -> None:
    key = _require_element_key(element, "XMLElement")
    name = element.get("name") or ""
    logical = name if is_root else f"{parent_logical}/{name}"
    key_path = parent_key_path + [f"*[@key='{key}']"]
    name_path = ([name] if is_root else parent_name_path + [name])

    # Only child ELEMENTS make an element structural; attributes never do.
    child_elements = _iter_children(element, "XMLElement")
    attribute_children = _iter_children(element, "XMLAttribute")
    # A repeating element (maxOccurs != 1) pushes a "[]" segment onto its
    # descendants' logical path (mirrors the JSON array convention).
    repeating = str(element.get("maxOccurs", "1")) != "1"
    child_segment = f"{logical}[]" if repeating else logical

    if child_elements:
        _add_profile_index_entry(
            field_index,
            mappable_paths,
            logical_path=logical,
            name=name,
            key=key,
            key_path=key_path,
            name_path=name_path,
            data_type=None,
            kind="element",
            mappable=False,
        )
    else:
        _add_profile_index_entry(
            field_index,
            mappable_paths,
            logical_path=logical,
            name=name,
            key=key,
            key_path=key_path,
            name_path=name_path,
            data_type=element.get("dataType"),
            kind="element",
            mappable=element.get("isMappable") == "true",
        )

    for attribute in attribute_children:
        _index_xml_attribute(
            attribute,
            parent_logical=child_segment,
            parent_key_path=key_path,
            parent_name_path=name_path,
            field_index=field_index,
            mappable_paths=mappable_paths,
        )
    for child in child_elements:
        _walk_xml_element(
            child,
            parent_logical=child_segment,
            parent_key_path=key_path,
            parent_name_path=name_path,
            is_root=False,
            field_index=field_index,
            mappable_paths=mappable_paths,
        )


def _index_xml_attribute(
    attribute: Any,
    *,
    parent_logical: str,
    parent_key_path: List[str],
    parent_name_path: List[str],
    field_index: Dict[str, Dict[str, Any]],
    mappable_paths: List[str],
) -> None:
    key = _require_element_key(attribute, "XMLAttribute")
    name = attribute.get("name") or ""
    _add_profile_index_entry(
        field_index,
        mappable_paths,
        logical_path=f"{parent_logical}/@{name}",
        name=name,
        key=key,
        key_path=parent_key_path + [f"*[@key='{key}']"],
        name_path=parent_name_path + [f"@{name}"],
        data_type=attribute.get("dataType"),
        kind="attribute",
        mappable=attribute.get("isMappable") == "true",
    )


# ---- Database ------------------------------------------------------------


def _db_execution_type(profile_root: Any) -> str:
    """Return the profile's ``executionType`` (``dbread`` / ``dbwrite``).

    Read from ``<ProfileProperties><DatabaseGeneralInfo executionType="...">``;
    defaults to ``dbread`` when absent (the read-profile field-index shape).
    """
    props = _first_child(profile_root, "ProfileProperties")
    if props is not None:
        info = _first_child(props, "DatabaseGeneralInfo")
        if info is not None:
            exec_type = info.get("executionType")
            if isinstance(exec_type, str) and exec_type.strip():
                return exec_type.strip()
    return "dbread"


def _index_db_profile(
    profile_root: Any,
    field_index: Dict[str, Dict[str, Any]],
    mappable_paths: List[str],
) -> None:
    # Mirror the DB build_field_index shapes exactly so a live-indexed DB profile
    # keys IDENTICALLY to a generated one:
    #   * dbread  (DatabaseReadProfileBuilder): result_set columns under DBFields,
    #     keyed by BARE column name.
    #   * dbwrite (DatabaseWriteProfileBuilder): writable columns under DBFields
    #     keyed "Fields/<name>", WHERE keys under DBConditions keyed
    #     "Conditions/<name>" (namespace-prefixed so a column appearing in both
    #     the SET fields and WHERE conditions of a dynamicupdate stays distinct;
    #     a dynamicdelete carries DBConditions only, no DBFields).
    # DBParameters are intentionally not indexed (matching the generated
    # read-profile contract).
    data_elements = _require_data_elements(profile_root)
    statement = _first_child(data_elements, "DBStatement")
    if statement is None:
        raise BuilderValidationError(
            "database profile has no <DBStatement>",
            error_code=PROFILE_INDEX_STRUCTURE_INVALID,
            field="component_xml",
        )
    statement_key = _require_element_key(statement, "DBStatement")
    statement_name = statement.get("name") or "Statement"
    is_write = _db_execution_type(profile_root) == "dbwrite"

    fields = _first_child(statement, "DBFields")
    if fields is not None:
        fields_key = _require_element_key(fields, "DBFields")
        fields_name = fields.get("name") or "Fields"
        for column in _iter_children(fields, "DatabaseElement"):
            column_key = _require_element_key(column, "DatabaseElement")
            column_name = column.get("name") or ""
            logical = f"{fields_name}/{column_name}" if is_write else column_name
            _add_profile_index_entry(
                field_index,
                mappable_paths,
                logical_path=logical,
                name=column_name,
                key=column_key,
                key_path=[
                    f"*[@key='{statement_key}']",
                    f"*[@key='{fields_key}']",
                    f"*[@key='{column_key}']",
                ],
                name_path=[statement_name, fields_name, column_name],
                data_type=column.get("dataType"),
                kind="simple",
                mappable=column.get("isMappable") == "true",
            )

    # WHERE-condition keys (dynamicupdate / dynamicdelete write profiles only).
    conditions = _first_child(statement, "DBConditions")
    if is_write and conditions is not None:
        conditions_key = _require_element_key(conditions, "DBConditions")
        conditions_name = conditions.get("name") or "Conditions"
        for condition in _iter_children(conditions, "DBCondition"):
            condition_key = _require_element_key(condition, "DBCondition")
            condition_name = condition.get("name") or ""
            _add_profile_index_entry(
                field_index,
                mappable_paths,
                logical_path=f"{conditions_name}/{condition_name}",
                name=condition_name,
                key=condition_key,
                key_path=[
                    f"*[@key='{statement_key}']",
                    f"*[@key='{conditions_key}']",
                    f"*[@key='{condition_key}']",
                ],
                name_path=[statement_name, conditions_name, condition_name],
                data_type=condition.get("dataType"),
                kind="simple",
                mappable=condition.get("isMappable") == "true",
            )


# ---------------------------------------------------------------------------
# Issue #95 M7.5 — supplied-index validation
# ---------------------------------------------------------------------------


def validate_supplied_profile_index(
    component_id: Any, entry: Any
) -> Optional[BuilderValidationError]:
    """Validate a caller-supplied ``profile_indexes_by_component_id`` entry.

    Returns ``None`` when ``entry`` is a well-formed index for ``component_id``
    (matching ``component_id``, a supported ``profile_component_type``, and a
    non-empty ``field_index_by_path`` whose every entry carries non-blank
    ``key`` / ``key_path`` / ``name_path`` and a boolean ``mappable``). Returns a
    ``MAP_PROFILE_INDEX_UNAVAILABLE`` error otherwise, so a malformed supplied
    index falls through to live discovery / rejection and never bypasses
    validation.
    """
    def _fail(message: str, detail: Optional[Dict[str, Any]] = None) -> BuilderValidationError:
        return BuilderValidationError(
            message,
            error_code=MAP_PROFILE_INDEX_UNAVAILABLE,
            field="profile_indexes_by_component_id",
            hint=(
                "Each supplied index must be the object returned by "
                "index_profile_component: {component_id, profile_component_type, "
                "field_index_by_path}."
            ),
            details=detail or {"component_id": str(component_id)},
        )

    if not isinstance(entry, Mapping):
        return _fail("supplied profile index must be an object")
    supplied_id = entry.get("component_id")
    if not isinstance(supplied_id, str) or supplied_id.strip() != str(component_id).strip():
        return _fail(
            "supplied profile index component_id does not match its map key",
            {"component_id": str(component_id), "supplied": supplied_id},
        )
    profile_type = entry.get("profile_component_type")
    if profile_type not in PROFILE_INDEX_SUPPORTED_TYPES:
        return _fail(
            f"supplied profile index has unsupported profile_component_type "
            f"{profile_type!r}",
            {"component_id": str(component_id), "profile_component_type": profile_type},
        )
    field_index = entry.get("field_index_by_path")
    if not isinstance(field_index, Mapping) or not field_index:
        return _fail(
            "supplied profile index field_index_by_path must be a non-empty object",
            {"component_id": str(component_id)},
        )
    for path, field_entry in field_index.items():
        if not isinstance(field_entry, Mapping):
            return _fail(
                f"supplied field index entry {path!r} must be an object",
                {"component_id": str(component_id), "path": path},
            )
        for required_key in ("key", "key_path", "name_path"):
            value = field_entry.get(required_key)
            if not isinstance(value, (str, int)) or not str(value).strip():
                return _fail(
                    f"supplied field index entry {path!r} is missing a valid "
                    f"{required_key}",
                    {"component_id": str(component_id), "path": path},
                )
        if not isinstance(field_entry.get("mappable"), bool):
            return _fail(
                f"supplied field index entry {path!r} must declare a boolean "
                "'mappable'",
                {"component_id": str(component_id), "path": path},
            )
    return None

"""Read-only transformation review surface (issue #46, M2.6e).

``review_transformation_action`` lets a caller inspect the source/target fields,
mappings, validation gaps, synthetic skeleton payloads, and expected-vs-actual
payload differences of a ``database_to_api_sync`` transform *before* committing
to ``build_integration(action="apply")``.

Safety contract — enforced on every response:
  * ``read_only=True``, ``boomi_mutation=False``, ``raw_xml_exposed=False``
  * never constructs a Boomi client, never calls ``get_current_user`` /
    ``get_secret``, never reaches the network.
  * never echoes Boomi-derived secrets — raw SQL, raw XML, credentials, or
    script bodies sourced from the spec/components. map_script summaries carry
    ``script_body_present`` (bool) only. ``raw_xml_exposed=False`` means no raw
    Boomi component XML is emitted.

``compare_expected_actual`` is the one exception to value-hiding, and
intentionally so: it diffs two *caller-supplied* payloads
(``config["expected_payload"]`` / ``config["actual_payload"]``) — the tool
never fetches ``actual_payload`` from Boomi — so its difference entries echo the
caller's own values back to the caller (a value diff that hid the mismatched
values would be useless). It surfaces no Boomi-sourced data.

It reuses the existing generated-profile field indexes and the map-function
registry rather than re-implementing Boomi XML logic.

Finding ``code`` values: structural review checks emit ``TRANSFORM_REVIEW_*``
codes (see module constants). Function/script gap checks delegate to the
canonical builder validators (``validate_function_mapping``) and surface those
validators' own machine codes verbatim (e.g. ``MAP_FUNCTION_INPUT_COUNT_MISMATCH``)
so callers get the most specific reason available.
"""

from __future__ import annotations

import json
from dataclasses import dataclass, field
from types import SimpleNamespace
from typing import Any, Dict, List, Mapping, Optional

from pydantic import BaseModel

from .components.builders.connector_builder import BuilderValidationError
from .components.builders.map_builder import _FORBIDDEN_SECRET_FIELDS
from .components.builders.map_function_registry import (
    get_function_family,
    validate_function_mapping,
)
from .components.builders.transform_map_validation import (
    resolve_map_profile_index,
    validate_transform_map,
)
from .components.builders.profile_generation import validate_supplied_profile_index

# ---------------------------------------------------------------------------
# Error / finding codes
# ---------------------------------------------------------------------------

TRANSFORM_REVIEW_INVALID_INPUT = "TRANSFORM_REVIEW_INVALID_INPUT"
TRANSFORM_REVIEW_NO_TRANSFORM_FOUND = "TRANSFORM_REVIEW_NO_TRANSFORM_FOUND"
TRANSFORM_REVIEW_PROFILE_INDEX_UNAVAILABLE = "TRANSFORM_REVIEW_PROFILE_INDEX_UNAVAILABLE"
TRANSFORM_REVIEW_XML_UNSUPPORTED = "TRANSFORM_REVIEW_XML_UNSUPPORTED"
TRANSFORM_REVIEW_UNSUPPORTED_ROUTE = "TRANSFORM_REVIEW_UNSUPPORTED_ROUTE"
TRANSFORM_REVIEW_FIELD_NOT_FOUND = "TRANSFORM_REVIEW_FIELD_NOT_FOUND"
TRANSFORM_REVIEW_FIELD_NOT_MAPPABLE = "TRANSFORM_REVIEW_FIELD_NOT_MAPPABLE"
TRANSFORM_REVIEW_DUPLICATE_TARGET = "TRANSFORM_REVIEW_DUPLICATE_TARGET"
TRANSFORM_REVIEW_REQUIRED_TARGET_UNMAPPED = "TRANSFORM_REVIEW_REQUIRED_TARGET_UNMAPPED"
TRANSFORM_REVIEW_SCRIPT_REF_MISSING = "TRANSFORM_REVIEW_SCRIPT_REF_MISSING"
TRANSFORM_REVIEW_COMPARE_FAILED = "TRANSFORM_REVIEW_COMPARE_FAILED"

# compare_expected_actual difference codes
MISSING_FIELD = "MISSING_FIELD"
EXTRA_FIELD = "EXTRA_FIELD"
TYPE_MISMATCH = "TYPE_MISMATCH"
VALUE_MISMATCH = "VALUE_MISMATCH"

VALID_ACTIONS = (
    "list_fields",
    "validate_unmapped",
    "mapping_diff",
    "generate_test_payload",
    "compare_expected_actual",
)

_PROFILE_TYPES = ("profile.db", "profile.json", "profile.xml")
_KNOWN_ROUTES = ("direct", "map_function", "map_script")

# Deterministic, type-based placeholders for synthetic skeletons. Unknown /
# structural types fall back to None.
_PLACEHOLDERS: Dict[str, Any] = {
    "character": "sample_text",
    "number": 123,
    "datetime": "2026-01-01T00:00:00Z",
    "boolean": True,
}


# ---------------------------------------------------------------------------
# Internal control-flow error
# ---------------------------------------------------------------------------


class _ReviewError(Exception):
    """Short-circuits review with a structured ``_success=False`` envelope."""

    def __init__(
        self,
        code: str,
        message: str,
        *,
        field: Optional[str] = None,
        hint: Optional[str] = None,
        details: Optional[Dict[str, Any]] = None,
    ) -> None:
        super().__init__(message)
        self.code = code
        self.message = message
        self.field = field
        self.hint = hint
        self.details = details


# ---------------------------------------------------------------------------
# Normalized review context
# ---------------------------------------------------------------------------


@dataclass
class _MapUnit:
    """One reviewable map: its source/target field indexes + mappings.

    A contract flow yields exactly one unit (``map_config=None`` — contract
    output is already validated, so there is no map config to re-run the
    builders on). An executable spec yields one unit per ``transform.map``
    component, each carrying the config/depends_on/components_by_key needed to
    delegate validity to the canonical ``validate_transform_map``.
    """

    source_index: Dict[str, Dict[str, Any]]
    target_index: Dict[str, Dict[str, Any]]
    mappings: List[Dict[str, Any]] = field(default_factory=list)
    map_config: Optional[Dict[str, Any]] = None
    depends_on: Any = field(default_factory=list)
    components_by_key: Optional[Dict[str, Any]] = None
    # Issue #95: supplied literal existing-profile UUID indexes (review is
    # offline — it cannot live-discover, only honor supplied indexes).
    literal_indexes: Optional[Dict[str, Dict[str, Any]]] = None


@dataclass
class _Context:
    source_kind: str  # "contract_flow" | "executable_components"
    source_fields: List[Dict[str, Any]]
    target_fields: List[Dict[str, Any]]
    source_index: Dict[str, Dict[str, Any]]
    target_index: Dict[str, Dict[str, Any]]
    mappings: List[Dict[str, Any]] = field(default_factory=list)
    units: List[_MapUnit] = field(default_factory=list)


def _context_from_units(source_kind: str, units: List[_MapUnit]) -> _Context:
    """Aggregate per-map units into a context. Field indexes are unioned by
    path (first wins); mappings are concatenated. Single-unit sources (contract
    flow / single map) aggregate to exactly that unit's data."""
    source_index: Dict[str, Dict[str, Any]] = {}
    target_index: Dict[str, Dict[str, Any]] = {}
    mappings: List[Dict[str, Any]] = []
    for unit in units:
        for path, rec in unit.source_index.items():
            source_index.setdefault(path, rec)
        for path, rec in unit.target_index.items():
            target_index.setdefault(path, rec)
        mappings.extend(unit.mappings)
    return _Context(
        source_kind=source_kind,
        source_fields=list(source_index.values()),
        target_fields=list(target_index.values()),
        source_index=source_index,
        target_index=target_index,
        mappings=mappings,
        units=units,
    )


# ---------------------------------------------------------------------------
# Response envelope helpers
# ---------------------------------------------------------------------------


def _flags(action: str) -> Dict[str, Any]:
    return {
        "action": action,
        "read_only": True,
        "boomi_mutation": False,
        "raw_xml_exposed": False,
    }


def _envelope(action: str, **payload: Any) -> Dict[str, Any]:
    return {"_success": True, **_flags(action), **payload}


def _error_envelope(action: str, err: _ReviewError) -> Dict[str, Any]:
    out: Dict[str, Any] = {
        "_success": False,
        **_flags(action),
        "error": err.message,
        "code": err.code,
    }
    if err.field is not None:
        out["field"] = err.field
    if err.hint is not None:
        out["hint"] = err.hint
    if err.details is not None:
        out["details"] = err.details
    return out


def _issue(
    severity: str,
    code: str,
    field_loc: Optional[str],
    message: str,
    *,
    hint: Optional[str] = None,
    **details: Any,
) -> Dict[str, Any]:
    return {
        "severity": severity,
        "code": code,
        "field": field_loc,
        "message": message,
        "hint": hint,
        "details": details,
    }


def _issue_from_builder_error(err: BuilderValidationError) -> Dict[str, Any]:
    return {
        "severity": "error",
        "code": err.error_code,
        "field": err.field,
        "message": str(err),
        "hint": err.hint,
        "details": err.details or {},
    }


# ---------------------------------------------------------------------------
# Input coercion
# ---------------------------------------------------------------------------


def _coerce_config(config: Any) -> Dict[str, Any]:
    if config is None:
        return {}
    if isinstance(config, str):
        if not config.strip():
            return {}
        try:
            parsed = json.loads(config)
        except (json.JSONDecodeError, TypeError) as exc:
            raise _ReviewError(
                TRANSFORM_REVIEW_INVALID_INPUT,
                f"config must be a valid JSON string: {exc}",
                field="config",
            )
        if not isinstance(parsed, dict):
            raise _ReviewError(
                TRANSFORM_REVIEW_INVALID_INPUT,
                "config must decode to a JSON object",
                field="config",
            )
        return parsed
    if isinstance(config, Mapping):
        return dict(config)
    raise _ReviewError(
        TRANSFORM_REVIEW_INVALID_INPUT,
        "config must be a dict, JSON string, or None",
        field="config",
    )


def _as_dict(value: Any, field_loc: str) -> Dict[str, Any]:
    if isinstance(value, BaseModel):
        return value.model_dump()
    if isinstance(value, Mapping):
        return dict(value)
    raise _ReviewError(
        TRANSFORM_REVIEW_INVALID_INPUT,
        f"{field_loc} must be an object",
        field=field_loc,
    )


def _mapping_or_empty(value: Any) -> Dict[str, Any]:
    """Coerce to a dict, treating any non-Mapping (incl. truthy junk) as empty.

    The contract-flow indexes are optional, so malformed/missing ones degrade
    to "no fields" rather than raising — keeping the never-raise contract.
    """
    return dict(value) if isinstance(value, Mapping) else {}


def _as_list(value: Any) -> List[Any]:
    """Coerce to a list, treating any non-list (incl. truthy junk like True or
    a string) as empty — iterating those would raise a TypeError."""
    return value if isinstance(value, list) else []


# ---------------------------------------------------------------------------
# Field / operation normalization
# ---------------------------------------------------------------------------


def _normalize_field(side: str, raw: Mapping[str, Any]) -> Dict[str, Any]:
    return {
        "side": side,
        "path": raw.get("path"),
        "name": raw.get("name"),
        "data_type": raw.get("data_type"),
        "required": bool(raw.get("required", False)),
        "mappable": bool(raw.get("mappable", False)),
        "kind": raw.get("kind") or "simple",
        "profile_component_type": raw.get("profile_component_type"),
        "source": raw.get("source"),
    }


def _clean_paths(values: Any) -> List[str]:
    out: List[str] = []
    if isinstance(values, list):
        for v in values:
            if isinstance(v, str) and v.strip():
                out.append(v.strip())
    elif isinstance(values, str) and values.strip():
        out.append(values.strip())
    return out


def _mapping_record(
    route: str,
    *,
    source_paths: List[str],
    target_paths: List[str],
    function_type: Optional[str] = None,
    parameters: Optional[Dict[str, Any]] = None,
    script_ref: Optional[str] = None,
    script_body_present: bool = False,
    unsupported: bool = False,
) -> Dict[str, Any]:
    return {
        "route": route,
        "source_paths": source_paths,
        "target_paths": target_paths,
        "function_type": function_type,
        "parameters": parameters,
        "script_ref": script_ref,
        "script_body_present": script_body_present,
        "_unsupported_route": unsupported,
    }


def _normalize_contract_operation(op: Mapping[str, Any]) -> Dict[str, Any]:
    """Normalize a transform-flow operation summary into a mapping record."""
    op_type = op.get("operation_type")
    if op_type == "direct":
        return _mapping_record(
            "direct",
            source_paths=_clean_paths(op.get("source_field")),
            target_paths=_clean_paths(op.get("target_path")),
        )
    if op_type == "map_function":
        params = op.get("parameters")
        return _mapping_record(
            "map_function",
            source_paths=_clean_paths(op.get("inputs")),
            target_paths=_clean_paths(op.get("target_path")),
            function_type=op.get("function_type"),
            parameters=dict(params) if isinstance(params, Mapping) else None,
        )
    if op_type == "map_script":
        # Presence is derived from a genuine, non-blank script_body — NOT the
        # caller's script_body_present summary flag. A spec can claim
        # script_body_present=true without a runnable body; downstream wrapper
        # synthesis still needs an actual body (or a $ref), so review must not
        # green-light a flag-only map_script. (The archetype always emits the
        # body alongside the flag, so real contract output is unaffected.)
        body = op.get("script_body")
        return _mapping_record(
            "map_script",
            source_paths=_clean_paths(op.get("inputs")),
            target_paths=_clean_paths(op.get("outputs")),
            script_ref=op.get("script_component_ref"),
            script_body_present=isinstance(body, str) and bool(body.strip()),
        )
    return _mapping_record(
        str(op_type),
        source_paths=[],
        target_paths=[],
        unsupported=True,
    )


# ---------------------------------------------------------------------------
# Context builders
# ---------------------------------------------------------------------------


def _build_context(config: Mapping[str, Any]) -> _Context:
    spec_raw = config.get("integration_spec")
    if spec_raw is None:
        raise _ReviewError(
            TRANSFORM_REVIEW_INVALID_INPUT,
            "config.integration_spec is required",
            field="integration_spec",
            hint=(
                "Pass the IntegrationSpecV1 produced by build_from_archetype "
                "or build_integration."
            ),
        )
    spec = _as_dict(spec_raw, "integration_spec")

    flow = _find_transform_flow(spec)
    if flow is not None:
        return _context_from_flow(flow)

    components = spec.get("components")
    if not isinstance(components, list):
        components = []
    if _has_transform_map(components):
        return _context_from_components(
            components, _supplied_literal_indexes(spec)
        )

    raise _ReviewError(
        TRANSFORM_REVIEW_NO_TRANSFORM_FOUND,
        "No transform flow or transform.map component found in integration_spec",
        field="integration_spec",
        hint=(
            "Use a database_to_api_sync contract (transform flow) or a spec "
            "whose components include a transform.map referencing generated "
            "profile components."
        ),
    )


def _find_transform_flow(spec: Mapping[str, Any]) -> Optional[Mapping[str, Any]]:
    flows = spec.get("flows")
    if not isinstance(flows, list):
        return None
    for flow in flows:
        if (
            isinstance(flow, Mapping)
            and flow.get("operation") == "transform"
            and "source_profile_generation" in flow
        ):
            return flow
    return None


def _context_from_flow(flow: Mapping[str, Any]) -> _Context:
    src_gen = _mapping_or_empty(flow.get("source_profile_generation"))
    tgt_gen = _mapping_or_empty(flow.get("target_profile_generation"))
    source_index = _index_from_records(
        "source", _mapping_or_empty(src_gen.get("field_index_by_path")).values()
    )
    target_index = _index_from_records(
        "target", _mapping_or_empty(tgt_gen.get("field_index_by_path")).values()
    )

    operations = flow.get("operations")
    mappings = [
        _normalize_contract_operation(op)
        for op in (operations if isinstance(operations, list) else [])
        if isinstance(op, Mapping)
    ]

    # Contract output is already validated upstream and has no map config to
    # re-run the builders on, so this single unit carries map_config=None.
    unit = _MapUnit(
        source_index=source_index, target_index=target_index, mappings=mappings
    )
    return _context_from_units("contract_flow", [unit])


def _index_from_records(side: str, records: Any) -> Dict[str, Dict[str, Any]]:
    index: Dict[str, Dict[str, Any]] = {}
    for rec in records:
        if isinstance(rec, Mapping):
            normalized = _normalize_field(side, rec)
            if normalized["path"]:
                index[normalized["path"]] = normalized
    return index


def _coerce_component(comp: Any) -> Optional[Dict[str, Any]]:
    if isinstance(comp, BaseModel):
        return comp.model_dump()
    if isinstance(comp, Mapping):
        return dict(comp)
    return None


def _comp_view(cd: Mapping[str, Any]) -> SimpleNamespace:
    """Lightweight component view exposing .type/.config/.name/.depends_on for
    the shared transform_map_validation helpers, without constructing (and
    validating) a pydantic IntegrationComponentSpec."""
    cfg = cd.get("config")
    deps = cd.get("depends_on")
    return SimpleNamespace(
        key=cd.get("key"),
        type=cd.get("type"),
        name=cd.get("name"),
        config=cfg if isinstance(cfg, Mapping) else {},
        # Coerce to a list — validate_transform_map does set(depends_on) and a
        # truthy non-list (e.g. a number) would raise. IntegrationComponentSpec
        # guarantees a list, but hand-authored review specs may not.
        depends_on=deps if isinstance(deps, list) else [],
    )


def _has_transform_map(components: Any) -> bool:
    for comp in components or []:
        cd = _coerce_component(comp)
        if cd is not None and cd.get("type") == "transform.map":
            return True
    return False


def _supplied_literal_indexes(
    spec: Mapping[str, Any]
) -> Dict[str, Dict[str, Any]]:
    """Build the literal-UUID index map from a spec's supplied
    ``profile_indexes_by_component_id`` (issue #95).

    Review is offline (no SDK / live discovery), so ONLY caller-supplied indexes
    are honored — mirroring build_integration's supplied-index precedence. A
    malformed supplied entry is dropped (never trusted), so review stays
    consistent with build for well-formed supplied indexes and honestly reports
    unavailable when an index must be live-discovered.
    """
    supplied = spec.get("profile_indexes_by_component_id")
    if not isinstance(supplied, Mapping):
        return {}
    resolved: Dict[str, Dict[str, Any]] = {}
    for uuid, entry in supplied.items():
        if not isinstance(uuid, str):
            continue
        if validate_supplied_profile_index(uuid, entry) is None:
            # {profile_component_type, field_index_by_path} wrapper — same shape
            # build_integration resolves, so review threads the canonical type.
            resolved[uuid.strip()] = {
                "profile_component_type": entry.get("profile_component_type"),
                "field_index_by_path": entry.get("field_index_by_path"),
            }
    return resolved


def _context_from_components(
    components: Any,
    literal_indexes: Optional[Dict[str, Dict[str, Any]]] = None,
) -> _Context:
    literal_indexes = literal_indexes or {}
    views = [
        _comp_view(cd)
        for cd in (_coerce_component(c) for c in components)
        if cd is not None
    ]
    components_by_key = {v.key: v for v in views if isinstance(v.key, str)}
    map_views = [v for v in views if v.type == "transform.map"]
    if not map_views:  # pragma: no cover - guarded by _has_transform_map
        raise _ReviewError(
            TRANSFORM_REVIEW_NO_TRANSFORM_FOUND,
            "No transform.map component found in integration_spec.components",
            field="components",
        )

    units: List[_MapUnit] = []
    for mv in map_views:
        # Inject the component-name fallback the builders expect, mirroring
        # _execute_component / resolve_map_profile_index.
        effective = dict(_mapping_or_empty(mv.config))
        if mv.name and not effective.get("component_name"):
            effective["component_name"] = mv.name

        if "xml" in effective:
            raise _ReviewError(
                TRANSFORM_REVIEW_XML_UNSUPPORTED,
                "transform.map uses the raw-XML escape hatch; review is unsupported",
                field="components",
                hint="Raw map XML import is future work (#48).",
            )

        source_index = _resolve_side_index(
            effective, "source", components_by_key, literal_indexes
        )
        target_index = _resolve_side_index(
            effective, "target", components_by_key, literal_indexes
        )
        units.append(
            _MapUnit(
                source_index=source_index,
                target_index=target_index,
                mappings=_mappings_from_map_config(effective),
                map_config=effective,
                depends_on=mv.depends_on,
                components_by_key=components_by_key,
                literal_indexes=literal_indexes,
            )
        )

    return _context_from_units("executable_components", units)


def _resolve_side_index(
    effective_config: Mapping[str, Any],
    side: str,
    components_by_key: Mapping[str, Any],
    literal_indexes: Optional[Dict[str, Dict[str, Any]]] = None,
) -> Dict[str, Dict[str, Any]]:
    """Resolve one side's normalized field index via the same generated-profile
    logic build_integration uses (resolve_map_profile_index). Raw-XML profiles
    and unresolvable refs (missing $ref / non-profile / literal UUID with no
    supplied index) surface as 'review unavailable', never silent emptiness.
    A literal existing-profile UUID resolves when ``literal_indexes`` supplies it
    (issue #95)."""
    ref = effective_config.get(f"{side}_profile_id")
    ref_key = (
        ref[len("$ref:"):]
        if isinstance(ref, str) and ref.startswith("$ref:")
        else None
    )
    profile = components_by_key.get(ref_key) if ref_key is not None else None

    if profile is not None and "xml" in (profile.config or {}):
        raise _ReviewError(
            TRANSFORM_REVIEW_XML_UNSUPPORTED,
            f"{side} profile uses the raw-XML escape hatch; review is unsupported",
            field=f"{side}_profile_id",
            details={"side": side},
        )

    index = resolve_map_profile_index(ref, components_by_key, literal_indexes)
    if index is None:
        raise _ReviewError(
            TRANSFORM_REVIEW_PROFILE_INDEX_UNAVAILABLE,
            f"{side}_profile_id could not be indexed — it is a literal "
            "existing-profile id with no supplied index, a missing $ref, or a "
            "non-profile component",
            field=f"{side}_profile_id",
            hint=(
                "For a literal existing-profile UUID, index it with "
                "index_profile_component and supply it via "
                "profile_indexes_by_component_id; review is offline and cannot "
                "live-discover."
            ),
            details={"side": side},
        )

    ptype = profile.type if profile is not None else None
    annotated: List[Dict[str, Any]] = []
    for rec in index.values():
        merged = dict(rec)
        merged.setdefault("profile_component_type", ptype)
        merged.setdefault("source", "executable_component")
        annotated.append(merged)
    return _index_from_records(side, annotated)


def _mappings_from_map_config(map_config: Mapping[str, Any]) -> List[Dict[str, Any]]:
    """Normalize a transform.map's mapping lists into review records (for
    list_fields / mapping_diff / coverage). Map-config VALIDITY is delegated to
    validate_transform_map, so this only parses shapes and never gates on
    map_type."""
    mappings: List[Dict[str, Any]] = []

    for fm in _as_list(map_config.get("field_mappings")):
        if isinstance(fm, Mapping):
            mappings.append(
                _mapping_record(
                    "direct",
                    source_paths=_clean_paths(fm.get("source_path")),
                    target_paths=_clean_paths(fm.get("target_path")),
                )
            )

    for fm in _as_list(map_config.get("function_mappings")):
        if isinstance(fm, Mapping):
            params = fm.get("parameters")
            mappings.append(
                _mapping_record(
                    "map_function",
                    source_paths=_clean_paths(fm.get("inputs")),
                    target_paths=_clean_paths(fm.get("target_path")),
                    function_type=fm.get("function_type"),
                    parameters=dict(params) if isinstance(params, Mapping) else None,
                )
            )

    for sm in _as_list(map_config.get("script_mappings")):
        if isinstance(sm, Mapping):
            mappings.append(
                _mapping_record(
                    "map_script",
                    source_paths=_clean_paths(
                        [e.get("source_path") for e in _as_list(sm.get("inputs")) if isinstance(e, Mapping)]
                    ),
                    target_paths=_clean_paths(
                        [e.get("target_path") for e in _as_list(sm.get("outputs")) if isinstance(e, Mapping)]
                    ),
                    script_ref=sm.get("script_component_id"),
                )
            )

    return mappings


# ---------------------------------------------------------------------------
# Actions
# ---------------------------------------------------------------------------


def _action_list_fields(action: str, ctx: _Context) -> Dict[str, Any]:
    required_target_count = sum(
        1 for f in ctx.target_fields if f["required"] and f["mappable"]
    )
    return _envelope(
        action,
        source_kind=ctx.source_kind,
        source_fields=ctx.source_fields,
        target_fields=ctx.target_fields,
        field_count=len(ctx.source_fields) + len(ctx.target_fields),
        required_target_count=required_target_count,
    )


def _action_validate_unmapped(action: str, ctx: _Context) -> Dict[str, Any]:
    issues: List[Dict[str, Any]] = []
    mapped_all: set = set()
    unmapped_required_all: set = set()
    for unit in ctx.units:
        unit_issues, unit_mapped, unit_unmapped = _validate_unit(unit)
        issues.extend(unit_issues)
        mapped_all |= unit_mapped
        unmapped_required_all.update(unit_unmapped)

    valid = not any(i["severity"] == "error" for i in issues)
    return _envelope(
        action,
        valid=valid,
        issue_count=len(issues),
        issues=issues,
        mapped_target_paths=sorted(mapped_all),
        unmapped_required_target_paths=sorted(unmapped_required_all),
    )


def _validate_unit(unit: _MapUnit):
    """Validate one map unit -> (issues, mapped_target_paths, unmapped_required)."""
    if unit.map_config is not None:
        # Source B: delegate map-config validity to the canonical builder so the
        # review verdict matches build_integration exactly.
        issues, mapped = _validate_executable_unit(unit)
    else:
        # Source A (contract flow): pre-validated output, but re-run the
        # lightweight field/route/function/script checks defensively.
        issues, mapped = _validate_contract_unit(unit)

    # Coverage (both sources) — the tool's unique value-add over the builders:
    # required, mappable target leaves left without any mapping.
    unmapped_required = sorted(
        path
        for path, rec in unit.target_index.items()
        if rec["mappable"] and rec["required"] and path not in mapped
    )
    for path in unmapped_required:
        issues.append(
            _issue(
                "error",
                TRANSFORM_REVIEW_REQUIRED_TARGET_UNMAPPED,
                "target_path",
                f"Required target leaf {path!r} has no mapping",
                path=path,
            )
        )
    return issues, mapped, unmapped_required


def _mapped_targets(unit: _MapUnit) -> set:
    mapped: set = set()
    for m in unit.mappings:
        for tp in m["target_paths"]:
            rec = unit.target_index.get(tp)
            if rec is not None and rec["mappable"]:
                mapped.add(tp)
    return mapped


def _validate_executable_unit(unit: _MapUnit):
    issues: List[Dict[str, Any]] = []
    err = validate_transform_map(
        unit.map_config,
        unit.depends_on,
        unit.components_by_key,
        literal_indexes=unit.literal_indexes,
    )
    if err is not None:
        issues.append(_issue_from_builder_error(err))
    return issues, _mapped_targets(unit)


def _validate_contract_unit(unit: _MapUnit):
    issues: List[Dict[str, Any]] = []
    mapped: set = set()
    target_binding_count: Dict[str, int] = {}

    for m in unit.mappings:
        route = m["route"]
        if m.get("_unsupported_route") or route not in _KNOWN_ROUTES:
            issues.append(
                _issue(
                    "error",
                    TRANSFORM_REVIEW_UNSUPPORTED_ROUTE,
                    "route",
                    f"Unsupported transform route {route!r}",
                    hint="Supported routes: direct, map_function, map_script.",
                    route=route,
                )
            )
            continue

        for sp in m["source_paths"]:
            entry = unit.source_index.get(sp)
            if entry is None:
                issues.append(
                    _issue(
                        "error",
                        TRANSFORM_REVIEW_FIELD_NOT_FOUND,
                        "source_path",
                        f"Source field {sp!r} is not declared in the source profile",
                        path=sp,
                        side="source",
                    )
                )
            elif not entry["mappable"]:
                issues.append(
                    _issue(
                        "error",
                        TRANSFORM_REVIEW_FIELD_NOT_MAPPABLE,
                        "source_path",
                        f"Source path {sp!r} resolves to a non-mappable structural node",
                        path=sp,
                        side="source",
                    )
                )

        for tp in m["target_paths"]:
            target_binding_count[tp] = target_binding_count.get(tp, 0) + 1
            entry = unit.target_index.get(tp)
            if entry is None:
                issues.append(
                    _issue(
                        "error",
                        TRANSFORM_REVIEW_FIELD_NOT_FOUND,
                        "target_path",
                        f"Target path {tp!r} is not declared in the target profile",
                        path=tp,
                        side="target",
                    )
                )
            elif not entry["mappable"]:
                issues.append(
                    _issue(
                        "error",
                        TRANSFORM_REVIEW_FIELD_NOT_MAPPABLE,
                        "target_path",
                        f"Target path {tp!r} resolves to a non-mappable structural node",
                        path=tp,
                        side="target",
                    )
                )
            else:
                mapped.add(tp)

        if route == "map_function":
            issues.extend(_validate_function_route(m))
        elif route == "map_script" and not (
            (isinstance(m.get("script_ref"), str) and m["script_ref"].strip())
            or m.get("script_body_present")
        ):
            # A contract-flow map_script is complete with EITHER a $ref to a
            # script component OR an inline script_body (the archetype emits
            # script_body without script_component_ref for downstream wrapper
            # synthesis — see database_to_api_sync.py). Only flag when neither
            # is present. Executable-spec maps go through validate_transform_map,
            # which keeps the stricter $ref requirement.
            issues.append(
                _issue(
                    "error",
                    TRANSFORM_REVIEW_SCRIPT_REF_MISSING,
                    "script_ref",
                    "map_script mapping has neither a script reference nor an "
                    "inline script body",
                    hint=(
                        "Provide an inline script_body, or reference a script "
                        "component via script_component_ref ('$ref:<script_key>')."
                    ),
                )
            )

    for tp, count in target_binding_count.items():
        entry = unit.target_index.get(tp)
        if count > 1 and entry is not None and entry["mappable"]:
            issues.append(
                _issue(
                    "error",
                    TRANSFORM_REVIEW_DUPLICATE_TARGET,
                    "target_path",
                    f"Target path {tp!r} is bound by {count} mappings",
                    path=tp,
                    binding_count=count,
                )
            )

    return issues, mapped


def _validate_function_route(m: Mapping[str, Any]) -> List[Dict[str, Any]]:
    function_type = m.get("function_type")
    family = get_function_family(function_type or "")
    if family is None:
        return [
            _issue(
                "error",
                TRANSFORM_REVIEW_UNSUPPORTED_ROUTE,
                "function_type",
                f"Unknown map function_type {function_type!r}",
                function_type=function_type,
            )
        ]
    err = validate_function_mapping(
        family,
        inputs=list(m["source_paths"]),
        parameters=m.get("parameters") or {},
        field_prefix="function_mappings",
    )
    if err is not None:
        return [_issue_from_builder_error(err)]
    return []


def _key_is_secret_shaped(key: Any) -> bool:
    """True when a parameter key looks credential-bearing. Case-insensitive
    substring match against the canonical forbidden tokens, with '-'/' '
    normalized to '_' so variants like API_KEY, db_password, AUTH-TOKEN, and
    x-api-key are all caught."""
    if not isinstance(key, str):
        return False
    norm = key.lower().replace("-", "_").replace(" ", "_")
    return any(token in norm for token in _FORBIDDEN_SECRET_FIELDS)


def _redact_parameters(params: Any) -> Any:
    """Mask secret-shaped map-function parameter values before echoing them in
    a mapping diff (no-credential-echo contract). Walks nested dicts/lists and
    returns NEW structures (the caller's spec is never mutated).

    Validation verdicts mirror the builders exactly (validate_transform_map),
    but this is read-only diff OUTPUT — masking is purely defensive, so it
    matches credential-shaped keys MORE aggressively (case-insensitive,
    substring, separator-normalized) than the builders' exact-key reject scan.
    Over-masking a display value is harmless; echoing a credential is not."""
    if isinstance(params, Mapping):
        return {
            key: ("[REDACTED]" if _key_is_secret_shaped(key) else _redact_parameters(value))
            for key, value in params.items()
        }
    if isinstance(params, list):
        return [_redact_parameters(item) for item in params]
    return params


def _comparable(m: Mapping[str, Any]) -> Dict[str, Any]:
    target_paths = sorted(m["target_paths"])
    discriminator = m.get("function_type") or m.get("script_ref") or ""
    identity = f"{m['route']}|{','.join(target_paths)}|{discriminator}"
    return {
        "identity": identity,
        "route": m["route"],
        "target_paths": target_paths,
        "source_paths": list(m["source_paths"]),
        "function_type": m.get("function_type"),
        "parameters": _redact_parameters(m.get("parameters")),
        "script_ref": m.get("script_ref"),
        "script_body_present": m.get("script_body_present", False),
    }


def _comparable_body(entry: Mapping[str, Any]) -> Dict[str, Any]:
    return {k: v for k, v in entry.items() if k != "identity"}


def _action_mapping_diff(
    action: str, ctx: _Context, config: Mapping[str, Any]
) -> Dict[str, Any]:
    current = [
        _comparable(m) for m in ctx.mappings if not m.get("_unsupported_route")
    ]
    current.sort(key=lambda c: c["identity"])

    previous_spec = config.get("previous_spec")
    if previous_spec is None:
        return _envelope(
            action,
            comparison_available=False,
            current_mappings=current,
            added=[],
            removed=[],
            changed=[],
            unchanged=[],
        )

    try:
        prev_ctx = _build_context({"integration_spec": previous_spec})
    except _ReviewError as exc:
        raise _ReviewError(
            exc.code,
            f"previous_spec could not be reviewed: {exc.message}",
            field="previous_spec",
            hint=exc.hint,
            details=exc.details,
        )

    previous = [
        _comparable(m) for m in prev_ctx.mappings if not m.get("_unsupported_route")
    ]

    cur_by_id = {c["identity"]: c for c in current}
    prev_by_id = {c["identity"]: c for c in previous}

    added = [c for ident, c in cur_by_id.items() if ident not in prev_by_id]
    removed = [c for ident, c in prev_by_id.items() if ident not in cur_by_id]
    changed: List[Dict[str, Any]] = []
    unchanged: List[Dict[str, Any]] = []
    for ident, cur in cur_by_id.items():
        if ident not in prev_by_id:
            continue
        prev = prev_by_id[ident]
        if _comparable_body(cur) == _comparable_body(prev):
            unchanged.append(cur)
        else:
            changed.append({"identity": ident, "previous": prev, "current": cur})

    added.sort(key=lambda c: c["identity"])
    removed.sort(key=lambda c: c["identity"])
    changed.sort(key=lambda c: c["identity"])
    unchanged.sort(key=lambda c: c["identity"])

    return _envelope(
        action,
        comparison_available=True,
        current_mappings=current,
        added=added,
        removed=removed,
        changed=changed,
        unchanged=unchanged,
    )


def _placeholder(data_type: Any) -> Any:
    return _PLACEHOLDERS.get(data_type)


def _insert_skeleton(root: Dict[str, Any], path: str, value: Any) -> None:
    segments = path.split("/")
    cur = root
    for i, seg in enumerate(segments):
        is_array = seg.endswith("[]")
        name = seg[:-2] if is_array else seg
        if not name:
            return
        last = i == len(segments) - 1
        if last:
            cur[name] = [value] if is_array else value
            return
        if is_array:
            existing = cur.get(name)
            if not (isinstance(existing, list) and existing and isinstance(existing[0], dict)):
                cur[name] = [{}]
            cur = cur[name][0]
        else:
            existing = cur.get(name)
            if not isinstance(existing, dict):
                cur[name] = {}
            cur = cur[name]


def _skeleton_from_index(index: Mapping[str, Mapping[str, Any]]) -> Dict[str, Any]:
    skeleton: Dict[str, Any] = {}
    for path, rec in index.items():
        # ``mappable`` is True only for scalar value leaves across every profile
        # type — JSON kind='simple', DB fields, and XML kind='element' — while
        # structural object/array/complex-element nodes are non-mappable.
        if rec.get("mappable"):
            _insert_skeleton(skeleton, path, _placeholder(rec.get("data_type")))
    return skeleton


def _action_generate_test_payload(action: str, ctx: _Context) -> Dict[str, Any]:
    return _envelope(
        action,
        source_payload_skeleton=_skeleton_from_index(ctx.source_index),
        target_payload_skeleton=_skeleton_from_index(ctx.target_index),
        notes=[
            "Synthetic, type-based placeholders only — no live data, SQL, or "
            "script output.",
            "Arrays show a single representative element.",
        ],
    )


# ---------------------------------------------------------------------------
# compare_expected_actual
# ---------------------------------------------------------------------------


def _type_name(value: Any) -> str:
    if isinstance(value, bool):
        return "boolean"
    if value is None:
        return "null"
    if isinstance(value, dict):
        return "object"
    if isinstance(value, list):
        return "array"
    if isinstance(value, (int, float)):
        return "number"
    if isinstance(value, str):
        return "string"
    return type(value).__name__


def _diff(
    expected: Any,
    actual: Any,
    path: str,
    *,
    ignored: set,
    allow_extra: bool,
    strict_types: bool,
    diffs: List[Dict[str, Any]],
) -> None:
    if path and path in ignored:
        return

    et = _type_name(expected)
    at = _type_name(actual)

    if et == "object" and at == "object":
        for key, exp_val in expected.items():
            child = f"{path}/{key}" if path else key
            if child in ignored:
                continue
            if key not in actual:
                diffs.append({"code": MISSING_FIELD, "path": child, "expected": exp_val})
            else:
                _diff(
                    exp_val,
                    actual[key],
                    child,
                    ignored=ignored,
                    allow_extra=allow_extra,
                    strict_types=strict_types,
                    diffs=diffs,
                )
        if not allow_extra:
            for key, act_val in actual.items():
                if key in expected:
                    continue
                child = f"{path}/{key}" if path else key
                if child in ignored:
                    continue
                diffs.append({"code": EXTRA_FIELD, "path": child, "actual": act_val})
        return

    if et == "array" and at == "array":
        for i in range(max(len(expected), len(actual))):
            child = f"{path}[{i}]"
            if child in ignored:
                continue
            if i >= len(actual):
                diffs.append({"code": MISSING_FIELD, "path": child, "expected": expected[i]})
            elif i >= len(expected):
                if not allow_extra:
                    diffs.append({"code": EXTRA_FIELD, "path": child, "actual": actual[i]})
            else:
                _diff(
                    expected[i],
                    actual[i],
                    child,
                    ignored=ignored,
                    allow_extra=allow_extra,
                    strict_types=strict_types,
                    diffs=diffs,
                )
        return

    if et != at:
        if et in ("object", "array") or at in ("object", "array"):
            diffs.append(
                {
                    "code": TYPE_MISMATCH,
                    "path": path,
                    "expected_type": et,
                    "actual_type": at,
                    "expected": expected,
                    "actual": actual,
                }
            )
            return
        if strict_types:
            diffs.append(
                {
                    "code": TYPE_MISMATCH,
                    "path": path,
                    "expected_type": et,
                    "actual_type": at,
                    "expected": expected,
                    "actual": actual,
                }
            )
            return
        if str(expected) != str(actual):
            diffs.append(
                {"code": VALUE_MISMATCH, "path": path, "expected": expected, "actual": actual}
            )
        return

    if expected != actual:
        diffs.append(
            {"code": VALUE_MISMATCH, "path": path, "expected": expected, "actual": actual}
        )


def _action_compare_expected_actual(
    action: str, config: Mapping[str, Any]
) -> Dict[str, Any]:
    if "expected_payload" not in config or "actual_payload" not in config:
        raise _ReviewError(
            TRANSFORM_REVIEW_COMPARE_FAILED,
            "expected_payload and actual_payload are required",
            field="config",
        )
    expected = config.get("expected_payload")
    actual = config.get("actual_payload")

    ignored_raw = config.get("ignored_paths") or []
    if not isinstance(ignored_raw, list):
        raise _ReviewError(
            TRANSFORM_REVIEW_COMPARE_FAILED,
            "ignored_paths must be a list of path strings",
            field="ignored_paths",
        )
    ignored = {p for p in ignored_raw if isinstance(p, str)}
    allow_extra = bool(config.get("allow_extra", False))
    strict_types = bool(config.get("strict_types", True))

    diffs: List[Dict[str, Any]] = []
    _diff(
        expected,
        actual,
        "",
        ignored=ignored,
        allow_extra=allow_extra,
        strict_types=strict_types,
        diffs=diffs,
    )

    return _envelope(
        action,
        match=len(diffs) == 0,
        difference_count=len(diffs),
        differences=diffs,
        allow_extra=allow_extra,
        strict_types=strict_types,
        ignored_paths=sorted(ignored),
    )


# ---------------------------------------------------------------------------
# Public entrypoint
# ---------------------------------------------------------------------------


def review_transformation_action(
    action: str, config: Any = None
) -> Dict[str, Any]:
    """Dispatch a read-only transformation review action.

    Args:
        action: one of ``VALID_ACTIONS``.
        config: dict, JSON string, or None (action-specific payload).
    """
    try:
        cfg = _coerce_config(config)
    except _ReviewError as exc:
        return _error_envelope(action, exc)

    if action not in VALID_ACTIONS:
        return {
            "_success": False,
            **_flags(action),
            "error": f"Unknown action {action!r}",
            "code": TRANSFORM_REVIEW_INVALID_INPUT,
            "valid_actions": list(VALID_ACTIONS),
        }

    try:
        if action == "compare_expected_actual":
            return _action_compare_expected_actual(action, cfg)

        ctx = _build_context(cfg)
        if action == "list_fields":
            return _action_list_fields(action, ctx)
        if action == "validate_unmapped":
            return _action_validate_unmapped(action, ctx)
        if action == "mapping_diff":
            return _action_mapping_diff(action, ctx, cfg)
        if action == "generate_test_payload":
            return _action_generate_test_payload(action, ctx)
    except _ReviewError as exc:
        return _error_envelope(action, exc)

    # Unreachable — action membership is checked above.
    return {
        "_success": False,
        **_flags(action),
        "error": f"Unhandled action {action!r}",
        "code": TRANSFORM_REVIEW_INVALID_INPUT,
    }

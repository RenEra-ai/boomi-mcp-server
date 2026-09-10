"""Shared, Boomi-free transform.map validation.

Extracted from ``integration_builder._build_plan`` so the plan path AND the
read-only ``review_transformation`` surface validate transform.map components
through one source of truth (no drift between what build rejects and what
review flags). Pure: no SDK, no network, no credential access.

``components_by_key`` values only need ``.type`` / ``.config`` / ``.name``
attribute access, so callers may pass ``IntegrationComponentSpec`` instances or
any lightweight object exposing those attributes.
"""

from typing import Any, Dict, List, Mapping, Optional, Tuple

from .connector_builder import BuilderValidationError
from .json_profile_builder import JSONGeneratedProfileBuilder
from .map_builder import get_map_builder, validate_document_cache_joins_structure
from .map_function_registry import get_function_family
from .profile_builder import DatabaseReadProfileBuilder, get_profile_builder
from .profile_generation import (
    MAP_DOCUMENT_CACHE_JOINS_INVALID,
    MAP_FUNCTION_COMPONENT_REF_REQUIRED,
)
from .xml_profile_builder import XMLGeneratedProfileBuilder

#: Issue #157 (M12.19): the ONE required-target-leaf coverage diagnostic. Owned
#: by the advisory review surface (#46) since it was introduced there; now ALSO
#: the hard gate every authoring entry point raises, so the same identity means
#: the same fact on every surface. `transformation_review` imports it from here.
TRANSFORM_REVIEW_REQUIRED_TARGET_UNMAPPED = "TRANSFORM_REVIEW_REQUIRED_TARGET_UNMAPPED"


def resolve_map_profile_index(
    profile_id: Any,
    components_by_key: Optional[Dict[str, Any]],
    literal_indexes: Optional[Dict[str, Dict[str, Any]]] = None,
    selected_indexes: Optional[Dict[str, Dict[str, Any]]] = None,
) -> Optional[Dict[str, Dict[str, Any]]]:
    """Resolve the field index for a transform.map's source / target profile
    reference.

    For a ``$ref:KEY`` reference the index is built from the in-spec profile
    component. For a LITERAL existing-profile UUID (issue #95 M7.5), the index
    is looked up in ``literal_indexes`` (uuid -> field_index_by_path), which the
    caller resolves from a supplied ``profile_indexes_by_component_id`` or live
    read-only discovery. Returns None when no index is available — the map
    validator then raises MAP_PROFILE_INDEX_UNAVAILABLE.

    ``selected_indexes`` (issue #157) is keyed by component KEY and carries the
    index of the artifact that was actually SELECTED for a ``$ref:KEY`` profile
    that apply will reuse rather than build — a ``reference_only`` reuse, or a
    name collision resolved to reuse. For such a profile the in-spec config is
    candidate material, not evidence of what the reused component contains, so
    it is never indexed: the selected index is used when supplied and the
    reference is unavailable otherwise."""
    if not isinstance(profile_id, str) or not profile_id.strip():
        return None
    # Test $ref on the RAW value (do not pre-strip): the depends_on coverage
    # check and _resolve_dependency_tokens also test the unstripped value, so a
    # whitespace-padded " $ref:..." must be treated as a literal here too (it
    # resolves to None -> MAP_PROFILE_INDEX_UNAVAILABLE) rather than silently
    # resolving in one place and staying an unresolved literal everywhere else.
    if not profile_id.startswith("$ref:"):
        # Literal existing-profile UUID — resolvable only from a supplied /
        # discovered index (issue #95). Each literal_indexes value is a
        # {profile_component_type, field_index_by_path} wrapper; return the field
        # map. Unknown UUID -> None -> unavailable.
        if literal_indexes:
            wrapper = literal_indexes.get(profile_id.strip())
            if isinstance(wrapper, Mapping):
                return wrapper.get("field_index_by_path")
        return None
    if components_by_key is None:
        return None
    ref_key = profile_id[len("$ref:") :]
    target_comp = components_by_key.get(ref_key)
    if target_comp is None:
        return None
    raw_config = target_comp.config or {}
    if selected_indexes is not None and ref_key in selected_indexes:
        selected = selected_indexes[ref_key]
        return selected if isinstance(selected, Mapping) else None
    if isinstance(raw_config, Mapping) and raw_config.get("reference_only") is True:
        # A reused profile's in-spec config establishes nothing about the
        # component apply will actually bind; without the selected artifact's
        # own index the reference is unavailable (issue #157).
        return None
    builder_cls = None
    if target_comp.type == "profile.json":
        builder_cls = JSONGeneratedProfileBuilder
    elif target_comp.type == "profile.xml":
        builder_cls = XMLGeneratedProfileBuilder
    elif target_comp.type == "profile.db":
        # Resolve the profile.db builder by profile_type so write profiles
        # (database.write) index as map targets, not only read profiles. When
        # the profile_type is unknown/missing, fall back to the read-profile
        # field index — the pre-#32 behavior (the read base's build_field_index
        # is shape-agnostic over output_fields).
        profile_type = raw_config.get("profile_type")
        profile_type = profile_type.lower() if isinstance(profile_type, str) else ""
        builder_instance = get_profile_builder("profile.db", profile_type)
        builder_cls = (
            type(builder_instance)
            if builder_instance is not None
            else DatabaseReadProfileBuilder
        )
    if builder_cls is None:
        return None
    # Mirror _execute_component's comp.name → component_name fallback. A
    # profile that supplies only the top-level component name (and omits
    # config.component_name) is valid — without the same injection here the
    # validate_config inside build_field_index would fail with
    # "component_name is required" and the map would erroneously surface
    # MAP_PROFILE_INDEX_UNAVAILABLE. Codex r1 P2 finding #2.
    effective_config = dict(raw_config)
    if target_comp.name and not effective_config.get("component_name"):
        effective_config["component_name"] = target_comp.name
    try:
        # DatabaseReadProfileBuilder.build_field_index doesn't run a
        # validate_config gate, so a malformed DB profile here would still
        # raise — caller treats the None return as "no index available".
        return builder_cls.build_field_index(effective_config)
    except BuilderValidationError:
        return None
    except Exception:
        # Defense-in-depth: an unexpected error in index-building shouldn't
        # crash the plan loop. Map validation surfaces MAP_PROFILE_INDEX_UNAVAILABLE.
        return None


def _literal_index_profile_type(
    literal_indexes: Optional[Dict[str, Dict[str, Any]]], profile_id: Any
) -> Optional[str]:
    """Return the CANONICAL ``profile_component_type`` carried alongside a
    resolved literal-UUID index (issue #95) — the validated outer type, looked
    up by UUID, never re-derived from a per-field stamp."""
    if not literal_indexes or not isinstance(profile_id, str):
        return None
    wrapper = literal_indexes.get(profile_id.strip())
    if isinstance(wrapper, Mapping):
        ptype = wrapper.get("profile_component_type")
        if isinstance(ptype, str) and ptype:
            return ptype
    return None


def _ref_target_input_names(target_comp: Any) -> List[str]:
    """Return the ordered list of input names exposed by the referenced
    component's external port surface."""
    cfg = target_comp.config or {}
    return [
        str(entry.get("name") or "").strip()
        for entry in (cfg.get("inputs") or [])
        if isinstance(entry, Mapping) and entry.get("name")
    ]


def _ref_target_output_names(target_comp: Any) -> List[str]:
    """Return the ordered list of output names exposed by the referenced
    component's external port surface."""
    cfg = target_comp.config or {}
    return [
        str(entry.get("name") or "").strip()
        for entry in (cfg.get("outputs") or [])
        if isinstance(entry, Mapping) and entry.get("name")
    ]


def _check_port_shape_alignment(
    *,
    sm_idx: int,
    ref_key: str,
    target_type: str,
    expected_inputs: List[str],
    actual_inputs: List[str],
    expected_outputs: List[str],
    actual_outputs: List[str],
) -> Optional[BuilderValidationError]:
    """Return a structured error if the map's script_mappings entry port
    shape diverges from the referenced component's declarations.

    Required: ordered list equality across both ``inputs`` and ``outputs``
    (count + names + order). Boomi binds the calling map's FunctionStep
    input/output ports to the wrapper's external ports by numeric ``key``
    (position), not by name — port names are for editor display only. A
    reordered list with the same name set would emit syntactically valid XML
    but misroute values at runtime, so set membership alone is insufficient.
    """
    field_prefix = f"script_mappings[{sm_idx}]"

    if actual_inputs != expected_inputs:
        return BuilderValidationError(
            f"{field_prefix}.inputs port order / names do not match the "
            f"referenced {target_type} component {ref_key!r}; map XML "
            "wires ports positionally by key, so the declared order "
            "must mirror the referenced component's external inputs",
            error_code="SCRIPT_MAPPING_VARIABLE_INVALID",
            field=f"{field_prefix}.inputs",
            hint=(
                f"Required ordered input port names: "
                + (", ".join(expected_inputs) or "(none)")
                + ". Reorder script_mappings inputs[] to match — Boomi "
                "binds map FunctionStep input port at key=1 to the "
                "referenced component's external port at key=1, key=2 "
                "to key=2, and so on."
            ),
            details={
                "script_mappings_index": sm_idx,
                "ref_key": ref_key,
                "expected_inputs": expected_inputs,
                "actual_inputs": actual_inputs,
            },
        )

    if actual_outputs != expected_outputs:
        return BuilderValidationError(
            f"{field_prefix}.outputs port order / names do not match the "
            f"referenced {target_type} component {ref_key!r}; map XML "
            "wires ports positionally by key, so the declared order "
            "must mirror the referenced component's external outputs",
            error_code="SCRIPT_MAPPING_VARIABLE_INVALID",
            field=f"{field_prefix}.outputs",
            hint=(
                f"Required ordered output port names: "
                + (", ".join(expected_outputs) or "(none)")
                + ". Reorder script_mappings outputs[] to match."
            ),
            details={
                "script_mappings_index": sm_idx,
                "ref_key": ref_key,
                "expected_outputs": expected_outputs,
                "actual_outputs": actual_outputs,
            },
        )

    return None


def normalized_map_destinations(
    map_config: Mapping[str, Any],
) -> Tuple[Tuple[str, str], ...]:
    """Every destination a transform.map config binds, as ``(route, target_path)``.

    The ONE interpretation of "what does this map write to" shared by the
    coverage gate below and by ``review_transformation``'s mapping records:
    direct ``field_mappings[].target_path``, ``function_mappings[].target_path``
    and every ``script_mappings[].outputs[].target_path``. Order is authored
    order; malformed entries are skipped here because map-config VALIDITY is
    the map builder's job, not this reader's.
    """
    destinations: List[Tuple[str, str]] = []
    for route, key in _DESTINATION_ROUTES:
        for entry in mapping_entries(map_config.get(key)):
            if isinstance(entry, Mapping):
                for path in destination_paths(route, entry):
                    destinations.append((route, path))
    return tuple(destinations)


#: ``(route, map-config key)`` for every mapping list a transform.map may carry.
_DESTINATION_ROUTES: Tuple[Tuple[str, str], ...] = (
    ("direct", "field_mappings"),
    ("map_function", "function_mappings"),
    ("map_script", "script_mappings"),
)


def mapping_entries(value: Any) -> Tuple[Any, ...]:
    """A mapping list's entries; anything that is not a list is empty.

    ``value or ()`` was NOT this: a truthy non-list — ``field_mappings: true``,
    ``outputs: true`` — reached ``for entry in True`` and left a TypeError
    escaping the hard coverage gate on every route that raises it. The advisory
    route already read these lists this way; consolidating the reading is what
    surfaced the difference. Public because the derived-flows projection reads
    the same lists and carried its own copies of ``or ()`` — including one that
    runs BEFORE this gate, so its ``TypeError`` reached the caller as an
    unstructured failure carrying no machine code at all (live QA r12).
    """
    return tuple(value) if isinstance(value, (list, tuple)) else ()


def _clean_target_paths(value: Any) -> Tuple[str, ...]:
    if isinstance(value, str):
        return (value.strip(),) if value.strip() else ()
    if isinstance(value, (list, tuple)):
        return tuple(item.strip() for item in value if isinstance(item, str) and item.strip())
    return ()


def destination_paths(route: str, entry: Mapping[str, Any]) -> Tuple[str, ...]:
    """The target paths ONE mapping entry binds, in authored order.

    The single reading of "what does this mapping write to", shared by the
    required-leaf coverage gate above and by ``review_transformation``'s mapping
    records. The advisory route used to carry its own copy of this reading — a
    direct/function ``target_path`` and a script's ``outputs[].target_path`` —
    which is the same fact modelled in two places, so the two could drift on any
    new route or spelling. Callers keep their own entry enumeration; only the
    destination interpretation lives here.
    """
    if route in ("direct", "map_function"):
        return _clean_target_paths(entry.get("target_path"))
    if route == "map_script":
        paths: List[str] = []
        for output in mapping_entries(entry.get("outputs")):
            if isinstance(output, Mapping):
                paths.extend(_clean_target_paths(output.get("target_path")))
        return tuple(paths)
    return ()


def required_target_coverage_gaps(
    target_index: Optional[Mapping[str, Mapping[str, Any]]],
    destinations: Any,
) -> Tuple[str, ...]:
    """The required, mappable target leaves that no destination binds — sorted.

    THE single implementation of required-target-leaf coverage (issue #157),
    replacing both legacy validator families: the JSON-profile walker
    (``_required_simple_leaf_paths``) and the DB-write required-target checks.
    It accepts a NORMALIZED target-field index — the shape both surviving
    generators emit (``profile_from_json_schema`` / the DB write builder's
    ``build_field_index``) — so JSON and DB targets are one case.

    An EMPTY index reports nothing. That preserves the DB deferral: an
    unsupported statement type or a write profile missing its required
    fields/conditions yields an empty write index, and the write-profile
    builder's own precise emit-time refusal must not be pre-empted by an
    invented coverage gap.
    """
    if not target_index:
        return ()
    bound = {
        path
        for _route, path in (
            destinations
            if all(isinstance(item, tuple) for item in destinations)
            else [("direct", item) for item in destinations]
        )
    }
    return tuple(
        sorted(
            path
            for path, record in target_index.items()
            if isinstance(record, Mapping)
            and bool(record.get("required"))
            and record.get("mappable", True)
            and path not in bound
        )
    )


def validate_required_target_coverage(
    effective_config: Mapping[str, Any],
    components_by_key: Optional[Dict[str, Any]],
    literal_indexes: Optional[Dict[str, Dict[str, Any]]] = None,
    *,
    selected_indexes: Optional[Dict[str, Dict[str, Any]]] = None,
) -> Optional[BuilderValidationError]:
    """The hard gate: a structured map leaving a required target leaf unbound.

    Resolves the target index through the same resolver every other map check
    uses; an unresolvable target is NOT this gate's finding (the map validator
    already reports ``MAP_PROFILE_INDEX_UNAVAILABLE`` for it). Offending paths
    travel in ``details`` for the caller's repair loop; the message carries a
    count only.
    """
    target_index = resolve_map_profile_index(
        effective_config.get("target_profile_id"),
        components_by_key,
        literal_indexes,
        selected_indexes,
    )
    if target_index is None:
        return None
    gaps = required_target_coverage_gaps(
        target_index, normalized_map_destinations(effective_config)
    )
    if not gaps:
        return None
    return BuilderValidationError(
        f"{len(gaps)} required target leaf path(s) have no mapping; every "
        "required simple leaf of the target profile must be the destination of "
        "at least one direct, map_function or map_script output.",
        error_code=TRANSFORM_REVIEW_REQUIRED_TARGET_UNMAPPED,
        field="target_profile_id",
        hint=(
            "Bind each path listed in details.missing_paths, or mark the target "
            "leaf optional in the profile."
        ),
        details={"missing_paths": list(gaps), "missing_count": len(gaps)},
    )


def validate_transform_map(
    effective_config: Mapping[str, Any],
    depends_on: Any,
    components_by_key: Optional[Dict[str, Any]],
    literal_indexes: Optional[Dict[str, Dict[str, Any]]] = None,
    selected_indexes: Optional[Dict[str, Dict[str, Any]]] = None,
) -> Optional[BuilderValidationError]:
    """Validate a transform.map component the way build_integration's plan does.

    ``effective_config`` should already carry ``component_name`` (callers inject
    the component's display name as a fallback, mirroring _execute_component).
    Returns the first ``BuilderValidationError`` found, or None when the map is
    valid. Covers: map_type/route support, source/target + script_mappings
    depends_on coverage, script-ref resolution + port-shape alignment, the
    canonical map-builder ``validate_config`` (route-class / profile_type /
    required lists / field-index checks), and unindexable / literal-UUID
    profile references.
    """
    gen_profile_err: Optional[BuilderValidationError] = None

    # transform.map: thread source / target field indexes from the in-spec
    # profile components so MAP_FIELD_NOT_FOUND fires when a $ref:KEY target
    # maps to a missing leaf in the referenced profile.
    source_index = resolve_map_profile_index(
        effective_config.get("source_profile_id"),
        components_by_key,
        literal_indexes,
        selected_indexes,
    )
    target_index = resolve_map_profile_index(
        effective_config.get("target_profile_id"),
        components_by_key,
        literal_indexes,
        selected_indexes,
    )
    # A non-string map_type (e.g. a JSON number/bool) must not raise on
    # .lower() — coerce to "" so get_map_builder returns None and the caller
    # gets a structured UNSUPPORTED_TRANSFORM_ROUTE instead of a TypeError.
    raw_map_type = effective_config.get("map_type")
    map_type = raw_map_type.lower() if isinstance(raw_map_type, str) else ""
    map_builder_instance = get_map_builder("transform.map", map_type)
    if map_builder_instance is None:
        return BuilderValidationError(
            f"map_type {map_type!r} is not supported for "
            "transform.map. Supported: direct, function, "
            "map_function, script, map_script.",
            error_code="UNSUPPORTED_TRANSFORM_ROUTE",
            field="map_type",
            hint=(
                "Use map_type='direct' for profile-to-profile "
                "mappings, map_type='function' for structured "
                "map-function primitives (#40), or "
                "map_type='script' for in-map calls to "
                "reusable script.mapping components (#41). "
                "XSLT remains tracked by #42."
            ),
        )

    # depends_on coverage for source/target profile $refs — keeps apply-time
    # topological ordering safe ($ref tokens resolve from earlier steps).
    declared_deps = set(depends_on or [])
    for side in ("source", "target"):
        ref_value = effective_config.get(f"{side}_profile_id")
        if isinstance(ref_value, str) and ref_value.startswith("$ref:"):
            ref_key = ref_value[len("$ref:") :]
            if ref_key not in declared_deps:
                gen_profile_err = BuilderValidationError(
                    f"{side}_profile_id $ref target must "
                    f"also appear in depends_on so the "
                    f"profile runs before the map",
                    error_code="MAP_PROFILE_REF_REQUIRED",
                    field="depends_on",
                    hint=(
                        f"Add the {side} profile key to "
                        "depends_on so the execution "
                        "order builds the profile before "
                        "the map."
                    ),
                    details={"side": side, "ref_key": ref_key},
                )
                break

    # script_mappings[].script_component_id $ref targets must appear in
    # depends_on AND resolve to an in-spec script.mapping / transform.function;
    # the map's port surface must match the referenced component's ports.
    if gen_profile_err is None and map_type in ("script", "map_script"):
        sm_list = effective_config.get("script_mappings") or []
        if isinstance(sm_list, list):
            for sm_idx, sm in enumerate(sm_list):
                if not isinstance(sm, Mapping):
                    continue
                ref_value = sm.get("script_component_id")
                if (
                    isinstance(ref_value, str)
                    and ref_value.strip()
                    and not ref_value.startswith("$ref:")
                ):
                    gen_profile_err = BuilderValidationError(
                        f"script_mappings[{sm_idx}]."
                        "script_component_id must be a "
                        "'$ref:KEY' pointing at an in-spec "
                        "script.mapping (auto-synth wrapper) "
                        "or transform.function wrapper. "
                        "Literal componentId values are "
                        "not supported in #41 — Boomi "
                        "requires the map FunctionStep "
                        "id to point at a transform.function "
                        "wrapper, which the system can "
                        "only synthesize from in-spec "
                        "components.",
                        error_code="SCRIPT_MAPPING_REF_REQUIRED",
                        field=(
                            f"script_mappings[{sm_idx}]."
                            "script_component_id"
                        ),
                        hint=(
                            "For existing-Boomi script "
                            "reuse: declare a "
                            "transform.function wrapper "
                            "as an in-spec component "
                            "(component_type="
                            "'transform.function' with "
                            "script_component_id referencing "
                            "the existing script.mapping "
                            "key) and reference it via "
                            "'$ref:<wrapper_key>'. For "
                            "in-spec script.mappings, use "
                            "'$ref:<script_key>' and the "
                            "wrapper is synthesized "
                            "automatically."
                        ),
                        details={"script_mappings_index": sm_idx},
                    )
                    break
                if not (
                    isinstance(ref_value, str)
                    and ref_value.startswith("$ref:")
                ):
                    continue
                ref_key = ref_value[len("$ref:") :]
                if ref_key not in declared_deps:
                    gen_profile_err = BuilderValidationError(
                        f"script_mappings[{sm_idx}]."
                        "script_component_id $ref "
                        "target must also appear in "
                        "depends_on so the script.mapping "
                        "applies before this map",
                        error_code="SCRIPT_MAPPING_REF_REQUIRED",
                        field="depends_on",
                        hint=(
                            "Add the script.mapping "
                            "component key to "
                            "depends_on so the "
                            "execution order builds "
                            "the script component "
                            "before the map."
                        ),
                        details={
                            "script_mappings_index": sm_idx,
                            "ref_key": ref_key,
                        },
                    )
                    break

                target_comp = (
                    components_by_key.get(ref_key)
                    if components_by_key is not None
                    else None
                )
                target_type = (
                    target_comp.type if target_comp is not None else None
                )
                if target_type not in (
                    "script.mapping",
                    "transform.function",
                ):
                    gen_profile_err = BuilderValidationError(
                        f"script_mappings[{sm_idx}]."
                        f"script_component_id $ref "
                        f"target {ref_key!r} resolves "
                        f"to a {target_type!r} "
                        "component, not a script.mapping "
                        "or transform.function wrapper",
                        error_code="SCRIPT_MAPPING_REF_REQUIRED",
                        field=(
                            f"script_mappings[{sm_idx}]."
                            "script_component_id"
                        ),
                        hint=(
                            "Use '$ref:<script_key>' "
                            "for an in-spec script.mapping "
                            "(auto-synth wrapper) or "
                            "'$ref:<wrapper_key>' for an "
                            "in-spec transform.function "
                            "wrapper. Literal componentIds "
                            "are not accepted at this "
                            "level — Boomi requires the "
                            "map FunctionStep id to point "
                            "at a wrapper, which the "
                            "system can only synthesize "
                            "from in-spec components."
                        ),
                        details={
                            "script_mappings_index": sm_idx,
                            "ref_key": ref_key,
                            "target_component_type": target_type,
                        },
                    )
                    break

                # Cross-validate the map's port surface against the referenced
                # component. Skip when inputs/outputs aren't lists yet —
                # validate_config below surfaces that structural error.
                raw_inputs = sm.get("inputs")
                raw_outputs = sm.get("outputs")
                if not isinstance(raw_inputs, list) or not isinstance(
                    raw_outputs, list
                ):
                    continue
                expected_input_names = _ref_target_input_names(target_comp)
                expected_output_names = _ref_target_output_names(target_comp)
                actual_input_names = [
                    str(entry.get("input_name") or "").strip()
                    for entry in raw_inputs
                    if isinstance(entry, Mapping)
                ]
                actual_output_names = [
                    str(entry.get("output_name") or "").strip()
                    for entry in raw_outputs
                    if isinstance(entry, Mapping)
                ]
                port_err = _check_port_shape_alignment(
                    sm_idx=sm_idx,
                    ref_key=ref_key,
                    target_type=target_type,
                    expected_inputs=expected_input_names,
                    actual_inputs=actual_input_names,
                    expected_outputs=expected_output_names,
                    actual_outputs=actual_output_names,
                )
                if port_err is not None:
                    gen_profile_err = port_err
                    break

    # function_mappings[] component references (defined_process_property_*):
    # the process_property_component_id $ref must appear in depends_on AND
    # resolve to an in-spec processproperty component.
    if gen_profile_err is None and map_type in ("function", "map_function"):
        fm_list = effective_config.get("function_mappings") or []
        if isinstance(fm_list, list):
            for fm_idx, fm in enumerate(fm_list):
                if not isinstance(fm, Mapping):
                    continue
                function_type = fm.get("function_type")
                family = (
                    get_function_family(function_type)
                    if isinstance(function_type, str)
                    else None
                )
                if family is None or family.component_reference_parameter is None:
                    continue
                param_name = family.component_reference_parameter
                field_path = f"function_mappings[{fm_idx}].parameters.{param_name}"
                parameters = fm.get("parameters")
                ref_value = (
                    parameters.get(param_name)
                    if isinstance(parameters, Mapping)
                    else None
                )
                if not (isinstance(ref_value, str) and ref_value.startswith("$ref:")):
                    gen_profile_err = BuilderValidationError(
                        f"{field_path} must be a '$ref:KEY' pointing at an "
                        f"in-spec {family.component_reference_type} component "
                        f"for function_type {family.name!r}; literal component "
                        "IDs are not accepted at plan time.",
                        error_code=MAP_FUNCTION_COMPONENT_REF_REQUIRED,
                        field=field_path,
                        hint=(
                            "Declare the Process Property component in-spec and "
                            f"reference it via '$ref:<key>'; add <key> to the "
                            "map's depends_on."
                        ),
                        details={
                            "function_mappings_index": fm_idx,
                            "function_type": family.name,
                            "parameter": param_name,
                        },
                    )
                    break
                ref_key = ref_value[len("$ref:") :]
                if ref_key not in declared_deps:
                    gen_profile_err = BuilderValidationError(
                        f"{field_path} $ref target {ref_key!r} must also appear "
                        "in depends_on so the Process Property component applies "
                        "before this map.",
                        error_code=MAP_FUNCTION_COMPONENT_REF_REQUIRED,
                        field="depends_on",
                        hint=(
                            "Add the processproperty component key to depends_on "
                            "so the execution order builds it before the map."
                        ),
                        details={
                            "function_mappings_index": fm_idx,
                            "ref_key": ref_key,
                        },
                    )
                    break
                target_comp = (
                    components_by_key.get(ref_key)
                    if components_by_key is not None
                    else None
                )
                target_type = target_comp.type if target_comp is not None else None
                if target_type != family.component_reference_type:
                    gen_profile_err = BuilderValidationError(
                        f"{field_path} $ref target {ref_key!r} resolves to a "
                        f"{target_type!r} component, not a "
                        f"{family.component_reference_type!r} component.",
                        error_code=MAP_FUNCTION_COMPONENT_REF_REQUIRED,
                        field=field_path,
                        hint=(
                            "Reference a Process Property component "
                            f"(type={family.component_reference_type!r}) declared "
                            "in-spec."
                        ),
                        details={
                            "function_mappings_index": fm_idx,
                            "ref_key": ref_key,
                            "target_component_type": target_type,
                        },
                    )
                    break

    # Issue #95: a resolved literal-UUID index must be TYPE-COMPATIBLE with the
    # map endpoint's declared profile type. Catch a wrong-type supplied/discovered
    # index pre-mutation (clearer than the MAP_FIELD_NOT_FOUND it would otherwise
    # surface once the paths inevitably mismatch).
    if gen_profile_err is None:
        for side, side_index in (
            ("source", source_index),
            ("target", target_index),
        ):
            ref_value = effective_config.get(f"{side}_profile_id")
            if not (
                isinstance(ref_value, str)
                and ref_value.strip()
                and not ref_value.startswith("$ref:")
                and side_index is not None
            ):
                continue
            declared = effective_config.get(f"{side}_profile_type")
            if not (isinstance(declared, str) and declared.strip()):
                continue
            index_type = _literal_index_profile_type(
                literal_indexes, effective_config.get(f"{side}_profile_id")
            )
            if index_type is not None and index_type != declared.strip():
                gen_profile_err = BuilderValidationError(
                    f"{side}_profile_id index type {index_type!r} does not match "
                    f"the declared {side}_profile_type {declared.strip()!r}",
                    error_code="MAP_PROFILE_INDEX_UNAVAILABLE",
                    field=f"{side}_profile_type",
                    hint=(
                        "Supply an index_profile_component index for the SAME "
                        "profile type as the map endpoint, or correct the "
                        f"declared {side}_profile_type."
                    ),
                    details={
                        "side": side,
                        "index_type": index_type,
                        "declared_type": declared.strip(),
                    },
                )
                break

    if gen_profile_err is None:
        gen_profile_err = type(map_builder_instance).validate_config(
            effective_config,
            source_index=source_index,
            target_index=target_index,
        )

    # A $ref pointing at a non-profile / missing / unindexable component
    # produces source_index/target_index == None. validate_config skips
    # path-existence checks when an index is None, so guard explicitly.
    if gen_profile_err is None:
        for side, side_index in (
            ("source", source_index),
            ("target", target_index),
        ):
            ref_value = effective_config.get(f"{side}_profile_id")
            if (
                isinstance(ref_value, str)
                and ref_value.startswith("$ref:")
                and side_index is None
            ):
                ref_key = ref_value[len("$ref:") :]
                target_comp = (
                    components_by_key.get(ref_key)
                    if components_by_key is not None
                    else None
                )
                target_type = (
                    target_comp.type if target_comp is not None else None
                )
                gen_profile_err = BuilderValidationError(
                    f"{side}_profile_id $ref target "
                    "could not be indexed — the referenced "
                    "component is missing, malformed, or "
                    "not a profile (profile.db / "
                    "profile.json / profile.xml).",
                    error_code="MAP_PROFILE_INDEX_UNAVAILABLE",
                    field=f"{side}_profile_id",
                    hint=(
                        "Confirm the referenced key exists "
                        "in the spec and is a profile "
                        "component the map builder can "
                        "index. Non-profile component "
                        "types cannot be referenced as "
                        "map endpoints in M2."
                    ),
                    details={
                        "side": side,
                        "ref_key": ref_key,
                        "target_component_type": target_type,
                    },
                )
                break

    # Literal-UUID profile refs (no $ref): issue #95 M7.5 makes these validatable
    # when a field index is supplied (profile_indexes_by_component_id) or is
    # live-discoverable. Only reject when NO index resolved for that side — an
    # available index already drove the specific MAP_FIELD_NOT_FOUND /
    # PROFILE_FIELD_NOT_MAPPABLE checks in validate_config above.
    if gen_profile_err is None:
        for side, side_index in (
            ("source", source_index),
            ("target", target_index),
        ):
            ref_value = effective_config.get(f"{side}_profile_id")
            if (
                isinstance(ref_value, str)
                and ref_value.strip()
                and not ref_value.startswith("$ref:")
                and side_index is None
            ):
                gen_profile_err = BuilderValidationError(
                    f"{side}_profile_id is a literal "
                    "existing-profile reference the map "
                    "builder could not index (no supplied "
                    "or discoverable field index).",
                    error_code="MAP_PROFILE_INDEX_UNAVAILABLE",
                    field=f"{side}_profile_id",
                    hint=(
                        f"Either declare the {side} "
                        "profile as an in-spec "
                        "profile.json / profile.xml / "
                        "profile.db component and "
                        "reference it via '$ref:KEY', or "
                        "index the existing profile with "
                        "index_profile_component and supply "
                        "it via "
                        "profile_indexes_by_component_id "
                        "(build_integration also discovers "
                        "indexable existing profiles live)."
                    ),
                    details={"side": side},
                )
                break

    # Issue #122 M11.3: authored map-level Document Cache joins — structural
    # shape plus $ref/depends_on and in-spec cache index/key cross-checks.
    if gen_profile_err is None:
        gen_profile_err = _validate_document_cache_joins(
            effective_config, declared_deps, components_by_key
        )

    # Issue #157: required-target-leaf COVERAGE is deliberately NOT part of this
    # structural validator. The legacy component-plan lint (and the legacy apply
    # refresh that reuses it) is frozen legacy surface until #160 and keeps its
    # pre-#157 verdict; the HARD gate is `validate_required_target_coverage`,
    # invoked by the canonical entry points — the typed intents' semantic
    # validation and the recipe engine — and consumed by the advisory review.
    return gen_profile_err


def _validate_document_cache_joins(
    effective_config: Mapping[str, Any],
    declared_deps: Any,
    components_by_key: Optional[Dict[str, Any]],
) -> Optional[BuilderValidationError]:
    """Validate authored ``document_cache_joins`` (issue #122 M11.3).

    Structure is checked by the shared map_builder helper (the same contract
    build-time rendering enforces). On top of that: a ``$ref:KEY``
    ``document_cache_id`` must appear in depends_on and resolve to an in-spec
    ``documentcache`` component, and when that component carries a structured
    config, the declared ``cache_index`` / ``cache_key_id`` values must exist
    in it (writer/reader/join key compatibility, pre-mutation).
    """
    joins = effective_config.get("document_cache_joins")
    if joins is None:
        return None
    structural = validate_document_cache_joins_structure(joins)
    if structural is not None:
        return structural
    deps = set(declared_deps or [])
    for i, join in enumerate(joins):
        join_field = f"document_cache_joins[{i}]"
        cache_id = str(join.get("document_cache_id") or "").strip()
        if not cache_id.startswith("$ref:"):
            # Literal id — an existing account cache; nothing to cross-check.
            continue
        ref_key = cache_id[len("$ref:") :]
        if ref_key not in deps:
            return BuilderValidationError(
                f"{join_field}.document_cache_id $ref target must also appear "
                "in depends_on so the cache builds before the map",
                error_code=MAP_DOCUMENT_CACHE_JOINS_INVALID,
                field="depends_on",
                hint=f"Add {ref_key!r} to the map component's depends_on.",
                details={"ref_key": ref_key},
            )
        target_comp = (
            components_by_key.get(ref_key)
            if components_by_key is not None
            else None
        )
        if target_comp is None or getattr(target_comp, "type", None) != "documentcache":
            return BuilderValidationError(
                f"{join_field}.document_cache_id $ref must resolve to an "
                "in-spec documentcache component",
                error_code=MAP_DOCUMENT_CACHE_JOINS_INVALID,
                field=f"{join_field}.document_cache_id",
                details={
                    "ref_key": ref_key,
                    "target_component_type": getattr(target_comp, "type", None),
                },
            )
        cache_config = getattr(target_comp, "config", None) or {}
        indexes = cache_config.get("indexes")
        if not isinstance(indexes, list) or not indexes:
            # Raw-XML or config-less cache spec — no structure to cross-check.
            continue
        index_entry = next(
            (
                idx
                for idx in indexes
                if isinstance(idx, Mapping)
                and idx.get("index_id") == join.get("cache_index")
            ),
            None,
        )
        if index_entry is None:
            return BuilderValidationError(
                f"{join_field}.cache_index {join.get('cache_index')!r} does not "
                f"exist in the referenced documentcache component {ref_key!r}",
                error_code=MAP_DOCUMENT_CACHE_JOINS_INVALID,
                field=f"{join_field}.cache_index",
                details={
                    "ref_key": ref_key,
                    "declared_indexes": [
                        idx.get("index_id")
                        for idx in indexes
                        if isinstance(idx, Mapping)
                    ],
                },
            )
        declared_key_ids = {
            key.get("id")
            for key in (index_entry.get("keys") or [])
            if isinstance(key, Mapping)
        }
        provided_key_ids = []
        for j, kv in enumerate(join.get("key_values") or []):
            if kv.get("cache_key_id") not in declared_key_ids:
                return BuilderValidationError(
                    f"{join_field}.key_values[{j}].cache_key_id "
                    f"{kv.get('cache_key_id')!r} does not exist in index "
                    f"{join.get('cache_index')!r} of {ref_key!r}",
                    error_code=MAP_DOCUMENT_CACHE_JOINS_INVALID,
                    field=f"{join_field}.key_values[{j}].cache_key_id",
                    details={
                        "ref_key": ref_key,
                        "declared_key_ids": sorted(
                            k for k in declared_key_ids if k is not None
                        ),
                    },
                )
            provided_key_ids.append(kv.get("cache_key_id"))
        # A join must bind EVERY key of the chosen index exactly once — a
        # composite index joined on a subset (or with duplicates) emits an
        # incomplete CacheKeyJoinValues block that finds nothing at runtime.
        if len(provided_key_ids) != len(set(provided_key_ids)) or set(
            provided_key_ids
        ) != declared_key_ids:
            return BuilderValidationError(
                f"{join_field}.key_values must bind every key of index "
                f"{join.get('cache_index')!r} of {ref_key!r} exactly once "
                f"(declared {sorted(k for k in declared_key_ids if k is not None)}, "
                f"provided {provided_key_ids})",
                error_code=MAP_DOCUMENT_CACHE_JOINS_INVALID,
                field=f"{join_field}.key_values",
                hint=(
                    "Add one key_values entry per cacheKey id in the joined "
                    "index — a partial or duplicated binding silently matches "
                    "no cached documents."
                ),
                details={
                    "ref_key": ref_key,
                    "declared_key_ids": sorted(
                        k for k in declared_key_ids if k is not None
                    ),
                    "provided_key_ids": provided_key_ids,
                },
            )
    return None

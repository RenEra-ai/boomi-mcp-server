"""Shared, Boomi-free transform.map validation.

Extracted from ``integration_builder._build_plan`` so the plan path AND the
read-only ``review_transformation`` surface validate transform.map components
through one source of truth (no drift between what build rejects and what
review flags). Pure: no SDK, no network, no credential access.

``components_by_key`` values only need ``.type`` / ``.config`` / ``.name``
attribute access, so callers may pass ``IntegrationComponentSpec`` instances or
any lightweight object exposing those attributes.
"""

from typing import Any, Dict, List, Mapping, Optional

from .connector_builder import BuilderValidationError
from .json_profile_builder import JSONGeneratedProfileBuilder
from .map_builder import get_map_builder
from .profile_builder import DatabaseReadProfileBuilder
from .xml_profile_builder import XMLGeneratedProfileBuilder


def resolve_map_profile_index(
    profile_id: Any,
    components_by_key: Optional[Dict[str, Any]],
) -> Optional[Dict[str, Dict[str, Any]]]:
    """Resolve the field index for a transform.map's source / target profile
    reference. Returns None when the reference is a literal UUID or points at a
    missing / non-profile component — in those cases the map builder's
    validator raises MAP_PROFILE_INDEX_UNAVAILABLE."""
    if components_by_key is None:
        return None
    if not isinstance(profile_id, str) or not profile_id.startswith("$ref:"):
        return None
    ref_key = profile_id[len("$ref:") :]
    target_comp = components_by_key.get(ref_key)
    if target_comp is None:
        return None
    raw_config = target_comp.config or {}
    builder_cls = None
    if target_comp.type == "profile.json":
        builder_cls = JSONGeneratedProfileBuilder
    elif target_comp.type == "profile.xml":
        builder_cls = XMLGeneratedProfileBuilder
    elif target_comp.type == "profile.db":
        builder_cls = DatabaseReadProfileBuilder
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


def validate_transform_map(
    effective_config: Mapping[str, Any],
    depends_on: Any,
    components_by_key: Optional[Dict[str, Any]],
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
        effective_config.get("source_profile_id"), components_by_key
    )
    target_index = resolve_map_profile_index(
        effective_config.get("target_profile_id"), components_by_key
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

    # Literal-UUID profile refs (no $ref) can't be indexed in M2 — indexing
    # live existing-profile XML is separate future work (NOT infer_profile_fields,
    # which infers from supplied artifacts). Reject so the caller knows what to fix.
    if gen_profile_err is None:
        for side in ("source", "target"):
            ref_value = effective_config.get(f"{side}_profile_id")
            if isinstance(ref_value, str) and not ref_value.startswith("$ref:"):
                gen_profile_err = BuilderValidationError(
                    f"{side}_profile_id is a literal "
                    "existing-profile reference without "
                    "an in-spec generated profile "
                    "component — the map builder has no "
                    "field index to validate against.",
                    error_code="MAP_PROFILE_INDEX_UNAVAILABLE",
                    field=f"{side}_profile_id",
                    hint=(
                        f"Either declare the {side} "
                        "profile as an in-spec "
                        "profile.json / profile.xml / "
                        "profile.db component and "
                        f"reference it via '$ref:KEY'. "
                        "Indexing live existing-profile "
                        "XML is separate future work, not "
                        "covered by infer_profile_fields."
                    ),
                    details={"side": side},
                )
                break

    return gen_profile_err

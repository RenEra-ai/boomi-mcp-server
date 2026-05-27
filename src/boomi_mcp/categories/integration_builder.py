"""
High-level integration builder orchestration.

This module provides a single action router that can:
- plan: normalize and validate an integration spec, then build an execution plan
- apply: execute component operations in deterministic dependency order
- verify: verify created/updated components and declared dependency wiring
"""

from __future__ import annotations

import re
from collections import defaultdict
from datetime import datetime, timezone
from typing import Any, Dict, List, Mapping, Optional
from uuid import uuid4

# Matches `subType="database"` and `subType='database'` with any (or no)
# whitespace around the `=`. XML attribute syntax allows whitespace there,
# so an exact substring check would miss valid raw XML and skip the
# database secret scan.
_XML_DATABASE_SUBTYPE_RE = re.compile(r'\bsubType\s*=\s*["\']database["\']')

# Same idea for REST Client raw XML — a connector_type-less raw payload that
# carries `subType="officialboomi-X3979C-rest-prod"` should still trigger the
# REST secret scan so plaintext credentials cannot leak through the plan echo
# (codex review item #2 against the superseded HTTP-issue-#24 implementation).
_XML_REST_SUBTYPE_RE = re.compile(
    r'\bsubType\s*=\s*["\']officialboomi-X3979C-rest-prod["\']'
)

from boomi import Boomi
from boomi.models import (
    ComponentMetadataQueryConfig,
    ComponentMetadataQueryConfigQueryFilter,
    ComponentMetadataSimpleExpression,
    ComponentMetadataSimpleExpressionOperator,
    ComponentMetadataSimpleExpressionProperty,
)

from ..models.integration_models import IntegrationComponentSpec, IntegrationSpecV1
from .components._shared import component_get_xml, paginate_metadata
from .components.builders import (
    BuilderValidationError,
    DatabaseConnectorBuilder,
    DatabaseGetOperationBuilder,
    DatabaseReadProfileBuilder,
    DatabaseStoredProcedureReadProfileBuilder,
    REST_CLIENT_SUBTYPE,
    RestClientConnectionBuilder,
    RestClientOperationBuilder,
    ProcessFlowBuilder,
    PROFILE_BUILDERS,
    PROCESS_FLOW_BUILDERS,
    get_process_flow_builder,
    get_profile_builder,
)
from .components.builders.connector_builder import _resolve_rest_connector_type
from .components.builders.json_profile_builder import JSONGeneratedProfileBuilder
from .components.builders.xml_profile_builder import XMLGeneratedProfileBuilder
from .components.builders.map_builder import DirectMapBuilder, get_map_builder
from .components.builders.script_mapping_builder import (
    ScriptMappingBuilder,
    get_script_mapping_builder,
)
from .components.builders.transform_function_wrapper_builder import (
    TransformFunctionWrapperBuilder,
    get_transform_function_wrapper_builder,
)
from .components.connectors import create_connector, update_connector
from .components.manage_component import create_component, update_component
from .components.processes import create_process, update_process
from .components.trading_partners import create_trading_partner, update_trading_partner


# Session-scoped; lost on server restart. Verify calls are best-effort.
_BUILD_REGISTRY: Dict[str, Dict[str, Any]] = {}

_TYPE_ALIASES = {
    "process": "process",
    "connector": "connector-settings",
    "connection": "connector-settings",
    "connector-settings": "connector-settings",
    "connector_action": "connector-action",
    "operation": "connector-action",
    "connector-action": "connector-action",
    "tradingpartner": "trading_partner",
    "trading_partner": "trading_partner",
    "component": "component",
    "profile.db": "profile.db",
    "profile.json": "profile.json",
    "profile.xml": "profile.xml",
    "transform.map": "transform.map",
}

_METADATA_TYPE_MAP = {
    "process": "process",
    "connector-settings": "connector-settings",
    "connector-action": "connector-action",
    "trading_partner": "tradingpartner",
    "profile.db": "profile.db",
    # Issue #26 adds builders for the three generated/direct component types.
    # Both the structured-config path and the raw-XML escape hatch route
    # through these metadata keys.
    "profile.json": "profile.json",
    "profile.xml": "profile.xml",
    "transform.map": "transform.map",
    # Issue #41: standalone script.mapping participates in metadata lookup
    # so conflict_policy=reuse/fail can find existing script components by
    # name and update-by-name resolves correctly. Mirrors the clone-suffix
    # safeguard block which already includes script.mapping.
    "script.mapping": "script.mapping",
    # Issue #41 r3: auto-synthesized transform.function wrappers also
    # participate in metadata lookup so repeated plan runs reuse the
    # same wrapper component instead of leaking duplicates.
    "transform.function": "transform.function",
}


def _normalize_component_type(value: str) -> str:
    key = (value or "").strip().lower()
    return _TYPE_ALIASES.get(key, key)


def _normalize_component(raw: Dict[str, Any], index: int) -> Dict[str, Any]:
    if not isinstance(raw, dict):
        raise ValueError(f"integration_spec.components[{index - 1}] must be a JSON object")

    key = raw.get("key") or raw.get("name") or f"component_{index}"
    component_type = raw.get("type") or raw.get("component_type")
    if not component_type:
        raise ValueError(f"Component '{key}' is missing required field: type")

    normalized_type = _normalize_component_type(component_type)
    action = (raw.get("action") or "create").lower()
    if action not in ("create", "update"):
        raise ValueError(f"Component '{key}' has invalid action '{action}'. Use create or update.")

    config = raw.get("config")
    if config is None:
        config = raw.get("spec", {})
    if not isinstance(config, dict):
        raise ValueError(f"Component '{key}' config must be a JSON object")

    depends_on = raw.get("depends_on")
    if depends_on is None:
        depends_on = raw.get("dependencies", [])
    if not isinstance(depends_on, list):
        raise ValueError(f"Component '{key}' depends_on must be an array")

    # Promote config.name to top-level name when the caller omitted it.
    # _resolve_existing_components matches against comp.name only — without
    # this fallback a process whose only name is inside config bypasses
    # collision detection (Codex review r7 P2.1).
    #
    # Strip whitespace from BOTH surfaces so collision lookup, the
    # PROCESS_NAME_CONFLICT check, and emitted XML all see the same
    # canonical value. Codex review r10: top-level `name="X"` with
    # `config.name=" X "` used to plan as `create` (lookup queried
    # `"X"`, found nothing) and then emit XML carrying `" X "` —
    # bypassing the r8 mismatch guard because the stripped comparison
    # treated them as equal.
    raw_name = raw.get("name")
    if isinstance(raw_name, str):
        raw_name = raw_name.strip()
        raw["name"] = raw_name  # not strictly needed downstream but keeps `raw` consistent for any in-place inspector
    config_name = config.get("name") if isinstance(config, dict) else None
    if isinstance(config_name, str) and isinstance(config, dict):
        config["name"] = config_name.strip()
        config_name = config["name"]
    effective_name = (
        raw_name
        if isinstance(raw_name, str) and raw_name
        else (config_name if isinstance(config_name, str) and config_name else raw_name)
    )

    return {
        "key": key,
        "type": normalized_type,
        "action": action,
        "name": effective_name,
        "component_id": raw.get("component_id"),
        "config": config,
        "depends_on": depends_on,
    }


def _normalize_to_spec(config: Dict[str, Any]) -> IntegrationSpecV1:
    if not isinstance(config, dict):
        raise ValueError("config must be a JSON object")

    mode = (config.get("mode") or "lift_shift").strip().lower()
    source_description = config.get("source_description")
    spec_payload = config.get("integration_spec")

    if spec_payload is None:
        if isinstance(source_description, dict):
            spec_payload = {
                "name": source_description.get("name") or config.get("name") or "Integration Build",
                "mode": mode,
                "components": source_description.get("components", []),
                "goals": source_description.get("goals", []),
                "endpoints": source_description.get("endpoints", []),
                "flows": source_description.get("flows", []),
                "naming": source_description.get("naming", {}),
                "folders": source_description.get("folders", {}),
                "runtime": source_description.get("runtime", {}),
                "validation_rules": source_description.get("validation_rules", {}),
            }
        else:
            spec_payload = {
                "name": config.get("name") or "Integration Build",
                "mode": mode,
                "components": config.get("components", []),
                "goals": [source_description] if isinstance(source_description, str) and source_description.strip() else [],
                "endpoints": config.get("endpoints", []),
                "flows": config.get("flows", []),
                "naming": config.get("naming", {}),
                "folders": config.get("folders", {}),
                "runtime": config.get("runtime", {}),
                "validation_rules": config.get("validation_rules", {}),
            }

    if not isinstance(spec_payload, dict):
        raise ValueError("integration_spec must be a JSON object")

    spec_data = dict(spec_payload)
    spec_data.setdefault("mode", mode)
    if "name" not in spec_data or not spec_data.get("name"):
        spec_data["name"] = config.get("name") or "Integration Build"

    raw_components = spec_data.get("components", [])
    if not isinstance(raw_components, list):
        raise ValueError("integration_spec.components must be an array")
    normalized_components = [_normalize_component(item, idx + 1) for idx, item in enumerate(raw_components)]
    spec_data["components"] = normalized_components

    return IntegrationSpecV1(**spec_data)


# Codex r3 P1 finding #1: Boomi's live transform.map XML references a
# ``transform.function`` wrapper from the userdefined ``<FunctionStep id=...>``
# attribute — NOT the script.mapping directly. The wrapper internally
# references the script.mapping via ``<Configuration><Scripting
# componentId="...">``. Maps that wire a script.mapping UUID into the
# FunctionStep id slot will not bind at Boomi runtime.
#
# Rather than make callers author the wrapper component manually, the
# integration builder synthesises one per referenced script.mapping at
# plan time. The synthesized wrapper is a first-class in-spec component:
# it appears in the plan output, applies in topological order before the
# calling map, and has its own depends_on edge to the underlying
# script.mapping.
#
# Shared-wrapper policy: one wrapper per (script.mapping key) within the
# spec — multiple maps that reference the same script.mapping share the
# same wrapper. Callers who need bespoke wrappers can declare them
# explicitly as transform.function components.
_AUTO_WRAPPER_KEY_PREFIX = "__auto_wrapper_"


def _synthesize_script_function_wrappers(spec: IntegrationSpecV1) -> None:
    """Synthesize transform.function wrappers for transform.map/script entries.

    Mutates ``spec.components`` in place: appends synthesized wrapper
    components, rewrites each calling map's
    ``script_mappings[].script_component_id`` to point at the wrapper
    key, and adds the wrapper key to the map's ``depends_on``.

    Only $ref:KEY references targeting in-spec ``script.mapping``
    components trigger synthesis. Literal componentId values and refs
    that target other types are left untouched — those are the caller's
    responsibility to wire correctly (and validation downstream surfaces
    the mismatch where it can).
    """
    components_by_key: Dict[str, IntegrationComponentSpec] = {
        comp.key: comp for comp in spec.components
    }

    # Existing wrappers (keyed by script.mapping key) so we can share
    # across multiple calling maps. Also covers caller-declared wrappers
    # already in the spec.
    synthesized_wrappers_by_script_key: Dict[str, str] = {}

    new_wrappers: List[IntegrationComponentSpec] = []

    for comp in spec.components:
        if comp.type != "transform.map":
            continue
        map_type = ((comp.config or {}).get("map_type") or "").strip().lower()
        if map_type not in ("script", "map_script"):
            continue
        script_mappings = (comp.config or {}).get("script_mappings")
        if not isinstance(script_mappings, list):
            continue

        for sm in script_mappings:
            if not isinstance(sm, dict):
                continue
            ref_value = sm.get("script_component_id")
            if not (
                isinstance(ref_value, str) and ref_value.startswith("$ref:")
            ):
                continue
            script_key = ref_value[len("$ref:") :]
            script_comp = components_by_key.get(script_key)
            if script_comp is None or script_comp.type != "script.mapping":
                # Not a script.mapping reference — leave untouched. The
                # plan-time validator surfaces a structured error for the
                # wrong-type case at the same level (Codex r1 P2 #4).
                continue

            wrapper_key = synthesized_wrappers_by_script_key.get(script_key)
            if wrapper_key is None:
                wrapper_key = f"{_AUTO_WRAPPER_KEY_PREFIX}{script_key}__"
                # If the caller happens to have declared a component with
                # exactly this key already, don't synthesize a duplicate —
                # trust the caller's declaration.
                if wrapper_key in components_by_key:
                    synthesized_wrappers_by_script_key[script_key] = wrapper_key
                else:
                    wrapper = _build_auto_wrapper_spec(
                        wrapper_key=wrapper_key,
                        script_key=script_key,
                        script_comp=script_comp,
                    )
                    new_wrappers.append(wrapper)
                    components_by_key[wrapper_key] = wrapper
                    synthesized_wrappers_by_script_key[script_key] = wrapper_key

            # Rewrite the map's reference to point at the wrapper.
            sm["script_component_id"] = f"$ref:{wrapper_key}"

            # Add the wrapper key to the calling map's depends_on.
            if wrapper_key not in comp.depends_on:
                comp.depends_on.append(wrapper_key)

    spec.components.extend(new_wrappers)


def _build_auto_wrapper_spec(
    *,
    wrapper_key: str,
    script_key: str,
    script_comp: IntegrationComponentSpec,
) -> IntegrationComponentSpec:
    """Construct an auto-synthesized transform.function wrapper IntegrationComponentSpec.

    The wrapper's structure is copied from the referenced script.mapping:
    same language, preserve_order, use_cache, script_body, inputs, outputs.
    The wrapper carries an inline ScriptToExecute snapshot (matching live
    Boomi shape) and references the script.mapping at runtime via
    Configuration/Scripting componentId.
    """
    script_cfg = script_comp.config or {}
    base_name = (
        script_comp.name
        or script_cfg.get("component_name")
        or script_key
    )
    wrapper_name = f"{base_name} (Wrapper)"

    # Inputs / outputs are copied verbatim from the script.mapping — they
    # define the wrapper's external port surface and the inner Scripting
    # variable declarations.
    inputs = [
        {
            "name": str(entry.get("name") or "").strip(),
            "data_type": str(entry.get("data_type") or "").strip(),
        }
        for entry in (script_cfg.get("inputs") or [])
        if isinstance(entry, Mapping)
    ]
    outputs = [
        {"name": str(entry.get("name") or "").strip()}
        for entry in (script_cfg.get("outputs") or [])
        if isinstance(entry, Mapping)
    ]

    wrapper_config: Dict[str, Any] = {
        "component_type": "transform.function",
        "component_name": wrapper_name,
        "script_component_id": f"$ref:{script_key}",
        "language": script_cfg.get("language"),
        "script_body": script_cfg.get("script_body"),
        "inputs": inputs,
        "outputs": outputs,
    }
    # Mirror optional script.mapping flags so the wrapper's inline
    # Configuration/Scripting attributes match the referenced component.
    for opt_key in ("preserve_order", "use_cache"):
        if opt_key in script_cfg:
            wrapper_config[opt_key] = script_cfg[opt_key]
    if script_cfg.get("folder_path"):
        wrapper_config["folder_path"] = script_cfg["folder_path"]

    return IntegrationComponentSpec(
        key=wrapper_key,
        type="transform.function",
        action="create",
        name=wrapper_name,
        config=wrapper_config,
        depends_on=[script_key],
    )


# Codex r5 P1 #2 helpers — extract the external port surface from a
# referenced script.mapping or transform.function wrapper so we can
# cross-validate the calling map's script_mappings entry against it.
def _ref_target_input_names(
    target_comp: IntegrationComponentSpec,
) -> List[str]:
    """Return the ordered list of input names exposed by the referenced
    component's external port surface."""
    cfg = target_comp.config or {}
    return [
        str(entry.get("name") or "").strip()
        for entry in (cfg.get("inputs") or [])
        if isinstance(entry, Mapping) and entry.get("name")
    ]


def _ref_target_output_names(
    target_comp: IntegrationComponentSpec,
) -> List[str]:
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

    Cross-checks:

    1. Input count match.
    2. Output count match.
    3. Input names — every actual name must exist in the expected set
       (order can differ; Boomi binds by name).
    4. Output names — same rule.

    The error code is ``SCRIPT_MAPPING_VARIABLE_INVALID`` since the
    mismatch always boils down to a variable-name / port mismatch on
    the calling side.
    """
    field_prefix = f"script_mappings[{sm_idx}]"

    if len(actual_inputs) != len(expected_inputs):
        return BuilderValidationError(
            f"{field_prefix}.inputs declares "
            f"{len(actual_inputs)} entries but the referenced "
            f"{target_type} component {ref_key!r} declares "
            f"{len(expected_inputs)} input ports",
            error_code="SCRIPT_MAPPING_VARIABLE_INVALID",
            field=f"{field_prefix}.inputs",
            hint=(
                "Each map script_mappings input must map to a declared "
                f"input on the referenced {target_type}; counts must "
                "match. Expected: " + ", ".join(expected_inputs) or "(none)"
            ),
            details={
                "script_mappings_index": sm_idx,
                "ref_key": ref_key,
                "expected_inputs": expected_inputs,
                "actual_inputs": actual_inputs,
            },
        )

    if len(actual_outputs) != len(expected_outputs):
        return BuilderValidationError(
            f"{field_prefix}.outputs declares "
            f"{len(actual_outputs)} entries but the referenced "
            f"{target_type} component {ref_key!r} declares "
            f"{len(expected_outputs)} output ports",
            error_code="SCRIPT_MAPPING_VARIABLE_INVALID",
            field=f"{field_prefix}.outputs",
            hint=(
                "Each map script_mappings output must map to a declared "
                f"output on the referenced {target_type}; counts must "
                "match. Expected: " + ", ".join(expected_outputs) or "(none)"
            ),
            details={
                "script_mappings_index": sm_idx,
                "ref_key": ref_key,
                "expected_outputs": expected_outputs,
                "actual_outputs": actual_outputs,
            },
        )

    expected_input_set = set(expected_inputs)
    for in_idx, actual_name in enumerate(actual_inputs):
        if actual_name not in expected_input_set:
            return BuilderValidationError(
                f"{field_prefix}.inputs[{in_idx}].input_name "
                f"{actual_name!r} does not match any declared input on "
                f"the referenced {target_type} component {ref_key!r}",
                error_code="SCRIPT_MAPPING_VARIABLE_INVALID",
                field=f"{field_prefix}.inputs[{in_idx}].input_name",
                hint=(
                    "Boomi binds map ports to the referenced component "
                    "by name. Expected one of: "
                    + (", ".join(expected_inputs) or "(none)")
                ),
                details={
                    "script_mappings_index": sm_idx,
                    "input_index": in_idx,
                    "ref_key": ref_key,
                    "actual_name": actual_name,
                    "expected_names": expected_inputs,
                },
            )

    expected_output_set = set(expected_outputs)
    for out_idx, actual_name in enumerate(actual_outputs):
        if actual_name not in expected_output_set:
            return BuilderValidationError(
                f"{field_prefix}.outputs[{out_idx}].output_name "
                f"{actual_name!r} does not match any declared output on "
                f"the referenced {target_type} component {ref_key!r}",
                error_code="SCRIPT_MAPPING_VARIABLE_INVALID",
                field=f"{field_prefix}.outputs[{out_idx}].output_name",
                hint=(
                    "Boomi binds map ports to the referenced component "
                    "by name. Expected one of: "
                    + (", ".join(expected_outputs) or "(none)")
                ),
                details={
                    "script_mappings_index": sm_idx,
                    "output_index": out_idx,
                    "ref_key": ref_key,
                    "actual_name": actual_name,
                    "expected_names": expected_outputs,
                },
            )

    return None


def _topological_order(spec: IntegrationSpecV1) -> List[str]:
    components_by_key = {comp.key: comp for comp in spec.components}
    if len(components_by_key) != len(spec.components):
        raise ValueError("Duplicate component keys are not allowed")

    indegree = {key: 0 for key in components_by_key}
    graph: Dict[str, List[str]] = defaultdict(list)

    for comp in spec.components:
        for dep in comp.depends_on:
            if dep not in components_by_key:
                raise ValueError(f"Component '{comp.key}' depends on unknown component '{dep}'")
            graph[dep].append(comp.key)
            indegree[comp.key] += 1

    ready = sorted([key for key, degree in indegree.items() if degree == 0])
    ordered: List[str] = []

    while ready:
        current = ready.pop(0)
        ordered.append(current)
        for dependent in sorted(graph.get(current, [])):
            indegree[dependent] -= 1
            if indegree[dependent] == 0:
                ready.append(dependent)
        ready.sort()

    if len(ordered) != len(spec.components):
        raise ValueError("Circular dependency detected in integration components")

    return ordered


def _metadata_type_for_component(comp: IntegrationComponentSpec) -> Optional[str]:
    if comp.type == "component":
        raw_type = comp.config.get("type")
        if isinstance(raw_type, str):
            return raw_type
        return None
    return _METADATA_TYPE_MAP.get(comp.type)


def _resolve_existing_components(
    boomi_client: Boomi, comp: IntegrationComponentSpec
) -> List[Dict[str, Any]]:
    """Return ALL metadata dicts matching *comp* by type + exact name.

    Each dict contains at least: component_id, name, folder_name, type.
    Returns an empty list when no matches exist or the component has
    no name / no resolvable metadata type.
    """
    if not comp.name:
        return []

    metadata_type = _metadata_type_for_component(comp)
    if not metadata_type:
        return []

    expression = ComponentMetadataSimpleExpression(
        operator=ComponentMetadataSimpleExpressionOperator.EQUALS,
        property=ComponentMetadataSimpleExpressionProperty.TYPE,
        argument=[metadata_type],
    )
    query_filter = ComponentMetadataQueryConfigQueryFilter(expression=expression)
    query_config = ComponentMetadataQueryConfig(query_filter=query_filter)
    components = paginate_metadata(boomi_client, query_config, show_all=False)
    matches = [item for item in components if item.get("name") == comp.name]
    matches.sort(key=lambda item: item.get("component_id", ""))
    return matches


def _extract_component_id(result: Dict[str, Any]) -> Optional[str]:
    if not isinstance(result, dict):
        return None

    direct_keys = ("component_id", "process_id", "id")
    for key in direct_keys:
        value = result.get(key)
        if isinstance(value, str) and value:
            return value

    trading_partner = result.get("trading_partner")
    if isinstance(trading_partner, dict):
        value = trading_partner.get("component_id")
        if isinstance(value, str) and value:
            return value

    components = result.get("components")
    if isinstance(components, dict) and len(components) == 1:
        only = next(iter(components.values()))
        if isinstance(only, dict):
            value = only.get("component_id")
            if isinstance(value, str) and value:
                return value

    return None


# --------------------------------------------------------------------------
# Cross-component $ref type classification (issue #49)
#
# These helpers read an IntegrationComponentSpec from `components_by_key` and
# classify it into the role buckets the preflight type checks compare against.
# They are read-only (no Boomi calls, no mutation). When a component's
# family cannot be reliably classified (raw XML connector-action with no
# structured connector_type, generic wrapper with an unrecognized
# config.type, etc.) the helpers return None — and because the call sites
# compare classifier output against the expected role string, a None result
# is treated as a mismatch and the in-spec ref is REJECTED with a
# *_REF_TYPE_MISMATCH error. Only OUTSIDE-spec refs (where
# `components_by_key.get(ref_key) is None` — i.e. direct UUIDs / literal
# live component-ids) skip the type check; ambiguous in-spec metadata
# fails plan-time per the source plan's "callers should add plan-only
# metadata or use direct UUIDs" guidance.
# --------------------------------------------------------------------------


def _effective_component_type(comp: IntegrationComponentSpec) -> Optional[str]:
    """Return the type to use for ref classification.

    For the generic ``"component"`` wrapper, fall back to ``config.type`` /
    ``config.component_type`` so a wrapper carrying ``config.type="profile.json"``
    classifies the same way as a top-level ``profile.json`` component.
    Mirrors the wrapper-unwrapping ``_metadata_type_for_component`` already
    does at integration_builder.py:245.
    """
    if comp.type != "component":
        return comp.type
    raw_type = comp.config.get("type") if isinstance(comp.config, dict) else None
    if isinstance(raw_type, str) and raw_type.strip():
        return raw_type.strip()
    raw_type = comp.config.get("component_type") if isinstance(comp.config, dict) else None
    if isinstance(raw_type, str) and raw_type.strip():
        return raw_type.strip()
    return None


def _classify_connector_settings(comp: IntegrationComponentSpec) -> Optional[str]:
    """Return ``"database connector-settings"`` / ``"REST Client connector-settings"`` / None.

    Uses the same family detectors ``_build_plan`` uses (raw XML subType
    fallback included) so the classification stays consistent with the
    routing in the call sites above.
    """
    if _effective_component_type(comp) != "connector-settings":
        return None
    raw_config = comp.config if isinstance(comp.config, dict) else {}
    xml_payload = raw_config.get("xml")
    xml_text = xml_payload if isinstance(xml_payload, str) else ""

    connector_type = raw_config.get("connector_type")
    if isinstance(connector_type, str) and connector_type.strip().lower() == "database":
        return "database connector-settings"
    if _XML_DATABASE_SUBTYPE_RE.search(xml_text):
        return "database connector-settings"
    if _resolve_rest_connector_type(connector_type) is not None:
        return "REST Client connector-settings"
    if _XML_REST_SUBTYPE_RE.search(xml_text):
        return "REST Client connector-settings"
    return None


def _classify_connector_action(
    comp: IntegrationComponentSpec,
) -> tuple[Optional[str], Optional[str]]:
    """Return (role, http_method_upper).

    ``role`` is one of:
      * ``"database connector-action Get"`` — database Get operation
      * ``"database connector-action <other>"`` — database non-Get operation
      * ``"REST Client connector-action"`` — REST Client operation (any method)
      * None — connector-action whose family/role cannot be classified

    ``http_method_upper`` is the declared HTTP method for REST operations
    (uppercased), or None when not declared / not a REST operation. The
    process-flow ref-type check uses it to compare against
    ``target.action_type``.
    """
    if _effective_component_type(comp) != "connector-action":
        return (None, None)
    raw_config = comp.config if isinstance(comp.config, dict) else {}

    connector_type = raw_config.get("connector_type")
    family_lower = (
        connector_type.strip().lower() if isinstance(connector_type, str) else ""
    )

    if family_lower == "database":
        mode = raw_config.get("operation_mode")
        mode_lower = mode.strip().lower() if isinstance(mode, str) else ""
        action_type = raw_config.get("action_type") or raw_config.get("actionType")
        action_lower = (
            action_type.strip().lower() if isinstance(action_type, str) else ""
        )
        if mode_lower == "get" or action_lower == "get":
            return ("database connector-action Get", None)
        return ("database connector-action <other>", None)

    if _resolve_rest_connector_type(connector_type) is not None:
        method = raw_config.get("method")
        if not (isinstance(method, str) and method.strip()):
            method = raw_config.get("action_type") or raw_config.get("actionType")
        method_upper = (
            method.strip().upper()
            if isinstance(method, str) and method.strip()
            else None
        )
        return ("REST Client connector-action", method_upper)

    return (None, None)


def _classify_profile(comp: IntegrationComponentSpec) -> Optional[str]:
    """Return ``"profile.db"`` / ``"profile.json"`` / ``"profile.xml"`` / None."""
    effective = _effective_component_type(comp)
    if effective in ("profile.db", "profile.json", "profile.xml"):
        return effective
    return None


def _format_actual_role(comp: IntegrationComponentSpec) -> str:
    """Short human label for ``details.actual_role`` in mismatch errors.

    Avoids echoing config values or secrets — uses only the structural
    type / family classification.
    """
    effective = _effective_component_type(comp) or "<unknown>"
    if effective == "connector-settings":
        family = _classify_connector_settings(comp)
        return family if family is not None else "connector-settings (unknown family)"
    if effective == "connector-action":
        role, method = _classify_connector_action(comp)
        if role is None:
            return "connector-action (unknown family)"
        if method:
            return f"{role} [{method}]"
        return role
    return effective


# Issue #26: resolve the field index for a transform.map's source / target
# profile reference. Returns None when the reference is a literal UUID or
# points at an unknown / non-profile component — in those cases the map
# builder's validator raises MAP_PROFILE_INDEX_UNAVAILABLE.
def _resolve_map_profile_index(
    profile_id: Any,
    components_by_key: Optional[Dict[str, "IntegrationComponentSpec"]],
) -> Optional[Dict[str, Dict[str, Any]]]:
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
    # profile that supplies only the top-level IntegrationComponentSpec.name
    # (and omits config.component_name) is valid — _execute_component
    # injects the default before invoking the builder. Without the same
    # injection here, the validate_config inside build_field_index would
    # fail with "component_name is required" and the map would erroneously
    # surface MAP_PROFILE_INDEX_UNAVAILABLE. Codex r1 P2 finding #2.
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
        # crash the plan loop. Map validation will surface MAP_PROFILE_INDEX_UNAVAILABLE.
        return None


def _check_database_get_dependencies(
    comp: IntegrationComponentSpec,
    raw_config: Dict[str, Any],
    components_by_key: Optional[Dict[str, IntegrationComponentSpec]] = None,
) -> Optional[BuilderValidationError]:
    """Cross-step dependency checks specific to database Get operations.

    Boomi binds a connection to an operation at the process connector step,
    not in the operation XML — so the connection ID is never embedded. But
    plan-time we still need the caller to declare both dependencies via
    `connection_ref_key` + `depends_on` (for connection) and `read_profile_id`
    + `depends_on` (when read_profile_id is a `$ref:KEY` token), otherwise
    the apply ordering would be unsafe.
    """
    depends_on = set(comp.depends_on or [])

    connection_ref_key = raw_config.get("connection_ref_key")
    if not connection_ref_key or not str(connection_ref_key).strip():
        return BuilderValidationError(
            "connection_ref_key is required for database Get operations",
            error_code="MISSING_DB_DEPENDENCY",
            field="connection_ref_key",
            hint=(
                "Declare the database connector-settings key the operation "
                "will bind to at process time, and add the same key to "
                "depends_on so plan ordering is correct."
            ),
        )
    if connection_ref_key not in depends_on:
        return BuilderValidationError(
            f"connection_ref_key {connection_ref_key!r} must also appear in depends_on",
            error_code="MISSING_DB_DEPENDENCY",
            field="depends_on",
            hint=(
                "Add the connector-settings key to depends_on so the "
                "execution order creates the connection before the operation."
            ),
        )

    # Issue #49: cross-component type check. Skip outside-spec refs
    # (components_by_key.get returns None for direct UUIDs / live IDs).
    if components_by_key is not None:
        target = components_by_key.get(connection_ref_key)
        if target is not None and _classify_connector_settings(target) != "database connector-settings":
            actual_role = _format_actual_role(target)
            return BuilderValidationError(
                f"connection_ref_key {connection_ref_key!r} must reference a "
                f"database connector-settings component (got {actual_role})",
                error_code="DB_REF_TYPE_MISMATCH",
                field="connection_ref_key",
                hint=(
                    "Point connection_ref_key at the database "
                    "connector-settings key; profile and connector-action "
                    "keys are not valid here."
                ),
                details={
                    "ref_key": connection_ref_key,
                    "expected_role": "database connector-settings",
                    "actual_role": actual_role,
                },
            )

    read_profile_id = raw_config.get("read_profile_id")
    if isinstance(read_profile_id, str) and read_profile_id.startswith("$ref:"):
        ref_key = read_profile_id[5:]
        if not ref_key:
            return BuilderValidationError(
                "read_profile_id $ref token is empty (expected '$ref:KEY')",
                error_code="MISSING_DB_READ_PROFILE_REF",
                field="read_profile_id",
                hint=(
                    "Use '$ref:db_read_profile' to reference a profile.db "
                    "component created earlier in the same integration spec."
                ),
            )
        if ref_key not in depends_on:
            return BuilderValidationError(
                f"read_profile_id $ref target {ref_key!r} must also appear in depends_on",
                error_code="MISSING_DB_DEPENDENCY",
                field="depends_on",
                hint=(
                    "Add the read profile key to depends_on so the execution "
                    "order creates the profile before the operation."
                ),
            )
        if components_by_key is not None:
            target = components_by_key.get(ref_key)
            if target is not None and _classify_profile(target) != "profile.db":
                actual_role = _format_actual_role(target)
                return BuilderValidationError(
                    f"read_profile_id $ref target {ref_key!r} must reference a "
                    f"profile.db component (got {actual_role})",
                    error_code="DB_REF_TYPE_MISMATCH",
                    field="read_profile_id",
                    hint=(
                        "Point read_profile_id at a profile.db component "
                        "declared earlier in the spec."
                    ),
                    details={
                        "ref_key": ref_key,
                        "expected_role": "profile.db",
                        "actual_role": actual_role,
                    },
                )

    return None


def _check_rest_operation_dependencies(
    comp: IntegrationComponentSpec,
    raw_config: Dict[str, Any],
    components_by_key: Optional[Dict[str, IntegrationComponentSpec]] = None,
) -> Optional[BuilderValidationError]:
    """Cross-step dependency checks specific to REST Client operations (issue #24).

    Boomi binds a REST connection to an operation at the process connector
    step, not in the operation XML — so the connection ID is never embedded.
    Plan-time we still need the caller to declare:
      * the connection (`connection_ref_key` + `depends_on`),
      * any referenced profiles via `$ref:KEY` tokens
        (`request_profile_id` AND `response_profile_id` — codex review item
        #3 against the superseded HTTP implementation),
      * any payload-source upstream step (`payload_source_ref_key`).

    Without these, apply-time ordering would be unsafe (operation runs before
    its inputs exist or before `_resolve_dependency_tokens` can substitute
    the `$ref` into a real component_id).
    """
    depends_on = set(comp.depends_on or [])

    connection_ref_key = raw_config.get("connection_ref_key")
    if not connection_ref_key or not str(connection_ref_key).strip():
        return BuilderValidationError(
            "connection_ref_key is required for REST operations",
            error_code="REST_CONNECTION_REF_REQUIRED",
            field="connection_ref_key",
            hint=(
                "Declare the REST connector-settings key the operation will "
                "bind to at process time, and add the same key to depends_on "
                "so plan ordering is correct."
            ),
        )
    if connection_ref_key not in depends_on:
        return BuilderValidationError(
            f"connection_ref_key {connection_ref_key!r} must also appear in depends_on",
            error_code="REST_DEPENDENCY_REQUIRED",
            field="depends_on",
            hint=(
                "Add the connector-settings key to depends_on so the execution "
                "order creates the connection before the operation."
            ),
        )

    # Issue #49: cross-component type check. Skip outside-spec refs.
    if components_by_key is not None:
        target = components_by_key.get(connection_ref_key)
        if target is not None and _classify_connector_settings(target) != "REST Client connector-settings":
            actual_role = _format_actual_role(target)
            return BuilderValidationError(
                f"connection_ref_key {connection_ref_key!r} must reference a "
                f"REST Client connector-settings component (got {actual_role})",
                error_code="REST_REF_TYPE_MISMATCH",
                field="connection_ref_key",
                hint=(
                    "Point connection_ref_key at the REST Client "
                    "connector-settings key; profile and connector-action "
                    "keys are not valid here."
                ),
                details={
                    "ref_key": connection_ref_key,
                    "expected_role": "REST Client connector-settings",
                    "actual_role": actual_role,
                },
            )

    for ref_field in ("request_profile_id", "response_profile_id"):
        value = raw_config.get(ref_field)
        if isinstance(value, str) and value.startswith("$ref:"):
            ref_key = value[5:]
            if not ref_key:
                return BuilderValidationError(
                    f"{ref_field} $ref token is empty (expected '$ref:KEY')",
                    error_code="REST_PROFILE_REF_UNRESOLVED",
                    field=ref_field,
                    hint=(
                        f"Use '$ref:<profile key>' to reference a profile "
                        "component declared earlier in the same integration spec."
                    ),
                )
            if ref_key not in depends_on:
                return BuilderValidationError(
                    f"{ref_field} $ref target {ref_key!r} must also appear in depends_on",
                    error_code="REST_DEPENDENCY_REQUIRED",
                    field="depends_on",
                    hint=(
                        "Add the profile key to depends_on so the execution "
                        "order creates the profile before the operation."
                    ),
                )
            if components_by_key is not None:
                target = components_by_key.get(ref_key)
                if target is not None and _classify_profile(target) not in ("profile.json", "profile.xml"):
                    actual_role = _format_actual_role(target)
                    return BuilderValidationError(
                        f"{ref_field} $ref target {ref_key!r} must reference a "
                        f"profile.json or profile.xml component (got {actual_role})",
                        error_code="REST_REF_TYPE_MISMATCH",
                        field=ref_field,
                        hint=(
                            "Point the profile ref at a profile.json or "
                            "profile.xml component; profile.db, "
                            "connector-settings, and connector-action "
                            "are not valid here."
                        ),
                        details={
                            "ref_key": ref_key,
                            "expected_role": "profile.json or profile.xml",
                            "actual_role": actual_role,
                        },
                    )

    payload_source_ref_key = raw_config.get("payload_source_ref_key")
    if (
        payload_source_ref_key
        and isinstance(payload_source_ref_key, str)
        and payload_source_ref_key.strip()
        and payload_source_ref_key not in depends_on
    ):
        return BuilderValidationError(
            f"payload_source_ref_key {payload_source_ref_key!r} must also appear in depends_on",
            error_code="REST_DEPENDENCY_REQUIRED",
            field="depends_on",
            hint=(
                "Add the payload source key to depends_on so the execution "
                "order creates the payload-producing step before the operation."
            ),
        )

    return None


def _check_process_flow_ref_types(
    comp: IntegrationComponentSpec,
    raw_config: Dict[str, Any],
    components_by_key: Dict[str, IntegrationComponentSpec],
) -> Optional[BuilderValidationError]:
    """Type-check $ref:KEY tokens in a structured process-flow config (issue #49).

    Returns None when all in-spec refs match the expected role for their slot.
    Direct UUID / literal component-id values are skipped (outside-spec —
    cannot be classified locally per the issue #49 non-goals).

    Runs only after ``ProcessFlowBuilder.validate_config`` has already
    confirmed the structural shape and depends_on reachability. Any padded
    ``" $ref:KEY "`` variants were rejected upstream as MISSING_PROCESS_DEPENDENCY,
    so this helper only needs to handle exact ``"$ref:KEY"`` strings.
    """
    source = raw_config.get("source") if isinstance(raw_config.get("source"), dict) else {}
    target = raw_config.get("target") if isinstance(raw_config.get("target"), dict) else {}

    slot_rules = (
        ("source.connection_id", source.get("connection_id"), "database connector-settings"),
        ("source.operation_id", source.get("operation_id"), "database connector-action Get"),
        ("target.connection_id", target.get("connection_id"), "REST Client connector-settings"),
        ("target.operation_id", target.get("operation_id"), "REST Client connector-action"),
    )

    target_op_ref_component: Optional[IntegrationComponentSpec] = None

    for field_path, raw_value, expected_role in slot_rules:
        if not (isinstance(raw_value, str) and raw_value.startswith("$ref:")):
            continue
        ref_key = raw_value[5:]
        if not ref_key:
            continue
        target_comp = components_by_key.get(ref_key)
        if target_comp is None:
            continue

        if expected_role == "database connector-settings":
            ok = _classify_connector_settings(target_comp) == expected_role
        elif expected_role == "database connector-action Get":
            role, _ = _classify_connector_action(target_comp)
            ok = role == expected_role
        elif expected_role == "REST Client connector-settings":
            ok = _classify_connector_settings(target_comp) == expected_role
        elif expected_role == "REST Client connector-action":
            role, _ = _classify_connector_action(target_comp)
            ok = role == expected_role
            if ok:
                target_op_ref_component = target_comp
        else:
            ok = True

        if not ok:
            actual_role = _format_actual_role(target_comp)
            return BuilderValidationError(
                f"{field_path} {raw_value!r} must reference a {expected_role} "
                f"component (got {actual_role})",
                error_code="PROCESS_REF_TYPE_MISMATCH",
                field=field_path,
                hint=(
                    "Point this $ref at a component whose declared role "
                    "matches the expected_role; swapped refs and unrelated "
                    "component types are rejected at plan time."
                ),
                details={
                    "ref_key": ref_key,
                    "expected_role": expected_role,
                    "actual_role": actual_role,
                },
            )

    # Optional method/action_type consistency check (issue #49): when
    # target.operation_id resolves to an in-spec REST operation that
    # carries a declared method, target.action_type must match it
    # uppercased. Skip silently when the referenced operation has no
    # declared method (cannot compare against unknown).
    if target_op_ref_component is not None:
        _, declared_method = _classify_connector_action(target_op_ref_component)
        declared_action_type = target.get("action_type")
        declared_action_upper = (
            declared_action_type.strip().upper()
            if isinstance(declared_action_type, str) and declared_action_type.strip()
            else None
        )
        if (
            declared_method is not None
            and declared_action_upper is not None
            and declared_method != declared_action_upper
        ):
            target_op_raw = target.get("operation_id")
            target_op_ref_key = (
                target_op_raw[5:]
                if isinstance(target_op_raw, str) and target_op_raw.startswith("$ref:")
                else ""
            )
            return BuilderValidationError(
                f"target.action_type {declared_action_upper!r} does not match "
                f"the method {declared_method!r} declared on the referenced "
                f"REST operation",
                error_code="PROCESS_REF_TYPE_MISMATCH",
                field="target.action_type",
                hint=(
                    "Align target.action_type with the HTTP method declared "
                    "on the referenced REST connector-action, or change the "
                    "referenced operation to one whose method matches."
                ),
                details={
                    "ref_key": target_op_ref_key,
                    "expected_role": declared_method,
                    "actual_role": declared_action_upper,
                },
            )

    return None


# REST config fields known to carry secret/credential-like values. When ANY
# REST validation error fires, these paths are scrubbed from the plan echo
# regardless of which validator won — otherwise an earlier failing check
# (missing connection_ref_key, missing base_url, etc.) leaves the sensitive
# data unredacted (codex review item P1, round-6). Paths are dotted to match
# `_redact_dotted_field_path`'s contract.
_REST_SENSITIVE_FIELD_PATHS = (
    "oauth2.client_secret",         # also caught by FORBIDDEN_SECRET_FIELDS
    "oauth2.client_secret_ref",     # raw value when it should be credential://
    "credential_ref",               # raw value when it should be credential://
    "request_headers",              # whole dict — Authorization / X-API-Key etc.
    "query_parameters",             # whole dict — api_key / token in querystring
    # Codex round-3 P1: the OAuth2 parameter blocks are deferred-emission
    # (rejected by validation with UNSUPPORTED_REST_OAUTH2_PARAMETERS) but
    # callers can put arbitrary content there — `prompt=consent`,
    # `audience=...`, custom claims, anything. Scrub on the rejection path
    # so the rejected payload doesn't echo through `integration_spec`.
    "oauth2.authorization_parameters",
    "oauth2.access_token_parameters",
)

# Cert refs are handled separately by `_redact_malformed_cert_refs` (below)
# because their redaction is conditional on shape: PEM/key/garbage gets
# scrubbed, but a valid GUID cert ref MUST survive so the caller can fix
# an unrelated error from the plan output without losing the cert binding.
# Codex review round-5 P2.
_REST_CERT_REF_FIELDS = ("private_certificate_ref", "public_certificate_ref")


def _redact_malformed_cert_refs(config: Any) -> None:
    """Conditional redaction for `private_certificate_ref` /
    `public_certificate_ref`.

    Cert refs are NOT a uniformly-secret field like `credential_ref`: the
    expected value is a Boomi component-id GUID, which is itself not a
    secret. We only need to scrub the field when the caller has put
    PEM/SSH-key/garbage there instead — that material IS secret-bearing.

    Codex round-5 P2: previously the cert refs were added to
    `_REST_SENSITIVE_FIELD_PATHS` so the always-on sweep scrubbed them
    unconditionally. That over-redacted valid GUIDs when an unrelated
    field failed validation (e.g. missing base_url), making the returned
    spec unusable for correction. This helper redacts only when the
    value isn't already in the documented GUID shape.
    """
    if not isinstance(config, dict):
        return
    for field in _REST_CERT_REF_FIELDS:
        value = config.get(field)
        if value in (None, ""):
            continue
        # Valid GUID — preserve (the caller can correct other errors and
        # resubmit without re-entering the cert binding).
        if (
            isinstance(value, str)
            and RestClientConnectionBuilder._BOOMI_COMPONENT_ID_RE.match(value.strip())
        ):
            continue
        # Anything else (PEM, SSH key, non-string, malformed) is treated
        # as potential secret material and scrubbed.
        config[field] = "[REDACTED]"


def _redact_dotted_field_path(config: Any, dotted_path: Optional[str]) -> None:
    """Replace the value at a dotted path inside `config` with '[REDACTED]'.

    Targeted at field names returned by REST validation when the offending
    value isn't a forbidden-key (which `redact_forbidden_secret_fields_in_place`
    handles): e.g. `oauth2.client_secret_ref` (raw value where a
    `credential://...` ref was expected) or `request_headers` /
    `query_parameters` (entire dict carries unverified non-empty values
    that may include Authorization / X-API-Key entries).

    Defense-in-depth: if walking the dotted path finds a non-dict at an
    intermediate step (e.g. caller passed `oauth2="raw-secret"` instead
    of a sub-dict), the deep leaf can't be located but the top-level
    segment IS still leaking. Redact the top-level segment in that case
    so the raw value never echoes into the plan output. This case was
    found in codex round-2 QA (Bug #126): widening the stale-oauth2 gate
    to reject non-dict values exposed a residual redaction gap because
    the original walk-down logic silently no-op'd on non-dict
    intermediates.
    """
    if not isinstance(dotted_path, str) or not dotted_path:
        return
    if not isinstance(config, dict):
        return
    parts = dotted_path.split(".")
    cursor: Any = config
    for part in parts[:-1]:
        if not isinstance(cursor, dict):
            return
        next_cursor = cursor.get(part)
        # Malformed intermediate (non-None, non-dict) — the deep leaf
        # can't be reached but the top-level segment carries the raw
        # value. Redact at the top level and return.
        if next_cursor is not None and not isinstance(next_cursor, dict):
            top = parts[0]
            if top in config:
                config[top] = "[REDACTED]"
            return
        cursor = next_cursor
    if not isinstance(cursor, dict):
        return
    leaf = parts[-1]
    if leaf in cursor:
        cursor[leaf] = "[REDACTED]"


def _resolve_dependency_tokens(value: Any, id_registry: Dict[str, str]) -> Any:
    if isinstance(value, str):
        if value.startswith("$ref:"):
            ref_key = value[5:]
            return id_registry.get(ref_key, value)
        return value
    if isinstance(value, list):
        return [_resolve_dependency_tokens(item, id_registry) for item in value]
    if isinstance(value, dict):
        return {k: _resolve_dependency_tokens(v, id_registry) for k, v in value.items()}
    return value


def _apply_clone_suffix(comp: IntegrationComponentSpec, config: Dict[str, Any]) -> Dict[str, Any]:
    suffix = "-clone"
    cloned = dict(config)

    if comp.type == "process":
        base = cloned.get("name") or comp.name
        if base:
            cloned["name"] = f"{base}{suffix}"
        return cloned

    if comp.type in ("connector-settings", "connector-action"):
        base = cloned.get("component_name") or cloned.get("name") or comp.name
        if base:
            cloned["component_name"] = f"{base}{suffix}"
            cloned.setdefault("name", cloned["component_name"])
        return cloned

    if comp.type == "trading_partner":
        base = cloned.get("component_name") or comp.name
        if base:
            cloned["component_name"] = f"{base}{suffix}"
        return cloned

    if comp.type == "profile.db":
        # profile.db participates in metadata lookup since Issue #23 added it
        # to _METADATA_TYPE_MAP, so conflict_policy=clone is reachable. Without
        # the suffix, create_clone would produce an indistinguishable duplicate
        # that the next plan would see as ambiguous.
        base = cloned.get("component_name") or comp.name
        if base:
            cloned["component_name"] = f"{base}{suffix}"
        return cloned

    # Issue #26 + #41: generated profile.json/profile.xml, transform.map,
    # script.mapping, and synthesized transform.function wrappers all
    # participate in metadata lookup and need the clone-suffix safeguard
    # so a second plan run can't see an identical duplicate as an
    # ambiguous match.
    if comp.type in (
        "profile.json",
        "profile.xml",
        "transform.map",
        "script.mapping",
        "transform.function",
    ):
        base = cloned.get("component_name") or comp.name
        if base:
            cloned["component_name"] = f"{base}{suffix}"
        return cloned

    return cloned


def _execute_component(
    boomi_client: Boomi,
    profile: str,
    comp: IntegrationComponentSpec,
    config: Dict[str, Any],
    target_id: Optional[str] = None,
    *,
    components_by_key: Optional[Dict[str, IntegrationComponentSpec]] = None,
) -> Dict[str, Any]:
    payload = dict(config)
    # Align apply-time dispatcher predicates with plan-time predicates:
    # _build_plan keys validation off comp.type, but the create_connector /
    # create_component dispatchers branch on config["component_type"]. A
    # spec with top-level type="connector-action" or "profile.db" that omits
    # the duplicate component_type key would plan clean against the right
    # validator and then misroute at apply (Codex review items 1+2 against
    # commit f398b35).
    if comp.type in (
        "connector-settings",
        "connector-action",
        "profile.db",
        "profile.json",
        "profile.xml",
        "transform.map",
        "script.mapping",
        "transform.function",
    ):
        payload.setdefault("component_type", comp.type)
    if comp.name:
        if comp.type == "process":
            payload.setdefault("name", comp.name)
        elif comp.type in ("connector-settings", "connector-action"):
            payload.setdefault("component_name", comp.name)
            payload.setdefault("name", comp.name)
        elif comp.type == "trading_partner":
            payload.setdefault("component_name", comp.name)
        elif comp.type in (
            "profile.db",
            "profile.json",
            "profile.xml",
            "transform.map",
            "script.mapping",
            "transform.function",
        ):
            # Mirror plan-time validation, which injects comp.name into
            # effective_config["component_name"] before calling validate_config.
            # Without this, a spec with top-level name="..." but no
            # config.component_name plans clean and then fails at apply with
            # the builder's missing-name error.
            payload.setdefault("component_name", comp.name)

    if comp.type == "process":
        # process_kind=... opts into the structured process-flow builder
        # (issue #25). _build_plan has already validated config + depends_on
        # for create/create_clone/update, and rejected the
        # process_kind + raw xml combination via PROCESS_KIND_XML_CONFLICT,
        # so by the time we land here either:
        #   - process_kind is set and we build the XML
        #   - process_kind is unset and we use the legacy JSON path
        # The two are mutually exclusive at the plan layer.
        process_kind = str(
            payload.get("process_kind") or payload.get("process_type") or ""
        ).strip().lower()
        if process_kind:
            builder_cls = get_process_flow_builder(process_kind)
            if builder_cls is None:
                return {
                    "_success": False,
                    "error_code": "PROCESS_KIND_UNSUPPORTED",
                    "error": (
                        f"process_kind {process_kind!r} is not supported "
                        f"by the structured process-flow builder."
                    ),
                    "field": "process_kind",
                    "hint": (
                        f"Supported process_kind values: "
                        f"{sorted(PROCESS_FLOW_BUILDERS)}."
                    ),
                }
            try:
                # payload["name"] takes precedence so _apply_clone_suffix's
                # "<name>-clone" suffix actually reaches the emitted XML.
                # _apply_clone_suffix writes the suffixed name into
                # config["name"] (which becomes payload["name"]); if we
                # consulted comp.name first the original unsuffixed name
                # would win and the clone would emit as a name-duplicate.
                # Codex review r3 P2 (clone bypass).
                #
                # No comp.key fallback: plan-time PROCESS_NAME_REQUIRED
                # (codex review r6 P2.1) guarantees one of these two is
                # set before we get here. Falling back to comp.key would
                # silently rename the Boomi-side process to the user's
                # internal dependency token on update.
                xml = builder_cls.build(
                    payload,
                    name=payload.get("name") or comp.name,
                    folder_name=payload.get("folder_name"),
                )
            except BuilderValidationError as exc:
                return {
                    "_success": False,
                    "error_code": exc.error_code,
                    "error": str(exc),
                    "field": exc.field,
                    "hint": exc.hint,
                }
            if comp.action == "create":
                return create_component(boomi_client, profile, {"xml": xml})
            if not target_id:
                return {
                    "_success": False,
                    "error": f"Missing process_id for update of component '{comp.key}'",
                }
            return update_component(boomi_client, profile, target_id, {"xml": xml})

        if comp.action == "create":
            return create_process(boomi_client, profile, payload)
        if not target_id:
            return {"_success": False, "error": f"Missing process_id for update of component '{comp.key}'"}
        return update_process(boomi_client, profile, target_id, payload)

    if comp.type in ("connector-settings", "connector-action"):
        # Normalize local-alias connector_types to their canonical Boomi form
        # BEFORE the get_connector sanity check, so Boomi's catalog lookup
        # recognizes the type. `rest` and `rest_client` are MCP-local aliases
        # for the canonical REST Client subtype `officialboomi-X3979C-rest-prod`;
        # Boomi's API only knows the canonical. Codex review item P2 against
        # the issue-#24 REST landing.
        rest_canonical = _resolve_rest_connector_type(payload.get("connector_type"))
        if rest_canonical is not None:
            payload["connector_type"] = rest_canonical
        connector_type = payload.get("connector_type")
        if connector_type:
            try:
                boomi_client.connector.get_connector(connector_type)
            except Exception as exc:
                return {
                    "_success": False,
                    "error": f"Connector type validation failed for '{connector_type}': {exc}",
                }
        if comp.action == "create":
            return create_connector(boomi_client, profile, payload)
        if not target_id:
            return {"_success": False, "error": f"Missing component_id for update of connector '{comp.key}'"}
        return update_connector(boomi_client, profile, target_id, payload)

    if comp.type == "trading_partner":
        if comp.action == "create":
            return create_trading_partner(boomi_client, profile, payload)
        if not target_id:
            return {"_success": False, "error": f"Missing component_id for update of trading partner '{comp.key}'"}
        return update_trading_partner(boomi_client, profile, target_id, payload)

    # Issue #41 r3: synthesized transform.function wrappers route through
    # TransformFunctionWrapperBuilder. End users do not typically author
    # these directly — they're materialized by _synthesize_script_function_wrappers
    # — but the apply path handles them like any other in-spec component.
    if comp.type == "transform.function" and not payload.get("xml"):
        wrapper_cls = get_transform_function_wrapper_builder(comp.type)
        if wrapper_cls is None:
            return {
                "_success": False,
                "error_code": "SCRIPT_MAPPING_VALIDATION_FAILED",
                "error": (
                    f"No TransformFunctionWrapperBuilder registered for "
                    f"{comp.type!r}."
                ),
                "field": "component_type",
            }
        try:
            built_xml = wrapper_cls().build(**payload)
        except BuilderValidationError as exc:
            return {
                "_success": False,
                "error_code": exc.error_code,
                "error": str(exc),
                "field": exc.field,
                "hint": exc.hint,
            }
        envelope = {"xml": built_xml, "component_type": "transform.function"}
        if comp.action == "create":
            return create_component(boomi_client, profile, envelope)
        if not target_id:
            return {
                "_success": False,
                "error": (
                    f"Missing component_id for update of wrapper '{comp.key}'"
                ),
            }
        return update_component(boomi_client, profile, target_id, envelope)

    # Issue #41: structured script.mapping routes through ScriptMappingBuilder.
    # Raw-XML bypass preserved — when payload['xml'] is set, the build()
    # call is skipped and the raw XML is used verbatim by create_component.
    if comp.type == "script.mapping" and not payload.get("xml"):
        builder_class = get_script_mapping_builder(comp.type)
        if builder_class is None:
            return {
                "_success": False,
                "error_code": "SCRIPT_MAPPING_VALIDATION_FAILED",
                "error": (
                    f"No ScriptMappingBuilder registered for {comp.type!r}."
                ),
                "field": "component_type",
            }
        try:
            built_xml = builder_class().build(**payload)
        except BuilderValidationError as exc:
            return {
                "_success": False,
                "error_code": exc.error_code,
                "error": str(exc),
                "field": exc.field,
                "hint": exc.hint,
            }
        envelope = {"xml": built_xml, "component_type": "script.mapping"}
        if comp.action == "create":
            return create_component(boomi_client, profile, envelope)
        if not target_id:
            return {
                "_success": False,
                "error": (
                    f"Missing component_id for update of script '{comp.key}'"
                ),
            }
        return update_component(boomi_client, profile, target_id, envelope)

    # Issue #26: generated profile.json / profile.xml route through the
    # profile-builder registry. Raw-XML bypass is preserved — when
    # payload['xml'] is set, the build() call is skipped and the raw XML
    # is used verbatim by create_component / update_component.
    if comp.type in ("profile.json", "profile.xml") and not payload.get("xml"):
        profile_type = (payload.get("profile_type") or "").lower()
        builder_instance = get_profile_builder(comp.type, profile_type)
        if builder_instance is None:
            return {
                "_success": False,
                "error_code": "UNSUPPORTED_PROFILE_GENERATION_MODE",
                "error": (
                    f"profile_type {profile_type!r} is not supported for "
                    f"{comp.type}."
                ),
                "field": "profile_type",
            }
        try:
            built_xml = builder_instance.build(**payload)
        except BuilderValidationError as exc:
            return {
                "_success": False,
                "error_code": exc.error_code,
                "error": str(exc),
                "field": exc.field,
                "hint": exc.hint,
            }
        envelope = {"xml": built_xml, "component_type": comp.type}
        if comp.action == "create":
            return create_component(boomi_client, profile, envelope)
        if not target_id:
            return {
                "_success": False,
                "error": (
                    f"Missing component_id for update of profile '{comp.key}'"
                ),
            }
        return update_component(boomi_client, profile, target_id, envelope)

    # Issue #26: transform.map routes through the direct-map builder. Source
    # and target field indexes are computed from the in-spec profile
    # components referenced by source_profile_id / target_profile_id ($ref:KEY).
    # The resolved config already has $ref:KEY substituted for real Boomi
    # UUIDs by _resolve_dependency_tokens — but to find the in-spec profile
    # component for index computation, we need the ORIGINAL comp.config
    # (where $ref:KEY is still a $ref:KEY string).
    if comp.type == "transform.map" and not payload.get("xml"):
        map_type = (payload.get("map_type") or "").lower()
        map_builder_instance = get_map_builder(comp.type, map_type)
        if map_builder_instance is None:
            return {
                "_success": False,
                "error_code": "UNSUPPORTED_TRANSFORM_ROUTE",
                "error": (
                    f"map_type {map_type!r} is not supported for transform.map. "
                    "Supported: direct, function, map_function, script, "
                    "map_script."
                ),
                "field": "map_type",
                "hint": (
                    "Use map_type='direct' for profile-to-profile direct "
                    "field mappings; map_type='function' for structured "
                    "map-function primitives (#40); map_type='script' for "
                    "in-map calls to reusable script.mapping components (#41)."
                ),
            }
        raw_comp_config = comp.config or {}
        source_index = _resolve_map_profile_index(
            raw_comp_config.get("source_profile_id"),
            components_by_key,
        )
        target_index = _resolve_map_profile_index(
            raw_comp_config.get("target_profile_id"),
            components_by_key,
        )
        if source_index is None or target_index is None:
            return {
                "_success": False,
                "error_code": "MAP_PROFILE_INDEX_UNAVAILABLE",
                "error": (
                    "Cannot compute source/target field index from in-spec "
                    "profile components. Literal existing-profile UUIDs are "
                    "not indexable in M2 (#47 owns existing-profile discovery)."
                ),
                "hint": (
                    "Reference both source and target profiles as in-spec "
                    "components via '$ref:KEY'."
                ),
            }
        try:
            built_xml = map_builder_instance.build(
                source_index=source_index,
                target_index=target_index,
                **payload,
            )
        except BuilderValidationError as exc:
            return {
                "_success": False,
                "error_code": exc.error_code,
                "error": str(exc),
                "field": exc.field,
                "hint": exc.hint,
            }
        envelope = {"xml": built_xml, "component_type": "transform.map"}
        if comp.action == "create":
            return create_component(boomi_client, profile, envelope)
        if not target_id:
            return {
                "_success": False,
                "error": (
                    f"Missing component_id for update of map '{comp.key}'"
                ),
            }
        return update_component(boomi_client, profile, target_id, envelope)

    if comp.action == "create":
        return create_component(boomi_client, profile, payload)
    if not target_id:
        return {"_success": False, "error": f"Missing component_id for update of component '{comp.key}'"}
    return update_component(boomi_client, profile, target_id, payload)


def _build_plan(boomi_client: Boomi, config: Dict[str, Any]) -> Dict[str, Any]:
    spec = _normalize_to_spec(config)
    # Issue #41 r3: inject transform.function wrappers between any
    # transform.map (map_type='script') and the script.mapping it
    # references. Live Boomi requires the indirection — see
    # _synthesize_script_function_wrappers docstring.
    _synthesize_script_function_wrappers(spec)
    conflict_policy = (config.get("conflict_policy") or "reuse").lower()
    if conflict_policy not in ("reuse", "clone", "fail"):
        return {
            "_success": False,
            "error": f"Invalid conflict_policy '{conflict_policy}'. Valid values: reuse, clone, fail.",
        }

    try:
        execution_order = _topological_order(spec)
    except ValueError as exc:
        return {"_success": False, "error": str(exc)}

    components_by_key = {comp.key: comp for comp in spec.components}
    steps: List[Dict[str, Any]] = []
    warnings: List[str] = []

    for key in execution_order:
        comp = components_by_key[key]

        # If the caller supplied an explicit component_id, skip ambiguity checking
        if comp.component_id:
            candidates: List[Dict[str, Any]] = []
            existing_id: Optional[str] = comp.component_id
        else:
            candidates = _resolve_existing_components(boomi_client, comp)
            existing_id = candidates[0].get("component_id") if len(candidates) == 1 else None

        planned_action = comp.action

        if comp.action == "create":
            if len(candidates) > 1:
                if conflict_policy == "clone":
                    # Clone creates a new component with a suffix — no targeting risk.
                    # Set existing_id so _apply_plan enters the clone-suffix branch.
                    planned_action = "create_clone"
                    existing_id = candidates[0].get("component_id")
                else:
                    planned_action = "error_ambiguous_match"
            elif len(candidates) == 1:
                if conflict_policy == "reuse":
                    planned_action = "reuse"
                elif conflict_policy == "clone":
                    planned_action = "create_clone"
                else:
                    planned_action = "error_if_exists"

        elif comp.action == "update" and not comp.component_id:
            if len(candidates) > 1:
                planned_action = "error_ambiguous_match"
            elif len(candidates) == 0:
                planned_action = "error_missing_target"

        # Process components opt into the structured process-flow builder
        # via config.process_kind (or config.process_type). Without it,
        # processes fall through to the legacy linear JSON-to-XML path
        # in create_process. process_flow_xml is the new structured route
        # added by issue #25 (M2.5).
        raw_config = comp.config or {}
        # str() coercion guards against non-string process_kind (e.g. 123)
        # before .strip(). The builder's validate_config does the same; this
        # site runs FIRST for route selection so it has to coerce too.
        # Codex review L1 / QA bug #128.
        process_kind = (
            str(raw_config.get("process_kind") or raw_config.get("process_type") or "")
            .strip()
            .lower()
        ) if comp.type == "process" else ""

        route = (
            "process_flow_xml"
            if comp.type == "process" and process_kind
            else "process_json_to_xml"
            if comp.type == "process"
            else "connector_builder_or_xml"
            if comp.type in ("connector-settings", "connector-action")
            else "trading_partner_json"
            if comp.type == "trading_partner"
            else "profile_builder_or_xml"
            if comp.type in ("profile.db", "profile.json", "profile.xml")
            else "map_builder_or_xml"
            if comp.type == "transform.map"
            else "generic_component_xml"
        )

        # Database connector-settings preflight. Two-tier validation:
        #
        # (a) scan_forbidden_secret_fields runs on EVERY database
        #     connector-settings step regardless of apply path. Plan output
        #     dumps comp.config verbatim, so a plaintext password in a
        #     reuse/update/raw-XML config would leak into the response even
        #     though apply itself wouldn't use it. We also infer "database"
        #     from raw XML subType when connector_type is omitted —
        #     create_connector's raw-XML path doesn't require connector_type.
        #
        # (b) validate_config (driver, auth, credential_ref, required fields)
        #     runs only when the apply path will actually invoke
        #     DatabaseConnectorBuilder.build(). Reuse short-circuits
        #     (_apply_plan line ~547), update goes through update_connector,
        #     and config.xml bypasses the builder in create_connector.
        #     Validating those paths would block legitimate plans. Mirror
        #     _execute_component's defaulting (component_name from comp.name).
        validation_error: Optional[Dict[str, Any]] = None
        raw_config = comp.config or {}
        xml_payload = raw_config.get("xml") or ""
        xml_says_database = bool(
            xml_payload and _XML_DATABASE_SUBTYPE_RE.search(xml_payload)
        )
        is_database_connector_settings = (
            comp.type == "connector-settings"
            and (
                raw_config.get("connector_type") == "database"
                or xml_says_database
            )
        )
        # Every profile.db component is a builder candidate (regardless of
        # profile_type value). The builder validator surfaces the right
        # structured error — UNSUPPORTED_DB_PROFILE_MODE for missing/blank
        # profile_type, MISSING_DB_QUERY for missing SQL, etc. Without this
        # widening, a malformed profile.db without profile_type would plan
        # as a clean `create` and leak any secret-shaped fields into the
        # plan echo (Codex review item #3).
        is_database_read_profile = (comp.type == "profile.db")
        # Every connector-action with connector_type='database' is a builder
        # candidate (regardless of operation_mode value). The validator
        # returns UNSUPPORTED_DB_OPERATION_MODE for send/upsert/missing, so
        # unknown modes can't slip through as clean `create` plans with
        # un-redacted secret echoes (Codex review item #4).
        is_database_get_operation = (
            comp.type == "connector-action"
            and (raw_config.get("connector_type") or "").lower() == "database"
        )
        will_invoke_builder = (
            (is_database_connector_settings
             or is_database_read_profile
             or is_database_get_operation)
            and not xml_payload
            and planned_action in ("create", "create_clone")
        )
        db_err: Optional[BuilderValidationError] = None
        secret_scanner_cls = None
        if is_database_connector_settings:
            secret_scanner_cls = DatabaseConnectorBuilder
        elif is_database_read_profile:
            secret_scanner_cls = DatabaseReadProfileBuilder
        elif is_database_get_operation:
            secret_scanner_cls = DatabaseGetOperationBuilder
        if secret_scanner_cls is not None:
            db_err = secret_scanner_cls.scan_forbidden_secret_fields(raw_config)
            if db_err is None and will_invoke_builder:
                effective_config = dict(raw_config)
                if comp.name:
                    effective_config.setdefault("component_name", comp.name)
                if is_database_connector_settings:
                    db_err = DatabaseConnectorBuilder.validate_config(effective_config)
                elif is_database_read_profile:
                    # Dispatch to the right profile builder via the registry.
                    # Select (database.read) and Stored Procedure
                    # (database.stored_procedure_read) share the same secret-
                    # scan contract but have statement-specific validation
                    # rules. If profile_type is missing/unknown, surface a
                    # unified UNSUPPORTED_DB_PROFILE_MODE error that lists
                    # all supported protocols.
                    profile_type = (effective_config.get("profile_type") or "").lower()
                    builder_instance = get_profile_builder("profile.db", profile_type)
                    if builder_instance is None:
                        valid = sorted({
                            pt for (ct, pt) in PROFILE_BUILDERS if ct == "profile.db"
                        })
                        db_err = BuilderValidationError(
                            f"profile_type {profile_type!r} is not supported "
                            f"for profile.db. Supported: {', '.join(valid)}.",
                            error_code="UNSUPPORTED_DB_PROFILE_MODE",
                            field="profile_type",
                            hint=(
                                "Use one of the supported profile_type values "
                                "(database.read for Select-statement profiles, "
                                "database.stored_procedure_read for Stored "
                                "Procedure profiles). Write profiles are "
                                "tracked by issue #32."
                            ),
                        )
                    else:
                        db_err = type(builder_instance).validate_config(effective_config)
                elif is_database_get_operation:
                    db_err = DatabaseGetOperationBuilder.validate_config(effective_config)
                    # Cross-step dependency checks only apply to the
                    # supported Get path — for unsupported modes (send,
                    # upsert, missing), validate_config above returns first
                    # with UNSUPPORTED_DB_OPERATION_MODE.
                    if db_err is None:
                        db_err = _check_database_get_dependencies(
                            comp, raw_config, components_by_key
                        )
        if db_err is not None:
            planned_action = "error_database_validation"
            validation_error = {
                "error_code": db_err.error_code,
                "error": str(db_err),
                "field": db_err.field,
                "hint": db_err.hint,
            }
            if db_err.details is not None:
                validation_error["details"] = db_err.details
            # Scrub EVERY plaintext secret-shaped field from the spec dump,
            # not just the one named in the error. scan_forbidden_secret_fields
            # stops on first match, but a single bad config can carry multiple
            # offenders — leaving the others as plaintext would still leak.
            # Walks nested dicts (pooling, write_options, etc.) too — otherwise
            # a secret stashed inside a sub-block would still appear in the
            # plan's spec echo.
            if db_err.error_code == "PLAINTEXT_SECRET_REJECTED" and secret_scanner_cls is not None:
                secret_scanner_cls.redact_forbidden_secret_fields_in_place(
                    raw_config
                )

        # REST Client connector-settings / connector-action preflight (issue #24).
        # Mirrors the database block above:
        #   (a) scan_forbidden_secret_fields runs on EVERY REST step regardless
        #       of apply path — so reuse/update/raw-XML configs cannot leak
        #       plaintext secrets into the plan echo (including nested
        #       oauth2.client_secret via the recursive walker, codex item #1).
        #   (b) validate_config + dependency check run only when the apply
        #       path will actually invoke the builder (create / create_clone,
        #       no raw XML).
        #   (c) Raw XML without connector_type still triggers (a) when the
        #       payload carries the REST Client subType (codex item #2).
        xml_says_rest = bool(
            xml_payload and _XML_REST_SUBTYPE_RE.search(xml_payload)
        )
        is_rest_connector_settings = (
            comp.type == "connector-settings"
            and (
                _resolve_rest_connector_type(raw_config.get("connector_type")) is not None
                or xml_says_rest
            )
        )
        is_rest_send_operation = (
            comp.type == "connector-action"
            and (
                _resolve_rest_connector_type(raw_config.get("connector_type")) is not None
                or xml_says_rest
            )
        )
        will_invoke_rest_builder = (
            (is_rest_connector_settings or is_rest_send_operation)
            and not xml_payload
            and planned_action in ("create", "create_clone")
        )
        rest_err: Optional[BuilderValidationError] = None
        rest_scanner_cls = None
        if is_rest_connector_settings:
            rest_scanner_cls = RestClientConnectionBuilder
        elif is_rest_send_operation:
            rest_scanner_cls = RestClientOperationBuilder

        if rest_scanner_cls is not None and db_err is None:
            rest_err = rest_scanner_cls.scan_forbidden_secret_fields(raw_config)
            if rest_err is None and will_invoke_rest_builder:
                effective_config = dict(raw_config)
                if comp.name:
                    effective_config.setdefault("component_name", comp.name)
                if is_rest_connector_settings:
                    rest_err = RestClientConnectionBuilder.validate_config(effective_config)
                else:  # is_rest_send_operation
                    rest_err = RestClientOperationBuilder.validate_config(effective_config)
                    if rest_err is None:
                        rest_err = _check_rest_operation_dependencies(
                            comp, raw_config, components_by_key
                        )

        if rest_err is not None:
            planned_action = "error_rest_validation"
            validation_error = {
                "error_code": rest_err.error_code,
                "error": str(rest_err),
                "field": rest_err.field,
                "hint": rest_err.hint,
            }
            if rest_err.details is not None:
                validation_error["details"] = rest_err.details
            if rest_err.error_code == "PLAINTEXT_SECRET_REJECTED" and rest_scanner_cls is not None:
                rest_scanner_cls.redact_forbidden_secret_fields_in_place(
                    raw_config
                )
            # Any REST validation error must scrub the documented sensitive
            # fields, not just the one named in the winning error. Without
            # this, a sensitive value (Authorization header, raw
            # client_secret_ref, raw credential_ref, populated
            # query_parameters) leaks into the plan echo when an EARLIER
            # validator (e.g. missing connection_ref_key, missing base_url)
            # fires first. Codex review item P1 round-6.
            for sensitive_path in _REST_SENSITIVE_FIELD_PATHS:
                _redact_dotted_field_path(raw_config, sensitive_path)
            # Cert refs: conditional redaction — scrub PEM/key material but
            # preserve valid GUIDs so the caller can correct unrelated
            # errors without losing the cert binding. Codex review round-5 P2.
            _redact_malformed_cert_refs(raw_config)

        # Process-flow builder preflight (issue #25, M2.5). Two-tier like
        # the database / REST blocks above:
        #   (a) scan_forbidden_secret_fields runs whenever process_kind is
        #       set, even on update/reuse paths — so a stray plaintext
        #       credential in process config cannot leak through the plan
        #       echo.
        #   (b) validate_config runs only when the apply path will
        #       actually invoke the builder (create / create_clone, and
        #       no raw-XML override). Unknown process_kind always fails
        #       so a typo cannot silently fall through to the legacy
        #       linear path.
        process_flow_err: Optional[BuilderValidationError] = None
        if (
            comp.type == "process"
            and process_kind
            and db_err is None
            and rest_err is None
        ):
            # Run the secret scan unconditionally. The xml-conflict check
            # below short-circuits early, so without scanning first a
            # process config like {process_kind, xml, password} would
            # surface PROCESS_KIND_XML_CONFLICT while leaving the
            # plaintext password in raw_config (== comp.config), which
            # then echoes through spec.model_dump(). Codex review r2 Q3.
            process_flow_err = ProcessFlowBuilder.scan_forbidden_secret_fields(raw_config)
            # Codex review r6 P2.1: require an explicit name. Without
            # this, _execute_component used to fall back to comp.key as
            # the emitted XML name attribute, which on update silently
            # renamed the existing process to its internal dependency
            # key (e.g. "main_process"). Reject at plan-time so the
            # caller must supply a real display name.
            if process_flow_err is None:
                config_name = raw_config.get("name")
                comp_name_clean = (
                    comp.name.strip()
                    if isinstance(comp.name, str) else ""
                )
                config_name_clean = (
                    config_name.strip()
                    if isinstance(config_name, str) else ""
                )
                effective_name = comp_name_clean or config_name_clean
                if not effective_name:
                    process_flow_err = BuilderValidationError(
                        "process component name is required for structured "
                        "process_kind components; without one the emitted "
                        "XML would carry the internal dependency key as "
                        "the display name (silent rename on update).",
                        error_code="PROCESS_NAME_REQUIRED",
                        field="name",
                        hint=(
                            "Set IntegrationComponentSpec.name or "
                            "config.name to the human-readable display "
                            "name the process should carry in Boomi."
                        ),
                    )
                # Codex review r8 F1: when BOTH surfaces are set and
                # they differ, plan-time collision lookup uses comp.name
                # but _execute_component's build() call prefers
                # payload["name"] (the r3 clone-suffix precedence).
                # That mismatch creates a duplicate on create / silently
                # renames on update because Boomi gets a different name
                # than the metadata search resolved. Reject the conflict
                # explicitly. (Apply-time _apply_clone_suffix intentionally
                # introduces a "-clone" difference; that path mutates
                # config["name"] AFTER plan, so this plan-time check
                # never sees it.)
                elif (
                    comp_name_clean
                    and config_name_clean
                    and comp_name_clean != config_name_clean
                ):
                    process_flow_err = BuilderValidationError(
                        f"top-level name {comp_name_clean!r} and "
                        f"config.name {config_name_clean!r} disagree; "
                        f"collision lookup uses the top-level name but "
                        f"the emitted XML would use config.name.",
                        error_code="PROCESS_NAME_CONFLICT",
                        field="name",
                        hint=(
                            "Either drop config.name or make it match "
                            "the top-level IntegrationComponentSpec.name. "
                            "Pick one surface so plan-time collision "
                            "detection and apply-time XML emission agree."
                        ),
                    )
            xml_override = bool(raw_config.get("xml"))
            # Codex review C4: process_kind + raw xml is ambiguous —
            # _execute_component cannot honor both, and falling through to
            # the legacy create_process path silently drops the user's XML.
            # Reject the conflict explicitly so callers must pick one.
            if process_flow_err is None and xml_override:
                process_flow_err = BuilderValidationError(
                    "process_kind and config.xml are mutually exclusive.",
                    error_code="PROCESS_KIND_XML_CONFLICT",
                    field="config.xml",
                    hint=(
                        "Choose one: process_kind for the structured "
                        "builder, OR omit process_kind and pass raw XML "
                        "to the legacy process_json_to_xml path."
                    ),
                )
            # Codex review r9: enum-membership check is a contract
            # assertion about the spec, not about the apply step. Run it
            # unconditionally so a typo like process_kind="bad" surfaces
            # even when conflict_policy=reuse finds an existing match
            # (planned_action="reuse" used to skip the whole block).
            builder_cls: Optional[type] = None
            if process_flow_err is None:
                builder_cls = get_process_flow_builder(process_kind)
                if builder_cls is None:
                    process_flow_err = BuilderValidationError(
                        f"process_kind {process_kind!r} is not supported.",
                        error_code="PROCESS_KIND_UNSUPPORTED",
                        field="process_kind",
                        hint=(
                            f"Supported process_kind values: "
                            f"{sorted(PROCESS_FLOW_BUILDERS)}."
                        ),
                    )

            # Codex review C2: process update also re-invokes the builder
            # (_execute_component → update_component({"xml": built_xml})),
            # unlike DB/REST whose update paths bypass the builder. So
            # full config validation runs on every mutating action; for
            # reuse / error_* the enum check above is enough — we won't
            # emit XML so source/target bindings don't matter.
            will_invoke_process_flow_builder = (
                process_flow_err is None
                and builder_cls is not None
                and planned_action in ("create", "create_clone", "update")
            )
            if will_invoke_process_flow_builder:
                process_flow_err = builder_cls.validate_config(
                    raw_config,
                    depends_on=comp.depends_on,
                )
                # Issue #49: after the local structural validator passes,
                # type-check every in-spec $ref:KEY against components_by_key.
                # Gated on builder_cls is ProcessFlowBuilder because the
                # source/target shape this helper reads is specific to the
                # database_to_api_sync structured process; future process_kind
                # builders will add their own ref-type helpers when they land.
                if process_flow_err is None and builder_cls is ProcessFlowBuilder:
                    process_flow_err = _check_process_flow_ref_types(
                        comp, raw_config, components_by_key
                    )

        if process_flow_err is not None:
            planned_action = "error_process_validation"
            validation_error = {
                "error_code": process_flow_err.error_code,
                "error": str(process_flow_err),
                "field": process_flow_err.field,
                "hint": process_flow_err.hint,
            }
            if process_flow_err.details is not None:
                validation_error["details"] = process_flow_err.details
            # Scrub plaintext secrets from comp.config before the spec is
            # echoed back via spec.model_dump(). Mirrors the DB/REST blocks
            # at lines ~860 and ~943 — without this, a flagged value still
            # leaks through the plan response. Codex review C1.
            if process_flow_err.error_code == "PLAINTEXT_SECRET_REJECTED":
                ProcessFlowBuilder.redact_forbidden_secret_fields_in_place(raw_config)

        # Issue #26 (M2.6): generated profile.json / profile.xml / transform.map
        # preflight. Mirrors the DB/REST/process blocks above — two-tier
        # secret scan + validate_config gated on apply path.
        gen_profile_err: Optional[BuilderValidationError] = None
        gen_profile_scanner_cls = None
        is_generated_json_profile = comp.type == "profile.json"
        is_generated_xml_profile = comp.type == "profile.xml"
        is_direct_map = comp.type == "transform.map"
        is_script_mapping_component = comp.type == "script.mapping"
        is_transform_function_wrapper = comp.type == "transform.function"
        if is_generated_json_profile:
            gen_profile_scanner_cls = JSONGeneratedProfileBuilder
        elif is_generated_xml_profile:
            gen_profile_scanner_cls = XMLGeneratedProfileBuilder
        elif is_direct_map:
            gen_profile_scanner_cls = DirectMapBuilder
        elif is_script_mapping_component:
            gen_profile_scanner_cls = ScriptMappingBuilder
        elif is_transform_function_wrapper:
            gen_profile_scanner_cls = TransformFunctionWrapperBuilder

        if (
            gen_profile_scanner_cls is not None
            and db_err is None
            and rest_err is None
            and process_flow_err is None
        ):
            # (a) Secret scan runs unconditionally so reuse/update/raw-XML
            #     configs cannot leak plaintext into the plan echo.
            gen_profile_err = gen_profile_scanner_cls.scan_forbidden_secret_fields(
                raw_config
            )
            # (b) validate_config gated on apply path actually invoking the
            #     builder. _execute_component invokes the structured builder
            #     on create / create_clone AND update (update_component({
            #     "xml": built_xml}) re-emits XML from config), so validation
            #     must cover update too — otherwise a bad structured update
            #     (e.g. JSON profile leaf with data_type='blob') plans clean
            #     and crashes apply after earlier steps have already mutated
            #     state. Codex r1 P2 finding #1.
            will_invoke_gen_profile_builder = (
                gen_profile_err is None
                and not xml_payload
                and planned_action in ("create", "create_clone", "update")
            )
            if will_invoke_gen_profile_builder:
                effective_config = dict(raw_config)
                if comp.name:
                    effective_config.setdefault("component_name", comp.name)
                # profile.json / profile.xml — straightforward validation.
                if is_generated_json_profile:
                    profile_type = (
                        (effective_config.get("profile_type") or "").lower()
                    )
                    builder_instance = get_profile_builder(
                        "profile.json", profile_type
                    )
                    if builder_instance is None:
                        gen_profile_err = BuilderValidationError(
                            f"profile_type {profile_type!r} is not supported "
                            "for profile.json. Supported: json.generated.",
                            error_code="UNSUPPORTED_PROFILE_GENERATION_MODE",
                            field="profile_type",
                            hint=(
                                "Use profile_type='json.generated' to drive "
                                "the structured JSON profile builder."
                            ),
                        )
                    else:
                        gen_profile_err = type(
                            builder_instance
                        ).validate_config(effective_config)
                elif is_generated_xml_profile:
                    profile_type = (
                        (effective_config.get("profile_type") or "").lower()
                    )
                    builder_instance = get_profile_builder(
                        "profile.xml", profile_type
                    )
                    if builder_instance is None:
                        gen_profile_err = BuilderValidationError(
                            f"profile_type {profile_type!r} is not supported "
                            "for profile.xml. Supported: xml.generated.",
                            error_code="UNSUPPORTED_PROFILE_GENERATION_MODE",
                            field="profile_type",
                            hint=(
                                "Use profile_type='xml.generated' for the "
                                "element-only structured XML profile builder."
                            ),
                        )
                    else:
                        gen_profile_err = type(
                            builder_instance
                        ).validate_config(effective_config)
                elif is_direct_map:
                    # transform.map: thread source / target field indexes from
                    # the in-spec profile components so MAP_FIELD_NOT_FOUND
                    # fires at plan time when a $ref:KEY target maps to a
                    # missing leaf in the referenced profile.
                    source_index = _resolve_map_profile_index(
                        effective_config.get("source_profile_id"),
                        components_by_key,
                    )
                    target_index = _resolve_map_profile_index(
                        effective_config.get("target_profile_id"),
                        components_by_key,
                    )
                    map_type = (effective_config.get("map_type") or "").lower()
                    map_builder_instance = get_map_builder(
                        "transform.map", map_type
                    )
                    if map_builder_instance is None:
                        gen_profile_err = BuilderValidationError(
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
                    else:
                        # depends_on coverage check (Codex r1 P2 finding #4).
                        # _apply_plan's _resolve_dependency_tokens substitutes
                        # $ref:KEY from id_registry, which is populated as
                        # components apply in topological order. If a map
                        # references a profile via $ref but that profile key
                        # isn't in the map's depends_on, the topo sort may
                        # place the map before the profile and apply will
                        # crash with unresolved $ref tokens. Require explicit
                        # declaration to keep ordering safe.
                        declared_deps = set(comp.depends_on or [])
                        for side in ("source", "target"):
                            ref_value = effective_config.get(
                                f"{side}_profile_id"
                            )
                            if (
                                isinstance(ref_value, str)
                                and ref_value.startswith("$ref:")
                            ):
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
                                        details={
                                            "side": side,
                                            "ref_key": ref_key,
                                        },
                                    )
                                    break

                        # Issue #41: script_mappings[].script_component_id
                        # $ref targets must also appear in depends_on so the
                        # referenced script.mapping component applies before
                        # the calling map (same topo-sort safety rule that
                        # applies to source/target profile $refs), AND the
                        # referenced component must actually be a
                        # script.mapping — otherwise apply resolves the
                        # $ref to a non-script UUID and emits a userdefined
                        # FunctionStep whose ``id`` points at the wrong
                        # component type (Codex r1 P2 finding #4).
                        if (
                            gen_profile_err is None
                            and map_type in ("script", "map_script")
                        ):
                            sm_list = effective_config.get("script_mappings") or []
                            if isinstance(sm_list, list):
                                for sm_idx, sm in enumerate(sm_list):
                                    if not isinstance(sm, Mapping):
                                        continue
                                    ref_value = sm.get("script_component_id")
                                    # Codex r5 P1 #1: literal componentIds
                                    # (non-$ref strings) bypass wrapper
                                    # synthesis. The map's FunctionStep
                                    # ``id`` would point directly at
                                    # whatever UUID the caller supplied —
                                    # if that UUID is a script.mapping
                                    # rather than a transform.function
                                    # wrapper, Boomi cannot bind script
                                    # inputs/outputs at runtime. Reject
                                    # literal IDs until live-fetch lets
                                    # us auto-create wrappers for
                                    # existing-script reuse.
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
                                            details={
                                                "script_mappings_index": sm_idx,
                                            },
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

                                    # Reject $refs that don't target a
                                    # script.mapping or a transform.function
                                    # wrapper. After plan-time synthesis,
                                    # ``script_mappings[].script_component_id``
                                    # references the wrapper component
                                    # (type=transform.function); caller-
                                    # declared refs may still target a
                                    # script.mapping directly (synthesis
                                    # handles the rewrite). Other types
                                    # would resolve to a wrong UUID and
                                    # Boomi would fail to bind inputs/
                                    # outputs at runtime.
                                    target_comp = (
                                        components_by_key.get(ref_key)
                                        if components_by_key is not None
                                        else None
                                    )
                                    target_type = (
                                        target_comp.type
                                        if target_comp is not None
                                        else None
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
                                                "Reference an in-spec "
                                                "script.mapping component "
                                                "(auto-synth wrapper) or a "
                                                "transform.function wrapper "
                                                "by '$ref:KEY', or a literal "
                                                "existing wrapper componentId."
                                            ),
                                            details={
                                                "script_mappings_index": sm_idx,
                                                "ref_key": ref_key,
                                                "target_component_type": target_type,
                                            },
                                        )
                                        break

                                    # Codex r5 P1 #2: cross-validate the
                                    # map's port surface against the
                                    # referenced script.mapping (or
                                    # transform.function wrapper). Boomi
                                    # binds map FunctionStep inputs/outputs
                                    # to the referenced component by name
                                    # AND by position; if counts or names
                                    # diverge, the map calls the wrapper
                                    # with incompatible ports at runtime.
                                    expected_input_names = _ref_target_input_names(
                                        target_comp
                                    )
                                    expected_output_names = _ref_target_output_names(
                                        target_comp
                                    )
                                    actual_input_names = [
                                        str(entry.get("input_name") or "").strip()
                                        for entry in (sm.get("inputs") or [])
                                        if isinstance(entry, Mapping)
                                    ]
                                    actual_output_names = [
                                        str(entry.get("output_name") or "").strip()
                                        for entry in (sm.get("outputs") or [])
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
                            gen_profile_err = type(
                                map_builder_instance
                            ).validate_config(
                                effective_config,
                                source_index=source_index,
                                target_index=target_index,
                            )
                        # Codex r1 P2 finding #3: a $ref pointing at a non-
                        # profile, missing, or otherwise unindexable component
                        # produces source_index/target_index == None. The map
                        # builder's validate_config skips path-existence checks
                        # when an index is None, so without this guard the
                        # plan would succeed and apply would fail. Treat
                        # unindexable $ref refs as plan-time failures, even
                        # though their syntax (starting with $ref:) looks
                        # superficially valid.
                        if gen_profile_err is None:
                            for side, side_index in (
                                ("source", source_index),
                                ("target", target_index),
                            ):
                                ref_value = effective_config.get(
                                    f"{side}_profile_id"
                                )
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
                                        target_comp.type
                                        if target_comp is not None
                                        else None
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

                        # Literal-UUID profile refs (no $ref) can't be indexed
                        # in M2 — #47 owns existing-profile discovery. Reject
                        # at plan time so the caller knows what to fix.
                        if gen_profile_err is None:
                            for side in ("source", "target"):
                                ref_value = effective_config.get(
                                    f"{side}_profile_id"
                                )
                                if (
                                    isinstance(ref_value, str)
                                    and not ref_value.startswith("$ref:")
                                ):
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
                                            f"reference it via '$ref:KEY', "
                                            "or wait for issue #47 "
                                            "(existing-profile schema "
                                            "discovery)."
                                        ),
                                        details={"side": side},
                                    )
                                    break
                elif is_script_mapping_component:
                    # Issue #41: script.mapping has no source/target profile
                    # refs to thread — it is profile-agnostic. Just run the
                    # builder's structured config validator.
                    gen_profile_err = ScriptMappingBuilder.validate_config(
                        effective_config
                    )
                elif is_transform_function_wrapper:
                    # Issue #41 r3: transform.function wrappers are auto-
                    # synthesized by _synthesize_script_function_wrappers,
                    # but plan-time validation still runs through the
                    # builder's validate_config to catch any caller-
                    # declared wrappers and to defend against synthesis
                    # bugs (defense-in-depth).
                    gen_profile_err = TransformFunctionWrapperBuilder.validate_config(
                        effective_config
                    )
                    # Codex r4 P2: caller-declared wrappers must satisfy
                    # the same depends_on + target-type checks the map-
                    # side script_mappings refs already enforce. Without
                    # this, a wrapper with script_component_id='$ref:KEY'
                    # where KEY is missing from depends_on can plan
                    # before the script applies (topo break → unresolved
                    # $ref at apply), and a $ref pointing at a profile
                    # would resolve to the wrong UUID and emit a
                    # <Scripting componentId='<profile-uuid>'/> that
                    # fails at Boomi runtime. Auto-synthesized wrappers
                    # satisfy these checks by construction; the cost is
                    # negligible to run them for every wrapper.
                    if gen_profile_err is None:
                        ref_value = effective_config.get("script_component_id")
                        if (
                            isinstance(ref_value, str)
                            and ref_value.startswith("$ref:")
                        ):
                            ref_key = ref_value[len("$ref:") :]
                            declared_deps = set(comp.depends_on or [])
                            if ref_key not in declared_deps:
                                gen_profile_err = BuilderValidationError(
                                    f"transform.function wrapper "
                                    f"script_component_id $ref target "
                                    f"{ref_key!r} must also appear in "
                                    "depends_on so the referenced "
                                    "script.mapping applies before the "
                                    "wrapper",
                                    error_code="SCRIPT_MAPPING_REF_REQUIRED",
                                    field="depends_on",
                                    hint=(
                                        "Add the script.mapping component "
                                        "key to the wrapper's depends_on "
                                        "so the execution order builds "
                                        "the script component before the "
                                        "wrapper."
                                    ),
                                    details={"ref_key": ref_key},
                                )
                            else:
                                target_comp = (
                                    components_by_key.get(ref_key)
                                    if components_by_key is not None
                                    else None
                                )
                                target_type = (
                                    target_comp.type
                                    if target_comp is not None
                                    else None
                                )
                                if target_type != "script.mapping":
                                    gen_profile_err = BuilderValidationError(
                                        f"transform.function wrapper "
                                        f"script_component_id $ref "
                                        f"target {ref_key!r} resolves to "
                                        f"a {target_type!r} component, "
                                        "not a script.mapping",
                                        error_code="SCRIPT_MAPPING_REF_REQUIRED",
                                        field="script_component_id",
                                        hint=(
                                            "Wrappers reference a "
                                            "script.mapping component via "
                                            "Configuration/Scripting "
                                            "componentId. Point the "
                                            "$ref at an in-spec "
                                            "script.mapping (or use a "
                                            "literal script.mapping "
                                            "componentId)."
                                        ),
                                        details={
                                            "ref_key": ref_key,
                                            "target_component_type": target_type,
                                        },
                                    )

        if gen_profile_err is not None:
            planned_action = "error_generated_profile_validation"
            validation_error = {
                "error_code": gen_profile_err.error_code,
                "error": str(gen_profile_err),
                "field": gen_profile_err.field,
                "hint": gen_profile_err.hint,
            }
            if gen_profile_err.details is not None:
                validation_error["details"] = gen_profile_err.details
            if (
                gen_profile_err.error_code == "PLAINTEXT_SECRET_REJECTED"
                and gen_profile_scanner_cls is not None
            ):
                gen_profile_scanner_cls.redact_forbidden_secret_fields_in_place(
                    raw_config
                )

        step: Dict[str, Any] = {
            "key": comp.key,
            "type": comp.type,
            "declared_action": comp.action,
            "planned_action": planned_action,
            "name": comp.name,
            "depends_on": comp.depends_on,
            "existing_component_id": existing_id,
            "route": route,
        }

        if candidates:
            step["candidates"] = [
                {
                    "component_id": c.get("component_id"),
                    "name": c.get("name"),
                    "folder_name": c.get("folder_name"),
                }
                for c in candidates
            ]

        if validation_error is not None:
            step["validation_error"] = validation_error

        # Issue #40: surface the function route explicitly in plan output so
        # plan readers can distinguish direct maps from function maps without
        # peeking at the raw config. Counts come from the same effective_config
        # the validator saw (which has $ref tokens resolved or kept verbatim).
        if comp.type == "transform.map":
            map_summary_config = raw_config or {}
            map_type_value = (
                str(map_summary_config.get("map_type") or "").strip().lower() or None
            )
            field_mappings_value = map_summary_config.get("field_mappings") or []
            function_mappings_value = map_summary_config.get("function_mappings") or []
            function_types_seen: List[str] = []
            seen_function_types: set = set()
            for fm in function_mappings_value if isinstance(function_mappings_value, list) else []:
                if isinstance(fm, Mapping):
                    ft = fm.get("function_type")
                    if isinstance(ft, str):
                        normalized = ft.strip().lower()
                        if normalized and normalized not in seen_function_types:
                            seen_function_types.add(normalized)
                            function_types_seen.append(normalized)
            transform_summary: Dict[str, Any] = {
                "map_type": map_type_value,
                "direct_mapping_count": (
                    len(field_mappings_value)
                    if isinstance(field_mappings_value, list)
                    else 0
                ),
                "function_count": (
                    len(function_mappings_value)
                    if isinstance(function_mappings_value, list)
                    else 0
                ),
                "function_types_used": function_types_seen,
            }
            # Issue #41: surface script-route counts + slot / language
            # inventories when this map calls reusable script.mapping
            # components. Only populated when map_type is script/map_script
            # so direct + function plans keep their existing shape.
            if map_type_value in ("script", "map_script"):
                script_mappings_value = (
                    map_summary_config.get("script_mappings") or []
                )
                script_slots_seen: List[str] = []
                seen_slots: set = set()
                script_langs_seen: List[str] = []
                seen_langs: set = set()
                for sm in (
                    script_mappings_value
                    if isinstance(script_mappings_value, list)
                    else []
                ):
                    if not isinstance(sm, Mapping):
                        continue
                    slot = sm.get("script_slot")
                    if isinstance(slot, str):
                        slot_n = slot.strip()
                        if slot_n and slot_n not in seen_slots:
                            seen_slots.add(slot_n)
                            script_slots_seen.append(slot_n)
                    lang = sm.get("language")
                    if isinstance(lang, str):
                        lang_n = lang.strip().lower()
                        if lang_n and lang_n not in seen_langs:
                            seen_langs.add(lang_n)
                            script_langs_seen.append(lang_n)
                transform_summary["script_count"] = (
                    len(script_mappings_value)
                    if isinstance(script_mappings_value, list)
                    else 0
                )
                transform_summary["script_slots_used"] = script_slots_seen
                transform_summary["script_languages_used"] = script_langs_seen
            step["transform_summary"] = transform_summary

        steps.append(step)

    if not spec.components:
        warnings.append("No components were provided; plan contains zero executable steps.")
    if config.get("source_description") and not config.get("integration_spec"):
        warnings.append("Spec was derived from source_description. Review normalized output before apply.")

    return {
        "_success": True,
        "integration_spec": spec.model_dump(),
        "conflict_policy": conflict_policy,
        "execution_order": execution_order,
        "steps": steps,
        "warnings": warnings or None,
    }


def _apply_plan(boomi_client: Boomi, profile: str, config: Dict[str, Any]) -> Dict[str, Any]:
    dry_run = bool(config.get("dry_run", True))
    planned = _build_plan(boomi_client, config)
    if not planned.get("_success"):
        return planned
    if dry_run:
        planned["dry_run"] = True
        planned["message"] = "Dry run only. Set dry_run=false to execute."
        return planned

    # Fail-fast: reject plans with unresolvable steps before executing anything
    unresolvable_steps = [
        step for step in planned["steps"]
        if step["planned_action"] in (
            "error_ambiguous_match",
            "error_missing_target",
            "error_database_validation",
            "error_rest_validation",
            "error_process_validation",
            "error_generated_profile_validation",
        )
    ]
    if unresolvable_steps:
        errors = []
        for step in unresolvable_steps:
            if step["planned_action"] == "error_ambiguous_match":
                candidate_info = step.get("candidates", [])
                ids = [c["component_id"] for c in candidate_info]
                errors.append(
                    f"Component '{step.get('name') or step['key']}' matched "
                    f"{len(candidate_info)} components: {ids}. "
                    f"Supply an explicit component_id to disambiguate."
                )
            elif step["planned_action"] == "error_missing_target":
                errors.append(
                    f"Component '{step.get('name') or step['key']}' has action=update "
                    f"but no matching component was found and no component_id was provided."
                )
            elif step["planned_action"] == "error_database_validation":
                ve = step.get("validation_error") or {}
                errors.append(
                    f"Component '{step.get('name') or step['key']}' failed "
                    f"database validation: "
                    f"{ve.get('error_code', 'DATABASE_CONNECTOR_VALIDATION_FAILED')} "
                    f"on field {ve.get('field')!r}."
                )
            elif step["planned_action"] == "error_rest_validation":
                ve = step.get("validation_error") or {}
                errors.append(
                    f"Component '{step.get('name') or step['key']}' failed "
                    f"REST validation: "
                    f"{ve.get('error_code', 'REST_CONNECTOR_VALIDATION_FAILED')} "
                    f"on field {ve.get('field')!r}."
                )
            elif step["planned_action"] == "error_process_validation":
                ve = step.get("validation_error") or {}
                errors.append(
                    f"Component '{step.get('name') or step['key']}' failed "
                    f"process-flow validation: "
                    f"{ve.get('error_code', 'PROCESS_XML_VALIDATION_FAILED')} "
                    f"on field {ve.get('field')!r}."
                )
            elif step["planned_action"] == "error_generated_profile_validation":
                ve = step.get("validation_error") or {}
                errors.append(
                    f"Component '{step.get('name') or step['key']}' failed "
                    f"generated profile / map validation: "
                    f"{ve.get('error_code', 'PROFILE_FIELD_VALIDATION_FAILED')} "
                    f"on field {ve.get('field')!r}."
                )
        return {
            "_success": False,
            "error": "Plan contains unresolvable steps. No operations were executed.",
            "unresolvable_steps": [
                {
                    "key": s["key"],
                    "planned_action": s["planned_action"],
                    "candidates": s.get("candidates", []),
                    "validation_error": s.get("validation_error"),
                }
                for s in unresolvable_steps
            ],
            "details": errors,
        }

    spec = IntegrationSpecV1(**planned["integration_spec"])
    conflict_policy = planned["conflict_policy"]
    execution_order = planned["execution_order"]
    components_by_key = {comp.key: comp for comp in spec.components}
    existing_ids = {step["key"]: step["existing_component_id"] for step in planned["steps"]}

    id_registry: Dict[str, str] = {}
    results: Dict[str, Dict[str, Any]] = {}

    for key in execution_order:
        comp = components_by_key[key]
        existing_id = existing_ids.get(key)
        resolved_config = _resolve_dependency_tokens(comp.config, id_registry)

        if comp.action == "create" and existing_id:
            if conflict_policy == "reuse":
                results[key] = {
                    "status": "reused",
                    "component_id": existing_id,
                    "type": comp.type,
                    "name": comp.name,
                }
                id_registry[key] = existing_id
                continue
            if conflict_policy == "fail":
                return {
                    "_success": False,
                    "error": f"Component '{comp.name or comp.key}' already exists and conflict_policy=fail",
                    "failed_step": key,
                    "partial_results": results,
                }
            resolved_config = _apply_clone_suffix(comp, resolved_config)

        target_id = comp.component_id or existing_id
        exec_result = _execute_component(
            boomi_client=boomi_client,
            profile=profile,
            comp=comp,
            config=resolved_config,
            target_id=target_id,
            components_by_key=components_by_key,
        )

        component_id = _extract_component_id(exec_result)
        if component_id:
            id_registry[key] = component_id

        results[key] = {
            "status": "updated" if comp.action == "update" else "created",
            "component_id": component_id,
            "type": comp.type,
            "name": comp.name,
            "result": exec_result,
        }

        if not exec_result.get("_success", False):
            return {
                "_success": False,
                "error": f"Failed at step '{key}'",
                "failed_step": key,
                "step_result": exec_result,
                "partial_results": results,
            }

    build_id = str(uuid4())
    _BUILD_REGISTRY[build_id] = {
        "created_at": datetime.now(timezone.utc).isoformat(),
        "profile": profile,
        "spec": spec.model_dump(),
        "results": results,
        "execution_order": execution_order,
    }

    return {
        "_success": True,
        "build_id": build_id,
        "message": f"Applied integration '{spec.name}' with {len(results)} steps.",
        "execution_order": execution_order,
        "results": results,
    }


def _verify_build(boomi_client: Boomi, config: Dict[str, Any]) -> Dict[str, Any]:
    build_id = config.get("build_id")
    if not build_id:
        return {"_success": False, "error": "build_id is required for verify action"}

    build = _BUILD_REGISTRY.get(build_id)
    if not build:
        return {"_success": False, "error": f"Unknown build_id '{build_id}'"}

    spec = IntegrationSpecV1(**build["spec"])
    results: Dict[str, Dict[str, Any]] = build["results"]

    verification: Dict[str, Any] = {"components": {}, "dependency_issues": []}
    verified_count = 0
    failed_count = 0

    for comp in spec.components:
        step = results.get(comp.key)
        component_id = step.get("component_id") if isinstance(step, dict) else None
        if not component_id:
            verification["components"][comp.key] = {
                "verified": False,
                "reason": "No component_id available in build results",
            }
            failed_count += 1
            continue

        try:
            if comp.type == "trading_partner":
                boomi_client.trading_partner_component.get_trading_partner_component(id_=component_id)
            else:
                component_get_xml(boomi_client, component_id)
            verification["components"][comp.key] = {"verified": True, "component_id": component_id}
            verified_count += 1
        except Exception as exc:
            verification["components"][comp.key] = {
                "verified": False,
                "component_id": component_id,
                "error": str(exc),
            }
            failed_count += 1

        for dep in comp.depends_on:
            dep_result = results.get(dep)
            dep_id = dep_result.get("component_id") if isinstance(dep_result, dict) else None
            if not dep_result or not dep_id:
                verification["dependency_issues"].append(
                    f"Component '{comp.key}' depends on '{dep}', but '{dep}' was not resolved to a component_id."
                )

    return {
        "_success": failed_count == 0 and not verification["dependency_issues"],
        "build_id": build_id,
        "verified_components": verified_count,
        "failed_components": failed_count,
        "dependency_issues": verification["dependency_issues"] or None,
        "verification": verification["components"],
    }


def build_integration_action(
    boomi_client: Boomi,
    profile: str,
    action: str,
    config: Optional[Dict[str, Any]] = None,
) -> Dict[str, Any]:
    """Route integration builder actions."""
    cfg = config or {}
    if not isinstance(cfg, dict):
        return {"_success": False, "error": "config must be a JSON object"}

    try:
        normalized_action = action.strip().lower()
        if normalized_action == "plan":
            result = _build_plan(boomi_client, cfg)
            result["profile"] = profile
            return result
        if normalized_action == "apply":
            result = _apply_plan(boomi_client, profile, cfg)
            result["profile"] = profile
            return result
        if normalized_action == "verify":
            result = _verify_build(boomi_client, cfg)
            result["profile"] = profile
            return result
        return {
            "_success": False,
            "error": f"Unknown action '{action}'",
            "hint": "Valid actions are: plan, apply, verify",
        }
    except ValueError as exc:
        return {
            "_success": False,
            "error": f"Validation error: {exc}",
            "exception_type": "ValidationError",
        }
    except Exception as exc:
        return {
            "_success": False,
            "error": f"Integration builder failed: {exc}",
            "exception_type": type(exc).__name__,
        }


__all__ = ["build_integration_action"]

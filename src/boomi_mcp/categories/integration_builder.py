"""
High-level integration builder orchestration.

This module provides a single action router that can:
- plan: normalize and validate an integration spec, then build an execution plan
- apply: execute component operations in deterministic dependency order
- verify: verify created/updated components and declared dependency wiring
"""

from __future__ import annotations

from collections import defaultdict
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional
from uuid import uuid4

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
from .components.connectors import create_connector, update_connector
from .components.manage_component import create_component, update_component
from .components.processes import create_process, update_process
from .components.trading_partners import create_trading_partner, update_trading_partner


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
}

_METADATA_TYPE_MAP = {
    "process": "process",
    "connector-settings": "connector-settings",
    "connector-action": "connector-action",
    "trading_partner": "tradingpartner",
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

    return {
        "key": key,
        "type": normalized_type,
        "action": action,
        "name": raw.get("name"),
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


def _find_existing_component_id(boomi_client: Boomi, comp: IntegrationComponentSpec) -> Optional[str]:
    if not comp.name:
        return None

    metadata_type = _metadata_type_for_component(comp)
    if not metadata_type:
        return None

    expression = ComponentMetadataSimpleExpression(
        operator=ComponentMetadataSimpleExpressionOperator.EQUALS,
        property=ComponentMetadataSimpleExpressionProperty.TYPE,
        argument=[metadata_type],
    )
    query_filter = ComponentMetadataQueryConfigQueryFilter(expression=expression)
    query_config = ComponentMetadataQueryConfig(query_filter=query_filter)
    components = paginate_metadata(boomi_client, query_config, show_all=False)
    matches = [item for item in components if item.get("name") == comp.name]
    if not matches:
        return None
    matches.sort(key=lambda item: item.get("component_id", ""))
    return matches[0].get("component_id")


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

    return cloned


def _execute_component(
    boomi_client: Boomi,
    profile: str,
    comp: IntegrationComponentSpec,
    config: Dict[str, Any],
    target_id: Optional[str] = None,
) -> Dict[str, Any]:
    payload = dict(config)
    if comp.name:
        if comp.type == "process":
            payload.setdefault("name", comp.name)
        elif comp.type in ("connector-settings", "connector-action"):
            payload.setdefault("component_name", comp.name)
            payload.setdefault("name", comp.name)
        elif comp.type == "trading_partner":
            payload.setdefault("component_name", comp.name)

    if comp.type == "process":
        if comp.action == "create":
            return create_process(boomi_client, profile, payload)
        if not target_id:
            return {"_success": False, "error": f"Missing process_id for update of component '{comp.key}'"}
        return update_process(boomi_client, profile, target_id, payload)

    if comp.type in ("connector-settings", "connector-action"):
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

    if comp.action == "create":
        return create_component(boomi_client, profile, payload)
    if not target_id:
        return {"_success": False, "error": f"Missing component_id for update of component '{comp.key}'"}
    return update_component(boomi_client, profile, target_id, payload)


def _build_plan(boomi_client: Boomi, config: Dict[str, Any]) -> Dict[str, Any]:
    spec = _normalize_to_spec(config)
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
        existing_id = _find_existing_component_id(boomi_client, comp)
        planned_action = comp.action
        if comp.action == "create" and existing_id:
            if conflict_policy == "reuse":
                planned_action = "reuse"
            elif conflict_policy == "clone":
                planned_action = "create_clone"
            else:
                planned_action = "error_if_exists"

        route = (
            "process_json_to_xml"
            if comp.type == "process"
            else "connector_builder_or_xml"
            if comp.type in ("connector-settings", "connector-action")
            else "trading_partner_json"
            if comp.type == "trading_partner"
            else "generic_component_xml"
        )

        steps.append(
            {
                "key": comp.key,
                "type": comp.type,
                "declared_action": comp.action,
                "planned_action": planned_action,
                "name": comp.name,
                "depends_on": comp.depends_on,
                "existing_component_id": existing_id,
                "route": route,
            }
        )

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

    spec = IntegrationSpecV1(**planned["integration_spec"])
    conflict_policy = planned["conflict_policy"]
    execution_order = planned["execution_order"]
    components_by_key = {comp.key: comp for comp in spec.components}

    id_registry: Dict[str, str] = {}
    results: Dict[str, Dict[str, Any]] = {}

    for key in execution_order:
        comp = components_by_key[key]
        existing_id = _find_existing_component_id(boomi_client, comp)
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

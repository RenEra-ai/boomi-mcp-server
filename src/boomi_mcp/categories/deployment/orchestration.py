"""Deployment orchestration: dry-run plan by default, real package/deploy on demand.

This module defines ``orchestrate_deploy_action``: an *internal* action that resolves a
prior ``build_integration(action='apply')`` build (recorded in the in-memory
``_BUILD_REGISTRY``) down to exactly one process component.

Issue #60 introduced the plan-only contract. Issue #61 adds the package + deploy stages:
- ``dry_run=True`` (the DEFAULT) preserves issue-#60 behavior exactly — **no Boomi SDK
  calls**; every stage is reported as it *would* run.
- ``dry_run=False`` performs idempotent work through ``manage_deployment_action``: it
  creates (or reuses) a versioned package for the resolved process component and deploys
  (or reuses an active deployment of) that package to the target environment, reporting
  package/deployment ids and active/current state. Failed package/deploy stages return
  structured error codes and **do not proceed** to the runtime/schedule/execution/log
  stages (those remain placeholders for #future M3.3/M3.4).

It is intentionally not wired into ``server.py`` as a public MCP tool; public wiring is
deferred to issue #64.

REGISTRY IMPORT — READ THIS BEFORE EDITING:
The build registry is bound via a RELATIVE import (``from .. import integration_builder``)
and read at CALL TIME (``integration_builder._BUILD_REGISTRY``). Do NOT switch to an absolute
``from boomi_mcp...`` / ``from src.boomi_mcp...`` import and do NOT cache/copy the dict at
import time. This repo runs under a dual-namespace layout (``src.boomi_mcp.*`` vs
``boomi_mcp.*`` are distinct module objects, each with its own ``_BUILD_REGISTRY``). A
relative import makes this module follow whatever namespace it was loaded under, so it shares
the same registry object as its caller; an absolute import silently binds a *different*
registry and breaks build resolution.
"""

from typing import Any, Dict, List, Literal, Optional, Tuple

from pydantic import BaseModel, Field, ValidationError

from .. import integration_builder  # registry accessed at call time — see module docstring
from .packages import manage_deployment_action  # sibling action reused for package/deploy

StageStatus = Literal[
    "planned",
    "skipped",
    "not_required",
    "created",
    "deployed",
    "reused",
    "failed",
    "blocked",
]

# Structured-error codes (stable contract identifiers).
INVALID_REQUEST = "INVALID_REQUEST"
BUILD_ID_REQUIRED = "BUILD_ID_REQUIRED"
ENVIRONMENT_ID_REQUIRED = "ENVIRONMENT_ID_REQUIRED"
RUNTIME_ID_REQUIRED = "RUNTIME_ID_REQUIRED"
BUILD_ID_UNKNOWN = "BUILD_ID_UNKNOWN"
BUILD_REGISTRY_ENTRY_MALFORMED = "BUILD_REGISTRY_ENTRY_MALFORMED"
BUILD_PROCESS_NOT_FOUND = "BUILD_PROCESS_NOT_FOUND"
BUILD_MULTIPLE_PROCESS_COMPONENTS = "BUILD_MULTIPLE_PROCESS_COMPONENTS"
BUILD_PROCESS_ID_MISSING = "BUILD_PROCESS_ID_MISSING"

# Package/deploy stage error codes (issue #61).
BOOMI_CLIENT_REQUIRED = "BOOMI_CLIENT_REQUIRED"
PACKAGE_LIST_FAILED = "PACKAGE_LIST_FAILED"
PACKAGE_CREATE_FAILED = "PACKAGE_CREATE_FAILED"
PACKAGE_ID_MISSING = "PACKAGE_ID_MISSING"
DEPLOY_LIST_FAILED = "DEPLOY_LIST_FAILED"
DEPLOY_AMBIGUOUS_EXISTING = "DEPLOY_AMBIGUOUS_EXISTING"
DEPLOY_CREATE_FAILED = "DEPLOY_CREATE_FAILED"
DEPLOY_ID_MISSING = "DEPLOY_ID_MISSING"


# ---------------------------------------------------------------------------
# Typed contracts
# ---------------------------------------------------------------------------
class OrchestrateDeployError(BaseModel):
    """A single structured error entry returned in the ``errors`` array."""

    code: str
    message: str
    field: Optional[str] = None
    details: Optional[Dict[str, Any]] = None


class OrchestrateDeployRequest(BaseModel):
    """Normalized request/config contract for ``orchestrate_deploy_action``."""

    build_id: Optional[str] = None
    environment_id: Optional[str] = None
    runtime_id: Optional[str] = None
    profile: Optional[str] = None
    schedule_override: Optional[Dict[str, Any]] = None
    run_test: bool = False
    dry_run: bool = True
    package_version: Optional[str] = None


class ComponentSummaryEntry(BaseModel):
    """One component in the resolved build's component summary."""

    key: str
    type: Optional[str] = None
    name: Optional[str] = None
    status: Optional[str] = None
    component_id: Optional[str] = None


class ComponentSummary(BaseModel):
    """Stable summary of every component recorded in the build."""

    total_components: int = 0
    by_type: Dict[str, int] = Field(default_factory=dict)
    components: List[ComponentSummaryEntry] = Field(default_factory=list)


class ResolvedBuildTarget(BaseModel):
    """The single process component a deploy would target, resolved from a build."""

    integration_name: Optional[str] = None
    process_key: str
    process_component_id: str
    process_name: Optional[str] = None
    process_status: Optional[str] = None
    component_summary: ComponentSummary


# --- Response-stage placeholder models -------------------------------------
# Stage statuses use a deliberate vocabulary:
#   "planned"      -> this stage would run in a real deploy
#   "skipped"      -> the caller opted out (e.g. run_test=False)
#   "not_required" -> the stage is not applicable to this plan (e.g. no schedule)
# All created-resource ids stay null in issue #60 (nothing is provisioned).
class PackageStage(BaseModel):
    status: StageStatus
    package_id: Optional[str] = None
    component_id: Optional[str] = None
    component_type: Optional[str] = None
    package_version: Optional[str] = None
    warnings: List[str] = Field(default_factory=list)


class DeploymentStage(BaseModel):
    status: StageStatus
    deployment_id: Optional[str] = None
    environment_id: Optional[str] = None
    package_id: Optional[str] = None
    active: Optional[bool] = None
    current_version: Optional[str] = None
    warnings: List[str] = Field(default_factory=list)


class RuntimeAttachmentStage(BaseModel):
    status: StageStatus
    attachment_id: Optional[str] = None
    runtime_id: Optional[str] = None


class ScheduleStage(BaseModel):
    status: StageStatus
    schedule_id: Optional[str] = None
    schedule_override: Optional[Dict[str, Any]] = None


class ExecutionStage(BaseModel):
    status: StageStatus
    execution_id: Optional[str] = None
    run_test: bool = False


class LogsStage(BaseModel):
    status: StageStatus
    log_ids: Optional[List[str]] = None


class CleanupStage(BaseModel):
    status: StageStatus
    cleanup_id: Optional[str] = None


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------
def _is_blank(value: Any) -> bool:
    """A value is blank if it is None or a whitespace-only string."""
    return value is None or (isinstance(value, str) and value.strip() == "")


def _normalize_type(value: Any) -> str:
    """Normalize a component ``type`` for case/whitespace-insensitive comparison."""
    return str(value or "").strip().lower()


def _effective_component_type(comp: Dict[str, Any]) -> str:
    """Effective (normalized) component type, unwrapping the generic ``component`` wrapper.

    A component authored through the generic ``type == "component"`` escape hatch keeps that
    literal top-level type in the stored build spec; its real type lives in ``config.type``
    (preferred) or ``config.component_type``. So a process built that way is recorded with
    ``type == "component"``, not ``"process"``. Mirror
    ``integration_builder._effective_component_type`` so a wrapped process resolves the same
    way a top-level ``type == "process"`` component does. Returns the (normalized) wrapper
    type unchanged when it cannot be unwrapped.
    """
    base = _normalize_type(comp.get("type"))
    if base != "component":
        return base
    config = comp.get("config")
    if isinstance(config, dict):
        for cfg_key in ("type", "component_type"):
            wrapped = config.get(cfg_key)
            if isinstance(wrapped, str) and wrapped.strip():
                return _normalize_type(wrapped)
    return base


def _error(
    code: str,
    message: str,
    field: Optional[str] = None,
    details: Optional[Dict[str, Any]] = None,
) -> OrchestrateDeployError:
    return OrchestrateDeployError(code=code, message=message, field=field, details=details)


def _validation_error_entry(err: Dict[str, Any]) -> OrchestrateDeployError:
    """Map a single Pydantic error dict into a structured contract error entry."""
    loc = err.get("loc") or ()
    field = str(loc[0]) if loc else None
    return _error(
        INVALID_REQUEST,
        err.get("msg", "Invalid value."),
        field=field,
        details={"type": err.get("type")},
    )


def _build_component_summary(
    components: List[Any],
    results: Dict[str, Any],
    execution_order: List[Any],
) -> ComponentSummary:
    """Summarize every component in the build, ordered by execution order.

    Components are emitted in ``execution_order`` first (the build's topological order),
    then any remaining components in their declared spec order, so the summary is stable.
    """
    comps_by_key: Dict[str, Dict[str, Any]] = {}
    declared_order: List[str] = []
    for comp in components:
        if not isinstance(comp, dict):
            continue
        key = comp.get("key")
        if key is None:
            continue
        comps_by_key[key] = comp
        declared_order.append(key)

    ordered_keys: List[str] = []
    seen = set()
    for key in list(execution_order) + declared_order:
        if key in comps_by_key and key not in seen:
            ordered_keys.append(key)
            seen.add(key)

    by_type: Dict[str, int] = {}
    entries: List[ComponentSummaryEntry] = []
    for key in ordered_keys:
        comp = comps_by_key[key]
        ctype = comp.get("type")
        result = results.get(key) if isinstance(results, dict) else None
        result = result if isinstance(result, dict) else {}
        type_label = ctype if ctype is not None else "unknown"
        by_type[type_label] = by_type.get(type_label, 0) + 1
        entries.append(
            ComponentSummaryEntry(
                key=key,
                type=ctype,
                name=result.get("name") or comp.get("name"),
                status=result.get("status"),
                component_id=result.get("component_id") or comp.get("component_id"),
            )
        )

    return ComponentSummary(
        total_components=len(entries),
        by_type=by_type,
        components=entries,
    )


def _resolve_build_deployment_target(
    build_id: str,
) -> Tuple[Optional[ResolvedBuildTarget], Optional[OrchestrateDeployError]]:
    """Resolve a recorded build to exactly one process component (read-only).

    Returns ``(target, None)`` on success or ``(None, error)`` on failure. Never mutates
    the registry and never calls the Boomi SDK.
    """
    registry = integration_builder._BUILD_REGISTRY
    entry = registry.get(build_id)
    if entry is None:
        return None, _error(
            BUILD_ID_UNKNOWN,
            f"No recorded build found for build_id '{build_id}'.",
            field="build_id",
            details={"build_id": build_id},
        )

    if not isinstance(entry, dict):
        return None, _error(
            BUILD_REGISTRY_ENTRY_MALFORMED,
            f"Build registry entry for '{build_id}' is malformed (not a mapping).",
            field="build_id",
            details={"build_id": build_id},
        )

    spec = entry.get("spec")
    if not isinstance(spec, dict):
        return None, _error(
            BUILD_REGISTRY_ENTRY_MALFORMED,
            f"Build '{build_id}' has no usable spec.",
            field="build_id",
            details={"build_id": build_id},
        )

    components = spec.get("components")
    if not isinstance(components, list):
        return None, _error(
            BUILD_REGISTRY_ENTRY_MALFORMED,
            f"Build '{build_id}' spec has no component list.",
            field="build_id",
            details={"build_id": build_id},
        )

    results = entry.get("results")
    results = results if isinstance(results, dict) else {}
    execution_order = entry.get("execution_order")
    execution_order = execution_order if isinstance(execution_order, list) else []
    integration_name = spec.get("name")

    process_candidates = [
        comp
        for comp in components
        if isinstance(comp, dict) and _effective_component_type(comp) == "process"
    ]

    if not process_candidates:
        return None, _error(
            BUILD_PROCESS_NOT_FOUND,
            f"Build '{build_id}' contains no process component to deploy.",
            field="build_id",
            details={"build_id": build_id},
        )

    if len(process_candidates) > 1:
        process_keys = [comp.get("key") for comp in process_candidates]
        return None, _error(
            BUILD_MULTIPLE_PROCESS_COMPONENTS,
            (
                f"Build '{build_id}' contains {len(process_candidates)} process components; "
                "orchestrate_deploy requires exactly one."
            ),
            field="build_id",
            details={"build_id": build_id, "process_keys": process_keys},
        )

    process_comp = process_candidates[0]
    process_key = process_comp.get("key")
    result_entry = results.get(process_key) if process_key is not None else None
    result_entry = result_entry if isinstance(result_entry, dict) else {}

    process_component_id = result_entry.get("component_id") or process_comp.get("component_id")
    if not process_component_id:
        return None, _error(
            BUILD_PROCESS_ID_MISSING,
            (
                f"Process component '{process_key}' in build '{build_id}' has no resolved "
                "component_id."
            ),
            field="build_id",
            details={"build_id": build_id, "process_key": process_key},
        )

    target = ResolvedBuildTarget(
        integration_name=integration_name,
        process_key=process_key,
        process_component_id=process_component_id,
        process_name=result_entry.get("name") or process_comp.get("name"),
        process_status=result_entry.get("status"),
        component_summary=_build_component_summary(components, results, execution_order),
    )
    return target, None


def _error_response(
    error_message: str,
    errors: List[OrchestrateDeployError],
) -> Dict[str, Any]:
    return {
        "_success": False,
        "error": error_message,
        "errors": [err.model_dump() for err in errors],
    }


# ---------------------------------------------------------------------------
# Package/deploy stage helpers (issue #61)
# ---------------------------------------------------------------------------
def _effective_package_version(package_version: Optional[str], build_id: str) -> str:
    """Effective package version: trimmed ``package_version`` if non-blank, else ``build_id``.

    Defaulting to ``build_id`` makes a retry of the *same build* target the same package
    version, so re-running orchestrate_deploy reuses (not duplicates) the package.
    """
    if isinstance(package_version, str) and package_version.strip():
        return package_version.strip()
    return build_id


def _call_deployment_action(
    boomi_client: Any,
    profile: Optional[str],
    action: str,
    config_data: Dict[str, Any],
) -> Dict[str, Any]:
    """Invoke a ``manage_deployment_action`` handler, passing ``boomi_client`` as the sdk.

    The router swallows ``ApiError``/``Exception`` into ``{"_success": False, "error": ...}``,
    so callers inspect the returned dict rather than catching exceptions.
    """
    return manage_deployment_action(
        sdk=boomi_client,
        profile=profile,
        action=action,
        config_data=config_data,
    )


def _deployment_is_active(dep: Any) -> bool:
    """Truthy active flag, mirroring ``packages._lookup_deployment_id`` rules.

    Accepts a real bool or the string forms ``"true"``/``"1"``/``"yes"``.
    """
    raw = dep.get("active") if isinstance(dep, dict) else None
    if isinstance(raw, str):
        return raw.strip().lower() in ("true", "1", "yes")
    return bool(raw) if raw is not None else False


def _created_date_key(pkg: Any) -> str:
    """Sort key for newest-first package selection; missing dates sort last."""
    if isinstance(pkg, dict):
        return pkg.get("created_date") or ""
    return ""


def _find_or_create_package(
    boomi_client: Any,
    profile: Optional[str],
    *,
    component_id: str,
    package_version: str,
) -> Tuple[Optional[PackageStage], Optional[OrchestrateDeployError]]:
    """Reuse an existing package for ``component_id`` + ``package_version`` or create one.

    Returns ``(PackageStage, None)`` on success or ``(None, error)`` on failure.
    """
    listed = _call_deployment_action(
        boomi_client, profile, "list_packages", {"component_id": component_id}
    )
    if not listed.get("_success"):
        return None, _error(
            PACKAGE_LIST_FAILED,
            listed.get("error") or "Failed to list existing packages.",
            field="build_id",
            details={"component_id": component_id},
        )

    packages = listed.get("packages") or []
    matches = [
        p
        for p in packages
        if isinstance(p, dict) and p.get("package_version") == package_version
    ]

    warnings: List[str] = []
    if matches:
        matches.sort(key=_created_date_key, reverse=True)
        selected = matches[0]
        status: StageStatus = "reused"
        if len(matches) > 1:
            warnings.append(
                f"{len(matches)} existing packages match component {component_id} "
                f"version {package_version}; reused the newest by created_date."
            )
    else:
        created = _call_deployment_action(
            boomi_client,
            profile,
            "create_package",
            {
                "component_id": component_id,
                "component_type": "process",
                "package_version": package_version,
            },
        )
        if not created.get("_success"):
            return None, _error(
                PACKAGE_CREATE_FAILED,
                created.get("error") or "Failed to create package.",
                field="build_id",
                details={"component_id": component_id, "package_version": package_version},
            )
        selected = created.get("package") or {}
        status = "created"

    package_id = selected.get("package_id") if isinstance(selected, dict) else None
    if not package_id:
        return None, _error(
            PACKAGE_ID_MISSING,
            "Package resolution returned no package_id.",
            field="build_id",
            details={"component_id": component_id, "package_version": package_version},
        )

    return (
        PackageStage(
            status=status,
            package_id=package_id,
            component_id=component_id,
            component_type="process",
            package_version=package_version,
            warnings=warnings,
        ),
        None,
    )


def _find_or_create_deployment(
    boomi_client: Any,
    profile: Optional[str],
    *,
    package_id: str,
    environment_id: str,
) -> Tuple[Optional[DeploymentStage], Optional[OrchestrateDeployError]]:
    """Reuse the single active deployment of ``package_id`` in ``environment_id`` or deploy.

    Returns ``(DeploymentStage, None)`` on success or ``(None, error)`` on failure. More than
    one active deployment is ambiguous and is NOT auto-resolved.
    """
    listed = _call_deployment_action(
        boomi_client,
        profile,
        "list_deployments",
        {"package_id": package_id, "environment_id": environment_id},
    )
    if not listed.get("_success"):
        return None, _error(
            DEPLOY_LIST_FAILED,
            listed.get("error") or "Failed to list existing deployments.",
            field="environment_id",
            details={"package_id": package_id, "environment_id": environment_id},
        )

    deployments = listed.get("deployments") or []
    active = [d for d in deployments if _deployment_is_active(d)]

    warnings: List[str] = []
    if len(active) > 1:
        return None, _error(
            DEPLOY_AMBIGUOUS_EXISTING,
            (
                f"{len(active)} active deployments already exist for package {package_id} "
                f"in environment {environment_id}; refusing to redeploy."
            ),
            field="environment_id",
            details={
                "package_id": package_id,
                "environment_id": environment_id,
                "active_count": len(active),
            },
        )

    if len(active) == 1:
        selected = active[0]
        status: StageStatus = "reused"
        if len(deployments) > 1:
            warnings.append(
                "Reused the existing active deployment; other (inactive) deployments exist."
            )
    else:
        created = _call_deployment_action(
            boomi_client,
            profile,
            "deploy",
            {"package_id": package_id, "environment_id": environment_id},
        )
        if not created.get("_success"):
            return None, _error(
                DEPLOY_CREATE_FAILED,
                created.get("error") or "Failed to deploy package.",
                field="environment_id",
                details={"package_id": package_id, "environment_id": environment_id},
            )
        selected = created.get("deployment") or {}
        status = "deployed"

    deployment_id = selected.get("deployment_id") if isinstance(selected, dict) else None
    if not deployment_id:
        return None, _error(
            DEPLOY_ID_MISSING,
            "Deployment resolution returned no deployment_id.",
            field="environment_id",
            details={"package_id": package_id, "environment_id": environment_id},
        )

    # The Boomi SDK ``DeployedPackage`` exposes the deployment revision under ``version``
    # (an int), not ``current_version`` — and ``packages._deployment_to_dict`` passes it
    # through uncoerced. Fall back to ``version`` so real deploy/reuse runs report the
    # revision instead of null, and coerce to str so the int revision satisfies the
    # ``Optional[str]`` stage field instead of raising a ValidationError.
    raw_current_version = selected.get("current_version") or selected.get("version")
    current_version = str(raw_current_version) if raw_current_version is not None else None
    return (
        DeploymentStage(
            status=status,
            deployment_id=deployment_id,
            environment_id=environment_id,
            package_id=package_id,
            active=_deployment_is_active(selected),
            current_version=current_version,
            warnings=warnings,
        ),
        None,
    )


# ---------------------------------------------------------------------------
# Response assembly
# ---------------------------------------------------------------------------
def _stage_summary(package: PackageStage, deployment: DeploymentStage) -> Dict[str, Any]:
    """Flat summary of the package/deploy outcome surfaced at the top of the response."""
    return {
        "package_id": package.package_id,
        "package_version": package.package_version,
        "deployment_id": deployment.deployment_id,
        "environment_id": deployment.environment_id,
        "deployment_active": deployment.active,
        "deployment_current_version": deployment.current_version,
        "stage_warnings": {
            "package": list(package.warnings),
            "deployment": list(deployment.warnings),
        },
    }


def _placeholder_downstream_stages(
    runtime_id: Optional[str],
    schedule_override: Optional[Dict[str, Any]],
    run_test: bool,
) -> Dict[str, Any]:
    """Runtime/schedule/execution/log/cleanup stages as plan placeholders (M3.3/M3.4)."""
    run_test_flag = bool(run_test)
    schedule_planned = schedule_override is not None
    return {
        "runtime_attachment": RuntimeAttachmentStage(status="planned", runtime_id=runtime_id),
        "schedule": ScheduleStage(
            status="planned" if schedule_planned else "not_required",
            schedule_override=schedule_override,
        ),
        "execution": ExecutionStage(
            status="planned" if run_test_flag else "skipped",
            run_test=run_test_flag,
        ),
        "logs": LogsStage(status="planned" if run_test_flag else "skipped"),
        "cleanup": CleanupStage(status="not_required"),
    }


def _blocked_downstream_stages(
    runtime_id: Optional[str],
    schedule_override: Optional[Dict[str, Any]],
    run_test: bool,
) -> Dict[str, Any]:
    """All later stages marked ``blocked`` after a package/deploy failure short-circuit."""
    return {
        "runtime_attachment": RuntimeAttachmentStage(status="blocked", runtime_id=runtime_id),
        "schedule": ScheduleStage(status="blocked", schedule_override=schedule_override),
        "execution": ExecutionStage(status="blocked", run_test=bool(run_test)),
        "logs": LogsStage(status="blocked"),
        "cleanup": CleanupStage(status="blocked"),
    }


def _assemble_response(
    *,
    success: bool,
    profile: Optional[str],
    build_id: Optional[str],
    dry_run: bool,
    plan_only: bool,
    target: ResolvedBuildTarget,
    package: PackageStage,
    deployment: DeploymentStage,
    downstream: Dict[str, Any],
    summary: Dict[str, Any],
    errors: List[OrchestrateDeployError],
    error_message: Optional[str] = None,
) -> Dict[str, Any]:
    """Build the full orchestrate_deploy response envelope (shared by every terminal path)."""
    response: Dict[str, Any] = {
        "_success": success,
        "profile": profile,
        "build_id": build_id,
        "dry_run": dry_run,
        "plan_only": plan_only,
        "integration_name": target.integration_name,
        "target": target.model_dump(),
        "component_summary": target.component_summary.model_dump(),
        "package": package.model_dump(),
        "deployment": deployment.model_dump(),
        "runtime_attachment": downstream["runtime_attachment"].model_dump(),
        "schedule": downstream["schedule"].model_dump(),
        "execution": downstream["execution"].model_dump(),
        "logs": downstream["logs"].model_dump(),
        "cleanup": downstream["cleanup"].model_dump(),
        "summary": summary,
        "warnings": [],
        "errors": [err.model_dump() for err in errors],
    }
    if error_message is not None:
        response["error"] = error_message
    return response


def _plan_response(
    *,
    profile: Optional[str],
    build_id: Optional[str],
    target: ResolvedBuildTarget,
    environment_id: Optional[str],
    runtime_id: Optional[str],
    schedule_override: Optional[Dict[str, Any]],
    run_test: bool,
    package_version: Optional[str],
) -> Dict[str, Any]:
    """Dry-run plan: every stage reported as it *would* run; no SDK calls were made."""
    effective_version = _effective_package_version(package_version, build_id)
    package = PackageStage(
        status="planned",
        component_id=target.process_component_id,
        component_type="process",
        package_version=effective_version,
    )
    deployment = DeploymentStage(status="planned", environment_id=environment_id)
    downstream = _placeholder_downstream_stages(runtime_id, schedule_override, run_test)
    return _assemble_response(
        success=True,
        profile=profile,
        build_id=build_id,
        dry_run=True,
        plan_only=True,
        target=target,
        package=package,
        deployment=deployment,
        downstream=downstream,
        summary=_stage_summary(package, deployment),
        errors=[],
    )


def _real_run_response(
    *,
    profile: Optional[str],
    build_id: Optional[str],
    target: ResolvedBuildTarget,
    package: PackageStage,
    deployment: DeploymentStage,
    runtime_id: Optional[str],
    schedule_override: Optional[Dict[str, Any]],
    run_test: bool,
) -> Dict[str, Any]:
    """Successful real-run response after the package + deploy stages completed."""
    downstream = _placeholder_downstream_stages(runtime_id, schedule_override, run_test)
    return _assemble_response(
        success=True,
        profile=profile,
        build_id=build_id,
        dry_run=False,
        plan_only=False,
        target=target,
        package=package,
        deployment=deployment,
        downstream=downstream,
        summary=_stage_summary(package, deployment),
        errors=[],
    )


def _blocked_real_run_response(
    *,
    profile: Optional[str],
    build_id: Optional[str],
    target: ResolvedBuildTarget,
    package: PackageStage,
    deployment: DeploymentStage,
    runtime_id: Optional[str],
    schedule_override: Optional[Dict[str, Any]],
    run_test: bool,
    error: OrchestrateDeployError,
) -> Dict[str, Any]:
    """Failed real-run response: the failing stage is marked, all later stages ``blocked``."""
    downstream = _blocked_downstream_stages(runtime_id, schedule_override, run_test)
    return _assemble_response(
        success=False,
        profile=profile,
        build_id=build_id,
        dry_run=False,
        plan_only=False,
        target=target,
        package=package,
        deployment=deployment,
        downstream=downstream,
        summary=_stage_summary(package, deployment),
        errors=[error],
        error_message=error.message,
    )


# ---------------------------------------------------------------------------
# Public internal action
# ---------------------------------------------------------------------------
def orchestrate_deploy_action(
    boomi_client: Any = None,
    profile: Optional[str] = None,
    build_id: Optional[str] = None,
    environment_id: Optional[str] = None,
    runtime_id: Optional[str] = None,
    schedule_override: Optional[Dict[str, Any]] = None,
    run_test: bool = False,
    dry_run: bool = True,
    package_version: Optional[str] = None,
) -> Dict[str, Any]:
    """Resolve a build, then plan (dry-run) or package + deploy it (issue #60/#61).

    With ``dry_run=True`` (the default) no ``boomi_client`` call is made — every stage is
    reported as it *would* run. With ``dry_run=False`` the package and deploy stages run for
    real (idempotently) through ``manage_deployment_action``; a package/deploy failure returns
    structured error codes and blocks the runtime/schedule/execution/log stages. All inputs are
    nullable so missing required values yield structured failures instead of raising.
    """
    # 0. Normalize the request through the typed contract so malformed input TYPES
    #    (e.g. a list build_id, which is unhashable, or a non-dict schedule_override)
    #    become structured errors instead of raw exceptions at the registry lookup or
    #    stage-model construction. Blank/None values are still permitted here and handled
    #    as required-field errors in step 1.
    try:
        request = OrchestrateDeployRequest(
            build_id=build_id,
            environment_id=environment_id,
            runtime_id=runtime_id,
            profile=profile,
            schedule_override=schedule_override,
            run_test=run_test,
            dry_run=dry_run,
            package_version=package_version,
        )
    except ValidationError as exc:
        return _error_response(
            "Invalid orchestrate_deploy request.",
            [_validation_error_entry(err) for err in exc.errors()],
        )

    build_id = request.build_id
    environment_id = request.environment_id
    runtime_id = request.runtime_id
    profile = request.profile
    schedule_override = request.schedule_override
    run_test = request.run_test
    dry_run = request.dry_run
    package_version = request.package_version

    # 1. Required-field validation (collect all missing inputs).
    required_errors: List[OrchestrateDeployError] = []
    if _is_blank(build_id):
        required_errors.append(
            _error(BUILD_ID_REQUIRED, "build_id is required.", field="build_id")
        )
    if _is_blank(environment_id):
        required_errors.append(
            _error(ENVIRONMENT_ID_REQUIRED, "environment_id is required.", field="environment_id")
        )
    if _is_blank(runtime_id):
        required_errors.append(
            _error(RUNTIME_ID_REQUIRED, "runtime_id is required.", field="runtime_id")
        )
    if required_errors:
        return _error_response("Missing required deployment inputs.", required_errors)

    # 2. Resolve the build to a single process component. This happens BEFORE any SDK call,
    #    so a resolver failure (e.g. BUILD_PROCESS_ID_MISSING) never touches boomi_client —
    #    even when dry_run is False.
    target, resolve_error = _resolve_build_deployment_target(build_id)
    if resolve_error is not None:
        return _error_response(resolve_error.message, [resolve_error])

    # 3a. Dry-run: assemble the plan-only response without any SDK call.
    if dry_run:
        return _plan_response(
            profile=profile,
            build_id=build_id,
            target=target,
            environment_id=environment_id,
            runtime_id=runtime_id,
            schedule_override=schedule_override,
            run_test=run_test,
            package_version=package_version,
        )

    # 3b. Real run: a Boomi client is required to package/deploy.
    effective_version = _effective_package_version(package_version, build_id)
    if boomi_client is None:
        package = PackageStage(
            status="failed",
            component_id=target.process_component_id,
            component_type="process",
            package_version=effective_version,
        )
        deployment = DeploymentStage(status="blocked", environment_id=environment_id)
        return _blocked_real_run_response(
            profile=profile,
            build_id=build_id,
            target=target,
            package=package,
            deployment=deployment,
            runtime_id=runtime_id,
            schedule_override=schedule_override,
            run_test=run_test,
            error=_error(
                BOOMI_CLIENT_REQUIRED,
                "A Boomi client is required to run package/deploy (dry_run=False).",
                field="boomi_client",
            ),
        )

    # 3c. Package stage (create or reuse). A failure blocks every later stage.
    package_stage, package_error = _find_or_create_package(
        boomi_client,
        profile,
        component_id=target.process_component_id,
        package_version=effective_version,
    )
    if package_error is not None:
        package = PackageStage(
            status="failed",
            component_id=target.process_component_id,
            component_type="process",
            package_version=effective_version,
        )
        deployment = DeploymentStage(status="blocked", environment_id=environment_id)
        return _blocked_real_run_response(
            profile=profile,
            build_id=build_id,
            target=target,
            package=package,
            deployment=deployment,
            runtime_id=runtime_id,
            schedule_override=schedule_override,
            run_test=run_test,
            error=package_error,
        )

    # 3d. Deploy stage (deploy or reuse active). A failure blocks every later stage but the
    #     resolved package stage is preserved in the response.
    deployment_stage, deployment_error = _find_or_create_deployment(
        boomi_client,
        profile,
        package_id=package_stage.package_id,
        environment_id=environment_id,
    )
    if deployment_error is not None:
        deployment = DeploymentStage(
            status="failed",
            environment_id=environment_id,
            package_id=package_stage.package_id,
        )
        return _blocked_real_run_response(
            profile=profile,
            build_id=build_id,
            target=target,
            package=package_stage,
            deployment=deployment,
            runtime_id=runtime_id,
            schedule_override=schedule_override,
            run_test=run_test,
            error=deployment_error,
        )

    # 3e. Success: both stages resolved. Later stages remain plan placeholders (M3.3/M3.4).
    return _real_run_response(
        profile=profile,
        build_id=build_id,
        target=target,
        package=package_stage,
        deployment=deployment_stage,
        runtime_id=runtime_id,
        schedule_override=schedule_override,
        run_test=run_test,
    )

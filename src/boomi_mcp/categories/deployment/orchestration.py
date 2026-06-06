"""Plan-only (dry-run) deployment orchestration contract — issue #60.

This module defines ``orchestrate_deploy_action``: an *internal* action that resolves a
prior ``build_integration(action='apply')`` build (recorded in the in-memory
``_BUILD_REGISTRY``) down to exactly one process component and returns a stable,
dry-run "deployment plan" response describing the stages a real deploy *would* run.

Issue #60 NON-GOALS — this contract performs **no Boomi SDK calls** and does **not**
package, deploy, attach runtimes, activate schedules, execute processes, or poll logs.
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

StageStatus = Literal["planned", "skipped", "not_required"]

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


class DeploymentStage(BaseModel):
    status: StageStatus
    deployment_id: Optional[str] = None
    environment_id: Optional[str] = None


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


def _plan_response(
    *,
    profile: Optional[str],
    build_id: Optional[str],
    target: ResolvedBuildTarget,
    environment_id: Optional[str],
    runtime_id: Optional[str],
    schedule_override: Optional[Dict[str, Any]],
    run_test: bool,
) -> Dict[str, Any]:
    component_summary = target.component_summary.model_dump()
    run_test_flag = bool(run_test)
    schedule_planned = schedule_override is not None

    package = PackageStage(status="planned")
    deployment = DeploymentStage(status="planned", environment_id=environment_id)
    runtime_attachment = RuntimeAttachmentStage(status="planned", runtime_id=runtime_id)
    schedule = ScheduleStage(
        status="planned" if schedule_planned else "not_required",
        schedule_override=schedule_override,
    )
    execution = ExecutionStage(
        status="planned" if run_test_flag else "skipped",
        run_test=run_test_flag,
    )
    logs = LogsStage(status="planned" if run_test_flag else "skipped")
    cleanup = CleanupStage(status="not_required")

    return {
        "_success": True,
        "profile": profile,
        "build_id": build_id,
        "dry_run": True,
        "plan_only": True,
        "integration_name": target.integration_name,
        "target": target.model_dump(),
        "component_summary": component_summary,
        "package": package.model_dump(),
        "deployment": deployment.model_dump(),
        "runtime_attachment": runtime_attachment.model_dump(),
        "schedule": schedule.model_dump(),
        "execution": execution.model_dump(),
        "logs": logs.model_dump(),
        "cleanup": cleanup.model_dump(),
        "warnings": [],
        "errors": [],
    }


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
) -> Dict[str, Any]:
    """Resolve a build and return a plan-only deployment plan (issue #60).

    ``boomi_client`` is accepted for downstream (issue #64) compatibility but is never used:
    this contract performs no Boomi SDK calls. All inputs are nullable so missing required
    values yield structured failures instead of raising.
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

    # 2. Resolve the build to a single process component.
    target, resolve_error = _resolve_build_deployment_target(build_id)
    if resolve_error is not None:
        return _error_response(resolve_error.message, [resolve_error])

    # 3. Assemble the dry-run deployment plan.
    return _plan_response(
        profile=profile,
        build_id=build_id,
        target=target,
        environment_id=environment_id,
        runtime_id=runtime_id,
        schedule_override=schedule_override,
        run_test=run_test,
    )

"""Deployment orchestration: dry-run plan by default, real package/deploy on demand.

This module defines ``orchestrate_deploy_action``: an *internal* action that resolves a
prior ``build_integration(action='apply')`` build (recorded in the in-memory
``_BUILD_REGISTRY``) down to exactly one process component.

Issue #60 introduced the plan-only contract. Issue #61 added the package + deploy stages.
Issue #62 added the runtime-attachment + schedule-activation stages. Issue #63 adds the
optional ``run_test`` execution + log/artifact summary stage:
- ``dry_run=True`` (the DEFAULT) preserves issue-#60 behavior exactly — **no Boomi SDK
  calls**; every stage is reported as it *would* run.
- ``dry_run=False`` performs idempotent work through the sibling action routers: it
  creates (or reuses) a versioned package for the resolved process component and deploys
  (or reuses an active deployment of) that package to the target environment, then verifies
  the environment/runtime and ensures the three bindings (runtime↔environment,
  process↔environment, process↔runtime) that make the process runnable, then applies the
  optional ``schedule_override`` (create/update + enable/disable, or clear/disable). Each
  stage runs strictly in order; a failure returns structured error codes and blocks every
  later stage. When ``run_test=True``, a final optional stage executes the resolved process
  (polling to a terminal status) and fetches bounded log excerpts/artifact metadata for
  diagnostics; a failed test execution is surfaced as ``_success=False`` with the prior
  stages preserved, while a log/artifact fetch failure is diagnostic only. Cleanup remains a
  placeholder. The run-test stage never runs in dry-run or after any prior-stage failure.

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
from ..environments import manage_environments_action  # verify environment exists
from ..runtimes import manage_runtimes_action  # runtime verify + runtime<->env attachment
from ..schedules import manage_schedules_action  # schedule update/delete/enable/disable
from ..execution import execute_process_action  # test-run execution (issue #63)
from ..monitoring import monitor_platform_action  # test-run log/artifact retrieval (issue #63)

StageStatus = Literal[
    "planned",
    "skipped",
    "not_required",
    "created",
    "deployed",
    "reused",
    "attached",
    "updated",
    "enabled",
    "disabled",
    "completed",
    "warning",
    "timeout",
    "retrieved",
    "unavailable",
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

# Runtime-attachment + schedule stage error codes (issue #62).
ENVIRONMENT_VERIFY_FAILED = "ENVIRONMENT_VERIFY_FAILED"
RUNTIME_VERIFY_FAILED = "RUNTIME_VERIFY_FAILED"
RUNTIME_ENV_ATTACHMENT_LIST_FAILED = "RUNTIME_ENV_ATTACHMENT_LIST_FAILED"
RUNTIME_ENV_ATTACHMENT_CREATE_FAILED = "RUNTIME_ENV_ATTACHMENT_CREATE_FAILED"
RUNTIME_ENV_ATTACHMENT_ID_MISSING = "RUNTIME_ENV_ATTACHMENT_ID_MISSING"
PROCESS_ENV_ATTACHMENT_LIST_FAILED = "PROCESS_ENV_ATTACHMENT_LIST_FAILED"
PROCESS_ENV_ATTACHMENT_CREATE_FAILED = "PROCESS_ENV_ATTACHMENT_CREATE_FAILED"
PROCESS_ENV_ATTACHMENT_ID_MISSING = "PROCESS_ENV_ATTACHMENT_ID_MISSING"
PROCESS_RUNTIME_ATTACHMENT_LIST_FAILED = "PROCESS_RUNTIME_ATTACHMENT_LIST_FAILED"
PROCESS_RUNTIME_ATTACHMENT_CREATE_FAILED = "PROCESS_RUNTIME_ATTACHMENT_CREATE_FAILED"
PROCESS_RUNTIME_ATTACHMENT_ID_MISSING = "PROCESS_RUNTIME_ATTACHMENT_ID_MISSING"
SCHEDULE_OVERRIDE_INVALID = "SCHEDULE_OVERRIDE_INVALID"
SCHEDULE_UPDATE_FAILED = "SCHEDULE_UPDATE_FAILED"
SCHEDULE_DELETE_FAILED = "SCHEDULE_DELETE_FAILED"
SCHEDULE_ENABLE_FAILED = "SCHEDULE_ENABLE_FAILED"
SCHEDULE_DISABLE_FAILED = "SCHEDULE_DISABLE_FAILED"
SCHEDULE_ID_MISSING = "SCHEDULE_ID_MISSING"

# Run-test stage error codes (issue #63).
TEST_EXECUTION_FAILED = "TEST_EXECUTION_FAILED"
TEST_EXECUTION_TIMEOUT = "TEST_EXECUTION_TIMEOUT"
TEST_REQUEST_ID_MISSING = "TEST_REQUEST_ID_MISSING"


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
    # Run-test stage inputs (issue #63). Only consulted when run_test=True on a real run.
    test_timeout_seconds: int = 300
    test_dynamic_properties: Optional[Dict[str, Any]] = None
    test_process_properties: Optional[Dict[str, Any]] = None
    test_log_level: str = "ALL"
    test_fetch_logs: bool = True
    test_fetch_artifacts: bool = True
    test_log_fetch_content: bool = True


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
    # ``attachment_id`` is preserved from issue #60/#61 as an alias of the runtime<->env
    # attachment id, so existing callers/tests that read it keep working.
    attachment_id: Optional[str] = None
    runtime_id: Optional[str] = None
    environment_id: Optional[str] = None
    process_id: Optional[str] = None
    # Three independent bindings make a process runnable on a runtime in an environment:
    #   runtime<->environment  (EnvironmentAtomAttachment)
    #   process<->environment  (ProcessEnvironmentAttachment)
    #   process<->runtime      (ProcessAtomAttachment)
    runtime_env_attachment_id: Optional[str] = None
    runtime_env_attachment_status: Optional[str] = None
    process_env_attachment_id: Optional[str] = None
    process_env_attachment_status: Optional[str] = None
    process_runtime_attachment_id: Optional[str] = None
    process_runtime_attachment_status: Optional[str] = None
    reused: bool = False
    changed: bool = False
    warnings: List[str] = Field(default_factory=list)


class ScheduleStage(BaseModel):
    status: StageStatus
    schedule_id: Optional[str] = None
    schedule_status_id: Optional[str] = None
    schedule_override: Optional[Dict[str, Any]] = None
    process_id: Optional[str] = None
    runtime_id: Optional[str] = None
    environment_id: Optional[str] = None
    cron: Optional[str] = None
    max_retry: Optional[int] = None
    enabled: Optional[bool] = None
    reused: bool = False
    changed: bool = False
    warnings: List[str] = Field(default_factory=list)


class ExecutionStage(BaseModel):
    status: StageStatus
    execution_id: Optional[str] = None
    run_test: bool = False
    # Run-test execution detail (issue #63); all null on plan/skipped/blocked placeholders.
    request_id: Optional[str] = None
    terminal_status: Optional[str] = None  # COMPLETE / COMPLETE_WARN / ERROR / ABORTED
    poll_status: Optional[str] = None  # COMPLETED / TIMEOUT
    elapsed_seconds: Optional[float] = None
    poll_count: Optional[int] = None
    process_id: Optional[str] = None
    environment_id: Optional[str] = None
    atom_id: Optional[str] = None
    document_counts: Optional[Dict[str, int]] = None  # inbound/outbound/inbound_error
    error: Optional[str] = None
    warnings: List[str] = Field(default_factory=list)


class LogsStage(BaseModel):
    status: StageStatus
    log_ids: Optional[List[str]] = None
    # Run-test diagnostics (issue #63); all null on plan/skipped/blocked placeholders.
    execution_id: Optional[str] = None
    log_level: Optional[str] = None
    status_code: Optional[int] = None
    message: Optional[str] = None
    download_url: Optional[str] = None
    downloaded: Optional[bool] = None
    log_excerpts: Optional[List[str]] = None  # bounded, first _RUN_TEST_LOG_MAX_FILES files
    artifact_status: Optional[StageStatus] = None
    artifact_status_code: Optional[int] = None
    artifact_message: Optional[str] = None
    artifact_download_url: Optional[str] = None
    error: Optional[str] = None
    artifact_error: Optional[str] = None
    warnings: List[str] = Field(default_factory=list)


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


def _call_environment_action(
    boomi_client: Any,
    profile: Optional[str],
    action: str,
    config_data: Dict[str, Any],
) -> Dict[str, Any]:
    """Invoke a ``manage_environments_action`` handler (router swallows exceptions into dicts)."""
    return manage_environments_action(
        sdk=boomi_client,
        profile=profile,
        action=action,
        config_data=config_data,
    )


def _call_runtime_action(
    boomi_client: Any,
    profile: Optional[str],
    action: str,
    config_data: Dict[str, Any],
) -> Dict[str, Any]:
    """Invoke a ``manage_runtimes_action`` handler (router swallows exceptions into dicts)."""
    return manage_runtimes_action(
        sdk=boomi_client,
        profile=profile,
        action=action,
        config_data=config_data,
    )


def _call_schedule_action(
    boomi_client: Any,
    profile: Optional[str],
    action: str,
    config_data: Dict[str, Any],
) -> Dict[str, Any]:
    """Invoke a ``manage_schedules_action`` handler (router swallows exceptions into dicts)."""
    return manage_schedules_action(
        sdk=boomi_client,
        profile=profile,
        action=action,
        config_data=config_data,
    )


def _call_execute_process_action(
    boomi_client: Any,
    profile: Optional[str],
    *,
    process_id: str,
    environment_id: Optional[str],
    atom_id: Optional[str],
    config_data: Dict[str, Any],
) -> Dict[str, Any]:
    """Invoke ``execute_process_action`` for the run-test stage (issue #63).

    Unlike the ``manage_*`` routers, ``execute_process_action`` takes ``process_id`` positionally
    (not a ``(sdk, profile, action, config_data)`` shape), so this wrapper passes
    ``process_id``/``environment_id``/``atom_id`` explicitly. The action swallows ``ApiError``/
    ``Exception`` into ``{"_success": False, "error": ...}``, so callers inspect the returned dict.
    """
    return execute_process_action(
        sdk=boomi_client,
        profile=profile,
        process_id=process_id,
        environment_id=environment_id,
        atom_id=atom_id,
        config_data=config_data,
    )


def _call_monitor_action(
    boomi_client: Any,
    profile: Optional[str],
    action: str,
    config_data: Dict[str, Any],
    creds: Optional[Dict[str, str]] = None,
) -> Dict[str, Any]:
    """Invoke ``monitor_platform_action`` for log/artifact retrieval (issue #63).

    Matches the router shape plus ``creds`` (needed to download + extract log/artifact content;
    without it only a ``download_url`` is returned). The router swallows exceptions into dicts.
    """
    return monitor_platform_action(
        boomi_client=boomi_client,
        profile=profile,
        action=action,
        config_data=config_data,
        creds=creds,
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
# Runtime-attachment stage helpers (issue #62)
# ---------------------------------------------------------------------------
def _resolve_attachment_leg(
    *,
    list_call,
    match,
    attach_call,
    list_error_code: str,
    create_error_code: str,
    id_error_code: str,
    field: Optional[str],
    details: Dict[str, Any],
) -> Tuple[Optional[str], Optional[str], Optional[OrchestrateDeployError]]:
    """Reuse a matching attachment or create one, for a single binding leg.

    Returns ``(attachment_id, leg_status, error)`` where ``leg_status`` is ``"reused"`` or
    ``"attached"``. ``list_call``/``attach_call`` are zero-arg callables returning the router's
    response dict; ``match`` is a predicate over a listed attachment dict.
    """
    listed = list_call()
    if not listed.get("_success"):
        return None, None, _error(
            list_error_code,
            listed.get("error") or "Failed to list existing attachments.",
            field=field,
            details=details,
        )

    attachments = listed.get("attachments") or []
    existing = next(
        (a for a in attachments if isinstance(a, dict) and match(a)),
        None,
    )
    if existing is not None:
        attachment_id = existing.get("id")
        leg_status = "reused"
    else:
        created = attach_call()
        if not created.get("_success"):
            return None, None, _error(
                create_error_code,
                created.get("error") or "Failed to create attachment.",
                field=field,
                details=details,
            )
        attachment = created.get("attachment") or {}
        attachment_id = attachment.get("id") if isinstance(attachment, dict) else None
        leg_status = "attached"

    if not attachment_id:
        # Surface ``leg_status`` even on this error: an "attached" status here means the create
        # call SUCCEEDED (the account was mutated) but returned no id, so the caller's ``changed``
        # accounting must still count it. A "reused" status means nothing was mutated.
        return None, leg_status, _error(
            id_error_code,
            "Attachment resolution returned no attachment id.",
            field=field,
            details=details,
        )
    return attachment_id, leg_status, None


def _ensure_runtime_attachment(
    boomi_client: Any,
    profile: Optional[str],
    *,
    process_id: str,
    environment_id: str,
    runtime_id: str,
) -> Tuple[RuntimeAttachmentStage, Optional[OrchestrateDeployError]]:
    """Verify env/runtime, then ensure all three bindings exist (reuse-or-attach, idempotent).

    Returns ``(stage, None)`` on success or ``(failed_stage, error)`` on the first failing leg.
    The three legs:
      1. runtime<->environment via ``manage_runtimes_action`` (EnvironmentAtomAttachment).
      2. process<->environment via ``manage_deployment_action`` (ProcessEnvironmentAttachment).
      3. process<->runtime    via ``manage_deployment_action`` (ProcessAtomAttachment).
    """
    base_details = {
        "process_id": process_id,
        "environment_id": environment_id,
        "runtime_id": runtime_id,
    }

    # Track each leg's resolved id/status as we go so a later-leg failure still reports the
    # bindings that DID resolve — including any attachment a prior leg actually created (so
    # ``changed`` stays accurate for retry/cleanup, not silently reset to False).
    runtime_env_id = runtime_env_status = None
    process_env_id = process_env_status = None
    process_runtime_id = process_runtime_status = None

    def _failed_stage() -> RuntimeAttachmentStage:
        statuses = (runtime_env_status, process_env_status, process_runtime_status)
        return RuntimeAttachmentStage(
            status="failed",
            attachment_id=runtime_env_id,
            runtime_id=runtime_id,
            environment_id=environment_id,
            process_id=process_id,
            runtime_env_attachment_id=runtime_env_id,
            runtime_env_attachment_status=runtime_env_status,
            process_env_attachment_id=process_env_id,
            process_env_attachment_status=process_env_status,
            process_runtime_attachment_id=process_runtime_id,
            process_runtime_attachment_status=process_runtime_status,
            reused=False,
            changed=any(s == "attached" for s in statuses),
        )

    # 1. Verify the environment and runtime exist before attaching anything.
    env_result = _call_environment_action(
        boomi_client, profile, "get", {"resource_id": environment_id}
    )
    if not env_result.get("_success"):
        return _failed_stage(), _error(
            ENVIRONMENT_VERIFY_FAILED,
            env_result.get("error") or "Failed to verify environment.",
            field="environment_id",
            details=base_details,
        )

    runtime_result = _call_runtime_action(
        boomi_client, profile, "get", {"resource_id": runtime_id}
    )
    if not runtime_result.get("_success"):
        return _failed_stage(), _error(
            RUNTIME_VERIFY_FAILED,
            runtime_result.get("error") or "Failed to verify runtime.",
            field="runtime_id",
            details=base_details,
        )

    # 2. runtime<->environment attachment (EnvironmentAtomAttachment).
    runtime_env_id, runtime_env_status, error = _resolve_attachment_leg(
        list_call=lambda: _call_runtime_action(
            boomi_client, profile, "list_attachments", {"environment_id": environment_id}
        ),
        match=lambda a: a.get("atom_id") == runtime_id,
        attach_call=lambda: _call_runtime_action(
            boomi_client, profile, "attach",
            {"resource_id": runtime_id, "environment_id": environment_id},
        ),
        list_error_code=RUNTIME_ENV_ATTACHMENT_LIST_FAILED,
        create_error_code=RUNTIME_ENV_ATTACHMENT_CREATE_FAILED,
        id_error_code=RUNTIME_ENV_ATTACHMENT_ID_MISSING,
        field="runtime_id",
        details=base_details,
    )
    if error is not None:
        return _failed_stage(), error

    # 3. process<->environment attachment (ProcessEnvironmentAttachment).
    process_env_id, process_env_status, error = _resolve_attachment_leg(
        list_call=lambda: _call_deployment_action(
            boomi_client, profile, "list_process_environment_attachments",
            {"process_id": process_id},
        ),
        match=lambda a: a.get("environment_id") == environment_id,
        attach_call=lambda: _call_deployment_action(
            boomi_client, profile, "attach_process_environment",
            {"process_id": process_id, "environment_id": environment_id},
        ),
        list_error_code=PROCESS_ENV_ATTACHMENT_LIST_FAILED,
        create_error_code=PROCESS_ENV_ATTACHMENT_CREATE_FAILED,
        id_error_code=PROCESS_ENV_ATTACHMENT_ID_MISSING,
        field="environment_id",
        details=base_details,
    )
    if error is not None:
        return _failed_stage(), error

    # 4. process<->runtime attachment (ProcessAtomAttachment).
    process_runtime_id, process_runtime_status, error = _resolve_attachment_leg(
        list_call=lambda: _call_deployment_action(
            boomi_client, profile, "list_process_atom_attachments",
            {"process_id": process_id},
        ),
        match=lambda a: a.get("atom_id") == runtime_id,
        attach_call=lambda: _call_deployment_action(
            boomi_client, profile, "attach_process_atom",
            {"process_id": process_id, "atom_id": runtime_id},
        ),
        list_error_code=PROCESS_RUNTIME_ATTACHMENT_LIST_FAILED,
        create_error_code=PROCESS_RUNTIME_ATTACHMENT_CREATE_FAILED,
        id_error_code=PROCESS_RUNTIME_ATTACHMENT_ID_MISSING,
        field="runtime_id",
        details=base_details,
    )
    if error is not None:
        return _failed_stage(), error

    leg_statuses = (runtime_env_status, process_env_status, process_runtime_status)
    changed = any(s == "attached" for s in leg_statuses)
    reused = all(s == "reused" for s in leg_statuses)
    stage = RuntimeAttachmentStage(
        status="attached" if changed else "reused",
        attachment_id=runtime_env_id,
        runtime_id=runtime_id,
        environment_id=environment_id,
        process_id=process_id,
        runtime_env_attachment_id=runtime_env_id,
        runtime_env_attachment_status=runtime_env_status,
        process_env_attachment_id=process_env_id,
        process_env_attachment_status=process_env_status,
        process_runtime_attachment_id=process_runtime_id,
        process_runtime_attachment_status=process_runtime_status,
        reused=reused,
        changed=changed,
    )
    return stage, None


# ---------------------------------------------------------------------------
# Schedule stage helpers (issue #62)
# ---------------------------------------------------------------------------
_SCHEDULE_ALLOWED_KEYS = {"mode", "cron", "enabled", "max_retry"}
_SCHEDULE_SCHEDULED_MODES = {"scheduled"}
_SCHEDULE_DISABLED_MODES = {"manual", "disabled"}

# Run-test stage bounds (issue #63). Poll interval / max-polls stay delegated to
# ``execute_process_action`` (2s until 30s, then 5s, bounded by ``test_timeout_seconds``).
_RUN_TEST_TIMEOUT_SECONDS = 300
_RUN_TEST_LOG_LEVEL = "ALL"
_RUN_TEST_LOG_EXCERPT_LINES = 80
_RUN_TEST_LOG_EXCERPT_CHARS = 8000
_RUN_TEST_LOG_MAX_FILES = 3


def _normalize_schedule_override(
    schedule_override: Optional[Dict[str, Any]],
) -> Tuple[Optional[Dict[str, Any]], Optional[OrchestrateDeployError]]:
    """Validate + normalize the schedule override into a canonical dict.

    Returns ``(None, None)`` when no schedule mutation is requested, ``(normalized, None)`` on
    success, or ``(None, error)`` with ``SCHEDULE_OVERRIDE_INVALID`` on any content problem.

    Canonical forms returned:
      - scheduled: ``{"mode": "scheduled", "cron": <5-part>, "enabled": <bool>, "max_retry": <0..5>}``
      - disabled:  ``{"mode": "disabled", "enabled": False}``
    """
    if schedule_override is None:
        return None, None

    def _invalid(message: str) -> Tuple[None, OrchestrateDeployError]:
        return None, _error(
            SCHEDULE_OVERRIDE_INVALID, message, field="schedule_override",
            details={"schedule_override": schedule_override},
        )

    if not isinstance(schedule_override, dict):
        return _invalid("schedule_override must be an object.")

    unknown = set(schedule_override) - _SCHEDULE_ALLOWED_KEYS
    if unknown:
        return _invalid(
            f"Unsupported schedule_override keys: {', '.join(sorted(unknown))}. "
            f"Allowed: {', '.join(sorted(_SCHEDULE_ALLOWED_KEYS))}."
        )

    mode = schedule_override.get("mode")
    cron = schedule_override.get("cron")
    enabled = schedule_override.get("enabled")
    max_retry = schedule_override.get("max_retry")

    if "enabled" in schedule_override and not isinstance(enabled, bool):
        return _invalid("schedule_override.enabled must be a boolean.")

    # Resolve the effective mode.
    if mode is not None:
        if not isinstance(mode, str) or mode.strip().lower() not in (
            _SCHEDULE_SCHEDULED_MODES | _SCHEDULE_DISABLED_MODES
        ):
            return _invalid(
                "schedule_override.mode must be one of 'scheduled', 'manual', 'disabled'."
            )
        mode = mode.strip().lower()
    elif cron is not None:
        mode = "scheduled"
    elif enabled is False:
        mode = "disabled"
    else:
        return _invalid(
            "schedule_override needs a 'cron' (to schedule) or 'enabled: false'/"
            "'mode' (to disable)."
        )

    if mode in _SCHEDULE_DISABLED_MODES:
        if cron is not None:
            return _invalid("cron is not allowed when disabling a schedule.")
        if max_retry is not None:
            return _invalid("max_retry is not allowed when disabling a schedule.")
        if enabled is True:
            return _invalid("enabled: true is incompatible with a disabled schedule.")
        return {"mode": "disabled", "enabled": False}, None

    # mode == "scheduled"
    if not isinstance(cron, str) or not cron.strip():
        return _invalid("cron is required for a scheduled override.")
    if len(cron.split()) != 5:
        return _invalid(
            "cron must have 5 parts (minute hour day_of_month month day_of_week)."
        )
    if max_retry is None:
        max_retry = 5
    elif not isinstance(max_retry, int) or isinstance(max_retry, bool) or not 0 <= max_retry <= 5:
        return _invalid("schedule_override.max_retry must be an integer between 0 and 5.")
    enabled_flag = True if enabled is None else enabled
    return (
        {
            "mode": "scheduled",
            "cron": cron.strip(),
            "enabled": enabled_flag,
            "max_retry": max_retry,
        },
        None,
    )


def _apply_schedule_override(
    boomi_client: Any,
    profile: Optional[str],
    *,
    process_id: str,
    environment_id: str,
    runtime_id: str,
    normalized: Optional[Dict[str, Any]],
    schedule_override: Optional[Dict[str, Any]],
) -> Tuple[ScheduleStage, Optional[OrchestrateDeployError]]:
    """Apply the (already-normalized) schedule override after runtime binding succeeds.

    Returns ``(stage, None)`` on success or ``(failed_stage, error)`` on the first failing call.
    Makes no SDK calls when ``normalized`` is ``None`` (no schedule requested).
    """
    ids = {"process_id": process_id, "atom_id": runtime_id}
    base_fields = {
        "schedule_override": schedule_override,
        "process_id": process_id,
        "runtime_id": runtime_id,
        "environment_id": environment_id,
    }

    if normalized is None:
        return (
            ScheduleStage(status="not_required", reused=False, changed=False, **base_fields),
            None,
        )

    # ``changed`` reflects whether a schedule mutation actually succeeded — a first-call
    # failure (update/delete returning ``_success=False``) means the account is untouched, so
    # those sites pass ``changed=False``; failures AFTER a successful update/delete keep
    # ``changed=True`` so the summary/cleanup still sees the mutation that landed.
    def _failed_stage(*, changed: bool, **extra: Any) -> ScheduleStage:
        return ScheduleStage(status="failed", changed=changed, **base_fields, **extra)

    if normalized["mode"] == "disabled":
        deleted = _call_schedule_action(boomi_client, profile, "delete", dict(ids))
        if not deleted.get("_success"):
            return _failed_stage(changed=False), _error(
                SCHEDULE_DELETE_FAILED,
                deleted.get("error") or "Failed to clear schedule.",
                field="schedule_override", details=ids,
            )
        schedule_id = (deleted.get("schedule") or {}).get("id")
        if not schedule_id:
            return _failed_stage(changed=True), _error(
                SCHEDULE_ID_MISSING, "Schedule clear returned no schedule id.",
                field="schedule_override", details=ids,
            )
        disabled = _call_schedule_action(boomi_client, profile, "disable", dict(ids))
        if not disabled.get("_success"):
            return _failed_stage(changed=True, schedule_id=schedule_id), _error(
                SCHEDULE_DISABLE_FAILED,
                disabled.get("error") or "Failed to disable schedule.",
                field="schedule_override", details=ids,
            )
        status_obj = disabled.get("status") or {}
        schedule_status_id = status_obj.get("id")
        if not schedule_status_id:
            return _failed_stage(changed=True, schedule_id=schedule_id), _error(
                SCHEDULE_ID_MISSING, "Schedule disable returned no status id.",
                field="schedule_override", details=ids,
            )
        return (
            ScheduleStage(
                status="disabled",
                schedule_id=schedule_id,
                schedule_status_id=schedule_status_id,
                enabled=False,
                changed=True,
                **base_fields,
            ),
            None,
        )

    # mode == "scheduled"
    cron = normalized["cron"]
    max_retry = normalized["max_retry"]
    enabled = normalized["enabled"]
    updated = _call_schedule_action(
        boomi_client, profile, "update",
        {**ids, "cron": cron, "max_retry": max_retry},
    )
    if not updated.get("_success"):
        return _failed_stage(changed=False, cron=cron, max_retry=max_retry, enabled=enabled), _error(
            SCHEDULE_UPDATE_FAILED,
            updated.get("error") or "Failed to update schedule.",
            field="schedule_override", details=ids,
        )
    schedule_id = (updated.get("schedule") or {}).get("id")
    if not schedule_id:
        return _failed_stage(changed=True, cron=cron, max_retry=max_retry, enabled=enabled), _error(
            SCHEDULE_ID_MISSING, "Schedule update returned no schedule id.",
            field="schedule_override", details=ids,
        )

    if enabled:
        status_result = _call_schedule_action(boomi_client, profile, "enable", dict(ids))
        status_code = SCHEDULE_ENABLE_FAILED
        final_status: StageStatus = "enabled"
    else:
        status_result = _call_schedule_action(boomi_client, profile, "disable", dict(ids))
        status_code = SCHEDULE_DISABLE_FAILED
        final_status = "disabled"

    if not status_result.get("_success"):
        return _failed_stage(
            changed=True, schedule_id=schedule_id, cron=cron, max_retry=max_retry, enabled=enabled
        ), _error(
            status_code,
            status_result.get("error") or "Failed to set schedule status.",
            field="schedule_override", details=ids,
        )
    status_obj = status_result.get("status") or {}
    schedule_status_id = status_obj.get("id")
    if not schedule_status_id:
        return _failed_stage(
            changed=True, schedule_id=schedule_id, cron=cron, max_retry=max_retry, enabled=enabled
        ), _error(
            SCHEDULE_ID_MISSING, "Schedule status update returned no status id.",
            field="schedule_override", details=ids,
        )
    return (
        ScheduleStage(
            status=final_status,
            schedule_id=schedule_id,
            schedule_status_id=schedule_status_id,
            cron=cron,
            max_retry=max_retry,
            enabled=bool(status_obj.get("enabled")) if "enabled" in status_obj else enabled,
            changed=True,
            **base_fields,
        ),
        None,
    )


# ---------------------------------------------------------------------------
# Run-test stage helpers (issue #63)
# ---------------------------------------------------------------------------
def _build_test_execution_config(request: "OrchestrateDeployRequest") -> Dict[str, Any]:
    """Config for ``execute_process_action``: force wait+timeout, pass optional properties.

    ``dynamic_properties``/``process_properties`` are only included when supplied, so the
    execute action's own empty-default builders apply when the caller omits them.
    """
    config: Dict[str, Any] = {
        "wait": True,
        "timeout": request.test_timeout_seconds,
    }
    if request.test_dynamic_properties is not None:
        config["dynamic_properties"] = request.test_dynamic_properties
    if request.test_process_properties is not None:
        config["process_properties"] = request.test_process_properties
    return config


def _execution_stage_from_result(
    exec_result: Dict[str, Any],
    *,
    run_test: bool,
    process_id: str,
    environment_id: Optional[str],
    atom_id: Optional[str],
) -> Tuple[ExecutionStage, Optional[OrchestrateDeployError]]:
    """Map an ``execute_process_action`` (wait=True) response into an ``ExecutionStage``.

    Returns ``(stage, None)`` for a non-failing terminal status (COMPLETE / COMPLETE_WARN) or
    ``(stage, error)`` for ERROR/ABORTED (``TEST_EXECUTION_FAILED``), TIMEOUT
    (``TEST_EXECUTION_TIMEOUT``), or a missing request id (``TEST_REQUEST_ID_MISSING``).
    """
    request_id = exec_result.get("request_id")
    poll = exec_result.get("execution_result")
    base_kwargs: Dict[str, Any] = dict(
        run_test=bool(run_test),
        request_id=request_id,
        process_id=process_id,
        environment_id=exec_result.get("environment_id") or environment_id,
        atom_id=exec_result.get("atom_id") or atom_id,
    )

    # No poll result means the request was never accepted (execute_process_action returned its
    # early failure before the wait branch). A blank request id is the canonical "no request_id".
    if poll is None:
        if not request_id:
            message = exec_result.get("error") or "Execution request returned no request_id."
            stage = ExecutionStage(status="failed", error=message, **base_kwargs)
            return stage, _error(TEST_REQUEST_ID_MISSING, message, field="run_test")
        message = exec_result.get("error") or "Execution produced no terminal result."
        stage = ExecutionStage(status="failed", error=message, **base_kwargs)
        return stage, _error(TEST_EXECUTION_FAILED, message, field="run_test")

    poll_status = poll.get("poll_status")
    elapsed = poll.get("elapsed_seconds")
    poll_count = poll.get("poll_count")

    if poll_status == "TIMEOUT":
        message = poll.get("message") or exec_result.get("error") or "Execution timed out."
        stage = ExecutionStage(
            status="timeout",
            poll_status=poll_status,
            elapsed_seconds=elapsed,
            poll_count=poll_count,
            error=message,
            **base_kwargs,
        )
        return stage, _error(TEST_EXECUTION_TIMEOUT, message, field="run_test")

    terminal = str(poll.get("status") or "").upper()
    document_counts = {
        "inbound": poll.get("inbound_document_count"),
        "outbound": poll.get("outbound_document_count"),
        "inbound_error": poll.get("inbound_error_document_count"),
    }
    common: Dict[str, Any] = dict(
        execution_id=exec_result.get("execution_id") or poll.get("execution_id"),
        terminal_status=poll.get("status"),
        poll_status=poll_status,
        elapsed_seconds=elapsed,
        poll_count=poll_count,
        document_counts=document_counts,
        **base_kwargs,
    )

    if terminal == "COMPLETE":
        return ExecutionStage(status="completed", **common), None
    if terminal == "COMPLETE_WARN":
        return (
            ExecutionStage(
                status="warning",
                warnings=["Test execution completed with warnings (COMPLETE_WARN)."],
                **common,
            ),
            None,
        )
    # ERROR / ABORTED / any other terminal status is a failed test run.
    message = poll.get("error") or f"Execution ended with status: {poll.get('status')}"
    stage = ExecutionStage(status="failed", error=message, **common)
    return stage, _error(TEST_EXECUTION_FAILED, message, field="run_test")


def _bounded_log_excerpts(log_result: Dict[str, Any]) -> List[str]:
    """Bounded, human-readable log excerpts from an ``execution_logs`` result.

    The monitoring content is either a ``files`` dict (name -> text, extracted from the ZIP) or a
    single ``content`` string. Take at most ``_RUN_TEST_LOG_MAX_FILES`` files and cap each excerpt
    to ``_RUN_TEST_LOG_EXCERPT_LINES`` lines and ``_RUN_TEST_LOG_EXCERPT_CHARS`` chars.
    """
    items: List[Tuple[str, str]] = []
    files = log_result.get("files")
    if isinstance(files, dict):
        for name, text in files.items():
            items.append((str(name), text if isinstance(text, str) else str(text)))
    else:
        content = log_result.get("content")
        if isinstance(content, str):
            items.append(("log", content))

    excerpts: List[str] = []
    for name, text in items[:_RUN_TEST_LOG_MAX_FILES]:
        lines = text.splitlines()
        truncated = len(lines) > _RUN_TEST_LOG_EXCERPT_LINES
        clipped = "\n".join(lines[:_RUN_TEST_LOG_EXCERPT_LINES])
        if len(clipped) > _RUN_TEST_LOG_EXCERPT_CHARS:
            clipped = clipped[:_RUN_TEST_LOG_EXCERPT_CHARS]
            truncated = True
        suffix = "\n... [truncated]" if truncated else ""
        excerpts.append(f"{name}:\n{clipped}{suffix}")
    return excerpts


def _logs_stage_from_results(
    log_result: Optional[Dict[str, Any]],
    artifact_result: Optional[Dict[str, Any]],
    *,
    execution_id: Optional[str],
    log_level: str,
    fetch_logs: bool,
    fetch_artifacts: bool,
) -> LogsStage:
    """Normalize log + artifact monitor results into a ``LogsStage``.

    A failed/absent log fetch is *diagnostic only* (``status="unavailable"``) — it never turns a
    successful test execution into a failed orchestration. The artifact leg is independent.
    """
    stage = LogsStage(
        status="not_required",
        execution_id=execution_id,
        log_level=log_level if fetch_logs else None,
    )
    warnings: List[str] = []

    if fetch_logs:
        if log_result is not None and log_result.get("_success"):
            stage.status = "retrieved"
            stage.status_code = log_result.get("status_code")
            stage.message = log_result.get("message")
            stage.download_url = log_result.get("download_url")
            stage.downloaded = (
                bool(log_result.get("_downloaded")) if "_downloaded" in log_result else None
            )
            stage.log_excerpts = _bounded_log_excerpts(log_result)
        else:
            stage.status = "unavailable"
            if log_result is not None:
                stage.status_code = log_result.get("status_code")
                stage.message = log_result.get("message")
                stage.download_url = log_result.get("download_url")
                stage.error = log_result.get("error") or "Log retrieval failed."
            else:
                stage.error = "Log retrieval failed."
            warnings.append("Test execution succeeded but log retrieval was unavailable.")

    if fetch_artifacts:
        if artifact_result is not None and artifact_result.get("_success"):
            stage.artifact_status = "retrieved"
            stage.artifact_status_code = artifact_result.get("status_code")
            stage.artifact_message = artifact_result.get("message")
            stage.artifact_download_url = artifact_result.get("download_url")
        else:
            stage.artifact_status = "unavailable"
            if artifact_result is not None:
                stage.artifact_status_code = artifact_result.get("status_code")
                stage.artifact_message = artifact_result.get("message")
                stage.artifact_error = (
                    artifact_result.get("error") or "Artifact retrieval failed."
                )
            else:
                stage.artifact_error = "Artifact retrieval failed."

    stage.warnings = warnings
    return stage


def _run_test_stage(
    boomi_client: Any,
    profile: Optional[str],
    request: "OrchestrateDeployRequest",
    *,
    target: ResolvedBuildTarget,
    environment_id: Optional[str],
    runtime_id: Optional[str],
    creds: Optional[Dict[str, str]] = None,
) -> Tuple[ExecutionStage, LogsStage, Optional[OrchestrateDeployError]]:
    """Execute the resolved process (wait=True), then fetch bounded diagnostics by execution id.

    Diagnostics (logs + artifacts) are fetched whenever an ``execution_id`` resolved — including
    on ERROR/ABORTED, where the logs are the whole point of the stage. TIMEOUT and a missing
    request id never produce an ``execution_id``, so there is nothing to fetch and logs are
    ``blocked``. Returns ``(execution_stage, logs_stage, error)`` where ``error`` is non-None only
    for a *failed test execution* (not for a diagnostic log/artifact fetch failure).
    """
    exec_result = _call_execute_process_action(
        boomi_client,
        profile,
        process_id=target.process_component_id,
        environment_id=environment_id,
        atom_id=runtime_id,
        config_data=_build_test_execution_config(request),
    )
    execution_stage, execution_error = _execution_stage_from_result(
        exec_result,
        run_test=request.run_test,
        process_id=target.process_component_id,
        environment_id=environment_id,
        atom_id=runtime_id,
    )

    execution_id = execution_stage.execution_id
    if not execution_id:
        # No execution id (timeout / missing request id / completed-without-id): nothing to fetch.
        logs_stage = LogsStage(
            status="blocked" if execution_error is not None else "not_required",
            execution_id=None,
        )
        return execution_stage, logs_stage, execution_error

    fetch_logs = bool(request.test_fetch_logs)
    fetch_artifacts = bool(request.test_fetch_artifacts)
    log_result: Optional[Dict[str, Any]] = None
    artifact_result: Optional[Dict[str, Any]] = None
    if fetch_logs:
        log_result = _call_monitor_action(
            boomi_client,
            profile,
            "execution_logs",
            {
                "execution_id": execution_id,
                "log_level": request.test_log_level,
                "fetch_content": request.test_log_fetch_content,
            },
            creds=creds,
        )
    if fetch_artifacts:
        artifact_result = _call_monitor_action(
            boomi_client,
            profile,
            "execution_artifacts",
            {
                "execution_id": execution_id,
                "fetch_content": request.test_log_fetch_content,
            },
            creds=creds,
        )

    logs_stage = _logs_stage_from_results(
        log_result,
        artifact_result,
        execution_id=execution_id,
        log_level=request.test_log_level,
        fetch_logs=fetch_logs,
        fetch_artifacts=fetch_artifacts,
    )
    return execution_stage, logs_stage, execution_error


# ---------------------------------------------------------------------------
# Response assembly
# ---------------------------------------------------------------------------
def _stage_summary(
    package: PackageStage,
    deployment: DeploymentStage,
    runtime_attachment: RuntimeAttachmentStage,
    schedule: ScheduleStage,
    *,
    execution: Optional[ExecutionStage] = None,
    logs: Optional[LogsStage] = None,
) -> Dict[str, Any]:
    """Flat summary of the package/deploy/runtime/schedule outcome at the top of the response.

    When a real run-test stage ran, ``execution``/``logs`` are supplied and a ``test`` sub-summary
    is added (issue #63). Plan / blocked / run_test=False paths omit it.
    """
    summary: Dict[str, Any] = {
        "package_id": package.package_id,
        "package_version": package.package_version,
        "deployment_id": deployment.deployment_id,
        "environment_id": deployment.environment_id,
        "deployment_active": deployment.active,
        "deployment_current_version": deployment.current_version,
        "runtime_id": runtime_attachment.runtime_id,
        "runtime_attachment_id": runtime_attachment.attachment_id,
        "runtime_attachment_status": runtime_attachment.status,
        "schedule_id": schedule.schedule_id,
        "schedule_status": schedule.status,
        "schedule_enabled": schedule.enabled,
        "resource_reuse": {
            "runtime_attachment": runtime_attachment.reused,
            "schedule": schedule.reused,
        },
        "resource_changes": {
            "runtime_attachment": runtime_attachment.changed,
            "schedule": schedule.changed,
        },
        "stage_warnings": {
            "package": list(package.warnings),
            "deployment": list(deployment.warnings),
            "runtime_attachment": list(runtime_attachment.warnings),
            "schedule": list(schedule.warnings),
        },
    }
    if execution is not None and logs is not None:
        summary["test"] = {
            "run_test": execution.run_test,
            "request_id": execution.request_id,
            "execution_id": execution.execution_id,
            "execution_status": execution.status,
            "terminal_status": execution.terminal_status,
            "poll_status": execution.poll_status,
            "elapsed_seconds": execution.elapsed_seconds,
            "poll_count": execution.poll_count,
            "document_counts": execution.document_counts,
            "execution_error": execution.error,
            "logs_status": logs.status,
            "log_download_url": logs.download_url,
            "log_excerpt_count": len(logs.log_excerpts or []),
            "artifact_download_url": logs.artifact_download_url,
            "log_error": logs.error,
            "artifact_error": logs.artifact_error,
        }
    return summary


def _execution_log_cleanup_stages(run_test: bool, *, blocked: bool) -> Dict[str, Any]:
    """Execution/log/cleanup stages — still M3.4 placeholders (planned/skipped) or ``blocked``."""
    if blocked:
        return {
            "execution": ExecutionStage(status="blocked", run_test=bool(run_test)),
            "logs": LogsStage(status="blocked"),
            "cleanup": CleanupStage(status="blocked"),
        }
    run_test_flag = bool(run_test)
    return {
        "execution": ExecutionStage(
            status="planned" if run_test_flag else "skipped",
            run_test=run_test_flag,
        ),
        "logs": LogsStage(status="planned" if run_test_flag else "skipped"),
        "cleanup": CleanupStage(status="not_required"),
    }


def _placeholder_downstream_stages(
    runtime_id: Optional[str],
    schedule_override: Optional[Dict[str, Any]],
    run_test: bool,
) -> Dict[str, Any]:
    """Runtime/schedule/execution/log/cleanup stages as plan placeholders (dry-run / M3.4)."""
    schedule_planned = schedule_override is not None
    return {
        "runtime_attachment": RuntimeAttachmentStage(status="planned", runtime_id=runtime_id),
        "schedule": ScheduleStage(
            status="planned" if schedule_planned else "not_required",
            schedule_override=schedule_override,
        ),
        **_execution_log_cleanup_stages(run_test, blocked=False),
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
        **_execution_log_cleanup_stages(run_test, blocked=True),
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
        summary=_stage_summary(
            package, deployment, downstream["runtime_attachment"], downstream["schedule"]
        ),
        errors=[],
    )


def _real_run_response(
    *,
    profile: Optional[str],
    build_id: Optional[str],
    target: ResolvedBuildTarget,
    package: PackageStage,
    deployment: DeploymentStage,
    runtime_attachment: RuntimeAttachmentStage,
    schedule: ScheduleStage,
    run_test: bool,
) -> Dict[str, Any]:
    """Successful real-run response after package, deploy, runtime binding, and schedule."""
    downstream = {
        "runtime_attachment": runtime_attachment,
        "schedule": schedule,
        **_execution_log_cleanup_stages(run_test, blocked=False),
    }
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
        summary=_stage_summary(package, deployment, runtime_attachment, schedule),
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
        summary=_stage_summary(
            package, deployment, downstream["runtime_attachment"], downstream["schedule"]
        ),
        errors=[error],
        error_message=error.message,
    )


def _runtime_or_schedule_failed_response(
    *,
    profile: Optional[str],
    build_id: Optional[str],
    target: ResolvedBuildTarget,
    package: PackageStage,
    deployment: DeploymentStage,
    runtime_attachment: RuntimeAttachmentStage,
    schedule: ScheduleStage,
    run_test: bool,
    error: OrchestrateDeployError,
) -> Dict[str, Any]:
    """Failed real-run after deploy: a runtime/schedule stage failed; execution onward blocked.

    The runtime and schedule stages are passed through verbatim so the response shows exactly
    how far binding got (a failed runtime stage with schedule blocked, or a completed runtime
    stage with a failed schedule stage).
    """
    downstream = {
        "runtime_attachment": runtime_attachment,
        "schedule": schedule,
        **_execution_log_cleanup_stages(run_test, blocked=True),
    }
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
        summary=_stage_summary(package, deployment, runtime_attachment, schedule),
        errors=[error],
        error_message=error.message,
    )


def _real_run_with_test_response(
    *,
    success: bool,
    profile: Optional[str],
    build_id: Optional[str],
    target: ResolvedBuildTarget,
    package: PackageStage,
    deployment: DeploymentStage,
    runtime_attachment: RuntimeAttachmentStage,
    schedule: ScheduleStage,
    execution: ExecutionStage,
    logs: LogsStage,
    errors: Optional[List[OrchestrateDeployError]] = None,
    error_message: Optional[str] = None,
) -> Dict[str, Any]:
    """Real-run response embedding the concrete run-test execution + logs stages (issue #63).

    Mirrors ``_real_run_response`` but carries the real ``ExecutionStage``/``LogsStage`` (cleanup
    stays ``not_required``). ``success`` is False for a failed test execution, with the failing
    stages still embedded for diagnostics; top-level ``warnings`` surface the stages' warnings.
    """
    downstream = {
        "runtime_attachment": runtime_attachment,
        "schedule": schedule,
        "execution": execution,
        "logs": logs,
        "cleanup": CleanupStage(status="not_required"),
    }
    response = _assemble_response(
        success=success,
        profile=profile,
        build_id=build_id,
        dry_run=False,
        plan_only=False,
        target=target,
        package=package,
        deployment=deployment,
        downstream=downstream,
        summary=_stage_summary(
            package, deployment, runtime_attachment, schedule,
            execution=execution, logs=logs,
        ),
        errors=errors or [],
        error_message=error_message,
    )
    test_warnings = list(execution.warnings) + list(logs.warnings)
    if test_warnings:
        response["warnings"] = test_warnings
    return response


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
    test_timeout_seconds: int = 300,
    test_dynamic_properties: Optional[Dict[str, Any]] = None,
    test_process_properties: Optional[Dict[str, Any]] = None,
    test_log_level: str = "ALL",
    test_fetch_logs: bool = True,
    test_fetch_artifacts: bool = True,
    test_log_fetch_content: bool = True,
    creds: Optional[Dict[str, str]] = None,
) -> Dict[str, Any]:
    """Resolve a build, then plan (dry-run) or package/deploy/bind/schedule it (#60/#61/#62).

    With ``dry_run=True`` (the default) no ``boomi_client`` call is made — every stage is
    reported as it *would* run. With ``dry_run=False`` the package, deploy, runtime-binding,
    and schedule stages run for real (idempotently) through the sibling action routers, in that
    order; any stage failure returns structured error codes and blocks every later stage. An
    invalid ``schedule_override`` is rejected up front (before any SDK call) in both modes. All
    inputs are nullable so missing required values yield structured failures instead of raising.
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
            test_timeout_seconds=test_timeout_seconds,
            test_dynamic_properties=test_dynamic_properties,
            test_process_properties=test_process_properties,
            test_log_level=test_log_level,
            test_fetch_logs=test_fetch_logs,
            test_fetch_artifacts=test_fetch_artifacts,
            test_log_fetch_content=test_log_fetch_content,
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

    # 2b. Validate schedule_override CONTENT (format) up front — a fail-fast structured error
    #     in BOTH dry-run and real-run, before any SDK call. The normalized form is reused by
    #     the real-run schedule stage; an invalid override never reaches package/deploy.
    normalized_schedule, schedule_override_error = _normalize_schedule_override(schedule_override)
    if schedule_override_error is not None:
        return _error_response(schedule_override_error.message, [schedule_override_error])

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

    # 3e. Runtime binding stage (issue #62): verify env/runtime and ensure the three bindings
    #     that make the deployed process runnable. Runs only after a successful deploy. A
    #     failure blocks the schedule/execution/log/cleanup stages.
    runtime_attachment, runtime_error = _ensure_runtime_attachment(
        boomi_client,
        profile,
        process_id=target.process_component_id,
        environment_id=environment_id,
        runtime_id=runtime_id,
    )
    if runtime_error is not None:
        blocked_schedule = ScheduleStage(
            status="blocked",
            schedule_override=schedule_override,
            process_id=target.process_component_id,
            runtime_id=runtime_id,
            environment_id=environment_id,
        )
        return _runtime_or_schedule_failed_response(
            profile=profile,
            build_id=build_id,
            target=target,
            package=package_stage,
            deployment=deployment_stage,
            runtime_attachment=runtime_attachment,
            schedule=blocked_schedule,
            run_test=run_test,
            error=runtime_error,
        )

    # 3f. Schedule activation stage (issue #62): only after deploy + runtime binding succeed,
    #     and never in dry-run (handled at 3a). A failure blocks execution/log/cleanup.
    schedule_stage, schedule_error = _apply_schedule_override(
        boomi_client,
        profile,
        process_id=target.process_component_id,
        environment_id=environment_id,
        runtime_id=runtime_id,
        normalized=normalized_schedule,
        schedule_override=schedule_override,
    )
    if schedule_error is not None:
        return _runtime_or_schedule_failed_response(
            profile=profile,
            build_id=build_id,
            target=target,
            package=package_stage,
            deployment=deployment_stage,
            runtime_attachment=runtime_attachment,
            schedule=schedule_stage,
            run_test=run_test,
            error=schedule_error,
        )

    # 3g. Success: package, deploy, runtime binding, and schedule all resolved. Without run_test
    #     the execution/log/cleanup stages stay skipped placeholders; with run_test the optional
    #     test stage executes the process (wait=True), then fetches bounded log/artifact
    #     diagnostics. A failed test execution returns _success=False with the prior stages
    #     preserved; a log/artifact fetch failure stays _success=True (diagnostic only). (#63)
    if not run_test:
        return _real_run_response(
            profile=profile,
            build_id=build_id,
            target=target,
            package=package_stage,
            deployment=deployment_stage,
            runtime_attachment=runtime_attachment,
            schedule=schedule_stage,
            run_test=run_test,
        )

    execution_stage, logs_stage, test_error = _run_test_stage(
        boomi_client,
        profile,
        request,
        target=target,
        environment_id=environment_id,
        runtime_id=runtime_id,
        creds=creds,
    )
    return _real_run_with_test_response(
        success=test_error is None,
        profile=profile,
        build_id=build_id,
        target=target,
        package=package_stage,
        deployment=deployment_stage,
        runtime_attachment=runtime_attachment,
        schedule=schedule_stage,
        execution=execution_stage,
        logs=logs_stage,
        errors=[test_error] if test_error is not None else [],
        error_message=test_error.message if test_error is not None else None,
    )

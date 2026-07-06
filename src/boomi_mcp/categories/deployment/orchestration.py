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
  stages preserved, while a log/artifact fetch failure is diagnostic only — unless
  ``require_test_logs=True`` (issue #81), which promotes an absent/unavailable log fetch after a
  successful test into a ``TEST_LOGS_UNAVAILABLE`` orchestration failure. Cleanup remains a
  placeholder. The run-test stage never runs in dry-run or after any prior-stage failure. Every
  full response also carries a top-level ``behavior_verified`` marker (issue #81) so the
  deploy-clean-is-not-verification gap is visible without parsing stage statuses.

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

import base64
import time
import urllib.error
import urllib.request
import xml.etree.ElementTree as ET
from datetime import datetime, timedelta, timezone
from typing import Any, Dict, List, Literal, Optional, Tuple

from pydantic import BaseModel, Field, StrictBool, ValidationError

from .. import integration_builder  # registry accessed at call time — see module docstring
from .packages import manage_deployment_action  # sibling action reused for package/deploy
from .deployment_utils import is_environment_account_signal  # shared env-account signal detection
from ..environments import manage_environments_action  # verify environment exists
from ..runtimes import manage_runtimes_action  # runtime verify + runtime<->env attachment
from ..schedules import manage_schedules_action  # schedule update/delete/enable/disable
from ..execution import execute_process_action  # test-run execution (issue #63)
from ..monitoring import monitor_platform_action  # test-run log/artifact retrieval (issue #63)
from ..shared_resources import manage_shared_resources_action  # listener apiType/auth preflight (M6 #12)
from ..components._shared import component_get_xml  # listener collision check component reads (M6 #12)
from ..components.analyze_component import (  # ASC route/XML extraction shared with the analyzer (M6.1 #133)
    _extract_wss_listen_binding,
    _extract_wss_operation_config,
    _parse_api_service_xml,
)
from ...patterns.primitives.wss_listen import (  # single-source WSS endpoint formula (M6 #12)
    compute_wss_endpoint,
    effective_api_service_route,
    normalize_api_service_path_segment,
    sentence_case_object_name,
    wss_http_method,
)

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

# Behavioral-verification opt-in code (issue #81). Unlike LOG_RETRIEVAL_FAILED (diagnostic-only),
# this code FLIPS ``_success`` to False: it is only emitted when the caller set
# ``require_test_logs=True`` and a test ran but its ProcessLog retrieval was absent/unavailable.
TEST_LOGS_UNAVAILABLE = "TEST_LOGS_UNAVAILABLE"

# Issue #102 B4 — an explicitly empty process-overrides set over a build whose process
# declares environment extensions orphans those extension values (the deploy pushes an empty
# override). Rejected before any SDK call.
EMPTY_PROCESS_OVERRIDES_REJECTED = "EMPTY_PROCESS_OVERRIDES_REJECTED"

# M6 (#12) — listener_verify stage error codes. The stage runs only for a build
# whose process is a WSS listener (validation_rules.listener in the build
# registry), between schedule and execution.
LISTENER_SERVER_INFO_FAILED = "LISTENER_SERVER_INFO_FAILED"
LISTENER_APITYPE_UNSUPPORTED = "LISTENER_APITYPE_UNSUPPORTED"
LISTENER_DEPLOYMENT_INACTIVE = "LISTENER_DEPLOYMENT_INACTIVE"
LISTENER_PATH_COLLISION = "LISTENER_PATH_COLLISION"
LISTENER_PROBE_FAILED = "LISTENER_PROBE_FAILED"
LISTENER_EXECUTION_RECORD_MISSING = "LISTENER_EXECUTION_RECORD_MISSING"

# M6.1 (#133) — API Service Component (ASC) publish-mode codes. On an
# apiType=advanced runtime, routes exist ONLY through a deployed ASC — bare
# WSS deploys clean but 404s (live-confirmed); on basic/intermediate the
# inverse holds. Deploy does NOT cascade: the ASC and each route process
# deploy independently to the same environment.
LISTENER_ASC_REQUIRED = "LISTENER_ASC_REQUIRED"
LISTENER_ASC_UNSUPPORTED_FOR_APITYPE = "LISTENER_ASC_UNSUPPORTED_FOR_APITYPE"
LISTENER_ASC_DEPLOYMENT_INACTIVE = "LISTENER_ASC_DEPLOYMENT_INACTIVE"
LISTENER_ROUTE_PROCESS_DEPLOYMENT_INACTIVE = "LISTENER_ROUTE_PROCESS_DEPLOYMENT_INACTIVE"
LISTENER_ASC_ROUTE_INVALID = "LISTENER_ASC_ROUTE_INVALID"
LISTENER_ASC_COLLISION = "LISTENER_ASC_COLLISION"

# Failure-hardening + cleanup-planning codes (issue #65).
# LOG_RETRIEVAL_FAILED / ARTIFACT_RETRIEVAL_FAILED are *diagnostic* — they annotate the logs
# stage but never flip ``_success`` to False. CLEANUP_OPERATION_FAILED is recorded only when
# the caller opts into destructive cleanup (cleanup_on_failure=True) and an operation fails.
LOG_RETRIEVAL_FAILED = "LOG_RETRIEVAL_FAILED"
ARTIFACT_RETRIEVAL_FAILED = "ARTIFACT_RETRIEVAL_FAILED"
CLEANUP_OPERATION_FAILED = "CLEANUP_OPERATION_FAILED"


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
    # Failure-recovery input (issue #65). ``StrictBool`` (not plain ``bool``) because this flag
    # TRIGGERS DESTRUCTION: True executes undeploy/delete/detach. Plain ``bool`` lax-coerces
    # ``"yes"``/``1``/``"true"`` to True, so a direct engine caller bypassing the wrapper's bool
    # guard could silently opt into destructive cleanup. StrictBool rejects non-bool with a
    # structured INVALID_REQUEST at request construction, before any SDK call. Only consulted on
    # failed real-run paths: False (default) returns a dry-run cleanup PLAN; True executes it.
    cleanup_on_failure: StrictBool = False
    # Run-test stage inputs (issue #63). Only consulted when run_test=True on a real run.
    test_timeout_seconds: int = 300
    test_dynamic_properties: Optional[Dict[str, Any]] = None
    test_process_properties: Optional[Dict[str, Any]] = None
    test_log_level: str = "ALL"
    test_fetch_logs: bool = True
    test_fetch_artifacts: bool = True
    test_log_fetch_content: bool = True
    # Behavioral-verification opt-in (issue #81). When True and a test ran but its ProcessLog
    # retrieval is absent/unavailable, the logs stage fails the orchestration with
    # ``TEST_LOGS_UNAVAILABLE`` instead of the default diagnostic-only success-with-warning. Plain
    # ``bool`` (lax coercion is acceptable) — unlike ``cleanup_on_failure`` this flag triggers NO
    # destruction; the wrapper still validates it as a real bool before any SDK call.
    require_test_logs: bool = False
    # Issue #102 B4 — process-overrides supplied for the deploy. ``None`` = "not supplied"
    # (existing environment-extension values are preserved). An explicitly empty ``{}`` over a
    # process that declares extensions is rejected (EMPTY_PROCESS_OVERRIDES_REJECTED) because it
    # would orphan those extension values. Inspected only — this issue does not mutate extensions.
    process_overrides: Optional[Dict[str, Any]] = None
    # M6 (#12) — listener_verify stage inputs. Only consulted on a real run of a build whose
    # process is a WSS listener (validation_rules.listener in the build registry).
    # listener_base_url overrides the probe base URL (e.g. http://localhost:9090 for a
    # docker-hosted local atom whose SharedServerInformation url is container-internal).
    listener_test_payload: Optional[str] = None
    listener_base_url: Optional[str] = None
    listener_probe_timeout_seconds: int = 30
    # Basic-auth username override for the probe (cloud attachments use an instance-id
    # username `accountId.suffix`; the default is the platform account_id from creds).
    listener_auth_username: Optional[str] = None


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


class ListenerVerifyStage(BaseModel):
    """M6 (#12): post-deploy verification of a WSS listener route.

    ListenerStatus is deliberately NOT used — live-proven (2026-07-04, both
    runtimes) to stay empty for WSS/ASC routes; it covers connector listeners
    (JMS/Solace/...) only. Verification is: SharedServerInformation preflight
    (apiType + auth), deployment-active check, component-query collision check
    (never a pre-probe — Boomi-managed clouds answer a uniform 401 pre-route),
    an authenticated live probe of the computed endpoint, and an
    execution-record readback (HTTP 200 is an ack, not process success).
    """

    status: StageStatus
    api_type: Optional[str] = None
    auth: Optional[str] = None
    endpoint_path: Optional[str] = None
    endpoint_url: Optional[str] = None
    http_method: Optional[str] = None
    probe_status_code: Optional[int] = None
    # Which objectName casing the runtime served ('sentence_case' — the
    # live-settled 2026-07-04 default — or 'verbatim' via the defensive
    # fallback probe, which would contradict that finding).
    served_object_name_casing: Optional[str] = None
    deployment_active: Optional[bool] = None
    collision_count: Optional[int] = None
    execution_record_found: Optional[bool] = None
    # False when the pre-probe baseline snapshot failed and the readback
    # degraded to accept-any-record (weaker evidence — the matched record may
    # predate the probe). behavior_verified requires True (Codex review, M6 #12).
    readback_baseline_available: Optional[bool] = None
    execution_id: Optional[str] = None
    execution_status: Optional[str] = None
    # M6.1 (#133) — ASC publish-mode fields; all None/default on bare-WSS
    # builds so pre-#133 model dumps stay shape-compatible.
    # 'api_service' when the build publishes through an API Service Component;
    # 'bare_wss' (or None on legacy builds) otherwise.
    publish_mode: Optional[str] = None
    api_service_component_id: Optional[str] = None
    api_service_package_id: Optional[str] = None
    # created/reused — whether THIS attempt created the ASC package (drives
    # the failure-cleanup plan, which only undoes what this attempt created).
    api_service_package_status: Optional[str] = None
    api_service_deployment_id: Optional[str] = None
    # deployed/reused — same cleanup rationale, plus the probe's
    # route-registration retry window keys off a FRESH ASC deployment.
    api_service_deployment_status: Optional[str] = None
    api_service_deployment_active: Optional[bool] = None
    route_process_ids: List[str] = Field(default_factory=list)
    route_deployments_active: Optional[bool] = None
    collision_paths: List[str] = Field(default_factory=list)
    error: Optional[str] = None
    error_code: Optional[str] = None
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
    # Structured diagnostics for a log/artifact retrieval failure (issue #65). ``LOG_RETRIEVAL_FAILED``
    # / ``ARTIFACT_RETRIEVAL_FAILED`` are diagnostic only and never flip orchestration ``_success``,
    # UNLESS ``require_test_logs=True`` (issue #81) promotes a log failure to ``TEST_LOGS_UNAVAILABLE``.
    error_code: Optional[str] = None
    failed_stage: Optional[str] = None
    next_step: Optional[str] = None
    artifact_error_code: Optional[str] = None
    artifact_failed_stage: Optional[str] = None
    artifact_next_step: Optional[str] = None
    warnings: List[str] = Field(default_factory=list)


class CleanupOperation(BaseModel):
    """One destructive operation a cleanup would perform, in reverse creation order (issue #65).

    A cleanup *plan* is a list of these named operations; by default nothing is executed. Each
    operation names the sibling tool/action and the resource it would undeploy/delete/detach.
    """

    tool: str  # e.g. "manage_deployment" / "manage_runtimes" / "manage_schedules"
    action: str  # e.g. "undeploy" / "delete_package" / "detach_process_atom" / "detach" / "delete"
    resource_type: str  # e.g. "package" / "deployment" / "process_runtime_attachment" / "schedule"
    resource_id: Optional[str] = None
    config: Dict[str, Any] = Field(default_factory=dict)
    reason: str
    destructive: bool = True


class CleanupStage(BaseModel):
    status: StageStatus
    cleanup_id: Optional[str] = None
    # Cleanup planning (issue #65). ``dry_run``/``mutation_allowed`` default to the safe values:
    # a plan that names operations without mutating. They flip only on explicit opt-in.
    dry_run: bool = True
    mutation_allowed: bool = False
    operations: List[CleanupOperation] = Field(default_factory=list)
    results: List[Dict[str, Any]] = Field(default_factory=list)
    warnings: List[str] = Field(default_factory=list)
    next_step: Optional[str] = None


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


def _safe_process_key(comp: Any) -> str:
    """A process component's key as a string, or ``"<unknown>"`` for a missing/blank/non-str key.

    Used so structured error details (e.g. ``BUILD_MULTIPLE_PROCESS_COMPONENTS.process_keys``)
    never carry ``None`` into a consumer that treats the entries as strings (#129 D8/D3).
    """
    key = comp.get("key") if isinstance(comp, dict) else None
    if isinstance(key, str) and key.strip():
        return key
    return "<unknown>"


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
        process_keys = [_safe_process_key(comp) for comp in process_candidates]
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

    # A malformed registry entry (missing/blank/non-string key — including an UNHASHABLE list/dict)
    # must surface as a structured error, not an uncaught exception (#129 D3). Validate BEFORE using
    # process_key as a dict key below: ``results.get(process_key)`` raises TypeError on an unhashable
    # key, and ResolvedBuildTarget (model field ``process_key: str``) raises ValidationError on a
    # non-string key — both must become BUILD_REGISTRY_ENTRY_MALFORMED instead.
    if not isinstance(process_key, str) or not process_key.strip():
        return None, _error(
            BUILD_REGISTRY_ENTRY_MALFORMED,
            f"Build '{build_id}' process component has a missing or non-string key.",
            field="build_id",
            details={"build_id": build_id, "process_key": _safe_process_key(process_comp)},
        )

    result_entry = results.get(process_key)
    result_entry = result_entry if isinstance(result_entry, dict) else {}

    # Resolve the effective component_id, preferring the result entry and falling back to the spec
    # only when the result entry has NONE at all. A plain ``x or y`` would silently drop a present
    # but FALSY non-string id (0, [], False) and misclassify it as "missing" instead of "malformed"
    # (#129 D3 review r2), so use an explicit ``is not None`` fallback that preserves the present
    # value for the type check below.
    result_component_id = result_entry.get("component_id")
    process_component_id = (
        result_component_id if result_component_id is not None
        else process_comp.get("component_id")
    )

    # Present-but-non-string component_id is malformed registry data, distinct from the blank/missing
    # id below; coerce to a structured error before model construction (#129 D3). Checked BEFORE the
    # truthiness test so a falsy non-string (0/[]/False) is classified malformed, not missing.
    if process_component_id is not None and not isinstance(process_component_id, str):
        return None, _error(
            BUILD_REGISTRY_ENTRY_MALFORMED,
            (
                f"Process component '{process_key}' in build '{build_id}' has a non-string "
                "component_id."
            ),
            field="build_id",
            details={"build_id": build_id, "process_key": process_key},
        )

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


def _build_declares_process_extensions(build_id: str) -> bool:
    """True when a recorded build's process declares environment extensions (#102 B4).

    Read-only registry scan: a process component whose ``config.process_extensions``
    carries connection / property / cross-reference override declarations. Used to gate
    the empty-process-overrides deploy guard. Tolerant of a missing/malformed registry
    entry (returns False).
    """
    entry = integration_builder._BUILD_REGISTRY.get(build_id)
    if not isinstance(entry, dict):
        return False
    spec = entry.get("spec")
    components = spec.get("components") if isinstance(spec, dict) else None
    if not isinstance(components, list):
        return False
    for comp in components:
        if not isinstance(comp, dict):
            continue
        config = comp.get("config")
        if not isinstance(config, dict):
            continue
        ext = config.get("process_extensions")
        if isinstance(ext, dict) and any(
            bool(ext.get(k)) for k in ("connections", "properties", "process_properties", "cross_references")
        ):
            return True
    return False


_WSS_CONNECTOR_ALIASES = frozenset({"wss", "web_services", "web_services_server"})

# M6 (#12): how long listener_verify re-probes a freshly created deployment
# whose route still answers the no-route signal (404 local / 401 cloud), and
# the pause between attempts. Live-observed registration lag: ~1 min after a
# first-time deploy on a local atom, up to ~4 min after an apiType flip.
_LISTENER_ROUTE_REGISTRATION_WINDOW_SECONDS = 240
_LISTENER_ROUTE_REGISTRATION_POLL_SECONDS = 15


def _listener_operation_ref_from_process(process_config: Any) -> Optional[str]:
    """The WSS Listen operation reference the process's SOURCE binding carries.

    Returns the operation_id token ('$ref:KEY' or a literal id) when the process
    is listener-sourced, else None. Two recognized shapes: a sync_pipeline
    ``listener`` stage, and a lowered/hand-authored ``source`` binding with a
    WSS connector_type + Listen action.
    """
    if not isinstance(process_config, dict):
        return None
    pipeline = process_config.get("pipeline")
    if isinstance(pipeline, dict):
        for stage in pipeline.get("stages") or []:
            if not isinstance(stage, dict):
                continue
            if str(stage.get("kind") or "").strip().lower() != "listener":
                continue
            stage_config = stage.get("config")
            if isinstance(stage_config, dict):
                operation_id = stage_config.get("operation_id")
                if isinstance(operation_id, str) and operation_id.strip():
                    return operation_id.strip()
            return None
    source = process_config.get("source")
    if isinstance(source, dict):
        connector_type = str(source.get("connector_type") or "").strip().lower()
        action_type = str(source.get("action_type") or "").strip()
        if connector_type in _WSS_CONNECTOR_ALIASES and action_type == "Listen":
            operation_id = source.get("operation_id")
            if isinstance(operation_id, str) and operation_id.strip():
                return operation_id.strip()
    return None


def _recorded_component_id(entry: Dict[str, Any], comp: Dict[str, Any]) -> Optional[str]:
    """The component id a build recorded for a spec component (apply result
    first, declared component_id as fallback)."""
    comp_key = comp.get("key")
    results = entry.get("results")
    if isinstance(results, dict) and isinstance(comp_key, str):
        result_entry = results.get(comp_key)
        if isinstance(result_entry, dict):
            recorded = result_entry.get("component_id")
            if isinstance(recorded, str) and recorded.strip():
                return recorded.strip()
    declared = comp.get("component_id")
    if isinstance(declared, str) and declared.strip():
        return declared.strip()
    return None


def _resolve_asc_binding(
    entry: Dict[str, Any],
    components: List[Any],
    process_comp: Dict[str, Any],
) -> Optional[Dict[str, Any]]:
    """The in-spec API Service Component routing to the deploy-target process
    (M6.1 #133), or None for bare-WSS builds.

    A webservice component binds the process when one of its ``routes[]``
    references it by ``$ref:<process key>`` or by its recorded/declared
    component id. Returns the ASC's spec key, recorded component id, config,
    the matching route's overrides, and every route's resolved process id
    (deploy-both verification needs all of them active).
    """
    process_key = process_comp.get("key")
    process_id = _recorded_component_id(entry, process_comp)
    for comp in components:
        if not isinstance(comp, dict):
            continue
        if str(comp.get("type") or "").strip().lower() != "webservice":
            continue
        config = comp.get("config")
        if not isinstance(config, dict):
            continue
        routes = config.get("routes")
        if not isinstance(routes, list):
            continue
        matched_route: Optional[Dict[str, Any]] = None
        route_process_ids: List[str] = []
        for route in routes:
            if not isinstance(route, dict):
                continue
            ref = route.get("process")
            if ref is None:
                ref = route.get("process_id")
            ref = str(ref or "").strip()
            resolved: Optional[str] = None
            if ref.startswith("$ref:"):
                ref_key = ref[5:].strip()
                for candidate in components:
                    if isinstance(candidate, dict) and candidate.get("key") == ref_key:
                        resolved = _recorded_component_id(entry, candidate)
                        break
                if ref_key and ref_key == process_key and matched_route is None:
                    matched_route = route
            else:
                resolved = ref or None
                if resolved and resolved == process_id and matched_route is None:
                    matched_route = route
            if resolved and resolved not in route_process_ids:
                route_process_ids.append(resolved)
        if matched_route is not None:
            return {
                "key": comp.get("key"),
                "component_id": _recorded_component_id(entry, comp),
                "config": config,
                "route": matched_route,
                "route_process_ids": route_process_ids,
            }
    return None


def _resolve_listener_metadata(build_id: Optional[str]) -> Optional[Dict[str, Any]]:
    """Read the recorded build's WSS listener metadata, or None for non-listener builds.

    Classification ALWAYS keys off the single deploy-target PROCESS's own
    source binding (a sync_pipeline ``listener`` stage or a lowered WSS/Listen
    source) — ``validation_rules.listener`` is caller-suppliable metadata on a
    hand-authored spec, so it is consulted only AFTER the binding confirms the
    deployed process really listens (architect review, M6 #12). Once confirmed,
    the archetype-emitted ``validation_rules.listener`` block is preferred (it
    carries the richer field set); otherwise the referenced Listen operation
    component is resolved in-spec and the endpoint fields derived from it. A
    spec that merely CONTAINS a WSS operation the deployed process does not
    listen on is NOT a listener build (Codex review, M6 #12) — and a confirmed
    listener whose operation is an external literal id with no metadata block
    cannot be endpoint-derived, so it returns None (no listener_verify rather
    than a wrong probe). Read-only registry inspection; tolerant of a
    missing/malformed entry (returns None).
    """
    entry = integration_builder._BUILD_REGISTRY.get(build_id)
    if not isinstance(entry, dict):
        return None
    spec = entry.get("spec")
    if not isinstance(spec, dict):
        return None
    components = spec.get("components")
    if not isinstance(components, list):
        return None

    # 1. Confirm the deploy target listens: the spec's single process (mirrors
    #    _resolve_build_deployment_target; ambiguous/multi-process specs are
    #    rejected there anyway) must carry a WSS Listen source binding.
    process_comps = [
        comp
        for comp in components
        if isinstance(comp, dict) and _effective_component_type(comp) == "process"
    ]
    if len(process_comps) != 1:
        return None
    operation_ref = _listener_operation_ref_from_process(process_comps[0].get("config"))
    if not operation_ref:
        return None

    # 1.5. M6.1 (#133): detect an in-spec API Service Component routing to the
    # confirmed listener process (publish_mode='api_service'). Detection keys
    # off the ASC's own route references, not caller metadata.
    asc_binding = _resolve_asc_binding(entry, components, process_comps[0])

    def _attach_asc(meta: Dict[str, Any]) -> Dict[str, Any]:
        if asc_binding is None:
            meta.setdefault("publish_mode", "bare_wss")
            return meta
        meta["publish_mode"] = "api_service"
        meta["api_service_component_key"] = asc_binding.get("key")
        if asc_binding.get("component_id"):
            meta["api_service_component_id"] = asc_binding["component_id"]
        meta["route_process_ids"] = list(asc_binding.get("route_process_ids") or [])
        # The ASC base urlPath — the platform's SHADOWING granularity: one
        # deployed webservice component serves per base path, whole-component
        # first-deployed-wins (live-proven A/B/A, #133 QA 2026-07-05). The
        # verify stage's collision scan compares it against other deployed
        # ASCs' bases.
        meta["api_service_base_url_path"] = str(
            asc_binding["config"].get("base_url_path") or ""
        )
        return meta

    # 2. Binding confirmed — prefer the archetype-emitted metadata block.
    validation_rules = spec.get("validation_rules")
    if isinstance(validation_rules, dict):
        listener = validation_rules.get("listener")
        if isinstance(listener, dict) and listener.get("endpoint_path"):
            return _attach_asc(dict(listener))

    # 3. No metadata block — resolve the referenced Listen operation component
    #    in-spec ($ref:KEY by component key; a literal id by recorded/declared
    #    component_id) and derive the endpoint fields from its config.
    op_comp: Optional[Dict[str, Any]] = None
    if operation_ref.startswith("$ref:"):
        ref_key = operation_ref[5:].strip()
        for comp in components:
            if isinstance(comp, dict) and comp.get("key") == ref_key:
                op_comp = comp
                break
    else:
        results = entry.get("results")
        results = results if isinstance(results, dict) else {}
        for comp in components:
            if not isinstance(comp, dict):
                continue
            comp_key = comp.get("key")
            result_entry = results.get(comp_key) if isinstance(comp_key, str) else None
            recorded_id = (
                result_entry.get("component_id") if isinstance(result_entry, dict) else None
            )
            if operation_ref in (comp.get("component_id"), recorded_id):
                op_comp = comp
                break
    if op_comp is None:
        return None

    config = op_comp.get("config")
    if not isinstance(config, dict):
        return None
    connector_type = str(config.get("connector_type") or "").strip().lower()
    operation_mode = str(config.get("operation_mode") or "").strip().lower()
    if connector_type not in _WSS_CONNECTOR_ALIASES or operation_mode != "listen":
        return None
    object_name = str(config.get("object_name") or "").strip()
    if not object_name:
        return None
    operation_type = str(config.get("operation_type") or "EXECUTE").strip().upper()
    input_type = str(config.get("input_type") or "singlejson").strip().lower()
    meta = {
        "object_name": object_name,
        "operation_type": operation_type,
        "input_type": input_type,
        "output_type": str(config.get("output_type") or "none").strip().lower(),
        "http_method": wss_http_method(input_type),
        "endpoint_path": compute_wss_endpoint(operation_type, object_name),
    }
    if asc_binding is not None:
        # Hand-authored ASC spec without a metadata block: derive the
        # effective /ws/rest route from the ASC config + the resolved WSS
        # operation via the shared inherit formula (#133).
        asc_config = asc_binding["config"]
        route = asc_binding["route"]
        effective = effective_api_service_route(
            str(asc_config.get("base_url_path") or ""),
            {
                "http_method": route.get("http_method"),
                "url_path": route.get("url_path"),
                "object_name": route.get("object_name"),
                "input_type": route.get("input_type"),
                "output_type": route.get("output_type"),
            },
            {
                "object_name": object_name,
                "input_type": input_type,
                "output_type": str(config.get("output_type") or "none").strip().lower(),
            },
        )
        meta["http_method"] = effective["method"]
        meta["endpoint_path"] = effective["path"]
        meta["bare_wss_endpoint_path"] = compute_wss_endpoint(operation_type, object_name)
    return _attach_asc(meta)


def _listener_probe(
    url: str,
    *,
    method: str,
    payload: Optional[bytes],
    headers: Dict[str, str],
    timeout_seconds: int,
) -> Tuple[Optional[int], Optional[str]]:
    """One bounded HTTP probe; returns (status_code, error_text).

    4xx/5xx come back as (status, None) — they are triage signals, not
    transport failures. Only a network/URL error yields (None, error_text).
    """
    request = urllib.request.Request(url, data=payload, method=method)
    for header_name, header_value in headers.items():
        request.add_header(header_name, header_value)
    try:
        with urllib.request.urlopen(request, timeout=timeout_seconds) as response:
            return int(response.status), None
    except urllib.error.HTTPError as exc:
        return int(exc.code), None
    except Exception as exc:  # URLError / timeout / SSL
        return None, str(exc)


def _run_listener_verify_stage(
    boomi_client: Any,
    profile: Optional[str],
    request: "OrchestrateDeployRequest",
    *,
    target: ResolvedBuildTarget,
    environment_id: Optional[str],
    runtime_id: Optional[str],
    listener_meta: Dict[str, Any],
    deployment_stage: DeploymentStage,
    creds: Optional[Dict[str, str]] = None,
    package_version: Optional[str] = None,
) -> Tuple[ListenerVerifyStage, Optional[OrchestrateDeployError]]:
    """Verify a deployed WSS listener route (M6 #12; ASC publish mode M6.1 #133).

    Order: SharedServerInformation preflight (apiType decides the publish
    mode: bare WSS on basic/intermediate, API Service Component on advanced)
    -> [ASC mode: package+deploy the ASC and check every route process active]
    -> deployment-active check -> component-query collision check ->
    authenticated live probe (bare mode keeps the one-shot objectName-casing
    fallback on 404; /ws/rest paths are case-verbatim) -> execution-record
    readback. ``ListenerStatus`` is never consulted (live-proven empty for
    WSS/ASC routes).
    """
    object_name = str(listener_meta.get("object_name") or "").strip()
    operation_type = str(listener_meta.get("operation_type") or "").strip().upper()
    input_type = str(listener_meta.get("input_type") or "singlejson").strip().lower()
    http_method = str(
        listener_meta.get("http_method") or wss_http_method(input_type)
    ).strip().upper()
    publish_mode = str(listener_meta.get("publish_mode") or "bare_wss").strip().lower()
    asc_mode = publish_mode == "api_service"
    endpoint_path = str(
        listener_meta.get("endpoint_path")
        or ("" if asc_mode else compute_wss_endpoint(operation_type, object_name))
    )
    route_process_ids = [
        str(pid).strip()
        for pid in (listener_meta.get("route_process_ids") or [])
        if isinstance(pid, str) and str(pid).strip()
    ]
    stage = ListenerVerifyStage(
        status="failed",
        endpoint_path=endpoint_path or None,
        http_method=http_method,
        publish_mode=publish_mode,
        route_process_ids=route_process_ids,
    )

    def _fail(code: str, message: str, **details: Any) -> Tuple[ListenerVerifyStage, OrchestrateDeployError]:
        stage.error = message
        stage.error_code = code
        return stage, _error(code, message, details=details or None)

    if asc_mode and not endpoint_path:
        # A confirmed listener published through an ASC whose effective route
        # could not be resolved (unreadable config) — fail structured rather
        # than probing a wrong path.
        return _fail(
            LISTENER_ASC_ROUTE_INVALID,
            "The build publishes through an API Service Component but its "
            "effective /ws/rest route could not be resolved from the build "
            "registry — cannot probe.",
        )

    # 1. Shared Web Server preflight: apiType decides the listener pattern; auth
    #    decides the probe credentials.
    try:
        info_result = manage_shared_resources_action(
            boomi_client, profile, "get_server_info", {"resource_id": runtime_id}
        )
    except Exception as exc:
        return _fail(
            LISTENER_SERVER_INFO_FAILED,
            f"SharedServerInformation preflight failed for runtime {runtime_id}: {exc}",
            runtime_id=runtime_id,
        )
    if not isinstance(info_result, dict) or not info_result.get("_success"):
        detail = (info_result or {}).get("error") if isinstance(info_result, dict) else None
        return _fail(
            LISTENER_SERVER_INFO_FAILED,
            "SharedServerInformation preflight failed for runtime "
            f"{runtime_id}: {detail or 'no server_info returned'}",
            runtime_id=runtime_id,
        )
    server_info = info_result.get("server_info") or {}
    api_type = str(server_info.get("api_type") or "").strip().lower()
    auth = str(server_info.get("auth") or server_info.get("min_auth") or "none").strip().lower()
    stage.api_type = api_type or None
    stage.auth = auth or None
    if asc_mode:
        # ASC routes exist only through the /ws/rest gateway of an
        # apiType=advanced Shared Web Server; on basic/intermediate the ASC
        # deploys clean but does not serve (live-confirmed, #133).
        if api_type != "advanced":
            return _fail(
                LISTENER_ASC_UNSUPPORTED_FOR_APITYPE,
                "The build publishes through an API Service Component, but the "
                f"runtime's Shared Web Server apiType is {api_type or 'unknown'!r} "
                "— ASC /ws/rest routes are served only on apiType='advanced'. "
                "Switch the runtime to advanced or disable the asc_wrapper "
                "(bare WSS serves basic/intermediate).",
                runtime_id=runtime_id,
                api_type=api_type,
            )
    elif api_type == "advanced":
        return _fail(
            LISTENER_ASC_REQUIRED,
            "The runtime's Shared Web Server apiType is 'advanced', which does not "
            "serve bare /ws/simple WSS routes — the deploy succeeds but every "
            "route 404s. Publish through an API Service Component instead "
            "(listener archetypes: asc_wrapper.enabled=true; issue #133).",
            runtime_id=runtime_id,
            api_type=api_type,
        )

    # 1.5. ASC mode (M6.1 #133): package + deploy the API Service Component
    #      itself and confirm every route process is active in the SAME
    #      environment — ASC deploy does not cascade (live-confirmed). Runs
    #      AFTER the apiType preflight so a tier mismatch never deploys an ASC.
    if asc_mode:
        asc_component_id = str(
            listener_meta.get("api_service_component_id") or ""
        ).strip()
        if not asc_component_id:
            return _fail(
                LISTENER_ASC_ROUTE_INVALID,
                "The build publishes through an API Service Component but no ASC "
                "component id was recorded in the build registry — apply the "
                "build (or supply the webservice component id) before verifying.",
            )
        stage.api_service_component_id = asc_component_id
        # Same version resolution as the process package (explicit override,
        # else the build id) so the ASC package pairs with the process one.
        asc_version = (
            str(package_version or "").strip()
            or _effective_package_version(request.package_version, str(request.build_id or ""))
        )
        asc_package, asc_deployment, asc_error = _ensure_api_service_deployment(
            boomi_client,
            profile,
            asc_component_id=asc_component_id,
            environment_id=str(environment_id),
            package_version=asc_version,
        )
        if asc_package is not None:
            stage.api_service_package_id = asc_package.package_id
            stage.api_service_package_status = asc_package.status
        if asc_deployment is not None:
            stage.api_service_deployment_id = asc_deployment.deployment_id
            stage.api_service_deployment_status = asc_deployment.status
            stage.api_service_deployment_active = bool(asc_deployment.active)
        if asc_error is not None:
            return _fail(
                LISTENER_ASC_DEPLOYMENT_INACTIVE,
                "The API Service Component could not be packaged/deployed to the "
                f"target environment: {asc_error.message}",
                asc_component_id=asc_component_id,
                environment_id=environment_id,
            )
        if not stage.api_service_deployment_active:
            return _fail(
                LISTENER_ASC_DEPLOYMENT_INACTIVE,
                "The API Service Component deployment is not active in the target "
                "environment — its /ws/rest routes cannot register.",
                asc_component_id=asc_component_id,
                environment_id=environment_id,
                deployment_id=stage.api_service_deployment_id,
            )
        # Every route process must be active in the same environment. The
        # deploy-target process is covered by this run's deployment stage;
        # other route processes (hand-authored multi-route ASCs) are checked
        # against the environment's active deployments.
        other_route_ids = [
            pid for pid in route_process_ids if pid != target.process_component_id
        ]
        if other_route_ids:
            try:
                route_deployments = manage_deployment_action(
                    boomi_client,
                    profile=profile,
                    action="list_deployments",
                    config_data={"environment_id": environment_id, "active_only": True},
                )
            except Exception as exc:
                route_deployments = {"_success": False, "error": str(exc)}
            if not (
                isinstance(route_deployments, dict)
                and route_deployments.get("_success")
            ):
                return _fail(
                    LISTENER_ROUTE_PROCESS_DEPLOYMENT_INACTIVE,
                    "Could not list active deployments to confirm the ASC route "
                    "processes are deployed: "
                    + str((route_deployments or {}).get("error") or "unknown error"),
                    environment_id=environment_id,
                )
            active_component_ids = {
                dep.get("component_id")
                for dep in (route_deployments.get("deployments") or [])
                if isinstance(dep, dict)
            }
            missing_routes = [
                pid for pid in other_route_ids if pid not in active_component_ids
            ]
            if missing_routes:
                stage.route_deployments_active = False
                return _fail(
                    LISTENER_ROUTE_PROCESS_DEPLOYMENT_INACTIVE,
                    "ASC deploy does not cascade: every route process must be "
                    "independently deployed to the same environment. Missing "
                    f"active deployment(s) for route process(es): {missing_routes}.",
                    environment_id=environment_id,
                    missing_route_process_ids=missing_routes,
                )
        stage.route_deployments_active = True

    # 2. Deployment-active check (reuses this run's deployment stage result —
    #    the deploy stage already confirmed/reused the active deployment).
    deployment_active = bool(deployment_stage.active)
    stage.deployment_active = deployment_active
    if not deployment_active:
        return _fail(
            LISTENER_DEPLOYMENT_INACTIVE,
            "The listener process deployment is not active in the target environment, "
            "so no route can be registered.",
            environment_id=environment_id,
            deployment_id=deployment_stage.deployment_id,
        )

    # 3. Collision check — component/deployment-query based, NEVER a pre-probe:
    #    on Boomi-managed clouds every unregistered path answers a uniform 401,
    #    so probing is uninformative (live-confirmed 2026-07-04).
    #    Bare mode: read each other active deployed process (bounded), resolve
    #    its Listen operation, and compare lowercase(operationType)+objectName
    #    case-insensitively (the served /ws/simple path re-cases the first
    #    objectName letter, so a case-only difference IS the same path).
    #    ASC mode (#133): the platform's shadowing granularity is the ASC's
    #    BASE urlPath — the perimeter binds ONE deployed webservice component
    #    per base path and the first-deployed wins for the ENTIRE component
    #    (routes do NOT merge across ASCs sharing a base; live-proven A/B/A
    #    2026-07-05: same component 404'd on base "" while a fixture ASC held
    #    it, served 200 on a distinct base). Read each other active deployed
    #    webservice component and flag ANY equal base (case-verbatim); the
    #    per-route effective method+path comparison is kept as a defensive
    #    second signal (it catches interior-slash equivalence across bases).
    collision_count = 0
    collision_paths: List[str] = []
    _COLLISION_SCAN_CAP = 25
    try:
        deployments_result = manage_deployment_action(
            boomi_client,
            profile=profile,
            action="list_deployments",
            config_data={"environment_id": environment_id, "active_only": True},
        )
    except Exception as exc:
        deployments_result = {"_success": False, "error": str(exc)}
    if isinstance(deployments_result, dict) and deployments_result.get("_success"):
        deployed = deployments_result.get("deployments") or []
        if asc_mode:
            my_route_key = f"{http_method} {endpoint_path}"
            my_base = normalize_api_service_path_segment(
                listener_meta.get("api_service_base_url_path")
            )
            other_asc_ids = []
            for dep in deployed:
                if not isinstance(dep, dict):
                    continue
                comp_id = dep.get("component_id")
                comp_type = str(dep.get("component_type") or "").strip().lower()
                if not comp_id or comp_id == stage.api_service_component_id:
                    continue
                if comp_type != "webservice":
                    continue
                if comp_id not in other_asc_ids:
                    other_asc_ids.append(comp_id)
            if len(other_asc_ids) > _COLLISION_SCAN_CAP:
                stage.warnings.append(
                    f"[LISTENER_COLLISION_SCAN_CAPPED] {len(other_asc_ids)} other deployed "
                    f"API Service Components in the environment; only the first "
                    f"{_COLLISION_SCAN_CAP} were scanned for /ws/rest path collisions."
                )
                other_asc_ids = other_asc_ids[:_COLLISION_SCAN_CAP]
            for comp_id in other_asc_ids:
                try:
                    asc_read = component_get_xml(boomi_client, comp_id)
                    parsed_asc = _parse_api_service_xml(asc_read["xml"])
                except Exception:
                    stage.warnings.append(
                        f"[LISTENER_COLLISION_SCAN_INCOMPLETE] could not read deployed API "
                        f"Service Component {comp_id}; its routes were not collision-checked."
                    )
                    continue
                # PRIMARY signal: equal base urlPath = whole-component
                # shadowing, regardless of the routes' paths (#133 QA #147).
                other_base = normalize_api_service_path_segment(
                    parsed_asc.get("base_url_path")
                )
                if other_base == my_base:
                    collision_count += 1
                    base_key = (
                        f"BASE /ws/rest/{my_base or ''} (component {comp_id})"
                    )
                    if base_key not in collision_paths:
                        collision_paths.append(base_key)
                    continue
                for route in parsed_asc.get("routes") or []:
                    overrides = {
                        k: (v if v is not None else "")
                        for k, v in (route.get("overrides") or {}).items()
                    }
                    wss_op_config = None
                    route_pid = route.get("process_id")
                    needs_op = not str(overrides.get("object_name") or "").strip() or not (
                        str(overrides.get("http_method") or "").strip()
                        or str(overrides.get("input_type") or "").strip()
                    )
                    if route_pid and needs_op:
                        try:
                            route_proc = component_get_xml(boomi_client, route_pid)
                            binding = _extract_wss_listen_binding(route_proc["xml"])
                            if binding["has_listen"] and binding["operation_id"]:
                                op_read = component_get_xml(
                                    boomi_client, binding["operation_id"]
                                )
                                wss_op_config = _extract_wss_operation_config(
                                    op_read["xml"]
                                )
                        except Exception:
                            stage.warnings.append(
                                f"[LISTENER_COLLISION_SCAN_INCOMPLETE] could not resolve the "
                                f"WSS operation behind ASC {comp_id} route process "
                                f"{route_pid}; its effective path was not collision-checked."
                            )
                            continue
                    effective = effective_api_service_route(
                        parsed_asc.get("base_url_path") or "", overrides, wss_op_config
                    )
                    other_key = f"{effective['method']} {effective['path']}"
                    if other_key == my_route_key:
                        collision_count += 1
                        if other_key not in collision_paths:
                            collision_paths.append(other_key)
        else:
            my_path_key = f"{operation_type.lower()}{object_name}".casefold()
            other_process_ids = []
            for dep in deployed:
                if not isinstance(dep, dict):
                    continue
                comp_id = dep.get("component_id")
                comp_type = str(dep.get("component_type") or "").strip().lower()
                if not comp_id or comp_id == target.process_component_id:
                    continue
                if comp_type and comp_type != "process":
                    continue
                if comp_id not in other_process_ids:
                    other_process_ids.append(comp_id)
            if len(other_process_ids) > _COLLISION_SCAN_CAP:
                stage.warnings.append(
                    f"[LISTENER_COLLISION_SCAN_CAPPED] {len(other_process_ids)} other deployed "
                    f"processes in the environment; only the first {_COLLISION_SCAN_CAP} were "
                    "scanned for WSS path collisions."
                )
                other_process_ids = other_process_ids[:_COLLISION_SCAN_CAP]
            for comp_id in other_process_ids:
                try:
                    process_read = component_get_xml(boomi_client, comp_id)
                    process_root = ET.fromstring(process_read["xml"])
                except Exception:
                    stage.warnings.append(
                        f"[LISTENER_COLLISION_SCAN_INCOMPLETE] could not read deployed process "
                        f"{comp_id}; its WSS path (if any) was not collision-checked."
                    )
                    continue
                listen_op_ids = [
                    ca.get("operationId")
                    for ca in process_root.iter("connectoraction")
                    if ca.get("actionType") == "Listen"
                    and str(ca.get("connectorType") or "").lower() == "wss"
                    and ca.get("operationId")
                ]
                for op_id in listen_op_ids:
                    try:
                        op_read = component_get_xml(boomi_client, op_id)
                        op_root = ET.fromstring(op_read["xml"])
                    except Exception:
                        stage.warnings.append(
                            f"[LISTENER_COLLISION_SCAN_INCOMPLETE] could not read WSS operation "
                            f"{op_id} referenced by deployed process {comp_id}."
                        )
                        continue
                    for action in op_root.iter("WebServicesServerListenAction"):
                        other_key = (
                            f"{str(action.get('operationType') or '').lower()}"
                            f"{str(action.get('objectName') or '')}"
                        ).casefold()
                        if other_key and other_key == my_path_key:
                            collision_count += 1
    else:
        stage.warnings.append(
            "[LISTENER_COLLISION_SCAN_UNAVAILABLE] active-deployment listing failed; "
            "WSS path collisions were not checked: "
            + str((deployments_result or {}).get("error") or "unknown error")
        )
    stage.collision_count = collision_count
    stage.collision_paths = collision_paths
    if collision_count:
        if asc_mode:
            return _fail(
                LISTENER_ASC_COLLISION,
                f"{collision_count} collision(s) with other deployed API Service "
                f"Component(s) in this environment ({collision_paths}). Shadowing "
                "granularity is the ASC's BASE urlPath: the platform binds ONE "
                "deployed webservice component per base and the FIRST-deployed "
                "serves — a later same-base ASC is shadowed IN ITS ENTIRETY, even "
                "for routes with unique paths (live-proven 2026-07-05); "
                "undeploying the winner does NOT activate the loser. Choose a "
                "distinct base_url_path or undeploy the colliding ASC.",
                endpoint_path=endpoint_path,
                collision_count=collision_count,
                collision_paths=collision_paths,
            )
        return _fail(
            LISTENER_PATH_COLLISION,
            f"{collision_count} other deployed process(es) in this environment serve the "
            f"same WSS endpoint path ({endpoint_path}); Boomi routes duplicate paths "
            "unpredictably. Use a unique objectName or undeploy the colliding process.",
            endpoint_path=endpoint_path,
            collision_count=collision_count,
        )

    # 4. Live probe of the computed endpoint. Base URL: explicit override first
    #    (e.g. a docker atom reachable on localhost), else the server-info url.
    base_url = (request.listener_base_url or "").strip() or str(server_info.get("url") or "").strip()
    if not base_url:
        return _fail(
            LISTENER_PROBE_FAILED,
            "No probe base URL: SharedServerInformation returned no url and no "
            "listener_base_url override was supplied.",
            runtime_id=runtime_id,
        )
    headers: Dict[str, str] = {}
    payload: Optional[bytes] = None
    if http_method != "GET":
        payload_text = request.listener_test_payload
        if payload_text is None and input_type.endswith("json"):
            payload_text = "{}"
        if payload_text is None:
            payload_text = ""
        payload = payload_text.encode("utf-8")
        if input_type.endswith("json"):
            headers["Content-Type"] = "application/json"
        elif input_type.endswith("xml"):
            headers["Content-Type"] = "application/xml"
        else:
            headers["Content-Type"] = "text/plain"
    if auth != "none":
        auth_token = str(server_info.get("auth_token") or "").strip()
        auth_username = (
            (request.listener_auth_username or "").strip()
            or str((creds or {}).get("account_id") or "").strip()
        )
        if not auth_token or not auth_username:
            return _fail(
                LISTENER_PROBE_FAILED,
                "The runtime requires Basic auth but no usable credentials were "
                "resolved: the Shared Web Server auth token is generated once in the "
                "runtime's Shared Web Server panel (UI-only step); after that it is "
                "readable via get_server_info. Provide listener_auth_username for an "
                "instance-id user (cloud attachments) if the account id is not the "
                "expected username.",
                runtime_id=runtime_id,
                auth=auth,
            )
        basic = base64.b64encode(f"{auth_username}:{auth_token}".encode("utf-8")).decode("ascii")
        headers["Authorization"] = f"Basic {basic}"

    # Baseline execution-id snapshot BEFORE the probe: the readback must prove
    # the probe itself triggered an execution, so a record that already existed
    # (listener traffic shortly before verification) must not count (Codex
    # review, M6 #12). Ids in this window are excluded from the readback match.
    probe_started_at = datetime.now(timezone.utc)
    window_start = (probe_started_at - timedelta(minutes=2)).strftime("%Y-%m-%dT%H:%M:%SZ")
    baseline_execution_ids: set = set()
    baseline_unavailable: Optional[str] = None
    try:
        baseline_result = monitor_platform_action(
            boomi_client,
            profile,
            "execution_records",
            config_data={
                "process_id": target.process_component_id,
                "start_date": window_start,
                "limit": 50,
            },
        )
    except Exception as exc:
        baseline_result = {"_success": False, "error": str(exc)}
    if isinstance(baseline_result, dict) and baseline_result.get("_success"):
        for record in baseline_result.get("execution_records") or []:
            if isinstance(record, dict) and record.get("execution_id"):
                baseline_execution_ids.add(record["execution_id"])
    else:
        baseline_unavailable = str((baseline_result or {}).get("error") or "unknown error")
        stage.warnings.append(
            "[LISTENER_READBACK_BASELINE_UNAVAILABLE] the pre-probe execution-record "
            f"baseline query failed ({baseline_unavailable}); the readback cannot "
            "distinguish the probe's execution from listener traffic in the last "
            "2 minutes — treat execution_record_found as weaker evidence."
        )
    stage.readback_baseline_available = baseline_unavailable is None

    url = base_url.rstrip("/") + endpoint_path
    # A FRESHLY CREATED deployment registers its WSS route asynchronously —
    # live-observed 2026-07-04: ~1 min on a local atom after a first-time
    # deploy, up to ~4 min after an apiType flip — during which the runtime
    # answers the no-route signal (404 locally; uniform 401 on Boomi-managed
    # clouds). Retry those signals within a bounded window so the headline
    # deploy-then-verify call does not fail on registration lag. A REUSED
    # active deployment serves immediately, so it keeps single-probe behavior.
    # ASC mode: the /ws/rest route registration follows the ASC deployment
    # (live-observed ~75s after an ASC deploy, #133 QA), so a FRESH ASC
    # deployment opens the retry window even when the process deployment was
    # reused (Codex review r1).
    registration_wait = deployment_stage.status == "deployed" or (
        asc_mode and stage.api_service_deployment_status == "deployed"
    )
    registration_deadline = time.monotonic() + _LISTENER_ROUTE_REGISTRATION_WINDOW_SECONDS
    probe_attempts = 0
    while True:
        probe_attempts += 1
        status_code, probe_error = _listener_probe(
            url,
            method=http_method,
            payload=payload,
            headers=headers,
            timeout_seconds=request.listener_probe_timeout_seconds,
        )
        stage.endpoint_url = url
        stage.probe_status_code = status_code
        served_casing: Optional[str] = None
        if status_code is not None and 200 <= status_code < 300:
            # Bare mode: the primary path carries the sentence-cased objectName
            # (live-settled 2026-07-04: Boomi upper-cases the first letter on
            # the served path). ASC /ws/rest paths are verbatim (#133).
            served_casing = "verbatim" if asc_mode else "sentence_case"
        elif status_code == 404 and not asc_mode:
            # Defensive verbatim fallback (BARE mode only — /ws/rest paths are
            # already verbatim, there is no casing transformation to fall back
            # from): sentence-casing was settled live on one runtime/tier; if a
            # runtime ever serves the verbatim spelling instead, one retry
            # keeps the verify green and records the contradiction.
            verbatim_path = f"/ws/simple/{operation_type.lower()}{object_name}"
            if verbatim_path != endpoint_path:
                alt_url = base_url.rstrip("/") + verbatim_path
                alt_status, _alt_error = _listener_probe(
                    alt_url,
                    method=http_method,
                    payload=payload,
                    headers=headers,
                    timeout_seconds=request.listener_probe_timeout_seconds,
                )
                if alt_status is not None and 200 <= alt_status < 300:
                    stage.endpoint_path = verbatim_path
                    stage.endpoint_url = alt_url
                    stage.probe_status_code = alt_status
                    status_code = alt_status
                    served_casing = "verbatim"
                    stage.warnings.append(
                        "[LISTENER_OBJECT_NAME_VERBATIM] the runtime served the "
                        f"verbatim path {verbatim_path!r}, not the sentence-cased "
                        f"{endpoint_path!r} — this contradicts the live-settled "
                        "/ws/simple casing (2026-07-04); record the runtime/tier."
                    )
        stage.served_object_name_casing = served_casing
        if (
            status_code in (401, 404)
            and registration_wait
            and time.monotonic() < registration_deadline
        ):
            time.sleep(_LISTENER_ROUTE_REGISTRATION_POLL_SECONDS)
            continue
        break
    if probe_attempts > 1 and status_code is not None and 200 <= status_code < 300:
        stage.warnings.append(
            f"[LISTENER_ROUTE_REGISTRATION_LAG] the route answered only on probe "
            f"attempt {probe_attempts} — a fresh deploy registers its WSS route "
            "asynchronously (~1-4 min observed live); no action needed."
        )

    if status_code is None:
        return _fail(
            LISTENER_PROBE_FAILED,
            f"Listener endpoint probe could not reach {url}: {probe_error}",
            endpoint_url=url,
        )
    if not (200 <= status_code < 300):
        if status_code == 401:
            triage = (
                "401 with the supplied credentials means either no route is registered "
                "for this tenant yet (the pre-first-route baseline on Boomi-managed "
                "clouds is a uniform 401 even with valid credentials — token sync can "
                "also lag a fresh deploy) or the credentials are wrong."
            )
        elif status_code == 404:
            triage = (
                "404 after authentication means the request reached the runtime but no "
                "route matches this path — check apiType, the path segments, and that "
                + (
                    "BOTH the API Service Component and its route process are deployed "
                    "(ASC deploy does not cascade). If another ASC sharing this base "
                    "urlPath deployed EARLIER, it shadows this ENTIRE component "
                    "(base-granularity first-deployed-wins) — the collision scan "
                    "flags known same-base ASCs, but one deployed mid-verify or "
                    "beyond the scan cap would surface here."
                    if asc_mode
                    else "the listener process (not just a parent component) is "
                    "deployed. On an apiType=advanced runtime a bare /ws/simple 404 "
                    "is the tier mismatch itself (the LISTENER_ASC_REQUIRED "
                    "preflight should have caught it)."
                )
            )
        else:
            triage = "Unexpected status from the listener endpoint."
        return _fail(
            LISTENER_PROBE_FAILED,
            f"Listener endpoint probe returned HTTP {status_code} for {url}. {triage}",
            endpoint_url=url,
            probe_status_code=status_code,
        )

    # 5. Execution-record readback: HTTP 200 is only the ack (with outputType=
    #    none it is fully decoupled from process outcome — live-proven via an
    #    ERROR execution behind a 200). Require a record NOT in the pre-probe
    #    baseline, so pre-existing listener traffic can never stand in for the
    #    probe's own execution (degraded to any-record + warning only when the
    #    baseline query itself failed above).
    record_found = False
    execution_id: Optional[str] = None
    execution_status: Optional[str] = None
    readback_deadline = time.monotonic() + 60
    readback_error: Optional[str] = None
    while time.monotonic() < readback_deadline:
        try:
            records_result = monitor_platform_action(
                boomi_client,
                profile,
                "execution_records",
                config_data={
                    "process_id": target.process_component_id,
                    "start_date": window_start,
                    "limit": 50,
                },
            )
        except Exception as exc:
            records_result = {"_success": False, "error": str(exc)}
        if isinstance(records_result, dict) and records_result.get("_success"):
            readback_error = None
            for record in records_result.get("execution_records") or []:
                if not isinstance(record, dict):
                    continue
                record_id = record.get("execution_id")
                if baseline_unavailable is None:
                    # Strict mode: only an id-bearing record OUTSIDE the
                    # baseline proves the probe triggered an execution.
                    if not record_id or record_id in baseline_execution_ids:
                        continue
                record_found = True
                execution_id = record_id or execution_id
                execution_status = record.get("status") or execution_status
            if record_found:
                break
        else:
            readback_error = str((records_result or {}).get("error") or "unknown error")
        time.sleep(5)
    stage.execution_record_found = record_found
    stage.execution_id = execution_id
    stage.execution_status = execution_status
    if not record_found:
        detail = f" (record query error: {readback_error})" if readback_error else ""
        return _fail(
            LISTENER_EXECUTION_RECORD_MISSING,
            "The listener endpoint acknowledged the probe but no execution record "
            "appeared for the process within the readback window — the HTTP ack is "
            f"decoupled from process execution, so this run is unverified{detail}.",
            endpoint_url=url,
            probe_status_code=status_code,
        )
    if execution_status and execution_status.upper() not in ("COMPLETE",):
        stage.warnings.append(
            f"[LISTENER_EXECUTION_{execution_status.upper()}] the probe returned HTTP "
            f"{status_code} but the triggered execution finished {execution_status} — "
            "HTTP 200 is an ack, not process success; inspect the execution log."
        )

    stage.status = "completed"
    return stage, None


def _error_response(
    error_message: str,
    errors: List[OrchestrateDeployError],
    *,
    profile: Optional[str] = None,
    build_id: Optional[Any] = None,
    dry_run: Any = True,
    plan_only: bool = False,
    target: Optional[ResolvedBuildTarget] = None,
    environment_id: Optional[Any] = None,
    runtime_id: Optional[Any] = None,
    schedule_override: Optional[Any] = None,
    run_test: Any = False,
    package_version: Optional[Any] = None,
) -> Dict[str, Any]:
    """Full-envelope response for a pre-stage/early failure (#129 D2).

    Early failures now return the SAME top-level shape as late-stage failures — every stage key
    present as a ``blocked`` placeholder — so a caller that branches on any stage key does not
    silently break when the error is raised before the first SDK call. Context is optional and
    defensively sanitized: raw request echoes (from the initial ``ValidationError`` path) are only
    passed through to the placeholder models when they already have the expected type, so a
    mistyped raw ``build_id``/``environment_id`` never makes a placeholder model raise.

    ``target`` is supplied only for errors that occur AFTER build-target resolution (schedule /
    process-overrides content validation); pre-resolution errors leave it ``None`` with an empty
    ``ComponentSummary``. No ``error_code``/``failed_stage``/``prior_stage_summary``/``next_step``
    metadata is attached — those belong to real-run stage failures, not pre-stage validation.
    """
    # Sanitize raw context: only echo values that already have the expected type into the
    # placeholder stage models AND the top-level envelope (the initial ValidationError path may
    # pass a list/dict/other non-string here). ``profile`` is sanitized too so it can never echo a
    # non-string raw object into the response contract or break JSON-serializability (#129 review).
    safe_profile = profile if isinstance(profile, str) else None
    safe_build_id = build_id if isinstance(build_id, str) else None
    safe_environment_id = environment_id if isinstance(environment_id, str) else None
    safe_runtime_id = runtime_id if isinstance(runtime_id, str) else None
    safe_package_version = package_version if isinstance(package_version, str) else None
    safe_schedule_override = schedule_override if isinstance(schedule_override, dict) else None
    # Type-check the bool flags rather than truthily coercing (bool("banana") == True would echo a
    # misleading run_test/dry_run into an INVALID_REQUEST envelope); a non-bool raw value falls back
    # to the field default (#129 review r2). This matches the str-or-None sanitization above.
    dry_run_flag = dry_run if isinstance(dry_run, bool) else True
    run_test_flag = run_test if isinstance(run_test, bool) else False

    if target is not None:
        pkg_version = _effective_package_version(safe_package_version, safe_build_id)
        package = PackageStage(
            status="blocked",
            component_id=target.process_component_id,
            component_type="process",
            package_version=pkg_version,
        )
        component_summary = target.component_summary
        integration_name = target.integration_name
    else:
        package = PackageStage(status="blocked", package_version=safe_package_version)
        component_summary = ComponentSummary()
        integration_name = None

    deployment = DeploymentStage(status="blocked", environment_id=safe_environment_id)
    runtime_attachment = RuntimeAttachmentStage(status="blocked", runtime_id=safe_runtime_id)
    schedule = ScheduleStage(status="blocked", schedule_override=safe_schedule_override)
    execution = ExecutionStage(status="blocked", run_test=run_test_flag)
    logs = LogsStage(status="blocked")
    cleanup = CleanupStage(status="blocked")

    return {
        "_success": False,
        "profile": safe_profile,
        "build_id": safe_build_id,
        "dry_run": dry_run_flag,
        "plan_only": bool(plan_only),
        "behavior_verified": _behavior_verified_marker(
            dry_run=dry_run_flag, execution=execution, logs=logs
        ),
        "integration_name": integration_name,
        "target": target.model_dump() if target is not None else None,
        "component_summary": component_summary.model_dump(),
        "package": package.model_dump(),
        "deployment": deployment.model_dump(),
        "runtime_attachment": runtime_attachment.model_dump(),
        "schedule": schedule.model_dump(),
        "execution": execution.model_dump(),
        "logs": logs.model_dump(),
        "cleanup": cleanup.model_dump(),
        # Summary matches the established blocked-downstream failure convention (deploy-fail /
        # runtime-fail paths): execution/logs are NOT passed here. Passing them would add a
        # ``summary["test"]`` sub-summary, breaking the contract that ``test`` appears ONLY when a
        # run-test stage actually ran (enforced by test_run_test_false_* — #129 review r3). The
        # blocked execution/logs/cleanup stages are still fully present as TOP-LEVEL keys above,
        # which is the D2 deliverable (a caller branching on any stage key never breaks).
        "summary": _stage_summary(package, deployment, runtime_attachment, schedule),
        "warnings": [],
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


def _created_date_key(pkg: Any) -> Tuple[str, str]:
    """Total sort key for newest-first package selection; missing dates sort last.

    A tuple of ``(created_date, package_id)`` (each defaulting to ``""``) so that identical or
    missing ``created_date`` values resolve deterministically by ``package_id`` under a reverse
    sort — instead of leaving ``matches[0]`` at the mercy of the backend list order (#129 D6).
    """
    if isinstance(pkg, dict):
        return (pkg.get("created_date") or "", pkg.get("package_id") or "")
    return ("", "")


# Conservative conflict-signal tokens for a duplicate create/deploy (#129 D4). The router
# swallows ApiError into ``{"_success": False, "error": ...}`` with no stable conflict field, so
# detection stays local: an explicit ``409`` status or a duplicate-flavored error string.
_CONFLICT_TOKENS = ("409", "conflict", "already exists", "already deployed", "duplicate")


def _is_create_conflict_response(response: Any) -> bool:
    """True when a failed create/deploy response looks like a concurrent-create conflict (#129 D4).

    Conservative and side-effect-free: a non-dict is never a conflict; an explicit
    ``status_code`` of 409 (int OR the string ``"409"``) is; otherwise the lowercased
    ``error``/``message``/``exception_type`` text is scanned for a duplicate-flavored token. Used to
    trigger a single re-list recovery. Detection stays deliberately conservative (exact status match
    + a small token set) so a NON-conflict failure is never misread as a conflict and made to reuse
    the wrong resource — this repo's ``manage_deployment_action`` exposes no stable conflict field,
    so the error text is the real signal (#129 D4).
    """
    if not isinstance(response, dict):
        return False
    # Exact match on int 409 or its string form only — no loose numeric parsing (false-positive safe).
    if response.get("status_code") in (409, "409"):
        return True
    haystack = " ".join(
        str(response.get(field) or "") for field in ("error", "message", "exception_type")
    ).lower()
    return any(token in haystack for token in _CONFLICT_TOKENS)


def _find_or_create_package(
    boomi_client: Any,
    profile: Optional[str],
    *,
    component_id: str,
    package_version: str,
    component_type: str = "process",
) -> Tuple[Optional[PackageStage], Optional[OrchestrateDeployError]]:
    """Reuse an existing package for ``component_id`` + ``package_version`` or create one.

    ``component_type`` defaults to the historical process-only behavior; the
    M6.1 (#133) ASC deploy-both path passes ``"webservice"``. Returns
    ``(PackageStage, None)`` on success or ``(None, error)`` on failure.
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
                f"version {package_version}; reused the newest by (created_date, package_id)."
            )
    else:
        created = _call_deployment_action(
            boomi_client,
            profile,
            "create_package",
            {
                "component_id": component_id,
                "component_type": component_type,
                "package_version": package_version,
            },
        )
        if not created.get("_success"):
            # A concurrent call may have created this same package in the list-then-create window
            # (#129 D4). On a conflict-flavored failure, re-list once and reuse the winner rather
            # than surfacing a spurious create failure; any other failure stays a hard error.
            if _is_create_conflict_response(created):
                relisted = _call_deployment_action(
                    boomi_client, profile, "list_packages", {"component_id": component_id}
                )
                relist_matches = (
                    [
                        p
                        for p in (relisted.get("packages") or [])
                        if isinstance(p, dict) and p.get("package_version") == package_version
                    ]
                    if relisted.get("_success")
                    else []
                )
                if relist_matches:
                    relist_matches.sort(key=_created_date_key, reverse=True)
                    selected = relist_matches[0]
                    status = "reused"
                    warnings.append(
                        f"create_package for component {component_id} version {package_version} "
                        "conflicted with a concurrent create; re-listed and reused the existing "
                        "package."
                    )
                else:
                    return None, _error(
                        PACKAGE_CREATE_FAILED,
                        created.get("error") or "Failed to create package.",
                        field="build_id",
                        details={"component_id": component_id, "package_version": package_version},
                    )
            else:
                return None, _error(
                    PACKAGE_CREATE_FAILED,
                    created.get("error") or "Failed to create package.",
                    field="build_id",
                    details={"component_id": component_id, "package_version": package_version},
                )
        else:
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
            component_type=component_type,
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
            # A concurrent call may have deployed this package in the list-then-deploy window
            # (#129 D4). On a conflict-flavored failure, re-list once: reuse a single active
            # deployment, refuse an ambiguous multi-active state, else surface the create failure.
            if _is_create_conflict_response(created):
                relisted = _call_deployment_action(
                    boomi_client,
                    profile,
                    "list_deployments",
                    {"package_id": package_id, "environment_id": environment_id},
                )
                if not relisted.get("_success"):
                    return None, _error(
                        DEPLOY_CREATE_FAILED,
                        created.get("error") or "Failed to deploy package.",
                        field="environment_id",
                        details={"package_id": package_id, "environment_id": environment_id},
                    )
                relist_active = [
                    d for d in (relisted.get("deployments") or []) if _deployment_is_active(d)
                ]
                if len(relist_active) > 1:
                    return None, _error(
                        DEPLOY_AMBIGUOUS_EXISTING,
                        (
                            f"deploy conflicted and re-listing found {len(relist_active)} active "
                            f"deployments for package {package_id} in environment "
                            f"{environment_id}; refusing to redeploy."
                        ),
                        field="environment_id",
                        details={
                            "package_id": package_id,
                            "environment_id": environment_id,
                            "active_count": len(relist_active),
                        },
                    )
                if len(relist_active) == 1:
                    selected = relist_active[0]
                    status = "reused"
                    warnings.append(
                        f"deploy for package {package_id} in environment {environment_id} "
                        "conflicted with a concurrent deploy; re-listed and reused the existing "
                        "active deployment."
                    )
                else:
                    return None, _error(
                        DEPLOY_CREATE_FAILED,
                        created.get("error") or "Failed to deploy package.",
                        field="environment_id",
                        details={"package_id": package_id, "environment_id": environment_id},
                    )
            else:
                return None, _error(
                    DEPLOY_CREATE_FAILED,
                    created.get("error") or "Failed to deploy package.",
                    field="environment_id",
                    details={"package_id": package_id, "environment_id": environment_id},
                )
        else:
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


def _ensure_api_service_deployment(
    boomi_client: Any,
    profile: Optional[str],
    *,
    asc_component_id: str,
    environment_id: str,
    package_version: str,
) -> Tuple[Optional[PackageStage], Optional[DeploymentStage], Optional[OrchestrateDeployError]]:
    """Package + deploy the API Service Component to the environment (M6.1 #133).

    ASC deploy does NOT cascade from the route process (live-confirmed) — the
    ASC needs its own PackagedComponent (``component_type='webservice'``) and
    DeployedPackage in the SAME environment as every route process. Reuses the
    existing find-or-create package/deployment helpers so idempotent re-runs
    reuse rather than duplicate.
    """
    asc_package, package_error = _find_or_create_package(
        boomi_client,
        profile,
        component_id=asc_component_id,
        package_version=package_version,
        component_type="webservice",
    )
    if package_error is not None or asc_package is None:
        return None, None, package_error
    asc_deployment, deploy_error = _find_or_create_deployment(
        boomi_client,
        profile,
        package_id=asc_package.package_id,
        environment_id=environment_id,
    )
    if deploy_error is not None:
        return asc_package, None, deploy_error
    return asc_package, asc_deployment, None


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
    """Verify env/runtime, then ensure the required bindings exist (reuse-or-attach, idempotent).

    Returns ``(stage, None)`` on success or ``(failed_stage, error)`` on the first failing leg.
    The three legs:
      1. runtime<->environment via ``manage_runtimes_action`` (EnvironmentAtomAttachment).
      2. process<->environment via ``manage_deployment_action`` (ProcessEnvironmentAttachment).
      3. process<->runtime    via ``manage_deployment_action`` (ProcessAtomAttachment).
    On environment-enabled accounts Boomi rejects leg 3 (the direct process<->atom binding) because
    legs 1+2 already make the process runnable on the runtime via the environment; that rejection is
    recorded as a ``not_required`` leg and does not fail the stage (see
    deployment_utils.is_environment_account_signal).
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

    # 4. process<->runtime attachment (ProcessAtomAttachment). Environment-enabled accounts reject
    # this direct leg ("This account uses environments. Please use ComponentEnvironmentAttachment");
    # there legs 2+3 above (runtime<->env + process<->env) already make the process runnable on the
    # runtime via the environment, so the direct leg is NOT required — record it as ``not_required``
    # and continue. Any other list/attach failure remains fatal.
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
        if is_environment_account_signal(error.message):
            process_runtime_id = None
            process_runtime_status = "not_required"
        else:
            return _failed_stage(), error

    # Only legs actually attempted (reused/attached) drive the stage's reused/changed flags; a
    # ``not_required`` leg (e.g. the direct process<->atom binding skipped on an environment-enabled
    # account) must not skew them or make a no-op re-run look not-reused.
    attempted_statuses = [
        s for s in (runtime_env_status, process_env_status, process_runtime_status)
        if s in ("reused", "attached")
    ]
    changed = any(s == "attached" for s in attempted_statuses)
    reused = bool(attempted_statuses) and all(s == "reused" for s in attempted_statuses)
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

    # No poll result means the wait branch never ran — execute_process_action returned an early
    # failure. Distinguish the canonical "request accepted but no request_id came back" sentinel
    # (TEST_REQUEST_ID_MISSING — Boomi gave us no handle to track the run) from any other
    # pre-request execute failure such as invalid dynamic/process properties (a ValueError from
    # the property builders) or an API/setup error, which are general execution failures.
    if poll is None:
        message = exec_result.get("error") or "Execution produced no terminal result."
        if not request_id and "no request_id" in message.lower():
            stage = ExecutionStage(status="failed", error=message, **base_kwargs)
            return stage, _error(TEST_REQUEST_ID_MISSING, message, field="run_test")
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


def _content_download_failed(result: Dict[str, Any]) -> bool:
    """True when the monitor created the URL but the content download/extract failed.

    ``handle_execution_logs``/``handle_execution_artifacts`` return ``_success=True`` even when
    ``_download_and_extract_zip`` fails — it merges ``{"_downloaded": False, "error": ...}`` onto
    the already-True result. A *pointer-only* result (content fetch skipped: no ``creds`` or
    ``fetch_content=False``) has no ``_downloaded`` key and is NOT a failure. So the reliable
    failure signal is an explicit ``_downloaded is False``.
    """
    return result.get("_downloaded") is False


def _promote_logs_unavailable_error(
    stage: LogsStage, execution_id: Optional[str]
) -> OrchestrateDeployError:
    """Promote an unavailable/absent log fetch to a ``TEST_LOGS_UNAVAILABLE`` failure (issue #81).

    Mutates ``stage`` so the failed logs stage carries the structured code/next-step, and returns
    the contract error that flips ``_success`` to False. Only called when ``require_test_logs=True``
    and a test ran but its ProcessLog retrieval was absent or unavailable.
    """
    stage.error_code = TEST_LOGS_UNAVAILABLE
    stage.failed_stage = "logs"
    stage.next_step = (
        "The deployment and test execution succeeded, but log retrieval was required "
        "(require_test_logs=true) and unavailable. Re-fetch via "
        "monitor_platform(action='execution_logs', execution_id=...), or re-run with "
        "require_test_logs=false to treat logs as diagnostic-only."
    )
    return _error(
        TEST_LOGS_UNAVAILABLE,
        "Test execution succeeded but required log retrieval (require_test_logs=true) was unavailable.",
        field="require_test_logs",
        details={"execution_id": execution_id, "logs_status": "unavailable"},
    )


def _logs_stage_from_results(
    log_result: Optional[Dict[str, Any]],
    artifact_result: Optional[Dict[str, Any]],
    *,
    execution_id: Optional[str],
    log_level: str,
    fetch_logs: bool,
    fetch_artifacts: bool,
    require_test_logs: bool = False,
    execution_succeeded: bool = True,
) -> Tuple[LogsStage, Optional[OrchestrateDeployError]]:
    """Normalize log + artifact monitor results into a ``LogsStage``.

    A failed/absent log fetch — including a created URL whose content download/extract failed
    (``_success=True`` but ``_downloaded=False``) — is *diagnostic only* (``status="unavailable"``,
    error surfaced, ``download_url`` preserved for manual retry): it never turns a successful test
    execution into a failed orchestration UNLESS ``require_test_logs=True`` (issue #81), which
    promotes it to a ``TEST_LOGS_UNAVAILABLE`` failure. ``test_fetch_logs=False`` with
    ``require_test_logs=True`` is treated as absent logs and likewise fails. The artifact leg is
    independent and never produces ``TEST_LOGS_UNAVAILABLE``. ``execution_succeeded`` controls the
    diagnostic wording: on a FAILED execution (ERROR/ABORTED, where logs are still fetched) the
    "the test execution succeeded" phrasing would be contradictory, so neutral wording is used
    (#81 review). Returns ``(stage, log_error)`` where ``log_error`` is non-None only for the
    require-test-logs promotion.
    """
    stage = LogsStage(
        status="not_required",
        execution_id=execution_id,
        log_level=log_level if fetch_logs else None,
    )
    warnings: List[str] = []
    log_error: Optional[OrchestrateDeployError] = None

    if fetch_logs:
        if (
            log_result is not None
            and log_result.get("_success")
            and not _content_download_failed(log_result)
        ):
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
                stage.downloaded = (
                    bool(log_result.get("_downloaded")) if "_downloaded" in log_result else None
                )
                stage.error = log_result.get("error") or "Log retrieval failed."
            else:
                stage.error = "Log retrieval failed."
            # Structured diagnostic metadata (issue #65) — does NOT flip orchestration success.
            stage.error_code = LOG_RETRIEVAL_FAILED
            stage.failed_stage = "logs"
            if execution_succeeded:
                stage.next_step = (
                    "The test execution itself succeeded; only log retrieval was unavailable. "
                    "Re-fetch via monitor_platform(action='execution_logs', execution_id=...)."
                )
                warnings.append("Test execution succeeded but log retrieval was unavailable.")
            else:
                # The execution itself failed (#81 review): never claim it succeeded here.
                stage.next_step = (
                    "Log retrieval was unavailable. Re-fetch via "
                    "monitor_platform(action='execution_logs', execution_id=...)."
                )
                warnings.append("Log retrieval was unavailable.")
            # Issue #81: when logs are required, promote this diagnostic to a hard failure.
            if require_test_logs:
                log_error = _promote_logs_unavailable_error(stage, execution_id)
    elif require_test_logs and execution_id:
        # ``test_fetch_logs=False`` but logs are required (issue #81): an absent log fetch after a
        # successful execution is itself the failure — mark the stage unavailable and fail.
        stage.status = "unavailable"
        stage.error = (
            "Log retrieval was required (require_test_logs=true) but test_fetch_logs=false; "
            "no logs were fetched."
        )
        log_error = _promote_logs_unavailable_error(stage, execution_id)

    if fetch_artifacts:
        if (
            artifact_result is not None
            and artifact_result.get("_success")
            and not _content_download_failed(artifact_result)
        ):
            stage.artifact_status = "retrieved"
            stage.artifact_status_code = artifact_result.get("status_code")
            stage.artifact_message = artifact_result.get("message")
            stage.artifact_download_url = artifact_result.get("download_url")
        else:
            stage.artifact_status = "unavailable"
            if artifact_result is not None:
                stage.artifact_status_code = artifact_result.get("status_code")
                stage.artifact_message = artifact_result.get("message")
                stage.artifact_download_url = artifact_result.get("download_url")
                stage.artifact_error = (
                    artifact_result.get("error") or "Artifact retrieval failed."
                )
            else:
                stage.artifact_error = "Artifact retrieval failed."
            # Structured diagnostic metadata (issue #65) — does NOT flip orchestration success.
            stage.artifact_error_code = ARTIFACT_RETRIEVAL_FAILED
            stage.artifact_failed_stage = "logs"
            if execution_succeeded:
                stage.artifact_next_step = (
                    "The test execution itself succeeded; only artifact retrieval was unavailable. "
                    "Re-fetch via monitor_platform(action='execution_artifacts', execution_id=...)."
                )
            else:
                # The execution itself failed (#81 review): never claim it succeeded here.
                stage.artifact_next_step = (
                    "Artifact retrieval was unavailable. Re-fetch via "
                    "monitor_platform(action='execution_artifacts', execution_id=...)."
                )

    stage.warnings = warnings
    return stage, log_error


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
    ``blocked``. Returns ``(execution_stage, logs_stage, error)`` where ``error`` is non-None for a
    *failed test execution* or, when ``require_test_logs=True`` (issue #81), an absent/unavailable
    log fetch after a SUCCESSFUL execution. Execution failures take precedence: an ERROR/ABORTED
    test still surfaces ``TEST_EXECUTION_FAILED``, never masked by a ``TEST_LOGS_UNAVAILABLE``.
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
        if execution_error is not None:
            # The execution itself failed — logs are blocked and the execution error stands.
            logs_stage = LogsStage(status="blocked", execution_id=None)
            return execution_stage, logs_stage, execution_error
        # A terminal SUCCESS that returned no execution_id: no ProcessLog can ever be fetched, so
        # with require_test_logs=true this is an absent-logs failure (#81 review). Otherwise it
        # stays the existing diagnostic-only not_required.
        logs_stage = LogsStage(status="not_required", execution_id=None)
        if request.require_test_logs:
            # No execution id exists, so the generic "re-fetch by execution_id" remediation is
            # impossible — use no-execution-id-specific metadata instead (#81 review).
            no_id_next_step = (
                "The test execution reached a terminal status but returned no execution_id, so "
                "its ProcessLog can never be retrieved. Re-run the test to obtain a run with a "
                "retrievable execution_id, or re-run with require_test_logs=false to treat log "
                "retrieval as diagnostic-only (prior stages are reused)."
            )
            logs_stage.status = "unavailable"
            logs_stage.error = (
                "Test execution completed but returned no execution_id, so no logs could be "
                "fetched and log retrieval was required (require_test_logs=true)."
            )
            logs_stage.error_code = TEST_LOGS_UNAVAILABLE
            logs_stage.failed_stage = "logs"
            logs_stage.next_step = no_id_next_step
            log_error = _error(
                TEST_LOGS_UNAVAILABLE,
                "Test execution completed but returned no execution_id; required log retrieval "
                "is impossible.",
                field="require_test_logs",
                details={"execution_id": None, "logs_status": "unavailable"},
            )
            return execution_stage, logs_stage, log_error
        return execution_stage, logs_stage, None

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

    # Execution-failure precedence: only enforce required-logs when the execution SUCCEEDED. On a
    # failed execution (ERROR/ABORTED) log retrieval stays diagnostic — the top-level failure is
    # TEST_EXECUTION_FAILED, and promoting the logs stage would contradictorily annotate it as
    # TEST_LOGS_UNAVAILABLE with a "test execution succeeded" hint (#81 review).
    require_logs_effective = request.require_test_logs and execution_error is None
    logs_stage, log_error = _logs_stage_from_results(
        log_result,
        artifact_result,
        execution_id=execution_id,
        log_level=request.test_log_level,
        fetch_logs=fetch_logs,
        fetch_artifacts=fetch_artifacts,
        require_test_logs=require_logs_effective,
        execution_succeeded=execution_error is None,
    )
    return execution_stage, logs_stage, execution_error if execution_error is not None else log_error


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
    listener_verify: Optional[ListenerVerifyStage] = None,
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
        # ``resource_reuse``/``resource_changes`` let an agent see, per stage, whether this run
        # reused an existing resource or created/changed one — the signal a safe retry needs to
        # avoid duplicating packages/deployments/attachments/schedules (issue #65).
        "resource_reuse": {
            "package": package.status == "reused",
            "deployment": deployment.status == "reused",
            "runtime_attachment": runtime_attachment.reused,
            "schedule": schedule.reused,
        },
        "resource_changes": {
            "package": package.status == "created",
            "deployment": deployment.status == "deployed",
            "runtime_attachment": runtime_attachment.changed,
            "schedule": schedule.changed,
        },
        "stage_statuses": {
            "package": package.status,
            "deployment": deployment.status,
            "runtime_attachment": runtime_attachment.status,
            "schedule": schedule.status,
        },
        "stage_warnings": {
            "package": list(package.warnings),
            "deployment": list(deployment.warnings),
            "runtime_attachment": list(runtime_attachment.warnings),
            "schedule": list(schedule.warnings),
        },
    }
    if listener_verify is not None:
        summary["stage_statuses"]["listener_verify"] = listener_verify.status
        summary["listener"] = {
            "api_type": listener_verify.api_type,
            "auth": listener_verify.auth,
            "endpoint_path": listener_verify.endpoint_path,
            "endpoint_url": listener_verify.endpoint_url,
            "http_method": listener_verify.http_method,
            "probe_status_code": listener_verify.probe_status_code,
            "served_object_name_casing": listener_verify.served_object_name_casing,
            "deployment_active": listener_verify.deployment_active,
            "collision_count": listener_verify.collision_count,
            "execution_record_found": listener_verify.execution_record_found,
            "readback_baseline_available": listener_verify.readback_baseline_available,
            "execution_id": listener_verify.execution_id,
            "execution_status": listener_verify.execution_status,
        }
    if execution is not None and logs is not None:
        summary["stage_statuses"]["execution"] = execution.status
        summary["stage_statuses"]["logs"] = logs.status
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
            "log_error_code": logs.error_code,
            "log_next_step": logs.next_step,
            "artifact_error_code": logs.artifact_error_code,
            "artifact_next_step": logs.artifact_next_step,
        }
    return summary


def _behavior_verified_marker(
    *,
    dry_run: bool,
    execution: ExecutionStage,
    logs: LogsStage,
    listener_verify: Optional[ListenerVerifyStage] = None,
) -> Dict[str, Any]:
    """Top-level marker making the "deploy-clean is not behavioral verification" gap explicit (#81).

    ``verified`` is True ONLY when the behavioral check for the build actually
    ran clean: a test execution reaching COMPLETE with logs retrieved — or, for
    a LISTENER build (M6 #12, no Test mode), a completed ``listener_verify``
    whose live probe triggered a NEW execution that finished COMPLETE. Every
    other path (dry-run, run_test=false, a prior-stage block,
    COMPLETE_WARN/ERROR/timeout, completed-but-logs-unavailable, or a listener
    probe whose execution errored) is ``verified=False`` with a ``reason`` it
    can branch on. The marker is purely additive: it changes no existing stage
    field and never flips ``_success``.
    """
    logs_status = logs.status
    if dry_run:
        return {"verified": False, "reason": "dry_run", "logs_status": logs_status}
    # M6 (#12): a listener build's behavioral check IS the listener_verify
    # stage (probe + new-execution readback) — the Test-mode stages are
    # not_required by design, so they must not decide the marker.
    if listener_verify is not None and listener_verify.status not in ("not_required",):
        if listener_verify.status == "completed":
            if (listener_verify.execution_status or "").upper() == "COMPLETE":
                # Degraded readback (pre-probe baseline query failed) cannot
                # prove the COMPLETE record was the probe's own execution —
                # never report verified on weaker evidence (Codex review).
                if listener_verify.readback_baseline_available is not True:
                    return {
                        "verified": False,
                        "reason": "listener_readback_degraded",
                        "logs_status": logs_status,
                    }
                return {
                    "verified": True,
                    "reason": "listener_probe_verified",
                    "logs_status": logs_status,
                }
            return {
                "verified": False,
                "reason": "listener_execution_not_complete",
                "logs_status": logs_status,
            }
        if listener_verify.status == "blocked":
            return {
                "verified": False,
                "reason": "listener_verify_blocked",
                "logs_status": logs_status,
            }
        return {
            "verified": False,
            "reason": "listener_verify_failed",
            "logs_status": logs_status,
        }
    if execution.status == "skipped":
        return {"verified": False, "reason": "test_not_run", "logs_status": logs_status}
    if execution.status == "not_required":
        # Defensive fallback (a not_required test stage without a listener
        # stage in the response) — there is no behavioral check to report.
        return {"verified": False, "reason": "test_not_supported", "logs_status": logs_status}
    if execution.status == "blocked":
        return {"verified": False, "reason": "test_blocked", "logs_status": logs_status}
    if execution.status == "completed":
        if logs_status == "retrieved":
            return {
                "verified": True,
                "reason": "test_ran_logs_retrieved",
                "logs_status": logs_status,
            }
        return {"verified": False, "reason": "logs_unavailable", "logs_status": logs_status}
    # A test ran but did not cleanly COMPLETE, so it is never behaviorally verified. Split the
    # reason so a caller can distinguish warn vs hard-fail vs timeout without re-parsing
    # execution.status/poll_status (#129 D7).
    if execution.status == "warning":
        return {
            "verified": False,
            "reason": "test_completed_with_warnings",
            "logs_status": logs_status,
        }
    if execution.status == "timeout" or execution.poll_status == "TIMEOUT":
        return {"verified": False, "reason": "test_timeout", "logs_status": logs_status}
    return {"verified": False, "reason": "test_failed", "logs_status": logs_status}


# ---------------------------------------------------------------------------
# Failure hardening + cleanup planning (issue #65)
# ---------------------------------------------------------------------------
# Map each structured error code to the orchestration stage it belongs to, so a failed
# response can name the stage that failed without each call site repeating the mapping.
_ERROR_CODE_STAGES: Dict[str, str] = {
    BOOMI_CLIENT_REQUIRED: "package",
    PACKAGE_LIST_FAILED: "package",
    PACKAGE_CREATE_FAILED: "package",
    PACKAGE_ID_MISSING: "package",
    DEPLOY_LIST_FAILED: "deployment",
    DEPLOY_AMBIGUOUS_EXISTING: "deployment",
    DEPLOY_CREATE_FAILED: "deployment",
    DEPLOY_ID_MISSING: "deployment",
    ENVIRONMENT_VERIFY_FAILED: "runtime_attachment",
    RUNTIME_VERIFY_FAILED: "runtime_attachment",
    RUNTIME_ENV_ATTACHMENT_LIST_FAILED: "runtime_attachment",
    RUNTIME_ENV_ATTACHMENT_CREATE_FAILED: "runtime_attachment",
    RUNTIME_ENV_ATTACHMENT_ID_MISSING: "runtime_attachment",
    PROCESS_ENV_ATTACHMENT_LIST_FAILED: "runtime_attachment",
    PROCESS_ENV_ATTACHMENT_CREATE_FAILED: "runtime_attachment",
    PROCESS_ENV_ATTACHMENT_ID_MISSING: "runtime_attachment",
    PROCESS_RUNTIME_ATTACHMENT_LIST_FAILED: "runtime_attachment",
    PROCESS_RUNTIME_ATTACHMENT_CREATE_FAILED: "runtime_attachment",
    PROCESS_RUNTIME_ATTACHMENT_ID_MISSING: "runtime_attachment",
    SCHEDULE_OVERRIDE_INVALID: "schedule",
    SCHEDULE_UPDATE_FAILED: "schedule",
    SCHEDULE_DELETE_FAILED: "schedule",
    SCHEDULE_ENABLE_FAILED: "schedule",
    SCHEDULE_DISABLE_FAILED: "schedule",
    SCHEDULE_ID_MISSING: "schedule",
    LISTENER_SERVER_INFO_FAILED: "listener_verify",
    LISTENER_APITYPE_UNSUPPORTED: "listener_verify",
    LISTENER_DEPLOYMENT_INACTIVE: "listener_verify",
    LISTENER_PATH_COLLISION: "listener_verify",
    LISTENER_PROBE_FAILED: "listener_verify",
    LISTENER_EXECUTION_RECORD_MISSING: "listener_verify",
    LISTENER_ASC_REQUIRED: "listener_verify",
    LISTENER_ASC_UNSUPPORTED_FOR_APITYPE: "listener_verify",
    LISTENER_ASC_DEPLOYMENT_INACTIVE: "listener_verify",
    LISTENER_ROUTE_PROCESS_DEPLOYMENT_INACTIVE: "listener_verify",
    LISTENER_ASC_ROUTE_INVALID: "listener_verify",
    LISTENER_ASC_COLLISION: "listener_verify",
    TEST_EXECUTION_FAILED: "execution",
    TEST_EXECUTION_TIMEOUT: "execution",
    TEST_REQUEST_ID_MISSING: "execution",
    LOG_RETRIEVAL_FAILED: "logs",
    ARTIFACT_RETRIEVAL_FAILED: "logs",
    TEST_LOGS_UNAVAILABLE: "logs",
}

# Stage execution order — used to derive the "prior stages" a retry can rely on.
# listener_verify (M6 #12) runs after schedule and before the test execution:
# a listener build has no Test mode, so the probe IS its behavioral test.
_STAGE_ORDER = [
    "package",
    "deployment",
    "runtime_attachment",
    "schedule",
    "listener_verify",
    "execution",
    "logs",
]


def _failed_stage_for_error_code(code: Optional[str]) -> str:
    """Resolve the orchestration stage name for a structured error code (``"unknown"`` fallback)."""
    return _ERROR_CODE_STAGES.get(code or "", "unknown")


def _prior_stage_summary(
    failed_stage: str,
    package: Optional[PackageStage],
    deployment: Optional[DeploymentStage],
    runtime_attachment: Optional[RuntimeAttachmentStage],
    schedule: Optional[ScheduleStage],
    *,
    execution: Optional[ExecutionStage] = None,
    listener_verify: Optional[ListenerVerifyStage] = None,
) -> Dict[str, Any]:
    """Compact status + ids of the stages that ran BEFORE ``failed_stage``.

    Lets a calling agent see exactly what already succeeded (and which resource ids now exist) so
    a retry resumes safely from the failed stage instead of re-creating prior resources.
    """
    stage_objs: Dict[str, Any] = {
        "package": package,
        "deployment": deployment,
        "runtime_attachment": runtime_attachment,
        "schedule": schedule,
        "listener_verify": listener_verify,
        "execution": execution,
    }
    try:
        cutoff = _STAGE_ORDER.index(failed_stage)
    except ValueError:
        cutoff = len(_STAGE_ORDER)

    summary: Dict[str, Any] = {}
    for name in _STAGE_ORDER[:cutoff]:
        stage = stage_objs.get(name)
        if stage is None:
            continue
        entry: Dict[str, Any] = {"status": stage.status}
        if name == "package":
            entry["package_id"] = stage.package_id
            entry["package_version"] = stage.package_version
        elif name == "deployment":
            entry["deployment_id"] = stage.deployment_id
            entry["environment_id"] = stage.environment_id
        elif name == "runtime_attachment":
            entry["runtime_env_attachment_id"] = stage.runtime_env_attachment_id
            entry["process_env_attachment_id"] = stage.process_env_attachment_id
            entry["process_runtime_attachment_id"] = stage.process_runtime_attachment_id
        elif name == "schedule":
            entry["schedule_id"] = stage.schedule_id
        elif name == "listener_verify":
            entry["endpoint_path"] = stage.endpoint_path
            entry["probe_status_code"] = stage.probe_status_code
        elif name == "execution":
            entry["execution_id"] = stage.execution_id
        summary[name] = entry
    return summary


def _next_step_for_failure(error: OrchestrateDeployError, failed_stage: str) -> str:
    """One actionable next step for a failed stage, specialized by error code where useful."""
    code = error.code
    if failed_stage == "package":
        if code == BOOMI_CLIENT_REQUIRED:
            return (
                "A non-dry-run deploy needs an authenticated Boomi client; provide credentials "
                "and re-run orchestrate_deploy."
            )
        return (
            "Verify the resolved process component exists and is packageable, then re-run "
            "orchestrate_deploy — the package version defaults to the build_id so a retry reuses "
            "the same package instead of creating a duplicate."
        )
    if failed_stage == "deployment":
        if code == DEPLOY_AMBIGUOUS_EXISTING:
            return (
                "Multiple active deployments already exist for this package/environment; undeploy "
                "the stale one (manage_deployment undeploy) or pass an explicit package_version, "
                "then re-run."
            )
        return (
            "Verify the environment_id and that the package can deploy there, then re-run; the "
            "package created by this run is reused on retry (no duplicate package)."
        )
    if failed_stage == "runtime_attachment":
        return (
            "Verify the environment and runtime exist and the account may attach them, then re-run "
            "orchestrate_deploy — already-attached legs are reused, so only the missing binding is "
            "created."
        )
    if failed_stage == "schedule":
        return (
            "Fix the schedule_override (or schedule permissions) and re-run orchestrate_deploy; "
            "package, deploy, and runtime bindings already succeeded and will be reused."
        )
    if failed_stage == "listener_verify":
        if code == LISTENER_ASC_REQUIRED:
            return (
                "The runtime's Shared Web Server apiType is 'advanced', which does not serve "
                "bare /ws/simple WSS routes (the deploy succeeds but every route 404s). "
                "Rebuild with the listener archetype's asc_wrapper.enabled=true (or author a "
                "webservice component routing to the listener process), or switch the "
                "runtime's apiType to basic/intermediate, then re-run."
            )
        if code == LISTENER_ASC_UNSUPPORTED_FOR_APITYPE:
            return (
                "The build publishes through an API Service Component, but ASC /ws/rest "
                "routes are served only on apiType='advanced'. Switch the runtime's Shared "
                "Web Server apiType to advanced, or rebuild without the asc_wrapper (bare "
                "WSS serves basic/intermediate), then re-run."
            )
        if code in (LISTENER_ASC_DEPLOYMENT_INACTIVE, LISTENER_ROUTE_PROCESS_DEPLOYMENT_INACTIVE):
            return (
                "ASC deploy does not cascade: the API Service Component AND every route "
                "process must each be packaged and deployed (active) to the same "
                "environment. Deploy the missing component (manage_deployment) and re-run "
                "orchestrate_deploy — existing packages/deployments are reused."
            )
        if code == LISTENER_ASC_COLLISION:
            return (
                "Another deployed API Service Component collides with this one. The "
                "platform binds ONE deployed webservice component per BASE urlPath — "
                "the first-deployed serves and a later same-base ASC is shadowed in "
                "its entirety, even for routes with unique paths (undeploying the "
                "winner does not activate the loser). Give this ASC a distinct "
                "base_url_path (asc_wrapper.base_url_path) or undeploy the colliding "
                "ASC, then re-run."
            )
        if code == LISTENER_APITYPE_UNSUPPORTED:
            return (
                "The runtime's Shared Web Server apiType does not serve this listener "
                "pattern. Bare /ws/simple WSS routes need basic/intermediate; /ws/rest "
                "API Service routes need advanced (asc_wrapper.enabled=true). Align the "
                "runtime apiType with the build's publish mode, then re-run."
            )
        if code == LISTENER_PATH_COLLISION:
            return (
                "Another deployed process in this environment serves the same WSS endpoint path "
                "(operationType+objectName) — Boomi routes duplicate paths unpredictably. Give "
                "this listener a unique objectName (rebuild) or undeploy the colliding process, "
                "then re-run."
            )
        if code == LISTENER_PROBE_FAILED:
            return (
                "The deploy succeeded but the live endpoint probe did not return 2xx. 401 with a "
                "known-good token means no route is registered (or credentials are wrong); 404 "
                "after authentication means the path is wrong. Check the runtime's Shared Web "
                "Server auth/token (Basic tokens are UI-provisioned once, then readable via "
                "get_server_info) and re-run; prior stages are reused."
            )
        return (
            "The deploy succeeded but listener verification failed; inspect the listener_verify "
            "stage (api_type/auth/endpoint/probe/execution readback), fix the cause, and re-run "
            "orchestrate_deploy — prior stages are reused."
        )
    if failed_stage == "execution":
        if code == TEST_EXECUTION_TIMEOUT:
            return (
                "The deployment succeeded but the test run did not finish within "
                "test_timeout_seconds. Increase test_timeout_seconds or inspect the run via "
                "monitor_platform, then re-run with run_test=true (prior stages are reused)."
            )
        return (
            "The deployment succeeded but the test execution failed; inspect summary.test/logs, "
            "fix the process, then re-run with run_test=true (prior stages are reused)."
        )
    if failed_stage == "logs":
        # Only reachable when require_test_logs=true promoted a log fetch failure (issue #81); the
        # default diagnostic-only LOG_RETRIEVAL_FAILED never sets a top-level failed_stage.
        if (error.details or {}).get("execution_id") is None:
            # No execution id ever resolved — a re-fetch by execution_id is impossible (#81 review).
            return (
                "The test execution reached a terminal status but returned no execution_id, so its "
                "ProcessLog can never be retrieved. Re-run the test to obtain a run with a "
                "retrievable execution_id, or re-run with require_test_logs=false to treat log "
                "retrieval as diagnostic-only (prior stages are reused)."
            )
        return (
            "The deployment and test execution succeeded, but log retrieval was required "
            "(require_test_logs=true) and unavailable. Re-fetch the logs via "
            "monitor_platform(action='execution_logs', execution_id=...), or re-run with "
            "require_test_logs=false to treat log retrieval as diagnostic-only (prior stages are "
            "reused)."
        )
    return "Inspect the errors array and re-run orchestrate_deploy after addressing the failure."


def _cleanup_operations_for_failure(
    package: Optional[PackageStage],
    deployment: Optional[DeploymentStage],
    runtime_attachment: Optional[RuntimeAttachmentStage],
    *,
    environment_id: Optional[str],
    listener_verify: Optional[ListenerVerifyStage] = None,
) -> Tuple[List[CleanupOperation], List[str]]:
    """Destructive operations (reverse creation order) undoing what THIS attempt created, plus
    manual-intervention warnings for mutations that cannot be expressed as an executable op.

    Only resources this orchestration attempt actually CREATED are listed — reused/not-required
    resources are omitted (a retry reuses them idempotently, so undoing them would break an
    already-correct prior state). The schedule is deliberately EXCLUDED: a process schedule is
    modified in place (``update``), not created; this run captures no prior cron/status to restore;
    and a retry re-applies ``schedule_override`` idempotently — so destructively deleting it would
    risk wiping a pre-existing schedule rather than undoing this attempt. The result *names* exactly
    what a caller would undeploy / delete / detach; it performs no mutation by itself.

    An attachment leg that was ``attached`` (mutated the account) but returned no id (#129 D5)
    cannot be undone by a detach-by-id op — a ``resource_id=None`` op is rejected by the detach
    handler — so instead of emitting a bogus op, its manual-cleanup need is surfaced as a warning.

    M6.1 (#133, Codex review r1): in ASC publish mode the listener stage
    packages/deploys the API Service Component AFTER the process resources, so
    a fresh ASC deployment/package recorded on ``listener_verify`` is undone
    FIRST — a leftover active webservice deployment would base-shadow the next
    attempt's ASC (LISTENER_ASC_COLLISION on retry).
    """
    ops: List[CleanupOperation] = []
    manual_warnings: List[str] = []

    # 0. ASC deployment + package (created LAST, by the listener stage).
    if listener_verify is not None:
        if (
            listener_verify.api_service_deployment_status == "deployed"
            and listener_verify.api_service_deployment_id
        ):
            ops.append(CleanupOperation(
                tool="manage_deployment",
                action="undeploy",
                resource_type="deployment",
                resource_id=listener_verify.api_service_deployment_id,
                config={
                    "deployment_id": listener_verify.api_service_deployment_id,
                    "package_id": listener_verify.api_service_package_id,
                    "environment_id": environment_id,
                },
                reason=(
                    "This run deployed the API Service Component package; undeploy it to undo "
                    "the new ASC deployment (a leftover active ASC base-shadows a retry)."
                ),
            ))
        if (
            listener_verify.api_service_package_status == "created"
            and listener_verify.api_service_package_id
        ):
            ops.append(CleanupOperation(
                tool="manage_deployment",
                action="delete_package",
                resource_type="package",
                resource_id=listener_verify.api_service_package_id,
                config={"package_id": listener_verify.api_service_package_id},
                reason=(
                    "This run created the API Service Component package; delete it to undo "
                    "the new package."
                ),
            ))

    # 1. process<->runtime attachment.
    if (
        runtime_attachment is not None
        and runtime_attachment.process_runtime_attachment_status == "attached"
    ):
        if runtime_attachment.process_runtime_attachment_id:
            ops.append(CleanupOperation(
                tool="manage_deployment",
                action="detach_process_atom",
                resource_type="process_runtime_attachment",
                resource_id=runtime_attachment.process_runtime_attachment_id,
                config={"resource_id": runtime_attachment.process_runtime_attachment_id},
                reason=(
                    "This run attached the process to the runtime; detach it to undo the new "
                    "process<->runtime binding."
                ),
            ))
        else:
            manual_warnings.append(
                "This run attached the process to the runtime but the create returned no "
                "attachment id, so it cannot be auto-detached. Re-list ProcessAtomAttachment for "
                "this process/runtime and detach it manually."
            )

    # 2. process<->environment attachment.
    if (
        runtime_attachment is not None
        and runtime_attachment.process_env_attachment_status == "attached"
    ):
        if runtime_attachment.process_env_attachment_id:
            ops.append(CleanupOperation(
                tool="manage_deployment",
                action="detach_process_environment",
                resource_type="process_env_attachment",
                resource_id=runtime_attachment.process_env_attachment_id,
                config={"resource_id": runtime_attachment.process_env_attachment_id},
                reason=(
                    "This run attached the process to the environment; detach it to undo the new "
                    "process<->environment binding."
                ),
            ))
        else:
            manual_warnings.append(
                "This run attached the process to the environment but the create returned no "
                "attachment id, so it cannot be auto-detached. Re-list "
                "ProcessEnvironmentAttachment for this process/environment and detach it manually."
            )

    # 3. runtime<->environment attachment. Detach by the attachment id ONLY (the direct path):
    #    manage_runtimes_action('detach') treats resource_id as a *runtime* id whenever
    #    environment_id is also present, so passing environment_id here would make it look up an
    #    atom by the attachment id and fail to detach.
    if (
        runtime_attachment is not None
        and runtime_attachment.runtime_env_attachment_status == "attached"
    ):
        if runtime_attachment.runtime_env_attachment_id:
            ops.append(CleanupOperation(
                tool="manage_runtimes",
                action="detach",
                resource_type="runtime_env_attachment",
                resource_id=runtime_attachment.runtime_env_attachment_id,
                config={"resource_id": runtime_attachment.runtime_env_attachment_id},
                reason=(
                    "This run attached the runtime to the environment; detach it to undo the new "
                    "runtime<->environment binding."
                ),
            ))
        else:
            manual_warnings.append(
                "This run attached the runtime to the environment but the create returned no "
                "attachment id, so it cannot be auto-detached. Re-list EnvironmentAtomAttachment "
                "for this runtime/environment and detach it manually."
            )

    # 4. deployment — undeploy the package this run deployed.
    if deployment is not None and deployment.status == "deployed":
        ops.append(CleanupOperation(
            tool="manage_deployment",
            action="undeploy",
            resource_type="deployment",
            resource_id=deployment.deployment_id,
            config={
                "deployment_id": deployment.deployment_id,
                "package_id": deployment.package_id,
                "environment_id": deployment.environment_id or environment_id,
            },
            reason="This run deployed the package; undeploy it to undo the new deployment.",
        ))

    # 5. package — delete the package this run created.
    if package is not None and package.status == "created":
        ops.append(CleanupOperation(
            tool="manage_deployment",
            action="delete_package",
            resource_type="package",
            resource_id=package.package_id,
            config={"package_id": package.package_id},
            reason="This run created the package; delete it to undo the new package.",
        ))

    return ops, manual_warnings


# Maps a cleanup operation's ``tool`` to the sibling-router wrapper that performs it.
_CLEANUP_TOOL_DISPATCH = {
    "manage_deployment": _call_deployment_action,
    "manage_runtimes": _call_runtime_action,
    "manage_schedules": _call_schedule_action,
}


def _cleanup_stage_for_failure(
    package: Optional[PackageStage],
    deployment: Optional[DeploymentStage],
    runtime_attachment: Optional[RuntimeAttachmentStage],
    *,
    environment_id: Optional[str],
    cleanup_on_failure: bool,
    boomi_client: Any = None,
    profile: Optional[str] = None,
    listener_verify: Optional[ListenerVerifyStage] = None,
) -> CleanupStage:
    """Plan (default) or, on explicit opt-in, execute cleanup of resources THIS attempt created.

    Defaults to a dry-run plan (``status="planned"``, ``dry_run=True``, ``mutation_allowed=False``)
    that names each destructive operation without calling anything. When nothing this attempt
    created needs undoing, ``status="not_required"``. Destructive cleanup runs only when the caller
    passes ``cleanup_on_failure=True`` and a client is available; each operation's result is
    recorded and a failed operation is surfaced (``CLEANUP_OPERATION_FAILED``) without raising.
    """
    operations, manual_warnings = _cleanup_operations_for_failure(
        package, deployment, runtime_attachment, environment_id=environment_id,
        listener_verify=listener_verify,
    )
    if not operations:
        # No executable ops. If a mutation could not be expressed as one (attached-without-id,
        # #129 D5), surface it as a ``warning`` with manual guidance rather than the misleading
        # ``not_required`` — the account WAS mutated and needs manual cleanup.
        if manual_warnings:
            return CleanupStage(
                status="warning",
                dry_run=True,
                mutation_allowed=bool(cleanup_on_failure),
                warnings=manual_warnings,
                next_step=(
                    "This attempt created attachment(s) that returned no id and cannot be "
                    "auto-detached; re-list the named attachment(s) and detach them manually "
                    "before retrying."
                ),
            )
        return CleanupStage(
            status="not_required",
            dry_run=True,
            mutation_allowed=bool(cleanup_on_failure),
            next_step=(
                "No resources created by this attempt require cleanup; a retry will reuse any "
                "existing resources."
            ),
        )

    if not cleanup_on_failure or boomi_client is None:
        return CleanupStage(
            status="planned",
            dry_run=True,
            mutation_allowed=False,
            operations=operations,
            warnings=manual_warnings,
            next_step=(
                "These destructive cleanup operations are planned only — nothing was mutated. "
                "Re-run with cleanup_on_failure=true to execute them, or run the named tools "
                "individually."
            ),
        )

    # Explicit opt-in: execute each planned operation in order, recording every result.
    results: List[Dict[str, Any]] = []
    warnings: List[str] = list(manual_warnings)
    all_ok = True
    for op in operations:
        caller = _CLEANUP_TOOL_DISPATCH.get(op.tool)
        if caller is None:
            all_ok = False
            warnings.append(f"No cleanup dispatcher registered for tool '{op.tool}'.")
            results.append({
                "action": op.action,
                "resource_type": op.resource_type,
                "resource_id": op.resource_id,
                "_success": False,
                "error_code": CLEANUP_OPERATION_FAILED,
                "error": f"No cleanup dispatcher registered for tool '{op.tool}'.",
            })
            continue
        result = caller(boomi_client, profile, op.action, dict(op.config))
        ok = bool(result.get("_success"))
        if not ok:
            all_ok = False
            warnings.append(
                f"Cleanup operation {op.action} ({op.resource_type} {op.resource_id}) failed: "
                f"{result.get('error') or 'unknown error'}"
            )
        results.append({
            "action": op.action,
            "resource_type": op.resource_type,
            "resource_id": op.resource_id,
            "_success": ok,
            "error_code": None if ok else CLEANUP_OPERATION_FAILED,
            "error": None if ok else result.get("error"),
        })

    # A manual-cleanup warning (attached-without-id, #129 D5) is not an executed-op failure but
    # still leaves an un-undone mutation, so it demotes a clean run to ``warning`` too.
    clean = all_ok and not manual_warnings
    return CleanupStage(
        status="completed" if clean else "warning",
        dry_run=False,
        mutation_allowed=True,
        operations=operations,
        results=results,
        warnings=warnings,
        next_step=(
            "Cleanup completed; re-run orchestrate_deploy to retry the deployment."
            if clean
            else "Some cleanup operations failed or require manual intervention; inspect "
            "cleanup.results/warnings and clean up the remaining resources manually before "
            "retrying."
        ),
    )


def _attach_failure_metadata(
    response: Dict[str, Any],
    error: OrchestrateDeployError,
    package: Optional[PackageStage],
    deployment: Optional[DeploymentStage],
    runtime_attachment: Optional[RuntimeAttachmentStage],
    schedule: Optional[ScheduleStage],
    *,
    execution: Optional[ExecutionStage] = None,
) -> Dict[str, Any]:
    """Add the top-level failure-hardening keys to a failed response envelope (issue #65).

    Every failed real-run response carries ``error_code`` (the structured code), ``failed_stage``
    (which stage failed), ``prior_stage_summary`` (what already succeeded), and ``next_step`` (one
    actionable hint). Success responses never gain these keys.
    """
    failed_stage = _failed_stage_for_error_code(error.code)
    response["error_code"] = error.code
    response["failed_stage"] = failed_stage
    response["prior_stage_summary"] = _prior_stage_summary(
        failed_stage, package, deployment, runtime_attachment, schedule, execution=execution,
    )
    response["next_step"] = _next_step_for_failure(error, failed_stage)
    return response


def _listener_no_test_warning() -> str:
    return (
        "[LISTENER_NO_TEST_MODE] listener processes cannot run in Test mode — the "
        "listener_verify live probe + execution-record readback is the behavioral test."
    )


def _listener_placeholder_stage(
    listener_meta: Optional[Dict[str, Any]], *, blocked: bool
) -> ListenerVerifyStage:
    """listener_verify placeholder: not_required (non-listener), planned, or blocked."""
    if not listener_meta:
        return ListenerVerifyStage(status="not_required")
    if blocked:
        return ListenerVerifyStage(status="blocked")
    return ListenerVerifyStage(
        status="planned",
        endpoint_path=listener_meta.get("endpoint_path"),
        http_method=listener_meta.get("http_method"),
        # M6.1 (#133): surface the resolved publish mode (bare_wss vs
        # api_service) on planned placeholders so dry-run callers see which
        # pattern the real run would verify.
        publish_mode=listener_meta.get("publish_mode"),
        api_service_component_id=listener_meta.get("api_service_component_id"),
        route_process_ids=[
            str(pid)
            for pid in (listener_meta.get("route_process_ids") or [])
            if isinstance(pid, str)
        ],
    )


def _execution_log_cleanup_stages(
    run_test: bool, *, blocked: bool, listener: bool = False
) -> Dict[str, Any]:
    """Execution/log/cleanup stages — still M3.4 placeholders (planned/skipped) or ``blocked``.

    M6 (#12): a listener build has no Test mode, so ``run_test=True`` resolves
    the execution/logs stages to ``not_required`` with an explanatory warning —
    the listener_verify probe is the behavioral test.
    """
    if blocked:
        return {
            "execution": ExecutionStage(status="blocked", run_test=bool(run_test)),
            "logs": LogsStage(status="blocked"),
            "cleanup": CleanupStage(status="blocked"),
        }
    run_test_flag = bool(run_test)
    if listener and run_test_flag:
        return {
            "execution": ExecutionStage(
                status="not_required",
                run_test=True,
                warnings=[_listener_no_test_warning()],
            ),
            "logs": LogsStage(status="not_required"),
            "cleanup": CleanupStage(status="not_required"),
        }
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
    listener_meta: Optional[Dict[str, Any]] = None,
) -> Dict[str, Any]:
    """Runtime/schedule/execution/log/cleanup stages as plan placeholders (dry-run / M3.4)."""
    schedule_planned = schedule_override is not None
    return {
        "runtime_attachment": RuntimeAttachmentStage(status="planned", runtime_id=runtime_id),
        "schedule": ScheduleStage(
            status="planned" if schedule_planned else "not_required",
            schedule_override=schedule_override,
        ),
        "listener_verify": _listener_placeholder_stage(listener_meta, blocked=False),
        **_execution_log_cleanup_stages(
            run_test, blocked=False, listener=bool(listener_meta)
        ),
    }


def _blocked_downstream_stages(
    runtime_id: Optional[str],
    schedule_override: Optional[Dict[str, Any]],
    run_test: bool,
    listener_meta: Optional[Dict[str, Any]] = None,
) -> Dict[str, Any]:
    """All later stages marked ``blocked`` after a package/deploy failure short-circuit."""
    return {
        "runtime_attachment": RuntimeAttachmentStage(status="blocked", runtime_id=runtime_id),
        "schedule": ScheduleStage(status="blocked", schedule_override=schedule_override),
        "listener_verify": _listener_placeholder_stage(listener_meta, blocked=True),
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
        # Additive behavioral-verification marker (issue #81) — present on every full envelope;
        # every terminal path routes through here, so dry-run/plan/blocked/test paths all carry it.
        # M6 (#12): a listener build's marker keys off the listener_verify stage.
        "behavior_verified": _behavior_verified_marker(
            dry_run=dry_run,
            execution=downstream["execution"],
            logs=downstream["logs"],
            listener_verify=downstream.get("listener_verify"),
        ),
        "integration_name": target.integration_name,
        "target": target.model_dump(),
        "component_summary": target.component_summary.model_dump(),
        "package": package.model_dump(),
        "deployment": deployment.model_dump(),
        "runtime_attachment": downstream["runtime_attachment"].model_dump(),
        "schedule": downstream["schedule"].model_dump(),
        # M6 (#12): not_required for every non-listener build; downstream dicts
        # built before the listener stage existed default safely.
        "listener_verify": (
            downstream.get("listener_verify") or ListenerVerifyStage(status="not_required")
        ).model_dump(),
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
    listener_meta: Optional[Dict[str, Any]] = None,
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
    downstream = _placeholder_downstream_stages(
        runtime_id, schedule_override, run_test, listener_meta
    )
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
            package, deployment, downstream["runtime_attachment"], downstream["schedule"],
            listener_verify=downstream.get("listener_verify") if listener_meta else None,
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
    listener_verify: Optional[ListenerVerifyStage] = None,
) -> Dict[str, Any]:
    """Successful real-run response after package, deploy, runtime binding, and schedule."""
    downstream = {
        "runtime_attachment": runtime_attachment,
        "schedule": schedule,
        "listener_verify": listener_verify or ListenerVerifyStage(status="not_required"),
        **_execution_log_cleanup_stages(
            run_test, blocked=False, listener=listener_verify is not None
        ),
    }
    response = _assemble_response(
        success=True,
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
            listener_verify=listener_verify,
        ),
        errors=[],
    )
    if listener_verify is not None and listener_verify.warnings:
        response.setdefault("warnings", []).extend(listener_verify.warnings)
    return response


def _blocked_real_run_response(
    *,
    profile: Optional[str],
    build_id: Optional[str],
    target: ResolvedBuildTarget,
    package: PackageStage,
    deployment: DeploymentStage,
    runtime_id: Optional[str],
    environment_id: Optional[str],
    schedule_override: Optional[Dict[str, Any]],
    run_test: bool,
    error: OrchestrateDeployError,
    cleanup_on_failure: bool = False,
    boomi_client: Any = None,
    listener_meta: Optional[Dict[str, Any]] = None,
) -> Dict[str, Any]:
    """Failed real-run response: the failing stage is marked, all later stages ``blocked``.

    Only the package/deploy stages ran on this path (runtime binding + schedule never started),
    so the cleanup plan considers package + deployment only — typically just ``delete_package``
    when a package was created before the deploy failed (issue #65).
    """
    downstream = _blocked_downstream_stages(
        runtime_id, schedule_override, run_test, listener_meta
    )
    downstream["cleanup"] = _cleanup_stage_for_failure(
        package, deployment, None,
        environment_id=environment_id,
        cleanup_on_failure=cleanup_on_failure,
        boomi_client=boomi_client,
        profile=profile,
    )
    response = _assemble_response(
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
    return _attach_failure_metadata(response, error, package, deployment, None, None)


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
    environment_id: Optional[str] = None,
    cleanup_on_failure: bool = False,
    boomi_client: Any = None,
    listener_verify: Optional[ListenerVerifyStage] = None,
    listener_meta: Optional[Dict[str, Any]] = None,
) -> Dict[str, Any]:
    """Failed real-run after deploy: a runtime/schedule/listener stage failed; later blocked.

    The runtime and schedule stages are passed through verbatim so the response shows exactly
    how far binding got (a failed runtime stage with schedule blocked, or a completed runtime
    stage with a failed schedule stage). M6 (#12): a failed listener_verify stage is passed
    through the same way (runtime + schedule already succeeded). The cleanup plan names exactly
    the attachment legs / deployment / package this attempt created, in reverse creation order
    (issue #65).
    """
    downstream = {
        "runtime_attachment": runtime_attachment,
        "schedule": schedule,
        "listener_verify": (
            listener_verify or _listener_placeholder_stage(listener_meta, blocked=True)
        ),
        **_execution_log_cleanup_stages(run_test, blocked=True),
    }
    downstream["cleanup"] = _cleanup_stage_for_failure(
        package, deployment, runtime_attachment,
        environment_id=environment_id or deployment.environment_id,
        cleanup_on_failure=cleanup_on_failure,
        boomi_client=boomi_client,
        profile=profile,
        # M6.1 (#133): a fresh ASC package/deployment recorded on the failed
        # listener stage joins the cleanup plan (undone first).
        listener_verify=listener_verify,
    )
    response = _assemble_response(
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
            package, deployment, runtime_attachment, schedule,
            listener_verify=listener_verify,
        ),
        errors=[error],
        error_message=error.message,
    )
    if listener_verify is not None and listener_verify.warnings:
        response.setdefault("warnings", []).extend(listener_verify.warnings)
    return _attach_failure_metadata(
        response, error, package, deployment, runtime_attachment, schedule
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
    environment_id: Optional[str] = None,
    cleanup_on_failure: bool = False,
    boomi_client: Any = None,
) -> Dict[str, Any]:
    """Real-run response embedding the concrete run-test execution + logs stages (issue #63).

    Mirrors ``_real_run_response`` but carries the real ``ExecutionStage``/``LogsStage``. On
    SUCCESS the cleanup stage stays ``not_required`` and no failure metadata is added. On a failed
    test execution (``success=False``) the response gains the issue-#65 failure metadata and a
    cleanup plan naming what this attempt created (the deployment itself succeeded; cleanup is
    informational/dry-run by default). Top-level ``warnings`` surface the stages' warnings.
    """
    if success:
        cleanup_stage: CleanupStage = CleanupStage(status="not_required")
    else:
        cleanup_stage = _cleanup_stage_for_failure(
            package, deployment, runtime_attachment,
            environment_id=environment_id or deployment.environment_id,
            cleanup_on_failure=cleanup_on_failure,
            boomi_client=boomi_client,
            profile=profile,
        )
    downstream = {
        "runtime_attachment": runtime_attachment,
        "schedule": schedule,
        "execution": execution,
        "logs": logs,
        "cleanup": cleanup_stage,
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
    if not success and errors:
        _attach_failure_metadata(
            response, errors[0], package, deployment, runtime_attachment, schedule,
            execution=execution,
        )
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
    cleanup_on_failure: bool = False,
    test_timeout_seconds: int = 300,
    test_dynamic_properties: Optional[Dict[str, Any]] = None,
    test_process_properties: Optional[Dict[str, Any]] = None,
    test_log_level: str = "ALL",
    test_fetch_logs: bool = True,
    test_fetch_artifacts: bool = True,
    test_log_fetch_content: bool = True,
    require_test_logs: bool = False,
    process_overrides: Optional[Dict[str, Any]] = None,
    listener_test_payload: Optional[str] = None,
    listener_base_url: Optional[str] = None,
    listener_probe_timeout_seconds: int = 30,
    listener_auth_username: Optional[str] = None,
    creds: Optional[Dict[str, str]] = None,
) -> Dict[str, Any]:
    """Resolve a build, then plan (dry-run) or package/deploy/bind/schedule it (#60/#61/#62).

    With ``dry_run=True`` (the default) no ``boomi_client`` call is made — every stage is
    reported as it *would* run. With ``dry_run=False`` the package, deploy, runtime-binding,
    and schedule stages run for real (idempotently) through the sibling action routers, in that
    order; any stage failure returns structured error codes and blocks every later stage. An
    invalid ``schedule_override`` is rejected up front (before any SDK call) in both modes. All
    inputs are nullable so missing required values yield structured failures instead of raising.

    Failure hardening (issue #65): every failed real-run response also carries top-level
    ``error_code``, ``failed_stage``, ``prior_stage_summary`` (what already succeeded, with ids),
    and ``next_step`` (one actionable hint), plus a ``cleanup`` plan that names — in reverse
    creation order — exactly the schedule/attachments/deployment/package THIS attempt created.
    The cleanup plan defaults to dry-run (no mutation); pass ``cleanup_on_failure=True`` to execute
    it. Because every stage reuses (never duplicates) existing resources, a retry after a partial
    failure resumes safely from the failed stage.

    Behavioral verification (issue #81): every full response carries a top-level
    ``behavior_verified`` marker (``{"verified", "reason", "logs_status"}``). Deploy/test success is
    not behavioral correctness — read the returned log excerpts before declaring an integration
    working; a terminal COMPLETE status alone is not behavioral verification. Set
    ``require_test_logs=True`` to make a failed/absent ProcessLog fetch (after a test ran) fail the
    orchestration with ``TEST_LOGS_UNAVAILABLE`` instead of the default diagnostic-only
    success-with-warning. The marker is additive and ``require_test_logs`` defaults False, so the
    existing success shape is preserved.
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
            cleanup_on_failure=cleanup_on_failure,
            test_timeout_seconds=test_timeout_seconds,
            test_dynamic_properties=test_dynamic_properties,
            test_process_properties=test_process_properties,
            test_log_level=test_log_level,
            test_fetch_logs=test_fetch_logs,
            test_fetch_artifacts=test_fetch_artifacts,
            test_log_fetch_content=test_log_fetch_content,
            require_test_logs=require_test_logs,
            process_overrides=process_overrides,
            listener_test_payload=listener_test_payload,
            listener_base_url=listener_base_url,
            listener_probe_timeout_seconds=listener_probe_timeout_seconds,
            listener_auth_username=listener_auth_username,
        )
    except ValidationError as exc:
        # ``request`` never bound — echo the RAW inputs; the builder sanitizes mistyped values.
        return _error_response(
            "Invalid orchestrate_deploy request.",
            [_validation_error_entry(err) for err in exc.errors()],
            profile=profile,
            build_id=build_id,
            dry_run=dry_run,
            environment_id=environment_id,
            runtime_id=runtime_id,
            schedule_override=schedule_override,
            run_test=run_test,
            package_version=package_version,
        )

    build_id = request.build_id
    environment_id = request.environment_id
    runtime_id = request.runtime_id
    profile = request.profile
    schedule_override = request.schedule_override
    run_test = request.run_test
    dry_run = request.dry_run
    package_version = request.package_version
    cleanup_on_failure = request.cleanup_on_failure

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
        return _error_response(
            "Missing required deployment inputs.",
            required_errors,
            profile=profile,
            build_id=build_id,
            dry_run=dry_run,
            environment_id=environment_id,
            runtime_id=runtime_id,
            schedule_override=schedule_override,
            run_test=run_test,
            package_version=package_version,
        )

    # 2. Resolve the build to a single process component. This happens BEFORE any SDK call,
    #    so a resolver failure (e.g. BUILD_PROCESS_ID_MISSING) never touches boomi_client —
    #    even when dry_run is False.
    target, resolve_error = _resolve_build_deployment_target(build_id)
    if resolve_error is not None:
        return _error_response(
            resolve_error.message,
            [resolve_error],
            profile=profile,
            build_id=build_id,
            dry_run=dry_run,
            environment_id=environment_id,
            runtime_id=runtime_id,
            schedule_override=schedule_override,
            run_test=run_test,
            package_version=package_version,
        )

    # 2b. Validate schedule_override CONTENT (format) up front — a fail-fast structured error
    #     in BOTH dry-run and real-run, before any SDK call. The normalized form is reused by
    #     the real-run schedule stage; an invalid override never reaches package/deploy.
    normalized_schedule, schedule_override_error = _normalize_schedule_override(schedule_override)
    if schedule_override_error is not None:
        return _error_response(
            schedule_override_error.message,
            [schedule_override_error],
            profile=profile,
            build_id=build_id,
            dry_run=dry_run,
            target=target,
            environment_id=environment_id,
            runtime_id=runtime_id,
            schedule_override=schedule_override,
            run_test=run_test,
            package_version=package_version,
        )

    # 2c. Build-basics deploy guards (issue #102). Read-only registry inspection, before any
    #     SDK call, in BOTH dry-run and real-run.
    declares_extensions = _build_declares_process_extensions(build_id)
    # M6 (#12): listener detection — a WSS listener build gets the listener_verify stage
    # (planned in dry-run, executed after schedule on a real run) and no test execution.
    listener_meta = _resolve_listener_metadata(build_id)
    process_overrides = request.process_overrides
    # B4 — an EXPLICITLY empty process-overrides set over a process that declares extensions
    #      would orphan those values. Reject fail-fast. (None = "not supplied" is fine — it
    #      preserves existing extension values; only an explicit empty {} is rejected.)
    if (
        isinstance(process_overrides, dict)
        and not process_overrides
        and declares_extensions
    ):
        return _error_response(
            "Empty process_overrides over a process that declares environment extensions.",
            [
                _error(
                    EMPTY_PROCESS_OVERRIDES_REJECTED,
                    "process_overrides was supplied as an empty set, but this build's process "
                    "declares environment extensions. Deploying empty overrides orphans those "
                    "extension values. Supply the intended overrides, or omit process_overrides "
                    "to preserve the existing environment-extension values.",
                    field="process_overrides",
                    details={"build_id": build_id},
                )
            ],
            profile=profile,
            build_id=build_id,
            dry_run=dry_run,
            target=target,
            environment_id=environment_id,
            runtime_id=runtime_id,
            schedule_override=schedule_override,
            run_test=run_test,
            package_version=package_version,
        )
    # F1 + B4 steering warnings (advisory, never block). Surfaced on the dry-run plan where
    # they are actionable before the caller commits to a real deploy.
    steering_warnings: List[str] = []
    if run_test and not require_test_logs:
        steering_warnings.append(
            "[REQUIRE_TEST_LOGS_RECOMMENDED] run_test=true without require_test_logs=true: a "
            "terminal COMPLETE status is NOT behavioral verification. Set require_test_logs=true "
            "so a missing/unavailable ProcessLog after the test fails the orchestration instead "
            "of passing silently."
        )
    if declares_extensions and process_overrides is None:
        steering_warnings.append(
            "[PROCESS_OVERRIDES_NOT_SUPPLIED] this build's process declares environment "
            "extensions but no process_overrides were supplied; the deploy preserves the "
            "existing environment-extension values and does not set them."
        )

    # 3a. Dry-run: assemble the plan-only response without any SDK call.
    if dry_run:
        plan = _plan_response(
            profile=profile,
            build_id=build_id,
            target=target,
            environment_id=environment_id,
            runtime_id=runtime_id,
            schedule_override=schedule_override,
            run_test=run_test,
            package_version=package_version,
            listener_meta=listener_meta,
        )
        if steering_warnings:
            plan.setdefault("warnings", []).extend(steering_warnings)
        return plan

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
            environment_id=environment_id,
            schedule_override=schedule_override,
            run_test=run_test,
            cleanup_on_failure=cleanup_on_failure,
            boomi_client=boomi_client,
            listener_meta=listener_meta,
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
            environment_id=environment_id,
            schedule_override=schedule_override,
            run_test=run_test,
            cleanup_on_failure=cleanup_on_failure,
            boomi_client=boomi_client,
            listener_meta=listener_meta,
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
            environment_id=environment_id,
            schedule_override=schedule_override,
            run_test=run_test,
            cleanup_on_failure=cleanup_on_failure,
            boomi_client=boomi_client,
            listener_meta=listener_meta,
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
            environment_id=environment_id,
            cleanup_on_failure=cleanup_on_failure,
            boomi_client=boomi_client,
            listener_meta=listener_meta,
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
            environment_id=environment_id,
            cleanup_on_failure=cleanup_on_failure,
            boomi_client=boomi_client,
            listener_meta=listener_meta,
        )

    # 3f½. Listener verification stage (M6 #12): only for a WSS listener build, after
    #      deploy + runtime binding + schedule succeed. apiType preflight, deployment-active
    #      check, component-query collision check, authenticated live probe, and
    #      execution-record readback. A failure blocks execution/log/cleanup like a
    #      runtime/schedule failure. A listener build never runs the Test-mode execution
    #      stage — listeners have no Test mode; the probe IS the behavioral test.
    if listener_meta is not None:
        listener_stage, listener_error = _run_listener_verify_stage(
            boomi_client,
            profile,
            request,
            target=target,
            environment_id=environment_id,
            runtime_id=runtime_id,
            listener_meta=listener_meta,
            deployment_stage=deployment_stage,
            creds=creds,
            # ASC mode packages/deploys the webservice component with the SAME
            # resolved version as the process package (M6.1 #133).
            package_version=package_stage.package_version if package_stage else None,
        )
        if listener_error is not None:
            return _runtime_or_schedule_failed_response(
                profile=profile,
                build_id=build_id,
                target=target,
                package=package_stage,
                deployment=deployment_stage,
                runtime_attachment=runtime_attachment,
                schedule=schedule_stage,
                run_test=run_test,
                error=listener_error,
                environment_id=environment_id,
                cleanup_on_failure=cleanup_on_failure,
                boomi_client=boomi_client,
                listener_verify=listener_stage,
                listener_meta=listener_meta,
            )
        return _real_run_response(
            profile=profile,
            build_id=build_id,
            target=target,
            package=package_stage,
            deployment=deployment_stage,
            runtime_attachment=runtime_attachment,
            schedule=schedule_stage,
            run_test=run_test,
            listener_verify=listener_stage,
        )

    # 3g. Success: package, deploy, runtime binding, and schedule all resolved. Without run_test
    #     the execution/log/cleanup stages stay skipped placeholders; with run_test the optional
    #     test stage executes the process (wait=True), then fetches bounded log/artifact
    #     diagnostics. A failed test execution returns _success=False with the prior stages
    #     preserved; a log/artifact fetch failure stays _success=True (diagnostic only) unless
    #     require_test_logs=True promotes a required-log failure to TEST_LOGS_UNAVAILABLE. (#63/#81)
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
        environment_id=environment_id,
        cleanup_on_failure=cleanup_on_failure,
        boomi_client=boomi_client,
    )

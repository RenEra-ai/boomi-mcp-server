"""database_to_api_sync archetype (M2.1a contract + M2.9 executable assembly).

Exposes a strict Pydantic parameter contract for a SQL Server source -> REST
target sync. Issue #29 turned this from contract-only output into an executable
assembly: it now composes the shipped #27 (db_extract, field_map) and #28
(rest_send_with_retry + operational) primitives into an executable
IntegrationSpecV1 (DB source, JSON transform, REST target, structured process)
suitable for build_integration(action='plan'). Every byte of XML is produced by
the existing component builders through those primitives; this file emits JSON
component specs only and never calls a live Boomi account. DLQ wires the
verified Try/Catch + DLQ catch path for modes document_cache_ref /
error_subprocess_ref (#51 M3.R1a), and caller retry (max_attempts 1..6 →
Try/Catch retry_count 0..5, platform-timed) is wired through to the emitted
process when a catch path is configured (#88 M4.5.3). Schedule activation,
watermark update, and dynamic operation-property wiring remain represented as
metadata only and are deferred (M3).

M2.1a (issue #44) replaces the legacy ``transform.mappings`` /
``transform.payload_template`` / ``transform.script_slots`` surface with:

  * caller-declared DB read result fields under ``source.read_operation.result_schema``,
  * caller-supplied JSON profile tree under ``target.payload_profile``, and
  * discriminated typed transform operations under ``transform.operations``
    (``direct`` -> #26, ``map_function`` -> #40, ``map_script`` -> #41;
    ``xslt`` is rejected with a pointer to #42).

The archetype does not parse SQL, browse the database, sample rows, infer
schema, or import existing integrations. Read-only profile-field inference from
supplied metadata / sample JSON / XSD / sample XML is available separately via
infer_profile_fields (issue #47); integration import is issue #48; live SQL
parsing / DB browse / row sampling remain out of scope.
"""

from __future__ import annotations

import re
from typing import Any, Dict, List, Literal, Optional, Set, Tuple

from pydantic import (
    BaseModel,
    ConfigDict,
    Field,
    field_validator,
    model_validator,
)

from ...categories.components.builders.connector_builder import (
    BuilderValidationError,
)
from ...categories.components.builders.process_flow_builder import (
    SyncPipelineBuilder,
    _NOTIFY_CAUGHT_ERROR_TOKEN,
    _SUPPORTED_NOTIFY_LEVELS,
)
from ...categories.components.builders.profile_generation import (
    build_profile_generation_artifacts,
)
from ...categories.components.builders.json_profile_builder import (
    JSONGeneratedProfileBuilder,
)
from ...models.integration_models import (
    IntegrationComponentSpec,
    IntegrationSpecV1,
)
from ..base import (
    ArchetypePattern,
    PatternExample,
    PatternKind,
    PatternMetadata,
    PrimitiveBuildContext,
)
from ..primitives._helpers import (
    ROLE_DB_CONNECTION,
    ROLE_DB_GET_OPERATION,
    ROLE_DB_READ_PROFILE,
    ROLE_REST_CONNECTION,
    ROLE_REST_OPERATION,
    ROLE_SCRIPT,
    ROLE_TARGET_PROFILE,
    ROLE_TRANSFORM_MAP,
    primitive_component_key,
)
from ..primitives.db_extract import (
    DbExtractParameters,
    DbExtractPrimitive,
    db_connection_extension_fields,
)
from ..primitives.field_map import FieldMapParameters, FieldMapPrimitive
from ..primitives.operational import (
    ErrorClassifierParameters,
    ErrorClassifierPrimitive,
    RunMetadataParameters,
    RunMetadataPrimitive,
    ScheduleEnvelopeParameters,
    ScheduleEnvelopePrimitive,
    WatermarkStateParameters,
    WatermarkStatePrimitive,
)
from ..primitives.rest_send import (
    RestSendWithRetryParameters,
    RestSendWithRetryPrimitive,
    rest_connection_extension_fields,
)

# The shared parameter contracts and assembly helpers this archetype used to own
# live in neutral modules now (#151, M12.14) so the archetypes that survive the
# #160 deletion do not import this scheduled module.
from ..archetype_parameters import (
    DatabaseSource,
    DirectTransformOperation,
    MapFunctionTransformOperation,
    MapScriptTransformOperation,
    NamingConfig,
    RestTarget,
    TransformConfig,
    _flatten_payload_profile_leaves,
    _required_simple_leaf_paths,
    _stripped_nonblank,
)
from ..archetype_assembly import (
    UNSUPPORTED_REST_AUTH_MODE,
    UNSUPPORTED_SCRIPT_COMPONENT_REF,
    _MAIN_PROCESS_KEY,
    _REST_CREATE_AUTH_MAP,
    _SOURCE_PREFIX,
    _TARGET_PREFIX,
    _TRANSFORM_PREFIX,
    _coerce_primitive_params,
    _component_names,
    _named,
)


# ---------------------------------------------------------------------------
# Execution
# ---------------------------------------------------------------------------


class Schedule(BaseModel):
    model_config = ConfigDict(extra="forbid")

    cron: str = Field(
        ...,
        description=(
            "Cron expression for the scheduled trigger. The contract does "
            "not parse or validate the cron syntax; downstream builders pass "
            "it to the Boomi schedule shape verbatim."
        ),
    )
    timezone: Optional[str] = Field(
        default=None,
        description=(
            "Optional IANA timezone string (e.g. 'UTC', 'America/New_York'). "
            "Surfaced verbatim to downstream builders; the contract does not "
            "validate it."
        ),
    )

    @field_validator("cron")
    @classmethod
    def _strip_cron(cls, value: str) -> str:
        return _stripped_nonblank(value)

    @field_validator("timezone")
    @classmethod
    def _strip_optional(cls, value: Optional[str]) -> Optional[str]:
        if value is None:
            return None
        return _stripped_nonblank(value)


class ExecutionTrigger(BaseModel):
    model_config = ConfigDict(extra="forbid")

    mode: Literal["manual", "scheduled"] = Field(
        ...,
        description=(
            "How the integration is started. 'manual' is invoked on demand; "
            "'scheduled' requires a Schedule and is fired by the Boomi "
            "scheduler once executable builders ship."
        ),
    )
    schedule: Optional[Schedule] = Field(
        default=None,
        description=(
            "Schedule configuration. Required when mode='scheduled'; must be "
            "omitted when mode='manual'."
        ),
    )

    @model_validator(mode="after")
    def _enforce_schedule_consistency(self) -> "ExecutionTrigger":
        if self.mode == "scheduled" and self.schedule is None:
            raise ValueError("mode='scheduled' requires schedule")
        if self.mode == "manual" and self.schedule is not None:
            raise ValueError("mode='manual' must not supply schedule")
        return self


class Watermark(BaseModel):
    model_config = ConfigDict(extra="forbid")

    field: str = Field(
        ...,
        description=(
            "Name of a source result field driving high-water-mark "
            "advancement (e.g. 'last_modified_at'). Must reference a name "
            "declared in source.read_operation.result_schema.fields; the "
            "cross-field validator rejects unknown names."
        ),
    )
    kind: Literal["timestamp", "sequence"] = Field(
        ...,
        description=(
            "Watermark kind. 'timestamp' compares chronological values; "
            "'sequence' compares monotonically-increasing integers."
        ),
    )
    initial_value: Optional[str] = Field(
        default=None,
        description=(
            "Optional initial high-water-mark value used on the first run "
            "before any state has been persisted. Surfaced verbatim to "
            "downstream builders."
        ),
    )
    persistence: Literal["dpp", "external_store"] = Field(
        default="dpp",
        description=(
            "Where the watermark is persisted. 'dpp' uses Boomi Dynamic "
            "Process Properties; 'external_store' delegates to an external "
            "key/value store whose binding is configured by future builders."
        ),
    )

    @field_validator("field")
    @classmethod
    def _strip_field(cls, value: str) -> str:
        return _stripped_nonblank(value)

    @field_validator("initial_value")
    @classmethod
    def _strip_optional(cls, value: Optional[str]) -> Optional[str]:
        if value is None:
            return None
        return _stripped_nonblank(value)


class ExecutionConfig(BaseModel):
    model_config = ConfigDict(extra="forbid")

    trigger: ExecutionTrigger = Field(
        ...,
        description=(
            "How the integration is started (manual or scheduled). Scheduled "
            "triggers require a Schedule."
        ),
    )
    watermark: Optional[Watermark] = Field(
        default=None,
        description=(
            "Optional high-water-mark configuration for incremental syncs. "
            "Omit for full-extract runs."
        ),
    )
    run_metadata: Dict[str, str] = Field(
        default_factory=dict,
        description=(
            "Optional opaque key/value metadata associated with every run "
            "(e.g. business owner, runbook URL). Surfaced verbatim to "
            "downstream builders; the contract does not interpret keys."
        ),
    )


# ---------------------------------------------------------------------------
# Reliability
# ---------------------------------------------------------------------------


class RetryPolicy(BaseModel):
    model_config = ConfigDict(extra="forbid")

    max_attempts: int = Field(
        default=1,
        ge=1,
        le=6,
        description=(
            "Total attempts per record, 1..6 (1 means no retry). Maps to the "
            "Boomi Try/Catch Retry Count as retry_count = max_attempts - 1 "
            "(so 6 attempts = 5 retries, the platform maximum). Wired to the "
            "emitted process only when a Try/Catch catch path (DLQ) is "
            "configured."
        ),
    )
    backoff: Literal["platform"] = Field(
        default="platform",
        description=(
            "Retry timing is platform-controlled and not caller-selectable: "
            "the Boomi Try/Catch shape retries the first attempt immediately "
            "and applies its built-in escalating wait schedule for subsequent "
            "retries (counts 2..5). Arbitrary fixed/exponential backoff with a "
            "caller-chosen interval is NOT supported by the platform; for a "
            "custom backoff window, design a scheduled re-run or queue-based "
            "retry instead (guidance_only — see design_doctrine "
            "connector_retry_design)."
        ),
    )


class DlqTarget(BaseModel):
    """Dead-letter destination, aligned to the builder's verified DLQ modes.

    The process builder (issue #51 M3.R1a) emits a verified Try/Catch + DLQ
    catch path for exactly two modes — ``document_cache_ref`` (catch leg routes
    to a Document Cache, bound via ``document_cache_id``) and
    ``error_subprocess_ref`` (catch leg calls an error subprocess, bound via
    ``process_id``). The binding is a literal Boomi component id or a
    ``$ref:KEY`` token whose KEY is an in-spec component. Legacy
    folder/topic/queue routing is NOT an emittable builder mode; it is retained
    ONLY as an explicitly-labeled ``guidance_only`` alias that records intent
    as metadata (no wiring) — never silently accepted as a real DLQ.
    """

    model_config = ConfigDict(extra="forbid")

    mode: Literal["document_cache_ref", "error_subprocess_ref", "guidance_only"] = Field(
        ...,
        description=(
            "Verified DLQ mode. 'document_cache_ref' / 'error_subprocess_ref' "
            "emit a real Try/Catch + DLQ catch path (require document_cache_id "
            "/ process_id). 'guidance_only' records legacy folder/topic/queue "
            "intent as metadata only and emits no wiring."
        ),
    )
    document_cache_id: Optional[str] = Field(
        default=None,
        description=(
            "DLQ Document Cache binding for mode='document_cache_ref': a literal "
            "Boomi component id, or a '$ref:KEY' token referencing an in-spec "
            "Document Cache component. Required for that mode; rejected otherwise."
        ),
    )
    process_id: Optional[str] = Field(
        default=None,
        description=(
            "Error-subprocess binding for mode='error_subprocess_ref': a literal "
            "Boomi component id, or a '$ref:KEY' token referencing an in-spec "
            "process/subprocess component. Required for that mode; rejected "
            "otherwise."
        ),
    )
    kind: Optional[Literal["folder", "topic", "queue"]] = Field(
        default=None,
        description=(
            "Legacy routing kind — accepted ONLY with mode='guidance_only' "
            "(recorded as metadata, never wired)."
        ),
    )
    address: Optional[str] = Field(
        default=None,
        description=(
            "Legacy destination address — accepted ONLY with "
            "mode='guidance_only'. Never echoed back (may carry sensitive "
            "content); only its presence is recorded."
        ),
    )
    reason: Optional[str] = Field(
        default=None,
        description="Optional free-form note for a guidance_only target.",
    )

    @field_validator("document_cache_id", "process_id", "address")
    @classmethod
    def _strip_required_present(cls, value: Optional[str]) -> Optional[str]:
        if value is None:
            return None
        return _stripped_nonblank(value)

    @model_validator(mode="after")
    def _enforce_mode_fields(self) -> "DlqTarget":
        if self.mode == "document_cache_ref":
            if not self.document_cache_id:
                raise ValueError(
                    "dlq.target.mode='document_cache_ref' requires "
                    "document_cache_id (a Boomi component id or '$ref:KEY')."
                )
            if self.process_id or self.kind or self.address:
                raise ValueError(
                    "dlq.target.mode='document_cache_ref' accepts only "
                    "document_cache_id (no process_id/kind/address)."
                )
        elif self.mode == "error_subprocess_ref":
            if not self.process_id:
                raise ValueError(
                    "dlq.target.mode='error_subprocess_ref' requires process_id "
                    "(a Boomi component id or '$ref:KEY')."
                )
            if self.document_cache_id or self.kind or self.address:
                raise ValueError(
                    "dlq.target.mode='error_subprocess_ref' accepts only "
                    "process_id (no document_cache_id/kind/address)."
                )
        else:  # guidance_only
            if not self.kind or not self.address:
                raise ValueError(
                    "dlq.target.mode='guidance_only' requires kind and address "
                    "(legacy folder/topic/queue intent recorded as metadata only "
                    "— no builder wiring)."
                )
            if self.document_cache_id or self.process_id:
                raise ValueError(
                    "dlq.target.mode='guidance_only' must not set "
                    "document_cache_id/process_id."
                )
        return self


class DlqPolicy(BaseModel):
    model_config = ConfigDict(extra="forbid")

    enabled: bool = Field(
        default=False,
        description=(
            "Whether dead-letter routing is enabled. When True, target is "
            "required."
        ),
    )
    target: Optional[DlqTarget] = Field(
        default=None,
        description=(
            "Dead-letter destination. Required when enabled=True; must be "
            "omitted when enabled=False."
        ),
    )

    @model_validator(mode="after")
    def _enforce_target(self) -> "DlqPolicy":
        if self.enabled and self.target is None:
            raise ValueError("dlq.enabled=True requires target")
        if not self.enabled and self.target is not None:
            raise ValueError("dlq.enabled=False must not supply target")
        return self


class ErrorClassifier(BaseModel):
    model_config = ConfigDict(extra="forbid")

    retriable_status_codes: List[int] = Field(
        default_factory=lambda: [502, 503, 504],
        description=(
            "HTTP status codes treated as retriable by the reliability "
            "policy. Defaults to common transient codes."
        ),
    )
    terminal_status_codes: List[int] = Field(
        default_factory=lambda: [400, 401, 403, 404, 422],
        description=(
            "HTTP status codes treated as terminal (no retry) by the "
            "reliability policy. Defaults to common client-error codes."
        ),
    )
    custom_rules: List[str] = Field(
        default_factory=list,
        description=(
            "Optional free-form rule labels describing additional classifier "
            "behavior to be implemented by downstream builders (e.g. "
            "'rate_limit_exhausted'). Values are opaque labels; no scripts."
        ),
    )


class CatchNotifyConfig(BaseModel):
    """Optional Notify step on the Try/Catch catch leg (issue #89 M4.5.4).

    Emitted at the head of a wired DLQ catch path
    (``catch -> notify -> dlq route -> stop``). Log-only: the emitted Notify has
    no platform email event, so email/SMS channels are out of scope. The message
    must reference the platform caught-error property so the Notify logs the real
    error; the builder binds that property as a notify track parameter.
    """

    model_config = ConfigDict(extra="forbid")

    message_template: str = Field(
        ...,
        description=(
            "Notify message text. Must reference the platform caught-error "
            "property token (meta.base.catcherrorsmessage); the builder binds it "
            "as a notify track parameter so the emitted Notify logs the real "
            "caught error. Parameterized text only — no canned content."
        ),
    )
    level: str = Field(
        ...,
        description=(
            "Notify message level: one of INFO, WARNING, ERROR (case-insensitive; "
            "normalized to uppercase)."
        ),
    )

    @field_validator("message_template")
    @classmethod
    def _require_caught_error_token(cls, value: str) -> str:
        text = _stripped_nonblank(value)
        if _NOTIFY_CAUGHT_ERROR_TOKEN not in text:
            raise ValueError(
                "reliability.catch_notify.message_template must reference the "
                f"caught-error property token ({_NOTIFY_CAUGHT_ERROR_TOKEN}) so "
                "the Notify logs the caught error."
            )
        return text

    @field_validator("level")
    @classmethod
    def _normalize_level(cls, value: str) -> str:
        canonical = value.strip().upper()
        if canonical not in _SUPPORTED_NOTIFY_LEVELS:
            raise ValueError(
                "reliability.catch_notify.level must be one of "
                f"{sorted(_SUPPORTED_NOTIFY_LEVELS)}."
            )
        return canonical


class ReliabilityConfig(BaseModel):
    model_config = ConfigDict(extra="forbid")

    retry: RetryPolicy = Field(
        ...,
        description=(
            "Retry policy applied to retriable failures during the send step."
        ),
    )
    dlq: DlqPolicy = Field(
        ...,
        description=(
            "Dead-letter queue policy applied after retries are exhausted."
        ),
    )
    error_classifier: ErrorClassifier = Field(
        ...,
        description=(
            "Rules that classify response errors as retriable or terminal. "
            "Drives the retry and DLQ policies above."
        ),
    )
    catch_notify: Optional[CatchNotifyConfig] = Field(
        default=None,
        description=(
            "Optional Notify step emitted at the head of the Try/Catch catch leg "
            "(issue #89). Requires a wired DLQ (target.mode='document_cache_ref' "
            "or 'error_subprocess_ref'); logs the caught error to the process "
            "log. Email/SMS notification channels are out of scope."
        ),
    )

    @model_validator(mode="after")
    def _enforce_retry_requires_wired_dlq(self) -> "ReliabilityConfig":
        # Issue #88 (M4.5.3): positive retry is only emittable inside a
        # Try/Catch whose catch leg routes to a DLQ — the platform Try/Catch
        # shape always carries a catch leg. So max_attempts > 1 (retry_count
        # > 0) requires a wired DLQ mode (document_cache_ref /
        # error_subprocess_ref); guidance_only / disabled DLQ cannot carry
        # retry. This mirrors the builder's PROCESS_RETRY_UNVERIFIED rule at the
        # contract layer with a clean parameter-validation error.
        if self.retry.max_attempts > 1:
            target = self.dlq.target if self.dlq.enabled else None
            wired = target is not None and target.mode in (
                "document_cache_ref",
                "error_subprocess_ref",
            )
            if not wired:
                raise ValueError(
                    "reliability.retry.max_attempts > 1 requires a wired DLQ "
                    "catch path: set reliability.dlq.enabled=true with "
                    "target.mode='document_cache_ref' or 'error_subprocess_ref'. "
                    "Positive retry is emitted only inside a Try/Catch whose "
                    "catch leg routes to a DLQ."
                )
        # Issue #89 (M4.5.4): the Notify step lives at the head of the catch
        # leg, so catch_notify likewise requires a wired DLQ catch path
        # (guidance_only / disabled DLQ has no catch leg to host it).
        if self.catch_notify is not None:
            target = self.dlq.target if self.dlq.enabled else None
            wired = target is not None and target.mode in (
                "document_cache_ref",
                "error_subprocess_ref",
            )
            if not wired:
                raise ValueError(
                    "reliability.catch_notify requires a wired DLQ catch path: "
                    "set reliability.dlq.enabled=true with "
                    "target.mode='document_cache_ref' or 'error_subprocess_ref'. "
                    "Notify is emitted only at the head of a Try/Catch catch leg."
                )
        return self


# ---------------------------------------------------------------------------
# Top-level parameters
# ---------------------------------------------------------------------------


class EnvironmentExtensionsConfig(BaseModel):
    """Which source DB and REST target connection fields the emitted process
    declares as per-environment override points (issue #92 M4.5.7 / #102 B1).

    A deployed Boomi process exposes a connection field through
    ``manage_environments(get_extensions)`` / ``update_extensions`` ONLY when the
    process declares it as an extension. Declaring the credential fields by
    default lets TEST -> PROD promotion supply the password per environment
    without embedding it in the connection component (the live-proven failure
    this milestone fixes). Endpoint fields default OFF because host/port are
    usually stable within a deployment lane and changing them can affect
    connection-pool / runtime behavior — so they require explicit opt-in.

    Applies to create-mode ``username_password`` DB sources AND reuse-mode DB
    sources (issue #102 B1 — an existing connection still benefits from
    per-environment credential overrides); ``windows_integrated`` create declares
    nothing. The REST target connection is covered separately by
    ``rest_credential_connection_fields`` / ``rest_endpoint_connection_fields``.
    Runtime acceptance is ``live_QA_required``.
    """

    model_config = ConfigDict(extra="forbid")

    credential_connection_fields: bool = Field(
        default=True,
        description=(
            "Declare the source DB connection credential fields (username, "
            "password) as per-environment override points. Default true: "
            "credentials vary per environment and must not be embedded in the "
            "connection component."
        ),
    )
    endpoint_connection_fields: bool = Field(
        default=False,
        description=(
            "Also declare the source DB connection endpoint fields (host, port) "
            "as override points. Default false (opt-in): endpoints are usually "
            "stable within a deployment lane; override them only when promotion "
            "actually retargets the host/port."
        ),
    )
    rest_credential_connection_fields: bool = Field(
        default=True,
        description=(
            "Issue #102 B1: declare the REST target connection credential fields "
            "(username, password) as per-environment override points. Default "
            "true: an authenticated REST target's credential must not be embedded "
            "in the connection component. Applies to REUSE-mode REST targets (an "
            "existing, already-authenticated connection); create-mode REST is "
            "always unauthenticated, so no REST credential is declared for it."
        ),
    )
    rest_endpoint_connection_fields: bool = Field(
        default=False,
        description=(
            "Issue #102 B1: also declare the REST target connection endpoint "
            "field (Base URL) as an override point. Default false (opt-in): "
            "declare it when TEST -> PROD promotion retargets the REST base URL."
        ),
    )


class DatabaseToApiSyncParameters(BaseModel):
    model_config = ConfigDict(extra="forbid")

    naming: NamingConfig = Field(
        ...,
        description=(
            "Naming, folder, and runtime-hint configuration for the emitted "
            "integration."
        ),
    )
    source: DatabaseSource = Field(
        ...,
        description=(
            "Database source configuration: connector binding, read operation, "
            "and caller-declared result schema."
        ),
    )
    target: RestTarget = Field(
        ...,
        description=(
            "REST target configuration: connector binding, send request, and "
            "caller-supplied JSON payload profile tree."
        ),
    )
    transform: TransformConfig = Field(
        ...,
        description=(
            "Typed transform operations that move source result fields into "
            "target JSON profile leaves. Discriminated by operation_type: "
            "direct (#26), map_function (#40), map_script (#41); xslt is "
            "rejected (#42)."
        ),
    )
    execution: ExecutionConfig = Field(
        ...,
        description=(
            "Execution trigger, optional watermark, and run metadata for "
            "the integration."
        ),
    )
    reliability: ReliabilityConfig = Field(
        ...,
        description=(
            "Retry policy, dead-letter policy, and error classifier applied "
            "to the send step."
        ),
    )
    environment_extensions: EnvironmentExtensionsConfig = Field(
        default_factory=EnvironmentExtensionsConfig,
        description=(
            "Which connection fields the emitted process declares as "
            "per-environment override points (issue #92 M4.5.7 / #102 B1). "
            "Defaults: DB credential fields (username, password) declared "
            "extensible; DB endpoint fields (host, port) opt-in. The REST target "
            "declares reuse-mode credentials by default and the base URL on "
            "opt-in (rest_*_connection_fields). Create-mode username_password AND "
            "reuse-mode DB sources emit a DB declaration; windows_integrated "
            "create declares nothing."
        ),
    )

    @model_validator(mode="after")
    def _enforce_watermark_consistency(self) -> "DatabaseToApiSyncParameters":
        if self.execution.watermark is not None:
            return self
        has_watermark_param = any(
            qp.value_source == "watermark"
            for qp in self.target.send_request.query_parameters
        )
        if has_watermark_param:
            # The offending parameter names are deliberately omitted: this
            # error envelope mirrors pattern_validation_error()'s policy of
            # never echoing caller-supplied input values, which can contain
            # credentials or other sensitive content.
            raise ValueError(
                "target.send_request.query_parameters with "
                "value_source='watermark' require execution.watermark to be "
                "configured"
            )
        return self

    @model_validator(mode="after")
    def _validate_transform_refs(self) -> "DatabaseToApiSyncParameters":
        source_field_names: Set[str] = {
            f.name for f in self.source.read_operation.result_schema.fields
        }
        target_leaves: Dict[str, str] = _flatten_payload_profile_leaves(
            self.target.payload_profile
        )

        unknown_source_refs = 0
        unknown_target_refs = 0
        duplicate_target_bindings = 0
        bound_target_paths: Set[str] = set()

        def _bind(target_path: str) -> None:
            nonlocal duplicate_target_bindings
            if target_path in bound_target_paths:
                duplicate_target_bindings += 1
            else:
                bound_target_paths.add(target_path)

        for op in self.transform.operations:
            if isinstance(op, DirectTransformOperation):
                if op.source_field not in source_field_names:
                    unknown_source_refs += 1
                if op.target_path in target_leaves:
                    _bind(op.target_path)
                else:
                    unknown_target_refs += 1
            elif isinstance(op, MapFunctionTransformOperation):
                for inp in op.inputs:
                    if inp not in source_field_names:
                        unknown_source_refs += 1
                if op.target_path in target_leaves:
                    _bind(op.target_path)
                else:
                    unknown_target_refs += 1
            elif isinstance(op, MapScriptTransformOperation):
                for inp in op.inputs:
                    if inp not in source_field_names:
                        unknown_source_refs += 1
                for out in op.outputs:
                    if out in target_leaves:
                        _bind(out)
                    else:
                        unknown_target_refs += 1

        # Issue #43 review r2 P2: every required simple leaf in the JSON
        # payload profile must be the destination of at least one transform
        # output, otherwise downstream profile/map builders (#26) could emit a
        # payload that omits a required field. The offending paths are
        # intentionally NOT echoed in the error message — same defense-in-depth
        # policy as the duplicate_target_bindings branch, since profile node
        # names can carry caller-specific identifiers.
        required_target_paths = _required_simple_leaf_paths(self.target.payload_profile)
        unmapped_required_count = len(required_target_paths - bound_target_paths)

        # Issue #100 G2: validate per-document REST path replacements. Each
        # {name} token must appear literally in the send path, names must be
        # unique, and every target_path must be a declared simple leaf that a
        # transform output binds (so the Set Properties step can source a mapped
        # value at runtime). Offending names/paths are NOT echoed — same
        # defense-in-depth policy as the branches above (counts only).
        send_request = self.target.send_request
        replacement_names: List[str] = [r.name for r in send_request.path_replacements]
        duplicate_replacement_names = len(replacement_names) - len(set(replacement_names))
        missing_token_count = 0
        replacement_unknown_target_refs = 0
        replacement_unbound_target_refs = 0
        for replacement in send_request.path_replacements:
            if "{" + replacement.name + "}" not in send_request.path:
                missing_token_count += 1
            if replacement.target_path not in target_leaves:
                replacement_unknown_target_refs += 1
            elif replacement.target_path not in bound_target_paths:
                replacement_unbound_target_refs += 1
        # Reverse check: when path_replacements is in use, every brace in the
        # path must belong to a declared '{name}' replacement. Strip each
        # declared token, then reject any residual '{' or '}'. This catches an
        # undeclared token (e.g. '{region}' in '/clients/{clientId}/{region}'),
        # an empty '{}' (issue #127 B3 — the old non-empty-token regex
        # '{([^{}]+)}' skipped it), and an unbalanced brace ('/v1/{clientId') —
        # all of which would otherwise survive into the emitted path as literal
        # braces and break the request at runtime. Only enforced when the caller
        # opts into dynamic paths so an empty path_replacements leaves a static
        # path with literal braces (if any) byte-for-byte unchanged.
        residual_brace_issue = False
        if send_request.path_replacements:
            residual = send_request.path
            for name in replacement_names:
                residual = residual.replace("{" + name + "}", "")
            residual_brace_issue = "{" in residual or "}" in residual

        issues: List[str] = []
        if unknown_source_refs:
            issues.append(
                f"transform.operations contain {unknown_source_refs} "
                "reference(s) to a source field name not declared in "
                "source.read_operation.result_schema.fields"
            )
        if unknown_target_refs:
            issues.append(
                f"transform.operations contain {unknown_target_refs} "
                "reference(s) to a target path that is not a declared simple "
                "leaf in target.payload_profile"
            )
        if duplicate_target_bindings:
            issues.append(
                f"transform.operations bind {duplicate_target_bindings} "
                "target leaf path(s) more than once; every leaf may be the "
                "destination of at most one direct/map_function/map_script "
                "output"
            )
        if unmapped_required_count:
            issues.append(
                f"transform.operations leave {unmapped_required_count} "
                "required target leaf path(s) unmapped; every required "
                "simple leaf in target.payload_profile must be the "
                "destination of at least one direct/map_function/map_script "
                "output"
            )
        if duplicate_replacement_names:
            issues.append(
                f"target.send_request.path_replacements declare "
                f"{duplicate_replacement_names} duplicate name(s); each "
                "replacement name must be unique"
            )
        if missing_token_count:
            issues.append(
                f"target.send_request.path_replacements declare "
                f"{missing_token_count} name(s) that do not appear as a "
                "'{name}' token in target.send_request.path"
            )
        if replacement_unknown_target_refs:
            issues.append(
                f"target.send_request.path_replacements reference "
                f"{replacement_unknown_target_refs} target_path(s) that are "
                "not a declared simple leaf in target.payload_profile"
            )
        if replacement_unbound_target_refs:
            issues.append(
                f"target.send_request.path_replacements reference "
                f"{replacement_unbound_target_refs} target_path(s) that no "
                "transform output binds; a dynamic path segment can only "
                "source a mapped leaf"
            )
        if residual_brace_issue:
            issues.append(
                "target.send_request.path contains an unresolved '{'/'}' brace "
                "after removing declared path_replacements token(s); every "
                "brace must belong to a declared '{name}' replacement (no "
                "undeclared '{token}', empty '{}', or unbalanced brace) so no "
                "unresolved placeholder reaches the emitted path"
            )

        if issues:
            raise ValueError(" | ".join(issues))

        if self.execution.watermark is not None:
            if self.execution.watermark.field not in source_field_names:
                raise ValueError(
                    "execution.watermark.field must reference a name declared "
                    "in source.read_operation.result_schema.fields"
                )

        return self


# ---------------------------------------------------------------------------
# Archetype
# ---------------------------------------------------------------------------


# Example payload sentinels — these intentionally do NOT look like real SQL,
# OData filters, SOAP envelopes, REST payloads, field mappings, or scripts.
# They exist only to demonstrate the parameter shape.
_EXAMPLE_SQL_SENTINEL = "<<user-authored DB read statement>>"


def _build_db_extract_params(
    parameters: "DatabaseToApiSyncParameters", overrides: Dict[str, str]
) -> DbExtractParameters:
    source = parameters.source
    binding = source.binding
    read = source.read_operation

    if binding.mode == "create":
        settings = binding.settings  # guaranteed present by the contract validator
        connection: Dict[str, Any] = {
            "mode": "create",
            # driver maps 1:1 onto DatabaseConnectorBuilder.SUPPORTED_DRIVER_IDS
            # ('microsoft_jdbc' is a recognized alias of 'sqlserver').
            "driver_id": settings.driver,
            # 'windows_integrated' is rejected by DatabaseConnectorBuilder
            # (UNSUPPORTED_DB_AUTH_MODE) — passed through so the builder, not the
            # archetype, owns the auth-mode vocabulary.
            "auth_mode": settings.auth_mode,
            "host": settings.host,
            "port": settings.port,
            "dbname": settings.database,
        }
        if settings.username is not None:
            connection["username"] = settings.username
        if settings.credential_ref is not None:
            connection["credential_ref"] = settings.credential_ref
        # jdbc_options (Dict[str,str]) is metadata-deferred (see
        # _deferred_intent): the contract carries no verbatim JDBC suffix, and
        # synthesizing one would violate the no-template rule.
    else:
        connection = {"mode": "reuse"}
        if binding.component_id:
            connection["component_id"] = binding.component_id
        if binding.component_name:
            connection["component_name"] = binding.component_name

    output_fields = [
        {"name": f.name, "data_type": f.data_type, "mandatory": f.required}
        for f in read.result_schema.fields
    ]
    read_profile: Dict[str, Any] = {"query": read.sql, "output_fields": output_fields}
    if read.parameters:
        # The Select read profile takes name + mappability; sql_type/direction
        # are not builder-supported and are metadata-deferred. 'in' parameters
        # are the mappable bind inputs.
        read_profile["parameters"] = [
            {"name": p.name, "mappable": (p.direction == "in")}
            for p in read.parameters
        ]

    operation: Dict[str, Any] = {}
    if read.batch_size is not None:
        operation["batch_count"] = read.batch_size
    if read.max_rows is not None:
        operation["max_rows"] = read.max_rows
    # fetch_size / link_element have no DB Get operation builder field — deferred.

    # Overrides are keyed by the documented component role (e.g. 'db_connection');
    # the prefixed emitted key ('source_db_connection') is accepted as a fallback.
    component_names: Dict[str, str] = {}
    conn_name = _named(
        overrides, ROLE_DB_CONNECTION, primitive_component_key(_SOURCE_PREFIX, ROLE_DB_CONNECTION)
    )
    if conn_name:
        component_names["connection"] = conn_name
    read_name = _named(
        overrides, ROLE_DB_READ_PROFILE, primitive_component_key(_SOURCE_PREFIX, ROLE_DB_READ_PROFILE)
    )
    if read_name:
        component_names["read_profile"] = read_name
    op_name = _named(
        overrides, ROLE_DB_GET_OPERATION, primitive_component_key(_SOURCE_PREFIX, ROLE_DB_GET_OPERATION)
    )
    if op_name:
        component_names["get_operation"] = op_name

    return _coerce_primitive_params(
        DbExtractParameters,
        {
            "key_prefix": _SOURCE_PREFIX,
            "connection": connection,
            "read_profile": read_profile,
            "operation": operation,
            "component_names": component_names,
        },
        field="source",
    )


def _build_field_map_params(
    parameters: "DatabaseToApiSyncParameters", overrides: Dict[str, str]
) -> FieldMapParameters:
    read = parameters.source.read_operation
    transform = parameters.transform

    # The DB Select result fields are the source leaf index; their logical path
    # is the field name (a flat result set), matching the transform.map
    # source_path the direct/function/script routes emit.
    source_field_index = {
        f.name: {"data_type": f.data_type, "mappable": True}
        for f in read.result_schema.fields
    }
    source_profile_key = primitive_component_key(_SOURCE_PREFIX, ROLE_DB_READ_PROFILE)

    direct: List[Dict[str, Any]] = []
    map_function: List[Dict[str, Any]] = []
    map_script: List[Dict[str, Any]] = []
    for op in transform.operations:
        if isinstance(op, DirectTransformOperation):
            direct.append({"source_field": op.source_field, "target_path": op.target_path})
        elif isinstance(op, MapFunctionTransformOperation):
            entry: Dict[str, Any] = {
                "function_type": op.function_type,
                "inputs": list(op.inputs),
                "target_path": op.target_path,
            }
            if op.parameters:
                entry["parameters"] = dict(op.parameters)
            map_function.append(entry)
        elif isinstance(op, MapScriptTransformOperation):
            # script_component_ref points at a script component the archetype
            # cannot emit into this spec, so it would plan with a dangling
            # dependency (build_integration would reject it). M2 materializes a
            # map script only from an inline script_body (which field_map emits
            # as an in-spec script.mapping). Reject the ref with a clear error
            # instead of producing a non-plannable "executable" spec.
            if op.script_component_ref is not None:
                raise BuilderValidationError(
                    "map_script.script_component_ref is not supported by this "
                    "archetype — the referenced script component is not part of "
                    "the emitted spec, so the plan cannot resolve it.",
                    error_code=UNSUPPORTED_SCRIPT_COMPONENT_REF,
                    field="transform.operations.script_component_ref",
                    hint=(
                        "Provide the script inline via map_script.script_body so "
                        "the archetype materializes the script.mapping component "
                        "in the spec. External script-component reuse is deferred "
                        "to #51."
                    ),
                )
            # The contract's inputs are source field names and outputs are
            # target leaf paths; field_map's MapScriptOp needs named ports, so
            # derive input_name from the field name and output_name from the
            # leaf segment. field_map enforces that script_body is present.
            script_entry: Dict[str, Any] = {
                "inputs": [{"source_path": name, "input_name": name} for name in op.inputs],
                "outputs": [
                    {"output_name": path.rsplit("/", 1)[-1], "target_path": path}
                    for path in op.outputs
                ],
                "language": op.language,
            }
            if op.script_body is not None:
                script_entry["script_body"] = op.script_body
            map_script.append(script_entry)

    # Role-keyed overrides (e.g. 'target_profile', 'transform_map', 'script'),
    # with the prefixed emitted key accepted as a fallback.
    component_names: Dict[str, str] = {}
    target_profile_name = _named(
        overrides, ROLE_TARGET_PROFILE, primitive_component_key(_TRANSFORM_PREFIX, ROLE_TARGET_PROFILE)
    )
    if target_profile_name:
        component_names["target_profile"] = target_profile_name
    map_name = _named(
        overrides, ROLE_TRANSFORM_MAP, primitive_component_key(_TRANSFORM_PREFIX, ROLE_TRANSFORM_MAP)
    )
    if map_name:
        component_names["transform_map"] = map_name
    script_prefix = _named(overrides, ROLE_SCRIPT, f"{_TRANSFORM_PREFIX}_{ROLE_SCRIPT}")
    if script_prefix:
        component_names["script_prefix"] = script_prefix

    return _coerce_primitive_params(
        FieldMapParameters,
        {
            "key_prefix": _TRANSFORM_PREFIX,
            "source": {
                "source_profile_id": f"$ref:{source_profile_key}",
                "source_profile_type": "profile.db",
                "source_field_index": source_field_index,
            },
            "target_payload_profile": parameters.target.payload_profile.model_dump(),
            "direct": direct,
            "map_function": map_function,
            "map_script": map_script,
            "component_names": component_names,
        },
        field="transform",
    )


def _build_rest_send_params(
    parameters: "DatabaseToApiSyncParameters", overrides: Dict[str, str]
) -> RestSendWithRetryParameters:
    target = parameters.target
    binding = target.binding
    send = target.send_request

    if binding.mode == "create":
        settings = binding.settings  # guaranteed present by the contract validator
        auth = _REST_CREATE_AUTH_MAP.get(settings.auth_mode)
        if auth is None:
            raise BuilderValidationError(
                "REST create-mode auth is not supported for executable "
                "assembly in M2 (only an unauthenticated connection can be "
                "created); use an existing connection instead.",
                error_code=UNSUPPORTED_REST_AUTH_MODE,
                field="target.binding.settings.auth_mode",
                hint=(
                    "Set target.binding.mode='reuse' with an existing REST "
                    "Client connection (component_id or component_name) for "
                    "secured auth, or wait for a verified connector-auth "
                    "extension (#51). The archetype never echoes credentials."
                ),
            )
        connection: Dict[str, Any] = {
            "mode": "create",
            "base_url": settings.base_url,
            "auth": auth,
        }
        # default_headers has no RestConnectionCreate field — metadata-deferred.
    else:
        connection = {"mode": "reuse"}
        if binding.component_id:
            connection["component_id"] = binding.component_id
        if binding.component_name:
            connection["component_name"] = binding.component_name

    target_profile_key = primitive_component_key(_TRANSFORM_PREFIX, ROLE_TARGET_PROFILE)
    # Issue #100 G2: with per-document path replacements the REST Client operation
    # declares NO in-operation path (the live REST Execute export carries a blank
    # path field); the full per-document path is supplied at the process connector
    # step's "Path" dynamic operation property. Pass the template path + flag
    # path_replacements; the rest_send primitive blanks the operation path when
    # replacements are present and the operation builder permits the blank. Without
    # replacements the static path is emitted verbatim as before.
    operation: Dict[str, Any] = {
        "method": send.method,
        "path": send.path,
        # Bind the operation request body to the generated JSON payload profile.
        "request_profile_id": f"$ref:{target_profile_key}",
        "request_profile_type": "json",
    }
    if send.path_replacements:
        operation["path_replacements"] = [
            {"name": r.name, "target_path": r.target_path}
            for r in send.path_replacements
        ]
    # Only literal query parameters are emitted onto the operation. Watermark-
    # sourced parameters need dynamic operation-property wiring (#51) and are
    # represented as operational intent, never as static query parameters.
    literal_qp = {
        qp.name: qp.literal_value
        for qp in send.query_parameters
        if qp.value_source == "literal" and qp.literal_value is not None
    }
    if literal_qp:
        operation["query_parameters"] = literal_qp

    # Role-keyed overrides (e.g. 'rest_connection', 'rest_operation'), with the
    # prefixed emitted key accepted as a fallback.
    component_names: Dict[str, str] = {}
    conn_name = _named(
        overrides, ROLE_REST_CONNECTION, primitive_component_key(_TARGET_PREFIX, ROLE_REST_CONNECTION)
    )
    if conn_name:
        component_names["connection"] = conn_name
    op_name = _named(
        overrides, ROLE_REST_OPERATION, primitive_component_key(_TARGET_PREFIX, ROLE_REST_OPERATION)
    )
    if op_name:
        component_names["operation"] = op_name

    return _coerce_primitive_params(
        RestSendWithRetryParameters,
        {
            "key_prefix": _TARGET_PREFIX,
            "connection": connection,
            "operation": operation,
            "component_names": component_names,
        },
        field="target",
    )


def _ref_dep_key(binding: Optional[str]) -> Optional[str]:
    """If a DLQ binding is a ``$ref:KEY`` token, return KEY (to add to the
    process ``depends_on`` so the builder's $ref-reachability walk passes);
    otherwise None (a literal component id needs no dependency edge)."""
    if isinstance(binding, str) and binding.startswith("$ref:"):
        key = binding[len("$ref:"):].strip()
        return key or None
    return None


def _derive_process_reliability(
    reliability: "ReliabilityConfig",
) -> Tuple[Dict[str, Any], Optional[str]]:
    """Map the caller's reliability config to the emitted process
    ``reliability`` block plus an optional ``$ref`` dependency key for the DLQ
    catch-leg binding.

    The emitted process ``retry_count`` is ``max_attempts - 1`` (0..5), wired
    when a Try/Catch catch path (a wired DLQ mode) is present (issue #88
    M4.5.3, building on #51 M3.R1a). The ReliabilityConfig validator guarantees
    ``max_attempts == 1`` (retry_count 0) on the disabled / guidance_only
    branches, so those still emit no retry and no Try/Catch.
    """
    retry_count = reliability.retry.max_attempts - 1
    dlq = reliability.dlq
    if not dlq.enabled or dlq.target is None:
        return {"retry_count": 0, "dlq": {"mode": "disabled"}}, None

    # Issue #89: the optional Notify rides the wired catch leg only. The
    # ReliabilityConfig validator guarantees catch_notify is absent on the
    # disabled / guidance_only branches, so it is only attached here.
    notify = reliability.catch_notify
    notify_block = (
        {"level": notify.level, "message_template": notify.message_template}
        if notify is not None
        else None
    )

    # Issue #99 G1: the wired DB->API sync path emits a connector-scoped
    # Try/Catch (a Try/Catch per connector — source retry 0, target retry N)
    # rather than one process-level Try/Catch spanning the whole chain. #91
    # Scenario 2 proved the old process scope re-executes the DB read on every
    # REST retry; connector scope isolates the retried send so the upstream read
    # runs once. ProcessFlowBuilder defaults try_catch_scope to "process" for
    # direct process authoring (back-compat); the archetype opts the sync into
    # the connector scope explicitly.
    target = dlq.target
    if target.mode == "document_cache_ref":
        block: Dict[str, Any] = {
            "retry_count": retry_count,
            "try_catch_scope": "connector",
            "dlq": {
                "mode": "document_cache_ref",
                "document_cache_id": target.document_cache_id,
            },
        }
        if notify_block is not None:
            block["catch_notify"] = notify_block
        return block, _ref_dep_key(target.document_cache_id)
    if target.mode == "error_subprocess_ref":
        block = {
            "retry_count": retry_count,
            "try_catch_scope": "connector",
            "dlq": {"mode": "error_subprocess_ref", "process_id": target.process_id},
        }
        if notify_block is not None:
            block["catch_notify"] = notify_block
        return block, _ref_dep_key(target.process_id)

    # guidance_only — recorded as intent in operational_intent; no builder
    # wiring. The validator guarantees max_attempts == 1 here (retry_count 0).
    return {"retry_count": 0, "dlq": {"mode": "disabled"}}, None


def _path_ddp_name(segments: List[Dict[str, Any]]) -> str:
    """Derive a per-endpoint Dynamic Document Property name for the REST path.

    Issue #100 G2: a Dynamic DOCUMENT Property (travels per-document) carries the
    per-document path so a multi-record run never overwrites it across documents
    (a Dynamic Process Property is execution-global and would clobber — Codex
    review P1). One endpoint -> one DDP name (e.g. ``DDP_PATH_CLIENTS``) so a
    process sending to multiple endpoints stays unambiguous. The resource is the
    last static path segment before the first dynamic token
    (``/admin/cdscm/api/v1/clients/{clientId}`` -> ``clients``).
    """
    resource = "PATH"
    for seg in segments:
        if seg["type"] != "static":
            break
        parts = [p for p in seg["value"].split("/") if p and "{" not in p]
        if parts:
            resource = parts[-1]
    safe = re.sub(r"[^A-Za-z0-9]+", "_", resource).strip("_").upper() or "PATH"
    return f"DDP_PATH_{safe}"


def _build_dynamic_path(
    parameters: "DatabaseToApiSyncParameters",
) -> Optional[Dict[str, Any]]:
    """Build the process-target ``dynamic_path`` block for REST path replacements.

    Issue #100 G2. Returns None when no ``path_replacements`` are configured (the
    path stays static — byte-for-byte the pre-#100 behavior). Otherwise returns
    the metadata ``ProcessFlowBuilder`` lowers into a Set Properties
    (``documentproperties``) shape that concatenates the path, plus the connector
    step's "Path" dynamic operation property sourcing the resulting Dynamic
    Document Property (per-document — safe for multi-record runs). The
    profile-element references (``element_id`` /
    ``element_name``) are read from the SAME ``JSONGeneratedProfileBuilder`` field
    index the transform map consumes, so the emitted ``<profileelement>`` matches
    the generated request profile by construction. Grounded in the live REST
    Client capture (see ``.codex/plans/issue-100-live-captures.md``).
    """
    send = parameters.target.send_request
    if not send.path_replacements:
        return None

    target_profile_key = primitive_component_key(_TRANSFORM_PREFIX, ROLE_TARGET_PROFILE)
    # Compute the target field index exactly as field_map does, so element keys /
    # name paths match the emitted profile component byte-for-byte.
    profile_config = {
        "profile_type": "json.generated",
        "component_name": target_profile_key,
        "root": parameters.target.payload_profile.model_dump().get("root"),
    }
    target_index = JSONGeneratedProfileBuilder.build_field_index(profile_config)

    by_name = {r.name: r for r in send.path_replacements}
    # Defense-in-depth backstop (issue #127 B3): the contract validator already
    # rejects any residual brace, but re-check here before segment construction
    # in case _build_dynamic_path is reached with a path that bypassed Pydantic
    # validation (e.g. a params object mutated after validation). Strip every
    # declared '{name}' token — names are guaranteed brace-free by the contract
    # validator — then any remaining '{'/'}' is malformed (empty '{}', undeclared
    # '{token}', or unbalanced brace) and must not be emitted as a static segment.
    residual_path = send.path
    for name in by_name:
        residual_path = residual_path.replace("{" + name + "}", "")
    if "{" in residual_path or "}" in residual_path:
        raise BuilderValidationError(
            "target.send_request.path contains an unresolved '{'/'}' brace "
            "after removing declared path_replacements token(s)",
            error_code="ARCHETYPE_PARAM_INVALID",
            field="target.send_request.path",
        )
    token_re = re.compile(r"\{([^{}]+)\}")
    segments: List[Dict[str, Any]] = []
    last = 0
    for match in token_re.finditer(send.path):
        replacement = by_name.get(match.group(1))
        if replacement is None:
            # Defense-in-depth backstop (issue #127 B3): the contract validator
            # already rejects any brace that is not a declared replacement, so
            # an unmatched token reaching here means an upstream invariant was
            # violated. Fail hard rather than silently emitting literal braces
            # into the request path.
            raise BuilderValidationError(
                "target.send_request.path contains a '{...}' token with no "
                "matching path_replacements entry",
                error_code="ARCHETYPE_PARAM_INVALID",
                field="target.send_request.path",
            )
        if match.start() > last:
            segments.append({"type": "static", "value": send.path[last:match.start()]})
        entry = target_index[replacement.target_path]
        name_path = entry["name_path"]
        leaf = name_path.split("/")[-1]
        segments.append(
            {
                "type": "profile",
                "element_id": str(entry["key"]),
                "element_name": f"{leaf} ({name_path})",
            }
        )
        last = match.end()
    if last < len(send.path):
        segments.append({"type": "static", "value": send.path[last:]})

    return {
        "ddp_name": _path_ddp_name(segments),
        "request_profile_id": f"$ref:{target_profile_key}",
        "profile_type": "profile.json",
        "segments": segments,
    }


def _build_sync_pipeline_adapter_config(
    parameters: "DatabaseToApiSyncParameters",
    *,
    db_conn_key: str,
    db_op_key: str,
    map_key: str,
    rest_conn_key: str,
    rest_op_key: str,
) -> Dict[str, Any]:
    """Build the M5.3 (#71) semantic ``sync_pipeline`` stage graph whose lowering
    reproduces the legacy ``database_to_api_sync`` linear core.

    The graph is the verified-linear ``read(db_read) -> map -> send(rest_send)``
    chain :class:`SyncPipelineBuilder` accepts. It is fed to
    ``SyncPipelineBuilder.lower_config`` to derive ``source``/``transform``/
    ``target`` from the pipeline foundation rather than hand-assembling them.

    Internal only: this config is NEVER populated onto ``IntegrationSpecV1.pipeline``
    (the returned spec keeps ``pipeline=None``) and the emitted process keeps its
    public ``process_kind="database_to_api_sync"``. Legacy-only blocks (reliability,
    dynamic_path, folder_name, process_extensions) are deliberately omitted here —
    ``lower_config`` does not carry them; the caller reattaches them post-lowering.
    """
    send = parameters.target.send_request
    return {
        "process_kind": "sync_pipeline",
        "pipeline": {
            "stages": [
                {
                    "key": "source",
                    "kind": "read",
                    "config": {
                        "primitive": "db_read",
                        "connection_id": f"$ref:{db_conn_key}",
                        "operation_id": f"$ref:{db_op_key}",
                    },
                },
                {
                    "key": "transform",
                    "kind": "map",
                    "config": {
                        "primitive": "map",
                        "map_ref": f"$ref:{map_key}",
                    },
                },
                {
                    "key": "target",
                    "kind": "send",
                    "config": {
                        "primitive": "rest_send",
                        "action_type": send.method,
                        "connection_id": f"$ref:{rest_conn_key}",
                        "operation_id": f"$ref:{rest_op_key}",
                    },
                },
            ],
            "dependencies": [
                {"from_stage": "source", "to_stage": "transform"},
                {"from_stage": "transform", "to_stage": "target"},
            ],
        },
    }


def _build_main_process(
    parameters: "DatabaseToApiSyncParameters", overrides: Dict[str, str]
) -> IntegrationComponentSpec:
    naming = parameters.naming
    reliability_block, dlq_dep_key = _derive_process_reliability(parameters.reliability)

    db_conn_key = primitive_component_key(_SOURCE_PREFIX, ROLE_DB_CONNECTION)
    db_op_key = primitive_component_key(_SOURCE_PREFIX, ROLE_DB_GET_OPERATION)
    map_key = primitive_component_key(_TRANSFORM_PREFIX, ROLE_TRANSFORM_MAP)
    rest_conn_key = primitive_component_key(_TARGET_PREFIX, ROLE_REST_CONNECTION)
    rest_op_key = primitive_component_key(_TARGET_PREFIX, ROLE_REST_OPERATION)

    process_name = (
        _named(overrides, "process", _MAIN_PROCESS_KEY)
        or f"{naming.component_prefix} DB to API Sync"
    )

    # Issue #71 (M5.3): derive the linear source/transform/target core through the
    # verified-linear sync_pipeline foundation instead of hand-assembling it. The
    # lowered config is byte-equivalent to the pre-M5.3 hand-built core (process_kind
    # stays "database_to_api_sync"); the semantic pipeline is internal-only and is
    # never populated onto the returned IntegrationSpecV1 (pipeline stays None).
    config: Dict[str, Any] = SyncPipelineBuilder.lower_config(
        _build_sync_pipeline_adapter_config(
            parameters,
            db_conn_key=db_conn_key,
            db_op_key=db_op_key,
            map_key=map_key,
            rest_conn_key=rest_conn_key,
            rest_op_key=rest_op_key,
        )
    )
    # Reattach the legacy-only reliability block lower_config does not carry.
    # Reliability derived from the caller's policy: a verified DLQ mode
    # (document_cache_ref / error_subprocess_ref) emits the live Try/Catch +
    # DLQ catch path via ProcessFlowBuilder, with retry_count = max_attempts
    # - 1 (0..5, platform-timed) wired through (#51 M3.R1a / #88 M4.5.3).
    # disabled / guidance_only emit no Try/Catch (retry stays 0).
    config["reliability"] = reliability_block
    # Issue #100 G2: per-document dynamic REST path. When path_replacements are
    # configured, emit a dynamic_path block the ProcessFlowBuilder lowers into a
    # Set Properties shape + the connector step's "Path" dynamic operation
    # property. Absent replacements, no dynamic_path key is added and the process
    # XML stays byte-for-byte identical to the static-path output.
    dynamic_path = _build_dynamic_path(parameters)
    if dynamic_path is not None:
        config["target"]["dynamic_path"] = dynamic_path
    if naming.folder_path:
        config["folder_name"] = naming.folder_path

    # Issue #92 M4.5.7 (+ #102 B1): declare the source DB connection fields as
    # per-environment override points so the deployed process exposes them
    # through manage_environments(get_extensions) — TEST -> PROD promotion can
    # then supply the credential per environment instead of embedding it in the
    # connection component. The declaration reuses the SAME $ref:db_conn_key the
    # source connector shape binds to, so apply-time substitution resolves both
    # to the one connection component (db_conn_key is already in depends_on, so
    # no new dependency edge is needed). ProcessFlowBuilder leaves processOverrides
    # UNOWNED so UI-populated per-environment override VALUES survive structured
    # updates.
    ext_connections: List[Dict[str, Any]] = []
    source_binding = parameters.source.binding
    # Declare the DB SOURCE connection fields as per-environment override points
    # for create-mode username_password sources AND for reuse-mode (an existing
    # connection still benefits from per-environment credential overrides — the
    # #102 B1 reuse-mode requirement; Codex review). windows_integrated create
    # has no archetype-owned credential to externalize.
    db_externalize = source_binding.mode == "reuse" or (
        source_binding.mode == "create"
        and source_binding.settings is not None
        and source_binding.settings.auth_mode == "username_password"
    )
    if db_externalize:
        extension_fields = db_connection_extension_fields(
            credentials=parameters.environment_extensions.credential_connection_fields,
            endpoint=parameters.environment_extensions.endpoint_connection_fields,
        )
        if extension_fields:
            ext_connections.append(
                {
                    "connection_id": f"$ref:{db_conn_key}",
                    "connector_type": "database",
                    "fields": extension_fields,
                }
            )

    # Issue #102 B1: broaden env-extension emission to the REST TARGET connection.
    # Create-mode authed REST is rejected upstream (UNSUPPORTED_REST_AUTH_MODE —
    # an executable create-mode REST connection is always UNAUTHENTICATED), so an
    # archetype-owned REST CREDENTIAL only exists for a reuse-mode (existing,
    # already-authenticated) connection; a create-mode REST target has only its
    # endpoint (Base URL) to externalize. A REST Client override keys purely by
    # field id with NO xpath — live_verified from the `Rest Example` process
    # export (ConnectionOverride 5a2c4949-...). The $ref:rest_conn_key resolves to
    # the same connection the target connector shape binds to (already in
    # depends_on), in both create and reuse modes.
    target_binding = parameters.target.binding
    rest_is_reuse = target_binding.mode == "reuse"
    rest_fields = rest_connection_extension_fields(
        credentials=(
            rest_is_reuse
            and parameters.environment_extensions.rest_credential_connection_fields
        ),
        endpoint=parameters.environment_extensions.rest_endpoint_connection_fields,
    )
    if rest_fields:
        ext_connections.append(
            {
                "connection_id": f"$ref:{rest_conn_key}",
                "connector_type": "rest",
                "fields": rest_fields,
            }
        )

    if ext_connections:
        config["process_extensions"] = {"connections": ext_connections}

    # depends_on must contain exactly the keys referenced by $ref tokens in the
    # process config (ProcessFlowBuilder enforces this). The read profile and
    # target profile are depended transitively by the operation/map components.
    # A $ref DLQ binding adds one more edge so the catch-leg target is reachable.
    depends_on = [db_conn_key, db_op_key, map_key, rest_conn_key, rest_op_key]
    if dlq_dep_key is not None and dlq_dep_key not in depends_on:
        depends_on.append(dlq_dep_key)
    # Issue #100 G2: the dynamic_path block carries a $ref to the generated
    # request profile (resolved to the profile uuid for the <profileelement>
    # source); declare the edge so reachability validation passes.
    if dynamic_path is not None:
        target_profile_key = primitive_component_key(_TRANSFORM_PREFIX, ROLE_TARGET_PROFILE)
        if target_profile_key not in depends_on:
            depends_on.append(target_profile_key)

    return IntegrationComponentSpec(
        key=_MAIN_PROCESS_KEY,
        type="process",
        action="create",
        name=process_name,
        config=config,
        depends_on=depends_on,
    )


def _default_dpp_name(field: str) -> str:
    """Deterministic default Dynamic Process Property name for a DPP watermark.

    The #29 contract carries no caller dpp_name, but #51 needs a stable property
    name to wire the watermark. Derived from the tracked field (sanitized to an
    identifier-safe token); marked ``dpp_name_generated`` in the metadata so a
    follow-up can honor or override it.
    """
    safe = re.sub(r"[^A-Za-z0-9]+", "_", field).strip("_") or "field"
    return f"watermark_{safe}"


def _watermark_intent(
    watermark: "Optional[Watermark]", context: PrimitiveBuildContext
) -> Dict[str, Any]:
    """Watermark strategy as metadata only (no executable update wiring in M2)."""
    if watermark is None:
        fragment = WatermarkStatePrimitive.emit_fragment(
            context,
            _coerce_primitive_params(
                WatermarkStateParameters, {"enabled": False}, field="execution.watermark"
            ),
        )
        return fragment["metadata"]["watermark"]

    if watermark.persistence == "dpp":
        params = _coerce_primitive_params(
            WatermarkStateParameters,
            {
                "enabled": True,
                "field": watermark.field,
                "kind": watermark.kind,
                "persistence": "dpp",
                # The contract has no dpp_name; supply a deterministic default so
                # #51 has a property name to wire (flagged generated below).
                "dpp_name": _default_dpp_name(watermark.field),
                "initial_value": watermark.initial_value,
            },
            field="execution.watermark",
        )
        fragment = WatermarkStatePrimitive.emit_fragment(context, params)
        intent = fragment["metadata"]["watermark"]
        intent["dpp_name_generated"] = True
        return intent

    # external_store: the contract carries no store_ref, so the primitive cannot
    # validate it. Represent the intent as metadata; store wiring is deferred.
    intent: Dict[str, Any] = {
        "enabled": True,
        "field": watermark.field,
        "kind": watermark.kind,
        "persistence": "external_store",
        "deferred_to": "#51",
        "note": "external-store watermark wiring (store reference) is deferred",
    }
    if watermark.initial_value is not None:
        intent["initial_value"] = watermark.initial_value
    return intent


def _deferred_intent(parameters: "DatabaseToApiSyncParameters") -> Dict[str, Any]:
    """Caller intent the current builders cannot emit — recorded, not dropped.

    Records only counts + notes (never the caller-authored keys/values) so the
    metadata can never echo a header value or JDBC option that might be
    sensitive.
    """
    deferred: Dict[str, Any] = {}
    sbind = parameters.source.binding
    if sbind.mode == "create" and sbind.settings and sbind.settings.jdbc_options:
        deferred["jdbc_options"] = {
            "count": len(sbind.settings.jdbc_options),
            "note": (
                "JDBC option map is not serialized onto the created connection "
                "in M2; use binding.mode='reuse' for connections needing JDBC "
                "URL options."
            ),
        }
    read = parameters.source.read_operation
    read_deferred: Dict[str, Any] = {}
    if read.fetch_size is not None:
        read_deferred["fetch_size"] = "metadata-only (no DB Get operation builder field in M2)"
    if read.link_element is not None:
        read_deferred["link_element"] = "metadata-only (no DB Get operation builder field in M2)"
    # The Select read profile only carries name + mappability; sql_type and a
    # non-default ('out') direction have no builder field, so the caller's
    # typing intent is preserved here rather than silently dropped. Bind
    # parameter names/types are SQL identifiers (not credentials), consistent
    # with how result-field names already surface in the contract flow summary.
    typed_parameters = [
        {"name": p.name, "sql_type": p.sql_type, "direction": p.direction}
        for p in (read.parameters or [])
        if p.sql_type is not None or p.direction != "in"
    ]
    if typed_parameters:
        read_deferred["bind_parameter_typing"] = typed_parameters
    if read_deferred:
        deferred["read_operation"] = read_deferred
    tbind = parameters.target.binding
    if tbind.mode == "create" and tbind.settings and tbind.settings.default_headers:
        deferred["default_headers"] = {
            "count": len(tbind.settings.default_headers),
            "note": "REST default headers are not emitted onto the created connection in M2.",
        }
    return deferred


def _build_operational_intent(
    parameters: "DatabaseToApiSyncParameters", context: PrimitiveBuildContext
) -> Dict[str, Any]:
    """Compose the operational primitives' fragments into intent metadata.

    Verified DLQ modes (document_cache_ref / error_subprocess_ref) ARE wired
    into the emitted process reliability (#51 M3.R1a), and caller retry
    (max_attempts → retry_count, #88 M4.5.3) is wired when a catch path is
    configured; this records the matching intent. guidance_only DLQ remains
    metadata-only.
    """
    execution = parameters.execution
    reliability = parameters.reliability
    send = parameters.target.send_request
    intent: Dict[str, Any] = {}

    # --- execution trigger (schedule) ---
    trigger = execution.trigger
    if trigger.mode == "scheduled" and trigger.schedule is not None:
        schedule_params = _coerce_primitive_params(
            ScheduleEnvelopeParameters,
            {
                "mode": "scheduled",
                "cron": trigger.schedule.cron,
                "timezone": trigger.schedule.timezone,
            },
            field="execution.trigger",
        )
    else:
        schedule_params = _coerce_primitive_params(
            ScheduleEnvelopeParameters, {"mode": "manual"}, field="execution.trigger"
        )
    schedule_fragment = ScheduleEnvelopePrimitive.emit_fragment(context, schedule_params)

    exec_intent: Dict[str, Any] = {}
    trigger_fragment = (
        schedule_fragment.get("process_config", {})
        .get("execution", {})
        .get("trigger")
    )
    if trigger_fragment:
        exec_intent["trigger"] = trigger_fragment
    schedule_meta = schedule_fragment.get("metadata", {}).get("schedule")
    if schedule_meta:
        intent["schedule"] = schedule_meta

    # --- run metadata ---
    if execution.run_metadata:
        run_fragment = RunMetadataPrimitive.emit_fragment(
            context,
            _coerce_primitive_params(
                RunMetadataParameters,
                {"static_metadata": dict(execution.run_metadata)},
                field="execution.run_metadata",
            ),
        )
        run_exec = run_fragment.get("process_config", {}).get("execution", {})
        if "run_metadata" in run_exec:
            exec_intent["run_metadata"] = run_exec["run_metadata"]
        dpp = run_fragment.get("metadata", {}).get("dynamic_process_properties")
        if dpp:
            exec_intent["dynamic_process_properties"] = dpp
    if exec_intent:
        intent["execution"] = exec_intent

    # --- watermark (metadata only) ---
    intent["watermark"] = _watermark_intent(execution.watermark, context)

    # --- reliability (error classifier + requested retry/DLQ intent) ---
    reliability_intent: Dict[str, Any] = {}
    classifier_fragment = ErrorClassifierPrimitive.emit_fragment(
        context,
        _coerce_primitive_params(
            ErrorClassifierParameters,
            {
                "retriable_status_codes": list(
                    reliability.error_classifier.retriable_status_codes
                ),
                "terminal_status_codes": list(
                    reliability.error_classifier.terminal_status_codes
                ),
                "custom_rules": list(reliability.error_classifier.custom_rules),
            },
            field="reliability.error_classifier",
        ),
    )
    classifier = (
        classifier_fragment.get("process_config", {})
        .get("reliability", {})
        .get("error_classifier")
    )
    if classifier:
        reliability_intent["error_classifier"] = classifier

    # Issue #88 (M4.5.3): retry is now wired to the emitted process when a
    # Try/Catch catch path (a wired DLQ) is present — process_retry_count =
    # max_attempts - 1. Timing is platform-controlled (backoff="platform").
    retry_intent: Dict[str, Any] = {
        "requested_max_attempts": reliability.retry.max_attempts,
        "backoff": reliability.retry.backoff,
        "process_retry_count": _derive_process_reliability(reliability)[0]["retry_count"],
    }
    reliability_intent["retry"] = retry_intent

    # Record the DLQ block actually emitted into the process (truthful — the
    # archetype now wires verified DLQ modes directly into process.reliability;
    # disabled / guidance_only emit {"mode": "disabled"}). #51 M3.R1a.
    emitted_block, _ = _derive_process_reliability(reliability)
    reliability_intent["dlq"] = emitted_block["dlq"]
    if reliability.dlq.enabled and reliability.dlq.target is not None:
        target = reliability.dlq.target
        if target.mode in ("document_cache_ref", "error_subprocess_ref"):
            binding = (
                target.document_cache_id
                if target.mode == "document_cache_ref"
                else target.process_id
            )
            # The binding is a Boomi component id / $ref token — structural, not
            # a secret — so it is safe to record (it also appears in the spec).
            reliability_intent["dlq_requested"] = {
                "requested": True,
                "status": "emitted",
                "builder_mode": target.mode,
                "binding": binding,
            }
        else:  # guidance_only
            # Never echo caller-supplied free-form values (address OR reason —
            # both can carry sensitive content) — only their presence + the
            # enum routing kind.
            reliability_intent["dlq_requested"] = {
                "requested": True,
                "status": "guidance_only",
                "kind": target.kind,
                "address_present": target.address is not None,
                "reason_present": target.reason is not None,
                "note": (
                    "folder/topic/queue is not a verified builder DLQ mode; "
                    "recorded as guidance only (no wiring)."
                ),
            }
    # Issue #89 (M4.5.4): record Notify intent without echoing the message body
    # (defense-in-depth — the template is caller free-form). references_caught_
    # error_property is always true (the CatchNotifyConfig validator enforces it).
    if reliability.catch_notify is not None:
        reliability_intent["catch_notify"] = {
            "requested": True,
            "status": "emitted",
            "level": reliability.catch_notify.level,
            "message_template_present": True,
            "references_caught_error_property": True,
        }
    intent["reliability"] = reliability_intent

    # --- expected status codes ---
    intent["expected_status_codes"] = list(send.expected_status_codes)

    # --- watermark-sourced query parameters (metadata only) ---
    # Watermark-bound query parameters are NOT emitted as static REST operation
    # query parameters; they need dynamic operation-property wiring (#51). Their
    # names are preserved here so the caller's intent is not lost between
    # build_from_archetype and the #51 follow-up.
    watermark_query_parameters = [
        {"name": qp.name, "bound_to": "watermark", "deferred_to": "#51"}
        for qp in send.query_parameters
        if qp.value_source == "watermark"
    ]
    if watermark_query_parameters:
        intent["watermark_query_parameters"] = watermark_query_parameters

    # --- deferred fields ---
    deferred = _deferred_intent(parameters)
    if deferred:
        intent["deferred"] = deferred

    return intent


class DatabaseToApiSyncArchetype(ArchetypePattern):
    metadata = PatternMetadata(
        name="database_to_api_sync",
        version="0.5.0",
        kind=PatternKind.ARCHETYPE,
        description=(
            "Archetype for replicating SQL Server records to a REST API on a "
            "manual or scheduled trigger. Validates parameters (caller-declared "
            "DB result fields, a caller-supplied JSON payload profile tree, and "
            "typed transform operations) and emits an executable "
            "IntegrationSpecV1 — DB source, JSON transform, REST target, and a "
            "structured process — suitable for build_integration(action='plan'). "
            "Every byte of XML is produced by the existing component builders; "
            "the archetype emits JSON component specs only and never calls "
            "Boomi. Emits a verified Try/Catch + DLQ catch path when DLQ is "
            "enabled with mode document_cache_ref or error_subprocess_ref, and "
            "wires caller retry (max_attempts 1..6 → platform-timed Try/Catch "
            "retry_count 0..5) through to the emitted process (#51 M3.R1a / #88 "
            "M4.5.3). Optionally emits a log-only Notify step on the catch path "
            "(reliability.catch_notify) recording the caught error (#89 M4.5.4). "
            "Schedule activation, watermark update, and dynamic "
            "operation-property wiring remain deferred (M3)."
        ),
        tags=[
            "database",
            "rest",
            "sync",
            "m2",
            "executable",
            "sql-server",
            "no-boomi-mutation",
        ],
        use_cases=[
            "replicate SQL Server table changes to a REST API",
            "scheduled incremental sync with watermark",
        ],
        not_for=[
            "bidirectional sync",
            "real-time change-data-capture",
            "deploying, scheduling, or executing the process (M3)",
        ],
    )
    parameters_model = DatabaseToApiSyncParameters

    capability_notes = [
        "Discoverable, fully-typed parameter contract for a SQL Server -> REST sync.",
        "Strict per-field validation surfaces structured PARAM_VALIDATION_FAILED errors.",
        "Caller-declared DB result schema and caller-supplied JSON profile tree are the M2 source of truth.",
        "Emits executable component specs (DB source, JSON transform, REST target, process) for build_integration(action='plan').",
        "All XML is produced by the existing component builders; the archetype emits JSON component specs only.",
        "Emits a verified Try/Catch + DLQ catch path when dlq.enabled with mode document_cache_ref or error_subprocess_ref; caller retry (max_attempts 1..6) is wired as platform-timed Try/Catch retry_count 0..5 (#51 M3.R1a / #88 M4.5.3).",
        "Optionally emits a verified Notify step at the head of the wired catch path (reliability.catch_notify) that logs the caught error to the process log; level INFO/WARNING/ERROR (#89 M4.5.4).",
        "Emits per-document dynamic REST paths (target.send_request.path_replacements) via a Set Properties shape that builds a per-endpoint Dynamic Document Property from mapped leaves, sourced by the connector step's 'Path' dynamic operation property; the document-scoped property keeps each record's path correct in a multi-record run; absent replacements the path stays static (#100 G2).",
        "Declares source DB connection credential fields (username, password) as per-environment override points by default so TEST -> PROD promotion supplies the password via environment extensions, never an embedded credential; host/port opt-in via environment_extensions.endpoint_connection_fields (#92 M4.5.7).",
        "Credentials cross the contract only as opaque credential_ref values and are never echoed in errors.",
    ]
    limitations = [
        "Emits JSON component specs only; performs no Boomi mutation and exposes no raw XML.",
        "DLQ wiring is emitted for mode document_cache_ref / error_subprocess_ref; legacy folder/topic/queue targets require mode='guidance_only' and are recorded as metadata only (no wiring).",
        "Retry timing is platform-controlled (Try/Catch built-in waits); caller-selected fixed/exponential backoff is NOT supported (backoff='platform' only). retry max_attempts>1 requires a wired DLQ catch path.",
        "Notify (reliability.catch_notify) is log-only and emitted only on a wired catch path; email/SMS notification channels and Notify outside catch paths are out of scope (#14/M4.5.5).",
        "Schedule intent is represented as metadata only; deployment and schedule activation are M3.",
        "Watermark is represented as metadata only; watermark-update and dynamic operation-property wiring are deferred (#51).",
        "REST create-mode emits only auth='none'; secured auth (basic / bearer / oauth2) requires binding.mode='reuse'.",
        "DB create-mode supports auth_mode='username_password' only; 'windows_integrated' requires reuse (#51).",
        "Environment-extension declarations cover create-mode username_password AND reuse-mode DB sources (windows_integrated create declares nothing), plus the REST target connection: reuse-mode REST credentials (username/password) by default and the REST base URL on opt-in (rest_endpoint_connection_fields). DB overrides are xpath-keyed, REST overrides are id-keyed (no xpath). The builder leaves processOverrides unowned so UI-populated per-environment override values survive structured updates. Override availability at runtime is live_QA_required (#92 M4.5.7 / #102 B1).",
        "jdbc_options and REST default_headers are metadata-deferred (no builder field in M2); use reuse for connections needing them.",
        "Does not mix map_function and map_script in one call (UNSUPPORTED_TRANSFORM_ROUTE); split into separate maps.",
        "Does not infer DB result fields from SQL, browse, metadata, or row samples; run infer_profile_fields (issue #47) separately for read-only inference from supplied metadata summaries / sample JSON / XSD / sample XML.",
        "Does not import existing integrations (issue #48 owns import / draft).",
        "operation_type='xslt' is rejected; the XSLT decision is owned by issue #42.",
        "credential_ref values are opaque end-to-end; the contract never resolves or validates secrets.",
    ]
    examples = [
        PatternExample(
            name="minimal_manual_sync",
            description=(
                "Smallest valid payload: create-mode SQL Server source with one "
                "declared result field, create-mode REST target with no auth and "
                "a one-leaf JSON payload profile, a single direct transform "
                "operation, manual trigger, DLQ disabled. Placeholder sentinels "
                "only — not a reusable template."
            ),
            parameters={
                "naming": {
                    "integration_name": "demo-db-to-api-sync",
                    "component_prefix": "DEMO",
                },
                "source": {
                    "binding": {
                        "mode": "create",
                        "settings": {
                            "driver": "microsoft_jdbc",
                            "auth_mode": "username_password",
                            "host": "db.internal",
                            "database": "AppDB",
                            "username": "svc_sync",
                            "credential_ref": "secrets/db/svc_sync",
                        },
                    },
                    "read_operation": {
                        "sql": _EXAMPLE_SQL_SENTINEL,
                        "result_schema": {
                            "fields": [
                                {
                                    "name": "source_field_a",
                                    "data_type": "character",
                                },
                            ],
                        },
                    },
                },
                "target": {
                    "binding": {
                        "mode": "create",
                        "settings": {
                            "base_url": "https://api.example.com",
                            "auth_mode": "none",
                        },
                    },
                    "send_request": {
                        "method": "POST",
                        "path": "/v1/items",
                    },
                    "payload_profile": {
                        "format": "json",
                        "root": {
                            "name": "Root",
                            "kind": "object",
                            "children": [
                                {
                                    "name": "target_a",
                                    "kind": "simple",
                                    "data_type": "character",
                                },
                            ],
                        },
                    },
                },
                "transform": {
                    "operations": [
                        {
                            "operation_type": "direct",
                            "source_field": "source_field_a",
                            "target_path": "Root/target_a",
                        },
                    ],
                },
                "execution": {
                    "trigger": {"mode": "manual"},
                },
                "reliability": {
                    "retry": {"max_attempts": 1},
                    "dlq": {"enabled": False},
                    "error_classifier": {},
                },
            },
        ),
        PatternExample(
            name="scheduled_with_watermark",
            description=(
                "Fuller payload: reuse-mode DB connection by component id with "
                "two declared result fields, reuse-mode REST target by "
                "component id (secured REST auth uses connection reuse in M2) "
                "and a nested JSON payload profile, two transform operations "
                "(one direct, one map_function), scheduled trigger, timestamp "
                "watermark, retry, DLQ enabled, and run metadata. Examples "
                "deliberately exclude map_script declarations to keep the "
                "published payload free of language tokens covered by the "
                "hygiene-marker guard."
            ),
            parameters={
                "naming": {
                    "integration_name": "demo-db-to-api-incremental",
                    "component_prefix": "DEMO-INC",
                    "folder_path": "Integrations/CRM/Sync",
                    "runtime_hints": {"atom_pool": "primary"},
                },
                "source": {
                    "binding": {
                        "mode": "reuse",
                        "component_id": "<<existing connector id>>",
                    },
                    "read_operation": {
                        "sql": _EXAMPLE_SQL_SENTINEL,
                        "result_schema": {
                            "fields": [
                                {
                                    "name": "source_a",
                                    "data_type": "character",
                                    "required": True,
                                },
                                {
                                    "name": "source_b",
                                    "data_type": "datetime",
                                },
                            ],
                        },
                        "parameters": [
                            {
                                "name": "<<bind parameter name>>",
                                "direction": "in",
                            },
                        ],
                        "batch_size": 500,
                    },
                },
                "target": {
                    "binding": {
                        "mode": "reuse",
                        "component_id": "<<existing REST connection id>>",
                    },
                    "send_request": {
                        "method": "POST",
                        "path": "/v1/customers",
                        "query_parameters": [
                            {
                                "name": "since",
                                "value_source": "watermark",
                            },
                        ],
                    },
                    "payload_profile": {
                        "format": "json",
                        "root": {
                            "name": "Root",
                            "kind": "object",
                            "children": [
                                {
                                    "name": "target_a",
                                    "kind": "simple",
                                    "data_type": "character",
                                    "required": True,
                                },
                                {
                                    "name": "target_b",
                                    "kind": "simple",
                                    "data_type": "datetime",
                                },
                            ],
                        },
                    },
                },
                "transform": {
                    "operations": [
                        {
                            "operation_type": "direct",
                            "source_field": "source_a",
                            "target_path": "Root/target_a",
                            "documentation_hint": "carry first column verbatim",
                        },
                        {
                            "operation_type": "map_function",
                            "function_type": "date_format",
                            "inputs": ["source_b"],
                            "target_path": "Root/target_b",
                            "parameters": {
                                "input_format": "<<source datetime format>>",
                                "output_format": "<<target datetime format>>",
                            },
                        },
                    ],
                },
                "execution": {
                    "trigger": {
                        "mode": "scheduled",
                        "schedule": {
                            "cron": "0 2 * * *",
                            "timezone": "UTC",
                        },
                    },
                    "watermark": {
                        "field": "source_b",
                        "kind": "timestamp",
                        "persistence": "dpp",
                    },
                    "run_metadata": {"owner": "crm-team"},
                },
                "reliability": {
                    "retry": {
                        "max_attempts": 5,
                        "backoff": "platform",
                    },
                    "dlq": {
                        "enabled": True,
                        "target": {
                            "mode": "document_cache_ref",
                            "document_cache_id": "<<dlq document cache component id>>",
                        },
                    },
                    "catch_notify": {
                        "level": "ERROR",
                        "message_template": "<<caller-authored notify message referencing meta.base.catcherrorsmessage>>",
                    },
                    "error_classifier": {
                        "custom_rules": ["rate_limit_exhausted"],
                    },
                },
            },
        ),
    ]

    @classmethod
    def emit_spec(
        cls, parameters: DatabaseToApiSyncParameters, *, recipe_version: Optional[str] = None
    ) -> IntegrationSpecV1:
        # Not migrated to the typed contribution path, so there is no
        # recipe to pin. Accepted for one uniform contract; ignored here.
        del recipe_version
        naming = parameters.naming
        source_binding = parameters.source.binding
        target_binding = parameters.target.binding
        target_send = parameters.target.send_request
        result_schema = parameters.source.read_operation.result_schema
        payload_profile = parameters.target.payload_profile
        operations = parameters.transform.operations

        # Endpoint summaries — no SQL, no payload bodies, no resolved URLs.
        db_endpoint: Dict[str, Any] = {
            "key": "db_source",
            "type": "database",
            "direction": "source",
            "binding_mode": source_binding.mode,
            "executable": False,
        }
        if source_binding.mode == "create" and source_binding.settings is not None:
            db_endpoint["driver"] = source_binding.settings.driver
            db_endpoint["auth_mode"] = source_binding.settings.auth_mode
        else:
            if source_binding.component_id:
                db_endpoint["component_id"] = source_binding.component_id
            if source_binding.component_name:
                db_endpoint["component_name"] = source_binding.component_name

        rest_endpoint: Dict[str, Any] = {
            "key": "rest_target",
            "type": "rest",
            "direction": "target",
            "binding_mode": target_binding.mode,
            "method": target_send.method,
            "executable": False,
        }
        if target_binding.mode == "create" and target_binding.settings is not None:
            rest_endpoint["auth_mode"] = target_binding.settings.auth_mode
        else:
            if target_binding.component_id:
                rest_endpoint["component_id"] = target_binding.component_id
            if target_binding.component_name:
                rest_endpoint["component_name"] = target_binding.component_name

        # Source schema summary — names + data types only, no SQL or row data.
        source_schema_summary: Dict[str, Any] = {
            "field_count": len(result_schema.fields),
            "fields": [
                {
                    "name": f.name,
                    "data_type": f.data_type,
                    "required": f.required,
                }
                for f in result_schema.fields
            ],
        }

        # Target payload-profile summary — leaf path index + data type only,
        # never a raw JSON body sample.
        target_leaves = _flatten_payload_profile_leaves(payload_profile)
        target_profile_summary: Dict[str, Any] = {
            "format": payload_profile.format,
            "root_name": payload_profile.root.name,
            "leaf_count": len(target_leaves),
            "leaves": [
                {"path": path, "data_type": data_type}
                for path, data_type in sorted(target_leaves.items())
            ],
        }

        # Transform operations summary — route + full operand structure so
        # downstream issues (#26/#40/#41) can compile the right rung directly
        # from the spec without re-reading the original archetype payload.
        # For map_script: ``script_body`` round-trips verbatim alongside
        # ``script_body_present`` so #41 wrapper-synthesis tooling can
        # materialise the matching script.mapping component from spec
        # metadata alone (Codex r3 P2 #3 — dropping the body would be
        # data-loss between build_from_archetype and downstream
        # compilation).
        operation_summaries: List[Dict[str, Any]] = []
        for op in operations:
            if isinstance(op, DirectTransformOperation):
                summary: Dict[str, Any] = {
                    "operation_type": "direct",
                    "future_builder_issue": "#26",
                    "source_field": op.source_field,
                    "target_path": op.target_path,
                }
                if op.documentation_hint is not None:
                    summary["documentation_hint"] = op.documentation_hint
                operation_summaries.append(summary)
            elif isinstance(op, MapFunctionTransformOperation):
                summary = {
                    "operation_type": "map_function",
                    "future_builder_issue": "#40",
                    "function_type": op.function_type,
                    "inputs": list(op.inputs),
                    "input_count": len(op.inputs),
                    "target_path": op.target_path,
                }
                if op.parameters is not None:
                    summary["parameters"] = dict(op.parameters)
                if op.documentation_hint is not None:
                    summary["documentation_hint"] = op.documentation_hint
                operation_summaries.append(summary)
            elif isinstance(op, MapScriptTransformOperation):
                summary = {
                    "operation_type": "map_script",
                    "future_builder_issue": "#41",
                    "script_slot": op.script_slot,
                    "language": op.language,
                    "inputs": list(op.inputs),
                    "input_count": len(op.inputs),
                    "outputs": list(op.outputs),
                    "output_count": len(op.outputs),
                    # Presence boolean for quick contract checks. The actual
                    # body is round-tripped below when supplied — downstream
                    # build_integration / #41 wrapper synthesis needs the
                    # runnable content to materialise a script.mapping
                    # component, so dropping it here would be data-loss
                    # (Codex r3 P2 finding #3).
                    "script_body_present": op.script_body is not None,
                }
                if op.script_body is not None:
                    summary["script_body"] = op.script_body
                if op.script_component_ref is not None:
                    summary["script_component_ref"] = op.script_component_ref
                if op.documentation_hint is not None:
                    summary["documentation_hint"] = op.documentation_hint
                operation_summaries.append(summary)

        # Issue #43 (M2.5b): build deterministic, builder-ready profile field
        # payloads + path indexes from the validated DB result schema and JSON
        # payload profile tree, plus normalized direct mapping metadata. By the
        # time we reach emit_spec all the structural validation has already run
        # (Pydantic + _validate_transform_refs), so these helpers are expected to
        # succeed for any payload that passed validate_parameters; they are still
        # invoked through the same code path the issue #26 emission and issue #47
        # (infer_profile_fields) layers use, so any
        # divergence between the strict contract and the generation helpers
        # surfaces immediately rather than as a downstream builder failure.
        gen_artifacts = build_profile_generation_artifacts(
            result_schema,
            payload_profile,
            direct_operations=[
                op for op in operations
                if isinstance(op, DirectTransformOperation)
            ],
        )

        # Flow summaries — labels + new schema/operation metadata.
        flows: List[Dict[str, Any]] = [
            {
                "key": "extract",
                "name": "Read from database",
                "source": "db_source",
                "target": None,
                "operation": "db_get",
                "executable": False,
            },
            {
                "key": "transform",
                "name": "Map source to JSON payload",
                "source": "extract",
                "target": None,
                "operation": "transform",
                "executable": False,
                "source_schema": source_schema_summary,
                "target_payload_profile": target_profile_summary,
                "operations": operation_summaries,
                # Issue #43 (M2.5b): generated profile field payloads + indexes
                # consumed by issue #26 / #40 / #41 builders without
                # reimplementing field indexing per builder.
                "source_profile_generation": gen_artifacts["source"],
                "target_profile_generation": gen_artifacts["target"],
                "direct_field_mappings": gen_artifacts["direct_mappings"],
            },
            {
                "key": "send",
                "name": "Send to REST target",
                "source": "transform",
                "target": "rest_target",
                "operation": "rest_send",
                "executable": False,
            },
            {
                "key": "reliability",
                "name": "Retry / DLQ",
                "source": "send",
                "target": "dlq" if parameters.reliability.dlq.enabled else None,
                "operation": "reliability",
                "executable": False,
            },
        ]
        if parameters.execution.watermark is not None:
            flows.append(
                {
                    "key": "watermark",
                    "name": "Advance watermark",
                    "source": "send",
                    "target": None,
                    "operation": "watermark",
                    "executable": False,
                }
            )

        naming_block: Dict[str, Any] = {
            "archetype": "database_to_api_sync",
            "integration_name": naming.integration_name,
            "component_prefix": naming.component_prefix,
            "component_names": naming.component_names or {},
        }
        # Issue #102 D1: pass the opt-in naming convention through so a caller can
        # activate build_integration's bracketed name-governance lint for the
        # emitted spec. Off by default (the archetype's own names are descriptive,
        # not bracketed) so it never floods the happy path with warnings.
        if naming.convention:
            naming_block["convention"] = naming.convention

        folders_block: Dict[str, Any] = (
            {"path": naming.folder_path} if naming.folder_path else {}
        )
        runtime_block: Dict[str, Any] = dict(naming.runtime_hints or {})

        # ---- Issue #29: executable component assembly --------------------
        # Compose the shipped #27 (db_extract, field_map) and #28
        # (rest_send_with_retry) primitives into the component list, then append
        # the structured process. Any BuilderValidationError raised here
        # (UNSUPPORTED_REST_AUTH_MODE, UNSUPPORTED_DB_AUTH_MODE,
        # UNSUPPORTED_TRANSFORM_ROUTE, SCRIPT_MAPPING_REF_REQUIRED, …) propagates
        # to the authoring layer, which returns a structured PatternError
        # without echoing caller parameters.
        overrides = _component_names(naming)
        context = PrimitiveBuildContext(
            integration_name=naming.integration_name,
            component_prefix=naming.component_prefix,
            folder_path=naming.folder_path,
        )

        components: List[IntegrationComponentSpec] = []
        components.extend(
            DbExtractPrimitive.emit_components(
                context, _build_db_extract_params(parameters, overrides)
            )
        )
        components.extend(
            FieldMapPrimitive.emit_components(
                context, _build_field_map_params(parameters, overrides)
            )
        )
        components.extend(
            RestSendWithRetryPrimitive.emit_components(
                context, _build_rest_send_params(parameters, overrides)
            )
        )
        components.append(_build_main_process(parameters, overrides))

        operational_intent = _build_operational_intent(parameters, context)

        return IntegrationSpecV1(
            version="1.0",
            name=naming.integration_name,
            mode="redesign",
            components=components,
            goals=[
                "Replicate data from a SQL Server source to a REST target on a "
                "manual or scheduled trigger.",
                "Emit executable component specs (DB source, JSON transform, "
                "REST target, structured process) for build_integration("
                "action='plan'); a verified Try/Catch + DLQ catch path is "
                "emitted for DLQ modes document_cache_ref / error_subprocess_ref, "
                "with caller retry (max_attempts 1..6 → platform-timed "
                "retry_count 0..5) wired through (#51 M3.R1a / #88 M4.5.3). "
                "Deployment and schedule activation remain M3.",
            ],
            endpoints=[db_endpoint, rest_endpoint],
            flows=flows,
            naming=naming_block,
            folders=folders_block,
            runtime=runtime_block,
            validation_rules={
                "contract_only": False,
                "component_count": len(components),
                "raw_xml_exposed": False,
                "boomi_mutation": False,
                "metadata_version": "0.5.0",
                # Representation of trigger / schedule / watermark / retry intent /
                # DLQ intent / error classifier / run metadata / expected status
                # codes / deferred follow-up notes. Verified DLQ modes + caller
                # retry (max_attempts → retry_count) are wired into the process
                # reliability (#51 M3.R1a / #88 M4.5.3).
                "operational_intent": operational_intent,
                "transform_review": {
                    "supported_actions": [
                        "list_fields",
                        "validate_unmapped",
                        "mapping_diff",
                        "generate_test_payload",
                        "compare_expected_actual",
                    ],
                    "recommended_before_apply": [
                        "validate_unmapped",
                        "generate_test_payload",
                    ],
                },
                "limitations": {
                    "schedule_activation": "M3 (deploy to a runtime first)",
                    "process_dlq": "emitted for document_cache_ref / error_subprocess_ref (#51 M3.R1a)",
                    "process_retry": "max_attempts 1..6 wired as platform-timed Try/Catch retry_count 0..5 (#88 M4.5.3); requires a wired DLQ catch path; no caller-selected backoff",
                    "process_notify": "optional log-only Notify on the catch path (reliability.catch_notify, #89 M4.5.4); requires a wired DLQ; email/SMS channels out of scope",
                    "watermark_update": "#51 (dynamic operation-property wiring)",
                    "db_create_auth": (
                        "username_password only; windows_integrated requires reuse"
                    ),
                    "environment_extensions": (
                        "create-mode username_password AND reuse-mode DB sources "
                        "declare credential connection fields (username/password) "
                        "as per-environment override points by default; host/port "
                        "opt-in via environment_extensions.endpoint_connection_fields; "
                        "the REST target also declares reuse-mode credentials by "
                        "default and the base URL on opt-in "
                        "(rest_endpoint_connection_fields). DB overrides are "
                        "xpath-keyed, REST overrides are id-keyed (no xpath). "
                        "processOverrides is unowned so UI-populated override "
                        "values survive updates; runtime override availability is "
                        "live_QA_required (#92 / #102 B1)"
                    ),
                    "rest_create_auth": (
                        "auth='none' only; secured auth requires reuse"
                    ),
                    "jdbc_options_and_default_headers": (
                        "metadata-deferred; no builder field in M2 (use reuse)"
                    ),
                },
                "profile_schema_strategy": (
                    "M2 uses caller-declared DB read result fields and a "
                    "caller-supplied JSON profile tree for the REST target; "
                    "no SQL parsing, DB metadata introspection, row sampling, "
                    "or Boomi browse is performed by this archetype. Issue "
                    "#43 generates deterministic source/target profile field "
                    "indexes and normalized direct mapping metadata on the "
                    "transform flow for downstream profile/map builders "
                    "(issue #26); metadata/sample inference is available "
                    "separately via infer_profile_fields (issue #47)."
                ),
                "transform_routes": {
                    "direct": "#26",
                    "map_function": "#40",
                    "map_script": "#41",
                    "xslt": "#42 (rejected in M2)",
                },
            },
        )

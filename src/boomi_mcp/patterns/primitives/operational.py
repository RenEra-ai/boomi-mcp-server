"""Issue #28: operational reliability primitives.

Five fragment-only primitives that represent operational intent for
``database_to_api_sync`` assembly (issue #29) *without* materializing Boomi
components:

  * ``schedule_envelope`` — manual vs scheduled (cron) execution trigger.
  * ``watermark_state``   — incremental-extraction state strategy (DPP or
                            external store); no live DPP mutation at build time.
  * ``error_classifier``  — retriable vs terminal HTTP status classification.
  * ``dlq_writer``        — dead-letter routing intent (document cache or error
                            subprocess reference); never generates payload bodies.
  * ``run_metadata``      — static run metadata + dynamic process property names.

Each implements ``emit_components`` as a no-op (``[]``) and overrides
``emit_fragment``. Emitting a ``reliability`` fragment is representation only —
these fragments do not drive process retry. Process-level Try/Catch retry/DLQ
is wired by the archetype's RetryPolicy/DlqPolicy + ProcessFlowBuilder (#51
M3.R1a / #88 M4.5.3), not by these primitive fragments.

Validation philosophy mirrors issue #27 ``db_extract``: parameter models carry
structural / cross-field shape constraints (raising pydantic ``ValidationError``
at ``validate_parameters``), while domain validation that yields a structured
error code (status-code ranges, secret-shaped keys) runs at ``emit_fragment``
and raises ``BuilderValidationError``.
"""

from __future__ import annotations

from typing import Any, Dict, List, Literal, Optional

from pydantic import (
    BaseModel,
    ConfigDict,
    Field,
    StrictInt,
    field_validator,
    model_validator,
)

from ...categories.components.builders.connector_builder import (
    BuilderValidationError,
)
from ...models.integration_models import IntegrationComponentSpec
from ..base import (
    PatternIOContract,
    PatternKind,
    PatternMetadata,
    PrimitiveBuildContext,
    PrimitivePattern,
)
from ._helpers import (
    nonblank_str,
    reject_status_code_overlap,
    scan_secret_keys,
    scan_secret_values,
    validate_status_codes,
)

# Primitive-layer error codes for run-metadata key/value hygiene.
BLANK_METADATA_KEY = "BLANK_METADATA_KEY"
BLANK_METADATA_VALUE = "BLANK_METADATA_VALUE"


class _FragmentOnlyPrimitive:
    """Mixin: operational primitives emit no components, only a fragment.

    Not a ``PrimitivePattern`` subclass, so the registry's discovery walk does
    not pick it up; it just supplies the concrete ``emit_components`` no-op that
    satisfies the abstract requirement for the real primitive classes.
    """

    @classmethod
    def emit_components(
        cls,
        context: PrimitiveBuildContext,
        parameters: BaseModel,
    ) -> List[IntegrationComponentSpec]:
        return []


# ===========================================================================
# schedule_envelope
# ===========================================================================


class ScheduleEnvelopeParameters(BaseModel):
    model_config = ConfigDict(extra="forbid")

    mode: Literal["manual", "scheduled"]
    cron: Optional[str] = Field(
        default=None, description="5-part cron expression; required for scheduled mode"
    )
    timezone: Optional[str] = Field(default=None, description="Optional schedule timezone metadata")
    max_retry: Optional[StrictInt] = Field(
        default=None, ge=0, le=5, description="Optional retry-schedule maximum (0..5)"
    )

    @field_validator("timezone", mode="before")
    @classmethod
    def _blank_to_none(cls, value: Any) -> Any:
        # Treat a blank / whitespace-only timezone as absent so it is neither
        # emitted as a meaningless "" nor mistaken for a real schedule field in
        # manual mode (matches the issue #27 blank-binding convention).
        if isinstance(value, str):
            return value.strip() or None
        return value

    @model_validator(mode="after")
    def _check_mode(self) -> "ScheduleEnvelopeParameters":
        if self.mode == "manual":
            # Manual execution has no schedule — reject schedule-only payloads
            # rather than silently dropping them.
            for fname in ("cron", "timezone", "max_retry"):
                if getattr(self, fname) is not None:
                    raise ValueError(
                        f"manual mode does not accept {fname!r}; schedule fields "
                        "are only valid with mode='scheduled'"
                    )
            return self
        # scheduled — require a well-formed 5-part cron.
        cron = nonblank_str(self.cron)
        if cron is None:
            raise ValueError("scheduled mode requires a non-blank 5-part cron expression")
        parts = cron.split()
        if len(parts) != 5:
            raise ValueError(
                f"cron must have exactly 5 whitespace-separated fields, got {len(parts)}"
            )
        return self


class ScheduleEnvelopePrimitive(_FragmentOnlyPrimitive, PrimitivePattern):
    """Represent a process execution trigger (manual or cron-scheduled).

    Per Boomi, a schedule only takes effect after the process is deployed to a
    runtime — this primitive records the intent; it never calls manage_schedules.
    """

    metadata = PatternMetadata(
        name="schedule_envelope",
        version="1.0.0",
        kind=PatternKind.PRIMITIVE,
        description=(
            "Represent a process execution trigger as planning metadata: "
            "manual run, or a cron-scheduled run that takes effect only after "
            "deployment to a runtime. Emits a process fragment; does not apply "
            "or activate any schedule."
        ),
        tags=["operation", "schedule", "trigger"],
        use_cases=[
            "Declare a manually executed integration",
            "Declare a cron-scheduled integration trigger for later deploy orchestration",
        ],
        not_for=[
            "Applying or activating a live schedule",
            "Listener / real-time triggers",
        ],
    )
    parameters_model = ScheduleEnvelopeParameters

    output_contract = PatternIOContract(
        name="schedule_fragment",
        description="Execution trigger fragment (manual or scheduled).",
    )

    @classmethod
    def emit_fragment(
        cls, context: PrimitiveBuildContext, parameters: BaseModel
    ) -> Dict[str, Any]:
        params: ScheduleEnvelopeParameters = parameters  # type: ignore[assignment]
        if params.mode == "manual":
            return {"process_config": {"execution": {"trigger": {"mode": "manual"}}}}

        trigger: Dict[str, Any] = {"mode": "scheduled", "cron": nonblank_str(params.cron)}
        if params.timezone is not None:
            trigger["timezone"] = params.timezone
        schedule_meta: Dict[str, Any] = {"applies_after_deploy": True}
        if params.max_retry is not None:
            schedule_meta["max_retry"] = params.max_retry
        return {
            "process_config": {"execution": {"trigger": trigger}},
            "metadata": {"schedule": schedule_meta},
        }


# ===========================================================================
# watermark_state
# ===========================================================================


class WatermarkStateParameters(BaseModel):
    model_config = ConfigDict(extra="forbid")

    enabled: bool
    field: Optional[str] = Field(default=None, description="Source field tracked as the watermark")
    kind: Optional[Literal["timestamp", "sequence"]] = None
    persistence: Optional[Literal["dpp", "external_store"]] = None
    dpp_name: Optional[str] = Field(default=None, description="Dynamic process property name for dpp persistence")
    initial_value: Optional[str] = Field(default=None, description="Opaque initial watermark value")
    store_ref: Optional[str] = Field(default=None, description="Opaque external-store reference")

    @field_validator("dpp_name", mode="before")
    @classmethod
    def _blank_to_none(cls, value: Any) -> Any:
        # Treat a blank / whitespace-only dpp_name as absent so it is not
        # emitted as a meaningless "" into the watermark fragment (a blank
        # property name would be a bad #29 assembly input).
        if isinstance(value, str):
            return value.strip() or None
        return value

    @model_validator(mode="after")
    def _check_state(self) -> "WatermarkStateParameters":
        present = {
            fname: getattr(self, fname)
            for fname in ("field", "kind", "persistence", "dpp_name", "initial_value", "store_ref")
            if getattr(self, fname) is not None
        }
        if not self.enabled:
            if present:
                raise ValueError(
                    "disabled watermark must not carry "
                    f"{sorted(present)}; set enabled=true to configure them"
                )
            return self

        # enabled — require the core triple.
        if nonblank_str(self.field) is None:
            raise ValueError("enabled watermark requires a non-blank field")
        if self.kind is None:
            raise ValueError("enabled watermark requires kind (timestamp | sequence)")
        if self.persistence is None:
            raise ValueError("enabled watermark requires persistence (dpp | external_store)")

        if self.persistence == "dpp":
            if self.store_ref is not None:
                raise ValueError("dpp persistence does not accept store_ref")
        else:  # external_store
            if nonblank_str(self.store_ref) is None:
                raise ValueError("external_store persistence requires a non-blank store_ref")
            for fname in ("dpp_name", "initial_value"):
                if getattr(self, fname) is not None:
                    raise ValueError(f"external_store persistence does not accept {fname!r}")
        return self


class WatermarkStatePrimitive(_FragmentOnlyPrimitive, PrimitivePattern):
    """Represent incremental-extraction watermark state strategy.

    Records the field, kind, and persistence strategy as planning metadata for
    issue #29 assembly. It does not read or write any live Dynamic Process
    Property value during build.
    """

    metadata = PatternMetadata(
        name="watermark_state",
        version="1.0.0",
        kind=PatternKind.PRIMITIVE,
        description=(
            "Represent the incremental-extraction watermark strategy (tracked "
            "field, timestamp or sequence kind, and dynamic-process-property or "
            "external-store persistence) as planning metadata. Does not mutate "
            "any live property value during build."
        ),
        tags=["operation", "watermark", "state", "incremental"],
        use_cases=[
            "Declare a timestamp watermark persisted as a dynamic process property",
            "Declare a sequence watermark backed by an external store",
        ],
        not_for=[
            "Reading or writing live dynamic process property values",
            "Full-load extractions that need no watermark",
        ],
    )
    parameters_model = WatermarkStateParameters

    output_contract = PatternIOContract(
        name="watermark_fragment",
        description="Watermark state strategy fragment.",
    )

    @classmethod
    def emit_fragment(
        cls, context: PrimitiveBuildContext, parameters: BaseModel
    ) -> Dict[str, Any]:
        params: WatermarkStateParameters = parameters  # type: ignore[assignment]
        if not params.enabled:
            return {"metadata": {"watermark": {"enabled": False}}}

        watermark: Dict[str, Any] = {
            "enabled": True,
            "field": nonblank_str(params.field),
            "kind": params.kind,
            "persistence": params.persistence,
        }
        if params.persistence == "dpp":
            if params.dpp_name is not None:
                watermark["dpp_name"] = params.dpp_name
            if params.initial_value is not None:
                watermark["initial_value"] = params.initial_value
        else:
            watermark["store_ref"] = nonblank_str(params.store_ref)
        return {"metadata": {"watermark": watermark}}


# ===========================================================================
# error_classifier
# ===========================================================================


_DEFAULT_RETRIABLE = [502, 503, 504]
_DEFAULT_TERMINAL = [400, 401, 403, 404, 422]


class ErrorClassifierParameters(BaseModel):
    model_config = ConfigDict(extra="forbid")

    # List[Any] (not List[int]) so validate_status_codes can reject bools and
    # strings explicitly rather than letting pydantic coerce them.
    retriable_status_codes: List[Any] = Field(default_factory=lambda: list(_DEFAULT_RETRIABLE))
    terminal_status_codes: List[Any] = Field(default_factory=lambda: list(_DEFAULT_TERMINAL))
    custom_rules: List[str] = Field(
        default_factory=list, description="Opaque classifier rule labels (no executable logic)"
    )


class ErrorClassifierPrimitive(_FragmentOnlyPrimitive, PrimitivePattern):
    """Classify response status codes as retriable vs terminal."""

    metadata = PatternMetadata(
        name="error_classifier",
        version="1.0.0",
        kind=PatternKind.PRIMITIVE,
        description=(
            "Classify target response status codes into retriable and terminal "
            "sets, with opaque custom rule labels. Emits a reliability fragment "
            "for assembly; it does not itself enable process retries."
        ),
        tags=["operation", "reliability", "error", "classifier"],
        use_cases=[
            "Mark gateway/timeout statuses as retriable and client errors as terminal",
            "Record custom error-classification labels for later wiring",
        ],
        not_for=[
            "Enabling unverified process retry/DLQ behavior",
            "Executable error-handling scripts",
        ],
    )
    parameters_model = ErrorClassifierParameters

    output_contract = PatternIOContract(
        name="error_classifier_fragment",
        description="Retriable/terminal status classification fragment.",
    )

    @classmethod
    def emit_fragment(
        cls, context: PrimitiveBuildContext, parameters: BaseModel
    ) -> Dict[str, Any]:
        params: ErrorClassifierParameters = parameters  # type: ignore[assignment]
        retriable = validate_status_codes(
            params.retriable_status_codes, "retriable_status_codes"
        )
        terminal = validate_status_codes(
            params.terminal_status_codes, "terminal_status_codes"
        )
        reject_status_code_overlap(retriable, terminal)
        return {
            "process_config": {
                "reliability": {
                    "error_classifier": {
                        "retriable_status_codes": retriable,
                        "terminal_status_codes": terminal,
                        "custom_rules": list(params.custom_rules),
                    }
                }
            }
        }


# ===========================================================================
# dlq_writer
# ===========================================================================


class DlqWriterParameters(BaseModel):
    model_config = ConfigDict(extra="forbid")

    mode: Literal["disabled", "document_cache_ref", "error_subprocess_ref"]
    document_cache_id: Optional[str] = Field(default=None, description="Existing Document Cache component id")
    document_cache_ref_key: Optional[str] = Field(
        default=None, description="In-spec Document Cache component key (added to process depends_on)"
    )
    process_id: Optional[str] = Field(default=None, description="Existing error-subprocess component id")
    process_ref_key: Optional[str] = Field(
        default=None, description="In-spec error-subprocess component key (added to process depends_on)"
    )

    @field_validator(
        "document_cache_id",
        "document_cache_ref_key",
        "process_id",
        "process_ref_key",
        mode="before",
    )
    @classmethod
    def _blank_to_none(cls, value: Any) -> Any:
        if isinstance(value, str):
            stripped = value.strip()
            return stripped or None
        return value

    @model_validator(mode="after")
    def _check_mode(self) -> "DlqWriterParameters":
        cache_fields = (self.document_cache_id, self.document_cache_ref_key)
        process_fields = (self.process_id, self.process_ref_key)

        if self.mode == "disabled":
            if any(cache_fields) or any(process_fields):
                raise ValueError(
                    "disabled DLQ must not carry document cache or error "
                    "subprocess references"
                )
            return self

        if self.mode == "document_cache_ref":
            if any(process_fields):
                raise ValueError(
                    "document_cache_ref mode does not accept process_id / process_ref_key"
                )
            if bool(self.document_cache_id) == bool(self.document_cache_ref_key):
                raise ValueError(
                    "document_cache_ref requires exactly one of document_cache_id "
                    "or document_cache_ref_key"
                )
            return self

        # error_subprocess_ref
        if any(cache_fields):
            raise ValueError(
                "error_subprocess_ref mode does not accept document_cache_id / "
                "document_cache_ref_key"
            )
        if bool(self.process_id) == bool(self.process_ref_key):
            raise ValueError(
                "error_subprocess_ref requires exactly one of process_id or process_ref_key"
            )
        return self


class DlqWriterPrimitive(_FragmentOnlyPrimitive, PrimitivePattern):
    """Represent dead-letter routing intent (document cache or error subprocess).

    Records where failed documents should be routed; it never generates payload
    bodies or error-report formats (those stay caller/task configured). A
    document cache is execution-scoped, so it is routing intent, not durable
    storage by itself.
    """

    metadata = PatternMetadata(
        name="dlq_writer",
        version="1.0.0",
        kind=PatternKind.PRIMITIVE,
        description=(
            "Represent dead-letter routing intent: disabled, a Document Cache "
            "reference, or an error-subprocess reference. Emits a reliability "
            "fragment for assembly; never generates payload bodies and does not "
            "enable unverified process error paths."
        ),
        tags=["operation", "reliability", "dlq"],
        use_cases=[
            "Route failed documents to an existing error subprocess",
            "Reference a Document Cache for in-execution failure capture",
        ],
        not_for=[
            "Generating dead-letter payload bodies or report formats",
            "Durable cross-execution storage (a document cache is execution-scoped)",
        ],
    )
    parameters_model = DlqWriterParameters

    output_contract = PatternIOContract(
        name="dlq_fragment",
        description="Dead-letter routing fragment.",
    )

    @classmethod
    def emit_fragment(
        cls, context: PrimitiveBuildContext, parameters: BaseModel
    ) -> Dict[str, Any]:
        params: DlqWriterParameters = parameters  # type: ignore[assignment]
        dlq: Dict[str, Any] = {"mode": params.mode}
        depends_on: List[str] = []

        if params.mode == "document_cache_ref":
            if params.document_cache_id:
                dlq["document_cache_id"] = params.document_cache_id
            else:
                dlq["document_cache_ref_key"] = params.document_cache_ref_key
                depends_on.append(params.document_cache_ref_key)
        elif params.mode == "error_subprocess_ref":
            if params.process_id:
                dlq["process_id"] = params.process_id
            else:
                dlq["process_ref_key"] = params.process_ref_key
                depends_on.append(params.process_ref_key)

        fragment: Dict[str, Any] = {"process_config": {"reliability": {"dlq": dlq}}}
        if depends_on:
            fragment["depends_on"] = depends_on
        return fragment


# ===========================================================================
# run_metadata
# ===========================================================================


class RunMetadataParameters(BaseModel):
    model_config = ConfigDict(extra="forbid")

    static_metadata: Dict[str, str] = Field(
        default_factory=dict, description="Static run metadata name/value pairs"
    )
    dynamic_process_properties: Optional[Dict[str, str]] = Field(
        default=None, description="Dynamic process property name -> description/source label"
    )
    correlation_id_property: Optional[str] = Field(
        default=None, description="Name of the property carrying a per-run correlation id"
    )


class RunMetadataPrimitive(_FragmentOnlyPrimitive, PrimitivePattern):
    """Represent static run metadata and dynamic process property names."""

    metadata = PatternMetadata(
        name="run_metadata",
        version="1.0.0",
        kind=PatternKind.PRIMITIVE,
        description=(
            "Represent static run metadata name/value pairs plus optional "
            "dynamic process property names and a correlation-id property. "
            "Rejects secret-shaped keys; emits an execution fragment for "
            "assembly. Does not set or persist live property values."
        ),
        tags=["operation", "run", "metadata"],
        use_cases=[
            "Tag a run with static ownership / environment metadata",
            "Declare dynamic process property names and a correlation id property",
        ],
        not_for=[
            "Storing credentials or secret-shaped values",
            "Setting or persisting live dynamic process property values",
        ],
    )
    parameters_model = RunMetadataParameters

    output_contract = PatternIOContract(
        name="run_metadata_fragment",
        description="Run metadata execution fragment.",
    )

    @classmethod
    def emit_fragment(
        cls, context: PrimitiveBuildContext, parameters: BaseModel
    ) -> Dict[str, Any]:
        params: RunMetadataParameters = parameters  # type: ignore[assignment]

        # Secret-shaped keys AND values are rejected for every caller-authored
        # metadata surface — the value scan is the backstop for an innocuous
        # key holding credential material (e.g. a JWT).
        scan_secret_keys(params.static_metadata, "static_metadata")
        scan_secret_values(params.static_metadata, "static_metadata")
        for key, value in params.static_metadata.items():
            if not key.strip():
                raise _blank_key_error("static_metadata")
            if not value.strip():
                raise _blank_value_error("static_metadata", key)

        dpp = params.dynamic_process_properties
        if dpp is not None:
            scan_secret_keys(dpp, "dynamic_process_properties")
            scan_secret_values(dpp, "dynamic_process_properties")
            for name in dpp:
                if not name.strip():
                    raise _blank_key_error("dynamic_process_properties")

        if params.correlation_id_property is not None:
            if not params.correlation_id_property.strip():
                raise _blank_value_error("correlation_id_property", "correlation_id_property")
            scan_secret_keys(
                {params.correlation_id_property: "x"}, "correlation_id_property"
            )

        execution: Dict[str, Any] = {}
        if params.static_metadata:
            execution["run_metadata"] = dict(params.static_metadata)
        if params.correlation_id_property is not None:
            execution["correlation_id_property"] = params.correlation_id_property

        fragment: Dict[str, Any] = {}
        if execution:
            fragment["process_config"] = {"execution": execution}
        if dpp:
            fragment["metadata"] = {"dynamic_process_properties": dict(dpp)}
        return fragment


# ---------------------------------------------------------------------------
# Local structured-error helpers (shared error envelope with the builders).
# ---------------------------------------------------------------------------


def _blank_key_error(field: str) -> BuilderValidationError:
    return BuilderValidationError(
        f"{field} contains a blank key",
        error_code=BLANK_METADATA_KEY,
        field=field,
        hint="Metadata keys must be non-blank.",
    )


def _blank_value_error(field: str, key: str) -> BuilderValidationError:
    return BuilderValidationError(
        f"{field} value for {key!r} is blank",
        error_code=BLANK_METADATA_VALUE,
        field=field,
        hint="Provide a non-blank value or omit the entry.",
        details={"offending_key": key},
    )

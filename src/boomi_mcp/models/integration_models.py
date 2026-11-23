"""
Pydantic models for high-level integration orchestration.
"""

from typing import Any, Dict, List, Literal, Optional, Tuple

from pydantic import BaseModel, Field, field_validator, model_validator
from pydantic_core import PydanticCustomError

from .pipeline_models import PipelineSpec
from .process_component import ProcessAuthoringUnitV1


class IntegrationComponentSpec(BaseModel):
    """Canonical description of a component operation in an integration build."""

    key: str = Field(..., description="Unique key for dependency references")
    type: str = Field(..., description="Component type (process, connector-settings, trading_partner, etc.)")
    action: Literal["create", "update"] = Field(default="create")
    name: Optional[str] = Field(default=None, description="Component display name")
    component_id: Optional[str] = Field(default=None, description="Required for direct updates when not discoverable")
    config: Dict[str, Any] = Field(
        default_factory=dict,
        description=(
            "Type-specific configuration payload. For type='process' a row "
            "only BINDS an existing process by its explicit id: "
            "action='create' with config.reference_only=true and "
            "config.component_id, or a top-level component_id under "
            "conflict_policy='reuse'. Any other process row — a process_kind / "
            "process_type selector, flow_sequence, xml or any other body field, "
            "an update, or a name without an id — is refused with "
            "PROCESS_COMPONENT_AUTHORING_UNIT_REQUIRED before any write; a "
            "process body is authored as a canonical unit in 'processes' (or "
            "authoring_request intent_kind='process_ir'). Use manage_component "
            "for an explicit raw process XML escape hatch."
        ),
    )
    depends_on: List[str] = Field(default_factory=list, description="Component keys this component depends on")

    @field_validator("depends_on")
    @classmethod
    def validate_no_self_dependency(cls, value: List[str], info):
        key = info.data.get("key")
        if key and key in value:
            raise ValueError(f"Component '{key}' cannot depend on itself")
        return value


#: The spec version that opted into the STRICT top-level pipeline surface
#: (ADR-001 §5). #159 S3: the planner no longer reads it (the strict authority
#: retired with the legacy process builders); the isolated legacy authority's
#: ``STRICT_VERSION`` is still pinned equal to it by
#: ``tests/test_issue_139_authority.py`` until #160.
STRICT_PIPELINE_VERSION = "1.1"


class IntegrationSpecV1(BaseModel):
    """Canonical JSON contract for integration planning and execution."""

    version: Literal["1.0", "1.1"] = Field(
        default="1.0",
        description=(
            "Authoring surface selector (issue #139D / ADR-001 §5, §9). '1.0' "
            "is the default. '1.1' is still accepted and selects no different "
            "behaviour since #159 S3: the strict pipeline-vs-process authority "
            "retired with the legacy process builders, so a top-level "
            "'pipeline' is an inert analysis view on both versions. The "
            "'source_description' and bare top-level forms rebuild the spec "
            "from a fixed key allowlist that carries no 'version', so they "
            "always normalize to '1.0'. V1 is NOT deprecated and emits no "
            "warning."
        ),
    )
    name: str = Field(..., description="Integration name")
    mode: Literal["lift_shift", "redesign"] = Field(default="lift_shift")
    components: List[IntegrationComponentSpec] = Field(
        default_factory=list,
        description=(
            "Supporting components — connections, operations, profiles, maps — "
            "and reference-only entries for processes that already exist. A "
            "LIST, deliberately: this is the legacy mutable authoring surface "
            "that build_integration edits in place. Canonical ProcessIR roots "
            "belong in 'processes' instead; this collection and that one share "
            "ONE key namespace, so a key used by a component and by a process "
            "envelope is a duplicate, and all four depends_on directions "
            "(component->component, component->process, process->component, "
            "process->process) enter one topological order with one cycle "
            "check."
        ),
    )
    processes: Tuple[ProcessAuthoringUnitV1, ...] = Field(
        # A TUPLE, per the plan's verbatim field type (§6 review AR1-08). A new
        # field on this legacy-tolerant model breaks no pre-#153 caller — none
        # carries `processes` — and pydantic coerces list input, so the wire
        # shape is unchanged; only in-place mutation becomes unrepresentable.
        default=(),
        description=(
            "Issue #153 M12.15 — canonical process roots authored as ProcessIR. "
            "Each entry is one ProcessAuthoringUnitV1: exactly one "
            "ProcessComponentEnvelopeV1 plus exactly one ProcessIRV1 root. These "
            "are compiled by the canonical chain and materialized by the neutral "
            "ProcessComponentMaterializer — they resolve NO legacy process_kind. "
            "'components' remains the home of supporting components and of "
            "reference-only existing processes. 'components' and 'processes' "
            "share ONE key namespace: a key used by a component and by a "
            "process envelope is a duplicate, and all four depends_on "
            "directions (component->component, component->process, "
            "process->component, process->process) enter one topological order "
            "with one cycle check. "
            "A TUPLE, per the plan's field type: pydantic coerces list input, "
            "so the wire shape is an ordinary JSON array and no caller sends "
            "anything different — but unlike this model's legacy List "
            "siblings, mutating the collection itself is unrepresentable. "
            "Per-element immutability lives on the frozen "
            "ProcessAuthoringUnitV1 entries."
        ),
    )
    goals: List[str] = Field(default_factory=list)
    endpoints: List[Dict[str, Any]] = Field(default_factory=list)
    flows: List[Dict[str, Any]] = Field(default_factory=list)
    naming: Dict[str, Any] = Field(
        default_factory=dict,
        description=(
            "Free-form naming governance anchor (issue #93 / account_governance "
            "M4.5.8). Recognized keys: 'component_names' (per-role display names "
            "an archetype emits) and the optional 'component_name_pattern' (a "
            "regex every created component name must match). The build_integration "
            "plan-time name lint reads this field and rejects/flags missing names, "
            "platform default names ('New Map', 'New Profile'), copy-induced "
            "numeric suffixes ('... 1'/'... 2'), duplicate create names, and "
            "(when component_name_pattern is supplied) names that do not match it "
            "— it never silently rewrites. Folder placement / roles / locking are "
            "GUI-only governance (see account_governance) and are NOT linted."
        ),
    )
    folders: Dict[str, Any] = Field(default_factory=dict)
    runtime: Dict[str, Any] = Field(default_factory=dict)
    validation_rules: Dict[str, Any] = Field(default_factory=dict)
    profile_indexes_by_component_id: Optional[Dict[str, Dict[str, Any]]] = Field(
        default=None,
        description=(
            "Issue #95 M7.5 — EPHEMERAL, validation-only. Keyed by literal "
            "existing-profile component UUID; each value is the object returned "
            "by index_profile_component: {component_id, profile_component_type, "
            "field_index_by_path}. Lets build_integration validate a transform.map "
            "whose source_profile_id / target_profile_id is a literal "
            "existing-profile UUID (not a '$ref:KEY' in-spec profile). When a "
            "literal-UUID map endpoint is not covered here, build_integration "
            "discovers the index live (read-only) or, failing that, rejects with "
            "MAP_PROFILE_INDEX_UNAVAILABLE. This field is NEVER emitted into a "
            "Boomi component or any mutation payload — build_integration only "
            "materializes 'components'."
        ),
    )
    pipeline: Optional[PipelineSpec] = Field(
        default=None,
        description=(
            "Optional semantic stage graph (M5 sync-pipeline contract). When "
            "present, describes the stage graph; no Boomi XML is emitted from "
            "this field — it is an inspectable/analysis view, never an "
            "executable input (ADR-001 §4), preserved as an inert echo on every "
            "version. A secret-shaped key anywhere in it is refused with "
            "PLAINTEXT_SECRET_REJECTED before normalization."
        ),
    )

    @model_validator(mode="after")
    def validate_shared_key_namespace(self) -> "IntegrationSpecV1":
        """``components[].key`` and ``processes[].envelope.component_key`` are ONE namespace.

        Deliberately scoped to the keys this slice introduces: duplicates WITHIN
        ``processes``, and collisions ACROSS the two lists. A components-only
        duplicate is left exactly where it has always been raised —
        ``integration_builder._topological_order`` — because tightening it here
        would change the legacy authoring surface for specs that carry no
        ``processes`` at all, which is precisely the silent strictification this
        additive field must not cause. No pre-#153 spec has a ``processes``
        entry, so nothing that validates today can begin failing.
        """
        if not self.processes:
            return self

        process_keys = [unit.envelope.component_key for unit in self.processes]
        duplicated = sorted(
            {key for key in process_keys if process_keys.count(key) > 1}
        )
        if duplicated:
            raise PydanticCustomError(
                "integration_component_key_duplicate",
                "process envelope key(s) declared more than once: {keys}",
                {"keys": ", ".join(duplicated)},
            )

        collisions = sorted(set(process_keys) & {comp.key for comp in self.components})
        if collisions:
            raise PydanticCustomError(
                "integration_component_key_duplicate",
                "key(s) used by both a component and a process envelope: {keys}. "
                "components[].key and processes[].envelope.component_key share one "
                "namespace",
                {"keys": ", ".join(collisions)},
            )
        return self

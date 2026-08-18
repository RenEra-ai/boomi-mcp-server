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
            "Type-specific configuration payload. For type='process', "
            "config.process_kind selects a structured process-flow builder "
            "(e.g. 'database_to_api_sync' or 'wrapper_subprocess' — issue #25) "
            "and is REQUIRED for any authoring action that emits or rebuilds "
            "process XML (create / create_clone / update): such a process "
            "without process_kind is rejected at plan time with "
            "PROCESS_KIND_REQUIRED (legacy freeform JSON-to-XML process "
            "authoring has been removed). A reference-only reuse of an existing "
            "process (no XML emitted) may omit it. Use manage_component for an "
            "explicit raw process XML escape hatch."
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


class IntegrationSpecV1(BaseModel):
    """Canonical JSON contract for integration planning and execution."""

    version: Literal["1.0", "1.1"] = Field(
        default="1.0",
        description=(
            "Authoring surface selector (issue #139D / ADR-001 §5, §9). "
            "'1.0' (the default, and the value every archetype emits) is the "
            "frozen legacy surface: an authored top-level 'pipeline' is accepted "
            "and preserved as an INERT echo that drives nothing, exactly as "
            "today. '1.1' opts in to the STRICT surface, on which a top-level "
            "'pipeline' that disagrees with the single authored process — or is "
            "ambiguous because the spec authors two or more processes — is "
            "rejected at plan time with LEGACY_ADAPTER_AUTHORITY_CONFLICT, "
            "before collision resolution and before any mutation. '1.1' NEVER "
            "makes 'pipeline' executable; the process component's own config is "
            "still the only executable authority. Selecting the strict surface "
            "requires the explicit 'integration_spec' input form — the "
            "'source_description' and bare top-level forms rebuild the spec from "
            "a fixed key allowlist that carries no 'version', so they always "
            "normalize to '1.0'. V1 is NOT deprecated and emits no warning."
        ),
    )
    name: str = Field(..., description="Integration name")
    mode: Literal["lift_shift", "redesign"] = Field(default="lift_shift")
    components: List[IntegrationComponentSpec] = Field(default_factory=list)
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
            "reference-only existing processes. Both tuples share ONE key "
            "namespace: a key used by a component and by a process envelope is a "
            "duplicate, and all four depends_on directions (component->component, "
            "component->process, process->component, process->process) enter one "
            "topological order with one cycle check. "
            "A LIST, not a tuple, deliberately: IntegrationSpecV1 is the legacy "
            "mutable authoring surface that build_integration edits in place, and "
            "every sibling collection here is a List. Immutability lives where it "
            "belongs — on the frozen ProcessAuthoringUnitV1 elements."
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
            "this field alone — it is an inspectable/analysis view, never an "
            "executable input (ADR-001 §4). On version='1.0' it is preserved as "
            "an inert echo even when it contradicts the authored process. On "
            "version='1.1' it is validated against the single authored process's "
            "normalized semantics and a disagreement is rejected with "
            "LEGACY_ADAPTER_AUTHORITY_CONFLICT; it is additionally withheld "
            "(echoed as null) when that process resolves to reuse of an existing "
            "component, because the request then authors no process for the view "
            "to describe (ADR-001 §5 view-faithfulness)."
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

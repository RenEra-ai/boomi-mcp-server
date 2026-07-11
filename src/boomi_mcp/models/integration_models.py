"""
Pydantic models for high-level integration orchestration.
"""

from typing import Any, Dict, List, Literal, Optional

from pydantic import BaseModel, Field, field_validator

from .pipeline_models import PipelineSpec


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

    version: Literal["1.0"] = "1.0"
    name: str = Field(..., description="Integration name")
    mode: Literal["lift_shift", "redesign"] = Field(default="lift_shift")
    components: List[IntegrationComponentSpec] = Field(default_factory=list)
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
            "this field alone — wiring to process-flow builders is M5.2+."
        ),
    )


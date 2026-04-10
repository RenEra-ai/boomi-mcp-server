"""
Pydantic models for high-level integration orchestration.
"""

from typing import Any, Dict, List, Literal, Optional

from pydantic import BaseModel, Field, field_validator


class IntegrationComponentSpec(BaseModel):
    """Canonical description of a component operation in an integration build."""

    key: str = Field(..., description="Unique key for dependency references")
    type: str = Field(..., description="Component type (process, connector-settings, trading_partner, etc.)")
    action: Literal["create", "update"] = Field(default="create")
    name: Optional[str] = Field(default=None, description="Component display name")
    component_id: Optional[str] = Field(default=None, description="Required for direct updates when not discoverable")
    config: Dict[str, Any] = Field(default_factory=dict, description="Type-specific configuration payload")
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
    naming: Dict[str, Any] = Field(default_factory=dict)
    folders: Dict[str, Any] = Field(default_factory=dict)
    runtime: Dict[str, Any] = Field(default_factory=dict)
    validation_rules: Dict[str, Any] = Field(default_factory=dict)


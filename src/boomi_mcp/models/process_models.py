"""
Pydantic models for Boomi process components.

These models provide type safety and validation for process creation via the orchestrator.
"""

from pydantic import BaseModel, Field, field_validator
from typing import List, Optional, Dict, Literal, Any


class ShapeConfig(BaseModel):
    """
    Configuration for a single shape in a Boomi process.

    Attributes:
        type: Shape type (start, stop, map, connector, message, etc.)
        name: Unique name for the shape within the process
        userlabel: Display label for the shape in Boomi UI
        config: Shape-specific configuration (e.g., map_id for map shapes)

    Examples:
        Start shape:
            ShapeConfig(type="start", name="start", userlabel="Start")

        Map shape:
            ShapeConfig(
                type="map",
                name="transform",
                userlabel="Transform Data",
                config={"map_id": "abc-123-def"}
            )

        Connector shape:
            ShapeConfig(
                type="connector",
                name="sf_query",
                userlabel="Query Salesforce",
                config={
                    "connector_id": "xyz-789",
                    "operation": "query",
                    "object_type": "Account"
                }
            )
    """
    type: Literal[
        "start",
        "stop",
        "return",  # alias for returndocuments
        "map",
        "connector",
        "message",
        "decision",
        "note"
    ]
    name: str = Field(..., description="Unique shape name within process")
    userlabel: Optional[str] = Field(None, description="Display label in Boomi UI")
    config: Optional[Dict[str, Any]] = Field(
        default_factory=dict,
        description="Shape-specific configuration"
    )

    @field_validator('name')
    @classmethod
    def validate_name(cls, v: str) -> str:
        """Validate shape name follows Boomi conventions."""
        if not v:
            raise ValueError("Shape name cannot be empty")
        # Allow alphanumeric, underscore, hyphen
        if not all(c.isalnum() or c in ('_', '-') for c in v):
            raise ValueError("Shape name must be alphanumeric with _ or - only")
        return v

    @field_validator('type')
    @classmethod
    def validate_shape_requirements(cls, v: str, info) -> str:
        """Validate shape type has required config."""
        # This is called before config is available, so we can't validate config here
        # That validation happens in ProcessBuilder
        return v


class ProcessConfig(BaseModel):
    """
    Configuration for a Boomi process component.

    Attributes:
        name: Process name
        folder_name: Folder path (e.g., "Integrations/Production")
        description: Process description
        shapes: List of shapes in the process
        allow_simultaneous: Allow multiple concurrent executions
        enable_user_log: Enable user logging
        process_log_on_error_only: Only log on errors

    Example:
        ProcessConfig(
            name="SF to NS ETL",
            folder_name="Integrations/Production",
            description="Sync customers from Salesforce to NetSuite",
            shapes=[
                ShapeConfig(type="start", name="start"),
                ShapeConfig(
                    type="map",
                    name="transform",
                    config={"map_ref": "Customer Transform"}
                ),
                ShapeConfig(type="stop", name="end")
            ]
        )
    """
    name: str = Field(..., description="Process name")
    folder_name: str = Field(
        default="Home",
        description="Folder path (e.g., 'Integrations/Production')"
    )
    description: str = Field(
        default="",
        description="Process description"
    )
    shapes: List[ShapeConfig] = Field(
        ...,
        min_length=2,  # At minimum: start + stop/return
        description="List of shapes in the process flow"
    )

    # Process-level attributes
    allow_simultaneous: bool = Field(
        default=False,
        description="Allow concurrent process executions"
    )
    enable_user_log: bool = Field(
        default=False,
        description="Enable user logging"
    )
    process_log_on_error_only: bool = Field(
        default=False,
        description="Only log when errors occur"
    )
    purge_data_immediately: bool = Field(
        default=False,
        description="Purge process data immediately after execution"
    )
    update_run_dates: bool = Field(
        default=True,
        description="Update process run dates"
    )
    workload: Literal["general", "high", "low"] = Field(
        default="general",
        description="Process workload priority"
    )

    @field_validator('shapes')
    @classmethod
    def validate_shapes_flow(cls, v: List[ShapeConfig]) -> List[ShapeConfig]:
        """Validate process flow structure."""
        if not v:
            raise ValueError("Process must have at least one shape")

        # First shape must be start
        if v[0].type != "start":
            raise ValueError("First shape must be type 'start'")

        # Last shape must be stop or return
        if v[-1].type not in ("stop", "return"):
            raise ValueError("Last shape must be type 'stop' or 'return'")

        # Check for duplicate shape names
        names = [shape.name for shape in v]
        if len(names) != len(set(names)):
            duplicates = [name for name in names if names.count(name) > 1]
            raise ValueError(f"Duplicate shape names found: {duplicates}")

        return v


class ComponentSpec(BaseModel):
    """
    Specification for a component with dependencies (used by orchestrator).

    This model is used when creating multiple components with dependencies
    between them. The orchestrator uses this to determine creation order
    and resolve references.

    Attributes:
        name: Component name (unique within orchestration session)
        type: Component type (process, map, connection, etc.)
        dependencies: List of component names this depends on
        config: Component-specific configuration

    Examples:
        Map component (no dependencies):
            ComponentSpec(
                name="Customer Transform",
                type="map",
                dependencies=[],
                config={
                    "source_profile": "SF_Customer",
                    "target_profile": "NS_Customer"
                }
            )

        Process component (depends on map):
            ComponentSpec(
                name="Main ETL Process",
                type="process",
                dependencies=["Customer Transform"],
                config=ProcessConfig(
                    name="Main ETL Process",
                    shapes=[
                        ShapeConfig(type="start", name="start"),
                        ShapeConfig(
                            type="map",
                            name="transform",
                            config={"map_ref": "Customer Transform"}
                        ),
                        ShapeConfig(type="stop", name="end")
                    ]
                )
            )
    """
    name: str = Field(..., description="Component name (unique in session)")
    type: Literal["process", "map", "connection", "profile"] = Field(
        ...,
        description="Component type"
    )
    dependencies: List[str] = Field(
        default_factory=list,
        description="Names of components this depends on"
    )
    config: Dict[str, Any] = Field(
        default_factory=dict,
        description="Component-specific configuration"
    )

    @field_validator('dependencies')
    @classmethod
    def validate_no_self_dependency(cls, v: List[str], info) -> List[str]:
        """Ensure component doesn't depend on itself."""
        if 'name' in info.data and info.data['name'] in v:
            raise ValueError(f"Component cannot depend on itself: {info.data['name']}")
        return v

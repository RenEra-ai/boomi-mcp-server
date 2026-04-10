"""
Pydantic models for Boomi MCP Server.

These models provide type safety and validation for all MCP tool operations.
"""

from .process_models import (
    ShapeConfig,
    ProcessConfig,
    ComponentSpec,
)
from .integration_models import (
    IntegrationSpecV1,
    IntegrationComponentSpec,
)

__all__ = [
    'ShapeConfig',
    'ProcessConfig',
    'ComponentSpec',
    'IntegrationSpecV1',
    'IntegrationComponentSpec',
]

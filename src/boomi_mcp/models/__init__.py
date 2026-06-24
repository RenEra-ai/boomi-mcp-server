"""
Pydantic models for Boomi MCP Server.

These models provide type safety and validation for all MCP tool operations.
"""

from .integration_models import (
    IntegrationSpecV1,
    IntegrationComponentSpec,
)
from .pipeline_models import (
    PipelineSpec,
    StageSpec,
    PipelineEdgeSpec,
    PipelineStageKind,
    PipelineEdgeKind,
)

__all__ = [
    'IntegrationSpecV1',
    'IntegrationComponentSpec',
    'PipelineSpec',
    'StageSpec',
    'PipelineEdgeSpec',
    'PipelineStageKind',
    'PipelineEdgeKind',
]

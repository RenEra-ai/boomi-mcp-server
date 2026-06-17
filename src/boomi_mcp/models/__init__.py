"""
Pydantic models for Boomi MCP Server.

These models provide type safety and validation for all MCP tool operations.
"""

from .integration_models import (
    IntegrationSpecV1,
    IntegrationComponentSpec,
)

__all__ = [
    'IntegrationSpecV1',
    'IntegrationComponentSpec',
]

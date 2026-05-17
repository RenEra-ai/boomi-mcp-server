"""Concrete V3 archetypes. Discovered by PatternRegistry.from_package('boomi_mcp.patterns')."""

from .database_to_api_sync import (
    DatabaseToApiSyncArchetype,
    DatabaseToApiSyncParameters,
)
from .stub_minimal import (
    StubMinimalIntegrationArchetype,
    StubMinimalIntegrationParameters,
)

__all__ = [
    "DatabaseToApiSyncArchetype",
    "DatabaseToApiSyncParameters",
    "StubMinimalIntegrationArchetype",
    "StubMinimalIntegrationParameters",
]

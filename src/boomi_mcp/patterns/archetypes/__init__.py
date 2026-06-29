"""Concrete V3 archetypes. Discovered by PatternRegistry.from_package('boomi_mcp.patterns')."""

from .api_to_api_sync import (
    ApiToApiSyncArchetype,
    ApiToApiSyncParameters,
)
from .api_to_database_sync import (
    ApiToDatabaseSyncArchetype,
    ApiToDatabaseSyncParameters,
)
from .database_to_api_sync import (
    DatabaseToApiSyncArchetype,
    DatabaseToApiSyncParameters,
)
from .stub_minimal import (
    StubMinimalIntegrationArchetype,
    StubMinimalIntegrationParameters,
)

__all__ = [
    "ApiToApiSyncArchetype",
    "ApiToApiSyncParameters",
    "ApiToDatabaseSyncArchetype",
    "ApiToDatabaseSyncParameters",
    "DatabaseToApiSyncArchetype",
    "DatabaseToApiSyncParameters",
    "StubMinimalIntegrationArchetype",
    "StubMinimalIntegrationParameters",
]

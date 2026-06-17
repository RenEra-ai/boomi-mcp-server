"""
Component management category for Boomi MCP Server.

This category contains tools for managing Boomi components:
- Trading Partners (B2B/EDI)
- Processes
- Component Query (list, get, search, bulk_get)
- Component Management (create, update, clone, delete)
- Component Analysis (where_used, dependencies, compare_versions)

Consumers import the owning submodule directly (e.g.
``from boomi_mcp.categories.components.query_components import query_components_action``).
This package intentionally performs no eager submodule imports, so a failure in one
submodule does not cascade across the other component tool categories.
"""

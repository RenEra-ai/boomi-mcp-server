"""
Component management category for Boomi MCP Server.

This category contains tools for managing Boomi components:
- Trading Partners (B2B/EDI)
- Processes
- Component Query (list, get, search, bulk_get)
- Component Management (create, update, clone, delete)
- Component Analysis (where_used, dependencies, compare_versions)
"""

# Lazy submodule exports (PEP 562). These names used to be imported EAGERLY here,
# which meant importing ANY components submodule (or integration_builder) ran this
# __init__ first and pulled in every sibling — so one broken submodule (e.g. a missing
# analyze_component.py) cascaded into a "No module named ...analyze_component" failure
# for all four tool categories. Lazy access isolates such a failure to the single
# category that actually uses the broken submodule. Nothing imports these package-level
# re-exports today, but they are preserved (lazily) for backward compatibility.
import importlib

_LAZY_EXPORTS = {
    # Trading Partners
    'create_trading_partner': 'trading_partners',
    'get_trading_partner': 'trading_partners',
    'list_trading_partners': 'trading_partners',
    'update_trading_partner': 'trading_partners',
    'delete_trading_partner': 'trading_partners',
    'analyze_trading_partner_usage': 'trading_partners',
    'manage_trading_partner_action': 'trading_partners',
    # Processes
    'list_processes': 'processes',
    'get_process': 'processes',
    'create_process': 'processes',
    'update_process': 'processes',
    'delete_process': 'processes',
    'manage_process_action': 'processes',
    # Component Tools
    'query_components_action': 'query_components',
    'manage_component_action': 'manage_component',
    'analyze_component_action': 'analyze_component',
    'manage_connector_action': 'connectors',
}

__all__ = list(_LAZY_EXPORTS)


def __getattr__(name):
    """Import the owning submodule on first access (PEP 562)."""
    submodule = _LAZY_EXPORTS.get(name)
    if submodule is None:
        raise AttributeError(f"module {__name__!r} has no attribute {name!r}")
    return getattr(importlib.import_module(f".{submodule}", __name__), name)


def __dir__():
    return sorted(set(__all__) | set(globals()))

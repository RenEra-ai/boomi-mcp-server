"""
Component management category for Boomi MCP Server.

This category contains tools for managing Boomi components:
- Trading Partners (B2B/EDI)
- Processes
- Component Query (list, get, search, bulk_get)
- Component Management (create, update, clone, delete)
- Component Analysis (where_used, dependencies, compare_versions)
"""

from .trading_partners import (
    create_trading_partner,
    get_trading_partner,
    list_trading_partners,
    update_trading_partner,
    delete_trading_partner,
    analyze_trading_partner_usage,
    manage_trading_partner_action
)

from .processes import (
    list_processes,
    get_process,
    create_process,
    update_process,
    delete_process,
    manage_process_action
)

from .query_components import query_components_action
from .manage_component import manage_component_action
from .analyze_component import analyze_component_action

__all__ = [
    # Trading Partners
    'create_trading_partner',
    'get_trading_partner',
    'list_trading_partners',
    'update_trading_partner',
    'delete_trading_partner',
    'analyze_trading_partner_usage',
    'manage_trading_partner_action',
    # Processes
    'list_processes',
    'get_process',
    'create_process',
    'update_process',
    'delete_process',
    'manage_process_action',
    # Component Tools
    'query_components_action',
    'manage_component_action',
    'analyze_component_action',
]

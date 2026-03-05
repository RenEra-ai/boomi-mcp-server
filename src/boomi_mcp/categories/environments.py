"""
Environment Management MCP Tools for Boomi Platform.

Provides 11 environment management actions:
- list: List all environments with optional classification/name filters
- get: Get single environment by ID
- create: Create new environment with name + classification
- update: Update environment name (classification is immutable)
- delete: Delete environment (permanent)
- get_extensions: Get environment-specific config overrides
- update_extensions: Update environment extensions (partial merge by default)
- query_extensions: Query which environments have extensions configured
- stats: Environment summary by classification
- get_properties: Get persisted process properties for a runtime (async)
- update_properties: Update persisted process properties for a runtime
"""

import time
from typing import Dict, Any, Optional, List

from boomi import Boomi
from boomi.net.transport.api_error import ApiError
from boomi.models import (
    Environment as EnvironmentModel,
    EnvironmentClassification,
    EnvironmentQueryConfig,
    EnvironmentQueryConfigQueryFilter,
    EnvironmentSimpleExpression,
    EnvironmentSimpleExpressionOperator,
    EnvironmentSimpleExpressionProperty,
    EnvironmentExtensions,
    EnvironmentExtensionsQueryConfig,
    EnvironmentExtensionsQueryConfigQueryFilter,
    EnvironmentExtensionsSimpleExpression,
    EnvironmentExtensionsSimpleExpressionOperator,
    EnvironmentExtensionsSimpleExpressionProperty,
)


# ============================================================================
# Helpers
# ============================================================================

VALID_CLASSIFICATIONS = {"TEST", "PROD"}


def _env_to_dict(env) -> Dict[str, Any]:
    """Convert SDK Environment object to plain dict."""
    classification = getattr(env, 'classification', '')
    if classification and hasattr(classification, 'value'):
        classification = classification.value
    result = {
        "id": getattr(env, 'id_', ''),
        "name": getattr(env, 'name', ''),
        "classification": str(classification),
    }
    # Include optional fields only when present
    for field in ('created_by', 'created_date'):
        val = getattr(env, field, None)
        if val:
            result[field] = str(val)
    return result


def _validate_classification(value: str) -> EnvironmentClassification:
    """Validate classification string and return the SDK enum."""
    upper = value.upper()
    if upper not in VALID_CLASSIFICATIONS:
        raise ValueError(
            f"Invalid classification: '{value}'. "
            f"Valid values: {', '.join(sorted(VALID_CLASSIFICATIONS))}"
        )
    return getattr(EnvironmentClassification, upper)


def _query_all_environments(sdk: Boomi, expression) -> List[Dict[str, Any]]:
    """Execute an environment query with pagination, return list of dicts."""
    query_filter = EnvironmentQueryConfigQueryFilter(expression=expression)
    query_config = EnvironmentQueryConfig(query_filter=query_filter)
    result = sdk.environment.query_environment(request_body=query_config)

    environments = []
    if hasattr(result, 'result') and result.result:
        environments.extend([_env_to_dict(e) for e in result.result])

    while hasattr(result, 'query_token') and result.query_token:
        result = sdk.environment.query_more_environment(request_body=result.query_token)
        if hasattr(result, 'result') and result.result:
            environments.extend([_env_to_dict(e) for e in result.result])

    return environments


def _sdk_to_dict(obj):
    """Recursively convert SDK model objects to JSON-serializable dicts."""
    if isinstance(obj, list):
        return [_sdk_to_dict(item) for item in obj]
    if isinstance(obj, dict):
        return {k: _sdk_to_dict(v) for k, v in obj.items()}
    if hasattr(obj, '__dict__') and not isinstance(obj, (str, int, float, bool)):
        return {k: _sdk_to_dict(v) for k, v in obj.__dict__.items() if not k.startswith('_')}
    return obj


def _extract_api_error_msg(e: ApiError) -> str:
    """Extract user-friendly error message from ApiError."""
    detail = getattr(e, 'error_detail', None)
    if detail:
        return detail
    resp = getattr(e, 'response', None)
    if resp:
        body = getattr(resp, 'body', None)
        if isinstance(body, dict):
            msg = body.get("message", "")
            if msg:
                return msg
    return getattr(e, 'message', '') or str(e)


def _extract_raw_extensions(result) -> Dict[str, Any]:
    """Extract raw extensions dict from SDK response (handles _kwargs wrapping)."""
    if hasattr(result, '_kwargs') and 'EnvironmentExtensions' in result._kwargs:
        return result._kwargs['EnvironmentExtensions']
    elif hasattr(result, '_kwargs') and result._kwargs:
        return result._kwargs
    elif hasattr(result, 'to_dict'):
        return result.to_dict()
    elif isinstance(result, dict):
        return result
    return {}


def _parse_extensions_response(result) -> Dict[str, Any]:
    """Parse the nested extensions response from the SDK."""
    data = _extract_raw_extensions(result)

    summary = {
        "environment_id": data.get('environmentId', getattr(result, 'environment_id', '')),
    }

    # Parse each extension type
    ext_types = {
        "connections": ("connections", "connection"),
        "operations": ("operations", "operation"),
        "properties": ("properties", "property"),
        "cross_references": ("crossReferences", "crossReference"),
        "trading_partners": ("tradingPartners", "tradingPartner"),
        "pgp_certificates": ("PGPCertificates", "PGPCertificate"),
        "process_properties": ("processProperties", "ProcessProperty"),
        "data_maps": ("dataMaps", "dataMap"),
    }

    for key, (outer, inner) in ext_types.items():
        section = data.get(outer, {})
        if isinstance(section, dict):
            items = section.get(inner, [])
        else:
            items = []
        if not isinstance(items, list):
            items = [items] if items else []
        summary[key] = {"count": len(items), "items": items}

    return summary


def _deep_merge(base: dict, override: dict) -> dict:
    """Deep merge override into base dict. Override values take precedence.

    For lists of dicts, items are matched by '@id' or 'id' key and merged
    individually so that sibling items in the base are preserved.
    """
    merged = base.copy()
    for key, value in override.items():
        if key in merged and isinstance(merged[key], dict) and isinstance(value, dict):
            merged[key] = _deep_merge(merged[key], value)
        elif key in merged and isinstance(merged[key], list) and isinstance(value, list):
            merged[key] = _merge_lists(merged[key], value)
        else:
            merged[key] = value
    return merged


def _merge_lists(base_list: list, override_list: list) -> list:
    """Merge two lists of dicts by matching on '@id' or 'id' fields.

    Override items with a matching ID are merged into the base item.
    Override items without a match are appended.
    Base items without a match are preserved.
    Falls back to replacement if items aren't dicts or have no ID key.
    """
    if not base_list or not override_list:
        return override_list if override_list else base_list

    # Check if items are dicts with an ID key
    id_key = None
    sample = base_list[0] if base_list else override_list[0]
    if isinstance(sample, dict):
        if '@id' in sample:
            id_key = '@id'
        elif 'id' in sample:
            id_key = 'id'

    if not id_key:
        # No ID field to match on — replace wholesale
        return override_list

    base_by_id = {item[id_key]: item for item in base_list if isinstance(item, dict) and id_key in item}
    seen_ids = set()

    result = []
    # Merge overrides into base items (preserve base order)
    override_by_id = {item[id_key]: item for item in override_list if isinstance(item, dict) and id_key in item}
    for item in base_list:
        if not isinstance(item, dict) or id_key not in item:
            result.append(item)
            continue
        item_id = item[id_key]
        if item_id in override_by_id:
            result.append(_deep_merge(item, override_by_id[item_id]))
            seen_ids.add(item_id)
        else:
            result.append(item)

    # Append new items from override that weren't in base
    for item in override_list:
        if isinstance(item, dict) and id_key in item and item[id_key] not in seen_ids:
            result.append(item)

    return result


# ============================================================================
# Action Handlers
# ============================================================================

def _action_list(sdk: Boomi, profile: str, **kwargs) -> Dict[str, Any]:
    """List all environments with optional classification or name filter."""
    classification = kwargs.get("classification")
    name_pattern = kwargs.get("name_pattern")

    if classification:
        # Validate classification
        upper = classification.upper()
        if upper not in VALID_CLASSIFICATIONS:
            return {
                "_success": False,
                "error": f"Invalid classification filter: '{classification}'. "
                         f"Valid values: {', '.join(sorted(VALID_CLASSIFICATIONS))}",
            }
        expression = EnvironmentSimpleExpression(
            operator=EnvironmentSimpleExpressionOperator.EQUALS,
            property=EnvironmentSimpleExpressionProperty.CLASSIFICATION,
            argument=[upper],
        )
    elif name_pattern:
        expression = EnvironmentSimpleExpression(
            operator=EnvironmentSimpleExpressionOperator.LIKE,
            property=EnvironmentSimpleExpressionProperty.NAME,
            argument=[name_pattern],
        )
    else:
        expression = EnvironmentSimpleExpression(
            operator=EnvironmentSimpleExpressionOperator.ISNOTNULL,
            property=EnvironmentSimpleExpressionProperty.ID,
            argument=[],
        )

    environments = _query_all_environments(sdk, expression)

    return {
        "_success": True,
        "environments": environments,
        "total_count": len(environments),
    }


def _action_get(sdk: Boomi, profile: str, **kwargs) -> Dict[str, Any]:
    """Get a single environment by ID."""
    resource_id = kwargs.get("resource_id")
    if not resource_id:
        return {"_success": False, "error": "resource_id is required for 'get' action"}

    env = sdk.environment.get_environment(id_=resource_id)
    return {
        "_success": True,
        "environment": _env_to_dict(env),
    }


def _action_create(sdk: Boomi, profile: str, **kwargs) -> Dict[str, Any]:
    """Create a new environment with name and classification."""
    name = kwargs.get("name")
    classification = kwargs.get("classification", "TEST")

    if not name:
        return {"_success": False, "error": "name is required for 'create' action"}

    class_enum = _validate_classification(classification)

    new_env = EnvironmentModel(
        name=name,
        classification=class_enum,
    )
    created = sdk.environment.create_environment(request_body=new_env)

    return {
        "_success": True,
        "environment": _env_to_dict(created),
    }


def _action_update(sdk: Boomi, profile: str, **kwargs) -> Dict[str, Any]:
    """Update environment name (classification is immutable)."""
    resource_id = kwargs.get("resource_id")
    name = kwargs.get("name")
    classification = kwargs.get("classification")

    if not resource_id:
        return {"_success": False, "error": "resource_id is required for 'update' action"}
    if not name:
        return {"_success": False, "error": "name is required for 'update' action (the new environment name)"}
    if classification:
        return {
            "_success": False,
            "error": "Classification is immutable after creation and cannot be changed. Only 'name' can be updated.",
        }

    # GET current environment to preserve classification (required in PUT body)
    current = sdk.environment.get_environment(id_=resource_id)

    update_request = EnvironmentModel(
        id_=resource_id,
        name=name,
        classification=current.classification,
    )
    updated = sdk.environment.update_environment(
        id_=resource_id,
        request_body=update_request,
    )

    return {
        "_success": True,
        "environment": _env_to_dict(updated),
    }


def _action_delete(sdk: Boomi, profile: str, **kwargs) -> Dict[str, Any]:
    """Delete an environment (permanent — cannot be undone)."""
    resource_id = kwargs.get("resource_id")
    if not resource_id:
        return {"_success": False, "error": "resource_id is required for 'delete' action"}

    # Get info first for the response message
    env = sdk.environment.get_environment(id_=resource_id)
    env_dict = _env_to_dict(env)

    sdk.environment.delete_environment(id_=resource_id)

    return {
        "_success": True,
        "deleted_environment": env_dict,
        "warning": "Environment deletion is permanent and cannot be undone.",
    }


def _action_get_extensions(sdk: Boomi, profile: str, **kwargs) -> Dict[str, Any]:
    """Get environment-specific configuration overrides."""
    resource_id = kwargs.get("resource_id")
    if not resource_id:
        return {"_success": False, "error": "resource_id is required for 'get_extensions' action"}

    result = sdk.environment_extensions.get_environment_extensions(id_=resource_id)
    extensions = _parse_extensions_response(result)

    return {
        "_success": True,
        "extensions": extensions,
    }


def _action_update_extensions(sdk: Boomi, profile: str, **kwargs) -> Dict[str, Any]:
    """Update environment extensions (partial merge by default)."""
    resource_id = kwargs.get("resource_id")
    extensions_data = kwargs.get("extensions")
    partial = kwargs.get("partial", True)

    if not resource_id:
        return {"_success": False, "error": "resource_id is required for 'update_extensions' action"}
    if extensions_data is None:
        return {"_success": False, "error": "extensions dict is required for 'update_extensions' action"}

    if partial:
        # GET current extensions first, then merge
        try:
            current_result = sdk.environment_extensions.get_environment_extensions(id_=resource_id)
            current_data = _extract_raw_extensions(current_result)
            merged = _deep_merge(current_data, extensions_data)
        except ApiError as e:
            status = getattr(e, 'status', None)
            if status == 404:
                # Environment not found or no extensions resource — safe fallback
                merged = extensions_data
            else:
                # Abort on any other error (including 400 = no extensible components
                # deployed, 500 = transient) to avoid destructive partial update.
                # Note: 400 typically means no extensions exist, in which case the
                # subsequent UPDATE would also fail — better to surface it here.
                return {
                    "_success": False,
                    "error": f"Failed to read current extensions for partial merge (HTTP {status}). "
                             f"Aborting to avoid data loss. If no extensions exist yet, deploy a "
                             f"process with extensible components first. Detail: {e}",
                }
    else:
        merged = extensions_data

    # Build the extensions update object
    merged['environmentId'] = resource_id
    extensions_update = EnvironmentExtensions(**merged)

    result = sdk.environment_extensions.update_environment_extensions(
        id_=resource_id,
        request_body=extensions_update,
    )

    return {
        "_success": True,
        "extensions": _parse_extensions_response(result),
        "partial_update": partial,
    }


def _action_query_extensions(sdk: Boomi, profile: str, **kwargs) -> Dict[str, Any]:
    """Query which environments have extensions configured."""
    resource_id = kwargs.get("resource_id")
    if not resource_id:
        return {"_success": False, "error": "resource_id is required for 'query_extensions' action"}

    expression = EnvironmentExtensionsSimpleExpression(
        operator=EnvironmentExtensionsSimpleExpressionOperator.EQUALS,
        property=EnvironmentExtensionsSimpleExpressionProperty.ENVIRONMENTID,
        argument=[resource_id],
    )
    query_filter = EnvironmentExtensionsQueryConfigQueryFilter(expression=expression)
    query_config = EnvironmentExtensionsQueryConfig(query_filter=query_filter)
    result = sdk.environment_extensions.query_environment_extensions(request_body=query_config)

    entries = []
    if hasattr(result, 'result') and result.result:
        for entry in result.result:
            entries.append(_parse_extensions_response(entry))

    while hasattr(result, 'query_token') and result.query_token:
        result = sdk.environment_extensions.query_more_environment_extensions(
            request_body=result.query_token
        )
        if hasattr(result, 'result') and result.result:
            for entry in result.result:
                entries.append(_parse_extensions_response(entry))

    return {
        "_success": True,
        "extensions_entries": entries,
        "total_count": len(entries),
    }


def _action_stats(sdk: Boomi, profile: str, **kwargs) -> Dict[str, Any]:
    """Environment summary by classification."""
    # Reuse the list logic to get all environments
    expression = EnvironmentSimpleExpression(
        operator=EnvironmentSimpleExpressionOperator.ISNOTNULL,
        property=EnvironmentSimpleExpressionProperty.ID,
        argument=[],
    )
    environments = _query_all_environments(sdk, expression)

    by_classification = {}
    for env in environments:
        cls = env.get("classification", "Unknown")
        by_classification[cls] = by_classification.get(cls, 0) + 1

    return {
        "_success": True,
        "total": len(environments),
        "by_classification": by_classification,
        "environments": environments,
    }


def _action_get_properties(sdk: Boomi, profile: str, **kwargs) -> Dict[str, Any]:
    """Get persisted process properties for a runtime/atom (async operation)."""
    resource_id = kwargs.get("resource_id")
    if not resource_id:
        return {"_success": False, "error": "resource_id (atom_id) is required for 'get_properties' action"}

    timeout = kwargs.get("timeout", 30)

    # Initiate async request
    token_result = sdk.persisted_process_properties.async_get_persisted_process_properties(
        id_=resource_id
    )

    if not hasattr(token_result, 'async_token') or not token_result.async_token:
        return {"_success": False, "error": "Failed to get async token for persisted properties request"}

    token = token_result.async_token.token

    # Poll for results
    start_time = time.time()
    poll_interval = 2

    while time.time() - start_time < timeout:
        time.sleep(poll_interval)
        try:
            response = sdk.persisted_process_properties.async_token_persisted_process_properties(
                token=token
            )
            if response:
                return {
                    "_success": True,
                    "atom_id": resource_id,
                    "properties": _sdk_to_dict(response),
                }
        except Exception as e:
            if "202" in str(e) or "not ready" in str(e).lower():
                continue
            return {"_success": False, "error": f"Error polling for properties: {e}"}

    return {"_success": False, "error": f"Timeout after {timeout}s waiting for persisted properties"}


def _action_update_properties(sdk: Boomi, profile: str, **kwargs) -> Dict[str, Any]:
    """Update persisted process properties for a runtime/atom."""
    resource_id = kwargs.get("resource_id")
    if not resource_id:
        return {"_success": False, "error": "resource_id (atom_id) is required for 'update_properties' action"}

    properties = kwargs.get("properties")
    if not properties:
        return {
            "_success": False,
            "error": "config must include 'properties' — a dict of process property fields. "
                     "Example: {\"ProcessProperty\": [{\"Name\": \"prop1\", \"Value\": \"val1\"}], "
                     "\"processId\": \"<process-id>\"}",
        }

    if not isinstance(properties, dict):
        return {
            "_success": False,
            "error": f"'properties' must be a dict, got {type(properties).__name__}. "
                     "Expected format: {\"ProcessProperty\": [{\"Name\": \"prop1\", \"Value\": \"val1\"}], "
                     "\"processId\": \"<process-id>\"}",
        }

    from boomi.models import PersistedProcessProperties

    properties_obj = PersistedProcessProperties(
        atom_id=resource_id,
        **{k: v for k, v in properties.items()},
    )

    result = sdk.persisted_process_properties.update_persisted_process_properties(
        id_=resource_id,
        request_body=properties_obj,
    )

    return {
        "_success": True,
        "atom_id": resource_id,
        "message": "Persisted process properties updated successfully",
    }


# ============================================================================
# Action Router
# ============================================================================

def manage_environments_action(
    sdk: Boomi,
    profile: str,
    action: str,
    config_data: Optional[Dict[str, Any]] = None,
    **kwargs,
) -> Dict[str, Any]:
    """
    Route to the appropriate environment action handler.

    Args:
        sdk: Authenticated Boomi SDK client
        profile: Profile name
        action: One of: list, get, create, update, delete, get_extensions, update_extensions, query_extensions, stats, get_properties, update_properties
        config_data: Action-specific configuration dict
        **kwargs: Additional parameters (resource_id, etc.)
    """
    if config_data is None:
        config_data = {}

    # Merge config_data into kwargs
    merged = {**config_data, **kwargs}

    actions = {
        "list": _action_list,
        "get": _action_get,
        "create": _action_create,
        "update": _action_update,
        "delete": _action_delete,
        "get_extensions": _action_get_extensions,
        "update_extensions": _action_update_extensions,
        "query_extensions": _action_query_extensions,
        "stats": _action_stats,
        "get_properties": _action_get_properties,
        "update_properties": _action_update_properties,
    }

    handler = actions.get(action)
    if not handler:
        return {
            "_success": False,
            "error": f"Unknown action: {action}",
            "valid_actions": list(actions.keys()),
        }

    try:
        return handler(sdk, profile, **merged)
    except ApiError as e:
        return {
            "_success": False,
            "error": f"Action '{action}' failed: {_extract_api_error_msg(e)}",
            "exception_type": "ApiError",
        }
    except Exception as e:
        return {
            "_success": False,
            "error": f"Action '{action}' failed: {str(e)}",
            "exception_type": type(e).__name__,
        }

"""
Integration Pack Management MCP Tool for Boomi Platform.

Provides 19 integration pack management actions:
- list_packs / get_pack: Query and get integration packs
- list_publisher_packs / get_publisher_pack / create_publisher_pack / update_publisher_pack / delete_publisher_pack: Publisher pack CRUD
- list_instances / install_instance / uninstall_instance: Instance management
- release_pack / update_release / get_release_status: Release lifecycle
- list_atom_attachments / attach_atom / detach_atom: Atom attachments
- list_environment_attachments / attach_environment / detach_environment: Environment attachments
"""

from typing import Dict, Any, Optional, List, Callable

from boomi import Boomi
from boomi.net.transport.api_error import ApiError
from boomi.models import (
    IntegrationPack,
    IntegrationPackQueryConfig,
    IntegrationPackQueryConfigQueryFilter,
    IntegrationPackSimpleExpression,
    IntegrationPackSimpleExpressionOperator,
    IntegrationPackSimpleExpressionProperty,
    PublisherIntegrationPack,
    PublisherIntegrationPackInstallationType,
    PublisherIntegrationPackQueryConfig,
    IntegrationPackInstance,
    IntegrationPackInstanceQueryConfig,
    IntegrationPackInstanceQueryConfigQueryFilter,
    IntegrationPackInstanceSimpleExpression,
    IntegrationPackInstanceSimpleExpressionOperator,
    IntegrationPackInstanceSimpleExpressionProperty,
    ReleaseIntegrationPack,
    ReleaseIntegrationPackReleaseSchedule,
    ReleaseIntegrationPackStatus,
    IntegrationPackAtomAttachment,
    IntegrationPackAtomAttachmentQueryConfig,
    IntegrationPackAtomAttachmentQueryConfigQueryFilter,
    IntegrationPackAtomAttachmentSimpleExpression,
    IntegrationPackAtomAttachmentSimpleExpressionOperator,
    IntegrationPackAtomAttachmentSimpleExpressionProperty,
    IntegrationPackEnvironmentAttachment,
    IntegrationPackEnvironmentAttachmentQueryConfig,
    IntegrationPackEnvironmentAttachmentQueryConfigQueryFilter,
    IntegrationPackEnvironmentAttachmentSimpleExpression,
    IntegrationPackEnvironmentAttachmentSimpleExpressionOperator,
    IntegrationPackEnvironmentAttachmentSimpleExpressionProperty,
)


# ============================================================================
# Helpers
# ============================================================================

def _extract_api_error_msg(e) -> str:
    """Extract user-friendly error message from ApiError."""
    detail = getattr(e, "error_detail", None)
    if detail:
        return detail
    resp = getattr(e, "response", None)
    if resp:
        body = getattr(resp, "body", None)
        if isinstance(body, dict):
            msg = body.get("message", "")
            if msg:
                return msg
    return getattr(e, "message", "") or str(e)


def _obj_to_dict(obj) -> Dict[str, Any]:
    """Generic serializer: convert an SDK model object to a plain dict."""
    if obj is None:
        return {}
    d = {}
    for attr in dir(obj):
        if attr.startswith("_"):
            continue
        val = getattr(obj, attr, None)
        if callable(val):
            continue
        if hasattr(val, "value"):
            # Enum
            val = val.value
        elif hasattr(val, "__dict__") and not isinstance(val, (str, int, float, bool)):
            val = _obj_to_dict(val)
        elif isinstance(val, list):
            val = [_obj_to_dict(item) if hasattr(item, "__dict__") else item for item in val]
        d[attr] = val
    return d


def _query_all(
    query_fn: Callable,
    query_more_fn: Callable,
    query_config,
) -> List[Dict[str, Any]]:
    """Execute a paginated query, return list of dicts."""
    result = query_fn(request_body=query_config)

    items: List[Dict[str, Any]] = []
    if hasattr(result, "result") and result.result:
        items.extend([_obj_to_dict(r) for r in result.result])

    while hasattr(result, "query_token") and result.query_token:
        result = query_more_fn(request_body=result.query_token)
        if hasattr(result, "result") and result.result:
            items.extend([_obj_to_dict(r) for r in result.result])

    return items


# ============================================================================
# Action Handlers -- Integration Packs (read-only)
# ============================================================================

def _action_list_packs(sdk: Boomi, profile: str, **kwargs) -> Dict[str, Any]:
    """Query all integration packs, with optional name filter."""
    name = kwargs.get("name")

    if name:
        expression = IntegrationPackSimpleExpression(
            operator=IntegrationPackSimpleExpressionOperator.LIKE,
            property=IntegrationPackSimpleExpressionProperty.NAME,
            argument=[name],
        )
        query_filter = IntegrationPackQueryConfigQueryFilter(expression=expression)
        query_config = IntegrationPackQueryConfig(query_filter=query_filter)
    else:
        query_config = None  # SDK accepts None for unfiltered query

    packs = _query_all(
        sdk.integration_pack.query_integration_pack,
        sdk.integration_pack.query_more_integration_pack,
        query_config,
    )

    return {
        "_success": True,
        "packs": packs,
        "total_count": len(packs),
    }


def _action_get_pack(sdk: Boomi, profile: str, **kwargs) -> Dict[str, Any]:
    """Get a specific integration pack by ID."""
    resource_id = kwargs.get("resource_id")
    if not resource_id:
        return {"_success": False, "error": "resource_id is required for 'get_pack' action."}

    result = sdk.integration_pack.get_integration_pack(id_=resource_id)
    return {
        "_success": True,
        "pack": _obj_to_dict(result),
    }


# ============================================================================
# Action Handlers -- Publisher Integration Packs (CRUD)
# ============================================================================

def _action_list_publisher_packs(sdk: Boomi, profile: str, **kwargs) -> Dict[str, Any]:
    """Query all publisher integration packs."""
    query_config = None  # SDK accepts None for unfiltered query

    packs = _query_all(
        sdk.publisher_integration_pack.query_publisher_integration_pack,
        sdk.publisher_integration_pack.query_more_publisher_integration_pack,
        query_config,
    )

    return {
        "_success": True,
        "publisher_packs": packs,
        "total_count": len(packs),
    }


def _action_get_publisher_pack(sdk: Boomi, profile: str, **kwargs) -> Dict[str, Any]:
    """Get a specific publisher integration pack by ID."""
    resource_id = kwargs.get("resource_id")
    if not resource_id:
        return {"_success": False, "error": "resource_id is required for 'get_publisher_pack' action."}

    result = sdk.publisher_integration_pack.get_publisher_integration_pack(id_=resource_id)
    return {
        "_success": True,
        "publisher_pack": _obj_to_dict(result),
    }


def _action_create_publisher_pack(sdk: Boomi, profile: str, **kwargs) -> Dict[str, Any]:
    """Create a new publisher integration pack."""
    name = kwargs.get("name")
    description = kwargs.get("description")
    installation_type = kwargs.get("installation_type", "SINGLE")

    if not name:
        return {"_success": False, "error": "name is required for 'create_publisher_pack' action."}
    if not description:
        return {"_success": False, "error": "description is required for 'create_publisher_pack' action."}

    pack = PublisherIntegrationPack(
        name=name,
        description=description,
        installation_type=PublisherIntegrationPackInstallationType(installation_type),
    )

    result = sdk.publisher_integration_pack.create_publisher_integration_pack(
        request_body=pack,
    )

    return {
        "_success": True,
        "publisher_pack": _obj_to_dict(result),
    }


def _action_update_publisher_pack(sdk: Boomi, profile: str, **kwargs) -> Dict[str, Any]:
    """Update an existing publisher integration pack."""
    resource_id = kwargs.get("resource_id")
    if not resource_id:
        return {"_success": False, "error": "resource_id is required for 'update_publisher_pack' action."}

    description = kwargs.get("description")
    if not description:
        return {"_success": False, "error": "description is required for 'update_publisher_pack' action."}

    build_kwargs: Dict[str, Any] = {"description": description}
    name = kwargs.get("name")
    if name:
        build_kwargs["name"] = name
    operation_type = kwargs.get("operation_type")
    if operation_type:
        build_kwargs["operation_type"] = operation_type

    pack = PublisherIntegrationPack(**build_kwargs)

    result = sdk.publisher_integration_pack.update_publisher_integration_pack(
        id_=resource_id,
        request_body=pack,
    )

    return {
        "_success": True,
        "publisher_pack": _obj_to_dict(result),
    }


def _action_delete_publisher_pack(sdk: Boomi, profile: str, **kwargs) -> Dict[str, Any]:
    """Delete a publisher integration pack by ID."""
    resource_id = kwargs.get("resource_id")
    if not resource_id:
        return {"_success": False, "error": "resource_id is required for 'delete_publisher_pack' action."}

    sdk.publisher_integration_pack.delete_publisher_integration_pack(id_=resource_id)

    return {
        "_success": True,
        "deleted_id": resource_id,
        "note": "Publisher integration pack deleted. It is automatically uninstalled from all accounts.",
    }


# ============================================================================
# Action Handlers -- Integration Pack Instances
# ============================================================================

def _action_list_instances(sdk: Boomi, profile: str, **kwargs) -> Dict[str, Any]:
    """Query integration pack instances, with optional integration_pack_id filter."""
    integration_pack_id = kwargs.get("integration_pack_id")

    if integration_pack_id:
        expression = IntegrationPackInstanceSimpleExpression(
            operator=IntegrationPackInstanceSimpleExpressionOperator.EQUALS,
            property=IntegrationPackInstanceSimpleExpressionProperty.INTEGRATIONPACKID,
            argument=[integration_pack_id],
        )
        query_filter = IntegrationPackInstanceQueryConfigQueryFilter(expression=expression)
        query_config = IntegrationPackInstanceQueryConfig(query_filter=query_filter)
    else:
        query_config = None

    instances = _query_all(
        sdk.integration_pack_instance.query_integration_pack_instance,
        sdk.integration_pack_instance.query_more_integration_pack_instance,
        query_config,
    )

    return {
        "_success": True,
        "instances": instances,
        "total_count": len(instances),
    }


def _action_install_instance(sdk: Boomi, profile: str, **kwargs) -> Dict[str, Any]:
    """Install (create) an integration pack instance."""
    integration_pack_id = kwargs.get("integration_pack_id")
    if not integration_pack_id:
        return {"_success": False, "error": "integration_pack_id is required for 'install_instance' action."}

    build_kwargs: Dict[str, Any] = {"integration_pack_id": integration_pack_id}
    override_name = kwargs.get("integration_pack_override_name")
    if override_name:
        build_kwargs["integration_pack_override_name"] = override_name

    instance = IntegrationPackInstance(**build_kwargs)

    result = sdk.integration_pack_instance.create_integration_pack_instance(
        request_body=instance,
    )

    return {
        "_success": True,
        "instance": _obj_to_dict(result),
    }


def _action_uninstall_instance(sdk: Boomi, profile: str, **kwargs) -> Dict[str, Any]:
    """Uninstall (delete) an integration pack instance by ID."""
    resource_id = kwargs.get("resource_id")
    if not resource_id:
        return {"_success": False, "error": "resource_id is required for 'uninstall_instance' action."}

    sdk.integration_pack_instance.delete_integration_pack_instance(id_=resource_id)

    return {
        "_success": True,
        "deleted_id": resource_id,
        "note": "Integration pack instance uninstalled.",
    }


# ============================================================================
# Action Handlers -- Release Lifecycle
# ============================================================================

def _action_release_pack(sdk: Boomi, profile: str, **kwargs) -> Dict[str, Any]:
    """Create a release for a publisher integration pack."""
    pack_id = kwargs.get("integration_pack_id") or kwargs.get("resource_id")
    if not pack_id:
        return {
            "_success": False,
            "error": "integration_pack_id (or resource_id) is required for 'release_pack' action.",
        }

    release_schedule = kwargs.get("release_schedule", "IMMEDIATELY")
    release_on_date = kwargs.get("release_on_date")

    build_kwargs: Dict[str, Any] = {
        "id_": pack_id,
        "release_schedule": ReleaseIntegrationPackReleaseSchedule(release_schedule),
    }
    if release_on_date:
        build_kwargs["release_on_date"] = release_on_date

    name = kwargs.get("name")
    if name:
        build_kwargs["name"] = name

    release = ReleaseIntegrationPack(**build_kwargs)

    result = sdk.release_integration_pack.create_release_integration_pack(
        request_body=release,
    )

    return {
        "_success": True,
        "release": _obj_to_dict(result),
    }


def _action_update_release(sdk: Boomi, profile: str, **kwargs) -> Dict[str, Any]:
    """Update a scheduled release for a publisher integration pack."""
    resource_id = kwargs.get("resource_id")
    if not resource_id:
        return {"_success": False, "error": "resource_id is required for 'update_release' action."}

    build_kwargs: Dict[str, Any] = {}
    release_schedule = kwargs.get("release_schedule")
    if release_schedule:
        build_kwargs["release_schedule"] = ReleaseIntegrationPackReleaseSchedule(release_schedule)
    release_on_date = kwargs.get("release_on_date")
    if release_on_date:
        build_kwargs["release_on_date"] = release_on_date
    name = kwargs.get("name")
    if name:
        build_kwargs["name"] = name

    release = ReleaseIntegrationPack(**build_kwargs)

    result = sdk.release_integration_pack.update_release_integration_pack(
        id_=resource_id,
        request_body=release,
    )

    return {
        "_success": True,
        "release": _obj_to_dict(result),
    }


def _action_get_release_status(sdk: Boomi, profile: str, **kwargs) -> Dict[str, Any]:
    """Get the release status by request_id."""
    resource_id = kwargs.get("resource_id") or kwargs.get("request_id")
    if not resource_id:
        return {
            "_success": False,
            "error": "resource_id (or request_id) is required for 'get_release_status' action.",
        }

    result = sdk.release_integration_pack_status.get_release_integration_pack_status(
        id_=resource_id,
    )

    return {
        "_success": True,
        "release_status": _obj_to_dict(result),
    }


# ============================================================================
# Action Handlers -- Atom Attachments
# ============================================================================

def _action_list_atom_attachments(sdk: Boomi, profile: str, **kwargs) -> Dict[str, Any]:
    """Query atom attachments, with optional instance_id or atom_id filter."""
    instance_id = kwargs.get("integration_pack_instance_id")
    atom_id = kwargs.get("atom_id")

    if instance_id:
        expression = IntegrationPackAtomAttachmentSimpleExpression(
            operator=IntegrationPackAtomAttachmentSimpleExpressionOperator.EQUALS,
            property=IntegrationPackAtomAttachmentSimpleExpressionProperty.INTEGRATIONPACKINSTANCEID,
            argument=[instance_id],
        )
        query_filter = IntegrationPackAtomAttachmentQueryConfigQueryFilter(expression=expression)
        query_config = IntegrationPackAtomAttachmentQueryConfig(query_filter=query_filter)
    elif atom_id:
        expression = IntegrationPackAtomAttachmentSimpleExpression(
            operator=IntegrationPackAtomAttachmentSimpleExpressionOperator.EQUALS,
            property=IntegrationPackAtomAttachmentSimpleExpressionProperty.ATOMID,
            argument=[atom_id],
        )
        query_filter = IntegrationPackAtomAttachmentQueryConfigQueryFilter(expression=expression)
        query_config = IntegrationPackAtomAttachmentQueryConfig(query_filter=query_filter)
    else:
        query_config = None

    attachments = _query_all(
        sdk.integration_pack_atom_attachment.query_integration_pack_atom_attachment,
        sdk.integration_pack_atom_attachment.query_more_integration_pack_atom_attachment,
        query_config,
    )

    return {
        "_success": True,
        "atom_attachments": attachments,
        "total_count": len(attachments),
    }


def _action_attach_atom(sdk: Boomi, profile: str, **kwargs) -> Dict[str, Any]:
    """Attach an integration pack instance to an atom."""
    instance_id = kwargs.get("integration_pack_instance_id")
    atom_id = kwargs.get("atom_id")

    if not instance_id:
        return {"_success": False, "error": "integration_pack_instance_id is required for 'attach_atom' action."}
    if not atom_id:
        return {"_success": False, "error": "atom_id is required for 'attach_atom' action."}

    attachment = IntegrationPackAtomAttachment(
        integration_pack_instance_id=instance_id,
        atom_id=atom_id,
    )

    result = sdk.integration_pack_atom_attachment.create_integration_pack_atom_attachment(
        request_body=attachment,
    )

    return {
        "_success": True,
        "atom_attachment": _obj_to_dict(result),
    }


def _action_detach_atom(sdk: Boomi, profile: str, **kwargs) -> Dict[str, Any]:
    """Detach an integration pack instance from an atom."""
    resource_id = kwargs.get("resource_id")
    if not resource_id:
        return {"_success": False, "error": "resource_id is required for 'detach_atom' action."}

    sdk.integration_pack_atom_attachment.delete_integration_pack_atom_attachment(
        id_=resource_id,
    )

    return {
        "_success": True,
        "deleted_id": resource_id,
        "note": "Integration pack instance detached from atom.",
    }


# ============================================================================
# Action Handlers -- Environment Attachments
# ============================================================================

def _action_list_environment_attachments(sdk: Boomi, profile: str, **kwargs) -> Dict[str, Any]:
    """Query environment attachments, with optional instance_id or environment_id filter."""
    instance_id = kwargs.get("integration_pack_instance_id")
    environment_id = kwargs.get("environment_id")

    if instance_id:
        expression = IntegrationPackEnvironmentAttachmentSimpleExpression(
            operator=IntegrationPackEnvironmentAttachmentSimpleExpressionOperator.EQUALS,
            property=IntegrationPackEnvironmentAttachmentSimpleExpressionProperty.INTEGRATIONPACKINSTANCEID,
            argument=[instance_id],
        )
        query_filter = IntegrationPackEnvironmentAttachmentQueryConfigQueryFilter(expression=expression)
        query_config = IntegrationPackEnvironmentAttachmentQueryConfig(query_filter=query_filter)
    elif environment_id:
        expression = IntegrationPackEnvironmentAttachmentSimpleExpression(
            operator=IntegrationPackEnvironmentAttachmentSimpleExpressionOperator.EQUALS,
            property=IntegrationPackEnvironmentAttachmentSimpleExpressionProperty.ENVIRONMENTID,
            argument=[environment_id],
        )
        query_filter = IntegrationPackEnvironmentAttachmentQueryConfigQueryFilter(expression=expression)
        query_config = IntegrationPackEnvironmentAttachmentQueryConfig(query_filter=query_filter)
    else:
        query_config = None

    attachments = _query_all(
        sdk.integration_pack_environment_attachment.query_integration_pack_environment_attachment,
        sdk.integration_pack_environment_attachment.query_more_integration_pack_environment_attachment,
        query_config,
    )

    return {
        "_success": True,
        "environment_attachments": attachments,
        "total_count": len(attachments),
    }


def _action_attach_environment(sdk: Boomi, profile: str, **kwargs) -> Dict[str, Any]:
    """Attach an integration pack instance to an environment."""
    instance_id = kwargs.get("integration_pack_instance_id")
    environment_id = kwargs.get("environment_id")

    if not instance_id:
        return {"_success": False, "error": "integration_pack_instance_id is required for 'attach_environment' action."}
    if not environment_id:
        return {"_success": False, "error": "environment_id is required for 'attach_environment' action."}

    attachment = IntegrationPackEnvironmentAttachment(
        integration_pack_instance_id=instance_id,
        environment_id=environment_id,
    )

    result = sdk.integration_pack_environment_attachment.create_integration_pack_environment_attachment(
        request_body=attachment,
    )

    return {
        "_success": True,
        "environment_attachment": _obj_to_dict(result),
    }


def _action_detach_environment(sdk: Boomi, profile: str, **kwargs) -> Dict[str, Any]:
    """Detach an integration pack instance from an environment."""
    resource_id = kwargs.get("resource_id")
    if not resource_id:
        return {"_success": False, "error": "resource_id is required for 'detach_environment' action."}

    sdk.integration_pack_environment_attachment.delete_integration_pack_environment_attachment(
        id_=resource_id,
    )

    return {
        "_success": True,
        "deleted_id": resource_id,
        "note": "Integration pack instance detached from environment.",
    }


# ============================================================================
# Action Router
# ============================================================================

def manage_integration_packs_action(
    sdk: Boomi,
    profile: str,
    action: str,
    config_data: Optional[Dict[str, Any]] = None,
    **kwargs,
) -> Dict[str, Any]:
    """
    Route to the appropriate integration pack action handler.

    Args:
        sdk: Authenticated Boomi SDK client
        profile: Profile name
        action: One of the 19 supported actions
        config_data: Action-specific configuration dict
        **kwargs: Additional parameters (resource_id, etc.)
    """
    if config_data is None:
        config_data = {}

    merged = {**config_data, **kwargs}

    actions = {
        # Integration Packs (read-only)
        "list_packs": _action_list_packs,
        "get_pack": _action_get_pack,
        # Publisher Integration Packs (CRUD)
        "list_publisher_packs": _action_list_publisher_packs,
        "get_publisher_pack": _action_get_publisher_pack,
        "create_publisher_pack": _action_create_publisher_pack,
        "update_publisher_pack": _action_update_publisher_pack,
        "delete_publisher_pack": _action_delete_publisher_pack,
        # Integration Pack Instances
        "list_instances": _action_list_instances,
        "install_instance": _action_install_instance,
        "uninstall_instance": _action_uninstall_instance,
        # Release Lifecycle
        "release_pack": _action_release_pack,
        "update_release": _action_update_release,
        "get_release_status": _action_get_release_status,
        # Atom Attachments
        "list_atom_attachments": _action_list_atom_attachments,
        "attach_atom": _action_attach_atom,
        "detach_atom": _action_detach_atom,
        # Environment Attachments
        "list_environment_attachments": _action_list_environment_attachments,
        "attach_environment": _action_attach_environment,
        "detach_environment": _action_detach_environment,
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

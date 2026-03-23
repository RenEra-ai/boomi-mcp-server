"""
Listener Management MCP Tool for Boomi Platform.

Provides 4 listener management actions:
- status: Get listener statuses for a container (async operation)
- pause: Pause listeners on a container
- resume: Resume listeners on a container
- restart: Restart listeners on a container
"""

from typing import Dict, Any, Optional

from boomi import Boomi
from boomi.net.transport.api_error import ApiError
from boomi.models import (
    ListenerStatusQueryConfig,
    ListenerStatusQueryConfigQueryFilter,
    ListenerStatusSimpleExpression,
    ListenerStatusSimpleExpressionOperator,
    ListenerStatusSimpleExpressionProperty,
    ChangeListenerStatusRequest,
    Action,
)

from boomi_mcp.utils.async_polling import poll_async_result


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


def _listener_status_to_dict(listener) -> Dict[str, Any]:
    """Convert SDK ListenerStatus object to plain dict."""
    return {
        "listener_id": getattr(listener, "listener_id", ""),
        "status": getattr(listener, "status", ""),
        "connector_type": getattr(listener, "connector_type", None),
    }


# ============================================================================
# Action Handlers
# ============================================================================

def _action_status(sdk: Boomi, profile: str, **kwargs) -> Dict[str, Any]:
    """Get listener statuses for a container (async operation)."""
    resource_id = kwargs.get("resource_id")
    listener_id = kwargs.get("listener_id")

    if not resource_id:
        return {
            "_success": False,
            "error": "resource_id (container/atom ID) is required for 'status' action.",
        }

    def _build_query():
        if listener_id:
            # Filter by both containerId and listenerId is not supported in a
            # single simple expression — use containerId as the required filter
            # and post-filter by listener_id client-side.
            pass
        expression = ListenerStatusSimpleExpression(
            operator=ListenerStatusSimpleExpressionOperator.EQUALS,
            property=ListenerStatusSimpleExpressionProperty.CONTAINERID,
            argument=[resource_id],
        )
        query_filter = ListenerStatusQueryConfigQueryFilter(expression=expression)
        return ListenerStatusQueryConfig(query_filter=query_filter)

    def initiate_fn():
        query_config = _build_query()
        return sdk.listener_status.async_get_listener_status(request_body=query_config)

    def poll_fn(token):
        return sdk.listener_status.async_token_listener_status(token=token)

    response = poll_async_result(
        initiate_fn=initiate_fn,
        poll_fn=poll_fn,
        timeout=60,
        interval=2,
        resource_label="listener status query",
    )

    listeners = []
    if hasattr(response, "result") and response.result:
        listeners = [_listener_status_to_dict(ls) for ls in response.result]

    # Client-side filter by listener_id if requested
    if listener_id:
        listeners = [ls for ls in listeners if ls["listener_id"] == listener_id]

    return {
        "_success": True,
        "container_id": resource_id,
        "listeners": listeners,
        "total_count": len(listeners),
    }


def _action_pause(sdk: Boomi, profile: str, **kwargs) -> Dict[str, Any]:
    """Pause listeners on a container."""
    resource_id = kwargs.get("resource_id")
    listener_id = kwargs.get("listener_id")

    if not resource_id:
        return {
            "_success": False,
            "error": "resource_id (container ID) is required for 'pause' action.",
        }

    action = Action.PAUSE if listener_id else Action.PAUSEALL
    request_kwargs = {
        "action": action,
        "container_id": resource_id,
    }
    if listener_id:
        request_kwargs["listener_id"] = listener_id

    request = ChangeListenerStatusRequest(**request_kwargs)
    sdk.change_listener_status.create_change_listener_status(request_body=request)

    return {
        "_success": True,
        "container_id": resource_id,
        "action": "pause",
        "message": "Pause request submitted",
    }


def _action_resume(sdk: Boomi, profile: str, **kwargs) -> Dict[str, Any]:
    """Resume listeners on a container."""
    resource_id = kwargs.get("resource_id")
    listener_id = kwargs.get("listener_id")

    if not resource_id:
        return {
            "_success": False,
            "error": "resource_id (container ID) is required for 'resume' action.",
        }

    action = Action.RESUME if listener_id else Action.RESUMEALL
    request_kwargs = {
        "action": action,
        "container_id": resource_id,
    }
    if listener_id:
        request_kwargs["listener_id"] = listener_id

    request = ChangeListenerStatusRequest(**request_kwargs)
    sdk.change_listener_status.create_change_listener_status(request_body=request)

    return {
        "_success": True,
        "container_id": resource_id,
        "action": "resume",
        "message": "Resume request submitted",
    }


def _action_restart(sdk: Boomi, profile: str, **kwargs) -> Dict[str, Any]:
    """Restart listeners on a container."""
    resource_id = kwargs.get("resource_id")
    listener_id = kwargs.get("listener_id")

    if not resource_id:
        return {
            "_success": False,
            "error": "resource_id (container ID) is required for 'restart' action.",
        }

    action = Action.RESTART if listener_id else Action.RESTARTALL
    request_kwargs = {
        "action": action,
        "container_id": resource_id,
    }
    if listener_id:
        request_kwargs["listener_id"] = listener_id

    request = ChangeListenerStatusRequest(**request_kwargs)
    sdk.change_listener_status.create_change_listener_status(request_body=request)

    return {
        "_success": True,
        "container_id": resource_id,
        "action": "restart",
        "message": "Restart request submitted",
    }


# ============================================================================
# Action Router
# ============================================================================

def manage_listeners_action(
    sdk: Boomi,
    profile: str,
    action: str,
    config_data: Optional[Dict[str, Any]] = None,
    **kwargs,
) -> Dict[str, Any]:
    """
    Route to the appropriate listener action handler.

    Args:
        sdk: Authenticated Boomi SDK client
        profile: Profile name
        action: One of: status, pause, resume, restart
        config_data: Action-specific configuration dict
        **kwargs: Additional parameters (resource_id, etc.)
    """
    if config_data is None:
        config_data = {}

    merged = {**config_data, **kwargs}

    actions = {
        "status": _action_status,
        "pause": _action_pause,
        "resume": _action_resume,
        "restart": _action_restart,
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

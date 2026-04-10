"""
Process Schedules MCP Tool for Boomi Platform.

Provides 8 schedule management actions:
- list: Query all process schedules with optional process_id/atom_id filters
- get: Get specific schedule by schedule_id (or by process_id + atom_id)
- update: Update/create schedule with cron expression
- delete: Clear/disable schedule (sets empty schedule array)
- list_status: Query schedule statuses with optional process_id/atom_id filters
- get_status: Get specific schedule status by ID or process_id + atom_id
- enable: Enable a process schedule
- disable: Disable a process schedule

SDK reference: boomi-python/examples/06_configure_deployment/manage_process_schedules.py
"""

import base64
from typing import Dict, Any, Optional, List

from boomi import Boomi
from boomi.net.transport.api_error import ApiError
from boomi.models import (
    ProcessSchedules,
    ProcessSchedulesQueryConfig,
    ProcessSchedulesQueryConfigQueryFilter,
    ProcessSchedulesSimpleExpression,
    ProcessSchedulesSimpleExpressionOperator,
    ProcessSchedulesSimpleExpressionProperty,
    ProcessScheduleStatus,
    ProcessScheduleStatusQueryConfig,
    ProcessScheduleStatusQueryConfigQueryFilter,
    ProcessScheduleStatusSimpleExpression,
    ProcessScheduleStatusSimpleExpressionOperator,
    ProcessScheduleStatusSimpleExpressionProperty,
    Schedule,
    ScheduleRetry,
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


def _schedule_id_from_ids(atom_id: str, process_id: str) -> str:
    """Build the base64-encoded schedule ID from atom and process IDs.

    Boomi uses unpadded base64, so we strip trailing '=' characters.
    """
    return base64.b64encode(f"CPS{atom_id}:{process_id}".encode()).decode().rstrip("=")


def _ids_from_schedule_id(schedule_id: str) -> tuple:
    """Decode base64 schedule ID back to (atom_id, process_id).

    Raises ValueError on invalid format.
    """
    try:
        # Pad to multiple of 4 — Boomi API returns unpadded base64
        schedule_id = schedule_id.strip()
        padded = schedule_id + "=" * (-len(schedule_id) % 4)
        decoded = base64.b64decode(padded, validate=True).decode()
    except Exception as e:
        raise ValueError(f"Invalid schedule ID (not valid base64): {schedule_id}") from e
    if not decoded.startswith("CPS"):
        raise ValueError(f"Invalid schedule ID (missing CPS prefix): {decoded}")
    payload = decoded[3:]  # strip "CPS"
    if ":" not in payload:
        raise ValueError(f"Invalid schedule ID (no ':' separator): {decoded}")
    atom_id, process_id = payload.split(":", 1)
    if not atom_id or not process_id:
        raise ValueError(f"Invalid schedule ID (empty atom_id or process_id): {decoded}")
    return (atom_id, process_id)


def _schedule_to_dict(sched) -> Dict[str, str]:
    """Convert SDK Schedule object to plain dict."""
    return {
        "minutes": getattr(sched, 'minutes', '*'),
        "hours": getattr(sched, 'hours', '*'),
        "days_of_month": getattr(sched, 'days_of_month', '*'),
        "months": getattr(sched, 'months', '*'),
        "days_of_week": getattr(sched, 'days_of_week', '*'),
        "years": getattr(sched, 'years', '*'),
    }


def _process_schedule_to_dict(ps) -> Dict[str, Any]:
    """Convert SDK ProcessSchedules object to plain dict."""
    schedules = []
    if hasattr(ps, 'schedule') and ps.schedule:
        schedules = [_schedule_to_dict(s) for s in ps.schedule]

    retry = None
    if hasattr(ps, 'retry') and ps.retry:
        retry = {"max_retry": getattr(ps.retry, 'max_retry', None)}

    return {
        "id": getattr(ps, 'id_', ''),
        "process_id": getattr(ps, 'process_id', ''),
        "atom_id": getattr(ps, 'atom_id', ''),
        "schedules": schedules,
        "retry": retry,
        "active": len(schedules) > 0,
    }


def _parse_cron(cron_expr: str) -> Dict[str, str]:
    """Parse 5-part cron expression into schedule fields.

    Format: minute hour day_of_month month day_of_week
    """
    parts = cron_expr.strip().split()
    if len(parts) != 5:
        raise ValueError(
            f"Cron expression must have 5 parts (minute hour day_of_month month day_of_week), "
            f"got {len(parts)}: '{cron_expr}'"
        )
    return {
        "minutes": parts[0],
        "hours": parts[1],
        "days_of_month": parts[2],
        "months": parts[3],
        "days_of_week": parts[4],
    }


def _query_all_schedules(sdk: Boomi, query_config) -> List[Dict[str, Any]]:
    """Execute a schedule query with pagination, return list of dicts."""
    result = sdk.process_schedules.query_process_schedules(request_body=query_config)

    schedules = []
    if hasattr(result, 'result') and result.result:
        schedules.extend([_process_schedule_to_dict(s) for s in result.result])

    while hasattr(result, 'query_token') and result.query_token:
        result = sdk.process_schedules.query_more_process_schedules(
            request_body=result.query_token
        )
        if hasattr(result, 'result') and result.result:
            schedules.extend([_process_schedule_to_dict(s) for s in result.result])

    return schedules


def _schedule_status_to_dict(status) -> Dict[str, Any]:
    """Convert SDK ProcessScheduleStatus object to plain dict."""
    return {
        "id": getattr(status, 'id_', ''),
        "process_id": getattr(status, 'process_id', ''),
        "atom_id": getattr(status, 'atom_id', ''),
        "enabled": bool(getattr(status, 'enabled', False)),
    }


def _query_all_schedule_statuses(sdk: Boomi, query_config) -> List[Dict[str, Any]]:
    """Execute a schedule status query with pagination, return list of dicts."""
    result = sdk.process_schedule_status.query_process_schedule_status(
        request_body=query_config
    )

    statuses = []
    if hasattr(result, 'result') and result.result:
        statuses.extend([_schedule_status_to_dict(s) for s in result.result])

    while hasattr(result, 'query_token') and result.query_token:
        result = sdk.process_schedule_status.query_more_process_schedule_status(
            request_body=result.query_token
        )
        if hasattr(result, 'result') and result.result:
            statuses.extend([_schedule_status_to_dict(s) for s in result.result])

    return statuses


# ============================================================================
# Action Handlers
# ============================================================================

def _action_list(sdk: Boomi, profile: str, **kwargs) -> Dict[str, Any]:
    """List all process schedules with optional process_id/atom_id filters."""
    process_id = kwargs.get("process_id")
    atom_id = kwargs.get("atom_id")

    if process_id and atom_id:
        return {
            "_success": False,
            "error": "Cannot filter by both process_id and atom_id in 'list'. Use one filter, or use action='get' with both.",
        }

    if process_id:
        expression = ProcessSchedulesSimpleExpression(
            operator=ProcessSchedulesSimpleExpressionOperator.EQUALS,
            property=ProcessSchedulesSimpleExpressionProperty.PROCESSID,
            argument=[process_id],
        )
        query_filter = ProcessSchedulesQueryConfigQueryFilter(expression=expression)
        query_config = ProcessSchedulesQueryConfig(query_filter=query_filter)
    elif atom_id:
        expression = ProcessSchedulesSimpleExpression(
            operator=ProcessSchedulesSimpleExpressionOperator.EQUALS,
            property=ProcessSchedulesSimpleExpressionProperty.ATOMID,
            argument=[atom_id],
        )
        query_filter = ProcessSchedulesQueryConfigQueryFilter(expression=expression)
        query_config = ProcessSchedulesQueryConfig(query_filter=query_filter)
    else:
        # Empty query to get all schedules
        query_config = ProcessSchedulesQueryConfig()

    schedules = _query_all_schedules(sdk, query_config)

    active_count = sum(1 for s in schedules if s["active"])
    return {
        "_success": True,
        "schedules": schedules,
        "total_count": len(schedules),
        "active_count": active_count,
    }


def _action_get(sdk: Boomi, profile: str, **kwargs) -> Dict[str, Any]:
    """Get a specific process schedule by ID or by process_id + atom_id."""
    resource_id = kwargs.get("resource_id")
    process_id = kwargs.get("process_id")
    atom_id = kwargs.get("atom_id")

    if not resource_id:
        if process_id and atom_id:
            resource_id = _schedule_id_from_ids(atom_id, process_id)
        else:
            return {
                "_success": False,
                "error": "Provide either resource_id (schedule ID) or both process_id and atom_id in config.",
            }

    result = sdk.process_schedules.get_process_schedules(id_=resource_id)
    return {
        "_success": True,
        "schedule": _process_schedule_to_dict(result),
    }


def _action_update(sdk: Boomi, profile: str, **kwargs) -> Dict[str, Any]:
    """Update/create a process schedule with cron expression."""
    resource_id = kwargs.get("resource_id")
    process_id = kwargs.get("process_id")
    atom_id = kwargs.get("atom_id")
    cron = kwargs.get("cron")
    max_retry = kwargs.get("max_retry", 5)

    if not cron:
        return {"_success": False, "error": "cron expression is required for 'update' action (e.g. '0 9 * * *')"}

    if not resource_id:
        if process_id and atom_id:
            resource_id = _schedule_id_from_ids(atom_id, process_id)
        else:
            return {
                "_success": False,
                "error": "Provide either resource_id or both process_id and atom_id in config.",
            }

    # Decode resource_id to fill missing process_id/atom_id
    if not process_id or not atom_id:
        try:
            decoded_atom, decoded_process = _ids_from_schedule_id(resource_id)
        except ValueError as e:
            return {"_success": False, "error": str(e)}
        if not process_id:
            process_id = decoded_process
        if not atom_id:
            atom_id = decoded_atom

    # Parse cron expression
    try:
        cron_parts = _parse_cron(cron)
    except ValueError as e:
        return {"_success": False, "error": str(e)}

    schedule = Schedule(
        minutes=cron_parts["minutes"],
        hours=cron_parts["hours"],
        days_of_month=cron_parts["days_of_month"],
        months=cron_parts["months"],
        days_of_week=cron_parts["days_of_week"],
        years="*",
    )
    retry = ScheduleRetry(max_retry=max_retry)

    ps_kwargs = {
        "id_": resource_id,
        "process_id": process_id,
        "atom_id": atom_id,
        "schedule": [schedule],
        "retry": retry,
    }

    process_schedule = ProcessSchedules(**ps_kwargs)

    result = sdk.process_schedules.update_process_schedules(
        id_=resource_id,
        request_body=process_schedule,
    )

    return {
        "_success": True,
        "schedule": _process_schedule_to_dict(result),
        "cron_applied": cron,
    }


def _action_delete(sdk: Boomi, profile: str, **kwargs) -> Dict[str, Any]:
    """Clear/disable a process schedule (sets empty schedule array)."""
    resource_id = kwargs.get("resource_id")
    process_id = kwargs.get("process_id")
    atom_id = kwargs.get("atom_id")

    if not resource_id:
        if process_id and atom_id:
            resource_id = _schedule_id_from_ids(atom_id, process_id)
        else:
            return {
                "_success": False,
                "error": "Provide either resource_id or both process_id and atom_id in config.",
            }

    # Decode resource_id to fill missing process_id/atom_id
    if not process_id or not atom_id:
        try:
            decoded_atom, decoded_process = _ids_from_schedule_id(resource_id)
        except ValueError as e:
            return {"_success": False, "error": str(e)}
        if not process_id:
            process_id = decoded_process
        if not atom_id:
            atom_id = decoded_atom

    # Empty schedule array disables scheduling
    retry = ScheduleRetry(max_retry=5)
    ps_kwargs = {
        "id_": resource_id,
        "process_id": process_id,
        "atom_id": atom_id,
        "schedule": [],
        "retry": retry,
    }

    process_schedule = ProcessSchedules(**ps_kwargs)

    result = sdk.process_schedules.update_process_schedules(
        id_=resource_id,
        request_body=process_schedule,
    )

    return {
        "_success": True,
        "schedule": _process_schedule_to_dict(result),
        "note": "Schedule cleared. Process will no longer run automatically.",
    }


def _action_list_status(sdk: Boomi, profile: str, **kwargs) -> Dict[str, Any]:
    """List schedule statuses with optional process_id/atom_id filters."""
    process_id = kwargs.get("process_id")
    atom_id = kwargs.get("atom_id")

    if process_id:
        expression = ProcessScheduleStatusSimpleExpression(
            operator=ProcessScheduleStatusSimpleExpressionOperator.EQUALS,
            property=ProcessScheduleStatusSimpleExpressionProperty.PROCESSID,
            argument=[process_id],
        )
        query_filter = ProcessScheduleStatusQueryConfigQueryFilter(expression=expression)
        query_config = ProcessScheduleStatusQueryConfig(query_filter=query_filter)
    elif atom_id:
        expression = ProcessScheduleStatusSimpleExpression(
            operator=ProcessScheduleStatusSimpleExpressionOperator.EQUALS,
            property=ProcessScheduleStatusSimpleExpressionProperty.ATOMID,
            argument=[atom_id],
        )
        query_filter = ProcessScheduleStatusQueryConfigQueryFilter(expression=expression)
        query_config = ProcessScheduleStatusQueryConfig(query_filter=query_filter)
    else:
        expression = ProcessScheduleStatusSimpleExpression(
            operator=ProcessScheduleStatusSimpleExpressionOperator.ISNOTNULL,
            property=ProcessScheduleStatusSimpleExpressionProperty.PROCESSID,
            argument=[],
        )
        query_filter = ProcessScheduleStatusQueryConfigQueryFilter(expression=expression)
        query_config = ProcessScheduleStatusQueryConfig(query_filter=query_filter)

    statuses = _query_all_schedule_statuses(sdk, query_config)

    return {
        "_success": True,
        "statuses": statuses,
        "total_count": len(statuses),
    }


def _action_get_status(sdk: Boomi, profile: str, **kwargs) -> Dict[str, Any]:
    """Get a specific schedule status by ID or by process_id + atom_id."""
    resource_id = kwargs.get("resource_id")
    process_id = kwargs.get("process_id")
    atom_id = kwargs.get("atom_id")

    if not resource_id:
        if process_id and atom_id:
            resource_id = _schedule_id_from_ids(atom_id, process_id)
        else:
            return {
                "_success": False,
                "error": "Provide either resource_id (schedule ID) or both process_id and atom_id in config.",
            }

    result = sdk.process_schedule_status.get_process_schedule_status(id_=resource_id)
    return {
        "_success": True,
        "status": _schedule_status_to_dict(result),
    }


def _action_enable(sdk: Boomi, profile: str, **kwargs) -> Dict[str, Any]:
    """Enable a process schedule."""
    resource_id = kwargs.get("resource_id")
    process_id = kwargs.get("process_id")
    atom_id = kwargs.get("atom_id")

    if not resource_id:
        if process_id and atom_id:
            resource_id = _schedule_id_from_ids(atom_id, process_id)
        else:
            return {
                "_success": False,
                "error": "Provide either resource_id (schedule ID) or both process_id and atom_id in config.",
            }

    # Decode resource_id to fill missing process_id/atom_id (API requires all fields)
    if not process_id or not atom_id:
        try:
            decoded_atom, decoded_process = _ids_from_schedule_id(resource_id)
        except ValueError as e:
            return {"_success": False, "error": str(e)}
        if not process_id:
            process_id = decoded_process
        if not atom_id:
            atom_id = decoded_atom

    result = sdk.process_schedule_status.update_process_schedule_status(
        id_=resource_id,
        request_body=ProcessScheduleStatus(id_=resource_id, enabled=True, atom_id=atom_id, process_id=process_id),
    )
    return {
        "_success": True,
        "status": _schedule_status_to_dict(result),
        "message": "Schedule enabled",
    }


def _action_disable(sdk: Boomi, profile: str, **kwargs) -> Dict[str, Any]:
    """Disable a process schedule."""
    resource_id = kwargs.get("resource_id")
    process_id = kwargs.get("process_id")
    atom_id = kwargs.get("atom_id")

    if not resource_id:
        if process_id and atom_id:
            resource_id = _schedule_id_from_ids(atom_id, process_id)
        else:
            return {
                "_success": False,
                "error": "Provide either resource_id (schedule ID) or both process_id and atom_id in config.",
            }

    # Decode resource_id to fill missing process_id/atom_id (API requires all fields)
    if not process_id or not atom_id:
        try:
            decoded_atom, decoded_process = _ids_from_schedule_id(resource_id)
        except ValueError as e:
            return {"_success": False, "error": str(e)}
        if not process_id:
            process_id = decoded_process
        if not atom_id:
            atom_id = decoded_atom

    result = sdk.process_schedule_status.update_process_schedule_status(
        id_=resource_id,
        request_body=ProcessScheduleStatus(id_=resource_id, enabled=False, atom_id=atom_id, process_id=process_id),
    )
    return {
        "_success": True,
        "status": _schedule_status_to_dict(result),
        "message": "Schedule disabled",
    }


# ============================================================================
# Action Router
# ============================================================================

def manage_schedules_action(
    sdk: Boomi,
    profile: str,
    action: str,
    config_data: Optional[Dict[str, Any]] = None,
    **kwargs,
) -> Dict[str, Any]:
    """
    Route to the appropriate schedule action handler.

    Args:
        sdk: Authenticated Boomi SDK client
        profile: Profile name
        action: One of: list, get, update, delete, list_status, get_status, enable, disable
        config_data: Action-specific configuration dict
        **kwargs: Additional parameters (resource_id, etc.)
    """
    if config_data is None:
        config_data = {}

    merged = {**config_data, **kwargs}

    actions = {
        "list": _action_list,
        "get": _action_get,
        "update": _action_update,
        "delete": _action_delete,
        "list_status": _action_list_status,
        "get_status": _action_get_status,
        "enable": _action_enable,
        "disable": _action_disable,
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

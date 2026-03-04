"""
Process Schedules MCP Tool for Boomi Platform.

Provides 4 schedule management actions:
- list: Query all process schedules with optional process_id/atom_id filters
- get: Get specific schedule by schedule_id (or by process_id + atom_id)
- update: Update/create schedule with cron expression
- delete: Clear/disable schedule (sets empty schedule array)

SDK reference: boomi-python/examples/06_configure_deployment/manage_process_schedules.py
"""

import base64
from typing import Dict, Any, Optional, List

from boomi import Boomi
from boomi.models import (
    ProcessSchedules,
    ProcessSchedulesQueryConfig,
    ProcessSchedulesQueryConfigQueryFilter,
    ProcessSchedulesSimpleExpression,
    ProcessSchedulesSimpleExpressionOperator,
    ProcessSchedulesSimpleExpressionProperty,
    Schedule,
    ScheduleRetry,
)


# ============================================================================
# Helpers
# ============================================================================

def _schedule_id_from_ids(atom_id: str, process_id: str) -> str:
    """Build the base64-encoded schedule ID from atom and process IDs."""
    return base64.b64encode(f"CPS{atom_id}:{process_id}".encode()).decode()


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


# ============================================================================
# Action Handlers
# ============================================================================

def _action_list(sdk: Boomi, profile: str, **kwargs) -> Dict[str, Any]:
    """List all process schedules with optional process_id/atom_id filters."""
    process_id = kwargs.get("process_id")
    atom_id = kwargs.get("atom_id")

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
        "schedule": [schedule],
        "retry": retry,
    }
    if process_id:
        ps_kwargs["process_id"] = process_id
    if atom_id:
        ps_kwargs["atom_id"] = atom_id

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

    # Empty schedule array disables scheduling
    retry = ScheduleRetry(max_retry=5)
    ps_kwargs = {
        "id_": resource_id,
        "schedule": [],
        "retry": retry,
    }
    if process_id:
        ps_kwargs["process_id"] = process_id
    if atom_id:
        ps_kwargs["atom_id"] = atom_id

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
        action: One of: list, get, update, delete
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
    except Exception as e:
        return {
            "_success": False,
            "error": f"Action '{action}' failed: {str(e)}",
            "exception_type": type(e).__name__,
        }

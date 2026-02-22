#!/usr/bin/env python3
"""
Monitoring MCP Tools for Boomi Platform.

Provides 4 read-only monitoring actions:
- execution_logs: Request process log download URL
- execution_artifacts: Request execution artifact download URL
- audit_logs: Query audit trail with filters
- events: Query platform events with filters
"""

from typing import Dict, Any, Optional, List


# ============================================================================
# Action: execution_logs
# ============================================================================

def handle_execution_logs(boomi_client, config_data: Dict[str, Any]) -> Dict[str, Any]:
    """Request process log download for an execution."""
    from boomi.models import ProcessLog, LogLevel

    execution_id = config_data.get("execution_id")
    if not execution_id:
        return {
            "_success": False,
            "error": "execution_id is required",
            "hint": "Provide the execution ID to download logs for"
        }

    # Map log_level string to LogLevel enum (case-insensitive)
    log_level_str = config_data.get("log_level", "ALL").upper()
    log_level_map = {
        "SEVERE": LogLevel.SEVERE,
        "WARNING": LogLevel.WARNING,
        "INFO": LogLevel.INFO,
        "CONFIG": LogLevel.CONFIG,
        "FINE": LogLevel.FINE,
        "FINER": LogLevel.FINER,
        "FINEST": LogLevel.FINEST,
        "ALL": LogLevel.ALL,
    }
    log_level = log_level_map.get(log_level_str)
    if not log_level:
        return {
            "_success": False,
            "error": f"Invalid log_level: {log_level_str}",
            "valid_values": list(log_level_map.keys())
        }

    process_log = ProcessLog(
        execution_id=execution_id,
        log_level=log_level
    )

    result = boomi_client.process_log.create_process_log(request_body=process_log)

    status_code = int(getattr(result, 'status_code', 0))
    message = getattr(result, 'message', '')
    download_url = getattr(result, 'url', None)

    if status_code == 202:
        return {
            "_success": True,
            "status_code": status_code,
            "message": message,
            "download_url": download_url,
            "note": "Download URL requires Basic auth (username:password). Content is a ZIP archive."
        }
    elif status_code == 504:
        return {
            "_success": False,
            "status_code": status_code,
            "message": message,
            "error": "Runtime unavailable — the Atom may be offline"
        }
    else:
        return {
            "_success": False,
            "status_code": status_code,
            "message": message,
            "error": f"Unexpected status code: {status_code}"
        }


# ============================================================================
# Action: execution_artifacts
# ============================================================================

def handle_execution_artifacts(boomi_client, config_data: Dict[str, Any]) -> Dict[str, Any]:
    """Request execution artifact download URL."""
    from boomi.models import ExecutionArtifacts

    execution_id = config_data.get("execution_id")
    if not execution_id:
        return {
            "_success": False,
            "error": "execution_id is required",
            "hint": "Provide the execution ID to download artifacts for"
        }

    artifacts_request = ExecutionArtifacts(execution_id=execution_id)
    result = boomi_client.execution_artifacts.create_execution_artifacts(
        request_body=artifacts_request
    )

    download_url = getattr(result, 'url', None)
    status_code = getattr(result, 'status_code', None)
    message = getattr(result, 'message', '')

    if status_code:
        status_code = int(status_code)

    if download_url:
        return {
            "_success": True,
            "status_code": status_code,
            "message": message,
            "download_url": download_url,
            "note": "Download URL requires Basic auth (username:password). Content is a ZIP archive."
        }
    else:
        return {
            "_success": False,
            "status_code": status_code,
            "message": message,
            "error": "No download URL returned"
        }


# ============================================================================
# Action: audit_logs
# ============================================================================

def handle_audit_logs(boomi_client, config_data: Dict[str, Any]) -> Dict[str, Any]:
    """Query audit logs with optional filters."""
    from boomi.models import (
        AuditLogQueryConfig,
        AuditLogQueryConfigQueryFilter,
        AuditLogSimpleExpression,
        AuditLogSimpleExpressionOperator,
        AuditLogSimpleExpressionProperty,
        AuditLogGroupingExpression,
    )

    limit = config_data.get("limit", 100)
    expressions = []

    # Date range filter (BETWEEN on DATE)
    start_date = config_data.get("start_date")
    end_date = config_data.get("end_date")
    if start_date and end_date:
        expressions.append(AuditLogSimpleExpression(
            operator=AuditLogSimpleExpressionOperator.BETWEEN,
            property=AuditLogSimpleExpressionProperty.DATE,
            argument=[start_date, end_date]
        ))
    elif start_date:
        # Use GREATER_THAN_OR_EQUAL for start-only
        expressions.append(AuditLogSimpleExpression(
            operator=AuditLogSimpleExpressionOperator.GREATERTHANOREQUAL,
            property=AuditLogSimpleExpressionProperty.DATE,
            argument=[start_date]
        ))

    # Optional EQUALS filters
    filter_map = {
        "user": AuditLogSimpleExpressionProperty.USERID,
        "action": AuditLogSimpleExpressionProperty.ACTION,
        "type": AuditLogSimpleExpressionProperty.TYPE,
        "level": AuditLogSimpleExpressionProperty.LEVEL,
        "source": AuditLogSimpleExpressionProperty.SOURCE,
    }
    for key, prop in filter_map.items():
        value = config_data.get(key)
        if value:
            expressions.append(AuditLogSimpleExpression(
                operator=AuditLogSimpleExpressionOperator.EQUALS,
                property=prop,
                argument=[value]
            ))

    if not expressions:
        return {
            "_success": False,
            "error": "At least one filter is required (e.g. start_date, user, action, type, level, source)",
            "hint": "Provide start_date/end_date for a date range, or other filters"
        }

    # Build query filter
    if len(expressions) == 1:
        query_filter = AuditLogQueryConfigQueryFilter(expression=expressions[0])
    else:
        query_filter = AuditLogQueryConfigQueryFilter(
            expression=AuditLogGroupingExpression(
                operator="and",
                nested_expression=expressions
            )
        )

    query_config = AuditLogQueryConfig(query_filter=query_filter)
    result = boomi_client.audit_log.query_audit_log(request_body=query_config)

    # Collect results with pagination
    all_logs = []
    if hasattr(result, 'result') and result.result:
        all_logs.extend(_convert_audit_logs(result.result))

    query_token = getattr(result, 'query_token', None)
    while query_token and len(all_logs) < limit:
        result = boomi_client.audit_log.query_more_audit_log(
            query_token=query_token
        )
        if hasattr(result, 'result') and result.result:
            all_logs.extend(_convert_audit_logs(result.result))
        query_token = getattr(result, 'query_token', None)

    # Apply limit
    if len(all_logs) > limit:
        all_logs = all_logs[:limit]

    return {
        "_success": True,
        "total_count": len(all_logs),
        "audit_logs": all_logs
    }


def _convert_audit_logs(entries) -> List[Dict[str, Any]]:
    """Convert SDK audit log entries to flat dicts."""
    logs = []
    for entry in entries:
        log_dict = {
            "user_id": getattr(entry, 'user_id', None),
            "action": getattr(entry, 'action', None),
            "type": getattr(entry, 'type_', None),
            "level": getattr(entry, 'level', None),
            "modifier": getattr(entry, 'modifier', None),
            "source": getattr(entry, 'source', None),
            "date": getattr(entry, 'date_', None),
            "container_id": getattr(entry, 'container_id', None),
            "account_id": getattr(entry, 'account_id', None),
        }
        # Strip None values for cleaner output
        logs.append({k: v for k, v in log_dict.items() if v is not None})
    return logs


# ============================================================================
# Action: events
# ============================================================================

def handle_events(boomi_client, config_data: Dict[str, Any]) -> Dict[str, Any]:
    """Query platform events with optional filters."""
    from boomi.models import (
        EventQueryConfig,
        EventQueryConfigQueryFilter,
        EventSimpleExpression,
        EventSimpleExpressionOperator,
        EventSimpleExpressionProperty,
        EventGroupingExpression,
    )

    limit = config_data.get("limit", 100)
    expressions = []

    # Date range (BETWEEN on EVENTDATE)
    start_date = config_data.get("start_date")
    end_date = config_data.get("end_date")
    if start_date and end_date:
        expressions.append(EventSimpleExpression(
            operator=EventSimpleExpressionOperator.BETWEEN,
            property=EventSimpleExpressionProperty.EVENTDATE,
            argument=[start_date, end_date]
        ))
    elif start_date:
        expressions.append(EventSimpleExpression(
            operator=EventSimpleExpressionOperator.GREATERTHANOREQUAL,
            property=EventSimpleExpressionProperty.EVENTDATE,
            argument=[start_date]
        ))

    # Event level (EQUALS on EVENTLEVEL): ERROR, WARN, INFO
    event_level = config_data.get("event_level")
    if event_level:
        expressions.append(EventSimpleExpression(
            operator=EventSimpleExpressionOperator.EQUALS,
            property=EventSimpleExpressionProperty.EVENTLEVEL,
            argument=[event_level.upper()]
        ))

    # Event type (EQUALS on EVENTTYPE)
    event_type = config_data.get("event_type")
    if event_type:
        expressions.append(EventSimpleExpression(
            operator=EventSimpleExpressionOperator.EQUALS,
            property=EventSimpleExpressionProperty.EVENTTYPE,
            argument=[event_type]
        ))

    # Process name (LIKE on PROCESSNAME with % wildcards)
    process_name = config_data.get("process_name")
    if process_name:
        if "%" not in process_name:
            process_name = f"%{process_name}%"
        expressions.append(EventSimpleExpression(
            operator=EventSimpleExpressionOperator.LIKE,
            property=EventSimpleExpressionProperty.PROCESSNAME,
            argument=[process_name]
        ))

    # Atom name (EQUALS on ATOMNAME)
    atom_name = config_data.get("atom_name")
    if atom_name:
        expressions.append(EventSimpleExpression(
            operator=EventSimpleExpressionOperator.EQUALS,
            property=EventSimpleExpressionProperty.ATOMNAME,
            argument=[atom_name]
        ))

    # Execution ID (EQUALS on EXECUTIONID)
    execution_id = config_data.get("execution_id")
    if execution_id:
        expressions.append(EventSimpleExpression(
            operator=EventSimpleExpressionOperator.EQUALS,
            property=EventSimpleExpressionProperty.EXECUTIONID,
            argument=[execution_id]
        ))

    # Build query config
    if expressions:
        if len(expressions) == 1:
            query_filter = EventQueryConfigQueryFilter(expression=expressions[0])
        else:
            query_filter = EventQueryConfigQueryFilter(
                expression=EventGroupingExpression(
                    operator="and",
                    nested_expression=expressions
                )
            )
        query_config = EventQueryConfig(query_filter=query_filter)
    else:
        # No filters — query all (Boomi requires a query config)
        query_config = EventQueryConfig()

    result = boomi_client.event.query_event(request_body=query_config)

    # Collect results with pagination
    all_events = []
    if hasattr(result, 'result') and result.result:
        all_events.extend(_convert_events(result.result))

    query_token = getattr(result, 'query_token', None)
    while query_token and len(all_events) < limit:
        result = boomi_client.event.query_more_event(
            query_token=query_token
        )
        if hasattr(result, 'result') and result.result:
            all_events.extend(_convert_events(result.result))
        query_token = getattr(result, 'query_token', None)

    # Apply limit
    if len(all_events) > limit:
        all_events = all_events[:limit]

    return {
        "_success": True,
        "total_count": len(all_events),
        "events": all_events
    }


def _convert_events(entries) -> List[Dict[str, Any]]:
    """Convert SDK event entries to flat dicts."""
    events = []
    for event in entries:
        event_dict = {
            "event_id": getattr(event, 'event_id', None),
            "event_level": getattr(event, 'event_level', None),
            "event_type": getattr(event, 'event_type', None),
            "event_date": getattr(event, 'event_date', None),
            "process_name": getattr(event, 'process_name', None),
            "atom_name": getattr(event, 'atom_name', None),
            "execution_id": getattr(event, 'execution_id', None),
            "error": getattr(event, 'error', None),
            "status": getattr(event, 'status', None),
            "environment": getattr(event, 'environment', None),
            "classification": getattr(event, 'classification', None),
            "title": getattr(event, 'title', None),
            "error_type": getattr(event, 'error_type', None),
            "errored_step_type": getattr(event, 'errored_step_type', None),
        }
        events.append({k: v for k, v in event_dict.items() if v is not None})
    return events


# ============================================================================
# Consolidated Action Router
# ============================================================================

def monitor_platform_action(
    boomi_client,
    profile: str,
    action: str,
    config_data: Optional[Dict[str, Any]] = None,
) -> Dict[str, Any]:
    """
    Consolidated monitoring function. Routes to handler based on action.

    Args:
        boomi_client: Authenticated Boomi SDK client
        profile: Profile name
        action: One of: execution_logs, execution_artifacts, audit_logs, events
        config_data: Action-specific configuration dict

    Returns:
        Action result dict with _success status
    """
    if config_data is None:
        config_data = {}

    try:
        if action == "execution_logs":
            return handle_execution_logs(boomi_client, config_data)
        elif action == "execution_artifacts":
            return handle_execution_artifacts(boomi_client, config_data)
        elif action == "audit_logs":
            return handle_audit_logs(boomi_client, config_data)
        elif action == "events":
            return handle_events(boomi_client, config_data)
        else:
            return {
                "_success": False,
                "error": f"Unknown action: {action}",
                "valid_actions": ["execution_logs", "execution_artifacts", "audit_logs", "events"]
            }

    except Exception as e:
        return {
            "_success": False,
            "error": f"Action '{action}' failed: {str(e)}",
            "exception_type": type(e).__name__
        }

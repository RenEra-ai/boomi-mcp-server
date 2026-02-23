#!/usr/bin/env python3
"""
Monitoring MCP Tools for Boomi Platform.

Provides 5 read-only monitoring actions:
- execution_records: Query execution history (like Process Reporting)
- execution_logs: Request process log download URL
- execution_artifacts: Request execution artifact download URL
- audit_logs: Query audit trail with filters
- events: Query platform events with filters
"""

from typing import Dict, Any, Optional, List

import httpx
import zipfile
import io

MAX_ZIP_BYTES = 10 * 1024 * 1024       # 10 MB
MAX_FILE_CHARS = 50_000                 # per file
MAX_TOTAL_CHARS = 200_000              # across all files in ZIP


def _download_and_extract_zip(download_url: str, creds: Dict[str, str]) -> Dict[str, Any]:
    """Download ZIP from Boomi and extract text content inline.

    Boomi returns 202 while the ZIP is being prepared, then 200 when ready.
    We poll up to 5 times with 2-second delays.
    """
    import time
    auth = (creds["username"], creds["password"])
    try:
        resp = None
        with httpx.Client(timeout=30) as client:
            for attempt in range(5):
                resp = client.get(download_url, auth=auth)
                if resp.status_code == 200:
                    break
                if resp.status_code == 202:
                    time.sleep(2)
                    continue
                # Any other status is a real error, stop immediately
                break
        if resp.status_code != 200:
            return {"_downloaded": False, "http_status": resp.status_code,
                    "error": f"Download failed with HTTP {resp.status_code} after polling"}

        if len(resp.content) > MAX_ZIP_BYTES:
            return {
                "_downloaded": False,
                "error": f"ZIP too large ({len(resp.content)} bytes, limit {MAX_ZIP_BYTES})",
                "download_url": download_url
            }

        zf = zipfile.ZipFile(io.BytesIO(resp.content))
        files = {}
        total_chars = 0
        for name in zf.namelist():
            if total_chars >= MAX_TOTAL_CHARS:
                files[name] = f"[skipped — total content limit reached ({MAX_TOTAL_CHARS} chars)]"
                continue
            try:
                raw = zf.read(name)
                content = raw.decode("utf-8", errors="replace")
                original_len = len(content)
                remaining = MAX_TOTAL_CHARS - total_chars
                if len(content) > min(MAX_FILE_CHARS, remaining):
                    limit = min(MAX_FILE_CHARS, remaining)
                    content = content[:limit] + f"\n\n... [truncated at {limit} of {original_len} chars]"
                total_chars += min(original_len, MAX_FILE_CHARS)
                files[name] = content
            except Exception:
                files[name] = "[binary file, not displayed]"

        result = {"_downloaded": True, "files": files}
        if total_chars >= MAX_TOTAL_CHARS:
            result["_truncation_note"] = (
                f"Total content limit reached ({MAX_TOTAL_CHARS} chars). "
                "Some files may be truncated or skipped. Use download_url with Basic auth for full content."
            )
        return result
    except httpx.TimeoutException:
        return {"_downloaded": False, "error": "Download timed out (30s)"}
    except zipfile.BadZipFile:
        return {"_downloaded": False, "error": "Response is not a valid ZIP file"}
    except Exception as e:
        return {"_downloaded": False, "error": str(e)}


# ============================================================================
# Action: execution_logs
# ============================================================================

def handle_execution_logs(boomi_client, config_data: Dict[str, Any], creds=None) -> Dict[str, Any]:
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
        result = {
            "_success": True,
            "status_code": status_code,
            "message": message,
            "download_url": download_url,
        }
        fetch_content = config_data.get("fetch_content", True)
        if isinstance(fetch_content, str):
            fetch_content = fetch_content.lower() not in ("false", "0", "no")
        if fetch_content and creds and download_url:
            extracted = _download_and_extract_zip(download_url, creds)
            result.update(extracted)
        elif not fetch_content:
            result["note"] = "Content fetch skipped (fetch_content=false). Use download_url with Basic auth."
        return result
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

def handle_execution_artifacts(boomi_client, config_data: Dict[str, Any], creds=None) -> Dict[str, Any]:
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
        result = {
            "_success": True,
            "status_code": status_code,
            "message": message,
            "download_url": download_url,
        }
        fetch_content = config_data.get("fetch_content", True)
        if isinstance(fetch_content, str):
            fetch_content = fetch_content.lower() not in ("false", "0", "no")
        if fetch_content and creds and download_url:
            extracted = _download_and_extract_zip(download_url, creds)
            result.update(extracted)
        elif not fetch_content:
            result["note"] = "Content fetch skipped (fetch_content=false). Use download_url with Basic auth."
        return result
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
# Action: execution_records
# ============================================================================

def handle_execution_records(boomi_client, config_data: Dict[str, Any]) -> Dict[str, Any]:
    """Query execution records (Process Reporting equivalent)."""
    from boomi.models import (
        ExecutionRecordQueryConfig,
        ExecutionRecordQueryConfigQueryFilter,
        ExecutionRecordSimpleExpression,
        ExecutionRecordSimpleExpressionOperator,
        ExecutionRecordSimpleExpressionProperty,
        ExecutionRecordGroupingExpression,
    )

    limit = config_data.get("limit", 100)
    expressions = []

    # Date range filter (BETWEEN on EXECUTIONTIME)
    start_date = config_data.get("start_date")
    end_date = config_data.get("end_date")
    if start_date and end_date:
        expressions.append(ExecutionRecordSimpleExpression(
            operator=ExecutionRecordSimpleExpressionOperator.BETWEEN,
            property=ExecutionRecordSimpleExpressionProperty.EXECUTIONTIME,
            argument=[start_date, end_date]
        ))
    elif start_date:
        expressions.append(ExecutionRecordSimpleExpression(
            operator=ExecutionRecordSimpleExpressionOperator.GREATERTHANOREQUAL,
            property=ExecutionRecordSimpleExpressionProperty.EXECUTIONTIME,
            argument=[start_date]
        ))

    # Status filter (EQUALS on STATUS)
    status = config_data.get("status")
    if status:
        expressions.append(ExecutionRecordSimpleExpression(
            operator=ExecutionRecordSimpleExpressionOperator.EQUALS,
            property=ExecutionRecordSimpleExpressionProperty.STATUS,
            argument=[status.upper()]
        ))

    # Process name filter (LIKE on PROCESSNAME with % wildcards)
    process_name = config_data.get("process_name")
    if process_name:
        if "%" not in process_name:
            process_name = f"%{process_name}%"
        expressions.append(ExecutionRecordSimpleExpression(
            operator=ExecutionRecordSimpleExpressionOperator.LIKE,
            property=ExecutionRecordSimpleExpressionProperty.PROCESSNAME,
            argument=[process_name]
        ))

    # Process ID filter (EQUALS on PROCESSID)
    process_id = config_data.get("process_id")
    if process_id:
        expressions.append(ExecutionRecordSimpleExpression(
            operator=ExecutionRecordSimpleExpressionOperator.EQUALS,
            property=ExecutionRecordSimpleExpressionProperty.PROCESSID,
            argument=[process_id]
        ))

    # Atom name filter (EQUALS on ATOMNAME)
    atom_name = config_data.get("atom_name")
    if atom_name:
        expressions.append(ExecutionRecordSimpleExpression(
            operator=ExecutionRecordSimpleExpressionOperator.EQUALS,
            property=ExecutionRecordSimpleExpressionProperty.ATOMNAME,
            argument=[atom_name]
        ))

    # Atom ID filter (EQUALS on ATOMID)
    atom_id = config_data.get("atom_id")
    if atom_id:
        expressions.append(ExecutionRecordSimpleExpression(
            operator=ExecutionRecordSimpleExpressionOperator.EQUALS,
            property=ExecutionRecordSimpleExpressionProperty.ATOMID,
            argument=[atom_id]
        ))

    # Execution ID filter (EQUALS on EXECUTIONID)
    execution_id = config_data.get("execution_id")
    if execution_id:
        expressions.append(ExecutionRecordSimpleExpression(
            operator=ExecutionRecordSimpleExpressionOperator.EQUALS,
            property=ExecutionRecordSimpleExpressionProperty.EXECUTIONID,
            argument=[execution_id]
        ))

    if not expressions:
        return {
            "_success": False,
            "error": "At least one filter is required",
            "hint": "Provide start_date/end_date, status, process_name, process_id, atom_name, atom_id, or execution_id"
        }

    # Build query filter
    if len(expressions) == 1:
        query_filter = ExecutionRecordQueryConfigQueryFilter(expression=expressions[0])
    else:
        query_filter = ExecutionRecordQueryConfigQueryFilter(
            expression=ExecutionRecordGroupingExpression(
                operator="and",
                nested_expression=expressions
            )
        )

    query_config = ExecutionRecordQueryConfig(query_filter=query_filter)
    result = boomi_client.execution_record.query_execution_record(request_body=query_config)

    # Collect results with pagination
    all_records = []
    if hasattr(result, 'result') and result.result:
        all_records.extend(_convert_execution_records(result.result))

    query_token = getattr(result, 'query_token', None)
    while query_token and len(all_records) < limit:
        result = boomi_client.execution_record.query_more_execution_record(
            query_token=query_token
        )
        if hasattr(result, 'result') and result.result:
            all_records.extend(_convert_execution_records(result.result))
        query_token = getattr(result, 'query_token', None)

    # Apply limit
    if len(all_records) > limit:
        all_records = all_records[:limit]

    return {
        "_success": True,
        "total_count": len(all_records),
        "execution_records": all_records
    }


def _convert_execution_records(entries) -> List[Dict[str, Any]]:
    """Convert SDK execution record entries to flat dicts."""
    records = []
    for entry in entries:
        record = {
            "execution_id": getattr(entry, 'execution_id', None),
            "process_name": getattr(entry, 'process_name', None),
            "process_id": getattr(entry, 'process_id', None),
            "status": getattr(entry, 'status', None),
            "execution_time": getattr(entry, 'execution_time', None),
            "recorded_date": getattr(entry, 'recorded_date', None),
            "execution_duration": getattr(entry, 'execution_duration', None),
            "execution_type": getattr(entry, 'execution_type', None),
            "atom_name": getattr(entry, 'atom_name', None),
            "atom_id": getattr(entry, 'atom_id', None),
            "message": getattr(entry, 'message', None),
            "inbound_document_count": getattr(entry, 'inbound_document_count', None),
            "outbound_document_count": getattr(entry, 'outbound_document_count', None),
            "inbound_error_document_count": getattr(entry, 'inbound_error_document_count', None),
        }
        records.append({k: v for k, v in record.items() if v is not None})
    return records


# ============================================================================
# Consolidated Action Router
# ============================================================================

def monitor_platform_action(
    boomi_client,
    profile: str,
    action: str,
    config_data: Optional[Dict[str, Any]] = None,
    creds: Optional[Dict[str, str]] = None,
) -> Dict[str, Any]:
    """
    Consolidated monitoring function. Routes to handler based on action.

    Args:
        boomi_client: Authenticated Boomi SDK client
        profile: Profile name
        action: One of: execution_records, execution_logs, execution_artifacts, audit_logs, events
        config_data: Action-specific configuration dict
        creds: Boomi credentials dict (username, password) for downloading log/artifact content

    Returns:
        Action result dict with _success status
    """
    if config_data is None:
        config_data = {}

    try:
        if action == "execution_records":
            return handle_execution_records(boomi_client, config_data)
        elif action == "execution_logs":
            return handle_execution_logs(boomi_client, config_data, creds=creds)
        elif action == "execution_artifacts":
            return handle_execution_artifacts(boomi_client, config_data, creds=creds)
        elif action == "audit_logs":
            return handle_audit_logs(boomi_client, config_data)
        elif action == "events":
            return handle_events(boomi_client, config_data)
        else:
            return {
                "_success": False,
                "error": f"Unknown action: {action}",
                "valid_actions": ["execution_records", "execution_logs", "execution_artifacts", "audit_logs", "events"]
            }

    except Exception as e:
        return {
            "_success": False,
            "error": f"Action '{action}' failed: {str(e)}",
            "exception_type": type(e).__name__
        }

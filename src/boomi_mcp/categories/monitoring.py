#!/usr/bin/env python3
"""
Monitoring MCP Tools for Boomi Platform.

Provides 17 read-only monitoring actions:
- execution_records: Query execution history (like Process Reporting)
- execution_logs: Request process log download URL
- execution_artifacts: Request execution artifact download URL
- audit_logs: Query audit trail with filters
- events: Query platform events with filters
- certificates: Query expiring/expired deployed certificates
- throughput: Account-level throughput metrics by date range
- execution_metrics: Aggregated execution statistics (success rate, avg duration, top failures)
- connector_documents: Document-level tracking for connector operations
- download_connector_document: Download actual content of a connector document
- execution_summary: Aggregated execution summary records (process_id, atom_id, date range)
- document_counts: Document count metrics per account or account group
- execution_counts: Execution count metrics per account or account group
- api_usage_counts: API usage count metrics
- connection_licensing_report: Generate connection licensing download report
- custom_tracked_fields: Query custom tracked fields
- edi_connector_records: Query EDI-standard connector records (x12, edifact, as2, etc.)
"""

import re
from typing import Dict, Any, Optional, List

from boomi.net.transport.api_error import ApiError
import httpx
import zipfile
import io


_PLAIN_DATE_RE = re.compile(r'^\d{4}-\d{2}-\d{2}$')


def _normalize_iso_date(date_str: str, role: str = 'start') -> str:
    """Convert plain YYYY-MM-DD to ISO 8601 datetime.

    If already ISO format, return as-is.
    role='start' -> T00:00:00Z, role='end' -> T23:59:59Z
    """
    if _PLAIN_DATE_RE.match(date_str):
        suffix = 'T00:00:00Z' if role == 'start' else 'T23:59:59Z'
        return date_str + suffix
    return date_str

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


def _reject_request_id_as_execution_id(execution_id):
    """Return a structured failure if a request_id was passed where an execution_id is expected.

    The Boomi async tracking id (request_id, 'executionrecord-<uuid>') is rejected by the
    ProcessLog/ExecutionArtifacts/ConnectorDocument platform APIs. Only the exact
    'executionrecord-' prefix is rejected — 'execution' alone prefixes BOTH id forms, so it
    must not be tested, and no execution-<uuid>-<date> shape regex is applied (the SDK gives
    execution_id no format contract). Returns None when the id is acceptable.
    """
    if isinstance(execution_id, str) and execution_id.startswith("executionrecord-"):
        return {
            "_success": False,
            "error": "execution_id looks like a request_id (executionrecord- prefix). Use the execution_id from execute_process execution_result.execution_id instead.",
            "hint": "execute_process returns two ids: request_id (executionrecord-..., for polling only) and execution_id (inside execution_result.execution_id, for logs/artifacts). Pass execution_result.execution_id here.",
        }
    return None


MAX_ZIP_BYTES = 10 * 1024 * 1024       # 10 MB
MAX_FILE_CHARS = 50_000                 # per file
MAX_TOTAL_CHARS = 200_000              # across all files in ZIP
MAX_UNCOMPRESSED_BYTES = 50 * 1024 * 1024  # 50 MB per entry (zip bomb guard)


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
        for info in zf.infolist():
            name = info.filename
            if total_chars >= MAX_TOTAL_CHARS:
                files[name] = f"[skipped — total content limit reached ({MAX_TOTAL_CHARS} chars)]"
                continue
            if info.file_size > MAX_UNCOMPRESSED_BYTES:
                files[name] = f"[skipped — uncompressed size {info.file_size} bytes exceeds {MAX_UNCOMPRESSED_BYTES} limit]"
                continue
            try:
                read_limit = min(MAX_FILE_CHARS, MAX_TOTAL_CHARS - total_chars)
                # Read slightly more than char limit to detect truncation
                read_bytes = read_limit + 1024
                with zf.open(name) as f:
                    raw = f.read(read_bytes)
                    byte_capped = bool(f.read(1))
                content = raw.decode("utf-8", errors="replace")
                if len(content) > read_limit or byte_capped:
                    content = content[:read_limit]
                    chars_emitted = len(content)
                    content += f"\n\n... [truncated at {chars_emitted} chars of ~{info.file_size} bytes]"
                    total_chars += chars_emitted
                else:
                    total_chars += len(content)
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


def _download_content(download_url: str, creds: Dict[str, str],
                      max_bytes: int = 10 * 1024 * 1024) -> Dict[str, Any]:
    """Download raw (non-ZIP) content from Boomi, polling 202→200.

    Returns inline text for text-like responses, base64 + metadata for binary.
    """
    import time
    import base64
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
                break
        if resp is None or resp.status_code != 200:
            status = resp.status_code if resp else "no response"
            return {"_downloaded": False, "http_status": status,
                    "error": f"Download failed with HTTP {status} after polling"}

        if len(resp.content) > max_bytes:
            return {
                "_downloaded": False,
                "error": f"Response too large ({len(resp.content)} bytes, limit {max_bytes})",
                "download_url": download_url,
            }

        content_type = resp.headers.get("content-type", "application/octet-stream")
        is_text = any(t in content_type.lower() for t in
                      ("text/", "json", "xml", "html", "csv", "yaml"))

        if is_text:
            text = resp.content.decode("utf-8", errors="replace")
            if len(text) > MAX_FILE_CHARS:
                text = text[:MAX_FILE_CHARS] + f"\n\n... [truncated at {MAX_FILE_CHARS} chars]"
            return {"_downloaded": True, "content": text,
                    "content_type": content_type, "size_bytes": len(resp.content)}
        else:
            return {"_downloaded": True,
                    "content_base64": base64.b64encode(resp.content).decode("ascii"),
                    "content_type": content_type, "size_bytes": len(resp.content)}

    except httpx.TimeoutException:
        return {"_downloaded": False, "error": "Download timed out (30s)"}
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

    rejection = _reject_request_id_as_execution_id(execution_id)
    if rejection:
        return rejection

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

    rejection = _reject_request_id_as_execution_id(execution_id)
    if rejection:
        return rejection

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
    if start_date:
        start_date = _normalize_iso_date(start_date, 'start')
    if end_date:
        end_date = _normalize_iso_date(end_date, 'end')
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
            request_body=query_token
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
    if start_date:
        start_date = _normalize_iso_date(start_date, 'start')
    if end_date:
        end_date = _normalize_iso_date(end_date, 'end')
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

    # Event level (EQUALS on EVENTLEVEL): ERROR, WARNING, INFO
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
            request_body=query_token
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
    if start_date:
        start_date = _normalize_iso_date(start_date, 'start')
    if end_date:
        end_date = _normalize_iso_date(end_date, 'end')
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
        valid_statuses = {"COMPLETE", "ERROR", "ABORTED", "COMPLETE_WARN", "INPROCESS"}
        if status.upper() not in valid_statuses:
            return {"_success": False, "error": f"Invalid status '{status}'. Valid: {', '.join(sorted(valid_statuses))}"}
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
            request_body=query_token
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
# Action: certificates
# ============================================================================

def handle_certificates(boomi_client, config_data: Dict[str, Any]) -> Dict[str, Any]:
    """Query expiring/expired deployed certificates."""
    from boomi.models import (
        DeployedExpiredCertificateQueryConfig,
        DeployedExpiredCertificateQueryConfigQueryFilter,
        DeployedExpiredCertificateSimpleExpression,
        DeployedExpiredCertificateSimpleExpressionOperator,
        DeployedExpiredCertificateSimpleExpressionProperty,
    )
    from datetime import datetime

    limit = config_data.get("limit", 100)

    # expirationBoundary: days ahead to look (default 30)
    # Positive = certs expiring within N days from now
    # Negative = certs that expired N days ago or later
    days_ahead = config_data.get("days_ahead", 30)

    expression = DeployedExpiredCertificateSimpleExpression(
        operator=DeployedExpiredCertificateSimpleExpressionOperator.LESSTHANOREQUAL,
        property=DeployedExpiredCertificateSimpleExpressionProperty.EXPIRATIONBOUNDARY,
        argument=[str(days_ahead)]
    )

    query_filter = DeployedExpiredCertificateQueryConfigQueryFilter(
        expression=expression
    )
    query_config = DeployedExpiredCertificateQueryConfig(
        query_filter=query_filter
    )

    result = boomi_client.deployed_expired_certificate.query_deployed_expired_certificate(
        request_body=query_config
    )

    all_certs = []
    if hasattr(result, 'result') and result.result:
        all_certs.extend(_convert_certificates(result.result))

    query_token = getattr(result, 'query_token', None)
    while query_token and len(all_certs) < limit:
        result = boomi_client.deployed_expired_certificate.query_more_deployed_expired_certificate(
            request_body=query_token
        )
        if hasattr(result, 'result') and result.result:
            all_certs.extend(_convert_certificates(result.result))
        query_token = getattr(result, 'query_token', None)

    if len(all_certs) > limit:
        all_certs = all_certs[:limit]

    return {
        "_success": True,
        "total_count": len(all_certs),
        "certificates": all_certs
    }


def _convert_certificates(entries) -> List[Dict[str, Any]]:
    """Convert SDK certificate entries to flat dicts."""
    certs = []
    for entry in entries:
        cert = {
            "certificate_id": getattr(entry, 'certificate_id', None),
            "certificate_name": getattr(entry, 'certificate_name', None),
            "certificate_type": getattr(entry, 'certificate_type', None),
            "expiration_date": getattr(entry, 'expiration_date', None),
            "location": getattr(entry, 'location', None),
            "container_id": getattr(entry, 'container_id', None),
            "container_name": getattr(entry, 'container_name', None),
            "environment_id": getattr(entry, 'environment_id', None),
            "environment_name": getattr(entry, 'environment_name', None),
        }
        certs.append({k: v for k, v in cert.items() if v is not None})
    return certs


# ============================================================================
# Action: throughput
# ============================================================================

def handle_throughput(boomi_client, config_data: Dict[str, Any]) -> Dict[str, Any]:
    """Query account-level throughput metrics by date range."""
    from boomi.models import (
        ThroughputAccountQueryConfig,
        ThroughputAccountQueryConfigQueryFilter,
        ThroughputAccountSimpleExpression,
        ThroughputAccountSimpleExpressionOperator,
        ThroughputAccountSimpleExpressionProperty,
        ThroughputAccountGroupingExpression,
    )

    limit = config_data.get("limit", 100)
    expressions = []

    start_date = config_data.get("start_date")
    end_date = config_data.get("end_date")
    if start_date and end_date:
        expressions.append(ThroughputAccountSimpleExpression(
            operator=ThroughputAccountSimpleExpressionOperator.BETWEEN,
            property=ThroughputAccountSimpleExpressionProperty.PROCESSDATE,
            argument=[start_date, end_date]
        ))
    elif start_date:
        expressions.append(ThroughputAccountSimpleExpression(
            operator=ThroughputAccountSimpleExpressionOperator.GREATERTHANOREQUAL,
            property=ThroughputAccountSimpleExpressionProperty.PROCESSDATE,
            argument=[start_date]
        ))
    elif end_date:
        expressions.append(ThroughputAccountSimpleExpression(
            operator=ThroughputAccountSimpleExpressionOperator.LESSTHANOREQUAL,
            property=ThroughputAccountSimpleExpressionProperty.PROCESSDATE,
            argument=[end_date]
        ))

    atom_id = config_data.get("atom_id")
    if atom_id:
        expressions.append(ThroughputAccountSimpleExpression(
            operator=ThroughputAccountSimpleExpressionOperator.EQUALS,
            property=ThroughputAccountSimpleExpressionProperty.ATOMID,
            argument=[atom_id]
        ))

    if not expressions:
        return {
            "_success": False,
            "error": "At least one filter is required (e.g. start_date, atom_id)",
            "hint": "Provide start_date/end_date for a date range, or atom_id"
        }

    if len(expressions) == 1:
        query_filter = ThroughputAccountQueryConfigQueryFilter(expression=expressions[0])
    else:
        query_filter = ThroughputAccountQueryConfigQueryFilter(
            expression=ThroughputAccountGroupingExpression(
                operator="and",
                nested_expression=expressions
            )
        )

    query_config = ThroughputAccountQueryConfig(query_filter=query_filter)
    result = boomi_client.throughput_account.query_throughput_account(
        request_body=query_config
    )

    all_records = []
    if hasattr(result, 'result') and result.result:
        all_records.extend(_convert_throughput(result.result))

    query_token = getattr(result, 'query_token', None)
    while query_token and len(all_records) < limit:
        result = boomi_client.throughput_account.query_more_throughput_account(
            request_body=query_token
        )
        if hasattr(result, 'result') and result.result:
            all_records.extend(_convert_throughput(result.result))
        query_token = getattr(result, 'query_token', None)

    if len(all_records) > limit:
        all_records = all_records[:limit]

    # Calculate summary
    total_bytes = sum(r.get("bytes", 0) for r in all_records)
    return {
        "_success": True,
        "total_count": len(all_records),
        "total_bytes": total_bytes,
        "total_mb": round(total_bytes / (1024 * 1024), 2) if total_bytes else 0,
        "throughput_records": all_records
    }


def _convert_throughput(entries) -> List[Dict[str, Any]]:
    """Convert SDK throughput entries to flat dicts."""
    records = []
    for entry in entries:
        record = {
            "date": getattr(entry, 'date_', None),
            "atom_id": getattr(entry, 'atom_id', None),
            "bytes": getattr(entry, 'value', None),
        }
        # Add MB for readability
        if record.get("bytes"):
            record["mb"] = round(record["bytes"] / (1024 * 1024), 2)
        records.append({k: v for k, v in record.items() if v is not None})
    return records


# ============================================================================
# Action: execution_metrics
# ============================================================================

def handle_execution_metrics(boomi_client, config_data: Dict[str, Any]) -> Dict[str, Any]:
    """Aggregate execution metrics: success rate, avg duration, top failures."""
    # Reuse the same execution_records query but aggregate results
    raw_result = handle_execution_records(boomi_client, config_data)
    if not raw_result.get("_success"):
        return raw_result

    records = raw_result.get("execution_records", [])
    if not records:
        return {
            "_success": True,
            "total_executions": 0,
            "success_rate_pct": None,
            "status_counts": {},
            "duration_ms": None,
            "error_count": 0,
            "top_failures": [],
            "message": "No execution records found for the given filters",
        }

    # Aggregate
    total = len(records)
    status_counts: Dict[str, int] = {}
    durations = []
    process_errors: Dict[str, int] = {}

    for rec in records:
        st = rec.get("status", "UNKNOWN")
        status_counts[st] = status_counts.get(st, 0) + 1

        dur = rec.get("execution_duration")
        if dur is not None:
            try:
                durations.append(int(dur))
            except (ValueError, TypeError):
                pass

        if st == "ERROR":
            pname = rec.get("process_name", "unknown")
            process_errors[pname] = process_errors.get(pname, 0) + 1

    success_count = status_counts.get("COMPLETE", 0) + status_counts.get("COMPLETE_WARN", 0)
    error_count = status_counts.get("ERROR", 0)
    success_rate = round((success_count / total) * 100, 1) if total else 0

    avg_duration = round(sum(durations) / len(durations)) if durations else None
    min_duration = min(durations) if durations else None
    max_duration = max(durations) if durations else None

    # Top N failures
    raw_top = config_data.get("top_failures", 5)
    try:
        top_n = max(0, int(raw_top))
    except (TypeError, ValueError):
        top_n = 5
    top_failures = sorted(process_errors.items(), key=lambda x: x[1], reverse=True)[:top_n]

    return {
        "_success": True,
        "total_executions": total,
        "success_rate_pct": success_rate,
        "status_counts": status_counts,
        "duration_ms": {
            "avg": avg_duration,
            "min": min_duration,
            "max": max_duration,
            "sample_size": len(durations),
        } if durations else None,
        "error_count": error_count,
        "top_failures": [{"process_name": name, "error_count": cnt} for name, cnt in top_failures],
    }


# ============================================================================
# Action: connector_documents
# ============================================================================

def handle_connector_documents(boomi_client, config_data: Dict[str, Any]) -> Dict[str, Any]:
    """Query connector document records for an execution."""
    from boomi.models import (
        GenericConnectorRecordQueryConfig,
        GenericConnectorRecordQueryConfigQueryFilter,
        GenericConnectorRecordSimpleExpression,
        GenericConnectorRecordSimpleExpressionOperator,
        GenericConnectorRecordSimpleExpressionProperty,
        GenericConnectorRecordGroupingExpression,
    )

    execution_id = config_data.get("execution_id")
    if not execution_id:
        return {
            "_success": False,
            "error": "execution_id is required",
            "hint": "GenericConnectorRecord queries require an execution_id. Use execution_records action to find one."
        }

    rejection = _reject_request_id_as_execution_id(execution_id)
    if rejection:
        return rejection

    limit = config_data.get("limit", 100)
    expressions = []

    # Required: execution_id
    expressions.append(GenericConnectorRecordSimpleExpression(
        operator=GenericConnectorRecordSimpleExpressionOperator.EQUALS,
        property=GenericConnectorRecordSimpleExpressionProperty.EXECUTIONID,
        argument=[execution_id]
    ))

    # Optional: connector_type
    connector_type = config_data.get("connector_type")
    if connector_type:
        expressions.append(GenericConnectorRecordSimpleExpression(
            operator=GenericConnectorRecordSimpleExpressionOperator.EQUALS,
            property=GenericConnectorRecordSimpleExpressionProperty.CONNECTORTYPE,
            argument=[connector_type]
        ))

    # Optional: status
    status = config_data.get("status")
    if status:
        expressions.append(GenericConnectorRecordSimpleExpression(
            operator=GenericConnectorRecordSimpleExpressionOperator.EQUALS,
            property=GenericConnectorRecordSimpleExpressionProperty.STATUS,
            argument=[status.upper()]
        ))

    # Optional: action_type
    action_type = config_data.get("action_type")
    if action_type:
        expressions.append(GenericConnectorRecordSimpleExpression(
            operator=GenericConnectorRecordSimpleExpressionOperator.EQUALS,
            property=GenericConnectorRecordSimpleExpressionProperty.ACTIONTYPE,
            argument=[action_type]
        ))

    if len(expressions) == 1:
        query_filter = GenericConnectorRecordQueryConfigQueryFilter(expression=expressions[0])
    else:
        query_filter = GenericConnectorRecordQueryConfigQueryFilter(
            expression=GenericConnectorRecordGroupingExpression(
                operator="and",
                nested_expression=expressions
            )
        )

    query_config = GenericConnectorRecordQueryConfig(query_filter=query_filter)
    result = boomi_client.generic_connector_record.query_generic_connector_record(
        request_body=query_config
    )

    all_records = []
    if hasattr(result, 'result') and result.result:
        all_records.extend(_convert_connector_records(result.result))

    query_token = getattr(result, 'query_token', None)
    while query_token and len(all_records) < limit:
        result = boomi_client.generic_connector_record.query_more_generic_connector_record(
            request_body=query_token
        )
        if hasattr(result, 'result') and result.result:
            all_records.extend(_convert_connector_records(result.result))
        query_token = getattr(result, 'query_token', None)

    if len(all_records) > limit:
        all_records = all_records[:limit]

    return {
        "_success": True,
        "total_count": len(all_records),
        "connector_records": all_records
    }


def _convert_connector_records(entries) -> List[Dict[str, Any]]:
    """Convert SDK generic connector record entries to flat dicts."""
    records = []
    for entry in entries:
        record = {
            "id": getattr(entry, 'id_', None),
            "execution_id": getattr(entry, 'execution_id', None),
            "connection_name": getattr(entry, 'connection_name', None),
            "operation_name": getattr(entry, 'operation_name', None),
            "connector_type": getattr(entry, 'connector_type', None),
            "action_type": getattr(entry, 'action_type', None),
            "status": getattr(entry, 'status', None),
            "date_processed": getattr(entry, 'date_processed', None),
            "size_kb": getattr(entry, 'size', None),
            "error_message": getattr(entry, 'error_message', None),
            "retryable": getattr(entry, 'retryable', None),
            "document_index": getattr(entry, 'document_index', None),
            # Provenance. The SDK carries these and they were being dropped, so a
            # per-document record could say a REST connector moved a document but
            # not WHICH operation, connection or connector row produced it — which
            # is exactly the attribution the evidence registry is built on.
            "operation_id": _unset_to_none(getattr(entry, 'operation_id', None)),
            "connection_id": _unset_to_none(getattr(entry, 'connection_id', None)),
            "execution_connector_id": _unset_to_none(
                getattr(entry, 'execution_connector_id', None)),
        }
        records.append({k: v for k, v in record.items() if v is not None})
    return records


def handle_download_connector_document(
    boomi_client, config_data: Dict[str, Any], creds=None
) -> Dict[str, Any]:
    """Download the actual content of a connector document.

    Uses the Boomi ConnectorDocument API: POST to get a download URL,
    then GET the URL on platform.boomi.com with Basic auth.
    """
    record_id = config_data.get("generic_connector_record_id")
    if not record_id:
        return {
            "_success": False,
            "error": "generic_connector_record_id is required for 'download_connector_document'",
        }

    fetch_content = config_data.get("fetch_content", True)

    from boomi.models import ConnectorDocument
    doc_request = ConnectorDocument(generic_connector_record_id=record_id)
    result = boomi_client.connector_document.create_connector_document(
        request_body=doc_request,
    )

    # Extract download URL from SDK response
    if hasattr(result, '_map'):
        result_data = result._map()
    elif isinstance(result, dict):
        result_data = result
    else:
        result_data = {}

    download_url = result_data.get("url", "")
    status_code = result_data.get("statusCode", result_data.get("status_code", ""))
    message = result_data.get("message", "")

    response = {
        "_success": True,
        "generic_connector_record_id": record_id,
        "download_url": download_url,
        "status_code": status_code,
        "message": message,
    }

    if not fetch_content:
        response["note"] = "fetch_content=false — use download_url with Basic auth to retrieve content"
        return response

    if not download_url:
        return {
            "_success": False,
            "error": "No download URL returned from ConnectorDocument API",
            "api_response": result_data,
        }

    if not creds:
        response["note"] = "No credentials available for download — use download_url with Basic auth"
        return response

    downloaded = _download_content(download_url, creds)
    response.update(downloaded)
    return response


# ============================================================================
# Action: execution_summary
# ============================================================================

def handle_execution_summary(boomi_client, config_data: Dict[str, Any]) -> Dict[str, Any]:
    """Query ExecutionSummaryRecord with optional filters."""
    from boomi.models import (
        ExecutionSummaryRecordQueryConfig,
        ExecutionSummaryRecordQueryConfigQueryFilter,
        ExecutionSummaryRecordSimpleExpression,
        ExecutionSummaryRecordSimpleExpressionOperator,
        ExecutionSummaryRecordSimpleExpressionProperty,
        ExecutionSummaryRecordGroupingExpression,
    )

    limit = config_data.get("limit", 100)
    expressions = []

    # Date range filter (BETWEEN on TIMEBLOCK)
    start_date = config_data.get("start_date")
    end_date = config_data.get("end_date")
    if start_date and end_date:
        expressions.append(ExecutionSummaryRecordSimpleExpression(
            operator=ExecutionSummaryRecordSimpleExpressionOperator.BETWEEN,
            property=ExecutionSummaryRecordSimpleExpressionProperty.TIMEBLOCK,
            argument=[start_date, end_date]
        ))
    elif start_date:
        expressions.append(ExecutionSummaryRecordSimpleExpression(
            operator=ExecutionSummaryRecordSimpleExpressionOperator.GREATERTHANOREQUAL,
            property=ExecutionSummaryRecordSimpleExpressionProperty.TIMEBLOCK,
            argument=[start_date]
        ))

    # Process ID filter
    process_id = config_data.get("process_id")
    if process_id:
        expressions.append(ExecutionSummaryRecordSimpleExpression(
            operator=ExecutionSummaryRecordSimpleExpressionOperator.EQUALS,
            property=ExecutionSummaryRecordSimpleExpressionProperty.PROCESSID,
            argument=[process_id]
        ))

    # Atom ID filter
    atom_id = config_data.get("atom_id")
    if atom_id:
        expressions.append(ExecutionSummaryRecordSimpleExpression(
            operator=ExecutionSummaryRecordSimpleExpressionOperator.EQUALS,
            property=ExecutionSummaryRecordSimpleExpressionProperty.ATOMID,
            argument=[atom_id]
        ))

    # Status filter
    status = config_data.get("status")
    if status:
        expressions.append(ExecutionSummaryRecordSimpleExpression(
            operator=ExecutionSummaryRecordSimpleExpressionOperator.EQUALS,
            property=ExecutionSummaryRecordSimpleExpressionProperty.STATUS,
            argument=[status.upper()]
        ))

    if not expressions:
        return {
            "_success": False,
            "error": "At least one filter is required",
            "hint": "Provide start_date/end_date, process_id, atom_id, or status"
        }

    if len(expressions) == 1:
        query_filter = ExecutionSummaryRecordQueryConfigQueryFilter(expression=expressions[0])
    else:
        query_filter = ExecutionSummaryRecordQueryConfigQueryFilter(
            expression=ExecutionSummaryRecordGroupingExpression(
                operator="and",
                nested_expression=expressions
            )
        )

    query_config = ExecutionSummaryRecordQueryConfig(query_filter=query_filter)
    result = boomi_client.execution_summary_record.query_execution_summary_record(
        request_body=query_config
    )

    all_records = []
    if hasattr(result, 'result') and result.result:
        all_records.extend(_convert_execution_summary(result.result))

    query_token = getattr(result, 'query_token', None)
    while query_token and len(all_records) < limit:
        result = boomi_client.execution_summary_record.query_more_execution_summary_record(
            request_body=query_token
        )
        if hasattr(result, 'result') and result.result:
            all_records.extend(_convert_execution_summary(result.result))
        query_token = getattr(result, 'query_token', None)

    if len(all_records) > limit:
        all_records = all_records[:limit]

    return {
        "_success": True,
        "total_count": len(all_records),
        "execution_summary_records": all_records
    }


def _convert_execution_summary(entries) -> List[Dict[str, Any]]:
    """Convert SDK ExecutionSummaryRecord entries to flat dicts."""
    records = []
    for entry in entries:
        record = {
            "process_id": getattr(entry, 'process_id', None),
            "process_name": getattr(entry, 'process_name', None),
            "atom_id": getattr(entry, 'atom_id', None),
            "atom_name": getattr(entry, 'atom_name', None),
            "time_block": getattr(entry, 'time_block', None),
            "status": getattr(entry, 'status', None),
            "execution_count": getattr(entry, 'execution_count', None),
            "elapsed_time": getattr(entry, 'elapsed_time', None),
            "max_elapsed_time": getattr(entry, 'max_elapsed_time', None),
            "inbound_doc_count": getattr(entry, 'inbound_doc_count', None),
            "inbound_doc_size": getattr(entry, 'inbound_doc_size', None),
            "outbound_doc_count": getattr(entry, 'outbound_doc_count', None),
            "outbound_doc_size": getattr(entry, 'outbound_doc_size', None),
            "return_doc_count": getattr(entry, 'return_doc_count', None),
            "return_doc_size": getattr(entry, 'return_doc_size', None),
            "launcher_id": getattr(entry, 'launcher_id', None),
            "report_key": getattr(entry, 'report_key', None),
            "account_id": getattr(entry, 'account_id', None),
        }
        records.append({k: v for k, v in record.items() if v is not None})
    return records


# ============================================================================
# Action: document_counts
# ============================================================================

def handle_document_counts(boomi_client, config_data: Dict[str, Any]) -> Dict[str, Any]:
    """Query document count metrics. Dispatches to account or account-group service."""
    account_group_id = config_data.get("account_group_id")

    if account_group_id:
        return _query_document_counts_group(boomi_client, config_data, account_group_id)
    return _query_document_counts_account(boomi_client, config_data)


def _query_document_counts_account(boomi_client, config_data: Dict[str, Any]) -> Dict[str, Any]:
    from boomi.models import (
        DocumentCountAccountQueryConfig,
        DocumentCountAccountQueryConfigQueryFilter,
        DocumentCountAccountSimpleExpression,
        DocumentCountAccountSimpleExpressionOperator,
        DocumentCountAccountSimpleExpressionProperty,
        DocumentCountAccountGroupingExpression,
    )

    limit = config_data.get("limit", 100)
    expressions = []

    start_date = config_data.get("start_date")
    end_date = config_data.get("end_date")
    if start_date and end_date:
        expressions.append(DocumentCountAccountSimpleExpression(
            operator=DocumentCountAccountSimpleExpressionOperator.BETWEEN,
            property=DocumentCountAccountSimpleExpressionProperty.PROCESSDATE,
            argument=[start_date, end_date]
        ))
    elif start_date:
        expressions.append(DocumentCountAccountSimpleExpression(
            operator=DocumentCountAccountSimpleExpressionOperator.GREATERTHANOREQUAL,
            property=DocumentCountAccountSimpleExpressionProperty.PROCESSDATE,
            argument=[start_date]
        ))

    if not expressions:
        return {
            "_success": False,
            "error": "At least one filter is required (e.g. start_date)",
            "hint": "Provide start_date/end_date for a date range"
        }

    if len(expressions) == 1:
        query_filter = DocumentCountAccountQueryConfigQueryFilter(expression=expressions[0])
    else:
        query_filter = DocumentCountAccountQueryConfigQueryFilter(
            expression=DocumentCountAccountGroupingExpression(
                operator="and",
                nested_expression=expressions
            )
        )

    query_config = DocumentCountAccountQueryConfig(query_filter=query_filter)
    result = boomi_client.document_count_account.query_document_count_account(
        request_body=query_config
    )

    all_records = []
    if hasattr(result, 'result') and result.result:
        all_records.extend(_convert_generic_results(result.result))

    query_token = getattr(result, 'query_token', None)
    while query_token and len(all_records) < limit:
        result = boomi_client.document_count_account.query_more_document_count_account(
            request_body=query_token
        )
        if hasattr(result, 'result') and result.result:
            all_records.extend(_convert_generic_results(result.result))
        query_token = getattr(result, 'query_token', None)

    if len(all_records) > limit:
        all_records = all_records[:limit]

    return {
        "_success": True,
        "scope": "account",
        "total_count": len(all_records),
        "document_counts": all_records
    }


def _query_document_counts_group(boomi_client, config_data: Dict[str, Any], account_group_id: str) -> Dict[str, Any]:
    from boomi.models import (
        DocumentCountAccountGroupQueryConfig,
        DocumentCountAccountGroupQueryConfigQueryFilter,
        DocumentCountAccountGroupSimpleExpression,
        DocumentCountAccountGroupSimpleExpressionOperator,
        DocumentCountAccountGroupSimpleExpressionProperty,
        DocumentCountAccountGroupGroupingExpression,
    )

    limit = config_data.get("limit", 100)
    expressions = []

    # Account group ID filter (required for group queries)
    expressions.append(DocumentCountAccountGroupSimpleExpression(
        operator=DocumentCountAccountGroupSimpleExpressionOperator.EQUALS,
        property=DocumentCountAccountGroupSimpleExpressionProperty.ACCOUNTGROUPID,
        argument=[account_group_id]
    ))

    start_date = config_data.get("start_date")
    end_date = config_data.get("end_date")
    if start_date and end_date:
        expressions.append(DocumentCountAccountGroupSimpleExpression(
            operator=DocumentCountAccountGroupSimpleExpressionOperator.BETWEEN,
            property=DocumentCountAccountGroupSimpleExpressionProperty.PROCESSDATE,
            argument=[start_date, end_date]
        ))
    elif start_date:
        expressions.append(DocumentCountAccountGroupSimpleExpression(
            operator=DocumentCountAccountGroupSimpleExpressionOperator.GREATERTHANOREQUAL,
            property=DocumentCountAccountGroupSimpleExpressionProperty.PROCESSDATE,
            argument=[start_date]
        ))

    if len(expressions) == 1:
        query_filter = DocumentCountAccountGroupQueryConfigQueryFilter(expression=expressions[0])
    else:
        query_filter = DocumentCountAccountGroupQueryConfigQueryFilter(
            expression=DocumentCountAccountGroupGroupingExpression(
                operator="and",
                nested_expression=expressions
            )
        )

    query_config = DocumentCountAccountGroupQueryConfig(query_filter=query_filter)
    result = boomi_client.document_count_account_group.query_document_count_account_group(
        request_body=query_config
    )

    all_records = []
    if hasattr(result, 'result') and result.result:
        all_records.extend(_convert_generic_results(result.result))

    query_token = getattr(result, 'query_token', None)
    while query_token and len(all_records) < limit:
        result = boomi_client.document_count_account_group.query_more_document_count_account_group(
            request_body=query_token
        )
        if hasattr(result, 'result') and result.result:
            all_records.extend(_convert_generic_results(result.result))
        query_token = getattr(result, 'query_token', None)

    if len(all_records) > limit:
        all_records = all_records[:limit]

    return {
        "_success": True,
        "scope": "account_group",
        "account_group_id": account_group_id,
        "total_count": len(all_records),
        "document_counts": all_records
    }


# ============================================================================
# Action: execution_counts
# ============================================================================

def handle_execution_counts(boomi_client, config_data: Dict[str, Any]) -> Dict[str, Any]:
    """Query execution count metrics. Dispatches to account or account-group service."""
    account_group_id = config_data.get("account_group_id")

    if account_group_id:
        return _query_execution_counts_group(boomi_client, config_data, account_group_id)
    return _query_execution_counts_account(boomi_client, config_data)


def _query_execution_counts_account(boomi_client, config_data: Dict[str, Any]) -> Dict[str, Any]:
    from boomi.models import (
        ExecutionCountAccountQueryConfig,
        ExecutionCountAccountQueryConfigQueryFilter,
        ExecutionCountAccountSimpleExpression,
        ExecutionCountAccountSimpleExpressionOperator,
        ExecutionCountAccountSimpleExpressionProperty,
        ExecutionCountAccountGroupingExpression,
    )

    limit = config_data.get("limit", 100)
    expressions = []

    start_date = config_data.get("start_date")
    end_date = config_data.get("end_date")
    if start_date and end_date:
        expressions.append(ExecutionCountAccountSimpleExpression(
            operator=ExecutionCountAccountSimpleExpressionOperator.BETWEEN,
            property=ExecutionCountAccountSimpleExpressionProperty.PROCESSDATE,
            argument=[start_date, end_date]
        ))
    elif start_date:
        expressions.append(ExecutionCountAccountSimpleExpression(
            operator=ExecutionCountAccountSimpleExpressionOperator.GREATERTHANOREQUAL,
            property=ExecutionCountAccountSimpleExpressionProperty.PROCESSDATE,
            argument=[start_date]
        ))

    if not expressions:
        return {
            "_success": False,
            "error": "At least one filter is required (e.g. start_date)",
            "hint": "Provide start_date/end_date for a date range"
        }

    if len(expressions) == 1:
        query_filter = ExecutionCountAccountQueryConfigQueryFilter(expression=expressions[0])
    else:
        query_filter = ExecutionCountAccountQueryConfigQueryFilter(
            expression=ExecutionCountAccountGroupingExpression(
                operator="and",
                nested_expression=expressions
            )
        )

    query_config = ExecutionCountAccountQueryConfig(query_filter=query_filter)
    result = boomi_client.execution_count_account.query_execution_count_account(
        request_body=query_config
    )

    all_records = []
    if hasattr(result, 'result') and result.result:
        all_records.extend(_convert_generic_results(result.result))

    query_token = getattr(result, 'query_token', None)
    while query_token and len(all_records) < limit:
        result = boomi_client.execution_count_account.query_more_execution_count_account(
            request_body=query_token
        )
        if hasattr(result, 'result') and result.result:
            all_records.extend(_convert_generic_results(result.result))
        query_token = getattr(result, 'query_token', None)

    if len(all_records) > limit:
        all_records = all_records[:limit]

    return {
        "_success": True,
        "scope": "account",
        "total_count": len(all_records),
        "execution_counts": all_records
    }


def _query_execution_counts_group(boomi_client, config_data: Dict[str, Any], account_group_id: str) -> Dict[str, Any]:
    from boomi.models import (
        ExecutionCountAccountGroupQueryConfig,
        ExecutionCountAccountGroupQueryConfigQueryFilter,
        ExecutionCountAccountGroupSimpleExpression,
        ExecutionCountAccountGroupSimpleExpressionOperator,
        ExecutionCountAccountGroupSimpleExpressionProperty,
        ExecutionCountAccountGroupGroupingExpression,
    )

    limit = config_data.get("limit", 100)
    expressions = []

    expressions.append(ExecutionCountAccountGroupSimpleExpression(
        operator=ExecutionCountAccountGroupSimpleExpressionOperator.EQUALS,
        property=ExecutionCountAccountGroupSimpleExpressionProperty.ACCOUNTGROUPID,
        argument=[account_group_id]
    ))

    start_date = config_data.get("start_date")
    end_date = config_data.get("end_date")
    if start_date and end_date:
        expressions.append(ExecutionCountAccountGroupSimpleExpression(
            operator=ExecutionCountAccountGroupSimpleExpressionOperator.BETWEEN,
            property=ExecutionCountAccountGroupSimpleExpressionProperty.PROCESSDATE,
            argument=[start_date, end_date]
        ))
    elif start_date:
        expressions.append(ExecutionCountAccountGroupSimpleExpression(
            operator=ExecutionCountAccountGroupSimpleExpressionOperator.GREATERTHANOREQUAL,
            property=ExecutionCountAccountGroupSimpleExpressionProperty.PROCESSDATE,
            argument=[start_date]
        ))

    if len(expressions) == 1:
        query_filter = ExecutionCountAccountGroupQueryConfigQueryFilter(expression=expressions[0])
    else:
        query_filter = ExecutionCountAccountGroupQueryConfigQueryFilter(
            expression=ExecutionCountAccountGroupGroupingExpression(
                operator="and",
                nested_expression=expressions
            )
        )

    query_config = ExecutionCountAccountGroupQueryConfig(query_filter=query_filter)
    result = boomi_client.execution_count_account_group.query_execution_count_account_group(
        request_body=query_config
    )

    all_records = []
    if hasattr(result, 'result') and result.result:
        all_records.extend(_convert_generic_results(result.result))

    query_token = getattr(result, 'query_token', None)
    while query_token and len(all_records) < limit:
        result = boomi_client.execution_count_account_group.query_more_execution_count_account_group(
            request_body=query_token
        )
        if hasattr(result, 'result') and result.result:
            all_records.extend(_convert_generic_results(result.result))
        query_token = getattr(result, 'query_token', None)

    if len(all_records) > limit:
        all_records = all_records[:limit]

    return {
        "_success": True,
        "scope": "account_group",
        "account_group_id": account_group_id,
        "total_count": len(all_records),
        "execution_counts": all_records
    }


# ============================================================================
# Action: api_usage_counts
# ============================================================================

def handle_api_usage_counts(boomi_client, config_data: Dict[str, Any]) -> Dict[str, Any]:
    """Query API usage count metrics."""
    from boomi.models import (
        ApiUsageCountQueryConfig,
        ApiUsageCountQueryConfigQueryFilter,
        ApiUsageCountSimpleExpression,
        ApiUsageCountSimpleExpressionOperator,
        ApiUsageCountSimpleExpressionProperty,
        ApiUsageCountGroupingExpression,
    )

    limit = config_data.get("limit", 100)
    expressions = []

    start_date = config_data.get("start_date")
    end_date = config_data.get("end_date")
    if start_date:
        start_date = _normalize_iso_date(start_date, 'start')
    if end_date:
        end_date = _normalize_iso_date(end_date, 'end')
    if start_date and end_date:
        expressions.append(ApiUsageCountSimpleExpression(
            operator=ApiUsageCountSimpleExpressionOperator.BETWEEN,
            property=ApiUsageCountSimpleExpressionProperty.PROCESSDATE,
            argument=[start_date, end_date]
        ))
    elif start_date:
        expressions.append(ApiUsageCountSimpleExpression(
            operator=ApiUsageCountSimpleExpressionOperator.GREATERTHANOREQUAL,
            property=ApiUsageCountSimpleExpressionProperty.PROCESSDATE,
            argument=[start_date]
        ))

    if not expressions:
        return {
            "_success": False,
            "error": "At least one filter is required (e.g. start_date)",
            "hint": "Provide start_date/end_date for a date range"
        }

    if len(expressions) == 1:
        query_filter = ApiUsageCountQueryConfigQueryFilter(expression=expressions[0])
    else:
        query_filter = ApiUsageCountQueryConfigQueryFilter(
            expression=ApiUsageCountGroupingExpression(
                operator="and",
                nested_expression=expressions
            )
        )

    query_config = ApiUsageCountQueryConfig(query_filter=query_filter)
    result = boomi_client.api_usage_count.query_api_usage_count(
        request_body=query_config
    )

    all_records = []
    if hasattr(result, 'result') and result.result:
        all_records.extend(_convert_generic_results(result.result))

    query_token = getattr(result, 'query_token', None)
    while query_token and len(all_records) < limit:
        result = boomi_client.api_usage_count.query_more_api_usage_count(
            request_body=query_token
        )
        if hasattr(result, 'result') and result.result:
            all_records.extend(_convert_generic_results(result.result))
        query_token = getattr(result, 'query_token', None)

    if len(all_records) > limit:
        all_records = all_records[:limit]

    return {
        "_success": True,
        "total_count": len(all_records),
        "api_usage_counts": all_records
    }


# ============================================================================
# Action: connection_licensing_report
# ============================================================================

def handle_connection_licensing_report(boomi_client, config_data: Dict[str, Any]) -> Dict[str, Any]:
    """Create a connection licensing report and return the download URL."""
    # The SDK accepts None for an empty body — returns all connector classes
    result = boomi_client.connection_licensing_report.create_connection_licensing_report(
        request_body=None
    )

    url = getattr(result, 'url', None)
    status_code = getattr(result, 'status_code', None)
    message = getattr(result, 'message', None)

    return {
        "_success": True,
        "url": url,
        "status_code": status_code,
        "message": message,
        "hint": "Use the returned URL with Basic auth (GET) to download the CSV report"
    }


# ============================================================================
# Action: custom_tracked_fields
# ============================================================================

def handle_custom_tracked_fields(boomi_client, config_data: Dict[str, Any]) -> Dict[str, Any]:
    """Query custom tracked fields (max 20 fields, no filters needed)."""
    result = boomi_client.custom_tracked_field.query_custom_tracked_field(
        request_body=None
    )

    limit = config_data.get("limit", 100)
    all_records = []
    if hasattr(result, 'result') and result.result:
        all_records.extend(_convert_generic_results(result.result))

    query_token = getattr(result, 'query_token', None)
    while query_token and len(all_records) < limit:
        result = boomi_client.custom_tracked_field.query_more_custom_tracked_field(
            request_body=query_token
        )
        if hasattr(result, 'result') and result.result:
            all_records.extend(_convert_generic_results(result.result))
        query_token = getattr(result, 'query_token', None)

    if len(all_records) > limit:
        all_records = all_records[:limit]

    return {
        "_success": True,
        "total_count": len(all_records),
        "custom_tracked_fields": all_records
    }


# ============================================================================
# Action: edi_connector_records
# ============================================================================

# Mapping of EDI standard name -> (sdk service attr, query method, query_more method)
_EDI_STANDARD_MAP = {
    "as2": ("as2_connector_record", "query_as2_connector_record", "query_more_as2_connector_record"),
    "edifact": ("edifact_connector_record", "query_edifact_connector_record", "query_more_edifact_connector_record"),
    "custom": ("edi_custom_connector_record", "query_edi_custom_connector_record", "query_more_edi_custom_connector_record"),
    "edi_custom": ("edi_custom_connector_record", "query_edi_custom_connector_record", "query_more_edi_custom_connector_record"),
    "hl7": ("hl7_connector_record", "query_hl7_connector_record", "query_more_hl7_connector_record"),
    "odette": ("odette_connector_record", "query_odette_connector_record", "query_more_odette_connector_record"),
    "oftp2": ("oftp2_connector_record", "query_oftp2_connector_record", "query_more_oftp2_connector_record"),
    "rosettanet": ("rosetta_net_connector_record", "query_rosetta_net_connector_record", "query_more_rosetta_net_connector_record"),
    "tradacoms": ("tradacoms_connector_record", "query_tradacoms_connector_record", "query_more_tradacoms_connector_record"),
    "x12": ("x12_connector_record", "query_x12_connector_record", "query_more_x12_connector_record"),
}


def handle_edi_connector_records(boomi_client, config_data: Dict[str, Any]) -> Dict[str, Any]:
    """Query EDI connector records for a specific standard."""
    standard = config_data.get("standard", "").lower()
    if not standard:
        return {
            "_success": False,
            "error": "standard is required",
            "valid_standards": sorted(_EDI_STANDARD_MAP.keys())
        }

    if standard not in _EDI_STANDARD_MAP:
        return {
            "_success": False,
            "error": f"Unknown EDI standard: {standard}",
            "valid_standards": sorted(_EDI_STANDARD_MAP.keys())
        }

    service_attr, query_method, query_more_method = _EDI_STANDARD_MAP[standard]
    service = getattr(boomi_client, service_attr)
    query_fn = getattr(service, query_method)
    query_more_fn = getattr(service, query_more_method)

    # Build a minimal query config — pass raw config_data as the request body
    # The SDK accepts None/empty query configs for these services
    result = query_fn(request_body=config_data.get("query_config"))

    limit = config_data.get("limit", 100)
    all_records = []
    if hasattr(result, 'result') and result.result:
        all_records.extend(_convert_generic_results(result.result))

    query_token = getattr(result, 'query_token', None)
    while query_token and len(all_records) < limit:
        result = query_more_fn(request_body=query_token)
        if hasattr(result, 'result') and result.result:
            all_records.extend(_convert_generic_results(result.result))
        query_token = getattr(result, 'query_token', None)

    if len(all_records) > limit:
        all_records = all_records[:limit]

    return {
        "_success": True,
        "standard": standard,
        "total_count": len(all_records),
        "edi_connector_records": all_records
    }


# ============================================================================
# Shared converter for generic SDK result objects
# ============================================================================

def _convert_generic_results(entries) -> List[Dict[str, Any]]:
    """Convert SDK result entries to flat dicts using _map() or getattr fallback."""
    records = []
    for entry in entries:
        if hasattr(entry, '_map'):
            record = entry._map()
        else:
            record = {k: v for k, v in vars(entry).items() if not k.startswith('_')}
        # Strip None values
        records.append({k: v for k, v in record.items() if v is not None})
    return records


# ============================================================================
# Consolidated Action Router
# ============================================================================


#: A bound on paging, so a pathological execution cannot spin this call forever.
#: Reaching it is REPORTED rather than hidden — a silent cap is how a truncated
#: answer comes to look like a complete one.
_MAX_QUERY_PAGES = 50


def _unset_to_none(value: Any) -> Any:
    """Normalise the SDK's unset marker to None.

    MEASURED, after a first version of this docstring asserted the opposite. The
    generated SDK names SENTINEL as a parameter DEFAULT, but assigns nothing when a
    caller omits the argument — a freshly built record's `__dict__` is empty, so
    `getattr(record, field, None)` yields None and never the sentinel. Across every
    live record captured for this work, zero attributes held a sentinel value.

    So the sentinel branch below is DEFENSIVE, not load-bearing, and saying so is
    the point: the earlier claim that it was load-bearing was tested with a fixture
    that `setattr`s a sentinel by hand — a state the SDK cannot produce — which made
    a passing test look like coverage of a path that does not exist.

    Comparison is by IDENTITY. The sentinel is a plain `object()`, with no
    distinguishing type name and no equality of its own; a type-name check matches
    nothing, which was the version before this one and was caught only by running
    it. And the test is not truthiness: `error_count` is legitimately 0, and 0 is a
    fact, not an absence.
    """
    from boomi.models.utils.sentinel import SENTINEL

    return None if value is None or value is SENTINEL else value


def handle_execution_connectors(boomi_client, config_data: Dict[str, Any]) -> Dict[str, Any]:
    """Query the per-connector rows the platform records for one execution.

    These rows are what make a capture attributable: they say WHICH connector ran
    inside an execution and how many documents it moved. Two cautions are baked
    into the shape of the result rather than left to the caller:

    * ``action_type`` is the platform's own value and is the SAME for every HTTP
      verb — measured across 95 rows spanning eight verbs. It is returned because
      it is what the platform said, but it cannot distinguish a read from a
      delete, and the field name here says so.
    * ``nodata`` and ``return`` appear as connector types beside genuine ones.
      They are execution sentinels, not connectors, and are flagged so a caller
      does not mistake one for a connector that ran.
    """
    from boomi_mcp.connector_replay.models import EXECUTION_SENTINELS
    from boomi.models import (
        ExecutionConnectorQueryConfig,
        ExecutionConnectorQueryConfigQueryFilter,
        ExecutionConnectorSimpleExpression,
        ExecutionConnectorSimpleExpressionOperator,
        ExecutionConnectorSimpleExpressionProperty,
    )

    execution_id = config_data.get("execution_id")
    if not execution_id:
        return {
            "_success": False,
            "error": "execution_id is required",
            "hint": (
                "ExecutionConnector queries require an execution_id. Use the "
                "execution_records action to find one."
            ),
        }

    rejection = _reject_request_id_as_execution_id(execution_id)
    if rejection:
        return rejection

    expression = ExecutionConnectorSimpleExpression(
        operator=ExecutionConnectorSimpleExpressionOperator.EQUALS,
        property=ExecutionConnectorSimpleExpressionProperty.EXECUTIONID,
        argument=[execution_id],
    )
    query = ExecutionConnectorQueryConfig(
        query_filter=ExecutionConnectorQueryConfigQueryFilter(expression=expression)
    )
    response = boomi_client.execution_connector.query_execution_connector(request_body=query)

    # Follow the query token to exhaustion, the same way this module's other
    # queries do. Reading only the first page returned a truncated `connectors`
    # list beside a `count` that described the truncation rather than the
    # execution — which is worse than an obvious error, because the caller has no
    # way to tell a short list from a complete one. An execution with many
    # connector rows is exactly the case these rows exist to describe.
    records = list(getattr(response, "result", None) or [])
    query_token = getattr(response, "query_token", None)
    pages = 1
    while query_token and pages < _MAX_QUERY_PAGES:
        response = boomi_client.execution_connector.query_more_execution_connector(
            request_body=query_token
        )
        records.extend(getattr(response, "result", None) or [])
        query_token = getattr(response, "query_token", None)
        pages += 1
    truncated = bool(query_token)

    rows = []
    for record in records:
        connector_type = _unset_to_none(getattr(record, "connector_type", None))
        row = {
            "execution_connector_id": _unset_to_none(getattr(record, "id_", None)),
            "execution_id": _unset_to_none(getattr(record, "execution_id", None)),
            # WHICH connector ran. Dropping this made the rows unattributable —
            # they could say a REST connector ran and how much it moved, but not
            # which one, which is the whole reason these rows are read.
            "execution_connector": _unset_to_none(getattr(record, "execution_connector", None)),
            "connector_type": connector_type,
            # Named for what it IS, not for what a reader might hope it is.
            "platform_action_type": _unset_to_none(getattr(record, "action_type", None)),
            "is_execution_sentinel": connector_type in EXECUTION_SENTINELS,
            "is_start_shape": _unset_to_none(getattr(record, "is_start_shape", None)),
            "record_type": _unset_to_none(getattr(record, "record_type", None)),
            "success_count": _unset_to_none(getattr(record, "success_count", None)),
            "error_count": _unset_to_none(getattr(record, "error_count", None)),
            "size": _unset_to_none(getattr(record, "size", None)),
        }
        rows.append({k: v for k, v in row.items() if v is not None})

    result = {
        "_success": True,
        "execution_id": execution_id,
        "count": len(rows),
        "pages_read": pages,
        "connectors": rows,
        "note": (
            "platform_action_type is identical for every HTTP verb; read the verb "
            "from the operation component, not from this field."
        ),
    }
    if truncated:
        # Say so rather than serving a short list as if it were the whole set.
        result["truncated"] = (
            "More connector rows exist than were read: the page limit of {0} was "
            "reached and the platform still offered a continuation token. The "
            "`connectors` list and `count` describe what was read, not the "
            "execution.".format(_MAX_QUERY_PAGES)
        )
    if not rows:
        # An empty result has two causes this query cannot tell apart, and reporting
        # it as a bare success invites the reader to conclude the execution used no
        # connectors. Say what is actually known instead.
        result["empty_result_is_ambiguous"] = (
            "No connector rows were returned. This does NOT mean the execution used "
            "no connectors: the same empty result is returned for an execution that "
            "does not exist, and for one whose rows have not yet materialised. "
            "Confirm the execution with the execution_records action before drawing "
            "a conclusion from this."
        )
    return result



#: Action name -> (handler, whether the handler needs credentials).
#:
#: This table is the AUTHORITY for which monitoring actions exist. Dispatch reads
#: it and so does the message that lists valid actions, so the two cannot drift.
#: Declaration order is the served order.
_MONITORING_ACTIONS = {
    "execution_records": (handle_execution_records, False),
    "execution_logs": (handle_execution_logs, True),
    "execution_artifacts": (handle_execution_artifacts, True),
    "audit_logs": (handle_audit_logs, False),
    "events": (handle_events, False),
    "certificates": (handle_certificates, False),
    "throughput": (handle_throughput, False),
    "execution_metrics": (handle_execution_metrics, False),
    "connector_documents": (handle_connector_documents, False),
    "download_connector_document": (handle_download_connector_document, True),
    "execution_summary": (handle_execution_summary, False),
    "document_counts": (handle_document_counts, False),
    "execution_counts": (handle_execution_counts, False),
    "api_usage_counts": (handle_api_usage_counts, False),
    "connection_licensing_report": (handle_connection_licensing_report, False),
    "custom_tracked_fields": (handle_custom_tracked_fields, False),
    "edi_connector_records": (handle_edi_connector_records, False),
    "execution_connectors": (handle_execution_connectors, False),
}


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
        action: One of: execution_records, execution_logs, execution_artifacts, audit_logs, events, certificates, throughput, execution_metrics, connector_documents, download_connector_document, execution_summary, document_counts, execution_counts, api_usage_counts, connection_licensing_report, custom_tracked_fields, edi_connector_records
        config_data: Action-specific configuration dict
        creds: Boomi credentials dict (username, password) for downloading log/artifact content

    Returns:
        Action result dict with _success status
    """
    if config_data is None:
        config_data = {}

    try:
        entry = _MONITORING_ACTIONS.get(action)
        if entry is None:
            return {
                "_success": False,
                "error": f"Unknown action: {action}",
                # DERIVED from the dispatch table, never re-listed. This message
                # used to carry a hand-written copy of the action names — a second
                # model of a fact the table already held. The two could disagree,
                # and both directions hurt: an action that dispatches but is not
                # advertised is invisible, and one advertised but not dispatched is
                # a promise that fails on use.
                "valid_actions": list(_MONITORING_ACTIONS),
            }
        handler, wants_creds = entry
        if wants_creds:
            return handler(boomi_client, config_data, creds=creds)
        return handler(boomi_client, config_data)

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

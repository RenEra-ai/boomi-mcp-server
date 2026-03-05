#!/usr/bin/env python3
"""
Monitoring MCP Tools for Boomi Platform.

Provides 9 read-only monitoring actions:
- execution_records: Query execution history (like Process Reporting)
- execution_logs: Request process log download URL
- execution_artifacts: Request execution artifact download URL
- audit_logs: Query audit trail with filters
- events: Query platform events with filters
- certificates: Query expiring/expired deployed certificates
- throughput: Account-level throughput metrics by date range
- execution_metrics: Aggregated execution statistics (success rate, avg duration, top failures)
- connector_documents: Document-level tracking for connector operations
"""

from typing import Dict, Any, Optional, List

import httpx
import zipfile
import io

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
    top_n = int(config_data.get("top_failures", 5))
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
        action: One of: execution_records, execution_logs, execution_artifacts, audit_logs, events, certificates, throughput, execution_metrics, connector_documents
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
        elif action == "certificates":
            return handle_certificates(boomi_client, config_data)
        elif action == "throughput":
            return handle_throughput(boomi_client, config_data)
        elif action == "execution_metrics":
            return handle_execution_metrics(boomi_client, config_data)
        elif action == "connector_documents":
            return handle_connector_documents(boomi_client, config_data)
        else:
            return {
                "_success": False,
                "error": f"Unknown action: {action}",
                "valid_actions": ["execution_records", "execution_logs", "execution_artifacts", "audit_logs", "events", "certificates", "throughput", "execution_metrics", "connector_documents"]
            }

    except Exception as e:
        return {
            "_success": False,
            "error": f"Action '{action}' failed: {str(e)}",
            "exception_type": type(e).__name__
        }

"""
Troubleshooting MCP Tools for Boomi Platform.

Provides 6 troubleshooting actions for failed executions:
- error_details: Get error details from failed execution records + process logs
- retry: Retry a failed execution with same or modified properties
- reprocess: Re-execute a failed process with new execution request
- list_queues: List all queues for a runtime (async operation)
- clear_queue: Clear messages from a stuck queue
- move_queue: Move messages between queues

SDK example references:
- boomi-python/examples/11_troubleshoot_fix/get_error_details.py
- boomi-python/examples/11_troubleshoot_fix/retry_failed_execution.py
- boomi-python/examples/11_troubleshoot_fix/reprocess_documents.py
- boomi-python/examples/11_troubleshoot_fix/manage_queues.py
"""

import time
from typing import Dict, Any, Optional, List

from boomi import Boomi
from boomi.net.transport.api_error import ApiError


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


# ============================================================================
# Helpers
# ============================================================================

def _query_execution_record(sdk: Boomi, execution_id: str):
    """Query a single execution record by ID. Returns the record or None."""
    from boomi.models import (
        ExecutionRecordQueryConfig,
        ExecutionRecordQueryConfigQueryFilter,
        ExecutionRecordSimpleExpression,
        ExecutionRecordSimpleExpressionOperator,
        ExecutionRecordSimpleExpressionProperty,
    )

    expression = ExecutionRecordSimpleExpression(
        operator=ExecutionRecordSimpleExpressionOperator.EQUALS,
        property=ExecutionRecordSimpleExpressionProperty.EXECUTIONID,
        argument=[execution_id],
    )
    query_filter = ExecutionRecordQueryConfigQueryFilter(expression=expression)
    query_config = ExecutionRecordQueryConfig(query_filter=query_filter)

    result = sdk.execution_record.query_execution_record(request_body=query_config)
    if hasattr(result, "result") and result.result:
        return result.result[0]
    return None


def _query_error_executions(sdk: Boomi, process_id: str = None,
                            days: int = 7, limit: int = 10):
    """Query recent error executions, optionally filtered by process_id."""
    from datetime import datetime, timedelta, timezone
    from boomi.models import (
        ExecutionRecordQueryConfig,
        ExecutionRecordQueryConfigQueryFilter,
        ExecutionRecordSimpleExpression,
        ExecutionRecordSimpleExpressionOperator,
        ExecutionRecordSimpleExpressionProperty,
        ExecutionRecordGroupingExpression,
    )

    expressions = []

    # Status = ERROR
    expressions.append(ExecutionRecordSimpleExpression(
        operator=ExecutionRecordSimpleExpressionOperator.EQUALS,
        property=ExecutionRecordSimpleExpressionProperty.STATUS,
        argument=["ERROR"],
    ))

    # Optional process_id filter
    if process_id:
        expressions.append(ExecutionRecordSimpleExpression(
            operator=ExecutionRecordSimpleExpressionOperator.EQUALS,
            property=ExecutionRecordSimpleExpressionProperty.PROCESSID,
            argument=[process_id],
        ))

    # Date filter
    since_date = (datetime.now(timezone.utc) - timedelta(days=days)).strftime("%Y-%m-%dT%H:%M:%SZ")
    expressions.append(ExecutionRecordSimpleExpression(
        operator=ExecutionRecordSimpleExpressionOperator.GREATERTHANOREQUAL,
        property=ExecutionRecordSimpleExpressionProperty.EXECUTIONTIME,
        argument=[since_date],
    ))

    if len(expressions) == 1:
        query_filter = ExecutionRecordQueryConfigQueryFilter(expression=expressions[0])
    else:
        grouping = ExecutionRecordGroupingExpression(
            operator="and", nested_expression=expressions
        )
        query_filter = ExecutionRecordQueryConfigQueryFilter(expression=grouping)

    query_config = ExecutionRecordQueryConfig(query_filter=query_filter)
    result = sdk.execution_record.query_execution_record(request_body=query_config)

    records = []

    def _collect(res):
        if hasattr(res, "result") and res.result:
            for rec in res.result:
                if len(records) >= limit:
                    return
                records.append(_execution_to_dict(rec))

    _collect(result)
    while len(records) < limit and hasattr(result, "query_token") and result.query_token:
        result = sdk.execution_record.query_more_execution_record(
            request_body=result.query_token
        )
        _collect(result)

    return records


def _execution_to_dict(execution) -> Dict[str, Any]:
    """Convert an execution record object to a flat dict."""
    d = {
        "execution_id": getattr(execution, "execution_id", None),
        "status": getattr(execution, "status", None),
        "process_name": getattr(execution, "process_name", None),
        "process_id": getattr(execution, "process_id", None),
        "atom_name": getattr(execution, "atom_name", None),
        "atom_id": getattr(execution, "atom_id", None),
        "execution_time": getattr(execution, "execution_time", None),
        "execution_duration": getattr(execution, "execution_duration", None),
        "message": getattr(execution, "message", None),
        "inbound_document_count": getattr(execution, "inbound_document_count", None),
        "outbound_document_count": getattr(execution, "outbound_document_count", None),
        "inbound_error_document_count": getattr(execution, "inbound_error_document_count", None),
    }
    return {k: v for k, v in d.items() if v is not None}


# ============================================================================
# Action: error_details
# ============================================================================

def handle_error_details(sdk: Boomi, execution_id: str = None,
                         process_id: str = None,
                         config: Dict[str, Any] = None) -> Dict[str, Any]:
    """Get error details from failed execution records and optionally download logs."""
    if config is None:
        config = {}

    try:
        days = int(config.get("days", 7))
        limit = int(config.get("limit", 10))
    except (ValueError, TypeError):
        return {"_success": False, "error": "config.days and config.limit must be numeric values"}
    fetch_logs = config.get("fetch_logs", False)
    log_level = config.get("log_level", "ALL")

    if execution_id:
        # Single execution lookup
        record = _query_execution_record(sdk, execution_id)
        if not record:
            return {"_success": False, "error": f"Execution record not found: {execution_id}"}

        exec_dict = _execution_to_dict(record)

        result = {
            "_success": True,
            "execution": exec_dict,
            "error_analysis": _analyze_error(exec_dict),
        }

        # Optionally fetch process logs
        if fetch_logs:
            log_result = _fetch_process_log(sdk, execution_id, log_level)
            result["process_log"] = log_result

        return result

    if days < 0 or limit < 1:
        return {"_success": False, "error": "config.days must be >= 0 and config.limit must be >= 1"}

    if process_id:
        # Query recent errors for a process
        records = _query_error_executions(sdk, process_id=process_id, days=days, limit=limit)
        return {
            "_success": True,
            "total_count": len(records),
            "error_executions": records,
            "hint": "Use execution_id with error_details action to get detailed analysis for a specific execution",
        }

    else:
        # Query all recent errors
        records = _query_error_executions(sdk, days=days, limit=limit)
        return {
            "_success": True,
            "total_count": len(records),
            "error_executions": records,
            "hint": "Use execution_id with error_details action to get detailed analysis for a specific execution",
        }


def _analyze_error(exec_dict: Dict[str, Any]) -> Dict[str, Any]:
    """Analyze an execution record and provide error categorization."""
    analysis = {
        "error_category": "General Error",
        "severity": "Medium",
        "troubleshooting_tips": [],
    }

    status = (exec_dict.get("status") or "").upper()
    if status != "ERROR":
        analysis["error_category"] = f"Non-error status: {status}"
        analysis["severity"] = "Low"
        return analysis

    inbound = _safe_int(exec_dict.get("inbound_document_count", 0))
    outbound = _safe_int(exec_dict.get("outbound_document_count", 0))
    errors = _safe_int(exec_dict.get("inbound_error_document_count", 0))

    if errors > 0:
        analysis["error_category"] = "Document Processing Error"
        analysis["troubleshooting_tips"].extend([
            "Check input document format and structure",
            "Verify data mapping and transformation logic",
            "Review connector configuration and endpoints",
        ])
    elif inbound == 0:
        analysis["error_category"] = "Input Error"
        analysis["troubleshooting_tips"].extend([
            "Check source connector configuration",
            "Verify trigger conditions and scheduling",
            "Ensure data source availability",
        ])
    elif outbound == 0 and inbound > 0:
        analysis["error_category"] = "Processing Error"
        analysis["troubleshooting_tips"].extend([
            "Check process logic and data transformation",
            "Review decision shapes and routing conditions",
            "Verify output connector configuration",
        ])

    # Message-based analysis
    message = (exec_dict.get("message") or "").lower()
    if any(p in message for p in ["timeout", "connection", "network"]):
        analysis["error_category"] = "Connectivity Error"
        analysis["severity"] = "High"
        analysis["troubleshooting_tips"].append("Check network connectivity and endpoint availability")
    elif any(p in message for p in ["authentication", "unauthorized", "401", "403"]):
        analysis["error_category"] = "Authentication Error"
        analysis["severity"] = "High"
        analysis["troubleshooting_tips"].append("Verify credentials and permissions")

    if errors > 10:
        analysis["severity"] = "High"

    analysis["troubleshooting_tips"].append(
        "Use fetch_logs=true in config to download process logs for detailed stack traces"
    )

    return analysis


def _fetch_process_log(sdk: Boomi, execution_id: str, log_level: str = "ALL") -> Dict[str, Any]:
    """Request process log download URL for an execution."""
    from boomi.models import ProcessLog, LogLevel

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
    level = log_level_map.get(log_level.upper(), LogLevel.ALL)

    try:
        process_log = ProcessLog(execution_id=execution_id, log_level=level)
        result = sdk.process_log.create_process_log(request_body=process_log)

        status_code = int(getattr(result, "status_code", 0))
        download_url = getattr(result, "url", None)
        message = getattr(result, "message", "")

        if status_code == 202 and download_url:
            return {
                "_success": True,
                "status_code": status_code,
                "download_url": download_url,
                "message": message,
                "note": "Use download_url with Basic auth to retrieve log content",
            }
        elif status_code == 504:
            return {"_success": False, "error": "Runtime unavailable - Atom may be offline"}
        else:
            return {"_success": False, "status_code": status_code, "message": message}
    except Exception as e:
        return {"_success": False, "error": f"Failed to request process log: {e}"}


def _safe_int(value) -> int:
    """Safely convert value to int."""
    try:
        if isinstance(value, (int, float)):
            return int(value)
        if isinstance(value, str) and value.isdigit():
            return int(value)
        return 0
    except (ValueError, TypeError):
        return 0


# ============================================================================
# Action: retry
# ============================================================================

def handle_retry(sdk: Boomi, execution_id: str,
                 config: Dict[str, Any] = None) -> Dict[str, Any]:
    """Retry a failed execution by creating a new execution request with same parameters."""
    if not execution_id:
        return {"_success": False, "error": "execution_id is required for retry action"}

    if config is None:
        config = {}

    # Get the original execution record
    record = _query_execution_record(sdk, execution_id)
    if not record:
        return {"_success": False, "error": f"Execution record not found: {execution_id}"}

    exec_dict = _execution_to_dict(record)

    process_id = exec_dict.get("process_id")
    atom_id = exec_dict.get("atom_id")

    if not process_id or not atom_id:
        return {"_success": False, "error": "Cannot retry - missing process_id or atom_id in execution record"}

    # Build execution request
    return _create_execution_request(
        sdk, process_id, atom_id,
        dynamic_properties=config.get("dynamic_properties"),
        original_execution_id=execution_id,
        context="retry",
    )


# ============================================================================
# Action: reprocess
# ============================================================================

def handle_reprocess(sdk: Boomi, execution_id: str = None,
                     process_id: str = None, environment_id: str = None,
                     config: Dict[str, Any] = None) -> Dict[str, Any]:
    """Re-execute a process: either from a failed execution or by specifying process+environment."""
    if config is None:
        config = {}

    resolved_process_id = process_id
    resolved_atom_id = config.get("atom_id")

    if execution_id:
        # Get details from the failed execution
        record = _query_execution_record(sdk, execution_id)
        if not record:
            return {"_success": False, "error": f"Execution record not found: {execution_id}"}

        exec_dict = _execution_to_dict(record)
        resolved_process_id = resolved_process_id or exec_dict.get("process_id")
        resolved_atom_id = resolved_atom_id or exec_dict.get("atom_id")

    if not resolved_process_id:
        return {"_success": False, "error": "process_id is required (provide directly or via execution_id)"}

    if not resolved_atom_id:
        # Try to resolve from environment_id
        if environment_id:
            from boomi_mcp.categories.execution import _resolve_atom_id
            try:
                resolved_atom_id, error = _resolve_atom_id(sdk, environment_id)
            except Exception as e:
                return {"_success": False, "error": f"Failed to resolve atom_id: {e}"}
            if error:
                return {"_success": False, "error": error}
        else:
            return {
                "_success": False,
                "error": "atom_id required: provide atom_id in config, environment_id, or execution_id from a previous run",
            }

    return _create_execution_request(
        sdk, resolved_process_id, resolved_atom_id,
        dynamic_properties=config.get("dynamic_properties"),
        original_execution_id=execution_id,
        context="reprocess",
    )


def _create_execution_request(sdk: Boomi, process_id: str, atom_id: str,
                               dynamic_properties: Dict[str, str] = None,
                               original_execution_id: str = None,
                               context: str = "retry") -> Dict[str, Any]:
    """Create an execution request (shared by retry and reprocess)."""
    from boomi.models import (
        ExecutionRequest,
        ExecutionRequestDynamicProcessProperties,
        DynamicProcessProperty,
    )

    # Build dynamic properties
    prop_list = []
    if dynamic_properties:
        if not isinstance(dynamic_properties, dict):
            return {"_success": False, "error": "dynamic_properties must be a dict of {key: value}"}
        for key, value in dynamic_properties.items():
            prop_list.append(DynamicProcessProperty(name=str(key), value=str(value)))

    dynamic_props = ExecutionRequestDynamicProcessProperties(
        dynamic_process_property=prop_list if prop_list else None
    )

    execution_request = ExecutionRequest(
        process_id=process_id,
        atom_id=atom_id,
        dynamic_process_properties=dynamic_props,
    )

    try:
        result = sdk.execution_request.create_execution_request(request_body=execution_request)
    except ApiError as e:
        msg = _extract_api_error_msg(e)
        return {"_success": False, "error": f"{context.capitalize()} failed: {msg}"}

    # Extract request_id
    request_id = None
    if hasattr(result, "request_id"):
        request_id = result.request_id
    elif hasattr(result, "_kwargs"):
        raw = result._kwargs
        if "ExecutionRequest" in raw:
            request_id = raw["ExecutionRequest"].get("@requestId", raw["ExecutionRequest"].get("requestId"))
        else:
            request_id = raw.get("@requestId", raw.get("requestId", raw.get("request_id")))
    elif isinstance(result, dict):
        request_id = result.get("requestId", result.get("request_id"))
    elif isinstance(result, str):
        request_id = result

    if not request_id:
        return {
            "_success": False,
            "error": f"{context.capitalize()} request accepted but no request_id returned. Check Boomi execution history manually.",
        }

    response = {
        "_success": True,
        "context": context,
        "request_id": request_id,
        "process_id": process_id,
        "atom_id": atom_id,
    }

    if original_execution_id:
        response["original_execution_id"] = original_execution_id

    if dynamic_properties:
        response["dynamic_properties"] = dynamic_properties

    response["next_step"] = (
        f"Poll status: monitor_platform(action='execution_records', "
        f"config='{{\"execution_id\": \"{request_id}\"}}')"
    )

    return response


# ============================================================================
# Action: list_queues
# ============================================================================

def handle_list_queues(sdk: Boomi, config: Dict[str, Any] = None) -> Dict[str, Any]:
    """List all queues for a runtime using async operations."""
    if config is None:
        config = {}

    atom_id = config.get("atom_id")
    if not atom_id:
        return {"_success": False, "error": "atom_id is required in config for list_queues action"}

    try:
        timeout_seconds = int(config.get("timeout", 60))
    except (ValueError, TypeError):
        return {"_success": False, "error": "config.timeout must be a numeric value (seconds)"}

    try:
        # Step 1: Initiate async list queues operation
        token_result = sdk.list_queues.async_get_list_queues(id_=atom_id)

        if not hasattr(token_result, "async_token") or not hasattr(token_result.async_token, "token"):
            return {"_success": False, "error": "Failed to get async operation token"}

        token = token_result.async_token.token

        # Step 2: Poll for results
        start_time = time.time()
        while time.time() - start_time < timeout_seconds:
            try:
                response = sdk.list_queues.async_token_list_queues(token=token)

                if hasattr(response, "result") and response.result:
                    queues = _parse_queue_response(response.result)
                    return {
                        "_success": True,
                        "atom_id": atom_id,
                        "total_queues": len(queues),
                        "queues": queues,
                    }

                time.sleep(2)

            except Exception as poll_error:
                if "still processing" in str(poll_error).lower():
                    time.sleep(2)
                    continue
                raise poll_error

        return {"_success": False, "error": f"Timeout after {timeout_seconds}s waiting for queue list"}

    except ApiError as e:
        msg = _extract_api_error_msg(e)
        return {"_success": False, "error": f"Failed to list queues: {msg}"}
    except Exception as e:
        return {"_success": False, "error": f"Failed to list queues: {e}"}


def _parse_queue_response(queue_results) -> List[Dict[str, Any]]:
    """Parse the async queue response into a usable format."""
    queues = []
    for result in queue_results:
        if hasattr(result, "queue_record") and result.queue_record:
            for queue_record in result.queue_record:
                queue_info = {
                    "name": getattr(queue_record, "queue_name", "Unknown"),
                    "type": getattr(queue_record, "queue_type", "Unknown"),
                    "message_count": getattr(queue_record, "messages_count", 0),
                    "dead_letter_count": getattr(queue_record, "dead_letters_count", 0),
                }

                # Add subscriber information for topics
                subscribers = []
                if hasattr(queue_record, "topic_subscribers") and queue_record.topic_subscribers:
                    for subscriber in queue_record.topic_subscribers:
                        subscribers.append({
                            "name": getattr(subscriber, "subscriber_name", "Unknown"),
                            "message_count": getattr(subscriber, "message_count", 0),
                        })
                if subscribers:
                    queue_info["subscribers"] = subscribers

                queues.append(queue_info)
    return queues


# ============================================================================
# Action: clear_queue
# ============================================================================

def handle_clear_queue(sdk: Boomi, config: Dict[str, Any] = None) -> Dict[str, Any]:
    """Clear messages from a queue."""
    if config is None:
        config = {}

    atom_id = config.get("atom_id")
    queue_name = config.get("queue_name")

    if not atom_id:
        return {"_success": False, "error": "atom_id is required in config for clear_queue action"}
    if not queue_name:
        return {"_success": False, "error": "queue_name is required in config for clear_queue action"}

    clear_dlq = config.get("dlq", False)
    subscriber_name = config.get("subscriber_name")

    try:
        from boomi.models import ClearQueueRequest

        clear_request = ClearQueueRequest(
            atom_id=atom_id,
            queue_name=queue_name,
            dlq=clear_dlq,
        )

        if subscriber_name:
            clear_request.subscriber_name = subscriber_name

        sdk.clear_queue.execute_clear_queue(
            id_=atom_id,
            request_body=clear_request,
        )

        return {
            "_success": True,
            "atom_id": atom_id,
            "queue_name": queue_name,
            "dlq": clear_dlq,
            "message": f"Clear queue operation submitted for '{queue_name}'" + (" (DLQ)" if clear_dlq else ""),
            "next_step": "Use list_queues action to verify the queue has been cleared",
        }

    except ApiError as e:
        msg = _extract_api_error_msg(e)
        return {"_success": False, "error": f"Failed to clear queue: {msg}"}
    except Exception as e:
        return {"_success": False, "error": f"Failed to clear queue: {e}"}


# ============================================================================
# Action: move_queue
# ============================================================================

def handle_move_queue(sdk: Boomi, config: Dict[str, Any] = None) -> Dict[str, Any]:
    """Move messages from one queue to another."""
    if config is None:
        config = {}

    atom_id = config.get("atom_id")
    source_queue = config.get("source_queue")
    dest_queue = config.get("dest_queue")

    if not atom_id:
        return {"_success": False, "error": "atom_id is required in config for move_queue action"}
    if not source_queue:
        return {"_success": False, "error": "source_queue is required in config for move_queue action"}
    if not dest_queue:
        return {"_success": False, "error": "dest_queue is required in config for move_queue action"}

    source_dlq = config.get("source_dlq", False)
    dest_dlq = config.get("dest_dlq", False)
    source_subscriber = config.get("source_subscriber")
    dest_subscriber = config.get("dest_subscriber")

    try:
        from boomi.models import MoveQueueRequest, QueueAttributes

        source_attrs = QueueAttributes(
            dlq=source_dlq,
            queue_name=source_queue,
        )
        if source_subscriber:
            source_attrs.subscriber_name = source_subscriber

        dest_attrs = QueueAttributes(
            dlq=dest_dlq,
            queue_name=dest_queue,
        )
        if dest_subscriber:
            dest_attrs.subscriber_name = dest_subscriber

        move_request = MoveQueueRequest(
            atom_id=atom_id,
            source_queue=source_attrs,
            destination_queue=dest_attrs,
        )

        sdk.move_queue_request.create_move_queue_request(request_body=move_request)

        return {
            "_success": True,
            "atom_id": atom_id,
            "source_queue": source_queue,
            "dest_queue": dest_queue,
            "source_dlq": source_dlq,
            "dest_dlq": dest_dlq,
            "message": f"Move queue operation submitted: '{source_queue}' -> '{dest_queue}'",
            "next_step": "Use list_queues action to verify message counts",
        }

    except ApiError as e:
        msg = _extract_api_error_msg(e)
        return {"_success": False, "error": f"Failed to move queue: {msg}"}
    except Exception as e:
        return {"_success": False, "error": f"Failed to move queue: {e}"}


# ============================================================================
# Consolidated Action Router
# ============================================================================

def troubleshoot_execution_action(
    sdk: Boomi,
    action: str,
    execution_id: str = None,
    process_id: str = None,
    environment_id: str = None,
    config: Dict[str, Any] = None,
) -> Dict[str, Any]:
    """
    Consolidated troubleshooting function. Routes to handler based on action.

    Args:
        sdk: Authenticated Boomi SDK client
        action: One of: error_details, retry, reprocess, list_queues, clear_queue
        execution_id: Execution ID (for error_details, retry, reprocess)
        process_id: Process ID (for error_details, reprocess)
        environment_id: Environment ID (for reprocess)
        config: Action-specific configuration dict

    Returns:
        Action result dict with _success status
    """
    if config is None:
        config = {}

    try:
        if action == "error_details":
            return handle_error_details(sdk, execution_id=execution_id,
                                        process_id=process_id, config=config)
        elif action == "retry":
            return handle_retry(sdk, execution_id=execution_id, config=config)
        elif action == "reprocess":
            return handle_reprocess(sdk, execution_id=execution_id,
                                    process_id=process_id,
                                    environment_id=environment_id, config=config)
        elif action == "list_queues":
            return handle_list_queues(sdk, config=config)
        elif action == "clear_queue":
            return handle_clear_queue(sdk, config=config)
        elif action == "move_queue":
            return handle_move_queue(sdk, config=config)
        else:
            return {
                "_success": False,
                "error": f"Unknown action: {action}",
                "valid_actions": ["error_details", "retry", "reprocess", "list_queues", "clear_queue", "move_queue"],
            }

    except Exception as e:
        return {
            "_success": False,
            "error": f"Action '{action}' failed: {str(e)}",
            "exception_type": type(e).__name__,
        }

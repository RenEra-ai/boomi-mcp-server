"""
Execution MCP Tool for Boomi Platform.

Provides process execution via the Boomi execution request API.
This is a dedicated tool (not merged into manage_process) because:
- Uses sdk.execution_request (JSON API), not sdk.component (XML API)
- Different parameter shape (process_id, environment_id, atom_id vs process config JSON)
- Purely destructive (triggers real side effects)
- MCP atomic principle: single focused operation

SDK example reference: boomi-python/examples/08_execute_test/execute_process.py
Poll example reference: boomi-python/examples/09_monitor_validate/poll_execution_status.py
"""

import time
from typing import Dict, Any, Optional, Tuple

from boomi import Boomi
from boomi.net.transport.api_error import ApiError
from boomi.models import (
    ExecutionRequest,
    ExecutionRequestDynamicProcessProperties,
    ExecutionRequestProcessProperties,
    DynamicProcessProperty,
    ProcessProperty,
    ProcessPropertyValue,
    EnvironmentAtomAttachmentQueryConfig,
    EnvironmentAtomAttachmentQueryConfigQueryFilter,
    EnvironmentAtomAttachmentSimpleExpression,
    EnvironmentAtomAttachmentSimpleExpressionOperator,
    EnvironmentAtomAttachmentSimpleExpressionProperty,
    ExecutionRecordQueryConfig,
    ExecutionRecordQueryConfigQueryFilter,
    ExecutionRecordSimpleExpression,
    ExecutionRecordSimpleExpressionOperator,
    ExecutionRecordSimpleExpressionProperty,
)


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


def _resolve_atom_id(sdk: Boomi, environment_id: str) -> tuple:
    """Find the single runtime attached to an environment.

    Returns (atom_id, error_string). If error_string is set, atom_id is None.
    """
    expression = EnvironmentAtomAttachmentSimpleExpression(
        operator=EnvironmentAtomAttachmentSimpleExpressionOperator.EQUALS,
        property=EnvironmentAtomAttachmentSimpleExpressionProperty.ENVIRONMENTID,
        argument=[environment_id],
    )
    query_filter = EnvironmentAtomAttachmentQueryConfigQueryFilter(
        expression=expression
    )
    query_config = EnvironmentAtomAttachmentQueryConfig(query_filter=query_filter)

    result = sdk.environment_atom_attachment.query_environment_atom_attachment(
        request_body=query_config
    )

    attachments = []

    def _collect(res):
        if hasattr(res, "result") and res.result:
            items = res.result if isinstance(res.result, list) else [res.result]
            attachments.extend(
                [getattr(a, "atom_id", "") for a in items if getattr(a, "atom_id", "")]
            )

    _collect(result)
    while hasattr(result, "query_token") and result.query_token:
        result = sdk.environment_atom_attachment.query_more_environment_atom_attachment(
            request_body=result.query_token
        )
        _collect(result)

    if len(attachments) == 0:
        return None, (
            f"No runtime attached to environment '{environment_id}'. "
            "Attach a runtime first using manage_runtimes(action='attach')."
        )
    if len(attachments) > 1:
        return None, (
            f"Multiple runtimes attached to environment '{environment_id}': {attachments}. "
            "Specify atom_id to choose which runtime to execute on."
        )
    return attachments[0], None


def _build_dynamic_properties(props_dict: Optional[Dict[str, str]]):
    """Build ExecutionRequestDynamicProcessProperties from a flat dict."""
    if not props_dict:
        return ExecutionRequestDynamicProcessProperties()

    if not isinstance(props_dict, dict):
        raise ValueError("dynamic_properties must be a dict of {key: value}")

    prop_list = [
        DynamicProcessProperty(name=str(k), value=str(v))
        for k, v in props_dict.items()
    ]
    return ExecutionRequestDynamicProcessProperties(
        dynamic_process_property=prop_list
    )


def _build_process_properties(props_dict: Optional[Dict[str, Dict[str, str]]]):
    """Build ExecutionRequestProcessProperties from nested dict.

    Format: {"component_id": {"key": "value", ...}, ...}
    Maps to ProcessProperty(component_id=..., process_property_value=[ProcessPropertyValue(key=..., value=...)])
    """
    if not props_dict:
        return ExecutionRequestProcessProperties()

    if not isinstance(props_dict, dict):
        raise ValueError("process_properties must be a dict of {componentId: {key: value}}")

    prop_list = []
    for component_id, values in props_dict.items():
        if not isinstance(values, dict):
            raise ValueError(
                f"process_properties['{component_id}'] must be a dict of {{key: value}}, got {type(values).__name__}"
            )
        ppv_list = [
            ProcessPropertyValue(key=str(k), value=str(v))
            for k, v in values.items()
        ]
        prop_list.append(
            ProcessProperty(component_id=str(component_id), process_property_value=ppv_list)
        )
    return ExecutionRequestProcessProperties(process_property=prop_list)


_TERMINAL_STATUSES = {"COMPLETE", "ERROR", "ABORTED", "COMPLETE_WARN"}


def _poll_execution_status(
    sdk: Boomi,
    execution_id: str,
    timeout: int = 300,
) -> Dict[str, Any]:
    """Poll execution_record.query() until status is terminal or timeout.

    Poll interval: 2s initially, backs off to 5s after 30s elapsed.
    Returns dict with execution details and polling metadata.
    """
    start = time.monotonic()
    poll_count = 0

    while True:
        elapsed = time.monotonic() - start
        if elapsed >= timeout:
            return {
                "poll_status": "TIMEOUT",
                "elapsed_seconds": round(elapsed, 1),
                "poll_count": poll_count,
                "message": f"Timed out after {timeout}s waiting for execution to complete",
            }

        poll_count += 1

        try:
            query_expression = ExecutionRecordSimpleExpression(
                operator=ExecutionRecordSimpleExpressionOperator.EQUALS,
                property=ExecutionRecordSimpleExpressionProperty.EXECUTIONID,
                argument=[execution_id],
            )
            query_filter = ExecutionRecordQueryConfigQueryFilter(
                expression=query_expression
            )
            query_config = ExecutionRecordQueryConfig(query_filter=query_filter)

            result = sdk.execution_record.query_execution_record(
                request_body=query_config
            )

            if result and hasattr(result, "result") and result.result:
                record = result.result[0]
                status = getattr(record, "status", "UNKNOWN")

                if status.upper() in _TERMINAL_STATUSES:
                    def _safe_int(val, default=0):
                        try:
                            return int(val)
                        except (ValueError, TypeError):
                            return default

                    return {
                        "poll_status": "COMPLETED",
                        "elapsed_seconds": round(time.monotonic() - start, 1),
                        "poll_count": poll_count,
                        "execution_id": getattr(record, "execution_id", execution_id),
                        "status": status,
                        "process_name": getattr(record, "process_name", None),
                        "atom_name": getattr(record, "atom_name", None),
                        "execution_time": getattr(record, "execution_time", None),
                        "execution_duration": getattr(record, "execution_duration", None),
                        "inbound_document_count": _safe_int(getattr(record, "inbound_document_count", 0)),
                        "outbound_document_count": _safe_int(getattr(record, "outbound_document_count", 0)),
                        "inbound_error_document_count": _safe_int(getattr(record, "inbound_error_document_count", 0)),
                        "error": getattr(record, "error", None),
                    }
        except Exception:
            # Transient errors — keep polling
            pass

        # Back off: 2s for first 30s, then 5s
        interval = 2 if elapsed < 30 else 5
        # Don't sleep past the timeout
        remaining = timeout - (time.monotonic() - start)
        if remaining <= 0:
            continue
        time.sleep(min(interval, remaining))


def execute_process_action(
    sdk: Boomi,
    profile: str,
    process_id: str,
    environment_id: str = None,
    atom_id: str = None,
    config_data: Dict[str, Any] = None,
) -> Dict[str, Any]:
    """Execute a Boomi process."""
    if config_data is None:
        config_data = {}

    # Validate: need at least one of atom_id or environment_id
    if not atom_id and not environment_id:
        return {
            "_success": False,
            "error": "Either atom_id or environment_id is required. "
                     "Provide atom_id to target a specific runtime, or "
                     "environment_id to auto-resolve the runtime.",
        }

    # Resolve atom_id if not provided
    if not atom_id:
        try:
            atom_id, error = _resolve_atom_id(sdk, environment_id)
        except ApiError as e:
            msg = _extract_api_error_msg(e)
            return {"_success": False, "error": f"Failed to resolve atom_id: {msg}"}
        except Exception as e:
            return {"_success": False, "error": f"Failed to resolve atom_id: {e}"}
        if error:
            return {"_success": False, "error": error}

    # Build properties
    try:
        dynamic_props = _build_dynamic_properties(config_data.get("dynamic_properties"))
        process_props = _build_process_properties(config_data.get("process_properties"))
    except (ValueError, TypeError, AttributeError) as e:
        return {"_success": False, "error": str(e)}

    # Create execution request
    execution_request = ExecutionRequest(
        atom_id=atom_id,
        process_id=process_id,
        dynamic_process_properties=dynamic_props,
        process_properties=process_props,
    )

    try:
        result = sdk.execution_request.create_execution_request(
            request_body=execution_request
        )
    except ApiError as e:
        msg = _extract_api_error_msg(e)
        return {"_success": False, "error": f"Execution failed: {msg}"}

    # Parse response — extract request_id
    request_id = None
    if hasattr(result, "request_id"):
        request_id = result.request_id
    elif hasattr(result, "_kwargs"):
        raw = result._kwargs
        if "ExecutionRequest" in raw:
            request_id = raw["ExecutionRequest"].get(
                "@requestId", raw["ExecutionRequest"].get("requestId")
            )
        else:
            request_id = raw.get(
                "@requestId", raw.get("requestId", raw.get("request_id"))
            )
    elif isinstance(result, dict):
        request_id = result.get("requestId", result.get("request_id"))
    elif isinstance(result, str):
        request_id = result

    if not request_id:
        return {
            "_success": False,
            "error": "Execution request accepted but no request_id returned. Check Boomi execution history manually.",
        }

    response = {
        "_success": True,
        "request_id": request_id,
        "process_id": process_id,
        "environment_id": environment_id,
        "atom_id": atom_id,
    }

    if config_data.get("dynamic_properties"):
        response["dynamic_properties"] = config_data["dynamic_properties"]

    # If wait=True, poll for completion before returning
    if config_data.get("wait") and request_id:
        try:
            poll_timeout = int(config_data.get("timeout", 300))
        except (ValueError, TypeError):
            return {"_success": False, "error": "config.timeout must be a numeric value (seconds)"}
        poll_result = _poll_execution_status(sdk, request_id, timeout=poll_timeout)
        response["execution_result"] = poll_result

        # Reflect terminal status in top-level success flag
        if poll_result.get("poll_status") == "COMPLETED":
            final_status = poll_result.get("status", "").upper()
            if final_status in ("ERROR", "ABORTED"):
                response["_success"] = False
                response["error"] = poll_result.get("error") or f"Execution ended with status: {final_status}"
        elif poll_result.get("poll_status") == "TIMEOUT":
            response["_success"] = False
            response["error"] = poll_result["message"]
    else:
        response["next_step"] = (
            f"Poll status: monitor_platform(action='execution_records', "
            f"config='{{\"execution_id\": \"{request_id}\"}}')"
        )

    return response

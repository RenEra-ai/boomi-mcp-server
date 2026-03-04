"""
Execution MCP Tool for Boomi Platform.

Provides process execution via the Boomi execution request API.
This is a dedicated tool (not merged into manage_process) because:
- Uses sdk.execution_request (JSON API), not sdk.component (XML API)
- Different parameter shape (process_id, environment_id, atom_id vs config_yaml)
- Purely destructive (triggers real side effects)
- MCP atomic principle: single focused operation

SDK example reference: boomi-python/examples/08_execute_test/execute_process.py
"""

from typing import Dict, Any, Optional

from boomi import Boomi
from boomi.net.transport.api_error import ApiError
from boomi.models import (
    ExecutionRequest,
    ExecutionRequestDynamicProcessProperties,
    ExecutionRequestProcessProperties,
    DynamicProcessProperty,
    EnvironmentAtomAttachmentQueryConfig,
    EnvironmentAtomAttachmentQueryConfigQueryFilter,
    EnvironmentAtomAttachmentSimpleExpression,
    EnvironmentAtomAttachmentSimpleExpressionOperator,
    EnvironmentAtomAttachmentSimpleExpressionProperty,
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
    if hasattr(result, "result") and result.result:
        items = result.result if isinstance(result.result, list) else [result.result]
        attachments = [getattr(a, "atom_id", "") for a in items if getattr(a, "atom_id", "")]

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

    prop_list = [
        DynamicProcessProperty(name=str(k), value=str(v))
        for k, v in props_dict.items()
    ]
    return ExecutionRequestDynamicProcessProperties(
        dynamic_process_property=prop_list
    )


def execute_process_action(
    sdk: Boomi,
    profile: str,
    process_id: str,
    environment_id: str,
    atom_id: str = None,
    config_data: Dict[str, Any] = None,
) -> Dict[str, Any]:
    """Execute a Boomi process."""
    if config_data is None:
        config_data = {}

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
    dynamic_props = _build_dynamic_properties(config_data.get("dynamic_properties"))
    process_props = ExecutionRequestProcessProperties()

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

    response = {
        "_success": True,
        "request_id": request_id,
        "process_id": process_id,
        "environment_id": environment_id,
        "atom_id": atom_id,
    }

    if config_data.get("dynamic_properties"):
        response["dynamic_properties"] = config_data["dynamic_properties"]

    response["next_step"] = (
        f"Poll status: monitor_platform(action='execution_records', "
        f"config='{{\"execution_id\": \"{request_id}\"}}')"
    )

    return response

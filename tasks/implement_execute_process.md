# Task: Implement `execute_process` Tool

**Date**: 2026-03-04
**Category**: 4 — Execution & Scheduling
**Priority**: High (core workflow — users need to run processes after building and deploying them)
**Estimated Effort**: 1 agent session (~200-350 lines of new code)
**Design Doc Reference**: `MCP_TOOL_DESIGN.md` line 827+
**Status**: ⏳ Not Started

---

## Overview

Implement the `execute_process` MCP tool as a **dedicated, separate tool** (not merged into `manage_process`).

**Why separate** (validated via MCP best practices):
1. **Different SDK service**: `manage_process` uses `sdk.component.*` (XML component CRUD). `execute_process` uses `sdk.execution_request.create_execution_request()` (JSON execution API). Completely different APIs.
2. **Different parameter shape**: `manage_process` takes `config_yaml` and `filters`. `execute_process` needs `process_id`, `environment_id`, `atom_id`, `dynamic_properties` — no overlap except `process_id`.
3. **MCP atomic principle**: Process execution is a single focused operation, not a CRUD family. The MCP best practice "keep tool operations focused and atomic" directly favors a dedicated tool.
4. **Annotations differ**: `manage_process` has mixed read/write. `execute_process` is purely `destructiveHint: true` (triggers real side effects).
5. **Agent discoverability**: `execute_process` is instantly clear when the agent needs to run a process. `manage_process(action="execute")` is less discoverable.
6. **Design doc confirms**: execute_process is listed under "Neither (few params)" pattern — it doesn't use config/action routing.

The tool covers 1 SDK example file and works as a natural pair with the existing `monitor_platform` tool:

| Example File | Maps to |
|---|---|
| `execute_process.py` | `execute_process` tool |
| `poll_execution_status.py` | Already in `monitor_platform` action="execution_records" |
| `retry_failed_execution.py` | Agent calls `execute_process` again with same params |

**Typical agent workflow**:
1. `execute_process(process_id=..., environment_id=...)` → returns `request_id`
2. `monitor_platform(action="execution_records", config='{"execution_id":"..."}')` → check status
3. If failed: `monitor_platform(action="execution_logs", config='{"execution_id":"..."}')` → debug

---

## SDK Example

**GitHub Path**: `RenEra-ai/boomi-python/examples/08_execute_test/execute_process.py`

**Key SDK imports and patterns** (from the example):

```python
from boomi.models import (
    ExecutionRequest,
    ExecutionRequestDynamicProcessProperties,
    ExecutionRequestProcessProperties,
    DynamicProcessProperty,
)
```

**SDK call pattern**:
```python
# Build dynamic properties
dynamic_property_list = [
    DynamicProcessProperty(name=key, value=value)
    for key, value in dynamic_properties.items()
]
dynamic_props = ExecutionRequestDynamicProcessProperties(
    dynamic_process_property=dynamic_property_list
)

# Create execution request
execution_request = ExecutionRequest(
    atom_id=atom_id,
    process_id=process_id,
    dynamic_process_properties=dynamic_props,
    process_properties=ExecutionRequestProcessProperties(),
)

# Execute
result = sdk.execution_request.create_execution_request(
    request_body=execution_request
)
# Result contains request_id for status polling
```

**SDK service**: `sdk.execution_request.create_execution_request()`

---

## MCP Tool Signature

```python
@mcp.tool(annotations={"destructiveHint": True, "openWorldHint": True})
def execute_process(
    profile: str,
    process_id: str,
    environment_id: str,
    atom_id: str = None,
    config: str = None,
) -> dict:
    """Execute a Boomi process on a runtime.

    Args:
        profile: Boomi profile name (required)
        process_id: The process component ID to execute (required)
        environment_id: The environment to execute in (required)
        atom_id: Runtime/Atom ID to execute on. If omitted, uses the runtime
                 attached to the environment (fails if multiple are attached).
        config: JSON string with optional parameters:
            dynamic_properties: Dict of key-value pairs passed to the process
            process_properties: Dict of component-scoped property overrides
            notes: Execution notes

    RECOMMENDED WORKFLOW:
      1. execute_process(...) → returns request_id
      2. monitor_platform(action="execution_records", config='{"execution_id":"<request_id>"}')
         → poll until status is COMPLETE or ERROR
      3. If ERROR: monitor_platform(action="execution_logs", config='{"execution_id":"<request_id>"}')

    Examples:
        Basic execution:
            process_id="abc-123", environment_id="env-456"

        With specific atom:
            process_id="abc-123", environment_id="env-456", atom_id="atom-789"

        With dynamic properties:
            process_id="abc-123", environment_id="env-456",
            config='{"dynamic_properties": {"inputFile": "/data/orders.csv", "batchSize": "100"}}'

        With process properties:
            config='{"process_properties": {"MyConnection": {"url": "https://api.example.com"}}}'

    Returns:
        request_id for status polling via monitor_platform
    """
```

**Note on `atom_id` auto-resolution**: If `atom_id` is not provided, the tool should attempt to find the runtime attached to the environment using `sdk.environment_atom_attachment.query_environment_atom_attachment()`. If exactly one attachment exists, use that atom_id. If zero or multiple, return a helpful error.

---

## Implementation Details

### File Location

```
src/boomi_mcp/categories/execution.py    # NEW file
```

Update `src/boomi_mcp/categories/__init__.py` to export `execute_process_action`.

### Core Function

```python
def execute_process_action(
    sdk: Boomi,
    profile: str,
    process_id: str,
    environment_id: str,
    atom_id: str = None,
    config_data: Dict[str, Any] = None,
) -> Dict[str, Any]:
```

### Auto-resolve atom_id

```python
def _resolve_atom_id(sdk, environment_id: str) -> tuple:
    """Find the single runtime attached to an environment.
    Returns (atom_id, error_string). If error_string is set, atom_id is None.
    """
    from boomi.models import (
        EnvironmentAtomAttachmentQueryConfig,
        EnvironmentAtomAttachmentQueryConfigQueryFilter,
        EnvironmentAtomAttachmentSimpleExpression,
        EnvironmentAtomAttachmentSimpleExpressionOperator,
        EnvironmentAtomAttachmentSimpleExpressionProperty,
    )

    expression = EnvironmentAtomAttachmentSimpleExpression(
        operator=EnvironmentAtomAttachmentSimpleExpressionOperator.EQUALS,
        property=EnvironmentAtomAttachmentSimpleExpressionProperty.ENVIRONMENTID,
        argument=[environment_id],
    )
    query_filter = EnvironmentAtomAttachmentQueryConfigQueryFilter(expression=expression)
    query_config = EnvironmentAtomAttachmentQueryConfig(query_filter=query_filter)

    result = sdk.environment_atom_attachment.query_environment_atom_attachment(
        request_body=query_config
    )

    attachments = []
    if hasattr(result, 'result') and result.result:
        items = result.result if isinstance(result.result, list) else [result.result]
        attachments = [getattr(a, 'atom_id', '') for a in items]

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
```

### Build dynamic properties

```python
def _build_dynamic_properties(props_dict: Dict[str, str]):
    """Build ExecutionRequestDynamicProcessProperties from a flat dict."""
    from boomi.models import (
        ExecutionRequestDynamicProcessProperties,
        DynamicProcessProperty,
    )
    if not props_dict:
        return ExecutionRequestDynamicProcessProperties()

    prop_list = [
        DynamicProcessProperty(name=str(k), value=str(v))
        for k, v in props_dict.items()
    ]
    return ExecutionRequestDynamicProcessProperties(
        dynamic_process_property=prop_list
    )
```

### Build process properties

```python
def _build_process_properties(props_dict: Dict[str, Dict[str, str]]):
    """Build ExecutionRequestProcessProperties from nested dict.

    Format: {"ComponentName": {"propertyName": "value"}}
    """
    from boomi.models import ExecutionRequestProcessProperties
    # Process properties structure depends on SDK — may need ProcessProperty model
    # If SDK supports it, build from dict. Otherwise pass empty.
    if not props_dict:
        return ExecutionRequestProcessProperties()
    # TODO: Check SDK for ProcessProperty model availability
    return ExecutionRequestProcessProperties()
```

### Main execution function

```python
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
        atom_id, error = _resolve_atom_id(sdk, environment_id)
        if error:
            return {"_success": False, "error": error}

    # Build properties
    dynamic_props = _build_dynamic_properties(config_data.get("dynamic_properties"))
    process_props = _build_process_properties(config_data.get("process_properties"))

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
    if hasattr(result, 'request_id'):
        request_id = result.request_id
    elif hasattr(result, '_kwargs'):
        raw = result._kwargs
        if 'ExecutionRequest' in raw:
            request_id = raw['ExecutionRequest'].get('@requestId', raw['ExecutionRequest'].get('requestId'))
        else:
            request_id = raw.get('@requestId', raw.get('requestId', raw.get('request_id')))
    elif isinstance(result, dict):
        request_id = result.get('requestId', result.get('request_id'))
    elif isinstance(result, str):
        request_id = result

    response = {
        "_success": True,
        "request_id": request_id,
        "process_id": process_id,
        "environment_id": environment_id,
        "atom_id": atom_id,
    }

    if config_data.get("notes"):
        response["notes"] = config_data["notes"]

    response["next_step"] = (
        f"Poll status: monitor_platform(action='execution_records', "
        f"config='{{\"execution_id\": \"{request_id}\"}}')"
    )

    return response
```

### Error helper (reuse pattern from runtimes.py)

```python
def _extract_api_error_msg(e) -> str:
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
```

---

## server.py Registration

```python
try:
    from src.boomi_mcp.categories.execution import execute_process_action
except ImportError:
    execute_process_action = None

if execute_process_action:
    @mcp.tool(annotations={"destructiveHint": True, "openWorldHint": True})
    def execute_process(
        profile: str,
        process_id: str,
        environment_id: str,
        atom_id: str = None,
        config: str = None,
    ):
        """Execute a Boomi process on a runtime.

        Args:
            profile: Boomi profile name (required)
            process_id: Process component ID to execute (required)
            environment_id: Environment to execute in (required)
            atom_id: Runtime ID (auto-detected if only one attached to environment)
            config: JSON string with optional parameters

        RECOMMENDED WORKFLOW:
          1. execute_process(...) → returns request_id
          2. monitor_platform(action="execution_records", config='{"execution_id":"<request_id>"}')
          3. If ERROR: monitor_platform(action="execution_logs", config='{"execution_id":"<request_id>"}')

        Examples:

            Basic:
                process_id="abc-123", environment_id="env-456"

            With dynamic properties:
                process_id="abc-123", environment_id="env-456",
                config='{"dynamic_properties": {"inputFile": "/data/orders.csv"}}'

        Config fields:
            dynamic_properties: Dict of key-value pairs (e.g. {"key": "value"})
            process_properties: Dict of component property overrides
            notes: Execution notes/description

        Returns:
            request_id for polling via monitor_platform(action="execution_records")
        """
        config_data = {}
        if config:
            try:
                config_data = json.loads(config)
            except (json.JSONDecodeError, TypeError) as e:
                return {"_success": False, "error": f"Invalid config JSON: {e}"}
            if not isinstance(config_data, dict):
                return {"_success": False, "error": "config must be a JSON object"}

        try:
            subject = get_current_user()
            print(f"[INFO] execute_process called by user: {subject}, profile: {profile}")
            creds = get_secret(subject, profile)

            sdk_params = {
                "account_id": creds["account_id"],
                "username": creds["username"],
                "password": creds["password"],
                "timeout": 30000,
            }
            if creds.get("base_url"):
                sdk_params["base_url"] = creds["base_url"]
            sdk = Boomi(**sdk_params)

            return execute_process_action(
                sdk, profile, process_id, environment_id,
                atom_id=atom_id, config_data=config_data,
            )

        except Exception as e:
            print(f"[ERROR] execute_process failed: {e}")
            import traceback
            traceback.print_exc()
            return {"_success": False, "error": str(e), "exception_type": type(e).__name__}

    print("[INFO] Execute process tool registered successfully")
```

---

## Design Document Updates

After implementation, update `MCP_TOOL_DESIGN.md`:

1. **Line 827**: Mark `execute_process` as ✅ Implemented
2. **Phase 3 tasks**: Mark execute_process as done
3. **Tool count**: Verify total tool count is accurate

---

## Test Script

Save as `test_execute_process.py` in the project root for `.fn()` testing:

```python
#!/usr/bin/env python3
"""Test execute_process tool via direct .fn() calls."""
import os, json
os.environ["BOOMI_LOCAL"] = "true"

from server import execute_process

# ============================================================
# VALIDATION TESTS (safe — no actual execution)
# ============================================================

# Test 1: Missing process_id (should fail — it's required in signature)
# Note: process_id is a required param, so this tests at server.py level
print("=" * 60)
print("TEST 1: Basic parameter validation")
# Can't easily test missing required params with .fn() — skip to config tests

# Test 2: Invalid JSON config
print("\n" + "=" * 60)
print("TEST 2: Invalid JSON config")
result = execute_process.fn(
    profile="dev",
    process_id="test-proc",
    environment_id="test-env",
    config="{bad json}"
)
print(json.dumps(result, indent=2, default=str)[:500])
assert result.get("_success") is False
assert "Invalid config" in result.get("error", "")

# Test 3: Invalid config type
print("\n" + "=" * 60)
print("TEST 3: Config must be JSON object")
result = execute_process.fn(
    profile="dev",
    process_id="test-proc",
    environment_id="test-env",
    config='"just a string"'
)
assert result.get("_success") is False

# ============================================================
# INTEGRATION TESTS (require real Boomi account)
# ============================================================

# Test 4: Execute with invalid process_id (should get API error)
print("\n" + "=" * 60)
print("TEST 4: Execute with invalid process_id")
result = execute_process.fn(
    profile="dev",
    process_id="00000000-0000-0000-0000-000000000000",
    environment_id="00000000-0000-0000-0000-000000000000",
    atom_id="00000000-0000-0000-0000-000000000000",
)
print(json.dumps(result, indent=2, default=str)[:500])
assert result.get("_success") is False  # Should fail with API error

# Test 5: Execute with auto-resolved atom_id (no atom_id param)
print("\n" + "=" * 60)
print("TEST 5: Auto-resolve atom_id (invalid env → should get clear error)")
result = execute_process.fn(
    profile="dev",
    process_id="00000000-0000-0000-0000-000000000000",
    environment_id="00000000-0000-0000-0000-000000000000",
)
print(json.dumps(result, indent=2, default=str)[:500])
assert result.get("_success") is False

# Test 6: Execute with dynamic properties (invalid IDs, but tests JSON parsing)
print("\n" + "=" * 60)
print("TEST 6: Dynamic properties parsing")
result = execute_process.fn(
    profile="dev",
    process_id="00000000-0000-0000-0000-000000000000",
    environment_id="00000000-0000-0000-0000-000000000000",
    atom_id="00000000-0000-0000-0000-000000000000",
    config='{"dynamic_properties": {"inputFile": "/data/test.csv", "batchSize": "50"}}'
)
print(json.dumps(result, indent=2, default=str)[:500])
# Will fail at API level but proves config parsing works

# ============================================================
# REAL EXECUTION TESTS (uncomment with real IDs)
# ============================================================

# Test R1: Execute a real process
# Replace with actual IDs from your Boomi account:
# PROCESS_ID = "YOUR-PROCESS-ID"
# ENV_ID = "YOUR-ENVIRONMENT-ID"
# ATOM_ID = "YOUR-ATOM-ID"  # optional if only one attached
#
# print("\n" + "=" * 60)
# print("TEST R1: Execute real process")
# result = execute_process.fn(
#     profile="dev",
#     process_id=PROCESS_ID,
#     environment_id=ENV_ID,
#     atom_id=ATOM_ID,
# )
# print(json.dumps(result, indent=2, default=str))
# assert result.get("_success") is True
# assert result.get("request_id") is not None
# print(f"Request ID: {result['request_id']}")
# print(f"Next step: {result.get('next_step', '')}")

# Test R2: Execute with dynamic properties
# result = execute_process.fn(
#     profile="dev",
#     process_id=PROCESS_ID,
#     environment_id=ENV_ID,
#     config='{"dynamic_properties": {"testMode": "true", "outputDir": "/tmp/test"}}'
# )
# assert result.get("_success") is True

# Test R3: Execute with auto-resolved atom_id
# result = execute_process.fn(
#     profile="dev",
#     process_id=PROCESS_ID,
#     environment_id=ENV_ID,
#     # No atom_id — should auto-resolve
# )
# assert result.get("_success") is True

# Test R4: End-to-end workflow — execute then poll
# from server import monitor_platform
# exec_result = execute_process.fn(
#     profile="dev",
#     process_id=PROCESS_ID,
#     environment_id=ENV_ID,
# )
# request_id = exec_result.get("request_id")
# import time
# time.sleep(5)
# status = monitor_platform.fn(
#     profile="dev",
#     action="execution_records",
#     config=json.dumps({"execution_id": request_id})
# )
# print(json.dumps(status, indent=2, default=str)[:1000])

print("\n" + "=" * 60)
print("ALL SAFE TESTS PASSED ✅")
```

---

## Acceptance Criteria

1. **Execution works**: Calling with valid process_id + environment_id + atom_id triggers process execution
2. **Returns request_id**: Response includes `request_id` for status polling via `monitor_platform`
3. **Auto-resolve atom_id**: When atom_id is omitted, auto-detects from environment attachments (1 = use it, 0 = error, 2+ = error with list)
4. **Dynamic properties**: `config.dynamic_properties` dict is correctly built into `ExecutionRequestDynamicProcessProperties`
5. **Process properties**: `config.process_properties` dict is passed through (or documented as TODO if SDK model not available)
6. **next_step hint**: Response includes `next_step` with the exact `monitor_platform` call to poll status
7. **Error handling**: API errors return `_success: false` with helpful messages
8. **Annotations**: Tool has `destructiveHint: true` and `openWorldHint: true`
9. **Import guard**: `server.py` uses try/except import with `execute_process_action = None` fallback
10. **Test script passes**: Safe tests (1-6) pass; real execution tests pass with valid Boomi account
11. **MCP parameter parity**: `config` is a JSON string (not a Python dict)

---

## QA Validation

After implementation, run the **boomi-qa-tester agent** to validate:

```
Run the boomi-qa-tester agent against execute_process with:
1. Verify tool is registered with correct annotations (destructiveHint, openWorldHint)
2. Test config JSON parsing (valid, invalid, non-object)
3. Test auto-resolve atom_id (mock or real environment with 0, 1, 2 attachments)
4. Test dynamic_properties building (verify DynamicProcessProperty list construction)
5. Test response includes request_id and next_step hint
6. Test end-to-end: execute_process → monitor_platform(execution_records) workflow
7. Verify MCP parameter parity (config as JSON string)
```

---

## What NOT to Implement

- **Status polling loop**: No built-in `wait_for_completion` or polling. The agent uses `monitor_platform(action="execution_records")` to poll. This keeps the tool atomic and avoids long-running HTTP requests.
- **Sync execution mode**: The Boomi API is async-only for process execution. "Sync" is just execute + poll, which the agent can do.
- **Input document upload**: The Boomi execution API doesn't accept inline documents in the REST call. Documents are typically sourced from connectors within the process.
- **Retry logic**: If execution fails, the agent calls `execute_process` again. No built-in retry.
- **Batch execution**: Running the same process across multiple environments is agent orchestration.
- **Execution scheduling**: Use `manage_process` schedule actions (future) for recurring execution.
- **Process property deep validation**: We build the model and let the API validate. No client-side schema enforcement.

---

## Comparison with Related Tools

| Aspect | execute_process | manage_process | monitor_platform |
|--------|----------------|---------------|-----------------|
| Purpose | Run a process | CRUD on process components | Query execution history/logs |
| SDK service | `execution_request` | `component` (XML) | `execution_record`, `audit_log`, `event` |
| Read/Write | Write only | Read + Write | Read only |
| Annotations | destructiveHint: true | — | readOnlyHint: true |
| Parameters | process_id, environment_id, atom_id, config | process_id, config_yaml, filters | action, config |
| Paired with | monitor_platform (status polling) | — | execute_process (triggering) |

---

## Dependencies

- **boomi-python SDK**: `sdk.execution_request.create_execution_request()`, `sdk.environment_atom_attachment.query_environment_atom_attachment()`
- **SDK models**: `ExecutionRequest`, `ExecutionRequestDynamicProcessProperties`, `DynamicProcessProperty`, `ExecutionRequestProcessProperties`
- **Attachment models** (for auto-resolve): `EnvironmentAtomAttachmentQueryConfig`, `EnvironmentAtomAttachmentSimpleExpression`, etc.
- **No imports from other category modules** (self-contained)
- **No new pip packages required**
- **`boomi.net.transport.api_error.ApiError`**: Import for specific error handling

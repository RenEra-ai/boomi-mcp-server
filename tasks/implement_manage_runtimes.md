# Task: Implement `manage_runtimes` Tool (Tool #6)

**Date**: 2026-03-02
**Category**: 2 — Environments & Runtimes
**Priority**: High (required for deployment workflows — runtimes must exist and be attached before processes can execute)
**Estimated Effort**: 1 agent session (~600-800 lines of new code)
**Design Doc Reference**: `MCP_TOOL_DESIGN.md` line 670+
**Status**: ⏳ Not Started

---

## Overview

Implement the `manage_runtimes` MCP tool that provides CRUD operations on Boomi runtimes (Atoms, Molecules, Clouds, Gateways), environment-runtime attachments, restart management, Java version management, and installer token creation.

The tool consolidates 10 actions into a single MCP tool, covering 9 SDK example files in `05_runtime_setup/`:

| Example File | Maps to Action |
|---|---|
| `manage_runtimes.py` | list, get, update, delete |
| `list_runtimes.py` | list |
| `query_runtimes.py` | list (filtered) |
| `create_environment_atom_attachment.py` | attach |
| `detach_runtime_from_environment.py` | detach |
| `query_environment_runtime_attachments.py` | list_attachments |
| `restart_runtime.py` | restart |
| `manage_java_runtime.py` | configure_java |
| `create_installer_token.py` | create_installer_token |

**Why combined**: All operations relate to the runtime lifecycle — discovery, configuration, environment binding, health management, and provisioning. The sub-operations (attachments, restart, Java, tokens) always require a `runtime_id` or `environment_id` and are too thin individually for separate tools. Follows the same consolidation pattern as `manage_environments` (9 actions) and `manage_trading_partner` (12 actions).

---

## Actions (10 total)

| # | Action | Read/Write | SDK Service | Description |
|---|--------|-----------|-------------|-------------|
| 1 | `list` | Read | `sdk.atom.query_atom()` | List runtimes with optional type/status/name filters |
| 2 | `get` | Read | `sdk.atom.get_atom()` | Get single runtime details (status, version, hostname, capabilities) |
| 3 | `update` | Write | `sdk.atom.update_atom()` | Update runtime name (GET first to preserve required fields) |
| 4 | `delete` | Write | `sdk.atom.delete_atom()` | Delete runtime (permanent, fails if attached to environments) |
| 5 | `attach` | Write | `sdk.environment_atom_attachment.create_*()` | Attach runtime to an environment |
| 6 | `detach` | Write | `sdk.environment_atom_attachment.delete_*()` | Detach runtime from environment (requires attachment_id) |
| 7 | `list_attachments` | Read | `sdk.environment_atom_attachment.query_*()` | List environment-runtime attachments with optional filters |
| 8 | `restart` | Write | `sdk.runtime_restart_request.create_*()` | Restart runtime (Cloud runtimes cannot be restarted via API) |
| 9 | `configure_java` | Write | `sdk.java_upgrade.*` / `sdk.java_rollback.*` | Upgrade or rollback Java version on a runtime |
| 10 | `create_installer_token` | Write | `sdk.installer_token.create_*()` | Create installer token for new runtime installation |

---

## SDK Examples (Absolute Paths)

### Runtime CRUD (Category 5)

| Example File | Absolute Path |
|---|---|
| `manage_runtimes.py` | `/sessions/quirky-elegant-mayer/mnt/examples/05_runtime_setup/manage_runtimes.py` |
| `list_runtimes.py` | `/sessions/quirky-elegant-mayer/mnt/examples/05_runtime_setup/list_runtimes.py` |
| `query_runtimes.py` | `/sessions/quirky-elegant-mayer/mnt/examples/05_runtime_setup/query_runtimes.py` |

### Environment-Runtime Attachments

| Example File | Absolute Path |
|---|---|
| `create_environment_atom_attachment.py` | `/sessions/quirky-elegant-mayer/mnt/examples/05_runtime_setup/create_environment_atom_attachment.py` |
| `detach_runtime_from_environment.py` | `/sessions/quirky-elegant-mayer/mnt/examples/05_runtime_setup/detach_runtime_from_environment.py` |
| `query_environment_runtime_attachments.py` | `/sessions/quirky-elegant-mayer/mnt/examples/05_runtime_setup/query_environment_runtime_attachments.py` |

### Restart, Java, and Installer Token

| Example File | Absolute Path |
|---|---|
| `restart_runtime.py` | `/sessions/quirky-elegant-mayer/mnt/examples/05_runtime_setup/restart_runtime.py` |
| `manage_java_runtime.py` | `/sessions/quirky-elegant-mayer/mnt/examples/05_runtime_setup/manage_java_runtime.py` |
| `create_installer_token.py` | `/sessions/quirky-elegant-mayer/mnt/examples/05_runtime_setup/create_installer_token.py` |

---

## SDK Models to Import

### Runtime CRUD (from `manage_runtimes.py` lines 41-48):

```python
from boomi.models import (
    Atom,
    AtomQueryConfig,
    AtomQueryConfigQueryFilter,
    AtomSimpleExpression,
    AtomSimpleExpressionOperator,
    AtomSimpleExpressionProperty,
)
```

**AtomSimpleExpressionProperty values**: `ID`, `NAME`, `TYPE`, `STATUS`

**AtomSimpleExpressionOperator values**: `EQUALS`, `LIKE`, `ISNOTNULL`, `ISNULL`, `CONTAINS`

**Runtime types** (via `type_` attribute): `ATOM`, `MOLECULE`, `CLOUD`, `GATEWAY`

**Runtime statuses**: `ONLINE`, `OFFLINE`

### Environment-Runtime Attachments (from `query_environment_runtime_attachments.py` lines 32-38):

```python
from boomi.models import (
    EnvironmentAtomAttachment,
    EnvironmentAtomAttachmentQueryConfig,
    EnvironmentAtomAttachmentQueryConfigQueryFilter,
    EnvironmentAtomAttachmentSimpleExpression,
    EnvironmentAtomAttachmentSimpleExpressionOperator,
    EnvironmentAtomAttachmentSimpleExpressionProperty,
)
```

**EnvironmentAtomAttachmentSimpleExpressionProperty values**: `ENVIRONMENTID`, `ATOMID`

### Restart (from `restart_runtime.py` line 85):

```python
from boomi.models import RuntimeRestartRequest
```

### Java Management (from `manage_java_runtime.py` lines 36-45):

```python
from boomi.models import (
    JavaUpgrade,
    JavaRollback,
)
```

### Installer Token (from `create_installer_token.py` line 48):

```python
from boomi.models import InstallerToken, InstallType
```

**InstallType enum values**: `ATOM`, `MOLECULE`, `CLOUD`, `BROKER`, `GATEWAY`

---

## MCP Tool Signature

```python
@mcp.tool()
def manage_runtimes(
    profile: str,
    action: str,
    resource_id: str = None,    # runtime_id (most actions) or attachment_id (detach)
    environment_id: str = None,  # for attach, detach, list_attachments
    config: str = None,          # JSON string with action-specific parameters
) -> dict:
```

**Note**: `environment_id` is a top-level parameter (not buried in config) because `attach` and `detach` are high-frequency operations where the user always needs to provide both IDs. This reduces friction compared to requiring JSON config for every attach/detach call.

---

## Implementation Pattern

### File: `src/boomi_mcp/categories/runtimes.py`

Follow the flat file pattern established by `environments.py`:

```
runtimes.py
├── Imports (SDK models)
├── Constants (VALID_RUNTIME_TYPES, VALID_STATUSES, VALID_INSTALL_TYPES)
├── Helpers
│   ├── _runtime_to_dict(runtime)           # SDK Atom → plain dict
│   ├── _attachment_to_dict(attachment)      # SDK attachment → plain dict
│   ├── _query_all_runtimes(sdk, expression) # Paginated runtime query
│   └── _query_all_attachments(sdk, expression) # Paginated attachment query
├── Action Handlers (10 functions)
│   ├── _action_list(sdk, profile, **kwargs)
│   ├── _action_get(sdk, profile, **kwargs)
│   ├── _action_update(sdk, profile, **kwargs)
│   ├── _action_delete(sdk, profile, **kwargs)
│   ├── _action_attach(sdk, profile, **kwargs)
│   ├── _action_detach(sdk, profile, **kwargs)
│   ├── _action_list_attachments(sdk, profile, **kwargs)
│   ├── _action_restart(sdk, profile, **kwargs)
│   ├── _action_configure_java(sdk, profile, **kwargs)
│   └── _action_create_installer_token(sdk, profile, **kwargs)
└── Action Router
    └── manage_runtimes_action(sdk, profile, action, config_data=None, **kwargs)
```

### File: `server.py` additions (~120 lines)

Follow the same registration pattern as `manage_environments`:

```python
try:
    from boomi_mcp.categories.runtimes import manage_runtimes_action
    print(f"[INFO] Runtime tools loaded successfully")
except ImportError as e:
    print(f"[WARNING] Failed to import runtime tools: {e}")
    manage_runtimes_action = None

if manage_runtimes_action:
    @mcp.tool()
    def manage_runtimes(
        profile: str,
        action: str,
        resource_id: str = None,
        environment_id: str = None,
        config: str = None,
    ):
        """..."""
        # Parse config JSON
        config_data = {}
        if config:
            try:
                config_data = json.loads(config)
            except (json.JSONDecodeError, TypeError) as e:
                return {"_success": False, "error": f"Invalid config: {e}"}
            if not isinstance(config_data, dict):
                return {"_success": False, "error": "config must be a JSON object"}

        try:
            subject = get_current_user()
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

            params = {}
            if resource_id:
                params["resource_id"] = resource_id
            if environment_id:
                params["environment_id"] = environment_id

            return manage_runtimes_action(sdk, profile, action, config_data=config_data, **params)

        except Exception as e:
            print(f"[ERROR] Failed to {action} manage_runtimes: {e}")
            import traceback
            traceback.print_exc()
            return {"_success": False, "error": str(e), "exception_type": type(e).__name__}
```

---

## Action Details

### 1. `list` — List runtimes

**Config options**: `runtime_type` (ATOM/MOLECULE/CLOUD/GATEWAY), `status` (ONLINE/OFFLINE), `name_pattern` (LIKE filter)

**SDK pattern** (from `manage_runtimes.py` lines 76-153):
```python
# Empty query = all runtimes
query_config = AtomQueryConfig()

# Filtered query
expression = AtomSimpleExpression(
    operator=AtomSimpleExpressionOperator.EQUALS,
    property=AtomSimpleExpressionProperty.TYPE,
    argument=["ATOM"]
)
query_filter = AtomQueryConfigQueryFilter(expression=expression)
query_config = AtomQueryConfig(query_filter=query_filter)
result = sdk.atom.query_atom(query_config)
```

**Response fields** (from SDK Atom object):
- `id_` → `id`
- `name`
- `type_` → `type` (ATOM, MOLECULE, CLOUD, GATEWAY)
- `status` (ONLINE, OFFLINE)
- `host_name` → `hostname`
- `current_version` → `version`
- `date_installed`
- `created_by`
- `capabilities` (list)

**Pagination**: Handle `query_token` for large accounts.

### 2. `get` — Get runtime details

**Requires**: `resource_id`

**SDK pattern**: `sdk.atom.get_atom(id_=resource_id)`

**Returns**: Full runtime dict with all available fields.

### 3. `update` — Update runtime name

**Requires**: `resource_id`, `config.name`

**SDK pattern** (from `manage_runtimes.py` lines 193-241):
```python
# GET first to preserve required fields
current_atom = sdk.atom.get_atom(id_=runtime_id)

update_data = {
    'id_': runtime_id,
    'name': new_name,
    'purge_history_days': getattr(current_atom, 'purge_history_days', 30),
    'purge_immediate': getattr(current_atom, 'purge_immediate', False),
    'force_restart_time': getattr(current_atom, 'force_restart_time', 0),
}
runtime_update = Atom(**update_data)
sdk.atom.update_atom(id_=runtime_id, request_body=runtime_update)
```

**Important**: Must GET current atom first to preserve `purge_history_days`, `purge_immediate`, and `force_restart_time` in the PUT body. Only `name` is updatable through this action.

### 4. `delete` — Delete runtime

**Requires**: `resource_id`

**SDK pattern**: `sdk.atom.delete_atom(id_=resource_id)`

**Error cases**: 409 = attached to environments (must detach first), 404 = not found.

**Response**: Include warning that deletion is permanent.

### 5. `attach` — Attach runtime to environment

**Requires**: `resource_id` (runtime_id) + `environment_id`

**SDK pattern** (from `create_environment_atom_attachment.py` lines 54-64):
```python
attachment_request = EnvironmentAtomAttachment(
    atom_id=runtime_id,
    environment_id=environment_id,
)
result = sdk.environment_atom_attachment.create_environment_atom_attachment(attachment_request)
```

**Response**: Returns attachment_id, atom_id, environment_id.

### 6. `detach` — Detach runtime from environment

**Requires**: `resource_id` (attachment_id)

**Note**: The `resource_id` here is the **attachment_id**, not the runtime_id. The user can find the attachment_id by calling `list_attachments` first.

**SDK pattern** (from `detach_runtime_from_environment.py` line 54):
```python
sdk.environment_atom_attachment.delete_environment_atom_attachment(id_=attachment_id)
```

**Alternative workflow**: If the user provides `resource_id` (runtime_id) + `environment_id` instead of an attachment_id, the implementation should query for the matching attachment first and then delete it. This is more user-friendly than requiring them to know the attachment_id.

### 7. `list_attachments` — Query environment-runtime attachments

**Optional filters**: `environment_id`, `resource_id` (runtime_id)

**SDK pattern** (from `query_environment_runtime_attachments.py` lines 53-61):
```python
expression = EnvironmentAtomAttachmentSimpleExpression(
    operator=EnvironmentAtomAttachmentSimpleExpressionOperator.EQUALS,
    property=EnvironmentAtomAttachmentSimpleExpressionProperty.ENVIRONMENTID,
    argument=[environment_id],
)
query_filter = EnvironmentAtomAttachmentQueryConfigQueryFilter(expression=expression)
query_config = EnvironmentAtomAttachmentQueryConfig(query_filter=query_filter)
result = sdk.environment_atom_attachment.query_environment_atom_attachment(query_config)
```

**Can filter by**: `ENVIRONMENTID` or `ATOMID`.

**When no filter given**: Use `CONTAINS` operator with empty string on ENVIRONMENTID to list all (from example line 56).

**Pagination**: Handle `query_token`.

### 8. `restart` — Restart runtime

**Requires**: `resource_id` (runtime_id)

**SDK pattern** (from `restart_runtime.py` lines 215-224):
```python
restart_request = RuntimeRestartRequest(
    runtime_id=atom_id,
    message="Restart initiated via MCP"
)
result = sdk.runtime_restart_request.create_runtime_restart_request(
    request_body=restart_request
)
```

**Cloud runtimes**: Cannot be restarted via API (HTTP 400). Detect and return helpful error message: "Cloud runtimes are managed by Boomi and restart automatically."

**Response handling**: Result may be a string, object with `message`, or dict — handle all three formats (see example lines 227-246).

### 9. `configure_java` — Java version management

**Requires**: `resource_id` (runtime_id), `config.java_action` (upgrade/rollback), `config.target_version` (for upgrade)

**Upgrade SDK pattern** (from `manage_java_runtime.py` lines 177-183):
```python
upgrade_request = JavaUpgrade(
    atom_id=atom_id,
    target_version="17.0"  # full version string
)
result = sdk.java_upgrade.create_java_upgrade(request_body=upgrade_request)
```

**Rollback SDK pattern** (from `manage_java_runtime.py` line 228):
```python
sdk.java_rollback.execute_java_rollback(id_=atom_id)
```

**Config options**:
- `java_action`: `"upgrade"` or `"rollback"` (required)
- `target_version`: `"11"`, `"17"`, or `"21"` (required for upgrade)

**Version mapping** (from example lines 59-64):
```python
JAVA_VERSIONS = {
    '8': '1.8.0',
    '11': '11.0',
    '17': '17.0',
    '21': '21.0',
}
```

### 10. `create_installer_token` — Create installer token

**Config options**: `install_type` (ATOM/MOLECULE/CLOUD/BROKER/GATEWAY), `duration_minutes` (30-1440, default 60)

**SDK pattern** (from `create_installer_token.py` lines 96-102):
```python
token_request = InstallerToken(
    install_type=InstallType.ATOM,
    duration_minutes=60,
)
result = sdk.installer_token.create_installer_token(token_request)
```

**Response fields**: `token`, `install_type`, `account_id`, `created`, `expiration`, `duration_minutes`

**Response parsing**: May be an object or wrapped in `_kwargs` — handle both (example lines 107-123).

---

## Helpers

| Helper | Purpose |
|--------|---------|
| `_runtime_to_dict(runtime)` | Convert SDK `Atom` object to plain dict. Map `id_`→`id`, `type_`→`type`, `host_name`→`hostname`, `current_version`→`version`. Include optional fields only when present. |
| `_attachment_to_dict(attachment)` | Convert SDK `EnvironmentAtomAttachment` to dict: `id`, `atom_id`, `environment_id`. |
| `_query_all_runtimes(sdk, expression)` | Execute `query_atom` with pagination (handle `query_token`). Returns list of dicts. |
| `_query_all_attachments(sdk, expression)` | Execute `query_environment_atom_attachment` with pagination. Returns list of dicts. |

---

## Constants

```python
VALID_RUNTIME_TYPES = {"ATOM", "MOLECULE", "CLOUD", "GATEWAY"}
VALID_STATUSES = {"ONLINE", "OFFLINE"}
VALID_INSTALL_TYPES = {"ATOM", "MOLECULE", "CLOUD", "BROKER", "GATEWAY"}

JAVA_VERSIONS = {
    '8': '1.8.0',
    '11': '11.0',
    '17': '17.0',
    '21': '21.0',
}
```

---

## Config Examples (for MCP tool docstring)

```
list — List all runtimes, optional filters:
    config='{"runtime_type": "ATOM"}'
    config='{"status": "ONLINE"}'
    config='{"name_pattern": "%prod%"}'

get — Get runtime by ID (no config needed):
    resource_id="abc-123-def"

update — Update runtime name:
    resource_id="abc-123-def"
    config='{"name": "Production Atom"}'

delete — Delete runtime (permanent!):
    resource_id="abc-123-def"

attach — Attach runtime to environment:
    resource_id="abc-123-def"           (runtime_id)
    environment_id="env-456-ghi"

detach — Detach runtime from environment:
    resource_id="attachment-789-jkl"    (attachment_id)
    OR:
    resource_id="abc-123-def"           (runtime_id)
    environment_id="env-456-ghi"        (auto-lookup attachment_id)

list_attachments — List environment-runtime attachments:
    environment_id="env-456-ghi"        (all runtimes in this env)
    resource_id="abc-123-def"           (all envs for this runtime)
    (neither = list all attachments)

restart — Restart runtime:
    resource_id="abc-123-def"

configure_java — Upgrade or rollback Java:
    resource_id="abc-123-def"
    config='{"java_action": "upgrade", "target_version": "17"}'
    config='{"java_action": "rollback"}'

create_installer_token — Create installer token:
    config='{"install_type": "ATOM", "duration_minutes": 120}'
```

---

## Test Script

Save as `test_manage_runtimes.py` in the project root for `.fn()` testing:

```python
#!/usr/bin/env python3
"""Test manage_runtimes tool via direct .fn() calls."""
import os, json
os.environ["BOOMI_LOCAL"] = "true"

from server import manage_runtimes

# Test 1: List all runtimes
print("=" * 60)
print("TEST 1: List all runtimes")
result = manage_runtimes.fn(profile="dev", action="list")
print(json.dumps(result, indent=2, default=str)[:2000])
assert result.get("_success") is True, f"list failed: {result}"

# Test 2: List with type filter
print("\n" + "=" * 60)
print("TEST 2: List ATOM runtimes")
result = manage_runtimes.fn(profile="dev", action="list", config='{"runtime_type": "ATOM"}')
print(json.dumps(result, indent=2, default=str)[:2000])
assert result.get("_success") is True

# Test 3: List with status filter
print("\n" + "=" * 60)
print("TEST 3: List ONLINE runtimes")
result = manage_runtimes.fn(profile="dev", action="list", config='{"status": "ONLINE"}')
print(json.dumps(result, indent=2, default=str)[:2000])
assert result.get("_success") is True

# Test 4: Get runtime by ID (use first from list)
print("\n" + "=" * 60)
print("TEST 4: Get runtime by ID")
list_result = manage_runtimes.fn(profile="dev", action="list")
if list_result.get("runtimes"):
    first_id = list_result["runtimes"][0]["id"]
    result = manage_runtimes.fn(profile="dev", action="get", resource_id=first_id)
    print(json.dumps(result, indent=2, default=str)[:1000])
    assert result.get("_success") is True

# Test 5: List all attachments
print("\n" + "=" * 60)
print("TEST 5: List all attachments")
result = manage_runtimes.fn(profile="dev", action="list_attachments")
print(json.dumps(result, indent=2, default=str)[:2000])
assert result.get("_success") is True

# Test 6: List attachments filtered by environment
print("\n" + "=" * 60)
print("TEST 6: List attachments for specific environment")
# Use a known environment_id from your account
result = manage_runtimes.fn(
    profile="dev", action="list_attachments",
    environment_id="74851c30-98b2-4a6f-838b-61eee5627b13"  # adjust to real ID
)
print(json.dumps(result, indent=2, default=str)[:2000])
assert result.get("_success") is True

# Test 7: Create installer token
print("\n" + "=" * 60)
print("TEST 7: Create installer token")
result = manage_runtimes.fn(
    profile="dev", action="create_installer_token",
    config='{"install_type": "ATOM", "duration_minutes": 30}'
)
print(json.dumps(result, indent=2, default=str)[:1000])
assert result.get("_success") is True
assert "token" in result

# Test 8: Invalid action
print("\n" + "=" * 60)
print("TEST 8: Invalid action")
result = manage_runtimes.fn(profile="dev", action="bogus")
print(json.dumps(result, indent=2, default=str)[:500])
assert result.get("_success") is False
assert "valid_actions" in result

# Test 9: Invalid runtime type
print("\n" + "=" * 60)
print("TEST 9: Invalid runtime type filter")
result = manage_runtimes.fn(
    profile="dev", action="list",
    config='{"runtime_type": "INVALID"}'
)
assert result.get("_success") is False

# Test 10: Invalid JSON config
print("\n" + "=" * 60)
print("TEST 10: Invalid JSON config")
result = manage_runtimes.fn(profile="dev", action="list", config="{bad json}")
assert result.get("_success") is False

# === DESTRUCTIVE TESTS (uncomment to run) ===
# WARNING: These modify real runtimes in your account!

# Test D1: Restart (only run on a test runtime)
# result = manage_runtimes.fn(profile="dev", action="restart", resource_id="your-test-runtime-id")
# assert result.get("_success") is True or "Cloud" in str(result.get("error", ""))

# Test D2: Attach/detach (only run on test env + runtime)
# result = manage_runtimes.fn(
#     profile="dev", action="attach",
#     resource_id="your-runtime-id",
#     environment_id="your-env-id"
# )
# assert result.get("_success") is True
# attachment_id = result.get("attachment", {}).get("id")
# result = manage_runtimes.fn(profile="dev", action="detach", resource_id=attachment_id)
# assert result.get("_success") is True

# Test D3: Configure Java (only run on test runtime)
# result = manage_runtimes.fn(
#     profile="dev", action="configure_java",
#     resource_id="your-test-runtime-id",
#     config='{"java_action": "upgrade", "target_version": "17"}'
# )

print("\n" + "=" * 60)
print("ALL SAFE TESTS PASSED ✅")
```

---

## Acceptance Criteria

1. **All 10 actions work**: list, get, update, delete, attach, detach, list_attachments, restart, configure_java, create_installer_token
2. **Runtime type validation**: list with invalid `runtime_type` returns helpful error with valid values
3. **Update preserves fields**: update action GETs current atom first, includes `purge_history_days`, `purge_immediate`, `force_restart_time` in PUT body
4. **Cloud restart handling**: restart returns clear error message for Cloud runtimes (HTTP 400)
5. **Detach flexibility**: detach works with either `attachment_id` or `runtime_id + environment_id` pair
6. **Installer token validation**: duration_minutes must be 30-1440, install_type must be valid enum
7. **Java version mapping**: Accepts short versions ("17") and maps to SDK format ("17.0")
8. **Pagination**: list and list_attachments handle `query_token` for large accounts
9. **Config JSON string**: All parameters passed via `config` JSON string (MCP parameter parity)
10. **Error handling**: Every action returns `_success` field, errors include helpful messages
11. **Import guard**: `server.py` uses try/except import pattern with `manage_runtimes_action = None` fallback
12. **No readOnlyHint**: Tool has write operations — no readOnlyHint annotation
13. **Test script passes**: Safe tests (1-10) pass with a real Boomi account

---

## Comparison with Existing Tools

| Aspect | manage_runtimes | manage_environments | manage_trading_partner |
|--------|----------------|-------------------|----------------------|
| Actions | 10 | 9 | 12 |
| API type | JSON (SDK typed models) | JSON (SDK typed models) | JSON + XML (SDK + builders) |
| Config param | JSON string | JSON string | JSON string |
| Complexity | Medium (5 SDK services) | Medium (2 SDK services) | High (7 standards) |
| File location | `categories/runtimes.py` | `categories/environments.py` | `categories/components/trading_partners.py` |
| XML manipulation | None | None | Extensive |
| SDK services used | 5 (atom, attachment, restart, java, token) | 2 (environment, extensions) | 1 (component XML) |
| Extra top-level params | `environment_id` | — | — |

---

## Dependencies

- **boomi-python SDK**: `sdk.atom.*`, `sdk.environment_atom_attachment.*`, `sdk.runtime_restart_request.*`, `sdk.java_upgrade.*`, `sdk.java_rollback.*`, `sdk.installer_token.*`
- **No imports from other category modules** (self-contained like environments.py)
- **No new pip packages required**
- **`boomi.net.transport.api_error.ApiError`**: Import for specific error handling (Cloud restart 400, detach 404)

---

## What NOT to Implement

- **Runtime monitoring/polling**: Continuous status monitoring is a CLI feature, not MCP tool behavior. Agent can call `get` repeatedly if needed.
- **Batch restart**: Rolling restart across multiple runtimes is orchestration logic — agent can loop over `restart` calls.
- **Scheduled restart**: Use `manage_process` schedule actions or external schedulers.
- **Java compatibility checks**: The Java compatibility analysis from `manage_java_runtime.py` is CLI presentation — the agent can inspect current Java version via `get` and make its own judgment.
- **Java batch upgrade**: Same as batch restart — agent orchestrates.
- **Runtime creation**: Runtimes are created by installing them on machines using installer tokens, not via API.
- **Molecule node management**: Individual molecule node operations are admin-level, use `invoke_boomi_api`.
- **Shared server configuration**: Use `invoke_boomi_api` for shared server resource management.

---

## Implementation Notes

### SDK Naming Convention
Boomi SDK calls runtimes "Atoms" internally (`sdk.atom.*`, `Atom` model). The MCP tool uses "runtime" in its public API to match Boomi's current UI terminology. Map `id_` → `id` and `type_` → `type` in response dicts (underscore suffix is SDK convention to avoid Python keyword conflicts).

### Detach Convenience Logic
The `detach` action should support two calling patterns:
1. **Direct**: `resource_id=attachment_id` — deletes the attachment directly
2. **Lookup**: `resource_id=runtime_id` + `environment_id` — queries for the matching attachment, then deletes it

This avoids forcing the user to know the attachment_id. Implementation: if `environment_id` is provided alongside `resource_id` for detach, treat `resource_id` as `runtime_id` and query for the attachment first.

### Error Handling for Restart
Cloud runtimes return HTTP 400 when restart is attempted. Detect this and return a clear message rather than a generic error. Pattern from `restart_runtime.py` lines 248-252.

### Installer Token Response Parsing
The response may come as a direct object (with `token` attribute), wrapped in `_kwargs['InstallerToken']`, or as a dict. Handle all three formats (same pattern as extension response parsing).

# Task: Implement `manage_environments` Tool (Tool #5)

**Date**: 2026-03-01 (updated 2026-03-02)
**Category**: 2 — Environments & Runtimes
**Priority**: High (core infrastructure tool)
**Estimated Effort**: 1 agent session (~500-700 lines of new code)
**Design Doc Reference**: `MCP_TOOL_DESIGN.md` line 606+
**Implementation Commit**: `dec4fe3` — `environments.py` (451 lines) + `server.py` registration (104 lines)
**Status**: ✅ Implemented

---

## Overview

Implement the `manage_environments` MCP tool that provides full CRUD operations on Boomi environments plus environment extensions configuration (connection parameters, properties, cross-references, trading partner configs, certificates, operations, and data maps). This is a **pure JSON API** tool — no XML builders needed.

The tool consolidates 9 actions into a single MCP tool, covering 9 SDK example files across two directories:
- 7 environment CRUD examples (`04_environment_setup/`)
- 2 environment extensions examples (`06_configure_deployment/`)

**Why combined**: Environment extensions are configuration overrides *scoped to a specific environment*. They always require an `environment_id` and are a natural sub-operation of environment management. The EnvironmentExtensions API has only 3 operations (GET, UPDATE, QUERY) — too thin for a separate tool. Extensions cannot be created or deleted through the API; they are auto-generated when processes with extensible components are deployed to an environment.

---

## Extension Lifecycle (Define → Deploy → Configure)

Understanding the full lifecycle is critical for this tool:

### Phase 1: DEFINE (Process Development Time)
In the Boomi UI, the **Extensions dialog** above the process canvas has 9 tabs (Connection Settings, Operation Settings, Partner Settings, Dynamic Properties, Process Properties, Object Definitions, Data Maps, Cross Reference, PGP). Checking a checkbox marks that component/field as extensible.

**This definition is stored in the process component XML** — it's part of the process itself, not a separate API. Through the API, extension definitions are baked into `componentXml` when you `GET /Component/{processId}`. This phase is handled by `manage_process` / `manage_component`.

### Phase 2: DEPLOY (Package & Deploy)
When the process is packaged and deployed to an environment, the platform **auto-generates** `EnvironmentExtensions` entries from the deployed process XML. There is no API to create or delete these entries — they exist automatically for any deployed process with extensible components.

### Phase 3: CONFIGURE (This Tool — Runtime Configuration)
The `EnvironmentExtensions` API lets you **read and override** field values per environment:

| Operation | Endpoint | What it does |
|-----------|----------|-------------|
| **GET** | `GET /EnvironmentExtensions/{environmentId}` | Returns all extensible components/fields with current values |
| **UPDATE** | `POST /EnvironmentExtensions/{environmentId}/update` | Set override values for specific fields |
| **QUERY** | `POST /EnvironmentExtensions/query` | Find which environments have extensions |

### The 8 Extension Types

| Type | JSON Key | Field Structure | Notes |
|------|----------|----------------|-------|
| Connections | `connections.connection[]` | `field[]: @id, @value, @useDefault, @usesEncryption, @encryptedValueSet` | Has encrypted fields + custom properties |
| Operations | `operations.operation[]` | Same field structure | APIM and SDK Listener operations only |
| Trading Partners | `tradingPartners.tradingPartner[]` | `category[]: @name` with sub-fields | B2B partner configs |
| Dynamic Process Properties | `properties.property[]` | `@name, @value` | No explicit useDefault; override by providing value |
| Process Properties | `processProperties.ProcessProperty[]` | `@name, @value` per property component | Must update ALL properties in a component together |
| Cross References | `crossReferences.crossReference[]` | `CrossReferenceRows.row[]` with `ref1, ref2, ...` | Must resend ALL rows even for partial updates |
| PGP Certificates | `PGPCertificates.PGPCertificate[]` | Component ID as identifier | Use Component Metadata API for details |
| Data Maps | `dataMaps.dataMap[]` | Map component overrides | Source/destination field mapping |

### Extension Field Attributes

Each extensible field has these attributes:
- **`useDefault`**: `true` = use component's built-in value; `false` = use override value
- **`value`**: The override value (only present when `useDefault="false"`)
- **`usesEncryption`**: `true` = field stores sensitive data (passwords, tokens)
- **`encryptedValueSet`**: `true` = an encrypted value has been set (GET never returns actual encrypted values)
- **`componentOverride`**: Shows component-level override status

### Extension Update Caveats

- **Partial updates** (default, recommended): Include only fields to modify; omitted fields keep current values
- **Complete updates**: Provide ALL fields; omitted fields **revert to component defaults** (destructive!)
- **Encrypted fields**: GET never returns actual values (`encryptedValueSet=true`); UPDATE requires actual value
- **Cross-reference rows**: Must resend ALL rows in a cross-reference even for partial updates
- **Custom properties**: Must resend ALL key-value pairs even for partial updates
- **`useDefault` attribute**: Set `true` to revert field to component default; `false` + `value` to override
- **`extensionGroupId`**: Used for multi-install integration pack extensions; identifies the specific pack instance

---

## Actions (9 total)

| # | Action | Read/Write | SDK Service | Description |
|---|--------|-----------|-------------|-------------|
| 1 | `list` | Read | `sdk.environment.query_environment()` | List all environments with optional classification/name filters |
| 2 | `get` | Read | `sdk.environment.get_environment()` | Get single environment by ID |
| 3 | `create` | Write | `sdk.environment.create_environment()` | Create new environment with name + classification |
| 4 | `update` | Write | `sdk.environment.update_environment()` | Update environment name (classification is immutable) |
| 5 | `delete` | Write | `sdk.environment.delete_environment()` | Delete environment (fails if runtimes attached or components deployed) |
| 6 | `get_extensions` | Read | `sdk.environment_extensions.get_environment_extensions()` | Get environment-specific config overrides (all 8 extension types) |
| 7 | `update_extensions` | Write | `sdk.environment_extensions.update_environment_extensions()` | Update environment extensions (partial merge by default) |
| 8 | `query_extensions` | Read | `sdk.environment_extensions.query_environment_extensions()` | Query which environments have extensions configured |
| 9 | `stats` | Read | Composite: list all → classify | Summary of environment counts by classification |

---

## SDK Examples (Absolute Paths)

These files contain the proven SDK patterns for all 9 actions. Read them carefully.

### Environment CRUD (Category 4)

| Example File | Absolute Path | What It Demonstrates |
|---|---|---|
| `manage_environments.py` | `/sessions/quirky-elegant-mayer/mnt/examples/04_environment_setup/manage_environments.py` | All CRUD operations, querying, stats, display formatting |
| `create_environment.py` | `/sessions/quirky-elegant-mayer/mnt/examples/04_environment_setup/create_environment.py` | Create with EnvironmentModel + EnvironmentClassification enum |
| `get_environment.py` | `/sessions/quirky-elegant-mayer/mnt/examples/04_environment_setup/get_environment.py` | Get by ID |
| `list_environments.py` | `/sessions/quirky-elegant-mayer/mnt/examples/04_environment_setup/list_environments.py` | List with pagination |
| `query_environments.py` | `/sessions/quirky-elegant-mayer/mnt/examples/04_environment_setup/query_environments.py` | Filter by property/operator/value |
| `update_environment.py` | `/sessions/quirky-elegant-mayer/mnt/examples/04_environment_setup/update_environment.py` | Update (requires GET first to preserve classification) |
| `delete_environment.py` | `/sessions/quirky-elegant-mayer/mnt/examples/04_environment_setup/delete_environment.py` | Delete with error handling |

### Environment Extensions (Category 6 — Deployment Config)

| Example File | Absolute Path | What It Demonstrates |
|---|---|---|
| `manage_environment_extensions.py` | `/sessions/quirky-elegant-mayer/mnt/examples/06_configure_deployment/manage_environment_extensions.py` | Get extensions, query extensions, analyze complexity, export config |
| `update_environment_extensions.py` | `/sessions/quirky-elegant-mayer/mnt/examples/06_configure_deployment/update_environment_extensions.py` | Update extensions with EnvironmentExtensions model |

---

## SDK Models to Import

### From `manage_environments.py` (lines 77-85):

```python
from boomi.models import (
    # Environment CRUD
    Environment as EnvironmentModel,
    EnvironmentClassification,
    EnvironmentQueryConfig,
    EnvironmentQueryConfigQueryFilter,
    EnvironmentSimpleExpression,
    EnvironmentSimpleExpressionOperator,
    EnvironmentSimpleExpressionProperty,
)
```

**Note**: The SDK model is named `Environment` but should be imported as `EnvironmentModel` to avoid name collision with Python builtins.

**EnvironmentClassification enum values**: `TEST`, `PROD` (implementation currently validates only these two)

**EnvironmentSimpleExpressionProperty values**: `ID`, `NAME`, `CLASSIFICATION`

**EnvironmentSimpleExpressionOperator values**: `EQUALS`, `LIKE`, `ISNOTNULL`, `ISNULL`, `CONTAINS`

### From `manage_environment_extensions.py` (lines 68-75):

```python
from boomi.models import (
    # Environment Extensions
    EnvironmentExtensions,
    EnvironmentExtensionsQueryConfig,
    EnvironmentExtensionsQueryConfigQueryFilter,
    EnvironmentExtensionsSimpleExpression,
    EnvironmentExtensionsSimpleExpressionOperator,
    EnvironmentExtensionsSimpleExpressionProperty,
)
```

**EnvironmentExtensionsSimpleExpressionProperty values**: `ENVIRONMENTID`

**Note**: `ISNOTNULL` operator doesn't work with extensions API — must provide specific environment_id.

---

## Implementation (Commit dec4fe3)

### File: `src/boomi_mcp/categories/environments.py` (451 lines)

The implementation follows the flat file pattern (same as `monitoring.py`, `folders.py`).

#### Key Architectural Decisions in Implementation

1. **Action router signature**: `manage_environments_action(sdk, profile, action, config_data=None, **kwargs)` — uses explicit `config_data` parameter (merged into kwargs) rather than flat kwargs only. This matches the server.py pattern where `config` JSON string is parsed into `config_data` dict before passing to the action router.

2. **Classification validation**: Currently validates only `TEST` and `PROD` (not `STAGING`, `DEV`). This matches what the Boomi account supports. The design doc lists all 4 values; the implementation can be expanded when needed.

3. **Extensions response parsing**: Uses `_parse_extensions_response()` helper that handles SDK's `_kwargs` wrapping:
   ```python
   if hasattr(result, '_kwargs') and 'EnvironmentExtensions' in result._kwargs:
       data = result._kwargs['EnvironmentExtensions']
   elif hasattr(result, '_kwargs') and result._kwargs:
       data = result._kwargs
   elif hasattr(result, 'to_dict'):
       data = result.to_dict()
   ```

4. **Extensions response summary**: Parses all 8 extension types (connections, operations, properties, cross_references, trading_partners, pgp_certificates, process_properties, data_maps) with count + items. Updated in commit `08f389b`.

5. **Deep merge for partial updates**: `_deep_merge()` helper recursively merges override dict into base dict. Falls back to provided data if GET fails (e.g., no extensions yet for new environment).

6. **Pagination**: Both `_query_all_environments()` and `_action_query_extensions()` handle `query_token` for large result sets.

#### Helpers

| Helper | Purpose |
|--------|---------|
| `_env_to_dict(env)` | Convert SDK Environment object to dict (handles classification enum `.value`) |
| `_validate_classification(value)` | Validate classification string → SDK enum |
| `_query_all_environments(sdk, expression)` | Execute query with pagination → list of dicts |
| `_extract_raw_extensions(result)` | Extract raw dict from SDK response (handles `_kwargs` wrapping) |
| `_parse_extensions_response(result)` | Parse nested SDK extensions response into summary dict (uses `_extract_raw_extensions`) |
| `_deep_merge(base, override)` | Recursive dict merge for partial extension updates |

#### Action Handlers

| Handler | Key behavior |
|---------|-------------|
| `_action_list` | Supports `classification`, `name_pattern` filters; uses `ISNOTNULL` on ID for unfiltered list |
| `_action_get` | Requires `resource_id`; returns single environment dict |
| `_action_create` | Requires `name`; defaults `classification` to "TEST"; validates via `_validate_classification()` |
| `_action_update` | Requires `resource_id` + `name`; **rejects** `classification` in config (immutable); GETs current env first |
| `_action_delete` | Requires `resource_id`; GETs info first for response message; warns deletion is permanent |
| `_action_get_extensions` | Requires `resource_id`; uses `_parse_extensions_response()` |
| `_action_update_extensions` | Requires `resource_id` + `extensions` dict; partial=true by default; deep merges with current |
| `_action_query_extensions` | Requires `resource_id`; filters by ENVIRONMENTID; handles pagination |
| `_action_stats` | Queries all environments; counts by classification |

### File: `server.py` additions (104 lines)

#### Import block (after existing category imports):
```python
try:
    from boomi_mcp.categories.environments import manage_environments_action
    print(f"[INFO] Environment tools loaded successfully")
except ImportError as e:
    print(f"[WARNING] Failed to import environment tools: {e}")
    manage_environments_action = None
```

#### Tool registration pattern:
```python
if manage_environments_action:
    @mcp.tool()
    def manage_environments(
        profile: str,
        action: str,
        resource_id: str = None,
        config: str = None,
    ):
```

**Server.py uses**:
- `get_current_user()` for auth subject
- `get_secret(subject, profile)` for credential retrieval (not `get_credentials`)
- `sdk_params` dict with optional `base_url` support
- `json.loads(config)` with type validation (`isinstance(config_data, dict)`)
- Passes `config_data=config_data` explicitly to action router

---

## MCP Tool Signature

```python
@mcp.tool()
def manage_environments(
    profile: str,
    action: str,
    resource_id: str = None,  # environment_id
    config: str = None,       # JSON string
) -> dict:
```

---

## Test Script

Save as `test_manage_environments.py` in the project root for `.fn()` testing:

```python
#!/usr/bin/env python3
"""Test manage_environments tool via direct .fn() calls."""
import os, json
os.environ["BOOMI_LOCAL"] = "true"

from server import manage_environments

# Test 1: List all environments
print("=" * 60)
print("TEST 1: List all environments")
result = manage_environments.fn(profile="dev", action="list")
print(json.dumps(result, indent=2, default=str)[:2000])
assert result.get("_success") is True, f"list failed: {result}"

# Test 2: List with classification filter
print("\n" + "=" * 60)
print("TEST 2: List TEST environments")
result = manage_environments.fn(profile="dev", action="list", config='{"classification": "TEST"}')
print(json.dumps(result, indent=2, default=str)[:2000])
assert result.get("_success") is True

# Test 3: Get environment by ID (use first from list)
print("\n" + "=" * 60)
print("TEST 3: Get environment by ID")
list_result = manage_environments.fn(profile="dev", action="list")
if list_result.get("environments"):
    first_id = list_result["environments"][0]["id"]
    result = manage_environments.fn(profile="dev", action="get", resource_id=first_id)
    print(json.dumps(result, indent=2, default=str)[:1000])
    assert result.get("_success") is True

# Test 4: Stats
print("\n" + "=" * 60)
print("TEST 4: Environment stats")
result = manage_environments.fn(profile="dev", action="stats")
print(json.dumps(result, indent=2, default=str)[:1000])
assert result.get("_success") is True
assert "by_classification" in result

# Test 5: Create test environment
print("\n" + "=" * 60)
print("TEST 5: Create environment")
result = manage_environments.fn(
    profile="dev", action="create",
    config='{"name": "MCP-Test-Environment", "classification": "TEST"}'
)
print(json.dumps(result, indent=2, default=str)[:1000])
assert result.get("_success") is True
created_id = result.get("environment", {}).get("id")

# Test 6: Update environment name
print("\n" + "=" * 60)
print("TEST 6: Update environment")
if created_id:
    result = manage_environments.fn(
        profile="dev", action="update",
        resource_id=created_id,
        config='{"name": "MCP-Test-Renamed"}'
    )
    print(json.dumps(result, indent=2, default=str)[:1000])
    assert result.get("_success") is True

# Test 7: Get extensions (new environment may have empty extensions)
print("\n" + "=" * 60)
print("TEST 7: Get extensions")
if created_id:
    result = manage_environments.fn(
        profile="dev", action="get_extensions",
        resource_id=created_id
    )
    print(json.dumps(result, indent=2, default=str)[:1000])
    # May return empty extensions for new environment — that's OK
    assert result.get("_success") is True or "not found" in str(result.get("error", "")).lower()

# Test 8: Query extensions
print("\n" + "=" * 60)
print("TEST 8: Query extensions")
if created_id:
    result = manage_environments.fn(
        profile="dev", action="query_extensions",
        resource_id=created_id
    )
    print(json.dumps(result, indent=2, default=str)[:1000])
    assert result.get("_success") is True

# Test 9: Delete test environment (cleanup)
print("\n" + "=" * 60)
print("TEST 9: Delete environment")
if created_id:
    result = manage_environments.fn(
        profile="dev", action="delete",
        resource_id=created_id
    )
    print(json.dumps(result, indent=2, default=str)[:500])
    assert result.get("_success") is True

# Test 10: Invalid action
print("\n" + "=" * 60)
print("TEST 10: Invalid action")
result = manage_environments.fn(profile="dev", action="bogus")
print(json.dumps(result, indent=2, default=str)[:500])
assert result.get("_success") is False
assert "valid_actions" in result

# Test 11: Invalid classification
print("\n" + "=" * 60)
print("TEST 11: Invalid classification")
result = manage_environments.fn(
    profile="dev", action="create",
    config='{"name": "Bad Env", "classification": "INVALID"}'
)
assert result.get("_success") is False

# Test 12: Invalid JSON
print("\n" + "=" * 60)
print("TEST 12: Invalid JSON config")
result = manage_environments.fn(profile="dev", action="list", config="{bad json}")
assert result.get("_success") is False

print("\n" + "=" * 60)
print("ALL TESTS PASSED ✅")
```

---

## Acceptance Criteria

1. **All 9 actions work**: list, get, create, update, delete, get_extensions, update_extensions, query_extensions, stats
2. **Classification validation**: Create rejects invalid classification values with helpful error
3. **Update preserves classification**: Update action GETs current environment first, includes classification in PUT
4. **Update rejects classification change**: If user passes `classification` in config, returns clear error
5. **Extensions parsing**: get_extensions correctly parses nested response (all 8 extension types: connections, operations, properties, cross-refs, trading partners, PGP certs, process properties, data maps)
6. **Partial extension updates**: update_extensions with `partial=true` (default) deep-merges rather than overwrites
7. **Stats aggregation**: stats action returns counts by classification
8. **Pagination**: list action and query_extensions handle `query_token` for large accounts
9. **Config JSON string**: All parameters passed via `config` JSON string (MCP parameter parity)
10. **Error handling**: Every action returns `_success` field, errors include helpful messages (409 = conflict, 404 = not found, 403 = permissions)
11. **Import guard**: `server.py` uses try/except import pattern with `manage_environments_action = None` fallback
12. **No readOnlyHint**: Tool annotation does NOT include `readOnlyHint` (tool has write operations)
13. **Test script passes**: All 12 tests in `test_manage_environments.py` pass with a real Boomi account
14. **Extension lifecycle documentation**: Docstring explains the 3-phase lifecycle (define → deploy → configure)

---

## Known Gaps / Future Improvements

1. **Classification values**: Implementation validates only `TEST`, `PROD`. Design doc lists `STAGING`, `DEV` as well. Expand `VALID_CLASSIFICATIONS` when Boomi account supports them.
2. ~~**Extension type coverage**~~: ✅ Fixed in commit `08f389b` — now handles all 8 types.
3. **Extension update caveats not yet enforced**: Cross-reference atomicity (must resend all rows) and custom properties atomicity (must resend all key-value pairs) are not enforced in code — the deep merge handles dicts but not array replacement semantics.
4. **Encrypted field guidance**: get_extensions could flag encrypted fields more prominently in the response.
5. **extensionGroupId support**: query_extensions could accept optional `extension_group_id` for multi-install integration pack scenarios.
6. ~~**Docstring enhancement**~~: ✅ Fixed in commit `08f389b` — extension lifecycle, 8 types, and update caveats added to server.py docstring.

---

## Comparison with Existing Tools

| Aspect | manage_environments | manage_folders | manage_trading_partner |
|--------|-------------------|---------------|----------------------|
| Actions | 9 | 7 | 12 |
| API type | JSON (SDK typed models) | JSON (SDK typed models) | JSON + XML (SDK + builders) |
| readOnlyHint | No | No | No |
| Config param | JSON string | JSON string | JSON string |
| Complexity | Medium (extensions parsing) | Low-medium | High |
| File location | `categories/environments.py` | `categories/folders.py` | `categories/deployment/trading_partners.py` |
| XML manipulation | None | Only for `move` action | Extensive (7 standards) |
| External imports | None | `_shared.component_get_xml` | Multiple XML builders |
| SDK services used | 2 (environment + environment_extensions) | 2 (folder + component_metadata) | 1 (component XML) |

---

## Dependencies

- **boomi-python SDK**: `sdk.environment.*` methods + `sdk.environment_extensions.*` methods
- **No imports from other category modules** (unlike manage_folders which needs `_shared.py`)
- **No new pip packages required**

---

## What NOT to Implement

- **Environment role management**: Roles are admin-level operations, handled by `invoke_boomi_api` generic tool
- **Environment-runtime attachments**: Handled by `manage_runtimes` tool (separate)
- **Extension definition (checkboxes)**: This is part of process component XML, handled by `manage_process` / `manage_component` — not this tool
- **Extensions export/import**: Not in design doc scope — use get_extensions + update_extensions workflow
- **Extension CREATE/DELETE**: Not supported by Boomi API — extensions are auto-generated from deployed processes
- **PersistedProcessProperties**: Completely different API (async, scoped to atom_id not environment_id) — handled by `invoke_boomi_api`
- **Environment cloning**: Not supported by Boomi API
- **Bulk environment operations**: Keep it simple — one environment at a time

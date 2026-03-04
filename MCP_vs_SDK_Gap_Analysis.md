# Boomi MCP Server vs boomi-python SDK — Gap Analysis

**Date:** 2026-03-04
**Purpose:** Identify which boomi-python SDK examples have been implemented as MCP tools and which remain.

---

## Summary

| Category | SDK Examples | Fully Covered | Partial | Missing | Coverage |
|----------|-------------|--------------|---------|---------|----------|
| 01 Discover & Analyze | 8 | 8 | 0 | 0 | 100% |
| 02 Organize & Structure | 3 | 2 | 0 | 1 | 67% |
| 03 Create & Modify | 6 | 6 | 0 | 0 | 100% |
| 04 Environment Setup | 8 | 7 | 0 | 1 | 88% |
| 05 Runtime Setup | 9 | 9 | 0 | 0 | 100% |
| 06 Configure & Deploy | 8 | 4 | 0 | 4 | 50% |
| 07 Package & Deploy | 7 | 7 | 0 | 0 | 100% |
| 08 Execute & Test | 2 | 2 | 0 | 0 | 100% |
| 09 Monitor & Validate | 10 | 5 | 2 | 3 | 60% |
| 10 Version & Compare | 3 | 2 | 0 | 1 | 67% |
| 11 Troubleshoot & Fix | 4 | 0 | 3 | 1 | ~38% |
| 12 Utilities | 2 | 1 | 0 | 1 | 50% |
| **TOTAL** | **70** | **53** | **5** | **12** | **76%** (83% incl. partial) |

**Notes on partial coverage:**
- ⚠️ Partial means the underlying SDK API calls are already exposed via existing MCP tools, but the higher-level workflow/combination is not wrapped as a dedicated action.
- Cat 09: `poll_execution_status` and `analyze_execution_metrics` both use `execution_record.query` which is in `monitor_platform(action="execution_records")` — missing the polling loop and aggregation logic respectively.
- Cat 11: `get_error_details`, `retry_failed_execution`, `reprocess_documents` combine APIs already exposed (`execution_records` + `execution_logs`/`execute_process`) — missing the combined workflow. Only `manage_queues` is truly new API surface.

---

## Detailed Mapping: What's Implemented

### 01_discover_analyze/ — ✅ FULLY COVERED

| SDK Example | MCP Tool | MCP Action |
|-------------|----------|------------|
| `list_all_components.py` | `query_components` | `list` |
| `query_process_components.py` | `query_components` | `list` (with type filter) |
| `get_component.py` | `query_components` | `get` |
| `bulk_get_components.py` | `query_components` | `bulk_get` |
| `find_where_used.py` | `analyze_component` | `where_used` |
| `find_what_uses.py` | `analyze_component` | `dependencies` |
| `analyze_dependencies.py` | `analyze_component` | `dependencies` (+ graph analysis) |
| `analyze_integration_pack.py` | `query_components` | `search` (partial — no dedicated integration pack tool) |

### 02_organize_structure/ — 67% COVERED

| SDK Example | MCP Tool | MCP Action | Status |
|-------------|----------|------------|--------|
| `folder_structure.py` | `manage_folders` | `list` (tree view) | ✅ |
| `manage_folders.py` | `manage_folders` | `list/get/create/move/delete/restore/contents` | ✅ |
| `manage_branches.py` | — | — | ❌ **MISSING** |

### 03_create_modify/ — ✅ FULLY COVERED

| SDK Example | MCP Tool | MCP Action |
|-------------|----------|------------|
| `create_process_component.py` | `manage_process` | `create` |
| `manage_components.py` | `manage_component` | `create/update/clone/delete` |
| `clone_component.py` | `manage_component` | `clone` |
| `update_component.py` | `manage_component` | `update` |
| `update_components.py` | `manage_component` | `update` (batch via repeated calls) |
| `delete_component.py` | `manage_component` | `delete` |

### 04_environment_setup/ — 88% COVERED

| SDK Example | MCP Tool | MCP Action | Status |
|-------------|----------|------------|--------|
| `list_environments.py` | `manage_environments` | `list` | ✅ |
| `create_environment.py` | `manage_environments` | `create` | ✅ |
| `get_environment.py` | `manage_environments` | `get` | ✅ |
| `update_environment.py` | `manage_environments` | `update` | ✅ |
| `delete_environment.py` | `manage_environments` | `delete` | ✅ |
| `query_environments.py` | `manage_environments` | `list` (with filters) | ✅ |
| `manage_environments.py` | `manage_environments` | `list/create/get/update/delete/stats` | ✅ |
| `manage_roles.py` | — | — | ❌ **MISSING** |

### 05_runtime_setup/ — ✅ FULLY COVERED

| SDK Example | MCP Tool | MCP Action |
|-------------|----------|------------|
| `list_runtimes.py` | `manage_runtimes` | `list` |
| `query_runtimes.py` | `manage_runtimes` | `list` (with filters) |
| `manage_runtimes.py` | `manage_runtimes` | `list/get/update/delete` |
| `restart_runtime.py` | `manage_runtimes` | `restart` |
| `create_environment_atom_attachment.py` | `manage_runtimes` | `attach` |
| `query_environment_runtime_attachments.py` | `manage_runtimes` | `list_attachments` |
| `detach_runtime_from_environment.py` | `manage_runtimes` | `detach` |
| `create_installer_token.py` | `manage_runtimes` | `create_installer_token` |
| `manage_java_runtime.py` | `manage_runtimes` | `configure_java` |

### 06_configure_deployment/ — 50% COVERED

| SDK Example | MCP Tool | MCP Action | Status |
|-------------|----------|------------|--------|
| `create_trading_partner.py` | `manage_trading_partner` | `create` | ✅ |
| `delete_trading_partner.py` | `manage_trading_partner` | `delete` | ✅ |
| `manage_environment_extensions.py` | `manage_environments` | `get_extensions/update_extensions/query_extensions` | ✅ |
| `update_environment_extensions.py` | `manage_environments` | `update_extensions` | ✅ |
| `manage_process_schedules.py` | — | — | ❌ **MISSING** |
| `manage_persisted_properties.py` | — | — | ❌ **MISSING** |
| `manage_shared_resources.py` | — | — | ❌ **MISSING** |
| `rotate_secrets.py` | — | — | ❌ **MISSING** |

### 07_package_deploy/ — ✅ FULLY COVERED

| SDK Example | MCP Tool | MCP Action |
|-------------|----------|------------|
| `create_packaged_component.py` | `manage_deployment` | `create_package` |
| `query_packaged_components.py` | `manage_deployment` | `list_packages` |
| `get_packaged_component.py` | `manage_deployment` | `get_package` |
| `delete_packaged_component.py` | `manage_deployment` | `delete_package` |
| `create_deployment.py` | `manage_deployment` | `deploy` |
| `query_deployed_packages.py` | `manage_deployment` | `list_deployments` |
| `promote_package_to_environment.py` | `manage_deployment` | `deploy` (same action, different env) |

### 08_execute_test/ — ✅ FULLY COVERED

| SDK Example | MCP Tool | MCP Action |
|-------------|----------|------------|
| `execute_process.py` | `execute_process` | (direct execution) |
| `execution_records.py` | `monitor_platform` | `execution_records` |

### 09_monitor_validate/ — 60% COVERED (70% incl. partial)

| SDK Example | MCP Tool | MCP Action | Status |
|-------------|----------|------------|--------|
| `download_process_log.py` | `monitor_platform` | `execution_logs` | ✅ |
| `download_execution_artifacts.py` | `monitor_platform` | `execution_artifacts` | ✅ |
| `query_audit_logs.py` | `monitor_platform` | `audit_logs` | ✅ |
| `query_events.py` | `monitor_platform` | `events` | ✅ |
| `get_execution_summary.py` | `monitor_platform` | `execution_records` (partial) | ✅ |
| `poll_execution_status.py` | `monitor_platform` | `execution_records` (API exists, missing polling loop) | ⚠️ **PARTIAL** |
| `analyze_execution_metrics.py` | `monitor_platform` | `execution_records` (API exists, missing aggregation) | ⚠️ **PARTIAL** |
| `monitor_certificates.py` | — | — | ❌ **MISSING** |
| `monitor_throughput.py` | — | — | ❌ **MISSING** |
| `manage_connector_documents.py` | — | — | ❌ **MISSING** |

### 10_version_compare/ — 67% COVERED

| SDK Example | MCP Tool | MCP Action | Status |
|-------------|----------|------------|--------|
| `compare_component_versions.py` | `analyze_component` | `compare_versions` | ✅ |
| `component_diff.py` | `analyze_component` | `compare_versions` | ✅ |
| `merge_components.py` | — | — | ❌ **MISSING** |

### 11_troubleshoot_fix/ — ~38% COVERED (PARTIAL — underlying APIs exist)

| SDK Example | MCP Tool | Status |
|-------------|----------|--------|
| `get_error_details.py` | `monitor_platform` (`execution_records` + `execution_logs`) | ⚠️ **PARTIAL** — underlying APIs exposed, combined workflow missing |
| `retry_failed_execution.py` | `monitor_platform` + `execute_process` | ⚠️ **PARTIAL** — uses `execution_record.query` + `execution_request.create`, both exposed |
| `reprocess_documents.py` | `monitor_platform` + `execute_process` | ⚠️ **PARTIAL** — same core pattern as retry |
| `manage_queues.py` | — | ❌ **MISSING** — queue APIs (`list_queues`, `clear_queue`) not exposed |

### 12_utilities/ — 50% COVERED

| SDK Example | MCP Tool | Status |
|-------------|----------|--------|
| `async_operations.py` | — | ❌ **MISSING** |
| `sample.py` | `boomi_account_info` | ✅ (performs `account.get_account` — same as `boomi_account_info` tool) |

---

## Complete List of Remaining Tools to Implement

### Priority 1 — High Value (Operational workflows)

| # | Feature | SDK Example | SDK Methods | Suggested MCP Tool | Notes |
|---|---------|-------------|-------------|-------------------|-------|
| 1 | **Process Schedules** | `manage_process_schedules.py` | `sdk.process_schedules.query/get/update` | `manage_process_schedules` or add to `manage_deployment` | Schedule create/read/update for automated process execution |
| 2 | **Queue Management** | `manage_queues.py` | `sdk.list_queues.async_get/async_token`, `sdk.clear_queue.execute`, `sdk.move_queue_request.create` | `manage_queues` | List, clear, move queues — critical for troubleshooting |
| 3 | **Retry Failed Execution** | `retry_failed_execution.py` | `sdk.execution_record.query`, `sdk.execution_request.create` | Add `retry` action to `execute_process` or new `troubleshoot` tool | Find failed executions and re-execute them |
| 4 | **Error Details** | `get_error_details.py` | `sdk.execution_record.query`, `sdk.process_log.create` | Add to `monitor_platform` as `error_details` action | Deep-dive into failed execution errors with log correlation |

### Priority 2 — Medium Value (Configuration & monitoring)

| # | Feature | SDK Example | SDK Methods | Suggested MCP Tool | Notes |
|---|---------|-------------|-------------|-------------------|-------|
| 5 | **Certificate Monitoring** | `monitor_certificates.py` | `sdk.deployed_expired_certificate.query` | Add to `monitor_platform` as `certificates` action | Query expiring/expired certificates |
| 6 | **Throughput Monitoring** | `monitor_throughput.py` | `sdk.throughput_account.query` | Add to `monitor_platform` as `throughput` action | Account-level throughput analytics |
| 7 | **Execution Metrics** | `analyze_execution_metrics.py` | `sdk.execution_record.query` (with aggregation) | Add to `monitor_platform` as `execution_metrics` action | Statistical analysis of execution performance |
| 8 | **Poll Execution Status** | `poll_execution_status.py` | `sdk.execution_record.query` (with polling) | Add to `execute_process` as polling/wait capability | Wait for execution completion with status updates |
| 9 | **Persisted Properties** | `manage_persisted_properties.py` | `sdk.persisted_process_properties.async_get/async_token/update` | `manage_persisted_properties` or add to `manage_environments` | Runtime-level persisted process properties |
| 10 | **Shared Resources** | `manage_shared_resources.py` | `sdk.shared_web_server.get/update`, `sdk.shared_communication_channel_component.query/get/create` | `manage_shared_resources` | Shared web servers, communication channels |

### Priority 3 — Lower Value (Advanced/niche features)

| # | Feature | SDK Example | SDK Methods | Suggested MCP Tool | Notes |
|---|---------|-------------|-------------|-------------------|-------|
| 11 | **Branch Management** | `manage_branches.py` | `sdk.branch.query/create/get/delete` | `manage_branches` | Component version branching |
| 12 | **Role Management** | `manage_roles.py` | `sdk.role.query/create/get/update/delete` | `manage_roles` | User roles and privileges (admin feature) |
| 13 | **Secrets Rotation** | `rotate_secrets.py` | `sdk.refresh_secrets_manager.refresh` | `manage_secrets` or add to `manage_environments` | Refresh secrets from external providers |
| 14 | **Connector Documents** | `manage_connector_documents.py` | `sdk.generic_connector_record.query`, `sdk.connector_document.create` | Add to `manage_connector` or `monitor_platform` | Track/resubmit connector documents |
| 15 | **Reprocess Documents** | `reprocess_documents.py` | `sdk.execution_record.query`, `sdk.execution_request.create` | Add to `troubleshoot` tool | Re-run with queue/document handling |
| 16 | **Component Merge** | `merge_components.py` | `sdk.component.get/update` (branch merge logic) | Add `merge` action to `analyze_component` | Merge component versions across branches |
| 17 | **Async Operations** | `async_operations.py` | Various `async_get_*/async_token_*` methods | Infrastructure (not a tool itself) | Atom counters, disk space, listener status, security policies |

---

## Recommended Implementation Order

### Phase 1: Troubleshooting & Operations (4 tools)
These fill the biggest functional gap — the entire 11_troubleshoot_fix category is missing.

1. **`manage_queues`** — List/clear/move queues
2. **Add `error_details` to `monitor_platform`** — Failed execution deep-dive
3. **Add `retry` to `execute_process`** — Retry failed executions
4. **Add `poll_status` to `execute_process`** — Wait for completion

### Phase 2: Enhanced Monitoring (3 actions)
Add to existing `monitor_platform` tool:

5. **`certificates`** — Expiring certificate alerts
6. **`throughput`** — Account throughput analytics
7. **`execution_metrics`** — Execution performance stats

### Phase 3: Configuration Management (4 tools)
The 06_configure_deployment gap:

8. **`manage_process_schedules`** — Schedule CRUD
9. **`manage_persisted_properties`** — Runtime process properties
10. **`manage_shared_resources`** — Shared web servers, comm channels
11. **`manage_secrets`** — Secret rotation

### Phase 4: Advanced Features (3 tools)

12. **`manage_branches`** — Component branching
13. **`manage_roles`** — Role/privilege management
14. **Add `merge` to `analyze_component`** — Cross-branch merges

---

## Async Operations Gap

The `async_operations.py` example reveals several SDK async capabilities not exposed as MCP tools:

| Async Operation | SDK Method | Current MCP Coverage |
|----------------|------------|---------------------|
| Atom Counters | `sdk.atom.async_get_atom_counters()` | ❌ Not exposed |
| Persisted Process Properties | `sdk.atom.async_get_persisted_process_properties()` | ❌ Not exposed |
| Atom Disk Space | `sdk.atom_disk_space.async_get_atom_disk_space()` | ❌ Not exposed |
| List Queues | `sdk.list_queues.async_get_list_queues()` | ❌ Not exposed |
| Listener Status | `sdk.listener_status.async_get_listener_status()` | ❌ Not exposed |
| Atom Security Policies | `sdk.atom_security_policies.async_get_atom_security_policies()` | ❌ Not exposed |

These could be consolidated into a `runtime_diagnostics` tool or added as actions to `manage_runtimes`.

---

## Notes

- **`invoke_boomi_api`** serves as a catch-all for any missing functionality, but dedicated tools provide better UX with validation, formatting, and error handling.
- **Environment extensions** are already covered in `manage_environments` (get_extensions, update_extensions, query_extensions).
- **`promote_package_to_environment.py`** is functionally identical to `deploy` — just targeting a different environment. Already covered.
- **`sample.py`** performs `account.get_account` — already covered by `boomi_account_info` tool.

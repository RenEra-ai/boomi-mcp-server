# Design Document: Remaining MCP Tool Implementation

**Date:** 2026-03-04
**Status:** Draft — pending review
**Related:** `MCP_vs_SDK_Gap_Analysis.md`

---

## 1. Current State

### Existing Tools & Action Density

| # | MCP Tool | Actions | Category |
|---|----------|---------|----------|
| 1 | `manage_trading_partner` | 7 (list, get, create, update, delete, analyze_usage, list_options) | Components |
| 2 | `manage_process` | 5 (list, get, create, update, delete) | Components |
| 3 | `query_components` | 4 (list, get, search, bulk_get) | Components |
| 4 | `manage_component` | 4 (create, update, clone, delete) | Components |
| 5 | `analyze_component` | 3 (where_used, dependencies, compare_versions) | Components |
| 6 | `manage_connector` | 7 (list_types, get_type, list, get, create, update, delete) | Components |
| 7 | `manage_folders` | 7 (list, get, create, move, delete, restore, contents) | Structure |
| 8 | `manage_environments` | 9 (list, get, create, update, delete, get_extensions, update_extensions, query_extensions, stats) | Infrastructure |
| 9 | `manage_runtimes` | 17 (list, get, create, update, delete, attach, detach, list_attachments, restart, configure_java, create_installer_token, available_clouds, cloud_list, cloud_get, cloud_create, cloud_update, cloud_delete) | Infrastructure |
| 10 | `manage_deployment` | 8 (list_packages, get_package, create_package, delete_package, deploy, undeploy, list_deployments, get_deployment) | Deployment |
| 11 | `execute_process` | 1 (execute) | Execution |
| 12 | `monitor_platform` | 5 (execution_records, execution_logs, execution_artifacts, audit_logs, events) | Monitoring |
| 13 | `get_schema_template` | N/A (reference data) | Meta |
| 14 | `invoke_boomi_api` | N/A (escape hatch) | Meta |
| 15 | `list_capabilities` | N/A (discovery) | Meta |
| 16 | `set_boomi_credentials` | N/A (local only) | Auth |
| 17 | `delete_boomi_profile` | N/A (local only) | Auth |
| 18 | `list_boomi_profiles` | N/A (both modes) | Auth |
| 19 | `boomi_account_info` | N/A (both modes) | Auth |

**Total user-facing tools: 15** (excluding auth/meta) with **77 actions**

### Design Constraints

From MCP best practices:

- **Tool descriptions must be narrow and unambiguous** — LLMs pick tools based on descriptions
- **Keep tool operations focused** — but atomic tools with 1 action each create discovery overhead
- **Avoid too many tools** — LLMs struggle with 40+ tools; tool selection accuracy drops
- **Avoid mega-tools** — tools with 15+ actions become confusing; LLMs guess wrong action names

**Sweet spot: 5–10 actions per tool, 15–25 total tools.**

Current `manage_runtimes` at 17 actions is already at the upper limit. Adding more mega-tools would hurt LLM performance.

---

## 2. Design Decisions

### Decision Framework

For each missing capability, the choice is:

| Option | When to use |
|--------|-------------|
| **Add action to existing tool** | Same domain, same SDK client, same `profile` + `action` pattern, tool stays under ~12 actions |
| **Create new tool** | Different operational domain, different user intent, would push existing tool past ~12 actions |
| **Skip / defer** | Niche feature, covered by `invoke_boomi_api` escape hatch, or utility pattern (not a tool) |

### Key Principle: Group by User Intent, Not SDK Structure

The SDK has `manage_process_schedules.py` and `manage_persisted_properties.py` as separate files because they're separate API endpoints. But the MCP user thinks in workflows: "I want to configure my deployment" or "I want to troubleshoot a failure." Tools should match user intent.

---

## 3. Tool Placement Plan

### 3.1 New Tool: `manage_schedules` (NEW — 4 actions)

**Why new tool:** Process schedules are a distinct operational concept — "when does my process run?" is fundamentally different from "what is my process?" or "deploy my process." Adding to `manage_deployment` or `manage_process` would muddy their purpose.

**Actions:**
| Action | SDK Example | SDK Method |
|--------|------------|------------|
| `list` | `manage_process_schedules.py` | `sdk.process_schedules.query_process_schedules()` |
| `get` | `manage_process_schedules.py` | `sdk.process_schedules.get_process_schedules()` |
| `update` | `manage_process_schedules.py` | `sdk.process_schedules.update_process_schedules()` |
| `delete` | `manage_process_schedules.py` | `sdk.process_schedules.delete_process_schedules()` |

**Parameters:** `profile, action, process_id, environment_id, config`

**File:** `src/boomi_mcp/categories/schedules.py`

---

### 3.2 New Tool: `troubleshoot_execution` (NEW — 5 actions)

**Why new tool:** The entire 11_troubleshoot_fix category is missing (0% coverage). These are high-value operational workflows that share a common pattern: find a problem → diagnose → fix. They don't belong in `monitor_platform` (which is read-only observation) or `execute_process` (which is about triggering runs). Troubleshooting is its own intent.

**Actions:**
| Action | SDK Example | SDK Method |
|--------|------------|------------|
| `error_details` | `get_error_details.py` | `sdk.execution_record.query()` + `sdk.process_log.create()` |
| `retry` | `retry_failed_execution.py` | `sdk.execution_record.query()` + `sdk.execution_request.create()` |
| `reprocess` | `reprocess_documents.py` | `sdk.execution_record.query()` + `sdk.execution_request.create()` |
| `list_queues` | `manage_queues.py` | `sdk.list_queues.async_get()` + `sdk.list_queues.async_token()` |
| `clear_queue` | `manage_queues.py` | `sdk.clear_queue.execute()` |

**Parameters:** `profile, action, execution_id, process_id, environment_id, config`

**File:** `src/boomi_mcp/categories/troubleshooting.py`

**Design notes:**
- `error_details` combines execution record lookup with log download — a single-call deep dive
- `retry` finds a failed execution and re-submits it with the same or modified properties
- `reprocess` re-runs connector document processing for failed records
- `list_queues` and `clear_queue` cover queue management for stuck processes
- `move_queue` from SDK example is omitted initially (very niche, can use `invoke_boomi_api`)

---

### 3.3 Extend: `monitor_platform` → add 4 actions (5 → 9 actions)

**Why extend:** These are all read-only monitoring/observability queries that fit the existing `monitor_platform` intent — "show me what's happening." The tool stays well under the action limit.

**New actions:**
| Action | SDK Example | SDK Method |
|--------|------------|------------|
| `certificates` | `monitor_certificates.py` | `sdk.deployed_expired_certificate.query()` |
| `throughput` | `monitor_throughput.py` | `sdk.throughput_account.query()` |
| `execution_metrics` | `analyze_execution_metrics.py` | `sdk.execution_record.query()` (with aggregation logic) |
| `connector_documents` | `manage_connector_documents.py` | `sdk.generic_connector_record.query()` |

**Updated tool signature:** No change — still `profile, action, config`

**File:** Extend `src/boomi_mcp/categories/monitoring.py`

**Design notes:**
- `certificates` queries `DeployedExpiredCertificate` — shows expiring/expired certs with days-until-expiry
- `throughput` queries `ThroughputAccount` — account-level usage metrics by date range
- `execution_metrics` reuses `execution_record.query()` but adds client-side aggregation (success rate, avg duration, top failures) — distinct from raw `execution_records` which just lists records
- `connector_documents` queries `GenericConnectorRecord` — document-level tracking for connector operations
- `poll_execution_status` is deliberately NOT added here — it's better as part of `execute_process` (see 3.6)

---

### 3.4 New Tool: `manage_shared_resources` (NEW — 5 actions)

**Why new tool:** Shared resources (web servers, communication channels, certificates) are a distinct configuration domain. They don't fit in `manage_environments` (which is about environment CRUD and extensions) or any existing tool. In Boomi, shared resources are atom-level configurations that multiple processes use.

**Actions:**
| Action | SDK Example | SDK Method |
|--------|------------|------------|
| `list_web_servers` | `manage_shared_resources.py` | `sdk.shared_web_server.get()` |
| `update_web_server` | `manage_shared_resources.py` | `sdk.shared_web_server.update()` |
| `list_channels` | `manage_shared_resources.py` | `sdk.shared_communication_channel_component.query()` |
| `get_channel` | `manage_shared_resources.py` | `sdk.shared_communication_channel_component.get()` |
| `create_channel` | `manage_shared_resources.py` | `sdk.shared_communication_channel_component.create()` |

**Parameters:** `profile, action, resource_id, config`

**File:** `src/boomi_mcp/categories/shared_resources.py`

---

### 3.5 Extend: `manage_environments` → add 2 actions (9 → 11 actions)

**Why extend:** Persisted properties and secrets rotation are environment/runtime-level configuration — they're about "how is my environment configured" which is exactly what `manage_environments` covers. Extensions are already here.

**New actions:**
| Action | SDK Example | SDK Method |
|--------|------------|------------|
| `get_properties` | `manage_persisted_properties.py` | `sdk.persisted_process_properties.async_get()` + `.async_token()` |
| `update_properties` | `manage_persisted_properties.py` | `sdk.persisted_process_properties.update()` |

**Updated tool signature:** No change — still `profile, action, resource_id, config`

**File:** Extend `src/boomi_mcp/categories/environments.py`

**Design notes:**
- Persisted properties are bound to an atom (runtime), queried by atom_id
- `resource_id` serves as atom_id for these actions
- This keeps environment configuration in one place: env CRUD + extensions + persisted properties

---

### 3.6 Extend: `execute_process` → add 1 action (1 → 2 actions)

**Why extend:** Polling for execution completion is part of the "execute and get results" workflow. Users who execute a process almost always want to know when it finishes.

**New action:**
| Action | SDK Example | SDK Method |
|--------|------------|------------|
| `execute_and_wait` | `poll_execution_status.py` | `sdk.execution_request.create()` + `sdk.execution_record.query()` (polling loop) |

**Updated tool behavior:**
- Current `execute_process` stays as-is (fire-and-forget)
- New parameter `wait: bool = False` — when True, polls execution status until completion or timeout
- Returns execution result including status, duration, error details if failed
- Max poll time: 5 minutes with configurable timeout in `config`

**File:** Extend `src/boomi_mcp/categories/execution.py`

---

### 3.7 Extend: `analyze_component` → add 1 action (3 → 4 actions)

**Why extend:** Component merging is a version/branch operation that fits with the existing analysis tool — `compare_versions` is already here, and `merge` is the natural next step after comparing.

**New action:**
| Action | SDK Example | SDK Method |
|--------|------------|------------|
| `merge` | `merge_components.py` | `sdk.component.get()` + `sdk.component.update()` |

**File:** Extend `src/boomi_mcp/categories/components/analyze_component.py`

---

### 3.8 New Tool: `manage_account` (NEW — 4 actions)

**Why new tool:** Roles, branches, and secrets rotation are account-level administrative operations. They don't fit into any component or infrastructure tool. Grouping them into one "account administration" tool keeps the total tool count low while covering three separate SDK examples.

**Actions:**
| Action | SDK Example | SDK Method |
|--------|------------|------------|
| `list_roles` | `manage_roles.py` | `sdk.role.query_role()` |
| `manage_role` | `manage_roles.py` | `sdk.role.create/get/update/delete_role()` |
| `list_branches` | `manage_branches.py` | `sdk.branch.query_branch()` |
| `manage_branch` | `manage_branches.py` | `sdk.branch.create/get/delete_branch()` |

**Parameters:** `profile, action, resource_id, config`

**File:** `src/boomi_mcp/categories/account.py`

**Design notes:**
- `manage_role` uses `config.operation` (create/get/update/delete) as a sub-action to avoid 8 separate actions
- `manage_branch` uses same pattern
- Secrets rotation (`rotate_secrets.py`) is deferred — it's a one-off admin operation that `invoke_boomi_api` handles well

---

### 3.9 Extend: `manage_runtimes` → add 3 actions (17 → 20 actions)

**Why extend (with caveat):** The async diagnostic operations (atom counters, disk space, listener status) are runtime-specific queries. However, `manage_runtimes` is already at 17 actions — the heaviest tool.

**Recommended approach:** Add these as a single `diagnostics` action that returns all diagnostics in one call rather than 3 separate actions.

**New action:**
| Action | SDK Example | SDK Method |
|--------|------------|------------|
| `diagnostics` | `async_operations.py` | `sdk.atom.async_get_atom_counters()`, `sdk.atom_disk_space.async_get()`, `sdk.listener_status.async_get()` |

**This is a single action that returns a combined diagnostic report:**
```json
{
  "counters": { ... },
  "disk_space": { ... },
  "listener_status": { ... }
}
```

**File:** Extend `src/boomi_mcp/categories/runtimes.py`

**Alternative:** If `manage_runtimes` is too overloaded, create a separate `runtime_diagnostics` read-only tool. Decision can be made during implementation.

---

### 3.10 Deferred Items

| Feature | SDK Example | Reason for Deferral |
|---------|------------|-------------------|
| **Secrets Rotation** | `rotate_secrets.py` | Niche admin operation. Single API call (`sdk.refresh_secrets_manager.refresh()`). Covered by `invoke_boomi_api`. Can add to `manage_account` later. |
| **Move Queue** | `manage_queues.py` | Very niche operation. `list_queues` and `clear_queue` cover 90% of queue use cases. Covered by `invoke_boomi_api`. |
| **Atom Security Policies** | `async_operations.py` | Admin-level security configuration. Covered by `invoke_boomi_api`. |
| **`sample.py`** | `12_utilities/sample.py` | Template file, not a feature. |

---

## 4. Final Tool Inventory (After Implementation)

### Updated Tool Count: 18 user-facing tools (+3 new)

| # | Tool | Actions | Status | Change |
|---|------|---------|--------|--------|
| 1 | `manage_trading_partner` | 7 | Existing | — |
| 2 | `manage_process` | 5 | Existing | — |
| 3 | `query_components` | 4 | Existing | — |
| 4 | `manage_component` | 4 | Existing | — |
| 5 | `analyze_component` | **4** | Extended | +1 (merge) |
| 6 | `manage_connector` | 7 | Existing | — |
| 7 | `manage_folders` | 7 | Existing | — |
| 8 | `manage_environments` | **11** | Extended | +2 (properties) |
| 9 | `manage_runtimes` | **18** | Extended | +1 (diagnostics) |
| 10 | `manage_deployment` | 8 | Existing | — |
| 11 | `execute_process` | **2** | Extended | +1 (wait) |
| 12 | `monitor_platform` | **9** | Extended | +4 (certs, throughput, metrics, docs) |
| 13 | `manage_schedules` | **4** | **NEW** | Process schedule CRUD |
| 14 | `troubleshoot_execution` | **5** | **NEW** | Error details, retry, reprocess, queues |
| 15 | `manage_shared_resources` | **5** | **NEW** | Web servers, comm channels |
| 16 | `manage_account` | **4** | **NEW** | Roles, branches |
| 17 | `get_schema_template` | N/A | Existing | — |
| 18 | `invoke_boomi_api` | N/A | Existing | — |
| 19 | `list_capabilities` | N/A | Existing | — |

**Total actions: ~104** across 19 tools (excluding auth tools)

### Coverage After Implementation

| Category | Before (full/partial) | After |
|----------|----------------------|-------|
| 01 Discover & Analyze | 100% | 100% |
| 02 Organize & Structure | 67% | **100%** |
| 03 Create & Modify | 100% | 100% |
| 04 Environment Setup | 88% | **100%** |
| 05 Runtime Setup | 100% | 100% |
| 06 Configure & Deploy | 50% (was undercounted as 25%) | **88%** (secrets rotation deferred) |
| 07 Package & Deploy | 100% | 100% |
| 08 Execute & Test | 100% | 100% |
| 09 Monitor & Validate | 60% (70% w/ partial) | **100%** |
| 10 Version & Compare | 67% | **100%** |
| 11 Troubleshoot & Fix | ~38% partial (was undercounted as 0%) | **100%** |
| 12 Utilities | 50% (sample.py = boomi_account_info) | **75%** (diagnostics added) |
| **OVERALL** | **76%** (83% incl. partial) | **97%** |

---

## 5. Implementation Order

### Phase 1: Highest Value (3 items)

These fill the biggest operational gaps and are the most requested by users:

1. **`troubleshoot_execution`** (NEW tool, 5 actions) — the entire troubleshooting category is missing
2. **`monitor_platform` extensions** (+4 actions) — certificates, throughput, metrics, connector docs
3. **`execute_process` + wait** (+1 action) — poll until completion

**Estimated effort:** Medium — mostly query wrappers with some aggregation logic
**Files touched:** 1 new file + 2 existing files

### Phase 2: Configuration Management (3 items)

4. **`manage_schedules`** (NEW tool, 4 actions) — process schedule management
5. **`manage_environments` + properties** (+2 actions) — persisted process properties
6. **`manage_shared_resources`** (NEW tool, 5 actions) — web servers, communication channels

**Estimated effort:** Medium — standard CRUD patterns
**Files touched:** 2 new files + 1 existing file

### Phase 3: Administration & Advanced (3 items)

7. **`manage_account`** (NEW tool, 4 actions) — roles and branches
8. **`analyze_component` + merge** (+1 action) — cross-branch component merge
9. **`manage_runtimes` + diagnostics** (+1 action) — atom counters, disk, listeners

**Estimated effort:** Low-Medium — mostly straightforward CRUD and query wrappers
**Files touched:** 1 new file + 2 existing files

---

## 6. File Structure (After Implementation)

```
src/boomi_mcp/categories/
├── components/
│   ├── trading_partners.py      # manage_trading_partner (existing)
│   ├── processes.py             # manage_process (existing)
│   ├── query_components.py      # query_components (existing)
│   ├── manage_component.py      # manage_component (existing)
│   ├── analyze_component.py     # analyze_component (EXTEND: +merge)
│   ├── connectors.py            # manage_connector (existing)
│   └── organizations.py         # manage_organization (existing, internal)
├── deployment/
│   └── packages.py              # manage_deployment (existing)
├── environments.py              # manage_environments (EXTEND: +properties)
├── runtimes.py                  # manage_runtimes (EXTEND: +diagnostics)
├── folders.py                   # manage_folders (existing)
├── execution.py                 # execute_process (EXTEND: +wait)
├── monitoring.py                # monitor_platform (EXTEND: +4 actions)
├── schedules.py                 # manage_schedules (NEW)
├── troubleshooting.py           # troubleshoot_execution (NEW)
├── shared_resources.py          # manage_shared_resources (NEW)
├── account.py                   # manage_account (NEW)
└── meta_tools.py                # get_schema_template, invoke_boomi_api, list_capabilities (existing)
```

---

## 7. server.py Registration Pattern

Each new tool/extension follows the existing pattern in `server.py`:

```python
# --- New Tool Import ---
try:
    from boomi_mcp.categories.troubleshooting import troubleshoot_execution_action
    print(f"[INFO] Troubleshooting tools loaded successfully")
except ImportError as e:
    print(f"[WARNING] Failed to import troubleshooting tools: {e}")
    troubleshoot_execution_action = None

# --- Register tool (inside tool registration block) ---
if troubleshoot_execution_action:
    @mcp.tool()
    def troubleshoot_execution(
        profile: str,
        action: str,
        execution_id: str = None,
        process_id: str = None,
        environment_id: str = None,
        config: str = None,
    ):
        """Troubleshoot failed executions: get error details, retry, reprocess documents, manage queues.

        Actions: error_details, retry, reprocess, list_queues, clear_queue
        """
        sdk = _get_sdk(profile)
        return troubleshoot_execution_action(sdk, action, ...)
```

---

## 8. Appendix: SDK Method → Tool Action Mapping (Complete)

| SDK Method | Tool | Action |
|-----------|------|--------|
| `sdk.process_schedules.query/get/update/delete` | `manage_schedules` | list, get, update, delete |
| `sdk.execution_record.query` + `sdk.process_log.create` | `troubleshoot_execution` | error_details |
| `sdk.execution_record.query` + `sdk.execution_request.create` | `troubleshoot_execution` | retry |
| `sdk.execution_record.query` + `sdk.execution_request.create` | `troubleshoot_execution` | reprocess |
| `sdk.list_queues.async_get/async_token` | `troubleshoot_execution` | list_queues |
| `sdk.clear_queue.execute` | `troubleshoot_execution` | clear_queue |
| `sdk.deployed_expired_certificate.query` | `monitor_platform` | certificates |
| `sdk.throughput_account.query` | `monitor_platform` | throughput |
| `sdk.execution_record.query` (aggregated) | `monitor_platform` | execution_metrics |
| `sdk.generic_connector_record.query` | `monitor_platform` | connector_documents |
| `sdk.execution_request.create` + poll loop | `execute_process` | execute (with wait=True) |
| `sdk.shared_web_server.get/update` | `manage_shared_resources` | list_web_servers, update_web_server |
| `sdk.shared_communication_channel_component.query/get/create` | `manage_shared_resources` | list_channels, get_channel, create_channel |
| `sdk.persisted_process_properties.async_get/update` | `manage_environments` | get_properties, update_properties |
| `sdk.role.query/create/get/update/delete` | `manage_account` | list_roles, manage_role |
| `sdk.branch.query/create/get/delete` | `manage_account` | list_branches, manage_branch |
| `sdk.component.get + update` (merge logic) | `analyze_component` | merge |
| `sdk.atom.async_get_atom_counters` + disk + listeners | `manage_runtimes` | diagnostics |
| `sdk.refresh_secrets_manager.refresh` | *deferred* | — |
| `sdk.move_queue_request.create` | *deferred* | — |

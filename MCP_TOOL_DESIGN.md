# Boomi MCP Integration Builder - V2 Design (Authoritative)

Status: active
Version: 2.0
Last updated: 2026-03-10

This document supersedes prior design notes. V2 is JSON-first with an explicit XML compiler boundary and a high-level integration orchestrator.

## 1. Problem and Target Outcome

Agents need to build Boomi integrations from a source-system description (components, goals, endpoints, dependencies) in two modes:

- Lift-and-shift: reproduce existing behavior with minimal functional change.
- Redesign: preserve goals while modernizing structure or routing.

V2 provides a single orchestration entrypoint (`build_integration`) and keeps low-level tools available for explicit control.

## 2. Core Decisions

1. JSON-only input for process and integration creation/update.
2. Hard removal of YAML process config paths.
3. XML remains required only where Boomi endpoints are XML-native (`/Component` family).
4. `build_integration` is the canonical high-level tool for multi-component builds.
5. Legacy trading-partner XML example modules are removed when not runtime-critical.

## 3. Tool Surface (V2)

### 3.1 `manage_process` (JSON config)

Purpose: process CRUD with JSON config that compiles to XML for Boomi Component API.

Inputs:
- `action`: `list | get | create | update | delete`
- `process_id`: required for `get | update | delete`
- `config`: JSON string, required for `create | update`
- `filters`: JSON string, optional for `list`

Behavior:
- `create/update` parse JSON into validated models (`ProcessConfig` / `ComponentSpec`).
- Compiler path: JSON -> process model -> XML -> `/Component` or `/Component/{componentId}`.

### 3.2 `manage_component` (XML for generic components)

Purpose: generic component lifecycle where raw XML is required.

Inputs:
- `action`: `create | update | clone | delete`
- `component_id`: required for `update | clone | delete`
- `config`: JSON string

Behavior:
- `create` requires `config.xml`.
- No YAML delegation path exists.

### 3.3 `build_integration` (new orchestrator)

Purpose: high-level integration build lifecycle with deterministic ordering and conflict policy.

Inputs:
- `action`: `plan | apply | verify`
- `config`: JSON string

Action semantics:
- `plan` (read-only): normalize + validate spec, resolve dependencies, detect existing components, return execution plan.
- `apply` (mutating): execute ordered operations. `dry_run` defaults to `true`.
- `verify` (read-only): validate created resources and declared dependency wiring using `build_id`.

Conflict policy:
- `reuse`: use existing component when create target exists.
- `clone`: create with deterministic clone naming.
- `fail`: stop when create target already exists.

## 4. Canonical Contract: `IntegrationSpecV1`

Internal schema (`src/boomi_mcp/models/integration_models.py`) used by `build_integration`.

```json
{
  "version": "1.0",
  "name": "Order Sync",
  "mode": "lift_shift",
  "components": [
    {
      "key": "order_connection",
      "type": "connector-settings",
      "action": "create",
      "name": "Order API Connection",
      "component_id": null,
      "depends_on": [],
      "config": {
        "connector_type": "http",
        "component_name": "Order API Connection",
        "url": "https://api.example.com/orders",
        "auth_type": "NONE"
      }
    },
    {
      "key": "order_process",
      "type": "process",
      "action": "create",
      "name": "Order Process",
      "depends_on": ["order_connection"],
      "config": {
        "name": "Order Process",
        "shapes": [
          {"type": "start", "name": "start"},
          {
            "type": "connector",
            "name": "get_orders",
            "config": {
              "connector_id": "$ref:order_connection",
              "operation": "Get",
              "object_type": "orders"
            }
          },
          {"type": "stop", "name": "end"}
        ]
      }
    }
  ],
  "goals": [],
  "endpoints": [],
  "flows": [],
  "naming": {},
  "folders": {},
  "runtime": {},
  "validation_rules": {}
}
```

Normalization supported:
- `type` aliases (`connector`, `connection`, `tradingpartner`, etc.).
- `depends_on` or `dependencies`.
- `config` or `spec`.
- Top-level `source_description` can be lifted into `integration_spec`.

## 5. XML Compiler Boundary

V2 keeps strict separation:

- JSON-native routes:
  - Trading partners (`TradingPartnerComponent` APIs)
  - Connector catalog discovery (`Connector` APIs)
  - Metadata query/discovery (`ComponentMetadata` APIs)

- XML-native routes:
  - Generic component create/update (`/Component`, `/Component/{componentId}`)
  - Process create/update (JSON compiled to component XML before transport)

This prevents pushing synthetic XML authoring to upstream agents and keeps endpoint-specific serialization in one place.

## 6. Endpoint Routing Policy

Routing used by `build_integration` and low-level tools:

| Route key | Component types | Endpoint family | Payload format | Notes |
|---|---|---|---|---|
| `process_json_to_xml` | `process` | `/Component`, `/Component/{componentId}` | JSON in -> compiled XML out | Uses process builder/orchestrator |
| `connector_builder_or_xml` | `connector-settings`, `connector-action` | `/Component*` + `/Connector*` for catalog | JSON builder or raw XML | Validates connector type via `/Connector/{type}` when present |
| `trading_partner_json` | `trading_partner` | `/TradingPartnerComponent*` | Typed JSON models | No XML template dependency |
| `generic_component_xml` | `component` (or unknown) | `/Component*` | Raw XML in `config.xml` | Explicit XML-only path |
| `metadata_discovery` | all during planning | `/ComponentMetadata/query`, `/ComponentMetadata/queryMore` | JSON | Used for existence checks |

Endpoint mapping source of truth:
- Local boomi-python service definitions under `../boomi-python/src/boomi/services/` (`component.py`, `component_metadata.py`, `trading_partner_component.py`, `connector.py`).

## 7. `build_integration` Execution Flow

### 7.1 `plan`

1. Normalize input into `IntegrationSpecV1`.
2. Validate schema and dependency graph.
3. Topologically sort components.
4. Detect existing components by type + name.
5. Compute per-step plan (`planned_action`, route, dependency chain).

Output includes:
- normalized `integration_spec`
- `execution_order`
- step-level route and conflict resolution plan

### 7.2 `apply`

1. Build plan (same logic as `plan`).
2. If `dry_run=true`, return plan only.
3. Resolve dependency tokens (`$ref:<key>`) using created/reused IDs.
4. Execute in deterministic order.
5. Persist run state in in-memory build registry and return `build_id`.

Failure model:
- Stop-on-first-failure.
- Return `failed_step` + `partial_results`.
- No rollback is attempted (Boomi API operations are not globally transactional).

### 7.3 `verify`

1. Load build state via `build_id`.
2. Verify each materialized component exists by route-specific read API.
3. Check dependency references were resolved to component IDs.
4. Return aggregate success and per-component verification.

## 8. Lift-and-Shift vs Redesign in V2

Both modes use the same contract; `mode` affects planning intent:

- `lift_shift`:
  - prefer structure parity
  - minimal renaming
  - `conflict_policy=reuse` usually preferred

- `redesign`:
  - allow structural optimization and renamed components
  - often paired with `clone` during migration
  - more explicit dependency declarations expected

In the current implementation, mode is preserved in the normalized spec and returned in plans/results for orchestration decisions and auditability.

## 9. MCP Tooling Practices Applied

V2 aligns with practical MCP design guidance:

1. Clear tool semantics:
   - read-only planning and verification separated from mutation (`apply`).
2. Deterministic behavior:
   - stable dependency order for reproducible runs.
3. Strong contracts:
   - explicit typed schema (`IntegrationSpecV1`) and strict JSON parsing.
4. Safe-by-default mutation:
   - `dry_run=true` default for `apply`.
5. Bounded scope per tool:
   - `manage_component` remains low-level XML transport; orchestration sits in `build_integration`.
6. Transparent errors:
   - structured error payloads with failing step and partial results.

Note on MCP annotations:
- `build_integration` is a mixed read/write tool. Read-only behavior is action-level (`plan`, `verify`), while `apply` is mutating.

## 10. Removed in V2

Hard removed:
- YAML process parser: `src/boomi_mcp/xml_builders/yaml_parser.py`
- `config_yaml` API paths from tool contracts

Aggressive legacy cleanup (non-runtime examples removed):
- `src/boomi_mcp/trading_partner_tools.py`
- `src/boomi_mcp/categories/deployment/trading_partners.py`
- example-only trading-partner XML builders in `src/boomi_mcp/categories/components/builders/`:
  - `base_builder.py`
  - `communication.py`
  - `x12_builder.py`

Deployment exports updated accordingly.

## 11. Current Module Map (V2)

- `server.py`
  - registers `manage_process`, `manage_component`, `build_integration`
- `src/boomi_mcp/categories/components/processes.py`
  - JSON process config path and action router
- `src/boomi_mcp/xml_builders/json_parser.py`
  - JSON -> `ComponentSpec` parser/normalizer
- `src/boomi_mcp/categories/components/manage_component.py`
  - XML-only generic component create/update/clone/delete
- `src/boomi_mcp/categories/integration_builder.py`
  - plan/apply/verify orchestrator
- `src/boomi_mcp/models/integration_models.py`
  - `IntegrationSpecV1` and `IntegrationComponentSpec`
- `src/boomi_mcp/categories/meta_tools.py`
  - updated JSON templates, integration templates, capabilities/workflows

## 12. Operational Guidance for Agent Workflows

Recommended sequence when another agent provides a source integration description:

1. Build normalized spec draft in `IntegrationSpecV1` format.
2. Call `build_integration(action='plan')`.
3. Review step routes and conflict actions.
4. Call `build_integration(action='apply', dry_run=false)`.
5. Call `build_integration(action='verify')`.
6. For edge cases not covered by high-level model, fall back to low-level tools:
   - `manage_process`
   - `manage_connector`
   - `manage_component`
   - `manage_trading_partner`

## 13. References

- Model Context Protocol docs: https://modelcontextprotocol.io/docs
- MCP specification: https://modelcontextprotocol.io/specification
- Boomi API developer portal: https://developer.boomi.com/
- Boomi endpoint and payload behavior used in this implementation: local boomi-python SDK service modules in `../boomi-python/src/boomi/services/`

# Boomi MCP Component Creation Design - V3

Status: active
Version: 3.0
Last updated: 2026-05-15
Supersedes: `docs/archive/MCP_TOOL_DESIGN_V2_2026-03-10.md`

## 1. Purpose

The component creation part of this MCP must let an LLM do the work a human Boomi developer previously did in the Boomi UI: understand an integration task, choose a proven design pattern, ask for or derive missing task details, create the required Boomi components, deploy them, test them, and inspect the result.

V2 made `build_integration` the high-level entrypoint for component-oriented JSON specs. V3 keeps that execution layer, but adds the missing authoring layer above it:

```text
user task or migration description
  -> archetype selection
  -> open task parameters
  -> reusable primitives
  -> IntegrationSpecV1
  -> component/XML builders
  -> Boomi deployment and test execution
```

The goal is not to make the LLM compose Boomi shapes by hand. The goal is to give the LLM a small catalog of integration architectures it can select and parameterize.

## 2. Core Decisions

1. JSON remains the external MCP contract. No YAML input path is introduced.
2. XML remains an internal compiler and transport detail for Boomi component APIs.
3. `IntegrationSpecV1` remains the canonical execution spec consumed by `build_integration`.
4. New authoring tools sit above `build_integration` and emit `IntegrationSpecV1`.
5. Archetypes define closed architecture and open task slots.
6. Archetypes do not ship content templates: no canned SQL, OData filters, SOAP envelopes, REST payloads, field mappings, or Groovy snippets.
7. Raw component/XML tools remain available only as escape hatches for unsupported cases.
8. The first implementation path is depth-first: make `database_to_api_sync` work end-to-end before broadening the catalog.

## 3. Current Baseline

The repo already has useful lower-level plumbing:

- Component discovery and metadata query tools.
- Folder, runtime, environment, schedule, listener, deployment, execution, monitoring, and troubleshooting tools.
- `build_integration(plan|apply|verify)` with dependency ordering, conflict policy, build registry, and `$ref:<key>` dependency resolution.
- Process XML builders and shape templates for a subset of common shapes.
- An HTTP connector settings builder.

The main gap is authoring:

- `IntegrationSpecV1` is component-oriented and requires callers to know the process/component structure.
- Most connector types do not have JSON builders.
- Profiles, maps, connector actions, database operations, SOAP/OData operations, and many process shapes are not first-class builders.
- There is no pattern catalog or workflow that tells the LLM how to turn a business request into a safe Boomi design.
- Deployment after `build_integration apply` still requires multiple manual tool calls.

## 4. Architecture Layers

### L0: User Task

Input is a natural-language task or migration description. Examples:

- "Sync active users from an on-prem SQL database to a REST API every 15 minutes."
- "Migrate this Mule flow that reads Graph users and posts them to a customer API."
- "Create an event-based endpoint that receives a webhook and inserts records into a database."

The LLM may receive exact task values from the user, such as SQL query, OpenAPI operation, WSDL operation, field mapping, schedule, endpoint, and credential reference. If required values are missing, the LLM asks the user or uses future discovery tools.

### L1: Archetype Catalog

An archetype is a named integration architecture. It describes when to use the pattern, what parameters it needs, what Boomi primitives it composes, and what `IntegrationSpecV1` it emits.

Planned tools:

- `list_integration_archetypes(query?: str, tags?: list[str])`
- `get_integration_archetype(name: str)`

`list_integration_archetypes` is the LLM's pattern search surface. `get_integration_archetype` returns the machine-readable parameter schema, documentation, examples, and constraints for one archetype.

### L2: Archetype Parameterization

The LLM fills the selected archetype's open parameters with task-specific values.

Planned tool:

- `build_from_archetype(name: str, parameters: dict)`

This tool validates parameters and returns a canonical `IntegrationSpecV1`. It does not call Boomi directly unless an explicit future action adds that behavior.

### L3: Reusable Primitives

Primitives are reusable building blocks used by archetypes. They are not usually exposed to end users as standalone tools in V1, but they are documented and tested because archetypes depend on them.

Primitive categories:

- Source: DB extract, REST fetch, OData fetch, SOAP fetch, HTTP listener receive.
- Transform: normalize profile, map fields, XML/JSON conversion, named script slot.
- Target: REST send, DB insert/upsert, SOAP send.
- Operations: schedule, watermark, retry, DLQ, error classifier, run metadata.

Each primitive owns a small Boomi subprocess or component group and has a clear input/output contract.

### L4: IntegrationSpecV1 Execution Spec

`IntegrationSpecV1` remains the normalized execution format for `build_integration`. It contains ordered component operations, dependency keys, and component-specific config.

Archetypes emit `IntegrationSpecV1`; agents should not normally author it directly except for advanced or unsupported cases.

### L5: Component Builders and XML Compiler Boundary

Component builders convert structured config into Boomi-compatible payloads:

- JSON-native Boomi endpoints use typed JSON models.
- XML-native Boomi component endpoints use internal XML builders.
- Raw XML is accepted only through explicit escape-hatch tools.

The LLM-facing contract must stay JSON. The internal XML templates remain implementation details.

### L6: Deploy, Test, and Observe

Planned tool:

- `orchestrate_deploy(build_id: str, environment_id: str, runtime_id: str, schedule_override?: dict, run_test?: bool)`

This tool should package, deploy, attach runtime, apply schedule when needed, execute a test run when requested, poll until terminal state, fetch logs, and return a concise deployment/test summary.

## 5. Archetype Contract

An archetype is implemented as a Python class under a future `src/boomi_mcp/patterns/archetypes/` package.

Required metadata:

- `name`: stable machine name, for example `database_to_api_sync`.
- `version`: semantic version for schema evolution.
- `description`: short agent-facing explanation.
- `tags`: searchable labels, for example `database`, `rest`, `scheduled`, `sync`.
- `use_cases`: examples of tasks that fit the archetype.
- `not_for`: examples of tasks that should use another archetype.
- `parameters_model`: Pydantic model class.
- `emit_spec(parameters) -> IntegrationSpecV1`.

Parameter model rules:

- Use structured fields for architecture-level choices, such as source kind, target kind, schedule, retry policy, DLQ policy, and auth reference.
- Use open string/JSON fields for task-specific content, such as SQL, OData filters, SOAP operation inputs, REST payloads, field mappings, and named script snippets.
- Secrets are referenced by opaque `credential_ref` values. Plaintext credentials must not appear in archetype parameters.
- Validation checks shape and required fields. It does not validate that user-authored SQL, OData, SOAP, payload, or mapping content is semantically correct unless a safe parser exists.

`get_integration_archetype(name)` returns:

- metadata
- JSON schema generated from the Pydantic parameter model
- required and optional parameter descriptions
- capability notes
- limitations
- one or more filled task examples marked as examples, not templates

## 6. Anti-Template Principle

Archetypes are structural. They must not include reusable business-content templates.

Forbidden in archetype source:

- canned SQL queries
- canned OData `$select`, `$filter`, or `$expand`
- canned SOAP envelopes
- canned REST request bodies
- canned field mappings
- canned Groovy scripts
- product/version enums such as `elite_3e_v3_10`

Allowed in archetype source:

- slots named `query`, `operation`, `endpoint`, `field_mappings`, `payload_template`, and `script_slots`
- validation for required slots
- safe defaults for architecture behavior, such as retry count, DLQ enabled, logging enabled, and watermark strategy
- Boomi structural scaffolding, such as facade, error handling, retry branch, document properties, and schedule envelope

This lets one archetype handle Elite 3e, Aderant, Microsoft Graph, Dynamics, SAP OData, generic SOAP, and generic REST cases through different parameter values instead of different hardcoded product patterns.

## 7. Initial Archetype Families

### `database_to_api_sync`

Scheduled or manually triggered source database extraction into an API target.

Typical flow:

```text
schedule/manual start
  -> DB extract
  -> normalize source result
  -> field mapping / transform
  -> API send
  -> retry/error classifier
  -> DLQ/logging
  -> watermark update
```

Initial target protocol: REST.

Later adapters: SOAP and OData targets where required.

### `api_to_database_sync`

Scheduled API extraction into a database target.

Typical sources:

- REST endpoint with optional pagination.
- OData endpoint with `$metadata`, paging, and task-authored query options.
- SOAP operation from WSDL-derived operation details.

Typical target: database insert/upsert operation.

### `api_to_api_sync`

Scheduled or manually triggered API-to-API flow with optional transform.

Initial protocol path: REST source to REST target.

Later protocol adapters:

- REST to SOAP
- SOAP to REST
- OData to REST
- REST to OData where applicable

### Listener/Event Archetypes

Event-triggered flows where Boomi receives an inbound request or listener event and sends data to a database or API.

Initial variants:

- HTTP listener to DB
- HTTP listener to REST API

Later variants:

- AS2 or partner inbound to DB/API
- queue/listener sources if required by real tasks

## 8. Primitive Contract

A primitive is implemented as a Python class under a future `src/boomi_mcp/patterns/primitives/` package.

Required metadata:

- `name`
- `version`
- `input_contract`
- `output_contract`
- `required_builders`
- `emit_components(context, parameters) -> list[IntegrationComponentSpec]`

Primitive rules:

- A primitive may emit one or more related Boomi components.
- A primitive must declare the profile/shape contract it hands to the next primitive.
- A primitive should not depend on another integration's shared subprocess in V1.
- Per-build subprocess copies are the default to avoid cross-build coupling and simplify rollback.
- Shared-library subprocesses are a future optimization only.

Initial primitive set:

- `db_extract`
- `rest_fetch`
- `odata_fetch`
- `soap_fetch`
- `http_listener_receive`
- `normalize_profile`
- `field_map`
- `xml_json_convert`
- `rest_send_with_retry`
- `db_upsert`
- `soap_send`
- `schedule_envelope`
- `watermark_state`
- `error_classifier`
- `dlq_writer`
- `run_metadata`

## 9. Tool Workflows

### Build from task

```text
1. LLM reads user task.
2. LLM calls list_integration_archetypes(query=...).
3. LLM calls get_integration_archetype(name=...).
4. LLM fills parameters from the task.
5. If required values are missing, LLM asks the user or uses future discovery tools.
6. LLM calls build_from_archetype(name, parameters).
7. LLM calls build_integration(action="plan", config=spec).
8. LLM reviews plan and calls build_integration(action="apply", dry_run=false).
9. LLM calls build_integration(action="verify", build_id=...).
10. LLM calls orchestrate_deploy(...) when implemented.
```

### Migration from another integration tool

```text
1. LLM parses source tool description/export.
2. LLM identifies source, target, trigger, transform, error handling, and deployment requirements.
3. LLM selects the closest Boomi archetype.
4. LLM maps source-tool concepts into archetype parameters.
5. LLM asks for missing credentials, schemas, endpoint details, and deployment target.
6. Normal build workflow continues.
```

### Escape hatch

If no archetype fits, the LLM may fall back to direct `IntegrationSpecV1` authoring or raw component tools. The response must explain why the archetype path is not sufficient.

## 10. Discovery Strategy

Discovery tools are not required for the first vertical slice, but the design must leave space for them.

Future read-only tools:

- `discover_openapi_spec(url, auth_ref?)`
- `discover_soap_wsdl(url, auth_ref?)`
- `discover_odata_metadata(base_url, auth_ref?)`
- `discover_db_schema(connection_ref, schema_filter?)`

Rules:

- Discovery tools are read-only.
- They return schema/spec information for the LLM to author open task slots.
- They do not create SQL, payloads, mappings, or scripts automatically.
- DB discovery should use Boomi-side execution or another safe mechanism; the MCP host should not require direct JDBC access to customer databases.

## 11. Error and Validation Model

New tools should return the existing success envelope plus structured errors:

```json
{
  "_success": false,
  "error_code": "PARAM_VALIDATION_FAILED",
  "error": "source.query is required",
  "suggestion": "Provide a SQL query or run discovery before building.",
  "retryable": true,
  "context": {}
}
```

Initial error codes:

- `ARCHETYPE_NOT_FOUND`
- `PARAM_VALIDATION_FAILED`
- `UNSUPPORTED_PROTOCOL`
- `BUILDER_NOT_AVAILABLE`
- `DEPENDENCY_UNRESOLVED`
- `COMPONENT_EXISTS`
- `BOOMI_API_ERROR`
- `DEPLOY_TIMEOUT`
- `TEST_EXECUTION_FAILED`

Validation layers:

1. Archetype parameter validation.
2. Primitive input/output contract validation.
3. `IntegrationSpecV1` validation.
4. `build_integration plan` dependency and route validation.
5. Live Boomi validation during apply, deploy, and test.

## 12. Compatibility

Existing tools remain available:

- `manage_process`
- `manage_component`
- `manage_connector`
- `query_components`
- `build_integration`
- deployment, execution, schedule, listener, monitoring, and troubleshooting tools

V3 does not remove `IntegrationSpecV1`. It reduces how often agents need to author it directly.

The legacy XML builder model is now internal implementation detail. Its previous standalone training README is archived because it encouraged shape-level authoring as the primary LLM path.

## 13. Acceptance Criteria for V3 Implementation

A completed implementation of this design must support the following end-to-end story:

1. User asks for a scheduled database-to-REST sync and provides connection references, SQL query, endpoint, mapping, schedule, and deployment target.
2. LLM discovers `database_to_api_sync`.
3. LLM obtains the parameter schema.
4. LLM fills open parameters without writing raw Boomi XML.
5. MCP emits `IntegrationSpecV1`.
6. MCP plans, applies, verifies, deploys, and test-runs the integration.
7. MCP returns deployment status, execution status, and relevant logs.
8. Failure path writes failed records to DLQ or returns enough detail for the LLM to explain the issue.

## 14. Related Roadmap

Implementation epics are maintained in `docs/INTEGRATION_AUTHORING_ROADMAP.md`.

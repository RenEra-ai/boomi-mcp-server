# Boomi MCP Component Creation Design - V3

Status: active
Version: 3.0
Last updated: 2026-06-04
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
7. Transformation compiles through native Boomi rungs first: direct map, map function, map script, then process-level script only when explicitly requested.
8. Raw component/XML tools remain available only as escape hatches for unsupported cases.
9. The first implementation path is depth-first: make `database_to_api_sync` work end-to-end before broadening the catalog.

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
- Transform: normalize profile, map fields, map functions, map scripts, XML/JSON conversion, named process-level script slot.
- Target: REST send, DB insert/upsert, SOAP send.
- Operations: schedule, watermark, retry, DLQ, error classifier, run metadata.

Each primitive owns a small Boomi subprocess or component group and has a clear input/output contract.

Transformation compiler policy:

- The LLM-facing source of truth is structured transform intent in archetype parameters and `IntegrationSpecV1`, not Boomi visual map XML or Groovy source.
- The compiler uses the least powerful Boomi-native rung that can represent the requested transform:
  1. direct field-to-field map
  2. `transform.function` for standard per-field operations such as date format, default value, string operations, simple lookup, sequential value, and math
  3. `script.mapping` for in-map scripted transformations
  4. `script.processing` only when the caller explicitly asks for process-level document manipulation
- Unsupported transform intent fails before apply with field-level errors. It must not silently fall back to process-level Groovy.
- XSLT is explicitly out of M2 for `database_to_api_sync`. The M2 transform ladder (direct map, `transform.function`, `script.mapping`) covers DB-to-REST payload construction without an XSLT rung. `operation_type='xslt'` is rejected before mutation with a structured pointer to issue #42, and direct/function/script schemas reject `xslt`/`xslt_source` keys as unsupported routes; this validation must remain in place.
- XSLT support is reconsidered only when a concrete trigger appears: XML-heavy migration, SOAP/XML-to-XML target shape, an unknown XML/JSON structure where Boomi's XSLT Stylesheet component is the right tool, or imported integration assets that already ship XSLT stylesheets. The decision rule is "real source artifact present," not "caller asks for XSLT."
- Any future XSLT work would land as a dedicated `xslt` component builder plus Data Process step integration in the process compiler, owned by a later milestone (likely M5 API variants, M7 discovery, or a separate XML-heavy migration issue). It will not be implemented as a `transform.map` fallback, and no canned or migrated stylesheet bodies will ship as templates — stylesheet payloads must be caller-authored, migrated, or discovered.
- Profile fields are generated from explicit schema contracts or discovery output. For M2, the supported sources are caller-declared DB read output fields and caller-supplied JSON schema/profile intent; browse/introspection and sample inference are discovery work.
- Existing component updates preserve unknown Component XML via read-merge-write (issue #45, shipped). `build_integration action='update'` for every builder-routed component (database/REST connectors and operations, `profile.db`/`profile.json`/`profile.xml`, `transform.map`/`script.mapping`/`transform.function`, and `process` with `process_kind`) fetches the current live XML, replaces only the builder-owned subtrees from the freshly-built desired XML, and pushes the merged result via `update_component_raw`. Each builder declares a `PRESERVATION_POLICY` whose `owned_paths` enumerate the XML subtrees it owns; `bns:encryptedValues` entries (existing isSet=true secret slots survive — desired-side entries are only added when their `@path` is new), `bns:processOverrides`, unknown root attributes, and any unknown children under `<bns:object>` are preserved. Plan output exposes `update_mode` (`read_merge_write` vs. `full_xml_replace`), `preserves_unknown_xml`, `owned_paths`, and `preserved_paths` per step. Raw-XML escape hatches via `manage_component`/`manage_connector` with `config.xml` remain explicit full-XML replacement (no preservation). Type/subType mismatches, malformed XML on either side, and missing owned subtrees raise structured `UPDATE_PRESERVATION_*` errors before any mutation. Merge granularity per owned subtree: connector bodies (`DatabaseConnectionSettings`, `DatabaseGetAction`) use `subtree_merge` (owned attrs + named child blocks updated, unknown attrs/children preserved); REST `GenericConnectionConfig`/`GenericOperationConfig` use field-id `key_merge` with coupled profile-type attrs. **Known gap (follow-up):** the transform/profile/process owned cores (`<Map>`, `<process>`, profile `DataElements`, `MappingScript`, `Function`) still use wholesale `replace`, so unknown attrs/children *inside* those objects are not yet preserved — tracked as "inner-object preservation hardening" (#50, which also covers REST operation conditional emission for profile-binding attrs so type-only updates apply). This is speculative future-proofing (builders are byte-locked to real exports; no unknown inner fields exist today).
- Agents review transformations through structured surfaces such as field lists, mapping diffs, unmapped-field validation, test payloads, and expected/actual comparison.

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

### Semantic sync pipeline foundation (M5 direction)

From M5 onward, integration authoring is framed as **presets over reusable semantic stages**, not as source/destination-pair templates. The layering is: preset selection → pipeline stages → reusable primitives → `IntegrationSpecV1`. The stable abstraction is stage semantics (read, fetch, lookup, map, send, write, finalize); connector direction is preset metadata, not the architecture.

- A **preset** (for example `database_to_api_sync`) is a thin adapter that selects a stage graph. Presets differ by the stages and primitives they choose, not by duplicated per-pair process XML.
- `sync_pipeline` is the **internal** process-builder kind that compiles a verified linear stage graph. It is not a public archetype name unless deliberately exposed later.
- `database_to_api_sync` stays the public preset/archetype name and a backward-compatible compatibility adapter over `sync_pipeline`. `IntegrationSpecV1` is not replaced.
- Each stage declares its execution semantics — cardinality, context effect, side effect, and failure behavior — so the validator checks correctness before any process XML or component planning runs.
- **Audit/provenance is opt-in metadata in v1**, not a mandatory always-on shell. Try/Catch + DLQ emission shipped as R1a (#51, closed) for `reliability.retry_count == 0` with `dlq.mode` in `{document_cache_ref, error_subprocess_ref}`, emitting verified Boomi Try/Catch/DLQ shapes compiled from live-exported reference XML, with a plan-time DLQ `$ref` type check (`PROCESS_REF_TYPE_MISMATCH`); `retry_count > 0` stays gated by `PROCESS_RETRY_UNVERIFIED` and end-to-end runtime failure-row proof remains blocked at M3/#9 closeout. Branch and Process Call behavior stay gated until their Boomi XML and live behavior are verified (see `docs/INTEGRATION_AUTHORING_ROADMAP.md` M5).

API/database variants (`api_to_api_sync`, `api_to_database_sync`) are added as thin presets over this foundation once the REST fetch source and database write primitives exist, rather than as independent pairwise archetypes.

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

- typed slots that callers populate without canned content: `source.read_operation.sql` (caller-authored read statement), `source.read_operation.result_schema` (caller-declared DB result fields), `target.send_request.method` / `target.send_request.path`, `target.payload_profile` (caller-supplied JSON profile tree), and `transform.operations` (discriminated typed operations: `direct` / `map_function` / `map_script`)
- validation for required slots and cross-field references (e.g. transform operations must reference declared source fields and target leaf paths)
- safe defaults for architecture behavior, such as retry count, DLQ enabled, logging enabled, and watermark strategy
- Boomi structural scaffolding, such as facade, error handling, retry branch, document properties, and schedule envelope

This lets one archetype handle Elite 3e, Aderant, Microsoft Graph, Dynamics, SAP OData, generic SOAP, and generic REST cases through different parameter values instead of different hardcoded product patterns.

Presets extend this principle to connector pairs: they compose the same reusable stages and primitives and must not duplicate per-pair process XML or ship per-pair content templates. A new connector pair is a new parameter set over the shared `sync_pipeline` foundation, not a new hardcoded builder.

## 7. Initial Archetype Families

`database_to_api_sync` is the reference preset. The API/database variants below are **thin presets over the shared `sync_pipeline` foundation** (see §4), reusing the `rest_fetch` and `db_write` primitives — not independent pairwise builders.

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

DB extract supports two `profile.db` Read profile variants (M2.3, Issue #23):

- `profile_type="database.read"` — caller-authored Select SQL via `query`.
- `profile_type="database.stored_procedure_read"` — invoke a stored procedure
  via `procedure_name`. Parameters carry `mode` direction (`in` / `out` /
  `in_out` / `return`; at most one `return` per statement).

Both variants are referenced by the same `connector-action database.get`
operation via `read_profile_id` (UUID or `$ref:KEY` token), so an archetype
can swap one for the other without changing the rest of the spec.

### `api_to_database_sync`

Thin preset over `sync_pipeline` (REST fetch → transform → database write), gated on the `rest_fetch` source primitive and `db_write` operation support.

Scheduled API extraction into a database target.

Typical sources:

- REST endpoint with optional pagination.
- OData endpoint with `$metadata`, paging, and task-authored query options.
- SOAP operation from WSDL-derived operation details.

Typical target: database insert/upsert operation.

### `api_to_api_sync`

Thin preset over `sync_pipeline` (REST fetch → transform → REST send), gated on the `rest_fetch` source primitive.

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
- `map_function_transform`
- `map_script_transform`
- `xml_json_convert`
- `rest_send_with_retry`
- `db_write`
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
- `infer_profile_fields(source_type, artifact, options?)`
- `import_existing_integration(artifact_type, artifact, context?)`

Rules:

- Discovery tools are read-only.
- They return schema/spec information for the LLM to author open task slots.
- They can produce profile-field contracts and migration-oriented `IntegrationSpecV1` drafts for review.
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

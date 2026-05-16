# Integration Authoring Roadmap

Status: active
Last updated: 2026-05-15
Parent design: `docs/MCP_TOOL_DESIGN.md`

## Summary

This roadmap implements V3 component creation in depth-first order. The first goal is a complete `database_to_api_sync` path that an LLM can use without raw XML or manual Boomi UI work. Later milestones broaden protocol coverage, listener/event flows, discovery, and composition.

Each milestone should be implemented as one project epic with smaller issues under it. A milestone is not complete until handler tests, MCP-surface tests, and the documented validation path pass.

## Roadmap Dates

Dates assume one main implementer, code review time, and live Boomi QA buffers. Repository milestones are the source of truth for due dates.

| Milestone | Start | Due | Status |
|---|---:|---:|---|
| M0 Docs Alignment | 2026-05-18 | 2026-05-18 | Done after docs/date commit is pushed |
| M1 Archetype Framework Skeleton | 2026-05-18 | 2026-05-29 | Done 2026-05-16 (sub-issues #15–#20 closed; 361 tests passing) |
| M2 `database_to_api_sync` Vertical Slice | 2026-06-01 | 2026-06-26 | Depends on M1 |
| M3 Deploy and Test Orchestration | 2026-06-29 | 2026-07-10 | Depends on M2 apply path |
| M4 Agent Ergonomics | 2026-07-13 | 2026-07-24 | Depends on M1/M2 tool surface |
| M5 API Variants | 2026-07-27 | 2026-08-21 | Depends on M2/M4 |
| M6 Event and Listener Variants | 2026-08-24 | 2026-09-11 | Depends on M3/M5 |
| M7 Discovery Tools | 2026-09-14 | 2026-10-02 | Depends on core archetypes |
| M8 Archetype Composition | 2026-10-05 | 2026-10-23 | Depends on at least 3 stable archetypes |

## M0: Docs Alignment

Goal: make the V3 design the active project direction.

Implementation focus:

- Archive superseded V2 design and legacy XML-builder training README.
- Replace `docs/MCP_TOOL_DESIGN.md` with V3 archetype-based design.
- Add this roadmap.
- Update `.gitignore` so active docs are tracked while bulk local archive/history remains ignored.

Exit criteria:

- Active design doc says V3 is authoritative.
- Active docs describe archetypes, primitives, `IntegrationSpecV1`, anti-template rules, and planned tools.
- Legacy V2 and XML-builder docs are preserved under `docs/archive/`.
- `git diff --check` passes.

Validation:

- `rg` over active docs does not find stale claims that V2 is authoritative.
- `rg` over active docs does not present YAML as an MCP input path.
- `rg` over active docs does not describe shape-by-shape LLM authoring as the primary path.

## M1: Archetype Framework Skeleton

Status: Done 2026-05-16 — sub-issues #15–#20 closed; `list_integration_archetypes`, `get_integration_archetype`, `build_from_archetype` registered; stub `StubMinimalIntegrationArchetype` emits a safe `IntegrationSpecV1`; 361 tests passing on `origin/dev`.

Goal: add the framework before adding real integration patterns.

Implementation focus:

- Add pattern package with base classes for archetypes and primitives.
- Add registry discovery for archetype and primitive classes.
- Add Pydantic parameter schema exposure.
- Add planned tools:
  - `list_integration_archetypes`
  - `get_integration_archetype`
  - `build_from_archetype`
- Add one stub archetype that validates parameters and emits a minimal safe `IntegrationSpecV1` fixture, but does not attempt a real Boomi deployment.

Exit criteria:

- MCP clients can list archetypes, fetch a schema, validate parameters, and receive a normalized spec from the stub archetype.
- Invalid parameters return structured errors with field paths and suggestions.
- No raw XML is exposed through the archetype API.

Validation:

- Handler tests for registry discovery, schema generation, validation success, validation failure, and spec emission.
- MCP wrapper tests for all three new tools.
- Existing `build_integration` tests still pass.

## M2: First Vertical Slice, `database_to_api_sync`

Goal: build the first real archetype end-to-end for scheduled database extraction into a REST API target.

Implementation focus:

- Add database connector/settings builder support needed for SQL Server first, with extension points for Postgres and Oracle.
- Add database operation/action builder support sufficient to run a task-authored query.
- Add minimum process-shape support required for facade, retry, error handling, and subprocess calls.
- Add profile/map support required by the vertical slice:
  - source result normalization
  - simple field-to-field mapping
  - XML/JSON conversion when required by Boomi execution
- Add primitives:
  - `db_extract`
  - `field_map`
  - `xml_json_convert`
  - `rest_send_with_retry`
  - `schedule_envelope`
  - `watermark_state`
  - `error_classifier`
  - `dlq_writer`
  - `run_metadata`
- Add the `database_to_api_sync` archetype.

Exit criteria:

- Given parameters for DB credential reference, SQL query, REST endpoint, mapping, schedule, retry/DLQ policy, and naming, the archetype emits a complete `IntegrationSpecV1`.
- `build_integration plan` produces deterministic component order.
- `build_integration apply` creates the expected component set in a test Boomi account.
- The emitted design includes error handling, retry behavior, run metadata, and DLQ behavior by default.

Validation:

- Golden-spec tests for each primitive and for the full archetype.
- Builder tests for emitted connector/process/profile/map XML or JSON payloads.
- MCP tests for `build_from_archetype("database_to_api_sync", ...)`.
- Live Boomi QA: create, deploy when M3 exists, run test, fetch logs, and exercise a failure row.

## M3: Deploy and Test Orchestration

Goal: remove the post-apply manual chain for agents.

Implementation focus:

- Add `orchestrate_deploy(build_id, environment_id, runtime_id, schedule_override?, run_test?)`.
- Use existing deployment, runtime attachment, schedule, execution, and monitoring modules.
- Return a single summary with package IDs, deployment IDs, runtime attachment result, schedule result, execution ID, terminal status, and log excerpts.
- Add cleanup helper behavior for failed builds only if it can be dry-run by default.

Exit criteria:

- An agent can move from `build_integration apply` to deployed/tested integration with one tool call.
- The tool is idempotent enough to retry safely when package/deployment already exists.
- Failed deployment or failed test execution returns structured error codes and diagnostic context.

Validation:

- Handler tests with mocked Boomi services for success, deploy failure, attach failure, schedule failure, test timeout, and log retrieval failure.
- MCP wrapper test for response shape.
- Live Boomi QA against the M2 archetype output.

## M4: Agent Ergonomics

Goal: make the authoring flow self-discoverable for a cold LLM client.

Implementation focus:

- Add structured error taxonomy across new authoring tools.
- Add workflow hints to `list_capabilities`.
- Extend `get_schema_template` or add equivalent schema access for:
  - `IntegrationSpecV1`
  - each archetype parameter model
  - planned workflow sequences
- Add filled examples under `examples/` for representative tasks. These are examples, not templates, and must be labeled that way.
- Add concise server instructions that route component creation tasks through archetype tools first.

Exit criteria:

- A new LLM session can discover the recommended build workflow without reading repository source.
- Errors are actionable and branchable by `error_code`.
- Examples cannot be mistaken for reusable SQL, mapping, or payload templates.

Validation:

- Unit tests for schema/template output.
- MCP tests for `list_capabilities` workflow block.
- `rg` over examples confirms anti-template labeling.

## M5: API Variants

Goal: broaden from DB-to-REST into the main scheduled sync cases.

Implementation focus:

- Add `api_to_database_sync`.
- Add `api_to_api_sync`.
- Add REST fetch and pagination support.
- Add OData source adapter and `odata_fetch` primitive.
- Add SOAP source/target adapter support where required by a real task.
- Keep task-specific OData filters, SOAP operation inputs, REST payloads, and mappings as open parameters.

Exit criteria:

- REST-to-DB, REST-to-REST, OData-to-REST, and one SOAP-involved scenario can emit valid `IntegrationSpecV1`.
- Protocol-specific builders are introduced only where required by these archetypes.
- No product-specific archetype forks are introduced for Elite 3e, Aderant, Microsoft Graph, Dynamics, or similar systems.

Validation:

- Golden-spec tests for each new archetype.
- Builder tests for REST, OData, and SOAP payload emission.
- Live Boomi QA for at least one REST-to-DB and one API-to-API flow.

## M6: Event and Listener Variants

Goal: support event-based integrations after scheduled syncs are stable.

Implementation focus:

- Add HTTP listener source primitive.
- Add listener-based archetypes:
  - HTTP listener to DB
  - HTTP listener to REST API
- Add inbound validation primitive.
- Add listener deployment/start/verify workflow integration.

Exit criteria:

- LLM can build a listener-triggered integration without hand-authoring listener components.
- Deployment orchestration can verify listener configuration and status.
- Error handling and DLQ behavior match scheduled archetypes where applicable.

Validation:

- Golden-spec tests for listener primitives and archetypes.
- MCP tests for listener archetype schema and validation.
- Live Boomi QA with an inbound test payload.

## M7: Discovery Tools

Goal: help the LLM author open task slots when the user did not provide enough schema/spec detail.

Implementation focus:

- Add read-only discovery tools:
  - `discover_openapi_spec`
  - `discover_soap_wsdl`
  - `discover_odata_metadata`
  - `discover_db_schema`
- Return structured schema/spec summaries suitable for LLM reasoning.
- Keep discovery separate from archetype building. Discovery suggests possible values but does not create hidden templates.

Exit criteria:

- The LLM can inspect source/target schema information before filling archetype parameters.
- Discovery tools do not mutate Boomi or customer systems.
- DB discovery does not require direct customer JDBC access from the MCP host.

Validation:

- Handler tests for successful parse, auth failure, invalid spec, unreachable endpoint, and truncation.
- MCP tests for response shape and read-only annotations.
- Manual validation against representative OpenAPI, WSDL, OData, and DB schema sources.

## M8: Archetype Composition

Goal: support larger integrations by composing stable standalone archetypes.

Implementation focus:

- Add `compose_archetypes(parts)` after at least three standalone archetypes are proven.
- Add document handoff primitive for linking parts.
- Add validation that output contracts from one part match input contracts of the next.
- Add at least one composed flow, such as DB source -> transform stage -> multi-target API fanout.

Exit criteria:

- Composition emits one coherent `IntegrationSpecV1`.
- Invalid contract links fail before any Boomi mutation.
- Composed output can still be deployed and tested through normal orchestration.

Validation:

- Contract validation tests for compatible and incompatible links.
- Golden-spec test for one composed integration.
- Live Boomi QA for the composed example.

## Cross-Cutting Rules

- Prefer vertical slices driven by real archetypes over broad speculative connector matrices.
- Do not add product/version-specific archetypes for schema variance. Use open task parameters and discovery.
- Keep credentials as opaque references. Never put plaintext secrets in examples, logs, specs, or test fixtures.
- Every new authoring tool must have handler tests and MCP wrapper tests.
- Every real archetype must have golden-spec tests and at least one live Boomi QA scenario before being called complete.
- Raw XML remains an escape hatch, not the normal LLM authoring interface.

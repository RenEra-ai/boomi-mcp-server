# Integration Authoring Roadmap

Status: active
Last updated: 2026-06-05
Parent design: `docs/MCP_TOOL_DESIGN.md`

## Summary

This roadmap implements V3 component creation in depth-first order. The first goal is a complete `database_to_api_sync` path that an LLM can use without raw XML or manual Boomi UI work. Later milestones broaden protocol coverage, listener/event flows, discovery, and composition.

Each milestone should be implemented as one project epic with smaller issues under it. A milestone is not complete until handler tests, MCP-surface tests, and the documented validation path pass.

## Roadmap Dates

Dates assume one main implementer, code review time, and live Boomi QA buffers. Repository milestones are the source of truth for due dates. This schedule was accelerated again on 2026-05-30 after M2 completed on 2026-05-30, thirteen days ahead of its prior 2026-06-12 due date. The new cadence keeps short QA buffers but removes the old multi-week gaps: M3 and M4 are each one focused week, M5 keeps a two-week window because it introduces new archetype direction and DB-write support, and M6-M8 each get one focused week after their prerequisites land. On 2026-06-05 the remaining schedule was shifted to start M3 on Monday 2026-06-08, placing each later milestone on a clean Monday-Friday week (M5 spanning two weeks); M0-M2 dates are historical and unchanged.

| Milestone | Start | Due | Status |
|---|---:|---:|---|
| M0 Docs Alignment | 2026-05-15 | 2026-05-15 | Done 2026-05-15 |
| M1 Archetype Framework Skeleton | 2026-05-15 | 2026-05-16 | Done 2026-05-16 (sub-issues #15-#20 closed; 361 tests passing) |
| M2 `database_to_api_sync` Vertical Slice | 2026-05-18 | 2026-05-30 | Done 2026-05-30 (#21-#31, #40-#46, #49, and #30 closed; parent #8 closed) |
| M3 Deploy and Test Orchestration | 2026-06-08 | 2026-06-12 | Done 2026-06-08 (#60-#66 closed; #51 R1a shipped; parent #9 closed by documented evidence) |
| M4 Agent Ergonomics | 2026-06-15 | 2026-06-19 | Depends on M1/M2 tool surface and M3 workflow handoff |
| M5 API Variants | 2026-06-22 | 2026-07-03 | Depends on M2/M4; semantic sync_pipeline foundation first, then API/DB presets (M5.0–M5.9 under #11; includes #32, #50) |
| M6 Event and Listener Variants | 2026-07-06 | 2026-07-10 | Depends on M3/M5 |
| M7 Discovery Tools | 2026-07-13 | 2026-07-17 | Depends on core archetypes; #47/#48 already staged |
| M8 Archetype Composition | 2026-07-20 | 2026-07-24 | Depends on the semantic sync_pipeline foundation, at least 2 stable presets, plus deploy/test orchestration |

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

Status: Done 2026-05-30 — parent #8 closed; all M2 sub-issues closed. The shipped path covers archetype emission, plan/dry-run/mocked apply/verify, transformation review, read-merge-write update preservation, cross-component `$ref` validation, and the tracked M2 live QA runbook. Runtime deployment, schedule activation, execution, execution-log retrieval, and verified retry/DLQ process emission moved to M3.

Goal: build the first real archetype end-to-end for scheduled database extraction into a REST API target.

Implementation focus:

- Add database connector/settings builder support needed for SQL Server first, with extension points for Postgres and Oracle.
- Add database operation/action builder support sufficient to run a task-authored query.
  - M2.3 (Issue #23, shipped 2026-05-18): `DatabaseReadProfileBuilder` for Select-statement Read profiles (`profile_type="database.read"`) + `DatabaseGetOperationBuilder` for Get operations.
  - M2.3 follow-up (shipped 2026-05-18): `DatabaseStoredProcedureReadProfileBuilder` for Stored Procedure Read profiles (`profile_type="database.stored_procedure_read"`). Same Get operation builder references either profile flavor by ID. Extended supported field/parameter types from `character`-only to `character`+`number`+`datetime` (verified against live SP profile XML).
- Add minimum process-shape support required for facade, retry, error handling, and subprocess calls.
- Add profile/map support required by the vertical slice:
  - source result normalization
  - direct field-to-field mapping
  - XML/JSON conversion when required by Boomi execution
  - M2.6 baseline (Issue #26): direct profile/map/conversion only
  - M2.6a (Issue #40): `transform.function` library for date format, default value, string ops, simple lookup, sequential value, and math
  - M2.6b (Issue #41): `script.mapping` as the in-map escape hatch
  - M2.6c (Issue #42): XSLT support decision recorded — XSLT is explicitly out of M2 for `database_to_api_sync`.
    - Rationale: M2 targets DB-to-REST payload construction, and the shipped transform ladder (direct map #26, `transform.function` #40, `script.mapping` #41) already covers field-to-field, standard per-field operations, and in-map custom logic without an XSLT rung. There is no DB-to-REST scenario in M2 that direct/function/script cannot express.
    - Current validation (must remain in place): `operation_type='xslt'` in `database_to_api_sync` fails before mutation with a `PARAM_VALIDATION_FAILED` error pointing at issue #42; `transform.map` direct/function/script schemas reject `xslt`/`xslt_source` keys as unsupported M2 routes; no XSLT component is emitted by `build_integration` plan or apply.
    - Reopen triggers (do not implement until at least one is present): XML-heavy migration with real source artifacts, SOAP/XML-to-XML target shape, unknown XML/JSON structure where Boomi's XSLT Stylesheet component is the right native tool, or imported integration assets that already ship XSLT stylesheets.
    - Likely future placement: a dedicated issue under M5 (API variants), M7 (discovery / import existing integration), or a separate XML-heavy migration milestone — not M2. Implementation will be an `xslt` component builder plus Data Process step integration in the process compiler, not a `transform.map` fallback. The anti-template rule applies: stylesheet bodies must be caller-authored, migrated, or discovered; canned XSLT will never ship as a template.
  - M2.6d (Issue #43): profile field generation for DB read fields and task-supplied JSON schema/profile intent; metadata/sample/XSD/XML inference is deferred to M7 Issue #47
  - M2.1a (Issue #44): amend `database_to_api_sync` with explicit source schema, target schema, and typed transform intent before M2.6/M2.7 consume the contract
  - M2.6d (Issue #45, shipped): Component XML read-merge-write preservation for `build_integration action='update'` across builder-generated database/REST connectors and operations, `profile.db`/`profile.json`/`profile.xml`, `transform.map`/`script.mapping`/`transform.function`, and `process` (`database_to_api_sync`). Each structured builder declares a `PRESERVATION_POLICY` listing the XML subtrees it owns; the apply path fetches current live XML via `component_get_xml`, merges only the builder-owned subtrees from the freshly-built desired XML, and pushes the merged XML via `update_component_raw`. `bns:encryptedValues` entries (existing isSet=true secret slots), `bns:processOverrides`, unknown root attributes, and unknown `<bns:object>` siblings survive the update. Raw-XML escape hatches on `manage_component`/`manage_connector` remain explicit full-XML replacement. Plan output surfaces `update_mode`, `preserves_unknown_xml`, `owned_paths`, and `preserved_paths` per step so callers can audit preservation behavior before applying. Connector bodies (`DatabaseConnectionSettings`, `DatabaseGetAction`) merge granularly (`subtree_merge`: owned attrs + named child blocks, unknown attrs/children preserved) and REST operation profile types travel with their bindings. Known follow-up: the transform/profile/process owned cores (`<Map>`, `<process>`, profile `DataElements`, `MappingScript`, `Function`) still wholesale-`replace`, so unknown attrs/children *inside* those objects aren't yet preserved — tracked as "inner-object preservation hardening" (#50, which also covers REST operation conditional emission for profile-binding attrs). Speculative; no unknown inner fields exist in builder-locked exports today.
  - M2.x (Issue #46): MCP transformation review surface for field lists, mapping diffs, unmapped validation, test payloads, and expected/actual comparison
  - M2.10 (Issue #30, shipped 2026-05-30): End-to-end validation + live QA for the `database_to_api_sync` vertical slice. Adds a focused local E2E module (`tests/patterns/test_database_to_api_sync_e2e.py`) and `build_integration` wrapper tests (`tests/test_build_integration_wrapper.py`) that prove the full path — `build_from_archetype` → `review_transformation` → `build_integration` plan / dry-run apply / mocked real apply / verify — without live Boomi: dry-run apply never calls `_execute_component`, mocked apply resolves every `$ref` before execution, `reference_only` connections record `status='reused'` without creation, and unresolvable plans fail fast. Ships a tracked live QA runbook (`docs/M2_DATABASE_TO_API_SYNC_LIVE_QA.md`). M2 validation thus covers local end-to-end assembly **plus** Boomi component create/apply/verify readiness. Package deploy, runtime attachment, schedule activation, runtime execution, execution-log retrieval, and failure-row runtime proof remain M3/#51 scope.
- Use the transformation escalation ladder:
  - direct map first
  - `transform.function` for supported standard operations
  - `script.mapping` for in-map custom logic
  - `script.processing` only when explicitly requested for process-level document manipulation
- Closed M2.1-M2.3 review disposition, 2026-05-23:
  - No immediate code changes are required for the already shipped M2.2/M2.3 builders.
  - Issue #21 remains closed; Issue #44 is the contract amendment before M2.6/M2.7 consume typed transformation and profile-schema fields.
  - Issue #22 remains valid for SQL Server connector/settings emission; full-replacement Component update preservation is tracked separately in Issue #45.
  - Issue #23 result-profile binding is treated as a deferred component reference; the builder validates `$ref` dependencies and apply resolves them before execution.
- Add primitives:
  - `db_extract`
  - `field_map`
  - `map_function_transform`
  - `map_script_transform`
  - `xml_json_convert`
  - `rest_send_with_retry`
  - `schedule_envelope`
  - `watermark_state`
  - `error_classifier`
  - `dlq_writer`
  - `run_metadata`
- Add the `database_to_api_sync` archetype.

Exit criteria:

- Given parameters for DB credential reference, SQL query, REST endpoint, source/target schema, mapping, schedule, retry/DLQ policy, and naming, the archetype emits a complete `IntegrationSpecV1`.
- `build_integration plan` produces deterministic component order.
- `build_integration apply` creates the expected component set in a test Boomi account.
- The emitted design includes error handling, retry behavior, run metadata, and DLQ behavior by default.
- Transform compilation is explicit and reviewable before apply; unsupported transforms fail before mutation instead of silently falling back to process-level Groovy.

Validation:

- Golden-spec tests for each primitive and for the full archetype.
- Builder tests for emitted connector/process/profile/map/function/script XML or JSON payloads.
- MCP tests for `build_from_archetype("database_to_api_sync", ...)`.
- Transformation review tests for field listing, mapping diff, unmapped validation, test payload generation, and expected/actual comparison.
- Live Boomi QA: create, deploy when M3 exists, run test, fetch logs, and exercise a failure row.

## M3: Deploy and Test Orchestration

Status: Done 2026-06-08. `orchestrate_deploy` is built and public (#60-#66 closed); exit criteria 1-3 are satisfied by #66 live QA, and criterion 4 is resolved via the permitted blocked-with-recorded-evidence path. #51 shipped R1a Try/Catch + DLQ emitter support (`retry_count == 0`); R1b (`retry_count > 0`) remains gated under `PROCESS_RETRY_UNVERIFIED`. Parent #9 is split into #60-#66. Reliability follow-up #51 is also assigned to M3 because runtime failure proof requires verified Try/Catch/DLQ behavior, but #51 is not a child of #9.

Goal: remove the post-apply manual chain for agents.

Implementation focus:

- Add `orchestrate_deploy(build_id, environment_id, runtime_id, schedule_override?, run_test?)`.
- Use existing deployment, runtime attachment, schedule, execution, and monitoring modules.
- Return a single summary with package IDs, deployment IDs, runtime attachment result, schedule result, execution ID, terminal status, and log excerpts.
- Add cleanup helper behavior for failed builds only if it can be dry-run by default.
- Complete verified Try/Catch/DLQ process emission (#51): shipped as R1a for `retry_count == 0` so emission no longer stops at `PROCESS_RETRY_UNVERIFIED` for that slice; `retry_count > 0` (R1b) remains gated under `PROCESS_RETRY_UNVERIFIED`.
- Follow the issue split:
  - #60 — `orchestrate_deploy` contract and build-result resolver
  - #61 — idempotent package creation and deployment stage
  - #62 — runtime attachment and schedule activation stage
  - #63 — optional test execution, polling, and log summary stage
  - #64 — public `orchestrate_deploy` MCP tool and discoverability wiring
  - #65 — failure/idempotency hardening and cleanup planning
  - #66 — live QA against the M2 archetype output
  - #51 — verified Try/Catch + DLQ process emission follow-up

Exit criteria:

- An agent can move from `build_integration apply` to deployed/tested integration with one tool call.
- The tool is idempotent enough to retry safely when package/deployment already exists.
- Failed deployment or failed test execution returns structured error codes and diagnostic context.
- Retry/DLQ process behavior needed for failure-row proof is either verified live through #51 or explicitly blocked with recorded Boomi evidence.

Closeout (2026-06-08): criteria 1-3 satisfied (#66); criterion 4 = blocked-with-recorded-evidence accepted — see `docs/M3_ORCHESTRATE_DEPLOY_LIVE_QA.md` "Epic #9 Closeout Evidence".

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

## M5: API Variants Over a Semantic Sync Pipeline

Status: Scheduled 2026-06-22 to 2026-07-03. Parent #11 is the milestone epic. M5 builds a reusable semantic `sync_pipeline` foundation first, then adds API/database variants as thin presets over it — not as new source/destination-pair archetypes.

Goal: add a reusable semantic pipeline layer, connector-operation primitives, and backward-compatible presets for API/database variants. The stable abstraction is stage semantics (read, fetch, lookup, map, send, write, finalize); connector direction is preset metadata, not the architecture.

Ordering (pipeline foundation before variants):

1. M5.0 — Update design docs and issue DAG for the semantic sync pipeline.
2. M5.1 — Add the PipelineSpec stage model and semantic validator (internal stage graph; no new Boomi XML; `IntegrationSpecV1` stays backward-compatible).
3. M5.2 — Add the `sync_pipeline` process builder for verified linear flows.
4. M5.3 — Preserve `database_to_api_sync` as a compatibility adapter over `sync_pipeline` (no public behavior change; no mandatory audit/reliability shell injected into legacy output).
5. M5.4 — Add the REST fetch/source primitive.
6. M5.5 (#50) — Complete inner-object preservation hardening and REST conditional emission.
7. M5.6 (#32) — Add database Send/write operation and write profile support (component-level only, not a full preset).
8. M5.7 — Add the `api_to_api_sync` preset (REST fetch → transform → REST send).
9. M5.8 — Add the `api_to_database_sync` preset (REST fetch → transform → database write).
10. M5.9 — Live QA for the new `sync_pipeline` presets via live MCP calls and deploy/test orchestration when M3 is available.

New child issues M5.0–M5.4 and M5.7–M5.9 are tracked under parent #11; existing issues #50 and #32 are reordered into the sequence as M5.5 and M5.6.

Constraints:

- `database_to_api_sync` stays backward-compatible; `sync_pipeline` is internal process-builder vocabulary, not a public archetype name.
- Audit/provenance is opt-in metadata in v1, not a mandatory always-on shell.
- Try/Catch + DLQ emission for `retry_count == 0` is verified and shipped via #51 (R1a); Branch, standalone Process Call, and `retry_count > 0` behavior stay gated until their Boomi XML and live behavior are verified through #51 R1b (or a follow-up live-XML issue).
- Keep task-specific OData filters, SOAP operation inputs, REST payloads, and field mappings as open parameters. No canned SQL, payloads, maps, SOAP envelopes, OData filters, or raw Boomi XML templates.
- Expand primitives before presets: `rest_fetch` before `api_to_api_sync`; `db_write` (#32) before `api_to_database_sync`.
- OData and SOAP source/target adapters are follow-on primitives over the same pipeline contract, added after the REST presets are validated — not as separate pairwise archetypes.

Exit criteria:

- A `sync_pipeline` builder emits the same linear shape currently used for `database_to_api_sync`, and existing `database_to_api_sync` tests stay green.
- `api_to_api_sync` and `api_to_database_sync` are implemented as thin presets that map to `sync_pipeline` stages, inspectable through existing MCP planning/review flows.
- No raw XML or canned payload templates are exposed; no product-specific archetype forks (Elite 3e, Aderant, Microsoft Graph, Dynamics, or similar) are introduced.
- Try/Catch + DLQ for `retry_count == 0` is verified/shipped via #51 (R1a); `retry_count > 0` remains gated unless verified live through #51 (R1b).

Validation:

- Validator unit tests reject duplicate stage keys, unknown refs, cycles, invalid side-effect ordering, and unsupported failure modes.
- Builder tests cover linear `sync_pipeline` emission and `database_to_api_sync` adapter equivalence; the R1a Try/Catch + DLQ catch-path (`retry_count == 0`) is covered by golden tests, while `retry_count > 0`/Branch/standalone Process Call paths fail closed until verified.
- Live Boomi QA for at least one REST-to-REST and one REST-to-DB flow once the presets exist; deploy/test QA uses `orchestrate_deploy` when M3 is complete.

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
  - `infer_profile_fields` for DB metadata, sample JSON, XSD, and sample XML (Issue #47)
  - `import_existing_integration` for migration artifacts/descriptions to pipeline drafts / preset parameters and `IntegrationSpecV1` drafts (Issue #48) — imports yield pipeline drafts and preset parameters, not source/destination-pair templates
- Return structured schema/spec summaries suitable for LLM reasoning.
- Keep discovery separate from archetype building. Discovery suggests possible values but does not create hidden templates.

Exit criteria:

- The LLM can inspect source/target schema information before filling archetype parameters.
- Existing integration artifacts or descriptions can be converted into reviewable pipeline drafts / preset parameters (`IntegrationSpecV1`) before normal build workflow.
- Discovery tools do not mutate Boomi or customer systems.
- DB discovery does not require direct customer JDBC access from the MCP host.

Validation:

- Handler tests for successful parse, auth failure, invalid spec, unreachable endpoint, and truncation.
- MCP tests for response shape and read-only annotations.
- Manual validation against representative OpenAPI, WSDL, OData, and DB schema sources.

## M8: Archetype Composition

Goal: support larger integrations by composing stable presets/archetypes after the semantic `sync_pipeline` foundation exists — advanced composition covering branching, fanout, subprocesses, and complex orchestration.

Implementation focus:

- Add `compose_archetypes(parts)` after at least two standalone presets are proven over the semantic `sync_pipeline` foundation.
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

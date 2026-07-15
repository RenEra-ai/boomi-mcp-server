# M12 Compatibility Inventory

**Status:** evolving — this file is the M12 migration ledger and is UPDATED as M12 issues land
(unlike [ADR-001](ADR-001-process-ir-authority.md), which is immutable once accepted).
**Measured baseline:** 2026-07-13, extended with review-round re-measurements dated inline as
2026-07-14 (issue #135, epic #134). Every `file:line` below was read and verified against the
checkout on the stated date; line numbers drift as the codebase moves — re-verify before relying
on them in a later issue.
**References:** epic [#134](https://github.com/RenEra-ai/boomi-mcp-server/issues/134),
issue [#135](https://github.com/RenEra-ai/boomi-mcp-server/issues/135),
[ADR-001: Process IR Authority and Compiler Boundary](ADR-001-process-ir-authority.md).

This inventory records, per authoring surface: its single authority status (matching the ADR-001
authority table), what it accepts today, whether it is public and/or executable, its concrete
readers/writers, its validation/lowering owner, defaults and aliases, measured unknown-field
behavior, existing error codes, its fixture/test coverage with honest assertion strength, its
adapter issue, and the migration gate that must close before its behavior may change.

**Checkout scope.** Per the repository convention (`.gitignore`: `docs/*` is local-only *except*
`docs/architecture/`, established by commit `56ae84e "chore: exclude docs folder from remote repo"`),
[ADR-001](ADR-001-process-ir-authority.md) and this inventory are the **checkout-authoritative** M12
records and cite only tracked sources (`src/…`, `tests/…`, `examples/…`). The M12 refreshes to
`docs/INTEGRATION_AUTHORING_ROADMAP.md` and `docs/MCP_TOOL_DESIGN.md` land in those local design docs,
which remain outside the tracked checkout by that convention — so this inventory grounds every claim in
tracked code/tests rather than in the local docs.

**The load-bearing structural fact** (the ADR crux, measured): `IntegrationSpecV1.pipeline` and
`main_process.config.pipeline` are two DISTINCT surfaces that are not wired to each other. The
spec-level field is write-only/inspection-only (zero source readers); the nested process-config
key is the real authoring-to-XML channel via `SyncPipelineBuilder.lower_config`.

---

## 1. Surface inventory

### 1.1 `IntegrationSpecV1.pipeline` (top-level spec field)

| Field | Measured state |
|---|---|
| Authority status (ADR-001) | Derived inspectable/analysis view |
| Current acceptance | Optional `PipelineSpec` on the spec envelope; validated strictly when present, `None` by default (`src/boomi_mcp/models/integration_models.py:90-97`) |
| Public / executable | Public (part of the pydantic spec schema); **NOT executable** — no Boomi XML is emitted from this field alone |
| Writers | 4 archetypes set it "so the plan is inspectable": `src/boomi_mcp/patterns/archetypes/api_to_api_sync.py:1681`, `api_to_database_sync.py:856`, `http_listener_to_db.py:1092`, `http_listener_to_rest.py:525`. Deliberate non-writer: `database_to_api_sync.py:2884-2885` (its internal adapter keeps `pipeline=None`) |
| Readers | **Source: NONE.** No `.py` under `src/` reads `spec.pipeline` to drive behavior. Test readers only: `tests/patterns/test_database_to_api_sync_assembly.py:357`, `test_api_to_api_sync_e2e.py:307-314`, `test_api_to_database_sync_e2e.py:291-293`, `test_stub_archetype.py:76`, `tests/test_pipeline_models.py:406-419` |
| Validation / lowering owner | Pydantic `PipelineSpec` validation only (`src/boomi_mcp/models/pipeline_models.py:218-231`); **no lowering path exists from this field** |
| Defaults & aliases | Default `None`; a `model_dump()` of the spec expands every default — per stage `component_ref: null` plus the four semantic metadata keys (`cardinality`/`context_effect`/`side_effect`/`failure_behavior`) as `null` (`pipeline_models.py:186-203`), per dependency `edge_kind: "ordering"`, `label: null`, `ordinal: null` (`:163-172`) — while a compact nested `config.pipeline` dict stays byte-identical (full expanded dump pinned by the freeze suite) |
| Unknown-field behavior | Spec envelope drops unknowns silently (§2.1); the `PipelineSpec` value itself rejects extras (§2.4) |
| Error codes | Pydantic `ValidationError` (no builder codes — nothing consumes it) |
| Fixtures / tests | The 4 pattern e2e tests above; #135 freeze suite `tests/test_issue_135_compatibility_freeze.py` |
| Assertion strength | Structural (dict/None comparisons); no XML coverage possible (emits nothing) |
| Adapter issue | #139 (M12.4 legacy adapters and golden parity) — must become a compiler-derived summary **for a single-process spec** (authored values checked by derived equality or rejected with `LEGACY_ADAPTER_AUTHORITY_CONFLICT`, never precedence); a **zero-process** spec's authored pipeline is preserved as a frozen inert value and a **multi-process** one is rejected as ambiguous (ADR §5) |
| Migration gate | Today the executable nested pipeline **wins silently** when the two disagree (nothing reconciles them — §2.5). #139 closes this; until then the freeze test pins the silent-precedence baseline |

### 1.2 `main_process.config.pipeline` (process-config dict `"pipeline"` key)

| Field | Measured state |
|---|---|
| Authority status (ADR-001) | Compatibility input through the linear adapter |
| Current acceptance | A `PipelineSpec`-shaped dict inside a process component's free-form `config`, required when `process_kind="sync_pipeline"`; only the verified-linear all-`ordering` subset lowers |
| Public / executable | Public (documented via `get_schema_template`, `src/boomi_mcp/categories/meta_tools.py:7955-8065`); **executable — THE authoring→XML channel** for pipeline-style configs |
| Writers | `patterns/archetypes/http_listener_to_db.py:747` (shared `_build_listener_main_process`, also imported by `http_listener_to_rest.py:75`; pipeline dicts built at `http_listener_to_db.py:1054` / `http_listener_to_rest.py:482`), `api_to_api_sync.py:1157`, `api_to_database_sync.py:519`, `database_to_api_sync.py:2893` (internal-only `_build_sync_pipeline_adapter_config`, lowered immediately, never surfaced onto the spec) |
| Readers | `src/boomi_mcp/categories/components/builders/process_flow_builder.py:6864` (`SyncPipelineBuilder.lower_config` — the lowering path), missing-pipeline errors `:6865-6871`; WSS listener detection: `src/boomi_mcp/categories/deployment/orchestration.py:781` and `src/boomi_mcp/categories/integration_builder.py:1893`; plan-time lowering + ref-type re-check `integration_builder.py:5908-5940` |
| Validation / lowering owner | `SyncPipelineBuilder.lower_config` (`process_flow_builder.py:6819`) — validates and lowers to a `database_to_api_sync` config in one pass |
| Defaults & aliases | Free-form dict, so no envelope defaults of its own; on validation the `PipelineSpec` model applies `edge_kind="ordering"` default (`pipeline_models.py:167-171`) and `None` stage metadata |
| Unknown-field behavior | The dict survives the spec envelope verbatim (§2.2); on lowering, extras inside the pipeline are rejected by `PipelineSpec(extra="forbid")` (§2.4); around it, the sync_pipeline top-level allowlist governs (§2.6) |
| Error codes | `SYNC_PIPELINE_CONFIG_INVALID`, `SYNC_PIPELINE_CONTROL_FLOW_UNSUPPORTED`, `SYNC_PIPELINE_STAGE_UNSUPPORTED` (see 1.3) |
| Fixtures / tests | `tests/test_sync_pipeline_builder.py` (77 tests), `tests/test_integration_builder.py:6967,7161,7256,8190,8327`, `tests/test_process_flow_builder_listener.py`, `tests/test_builder_xml_invariants.py:1277`, `tests/test_schema_template_process_flow.py:602,656`, pattern e2e suites |
| Assertion strength | Lowered-dict equality + differential XML equality vs `ProcessFlowBuilder.build` (see §3 — **no committed sync_pipeline golden**) |
| Adapter issue | #139 (the linear `sync_pipeline` adapter must produce a ProcessIRV1 root); lowering contract ownership moves under #137 |
| Migration gate | Golden parity in #139: adapter output must match today's lowered `database_to_api_sync` config and XML before any rerouting |

### 1.3 `sync_pipeline` (process kind)

| Field | Measured state |
|---|---|
| Authority status (ADR-001) | Compatibility input through the linear adapter (this process kind IS the linear adapter) |
| Current acceptance | `process_kind="sync_pipeline"` + `pipeline` stage graph; lowers ONLY `read(db_read) \| fetch(rest_fetch\|soap_fetch) \| listener(wss_listen) → [map] → send(rest_send\|soap_send) \| write(db_write)` (`_SYNC_PIPELINE_SUPPORTED_KINDS`, `process_flow_builder.py:6686-6688`); all other declared `PipelineStageKind` values (`pipeline_models.py:69-130`) are reserved and rejected |
| Public / executable | Internal builder vocabulary: `sync_pipeline` is a `process_kind` (`SyncPipelineBuilder.PROCESS_KIND`, `process_flow_builder.py:6816`), **not** one of the public archetype names returned by `list_integration_archetypes` — yet it is reachable through `build_integration` process config and documented by `get_schema_template` (`meta_tools.py:7955-8065`); executable |
| Writers | Archetypes emit it: `api_to_api_sync.py:1156,1688`; `api_to_database_sync.py:519,863`; `http_listener_to_db.py:746,1099`; `http_listener_to_rest.py:532`; `database_to_api_sync.py:2892` (internal adapter). Primitives feeding stages: `patterns/primitives/soap_send.py`, `soap_fetch.py`, `wss_listen.py` |
| Readers (routing/detection) | `integration_builder.py:5908-5940` routes `SyncPipelineBuilder` configs through `lower_config` then re-runs ref-type + lineage checks on the lowered config; `orchestration.py:775,888` recognizes the sync_pipeline `listener` stage |
| Validation / lowering owner | `SyncPipelineBuilder` (`process_flow_builder.py:6816` `PROCESS_KIND`, `:6819` `lower_config`, `:7453` `validate_config`, `:7495` `build`) |
| Defaults & aliases | `process_kind` / `process_type` both accepted (both in the top-level allowlist `:6764-6776`; the base builder reads `process_kind or process_type`, `:642`); per-kind primitive defaults `_SYNC_PIPELINE_STAGE_PRIMITIVE` (`:6692-6700`) with SOAP alternates (`:6705-6708`) |
| Unknown-field behavior | Fail-closed at both levels — see §2.6 |
| Error codes | `SYNC_PIPELINE_CONFIG_INVALID` (unknown top-level key, bad pipeline, non-control-flow gated blocks — `:6839-6862`), `SYNC_PIPELINE_CONTROL_FLOW_UNSUPPORTED` (`branch`/`process_calls` gated keys `:6844-6847`, non-`ordering` edges `:6890-6900`, non-linear chains `:7015-7020`), `SYNC_PIPELINE_STAGE_UNSUPPORTED` (reserved stage kinds `:6905-6915`) |
| Fixtures / tests | `tests/test_sync_pipeline_builder.py` (dedicated, 77 tests); plus the reader suites in 1.2 |
| Assertion strength | Differential: `lower_config` output equals the hand-written `database_to_api_sync` core dict (`tests/test_sync_pipeline_builder.py:151-189`) and emitted XML is byte-identical to `ProcessFlowBuilder.build` of that core (`:191-195`). **No committed `sync_pipeline_*.xml` golden exists** (§3) |
| Adapter issue | #139 (adapter + golden parity); compile contracts #137 |
| Migration gate | #139 must add golden parity fixtures for the lowered surface before the adapter is rerouted through ProcessIRV1; the reject-don't-drop allowlists must be preserved verbatim (legacy codes stay stable per ADR-001) |

### 1.4 `flow_sequence` (recursive process-builder config key)

| Field | Measured state |
|---|---|
| Authority status (ADR-001) | Compatibility input and semantic seed for ProcessIRV1 |
| Current acceptance | Ordered list of one or more typed step objects composing M10/M11 shapes in one `database_to_api_sync` process (a single-step sequence is accepted — measured 2026-07-14, pinned by the freeze suite); control kinds (decision/branch) and the exception terminal must be the LAST step of their sequence; branch legs are linear sub-flows; decision legs may nest one branch/exception level |
| Public / executable | Public (capability catalog + schema docs: `meta_tools.py:7298-7329` field list, `:7401,7507-7522` schema + error taxonomy); executable |
| Writers | `src/boomi_mcp/patterns/composition.py:852` (compose_archetypes rewrites parts onto the Branch surface), `patterns/primitives/document_cache_put.py:116` and `document_cache_lookup.py:118` (fragments carrying a `process_config.flow_sequence` step) |
| Readers | Dispatch: `process_flow_builder.py:655-669` (validate) and `:848-852` (build) route to the composed path when present (`_flow_sequence_enabled` `:4616`, reads `:4624`). Ref-type checks: `integration_builder.py:2391` calling `_check_flow_sequence_ref_types` (def `:2501`, recursive `:2566+`). Lineage pass gate: `integration_builder.py:5964-5968`; cache/property lineage walk `src/boomi_mcp/categories/components/builders/cache_property_lineage.py:336-360` |
| Validation / lowering owner | `_validate_flow_sequence_config` (`process_flow_builder.py:4642`), `_validate_flow_sequence_steps` (`:4777`), `_validate_flow_sequence_step` (`:4865`); emitter `_emit_composed_flow_shapes` (`:5709`) |
| Defaults & aliases | Per-kind key allowlists `_FLOW_SEQUENCE_STEP_KEYS` (`:289`), allowed kinds `_FLOW_SEQUENCE_ALLOWED_KINDS` (`:283`); `label` optional per step; legacy kind aliases exist inside the vocabulary (e.g. `doccacheload` treated like `cache_put` in the consume-stream guards `:4756,4839`) |
| Unknown-field behavior | Fail-closed at step level, **lenient at the surrounding config root** — see §2.7 |
| Error codes | `PROCESS_FLOW_SEQUENCE_CONFIG_INVALID` (unknown kind `:4874-4881`, extra step keys `:4882-4890`, legacy-sibling blocks `:4627-4639`, ordering/terminal violations); `PROCESS_LINEAGE_BRANCH_ORDER_INVALID` from the lineage pass (pinned by `tests/test_m11_composed_examples.py:144`) |
| Fixtures / tests | 5 committed goldens (§3), `tests/test_process_flow_builder.py` flow_sequence sections, `tests/test_m11_composed_examples.py`, `tests/test_builder_xml_invariants.py` (structural invariants) |
| Assertion strength | Raw-byte golden equality for the 5 flow_sequence goldens (§3) plus structural ET assertions |
| Adapter issue | **#136** (M12.1 — promote flow_sequence into strict ProcessIRV1 models); semantic validation unification #143 |
| Migration gate | #136 owns closing the permissive config root (§2.7) via strict models — never silently tightened here; step-level codes stay stable until #139's adapter mapping review |

### 1.5 `wrapper_subprocess` (process kind)

| Field | Measured state |
|---|---|
| Authority status (ADR-001) | Compatibility input through named adapters/recipes |
| Current acceptance | `process_kind="wrapper_subprocess"` + non-empty `process_calls` list (each entry exactly one of `subprocess_ref="$ref:KEY"` / `process_id`); optional `process_extensions`, `return_documents`, `description` |
| Public / executable | Public (schema docs `meta_tools.py:7838-7937`); executable (start → processcall(s) → stop/return) |
| Writers | Caller-authored configs; QA fixture provisioning `scripts/provision_qa_noop_fixture.py:15,24,27` |
| Readers (plan-time synthesis + ref checks) | `integration_builder.py:523` `_synthesize_wrapper_subprocess_edges` (called `:5103`), `:672` `_synthesize_wrapper_subprocess_extensions` (called `:5108`), `:2680` `_check_wrapper_subprocess_ref_types` (called from the `:5905` region) |
| Validation / lowering owner | `WrapperSubprocessBuilder` (`process_flow_builder.py:6534` `PROCESS_KIND`, `:6537` `validate_config`, `:6613` `build`); per-entry checks `_validate_processcall_entry` (`:6420`) |
| Defaults & aliases | `wait` defaults `true`, `abort_on_error` defaults `false` at emit (`:6647-6648`); `process_kind`/`process_type` alias (`:6543-6545`) |
| Unknown-field behavior | **Permissive at root and per-call entry** — accepted and ignored, subject only to the secret scan — see §2.8 |
| Error codes | `PROCESS_KIND_UNSUPPORTED` (`:6549`), `PROCESS_REF_MISSING` (`:6557` and entry-level), `PROCESS_REF_AMBIGUOUS` (`:6445`), `PROCESS_CALL_CONFIG_INVALID` (`:6487,6498`), `PLAINTEXT_SECRET_REJECTED` (inherited scan, `:564` in `scan_forbidden_secret_fields` `:509`) |
| Fixtures / tests | Dedicated: `tests/test_wrapper_subprocess_builder.py` (golden `processcall_standalone_parent.xml`), `tests/test_wrapper_subprocess_extensions_hoist.py`; plus `test_integration_builder.py`, `test_process_flow_builder.py:1338-2091`, `test_schema_template_process_flow.py`, `test_design_doctrine.py` |
| Assertion strength | **Canonicalized** XML equality (`ET.canonicalize`, `tests/test_wrapper_subprocess_builder.py:82`) — not raw-byte; plus structural shape/wiring assertions |
| Adapter issue | #139 (named adapter over Process Call semantics) |
| Migration gate | The accepted-and-ignored root/call extras (§2.8) are a gate to close in #139 — the adapter must map or reject them explicitly, never silently tighten; `PLAINTEXT_SECRET_REJECTED` stays stable |

### 1.6 Primitive `emit_fragment`

| Field | Measured state |
|---|---|
| Authority status (ADR-001) | Internal legacy compatibility contribution |
| Current acceptance | Free-form dict returned per primitive; base contract returns `{}` (`src/boomi_mcp/patterns/base.py:172`, issue #28) |
| Public / executable | Internal only — consumed inside archetype assembly; no MCP-facing schema |
| Writers (overrides) | ~21 primitives under `src/boomi_mcp/patterns/primitives/`: `soap_send.py:132`, `soap_fetch.py:135`, `rest_fetch.py:554`, `rest_send.py:370`, `return_documents.py:108`, `inbound_validate.py:145`, `data_process.py:225`, `flow_control.py:113`, `wss_listen.py:295`, `throw_exception.py:143`, `branch.py:148`, `decision.py:191`, `document_cache_put.py:98`, `document_cache_lookup.py:99`, `document_cache_retrieve.py:134`, `document_cache_remove.py:131`, and `operational.py:168,284,358,489,565` |
| Readers (explicit invocations) | `patterns/archetypes/http_listener_to_db.py:510`; `database_to_api_sync.py:3090,3113,3212,3228,3250`. **No central dispatch loop** ties `emit_fragment` to spec lowering — it is largely orthogonal to the pipeline/flow_sequence/sync_pipeline surfaces (some fragments carry a `process_config.flow_sequence` step: `document_cache_put.py:116`, `document_cache_lookup.py:118`) |
| Validation / lowering owner | None (convention between primitive and consuming archetype) |
| Defaults & aliases | Base default `{}`; keys defined per primitive by convention |
| Unknown-field behavior | Consumed by convention; **unknown-key preservation is NOT a promised contract** (§2.9) |
| Error codes | None at this boundary |
| Fixtures / tests | `tests/patterns/test_m10_primitive_builder_contract.py`, `tests/patterns/test_primitives_source_transform.py`, per-archetype assembly tests |
| Assertion strength | Structural (fragment-dict and consuming-archetype assertions) |
| Adapter issue | #138 (verified process-emitter registry) with typed contributions in #145 |
| Migration gate | Replacing the convention with a typed contract is #138/#145 scope; the convention must be characterized (this ledger) rather than silently formalized elsewhere |

### 1.7 Auxiliary surfaces

| Surface | Authority status (ADR-001) | Readers / writers (measured) | Owning issue | Notes |
|---|---|---|---|---|
| Archetype / composition inputs (`build_from_archetype`, `compose_archetypes` parts) | Compatibility inputs through named adapters/recipes | Entry points `src/boomi_mcp/categories/integration_authoring.py:164` (`build_from_archetype_action`) and `:247` (`compose_archetypes_action`); composition engine `src/boomi_mcp/patterns/composition.py:878` (`compose_archetypes`), Branch rewrite onto `flow_sequence` `:852` | #139 (adapters) / #145 (typed recipe contributions) | Recipes emit `IntegrationSpecV1` plans; they never emit XML directly |
| Materialization `depends_on` | Authoritative component/materialization plan **only** | Schema `integration_models.py:35` (+ self-dependency validator `:37-43`); topo-sort `integration_builder.py:867` (`_topological_order`), consumed at `:5124`; wrapper edge synthesis appends to it (`:523`) | Unchanged in M12 (verified end-to-end by #147) | The ONE thing `IntegrationSpecV1` stays authoritative for under ADR-001 |
| Cache / property lineage pass | Derived verification/analysis view (internal validation pass; not an authored surface — nearest ADR row is the derived-views row) | `src/boomi_mcp/categories/components/builders/cache_property_lineage.py:336-360` (`_walk_steps` over `config.get("flow_sequence")`); plan-time gates `integration_builder.py:5931-5940` (lowered sync_pipeline map) and `:5964-5968` (composed/legacy configs) | #143 (unify semantic validation on ProcessIRV1) | Emits `PROCESS_LINEAGE_*` codes (e.g. `PROCESS_LINEAGE_BRANCH_ORDER_INVALID`) |
| Verifier output (`process_graph_verifier`) | Derived verification/analysis view | `src/boomi_mcp/categories/components/process_graph_verifier.py:141` (`verify_process_graph`); post-emission consumer of built XML | #138 (emitter registry keeps it as the outer gate) / #146 (verify surface exposure) | Report shape is an output contract, never an input |
| Doctrine views (design doctrine, gotchas, governance prose) | Advisory text | `src/boomi_mcp/kb/design_doctrine.py:232` (`wrapper_subprocess_separation`) + cross-refs; `src/boomi_mcp/kb/operational_gotchas.py:1364,1398` (`applies_to` includes `flow_sequence`) | Stays advisory (docs refresh in #147) | Never validation-bearing per ADR-001 |
| `import_integration_draft.pipeline_draft` | Derived verification/analysis view (analysis-only) | `src/boomi_mcp/categories/integration_import.py:1093` (`_build_pipeline_draft`), attached `:1354-1356`, response key `:1476`; tool docs `meta_tools.py:9665-9669` | #146 (MCP surface updates) | A validated `PipelineSpec` dump describing an EXISTING component — never an executable input |

Remaining ledger dimensions for the auxiliary surfaces (fields that are `n/a` reflect that the
surface is not an authored input; "not measured" entries are explicit gates for the owning issue,
never assumptions):

| Surface | Acceptance / public / executable | Validation / lowering owner | Defaults & aliases | Unknown-field behavior | Errors | Fixtures & assertion strength | Migration gate |
|---|---|---|---|---|---|---|---|
| Archetype / composition inputs | Typed per-archetype/composition parameters; public tools; emit `IntegrationSpecV1` plans, never XML directly | Per-archetype emitters (`patterns/archetypes/*`) and the composition engine (`patterns/composition.py`); entry-point parameter checks in `integration_authoring.py:164/:247` | Archetype side: per-archetype preset parameter defaults (each archetype's schema via `get_schema_template(schema_name="archetype:<name>")`). Composition side (`compose_archetypes`): omitting `options` entirely is NOT supported — the parse-time fallback `CompositionOptions(naming={})` (`composition.py:897-898`) then fails downstream validation because `naming.integration_name`/`component_prefix` are required, so the public action returns `PARAM_VALIDATION_FAILED` (measured live 2026-07-14); omitted `links` → inferred v1 star topology, `db_source → transform → each rest_target`, all `document_stream` (`:255-260`); omitted `execution` → `{"trigger": {"mode": "manual"}}` (`:666`) | Not measured in this baseline — measure before adapter rework | Per-tool validation errors — not inventoried here; measuring them is an explicit #139/#145 gate | `examples/m8/` JSON round-trips (structural, `tests/patterns/test_archetype_composition.py:346-371,692-717`); archetype XML via `try_catch_*` goldens (§3) | #139/#145 must freeze parameter acceptance before rerouting through ProcessIRV1 |
| Materialization `depends_on` | List of in-spec component keys; public spec field; drives topo-sorted execution order | Model validator (`integration_models.py:37-43`); topo-sort + plan checks in `integration_builder.py` (`_topological_order` `:867`, consumed `:5124`) | Default `[]`; alias `dependencies` → `depends_on` (§1.8) | n/a (list of strings). Self-dependency rejected at the model (`integration_models.py:37-43`); a dangling key hard-fails the plan (`_success: false`, "depends on unknown component" — measured 2026-07-14) | Model `ValueError` (self-dep); plan-level error (dangling key); `MISSING_PROCESS_DEPENDENCY` for unresolvable `$ref:KEY` values (raise sites `process_flow_builder.py:6398,6410`) | Exercised structurally throughout `tests/test_integration_builder.py` (topo-sort, wrapper edge synthesis) | Unchanged in M12; #147 re-verifies end-to-end |
| Cache / property lineage pass | n/a — internal plan-time validation pass, not authored, not public | n/a — it IS a validation pass (`cache_property_lineage.py`), not a validated surface | n/a | n/a | `PROCESS_LINEAGE_*` family | `tests/test_cache_property_lineage.py` (dedicated, structural) | #143 absorbs it into ProcessIRV1 semantic validation |
| Verifier output | n/a — post-emission report, never an input | n/a | n/a | n/a | Report entries (its own report shape, not `BuilderValidationError`) | `tests/test_process_graph_verifier.py` (dedicated, structural) | #138 keeps it as the outer gate; #146 exposes the verify surface |
| Doctrine views | n/a — advisory text; never accepted as input | n/a | n/a | n/a | None (never validation-bearing) | `tests/test_design_doctrine.py`, `tests/test_doctrine_emitter_consistency.py` (structural consistency) | Stays advisory; docs refresh in #147 |
| `import_integration_draft.pipeline_draft` | Read-only tool output; public; never accepted as input by any tool (zero consumers measured) | n/a — produced by `_build_pipeline_draft` (`integration_import.py:1093`), standalone migration analysis | n/a | n/a (output, not input) | n/a | `tests/test_integration_import.py`, `tests/test_integration_import_wrapper.py` (structural) | Analysis-only derived view of standalone migration analysis (no compiler involvement today); #146 exposes/documents its surface |

Shared plan/builder error machinery (applies across the process surfaces above, owned by the
shared validation passes rather than any single surface): cross-component `$ref` type checks
(`PROCESS_REF_TYPE_MISMATCH`, `MISSING_PROCESS_DEPENDENCY`), connector binding validation
(`PROCESS_CONNECTOR_BINDING_INVALID`), process naming (`PROCESS_NAME_REQUIRED`,
`PROCESS_NAME_CONFLICT`), the plaintext-secret scan (`PLAINTEXT_SECRET_REJECTED`), and the
lineage pass (`PROCESS_LINEAGE_*`). The per-tool taxonomies surfaced by
`get_schema_template`/`list_capabilities` (`src/boomi_mcp/categories/meta_tools.py`) cover these
families EXCEPT `PROCESS_LINEAGE_*`: its five codes (`PROCESS_LINEAGE_AMBIGUOUS_LAST_WRITE`,
`PROCESS_LINEAGE_BRANCH_ORDER_INVALID`, `PROCESS_LINEAGE_CACHE_WRITER_MISSING`,
`PROCESS_LINEAGE_DDP_SCOPE_INVALID`, `PROCESS_LINEAGE_PROPERTY_READ_BEFORE_WRITE`) are defined in
`src/boomi_mcp/categories/components/builders/profile_generation.py:133-137` and raised only by
the lineage pass (`cache_property_lineage.py`); they are NOT published by `meta_tools.py`
(measured 2026-07-14). This ledger does not duplicate the published taxonomies —
`LEGACY_ADAPTER_*` (#139) must map each family explicitly before any adapter rewires these
surfaces, sourcing the lineage family from `profile_generation.py`.

### 1.8 Component-envelope aliases (`_normalize_component`)

`_normalize_component` (`src/boomi_mcp/categories/integration_builder.py:291-351`) applies these
aliases/promotions to every component dict before `IntegrationSpecV1` validation:

- **`spec` → `config`**: when `config` is absent, `raw.get("spec", {})` is used (`:305-307`).
- **`dependencies` → `depends_on`**: when `depends_on` is absent, `raw.get("dependencies", [])`
  is used (`:311-313`).
- **`config.name` promotion + whitespace strip**: a missing top-level `name` is promoted from
  `config.name`; BOTH surfaces are stripped so collision lookup, `PROCESS_NAME_CONFLICT`, and
  emitted XML see one canonical value (`:317-341`).
- **`type` aliasing**: `type`/`component_type` accepted; value normalized via `_TYPE_ALIASES`
  (`:286-288,295-300`).
- Missing `key` falls back to `name` then `component_{index}` (`:295`); `action` defaults to
  `"create"` (`:301`).

---

## 2. Measured unknown-field boundary behavior

Every claim below was verified by reading the cited code on 2026-07-13; entries added during
the #135 review rounds carry their own inline measurement date (2026-07-14).

### 2.1 `IntegrationSpecV1` / `IntegrationComponentSpec` — extras silently ignored

Both are plain `BaseModel`s with **no `model_config`** (`src/boomi_mcp/models/integration_models.py:12`
for `IntegrationComponentSpec`, `:46` for `IntegrationSpecV1`), so pydantic's default
`extra="ignore"` applies: unknown top-level fields are **silently dropped** and absent from
`model_dump()` output. Defaults: `version="1.0"` (`:49`), `mode="lift_shift"` (`:51`), empty
lists/dicts for `components/goals/endpoints/flows/naming/folders/runtime/validation_rules`
(`:52-73`), `profile_indexes_by_component_id=None` (`:74`), `pipeline=None` (`:90`).

### 2.2 Nested `config` / `naming` / `folders` / `runtime` dicts — preserved verbatim

`IntegrationComponentSpec.config` is `Dict[str, Any]` (`integration_models.py:20`), and
`naming`/`folders`/`runtime`/`validation_rules` are likewise free-form (`:56-73`): anything inside
these dicts is **preserved verbatim and never schema-validated** at the model layer. This is why
`process_kind`, `pipeline`, and `flow_sequence` live free-form inside `config`.

### 2.3 `_normalize_to_spec` — only `config.integration_spec.pipeline` survives

`_normalize_to_spec` (`src/boomi_mcp/categories/integration_builder.py:354-416`, called from
`_build_plan` at `:5094`) handles three input shapes:

1. **`config.integration_spec` present** (`:360-362`): the payload dict is copied straight into
   `IntegrationSpecV1(**spec_data)` (`:405,416`) — a `pipeline` key **survives** and is validated
   as `PipelineSpec`.
2. **`source_description` is a dict** (`:363-381`): `spec_payload` is rebuilt from an explicit key
   allowlist (`name/mode/components/goals/endpoints/flows/naming/folders/runtime/validation_rules/`
   `profile_indexes_by_component_id`) that **omits `pipeline`** — a `source_description.pipeline`
   is silently dropped.
3. **Flat top-level** (`:382-400`): same allowlist rebuild from `config` — a top-level `pipeline`
   is silently dropped. A **string** `source_description` becomes `goals=[text]` (`:387`).

### 2.4 `PipelineSpec` / `StageSpec` / `PipelineEdgeSpec` — extras rejected, stage config open

All three declare `model_config = ConfigDict(extra="forbid")`
(`src/boomi_mcp/models/pipeline_models.py:225` / `:186` / `:163`): unknown/extra fields raise
`ValidationError`. Exception: `StageSpec.config` is `Dict[str, Any]` (`:190`) — the free-form
stage payload passes through untouched; only the stage **envelope** is strict.

Net: the spec envelope is lenient (drops unknowns), the pipeline graph envelope is strict
(rejects unknowns), and both keep an escape-hatch `config` dict that passes through unchanged.

### 2.5 Top-level pipeline is inert and may disagree with the executable nested pipeline

No code reconciles `spec.pipeline` with `main_process.config.pipeline`. A spec carrying a
spec-level `fetch→send` pipeline and a nested `read→send` config pipeline validates and plans;
lowering reads ONLY the nested dict (`process_flow_builder.py:6864`). The executable nested
pipeline **wins silently** — this is the measured baseline #139 must replace with derived
equality or `LEGACY_ADAPTER_AUTHORITY_CONFLICT` (per ADR-001), never precedence.

### 2.6 `sync_pipeline` — fail-closed allowlists at both levels

- Top-level allowlist `_SYNC_PIPELINE_ALLOWED_TOP_LEVEL` (`process_flow_builder.py:6764-6776`):
  `process_kind/process_type/pipeline/description/folder_name/process_extensions/name/`
  `component_type/component_name`. `folder_id` is deliberately excluded.
- Gated blocks with tailored hints `_SYNC_PIPELINE_GATED_TOP_LEVEL` (`:6781-6789`):
  `reliability/branch/process_calls/return_documents/source/target/transform`.
- Rejection loop (`:6839-6862`): gated `branch`/`process_calls` →
  `SYNC_PIPELINE_CONTROL_FLOW_UNSUPPORTED`; every other gated or unknown top-level key →
  `SYNC_PIPELINE_CONFIG_INVALID` with `field=<key>`. Nothing is silently dropped.
- Reserved stage kinds → `SYNC_PIPELINE_STAGE_UNSUPPORTED` (`:6905-6915`). Hints come from the
  reserved-kind map (`:6714-6725`): 10 of the 17 reserved kinds carry a kind-specific hint
  (9 naming the owning surface/issue; `finalize`'s names none); the 7 M11 property/cache kinds (`set_ddp`, `set_dpp`, `get_property`,
  `set_process_property`, `cache_put`, `cache_get`, `cache_join`) share the generic hint
  "Reserved stage kind (no PipelineSpec lowering in M5.2)." naming no owning issue (measured
  2026-07-14; enriching the hint map is a runtime string change out of #135's scope and belongs
  to the M12 issue that next touches this surface).
- Stage-level config allowlists: binding stages `_SYNC_PIPELINE_BINDING_KEYS` (`:6741-6743`,
  enforced `:7262`), listener `_SYNC_PIPELINE_LISTENER_KEYS` (`:6747-6749`, enforced `:7226`),
  map `_SYNC_PIPELINE_MAP_KEYS` (`:6751`, enforced `:7429`) — unknown stage-config keys rejected.

### 2.7 `flow_sequence` — strict steps inside a lenient config root

- Per-kind step-key allowlists `_FLOW_SEQUENCE_STEP_KEYS` (`process_flow_builder.py:289`);
  unknown kind → `PROCESS_FLOW_SEQUENCE_CONFIG_INVALID` with `field=<step>.kind` (`:4874-4881`);
  extra step keys → same code with `field=<step path>` (`:4882-4890`).
- The strictness is **recursive** (measured 2026-07-14, pinned by the freeze suite): an unknown
  key on a branch leg object → `PROCESS_FLOW_SEQUENCE_CONFIG_INVALID` with
  `field=flow_sequence[i].legs[j]`; an unknown key on a step nested inside a leg → same code
  with `field=flow_sequence[i].legs[j].steps[k]`.
- The config ROOT next to `flow_sequence` has no allowlist: an unknown root key whose value
  carries no `$ref:` token **and is not secret-shaped** is accepted AND ignored —
  `ProcessFlowBuilder.build()` output is string-identical with and without it (measured
  2026-07-14, pinned by the freeze suite). The root is still subject to the cross-cutting
  `$ref` reachability scan, which reads EVERY config value including unknown root keys.
  It is **also** subject to the cross-cutting plaintext-secret scan: on the public `_build_plan`
  path, `ProcessFlowBuilder.scan_forbidden_secret_fields(raw_config)` runs FIRST in the typed
  `process_kind` branch (`integration_builder.py:5762-5774`), **before** builder validation and
  **even on the reuse/reference/rejection paths**, so a secret-shaped root extra whose value is a
  **non-empty string or a dict/list** (`password: "x"`, `authorization: {…}`) is rejected and
  redacted with `PLAINTEXT_SECRET_REJECTED`. The scan is value-shape-sensitive: a forbidden key
  carrying an **empty string, `null`, or a bare scalar** (`password: ""`, `password: null`,
  `password: 123`) is deliberately **not** rejected (`process_flow_builder.py:523-545` — "scalars
  carry no plaintext to leak", and empty strings skip like the DB builder's `value and value`
  guard), so such a value remains accepted-and-ignored like any other non-secret extra. The
  "accepted-and-ignored" behavior above is therefore scoped to root values that are **not
  secret-shaped strings/containers**; the plaintext-secret guard is a plan-time precedence, not a
  widened boundary (the same cross-cutting scan already pinned for `wrapper_subprocess` in §2.8 by
  `test_wrapper_rejects_secret_looking_extras`). Declarations flow through `validate_config`'s `depends_on=`
  keyword parameter — which `_build_plan` supplies from the component spec — NOT through the
  config dict: a `depends_on` KEY inside the config is just another ignored root extra, never a
  declaration. A `$ref:` token inside an unknown root extra is therefore rejected with
  `MISSING_PROCESS_DEPENDENCY` at `depends_on` unless the token is declared via the keyword
  parameter (equivalently, in the component spec's `depends_on` at the `_build_plan` layer), in
  which case it is accepted and ignored like any other extra (identical planned steps).
  `ProcessFlowBuilder.build()` takes no declaration parameter and never runs the scan — emitted
  XML is byte-identical with or without a ref-bearing extra, declared or not; the scan is
  validation/plan-time only. The plan-layer rejection applies to authoring actions (measured on
  create and update): when a same-name component is found under the default
  `conflict_policy="reuse"`, `_build_plan` skips builder validation entirely and the SAME config
  — undeclared `$ref` extra included — plans as a clean `reuse` step with no validation error
  (all measured 2026-07-14, all pinned by the freeze suite). The one exception is a secret-shaped
  root extra **with a non-empty string / dict / list value**: the plaintext-secret scan precedes
  the reuse skip (`integration_builder.py:5762-5774`) and still rejects it with
  `PLAINTEXT_SECRET_REJECTED` (an empty/`null`/scalar secret-shaped value is skipped — see above).
  Any adapter gate for root leniency must scope to non-`$ref`, **non-secret-string/-container**
  values and account for the validation-skipping reuse path (past which the plaintext-secret scan
  still runs). A one-step `flow_sequence` is accepted
  (no 2+ minimum; an empty list is rejected with `PROCESS_FLOW_SEQUENCE_CONFIG_INVALID` at
  `flow_sequence`).
- Legacy single-slot sibling blocks (`flow_control`/`branch`/`decision`/non-passthrough
  `transform`/Try-Catch `reliability`) are rejected **by presence** alongside a `flow_sequence`
  (`:4627-4639,4672-4695`).
- **BUT the surrounding `database_to_api_sync` process config has NO global top-level allowlist**:
  `ProcessFlowBuilder.validate_config` (`:596`) checks only the blocks it knows
  (process_kind, source/target, transform, reliability, flow_sequence, refs) — an unknown
  top-level config key is **ignored**, in contrast to `sync_pipeline`'s fail-closed root (§2.6).
  This leniency is a #136 migration gate, not a contract.

### 2.8 `wrapper_subprocess` — no root allowlist; secret scan is the only extra-key guard

`WrapperSubprocessBuilder.validate_config` (`process_flow_builder.py:6537-6610`) validates
`process_kind`, `process_calls` entries, `process_extensions`, and `return_documents` — there is
**no root-key allowlist**: unknown root keys AND unknown keys inside a process-call entry are
accepted and ignored (build output is unchanged by their presence). The single guard over extras
is the inherited plaintext-secret scan (`scan_forbidden_secret_fields`, `:509`, code
`PLAINTEXT_SECRET_REJECTED` at `:564`): a secret-looking root key such as `password` **whose value
is a non-empty string or a dict/list** is rejected. This is the identical inherited scanner —
`validate_config` returns `cls.scan_forbidden_secret_fields(config)` (`:6610`) — so the same
value-shape rule as §2.7 applies: a forbidden key carrying an empty string, `null`, or a bare
scalar (`password: ""`, `password: null`, `password: 123`) is **not** rejected and stays
accepted-and-ignored (`process_flow_builder.py:523-545`). Pinned today by
`tests/test_wrapper_subprocess_builder.py:220-224` (`test_rejects_plaintext_secret`), which uses a
non-empty string value.

### 2.9 `emit_fragment` — convention, not contract

The fragment dict is consumed by convention between each primitive and its consuming archetype
(base default `{}` at `patterns/base.py:172`). Unknown-key preservation is **not a promised
contract**; nothing validates fragment keys at the boundary.

---

## 3. Fixture ledger

Golden directory: `tests/fixtures/golden_xml/` (the only golden dir). Each fixture below is
labeled by the **measured comparison mode of its comparing test** (read on 2026-07-13). XML
coverage is NOT uniformly byte-locked: 18 goldens are raw-byte, 8 are canonicalized, and the M8
JSON examples are structural round-trips.

### 3.1 Raw-byte equality (`emitted == golden.read_text()`)

| Golden | Comparing test |
|---|---|
| `dataprocess_groovy_transform.xml` | `tests/test_process_flow_builder.py:458` (docstring `:454` states "raw-string equality, not canonicalized") |
| `dataprocess_split_json_transform.xml` | `tests/test_process_flow_builder.py:692` |
| `dataprocess_split_xml_transform.xml` | `tests/test_process_flow_builder.py:700` |
| `dataprocess_combine_json_transform.xml` | `tests/test_process_flow_builder.py:708` |
| `dataprocess_combine_xml_transform.xml` | `tests/test_process_flow_builder.py:716` |
| `document_cache_retrieve.xml` | `tests/test_process_flow_builder.py:878` |
| `document_cache_remove.xml` | `tests/test_process_flow_builder.py:1082` |
| `return_documents_terminal.xml` | `tests/test_process_flow_builder.py:1253` |
| `branch_fanout.xml` | `tests/test_process_flow_builder.py:2497` |
| `decision_conditional.xml` | `tests/test_process_flow_builder.py:2828` |
| `flow_control_batching.xml` | `tests/test_process_flow_builder.py:3052` |
| `flow_sequence_decision_branch_map.xml` | `tests/test_process_flow_builder.py:3493` |
| `flow_sequence_cache_load_retrieve_remove.xml` | `tests/test_process_flow_builder.py:3527` |
| `flow_sequence_exception_terminal.xml` | `tests/test_process_flow_builder.py:3547` |
| `set_properties_ddp_dpp_flow_sequence.xml` | `tests/test_process_flow_builder.py:4001` |
| `flow_sequence_cache_put_get.xml` | `tests/test_process_flow_builder.py:4256` |
| `m11_cache_property_basic.xml` | `tests/test_m11_composed_examples.py:73` |
| `m11_processproperty_map_function.xml` | `tests/test_m11_composed_examples.py:96` |

### 3.2 Canonicalized XML equality (`ET.canonicalize(emitted) == ET.canonicalize(golden)`)

Whitespace/attribute-order tolerant — NOT byte-locked:

| Golden | Comparing test |
|---|---|
| `try_catch_dlq_document_cache.xml` | `tests/test_process_flow_builder_trycatch_dlq.py:123` |
| `try_catch_dlq_retry_count_2.xml` | `tests/test_process_flow_builder_trycatch_dlq.py:142` |
| `try_catch_notify_dlq_document_cache.xml` | `tests/test_process_flow_builder_trycatch_dlq.py:444` |
| `connector_scoped_trycatch_notify_dlq_document_cache.xml` | `tests/test_process_flow_builder_trycatch_dlq.py:689` |
| `exception_catch_path.xml` | `tests/test_process_flow_builder_trycatch_dlq.py:882` |
| `processcall_standalone_parent.xml` | `tests/test_wrapper_subprocess_builder.py:82` |
| `try_catch_dlq_document_cache_archetype.xml` | `tests/patterns/test_database_to_api_sync_dlq.py:445` |
| `try_catch_notify_dlq_document_cache_archetype.xml` | `tests/patterns/test_database_to_api_sync_dlq.py:552` |

### 3.3 Structural verification (no golden-file comparison)

- `tests/test_builder_xml_invariants.py` — invariant audit over PARSED builder output (focused
  `ET` attribute assertions; its docstring `:1-11` defers whole-shape byte-locking to the goldens
  above). Includes a sync_pipeline listener config at `:1277`.
- `tests/test_m11_composed_examples.py:110-131` — the M11 join example asserts the live-captured
  `DocumentCacheJoins` wire section as an inline string, plus the reversed-legs lineage rejection
  (`:134-144`).
- `tests/test_process_graph_verifier.py` — verifier-report assertions over built XML.

### 3.4 No `sync_pipeline_*.xml` golden exists

There is **no committed golden XML for sync_pipeline emission**. Its XML coverage rides on:

1. **Differential equality** in `tests/test_sync_pipeline_builder.py`: `lower_config` output must
   equal the hand-written `database_to_api_sync` core dict (`_CORE_CONFIG` `:151`, asserts
   `:169-189`), and `SyncPipelineBuilder.build` XML must be byte-identical to
   `ProcessFlowBuilder.build` of that core (`test_build_xml_equals_process_flow_builder_with_map`
   `:191-195`) — equality against another builder's live output, not against a committed file.
2. The lowered `database_to_api_sync` surface's own goldens (the `try_catch_*` /
   `*document_cache*` archetype goldens in §3.2 and the shape goldens in §3.1).

Adding true golden parity fixtures for the adapter is #139 scope.

### 3.5 JSON example fixtures (M8 / M11 / authoring)

| Example | Consuming test | Assertion mode |
|---|---|---|
| `examples/m8/composed_db_to_api_fanout.integration.json` | `tests/patterns/test_archetype_composition.py:47,346-371` | Structural JSON round-trip: `compose_archetypes_action` output `integration_spec` must EQUAL the recorded spec (`:355-356`), then plan clean through `_build_plan` (`:359-371`) |
| `examples/m8/cache_handoff_staged_fanout.integration.json` | `tests/patterns/test_archetype_composition.py:684,692-717` | Same round-trip + plans-clean pattern (`:692-717`) |
| `examples/m11/cache_property_authoring_basic.integration.json` | `tests/test_m11_composed_examples.py:68-84` | Parses as spec, plans clean, process XML raw-byte vs `m11_cache_property_basic.xml` |
| `examples/m11/process_property_map_function.integration.json` | `tests/test_m11_composed_examples.py:87-107` | Plans clean; processproperty XML raw-byte vs `m11_processproperty_map_function.xml` |
| `examples/m11/cache_property_authoring_join.integration.json` | `tests/test_m11_composed_examples.py:110-144` | Structural (inline wire-section string + lineage rejection) |
| `examples/authoring/*.json` (3 files) | `tests/test_authoring_examples_policy.py` | Anti-template policy assertions (not emission goldens) |

All example payloads carry `example_not_template: true` / `template_status:
"example_only_not_reusable_template"` markers, asserted by their tests.

### 3.6 #135 additions

`tests/fixtures/compatibility/issue_135/authoring_boundaries.json` +
`tests/test_issue_135_compatibility_freeze.py` characterize the §2 boundary behavior (JSON-level
only; no new XML goldens — the suites above remain the XML baseline).

---

## 4. Migration ownership map

Owning issues (verified against the live tracker 2026-07-13): #136 M12.1 promote flow_sequence
into strict ProcessIRV1 models · #137 M12.2 compiler-owned internal CFG and lowering contracts ·
#138 M12.3 verified process-emitter registry · #139 M12.4 legacy adapters and golden parity ·
#140 M12.5 first-class ConnectorCall and mixed linear flow · #141 M12.6 rich Branch and Decision
bodies · #142 M12.7 scoped error handling and retry/idempotency safety · #143 M12.8 unify
semantic validation on ProcessIRV1 · #144 M12.9 capability-gated SystemTopologySpecV1 planning ·
#145 M12.10 typed executable recipe contributions · #146 M12.11 MCP authoring/planning/compile/
verify surfaces · #147 M12.12 complete migration, documentation, examples, and live QA.

| Surface | Owning issue(s) | Migration gate (must close in the owning issue — never silently tightened) |
|---|---|---|
| `IntegrationSpecV1.pipeline` | #139 | Silent-precedence baseline (§2.5) replaced by derived equality or `LEGACY_ADAPTER_AUTHORITY_CONFLICT`; the field becomes a compiler-derived summary for a single-process spec, a preserved frozen inert value for a zero-process spec, and a rejected ambiguous input for a multi-process spec (ADR §5) |
| `main_process.config.pipeline` / `sync_pipeline` | #139 (adapter), #137 (lowering contracts) | Golden parity for the lowered config + XML (§3.4 has no committed golden today); `SYNC_PIPELINE_*` codes stay stable until the adapter mapping review |
| `flow_sequence` | #136 (strict models), #143 (semantic validation) | **Permissive config root** (§2.7 — unknown top-level keys around a flow_sequence are ignored) closed by strict ProcessIRV1 models in #136, with explicit rejection/mapping — not a quiet allowlist add here; `PROCESS_FLOW_SEQUENCE_CONFIG_INVALID` stays stable |
| `wrapper_subprocess` | #139 | **Root/call extras accepted-and-ignored** (§2.8) is a gate: the adapter must explicitly map or reject extras; `PLAINTEXT_SECRET_REJECTED` and the `PROCESS_REF_*` codes stay stable |
| Legacy `source`/`transform`/`target` blocks | #139 | Adapter + parity gates before any deprecation (ADR-001 versioning policy) |
| Primitive `emit_fragment` | #138, #145 | **Convention-not-contract** (§2.9) is a gate: replaced by the typed emitter-registry/recipe contract, with fragment parity tests, before any consuming archetype is rerouted |
| Archetype / composition inputs | #139, #145 | Recipes become typed contributions producing ProcessIR roots; existing composed goldens (§3) are the parity baseline |
| Rich control flow (ConnectorCall, Branch/Decision bodies, scoped Try/Catch) | #140, #141, #142 | Capability-gated per ADR-001 matrix; existing M10/M11 goldens (§3.1) pin the current shapes |
| Cache/property lineage pass | #143 | `PROCESS_LINEAGE_*` semantics re-homed onto ProcessIRV1 with unchanged verdicts on the current fixtures |
| Materialization `depends_on` / topo-sort | — (unchanged; verified by #147) | Stays the authoritative component plan; no behavior change permitted in M12 |
| Topology (`SystemTopologySpecV1`) | #144 | New capability-gated planning-only surface; `TOPOLOGY_*` family reserved |
| Verifier output / MCP verify-compile exposure | #138, #146 | Verifier remains the post-emission outer gate; new tool exposure is #146-gated (none ships in #135) |
| `import_integration_draft.pipeline_draft` | #146 | Stays analysis-only; must be labeled a derived view per ADR-001 |
| Doctrine views | #147 (docs refresh only) | Remains advisory text; never validation-bearing |
| Fixture/parity ledger (this file) | every issue as it lands; final sweep #147 | Each landing issue updates its row(s) here; #147 verifies the whole ledger against the shipped state |

**Standing rule** (ADR-001): permissive or un-goldened behavior identified above is a **gate to
close in the owning issue** with an explicit adapter mapping or rejection — silently tightening a
measured-lenient boundary in an unrelated change is a compatibility break.

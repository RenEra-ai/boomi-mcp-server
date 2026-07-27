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
| Adapter issue | #139 (M12.4 legacy adapters and golden parity) — must become a compiler-derived summary **for a single-process spec** (authored values checked by derived equality, or — on the strict surface / after announced V1 deprecation — rejected with `LEGACY_ADAPTER_AUTHORITY_CONFLICT`, never precedence; V1 preserves a disagreeing value inert until then); a **zero-process** spec's authored pipeline is preserved as a frozen inert value and a **multi-process** one is rejected as ambiguous on the strict surface (V1 preserves it inert until an announced §9 deprecation — ADR §5) |
| Migration gate | Today the executable nested pipeline **wins silently** when the two disagree (nothing reconciles them — §2.5); the strict `version="1.1"` authority selector that resolves this is **ACTIVE as of #139D** (see the #139D section) — on V1 the freeze test still pins the silent-precedence baseline, unchanged. **#139A DID close the ADR-001 §11 secret gap:** a plaintext secret in the top-level `spec.pipeline` stage config is now rejected with `PLAINTEXT_SECRET_REJECTED` (value-free path) in `_build_plan` before any echo/mutation — the two former known-gap freeze tests are flipped to the rejection behavior |

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
| Assertion strength | Lowered-dict equality + differential XML equality vs `ProcessFlowBuilder.build`, plus (since #138) a committed raw-byte golden `sync_pipeline_db_read_map_rest_send.xml` |
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
| Assertion strength | Differential: `lower_config` output equals the hand-written `database_to_api_sync` core dict (`tests/test_sync_pipeline_builder.py:151-189`) and emitted XML is byte-identical to `ProcessFlowBuilder.build` of that core (`:191-195`); since #138 there is ALSO a committed raw-byte golden `sync_pipeline_db_read_map_rest_send.xml` (§3.4) |
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
| Adapter issue | **#136 LANDED** (2026-07-19): the strict ProcessIRV1 models exist dark (`src/boomi_mcp/models/process_ir.py` + the private test-only `_process_ir_compat` codec; see [PROCESS_IR_V1.md](PROCESS_IR_V1.md)) — the legacy `flow_sequence` surface is UNCHANGED; the **legacy** config-root leniency (§2.7) stays under the **#139** legacy adapter's ownership (mapped as a compatibility no-op, not tightened), not #136; semantic validation unification #143 |
| Migration gate | **#139A CUT OVER (2026-07-22):** the composed `flow_sequence` build path now routes through the canonical `ProcessIRV1 → compile_process_ir_v1 → emit_process` chain (see the #139 M12.4 ledger section below); the pre-#139 `_emit_composed_flow_shapes` orchestration is deleted. The permissive **legacy** config root (§2.7) is mapped as a **compatibility no-op** by the adapter's projection (still accepted, recorded as `compatibility_noop_paths`), never tightened, never rejected without a separately announced §9 deprecation. Every existing golden stays byte-identical. Step-level codes stay stable |

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
| Assertion strength | Raw-byte XML equality since #138 (`processcall_standalone_parent.xml`, `tests/test_wrapper_subprocess_builder.py`; converted from `ET.canonicalize` — §3.2); plus structural shape/wiring assertions |
| Adapter issue | #139 (named adapter over Process Call semantics) — **#139A LANDED** (adapter `compiler/process_ir/legacy_adapters/wrapper_subprocess.py`) |
| Migration gate | **#139A CUT OVER (2026-07-22):** `WrapperSubprocessBuilder.build` now produces its shapes through the canonical `ProcessIRV1 → compile → emit_process` chain (see the #139 M12.4 ledger below). The accepted-and-ignored root/call extras (§2.8) are mapped as a **compatibility no-op** (still accepted, recorded as `compatibility_noop_paths`), never tightened, never rejected without a separately announced §9 deprecation. `processcall_standalone_parent.xml` stays byte-identical; `PLAINTEXT_SECRET_REJECTED` and the `PROCESS_REF_MISSING`/`PROCESS_EXTENSIONS_INVALID` totality guards stay stable |

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
equality or — on the strict surface / after announced V1 deprecation —
`LEGACY_ADAPTER_AUTHORITY_CONFLICT` (per ADR-001 §5), never precedence; V1 preserves a
disagreeing value inert until then (the same qualifier this file already carries at §1's
`IntegrationSpecV1.pipeline` row and in the §5 migration ledger).

**Secret-boundary gap (measured, pre-existing).** The `scan_forbidden_secret_fields` scanners are
invoked **per component**, keyed on component type, during step planning — e.g. `integration_builder.py:5338`
(DB), `:5479` (REST), `:5566` (SOAP), `:5625` (WSS), `:5744`/`:5774` (process-flow), and `:6032`
(the generic/profile scanner covering profiles, maps, scripts, process properties, caches, and
webservices); the `:5430`/`:5503`/… sites are the *redaction* branches that fire **after** a scan
returns `PLAINTEXT_SECRET_REJECTED`, not the scan calls. Each scanner traverses **only that component's own
`raw_config`, never the top-level `spec.pipeline`** — so the gap is **not** "zero components → no
scan" (a spec with a DB/REST/SOAP/WSS/profile component *does* run that component's scanner); the
gap is that **no scanner ever traverses the spec-level `spec.pipeline`, whatever components exist**.
The `components: []` case is merely the cleanest demonstration (zero scanners run at all). Combined
with `StageSpec.config` being an open `Dict[str, Any]` (§2.4) and `_build_plan` echoing
`spec.model_dump()` (`:6502`), a **secret-shaped value inside a top-level `spec.pipeline` stage's
`config` is accepted and echoed back unchanged** — unlike the `flow_sequence`/`wrapper_subprocess`
root extras (§2.7/§2.8), which *are* covered by the cross-cutting scan on their process component.
This is a **pre-existing** gap that #135 only **characterizes**; it is not introduced or fixed here.
Both halves of the claim above are freeze-pinned: `test_zero_process_pipeline_secret_config_echoed_is_known_gap`
(the `components: []` isolation) and `test_component_bearing_pipeline_secret_config_echoed_is_known_gap`
(the **whatever components exist** half — its control run plants the same sentinel in the component's
own config and gets `PLAINTEXT_SECRET_REJECTED`, proving that component's scanner really runs while
the `spec.pipeline` value still passes unscanned in the very same plan).
**Owner:** the #139 legacy adapter, whose contract already forbids promoting "free-form
credential/auth fields into ProcessIR, logs, diagnostics, or derived pipeline summaries" — it must
extend secret-scanning to `spec.pipeline` `stage.config` before that view becomes a supported
(non-inert) contract. ADR-001 §11 takes precedence over the §5 preserve rule.

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
  This leniency is a **#139 legacy-adapter** migration gate, not a contract — #136 only makes the new ProcessIRV1 models strict and does not tighten this legacy envelope. #139's adapter must map a currently-accepted extra as a compatibility no-op (still accepted); rejecting any currently-accepted field waits for a separately announced deprecation (ADR-001 §9).

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

Golden directory: `tests/fixtures/golden_xml/` — the only one this section enumerates. There is a
SECOND golden dir it deliberately does not cover: `tests/fixtures/process_ir/emitter_parity/`
(3 `*.process.xml` files), owned by the #138 emitter-parity oracle rather than by a builder fixture
ledger. Statements elsewhere in this document about "31 golden_xml + 3 emitter_parity fixtures" count
both; this section counts only the first. Each fixture below is
labeled by the **measured comparison mode of its comparing test**. Since #138 (M12.3) the
process-emitter goldens are UNIFORMLY byte-locked: all **34** committed `golden_xml/*.xml` fixtures
are raw-byte, **0 are canonicalized** (the 8 formerly-canonicalized fixtures in §3.2 were converted
to raw `==` and 5 new byte anchors were added — §3.2/§3.4 and the §138 note; #139C then added the
3 sync_pipeline anchors in §3.4, taking 31 → 34). The M8 JSON examples remain structural round-trips.

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

### 3.2 Formerly canonicalized goldens — converted to raw byte equality (#138)

These eight were compared via `ET.canonicalize(...)` (whitespace/attribute-order tolerant) until
#138 (M12.3), which converted them to raw `==` byte equality so the emitter-registry extraction has a
strict byte gate. They now belong to the byte-locked set (§3.1); no committed bytes changed in the
conversion (each already reproduced byte-for-byte).

| Golden | Comparing test |
|---|---|
| `try_catch_dlq_document_cache.xml` | `tests/test_process_flow_builder_trycatch_dlq.py` |
| `try_catch_dlq_retry_count_2.xml` | `tests/test_process_flow_builder_trycatch_dlq.py` |
| `try_catch_notify_dlq_document_cache.xml` | `tests/test_process_flow_builder_trycatch_dlq.py` |
| `connector_scoped_trycatch_notify_dlq_document_cache.xml` | `tests/test_process_flow_builder_trycatch_dlq.py` |
| `exception_catch_path.xml` | `tests/test_process_flow_builder_trycatch_dlq.py` |
| `processcall_standalone_parent.xml` | `tests/test_wrapper_subprocess_builder.py` |
| `try_catch_dlq_document_cache_archetype.xml` | `tests/patterns/test_database_to_api_sync_dlq.py` |
| `try_catch_notify_dlq_document_cache_archetype.xml` | `tests/patterns/test_database_to_api_sync_dlq.py` |

### 3.3 Structural verification (no golden-file comparison)

- `tests/test_builder_xml_invariants.py` — invariant audit over PARSED builder output (focused
  `ET` attribute assertions; its docstring `:1-11` defers whole-shape byte-locking to the goldens
  above). Includes a sync_pipeline listener config at `:1277`.
- `tests/test_m11_composed_examples.py:110-131` — the M11 join example asserts the live-captured
  `DocumentCacheJoins` wire section as an inline string, plus the reversed-legs lineage rejection
  (`:134-144`).
- `tests/test_process_graph_verifier.py` — verifier-report assertions over built XML.

### 3.4 `sync_pipeline_*.xml` golden coverage

Since #139C (M12.4) there are FIVE committed raw-byte goldens built through
`SyncPipelineBuilder.build` — the first two landed with #138 (M12.3), the last three with #139C,
which generated them through the LEGACY renderer *before* the cut-over so they pin pre-change bytes
and cannot be self-confirming:

1. `listener_wss_start.xml` — a WSS-listener sync-pipeline build
   (`tests/test_process_flow_builder_listener.py` `test_listener_wss_start_matches_golden`),
2. `sync_pipeline_db_read_map_rest_send.xml` — a non-listener db-read → map → rest-send build
   (`tests/test_sync_pipeline_builder.py` `test_sync_pipeline_matches_golden_fixture`),
3. `sync_pipeline_fetch_map_db_write.xml` — fetch(rest_fetch) → map → write(db_write); the FIRST
   byte anchor anywhere for a `database`/`Send` target, i.e. the whole `api_to_database_sync` family,
4. `sync_pipeline_fetch_rest_send_no_map.xml` — fetch(rest_fetch) → send(rest_send); the first anchor
   for a REST *source* and for the map-less (passthrough) chain, and
5. `sync_pipeline_soap_fetch_soap_send.xml` — soap_fetch → soap_send; the SOAP Client family
   end to end (all three in `tests/test_sync_pipeline_builder.py`).

Before #139C only rows 1–2 existed, so a single-byte change to a `fetch`, `write`, `soap_fetch` or
`soap_send` chain broke **no test at all**. The remaining un-anchored micro-variant — a SOAP target
carrying a non-uppercase `execute` verb — is covered differentially instead
(`tests/test_sync_pipeline_adapter_cutover.py`), which is stronger than a byte file because it
compares against the live legacy renderer.

All five compare the complete `SyncPipelineBuilder.build` output byte-for-byte against a committed file.
This matters because the differential `xml_sync == xml_core` check compares two callers through the
ONE shared renderer, so it cannot catch a drift in that shared template; the committed fixtures pin
the actual bytes. The remaining sync_pipeline XML coverage still rides on:

1. **Differential equality** in `tests/test_sync_pipeline_builder.py`: `lower_config` output must
   equal the hand-written `database_to_api_sync` core dict (`_CORE_CONFIG` `:151`), and
   `SyncPipelineBuilder.build` XML must be byte-identical to `ProcessFlowBuilder.build` of that core
   (`test_build_xml_equals_process_flow_builder_with_map`) — equality against another builder's live
   output.
2. The lowered `database_to_api_sync` surface's own goldens (the `try_catch_*` /
   `*document_cache*` goldens in §3.2 and the shape goldens in §3.1).

Broadening golden parity across the remaining sync_pipeline variants (fetch/write, SOAP) was **#139C
adapter scope and is now DONE** — see §3.4 rows 3–5. #138 added the first two byte anchors (listener +
linear); #139C added the fetch/write, map-less and SOAP anchors ahead of its cut-over.

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
`tests/test_issue_135_compatibility_freeze.py` characterize the §2 boundary behavior using **JSON
fixtures plus differential emitted-XML equality** — several cases build with and without an extra
and assert `ProcessFlowBuilder.build(...)` output is string-identical (e.g. the flow_sequence
root-leniency case), and the wrapper cases assert scanner behavior — with **no new committed XML
golden** (the suites in §3.1–§3.4 remain the golden XML baseline).

### 3.7 #136 additions (ProcessIRV1 models — dark)

New fixture directory `tests/fixtures/process_ir/` (JSON only; **no XML fixture added or
changed** — the §3.1 goldens stay the emission baseline):

| Fixture | Consuming test | Assertion mode |
|---|---|---|
| `process_ir_v1.json` (3 full-vocabulary canonical IR documents) | `tests/test_process_ir_models.py` golden pins | **Byte-equal** canonical JSON, generated twice per run |
| `process_ir_v1.schema.json` | `tests/test_process_ir_models.py` golden pins | **Byte-equal** canonical JSON Schema (pinned to the current pydantic; an upgrade forces a reviewed regeneration) |
| `flow_sequence_compat_cases.json` (10 sentinel legacy configs incl. the semantic shapes of the five §3.1 flow_sequence goldens + a wrapper case) | `tests/test_process_ir_flow_sequence_codec.py` | Canonical-IR round-trip equality (`legacy→IR` == `legacy→IR→legacy→IR`) + reconstructed configs pass the UNCHANGED `ProcessFlowBuilder`/`WrapperSubprocessBuilder.validate_config` |

Model-boundary coverage lives in `tests/test_process_ir_models.py` (structural: every node
kind, strictness, `PROCESS_IR_*` diagnostics with pinned JSON pointers, secret/repr
suppression, closed-schema assertions). The codec is PRIVATE and test-only; builder execution
is never rerouted through it.

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
| `IntegrationSpecV1.pipeline` | #139 | Silent-precedence baseline (§2.5) replaced by derived equality or `LEGACY_ADAPTER_AUTHORITY_CONFLICT`; the field becomes a compiler-derived summary for a single-process spec, a preserved frozen inert value for a zero-process spec, and (on the strict surface / after announced V1 deprecation) a rejected ambiguous input for a multi-process spec — V1 preserves it inert until then (ADR §5) |
| `main_process.config.pipeline` / `sync_pipeline` | #139 (adapter), #137 (lowering contracts) | Golden parity for the lowered config + XML (§3.4 — #138 added the first committed sync_pipeline golden; #139 broadens variant coverage); `SYNC_PIPELINE_*` codes stay stable until the adapter mapping review |
| `flow_sequence` | #136 (new strict ProcessIRV1 models — **LANDED** 2026-07-19, dark: models + `PROCESS_IR_*` codes in `boomi_mcp.errors` + goldens, zero legacy-surface change; §3.7), #139 (legacy config-root adapter), #143 (semantic validation) | #136's half of this gate is CLOSED: the **new** ProcessIRV1 models are strict and the frozen vocabulary is losslessly representable (codec parity, §3.7); the **legacy** permissive config root (§2.7 — unknown top-level keys around a flow_sequence are ignored) stays with **#139**'s adapter, which maps today's accepted extras as a compatibility no-op (still accepted) — never a quiet allowlist add and never rejected without an announced deprecation (§9); `PROCESS_FLOW_SEQUENCE_CONFIG_INVALID` stays stable |
| `wrapper_subprocess` | #139 | **Root/call extras accepted-and-ignored** (§2.8) is a gate: the adapter maps them as a compatibility no-op (still accepted), never rejecting a currently-accepted extra without an announced deprecation (§9); `PLAINTEXT_SECRET_REJECTED` and the `PROCESS_REF_*` codes stay stable |
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

**Standing rule** (ADR-001): the **measured accepted-but-ignored unknown-field extras** identified
above (the permissive `flow_sequence` config root, wrapper root/call extras) are a **gate to close
in the owning issue** with an explicit adapter **no-op mapping** — such currently-accepted extras
stay accepted; rejecting one is a compatibility break requiring a separately announced deprecation
(ADR-001 §9), and silently tightening a measured-lenient boundary in an unrelated change is likewise
forbidden. **This no-op rule does NOT govern the mandated `LEGACY_ADAPTER_AUTHORITY_CONFLICT`
rejections of ADR §5** (a disagreeing single-process, or any multi-process, authored
`spec.pipeline`): those are the deliberate M12 authority decision — not the silent tightening of an
ignored extra. **But because that rejection withdraws an acceptance the freeze suite proves exists
today — `test_contradictory_pipelines_silent_precedence_through_build_plan` pins the disagreeing
single-process shape and `test_multi_authored_spec_with_top_level_pipeline_accepted_today` pins the
multi-process one (its collision variants — one or both authored processes collapsing to
collision-driven reuse — are pinned by `test_multi_authored_collision_reuse_keeps_acceptance_and_echo`,
so the ADR §5 account-independence claim has a measured baseline) — it is still a compatibility
tightening governed by ADR §9's
announced-policy-before-removal gate** — #139 lands the derived-equality reconciliation and the diagnostic, and the hard rejection of
currently-accepted contradictory/ambiguous input ships only behind an announced deprecation with a
documented replacement surface (not by design at #139's cutover, and not via the no-op rule).
Un-goldened parity gaps are closed by establishing baselines (§3.4), not by a no-op.

## #138 M12.3 — process-emitter registry extraction (mechanical consolidation)

The verified shape serializers were extracted out of `ProcessFlowBuilder` into
`categories/components/builders/process_emitters/` (a single copy of every template + layout
primitives), and a typed, test-only registry (`compiler/process_ir/emitter_registry.py`) now emits
from that same copy behind the `EmissionPlanV1` boundary. Compatibility facts:

- **17 registry keys / 16 model classes** at #138 (the `connectoraction_source`/
  `connectoraction_target` keys share `ConnectorActionInputV1`); **18 / 17 after #142**, which
  registered `catcherrors`. Every currently-emitted process shape kind has exactly one registered
  typed emitter or a documented legacy-only exception (`_emit_start_listen` → #140, `_emit_notify`
  → still legacy-only, connector `dynamic_path` → #139/#140, `emit_fragment` permanently excluded,
  `route` no entry). `_emit_catcherrors` left this exception list in #142.
- **Zero normalized comparisons remain among the process-emitter goldens.** The eight C14N-compared
  fixtures (try_catch/notify/connector-scoped/exception/processcall/archetype) were converted to raw
  `==`; five new raw fixtures freeze previously structural-only or differential-only paths
  (`listener_wss_start.xml`, `dynamic_path_target_profile.xml`, `dynamic_path_source_ddp.xml`,
  `try_catch_dlq_error_subprocess.xml`, and the first committed `sync_pipeline_db_read_map_rest_send.xml`).
  **No existing golden's committed bytes changed.** The `tests/fixtures/process_overrides/`
  `strip_text` behavior test stays (it covers component-envelope serialization, which remains in
  `ProcessFlowBuilder`).
- No public schema/request/default/error/dispatch/plan/apply/verification behavior changed; the
  legacy builder keeps its external error contract. See
  `docs/architecture/PROCESS_EMITTER_REGISTRY_V1.md`.

## #139 M12.4 — legacy adapters and golden parity (first slice, #139A)

**Landed 2026-07-22.** This is a deliberately partial first slice of #139: it introduces the internal
legacy-adapter boundary and cuts TWO executable dialects over to the one canonical path, closes the
top-level pipeline secret gap, and leaves the rest of #139 explicitly pending (so the issue stays
OPEN and #140 stays blocked while any executable row below is still `legacy`).

**The adapter boundary.** `src/boomi_mcp/compiler/process_ir/legacy_adapters/` (DARK, imported directly
by the migrated build paths, never via `process_ir.__all__`):

- `contracts.py` — frozen `LegacyAdapterResultV1` (process_ir + `symbol_requirements` +
  `compatibility_noop_paths` + reserved `pipeline_view`/`pipeline_view_status`), `LegacySymbolRequirementV1`,
  `LegacyAdapterDiagnosticV1`, `LegacyAdapterError`. Repr-redacted; carries no XML/CFG/layout/shape-id/
  credential/raw-config (ADR-001 §6, pinned by `tests/test_legacy_process_ir_adapters.py`).
- `registry.py` — immutable `MappingProxyType` keyed by **qualified dialect**: migrated =
  {`wrapper_subprocess`, `database_to_api_sync/flow_sequence`}; reserved-but-unmigrated =
  {`database_to_api_sync` (ordinary), `sync_pipeline`}.
- `wrapper_subprocess.py`, `flow_sequence.py` — the two production adapters.
- `emission.py` — `emit_legacy_result`: builds `SymbolTableV1` from the requirements and drives
  `compile_process_ir_v1 → emit_process`, returning the verified `shape_xml_parts`.

The `flow_sequence` adapter REUSES the single forward translator `legacy_flow_sequence_to_ir`
(`models/_process_ir_compat.py`, no longer test-only-imported) on a config projected to the codec's
known keys; safe unknown root/binding keys are recorded as `compatibility_noop_paths`, never rejected
(no unknown-field tightening). Its symbol requirements are derived from the compiled emission plan
(the exact component-id/type pairs the emitter validates), so a role/kind can never be missed or
mistyped. The `wrapper_subprocess` adapter builds its IR directly (resolved-ref semantics, matching
`build()`), deduping a repeated child into one requirement.

### #139A migration ledger

| Surface | Adapter | Executed subset | Public error translation | XML fixtures (byte-identical) | Verifier | Live-QA | Cutover status |
|---|---|---|---|---|---|---|---|
| `wrapper_subprocess` | `wrapper_subprocess.py` | ordered process_calls + Stop/Return Documents; `wait`/`abort_on_error`/`label` | adapter/compile/emit defect → `PROCESS_XML_VALIDATION_FAILED`; `PROCESS_REF_MISSING`/`PROCESS_EXTENSIONS_INVALID` totality guards preserved (precede the adapter) | `processcall_standalone_parent.xml` | inside `emit_process` (`verify_process_graph`) | required | **canonical** |
| `database_to_api_sync` / `flow_sequence` | `flow_sequence.py` | 11 linear kinds; terminal branch/decision/exception; nested branch/decision arms; Return-Documents (linear only); target-less cache_put staging legs | adapter/compile/emit defect → `PROCESS_XML_VALIDATION_FAILED`; step/config codes unchanged (validator runs first) | `flow_sequence_decision_branch_map.xml`, `flow_sequence_cache_load_retrieve_remove.xml`, `flow_sequence_exception_terminal.xml`, `set_properties_ddp_dpp_flow_sequence.xml`, `flow_sequence_cache_put_get.xml`, `m11_cache_property_basic.xml` (the M11 process; `m11_processproperty_map_function.xml` is a **processproperty component** built by `ProcessPropertyBuilder`, NOT the flow_sequence process — its process is exercised structurally, not as a byte golden) | inside `emit_process` | required | **canonical** |
| composition process emission (`patterns/composition.py`) | (inherits `flow_sequence`) | main process rewritten to `database_to_api_sync` + `flow_sequence=[map_ref, terminal branch]` | via the flow_sequence adapter | archetype-composition suite (raw XML parity) | inside `emit_process` | required | **canonical-inherited** (recipe adapter pending #145) |
| `sync_pipeline` + 4 sync archetypes | `sync_pipeline.py` (#139C) | the 6 NON-listener stage chains: `read\|fetch(rest_fetch\|soap_fetch)` → `[map]` → `send(rest_send\|soap_send)\|write(db_write)`. The 4 WSS **listener** chains stay on the legacy renderer behind an explicit routing gate (`_sync_pipeline_is_canonical`) — #140 owns the fused `start_listen` entry | adapter/compile/emit defect → `PROCESS_XML_VALIDATION_FAILED`; `SYNC_PIPELINE_*` codes unchanged and keep precedence (`lower_config` runs first) | `sync_pipeline_db_read_map_rest_send.xml`, `sync_pipeline_fetch_map_db_write.xml`, `sync_pipeline_fetch_rest_send_no_map.xml`, `sync_pipeline_soap_fetch_soap_send.xml` (+ `listener_wss_start.xml` on the legacy arm) | inside `emit_process` | required | **canonical** (listener arm pending #140) |
| ordinary `database_to_api_sync` (single/linear, Try-Catch, dynamic path, listener) | reserved | — | unchanged | existing goldens | n/a | n/a | **pending-capability** (needs canonical start_listen / dynamic-path / catcherrors / notify emission). #139C's adapter is a *linear single-shape core* adapter deliberately shaped to be promoted here once those four close |
| `emit_fragment` primitives | — | — | unchanged | — | n/a | n/a | **n/a** (never canonical) |
| top-level `spec.pipeline` | secret scan only | — | `PLAINTEXT_SECRET_REJECTED` (value-free path) | — | n/a | required | **V1-inert; §11 secret gap CLOSED; strict `version="1.1"` selector ACTIVE (#139D) — V1 unchanged** |

### Deletions / no duplicate emitter path

The now-unreachable composed-flow XML orchestration (`_emit_composed_flow_shapes`,
`_source_prefix_flow_entries`, `_target_terminal_entries`, `_append_path`, `_append_branch`,
`_append_decision`, `_emit_seq_linear`, `_append_linear_entries`, `_seq_step_to_flow_entry`,
`_seq_exception_params`) was DELETED from `process_flow_builder.py`; the shared shape templates
(`_emit_flow_shape`, `_emit_linear_shapes`, `_emit_branch_shapes`, `_emit_decision_shapes`, the
Try/Catch emitters) stay — they still serve the non-migrated ordinary `database_to_api_sync` paths.
`tests/test_legacy_adapter_cutover.py` pins their absence and that the migrated builds drive
`emit_process`/`compile_process_ir_v1`.

### ~~Locked-but-not-activated strict authority selector~~ — ACTIVATED in #139D

`config.integration_spec.version="1.1"` (design fixed in [ADR-001](ADR-001-process-ir-authority.md) §5)
is **ACTIVE as of #139D** — see the #139D section at the end of this document.

The #139A note here previously said activation had to wait "until every executable dialect
(sync/ordinary/listener) has a faithful normalization path so a derived `PipelineSpec` view can be
computed." **That premise was wrong, and dropping it is what unblocked the slice.** The strict surface
does not derive a view at all: it *validates and preserves* the authored one. Deriving would need a
config→`PipelineSpec` direction that exists nowhere in the tree (the only normalizer,
`SyncPipelineBuilder.lower_config`, runs `PipelineSpec` → block config), i.e. a second semantic
compiler — which ADR-001 §6 forbids. Comparing **both** surfaces through that one existing normalizer
needs no new derivation and no dialect coverage, so activation never depended on #140/#142 at all.

`LEGACY_ADAPTER_PIPELINE_DRAFT_ONLY` remains a reserved, unraised code (see #139D below).

### Deferred (issue stays OPEN)

~~sync_pipeline golden-baseline + adapter~~ (**DONE in #139C**), ~~authority activation~~ (**DONE in
#139D**), ordinary database_to_api_sync (needs canonical listener / dynamic-path / catcherrors /
notify emission — currently owned by #140/#142), the recipe/archetype named adapters (#145), and the
final cutover that removes the remaining legacy XML dispatch. Full-#139 DOD requires either an
ownership adjustment allowing parity-only support for those already-shipped capabilities or a
milestone dependency change; the shipped slices do not fake completion around that gap.

**After #139D, every remaining #139 item is blocked on another issue** (#140 / #142 / #145) — none is
work #139 can do on its own merits.

**Deferred faithfulness items (do NOT affect any valid/deployable config):**

- **Denormalized `profile_type` fail-closed.** A non-canonical-case property-source `profile_type`
  (e.g. `PROFILE.JSON`) is validate-accepted but the #138 emitter registry deliberately fails closed
  on denormalized profile kinds (`emitter_registry.py:443-466`, exact membership). The pre-#139
  renderer passed the spelling through verbatim, producing an undeployable `profileType` (Boomi
  profile types are canonical lowercase). The cutover surfacing this as a build-time rejection is the
  intended M12 tightening, not a parity regression on a deployable config.
- **Remaining adapter-boundary contract** (`LegacyPathBindingV1`/`path_bindings`, `Literal`-typed
  `pipeline_view_status`, `PipelineSpec`-typed `pipeline_view`). The opaque `legacy_selector` and
  exact RFC 6901 source pointers were DELIVERED by #139B (see below); the remaining typed
  path-binding / pipeline-view fields are needed by the deferred strict authority selector — follow-up
  with that work.

### #139B landed — occurrence-scoped IR references (2026-07-23)

The flow adapter now rewrites every component id/ref occurrence to an OCCURRENCE-SCOPED alias
`$ref:legacy.adapter:<RFC6901-pointer>` (embedding no authored value) before the unchanged #136 codec
runs, and each `LegacySymbolRequirementV1` carries a redacted `legacy_selector` (the original id) that
`emission._symbol_table` resolves to the real `component_id`. Because `SymbolTableV1` permits distinct
refs sharing one `component_id`, ONE id reused across roles — even incompatible ones (a `map_ref` and
a `document_cache_id`, or a database source operation and a REST target operation) — now round-trips
BYTE-FAITHFULLY (proven by control-substitution oracles) instead of failing closed. This DELETED the
#139A `_collect_binding_meta` connector-conflict guard entirely (no collision is possible) and its
`_root_target_emitted` predicate (a dead root target's alias never reaches the CFG, so it is excluded
structurally). All 31 golden_xml + 3 emitter_parity fixtures stay byte-identical; the frozen #136
codec + its tests are unchanged. A live IR alias with no recorded selector fails closed
(`LEGACY_ADAPTER_SEMANTIC_LOSS`), guarding future codec vocabulary additions from silently
reintroducing raw-id collapse. Incompatible-role reuse may still be undeployable in Boomi, but now
reproduces the pre-#139 builder's accepted byte output instead of failing during canonical compilation.

### #139C landed — the `sync_pipeline` adapter (2026-07-24)

`SyncPipelineBuilder.build` now lowers its pipeline and emits the resulting linear core through the
canonical `ProcessIRV1 → compile_process_ir_v1 → emit_process` chain, for **6 of its 10 accepted
stage chains**. The adapter (`legacy_adapters/sync_pipeline.py`) builds IR **directly** — the
`wrapper_subprocess` approach — because the frozen #136 codec cannot serve this dialect at all: it
hard-requires a non-empty `flow_sequence`, and 5 of the 10 chains are map-less, so there is no
non-empty step list to hand it (and `_process_ir_compat.py` is a hard boundary, so the requirement
cannot be relaxed).

**Scope and routing.** Migrated: `read | fetch(rest_fetch | soap_fetch)` → `[map]` →
`send(rest_send | soap_send) | write(db_write)`. NOT migrated: the 4 WSS **listener** chains, which
stay on the legacy renderer behind an explicit predicate (`_sync_pipeline_is_canonical`, reusing the
very resolver the legacy body uses to select `start_listen`, so the two cannot disagree). A listener
entry FUSES the start and connector shapes into one `start_listen`, which ProcessIR v1 cannot express
at three independent layers (no registry key; the compiler fails closed; `SourceEndpointV1.connection_ref`
is required while a lowered listener binding has no `connection_id` at all) — **#140** owns it. This
is an ALLOW decision on one named gap, never a catch-all fallback: a chain that passes the gate but
that the adapter cannot represent RAISES and is never re-routed. The adapter refuses a listener
independently (Gate 2), so a future caller cannot route one past the gate.

**A compiler defect was found and corrected: `lowering.py::_canonical_connector_metadata`.** It
upper-cased the action for `role == "target"` regardless of connector family, reproducing the
**deleted composed-flow** target emitter rather than the ordinary/linear target path this dialect
delegates to. Three divergence classes resulted, all measured end to end:

| role | connector_type | action_type | legacy emitted | compiler emitted (pre-#139C) |
|---|---|---|---|---|
| target | `database` | `Send` | `('database','Send')` | `('database','SEND')` |
| target | `DATABASE` | `Send` | `('database','Send')` | `('DATABASE','SEND')` |
| target | `soap_client` | `execute` | `('wssoapclientsdk','execute')` | `('wssoapclientsdk','EXECUTE')` |

The first fires on **100 %** of DB-write builds (the verb is pinned to the exact string `Send` at
lowering), i.e. every `api_to_database_sync` build. The rule is now family-conditional and
role-independent — REST upper-cases in either role, every other family keeps the authored verb
verbatim and lower-cases the subtype — matching the linear builder, which carries an explicit in-code
warning against exactly this corruption.

This required lifting `lowering.py` from the #139B "explicitly unchanged boundaries" list. That list
is a means to four stated gates, and **all four were measured to hold**: 31 golden_xml byte-identical,
3 emitter_parity byte-identical, the frozen #136 codec suite unchanged and green, no fixture
regeneration. The reason is structural, not lucky: `flow_sequence` can never carry a DB or SOAP target
(`_validate_target_binding` defaults `allow_db_target=False`/`allow_soap_target=False` at every call
site), so every already-migrated target is REST and takes the arm whose behaviour is unchanged;
`wrapper_subprocess` has no connectors at all. Measured blast radius across the whole suite: **3
failing tests**, all hand-authored compiler fixtures asserting the buggy string, plus **one that did
NOT fail but went vacuous** (`test_synthetic_stop_must_follow_its_routed_target` — the recomputation
mismatch would have raised the same code as the adjacency invariant it is named for, so it would pass
even with that check deleted). All four were repaired, and the canonicalization test was re-anchored
on the linear builder with a new pin requiring agreement for every reachable
`(family, role, verb)` triple.

**Byte-parity oracles.** Because the interception happens inside `SyncPipelineBuilder.build` *before*
it would delegate, `ProcessFlowBuilder.build` on the same lowered config remains a genuinely
independent legacy renderer — so every migrated chain is asserted differentially against it on every
run (`tests/test_sync_pipeline_adapter_cutover.py`). Three new byte goldens (§3.4) complement it by
catching a uniform drift that would move both sides together; they were generated through the legacy
renderer *before* the cut-over landed, so they cannot be self-confirming. Reuse of one component id
across roles is pinned with the #139B control-substitution oracle, including the cross-type
`map_ref == connection_id` case. `compatibility_noop_paths` is always `()` for this dialect and pinned
as such: its config gate is a strict allow-list at every level, so nothing is accepted-and-ignored.

**Unchanged:** `lower_config` / `validate_config` and every `SYNC_PIPELINE_*` code and its precedence
(lowering runs first); the archetype caller `database_to_api_sync.py`, which calls `lower_config` and
builds through `ProcessFlowBuilder`, never `SyncPipelineBuilder.build`, and so stays on the legacy
renderer (pinned by a test). Rollback is one boolean: making `_sync_pipeline_is_canonical` return
`False` restores the legacy renderer for every chain with all goldens still passing, because they are
legacy bytes by construction.

### #139D landed — the strict pipeline-authority selector (2026-07-25)

Closes the #139 acceptance criterion *"Top-level/component pipeline disagreement has a deterministic
tested conflict error on the strict/opt-in surface, and a tested preserved-inert (non-precedence) echo
on V1"* (ADR-001 §5 transition, §9 gate). **Zero emitted-XML impact** — this slice is plan-time
validation only; all 34 `golden_xml` fixtures and the 3 emitter-parity fixtures are byte-identical, and
no emitter, builder, adapter, archetype, or fixture was touched.

**The selector.** `IntegrationSpecV1.version` becomes `Literal["1.0", "1.1"]`, default `"1.0"`.

ADR-001 §5 requires a selector *"a pre-#139 server cannot silently drop"*. The code decides which
mechanism qualifies: `IntegrationSpecV1` declares no `model_config`, so pydantic's default
`extra="ignore"` applies (pinned by `test_spec_envelope_ignores_unknown_fields`) — an ordinary optional
flag would be **silently discarded** and the request would degrade to legacy precedence, the exact
failure the ADR names. A new `version` literal cannot degrade: the old contract rejects it outright.

Reachability is **explicit-form only**. `_normalize_to_spec` passes caller keys through verbatim for
`integration_spec`, but the `source_description` and bare top-level forms rebuild the spec from a fixed
key allowlist that carries no `version` — so they always normalize to `1.0`. That is fail-*safe*
(degrades to frozen legacy), and it is contractual, not incidental.

**Nothing is dragged onto the strict surface.** All six archetype spec constructors hard-pin
`version="1.0"`; the four that also author a top-level `pipeline` (`api_to_database_sync`,
`api_to_api_sync`, `http_listener_to_db`, `http_listener_to_rest`) therefore stay on V1 unchanged.

**Dispositions** (evaluated in `_build_plan` immediately after `_normalize_to_spec`, i.e. before
wrapper synthesis, before collision resolution, before any live lookup, and before any mutation):

| declared authoring processes | submitted semantics | outcome |
|---|---|---|
| 0 | — | accept; preserve the inert view |
| 1 | unavailable (bad/missing `process_kind`, invalid config) | **no disposition** — that payload's own error surfaces untouched (clean-plan gate) |
| 1 | valid, comparable, equal | accept; keep the view |
| 1 | valid but not representable by a singular linear view | **reject** — `LEGACY_ADAPTER_AUTHORITY_CONFLICT` |
| 1 | valid, comparable, unequal | **reject** — same code |
| ≥2 | anything, including unavailable | **reject** as ambiguous — same code, decided first |

Ambiguity is counted over *declared* authoring actions before collision resolution: an `update` always
authors (it re-emits XML from its config) even when flagged `reference_only`, which the planner honours
only for `create`; only a `reference_only` **create** is excluded. Ambiguity therefore outranks the
clean-plan gate — the choice ADR-001 §5 explicitly leaves to #139.

**The comparison, and why it needed no new derivation.** The only normalizer in the tree,
`SyncPipelineBuilder.lower_config`, runs **`PipelineSpec` → `database_to_api_sync` block config**. There
is no config→`PipelineSpec` direction anywhere. Rather than build one (a second semantic compiler,
forbidden by ADR-001 §6), #139D normalizes **both** surfaces through that same existing lowering and
compares the projected `{process_kind, source, transform, target}` core.

That fixes the comparison's domain exactly: **the normal form is the image of `lower_config`**, so a
submitted process carrying anything lowering could never emit is *valid but not representable* — a
disagreement, never agreement-by-omission (ADR-001 §5: "the absence of a nested `config.pipeline` is
not agreement"). This is **fail-closed and load-bearing**: `ProcessFlowBuilder.validate_config` accepts
unknown root keys, so a shipped Try/Catch or Notify block — or any feature block added after this slice
— would otherwise read as *agreement*. It is mutation-tested (dropping the containment check fails four
tests).

**The clean-plan gate closes a CLASS, not a list of passes.** This repo validates a process in several
passes — structural lowering, then `validate_config`, then `$ref` type-checking, then lints — and an
authority conflict must never **mask** the actionable error any of them produces. Pre-running them
inside the comparison closed three instances of one bug in a row (lower-time, then `validate_config`-time
via an undeclared `$ref`/unsupported REST verb, then `$ref`-type-time), each found by a separate review
or QA round. Enumerating passes was the wrong shape of fix.

The **first attempt at closing it was worse than the bug.** Deferring the conflict dispositions to the
end of the plan, so they could yield to whatever error the plan produced, made the strict verdict
**account-DEPENDENT**: the plan gates `$ref` type-checking on `will_invoke_process_flow_builder`, which
depends on `planned_action`, so an identical payload was *accepted* (with `PROCESS_REF_TYPE_MISMATCH`)
when no same-name component existed and *rejected* as an authority conflict when one did, because reuse
skipped that validator. ADR-001 §5 forbids exactly this, and explicitly rejects deferring the
disposition post-collision for this reason. A third Codex round caught it; the QA account-independence
sweep had missed it because every payload in that sweep had a *clean* authored process.

The gate therefore **re-runs the account-independent validation passes itself**, before collision
resolution — so the whole decision still lands before any live lookup, and no unrelated later plan exit
(a topological-order failure, an invalid `conflict_policy`) can suppress an already-computed conflict.

**Enumerating the passes failed four times, so the gate does not enumerate them.** Each fix closed one
instance and missed the next — lower-time, then `validate_config`-time, then `$ref`-type-time, then the
name/xml preflights. The planner's account-independent process validation is therefore **extracted into
one function**, `_process_component_preflight`, which the component loop and the gate both call. A pass
added to the planner is in the gate the moment it is written; there is no second copy to drift.

`planned_action` is a parameter rather than a closure read, and that is what makes the gate
account-independent: it asks the question for a fresh `"create"` — the no-collision case — so live
account contents cannot change the answer. The loop passes its real `planned_action`, so plan behaviour
is unchanged (in particular `validate_config` still does not run for a reuse step, which would otherwise
newly reject payloads that plan clean today).

A **differential oracle** still pins gate-vs-plan agreement across the preflight surface (nine mutation
shapes, plus a separate `sync_pipeline` arm, which reaches `$ref` checking only after lowering). Note
the oracle already existed when instance four landed — its input set was simply too narrow, which is the
more useful lesson than the fix itself. It compares against the **V1 twin** on purpose: on the strict
surface a clean gate verdict short-circuits the plan, so comparing there would be circular.

**One deliberate exclusion.** Name governance (#93/#102) is *not* in the gate. ADR-001 §5 scopes the
clean-plan gate to an "authored-semantics-unavailable" failure, and a process named `New Map` has
perfectly well-defined semantics — an authored view that contradicts it genuinely does conflict, so
reporting the conflict is correct rather than masking. Folding it in would also reintroduce
account-dependence, since the governance lint only flags create/create_clone steps and a reuse would
skip it. Pinned by `test_name_governance_is_deliberately_outside_the_clean_plan_gate` so the boundary
cannot drift silently.

**Ambiguity is exempt** and rejects with no live lookup at all: ADR-001 §5 counts declared authoring
actions, so it needs no process semantics and "stands even when a process's semantics are unavailable".

**The kind is RESOLVED, not read.** Every other layer resolves `process_kind or process_type` — the
plan-time gate and each builder's `validate_config`/`build`. A Codex review caught this check reading
only `process_kind`, which let a caller opt in to the strict surface, author a contradictory view, spell
the kind `process_type`, and fall through to *undecidable* while the process still built: a silent
bypass of the entire guarantee. The resolved kind is also what lands in the normal form, so the alias
spelling cannot manufacture a conflict either.

Equality traps the tests pin:

- The typed top-level dump expands every default while a nested config pipeline stays byte-compact, so a
  raw `==` between them **always** differs on identical semantics — the comparison must run on the
  lowered form.
- `map_ref`/`map_id` are one selector on **both** sides (`ProcessFlowBuilder` accepts
  `{"mode": "map_ref", "map_id": ...}`).
- **Casing is family-conditional, and delegated.** The same Codex review caught the comparison using raw
  stripped spellings, which manufactured false conflicts in two directions at once: the legacy renderer
  UPPER-cases a REST verb (`post` and `POST` emit identical XML) and LOWER-cases a non-REST connector
  type (`Database` and `database` emit identically), while PRESERVING a non-REST verb (`Send` must never
  become `SEND` — #139C's latent defect). The comparison now delegates to #139C's own
  `_canonical_connector_metadata`, which is already pinned against the legacy builder, so it agrees with
  emission by construction rather than by a re-derived table. A varying *database* verb turns out to be
  unreachable anyway: validation pins a DB source to exactly `Get`, so `get`/`GET` are the clean-plan
  gate, not a comparison.

**View-faithfulness.** On the strict surface an authored view may only describe a process the request
actually authors *and* materializes. When the single authored process resolves to reuse, the submitted
config is discarded and the reused component's own definition executes, so the view is **withheld**
(`pipeline: null`) — and the bearer is never rejected for it. The predicate mirrors the *apply-time*
branch (a declared `create` carrying an `existing_component_id` under `conflict_policy="reuse"`), not
`planned_action`: an explicit `component_id` keeps `planned_action="create"` at plan time while apply
still reuses, and reading `planned_action` alone would echo a view of a component the request never
authored. This touches only the inert view — accept-vs-reject was settled structurally beforehand, so
live account contents can never move a payload across the reject boundary (ADR-001 §5 determinism note).

**Three rejections, one code, three remediations.** Ambiguity, ordinary disagreement, and
*not-representable* share the stable `LEGACY_ADAPTER_AUTHORITY_CONFLICT` code (callers key on it) but
carry different messages. The third exists because of a live-QA finding: a process that is valid yet
has no linear representation (a `wrapper_subprocess`, a `flow_sequence`, a **wired** Try/Catch + DLQ
path) can never be matched by ANY authored view, so the ordinary "make the view semantically identical"
remediation is literally unsatisfiable there. That case says the view has no representation and points
at the workaround (author the same linear integration as `process_kind="sync_pipeline"`). This is a
*diagnostics* split only — all three were, and remain, rejections.

**Not-representable is a property of the PROCESS only.** A second QA round caught the first fix
overreaching: when the process *does* have a linear view and only the **authored view** fails to lower
(a `branch` / `decision` / `listener` / `write` stage kind, an empty stage list), blaming the process is
false, and "author it as `sync_pipeline`" is advice the caller has often already followed. That is an
ordinary disagreement — the view is wrong, and correcting it is an achievable remedy. This matters
because a mis-written view is the *likeliest* authoring mistake, so mis-classifying it would have moved
the ambiguity rather than removed it.

**One measured inertness exception.** A `reliability` block that emits no Try/Catch changes no emitted
byte — and `{retry_count: 0, dlq: {mode: "disabled"}}` is the `database_to_api_sync` archetype's own
DEFAULT shape, so treating its mere presence as a feature locked the flagship archetype's output out of
the strict surface for no semantic reason (QA proved the two variants emit byte-identical XML). The
exception is decided by the **builder's own** `_reliability_requests_try_catch` predicate — the same one
the Branch v1 composition guard uses — so the two can never drift, and it is sound only because it runs
*after* `validate_config`, which rejects the gated shapes that predicate also reports as False.

Be precise about how far that exception reaches, because the obvious stronger claim is **false**.
Unknown keys are fail-closed only at the **top level** of the process config: a new sibling block
(`unknown_future_block`, a future `flow_control`) is non-representable automatically, which is the
property the containment check exists for. *Inside* `reliability` the rule is not "unknown keys fail
closed" but **"inert iff no Try/Catch is emitted"** — so `reliability: {"bogus_key": 1}` is accepted
(measured). That outcome is correct, since such a block emits nothing and the view still describes the
emitted XML faithfully, but it means a future *catch-emitting* `reliability` key would need
`_reliability_requests_try_catch` taught about it. Sharing that one predicate with the emitter and the
Branch guard is exactly what keeps this safe: the key cannot start emitting a Try/Catch without the
predicate that gates this check learning about it at the same time.

**Error contract.** `LEGACY_ADAPTER_AUTHORITY_CONFLICT` surfaces **publicly and untranslated** — the one
deliberate exception to the adapter-family translation rule, because it is not an adapter defect on
already-validated input but a plan-time rejection of the caller's own payload on a surface with no
legacy external contract to preserve. The payload is exactly
`{_success, error_code, error, field, hint}` with `field="integration_spec.pipeline"` — deliberately
value-free (no component keys, names, ids, config, or normalized form), pinned by a leak test.

**`LEGACY_ADAPTER_PIPELINE_DRAFT_ONLY` stays reserved and unraised.** Reserved/unlowered `PipelineSpec`
kinds already fail *before mutation* with their exact `SYNC_PIPELINE_*` code/field pairs, which the #135
characterization suite freezes; re-coding them would break those pins for no behavioural gain. The #139
criterion "every reserved/unlowered PipelineSpec kind fails as draft-only before mutation" is therefore
satisfied behaviourally, by the existing codes.

**Reserved adapter hooks stay unused.** `pipeline_view` / `pipeline_view_status` on
`LegacyAdapterResultV1` remain `None` / `"not_representable"`: validating-and-preserving needs no
per-adapter derived view. They stay reserved for a future slice that genuinely needs one.

**Compatibility budget: zero.** Every authority-baseline freeze test constructs a `version="1.0"`
payload, so all 37 pass unchanged. That suite is the selector-leak detector — if any of it moves, the
strict surface has leaked into V1.

**Publication.** `get_schema_template(schema_name="IntegrationSpecV1")` and the `build_integration`
entry of `list_capabilities` both carry an `authority_versions` record (default/strict, selector path,
strict-only input form, `v1_deprecated: false`); the generated JSON schema independently shows
`enum: ["1.0","1.1"]` with `default: "1.0"`. `docs/MCP_TOOL_DESIGN.md` carries the full outcome table.
**V1 is not deprecated and emits no warning** — ADR-001 §9 permits this slice precisely because it
withdraws no V1 acceptance.

## #140 M12.5 — first-class ConnectorCall and mixed linear flow

**Landed 2026-07-25, DARK.** A new authored node kind (`connector_call`) plus its resolution and
capability layer, on the canonical `IR → CFG → plan → emit` chain #139 made production. **No public
surface changes**: no MCP tool, action, request field, `IntegrationSpecV1` field, schema-template
route, server dispatch, plan/apply behaviour, deployment behaviour, or execution behaviour. Direct
ProcessIR authoring remains #146's. The only outward addition is the conventional Python model export
`boomi_mcp.models.ConnectorCallNodeV1` and the node's presence in the *internal* ProcessIR schema —
both pinned by `tests/test_process_ir_compiler_surface.py`, which also asserts `connector_call`
reaches no MCP tool schema and no IntegrationSpec schema.

**No legacy dialect changed.** No adapter, archetype, builder, emitter, or renderer was modified;
`emitter_registry.py`, `process_emitters/rendering.py`, `process_flow_builder.py`,
`legacy_adapters/`, `_process_ir_compat.py` and `integration_builder.py` are untouched. The emitter
registry stays at 17 keys (ConnectorCall reuses the two existing connector registrations). Every
pre-existing XML golden is byte-identical; the only regenerated fixture is
`tests/fixtures/process_ir/process_ir_v1.schema.json`, which gains the new node.

**Capability matrix (closed allowlist — everything else fails closed by absence).**

| Family | Action | Status | Evidence |
|---|---|---|---|
| `officialboomi-X3979C-rest-prod` | `GET`, `PATCH` | supported | operation declares request/response profiles; `rest_fetch`/`rest_send` primitives; live `connectoraction actionType="PATCH"` capture |
| `wssoapclientsdk` | `EXECUTE` | supported | #126 reference components; request + response XML profiles; both `soap_fetch` and `soap_send` |
| `database` | `Get` | supported | `ReadProfile` is the output ("in a map, the Read profile is referenced as the source profile") |
| `database` | `Send` | supported, **terminal** | official Get-vs-Send rule + no response profile — see below |
| `database` | continuation after `Send` | **gated** | official: a Send "does not return any data to the process for further processing" |
| WSS `LISTEN` | — | **gated** | no `start_listen` emitter key; the fused legacy entry is unrepresentable |
| Database **V2** | any | **gated** | different connector, no verified builder/emitter contract; never aliased to legacy `database` |
| OEM / unrecognized subtypes | any | **gated** | e.g. the live `intappoemprod-…` REST subtype — a family is an opaque account-scoped string |
| REST `POST`/`PUT`/`DELETE`/`HEAD`/`OPTIONS`/`TRACE` | — | **gated** | no checkout-pinned process binding + cardinality capture |
| dynamic-path / parameter-bound steps | — | **gated** | only the simple binding is enabled |

Capture ledger: `.codex/plans/issue-140-live-captures.md` (read-only probes + official documentation).

**The one place #140 departs from its own issue text, and why.** The issue's representative flow is
`REST GET → Map → SOAP EXECUTE → DB Send → REST PATCH → terminal`. Official Boomi documentation states
a `Send` action "does not return any data to the process for further processing", and Database
(Legacy) declares no response profile at all — so a call *after* a Send has no documents to consume.
Per the issue's own research gate ("if any required family/action cannot be safely emitted, gate it
explicitly instead of inventing fields") that ordering ships **gated**, rejected with
`PROCESS_IR_SEMANTIC_CARDINALITY_MISMATCH`, and the representative flow is realized with the Send
last — the same five calls, the same three families, the same single map:

```
REST GET → MapRef → SOAP EXECUTE → REST PATCH → Database Send → Stop
```

Golden: `tests/fixtures/process_ir/emitter_parity/connector_call_mixed.process.xml`. It is the first
golden with **no legacy oracle** (the legacy builder cannot express a multi-connector flow); the four
substitute checks are documented in [PROCESS_IR_COMPILER_V1 §10a](PROCESS_IR_COMPILER_V1.md).

**Error codes.** Seven new codes, all `category="process_ir"`, `retryable=False`, `owner="#140"`,
added to four families that already exist (ADR-001 §7 — no eleventh family, no code re-registered):
`PROCESS_IR_REFERENCE_OPERATION_NOT_FOUND`, `PROCESS_IR_REFERENCE_CONNECTION_NOT_FOUND`,
`PROCESS_IR_REFERENCE_CONNECTION_MISMATCH`, `PROCESS_IR_CAPABILITY_CONNECTOR_ACTION_UNSUPPORTED`,
`PROCESS_IR_SEMANTIC_PROFILE_MISMATCH`, `PROCESS_IR_SEMANTIC_CARDINALITY_MISMATCH`,
`PROCESS_IR_COMPILE_CONNECTOR_BINDING_INVALID`.

**Materialization DAG untouched.** `integration_builder._topological_order` remains the sole
component-order algorithm; #140 computes only the runtime CFG and never derives component
dependencies from it. The two orders stay separate exactly as ADR-001 §1 requires.

**#139 ledger impact: none, except one row's owner.** The "ordinary `database_to_api_sync`" row still
needs canonical `start_listen` / dynamic-path / `catcherrors` / `notify` emission. #140 **settled the
listener entry policy** (unsupported — there is no emitter to cut over to) but ships no `start_listen`
emitter, so that row stays **pending-capability** and the WSS arm of `sync_pipeline` stays on the
legacy renderer.

## #141 M12.6 ledger — rich Branch/Decision bodies are DIRECT-IR ONLY

Rich control bodies extend the **new IR surface only**. The legacy `flow_sequence` dialect is
unchanged in every observable way, and the asymmetry is deliberate:

| Construct | Direct ProcessIR (#141) | Legacy `flow_sequence` |
|---|---|---|
| `connector_call` / `process_call` in a Branch leg or Decision arm | accepted (matrix + path mode) | not expressible |
| nested `decision` (in a leg or either arm) | accepted, depth ≤ 2 | still rejected |
| bare `stop` on a Decision FALSE arm | accepted | still rejected |
| control-only root (`[branch]` / `[decision]`) | accepted | not expressible |
| `branch`/`decision` terminating a `connector_call` flow | accepted | n/a |

The legacy codec (`_process_ir_compat`) and both adapters emit only the pre-#141 `steps + terminal`
shapes, so no legacy config can reach a new form and none of the accepted-set, diagnostics or
emitted bytes move. Pinned by the unchanged adapter/codec suites plus byte-identical
`branch_fanout.xml`, `decision_conditional.xml`, `flow_sequence_decision_branch_map.xml` and
`process_ir/emitter_parity/control_flow.process.xml`.

`tests/fixtures/process_ir/process_ir_v1.schema.json` was regenerated (a **reviewed** regeneration,
per §5 of PROCESS_IR_V1): the diff adds and removes no `$defs`, touching only `BranchLegV1`,
`DecisionTrueArmV1`, `DecisionFalseArmV1` and `SequenceNodeV1`. The three golden *documents* in
`process_ir_v1.json` are byte-identical, which is the backward-compatibility signal.


## #142 M12.7 ledger — scoped Try/Catch is DIRECT-IR ONLY

Scoped error handling extends the **new IR surface only**. The legacy `flow_sequence` dialect and
`ProcessFlowBuilder`'s own `reliability` path are unchanged in every observable way.

| Construct | Direct ProcessIR (#142) | Legacy `flow_sequence` / builder |
|---|---|---|
| `try_catch` node (process or connector scope) | accepted | not expressible; builder keeps its own `reliability` config |
| bounded `retry.count` 0–5 | accepted, **safety-checked** | builder validator keeps its existing 0–5 range check, unchanged |
| typed `idempotency` evidence on a call | accepted | not expressible |
| positive retry over a write | **rejected** (no row ships as replay-safe) | builder behaviour unchanged — #142 adds no new rejection to the legacy path |
| catch body as an ordinary IR body | accepted | fixed legacy catch layouts only |

**The new safety rules apply to new ProcessIR retry intent only.** The issue's backward-compatibility
requirement is explicit that legacy behaviour changes only through a separately documented safety fix
with its own compatibility analysis — so no existing `catch-errors`/Try/Catch request became invalid,
and no legacy diagnostic moved.

**Byte compatibility.** All 11 pre-existing `catchAll="true"` goldens are byte-identical, verified by
`git diff --exit-code --diff-filter=MDR <baseline> -- tests/fixtures/golden_xml
tests/fixtures/process_ir/emitter_parity` (additive fixtures permitted; any modification, deletion or
rename fails). `render_catcherrors` gained documentation only — no output change — and is now shared
between the legacy adapters and the ProcessIR registry, which is precisely why nothing moved.

`tests/fixtures/process_ir/process_ir_v1.schema.json` was regenerated (a **reviewed** regeneration,
per §5 of PROCESS_IR_V1). The diff is exactly:

- **added `$defs`** — `TryCatchNodeV1`, `TryCatchTryBodyV1`, `TryCatchCatchBodyV1`, `RetryPolicyV1`,
  `VerifiedActionIdempotencyV1`, `KeyReferenceIdempotencyV1`;
- **removed `$defs`** — none;
- **changed `$defs`** — `ConnectorCallNodeV1` (gains the optional `idempotency` field) and
  `SequenceNodeV1` (its step discriminator gains `try_catch`).

Nothing was removed or narrowed, so no previously-valid document became invalid. The three golden
*documents* in `process_ir_v1.json` and the compiler fixtures in `process_ir_compiler_v1.json` are
byte-identical, which is the backward-compatibility signal.

**Surface stays dark.** No ProcessIR field was added to `build_integration`; #146 owns that
boundary. The MCP-surface guard test additionally forbids `IdempotencyContractSymbolV1`,
`TryCatchSemanticV1`, `CatchErrorsInputV1`, `ErrorRegionV1` and `retry_safety` from every tool
schema and description — exported-to-the-compiler and visible-to-an-LLM are separate questions.

**Reservations retained.** Listener error scopes, queue/Event-Streams topology, nested Try/Catch and
failure-trigger selection all stay out; see PROCESS_IR_V1 §8 for which are `gated` ("not yet") and
which are `unsupported` ("never"), and `.codex/plans/issue-142-live-captures.md` for the evidence
behind each.


---

## 7. #143 M12.8 — unified semantic validation (migration ledger)

Added by [#143](https://github.com/RenEra-ai/boomi-mcp-server/issues/143). Discharges the acceptance
criterion "each existing validator is accounted for in a migration matrix with soundness decision and
regression evidence". Contract: [PROCESS_IR_SEMANTIC_VALIDATION_V1](./PROCESS_IR_SEMANTIC_VALIDATION_V1.md).

Decisions come from a closed set: `port-unchanged`, `refine-with-typed-facts`, `adapter-only-compat`,
`retire-with-regression-fixture`.

### 7.1 Validator migration matrix

| Validator (file:symbol) | What it checks today | Error code(s) it raises | Sound? / Complete? | Decision | Regression evidence (test file::test or golden) | Exemption owner + removal gate |
|---|---|---|---|---|---|---|
| `src/boomi_mcp/compiler/process_ir/body_capabilities.py:validate_body_capabilities` | Runs placement and closed body/depth checks. | `PROCESS_IR_CAPABILITY_ERROR_SCOPE_UNSUPPORTED`, `PROCESS_IR_CAPABILITY_NODE_NOT_ALLOWED_IN_BODY`, `PROCESS_IR_SEMANTIC_CATCH_UNTERMINATED`, `PROCESS_IR_SEMANTIC_NESTING_LIMIT` | Sound for V1; fail-fast. | port-unchanged | `tests/test_process_ir_rich_control_bodies.py::test_the_mixing_gate_holds_on_the_COMPILER_entry_point_too` | None; keep facade until collector parity. |
| `src/boomi_mcp/compiler/process_ir/body_capabilities.py:_check_try_catch_placement` | Enforces last/root/scope placement. | `PROCESS_IR_CAPABILITY_ERROR_SCOPE_UNSUPPORTED` | Sound/complete for verified placements. | port-unchanged | `tests/test_process_ir_error_handling.py::test_mutated_placement_is_rejected_by_the_compiler` | None; widen only with capability evidence. |
| `src/boomi_mcp/compiler/process_ir/body_capabilities.py:_walk_body` | Rechecks slot allowlists and connector/ProcessCall path mixing. | Body, scope, catch, nesting codes above | Sound for admitted bodies; fail-fast. | port-unchanged | `tests/test_process_ir_rich_control_bodies.py::test_the_mixing_gate_propagates_in_BOTH_directions` | None. |
| `src/boomi_mcp/compiler/process_ir/body_capabilities.py:_walk_control` | Dispatches controls and counts Branch/Decision depth. | `PROCESS_IR_SEMANTIC_NESTING_LIMIT` plus propagated codes | Sound for V1 control trees. | port-unchanged | `tests/test_process_ir_rich_control_bodies.py::test_depth_at_limit_plus_one_is_rejected_before_any_cfg_exists` | None. |
| `src/boomi_mcp/compiler/process_ir/body_capabilities.py:_walk_try_catch` | Rechecks try/catch terminals, scopes, topology, and slots. | `PROCESS_IR_SEMANTIC_CATCH_UNTERMINATED`, `PROCESS_IR_CAPABILITY_ERROR_SCOPE_UNSUPPORTED`, `PROCESS_IR_CAPABILITY_NODE_NOT_ALLOWED_IN_BODY` | Sound/complete for current vocabulary. | port-unchanged | `tests/test_process_ir_error_handling.py::test_mutated_away_catch_terminal_is_rejected_by_the_compiler` | None. |
| `src/boomi_mcp/compiler/process_ir/body_capabilities.py:is_allowed` | Closed `(context, slot, kind)` lookup. | None | Sound for admitted rows; absence deliberately denies. | port-unchanged | `tests/test_process_ir_rich_control_bodies.py::test_registry_lookup_is_absence_as_denial` | None. |
| `src/boomi_mcp/compiler/process_ir/body_capabilities.py:_check` | Converts denied lookup to a stable path diagnostic. | `PROCESS_IR_CAPABILITY_NODE_NOT_ALLOWED_IN_BODY` | Complete relative to registry. | port-unchanged | `tests/test_process_ir_rich_control_bodies.py::test_registry_rejects_a_wrong_context_node_before_lowering` | None. |
| `src/boomi_mcp/compiler/process_ir/invariants.py:check_cfg_invariants` | CFG identity/order/entry/flow/reachability/terminal/control/error-region integrity. | Existing semantic and compile invariant codes | Sound checks but mixes authored findings with compiler assertions and fails fast. | refine-with-typed-facts | `tests/test_process_ir_compiler_invariants.py::test_unreachable_node_is_a_semantic_defect`; `::test_duplicate_node_id_is_a_compiler_defect` | None; retain low-level oracle. |
| `src/boomi_mcp/compiler/process_ir/invariants.py:check_emission_plan_invariants` | Plan/CFG correspondence, symbols, geometry, wiring, transitions, terminals. | Propagated invariant codes; `PROCESS_IR_COMPILE_CONTROL_WIRING_INVALID`; `PROCESS_IR_COMPILE_EMISSION_PLAN_INVALID` | Sound post-lowering oracle, not semantic validation. | port-unchanged | `tests/test_process_ir_compiler_invariants.py::test_plan_cfg_correspondence_is_enforced` | None; permanent compiler oracle. |
| `src/boomi_mcp/compiler/process_ir/invariants.py:_check_region_containment` | Ensures control/error subtrees remain inside provenance regions. | `PROCESS_IR_SEMANTIC_AMBIGUOUS_FLOW` or caller-supplied `PROCESS_IR_COMPILE_ERROR_REGION_INVALID` | Sound after endpoint/tree prerequisites. | port-unchanged | `tests/test_process_ir_error_handling.py::test_region_escape_reports_the_same_code_at_every_depth` | None. |
| `src/boomi_mcp/compiler/process_ir/invariants.py:_check_every_control_path_terminates` | Every divergent path must reach an exit. | `PROCESS_IR_SEMANTIC_UNTERMINATED_PATH` | Sound/complete for acyclic join-free CFG. | port-unchanged | `tests/test_process_ir_rich_control_bodies.py::test_an_unterminated_control_leg_reports_its_own_code` | None. |
| `src/boomi_mcp/compiler/process_ir/invariants.py:_check_control_depth` | Re-derives nesting depth from CFG. | `PROCESS_IR_SEMANTIC_NESTING_LIMIT` | Sound defense in depth. | port-unchanged | `tests/test_process_ir_rich_control_bodies.py::test_the_depth_bound_is_re_derived_from_the_cfg` | None. |
| `src/boomi_mcp/compiler/process_ir/connector_resolution.py:validate_connector_calls` | Orchestrates binding, retry, cardinality, and profile checks. | Existing #140/#142 reference, capability, semantic, and error-region codes | Sound for current rows; incomplete and fail-fast. | refine-with-typed-facts | `tests/test_connector_call_mixed_flow.py::test_a_call_after_a_non_producing_send_is_rejected` | None; facade remains until prepared-fact migration. |
| `src/boomi_mcp/compiler/process_ir/connector_resolution.py:resolve_connector_call_bindings` | Resolves operation, canonical action/family, capability, connection, and profiles. | `PROCESS_IR_REFERENCE_OPERATION_NOT_FOUND`, `PROCESS_IR_REFERENCE_CONNECTION_NOT_FOUND`, `PROCESS_IR_REFERENCE_CONNECTION_MISMATCH`, `PROCESS_IR_CAPABILITY_CONNECTOR_ACTION_UNSUPPORTED` | Sound for supplied symbols; first-error only. | refine-with-typed-facts | `tests/test_connector_call_resolution.py::test_the_capability_gate_is_settled_before_the_connection` | None. |
| `src/boomi_mcp/compiler/process_ir/connector_resolution.py:validate_connector_call_semantics` | Validates declared profiles then path-local map/cardinality behavior. | `PROCESS_IR_SEMANTIC_PROFILE_MISMATCH`, `PROCESS_IR_SEMANTIC_CARDINALITY_MISMATCH` | Sound; deliberately no blanket adjacent-call equality. | refine-with-typed-facts | `tests/test_connector_call_mixed_flow.py::test_a_declared_profile_ref_must_resolve_on_every_call_not_only_beside_a_map` | None. |
| `src/boomi_mcp/compiler/process_ir/connector_resolution.py:_walk_paths` | DFS over path-local producer/block/map/catch state. | `PROCESS_IR_SEMANTIC_CARDINALITY_MISMATCH`, `PROCESS_IR_SEMANTIC_PROFILE_MISMATCH` | Sound for current facts; incomplete for unified lineage/effects. | refine-with-typed-facts | `tests/test_process_ir_rich_control_bodies.py::test_a_send_in_one_leg_does_not_block_its_sibling` | None. |
| `src/boomi_mcp/compiler/process_ir/connector_resolution.py:_check_map_pair` | Exact upstream-output/map-input and map-output/downstream-input matching. | `PROCESS_IR_SEMANTIC_PROFILE_MISMATCH` | Sound when all typed profiles resolve. | refine-with-typed-facts | `tests/test_connector_call_mixed_flow.py::test_map_source_profile_must_match_the_preceding_calls_output` | None. |
| `src/boomi_mcp/compiler/process_ir/connector_resolution.py:_profile_identity` | Recognizes closed profile component types and canonical identity. | None | Sound for supported profiles; intentionally closed. | refine-with-typed-facts | `tests/test_connector_call_mixed_flow.py::test_every_real_boomi_profile_kind_is_accepted_as_a_profile` | None. |
| `src/boomi_mcp/compiler/process_ir/error_handling.py:validate_error_handling` | Retry regions, source reexecution, replay safety, evidence, contract matching. | `PROCESS_IR_COMPILE_ERROR_REGION_INVALID` and existing #142 semantic codes | Sound for connectors; incomplete for non-connector effects. | refine-with-typed-facts | `tests/test_process_ir_error_handling.py::test_conditionally_idempotent_requires_a_matching_key_contract` | None. |
| `src/boomi_mcp/compiler/process_ir/error_handling.py:derive_error_regions` | Derives/validates disjoint try/catch subtrees and ordered wiring. | `PROCESS_IR_COMPILE_ERROR_REGION_INVALID` | Sound structural oracle. | port-unchanged | `tests/test_process_ir_error_handling.py::test_swapped_try_catch_local_ordinals_are_a_region_defect` | None. |
| `src/boomi_mcp/compiler/process_ir/error_handling.py:catch_region_node_ids` | Unions derived catch subtree IDs. | Propagates `PROCESS_IR_COMPILE_ERROR_REGION_INVALID` | Complete relative to region derivation. | port-unchanged | `tests/test_process_ir_error_handling.py::test_catch_row_geometry_matches_the_shared_renderer_constants` | None. |
| `src/boomi_mcp/compiler/process_ir/error_handling.py:_producers_upstream_of` | Detects producers strictly upstream of retry region. | `PROCESS_IR_COMPILE_ERROR_REGION_INVALID` on malformed CFG | Sound for connector-only model; incomplete for richer sources. | refine-with-typed-facts | `tests/test_process_ir_error_handling.py::test_source_isolation_is_derived_from_the_graph_not_the_authored_scope` | None. |
| `src/boomi_mcp/compiler/process_ir/error_handling.py:_require_resolvable` | Requires read-only key evidence to resolve and bind same operation. | `PROCESS_IR_SEMANTIC_IDEMPOTENCY_EVIDENCE_MISSING` | Sound/complete for typed evidence. | port-unchanged | `tests/test_process_ir_error_handling.py::test_read_only_needs_no_evidence_but_a_dangling_contract_still_fails` | None. |
| `src/boomi_mcp/compiler/process_ir/error_handling.py:_collect_subtree` | Rejects error-region overlap/re-entry/cycles during traversal. | `PROCESS_IR_COMPILE_ERROR_REGION_INVALID` | Sound structural helper. | port-unchanged | `tests/test_process_ir_error_handling.py::test_a_region_contains_only_its_own_try_subtree` | None. |
| `src/boomi_mcp/compiler/process_ir/connector_capabilities.py:lookup_capability` | Closed family/action capability lookup. | None | Sound for admitted rows; absence denies. | port-unchanged | `tests/test_connector_call_resolution.py::test_gated_and_unknown_pairs_are_absent` | None. |
| `src/boomi_mcp/compiler/process_ir/connector_capabilities.py:canonicalize_connector_metadata` | Canonical family aliases and action spelling. | None | Sound for builder-supported aliases. | port-unchanged | `tests/test_connector_call_resolution.py::test_canonicalization_resolves_aliases_and_preserves_action_spelling` | None. |
| `src/boomi_mcp/compiler/process_ir/connector_capabilities.py:_rows` | Builds immutable registry and rejects duplicates. | Raw `ValueError` | Sound duplicate check. | port-unchanged | `tests/test_connector_call_resolution.py::test_registry_is_immutable_and_closed` | None. |
| `src/boomi_mcp/models/process_ir.py:_validate_component_ref` | Exact `$ref:KEY` or trimmed literal syntax. | `PROCESS_IR_REFERENCE_INVALID_FORMAT` | Complete for syntax, not resolution. | port-unchanged | `tests/test_process_ir_models.py::test_reference_syntax_rejected` | None. |
| `src/boomi_mcp/models/process_ir.py:_validate_contract_ref` | Idempotency evidence must be exact `$ref:KEY`. | `PROCESS_IR_REFERENCE_INVALID_FORMAT` | Sound provenance boundary. | port-unchanged | `tests/test_process_ir_error_handling.py::test_a_contract_ref_must_be_a_reference_not_an_assertion` | None. |
| `src/boomi_mcp/models/process_ir.py:_keyed_cache_true_only` | Gates keyed/indexed cache forms. | `PROCESS_IR_CAPABILITY_UNSUPPORTED` | Complete for current capability. | port-unchanged | `tests/test_process_ir_models.py::test_keyed_cache_literal_false_is_capability_gated` | None. |
| `src/boomi_mcp/models/process_ir.py:_use_cache_true_only` | Requires script compilation cache. | Effective `PROCESS_IR_SCHEMA_INVALID` | Complete locally. | port-unchanged | `tests/test_process_ir_models.py::test_use_cache_false_rejected` | None. |
| `src/boomi_mcp/models/process_ir.py:ProfilePropertySourceV1._non_blank` | Profile element identifiers/type nonblank. | `PROCESS_IR_SCHEMA_INVALID_CARDINALITY` | Complete locally. | port-unchanged | `tests/test_process_ir_models.py::test_whitespace_only_profile_source_identifiers_rejected` | None. |
| `src/boomi_mcp/models/process_ir.py:DdpPropertySourceV1._non_blank` | DDP source name nonblank. | `PROCESS_IR_SCHEMA_INVALID_CARDINALITY` | Complete locally. | port-unchanged | `tests/test_process_ir_models.py::test_whitespace_only_property_source_name_rejected` | None. |
| `src/boomi_mcp/models/process_ir.py:DppPropertySourceV1._non_blank` | DPP source name nonblank. | `PROCESS_IR_SCHEMA_INVALID_CARDINALITY` | Complete locally; dedicated negative missing. | port-unchanged | New `tests/test_process_ir_semantic_validation.py::test_model_validator_parity_for_blank_dpp_source` | None. |
| `src/boomi_mcp/models/process_ir.py:CustomScriptingOpV1._script_non_blank` | Script source nonblank. | `PROCESS_IR_SCHEMA_INVALID_CARDINALITY` | Local only; no effect claim. | port-unchanged | New `tests/test_process_ir_semantic_validation.py::test_model_validator_parity_for_blank_script` | None. |
| `src/boomi_mcp/models/process_ir.py:SplitDocumentsOpV1._non_blank` | Split link identifiers nonblank. | `PROCESS_IR_SCHEMA_INVALID_CARDINALITY` | Complete locally; dedicated negative missing. | port-unchanged | New `tests/test_process_ir_semantic_validation.py::test_model_validator_parity_for_blank_split_identifier` | None. |
| `src/boomi_mcp/models/process_ir.py:CombineDocumentsOpV1._non_blank` | Combine link identifiers/target nonblank. | `PROCESS_IR_SCHEMA_INVALID_CARDINALITY` | Complete locally. | port-unchanged | `tests/test_process_ir_models.py::test_whitespace_only_dataprocess_identifiers_rejected` | None. |
| `src/boomi_mcp/models/process_ir.py:TrackOperandV1._property_id_non_blank` | Decision property ID nonblank. | `PROCESS_IR_SCHEMA_INVALID_CARDINALITY` | Local only; lineage later. | port-unchanged | New `tests/test_process_ir_semantic_validation.py::test_model_validator_parity_for_blank_track_id` | None. |
| `src/boomi_mcp/models/process_ir.py:ConnectorCallNodeV1._action_non_blank` | Optional action assertion nonblank. | `PROCESS_IR_SCHEMA_INVALID_CARDINALITY` | Complete locally. | port-unchanged | `tests/test_process_ir_models.py::test_connector_call_blank_action_is_rejected` | None. |
| `src/boomi_mcp/models/process_ir.py:SetDdpNodeV1._name_rules` | Bare DDP name syntax/prefix ownership. | `PROCESS_IR_SCHEMA_INVALID_CARDINALITY`, `PROCESS_IR_CAPABILITY_UNSUPPORTED` | Sound local boundary. | port-unchanged | `tests/test_process_ir_models.py::test_property_name_rules` | None. |
| `src/boomi_mcp/models/process_ir.py:SetDppNodeV1._name_rules` | Bare DPP name syntax/prefix ownership. | Same two codes | Sound locally; dedicated negative missing. | port-unchanged | New `tests/test_process_ir_semantic_validation.py::test_model_validator_parity_for_set_dpp_name` | None. |
| `src/boomi_mcp/models/process_ir.py:ExceptionNodeV1._placeholder_rules` | Nonblank template and required `{1}` binding. | `PROCESS_IR_SCHEMA_INVALID_CARDINALITY` | Complete for renderer. | port-unchanged | `tests/test_process_ir_models.py::test_exception_placeholder_required_when_binding` | None. |
| `src/boomi_mcp/models/process_ir.py:BranchLegV1._leg_rules` | Cache ordering, ProcessCall path mode, nonempty Stop leg. | `PROCESS_IR_SCHEMA_INVALID_CARDINALITY`, `PROCESS_IR_CAPABILITY_NODE_NOT_ALLOWED_IN_BODY` | Sound for V1 grammar. | port-unchanged | `tests/test_process_ir_models.py::test_process_call_allowed_in_a_branch_leg_only_in_path_mode` | None. |
| `src/boomi_mcp/models/process_ir.py:DecisionTrueArmV1._arm_rules` | Cache ordering, ProcessCall path mode, nonempty Stop. | Same two codes | Sound for V1 true arm. | port-unchanged | `tests/test_process_ir_rich_control_bodies.py::test_a_decision_arm_admits_at_most_one_process_call` | None. |
| `src/boomi_mcp/models/process_ir.py:DecisionFalseArmV1._arm_rules` | Cache consumption/trailing-before-Stop allowance. | `PROCESS_IR_SCHEMA_INVALID_CARDINALITY` | Sound for false arm. | port-unchanged | `tests/test_process_ir_models.py::test_false_arm_trailing_cache_put_allowed_only_before_stop` | None. |
| `src/boomi_mcp/models/process_ir.py:TryCatchTryBodyV1._try_body_rules` | Cache consumption/trailing placement. | `PROCESS_IR_SCHEMA_INVALID_CARDINALITY` | Sound locally; targeted negative missing. | port-unchanged | New `tests/test_process_ir_semantic_validation.py::test_model_validator_parity_for_try_trailing_cache_put` | None. |
| `src/boomi_mcp/models/process_ir.py:TryCatchCatchBodyV1._catch_body_rules` | Catch cache placement and rejects no-op Stop. | `PROCESS_IR_SCHEMA_INVALID_CARDINALITY` | Sound for recovery body. | port-unchanged | `tests/test_process_ir_error_handling.py::test_a_bare_stop_catch_body_recovers_nothing_and_is_rejected` | None. |
| `src/boomi_mcp/models/process_ir.py:TryCatchNodeV1._try_catch_rules` | Verified connector/process-scope topology. | `PROCESS_IR_CAPABILITY_ERROR_SCOPE_UNSUPPORTED` | Complete for two supported scopes. | port-unchanged | `tests/test_process_ir_error_handling.py::test_process_scope_try_body_must_begin_with_the_producing_call` | None. |
| `src/boomi_mcp/models/process_ir.py:SequenceNodeV1._sequence_rules` | Root grammar, ordering, terminals, controls, maps, cache, mixing. | Existing schema/capability/control-continuation codes | Sound local grammar; not resolved semantics. | port-unchanged | `tests/test_process_ir_models.py::test_sequence_ordering_rules` | None. |
| `src/boomi_mcp/models/process_ir.py:_find_secret_shaped_key` | Recursive secret-shaped-key scan. | `PROCESS_IR_CAPABILITY_UNSUPPORTED` through parse | Conservative security boundary. | port-unchanged | `tests/test_process_ir_models.py::test_secret_shaped_key_rejected_without_echo` | None. |
| `src/boomi_mcp/models/process_ir.py:_require_non_blank` | Shared stripped-nonblank check. | `PROCESS_IR_SCHEMA_INVALID_CARDINALITY` | Complete for supplied fields. | port-unchanged | `tests/test_process_ir_models.py::test_whitespace_only_profile_source_identifiers_rejected` | None. |
| `src/boomi_mcp/models/process_ir.py:_validate_bare_property_name` | Shared property name/prefix check. | `PROCESS_IR_SCHEMA_INVALID_CARDINALITY`, `PROCESS_IR_CAPABILITY_UNSUPPORTED` | Sound local boundary. | port-unchanged | `tests/test_process_ir_models.py::test_property_name_rules` | None. |
| `src/boomi_mcp/models/process_ir.py:_check_cache_put_followed_by_read` | Cache write must be followed by stream replacement. | `PROCESS_IR_SCHEMA_INVALID_CARDINALITY` | Sound within supplied list. | port-unchanged | `tests/test_process_ir_models.py::test_sequence_ordering_rules` | None. |
| `src/boomi_mcp/models/process_ir.py:_check_process_call_path_mode` | ProcessCall-only body, Stop terminal, optional count. | `PROCESS_IR_CAPABILITY_NODE_NOT_ALLOWED_IN_BODY` | Sound for verified mode. | port-unchanged | `tests/test_process_ir_models.py::test_process_call_allowed_in_a_branch_leg_only_in_path_mode` | None. |
| `src/boomi_mcp/models/process_ir.py:_check_stop_terminal_has_work` | Rejects no-op Stop in selected bodies. | `PROCESS_IR_SCHEMA_INVALID_CARDINALITY` | Sound fail-closed V1 rule. | port-unchanged | `tests/test_process_ir_models.py::test_branch_leg_and_true_arm_still_reject_a_bare_stop` | None. |
| `src/boomi_mcp/models/process_ir.py:_walk_controls` | Whole-path depth and connector/ProcessCall mixing. | `PROCESS_IR_SEMANTIC_NESTING_LIMIT`, `PROCESS_IR_CAPABILITY_NODE_NOT_ALLOWED_IN_BODY` | Complete for its two properties. | port-unchanged | `tests/test_process_ir_rich_control_bodies.py::test_process_call_mixing_is_checked_along_the_whole_path_not_just_the_root` | None. |
| `src/boomi_mcp/models/process_ir.py:_check_whole_document_rules` | Seeds whole-control walk. | Same two codes | Complete orchestration. | port-unchanged | `tests/test_process_ir_rich_control_bodies.py::test_depth_at_limit_plus_one_is_rejected_before_any_cfg_exists` | None. |
| `src/boomi_mcp/models/process_ir.py:_translate_pydantic_error` | Safe, stable Pydantic-to-ProcessIR translation. | Existing model/schema/reference/capability codes | Complete for current Pydantic vocabulary. | port-unchanged | `tests/test_process_ir_models.py::test_diagnostics_sorted_and_deterministic` | None. |
| `src/boomi_mcp/models/process_ir.py:parse_process_ir_v1` | Object, secret, version, strict model, translation, whole-doc checks. | Existing model/local codes | Complete local boundary; incomplete for resolved semantics. | port-unchanged | `tests/test_process_ir_models.py::test_version_gate`; `::test_diagnostics_sorted_and_deterministic` | None. |
| `src/boomi_mcp/models/process_ir.py:_kinds_of` | Derives union discriminator kinds for registry parity. | None | Sound reflection helper. | port-unchanged | `tests/test_process_ir_rich_control_bodies.py::test_registry_step_rows_match_the_model_unions` | None. |
| `src/boomi_mcp/models/process_ir.py:TryCatchNodeV1.retry_count` | Normalizes absent retry to zero. | None | Complete normalization fact. | port-unchanged | `tests/test_process_ir_error_handling.py::test_absent_retry_is_exactly_retry_zero` | None. |
| `src/boomi_mcp/models/process_ir.py:_loc_to_path` | Converts Pydantic locations to authored RFC 6901 paths. | None | Sound for current location shapes. | port-unchanged | `tests/test_process_ir_models.py::test_unknown_field_on_nested_arm_pins_pointer` | None. |
| `src/boomi_mcp/models/process_ir.py:_nesting_error` | Dead custom-error constructor. | None at runtime | Redundant; zero callers. | retire-with-regression-fixture | `tests/test_process_ir_rich_control_bodies.py::test_depth_at_limit_plus_one_is_rejected_before_any_cfg_exists` | Owner #143; remove only with identical active depth code/path. |
| `src/boomi_mcp/models/process_ir.py:_control_depth` | Dead recursive depth helper. | None | Duplicates active authored/CFG walks; zero callers. | retire-with-regression-fixture | `tests/test_process_ir_rich_control_bodies.py::test_the_depth_bound_is_re_derived_from_the_cfg` | Owner #143; remove only while both depth walks remain pinned. |
| `src/boomi_mcp/models/pipeline_models.py:StageSpec._validate_config_xor_component_ref` | At-most-one of config/component_ref. | Pydantic `value_error` | Sound for legacy StageSpec. | adapter-only-compat | `tests/test_pipeline_models.py::test_stage_config_and_component_ref_xor_rejected` | PipelineSpec owner; ADR §9 retirement only. |
| `src/boomi_mcp/models/pipeline_models.py:PipelineSpec._classify_edges` | Keys/endpoints/cycles/loop closure, generic write→read, catch/retry metadata. | Pydantic `value_error` | Structural parts sound; generic write→read globally unsound. | adapter-only-compat | `tests/test_pipeline_models.py::test_pipeline_write_before_read_ordering_rejected`; `::test_pipeline_read_before_write_ordering_allowed` | #143/PipelineSpec; never port generic rule. |
| `src/boomi_mcp/models/pipeline_models.py:_has_cycle` | Non-loop directed cycle detection. | None | Sound/complete for graph. | adapter-only-compat | `tests/test_pipeline_models.py::test_untyped_cycle_rejected_when_edge_kind_defaults_to_ordering` | PipelineSpec retirement gate. |
| `src/boomi_mcp/models/pipeline_models.py:_reachable` | Tests loop-back closure over forward path. | None | Sound/complete for graph. | adapter-only-compat | `tests/test_pipeline_models.py::test_loop_back_that_does_not_close_forward_path_is_rejected` | PipelineSpec retirement gate. |
| `src/boomi_mcp/categories/components/builders/cache_property_lineage.py:_property_read_from_operand` | Records lenient Decision DDP/DPP reads. | None directly | Intentionally incomplete/lenient. | adapter-only-compat | `tests/test_cache_property_lineage.py::test_decision_operand_read_without_writer_is_lenient` | `LEGACY_ADAPTER_EXEMPTION_DECISION_PROPERTY_READ`; remove after explicit initialization/default migration. |
| `src/boomi_mcp/categories/components/builders/cache_property_lineage.py:_walk_set_properties_step` | Records source reads before DDP/DPP write. | None | Sound explicit order; incomplete for opaque effects. | refine-with-typed-facts | `tests/test_cache_property_lineage.py::test_one_to_one_trunk_write_then_read_passes` | None; legacy helper remains for precedence. |
| `src/boomi_mcp/categories/components/builders/cache_property_lineage.py:_add_map_join_reads` | Adds in-spec map cache-join reads. | None | Sound with component context; incomplete for literal maps. | refine-with-typed-facts | `tests/test_integration_builder.py::TestMapJoinCacheReaderLineage::test_joined_map_without_cache_writer_rejected` | Remove after verified map effects cover fixtures. |
| `src/boomi_mcp/categories/components/builders/cache_property_lineage.py:_walk_steps` | Walks legacy order/paths; treats map/script as wildcard property writers that may satisfy but never condemn. | None directly | Explicit path facts partly sound; wildcard proof deliberately unsound compatibility behavior. | refine-with-typed-facts | `tests/test_cache_property_lineage.py::test_collect_events_orders_and_paths`; `::test_wildcard_writer_never_condemns_a_read` | `LEGACY_ADAPTER_EXEMPTION_OPAQUE_STATE_WRITER`; remove after typed effects cover frozen fixtures. |
| `src/boomi_mcp/categories/components/builders/cache_property_lineage.py:collect_lineage_events` | Collects legacy transform/sequence/dynamic-path events. | None | Complete only for documented legacy vocabulary. | adapter-only-compat | `tests/test_cache_property_lineage.py::test_dynamic_path_events_collected_for_legacy_configs` | #143; external-writer/typed-effect migration gate. |
| `src/boomi_mcp/categories/components/builders/cache_property_lineage.py:_writer_visible` | Prior order, Decision implication, DDP Branch scope, DPP/cache sequential legs. | None | Sound for explicit events; incomplete for TryCatch/ProcessCall. | refine-with-typed-facts | `tests/test_cache_property_lineage.py::test_ddp_written_in_sibling_leg_fails_scope` | None. |
| `src/boomi_mcp/categories/components/builders/cache_property_lineage.py:validate_cache_property_lineage` | First-error cache/property writer, DDP scope, leg order, exclusive-path checks. | Five `PROCESS_LINEAGE_*` codes | Named-writer results sound; wildcard proof compatibility-only. | refine-with-typed-facts | `tests/test_cache_property_lineage.py::test_property_read_before_any_write_fails_strict` | Wildcard downgrade owned by #143 exemption. |
| `src/boomi_mcp/categories/components/builders/cache_property_lineage.py:validate_config_lineage` | Collects then validates legacy lineage. | Same five `PROCESS_LINEAGE_*` codes | Sound only for legacy surface. | adapter-only-compat | `tests/test_integration_builder.py::TestSyncPipelineJoinedMapLineage::test_sync_pipeline_joined_map_without_writer_rejected` | Remove after typed facts plus frozen outcome/XML parity. |
| `src/boomi_mcp/categories/components/process_graph_verifier.py:_is_terminal` | Classifies emitted XML terminals. | None | Sound for recognized XML; not semantic. | port-unchanged | `tests/test_process_graph_verifier.py::test_returndocuments_terminal_is_clean` | None; permanent post-emission oracle. |
| `src/boomi_mcp/categories/components/process_graph_verifier.py:verify_process_graph` | Parses XML and checks IDs, edges, dead ends, terminals, Start, reachability, and warnings. | Existing `PROCESS_XML_*`/graph inline codes | Sound for enumerated XML invariants; semantically incomplete. | port-unchanged | `tests/test_process_graph_verifier.py::test_valid_linear_process_is_clean`; `::test_orphan_unreachable_shape` | None; failures remain `PROCESS_IR_COMPILE_VERIFIER_FAILED`. |
| `src/boomi_mcp/categories/components/builders/process_flow_builder.py:ProcessFlowBuilder.validate_config` | Legacy top-level syntax/order/precedence gate. | `PROCESS_KIND_UNSUPPORTED` plus child codes | Sound for legacy vocabulary; not canonical semantics. | adapter-only-compat | `tests/test_process_flow_builder.py::TestValidateConfig::test_passes_on_minimal_valid_config` | Retain until legacy public contract retires. |
| `src/boomi_mcp/categories/components/builders/process_flow_builder.py:_validate_flow_sequence_config` | Composed flow structure, siblings, bindings, reliability, terminal, nested sequence. | `PROCESS_FLOW_SEQUENCE_CONFIG_INVALID` plus child codes | Sound for frozen legacy lowering. | adapter-only-compat | `tests/test_process_flow_builder.py::test_flow_sequence_rejects_sibling_transform` | Legacy dialect retirement gate. |
| `src/boomi_mcp/categories/components/builders/process_flow_builder.py:_validate_flow_sequence_steps` | List, terminal/control positions, nesting, cache consumption. | `PROCESS_FLOW_SEQUENCE_CONFIG_INVALID` plus child codes | Sound for legacy grammar. | adapter-only-compat | `tests/test_process_flow_builder.py::test_flow_sequence_rejects_control_not_last` | Legacy dialect retirement gate. |
| `src/boomi_mcp/categories/components/builders/process_flow_builder.py:_validate_flow_sequence_step` | Step object/kind/key/label dispatch. | Legacy step-family codes | Complete for declared legacy allowlists. | adapter-only-compat | `tests/test_process_flow_builder.py::test_flow_sequence_rejects_unknown_kind` | Retain for code precedence. |
| `src/boomi_mcp/categories/components/builders/process_flow_builder.py:_validate_sequence_linear_step` | Reprojects linear steps to existing validators. | Existing shape-specific codes | Sound for parity only. | adapter-only-compat | `tests/test_process_flow_builder.py::test_m10_load_retrieve_remove_chain_still_valid` | Retire with dialect. |
| `src/boomi_mcp/categories/components/builders/process_flow_builder.py:_validate_sequence_doccacheload_step` | Requires cache selector. | `PROCESS_FLOW_SEQUENCE_CONFIG_INVALID` | Sound local syntax. | adapter-only-compat | `tests/test_process_flow_builder.py::test_flow_sequence_rejects_doccacheload_missing_cache_id` | Retire with dialect. |
| `src/boomi_mcp/categories/components/builders/process_flow_builder.py:_validate_property_source_value` | Typed source keys/fields and `definedparameter` gate. | `PROCESS_PROPERTY_SOURCE_INVALID` | Sound for verified legacy vocabulary. | adapter-only-compat | `tests/test_process_flow_builder.py::test_set_properties_step_definedparameter_source_gated` | Capability expansion requires separate evidence. |
| `src/boomi_mcp/categories/components/builders/process_flow_builder.py:_validate_sequence_set_properties_step` | Property name/source/persist syntax. | `PROCESS_PROPERTY_NAME_INVALID`, `PROCESS_SET_PROPERTIES_CONFIG_INVALID`, child source code | Sound local syntax. | adapter-only-compat | `tests/test_process_flow_builder.py::test_set_properties_step_prefixed_name_rejected` | None. |
| `src/boomi_mcp/categories/components/builders/process_flow_builder.py:_validate_sequence_cache_put_step` | Requires Document Cache selector. | `PROCESS_FLOW_SEQUENCE_CONFIG_INVALID` | Sound local syntax. | adapter-only-compat | `tests/test_process_flow_builder.py::test_cache_put_requires_document_cache_id` | None. |
| `src/boomi_mcp/categories/components/builders/process_flow_builder.py:_validate_sequence_cache_get_step` | Gates keyed mode, requires all-doc mode, validates external writer. | `PROCESS_DOCCACHE_RETRIEVE_CONFIG_INVALID` | Sound for captured mode; lineage incomplete. | adapter-only-compat | `tests/test_process_flow_builder.py::test_cache_get_keyed_mode_gated_with_named_error` | `LEGACY_ADAPTER_EXEMPTION_STANDALONE_CACHE_READ`; remove with typed external-writer coverage. |
| `src/boomi_mcp/categories/components/builders/process_flow_builder.py:_validate_sequence_decision_step` | Operands, recursive arms, target-starving cache tail. | `PROCESS_DECISION_CONFIG_INVALID`, `PROCESS_FLOW_SEQUENCE_CONFIG_INVALID`, child codes | Sound legacy grammar; CFG semantics incomplete. | adapter-only-compat | `tests/test_process_flow_builder.py::test_decision_true_leg_trailing_cache_put_rejected` | Decision-read exemption applies only to lineage. |
| `src/boomi_mcp/categories/components/builders/process_flow_builder.py:_validate_sequence_branch_step` | Leg bounds/schema/targets and linear-only legs. | `PROCESS_FLOW_SEQUENCE_CONFIG_INVALID`, child binding code | Sound for legacy no-join grammar. | adapter-only-compat | `tests/test_process_flow_builder.py::test_flow_sequence_rejects_branch_too_many_legs` | None. |
| `src/boomi_mcp/categories/components/builders/process_flow_builder.py:_validate_sequence_exception_step` | Delegates exception terminal body. | `PROCESS_EXCEPTION_CONFIG_INVALID` | Sound for verified renderer. | adapter-only-compat | `tests/test_process_flow_builder.py::test_flow_sequence_exception_body_error_uses_exception_code` | None. |
| `src/boomi_mcp/categories/components/builders/process_flow_builder.py:_validate_processcall_entry` | Exactly one target, exact ref syntax, boolean flags. | `PROCESS_REF_MISSING`, `PROCESS_REF_AMBIGUOUS`, `PROCESS_CALL_CONFIG_INVALID` | Sound legacy syntax; no child effect summary. | adapter-only-compat | `tests/test_wrapper_subprocess_builder.py::TestValidate::test_rejects_entry_with_both_targets` | Summary handled by wrapper exemption. |
| `src/boomi_mcp/categories/components/builders/process_flow_builder.py:WrapperSubprocessBuilder.validate_config` | Wrapper kind/calls/extensions/reachability/terminal/secrets. | Existing wrapper/builder codes | Sound syntax; incomplete without summaries. | adapter-only-compat | `tests/test_wrapper_subprocess_builder.py::TestValidate::test_accepts_literal_process_id` | `LEGACY_ADAPTER_EXEMPTION_SUBPROCESS_SUMMARY`; remove after all wrapper summaries exist. |
| `src/boomi_mcp/categories/components/builders/process_flow_builder.py:_sync_pipeline_is_canonical` | Routes non-listener lowered pipelines to adapter. | None | Sound for current listener capability gap. | adapter-only-compat | `tests/test_sync_pipeline_adapter_cutover.py::test_listener_chain_does_not_reach_the_adapter` | #140/#143; remove when listener entry is representable. |
| `src/boomi_mcp/categories/components/builders/process_flow_builder.py:SyncPipelineBuilder.lower_config` | Strict linear PipelineSpec validation and lowering. | `SYNC_PIPELINE_CONFIG_INVALID`, `SYNC_PIPELINE_CONTROL_FLOW_UNSUPPORTED`, `SYNC_PIPELINE_STAGE_UNSUPPORTED`, `PROCESS_CONNECTOR_BINDING_INVALID` | Sound for verified-linear legacy dialect only. | adapter-only-compat | `tests/test_sync_pipeline_builder.py::test_all_stage_and_dependency_permutations_lower_identically` | Retire with dialect. |
| `src/boomi_mcp/categories/components/builders/process_flow_builder.py:SyncPipelineBuilder.validate_config` | Kind/lowering/delegated legacy flow validation. | Lowering and legacy builder codes | Sound for legacy dialect. | adapter-only-compat | `tests/test_sync_pipeline_builder.py::test_valid_with_map_validates_clean` | Retire with dialect. |
| `src/boomi_mcp/categories/integration_builder.py:_check_process_flow_ref_types` | In-spec connector/cache/process/profile/extension role checks. | `PROCESS_REF_TYPE_MISMATCH` | Sound for classifiable `$ref`s; literal refs skipped. | refine-with-typed-facts | `tests/test_integration_builder.py::TestBuildPlanSyncPipeline::test_swapped_source_refs_surface_type_mismatch` | Legacy helper retained for precedence. |
| `src/boomi_mcp/categories/integration_builder.py:_check_rest_target_ref_types` | Nested REST connection/action type and method agreement. | `PROCESS_REF_TYPE_MISMATCH` | Sound for in-spec refs. | refine-with-typed-facts | `tests/test_integration_builder.py::TestBuildPlanFlowSequenceRefTypes::test_flow_sequence_nested_branch_leg_swapped_ref_errors_with_mismatch` | None. |
| `src/boomi_mcp/categories/integration_builder.py:_check_flow_sequence_steps_ref_types` | Recursive cache/Branch/Decision/Data Process ref roles. | `PROCESS_REF_TYPE_MISMATCH` | Sound for exact in-spec refs. | refine-with-typed-facts | `tests/test_integration_builder.py::TestBuildPlanFlowSequenceRefTypes::test_flow_sequence_cache_step_swapped_ref_errors_with_mismatch` | None. |
| `src/boomi_mcp/categories/integration_builder.py:_check_flow_sequence_ref_types` | Entry wrapper for nested flow-sequence ref pass. | `PROCESS_REF_TYPE_MISMATCH` | Sound but legacy-surface-only. | refine-with-typed-facts | `tests/test_integration_builder.py::TestBuildPlanFlowSequenceRefTypes::test_flow_sequence_nested_branch_leg_refs_plan_clean` | Legacy facade retained. |
| `src/boomi_mcp/categories/integration_builder.py:_check_wrapper_subprocess_ref_types` | Self/not-found/wrong child type and extension refs. | `PROCESS_REF_SELF_REFERENCE`, `PROCESS_REF_NOT_FOUND`, `PROCESS_REF_TYPE_MISMATCH` | Sound for in-spec refs; no effect proof. | refine-with-typed-facts | `tests/test_integration_builder.py::TestWrapperSubprocessPlan::test_type_mismatch_errors` | Summary exemption never downgrades reference/type errors. |
| `src/boomi_mcp/categories/integration_builder.py:_process_component_preflight` | Pure create/clone/update secret/name/builder/ref/lineage gate; feeds `error_process_validation`. | Existing builder/ref/lineage codes | Narrow checks sound; distributed/fail-fast today. | refine-with-typed-facts | `tests/test_integration_builder.py::TestApplyPlanProcessFlow::test_apply_aborts_when_process_flow_validation_fails` | Registry policy only; includes legacy lineage-pass compatibility. |
| `src/boomi_mcp/compiler/process_ir/pipeline.py:_guarded` | Redacts unexpected compiler-stage failures. | `PROCESS_IR_COMPILE_INTERNAL` | Sound compiler-defect boundary. | port-unchanged | New `tests/test_process_ir_semantic_validation.py::test_unexpected_phase_failure_is_redacted` | None; no new compile code. |
| `src/boomi_mcp/compiler/process_ir/pipeline.py:compile_process_ir_v1` | Current distributed fail-fast compile order. | Called-stage ProcessIR codes | Sound pieces, incomplete as one semantic gate. | refine-with-typed-facts | `tests/test_process_ir_compiler.py::test_compile_process_ir_v1_returns_checked_artifacts` | Strict direct policy. |
| `src/boomi_mcp/compiler/process_ir/pipeline.py:parse_and_compile_process_ir_v1` | Parse translation then compile. | Schema codes; `PROCESS_IR_COMPILE_INTERNAL`; called-stage codes | Sound parse translation; needs unified gate. | refine-with-typed-facts | `tests/test_process_ir_compiler.py::test_parse_and_compile_translates_schema_diagnostics_verbatim` | None. |
| `src/boomi_mcp/compiler/process_ir/legacy_adapters/registry.py:adapter_for` | Immutable migrated-dialect lookup. | None | Complete for registered dialects. | adapter-only-compat | `tests/test_legacy_process_ir_adapters.py::test_registry_unknown_dialect_returns_none` | #139; extend row with internal policy only. |
| `src/boomi_mcp/compiler/process_ir/legacy_adapters/flow_sequence.py:_alias_refs` | Occurrence-scoped aliases and duplicate-alias prevention. | `LEGACY_ADAPTER_SEMANTIC_LOSS` | Sound fail-closed adapter guard. | adapter-only-compat | `tests/test_legacy_process_ir_adapters.py::test_flow_aliasing_is_deterministic_across_repeated_adaptations` | #139; adapter retirement. |
| `src/boomi_mcp/compiler/process_ir/legacy_adapters/flow_sequence.py:_requirements_from_ir` | Derives exact symbol roles and fails on unmapped live refs. | `LEGACY_ADAPTER_SEMANTIC_LOSS` | Sound fail-closed parity guard. | adapter-only-compat | `tests/test_legacy_process_ir_adapters.py::test_flow_live_ref_without_recorded_selector_fails_closed` | #139; adapter retirement. |
| `src/boomi_mcp/compiler/process_ir/legacy_adapters/flow_sequence.py:adapt_flow_sequence` | Projects legacy input, records no-ops, validates refs, creates strict IR. | `LEGACY_ADAPTER_SEMANTIC_LOSS`; internal schema errors | Sound for frozen dialect. | adapter-only-compat | `tests/test_legacy_process_ir_adapters.py::test_adapter_returns_validated_ir` | #139; byte/error parity retirement gate. |
| `src/boomi_mcp/compiler/process_ir/legacy_adapters/sync_pipeline.py:_require_dict` | Object-shape guard. | `LEGACY_ADAPTER_SEMANTIC_LOSS` | Sound local boundary. | adapter-only-compat | `tests/test_sync_pipeline_adapter_cutover.py::test_adapter_fails_closed` | #139; adapter retirement. |
| `src/boomi_mcp/compiler/process_ir/legacy_adapters/sync_pipeline.py:_check_binding` | Rejects unrepresentable binding keys. | `LEGACY_ADAPTER_UNSUPPORTED_KIND` | Sound fail-closed representation gate. | adapter-only-compat | `tests/test_sync_pipeline_adapter_cutover.py::test_adapter_fails_closed` | Remove only with implemented capability and parity. |
| `src/boomi_mcp/compiler/process_ir/legacy_adapters/sync_pipeline.py:_is_listener` | Detects unsupported listener source. | None; drives unsupported-kind code | Sound capability refusal. | adapter-only-compat | `tests/test_sync_pipeline_adapter_cutover.py::test_adapter_refuses_a_listener_source` | #140; listener capability gate. |
| `src/boomi_mcp/compiler/process_ir/legacy_adapters/sync_pipeline.py:_binding_slots` | Requires resolved connection/operation selectors. | `LEGACY_ADAPTER_SEMANTIC_LOSS` | Sound for lowered dialect. | adapter-only-compat | `tests/test_sync_pipeline_adapter_cutover.py::test_symbol_requirement_contract_for_read_map_send` | #139. |
| `src/boomi_mcp/compiler/process_ir/legacy_adapters/sync_pipeline.py:_map_slot` | Passthrough/map only, extra-key and selector guards. | `LEGACY_ADAPTER_UNSUPPORTED_KIND`, `LEGACY_ADAPTER_SEMANTIC_LOSS` | Sound for dialect; intentionally closed. | adapter-only-compat | `tests/test_sync_pipeline_adapter_cutover.py::test_map_pointer_names_the_spelling_the_author_actually_used` | #139; capability/parity gate. |
| `src/boomi_mcp/compiler/process_ir/legacy_adapters/sync_pipeline.py:adapt_sync_pipeline` | Root/listener/binding/map guards and strict IR construction. | Two existing adapter codes; internal schema errors | Sound fail-closed for non-listener core. | adapter-only-compat | `tests/test_sync_pipeline_adapter_cutover.py::test_cutover_is_byte_identical_to_the_legacy_renderer` | #139/#140; dialect/listener retirement. |
| `src/boomi_mcp/compiler/process_ir/legacy_adapters/sync_pipeline.py:adapt_sync_pipeline_config` | Raw config lowering, path rebasing, adapter invocation. | Legacy sync-builder plus adapter codes | Complete for registered raw dialect. | adapter-only-compat | `tests/test_legacy_process_ir_adapters.py::test_every_migrated_adapter_accepts_its_raw_dialect_config` | #139. |
| `src/boomi_mcp/compiler/process_ir/legacy_adapters/wrapper_subprocess.py:adapt_wrapper_subprocess` | Validated call/target conversion, deduped requirements, terminal choice. | `LEGACY_ADAPTER_SEMANTIC_LOSS`; internal schema errors | Sound syntax; incomplete without child summary. | adapter-only-compat | `tests/test_legacy_process_ir_adapters.py::test_wrapper_requirements_are_process_typed_and_deduped` | `LEGACY_ADAPTER_EXEMPTION_SUBPROCESS_SUMMARY`; summary coverage gate. |
| `src/boomi_mcp/compiler/process_ir/legacy_adapters/emission.py:_symbol_table` | Resolves adapter selectors into compiler symbols. | Contract failure only on adapter defect | Sound for adapter requirements. | adapter-only-compat | `tests/test_legacy_adapter_cutover.py::test_resolver_return_value_lands_in_xml_never_the_alias_or_selector` | #139. |
| `src/boomi_mcp/compiler/process_ir/legacy_adapters/emission.py:emit_legacy_result` | Compiles/emits adapter IR and wraps parity failure. | `LEGACY_ADAPTER_OUTPUT_PARITY_FAILED` | Sound parity boundary; no accumulation today. | adapter-only-compat | `tests/test_legacy_adapter_cutover.py::test_canonical_emit_failure_internal_cause_is_output_parity_failed` | #139; keep until legacy emission removed. |
| `src/boomi_mcp/compiler/process_ir/legacy_adapters/authority.py:_canonical_binding` | Normalizes binding through compiler canonicalization. | None | Sound for sync-lowerer image; fail-closed outside it. | adapter-only-compat | `tests/test_issue_139_authority.py::test_casing_follows_emission_not_raw_spelling` | #139 authority lifecycle. |
| `src/boomi_mcp/compiler/process_ir/legacy_adapters/authority.py:_canonical_transform` | Normalizes passthrough/map or returns unrepresentable. | None | Sound for singular linear view. | adapter-only-compat | `tests/test_issue_139_authority.py::test_passthrough_versus_mapped_is_a_disagreement` | #139. |
| `src/boomi_mcp/compiler/process_ir/legacy_adapters/authority.py:_core_from_authored_pipeline` | Real sync lowering; failure becomes unrepresentable. | None | Sound fail-closed comparison. | adapter-only-compat | `tests/test_issue_139_authority.py::test_an_unlowerable_authored_view_is_an_ordinary_mismatch` | #139. |
| `src/boomi_mcp/compiler/process_ir/legacy_adapters/authority.py:_core_from_submitted_config` | Real legacy validation/lowering; distinguishes invalid/unrepresentable. | None directly | Sound for current dialects. | adapter-only-compat | `tests/test_issue_139_authority.py::test_an_invalid_submitted_process_is_the_clean_plan_gate_not_a_conflict` | #139. |
| `src/boomi_mcp/compiler/process_ir/legacy_adapters/authority.py:_is_authoring_process` | Detects materializing authored process components. | None | Sound for current action model. | adapter-only-compat | `tests/test_issue_139_authority.py::test_declared_authoring_predicate_excludes_only_a_reference_only_create` | #139. |
| `src/boomi_mcp/compiler/process_ir/legacy_adapters/authority.py:evaluate_pipeline_authority` | Pure strict-authority disposition. | None; wrapper uses conflict code | Sound within singular comparison domain. | adapter-only-compat | `tests/test_issue_139_authority.py::test_strict_disagreement_is_rejected` | #139; ADR §9 lifecycle. |
| `src/boomi_mcp/categories/integration_builder.py:_authority_rejection` | Converts authority conflict to redacted plan rejection. | `LEGACY_ADAPTER_AUTHORITY_CONFLICT` | Sound/redacted compatibility gate. | adapter-only-compat | `tests/test_issue_139_authority.py::test_every_authority_rejection_stays_value_free` | #139; never exempt. |
| `src/boomi_mcp/categories/components/builders/process_flow_builder.py:SyncPipelineBuilder._linear_stage_order` | Exactly one connected, acyclic, non-fanout linear order. | `SYNC_PIPELINE_CONTROL_FLOW_UNSUPPORTED` | Sound for verified-linear dialect. | adapter-only-compat | `tests/test_sync_pipeline_builder.py::test_fan_out_rejected` | Retire with dialect. |
| `src/boomi_mcp/categories/components/builders/process_flow_builder.py:SyncPipelineBuilder._check_stage_primitive` | Exact supported primitive per stage kind. | `SYNC_PIPELINE_CONFIG_INVALID`, `SYNC_PIPELINE_STAGE_UNSUPPORTED` | Sound for legacy vocabulary. | adapter-only-compat | `tests/test_sync_pipeline_builder.py::test_reserved_primitive_on_read_stage_rejected` | None. |
| `src/boomi_mcp/categories/components/builders/process_flow_builder.py:SyncPipelineBuilder._check_source_connector_family` | Explicit family cannot contradict source primitive. | `SYNC_PIPELINE_CONFIG_INVALID` | Sound for explicit metadata; resolved facts stronger. | refine-with-typed-facts | `tests/test_sync_pipeline_builder.py::test_read_stage_connector_type_rest_rejected` | Legacy helper retained. |
| `src/boomi_mcp/categories/components/builders/process_flow_builder.py:SyncPipelineBuilder._check_target_connector_family` | Explicit family cannot contradict target primitive. | `SYNC_PIPELINE_CONFIG_INVALID` | Sound for explicit metadata. | refine-with-typed-facts | `tests/test_sync_pipeline_builder.py::test_write_stage_forced_to_rest_connector_type_rejected` | Legacy helper retained. |
| `src/boomi_mcp/categories/components/builders/process_flow_builder.py:SyncPipelineBuilder._lower_binding_stage` | Binding keys, primitive/family/action requirements while lowering. | Sync config/stage and connector-binding codes | Sound for six legacy stage kinds. | adapter-only-compat | `tests/test_sync_pipeline_builder.py::test_send_without_action_type_rejected_by_lowering` | None. |
| `src/boomi_mcp/categories/components/builders/process_flow_builder.py:SyncPipelineBuilder._lower_map_stage` | Map stage keys and required selector. | `SYNC_PIPELINE_CONFIG_INVALID` | Sound for optional legacy map slot. | adapter-only-compat | `tests/test_sync_pipeline_builder.py::test_map_stage_without_map_ref_rejected` | None. |
| `src/boomi_mcp/categories/components/builders/process_flow_builder.py:_validate_ref_reachability` | Exact `$ref:KEY` must appear in `depends_on`. | `MISSING_PROCESS_DEPENDENCY` | Sound for in-spec substitution; literal IDs allowed. | refine-with-typed-facts | `tests/test_process_flow_builder.py::TestValidateConfig::test_rejects_undeclared_ref_in_source` | Legacy helper retained for precedence. |
| `src/boomi_mcp/categories/components/builders/process_flow_builder.py:_build_composed_process_flow` | Build-bypass structural/lineage/parity guard. | Existing legacy codes; `PROCESS_XML_VALIDATION_FAILED` | Sound parity boundary, not semantic authority. | adapter-only-compat | `tests/test_legacy_adapter_cutover.py::test_post_validation_compile_failure_translates_to_public_code` | #139/#143; adapter retirement. |
| `src/boomi_mcp/categories/components/builders/process_flow_builder.py:WrapperSubprocessBuilder.build` | Build-bypass target/extension/parity guards. | Wrapper codes; `PROCESS_XML_VALIDATION_FAILED` | Sound totality boundary. | adapter-only-compat | `tests/test_wrapper_subprocess_builder.py::test_build_bypass_raises_on_missing_target` | Summary exemption coverage gate. |
| `src/boomi_mcp/categories/components/builders/process_flow_builder.py:SyncPipelineBuilder.build` | Re-lowers, preserves precedence, routes listener, translates parity defects. | Legacy lowering/extension/XML codes | Sound cutover boundary. | adapter-only-compat | `tests/test_sync_pipeline_adapter_cutover.py::test_cutover_is_byte_identical_to_the_legacy_renderer` | #139/#140 retirement gate. |
| `src/boomi_mcp/categories/integration_builder.py:_authored_process_validation_error` | Reuses preflight for authority precedence. | Existing builder error | Sound because it shares planner preflight. | adapter-only-compat | `tests/test_issue_139_authority.py::test_the_gate_shares_the_planners_preflight` | #139. |
| `src/boomi_mcp/categories/integration_builder.py:_authored_step_will_reuse` | Detects collision reuse before authority comparison. | None | Sound presentation predicate. | adapter-only-compat | `tests/test_issue_139_authority.py::test_withholding_follows_the_apply_predicate_not_planned_action` | #139. |
| `src/boomi_mcp/compiler/process_ir/legacy_adapters/authority.py:_canonical_core` | Projects source/transform/target to exact lowerer image. | None | Sound fail-closed normal form. | adapter-only-compat | `tests/test_issue_139_authority.py::test_a_valid_but_richer_process_is_not_representable_and_conflicts` | #139. |
| `src/boomi_mcp/compiler/process_ir/legacy_adapters/contracts.py:adapter_diagnostic` | Creates frozen value-free adapter diagnostics. | Caller-selected existing adapter code | Sound redaction carrier. | port-unchanged | `tests/test_sync_pipeline_adapter_cutover.py::test_diagnostics_never_echo_an_authored_value` | None. |

The implementation must preserve the matrix’s existing positive evidence and add the explicitly named missing model-parity negatives before the matrix is treated as complete.

### 7.2 Error-code ledger

The `Changed?` column takes **four** values across its **61** rows. The `re-homed`/`delegated`
distinction is load-bearing — an earlier draft used `re-homed` for all twenty rows in one group,
which made it impossible to tell a code whose construction genuinely moved from one that never moved
at all:

| Verdict | Rows | Means |
|---|---|---|
| `no` | 32 | raised exactly where it always was, by a site the semantic pass never calls |
| `new` | 17 | introduced by #143 |
| `delegated` | 9 | `connector_resolution` / `error_handling` still raises it and `flow.py` translates it into the report — one implementation, re-presented |
| `re-homed` | 3 | a unified collector in `semantic_validation/` now CONSTRUCTS the finding |

**Of the twenty rows this change reclassified** (all of which previously read `re-homed`): 3 are
genuinely re-homed, 9 are delegated, and 8 are unchanged. Those counts are scoped to those twenty
rows — the column-wide totals are the table above.

Note the `no` definition says "a site", not "a model validator or compiler oracle": five `no` rows
are `LEGACY_ADAPTER_*` codes raised from the tool layer or the adapters themselves.
`LEGACY_ADAPTER_OUTPUT_PARITY_FAILED` is the sharpest — `emit_legacy_result` is the function that
*calls* the #143 gate, so "the semantic pass never calls it" holds only because the direction is the
reverse of what a narrower wording would imply.

The 8 reclassified `no` rows matter most:
several are raised by `parse_process_ir_v1`, so the payload is rejected before `validate_process_ir`
ever runs. There is no body collector. The capability collector now exists —
`semantic_validation/pipeline.py` runs it first, reporting a supplied effect contract that binds to
nothing in the IR.

This reflects the slice-9 delegation decision recorded in
[PROCESS_IR_SEMANTIC_VALIDATION_V1](./PROCESS_IR_SEMANTIC_VALIDATION_V1.md) §8: rather than re-derive
#140/#142's rules into new collectors, the flow collector delegates to `validate_connector_calls`, so
map-bracketing and the non-producing-connector rule keep ONE implementation.

“Re-homed” means canonical diagnostic construction moves to the unified collector while the existing code meaning, family, and compatibility facade remain unchanged.

| Code | Family | Raised by (file:symbol) | After #143, still raised from where | Changed? (no / new / delegated / re-homed) |
|---|---|---|---|---|
| `PROCESS_IR_SCHEMA_UNKNOWN_NODE` | `PROCESS_IR_SCHEMA_*` | `models/process_ir.py:_translate_pydantic_error`, `parse_process_ir_v1` | Same model/parse boundary; unified report preserves it | no |
| `PROCESS_IR_SCHEMA_UNKNOWN_FIELD` | `PROCESS_IR_SCHEMA_*` | Same symbols | Same model/parse boundary | no |
| `PROCESS_IR_SCHEMA_INVALID_CARDINALITY` | `PROCESS_IR_SCHEMA_*` | `_cardinality_error`, `_translate_pydantic_error`, `parse_process_ir_v1` | Same local model validators | no |
| `PROCESS_IR_SCHEMA_VERSION_UNSUPPORTED` | `PROCESS_IR_SCHEMA_*` | `parse_process_ir_v1` | Same version gate | no |
| `PROCESS_IR_SCHEMA_INVALID` | `PROCESS_IR_SCHEMA_*` | `_translate_pydantic_error`, `parse_process_ir_v1`; `_process_ir_compat.py` codec | Same model/private-codec boundaries | no |
| `PROCESS_IR_REFERENCE_INVALID_FORMAT` | `PROCESS_IR_REFERENCE_*` | `_validate_component_ref`, `_validate_contract_ref`, parse translation | Same syntax gate | no |
| `PROCESS_IR_CAPABILITY_UNSUPPORTED` | `PROCESS_IR_CAPABILITY_*` | Model capability helpers/translation; compatibility codec; `lowering.py:_emitter_input_for` | Unchanged — `models/process_ir.py` (parse/translation), the `_process_ir_compat` codec, and `lowering.py:_emitter_input_for`; the semantic pass calls none of them | no |
| `PROCESS_IR_SEMANTIC_UNREACHABLE` | `PROCESS_IR_SEMANTIC_*` | `invariants.py:check_cfg_invariants` | `semantic_validation/flow.py` constructs it; invariant facade remains | re-homed |
| `PROCESS_IR_SEMANTIC_MISSING_TERMINAL` | `PROCESS_IR_SEMANTIC_*` | `check_cfg_invariants`, `check_emission_plan_invariants` | `semantic_validation/flow.py` constructs it; invariant facade remains | re-homed |
| `PROCESS_IR_SEMANTIC_AMBIGUOUS_FLOW` | `PROCESS_IR_SEMANTIC_*` | `check_cfg_invariants`, `_check_region_containment`, `check_emission_plan_invariants` | Unchanged — `invariants.check_cfg_invariants`, a compiler oracle the semantic pass never calls | no |
| `PROCESS_IR_COMPILE_INTERNAL` | `PROCESS_IR_COMPILE_*` | `pipeline.py:_guarded`, parse/compile; invariants; emitter; `diagnostics.py:internal_defect` | Unchanged — same compiler-defect boundary; nothing moved | no |
| `PROCESS_IR_COMPILE_NONDETERMINISTIC` | `PROCESS_IR_COMPILE_*` | CFG/emission invariants | Same compiler oracles | no |
| `PROCESS_IR_COMPILE_EMISSION_PLAN_INVALID` | `PROCESS_IR_COMPILE_*` | `lowering.py:_resolve`, `_emitter_input_for`; emission invariants | Same lowering/oracle fallback | no |
| `PROCESS_IR_COMPILE_EMITTER_MISSING` | `PROCESS_IR_COMPILE_*` | `emitter_registry.py:_preflight_node`, `emit_process` | Same emitter gate | no |
| `PROCESS_IR_COMPILE_EMITTER_INPUT_INVALID` | `PROCESS_IR_COMPILE_*` | Same emitter symbols | Same emitter gate | no |
| `PROCESS_IR_COMPILE_SYMBOL_UNRESOLVED` | `PROCESS_IR_COMPILE_*` | Same emitter symbols | Same post-validation assertion | no |
| `PROCESS_IR_COMPILE_XML_INVALID` | `PROCESS_IR_COMPILE_*` | `emitter_registry.py:emit_process` | Same XML oracle | no |
| `PROCESS_IR_COMPILE_VERIFIER_FAILED` | `PROCESS_IR_COMPILE_*` | `emitter_registry.py:emit_process` | Same separate graph-verifier oracle | no |
| `LEGACY_ADAPTER_UNSUPPORTED_KIND` | `LEGACY_ADAPTER_*` | `sync_pipeline.py:_check_binding`, `_map_slot`, `adapt_sync_pipeline` | Same adapter compatibility boundary | no |
| `LEGACY_ADAPTER_PIPELINE_DRAFT_ONLY` | `LEGACY_ADAPTER_*` | Registered, intentionally unraised | Remains reserved/unraised | no |
| `LEGACY_ADAPTER_AUTHORITY_CONFLICT` | `LEGACY_ADAPTER_*` | `integration_builder.py:_authority_rejection` | Same authority gate | no |
| `LEGACY_ADAPTER_SEMANTIC_LOSS` | `LEGACY_ADAPTER_*` | Flow/sync/wrapper adapter guards | Same normalization/loss checks | no |
| `LEGACY_ADAPTER_OUTPUT_PARITY_FAILED` | `LEGACY_ADAPTER_*` | `legacy_adapters/emission.py:emit_legacy_result` | Same parity translation after semantic preflight | no |
| `PROCESS_IR_REFERENCE_OPERATION_NOT_FOUND` | `PROCESS_IR_REFERENCE_*` | `connector_resolution.py:resolve_connector_call_bindings` | `connector_resolution`/`error_handling` raises it; `flow.py` translates it into the report | delegated |
| `PROCESS_IR_REFERENCE_CONNECTION_NOT_FOUND` | `PROCESS_IR_REFERENCE_*` | Same resolver | `connector_resolution`/`error_handling` raises it; `flow.py` translates it into the report | delegated |
| `PROCESS_IR_REFERENCE_CONNECTION_MISMATCH` | `PROCESS_IR_REFERENCE_*` | Same resolver | `connector_resolution`/`error_handling` raises it; `flow.py` translates it into the report | delegated |
| `PROCESS_IR_CAPABILITY_CONNECTOR_ACTION_UNSUPPORTED` | `PROCESS_IR_CAPABILITY_*` | Same resolver | `connector_resolution`/`error_handling` raises it; `flow.py` translates it into the report | delegated |
| `PROCESS_IR_SEMANTIC_PROFILE_MISMATCH` | `PROCESS_IR_SEMANTIC_*` | `validate_connector_call_semantics`, `_profile_failure`, `_check_map_pair`, `_walk_paths` | `connector_resolution`/`error_handling` raises it; `flow.py` translates it into the report | delegated |
| `PROCESS_IR_SEMANTIC_CARDINALITY_MISMATCH` | `PROCESS_IR_SEMANTIC_*` | `_cardinality_failure`, `_walk_paths`, connector semantic facade | `connector_resolution`/`error_handling` raises it; `flow.py` translates it into the report | delegated |
| `PROCESS_IR_COMPILE_CONNECTOR_BINDING_INVALID` | `PROCESS_IR_COMPILE_*` | `lowering.py:_emitter_input_for` | Same lowering assertion/fallback | no |
| `PROCESS_IR_SCHEMA_BRANCH_CARDINALITY` | `PROCESS_IR_SCHEMA_*` | Model parse translation | Same model boundary | no |
| `PROCESS_IR_SEMANTIC_CONTROL_CONTINUATION_UNSUPPORTED` | `PROCESS_IR_SEMANTIC_*` | Model continuation helper, `SequenceNodeV1._sequence_rules`, parse translation | Same local model rule | no |
| `PROCESS_IR_SEMANTIC_JOIN_UNSUPPORTED` | `PROCESS_IR_SEMANTIC_*` | `invariants.py:check_cfg_invariants` | Unchanged — `invariants.check_cfg_invariants` | no |
| `PROCESS_IR_SEMANTIC_NESTING_LIMIT` | `PROCESS_IR_SEMANTIC_*` | Model whole-control walk; body walk; CFG depth check | Unchanged — model validators / `body_capabilities` / `invariants` | no |
| `PROCESS_IR_SEMANTIC_UNTERMINATED_PATH` | `PROCESS_IR_SEMANTIC_*` | `invariants.py:_check_every_control_path_terminates` | `semantic_validation/flow.py` constructs it; invariant facade remains | re-homed |
| `PROCESS_IR_CAPABILITY_NODE_NOT_ALLOWED_IN_BODY` | `PROCESS_IR_CAPABILITY_*` | Model/body slot validators | Unchanged — raised by `parse_process_ir_v1`, so the payload never reaches the semantic pass | no |
| `PROCESS_IR_COMPILE_CONTROL_WIRING_INVALID` | `PROCESS_IR_COMPILE_*` | `invariants.py:_wiring_code`, emission-plan checks | Same plan oracle | no |
| `PROCESS_IR_SCHEMA_RETRY_COUNT` | `PROCESS_IR_SCHEMA_*` | Model parse translation | Same model boundary | no |
| `PROCESS_IR_CAPABILITY_ERROR_SCOPE_UNSUPPORTED` | `PROCESS_IR_CAPABILITY_*` | Model scope validators; body Try/Catch validators | Unchanged — raised by `parse_process_ir_v1`, so the payload never reaches the semantic pass | no |
| `PROCESS_IR_SEMANTIC_RETRY_SOURCE_REEXECUTION` | `PROCESS_IR_SEMANTIC_*` | `error_handling.py:validate_error_handling` | `connector_resolution`/`error_handling` raises it; `flow.py` translates it into the report | delegated |
| `PROCESS_IR_SEMANTIC_RETRY_NON_IDEMPOTENT_WRITE` | `PROCESS_IR_SEMANTIC_*` | Same retry validator | `connector_resolution`/`error_handling` raises it; `flow.py` translates it into the report | delegated |
| `PROCESS_IR_SEMANTIC_IDEMPOTENCY_EVIDENCE_MISSING` | `PROCESS_IR_SEMANTIC_*` | `_evidence_missing`, `_require_resolvable`, retry facade | `connector_resolution`/`error_handling` raises it; `flow.py` translates it into the report | delegated |
| `PROCESS_IR_SEMANTIC_CATCH_UNTERMINATED` | `PROCESS_IR_SEMANTIC_*` | Model catch validator/translation; body walk | Unchanged — raised by `parse_process_ir_v1`, so the payload never reaches the semantic pass | no |
| `PROCESS_IR_COMPILE_ERROR_REGION_INVALID` | `PROCESS_IR_COMPILE_*` | `error_handling.py` region helpers; CFG invariants | Same structural oracle; never a user semantic finding | no |
| `PROCESS_IR_REFERENCE_COMPONENT_NOT_FOUND` | `PROCESS_IR_REFERENCE_*` | Not currently registered | Unified resolver for non-specialized component refs | new |
| `PROCESS_IR_REFERENCE_COMPONENT_TYPE_MISMATCH` | `PROCESS_IR_REFERENCE_*` | Not currently registered | Unified resolver for non-specialized component roles | new |
| `PROCESS_IR_CAPABILITY_EFFECT_CONTRACT_INVALID` | `PROCESS_IR_CAPABILITY_*` | Not currently registered | Emitted by the `capability`-phase collector when a supplied contract binds to nothing in the IR | new |
| `PROCESS_IR_SEMANTIC_LINEAGE_PROPERTY_READ_BEFORE_WRITE` | `PROCESS_IR_SEMANTIC_*` | Not currently registered | Unified lineage collector | new |
| `PROCESS_IR_SEMANTIC_LINEAGE_DDP_SCOPE_INVALID` | `PROCESS_IR_SEMANTIC_*` | Not currently registered | Document-copy scope validation | new |
| `PROCESS_IR_SEMANTIC_LINEAGE_BRANCH_ORDER_INVALID` | `PROCESS_IR_SEMANTIC_*` | Not currently registered | Sequential-leg order validation | new |
| `PROCESS_IR_SEMANTIC_LINEAGE_CACHE_WRITER_MISSING` | `PROCESS_IR_SEMANTIC_*` | Not currently registered | Cache lineage collector | new |
| `PROCESS_IR_SEMANTIC_LINEAGE_AMBIGUOUS_LAST_WRITE` | `PROCESS_IR_SEMANTIC_*` | Not currently registered | Registered, intentionally unraised — no collector emits it (see §10 B) | new |
| `PROCESS_IR_SEMANTIC_LINEAGE_EFFECT_UNKNOWN` | `PROCESS_IR_SEMANTIC_*` | Not currently registered | Missing opaque-effect metadata diagnostic | new |
| `PROCESS_IR_SEMANTIC_LINEAGE_EXTERNAL_WRITER_ASSUMED` | `PROCESS_IR_SEMANTIC_*` | Not currently registered | Nonfatal typed external-writer assumption | new |
| `PROCESS_IR_SEMANTIC_SIDE_EFFECT_ORDERING_UNSAFE` | `PROCESS_IR_SEMANTIC_*` | Not currently registered | Demonstrated unsafe ordering collector | new |
| `PROCESS_IR_SEMANTIC_SIDE_EFFECT_ORDERING_UNKNOWN` | `PROCESS_IR_SEMANTIC_*` | Not currently registered | Unproven asynchronous/shared-state ordering | new |
| `PROCESS_IR_SEMANTIC_RETRY_EFFECT_UNSAFE` | `PROCESS_IR_SEMANTIC_*` | Not currently registered | Non-connector replay hazards only | new |
| `LEGACY_ADAPTER_EXEMPTION_OPAQUE_STATE_WRITER` | `LEGACY_ADAPTER_EXEMPTION_*` | Not currently registered | Registry-owned advisory policy | new |
| `LEGACY_ADAPTER_EXEMPTION_DECISION_PROPERTY_READ` | `LEGACY_ADAPTER_EXEMPTION_*` | Not currently registered | Registry-owned advisory policy | new |
| `LEGACY_ADAPTER_EXEMPTION_STANDALONE_CACHE_READ` | `LEGACY_ADAPTER_EXEMPTION_*` | Not currently registered | Registry-owned advisory policy | new |
| `LEGACY_ADAPTER_EXEMPTION_SUBPROCESS_SUMMARY` | `LEGACY_ADAPTER_EXEMPTION_*` | Not currently registered | Registry-owned advisory policy | new |

The ledger contains 44 existing registered codes plus 17 new #143 additions. No new `PROCESS_IR_COMPILE_*` code is introduced.



### 7.3 Exemption ledger — owners and removal gates

| Exemption | Covers | Adapters | Owner | Removal gate |
|---|---|---|---|---|
| `LEGACY_ADAPTER_EXEMPTION_OPAQUE_STATE_WRITER` (**live**) | `…LINEAGE_PROPERTY_READ_BEFORE_WRITE` | `flow_sequence`, `sync_pipeline` | epic #134 | the dialect declares typed map/script effect contracts |
| `LEGACY_ADAPTER_EXEMPTION_DECISION_PROPERTY_READ` (**inert**) | `…LINEAGE_EFFECT_UNKNOWN` | `flow_sequence` | epic #134 | as above — currently never fires: the target ships as a WARNING and `apply_policy` reclassifies only errors |
| `LEGACY_ADAPTER_EXEMPTION_STANDALONE_CACHE_READ` (**live**) | `…LINEAGE_CACHE_WRITER_MISSING` | `flow_sequence` | epic #134 | the dialect declares `external_writer` or an in-process writer |
| `LEGACY_ADAPTER_EXEMPTION_SUBPROCESS_SUMMARY` (**inert**) | `…SIDE_EFFECT_ORDERING_UNKNOWN` | `wrapper_subprocess` | epic #134 | as above — currently never fires: the target ships as a WARNING |

An exemption RECLASSIFIES an error as an advisory carrying the original code as evidence — it never
deletes the finding, so this ledger stays falsifiable.

**Live-census note (2026-07-26, #143 QA).** #143 adds 17 codes: **13 diagnostic** codes and **4
`LEGACY_ADAPTER_EXEMPTION_*`** rows. The two groups are counted separately below because "producible"
means different things for them — a diagnostic is produced by a collector, an exemption row fires
only when the error it covers does.

Over 5933 authorable configs:

| Group | Count | Status |
|---|---|---|
| diagnostic codes producible from some config | 7 of 13 | observed firing |
| diagnostic codes implemented and wired, but unreachable from these dialects | 5 of 13 | see §10 A |
| diagnostic codes registered with NO collector | 1 of 13 | see §10 B |
| exemption rows observed firing | 2 of 4 | live |
| exemption rows inert | 2 of 4 | their target codes ship as WARNINGS, and `apply_policy` reclassifies only errors |

So **6 of the 13 diagnostic codes** are not producible from this corpus, and separately **2 of the 4
exemption rows** are inert. An earlier version stated "6 of 17", which mixed the two denominators —
the 4 exemption rows are among the 17 but are not diagnostics, so they cannot be counted against the
same "producible" test.

**Scope of that measurement.** The census ran the three LEGACY DIALECTS through
`legacy_bridge.validate_legacy_process_config`. It says nothing about the direct ProcessIR API, and
the two must not be conflated: `…SIDE_EFFECT_ORDERING_UNSAFE` is **producible through
`validate_process_ir`** on an authored ProcessIR document (a non-waiting `process_call` in one Branch
leg and a property read in a later one, under the empty default capabilities — pinned by
`test_the_unsafe_branch_is_producible_from_an_authorable_document`), while remaining unreachable from
every legacy config in this corpus, because `flow_sequence` cannot author a `process_call` and
`wrapper_subprocess` cannot author the Branch and its property reader. It is counted here as
legacy-unreachable, which is what this census measures, and §10 of the sibling document classifies
it by the ProcessIR surface instead.

The 5 implemented codes and the 2 inert exemption rows ARE wired correctly — forcing the condition
makes each fire. That argument does **not** extend to the single group-B code: `finding()` accepts
any code outside the compile family (registered or not), so building a synthetic report carrying it
proves REGISTRATION, not wiring. `PROCESS_IR_SEMANTIC_LINEAGE_AMBIGUOUS_LAST_WRITE` has no rule
behind it at all.

None of this is a regression — the baseline had no such checks. It is a statement of how much of the
new surface is currently exercised, and by what.

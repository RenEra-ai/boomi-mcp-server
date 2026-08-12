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
| Topology (`SystemTopologySpecV1`) | #144 | **LANDED, dark** — capability-gated planning-only surface; `TOPOLOGY_*` family opened and closed by #144 (14 codes). No existing surface or schema migrated; no MCP exposure (that is #146). See §8 and `SYSTEM_TOPOLOGY_V1.md` |
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
| composition process emission (`patterns/composition.py`) | (inherits `flow_sequence`) | main process rewritten to `database_to_api_sync` + `flow_sequence=[map_ref, terminal branch]` | via the flow_sequence adapter | archetype-composition suite (raw XML parity) | inside `emit_process` | required | **canonical-inherited** through `flow_sequence`; the recipe adapter is **no longer pending** — #145 (M12.10) shipped the typed-recipe contribution surface and `patterns/composition.py` invokes typed recipes, so nothing here waits on it |
| `sync_pipeline` + 4 sync archetypes | `sync_pipeline.py` (#139C) | the 6 NON-listener stage chains: `read\|fetch(rest_fetch\|soap_fetch)` → `[map]` → `send(rest_send\|soap_send)\|write(db_write)`. The 4 WSS **listener** chains stay on the legacy renderer behind an explicit routing gate (`_sync_pipeline_is_canonical`) — #140 owns the fused `start_listen` entry | adapter/compile/emit defect → `PROCESS_XML_VALIDATION_FAILED`; `SYNC_PIPELINE_*` codes unchanged and keep precedence (`lower_config` runs first) | `sync_pipeline_db_read_map_rest_send.xml`, `sync_pipeline_fetch_map_db_write.xml`, `sync_pipeline_fetch_rest_send_no_map.xml`, `sync_pipeline_soap_fetch_soap_send.xml` (+ `listener_wss_start.xml` on the legacy arm) | inside `emit_process` | required | **canonical** for **all 16** non-listener chains at the PRIMITIVE level (3 source primitives × 3 target primitives × ±map = 18, minus `db_read`→`db_write` ±map which is rejected upstream as `SYNC_PIPELINE_CONTROL_FLOW_UNSUPPORTED`) — pinned since #139E by `tests/fixtures/process_ir/sync_pipeline_emitter_parity_cases.json` (17 cases / 16 fingerprints; the 17th repeats the SOAP chain with the non-uppercase `execute` verb), whose coverage is **derived from the builder's own primitive tables**, not enumerated. #139C's inline set described only the 6 *stage-kind* chains and covered 7 of these 16. The 4 WSS **listener** chains are **legacy / pending-capability** — NOT "pending #140": #140 landed and added no fused `start_listen`, so the registry still holds 18 keys with none of them `start_listen` |
| ordinary `database_to_api_sync` (single/linear, Try-Catch, dynamic path, listener) | reserved | — | unchanged | existing goldens | n/a | n/a | **pending-capability**, re-verified at #139E (HEAD `1bd0b69`): `catcherrors` **now exists** canonically (#142 — `TryCatchNodeV1`, `CatchErrorsInputV1`, registry key `catcherrors`), but `start_listen`, connector `dynamic_path` and Notify remain **unrepresentable** — `_REGISTRATIONS` holds 18 keys with neither `start_listen` nor `notify`, and no connector input model carries a dynamic-path field. #139C's adapter is a *linear single-shape core* adapter deliberately shaped to be promoted here once those **three** close (not four) |
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
#139D**), ~~the recipe/archetype named adapters~~ (**DONE in #145** — `api_to_api_sync`,
`api_to_database_sync` and `compose_archetypes` invoke typed recipes, and their process paths already
run through the canonical sync / flow_sequence adapters), ~~extending the emitter-parity oracle to
`sync_pipeline`~~ (**DONE in #139E**). Still deferred: the ordinary `database_to_api_sync` process
dialect, and the final legacy-dispatch reachability audit that removes the remaining legacy XML
dispatch (**#147**, which owns that criterion verbatim).

**The "#140 / #142 / #145 blocker" note above was only PARTLY resolved, and #139E is its
counter-example.** #142 shipped canonical `catcherrors` and #145 shipped the recipe adapters — but
fused `start_listen`, connector `dynamic_path` and Notify have **no** canonical emission anywhere in
the tree, so the ordinary dialect and the 4 WSS listener chains genuinely stay `pending-capability`,
and those three are **unowned capability work rather than #139 adapter work** (issue #139 lists new
capability as out of scope). What the note got wrong is the universal quantifier: the
already-canonical non-listener `sync_pipeline` surface was never blocked at all, and #139E pinned it
on its own merits without touching a single production file. `database_to_api_sync` additionally
remains a distinct raw dialect because it reattaches `reliability` and `target.dynamic_path` **after**
`SyncPipelineBuilder.lower_config`.

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

### #139E landed — sync_pipeline emitter-parity reachability and capability reconciliation

**Test-and-docs only. Zero production diff:** no file under `src/` was touched, no golden was
regenerated, no emitter / adapter / builder / archetype changed, and the emitter registry stays at 18
keys. No `pending`/`reserved` row above becomes canonical in this slice, and no listener chain is
promoted.

**Why a second harness was needed.** The generic emitter-parity oracle
(`tests/test_process_emitter_parity.py`) drives frozen `ProcessIRV1` documents through
`ir_to_legacy_flow_sequence`. That oracle is **structurally unreachable** for the `sync_pipeline`
dialect, and #140/#146 did nothing to change it: the frozen #136 codec hard-requires a non-empty
`flow_sequence`, while 5 of the 10 accepted sync chains are map-less (the #139C finding). The only way
into this dialect is its own normalizer. #139E therefore adds a **separate** adapter-dialect harness in
the same file, using `SyncPipelineBuilder.lower_config` as the **sole** normalizer — there is no
inverse — and comparing canonical emission against `ProcessFlowBuilder` on that same lowered core.
(Not "exactly once per case": `build()` lowers again internally. The property that matters is that no
other normalization path exists, not the call count.) It does **not** invent a
ProcessIR-to-`PipelineSpec` inverse: no such direction exists in the tree, ADR-001 §6 forbids that
second semantic compiler, and #139D already settled the point.

**The corpus, and why it is DERIVED rather than enumerated.** The first draft of this slice froze
#139C's inline eight-case set verbatim and described it as the canonical set. Live QA graded the
corpus's *universe* instead of its members and refuted that: at the primitive level there are **16**
canonical non-listener chains (3 source primitives × 3 target primitives × ±map = 18, minus
`db_read`→`db_write` ±map, rejected upstream), and the inherited set covered **7**. The gap was the
map-ful SOAP chain plus every mixed-connector-family combination — precisely the surface the #139A
cross-role connector guard and #139C's own family-conditional canonicalizer defect live on. No
coverage had been *lost* (the set came straight from #139C), but the slice had newly added prose
asserting a completeness that measurement refuted.

`tests/fixtures/process_ir/sync_pipeline_emitter_parity_cases.json` therefore carries **all 16**
fingerprints plus one verb-casing duplicate (17 cases), and case names *are* the fingerprint
(`<source_primitive>[_map]_<target_primitive>`) so an omission is visible by inspection.

**The derivation then failed open twice more, and only the third fix is durable.** Each time the
*model* was the defect, not its parameters — the same shape as the original finding, one level deeper:

1. The first fix recomputed the universe from `_SYNC_PIPELINE_STAGE_PRIMITIVE` /
   `_SYNC_PIPELINE_STAGE_ALT_PRIMITIVE`, but still modelled the grammar as `source × target × ±map`,
   assigning kinds to slots via the adapter's `_SOURCE_STAGE_KINDS` / `_TARGET_STAGE_KINDS`. The
   **Codex impl-vs-plan review** found that a kind in *both* role sets is silently filed source-only
   by the `if`/`elif`: give `fetch` a target role and permit `read → fetch`, and `db_read_rest_fetch`
   is reachable, canonical and uncovered with every guard green.
2. Dropping the role model left an **arity** model — lengths fixed to {2, 3}. Permitting a 4-stage
   `fetch → map → map → send` left four canonical chains uncovered, and those are byte-**distinct**
   (both map stages are dropped, emitting zero map shapes) while parity still holds, because
   canonical and legacy drop them identically. That is exactly the uniform drift a differential
   cannot see, so this missing coverage was real coverage — unlike the byte-identical case above it.
3. Replacing the fixed lengths with a sweep that walked upward until a length was dry was *still* a
   model: dryness is **local**. A grammar accepting 5-stage chains while 4 stays dry stops the sweep
   at 4 and is missed entirely — verified reachable and canonical by mutation.

No bounded sampling of the chain space is complete against a grammar that can change shape. So
`test_sync_corpus_covers_every_canonical_chain` stopped sampling and now **reads the grammar**: it
extracts `lower_config`'s accepted stage-kind sequences from the builder's own source by AST,
enumerates every primitive assignment over exactly those sequences, and lets the routing gate split
the results. A bounded behavioural sweep then cross-checks that nothing accepted lives outside the
extracted literal. A newly permitted stage **sequence** fails the suite until the corpus grows, as does
a new stage kind or primitive **added to those tables that yields an accepted grammar assignment**.
Both qualifiers are load-bearing and were established by counter-example: a table-only key the
supported-kind gate rejects before the primitive check ever runs (`shadow: db_read`) leaves the suite
green, and so does a primitive admitted by any mechanism other than those tables — `_check_stage_primitive`
derives from them today, but nothing pins that.

4. **And reading it by AST was itself a fourth model — a *syntax* model.** The first extraction searched
   for a comparison that looked like the grammar and skipped anything else, so *extending* the guard
   stayed invisible while the match count held at 1. Hoisting the tuple into a module constant
   (`… and kinds not in _CONST`) or adding `… and len(kinds) != 5` both evaded it — and the first of
   those is an ordinary cleanup no reviewer would question, which is why it was graded above the
   earlier two. The extraction now anchors on the guard **statement** and refuses any test it cannot
   read, so an unrecognised form kills instead of slipping through.

**The honest limit — stated as a principle, because every attempt to state it as a LIST of residual
classes has itself been incomplete.** Every draft of this passage that enumerated "the"
residual classes and each was refuted within one review round; enumerating them is the same mistake as
modelling the grammar, one level up.

*What is proven:* for the chains the harness reaches, the corpus covers exactly those the routing gate
calls canonical. *What is not proven, and cannot be by anything in the test:* that the harness reaches
every chain the builder accepts. It learns the accepted set from a statement in `lower_config`, and it
can only detect acceptance happening elsewhere by trying inputs and observing — so any acceptance path
the trials do not exercise is invisible.

The trials are limited in two dimensions. In **chain length** by *two* constants that deliberately
disagree — the bare sweep runs to `_SYNC_CROSSCHECK_MAX_LENGTH`, the shape sampling only to
`_SYNC_ENRICHED_MAX_LENGTH`; naming only the higher would imply a uniform limit, and the gap between
them is exactly where a bypass hides (measured both ways: a config-field-gated bypass *at* the enriched
bound is killed, the same bypass one length *above* it survives). And in **config shape**, which is not
bounded but *sampled* (`_SYNC_PROBE_ENRICHMENTS`) — the difference is the whole point. No finite variant set closes that dimension, because acceptance can be
keyed on a **value** (`label == "magic"`) rather than on a field's presence, so a bypass always exists
and is constructible in minutes. Successive versions of the sampling were broken in review —
chain length, then root-key shape, then stage/edge shape — and a fourth would be no harder. Adding
dimensions moves the bypass; it never removes it.

**Three selection errors inside that are worth recording, because every one was about the *criterion*
rather than a missing entry** — the same failure the grammar had, one level over:

1. The set was chosen as "root keys `lower_config` accepts **and carries through**". Carriage is
   irrelevant: a bypass conditions on a value being *present in the input*, not on it surviving
   lowering. That alone hid five root keys.
2. It was a **hand list**, which missed `dependencies[].label` and five `StageSpec` fields nobody had
   noticed were accepted (`cardinality`, `context_effect`, `failure_behavior`, `side_effect`).
3. The root level was *kept* hand-listed on the stated premise that nothing in the builder declares
   it. False — `_SYNC_PIPELINE_ALLOWED_TOP_LEVEL` sits in the same module the derivation already
   imports its stage allow-lists from. The hand list happened to be complete, but complete *by
   coincidence*, which is precisely the condition that produced the two misses above it.

All four levels are now **derived** from the builder's own declarations — root from
`_SYNC_PIPELINE_ALLOWED_TOP_LEVEL`, stage-config from its allow-lists, stage-level from
`StageSpec.model_fields`, edge from `PipelineEdgeSpec.model_fields` — with declared-but-unprobeable
fields pinned. (`PipelineSpec` contributes nothing: its fields are exactly `stages` and
`dependencies`, so these are all the levels the config tree has.) A field is probed with **every declared option that is accepted when set alone on the probe chain**,
not just the first: `Literal` options come in declaration order and the first is invariably the
neutral default, so the set was sending `side_effect="none"` and never `"write"` — backwards, since a
guard is far likelier to key on the latter. Pinning the *omitted options per field* — not just wholly-unprobeable fields — is what made the
remaining gaps visible, and it paid twice. It surfaced `edge_kind`'s four control-flow options
(rejected by this dialect by design, so genuinely unreachable and still pinned), and it exposed a
false reason I had written for `failure_behavior`'s `retry`/`catch`: I claimed they "need companion
metadata this generic chain does not supply". **The validator's own rejection text said otherwise** —
`failure_behavior='retry' requires side_effect read/write/read_write`, `'catch' requires
context_effect='new_connection'` — and both companions are fields the derivation already probes. The
real cause was that an enrichment wrote exactly ONE field, so the needed pair was not constructible;
a rescue pass now retries each rejected option with one companion and reaches both (15 and 4 accepted
combinations respectively). The lesson is cheap and general: **when a validator rejects a probe, its
message is the specification for what the probe is missing** — read it before hypothesising. Fields whose type declares no options get one truthy and one falsy accepted value — the **first**
accepted value from each group, and nothing more. That covers presence- and truthiness-keyed
conditions and no other value-blind predicate: a guard keying on type (`isinstance(x, dict)`), on
`is None`, on length, or on internal structure is unsampled, verified rather than assumed — a
type-gated `description` bypass survives this sweep today. (Earlier drafts called truthy/falsy "the
only two predicates a guard can apply" and implied every accepted value was sent; both were wrong.)
They are joined by a combined all-root-metadata shape, because single-field enrichments cannot express
a **co-presence** condition — dropping that shape when the set went one-field-at-a-time was itself a
coverage regression, caught in review.

**What the shape sampling does NOT reach**, stated because naming it is not the same as covering it:
it varies the four config levels' *fields*, never descending into structured values.
`process_extensions` carries a nested `connections[].fields[]` shape that `lower_config` passes
through, and the probe sends it a scalar. An arbitrarily nested value space is the same unreachable
case as a value-keyed condition. Nor is every enrichment applied to every candidate, in two distinct ways. The variant loop
short-circuits at the first shape that lowers and the bare shape is first, so enrichments act only on
assignments the bare shape *rejects* — which is the bypass case, and is what makes them cheap, but is
not "full enrichment". And the enrichment bound (`_SYNC_ENRICHED_MAX_LENGTH`) is **lower than** the
sweep bound (`_SYNC_CROSSCHECK_MAX_LENGTH`), so a bypass at the top chain length gated on a config
field is unsampled. That disagreement is a real hole, not a neutral allocation; making the two agree
was measured at 36.8s for the file versus its current 4.9s — 7.5× — and rejected on cost alone.

**Two remedies are recorded here rather than done in #139E, and neither is a proof on its own:**

- **Hoisting** the accepted-sequence tuple to a module-level constant in `process_flow_builder.py` and
  importing it removes the *extraction* half — nothing left to misread. ~3 lines moving no emitted byte
  and no MCP surface; **the intended resolution for whichever slice next touches that file**. It does
  **not** close the class above: under a bypass the extraction already returns the correct,
  byte-identical sequences, so an imported constant would too. An earlier draft claimed hoisting was
  "the end state that removes the class"; that was false, asserted from reasoning and never measured.
- **A different oracle detects a different class than sampling does:** run the probe corpus under a
  tracer and assert every branch on `lower_config`'s accept/reject path was exercised, so a bypass
  branch the probes never trigger surfaces as an *uncovered branch* rather than as silence.
  Precedented here by **#145**'s reachability tracer
  (`tests/test_recipe_registry.py::test_every_reachable_registry_build_defect_is_exercised_by_a_test`).
  **It is not a completeness proof either** — an earlier draft of this passage said it would "end the
  sequence", which overclaimed: a value-keyed bypass folded into an already-covered path (a table
  lookup rather than a new branch) introduces no uncovered branch to find. Materially bigger than a
  test-only refactor, and it plausibly belongs to whichever slice owns the lowering path.

**Re-entry criterion.** Re-run this attack when an acceptance path near the routing gate changes.
**#139F and #140 both plausibly add one** when `start_listen` is promoted — that is the point at which
the cross-check bound stops being a documented limit and becomes load-bearing.

This is the #139D lesson applied four times over, and the transferable form is sharper than "fix the
class": **when a guard describes a rule the product states independently, it is a model of that rule,
and every model has an unmodelled region — so mutate the product's statement, not just the guard's
inputs.** Every one of these holes was invisible to a mutation matrix that varied only the guard's own
constants; each was found by mutating the builder instead.

**Every case is byte-anchored, and the timing was the point.** #139E's first draft anchored only 4 of
17 cases; the other 13 were a differential against the legacy renderer and nothing else. Since that
renderer is scheduled for deletion, those 13 would have evaporated silently at deletion, leaving the
**surviving** `sync_pipeline` dialect anchored on 4 of its 16 primitive chains. All 13 were therefore
generated **through the legacy renderer before its removal** — the only independent source of those
bytes; a golden written afterwards would confirm the canonical arm against itself. The same reasoning
applied to the **4 WSS listener chains**, which stay on the legacy arm: `sync_pipeline_listener_*.xml`
are committed and asserted by `tests/test_sync_pipeline_adapter_cutover.py::test_listener_chain_matches_its_committed_golden`,
and pin the fused `start_listen` entry ProcessIR v1 still cannot express. 21 `sync_pipeline_*.xml`
goldens now exist where 4 did; all 17 additions were mutation-checked as live rather than inert.

The anchored cases are asserted on BOTH renderers, and the harness asserts the
**legacy** renderer against those bytes as well as the canonical one — the non-redundant half, because
nothing else in the tree pinned `ProcessFlowBuilder`'s own output against them, and a uniform drift
moves both sides of a differential together. `tests/test_sync_pipeline_adapter_cutover.py` now loads
its migrated set from the same fixture, so "which chains are canonical" has exactly **one** definition.

The corpus is pinned **fail-closed in every direction**, because a harness that merely iterates a
fixture fails *open*: coverage must equal the derived canonical set (superset direction); exactly one
case per fingerprint, with `soap_lowercase_execute` the single sanctioned duplicate (deletion and
silent-duplicate directions); the primitive-only fingerprint must stay *injective* over the canonical
set, or the corpus key can no longer identify a chain; the claimed anchors must equal the **canonical**
`golden_xml` inventory; and `_sync_pipeline_is_canonical` must be `True` for every case, so a listener
chain smuggled into the corpus cannot quietly turn the parity assertions into the legacy renderer
compared with itself.

The anchor inventory is split by prefix rather than globbing every `sync_pipeline_*.xml`, which the
review showed would **deadlock #139F**: that slice must commit legacy-rendered
`sync_pipeline_listener_*.xml` fixtures *before* it touches the routing gate (the same pre-cutover
discipline that kept #139C's anchors from being self-confirming), and under a single glob the new
fixture fails the equality while adding it as a corpus case fails the canonical-routing assertion. The
legacy inventory is asserted **empty** rather than ignored, so those fixtures landing is a deliberate
edit here, never a silently widened glob.

Every guard was mutation-checked, including the two that had already survived an earlier round.

**Capability reconciliation.** Re-verified at `1bd0b69`: `catcherrors` has canonical emission (#142),
so the ledger no longer lists it as a blocker for the ordinary dialect. `start_listen`, connector
`dynamic_path` and Notify do **not** — `_REGISTRATIONS` carries 18 keys with neither `start_listen` nor
`notify`, and no connector input model has a dynamic-path field. Those three are **unowned capability
work**, not #139 adapter work (#139 lists new capability as out of scope), and both the listener
cutover and the ordinary `database_to_api_sync` adapter remain gated behind them. The listener routing
and adapter-refusal tests are deliberately left byte-unchanged: they are the evidence that the legacy
fallback is *intentional* rather than incidental.

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
legacy-unreachable, which is what this census measures — and §10 A of the sibling document uses the
SAME legacy scope, so the two agree. Both also record the direct-API reachability separately, because
that is a different question with a different answer and conflating them is what made three earlier
drafts of these sections contradict each other.

The 5 implemented codes and the 2 inert exemption rows ARE wired correctly — forcing the condition
makes each fire. That argument does **not** extend to the single group-B code: `finding()` accepts
any code outside the compile family (registered or not), so building a synthetic report carrying it
proves REGISTRATION, not wiring. `PROCESS_IR_SEMANTIC_LINEAGE_AMBIGUOUS_LAST_WRITE` has no rule
behind it at all.

None of this is a regression — the baseline had no such checks. It is a statement of how much of the
new surface is currently exercised, and by what.

---

## 8. #144 M12.9 — capability-gated `SystemTopologySpecV1` (planning-only, dark)

Placed as its own section rather than folded into §7. §7.2's tally is re-derived from the text
between its own markers by `tests/test_m12_migration_matrix_evidence.py`, and §7 is #143's ledger —
adding a `TOPOLOGY_*` row inside it would both break that arithmetic and attribute #144's codes to
#143's migration.

### 8.1 What landed

A strict, versioned authored contract (`src/boomi_mcp/models/system_topology.py`) plus a pure planner
package (`src/boomi_mcp/compiler/system_topology/`). Nothing at runtime constructs or consumes
either. No MCP tool or action is registered; no existing schema, wrapper signature or behavior
changed.

| Artifact | Verdict |
|---|---|
| `SystemTopologySpecV1` — 10 object kinds, 8 relation kinds, closed discriminated unions | new |
| `TOPOLOGY_*` error family — 14 codes, `category="topology"`, owner `#144` | new |
| Topology planner (validate / plan, pure, no I/O) | new |
| Read-only discovery Protocol — 7 reads, no shipped adapter | new |
| `IntegrationSpecV1`, `_build_plan`, `_topological_order`, `orchestrate_deploy` | **unchanged** |
| `ProcessIRV1` schema and diagnostics | **unchanged** |
| Free-form `endpoints` / `flows` / `runtime` / `validation_rules` | **not reinterpreted** |

### 8.2 Authority boundary

ADR-001 §3 assigns topology "a future capability-gated, planning-only topology authority (#144). It
never mutates runtime state and never feeds the process compiler." Three graphs stay disjoint —
ProcessIR CFG edges, ComponentPlan build dependencies (`depends_on`), and topology runtime
(`process_call`) edges — enforced by vocabulary, import isolation and byte-independence proofs
(`tests/test_system_topology_graph_namespaces.py`).

### 8.3 Evidence corrections to this milestone's stated census

The #144 research gate contradicted six claims in the issue text. Each weakened a claim; none
strengthened one. Full detail in `SYSTEM_TOPOLOGY_V1.md` §4.

| Claim | Live finding | Effect on the contract |
|---|---|---|
| component counts | page-capped (`documentcache`: 100 of 186, `has_more: true`) | no census number is hard-coded; pagination provenance recorded per query |
| ASC/listener bindings exist | **zero `webservice` components in either profile** | `api_service` live leg `unavailable`; route needs a typed-builder/XML witness |
| ProcessCall observable live | dependency API is a flat one-level mixed-type list with no edge kind | dependency rows are `corroborating_only`; the API is a registered **unsupported** witness |
| six inactive schedules | all six carry an **empty** `schedules: []` body | schedule *content* is `guidance-only` and absent from the schema |
| zero active deployments | **false as stated** — `work` has 18 inactive records, but `renera` has an active one. Deployment reads establish that records exist and can be listed; they establish nothing about creating one | verdict unchanged (`plannable-only`, no apply path); the published *reason* was corrected — see QA #207 |
| zero queue components | confirmed | queue/Event Streams permanently `gated-no-evidence` in V1 |

Two surfaces have no capture at all and are recorded as such: `account_capability_limits`
(`not_captured` on all three legs) and `listener_status_as_api_route_witness` (`conflicting` source
and documentation).

### 8.4 `TOPOLOGY_*` ledger

All 14 codes are **new**; none is re-homed, delegated, or migrated from a prior family. #144 is the
family's sole introducer (ADR-001 §7), asserted as a biconditional.

| Code | Verdict |
|---|---|
| `TOPOLOGY_SCHEMA_UNKNOWN_OBJECT` | new |
| `TOPOLOGY_SCHEMA_UNKNOWN_RELATION` | new |
| `TOPOLOGY_SCHEMA_UNKNOWN_FIELD` | new |
| `TOPOLOGY_SCHEMA_INVALID_CARDINALITY` | new |
| `TOPOLOGY_SCHEMA_DUPLICATE_KEY` | new |
| `TOPOLOGY_SCHEMA_VERSION_UNSUPPORTED` | new |
| `TOPOLOGY_SCHEMA_INVALID` | new |
| `TOPOLOGY_REFERENCE_NOT_FOUND` | new |
| `TOPOLOGY_REFERENCE_TYPE_MISMATCH` | new |
| `TOPOLOGY_RELATION_UNSUPPORTED` | new |
| `TOPOLOGY_CAPABILITY_GATED` | new |
| `TOPOLOGY_ENVIRONMENT_MISMATCH` | new |
| `TOPOLOGY_DEPENDENCY_CYCLE` | new |
| `TOPOLOGY_APPLY_NOT_SUPPORTED` | new |

Census: **14 new, 0 re-homed, 0 delegated, 0 unchanged.**

### 8.5 Evidence

Every claim above is pinned by a collectible test:

- `tests/test_error_taxonomy.py::test_issue_144_adds_exactly_fourteen_codes`
- `tests/test_error_taxonomy.py::test_issue_144_owns_the_whole_topology_family`
- `tests/test_system_topology_capabilities.py::test_registry_covers_every_object_kind_exactly`
- `tests/test_system_topology_capabilities.py::test_api_service_is_emittable_but_its_live_leg_is_unavailable`
- `tests/test_system_topology_capabilities.py::test_dependency_api_is_registered_as_an_unsupported_process_call_witness`
- `tests/test_system_topology_discovery.py::test_pagination_records_returned_total_and_truncation`
- `tests/test_system_topology_discovery.py::test_an_empty_schedule_body_is_recorded_without_inventing_content`
- `tests/test_system_topology_graph_namespaces.py::test_changing_the_build_graph_leaves_the_runtime_order_byte_identical`
- `tests/test_system_topology_no_mutation.py::test_planning_performs_no_write_network_or_process_operation`
- `tests/test_system_topology_regressions.py::test_integration_spec_still_accepts_its_permissive_payload`

---

# 9. Issue #145 (M12.10) — Typed executable recipe contributions

Completes the classification the M12 research gate calls for: every archetype,
composition, pattern, primitive, planner, guidance and capability-registry surface is
now labelled, and the label decides what the surface is *allowed to do*.

Contract detail lives in `docs/architecture/TYPED_RECIPE_CONTRIBUTIONS_V1.md`. This
section is the inventory.

## 9.1 Pattern infrastructure and archetypes

| Surface | Classification | Evidence |
|---|---|---|
| `patterns/base.py` — `PatternMetadata`, `PatternIOContract`, `PatternExample` | ADVISORY | Descriptive only. `PatternExample` forces `example_only_not_reusable_template`; `PatternIOContract` is not compiler-enforced. |
| `patterns/base.py::ArchetypePattern.emit_spec`, `PrimitivePattern.emit_components` | EXECUTABLE RECIPE (legacy/untyped) | Code-defined materialization, but returns a whole open `IntegrationSpecV1` rather than declared contribution effects. |
| `patterns/base.py::PrimitivePattern.emit_fragment` | COMPATIBILITY ADAPTER | Free-form dict convention (`components` / `process_config` / `depends_on` / `metadata`). **Retained unchanged**; docstring now says so. |
| `patterns/registry.py` | COMPATIBILITY ADAPTER | Dynamic package scan, `archetype\|primitive` only, no version selector, no provenance, no output declaration. |
| `patterns/errors.py` | CONSTRAINT-ONLY support | Sanitizes pydantic errors; drops raw input. |
| `patterns/archetypes/api_to_api_sync.py` | **EXECUTABLE RECIPE — MIGRATED** | Routes through `boomi.archetype.api_to_api_sync@0.1.0`. |
| `patterns/archetypes/api_to_database_sync.py` | **EXECUTABLE RECIPE — MIGRATED** | Routes through `boomi.archetype.api_to_database_sync@0.1.0`. |
| `patterns/archetypes/database_to_api_sync.py` | EXECUTABLE RECIPE + COMPATIBILITY ADAPTER | Emits a whole spec and retains legacy `flow_sequence` lowering, retry/DLQ/watermark advisory intent. Not migrated. |
| `patterns/archetypes/http_listener_to_db.py`, `http_listener_to_rest.py` | EXECUTABLE RECIPE (legacy/untyped) | Not migrated. |
| `patterns/archetypes/stub_minimal.py` | COMPATIBILITY ADAPTER / test fixture | Registry-visible, explicitly non-executable. Not dead. |
| `patterns/composition.py` | **EXECUTABLE RECIPE — MIGRATED** | Routes through `boomi.compose.db_rest_fanout@1.0.0` behind an unchanged public adapter. |
| `patterns/recipe_bridge.py` | COMPATIBILITY ADAPTER | The one place legacy input is materialized and a safe recipe input is projected. |
| `categories/integration_authoring.py` list/get/build/compose | COMPATIBILITY ADAPTER | Public read-only facades. Now additionally publish recipe provenance and registry skew. |

## 9.2 Primitives

| Files | Classification |
|---|---|
| `db_extract`, `db_write`, `field_map`, `rest_fetch`, `rest_send`, `soap_fetch`, `soap_send`, `wss_listen`, `xml_json_convert` | EXECUTABLE RECIPE — internal legacy building blocks; remain internal materializers |
| `branch`, `decision`, `flow_control`, `return_documents`, `throw_exception` | COMPATIBILITY ADAPTER — free-form `emit_fragment()` |
| `document_cache_lookup/put/remove/retrieve` | COMPATIBILITY ADAPTER — free-form legacy cache fragments |
| `data_process` | COMPATIBILITY ADAPTER — accepts raw Groovy; **cannot** become a native recipe input under the security contract |
| `inbound_validate` | CONSTRAINT-ONLY — emits validation metadata, materializes nothing |
| `operational.py` (`schedule_envelope`, `watermark_state`, `error_classifier`, `dlq_writer`, `run_metadata`) | ADVISORY — records intent only; no activation, mutation, wiring or persistence |
| `_helpers`, `_soap_common`, `rest_runtime` | COMPATIBILITY ADAPTER support — helpers, not registered patterns |

## 9.3 Planner, doctrine and guidance

| Surface | Classification |
|---|---|
| `meta_tools::plan_integration_design_action` | ADVISORY — returns recommendations and now exact `recommended_recipes` **references**; never a contribution or an input |
| `meta_tools::get_schema_template` skeletons, workflow sequences, operating doctrine | ADVISORY |
| `kb/design_doctrine.py`, `kb/account_governance.py`, `kb/operational_gotchas.py` | ADVISORY |
| `categories/marketplace.py::search_marketplace_recipes` | ADVISORY — public reference patterns; never installed, registered or executed |
| `categories/integration_import.py` | COMPATIBILITY ADAPTER — read-only migration planner; now carries `selected_recipe_ref` for the two migrated presets, `null` otherwise |
| `models/pipeline_models.py`, `IntegrationSpecV1.pipeline` | COMPATIBILITY ADAPTER / inert view |
| `docs/companion/**` | ADVISORY — vendored reference |
| `docs/archive/**` | DEPRECATED-DEAD |
| `meta_tools::_PROCESS_CREATE_REMOVED` | DEPRECATED-DEAD — must never be revived as a recipe |

## 9.4 Canonical authorities (unchanged by this issue)

`models/process_ir.py`, `compiler/process_ir/pipeline.py`, `emitter_registry.py`,
`body_capabilities.py`, `connector_capabilities.py`, `models/system_topology.py`,
`compiler/system_topology/`, `process_graph_verifier.py`, and the component builders all
remain CONSTRAINT-ONLY authorities. The recipe layer funnels **into** them; it neither
extends nor exempts them.

`semantic_validation/validation_policy.py` and `legacy_adapters/` remain COMPATIBILITY
ADAPTERS. **A native recipe never receives a `LegacyValidationPolicyV1`** — the engine has
no parameter through which one could be passed.

## 9.5 Hidden assumptions preserved

- Component identities are dependency keys; in-spec references use exact `$ref:KEY` tokens.
- `depends_on` must cover every `$ref`, but materialization order is the **sorted**
  topological order from `_topological_order`, not declaration order.
- Reuse connections stay `action="create"` + `config.reference_only=True`.
- Composition supports one `db_source`, one `transform`, 2–25 `rest_target` — 24 with a
  cache handoff, which spends one Branch leg on the staging put.
- The first target is privileged: fixed `target_*` keys and the only watermark-derived query.
- Cache mode uses the fixed key `handoff_document_cache`; the staging leg is inserted
  immediately before the first consumer; Branch legs run sequentially.
- Standard two-target stream component order, cache insert position, and both preset orders
  are pinned byte-for-byte by `tests/fixtures/recipe_parity/*` captured at baseline
  `060dabad64e028d83d192e5820d8f37df64d54d3`.

## 9.6 Error-code census

| Code | Status |
|---|---|
| `RECIPE_NOT_FOUND` | new |
| `RECIPE_VERSION_UNAVAILABLE` | new |
| `RECIPE_CAPABILITY_GATED` | new |
| `RECIPE_INPUT_INVALID` | new |
| `RECIPE_CONTRIBUTION_INVALID` | new |
| `RECIPE_PATCH_TARGET_NOT_FOUND` | new |
| `RECIPE_PATCH_CONFLICT` | new |
| `RECIPE_CONSTRAINT_FAILED` | new |
| `RECIPE_OUTPUT_NONDETERMINISTIC` | new |

Census: **9 new, 0 re-homed, 0 delegated, 0 unchanged.**

## 9.7 Evidence

- `tests/test_error_taxonomy.py::test_issue_145_adds_exactly_ten_codes`
- `tests/test_error_taxonomy.py::test_issue_145_owns_the_whole_recipe_family`
- `tests/test_recipe_contribution_models.py::test_contribution_kinds_are_derived_from_the_union_not_hand_listed`
- `tests/test_recipe_contribution_models.py::test_recipe_component_types_are_pinned_against_the_builder_authority`
- `tests/test_recipe_registry.py::test_registry_revision_is_invariant_under_registration_order`
- `tests/test_recipe_registry.py::test_equal_versions_with_different_code_are_a_mismatch_not_a_match`
- `tests/test_recipe_registry.py::test_doctrine_prose_never_resolves_to_a_recipe`
- `tests/test_recipe_registry.py::test_the_registry_module_exposes_no_runtime_registration_api`
- `tests/test_recipe_process_patch_composition.py::test_one_recipe_conflicting_with_itself_still_names_two_producers`
- `tests/test_recipe_security.py::test_compose_sentinels_never_reach_the_recipe_side`
- `tests/test_recipe_validation_gate.py::test_compile_is_always_called_with_no_validation_policy`
- `tests/test_recipe_validation_gate.py::test_a_blocking_planned_step_produces_zero_execute_component_calls`
- `tests/patterns/test_recipe_preset_parity.py::test_l4_legacy_cache_arm_still_requires_its_exemption`

---

# 10. Issue #146 (M12.11) — MCP authoring, planning, compile and verify surfaces

## 10.1 Public surfaces touched

| Surface | Change | Compatibility |
|---|---|---|
| `server.py:build_integration` | docstring only | signature **byte-identical** — `(profile, action, config=None)`. The typed input rides inside the existing `config` JSON precisely so this stays true. |
| `server.py:list_capabilities` | one trailing optional param `expected_capability_revision=None` | additive; a no-argument call is unchanged |
| `server.py:get_schema_template` | docstring only | selectors are additive |
| `server.py:plan_integration_design` | unchanged signature | two additive response fields, neither `required` |
| `server.py:build_from_archetype` | unchanged | — |
| `build_integration_action` | new `compile` branch; typed `plan`/`apply` branches | selected **only** by an explicit `config.authoring_request`; a legacy request never enters the typed path |

## 10.2 Frozen behavior preserved

- `plan`, `apply`, `verify` keep their existing request and response contracts. `compile` is additive.
- `_build_plan`, `_apply_plan` and `_verify_build` legacy bodies are untouched for legacy requests.
- Legacy build records keep exactly their five original keys (`created_at`, `profile`, `spec`,
  `results`, `execution_order`). Typed builds add one optional `authoring` key.
- `verify` gains `authoring_provenance` **only** for typed builds; the key is ABSENT (not null) for
  legacy builds, so no existing assertion changes.
- `PLAN_INTEGRATION_DESIGN_OUTPUT_SCHEMA["required"]` is unchanged — the two new properties are
  optional, because adding a required property is a breaking change for every validating caller.
- Existing `get_schema_template` selectors return structurally identical payloads; the new
  `schema_hash` / `revision_binding` metadata rides on **new** selectors only.

## 10.3 New selectors and capabilities

Selectors: `ProcessIRV1`, `SystemTopologySpecV1`, `validation_report`, `AuthoringRequestV1`,
`AuthoringPlanResultV1`, `AuthoringCompileResultV1`, `AuthoringRevisionBindingV1`,
`AuthoringBuildProvenanceV1`, `authoring_workflow`. An optional `@<version>` suffix is accepted; a
bare selector stays valid.

Capabilities published as `unsupported` rather than omitted (an absent capability is
indistinguishable from one the client forgot to ask about):

| Capability | Reason code |
|---|---|
| `authoring.system_topology.deploy` | `TOPOLOGY_APPLY_NOT_SUPPORTED` |
| `authoring.typed_apply.process_materialization` | `PROCESS_KIND_REQUIRED` |

## 10.4 Dark-shipping pins retired

`SystemTopologySpecV1` shipped DARK in #144 behind two pins whose own docstring named this issue as
their successor (*"#144 wires itself to nothing; #146 owns the wiring"*). #146 retires the half that
has done its job and **keeps the half that has not**: category modules and `server.py` may now NAME
the topology schema selector, but must still not reach the planner. The consumer set is now pinned as
a closed, enumerated list (`recipes/engine.py`, `recipes/registry.py`, `models/__init__.py`,
`authoring/workflow.py`) so a new unreviewed consumer fails.

## 10.5 Error-code census

| Code | Status |
|---|---|
| `AUTHORING_SCHEMA_VERSION_UNAVAILABLE` | new |
| `AUTHORING_CAPABILITY_REVISION_MISMATCH` | new |
| `AUTHORING_LIVE_DEPLOYMENT_DRIFT` | new |
| `AUTHORING_REQUIRED_DECISION_MISSING` | new |
| `AUTHORING_COMPILE_BLOCKED` | new |
| `AUTHORING_PLAN_STALE` | new |
| `AUTHORING_APPLY_VALIDATION_REQUIRED` | new |

Census: **7 new, 0 re-homed, 0 delegated, 0 unchanged.** #146 introduces no code in any other
family — canonical ProcessIR / topology / recipe codes travel verbatim as value-free `cause_codes`.

## 10.6 Known limits recorded rather than hidden

- A **direct `ProcessIRV1` intent is plan/compile-only.** Process materialization emits XML from the
  component plan, so applying one would create an artifact the compile hash does not describe.
  Refused by intent kind, reported as a capability gap at plan time.
- Artifact fingerprints describe the canonical **emission plan**, not emitted XML. Live drift is a
  separate comparison of apply-time vs verify-time live component XML.
- `_BUILD_REGISTRY` remains session-scoped; provenance inherits that limit. The binding does not
  depend on it — apply recomputes rather than looking anything up.
- An unresolvable `$ref` passes semantic validation (reference resolution is not one of its phases)
  and is caught at compile. A typed apply runs through compile, so it cannot reach a mutation.

## 10.7 Evidence

- `tests/test_m12_11_wrapper_contracts.py` — signature freeze + four-surface action parity
- `tests/test_m12_11_schema_templates.py` — served schema == runtime model schema, version selection
- `tests/test_m12_11_capabilities.py` — six-archetype reporting, `not_requested` honesty, simulated
  four-vs-six mismatch
- `tests/test_m12_11_authoring_plan.py` — zero-mutation spies, hash stability under key reordering
- `tests/test_m12_11_authoring_compile.py` — #143 gate not bypassable, no XML in any response
- `tests/test_m12_11_revision_binding.py` — every stale/mismatched/replayed binding refused, zero mutation
- `tests/test_m12_11_apply_verify.py` — preflight-before-materializer ordering, provenance, drift
- `tests/test_m12_11_diagnostics_security.py` — ordering, secret exclusion, terminology


---

# 11. Issue #149 (M12.12) — pre-deletion legacy reachability inventory and allowlist baseline

This section is the **instrument** that makes #160's deletion a checked-off subtraction rather
than a discovery exercise. It PASSES with every legacy path still present: post-deletion
zero-reachability enforcement (the `M12_LEGACY_PATH_REACHABLE` gate) belongs to #160, not here.

Everything below is DERIVED. The tables in §11.2–§11.6 are emitted from
`tests/fixtures/m12_12/legacy_reachability_inventory.json` by
`tests/_m12_12_legacy_inventory.py --emit-markdown` and pasted verbatim;
`tests/test_issue_149_legacy_reachability_freeze.py::test_the_markdown_tables_are_regenerable_from_the_json`
fails if anyone hand-edits them apart from the machine record.

## 11.1 Baseline identity and derivation contract

| Field | Value |
|---|---|
| Baseline commit | `9711a9c0cb6c88dda41ada94d88694915b659f36` (branch `dev`) |
| Capture date | 2026-08-12 |
| Schema / scanner version | 1 / 1 |
| Scan roots | `server.py`, `src/boomi_mcp/**/*.py` (187 files), `examples/**/*.json` (8 files) |
| Census rows | 237 |
| Ledger rows (§11.2–§11.4 grain) | 130 |
| Component-XML write routes | 21 |
| Frozen served artifacts | 94 |
| Machine record | `tests/fixtures/m12_12/legacy_reachability_inventory.json` |

**The watched vocabulary is derived from the runtime authority, never typed out.** Builders come
from `PROCESS_FLOW_BUILDERS` and `builders.__all__`, legacy emitters from
`dir(process_emitters.legacy)`, legacy semantic validation from the `legacy_bridge` module, and the
raw Component-XML sinks from the installed SDK's `ComponentService` (mutating verbs only). A builder
that #151–#158 registers is therefore watched the day it registers, not the day someone remembers to
extend a list. `assert_vocabulary_non_vacuous()` fails the suite if any family derives empty, so the
derivation cannot fail open.

**Nothing positional is frozen.** The semantic key is `(census, path, symbol, form)` plus a call
count. Line numbers ride along as `evidence_line` for human navigation and are excluded from
equality — an inserted blank line never breaks the gate (pinned by
`test_an_inserted_blank_line_does_not_break_the_freeze`), while a second call inside an existing
function always does (pinned by
`test_a_second_call_in_an_existing_function_is_reported_as_a_count_change`). **Baseline line numbers
in the tables below will rot as files grow; the row IDs and qualified symbols are authoritative.**

**Re-baselining after an intentional change** (same doctrine as §1's #135 freeze):

```bash
PYTHONPATH=src .venv/bin/python tests/_m12_12_legacy_inventory.py \
    --write tests/fixtures/m12_12/legacy_reachability_inventory.json
PYTHONPATH=src .venv/bin/python tests/_m12_12_legacy_inventory.py --emit-markdown
```

then paste the regenerated tables into §11.2–§11.6 **in the same change**. The pin and the ledger
move together or `test_the_ledger_and_the_json_are_two_way_complete` fails.

### Two corrections to the issue text, on the record

1. **`integration_builder.py:3520` does not dispatch caller raw XML.** Issue #149 states that the
   process and connector-settings branches "dispatch raw XML before the `:4140` fall-through ever
   runs". Verified at HEAD: `:3520-3564` is the structured `comp.type == "process"` arm — it reads
   `payload["process_kind"]`/`["process_type"]`, resolves a builder at `:3532` and calls
   `builder_cls.build(...)` at `:3561`. Caller `config.xml` is never forwarded there. The
   *conclusion* the issue draws is still correct and unchanged: #160's content guard must precede
   all type dispatch, because the **connector** arm and the **generic fall-through** are both
   raw-process-capable (routes `WRT-manage-connector-create`, `WRT-manage-connector-update`,
   `WRT-build-integration-generic`).
2. **`_update_component_xml` dormancy is production-scoped.** It has zero callers under `src/` or in
   `server.py`, which is what `test_the_dormant_shared_writer_has_no_production_callers` enforces. It
   does have two deliberate callers in `tests/test_component_raw_transport.py:59,71`, which are
   evidence the transport works, not violations.

## 11.2 Legacy renderer and semantic-validation reachability

Registry lookups, renderer calls, legacy `_emit_*` reachability, legacy semantic validation, and the
fail-closed residue class. The four registry-resolution sites #149 names —
`integration_builder.py` `_resolve_preservation_policy` (`:1234`), `build_structured_update_xml`
(`:3152`), `_execute_component` (`:3532`) and `_process_component_preflight` (`:5422`) — are pinned
individually by `test_the_scan_universe_is_complete_and_non_vacuous`.

The WSS-listener fallback is `SyncPipelineBuilder.build` delegating to `ProcessFlowBuilder.build`
(`process_flow_builder.py:6049-6050`), gated by `_sync_pipeline_is_canonical` at `:5201-5219` — the
row `renderer_call | …process_flow_builder.py | SyncPipelineBuilder.build | ProcessFlowBuilder.build(...)`
below.

`unclassified_dynamic` is the **fail-closed residue class**: a watched name reached through
`getattr` cannot be resolved statically, so it is recorded rather than dropped, and the set is
frozen. `test_an_unresolvable_dynamic_sink_access_is_reported_not_ignored` proves the class is live
rather than aspirational.

| Ledger ID | Census | Path | Symbol | Sites | Baseline line | Owning issue | Disposition |
|---|---|---|---|---|---|---|---|
| LG-aee1eb03 | legacy_emitter | src/boomi_mcp/categories/components/builders/process_emitters/legacy.py | _emit_setproperties | 3 | 532 | #160 | delete with the legacy renderer |
| LG-420f1954 | legacy_emitter | src/boomi_mcp/categories/components/builders/process_emitters/legacy.py | _emit_setproperties_step | 3 | 472 | #160 | delete with the legacy renderer |
| LG-a8efa464 | legacy_emitter | src/boomi_mcp/categories/components/builders/process_flow_builder.py | <module> | 29 | 72 | #160 | delete the legacy semantic shell |
| LG-e3bb78f3 | legacy_emitter | src/boomi_mcp/categories/components/builders/process_flow_builder.py | _emit_branch_shapes | 1 | 3560 | #160 | delete the legacy semantic shell |
| LG-09c051cf | legacy_emitter | src/boomi_mcp/categories/components/builders/process_flow_builder.py | _emit_catch_leg | 5 | 4647 | #160 | delete the legacy semantic shell |
| LG-43cb4e79 | legacy_emitter | src/boomi_mcp/categories/components/builders/process_flow_builder.py | _emit_connector_scoped_try_catch_shapes | 3 | 4761 | #160 | delete the legacy semantic shell |
| LG-51f10b57 | legacy_emitter | src/boomi_mcp/categories/components/builders/process_flow_builder.py | _emit_decision_shapes | 1 | 3637 | #160 | delete the legacy semantic shell |
| LG-e1ff295b | legacy_emitter | src/boomi_mcp/categories/components/builders/process_flow_builder.py | _emit_flow_shape | 14 | 3457 | #160 | delete the legacy semantic shell |
| LG-e9731d65 | legacy_emitter | src/boomi_mcp/categories/components/builders/process_flow_builder.py | _emit_try_catch_shapes | 2 | 4517 | #160 | delete the legacy semantic shell |
| LG-9f90fa64 | legacy_semantic_validation | src/boomi_mcp/categories/integration_builder.py | _process_component_preflight | 1 | 5552 | #160 | delete with the legacy semantic shell |
| LG-599ecc0d | legacy_semantic_validation | src/boomi_mcp/categories/integration_builder.py | _process_ir_semantic_error | 1 | 5578 | #160 | delete with the legacy semantic shell |
| LG-48cc5eb8 | legacy_transitive_call | server.py | analyze_component | 1 | 1892 | #160 | delete or re-home with the callee |
| LG-853101d8 | legacy_transitive_call | server.py | apply_component_edit | 1 | 2050 | #160 | delete or re-home with the callee |
| LG-b6bb8a5c | legacy_transitive_call | server.py | build_integration | 1 | 2481 | #160 | delete or re-home with the callee |
| LG-489d0ece | legacy_transitive_call | server.py | compose_archetypes | 1 | 2604 | #160 | delete or re-home with the callee |
| LG-f5622f58 | legacy_transitive_call | server.py | invoke_boomi_api | 1 | 3411 | #160 | delete or re-home with the callee |
| LG-963c3aaf | legacy_transitive_call | server.py | manage_component | 1 | 1791 | #160 | delete or re-home with the callee |
| LG-2976fe39 | legacy_transitive_call | server.py | manage_connector | 1 | 2154 | #160 | delete or re-home with the callee |
| LG-ceea4ac3 | legacy_transitive_call | server.py | prepare_component_edit | 1 | 1970 | #160 | delete or re-home with the callee |
| LG-af3aef13 | legacy_transitive_call | src/boomi_mcp/authoring/workflow.py | _legacy_plan_echo | 1 | 662 | #160 | delete or re-home with the callee |
| LG-b7cd58ae | legacy_transitive_call | src/boomi_mcp/authoring/workflow.py | compile_authoring_request_v1 | 1 | 1444 | #160 | delete or re-home with the callee |
| LG-14572d6c | legacy_transitive_call | src/boomi_mcp/authoring/workflow.py | plan_authoring_request_v1 | 1 | 1144 | #160 | delete or re-home with the callee |
| LG-f57fd778 | legacy_transitive_call | src/boomi_mcp/authoring/workflow.py | preflight_typed_apply_v1 | 1 | 1616 | #160 | delete or re-home with the callee |
| LG-3f49de44 | legacy_transitive_call | src/boomi_mcp/categories/components/analyze_component.py | analyze_component_action | 1 | 900 | #160 | delete or re-home with the callee |
| LG-bb10b9ca | legacy_transitive_call | src/boomi_mcp/categories/components/builders/process_flow_builder.py | ProcessFlowBuilder.build | 3 | 1071 | #160 | delete the legacy semantic shell |
| LG-a4d26b62 | legacy_transitive_call | src/boomi_mcp/categories/components/builders/process_flow_builder.py | _emit_branch_shapes | 2 | 3547 | #160 | delete the legacy semantic shell |
| LG-020c9559 | legacy_transitive_call | src/boomi_mcp/categories/components/builders/process_flow_builder.py | _emit_connector_scoped_try_catch_shapes | 7 | 4753 | #160 | delete the legacy semantic shell |
| LG-2296966e | legacy_transitive_call | src/boomi_mcp/categories/components/builders/process_flow_builder.py | _emit_decision_leg | 1 | 3589 | #160 | delete the legacy semantic shell |
| LG-8375bff7 | legacy_transitive_call | src/boomi_mcp/categories/components/builders/process_flow_builder.py | _emit_decision_shapes | 3 | 3623 | #160 | delete the legacy semantic shell |
| LG-2dd324b1 | legacy_transitive_call | src/boomi_mcp/categories/components/builders/process_flow_builder.py | _emit_flow_shape | 2 | 3476 | #160 | delete the legacy semantic shell |
| LG-0b2a5bd8 | legacy_transitive_call | src/boomi_mcp/categories/components/builders/process_flow_builder.py | _emit_linear_shapes | 1 | 3518 | #160 | delete the legacy semantic shell |
| LG-3ac90371 | legacy_transitive_call | src/boomi_mcp/categories/components/builders/process_flow_builder.py | _emit_try_catch_shapes | 2 | 4511 | #160 | delete the legacy semantic shell |
| LG-225b9aa0 | legacy_transitive_call | src/boomi_mcp/categories/components/connectors.py | create_connector | 3 | 382 | #160 | delete or re-home with the callee |
| LG-b0e06309 | legacy_transitive_call | src/boomi_mcp/categories/components/connectors.py | manage_connector_action | 2 | 727 | #160 | delete or re-home with the callee |
| LG-6f781717 | legacy_transitive_call | src/boomi_mcp/categories/components/manage_component.py | clone_component | 1 | 376 | #160 | delete or re-home with the callee |
| LG-9afe2de9 | legacy_transitive_call | src/boomi_mcp/categories/components/manage_component.py | create_component | 6 | 66 | #160 | delete or re-home with the callee |
| LG-2cc27d39 | legacy_transitive_call | src/boomi_mcp/categories/components/manage_component.py | manage_component_action | 3 | 459 | #160 | delete or re-home with the callee |
| LG-dce7aa49 | legacy_transitive_call | src/boomi_mcp/categories/components/safe_edit_component.py | _compute_merged_xml | 1 | 314 | #160 | retract the served raw-XML steer; body edits move to canonical apply |
| LG-a3fb531d | legacy_transitive_call | src/boomi_mcp/categories/components/safe_edit_component.py | apply_component_edit_action | 1 | 544 | #160 | retract the served raw-XML steer; body edits move to canonical apply |
| LG-0b6058fb | legacy_transitive_call | src/boomi_mcp/categories/components/safe_edit_component.py | prepare_component_edit_action | 1 | 381 | #160 | retract the served raw-XML steer; body edits move to canonical apply |
| LG-77376652 | legacy_transitive_call | src/boomi_mcp/categories/integration_authoring.py | compose_archetypes_action | 1 | 479 | #160 | delete or re-home with the callee |
| LG-3cf79d6e | legacy_transitive_call | src/boomi_mcp/categories/integration_builder.py | _apply_plan | 3 | 6896 | #160 | delete or re-home with the callee |
| LG-3a7213a4 | legacy_transitive_call | src/boomi_mcp/categories/integration_builder.py | _authored_process_validation_error | 1 | 5221 | #160 | delete or re-home with the callee |
| LG-86e93983 | legacy_transitive_call | src/boomi_mcp/categories/integration_builder.py | _build_plan | 4 | 5648 | #160 | delete or re-home with the callee |
| LG-ab4b2334 | legacy_transitive_call | src/boomi_mcp/categories/integration_builder.py | _compile_authoring | 1 | 7792 | #160 | delete or re-home with the callee |
| LG-f16d0493 | legacy_transitive_call | src/boomi_mcp/categories/integration_builder.py | _execute_component | 23 | 3532 | #160 | delete or re-home with the callee |
| LG-308b6e78 | legacy_transitive_call | src/boomi_mcp/categories/integration_builder.py | _plan_authoring | 1 | 7730 | #160 | delete or re-home with the callee |
| LG-f1e10478 | legacy_transitive_call | src/boomi_mcp/categories/integration_builder.py | _process_component_preflight | 2 | 5422 | #160 | delete or re-home with the callee |
| LG-4ef294c2 | legacy_transitive_call | src/boomi_mcp/categories/integration_builder.py | _resolve_preservation_policy | 1 | 1234 | #160 | delete or re-home with the callee |
| LG-1f766152 | legacy_transitive_call | src/boomi_mcp/categories/integration_builder.py | build_integration_action | 4 | 7959 | #160 | delete or re-home with the callee |
| LG-8796f643 | legacy_transitive_call | src/boomi_mcp/categories/integration_builder.py | build_structured_update_xml | 1 | 3152 | #160 | delete or re-home with the callee |
| LG-568713cc | legacy_transitive_call | src/boomi_mcp/categories/shared_resources.py | _action_update_channel | 1 | 471 | #160 | delete or re-home with the callee |
| LG-dec16977 | legacy_transitive_call | src/boomi_mcp/compiler/process_ir/legacy_adapters/authority.py | evaluate_pipeline_authority | 2 | 392 | #151 | re-home onto the neutral extraction |
| LG-8c220ac8 | legacy_transitive_call | src/boomi_mcp/patterns/archetypes/api_to_api_sync.py | ApiToApiSyncArchetype.emit_spec | 2 | 1661 | #159 | migrate the archetype to canonical ProcessIR |
| LG-492bfbd5 | legacy_transitive_call | src/boomi_mcp/patterns/archetypes/api_to_database_sync.py | ApiToDatabaseSyncArchetype.emit_spec | 2 | 836 | #159 | migrate the archetype to canonical ProcessIR |
| LG-492d9518 | legacy_transitive_call | src/boomi_mcp/patterns/archetypes/database_to_api_sync.py | DatabaseToApiSyncArchetype.emit_spec | 1 | 3920 | #159 | migrate the archetype to canonical ProcessIR |
| LG-d88fe3f1 | legacy_transitive_call | src/boomi_mcp/patterns/recipe_bridge.py | run_sync_preset_recipe | 1 | 244 | #160 | delete or re-home with the callee |
| LG-a6b08ee4 | registry_lookup | src/boomi_mcp/categories/components/builders/process_flow_builder.py | get_process_flow_builder | 2 | 6152 | #160 | delete the legacy semantic shell |
| LG-e131eb91 | registry_lookup | src/boomi_mcp/categories/integration_builder.py | _execute_component | 3 | 3532 | #153 | replace with canonical ProcessIR materialization/apply |
| LG-85b86f7b | registry_lookup | src/boomi_mcp/categories/integration_builder.py | _process_component_preflight | 3 | 5318 | #153 | replace with canonical ProcessIR materialization/apply |
| LG-f76e62cd | registry_lookup | src/boomi_mcp/categories/integration_builder.py | _resolve_preservation_policy | 1 | 1234 | #153 | replace with canonical ProcessIR materialization/apply |
| LG-4e882820 | registry_lookup | src/boomi_mcp/categories/integration_builder.py | build_structured_update_xml | 3 | 3148 | #153 | replace with canonical ProcessIR materialization/apply |
| LG-e4a0a304 | renderer_call | src/boomi_mcp/categories/components/builders/process_flow_builder.py | SyncPipelineBuilder.build | 1 | 6050 | #160 | delete the legacy semantic shell |
| LG-088efd1d | renderer_call | src/boomi_mcp/categories/components/builders/process_flow_builder.py | SyncPipelineBuilder.validate_config | 1 | 5994 | #160 | delete the legacy semantic shell |
| LG-9532f2cf | renderer_call | src/boomi_mcp/categories/integration_builder.py | _execute_component | 1 | 3561 | #153 | replace with canonical ProcessIR materialization/apply |
| LG-e5f61bc6 | renderer_call | src/boomi_mcp/categories/integration_builder.py | _process_component_preflight | 2 | 5446 | #153 | replace with canonical ProcessIR materialization/apply |
| LG-7c23f2fd | renderer_call | src/boomi_mcp/categories/integration_builder.py | build_structured_update_xml | 1 | 3165 | #153 | replace with canonical ProcessIR materialization/apply |
| LG-8e9df2b4 | renderer_call | src/boomi_mcp/compiler/process_ir/legacy_adapters/authority.py | _core_from_authored_pipeline | 1 | 252 | #151 | re-home onto the neutral extraction |
| LG-f973cf28 | renderer_call | src/boomi_mcp/compiler/process_ir/legacy_adapters/authority.py | _core_from_submitted_config | 4 | 289 | #151 | re-home onto the neutral extraction |
| LG-819f78b4 | renderer_call | src/boomi_mcp/compiler/process_ir/legacy_adapters/sync_pipeline.py | adapt_sync_pipeline_config | 1 | 481 | #151 | re-home onto the neutral extraction |
| LG-bfc762c3 | renderer_call | src/boomi_mcp/patterns/archetypes/database_to_api_sync.py | _build_main_process | 1 | 2955 | #159 | migrate the archetype to canonical ProcessIR |
| LG-08a97ecd | renderer_call | src/boomi_mcp/patterns/composition.py | compose_archetypes | 1 | 1103 | #160 | delete with the legacy renderer |

## 11.3 Legacy `process_kind` producer census

`process_kind_producer` rows are **writes** — a dict-literal key, a subscript assignment, a keyword
argument, or an `IntegrationComponentSpec(type="process")` construction. `process_kind_consumer`
rows are reads, listed separately because a read is dispatch evidence, not production. Archetype and
example producers are globbed, never enumerated: a sixth example is a diff, not a silent pass.

| Ledger ID | Census | Path | Symbol | Sites | Baseline line | Owning issue | Disposition |
|---|---|---|---|---|---|---|---|
| LG-a4f2eecb | authoring_boundary | server.py | build_from_archetype | 1 | 2553 | #159 | migrate the boundary to canonical ProcessIR |
| LG-702def56 | authoring_boundary | server.py | build_integration | 1 | 2321 | #159 | migrate the boundary to canonical ProcessIR |
| LG-e2d1badd | authoring_boundary | server.py | compose_archetypes | 1 | 2581 | #159 | migrate the boundary to canonical ProcessIR |
| LG-4b265c33 | authoring_boundary | server.py | import_integration_draft | 1 | 2817 | #159 | migrate the boundary to canonical ProcessIR |
| LG-03af32a7 | authoring_boundary | src/boomi_mcp/patterns/archetypes/api_to_api_sync.py | api_to_api_sync | 1 | 1 | #159 | migrate the archetype to canonical ProcessIR |
| LG-3a04479d | authoring_boundary | src/boomi_mcp/patterns/archetypes/api_to_database_sync.py | api_to_database_sync | 1 | 1 | #159 | migrate the archetype to canonical ProcessIR |
| LG-55c06295 | authoring_boundary | src/boomi_mcp/patterns/archetypes/database_to_api_sync.py | database_to_api_sync | 1 | 1 | #159 | migrate the archetype to canonical ProcessIR |
| LG-d2c9b0c8 | authoring_boundary | src/boomi_mcp/patterns/archetypes/http_listener_to_db.py | http_listener_to_db | 1 | 1 | #159 | migrate the archetype to canonical ProcessIR |
| LG-3aa639d0 | authoring_boundary | src/boomi_mcp/patterns/archetypes/http_listener_to_rest.py | http_listener_to_rest | 1 | 1 | #159 | migrate the archetype to canonical ProcessIR |
| LG-8cbd0b5b | example_producer | examples/m11/cache_property_authoring_basic.integration.json | integration_spec.components[0].config.process_kind | 1 | 16 | #159 | migrate the example to canonical ProcessIR |
| LG-04ef53a3 | example_producer | examples/m11/cache_property_authoring_join.integration.json | integration_spec.components[2].config.process_kind | 1 | 76 | #159 | migrate the example to canonical ProcessIR |
| LG-3740584a | example_producer | examples/m8/cache_handoff_staged_fanout.integration.json | integration_spec.components[10].config.process_kind | 1 | 402 | #159 | migrate the example to canonical ProcessIR |
| LG-637793a5 | example_producer | examples/m8/composed_db_to_api_fanout.integration.json | integration_spec.components[9].config.process_kind | 1 | 353 | #159 | migrate the example to canonical ProcessIR |
| LG-851b002a | process_kind_consumer | src/boomi_mcp/authoring/workflow.py | _normalize_intent | 1 | 413 | #160 | delete with the legacy consumer |
| LG-65a53d30 | process_kind_consumer | src/boomi_mcp/categories/components/builders/process_flow_builder.py | ProcessFlowBuilder.validate_config | 2 | 656 | #160 | delete the legacy semantic shell |
| LG-bba865c6 | process_kind_consumer | src/boomi_mcp/categories/components/builders/process_flow_builder.py | SyncPipelineBuilder.validate_config | 2 | 5976 | #160 | delete the legacy semantic shell |
| LG-42e9928c | process_kind_consumer | src/boomi_mcp/categories/components/builders/process_flow_builder.py | WrapperSubprocessBuilder.validate_config | 2 | 5006 | #160 | delete the legacy semantic shell |
| LG-18b251bf | process_kind_consumer | src/boomi_mcp/categories/integration_builder.py | _authored_process_validation_error | 2 | 5215 | #160 | delete with the legacy consumer |
| LG-0f6e121e | process_kind_consumer | src/boomi_mcp/categories/integration_builder.py | _bracketed_naming_warning | 2 | 4845 | #160 | delete with the legacy consumer |
| LG-adf3120a | process_kind_consumer | src/boomi_mcp/categories/integration_builder.py | _build_plan | 2 | 5803 | #160 | delete with the legacy consumer |
| LG-79a2008e | process_kind_consumer | src/boomi_mcp/categories/integration_builder.py | _execute_component | 2 | 3529 | #160 | delete with the legacy consumer |
| LG-7e8d39d2 | process_kind_consumer | src/boomi_mcp/categories/integration_builder.py | _process_models_error_handling | 2 | 4198 | #160 | delete with the legacy consumer |
| LG-f633058d | process_kind_consumer | src/boomi_mcp/categories/integration_builder.py | _resolve_preservation_policy | 2 | 1226 | #160 | delete with the legacy consumer |
| LG-331a98ac | process_kind_consumer | src/boomi_mcp/categories/integration_builder.py | _synthesize_wrapper_subprocess_edges | 2 | 550 | #160 | delete with the legacy consumer |
| LG-76727309 | process_kind_consumer | src/boomi_mcp/categories/integration_builder.py | _synthesize_wrapper_subprocess_extensions | 2 | 714 | #160 | delete with the legacy consumer |
| LG-0e50752e | process_kind_consumer | src/boomi_mcp/categories/integration_builder.py | build_structured_update_xml | 2 | 3135 | #160 | delete with the legacy consumer |
| LG-f82294a9 | process_kind_consumer | src/boomi_mcp/compiler/process_ir/legacy_adapters/authority.py | _resolve_process_kind | 2 | 217 | #151 | re-home onto the neutral extraction |
| LG-af904e61 | process_kind_consumer | src/boomi_mcp/models/_process_ir_compat.py | legacy_flow_sequence_to_ir | 2 | 678 | #159 | migrate the compatibility codec, then delete |
| LG-885d1a16 | process_kind_producer | src/boomi_mcp/categories/components/builders/process_flow_builder.py | SyncPipelineBuilder.lower_config | 1 | 5488 | #160 | delete the legacy semantic shell |
| LG-e846e889 | process_kind_producer | src/boomi_mcp/categories/meta_tools.py | <module> | 6 | 7692 | #160 | retract the served legacy guidance / guard the raw route |
| LG-275fe665 | process_kind_producer | src/boomi_mcp/compiler/process_ir/legacy_adapters/authority.py | _canonical_core | 1 | 230 | #151 | re-home onto the neutral extraction |
| LG-826a5241 | process_kind_producer | src/boomi_mcp/compiler/process_ir/legacy_adapters/authority.py | _core_from_authored_pipeline | 1 | 253 | #151 | re-home onto the neutral extraction |
| LG-036eebde | process_kind_producer | src/boomi_mcp/models/_process_ir_compat.py | ir_to_legacy_flow_sequence | 2 | 1067 | #159 | migrate the compatibility codec, then delete |
| LG-135ab897 | process_kind_producer | src/boomi_mcp/patterns/archetypes/api_to_api_sync.py | ApiToApiSyncArchetype.emit_spec | 1 | 1704 | #159 | migrate the archetype to canonical ProcessIR |
| LG-6bbdfefd | process_kind_producer | src/boomi_mcp/patterns/archetypes/api_to_api_sync.py | _build_main_process | 2 | 1158 | #159 | migrate the archetype to canonical ProcessIR |
| LG-dc90b5ae | process_kind_producer | src/boomi_mcp/patterns/archetypes/api_to_database_sync.py | ApiToDatabaseSyncArchetype.emit_spec | 1 | 879 | #159 | migrate the archetype to canonical ProcessIR |
| LG-76cde1a2 | process_kind_producer | src/boomi_mcp/patterns/archetypes/api_to_database_sync.py | _build_main_process | 2 | 521 | #159 | migrate the archetype to canonical ProcessIR |
| LG-0bf8b950 | process_kind_producer | src/boomi_mcp/patterns/archetypes/database_to_api_sync.py | _build_main_process | 1 | 3065 | #159 | migrate the archetype to canonical ProcessIR |
| LG-22b7f2cc | process_kind_producer | src/boomi_mcp/patterns/archetypes/database_to_api_sync.py | _build_sync_pipeline_adapter_config | 1 | 2894 | #159 | migrate the archetype to canonical ProcessIR |
| LG-78a58f1a | process_kind_producer | src/boomi_mcp/patterns/archetypes/http_listener_to_db.py | HttpListenerToDbArchetype.emit_spec | 1 | 1104 | #159 | migrate the archetype to canonical ProcessIR |
| LG-2749063b | process_kind_producer | src/boomi_mcp/patterns/archetypes/http_listener_to_db.py | _build_listener_main_process | 2 | 746 | #159 | migrate the archetype to canonical ProcessIR |
| LG-1e119c4f | process_kind_producer | src/boomi_mcp/patterns/archetypes/http_listener_to_rest.py | HttpListenerToRestArchetype.emit_spec | 1 | 537 | #159 | migrate the archetype to canonical ProcessIR |

## 11.4 Caller-reachable Component-XML write routes

The **sinks** are derived by the scanner; this table only says what each one MEANS and who owns it.
`reconcile_routes()` proves the join is total in both directions — no derived sink location is
unclassified (`unclassified == []`) and no route cites a call site the scanner cannot find
(`stale_claims == []`) — so #160 cannot inherit a route this ledger forgot, nor a citation that has
moved.

Five locations legitimately host several routes (a single function containing both the raw-XML arm
and the typed builders). That is recorded rather than rejected, and the exact set is frozen, so a
NEW sharing is a diff instead of a silent reclassification:

- `src/boomi_mcp/categories/components/connectors.py::create_connector -> WRT-connector-typed-build, WRT-manage-connector-create`
- `src/boomi_mcp/categories/components/connectors.py::update_connector -> WRT-manage-connector-metadata, WRT-manage-connector-update`
- `src/boomi_mcp/categories/components/manage_component.py::create_component -> WRT-manage-component-create, WRT-manage-component-typed-create`
- `src/boomi_mcp/categories/components/manage_component.py::update_component -> WRT-manage-component-metadata, WRT-manage-component-update`
- `src/boomi_mcp/categories/integration_builder.py::_execute_component -> WRT-build-integration-generic, WRT-build-integration-structured-process, WRT-build-integration-typed-nonprocess`

| Route ID | Derived sink location(s) | Classification | Summary | Owning issue | Required post-retraction assertion |
|---|---|---|---|---|---|
| WRT-manage-component-dispatch | src/boomi_mcp/categories/components/manage_component.py::manage_component_action | raw_process_capable | Caller-facing dispatcher: forwards `config.xml` verbatim to the create and update arms with no component-type restriction. | #160 | the shared process-content classifier runs at the dispatcher, before either arm. |
| WRT-manage-component-create | src/boomi_mcp/categories/components/manage_component.py::create_component | raw_process_capable | Caller `config.xml` is posted verbatim, so `<Component type="process">` mints a process. The same function also hosts the typed non-process builders (shared location). | #160 | create with a process XML root is REJECTED by the shared process-content classifier, whatever the declared type. |
| WRT-manage-component-typed-create | src/boomi_mcp/categories/components/manage_component.py::create_component | typed_non_process | The typed builder arms of the same function emit connector/profile/operation XML, never a process root. | #160 | unchanged — the content guard never matches these roots. |
| WRT-manage-component-update | src/boomi_mcp/categories/components/manage_component.py::update_component | raw_process_capable | Caller `config.xml` full-replaces the named component (shared location with the metadata smart-merge). | #160 | two-sided check — payload root `process` OR live target type `process` refuses; lookup/parse failure fails closed. |
| WRT-manage-component-metadata | src/boomi_mcp/categories/components/manage_component.py::update_component | preserve | Metadata smart-merge: reads live XML, rewrites name/folder/description, resubmits the full document. | #160 | semantic-body identity under the single shared projection. |
| WRT-manage-component-clone | src/boomi_mcp/categories/components/manage_component.py::clone_component | platform_sourced_rematerialization | Re-posts platform-sourced XML with the identity attributes stripped, creating a NEW component with a NEW server-assigned id — a second unattested materialization, not metadata drift. | #160 | process-typed clone REJECTED; non-process clone preserved. |
| WRT-manage-connector-create | src/boomi_mcp/categories/components/connectors.py::create_connector | raw_process_capable | Raw-XML create posts caller XML with NO type check, so `<Component type="process">` mints a process (shared location with the typed connector builders). | #160 | content-based refusal on a process XML root. |
| WRT-connector-typed-build | src/boomi_mcp/categories/components/connectors.py::create_connector | typed_non_process | Typed connector / connector-operation builders emit connector-family XML only. | #160 | unchanged — the content guard never matches a connector root. |
| WRT-manage-connector-update | src/boomi_mcp/categories/components/connectors.py::update_connector | raw_process_capable | Raw-XML update full-replaces ANY `component_id`, including a process (shared location with the metadata smart-merge). | #160 | two-sided payload-OR-live-target refusal. |
| WRT-manage-connector-metadata | src/boomi_mcp/categories/components/connectors.py::update_connector | preserve | Metadata smart-merge over the live connector XML. | #160 | semantic-body identity under the shared projection. |
| WRT-build-integration-generic | src/boomi_mcp/categories/integration_builder.py::_execute_component | raw_process_capable | Generic create/update fall-through forwards `config.xml` for ANY unhandled declared type — `IntegrationComponentSpec.type` is unrestricted, so the type="process" plan gates (which key off `comp.type == "process"`) are skipped entirely. | #160 | content guard at BOTH plan and apply boundaries, placed BEFORE all type dispatch (the process and connector arms dispatch before the fall-through ever runs); mutation-tested with an unknown/future declared type. |
| WRT-build-integration-structured-process | src/boomi_mcp/categories/integration_builder.py::_execute_component | legacy_structured_process | The structured `comp.type == "process"` arm resolves a legacy builder and renders `<process>`. It does NOT forward caller raw XML — the issue text's claim that this branch dispatches raw XML is incorrect (see the §11.2 note); the guard still belongs before all type dispatch because the connector and generic arms are raw-process-capable. | #153 | replaced by canonical ProcessIR materialization/apply. |
| WRT-build-integration-typed-nonprocess | src/boomi_mcp/categories/integration_builder.py::_execute_component | typed_non_process | Typed connector/profile/map/operation arms of the same executor. | #160 | unchanged — the content guard never matches these roots. |
| WRT-build-integration-preservation-merge | src/boomi_mcp/categories/integration_builder.py::_apply_structured_update | preserve | Read-merge-write update preservation over the live document; the process BODY it merges is produced by `build_structured_update_xml`, which rides the legacy builder. | #153 | preserved, re-homed onto the canonical apply path. |
| WRT-safe-edit-metadata | src/boomi_mcp/categories/components/safe_edit_component.py::apply_component_edit_action | preserve | The FOURTH administrative writer: metadata edits reserialize and re-submit the FULL live process XML as a full-replace update (the #45/#50 merge). Its process BODY edits ride the legacy builder via `build_structured_update_xml`. | #160 | route-sensitive projection — the permitted subset is exactly the requested name/folderId/folderName/immediate description; process BODY edits are REJECTED in favour of canonical ProcessIR apply. |
| WRT-analyze-component-merge | src/boomi_mcp/categories/components/analyze_component.py::merge_versions | platform_sourced_rematerialization | Version/branch merge writes the SOURCE version's body into the target — a semantic change to the target by design, with no caller XML. | #160 | REJECT when the target is a process OR the source/merged root is a process — the merged root IS the source root, so a target-only check never sees a process-root source over a non-process target. |
| WRT-folders-move-component | src/boomi_mcp/categories/folders.py::_action_move_component | preserve | folderId-only rewrite of the live XML, then verify. | #160 | semantic-body identity under the shared projection. |
| WRT-shared-raw-create-sink | src/boomi_mcp/categories/components/_shared.py::_create_component_raw | raw_process_capable | The single shared create sink every raw and typed create funnels through; posts the XML byte-for-byte via the SDK. | #160 | the shared process-content classifier is enforced at or before this sink for every caller. |
| WRT-shared-dormant-writer | src/boomi_mcp/categories/components/_shared.py::_update_component_xml | dormant | A full-raw-XML update through the SDK with ZERO production callers at HEAD — inventoried precisely BECAUSE it is dormant, so a future caller cannot revive an unguarded raw write route. Its only callers are the transport tests (tests/test_component_raw_transport.py:59,71). | #160 | sits behind the two-sided guard, or is deleted. |
| WRT-shared-channel-lossless-read | src/boomi_mcp/categories/shared_resources.py::_get_channel_raw_json | typed_non_process | A deliberate SDK bypass (`Serializer` + `send_request`) that reads a SharedCommunicationChannelComponent as raw JSON, because the typed GET hydrates into a model whose round-trip DROPS nested protocol config. It targets the SharedCommunicationChannelComponent endpoint, never /Component, and it is a READ feeding the update merge. | #160 | unchanged — the endpoint is not /Component and the call is a read, so the process-content classifier never applies; it is inventoried so a future edit cannot turn a hand-rolled transport into a write route unnoticed. |
| WRT-raw-api-component | server.py::invoke_boomi_api<br>src/boomi_mcp/categories/meta_tools.py::invoke_api | raw_process_capable | The generic raw invoker reaches POST/PUT `/Component` unrestricted by type; gated only by `confirm_write=true`. Classification splits its own copy of the endpoint while transport interpolates the raw string. | #160 | ONE canonical endpoint parser feeds classification, ID extraction AND transport; the reserved literal `bulk` is matched BEFORE the `<id>` arm and is never a componentId; every update-shaped call runs the two-sided process check. |

Every derived write-sink CALL SITE, at ledger grain — the rows #160 checks off. The route table
above says what each location means; this one says exactly where the calls are.

| Ledger ID | Census | Path | Symbol | Sites | Baseline line | Owning issue | Disposition |
|---|---|---|---|---|---|---|---|
| LG-4e092c94 | component_xml_write | src/boomi_mcp/categories/components/_shared.py | _create_component_raw | 1 | 387 | #160 | guard behind the shared process-content classifier |
| LG-c7a85e57 | component_xml_write | src/boomi_mcp/categories/components/_shared.py | _update_component_xml | 1 | 419 | #160 | guard behind the shared process-content classifier |
| LG-08665fc8 | component_xml_write | src/boomi_mcp/categories/components/analyze_component.py | merge_versions | 1 | 658 | #160 | guard behind the shared process-content classifier |
| LG-b091f264 | component_xml_write | src/boomi_mcp/categories/components/connectors.py | create_connector | 3 | 382 | #160 | guard behind the shared process-content classifier |
| LG-91d77b6a | component_xml_write | src/boomi_mcp/categories/components/connectors.py | update_connector | 2 | 560 | #160 | guard behind the shared process-content classifier |
| LG-eb7e27c9 | component_xml_write | src/boomi_mcp/categories/components/manage_component.py | clone_component | 1 | 376 | #160 | guard behind the shared process-content classifier |
| LG-87f4dbe2 | component_xml_write | src/boomi_mcp/categories/components/manage_component.py | create_component | 6 | 66 | #160 | guard behind the shared process-content classifier |
| LG-f1dd2559 | component_xml_write | src/boomi_mcp/categories/components/manage_component.py | manage_component_action | 2 | 459 | #160 | guard behind the shared process-content classifier |
| LG-56fa02b3 | component_xml_write | src/boomi_mcp/categories/components/manage_component.py | update_component | 3 | 232 | #160 | guard behind the shared process-content classifier |
| LG-2a29dc0c | component_xml_write | src/boomi_mcp/categories/components/safe_edit_component.py | apply_component_edit_action | 1 | 553 | #160 | retract the served raw-XML steer; body edits move to canonical apply |
| LG-8f886cf4 | component_xml_write | src/boomi_mcp/categories/folders.py | _action_move_component | 1 | 324 | #160 | guard behind the shared process-content classifier |
| LG-5f228a61 | component_xml_write | src/boomi_mcp/categories/integration_builder.py | _apply_structured_update | 1 | 3063 | #160 | guard behind the shared process-content classifier |
| LG-c45135b8 | component_xml_write | src/boomi_mcp/categories/integration_builder.py | _execute_component | 10 | 3575 | #160 | guard behind the shared process-content classifier |
| LG-1d129c81 | component_xml_write | src/boomi_mcp/categories/meta_tools.py | invoke_api | 1 | 5864 | #160 | retract the served legacy guidance / guard the raw route |
| LG-0c9343f4 | component_xml_write | src/boomi_mcp/categories/shared_resources.py | _get_channel_raw_json | 1 | 204 | #160 | guard behind the shared process-content classifier |
| LG-c3bf6d69 | raw_api_invoker | server.py | invoke_boomi_api | 1 | 3411 | #160 | guard behind the canonical endpoint parser |

**SDK evidence** (derived by reading the installed SDK's own method bodies — semantic facts only, no
vendor line numbers, which drift independently under `boomi>=3.0.1`):

| Fact | Derived value |
|---|---|
| `create_component` | `POST` `/Component`, `Accept: application/xml` |
| `update_component` | `POST` `/Component/{componentId}`, `Accept: application/xml` — **POST to the id, there is no PUT** |
| `bulk_component` | `POST` `/Component/bulk`, `Accept: application/xml` — the bulk **READ** route, XML forced |
| `ComponentBulkRequestType` members | `CREATE`, `DELETE`, `GET`, `UPDATE` — envelope-parses is **not** read-only |
| `ComponentBulkRequest` optional init params | `request`, `type_` — both fields optional |
| Component write verbs | `create_component`, `create_component_raw`, `update_component`, `update_component_raw` |

The installed version STRING is deliberately not frozen: `server.py:206-207` appends a sibling
`../boomi-python/src` checkout to `sys.path` when one exists, so
`importlib.metadata.version("boomi")` answers differently before and after `import server` on a
developer machine that has the checkout (measured: `3.0.1` then `2.1.0`). The imported module is
unaffected — every fact above is read from the installed distribution — but pinning the metadata
string would freeze a machine-local accident. The shape facts are the better upgrade signal anyway.

## 11.5 Allowlist and endgame ownership

Every ledger row carries an owning endgame issue and a disposition; **no cell reads "unknown"**
(`test_no_owner_or_disposition_cell_is_left_unfilled`). Ownership is assigned by path-scoped rules
over the census, so a row cannot be silently orphaned when a file moves.

| Owning issue | Disposition | Ledger rows |
|---|---|---|
| #151 | re-home onto the neutral extraction | 7 |
| #153 | replace with canonical ProcessIR materialization/apply | 7 |
| #159 | migrate the archetype to canonical ProcessIR | 18 |
| #159 | migrate the boundary to canonical ProcessIR | 4 |
| #159 | migrate the compatibility codec, then delete | 2 |
| #159 | migrate the example to canonical ProcessIR | 4 |
| #160 | delete or re-home with the callee | 31 |
| #160 | delete the legacy semantic shell | 22 |
| #160 | delete with the legacy consumer | 10 |
| #160 | delete with the legacy renderer | 3 |
| #160 | delete with the legacy semantic shell | 2 |
| #160 | guard behind the canonical endpoint parser | 1 |
| #160 | guard behind the shared process-content classifier | 13 |
| #160 | retract the served legacy guidance / guard the raw route | 2 |
| #160 | retract the served raw-XML steer; body edits move to canonical apply | 4 |

Owner semantics: **#151** neutral extraction and `flow_sequence` parity/reachability; **#153**
canonical ProcessIR component materialization/apply; **#154** grammar/effect foundation; **#155**
connector dynamic-path/capability transfer; **#156** Notify/recovery transfer; **#157** authoring
envelope/governance; **#158** WSS listener entry/fallback replacement; **#159** archetype, recipe,
composition, example and internal-producer migration; **#160** final legacy deletion, Component-XML
guards/rejections, raw-API protection, and served-text retraction.

## 11.6 Served-surface retraction matrix

One row per served surface CLASS, each with its caller-visible producer, its HEAD source anchors,
the count of frozen artifacts pinning its text, and the assertion #160 must satisfy after
retraction. **#160 can execute the sweep from this matrix alone** —
`test_the_retraction_matrix_is_executable_from_the_matrix_alone` checks every anchor resolves at
HEAD and every row names a producer, the guidance it exposes, and a post-retraction assertion.

A deleted-name grep is NOT the acceptance mechanism: obsolete guidance survives under names that are
never deleted. That is why the freeze snapshots the **served values** — 94 artifacts collected
by calling the real read-only producers (`server.mcp.list_tools()`,
`meta_tools.get_schema_template_action`, `list_capabilities_action`,
`list_integration_archetypes_action`, `IntegrationComponentSpec.model_json_schema()`, and pure
error-envelope probes) and comparing them by value and by SHA-256.
`test_the_served_collection_cannot_touch_boomi_transport` runs the whole collection with every SDK
request/create/update method replaced by a raising spy, so "read-only" is measured, not asserted.

Large payloads (canonical length over 8192 characters) store their SHA-256 plus every string
that carries a legacy token, instead of a second verbatim copy of a schema already pinned elsewhere
in the suite. Drift detection is unaffected — the hash covers the complete canonical value.

| Surface ID | Surface class | Caller-visible producer(s) | HEAD source anchor(s) | Frozen artifact IDs | Legacy guidance exposed | Owning endgame step | Required post-retraction assertion |
|---|---|---|---|---|---|---|---|
| SS-PYDANTIC | Pydantic schema / field descriptions | IntegrationComponentSpec.model_json_schema() | src/boomi_mcp/models/integration_models.py:20-34 (description string :22-33) | SS-PYDANTIC:properties.config.description | steers callers to "manage_component for an explicit raw process XML escape hatch" and advertises process_kind | #160 | the served config description names no raw process XML escape hatch and no legacy process_kind. |
| SS-BUILDER-DIAGNOSTICS | Builder error texts and hints | integration_builder plan preflight<br>processes.manage_process_action(action='create') | src/boomi_mcp/categories/integration_builder.py:3148<br>src/boomi_mcp/categories/integration_builder.py:3162<br>src/boomi_mcp/categories/integration_builder.py:3544<br>src/boomi_mcp/categories/integration_builder.py:3610<br>src/boomi_mcp/categories/integration_builder.py:5318<br>src/boomi_mcp/categories/integration_builder.py:5403-5430 | SS-BUILDER-DIAGNOSTICS:ACTION_UNSUPPORTED<br>SS-BUILDER-DIAGNOSTICS:PROCESS_KIND_REQUIRED<br>SS-BUILDER-DIAGNOSTICS:PROCESS_KIND_UNSUPPORTED<br>SS-BUILDER-DIAGNOSTICS:PROCESS_KIND_XML_CONFLICT | PROCESS_KIND_* hints enumerate sorted(PROCESS_FLOW_BUILDERS) and PROCESS_KIND_XML_CONFLICT actively steers raw process XML onto manage_component(type="component", config.xml) | #160 | no served builder envelope names a legacy process_kind value or routes raw process XML to another tool. |
| SS-MCP-DESCRIPTIONS | Registered MCP tool descriptions and parameter schemas | server.mcp.list_tools() | server.py:1383-1385 (manage_process)<br>server.py:1712 (manage_component tool)<br>server.py:1731-1732 (manage_component escape-hatch blessing)<br>server.py:2098 (manage_connector raw create)<br>server.py:2103 (manage_connector raw update) | SS-MCP-DESCRIPTIONS:get_schema_template.description<br>SS-MCP-DESCRIPTIONS:get_schema_template.parameters<br>SS-MCP-DESCRIPTIONS:index_profile_component.description<br>SS-MCP-DESCRIPTIONS:index_profile_component.parameters<br>SS-MCP-DESCRIPTIONS:infer_profile_fields.description<br>SS-MCP-DESCRIPTIONS:infer_profile_fields.parameters<br>SS-MCP-DESCRIPTIONS:invoke_boomi_api.description<br>SS-MCP-DESCRIPTIONS:invoke_boomi_api.parameters<br>SS-MCP-DESCRIPTIONS:manage_component.description<br>SS-MCP-DESCRIPTIONS:manage_component.parameters<br>SS-MCP-DESCRIPTIONS:manage_connector.description<br>SS-MCP-DESCRIPTIONS:manage_connector.parameters<br>SS-MCP-DESCRIPTIONS:manage_process.description<br>SS-MCP-DESCRIPTIONS:manage_process.parameters<br>SS-MCP-DESCRIPTIONS:prepare_component_edit.description<br>SS-MCP-DESCRIPTIONS:prepare_component_edit.parameters<br>SS-MCP-DESCRIPTIONS:review_transformation.description<br>SS-MCP-DESCRIPTIONS:review_transformation.parameters | tool descriptions bless raw process XML as an escape hatch and advertise raw-XML connector create/update | #160 | no registered tool description blesses raw process XML. |
| SS-SAFE-EDIT | Safe-edit guidance | safe_edit_component._validate_patch_shape(...) | src/boomi_mcp/categories/components/safe_edit_component.py:176-191 | SS-SAFE-EDIT:COMPONENT_EDIT_RAW_XML_UNSUPPORTED | the raw-XML refusal steers callers to manage_component(action='update') with config.xml | #160 | the refusal names no full-replacement escape hatch. |
| SS-SCHEMA-TEMPLATES | get_schema_template templates | meta_tools.get_schema_template_action(...)<br>server.get_schema_template.__doc__ | src/boomi_mcp/categories/meta_tools.py:595-598 (raw_xml_escape_hatch)<br>src/boomi_mcp/categories/meta_tools.py:762-785 (_COMPONENT_CREATE, type="process" at :770, workflow step 4 at :778-784)<br>src/boomi_mcp/categories/meta_tools.py:4989 (force-clear hint)<br>src/boomi_mcp/categories/meta_tools.py:5180-5190 (_COMPONENT_CLONE)<br>src/boomi_mcp/categories/meta_tools.py:8867 (serves _COMPONENT_CREATE)<br>src/boomi_mcp/categories/meta_tools.py:8880 (serves _COMPONENT_CLONE) | SS-SCHEMA-TEMPLATES:resource_type=component\|operation=clone<br>SS-SCHEMA-TEMPLATES:resource_type=component\|operation=create<br>SS-SCHEMA-TEMPLATES:resource_type=component\|operation=create\|component_type=certificate<br>SS-SCHEMA-TEMPLATES:resource_type=component\|operation=create\|component_type=certificate.pgp<br>SS-SCHEMA-TEMPLATES:resource_type=component\|operation=create\|component_type=connector-action<br>SS-SCHEMA-TEMPLATES:resource_type=component\|operation=create\|component_type=connector-settings<br>SS-SCHEMA-TEMPLATES:resource_type=component\|operation=create\|component_type=crossref<br>SS-SCHEMA-TEMPLATES:resource_type=component\|operation=create\|component_type=edistandard<br>SS-SCHEMA-TEMPLATES:resource_type=component\|operation=create\|component_type=flowservice<br>SS-SCHEMA-TEMPLATES:resource_type=component\|operation=create\|component_type=process<br>SS-SCHEMA-TEMPLATES:resource_type=component\|operation=create\|component_type=processproperty<br>SS-SCHEMA-TEMPLATES:resource_type=component\|operation=create\|component_type=processroute<br>SS-SCHEMA-TEMPLATES:resource_type=component\|operation=create\|component_type=profile.db<br>SS-SCHEMA-TEMPLATES:resource_type=component\|operation=create\|component_type=profile.edi<br>SS-SCHEMA-TEMPLATES:resource_type=component\|operation=create\|component_type=profile.flatfile<br>SS-SCHEMA-TEMPLATES:resource_type=component\|operation=create\|component_type=profile.json<br>SS-SCHEMA-TEMPLATES:resource_type=component\|operation=create\|component_type=profile.xml<br>SS-SCHEMA-TEMPLATES:resource_type=component\|operation=create\|component_type=queue<br>SS-SCHEMA-TEMPLATES:resource_type=component\|operation=create\|component_type=script.mapping<br>SS-SCHEMA-TEMPLATES:resource_type=component\|operation=create\|component_type=script.processing<br>SS-SCHEMA-TEMPLATES:resource_type=component\|operation=create\|component_type=tpcommoptions<br>SS-SCHEMA-TEMPLATES:resource_type=component\|operation=create\|component_type=tpgroup<br>SS-SCHEMA-TEMPLATES:resource_type=component\|operation=create\|component_type=tporganization<br>SS-SCHEMA-TEMPLATES:resource_type=component\|operation=create\|component_type=tradingpartner<br>SS-SCHEMA-TEMPLATES:resource_type=component\|operation=create\|component_type=transform.function<br>SS-SCHEMA-TEMPLATES:resource_type=component\|operation=create\|component_type=transform.map<br>SS-SCHEMA-TEMPLATES:resource_type=component\|operation=create\|component_type=webservice<br>SS-SCHEMA-TEMPLATES:resource_type=component\|operation=create\|component_type=webservice.external<br>SS-SCHEMA-TEMPLATES:resource_type=component\|operation=create\|component_type=xslt<br>SS-SCHEMA-TEMPLATES:resource_type=integration\|operation=plan<br>SS-SCHEMA-TEMPLATES:resource_type=process<br>SS-SCHEMA-TEMPLATES:resource_type=process\|operation=create<br>SS-SCHEMA-TEMPLATES:schema_name=AuthoringCompileResultV1<br>SS-SCHEMA-TEMPLATES:schema_name=AuthoringPlanResultV1<br>SS-SCHEMA-TEMPLATES:schema_name=AuthoringRequestV1<br>SS-SCHEMA-TEMPLATES:schema_name=IntegrationSpecV1<br>SS-SCHEMA-TEMPLATES:schema_name=archetype:api_to_api_sync<br>SS-SCHEMA-TEMPLATES:schema_name=archetype:api_to_database_sync<br>SS-SCHEMA-TEMPLATES:schema_name=archetype:database_to_api_sync<br>SS-SCHEMA-TEMPLATES:schema_name=archetype:http_listener_to_db<br>SS-SCHEMA-TEMPLATES:schema_name=archetype:http_listener_to_rest<br>SS-SCHEMA-TEMPLATES:schema_name=design_doctrine<br>SS-SCHEMA-TEMPLATES:schema_name=design_pattern:component_profile_reuse<br>SS-SCHEMA-TEMPLATES:schema_name=design_pattern:cross_cutting_framework_services<br>SS-SCHEMA-TEMPLATES:schema_name=design_pattern:inline_vs_branch_cache_invocation<br>SS-SCHEMA-TEMPLATES:schema_name=design_pattern:microservice_vs_monolith_decomposition<br>SS-SCHEMA-TEMPLATES:schema_name=design_pattern:migration_pattern_templating<br>SS-SCHEMA-TEMPLATES:schema_name=design_pattern:native_over_custom_scripting<br>SS-SCHEMA-TEMPLATES:schema_name=design_pattern:process_route_fanout<br>SS-SCHEMA-TEMPLATES:schema_name=design_pattern:unit_testing_via_swappable_data_source<br>SS-SCHEMA-TEMPLATES:schema_name=design_pattern:wrapper_subprocess_separation<br>SS-SCHEMA-TEMPLATES:schema_name=recipe_contributions<br>SS-SCHEMA-TEMPLATES:schema_name=script_dataprocess<br>SS-SCHEMA-TEMPLATES:schema_name=workflow:compose_multi_target_integration<br>SS-SCHEMA-TEMPLATES:schema_name=workflow:create_and_deploy_process<br>SS-SCHEMA-TEMPLATES:schema_name=workflow_sequences<br>SS-SCHEMA-TEMPLATES:wrapper_docstring | served templates carry type="process" raw XML, the raw_xml_escape_hatch text, and legacy process protocols | #160 | no served template emits a process-typed raw XML skeleton or a raw-XML escape-hatch instruction. |
| SS-RAW-API | Raw-API catalog and typed-alternative entries | meta_tools._classify_raw_api_request('POST', '/Component/abc')<br>meta_tools._raw_write_confirmation_guard(...)<br>meta_tools._typed_alternatives_for_endpoint('/Component/abc') | src/boomi_mcp/categories/meta_tools.py:5622 (_classify_raw_api_request)<br>src/boomi_mcp/categories/meta_tools.py:5728-5760 (_raw_write_confirmation_guard)<br>src/boomi_mcp/categories/meta_tools.py:5763 (invoke_api)<br>src/boomi_mcp/categories/meta_tools.py:5811 (transport interpolation) | SS-RAW-API:RAW_WRITE_CONFIRMATION_REQUIRED<br>SS-RAW-API:component_typed_alternatives<br>SS-RAW-API:component_update_classification | the raw invoker is type-unrestricted, so POST/PUT to /Component can mint or replace a process; classification splits its own copy of the endpoint while transport interpolates the raw string | #160 | ONE canonical endpoint parser feeds classification, ID extraction AND transport; every update-shaped call runs the two-sided process check; `bulk` is matched before the <id> arm and never treated as a componentId. |
| SS-CAPABILITY-CATALOG | list_capabilities catalog entries, workflows and hints | meta_tools.list_capabilities_action() | src/boomi_mcp/categories/meta_tools.py:10145 (list_capabilities_action) | SS-CAPABILITY-CATALOG:authoring_contract<br>SS-CAPABILITY-CATALOG:design_doctrine<br>SS-CAPABILITY-CATALOG:operating_doctrine<br>SS-CAPABILITY-CATALOG:tools<br>SS-CAPABILITY-CATALOG:workflows | catalog entries, workflow steps and hints reference legacy process protocols and the raw-XML route | #160 | no catalog entry, workflow step or hint references a legacy process_kind or the raw process XML route. |
| SS-ARCHETYPE-CATALOG | Served archetype descriptors and parameter schemas | integration_authoring.list_integration_archetypes_action() | src/boomi_mcp/authoring/contract.py:335 (list_archetype_registry)<br>src/boomi_mcp/categories/integration_authoring.py:205 (list_integration_archetypes_action) | SS-ARCHETYPE-CATALOG:api_to_api_sync<br>SS-ARCHETYPE-CATALOG:api_to_database_sync<br>SS-ARCHETYPE-CATALOG:database_to_api_sync<br>SS-ARCHETYPE-CATALOG:http_listener_to_db<br>SS-ARCHETYPE-CATALOG:http_listener_to_rest | every registered archetype whose emitted spec carries a legacy process_kind advertises it through the served descriptor | #159 | no served archetype descriptor emits or documents a legacy process_kind. |

## 11.7 Freeze evidence and re-baselining

| Property | Regression test |
|---|---|
| Baseline identity, unique IDs, closed vocabularies | `tests/test_issue_149_legacy_reachability_freeze.py::test_baseline_identity_and_schema_are_frozen` |
| No owner/disposition cell left unfilled | `tests/test_issue_149_legacy_reachability_freeze.py::test_no_owner_or_disposition_cell_is_left_unfilled` |
| Scan universe complete and non-vacuous | `tests/test_issue_149_legacy_reachability_freeze.py::test_the_scan_universe_is_complete_and_non_vacuous` |
| Vocabulary derived from the live runtime | `tests/test_issue_149_legacy_reachability_freeze.py::test_the_vocabulary_is_derived_from_the_live_runtime` |
| Census matches the frozen baseline | `tests/test_issue_149_legacy_reachability_freeze.py::test_legacy_callers_and_process_kind_producers_match_the_baseline` |
| Derivation is deterministic | `tests/test_issue_149_legacy_reachability_freeze.py::test_the_derivation_is_deterministic` |
| **Mutation:** a synthetic legacy caller breaks the freeze | `tests/test_issue_149_legacy_reachability_freeze.py::test_a_synthetic_legacy_caller_breaks_the_freeze` |
| **Mutation:** a renderer off a registry subscript is reported | `tests/test_issue_149_legacy_reachability_freeze.py::test_a_renderer_invoked_straight_off_a_registry_subscript_is_reported` |
| **Mutation:** a new emitter/write-sink caller breaks the freeze | `tests/test_issue_149_legacy_reachability_freeze.py::test_a_new_legacy_emitter_or_write_sink_caller_breaks_the_freeze` |
| **Mutation:** a module-qualified registry lookup is reported | `tests/test_issue_149_legacy_reachability_freeze.py::test_a_renderer_reached_through_a_module_qualified_registry_is_reported` |
| **Mutation:** a builder method reached via `getattr` is reported | `tests/test_issue_149_legacy_reachability_freeze.py::test_a_builder_method_reached_through_getattr_is_reported` |
| **Mutation:** a `bulk_component` caller is a write sink | `tests/test_issue_149_legacy_reachability_freeze.py::test_a_bulk_component_caller_is_reported_as_a_write_sink` |
| A tool that starts advertising a legacy path is collected | `tests/test_issue_149_legacy_reachability_freeze.py::test_a_new_tool_that_advertises_a_legacy_path_is_collected` |
| The comparator reads every frozen section | `tests/test_issue_149_legacy_reachability_freeze.py::test_the_comparator_reads_every_frozen_section` |
| **Mutation:** unresolvable dynamic access is reported, not ignored | `tests/test_issue_149_legacy_reachability_freeze.py::test_an_unresolvable_dynamic_sink_access_is_reported_not_ignored` |
| **Negative control:** an unrelated addition does not break it | `tests/test_issue_149_legacy_reachability_freeze.py::test_an_unrelated_addition_does_not_break_the_freeze` |
| A duplicated call site is reported as a count change | `tests/test_issue_149_legacy_reachability_freeze.py::test_a_second_call_in_an_existing_function_is_reported_as_a_count_change` |
| An inserted blank line does not break it | `tests/test_issue_149_legacy_reachability_freeze.py::test_an_inserted_blank_line_does_not_break_the_freeze` |
| Every write sink classified, no stale route claim | `tests/test_issue_149_legacy_reachability_freeze.py::test_every_component_xml_sink_is_classified_and_no_route_is_stale` |
| The dormant raw writer has no production caller | `tests/test_issue_149_legacy_reachability_freeze.py::test_the_dormant_shared_writer_has_no_production_callers` |
| Served artifacts match the committed values | `tests/test_issue_149_legacy_reachability_freeze.py::test_served_artifacts_match_the_committed_values` |
| The served collection reaches no transport | `tests/test_issue_149_legacy_reachability_freeze.py::test_the_served_collection_cannot_touch_boomi_transport` |
| **Guard the guard:** the transport sentinel really fires | `tests/test_issue_149_legacy_reachability_freeze.py::test_the_transport_sentinel_is_actually_armed` |
| SDK evidence is read from source, not patched attributes | `tests/test_issue_149_legacy_reachability_freeze.py::test_the_sdk_evidence_survives_the_transport_sentinel` |
| This table cites every test in the suite | `tests/test_issue_149_legacy_reachability_freeze.py::test_section_11_7_cites_every_test_in_this_module` |
| Every artifact belongs to exactly one matrix class | `tests/test_issue_149_legacy_reachability_freeze.py::test_every_served_artifact_belongs_to_exactly_one_matrix_class` |
| The matrix is executable from the matrix alone | `tests/test_issue_149_legacy_reachability_freeze.py::test_the_retraction_matrix_is_executable_from_the_matrix_alone` |
| Ledger and JSON are two-way complete | `tests/test_issue_149_legacy_reachability_freeze.py::test_the_ledger_and_the_json_are_two_way_complete` |
| Ledger sections partition every census kind | `tests/test_issue_149_legacy_reachability_freeze.py::test_the_ledger_sections_partition_every_census_kind` |
| The tables are regenerable from the JSON | `tests/test_issue_149_legacy_reachability_freeze.py::test_the_markdown_tables_are_regenerable_from_the_json` |
| **Mutation:** every module-qualified registry spelling is reported | `tests/test_issue_149_legacy_reachability_freeze.py::test_every_module_qualified_registry_spelling_is_reported` |
| **Precision:** a generic method name on an unrelated target is not reported | `tests/test_issue_149_legacy_reachability_freeze.py::test_a_generic_method_name_on_an_unrelated_target_is_not_reported` |
| Every served artifact stores its exact value | `tests/test_issue_149_legacy_reachability_freeze.py::test_every_served_artifact_stores_its_exact_value` |
| The templates #149 names by file:line are frozen | `tests/test_issue_149_legacy_reachability_freeze.py::test_the_templates_issue_149_names_are_frozen` |
| **Guard the guard:** the template walk descends its axes | `tests/test_issue_149_legacy_reachability_freeze.py::test_the_schema_template_walk_descends_its_axes` |
| The matrix carries artifact IDs, not counts | `tests/test_issue_149_legacy_reachability_freeze.py::test_the_retraction_matrix_carries_artifact_ids_not_counts` |
| Every ledger row carries a real file:line | `tests/test_issue_149_legacy_reachability_freeze.py::test_every_ledger_row_carries_a_real_line` |
| **Mutation:** a wrapper (and a wrapper of a wrapper) around a legacy path is reported | `tests/test_issue_149_legacy_reachability_freeze.py::test_a_wrapper_around_a_legacy_path_is_reported` |
| **Mutation:** a `setdefault` process_kind producer is reported | `tests/test_issue_149_legacy_reachability_freeze.py::test_a_setdefault_producer_is_reported` |
| **Mutation:** a hand-rolled transport POST is a write sink | `tests/test_issue_149_legacy_reachability_freeze.py::test_a_hand_rolled_transport_post_is_reported_as_a_write_sink` |
| **Mutation:** a legacy semantic-validation caller is reported | `tests/test_issue_149_legacy_reachability_freeze.py::test_a_legacy_semantic_validation_caller_is_reported` |

**Darkness.** This slice changes nothing under `src/boomi_mcp/` or `server.py`:
`git diff 9711a9c0cb6c88dda41ada94d88694915b659f36 -- src/boomi_mcp server.py` is empty. The only
deliverables are this section, the batched §7/§12 corrections in
`TYPED_RECIPE_CONTRIBUTIONS_V1.md`, the test-only helper, the fixture, and the freeze suite.

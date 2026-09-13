# Sibling inventory — cache-step document emission (#184 amendment 3, structural fix)

Defect class `cache-step-document-emission` (ledger rows E0-184-01, E0-184-03, E1-184-01).

**Mechanism.** A hand-written rule decides which steps hand documents to their successor.

**Runtime authority.** Platform execution. A step with no inbound documents is skipped ("No
documents found. Skipping execution for …") while the run reads COMPLETE. Add to Cache and
all-document Remove from Cache emit zero documents, and a retrieve runs only when a document
arrives. See `docs/architecture/evidence/issue-184/document_emission/CAPTURE_INDEX.json`.

**Method.** A read-only sweep of the working tree at `ae79c1e` (grep plus reading), before any
`src/` edit. It ran no tests. Every site that encodes, assumes or publishes cache continuation or
emission is listed below.

**Dispositions.**

| Disposition | Meaning |
| --- | --- |
| `authority` | the site is replaced by a query of `models/process_ir_document_semantics.py` |
| `fix` | corrected in place to the measured behaviour |
| `justify` | no change, with the one-line reason |
| `§7` / `§8` / `§10` | corrected by that amendment step, not by the emission batch |

Dispositions are the plan of record for the structural batch. They are updated to their final
values in the same commit that lands each correction.

## 1. Model grammar (`src/boomi_mcp/models/process_ir.py`)

| Site | Current assumption | Disposition |
| --- | --- | --- |
| module docstring `:40` | names an "Add-to-Cache consume guard" | fix |
| `CachePutNodeV1` docstring `:1497-1510` | a mid-list put must be followed by a read; a trailing put is tolerated before a false-arm stop or a catch stop/exception | fix |
| `CacheGetNodeV1` / `DocumentCacheRetrieveNodeV1` docstrings | silent on needing an arriving document | fix |
| `CacheRemoveNodeV1` docstring `:1562-1572` | documents pass on through a remove | fix |
| `_CACHE_READ_KINDS` `:1985` | the only legal followers of a put | authority |
| `_check_cache_put_followed_by_read` `:1988-1997` | requires a read straight after a mid-list put | authority (replaced by the terminal cache-action verdict) |
| `TRAILING_CACHE_PUT_TERMINALS` / `_check_trailing_cache_put` `:2000-2049` | a Stop or Exception may follow a trailing put in a false arm or catch | authority (withdrawn) |
| `PROCESS_CALL_ATTESTED_PREDECESSORS` `:2132-2172` | reads admitted before a call; remove refused | justify (already matches: a read is triggered by its leg's input; a remove's successor never runs); keys gain entry form and wait under §8 |
| `BranchLegV1` terminal union `:2531-2565` | no terminal cache_remove | fix (add `CacheRemoveNodeV1`, Branch leg only) |
| `BranchLegV1._leg_rules`, `DecisionTrueArmV1._arm_rules`, `DecisionFalseArmV1._arm_rules`, `TryCatchTryBodyV1`, `TryCatchCatchBodyV1._catch_body_rules` | put → read admitted; trailing put tolerated per slot | authority |
| `_check_serialized_region_chain` `:3239-3243` | re-runs followed-by-read on handler chains | authority |
| `_check_listener_root` connector and endpoint forms `:3360-3389` | followed-by-read plus trailing check | authority |
| `_check_passthrough_root` `:3424-3511` | a mid-run put is followed by a read | authority |
| `SequenceNodeV1` docstring `:3541` and connector-root branch `:3793-3808` | put must be followed by a read | authority |
| `ROOT_ENTRY_READ_KINDS` `:3120-3125`, D8 branch `:3811-3839` | the read is "the flow's first producer" | authority (the scheduled Start's empty document triggers it; membership derived) |
| legacy source/target branch `:3895-3897` | followed-by-read refuses a put feeding the target | authority |
| `PROCESS_IR_SEMANTIC_CATCH_UNTERMINATED` remediation `:4300-4305` | "end the catch body with … a staging cache_put" | justify (a terminal put in a catch is still admitted) |
| `_translate_pydantic_error` keyed-cache gate `:4507-4515` | keyed on `cache_get` | justify (keyed reads, not continuation) |

## 2. Body capabilities (`compiler/process_ir/body_capabilities.py`)

| Site | Current assumption | Disposition |
| --- | --- | --- |
| comment `:284-291` | a catch staging put hands documents to a downstream sink | fix |
| `_check_try_catch_placement` `:722-730`, `_check_passthrough_placement` `:848-868` | run the model's cache checks on the compiler side | authority (same shared verdict) |
| no compiler-side leg/arm/root cache re-check | relies on the #178 dump-and-reparse (`pipeline.py:345-362`) | justify (the reparse runs the model validators, so both entry points run the one verdict; pinned by parity tests) |

## 3. Read aliases

| Site | Current assumption | Disposition |
| --- | --- | --- |
| every alias pair listed in the sweep (model kind sets, connector producer set, lineage read and replacing sets, lowering `:1095-1113`, contracts `:775-789`, effects inspectable kinds) | both spellings handled identically | authority (one retrieve row, two authored aliases, one emitter kind; alias coverage pinned) |
| `load_all_documents` only on the retrieve; `external_writer` only on `cache_get`; `_process_ir_compat.py:510-538`; `cache_property_lineage.py:252/324/356`; `meta_tools.py:6438` | field-level differences | justify (configuration fields, not emission) |
| projection `_DOCS` `("optional", …)` for both reads `:531,553` | a read needs no arriving document | §10 (input becomes required) |

## 4. Connector walk (`compiler/process_ir/connector_resolution.py`)

| Site | Current assumption | Disposition |
| --- | --- | --- |
| `_STREAM_PRODUCING_KINDS` `:457-459` | a read supplies documents regardless of upstream state | authority |
| `_MAY_FOLLOW_NON_PRODUCER` `:461-465`, Send gate `:613-637` | a read may follow a non-producing Send | authority (refused `PROCESS_IR_SEMANTIC_CARDINALITY_MISMATCH` at the Send's `/operation_ref`; `cap184-send-then-read` OPEN) |
| read handling `:758-761` | a read sets a producer whatever arrived | authority (only when triggered) |
| put handling `:779-783` | relies on the model requiring a read next | authority |
| no `cache_remove` case | a remove passes the producer on | authority (a remove exhausts the path) |
| A4 map/put refusal `:667-682` | refuses consumers with no producer | justify (usable-payload requirement retained) |

## 5. Lineage and property summaries

| Site | Current assumption | Disposition |
| --- | --- | --- |
| `lineage.py` stream comments `:95-135` | no-producer states equal the walk's no-producer points | fix (trigger vs usable payload) |
| `_advance_stream` put `:1156-1183` | "the model requires a read next" | authority |
| `_advance_stream` reads `:1185-1194` | a read yields KNOWN/UNKNOWN even on an exhausted stream | authority (no trigger, no stream) |
| `_advance_stream` remove `:1196-1199` | returns the arriving stream unchanged | authority (exhausts the path) |
| `_ABNORMAL_EXIT_ROLES` / `cache_stage` normal exit `:653-705`, `:1467-1475` | a staging put completes normally | justify (true for put and remove: consuming the documents is normal completion); extended to the terminal remove |
| `PROPERTY_SURVIVAL_V1` read cells `:411-416`, `_drop_replaced_document_keys` `:443-493` | a read loses every DDP | §7 |
| `findings.py` cache-writer text `:64-65, 136-137` | "add a cache write ahead of this read on the same path" | §10 (a write ends its path; the writer belongs to an earlier leg) |
| `findings.py` DDP remediation `:139-146` | a cache retrieval loses the property | §10 (exact remediation sentence) |
| `effects.py` `_replay_hazard` `:106-110` | only `cache_put` is a cache mutation | fix (the remove joins cache mutation) |
| `semantic_validation/contracts.py:381-440` | no continuation claim | justify |

## 6. Child, effect and replay paths

| Site | Current assumption | Disposition |
| --- | --- | --- |
| `process_ir_effects.py` `derive_subprocess_effect` `:796-850` | any `cache_put` makes a child replay-unsafe; a remove does not | fix (the remove joins) |
| `process_ir_effects.py` `_occurrences` / external-writer binding `:143-156, 1044-1083` | keyed on `cache_get` | justify (external-writer declaration, not emission) |
| `INSPECTABLE_CHILD_KINDS` `:646-686` | all four cache kinds inspectable | justify |
| `cache_property_lineage.py` (legacy walker) | retrieve and remove exempt; DLQ writes not collected | justify (legacy read-before-write lint, no continuation rule); the "add an upstream cache_put" remediation is covered with §10 prose |

## 7. Lowering, contracts, invariants, emitter registry

| Site | Current assumption | Disposition |
| --- | --- | --- |
| `lowering.py` `_lower_terminal` `:545-553` | only a terminal put gets `cache_stage` | fix (a terminal remove gets the terminal cache-action role) |
| `lowering.py` root sequence `:775-783`, `_EXIT_KINDS` `:113-122` | a mid-list put or remove chains to a successor | authority (unreachable after the model verdict; the forged path is refused at emission) |
| `contracts.py` `CfgExitRoleV1` comment `:952-966` | staging ends with no terminal shape | fix (generalised to both actions) |
| `invariants.py` `_ALLOWED_EXIT_ROLES["cache_put"]` `(None, "cache_stage")` `:104-113` | a role-less put may have a successor | fix (put and remove require the terminal role) |
| `invariants.py` `_CACHE_STAGE_PATH` `:127-131`, message `:439-449` | stale "only valid in a branch leg terminal" | fix (put in leg/catch terminals; remove in leg terminals only) |
| `invariants.py` successor rule `:803-818` | a role-less put or remove needs one successor | authority |
| `emitter_registry.py` `doccacheload` `ZERO_OR_ONE` `:739` | a load may continue | authority (`EXACT_ZERO`) |
| `emitter_registry.py` `doccacheremove` `EXACT_ONE` `:741` | a remove needs a successor | authority (`EXACT_ZERO`) |
| `emitter_registry.py` `ZERO_OR_ONE` docstring `:141-156` | "terminal-or-continuing cache-load" | fix (removed if unused) |

## 8. Rendering and graph verification

| Site | Current assumption | Disposition |
| --- | --- | --- |
| `rendering.py` `_dragpoints_block` `:248-254`, `render_doccacheload` `:895-907` | a load renders terminal or forward | fix (reject transitions) |
| `rendering.py` `render_doccacheremove` `:603-620` | always renders a forward dragpoint | fix (terminal form only) |
| `rendering.py` `render_doccacheretrieve` `:581-600` | always forward | justify (a retrieve has one configured successor) |
| `legacy.py` `_emit_doccacheremove` `:344-374`, `_emit_doccacheload` `:730-746` | forward transitions | fix |
| `legacy.py` `_emit_doccacheretrieve` `:295-341` | forward | justify |
| `process_graph_verifier.py` `_TERMINAL_SHAPE_TYPES` / `_ALWAYS_TERMINAL_SHAPE_TYPES` `:36-66, 178-189` | a load can continue; a remove is not terminal | authority (both always terminal) |
| verifier Pass 2 `:404-420` | a terminal remove is a dead end | authority |
| verifier Pass 2a `:430-445` | never fires for load/remove successors | authority (`TERMINAL_SHAPE_HAS_OUTBOUND`) |
| verifier `CONTROL_BRANCH_BARE_STOP` remediation `:586-588` | "route through Document Cache before the Stop" | fix |

## 9. Legacy transform and sequence routes

| Site | Current assumption | Disposition |
| --- | --- | --- |
| `process_flow_builder.py` transform-mode sets `:160, 215`, comments `:245-288`, `:1042-1055` | `doccacheremove` is a linear transform between source and target | fix (refused `PROCESS_DOCCACHE_REMOVE_CONFIG_INVALID` at `transform.mode`) |
| `process_flow_builder.py` `:1024-1041` | `doccacheretrieve` between source and target | justify (triggered by the source's documents, one successor) |
| flow_sequence kinds `:313-360`, `_emit_flow_shape` `:3223-3249`, `validate_config` `:3591-3606`, `_validate_flow_sequence_steps` `:3670-3695` | a put may be followed by a read; a remove is linear | fix (any successor of a load or remove refused `PROCESS_FLOW_SEQUENCE_CONFIG_INVALID`; implicit target blames the cache step) |
| decision `true_steps` `:4073-4083`; false leg "trailing cache write is harmless" | the false leg's Stop after a put is harmless | fix |
| branch leg staging `:4142-4158` hint "follow the cache_put with cache_get" | a read may follow a put | fix (hint names an ordered later leg) |
| reliability validation `:2815-2850, 2936-2975` | `catch_exception` with `document_cache_ref` allowed | fix (refused) |
| `_emit_catch_leg` `:4326-4500`, `_emit_connector_scoped_try_catch_shapes` `:4502-4578` | the DLQ load routes to a synthetic Stop or an Exception | fix (terminal load; synthetic Stop omitted with shape ids preserved) |
| `_process_ir_compat.py` `_convert_branch_step` / `_split_arm_steps` / `_branch_to_legacy` `:600-671, 1064-1075` | a false-arm trailing put stays a step before `stop` | authority (the model refuses it); round-trip of a terminal remove added |
| `legacy_adapters/flow_sequence.py:328-330`, `sync_pipeline.py:75,352` | requirement mapping only | justify |
| archetype `database_to_api_sync.py:315-420`, `operational.py:383-510` `dlq_writer` | feed the builder catch leg | justify (the builder fix covers them; goldens 000060 and the archetype tests re-derive) |
| `integration_authoring.py:140-198`, `integration_builder.py` cache reference checks | hints and reference types | justify |

## 10. Cache primitive metadata (`src/boomi_mcp/patterns/primitives/`)

| Site | Current assumption | Disposition |
| --- | --- | --- |
| `document_cache_put.py:3-9, 61-84` | output `cached_document_stream`, "terminal-ish" | fix (terminal) |
| `document_cache_remove.py:99-116` | documents pass through and continue downstream | fix (terminal consumption; refused inline legacy composition) |
| `document_cache_retrieve.py:95-119` | re-emits for downstream | fix (needs an arriving document) |
| `document_cache_lookup.py:3-5, 55-85` | consumes what an earlier leg staged | justify (matches) |

## 11. Served contract and documentation text

| Site | Current assumption | Disposition |
| --- | --- | --- |
| `process_ir_projection.py` `node.cache_put` `:501-519`, `_derived_trailing_cache_put_fact` `:1210-1239`, `_node_entries` `:1323-1327` | put must be followed by a read; trailing tolerance sentence | authority now (the derived sentence is withdrawn with its table); exact prose §10 |
| `process_ir_projection.py` reads `:521-556`, `node.cache_remove` `:558-572` | optional input; a remove acts only on the cache | §10 |
| `docs/architecture/PROCESS_IR_V1.md:78, 116-119, 169, 264-274` | put followed by a read; false-arm put before stop | §10 |
| `docs/architecture/PROCESS_IR_COMPILER_V1.md:101, 195-196, 250, 317, 500-503, 650-656`, `PROCESS_EMITTER_REGISTRY_V1.md:73-75` | a read may follow a Send; load 0 or 1, remove 1 | §10 |
| `meta_tools.py:6518, 7025-7027, 7150-7165, 8190, 8289-8390, 10725` | retrieve and remove are linear non-terminal steps; the census sequence is purely linear | §10 |
| `kb/design_doctrine.py:162-176` | remove is a linear non-terminal step | §10 |
| `kb/operational_gotchas.py:1339-1367` | Add to Cache consumes documents | justify (matches) |
| `docs/companion/boomi-integration/references/steps/document_cache_steps.md:56,108` and its twin | remove is linear non-terminal | §10 |
| committed contract snapshot `tests/fixtures/authoring_contract/process_ir_authoring_v1.contract.json` | the sentences above | §10 (rebaseline through its producer) |

## 12. Tests pinning the old semantics

Every test named in the sweep is rewritten or retired in the batch that changes its subject.
Retired nodes keep their ids in `tests/fixtures/wave_gate/test_nodes.jsonl` by asserting the
refusal. The sweep's list, grouped:

* **put → read adjacency**:
  * `test_process_flow_builder.py` 4045–4245 group;
  * `test_process_ir_models.py:604/615/621`;
  * `test_process_ir_semantic_validation.py:521`;
  * `test_process_ir_entrypoint_diagnostic_parity.py` row 818;
  * `test_process_ir_authoring_contract_parity.py:1489/1507/1570`;
  * `test_issue_184_passthrough_entry.py:200-210`;
  * `test_issue_184_caller_entry_stream.py:115`;
  * `test_issue_184_lineage_controller.py:110`;
  * `_process_ir_capability_witnesses.py:1156-1166`.
* **read as a producer after a Send**:
  * `test_process_ir_rich_control_bodies.py:952/996`;
  * `test_issue_184_absent_stream.py:157/190`;
  * `test_issue_184_native_sequences.py:234`;
  * `test_process_ir_semantic_lineage.py:1013-1043`.
* **successor after a load or remove**:
  * `test_process_flow_builder.py:982/1005/1130/2208/3338/4156`;
  * `test_process_flow_builder_trycatch_dlq.py:444/476/571/694/812/856/970`;
  * `test_builder_xml_invariants.py:426`;
  * `test_process_graph_verifier.py:207/219/737/919`;
  * `test_issue_184_stream_profiles.py:191`;
  * `test_process_ir_notify_recovery.py:637/951/1106/1351/1370`;
  * `patterns/test_database_to_api_sync_dlq.py:349/355/462`;
  * `test_doctrine_emitter_consistency.py:321`.
* **trailing put and staging exits**:
  * `test_process_ir_models.py:621`;
  * `test_process_ir_error_handling.py:696/1003/1465`;
  * `test_issue_154_grammar_goldens.py:38`;
  * `test_process_ir_effect_declarations.py:2865/2912`;
  * `test_process_ir_compiler_invariants.py:1515/1537`.
* **"lost" cells** (§7):
  * `test_process_ir_semantic_lineage.py:1082/1525/1546/1596`;
  * `test_issue_184_ddp_invalidation.py`.

`test_m11_live_fixture_shapes.py:187` pins a platform-authored capture whose removes are
terminal. It matches the fact and is kept.

## 13. Other

| Site | Current assumption | Disposition |
| --- | --- | --- |
| `recipes/builtins/fanout.py`, `recipes/builtins/catalog.py`, `recipes/materialization.py:252`, `recipes/contracts.py:320`, `patterns/composition.py`, `patterns/recipe_bridge.py` | stage in a target-less leg and read in a later leg | justify (already the measured shape); re-verified by the recipe parity tests |
| `models/pipeline_models.py`, `models/cache_property_models.py`, `authoring/workflow.py:1515`, `models/authoring_workflow.py:554` | reserved kinds and external-writer text | justify |
| `compiler/process_ir/pipeline.py:353` reparse docstring | names a trailing put divergence as its example | fix |
| `document_cache_builder.py`, `map_builder.py:546`, `process_component_materializer.py:92`, `canonical_process_apply.py`, `deployment/orchestration.py` | component references only, or none | justify |
| active goldens with a cache-sink outbound edge | seven rows, ten edges (measured) | retired and replaced (ledger C15) |
| `tests/fixtures/process_ir/emitter_parity/linear_flow.process.xml`, `control_flow.process.xml` | one `doccache` shape each | to be measured in the batch |

**Coverage claim.** The claim covers the discovered finite vocabulary: the ProcessIR node kinds, the
emitter kinds, and the legacy transform and flow-sequence kinds. It covers the evidence cases
indexed in `CAPTURE_INDEX.json`, not every possible platform operation.

## Structural batch outcome (amendment 3 step 2)

Final dispositions, measured on the tree that carries the batch. Every `authority` row above now
queries `models/process_ir_document_semantics.py`, and every `fix` row is corrected in place.
Where the result differs from the planned row, it is named here.

| Area | Outcome |
| --- | --- |
| Model grammar | One verdict, `terminal_cache_action_verdict`, at every context in heading 1. It refuses a zero-emission step with an authored successor, with `PROCESS_IR_SCHEMA_INVALID_CARDINALITY` at that step's `/cache_ref`. The write-then-read rule and the trailing-cache-put table and check are withdrawn. The admitting slots named in the refusal are read off the terminal unions. `BranchLegV1.terminal` admits `CacheRemoveNodeV1`. `ROOT_ENTRY_READ_KINDS` is the authority's `TRIGGERED_REPLACEMENT_KINDS` |
| Connector walk | A read replaces the payload only when not exhausted. Zero-emission kinds clear the producer and exhaust the path. `_MAY_FOLLOW_NON_PRODUCER` is `{"stop"}`, so a Send before a read is refused at the Send's `/operation_ref` |
| Lineage | Put and remove give `absent`. A read on an absent stream invents nothing. The retrieve property transfer is the separate step-3 batch (§7) |
| Lowering, invariants, registry | A zero-emission terminal carries `cache_stage`. The role is required for put and remove, with positions per kind: remove in branch legs only. Load and remove are `EXACT_ZERO`, and `ZERO_OR_ONE` is removed |
| Rendering | **Deviation.** Amendment 3 §4 says the renderer rejects outgoing transitions. `rendering.py` is PURE by its module contract, so the refusal lives in the registry's `EXACT_ZERO` preflight and in the legacy emitter guards (`legacy._emit_doccacheload`, `legacy._emit_doccacheremove`). `render_doccacheremove` renders the terminal `<dragpoints/>` form |
| Graph verifier | Load and remove are terminal and always terminal, read from the authority. An outbound edge is `TERMINAL_SHAPE_HAS_OUTBOUND` |
| Effects | `cache_remove` is a retry-replay hazard (`effect_kind` `cache_remove`) and makes a child replay-unsafe |
| Legacy builder | The `transform.mode="doccacheremove"` refusal is at `transform.mode`. Flow-sequence successors of a load or put are refused at the successor's `.kind`; an implicit continuation is refused at the cache step's `.kind`. `document_cache_ref` combined with `catch_exception` is refused through the shared composition check. The DLQ synthetic Stop is dropped **without a reserved slot (ledger C16)**. `doccacheremove` is **withdrawn from the flow_sequence vocabulary and refused by name (ledger C18)** |
| Primitives | Put, remove and retrieve metadata state terminal consumption and trigger dependence. Remove documents its refused inline composition |
| Served contract | The cache node summaries carry the exact §10 cache summary. The document semantics set the reads' input to `required` and the removal to `consumed`. The derived document-emission fact replaces the trailing-cache-put sentence. The flow_sequence served kind list drops `doccacheremove`. The remaining §10 prose (process_call and child text, DDP remediation, `PROCESS_IR_V1.md`, meta-tools legacy prose, doctrine, companion docs) is step 6 |
| Goldens | 000005, 000012, 000018, 000019, 000059, 000060 and 000066 are tombstoned, and 000082–000088 are registered. The active floor is 80. The #178 ledger-citation guard resolves retired paths through their tombstones (ledger C17) |
| Tests | Every test named in heading 12 asserts the measured behaviour under its original node id, or is repointed to a replacement golden. Expected bytes that changed were re-frozen only from the pristine branch-point worktree |

**Non-vacuity witnesses** for the structural rule:
- the refusal of every linear put→read, put→exception and remove→call form, at both entry points
  (`tests/test_process_ir_models.py`, `tests/test_issue_184_native_sequences.py`,
  `tests/test_process_ir_entrypoint_diagnostic_parity.py`);
- the byte-exact replacement goldens;
- the #151 direct-corpus specimen coverage for `cache_remove`;
- the C17 tombstone test.

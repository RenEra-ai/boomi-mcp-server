# ProcessIR Compiler V1 — Internal CFG and Lowering Contracts (issue #137, M12.2)

**Status:** shipped dark. The compiler exists (`src/boomi_mcp/compiler/process_ir/`) and is
covered by parity, determinism, and invariant tests, but **nothing at runtime constructs or
consumes it**: no MCP tool, no builder, no emitter, no JSON Schema, no XML behavior change, and
no deprecation. `flow_sequence` and every other legacy dialect continue through their unchanged
paths until #138 (emitter registry) and #139 (production adapters) reach parity. Pinned by test:
importing `boomi_mcp.models` or `server` must not pull in `boomi_mcp.compiler`.

**References:** [ADR-001](ADR-001-process-ir-authority.md) (§6 authored-vs-derived, §7 error
families, §11 security), [PROCESS_IR_V1](PROCESS_IR_V1.md) (the models this consumes),
[M12 Compatibility Inventory](M12_COMPATIBILITY_INVENTORY.md), issue #137 / epic #134.

## 1. Phases

```
authored payload ──parse──▶ ProcessIRV1 ──lower──▶ SemanticCfgV1 ──lower──▶ EmissionPlanV1
                  (#136)                 (#137)                    (#137)
```

| Phase | Owner | Fails with |
|---|---|---|
| `schema` | #136 `parse_process_ir_v1` | the seven `PROCESS_IR_SCHEMA_*` / `_REFERENCE_*` / `_CAPABILITY_*` codes, translated verbatim |
| `reference_resolution` | symbol-table binding | `PROCESS_IR_COMPILE_EMISSION_PLAN_INVALID`, `PROCESS_IR_CAPABILITY_UNSUPPORTED` |
| `semantic_lowering` | IR → CFG + CFG invariants | `PROCESS_IR_SEMANTIC_*`, `PROCESS_IR_COMPILE_INTERNAL/NONDETERMINISTIC` |
| `emission_planning` | CFG → plan + plan invariants | `PROCESS_IR_COMPILE_*` |

Entry points (`pipeline.py`): `parse_and_compile_process_ir_v1(payload, symbols)` and
`compile_process_ir_v1(ir, symbols)`. Orchestration lives in `pipeline.py` rather than
`diagnostics.py` because `diagnostics` is imported *by* `lowering` and `invariants` — hosting the
entry points there would close an import cycle.

## 2. Why two layers

`SemanticCfgV1` carries control-flow **meaning** only: node identity (an RFC 6901 pointer into the
authored payload), the node's own semantic facts, typed edges, exit roles. It has **no** shape id,
coordinate, dragpoint, or XML state — pinned by test (`test_cfg_carries_no_layout_or_shape_state`).

`EmissionPlanV1` owns everything **generated**: the synthetic Start, the synthetic Stops after
routed targets, `shapeN` identities, geometry, dragpoints, resolved component ids, and emitter
inputs. Synthetic nodes exist only here and carry no authored provenance.

That split is the point of the issue: a caller cannot author reachability, wiring, a shape id, or
a synthetic node, because those concepts do not exist at the layer a caller can reach.

## 3. Contracts

- Every collection is a `tuple` — natively frozen by Pydantic and order-preserving. No `Mapping`
  anywhere: the runtime compiler must not depend on the test-only
  `_process_ir_compat._FrozenMapping`, and re-implementing a frozen mapping would re-open the
  freeze-contract escalation that cost #136 four review rounds. `SymbolTableV1` is therefore a
  sorted tuple with a lookup method, canonicalised on `ref` at construction so caller insertion
  order cannot reach output.
- All models are `extra="forbid", frozen=True`, with `__repr_args__` suppressing every value
  outside a small structural allow-list (mirroring `_ProcessIRBase`).
- Identities are numeric-ascending (`n1..nN`, `e1..eM`, `shape1..shapeN`) and are **never**
  lexically sorted — `shape10` sorts before `shape2` lexically, which would silently reorder a
  plan.
- Edge order is canonicalised at the end of lowering to `(source node ordinal, local ordinal)`.
  A depth-first walk naturally emits a decision's false-arm edge only *after* the entire true-arm
  subtree; canonicalising means the traversal strategy cannot leak into output.
- Canonical JSON reuses #136's recipe exactly: `model_dump(mode="json")` then
  `json.dumps(sort_keys=True, separators=(",", ":"), ensure_ascii=True)`. `sort_keys` orders
  object keys only, so tuple order survives.

### Provenance grammar

Pointers are **absolute from the document root**, matching #136's `_json_pointer`:

```
/body/steps/{i}
/body/steps/{i}/legs/{j}                      (branch leg edge)
/body/steps/{i}/legs/{j}/steps/{k}
/body/steps/{i}/legs/{j}/terminal
/body/steps/{i}/true_arm|false_arm            (decision outcome edge)
/body/steps/{i}/true_arm|false_arm/steps/{k}
/body/steps/{i}/true_arm|false_arm/terminal
```

Nested control nests further, e.g.
`/body/steps/2/true_arm/terminal/legs/0/terminal` for a target inside a branch inside a decision
true arm.

### Edge kinds

`ordering` · `branch_leg` · `decision_outcome` · `terminal` · `catch`

`terminal` is the sequential edge landing on a node that carries an exit role — it marks the edge
that ends a path, while `ordering` is an ordinary step-to-step edge. `catch` is **reserved for
#142** scoped Try/Catch; V1 generates none and the invariant checker rejects any edge carrying it,
so the reservation cannot rot into an accidental capability.

### Exit roles

| Role | Meaning | Plan consequence |
|---|---|---|
| `stop` | authored `StopNodeV1` | emitted as-is, `origin="ir"` |
| `return_documents` | authored standalone terminal | emitted as-is |
| `exception` | authored `ExceptionNodeV1` | emitted as-is, no Stop follows |
| `routed_target` | a `target` terminating a branch leg or decision true arm | **compiler appends a synthetic Stop** |
| `cache_stage` | a target-less staging leg ending in `cache_put` | **no additional synthetic Stop** — the cache shape itself is the terminal |

## 4. Research ledger (the #137 research gate)

All anchors are in `src/boomi_mcp/categories/components/builders/process_flow_builder.py` unless
noted. Verified by reading the code and by the parity tests that pin these facts against emitted
XML.

**Shape identity and ordering.** Names are purely positional `shape{N}`, 1-based
(`_emit_linear_shapes:4334`); `_emit_stop` even re-derives its index from the name's digits
(`:4221`). `_append_path:5564` allocates indices **depth-first** — linear prefix, then the terminal
block, then each control subtree in authored order — and `_append_decision:5619` /
`_append_branch:5665` append the control shape *before* their subtrees, so index-allocation order
and XML document order coincide. The composed path puts the `[start_noaction, connectoraction_source]`
prefix at shape1/shape2 and starts the sequence at shape3 (`_emit_composed_flow_shapes:5709`).

**Layout.** `_SHAPE_Y=96.0`, `_START_SHAPE_X=96.0`, `_START_SHAPE_Y=94.0`, `_SHAPE_X_STEP=160.0`,
`_DRAGPOINT_X_OFFSET=144.0`, `_DRAGPOINT_Y=104.0`, `_CATCH_SHAPE_Y=456.0`,
`_CATCH_DRAGPOINT_Y=464.0` (`:425-435`); `_shape_x(i)=96.0+(i-1)*160.0` (`:438`),
`_dragpoint_x(i)=_shape_x(i)+144.0` (`:443`). **These are floats and render with a trailing `.0`**
(`x="96.0"`). Carrying them as ints would silently break byte parity for the #138 emitter.

**Dragpoints.** The plain helper `_emit_dragpoints:4231` writes `name`, `toShape`, `x`, `y` and
**no** `identifier`/`text`. Branch (`_emit_branch:4346`) writes the same 1-based integer in both
`identifier` and `text`, all legs sharing one `x` and `y=104.0`. Decision (`_emit_decision:4486`)
writes exactly two, with a **case asymmetry**: `identifier="true"`/`"false"` lowercase but
`text="True"`/`"False"` title-case; the true edge sits on `y=104.0` and the false edge on
`y=464.0` — the only place the catch-row dragpoint Y is reused for a non-catch edge.

**Stop/Return insertion.** There is **no `stop` kind in `_FLOW_SEQUENCE_ALLOWED_KINDS`** — a
legacy caller can never author a Stop, so every `<shape shapetype="stop">` in composed output is
builder-invented: `_target_terminal_entries:5500` (`[target, stop]`), `_append_decision:5648`
(false-arm `[stop]`), `_append_branch:5692` (per-targeted-leg `[target, stop]`),
`_terminal_flow_entry:3072`. **ProcessIRV1 is different**: it has a real `StopNodeV1`, and the
root sequence and decision false arm author one explicitly. The compiler therefore attributes a
Stop to IR whenever IR authored it and synthesises one only after a `routed_target` — the single
case where legacy emits a Stop that IR does not represent. `return_documents.enabled` *replaces*
the Stop (verifier invariant `RETURN_DOCS_STOP_EXCLUSIVE`), and a target-less staging leg gets no
*additional* terminal shape — its last linear shape (the cache write) is emitted with no outbound
wire and is itself the terminal (`:5685-5688`).

**Branch/Decision edge ordering.** Branch legs run in authored order with indices allocated
leg-by-leg (leg *n+1* starts where leg *n* ended). Decision allocates the **true** arm first
(from `decision_index + 1`), then the false arm. The true arm's fallthrough is the top-level
success terminal (`_target_terminal_entries(config)`); the false arm's is its own Stop only.

**Connector-to-map transport shape: none exists.** Three independent lines of evidence — the
complete enumerated `shapetype` literal set contains no transport kind (`grep -ic transport` → 0);
flow assembly is a flat ordered list with nothing inserted unconditionally between the source
connector and the transform slot (`:880-1149`); and `_emit_map:3612` emits a single dragpoint
straight to the next shape name. Pinned by
`test_no_transport_shape_between_connector_and_map`.

**Connector metadata is NORMALIZED, not passed through.** The legacy builder resolves connector
aliases to a canonical subtype and normalizes action case, with role-dependent rules:
`_canonical_connector_type:` maps `rest_client`/`rest` → `officialboomi-X3979C-rest-prod` and
`soap_client` → `wssoapclientsdk`, passing other families through. A **source**
(`_source_prefix_flow_entries:5467`) uses the canonical subtype and upper-cased action for the REST
family, but a **lower-cased** subtype and raw action otherwise; a **target**
(`_target_terminal_entries:5500`, `_branch_target_params:2402`) always uses the canonical subtype
and an upper-cased action. The compiler reproduces these rules exactly, reusing the builder's own
helper so the alias table cannot drift (pinned by
`test_connector_canonicalization_matches_the_legacy_builder`). Passing a symbol's raw alias through
would hand #138 an input that serialises non-parity connector XML — the frozen compat bindings
literally carry `rest_client`.

**Why the compiler canonicalizes rather than trusting the symbol table.** The symbol table is
*specified* to carry canonical connector metadata, so canonicalizing is a **no-op on conforming
input** — `_canonical_connector_type` is idempotent. It is applied anyway because the alternative
to normalizing is not decoupling: detecting non-canonical input requires the *same* alias
knowledge, and the canonical set is open (`database` and any future family pass through verbatim),
so no closed accept-set can be enumerated. Given both options need the knowledge, normalizing is
strictly more useful than rejecting — it turns a contract violation by a future adapter into
correct output instead of silently wrong XML, which is exactly the defect this replaced. The
dependency is a deliberate, drift-tested M12 seam and moves to #139 when the production adapters
own legacy normalization.

**Exception parameter sources are resolved, not passed through.** `ExceptionInputV1` carries a
closed `binding` union — `none` (emits nothing), `current_document` (a bare current
`parametervalue`), or `caught_error` (the fixed `meta.base.catcherrorsmessage` /
`Base - Try/Catch Message` token) — mirroring `_emit_exception_parameters` (builder `:6164`), so
#138 only has to serialise it. This matches how Data Process operations already carry their fixed
wire metadata rather than a raw enum.

**Property names are stripped on the wire.** `_validate_bare_property_name` checks the *stripped*
name but `SetDdpNodeV1`/`SetDppNodeV1` store the original, so `" DDP_X "` is a **valid** ProcessIR
payload. The legacy emitter strips it (`_seq_linear_emit:5443` for the step name;
`_emit_property_source_value:4051`/`:4063` for ddp/dpp source `property_name`; `:4040` for
`profile_type`), so the compiler strips at snapshot time. `default_value` and static `value` are
deliberately **not** stripped, matching the emitter.

**Mapping dictionaries.** `_DATAPROCESS_OPERATIONS:162` (`custom_scripting`→`processtype "12"`
"Custom Scripting", `split_documents`→`"8"` "Split Documents", `combine_documents`→`"9"` "Combine
Documents"); cache aliases `cache_put`→`doccacheload`, `cache_get`→`doccacheretrieve` (with
`loadAllDoc="true"` hard-coded), `doccacheremove`→`doccacheremove` (`_seq_linear_emit:5417-5436`);
DDP/DPP wire prefixes `dynamicdocument.` / `process.` with display names `Dynamic Document
Property - {name}` / `Dynamic Process Property - {name}`, and **DDP `persist` is always `"false"`
on the wire** while DPP honours the authored flag (`_emit_documentproperty_assignment:4077-4102`);
property source `valueType` mapping where `ddp`→`track` and `dpp`→`process`
(`_emit_property_source_value:4016`). `SyncPipelineBuilder._linear_stage_order:6998` derives stage
order from the indegree-0 walk, **not** list position. Verifier terminal sets live at
`process_graph_verifier.py:45-64`.

**Determinism proof.** `test_all_stage_and_dependency_permutations_lower_identically` runs all 12
stage/dependency permutations of a three-stage chain and requires identical lowering *and*
byte-identical XML; `test_input_key_order_cannot_change_output` and
`test_symbol_insertion_order_cannot_change_output` do the same for the compiler itself. The
emission-plan schema was frozen only after these passed.

### Contractual vs incidental

| Aspect | Classification |
|---|---|
| Topology (nodes, edges, reachability, terminals) | **contractual** |
| Ordering (shape index allocation, branch leg order, decision true-then-false) | **contractual** |
| Synthesis (which Stops the compiler owns, the synthetic Start) | **contractual** |
| Wiring (`toShape` targets, dragpoint identifier/text) | **contractual** |
| Geometry (x/y coordinates, dragpoint rows) | semantically **incidental** — the builder's own comment calls it "decorative only; correctness is driven by `toShape` wiring" (`:422`) — but **compatibility-pinned in V1**, because #138 must reproduce current XML byte-for-byte |

### Live observation

A read-only inspection of the `renera` account confirmed the Start→Message→Stop geometry
(`start` at `x="96.0" y="94.0"`, mid-row shapes at `y="96.0"`, dragpoints at `y="104.0"`) and
`<stop continue="true"/>`, matching the constants above. No live component id is recorded here —
the fixtures and this document use sentinel references only.

## 5. Parity oracle

The plan is validated against the **unchanged legacy builder**, not against a hand-written
expectation. `test_process_ir_compiler.py` projects emitted XML into
`(shape name, shapetype, x, y, dragpoints[name, toShape, x, y, identifier, text])` facts and
requires the emission plan to describe exactly those facts for:

- all three golden IR documents (`process_ir_v1.json`),
- all ten frozen codec parity cases (`flow_sequence_compat_cases.json`),
- a constructed decision-with-false-arm-Stop case (no fixture exercises that shape, and it is the
  one process containing both an IR-authored Stop and a compiler-owned Stop).

There is deliberately **no test-only plan→XML emitter**: emission is #138's boundary, and a second
emitter would only prove the two agreed with each other.

## 5a. What the invariant checkers cover

The checkers exist to catch a **compiler** defect, so they are exercised against hand-built
malformed records, not just against real lowering output. Beyond the obvious (duplicate/dangling
ids, unreachable nodes, missing terminals, noncanonical ordinals) they enforce:

- **Exit role agrees with semantics** — a `stop`/`return_documents`/`exception` node must carry its
  role, a `target` may be `routed_target` only, a `cache_put` may be `cache_stage` only, and no
  other kind may claim a role. Without this a Stop with `exit_role=None` reads as a linear node and
  would be planned with an outgoing transition the Stop emitter cannot serialise.
- **Forward-only control flow** — checked *after* reachability, because a fully-reachable, acyclic,
  join-free graph can still be ordered backwards (`n1 -> n3 -> n2`), which would wire a later shape
  to an earlier one.
- **Per-source local ordinals unique and contiguous** — sorted order alone accepts two edges sharing
  `(source, local_ordinal)`, which plan lowering would silently renumber.
- **Transitions match their CFG edges, IN ORDER** — a node's ordered `cfg_edge_id` sequence must
  equal its ordered CFG out-edges, and each transition must target that edge's shape. Per-transition
  checking alone is too weak: swapping *both* the `cfg_edge_id` and `to_shape_id` of a Decision's
  two wires leaves each individually consistent while the position-fixed dragpoint labels route
  `True` down the false arm. The synthetic Start must wire to the CFG entry, and `synthetic`
  provenance is restricted to exactly the Start wire and routed-target Stop wires — otherwise a
  malformed plan could relabel an ordinary wire as synthetic and skip correspondence entirely.
- **`routed_target` is role- and position-checked** — only a `target` endpoint may carry it (a
  *source* marked routed would get a synthetic Stop appended after it), and only where the IR can
  actually author a target terminal: `…/legs/{j}/terminal` or `…/true_arm/terminal`. Keying the
  exit-role table on `semantic_kind` alone missed the role; a bare `/terminal` suffix test missed
  the position, since `DecisionFalseArmV1.terminal` is Stop/Branch/Exception only — a target there
  is unrepresentable, and planning it would append a synthetic Stop on the reject route.
- **Synthetic Stops are inert and declared** — a `terminal_stop` must have no outgoing transitions
  *and* appear in `terminal_shape_ids`. The generic terminal check only inspects shapes that are
  declared, so a multi-exit plan could otherwise wire one synthetic Stop onward to another exit and
  simply omit it from the declaration.

**The plan checker validates the CFG first.** Most plan invariants are stated *against* the CFG and
silently borrow its guarantees: "one plan node per CFG node" borrows id/path uniqueness and canonical
node order; transition-to-edge correspondence borrows endpoint uniqueness, canonical edge order and
edge kinds; routed-target Stop synthesis borrows valid exit roles and positions; and reachability,
acyclicity, join-freedom and forward-only flow are borrowed outright. Because
`check_emission_plan_invariants` is **exported and callable directly**, a caller who skipped
`check_cfg_invariants` would get silent acceptance of a malformed graph rather than a diagnostic — so
it now re-validates the CFG up front. That is O(V+E), cheap against the cost of shipping a plan built
from a broken graph, and it closes the class rather than the instance: this was first noticed as a
single leafless-cycle hole, and enumerating the borrowed invariants showed the hole was general.

**Complexity.** Validation is linear in nodes+edges. Two places matter: CFG out-edges are grouped
by source **once** before the plan-node loop (rescanning `cfg.edges` per node would make it O(V·E)),
and reference resolution runs against an index built **once per pass** via
`SymbolTableV1.build_index()` (the checker resolves every node's references, so a per-reference scan
would make it O(nodes x symbols)). The index is deliberately **not** cached on the model: pydantic v2
includes private attributes in `__eq__`, so a lazy cache makes two identical tables compare unequal
once one is used; `model_copy(update=...)` does not re-run `model_post_init`, so an eager cache goes
stale and silently resolves a present symbol to `None`; and a private attr stays writable despite
`frozen=True`. `SequenceNodeV1.steps` has no upper bound, so neither cost is bounded by the schema.

Both dimensions are guarded structurally (iteration-counting, not wall-clock, so they cannot flake):
`test_plan_validation_never_rescans_cfg_edges_per_node` for the edge dimension and
`test_plan_validation_is_linear_in_symbols_too` for the symbol dimension — the latter exists because
the node-count guards pass an EMPTY symbol table and so are blind to lookup cost. Each was verified
to FAIL when its optimisation is reverted. `test_plan_validation_scales_linearly_with_node_count`
guards this and is calibrated to discriminate (measured: ~8.3× for 8× nodes grouped, ~30× rescanned;
it fails if the rescan returns, and sizes below ~400 do not discriminate at all).
- **Emitter input matches the node's semantics** (and, for connectors, its role), so a Map node
  cannot carry a `MessageInputV1`.
- **Emitter inputs are RECOMPUTED and compared exactly** — the checker re-derives each node's
  emitter input from its CFG semantics plus the symbol table and requires equality. Checking only
  the emitter *kind* plus global component-id membership was far too weak: a wrong semantic value,
  a Stop with `continue_=False`, or a map id belonging to an unrelated symbol all passed.
  Recomputation makes the check total and is simpler than enumerating per-field rules.
- **Control edges are bound to their authored subtree** — a decision outcome must target a node
  under its own `true_arm`/`false_arm`, and a branch leg edge under its own `legs/{j}`. Ordering
  alone would let two targets be swapped while every ordinal stayed valid.
- **`cache_stage` is position-checked** like `routed_target` — it is authored only as
  `BranchLegV1.terminal`, so a root or mid-flow `cache_put` claiming it would mark an ordinary
  linear node terminal and silently truncate the path.
- **Identities and terminal sets are canonical** — `edge_id` must equal `e{ordinal}` (not merely be
  unique), and `terminal_shape_ids` must be exactly the ordered set of shapes with no outgoing
  flow, so duplicates or reordering cannot make two equivalent plans serialise differently.
- **Synthetic Stop adjacency** — a routed target must be *immediately* followed by its own synthetic
  Stop and wired to it; matching counts alone would let the Stop sit anywhere.
- **Branch dragpoint row** — Branch dragpoints all sit on `DRAGPOINT_Y`; unlike Decision there is no
  second row.

## 6. Diagnostics

Every diagnostic carries a stable `code`, the authored RFC 6901 `path`, a `node_identity` (the
nearest authored node path, `<root>` when the pointer names no node), a static `message`, static
`remediation`, and the `phase`. Diagnostics sort by `(phase rank, path, code)` so the earliest
failure in the pipeline reads first.

The `SEMANTIC_*` / `COMPILE_*` split is a contract, not decoration: `SEMANTIC_*` blames the
authored payload, `COMPILE_*` blames the compiler. Reporting a compiler defect as a user error is
how a caller ends up "fixing" correct input.

## 7. Security (ADR-001 §11)

- Messages and remediations are **static strings selected by code**. No authored value, resolved
  component id, or exception text is ever interpolated — including in the raised exception's
  `__str__`, which is what reaches a log. An unexpected exception becomes a bare
  `PROCESS_IR_COMPILE_INTERNAL` with the exception text deliberately discarded.
- `ComponentSymbolV1` accepts only emitter-safe facts: resolved component id/type and derived
  connector/action metadata. Never configuration, credentials, headers, or document content.
- Generated identities are pure ordinals, so they cannot encode a secret.
- `__repr_args__` suppresses every value outside a structural allow-list. Pinned by a sentinel
  test that seeds a marker into every authored and symbol slot and asserts it appears in no
  diagnostic field, no `repr`, and no exception string.

## 8. Boundaries and gates

- **WSS / listener entry is rejected** with #136's `PROCESS_IR_CAPABILITY_UNSUPPORTED` in the
  `reference_resolution` phase. The legacy path *fuses* the start and connector into a single
  `start_listen` shape (`_emit_start_listen:3430`), whereas this compiler always emits the
  `start_noaction` + `connectoraction` pair — so a listener source would be silently mis-shaped.
  Note the guard lives in reference resolution, **not** IR lowering: `ProcessIRV1` has no listener
  node kind at all, so such an entry can only arrive through the symbol table's `connector_type`.
  #140 owns the alternate entry policy; no WSS cutover may happen before it lands.
- **`return_documents` with a control terminal is unrepresentable** — rejected by both
  `_validate_flow_sequence_config:4733` and the #136 codec, so the compiler has no branch for it.
- The six #137 codes are the **first** codes of the `PROCESS_IR_SEMANTIC_*` and
  `PROCESS_IR_COMPILE_*` families (ADR-001 §7). `PROCESS_IR_CAPABILITY_UNSUPPORTED` is
  **referenced, never re-registered**: `ERROR_TAXONOMY` is a dict comprehension keyed on
  `spec.code`, so a duplicate entry would silently overwrite #136's and flip its owner. Pinned by
  `test_issue_136_codes_still_owned_by_136`.

## 9. Ownership boundaries

**#137** (this document) owns the internal CFG, the emission plan, and the lowering contracts.
**#138** owns the verified emitter registry that turns an `EmissionPlanV1` into XML — every XML
tag, attribute order, escaping rule, and image name is its boundary, not this one's. **#139** owns
the production legacy adapters. **#141/#142** own the gated control-flow and error-handling
capabilities (continuation after Branch/Decision, scoped Try/Catch — the reserved `catch` edge
kind). **#143** owns CFG-aware semantic validation built on these types.

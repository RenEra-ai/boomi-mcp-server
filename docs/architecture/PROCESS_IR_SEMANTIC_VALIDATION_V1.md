# ProcessIR Semantic Validation V1 (#143, M12.8)

- **Status:** Shipped
- **Owner issue:** [#143](https://github.com/RenEra-ai/boomi-mcp-server/issues/143) (M12.8), epic #134
- **References:** [ADR-001](./ADR-001-process-ir-authority.md) §6 (authority), §7 (error families),
  §11 (secrets); [PROCESS_IR_V1](./PROCESS_IR_V1.md); [PROCESS_IR_COMPILER_V1](./PROCESS_IR_COMPILER_V1.md);
  [M12 Compatibility Inventory](./M12_COMPATIBILITY_INVENTORY.md) (the validator migration matrix)

One pure, deterministic semantic pass over a resolved `ProcessIRV1` and its compiler-derived CFG,
producing an explainable report **before** any component is built or mutated.

---

## 1. Contract

```python
validate_process_ir(
    ir: ProcessIRV1,
    symbol_table: SymbolTableV1,
    capabilities: ProcessIRValidationCapabilitiesV1 = DEFAULT_VALIDATION_CAPABILITIES,
) -> ValidationReportV1
```

Pure: no network, no filesystem, no clock, no global state, no mutation. The same triple always
yields the same report.

**Expected invalidity is returned, never raised.** A payload being wrong is the normal case this
function describes. Only a COMPILER defect raises, and it raises as itself (`ProcessIRCompileError`),
so existing handling is unchanged.

`ProcessIRV1` is strict but **not frozen**, so the validator re-validates a dump at entry
(`ProcessIRV1.model_validate(ir.model_dump())`) and every phase reads only that snapshot. Validation
exists to gate a mutation, which makes "payload changed after it was checked" the failure that
matters most.

## 2. Severities

| Severity | Blocks? | Meaning |
|---|---|---|
| `error` | **yes** | The payload is wrong. Compilation and mutation stop. |
| `warning` | no | Unproven, not wrong. Reported and ignored by every gate. |
| `advisory` | no | A named legacy exemption fired, or an assumption was recorded. |

`report.is_valid` is `not report.errors`. Nothing else blocks — which is what lets a tightened rule
ship and be observed before it is enforced.

## 3. Phase order

Rank, not name, drives ordering, so the earliest failure in the pipeline reads first:

```
model → capability → reference → terminal → reachability → profile
      → cardinality → lineage → side_effect → retry → compatibility
```

Reordering this tuple is a contract change: it changes report ordering.

**Determinism.** Each severity bucket sorts by `(phase rank, authored path, node identity, code,
canonical evidence)` and deduplicates on `(code, path, evidence)`. Every component is needed for a
TOTAL order — stopping at `code` would leave two findings that differ only in evidence in arbitrary
relative order, stable within a process and unstable across runs.

## 4. Diagnostic schema

Every finding carries a stable `code`, `severity`, `phase`, an RFC 6901 `path` into the AUTHORED
payload, its `node_identity`, a static `message` and `remediation`, closed `evidence`, and an
optional compiler-internal `internal_node_id`.

**Codes never depend on message wording.** Messages and remediations are static strings selected by
code and are never interpolated.

## 5. Security — where the redaction guarantee comes from

Two controls, and it matters which does the work:

1. **The closed evidence-key allowlist is the primary control.** Keys are chosen by code, never by a
   caller, and no key carries a name, id, ref, label, or free text. A property name like
   `dpp_customer_email` is lexically indistinguishable from a structural token, so no value rule
   could reject it — the reason it cannot appear is that **there is no key to put it under**.
2. **The value-shape rule is defense in depth.** It rejects what *is* distinguishable: component ids
   (dashes), `$ref:` tokens, labels (spaces/mixed case), script and exception text (newlines), and
   anything over a short bound.

`__repr_args__` suppresses every non-structural field, so neither a log line nor a traceback carries
authored text.

## 6. The state model

| Scope | Lifetime | Branch-leg behavior |
|---|---|---|
| `ddp` (document property) | travels WITH a document | pre-Branch writes reach every leg; a leg-local write does **not** reach a sibling |
| `dpp` (process property) | the execution | an EARLIER leg's write **is** visible to a later leg |
| `cache` | the execution | same as `dpp` |

Branch legs execute **sequentially**, in leg order. That single fact produces all three rules, and a
model that treated legs as isolated would get the third wrong.

- **Decision arms are exclusive.** Converging paths **meet** (intersection), never union. A property
  written on one arm only is not established after the merge.
- **Try/Catch** forks from scope-entry state: a write inside the try body may not have happened when
  the failure occurred.
- **Non-strict readers.** Decision operands emit `defaultValue=""` on the wire, so an unwritten
  property is a defined empty string, not an error. They fail only when a writer EXISTS but is
  provably invisible. The legacy walker encodes the identical rule
  (`cache_property_lineage.LineageEvent.strict`), and the shipped `control_flow` golden depends on it.

## 7. Conservative-effect rules

An undeclared map or script contributes **uncertainty, never proof** — the inversion of the legacy
wildcard default, and the precision the issue exists to add. A typed contract contributes exact
reads and writes:

| Contract | Bound to |
|---|---|
| `MapEffectContractV1` | the map component (`map_ref`) |
| `ScriptEffectContractV1` | `language` + **SHA-256 of the exact source** |
| `SubprocessSummaryV1` | the child's `process_ref` |

Binding a script contract to its digest makes it non-transferable: editing the script invalidates
the contract automatically instead of silently vouching for code that no longer exists.

**There is no `trusted=True`.** Presence in the typed capability set IS the verification boundary.
Capabilities are compiler context, never authored IR fields (ADR-001 §6), so no payload can assert
its own trustworthiness. `DEFAULT_VALIDATION_CAPABILITIES` is empty — **strict is the default**, not
an opt-in.

## 8. Where the gate runs

| Site | Slice | What it gates | Policy |
|---|---|---|---|
| `integration_builder._process_component_preflight` | 8 | plan/apply, for `create`/`create_clone`/`update` on process components | the config's dialect exemptions (`legacy_bridge.py`) |
| `legacy_adapters.emission.emit_legacy_result` | 9 | the canonical `compile → emit` chain | the named dialect's registered exemptions |

**Both gates apply the same policy.** An earlier draft of this table said the plan gate was
"strict"; that was wrong, and the two-row contrast wrongly implied a payload could survive plan and
die at emission. No payload can: `legacy_bridge.validate_legacy_process_config` resolves the adapter
from the config and applies its registered policy exactly as the emission gate does.

The shipped behavior is the one the feature needs. A genuinely strict plan gate would reject every
legacy `flow_sequence` standalone cache read at plan time, and
`LEGACY_ADAPTER_EXEMPTION_STANDALONE_CACHE_READ` could never be observed at all.

`create_clone` is in the guard and is **reachable**, though not by authoring it: `IntegrationComponentSpec.action`
is `Literal["create", "update"]`, so no caller can supply it — but `_build_plan` DERIVES
`planned_action = "create_clone"` itself when `action: "create"` meets a name collision under
`conflict_policy: "clone"`. The guard tests the derived action, so that arm is live. An earlier
version of this section called it unreachable, conflating the authored action with the derived one.

Both run **after** every existing legacy check. That ordering is load-bearing: existing public error
codes keep winning, so a payload that fails a legacy check still reports the code it always did.
These gates can only ADD a rejection the legacy path missed.

A fatal report at the plan gate sets `planned_action = "error_process_validation"`, which is in
`_apply_plan`'s fail-fast `unresolvable_steps` set — so **no `_execute_component` runs**.

### Deviation from the architect plan, and why

The plan placed the compiler gate inside `compile_process_ir_v1`. That was implemented and measured:
it breaks the legacy parity suites for TWO independent reasons.

The first is structural: canonical validation is deliberately stricter than the legacy surface, the
legacy surface keeps its behavior through exemptions keyed on ADAPTER IDENTITY, and
`compile_process_ir_v1(ir, symbols)` does not know which adapter produced its IR, so it cannot look a
policy up.

The second is independent and no exemption addresses it: the compiler's own fixtures use placeholder
component types (`component_type="t"`), which the reference phase reports as
`…REFERENCE_COMPONENT_TYPE_MISMATCH`. A faithful reproduction at `ba0d51c` measures **20 failing
tests** — `…LINEAGE_CACHE_WRITER_MISSING` (17), `…LINEAGE_PROPERTY_READ_BEFORE_WRITE` (7, with 7
tests carrying both), and `…REFERENCE_COMPONENT_TYPE_MISMATCH` (3, exempted by nothing). An earlier
version of this section said the failures were "exactly" the two exemption-covered codes; that was
wrong. The third code strengthens the case — even a policy-aware compiler gate would still reject
those three.

`emit_legacy_result` is the single production entry into the canonical chain **and** knows its
dialect, so the gate lives there. With policies applied, all 40 raw-byte XML goldens and every parity
suite stay green. `compile_process_ir_v1` keeps its existing contract for direct callers.

## 9. Legacy exemptions

Named, registry-owned, and keyed on adapter identity. A caller cannot select one: there is no policy
field on `ProcessIRV1`, no policy argument on `validate_process_ir`, and no token a payload could
supply that reaches `lookup_policy`. An exemption a caller can request is not an exemption, it is a
bypass.

An exemption **reclassifies** an error as an advisory carrying the original code in its evidence —
it never deletes the finding. An exemption that hid its finding would make the migration ledger
unfalsifiable.

| Exemption | Covers | Adapters | Status |
|---|---|---|---|
| `LEGACY_ADAPTER_EXEMPTION_OPAQUE_STATE_WRITER` | `…LINEAGE_PROPERTY_READ_BEFORE_WRITE` | `flow_sequence`, `sync_pipeline` | **live** — observed firing on a real build |
| `LEGACY_ADAPTER_EXEMPTION_DECISION_PROPERTY_READ` | `…LINEAGE_EFFECT_UNKNOWN` | `flow_sequence` | **inert** — target ships as a warning (see §10) |
| `LEGACY_ADAPTER_EXEMPTION_STANDALONE_CACHE_READ` | `…LINEAGE_CACHE_WRITER_MISSING` | `flow_sequence` | **live** — observed firing on a real build |
| `LEGACY_ADAPTER_EXEMPTION_SUBPROCESS_SUMMARY` | `…SIDE_EFFECT_ORDERING_UNKNOWN` | `wrapper_subprocess` | **inert** — target ships as a warning (see §10) |

**Removal gate:** each exemption retires when its adapter's dialect declares typed effect contracts
for the constructs it covers. Owner: the M12 epic (#134); the retiring issue is the one that migrates
the dialect off the legacy surface.

## 10. Soundness and completeness

- **Sound** for explicit IR facts and trusted typed metadata.
- **Deliberately incomplete** for arbitrary script/map/subprocess internals — unknown effects never
  establish state.
- Makes **no** claim about topology, deployment, or runtime execution.
- The post-emission XML graph verifier remains a SEPARATE compiler-defect oracle. It cannot validate
  authored semantics, and a verifier failure is classified as a compiler/emitter defect, never a user
  semantic error.

### Known reachability gaps — what is PRE-POSITIONED rather than live

Measured by a live census over 5933 authorable configs plus a 606-config gate differential (issue
#143 QA, 2026-07-26). Two DIFFERENT situations are separated below, because collapsing them is
exactly how a taxonomy entry gets mistaken for a shipped rule.

**A. Implemented, unit-tested, wired — but not producible from an authorable config (4 of 17).**
Forcing the condition makes each of these fire; the v1 authored surface simply cannot produce the
shape.

| Code | Why it cannot fire yet |
|---|---|
| `…SIDE_EFFECT_ORDERING_UNSAFE` | needs a property read downstream of a non-waiting `process_call`; a `process_call` may live only in a pure process-call sequence (`process_call_connector_mixing` is gated) and is rejected inside a Branch/Decision body |
| `PROCESS_IR_REFERENCE_COMPONENT_NOT_FOUND` | the legacy adapter's symbol table carries only refs it declared, so an undeclared ref never reaches the collector |
| `PROCESS_IR_REFERENCE_COMPONENT_TYPE_MISMATCH` | same — a literal wrong-type id (a `map_ref` bound to a `profile.json` id, or an unresolvable UUID) still reports `is_valid: true` |
| `…RETRY_EFFECT_UNSAFE` | **no legacy dialect can project a Try/Catch region at all**, so `derive_error_regions` returns an empty tuple on every legacy-projected CFG and `collect_retry_effect_findings` never has a region to examine. The blocker is one layer earlier than "the hazard never lands inside a retried region" |

**B. Registered in the taxonomy with NO rule behind it (2 of 17).**

TWO codes are declared with no collector emitting them. Group A's defining test — forcing the
condition makes it fire — does **not** hold for either: `finding()` accepts any code outside the
compile family (registered or not), so a synthetic report proves registration, not wiring.

`PROCESS_IR_CAPABILITY_EFFECT_CONTRACT_INVALID` is the first. No collector references it. An earlier
draft filed it under A with the reason "no production caller supplies typed effect contracts yet",
which describes a reachability gap and so implies a rule a caller could reach. There is none: the
capability contracts are validated structurally by pydantic at construction, and nothing re-checks
them against the IR.

`PROCESS_IR_SEMANTIC_LINEAGE_AMBIGUOUS_LAST_WRITE` is the second. It exists as a
constant, a taxonomy row, and a message/remediation entry — no collector references it, and it has no
unit test. A synthetic report can carry it only because `finding()` accepts any code outside the
compile family — registered or not.

This is deliberate rather than an oversight, and the reason is structural: ProcessIR v1 has **no
merge point**. Control nodes are terminal fan-out — nothing may follow a Branch or Decision
(`PROCESS_IR_SEMANTIC_CONTROL_CONTINUATION_UNSUPPORTED`) — so converging paths with disagreeing last
writers cannot exist by construction, not merely for want of input. Writing a merge rule now would be
implementing a check for a construct the IR does not have. The code stays registered so the family is
stable when control continuation lands; the rule arrives with it.

The equivalent LEGACY check is real and unaffected: `cache_property_lineage` emits
`PROCESS_LINEAGE_AMBIGUOUS_LAST_WRITE` (a different, legacy-family code) from
`validate_cache_property_lineage`, which the migration matrix classifies **`refine-with-typed-facts`**
(not `adapter-only-compat` — that is the neighbouring `validate_config_lineage` row).

The shared name is misleading and worth the caveat: the legacy check fires when *the only writer sits
on a mutually exclusive decision path* — an exclusive-path condition, not a merge. So the two
same-named checks are not the same rule, which strengthens rather than weakens the "different family"
point.

Nothing here is a **regression** — the baseline had no such checks at all. This section states how
much of the new surface is currently exercised, and by what.

**Exemption rows that are inert (2 of 4):** `LEGACY_ADAPTER_EXEMPTION_DECISION_PROPERTY_READ` and
`LEGACY_ADAPTER_EXEMPTION_SUBPROCESS_SUMMARY` cover codes that ship as **warnings**
(`…LINEAGE_EFFECT_UNKNOWN`, `…SIDE_EFFECT_ORDERING_UNKNOWN`), and `apply_policy` reclassifies only
ERRORS — a warning never blocked, so exempting one would merely hide it. They therefore never fire
in practice. The other two (`…OPAQUE_STATE_WRITER`, `…STANDALONE_CACHE_READ`) are live and were
observed firing on real builds.

## 11. Failure taxonomy (#143's 17 codes)

ADR-001 §7 makes #143 an introducer for four families only. It introduces **no**
`PROCESS_IR_COMPILE_*` code — that family blames the compiler, and a `ValidationReportV1` cannot
carry one.

| Code | Severity | Fires when |
|---|---|---|
| `PROCESS_IR_REFERENCE_COMPONENT_NOT_FOUND` | error | a non-connector component ref does not resolve |
| `PROCESS_IR_REFERENCE_COMPONENT_TYPE_MISMATCH` | error | a resolved symbol is the wrong type for its role |
| `PROCESS_IR_CAPABILITY_EFFECT_CONTRACT_INVALID` | error | **taxonomy-only — no collector emits it** (see §10 B) |
| `PROCESS_IR_SEMANTIC_LINEAGE_PROPERTY_READ_BEFORE_WRITE` | error | a strict read has no establishing write on its path |
| `PROCESS_IR_SEMANTIC_LINEAGE_DDP_SCOPE_INVALID` | error | a DDP is read outside the document copy that wrote it |
| `PROCESS_IR_SEMANTIC_LINEAGE_BRANCH_ORDER_INVALID` | error | a leg depends on state written by a LATER leg |
| `PROCESS_IR_SEMANTIC_LINEAGE_CACHE_WRITER_MISSING` | error | a cache is read with no preceding write |
| `PROCESS_IR_SEMANTIC_LINEAGE_AMBIGUOUS_LAST_WRITE` | error | **taxonomy-only — no collector emits it** (see §10 B) |
| `PROCESS_IR_SEMANTIC_LINEAGE_EFFECT_UNKNOWN` | warning | a map/script has no typed effect contract |
| `PROCESS_IR_SEMANTIC_LINEAGE_EXTERNAL_WRITER_ASSUMED` | warning | state is assumed to come from a declared external writer |
| `PROCESS_IR_SEMANTIC_SIDE_EFFECT_ORDERING_UNSAFE` | error | a demonstrated unordered dependency (see §10 gap) |
| `PROCESS_IR_SEMANTIC_SIDE_EFFECT_ORDERING_UNKNOWN` | warning | a non-waiting call with undeclared child effects |
| `PROCESS_IR_SEMANTIC_RETRY_EFFECT_UNSAFE` | error | a retried region replays a non-connector effect |
| `LEGACY_ADAPTER_EXEMPTION_*` (4) | advisory | a named legacy policy reclassified an error |

Connector retry/idempotency hazards keep their **#142** codes; connector operation/connection
resolution keeps its **#140** codes. Those say strictly more than the generic equivalents, and
unification must not downgrade them.

## 12. Rollout

Ten slices, each independently committable and each leaving the suite green. Slices 1–7 shipped dark
(wired to nothing); slice 8 is the first mutation gate; slice 9 is the emission gate; slice 10 is
this documentation. Baseline at `4a5ad67`: 7269 passed / 16 skipped / 0 failed.

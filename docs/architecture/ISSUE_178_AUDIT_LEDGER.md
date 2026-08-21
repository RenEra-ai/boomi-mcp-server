# Audit ledger — issue #178 (M12 defect slice, DC-175-E structural fix)

Instantiated at Stage-1 step 0 per `docs/architecture/templates/AUDIT_LEDGER_TEMPLATE.md`.
Conventions inherited from #175's end state apply from row one; platform-behaviour claims
carry a provenance marker (`measured here` / `documented, not measured` / `assumption`).
Counts are derived from the rows, never hand-maintained; the defect-class table is derived
LAST, after the final finding row of the final batch.

## Baseline (Stage-1 step 0)

- Issue: #178 — Diagnostic parity between the two ProcessIR entry points (DC-175-E structural fix)
- Step-0 baseline: `cdd7a3bf8e2e7ae6773f6ec4250844e5c2e8cf8f`
- Measured green baseline at that tree: **10181 passed, 17 skipped** in 890.12s — *measured here*,
  quoted from the run's own summary line, full non-KB suite (`.venv` 3.12, `PYTHONPATH=src`,
  `pytest tests --ignore=tests/kb -p no:cacheprovider`). The run covered the step-0 tree plus this
  ledger and its archive skeleton, which add no collected test nodes; the count is identical to
  #175's closing measurement at `05f9b96`.
- Slice kind: **behaviour-affecting** — the diagnostic a caller receives changes for the documents
  in the regression corpus, so the served `(code, pointer, message)` triple changes on a
  machine-facing surface. Not dark.
- Artifact trust boundary. The slice CREATES AND OWNS: the compile-entry re-parse boundary in
  `src/boomi_mcp/compiler/process_ir/pipeline.py`, the derived parity case generator with its node
  palette and carrier factories, the five-row regression corpus, this ledger, and its evidence
  archive under `docs/architecture/evidence/issue-178/`. It CONSUMES, unchanged and as authorities
  it may never rewrite: `parse_process_ir_v1` and its diagnostic tables in
  `src/boomi_mcp/models/process_ir.py`; `BODY_CAPABILITIES_V1` in
  `src/boomi_mcp/compiler/process_ir/body_capabilities.py`; the `LegacyValidationPolicyV1` registry
  and its exemption map; the in-tree UI-built live captures under `tests/fixtures/live_xml/m11/`;
  and `docs/architecture/ISSUE_175_AUDIT_LEDGER.md`, which is append-only and is never edited by
  this slice.
- Expected defect classes, pre-enumerated so a second instance triggers structurally ON ARRIVAL:
  1. **DC-178-A** (inherits **DC-175-E**) — one rule enforced by two independently written
     validators that share outcomes but not rule ORDERING; runtime authority: the parser's gate
     order in `parse_process_ir_v1`.
  2. A hand-enumeration shadowing a derivable authority — the standing class; the derived
     generator is precisely the fix shape here, so a hand-listed case set is an instance.
  3. A derived value re-derived by hand at the wrong moment (inherits **DC-175-D**) — this slice
     maintains both a defect-class table and manifest floors, and both are derived.
  4. Hand-modelled platform behaviour in served text.
  5. A guard that enumerates nothing and therefore passes everything — recorded in this repo four
     separate times (#149, #151, #162, #175); the non-vacuity witness exists for this class.

## Loop roster (fixed BEFORE the first correction; a gate not listed here cannot mint a loop
## mid-run — a roster addition is itself a recorded checkpoint decision)

1. **Stage-1 QA** — live `boomi-qa-tester` engagement through the public MCP tool boundary,
   scoped to the ProcessIR authoring / materialization / apply surfaces this slice changes.
2. **Stage-2 repo Codex review** — detached, `--base cdd7a3bf8e2e7ae6773f6ec4250844e5c2e8cf8f`,
   then delta-scoped fix rounds. Every round COLLECTED via step 5d, never read from `wait`.
3. **§6 architect implementation review** — declared additive gate, run under
   `/codex-claude:codex-issue`. **Its window is FIXED AT 3 EVALUATIONS by owner decision recorded
   at instantiation** (see *Owner decision* below), with terminal validation for its corrections
   reassigned to loop 2. Recorded here, in advance, so it is not a mid-run roster change.
4. **Composite wave gate** — full non-KB suite + every active golden-manifest entry + deterministic
   compile/fingerprint checks + one integration-level review of the wave delta + one live scenario
   per changed capability class. One composite evaluation, not five loops.
5. **Terminal correction loop** — ONLY via a recorded roster-addition checkpoint.

### Owner decision recorded at instantiation (2026-08-20)

The owner fixed the §6 architect implementation review at **three evaluations** for this slice.
After the third evaluation its findings are applied in one batch, that batch receives its affected
QA and a Stage-2 delta-scoped repo review, and the slice closes on a clean Stage-2 review without a
fourth §6 evaluation. This is a roster/window decision, not a waiver of owed validation: every
applied correction still receives affected QA plus a delta-scoped review under loop 2, so no
correction reaches HEAD unvalidated. The critical-tier rules are unaffected — an unresolved
critical finding still forbids closure and forces `ESCALATE-OPEN` with the issue open.

## Defect-class ledger (empty at instantiation; a class is a (mechanism, runtime-authority) pair,
## assigned at reconciliation, revisable with the original retained; DERIVED LAST from the rows)

| Class | Mechanism | Runtime authority | Instances (derived from rows) | Resolution |
| --- | --- | --- | --- | --- |

Second-instance check: run against this table AT ROW-WRITE TIME, and again when a second finding
lands in the same file or subsystem within a loop. On the second instance of a pair the structural
fix is mandatory in that batch — or in the immediately-next dedicated batch where it touches
dispatch, compiler authority, materialization or shared apply code — with a sibling sweep, a
non-vacuity witness, and a coverage claim derived from the authority's full case set.

## Finding rows (one per raw finding; append-only; exactly one disposition each)

Seeded with the two `INH-*` rows this slice exists to discharge, quoted verbatim from
`docs/architecture/ISSUE_175_AUDIT_LEDGER.md` rows `L3R3-01` and `L3R3-02`. Both spent their
single `window-exhausted` allowance at #175 under reason class `blocked-by-mechanism`, so neither
may be deferred again: each takes a terminal disposition of `fixed`, `finding-refuted`, or
escalation with this issue left open.

| ID | Source gate + run dir + attestation | Verbatim summary | Original label | Blocking class | Defect class | Derived tier (anchor inline) | SHA/delta | Disposition |
| --- | --- | --- | --- | --- | --- | --- | --- | --- |
| INH-L3R3-01 | INHERITED, not run by this slice — origin row `L3R3-01` of `docs/architecture/ISSUE_175_AUDIT_LEDGER.md` (§6 architect implementation review of #175, round 4, base `c2ebc43`, head `05f9b96`, verdict `ISSUES FOUND`, attested against the plan bytes). The run directory name and its collected attestation are recorded on that origin row and archived under `docs/architecture/evidence/issue-175/architect-reviews/`; this slice does not restate the run token, because #178's own archive does not hold that run and a citation its archive cannot back is a fabricated row | "Root placement precedence still diverges" — `[process_call, source]` parses as cardinality at `/body` but compiles as connector mixing at `/body`; `[branch, process_call]` parses as control-continuation at `/body` and fully compiles as ambiguous-flow at `/body/steps/0`, and the added fail-closed test only asserts that SOME nonempty code exists | **P2** | machine-served schemas/contracts | DC-178-A (inherits DC-175-E) | Standard — anchor: source label P2; no critical class or anchor. | inherited at `cdd7a3bf8e2e7ae6773f6ec4250844e5c2e8cf8f` | `inherited-open` — terminal disposition recorded at #178 closure; `window-exhausted` is spent and unavailable |
| INH-L3R3-02 | INHERITED, not run by this slice — origin row `L3R3-02` of `docs/architecture/ISSUE_175_AUDIT_LEDGER.md`, same #175 round-4 gate and archive location as INH-L3R3-01 | "Body and ancestor placement remain separate authorities" — three cases: an ancestor-connector message present on one path and absent on the other; a process-scoped `try_body` first step giving `PROCESS_IR_CAPABILITY_NODE_NOT_ALLOWED_IN_BODY` versus `PROCESS_IR_CAPABILITY_ERROR_SCOPE_UNSUPPORTED`; a Branch leg `steps=[cache_put]` with a call terminal giving `PROCESS_IR_SCHEMA_INVALID_CARDINALITY` versus the dedicated return-path code | **P2** | machine-served schemas/contracts | DC-178-A (inherits DC-175-E) | Standard — anchor: source label P2. Same class and round as INH-L3R3-01. | inherited at `cdd7a3bf8e2e7ae6773f6ec4250844e5c2e8cf8f` | `inherited-open` — terminal disposition recorded at #178 closure; `window-exhausted` is spent and unavailable |
| L2R1-01 | Stage-2 repo Codex review, round 1, run dir `cdx-review.kNJH4n`, base `cdd7a3bf8e2e7ae6773f6ec4250844e5c2e8cf8f`, head `818e0dae78ea729768add36517e5ef01f657f068`, dirty=false, `STATUS: completed` | "[P2] Revalidate raw values before JSON coercion" — when a caller mutates a string field to a JSON-serializable non-string such as `datetime` or `bytes`, `model_dump(mode="json")` converts it to a valid string before the parser sees it; `parse_and_compile_process_ir_v1` rejects the raw value with `PROCESS_IR_SCHEMA_INVALID` while `compile_process_ir_v1` accepts the mutated model | **P2** | runtime behavior | DC-178-B | Standard — anchor: source label P2; no critical class or anchor. | `818e0da` -> this batch | `fixed` for the `datetime` half. The `bytes` half is **refuted on measurement**: `parse_process_ir_v1` ACCEPTS a raw `bytes` value (pydantic lax-coerces `bytes` to `str`), so the claim "parse_and_compile rejects the raw value" is false for `bytes` and the two entry points never disagreed there. Implementing the finding as written produced QA-178-r2-01. |
| QA-178-r2-01 | Stage-1 QA loop, round 2, live through the public MCP tool boundary, `818e0da` + fix delta, freeze stamp `818e0dae78ea/dirty:4db27b9d66ac/code=c4bc8a19804f`; evidence `docs/architecture/evidence/issue-178/qa-round-2.md` | "entry-point parity only partially restored — the `bytes` case and the production call site still diverge" — after the round-1 fix `compile_process_ir_v1` REFUSES a raw `bytes` value while `parse_and_compile` ACCEPTS it, where at `818e0da` both accepted; and `workflow.py` still dumped `mode="json", warnings=False` before compiling, so that call site accepted a mutated model the compile entry refused | **Medium** | runtime behavior | DC-178-B | Standard — anchor: source label Medium; no critical class or anchor. | this batch | `fixed` — the compile entry now mirrors the parser against the RAW state instead of strict-validating (a rule stricter than its own authority is the DC-175-E mechanism reintroduced), and the model->payload conversion moved into one place, `compile_process_ir_model_v1`, so no call site picks its own dump mode. |
| QA-178-r2-02 | Stage-1 QA loop, round 2, same run | "`canonical_process_ir_json` interpolates the authored value into a serializer warning" — it dumps with pydantic's default `warnings=True`, so a `bytes` canary in `message.text` emits the planted secret verbatim in the warning text; measured IDENTICAL at `818e0da`, so pre-existing and not introduced by this slice | **Low** | *(audit/serialization hygiene — NOT one of the eight blocking classes; the warning is not a served output)* | DC-178-C | Standard — anchor: source label Low; no critical class or anchor. | pre-existing at `cdd7a3b`, fixed in this batch | `fixed` — plus a sibling sweep: `recipes/composer.py` x2 and `authoring/workflow.py:1750` carried the same unhardened dump and are hardened with it. Zero unhardened ProcessIR dump sites remain in `src/`. |
| QA-178-r2-03 | Stage-1 QA loop, round 2, same run | "the new test's docstring claim is false for the `bytes` half" — it states the same raw value through `parse_and_compile_process_ir_v1` is refused; measured `datetime` refused, `bytes` ACCEPTED, and the docstring's own next paragraph concedes the cause | **Low** | *(prose in a test docstring — not served to callers)* | DC-178-B | Standard — anchor: source label Low. | this batch | `fixed` — the prose was rewritten and the `bytes` case became its own test asserting the compile entry must NOT be stricter than the parser. |

Dispositions: `fixed` · `finding-refuted` · `severity-refuted` · `not-validated` · `deferred`
(issue, reason class, placement). `inherited-open` is used only for a seeded `INH-*` row before
this slice has measured it, and is revised to a terminal disposition on a dated line with the
original retained. A refutation names the disputed claim and the concrete code, test or runtime
evidence; a documented reproduction attempt counts, a bare "could not validate" does not. An
original label is never edited.

## Checkpoints (a row is written IN FLIGHT at every third evaluation of each loop — 3, 6, 9, … —
## in the batch it governs, never reconstructed at close)

| Loop | Evaluation (window / cumulative) | SHA (+dirty) | Outcome | Rationale |
| --- | --- | --- | --- | --- |

Each rationale records: per-tier counts and breadth, new/resolved/recurring defect classes derived
from the rows, the trend vector, explicit rule-outs of the other outcomes, and a NAMED finite next
correction. The decision comes after the owed validation and before the next mutation.

## Deferrals

Pointer-only — reason class, placement and lineage live on the finding row and in the filed issue.
RULE: a deferral's issue body quotes the commit SHA of the already-committed checkpoint row it
cites; the issue body is never the first place the deferral exists. `window-exhausted` is
single-use per finding. The two seeded `INH-*` rows have already spent theirs at #175.

## Evidence index

Collected run directories and QA reports are archived, byte-verified, under
`docs/architecture/evidence/issue-178/` with `index.jsonl` + `SHA256SUMS`, in the batch that
collects them — `/tmp` is session-lifetime, and
`test_audit_ledger_attestations_have_durable_matching_evidence` re-verifies every archived
attestation per collector type. An attestation this file claims and the archive cannot back is a
fabricated row. The archive skeleton is created in the same Stage-1.5 commit as this file. Run
citations use the COMPLETE run-dir name (`cdx-review.<suffix>` / `cdx-gate-review.<suffix>`,
backticked); a bare or shortened suffix is a scanner failure.

## Baseline characterization of the regression corpus (measured before any source edit)

The five divergences #178 inherits are characterized at the step-0 baseline, read-only, and the
measured strings archived, BEFORE the fix is applied — a characterization taken after the change is
not a characterization. Two of the five were reviewer-reported at #175 and never independently
reproduced there. A row that does not reproduce is recorded with its measurement and retained as an
equality regression; it is never deleted, skipped, or quietly dropped from the corpus.

| Stable case ID | Reproduced at baseline | Measured parser triple | Measured compiler triple |
| --- | --- | --- | --- |
| `branch-cache-prefix-process-call-terminal` | **CONFIRMED** — all three fields diverge | `PROCESS_IR_SCHEMA_INVALID_CARDINALITY` at `/body/steps/0/legs/0` | `PROCESS_IR_CAPABILITY_PROCESS_CALL_RETURN_PATH_BINDING_UNSUPPORTED` at `/body/steps/0/legs/0/terminal` |
| `root-branch-then-process-call` | **CONFIRMED** | `PROCESS_IR_SEMANTIC_CONTROL_CONTINUATION_UNSUPPORTED` at `/body` | `PROCESS_IR_SEMANTIC_AMBIGUOUS_FLOW` at `/body/steps/0` (`validate_body_capabilities` yields silently) |
| `root-process-call-then-source` | **CONFIRMED — reproduced independently for the first time**; #175 carried it as reviewer-reported and could not construct it. The `source` endpoint needs BOTH `connection_ref` and `operation_ref`, which is what defeated the earlier probe. | `PROCESS_IR_SCHEMA_INVALID_CARDINALITY` at `/body` | `PROCESS_IR_CAPABILITY_UNSUPPORTED` at `/body` |
| `process-try-process-call-first-step` | **CONFIRMED** — code and pointer DEPTH both differ | `PROCESS_IR_CAPABILITY_NODE_NOT_ALLOWED_IN_BODY` at `/body/steps/0/try_body/steps/0` | `PROCESS_IR_CAPABILITY_ERROR_SCOPE_UNSUPPORTED` at `/body/steps/0/try_body/steps` |
| `root-connector-branch-process-call-terminal` | **CONFIRMED, both readings** — the #175 wording does not say whether the leg carries prefix steps, and the two readings are different defects, so both are pinned rather than collapsed | no-prefix: `PROCESS_IR_CAPABILITY_NODE_NOT_ALLOWED_IN_BODY` at `/body/steps/1/legs/0/terminal`, message carries *" — a connector runs upstream of this body"* | no-prefix: same code and pointer, message omits the clause. Prefix reading (`leg0.steps=[set_dpp]`) is a CODE divergence, not a message one |

Full measured strings, mechanisms and controls: `docs/architecture/evidence/issue-178/baseline-corpus-characterization.md`.

### Acceptance criterion 4's premise is REFUTED by measurement (recorded, not absorbed)

#178's body asserts the safety property — *"no document is accepted by one entry point and refused
by the other"* — **already holds** and merely needs a test. It does not hold. Measured at the
baseline, the compiler ACCEPTS and fully COMPILES documents the parser REFUSES: a Branch leg with
`steps=[cache_put]`/`terminal=stop` (compiles to a real emission plan); root
`[set_dpp, source, target, stop]` with `source` out of position (compiles to a 4-node CFG); a
Branch with ONE leg (crashes to `PROCESS_IR_COMPILE_INTERNAL` with an empty path); and any document
whose `version` has been mutated — no compiler stage reads `version` at all.

So the compile-entry re-parse is not only a diagnostic-identity change: it closes a real
accept-direction hole and converts silent mis-compiles into refusals. The property this slice can
pin is **grammar acceptance at the compile entry**, not full-compile acceptance — the parser is not
a superset either (41 of 154 census documents are refused by the compiler and accepted by the
parser, because compiler-only codes need a symbol table the parser lacks). The safety test is
therefore written to FAIL at the baseline and PASS after the fix, which is the strongest available
witness that it is not vacuous.

### Deliberate diagnostic-quality costs of the parser-authority decision

- `process-try-process-call-first-step` (row 4): the specific process-scope message is replaced by
  the generic slot message, while the pointer gains the step index. Recorded as an accepted cost.
- `PROCESS_IR_SEMANTIC_CATCH_UNTERMINATED` degrading to `PROCESS_IR_SCHEMA_INVALID` on a
  mutated-away catch terminal is **NOT** accepted — it is a strict loss on served contract text and
  is FIXED in this slice (the parser's dedicated branch matches only the `missing` pydantic error
  form; a dump renders `"terminal": null` with the key present, which reports
  `model_attributes_type`). Widening that branch is part of the production change.

## Stage-1 QA (loop 1)

| Evaluation | Tree | Verdict | Evidence |
| --- | --- | --- | --- |
| 1 | `cdd7a3bf8e2e7ae6773f6ec4250844e5c2e8cf8f` + uncommitted tree, freeze stamp `cdd7a3bf8e2e/dirty:2bbf52bd8fe0/code=65f521bce7b4` | **CLEAN — zero findings** | `docs/architecture/evidence/issue-178/qa-round-1.md` |
| 2 | `818e0dae78ea729768add36517e5ef01f657f068` + fix delta, freeze stamps `818e0dae78ea/dirty:4db27b9d66ac/code=c4bc8a19804f` and `026385aa589b` | regression-clean; **3 findings**, all `fixed` (`QA-178-r2-01/02/03`) | `docs/architecture/evidence/issue-178/qa-round-2.md` |

Suite spent once at HEAD: **10209 passed, 17 skipped, 0 failed** — *measured here*. Account and repo
tree byte-identical after the round; every provisioned component deleted.

**What this round can and cannot prove — recorded so the clean verdict is not over-read.** QA
established by measurement that most of the parity change is NOT observable at the MCP tool
boundary: scenarios A and C are refused at the `AuthoringRequestV1` INTAKE parse, and a full
leaf-diff of all 22 served envelopes baseline-versus-HEAD shows zero behavioural difference beyond
expected source-digest drift. That is the correct characterization rather than a defect — #178's
hole is reached by mutating an exported `ProcessIRV1` and handing it to the compiler DIRECTLY, and
the tool boundary already re-parses at intake (`authoring/workflow.py::_reparsed_unit`), so no
tool-boundary caller could ever reach the divergence. Live QA therefore proves no regression plus
one improvement; the parity property itself is proven by the derived gate in
`tests/test_process_ir_entrypoint_diagnostic_parity.py`, at the direct compiler API where the defect
lives. QA additionally measured that four public arms emitting real process XML never enter
`compile_process_ir_v1` at all — verified on BOTH trees, so it is a pre-existing repo fact.

**Boundary-visible improvement, confirmed live:** an explicitly `null` `catch_body.terminal` moved
from `PROCESS_IR_SCHEMA_INVALID` to `PROCESS_IR_SEMANTIC_CATCH_UNTERMINATED`, carrying its message,
remediation and `authoring_contract_entry_ids`. The wrong-TYPE discriminator holds live:
`terminal: 42`, `terminal: "stop"` and `catch_body: 42` all keep `PROCESS_IR_SCHEMA_INVALID`.

**Pre-existing observation, no disposition here:** `cause_codes[0]` can serve the string
`"ProcessIRCompileError"` when that error escapes the `process_materialization.py` call site, which
has no local handler. QA proved it pre-existing by seeding the identical line in both trees and
obtaining identical envelopes. Unchanged by this slice; recorded, not filed, per the standing rule
that an accepted pre-existing limitation is not minted as debt.

## Stage-2 repo Codex review (loop 2)

| Evaluation | Run dir | Base -> head | Result | Archived |
| --- | --- | --- | --- | --- |
| 1 | `cdx-review.kNJH4n` | `cdd7a3bf8e2e7ae6773f6ec4250844e5c2e8cf8f` -> `818e0dae78ea729768add36517e5ef01f657f068`, dirty=false | `STATUS: completed`, one **P2** (`L2R1-01`) | `docs/architecture/evidence/issue-178/commit-reviews/cdx-review.kNJH4n/`, teardown `confirmed stopped` |

## Structural-fix record (second-instance trigger, DC-178-B)

**DC-178-B** — *a coercive projection stands in for the raw state the authority must judge*.
Runtime authority: `parse_process_ir_v1`. Instances: `L2R1-01` (Stage-2 round 1) and
`QA-178-r2-01` (Stage-1 QA round 2). The second instance triggered the structural fix in the same
batch, per the rule:

- **The invariant**, derived from the authority rather than enumerated: the model-to-payload
  conversion happens in exactly ONE place, `compile_process_ir_model_v1`, and the compile entry
  judges the RAW state through `parse_process_ir_v1` itself. It never applies a rule of its own —
  the withdrawn `strict=True` revision is the recorded proof that exceeding the authority
  reintroduces the very class being closed.
- **Sibling sweep** (every `model_dump` of a ProcessIR model in `src/`, enumerated, not sampled):
  `pipeline.py` (the authority, `warnings="error"`), `models/process_ir.py::raw_process_ir_payload`
  and `::canonical_process_ir_json`, `authoring/workflow.py:379` and `:1750`,
  `recipes/composer.py` x2, `semantic_validation/context.py` x2. Three were unhardened and are
  fixed; zero unhardened sites remain. `authoring/workflow.py:379` keeps its dump with an explicit
  justification: it re-parses for server OWNERSHIP at intake and sits upstream of the compile
  entry, which is now the single authority for the same property.
- **Non-vacuity witnesses**: `test_the_gate_fails_when_the_structural_fix_is_removed` (neutering
  the re-parse produces 422 mismatches of 820; deleting the line from the real source fails 11 of
  19 tests) and `test_a_mutated_value_never_reaches_a_serializer_warning`, whose control asserts
  the UNHARDENED dump still leaks, so the guard cannot silently stop detecting regressions.
- **Coverage claim, derived from the authority's own case set**: every `(context, slot, kind)`
  cell of `BODY_CAPABILITIES_V1` crossed with the parser's closed node vocabulary and both ancestor
  modes, plus the root singleton and ordered-pair product the matrix has no row for.

**DC-178-C** — *a ProcessIR model dumped under pydantic's default `warnings=True`, interpolating
the authored value into a serializer warning*. Runtime authority: the AR2-01 rule. One instance
(`QA-178-r2-02`, pre-existing) plus three siblings found by the same sweep and fixed with it.

## Final-tree validation (filled at close; every roster gate current on the FINAL sha)

| Gate | Evidence (quoted output / run URL / archived round) | SHA |
| --- | --- | --- |

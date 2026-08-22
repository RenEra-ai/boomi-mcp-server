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
| L2R2-01 | Stage-2 repo Codex review, round 2, run dir `cdx-review.3Vo4vh`, base `818e0dae78ea729768add36517e5ef01f657f068`, head `1618f99e48ee5adbf4488970b660a0f273bd4c1b`, dirty=false, `STATUS: completed` | "[P2] Avoid exhausting one-shot fields before raw reparsing" — when a mutable list field contains a one-shot iterable, the json dump consumes the generator before raising its serialization warning; the fallback then reparses the already-exhausted iterator and reports an empty `/body/steps`, although `parse_process_ir_v1` accepts an equivalent raw generator and the previous one-pass implementation compiled it | **P2** | runtime behavior | DC-178-B | Standard — anchor: source label P2; no critical class or anchor. | `1618f99` -> this batch | `fixed` — CONFIRMED by measurement (compiled at `818e0da` with 3 steps, refused at `1618f99`, and the served cardinality diagnostic described the probe's damage rather than the document). Fixed by DELETING the two-pass design: the entry now performs exactly one `model_dump(mode="python", warnings=False)` and parses it, so no JSON projection of a caller model is built anywhere and the `warnings="error"` machinery is gone. The round-2 fix is strictly smaller than the round-1 fix it replaces. |
| QA-178-r3-01 | Stage-1 QA loop, round 3, live + direct API, `1618f99` + delta, freeze stamps `3ef7c5a828ef` / `5702803c4b60`; evidence `docs/architecture/evidence/issue-178/qa-round-3.md` | "the compile entry is still a DESTRUCTIVE read, and the apply path compiles one model three times" — compiling the same model object twice gives ACCEPTED then REFUSED as the caller's `body.steps` drains to 0, and a live typed apply was measured handing ONE model object to the public entry three times | **Low** | *(not reachable today — every production model comes from a parse and holds materialised containers)* | DC-178-B | Standard — anchor: source label Low; no critical class or anchor. | this batch | `fixed` as a documented CONTRACT rather than a code change, because it cannot be sequenced away: the identical drain was measured on the pre-#178 dump shape, so it is a property of re-parsing at all, and Python offers no way to read a one-shot iterable twice. The public entry now states the read-exactly-once precondition, two tests pin it (including that the returned model IS the escape hatch and compiles repeatedly), and the apply path is safe because its models are parsed. |
| QA-178-r3-02 | Stage-1 QA loop, round 3, same run | "the baseline attestation was inexact — three corrections reached a committed baseline without their own QA round" — the round-3 dispatch called `1618f99` "the tree you tested in round 2", but it also carries `authoring/workflow.py`, `recipes/composer.py` and the `canonical_process_ir_json` change, all made in response to round 2 and none present in the tree round 2 tested | **Low** | *(audit-record integrity — NOT one of the eight blocking classes)* | DC-178-D | Standard — anchor: source label Low. | this batch | `fixed` — the claim was mine and it was wrong; `git diff 818e0da 1618f99 -- src/` confirms the three files. The QA loop table above is corrected, an *Attestation correction* section records what actually happened, and the practice of diffing the named baseline against the previous one BEFORE attesting is adopted for the rest of the slice. No harm resulted: round 3 validated all three corrections. |
| L2R3-01 | Stage-2 repo Codex review, round 3, run dir `cdx-review.Laubc9`, base `1618f99e48ee5adbf4488970b660a0f273bd4c1b`, head `3cb564d7e5bd7dee9beb1ccde7152915867095d2`, dirty=false, `STATUS: completed` | "[P2] Add the mandatory third-evaluation checkpoint" — once the ledger records Stage-1 QA evaluation 3, the Checkpoints section requires an in-flight checkpoint decision before the next mutation; the checkpoint table is still empty even though the patch applies the two round-3 dispositions, so the audit cannot establish a legal CLOSE/CONTINUE/DEFER outcome | **P2** | *(audit-record integrity — NOT one of the eight blocking classes)* | DC-178-D | Standard — anchor: source label P2; no critical class or anchor. | `3cb564d` -> this batch | `fixed` — CONFIRMED: both loops had reached three evaluations with an empty Checkpoints table, and the workflow requires the row in flight rather than reconstructed at close. Two checkpoint rows are recorded below, each with per-tier counts, breadth, class movement, the trend vector across all four axes, explicit rule-outs and a named finite next correction. |
| L2R3-02 | Stage-2 repo Codex review, round 3, same run dir `cdx-review.Laubc9` | "[P2] Include round 2 in the Stage-2 evaluation history" — the newly archived completed run is Stage-2 evaluation 2, but the Stage-2 review table still lists only evaluation 1; because checkpoint accounting uses the cumulative evaluation number, the omission can cause the next review to be treated as round 2 and skip the mandatory round-3 checkpoint | **P2** | *(audit-record integrity — NOT one of the eight blocking classes)* | DC-178-D | Standard — anchor: source label P2. | `3cb564d` -> this batch | `fixed` — CONFIRMED: round 2's run was archived but never added to the evaluation table. Rounds 2 and 3 are now both listed, so the cumulative count the checkpoint rule keys on is correct. |
| L2R4-01 | Stage-2 repo Codex review, round 4 (fix-only over the record batch), run dir `cdx-review.BmT1ps`, base `3cb564d7e5bd7dee9beb1ccde7152915867095d2`, head `93ac80e9055e2d4caa3e9e814234aabe5095faaf`, dirty=false, `STATUS: completed` | "[P2] Record checkpoint state before applying the correction batch" — the checkpoints do not describe the required pre-mutation state: the Stage-1 row is added after `3cb564d` already committed the round-3 corrections and therefore retrospectively says nothing is owed, while the Stage-2 row marks `L2R3-01/02` fixed even though their fixes are this post-`3cb564d` diff | **P2** | *(audit-record integrity — NOT one of the eight blocking classes)* | DC-178-D | Standard — anchor: source label P2; no critical class or anchor. | `93ac80e` -> this batch | `fixed` — CONFIRMED and the criticism is exact: describing the POST-correction state makes any checkpoint decision look trivially safe, which is the opposite of what the record is for. Both rows now state the state AT THE DECISION POINT, count the then-unresolved findings, and disclose that one was recorded late and one mid-correction — which the workflow permits only when recorded as such. |
| L2R4-02 | Stage-2 repo Codex review, round 4, same run dir `cdx-review.BmT1ps` | "[P2] Add the required structural disposition for DC-178-D" — `QA-178-r3-02` already belongs to DC-178-D and both `L2R3` findings are assigned to the same class, so the second-instance rule has triggered; the checkpoint nevertheless claims no repeated class was instance-patched, while the structural-fix section covers only DC-178-B/C and no permitted deviation is recorded | **P2** | *(audit-record integrity — NOT one of the eight blocking classes)* | DC-178-D | Standard — anchor: source label P2. | `93ac80e` -> this batch | `fixed` — CONFIRMED: three rows carry DC-178-D and the rule had fired unacknowledged. The structural fix is recorded below and is executable rather than prose: `tests/test_issue_178_ledger_is_derived_from_its_archive.py` derives the review history and the checkpoint obligation from `index.jsonl`. Both mutants were hand-run before the claim was written. |
| L2R5-01 | Stage-2 repo Codex review, round 5 (fix-only over the round-4 record batch), run dir `cdx-review.yTxPi2`, base `93ac80e9055e2d4caa3e9e814234aabe5095faaf`, head `e59245394825d198184d11c826cb3f47a4461cc5`, dirty=false, `STATUS: completed` | "[P2] Restrict checkpoint counts to the Stage-2 loop" — a downstream or wave correction's repo review uses the same `commit-review-collect` collector but belongs to a different `logical_loop`, and the helper counted it as a Stage-2 evaluation, so once the combined count crossed another multiple of three the guard would demand a spurious L2 checkpoint | **P2** | *(audit-record integrity — NOT one of the eight blocking classes)* | DC-178-D | Standard — anchor: source label P2; no critical class or anchor. | `e592453` -> this batch | `fixed` — CONFIRMED: the helper filtered on collector and status but not on `logical_loop`, which every archived row already carries. It now filters on it, and a second assertion refuses a table row the loop does not own, so the count cannot be inflated from either direction. |
| L2R5-02 | Stage-2 repo Codex review, round 5, same run dir `cdx-review.yTxPi2` | "[P2] Require each run in the Stage-2 history table" — if an archived round is omitted from the Stage-2 evaluation table but remains mentioned elsewhere, such as in its finding row, the whole-file substring check still passes, permitting the exact `L2R3-02` accounting defect to recur | **P2** | *(audit-record integrity — NOT one of the eight blocking classes)* | DC-178-D | Standard — anchor: source label P2. | `e592453` -> this batch | `fixed` — CONFIRMED BY MEASUREMENT, and the guard was failing at exactly the thing it was written to prevent: deleting round 2's evaluation-table row while leaving its finding-row citation intact left all six assertions GREEN. The check now parses the evaluation TABLE rather than the file, and the same mutant hand-run afterwards fails two assertions. |
| L2R5-03 | Stage-2 repo Codex review, round 5, same run dir `cdx-review.yTxPi2` | "[P2] Cover the QA-baseline defect in the structural guard" — the declared authority contains only collected commit-review rounds and no QA baseline or source-diff information, so if a Stage-1 evaluation again describes the wrong tested tree every new invariant still passes; `QA-178-r3-02` is not structurally prevented despite being claimed as an instance covered by this fix | **P2** | *(audit-record integrity — NOT one of the eight blocking classes)* | DC-178-D -> revised to **DC-178-E** for `QA-178-r3-02` (original class retained on that row; revision recorded here, dated 2026-08-21, second-instance check re-run before this batch) | Standard — anchor: source label P2. | `e592453` -> this batch | `fixed` by WITHDRAWING the false coverage claim rather than rewording it. The invariant genuinely does not cover that instance, so the instance is reclassified: DC-178-E's authority is `git diff`, not the archive, and its disposition is a recorded deviation on the ground that the failing claim lived in a dispatch message, which is not a tree artifact any in-tree test can assert against. |
| L2R6-01 | Stage-2 repo Codex review, round 6 (fix-only over the round-5 record batch), run dir `cdx-review.Lfsbye`, base `e59245394825d198184d11c826cb3f47a4461cc5`, head `f2937f4aa273486e3f07e6e396134494c009d48c`, dirty=false, `STATUS: completed` | "[P2] Scope the parser to the Stage-2 table" — when the already-rostered §6 or wave gate is later recorded in a numbered table, a row such as `| 1 | cdx-gate-review... |` matches the whole-file scan because the regex accepts gate-review names, and since the archive helper holds only L2 commit reviews the ownership test would reject valid downstream history | **P2** | *(audit-record integrity — NOT one of the eight blocking classes)* | DC-178-D | Standard — anchor: source label P2; no critical class or anchor. | `f2937f4` -> this batch | `fixed`, and NOT hypothetical: §6 is the next gate on this slice's own roster, so the guard would have failed on a correctly-kept record within the hour. Parsing is now bounded to the `## Stage-2 repo Codex review (loop 2)` section and stops at the next top-level heading, and accepts `cdx-review.` only. Hand-run: appending a §6 table of `cdx-gate-review.*` rows leaves the ownership assertion green. |
| L2R6-02 | Stage-2 repo Codex review, round 6, same run dir `cdx-review.Lfsbye` | "[P2] Reject duplicate Stage-2 history rows" — if an archived Stage-2 run is listed twice in the evaluation table both entries remain members of the owned SET, so the check passes even though the table's evaluation count is padded | **P2** | *(audit-record integrity — NOT one of the eight blocking classes)* | DC-178-D | Standard — anchor: source label P2. | `f2937f4` -> this batch | `fixed` — the comparison is now a multiset (`Counter`) equality in both directions, so a duplicated row and an omitted row are both detected. A padded count corrupts the checkpoint accounting exactly as an omission does, pointing the other way. Hand-run: duplicating round 5's row fails the assertion. |
| L2R7-01 | Stage-2 repo Codex review, round 7 (terminal validation of the round-6 batch), run dir `cdx-review.wHRVuF`, base `f2937f4aa273486e3f07e6e396134494c009d48c`, head `4b39bb051e53af9fa539f0682b56211088bc5557`, dirty=false, `STATUS: completed` | "[P2] Reject foreign rows inside the Stage-2 section" — an accidental `| 7 | cdx-gate-review... |` row inside the Stage-2 section is SKIPPED by the regex rather than treated as unowned, and because the whole-file citation check accepts any legitimately archived gate run, every test stays green while the table is padded | **P2** | *(audit-record integrity — NOT one of the eight blocking classes)* | DC-178-D | Standard — anchor: source label P2; no critical class or anchor. | `4b39bb0` | **`deferred` — recorded deviation, NOT fixed.** The claim is technically CORRECT and is accepted as accurate; the disposition is a deliberate stop, not a refutation. Grounds: it is a robustness gap in the guard against a hypothetical mis-keyed row, contains no false claim, and arose from the validation of a non-blocking batch — which the workflow says "never earns a second batch". Reason class `out-of-scope-by-design`; no follow-up issue is filed, per the standing rule that an accepted limitation is recorded rather than minted as debt. Discriminating evidence a second instance must supply: an actual padded Stage-2 table reaching a commit, which would make this `window-exhausted` rather than by-design. |
| L2R7-02 | Stage-2 repo Codex review, round 7, same run dir `cdx-review.wHRVuF` | "[P3] Correct the fixed-finding count at checkpoint six" — rounds 1–5 contain nine fixed Stage-2 findings (`1 + 1 + 2 + 2 + 3`), not six, making the checkpoint's per-tier accounting inconsistent with the finding rows and capable of misstating the trend used for the continuation decision | **P3** | *(audit-record integrity — NOT one of the eight blocking classes)* | DC-178-D | Standard — anchor: source label P3; no critical class or anchor. | `4b39bb0` -> this correction | `fixed` — CONFIRMED by deriving the count from the rows rather than trusting either side: eleven `L2R*` rows exist, nine of them predate round 6. The hand-typed "six" was wrong and is corrected. This is the DC-178-D mechanism once more — a number the rows already knew — and it is corrected as a matter of record TRUTH rather than as a further improvement batch: a checkpoint whose per-tier accounting contradicts its own rows cannot support a closure decision. |
| L3R1-01 | §6 architect implementation review, round 1, run dir `cdx-gate-review.CX1MhW`, base `cdd7a3bf8e2e7ae6773f6ec4250844e5c2e8cf8f`, head `aedca45344ea9215f77deede343f6724fa264a09`, `parsedVerdict: ISSUES FOUND`, attested against the plan bytes | "STANDARD — Acceptance criterion 2 is not implemented as designed" — the generator crosses matrix row x ancestor mode x candidate kind but never crosses STEP candidates with every legal terminal or TERMINAL candidates with every legal/empty prefix; the count assertion recomputes this REDUCED formula (400 body + 420 root versus the plan-derived 3,000 + 420), and it overwrites required Try-body anchors | **STANDARD** | machine-served schemas/contracts | DC-178-F | Standard — anchor: source label STANDARD; no critical class or anchor. | `aedca45` -> this batch | `fixed` — CONFIRMED by measurement, and the criticism is exact on both halves. The product is now the plan's: **3,420 cases (3,000 body + 420 root)**, and the count assertion recomputes the PLAN's formula from `_neighbours()` rather than its own. The neighbour dimension is load-bearing, not decorative — corpus row 1 IS a (prefix `cache_put`, terminal `process_call`) INTERACTION that no single-slot product can generate, so the gate was pinning by hand a divergence it could never derive. Try-body anchors are now preserved around the candidate; measured effect: the parser-accepted partition grew 126 -> 682 and try-body STEP cases reach ACCEPTED instead of collapsing onto the scope diagnostic. Mismatches remain zero. |
| L3R1-02 | §6 architect implementation review, round 1, same run dir `cdx-gate-review.CX1MhW` | "STANDARD — Unexpected dump failures receive the wrong diagnostic" — `raw_process_ir_payload` converted every `model_dump` exception into `PROCESS_IR_SCHEMA_INVALID` and the compile entry then translated it as an ordinary parser refusal, where the plan requires unexpected dump/parser failures to become `PROCESS_IR_COMPILE_INTERNAL`, phase `schema` | **STANDARD** | runtime behavior | DC-178-G | Standard — anchor: source label STANDARD; no critical class or anchor. | `aedca45` -> this batch | `fixed` — a dump failure is not a document defect, and serving it as one tells a caller their document is wrong when the truth is that the serializer broke. The models helper no longer classifies it; the compile entry wraps the call and raises the value-free `PROCESS_IR_COMPILE_INTERNAL`. Verified with a forced failure: code `PROCESS_IR_COMPILE_INTERNAL`, empty path, and the exception text does NOT reach the served message. |
| L3R1-03 | §6 architect implementation review, round 1, same run dir `cdx-gate-review.CX1MhW` | "STANDARD — The mandated policy-bearing negative regression is missing" — no single behavioural test proves that a policy-required specimen fails strict semantics, succeeds with policy after reparse, and remains refused after a grammar-invalid mutation; a future policy-conditional reparse bypass could evade the present tests | **STANDARD** | runtime behavior | DC-178-F | Standard — anchor: source label STANDARD. | `aedca45` -> this batch | `fixed` — the plan mandated this test (T5a) and it was simply never written; the code-level disjointness assertion proves neither half behaviourally. `test_a_policy_bearing_compile_survives_the_reparse_but_gains_no_grammar_bypass` now covers all four parts against a real specimen that reaches emission planning, so part (ii) is a COMPLETED compile rather than "a different failure" — connector metadata was added to the fixture instead of weakening the assertion. |
| L3R1-04 | §6 architect implementation review, round 1, same run dir `cdx-gate-review.CX1MhW` | "STANDARD — The five-case corpus does not hard-pin the archived baseline triples" — the corpus stored only code and path, obtained the message from the CURRENT parser and compared compile against that live value, so simultaneous parser/compiler message drift passes | **STANDARD** | machine-served schemas/contracts | DC-178-F | Standard — anchor: source label STANDARD. | `aedca45` -> this batch | `fixed` — and the failure mode is precisely the one #178 exists to prevent: two paths drifting IN STEP is what "identical diagnostics" looks like from the inside, so a live-derived expectation cannot detect it. All six corpus rows now hard-pin the message measured at `cdd7a3b` and archived in `baseline-corpus-characterization.md`. |
| L3R1-05 | §6 architect implementation review, round 1, same run dir `cdx-gate-review.CX1MhW` | "LOW — The planned pytest-manifest reconciliation is absent" — `tests/fixtures/wave_gate/test_nodes.jsonl` is unchanged from baseline, retains the `10198` floors and contains none of the new #178 test nodes; `pytest-010233` was correctly retained | **LOW** | *(manifest floors — reconciled at the wave gate by design)* | DC-178-D | Standard — anchor: source label LOW; no critical class or anchor. | `aedca45` | `deferred` within this slice to the composite wave gate, per the implementation plan's own ordering: the manifest is the LAST content edit because a manifest reconciled before the test set is final is stale on arrival — recorded three times in #175 as DC-175-D. Reason class `out-of-scope-by-design` for this batch only; it is NOT deferred out of the slice and the wave gate cannot pass without it. The reviewer's observation that `pytest-010233` was retained confirms the manifest-identity constraint held. |
| L2R8-01 | Stage-2 repo Codex review, round 8 (support review for the §6 loop's correction), run dir `cdx-review.wr3vzT`, base `aedca45344ea9215f77deede343f6724fa264a09`, head `fc3cb0e5cfe1c7ea3191df7c3cb93a2696ef0fec`, dirty=false, `STATUS: completed` | "[P2] Classify validation-shaped dump failures as internal" — when a caller supplies a `ProcessIRV1` subclass whose `model_dump` raises `ProcessIRValidationError`, the branch treats the exception as parser-authored even though parsing has not started, so its arbitrary code/message/remediation are served verbatim instead of the promised value-free `PROCESS_IR_COMPILE_INTERNAL` | **P2** | machine-served schemas/contracts | DC-178-G | Standard — anchor: source label P2. **Same defect as `QA-178-r4-01`, which its own gate labeled High; the two rows are ONE distinct defect for class counting, and the CRITICAL tier derived from the higher anchor governs the response.** | `fc3cb0e` -> this batch | `fixed` — the branch was dead for its stated purpose and live as a hazard: `raw_process_ir_payload` no longer raises that type, so anything arriving as it cannot be parser-authored. Every dump exception now takes the internal path. |
| QA-178-r4-01 | Stage-1 QA loop, round 4, direct API + live matrices, attested SHA `fc3cb0e5cfe1c7ea3191df7c3cb93a2696ef0fec`; evidence `docs/architecture/evidence/issue-178/qa-round-4.md` | "the delta's `except ProcessIRValidationError` passthrough lets a caller forge a compiler diagnostic — planted secret served intact" — a `ProcessIRV1` subclass whose `model_dump` raises a parser-typed error injects its own code, pointer, message and remediation into the compiler's authoritative error channel, measured serving a CALLER-CHOSEN code at the caller's own pointer `/QA178/forged/pointer`, with the planted canary `QA178INTERNALCANARY-sk_live_b0rk3d` intact in both message and remediation. The forged code literal is deliberately NOT reproduced here: a ledger may not name a diagnostic the gate cannot emit, and the scanner caught this row doing exactly that. The verbatim capture is in `docs/architecture/evidence/issue-178/qa-round-4.md` | **High** | machine-served schemas/contracts (secondary: secrets — the AR2-01 value-free contract) | DC-178-G | **Critical — anchor: the source gate labeled it High, and the tier rules derive Critical from a P0/P1/Critical/High source label. Not deferrable, not closable over.** | `fc3cb0e` -> fixed in this batch | `fixed`. QA's three-tree A/B establishes it as a regression INTRODUCED by the §6 round-1 delta: at `3cb564d` the helper normalised every exception, which incidentally closed it. QA offered the retier explicitly (*"the raw label is yours to set"*) and I am taking the STRICTER reading rather than the reviewer's P2, because caller-controlled text entering a served, logged diagnostic channel is what the value-free contract exists to prevent. Validation owed as Critical: a QA round covering the branch removal end-to-end plus a delta review, both run before closure. |
| QA-178-r4-02 | Stage-1 QA loop, round 4, same run | "the worktree was edited while this round was reading it" — `pipeline.py`, the file under test, gained uncommitted edits mid-round; the `code=` freeze hashes let QA prove which results survived, but had the edit landed seconds earlier it would have voided the live matrices and cost ~12 minutes of live account work, the same class as the #153 r4/r5 losses | **Low** | *(run integrity — NOT one of the eight blocking classes)* | DC-178-H | Standard — anchor: source label Low. | this batch | `fixed` as a practice, and the fault is MINE: I applied the branch-removal fix while QA was mid-round, which is the exact hazard this repo already recorded as *never write the worktree while a gate reads it*. Knowing the rule did not prevent it, so the correction is procedural rather than a note: while any gate holds the tree, corrections are staged in the scratchpad and applied only after the gate reports. QA's second point is also owed — the branch removal is a further behavioural change to the same function that no QA round has covered end-to-end, so it takes its own round before closure. |
| QA-178-r5-01 | Stage-1 QA loop, round 5, self-reported correction to round 4's own evidence; `docs/architecture/evidence/issue-178/qa-round-5.md` | "correction to my own round-4 evidence" — round 4 argued the E-matrix survived the mid-round edit because its `files=133 code=…` matched a clean-tree run, but `pipeline.py` is imported LAZILY and is therefore not in the 133-file pinned set; the `code=` hash is byte-identical across `fc3cb0e` and `77ed08a` even though `pipeline.py` differs, so it proved the eagerly-loaded files matched and said nothing about the file under test | **Low** | *(QA evidence integrity — NOT one of the eight blocking classes)* | DC-178-H | Standard — anchor: source label Low. | round-4 evidence | `finding-refuted` as to the CONCLUSION, `fixed` as to the reasoning: QA re-established the round-4 result on sound grounds (that E-matrix was byte-identical to round 3's and none of its arms reach the changed code) and repaired the instrument. Recorded because I ACTED on the round-4 report, and a load-bearing line of its reasoning turned out to be unsound — QA raising this against itself, unprompted, is the reason the round-4 conclusion can still be relied on. |
| L3R2-01 | §6 architect implementation review, round 2, run dir `cdx-gate-review.FrQAIh`, base `aedca45344ea9215f77deede343f6724fa264a09`, head `ca5d778`, `parsedVerdict: ISSUES FOUND`, attested against the plan bytes | "CRITICAL — forged diagnostics remain possible" — `_parse_payload_for_compile` treats exception type as provenance; a `ProcessIRV1` subclass can RETURN a `dict` subclass whose `items()` hook, invoked by the parser's secret pre-scan, raises a forged `ProcessIRValidationError`, and the translation then preserves attacker-controlled diagnostics verbatim; a malformed validation error can also make translation raise a raw `RuntimeError`, bypassing handlers that catch only `ProcessIRCompileError` | **CRITICAL** | machine-served schemas/contracts (secondary: secrets — the AR2-01 value-free contract) | DC-178-G | **Critical — anchor: source gate labeled it CRITICAL. Not deferrable, not closable over.** | `ca5d778` -> this batch | `fixed`, and REPRODUCED before fixing: a caller-chosen code at `/forged` with planted secret text reached the served diagnostic. My earlier fix closed only the variant where the dump RAISES; this is the identical trust one boundary later, at the parse. The translation now validates every diagnostic against the parser's OWN served code set (`process_ir_v1_parse_diagnostic_specs()`) — a code outside that set was not authored by the parser whatever type carried it — degrades the whole error to the value-free internal refusal rather than trusting it partially, and is itself guarded so a malformed item cannot escape as a bare exception. Both attack variants verified closed; legitimate refusals still travel verbatim. |
| L3R2-02 | §6 architect implementation review, round 2, same run dir `cdx-gate-review.FrQAIh` | "STANDARD — the generated product is correct today, but its coverage guard remains weaker than the plan" — the count assertion reuses `_neighbours`, the same helper generation uses, so reducing that helper recreated the old 820-case product while the count, partitions, denied-cell, parity and safety tests all still passed; the witness pins `step=cache_put/opp=stop` rather than the mandated `terminal=process_call/opp=cache_put` interaction; and catch-all carrier routing lets a new matrix context silently mutate `catch_body` | **STANDARD** | machine-served schemas/contracts | DC-178-F | Standard — anchor: source label STANDARD; no critical class or anchor. | `ca5d778` -> this batch | `fixed`, all three parts. This is the round-1 defect one level up — an expectation that moves with the thing it checks — so the count is now computed from `MATRIX` directly and independence was verified by MUTATION: shrinking `_neighbours` fails 2 tests where it previously failed none. The witness is re-aimed onto corpus row 1's actual shape and asserts the precise divergence (parser cardinality versus the compiler's return-path code) rather than "these differ somehow". Carrier routing and `_body_of` are exact, with unknown contexts failing loudly. |
| L3R2-03 | §6 architect implementation review, round 2, same run dir `cdx-gate-review.FrQAIh` | "LOW — the repo-wide ledger guard contradicts its durable record" — the expansion is a defensible structural sibling sweep and NOT improper scope creep, but the ledger still states the invariant is deliberately not repo-wide and retains superseded counts, so the shipped audit record says the opposite of the correction | **LOW** | *(audit-record integrity — NOT one of the eight blocking classes)* | DC-178-D | Standard — anchor: source label LOW. | `ca5d778` -> this batch | `fixed` — the record now matches what shipped, and the correction is explicit that the original "65 uncited runs in #152" figure was miscounted: it counted gate-review runs from other logical loops rather than Stage-2 rounds, and the scoping conclusion drawn from it was wrong. The reviewer's judgement that the widening is a faithful sibling sweep rather than scope creep is recorded, since I had asked it to test that adversarially. |
| QA-178-r6-01 | Stage-1 QA loop, round 6, direct-API forgery battery at `39e4f6d92d6500438713d6a69377d7e5717021b2`; evidence `docs/architecture/evidence/issue-178/qa-round-6.md` | "the allowlist validates `code` only" — `path`, `message` and `remediation` are type-checked but never compared to the parser's own text, so any of the 15 real codes carries arbitrary attacker text; measured serving `PROCESS_IR_SCHEMA_INVALID` at `/forged` with the canary in message and remediation | **High** | machine-served schemas/contracts (secondary: secrets — AR2-01) | DC-178-G | **Critical — anchor: source gate labeled High.** | `39e4f6d` -> this batch | `fixed` at the ROOT rather than by a fourth output check. QA's own control (`A0`) proves the allowlist is not inert and item 2 proves it does not over-reject — 8/8 real refusals travel verbatim — so the allowlist stays; what changed is that the parse now runs on INERT data, so caller code cannot execute inside `parse_process_ir_v1` and the forged error is never raised. QA also withdrew two of its own candidate vectors (`A3` a `str`-subclass code, `A4` a hostile container) after measuring them as instances of `A1` rather than distinct — two root causes, not four. |
| QA-178-r6-02 | Stage-1 QA loop, round 6, same run | "`_parse_payload_for_compile` has `except ProcessIRCompileError: raise`" — a forged compile error raised from inside the parse bypasses the guarded translation entirely with no allowlist, serving a caller-chosen code and pointer with the canary in message and remediation; the arm is dead for its stated purpose, since nothing under `models/` raises that type and all eight real refusals raise `ProcessIRValidationError` | **High** | machine-served schemas/contracts (secondary: secrets — AR2-01) | DC-178-G | **Critical — anchor: source gate labeled High.** | `39e4f6d` -> this batch | `fixed` by DELETING the arm. It is the same shape as the arm deleted one boundary earlier at `77ed08a`, surviving one boundary over — which is the observation that turned this from a fourth instance patch into the root fix. |
| L2R10-01 | Stage-2 repo Codex review, round 10 (support review for the §6 loop), run dir `cdx-review.5Nd1lG`, base `ca5d778ef11bfe4751e849eb67273f5c4649fe74`, head `39e4f6d92d6500438713d6a69377d7e5717021b2`, dirty=false, `STATUS: completed` | "[P1] Reject forged text for allowlisted parser codes" — when `model_dump` returns a dict subclass whose `items()` raises a forged `ProcessIRValidationError` using a real parser code, the checks pass because they validate only code membership and string types, and the caller-controlled path, message and remediation are copied into the served and logged error | **P1** | machine-served schemas/contracts | DC-178-G | **Critical — anchor: source label P1.** Same defect as `QA-178-r6-01`; one distinct defect, two raw findings. | `39e4f6d` -> this batch | `fixed`. Both gates reached the same conclusion independently and the reviewer named the remedy precisely: *"normalize the diagnostic from trusted parser data or otherwise establish actual parser provenance"*. Provenance is what was established — by making the parse input inert — because normalising the text would have destroyed the specific per-instance messages this slice exists to preserve. |
| L2R10-02 | Stage-2 repo Codex review, round 10, same run dir `cdx-review.5Nd1lG` | "[P2] Assert the exact generated product rather than only its size" — when `_neighbours` returns the correct NUMBER of entries but substitutes the wrong vocabulary, `expected_body` still matches because it checks only cardinality; replacing the Branch STEP neighbour `target` with `exception` leaves the count, uniqueness, partition, denied-cell, parity, safety and killed-reparse checks green while removing all 40 required `opp=target` cases | **P2** | machine-served schemas/contracts | DC-178-F | Standard — anchor: source label P2; no critical class or anchor. | `39e4f6d` -> this batch | `fixed` — the product is now compared as an exact ID SET derived from `MATRIX`, not a count, and the substitution mutant was hand-run: it fails where it previously passed everything. This is the SECOND distinct way this coverage guard was weaker than it claimed (round 2 found the shared-helper form), and both are now closed by comparing against the authority instead of against itself. |
| L3R3-01 | §6 architect implementation review, round 3, run dir `cdx-gate-review.MrjQv5`, base `ca5d778ef11bfe4751e849eb67273f5c4649fe74`, head `6ac2b5d45b14ee056aeb584a1e0b966229fba21e`, `parsedVerdict: ISSUES FOUND`, attested against the plan bytes | "CRITICAL — the payload is not fully inert" — `_inert_payload` preserves scalar objects and dict keys, so a `str` subclass placed in `version` can override `__ne__`, which the parser invokes during the pre-validation version comparison; reproduced raising a forged `ProcessIRValidationError` with a REAL parser code, serving the caller-chosen path, message, remediation and canary verbatim. *"Thus this is another instance patch, not a root fix."* | **CRITICAL** | machine-served schemas/contracts (secondary: secrets — AR2-01) | DC-178-G | **Critical — anchor: source gate labeled it CRITICAL.** | `6ac2b5d` -> this batch | `fixed` for the demonstrated vector AND the class is CLOSED by an accepted limitation, recorded above with the five-variant table. The reviewer is right that my previous fix was the fifth instance patch, and it offered the exit conditionally: the limitation is accepted, the hardening stops, and the `_inert_payload` contract is NARROWED to what it delivers — which is the part that makes the acceptance honest rather than a silent downgrade. Builtin-derived scalars and dict keys are now normalised, closing variants 1–5; the non-builtin residue is stated in the docstring and in the ledger. |
| L3R3-02 | §6 architect implementation review, round 3, same run dir `cdx-gate-review.MrjQv5` | "STANDARD — the exact-product assertion validates labels, not constructed documents" — `_build` composes the ID from its own arguments while the assertion compares only IDs, so substituting the Branch terminal `target` with `exception` inside `_build` while retaining the original IDs left 40 cases labelled `opp=target` containing `exception`, with the exact-product, partition, denied-cell, parity and safety tests all passing; the missing plan requirement is an independent projection from each constructed document back to its claimed signature | **STANDARD** | machine-served schemas/contracts | DC-178-F | Standard — anchor: source label STANDARD; no critical class or anchor. | `6ac2b5d` -> this batch | `fixed` — `_observed_signature` now reads the `(candidate, neighbour)` pair back OUT of every constructed model, stripping the Try anchor on the side `_anchored` adds it, and compares it against what the ID claims. Hand-run with the reviewer's exact mutation: the projection test fails where the whole suite previously passed. **This is the FOURTH distinct way this one guard was weaker than it claimed** (too small; shared helper; cardinality only; labels not documents) — each found by a different gate, and each closed by comparing against the authority rather than against itself. |
| QA-178-r7-01 | Stage-1 QA loop, round 7, inertness battery at `d399c89db9d885e66888ffe50957badc905de99a`; evidence `docs/architecture/evidence/issue-178/qa-round-7.md` | "the docstring's builtin-scalar claim is false for two types" — `complex` and `bytearray` are builtin-derived, carry caller dunders, survive both stages and forge, so "every builtin-derived scalar reaching the parser is a plain builtin" is not true as written | **Low** | *(prose in a docstring — not served to callers)* | DC-178-F | Standard — anchor: source label Low. | `d399c89` -> this batch | `fixed` by NARROWING the claim, which is what QA recommended and why: *"Adding `complex`/`bytearray` would be another instance patch of exactly the kind this slice stopped doing, and it would buy nothing: the same capability is already accepted under the `datetime`/plain-object carrier."* The enumeration is now named as an enumeration, its two absences called out as accurate rather than oversights, and the guarantee stated only for the five types actually normalised. QA also corrected an attribution I would have gotten wrong: `set`/`frozenset` are neutralised by PYDANTIC's dump, not by this fix, so the fix is not credited for them. |
| L2R11-01 | Stage-2 repo Codex review, round 11 (support review for the §6 loop), run dir `cdx-review.z0Xhup`, base `6ac2b5d45b14ee056aeb584a1e0b966229fba21e`, head `d399c89db9d885e66888ffe50957badc905de99a`, dirty=false, `STATUS: completed` | "[P1] Force scalar normalization to return exact builtins" — when `value` is a `str` subclass, `builtin(value)` dispatches its overridable `__str__`, which may return ANOTHER `str` subclass rather than an exact builtin; a `version` subclass can therefore return a second subclass whose `__ne__` raises a forged real-code error, and the caller-controlled path, message and remediation are served verbatim | **P1** | machine-served schemas/contracts (secondary: secrets — AR2-01) | DC-178-G | **Critical — anchor: source label P1.** | `d399c89` -> this batch | `fixed`, and REPRODUCED first: `str(Evil1("1"))` returns `Evil2`, not `str`, so the coercion alone handed back an object still carrying caller dunders. The reviewer's remedy is implemented exactly — the reduced value's type is now VERIFIED to be the exact builtin, and a value that will not reduce is refused with the value-free internal error rather than trusted. Sixth variant of DC-178-G, and the last: it is closed by verification rather than by another enumeration entry. |
| L2R11-02 | Stage-2 repo Codex review, round 11, same run dir `cdx-review.z0Xhup` | "[P2] Make the case projection independent and complete" — `_build` already uses `_body_of` to choose the mutation target, so using the same helper in the projection makes the context dimension self-validating (a `decision_false_arm` routed to `true_arm` would be inspected as the true arm by both, with count, signature, partition, parity and safety all green); the projection also ignored steps past `steps[0]` and discarded the Try anchor without checking its kind | **P2** | machine-served schemas/contracts | DC-178-F | Standard — anchor: source label P2. | `d399c89` -> this batch | `fixed` — the projection now walks from the ROOT independently, asserts the control node's own kind against the context claiming it, asserts decision-arm identity, asserts the Try anchor's kind and side rather than discarding it, and bounds step cardinality. Hand-run against both mutations the old form would have missed — routing `false_arm` to `true_arm`, and smuggling an extra step into every body — and it fails on each. **This is the FIFTH distinct weakness found in this one guard**, and the third caused by sharing a helper with the thing under test. |

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
| L1 — Stage-1 QA | 3 / 3 | decision state `1618f99e48ee5adbf4488970b660a0f273bd4c1b` + uncommitted round-3 delta (the tree as QA round 3 returned it) | **CONTINUE** | **RECORDED LATE, and recorded as such** (`L2R4-01`): this row was first written into `93ac80e` and its original text described the post-correction tree `3cb564d`, which made the decision look trivially safe by counting findings that the correction had already resolved. The workflow requires the decision BEFORE the next mutation, so the state below is the state at the decision point. **Owed validation ran first:** QA round 3 exercised the live arms its own round-1 census proved reach `compile_process_ir_v1`; all five emitted-XML digests and both frozen M8 oracles were byte-identical for a third consecutive round. **Per-tier AT THE DECISION POINT:** zero Critical; **two Standard UNRESOLVED** — `QA-178-r3-01` (destructive read, not reachable today) and `QA-178-r3-02` (inexact baseline attestation); three earlier Standard findings already `fixed`. **Breadth:** one docstring contract and the audit record. **Classes:** DC-178-B recurring (answered structurally, see below); DC-178-D new at this evaluation. **Trend vector, four axes:** highest unrefuted severity Standard -> Standard -> Standard (flat); unresolved at decision 0 -> 0 -> 2 (**worsening, recorded not explained away**); blocking-class findings 0 -> 1 -> 0 (**materially better**); affected-class breadth runtime behavior -> runtime behavior -> non-blocking only (**materially better**). Two axes materially better, one flat, one worsening in raw count only — both new findings sit outside the eight blocking classes. **Named finite next correction:** apply the two round-3 dispositions — document the read-exactly-once precondition with tests, and correct the attestation record. (That correction became `3cb564d`.) **Rule-outs:** CLOSE-CLEAN unavailable — residue was unresolved at the decision point and the §6 and wave gates had not covered the tree. DEFER-* unavailable: `window-exhausted` is spent for the inherited rows and neither new finding is blocked by a mechanism. ESCALATE-OPEN unwarranted — severity unambiguous, validation available, both findings had concrete next actions. |
| L2 — Stage-2 repo Codex review | 3 / 3 | decision state `3cb564d7e5bd7dee9beb1ccde7152915867095d2`, clean (the tree round 3 reviewed) | **CONTINUE** | **RECORDED MID-CORRECTION, and recorded as such** (`L2R4-01`): this row was written in the same batch as the fix for the findings it governs, and its original text marked `L2R3-01/02` `fixed` when at the decision point they were unresolved. Corrected below. **Per-tier AT THE DECISION POINT:** zero Critical; **two Standard UNRESOLVED** (`L2R3-01`, `L2R3-02`), both audit-record integrity; two earlier Standard findings (`L2R1-01`, `L2R2-01`) already `fixed`. **Composition is the decisive fact:** rounds 1 and 2 each found a genuine runtime-behavior defect IN THE FIX ITSELF; round 3 found **none** — its summary reads \"the runtime change appears coherent\" — and both of its findings are audit-record integrity, explicitly outside the eight blocking classes. **Trend vector, four axes:** highest unrefuted severity P2 -> P2 -> P2 (flat); unresolved at decision 0 -> 0 -> 2 (**worsening in count, recorded**); blocking-class findings 1 -> 1 -> 0 (**materially better**); affected-class breadth runtime behavior -> runtime behavior -> audit record only (**materially better**). **No repeated class was instance-patched:** DC-178-B recurred three times and was answered with the structural fix recorded below, each successive fix SMALLER than the one it replaced — the round-2 fix deleted the round-1 machinery outright. DC-178-D reached its second instance at this evaluation and takes its own structural fix, also recorded below. **Named finite next correction:** the batched non-blocking record correction — the two checkpoint rows, the Stage-2 evaluation history, and the DC-178-D invariant — then its own darkness proof and fix-only review. **Rule-outs:** CLOSE-CLEAN premature — the record correction was not yet covered by a review. DEFER-* unavailable and unnecessary. ESCALATE-OPEN unwarranted on every ground. |
| L2 — Stage-2 repo Codex review | 3 / 6 | decision state `f2937f4aa273486e3f07e6e396134494c009d48c`, clean (the tree round 6 reviewed) | **CONTINUE** | Sixth cumulative evaluation, third of the window opened at 3/3, so a checkpoint is forced. **Per-tier AT THE DECISION POINT:** zero Critical; **two Standard UNRESOLVED** (`L2R6-01`, `L2R6-02`), both audit-record integrity, both defects in the GUARD rather than in shipped behaviour; **nine** earlier Standard findings `fixed` (`L2R1-01`, `L2R2-01`, `L2R3-01/02`, `L2R4-01/02`, `L2R5-01/02/03` — derived by counting the rows, after a hand-typed "six" was caught by `L2R7-02`). **Breadth:** one test file. **Classes:** DC-178-D recurring; no new class. **Trend vector, four axes:** highest unrefuted severity P2 flat across all six; unresolved at decision 0/0/2/2/3/2; blocking-class findings 1 -> 1 -> 0 -> 0 -> 0 -> 0 (**materially better and now stable at zero for four consecutive evaluations**); affected-class breadth runtime behavior -> runtime behavior -> audit record -> audit record -> audit record -> the guard itself (**materially better** — the subject has moved from the product, to the record of the product, to the checker of the record). **The code has been unchanged and clean since evaluation 3**; `git diff 3cb564d -- src/` is empty at this tree. **Named finite next correction:** bound the guard's parser to its own section and compare the history as a multiset — both applied in this batch, both hand-run against mutants. **Rule-outs:** CLOSE-CLEAN premature — this batch is not yet covered by a review. DEFER-* unnecessary — zero residue after this batch. ESCALATE-OPEN unwarranted: severity unambiguous, validation available, every finding has a concrete disposition, and round count alone is never grounds to escalate. **Terminal condition declared in advance, so it is a decision rather than fatigue:** the next review validates this batch; per the workflow's non-blocking rule, notes from it that are non-blocking and contain no false claim take a recorded disposition and NO further batch, and Stage-2 closes. |
| L2 — Stage-2 repo Codex review | 3 / 9 | decision state `77ed08a9dd887023c1a0a442b372bc874a2e4aea`, clean (the tree round 9 reviewed) | **CLOSE-CLEAN** | Ninth cumulative evaluation, third of the window opened at 3/6, so a checkpoint is forced — and this row exists because the slice's OWN derived guard demanded it: `test_a_checkpoint_row_exists_for_every_third_review_evaluation` failed on the ninth archived round and would not let the record close without it. That is the DC-178-D structural fix working on its author. **Per-tier AT THE DECISION POINT:** zero Critical unresolved; zero Standard unresolved; **round 9 returned ZERO findings** — *"correctly classifies every pre-parse dump failure as an internal compiler error, preventing caller-controlled diagnostics from being forwarded"*. **The window's history:** round 7 two audit-record findings, round 8 one **P2** that was the same defect QA independently tiered High and I recorded **Critical**, round 9 clean. **Trend vector, four axes:** highest unrefuted severity P2 -> **Critical** -> none (the Critical spike is recorded, not smoothed: it was a defect I INTRODUCED in the §6 round-1 fix, found by two gates independently, and closed with 8/8 exception types normalised and the canary absent under a firing scanner control); unresolved at decision 2 -> 1 -> **0**; blocking-class findings 0 -> 1 -> **0**; affected-class breadth audit record -> runtime behavior -> **none**. **Named finite next correction:** none owed in this loop. The record batch that follows is `docs/architecture/**` only, darkness-proven with a control, and earns no further review under the non-blocking rule. **Rule-outs:** CONTINUE is unwarranted — there is no residue to continue on and no named next correction, and continuing a loop with nothing to fix is the churn the one-pass rule exists to prevent. DEFER-* unavailable and unnecessary: zero residue at either tier. ESCALATE-OPEN unwarranted on every ground. **Scope of this closure:** it closes the L2 loop against the current tree ONLY. The slice is NOT closed — the §6 loop is at evaluation 1 of its owner-fixed 3, and the composite wave gate has not run. |

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
| 3 | `1618f99e48ee5adbf4488970b660a0f273bd4c1b` + fix delta, freeze stamps `3ef7c5a828ef` (live) / `5702803c4b60` (direct API) | regression-clean; all five emitted-XML digests unchanged; r2 findings verified resolved; **2 Low residues** (`QA-178-r3-01/02`) | `docs/architecture/evidence/issue-178/qa-round-3.md` |
| 4 | `fc3cb0e5cfe1c7ea3191df7c3cb93a2696ef0fec` | regression-clean (fifth consecutive identical digest set); **one CRITICAL-tier finding** (`QA-178-r4-01`) and one Low process finding (`QA-178-r4-02`) | `docs/architecture/evidence/issue-178/qa-round-4.md` |
| 5 | `77ed08a9dd887023c1a0a442b372bc874a2e4aea` | **CLEAN** — `QA-178-r4-01` closed; 8/8 `Exception` types normalised with no leak, 4/4 `BaseException` types correctly escape; sixth consecutive identical digest set; one Low self-correction (`QA-178-r5-01`) | `docs/architecture/evidence/issue-178/qa-round-5.md` |
| 6 | `39e4f6d92d6500438713d6a69377d7e5717021b2` | fix works for its own case and does not damage the legitimate path (8/8 real refusals verbatim), but **two forged-diagnostic root causes remain open** (`QA-178-r6-01`, `QA-178-r6-02`); seventh consecutive identical digest set | `docs/architecture/evidence/issue-178/qa-round-6.md` |
| 7 | `d399c89db9d885e66888ffe50957badc905de99a` | variant 5 closed **and all four round-6 breaches closed with it**; accepted limitation CONFIRMED real by construction and the dominance argument MEASURED; one Low claim-accuracy finding (`QA-178-r7-01`); eighth consecutive identical digest set | `docs/architecture/evidence/issue-178/qa-round-7.md` |

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

### Attestation correction (recorded 2026-08-21, raised by `QA-178-r3-02`)

The round-3 dispatch described `1618f99e48ee5adbf4488970b660a0f273bd4c1b` as "the tree you tested
in round 2". That was INEXACT and is corrected here rather than left standing. `1618f99` also
carries three corrections made IN RESPONSE to round 2 — `authoring/workflow.py` (+23/-13, including
the `compile_process_ir_model_v1` call site), `recipes/composer.py` (+8), and
`canonical_process_ir_json(warnings=False)` — none of which existed in the tree round 2 actually
tested. So three applied corrections reached a committed baseline without a QA round of their own.

No harm resulted: round 3 validated all three, and its findings above are the evidence. But taken
at face value the attestation would have directed QA to skip exactly the code that changed, which
is the failure the workflow's "every applied correction gets its affected QA" rule exists to
prevent. Adopted as standing practice for the rest of this slice: diff the named baseline against
the previous one (`git diff <prev> <named> -- src/`) BEFORE attesting it, and state what the delta
contains rather than describing it by which round last ran.

### Darkness proof for the round-5 record batch (owed validation) — TERMINAL record batch

The batch answering `L2R5-01/02/03` touches only `docs/architecture/` and
`tests/test_issue_178_ledger_is_derived_from_its_archive.py`. Measured at
`3cb564d7e5bd7dee9beb1ccde7152915867095d2`: `git diff <sha> -- src/` is EMPTY, with the same
append-and-revert control used for the round-4 proof.

**This is the terminal record batch, and the reason it is not simply churn is recorded here.** The
workflow says non-blocking notes arising from a batch's own validation never earn a further batch.
Rounds 4 and 5 were nevertheless batched because each surfaced a FALSE CLAIM in the audit record —
a checkpoint describing the post-correction state as though it were the decision state
(`L2R4-01`), a structural-fix claim that measurement showed did not prevent its own defect
(`L2R5-02`), and a coverage claim over an instance the invariant cannot reach (`L2R5-03`). A
knowingly-false claim invalidates closure under this repo's own rules, so correcting them was
required rather than optional. Notes from the review of THIS batch that are non-blocking and
contain no false claim take a recorded disposition and no further batch.

### Darkness proof for the round-4 record batch (owed validation)

The batch answering `L2R4-01`/`L2R4-02` touches only `docs/architecture/` and one new test file, so
its affected QA is a darkness proof. Measured at `3cb564d7e5bd7dee9beb1ccde7152915867095d2`:
`git diff <sha> -- src/` is EMPTY, and the batch's paths are `docs/architecture/**` plus
`tests/test_issue_178_ledger_is_derived_from_its_archive.py`. Control: appending one line to
`src/boomi_mcp/models/process_ir.py` made the same command report `1 file changed, 2 insertions(+)`;
reverting returned it to empty. Full non-KB suite at this tree: **10223 passed, 17 skipped** —
*measured here*.

Per the workflow's non-blocking rule, new non-blocking notes arising from THIS batch's own
validation do not earn a further batch; they are recorded with dispositions and Stage-2 closes.

### Darkness proof for the round-3 record batch (owed validation, run before the batch closed)

The batch answering `L2R3-01`/`L2R3-02` touches only `docs/architecture/`, so its affected QA is a
darkness proof rather than a live matrix. Measured at `3cb564d7e5bd7dee9beb1ccde7152915867095d2`:
`git diff <sha> -- src/ tests/` is EMPTY and no staged path lies outside `docs/architecture/`.

The proof carries its own control, because a differential that cannot fail proves nothing: appending
one line to `src/boomi_mcp/compiler/process_ir/pipeline.py` made the same command report
`1 file changed, 2 insertions(+)`, and reverting it returned the diff to empty. So the emptiness is
an observation, not a broken probe.

## Stage-2 repo Codex review (loop 2)

| Evaluation | Run dir | Base -> head | Result | Archived |
| --- | --- | --- | --- | --- |
| 1 | `cdx-review.kNJH4n` | `cdd7a3bf8e2e7ae6773f6ec4250844e5c2e8cf8f` -> `818e0dae78ea729768add36517e5ef01f657f068`, dirty=false | `STATUS: completed`, one **P2** (`L2R1-01`) | `docs/architecture/evidence/issue-178/commit-reviews/cdx-review.kNJH4n/`, teardown `confirmed stopped` |
| 2 | `cdx-review.3Vo4vh` | `818e0dae78ea729768add36517e5ef01f657f068` -> `1618f99e48ee5adbf4488970b660a0f273bd4c1b`, dirty=false | `STATUS: completed`, one **P2** (`L2R2-01`) | `docs/architecture/evidence/issue-178/commit-reviews/cdx-review.3Vo4vh/`, teardown `confirmed stopped` |
| 3 | `cdx-review.Laubc9` | `1618f99e48ee5adbf4488970b660a0f273bd4c1b` -> `3cb564d7e5bd7dee9beb1ccde7152915867095d2`, dirty=false | `STATUS: completed`, **zero code findings** ("the runtime change appears coherent"); two **P2** audit-record findings (`L2R3-01`, `L2R3-02`) | `docs/architecture/evidence/issue-178/commit-reviews/cdx-review.Laubc9/`, teardown `confirmed stopped` |
| 4 | `cdx-review.BmT1ps` | `3cb564d7e5bd7dee9beb1ccde7152915867095d2` -> `93ac80e9055e2d4caa3e9e814234aabe5095faaf`, dirty=false | `STATUS: completed`, fix-only over the record batch; two **P2** audit-record findings (`L2R4-01`, `L2R4-02`) | `docs/architecture/evidence/issue-178/commit-reviews/cdx-review.BmT1ps/`, teardown `confirmed stopped` |
| 5 | `cdx-review.yTxPi2` | `93ac80e9055e2d4caa3e9e814234aabe5095faaf` -> `e59245394825d198184d11c826cb3f47a4461cc5`, dirty=false | `STATUS: completed`, fix-only over the round-4 record batch; three **P2** audit-record findings (`L2R5-01/02/03`) | `docs/architecture/evidence/issue-178/commit-reviews/cdx-review.yTxPi2/`, teardown `confirmed stopped` |
| 6 | `cdx-review.Lfsbye` | `e59245394825d198184d11c826cb3f47a4461cc5` -> `f2937f4aa273486e3f07e6e396134494c009d48c`, dirty=false | `STATUS: completed`, fix-only over the round-5 record batch; two **P2** audit-record findings (`L2R6-01`, `L2R6-02`) | `docs/architecture/evidence/issue-178/commit-reviews/cdx-review.Lfsbye/`, teardown `confirmed stopped` |
| 7 | `cdx-review.wHRVuF` | `f2937f4aa273486e3f07e6e396134494c009d48c` -> `4b39bb051e53af9fa539f0682b56211088bc5557`, dirty=false | `STATUS: completed`, terminal validation of the round-6 batch; one **P2** deferred by design (`L2R7-01`), one **P3** fixed (`L2R7-02`) | `docs/architecture/evidence/issue-178/commit-reviews/cdx-review.wHRVuF/`, teardown `confirmed stopped` |
| 8 | `cdx-review.wr3vzT` | `aedca45344ea9215f77deede343f6724fa264a09` -> `fc3cb0e5cfe1c7ea3191df7c3cb93a2696ef0fec`, dirty=false | `STATUS: completed`, support review for the §6 loop; one **P2** (`L2R8-01`) — the same defect QA independently tiered High | `docs/architecture/evidence/issue-178/commit-reviews/cdx-review.wr3vzT/`, teardown `confirmed stopped` |
| 9 | `cdx-review.xOU3Qb` | `fc3cb0e5cfe1c7ea3191df7c3cb93a2696ef0fec` -> `77ed08a9dd887023c1a0a442b372bc874a2e4aea`, dirty=false | `STATUS: completed`, **zero findings** — "correctly classifies every pre-parse dump failure as an internal compiler error, preventing caller-controlled diagnostics from being forwarded" | `docs/architecture/evidence/issue-178/commit-reviews/cdx-review.xOU3Qb/`, teardown `confirmed stopped` |
| 10 | `cdx-review.5Nd1lG` | `ca5d778ef11bfe4751e849eb67273f5c4649fe74` -> `39e4f6d92d6500438713d6a69377d7e5717021b2`, dirty=false | `STATUS: completed`, support review for the §6 loop; one **P1** (`L2R10-01`) and one **P2** (`L2R10-02`) | `docs/architecture/evidence/issue-178/commit-reviews/cdx-review.5Nd1lG/`, teardown `confirmed stopped` |
| 11 | `cdx-review.z0Xhup` | `6ac2b5d45b14ee056aeb584a1e0b966229fba21e` -> `d399c89db9d885e66888ffe50957badc905de99a`, dirty=false | `STATUS: completed`, support review for the §6 loop; one **P1** (`L2R11-01`) and one **P2** (`L2R11-02`) | `docs/architecture/evidence/issue-178/commit-reviews/cdx-review.z0Xhup/`, teardown `confirmed stopped` |

### Stage-2 closure decision (recorded after evaluation 7)

**Stage-2 is CLOSED for the blocking bar.** Zero Critical at any evaluation; zero unresolved
blocking-class findings; `src/` unchanged and clean since evaluation 3, and `git diff 3cb564d --
src/` is empty at this tree. Evaluations 4–7 produced no code finding at all — every finding was in
the audit record or in the guard that checks it.

**Why this stops here, and why that is a decision rather than fatigue.** The terminal condition was
written into the 3/6 checkpoint BEFORE round 7 ran: notes that are non-blocking and contain no false
claim take a recorded disposition and no further batch. Round 7 returned exactly one of each, and
they are dispositioned accordingly — `L2R7-02` corrected because a false count in a checkpoint's
per-tier accounting cannot support a closure decision, `L2R7-01` deferred by design because it is a
robustness gap with no false claim. The workflow's own words govern: residue outside the eight
blocking classes — *"prose, comments, docstrings, historical counts — anything not served to
callers"* — gets ONE correction pass and never reopens a gate, and *"new non-blocking notes from
[a batch's validation] never earn a second batch"*.

**No further fix-only review is run for the `L2R7-02` correction**, and that is recorded here rather
than left as an omission. It is a single hand-typed number replaced by the value derived from the
rows, in a batch that earned no review under the rule above; its validation is the full non-KB suite
plus the in-tree derived guard, both green. Running an eighth review over a one-word record
correction is the behaviour the ONE-correction-pass rule exists to prevent.

## §6 architect implementation review (loop 3)

| Evaluation | Run dir | Base -> head | Verdict | Archived |
| --- | --- | --- | --- | --- |
| 1 | `cdx-gate-review.CX1MhW` | `cdd7a3bf8e2e7ae6773f6ec4250844e5c2e8cf8f` -> `aedca45344ea9215f77deede343f6724fa264a09` | `ISSUES FOUND` — five findings (`L3R1-01`..`L3R1-05`) | `docs/architecture/evidence/issue-178/architect-reviews/cdx-gate-review.CX1MhW/` |
| 2 | `cdx-gate-review.FrQAIh` | `aedca45344ea9215f77deede343f6724fa264a09` -> `ca5d778ef11bfe4751e849eb67273f5c4649fe74` | `ISSUES FOUND` — one **CRITICAL** (`L3R2-01`), one Standard (`L3R2-02`), one Low (`L3R2-03`) | `docs/architecture/evidence/issue-178/architect-reviews/cdx-gate-review.FrQAIh/` |
| — | `cdx-gate-review.GPK3M5` | — | **TECHNICALLY FAILED**, not an evaluation: both the send and its one sanctioned retry ended `status:"failed"` with no review text (turn 1 truncated mid-preamble, turn 2 empty after ~13 min of healthy streaming). Collected `--outcome failed`; the collector REFUSED to attest (`declared_failed`), wrote `refusal.json` and confirmed teardown. A gate that produced no decision-bearing result is not an evaluation, so it does not consume the owner-fixed window. | `docs/architecture/evidence/issue-178/architect-reviews/cdx-gate-review.GPK3M5/` — the `refusal.json` and `start.json` ONLY. Archived as a failure record, not as a round: it carries no attestation and no review, which is precisely what makes citing it honest. My own derived guard forced this — naming a run the archive did not hold failed `test_no_ledger_cites_a_review_run_its_archive_lacks`. |
| 3 | `cdx-gate-review.MrjQv5` | `ca5d778ef11bfe4751e849eb67273f5c4649fe74` -> `6ac2b5d45b14ee056aeb584a1e0b966229fba21e` | `ISSUES FOUND` — one **CRITICAL** (`L3R3-01`), one Standard (`L3R3-02`) | `docs/architecture/evidence/issue-178/architect-reviews/cdx-gate-review.MrjQv5/` |

**Owner-fixed window: 3 evaluations** (see *Owner decision* above). This is evaluation 1.

**What this gate caught that seven Stage-2 rounds structurally could not.** Four of the five findings
are plan-versus-implementation gaps. The Stage-2 loop is delta-scoped by design — each round compares
a fix against the fix before it — so a requirement the plan stated and the implementation never met
is invisible to it. `L3R1-01` is the clearest case: the derived case set was a REDUCED product whose
own count assertion recomputed the reduced formula, so it agreed with itself while generating 400
body cases instead of 3,000, and the missing neighbour dimension is exactly the shape of corpus
row 1. Nothing in a delta review would ever have surfaced that.

## Accepted limitation — forged diagnostics from an in-process caller (DC-178-G, closed)

**Decision: the hardening loop STOPS here, and the residue is an accepted, recorded limitation
rather than a sixth patch.** Recorded because the alternative was to keep going, and the evidence
says that would not converge.

**The mechanism.** An exception was repeatedly treated as evidence of who authored it. Five variants
were found by THREE independent gates across four rounds, and every fix bought exactly one round:

| # | Variant | Found by | Answer |
| --- | --- | --- | --- |
| 1 | `model_dump` RAISES a parser-typed error | QA r4 + Stage-2 r8 | deleted the arm |
| 2 | dump RETURNS a hostile `items()`, code outside the allowlist | §6 r2 | code allowlist |
| 3 | same, with a REAL parser code | QA r6 + Stage-2 r10 | inert container rebuild |
| 4 | forged `ProcessIRCompileError` raised inside the parse | QA r6 | deleted the dead arm |
| 5 | `str` SUBCLASS in `version` overriding `__ne__`, invoked by the pre-validation version check | §6 r3 | scalar + key normalisation |

Variants 1–4 were each described at the time as closing the class. Each was an instance patch. That
is this repo's own recorded signature — *a finite artifact binding an unbounded space* — and Python
supplies an unbounded supply of dunders on an unbounded supply of types, so the enumeration cannot
be completed by widening it again.

**Why the residue is accepted rather than fixed.** Every variant requires in-process Python that
subclasses the EXPORTED `ProcessIRV1`. A caller with that capability can already monkeypatch
`compile_process_ir_v1`, `parse_process_ir_v1`, or the logging handler directly — so no security
boundary is being crossed, and nothing this layer does can defend one. The property genuinely worth
protecting is the AR2-01 value-free contract, which exists to stop authored values leaking into logs
by ACCIDENT, not to defend against a caller deliberately forging diagnostics about their own data.
Both the §6 round-3 reviewer and live QA reached this independently and both offered it as the
correct engineering answer; the §6 reviewer stated the condition precisely — *"If such callers are
outside the trust boundary, that limitation should be explicitly accepted and this hardening loop
should stop; under the currently claimed contract, it remains Critical."* The contract has therefore
been narrowed to what the code actually delivers, which is what makes the acceptance honest rather
than a downgrade.

**The dominance argument was MEASURED, not asserted.** Live QA round 7 demonstrated that the same
in-process caller every variant requires can already: monkeypatch the public entry (serving a code
and message of its choosing), monkeypatch the guard itself, and install a logging handler that
captures the canary from the AR2-01-protected stream — including `_internal_compile_error`, the
value-free helper every one of these fixes funnels into. The forgery path therefore grants strictly
nothing the caller does not already have. QA's verdict: *"the reasoning is sound and I'd land it."*

**Reopening condition, recorded precisely because an accepted limitation without one is just a
shrug.** This holds BECAUSE `ProcessIRV1` is reachable only by in-process construction. If a
deserialisation path ever reaches it — pickle, or a plugin loader that instantiates caller-named
classes — then "in-process subclass" stops being equivalent to "already owns the process", the
dominance argument fails, and this decision must be reopened. Raised by QA and adopted verbatim.

**Precedent.** #145 reached the same conclusion for a hostile REGISTERED input model and recorded it
as an accepted limit: *when a review series stops converging, ask whether the threat is inside the
trust boundary at all.*

**What still ships, because it was cheap and complete for its own case:** every mapping, sequence,
dict key and builtin-derived scalar reaching the parser is a plain builtin, so variants 1–5 are all
closed. **What is knowingly NOT closed:** a non-builtin object — a `datetime` subclass being the
obvious one — is passed through unchanged BY DESIGN so the parser can refuse it wrong-typed, and it
still carries its own dunders which the pre-validation version comparison will invoke. The
`_inert_payload` docstring states this precisely rather than claiming inertness it does not have. No
follow-up issue is filed: this is an accepted limitation, not debt, and minting an issue for a
non-boundary would be the debt-minting the workflow warns against.

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

**DC-178-D** — *a required ledger ROW the durable evidence already knows about, left unwritten*.
Runtime authority: `docs/architecture/evidence/issue-178/index.jsonl`, which the collector writes
one row into per COLLECTED review round. Instances: `L2R3-02` (a round archived but absent from the
evaluation table), `L2R3-01` (three evaluations accrued with the checkpoint table empty), and
`L2R4-01`/`L2R4-02` from the same family. Second instance triggered the structural fix:

**Coverage claim CORRECTED (`L2R5-03`).** An earlier revision of this record also listed
`QA-178-r3-02` here and claimed the invariant covered it. Measured: it does not — this authority
contains only collected commit-review rounds, with no QA baseline or source-diff information, so a
Stage-1 evaluation naming the wrong tested tree leaves every assertion green. The claim is WITHDRAWN
rather than reworded, and that finding is reclassified below as DC-178-E.

- **The invariant is EXECUTABLE, not prose** — `tests/test_issue_178_ledger_is_derived_from_its_archive.py`.
  It derives the collected-round set and the checkpoint obligation from `index.jsonl` and asserts
  the ledger against them in BOTH directions: every archived round is cited, and no cited round is
  unarchived. Prose can now disagree with the archive only by failing a test. This is the shape
  #171 recorded as holding where prose structural fixes recur.
- **Non-vacuity, hand-run before the claim was written**: removing one round's citation fails three
  assertions; deleting the Stage-2 checkpoint row fails the obligation check; the ledger was
  restored byte-identically and the suite returned green.
- **Coverage claim, derived from the authority's full case set**: every row in this slice's
  `index.jsonl` with `collector == "commit-review-collect"` and `status == "completed"`, and the
  checkpoint obligation computed from that count rather than from memory.
- **Sibling sweep — WIDENED, and the earlier scoping note in this row was measured wrong.**
  A prior revision of this bullet said the invariant could not go repo-wide, citing "`issue-152`
  has 65 archived runs its ledger never cites, `issue-153` has 11, `issue-175` has 1". Those
  figures counted gate-review runs belonging to OTHER logical loops, not Stage-2 rounds, and the
  conclusion drawn from them was wrong. Re-measured against the collector-written fields, two of
  the three assertions widen cleanly and now do:
  - **No ledger cites a run its archive lacks** — holds for ALL NINE ledgers with no exemption, and
    is enforced repo-wide.
  - **Every completed archived run is cited in its ledger** — holds for six of nine. The three that
    do not (`issue-152` 64 runs, `issue-153` 8, `issue-175` 1; 73 total) predate the convention and
    are FROZEN in `tests/fixtures/audit_ledger_citation_legacy_baseline.json`. The freeze is
    asserted exactly and in both directions: it cannot be padded with a run that IS cited (which
    would license dropping it later), and a NEW uncited run in any ledger fails — including in the
    three exempt ones. Every future ledger is covered automatically.
  What remains #178-scoped is only the evaluation-table and checkpoint-row parsing, because that
  table FORMAT exists in no other ledger (measured: zero numbered `cdx-review` rows elsewhere), and
  asserting a format nobody adopted would fail correct records. One field is deliberately not used
  as an authority anywhere in the guard: `logical_loop` is free-typed prose — 27 distinct spellings
  across the archives and 66 rows with no value — so deriving from it would be this very class.

**DC-178-E** — *a prose CLAIM about which tree was tested, hand-modelled instead of derived from
the diff*. Runtime authority: `git diff <previous> <named> -- src/`. One instance:
`QA-178-r3-02`. Split out of DC-178-D on measurement (`L2R5-03`) because the mechanism and the
authority both differ: DC-178-D is a row the archive already knows about and can therefore be
derived; DC-178-E is an assertion about a delta that no archived artifact records, since the
dispatch prose lives outside the tree.

Disposition — a recorded DEVIATION under the Standard-tier path, on the ground that the invariant
is infeasible in-slice: the claim that failed was made in a QA dispatch message, which is not a
tree artifact and cannot be asserted against by any in-tree test. What IS in-tree is the practice
that replaces it, adopted in the *Attestation correction* section above: diff the named baseline
against the previous one before attesting, and describe a tree by its delta rather than by which
round last ran. No follow-up issue is filed, per the standing rule that an accepted limitation is
recorded rather than minted as debt — the discriminating evidence a second instance must supply is
a case where the mispositioned claim is reachable from an in-tree artifact, which would make it
DC-178-D and mechanically preventable.

**DC-178-C** — *a ProcessIR model dumped under pydantic's default `warnings=True`, interpolating
the authored value into a serializer warning*. Runtime authority: the AR2-01 rule. One instance
(`QA-178-r2-02`, pre-existing) plus three siblings found by the same sweep and fixed with it.

## Final-tree validation (filled at close; every roster gate current on the FINAL sha)

| Gate | Evidence (quoted output / run URL / archived round) | SHA |
| --- | --- | --- |

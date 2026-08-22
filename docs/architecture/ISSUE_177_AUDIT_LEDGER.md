# Audit ledger — issue #177 (M12 — served-text ↔ enforcement consistency gate, DC-175-D structural fix)

Instantiated at Stage-1 step 0 per `docs/architecture/templates/AUDIT_LEDGER_TEMPLATE.md`, committed
in the Stage-1.5 baseline commit. Conventions inherited from #175/#178's end state apply from row
one: no hand-maintained totals or instance lists — counts are derived from the rows; every derived
field carries its deriving anchor inline; platform-behaviour claims carry a provenance marker
(`measured here` / `documented, not measured` / `assumption`); measured output is quoted, never
re-keyed from memory; audit-record-integrity findings are NON-blocking from round one; the
defect-class table is derived LAST, after the final finding row of the final batch.

## Baseline (Stage-1 step 0)

- Issue: #177 — Served-text ↔ enforcement consistency gate (DC-175-D structural fix)
- Step-0 baseline: `6f26caff7481356119fee5b36a1730cec0fb5df2`
- Measured green baseline at that tree: **10236 passed, 17 skipped** in 863.35s — *measured here*,
  quoted verbatim from the run's own summary line, full non-KB suite (`.venv` 3.12, `PYTHONPATH=src`,
  `pytest tests --ignore=tests/kb -p no:cacheprovider`). 10253 collected, which equals the current
  `minimum_collected` floor in `tests/fixtures/wave_gate/test_nodes.jsonl`.
- Slice kind: **behaviour-affecting**. The served static text of seven compiler diagnostic codes
  changes, and three of them change the summary a caller receives through the machine-facing
  `process_ir_authoring` projection (today those three serve their remediation in the summary slot).
  The ProcessIR authoring / compiler revision digests therefore move. Not dark.
- Artifact trust boundary. The slice **CREATES AND OWNS**: the served-text/enforcement guard modules
  and their AST emission collector; the manifest-keyed capability witness registry; the extracted
  dual-entry-point differential helper; the strict §8 capability-table parser; the seven canonical
  compiler messages; this ledger and its evidence archive under
  `docs/architecture/evidence/issue-177/`. It **CONSUMES**, unchanged and as authorities it may
  never rewrite: `PROCESS_IR_V1_CAPABILITIES` in `src/boomi_mcp/models/process_ir.py` (values and
  ordering); the §8 table in `docs/architecture/PROCESS_IR_V1.md`; `BODY_CAPABILITIES_V1`; the
  parser's own diagnostic tables and `parse_process_ir_v1`; #178's derived parity case generator and
  its corpus; `docs/architecture/ISSUE_175_AUDIT_LEDGER.md`, which is append-only and is never edited
  by this slice.
- Expected defect classes, pre-enumerated so a second instance triggers structurally ON ARRIVAL:
  1. **DC-177-A** (inherits **DC-175-D**) — served prose/text describing a capability or diagnostic
     the enforcement no longer grants; runtime authority: the capability manifest, the diagnostic
     registries, and the graph verifier. This slice exists to ship this class's invariant, so a fresh
     instance inside it is a direct signal the invariant is too narrow.
  2. A hand-enumeration shadowing a derivable authority — the standing class. This slice's whole
     deliverable is authority-derived case sets, so any hand-listed code set, capability set, or
     count is an instance.
  3. A guard that enumerates nothing and therefore passes everything — recorded in this repo five
     separate times (#149, #151, #162, #175, and #175's own prose scanner). Every invariant here
     carries a non-vacuity witness for exactly this class.
  4. A derived value re-derived by hand at the wrong moment (inherits **DC-175-D**) — this slice
     maintains manifest floors, a defect-class table, and three coverage claims, all derived.
  5. Hand-modelled platform behaviour in served text.

## Loop roster (fixed BEFORE the first correction; a gate not listed here cannot mint a loop
## mid-run — a roster addition is itself a recorded checkpoint decision)

1. **Stage-1 QA** — live `boomi-qa-tester` engagement through the public MCP tool boundary, scoped to
   the served `process_ir_authoring` diagnostic and capability projections and the ProcessIR
   plan/compile refusal paths this slice changes.
2. **Stage-2 repo Codex review** — detached, `--base 6f26caff7481356119fee5b36a1730cec0fb5df2`, then
   delta-scoped fix rounds. Every round COLLECTED via step 5d, never read from `wait`.
3. **§6 architect implementation review** — declared additive gate, run under
   `/codex-claude:codex-issue`, judging implementation-vs-design-plan. Fresh checkpoint window.
4. **Composite wave gate** — full non-KB suite + every active golden-manifest entry + deterministic
   compile/fingerprint checks + one integration-level review of the wave delta + one live scenario
   per changed capability class. ONE composite evaluation, not five loops.
5. **Terminal correction loop** — ONLY via a recorded roster-addition checkpoint.

## Recorded design decisions (made at Stage-1 step 0, before the first correction)

- **`diagnostic()` in `src/boomi_mcp/compiler/process_ir/diagnostics.py` stays TOTAL.** The design
  plan proposed making it raise for an unregistered code. It is called from inside the handlers that
  exist to stop a raw escape (`pipeline._guarded`, `emitter_registry`), and every production handler
  catches only `ProcessIRCompileError` — a raise there converts a diagnostic-registration gap into an
  unhandled error on a live MCP tool. `pipeline._compile_error_from_validation`'s own docstring
  records that exact failure mode being fixed once already. Fail-closed lives in the guard instead;
  the fallback branch is provably unreachable once the guard is green, and is retained as a
  degradation path, not as a silent default. *Deviation from the architect design plan, recorded.*
- **The compiler's served text is the compiler layer's OWN authority, not a copy of the parser's.**
  Four of the seven codes with an empty compiler message also carry a parser message. Hand-writing
  compiler text for them is NOT a DC-177-A instance: the mechanism of that class is a hand-written
  description of a fact whose runtime authority lives *elsewhere*, and the compiler table is itself
  the authority for compiler-served text. The repo's shipped design already keeps per-layer
  remediations deliberately distinct and the projection attributes each to its producer. Consequence
  recorded here so it is not re-litigated: **no cross-producer text-identity assertion is added** —
  it would contradict the shipped design and
  `test_every_producer_s_message_AND_remediation_is_findable_in_its_entry`.
- **Case sets are never narrowed; only mechanisms are.** Acceptance criterion 3 requires coverage
  derived from the authority's full case set, so all 27 manifest keys, the whole §8 table, and the
  full served diagnostic union are in scope. Two mechanisms are narrowed against non-convergence
  risk: the AST collector pins an unresolved-SITE table instead of modelling Python control flow, and
  the capability witnesses do not restate #178's cross-entry vector equality (already gated by a
  derived product over the whole placement matrix). *Deviations from the architect design plan,
  recorded.*
- **Issue scope item 4 is WITHDRAWN by the issue itself** and is not built here. No general
  English-semantics checker. #175's prose scanner stays exactly where it is, as a best-effort lint.

## Defect-class ledger (empty at instantiation; a class is a (mechanism,
## runtime-authority) pair, assigned at reconciliation, revisable with original retained)

| Class | Mechanism | Runtime authority | Instances (derived from rows) | Resolution |
| --- | --- | --- | --- | --- |
| **DC-177-A** (inherits **DC-175-D**) | a hand-written record of a fact whose runtime authority lives elsewhere — served text, or durable prose, describing a capability or diagnostic the enforcement no longer grants | the capability manifest, the three diagnostic registries, and the emitting modules | **2 rows / 2 distinct defects** — QA-177-r1-01, QA-177-r1-02. Both are in the NON-SERVED region (a docstring and a module comment), proven not served by the round-1 sweep. | **STRUCTURAL FIX SHIPPED** for the served region, which is the region the issue scoped: three executable invariants, each deriving its expectation from a runtime authority, each with a bidirectional pin, a hand-run non-vacuity witness against the real historical defect, and a coverage claim over the authority's full case set. The two rows above are the residue the issue itself PREDICTED and deliberately excluded: scope item 4 (a predicate over English prose) was withdrawn because prose has no closed case set, so no invariant here can cover a docstring. Its doc-prose half is #147's `M12_CAPABILITY_DOC_DRIFT`. Both instances are fixed in the one batched non-blocking pass, with a sibling sweep over the old strings. |

Second-instance check: run against this table AT ROW-WRITE TIME, and also when a second finding lands
in the same file/subsystem within a loop. On the second instance the structural fix is mandatory in
that batch (or the immediately-next dedicated batch for dispatch/compiler/materialization/shared-apply
code), with sibling sweep, non-vacuity witness, and an authority-derived coverage claim.

## Finding rows (one per raw finding; append-only; exactly one disposition each)

| ID | Source gate + run dir + attestation | Verbatim summary | Original label | Blocking class | Defect class | Derived tier (anchor inline) | SHA/delta | Disposition |
| --- | --- | --- | --- | --- | --- | --- | --- | --- |
| INH-177-D | Inherited — `docs/architecture/ISSUE_175_AUDIT_LEDGER.md` DC-175-D class row, recorded deviation filed at that reconciliation | "**RECORDED DEVIATION → #177, reason class `blocked-by-mechanism`, filed at this reconciliation.** Standard tier only, which is the sole tier a deviation is available to. Grounds: the invariant needs a general mechanism tying served text to the enforcement behind it, across capability entries, diagnostic registries, architecture docs and the compatibility inventory — repo-wide machinery, not a Process Call change." | *(no source severity label — a class-level deviation, Standard tier by the rule quoted)* | machine-served schemas/contracts | DC-177-A | Standard — anchor: the deviation is recorded as Standard tier only; no critical class (not secrets, data loss, or mutation accounting) and no critical anchor. | this slice | `fixed` — the class invariant SHIPPED for the served region: invariant 1 (every emittable code carries complete served text, derived from an AST read of the emitting modules), invariant 2 (a manifest-keyed capability witness registry, 27/27 keys), invariant 3 (§8 key-AND-state equality against the manifest). Each carries a bidirectional pin and a hand-run non-vacuity witness against the real historical defect; evidence in `docs/architecture/evidence/issue-177/non-vacuity.md`. |
| INH-177-G | Inherited — `docs/architecture/ISSUE_175_AUDIT_LEDGER.md` DC-175-G class row, recorded deviation filed at that reconciliation | "**DEVIATED → #177** (`blocked-by-mechanism`, filed at this reconciliation) … English has no closed case set, so a predicate over prose cannot meet that bar … It is RETAINED as a best-effort lint, pinned by the real historical defect and by every counter-example a reviewer supplied, and it is explicitly not the class invariant. #177 owns the invariant, where checks derive from runtime authorities instead of from reading English." | *(no source severity label — a class-level deviation, Standard tier by the rule quoted)* | machine-served schemas/contracts | DC-177-A | Standard — anchor: same as INH-177-D; the deviation is Standard tier only. | this slice | `deferred` — **#147**, reason class `out-of-scope-by-design`, placement: #147 is the post-#160 M12 closeout that already owns `M12_CAPABILITY_DOC_DRIFT` and consumes this gate. Grounds, quoted from the issue's own withdrawal of scope item 4: "English has no closed case set, so a predicate over prose cannot make the coverage claim the structural-fix rule requires." The BOUNDED half — served machine text — is discharged here by invariants 1-3. #175's prose scanner stays exactly where it is, a best-effort lint, explicitly not this class's invariant. This is `out-of-scope-by-design`, not `window-exhausted`: no lineage is consumed. |
| QA-177-r1-01 | Stage-1 QA, live, `agents/reports/2026-08-22-issue-177-served-text-gate-r1.md`, round 1 | "`_diagnostic_summary`'s docstring in `process_ir_projection.py` still says \"Seven compiler codes have no entry in the static `_MESSAGES` table\" and \"Only THREE reach this fallback\" (measured: 0 and 0), and cites `test_exactly_the_expected_codes_fall_back_to_their_remediation` as its measurement authority — a node **this same slice tombstoned**. Its fallback loop is now dead code." | **Low** | *(none — durable prose, PROVEN NOT SERVED by a 405 KB sweep across list_capabilities, the root template, all ten category pages and 45 tool descriptions, with two control phrases firing)* | DC-177-A | Standard — anchor: source label Low; no critical class and no critical anchor. Outside the eight blocking classes, so it takes the ONE batched correction pass. | this fix | `fixed` — the docstring now states what is true (all three counts are zero, measured), explains the historical shape because that is why the pin exists, and cites the LIVE authority `test_no_compiler_diagnostic_falls_back_to_its_remediation` instead of the tombstoned node. The fallback loop is RETAINED as an honest degradation path rather than deleted, and is pinned empty by that test — deleting it would remove the only behaviour a future regression could land on. |
| QA-177-r1-02 | Stage-1 QA, same report and round | "the identical three-place comment about serving an empty string \"so a caller can see the gap\" was corrected in `diagnostics.py` and removed in `models/process_ir.py`, but is unchanged in `findings.py`, three lines above the `finding_specs()` docstring that says \"Fails closed since #177\". A sibling-sweep miss." | **Low** | *(none — module comment, proven not served by the same sweep)* | DC-177-A | Standard — anchor: source label Low. Outside the blocking classes. | this fix | `fixed` — and the finding is exactly right: I corrected two of three siblings and left the third contradicting the docstring immediately below it. The comment now records that the blank-emitting rationale was MEASURED false (seven rows served for four slices, no caller looked) rather than merely reversed. Swept afterwards for the old strings across `src/`, `tests/` and `docs/`: the only remaining occurrences are the new text quoting the old rationale as history. |

Dispositions: `fixed` · `finding-refuted` · `severity-refuted` · `not-validated` · `deferred` (issue,
reason class, placement). A refutation names the disputed claim and the concrete evidence. An original
label is never edited — a revision is a new dated line with the original retained.

## Checkpoints (a row is written IN FLIGHT at every third evaluation of each loop —
## 3, 6, 9, … — in the batch it governs, never reconstructed at close)

| Loop | Evaluation (window / cumulative) | SHA (+dirty) | Outcome | Rationale |
| --- | --- | --- | --- | --- |
| Stage-2 repo Codex review | 3 / 3 | `f1ebd0d1074607a5d703c9d7562fa4d97c32475c` (clean) | `CONTINUE` | Recorded AFTER the round-3 correction's owed validation (round 4), per the rule that reaching a checkpoint never cancels validation already owed. **Per-tier counts:** 0 critical in every round; Standard 4 (r1) → 3 (r2) → 3 (r3) → 2 (r4). **Breadth:** r1 spanned three artifacts (collector, witness registry, guard); r2 two; r3 and r4 are entirely inside ONE function, `resolve_forward`, which r2 caused me to add. **Defect classes** (derived from the rows): DC-177-B *(a guard whose identity key is coarser than the property it claims to pin)* — instances r1-01, r1-02, r2-03; structural fix applied at r2 (full site identity + per-site counts, with a hand-run witness proving the old key passed both escape shapes); **no instance since → resolved**. DC-177-C *(a witness that does not reach the enforcement it claims to pin)* — instances r1-03, r1-04, r2-01; fixed by adding the compile-path halves and the lowered-retry assertion; **no instance since → resolved**. DC-177-D *(an AST reader modelling an open-ended Python form space and failing OPEN on unmodelled forms)* — instances r2-02, r3-01, r3-02, r3-03, r4-01, r4-02; **recurring**. **Trend:** none worsening; two classes resolved; breadth collapsed from three artifacts to one function. **Rule-outs.** `CLOSE-CLEAN` is illegal — r4 carries validated findings. `DEFER-STANDARD-AND-PROCEED`/`AND-CLOSE` rejected: the residue is not blocked by mechanism or out of scope by design, and a fix exists that costs one batch. `ESCALATE-OPEN` rejected: the structural-fix rule escalates only when a class recurs after its structural fix **with no materially different action left**, and there is one — DC-177-D's r3 fix EXTENDED the resolver, whereas the named correction below DELETES it. **Named finite next correction:** remove `resolve_forward`/`_is_rebound` entirely; report any forwarded parameter that is not a sink's own first parameter as unresolved; restore `invariants.py` `_fail` to the pinned table with a human-stated two-code authority that the guard asserts is served; replace the resolver's control with one over the reporting property. Applied in this batch. |
| Stage-2 repo Codex review | 3 / 6 | `6f4512c018f381365898be1c0725543399dbd832` (clean) | `CONTINUE` | Second window's third evaluation, recorded after r6's findings were reconciled and before the next mutation. **Per-tier counts:** 0 critical in every round; Standard 4 / 3 / 3 / 2 / 2 / 2. **Breadth:** r6's two findings are both inside ONE test function, the census added at r5. **Defect classes:** DC-177-D *(an AST reader modelling an open-ended Python form space)* — the class that could not converge — is **ELIMINATED, not patched**: r4's decision deleted `resolve_forward`/`_is_rebound` outright rather than adding a sixth form, and r5 and r6 contain no instance of it. DC-177-B *(an identity key coarser than the property it pins)* **recurred** at r6 in new code: the census compared against the MERGED served union and applied its by-design allowlist by code alone. Both are bounded — they are scoping errors in a check, not an open-ended domain — and both are fixed in this batch by keying the census on the referencing path's producer and on the allowed path prefix, with the two escape scenarios the reviewer described hand-run RED and recorded. **Trend:** none worsening; the non-convergent class is gone, which is the materially-better dimension — a flat count of 2 that no longer contains an unbounded class is not the same residue as before. **Rule-outs.** `CLOSE-CLEAN` illegal: r6 carried validated findings. `DEFER-*` rejected: the residue is neither `blocked-by-mechanism` nor `out-of-scope-by-design`, and the correction costs one batch — deferring a one-batch fix into a freshly-minted issue is exactly the debt-minting the default warns against. `ESCALATE-OPEN` rejected: no critical residue, and a concrete corrective action existed and is applied. **Named finite next correction:** scope the census by producer table and by allowed source path (applied here); r7 is its owed delta-scoped validation. |

Each rationale records: per-tier counts and breadth, new/resolved/recurring defect classes (derived
from the rows), the trend vector, explicit rule-outs of the other outcomes, and a NAMED finite next
correction.

## Deferrals

Pointer-only — reason class, placement, and lineage live on the finding row and in the filed issue.
RULE: a deferral's issue body QUOTES the commit SHA of the already-committed checkpoint row it cites.
`window-exhausted` is single-use per finding.

*(none at instantiation)*

## Evidence index

Collected run directories are archived (byte-verified, allowlisted sidecars) under
`docs/architecture/evidence/issue-177/` with `index.jsonl` + `SHA256SUMS`, in the batch that collects
them. Run citations always use the COMPLETE run-dir name (`cdx-review.<suffix>` /
`cdx-gate-review.<suffix>`, backticked).

Non-vacuity mutant evidence lives in `docs/architecture/evidence/issue-177/non-vacuity.md`: for each
invariant, the exact file and edit, the guard that went red with its first assertion line, and the
restored-green confirmation.

## Final-tree validation (filled at close; every roster gate current on the FINAL sha)

| Gate | Evidence (quoted output / run URL / archived round) | SHA |
| --- | --- | --- |

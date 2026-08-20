# Audit ledger — issue #175 (M12 defect slice)

Instantiated at Stage-1 step 0 per `docs/architecture/templates/AUDIT_LEDGER_TEMPLATE.md`.
Conventions inherited from #152's end state apply from row one; platform-behaviour claims
carry a provenance marker (`measured here` / `documented, not measured` / `assumption`).

## Baseline (Stage-1 step 0)

- Issue: #175 — Process Call outbound connectivity contradicts the platform's return-path model
- Step-0 baseline: `3fd5027e16d4f9fe5377884a0140909a3b4d1e67`
- Measured green baseline at that tree: **10096 passed, 17 skipped** (full non-KB suite,
  `.venv` 3.12, `PYTHONPATH=src`, plugin autoload disabled) — *measured here*.
- Slice kind: behaviour-affecting (emitted process XML changes on every Process Call surface)
- Artifact trust boundary: the slice CREATES AND OWNS the terminal Process Call grammar, the
  new capability code and its remediation, the graph-verifier continuation invariant, the
  legacy catch-leg composition rules, the wrapper singleton rules, this ledger, and its
  archive. It CONSUMES, unchanged and as the positive oracle it may never rewrite: the two
  M11 UI-built live captures under `tests/fixtures/live_xml/m11/`, the platform's own Process
  Call and Return Documents documentation, and the pre-baseline frozen bytes recorded in the
  archive. Generated goldens are NEGATIVE-ONLY witnesses in this slice; they are never
  promoted to platform evidence.
- Expected defect classes (pre-enumerated so a second instance triggers structurally ON
  ARRIVAL): a hand-model of a fact whose authority lives in another component; a guard that
  permits a condition without rejecting its inconsistent inverse; served prose that outlives
  the enforcement it describes; a generated artifact used as its own oracle.

## Platform model (the authority this slice is derived from)

A Process Call's outbound connection is **not** a free graph edge. The parent shape carries a
return-path element naming the CALLED process's Return Documents shapes; only those make a
forward connection valid, and a call whose child returns nothing is a path terminal.
*Documented* (help.boomi.com, "Process Call step" / "Return Documents step") and *measured
here* against the in-tree UI-built captures: in
`tests/fixtures/live_xml/m11/process_doccacheretrieve_loadalldoc_variant.xml`, shapes
`shape4`, `shape33` and `shape34` each carry an empty return-path element and NO outgoing
dragpoint, while `shape10` — the only one with a populated entry — is also the only one with a
forward edge, and that edge carries an `identifier` attribute equal to the entry's
`childShapeName`. `process_dpp_profile_decision_flow.xml` shape `shape61` is a fifth terminal
instance. Four terminal to one connected, and the connected one needs TWO coordinated bindings.

## Deferred capability (recorded at instantiation; the target issue already exists)

Returning-child return-path binding is transferred to **#176** ("Process Call return-path late
binding for returning subprocesses", filed 2026-08-20 under epic #134, placed immediately
after #175 and before #156). Reason class: `out-of-scope-by-design`. Two measured facts size
it and are recorded in its body: the connected form needs a return-path entry AND a matching
dragpoint identifier keyed the same; and `render_dragpoints` has exactly two byte-exact forms
today (labeled = identifier and text; plain = neither), so the live returning form — identifier
without text — is a THIRD form this repo cannot currently emit at all (*measured here*). This
is a gated capability transfer, not blocking residue, once every connected form is unreachable.

## Loop roster (fixed BEFORE the first correction; a gate not listed here cannot mint a loop
## mid-run — a roster addition is itself a recorded checkpoint decision)

1. Stage-1 QA — one live `boomi-qa-tester` engagement through the public MCP tool boundary,
   slice-scoped, with the pre-fix characterization probes as its first task.
2. Stage-2 repo Codex review — detached, `--base 3fd5027e16d4f9fe5377884a0140909a3b4d1e67`,
   then delta-scoped fix rounds. Every round COLLECTED, never read from `wait`.
3. Architect implementation review — the additive gate this run's wrapping pipeline declares,
   judging implementation against the attested design plan.
4. Composite wave gate — full suite + every active golden-manifest entry + deterministic
   compile/fingerprint checks + one integration-level review of the wave delta + one live
   scenario per changed capability class.
5. Terminal correction loop — ONLY via a recorded roster-addition checkpoint.

## Defect-class ledger (a class is a (mechanism, runtime-authority) pair, assigned at
## reconciliation; counts derived from the rows' class cells, never typed)

| Class | Mechanism | Runtime authority | Instances (derived from rows) | Resolution |
| --- | --- | --- | --- | --- |
| **DC-175-A** | assert that a Process Call continues, and emit the outgoing edge, without consulting the called process | the called process's Return Documents shapes | **2** — SRC-175-1, SRC-175-2 | **Second instance on arrival ⇒ the structural fix is mandatory in this batch, not an instance patch.** The two instances are the canonical emission chain and the legacy catch composition; they share the mechanism and the runtime authority exactly. The structural fix is the graph-verifier invariant — an empty return-path set forbids an outgoing dragpoint — enforced at the one place every emitted graph passes through, instead of patching each emission site. Sibling sweep: the four return-path occurrences in `src/` (three verifier read sites, one legacy comment, and the single hardcoded writer in the shared renderer). Non-vacuity witness: a frozen pre-baseline negative fixture PLUS a two-directional in-memory mutation control (clear the populated entry on the valid capture → the error appears; restore it → it does not). |
| **DC-175-B** | a grammar rule asserting a fact about another component's graph that the model has no field to carry and no check to verify | the called process's Return Documents shapes | **1** — SRC-175-3 | Same authority as DC-175-A but a different mechanism (grammar, not emission), so it is recorded as its own class rather than a third instance. Resolved by making the terminal form the supported one and every connected form a typed refusal pointing at #176. |
| **DC-175-C** | a generated artifact used as evidence for the behaviour that generated it | the platform's own UI-built export | **0** at instantiation | Structurally prevented rather than detected: the pre-fix bytes are frozen into the archive BEFORE the first source edit, the clean-room provenance rule binds every QA fixture row, and generated goldens are declared negative-only in the trust boundary above. |
| **DC-175-D** | served prose describing a capability the enforcement no longer grants | the enforcement registries and the graph verifier | **0** at instantiation | Every contracted form in the inventory carries the served surface that advertises it; the served text moves in the same batch as the enforcement. |
| **DC-175-E** | a #175 consequence mirrored into ONE of two sibling protocol surfaces — `wrapper_subprocess` got the treatment at every layer, `database_to_api_sync` at none | the shared catch-leg emitter's actual composition behaviour | **2** — QA-175-r1-01, QA-175-r1-03 | **Second instance on arrival ⇒ structural fix mandatory in this batch.** Both instances are the same mechanism (hand-mirroring, per protocol, a rule the shared emitter owns), so the response is not two patches: the composition rule now has ONE definition that both the plan-time and emit-time sites call, and the served-code expectation is DERIVED by driving each builder rather than hand-listed. Sibling sweep: 2 plan-time call sites and 3 emit-time call sites, all reachable through the two functions — enumerated and confirmed. Non-vacuity witnesses: a matrix test asserting the two sites refuse the SAME cells, derived from the authority's own case set (every supported DLQ mode × notify × exception) with an explicit non-emptiness assertion; and a mutation control that neutralises the shared function and proves BOTH sites stop refusing on it — which also surfaced, and now pins, a third independent boundary one layer down in the renderer. |
| **DC-175-F** | an unpinned hand-copy of a step-status derivation whose authority is another code path in the same module | the canonical executor's outcome-derived status | **1** — QA-175-r1-02 | Same MECHANISM as DC-175-A (a hand-model of a fact whose authority lives elsewhere) but a DIFFERENT runtime authority, so it is recorded as a recurrence rather than treated as a cross-subsystem refactor, exactly as the structural-fix rule directs. Fixed at the instance, and the sibling sweep was RUN rather than deferred: the module has exactly two action-derived status sites (the canonical executor's, already outcome-aware, and this one), so no third hand-copy exists and no follow-up is warranted. Pinned two ways — a matrix over the authority's own outcome cases, and a source-level assertion that the pre-fix line cannot return. |
| **DC-175-G** | a served diagnostic asserting a consequence the check itself did not establish | the graph's actual edge set | **1** — L2-r4-01 | Not a second instance of anything, so no structural trigger. Corrected at the instance AND at its cause: the message now names the offending CONNECTION rather than its target, and states only what this check knows — the platform drops an unattributed connection — instead of asserting an unreachability that depends on the rest of the graph. The cause is recorded because it is the reusable lesson: a TEST asserted the target must appear in the message, which is what pushed the diagnostic into naming the wrong thing. A test can make a message wrong. |

## Finding rows (one per raw finding; append-only; exactly one disposition each)

| ID | Source gate + run dir + attestation | Verbatim summary | Original label | Blocking class | Defect class | Derived tier (anchor inline) | SHA/delta | Disposition |
| --- | --- | --- | --- | --- | --- | --- | --- | --- |
| SRC-175-1 | originating report — issue #175 body, §3(a); reporter Hlib (UI observation) + repo forensics; no run dir (pre-gate intake) | "Our emitter hardcodes an empty return-path element in every case and *still* draws a dragpoint to a mandatory trailing Stop … The UI resolves the contradiction by not rendering the connection — leaving an orphaned Stop floating on the canvas." | **P1** (source label, immutable) | emitted XML or graph validity | DC-175-A | **Critical** — anchor: source label P1 derives to Critical per the tier rule ("any finding its source gate/reviewer labeled P0/P1/Critical/High"). Never deferred, never closed over. | baseline `3fd5027e16d4f9fe5377884a0140909a3b4d1e67` | `fixed` — every emission surface now produces the TERMINAL form. Validated three ways: the regenerated goldens are structurally byte-identical to the live UI-built terminal shape (3/3 emitted calls match the single skeleton the m11 capture's three terminal calls collapse to); live QA built a wrapper and a branch through the public MCP boundary and read back `['start','processcall']` / `['start','branch','processcall','processcall']` with zero `stop` shapes; and `verify_process_graph` returns clean with a non-zero `shapes_checked`. |
| SRC-175-2 | originating report — issue #175 body, §4 blast radius; extended during implementation planning to the legacy catch leg (*measured here*: the catch composition wires the DLQ Process Call to a downstream terminal) | "Every surface that emits `processcall` produces the pattern today" — the canonical PATH MODE placements, the wrapper thin parents, the emitter parity fixtures, and (found in planning) the legacy catch leg's error-subprocess composition. | **P1** (inherits the report's label; same defect, second site) | emitted XML or graph validity | DC-175-A | **Critical** — anchor: same source P1 label as SRC-175-1; this is the SECOND instance of DC-175-A, which is what makes the structural fix mandatory in this batch. | baseline `3fd5027e16d4f9fe5377884a0140909a3b4d1e67` | `fixed` — the second site, the legacy catch leg, is closed too: `catch_notify + error_subprocess_ref` now emits notify → TERMINAL call with no trailing Stop, and the two `catch_exception` compositions are refused typed and pre-mutation. The `document_cache_ref` route is the over-firing control and is unchanged in all three compositions. |
| SRC-175-3 | originating report — issue #175 body, §3(b) | "for a non-returning child the grammar **forces** authoring the exact shape the platform forbids" — the PATH MODE rule requires a stop terminal, and the node model has no field, and no cross-component check, that could distinguish a returning child from a non-returning one. | **P1** (inherits the report's label) | capability reachability | DC-175-B | **Critical** — anchor: same source P1 label; the grammar is what makes the invalid shape the ONLY authorable one. | baseline `3fd5027e16d4f9fe5377884a0140909a3b4d1e67` | `fixed` — `process_call` moved from the body STEP unions to the TERMINAL unions and the root became the exact singleton, so the shape the platform forbids is no longer authorable; every connected form raises `PROCESS_IR_CAPABILITY_PROCESS_CALL_RETURN_PATH_BINDING_UNSUPPORTED`. Live QA confirmed all three direct-ProcessIR forms serve that code at compile with `mutation_performed: false`. |
| SRC-175-4 | originating report — issue #175 body, §3(c) | "the check only *permits* terminal-ness — it never flags the inconsistent inverse (empty `returnpaths` **plus** an outgoing dragpoint), so our contradictory output passes `verify_process_graph` clean." | **P1** (inherits the report's label) | emitted XML or graph validity | DC-175-A | **Critical** — anchor: same source P1 label. Closing this row IS the DC-175-A structural fix; it is recorded separately from SRC-175-1/2 because it names the one-sided guard rather than an emission site. | baseline `3fd5027e16d4f9fe5377884a0140909a3b4d1e67` | `fixed` — this row IS the DC-175-A structural fix. The invariant (`no declared return path ⇒ no outgoing connection`) is enforced at the one place every emitted graph passes through, and is proven non-vacuous in BOTH directions against a UI-built capture: it stays clean on the live connected call, fires when only that call's return path is cleared, and goes clean again on a byte-identical restore. Live QA independently reached the code at the public boundary. |
| QA-175-r1-01 | Stage-1 QA — `agents/reports/2026-08-20-issue-175-r1.md`, live `renera`, one engagement | "the `catch_exception` + `error_subprocess_ref` refusal has no plan-time gate … `plan` returns `planned_action: \"create\"`, and apply really creates the preceding component (inventory 26→27) before refusing" | **High** | machine-served contracts + mutation accounting (a refusal that fires after a write) | DC-175-E | **Critical** — anchor: source label High derives to Critical per the tier rule. | slice tip (uncommitted) | `fixed` — the rule now lives in ONE function, `_process_call_catch_composition_error`, which BOTH the plan-time validator and the emit-time guard call. Verified closed by re-measuring: `validate_config` now returns `PROCESS_CALL_CONFIG_INVALID` on `reliability.catch_exception`. |
| QA-175-r1-02 | Stage-1 QA — same engagement; reproduced on four distinct failure modes | "`results[key][\"status\"]` is derived from the requested action, so a failed step reads `\"status\": \"created\", \"component_id\": null`" | **Medium** | **mutation accounting** | DC-175-F | **Critical** — anchor: the tier rule makes ANY validated finding in mutation accounting critical, regardless of the source label. Not deferrable. | slice tip (uncommitted) | `fixed` — the apply loop now derives status from the OUTCOME (`refused` when the step provably wrote nothing, else `failed`), matching the derivation that already existed in `_execute_canonical_process`. PRE-EXISTING and outside the slice diff; fixed rather than deferred because a mutation-accounting misreport is critical by class and #175 increases its reachability. |
| QA-175-r1-03 | Stage-1 QA — same engagement | "the `database_to_api_sync` schema template never publishes `PROCESS_CALL_CONFIG_INVALID`, and its #108 note still advertises `[notify ->] [dlq route ->] exception` (now refused) while its #89 note still promises a catch-row Stop that is no longer emitted" | **High** | machine-served schemas/contracts | DC-175-E | **Critical** — anchor: source label High. | slice tip (uncommitted) | `fixed` — the sync protocol now publishes the code, and both composition notes state the error-subprocess exclusion. Covered structurally by a check that DERIVES the served-code expectation by driving each builder, rather than re-asserting a hand-listed one. |
| L2-r1-01 | Stage-2 repo Codex review, run dir `cdx-review.bMSYKi`, base `3fd5027`, head `3c07ad2`, `STATUS: completed`, teardown `confirmed stopped` | "[P2] Require a real Process Call return-path declaration — when raw or escape-hatch XML contains `<returnpaths><returnpaths/></returnpaths>` (or any unrelated child) plus an outgoing dragpoint, this returns true even though no non-empty `childShapeName` is declared … `verify_process_graph` reports no errors for a continuation the platform cannot bind" | **P2** | emitted XML or graph validity | DC-175-A | Standard — anchor: source label P2; not in a critical class and no critical anchor. | `3c07ad2` + this fix | `fixed` — the check asked "does the element have any children?" rather than "is any return path BINDABLE". Verified by reproduction before the fix (three malformed shapes escaped) and after (all four are flagged), with the real platform form and the live m11 capture staying clean in both runs — the discriminator that stops the tightening from degenerating into "flag every call with an edge". The bindability test is derived from the capture, which is why `childShapeName` is required and `returnLabel`, legitimately empty there, is not. |
| L2-r2-01 | Stage-2 repo Codex review (fix-only, auto scope), run dir `cdx-review.tAoZSM`, scope `working tree diff` on head `3c07ad2` (dirty), `STATUS: completed`, teardown `confirmed stopped` | "[P2] Match return paths to outgoing dragpoint identifiers — when a declared return path does not match an outgoing dragpoint `identifier` (or the dragpoint omits it), this predicate still returns true … the newly added positive test is actually malformed because its base fixture's dragpoint has no identifier" | **P2** | emitted XML or graph validity | DC-175-A | Standard — anchor: source label P2; no critical class or anchor. | this fix (uncommitted at review time) | `fixed` — BOTH halves were correct. A declared return path is not a BOUND one: the platform attributes an edge to a branch through `dragpoint/@identifier` carrying that branch's `childShapeName`, which the live capture pairs exactly. Pass 2a′ now checks declaration and correspondence as SEPARATE conditions, leaving `_is_terminal` (a question about the declaration, not the edge) unchanged. The test half was the sharper catch: my "valid connected call" fixture set the declaration and left the base fixture's identifier-less dragpoint, asserting clean for a shape the live evidence says is invalid — a fixture written from memory, which is the #146 trap. Measured after: no-identifier and wrong-identifier are flagged, matching-identifier is clean, and both live captures stay clean. |
| L2-r3-01 | Stage-2 repo Codex review (fix-only), run dir `cdx-review.mVWUMU`, base `3c07ad2`, head `45ceecb`, `STATUS: completed`, teardown `confirmed stopped` | "[P2] Validate every Process Call dragpoint binding — when a Process Call has multiple outgoing dragpoints and only one identifier matches a declared `childShapeName`, this `any(...)` suppresses the error for the entire shape … a mismatched or identifier-less branch is treated as reachable even though the platform drops it" | **P2** | emitted XML or graph validity | DC-175-A | Standard — anchor: source label P2; no critical class or anchor. | this fix | `fixed` — reproduced first: a mixed set (one attributed edge, one not) passed the check entirely. The response is the CP-1 named correction rather than a third condition: the guard is now ONE universal statement — every outgoing connection must be attributed to a return path the call declares — and the no-declaration case falls out of it (the declared set is empty, so every edge is unbound) instead of needing its own branch. Measured: all five shapes from rounds 1–3 are flagged by the single rule, the valid form is clean, a legitimate multi-return fan-out with every branch attributed is clean, and both live captures are clean. The message still discriminates the two remedies and now names the unattributed target. |
| L2-r4-01 | Stage-2 repo Codex review (fix-only), run dir `cdx-review.ALvWTM`, base `45ceecb`, head `a4d62c9`, `STATUS: completed`, teardown `confirmed stopped` | "[P2] Identify the unbound dragpoint rather than the shared target — when a valid and an invalid dragpoint both point to the same downstream shape … `targets` collapses them to `shape3`, so this message says the connections to `shape3` are unattributed and that the shape is unreachable. The valid `shape233` dragpoint still reaches it" | **P2** | machine-served schemas/contracts | DC-175-G | Standard — anchor: source label P2; no critical class or anchor. The RULE was not disputed and did not change; this is the served text only. | this fix | `fixed` — measured before: the message named `shape3` and claimed it unreachable while the bound dragpoint still reached it, and never named `shape2.dragpoint2`. Now names the connection and its identifier state, and claims only that the platform drops an unattributed connection. The test that induced the wrong message was corrected too, and now pins the inverse: the bound connection must NOT be named, and the shared target must NOT appear. |

Dispositions: `fixed` · `finding-refuted` · `severity-refuted` · `not-validated` ·
`deferred` (issue, reason class, placement). A refutation names the disputed claim and the
concrete evidence. An original label is never edited — a revision is a new dated line with the
original retained.

## Checkpoints (a row is written IN FLIGHT at every third evaluation of each loop — 3, 6, 9, …
## — in the batch it governs, never reconstructed at close)

| Loop | Evaluation (window / cumulative) | SHA (+dirty) | Outcome | Rationale |
| --- | --- | --- | --- | --- |
| L2 (Stage-2 repo Codex review) | 3 / 3 | `45ceecb` (clean) | **CONTINUE** | **Per-tier counts:** zero Critical, one Standard (L2-r3-01, P2, emitted XML/graph validity). Every Critical row in this slice is already `fixed`. **Breadth:** NARROWING, not spreading — all three L2 findings land in one defect class (DC-175-A), one file, one function; rounds 2 and 3 are the same condition. **Classes:** no NEW class in three rounds; DC-175-A recurring, each instance resolved. **Trend vector:** highest unrefuted severity flat at P2 (never critical); unresolved count flat at 1; breadth narrowing. **The honest risk, named:** three successive strengthenings of one predicate — declaration validity, then correspondence existence, then correspondence universality — IS the instance-patch pattern, and a fourth condition would confirm it. Each round found a WEAKER form of one rule rather than a different defect, which is the "count fix KINDS, not findings" signal. **Named finite next correction:** restate the guard as ONE universal invariant — every outgoing connection of a Process Call must be attributed to a return path that call declares — which makes all three round-findings unwritable instead of adding a third condition. Not a widening of samples; a single statement that subsumes them. **Rule-outs:** CLOSE-CLEAN is unavailable (validated blocking residue exists). DEFER-STANDARD-AND-PROCEED is legal here (zero critical residue, not the last owed gate) but refused: the residue is a hole in the very guard this slice exists to add, and deferring it would ship a guard that certifies what it was built to reject. ESCALATE-OPEN is unwarranted — validation is available, severity is unambiguous, and the next action is concrete. |

Each rationale records: per-tier counts and breadth, new/resolved/recurring defect classes
(derived from the rows), the trend vector, explicit rule-outs of the other outcomes, and a
NAMED finite next correction. The decision comes after the owed validation and before the next
mutation.

## Deferrals

Pointer-only — reason class, placement, and lineage live on the finding row and in the filed
issue. The one deferral this slice records at instantiation (returning-child return-path
binding → #176, `out-of-scope-by-design`) is described in the Deferred-capability section
above; it carries no finding row because it is a capability transfer decided at design time,
not a validated finding set aside. Any finding later deferred gets its own row and its own
lineage. `window-exhausted` is single-use per finding.

## Evidence index

Collected run directories are archived (byte-verified, allowlisted sidecars) under
`docs/architecture/evidence/issue-175/` with `index.jsonl` + `SHA256SUMS`, in the batch that
collects them. Run citations always use the COMPLETE run-dir name, backticked. The archive is
seeded at instantiation with the pre-baseline frozen bytes described in the trust boundary —
the negative-only witnesses whose whole value is that they were captured BEFORE the first
source edit and can never be regenerated afterwards.

## Final-tree validation (filled at close; every roster gate current on the FINAL sha)

| Gate | Evidence (quoted output / run URL / archived round) | SHA |
| --- | --- | --- |

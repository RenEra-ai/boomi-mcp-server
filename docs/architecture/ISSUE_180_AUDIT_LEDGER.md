# Issue #180 — M12.16 follow-up: prove the effect-declaration channel at the public boundary

Audit record for the completion workflow (`CLAUDE.md`, amended 2026-08-12 / -08-14; standing
rules in `docs/architecture/COMPLETION_WORKFLOW_RULES.md`).

This slice discharges the residue #154 deferred at its CP-13 (`ARCH-154-e3-07`, reason class
`window-exhausted`). It is worked into #154's tree by owner instruction rather than by reopening
#154, which stays CLOSED.

## Stage-1 step 0 — baseline

| Field | Value |
| --- | --- |
| Baseline SHA (`$BASELINE`) | `245d7d7961675a847c0694b93b33758118d26a38` |
| Branch | `codex/issue-180` |
| Branch point | `origin/dev` @ `245d7d79` (tip of #154) |
| Baseline suite | 10484 passed, 17 skipped (10501 collected) — the #154 closing wave-gate figure at this exact tip |
| Ledger instantiated | AFTER the first edits, not at step 0 — recorded as a process deviation below rather than backdated |

**Process deviation, recorded rather than hidden.** The baseline SHA was printed and kept before
any edit, which is the load-bearing half of step 0. The ledger file itself was created once the
public-boundary tests were already green. Nothing was backdated: every row below carries the SHA or
delta it was found against.

## Evidence archive

Durable gate evidence for this slice is archived under `docs/architecture/evidence/issue-180/`.
Every attestation this ledger cites resolves there: the collector-written artifact, its
`attestation.json`, and the prompts the gate actually ran. The archive is instantiated with the
ledger (header-only `index.jsonl` + `SHA256SUMS`).

## Loop roster (enumerated in advance, before the first correction)

| # | Loop identity | Authority | Scope | Window |
| --- | --- | --- | --- | --- |
| 1 | Stage-1 QA | `boomi-qa-tester` via the public MCP tool boundary | this slice | severity-aware checkpoint every 3rd evaluation |
| 2 | Stage-2 repo commit review | Codex detached review, `CLAUDE.md` §5 recipe | slice, then fix deltas | severity-aware checkpoint every 3rd evaluation |
| 3 | Composite wave gate | `scripts/wave_gate.py wave` | wave — suite, goldens x2, fingerprint seam | severity-aware checkpoint every 3rd evaluation |
| 4 | Terminal correction loop | only if a final non-blocking batch mutates the tree | that batch | severity-aware checkpoint |

Declared order: **1 -> 2 -> 3**. There is no §6 architect implementation review on this roster: no
`/codex-issue` run wraps this slice, so no additive gate is owed. A gate not on this list cannot
mint a loop mid-run.

## Finding ledger

One row per raw finding. Columns per the audit-record rule: source ID + verbatim summary, source
gate / run directory / attestation, original severity label, blocking class, defect class, derived
tier + anchor, affected SHA/delta, exactly one disposition.

| ID | Verbatim summary | Source gate / run dir | Orig. label | Blocking class | Defect class | Derived tier (anchor) | Affected SHA | Disposition |
| --- | --- | --- | --- | --- | --- | --- | --- | --- |
| SELF-180-01 | "the effect-declaration channel never reaches apply" — `materialize_canonical_process_xml` recompiles the plan's root against real ids with no trusted context, so a root whose declaration turned a blocking finding into a warning plans clean, compiles clean, and then dies at materialization with `PROCESS_IR_SEMANTIC_LINEAGE_PROPERTY_READ_BEFORE_WRITE`. Found while building the first effect golden through the public chain — the exact work #180 exists to do. | Implementation, this slice; measured by driving `compile_authoring_request_v1` and handing the resulting plan to the materializer | (self-found; no gate label) | runtime behavior; capability reachability; apply/update preservation | DC-154-A (server-built trusted context threaded to SOME compile sites, authority = the set of compile entries a root passes through) — **second instance**, first was `QA-154-r1-01` | Standard — anchor: no critical class (no secrets/security, data loss, or mutation accounting) and no P0/P1/Critical/High source label. Fixed rather than deferred. | `245d7d7` -> fixed | `fixed` **structurally**. The plan now RECORDS the context it was compiled under (`ProcessComponentMaterializationPlanV1.effect_capabilities`, covered by the fingerprint), and the materializer reads it off the plan — so there is no argument a future recompile site can forget to pass. **Sibling sweep + invariant:** `tests/test_issue_180_compile_entry_context.py` derives the entry set from the compiler's own signatures, parses `src/` for every call site, and requires each to supply context or appear in the strict-by-design allowlist with a reason; the one exemption is the legacy dialect adapter, which has no declarations to resolve. A stale exemption is itself a failure. **Non-vacuity witness:** `test_the_check_reports_a_strict_call_it_has_not_been_told_about` feeds the checker a synthetic strict call and requires it reported, with four silent controls. **Coverage claim:** the authority's full case set is every public pipeline function carrying a `capabilities` parameter (five today), asserted non-empty and asserted to contain the three the authoring path uses. |
| SELF-180-02 | "the ledger's note beside the first instance enumerated the compile sites, and the enumeration was wrong" — `process_materialization.py` records "this compile is the THIRD one the authoring path runs for a root, and it was the only one still strict". There is a fourth. | Implementation, this slice, while reconciling `SELF-180-01` | (self-found; no gate label) | machine-served schemas/contracts (in-tree claim, not served text) | DC-154-A (same pair — the claim IS the enumeration the structural fix replaces) | Standard — anchor: no critical class or label. | `245d7d7` -> fixed | `fixed` — the comment no longer counts compiles; it points at the invariant that enumerates them. The false claim is retained here rather than silently overwritten, per the #154 pattern for corrected closures. |
| SELF-180-03 | "a `.md` provenance file placed in `tests/fixtures/golden_xml/` fails the wave gate" — `check_golden_tree` refuses any non-`.xml` entry in that directory with `GOLDEN_FILE_UNDECLARED`, and the ordinary suite does NOT catch it (its set comparison only looks at `.xml`). | Implementation, this slice; caught by reading `scripts/wave_gate.py:1334` before running the gate | (self-found; no gate label) | (not a blocking class — a fixture-location mistake in this slice's own new material) | n/a (single instance, no mechanism/authority pair) | Standard — anchor: none; recorded for completeness because it would have failed a required gate. | uncommitted -> fixed | `fixed` — provenance moved to `tests/fixtures/process_ir/issue180/PROVENANCE.md`, matching where #154 keeps its own. |

## Checkpoint records

*(To be appended as evaluations complete. No checkpoint is due before the first gate result.)*

## Validation evidence (chronological)

| # | Loop / scope | Delta | Evidence | Result |
| --- | --- | --- | --- | --- |
| 1 | Pre-gate developer run — affected modules only, NOT a gate | working tree vs `245d7d7` | `pytest` over the eleven affected modules, local `.venv` 3.12, `-p no:randomly` | 762 passed |
| 2 | Pre-gate developer run — served-digest freeze | working tree vs `245d7d7` | `tests/test_issue_149_legacy_reachability_freeze.py`, `test_m12_11_revision_binding.py`, both authoring-contract modules | 372 passed — the new covered plan field drifts no served revision digest |
| 3 | Manifest pre-check — NOT a gate | working tree vs `245d7d7` | `scripts/wave_gate.py manifests --base 245d7d79...` | `manifests ok (10501 required nodes, 70 active goldens)` |

## Recorded limitations

* **The registered-script family has no public verdict change, and cannot have one at this HEAD.**
  the shipped vetted-script registry is empty by design, and `_validate_processes` takes no registry
  parameter, so a public caller reaches only the empty production table. Every well-formed script
  declaration is therefore INERT through the public entry. What the slice proves instead is that the
  public path really consults the script authority — a mismatched digest is refused publicly, which
  is only reachable if the branch ran. `test_a_script_declaration_reaches_the_public_boundary_and_is_inert_here`
  pins the emptiness to the shipped registry object itself and fails loudly the day a script is
  vetted, so the limitation cannot outlive its cause unnoticed.
* **The effect goldens are regression pins, not oracles.** They are rendered by the compiler and the
  canonical materializer. Provenance, and what they do not attest, are recorded in
  `tests/fixtures/process_ir/issue180/PROVENANCE.md`.
* **A map declaration is inert under `conflict_policy="reuse"`.** The plan may substitute an
  existing component, so derivation declines to speak for the authored config. This is the correct
  answer rather than a gap, and it has its own public test
  (`test_a_map_declaration_is_inert_when_the_plan_may_substitute_the_component`) with the `fail`
  control beside it.

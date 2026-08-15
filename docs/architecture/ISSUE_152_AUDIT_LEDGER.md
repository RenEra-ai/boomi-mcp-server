# Issue #152 (M12.13) — slice audit ledger

Durable, in-tree record for the completion workflow in `CLAUDE.md`. It lives under
`docs/architecture/` because that is the repository's only tracked documentation
carve-out (`.gitignore:105`), and the amended policy requires a deferral's cited
checkpoint to be in-tree — extending the carve-out to a new `docs/audit/` would have
changed the repo's documentation policy, which is an owner decision, not a slice's. The deferrals filed
from this slice cite the checkpoints below, and the amended policy requires a cited
checkpoint to exist as an in-tree ledger row at the time the deferral is recorded.

Baseline (Stage 1 step 0): `9080e3c2d0fcc82b01f781b2352d60995ba58ad8`.
`dev` carries neither manifest at that commit, so the landing push is a BOOTSTRAP and
`validate_transition()` does not run for it — ratified by the attested §6 gate.

## Loop roster (enumerated in advance)

1. Stage-1 QA — dark slice, darkness proof per round (no `src`/golden/`examples` delta).
2. Stage-2 repo Codex review — delta-scoped per the Critical scoping rule.
3. §6 architect implementation-vs-plan review (additive, declared by the wrapping
   `/codex-issue` pipeline).
4. Composite wave gate — full suite, all 60 goldens, determinism, manifests.

## Defect classes observed (mechanism, runtime-authority)

A defect class is a `(mechanism, runtime-authority)` pair. Instances are counted
post-reconciliation across all roster gates; `finding-refuted` rows never count.

| # | Defect class | Instances | Resolution |
| --- | --- | --- | --- |
| DC-1 | (compare path **strings**, filesystem identity) | 2 | **structural** — `(st_dev, st_ino)` identity, ancestry walked by `..` through descriptors. Sibling sweep: no path-string comparison remains in any security decision (only `os.path.dirname` locating the repo root from `__file__`). |
| DC-2 | (exception escaping a boundary, **process exit status**) | 7 | **structural** — outermost `except BaseException` in `main()`; see below. |
| DC-3 | (destructive op resolving a **mutable name**, filesystem object identity) | 7 | **structural** — descriptor-anchored removal + post-hoc outcome proof. Irreducible residual → **#164**. |
| DC-4 | (ownership **asserted** rather than established, exclusive creation) | 2 | **structural** — `O_EXCL`/`O_NOFOLLOW`; record appended only after the exclusive create. Sibling sweep: every create site is exclusive (`O_EXCL`, `open(...,"x")`, `os.mkdir`). |
| DC-5 | (bool/int conflation in a discriminator, Python type semantics) | 2 | **structural** — `type(x) is int`. Sibling sweep: line 647 already excludes `bool`; all other coercions are `isinstance(..., str)`, and `bool` is not a `str` subclass. |
| DC-6 | (test harness matching a **call shape that moved**, the code under test) | 3 | **structural** — every negative test with a CONDITIONAL patch carries an explicit `assert <hook> fired`. Sibling sweep: 16 candidates enumerated, 2 conditional (fixed), 14 unconditional/env-only and therefore non-vacuous by construction — a patch that never fires makes the assertion fail. |
| DC-7 | (served prose drifting from code, the code) | 4 | instance-fixed; the diagnostic-code roster is pinned by `test_every_diagnostic_code_the_gate_can_raise_is_documented`, prose is not mechanically pinned. |
| DC-8 | (a fix silently removing a property an earlier round established, that property's own test) | 3 | **structural** — every such property now has a named regression that fails when it is removed. The three that regressed were each pinned only by the narrower test of the round that introduced them. |
| DC-9 | (a SHAPE test standing in for membership in a closed set, the set itself) | 3 | **structural** — `DIAGNOSTIC_CODES` is the single authority, pinned bidirectionally against the source and the docs roster; `_own_code()` requires the exact builtin `str`. Sibling sweep: the other nine `isinstance(..., str)` sites validate JSON-derived values, which `json.loads` guarantees are exactly `str` (measured), so only the provider-facing site needed `type(...) is str`. |

### DC-2 — the sweep that ended it

Five instances, each one statement further out, each found by a separate review round:
`dispose()` totality; its `close()` outside the guard; `execute()` re-raising the
original exception; the CLOSING fingerprint outside the boundary; and — found by the
sibling sweep rather than a sixth round — the OPENING fingerprint and everything else
before `execute()`'s inner try. Measured before the fix:
`SystemExit ESCAPED main() with code: 0 -> the process exits 0`.

Each earlier fix guarded a REGION; the class is about the PROCESS boundary. The invariant
is now stated once, at the only place that can enforce it: **the gate decides its exit
status, and an exception never decides it for the gate.**

* **Sibling sweep** — all 51 `try` blocks enumerated; the 8 that decide exit status or
  hold evidence reviewed; the outermost handler subsumes the rest.
* **Non-vacuity witness** — `SystemExit(0)` from the opening fingerprint, a concrete case
  the invariant excludes and which previously exited green.
A **sixth** instance was then found in the new handler itself: reporting runs INSIDE an
`except` suite, where a raise escapes the enclosing `try`, so an `_emit` that throws
exited green from the very handler meant to prevent it — and the older
`GATE_DIAGNOSTIC_UNRENDERABLE` fallback had the same shape. Every `_emit` inside an
`except` suite now goes through `_report`, which cannot throw. Rendering (which can run
foreign `__str__`/`__format__`) stays separately guarded, and the exit status is decided
before either. Measured after: throwing sink + unexpected error → 1; throwing sink +
ordinary `GateFailure` → 2.

* **Coverage claim** — the authority's full case set is every exception that can cross
  `main()`: ordinary `Exception`; `BaseException` that is not `Exception`
  (`SystemExit`, `KeyboardInterrupt`, `GeneratorExit`); and one whose rendering itself
  misbehaves. All five are parametrized in
  `test_no_exception_can_decide_the_gates_exit_status`, and the boundary reads nothing
  about the object, so no case can distinguish itself by its own code.

## Deferrals (reason class and placement recorded at filing)

| Issue | Findings | Reason class | Placement | Lineage |
| --- | --- | --- | --- | --- |
| **#164** | DC-3 residual: `_removal_proved`'s two observations, and owned-child bindings during deletion — both sites enumerated in the issue body | `blocked-by-mechanism` — POSIX offers no remove-by-descriptor and no atomic multi-namespace observation, so the class cannot be closed by another check | **after #160 lands, before M12 close** — the roadmap owner's slotting, recorded in the issue title. An earlier revision of this row inferred "independent of the chain" from the measured bound; where the two differ the owner's slotting governs, and it is recorded here rather than argued with | first deferral |
| **#165** | Golden-corpus authority direction: the registry imports the thirteen producer test modules instead of those tests consuming it | `out-of-scope-by-design` — recorded in `.codex/plans/issue-152.claude.md` ("build the registry, DEFER the thirteen-module refactor, file a follow-up issue") | **after #152 lands, before #159 starts** — the roadmap owner's slotting, recorded in the issue title, and consistent with the technical reason: #159's `transitional_oracle` and #160's `deletion_only` exist to keep goldens running after the owning tests are removed, which is exactly the removal this direction breaks | first deferral |

Neither is `window-exhausted`; neither is debt minted to end a loop.

**Bound on the #164 residual, measured and pinned by
`test_content_moved_into_the_worktree_is_caught_by_the_fingerprint`:**

```
A) a NON-empty directory moved into the repo : WORKTREE_DIRTY -> gate red
B) an EMPTY directory moved into the repo    : BLIND (git does not track empty dirs)
C) the child-binding proof on a renamed child: _ForeignEntry raised -> refused
```

Content cannot be smuggled past the gate; only an empty untracked directory is invisible.

## Checkpoints

| Loop | Evaluation | SHA | Outcome | Rationale |
| --- | --- | --- | --- | --- |
| Stage-2 repo review | 9 | `4784406` | `CLOSE-CLEAN` | zero residue; DC-1 closed structurally |
| Stage-2 repo review | 12 | `6176ff9` | `CONTINUE` | DC-3 recurring → structural fix named and applied (no destructive op resolves a pathname) |
| Stage-2 repo review | 17 | `58d6ed0` | `DEFER-STANDARD-AND-PROCEED` | zero critical residue; DC-3 remainder → #164; §6 still owed |
| §6 architect review | 9 (attested, `/tmp/cdx-gate-review.Eredut`) | `1083413` | `CONTINUE` | five findings, all fixed; the gate ratified the bootstrap judgment; DC-3 remainder already deferred to #164, DC-6 direction to #165 |
| §6 architect review | 10 (attested, `/tmp/cdx-gate-review.kkeLZP`) | `96ead47` | `CONTINUE` | three findings, all fixed; named next action was the DC-2 sibling sweep, which then found the fifth instance |
| Stage-2 repo review | 30 | `706ec9d` | **`CLOSE-CLEAN`** | no findings; first clean repo review after the 2026-08-14 amendment |
| §6 architect review | 11 (attested, `/tmp/cdx-gate-review.Hj8pIv`) | `706ec9d` | `CONTINUE` | four findings; one fail-open fixed (provider `str` subclass), one refuted on evidence, two ledger corrections applied here |

## Full finding rows

The per-finding rows (stable source ID, verbatim summary, source gate + run directory +
attestation, original label, blocking class, defect class, derived tier with anchor,
affected SHA, and exactly one disposition) are reproduced in the slice's final report.
Attested gate artifacts live in their run directories under `/tmp/cdx-gate-review.*` and
`/tmp/cdx-review.*` for the duration of the session; the durable summary is this file.

## Closing checkpoint — deferrals re-decided against an in-tree row

**Defect in the first recording, and its remedy.** Both deferral records (#164, #165)
were written into their issue bodies BEFORE this ledger first entered the tree, so at the
moment each deferral was recorded its cited checkpoint did not yet exist as an in-tree
row — the exact ordering the amended policy forbids. The remedy is not to argue the
citation was "morally" present: it is to record a compliant checkpoint now, with the
ledger already committed, and re-decide both deferrals against it.

**Checkpoint — slice #152, closing.**

- **Loops covered:** Stage-1 QA (darkness proofs, dark slice throughout), Stage-2 repo
  Codex review (30 evaluations), §6 architect review (11 evaluations, rounds 9–11
  attested), composite wave gate.
- **Tree:** `docs/architecture/ISSUE_152_AUDIT_LEDGER.md` present and committed before
  this decision; deferral citations updated only afterwards.
- **Per-tier residue:** critical **0** unresolved. Standard: 0 unresolved in-slice; two
  deferred, below.
- **Validation current on this tree:** `ci` (push, `before=9080e3c`) → BOOTSTRAP,
  manifests ok, collection ok, suite green, exit 0; `wave --base 9080e3c --bootstrap` →
  60 goldens deterministic and byte-exact, exit 0; Stage-2 round 30 CLEAN.

**Deferrals, re-decided:**

| Issue | Reason class | Placement (roadmap owner's slotting) | Lineage |
| --- | --- | --- | --- |
| **#164** | `blocked-by-mechanism` — POSIX offers no remove-by-descriptor and no atomic multi-namespace observation | after #160 lands, before M12 close | first deferral; NOT `window-exhausted` |
| **#165** | `out-of-scope-by-design` — cited to `.codex/plans/issue-152.claude.md` | after #152 lands, before #159 starts | first deferral; NOT `window-exhausted` |

Both cite THIS checkpoint row, which exists in-tree at the moment of the citation.

*A review round asserted that #165's body had been "re-decided as the single permitted
`window-exhausted` deferral". That text does not exist in the issue; its body reads
`out-of-scope-by-design` … `Lineage: first deferral. Not window-exhausted.` Recorded as
`finding-refuted` on the issue body itself as evidence.*

## Owed after landing (NOT satisfied by this slice)

The architect plan's Definition of Done includes branch protection requiring the
`Python 3.11 non-KB` check on `dev`. Measured: `gh api repos/.../rulesets` returns `[]` —
**no ruleset exists**, so `README.md:594` stating the requirement is not yet configured is
ACCURATE, not stale. The check must run at least once before GitHub will accept it as a
required status check, so the order is: land → the workflow runs → configure the ruleset.
That final step is a repository-settings change and is called out explicitly in the
slice's final report rather than silently claimed as done.

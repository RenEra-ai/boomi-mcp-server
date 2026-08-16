# Round-1 substitute review — verdicts and findings (issue #165)

Reviewed delta: `da320eb08e5f5d9ba55a0d7b54ccbb4d28d01bbb..7f20ea8d9fe99c226f874ddad6af552cc519f768`.
Three independent fresh-context Claude reviewer agents, read-only, one charter each
(`lens-charters.md`). No Codex was used and no Codex attestation is claimed — see the
Deviation section of `docs/architecture/ISSUE_165_AUDIT_LEDGER.md`.

This file records the verdicts and the finding set as reconciled into the ledger. The
reviewers' full narrative reports are session artifacts; every finding they raised is
carried as a ledger row (R1-1 … R1-10) with its own disposition, so nothing rests on
this summary.

## Verdicts

| Lens | Charter | Verdict |
| --- | --- | --- |
| A | golden integrity / behaviour preservation | `ISSUES FOUND` |
| B | duplication + authority audit (criterion 2) | `ISSUES FOUND` |
| C | adversarial vacuity + record integrity | `ISSUES FOUND` |

## Findings raised (ledger rows in parentheses)

- Inert non-vacuity witness in the new criterion-3 regression — raised INDEPENDENTLY by
  all three lenses, counted as one instance (R1-1, P2).
- "thirteen" restated as fact about the post-inversion tree; 11 modules actually consume
  the corpus (R1-2, P2, lens C).
- `RECIPE_GOLDEN_ARMS` comment overclaims; only `"fanout"` discriminated (R1-3, P3, lens B).
- Three test-only constants migrated into the corpus (R1-4, P3, lens B).
- Dead aliases and imports left behind by the inversion (R1-5, P3, lens B).
- `_linear_with_map()` lost its byte anchor (R1-6, P3, lenses A + B).
- Deep-copy CONTRACT bullet untrue of three renderers (R1-7, P3, lens A).
- Deviation precedent not locatable in the #171 ledger or archive (R1-8, P3, lens C).
- Synthetic `generated_at` in the archive index (R1-9, P3, lens C).
- Stale `_render_env` docstring rationale in `scripts/wave_gate.py` (R1-10, P3, lens A note).

## What the lenses independently VERIFIED as clean

Recorded because it is the evidence the acceptance criteria rest on, and it was produced
by reviewers rather than by the author:

- All 60 goldens render byte-identically between `da320eb` and `7f20ea8` — reproduced by
  lenses A and B independently, lens A by diffing full base64 payloads (not only hashes).
- pytest node ids unchanged in COLLECTION ORDER, exactly one node added.
- Import spelling correct per section against each owning module's own imports.
- Corpus imports zero test modules; the no-pytest render contract holds (lens A rendered
  all 60 cases with `pytest`/`_pytest`/`py` blocked at `meta_path`).
- Every JSON-fixture witness is pinned by a real equality test (lens B named each one).
- Manifest transaction exact; `manifests --base` exit 0; measurements reproduce; archive
  checksums verify.
- Mutation-hardness: lens B killed 111 of 114 mutations of the corpus's invocation
  literals and id constants via committed golden bytes; the three survivors are R1-4.

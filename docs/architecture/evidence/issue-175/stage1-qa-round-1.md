# Stage-1 QA round 1 — outcome summary

Full report (gitignored, agent-owned): `agents/reports/2026-08-20-issue-175-r1.md`.
One live engagement on the disposable `renera` account, through the public MCP tool
boundary. This file records the load-bearing outcomes so the audit record does not
depend on a gitignored file.

## The P1 gate — the probe that could have changed the slice

The pre-fix artifact could not be rebuilt by the fixed code, so the probe submitted the
FROZEN pre-baseline bytes verbatim (`prefix-goldens/processcall_standalone_parent.xml`) —
the exact component this repo shipped — aimed at a child proven to return no documents by
reading the CHILD's stored graph for the ABSENCE of a `returndocuments` shape.

- **(a) Boomi strips the dragpoint — REFUTED.** One canonical projection applied to BOTH
  byte streams returned `canonical_graph_equal = True`; the stored form keeps the call's
  `dragpoint toShape="shape3"` and the trailing `stop`. Reconfirmed through a second,
  independent write path. **Consequence: no P0 escalation, and no mutation-accounting or
  apply-preservation class arises from platform storage.**
- **(d) the downstream shape executes — REFUTED, with a positive control.** A Stop emits
  no signal, so the probe used a shape-graph-identical variant ending in an Exception
  carrying a canary:

  | | graph | execution |
  |---|---|---|
  | CONTROL | `start → message → exception` | **ERROR**, canary message verbatim |
  | PROBE | `start → processcall(empty returnpaths, dragpoint→shape3) → exception` | **COMPLETE**, 0 error docs, no error record |

  This is a STRONGER result than the issue reported: the connection is dead at RUNTIME,
  not merely unrendered on the canvas.
- **(b) vs (c) — INCONCLUSIVE.** They differ only in whether the canvas draws the edge,
  and the browser automation was unavailable. Recorded as open rather than guessed;
  carried to #176. Both branches leave the premise intact.

## P2 — the new verifier rule is not refuted

Across all five UI-built `processcall` shapes in the frozen m11 captures: four carry empty
`returnpaths` AND zero dragpoints; the fifth pairs `returnpaths/@childShapeName="shape233"`
with `dragpoint/@identifier="shape233"`. **Zero counterexamples.** Boomi's own documentation
agrees: *"This new branch extends from the associated Process Call step for each new Return
Documents step."*

## P3 — post-fix, all pass

Wrapper create/update → `['start','processcall']`, empty `<returnpaths/>` and
`<dragpoints/>`, **zero stops**, verifier clean with `shapes_checked: 2`. Live branch build
→ `['start','branch','processcall','processcall']`, zero stops, `shapes_checked: 4`. All
three direct-ProcessIR connected forms serve the return-path-binding capability code at
compile with `mutation_performed: false`. The graph-verifier code was reached and shown
non-vacuous at the public boundary.

**Explicitly measured, because it decided a conditional plan item:** updating the pre-fix
component with a corrected spec removed the orphan Stop AND its dead edge (`stop shapes
remaining: []`, `dragpoints: []`). **No `integration_builder` merge-authority change is
needed** — the plan's conditional item is closed as not-required.

**Terminality does not break invocation:** the child ran at the terminal call's shape and
minted its own execution record.

## Findings

Three, all recorded as rows in the ledger and all `fixed`: QA-175-r1-01 (missing plan-time
gate — a refusal that fired after a write), QA-175-r1-02 (pre-existing: a failed apply step
recorded as `created`), QA-175-r1-03 (sync-protocol served text and error list stale).

## Account hygiene

12 components created, **12 deleted**, 5 undeployed, process count returned to 22. Deletion
ran from an explicit allowlist with a name assertion, never harvested from apply results.
The working tree was byte-identical at session start and end.

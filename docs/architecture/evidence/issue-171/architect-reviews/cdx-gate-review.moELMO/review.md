I’ll review only `e541a3d…HEAD`, with separate passes for runtime/test semantics, workflow/docs/evidence accuracy, and manifest/acceptance arithmetic. I’ll keep this read-only and report only actionable defects.The implementation and workflow are byte-identical to the original architect-gate tip; the correction delta is confined to tests, docs, ledger, and archived evidence. I’m now checking whether the added test witness and manifest floor stayed mutually consistent with the served specification.The functional corrections check out, but the audit/evidence pass found a higher-impact inconsistency: the current tree already records this architect replay as completed even though this request is the replay and no replay artifact exists. I’m tracing every copy of that claim before finalizing the finding set.Three actionable issues remain.

1. The ledger pre-claims this architect replay. [ISSUE_171_AUDIT_LEDGER.md](/Users/gleb/Documents/Projects/Renera/boomi-mcp-server/docs/architecture/ISSUE_171_AUDIT_LEDGER.md:74), [rows R5-1/R5-2](/Users/gleb/Documents/Projects/Renera/boomi-mcp-server/docs/architecture/ISSUE_171_AUDIT_LEDGER.md:164), and [index.jsonl](/Users/gleb/Documents/Projects/Renera/boomi-mcp-server/docs/architecture/evidence/issue-171/index.jsonl:7) say the architect gate was replayed and CP-3/D-1b/D-2b exist. They do not: the archive contains only the original `cdx-gate-review.ZxeOl1`, generic support reviews, and no replay section or CP-3 rows. This falsely makes a required gate appear current. Supersede these claims as premature, archive this ISSUES FOUND replay, then record CP-3/deferral revisions only after correction, validation, and a clean architect replay.

2. A6-1’s premature-evidence claim survives. [The evidence README](/Users/gleb/Documents/Projects/Renera/boomi-mcp-server/docs/architecture/evidence/issue-171/README.md:27) and [the ledger](/Users/gleb/Documents/Projects/Renera/boomi-mcp-server/docs/architecture/ISSUE_171_AUDIT_LEDGER.md:470) state that Actions logs and no-PR queries are archived and hash-covered, but neither `actions/` nor `no-pr/` exists. Use explicit pending/future wording until criteria 4/5 artifacts are actually captured.

3. A6-4’s sibling sweep is incomplete. [tests.yml](/Users/gleb/Documents/Projects/Renera/boomi-mcp-server/.github/workflows/tests.yml:22) says every pushed ref gets a verdict, despite line 29 acknowledging that `[skip ci]` starts no run. [ENDGAME_VERIFICATION_GATE.md](/Users/gleb/Documents/Projects/Renera/boomi-mcp-server/docs/architecture/ENDGAME_VERIFICATION_GATE.md:743) and [README.md](/Users/gleb/Documents/Projects/Renera/boomi-mcp-server/README.md:622) repeat the unqualified scratch-push claim. A scratch push carrying a skip directive disproves all three. Qualify them as matching pushes that start the workflow and apply the promised provenance markers.

The functional corrections are sound:

- `scripts/wave_gate.py` and workflow behavior are byte-identical to `e541a3d`; the design constraints remain intact.
- A6-2 is fixed: [#172](https://github.com/RenEra-ai/boomi-mcp-server/issues/172) exists, is open, predates the deferral, and contains per-item criteria, reason class, lineage, and M12 placement.
- A6-3 is fixed.
- A6-5 is durable: restoring the old slice predicate fails the committed dedented-second-job test.
- A6-6’s push/PR witnesses discriminate correctly.
- Manifest arithmetic is legal: `9788 + 2 appends − 1 tombstone = 9789`; fresh collection found 9789 nodes. The rollout seed must therefore change `9789 → 99999`, despite the stale `9788` instruction in [.codex/plans/issue-171.claude.md](/Users/gleb/Documents/Projects/Renera/boomi-mcp-server/.codex/plans/issue-171.claude.md:338).
- The gate test module passes: 244 tests.

Criteria 1, 2, 3, 6, and 7 hold. Before closure, fix and validate these findings, replay this architect gate again, execute criteria 4/5 with no-PR evidence and final exact-SHA GREEN, then run the still-unrecorded composite wave gate and fill the final-tree table.

VERDICT: ISSUES FOUND

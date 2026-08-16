# Slice #165 substitute-review prompts (3 lenses)

Shared header for each agent:

Repo: /Users/gleb/Documents/Projects/Renera/boomi-mcp-server, branch codex/issue-165.
Review the committed delta `git diff da320eb08e5f5d9ba55a0d7b54ccbb4d28d01bbb..HEAD`
(use `git log --oneline da320eb..HEAD` + `git show` / Read as needed). READ-ONLY — do
not edit anything. You are an independent reviewer; the author's claims are not
evidence. Issue #165's acceptance criteria (binding): (1) tests/_wave_gate_golden_corpus.py
owns the case definitions; the thirteen golden-producing test modules consume them;
(2) no case definition exists in two places; (3) deleting an owning test module leaves
every transitional_oracle and deletion_only golden renderable, with a regression proving
it; (4) registry import cost measured and recorded, not estimated; (5)
docs/architecture/ENDGAME_VERIFICATION_GATE.md updated to the direction that ships.
Report findings as: [P1/P2/P3] file:line — claim — concrete failure scenario. End with
`VERDICT: NO ISSUES` or `VERDICT: ISSUES FOUND`.

Lens A (golden-integrity): Verify the refactor preserves behavior byte-for-byte. Check
each corpus section (A–L) against its consuming module: import SPELLING preserved per
module (src. vs bare — the dual-module hazard), deep-copy discipline where module-level
state is shared, the `CASE_REGISTRY = _build_registry()` marker still unique with no
`golden_xml` mention above it, CASE_REGISTRY still a plain dict of (renderer, factory)
2-tuples, the skip-neutralisation and --render contract untouched, aliases in test
modules complete (no stale local def shadowing a moved one), the LISTENER_CHAINS /
_WIRED_DC shared-mutable-object hazard, and the pytest node-id-bearing parametrizes
(_ANCHORS, SYNC_CASES, LISTENER_CHAINS, function-object params) unchanged in content
and order.

Lens B (duplication/authority audit): For EVERY one of the thirteen modules plus
tests/test_sync_pipeline_builder.py, hunt for a case definition that still exists in two
places or a golden test still DEFINING its input rather than consuming the corpus/JSON.
Evaluate the witness doctrine (JSON is the definition; in-test literals pinned by
equality tests) against criterion 2 — is any witness NOT pinned by an equality test?
Check the corpus's RECIPE_GOLDEN_ARMS / name derivations / invocation args for remaining
hand-copies that could drift silently (i.e. not pinned by committed golden bytes).

Lens C (adversarial vacuity): Try to REFUTE the new regression test
(tests/test_wave_gate_goldens.py::test_every_active_golden_renders_with_all_test_modules_unimportable):
can the blocker fail to arm? can a test module leak in anyway (sys.path, cwd, pyc,
parent env)? does the sha comparison actually bind to committed bytes? Also audit: the
manifest transaction (row pytest-009792, floors 9790→9791, field order, append-at-end),
the measurement claims in the corpus docstring + spec §7 vs the archived raw outputs,
ISSUE_165_AUDIT_LEDGER.md conformance to docs/architecture/templates/AUDIT_LEDGER_TEMPLATE.md
(row shape, no cdx-* citations, evidence pointer, INH seed correctness vs issue #165
body), and the spec edits (stale claims left behind? grep for "0.39", "imports the
owning test modules", "WHY IT IMPORTS").

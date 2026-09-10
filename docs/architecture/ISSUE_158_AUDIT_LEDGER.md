# Audit ledger — issue #158 (M12.20 Canonical WSS listener entry and materialization, T7)

Audit record for the completion workflow (`CLAUDE.md`, amended 2026-08-12 / -08-14; standing
rules in `docs/architecture/COMPLETION_WORKFLOW_RULES.md`).

Plan of record: `.codex/plans/issue-158.md` — the attested Codex architect plan, attestation
`.codex/plans/issue-158.attest.json` (message sha256
`f28242a0766fed357e8ac86d1f3215fc3408e09d980bd723ed0c448b7d3c14c8`, thread
`01a08cc1-3f8f-7b71-8306-41496c06b40d`, turn token 1, gate `architect`, collected
`ok:true`/`stopped:true`, teardown `confirmed`). Where the plan and this ledger disagree, the
correction is recorded here with its evidence and the plan is NOT silently re-authored.

## Stage-1 step 0 — baseline

| Field | Value |
| --- | --- |
| Issue | #158 — M12.20 Canonical WSS listener entry and materialization (T7) |
| Step-0 baseline (`$BASELINE`) | `3a8469e109cbf414aff1f9db35649ef06071fb66` |
| Branch | `codex/issue-158` |
| Branch point | `dev` @ `3a8469e` (closing commit of #157) |
| Baseline suite | 12026 passed, 19 skipped (12045 collected) — full non-KB, local `.venv` 3.12, `PYTHONPATH=src`, 976 s, run to completion BEFORE any edit, exit 0 |
| Baseline manifests | 12045 required nodes (`tests/fixtures/wave_gate/test_nodes.jsonl`), 76 active goldens (`tests/fixtures/wave_gate/goldens.jsonl`) |
| Pristine oracle tree | `git archive 3a8469e` extracted outside the repository (scratchpad) with the repo `.venv` symlinked in; the two pre-flip SOAP-target anchors are rendered from THAT extraction, never from the working tree |
| Slice kind | behaviour-affecting (new IR node kind, emitter registration, entry-shape invariants, routing-gate flips, deployment recognizers, served-contract changes) |

The collected count 12045 equals the recorded floor, so the floor is current on the branch point.

### Environment corrections recorded at step 0

1. **The first architect gate session never produced a plan.** Both turns of session
   `01a08cb7-6a39-7a42-956a-122783b8c3d6` (primary brief, then the one in-session retry) ended
   `failed` within a minute with an empty message. The Codex app-server log for that session reads
   "You've hit your usage limit … try again at Sep 14th, 2026 9:42 PM", and a direct CLI probe on a
   second model returned the same text, so the limit was account-wide rather than model-specific.
   The round was collected with `--outcome failed` (refused `declared_failed`, daemon `stopped:true`)
   and nothing from it is used. The owner restored Codex capacity the same day; a one-token probe
   confirmed it before a fresh session was started. The fresh session's primary turn certified.
2. **Gate sessions run under a Codex home with no MCP servers and no plugins.** On #157 an architect
   plan turn parked on an MCP tool-approval elicitation that a headless driver cannot answer. The
   architect brief therefore told Codex that MCP tools are unavailable and to ground the plan in the
   committed goldens, the evidence archive and the source. The repo's own Stage-2 commit review is
   unaffected by this choice and runs under the default home per `CLAUDE.md` §5b.

## Loop roster (enumerated in advance, before the first correction)

Per `CLAUDE.md` the roster is fixed before any correction is applied. A gate not on this list cannot
mint a loop mid-run; adding one is itself a recorded checkpoint decision.

| # | Logical loop | Authority | Scope |
| --- | --- | --- | --- |
| 1 | Stage-1 QA | `boomi-qa-tester`, live through the public MCP tool boundary | listener deploy + inbound payload per distinct listener family (bare WSS and API Service routes), canonical-root deployment-target selection, the named negative probes, and the scheduled-root controls |
| 2 | Stage-2 repo commit review | detached Codex review per `CLAUDE.md` §5b–5e | initial: `--base $BASELINE`; then each fix delta |
| 3 | Architect implementation review | `/codex-issue` §6 gate (`--gate review`) | implementation vs `.codex/plans/issue-158.md`; **capped at 3 evaluations** by `docs/architecture/COMPLETION_WORKFLOW_RULES.md` |
| 4 | Composite wave gate | `scripts/wave_gate.py` | full suite, golden manifest, determinism, wave-delta review, one live scenario per changed capability class |
| 5 | Terminal correction loop | only if a final non-blocking batch mutates the tree | that batch |

The pre-flip SOAP-target anchor capture below is **evidence provisioning**, not a QA evaluation:
it runs from the pristine extraction of the baseline tree with no source change and debits no loop.

## Pre-flip legacy-oracle anchors (captured before either routing gate changed)

The issue's hard constraint: capture the two missing SOAP-target listener anchors BEFORE the flip,
while the legacy renderer still emits them. After the flip no independent producer exists.

Archive: `docs/architecture/evidence/issue-158/preflip/` — the capture script, both raw inputs, both
output byte streams and a manifest carrying the baseline SHA, the interpreter version and each
output's digest. Each form was rendered twice — through `SyncPipelineBuilder.build`, which routes a
listener chain to the legacy arm at the baseline, and through `ProcessFlowBuilder.build` on the
lowered config — and the capture refuses unless the two agree byte-for-byte. They agreed.

| Chain | Stages | Component name | Bytes | sha256 |
| --- | --- | --- | --- | --- |
| `listener_soap_send` | listen → SOAP send | Sync Listener Soap Send Golden | 1562 | `a678f4831b3f5071b421649b8881cbd81227bc9e1a2bbc5b932796a1ddb766a7` |
| `listener_map_soap_send` | listen → map → SOAP send | Sync Listener Map Soap Send Golden | 1810 | `b9c50541da707cfcdd0bec2cf68aaa700626a7709c7f8fe139e68c73b77e16c0` |

Provenance class, per the `CLAUDE.md` Stage-1 fixture rule: **legacy-oracle parity capture** from a
tree causally independent of the implementation under test.

## Findings ledger

One row per raw finding. Columns: source ID · verbatim summary · source gate / run dir /
attestation · original severity label · blocking class · defect class · derived tier + anchor ·
affected SHA/delta · disposition.

| ID | Summary (verbatim) | Source / run dir | Orig. label | Blocking class | Defect class | Tier (anchor) | Affected SHA | Disposition |
| --- | --- | --- | --- | --- | --- | --- | --- | --- |

## Checkpoint records

Each checkpoint records: loop identity, window and cumulative evaluation numbers, current SHA and
dirty state, per-tier counts and breadth, new/resolved/recurring defect classes, trend evidence,
outcome and rationale.

| # | Loop | Window / cumulative | SHA (dirty) | Critical | Standard | Defect classes | Outcome | Rationale |
| --- | --- | --- | --- | --- | --- | --- | --- | --- |

## Gate evidence

| Gate | Round | Run directory | Reviewed SHA | Outcome | Teardown |
| --- | --- | --- | --- | --- | --- |
| architect plan | 0 (usage-limited session, NOT a plan of record) | `/tmp/cdx-gate-architect.TgaE9v` | n/a (plan over `3a8469e`) | `failed` — both turns ended empty on the account usage limit; collected `--outcome failed`, refused `declared_failed` | confirmed stopped |
| architect plan | 1 (primary turn certified) | `/tmp/cdx-gate-architect.ZFvc0t` (removed after copy to `.codex/plans/issue-158.md` + `.attest.json`) | n/a (plan over `3a8469e`) | `completed`, attested `ok:true` | confirmed stopped |

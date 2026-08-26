# Audit ledger — issue #155 (M12.17 connector dynamic path and connector/replay contracts)

Audit record for the completion workflow (`CLAUDE.md`, amended 2026-08-12 / -08-14; standing
rules in `docs/architecture/COMPLETION_WORKFLOW_RULES.md`).

Plan of record: `.codex/plans/issue-155.md` (the attested architect plan, plus the folded
2026-08-25 addendum and the corrections that override it), sha256
`d60496a6fb2083f9c371deae8dc84009051aa44551204fe157aa45b3fee00f0d`; attestation
`.codex/plans/issue-155.attest.json`. The landing strategy this ledger executes lives at
`docs/plans/issue-155-strategy.md` (local-only, like every plan file here).

## Slice map (six governed slices, ONE open issue, ONE ledger, ONE archive)

The issue is landed in six separately-gated slices. Each gets its own Stage-1 step-0 baseline
block below, its own roster windows, its own composite wave gate and its own closing protocol;
#155 stays OPEN until the last one. A finding whose fix belongs to a later slice's unit is
deferred `blocked-by-mechanism` naming that slice, never silently carried.

| Slice | Scope | Kind | Status |
| --- | --- | --- | --- |
| A | canonical dynamic-path binding on both connector roles, its lineage rule, the published family capability, and the explicit process-source replay policy | behaviour-affecting | in progress |
| B | the packaged replay-evidence registry, identifier grammar, both digest algorithms, the derived audit report, the execution-connector monitor action | src-changing, narrowly reachable | not started |
| C | the trusted connector-resolution snapshot and the blank-path cross-check | behaviour-affecting | not started |
| D | per-call replay grants, the shared reference grammar, candidate discovery, the account-independent semantic revision | mostly unreachable in production | not started |
| E | apply-boundary identity rechecks and the evidence attestation tuple | mutation accounting | not started |
| F | evidence ingestion and production enablement — the only slice that can close #155 | evidence-gated | not started |

Diagnostic identifiers a later slice introduces are described here in prose until the slice that
registers them lands; the ledger names a code only once the taxonomy carries it.

## Slice A — Stage-1 step 0 — baseline

| Field | Value |
| --- | --- |
| Issue | #155 — M12.17 connector dynamic path and connector/replay contracts |
| Step-0 baseline (`$BASELINE`) | `9860842e5932c2e091a115e4697722bd8429953c` |
| Branch | `codex/issue-155` |
| Branch point | `origin/dev` @ `9860842` (tip of #180) |
| Baseline suite | 10520 passed, 17 skipped (10537 collected) — full non-KB, local `.venv` 3.12, `PYTHONPATH=src`, before any edit |
| Baseline manifests | 10537 required nodes, 70 active goldens |
| Slice kind | behaviour-affecting |

The collected count 10537 equals the recorded floor from #180, so the floor is current on the
branch point.

## Artifact trust boundary

The slice CREATES AND OWNS: the canonical path binding and its semantic mirrors, the family
capability authority and its published projection, the replay-evidence package and its derived
report, the resolution snapshot, the grant symbols, the attestation tuple, the issue-155 goldens
and fixtures (regression pins, never their own oracle), this ledger and its evidence archive, and
its own entries in the wave-gate test module. It CONSUMES, and never re-authors: the legacy
process-flow builder and shape renderer (the parity oracle), the Boomi platform readbacks and
execution records, the sandbox counterparty, and the committed wave-gate manifests. A finding that
only hardens a self-owned artifact gets a boundary verdict at reconciliation instead of a fix batch
by default.

## Loop roster (fixed BEFORE the first correction; identical shape per slice, FRESH windows each)

A gate not on this list cannot mint a loop mid-run; adding one is itself a recorded checkpoint
decision.

1. Stage-1 QA — live scoped pass through the public MCP tool boundary (`boomi-qa-tester`)
2. Stage-2 repo Codex commit review — delta-scoped; every round COLLECTED, never read from a poll
3. Architect implementation review — FIXED CAP of three evaluations per
   `docs/architecture/COMPLETION_WORKFLOW_RULES.md`; the prompt inlines the plan of record and is
   scoped to the slice's units, and an absence finding for a later slice's unit is refuted citing
   the slice map
4. Composite wave gate — full suite, every active golden, determinism, manifests; its passing SHA
   is named in the checkpoint row
5. Closing protocol — the record-only commits after the wave gate, validated by a darkness proof
   and the ledger scanners; a terminal correction loop exists only via a recorded roster addition

Evidence-capture dispatches, polls, approvals, claim-only re-asks and the advisory image-parity
workflow are not evaluations.

## Defect-class ledger (a class is a (mechanism, runtime-authority) pair, assigned at reconciliation)

Pre-enumerated from prior slices so a second instance triggers structurally ON ARRIVAL.

| Class | Mechanism | Runtime authority | Instances (derived from rows) | Resolution |
| --- | --- | --- | --- | --- |
| DC-155-A | a per-family fact hand-copied into per-action rows | the connector family capability table | 0 | — |
| DC-155-B | a second graph walk or a second resolver of an already-resolved fact | the compiler's own lowering and binding resolution; the existing component resolver; the lineage visitor | 0 | — |
| DC-155-C | a hand-listed enumeration of references, actions or codes | pydantic annotations; the router's own action list; the registry; the error taxonomy | 0 | — |
| DC-155-D | a guard that enumerates its own universe | derive the population by parsing the tree | 0 | — |
| DC-155-E | a caller-asserted fact treated as authoritative | the trusted resolution snapshot | 0 | — |
| DC-155-F | a deny-list standing in for a format constraint | a closed grammar or enum | 0 | — |
| DC-155-G | a closure recorded ahead of its owed validation | the validation itself (#154's recurring process defect) | 0 | — |
| DC-155-H | a derived manifest regenerated from a non-final tree | the final tree | 0 | — |

Second-instance check runs at row-write time and whenever a second finding lands in the same
file or subsystem within a loop.

## Finding rows (one per raw finding; append-only; exactly one disposition each)

| ID | Source gate + run dir + attestation | Verbatim summary | Original label | Blocking class | Defect class | Derived tier (anchor inline) | SHA/delta | Disposition |
| --- | --- | --- | --- | --- | --- | --- | --- | --- |
| EVAL-155-01 | self, step 0, measured at `9860842` (no gate run) | "A ProcessIR document with a dynamic-path binding on the source ... compiles and emits byte-identical XML to `dynamic_path_source_ddp.xml`" — the issue's acceptance criterion names a source-side oracle that does not exist: both `dynamic_path_source_ddp.xml` and `dynamic_path_target_profile.xml` are TARGET-side REST PATCH shapes, "source" naming the DDP segment source rather than the connector role, so the criterion is unsatisfiable as written | none (self-recorded premise refutation) | machine-served schemas/contracts | none | standard — anchor: no unfixed defect in shipped behaviour; the artifact is an acceptance premise, not code | `9860842` | fixed — a true source-role legacy oracle was frozen before any source edit (`golden-000071`, provenance recorded); the criterion is read as "the source-role binding equals the source-role legacy render", and the issue description is corrected |
| EVAL-155-02 | self, step 0, measured at `9860842` (no gate run) | The issue's "a dynamic-path binding on the source AND one on the target" in one document is only partly expressible: the mapped both-sides composition the legacy chain emits today is refused at parse time by the root map-adjacency rule, so it is unrepresentable on the canonical chain | none (self-recorded scope finding) | capability reachability | none | standard — anchor: a legacy capability with no canonical spelling, discovered before implementation | `9860842` | deferred — reason class `out-of-scope-by-design`; placement: recorded against #160 as a cutover blocker in both issue descriptions; the no-map both-bound form IS covered by this slice |
| EVAL-155-03 | self, step 0, measured at `9860842` (no gate run) | Composition decision for path-bound connector nodes adjacent to a Branch or Decision: bound nodes inherit the existing placement rules and no new placement refusal is introduced, with bound-target, bound-call and bound-entry fan-out witnesses proving each admitted shape | none (self-recorded design decision) | runtime behavior | none | standard — anchor: a design decision recorded before the first correction, not a defect | `9860842` | fixed — decision recorded here and carried into the slice's test matrix |

Dispositions: `fixed` · `finding-refuted` · `severity-refuted` · `not-validated` · `deferred`
(issue, reason class, placement). An original label is never edited — a revision is a new dated
row whose id carries an `a` suffix, with the original retained and a Supersession map entry.

## Checkpoints (written IN FLIGHT at every third evaluation of each loop, in the batch it governs)

| Loop | Evaluation (window / cumulative) | SHA (+dirty) | Outcome | Rationale |
| --- | --- | --- | --- | --- |

## Deferrals

Pointer-only — reason class, placement and lineage live on the finding row. `EVAL-155-02` is the
only open deferral; it is `out-of-scope-by-design`, so it carries no window-exhausted budget.

## Evidence index

Collected run directories and live captures are archived, byte-verified, under
`docs/architecture/evidence/issue-155/` with `index.jsonl` + `SHA256SUMS`. Run citations always
use the COMPLETE run-directory name. The archive skeleton is created in this same commit.

## Live-evidence capture (slice F's input; decoupled from ingestion)

The renera account `traininghlibbochkarov-JKIY2X` expires 2026-08-28T05:00Z. Class-level capture
evidence is account-independent and survives the roll; an operation-specific record is bound to its
account and is deliberately NOT ingested from this account — slice F mints its record on whatever
account is active when it lands. Captures are archived under `captures/` as they are taken.

## Final-tree validation (filled at close; every roster gate current on the FINAL sha)

| Gate | Evidence (quoted output / run URL / archived round) | SHA |
| --- | --- | --- |
